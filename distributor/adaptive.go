package distributor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

const BlockSize = 1 << 22
const Pipeline = 5
const ReportDur = 5 * time.Second

type perf struct {
	blockCount  int
	elapsedTime time.Duration
}

type senderPerf struct {
	networkPerf perf
	clientPerf  map[SourceType]perf
}

func newSenderPerf() *senderPerf {
	return &senderPerf{clientPerf: make(map[SourceType]perf)}
}

// AdaptiveLeaderNode implements flow-based adaptive leader node.
type AdaptiveLeaderNode struct {
	*LeaderNode
	LayerDests    map[LayerID]NodeID // caution: only one dest is accepted in this implementaion!
	nodeNetworkBW map[NodeID]int64
	nodeClientBW  map[NodeID]map[SourceType]int64
	client        Client
	// start reporting once the receiver gets either reqMsg (as a sender) or blockMsg (as a receiver)
	reportOnce           sync.Once
	senderPerf           *senderPerf
	senderJobsMap        map[NodeID][]job
	senderReportMsgMap   map[NodeID]*senderReportMsg
	receiverReportMsgMap map[NodeID]*receiverReportMsg

	flowGraph *flowGraph
	mu        sync.RWMutex
}

func NewAdaptiveLeaderNode(node node, layers LayersSrc, assignment Assignment, nodeNetworkBW map[NodeID]int64, limitersMap LimitersMap) *AdaptiveLeaderNode {
	leaderBase := newLeaderNodeBase(node, layers, assignment)

	// initialize layerDests
	layerDests := make(map[LayerID]NodeID)
	for destID, layerIDs := range assignment {
		for layerID := range layerIDs {
			if _, ok := layerDests[layerID]; !ok {
				layerDests[layerID] = destID
			} else {
				log.Error().Uint("layerID", uint(layerID)).Msg("a layer assigned to multiple layers")
			}
		}
	}

	// copy to avoid accidental external mutation while scheduling jobs
	networkBW := make(map[NodeID]int64, len(nodeNetworkBW))
	for nodeID, bw := range nodeNetworkBW {
		networkBW[nodeID] = bw
	}

	// initialize client BW
	clientBW := make(map[NodeID]map[SourceType]int64)
	for senderID, layerIDs := range leaderBase.status {
		clientBW[senderID] = make(map[SourceType]int64)
		for _, meta := range layerIDs {
			clientBW[senderID][meta.SourceType] = meta.LimitRate
		}
	}

	alNode := &AdaptiveLeaderNode{leaderBase, layerDests, networkBW, clientBW, NewInmemClient(node, layers, &leaderBase.mu, limitersMap), sync.Once{}, newSenderPerf(), make(map[NodeID][]job), make(map[NodeID]*senderReportMsg), make(map[NodeID]*receiverReportMsg), nil, sync.RWMutex{}}

	// alNode.sendLayersFn = alNode.sendLayers

	alNode.handleIncomingMsg()

	return alNode
}

func (leader *AdaptiveLeaderNode) handleIncomingMsg() {
	go func() {
		for incomingMsg := range leader.GetTransport().Deliver() {
			log.Debug().Msgf("incoming msg[%T]: %s", incomingMsg, incomingMsg)
			switch v := incomingMsg.(type) {

			case *announceMsg:
				go leader.handleAnnounceMsg(v)
			case *ackMsg:
				go leader.handleAckMsg(v)
			case *reqMsg:
				go leader.handleReqMsg(v)
			case *senderReportMsg:
				go leader.handleSenderReportMsg(v)
			case *receiverReportMsg:
				go leader.handleReceiverReportMsg(v)

			// start the inference engine
			case *layerMsg:
				go leader.handleLayerMsg(v)
			}
		}
	}()
}

func (leader *AdaptiveLeaderNode) handleAnnounceMsg(announceMsg *announceMsg) {

	leader.mu.Lock()
	// checks if the announcement is already received
	_, ok := leader.status[announceMsg.SrcID]

	if !ok {
		// initialize the value (map of layers the receiver already has)
		leader.status[announceMsg.SrcID] = announceMsg.LayerIDs
		// add the receiver as neighbor
		leader.node.addNode(announceMsg.SrcID)

		// update nodeClientBW for the new node
		leader.nodeClientBW[announceMsg.SrcID] = make(map[SourceType]int64)
		for _, meta := range announceMsg.LayerIDs {
			leader.nodeClientBW[announceMsg.SrcID][meta.SourceType] = meta.LimitRate
		}
	}
	leader.mu.Unlock()

	leader.mu.RLock()
	a := leader.assignment
	s := leader.status
	leader.mu.RUnlock()
	// checks if all nodes in the assignment are connected by comparing the keys of assignment and status
	for nodeID := range a {
		_, ok := s[nodeID]
		if !ok {
			return
		}
	}

	// start sending layers
	leader.startDistributionChan <- a
	log.Info().Msg("timer start")
	_, selfJobsMap, jobsMap := leader.assignJobs()

	leader.mu.Lock()
	for _, jobs := range jobsMap {
		for _, job := range jobs {
			leader.senderJobsMap[job.SenderID] = append(leader.senderJobsMap[job.SenderID], job)
		}
	}
	leader.mu.Unlock()

	err := leader.dispatchJobs(selfJobsMap, jobsMap)
	if err != nil {
		log.Error().Err(err).Send()
	}
}

func (leader *AdaptiveLeaderNode) handleReqMsg(reqMsg *reqMsg) {
	leader.reportOnce.Do(func() {
		go leader.senderReportLoop()
	})
	err := sendBlock(leader.node, leader.client, leader.layers, leader.senderPerf, &leader.mu, reqMsg)

	if err != nil {
		log.Error().Err(err).Send()
	}
}

// todo: integrate duplicated code
func (leader *AdaptiveLeaderNode) senderReportLoop() {
	c := time.Tick(ReportDur)
	counter := 0

	// report client/network rate
	// count numbers of blocks and elapsed time
	for range c {
		leader.mu.RLock()

		cliPerf := leader.senderPerf.clientPerf
		netPerf := leader.senderPerf.networkPerf

		cliRate := make(map[SourceType]int64)

		for sourceType, perf := range cliPerf {
			cliRate[sourceType] = int64(perf.blockCount*BlockSize) * int64(time.Second) / int64(perf.elapsedTime)
		}

		netRate := int64(netPerf.blockCount*BlockSize) * int64(time.Second) / int64(netPerf.elapsedTime)
		leader.mu.RUnlock()

		counter++

		senderRepoMsg := NewSenderReportMsg(leader.GetMyID(), cliRate, netRate, counter)

		log.Debug().Str("msg", fmt.Sprintln(senderRepoMsg)).Msg("senderRepoMsg sent")

		err := leader.GetTransport().Send(leader.getLeader(), senderRepoMsg)
		if err != nil {
			log.Error().Err(err).Send()
		}
	}
}

func (leader *AdaptiveLeaderNode) handleSenderReportMsg(repoMsg *senderReportMsg) error {
	log.Debug().Uint("sender", uint(repoMsg.SrcID)).Msg("repoMsg received")

	// collect reports
	leader.mu.Lock()
	leader.senderReportMsgMap[repoMsg.SrcID] = repoMsg
	leader.mu.Unlock()

	if !leader.reportCollected() {
		return nil
	}
	return leader.jobsReassignment()

}

func (leader *AdaptiveLeaderNode) handleReceiverReportMsg(repoMsg *receiverReportMsg) error {
	log.Debug().Uint("receiver", uint(repoMsg.SrcID)).Msg("repoMsg received")

	// collect reports
	leader.mu.Lock()
	leader.receiverReportMsgMap[repoMsg.SrcID] = repoMsg
	leader.mu.Unlock()

	if !leader.reportCollected() {
		return nil
	}
	return leader.jobsReassignment()
}

func (leader *AdaptiveLeaderNode) jobsReassignment() error {

	log.Info().Msg("re-assigning jobs")

	leader.mu.Lock()

	for senderID, repoMsg := range leader.senderReportMsgMap {
		// update sender's network bw
		leader.flowGraph.nodeNetworkBW[senderID] = repoMsg.NetworkRate

		// update sender's client bw
		for sourceType, rate := range repoMsg.ClientRate {
			leader.flowGraph.nodeClientBW[senderID][sourceType] = rate
		}
	}

	for _, repoMsg := range leader.receiverReportMsgMap {
		// todo: update receiver's network bw
		// leader.flowGraph.nodeNetworkBW[receiverID] = repoMsg.networkRate

		// update remaining data size
		for layerID, size := range repoMsg.RemainingDataSize {
			layerSrc, ok := leader.layers[layerID]
			if !ok {
				log.Warn().Uint("layerID", uint(layerID)).Msg("layer not found")
				continue
			}
			layerSrc.DataSize = size
			leader.layers[layerID] = layerSrc
		}
	}

	_, updatedJobsMap := leader.flowGraph.getJobAssignment()

	// reset current map
	leader.senderJobsMap = make(map[NodeID][]job)
	leader.senderReportMsgMap = make(map[NodeID]*senderReportMsg)
	leader.receiverReportMsgMap = make(map[NodeID]*receiverReportMsg)

	for _, jobs := range updatedJobsMap {
		for _, job := range jobs {
			leader.senderJobsMap[job.SenderID] = append(leader.senderJobsMap[job.SenderID], job)
		}
	}

	leader.mu.Unlock()

	return leader.dispatchJobs(make(destJobsMap), updatedJobsMap)
}

// judges if reportMsgs are collected from all senders
func (leader *AdaptiveLeaderNode) reportCollected() bool {
	leader.mu.RLock()
	defer leader.mu.RUnlock()

	// sender
	if len(leader.senderReportMsgMap) < len(leader.senderJobsMap) {
		return false
	}

	for senderID := range leader.senderJobsMap {
		_, ok := leader.senderReportMsgMap[senderID]
		if !ok {
			return false
		}
	}

	// receiver
	if len(leader.receiverReportMsgMap) < len(leader.assignment) {
		return false
	}

	for receiverID := range leader.assignment {
		_, ok := leader.receiverReportMsgMap[receiverID]
		if !ok {
			return false
		}
	}

	return true
}

func (leader *AdaptiveLeaderNode) assignJobs() (t int64, selfJobsMap, jobsMap destJobsMap) {
	// divide jobs into self-assignment and others
	selfJobsMap = make(destJobsMap)
	modifiedAssignment := make(Assignment)

	for destID, layerIDs := range leader.assignment {
		for layerID, meta := range layerIDs {
			// if the destination has the layer in its client, directly send the layer from the client to the node
			if _, ok := leader.status[destID][layerID]; ok {
				selfJobsMap[destID] = append(selfJobsMap[destID], job{destID, layerID, meta.LimitRate, leader.layers[layerID].DataSize})
			} else {
				if _, ok := modifiedAssignment[destID]; !ok {
					modifiedAssignment[destID] = make(LayerIDs)
				}
				modifiedAssignment[destID][layerID] = meta
			}
		}
	}

	// skip flow calculation if no jobs to assign
	if len(modifiedAssignment) == 0 {
		log.Info().Msg("No jobs to assign other than self-assignment")
		return 0, selfJobsMap, make(destJobsMap)
	}

	t0 := time.Now()
	leader.flowGraph = newFlowGraph(modifiedAssignment, leader.status, leader.layers, leader.nodeNetworkBW, leader.nodeClientBW)
	t, jobsMap = leader.flowGraph.getJobAssignment()

	t1 := time.Since(t0)

	log.Info().Dur("computation time", t1).Msg("Job assignment completed")

	return t, selfJobsMap, jobsMap
}

// Jobs are assigned to receivers
func (leader *AdaptiveLeaderNode) dispatchJobs(selfJobsMap, destJobs destJobsMap) error {
	// self-assignment
	for destID, jobInfos := range selfJobsMap {
		for _, job := range jobInfos {
			// fixme: currently only block 0 is requested
			// todo: use clientReqMsg?
			frMsg := NewReqMsg(
				leader.node.GetMyID(), job.LayerID, job.SenderID, 0, job.Rate)

			// this time, jobs are sent to receivers
			err := leader.GetTransport().Send(destID, frMsg)
			if err != nil {
				return err
			}
		}
	}

	// for each sender, assign jobs from jobs slice
	for destID, jobs := range destJobs {
		jobMsg := NewJobMsg(leader.node.GetMyID(), jobs)

		// this time, jobs are sent to receivers
		err := leader.GetTransport().Send(destID, jobMsg)
		if err != nil {
			return err
		}
	}

	return nil
}

// AdaptiveReceiverNode

type AdaptiveReceiverNode struct {
	*ReceiverNode
	layerManagerMap map[LayerID]*LayerManager
	client          Client
	senderPerf      *senderPerf

	// start reporting once the receiver gets either reqMsg (as a sender) or blockMsg (as a receiver)
	reportOnce sync.Once
}

type BlockID int

func (blockID *BlockID) getOffset() int64 {
	return int64(*blockID) * BlockSize
}

type ActiveSender struct {
	rate     int64
	inflight chan struct{}
	stop     chan struct{}
}

type LayerManager struct {
	ReceivedBlocks map[BlockID]struct{}
	NextReqBlock   BlockID
	TotalBlockNum  BlockID
	// key: sender, val: rate, inflight, stop
	ActiveSenders map[NodeID]ActiveSender
}

func (lm *LayerManager) getNextAndIncrement() (BlockID, bool) {
	cur := lm.NextReqBlock
	// no block remaining to be requested
	if cur >= lm.TotalBlockNum {
		return 0, false
	}

	lm.NextReqBlock += 1
	return cur, true
}

func (receiver *AdaptiveReceiverNode) handleBlockMsg(blockMsg *blockMsg) {
	receiver.reportOnce.Do(func() {
		go receiver.receiverReportLoop()
	})
	receiver.mu.Lock()

	lm := receiver.layerManagerMap[blockMsg.LayerID]
	lm.ReceivedBlocks[blockMsg.BlockID] = struct{}{}

	receiveDone := len(lm.ReceivedBlocks) == int(lm.TotalBlockNum)

	receiver.mu.Unlock()

	if receiveDone {
		// layer download completed; send ackMsg to leader
		ackMsg := NewAckMsg(receiver.GetMyID(), blockMsg.LayerID)
		err := receiver.GetTransport().Send(receiver.getLeader(), ackMsg)
		if err != nil {
			log.Error().Err(err).Msg("failed to send ackMsg")
		}
	} else {
		// request next block
		lm.ActiveSenders[blockMsg.SrcID].inflight <- struct{}{}
	}
}

func NewAdaptiveReceiverNode(node node, layers LayersSrc, storagePath string, layerManagerMap map[LayerID]*LayerManager, limitersMap LimitersMap) *AdaptiveReceiverNode {
	receiverBase := newReceiverNodeBase(node, layers, storagePath)

	arNode := &AdaptiveReceiverNode{
		receiverBase,
		layerManagerMap,
		NewInmemClient(node, layers, &receiverBase.mu, limitersMap),
		newSenderPerf(),
		sync.Once{}}

	arNode.handleIncomingMsg()

	return arNode
}

func (receiver *AdaptiveReceiverNode) handleIncomingMsg() {
	go func() {
		for incomingMsg := range receiver.GetTransport().Deliver() {
			log.Debug().Msgf("incoming msg[%T]: %s", incomingMsg, incomingMsg)
			switch v := incomingMsg.(type) {
			case *layerMsg:
				go receiver.handleLayerMsg(v)
			case *jobMsg:
				go receiver.handleJobMsg(v)
			case *reqMsg:
				go receiver.handleReqMsg(v)
			case *blockMsg:
				// mark current block as received
				go receiver.handleBlockMsg(v)

			// start the inference engine
			case *startupMsg:
				go receiver.handleStartupMsg(v)
			}
		}
	}()
}

// handleLayerMsg stores the layer to its memory, and then sends ack to the leader.
func (receiver *AdaptiveReceiverNode) handleLayerMsg(layerMsg *layerMsg) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()

	var layerSrc LayerSrc

	layerSrc, ok := receiver.layers[layerMsg.LayerID]
	if !ok {
		data := make(LayerData, 0)
		// initialize layerSrc
		layerSrc = LayerSrc{
			InmemData: &data,
			Fp:        "",
			DataSize:  0,
			Offset:    0,
			Meta: LayerMeta{
				Location: InmemLayer,
			},
		}
	}

	if layerSrc.DataSize < layerMsg.TotalSize {
		// save a part of layer
		layerSrc.DataSize += layerMsg.LayerSrc.DataSize
		// partialData := layerMsg.LayerSrc.InmemData
		// buf := *layerSrc.InmemData
		// copy(buf[layerMsg.LayerSrc.Offset:], *partialData)

		// store layer
		receiver.layers[layerMsg.LayerID] = layerSrc
		// log.Debug().Msgf("saved layer %v in memory", layerMsg.LayerID)

		log.Info().Msgf("l%d downloaded (%d B / %d B)", layerMsg.LayerID, layerSrc.DataSize, layerMsg.TotalSize)
	}
	if layerSrc.DataSize == layerMsg.TotalSize {
		log.Info().
			Uint("layer", uint(layerMsg.LayerID)).
			Int64("total_bytes", layerMsg.TotalSize).
			Msg("layer fully received")
		// send ack to leader
		ackMsg := NewAckMsg(receiver.node.GetMyID(), layerMsg.LayerID)
		err := receiver.GetTransport().Send(receiver.getLeader(), ackMsg)
		if err != nil {
			log.Error().Err(err).Msg("failed to send ackMsg")
		}
	}
}

// receivers request blocks based on the information in jobMsg
func (receiver *AdaptiveReceiverNode) handleJobMsg(jobMsg *jobMsg) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()

	for _, lm := range receiver.layerManagerMap {
		for _, senderInfo := range lm.ActiveSenders {
			// stop all the active senders, using channels
			close(senderInfo.stop)
		}

		lm.ActiveSenders = make(map[NodeID]ActiveSender)
	}

	// activeSenders field should be empty by this time; Fill it with new jobs

	for _, job := range jobMsg.Jobs {
		// update layerManagerMap
		_, ok := receiver.layerManagerMap[job.LayerID]
		if !ok {
			log.Error().Msg("LayerManager not initialized")
		}

		newSender := &ActiveSender{
			rate:     job.Rate,
			inflight: make(chan struct{}, Pipeline),
			stop:     make(chan struct{}),
		}

		receiver.layerManagerMap[job.LayerID].ActiveSenders[job.SenderID] = *newSender

		// maybe this causes deadlock? (mutex)
		go receiver.requestPipeline(job.LayerID, job.SenderID, newSender)

		// fill the pipeline with inflight structs
		for range Pipeline {
			newSender.inflight <- struct{}{}
		}
	}

}

func (receiver *AdaptiveReceiverNode) requestPipeline(layerID LayerID, senderID NodeID, sender *ActiveSender) {
	limiter := rate.NewLimiter(rate.Limit(sender.rate), BlockSize)
	for {
		// next request is dispatched as soon as inflight chan receive a new value
		select {
		case <-sender.stop:
			return
		case <-sender.inflight:
			receiver.mu.Lock()
			lm, ok := receiver.layerManagerMap[layerID]
			if !ok {
				log.Error().Msg("layerMsg not exist")
			}

			// create and send msg for getting next block (if any)
			nextBlockID, ok := lm.getNextAndIncrement()
			if !ok {
				receiver.mu.Unlock()
				return
			}

			reqMsg := NewReqMsg(receiver.GetMyID(), layerID, senderID, nextBlockID, sender.rate)

			receiver.mu.Unlock()

			err := receiver.GetTransport().Send(senderID, reqMsg)
			if err != nil {
				log.Error().Err(err).Send()
			}

			if err := limiter.WaitN(context.Background(), BlockSize); err != nil {
				return
			}
		}
	}
}

func (receiver *AdaptiveReceiverNode) handleReqMsg(reqMsg *reqMsg) {
	receiver.reportOnce.Do(func() {
		go receiver.senderReportLoop()
	})
	err := sendBlock(receiver.node, receiver.client, receiver.layers, receiver.senderPerf, &receiver.mu, reqMsg)
	if err != nil {
		log.Error().Err(err).Send()
	}
}

func (receiver *AdaptiveReceiverNode) senderReportLoop() {
	c := time.Tick(ReportDur)
	counter := 0

	// report client/network rate
	// count numbers of blocks and elapsed time
	for range c {
		receiver.mu.RLock()

		cliPerf := receiver.senderPerf.clientPerf
		netPerf := receiver.senderPerf.networkPerf

		cliRate := make(map[SourceType]int64)

		for sourceType, perf := range cliPerf {
			cliRate[sourceType] = int64(perf.blockCount*BlockSize) * int64(time.Second) / int64(perf.elapsedTime)
		}

		netRate := int64(netPerf.blockCount*BlockSize) * int64(time.Second) / int64(netPerf.elapsedTime)
		receiver.mu.RUnlock()

		counter++

		senderRepoMsg := NewSenderReportMsg(receiver.GetMyID(), cliRate, netRate, counter)

		log.Debug().Str("msg", fmt.Sprintln(senderRepoMsg)).Msg("senderRepoMsg sent")

		err := receiver.GetTransport().Send(receiver.getLeader(), senderRepoMsg)
		if err != nil {
			log.Error().Err(err).Send()
		}
	}
}

func (receiver *AdaptiveReceiverNode) receiverReportLoop() {

	c := time.Tick(ReportDur)
	counter := 0

	// report network rate & amount of data remaining to be collected
	// count numbers of blocks and elapsed time
	for range c {
		receiver.mu.RLock()

		// todo: measure elapsed time
		// netPerf := receiver.senderPerf.networkPerf
		// netRate := int64(netPerf.blockCount*BlockSize) * int64(time.Second) / int64(netPerf.elapsedTime)
		netRate := int64(0)

		// remaining data size
		remainingDataSize := make(map[LayerID]int64)

		for layerID, lm := range receiver.layerManagerMap {
			remainingDataSize[layerID] = int64((int(lm.TotalBlockNum) - len(lm.ReceivedBlocks)) * BlockSize)
		}

		receiver.mu.RUnlock()

		counter++

		receiverRepoMsg := NewReceiverReportMsg(receiver.GetMyID(), netRate, remainingDataSize, counter)

		log.Debug().Str("msg", fmt.Sprintln(receiverRepoMsg)).Msg("receiverRepoMsg sent")

		err := receiver.GetTransport().Send(receiver.getLeader(), receiverRepoMsg)
		if err != nil {
			log.Error().Err(err).Send()
		}
	}
}

func sendBlock(n node, client Client, layers LayersSrc, senderPerf *senderPerf, mu *sync.RWMutex, reqMsg *reqMsg) error {

	mu.RLock()
	layerSrc := layers[reqMsg.LayerID]
	mu.RUnlock()

	n.addNode(reqMsg.DestID)

	switch layerSrc.Meta.Location {
	case InmemLayer, DiskLayer:
		return fmt.Errorf("not implemented")

	case ClientLayer:
		block, durClientLoad := client.FetchBlock(blockReq{reqMsg.LayerID, reqMsg.BlockID, make(chan LayerSrc)})
		blockMsg := NewBlockMsg(n.GetMyID(), reqMsg.LayerID, block, reqMsg.BlockID)
		timeClientLoad := time.Now()

		err := n.GetTransport().Send(reqMsg.SrcID, blockMsg)
		timeNetworkSend := time.Now()
		if err != nil {
			return err
		}

		durNetworkSend := timeNetworkSend.Sub(timeClientLoad)

		mu.Lock()
		if _, ok := senderPerf.clientPerf[layerSrc.Meta.SourceType]; !ok {
			senderPerf.clientPerf[layerSrc.Meta.SourceType] = perf{}
		}
		senderPerf.clientPerf[layerSrc.Meta.SourceType] = perf{
			blockCount:  senderPerf.clientPerf[layerSrc.Meta.SourceType].blockCount + 1,
			elapsedTime: senderPerf.clientPerf[layerSrc.Meta.SourceType].elapsedTime + durClientLoad,
		}

		senderPerf.networkPerf = perf{
			blockCount:  senderPerf.networkPerf.blockCount + 1,
			elapsedTime: senderPerf.networkPerf.elapsedTime + durNetworkSend,
		}
		mu.Unlock()

		return nil

	default:
		return fmt.Errorf("unknown location")
	}
}
