package distributor

import (
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const BlockSize = 256 * 1024
const Pipeline = 5

// AdaptiveLeaderNode implements flow-based adaptive leader node.
type AdaptiveLeaderNode struct {
	*LeaderNode
	LayerDests    map[LayerID]NodeID // caution: only one dest is accepted in this implementaion!
	NodeNetworkBW map[NodeID]int64
	client        Client
}

func NewAdaptiveLeaderNode(node node, layers LayersSrc, assignment Assignment, nodeNetworkBW map[NodeID]int64) *AdaptiveLeaderNode {
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

	alNode := &AdaptiveLeaderNode{leaderBase, layerDests, networkBW, NewInmemClient(node, layers, &leaderBase.mu)}

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
			// 	// receive layer
			case *reqMsg:
				go leader.handleReqMsg(v)
			case *progressReportMsg:
				go leader.handleReportMsg(v)
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
	time, selfJobsMap, jobsMap := leader.assignJobs()
	leader.dispatchJobs(time, selfJobsMap, jobsMap)
}

func (leader *AdaptiveLeaderNode) handleReqMsg(reqMsg *reqMsg) error {
	return sendBlock(leader.node, leader.client, leader.layers, &leader.mu, reqMsg)
}

func (leader *AdaptiveLeaderNode) handleReportMsg(repoMsg *progressReportMsg) error {

	// todo: collect reports (store msgs in somewhere?)

	if leader.reportCollected() {
		// update
		// todo: leader.assignJobs()
		// todo: leader.dispatchJobs()
	}

	return nil
}

func (leader *AdaptiveLeaderNode) reportCollected() bool {
	// todo
	return false
}

func (leader *AdaptiveLeaderNode) assignJobs() (t int64, selfJobsMap, jobsMap destJobs) {
	// divide jobs into self-assignment and others
	selfJobsMap = make(destJobs)
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
		return 0, selfJobsMap, make(destJobs)
	}

	t0 := time.Now()
	g := newFlowGraph(modifiedAssignment, leader.status, leader.layers, leader.NodeNetworkBW)
	t, jobsMap = g.getJobAssignment()

	t1 := time.Since(t0)

	log.Info().Dur("computation time", t1).Msg("Job assignment completed")

	return t, selfJobsMap, jobsMap
}

// Jobs are assigned to receivers
func (leader *AdaptiveLeaderNode) dispatchJobs(minTime int64, selfJobsMap, destJobs destJobs) error {
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

func (receiver *AdaptiveReceiverNode) markReceived(blockMsg *blockMsg) {
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

func NewAdaptiveReceiverNode(node node, layers LayersSrc, storagePath string, layerManagerMap map[LayerID]*LayerManager) *AdaptiveReceiverNode {
	receiverBase := newReceiverNodeBase(node, layers, storagePath)

	arNode := &AdaptiveReceiverNode{
		ReceiverNode:    receiverBase,
		layerManagerMap: layerManagerMap,
		client:          NewInmemClient(node, layers, &receiverBase.mu),
	}

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
				go func() {
					// mark current block as received
					receiver.markReceived(v)

				}()
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
		}
	}
}

func (receiver *AdaptiveReceiverNode) handleReqMsg(reqMsg *reqMsg) {
	err := sendBlock(receiver.node, receiver.client, receiver.layers, &receiver.mu, reqMsg)
	if err != nil {
		log.Error().Err(err).Send()
	}
}

func sendBlock(n node, client Client, layers LayersSrc, mu *sync.RWMutex, reqMsg *reqMsg) error {

	mu.RLock()
	layerSrc := layers[reqMsg.LayerID]
	mu.RUnlock()

	n.addNode(reqMsg.DestID)

	switch layerSrc.Meta.Location {
	case InmemLayer, DiskLayer:
		return fmt.Errorf("not implemented")

	case ClientLayer:

		blockMsg := NewBlockMsg(n.GetMyID(), reqMsg.LayerID, client.FetchBlock(blockReq{reqMsg.LayerID, reqMsg.BlockID, make(chan LayerSrc)}), reqMsg.BlockID)
		err := n.GetTransport().Send(reqMsg.SrcID, blockMsg)
		return err

	default:
		return fmt.Errorf("unknown location")
	}
}
