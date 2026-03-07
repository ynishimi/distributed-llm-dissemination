package distributor

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// node interface has its ID.
type node interface {
	// Shows its ID
	GetMyID() NodeID
	// returns leader's nodeID
	getLeader() NodeID
	// returns next hop according to the routing table
	getNextHop(goal NodeID) (NodeID, error)

	// Adds a node and connects to it using transport layer
	addNode(goal NodeID)
	// adds a remote node to the routing table
	addRoutingTable(goal NodeID, nextHop NodeID, remainingHops uint)
	// Updates nodeID of leader
	updateLeader(nodeID NodeID) error
	// Returns transport
	GetTransport() Transport
}

type N struct {
	myID     NodeID
	leaderID NodeID
	t        Transport

	// todo: accept multiple values? (for indirect loading)
	routingTable map[NodeID]routingInfo

	mu sync.RWMutex
}

// Creates a new node.
func NewNode(myID NodeID, leaderID NodeID, t Transport) *N {
	newNode := &N{
		myID:         myID,
		leaderID:     leaderID,
		t:            t,
		routingTable: make(map[NodeID]routingInfo),
	}

	// add myself
	// newNode.addNode(myID)
	// add leader
	if myID != leaderID {
		newNode.addNode(leaderID)
	}

	return newNode
}

func (n *N) GetMyID() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.myID
}

// returns leader's nodeID
func (n *N) getLeader() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.leaderID
}

func (n *N) getNextHop(goalID NodeID) (NodeID, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	info, ok := n.routingTable[goalID]
	if !ok {
		return 0, fmt.Errorf("routing entry for the specified the goal does not exist")
	}

	return info.nextHop, nil
}

// adds node
func (n *N) addNode(goal NodeID) {
	// add it to routing table
	n.addRoutingTable(goal, goal, 1)
}

// adds node that is not directly connected to itself
func (n *N) addRoutingTable(goal NodeID, nextHop NodeID, remainingHops uint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.routingTable[goal] = routingInfo{
		nextHop:       nextHop,
		remainingHops: remainingHops,
	}
}

func (n *N) updateLeader(leaderID NodeID) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// leader can only be specified after being confirmed that the entry exists

	_, ok := n.routingTable[leaderID]
	if !ok {
		return fmt.Errorf("routing entry for the specified leader does not exist")
	}

	n.leaderID = leaderID
	return nil
}

func (n *N) GetTransport() Transport {
	return n.t
}

type NodeID uint
type LayerID uint

// set of NodeIDs
type NodeIDs map[NodeID]struct{}

type LayerMeta struct {
	Location  LayerLocation
	LimitRate int
}

// set of LayerIDs with its location, rate (used for job assignment)
type LayerIDs map[LayerID]LayerMeta

func (l LayerIDs) String() string {
	layerIds := make([]LayerID, 0, len(l))
	for id := range l {
		layerIds = append(layerIds, id)
	}
	return fmt.Sprint(layerIds)
}

// func (l LayerIDs) String() string {
// 	layerIds := make([]struct {
// 		layerID   LayerID
// 		limitRate int
// 	}, 0, len(l))
// 	for id, meta := range l {
// 		layerIds = append(layerIds, struct {
// 			layerID   LayerID
// 			limitRate int
// 		}{id, meta.LimitRate})
// 	}
// 	return fmt.Sprint(layerIds)
// }

// map of layers (has data in memory or has a path to the file)
type Layers map[LayerID]LayerSrc

type routingInfo struct {
	nextHop       NodeID
	remainingHops uint
}

// key: node, value: layers
type Assignment map[NodeID]LayerIDs

type status map[NodeID]LayerIDs

// content of LayerData
type LayerData []byte

// LayerLocation is an enum to identify the location of the layer.
type LayerLocation uint8

const (
	InmemLayer LayerLocation = iota
	DiskLayer
	ClientLayer
)

type LayerSrc struct {
	// InmemData is nil if layer is not in memory
	InmemData *LayerData
	// file path of the layer (in disk)
	Fp string
	// file Size
	Size int
	// Offset (not used yet)
	Offset int64
	// metadata
	Meta LayerMeta
}

// Leader
type Leader interface {
	// todo: Receives a new assignment and remember it. Return nil if the assignment is successfully registered.
	// update(a assignment) error

	// todo
	// crash(n node)

	// Notifies that the leader started distribution
	StartDistribution() <-chan Assignment

	// Notifies that the assignment is Ready
	Ready() <-chan Assignment // todo: maybe error should be sent when assignment was interrupted?
}

type LeaderNode struct {
	node
	layers     Layers
	assignment Assignment
	status     status
	// startDistributionChan notifies the start of distribution.
	startDistributionChan chan Assignment
	fetchChan             map[LayerID]chan LayerSrc
	readyChan             chan Assignment
	mu                    sync.RWMutex
}

func newLeaderNodeBase(node node, layers Layers, assignment Assignment) *LeaderNode {
	l := &LeaderNode{
		node:                  node,
		layers:                layers,
		assignment:            assignment,
		status:                make(status, len(assignment)),
		startDistributionChan: make(chan Assignment),
		fetchChan:             make(map[LayerID]chan LayerSrc),
		readyChan:             make(chan Assignment),
	}

	// only send keys of the map
	curLayerIDs := make(LayerIDs, len(l.layers))
	for k, layerSrc := range l.layers {
		curLayerIDs[k] = LayerMeta{layerSrc.Meta.Location, layerSrc.Meta.LimitRate}
	}

	l.status[l.GetMyID()] = curLayerIDs

	return l
}

func NewLeaderNode(node node, layers Layers, assignment Assignment) *LeaderNode {
	leaderNode := newLeaderNodeBase(node, layers, assignment)

	leaderNode.handleIncomingMsg()

	return leaderNode
}

// handle msg
func (leader *LeaderNode) handleIncomingMsg() {
	go func() {
		for incomingMsg := range leader.GetTransport().Deliver() {
			log.Debug().Msgf("incoming msg[%T]: %s", incomingMsg, incomingMsg)
			switch v := incomingMsg.(type) {
			// registers the peer
			case *announceMsg:
				go leader.handleAnnounceMsg(v)
			case *ackMsg:
				go leader.handleAckMsg(v)
				// receive layer
			case *layerMsg:
				go leader.handleLayerMsg(v)
			}
		}
	}()
}

// todo
// func (leader *leaderNode) update(a assignment) error {

// }

// handleAnnounceMsg registers a peer and starts sending the requested layers.
func (leader *LeaderNode) handleAnnounceMsg(announceMsg *announceMsg) {

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
	leader.sendLayers()
}

func (leader *LeaderNode) sendLayers() {
	leader.mu.RLock()
	a := leader.assignment
	s := leader.status
	leader.mu.RUnlock()

	for nodeID, layerIDs := range a {
		for layerID := range layerIDs {
			// skip the layer which is already stored in memory by the node
			if meta, ok := s[nodeID][layerID]; ok && meta.Location == InmemLayer {
				continue
			}

			layer, ok := leader.layers[layerID]
			if !ok {
				log.Warn().Msgf("no layers found for layerID:%v", layerID)
			}
			go func() {
				// always saves to the memory this time
				err := leader.sendLayer(nodeID, layerID, layer)
				if err != nil {
					log.Error().Err(err).Msgf("couldn't send a layer %v", layerID)
				}
			}()
		}
	}
}

func (leader *LeaderNode) sendLayer(dest NodeID, layerID LayerID, layerSrc LayerSrc) error {
	log.Debug().Msgf("sending layer %v", layerID)
	ls := layerSrc
	if layerSrc.Meta.Location == ClientLayer {
		log.Debug().Uint("layer", uint(layerID)).Msg("loading layer from client")
		// load the layer from the client
		pLs, err := leader.fetchFromClient(layerID)
		if err != nil {
			return err
		}
		ls = *pLs
	}
	err := leader.GetTransport().Send(dest, NewLayerMsg(leader.node.GetMyID(), layerID, ls))
	return err
}

func (leader *LeaderNode) fetchFromClient(layerID LayerID) (*LayerSrc, error) {
	log.Debug().Uint("layerID", uint(layerID)).Msg("fetching from client")
	leader.mu.Lock()
	ch, ok := leader.fetchChan[layerID]
	if !ok {
		ch = make(chan LayerSrc)
		leader.fetchChan[layerID] = ch
		leader.mu.Unlock()

		err := leader.GetTransport().Send(ClientID, NewClientReqMsg(leader.GetMyID(), layerID, false))
		if err != nil {
			return nil, err
		}
	}

	ls := <-ch
	leader.mu.Lock()
	delete(leader.fetchChan, layerID)
	leader.mu.Unlock()

	return &ls, nil
}

// handleLayerMsg stores the layer to its memory, and then sends ack to the leader.
func (leader *LeaderNode) handleLayerMsg(layerMsg *layerMsg) {
	leader.mu.Lock()
	defer leader.mu.Unlock()

	var layerSrc LayerSrc
	// load the layer to its memory
	layerSrc = LayerSrc{
		InmemData: layerMsg.LayerSrc.InmemData,
		Fp:        "",
		Size:      len(*layerMsg.LayerSrc.InmemData),
		Offset:    0,
		Meta: LayerMeta{
			Location: InmemLayer,
		},
	}
	log.Debug().Msgf("saved layer %v in memory", layerMsg.LayerID)

	// store layer
	leader.layers[layerMsg.LayerID] = layerSrc

	if ch, ok := leader.fetchChan[layerMsg.LayerID]; ok {
		ch <- layerSrc
	}

	// update my status
	// send ack to leader
	ackMsg := NewAckMsg(leader.node.GetMyID(), layerMsg.LayerID, layerSrc.Meta.Location)
	err := leader.GetTransport().Send(leader.getLeader(), ackMsg)
	if err != nil {
		log.Error().Err(err).Msg("failed to send ackMsg")
	}
}

// marks the delivery of ackMsg.layer to be done.
func (leader *LeaderNode) handleAckMsg(ackMsg *ackMsg) {
	leader.mu.Lock()

	curStatus := leader.status[ackMsg.SrcID]
	// add the layer to current status
	curStatus[ackMsg.LayerID] = LayerMeta{
		Location: ackMsg.location,
	}

	log.Debug().Str("status", fmt.Sprint(leader.status)).Msg("handleAckMsg")

	// checks if the assignment is completed
	if assignmentSatisfied(leader.assignment, leader.status) {
		leader.mu.Unlock()
		leader.sendStartup()
		// notify the assignment to be ready
		leader.readyChan <- leader.assignment
		return
	}

	leader.mu.Unlock()
}

// assignmentSatisfied checks if, for each node, to have all the assignmented layers.
func assignmentSatisfied(a Assignment, s status) bool {
	for node, layers := range a {
		for layer := range layers {
			// checks if the layer exists (in memory, not in disk or client) in the current status
			meta, ok := s[node][layer]
			if !ok || meta.Location != InmemLayer {
				return false
			}
		}
	}
	return true
}

func (leader *LeaderNode) StartDistribution() <-chan Assignment {
	return leader.startDistributionChan
}

func (leader *LeaderNode) Ready() <-chan Assignment {
	return leader.readyChan
}

func (leader *LeaderNode) sendStartup() error {
	leader.mu.RLock()
	s := leader.status
	leader.mu.RUnlock()

	for receiver := range s {
		startupMsg := NewStartupMsg(leader.node.GetMyID())
		err := leader.GetTransport().Send(receiver, startupMsg)
		if err != nil {
			return err
		}
	}
	return nil
}

// RetransmitLeaderNode has layer retransmission function.
type RetransmitLeaderNode struct {
	*LeaderNode
	// layerOwners has owners of each layer.
	layerOwners map[LayerID]NodeIDs
}

func NewRetransmitLeaderNodeBase(node node, layers Layers, assignment Assignment) *RetransmitLeaderNode {
	leaderBase := newLeaderNodeBase(node, layers, assignment)

	// initialize each value of layerOwners
	layerOwners := make(map[LayerID]NodeIDs, len(layers))
	for layerID := range layers {
		layerOwners[layerID] = make(NodeIDs)
	}

	retransmitLeaderBase := &RetransmitLeaderNode{
		LeaderNode:  leaderBase,
		layerOwners: layerOwners,
	}

	return retransmitLeaderBase
}

func NewRetransmitLeaderNode(node node, layers Layers, assignment Assignment) *RetransmitLeaderNode {
	retransmitLeader := NewRetransmitLeaderNodeBase(node, layers, assignment)

	retransmitLeader.handleIncomingMsg()

	return retransmitLeader
}

// handle msg
func (rLeader *RetransmitLeaderNode) handleIncomingMsg() {
	go func() {
		for incomingMsg := range rLeader.GetTransport().Deliver() {
			log.Debug().Msgf("incoming msg[%T]: %s", incomingMsg, incomingMsg)
			switch v := incomingMsg.(type) {
			case *announceMsg:
				go rLeader.handleAnnounceMsg(v)
			case *ackMsg:
				go rLeader.handleAckMsg(v)
			case *layerMsg:
				go rLeader.handleLayerMsg(v)
			}
		}
	}()
}

// handleAnnounceMsg registers a peer and starts sending the requested layers.
// In this case,
func (rLeader *RetransmitLeaderNode) handleAnnounceMsg(announceMsg *announceMsg) {

	rLeader.mu.Lock()
	// checks if the announcement is already received
	_, ok := rLeader.status[announceMsg.SrcID]

	if !ok {
		// initialize the value (map of layers the receiver already has)
		rLeader.status[announceMsg.SrcID] = announceMsg.LayerIDs
		// add the receiver as neighbor
		rLeader.node.addNode(announceMsg.SrcID)
	}
	rLeader.mu.Unlock()

	rLeader.mu.RLock()
	a := rLeader.assignment
	s := rLeader.status
	rLeader.mu.RUnlock()
	// checks if all nodes in the assignment are connected by comparing the keys of assignment and status
	for nodeID := range a {
		_, ok := s[nodeID]
		if !ok {
			return
		}
	}

	// start sending layers
	rLeader.startDistributionChan <- a
	rLeader.sendLayers()
}

// This time, the leader node fills layerOwners map, and retransmit if possible.
func (rLeader *RetransmitLeaderNode) sendLayers() {
	rLeader.mu.Lock()
	a := rLeader.assignment

	// add entries to layerOwners based on status map
	for nodeID, layerIDs := range rLeader.status {
		for layerID := range layerIDs {
			owners, ok := rLeader.layerOwners[layerID]
			if !ok {
				log.Error().Msgf("layerOwners is not initialized for the key %v", layerID)
			}
			owners[nodeID] = struct{}{}
			rLeader.layerOwners[layerID] = owners
		}
	}

	lo := rLeader.layerOwners
	rLeader.mu.Unlock()

	for nodeID, layerIDs := range a {
		for layerID := range layerIDs {
			// if some receivers already has the layer, the leader sends retransmit message instead
			if owners, ok := lo[layerID]; ok && len(owners) > 0 {
				// skip the layer which is already obtained by the node
				if _, ok = owners[nodeID]; ok {
					continue
				}

				// this time, the leader simply chooses a random owner of the map
				var owner NodeID
				for o := range owners {
					owner = o
					break
				}
				// send retransmit msg
				err := rLeader.sendRetransmit(layerID, owner, nodeID)
				if err != nil {
					log.Error().Err(err).Msgf("couldn't send retransmit of %v to owner %v", layerID, owner)
				}
			} else {
				layer, ok := rLeader.layers[layerID]
				if !ok {
					log.Warn().Msgf("no layers found for layerID:%v", layerID)
				}
				go func() {
					// always saves to the memory this time
					err := rLeader.sendLayer(nodeID, layerID, layer)
					if err != nil {
						log.Error().Err(err).Msgf("couldn't send a layer %v", layerID)
					}
				}()
			}
		}
	}
}

// sendRetransmit sends retransmit msg that asks the owner of a layer to the new destination
func (rLeader *RetransmitLeaderNode) sendRetransmit(layerID LayerID, owner NodeID, dest NodeID) error {
	log.Debug().Msgf("sending retransmit %v", layerID)
	// specifies the layer ID and the new dest.
	if owner == rLeader.GetMyID() {
		// if the owner is the leader itself, the owner directly sends the layer to dest's memory
		layer, ok := rLeader.layers[layerID]
		if !ok {
			log.Warn().Msgf("no layers found for layerID:%v", layerID)
		}
		return rLeader.sendLayer(dest, layerID, layer)
	}
	transmitMsg := NewRetransmitMsg(rLeader.node.GetMyID(), layerID, dest)
	// yet the transmitMsg itself is sent to the owner, not the dest of the layer.
	err := rLeader.GetTransport().Send(owner, transmitMsg)
	return err
}

// jobStatus indicates the status of the job
type jobStatus int

const (
	Pending jobStatus = iota
	// SendingDirectly           // todo: delete?
	// SendingIndirectly
	SendingRetransmit
	// Done
)

type jobInfo struct {
	// node which sends the layer to the destination
	sender         NodeID
	status         jobStatus
	retransmitTime *time.Time
}

// key: dest of the layer, val: sender and status
type jobInfos map[NodeID]jobInfo

// key: layer, val: map of jobs
type jobsInfoMap map[LayerID]jobInfos

// senderLoadCounter counts the current load of each sender.
// key: sender, val: count
type senderLoadCounter map[NodeID]uint

// nodePerformance stores the average throughput and the number of completed jobs achieved by the node
type nodePerformance map[NodeID]struct {
	aveThroughput       time.Duration
	completedJobCounter uint
}

// PullRetransmitLeaderNode implements pull-based transmit leader node.
type PullRetransmitLeaderNode struct {
	*RetransmitLeaderNode
	jobsInfoMap       jobsInfoMap
	senderLoadCounter senderLoadCounter

	nodePerformance nodePerformance

	// nodeCompletionStatus stores if the node satisfies the assignment
	nodeCompletionStatus map[NodeID]bool
}

func NewPullRetransmitLeaderNode(node node, layers Layers, assignment Assignment) *PullRetransmitLeaderNode {
	rLeaderBase := NewRetransmitLeaderNodeBase(node, layers, assignment)

	prLeader := &PullRetransmitLeaderNode{
		RetransmitLeaderNode: rLeaderBase,
		jobsInfoMap:          make(jobsInfoMap),
		senderLoadCounter:    make(senderLoadCounter),

		nodePerformance: make(nodePerformance),

		nodeCompletionStatus: make(map[NodeID]bool),
	}

	prLeader.handleIncomingMsg()

	return prLeader
}

// handle msg
func (prLeader *PullRetransmitLeaderNode) handleIncomingMsg() {
	go func() {
		for incomingMsg := range prLeader.GetTransport().Deliver() {
			log.Debug().Msgf("incoming msg[%T]: %s", incomingMsg, incomingMsg)
			switch v := incomingMsg.(type) {
			case *announceMsg:
				go prLeader.handleAnnounceMsg(v)
			case *ackMsg:
				go prLeader.handleAckMsg(v)
			case *layerMsg:
				go prLeader.handleLayerMsg(v)
			}
		}
	}()
}

func (prLeader *PullRetransmitLeaderNode) handleAnnounceMsg(announceMsg *announceMsg) {

	prLeader.mu.Lock()
	// checks if the announcement is already received
	_, ok := prLeader.status[announceMsg.SrcID]

	if !ok {
		// initialize the value (map of layers the receiver already has)
		prLeader.status[announceMsg.SrcID] = announceMsg.LayerIDs
		// add the receiver as neighbor
		prLeader.node.addNode(announceMsg.SrcID)
	}
	prLeader.mu.Unlock()

	prLeader.mu.RLock()
	a := prLeader.assignment
	s := prLeader.status
	prLeader.mu.RUnlock()
	// checks if all nodes in the assignment are connected by comparing the keys of assignment and status
	for nodeID := range a {
		_, ok := s[nodeID]
		if !ok {
			return
		}
	}

	// start sending layers
	prLeader.startDistributionChan <- a
	prLeader.sendLayers()
}

// This time, assigns a new job to the idle node.
func (prLeader *PullRetransmitLeaderNode) handleAckMsg(ackMsg *ackMsg) {
	prLeader.mu.Lock()

	curStatus := prLeader.status[ackMsg.SrcID]
	// add the layer to current status
	curStatus[ackMsg.LayerID] = LayerMeta{
		Location: ackMsg.location,
	}

	log.Debug().Str("status", fmt.Sprint(prLeader.status)).Msg("got ack msg")

	// checks if the assignment is completed for the first time
	if !prLeader.nodeCompletionStatus[ackMsg.SrcID] && assignmentSatisfied(prLeader.assignment, prLeader.status) {
		prLeader.nodeCompletionStatus[ackMsg.SrcID] = true
		prLeader.mu.Unlock()
		prLeader.sendStartup()
		// notify the assignment to be ready
		log.Info().Uint("id", uint(prLeader.GetMyID())).Msgf("startup")
		prLeader.readyChan <- prLeader.assignment
	} else {
		prLeader.mu.Unlock()
	}

	// delete a job and assign a new job (if applicable)
	jobInfo, ok := prLeader.jobsInfoMap[ackMsg.LayerID][ackMsg.SrcID]
	if !ok {
		// if the ack is for the layer loaded from the client, ignore it.
		// log.Error().Uint("node", uint(jobInfo.sender)).Uint("layerID", uint(ackMsg.LayerID)).Msg("unknown job")
		return
	}

	log.Info().Uint("node", uint(jobInfo.sender)).Uint("layerID", uint(ackMsg.LayerID)).Msg("job completed")

	throughput := time.Since(*jobInfo.retransmitTime)
	log.Debug().Str("throughput", throughput.String()).Send()

	prLeader.mu.Lock()

	nodePerformance, ok := prLeader.nodePerformance[jobInfo.sender]
	if !ok {
		// initialization
		a := struct {
			aveThroughput       time.Duration
			completedJobCounter uint
		}{0, 0}
		prLeader.nodePerformance[jobInfo.sender] = a
	}

	curAveThroughput := time.Duration(int64(throughput+nodePerformance.aveThroughput) / int64(nodePerformance.completedJobCounter+1))
	prLeader.nodePerformance[jobInfo.sender] = struct {
		aveThroughput       time.Duration
		completedJobCounter uint
	}{curAveThroughput, nodePerformance.completedJobCounter + 1}

	log.Debug().Str("ave throughput", time.Duration(curAveThroughput).String()).Uint("completed jobs", nodePerformance.completedJobCounter+1).Send()

	// delete completed job
	delete(prLeader.jobsInfoMap[ackMsg.LayerID], ackMsg.SrcID)

	prLeader.mu.Unlock()

	// assign a new job to the sender of the job
	err := prLeader.assignNewJob(jobInfo.sender)
	if err != nil {
		log.Error().Err(err).Msgf("failed to assign a new job after its ack %v", jobInfo.sender)
	}
}

// This time, the leader sends only the layers no other node has initially. Otherwise, it asks for retransmission.
func (prLeader *PullRetransmitLeaderNode) sendLayers() {
	log.Debug().Msg("start sending layers")
	prLeader.mu.Lock()
	a := prLeader.assignment

	// // includes the leader this time
	// myID := prLeader.GetMyID()
	// if _, ok := prLeader.status[myID]; !ok {
	// 	leaderLayers := make(LayerIDs)
	// 	for layerID := range prLeader.layers {
	// 		log.Debug().Uint("layerID", uint(layerID)).Msg("loaded a layer")
	// 		leaderLayers[layerID] = struct{}{}
	// 	}
	// 	prLeader.status[myID] = leaderLayers
	// }

	// add entries to layerOwners based on status map
	for nodeID, layerIDs := range prLeader.status {
		for layerID := range layerIDs {
			owners, ok := prLeader.layerOwners[layerID]
			if !ok {
				// log.Error().Msgf("layerOwners is not initialized for the key %v", layerID)
				owners = make(NodeIDs)
				prLeader.layerOwners[layerID] = owners
			}
			owners[nodeID] = struct{}{}
			prLeader.layerOwners[layerID] = owners
		}
	}
	layerOwners := prLeader.layerOwners

	// get the slice of layers sored in the ascending order regarding the number of owners
	sortedLayers := make([]LayerID, 0, len(layerOwners))
	for layerID := range layerOwners {
		sortedLayers = append(sortedLayers, layerID)
	}
	slices.SortFunc(sortedLayers, func(a, b LayerID) int {
		if len(layerOwners[a]) != len(layerOwners[b]) {
			return cmp.Compare(len(layerOwners[a]), len(layerOwners[b]))
		}
		return cmp.Compare(a, b) // tiebreak by layerID
	})

	// initialize jobsMap
	for dest, layerIDs := range a {
		nodeStatus := prLeader.status[dest]

		for layerID := range layerIDs {
			if meta, ok := nodeStatus[layerID]; !ok || meta.Location != InmemLayer {
				// create new jobs map if it doesn't exist
				if _, ok := prLeader.jobsInfoMap[layerID]; !ok {
					prLeader.jobsInfoMap[layerID] = make(jobInfos)
				}
				// register the layer ID and its destination (the job assignment is yet to be decided)
				prLeader.jobsInfoMap[layerID][dest] = jobInfo{}
			}
		}
	}

	// initialize job counter
	for nodeID := range prLeader.status {
		if _, ok := prLeader.senderLoadCounter[nodeID]; !ok {
			prLeader.senderLoadCounter[nodeID] = 0
		}
	}

	// assign jobs using sortedLayers
	for _, layerID := range sortedLayers {
		for dest := range prLeader.jobsInfoMap[layerID] {
			// assign a job to the node with minimum job
			sender := prLeader.getMinLoadedSender(layerID)
			prLeader.jobsInfoMap[layerID][dest] = jobInfo{sender, Pending, nil}
			prLeader.senderLoadCounter[sender]++
			log.Info().Msgf("job assignment: layer: %v, sender: %v", layerID, sender)
		}
	}

	prLeader.mu.Unlock()

	// sort nodeIDs in ascending order, for deterministic assignment
	nodeIDs := make([]NodeID, 0, len(a))
	for node := range a {
		nodeIDs = append(nodeIDs, node)
	}
	slices.Sort(nodeIDs)

	for _, node := range nodeIDs {
		go func() {
			err := prLeader.assignNewJob(node)
			if err != nil {
				log.Error().Err(err).Msgf("failed to assign a new job to node %v", node)
			}
		}()
	}
}

// assignNewJob assigns a new job to the node.
// If the node has the layer another node needs, the leader asks the node to retransmit the layer.
// Otherwise, the leader sends a layer in the job list.
func (prLeader *PullRetransmitLeaderNode) assignNewJob(node NodeID) error {
	prLeader.mu.Lock()

	if layerID, dest, jobInfo, ownJobAvailable := prLeader.getRarestOwnJob(node); ownJobAvailable {
		log.Debug().Uint("node", uint(node)).Uint("layer", uint(layerID)).Msg("pass a job initially assigned")
		jobInfo.status = SendingRetransmit
		// set timestamp
		now := time.Now()
		jobInfo.retransmitTime = &now
		prLeader.jobsInfoMap[layerID][dest] = jobInfo
		prLeader.senderLoadCounter[node]--
		prLeader.mu.Unlock()
		return prLeader.sendRetransmit(layerID, node, dest)
	}

	// if not, then tries to steal other's job which requires the layers the node has
	stolenLayerID, dest, stolenSender, stealableJobAvailable := prLeader.getRarestStealableJob(node)
	if stealableJobAvailable {
		log.Debug().Uint("layer", uint(stolenLayerID)).Msgf("steal a job from the most loaded node (%v) to node %v", stolenSender, node)
	} else {
		prLeader.mu.Unlock()
		log.Info().Uint("node", uint(node)).Msg("there is no job left to assign")
		return nil
	}

	prLeader.senderLoadCounter[stolenSender]--
	jobInfo := prLeader.jobsInfoMap[stolenLayerID][dest]
	jobInfo.sender = node
	jobInfo.status = SendingRetransmit
	now := time.Now()
	jobInfo.retransmitTime = &now
	prLeader.jobsInfoMap[stolenLayerID][dest] = jobInfo
	prLeader.mu.Unlock()

	// assigns the job to the node
	return prLeader.sendRetransmit(stolenLayerID, node, dest)
}

// getMinLoadedSender returns the sender with lowest limit rate, or the one with minimum jobs that has specified layer
func (prLeader *PullRetransmitLeaderNode) getMinLoadedSender(layerID LayerID) NodeID {
	var bestSender NodeID
	var bestRate int
	var minCount uint

	minCount = math.MaxUint
	for sender, count := range prLeader.senderLoadCounter {
		meta, ok := prLeader.status[sender][layerID]
		if !ok {
			continue
		}
		// prioritize the node with the lowest limit effectiveRate
		effectiveRate := meta.LimitRate
		if effectiveRate == 0 {
			// no rate limit!
			effectiveRate = math.MaxInt
		}

		// adopt the node with faster rate
		if (effectiveRate > bestRate) ||
			// make the selection deterministic (should be modified if the selection should be randomized)
			(effectiveRate == bestRate &&
				(count < minCount ||
					(count == minCount && sender < bestSender))) {
			bestSender = sender
			bestRate = effectiveRate
			minCount = count
		}
	}
	return bestSender
}

// getRarestOwnJob returns the rarest job the node is assigned.
func (prLeader *PullRetransmitLeaderNode) getRarestOwnJob(node NodeID) (rarestLayerID LayerID, rarestJobDest NodeID, rarestJobInfo jobInfo, ok bool) {
	minLayerOwnerCount := math.MaxInt
	ok = false

	// get a job initially assigned
	for layerID := range prLeader.status[node] {
		layerJobs, exists := prLeader.jobsInfoMap[layerID]
		if !exists || len(layerJobs) <= 0 {
			continue
		}
		for dest, jobInfo := range layerJobs {
			// if there is a still a job assigned to it, assign the job.
			if jobInfo.sender != node || jobInfo.status != Pending {
				continue
			}
			layerOwnerCount := len(prLeader.layerOwners[layerID])
			if layerOwnerCount < minLayerOwnerCount ||
				// deterministic selection
				(layerOwnerCount == minLayerOwnerCount && layerID < rarestLayerID) {
				minLayerOwnerCount = layerOwnerCount
				rarestLayerID = layerID
				rarestJobDest = dest
				rarestJobInfo = jobInfo
				ok = true
			}
		}
	}

	return rarestLayerID, rarestJobDest, rarestJobInfo, ok
}

func (prLeader *PullRetransmitLeaderNode) getRarestStealableJob(node NodeID) (rarestLayerID LayerID, rarestJobDest NodeID, stolenSender NodeID, ok bool) {

	type canditate struct {
		layerID      LayerID
		dest         NodeID
		sender       NodeID
		ownerCount   int
		timeToFinish time.Duration
	}

	var best *canditate

	// var maxTimeToFinish time.Duration

	// // get the list of jobs which the node can act.
	// jobStealableNodes := make([]NodeID, len(prLeader.assignment))

	for layerID := range prLeader.status[node] {
		ownerCount := len(prLeader.layerOwners[layerID])

		for dest, job := range prLeader.jobsInfoMap[layerID] {
			if job.sender == node || job.status != Pending {
				continue
			}
			sender := job.sender
			if prLeader.senderLoadCounter[sender] == 0 {
				continue
			}

			var timeToFinish time.Duration

			if _, ok := prLeader.nodePerformance[sender]; !ok {
				// as the sender is still stuck at its first job, the node should be prioritized over other nodes
				timeToFinish = math.MaxInt64

			} else {
				// calculate estimated time to finish
				timeToFinish = time.Duration(int64(prLeader.nodePerformance[sender].aveThroughput) * int64(prLeader.senderLoadCounter[sender]))
			}

			c := &canditate{layerID, dest, sender, ownerCount, timeToFinish}

			// 1st canditate
			if best == nil ||
				// canditate with rarer layer
				c.ownerCount < best.ownerCount ||
				// canditate with slower node
				(c.ownerCount == best.ownerCount && c.timeToFinish > best.timeToFinish) {
				best = c
			}
		}
	}

	if best == nil {
		return 0, 0, 0, false
	}

	return best.layerID, best.dest, best.sender, true
}

// Receiver
type Receiver interface {
	// announces its existence (with the layers it has) to leader
	Announce() error

	// Startup tells application layer that the layers are ready.
	Ready() <-chan struct{}
}

type ReceiverNode struct {
	node
	layers      Layers
	storagePath string
	readyChan   chan struct{}
	fetchChan   map[LayerID]chan LayerSrc
	mu          sync.RWMutex
}

func newReceiverNodeBase(node node, layers Layers, storagePath string) *ReceiverNode {

	return &ReceiverNode{
		node:        node,
		storagePath: storagePath,
		readyChan:   make(chan struct{}),
		fetchChan:   make(map[LayerID]chan LayerSrc),
		layers:      layers,
	}
}

func NewReceiverNode(node node, layers Layers, storagePath string) *ReceiverNode {
	receiverNode := newReceiverNodeBase(node, layers, storagePath)

	receiverNode.handleIncomingMsg()

	return receiverNode
}

// handle msg
func (receiver *ReceiverNode) handleIncomingMsg() {
	go func() {
		for incomingMsg := range receiver.GetTransport().Deliver() {
			log.Debug().Msgf("incoming msg[%T]: %s", incomingMsg, incomingMsg)
			switch v := incomingMsg.(type) {
			// receive layer
			case *layerMsg:
				go receiver.handleLayerMsg(v)
			// start the inference engine
			case *startupMsg:
				go receiver.handleStartupMsg(v)
			}
		}
	}()
}

func (receiver *ReceiverNode) fetchFromClient(layerID LayerID) (*LayerSrc, error) {
	log.Debug().Uint("layerID", uint(layerID)).Msg("fetching from client")
	receiver.mu.Lock()
	ch, ok := receiver.fetchChan[layerID]
	if !ok {
		ch = make(chan LayerSrc)
		receiver.fetchChan[layerID] = ch
		receiver.mu.Unlock()

		err := receiver.GetTransport().Send(ClientID, NewClientReqMsg(receiver.GetMyID(), layerID, false))
		if err != nil {
			return nil, err
		}
	}

	ls := <-ch
	receiver.mu.Lock()
	delete(receiver.fetchChan, layerID)
	receiver.mu.Unlock()

	return &ls, nil
}

// handleLayerMsg stores the layer to its memory, and then sends ack to the leader.
func (receiver *ReceiverNode) handleLayerMsg(layerMsg *layerMsg) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()

	var layerSrc LayerSrc
	// load the layer to its memory
	layerSrc = LayerSrc{
		InmemData: layerMsg.LayerSrc.InmemData,
		Fp:        "",
		Size:      len(*layerMsg.LayerSrc.InmemData),
		Offset:    0,
		Meta: LayerMeta{
			Location: InmemLayer,
		},
	}
	log.Debug().Msgf("saved layer %v in memory", layerMsg.LayerID)

	// store layer
	receiver.layers[layerMsg.LayerID] = layerSrc

	if ch, ok := receiver.fetchChan[layerMsg.LayerID]; ok {
		ch <- layerSrc
	}

	// send ack to leader
	ackMsg := NewAckMsg(receiver.node.GetMyID(), layerMsg.LayerID, layerSrc.Meta.Location)
	err := receiver.GetTransport().Send(receiver.getLeader(), ackMsg)
	if err != nil {
		log.Error().Err(err).Msg("failed to send ackMsg")
	}
}

// handleStartupMsg tells that the layers are ready to application layer.
func (receiver *ReceiverNode) handleStartupMsg(*startupMsg) {
	receiver.readyChan <- struct{}{}
}

// Announce announces its existence (with the layers it has) to leader.
func (receiver *ReceiverNode) Announce() error {
	receiver.mu.RLock()
	// only send keys of the map
	curLayerIDs := make(LayerIDs, len(receiver.layers))
	for k, layerSrc := range receiver.layers {
		curLayerIDs[k] = LayerMeta{
			Location:  layerSrc.Meta.Location,
			LimitRate: layerSrc.Meta.LimitRate,
		}

	}
	receiver.mu.RUnlock()

	nextHop, err := receiver.node.getNextHop(receiver.getLeader())
	if err != nil {
		log.Error().Msg("leader not found in routing table")
		return err
	}

	announceMsg := NewAnnounceMsg(receiver.node.GetMyID(), curLayerIDs)

	err = receiver.GetTransport().Send(nextHop, announceMsg)
	return err
}
func (receiver *ReceiverNode) Ready() <-chan struct{} {
	return receiver.readyChan
}

// RetransmitReceiverNode has layer retransmission function.
type RetransmitReceiverNode struct {
	*ReceiverNode
}

func NewRetransmitReceiverNode(node node, layers Layers, storagePath string) *RetransmitReceiverNode {
	receiverBase := newReceiverNodeBase(node, layers, storagePath)

	rReceiverNode := &RetransmitReceiverNode{
		ReceiverNode: receiverBase,
	}

	rReceiverNode.handleIncomingMsg()

	return rReceiverNode
}

// handle msg
func (rReceiver *RetransmitReceiverNode) handleIncomingMsg() {
	go func() {
		for incomingMsg := range rReceiver.GetTransport().Deliver() {
			log.Debug().Msgf("incoming msg[%T]: %s", incomingMsg, incomingMsg)
			switch v := incomingMsg.(type) {
			case *layerMsg:
				go rReceiver.handleLayerMsg(v)
			case *retransmitMsg:
				go rReceiver.handleRetransmitMsg(v)
			// start the inference engine
			case *startupMsg:
				go rReceiver.handleStartupMsg(v)
			}
		}
	}()
}

// handleRetransmitMsg sends specified layer to the destination.
func (rReceiver *RetransmitReceiverNode) handleRetransmitMsg(retransmitMsg *retransmitMsg) error {
	rReceiver.mu.RLock()
	ls := rReceiver.layers[retransmitMsg.LayerID]
	rReceiver.mu.RUnlock()

	// add the destination node to the routing table and connect to it
	rReceiver.addNode(retransmitMsg.DestID)

	if ls.Meta.Location == ClientLayer {
		log.Debug().Uint("layer", uint(retransmitMsg.LayerID)).Msg("loading layer from client")
		// load the layer from the client
		pLs, err := rReceiver.fetchFromClient(retransmitMsg.LayerID)
		if err != nil {
			return err
		}
		ls = *pLs
	}

	// send (retransmit) layer to dest.
	// the layer should be stored in memory
	layerMsg := NewLayerMsg(rReceiver.GetMyID(), retransmitMsg.LayerID, ls)
	err := rReceiver.GetTransport().Send(retransmitMsg.DestID, layerMsg)
	if err != nil {
		log.Error().Err(err).Msgf("failed to send layer to %v", retransmitMsg.DestID)
	}
	return err
}
