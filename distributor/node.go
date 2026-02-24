package distributor

import (
	"fmt"
	"os"
	"sync"

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

	// tell it to the transport layer
	log.Debug().Msgf("%v: connecting to %v", n.myID, goal)
	if err := n.GetTransport().Connect(goal); err != nil {
		log.Debug().Err(err).Msgf("failed to connect to %v", goal)
	}
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

// set of LayerIDs
type LayerIDs map[LayerID]struct{}

// map of layers (has data in memory or has a path to the file)
type Layers map[LayerID]*LayerSrc

type routingInfo struct {
	nextHop       NodeID
	remainingHops uint
}

// key: node, value: layers
type Assignment map[NodeID]LayerIDs

type status map[NodeID]LayerIDs

// content of LayerData
type LayerData []byte

type LayerSrc struct {
	// InmemData is nil if layer is not in memory
	InmemData *LayerData
	// file path of the layer (in disk)
	Fp string
	// file Size
	Size uint
	// Offset (not used yet)
	Offset int64
}

func (ls *LayerSrc) Read() (*LayerData, error) {
	if ls.InmemData != nil {
		// the layer is in memory
		return ls.InmemData, nil
	}

	if ls.Fp == "" {
		return nil, fmt.Errorf("no data source specified")
	}

	// the layer is in disk
	f, err := os.Open(ls.Fp)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, ls.Size)
	_, err = f.ReadAt(buf, ls.Offset)
	if err != nil {
		return nil, err
	}

	layerData := LayerData(buf)
	return &layerData, nil

}

func (l LayerIDs) String() string {
	layerIds := make([]LayerID, 0, len(l))
	for id := range l {
		layerIds = append(layerIds, id)
	}
	return fmt.Sprint(layerIds)
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
	readyChan             chan Assignment
	mu                    sync.RWMutex
}

func newLeaderNodeBase(node node, layers Layers, assignment Assignment) *LeaderNode {
	return &LeaderNode{
		node:                  node,
		layers:                layers,
		assignment:            assignment,
		status:                make(status, len(assignment)),
		startDistributionChan: make(chan Assignment),
		readyChan:             make(chan Assignment),
	}
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
			// skip the layer which is already obtained by the node
			if _, ok := s[nodeID][layerID]; ok {
				continue
			}

			layer, ok := leader.layers[layerID]
			if !ok {
				log.Warn().Msgf("no layers found for layerID:%v", layerID)
			}
			err := leader.sendLayer(nodeID, layerID, layer)
			if err != nil {
				log.Error().Err(err).Msgf("couldn't send a layer %v", layerID)
			}
		}
	}
}

func (leader *LeaderNode) sendLayer(dest NodeID, layerID LayerID, layerSrc *LayerSrc) error {
	log.Debug().Msgf("sending layer %v", layerID)
	layerMsg := NewLayerMsg(leader.node.GetMyID(), layerID, layerSrc)
	err := leader.GetTransport().Send(dest, layerMsg)
	return err
}

// marks the delivery of ackMsg.layer to be done.
func (leader *LeaderNode) handleAckMsg(ackMsg *ackMsg) {
	leader.mu.Lock()

	curStatus := leader.status[ackMsg.SrcID]
	// add the layer to current status
	curStatus[ackMsg.LayerID] = struct{}{}

	log.Debug().Str("status", fmt.Sprint(leader.status)).Msg("status")

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
			// checks if the layer exists in the current status
			if _, ok := s[node][layer]; !ok {
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

func NewRetransmitLeaderNode(node node, layers Layers, assignment Assignment) *RetransmitLeaderNode {
	leaderBase := newLeaderNodeBase(node, layers, assignment)

	// initialize each value of layerOwners
	layerOwners := make(map[LayerID]NodeIDs, len(layers))
	for layerID := range layers {
		layerOwners[layerID] = make(NodeIDs)
	}

	retransmitLeader := &RetransmitLeaderNode{
		LeaderNode:  leaderBase,
		layerOwners: layerOwners,
	}

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
				err := rLeader.sendLayer(nodeID, layerID, layer)
				if err != nil {
					log.Error().Err(err).Msgf("couldn't send a layer %v", layerID)
				}
			}
		}
	}
}

// sendRetransmit sends retransmit msg that asks the owner of a layer to the new destination
func (rLeader *RetransmitLeaderNode) sendRetransmit(layerID LayerID, owner NodeID, dest NodeID) error {
	log.Debug().Msgf("sending retransmit %v", layerID)
	// specifies the layer ID and the new dest.
	transmitMsg := NewRetransmitMsg(rLeader.node.GetMyID(), layerID, dest)
	// yet the transmitMsg itself is sent to the owner, not the dest of the layer.
	err := rLeader.GetTransport().Send(owner, transmitMsg)
	return err
}

// jobStatus indicates the status of the job
type jobStatus int

const (
	Pending jobStatus = iota
	SendingDirectly
	SendingIndirectly
	SendingRetransmit
	// Done
)

type jobInfo struct {
	// node which sends the layer to the destination
	sender NodeID
	status jobStatus
}

// key: dest of the layer, val: sender and status
type jobs map[NodeID]jobInfo

// key: layer, val: map of jobs
type jobsMap map[LayerID]jobs

// senderLoadCounter counts the current load of each sender.
// key: sender, val: count
type senderLoadCounter map[NodeID]uint

// PullRetransmitLeaderNode implements pull-based transmit leader node.
type PullRetransmitLeaderNode struct {
	*RetransmitLeaderNode
	jobsMap           jobsMap
	senderLoadCounter senderLoadCounter
	// nodeCompletionStatus stores if the node satisfies the assignment
	nodeCompletionStatus map[NodeID]bool
}

func NewPullRetransmitLeaderNode(node node, layers Layers, assignment Assignment) *PullRetransmitLeaderNode {
	rLeader := NewRetransmitLeaderNode(node, layers, assignment)

	prLeader := &PullRetransmitLeaderNode{
		RetransmitLeaderNode: rLeader,
		jobsMap:              make(jobsMap),
		senderLoadCounter:    make(senderLoadCounter),
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
	curStatus[ackMsg.LayerID] = struct{}{}

	log.Debug().Str("status", fmt.Sprint(prLeader.status)).Msg("status")

	// checks if the assignment is completed for the first time
	if !prLeader.nodeCompletionStatus[ackMsg.SrcID] && assignmentSatisfied(prLeader.assignment, prLeader.status) {
		prLeader.nodeCompletionStatus[ackMsg.SrcID] = true
		prLeader.mu.Unlock()
		prLeader.sendStartup()
		// notify the assignment to be ready
		prLeader.readyChan <- prLeader.assignment
	} else {
		prLeader.mu.Unlock()
	}

	// delete a job (if applicable)
	delete(prLeader.jobsMap[ackMsg.LayerID], ackMsg.SrcID)

	// assign a new job
	err := prLeader.assignNewJob(ackMsg.SrcID)
	if err != nil {
		log.Error().Err(err).Msgf("failed to assign a new job after its ack %v", ackMsg.SrcID)
	}
}

// This time, the leader sends only the layers no other node has. Otherwise, it asks for retransmission.
func (prLeader *PullRetransmitLeaderNode) sendLayers() {
	prLeader.mu.Lock()
	a := prLeader.assignment

	// add entries to layerOwners based on status map
	for nodeID, layerIDs := range prLeader.status {
		for layerID := range layerIDs {
			owners, ok := prLeader.layerOwners[layerID]
			if !ok {
				log.Error().Msgf("layerOwners is not initialized for the key %v", layerID)
			}
			owners[nodeID] = struct{}{}
			prLeader.layerOwners[layerID] = owners
		}
	}
	lo := prLeader.layerOwners

	for dest, layerIDs := range a {
		// compare it with the current status
		nodeStatus := prLeader.status[dest]

		for layerID := range layerIDs {
			if _, ok := nodeStatus[layerID]; !ok {
				// register a new job
				sender := dest
				status := SendingDirectly
				if owners, ok := lo[layerID]; ok && len(owners) > 0 {
					// one of the owners becomes the sender (for retransmission)
					for owner := range owners {
						sender = owner
						status = Pending
						break
					}
				}

				// create new jobs map if it doesn't exist
				if _, ok := prLeader.jobsMap[layerID]; !ok {
					prLeader.jobsMap[layerID] = make(jobs)
				}
				prLeader.jobsMap[layerID][dest] = jobInfo{sender, status}
				prLeader.senderLoadCounter[sender]++
			}
		}
	}

	prLeader.mu.Unlock()

	for node := range a {
		// assigns a new job to each node
		err := prLeader.assignNewJob(node)
		if err != nil {
			log.Error().Err(err).Msgf("failed to assign a new job to node %v", node)
		}
	}

	// send layers which are stored only by the leader directly to the dest
	for layerID, jobs := range prLeader.jobsMap {
		for dest, jobInfo := range jobs {
			if jobInfo.status == SendingDirectly {
				layer, ok := prLeader.layers[layerID]
				if !ok {
					log.Warn().Msgf("no layers found for layerID:%v", layerID)
				}
				err := prLeader.sendLayer(dest, layerID, layer)
				if err != nil {
					log.Error().Err(err).Msgf("couldn't send a layer %v", layerID)
				}
			}
		}
	}
}

// assignNewJob assigns a new job to the node.
// If the node has the layer another node needs, the leader asks the node to retransmit the layer.
// Otherwise, the leader sends a layer in the job list.
func (prLeader *PullRetransmitLeaderNode) assignNewJob(node NodeID) error {
	prLeader.mu.RLock()
	nodeStatus := prLeader.status[node]
	prLeader.mu.RUnlock()

	// find jobs regarding the layer the node has
	for layer := range nodeStatus {
		if layerJobs, ok := prLeader.jobsMap[layer]; ok && len(layerJobs) > 0 {
			for dest, jobInfo := range layerJobs {
				// if there is a still a job assigned to it, assign the job.
				if jobInfo.sender == node && (jobInfo.status == Pending || jobInfo.status == SendingIndirectly) {

					// assigns the job to the node
					err := prLeader.sendRetransmit(layer, node, dest)
					if err != nil {
						return fmt.Errorf("failed to assign a new job to node %v: %w", node, err)
					}

					prLeader.mu.Lock()
					jobInfo.status = SendingRetransmit
					prLeader.jobsMap[layer][dest] = jobInfo
					prLeader.senderLoadCounter[node]--
					prLeader.mu.Unlock()

					return nil
				}
			}
		}
	}

	// if not, then tries to steal other's job which requires the layers the node has
	for layer := range nodeStatus {
		if layerJobs, ok := prLeader.jobsMap[layer]; ok && len(layerJobs) > 0 {
			for dest, jobInfo := range layerJobs {
				// if there is a still a job assigned to it, assign the job.
				if jobInfo.status == Pending {
					// assigns the job to the node
					err := prLeader.sendRetransmit(layer, node, dest)
					if err != nil {
						return fmt.Errorf("failed to assign a new job to node %v: %w", node, err)
					}

					// as the job is stolen, we should update the jobInfo and counter
					prLeader.mu.Lock()
					// decrement the count of the original sender
					prLeader.senderLoadCounter[jobInfo.sender]--
					// update the jobInfo with node
					jobInfo.sender = node
					jobInfo.status = SendingRetransmit
					prLeader.jobsMap[layer][dest] = jobInfo
					prLeader.mu.Unlock()

					return nil
				}
			}
		}
	}

	// As there are no layers that is associated with retransmission, then attempt indirect retransmission.
	stolenLayerID, stolenLayerSrc, stolenSender, ok := prLeader.getFromMostLoaded()
	if !ok {
		log.Info().Msg("there is no job left to assign")
		return nil
	}

	// indirect retransmission
	err := prLeader.sendLayer(node, stolenLayerID, stolenLayerSrc)
	if err != nil {
		return fmt.Errorf("failed to transmit layer %v indirectly to node %v: %w", stolenLayerID, node, err)
	}
	prLeader.mu.Lock()

	prLeader.senderLoadCounter[stolenSender]--
	prLeader.senderLoadCounter[node]++

	jobInfo := prLeader.jobsMap[stolenLayerID][node]
	jobInfo.sender = node
	jobInfo.status = SendingIndirectly
	prLeader.jobsMap[stolenLayerID][stolenSender] = jobInfo

	prLeader.mu.Unlock()

	return nil
}

// getFromMostLoaded returns a job from the most loaded node, if any.
func (prLeader *PullRetransmitLeaderNode) getFromMostLoaded() (LayerID, *LayerSrc, NodeID, bool) {
	prLeader.mu.Lock()
	defer prLeader.mu.Unlock()

	var maxSender NodeID
	var maxCount uint
	for sender, count := range prLeader.senderLoadCounter {
		if count > uint(maxCount) {
			maxSender = sender
			maxCount = count
		}
	}
	if maxCount == 0 {
		log.Debug().Msg("no pending jobs left")
		return 0, &LayerSrc{}, 0, false
	}

	// gets one of jobs from maxSender
	for layerID, jobs := range prLeader.jobsMap {
		for _, jobInfo := range jobs {
			if jobInfo.sender == maxSender && jobInfo.status == Pending {
				layerSrc, ok := prLeader.layers[layerID]
				if !ok {
					log.Error().Msgf("layerSrc not found for %v", layerID)
					continue
				}

				return layerID, layerSrc, jobInfo.sender, true
			}
		}
	}

	log.Debug().Msg("no jobs found")
	return 0, &LayerSrc{}, 0, false
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
	layers    Layers
	readyChan chan struct{}
	mu        sync.RWMutex
}

func newReceiverNodeBase(node node, layers Layers) *ReceiverNode {
	return &ReceiverNode{
		node:      node,
		readyChan: make(chan struct{}),
		layers:    layers,
	}
}

func NewReceiverNode(node node, layers Layers) *ReceiverNode {
	receiverNode := newReceiverNodeBase(node, layers)

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

// handleLayerMsg stores the layer to its storage, and then sends ack to the leader.
func (receiver *ReceiverNode) handleLayerMsg(layerMsg *layerMsg) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()

	// load the layer to its memory
	layerSrc := LayerSrc{
		InmemData: layerMsg.LayerData,
		Fp:        "",
		Size:      uint(len(*layerMsg.LayerData)),
		Offset:    0,
	}

	// store layer
	receiver.layers[layerMsg.LayerID] = &layerSrc

	// send ack to leader
	ackMsg := NewAckMsg(receiver.node.GetMyID(), layerMsg.LayerID)
	err := receiver.GetTransport().Send(receiver.getLeader(), ackMsg)
	if err != nil {
		log.Error().Err(err).Msg("failed to send ackMsg")
	}
}

// handleStartupMsg tells that the layers are ready to application layer.
func (receiver *ReceiverNode) handleStartupMsg(*startupMsg) {
	receiver.readyChan <- struct{}{}
}

// Announce() announces its existence (with the layers it has) to leader.
func (receiver *ReceiverNode) Announce() error {
	receiver.mu.RLock()
	// only send keys of the map
	curLayerIDs := make(LayerIDs, len(receiver.layers))
	for k := range receiver.layers {
		curLayerIDs[k] = struct{}{}
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

func NewRetransmitReceiverNode(node node, layers Layers) *RetransmitReceiverNode {
	receiverBase := newReceiverNodeBase(node, layers)

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
			// receive layer
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
	layer := rReceiver.layers[retransmitMsg.LayerID]
	rReceiver.mu.RUnlock()

	// add the destination node to the routing table and connect to it
	rReceiver.addNode(retransmitMsg.DestID)

	// send (retransmit) layer to dest.
	layerMsg := NewLayerMsg(rReceiver.GetMyID(), retransmitMsg.LayerID, layer)
	err := rReceiver.GetTransport().Send(retransmitMsg.DestID, layerMsg)
	if err != nil {
		log.Error().Err(err).Msgf("failed to send layer to %v", retransmitMsg.DestID)
	}
	return err
}
