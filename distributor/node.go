package distributor

import (
	"fmt"
	"reflect"
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

type n struct {
	myID     NodeID
	leaderID NodeID
	t        Transport

	// todo: accept multiple values? (for indirect loading)
	routingTable map[NodeID]routingInfo

	mu sync.RWMutex
}

// Creates a new node.
func NewNode(myID NodeID, leaderID NodeID, t Transport) *n {
	newNode := &n{
		myID:         myID,
		leaderID:     leaderID,
		t:            t,
		routingTable: make(map[NodeID]routingInfo),
	}

	// add myself
	newNode.addNode(myID)
	// add leader
	newNode.addNode(leaderID)

	return newNode
}

func (n *n) GetMyID() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.myID
}

// returns leader's nodeID
func (n *n) getLeader() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.leaderID
}

func (n *n) getNextHop(goalID NodeID) (NodeID, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	info, ok := n.routingTable[goalID]
	if !ok {
		return 0, fmt.Errorf("routing entry for the specified the goal does not exist")
	}

	return info.nextHop, nil
}

// adds node
func (n *n) addNode(goal NodeID) {
	// add it to routing table
	n.addRoutingTable(goal, goal, 1)

	// tell it to the transport layer
	log.Debug().Msgf("%v: connecting to %v", n.myID, goal)
	if err := n.GetTransport().Connect(goal); err != nil {
		log.Debug().Err(err).Msgf("failed to connect to %v", goal)
	}
}

// adds node that is not directly connected to itself
func (n *n) addRoutingTable(goal NodeID, nextHop NodeID, remainingHops uint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.routingTable[goal] = routingInfo{
		nextHop:       nextHop,
		remainingHops: remainingHops,
	}
}

func (n *n) updateLeader(leaderID NodeID) error {
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

func (n *n) GetTransport() Transport {
	return n.t
}

type NodeID uint
type LayerID uint

// set of NodeIDs
type NodeIDs map[NodeID]struct{}

// set of LayerIDs
type LayerIDs map[LayerID]struct{}

type Layers map[LayerID]*Layer

type routingInfo struct {
	nextHop       NodeID
	remainingHops uint
}

// key: node, value: layers
type Assignment map[NodeID]LayerIDs

type status map[NodeID]LayerIDs

// content of Layer
type Layer []byte

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
		leader.status[announceMsg.SrcID] = make(LayerIDs)
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

func (leader *LeaderNode) sendLayer(dest NodeID, layerID LayerID, layer *Layer) error {
	log.Debug().Msgf("sending layer %v", layerID)
	layerMsg := NewLayerMsg(leader.node.GetMyID(), layerID, *layer)
	err := leader.GetTransport().Send(dest, layerMsg)
	return err
}

// marks the delivery of ackMsg.layer to be done.
func (leader *LeaderNode) handleAckMsg(ackMsg *ackMsg) {
	leader.mu.Lock()

	curStatus := leader.status[ackMsg.SrcID]
	// add the layer to current status
	curStatus[ackMsg.LayerID] = struct{}{}

	// checks if the assignment is completed
	if reflect.DeepEqual(leader.status, status(leader.assignment)) {
		leader.mu.Unlock()
		// notify the assignment to be ready
		leader.readyChan <- leader.assignment
		return
	}

	leader.mu.Unlock()
}

func (leader *LeaderNode) StartDistribution() <-chan Assignment {
	return leader.startDistributionChan
}

func (leader *LeaderNode) Ready() <-chan Assignment {
	return leader.readyChan
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

// Receiver
type Receiver interface {
	// announces its existence (with the layers it has) to leader
	Announce() error
}

type ReceiverNode struct {
	node
	layers Layers
	mu     sync.RWMutex
}

func newReceiverNodeBase(node node, layers Layers) *ReceiverNode {
	return &ReceiverNode{
		node:   node,
		layers: layers,
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
				// todo: start the inference engine
				// case *startupMsg:
				// 	go receiver.handleStartupMsg(v)
			}
		}
	}()
}

// handleLayerMsg stores the layer to its storage, and then sends ack to the leader.
func (receiver *ReceiverNode) handleLayerMsg(layerMsg *layerMsg) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()

	// store layer
	receiver.layers[layerMsg.LayerID] = &layerMsg.LayerData

	// send ack
	ackMsg := NewAckMsg(receiver.node.GetMyID(), layerMsg.LayerID)
	err := receiver.GetTransport().Send(layerMsg.SrcID, ackMsg)
	if err != nil {
		log.Error().Err(err).Msg("failed to send ackMsg")
	}
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

	// todo: conversion of a nodeID to addr
	err = receiver.GetTransport().Send(nextHop, announceMsg)
	return err
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
				// todo: start the inference engine
				// case *startupMsg:
				// 	go receiver.handleStartupMsg(v)
			}
		}
	}()
}

// handleRetransmitMsg sends specified layer to the destination.
func (rReceiver *RetransmitReceiverNode) handleRetransmitMsg(retransmitMsg *retransmitMsg) error {
	rReceiver.mu.RLock()
	layer := rReceiver.layers[retransmitMsg.LayerID]
	rReceiver.mu.RUnlock()

	// send layer to dest.
	// todo: should the receiver set its SrcID?
	layerMsg := NewLayerMsg(retransmitMsg.SrcID, retransmitMsg.LayerID, *layer)
	return rReceiver.GetTransport().Send(retransmitMsg.DestID, layerMsg)
}
