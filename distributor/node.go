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
	getMyID() NodeID
	// returns leader's nodeID
	getLeader() NodeID
	// returns next hop according to the routing table
	getNextHop(goal NodeID) (NodeID, error)

	// Adds a node
	addNode(goal NodeID, nextHop NodeID, remainingHops uint)
	// Updates nodeID of leader
	updateLeader(nodeID NodeID) error
}

type n struct {
	myID     NodeID
	leaderID NodeID

	// todo: accept multiple values? (for indirect loading)
	routingTable map[NodeID]routingInfo

	mu sync.RWMutex
}

// Creates a new node.
func NewNode(myID NodeID, leaderID NodeID) *n {
	newNode := &n{
		myID:         myID,
		leaderID:     leaderID,
		routingTable: make(map[NodeID]routingInfo),
	}

	// add leader
	newNode.addNode(leaderID, leaderID, 1)

	return newNode
}

func (n *n) getMyID() NodeID {
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
		// todo: return a non-zero value?
		return 0, fmt.Errorf("routing entry for the specified the goal does not exist")
	}

	return info.nextHop, nil
}

// adds node
func (n *n) addNode(goal NodeID, nextHop NodeID, remainingHops uint) {
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

type NodeID uint
type LayerID uint

// set of LayerIDs
type LayerIDs map[LayerID]struct{}

type Layers map[LayerID]*layer

type routingInfo struct {
	nextHop       NodeID
	remainingHops uint
}

// key: node, value: layers
type Assignment map[NodeID]LayerIDs

type status map[NodeID]LayerIDs

// content of layer
type layer []byte

func (l LayerIDs) String() string {
	layerIds := make([]LayerID, 0, len(l))
	for id := range l {
		layerIds = append(layerIds, id)
	}
	return fmt.Sprint(layerIds)
}

// leader
type leader interface {
	// todo: Receives a new assignment and remember it. Return nil if the assignment is successfully registered.
	// update(a assignment) error

	// todo
	// crash(n node)

	// Notifies that the assignment is Ready
	Ready() <-chan Assignment // todo: maybe error should be sent when assignment was interrupted?
}

type LeaderNode struct {
	node
	t         Transport
	layers    Layers
	a         Assignment
	s         status
	readyChan chan Assignment
	mu        sync.RWMutex
}

func NewLeaderNode(node node, t Transport, layers Layers, a Assignment) *LeaderNode {
	leaderNode := &LeaderNode{
		node:      node,
		t:         t,
		layers:    layers,
		a:         a,
		s:         make(status),
		readyChan: make(chan Assignment),
	}

	leaderNode.handleIncomingMsg()

	return leaderNode
}

// handle msg
func (leader *LeaderNode) handleIncomingMsg() {
	go func() {
		for incomingMsg := range leader.t.Deliver() {
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
	leader.node.addNode(announceMsg.src, announceMsg.src, 1)

	src := announceMsg.src
	for layerID := range announceMsg.layerIDs {
		layer := leader.layers[layerID]
		err := leader.sendLayer(src, &layerID, layer)
		if err != nil {
			log.Error().Err(err).Msgf("couldn't send a layer %v", layerID)
		}
	}
}

// func (leader *LeaderNode) addNode(goal NodeID, nextHop NodeID, remainingHops uint) error {
// 	// adds it to node
// 	leader.node.addNode(goal, nextHop, remainingHops)

// 	return nil
// }

func (leader *LeaderNode) sendLayer(dest NodeID, layerID *LayerID, layer *layer) error {
	layerMsg := NewLayerMsg(leader.node.getMyID(), *layerID, *layer)
	err := leader.t.Send(fmt.Sprint(dest), layerMsg)
	return err
}

// marks the delivery of ackMsg.layer to be done.
func (leader *LeaderNode) handleAckMsg(ackMsg *ackMsg) {
	leader.mu.Lock()

	curStatus := leader.s[ackMsg.src]
	// add the layer to current status
	curStatus[ackMsg.layerID] = struct{}{}

	// checks if the assignment is completed
	if reflect.DeepEqual(leader.s, leader.a) {
		leader.mu.Unlock()
		// notify the assignment to be ready
		leader.readyChan <- leader.a
		return
	}

	leader.mu.Unlock()
}

func (leader *LeaderNode) Ready() <-chan Assignment {
	return leader.readyChan
}

// receiver
type receiver interface {
	// announces its existence (with the layers it has) to leader
	Announce() error
}

type ReceiverNode struct {
	node
	t      Transport
	layers Layers
	mu     sync.RWMutex
}

func NewReceiverNode(node node, t Transport, layers Layers) *ReceiverNode {
	receiverNode := &ReceiverNode{
		node:   node,
		t:      t,
		layers: layers,
	}

	receiverNode.handleIncomingMsg()

	return receiverNode
}

// handle msg
func (receiver *ReceiverNode) handleIncomingMsg() {
	go func() {
		for incomingMsg := range receiver.t.Deliver() {
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
	receiver.layers[layerMsg.layerID] = &layerMsg.layer

	// send ack
	ackMsg := NewAckMsg(receiver.node.getMyID(), layerMsg.layerID)
	err := receiver.t.Send(layerMsg.Src(), ackMsg)
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

	announceMsg := NewAnnounceMsg(receiver.node.getMyID(), curLayerIDs)

	// todo: conversion of a nodeID to addr
	err = receiver.t.Send(fmt.Sprint(nextHop), announceMsg)
	return err
}
