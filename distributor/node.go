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
	getMyID() nodeID
	// returns leader's nodeID
	getLeader() nodeID
	// returns next hop according to the routing table
	getNextHop(goal nodeID) (nodeID, error)

	// Adds a node
	addNode(goal nodeID, nextHop nodeID, remainingHops uint)
	// Updates nodeID of leader
	updateLeader(nodeID nodeID) error
}

type n struct {
	myID     nodeID
	leaderID nodeID

	// todo: accept multiple values? (for indirect loading)
	routingTable map[nodeID]routingInfo

	mu sync.RWMutex
}

// Creates a new node.
func NewNode(myID nodeID, leaderID nodeID) *n {
	return &n{
		myID:         myID,
		leaderID:     leaderID,
		routingTable: make(map[nodeID]routingInfo),
	}
}

func (n *n) getMyID() nodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.myID
}

// returns leader's nodeID
func (n *n) getLeader() nodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.leaderID
}

func (n *n) getNextHop(goalID nodeID) (nodeID, error) {
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
func (n *n) addNode(goal nodeID, nextHop nodeID, remainingHops uint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.routingTable[goal] = routingInfo{
		nextHop:       nextHop,
		remainingHops: remainingHops,
	}
}

func (n *n) updateLeader(leaderID nodeID) error {
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

type nodeID uint
type layerID uint

// set of layerIDs
type layerIDs map[layerID]struct{}

type layers map[layerID]*layer

type routingInfo struct {
	nextHop       nodeID
	remainingHops uint
}

// key: node, value: layers
type assignment map[nodeID]layerIDs

type status map[nodeID]layerIDs

// content of layer
type layer []byte

func (l layerIDs) String() string {
	layerIds := make([]layerID, 0, len(l))
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

	// Notifies that the assignment is ready
	ready(a assignment) <-chan assignment // todo: maybe error should be sent when assignment was interrupted?
}

type leaderNode struct {
	node
	t         Transport
	layers    layers
	a         assignment
	s         status
	readyChan chan assignment
	mu        sync.RWMutex
}

func newLeaderNode(node node, t Transport, layers layers, a assignment) *leaderNode {
	leaderNode := &leaderNode{
		node:      node,
		t:         t,
		layers:    layers,
		a:         a,
		s:         make(status),
		readyChan: make(chan assignment),
	}

	leaderNode.handleIncomingMsg()

	return leaderNode
}

// handle msg
func (leader *leaderNode) handleIncomingMsg() {
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
func (leader *leaderNode) handleAnnounceMsg(announceMsg *announceMsg) {
	err := leader.registerPeer()
	if err != nil {
		log.Error().Err(err).Msg("couldn't register a peer")
	}

	src := announceMsg.src
	for layerID := range announceMsg.layerIDs {
		layer := leader.layers[layerID]
		err := leader.sendLayer(src, &layerID, layer)
		if err != nil {
			log.Error().Err(err).Msgf("couldn't send a layer %v", layerID)
		}
	}
}

func (leader *leaderNode) registerPeer() error {
	// todo
	return nil
}

func (leader *leaderNode) sendLayer(dest nodeID, layerID *layerID, layer *layer) error {
	layerMsg := NewLayerMsg(leader.node.getMyID(), *layerID, *layer)
	err := leader.t.Send(fmt.Sprint(dest), layerMsg)
	return err
}

// marks the delivery of ackMsg.layer to be done.
func (leader *leaderNode) handleAckMsg(ackMsg *ackMsg) {
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

func (leader *leaderNode) ready(a assignment) <-chan assignment {
	return leader.readyChan
}

// receiver
type receiver interface {
	// announces its existence (with the layers it has) to leader
	announce() error
}

type receiverNode struct {
	node
	t      Transport
	layers layers
	mu     sync.RWMutex
}

func newReceiverNode(t Transport, node node, layers layers) *receiverNode {
	receiverNode := &receiverNode{
		node:   node,
		t:      t,
		layers: layers,
	}

	receiverNode.handleIncomingMsg()

	return receiverNode
}

// handle msg
func (receiver *receiverNode) handleIncomingMsg() {
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
func (receiver *receiverNode) handleLayerMsg(layerMsg *layerMsg) {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()

	// todo
	// - store layer
	receiver.layers[layerMsg.layerID] = &layerMsg.layer

	// - send ack
	ackMsg := NewAckMsg(receiver.node.getMyID(), layerMsg.layerID)
	receiver.t.Send(layerMsg.Src(), ackMsg)
}

// announce() announces its existence (with the layers it has) to leader.
func (receiver *receiverNode) announce() error {
	receiver.mu.RLock()
	// only send keys of the map
	curLayerIDs := make(layerIDs, len(receiver.layers))
	for k := range receiver.layers {
		curLayerIDs[k] = struct{}{}
	}
	receiver.mu.RUnlock()

	nextHop, err := receiver.node.getNextHop(receiver.getLeader())
	if err != nil {
		log.Error().Msg("leader not found in routing table")
		return err
	}

	announceMsg := &announceMsg{
		src:      receiver.node.getMyID(),
		layerIDs: curLayerIDs,
	}

	// todo: conversion of a nodeID to addr
	err = receiver.t.Send(fmt.Sprint(nextHop), announceMsg)
	return err
}
