package distributor

import (
	"fmt"
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

// set of NodeIDMap
type NodeIDMap map[NodeID]struct{}

type LayerMeta struct {
	Location   LayerLocation
	LimitRate  int64
	SourceType SourceType
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

// map of layers (has data in memory or has a path to the file)
type LayersSrc map[LayerID]LayerSrc

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

// for physically saved location
const (
	InmemLayer LayerLocation = iota
	DiskLayer
	ClientLayer
)

// for simulated location
type SourceType uint8

const (
	SourceClient SourceType = iota
	SourceDisk
	SourceMem
)

type LayerSrc struct {
	// InmemData is nil if layer is not in memory
	InmemData *LayerData
	// file path of the layer (in disk)
	Fp string
	// file DataSize
	DataSize int64
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
	layers     LayersSrc
	assignment Assignment
	status     status
	// startDistributionChan notifies the start of distribution.
	startDistributionChan chan Assignment
	// fetchChan             map[LayerID]chan LayerSrc
	readyChan chan Assignment
	mu        sync.RWMutex

	sendLayersFn func()
}

func newLeaderNodeBase(node node, layers LayersSrc, assignment Assignment) *LeaderNode {
	l := &LeaderNode{
		node:                  node,
		layers:                layers,
		assignment:            assignment,
		status:                make(status, len(assignment)),
		startDistributionChan: make(chan Assignment),
		// fetchChan:             make(map[LayerID]chan LayerSrc),
		readyChan: make(chan Assignment),
	}

	// only send keys of the map
	curLayerIDs := make(LayerIDs, len(l.layers))
	for k, layerSrc := range l.layers {
		curLayerIDs[k] = layerSrc.Meta
	}

	l.status[l.GetMyID()] = curLayerIDs

	l.sendLayersFn = l.sendLayers

	return l
}

func NewLeaderNode(node node, layers LayersSrc, assignment Assignment) *LeaderNode {
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
	leader.sendLayersFn()
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

func (leader *LeaderNode) sendLayer(destID NodeID, layerID LayerID, layerSrc LayerSrc) error {
	log.Debug().Msgf("sending layer %v", layerID)
	ls := layerSrc
	if ls.Meta.Location == ClientLayer {
		log.Debug().Uint("layer", uint(layerID)).Msg("loading layer from client")
		// load the layer from the client
		return leader.fetchFromClient(layerID, destID)
	}

	err := leader.GetTransport().Send(destID, NewLayerMsg(leader.node.GetMyID(), layerID, ls, ls.DataSize))
	return err
}

func (leader *LeaderNode) fetchFromClient(layerID LayerID, destID NodeID) error {
	log.Debug().Uint("layerID", uint(layerID)).Msg("ask the client to send the layer")

	leader.GetTransport().RegisterPipe(layerID, destID)

	return leader.GetTransport().Send(ClientID, NewClientReqMsg(leader.GetMyID(), layerID, false))
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
		DataSize:  int64(len(*layerMsg.LayerSrc.InmemData)),
		Offset:    0,
		Meta: LayerMeta{
			Location: InmemLayer,
		},
	}
	log.Debug().Msgf("saved layer %v in memory", layerMsg.LayerID)

	// store layer
	leader.layers[layerMsg.LayerID] = layerSrc

	// if ch, ok := leader.fetchChan[layerMsg.LayerID]; ok {
	// 	ch <- layerSrc
	// }

	// update my status
	// send ack to leader
	ackMsg := NewAckMsg(leader.node.GetMyID(), layerMsg.LayerID)
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
	curStatus[ackMsg.LayerID] = LayerMeta{}

	log.Debug().Str("status", fmt.Sprint(leader.status)).Msg("handleAckMsg")

	// checks if the assignment is completed
	if assignmentSatisfied(leader.assignment, leader.status) {
		leader.mu.Unlock()
		log.Info().Msg("timer stop: startup")
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

// Receiver
type Receiver interface {
	// announces its existence (with the layers it has) to leader
	Announce() error

	// Startup tells application layer that the layers are ready.
	Ready() <-chan struct{}
}

type ReceiverNode struct {
	node
	layers      LayersSrc
	storagePath string
	readyChan   chan struct{}
	// fetchChan   map[LayerID]chan LayerSrc
	mu sync.RWMutex
}

func newReceiverNodeBase(node node, layers LayersSrc, storagePath string) *ReceiverNode {

	return &ReceiverNode{
		node:        node,
		storagePath: storagePath,
		readyChan:   make(chan struct{}),
		// fetchChan:   make(map[LayerID]chan LayerSrc),
		layers: layers,
	}
}

func NewReceiverNode(node node, layers LayersSrc, storagePath string) *ReceiverNode {
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

// fetchFromClient fetches a layer from client
func (receiver *ReceiverNode) fetchFromClient(layerID LayerID, destID NodeID) error {
	log.Debug().Uint("layerID", uint(layerID)).Msg("ask the client to send the layer")

	receiver.GetTransport().RegisterPipe(layerID, destID)

	return receiver.GetTransport().Send(ClientID, NewClientReqMsg(receiver.GetMyID(), layerID, false))
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
		DataSize:  int64(len(*layerMsg.LayerSrc.InmemData)),
		Offset:    0,
		Meta: LayerMeta{
			Location: InmemLayer,
		},
	}
	log.Debug().Msgf("saved layer %v in memory", layerMsg.LayerID)

	// store layer
	receiver.layers[layerMsg.LayerID] = layerSrc

	// if ch, ok := receiver.fetchChan[layerMsg.LayerID]; ok {
	// 	ch <- layerSrc
	// }

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
