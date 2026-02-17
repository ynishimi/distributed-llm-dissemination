package distributor

import (
	"fmt"
	"sync"
)

type Transport interface {
	Connect(addr string) error
	Send(dest string, message Message) error
	Broadcast(message Message) error
	Deliver() <-chan Message
	GetAddress() string
}

// todo: Implementation of TCP Transport.
type TcpTransport struct{}

func NewTcpTransport() *TcpTransport {
	return &TcpTransport{}
}

// todo: Connect, Send, Broadcast, Deliver, GetAddress

// Implementation of Inmemory Transport (only for testing purpose. Copied from https://github.com/ynishimi/paxos-tob)
type InmemoryTransport struct {
	addr            string
	incomingMsgChan chan Message
	peers           map[string]*InmemoryTransport
	mu              sync.RWMutex
}

var (
	// global registry (addr (name of the node this time) - instance of the transport of node)
	inmemRegistry   = make(map[string]*InmemoryTransport)
	inmemRegistryMu sync.Mutex
)

// creates an instance of InmemoryTransport
func NewInmemTransport(addr string) *InmemoryTransport {
	t := &InmemoryTransport{
		addr:            addr,
		incomingMsgChan: make(chan Message, 10),
		peers:           make(map[string]*InmemoryTransport),
	}
	t.peers[addr] = t

	// adds it to the global register as well
	inmemRegistryMu.Lock()
	inmemRegistry[addr] = t
	inmemRegistryMu.Unlock()

	return t
}

// Connect etablishes the connection.
func (t *InmemoryTransport) Connect(addr string) error {
	// search the instance of the addr using the global registry
	inmemRegistryMu.Lock()
	peer, ok := inmemRegistry[addr]
	inmemRegistryMu.Unlock()

	if !ok {
		return fmt.Errorf("peer %s not found", addr)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.peers[addr] = peer

	return nil
}

func (t *InmemoryTransport) AddPeer(newPeer *InmemoryTransport) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.peers[newPeer.addr] = newPeer
}

func (t *InmemoryTransport) Send(dest string, message Message) error {
	// send message to a dest's incomingMessage channel
	t.mu.RLock()
	destTransport, ok := t.peers[dest]
	t.mu.RUnlock()

	if !ok {
		return fmt.Errorf("peer not found")
	}

	destTransport.incomingMsgChan <- message
	return nil
}

func (t *InmemoryTransport) Broadcast(message Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// assumption: all nodes are in the map
	for _, destTransport := range t.peers {
		destTransport.incomingMsgChan <- message
	}

	// send to yourself
	t.incomingMsgChan <- message

	return nil
}

// returns chan to receive messages
func (t *InmemoryTransport) Deliver() <-chan Message {
	return t.incomingMsgChan
}

func (t *InmemoryTransport) GetAddress() string {
	return t.addr
}
