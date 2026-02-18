package distributor

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/rs/zerolog/log"
)

type Transport interface {
	Connect(addr string) error
	Send(dest string, message Message) error
	Broadcast(message Message) error
	Deliver() <-chan Message
	GetAddress() string
	Close() error
}

// TCP Transport
type TcpTransport struct {
	addr            string
	ln              net.Listener
	incomingMsgChan chan Message
	conns           map[string]net.Conn
	mu              sync.RWMutex
}

func NewTcpTransport(addr string, bufSize uint) *TcpTransport {
	t := &TcpTransport{
		addr:            addr,
		incomingMsgChan: make(chan Message, bufSize),
		conns:           make(map[string]net.Conn),
	}

	// start listening
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error().Err(err).Msg("failed to start listening")
		return t
	}
	t.ln = ln

	// wait for the connection
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Error().Err(err).Msg("failed to accept a new connection")
				// listener was closed, stop the loop
				return
			}
			log.Debug().Msg("conn established")
			go t.handleIncomingMsg(conn)
		}
	}()

	return t
}

func (t *TcpTransport) handleIncomingMsg(conn net.Conn) {
	defer conn.Close()

	d := json.NewDecoder(conn)

	for {
		var m TransportMsg
		err := d.Decode(&m)
		if err != nil {
			log.Error().Err(err).Msg("failed to decode envelope")
		}

		// notifies the message to upper layer, removing transportMsg part
		msg, err := decodeMsg(m)
		if err != nil {
			log.Error().Err(err).Msg("failed to decode TransportMsg")
			return
		}

		log.Debug().Msgf("incoming msg: %s", msg.String())

		t.incomingMsgChan <- msg
	}
}

// Connect tries to connect to the node which has the address using Dial.
func (t *TcpTransport) Connect(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	//  save conn
	t.mu.Lock()
	t.conns[addr] = conn
	t.mu.Unlock()

	return nil
}

func (t *TcpTransport) Send(dest string, message Message) error {
	t.mu.RLock()
	conn, ok := t.conns[dest]
	t.mu.RUnlock()

	if !ok {
		return fmt.Errorf("conn not found for addr %v", dest)
	}
	return t.sendTransportMsg(conn, message)
}
func (t *TcpTransport) Broadcast(message Message) error {
	t.mu.RLock()
	conns := t.conns
	t.mu.RUnlock()
	for addr, conn := range conns {
		err := t.sendTransportMsg(conn, message)
		if err != nil {
			return fmt.Errorf("failed to send msg to addr %v", addr)
		}
	}

	return nil
}

func (t *TcpTransport) sendTransportMsg(conn net.Conn, message Message) error {
	// sends encoded TransportMsg
	marshaledMsg, err := json.Marshal(message)
	if err != nil {
		return err
	}

	transportMsg := TransportMsg{
		Type:    message.Type(),
		Src:     message.Src(),
		Payload: marshaledMsg,
	}

	e := json.NewEncoder(conn)
	return e.Encode(transportMsg)
}

func (t *TcpTransport) Deliver() <-chan Message {
	return t.incomingMsgChan
}
func (t *TcpTransport) GetAddress() string {
	return t.addr
}

func (t *TcpTransport) Close() error {
	if t.ln != nil {
		if err := t.ln.Close(); err != nil {
			return fmt.Errorf("failed to close listener: %w", err)
		}
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	for addr, conn := range t.conns {
		err := conn.Close()
		if err != nil {
			return fmt.Errorf("failed to close conn for addr %s", addr)
		}
	}
	return nil
}

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
func NewInmemTransport(addr string, bufSize uint) *InmemoryTransport {
	t := &InmemoryTransport{
		addr:            addr,
		incomingMsgChan: make(chan Message, bufSize),
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
	for addr, destTransport := range t.peers {
		if addr == t.addr {
			continue
		}
		destTransport.incomingMsgChan <- message
	}

	return nil
}

// returns chan to receive messages
func (t *InmemoryTransport) Deliver() <-chan Message {
	return t.incomingMsgChan
}

func (t *InmemoryTransport) GetAddress() string {
	return t.addr
}

func (t *InmemoryTransport) Close() error {
	close(t.incomingMsgChan)
	return nil
}
