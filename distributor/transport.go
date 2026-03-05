package distributor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

type Transport interface {
	Send(destID NodeID, message Message) error
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
	conns           map[string]*protectedConn
	limiter         *rate.Limiter
	addrRegistry    AddrRegistry

	mu sync.RWMutex
}

type protectedConn struct {
	conn net.Conn
	mu   sync.Mutex
}

type tempLayerInfo struct {
	SrcID     NodeID
	LayerID   LayerID
	LayerSize int
	// SaveDisk  bool
}

// AddrRegistry stores the mapping of NodeID and its addr.
type AddrRegistry map[NodeID]string

func NewTcpTransport(addr string, bufSize uint, addrRegistory AddrRegistry, limitRate int) (*TcpTransport, error) {
	t := &TcpTransport{
		addr:            addr,
		incomingMsgChan: make(chan Message, bufSize),
		conns:           make(map[string]*protectedConn),
		limiter:         nil,
		addrRegistry:    addrRegistory,
	}

	if limitRate != 0 {
		t.limiter = rate.NewLimiter(rate.Limit(limitRate), limitRate)
	}

	// start listening
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to start listening: %w", err)
	}
	t.ln = ln
	log.Debug().Str("addr", addr).Msg("start listening")

	// wait for the connection
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					log.Debug().Msg("listener closed")
				} else {
					log.Error().Err(err).Msg("failed to accept a new connection")
				}
				return
			}
			log.Debug().Msg("conn established")
			go t.handleIncomingMsg(conn)
		}
	}()

	return t, nil
}

func (t *TcpTransport) handleIncomingMsg(conn net.Conn) {
	defer conn.Close()

	d := json.NewDecoder(conn)
	for {
		var m TransportMsg
		err := d.Decode(&m)
		if err != nil {
			// when the connection is closed by its counterpart, or the connection is already closed
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				log.Debug().Msg("connection closed")
			} else {
				log.Error().Err(err).Msg("failed to decode envelope")
			}
			return
		}

		if m.Type == MsgTypeLayer {
			// receive layer in binary
			// loads header at first
			var temp tempLayerInfo
			err := json.Unmarshal(m.Payload, &temp)
			if err != nil {
				log.Error().Err(err).Msg("failed to decode TransportMsg(MsgTypeLayer)")
				return
			}

			// then loads layer
			data := make(LayerData, temp.LayerSize)
			_, err = io.ReadFull(io.MultiReader(d.Buffered(), conn), data)
			if err != nil {
				log.Error().Err(err).Msg("failed to read layer")
				return
			}

			// moves the decoder
			d = json.NewDecoder(conn)

			// fixme: currently, always loads the layer to memory.
			layerSrc := LayerSrc{&data, "", len(data), 0, InmemLayer}

			t.incomingMsgChan <- &layerMsg{temp.SrcID, temp.LayerID, layerSrc}
			continue
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

// getOrConnect tries to connect to the node which has the address using Dial, if there is no connection.
func (t *TcpTransport) getOrConnect(destAddr string) (*protectedConn, error) {
	t.mu.RLock()
	curPConn, ok := t.conns[destAddr]
	t.mu.RUnlock()

	if ok && curPConn != nil {
		log.Debug().Str("addr", destAddr).Msg("conn already established")
		return curPConn, nil
	}

	if destAddr == t.addr {
		log.Debug().Msg("skip dialing to myself")
		return nil, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	newConn, err := net.Dial("tcp", destAddr)
	if err != nil {
		return nil, err
	}

	//  save conn
	newPConn := &protectedConn{conn: newConn}
	t.conns[destAddr] = newPConn

	return newPConn, nil
}

func (t *TcpTransport) Send(destID NodeID, message Message) error {
	t.mu.RLock()
	dest, ok := t.addrRegistry[destID]
	t.mu.RUnlock()

	if !ok {
		return fmt.Errorf("addr of %d does not exist", destID)
	}

	conn, err := t.getOrConnect(dest)
	if err != nil {
		return fmt.Errorf("failed to connect to %v: %w", dest, err)
	}

	if conn == nil {
		// it's myself
		t.incomingMsgChan <- message
		return nil
	}

	return t.sendTransportMsg(conn, message)
}
func (t *TcpTransport) Broadcast(message Message) error {
	t.mu.RLock()
	ids := make([]NodeID, 0, len(t.addrRegistry))
	for id := range t.addrRegistry {
		ids = append(ids, id)
	}
	t.mu.RUnlock()

	for _, id := range ids {
		err := t.Send(id, message)
		if err != nil {
			log.Error().Err(err).Msgf("failed to broadcast to %v", id)
		}
	}

	return nil
}

func (t *TcpTransport) sendTransportMsg(pConn *protectedConn, message Message) error {
	pConn.mu.Lock()
	defer pConn.mu.Unlock()

	conn := pConn.conn

	if layerMsg, ok := message.(*layerMsg); ok {
		// sends header and layer separately for avoiding unnecesary memory occupation due to decoding
		header := tempLayerInfo{layerMsg.SrcID, layerMsg.LayerID, layerMsg.LayerSrc.Size}

		// sends header first
		marshaledHdr, err := json.Marshal(header)
		if err != nil {
			return err
		}
		transportMsg := TransportMsg{
			Type:    message.Type(),
			Src:     message.Src(),
			Payload: marshaledHdr,
		}
		marshaledTransportHdr, err := json.Marshal(transportMsg)
		if err != nil {
			return err
		}

		_, err = conn.Write(marshaledTransportHdr)
		if err != nil {
			return err
		}

		if inmemData := layerMsg.LayerSrc.InmemData; inmemData != nil && layerMsg.LayerSrc.LayerLocation == InmemLayer {
			// sends layerData directly
			if t.limiter != nil {
				// limit speed
				log.Debug().Uint("layerID", uint(layerMsg.LayerID)).Msg("sending with limit")
				data := *inmemData
				for len(data) > 0 {
					n := min(len(data), t.limiter.Burst())
					t.limiter.WaitN(context.Background(), n)
					_, err = conn.Write(data[:n])
					if err != nil {
						return err
					}
					data = data[n:]
				}
				log.Debug().Uint("layerID", uint(layerMsg.LayerID)).Msg("completed")
			} else {
				_, err = conn.Write(*inmemData)
				if err != nil {
					return err
				}
			}
		} else if layerMsg.LayerSrc.LayerLocation == DiskLayer {
			if layerMsg.LayerSrc.Fp == "" {
				return fmt.Errorf("no data source specified")
			}

			// the layer is in disk
			f, err := os.Open(layerMsg.LayerSrc.Fp)
			if err != nil {
				return err
			}
			defer f.Close()

			// directly send file from disk, using sendFile syscall
			_, err = io.Copy(conn, f)
			return err
		}

		return nil
	} else {
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

		marshaledTransportMsg, err := json.Marshal(transportMsg)
		if err != nil {
			return err
		}

		_, err = conn.Write(marshaledTransportMsg)
		if err != nil {
			return err
		}

		return nil
	}
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

	for addr, pConn := range t.conns {
		err := pConn.conn.Close()
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

func (t *InmemoryTransport) AddPeer(newPeer *InmemoryTransport) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.peers[newPeer.addr] = newPeer
}

func (t *InmemoryTransport) Send(destID NodeID, message Message) error {
	// send message to a dest's incomingMessage channel
	dest := fmt.Sprint(destID)
	t.mu.RLock()
	destTransport, ok := t.peers[dest]
	t.mu.RUnlock()

	if !ok {
		// try to find from global registry
		inmemRegistryMu.Lock()
		peer, ok := inmemRegistry[dest]
		inmemRegistryMu.Unlock()

		if !ok {
			return fmt.Errorf("peer %s not found", dest)
		}

		t.mu.Lock()
		t.peers[dest] = peer
		destTransport = peer
		t.mu.Unlock()
	}

	destTransport.incomingMsgChan <- message
	return nil
}

func (t *InmemoryTransport) Broadcast(message Message) error {
	inmemRegistryMu.Lock()
	addrs := make([]string, 0, len(inmemRegistry))
	for addr := range inmemRegistry {
		addrs = append(addrs, addr)
	}
	inmemRegistryMu.Unlock()

	for _, addr := range addrs {
		if addr == t.addr {
			continue
		}
		var id uint
		_, err := fmt.Sscanf(addr, "%d", &id)
		if err != nil {
			// fallback if it's not a number
			t.mu.RLock()
			peer, ok := t.peers[addr]
			t.mu.RUnlock()
			if ok {
				peer.incomingMsgChan <- message
			}
			continue
		}
		t.Send(NodeID(id), message)
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
	inmemRegistryMu.Lock()
	delete(inmemRegistry, t.addr)
	inmemRegistryMu.Unlock()
	close(t.incomingMsgChan)
	return nil
}

// // transport with rate limiter
// type RateLimitedTransport struct {
// 	t       Transport
// 	limiter *rate.Limiter
// }

// func NewRateLimitedTransport(t Transport, bytePerSecond int) *RateLimitedTransport {
// 	return &RateLimitedTransport{t, rate.NewLimiter(rate.Limit(bytePerSecond), bytePerSecond)}
// }

// func (t *RateLimitedTransport) Send(destID NodeID, message Message) error {

// }
