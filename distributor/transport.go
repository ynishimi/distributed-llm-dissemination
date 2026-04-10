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
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

type Transport interface {
	Send(destID NodeID, message Message) error
	RegisterPipe(layerID LayerID, destID NodeID) error
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
	isClient        bool
	addrRegistry    AddrRegistry

	// pipes stores the nodes which needs transmitting a layer in the client
	pipes map[LayerID]NodeID

	mu sync.RWMutex
}

type protectedConn struct {
	conn net.Conn
	mu   sync.Mutex
}

type tempLayerInfo struct {
	SrcID     NodeID
	LayerID   LayerID
	LayerSize int64
	TotalSize int64
	Offert    int64
	// SaveDisk  bool
}

// AddrRegistry stores the mapping of NodeID and its addr.
type AddrRegistry map[NodeID]string

func NewTcpTransport(addr string, bufSize uint, addrRegistory AddrRegistry, isClient bool) (*TcpTransport, error) {
	t := &TcpTransport{
		addr:            addr,
		incomingMsgChan: make(chan Message, bufSize),
		conns:           make(map[string]*protectedConn),
		isClient:        isClient,
		addrRegistry:    addrRegistory,
		pipes:           make(map[LayerID]NodeID),
	}

	// start listening
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to start listening: %w", err)
	}
	t.ln = ln
	log.Info().Str("addr", addr).Msg("start listening")

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

		if m.Type != MsgTypeLayer {
			// notifies the message to upper layer, removing transportMsg part
			msg, err := decodeMsg(m)
			if err != nil {
				log.Error().Err(err).Msg("failed to decode TransportMsg")
				return
			}

			t.incomingMsgChan <- msg

			continue
		}

		// handles MsgTypeLayer
		// log.Debug().Msg("received LayerMsg")

		// receive layer in binary
		// loads header at first
		var temp tempLayerInfo
		var buf LayerData

		err = json.Unmarshal(m.Payload, &temp)
		if err != nil {
			log.Error().Err(err).Msg("failed to decode TransportMsg(MsgTypeLayer)")
			return
		}

		log.Info().Int("layerID", int(temp.LayerID)).Int("layer_size", int(temp.LayerSize)).Int("total_size", int(temp.TotalSize)).Msg("start receiving layer")
		t0 := time.Now()

		// if pipe exists, the layer should be piped at the same time
		if pDestConn, ok := t.getAndUnregisterPipe(temp.LayerID); ok {
			log.Debug().Msg("got pipe")

			pDestConn.mu.Lock()

			destConn := pDestConn.conn

			// todo: srcID should be modified accordingly; currently it is the original sender of the layer
			pipeTemp := temp

			// sends header first

			marshaledHdr, err := json.Marshal(pipeTemp)
			if err != nil {
				log.Error().Err(err).Msgf("failed to pipe the layer %v", pipeTemp.LayerID)
			}
			transportMsg := TransportMsg{
				Type: m.Type,
				// todo: src should be modified accordingly; currently it is the original sender of the layer
				Src:     m.Src,
				Payload: marshaledHdr,
			}
			marshaledTransportHdr, err := json.Marshal(transportMsg)
			if err != nil {
				log.Error().Err(err).Send()
				pDestConn.mu.Unlock()
				return
			}

			_, err = destConn.Write(marshaledTransportHdr)
			log.Debug().Msg("header written to dest")
			if err != nil {
				log.Error().Err(err).Send()
				pDestConn.mu.Unlock()
				return
			}

			// then receive from conn & pipe the layer into destConn at the same time
			buf = make(LayerData, pipeTemp.LayerSize)
			log.Debug().Int64("size", pipeTemp.LayerSize).Msg("starting TeeReader ReadFull")
			tee := io.TeeReader(io.MultiReader(d.Buffered(), conn), destConn)
			_, err = io.ReadFull(tee, buf)
			log.Debug().Msg("TeeReader ReadFull completed")
			log.Debug().Msg("ReadFull completed")
			if err != nil {
				log.Error().Err(err).Msg("failed to read layer")
				pDestConn.mu.Unlock()
				return
			}

			pDestConn.mu.Unlock()

		} else {
			log.Debug().Msg("no pipe found")
			// then loads layer

			buf = make(LayerData, temp.LayerSize)
			reader := io.MultiReader(d.Buffered(), conn)
			_, err = io.ReadFull(reader, buf)
			if err != nil {
				log.Error().Err(err).Msg("failed to read layer")
				return
			}
		}

		// moves the decoder
		d = json.NewDecoder(conn)

		t1 := time.Since(t0)
		log.Info().
			Int("layerID", int(temp.LayerID)).
			Int("layer_size", int(temp.LayerSize)).
			Int("total_size", int(temp.TotalSize)).
			Dur("duration[ms]", t1).
			Msg("(a franction of) layer received")
		// fixme: all the fraction of the layers are once saved in the buf and then copied again
		layerSrc := LayerSrc{&buf, "", int64(len(buf)), 0, LayerMeta{Location: InmemLayer}}
		t.incomingMsgChan <- &layerMsg{temp.SrcID, temp.LayerID, layerSrc, temp.TotalSize}

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
		// log.Debug().Msg("skip dialing to myself")
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

	// for layerMsg, creates a new connection (to make use of parallelism)
	switch m := message.(type) {
	case *layerMsg, *blockMsg:
		conn, err := net.Dial("tcp", dest)
		if err != nil {
			return err
		}
		defer conn.Close()
		return t.sendLayerMsg(conn, m)
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

func (t *TcpTransport) sendLayerMsg(conn net.Conn, layerMsg *layerMsg) error {

	// sends header and layer separately for avoiding unnecesary memory occupation due to decoding
	header := tempLayerInfo{layerMsg.SrcID, layerMsg.LayerID, layerMsg.LayerSrc.DataSize, layerMsg.TotalSize, layerMsg.LayerSrc.Offset}

	// sends header first
	marshaledHdr, err := json.Marshal(header)
	if err != nil {
		return err
	}
	transportMsg := TransportMsg{
		Type:    layerMsg.Type(),
		Src:     layerMsg.Src(),
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

	if inmemData := layerMsg.LayerSrc.InmemData; inmemData != nil && layerMsg.LayerSrc.Meta.Location == InmemLayer {
		// sends layerData directly
		if t.isClient {
			err := t.writeWithLimit(layerMsg.LayerSrc.Meta.LimitRate, layerMsg.LayerID, *inmemData, conn)
			if err != nil {
				return err
			}
		} else {
			// the node sends a layer to its dest
			partialData := (*layerMsg.LayerSrc.InmemData)[layerMsg.LayerSrc.Offset : layerMsg.LayerSrc.Offset+layerMsg.LayerSrc.DataSize]

			// _, err = conn.Write(partialData)
			err := t.writeWithLimit(layerMsg.LayerSrc.Meta.LimitRate, layerMsg.LayerID, partialData, conn)

			if err != nil {
				return err
			}
		}
	} else if layerMsg.LayerSrc.Meta.Location == DiskLayer {
		if layerMsg.LayerSrc.Fp == "" {
			return fmt.Errorf("no data source specified")
		}

		// the layer is in disk
		f, err := os.Open(layerMsg.LayerSrc.Fp)
		if err != nil {
			return err
		}
		defer f.Close()

		sr := io.NewSectionReader(f, layerMsg.LayerSrc.Offset, layerMsg.LayerSrc.DataSize)

		// directly send file from disk, using sendFile syscall
		_, err = io.Copy(conn, sr)
		return err
	} else {
		return fmt.Errorf("unknown error sending layer %v", layerMsg.LayerID)
	}

	return nil
}

func (t *TcpTransport) sendTransportMsg(pConn *protectedConn, message Message) error {
	pConn.mu.Lock()
	defer pConn.mu.Unlock()

	conn := pConn.conn

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

func (t *TcpTransport) writeWithLimit(limitRate int64, layerID LayerID, data LayerData, conn net.Conn) error {
	const BucketSize = 256 * 1024
	limiter := rate.NewLimiter(rate.Limit(limitRate), BucketSize)
	// limit speed
	log.Debug().Uint("layerID", uint(layerID)).Msgf("sending with limit: %v MiB/s", int64(limiter.Limit())>>20)
	for len(data) > 0 {
		n := min(len(data), limiter.Burst())
		limiter.WaitN(context.Background(), n)
		_, err := conn.Write(data[:n])
		if err != nil {
			return err
		}
		data = data[n:]
	}
	log.Debug().Uint("layerID", uint(layerID)).Msg("completed")

	return nil
}

// RegisterPipe registers a node which requires a layer in the client.
func (t *TcpTransport) RegisterPipe(layerID LayerID, destID NodeID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.pipes[layerID]; !ok {
		t.pipes[layerID] = destID
		return nil
	} else {
		return fmt.Errorf("pipe already registered")
	}
}

func (t *TcpTransport) getAndUnregisterPipe(layerID LayerID) (*protectedConn, bool) {
	t.mu.Lock()

	destID, ok := t.pipes[layerID]
	if !ok {
		log.Debug().Msgf("pipe not exist for layer %v", layerID)
		t.mu.Unlock()
		return nil, false
	}
	delete(t.pipes, layerID)

	dest, ok := t.addrRegistry[destID]
	if !ok {
		log.Error().Msgf("addr of %d does not exist", destID)
		t.mu.Unlock()
		return nil, false
	}

	t.mu.Unlock()

	conn, err := t.getOrConnect(dest)
	if err != nil {
		log.Error().Err(err).Send()
		return nil, false
	}

	return conn, true
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
	addrRegistry    AddrRegistry
	isClient        bool

	// pipes stores the nodes which needs transmitting a layer in the client
	pipes map[LayerID]NodeID

	mu sync.RWMutex
}

var (
	// global registry (addr (name of the node this time) - instance of the transport of node)
	inmemRegistry   = make(map[string]*InmemoryTransport)
	inmemRegistryMu sync.Mutex
)

// creates an instance of InmemoryTransport
func NewInmemTransport(addr string, bufSize uint, addrRegistry AddrRegistry, isClient bool) *InmemoryTransport {
	t := &InmemoryTransport{
		addr:            addr,
		incomingMsgChan: make(chan Message, bufSize),
		peers:           make(map[string]*InmemoryTransport),
		addrRegistry:    addrRegistry,
		isClient:        isClient,
		pipes:           make(map[LayerID]NodeID),
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
	t.mu.RLock()
	dest, ok := t.addrRegistry[destID]
	t.mu.RUnlock()

	if !ok {
		dest = fmt.Sprint(destID)
	}

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

// RegisterPipe registers a node which requires a layer in the client.
func (t *InmemoryTransport) RegisterPipe(layerID LayerID, destID NodeID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.pipes[layerID]; !ok {
		t.pipes[layerID] = destID
		return nil
	} else {
		return fmt.Errorf("pipe already registered")
	}
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
