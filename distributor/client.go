package distributor

import (
	"context"
	"math"
	"sync"

	"golang.org/x/time/rate"
)

type Client interface {
	// FetchBlock returns a block upon a request.
	FetchBlock(blockReq blockReq) LayerSrc
}

type blockReq struct {
	layerID LayerID
	blockID BlockID
	respCh  chan LayerSrc
}

// InmemClient serves as a client in a sender, limiting the transmit rate.
type InmemClient struct {
	// reqCh waits for new requests.
	blockReqChan chan blockReq
}

// NewInmemClient creates a new InmemClient. This should be called at launch of senders.
func NewInmemClient(n node, layers LayersSrc, mu *sync.RWMutex) *InmemClient {
	c := &InmemClient{
		blockReqChan: make(chan blockReq),
	}

	go c.blockGetter(n, layers, mu)

	return c
}

// FetchBlock returns a reader of a block
func (c *InmemClient) FetchBlock(blockReq blockReq) LayerSrc {
	c.blockReqChan <- blockReq

	return <-blockReq.respCh
}

func (c *InmemClient) blockGetter(n node, layers LayersSrc, mu *sync.RWMutex) {
	const BucketSize = 1024

	for req := range c.blockReqChan {
		// return a block with rate limit

		mu.RLock()
		fullLayerSrc := layers[req.layerID]
		mu.RUnlock()

		blockLayerSrc := fullLayerSrc

		blockLayerSrc.DataSize = BlockSize
		blockLayerSrc.Offset = req.blockID.getOffset()
		blockLayerSrc.Meta.Location = InmemLayer

		fullData := *fullLayerSrc.InmemData
		blockData := fullData[blockLayerSrc.Offset : blockLayerSrc.Offset+blockLayerSrc.DataSize]
		blockLayerSrc.InmemData = &blockData

		// set limit (but doesn't actually copy data, as layers are stored in the sender)

		limiter := rate.NewLimiter(rate.Limit(fullLayerSrc.Meta.LimitRate), BucketSize)

		pos := 0
		for pos < len(blockData) {
			n := min(len(blockData)-pos, limiter.Burst())
			limiter.WaitN(context.Background(), n)
			pos += n
		}

		req.respCh <- blockLayerSrc
	}
}

// todo
// client as a different process (obsolete)

const ClientID = NodeID(math.MaxUint)

//
// type ProcessClient struct {
// 	// nodeID is ID of the nodeID the client connects to
// 	nodeID NodeID
// 	t      Transport
// 	layers LayersSrc
// 	mu     sync.RWMutex
// }

// // NewClient creates a client.
// func NewClient(nodeID NodeID, t Transport, layers LayersSrc) *ProcessClient {
// 	c := &ProcessClient{
// 		nodeID: nodeID,
// 		t:      t,
// 		layers: layers,
// 	}

// 	// wait for commands
// 	c.handleIncomingMsg()

// 	return c
// }

// // handle msg
// func (c *ProcessClient) handleIncomingMsg() {
// 	go func() {
// 		for incomingMsg := range c.t.Deliver() {
// 			log.Debug().Msgf("incoming msg[%T]: %s", incomingMsg, incomingMsg)
// 			switch v := incomingMsg.(type) {
// 			case *clientReqMsg:
// 				go c.handleClientReqMsg(v)
// 			}
// 		}
// 	}()
// }

// // handleClientReqMsg sends specified layer to the node.
// func (c *ProcessClient) handleClientReqMsg(clientReqMsg *clientReqMsg) error {
// 	c.mu.RLock()
// 	layerSrc := c.layers[clientReqMsg.LayerID]
// 	c.mu.RUnlock()

// 	log.Debug().Uint("layerID", uint(clientReqMsg.LayerID)).Msg("sending layer")

// 	// send (retransmit) layer to dest.
// 	// the layer should be stored in memory
// 	layerMsg := NewLayerMsg(ClientID, clientReqMsg.LayerID, layerSrc, layerSrc.DataSize)
// 	err := c.t.Send(c.nodeID, layerMsg)
// 	if err != nil {
// 		log.Error().Err(err).Msgf("failed to send layer to %v", clientReqMsg.SrcID)
// 	}
// 	return err
// }
