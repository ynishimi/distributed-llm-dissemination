package distributor

import (
	"math"
	"sync"

	"github.com/rs/zerolog/log"
)

const ClientID = NodeID(math.MaxUint)

type Client struct {
	// nodeID is ID of the nodeID the client connects to
	nodeID NodeID
	t      Transport
	layers Layers
	mu     sync.RWMutex
}

// NewClient creates a client.
func NewClient(nodeID NodeID, t Transport, layers Layers) *Client {
	c := &Client{
		nodeID: nodeID,
		t:      t,
		layers: layers,
	}

	// wait for commands
	c.handleIncomingMsg()

	return c
}

// handle msg
func (c *Client) handleIncomingMsg() {
	go func() {
		for incomingMsg := range c.t.Deliver() {
			log.Debug().Msgf("incoming msg[%T]: %s", incomingMsg, incomingMsg)
			switch v := incomingMsg.(type) {
			case *clientReqMsg:
				go c.handleClientReqMsg(v)
			}
		}
	}()
}

// handleClientReqMsg sends specified layer to the node.
func (c *Client) handleClientReqMsg(clientReqMsg *clientReqMsg) error {
	c.mu.RLock()
	layerSrc := c.layers[clientReqMsg.LayerID]
	c.mu.RUnlock()

	log.Debug().Uint("layerID", uint(clientReqMsg.LayerID)).Msg("sending layer")

	// send (retransmit) layer to dest.
	// the layer should be stored in memory
	layerMsg := NewLayerMsg(ClientID, clientReqMsg.LayerID, layerSrc, layerSrc.DataSize)
	err := c.t.Send(c.nodeID, layerMsg)
	if err != nil {
		log.Error().Err(err).Msgf("failed to send layer to %v", clientReqMsg.SrcID)
	}
	return err
}
