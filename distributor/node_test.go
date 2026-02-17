package distributor_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

func TestSimpleDistribution(t *testing.T) {
	// assignment and layers
	const NumLayers = 3
	const NumPeers = 3
	const LeaderNodeID = 0
	layers := make(distributor.Layers, NumLayers)

	assignment := make(distributor.Assignment)
	for i := range NumPeers {
		layerIDs := make(distributor.LayerIDs)
		// assign layer i to node i this time
		layerIDs[distributor.LayerID(i+LeaderNodeID+1)] = struct{}{}
		assignment[distributor.NodeID(i+LeaderNodeID+1)] = layerIDs
	}

	t.Run("inmem", func(t *testing.T) {
		// leader
		leader := distributor.NewLeaderNode(distributor.NewNode(LeaderNodeID, LeaderNodeID), distributor.NewInmemTransport(fmt.Sprint(LeaderNodeID)), layers, assignment)

		// receivers
		receivers := make([]*distributor.ReceiverNode, NumPeers)
		for i := range NumPeers {
			receiver := distributor.NewReceiverNode(distributor.NewNode(distributor.NodeID(i+LeaderNodeID+1), LeaderNodeID), distributor.NewInmemTransport(fmt.Sprint(i)), layers)
			receivers[i] = receiver

			// receivers announce its existence to the leader
			err := receiver.Announce()
			require.NoError(t, err)
		}

		// leader should send layers; wait for the leader to collect acks from receivers
		select {
		case ready := <-leader.Ready():
			require.Equal(t, ready, assignment)

		case <-time.After(time.Second):
			t.Fatal("timeout waiting for Ready()")
		}
	})
}
