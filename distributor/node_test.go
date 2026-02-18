package distributor_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

func TestSimpleDistribution(t *testing.T) {
	// assignment and layers
	const NumLayers = 3
	const NumReceivers = 3
	const NumPeers = NumReceivers + 1
	const LeaderNodeID = 0
	layers := make(distributor.Layers)
	for i := range NumReceivers {
		// add dummy data as small random Bytes
		randBytes := make([]byte, 1)
		rand.Read(randBytes)
		layer := distributor.Layer(randBytes)

		layers[distributor.LayerID(i+LeaderNodeID+1)] = &layer
	}

	assignment := make(distributor.Assignment)
	for i := range NumReceivers {
		layerIDs := make(distributor.LayerIDs)
		// assign layer i to node i this time
		layerIDs[distributor.LayerID(i+LeaderNodeID+1)] = struct{}{}
		assignment[distributor.NodeID(i+LeaderNodeID+1)] = layerIDs
	}

	distribute := func(t *testing.T, transports []distributor.Transport) {
		t.Helper()
		transCounter := 0

		// leader
		n := distributor.NewNode(LeaderNodeID, LeaderNodeID, transports[transCounter])
		transCounter++
		leader := distributor.NewLeaderNode(n, layers, assignment)

		// receivers
		receivers := make([]*distributor.ReceiverNode, NumReceivers)
		for i := range NumReceivers {
			receiverTransport := transports[transCounter]
			transCounter++
			receiver := distributor.NewReceiverNode(distributor.NewNode(distributor.NodeID(LeaderNodeID+i+1), LeaderNodeID, receiverTransport), layers)
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
	}

	t.Run("inmem", func(t *testing.T) {

		transports := make([]distributor.Transport, NumPeers)
		for i := range NumPeers {
			transports[i] = distributor.NewInmemTransport(fmt.Sprint(LeaderNodeID+i), NumPeers)
		}

		distribute(t, transports)
	})
	t.Run("tcp", func(t *testing.T) {
		addrs := make(distributor.AddrRegistory, NumPeers)
		transports := make([]distributor.Transport, NumPeers)
		for i := range NumPeers {
			addrs[distributor.NodeID(i)] = fmt.Sprintf(":%d", 8080+LeaderNodeID+i)
		}
		for i := range NumPeers {
			transports[i] = distributor.NewTcpTransport(addrs[distributor.NodeID(i)], NumPeers, addrs)
		}
		t.Cleanup(func() {
			for _, tr := range transports {
				tr.Close()
			}
		})

		distribute(t, transports)
	})
}
