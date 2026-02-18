package distributor_test

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

func TestMain(m *testing.M) {
	// ignores debug logs
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	m.Run()
}

func createLeaderAndEmptyReceivers(transports []distributor.Transport, assignment distributor.Assignment, layers distributor.Layers, LeaderNodeID distributor.NodeID, NumReceivers int) (*distributor.LeaderNode, []*distributor.ReceiverNode) {
	transCounter := 0
	// leader
	n := distributor.NewNode(LeaderNodeID, LeaderNodeID, transports[transCounter])
	transCounter++
	leader := distributor.NewLeaderNode(n, layers, assignment)

	// receivers
	receivers := make([]*distributor.ReceiverNode, NumReceivers)
	for i := range NumReceivers {
		// gets transport
		receiverTransport := transports[transCounter]
		transCounter++

		// creates a new receiver node with no layers
		node := distributor.NewNode(distributor.NodeID(int(LeaderNodeID)+i+1), LeaderNodeID, receiverTransport)
		receiver := distributor.NewReceiverNode(node, make(distributor.Layers))
		receivers[i] = receiver
	}
	return leader, receivers
}

func createMockLayers(NumLayers, NumReceivers, LeaderNodeID int) *distributor.Layers {
	// layers which the leader should distribute to receivers
	layers := make(distributor.Layers, NumLayers)
	for i := range NumReceivers {
		// add dummy data as small random Bytes
		randBytes := make([]byte, 1)
		rand.Read(randBytes)
		layer := distributor.Layer(randBytes)

		layers[distributor.LayerID(i+LeaderNodeID+1)] = &layer
	}
	return &layers
}

// assign layer i to node i (i = LeaderNodeID + 1, LeaderNodeID + 2, LeaderNodeID + 3, etc).
func createSimpleAssignment(NumReceivers, LeaderNodeID int) *distributor.Assignment {
	assignment := make(distributor.Assignment)
	for i := range NumReceivers {
		layerIDs := make(distributor.LayerIDs)

		layerIDs[distributor.LayerID(i+LeaderNodeID+1)] = struct{}{}
		assignment[distributor.NodeID(i+LeaderNodeID+1)] = layerIDs
	}
	return &assignment
}

// execDistribution executes the distribution of the layers, and notifies when it starts distribution.
func execDistribution(t testing.TB, assignment *distributor.Assignment, leader *distributor.LeaderNode, receivers []*distributor.ReceiverNode) (<-chan distributor.Assignment, <-chan distributor.Assignment) {
	for _, receiver := range receivers {
		// receivers announce its existence to the leader
		err := receiver.Announce()
		require.NoError(t, err)
	}

	// leader should start sending layers after collecting annoucements from receivers
	var start distributor.Assignment
	var ready distributor.Assignment

	// on getting StartDistribution, distributes notifies its start
	startChan := make(chan distributor.Assignment, 1)
	readyChan := make(chan distributor.Assignment, 1)

	go func() {
		select {
		case start = <-leader.StartDistribution():
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for announcements from receivers")
		}

		startChan <- start

		// leader should send layers; wait for the leader to collect acks from receivers
		select {
		case ready = <-leader.Ready():
			require.Equal(t, ready, *assignment)

		case <-time.After(time.Second):
			t.Fatal("timeout waiting for Ready()")
		}

		readyChan <- ready

	}()

	return startChan, readyChan
}

// createTcpTransports creates a slice of TCP transports.
func createTcpTransports(NumPeers, LeaderNodeID int) []distributor.Transport {
	addrs := make(distributor.AddrRegistory, NumPeers)
	transports := make([]distributor.Transport, NumPeers)
	for i := range NumPeers {
		addrs[distributor.NodeID(i)] = fmt.Sprintf(":%d", 8080+LeaderNodeID+i)
		transports[i] = distributor.NewTcpTransport(addrs[distributor.NodeID(i)], uint(NumPeers), addrs)
	}

	return transports
}

func TestSimpleDistribution(t *testing.T) {
	// assignment and layers
	const NumLayers = 3
	const NumReceivers = 3
	const NumPeers = NumReceivers + 1
	const LeaderNodeID = 0

	layers := createMockLayers(NumLayers, NumReceivers, LeaderNodeID)
	assignment := createSimpleAssignment(NumReceivers, LeaderNodeID)

	t.Run("inmem", func(t *testing.T) {

		transports := make([]distributor.Transport, NumPeers)
		for i := range NumPeers {
			transports[i] = distributor.NewInmemTransport(fmt.Sprint(LeaderNodeID+i), NumPeers)
		}

		leader, receivers := createLeaderAndEmptyReceivers(transports, *assignment, *layers, LeaderNodeID, NumReceivers)
		_, ready := execDistribution(t, assignment, leader, receivers)

		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Error("timeout waiting for message delivery")
		}

		for _, tr := range transports {
			tr.Close()
		}
	})
	t.Run("tcp", func(t *testing.T) {
		transports := createTcpTransports(NumPeers, LeaderNodeID)
		t.Cleanup(func() {
			for _, tr := range transports {
				tr.Close()
			}
		})

		leader, receivers := createLeaderAndEmptyReceivers(transports, *assignment, *layers, LeaderNodeID, NumReceivers)
		_, ready := execDistribution(t, assignment, leader, receivers)

		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Error("timeout waiting for message delivery")
		}

		for _, tr := range transports {
			tr.Close()
		}
	})
}

func BenchmarkSimpleDistributionTcp(b *testing.B) {
	// assignment and layers
	const NumLayers = 3
	const NumReceivers = 3
	const NumPeers = NumReceivers + 1
	const LeaderNodeID = 0

	layers := createMockLayers(NumLayers, NumReceivers, LeaderNodeID)
	assignment := createSimpleAssignment(NumReceivers, LeaderNodeID)

	// for b.Loop() {
	// TCP transport
	transports := createTcpTransports(NumPeers, LeaderNodeID)

	leader, receivers := createLeaderAndEmptyReceivers(transports, *assignment, *layers, LeaderNodeID, NumReceivers)
	start, ready := execDistribution(b, assignment, leader, receivers)

	// start the timer
	<-start
	b.ResetTimer()

	<-ready

	for _, tr := range transports {
		tr.Close()
	}
	// }
}
