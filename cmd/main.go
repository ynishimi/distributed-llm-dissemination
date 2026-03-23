package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

var myID = flag.Int("id", -1, "my ID")
var fileName = flag.String("f", "", "filename of topology JSON file")
var storagePath = flag.String("s", "", "path of storing layers")
var mode = flag.Int("m", -1, "0: naive, 1: layer retransmit")
var layerSetup = flag.Bool("l", false, "create layer files and exit")
var client = flag.Bool("c", false, "if the process is client")
var verbose = flag.Bool("v", false, "output debug messages")

func main() {
	// get input
	flag.Parse()
	if *myID < 0 || *fileName == "" {
		fmt.Println("usage: -id 0 -f config.json -s . -m 2 -l -v")
		fmt.Println()
		// PrintJsonExample()
		return
	}
	myID := distributor.NodeID(*myID)
	mode := uint(*mode)

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	if *verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// read JSON files
	conf, err := ReadJson(*fileName)
	if err != nil {
		return
	}

	leaderNodeConf, err := GetLeaderConf(conf)
	if err != nil {
		log.Error().Err(err).Msg("leader not found in config")
		return
	}

	myNodeConf, err := GetNodeConf(conf, distributor.NodeID(myID))
	if err != nil {
		log.Error().Err(err).Msg("node not found in config")
		return
	}

	myClientConf, err := GetClientConf(conf, distributor.NodeID(myID))
	if err != nil {
		log.Warn().Err(err).Msg("client not found in config")
	}

	if *client {
		// creates registory (only the node to which the client connects)
		addrRegistry := make(distributor.AddrRegistry, 1)
		if myClientConf.ID != myNodeConf.ID {
			log.Error().Err(err).Msg("weird node")
			return
		}
		addrRegistry[myNodeConf.ID] = myNodeConf.Addr

		// create transport
		t, err := distributor.NewTcpTransport(myClientConf.Addr, 1, addrRegistry, true)
		if err != nil {
			log.Error().Err(err).Msg("failed to create transport")
			return
		}

		layers := make(distributor.Layers)
		for layerID, rateLimit := range myClientConf.LayersRateLimit {
			layers[layerID] = CreateClientLayer(layerID, conf.LayerSize, rateLimit)
		}

		RunClient(myClientConf.ID, t, layers)
	}

	var saveDisk = true
	if *storagePath == "" {
		saveDisk = false
	}

	numPeers := uint(len(conf.Nodes))

	// load (dummy) layers
	layers := CreateLayers(myNodeConf, saveDisk)

	// If there is a client connecte to the node, add layers of it
	if myClientConf != nil {
		layers = AddClientLayers(myClientConf, conf.LayerSize, layers)
	}

	if *layerSetup {
		log.Info().Msg("layer set up")
		return
	}

	// creates registory
	addrRegistry := make(distributor.AddrRegistry, numPeers)
	for _, nodeconf := range conf.Nodes {
		addrRegistry[nodeconf.ID] = nodeconf.Addr
	}
	if myClientConf != nil {
		addrRegistry[distributor.ClientID] = myClientConf.Addr
	}

	// create transport
	t, err := distributor.NewTcpTransport(myNodeConf.Addr, numPeers, addrRegistry, false)
	if err != nil {
		log.Error().Err(err).Msg("failed to create transport")
		return
	}

	n := distributor.NewNode(myID, leaderNodeConf.ID, t)

	if myNodeConf.IsLeader {
		err = RunLeader(myID, n, t, layers, conf.Assignment, mode)
		if err != nil {
			log.Error().Err(err).Msg("leader failed")
		}
	} else {
		err = RunReceiver(myID, n, leaderNodeConf.ID, t, layers, mode)
		if err != nil {
			log.Error().Err(err).Msg("receiver failed")
		}
	}

}

func RunLeader(myID distributor.NodeID, n *distributor.N, t distributor.Transport, layers distributor.Layers, assignment distributor.Assignment, mode uint) error {
	fmt.Printf("launching leader...\n[addr: %s, id: %v, filename: %s, storagePath: %v, mode: %v]\n", n.GetTransport().GetAddress(), myID, *fileName, *storagePath, mode)

	var leaderNode distributor.Leader
	switch mode {
	case 0:
		leaderNode = distributor.NewLeaderNode(n, layers, assignment)
	case 1:
		leaderNode = distributor.NewRetransmitLeaderNode(n, layers, assignment)
	case 2:
		leaderNode = distributor.NewPullRetransmitLeaderNode(n, layers, assignment)
	case 3:
		leaderNode = distributor.NewFlowRetransmitLeaderNode(n, layers, assignment)

	default:
		return fmt.Errorf("unknown mode")
	}

	ttd := executeLeader(leaderNode)
	fmt.Printf("Time to deliver: %v\n", ttd)

	return nil
}

func executeLeader(leader distributor.Leader) time.Duration {
	<-leader.StartDistribution()
	t0 := time.Now()

	<-leader.Ready()
	t1 := time.Since(t0)

	return t1
}

func RunReceiver(myID distributor.NodeID, n *distributor.N, leaderID distributor.NodeID, t distributor.Transport, layers distributor.Layers, mode uint) error {
	fmt.Printf("launching receiver...\n[addr: %s, id: %v, filename: %s, storagePath: %v, mode: %v]\n", n.GetTransport().GetAddress(), myID, *fileName, *storagePath, mode)

	var receiverNode distributor.Receiver
	switch mode {
	case 0:
		receiverNode = distributor.NewReceiverNode(n, layers, *storagePath)
	case 1, 2:
		receiverNode = distributor.NewRetransmitReceiverNode(n, layers, *storagePath)
	case 3:
		receiverNode = distributor.NewFlowRetransmitReceiverNode(n, layers, *storagePath)

	default:
		return fmt.Errorf("unknown mode")
	}

	err := executeReceiver(receiverNode)
	if err != nil {
		log.Error().Err(err).Msg("receiver failed to execute")
	}
	return nil
}

func executeReceiver(receiver distributor.Receiver) error {
	err := receiver.Announce()

	if err != nil {
		return err
	}

	<-receiver.Ready()
	return nil
}

func RunClient(nodeID distributor.NodeID, t distributor.Transport, layers distributor.Layers) {
	_ = distributor.NewClient(nodeID, t, layers)
	select {}
}
