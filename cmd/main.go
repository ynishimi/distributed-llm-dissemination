package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

var myID = flag.Int("id", -1, "my ID")
var fileName = flag.String("f", "", "filename of topology JSON file")
var storagePath = flag.String("s", "", "path of storing layers")
var mode = flag.Int("m", -1, "0: naive, 1: layer retransmit")

const SaveDisk = true

func main() {
	// get input
	flag.Parse()
	if *myID < 0 || *fileName == "" {
		fmt.Println("usage: -id  0 -f config.json -s /mnt -m 0")
		fmt.Println()
		PrintJsonExample()
		return
	}
	myID := distributor.NodeID(*myID)
	mode := uint(*mode)

	// read JSON files
	conf, err := ReadJson(*fileName)
	if err != nil {
		return
	}
	myConf, err := GetsConf(conf, distributor.NodeID(myID))
	if err != nil {
		log.Error().Err(err).Msg("node not found in config")
		return
	}

	leaderConf, err := GetsLeaderConf(conf)
	if err != nil {
		log.Error().Err(err).Msg("leader not found in config")
		return
	}
	numPeers := uint(len(conf.Nodes))

	// load (dummy) layers
	layers := CreateLayers(myConf, conf.LayerSize, SaveDisk)

	// creates registory
	addrRegistry := make(distributor.AddrRegistory, numPeers)
	for _, nodeconf := range conf.Nodes {
		addrRegistry[nodeconf.ID] = nodeconf.Addr
	}

	// create transport
	t := distributor.NewTcpTransport(myConf.Addr, numPeers, addrRegistry)
	n := distributor.NewNode(myID, leaderConf.ID, t)

	if myConf.IsLeader {
		err = RunLeader(myID, n, t, layers, conf.Assignment, mode)
		if err != nil {
			log.Error().Err(err).Msg("leader failed")
		}
	} else {
		err = RunReceiver(myID, n, leaderConf.ID, t, layers, mode)
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
	fmt.Printf("launching receiver...\n[addr: %s, id: %v, filename: %s]\n", n.GetTransport().GetAddress(), myID, *fileName)

	var receiverNode distributor.Receiver
	switch mode {
	case 0:
		receiverNode = distributor.NewReceiverNode(n, layers)
	case 1:
		receiverNode = distributor.NewRetransmitReceiverNode(n, layers)
	default:
		return fmt.Errorf("unknown mode")
	}

	return executeReceiver(receiverNode)
}

func executeReceiver(receiver distributor.Receiver) error {
	err := receiver.Announce()

	if err != nil {
		return err
	}

	<-receiver.Ready()
	return nil
}
