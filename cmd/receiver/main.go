package main

import (
	"flag"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/ynishimi/distributed-llm-dissemination/cmd/common"
	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

var myAddr = flag.String("addr", "", "my address")
var myID = flag.Int("id", -1, "my ID")
var fileName = flag.String("filename", "", "filename of topology JSON file")
var mode = flag.Int("mode", -1, "0: naive, 1: layer retransmit")

type config struct {
	Nodes      []nodeConf
	Assignment distributor.Assignment
	LayerSize  uint
}

type nodeConf struct {
	Id       distributor.NodeID
	Addr     string
	IsLeader bool
}

func main() {
	// get input
	flag.Parse()
	if *myAddr == "" || *myID < 0 || *fileName == "" || *mode < 0 {
		fmt.Println("usage: -addr :8080 -id  0 -filename config.json -mode 0")
		return
	}

	fmt.Printf("launching receiver...\n[addr: %s, id: %v, filename: %s]\n", *myAddr, *myID, *fileName)

	// read JSON files
	conf, err := common.ReadJson(*fileName)
	if err != nil {
		return
	}

	leaderConf, err := common.GetsLeaderConf(conf)
	if err != nil {
		log.Error().Err(err).Msg("leader not found in config")
		return
	}
	numPeers := uint(len(conf.Nodes))
	parsedID := uint(*myID)

	// load (dummy) layers
	layers := common.CreateInmemReceiverLayers(parsedID, numPeers-1, conf.LayerSize)

	// creates registory
	addrRegistry := make(distributor.AddrRegistory, numPeers)
	for _, nodeconf := range conf.Nodes {
		addrRegistry[nodeconf.Id] = nodeconf.Addr
	}

	// create transport
	t := distributor.NewTcpTransport(*myAddr, numPeers, addrRegistry)
	n := distributor.NewNode(distributor.NodeID(parsedID), leaderConf.Id, t)

	mode := uint(*mode)

	var receiverNode distributor.Receiver
	switch mode {
	case 0:
		receiverNode = distributor.NewReceiverNode(n, layers)
	case 1:
		receiverNode = distributor.NewRetransmitReceiverNode(n, layers)
	default:
		log.Error().Msg("unknown mode")
		return
	}

	err = receiverNode.Announce()

	if err != nil {
		log.Error().Err(err).Msg("failed to announce")
		return
	}

	select {}
}
