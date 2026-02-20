package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/ynishimi/distributed-llm-dissemination/cmd/common"
	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

var myAddr = flag.String("addr", "", "my address")
var myID = flag.Int("id", -1, "my ID")
var fileName = flag.String("filename", "", "filename of topology JSON file")
var mode = flag.Int("mode", -1, "0: naive, 1: layer retransmit")

func main() {
	// get input
	flag.Parse()
	if *myAddr == "" || *myID < 0 || *fileName == "" {
		fmt.Println("usage: -addr :8080 -id  0 -filename config.json -mode 0")
		return
	}

	fmt.Printf("launching leader...\n[addr: %s, id: %v, filename: %s]\n", *myAddr, *myID, *fileName)

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

	// load (dummy) layers
	layers := common.CreateInmemLeaderLayers(numPeers-1, conf.LayerSize)

	// creates registory
	addrRegistry := make(distributor.AddrRegistory, numPeers)
	for _, nodeconf := range conf.Nodes {
		addrRegistry[nodeconf.Id] = nodeconf.Addr
	}

	// create transport
	t := distributor.NewTcpTransport(*myAddr, numPeers, addrRegistry)
	parsedID := uint(*myID)
	n := distributor.NewNode(distributor.NodeID(parsedID), leaderConf.Id, t)

	mode := uint(*mode)

	var leaderNode distributor.Leader
	switch mode {
	case 0:
		leaderNode = distributor.NewLeaderNode(n, layers, conf.Assignment)
	case 1:
		leaderNode = distributor.NewRetransmitLeaderNode(n, layers, conf.Assignment)
	default:
		log.Error().Msg("unknown mode")
		return
	}

	ttd := run(leaderNode)
	fmt.Printf("Time to deliver: %v\n", ttd)
}

func run(leader distributor.Leader) time.Duration {
	<-leader.StartDistribution()
	t0 := time.Now()

	<-leader.Ready()
	t1 := time.Since(t0)

	return t1
}
