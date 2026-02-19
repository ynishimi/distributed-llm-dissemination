package main

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog/log"
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
	if *myAddr == "" || *myID < 0 || *fileName == "" {
		fmt.Println("usage: -addr :8080 -id  0 -filename config.json")
		return
	}

	fmt.Printf("launching leader...\n[addr: %s, id: %v, filename: %s]\n", *myAddr, *myID, *fileName)

	// read JSON files
	conf, err := readJson(*fileName)
	if err != nil {
		return
	}

	leaderConf, err := getsLeaderConf(conf)
	if err != nil {
		log.Error().Err(err).Msg("leader not found in config")
		return
	}
	numPeers := uint(len(conf.Nodes))

	// load (dummy) layers
	layers := createMockLayers(numPeers-1, conf.LayerSize)

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

// createMockLayers creates layers based on the number and the size of layers specified.
func createMockLayers(numLayers uint, layerSize uint) distributor.Layers {
	layers := make(distributor.Layers, numLayers)
	for i := range numLayers {
		// add dummy data as random Bytes
		randBytes := make([]byte, layerSize)
		rand.Read(randBytes)
		layer := distributor.Layer(randBytes)

		layers[distributor.LayerID(i+1)] = &layer
	}
	return layers
}

func getsLeaderConf(conf *config) (nodeConf, error) {
	// gets leader ID
	for _, nodeconf := range conf.Nodes {
		if nodeconf.IsLeader {
			return nodeconf, nil
		}
	}
	return nodeConf{}, fmt.Errorf("no leader found ")
}

func readJson(fileName string) (*config, error) {
	jsonFile, err := os.Open(fileName)
	if err != nil {
		log.Error().Err(err).Msgf("failed to load json file: %s", fileName)
		return nil, err
	}
	defer jsonFile.Close()

	var conf config

	byteValue, _ := io.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &conf)

	return &conf, nil
}

// example of config
// ncs := make([]nodeConf, 2)
// ncs[0] = nodeConf{0, ":8080", true}
// ncs[1] = nodeConf{1, ":8081", false}
// a := make(distributor.Assignment)
// a[0] = make(distributor.LayerIDs)
// a[0][0] = struct{}{}
// a[0][1] = struct{}{}
// a[1] = make(distributor.LayerIDs)
// a[1][2] = struct{}{}

// c := config{
// 	Nodes:      ncs,
// 	Assignment: a,
// }

// b, _ := json.Marshal(c)
// fmt.Println(string(b))
