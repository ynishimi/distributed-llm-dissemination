package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

var myAddr = flag.String("addr", "", "my address")
var myID = flag.Int("id", -1, "my ID")
var fileName = flag.String("filename", "", "filename of topology JSON file")

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

	fmt.Printf("launching receiver...\n[addr: %s, id: %v, filename: %s]\n", *myAddr, *myID, *fileName)

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
	layers := make(distributor.Layers)

	// creates registory
	addrRegistry := make(distributor.AddrRegistory, numPeers)
	for _, nodeconf := range conf.Nodes {
		addrRegistry[nodeconf.Id] = nodeconf.Addr
	}

	// create transport
	t := distributor.NewTcpTransport(*myAddr, numPeers, addrRegistry)
	parsedID := uint(*myID)
	n := distributor.NewNode(distributor.NodeID(parsedID), leaderConf.Id, t)

	receiverNode := distributor.NewReceiverNode(n, layers)
	err = receiverNode.Announce()

	if err != nil {
		log.Error().Err(err).Msg("failed to announce")
		return
	}

	select {}
}

// // createMockLayers creates layers based on the number and the size of layers specified.
// func createMockLayers(numLayers uint, layerSize uint) distributor.Layers {
// 	layers := make(distributor.Layers, numLayers)
// 	for i := range numLayers {
// 		// add dummy data as random Bytes
// 		randBytes := make([]byte, layerSize)
// 		rand.Read(randBytes)
// 		layer := distributor.Layer(randBytes)

// 		layers[distributor.LayerID(i)] = &layer
// 	}
// 	return layers
// }

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
