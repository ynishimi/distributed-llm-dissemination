package common

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

type config struct {
	Nodes      []NodeConf
	Assignment distributor.Assignment
	LayerSize  uint
}

type NodeConf struct {
	Id       distributor.NodeID
	Addr     string
	IsLeader bool
}

// ReadJson reads Json file and returns config struct.
func ReadJson(fileName string) (*config, error) {
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

func GetsLeaderConf(conf *config) (NodeConf, error) {
	// gets leader ID
	for _, nodeconf := range conf.Nodes {
		if nodeconf.IsLeader {
			return nodeconf, nil
		}
	}
	return NodeConf{}, fmt.Errorf("no leader found ")
}

// CreateInmemLeaderLayers creates layers based on the number and the size of layers specified.
func CreateInmemLeaderLayers(numLayers uint, layerSize uint) distributor.Layers {
	layers := make(distributor.Layers, numLayers)
	for i := range numLayers {
		// add dummy data in memory
		layerData := distributor.LayerData(make([]byte, layerSize))
		layerSrc := distributor.LayerSrc{
			InmemData: &layerData,
			Fp:        "",
			Size:      layerSize,
			Offset:    0,
		}

		layers[distributor.LayerID(i+1)] = &layerSrc
	}
	return layers
}

// CreateInmemReceiverLayers creates layers based on the number and the size of layers specified.
func CreateInmemReceiverLayers(parsedID, numLayers, layerSize uint) distributor.Layers {
	layerNum := 2

	dummyLayerData := distributor.LayerData(make([]byte, layerNum))
	DummyLayerSrc := distributor.LayerSrc{
		InmemData: &dummyLayerData,
		Fp:        "",
		Size:      layerSize,
		Offset:    0,
	}

	layers := make(distributor.Layers, layerNum)
	layers[distributor.LayerID(parsedID%numLayers+1)] = &DummyLayerSrc
	layers[distributor.LayerID((parsedID+1)%numLayers)+1] = &DummyLayerSrc
	return layers
}

// PrintJsonExample prints an example of config.
func PrintJsonExample() {
	ncs := make([]NodeConf, 2)
	ncs[0] = NodeConf{0, ":8080", true}
	ncs[1] = NodeConf{1, ":8081", false}
	a := make(distributor.Assignment)
	a[0] = make(distributor.LayerIDs)
	a[0][0] = struct{}{}
	a[0][1] = struct{}{}
	a[1] = make(distributor.LayerIDs)
	a[1][2] = struct{}{}

	c := config{
		Nodes:      ncs,
		Assignment: a,
	}

	b, _ := json.Marshal(c)
	fmt.Println(string(b))
}
