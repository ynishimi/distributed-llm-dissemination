package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

type config struct {
	Nodes      []NodeConf
	Assignment distributor.Assignment
	LayerSize  uint
}

type NodeConf struct {
	ID            distributor.NodeID
	Addr          string
	IsLeader      bool
	InitialLayers distributor.LayerIDs
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

func GetsConf(conf *config, node distributor.NodeID) (NodeConf, error) {
	// gets leader ID
	for _, nodeconf := range conf.Nodes {
		if nodeconf.ID == node {
			return nodeconf, nil
		}
	}
	return NodeConf{}, fmt.Errorf("no leader found ")
}

// // CreateInmemLeaderLayers creates layers based on the number and the size of layers specified.
// func CreateInmemLeaderLayers(numLayers uint, layerSize uint) distributor.Layers {
// 	layers := make(distributor.Layers, numLayers)
// 	for i := range numLayers {
// 		// add dummy data in memory
// 		layerData := distributor.LayerData(make([]byte, layerSize))
// 		layerSrc := distributor.LayerSrc{
// 			InmemData: &layerData,
// 			Fp:        "",
// 			Size:      layerSize,
// 			Offset:    0,
// 		}

// 		layers[distributor.LayerID(i+1)] = &layerSrc
// 	}
// 	return layers
// }

// // CreateInmemReceiverLayers creates layers based on the number and the size of layers specified.
// func CreateInmemReceiverLayers(parsedID, numLayers, layerSize uint) distributor.Layers {
// 	const LayerNum = 2

// 	dummyLayerData := distributor.LayerData(make([]byte, layerSize))
// 	DummyLayerSrc := distributor.LayerSrc{
// 		InmemData: &dummyLayerData,
// 		Fp:        "",
// 		Size:      layerSize,
// 		Offset:    0,
// 	}

// 	layers := make(distributor.Layers, LayerNum)
// 	layers[distributor.LayerID(parsedID%numLayers+1)] = &DummyLayerSrc
// 	layers[distributor.LayerID((parsedID+1)%numLayers)+1] = &DummyLayerSrc
// 	return layers
// }

// // CreateDiscReceiverThreeLayers creates dummy data and saves it in the disk, and save the source to layers.
// // This is for the topology with 3 nodes, 3 layers.
// // r2 gets a differnt layer from r1 and r3.
// // The topology should look like this:
// // r1 -> r2 <- r3
// func CreateDiscReceiverThreeLayers(parsedID, numLayers, layerSize uint) distributor.Layers {

// 	dummyLayerData := distributor.LayerData(make([]byte, layerSize))

// 	const NumLayers = 1
// 	layers := make(distributor.Layers, NumLayers)
// 	layerNums := []uint{parsedID%numLayers + 1}

// 	for _, layerNum := range layerNums {
// 		path := filepath.Join(os.TempDir(), fmt.Sprintf("%d.layer", layerNum))
// 		err := os.WriteFile(path, dummyLayerData, 0644)
// 		if err != nil {
// 			log.Error().Err(err).Msg("failed to write file")
// 		}

// 		layerSrc := distributor.LayerSrc{
// 			InmemData: nil,
// 			Fp:        path,
// 			Size:      layerSize,
// 			Offset:    0,
// 		}
// 		layers[distributor.LayerID(layerNum)] = &layerSrc
// 	}

// 	return layers
// }

func CreateLayers(myConf NodeConf, layerSize uint, saveDisk bool) distributor.Layers {
	layers := make(distributor.Layers)

	for layerID := range myConf.InitialLayers {
		var layerSrc *distributor.LayerSrc
		if saveDisk {
			layerSrc = CreateDiskLayer(layerID, layerSize)
		} else {
			layerSrc = CreateInmemLayer(layerID, layerSize)
		}
		layers[layerID] = layerSrc
	}

	return layers
}

func CreateDiskLayer(layerID distributor.LayerID, layerSize uint) *distributor.LayerSrc {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("%d.layer", layerID))
	dummyLayerData := distributor.LayerData(make([]byte, layerSize))
	err := os.WriteFile(path, dummyLayerData, 0644)
	if err != nil {
		log.Error().Err(err).Msg("failed to write file")
	}

	return &distributor.LayerSrc{
		InmemData: nil,
		Fp:        path,
		Size:      layerSize,
		Offset:    0,
	}
}

func CreateInmemLayer(layerID distributor.LayerID, layerSize uint) *distributor.LayerSrc {
	// add dummy data in memory
	layerData := distributor.LayerData(make([]byte, layerSize))
	return &distributor.LayerSrc{
		InmemData: &layerData,
		Fp:        "",
		Size:      layerSize,
		Offset:    0,
	}
}

// PrintJsonExample prints an example of config.
func PrintJsonExample() {
	ncs := make([]NodeConf, 4)
	// todo: layer setup
	ncs[0] = NodeConf{0, ":8080", true, make(distributor.LayerIDs)}
	ncs[1] = NodeConf{1, ":8081", false, make(distributor.LayerIDs)}
	ncs[2] = NodeConf{2, ":8082", false, make(distributor.LayerIDs)}
	ncs[3] = NodeConf{3, ":8083", false, make(distributor.LayerIDs)}

	// leader should have all the layers
	ncs[0].InitialLayers[1] = struct{}{}
	// ncs[0].InitialLayers[2] = struct{}{}
	ncs[0].InitialLayers[3] = struct{}{}

	ncs[1].InitialLayers[1] = struct{}{}
	// ncs[2].InitialLayers[2] = struct{}{}
	ncs[3].InitialLayers[3] = struct{}{}

	a := make(distributor.Assignment)

	a[1] = make(distributor.LayerIDs)
	a[2] = make(distributor.LayerIDs)
	a[3] = make(distributor.LayerIDs)

	// a[0][1] = struct{}{}
	// a[0][2] = struct{}{}
	// a[0][3] = struct{}{}

	a[1][1] = struct{}{}

	a[2][1] = struct{}{}
	// a[2][2] = struct{}{}
	a[2][3] = struct{}{}

	a[3][3] = struct{}{}

	c := config{
		Nodes:      ncs,
		Assignment: a,
		// 2 GiB per layer (65 GiB/32 layers ~ 2)
		LayerSize: 1 * uint(math.Pow(2, 30)),
	}

	b, _ := json.Marshal(c)
	fmt.Println(string(b))
}
