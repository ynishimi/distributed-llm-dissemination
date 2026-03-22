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
	Clients    []ClientConf
	Assignment distributor.Assignment
	LayerSize  int64
}

type NodeConf struct {
	ID            distributor.NodeID
	Addr          string
	IsLeader      bool
	InitialLayers distributor.LayerIDs
}

// set of LayerIDs for clients
type LayerIDsRateLimit map[distributor.LayerID]int64

type ClientConf struct {
	ID              distributor.NodeID
	Addr            string
	LayersRateLimit LayerIDsRateLimit `json:"Layers"`
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

func GetLeaderConf(conf *config) (NodeConf, error) {
	// gets leader ID
	for _, nodeconf := range conf.Nodes {
		if nodeconf.IsLeader {
			return nodeconf, nil
		}
	}
	return NodeConf{}, fmt.Errorf("no leader found ")
}

func GetNodeConf(conf *config, node distributor.NodeID) (NodeConf, error) {
	// gets leader ID
	for _, nodeconf := range conf.Nodes {
		if nodeconf.ID == node {
			return nodeconf, nil
		}
	}
	return NodeConf{}, fmt.Errorf("no node found")
}

func GetClientConf(conf *config, node distributor.NodeID) (*ClientConf, error) {
	// gets leader ID
	for _, clientConf := range conf.Clients {
		if clientConf.ID == node {
			return &clientConf, nil
		}
	}
	return nil, fmt.Errorf("no client found")
}

func CreateLayers(myConf NodeConf, layerSize int64, saveDisk bool) distributor.Layers {
	layers := make(distributor.Layers)

	for layerID := range myConf.InitialLayers {
		var layerSrc distributor.LayerSrc
		if saveDisk {
			layerSrc = CreateDiskLayer(myConf.ID, layerID, layerSize, *storagePath)
		} else {
			layerSrc = CreateInmemLayer(layerID, layerSize)
		}
		layers[layerID] = layerSrc
	}

	return layers
}

func AddClientLayers(clientConf *ClientConf, layerSize int64, layers distributor.Layers) distributor.Layers {
	for layerID, limitRate := range clientConf.LayersRateLimit {
		if _, ok := layers[layerID]; ok {
			// already in memory/disk
			continue
		}

		layers[layerID] = CreateClientLayerInfo(layerID, layerSize, limitRate)

	}

	return layers
}

func CreateDiskLayer(myID distributor.NodeID, layerID distributor.LayerID, layerSize int64, storagePath string) distributor.LayerSrc {
	// save as myID/layerID.layer
	dir := filepath.Join(storagePath, "layers/", fmt.Sprintf("%d", myID))
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Error().Err(err).Msg("failed to create directory")
	}
	path := filepath.Join(dir, fmt.Sprintf("%d.layer", layerID))
	if _, err := os.Stat(path); os.IsNotExist(err) {
		dummyLayerData := distributor.LayerData(make([]byte, layerSize))
		err = os.WriteFile(path, dummyLayerData, 0644)
		if err != nil {
			log.Error().Err(err).Msg("failed to write file")
		}
	}

	return distributor.LayerSrc{
		InmemData: nil,
		Fp:        path,
		DataSize:  layerSize,
		Offset:    0,
		Meta: distributor.LayerMeta{
			Location: distributor.DiskLayer,
		},
	}
}

func CreateInmemLayer(layerID distributor.LayerID, layerSize int64) distributor.LayerSrc {
	// add dummy data in memory
	layerData := distributor.LayerData(make([]byte, layerSize))
	return distributor.LayerSrc{
		InmemData: &layerData,
		Fp:        "",
		DataSize:  layerSize,
		Offset:    0,
		Meta: distributor.LayerMeta{
			Location: distributor.InmemLayer,
		},
	}
}

// CreateClientLayer creates layers with rate limit.
func CreateClientLayer(layerID distributor.LayerID, layerSize int64, limitRate int64) distributor.LayerSrc {
	layerSrc := CreateInmemLayer(layerID, layerSize)
	layerSrc.Meta = distributor.LayerMeta{
		// the layer is stored in memory of the client
		Location:  distributor.InmemLayer,
		LimitRate: limitRate,
	}

	log.Debug().Int64("limitRate", limitRate).Send()
	return layerSrc
}

// CreateClientLayerInfo creates layerSrc information remembered by the node.
func CreateClientLayerInfo(layerID distributor.LayerID, layerSize int64, limitRate int64) distributor.LayerSrc {
	return distributor.LayerSrc{
		InmemData: nil,
		Fp:        "",
		DataSize:  layerSize,
		Offset:    0,
		Meta: distributor.LayerMeta{
			Location:  distributor.ClientLayer,
			LimitRate: limitRate,
		},
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
	ncs[0].InitialLayers[1] = distributor.LayerMeta{
		Location: distributor.InmemLayer,
	}

	ncs[0].InitialLayers[3] = distributor.LayerMeta{
		Location: distributor.InmemLayer,
	}

	ncs[1].InitialLayers[1] = distributor.LayerMeta{
		Location: distributor.InmemLayer,
	}

	ncs[3].InitialLayers[3] = distributor.LayerMeta{
		Location: distributor.InmemLayer,
	}

	a := make(distributor.Assignment)

	a[1] = make(distributor.LayerIDs)
	a[2] = make(distributor.LayerIDs)
	a[3] = make(distributor.LayerIDs)

	a[1][1] = distributor.LayerMeta{
		Location: distributor.InmemLayer,
	}

	a[2][1] = distributor.LayerMeta{
		Location: distributor.InmemLayer,
	}
	// a[2][2] = struct{}{}
	a[2][3] = distributor.LayerMeta{
		Location: distributor.InmemLayer,
	}

	a[3][3] = distributor.LayerMeta{
		Location: distributor.InmemLayer,
	}

	c := config{
		Nodes:      ncs,
		Assignment: a,
		// 2 GiB per layer (65 GiB/32 layers ~ 2)
		LayerSize: 1 * int64(math.Pow(2, 30)),
	}

	b, _ := json.Marshal(c)
	fmt.Println(string(b))
}
