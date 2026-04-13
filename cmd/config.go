package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/ynishimi/distributed-llm-dissemination/distributor"
)

type config struct {
	Nodes      []NodeConf
	Clients    []ClientConf
	Layers     map[distributor.LayerID]layerConf
	Assignment distributor.Assignment
}

type layerConf struct {
	LayerSize int64
}

type NodeConf struct {
	ID            distributor.NodeID
	Addr          string
	NetworkBW     int64
	IsLeader      bool
	Sources       map[distributor.SourceType]int64
	InitialLayers InitialLayers
}

type InitialLayers map[distributor.SourceType]InitLayersBySource

type InitLayersBySource map[distributor.LayerID]struct{}

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
	err = json.Unmarshal(byteValue, &conf)

	return &conf, err
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

func CreateLayers(myConf NodeConf, saveDisk bool, layerConfMap map[distributor.LayerID]layerConf) distributor.LayersSrc {
	layers := make(distributor.LayersSrc)

	for sourceType, layersBySource := range myConf.InitialLayers {
		for layerID := range layersBySource {
			currentLayerSize := layerConfMap[layerID].LayerSize
			if currentLayerSize < 0 {
				currentLayerSize = 0
			}

			var layerSrc distributor.LayerSrc
			if saveDisk {
				layerSrc = CreateDiskLayer(myConf.ID, layerID, currentLayerSize, *storagePath)
			} else {
				layerSrc = CreateInmemLayer(layerID, currentLayerSize)
			}
			layerSrc.DataSize = currentLayerSize
			layerSrc.Meta.LimitRate = myConf.Sources[sourceType]
			layers[layerID] = layerSrc
		}
	}

	return layers
}

func AddClientLayers(clientConf *ClientConf, layerConfMap map[distributor.LayerID]layerConf, layers distributor.LayersSrc) distributor.LayersSrc {
	for layerID, limitRate := range clientConf.LayersRateLimit {
		if _, ok := layers[layerID]; ok {
			// already in memory/disk
			continue
		}

		layers[layerID] = CreateClientLayerInfo(layerID, layerConfMap[layerID].LayerSize, limitRate)

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

// Create a set of layer managers for receivers
func CreateLayerManagers(layerIDs distributor.LayerIDs, layersMap map[distributor.LayerID]layerConf) map[distributor.LayerID]*distributor.LayerManager {
	layerManagers := make(map[distributor.LayerID]*distributor.LayerManager)

	for layerID := range layerIDs {
		newManager := &distributor.LayerManager{
			ReceivedBlocks: make(map[distributor.BlockID]struct{}),
			NextReqBlock:   0,
			TotalBlockNum:  distributor.BlockID(layersMap[layerID].LayerSize / distributor.BlockSize),
			ActiveSenders:  make(map[distributor.NodeID]distributor.ActiveSender),
		}
		layerManagers[layerID] = newManager
	}

	return layerManagers
}
