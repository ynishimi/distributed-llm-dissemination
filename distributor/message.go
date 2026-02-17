package distributor

import (
	"encoding/json"
	"fmt"
)

type Message interface {
	Src() string
	Payload() []byte
	String() string
}

// announceMsg
type announceMsg struct {
	src      NodeID
	layerIDs LayerIDs
}

func NewAnnounceMsg(src NodeID, layers LayerIDs) *announceMsg {
	return &announceMsg{
		src:      src,
		layerIDs: layers,
	}
}

func (m *announceMsg) Src() string {
	return fmt.Sprint(m.src)
}

func (m *announceMsg) Payload() []byte {
	data, _ := json.Marshal(m.layerIDs)
	return data
}

func (m *announceMsg) String() string {
	return fmt.Sprintf("%v: %s", m.src, m.layerIDs.String())
}

// ackMsg

type ackMsg struct {
	src     NodeID
	layerID LayerID
}

func NewAckMsg(src NodeID, layerID LayerID) *ackMsg {
	return &ackMsg{
		src:     src,
		layerID: layerID,
	}
}

func (m *ackMsg) Src() string {
	return fmt.Sprint(m.src)
}

func (m *ackMsg) Payload() []byte {
	data, _ := json.Marshal(m.layerID)
	return data
}

func (m *ackMsg) String() string {
	return fmt.Sprintf("%v: %v", m.src, m.layerID)
}

// layerMsg

type layerMsg struct {
	src     NodeID
	layerID LayerID
	layer   Layer
}

func NewLayerMsg(src NodeID, layerID LayerID, layer Layer) *layerMsg {
	return &layerMsg{
		src:     src,
		layerID: layerID,
		layer:   layer,
	}
}

func (m *layerMsg) Src() string {
	return fmt.Sprint(m.src)
}

func (m *layerMsg) Payload() []byte {
	return m.layer
}

func (m *layerMsg) String() string {
	return fmt.Sprintf("%v: %v", m.src, m.layerID)
}

// SimpleMsg for testing.
type SimepleMsg struct {
	src     string
	payload string
}

func NewSimpleMsg(src string, payload string) *SimepleMsg {
	return &SimepleMsg{
		src:     src,
		payload: payload,
	}
}

func (m *SimepleMsg) Src() string {
	return m.src
}

func (m *SimepleMsg) Payload() []byte {
	return []byte(m.payload)
}

func (m *SimepleMsg) String() string {
	return fmt.Sprintf("%s: %s", m.src, m.payload)
}
