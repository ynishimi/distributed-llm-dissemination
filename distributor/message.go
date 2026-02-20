package distributor

import (
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"
)

type Message interface {
	Src() string
	Type() MsgType
	// Payload() []byte
	String() string
}

// MsgType is a enum for indicating the type of the message.
type MsgType uint8

const (
	MsgTypeAnnounce MsgType = iota
	MsgTypeAck
	MsgTypeLayer
	MsgTypeRetransmit
	MsgTypeStartup
	MsgTypeSimple
	MsgTypeTransport
)

// announceMsg
type announceMsg struct {
	SrcID    NodeID
	LayerIDs LayerIDs
}

func NewAnnounceMsg(src NodeID, layers LayerIDs) *announceMsg {
	return &announceMsg{
		SrcID:    src,
		LayerIDs: layers,
	}
}

func (m *announceMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *announceMsg) Type() MsgType {
	return MsgTypeAnnounce
}

func (m *announceMsg) Payload() []byte {
	data, _ := json.Marshal(m.LayerIDs)
	return data
}

func (m *announceMsg) String() string {
	return fmt.Sprintf("%v: %s", m.SrcID, m.LayerIDs.String())
}

// ackMsg

type ackMsg struct {
	SrcID   NodeID
	LayerID LayerID
}

func NewAckMsg(src NodeID, layerID LayerID) *ackMsg {
	return &ackMsg{
		SrcID:   src,
		LayerID: layerID,
	}
}

func (m *ackMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *ackMsg) Type() MsgType {
	return MsgTypeAck
}

func (m *ackMsg) Payload() []byte {
	data, _ := json.Marshal(m.LayerID)
	return data
}

func (m *ackMsg) String() string {
	return fmt.Sprintf("%v: %v", m.SrcID, m.LayerID)
}

// retransmitMsg
type retransmitMsg struct {
	SrcID   NodeID
	LayerID LayerID
	DestID  NodeID
}

func NewRetransmitMsg(src NodeID, layerID LayerID, destID NodeID) *retransmitMsg {
	return &retransmitMsg{
		SrcID:   src,
		LayerID: layerID,
		DestID:  destID,
	}
}

func (m *retransmitMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *retransmitMsg) Type() MsgType {
	return MsgTypeRetransmit
}

func (m *retransmitMsg) String() string {
	return fmt.Sprintf("from %v: layer %v, to %v", m.SrcID, m.LayerID, m.DestID)
}

// layerMsg
type layerMsg struct {
	SrcID     NodeID
	LayerID   LayerID
	LayerData LayerData
}

// NewLayerMsg creates a new layerMsg. If the layer is not in memory, it fetches the file from the disk.
func NewLayerMsg(src NodeID, layerID LayerID, layerSrc *LayerSrc) *layerMsg {
	layerData, err := layerSrc.Read()
	if err != nil {
		log.Error().Err(err).Msg("failed to get layer")
	}

	return &layerMsg{
		SrcID:     src,
		LayerID:   layerID,
		LayerData: *layerData,
	}
}

func (m *layerMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *layerMsg) Type() MsgType {
	return MsgTypeLayer
}

func (m *layerMsg) Payload() []byte {
	return m.LayerData
}

func (m *layerMsg) String() string {
	return fmt.Sprintf("from %v: layer %v", m.SrcID, m.LayerID)
}

// startupMsg for start the inference engine on the receiver.
type startupMsg struct {
	SrcID NodeID
}

func NewStartupMsg(src NodeID) *startupMsg {
	return &startupMsg{
		SrcID: src,
	}
}

func (m *startupMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *startupMsg) Type() MsgType {
	return MsgTypeStartup
}

func (m *startupMsg) Payload() []byte {
	return nil
}

func (m *startupMsg) String() string {
	return fmt.Sprintf("from %v: startup", m.SrcID)
}

// SimpleMsg for testing.
type SimepleMsg struct {
	SrcAddr    string
	PayloadStr string
}

func NewSimpleMsg(src string, payload string) *SimepleMsg {
	return &SimepleMsg{
		SrcAddr:    src,
		PayloadStr: payload,
	}
}

func (m *SimepleMsg) Src() string {
	return m.SrcAddr
}

func (m *SimepleMsg) Type() MsgType {
	return MsgTypeSimple
}

func (m *SimepleMsg) Payload() []byte {
	return []byte(m.PayloadStr)
}

func (m *SimepleMsg) String() string {
	return fmt.Sprintf("%s: %s", m.SrcAddr, m.PayloadStr)
}

// TransportMsg for sending messages in TCP.
type TransportMsg struct {
	Type    MsgType         `json:"type"`
	Src     string          `json:"src"`
	Payload json.RawMessage `json:"payload"`
}

// decodeMsg decodes a Message from TransportMsg.
func decodeMsg(m TransportMsg) (Message, error) {
	switch m.Type {
	case MsgTypeAnnounce:
		return unmarshalRawMsg[*announceMsg](m.Payload)
	case MsgTypeAck:
		return unmarshalRawMsg[*ackMsg](m.Payload)
	case MsgTypeLayer:
		return unmarshalRawMsg[*layerMsg](m.Payload)
	case MsgTypeRetransmit:
		return unmarshalRawMsg[*retransmitMsg](m.Payload)
	case MsgTypeStartup:
		return unmarshalRawMsg[*startupMsg](m.Payload)
	case MsgTypeSimple:
		return unmarshalRawMsg[*SimepleMsg](m.Payload)
	default:
		return nil, fmt.Errorf("unknown MsgType: %d", m.Type)
	}
}

func unmarshalRawMsg[T Message](raw json.RawMessage) (T, error) {
	var v T
	err := json.Unmarshal(raw, &v)
	return v, err
}
