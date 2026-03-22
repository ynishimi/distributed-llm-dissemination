package distributor

import (
	"encoding/json"
	"fmt"
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
	MsgTypeFlowRetransmit
	MsgTypeClientReq
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
	SrcID    NodeID
	LayerID  LayerID
	location LayerLocation
}

func NewAckMsg(src NodeID, layerID LayerID, layerLocation LayerLocation) *ackMsg {
	return &ackMsg{
		SrcID:    src,
		LayerID:  layerID,
		location: layerLocation,
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
	return fmt.Sprintf("from %v: layer %v, to %v, ", m.SrcID, m.LayerID, m.DestID)
}

// flowRetransmitMsg
type flowRetransmitMsg struct {
	SrcID    NodeID
	LayerID  LayerID
	DestID   NodeID
	DataSize int64
	Offset   int64
}

func NewFlowRetransmitMsg(src NodeID, layerID LayerID, destID NodeID, dataSize int64, offset int64) *flowRetransmitMsg {
	return &flowRetransmitMsg{
		SrcID:    src,
		LayerID:  layerID,
		DestID:   destID,
		DataSize: dataSize,
		Offset:   offset,
	}
}

func (m *flowRetransmitMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *flowRetransmitMsg) Type() MsgType {
	return MsgTypeFlowRetransmit
}

func (m *flowRetransmitMsg) String() string {
	return fmt.Sprintf("from %v: layer %v, to %v, size: %d, offset: %d", m.SrcID, m.LayerID, m.DestID, m.DataSize, m.Offset)
}

// layerMsg
type layerMsg struct {
	SrcID    NodeID
	LayerID  LayerID
	LayerSrc LayerSrc
	// SaveDisk stores the flag of if the layer is saved in the disk or not
	// SaveDisk bool
	// limitRate int
}

// NewLayerMsg creates a new layerMsg. If the layer is not in memory, it fetches the file from the disk.
func NewLayerMsg(src NodeID, layerID LayerID, layerSrc LayerSrc) *layerMsg {
	return &layerMsg{
		SrcID:    src,
		LayerID:  layerID,
		LayerSrc: layerSrc,
		// SaveDisk: saveDisk,
		// limitRate: layerSrc.Meta.LimitRate,
	}
}

func (m *layerMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *layerMsg) Type() MsgType {
	return MsgTypeLayer
}

func (m *layerMsg) Payload() []byte {
	return nil
}

func (m *layerMsg) String() string {
	return fmt.Sprintf("from %v: layer %v, location: %v, rate: %v", m.SrcID, m.LayerID, m.LayerSrc.Meta.Location, m.LayerSrc.Meta.LimitRate)
}

// clientReqMsg
type clientReqMsg struct {
	SrcID   NodeID
	LayerID LayerID
	// DestID   NodeID
	SaveDisk bool
}

func NewClientReqMsg(src NodeID, layerID LayerID, SaveDisk bool) *clientReqMsg {
	return &clientReqMsg{src, layerID, SaveDisk}
}

func (m *clientReqMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *clientReqMsg) Type() MsgType {
	return MsgTypeClientReq
}

func (m *clientReqMsg) String() string {
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
	// case MsgTypeLayer:
	// 	return unmarshalRawMsg[*layerMsg](m.Payload)
	case MsgTypeRetransmit:
		return unmarshalRawMsg[*retransmitMsg](m.Payload)
	case MsgTypeFlowRetransmit:
		return unmarshalRawMsg[*flowRetransmitMsg](m.Payload)
	case MsgTypeClientReq:
		return unmarshalRawMsg[*clientReqMsg](m.Payload)
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
