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
	MsgTypeReq
	MsgTypeJob
	MsgTypeProgressReport
	MsgTypeClientReq
	MsgTypeStartup
	MsgTypeSimple
	MsgTypeTransport
	MsgTypeBlock
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
	// location LayerLocation
}

func NewAckMsg(src NodeID, layerID LayerID) *ackMsg {
	return &ackMsg{
		SrcID:   src,
		LayerID: layerID,
		// location: layerLocation,
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

// coordinator -> receiver
type jobMsg struct {
	SrcID NodeID
	jobs  []job
}

func NewJobMsg(srcID NodeID, jobs []job) *jobMsg {
	return &jobMsg{srcID, jobs}
}

func (m *jobMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *jobMsg) Type() MsgType {
	return MsgTypeJob
}

func (m *jobMsg) String() string {
	return fmt.Sprintf("from %v", m.SrcID)
}

// reqMsg
type reqMsg struct {
	// receiver
	SrcID   NodeID
	LayerID LayerID
	// sender
	DestID  NodeID
	BlockID BlockID
	Rate    int64
}

func NewReqMsg(srcID NodeID, layerID LayerID, destID NodeID, blockID BlockID, rate int64) *reqMsg {
	return &reqMsg{
		SrcID:   srcID,
		LayerID: layerID,
		DestID:  destID,
		BlockID: blockID,
		Rate:    rate,
	}
}

func (m *reqMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *reqMsg) Type() MsgType {
	return MsgTypeReq
}

func (m *reqMsg) String() string {
	return fmt.Sprintf("from %v: layer %v, to %v, block: %d, rate: %d", m.SrcID, m.LayerID, m.DestID, m.BlockID, m.Rate)
}

// progressReportMsg
type progressReportMsg struct {
	SrcID NodeID
	// LayerID  LayerID
	// DestID   NodeID
	// DataSize int64
	// Offset   int64
	// Rate     int64
}

func NewProgressReportMsg(src NodeID) *progressReportMsg {
	return &progressReportMsg{
		SrcID: src,
		// LayerID:  layerID,
		// DestID:   destID,
		// DataSize: dataSize,
		// Offset:   offset,
		// Rate:     rate,
	}
}

func (m *progressReportMsg) Src() string {
	return fmt.Sprint(m.SrcID)
}

func (m *progressReportMsg) Type() MsgType {
	return MsgTypeProgressReport
}

func (m *progressReportMsg) String() string {
	return fmt.Sprintf("")
}

// layerMsg
type layerMsg struct {
	SrcID     NodeID
	LayerID   LayerID
	LayerSrc  LayerSrc
	TotalSize int64
	// SaveDisk stores the flag of if the layer is saved in the disk or not
	// SaveDisk bool
	// limitRate int
}

// NewLayerMsg creates a new layerMsg. If the layer is not in memory, it fetches the file from the disk.
func NewLayerMsg(src NodeID, layerID LayerID, layerSrc LayerSrc, totalSize int64) *layerMsg {
	return &layerMsg{
		SrcID:     src,
		LayerID:   layerID,
		LayerSrc:  layerSrc,
		TotalSize: totalSize,
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

// blockMsg
type blockMsg struct {
	*layerMsg
	BlockID BlockID
}

func NewBlockMsg(src NodeID, layerID LayerID, layerSrc LayerSrc, blockID BlockID) *blockMsg {
	return &blockMsg{
		layerMsg: &layerMsg{
			SrcID:    src,
			LayerID:  layerID,
			LayerSrc: layerSrc,
		},
		BlockID: blockID,
	}
}

func (m *blockMsg) Type() MsgType {
	return MsgTypeBlock
}

func (m *blockMsg) String() string {
	return fmt.Sprintf("from %v: layer %v, block: %d, rate: %v", m.SrcID, m.LayerID, m.BlockID, m.LayerSrc.Meta.LimitRate)
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
	case MsgTypeReq:
		return unmarshalRawMsg[*reqMsg](m.Payload)
	case MsgTypeJob:
		return unmarshalRawMsg[*jobMsg](m.Payload)
	case MsgTypeProgressReport:
		return unmarshalRawMsg[*progressReportMsg](m.Payload)
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
