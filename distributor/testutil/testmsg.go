package testutil

import "fmt"

type TestMsg struct {
	src     string
	payload string
}

func NewTestMsg(src string, payload string) *TestMsg {
	return &TestMsg{
		src:     src,
		payload: payload,
	}
}

func (m *TestMsg) Src() string {
	return m.src
}

func (m *TestMsg) Payload() []byte {
	return []byte(m.payload)
}

func (m *TestMsg) String() string {
	return fmt.Sprintf("%s: %s", m.src, m.payload)
}
