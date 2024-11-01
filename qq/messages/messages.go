package messages

import (
	"encoding/binary"

	"github.com/google/uuid"
)

type Result struct {
	SessionID uuid.UUID
	Prod      bool
	Body      []byte
}

func (m *Result) Unmarshal(b []byte) error {
	m.SessionID = uuid.UUID(b[0:16])
	m.Prod = b[16] == 1
	bodyLen := binary.LittleEndian.Uint32(b[17:21])
	m.Body = b[21 : 21+bodyLen]
	return nil
}

func (m *Result) Marshal(b []byte) []byte {
	b = append(b, m.SessionID[:]...)
	if m.Prod {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.Body)))
	b = append(b, m.Body...)
	return b
}

type Stat struct {
	SessionID uuid.UUID
	Type      string
	Body      []byte
}

func (s *Stat) Unmarshal(b []byte) error {
	s.SessionID = uuid.UUID(b[0:16])
	s.Type = string(b[16:20])
	bodyLen := binary.LittleEndian.Uint32(b[20:24])
	s.Body = b[24 : 24+bodyLen]
	return nil
}

func (s *Stat) Marshal(b []byte) []byte {
	b = b[:0]
	b = append(b, s.SessionID[:]...)
	if len(s.Type) != 4 {
		panic("wrong type length")
	}
	b = append(b, []byte(s.Type)...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s.Body)))
	b = append(b, s.Body...)
	return b
}
