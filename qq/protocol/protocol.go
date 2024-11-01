package protocol

import "encoding/binary"

type Packet interface {
	Marshal([]byte) []byte
	Unmarshal([]byte) error
}

//

type LoginRequest struct {
	Password string
}

func (l *LoginRequest) Marshal(b []byte) []byte {
	b = append(b, []byte("logi")...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(l.Password)))
	b = append(b, []byte(l.Password)...)
	return b
}

func (l *LoginRequest) Unmarshal(b []byte) error {
	passwordLen := binary.LittleEndian.Uint32(b)
	l.Password = string(b[4 : 4+passwordLen])
	return nil
}

//

type LoginResponse struct {
	Success bool
}

func (l *LoginResponse) Marshal(b []byte) []byte {
	if l.Success {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return b
}

func (l *LoginResponse) Unmarshal(b []byte) error {
	l.Success = b[0] == 1
	return nil
}

//

type SubscribeRequest struct {
	Queue string
}

func (s *SubscribeRequest) Marshal(b []byte) []byte {
	b = append(b, []byte("subs")...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s.Queue)))
	b = append(b, []byte(s.Queue)...)
	return b
}

func (s *SubscribeRequest) Unmarshal(b []byte) error {
	queueLen := binary.LittleEndian.Uint32(b)
	s.Queue = string(b[4 : 4+queueLen])
	return nil
}

//

type SubscribeResponse struct {
	SubscriptionID uint32
}

func (s *SubscribeResponse) Marshal(b []byte) []byte {
	b = binary.LittleEndian.AppendUint32(b, s.SubscriptionID)
	return b
}

func (s *SubscribeResponse) Unmarshal(b []byte) error {
	s.SubscriptionID = binary.LittleEndian.Uint32(b)
	return nil
}

//

type Publish struct {
	Queue string
	Data  []byte
}

func (p *Publish) Marshal(b []byte) []byte {
	b = append(b, []byte("publ")...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(p.Queue)))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(p.Data)))
	b = append(b, []byte(p.Queue)...)
	b = append(b, p.Data...)
	return b
}

func (p *Publish) Unmarshal(b []byte) error {
	queueLen := binary.LittleEndian.Uint32(b)
	dataLen := binary.LittleEndian.Uint32(b[4:])
	p.Queue = string(b[8 : 8+queueLen])
	p.Data = b[8+queueLen : 8+queueLen+dataLen]
	return nil
}

//

type Error struct {
	Err string
}

func (l *Error) Marshal(b []byte) []byte {
	b = append(b, []byte("erro")...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(l.Err)))
	b = append(b, []byte(l.Err)...)
	return b
}

func (l *Error) Unmarshal(b []byte) error {
	errLen := binary.LittleEndian.Uint32(b)
	l.Err = string(b[4 : 4+errLen])
	return nil
}

//

type SubscriptionEvent struct {
	SubscriptionID uint32
	Data           []byte
}

func (s *SubscriptionEvent) Marshal(b []byte) []byte {
	b = append(b, []byte("sube")...)
	b = binary.LittleEndian.AppendUint32(b, s.SubscriptionID)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s.Data)))
	b = append(b, s.Data...)
	return b
}

func (s *SubscriptionEvent) Unmarshal(b []byte) error {
	s.SubscriptionID = binary.LittleEndian.Uint32(b)
	dataLen := binary.LittleEndian.Uint32(b[4:])
	s.Data = b[8 : 8+dataLen]
	return nil
}
