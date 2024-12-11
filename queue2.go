package fast

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const (
	IDLogin = 1 + iota
	IDResponse
	IDPush
)

var respondyPackets = []bool{
	false, true, false, false,
}

type Packet interface {
	Marshal(b []byte) []byte
	Unmarshal(b []byte)
}

type LoginPacket struct {
	SessionID uuid.UUID
	Password  string
}

func s2b(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func (l *LoginPacket) Marshal(b []byte) []byte {
	b = append(b, l.SessionID[:]...)
	b = append(b, uint8(len(l.Password)))
	b = append(b, s2b(l.Password)...)
	return b
}

func (l *LoginPacket) Unmarshal(b []byte) {
	copy(l.SessionID[:], b[:16])
	passwordLen := b[16]
	l.Password = string(b[17 : 17+passwordLen])
}

type PushPacket struct {
	Data []byte
}

func (l *PushPacket) Marshal(b []byte) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(l.Data)))
	b = append(b, l.Data...)
	return b
}

func (l *PushPacket) Unmarshal(b []byte) {
	dataLen := binary.LittleEndian.Uint32(b)
	l.Data = b[4 : 4+dataLen]
}

type ResponsePacket struct {
	err error
}

func (l *ResponsePacket) Marshal(b []byte) []byte {
	if l.err == nil {
		b = append(b, []byte{0, 0, 0, 0}...)
		return b
	}
	errStr := l.err.Error()
	b = binary.LittleEndian.AppendUint32(b, uint32(len(errStr)))
	b = append(b, s2b(errStr)...)
	return b
}

func (l *ResponsePacket) Unmarshal(b []byte) {
	errLen := binary.LittleEndian.Uint32(b)
	if errLen == 0 {
		l.err = nil
		return
	}
	l.err = errors.New(string(b[4 : 4+errLen]))
}

func packetById(id uint8) Packet {
	switch id {
	case IDLogin:
		return &LoginPacket{}
	case IDResponse:
		return &ResponsePacket{}
	case IDPush:
		return &PushPacket{}
	default:
		return nil
	}
}

type PushClient struct {
	url          *url.URL
	sessionId    uuid.UUID
	conn         net.Conn
	connLock     sync.Mutex
	pingTicker   *time.Ticker
	readErr      atomic.Pointer[error]
	transactions sync.Map
	pushes       chan []byte
	stopChan     chan struct{}

	transactionID uint16
}

func NewPushClient(serverUrl string, sessionId uuid.UUID) (q *PushClient, err error) {
	q = &PushClient{
		pushes:   make(chan []byte, 100),
		stopChan: make(chan struct{}),
	}
	q.url, err = url.Parse(serverUrl)
	if err != nil {
		return nil, err
	}
	q.sessionId = sessionId
	go q.pusher()

	return q, nil
}

// connect and login
func (q *PushClient) connect() (err error) {
	q.transactions = sync.Map{}
	q.readErr.Store(nil)
	username := q.url.User.Username()
	q.conn, err = net.Dial("tcp", q.url.Host)
	if err != nil {
		return err
	}

	go q.readLoop()
	if q.pingTicker != nil {
		q.pingTicker.Stop()
	}
	q.pingTicker = time.NewTicker(9 * time.Second)
	go func() {
		for range q.pingTicker.C {
			q.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			q.conn.Write([]byte{0, 0, 0, 0, 0, 0, 0}) // empty packet for ping
		}
	}()

	transactionID, err := q.writePacket(IDLogin, &LoginPacket{
		SessionID: q.sessionId,
		Password:  username,
	})
	if err != nil {
		return err
	}
	resp, err := q.readResponse(transactionID)
	if err != nil {
		return err
	}
	response, ok := resp.(*ResponsePacket)
	if !ok {
		return fmt.Errorf("unexpected packet %v", resp)
	}
	if response.err != nil {
		return response.err
	}
	return nil
}

func (q *PushClient) writePacket(id uint8, packet Packet) (transactionID uint16, err error) {
	q.connLock.Lock()
	defer q.connLock.Unlock()

	q.transactionID++
	transactionID = q.transactionID

	// serialize packet
	var sendBuf []byte
	sendBuf = serializePacket(sendBuf, id, packet, transactionID)

	// register reply
	if respondyPackets[id] {
		m := &sync.Mutex{}
		m.Lock()
		q.transactions.Store(transactionID, m)
	}

	q.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = q.conn.Write(sendBuf)
	if err != nil {
		return 0, err
	}

	return transactionID, err
}

func (q *PushClient) readResponse(transactionID uint16) (Packet, error) {
	// wait
	m, ok := q.transactions.Load(transactionID)
	if !ok {
		return nil, fmt.Errorf("no transaction with id %x", transactionID)
	}
	switch m := m.(type) {
	case *sync.Mutex:
		m.Lock()
	case Packet:
		q.transactions.Delete(transactionID)
		return m, nil
	}
	if err := q.readErr.Load(); err != nil {
		return nil, *err
	}
	// take packet
	t, _ := q.transactions.LoadAndDelete(transactionID)
	return t.(Packet), nil
}

func (q *PushClient) readLoop() {
	defer func() {
		q.transactions.Range(func(key, value any) bool {
			m, _ := q.transactions.LoadAndDelete(key)
			m.(*sync.Mutex).Unlock()
			return true
		})
	}()

	var recvBuf []byte
	for {
		q.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		packet, transactionID, err := readPacket(q.conn, &recvBuf, nil)
		if err != nil {
			q.readErr.Store(&err)
			return
		}
		if packet == nil {
			continue
		}
		m, ok := q.transactions.Swap(transactionID, packet)
		if !ok {
			err = fmt.Errorf("unexpected %v packet", packet)
			q.readErr.Store(&err)
			return
		}
		m.(*sync.Mutex).Unlock()
	}
}

func (q *PushClient) useConn(fn func(c net.Conn) error) (err error) {
	if q.conn == nil {
		b := backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(1*time.Second),
			backoff.WithMaxInterval(60*time.Second),
		)
	retry:
		if err = q.connect(); err != nil {
			bo := b.NextBackOff()
			logrus.Errorf("Failed to Connect: %s, waiting %s to retry", err, bo.Truncate(time.Second))
			time.Sleep(bo)
			goto retry
		}
	}
	err = fn(q.conn)
	if err != nil {
		q.conn.Close()
		q.pingTicker.Stop()
		q.conn = nil
		return q.useConn(fn)
	}
	return err
}

func (c *PushClient) pusher() {
	var buf []byte

	batch := 0
	flush := func() {
		if batch == 0 {
			return
		}
	retry:
		err := c.useConn(func(c net.Conn) error {
			c.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err := c.Write(buf)
			buf = buf[:0]
			return err
		})
		if err != nil {
			logrus.Errorf("Push %s", err)
			time.Sleep(1 * time.Second)
			goto retry
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var pp PushPacket

	for {
		select {
		case data := <-c.pushes:
			pp.Data = data
			buf = serializePacket(buf, IDPush, &pp, 0)
			batch++
			if batch >= 100 {
				flush() // Flush when batch size reaches 100
			}
		case <-ticker.C:
			flush() // Flush at regular intervals even if batch < 100
		case <-c.stopChan:
			flush() // Final flush on stop
			return
		}
	}
}

func (q *PushClient) Push(data []byte) error {
	q.pushes <- data
	return nil
}

type Push struct {
	Session uuid.UUID
	Data    []byte
}

type PushServer struct {
	listener net.Listener
	Password string
	Push     chan Push
	pushers  atomic.Int64
	closing  bool
}

func NewPushServer(address string) (*PushServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	server := &PushServer{
		listener: listener,
		Push:     make(chan Push, 1000),
	}

	return server, nil
}

func (s *PushServer) Address() string {
	return s.listener.Addr().String()
}

func (s *PushServer) Run() error {
	s.acceptLoop()
	return nil
}

func (s *PushServer) Close() error {
	s.closing = true
	return s.listener.Close()
}

func (s *PushServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}

		logrus.Infof("Accepted new connection from %v", conn.RemoteAddr())
		go s.handleConnection(conn)
	}
}

func (s *PushServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	s.pushers.Add(1)
	defer func() {
		if s.closing && s.pushers.Add(-1) == 0 {
			close(s.Push)
		}
	}()

	var (
		sessionID uuid.UUID
		loggedIn  bool
	)

	pingTicker := time.NewTicker(9 * time.Second)
	defer pingTicker.Stop()
	go func() {
		for range pingTicker.C {
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			conn.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) // empty packet for ping
		}
	}()

	br := bufio.NewReader(conn)

	var packets = map[uint8]Packet{
		IDLogin:    &LoginPacket{},
		IDPush:     &PushPacket{},
		IDResponse: &ResponsePacket{},
	}

	var recvBuf []byte

	for {
		packet, transactionID, err := readPacket(br, &recvBuf, packets)
		if err != nil {
			logrus.Errorf("Failed to read packet: %v", err)
			return
		}
		if packet == nil { // ping
			continue
		}

		if !loggedIn {
			packet, ok := packet.(*LoginPacket)
			if !ok {
				return
			}

			logrus.Infof("Received login packet with session ID %s", packet.SessionID)
			if packet.Password != s.Password {
				buf := serializePacket(nil, IDResponse, &ResponsePacket{
					err: fmt.Errorf("Unauthorized"),
				}, transactionID)
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := conn.Write(buf); err != nil {
					logrus.Errorf("failed to send login response: %v", err)
				}
				return
			}

			sessionID = packet.SessionID
			loggedIn = true

			buf := serializePacket(nil, IDResponse, &ResponsePacket{}, transactionID)
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err = conn.Write(buf); err != nil {
				logrus.Errorf("failed to send login response: %v", err)
			}
			continue
		}

		switch packet := packet.(type) {
		case *PushPacket:
			s.Push <- Push{Session: sessionID, Data: packet.Data}
		default:
			logrus.Errorf("Received unsupported packet: %t", packet)
		}
	}
}

func readPacket(r io.Reader, recvBufP *[]byte, packets map[uint8]Packet) (Packet, uint16, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, err
	}
	if header[0] == 0 { // ping
		return nil, 0, nil
	}

	var packet Packet
	if packets == nil {
		packet = packetById(header[0])
	} else {
		packet = packets[header[0]]
	}
	if packet == nil {
		return nil, 0, fmt.Errorf("packet id 0x%x invalid", header[0])
	}
	packetLen := int(binary.LittleEndian.Uint32(header[2:]))
	transactionID := uint16(binary.LittleEndian.Uint16(header[6:]))
	if packetLen == 0 {
		return packet, transactionID, nil
	}
	if packetLen > 0x100000 {
		return nil, 0, fmt.Errorf("packet too large")
	}

	var recvBuf []byte
	if recvBufP != nil {
		if cap(*recvBufP) >= int(packetLen) {
			*recvBufP = (*recvBufP)[:packetLen]
		} else {
			*recvBufP = (*recvBufP)[:cap(*recvBufP)]
			*recvBufP = append(*recvBufP, make([]byte, packetLen-len(*recvBufP))...)
		}
		if _, err := io.ReadFull(r, *recvBufP); err != nil {
			return nil, 0, err
		}
		recvBuf = *recvBufP
	} else {
		recvBuf = make([]byte, packetLen)
		if _, err := io.ReadFull(r, recvBuf); err != nil {
			return nil, 0, err
		}
	}
	packet.Unmarshal(recvBuf)
	return packet, transactionID, nil
}

func serializePacket(b []byte, id uint8, packet Packet, transactionID uint16) []byte {
	b = append(b, id)
	b = append(b, []byte{0, 0, 0, 0, 0}...)
	b = binary.LittleEndian.AppendUint16(b, transactionID)
	b = packet.Marshal(b)
	packetLen := len(b) - 8
	binary.LittleEndian.PutUint32(b[2:], uint32(packetLen))
	return b
}
