package fast

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const (
	IDLogin = 1 + iota
	IDResponse
	IDPush
)

type Packet interface {
	io.WriterTo
	io.ReaderFrom
	ID() uint8
	ExpectReply() bool
}

type LoginPacket struct {
	SessionID uuid.UUID
	Password  string
}

func (l *LoginPacket) WriteTo(w io.Writer) (int64, error) {
	if _, err := w.Write(l.SessionID[:]); err != nil {
		return 0, err
	}
	binary.Write(w, binary.LittleEndian, uint8(len(l.Password)))
	_, err := w.Write([]byte(l.Password))
	return int64(16 + 1 + len(l.Password)), err
}

func (l *LoginPacket) ReadFrom(r io.Reader) (int64, error) {
	if _, err := io.ReadFull(r, l.SessionID[:]); err != nil {
		return 0, err
	}
	var passwordLen uint8
	if err := binary.Read(r, binary.LittleEndian, &passwordLen); err != nil {
		return 0, err
	}
	if passwordLen > 128 {
		return 0, fmt.Errorf("password too long %d", passwordLen)
	}
	var password = make([]byte, passwordLen)
	if _, err := io.ReadFull(r, password); err != nil {
		return 0, err
	}
	l.Password = string(password)
	return 16 + 1 + int64(passwordLen), nil
}

func (LoginPacket) ID() uint8 {
	return IDLogin
}

func (LoginPacket) ExpectReply() bool {
	return true
}

type PushPacket struct {
	Data []byte
}

func (l *PushPacket) WriteTo(w io.Writer) (int64, error) {
	binary.Write(w, binary.LittleEndian, uint32(len(l.Data)))
	_, err := w.Write(l.Data)
	return 4 + int64(len(l.Data)), err
}

func (l *PushPacket) ReadFrom(r io.Reader) (int64, error) {
	var dataLen uint32
	if err := binary.Read(r, binary.LittleEndian, &dataLen); err != nil {
		return 0, err
	}
	if dataLen > 0x100000 {
		return 0, fmt.Errorf("data too long %d", dataLen)
	}
	var data = make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, err
	}
	l.Data = data
	return 4 + int64(dataLen), nil
}

func (PushPacket) ID() uint8 {
	return IDPush
}

func (PushPacket) ExpectReply() bool {
	return false
}

type ResponsePacket struct {
	err error
}

func (l *ResponsePacket) WriteTo(w io.Writer) (int64, error) {
	if l.err == nil {
		w.Write([]byte{0, 0, 0, 0})
		return 4, nil
	}
	errStr := l.err.Error()
	binary.Write(w, binary.LittleEndian, uint32(len(errStr)))
	_, err := w.Write([]byte(errStr))
	return int64(4 + len(errStr)), err
}

func (l *ResponsePacket) ReadFrom(r io.Reader) (int64, error) {
	var errLen uint32
	if err := binary.Read(r, binary.LittleEndian, &errLen); err != nil {
		return 0, err
	}
	if errLen == 0 {
		l.err = nil
		return 4, nil
	}
	var errStr = make([]byte, errLen)
	if _, err := io.ReadFull(r, errStr); err != nil {
		return 0, err
	}
	l.err = errors.New(string(errStr))
	return int64(errLen) + 4, nil
}

func (ResponsePacket) ID() uint8 {
	return IDResponse
}

func (ResponsePacket) ExpectReply() bool {
	return false
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
	sendBuf      bytes.Buffer
	transactions sync.Map
	pushes       chan []byte
	stopChan     chan struct{}
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
			q.conn.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) // empty packet for ping
		}
	}()

	transactionID, err := q.writePacket(&LoginPacket{
		SessionID: q.sessionId,
		Password:  username,
	}, true)
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

func (q *PushClient) writePacket(packet Packet, locked bool) (int64, error) {
	// serialize packet
	q.sendBuf.Reset()
	q.sendBuf.WriteByte(packet.ID())
	q.sendBuf.Write([]byte{0, 0, 0, 0})
	transactionID := rand.Int64()
	binary.Write(&q.sendBuf, binary.LittleEndian, transactionID)
	packetLen, err := packet.WriteTo(&q.sendBuf)
	if err != nil {
		return transactionID, err
	}
	binary.LittleEndian.PutUint32(q.sendBuf.Bytes()[1:], uint32(packetLen))

	// register reply
	if packet.ExpectReply() {
		m := &sync.Mutex{}
		m.Lock()
		q.transactions.Store(transactionID, m)
	}

	// write
	if locked {
		q.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_, err = q.sendBuf.WriteTo(q.conn)
	} else {
		q.useConn(func(c net.Conn) error {
			c.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = q.sendBuf.WriteTo(c)
			return err
		})
	}
	if err != nil {
		return 0, err
	}

	return transactionID, err
}

func (q *PushClient) readResponse(transactionID int64) (Packet, error) {
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
	for {
		q.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		packet, transactionID, err := readPacket(q.conn)
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
	q.connLock.Lock()
	defer q.connLock.Unlock()
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
	}
	return err
}

func (c *PushClient) pusher() {
	var buf bytes.Buffer
	batch := 0
	flush := func() {
		if batch == 0 {
			return
		}
	retry:
		err := c.useConn(func(c net.Conn) error {
			c.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err := c.Write(buf.Bytes())
			buf.Reset()
			return err
		})
		if err != nil {
			logrus.Errorf("Push %s", err)
			time.Sleep(1 * time.Second)
			goto retry
		}
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case data := <-c.pushes:
			if err := serializePacket(&buf, &PushPacket{Data: data}, 0); err != nil {
				panic(err)
			}
			batch++
			if batch >= 100 {
				flush() // Flush when batch size reaches 20
			}
		case <-ticker.C:
			flush() // Flush at regular intervals even if batch < 20
		case <-c.stopChan:
			flush() // Final flush on stop
			return
		}
	}
}

// push data
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
			logrus.Errorf("Failed to accept connection: %v", err)
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
	for {
		packet, transactionID, err := readPacket(br)
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
				var buf bytes.Buffer
				if err := serializePacket(&buf, &ResponsePacket{
					err: fmt.Errorf("Unauthorized"),
				}, transactionID); err != nil {
					logrus.Error(err)
					return
				}
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if _, err := conn.Write(buf.Bytes()); err != nil {
					logrus.Errorf("failed to send login response: %v", err)
				}
				return
			}

			sessionID = packet.SessionID
			loggedIn = true

			var buf bytes.Buffer
			err := serializePacket(&buf, &ResponsePacket{}, transactionID)
			if err != nil {
				logrus.Error(err)
				return
			}
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err = conn.Write(buf.Bytes()); err != nil {
				logrus.Errorf("failed to send login response: %v", err)
			}
			continue
		}

		switch packet := packet.(type) {
		case *PushPacket:
			s.Push <- Push{Session: sessionID, Data: packet.Data}
		default:
			logrus.Errorf("Received unsupported packet ID: %d", packet.ID())
		}
		if err != nil {
			logrus.Errorf("Error handling packet: %v", err)
			return
		}
	}
}

func readPacket(r io.Reader) (Packet, int64, error) {
	var header [1 + 4 + 8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, err
	}
	if header[0] == 0 { // ping
		return nil, 0, nil
	}
	packet := packetById(header[0])
	if packet == nil {
		return nil, 0, fmt.Errorf("packet id 0x%x invalid", header[0])
	}
	packetLen := binary.LittleEndian.Uint32(header[1:])
	transactionID := binary.LittleEndian.Uint64(header[5:])
	if packetLen > 0x100000 {
		return nil, 0, fmt.Errorf("packet too large")
	}
	packetBuf := make([]byte, packetLen)
	if _, err := io.ReadFull(r, packetBuf); err != nil {
		return nil, 0, err
	}
	if _, err := packet.ReadFrom(bytes.NewReader(packetBuf)); err != nil {
		return nil, 0, err
	}
	return packet, int64(transactionID), nil
}

func serializePacket(buf *bytes.Buffer, packet Packet, transactionID int64) error {
	buf.Write([]byte{packet.ID()})
	o := buf.Len()
	buf.Write(make([]byte, 4)) // reserve space for length
	binary.Write(buf, binary.LittleEndian, transactionID)
	packetLen, err := packet.WriteTo(buf)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(buf.Bytes()[o:], uint32(packetLen))
	return err
}
