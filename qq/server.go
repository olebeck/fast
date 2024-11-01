package qq

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"slices"
	"sync"

	"github.com/olebeck/fast/qq/protocol"
	"github.com/valyala/bytebufferpool"
)

type Sub struct {
	id uint32
	sc *clientConn
}

func (s *Sub) publish(eventData []byte) error {
	return s.sc.writePacket(0, &protocol.SubscriptionEvent{
		SubscriptionID: s.id,
		Data:           eventData,
	})
}

type clientConn struct {
	conn net.Conn
	bufr bufio.Reader
	mu   sync.Mutex

	loggedIn bool
	server   *Server
}

type Server struct {
	subs map[string][]Sub
	mu   sync.RWMutex

	address  string
	password string
}

func NewServer(address, password string) *Server {
	return &Server{
		address:  address,
		password: password,
		subs:     make(map[string][]Sub),
	}
}

func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	fmt.Printf("Listening on %s\n", ln.Addr())
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		connectionCount.Inc()
		activeConnections.Inc()

		sc := clientConn{
			conn:   conn,
			bufr:   *bufio.NewReader(conn),
			server: s,
		}
		go func(conn net.Conn) {
			defer conn.Close()
			defer activeConnections.Dec()

			var hello = make([]byte, 11)
			_, err = io.ReadFull(&sc.bufr, hello)
			if err != nil {
				fmt.Printf("read hello %s: %s\n", conn.RemoteAddr(), err)
				return
			}
			helloS := string(hello)
			if helloS != "hello im qq" {
				return
			}

			err := sc.handle()
			if err != nil {
				fmt.Printf("%s: %s\n", conn.RemoteAddr(), err)
			}
		}(conn)
	}
}

func (s *Server) addSub(queue string, sub Sub) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	subscriptionCount.WithLabelValues(queue).Add(1)
	s.subs[queue] = append(s.subs[queue], sub)
	return nil
}

func (s *Server) handleEvent(queue string, eventData []byte) error {
	s.mu.RLock()
	subs, ok := s.subs[queue]
	s.mu.RUnlock()
retry:
	if !ok || len(subs) == 0 {
		noReceiverCount.WithLabelValues(queue).Inc()
		return nil
	}
	sub := subs[rand.Intn(len(subs))]
	err := sub.publish(eventData)
	if err != nil {
		s.mu.Lock()
		subs = slices.DeleteFunc(subs, func(ss Sub) bool {
			return ss.id == sub.id
		})
		subscriptionCount.WithLabelValues(queue).Add(-1)
		s.subs[queue] = subs
		s.mu.Unlock()
		goto retry
	}
	successfulPublishes.WithLabelValues(queue).Inc()
	return nil
}

func (sc *clientConn) handlePacket(reId uint32, packetID string, packetData []byte) error {
	requestCount.WithLabelValues(packetID).Inc()
	if !sc.loggedIn {
		if packetID != "logi" {
			if err := sc.writePacket(0, &protocol.Error{
				Err: "Not Logged In",
			}); err != nil {
				return err
			}
		}

		var req protocol.LoginRequest
		if err := req.Unmarshal(packetData); err != nil {
			return err
		}
		sc.loggedIn = req.Password == sc.server.password
		if err := sc.writePacket(reId, &protocol.LoginResponse{
			Success: sc.loggedIn,
		}); err != nil {
			return err
		}
		return nil
	}

	switch packetID {
	case "subs":
		var req protocol.SubscribeRequest
		if err := req.Unmarshal(packetData); err != nil {
			return err
		}
		sub := Sub{sc: sc, id: rand.Uint32()}
		sc.server.addSub(req.Queue, sub)
		sc.writePacket(reId, &protocol.SubscribeResponse{
			SubscriptionID: sub.id,
		})

	case "publ":
		var req protocol.Publish
		if err := req.Unmarshal(packetData); err != nil {
			return err
		}
		if err := sc.server.handleEvent(req.Queue, req.Data); err != nil {
			return err
		}
	}
	return nil
}

func (sc *clientConn) handle() error {
	for {
		reId, packetData, err := readPacket(&sc.bufr)
		if err != nil {
			return err
		}
		packetID := string(packetData[0:4])
		packetData = packetData[4:]
		err = sc.handlePacket(reId, packetID, packetData)
		if err != nil {
			errorCount.WithLabelValues("type", packetID).Inc()
			return err
		}
	}
}

func (sc *clientConn) writePacket(reId uint32, packet protocol.Packet) error {
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	bb.Write([]byte{0, 0, 0, 0})
	bb.B = binary.LittleEndian.AppendUint32(bb.B, uint32(reId))
	bb.B = packet.Marshal(bb.B)
	binary.LittleEndian.PutUint32(bb.B, uint32(len(bb.B)-8))

	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, err := sc.conn.Write(bb.B)
	return err
}

func readPacket(r io.Reader) (reId uint32, packetData []byte, err error) {
	// read length
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return 0, nil, err
	}
	if length > 0xffff {
		return 0, nil, fmt.Errorf("packet too long")
	}

	// read response id
	var reID uint32
	if err := binary.Read(r, binary.LittleEndian, &reID); err != nil {
		return 0, nil, err
	}

	// read body
	packetData = make([]byte, length)
	if _, err := io.ReadFull(r, packetData); err != nil {
		return 0, nil, err
	}
	return reID, packetData, nil
}
