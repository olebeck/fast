package qq

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"

	"github.com/olebeck/fast/qq/protocol"
	"github.com/valyala/bytebufferpool"
)

type Client struct {
	mu   sync.Mutex
	conn net.Conn
	bufr *bufio.Reader
	err  atomic.Pointer[error]
	rees sync.Map
	subs sync.Map

	address  string
	password string
}

func NewClient(address, password string) *Client {
	return &Client{
		address:  address,
		password: password,
	}
}

func (c *Client) Connect() (err error) {
	if c.conn, err = net.Dial("tcp", c.address); err != nil {
		c.err.Store(&err)
		return err
	}
	c.bufr = bufio.NewReader(c.conn)
	c.conn.Write([]byte("hello im qq"))
	go c.beginReading()
	return c.login()
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type waiter struct {
	mu  sync.Mutex
	res protocol.Packet
	err error
}

func (c *Client) doRequest(req, res protocol.Packet) (err error) {
	reId, err := c.writePacket(req)
	if err != nil {
		return err
	}
	w := &waiter{res: res}
	w.mu.Lock()
	c.rees.Store(reId, w)
	w.mu.Lock()
	return w.err
}

func (c *Client) login() error {
	var resp protocol.LoginResponse
	if err := c.doRequest(&protocol.LoginRequest{
		Password: c.password,
	}, &resp); err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("login Rejected")
	}
	return nil
}

func (c *Client) beginReading() {
	for {
		reID, packetData, err := c.readPacket()
		if err != nil {
			fmt.Printf("error reading packet: %v\n", err)
			c.setErr(err)
			return
		}
		if packetData != nil {
			if reID != 0 {
				err = fmt.Errorf("unexpected packet %s", string(packetData))
				c.setErr(err)
				c.conn.Close()
				return
			}
			if err := c.handleEvent(packetData); err != nil {
				c.setErr(err)
				c.conn.Close()
				return
			}
		}
	}
}

func (c *Client) handleEvent(packetData []byte) error {
	packetID := string(packetData[0:4])
	packetData = packetData[4:]
	switch packetID {
	case "erro":
		var req protocol.Error
		if err := req.Unmarshal(packetData); err != nil {
			return err
		}
		return fmt.Errorf("server: %s", req.Err)

	case "sube":
		var req protocol.SubscriptionEvent
		if err := req.Unmarshal(packetData); err != nil {
			return err
		}
		sub, ok := c.subs.Load(req.SubscriptionID)
		if !ok {
			return fmt.Errorf("unknown sub: %x", req.SubscriptionID)
		}
		subHandler := sub.(func([]byte))
		subHandler(req.Data)
		return nil

	default:
		return fmt.Errorf("unknown packet: %s", string(packetData))
	}
}

// publish a event to a queue
func (c *Client) Publish(queue string, data []byte) error {
	_, err := c.writePacket(&protocol.Publish{
		Queue: queue,
		Data:  data,
	})
	return err
}

// subscribe to a queue, receiving events until error
func (c *Client) Subscribe(queue string, process func(messageData []byte)) error {
	var resp protocol.SubscribeResponse
	if err := c.doRequest(&protocol.SubscribeRequest{
		Queue: queue,
	}, &resp); err != nil {
		return err
	}
	c.subs.Store(resp.SubscriptionID, func(data []byte) {
		process(data)
	})
	return nil
}

func (c *Client) setErr(err error) {
	if errP := c.err.Load(); errP == nil {
		c.err.Store(&err)
	}
}

// u32(len) u32(reId) bytes(data)
func (c *Client) writePacket(packet protocol.Packet) (uint32, error) {
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)

	// serialize the packet (length, reId, packetData)
	reId := rand.Int31()
	bb.Write([]byte{0, 0, 0, 0})
	bb.B = binary.LittleEndian.AppendUint32(bb.B, uint32(reId))
	bb.B = packet.Marshal(bb.B)
	binary.LittleEndian.PutUint32(bb.B, uint32(len(bb.B)-8))

	// write to the connection
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.conn.Write(bb.B)
	if err != nil {
		c.setErr(err)
	}
	return uint32(reId), err
}

// reads a packet from the connection, if its a response it notifies the requester, parses the resposne
func (c *Client) readPacket() (reId uint32, packetData []byte, err error) {

	// read length
	var length uint32
	if err := binary.Read(c.bufr, binary.LittleEndian, &length); err != nil {
		return 0, nil, err
	}
	if length > 0xffff {
		return 0, nil, fmt.Errorf("packet too long")
	}

	// read response id
	var reID uint32
	if err := binary.Read(c.bufr, binary.LittleEndian, &reID); err != nil {
		return 0, nil, err
	}

	// read body
	b := make([]byte, length)
	if _, err := io.ReadFull(c.bufr, b); err != nil {
		return 0, nil, err
	}

	// if this is a response parse it and notify
	ree, ok := c.rees.Load(reID)
	if !ok {
		return reID, b, nil
	}
	w := ree.(*waiter)
	w.err = w.res.Unmarshal(b)
	w.mu.Unlock()
	return 0, nil, w.err
}
