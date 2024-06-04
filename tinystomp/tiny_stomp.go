package tinystomp

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/valyala/bytebufferpool"
)

type Conn struct {
	Host string
	User string
	Pass string

	nc net.Conn
	br *bufio.Reader
	bw *bufio.Writer
	bb *bytebufferpool.ByteBuffer
	l  sync.Mutex

	responsePool sync.Pool
	errorCh      chan error
	connectedCh  chan struct{}
}

type Response struct {
	Name    string
	Headers map[string]string
	Body    []byte
}

func NewConn() *Conn {
	return &Conn{
		bb: bytebufferpool.Get(),
		responsePool: sync.Pool{
			New: func() any {
				return &Response{
					Headers: make(map[string]string),
					Body:    make([]byte, 0, 1024),
				}
			},
		},
		errorCh:     make(chan error, 1),
		connectedCh: make(chan struct{}),
	}
}

func (c *Conn) Dial(addr string) (err error) {
	c.nc, err = net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	c.br = bufio.NewReader(c.nc)
	c.bw = bufio.NewWriter(c.nc)
	go c.readLoop()
	return c.Connect()
}

func (c *Conn) Connect() error {
	c.l.Lock()
	defer c.l.Unlock()
	_, err := c.bw.WriteString(
		"CONNECT\r\n" +
			"accept-version:1.0,1.1,2.0\r\n" +
			"host:" + c.Host + "\r\n" +
			"login:" + c.User + "\r\n" +
			"passcode:" + c.Pass + "\r\n\r\n" + "\x00")
	if err != nil {
		return err
	}
	err = c.bw.Flush()
	if err != nil {
		return err
	}

	select {
	case <-c.connectedCh:
		return nil
	case err = <-c.errorCh:
		return err
	}
}

func (c *Conn) Send(destination, contentType string, body []byte, headers map[string]string) error {
	c.l.Lock()
	defer c.l.Unlock()
	<-c.connectedCh

	_, err := c.bw.WriteString("SEND\r\n")
	if err != nil {
		return err
	}
	c.bw.WriteString("destination:" + destination + "\r\n")
	c.bw.WriteString("content-length:" + strconv.Itoa(len(body)) + "\r\n")
	if contentType != "" {
		c.bw.WriteString("content-type:" + contentType + "\r\n")
	}

	for key, value := range headers {
		_, err := c.bw.WriteString(key + ":" + value + "\r\n")
		if err != nil {
			return err
		}
	}

	_, err = c.bw.WriteString("\r\n")
	if err != nil {
		return err
	}

	_, err = c.bw.Write(body)
	if err != nil {
		return err
	}

	_, err = c.bw.WriteRune('\x00')
	if err != nil {
		return err
	}

	err = c.bw.Flush()
	if err != nil {
		return err
	}

	select {
	case err := <-c.errorCh:
		return err
	default:
		return nil
	}
}

func (c *Conn) readLoop() {
	for {
		resp, err := c.readResponse()
		if err != nil {
			c.errorCh <- err
			return
		}
		if resp.Name == "ERROR" {
			c.errorCh <- fmt.Errorf("received ERROR frame from server %s", string(resp.Body))
			return
		}
		if resp.Name == "CONNECTED" {
			close(c.connectedCh)
			continue
		}
	}
}

func (c *Conn) putResponse(resp *Response) {
	for k := range resp.Headers {
		delete(resp.Headers, k)
	}
	resp.Body = resp.Body[:0]
	c.responsePool.Put(resp)
}

func (c *Conn) readResponse() (resp *Response, err error) {
	resp = c.responsePool.Get().(*Response)

	resp.Name, err = c.readLine()
	if err != nil {
		return nil, err
	}

	for {
		line, err := c.readLine()
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			break
		}
		idxColon := strings.IndexRune(line, ':')
		if idxColon < 0 {
			return nil, errors.New("malformed header line")
		}
		key := line[:idxColon]
		value := line[idxColon+1:]
		resp.Headers[key] = strings.TrimSpace(value)
	}

	err = c.readBody(resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Conn) readLine() (string, error) {
	line, err := c.br.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func (c *Conn) readBody(resp *Response) error {
	c.bb.Reset()
	for {
		b, err := c.br.ReadByte()
		if err != nil {
			return err
		}
		if b == 0 {
			break
		}
		c.bb.WriteByte(b)
	}
	resp.Body = append(resp.Body[:0], c.bb.Bytes()...)
	return nil
}

func (c *Conn) Disconnect() error {
	c.l.Lock()
	defer c.l.Unlock()
	c.bw.WriteString("DISCONNECT\r\n\x00")
	c.bw.Flush()
	c.nc.Close()
	return nil
}
