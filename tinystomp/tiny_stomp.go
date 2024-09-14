package tinystomp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

type Conn struct {
	Host string
	User string
	Pass string

	nc net.Conn
	b  *bufio.ReadWriter
	l  sync.Mutex

	errorCh     chan error
	connectedCh chan struct{}
}

type Response struct {
	Name    string
	Headers map[string]string
	Body    []byte
}

func NewConn() *Conn {
	return &Conn{
		errorCh:     make(chan error, 1),
		connectedCh: make(chan struct{}),
	}
}

func (c *Conn) Dial(addr string) (err error) {
	c.nc, err = net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	c.b = bufio.NewReadWriter(bufio.NewReader(c.nc), bufio.NewWriterSize(c.nc, 1400))
	go c.readLoop()
	return c.Connect()
}

func (c *Conn) Connect() error {
	c.l.Lock()
	defer c.l.Unlock()
	_, err := c.b.WriteString(
		"CONNECT\n" +
			"accept-version:1.0,1.1,2.0\n" +
			"host:" + c.Host + "\n" +
			"login:" + c.User + "\n" +
			"passcode:" + c.Pass + "\n\n" + "\x00\n")
	if err != nil {
		return err
	}
	err = c.b.Flush()
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

	_, err := c.b.WriteString("SEND\n")
	if err != nil {
		return err
	}
	c.b.WriteString("destination:" + destination + "\n")
	c.b.WriteString("content-length:" + strconv.Itoa(len(body)) + "\n")
	if contentType != "" {
		c.b.WriteString("content-type:" + contentType + "\n")
	}

	for key, value := range headers {
		_, err := c.b.WriteString(key + ":" + value + "\n")
		if err != nil {
			return err
		}
	}

	_, err = c.b.WriteRune('\n')
	if err != nil {
		return err
	}

	_, err = c.b.Write(body)
	if err != nil {
		return err
	}

	_, err = c.b.WriteString("\x00\n")
	if err != nil {
		return err
	}

	err = c.b.Flush()
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

func (c *Conn) readResponse() (resp *Response, err error) {
	resp = &Response{
		Headers: make(map[string]string),
	}

	resp.Name, err = c.b.ReadString('\n')
	if err != nil {
		return nil, err
	}
	resp.Name = resp.Name[:len(resp.Name)-1]

	var contentLength = -1
	for {
		line, err := c.b.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = line[:len(line)-1]
		if len(line) == 0 {
			break
		}
		idxColon := strings.IndexRune(line, ':')
		if idxColon < 0 {
			return nil, errors.New("malformed header line")
		}
		key := line[:idxColon]
		value := line[idxColon+1:]
		resp.Headers[key] = value
		if key == "content-length" {
			contentLength, err = strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
		}
	}

	if contentLength >= 0 {
		if contentLength > 0 {
			resp.Body = make([]byte, contentLength)
			_, err = io.ReadFull(c.b, resp.Body)
			if err != nil {
				return nil, err
			}
		}
		null, err := c.b.ReadByte()
		if err != nil {
			return nil, err
		}
		if null != 0 {
			return nil, fmt.Errorf("not 0x00 at the end")
		}
	} else {
		resp.Body, err = c.b.ReadBytes('\x00')
		if err != nil {
			return nil, err
		}
		if len(resp.Body) > 0 {
			resp.Body = resp.Body[:len(resp.Body)-1]
		}
	}
	eol, err := c.b.ReadByte()
	if err != nil {
		return nil, err
	}
	if eol != '\n' {
		return nil, err
	}

	return resp, nil
}

func (c *Conn) Disconnect() error {
	if c.nc == nil {
		return nil
	}
	c.l.Lock()
	defer c.l.Unlock()
	c.b.WriteString("DISCONNECT\n\x00\n")
	c.b.Flush()
	c.nc.Close()
	c.b = nil
	c.nc = nil
	return nil
}
