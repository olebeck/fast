package http

import (
	"bufio"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

type Conn struct {
	client       *Client
	nc           ncc
	lastReq      time.Time
	RequestsSent int
}

func (h *Conn) Closed() bool {
	return h.nc.Conn == nil
}

func (hc *Conn) Close() error {
	if hc.Closed() {
		return nil
	}
	err := hc.nc.Close()
	hc.client = nil
	hc.nc.Conn = nil
	return err
}

var readerPool sync.Pool
var writerPool sync.Pool

func (hc *Conn) acquireWriter() *bufio.Writer {
	v := writerPool.Get()
	if v == nil {
		return bufio.NewWriterSize(hc.nc, 4096)
	}

	bw := v.(*bufio.Writer)
	bw.Reset(hc.nc)
	return bw
}

func (hc *Conn) acquireReader() *bufio.Reader {
	v := readerPool.Get()
	if v == nil {
		return bufio.NewReaderSize(&hc.nc, 4096)
	}

	br := v.(*bufio.Reader)
	br.Reset(&hc.nc)
	return br
}

func (hc *Conn) DoTimeout(req *fasthttp.Request, res *fasthttp.Response, timeout time.Duration) (err error) {
	hc.RequestsSent += 1
	hc.nc.SetWriteDeadline(time.Now().Add(timeout))

	// send request
	bw := hc.acquireWriter()
	req.Write(bw)
	err = bw.Flush()
	writerPool.Put(bw)
	bw = nil
	if err != nil {
		hc.Close()
		return err
	}

	hc.nc.SetReadDeadline(time.Now().Add(timeout))

	// read response
	err = hc.nc.Wait()
	if err != nil {
		hc.Close()
		return err
	}
	br := hc.acquireReader()
	err = res.Read(br)
	readerPool.Put(br)
	br = nil
	if err != nil {
		hc.Close()
		return err
	}

	// return connection to queue
	hc.lastReq = time.Now()
	return nil
}
