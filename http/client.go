package http

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"github.com/olebeck/fast"
	"github.com/olebeck/fast/tls"
	"github.com/valyala/fasthttp"
)

type Client struct {
	dialFunc  fasthttp.DialFuncWithTimeout
	wait      time.Duration
	conns     chan *Conn
	maxCount  int
	tlsConfig *tls.Config

	Stats Stats
}

func NewClient(count int, wait time.Duration, dialFunc fasthttp.DialFuncWithTimeout, tlsConfig *tls.Config) *Client {
	return &Client{
		dialFunc:  dialFunc,
		wait:      wait,
		conns:     make(chan *Conn, count),
		maxCount:  count,
		tlsConfig: tlsConfig,
		Stats: Stats{
			RequestTimeMS: fast.NewMovingAverage(100),
		},
	}
}

func (h *Client) GetConn() (hc *Conn) {
	if int(h.Stats.OpenConnections.Load()) >= h.maxCount {
		hc = <-h.conns
	} else {
		h.Stats.OpenConnections.Add(1)
		return nil
	}

	if hc.client != h {
		panic("conn from wrong client in queue")
	}

	// wait out ratelimit
	if h.wait > time.Duration(0) {
		tt := time.Since(hc.lastReq)
		if tt < h.wait {
			time.Sleep((h.wait - tt) + (time.Duration(rand.Intn(250)) * time.Millisecond))
		}
	}

	return hc
}

func (h *Client) ReturnConn(hc **Conn) {
	if *hc == nil || (*hc).Closed() {
		h.Stats.OpenConnections.Add(-1)
		return
	}
	h.conns <- *hc
}

// newConn creates a new connection to the server using the dial function
func (h *Client) NewConn(req *fasthttp.Request, timeout time.Duration) (*Conn, error) {
	h.Stats.TotalConnects.Add(1)
	addr := string(req.Host())
	isTLS := bytes.Equal(req.URI().Scheme(), []byte("https"))
	if isTLS {
		addr += ":443"
	} else {
		addr += ":80"
	}
	nc, err := h.dialFunc(addr, timeout)
	if err != nil {
		return nil, err
	}

	if isTLS {
		nc = tls.Client(nc, h.tlsConfig)
	}

	return &Conn{
		client:  h,
		nc:      ncc{Conn: nc},
		lastReq: time.Time{},
	}, nil
}

func (h *Client) DoRetry(req *fasthttp.Request, res *fasthttp.Response, hc **Conn, statusCheck func(int) bool) error {
	var err error
	var t1 time.Time
	for retry := 0; retry < 8; retry++ {
		if *hc == nil || (*hc).Closed() {
			*hc, err = h.NewConn(req, 30*time.Second)
			if err != nil {
				goto retryErr
			}
		}

		h.Stats.RequestsSent.Add(1)
		t1 = time.Now()
		err = (*hc).DoTimeout(req, res, 30*time.Second)
		if res.StatusCode() == 429 {
			(*hc).Close()
			*hc = nil
			continue
		}
		if err == nil && statusCheck(res.StatusCode()) {
			err = &StatusError{i: (*hc).RequestsSent, url: req.URI().String(), status: res.StatusCode()}
		}

	retryErr:
		if err != nil {
			h.Stats.LastErr = fmt.Errorf("%v %s", err, string(req.Body()))
			h.Stats.Retries.Add(1)
			continue
		}
		h.Stats.RequestTimeMS.Add(float64(time.Since(t1).Milliseconds()))
		break
	}

	return err
}
