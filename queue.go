package fast

import (
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/olebeck/fast/tinystomp"
	"github.com/sirupsen/logrus"
)

type Queue struct {
	url *url.URL
	c   atomic.Pointer[tinystomp.Conn]

	needReconnect atomic.Bool
	mu            sync.Mutex
	cond          *sync.Cond
	closed        atomic.Bool

	Headers map[string]string
}

func NewQueue(queueUrl string) (q *Queue, err error) {
	q = &Queue{}
	q.url, err = url.Parse(queueUrl)
	if err != nil {
		return nil, err
	}
	q.cond = sync.NewCond(&q.mu)
	err = q.Connect()
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Queue) Connect() (err error) {
	addr := q.url.Hostname() + ":" + q.url.Port()
	password, _ := q.url.User.Password()
	c := tinystomp.NewConn()
	c.Host = q.url.Path[1:]
	c.User = q.url.User.Username()
	c.Pass = password
	err = c.Dial(addr)
	if err != nil {
		return err
	}
	q.c.Store(c)
	q.needReconnect.Store(false)
	q.cond.Broadcast()
	return nil
}

func (q *Queue) Send(body []byte) error {
retry:
	if q.closed.Load() {
		return fmt.Errorf("connection is closed")
	}

	conn := q.c.Load()
	err := conn.Send("/amq/queue/javaprofiles", "application/json", body, q.Headers)
	if err != nil {
		logrus.Error(err)
		if q.needReconnect.CompareAndSwap(false, true) {
			go q.reconnect()
		}

		// Wait for reconnection to finish
		q.mu.Lock()
		for q.needReconnect.Load() {
			q.cond.Wait()
		}
		q.mu.Unlock()

		goto retry
	}
	return err
}

func (q *Queue) reconnect() {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if another goroutine has already reconnected
	if !q.needReconnect.Load() {
		return
	}

	logrus.Info("Attempting to reconnect")
	err := q.Connect()
	if err != nil {
		logrus.Error("Reconnection failed: ", err)
		// Optional: add some backoff strategy and retry mechanism here
		q.needReconnect.Store(true) // Keep the needReconnect flag set to true if reconnection fails
		return
	}

	logrus.Info("Reconnected successfully")
	q.needReconnect.Store(false)
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed.CompareAndSwap(false, true) {
		// Safely close the connection
		conn := q.c.Load()
		err := conn.Disconnect()
		if err != nil {
			return err
		}
		q.cond.Broadcast() // wake up any waiting goroutines
		return nil
	}
	return fmt.Errorf("connection already closed")
}
