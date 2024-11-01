package fast

import (
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/olebeck/fast/qq"
	"github.com/sirupsen/logrus"
)

type sub struct {
	name    string
	process func([]byte)
}

type Queue2 struct {
	url *url.URL
	c   atomic.Pointer[qq.Client]

	needReconnect atomic.Bool
	mu            sync.Mutex
	cond          *sync.Cond
	closed        atomic.Bool

	subs []sub
}

func NewQueue2(queueUrl string) (q *Queue2, err error) {
	q = &Queue2{}
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

func (q *Queue2) Connect() (err error) {
	password := q.url.User.Username()
	conn := qq.NewClient(q.url.Host, password)
	if err = conn.Connect(); err != nil {
		return err
	}
	if oldConn := q.c.Swap(conn); oldConn != nil {
		oldConn.Close()
	}
	q.needReconnect.Store(false)
	q.cond.Broadcast()
	return nil
}

func (q *Queue2) Send(queueName string, body []byte) error {
retry:
	if q.closed.Load() {
		return fmt.Errorf("connection is closed")
	}

	conn := q.c.Load()
	err := conn.Publish(queueName, body)
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

func (q *Queue2) Subscribe(queueName string, process func(messageData []byte)) error {
retry:
	if q.closed.Load() {
		return fmt.Errorf("connection is closed")
	}

	q.subs = append(q.subs, sub{name: queueName, process: process})

	conn := q.c.Load()
	err := conn.Subscribe(queueName, process)
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

func (q *Queue2) reconnect() {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if another goroutine has already reconnected
	if !q.needReconnect.Load() {
		return
	}

retry:
	logrus.Info("Attempting to reconnect")
	err := q.Connect()
	if err != nil {
		logrus.Error("Reconnection failed: ", err)
		time.Sleep(5 * time.Second)
		goto retry
	}

	for _, sub := range q.subs {
		err = q.Subscribe(sub.name, sub.process)
		if err != nil {
			logrus.Error("Subscribe failed: ", err)
			time.Sleep(5 * time.Second)
			goto retry
		}
	}

	logrus.Info("Reconnected successfully")
	q.needReconnect.Store(false)
}

func (q *Queue2) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed.CompareAndSwap(false, true) {
		// Safely close the connection
		conn := q.c.Load()
		err := conn.Close()
		if err != nil {
			return err
		}
		q.cond.Broadcast() // wake up any waiting goroutines
		return nil
	}
	return fmt.Errorf("connection already closed")
}
