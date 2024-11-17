package statssender

import (
	"context"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type StatsSender struct {
	sessionID string
	url       string
	conn      *websocket.Conn
	statFunc  func() any
}

func New(sessionID string, url string, StatFunc func() any) *StatsSender {
	return &StatsSender{
		sessionID: sessionID,
		url:       url,
		statFunc:  StatFunc,
	}
}

func (s *StatsSender) connect() error {
	conn, res, err := websocket.DefaultDialer.Dial(s.url, http.Header{})
	if err != nil {
		return err
	}
	_ = res
	s.conn = conn
	return nil
}

func (s *StatsSender) sendJson(a any) error {
	b := backoff.NewExponentialBackOff(backoff.WithMaxInterval(1 * time.Minute))
retry:
	if s.conn == nil {
		err := s.connect()
		if err != nil {
			s.conn = nil
			bo := b.NextBackOff()
			logrus.WithField("retry-in", bo).Error(err)
			time.Sleep(bo)
			goto retry
		}
	}

	if err := s.conn.WriteJSON(a); err != nil {
		s.conn.Close()
		s.conn = nil
		bo := b.NextBackOff()
		logrus.WithField("retry-in", bo).Error(err)
		time.Sleep(bo)
		goto retry
	}

	return nil
}

func (s *StatsSender) Run(ctx context.Context, info any) {
	err := s.sendJson(&struct {
		Type      string
		SessionID string
		Data      any
	}{
		Type:      "start",
		SessionID: s.sessionID,
		Data:      info,
	})
	if err != nil {
		logrus.Error(err)
		return
	}

	t := time.NewTicker(15 * time.Second)
	for range t.C {
		if ctx.Err() != nil {
			return
		}
		stat := s.statFunc()
		err := s.sendJson(&struct {
			Type      string
			SessionID string
			Data      any
		}{
			Type:      "stat",
			SessionID: s.sessionID,
			Data:      stat,
		})
		if err != nil {
			logrus.Error(err)
		}
	}
}
