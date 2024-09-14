package statssender

import (
	"context"
	"encoding/json"
	"time"

	"github.com/olebeck/fast"
	"github.com/sirupsen/logrus"
)

type StatsSender struct {
	sessionID string
	statFunc  func() any
	queue     *fast.Queue
}

func New(sessionID string, queue *fast.Queue, StatFunc func() any) *StatsSender {
	return &StatsSender{
		sessionID: sessionID,
		queue:     queue,
		statFunc:  StatFunc,
	}
}

func (s *StatsSender) Run(ctx context.Context, info any) {
	body, err := json.Marshal(&struct {
		Type      string
		SessionID string
		Data      any
	}{
		Type:      "start_session",
		SessionID: s.sessionID,
		Data:      info,
	})
	if err != nil {
		logrus.Error(err)
		return
	}
	err = s.queue.Send("stats", body)
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
		body, err := json.Marshal(&struct {
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
			continue
		}

		err = s.queue.Send("stats", body)
		if err != nil {
			logrus.Error(err)
			continue
		}
	}
}
