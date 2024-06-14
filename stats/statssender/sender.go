package statssender

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

type StatsSender struct {
	sessionID   string
	statsServer string
	statFunc    func() any
	client      http.Client
}

func New(sessionID, statsServer string, StatFunc func() any) *StatsSender {
	return &StatsSender{
		sessionID:   sessionID,
		statsServer: statsServer,
		statFunc:    StatFunc,
	}
}

func (s *StatsSender) Run(ctx context.Context, info any) {
	body, err := json.Marshal(&struct {
		SessionID string
		Info      any
	}{
		SessionID: s.sessionID,
		Info:      info,
	})
	if err != nil {
		logrus.Error(err)
		return
	}
	_, err = s.client.Post(s.statsServer+"/api/start_session", "application/json", bytes.NewReader(body))
	if err != nil {
		logrus.Error(err)
	}

	t := time.NewTicker(15 * time.Second)
	for range t.C {
		if ctx.Err() != nil {
			return
		}

		stat := s.statFunc()
		body, err := json.Marshal(&struct {
			SessionID string
			Data      any
		}{
			SessionID: s.sessionID,
			Data:      stat,
		})
		if err != nil {
			logrus.Error(err)
			continue
		}
		_, err = s.client.Post(s.statsServer+"/api/submit_stat", "application/json", bytes.NewReader(body))
		if err != nil {
			logrus.Error(err)
			continue
		}
	}
}
