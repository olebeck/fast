package statsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"slices"
	"sync"
	"testing/fstest"
	"time"

	_ "embed"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"
	"github.com/google/uuid"
	"github.com/olebeck/fast/stats"
	"github.com/sirupsen/logrus"
	"golang.org/x/text/message"
)

//go:embed statspage.html
var statspage_html []byte

func StatsPage[Tinfo any, Tstat stats.StatData](group fiber.Router, statTemplate, totalStatTemplate []byte) (*Stats[Tinfo, Tstat], error) {
	statsDB, err := NewStats[Tinfo, Tstat]()
	if err != nil {
		return nil, fmt.Errorf("init db: %w", err)
	}

	views := html.NewFileSystem(http.FS(fstest.MapFS{
		"statspage.html": &fstest.MapFile{
			Data: statspage_html,
		},
		"stats.html": &fstest.MapFile{
			Data: statTemplate,
		},
		"totalstat.html": &fstest.MapFile{
			Data: totalStatTemplate,
		},
	}), ".html")
	views.AddFunc("duration", func(ms int) time.Duration {
		return time.Duration(ms)
	})
	views.AddFunc("time", func(t time.Time) string {
		return t.Format(time.RFC822)
	})
	views.AddFunc("rate", func(count int, start time.Time, curr time.Time) int {
		t := curr.Sub(start)
		return count / int(t.Seconds())
	})
	printer := message.NewPrinter(message.MatchLanguage("en"))
	views.AddFunc("num", func(num int) string {
		return printer.Sprint(num)
	})

	api := group.Group("/api")

	api.Use("/ws", func(c *fiber.Ctx) error {
		// IsWebSocketUpgrade returns true if the client
		// requested upgrade to the WebSocket protocol.
		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	var wsClientsLock sync.Mutex
	var wsClients = make(map[netip.Addr]*websocket.Conn)

	broadcast := func(d any) error {
		data, err := json.Marshal(d)
		if err != nil {
			return err
		}

		wsClientsLock.Lock()
		for k, c := range wsClients {
			err := c.WriteMessage(websocket.TextMessage, data)
			if err != nil {
				delete(wsClients, k)
			}
		}
		wsClientsLock.Unlock()
		return nil
	}

	api.Get("/ws/submit", websocket.New(func(c *websocket.Conn) {
		for {
			var msg struct {
				SessionID uuid.UUID
				Type      string
				Data      json.RawMessage
			}
			err = c.ReadJSON(&msg)
			if err != nil {
				logrus.Error(err)
				return
			}
			switch msg.Type {
			case "start":
				var info Tinfo
				if err = json.Unmarshal(msg.Data, &info); err != nil {
					logrus.Error(err)
					return
				}
				if err = statsDB.NewSession(msg.SessionID, time.Now(), info); err != nil {
					logrus.Error(err)
					return
				}
				broadcast(msg)
			case "stat":
				var stat Tstat
				if err = json.Unmarshal(msg.Data, &stat); err != nil {
					logrus.Error(err)
					return
				}
				if err = statsDB.HandleSubmit(msg.SessionID, &stat, time.Now()); err != nil {
					logrus.Error(err)
					return
				}
				broadcast(msg)
			}
		}
	}))

	api.Get("/ws/stats", websocket.New(func(c *websocket.Conn) {
		wsClientsLock.Lock()
		addr := netip.MustParseAddr(c.RemoteAddr().String())
		wsClients[addr] = c
		wsClientsLock.Unlock()
		defer func() {
			wsClientsLock.Lock()
			delete(wsClients, addr)
			wsClientsLock.Unlock()
		}()
		for {
			var a any
			err := c.ReadJSON(a)
			if err != nil {
				break
			}
		}
	}))

	api.Get("/stats", func(c *fiber.Ctx) error {
		duration, err := time.ParseDuration(c.Query("duration", "2h"))
		if err != nil {
			return err
		}
		sessions, err := statsDB.GetStats(duration)
		if err != nil {
			return err
		}

		total := stats.Total(sessions)

		sessionCount := len(sessions)
		sessions = slices.DeleteFunc(sessions, func(s *stats.Session[Tinfo, Tstat]) bool {
			if s.LatestStat == nil {
				return false
			}
			return time.Since(*s.LatestStat) > duration
		})

		return c.JSON(fiber.Map{
			"SessionCount": sessionCount,
			"Sessions":     sessions,
			"Total":        total,
		})
	})

	api.Post("/add_found", func(c *fiber.Ctx) error {
		var FoundCounts map[uuid.UUID]int
		err := c.BodyParser(&FoundCounts)
		if err != nil {
			return err
		}

		for id, count := range FoundCounts {
			err := statsDB.AddFound(id, count)
			if err != nil {
				return err
			}
		}

		return c.SendStatus(200)
	})

	group.Get("/", func(c *fiber.Ctx) error {
		duration, err := time.ParseDuration(c.Query("duration", "2h"))
		if err != nil {
			return err
		}
		sessions, err := statsDB.GetStats(duration)
		if err != nil {
			return err
		}

		total := stats.Total(sessions)

		sessionCount := len(sessions)
		sessions = slices.DeleteFunc(sessions, func(s *stats.Session[Tinfo, Tstat]) bool {
			if s.LatestStat == nil {
				return time.Since(s.Start) > duration
			}
			return time.Since(*s.LatestStat) > duration
		})

		buf := bytes.NewBuffer(nil)
		err = views.Render(buf, "statspage", fiber.Map{
			"StatsDataInit": fiber.Map{
				"SessionCount": sessionCount,
				"Sessions":     sessions,
				"Total":        total,
			},
		})
		if err != nil {
			return err
		}
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return c.Send(buf.Bytes())
	})

	return statsDB, nil
}
