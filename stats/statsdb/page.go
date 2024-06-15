package statsdb

import (
	"bytes"
	"fmt"
	"net/http"
	"slices"
	"testing/fstest"
	"time"

	_ "embed"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/template/html/v2"
	"github.com/google/uuid"
	"github.com/olebeck/fast/stats"
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
	api.Post("/start_session", func(c *fiber.Ctx) error {
		var session stats.Session[Tinfo, Tstat]
		err := c.BodyParser(&session)
		if err != nil {
			return err
		}
		err = statsDB.NewSession(&session)
		if err != nil {
			return err
		}
		return c.SendStatus(200)
	})

	api.Post("/submit_stat", func(c *fiber.Ctx) error {
		var stat StatSubmit[Tstat]
		err := c.BodyParser(&stat)
		if err != nil {
			return err
		}
		err = statsDB.HandleSubmit(&stat, time.Now())
		if err != nil {
			return err
		}
		return c.SendStatus(200)
	})

	api.Get("/stats", func(c *fiber.Ctx) error {
		duration, err := time.ParseDuration(c.Query("duration", "12h"))
		if err != nil {
			return err
		}
		sessions, err := statsDB.GetStats()
		if err != nil {
			return err
		}

		total := stats.Total(sessions)

		sessionCount := len(sessions)
		sessions = slices.DeleteFunc(sessions, func(s *stats.Session[Tinfo, Tstat]) bool {
			return time.Since(s.LatestStat) > duration
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
		duration, err := time.ParseDuration(c.Query("duration", "12h"))
		if err != nil {
			return err
		}
		sessions, err := statsDB.GetStats()
		if err != nil {
			return err
		}

		total := stats.Total(sessions)

		sessionCount := len(sessions)
		sessions = slices.DeleteFunc(sessions, func(s *stats.Session[Tinfo, Tstat]) bool {
			return time.Since(s.LatestStat) > duration
		})

		buf := bytes.NewBuffer(nil)
		err = views.Render(buf, "statspage", fiber.Map{
			"SessionCount": sessionCount,
			"Sessions":     sessions,
			"Total":        total,
		})
		if err != nil {
			return err
		}
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return c.Send(buf.Bytes())
	})

	return statsDB, nil
}
