package main

import (
	_ "embed"

	"github.com/gofiber/fiber/v2"
	"github.com/olebeck/fast/stats/statsdb"
	"github.com/sirupsen/logrus"
)

//go:embed stats.html
var stats_html []byte

//go:embed totalstats.html
var totalstats_html []byte

type SessionInfo struct {
	Name string
}

type SessionStat struct {
	Count int
}

func (s SessionStat) Sum(_in any) {
	in := _in.(*SessionStat)
	in.Count += s.Count
}

func main() {
	app := fiber.New()
	_, err := statsdb.StatsPage[SessionInfo, SessionStat](app.Group("/stats"), stats_html, totalstats_html)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Fatal(app.Listen(":7688"))
}
