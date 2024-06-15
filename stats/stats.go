package stats

import (
	"time"

	"github.com/google/uuid"
)

type StatData interface {
	Sum(Prev any)
}

func Total[Tinfo any, Tstat StatData](sessions []*Session[Tinfo, Tstat]) Tstat {
	var total = new(Tstat)
	for _, session := range sessions {
		if len(session.Stats) == 0 {
			continue
		}
		data := session.Stats[0].Data
		data.Sum(total)
	}

	return *total
}

type Stat[Tstat StatData] struct {
	Time       time.Time
	FoundCount int
	Data       Tstat
}

type Session[Tinfo any, Tstat StatData] struct {
	ID         uuid.UUID
	Start      time.Time
	LatestStat time.Time
	Stats      []Stat[Tstat]
	Info       *Tinfo
}

func (s *Session[Tinfo, Tstat]) Inactive() bool {
	return time.Since(s.LatestStat) > 45*time.Second
}

func (s *Session[Tinfo, Tstat]) Latest() *Stat[Tstat] {
	if len(s.Stats) == 0 {
		return nil
	}
	return &s.Stats[0]
}
