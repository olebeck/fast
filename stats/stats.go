package stats

import "time"

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
	Time time.Time
	Data Tstat
}

type Session[Tinfo any, Tstat StatData] struct {
	ID    string
	Start time.Time
	Stats []Stat[Tstat]
	Info  *Tinfo
}
