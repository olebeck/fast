package statsdb

import (
	"database/sql"
	"encoding/json"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/olebeck/fast/stats"
)

type Stats[Tinfo any, Tstat stats.StatData] struct {
	db *sql.DB
}

func NewStats[Tinfo any, Tstat stats.StatData]() (s *Stats[Tinfo, Tstat], err error) {
	s = &Stats[Tinfo, Tstat]{}

	s.db, err = sql.Open("sqlite3", "stats.db?cache=shared")
	if err != nil {
		return nil, err
	}
	s.db.SetMaxOpenConns(1)

	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS stats(
			id UUID,
			dt DATETIME,
			data BLOB,
			PRIMARY KEY(id,dt)
		) WITHOUT ROWID;
	`)
	if err != nil {
		return nil, err
	}

	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS sessions(
			id UUID,
			start DATETIME,
			latest_stat DATETIME,
			info BLOB,
			PRIMARY KEY(id)
		)
	`)
	if err != nil {
		return nil, err
	}

	return s, nil
}

type StatSubmit[Tstat stats.StatData] struct {
	SessionID string
	Data      Tstat
}

func (s *Stats[Tinfo, Tstat]) HandleSubmit(stat *StatSubmit[Tstat]) error {
	_, err := s.db.Exec(`
		INSERT INTO stats (id,dt,data) VALUES ($1,$2,$3);
		UPDATE sessions SET latest_stat = $2;
	`, stat.SessionID, time.Now(), stat.Data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Stats[Tinfo, Tstat]) NewSession(session *stats.Session[Tinfo, Tstat]) error {
	_, err := s.db.Exec(`
		INSERT INTO sessions (id, start, info)
		VALUES ($1,$2,$3);
	`, session.ID, time.Now(), session.Info)
	if err != nil {
		return err
	}
	return nil
}

func (s *Stats[Tinfo, Tstat]) GetStats(duration time.Duration) ([]*stats.Session[Tinfo, Tstat], error) {
	var sessions []*stats.Session[Tinfo, Tstat]

	rows, err := s.db.Query(`
		SELECT s.id, s.start, s.info, st.dt, st.data
		FROM sessions s
		LEFT JOIN stats st ON s.id = st.id
		WHERE s.latest_stat > datetime('now', time(?))
		ORDER BY s.id, st.dt DESC
	`, duration)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var currentID string
	var session *stats.Session[Tinfo, Tstat]

	for rows.Next() {
		var id string
		var start time.Time
		var infoJSON []byte
		var statTime sql.NullTime
		var statData sql.NullString

		err = rows.Scan(&id, &start, &infoJSON, &statTime, &statData)
		if err != nil {
			return nil, err
		}

		var info Tinfo
		if err := json.Unmarshal(infoJSON, &info); err != nil {
			return nil, err
		}

		if currentID != id {
			// New session encountered
			currentID = id
			session = &stats.Session[Tinfo, Tstat]{
				ID:    id,
				Start: start,
				Info:  &info,
			}
			sessions = append(sessions, session)
		}

		if statTime.Valid && statData.Valid {
			var stat Tstat
			if err := json.Unmarshal([]byte(statData.String), &stat); err != nil {
				return nil, err
			}
			session.Stats = append(session.Stats, stats.Stat[Tstat]{
				Time: statTime.Time,
				Data: stat,
			})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return sessions, nil
}

func (s *Stats[Tinfo, Tstat]) SessionCount() (int, error) {
	row := s.db.QueryRow("SELECT COUNT(*) FROM sessions;")
	if err := row.Err(); err != nil {
		return 0, err
	}
	var val int
	err := row.Scan(&val)
	if err != nil {
		return 0, err
	}
	return val, nil
}
