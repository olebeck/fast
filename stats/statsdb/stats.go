package statsdb

import (
	"database/sql"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/olebeck/fast/stats"
)

type Stats[Tinfo any, Tstat stats.StatData] struct {
	db       *sql.DB
	foundAdd map[uuid.UUID]int
	l        sync.Mutex
}

func NewStats[Tinfo any, Tstat stats.StatData]() (s *Stats[Tinfo, Tstat], err error) {
	s = &Stats[Tinfo, Tstat]{
		foundAdd: make(map[uuid.UUID]int),
	}

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
			found_count NUMBER DEFAULT 0,
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
	SessionID uuid.UUID
	Data      Tstat
}

func (s *Stats[Tinfo, Tstat]) HandleSubmit(id uuid.UUID, stat *Tstat, now time.Time) error {
	// Start a transaction
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Retrieve the last found_count value
	var lastFoundCount sql.NullInt64
	err = tx.QueryRow(`
		SELECT found_count
		FROM stats
		WHERE id = ?
		ORDER BY dt DESC
		LIMIT 1
	`, id).Scan(&lastFoundCount)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Marshal the stat data to JSON
	statData, err := json.Marshal(stat)
	if err != nil {
		return err
	}

	s.l.Lock()
	add := s.foundAdd[id]
	lastFoundCount.Int64 += int64(add)
	delete(s.foundAdd, id)
	s.l.Unlock()

	// Insert the new stat entry
	_, err = tx.Exec(`
		INSERT INTO stats (id, dt, data, found_count) VALUES (?, ?, ?, ?);
	`, id, now, statData, lastFoundCount.Int64)
	if err != nil {
		return err
	}

	// Update the latest_stat in the sessions table
	_, err = tx.Exec(`
		UPDATE sessions SET latest_stat = ? WHERE id = ?;
	`, now, id)
	if err != nil {
		return err
	}

	// Commit the transaction
	return tx.Commit()
}

func (s *Stats[Tinfo, Tstat]) NewSession(id uuid.UUID, start time.Time, info Tinfo) error {
	sessionInfo, err := json.Marshal(info)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(`
		INSERT INTO sessions (id, start, info)
		VALUES ($1,$2,$3);
	`, id, start, sessionInfo)
	if err != nil {
		return err
	}
	return nil
}

func (s *Stats[Tinfo, Tstat]) GetStats(duration time.Duration) ([]*stats.Session[Tinfo, Tstat], error) {
	var sessions []*stats.Session[Tinfo, Tstat]

	rows, err := s.db.Query(`
		SELECT s.id, s.start, s.latest_stat, s.info, st.dt, st.data, st.found_count
		FROM sessions s
		LEFT JOIN stats st ON s.id = st.id
		WHERE unixepoch(s.latest_stat) - unixepoch(st.dt) < $1
		ORDER BY s.id, st.dt DESC
	`, duration.Seconds())

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var currentID uuid.UUID
	var session *stats.Session[Tinfo, Tstat]

	for rows.Next() {
		var id uuid.UUID
		var start time.Time
		var latest sql.NullTime
		var infoJSON []byte
		var statTime sql.NullTime
		var statData sql.NullString
		var foundCount sql.NullInt64

		err = rows.Scan(&id, &start, &latest, &infoJSON, &statTime, &statData, &foundCount)
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
			var LatestStat *time.Time
			if latest.Valid {
				LatestStat = &latest.Time
			}
			session = &stats.Session[Tinfo, Tstat]{
				ID:         id,
				Start:      start,
				LatestStat: LatestStat,
				Info:       &info,
			}
			sessions = append(sessions, session)
		}

		if statTime.Valid && statData.Valid {
			var stat Tstat
			if err := json.Unmarshal([]byte(statData.String), &stat); err != nil {
				return nil, err
			}
			session.Stats = append(session.Stats, stats.Stat[Tstat]{
				Time:       statTime.Time,
				FoundCount: int(foundCount.Int64),
				Data:       stat,
			})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	slices.SortFunc(sessions, func(a, b *stats.Session[Tinfo, Tstat]) int {
		if a.Inactive() != b.Inactive() {
			if a.Inactive() {
				return 1
			} else {
				return -1
			}
		}
		return -a.Start.Compare(b.Start)
	})

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

func (s *Stats[Tinfo, Tstat]) AddFound(id uuid.UUID, count int) error {
	s.l.Lock()
	s.foundAdd[id] += count
	s.l.Unlock()
	return nil
}
