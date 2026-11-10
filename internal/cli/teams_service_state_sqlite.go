package cli

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

// One scoped service database holds all hosts' restart counters. The global
// service database holds host-keyed watchdog and supervisor runtime records.
// Core-scoped counters must not become global merely because storage changes.
type teamsServiceStateDB struct {
	db   *sql.DB
	host string
}
type teamsServiceRestartLocation struct{ Database, Host string }

func serviceTime(v time.Time) string {
	if v.IsZero() {
		return ""
	}
	return v.Format(time.RFC3339Nano)
}
func parseServiceTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
func serviceNativePath(path string) string {
	if strings.EqualFold(filepath.Ext(path), ".sqlite") {
		return path
	}
	return strings.TrimSuffix(path, filepath.Ext(path)) + ".sqlite"
}
func openTeamsServiceState(path string, watchdogLegacy []string) (*teamsServiceStateDB, error) {
	path = serviceNativePath(path)
	host, err := teamsHostname()
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(path, false)
	if err != nil {
		return nil, err
	}
	if err = migrateTeamsServiceState(db, path, host, watchdogLegacy); err != nil {
		db.Close()
		return nil, err
	}
	return &teamsServiceStateDB{db: db, host: host}, nil
}
func checkTeamsServiceState(db *sql.DB) error {
	var n int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&n); err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("unsupported service state version %d", n)
	}
	return nil
}
func (s *teamsServiceStateDB) Close() error { return s.db.Close() }
func (s *teamsServiceStateDB) Restart(host string) (teamsServiceRestartRecord, error) {
	var v teamsServiceRestartRecord
	var start, wait string
	err := s.db.QueryRow(`SELECT streak,last_start,wait_until FROM service_restarts WHERE host=?`, host).Scan(&v.Streak, &start, &wait)
	if errors.Is(err, sql.ErrNoRows) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	v.LastStart, err = parseServiceTime(start)
	if err != nil {
		return v, err
	}
	v.WaitUntil, err = parseServiceTime(wait)
	return v, err
}

const restartPut = `INSERT INTO service_restarts(host,streak,last_start,wait_until) VALUES(?,?,?,?) ON CONFLICT(host) DO UPDATE SET streak=excluded.streak,last_start=excluded.last_start,wait_until=excluded.wait_until WHERE (streak,last_start,wait_until) IS NOT (excluded.streak,excluded.last_start,excluded.wait_until)`

func (s *teamsServiceStateDB) SaveRestart(host string, v teamsServiceRestartRecord) error {
	_, err := s.db.Exec(restartPut, host, v.Streak, serviceTime(v.LastStart), serviceTime(v.WaitUntil))
	return err
}
func (s *teamsServiceStateDB) Watchdog() (teamsServiceWatchdogState, error) {
	var v teamsServiceWatchdogState
	var action, updated string
	err := s.db.QueryRow(`SELECT consecutive_stale,last_reason,last_action,last_action_at,updated_at FROM service_watchdogs WHERE host=?`, s.host).Scan(&v.ConsecutiveStale, &v.LastReason, &v.LastAction, &action, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	v.LastActionAt, err = parseServiceTime(action)
	if err != nil {
		return v, err
	}
	v.UpdatedAt, err = parseServiceTime(updated)
	return v, err
}

const watchdogPut = `INSERT INTO service_watchdogs(host,consecutive_stale,last_reason,last_action,last_action_at,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(host) DO UPDATE SET consecutive_stale=excluded.consecutive_stale,last_reason=excluded.last_reason,last_action=excluded.last_action,last_action_at=excluded.last_action_at,updated_at=excluded.updated_at WHERE (consecutive_stale,last_reason,last_action,last_action_at,updated_at) IS NOT (excluded.consecutive_stale,excluded.last_reason,excluded.last_action,excluded.last_action_at,excluded.updated_at)`

func (s *teamsServiceStateDB) SaveWatchdog(v teamsServiceWatchdogState) error {
	_, err := s.db.Exec(watchdogPut, s.host, v.ConsecutiveStale, v.LastReason, v.LastAction, serviceTime(v.LastActionAt), serviceTime(v.UpdatedAt))
	return err
}
func loadTeamsServiceRestartRecord(p teamsServiceRestartLocation) (teamsServiceRestartRecord, error) {
	db, err := sqlitestate.Open(p.Database, true)
	if os.IsNotExist(err) {
		// First startup owns legacy conversion; a watchdog observation must not
		// create a service database or claim that a corrupt backoff was empty.
		sources, e := legacyRestartSources(filepath.Dir(p.Database))
		if e != nil {
			return teamsServiceRestartRecord{}, e
		}
		if len(sources) == 0 {
			return teamsServiceRestartRecord{}, nil
		}
		st, e := openTeamsServiceState(p.Database, nil)
		if e != nil {
			return teamsServiceRestartRecord{}, e
		}
		defer st.Close()
		return st.Restart(p.Host)
	}
	if err != nil {
		return teamsServiceRestartRecord{}, err
	}
	defer db.Close()
	if err = checkTeamsServiceState(db); err != nil {
		return teamsServiceRestartRecord{}, err
	}
	return (&teamsServiceStateDB{db: db}).Restart(p.Host)
}
func loadTeamsServiceWatchdogNative(path string) (teamsServiceWatchdogState, error) {
	db, err := sqlitestate.Open(serviceNativePath(path), true)
	if os.IsNotExist(err) {
		sources, e := serviceWatchdogLegacySources(path)
		if e != nil {
			return teamsServiceWatchdogState{}, e
		}
		for _, source := range sources {
			if _, e = os.Lstat(source); e == nil {
				return teamsServiceWatchdogState{}, fmt.Errorf("service state requires migration before read-only inspection: %s", source)
			} else if !os.IsNotExist(e) {
				return teamsServiceWatchdogState{}, e
			}
		}
		return teamsServiceWatchdogState{}, nil
	}
	if err != nil {
		return teamsServiceWatchdogState{}, err
	}
	defer db.Close()
	if err = checkTeamsServiceState(db); err != nil {
		return teamsServiceWatchdogState{}, err
	}
	host, err := teamsHostname()
	if err != nil {
		return teamsServiceWatchdogState{}, err
	}
	return (&teamsServiceStateDB{db: db, host: host}).Watchdog()
}
func openTeamsWatchdogState() (*teamsServiceStateDB, error) {
	path, err := teamsServiceWatchdogStatePath()
	if err != nil {
		return nil, err
	}
	sources, err := serviceWatchdogLegacySources(path)
	if err != nil {
		return nil, err
	}
	return openTeamsServiceState(path, sources)
}
func loadTeamsServiceWatchdogState() (teamsServiceWatchdogState, error) {
	st, err := openTeamsWatchdogState()
	if err != nil {
		return teamsServiceWatchdogState{}, err
	}
	defer st.Close()
	return st.Watchdog()
}
func saveTeamsServiceWatchdogState(v teamsServiceWatchdogState) error {
	st, err := openTeamsWatchdogState()
	if err != nil {
		return err
	}
	defer st.Close()
	return st.SaveWatchdog(v)
}
func defaultTeamsServiceWatchdogStatePath() (string, error) {
	return appdirs.StatePath("teams", "service", "service-state.sqlite")
}
