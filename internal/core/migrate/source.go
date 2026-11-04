package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"modernc.org/sqlite"
)

// Version identifies a supported legacy store.
type Version string

const (
	V0120 Version = "v0.1.20"
	RC59  Version = "v0.1.22-rc.59"
)

// ErrUnsupported is returned for any source that is not v0.1.20 or rc.59.
var ErrUnsupported = errors.New("unsupported Teams store version: run v0.1.22-rc.59 once against this store, then upgrade")

// ErrLiveWriter is returned when the old service may still be writing.
var ErrLiveWriter = errors.New("the old Teams service may still be running: stop it (cxp teams service stop) and retry")

// LiveWindow is how recent a lease or heartbeat must be to count as live.
const LiveWindow = 5 * time.Minute

type pointer struct {
	SchemaVersion  int    `json:"schema_version"`
	StorageBackend string `json:"storage_backend"`
	StorageVersion int    `json:"storage_version"`
	Path           string `json:"path"`
}

// identify reads <dir>/state.json and returns the version, and the SQLite
// store path for the pointer form ("" for the full-JSON form).
func identify(dir string) (Version, string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		return "", "", err
	}
	var p pointer
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", "", fmt.Errorf("state.json: %w", err)
	}
	if p.StorageBackend != "" || p.Path != "" {
		db := p.Path
		if db == "" {
			db = "store.sqlite"
		}
		if !filepath.IsAbs(db) {
			db = filepath.Join(dir, db)
		}
		switch {
		case p.SchemaVersion == 6 && p.StorageVersion == 1:
			return V0120, db, nil
		case p.SchemaVersion == 12:
			return RC59, db, nil
		}
		return "", "", fmt.Errorf("%w (pointer schema %d, storage %d)", ErrUnsupported, p.SchemaVersion, p.StorageVersion)
	}
	switch p.SchemaVersion {
	case 5:
		return V0120, "", nil
	case 11:
		return RC59, "", nil
	}
	return "", "", fmt.Errorf("%w (state schema %d)", ErrUnsupported, p.SchemaVersion)
}

// legacy is the converter's view of one source store. Entity rows are read
// lazily (each) so large tables are streamed; the small cold sections are
// decoded up front.
type legacy struct {
	version Version
	dir     string
	db      *sql.DB // nil for the full-JSON form
	cold    map[string]json.RawMessage
	runtime map[string]json.RawMessage
	watch   map[string]json.RawMessage
	notes   []string
}

func (l *legacy) close() {
	if l.db != nil {
		l.db.Close()
	}
}

// section returns a runtime section: runtime_state overrides the cold blob.
func (l *legacy) section(key string) json.RawMessage {
	if v, ok := l.runtime[key]; ok && len(v) > 0 && string(v) != "null" {
		return v
	}
	return l.cold[key]
}

// tableOf maps entity sections to their pointer-form tables.
var tableOf = map[string]string{
	"sessions":              "sessions",
	"inbound_events":        "inbound_events",
	"turns":                 "turns",
	"outbox_messages":       "outbox_messages",
	"message_provenance":    "message_provenance",
	"chat_polls":            "chat_polls",
	"chat_rate_limits":      "chat_rate_limits",
	"import_checkpoints":    "import_checkpoints",
	"transcript_deliveries": "transcript_deliveries",
	"helper_deliveries":     "helper_deliveries",
	"artifact_records":      "artifact_records",
	"notifications":         "notifications",
	"fork_operations":       "fork_operations",
}

var keyCol = map[string]string{"chat_polls": "chat_id", "chat_rate_limits": "chat_id"}

// statusCol is the table column that holds a row's status, used to decide
// terminal vs non-terminal for a row whose JSON cannot be parsed.
var statusCol = map[string]string{
	"sessions": "status", "inbound_events": "status", "turns": "status", "outbox_messages": "status",
	"import_checkpoints": "status", "transcript_deliveries": "status", "helper_deliveries": "status",
	"artifact_records": "status", "notifications": "status", "fork_operations": "phase",
}

// row is one legacy entity row.
type row struct {
	ID     string
	Status string // from the table column ("" for the JSON form)
	Cols   map[string]string
	JSON   []byte
}

// extraCols are table columns read along with the JSON so an opaque row can
// still be attributed (conversation, ext id).
var extraCols = map[string][]string{
	"sessions":        {"teams_chat_id"},
	"inbound_events":  {"session_id", "teams_chat_id", "teams_message_id"},
	"outbox_messages": {"session_id", "teams_chat_id", "teams_message_id"},
	"turns":           {"session_id"},
}

// each streams every row of an entity: table rows first, then cold-blob
// entries whose id is not in the table (the table is authoritative).
func (l *legacy) each(entity string, fn func(row) error) error {
	seen := map[string]bool{}
	if l.db != nil {
		if t, ok := tableOf[entity]; ok && l.hasTable(t) {
			kc := keyCol[entity]
			if kc == "" {
				kc = "id"
			}
			cols := []string{kc, "json"}
			sc := statusCol[entity]
			if sc != "" && !l.hasColumn(t, sc) {
				sc = ""
			}
			if sc != "" {
				cols = append(cols, sc)
			}
			var extra []string
			for _, c := range extraCols[entity] {
				if l.hasColumn(t, c) {
					extra = append(extra, c)
				}
			}
			cols = append(cols, extra...)
			q := "SELECT "
			for i, c := range cols {
				if i > 0 {
					q += ","
				}
				q += "COALESCE(CAST(" + c + " AS TEXT),'')"
			}
			q += " FROM " + t + " ORDER BY rowid"
			rows, err := l.db.QueryContext(context.Background(), q)
			if err != nil {
				return fmt.Errorf("%s: %w", t, err)
			}
			for rows.Next() {
				vals := make([]string, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					rows.Close()
					return err
				}
				r := row{ID: vals[0], JSON: []byte(vals[1]), Cols: map[string]string{}}
				k := 2
				if sc != "" {
					r.Status = vals[2]
					k = 3
				}
				for i, c := range extra {
					r.Cols[c] = vals[k+i]
				}
				seen[r.ID] = true
				if err := fn(r); err != nil {
					rows.Close()
					return err
				}
			}
			if err := rows.Close(); err != nil {
				return err
			}
		}
	}
	var m map[string]json.RawMessage
	if entity == "history_watch" {
		m = l.watch
	} else if raw := l.cold[entity]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			l.notes = append(l.notes, fmt.Sprintf("cold section %s unreadable: %v", entity, err))
			return nil
		}
	}
	for _, id := range sortedKeys(m) {
		if seen[id] {
			continue
		}
		if err := fn(row{ID: id, JSON: m[id], Cols: map[string]string{}}); err != nil {
			return err
		}
	}
	return nil
}

func (l *legacy) hasTable(t string) bool {
	var n int
	_ = l.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, t).Scan(&n)
	return n == 1
}

func (l *legacy) hasColumn(t, c string) bool {
	var n int
	_ = l.db.QueryRow(`SELECT count(*) FROM pragma_table_info(?) WHERE name=?`, t, c).Scan(&n)
	return n == 1
}

// openSource opens the source store for reading. For the pointer form it
// either snapshots the live file (online backup API, includes the WAL) into
// snapDir, or, with immutable, reads an existing consistent snapshot
// directly (refused when a non-empty -wal file exists: those rows would be
// invisible).
func openSource(dir string, version Version, dbPath string, immutable bool, snapDir string) (*legacy, error) {
	l := &legacy{version: version, dir: dir, runtime: map[string]json.RawMessage{}}
	if dbPath == "" {
		raw, err := os.ReadFile(filepath.Join(dir, "state.json"))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &l.cold); err != nil {
			return nil, fmt.Errorf("state.json: %w", err)
		}
		l.watch = decodeMap(l.cold["history_watch"])
		return l, nil
	}
	read := dbPath
	if immutable {
		if st, err := os.Stat(dbPath + "-wal"); err == nil && st.Size() > 0 {
			return nil, fmt.Errorf("%s-wal is not empty: a direct read would miss its rows; migrate without --immutable (online backup)", dbPath)
		}
	} else {
		if err := os.MkdirAll(snapDir, 0o700); err != nil {
			return nil, err
		}
		read = filepath.Join(snapDir, "source.sqlite")
		_ = os.Remove(read)
		if err := backup(dbPath, read); err != nil {
			return nil, fmt.Errorf("snapshot: %w", err)
		}
	}
	q := url.Values{}
	q.Set("mode", "ro")
	if immutable || read != dbPath {
		q.Set("immutable", "1")
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(read)+"?"+q.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	l.db = db
	if err := l.loadMeta(); err != nil {
		db.Close()
		return nil, err
	}
	return l, nil
}

func decodeMap(raw json.RawMessage) map[string]json.RawMessage {
	var m map[string]json.RawMessage
	_ = json.Unmarshal(raw, &m)
	return m
}

func (l *legacy) loadMeta() error {
	var cold []byte
	err := l.db.QueryRow(`SELECT value FROM state_meta WHERE key = 'state_json'`).Scan(&cold)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("state_meta: %w", err)
	}
	if len(cold) > 0 {
		if err := json.Unmarshal(cold, &l.cold); err != nil {
			return fmt.Errorf("state_json: %w", err)
		}
	}
	if l.hasTable("runtime_state") {
		rows, err := l.db.Query(`SELECT key, json FROM runtime_state`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var k string
			var v []byte
			if err := rows.Scan(&k, &v); err != nil {
				rows.Close()
				return err
			}
			l.runtime[k] = v
		}
		rows.Close()
	}
	l.watch = decodeMap(l.cold["history_watch"])
	// rc.59 keeps a newer history_watch projection; it is authoritative only
	// when it was written for the current cold revision.
	var projRaw, revRaw []byte
	if l.db.QueryRow(`SELECT value FROM state_meta WHERE key = 'history_watch_projection'`).Scan(&projRaw) == nil &&
		l.db.QueryRow(`SELECT value FROM state_meta WHERE key = 'state_json_revision'`).Scan(&revRaw) == nil {
		var proj struct {
			HistoryWatch map[string]json.RawMessage `json:"history_watch"`
			Revision     json.Number                `json:"state_json_revision"`
		}
		if err := json.Unmarshal(projRaw, &proj); err != nil {
			l.notes = append(l.notes, "history_watch projection unreadable: "+err.Error())
		} else if strings.TrimSpace(proj.Revision.String()) == strings.TrimSpace(string(revRaw)) {
			l.watch = proj.HistoryWatch
			l.notes = append(l.notes, fmt.Sprintf("history_watch: projection (revision %s, %d entries) used over the cold blob", proj.Revision, len(proj.HistoryWatch)))
		} else {
			l.notes = append(l.notes, fmt.Sprintf("history_watch: projection revision %s != state revision %s; cold blob used", proj.Revision, revRaw))
		}
	}
	return nil
}

// backup copies src (including its WAL) to dst with the online backup API.
func backup(src, dst string) error {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(src)+"?mode=ro&_pragma=busy_timeout(10000)")
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(dc any) error {
		type backuper interface {
			NewBackup(string) (*sqlite.Backup, error)
		}
		b, ok := dc.(backuper)
		if !ok {
			return errors.New("driver does not support the backup API")
		}
		bk, err := b.NewBackup(dst)
		if err != nil {
			return err
		}
		for {
			more, err := bk.Step(-1)
			if err != nil {
				bk.Finish()
				return err
			}
			if !more {
				break
			}
		}
		return bk.Finish()
	})
}

// checkLive refuses when the old service may still be writing: a control
// lease that has not expired, or an owner heartbeat younger than LiveWindow.
func (l *legacy) checkLive(now time.Time) error {
	var lease legacyLease
	_, _ = decode(l.section("control_lease"), &lease)
	if lease.LeaseUntil.After(now) {
		return fmt.Errorf("%w: control lease valid until %s", ErrLiveWriter, lease.LeaseUntil.Format(time.RFC3339))
	}
	if !lease.LastHeartbeat.IsZero() && now.Sub(lease.LastHeartbeat.Time) < LiveWindow {
		return fmt.Errorf("%w: lease heartbeat %s", ErrLiveWriter, lease.LastHeartbeat.Format(time.RFC3339))
	}
	for _, k := range []string{"service_owner", "lock_owner"} {
		var o legacyOwner
		_, _ = decode(l.section(k), &o)
		if !o.LastHeartbeat.IsZero() && now.Sub(o.LastHeartbeat.Time) < LiveWindow {
			return fmt.Errorf("%w: %s pid %d heartbeat %s", ErrLiveWriter, k, o.PID, o.LastHeartbeat.Format(time.RFC3339))
		}
	}
	return nil
}

// lockSource takes <dir>/state.json.lock (the old service holds it while
// running). The returned function releases it.
func lockSource(dir string, create bool) (func(), error) {
	path := filepath.Join(dir, "state.json.lock")
	if _, err := os.Stat(path); err != nil && !create {
		return func() {}, nil
	}
	fl := flock.New(path)
	ok, err := fl.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s is held", ErrLiveWriter, path)
	}
	return func() { fl.Unlock() }, nil
}
