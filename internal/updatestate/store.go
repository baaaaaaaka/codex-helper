// Package updatestate owns the native records shared by helper update callers.
// Runtime selection and process ownership remain in their existing packages.
package updatestate

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

const Filename = "update-state.sqlite"

type Store struct{ db *sql.DB }
type Check struct {
	Next      time.Time `json:"next"`
	Failures  int       `json:"failures,omitempty"`
	LastError string    `json:"last_error,omitempty"`
}
type Pending struct {
	Schema         int    `json:"schema"`
	TargetVersion  string `json:"target_version"`
	TargetSHA256   string `json:"target_sha256"`
	PreviousActive string `json:"previous_active,omitempty"`
	TargetActive   string `json:"target_active"`
	RuntimeRoot    string `json:"runtime_root"`
	EntryPath      string `json:"entry_path"`
	RecordPath     string `json:"record_path,omitempty"`
	RequestID      string `json:"request_id"`
}
type ActivationOwner struct {
	Version       int       `json:"version"`
	Kind          string    `json:"kind"`
	TargetVersion string    `json:"target_version"`
	CreatedAt     time.Time `json:"created_at"`
}
type ActivationResult struct {
	Version   int       `json:"version"`
	Status    string    `json:"status"`
	Message   string    `json:"message"`
	Source    string    `json:"source"`
	Dest      string    `json:"dest"`
	Want      string    `json:"want"`
	UpdatedAt time.Time `json:"updated_at"`
}

func Open(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(filepath.Join(root, Filename), false)
	if err != nil {
		return nil, err
	}
	if err = migrate(db, root); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}

// OpenRead creates no database for a new installation. A legacy source enters
// the one-time importer; native state is never replaced from an old source.
func OpenRead(root string) (*Store, error) {
	path := filepath.Join(root, Filename)
	if _, err := sqlitestate.Inspect(path); os.IsNotExist(err) {
		found, err := hasLegacy(root)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, os.ErrNotExist
		}
		s, err := Open(root)
		if err != nil {
			return nil, err
		}
		s.Close()
	} else if err != nil {
		return nil, err
	}
	info, err := sqlitestate.Inspect(path)
	if err != nil {
		return nil, err
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("update state permissions %04o expose private state", info.Mode().Perm())
	}
	db, err := sqlitestate.Open(path, true)
	if err != nil {
		return nil, err
	}
	var v int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err == nil && v == 0 {
		db.Close()
		migrated, err := Open(root)
		if err != nil {
			return nil, err
		}
		migrated.Close()
		return OpenRead(root)
	}
	if err == nil && v != 1 {
		err = fmt.Errorf("unsupported update state database version %d", v)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}
func (s *Store) Close() error { return s.db.Close() }
func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
func parse(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
func noRow(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return os.ErrNotExist
	}
	return err
}
func (s *Store) Check() (Check, error) {
	var r Check
	var next string
	err := s.db.QueryRow(`SELECT next,failures,last_error FROM auto_checks WHERE id=1`).Scan(&next, &r.Failures, &r.LastError)
	if errors.Is(err, sql.ErrNoRows) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	r.Next, err = parse(next)
	return r, err
}

const checkPut = `INSERT INTO auto_checks VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET next=excluded.next,failures=excluded.failures,last_error=excluded.last_error WHERE (next,failures,last_error) IS NOT (excluded.next,excluded.failures,excluded.last_error)`

func (s *Store) SaveCheck(r Check) error {
	_, err := s.db.Exec(checkPut, stamp(r.Next), r.Failures, r.LastError)
	return err
}
func (s *Store) Pending() (Pending, error) {
	var p Pending
	err := s.db.QueryRow(`SELECT schema_version,target_version,target_sha256,previous_active,target_active,runtime_root,entry_path,record_path,request_id FROM pending_updates WHERE id=1`).Scan(&p.Schema, &p.TargetVersion, &p.TargetSHA256, &p.PreviousActive, &p.TargetActive, &p.RuntimeRoot, &p.EntryPath, &p.RecordPath, &p.RequestID)
	if err != nil {
		return p, noRow(err)
	}
	if p.Schema != 1 {
		return p, fmt.Errorf("unsupported pending update schema %d", p.Schema)
	}
	return p, nil
}

const pendingPut = `INSERT INTO pending_updates VALUES(1,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET schema_version=excluded.schema_version,target_version=excluded.target_version,target_sha256=excluded.target_sha256,previous_active=excluded.previous_active,target_active=excluded.target_active,runtime_root=excluded.runtime_root,entry_path=excluded.entry_path,record_path=excluded.record_path,request_id=excluded.request_id WHERE (schema_version,target_version,target_sha256,previous_active,target_active,runtime_root,entry_path,record_path,request_id) IS NOT (excluded.schema_version,excluded.target_version,excluded.target_sha256,excluded.previous_active,excluded.target_active,excluded.runtime_root,excluded.entry_path,excluded.record_path,excluded.request_id)`

func pendingArgs(p Pending) []any {
	return []any{p.Schema, p.TargetVersion, p.TargetSHA256, p.PreviousActive, p.TargetActive, p.RuntimeRoot, p.EntryPath, p.RecordPath, p.RequestID}
}
func (s *Store) SavePending(p Pending) error {
	if p.Schema != 1 {
		return fmt.Errorf("unsupported pending update schema %d", p.Schema)
	}
	_, err := s.db.Exec(pendingPut, pendingArgs(p)...)
	return err
}
func (s *Store) DeletePending(request string) error {
	_, err := s.db.Exec(`DELETE FROM pending_updates WHERE id=1 AND request_id=?`, request)
	return err
}
func (s *Store) Owner(path string) (ActivationOwner, error) {
	var v ActivationOwner
	var created string
	err := s.db.QueryRow(`SELECT version,kind,target_version,created_at FROM activation_owners WHERE pending_path=?`, path).Scan(&v.Version, &v.Kind, &v.TargetVersion, &created)
	if err != nil {
		return v, noRow(err)
	}
	if v.Version != 1 {
		return v, fmt.Errorf("unsupported activation owner version %d", v.Version)
	}
	v.CreatedAt, err = parse(created)
	return v, err
}

const ownerPut = `INSERT INTO activation_owners VALUES(?,?,?,?,?) ON CONFLICT(pending_path) DO UPDATE SET version=excluded.version,kind=excluded.kind,target_version=excluded.target_version,created_at=excluded.created_at WHERE (version,kind,target_version,created_at) IS NOT (excluded.version,excluded.kind,excluded.target_version,excluded.created_at)`

func (s *Store) SaveOwner(path string, v ActivationOwner) error {
	if v.Version != 1 {
		return fmt.Errorf("unsupported activation owner version %d", v.Version)
	}
	_, err := s.db.Exec(ownerPut, path, v.Version, v.Kind, v.TargetVersion, stamp(v.CreatedAt))
	return err
}

const resultPut = `INSERT INTO activation_results VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(pending_path) DO UPDATE SET version=excluded.version,status=excluded.status,message=excluded.message,source=excluded.source,dest=excluded.dest,want=excluded.want,updated_at=excluded.updated_at WHERE (version,status,message,source,dest,want,updated_at) IS NOT (excluded.version,excluded.status,excluded.message,excluded.source,excluded.dest,excluded.want,excluded.updated_at)`

func (s *Store) SaveResult(path string, v ActivationResult) error {
	if v.Version != 1 {
		return fmt.Errorf("unsupported activation result version %d", v.Version)
	}
	_, err := s.db.Exec(resultPut, path, v.Version, v.Status, v.Message, v.Source, v.Dest, v.Want, stamp(v.UpdatedAt))
	return err
}
func (s *Store) Result(path string) (ActivationResult, error) {
	var v ActivationResult
	var updated string
	err := s.db.QueryRow(`SELECT version,status,message,source,dest,want,updated_at FROM activation_results WHERE pending_path=?`, path).Scan(&v.Version, &v.Status, &v.Message, &v.Source, &v.Dest, &v.Want, &updated)
	if err != nil {
		return v, noRow(err)
	}
	if v.Version != 1 {
		return v, fmt.Errorf("unsupported activation result version %d", v.Version)
	}
	v.UpdatedAt, err = parse(updated)
	return v, err
}
