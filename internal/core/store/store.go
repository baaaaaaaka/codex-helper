// Package store is the native-SQLite core store (STORE-DESIGN.md).
//
// One file, typed columns, no JSON, no triggers. One writer connection; every
// write transaction starts with BEGIN IMMEDIATE and checks the owner
// generation (the stale-writer fence for the store; Fence is the one for
// writes outside it). No method performs network or Codex I/O, and no
// caller may perform I/O inside a write transaction: the write lock is held
// for its duration.
//
// On a network filesystem the store has a single connection for reads and
// writes, in EXCLUSIVE locking mode (see OpenWith). So no Update or View
// callback may call another method of the Store, and no goroutine may wait
// for the Store while it holds a lock that such a callback takes: either
// would wait forever for the one connection. The file lock is held from
// open to Close, so no other process opens the store meanwhile; a command
// that needs the state of a running helper reads what the helper
// publishes instead (internal/cli, core-status.json).
package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	_ "modernc.org/sqlite" // driver "sqlite"
)

var (
	// ErrFenced means another process acquired the store after us. The caller
	// must stop writing and exit.
	ErrFenced = errors.New("store: fenced by a newer owner generation")
	// ErrNotAcquired means Update was called before Acquire.
	ErrNotAcquired = errors.New("store: owner not acquired")
	// ErrOwnerLive means another live process holds the store.
	ErrOwnerLive = errors.New("store: another live owner holds the store")
	// ErrLocked means the store's write lock stayed held for the whole
	// busy timeout while the owner's heartbeat is stale: the owner stopped
	// inside a write transaction (frozen). The lock is released only when
	// that process resumes or exits.
	ErrLocked = errors.New("store: the write lock is held by a stalled process")
	// ErrDisabled means the kill-switch file <dir>/DISABLED exists.
	ErrDisabled = errors.New("store: disabled by kill-switch file")
	// ErrWrongHost means the service was installed for another host.
	ErrWrongHost = errors.New("store: service is pinned to another host")
	// ErrUnknownVersion means the file has a newer or foreign schema.
	ErrUnknownVersion = errors.New("store: unknown schema version")
	// ErrNotFound is returned for a missing row.
	ErrNotFound = errors.New("store: not found")
	// ErrState is returned for an illegal state transition.
	ErrState = errors.New("store: illegal state transition")
)

// OwnerStaleAfter is how old a heartbeat must be before another process may
// take over (§2).
const OwnerStaleAfter = 90 * time.Second

// Options tune Open. The zero value is production behaviour.
type Options struct {
	// Now is the clock (unix ms is derived from it). Defaults to time.Now.
	Now func() time.Time
	// PIDAlive reports whether pid is alive on this host. Defaults to an OS check.
	PIDAlive func(pid int) bool
	// ReadOnly opens the file for reading only (a command run while no
	// helper holds the store); Update fails. On a network filesystem it
	// takes the file's exclusive lock like any open there (OpenWith), so
	// the caller must close it promptly.
	ReadOnly bool
	// Lease, when set, is the owner lease of a store in a directory that
	// hosts may share (internal/core/lease). It replaces the heartbeat in
	// the owner row: every write transaction checks it first, and Fence
	// verifies it.
	Lease Lease

	// Exclusive forces the network-filesystem mode (one connection,
	// locking_mode=EXCLUSIVE) on any filesystem. Production leaves it
	// unset: the mode follows the filesystem. The engine's tests set it,
	// so a store read made inside a transaction (which waits forever on
	// the single connection) fails there instead of only on NFS.
	Exclusive bool
}

// Lease is the owner lease that gates writes (internal/core/lease.Holder).
type Lease interface {
	// Check returns nil while the lease is held. It is cheap within the
	// lease's validity and renews past it.
	Check() error
	// Verify reads the lease now: nil means no later owner has claimed it.
	Verify() error
}

// leaseErr maps the lease's errors to the store's.
func leaseErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, lease.ErrFenced):
		return ErrFenced
	case errors.Is(err, lease.ErrDisabled):
		return ErrDisabled
	case errors.Is(err, lease.ErrReleased):
		return ErrNotAcquired
	}
	return err
}

// Store is an open core store.
type Store struct {
	path string
	w    *sql.DB // single writer connection, BEGIN IMMEDIATE
	r    *sql.DB // read-only pool; w itself in exclusive mode
	now  func() time.Time
	pid  func(int) bool
	gen  int64

	lease  sync.Mutex // guards beat and fenced; held by Heartbeat, Fence, Acquire, Release
	beat   time.Time  // start of the last successful heartbeat or acquire
	fenced bool       // a heartbeat found a newer generation
	ls     Lease      // the owner lease (nil: the owner row's heartbeat)

	changed chan struct{} // signalled after each commit (Changes)
}

// Open opens or creates the store at path with production options.
func Open(path string) (*Store, error) { return OpenWith(path, Options{}) }

// busyTimeoutMS is how long a statement waits for a lock another
// connection holds (tests shorten it).
var busyTimeoutMS = 5000

// pragmas are the connection settings. A network filesystem gets
// synchronous=FULL: another host may have to take the store over after
// this one dies, so every commit must be on the server (fsync is an NFS
// COMMIT), not only in this host's page cache.
func pragmas(path string, ro bool) []string {
	sync := "synchronous(NORMAL)"
	if !ro && networkFS(filepath.Dir(path)) {
		sync = "synchronous(FULL)"
	}
	return []string{
		fmt.Sprintf("busy_timeout(%d)", busyTimeoutMS),
		"foreign_keys(ON)",
		sync,
		"temp_store(MEMORY)",
		"cache_size(-16000)",
		"journal_size_limit(67108864)",
	}
}

func dsn(path string, ro bool) string {
	q := url.Values{}
	for _, p := range pragmas(path, ro) {
		q.Add("_pragma", p)
	}
	if ro {
		q.Set("mode", "ro")
	} else {
		q.Set("_txlock", "immediate")
	}
	return "file:" + filepath.ToSlash(path) + "?" + q.Encode()
}

// openDB opens the writer of the store at path: one connection, the only
// one in exclusive mode.
//
// Exclusive mode is for a network filesystem. In WAL mode every transaction
// takes and drops fcntl locks on the -shm file, which the connection also
// maps into memory. On NFS an unlock first waits for the file's in-flight
// I/O, and with several connections in one process (a writer and a read
// pool) the others keep dirtying the mapping: the unlock starves and the
// store stalls for minutes (TAKEOVER-REPORT §9.3, finding 1). With
// locking_mode=EXCLUSIVE set before the database is first read, SQLite
// keeps the WAL index in heap memory: there is no -shm, and the file lock
// is taken once and held until Close. That is safe because only the lease
// holder opens the store, and only one connection then uses it.
//
// A crash leaves the database and its WAL, as in the shared mode; whoever
// opens them next rebuilds the WAL index from the WAL (CopySnapshot).
//
// readOnly (exclusive mode only) adds query_only: the connection never
// writes a row. It still needs a writable file descriptor, because opening
// a WAL database in exclusive mode takes a write lock on the file; and when
// it closes, SQLite folds a WAL left by a crash into the database, as the
// next owner would at its open.
func openDB(path string, exclusive, readOnly bool) (*sql.DB, error) {
	if !exclusive {
		return sql.Open("sqlite", dsn(path, readOnly))
	}
	// The driver applies DSN pragmas in name order, and some of them read
	// the database (cache_size, synchronous). The locking mode must come
	// before that first read, so the DSN carries only it and the busy
	// timeout, and the connector applies the rest.
	q := url.Values{}
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMS))
	q.Add("_pragma", "locking_mode(EXCLUSIVE)")
	q.Set("_txlock", "immediate")
	probe, err := sql.Open("sqlite", "")
	if err != nil {
		return nil, err
	}
	drv := probe.Driver()
	_ = probe.Close()
	var after []string
	for _, p := range pragmas(path, false) {
		if !strings.HasPrefix(p, "busy_timeout") {
			after = append(after, "PRAGMA "+p)
		}
	}
	if readOnly {
		after = append(after, "PRAGMA query_only(1)")
	}
	return sql.OpenDB(exclusiveConnector{drv: drv, dsn: "file:" + filepath.ToSlash(path) + "?" + q.Encode(), after: after}), nil
}

// exclusiveConnector opens a connection with the DSN, then applies the
// remaining pragmas in order.
type exclusiveConnector struct {
	drv   driver.Driver
	dsn   string
	after []string
}

func (c exclusiveConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	ex, ok := conn.(driver.ExecerContext)
	if !ok {
		conn.Close()
		return nil, errors.New("store: the SQLite driver cannot execute pragmas")
	}
	for _, p := range c.after {
		if _, err := ex.ExecContext(ctx, p, nil); err != nil {
			conn.Close()
			return nil, fmt.Errorf("store: %s: %w", p, err)
		}
	}
	return conn, nil
}

func (c exclusiveConnector) Driver() driver.Driver { return c.drv }

// OpenWith opens or creates the store at path.
//
// On a network filesystem (networkFS) the store is opened in exclusive
// mode (openDB): a single connection serves reads and writes, and other
// processes cannot open the file while it is open. That holds for a
// read-only Store too, so a command opens the store only while no helper
// holds it (the lease) and closes it at once. A helper that starts
// meanwhile waits for it (openLockWait).
func OpenWith(path string, opt Options) (*Store, error) {
	s := &Store{path: path, now: opt.Now, pid: opt.PIDAlive, ls: opt.Lease, changed: make(chan struct{}, 1)}
	if s.now == nil {
		s.now = time.Now
	}
	if s.pid == nil {
		s.pid = pidAlive
	}
	_, statErr := os.Stat(path)
	fresh := errors.Is(statErr, os.ErrNotExist)
	if fresh && opt.ReadOnly {
		return nil, fmt.Errorf("store: %s does not exist", path)
	}
	exclusive := opt.Exclusive || networkFS(filepath.Dir(path))
	if opt.ReadOnly {
		return s.openReadOnly(exclusive)
	}
	deadline := time.Now().Add(openLockWait)
	for {
		err := s.openWriter(fresh, exclusive)
		if err == nil {
			break
		}
		if !isBusy(err) || !time.Now().Before(deadline) {
			if isBusy(err) {
				err = fmt.Errorf("%w (%v)", ErrBusy, err)
			}
			return nil, err
		}
		// Another process holds the file for a moment (a command reading
		// it while no helper ran); each attempt already waited for the
		// busy timeout.
		time.Sleep(200 * time.Millisecond)
	}
	if exclusive {
		s.r = s.w
		return s, nil
	}
	r, err := sql.Open("sqlite", dsn(path, true))
	if err != nil {
		s.Close()
		return nil, err
	}
	r.SetMaxOpenConns(4)
	s.r = r
	return s, nil
}

// openLockWait bounds how long OpenWith waits for a file another process
// holds in exclusive mode (a read-only command; each holds it for well
// under a second).
var openLockWait = 30 * time.Second

// ErrBusy means another process held the store file for longer than the
// open waits.
var ErrBusy = errors.New("store: the file is in use by another process")

func (s *Store) openWriter(fresh, exclusive bool) error {
	w, err := openDB(s.path, exclusive, false)
	if err != nil {
		return err
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	w.SetConnMaxLifetime(0)
	s.w = w
	if err := s.prepare(fresh, exclusive); err != nil {
		w.Close()
		s.w = nil
		return err
	}
	return nil
}

// openReadOnly opens the file for reading: a pool of read-only
// connections, or on a network filesystem the one exclusive connection
// with query_only.
func (s *Store) openReadOnly(exclusive bool) (*Store, error) {
	r, err := openDB(s.path, exclusive, true)
	if err != nil {
		return nil, err
	}
	if exclusive {
		r.SetMaxOpenConns(1)
		r.SetMaxIdleConns(1)
		r.SetConnMaxLifetime(0)
	} else {
		r.SetMaxOpenConns(4)
	}
	s.r = r
	if err := checkVersion(r, false); err != nil {
		r.Close()
		if isBusy(err) {
			err = fmt.Errorf("%w (%v)", ErrBusy, err)
		}
		return nil, err
	}
	return s, nil
}

func (s *Store) prepare(fresh, exclusive bool) error {
	ctx := context.Background()
	if exclusive {
		var lm string
		if err := s.w.QueryRowContext(ctx, "PRAGMA locking_mode").Scan(&lm); err != nil {
			return err
		}
		if lm != "exclusive" {
			return fmt.Errorf("store: locking_mode is %q, want exclusive", lm)
		}
	}
	if fresh {
		// Settings that must precede the first table.
		for _, p := range []string{
			"PRAGMA page_size = 4096",
			"PRAGMA auto_vacuum = INCREMENTAL",
			fmt.Sprintf("PRAGMA application_id = %d", ApplicationID),
		} {
			if _, err := s.w.ExecContext(ctx, p); err != nil {
				return err
			}
		}
	}
	var mode string
	if err := s.w.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		return err
	}
	if mode != "wal" {
		return fmt.Errorf("store: journal_mode is %q, want wal", mode)
	}
	// wal_autocheckpoint stays at the SQLite default (1000). Never 0 (§5.3-1).
	if _, err := s.w.ExecContext(ctx, "PRAGMA wal_autocheckpoint = 1000"); err != nil {
		return err
	}
	return s.migrate(ctx)
}

func checkVersion(db *sql.DB, allowOlder bool) error {
	var app, ver int64
	if err := db.QueryRow("PRAGMA application_id").Scan(&app); err != nil {
		return err
	}
	if err := db.QueryRow("PRAGMA user_version").Scan(&ver); err != nil {
		return err
	}
	if app != ApplicationID {
		return fmt.Errorf("%w: application_id %#x", ErrUnknownVersion, app)
	}
	if ver > SchemaVersion || (!allowOlder && ver != SchemaVersion) {
		return fmt.Errorf("%w: user_version %d (this build understands %d)", ErrUnknownVersion, ver, SchemaVersion)
	}
	return nil
}

func (s *Store) migrate(ctx context.Context) error {
	var app, ver int64
	if err := s.w.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return err
	}
	if err := s.w.QueryRowContext(ctx, "PRAGMA user_version").Scan(&ver); err != nil {
		return err
	}
	if app == 0 && ver == 0 {
		// Created by a previous Open that died before the schema commit, or an
		// empty file: only accept it if it has no tables at all.
		var n int
		if err := s.w.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema").Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("%w: not a core store (application_id 0)", ErrUnknownVersion)
		}
		if _, err := s.w.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id = %d", ApplicationID)); err != nil {
			return err
		}
		app = ApplicationID
	}
	if app != ApplicationID {
		return fmt.Errorf("%w: application_id %#x", ErrUnknownVersion, app)
	}
	if ver > SchemaVersion {
		return fmt.Errorf("%w: user_version %d (this build understands %d)", ErrUnknownVersion, ver, SchemaVersion)
	}
	for v := ver; v < SchemaVersion; v++ {
		tx, err := s.w.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, stmt := range migrations[v] {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				tx.Rollback()
				return fmt.Errorf("store: migration %d: %w", v+1, err)
			}
		}
		if v == 0 {
			now := s.ms()
			if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES ('created_at', ?)`, fmt.Sprint(now)); err != nil {
				tx.Rollback()
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", v+1)); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// Path returns the database file path.
func (s *Store) Path() string { return s.path }

// Close closes all connections.
func (s *Store) Close() error {
	var err error
	if s.r != nil && s.r != s.w {
		err = s.r.Close()
	}
	if s.w != nil {
		if e := s.w.Close(); e != nil {
			err = e
		}
	}
	return err
}

func (s *Store) ms() int64 { return s.now().UnixMilli() }

// Generation returns the owner generation acquired by this Store (0 if none).
func (s *Store) Generation() int64 { return s.gen }

// Tx is one write transaction. It is only valid inside Update.
type Tx struct {
	tx  *sql.Tx
	s   *Store
	now int64
}

// Now returns the transaction's clock in unix ms.
func (t *Tx) Now() int64 { return t.now }

// Update runs fn in one BEGIN IMMEDIATE transaction after checking that the
// owner generation is still ours. fn's error rolls the transaction back.
// fn must not call the Store's methods (see the package comment).
func (s *Store) Update(fn func(*Tx) error) error {
	if s.w == nil {
		return errors.New("store: opened read-only")
	}
	if s.gen == 0 {
		return ErrNotAcquired
	}
	return s.update(true, fn)
}

func (s *Store) update(fence bool, fn func(*Tx) error) error {
	if s.ls != nil {
		if err := s.ls.Check(); err != nil {
			return leaseErr(err)
		}
	}
	ctx := context.Background()
	tx, err := s.w.BeginTx(ctx, nil) // _txlock=immediate
	if err != nil {
		return err
	}
	if fence {
		var gen int64
		err := tx.QueryRowContext(ctx, `SELECT generation FROM owner WHERE id = 1`).Scan(&gen)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && gen != s.gen) {
			tx.Rollback()
			return ErrFenced
		}
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	t := &Tx{tx: tx, s: s, now: s.ms()}
	if err := fn(t); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	select {
	case s.changed <- struct{}{}:
	default: // one signal is pending already
	}
	return nil
}

// Changes receives after a write transaction commits. Signals coalesce:
// one pending signal stands for any number of commits. It is for a single
// reader (the helper's status publisher).
func (s *Store) Changes() <-chan struct{} { return s.changed }

// View runs fn against the read-only pool inside one read transaction.
// fn must not call the Store's methods (see the package comment).
func (s *Store) View(fn func(q Querier) error) error {
	tx, err := s.r.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
}

// Querier is the read surface shared by *sql.DB, *sql.Tx and *sql.Conn.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (t *Tx) exec(q string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(context.Background(), q, args...)
}

func (t *Tx) row(q string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(context.Background(), q, args...)
}

func (t *Tx) rows(q string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(context.Background(), q, args...)
}

// Meta returns a meta value.
func (t *Tx) Meta(key string) (string, bool, error) {
	var v string
	err := t.row(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v, err == nil, err
}

// SetMeta writes a meta value (only when it changes).
func (t *Tx) SetMeta(key, value string) error {
	_, err := t.exec(`INSERT INTO meta(key,value) VALUES (?,?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value WHERE value <> excluded.value`, key, value)
	return err
}

// Meta reads a meta value through the read pool.
func (s *Store) Meta(key string) (string, bool, error) {
	var v string
	err := s.r.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v, err == nil, err
}

func nullInt(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func affected(res sql.Result, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
