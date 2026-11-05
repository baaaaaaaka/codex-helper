package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	sqlite "modernc.org/sqlite"
)

// Ident identifies the process that wants to own the store (§2).
type Ident struct {
	Host        string // hostname of this process
	InstallHost string // host this service was installed for ("" = not pinned)
	PID         int
	BootID      string // /proc/sys/kernel/random/boot_id or equivalent
}

// Owner is the owner row.
type Owner struct {
	Generation  int64
	Host        string
	InstallHost string
	PID         int
	BootID      string
	HeartbeatAt int64
}

// DisabledFile is the kill-switch file name next to the database.
const DisabledFile = "DISABLED"

// Acquire takes ownership of the store and returns the new generation.
//
// Rules:
//   - <dir>/DISABLED blocks acquire on every host.
//   - a service pinned to another host (InstallHost != Host) never acquires.
//   - a holder with a heartbeat younger than OwnerStaleAfter blocks acquire,
//     unless it is a dead process of the same host and boot.
func (s *Store) Acquire(id Ident) (int64, error) {
	if s.w == nil {
		return 0, errors.New("store: opened read-only")
	}
	if s.Disabled() {
		return 0, ErrDisabled
	}
	if id.InstallHost != "" && !strings.EqualFold(id.InstallHost, id.Host) {
		return 0, fmt.Errorf("%w: installed for %q, running on %q", ErrWrongHost, id.InstallHost, id.Host)
	}
	var gen int64
	start := s.now()
	err := s.update(false, func(t *Tx) error {
		cur, ok, err := t.owner()
		if err != nil {
			return err
		}
		if ok {
			fresh := t.now-cur.HeartbeatAt < OwnerStaleAfter.Milliseconds()
			sameBoot := strings.EqualFold(cur.Host, id.Host) && cur.BootID == id.BootID && cur.BootID != ""
			deadHere := sameBoot && (cur.PID == id.PID || !s.pid(cur.PID))
			if fresh && !deadHere {
				return fmt.Errorf("%w: %s pid %d (heartbeat %d ms ago)", ErrOwnerLive, cur.Host, cur.PID, t.now-cur.HeartbeatAt)
			}
		}
		gen = cur.Generation + 1
		_, err = t.exec(`INSERT INTO owner(id,generation,host,install_host,pid,boot_id,heartbeat_at)
VALUES (1,?,?,?,?,?,?)
ON CONFLICT(id) DO UPDATE SET generation=excluded.generation, host=excluded.host,
  install_host=excluded.install_host, pid=excluded.pid, boot_id=excluded.boot_id,
  heartbeat_at=excluded.heartbeat_at`, gen, id.Host, id.InstallHost, id.PID, id.BootID, t.now)
		return err
	})
	if isBusy(err) {
		return 0, s.busyOwner(err)
	}
	if err != nil {
		return 0, err
	}
	s.lease.Lock()
	s.gen, s.beat, s.fenced = gen, start, false
	s.lease.Unlock()
	return gen, nil
}

// isBusy reports SQLITE_BUSY: the write lock stayed held by another
// connection for the whole busy_timeout.
func isBusy(err error) bool {
	var e *sqlite.Error
	return errors.As(err, &e) && e.Code()&0xff == 5 // SQLITE_BUSY
}

// busyOwner explains a busy store at acquire. Write transactions are short
// and never span I/O, so a lock held for the whole busy_timeout belongs to
// a process that stopped inside one (frozen, or wedged on its disk). Such a
// lock cannot be broken: it goes away only when that process resumes or
// exits. While the owner's heartbeat is fresh the refusal is the ordinary
// ErrOwnerLive; once it is stale, ErrLocked names the owner.
func (s *Store) busyOwner(cause error) error {
	o, ok, err := s.OwnerInfo() // WAL readers are not blocked by the writer
	if err != nil || !ok {
		return fmt.Errorf("%w (%v)", ErrLocked, cause)
	}
	age := time.Duration(s.ms()-o.HeartbeatAt) * time.Millisecond
	if age < OwnerStaleAfter {
		return fmt.Errorf("%w: %s pid %d (heartbeat %d ms ago; store busy)", ErrOwnerLive, o.Host, o.PID, age.Milliseconds())
	}
	return fmt.Errorf("%w: the owner %s pid %d last renewed its heartbeat %s ago", ErrLocked, o.Host, o.PID, age.Round(time.Second))
}

func (t *Tx) owner() (Owner, bool, error) {
	var o Owner
	err := t.row(`SELECT generation,host,install_host,pid,boot_id,heartbeat_at FROM owner WHERE id = 1`).
		Scan(&o.Generation, &o.Host, &o.InstallHost, &o.PID, &o.BootID, &o.HeartbeatAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Owner{}, false, nil
	}
	return o, err == nil, err
}

// Heartbeat refreshes the owner heartbeat. It is the one periodic write.
//
// It returns ErrDisabled without renewing the heartbeat once the kill-switch
// file exists, so a running owner stops at its next heartbeat instead of
// only the next start being refused.
func (s *Store) Heartbeat() error {
	s.lease.Lock()
	defer s.lease.Unlock()
	return s.heartbeatLocked()
}

func (s *Store) heartbeatLocked() error {
	if s.gen == 0 {
		return ErrNotAcquired
	}
	if s.fenced {
		return ErrFenced
	}
	if s.Disabled() {
		return ErrDisabled
	}
	start := s.now()
	n, err := affected(s.w.Exec(`UPDATE owner SET heartbeat_at = ? WHERE id = 1 AND generation = ?`, start.UnixMilli(), s.gen))
	if err != nil {
		return err
	}
	if n == 0 {
		s.fenced = true
		return ErrFenced
	}
	s.beat = start
	return nil
}

// ownerLease is how long after the start of its last successful heartbeat
// (or acquire) the owner may act outside the store without renewing it.
//
// Fence rule. Another process takes over only when, by its own clock, the
// owner's heartbeat_at is OwnerStaleAfter old (Acquire, under the write
// lock). A heartbeat that commits with our generation therefore proves that
// no takeover happened before it and none can happen until heartbeat_at +
// OwnerStaleAfter. Acting only within ownerLease of the heartbeat's start
// leaves OwnerStaleAfter - ownerLease (45 s) for clock skew between hosts.
// Past the lease the owner must renew first: a renewal that commits proves
// it is still the owner however long it was stopped (frozen, suspended);
// one that finds a newer generation fences it for good.
const ownerLease = OwnerStaleAfter / 2

// Fence reports whether this process may write to the outside world (a
// Graph request) on the owner's behalf now. Within the lease it costs one
// clock read; past it, it renews the heartbeat first (one write; it waits
// up to busy_timeout for the write lock). It returns nil, ErrFenced,
// ErrNotAcquired, ErrDisabled, or a store error (busy: try again).
//
// The caller must not hold a write transaction (the renewal needs the one
// writer connection); no transaction spans I/O.
func (s *Store) Fence() error {
	s.lease.Lock()
	defer s.lease.Unlock()
	if s.gen != 0 && !s.fenced && leaseAge(s.now(), s.beat) < ownerLease {
		return nil
	}
	return s.heartbeatLocked()
}

// leaseAge is the time since start by the monotonic clock or the wall
// clock, whichever is longer: the monotonic clock does not advance while
// the machine is suspended, the wall clock can be set back.
func leaseAge(now, start time.Time) time.Duration {
	d := now.Sub(start)
	if w := now.Round(0).Sub(start.Round(0)); w > d {
		d = w
	}
	return d
}

// Disabled reports whether the kill-switch file <dir>/DISABLED exists.
func (s *Store) Disabled() bool {
	_, err := os.Stat(filepath.Join(filepath.Dir(s.path), DisabledFile))
	return err == nil
}

// Release ends ownership so the next process can acquire immediately.
func (s *Store) Release() error {
	s.lease.Lock()
	defer s.lease.Unlock()
	if s.gen == 0 {
		return nil
	}
	_, err := s.w.Exec(`UPDATE owner SET heartbeat_at = 0 WHERE id = 1 AND generation = ?`, s.gen)
	s.gen = 0
	return err
}

// OwnerInfo reads the owner row through the read pool.
func (s *Store) OwnerInfo() (Owner, bool, error) {
	var o Owner
	err := s.r.QueryRow(`SELECT generation,host,install_host,pid,boot_id,heartbeat_at FROM owner WHERE id = 1`).
		Scan(&o.Generation, &o.Host, &o.InstallHost, &o.PID, &o.BootID, &o.HeartbeatAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Owner{}, false, nil
	}
	return o, err == nil, err
}
