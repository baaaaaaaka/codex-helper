package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	if _, err := os.Stat(filepath.Join(filepath.Dir(s.path), DisabledFile)); err == nil {
		return 0, ErrDisabled
	}
	if id.InstallHost != "" && !strings.EqualFold(id.InstallHost, id.Host) {
		return 0, fmt.Errorf("%w: installed for %q, running on %q", ErrWrongHost, id.InstallHost, id.Host)
	}
	var gen int64
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
	if err != nil {
		return 0, err
	}
	s.gen = gen
	return gen, nil
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
func (s *Store) Heartbeat() error {
	if s.gen == 0 {
		return ErrNotAcquired
	}
	n, err := affected(s.w.Exec(`UPDATE owner SET heartbeat_at = ? WHERE id = 1 AND generation = ?`, s.ms(), s.gen))
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrFenced
	}
	return nil
}

// Release ends ownership so the next process can acquire immediately.
func (s *Store) Release() error {
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
