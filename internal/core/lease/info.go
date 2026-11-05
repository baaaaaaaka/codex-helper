package lease

import (
	"os"
	"strings"
	"time"
)

// Info is a one-time read of the lease for status and other read-only
// commands. It cannot tell a live owner from a stale one by itself (that
// takes watching the slot over time); Age is from the owner's clock and
// is for display only.
type Info struct {
	// Exists: the lease file exists (the store is managed by the lease).
	Exists bool
	// Head is the highest generation that holds or is taking the lease.
	Head    Record
	HeadGen int64
	HeadOK  bool // Head decoded
	// Current is the latest generation that served; DB is its store.
	Current    Record
	CurrentGen int64
	DB         string
	Disabled   bool
}

// Read reads the lease next to the store anchor.
func Read(anchor string) (Info, error) {
	p := newPaths(anchor)
	var in Info
	if _, err := os.Stat(p.disabled()); err == nil {
		in.Disabled = true
	}
	if _, err := os.Stat(p.records); err == nil {
		in.Exists = true
	}
	v, err := readView(p.records)
	if err != nil {
		return in, err
	}
	in.HeadGen = v.head()
	in.Head, in.HeadOK = v.record(in.HeadGen)
	in.Current, in.CurrentGen, _ = v.current(v.top + 1)
	if in.CurrentGen != 0 {
		in.DB = p.dbPath(in.Current.DB)
	} else {
		in.DB = coldSource(p)
	}
	return in, nil
}

// Path is the lease file for a store anchor.
func Path(anchor string) string { return newPaths(anchor).records }

// DBHost is the host whose processes open the current store with SQLite:
// the host of the generation that served last, else of the head, else ""
// (no generation yet: any host may read the store).
func (in Info) DBHost() string {
	switch {
	case in.CurrentGen != 0:
		return in.Current.Host
	case in.HeadOK:
		return in.Head.Host
	}
	return ""
}

// Readable reports whether a process on host may open the current store
// with SQLite (read-only commands too): only processes of one host ever
// open a store file, because SQLite's WAL index is shared memory that does
// not work across hosts.
func (in Info) Readable(host string) bool {
	h := in.DBHost()
	return h == "" || strings.EqualFold(h, host)
}

// Live reports whether the head holds the lease (claimed or active) and
// has renewed within stale by its own clock. It is a display heuristic for
// one-time reads; standbys decide by watching the slot.
func (in Info) Live(now time.Time, stale time.Duration) bool {
	if !in.HeadOK || (in.Head.State != Active && in.Head.State != Claimed) {
		return false
	}
	return now.Sub(time.UnixMilli(in.Head.Wall)) < stale
}

// Age is the time since the head's last renewal by its own clock.
func (in Info) Age(now time.Time) time.Duration {
	if !in.HeadOK {
		return 0
	}
	return now.Sub(time.UnixMilli(in.Head.Wall))
}
