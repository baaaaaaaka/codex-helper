// Package lease decides which process owns the Teams core store when
// several hosts share one state directory (an NFS home). TAKEOVER-REPORT.md
// has the full design; this is the summary.
//
// # Medium
//
// Next to the store, <base>.lease is a file of fixed-size slots. Slot N
// is generation N's record: its owner's identity, its state, a heartbeat
// counter and the store file it serves. The file is never renamed or
// replaced, so its name always resolves to the same inode, and every read
// opens it again: NFS close-to-open consistency then returns what the
// writers had flushed on close. Each slot has a single writer:
//   - The right to write slot N is the hard link <base>.lease.<N>. It is
//     created with link(2) from a unique temporary file. The NFS server
//     arbitrates the link, so exactly one process wins each generation. A
//     lost reply is recognised by the temporary file's link count (the
//     recipe in open(2)).
//   - The winner writes its slot, then reads the slots above it: a
//     non-empty one means a later claimer won, and it backs off. Of two
//     claimers that each write and then read, at least one sees the other.
//
// Nothing depends on fcntl/flock locks, on rename-over (whose old inode a
// client may keep seeing), or on whether a name is absent (negative lookups
// are cached).
//
// # Liveness without synchronized clocks
//
// The owner rewrites its slot every Timing.Beat, which bumps a counter. A
// standby compares the slot's bytes between polls, so any version's record
// works. It counts as stale once the bytes have not changed for
// Timing.Stale, measured by the standby's own monotonic clock. No timestamp
// from another host is ever compared with a local one. The owner may act
// only within Timing.Valid of the start of its last successful renewal
// (its own clock). The only clock assumption is that clock rates on
// different hosts agree to within a factor of 1.5. See Timing.
//
// # The store files
//
// A host opens SQLite only after it holds the lease. Each store file is
// opened with SQLite by processes of exactly one host, ever. Taking over
// from another host, from a stalled owner, or after an unclean claim copies
// the predecessor's store into a new file named for the new generation (see
// Plan). The old owner may thaw with a write transaction still in flight.
// It then writes into its own file, which nobody uses any more, and the
// SQLite shared memory (-shm) is never shared between hosts.
package lease

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrFenced means a later generation owns the store: stop writing,
	// close the store and exit.
	ErrFenced = errors.New("lease: superseded by a newer owner generation")
	// ErrDisabled means the kill-switch file exists.
	ErrDisabled = errors.New("lease: disabled by the kill-switch file")
	// ErrOwnerLive means another live process owns the store (only returned
	// when the caller does not wait).
	ErrOwnerLive = errors.New("lease: another live helper owns the store")
	// ErrReleased means the holder was released.
	ErrReleased = errors.New("lease: released")
)

// DisabledFile is the kill-switch file next to the store.
const DisabledFile = "DISABLED"

// State is a record's state.
type State string

const (
	// Claimed: the generation was won; its owner is preparing (waiting
	// out the previous owner, copying the store). It has not served.
	Claimed State = "claimed"
	// Active: the owner serves from DB.
	Active State = "active"
	// Released: the owner stopped cleanly after closing DB.
	Released State = "released"
	// Aborted: the claim was given up before the owner ever served.
	Aborted State = "aborted"
)

// Record is one generation's slot.
type Record struct {
	V     int    `json:"v"`
	Gen   int64  `json:"gen"`
	State State  `json:"state"`
	Host  string `json:"host"`
	Boot  string `json:"boot,omitempty"`
	PIDNS string `json:"pidns,omitempty"`
	PID   int    `json:"pid"`
	Start string `json:"start,omitempty"`
	Batch bool   `json:"batch,omitempty"`
	Nonce string `json:"nonce"`
	// Beat counts renewals. Observers compare the whole slot, not this.
	Beat int64 `json:"beat"`
	// Wall is the owner's clock at the last renewal (unix ms). It is shown
	// to users and never used to decide anything.
	Wall int64 `json:"wall"`
	// DB is the store file this generation serves, a base name next to
	// the lease file. It is set once the store is ready (Active).
	DB string `json:"db,omitempty"`
	// From and Why describe the takeover that created this generation.
	From string `json:"from,omitempty"`
	Why  string `json:"why,omitempty"`
	Mode string `json:"mode,omitempty"`
}

const (
	recordVersion = 1
	slotSize      = 1024
	slotHeader    = 10 // magic(4) + length(2) + crc32(4)
	slotMagic     = "CXL1"
)

// encode renders r into one slot.
func encode(r Record) ([]byte, error) {
	r.V = recordVersion
	js, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(js) > slotSize-slotHeader {
		return nil, fmt.Errorf("lease: record of %d bytes does not fit a slot", len(js))
	}
	buf := make([]byte, slotSize)
	copy(buf, slotMagic)
	binary.BigEndian.PutUint16(buf[4:], uint16(len(js)))
	binary.BigEndian.PutUint32(buf[6:], crc32.ChecksumIEEE(js))
	copy(buf[slotHeader:], js)
	return buf, nil
}

// decode parses a slot: ok is false for an empty slot, err non-nil for a
// slot with data that is not a complete record of this version (being
// written, torn, or from a newer helper).
func decode(slot []byte) (r Record, ok bool, err error) {
	if isEmpty(slot) {
		return Record{}, false, nil
	}
	if len(slot) < slotHeader || string(slot[:4]) != slotMagic {
		return Record{}, true, errors.New("lease: unknown slot format")
	}
	n := int(binary.BigEndian.Uint16(slot[4:]))
	if slotHeader+n > len(slot) {
		return Record{}, true, errors.New("lease: truncated slot")
	}
	js := slot[slotHeader : slotHeader+n]
	if crc32.ChecksumIEEE(js) != binary.BigEndian.Uint32(slot[6:]) {
		return Record{}, true, errors.New("lease: slot checksum mismatch (being written?)")
	}
	if err := json.Unmarshal(js, &r); err != nil {
		return Record{}, true, err
	}
	if r.V != recordVersion {
		return Record{}, true, fmt.Errorf("lease: record version %d", r.V)
	}
	return r, true, nil
}

func isEmpty(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// paths are the lease's files for a store anchor.
type paths struct {
	dir     string // the store's directory
	base    string // store base name without .sqlite ("core")
	anchor  string // the store's anchor file (<dir>/core.sqlite)
	records string // <dir>/<base>.lease
}

func newPaths(anchor string) paths {
	dir, name := filepath.Split(anchor)
	base := strings.TrimSuffix(name, ".sqlite")
	return paths{dir: filepath.Clean(dir), base: base, anchor: anchor, records: filepath.Join(dir, base+".lease")}
}

func (p paths) claim(gen int64) string { return p.records + "." + strconv.FormatInt(gen, 10) }

// genDB is the store file a copy-mode takeover of generation gen creates.
func (p paths) genDB(gen int64) string {
	return filepath.Join(p.dir, fmt.Sprintf("%s.g%d.sqlite", p.base, gen))
}

// dbPath resolves a record's DB base name ("" = the anchor).
func (p paths) dbPath(name string) string {
	if name == "" {
		return p.anchor
	}
	return filepath.Join(p.dir, filepath.Base(name))
}

func (p paths) disabled() string { return filepath.Join(p.dir, DisabledFile) }

// view is what one read of the lease file shows.
type view struct {
	top   int64            // highest non-empty slot (0: none)
	slots map[int64][]byte // the slots read, by generation
	low   int64            // lowest generation read
}

func (v view) raw(gen int64) []byte { return v.slots[gen] }

func (v view) record(gen int64) (Record, bool) {
	r, ok, err := decode(v.slots[gen])
	return r, ok && err == nil
}

// aborted reports whether a slot holds a record that gave up its claim.
// Such a claimer never opened a store and never will.
func aborted(slot []byte) bool {
	r, ok, err := decode(slot)
	return ok && err == nil && r.State == Aborted
}

// head is the highest slot that holds data and is not aborted: the
// generation that owns the store or is taking it over (0: none).
func (v view) head() int64 {
	for g := v.top; g >= v.low && g >= 1; g-- {
		if s := v.slots[g]; !isEmpty(s) && !aborted(s) {
			return g
		}
	}
	return 0
}

// current is the latest generation below the given one that served
// (Active or Released with a store), and whether a slot strictly between
// holds data that is not an abort: a claimer that may still be preparing.
func (v view) current(below int64) (cur Record, gen int64, gap bool) {
	for g := below - 1; g >= v.low && g >= 1; g-- {
		slot := v.slots[g]
		if isEmpty(slot) {
			continue
		}
		r, ok, err := decode(slot)
		if err == nil && ok && (r.State == Active || r.State == Released) && r.DB != "" {
			return r, g, gap
		}
		if !aborted(slot) {
			gap = true
		}
	}
	return Record{}, 0, gap
}

// readView reads the lease file backwards from its end until a slot that
// served (or the start of the file), in chunks of 16 slots.
func readView(path string) (view, error) {
	v := view{slots: map[int64][]byte{}}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return v, err
	}
	n := (fi.Size() + slotSize - 1) / slotSize
	const chunk = 16
	for hi := n; hi >= 1; {
		lo := max(hi-chunk+1, 1)
		buf := make([]byte, (hi-lo+1)*slotSize)
		m, err := f.ReadAt(buf, (lo-1)*slotSize)
		if err != nil && !errors.Is(err, io.EOF) {
			return v, err
		}
		buf = buf[:m]
		for g := lo; g <= hi; g++ {
			off := min((g-lo)*slotSize, int64(len(buf)))
			end := min(off+slotSize, int64(len(buf)))
			v.slots[g] = append([]byte(nil), buf[off:end]...)
		}
		v.low = lo
		if _, cg, _ := v.current(n + 1); cg != 0 {
			break
		}
		hi = lo - 1
	}
	for g := n; g >= v.low && g >= 1; g-- {
		if !isEmpty(v.slots[g]) {
			v.top = g
			break
		}
	}
	return v, nil
}

// writeSlot writes slot gen in place. Close flushes it to the server (NFS
// close-to-open), and its error reports a failed write-back.
func writeSlot(path string, gen int64, data []byte) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(data, (gen-1)*slotSize); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// readOwn reads slot gen and whether a slot above it holds data that is
// not an abort (a later claimer).
func readOwn(path string, gen int64) (own []byte, later bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	n := (fi.Size() + slotSize - 1) / slotSize
	if n < gen {
		return nil, false, nil
	}
	buf := make([]byte, (n-gen+1)*slotSize)
	m, err := f.ReadAt(buf, (gen-1)*slotSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false, err
	}
	buf = buf[:m]
	own = buf[:min(slotSize, len(buf))]
	for off := slotSize; off < len(buf); off += slotSize {
		s := buf[off:min(off+slotSize, len(buf))]
		if !isEmpty(s) && !aborted(s) {
			later = true
		}
	}
	return own, later, nil
}

// claim wins generation gen by hard-linking a unique file to its claim
// name. Exactly one process wins each name: link(2) is atomic on the
// server, and a retransmitted link whose first reply was lost is
// recognised by the link count of our own file (open(2), O_EXCL notes).
func claim(p paths, gen int64, nonce string) (bool, error) {
	tmp := filepath.Join(p.dir, fmt.Sprintf("%s.lease.tmp-%s", p.base, nonce))
	if err := os.WriteFile(tmp, []byte(nonce+"\n"), 0o600); err != nil {
		return false, err
	}
	defer os.Remove(tmp)
	err := os.Link(tmp, p.claim(gen))
	if err == nil {
		return true, nil
	}
	if fi, serr := os.Stat(tmp); serr == nil {
		if n, known := linkCount(fi); known && n == 2 {
			return true, nil
		}
	}
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	return false, err
}

// garbage removes claim names far below gen and temporary files left by
// claimers that died. It is best effort.
func garbage(p paths, gen int64, now time.Time) {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return
	}
	prefix := p.base + ".lease."
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		rest := strings.TrimPrefix(name, prefix)
		if strings.HasPrefix(rest, "tmp-") {
			if fi, err := e.Info(); err == nil && now.Sub(fi.ModTime()) > time.Hour {
				_ = os.Remove(filepath.Join(p.dir, name))
			}
			continue
		}
		if g, err := strconv.ParseInt(rest, 10, 64); err == nil && g < gen-16 {
			_ = os.Remove(filepath.Join(p.dir, name))
		}
	}
}
