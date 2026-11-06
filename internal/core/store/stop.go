package store

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Stopping everywhere (owner ruling 2026-11-05): `cxp teams stop` and the
// control-chat command `shutdown` create the kill-switch file DISABLED next
// to the store, recording who stopped the helper, where and when; `cxp
// teams start` removes it. Every helper sharing the store sees the file: the
// owner stops at its next heartbeat and standbys and services stay idle.
// A file made by hand works exactly as before; it simply carries no record.

// Stop sources.
const (
	StopByCLI   = "cli"   // `cxp teams stop`
	StopByTeams = "teams" // `shutdown` in the Teams control chat
	// StopBySuperseded: another copy of this state (a copied home) took
	// over; Host is where the helper now runs (engine takeover.go).
	StopBySuperseded = "superseded"
)

// StopRecord is the content of the kill-switch file.
type StopRecord struct {
	Source string // StopByCLI, StopByTeams, or "" (made by hand)
	Host   string
	At     time.Time
}

// DisabledPath is the kill-switch file of the store in dir.
func DisabledPath(dir string) string { return filepath.Join(dir, DisabledFile) }

// WriteStop creates the kill-switch file in dir with r. An existing file
// is kept as it is (stopping is idempotent; the first record stays):
// created reports whether this call made it.
func WriteStop(dir string, r StopRecord) (created bool, err error) {
	path := DisabledPath(dir)
	if _, err := os.Lstat(path); err == nil {
		return false, nil
	}
	var b strings.Builder
	b.WriteString("The Teams helper is stopped on all machines that share this directory.\n")
	b.WriteString("Run `cxp teams start` (or delete this file) to start it again.\n\n")
	fmt.Fprintf(&b, "source: %s\nhost: %s\ntime: %s\n", r.Source, oneLine(r.Host), r.At.Format(time.RFC3339))
	// Written whole under a temporary name and linked into place, so a
	// reader never sees a partial record and an existing file is never
	// replaced.
	tmp, err := os.CreateTemp(dir, ".DISABLED-*")
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString(b.String()); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	if err := os.Link(tmp.Name(), path); err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		// No hard links here: create it exclusively instead.
		f, cerr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(cerr, os.ErrExist) {
			return false, nil
		}
		if cerr != nil {
			return false, err
		}
		_, werr := f.WriteString(b.String())
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		return true, werr
	}
	return true, nil
}

// ReadStop reads the kill-switch file in dir: ok is false when there is
// none. A file without a record (made by hand, empty) gives a zero record.
func ReadStop(dir string) (r StopRecord, ok bool, err error) {
	f, err := os.Open(DisabledPath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return r, false, nil
	}
	if err != nil {
		// It exists (or cannot be checked): stopped, record unknown.
		if _, serr := os.Lstat(DisabledPath(dir)); serr == nil {
			return r, true, nil
		}
		return r, false, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, found := strings.Cut(sc.Text(), ":")
		if !found {
			continue
		}
		v = strings.TrimSpace(v)
		switch strings.TrimSpace(k) {
		case "source":
			if v == StopByCLI || v == StopByTeams || v == StopBySuperseded {
				r.Source = v
			}
		case "host":
			r.Host = v
		case "time":
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				r.At = t
			}
		}
	}
	return r, true, nil
}

// RemoveStop deletes the kill-switch file in dir; removed is false when
// there was none.
func RemoveStop(dir string) (removed bool, err error) {
	err = os.Remove(DisabledPath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// Describe is the one-line account of the stop, for status and refusals:
// "Stopped on all machines by `cxp teams stop` (host, time). Start again
// with `cxp teams start`." path is the kill-switch file, named when the
// file carries no record.
func (r StopRecord) Describe(path string) string {
	by := r.by()
	if r.Source == StopBySuperseded {
		return "Stopped " + by + ". Start again with `cxp teams start`."
	}
	if by == "" {
		return "Stopped on all machines: the kill-switch file " + path + " exists. Start again with `cxp teams start`."
	}
	return "Stopped on all machines " + by + ". Start again with `cxp teams start`."
}

// Reason is the stop as a waiting helper reports it.
func (r StopRecord) Reason() string {
	by := r.by()
	if r.Source == StopBySuperseded {
		return "stopped " + by + " until `cxp teams start`"
	}
	if by == "" {
		return "the kill-switch file " + DisabledFile + " exists"
	}
	return "stopped on all machines " + by + " until `cxp teams start`"
}

// by is who stopped the helper, where and when ("" without a record).
func (r StopRecord) by() string {
	var by string
	switch r.Source {
	case StopByCLI:
		by = "by `cxp teams stop`"
	case StopByTeams:
		by = "by `shutdown` in the Teams control chat"
	case StopBySuperseded:
		by = "here: a copy of this state took over and the helper now runs on " + r.Host
		if !r.At.IsZero() {
			by += " (" + r.At.Local().Format("2006-01-02 15:04:05 -07:00") + ")"
		}
		return by
	default:
		return ""
	}
	var where []string
	if r.Host != "" {
		where = append(where, r.Host)
	}
	if !r.At.IsZero() {
		where = append(where, r.At.Local().Format("2006-01-02 15:04:05 -07:00"))
	}
	if len(where) > 0 {
		by += " (" + strings.Join(where, ", ") + ")"
	}
	return by
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }
