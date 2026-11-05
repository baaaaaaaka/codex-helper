package lease

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Holder holds one generation of the lease. Its goroutine renews the slot
// every Timing.Beat, apart from the caller's own work, so a long store
// transaction or a slow Graph call never delays a renewal.
type Holder struct {
	cfg  Config
	p    paths
	plan Plan

	io sync.Mutex // serialises slot I/O

	mu      sync.Mutex
	rec     Record
	raw     []byte    // the bytes last written to our slot
	renewed time.Time // start of the last successful renewal or check
	err     error     // ErrFenced or ErrDisabled once lost
	done    bool      // released

	lost     chan struct{}
	lostOnce sync.Once
	stop     chan struct{}
	stopOnce sync.Once
	stopped  chan struct{}
}

func newHolder(cfg Config, p paths, rec Record, raw []byte, start time.Time) *Holder {
	return &Holder{
		cfg: cfg, p: p, rec: rec, raw: raw, renewed: start,
		lost: make(chan struct{}), stop: make(chan struct{}), stopped: make(chan struct{}),
	}
}

func (h *Holder) start() { go h.loop() }

// halt stops the renewal goroutine.
func (h *Holder) halt() {
	h.stopOnce.Do(func() { close(h.stop) })
	<-h.stopped
}

func (h *Holder) loop() {
	defer close(h.stopped)
	t := time.NewTicker(h.cfg.Timing.Beat)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-h.lost:
			return
		case <-t.C:
			_ = h.renew(true)
		}
	}
}

// Gen is the generation held.
func (h *Holder) Gen() int64 { return h.plan.Gen }

// Plan is how this generation gets its store.
func (h *Holder) Plan() Plan { return h.plan }

// Lost is closed when the lease is lost: a later generation claimed it
// (Err is ErrFenced) or the kill-switch file appeared (ErrDisabled).
func (h *Holder) Lost() <-chan struct{} { return h.lost }

// Err is why the lease was lost (nil while held).
func (h *Holder) Err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *Holder) lose(err error) error {
	h.mu.Lock()
	if h.err == nil {
		h.err = err
	}
	err = h.err
	h.mu.Unlock()
	h.lostOnce.Do(func() { close(h.lost) })
	return err
}

// renew reads our slot and those above it and, with write, rewrites ours
// with the beat counter bumped. A later generation or the kill switch
// loses the lease for good; an I/O error leaves the lease to run out.
func (h *Holder) renew(write bool) error {
	h.io.Lock()
	defer h.io.Unlock()
	if err := h.Err(); err != nil {
		return err
	}
	start := h.cfg.Clock()
	own, later, err := readOwn(h.p.records, h.plan.Gen)
	if err != nil {
		return fmt.Errorf("lease: read: %w", err)
	}
	h.mu.Lock()
	raw := h.raw
	h.mu.Unlock()
	if later {
		return h.lose(ErrFenced)
	}
	if !bytes.Equal(own, raw) {
		return h.lose(fmt.Errorf("%w (slot %d was rewritten)", ErrFenced, h.plan.Gen))
	}
	if _, err := os.Stat(h.p.disabled()); err == nil {
		return h.lose(ErrDisabled)
	}
	if write {
		h.mu.Lock()
		rec := h.rec
		h.mu.Unlock()
		rec.Beat++
		rec.Wall = h.cfg.Clock().UnixMilli()
		data, err := encode(rec)
		if err != nil {
			return err
		}
		if err := writeSlot(h.p.records, h.plan.Gen, data); err != nil {
			return fmt.Errorf("lease: write: %w", err)
		}
		h.mu.Lock()
		h.rec, h.raw = rec, data
		h.mu.Unlock()
	}
	h.mu.Lock()
	h.renewed = start
	h.mu.Unlock()
	return nil
}

// Check returns nil when the lease is held: within Timing.Valid of the
// start of the last renewal it costs one clock read; past that it renews
// first. Call it before every store write transaction.
func (h *Holder) Check() error {
	h.mu.Lock()
	err, age := h.err, leaseAge(h.cfg.Clock(), h.renewed)
	h.mu.Unlock()
	if err != nil {
		return err
	}
	if age < h.cfg.Timing.Valid {
		return nil
	}
	return h.renew(true)
}

// Verify reads the lease file now and returns nil when no later
// generation has claimed it. Call it right before an effect outside the
// store (a Graph write).
func (h *Holder) Verify() error { return h.renew(false) }

// leaseAge is the time since start by the monotonic clock or the wall
// clock, whichever is longer: the monotonic clock may stop while the
// machine is suspended, the wall clock may be set back.
func leaseAge(now, start time.Time) time.Duration {
	d := now.Sub(start)
	if w := now.Round(0).Sub(start.Round(0)); w > d {
		d = w
	}
	return d
}

// WaitQuiet waits out Timing.Quiet when the plan asks for it, then
// verifies the lease is still held.
func (h *Holder) WaitQuiet(ctx context.Context) error {
	if h.plan.Quiet {
		start := h.cfg.Clock()
		for {
			left := h.cfg.Timing.Quiet - h.cfg.Clock().Sub(start)
			if left <= 0 {
				break
			}
			step := min(left, h.cfg.Timing.Beat)
			if err := h.cfg.Sleep(ctx, step); err != nil {
				return err
			}
			if err := h.Err(); err != nil {
				return err
			}
		}
	}
	return h.Verify()
}

// Activate records that this generation serves from its plan's DB. The
// store must be ready (copied or migrated) and not yet used.
func (h *Holder) Activate() error {
	if err := h.Verify(); err != nil {
		return err
	}
	h.io.Lock()
	h.mu.Lock()
	rec := h.rec
	h.mu.Unlock()
	rec.State = Active
	rec.DB = filepath.Base(h.plan.DB)
	rec.Wall = h.cfg.Clock().UnixMilli()
	data, err := encode(rec)
	if err == nil {
		err = writeSlot(h.p.records, h.plan.Gen, data)
	}
	if err == nil {
		h.mu.Lock()
		h.rec, h.raw = rec, data
		h.mu.Unlock()
	}
	h.io.Unlock()
	if err != nil {
		return err
	}
	if err := h.Verify(); err != nil {
		return err
	}
	h.cleanup()
	return nil
}

// cleanup removes claim names and temporary files no one needs, and the
// stores of generations before the previous one. Best effort.
func (h *Holder) cleanup() {
	now := h.cfg.Clock()
	garbage(h.p, h.plan.Gen, now)
	entries, err := os.ReadDir(h.p.dir)
	if err != nil {
		return
	}
	keep := map[string]bool{filepath.Base(h.plan.DB): true, filepath.Base(h.plan.Src): true}
	for _, e := range entries {
		name := e.Name()
		db := strings.TrimSuffix(strings.TrimSuffix(name, "-wal"), "-shm")
		tmp := strings.HasSuffix(db, ".tmp")
		db = strings.TrimSuffix(db, ".tmp")
		g, ok := genOf(h.p, db)
		if !ok || g >= h.plan.Gen || keep[db] && !tmp {
			continue
		}
		if fi, err := e.Info(); err == nil && tmp && now.Sub(fi.ModTime()) < time.Hour && g > h.plan.FromGen {
			continue // a claimer between From and us may still be copying
		}
		_ = os.Remove(filepath.Join(h.p.dir, name))
	}
}

// Release ends the lease after the store was closed. A lost lease is left
// alone: a later generation owns the store.
func (h *Holder) Release() error {
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		return nil
	}
	h.done = true
	h.mu.Unlock()
	h.halt()
	h.io.Lock()
	defer h.io.Unlock()
	if h.Err() != nil && !errors.Is(h.Err(), ErrDisabled) {
		return nil
	}
	own, later, err := readOwn(h.p.records, h.plan.Gen)
	h.mu.Lock()
	rec, raw := h.rec, h.raw
	h.mu.Unlock()
	if err != nil {
		return err
	}
	if later || !bytes.Equal(own, raw) {
		return nil
	}
	if rec.State == Active {
		rec.State = Released
	} else {
		rec.State = Aborted
	}
	rec.Wall = h.cfg.Clock().UnixMilli()
	data, err := encode(rec)
	if err != nil {
		return err
	}
	err = writeSlot(h.p.records, h.plan.Gen, data)
	h.lose(ErrReleased)
	return err
}

// Record is this generation's last written record.
func (h *Holder) Record() Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rec
}
