package lease

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"
)

// Timing are the lease's intervals. Production values are Scaled(90s).
//
//   - Beat: the owner rewrites its slot this often (its own goroutine).
//   - Valid: the owner acts for at most this long after the start of its
//     last successful renewal (or check) without renewing again.
//   - Poll: a standby reads the lease file this often.
//   - Stale: a standby takes over once the owner's slot has not changed
//     for this long by the standby's monotonic clock.
//   - Quiet: a successor of an owner that did not release waits this long
//     after writing its claim before it copies the store and serves.
//   - BatchDefer: a batch-job host waits this much longer than an
//     interactive standby before taking over from another host.
//
// Safety needs Quiet >= 1.5 * Valid: an owner whose last renewal read the
// file just before a claim was written acts for at most Valid after that
// renewal started (by its clock), and its successor starts Quiet after
// writing the claim (by its own clock), so the old owner has stopped first
// as long as the two clocks run at rates within a factor of 1.5. Clock
// offsets do not matter: no host compares its clock with another's.
// Liveness needs Stale far above Beat + Poll, so that a renewing owner is
// never judged stale (production: a change every 10 s, looked at every
// 20 s, judged after 90 s without one).
type Timing struct {
	Beat, Valid, Poll, Stale, Quiet, BatchDefer time.Duration
}

// ProductionStale is the production Stale bound.
const ProductionStale = 90 * time.Second

// Production is the production timing: beat 10s, valid 20s, poll 20s,
// stale 90s, quiet 30s, batch defer 60s.
func Production() Timing { return Scaled(ProductionStale) }

// Scaled derives every interval from the stale bound.
func Scaled(stale time.Duration) Timing {
	if stale <= 0 {
		stale = ProductionStale
	}
	return Timing{
		Beat:       stale / 9,
		Valid:      stale * 2 / 9,
		Poll:       stale * 2 / 9,
		Stale:      stale,
		Quiet:      stale / 3,
		BatchDefer: stale * 2 / 3,
	}
}

// Config is an Acquire request.
type Config struct {
	// Anchor is the store's base path (<dir>/core.sqlite). The lease files
	// live next to it.
	Anchor string
	Self   Ident
	Timing Timing
	// Wait makes a standby wait for the lease. Without it Acquire returns
	// ErrOwnerLive or ErrDisabled as soon as it sees a live owner or the
	// kill switch.
	Wait bool
	// Immediate claims only what can be claimed at the first look (no
	// owner, a released one, or a dead process of this machine) and
	// returns ErrOwnerLive otherwise: for a short command that writes the
	// store, never for a helper.
	Immediate bool
	// Report is told when the standby's reason for waiting changes.
	Report func(Standby)

	// Test hooks.
	Clock     func() time.Time
	Sleep     func(context.Context, time.Duration) error
	Alive     func(pid int) bool
	StartTime func(pid int) string
}

// Standby says why Acquire is waiting.
type Standby struct {
	Reason Reason
	Owner  Record // the slot being watched, if it decoded
	Gen    int64
	Err    error // ReasonError
}

// Reason is a standby state.
type Reason string

const (
	ReasonDisabled  Reason = "disabled"  // the kill-switch file exists
	ReasonLive      Reason = "live"      // the owner is renewing its lease
	ReasonWatching  Reason = "watching"  // not yet seen renewing; waiting for Stale
	ReasonBatch     Reason = "batch"     // eligible, deferring to interactive hosts
	ReasonError     Reason = "error"     // the lease file could not be read or claimed
	ReasonClaimLost Reason = "claimlost" // another standby won the claim
)

func (c *Config) defaults() {
	if c.Timing == (Timing{}) {
		c.Timing = Production()
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	if c.Sleep == nil {
		c.Sleep = sleepCtx
	}
	if c.Alive == nil {
		c.Alive = pidAlive
	}
	if c.StartTime == nil {
		c.StartTime = startTime
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// dead reports whether r's process is known to be gone: a process of this
// machine whose pid is gone or now names another process, or a process of
// this host from before its last boot.
func (c *Config) dead(r Record) bool {
	if c.Self.rebooted(r) {
		return true
	}
	if !c.Self.sameMachine(r) {
		return false
	}
	if r.PID == c.Self.PID && r.Start == c.Self.Start {
		return false // this very process (cannot hold an unknown claim)
	}
	if !c.Alive(r.PID) {
		return true
	}
	if r.Start != "" {
		if st := c.StartTime(r.PID); st != "" && st != r.Start {
			return true
		}
	}
	return false
}

// observation tracks one slot's bytes by the local monotonic clock.
type observation struct {
	gen      int64
	raw      []byte
	since    time.Time // when these bytes were first seen
	valid    bool
	active   bool      // a change was seen: the owner renews
	eligible time.Time // when the slot first became eligible (batch defer)
}

// Acquire waits for and claims the next generation of the lease. On
// success the returned Holder renews it from its own goroutine; the caller
// follows the Holder's Plan, then calls Activate and finally Release.
func Acquire(ctx context.Context, cfg Config) (*Holder, error) {
	cfg.defaults()
	p := newPaths(cfg.Anchor)
	var (
		obs        observation
		claimedTop int64 // a generation another standby won but has not written yet
		last       Standby
	)
	report := func(s Standby) {
		if cfg.Report != nil && (s.Reason != last.Reason || s.Gen != last.Gen) {
			cfg.Report(s)
		}
		last = s
	}
	poll := cfg.Timing.Poll
	if !cfg.Wait {
		// A foreground run decides quickly: it polls twice per beat until
		// the owner is seen renewing (refuse) or stale (take over).
		poll = max(cfg.Timing.Beat/2, time.Millisecond)
	}
	for first := true; ; first = false {
		if !first {
			if err := cfg.Sleep(ctx, poll); err != nil {
				return nil, err
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := os.Stat(p.disabled()); err == nil {
			if !cfg.Wait {
				return nil, ErrDisabled
			}
			report(Standby{Reason: ReasonDisabled})
			obs = observation{}
			continue
		}
		v, err := readView(p.records)
		if err != nil {
			if !cfg.Wait {
				return nil, err
			}
			report(Standby{Reason: ReasonError, Err: err})
			continue
		}
		now := cfg.Clock()
		g := v.head()
		if claimedTop > g {
			if s := v.raw(claimedTop); aborted(s) {
				claimedTop = 0
			} else {
				g = claimedTop
			}
		}
		raw := v.raw(g)
		if !obs.valid || g != obs.gen || !bytes.Equal(raw, obs.raw) {
			active := obs.active
			if obs.valid {
				active = true
			}
			obs = observation{gen: g, raw: raw, since: now, valid: true, active: active}
		}
		rec, recOK := v.record(g)
		why, ok := "", false
		switch {
		case g == 0:
			why, ok = "cold", true
		case recOK && rec.State == Released:
			why, ok = "released", true
		case recOK && cfg.dead(rec):
			why, ok = "dead", true
		case now.Sub(obs.since) >= cfg.Timing.Stale:
			why, ok = "stale", true
		}
		if cfg.Immediate && (!ok || why == "stale") {
			return nil, fmt.Errorf("%w: %s", ErrOwnerLive, describe(rec, recOK, g))
		}
		liveHere := recOK && !ok && cfg.Self.sameMachine(rec)
		st := Standby{Reason: ReasonWatching, Gen: g, Owner: rec}
		if !ok {
			obs.eligible = time.Time{}
			if obs.active || liveHere {
				st.Reason = ReasonLive
				if !cfg.Wait {
					return nil, fmt.Errorf("%w: %s", ErrOwnerLive, describe(rec, recOK, g))
				}
			}
			report(st)
			continue
		}
		if obs.eligible.IsZero() {
			obs.eligible = now
		}
		// A batch host lets interactive standbys take over from another
		// host first; it never defers to a successor of its own host.
		if cfg.Self.Batch && g != 0 && !(recOK && strings.EqualFold(rec.Host, cfg.Self.Host)) &&
			now.Sub(obs.eligible) < cfg.Timing.BatchDefer {
			if cfg.Immediate {
				return nil, fmt.Errorf("%w: %s", ErrOwnerLive, describe(rec, recOK, g))
			}
			st.Reason = ReasonBatch
			report(st)
			continue
		}
		gen := max(v.top, claimedTop) + 1
		h, err := tryClaim(&cfg, p, gen, obs, why)
		switch {
		case err != nil:
			if !cfg.Wait {
				return nil, err
			}
			report(Standby{Reason: ReasonError, Err: err, Gen: g})
		case h == nil:
			// Another standby won gen, or the watched slot changed.
			report(Standby{Reason: ReasonClaimLost, Gen: gen})
			claimedTop = gen
			obs = observation{}
		default:
			return h, nil
		}
	}
}

func describe(r Record, ok bool, gen int64) string {
	if !ok {
		return fmt.Sprintf("generation %d", gen)
	}
	return fmt.Sprintf("%s pid %d (generation %d)", r.Host, r.PID, gen)
}

func newNonce() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// tryClaim claims generation gen after obs's slot became eligible (why).
// It returns nil, nil when another process won gen or the watched slot
// changed meanwhile (its owner is alive after all).
func tryClaim(cfg *Config, p paths, gen int64, obs observation, why string) (*Holder, error) {
	nonce := newNonce()
	won, err := claim(p, gen, cfg.Self.Host+"-"+nonce)
	if err != nil || !won {
		return nil, err
	}
	rec := Record{
		Gen: gen, State: Claimed, Host: cfg.Self.Host, Boot: cfg.Self.Boot, PIDNS: cfg.Self.PIDNS,
		PID: cfg.Self.PID, Start: cfg.Self.Start, Batch: cfg.Self.Batch, Nonce: nonce,
		Wall: cfg.Clock().UnixMilli(), Why: why,
	}
	if r, ok, err := decode(obs.raw); ok && err == nil {
		rec.From = r.Host
	}
	abort := func() {
		rec.State = Aborted
		if data, err := encode(rec); err == nil {
			_ = writeSlot(p.records, gen, data)
		}
	}
	// The watched slot must still hold the bytes judged eligible: a
	// stale owner that resumed renewing keeps the store.
	if obs.gen != 0 {
		cur, _, err := readOwn(p.records, obs.gen)
		if err != nil {
			abort()
			return nil, err
		}
		if !bytes.Equal(cur, obs.raw) && !(isEmpty(cur) && isEmpty(obs.raw)) {
			abort()
			return nil, nil
		}
	}
	start := cfg.Clock()
	data, err := encode(rec)
	if err != nil {
		return nil, err
	}
	if err := writeSlot(p.records, gen, data); err != nil {
		return nil, err
	}
	// Of two claimers that each write their slot and then read the slots
	// above it, at least one sees the other; the lower one backs off.
	own, later, err := readOwn(p.records, gen)
	if err != nil {
		abort()
		return nil, err
	}
	if later || !bytes.Equal(own, data) {
		abort()
		return nil, nil
	}
	v, err := readView(p.records)
	if err != nil {
		abort()
		return nil, err
	}
	h := newHolder(*cfg, p, rec, data, start)
	h.plan = plan(cfg, p, v, gen, why)
	h.rec.From = h.plan.From.Host
	h.start()
	return h, nil
}
