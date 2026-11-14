package teams

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

const (
	readFallbackInterval       = time.Second
	readRecoveryTimeout        = 5 * time.Second
	readRecoveryBackoff        = 5 * time.Second
	readFallbackFailureBackoff = 30 * time.Second
)

// readRoute owns short-lived credential cooldowns, not conversation state.
// Normal reads are concurrent. Degraded reads share one cancellable lane:
// this also makes fallback and recovery probes single-flight without workers.
type readRoute struct {
	mu                                sync.Mutex
	primaryUntil, fullUntil, nextFull time.Time
	cause                             error
	primaryThrottles                  atomic.Uint64
	fullRequests                      atomic.Uint64
	fullSuccesses                     atomic.Uint64
	fullFailures                      atomic.Uint64
	recoveries                        atomic.Uint64
	lane                              chan struct{}
	now                               func() time.Time
	sleep                             func(context.Context, time.Duration) error
	log                               func(string, ...any)
}

func newReadRoute(now func() time.Time, sleep func(context.Context, time.Duration) error, log func(string, ...any)) *readRoute {
	return &readRoute{lane: make(chan struct{}, 1), now: now, sleep: sleep, log: log}
}

func cooldown(d time.Duration) time.Duration {
	// A zero/past Retry-After must not create a busy recovery loop.
	return min(maxRetryAfter, max(time.Second, d))
}

func (r *readRoute) blockFull(d time.Duration) {
	r.mu.Lock()
	r.fullUntil = maxTime(r.fullUntil, r.now().Add(cooldown(d)))
	r.mu.Unlock()
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func switchable(err error) (*channel.Throttled, bool) {
	if err == nil {
		return nil, false
	}
	th, ok := channel.AsThrottled(err)
	var status *statusError
	return th, ok && th.Scope == channel.ScopeUnknown && errors.As(err, &status) && status.Status == http.StatusTooManyRequests
}

func (r *readRoute) blockPrimary(th *channel.Throttled, until time.Time) {
	r.primaryThrottles.Add(1)
	r.mu.Lock()
	first := r.primaryUntil.IsZero()
	r.primaryUntil = maxTime(r.primaryUntil, until)
	r.cause = th
	r.mu.Unlock()
	if first && r.log != nil {
		r.log("teams read route: primary cooling; probing full (HTTP 429)")
	}
}

func (g *graph) readOnce(ctx context.Context, c call) (response, error) {
	r := g.readRoute
	if r == nil || !c.monitor || c.write || c.noAuth || c.method != http.MethodGet {
		return g.once(ctx, c)
	}
	r.mu.Lock()
	normal := r.primaryUntil.IsZero()
	r.mu.Unlock()
	if normal {
		resp, err := g.onceWithTokens(ctx, c, g.readTokens)
		th, ok := switchable(err)
		if !ok {
			return resp, err
		}
		until := r.now().Add(cooldown(th.RetryAfter))
		// If borrowing cannot be proved safe, preserve the original channel
		// throttle and its deadline. Do not create an artificial second lane
		// for opaque, missing, same-app, or differently signed-in credentials.
		proofCtx, cancel := g.readProbeContext(ctx)
		candidate, proofErr := sameIdentitySource(proofCtx, g.readTokens, g.tokens)
		if proofErr == nil {
			_, proofErr = candidate.Token(proofCtx)
		}
		cancel()
		if proofErr != nil {
			if r.log != nil {
				r.log("teams read route: keeping primary throttle; full identity unavailable: %v", proofErr)
			}
			return resp, err
		}
		r.blockPrimary(th, until)
	}
	// Bound lane waits and identity/refresh work as well as HTTP I/O.
	if g.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.timeout)
		defer cancel()
	}
	select {
	case r.lane <- struct{}{}:
		defer func() { <-r.lane }()
	case <-ctx.Done():
		return response{}, &channel.Rejected{Err: ctx.Err()}
	}
	if err := ctx.Err(); err != nil {
		return response{}, &channel.Rejected{Err: err}
	}
	r.mu.Lock()
	until := r.primaryUntil
	r.mu.Unlock()
	if until.IsZero() || !r.now().Before(until) {
		// A recovery probe must not monopolize the borrowed lane for the
		// normal 60s HTTP timeout. Leave time to complete the read on full.
		probeCtx, cancel := g.readProbeContext(ctx)
		resp, err := g.onceWithTokens(probeCtx, c, g.readTokens)
		cancel()
		if err == nil {
			r.mu.Lock()
			// An older concurrent read can still report a newer cooldown.
			if !until.IsZero() && r.primaryUntil.Equal(until) {
				r.primaryUntil = time.Time{}
				r.recoveries.Add(1)
			}
			r.mu.Unlock()
			if !until.IsZero() && r.log != nil {
				r.log("teams read route: primary recovered")
			}
			return resp, nil
		}
		if th, ok := switchable(err); ok {
			r.blockPrimary(th, r.now().Add(cooldown(th.RetryAfter)))
		} else if retryableRead(err) && ctx.Err() == nil && !until.IsZero() {
			// We are already borrowing after a confirmed 429. A transient
			// recovery failure must not discard a working alternate or probe
			// primary again on every queued read. Initial transport failures
			// still never initiate fallback.
			r.mu.Lock()
			r.primaryUntil = maxTime(r.primaryUntil, r.now().Add(readRecoveryBackoff))
			r.mu.Unlock()
		} else {
			// Do not turn transport, auth, 503 or explicit shared throttles
			// into credential switching. Keep the original error contract.
			return response{}, err
		}
	}
	if err := r.unavailable(); err != nil {
		return response{}, err
	}
	// Check actual tokens, including every refreshed full token. The check
	// fails closed and never changes either credential's login/cache.
	tokens, err := sameIdentitySource(ctx, g.readTokens, g.tokens)
	if err == nil {
		c.borrowed = true
		var resp response
		resp, err = g.onceWithTokens(ctx, c, tokens)
		if err == nil {
			r.fullSuccesses.Add(1)
			return resp, nil
		}
	}
	if ctx.Err() != nil {
		return response{}, &channel.Rejected{Err: ctx.Err()}
	}
	if errors.Is(err, errReadBudget) {
		return response{}, err
	}
	r.fullFailures.Add(1)
	delay := readFallbackFailureBackoff
	if th, ok := channel.AsThrottled(err); ok {
		delay = th.RetryAfter
	}
	r.blockFull(delay)
	if r.log != nil {
		r.log("teams read route: full unavailable; deferring to primary recovery: %v", err)
	}
	return response{}, r.deferred()
}

// unavailable is non-nil only when neither lane may currently be used.
// Return the earliest usable credential, not the last server's deadline.
func (r *readRoute) unavailable() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if !now.Before(r.primaryUntil) || !now.Before(r.fullUntil) {
		return nil
	}
	until := r.primaryUntil
	if r.fullUntil.Before(until) {
		until = r.fullUntil
	}
	return &channel.Throttled{Scope: channel.ScopeAccount, RetryAfter: until.Sub(now), Err: r.cause}
}

// Called for each physical borrowed HTTP request, including a 401 retry.
// Writes bypass this lane and this pacing. No mutex spans a wait or I/O.
func (r *readRoute) waitFull(ctx context.Context) error {
	r.mu.Lock()
	wait := r.nextFull.Sub(r.now())
	r.mu.Unlock()
	if wait > 0 {
		if err := r.sleep(ctx, wait); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if now.Before(r.fullUntil) {
		return &channel.Throttled{Scope: channel.ScopeAccount, RetryAfter: r.fullUntil.Sub(now), Err: r.cause}
	}
	r.nextFull = now.Add(readFallbackInterval)
	return nil
}

// Even if a deadline elapsed while the failed request was in flight, return
// an error, never a zero response with nil error. Core may retry promptly.
func (r *readRoute) deferred() error {
	if err := r.unavailable(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return &channel.Throttled{Scope: channel.ScopeAccount, RetryAfter: time.Second, Err: r.cause}
}

// ReadFallbackStats counts routing outcomes without logging message traffic.
// FullRequests includes physical 401 retries; FullSuccesses counts HTTP reads.
type ReadFallbackStats struct {
	PrimaryThrottles, FullRequests, FullSuccesses, FullFailures, Recoveries uint64
}

func (a *Adapter) ReadFallbackStats() ReadFallbackStats {
	r := a.g.readRoute
	if r == nil {
		return ReadFallbackStats{}
	}
	return ReadFallbackStats{r.primaryThrottles.Load(), r.fullRequests.Load(), r.fullSuccesses.Load(), r.fullFailures.Load(), r.recoveries.Load()}
}

// Both initial identity proof and primary recovery are optional work: do not
// let them consume the full request timeout and delay the usable read path.
func (g *graph) readProbeContext(ctx context.Context) (context.Context, context.CancelFunc) {
	limit := readRecoveryTimeout
	if g.timeout > 0 {
		limit = min(limit, max(time.Nanosecond, g.timeout/4))
	}
	return context.WithTimeout(ctx, limit)
}
