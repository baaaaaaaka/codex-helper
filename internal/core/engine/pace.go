package engine

import (
	"math"
	"sync"
	"time"
)

// pacer paces the account's writes and reads (CORE-DESIGN §3): a token
// bucket whose rate adapts to the service's budget (AIMD).
//
//   - It starts at the ceiling (Config.WriteRate / ReadRate).
//   - An account (or unknown) scope throttle lowers the rate by one step (a
//     sixteenth of the ceiling). A second throttle within one quiet period
//     (Timing.PaceIncrease, counted from when requests resumed after the
//     first) halves it, down to the floor (a quarter of the ceiling): a
//     lone 429 is noise at the edge of the budget, repeated ones mean the
//     budget is lower. Requests that were already in flight when the rate
//     was cut do not cut it again: one burst of 429s is one decrease.
//   - The rate at a cut, minus one step, is the learned ceiling: the
//     steady state stays just under the budget instead of touching it
//     again. After a long quiet time at the learned ceiling it probes one
//     step higher, up to the configured ceiling.
//   - The first quiet period after a cut restores about 0.9× the rate
//     before the cut; each further one adds a step, up to the learned
//     ceiling.
//   - After a throttle the bucket is empty until the Retry-After ends and
//     then refills at the (cut) rate one request at a time (burst 1 for a
//     quiet period): the release is gradual, never a burst of everything
//     that waited.
type pacer struct {
	mu          sync.Mutex
	ceil, floor float64
	learned     float64 // learned ceiling (<= ceil)
	step        float64 // additive change
	every       time.Duration
	rate        float64
	before      float64 // the rate before the last cut
	fast        bool    // the next quiet period restores paceRestore × before
	tokens      float64
	last        time.Time // tokens are counted up to last (may be in the future: Retry-After)
	since       time.Time // quiet periods are counted from since (the last change, or the resume after a cut)
	cut         time.Time // the last cut
	resumed     time.Time // the end of the last cut's Retry-After
	refused     time.Time // the last refused take (demand above the rate)
	now         func() time.Time
}

const (
	paceFloorShare = 0.25       // floor = ceiling × 1/4
	paceStepShare  = 1.0 / 16.0 // additive change = ceiling × 1/16
	paceCut        = 0.5        // multiplicative decrease (repeated throttles)
	paceRestore    = 0.9        // first quiet period after a cut: 0.9 × the rate before it
	paceProbe      = 10         // quiet periods at the learned ceiling before probing a step higher
)

func newPacer(ceil float64, every time.Duration, now func() time.Time) *pacer {
	if every <= 0 {
		every = 30 * time.Second
	}
	t := now()
	p := &pacer{ceil: ceil, learned: ceil, floor: ceil * paceFloorShare, step: ceil * paceStepShare, every: every,
		rate: ceil, last: t, since: t, now: now}
	p.tokens = p.burst(t)
	return p
}

// burst is the bucket size: one second of the current rate (at least one),
// and one request for a quiet period after a cut.
func (p *pacer) burst(now time.Time) float64 {
	if !p.cut.IsZero() && now.Sub(p.quietFrom()) < p.every {
		return 1
	}
	return math.Max(1, p.rate)
}

// quietFrom is when the quiet time after the last cut started: when the
// requests resumed.
func (p *pacer) quietFrom() time.Time {
	if p.resumed.After(p.cut) {
		return p.resumed
	}
	return p.cut
}

// advance applies the increases due by now and refills the bucket.
// p.mu must be held.
func (p *pacer) advance(now time.Time) {
	switch {
	case p.rate < p.learned:
		for n := int(now.Sub(p.since) / p.every); n > 0 && p.rate < p.learned; n-- {
			if p.fast {
				p.rate = math.Max(p.rate+p.step, p.before*paceRestore)
				p.fast = false
			} else {
				p.rate += p.step
			}
			p.since = p.since.Add(p.every)
		}
		p.rate = math.Min(p.rate, p.learned)
	case p.learned < p.ceil:
		// At the learned ceiling: probe one step higher after a long
		// quiet time (the budget may have grown).
		if now.Sub(p.since) >= paceProbe*p.every {
			p.learned = math.Min(p.ceil, p.learned+p.step)
			p.rate = p.learned
			p.since = now
		}
	default:
		p.since = now
	}
	if now.After(p.last) {
		p.tokens = math.Min(p.burst(now), p.tokens+now.Sub(p.last).Seconds()*p.rate)
		p.last = now
	}
}

// take takes one token; it returns false (and how long to wait) when none
// is available.
func (p *pacer) take() (bool, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	p.advance(now)
	if p.tokens >= 1 && !now.Before(p.last) {
		p.tokens--
		return true, 0
	}
	p.refused = now
	wait := time.Duration((1 - p.tokens) / p.rate * float64(time.Second))
	if d := p.last.Sub(now); d > 0 {
		wait += d
	}
	return false, wait
}

// borrow takes one token even when the bucket is empty (down to minus one
// burst): for the read that serves every chat at once (the activity scan),
// which must not lose to per-chat reads; the debt delays those instead.
func (p *pacer) borrow() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	p.advance(now)
	p.tokens = math.Max(-p.burst(now), p.tokens-1)
}

// throttled records an account throttle of a request that started at
// started; requests resume at resume (the Retry-After). It returns the new
// rate when this throttle cut it.
func (p *pacer) throttled(started, resume time.Time) (cut bool, rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	p.advance(now)
	if started.After(p.cut) {
		prev := p.rate
		if !p.cut.IsZero() && now.Sub(p.quietFrom()) < p.every {
			p.rate = math.Max(p.floor, p.rate*paceCut)
		} else {
			p.rate = math.Max(p.floor, p.rate-p.step)
		}
		p.learned = math.Max(p.floor, prev-p.step)
		p.before, p.fast = prev, true
		p.cut, p.since = now, now
		cut = true
	}
	p.tokens = 0
	if resume.After(p.last) {
		p.last = resume
	}
	if resume.After(p.resumed) {
		p.resumed = resume
	}
	if resume.After(p.since) {
		p.since = resume
	}
	return cut, p.rate
}

// limited reports whether the requests are paced below the demand: the
// rate is cut below the learned ceiling, or a request waited for a token
// within the last quiet period.
func (p *pacer) limited() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	p.advance(now)
	return p.rate < p.learned || (!p.refused.IsZero() && now.Sub(p.refused) < p.every)
}

// currentRate is the paced rate (requests per second).
func (p *pacer) currentRate() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.advance(p.now())
	return p.rate
}
