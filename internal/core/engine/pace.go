package engine

import (
	"math"
	"sync"
	"time"
)

// pacer paces the account's writes (CORE-DESIGN §3): a token bucket whose
// rate adapts to the service's write budget (AIMD).
//
//   - It starts at the ceiling (Config.WriteRate).
//   - An account (or unknown) scope throttle halves the rate, down to the
//     floor (a quarter of the ceiling: 1/s at the default 4/s). Sends that
//     were already in flight when the rate was cut do not cut it again: one
//     burst of 429s is one decrease.
//   - Each quiet Timing.PaceIncrease without a throttle adds a sixteenth of
//     the ceiling (0.25/s at 4/s), up to the ceiling.
//   - After a throttle the bucket is empty until the Retry-After ends and
//     then refills at the (cut) rate: the release is gradual, never a burst
//     of everything that waited.
//
// The steady state sits just under the service's budget with few 429s,
// instead of bursting into it after every Retry-After.
type pacer struct {
	mu          sync.Mutex
	ceil, floor float64
	step        float64       // additive increase
	every       time.Duration // quiet period per increase
	rate        float64
	tokens      float64
	last        time.Time // tokens are counted up to last (may be in the future: Retry-After)
	since       time.Time // the last change of rate (cut or increase)
	cut         time.Time // the last cut
	refused     time.Time // the last refused take (demand above the rate)
	now         func() time.Time
}

const (
	paceFloorShare = 0.25       // floor = ceiling × 1/4
	paceStepShare  = 1.0 / 16.0 // additive increase = ceiling × 1/16
	paceCut        = 0.5        // multiplicative decrease
)

func newPacer(ceil float64, every time.Duration, now func() time.Time) *pacer {
	if every <= 0 {
		every = 30 * time.Second
	}
	t := now()
	p := &pacer{ceil: ceil, floor: ceil * paceFloorShare, step: ceil * paceStepShare, every: every,
		rate: ceil, last: t, since: t, now: now}
	p.tokens = p.burst()
	return p
}

// burst is the bucket size: one second of the current rate, at least one.
func (p *pacer) burst() float64 { return math.Max(1, p.rate) }

// advance applies the additive increases due by now and refills the bucket.
// p.mu must be held.
func (p *pacer) advance(now time.Time) {
	if p.rate < p.ceil {
		if n := int(now.Sub(p.since) / p.every); n > 0 {
			p.rate = math.Min(p.ceil, p.rate+float64(n)*p.step)
			p.since = p.since.Add(time.Duration(n) * p.every)
		}
	} else {
		p.since = now
	}
	if now.After(p.last) {
		p.tokens = math.Min(p.burst(), p.tokens+now.Sub(p.last).Seconds()*p.rate)
		p.last = now
	}
}

// take takes one write token; it returns false (and how long to wait) when
// none is available.
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

// throttled records an account write throttle of a write that started at
// started; writes resume at resume (the Retry-After). It returns the new
// rate when this throttle cut it.
func (p *pacer) throttled(started, resume time.Time) (cut bool, rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	p.advance(now)
	if started.After(p.cut) {
		p.rate = math.Max(p.floor, p.rate*paceCut)
		p.cut, p.since = now, now
		cut = true
	}
	p.tokens = 0
	if resume.After(p.last) {
		p.last = resume
	}
	return cut, p.rate
}

// limited reports whether the account's writes are paced below the demand:
// the rate is cut below the ceiling, or a write waited for a token within
// the last quiet period.
func (p *pacer) limited() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	p.advance(now)
	return p.rate < p.ceil || (!p.refused.IsZero() && now.Sub(p.refused) < p.every)
}

// currentRate is the paced rate (writes per second).
func (p *pacer) currentRate() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.advance(p.now())
	return p.rate
}
