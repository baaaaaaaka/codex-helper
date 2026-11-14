package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// A lone account throttle lowers the rate by one step (a 429 at the edge of
// the budget is noise); throttles of requests in flight with it do not
// lower it again; the rate before it, minus one step, is the learned
// ceiling, which is probed one step higher after a long quiet time.
func TestPacerLoneThrottleStepsDownAdditively(t *testing.T) {
	clk := &testClock{t: time.Unix(1000, 0)}
	p := newPacer(4, 30*time.Second, clk.now)
	inFlight := clk.now()
	clk.add(100 * time.Millisecond)
	if cut, rate := p.throttled(inFlight, clk.now().Add(time.Second)); !cut || rate != 3.75 {
		t.Fatalf("lone throttle: cut=%v rate=%v, want one step down to 3.75", cut, rate)
	}
	if cut, rate := p.throttled(inFlight, clk.now().Add(time.Second)); cut || rate != 3.75 {
		t.Fatalf("throttle of a request in flight before the cut: cut=%v rate=%v, want no cut", cut, rate)
	}
	// Quiet periods do not go back above the learned ceiling (3.75)...
	clk.add(5 * time.Minute)
	if r := p.currentRate(); r != 3.75 {
		t.Fatalf("rate %v after 5 quiet minutes, want the learned ceiling 3.75", r)
	}
	if p.limited() {
		t.Fatal("limited at the learned ceiling without demand")
	}
	// ...until a long quiet time (10 periods from the resume) probes one
	// step higher, up to the configured ceiling.
	clk.add(time.Second)
	if r := p.currentRate(); r != 4 {
		t.Fatalf("rate %v after 10 quiet periods, want a probe to the ceiling 4", r)
	}
	clk.add(time.Hour)
	if r := p.currentRate(); r != 4 {
		t.Fatalf("rate %v after a long quiet time, want at most the ceiling 4", r)
	}
}

// A second throttle within one quiet period after the first halves the rate
// (down to the floor). The first quiet period after a halving restores
// about 0.9× the rate before it; later ones add a step up to the learned
// ceiling (the rate before the last cut minus one step).
func TestPacerRepeatedThrottleHalvesAndRestoresFast(t *testing.T) {
	clk := &testClock{t: time.Unix(1000, 0)}
	p := newPacer(4, 30*time.Second, clk.now)
	p.throttled(clk.now(), clk.now().Add(time.Second)) // 4 → 3.75
	clk.add(3 * time.Second)
	started := clk.now()
	clk.add(10 * time.Millisecond)
	if cut, rate := p.throttled(started, clk.now().Add(time.Second)); !cut || rate != 1.875 {
		t.Fatalf("second throttle within a quiet period: cut=%v rate=%v, want a halving to 1.875", cut, rate)
	}
	if !p.limited() {
		t.Fatal("not limited with the rate below the learned ceiling")
	}
	clk.add(time.Second) // the Retry-After ends: the quiet time starts
	clk.add(29 * time.Second)
	if r := p.currentRate(); r != 1.875 {
		t.Fatalf("rate %v before a quiet period, want 1.875", r)
	}
	clk.add(time.Second)
	if r := p.currentRate(); math.Abs(r-3.375) > 1e-9 {
		t.Fatalf("rate %v after one quiet period, want 0.9 × 3.75 = 3.375", r)
	}
	clk.add(30 * time.Second)
	if r := p.currentRate(); r != 3.5 {
		t.Fatalf("rate %v after two quiet periods, want the learned ceiling 3.5", r)
	}
	clk.add(2 * time.Minute)
	if r := p.currentRate(); r != 3.5 {
		t.Fatalf("rate %v at the learned ceiling, want 3.5 until a probe", r)
	}

	// A throttle after a full quiet period is a lone one again: one step.
	clk.add(2 * time.Second)
	started = clk.now()
	clk.add(10 * time.Millisecond)
	if _, rate := p.throttled(started, clk.now().Add(time.Second)); rate != 3.25 {
		t.Fatalf("throttle after quiet periods: rate %v, want one step down to 3.25", rate)
	}
	// Repeated throttles halve down to the floor (a quarter of the ceiling).
	for i := 0; i < 3; i++ {
		clk.add(2 * time.Second)
		started := clk.now()
		clk.add(10 * time.Millisecond)
		p.throttled(started, clk.now().Add(time.Second))
	}
	if r := p.currentRate(); r != 1 {
		t.Fatalf("rate %v after repeated throttles, want the floor 1", r)
	}
}

// After a cut the bucket holds one request for a quiet period: the requests
// that waited for the Retry-After are released one at a time.
func TestPacerBurstIsOneAfterACut(t *testing.T) {
	clk := &testClock{t: time.Unix(1000, 0)}
	p := newPacer(4, 30*time.Second, clk.now)
	for i := 0; i < 4; i++ {
		if ok, _ := p.take(); !ok {
			t.Fatalf("take %d refused with a full bucket", i)
		}
	}
	resume := clk.now().Add(time.Second)
	p.throttled(clk.now(), resume)
	clk.add(10 * time.Second)
	granted := 0
	for i := 0; i < 4; i++ {
		if ok, _ := p.take(); ok {
			granted++
		}
	}
	if granted != 1 {
		t.Fatalf("%d requests granted at once 9 s after the Retry-After, want 1", granted)
	}
	clk.add(time.Minute) // a quiet period later the burst is one second of the rate again
	granted = 0
	for i := 0; i < 4; i++ {
		if ok, _ := p.take(); ok {
			granted++
		}
	}
	if granted != 3 {
		t.Fatalf("%d requests granted at once a quiet period later, want 3 (one second at 3.75/s)", granted)
	}
}

// The recovery contract applies to token grants, including after a scheduler
// pause, and ends at the configured quiet-period boundary. No wall-clock
// sleep or worker-start timestamp is needed to exercise either side.
func TestPacerRecoveryWindowWithDelayedScheduler(t *testing.T) {
	for _, quiet := range []time.Duration{600 * time.Millisecond, 30 * time.Second} {
		t.Run(quiet.String(), func(t *testing.T) {
			clk := &testClock{t: time.Unix(1000, 0)}
			p := newPacer(4, quiet, clk.now)
			resume := clk.now().Add(time.Second)
			p.throttled(clk.now(), resume)
			clk.add(time.Second - time.Nanosecond)
			if ok, _ := p.take(); ok {
				t.Fatal("request granted before Retry-After")
			}
			clk.add(quiet) // delayed worker wakes just before the recovery window ends
			if ok, _ := p.take(); !ok {
				t.Fatal("no request granted after the scheduler pause")
			}
			if ok, _ := p.take(); ok {
				t.Fatal("scheduler pause accumulated a burst during recovery")
			}
			clk.add(quiet + time.Second) // recovery ended; a normal burst may accrue
			granted := 0
			for i := 0; i < 4; i++ {
				if ok, _ := p.take(); ok {
					granted++
				}
			}
			if granted != 3 {
				t.Fatalf("normal burst after recovery: %d grants, want 3 at 3.75/s", granted)
			}
		})
	}
}

// Keep coverage of the connection between the receiver and the pacer. A
// correct pacer alone would not catch a receiver that stopped consulting it.
func TestReadDispatchHonorsRecoveryBudget(t *testing.T) {
	clk := &testClock{t: time.Unix(1000, 0)}
	e := priorityEngine()
	e.now = clk.now
	e.readTokens = newPacer(4, 30*time.Second, clk.now)
	for id := int64(1); id <= 8; id++ {
		e.addConv(id, &convState{nextRead: clk.now()})
	}
	resume := clk.now().Add(time.Second)
	e.readTokens.throttled(clk.now(), resume)
	e.scanFirst.Store(true)
	clk.add(2 * time.Second) // a delayed receiver now has one recovered token
	dispatch := func() []int64 {
		jobs := make(chan int64, 16)
		if _, ok := e.dispatchReads(context.Background(), jobs); !ok {
			t.Fatal("read dispatch canceled")
		}
		close(jobs)
		var ids []int64
		for id := range jobs {
			ids = append(ids, id)
		}
		return ids
	}
	if got := dispatch(); len(got) != 0 {
		t.Fatalf("chat reads preceded the recovery scan: %v", got)
	}
	// Model completion of the scan: it consumes the reserved token and
	// releases the receiver. There is no chat-read token left yet.
	e.readTokens.borrow()
	e.scanFirst.Store(false)
	if got := dispatch(); len(got) != 0 {
		t.Fatalf("reads dispatched without recovery tokens: %v", got)
	}
	clk.add(300 * time.Millisecond)
	if got := dispatch(); len(got) != 1 {
		t.Fatalf("one recovered token dispatched %v, want one chat", got)
	}
	if got := dispatch(); len(got) != 0 {
		t.Fatalf("a recovery token was reused: %v", got)
	}
}

// After a Retry-After the writes resume one at a time at the paced rate,
// not as a burst of everything that waited.
func TestPacerReleasesGraduallyAfterRetryAfter(t *testing.T) {
	clk := &testClock{t: time.Unix(1000, 0)}
	p := newPacer(4, 30*time.Second, clk.now)
	started := clk.now()
	resume := clk.now().Add(time.Second)
	p.throttled(started, resume)
	clk.add(500 * time.Millisecond)
	if ok, wait := p.take(); ok || wait < 500*time.Millisecond {
		t.Fatalf("take during Retry-After: ok=%v wait=%s, want a refusal until it ends", ok, wait)
	}
	clk.add(500 * time.Millisecond) // the gate opens; the bucket waited too
	var at []time.Duration
	for step := time.Duration(0); step <= 2*time.Second; step += 10 * time.Millisecond {
		if ok, _ := p.take(); ok {
			at = append(at, clk.now().Sub(resume))
		}
		clk.add(10 * time.Millisecond)
	}
	// 2 s at the stepped-down rate (3.75/s): about 7 writes, never two
	// within one interval (267 ms).
	if len(at) < 6 || len(at) > 8 {
		t.Fatalf("%d writes in the 2 s after Retry-After (%v), want about 7", len(at), at)
	}
	if at[0] < 250*time.Millisecond {
		t.Fatalf("first write %s after Retry-After, want one paced interval", at[0])
	}
	for i := 1; i < len(at); i++ {
		if at[i]-at[i-1] < 250*time.Millisecond {
			t.Fatalf("writes %s and %s after Retry-After: a burst", at[i-1], at[i])
		}
	}
}

// A refused take (demand above the rate) marks the writes limited for one
// quiet period.
func TestPacerLimitedWhileDemandExceedsRate(t *testing.T) {
	clk := &testClock{t: time.Unix(1000, 0)}
	p := newPacer(2, 30*time.Second, clk.now)
	for i := 0; i < 2; i++ {
		if ok, _ := p.take(); !ok {
			t.Fatalf("take %d refused with a full bucket", i)
		}
	}
	if p.limited() {
		t.Fatal("limited before any write waited")
	}
	if ok, _ := p.take(); ok {
		t.Fatal("third take within the burst granted")
	}
	if !p.limited() {
		t.Fatal("not limited after a write waited for a token")
	}
	clk.add(31 * time.Second)
	if p.limited() {
		t.Fatal("still limited a quiet period after the last wait")
	}
}

// accountBudget is a service write budget: a token bucket of rate/s that
// answers 429 with a Retry-After rounded up to a grain, like Graph's
// whole-second hint.
type accountBudget struct {
	mu                sync.Mutex
	rate, tokens      float64
	last              time.Time
	grain             time.Duration
	served, throttled int
	first, lastServed time.Time
}

func (b *accountBudget) fault(string, channel.Part) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if b.last.IsZero() {
		b.last, b.tokens = now, b.rate
	}
	b.tokens = math.Min(b.rate, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if b.tokens < 1 {
		b.throttled++
		wait := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
		ra := (wait + b.grain - 1) / b.grain * b.grain
		return false, &channel.Throttled{RetryAfter: ra, Scope: channel.ScopeAccount, Err: errors.New("429 account write budget")}
	}
	b.tokens--
	b.served++
	if b.first.IsZero() {
		b.first = now
	}
	b.lastServed = now
	return false, nil
}

// backlogRig is a rig whose store holds work chats with queued outbox rows
// (kind) before the engine starts.
func backlogRig(t *testing.T, tweak func(*Config), chats, rows int, kindOf func(chat int) string) (*rig, []string) {
	t.Helper()
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"), log: &strings.Builder{}, logMu: &sync.Mutex{}}
	r.tweak = tweak
	t.Cleanup(r.stop)
	ctx := context.Background()
	var ids []string
	for i := 0; i < chats; i++ {
		ci, _ := r.ch.CreateChat(ctx, "w", fmt.Sprintf("k%02d", i))
		ids = append(ids, ci.ID)
	}
	st, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Import(func(im *store.Importer) error {
		for i, chat := range ids {
			id, err := im.Conversation(store.Conversation{Label: fmt.Sprintf("s%03d", i+1), Kind: store.KindWork, Channel: "teams", ExtChatID: chat, State: store.ConvActive})
			if err != nil {
				return err
			}
			if err := im.Stream(store.Stream{Key: store.ChatStreamKey("teams", chat), ConvID: id, Cursor: "0", Seeded: true}); err != nil {
				return err
			}
			kind := kindOf(i)
			for k := 0; k < rows; k++ {
				key := fmt.Sprintf("bl:%s:%03d", chat, k)
				if _, _, err := im.Outbox(store.Outbox{ConvID: id, DeliveryKey: key, Seq: int64(k + 1), Kind: kind, Body: kind + " backlog " + key, State: store.OutQueued, CreatedAt: im.Now()}, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	r.start()
	return r, ids
}

// skipWallClockPacingUnderRace skips a test that checks what the pacer lets
// through at a real write rate (goodput, which class goes first, how much
// is coalesced). Under -race on a small CI runner the engine falls behind
// the wall clock and those orders and counts are not meaningful; the
// fake-clock pacer tests cover the same logic deterministically, and
// upper bounds on writes (including already admitted workers) still run.
func skipWallClockPacingUnderRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Skip("wall-clock pacing outcome; not meaningful under the race detector")
	}
}

// Benchmark finding (throttled variants): against an account write budget
// the sender burst after every Retry-After, 42% of the writes were 429'd
// and the goodput fell to about half the budget. With adaptive pacing the
// writes settle just under the budget: few 429s, most of the budget used.
// (Scaled: a 40/s budget with a 100 ms Retry-After grain stands for 4/s
// with Graph's 1 s; the ceiling is twice the budget.)
func TestWritesPacedUnderAccountBudget(t *testing.T) {
	skipWallClockPacingUnderRace(t)
	if runtime.GOOS == "windows" {
		// The scaled 25 ms write interval is within the scheduling noise
		// of the 2-vCPU Windows runners (goodput 13/s of 40/s once, with
		// 5% throttled); production intervals are ten times longer and
		// the pacer is the same code on every OS, covered on Linux/macOS.
		t.Skip("compressed wall-clock goodput; not meaningful on Windows CI runners")
	}
	budget := &accountBudget{rate: 40, grain: 100 * time.Millisecond}
	const chats, rows = 16, 15
	r, ids := backlogRig(t, func(c *Config) {
		c.WriteRate, c.Timing.ChatRate = 80, time.Millisecond
		c.Channel.(*channeltest.Fake).SendFault = budget.fault
	}, chats, rows, func(int) string { return store.OutNotice })
	delivered := func() int {
		n := 0
		for _, c := range ids {
			n += len(r.own(c, "backlog"))
		}
		return n
	}
	deadline := time.Now().Add(30 * time.Second)
	for delivered() < chats*rows {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d backlog messages delivered in 30 s", delivered(), chats*rows)
		}
		time.Sleep(20 * time.Millisecond)
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	attempts := budget.served + budget.throttled
	share := float64(budget.throttled) / float64(attempts)
	goodput := float64(budget.served-1) / budget.lastServed.Sub(budget.first).Seconds()
	t.Logf("served %d, throttled %d (%.0f%%), goodput %.1f/s of a 40/s budget", budget.served, budget.throttled, 100*share, goodput)
	if share > 0.10 {
		t.Errorf("%.0f%% of the writes were throttled, want at most 10%%", 100*share)
	}
	if goodput < 0.6*budget.rate {
		t.Errorf("goodput %.1f/s, want at least 60%% of the %.0f/s budget", goodput, budget.rate)
	}
}

// When the write budget is short, ACKs, finals and notices go before
// progress across conversations (round robin among each class).
func TestRepliesBeforeProgressWhenWritesArePaced(t *testing.T) {
	skipWallClockPacingUnderRace(t)
	var (
		mu    sync.Mutex
		order []string
	)
	const chats = 12
	r, _ := backlogRig(t, func(c *Config) {
		c.WriteRate = 4
		// One sender makes the observed channel order the admission order.
		// With several workers a later job can reach Send before an earlier
		// one, even though the sender admitted the higher-priority job first.
		c.Senders = 1
		c.Channel.(*channeltest.Fake).SendFault = func(_ string, p channel.Part) (bool, error) {
			mu.Lock()
			order = append(order, p.Body)
			mu.Unlock()
			return false, nil
		}
	}, chats, 1, func(i int) string {
		if i%2 == 0 {
			return store.OutProgress
		}
		return store.OutFinal
	})
	r.wait("every final", func() bool {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, b := range order {
			if strings.HasPrefix(b, store.OutFinal+" backlog") {
				n++
			}
		}
		return n == chats/2
	})
	mu.Lock()
	defer mu.Unlock()
	progress := 0
	for _, b := range order {
		switch {
		case strings.HasPrefix(b, store.OutProgress+" backlog"):
			progress++
		case strings.HasPrefix(b, store.OutFinal+" backlog"):
			if progress > 0 {
				t.Fatalf("%d progress messages went before a final under a short write budget; order:\n%s", progress, strings.Join(order, "\n"))
			}
		}
	}
}

// A foreground writer waiting for the account budget takes the next token
// ahead of the background sender. Hold the writer after it registers, then
// make a token available; this checks the admission decision independently
// of the order in which concurrent channel workers happen to start.
func TestWaitingWriterGetsNextWriteToken(t *testing.T) {
	clk := &testClock{t: time.Unix(1000, 0)}
	e := priorityEngine()
	e.writeTokens = newPacer(1, time.Minute, clk.now)
	if ok, _ := e.writeTokens.take(); !ok {
		t.Fatal("initial token unavailable")
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var first, releaseOnce sync.Once
	e.now = func() time.Time {
		first.Do(func() {
			close(entered)
			<-release
		})
		return clk.now()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.waitWrite(ctx) }()
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		if done != nil {
			<-done
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("foreground writer did not enter")
	}
	clk.add(time.Second)
	if ok, _ := e.takeWrite(); ok {
		t.Fatal("background sender took a token while a foreground writer was waiting")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		done = nil
		if err != nil {
			t.Fatalf("foreground writer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("foreground writer did not receive the token")
	}
	if ok, _ := e.takeWrite(); ok {
		t.Fatal("foreground writer did not consume the token")
	}
}

// While the account's writes are paced below the demand (no 429 needed),
// a turn preserves all progress while combining adjacent records; its ACK
// and final still go ahead of the progress backlog of other chats.
func TestProgressCoalescedWhileWritesArePaced(t *testing.T) {
	skipWallClockPacingUnderRace(t)
	// Each backlog chat sends at most once per ChatRate (1 s): 12 chats
	// ask for 12 writes/s against 4/s, so the writes are clearly paced.
	// (4 chats asked for exactly the rate, and a slow runner fell just
	// under it: nothing was refused and nothing coalesced.)
	r, _ := backlogRig(t, func(c *Config) { c.WriteRate = 4 }, 12, 200, func(int) string { return store.OutProgress })
	r.ag.mu.Lock()
	r.ag.notes = []string{"step A", "step B", "step C"}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "paced progress turn")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to paced progress turn")) == 1 })
	n := 0
	for _, m := range r.ch.Messages(work) {
		if m.Marker != "" && strings.Contains(m.Text, "step ") {
			n++
		}
	}
	if n < 1 || n > 3 {
		t.Fatalf("unexpected progress message count %d", n)
	}
	for _, text := range []string{"step A", "step B", "step C"} {
		if len(r.own(work, text)) != 1 {
			t.Fatalf("missing or duplicate %q", text)
		}
	}
}

// fakeBudget is the benchmark's throttle model: a token bucket per account
// and request class that answers 429 with a fixed Retry-After when empty.
type fakeBudget struct {
	rate, burst, tokens float64
	last                time.Time
}

func (b *fakeBudget) allow(now time.Time) bool {
	b.tokens = math.Min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Benchmark S6 (storm gate): under a budget far below the pacer's floor
// (0.2/s, burst 3, Retry-After 30 s) every Retry-After window ended in a
// 429: the pacer released four requests at its floor rate into a burst of
// three. A throttle that follows an earlier one now measures the budget,
// so once one has been measured the window after each reopening is free
// of 429s, and more requests get through than before.
func TestPacerSettlesUnderABudgetFarBelowTheFloor(t *testing.T) {
	const ra = 30 * time.Second
	for _, scan := range []bool{false, true} { // reads: the activity scan borrows at each reopening
		clk := &testClock{t: time.Unix(1000, 0)}
		p := newPacer(4, 30*time.Second, clk.now)
		b := &fakeBudget{rate: 0.2, burst: 3, tokens: 3, last: clk.now()}
		start := clk.now()
		var throttles []time.Time
		served := 0
		reopen := time.Time{}
		for clk.now().Sub(start) < 10*time.Minute {
			now := clk.now()
			if scan && !reopen.IsZero() && !now.Before(reopen) {
				p.borrow()
				reopen = time.Time{}
			} else if ok, wait := p.take(); !ok {
				clk.add(max(wait, 10*time.Millisecond))
				continue
			}
			if b.allow(now) {
				served++
			} else {
				throttles = append(throttles, now)
				p.throttled(now, now.Add(ra))
				reopen = now.Add(ra)
			}
			clk.add(50 * time.Millisecond) // the request's duration
		}
		t.Logf("scan=%v: %d served, %d throttled at %v", scan, served, len(throttles), throttles)
		// The first two throttles teach the budget (the storm's onset and
		// the first reopening); after that the window following each
		// reopening has no 429.
		for i := 2; i < len(throttles); i++ {
			if gap := throttles[i].Sub(throttles[i-1]); gap < 2*ra {
				t.Fatalf("scan=%v: throttle %d came %s after the previous one: inside the window after the reopening", scan, i, gap)
			}
		}
		if len(throttles) > 8 {
			t.Fatalf("scan=%v: %d throttles in 10 min, want a few (the old pacer: one per window, 18)", scan, len(throttles))
		}
		if served < 70 {
			t.Fatalf("scan=%v: %d requests served in 10 min, want more than the old pacer's 54", scan, served)
		}
	}
}
