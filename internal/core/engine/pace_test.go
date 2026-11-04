package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
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

// One burst of account throttles halves the write rate once; later
// throttles cut again down to the floor; quiet periods raise it additively
// back to the ceiling.
func TestPacerCutsOncePerBurstAndRecoversAdditively(t *testing.T) {
	clk := &testClock{t: time.Unix(1000, 0)}
	p := newPacer(4, 30*time.Second, clk.now)
	inFlight := clk.now()
	clk.add(100 * time.Millisecond)
	if cut, rate := p.throttled(inFlight, clk.now().Add(time.Second)); !cut || rate != 2 {
		t.Fatalf("first throttle: cut=%v rate=%v, want a cut to 2", cut, rate)
	}
	// Sends that were in flight with the first one: no further cut.
	if cut, rate := p.throttled(inFlight, clk.now().Add(time.Second)); cut || rate != 2 {
		t.Fatalf("throttle of a send in flight before the cut: cut=%v rate=%v, want no cut", cut, rate)
	}
	for _, want := range []float64{1, 1} {
		clk.add(2 * time.Second)
		started := clk.now()
		clk.add(10 * time.Millisecond)
		if _, rate := p.throttled(started, clk.now().Add(time.Second)); rate != want {
			t.Fatalf("rate %v after a later throttle, want %v (floor 1)", rate, want)
		}
	}
	if !p.limited() {
		t.Fatal("not limited with the rate below the ceiling")
	}
	clk.add(29 * time.Second)
	if r := p.currentRate(); r != 1 {
		t.Fatalf("rate %v before a quiet period, want 1", r)
	}
	clk.add(time.Second)
	if r := p.currentRate(); r != 1.25 {
		t.Fatalf("rate %v after one quiet period, want 1.25", r)
	}
	clk.add(10 * time.Minute)
	if r := p.currentRate(); r != 4 {
		t.Fatalf("rate %v after a long quiet time, want the ceiling 4", r)
	}
	if p.limited() {
		t.Fatal("limited at the ceiling without demand")
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
	// 2 s at the cut rate (2/s): about 4 writes, never two within 400 ms.
	if len(at) < 3 || len(at) > 5 {
		t.Fatalf("%d writes in the 2 s after Retry-After (%v), want about 4", len(at), at)
	}
	if at[0] < 400*time.Millisecond {
		t.Fatalf("first write %s after Retry-After, want one paced interval", at[0])
	}
	for i := 1; i < len(at); i++ {
		if at[i]-at[i-1] < 400*time.Millisecond {
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

// Benchmark finding (throttled variants): against an account write budget
// the sender burst after every Retry-After, 42% of the writes were 429'd
// and the goodput fell to about half the budget. With adaptive pacing the
// writes settle just under the budget: few 429s, most of the budget used.
// (Scaled: a 40/s budget with a 100 ms Retry-After grain stands for 4/s
// with Graph's 1 s; the ceiling is twice the budget.)
func TestWritesPacedUnderAccountBudget(t *testing.T) {
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
	var (
		mu    sync.Mutex
		order []string
	)
	const chats = 12
	r, _ := backlogRig(t, func(c *Config) {
		c.WriteRate = 4
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

// While the account's writes are paced below the demand (no 429 needed),
// a turn posts at most one progress message; its ACK and final still go
// ahead of the progress backlog of other chats.
func TestProgressCoalescedWhileWritesArePaced(t *testing.T) {
	r, _ := backlogRig(t, func(c *Config) { c.WriteRate = 4 }, 4, 200, func(int) string { return store.OutProgress })
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
	if n != 1 {
		t.Fatalf("%d progress messages while the account's writes are paced, want 1", n)
	}
}
