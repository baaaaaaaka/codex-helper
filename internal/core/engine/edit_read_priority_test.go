package engine

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// A chat edit's read (the user label on the owner's message) waits for a
// read token behind a catch-up of hundreds of background reads. While it
// waits, the receiver dispatches only the live reads (the control chat, a
// chat with a running turn) and leaves the next token to the edit; before,
// the background reads took every token and the label was dropped after
// its tries (CATCHUP-LIVE).
func TestAnEditWaitingForAReadGoesBeforeTheBackgroundReads(t *testing.T) {
	clk := &testClock{t: time.Unix(10_000, 0)}
	e := priorityEngine()
	e.now = clk.now
	e.readWake = make(chan struct{}, 1)
	e.readTokens = newPacer(100, 30*time.Second, clk.now)
	t0 := clk.now()
	for id := int64(10); id < 20; id++ {
		e.addConv(id, &convState{nextRead: t0.Add(-time.Minute), lastActivity: t0.Add(-time.Hour)})
	}
	e.addConv(1, &convState{kind: store.KindControl, nextRead: t0})
	e.addConv(2, &convState{running: &runState{}, nextRead: t0})

	dispatch := func() []int64 {
		jobs := make(chan int64, 64)
		if _, ok := e.dispatchReads(context.Background(), jobs); !ok {
			t.Fatal("dispatchReads: context ended")
		}
		close(jobs)
		var got []int64
		for id := range jobs {
			got = append(got, id)
		}
		return got
	}

	e.readWaiters.Add(1)
	got := dispatch()
	slices.Sort(got)
	if !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("with an edit waiting the receiver dispatched %v, want only the live reads [1 2]", got)
	}
	e.mu.Lock()
	for id := int64(10); id < 20; id++ {
		if e.convs[id].reading {
			e.mu.Unlock()
			t.Fatalf("background read %d stays marked as reading while the edit waits", id)
		}
	}
	for _, id := range got {
		e.convs[id].reading = false
		e.convs[id].nextRead = t0.Add(time.Hour)
	}
	e.mu.Unlock()

	// The edit has its token: the background reads go again.
	e.readWaiters.Add(-1)
	got = dispatch()
	if len(got) != 10 {
		t.Fatalf("after the edit the receiver dispatched %v, want the 10 background reads", got)
	}
}

// An edit deferred because its read could not get a token in time is not
// a failed try: it is tried again with backoff for editDeferFor, also
// after more deferrals than editTries, and dropped only then.
func TestADeferredEditIsRetriedUntilItsDeferralWindowEnds(t *testing.T) {
	clk := &testClock{t: time.Unix(10_000, 0)}
	e := priorityEngine()
	e.now = clk.now
	e.log = io.Discard
	e.senderWake = make(chan struct{}, 1)
	e.addConv(5, &convState{})
	runs := 0
	e.queueEdit(5, chatEdit{key: "label:m", what: "user label", run: func(context.Context, string) error {
		runs++
		return errEditLater
	}})
	var gaps []time.Duration
	for i := 0; i < 50; i++ {
		e.mu.Lock()
		q := e.edits[5]
		e.mu.Unlock()
		if len(q) == 0 {
			break
		}
		if !q[0].notBefore.IsZero() {
			gaps = append(gaps, q[0].notBefore.Sub(clk.now()))
			clk.add(q[0].notBefore.Sub(clk.now()))
		}
		e.runEdit(context.Background(), 5)
	}
	if runs <= editTries {
		t.Fatalf("a deferred edit ran %d times, want more than editTries (%d): a deferral is not a failure", runs, editTries)
	}
	e.mu.Lock()
	left := len(e.edits[5])
	e.mu.Unlock()
	if left != 0 {
		t.Fatalf("the edit is still queued after %d runs; it must be dropped after editDeferFor", runs)
	}
	var total time.Duration
	for i, g := range gaps {
		total += g
		if g > editDeferMax || i > 0 && g < gaps[i-1] {
			t.Fatalf("retry gaps %v: want a backoff growing up to %s", gaps, editDeferMax)
		}
	}
	if gaps[0] != e.t.Cool || total < editDeferFor || total > editDeferFor+editDeferMax {
		t.Fatalf("retry gaps %v (total %s): want from Cool (%s) for about editDeferFor (%s)", gaps, total, e.t.Cool, editDeferFor)
	}
}

// A real failure still counts: an edit that fails editTries times is
// dropped.
func TestAFailingEditIsDroppedAfterItsTries(t *testing.T) {
	clk := &testClock{t: time.Unix(10_000, 0)}
	e := priorityEngine()
	e.now = clk.now
	e.log = io.Discard
	e.senderWake = make(chan struct{}, 1)
	e.addConv(5, &convState{})
	runs := 0
	e.queueEdit(5, chatEdit{key: "label:m", what: "user label", run: func(context.Context, string) error {
		runs++
		return errors.New("boom")
	}})
	for i := 0; i < 50; i++ {
		e.mu.Lock()
		q := e.edits[5]
		e.mu.Unlock()
		if len(q) == 0 {
			break
		}
		clk.add(q[0].notBefore.Sub(clk.now()))
		e.runEdit(context.Background(), 5)
	}
	if runs != editTries {
		t.Fatalf("a failing edit ran %d times, want editTries (%d)", runs, editTries)
	}
}
