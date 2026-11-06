package engine

import (
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// priorityEngine is an engine in scan mode with production timing and no
// conversations.
func priorityEngine() *Engine {
	return &Engine{t: ProductionTiming(), gRead: "read", gates: map[string]time.Time{},
		convs: map[int64]*convState{}, byChat: map[string]int64{}, scanner: &lagScanner{}}
}

func (e *Engine) addConv(id int64, cs *convState) *convState {
	cs.id = id
	if cs.chat == "" {
		cs.chat = fmt.Sprintf("chat-%d", id)
	}
	if cs.kind == "" {
		cs.kind = store.KindWork
	}
	if cs.state == "" {
		cs.state = store.ConvActive
	}
	e.convs[id], e.byChat[cs.chat] = cs, id
	return cs
}

// A catch-up queues hundreds of background reads (one start-up read of
// every chat, a read-back of each chat whose newest message is a catch-up
// post) at the account's read pace. The reads the owner waits for go
// first: the control chat's poll, a chat with a running turn or recent
// owner activity, and a chat the scan showed with a message that is not the
// helper's own post; the background reads follow, still oldest first.
// Before, the control chat's poll (due 5 s after its last read) sorted
// after every start-up read (due at the start) and waited 184 s in the
// real-store run.
func TestLiveReadsGoBeforeTheCatchUpsBackgroundReads(t *testing.T) {
	t0 := time.Unix(10_000, 0)
	now := t0.Add(30 * time.Second)
	e := priorityEngine()
	// Start-up reads, due since the start.
	for id := int64(10); id < 20; id++ {
		e.addConv(id, &convState{nextRead: t0, lastActivity: t0})
	}
	e.addConv(1, &convState{kind: store.KindControl, nextRead: now.Add(-time.Second)})
	e.addConv(2, &convState{running: &runState{}, nextRead: now})
	e.addConv(3, &convState{lastUser: now.Add(-time.Minute), nextRead: now})
	// Read-backs: the scan shows the helper's own catch-up post.
	for id := int64(30); id < 33; id++ {
		cs := e.addConv(id, &convState{ownPosts: []string{fmt.Sprintf("own-%d", id)}, nextRead: now.Add(time.Hour)})
		e.markScanned([]channel.ChatActivity{{Chat: cs.chat, LastID: fmt.Sprintf("own-%d", id), Last: t0}}, t0.Add(-time.Minute), t0.Add(time.Duration(id-20)*time.Second))
	}
	// The owner posts in an idle chat: the scan shows a message that is
	// not the helper's.
	owner := e.addConv(40, &convState{nextRead: now.Add(time.Hour)})
	e.markScanned([]channel.ChatActivity{{Chat: owner.chat, LastID: "owner-msg", Last: now}}, t0.Add(-time.Minute), now)

	got, live := e.orderReads(now)
	wantLive := []int64{40, 1, 2, 3} // the scan's live flag, then the polls most overdue first
	if !slices.Equal(got[:3], wantLive[:3]) || !live[0] || !live[1] || !live[2] {
		t.Fatalf("first reads %v (live %v), want %v first", got, live, wantLive)
	}
	// Three live reads in a row, then the oldest background read (the
	// first read-back), then the last live read.
	if got[3] != 30 || live[3] || got[4] != 3 || !live[4] {
		t.Fatalf("reads %v (live %v): want the oldest read-back (30) after three live reads, then the last live read (3)", got, live)
	}
	wantBG := []int64{31, 32, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19} // read-backs oldest flag first, then start-up reads
	if !slices.Equal(got[5:], wantBG) {
		t.Fatalf("background reads %v, want %v", got[5:], wantBG)
	}
	if slices.Contains(live[5:], true) {
		t.Fatalf("background reads marked live: %v", live)
	}
}

// Without an activity scan (polling mode) a chat's timed poll is the only
// way the owner's message in it is found: every read is live and the most
// overdue goes first, as before. The control chat polled more often than
// the read budget allows does not take three reads in four from an idle
// chat's overdue poll (engine test TestProgressCoalescedWhileWritesArePaced
// saw the prompt read 4 s late when polls were background reads).
func TestPollingModeReadsMostOverdueFirst(t *testing.T) {
	now := time.Unix(10_000, 0)
	e := priorityEngine()
	e.scanner = nil
	e.addConv(1, &convState{kind: store.KindControl, nextRead: now.Add(-20 * time.Millisecond)})
	e.addConv(2, &convState{nextRead: now.Add(-time.Second)})
	e.addConv(3, &convState{lastUser: now.Add(-time.Second), nextRead: now.Add(-500 * time.Millisecond)})
	got, live := e.orderReads(now)
	if !slices.Equal(got, []int64{2, 3, 1}) || slices.Contains(live, false) {
		t.Fatalf("reads %v (live %v), want [2 3 1], all live", got, live)
	}
}

// Live reads never starve the background: while live reads keep coming,
// every fourth read is the oldest background read (liveReadsInARow).
func TestBackgroundReadsGetEveryFourthReadWhileLiveReadsWait(t *testing.T) {
	now := time.Unix(10_000, 0)
	e := priorityEngine()
	for id := int64(1); id <= 8; id++ {
		e.addConv(id, &convState{running: &runState{}, nextRead: now.Add(-time.Duration(id) * time.Millisecond)})
	}
	for id := int64(20); id < 23; id++ {
		e.addConv(id, &convState{nextRead: now.Add(-time.Hour)})
	}
	_, live := e.orderReads(now)
	want := []bool{true, true, true, false, true, true, true, false, true, true, false}
	if !slices.Equal(live, want) {
		t.Fatalf("live pattern %v, want %v", live, want)
	}
	// The run carries over dispatches: after three live reads the next
	// background read goes first.
	for _, cs := range e.convs {
		cs.reading = false
	}
	e.liveRun = liveReadsInARow
	got, live := e.orderReads(now)
	if live[0] || got[0] != 20 {
		t.Fatalf("after three live reads the first read is %d (live %v), want background 20", got[0], live[0])
	}
}

// A live flag is cleared by a read that started after the change, kept by
// one that was already running; a later scan that shows only the helper's
// own post keeps a pending live flag.
func TestLiveFlagSurvivesAReadThatStartedBeforeIt(t *testing.T) {
	now := time.Unix(10_000, 0)
	e := priorityEngine()
	cs := e.addConv(1, &convState{ownPosts: []string{"own-1", "own-2"}, nextRead: now.Add(time.Hour), lastActivity: now})
	scan := func(last string) {
		e.markScanned([]channel.ChatActivity{{Chat: cs.chat, LastID: last, Last: now}}, now.Add(-time.Minute), now)
	}
	scan("own-1")
	if !cs.scanDue || cs.scanLive != 0 {
		t.Fatalf("own post: due %v live %d, want a background read-back", cs.scanDue, cs.scanLive)
	}
	gen := cs.scanGen // a read starts
	scan("owner-1")   // the owner posts while it runs
	scan("own-2")     // and the helper's next post lands
	if cs.scanLive == 0 {
		t.Fatal("a scan of the helper's post dropped the owner's pending change")
	}
	finishRead(e, cs, gen, now)
	if !cs.scanDue || cs.scanLive == 0 || !e.liveReadLocked(cs, now) {
		t.Fatalf("read that started before the owner's message cleared it: due %v live %d", cs.scanDue, cs.scanLive)
	}
	finishRead(e, cs, cs.scanGen, now)
	if cs.scanDue || cs.scanLive != 0 {
		t.Fatalf("read after the change left due %v live %d", cs.scanDue, cs.scanLive)
	}
}

// finishRead applies the successful completion of a read that started at
// scan mark gen, as read() does.
func finishRead(e *Engine, cs *convState, gen uint64, now time.Time) {
	cs.nextRead = e.nextReadLocked(cs, now, now, false)
	if cs.scanLive <= gen {
		cs.scanLive = 0
	}
	if cs.scanGen != gen {
		cs.nextRead = now
	}
	cs.scanDue = cs.scanGen != gen
}

// Problem 2 of the 2026-11-05 real-store run: 362 chats read together at
// the catch-up came back together every SafetyRead as one wave of reads at
// the full read pace (idle CPU 0.252% in that minute, budget 0.2%). A read
// that was not a safety read brings the next safety read forward by up to
// half the window, by a fraction spread over the chat ids; a safety read
// schedules the next exactly one window later, on time or late (waiting for
// a read token). So the wave spreads over at least half the window once,
// stays spread, and no read is added: one read per chat per window.
//
// A first version put the safety reads on a per-chat lattice of wall-clock
// slots half a window apart, the next being the first slot after half a
// window. A read a hair before its slot then counted as before it: in the
// real-store run most chats were read every 5 min instead of every 10 (845
// chat reads in the 12-minute idle window instead of about 520). The
// interval of a safety read must not depend on where it falls in time.
func TestSafetyReadsSpreadOverTheWindow(t *testing.T) {
	period := ProductionTiming().SafetyRead
	t0 := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	const chats, waves = 362, 6
	e := priorityEngine()
	reads := make([][]time.Time, waves)
	for id := int64(1); id <= chats; id++ {
		cs := e.addConv(id, &convState{lastUser: t0.Add(-time.Hour)})
		// The catch-up's start-up read or read-back, at 4/s.
		start := t0.Add(time.Duration(id) * 250 * time.Millisecond)
		cs.nextRead = start
		if safetyReadLocked(cs) {
			t.Fatalf("chat %d: the catch-up's read counted as a safety read", id)
		}
		next := e.nextReadLocked(cs, start, start, false)
		if !next.After(start.Add(period/2)) || next.After(start.Add(period)) {
			t.Fatalf("chat %d read at %s: next safety read after %s, want in (%s, %s]", id, start.Sub(t0), next.Sub(start), period/2, period)
		}
		for w := 0; w < waves; w++ {
			cs.nextRead = next
			if !safetyReadLocked(cs) {
				t.Fatalf("chat %d wave %d: the scheduled safety read did not count as one", id, w)
			}
			// On time, a millisecond late, or 0.7 s late (a read token).
			s := next.Add([]time.Duration{0, time.Millisecond, 700 * time.Millisecond}[(int(id)+w)%3])
			reads[w] = append(reads[w], s)
			next = e.nextReadLocked(cs, s, s, true)
			if next.Sub(s) != period {
				t.Fatalf("chat %d wave %d: safety read at %s schedules the next after %s, want %s", id, w, s.Sub(t0), next.Sub(s), period)
			}
		}
	}
	for w, at := range reads {
		sort.Slice(at, func(i, j int) bool { return at[i].Before(at[j]) })
		if span := at[len(at)-1].Sub(at[0]); span < period/2 {
			t.Fatalf("wave %d: the safety reads of %d chats span %s, want at least %s", w, chats, span, period/2)
		}
		// Before: 362 reads in 90 s (about 240 in the busiest minute).
		// Spread over 5 min they average about 72 a minute.
		worst := 0
		for i := range at {
			j := sort.Search(len(at), func(k int) bool { return !at[k].Before(at[i].Add(time.Minute)) })
			worst = max(worst, j-i)
		}
		if worst > 100 {
			t.Fatalf("wave %d: %d safety reads in the busiest minute, want at most 100", w, worst)
		}
	}
}

// Only the scheduled safety read keeps its interval: a read the scan or
// the owner's activity caused, or that a failure moved, is not one. Only
// an idle chat in scan mode gets a safety read: the control chat and polled
// chats keep their poll interval.
func TestOnlyIdleChatsTakeASafetyRead(t *testing.T) {
	now := time.Unix(10_000, 0)
	e := priorityEngine()
	e.now = func() time.Time { return now }
	ctl := e.addConv(1, &convState{kind: store.KindControl})
	hot := e.addConv(2, &convState{lastUser: now.Add(-time.Second)})
	idle := e.addConv(3, &convState{lastUser: now.Add(-time.Hour)})
	if got := e.nextReadLocked(ctl, now, now, false); !got.Equal(now.Add(e.t.Control)) || !ctl.safeAt.IsZero() {
		t.Errorf("control chat next read after %s, want %s", got.Sub(now), e.t.Control)
	}
	if got := e.nextReadLocked(hot, now, now, true); !got.Equal(now.Add(e.t.Hot)) || !hot.safeAt.IsZero() {
		t.Errorf("hot chat next read after %s, want %s", got.Sub(now), e.t.Hot)
	}
	want := now.Add(e.t.SafetyRead - safetyJitter(3, e.t.SafetyRead))
	if got := e.nextReadLocked(idle, now, now, false); !got.Equal(want) {
		t.Errorf("idle chat next read after %s, want %s", got.Sub(now), want.Sub(now))
	}
	idle.nextRead = want
	if !safetyReadLocked(idle) {
		t.Fatal("the scheduled safety read does not count as one")
	}
	e.markScanned([]channel.ChatActivity{{Chat: idle.chat, LastID: "owner-msg", Last: now}}, now.Add(-time.Minute), now)
	if safetyReadLocked(idle) {
		t.Error("a read the scan marked counts as a safety read")
	}
	idle.scanDue, idle.nextRead = false, want
	e.heatUser(3)
	if safetyReadLocked(idle) {
		t.Error("a read the owner's activity moved counts as a safety read")
	}
}
