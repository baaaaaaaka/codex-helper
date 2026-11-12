package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestParkPolicyCommandsPersistWithoutWaking(t *testing.T) {
	r := newRig(t)
	ctl := r.control()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "helper park off")
	r.wait("work chat disables auto park", func() bool { return len(r.own(work, "s001: Auto park: off")) == 1 })
	if got := r.e.activeChatsText(); !strings.Contains(got, "**Auto park:** off") {
		t.Fatalf("control status omitted policy: %s", got)
	}
	if got := r.e.sessionDetails(r.e.findLabel("s001").id); !strings.Contains(got, "Auto park: off") {
		t.Fatalf("details omitted policy: %s", got)
	}
	r.ch.PostUser(work, "helper park")
	r.wait("manual park still works", func() bool { return len(r.own(work, "Parked s001")) == 1 })
	r.ch.PostUser(ctl, "park s001 status")
	r.wait("status says parked", func() bool { return len(r.own(ctl, "Chat state: parked")) == 1 })
	r.ch.PostUser(ctl, "park s001 off")
	r.wait("setting does not wake parked chat", func() bool { return len(r.own(ctl, "The chat remains parked")) == 2 })
	r.stop()
	r.start()
	r.control()
	r.ch.PostUser(ctl, "park s001 status")
	r.wait("policy survives restart", func() bool { return len(r.own(ctl, "The chat remains parked")) == 3 })
	r.ch.PostUser(ctl, "r s001")
	r.wait("resumed", func() bool { return len(r.own(ctl, "Resumed s001")) == 1 })
	r.ch.PostUser(work, "helper park status")
	r.wait("resumed chat retains off", func() bool { return len(r.own(work, "s001: Auto park: off")) == 2 })
	r.ch.PostUser(ctl, "park s001 on")
	r.wait("control enables auto park", func() bool { return len(r.own(ctl, "s001: Auto park: on")) == 1 })
	r.ch.PostUser(work, "helper park off extra")
	r.wait("invalid args show usage", func() bool { return len(r.own(work, "Usage:")) > 0 })
	r.ch.PostUser(work, "still active")
	r.wait("invalid args did not park", func() bool { return len(r.own(work, "ANSWER to still active")) == 1 })
	res, err := r.e.CLIRequest(context.Background(), CLIRequest{ID: "park-off", Op: CLISetParkPolicy, Selector: "s001", Message: "off"})
	if err != nil || res.Error != "" {
		t.Fatalf("CLI setting: %+v %v", res, err)
	}
	res, err = r.e.CLIRequest(context.Background(), CLIRequest{ID: "park-status", Op: CLISetParkPolicy, Selector: "s001", Message: "status"})
	if err != nil || len(res.Lines) != 1 {
		t.Fatalf("CLI query: %+v %v", res, err)
	}
	if err := r.st.Read(func(tx *store.Tx) error {
		_, ok, err := tx.CLIProgress("park-status")
		if ok {
			t.Error("status persisted CLI business state")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestParkPolicyMaintenanceRechecksStore(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	st, err := store.OpenWith(filepath.Join(t.TempDir(), "park.sqlite"), store.Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err = st.Acquire(store.Ident{Host: "test", PID: 1, BootID: "test"}); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err = st.Update(func(tx *store.Tx) error {
		var err error
		id, err = tx.CreateConversation(store.NewConversation{Label: "s001", Kind: store.KindWork, Channel: "teams", ExtChatID: "work"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{Store: st, Channel: channeltest.New(), Agent: newAgent(), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.load(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(72 * time.Hour)
	// Deliberately leave the candidate cache at on: the transaction must
	// observe the persisted off even before the setting's cache is published.
	if err = st.Update(func(tx *store.Tx) error { return tx.SetAutoPark(id, false) }); err != nil {
		t.Fatal(err)
	}
	e.parkIdle()
	if err = st.Read(func(tx *store.Tx) error {
		c, _, err := tx.Conversation(id)
		if c.State != store.ConvActive {
			t.Fatal("stale candidate parked an off chat")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	cs := e.convs[id]
	cs.nextRead = now
	e.mu.Unlock()
	if ids := e.dueReads(now); len(ids) != 1 || ids[0] != id {
		t.Fatalf("off chat no longer polled after 72h: %v", ids)
	}
	e.mu.Lock()
	cs.reading = false
	e.mu.Unlock()
	if err = st.Update(func(tx *store.Tx) error { return tx.SetAutoPark(id, true) }); err != nil {
		t.Fatal(err)
	}
	e.parkIdle()
	if err = st.Read(func(tx *store.Tx) error {
		c, _, err := tx.Conversation(id)
		if c.State != store.ConvParked {
			t.Fatal("expired on chat did not park")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// A rejected store transaction must not publish the attempted off policy.
	if err = st.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err = e.cliParkPolicy(CLIRequest{Selector: "s001", Message: "off"}); !errors.Is(err, store.ErrNotAcquired) {
		t.Fatalf("failed setting: %v", err)
	}
	if e.convs[id].autoParkOff {
		t.Fatal("uncommitted policy leaked into cache")
	}

}

func TestParkPolicyTierBoundaries(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	for _, scan := range []bool{false, true} {
		for _, off := range []bool{false, true} {
			t.Run(fmt.Sprintf("scan=%v/off=%v", scan, off), func(t *testing.T) {
				e := priorityEngine()
				if !scan {
					e.scanner = nil
				}
				cs := &convState{kind: store.KindWork, autoParkOff: off}
				for _, tc := range []struct{ age, want time.Duration }{{0, 2 * time.Second}, {2*time.Minute - time.Nanosecond, 2 * time.Second}, {2 * time.Minute, 5 * time.Second}, {15*time.Minute - time.Nanosecond, 5 * time.Second}, {15 * time.Minute, 10 * time.Second}, {4*time.Hour - time.Nanosecond, 10 * time.Second}, {4 * time.Hour, 30 * time.Second}, {72 * time.Hour, 30 * time.Second}} {
					cs.lastActivity = now.Add(-tc.age)
					cs.lastUser = cs.lastActivity
					if got := e.intervalLocked(cs, now); got != tc.want {
						t.Errorf("age %s: %s want %s", tc.age, got, tc.want)
					}
				}
				if scan {
					e.scanBlind = time.Minute
					want := time.Minute
					if off {
						want = 30 * time.Second
					}
					if got := e.intervalLocked(cs, now); got != want {
						t.Fatalf("scan outage: %s want %s", got, want)
					}
				}
				cs.running = &runState{}
				if got := e.intervalLocked(cs, now); got != 2*time.Second {
					t.Fatalf("running: %s", got)
				}
				cs.kind = store.KindControl
				if got := e.intervalLocked(cs, now); got != 5*time.Second {
					t.Fatalf("control: %s", got)
				}
			})
		}
	}
}

type parkForkAgent struct{ *fakeAgent }

func (a *parkForkAgent) Fork(context.Context, int64, string, string, string) (Forked, error) {
	return Forked{ThreadID: "park-policy-child"}, nil
}

func TestParkPolicyRecreateAndForkDefaults(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Agent = &parkForkAgent{c.Agent.(*fakeAgent)} })
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "bind the parent")
	r.wait("parent bound", func() bool { return len(r.own(work, "ANSWER to bind the parent")) == 1 })
	res := cliRun(t, r.e, CLIRequest{ID: "off-parent", Op: CLISetParkPolicy, Selector: "s001", Message: "off"})
	if res.Error != "" {
		t.Fatal(res.Error)
	}
	res = cliRun(t, r.e, CLIRequest{ID: "recreate-parent", Op: CLIRecreateChat, Selector: "s001"})
	if res.Error != "" {
		t.Fatal(res.Error)
	}
	res = cliRun(t, r.e, CLIRequest{ID: "query-parent", Op: CLISetParkPolicy, Selector: "s001", Message: "status"})
	if !strings.Contains(strings.Join(res.Lines, "\n"), "Auto park: off") {
		t.Fatalf("recreate lost setting: %+v", res)
	}
	work = r.chatOf("s001")
	r.ch.PostUser(work, "helper fork")
	r.wait("fork chat", func() bool { return r.chatOf("s002") != "" })
	res = cliRun(t, r.e, CLIRequest{ID: "query-child", Op: CLISetParkPolicy, Selector: "s002", Message: "status"})
	if !strings.Contains(strings.Join(res.Lines, "\n"), "Auto park: on") {
		t.Fatalf("fork inherited off: %+v", res)
	}
}

func BenchmarkParkPolicyInterval10000Chats(b *testing.B) {
	e := priorityEngine()
	now := time.Unix(1_800_000_000, 0)
	chats := make([]convState, 10000)
	for i := range chats {
		chats[i] = convState{kind: store.KindWork, lastActivity: now.Add(-24 * time.Hour), autoParkOff: true}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range chats {
			e.intervalLocked(&chats[j], now)
		}
	}
}

func TestParkPolicySlowerPollPreservesRetryBase(t *testing.T) {
	e := priorityEngine()
	if e.t.Hot != 2*time.Second || e.retryBase() != time.Second {
		t.Fatalf("poll %s retry %s", e.t.Hot, e.retryBase())
	}
	e.t.Hot = 20 * time.Millisecond
	if e.retryBase() != e.t.Hot {
		t.Fatal("short test/explicit interval no longer shortens retries")
	}
}
