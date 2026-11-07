package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

type steerAgent struct {
	*fakeAgent
	smu     sync.Mutex
	inputs  []TurnRequest
	reject  bool
	current string
	targets []string
	failure error
}

func (a *steerAgent) Steer(_ context.Context, r TurnRequest, expected string) error {
	a.smu.Lock()
	defer a.smu.Unlock()
	a.inputs = append(a.inputs, r)
	a.targets = append(a.targets, expected)
	if a.failure != nil {
		return a.failure
	}
	if a.reject {
		return ErrSteerExpired
	}
	return nil
}
func (a *steerAgent) count() int { a.smu.Lock(); defer a.smu.Unlock(); return len(a.inputs) }

func TestOrdinaryInputSteersWithoutAnotherTurnSlot(t *testing.T) {
	a := &steerAgent{fakeAgent: newAgent()}
	release := make(chan struct{})
	a.block["first work"] = release
	r := newRig(t, func(c *Config) { c.Agent = a; c.Turns = 1; c.FilesDir = t.TempDir() })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "first work")
	r.wait("first execution", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "helper queue queued work")
	r.ch.PostUser(work, "change direction")
	r.wait("steer at full concurrency", func() bool { return a.count() == 1 })
	if len(a.runLog()) != 1 {
		t.Fatal("steer created another execution")
	}
	a.smu.Lock()
	if !strings.Contains(a.inputs[0].Text, teamsSteerSafety) || strings.Contains(a.inputs[0].Text, teamsRequestSafety) {
		t.Fatal("steer must retain the active task's context")
	}
	ownerRoot := r.e.outboundDir(a.runLog()[0].TurnKey)
	if !strings.Contains(a.inputs[0].Text, ownerRoot) {
		t.Fatal("steer changed the active execution's artifact directory")
	}
	if !strings.Contains(a.inputs[0].Text, "change direction") {
		t.Fatal(a.inputs[0].Text)
	}
	a.smu.Unlock()
	close(release)
	r.wait("queued execution", func() bool { return len(a.runLog()) == 2 })
	runs := a.runLog()
	if !runs[1].Queue || strings.Contains(runs[1].Text, "helper queue") || !strings.Contains(runs[1].Text, "queued work") {
		t.Fatalf("queued request: %+v", runs[1])
	}
	r.wait("one answer per execution", func() bool { return len(r.own(work, "ANSWER to queued work")) == 1 })
}

func TestExpiredSteerBecomesNewTurnWithoutReplay(t *testing.T) {
	a := &steerAgent{fakeAgent: newAgent(), reject: true}
	release := make(chan struct{})
	a.block["first work"] = release
	r := newRig(t, func(c *Config) { c.Agent = a; c.Turns = 1 })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "first work")
	r.wait("running", func() bool { return len(a.runLog()) == 1 })
	r.ch.PostUser(work, "after boundary")
	r.wait("rejected steer", func() bool { return a.count() > 0 })
	close(release)
	r.wait("new execution", func() bool { return len(a.runLog()) == 2 })
	r.wait("new answer", func() bool { return len(r.own(work, "ANSWER to after boundary")) == 1 })
}

func TestExplicitQueueKeepsCommandTextOpaque(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "helper queue helper cancel all")
	r.wait("opaque queued input", func() bool { return len(r.ag.runLog()) == 1 })
	run := r.ag.runLog()[0]
	if !run.Queue || !strings.Contains(run.Text, "helper cancel all") {
		t.Fatalf("%+v", run)
	}
	// The same body in different submission modes must not be body-deduplicated.
	r.ch.PostUser(work, "helper cancel all")
	r.wait("command answered", func() bool { return len(r.own(work, "Nothing")) > 0 || len(r.own(work, "cancel")) > 0 })
}

func (a *fakeAgent) runLog() []TurnRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]TurnRequest(nil), a.runs...)
}

func (a *steerAgent) CurrentTurn(int64, string) string {
	a.smu.Lock()
	defer a.smu.Unlock()
	return a.current
}

// Neither external execution nor waiting for a native queued input should
// consume an extra helper execution just to append an ordinary user message.
func TestOrdinaryInputTargetsNativeTurnWithOrWithoutQueuedObserver(t *testing.T) {
	for _, queue := range []bool{false, true} {
		t.Run(map[bool]string{false: "external-only", true: "queue-awaiting-dispatch"}[queue], func(t *testing.T) {
			a := &steerAgent{fakeAgent: newAgent()}
			release := make(chan struct{})
			a.block["later task"] = release
			r := newRig(t, func(c *Config) { c.Agent = a; c.Turns = 1 })
			r.ag = a.fakeAgent
			work := r.newWork(t.TempDir())
			r.ch.PostUser(work, "seed")
			r.wait("seed completed", func() bool { return len(r.own(work, "ANSWER to seed")) == 1 })
			a.smu.Lock()
			a.current = "native-active"
			a.smu.Unlock()
			if queue {
				r.ch.PostUser(work, "helper queue later task")
				r.wait("queued observer", func() bool { return len(a.runLog()) == 2 })
			}
			r.ch.PostUser(work, "additional direction")
			r.wait("input delivered", func() bool { return a.count() == 1 })
			a.smu.Lock()
			target := a.targets[0]
			a.smu.Unlock()
			if target != "native-active" {
				t.Fatalf("wrong target %q", target)
			}
			want := 1
			if queue {
				want = 2
			}
			if len(a.runLog()) != want {
				t.Fatal("additional input created an execution")
			}
			a.smu.Lock()
			a.current = ""
			a.smu.Unlock()
			close(release)
		})
	}
}

func TestUncertainSteerPausesWithoutReplay(t *testing.T) {
	a := &steerAgent{fakeAgent: newAgent(), failure: errors.New("connection lost after write")}
	r := newRig(t, func(c *Config) { c.Agent = a })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "seed")
	r.wait("seed", func() bool { return len(r.own(work, "ANSWER to seed")) == 1 })
	a.smu.Lock()
	a.current = "native-active"
	a.smu.Unlock()
	r.ch.PostUser(work, "uncertain input")
	r.wait("uncertain notice", func() bool { return len(r.own(work, "may have reached Codex")) == 1 })
	if a.count() != 1 || len(a.runLog()) != 1 {
		t.Fatal("uncertain input was replayed")
	}
}
