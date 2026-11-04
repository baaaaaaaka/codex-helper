package engine

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// fakeDeleg is a plain-text delegation protocol:
//
//	DREQ <id> <machine> <prompt words…>
//	DCLAIM|DRUN <id> <machine> <worker>
//	DRESULT <id> <machine> <worker> ok|fail <body…>
//	DCANCEL <id>
type fakeDeleg struct{ machine string }

func (d fakeDeleg) fields(text string) []string {
	return strings.Fields(strings.NewReplacer("`", " ", ":", " ").Replace(text))
}

func (d fakeDeleg) Decode(text string, _ time.Time) (DelegRecord, bool) {
	f := d.fields(text)
	if len(f) < 2 {
		return DelegRecord{}, false
	}
	r := DelegRecord{ID: f[1]}
	switch f[0] {
	case "DREQ":
		r.Kind = DelegRequest
	case "DCLAIM":
		r.Kind = DelegClaim
	case "DRUN":
		r.Kind = DelegStatus
	case "DRESULT":
		r.Kind = DelegResult
	case "DCANCEL":
		r.Kind = DelegCancel
		return r, true
	default:
		return DelegRecord{}, false
	}
	if len(f) < 3 {
		return DelegRecord{}, false
	}
	r.Mine = f[2] == d.machine
	if len(f) > 3 && r.Kind != DelegRequest {
		r.Worker = f[3]
	}
	return r, true
}

func (d fakeDeleg) req(request string) []string { return d.fields(request) }

func (d fakeDeleg) Prompt(request string) (string, error) {
	return "Delegated: " + strings.Join(d.req(request)[3:], " "), nil
}
func (d fakeDeleg) Claim(request, worker string, _ time.Time) (string, error) {
	f := d.req(request)
	return fmt.Sprintf("DCLAIM %s %s %s", f[1], f[2], worker), nil
}
func (d fakeDeleg) Running(request, worker string, _ time.Time) (string, error) {
	f := d.req(request)
	return fmt.Sprintf("DRUN %s %s %s", f[1], f[2], worker), nil
}
func (d fakeDeleg) Result(request, worker string, ok bool, body string, _ time.Time) (string, error) {
	f := d.req(request)
	st := "fail"
	if ok {
		st = "ok"
	}
	return fmt.Sprintf("DRESULT %s %s %s %s %s", f[1], f[2], worker, st, body), nil
}

func delegRig(t *testing.T, tweak ...func(*Config)) (*rig, string) {
	r := newRig(t, append([]func(*Config){func(c *Config) {
		c.Registry, c.Delegations, c.DelegationDir = true, fakeDeleg{machine: "m"}, t.TempDir()
	}}, tweak...)...)
	r.control()
	var inbox string
	r.wait("inbox chat", func() bool { inbox = r.chatOf("inbox"); return inbox != "" })
	return r, inbox
}

// A request addressed to this machine is claimed, run once as a Codex
// turn and answered with one result; a repeated copy is not run again;
// requests for other machines are left alone.
func TestDelegationRoundTripRunsOnce(t *testing.T) {
	r, inbox := delegRig(t)
	r.ch.PostUser(inbox, "DREQ d1 m check the build")
	r.ch.PostUser(inbox, "DREQ d2 other not for us")
	r.wait("result", func() bool { return len(r.own(inbox, "DRESULT d1")) == 1 })
	if n := r.ag.calls("Delegated: check the build"); n != 1 {
		t.Fatalf("%d runs", n)
	}
	res := r.own(inbox, "DRESULT d1")[0].Text
	if !strings.Contains(res, " ok ANSWER to Delegated: check the build") {
		t.Fatalf("result %q", res)
	}
	for _, rec := range []string{"DCLAIM d1", "DRUN d1"} {
		if n := len(r.own(inbox, rec)); n != 1 {
			t.Fatalf("%d %s records", n, rec)
		}
	}
	r.ch.PostUser(inbox, "DREQ d1 m check the build")
	time.Sleep(1500 * time.Millisecond) // > one safety read in fast timing
	if n := r.ag.calls("Delegated: check the build"); n != 1 {
		t.Fatalf("repeated request ran again (%d runs)", n)
	}
	if n := len(r.own(inbox, "DRESULT d1")); n != 1 {
		t.Fatalf("%d results", n)
	}
	// " d2 ": the random worker id of d1's records may contain "d2".
	if r.ag.calls("not for us") != 0 || len(r.own(inbox, " d2 ")) != 0 {
		t.Fatal("a request for another machine was taken")
	}
	// Only protocol records in the inbox: no acks, progress or notices.
	for _, m := range r.ch.Messages(inbox) {
		if m.ByOwner && m.Marker != "" && !strings.Contains(m.Text, "d1") {
			t.Fatalf("non-record message in the inbox: %q", m.Text)
		}
	}
	if m := len(r.own(r.chatOf("control"), "Codex finished")); m != 0 {
		t.Fatalf("%d owner notifications for a delegated task", m)
	}
}

// A helper stop while a delegated task runs: after the restart the task is
// not run again and the requester gets exactly one result naming the
// interruption; a second restart adds nothing.
func TestDelegationRestartMidTaskNotRerun(t *testing.T) {
	r, inbox := delegRig(t)
	r.ag.mu.Lock()
	r.ag.block["long migration"] = make(chan struct{})
	r.ag.mu.Unlock()
	r.ch.PostUser(inbox, "DREQ d7 m run the long migration")
	r.wait("task running", func() bool { return r.ag.calls("long migration") == 1 })
	r.wait("claimed", func() bool { return len(r.own(inbox, "DRUN d7")) == 1 })
	r.stop()
	r.start()
	r.wait("result after restart", func() bool { return len(r.own(inbox, "DRESULT d7")) == 1 })
	res := r.own(inbox, "DRESULT d7")[0].Text
	if !strings.Contains(res, " fail ") || !strings.Contains(res, "interrupted") {
		t.Fatalf("result %q", res)
	}
	r.stop()
	r.start()
	time.Sleep(1500 * time.Millisecond)
	if n := r.ag.calls("long migration"); n != 1 {
		t.Fatalf("interrupted task ran %d times", n)
	}
	for _, rec := range []string{"DCLAIM d7", "DRUN d7", "DRESULT d7"} {
		if n := len(r.own(inbox, rec)); n != 1 {
			t.Fatalf("%d %s records", n, rec)
		}
	}
}

// A requester cancel before the task starts: it never runs and gets no
// result (the protocol ignores results after a cancel).
func TestDelegationCancelledBeforeStart(t *testing.T) {
	r, inbox := delegRig(t)
	r.ag.mu.Lock()
	r.ag.block["first task"] = make(chan struct{})
	release := r.ag.block["first task"]
	r.ag.mu.Unlock()
	r.ch.PostUser(inbox, "DREQ d8 m first task")
	r.wait("first running", func() bool { return r.ag.calls("first task") == 1 })
	r.ch.PostUser(inbox, "DREQ d9 m second task")
	r.wait("second claimed", func() bool { return len(r.own(inbox, "DCLAIM d9")) == 1 })
	r.ch.PostUser(inbox, "DCANCEL d9")
	time.Sleep(1500 * time.Millisecond)
	close(release)
	r.wait("first result", func() bool { return len(r.own(inbox, "DRESULT d8")) == 1 })
	time.Sleep(time.Second)
	if n := r.ag.calls("second task"); n != 0 {
		t.Fatalf("a cancelled task ran %d times", n)
	}
	if n := len(r.own(inbox, "DRESULT d9")); n != 0 {
		t.Fatalf("%d results for a cancelled task", n)
	}
}
