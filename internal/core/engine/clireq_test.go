package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// cliRig starts the rig's engine itself to reach CLIRequest (gap batch
// G: the `cxp teams` commands that act through the running helper).
func cliRig(t *testing.T, tweak ...func(*Config)) (*rig, *Engine) {
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"),
		log: &strings.Builder{}, logMu: &sync.Mutex{}}
	st, err := store.OpenWith(r.path, store.Options{Exclusive: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	r.st = st
	cfg := Config{Store: st, Channel: r.ch, Agent: r.ag, Timing: FastTiming(20 * time.Millisecond),
		Machine: "m", Log: lockedWriter{r.logMu, r.log}}
	for _, f := range tweak {
		f(&cfg)
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel, r.done = cancel, make(chan error, 1)
	go func() { r.done <- e.Run(ctx) }()
	t.Cleanup(func() {
		if t.Failed() {
			r.logMu.Lock()
			t.Logf("engine log:\n%s", r.log.String())
			r.logMu.Unlock()
		}
		r.stop()
	})
	return r, e
}

func cliRun(t *testing.T, e *Engine, req CLIRequest) CLIResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		res, err := e.CLIRequest(ctx, req)
		if err == ErrCLIRetry && ctx.Err() == nil {
			time.Sleep(20 * time.Millisecond) // the control chat is not ready yet
			continue
		}
		if err != nil {
			t.Fatalf("CLIRequest %s: %v", req.Op, err)
		}
		return res
	}
}

func TestCLIRequestRefusals(t *testing.T) {
	r, e := cliRig(t)
	r.control()
	for _, c := range []struct {
		req  CLIRequest
		want string
	}{
		{CLIRequest{ID: "../x", Op: CLIRecreateControl}, "invalid request id"},
		{CLIRequest{ID: "1-a", Op: "frobnicate"}, `unknown request "frobnicate"`},
		{CLIRequest{ID: "1-b", Op: CLIRecreateChat, Selector: "s999"}, `Teams session not found for "s999"`},
		{CLIRequest{ID: "1-c", Op: CLIPublishHistory, Selector: "s999"}, `Teams session not found for "s999"`},
		{CLIRequest{ID: "1-d", Op: CLISendFile, Selector: "s999", File: "/nonexistent"}, "Teams session not found: s999"},
		{CLIRequest{ID: "1-e", Op: CLISendFile, ChatID: "not-a-chat", File: "/nonexistent"}, "Teams chat not-a-chat is not a chat of this helper"},
	} {
		res := cliRun(t, e, c.req)
		if !strings.Contains(res.Error, c.want) || len(res.Lines) != 0 {
			t.Errorf("%s %q: got %+v, want error %q", c.req.Op, c.req.Selector, res, c.want)
		}
	}
}

// A work chat recreated by its chat id (any case, the 12-character prefix
// match of v0.1.20; the fake ids are shorter than 12): the old chat
// gets the moved link, the session answers in the new one, and the same
// request handed in again returns the saved result without a second chat.
func TestCLIRecreateWorkChatOnceByChatID(t *testing.T) {
	r, e := cliRig(t)
	old := r.newWork(t.TempDir())
	req := CLIRequest{ID: "17-ab", Op: CLIRecreateChat, Selector: strings.ToUpper(old)}
	res := cliRun(t, e, req)
	if res.Error != "" {
		t.Fatalf("recreate: %s", res.Error)
	}
	fresh := r.chatOf("s001")
	if fresh == "" || fresh == old {
		t.Fatalf("s001 still on %q (was %q)", fresh, old)
	}
	joined := strings.Join(res.Lines, "\n")
	for _, want := range []string{"Teams work chat recreated: ", "Chat ID: " + fresh, "Session: s001", "Previous Chat ID: " + old} {
		if !strings.Contains(joined, want) {
			t.Errorf("result lacks %q:\n%s", want, joined)
		}
	}
	if n := len(r.own(old, "This chat moved")); n != 1 {
		t.Errorf("old chat has %d moved links, want 1", n)
	}
	r.wait("recreated notice", func() bool { return len(r.own(fresh, "Work chat recreated: s001.")) == 1 })

	again := cliRun(t, e, req)
	if strings.Join(again.Lines, "\n") != joined || again.Error != "" {
		t.Errorf("replay returned %+v, want the saved result", again)
	}
	if got := r.chatOf("s001"); got != fresh {
		t.Errorf("replay moved s001 to %q", got)
	}
	time.Sleep(200 * time.Millisecond)
	if n := len(r.own(old, "This chat moved")); n != 1 {
		t.Errorf("after replay the old chat has %d moved links, want 1", n)
	}

	r.ch.PostUser(fresh, "after recreate")
	r.wait("answer in the new chat", func() bool { return len(r.own(fresh, "ANSWER to after recreate")) == 1 })
	r.ch.PostUser(old, "in the old chat")
	time.Sleep(300 * time.Millisecond)
	if n := len(r.own(old, "ANSWER to in the old chat")); n != 0 {
		t.Errorf("the old chat is still served (%d answers)", n)
	}
}

func TestCLISendFileToSession(t *testing.T) {
	files := t.TempDir()
	r, e := cliRig(t, func(c *Config) { c.FilesDir = files })
	work := r.newWork(t.TempDir())
	src := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(src, []byte("notes"), 0o600); err != nil {
		t.Fatal(err)
	}
	res := cliRun(t, e, CLIRequest{ID: "18-cd", Op: CLISendFile, Selector: "s001", File: src, Message: "see notes"})
	if res.Error != "" || len(res.Lines) < 2 || res.Lines[0] != "Sent Teams file attachment: notes.txt" || res.Lines[1] != "Target session: s001" {
		t.Fatalf("send-file: %+v", res)
	}
	msgs := r.ch.Messages(work)
	msgAt, fileAt := -1, -1
	for i, m := range msgs {
		if m.Marker != "" && strings.Contains(m.Text, "see notes") && msgAt < 0 {
			msgAt = i
		}
		if m.Marker != "" && strings.Contains(m.Text, "notes.txt") && fileAt < 0 { // the attachment card
			fileAt = i
		}
	}
	if msgAt < 0 || fileAt < 0 || msgAt > fileAt {
		t.Fatalf("message at %d, file at %d; want the message first", msgAt, fileAt)
	}
}

// `chat recreate --history full` with the store in the single-connection
// mode of a network filesystem (the rig's store): the request's store
// reads happen outside its transaction, so it completes instead of
// waiting forever on the one connection.
func TestCLIRecreateWithHistorySingleConnection(t *testing.T) {
	sessions := t.TempDir()
	r, e := cliRig(t, func(c *Config) { c.SessionsDir = sessions })
	r.control()
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "local prompt", Answer: "local answer"}); err != nil {
		t.Fatal(err)
	}
	old := r.localChat()
	r.wait("local records mirrored", func() bool { return len(r.own(old, "local answer")) == 1 })
	res := cliRun(t, e, CLIRequest{ID: "18-ab", Op: CLIRecreateChat, Selector: "s001", History: true})
	if res.Error != "" {
		t.Fatalf("recreate: %s", res.Error)
	}
	fresh := r.chatOf("s001")
	if fresh == "" || fresh == old {
		t.Fatalf("s001 still on %q (was %q)", fresh, old)
	}
	r.wait("history in the new chat", func() bool { return len(r.own(fresh, "local answer")) == 1 })
	r.wait("recreated notice", func() bool { return len(r.own(fresh, "Work chat recreated: s001.")) == 1 })
}
