package entry

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// shadowLive injects live activity during the drain of a "live" shadow
// run: once the catch-up has posted `after` messages, the owner starts a
// work chat (`new`) and sends it a prompt, and a local Codex session (a
// rollout the test writes into a writable day directory of the sessions
// tree: the real day directories stay read-only) runs one turn with a
// commentary. It measures how long each takes to reach Teams while the
// catch-up is still pacing, and what the owner is notified of.
type shadowLive struct {
	mu   sync.Mutex
	done chan struct{}
	res  map[string]any
	errs []string
}

func (l *shadowLive) set(k string, v any) {
	l.mu.Lock()
	l.res[k] = v
	l.mu.Unlock()
}

func (l *shadowLive) fail(format string, a ...any) {
	l.mu.Lock()
	l.errs = append(l.errs, fmt.Sprintf(format, a...))
	l.mu.Unlock()
}

func shadowPosts(s *harness.Scenario) int {
	return len(s.Graph.Requests(fakegraph.RequestFilter{Route: routePostMessage, OnlyOK: true}))
}

// shadowWaitText waits for a SUT message containing sub in chat ("": any).
func shadowWaitText(s *harness.Scenario, chat, sub string, d time.Duration) (fakegraph.Message, bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) && s.Ctx().Err() == nil {
		if ms := s.SUTMessagesContaining(chat, sub); len(ms) > 0 {
			return ms[0], true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fakegraph.Message{}, false
}

func startShadowLive(s *harness.Scenario, home, control string, after int) *shadowLive {
	l := &shadowLive{done: make(chan struct{}), res: map[string]any{}}
	go func() {
		defer close(l.done)
		for shadowPosts(s) < after {
			if s.Ctx().Err() != nil {
				return
			}
			time.Sleep(time.Second)
		}
		l.set("injected_after_posts", shadowPosts(s))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); l.owner(s, control) }()
		go func() { defer wg.Done(); l.local(s, home) }()
		wg.Wait()
		l.set("posts_when_done", shadowPosts(s))
	}()
	return l
}

// owner: `new <dir>` in the control chat, then a prompt in the new chat.
func (l *shadowLive) owner(s *harness.Scenario, control string) {
	dir := "/tmp/shadow-live-work"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		l.fail("mkdir: %v", err)
		return
	}
	t0 := time.Now()
	if _, err := s.Graph.PostAsUser(control, "new "+dir); err != nil {
		l.fail("post new: %v", err)
		return
	}
	created, ok := shadowWaitText(s, "", "Working directory: "+dir, 30*time.Minute)
	if !ok {
		l.fail("no work chat for `new %s`", dir)
		return
	}
	l.set("owner_new_to_chat_s", time.Since(t0).Seconds())
	token := s.Token("LIVEOWNER")
	t1 := time.Now()
	if _, err := s.Graph.PostAsUser(created.ChatID, "live owner prompt "+token); err != nil {
		l.fail("post prompt: %v", err)
		return
	}
	if _, ok := shadowWaitText(s, created.ChatID, "Request accepted", 30*time.Minute); ok {
		l.set("owner_prompt_to_ack_s", time.Since(t1).Seconds())
	}
	if _, ok := shadowWaitText(s, created.ChatID, "Codex finished responding", 30*time.Minute); !ok {
		l.fail("no answer to the owner prompt")
		return
	}
	l.set("owner_prompt_to_answer_s", time.Since(t1).Seconds())
	l.set("owner_chat", created.ChatID)
}

// local: a new local session with one turn (commentary + answer).
func (l *shadowLive) local(s *harness.Scenario, home string) {
	sessions := filepath.Join(home, "sessions")
	// rollout.New writes under today's UTC day directory; when the real
	// tree has that day (read-only), start the session tomorrow instead.
	start := time.Now().UTC()
	if _, err := os.Stat(filepath.Join(sessions, start.Format("2006/01/02"))); err == nil {
		if f, err := os.CreateTemp(filepath.Join(sessions, start.Format("2006/01/02")), ".probe-"); err == nil {
			f.Close()
			os.Remove(f.Name())
		} else {
			start = start.AddDate(0, 0, 1)
		}
	}
	opts := rollout.Options{}
	if start.Day() != time.Now().UTC().Day() {
		opts.Start = start
	}
	cwd := "/tmp/shadow-live-local"
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		l.fail("mkdir: %v", err)
		return
	}
	w, err := rollout.New(sessions, cwd, opts)
	if err != nil {
		l.fail("rollout: %v", err)
		return
	}
	token := s.Token("LIVELOCAL")
	t0 := time.Now()
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "live local prompt " + token, Commentary: "live local commentary " + token,
		Answer: "live local answer " + token}); err != nil {
		l.fail("append: %v", err)
		return
	}
	l.set("local_rollout", w.Path)
	p, ok := shadowWaitText(s, "", "live local prompt "+token, 30*time.Minute)
	if !ok {
		l.fail("local prompt never mirrored")
		return
	}
	l.set("local_to_prompt_s", time.Since(t0).Seconds())
	if _, ok := shadowWaitText(s, "", "live local answer "+token, 30*time.Minute); !ok {
		l.fail("local answer never mirrored")
		return
	}
	l.set("local_to_answer_s", time.Since(t0).Seconds())
	l.set("local_chat", p.ChatID)
	if _, ok := shadowWaitText(s, "", "live local commentary "+token, time.Minute); ok {
		l.set("local_commentary_mirrored", true)
	} else {
		l.set("local_commentary_mirrored", false)
	}
}

// shadowNotifications lists what the control chat told the owner
// (@mentions): final notices and local sessions announced. Which finals
// a notice covers is in the store (delivery key notify:<id>), counted by
// shadow_analyze.py.
func shadowNotifications(s *harness.Scenario, control string) (finals, locals int, texts []string) {
	for _, m := range s.SUTMessages(control) {
		t := harness.NormalizeSpace(m.PlainText())
		switch {
		case strings.Contains(m.Content, "<strong>✅ Codex finished</strong>"):
			finals++
		case strings.Contains(t, "New local Codex chat detected"):
			locals++
		default:
			continue
		}
		texts = append(texts, clip(t, 160))
	}
	return finals, locals, texts
}
