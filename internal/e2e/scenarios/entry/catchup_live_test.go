package entry

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Live poll latencies of the production timing (SPEC N-2, N-3; v0.1.20
// poll_scheduler.go): the control chat is read every 5 s regardless of the
// backlog, a work chat with a running turn or recent owner activity every
// 1 s, and the activity scan that finds a new message in any other chat
// runs every 5 s. readSlack is what a read may add on top: waiting for the
// account's read token (4/s, one background read in between) and the
// request itself.
const (
	controlPoll = 5 * time.Second
	hotPoll     = time.Second
	scanPoll    = 5 * time.Second
	readSlack   = 2 * time.Second
)

// CATCHUP-LIVE: the owner uses the helper while a large catch-up runs. The
// first start on an rc.59 store with catchUpChats active chats reads every
// one of them once (start-up reads) while catchUpSessions local sessions
// are caught up silently (chat created, records posted, each post read
// back). These background reads queue at the account's read pace for well
// over a minute. Meanwhile the control chat is still read every 5 s; the
// owner's `new` there, a prompt in the new chat (found by the activity
// scan) and three follow-ups in that now active chat are each read within their
// live poll interval and answered; the catch-up stays silent and complete.
func TestCatchUpOwnerStaysLive(t *testing.T) {
	harness.Parallel(t)
	const (
		catchUpChats    = 300
		catchUpSessions = 16
	)
	s := harness.NewScenario(t, "CATCHUP-LIVE", harness.Options{
		Guarantees: []string{"N-2", "N-3", "catch-up-silent", "auto-migrate", "F-MIR-4"},
		Title:      "the owner's messages are read at the live poll interval while a large catch-up runs",
		Budget:     6 * time.Minute,
		NoStart:    true,
		Seam:       harness.Seam{ProductionTiming: true},
		Invariants: harness.Invariants{CPUBudget: 30 * time.Second},
		Golden: harness.GoldenOptions{Masks: []harness.Mask{{Pattern: regexp.MustCompile(`queued for \d+ min`), Replace: "queued for <n> min",
			Reason: "the rc.59 fixture's queued prompts date from 2026-09-01: their age at the reminder depends on the day the suite runs"}},
			Floating: []harness.Float{{Text: "New local Codex chats", Reason: "the new-chat summary waits until the catch-up has read every session; " +
				"the owner posts `new` while it still reads, so which comes first depends on the runner's speed"}}},
	})
	defer s.Finish()

	// The old store knows catchUpChats more active chats, polled until the
	// old helper stopped an hour ago.
	stopped := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	_, _, ids := rc59Store(s, func(state map[string]json.RawMessage) {
		var sessions, polls map[string]json.RawMessage
		if err := json.Unmarshal(state["sessions"], &sessions); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(state["chat_polls"], &polls); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < catchUpChats; i++ {
			label, chat := fmt.Sprintf("s%03d", 100+i), fmt.Sprintf("19:chat-l%03d@thread.v2", i)
			sessions[label] = mustJSON(t, map[string]any{
				"id": label, "status": "active", "teams_chat_id": chat,
				"codex_thread_id":           fmt.Sprintf("11111111-1111-4111-8111-%012d", 1000+i),
				"cwd":                       "/work/l",
				"poll_frontier_initialized": true,
				"created_at":                "2026-08-01T00:00:00Z", "updated_at": stopped,
			})
			polls[chat] = mustJSON(t, map[string]any{
				"chat_id": chat, "seeded": true, "last_modified_cursor": stopped, "last_successful_poll_at": stopped,
			})
		}
		state["sessions"], state["chat_polls"] = mustJSON(t, sessions), mustJSON(t, polls)
	})
	const control = "19:control@thread.v2"
	var synthetic []string
	for id := range ids {
		if strings.HasPrefix(id, "19:chat-l") {
			synthetic = append(synthetic, id)
		}
	}
	s.Check("the old store holds the synthetic chats", len(synthetic) == catchUpChats, fmt.Sprint(catchUpChats), fmt.Sprint(len(synthetic)))

	// Local sessions written while no helper ran: caught up silently.
	var tokens []string
	for i := 0; i < catchUpSessions; i++ {
		start := time.Now().Add(-time.Duration(i*6+3) * time.Hour)
		r := harness.NewRolloutAt(t, s.Env, filepath.Join(s.Env.WorkRoot, fmt.Sprintf("backlog%02d", i%4)), start)
		for k := 0; k < 3; k++ {
			p, a := fmt.Sprintf("backlog-p-%02d-%d", i, k), fmt.Sprintf("backlog-a-%02d-%d", i, k)
			r.AppendTurn(t, "Backlog prompt "+p, "", "Backlog answer "+a)
			tokens = append(tokens, p, a)
		}
		if err := r.SetModTime(start.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	s.StartSUT()
	s.Require("the helper runs on the converted store", 60*time.Second, func() (bool, string) {
		return strings.Contains(s.SUT.Output(), runningLine), s.OutputTail(300)
	})
	s.Require("the first control-chat poll", 30*time.Second, func() (bool, string) {
		return len(chatReads(s, control, 0)) > 0, strings.Join(s.RequestTail(5), "\n")
	})
	s.Require("the catch-up posts", 60*time.Second, func() (bool, string) {
		n := catchUpPosts(s, tokens)
		return n >= 8, fmt.Sprintf("%d catch-up posts", n)
	})
	unread := func() int {
		n := 0
		for _, id := range synthetic {
			if len(chatReads(s, id, 0)) == 0 {
				n++
			}
		}
		return n
	}

	// 1. The owner opens a work chat from the control chat.
	before := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		before[c.ID] = true
	}
	dir := s.Env.WorkDir(t, "live")
	pending := unread()
	s.Note("owner posts `new` with %d of %d synthetic chats not read yet and %d catch-up posts made", pending, catchUpChats, catchUpPosts(s, tokens))
	s.Check("the owner's first message comes while the start-up reads are queued", pending >= catchUpChats/2,
		fmt.Sprintf(">= %d synthetic chats not read yet", catchUpChats/2), fmt.Sprint(pending))
	newRead := ownerPost(s, control, "new "+dir)
	var work fakegraph.ChatInfo
	s.Require("the work chat is created", 60*time.Second, func() (bool, string) {
		for _, c := range s.Graph.Chats() {
			if !before[c.ID] && steps.CountContaining(s.SUTMessages(c.ID), "Work chat is ready") > 0 {
				work = c
				return true, ""
			}
		}
		return false, strings.Join(s.Transcript(control, 4), " | ")
	})
	s.Check("the control chat is read within its poll interval after the owner's `new`", newRead.wait(s, controlPoll+readSlack),
		"<= "+(controlPoll+readSlack).String(), newRead.String())

	// 2. A prompt in the new chat: no running turn and no owner message
	// there yet, so the activity scan finds it.
	tok1 := s.Token("first")
	p1 := ownerPost(s, work.ID, "Live task "+tok1)
	_, ok1 := steps.WaitAnswered(s, work.ID, tok1, 60*time.Second)
	s.Check("the prompt in the new chat is read within one activity scan", p1.wait(s, scanPoll+readSlack),
		"<= "+(scanPoll+readSlack).String(), p1.String())
	s.Check("the prompt in the new chat is answered", ok1, "answered", fmt.Sprint(ok1), s.Transcript(work.ID, 8)...)

	// 3. Follow-ups in the now active chat (recent owner activity): each
	// is read by its 1 s poll, not left to the next activity scan.
	for i := 1; i <= 3; i++ {
		tok := s.Token(fmt.Sprintf("followup%d", i))
		pending = unread()
		s.Note("owner posts follow-up %d with %d synthetic chats not read yet", i, pending)
		p := ownerPost(s, work.ID, fmt.Sprintf("Follow-up %d %s", i, tok))
		_, ok := steps.WaitAnswered(s, work.ID, tok, 60*time.Second)
		s.Check(fmt.Sprintf("follow-up %d in the active chat is read within its hot poll interval", i), p.wait(s, hotPoll+readSlack),
			"<= "+(hotPoll+readSlack).String(), p.String())
		s.Check(fmt.Sprintf("follow-up %d is answered", i), ok, "answered", fmt.Sprint(ok), s.Transcript(work.ID, 12)...)
	}

	// The catch-up completes: every synthetic chat read, every record
	// posted once.
	s.Require("every synthetic chat is read once at the start", 5*time.Minute, func() (bool, string) {
		n := unread()
		return n == 0, fmt.Sprintf("%d not read yet", n)
	})
	s.Require("the catch-up posts every record", 3*time.Minute, func() (bool, string) {
		n := catchUpPosts(s, tokens)
		return n >= len(tokens), fmt.Sprintf("%d of %d", n, len(tokens))
	})
	end := time.Now()
	gap, at := maxReadGap(s, control, end)
	s.Note("longest gap between control-chat reads until the catch-up's reads ended: %s (at %s)", gap.Round(10*time.Millisecond), at)
	s.Check("the control chat is read every poll interval during the catch-up", gap <= controlPoll+readSlack,
		"<= "+(controlPoll+readSlack).String(), gap.Round(10*time.Millisecond).String())
	s.Check("each catch-up record is posted once", catchUpPosts(s, tokens) == len(tokens), fmt.Sprint(len(tokens)), fmt.Sprint(catchUpPosts(s, tokens)))
	s.Settle(5*time.Second, 60*time.Second)
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// chatReads are the helper's successful reads of chat's messages that
// arrived after request seq since.
func chatReads(s *harness.Scenario, chat string, since int64) []fakegraph.Request {
	var out []fakegraph.Request
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Route: "GET /chats/{chat}/messages", ChatID: chat, Since: since, OnlyOK: true}) {
		if r.Instance != "cxp-cli" {
			out = append(out, r)
		}
	}
	return out
}

// catchUpPosts counts the helper's messages carrying a catch-up token.
func catchUpPosts(s *harness.Scenario, tokens []string) int {
	n := 0
	for _, c := range s.Graph.Chats() {
		for _, m := range s.SUTMessages(c.ID) {
			if harness.NewChatsSummary(m) {
				continue // names the chats (their titles), posts no record
			}
			text := m.PlainText()
			for _, tok := range tokens {
				if strings.Contains(text, tok) {
					n++
					break
				}
			}
		}
	}
	return n
}

// postRead is an owner message and the helper's first read of its chat
// after it.
type postRead struct {
	chat  string
	seq   int64
	at    time.Time
	read  time.Time
	label string
}

// ownerPost posts text into chat as the owner.
func ownerPost(s *harness.Scenario, chat, text string) *postRead {
	s.T.Helper()
	seq := s.Graph.LastSeq()
	at := time.Now()
	steps.MustPost(s, chat, text)
	return &postRead{chat: chat, seq: seq, at: at, label: text}
}

// wait waits up to twice bound for the first read of the chat that
// arrived after the post and reports whether it came within bound.
func (p *postRead) wait(s *harness.Scenario, bound time.Duration) bool {
	if p.read.IsZero() {
		s.Poll(max(time.Until(p.at.Add(2*bound)), 100*time.Millisecond), func() bool {
			if r := chatReads(s, p.chat, p.seq); len(r) > 0 {
				p.read = r[0].At
				return true
			}
			return false
		})
	}
	return !p.read.IsZero() && p.read.Sub(p.at) <= bound
}

func (p *postRead) String() string {
	if p.read.IsZero() {
		return "not read"
	}
	return p.read.Sub(p.at).Round(10 * time.Millisecond).String()
}

// maxReadGap is the longest time between two successive reads of chat,
// from its first read until end, and when it ended (from the first read).
func maxReadGap(s *harness.Scenario, chat string, end time.Time) (time.Duration, string) {
	reads := chatReads(s, chat, 0)
	if len(reads) == 0 {
		return 0, "no read"
	}
	var gap time.Duration
	var at time.Time
	prev := reads[0].At
	for _, r := range append(reads[1:], fakegraph.Request{At: end}) {
		if r.At.After(end) {
			r.At = end
		}
		if d := r.At.Sub(prev); d > gap {
			gap, at = d, r.At
		}
		prev = r.At
		if !prev.Before(end) {
			break
		}
	}
	return gap, at.Sub(reads[0].At).Round(time.Second).String()
}
