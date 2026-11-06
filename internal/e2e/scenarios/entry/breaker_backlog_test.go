package entry

import (
	"encoding/json"
	"fmt"
	"html"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// BREAKER-BACKLOG (finding 8 of the cluster drills, drill 8): the old
// helper (rc.59) served two work chats until it stopped. Its last posts in
// each chat and in the control chat came after its last read, each with its
// hidden marker, and the owner kept sending prompts to both work chats while
// no helper ran (the drill's 7.5 min conversion wait). The first start
// converts the store and re-reads every chat from 2 minutes before its
// cursor: that first read holds the old helper's posts and the queued
// prompts. Before the fix the old posts counted as "another helper
// instance" (the marker self-test had not run yet) and the self-echo
// breaker paused all three chats, so no queued prompt was ever answered.
//
// Now no chat is paused: every queued prompt is answered once, in order.
// A live self-echo loop afterwards (three marker-less copies of the
// helper's posts) still trips the breaker, in that chat only, with one
// notice there and one in the control chat; the other chat keeps working.
func TestBreakerIgnoresBacklogAfterGap(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "BREAKER-BACKLOG", harness.Options{
		Guarantees: []string{"F-WRK-14", "auto-migrate", "R-1", "R-7", "R-11"},
		Title:      "first start after a gap: the old helper's last posts and the prompts queued meanwhile pause no chat; every prompt answered once; a live loop still trips",
		Budget:     150 * time.Second,
		NoStart:    true,
		Golden: harness.GoldenOptions{FinishedNotices: "the two work chats answer their queued prompts concurrently",
			Masks: []harness.Mask{{Pattern: regexp.MustCompile(`(core-files/outbound/cxp-[a-z0-9]+-)[0-9]+`), Replace: "${1}<inbound id>",
				Reason: "the two chats' first reads are admitted in either order, so their prompts' inbound ids interleave either way"}}},
	})
	defer s.Finish()
	const control = "19:control@thread.v2"
	chats := []string{"19:chat-b1@thread.v2", "19:chat-b2@thread.v2"}
	labels := []string{"s201", "s202"}
	// The old helper stopped 10 minutes ago; its last read of each chat
	// was a minute before that.
	stopped := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	cursor := stopped.Add(-time.Minute)
	rfc := func(at time.Time) string { return at.UTC().Format(time.RFC3339) }

	type oldPost struct {
		chat, session, id, oid, kind, html string
		at                                 time.Time
	}
	var posts []oldPost
	// Its last three posts per chat, 50 to 44 s before it stopped, with the
	// marker of its own outbox id; its store recorded their message ids.
	for i, chat := range append(append([]string{}, chats...), control) {
		at := stopped.Add(-50*time.Second + time.Duration(i)*100*time.Millisecond)
		session := ""
		if i < len(labels) {
			session = labels[i]
		}
		bodies := []struct{ kind, text string }{
			{"ack", `<p><strong>🔧 Helper:</strong> ⏳ Codex is working. Request accepted.</p>`},
			{"final", `<p><strong>🤖 ✅ Codex answer:</strong></p><p>the old helper's last answer</p><p><strong>🔧 Helper:</strong> ✅ Codex finished responding.</p>`},
			{"helper-status", `<p><strong>🔧 Helper:</strong> ▶️ Codex is starting this queued request.</p>`},
		}
		if chat == control {
			bodies = []struct{ kind, text string }{
				{"helper-status", `<p><strong>🔧 Helper:</strong> ✅ Codex finished. Chat: b1</p>`},
				{"helper-status", `<p><strong>🔧 Helper:</strong> ✅ Codex finished. Chat: b2</p>`},
				{"helper-status", `<p><strong>🔧 Helper:</strong> ✅ Codex finished. Chat: b1</p>`},
			}
		}
		for k, b := range bodies {
			at := at.Add(time.Duration(k) * 3 * time.Second)
			oid := fmt.Sprintf("outbox:old-%d-%d", i, k)
			posts = append(posts, oldPost{chat: chat, session: session, id: fmt.Sprint(at.UnixMilli()), oid: oid, kind: b.kind, at: at,
				html: b.text + "<!-- codex-helper-outbox:" + oid + " -->"})
		}
	}

	var dirs []string
	threads := map[string]string{}
	for i, label := range labels {
		dir := s.Env.WorkDir(t, "backlog-"+label)
		dirs = append(dirs, dir)
		// Distinct starts: a rollout's session id derives from its start.
		created := stopped.Add(-time.Hour + time.Duration(i)*time.Minute)
		r := harness.NewRolloutAt(t, s.Env, dir, created)
		// The fake app-server resumes only the threads in its own store
		// (real Codex resumes any rollout under $CODEX_HOME/sessions).
		st := fakecodex.Store{Dir: filepath.Join(s.Env.CodexHome, "e2e-fakecodex")}
		if err := st.Save(&fakecodex.Thread{ID: r.SessionID, Cwd: dir, CreatedAt: created.Unix(), UpdatedAt: created.Unix(),
			RolloutPath: r.Path, Turns: []*fakecodex.Turn{}}); err != nil {
			t.Fatal(err)
		}
		threads[labels[i]] = r.SessionID
	}
	// The old helper's control chat, named as v0.1.20 names it (the
	// golden lists its "Codex finished" notices sorted).
	s.Graph.SeedChat(control, "🏠 Codex Control - e2ehost")
	rc59Store(s, func(state map[string]json.RawMessage) {
		edit := func(key string, f func(m map[string]map[string]any)) {
			var m map[string]map[string]any
			if err := json.Unmarshal(state[key], &m); err != nil {
				t.Fatal(err)
			}
			f(m)
			state[key] = mustJSON(t, m)
		}
		edit("sessions", func(m map[string]map[string]any) {
			for i, label := range labels {
				m[label] = map[string]any{
					"id": label, "status": "active", "teams_chat_id": chats[i], "codex_thread_id": threads[label], "cwd": dirs[i],
					"poll_frontier_initialized": true, "created_at": rfc(stopped.Add(-time.Hour)), "updated_at": rfc(cursor),
				}
			}
		})
		edit("chat_polls", func(m map[string]map[string]any) {
			for _, chat := range chats {
				m[chat] = map[string]any{"chat_id": chat, "seeded": true, "last_modified_cursor": rfc(cursor), "last_successful_poll_at": rfc(cursor)}
			}
			m[control]["last_modified_cursor"] = rfc(cursor)
		})
		edit("outbox_messages", func(m map[string]map[string]any) {
			for k, p := range posts {
				m[p.oid] = map[string]any{
					"id": p.oid, "session_id": p.session, "teams_chat_id": p.chat, "kind": p.kind, "body": "old post",
					"sequence": 100 + k, "part_index": 1, "part_count": 1, "status": "sent", "teams_message_id": p.id,
					"created_at": rfc(p.at), "sent_at": rfc(p.at),
				}
			}
			// The fixture's unconfirmed answer (ENTRY-OLDANSWER) is sent:
			// its history check is not part of this scenario.
			m["o2"]["status"], m["o2"]["teams_message_id"], m["o2"]["sent_at"] = "sent", "1788256821000", "2026-09-01T10:00:21Z"
		})
		// The fixture's queued prompts would run or get queue reminders that
		// are not part of this scenario.
		edit("inbound_events", func(m map[string]map[string]any) {
			for _, in := range m {
				if in["status"] == "queued" || in["status"] == "deferred" {
					in["status"] = "persisted"
				}
			}
		})
		edit("turns", func(m map[string]map[string]any) {
			for _, tr := range m {
				if tr["status"] == "queued" {
					tr["status"], tr["completed_at"] = "completed", tr["queued_at"]
				}
			}
		})
	})
	for _, p := range posts {
		if err := s.Graph.SeedMessage(p.chat, p.id, p.at, p.html); err != nil {
			t.Fatal(err)
		}
	}
	// The owner's prompts while no helper ran: four per work chat, a
	// minute apart, the chats alternating.
	toks := make([][]string, len(chats))
	for k := 0; k < 4; k++ {
		for i, chat := range chats {
			tok := s.Token(fmt.Sprintf("q%d%d", i+1, k+1))
			toks[i] = append(toks[i], tok)
			at := stopped.Add(time.Duration(2*k+i+1) * 30 * time.Second)
			if err := s.Graph.SeedMessage(chat, fmt.Sprint(at.UnixMilli()), at, "<p>"+html.EscapeString(fmt.Sprintf("Queued while down %d %s", k+1, tok))+"</p>"); err != nil {
				t.Fatal(err)
			}
		}
	}

	s.StartSUT()
	s.Require("the helper runs on the converted store", 60*time.Second, func() (bool, string) {
		return strings.Contains(s.SUT.Output(), runningLine), s.OutputTail(300)
	})
	answered := func(chat, tok string) int {
		n := 0
		for _, c := range s.Codex.CallsWithToken(tok) {
			if c.Answer != "" {
				n += len(s.SUTMessagesContaining(chat, c.Answer))
			}
		}
		return n
	}
	allAnswered := s.Poll(90*time.Second, func() bool {
		for i, chat := range chats {
			for _, tok := range toks[i] {
				if answered(chat, tok) == 0 {
					return false
				}
			}
		}
		return true
	})
	s.Settle(3*time.Second, 15*time.Second)
	var tr []string
	for _, chat := range append(append([]string{}, chats...), control) {
		tr = append(tr, s.Transcript(chat, 20)...)
	}
	backlogNotices := 0
	for _, chat := range append(append([]string{}, chats...), control) {
		backlogNotices += len(pauseNotices(s, chat))
	}
	s.Check("the first read after the gap pauses no chat", backlogNotices == 0, "0 breaker notices",
		fmt.Sprintf("%d: %q", backlogNotices, append(pauseNotices(s, chats[0]), append(pauseNotices(s, chats[1]), pauseNotices(s, control)...)...)), append(tr, s.OutputTail(600))...)
	s.Check("every prompt queued while no helper ran is answered", allAnswered, "8 answers", fmt.Sprint(allAnswered), tr...)
	for i, chat := range chats {
		var bad []string
		var last time.Time
		ordered := true
		for _, tok := range toks[i] {
			calls := s.Codex.CallsWithToken(tok)
			if n := answered(chat, tok); len(calls) != 1 || n != 1 {
				bad = append(bad, fmt.Sprintf("%s: %d turns, %d answers", tok, len(calls), n))
			}
			if len(calls) > 0 {
				if calls[0].Started.Before(last) {
					ordered = false
				}
				last = calls[0].Started
			}
		}
		s.Check(fmt.Sprintf("%s: each queued prompt runs once and is answered once", labels[i]), len(bad) == 0, "1 turn and 1 answer each", fmt.Sprint(bad), s.Transcript(chat, 20)...)
		s.Check(fmt.Sprintf("%s: the queued prompts run in the order they were sent", labels[i]), ordered, "in order", fmt.Sprint(ordered))
	}
	stray := 0
	for _, c := range s.Codex.Calls() {
		if len(c.Tokens) == 0 {
			stray++
		}
	}
	s.Check("the old helper's posts start no turn", stray == 0, "0 turns without a prompt token", fmt.Sprint(stray))

	// A live self-echo loop in b1: three marker-less copies of the helper's
	// last posts there, as from a client that strips the marker.
	marker := regexp.MustCompile(`<!--.*?-->`)
	var ours []fakegraph.Message
	sut := s.SUTMessages(chats[0])
	for i := len(sut) - 1; i >= 0 && len(ours) < 3; i-- {
		ours = append(ours, sut[i])
	}
	for i := len(ours) - 1; i >= 0; i-- {
		if _, err := s.Graph.PostAsOtherHelper(chats[0], marker.ReplaceAllString(ours[i].Content, "")); err != nil {
			t.Fatal(err)
		}
	}
	tripped := s.Poll(30*time.Second, func() bool { return len(pauseNotices(s, chats[0])) > 0 && len(pauseNotices(s, control)) > 0 })
	s.Settle(2*time.Second, 8*time.Second)
	held, other := s.Token("held"), s.Token("other")
	steps.MustPost(s, chats[0], "Prompt while paused "+held)
	steps.MustPost(s, chats[1], "Prompt in the other chat "+other)
	otherAnswered := s.Poll(30*time.Second, func() bool { return answered(chats[1], other) == 1 })
	s.Settle(2*time.Second, 8*time.Second)
	nb1, nb2, nc := len(pauseNotices(s, chats[0])), len(pauseNotices(s, chats[1])), len(pauseNotices(s, control))
	s.Check("a live self-echo loop still trips the breaker", tripped, "a breaker notice", fmt.Sprint(tripped), s.Transcript(chats[0], 12)...)
	s.Check("the loop pauses that chat only, with one notice there and one in the control chat", nb1 == 1 && nb2 == 0 && nc == 1,
		"b1=1 b2=0 control=1", fmt.Sprintf("b1=%d b2=%d control=%d", nb1, nb2, nc), s.Transcript(control, 12)...)
	s.Check("while paused a new prompt is held, not run", len(s.Codex.CallsWithToken(held)) == 0, "0 turns", fmt.Sprint(len(s.Codex.CallsWithToken(held))))
	s.Check("the other chat keeps working", otherAnswered, "answered", fmt.Sprint(otherAnswered), s.Transcript(chats[1], 8)...)
}

// pauseNotices returns the breaker notices the SUT posted in chatID.
func pauseNotices(s *harness.Scenario, chatID string) []string {
	var out []string
	for _, m := range s.SUTMessages(chatID) {
		if strings.Contains(m.PlainText(), "Paused auto-start") {
			out = append(out, clip(harness.NormalizeSpace(m.PlainText()), 140))
		}
	}
	return out
}
