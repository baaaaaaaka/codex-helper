package entry

import (
	"encoding/json"
	"fmt"
	"html"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

// ENTRY-RESUMECHECK (live canary 2026-11-05): s001's rollout has an old
// checkpoint but no delivery proof (the old store's echo inbound row has no
// text), so the converted catch-up starts at byte 0. The chat already has
// the records before the checkpoint, posted as v0.1.20 posts them (one
// answer split in two parts). The first start reads the chat's history
// before s001's catch-up and resumes after the newest record found: only
// the turns written after the checkpoint are posted, each once.
func TestEntryResumeCheckedAgainstHistory(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-RESUMECHECK", harness.Options{
		Guarantees: []string{"auto-migrate", "catch-up-silent", "F-MIR-4"},
		Title:      "first start on an rc.59 store with a no-proof checkpoint: the chat's history decides where the catch-up resumes",
		Budget:     120 * time.Second,
		NoStart:    true,
	})
	defer s.Finish()
	start := time.Now().Add(-3 * time.Hour).Truncate(time.Second)
	r := harness.NewRolloutAt(t, s.Env, s.Env.WorkDir(t, "s001"), start)
	type turn struct{ prompt, answer string }
	before := []turn{
		{"resume-p1 first local prompt", "resume-a1 first local answer"},
		{"resume-p2 second local prompt", "resume-a2 " + strings.Repeat("a long answer the old helper split into two parts ", 8)},
		{"resume-p3 third local prompt", "resume-a3 third local answer, the last one before the checkpoint"},
	}
	after := []turn{
		{"resume-p4 written after the checkpoint", "resume-a4 answer written after the checkpoint"},
		{"resume-p5 another prompt after the checkpoint", "resume-a5 another answer after the checkpoint"},
	}
	for _, tn := range before {
		r.AppendTurn(t, tn.prompt, "", tn.answer)
	}
	raw, err := os.ReadFile(r.Path)
	if err != nil {
		t.Fatal(err)
	}
	ckpt, ckptLine := len(raw), strings.Count(string(raw), "\n")
	for _, tn := range after {
		r.AppendTurn(t, tn.prompt, "", tn.answer)
	}
	if err := r.SetModTime(start.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	cursor := time.Now().Add(-20 * time.Minute).UTC().Format(time.RFC3339)
	rc59Store(s, func(state map[string]json.RawMessage) {
		edit := func(key string, f func(m map[string]map[string]any)) {
			var m map[string]map[string]any
			if err := json.Unmarshal(state[key], &m); err != nil {
				t.Fatal(err)
			}
			f(m)
			state[key] = mustJSON(t, m)
		}
		edit("sessions", func(m map[string]map[string]any) { m["s001"]["codex_thread_id"] = r.SessionID })
		edit("import_checkpoints", func(m map[string]map[string]any) {
			cp := m["transcript:s001"]
			cp["source_path"], cp["last_offset"], cp["last_source_line"], cp["source_size"] = r.Path, ckpt, ckptLine, ckpt
			cp["last_record_id"], cp["source_mod_time"] = "", start.Add(time.Hour).UTC().Format(time.RFC3339)
		})
		// The unconfirmed answer o2 (ENTRY-OLDANSWER) is sent here: its
		// history check would race with the catch-up in s001's posts.
		edit("outbox_messages", func(m map[string]map[string]any) {
			m["o2"]["status"], m["o2"]["teams_message_id"], m["o2"]["sent_at"] = "sent", "1788256821000", "2026-09-01T10:00:21Z"
		})
		// The old helper read s001 after its posts: they are not prompts.
		edit("chat_polls", func(m map[string]map[string]any) { m[s001]["last_modified_cursor"] = cursor })
		// The old helper's echo of its own posts carries no text: no
		// delivery proof.
		edit("inbound_events", func(m map[string]map[string]any) {
			for _, in := range m {
				if in["teams_chat_id"] == s001 && in["status"] == "ignored" {
					in["text"] = ""
				}
				// Other chats' queued prompts would get queue reminders
				// at a time that depends on the scenario's length.
				if in["teams_chat_id"] != s001 && (in["status"] == "queued" || in["status"] == "deferred") {
					in["status"] = "persisted"
				}
			}
		})
		edit("turns", func(m map[string]map[string]any) {
			for _, tr := range m {
				if tr["session_id"] != "s001" && tr["status"] == "queued" {
					tr["status"], tr["completed_at"] = "completed", tr["queued_at"]
				}
			}
		})
	})
	// The old helper's posts of the turns before the checkpoint.
	at := start.Add(time.Hour)
	seed := func(body string) {
		at = at.Add(time.Second)
		if err := s.Graph.SeedMessage(s001, fmt.Sprint(at.UnixMilli()), at, body); err != nil {
			t.Fatal(err)
		}
	}
	const footer = `<p><strong>🔧 Helper:</strong> ✅ Codex finished responding.</p>`
	for i, tn := range before {
		seed(`<p><strong>🧑‍💻 User:</strong></p><p>` + html.EscapeString(tn.prompt) + `</p>`)
		if i == 1 {
			half := len(tn.answer) / 2
			seed(`<p><strong>🤖 ✅ Codex answer [part 1/2]:</strong></p><p>` + html.EscapeString(tn.answer[:half]) + `</p>`)
			seed(`<p><strong>🤖 ✅ Codex answer [part 2/2]:</strong></p><p>` + html.EscapeString(tn.answer[half:]) + `</p>` + footer)
			continue
		}
		seed(`<p><strong>🤖 ✅ Codex answer:</strong></p><p>` + html.EscapeString(tn.answer) + `</p>` + footer)
	}

	posts := func(tok string) int {
		n := 0
		for _, m := range s.SUTMessages(s001) {
			if strings.Contains(m.PlainText(), tok) {
				n++
			}
		}
		return n
	}
	s.StartSUT()
	s.WaitFor("the catch-up of the turns after the checkpoint", 60*time.Second, func() (bool, string) {
		return posts("resume-a5") > 0, s.OutputTail(400)
	})
	time.Sleep(2 * time.Second)
	out := s.SUT.Output()
	s.Check("the chat's history is read once before s001's catch-up", strings.Count(out, "resume history check of ") == 1 &&
		strings.Contains(out, "the records before the old position are in the chat: catch-up from there"), "1 decision: resume at the checkpoint", s.OutputTail(600))
	for _, tok := range []string{"resume-p1", "resume-a1", "resume-p2", "resume-a2", "resume-p3", "resume-a3"} {
		s.Check("not posted again: "+tok, posts(tok) == 0, "0", fmt.Sprint(posts(tok)), s.Transcript(s001, 12)...)
	}
	for _, tok := range []string{"resume-p4", "resume-a4", "resume-p5", "resume-a5"} {
		s.Check("caught up once: "+tok, posts(tok) == 1, "1", fmt.Sprint(posts(tok)), s.Transcript(s001, 12)...)
	}
}
