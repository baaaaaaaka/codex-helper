package engine

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

func TestScheduledPresentationLeavesOrdinaryMessagesUnchanged(t *testing.T) {
	for _, doc := range []channel.RichDoc{answerDoc("中文回答"), statusDoc("progress"), userDoc("question")} {
		got := scheduledDoc(doc, transcript.Output{})
		if got.Label != doc.Label || got.Markdown != doc.Markdown || got.Footer != doc.Footer || got.FooterLabel != doc.FooterLabel {
			t.Fatalf("ordinary doc changed: %+v", got)
		}
	}
	s := &transcript.Schedule{ID: "watch-pr_123", Instructions: "检查 PR"}
	o := transcript.Output{Schedule: s, Kind: transcript.OutputFinal, Text: "中文回答"}
	doc := scheduledDoc(answerDoc(o.Text), o)
	if doc.Label != "🕒 Scheduled task result (watch-pr_123)" || doc.Markdown != "中文回答" || doc.Footer != "" || doc.FooterLabel != "" {
		t.Fatalf("scheduled doc %+v", doc)
	}
	o.User, o.Kind, s.Input = true, transcript.OutputPrompt, true
	doc = scheduledDoc(userDoc("<heartbeat>internal</heartbeat>"), o)
	if doc.Label != "🕒 Scheduled task instructions (watch-pr_123)" || doc.Markdown != "检查 PR" {
		t.Fatalf("instructions %+v", doc)
	}
	s.Instructions = ""
	if empty := scheduledDoc(userDoc("<heartbeat>internal</heartbeat>"), o); empty.Markdown != "_No instructions were included in this scheduled input._" {
		t.Fatalf("empty instructions exposed scheduler wrapper: %+v", empty)
	}
	s.Input = false
	s.Mixed = true
	doc = scheduledDoc(userDoc("先别修改"), o)
	if !strings.Contains(doc.Label, "Additional input") || doc.Markdown != "先别修改" {
		t.Fatalf("human input rewritten %+v", doc)
	}
	o.User, o.Kind = false, transcript.OutputStatus
	doc = scheduledDoc(statusDoc("正在检查"), o)
	if !strings.Contains(doc.Label, "Scheduled task status with additional input") || doc.Markdown != "正在检查" {
		t.Fatalf("status mislabeled as a result %+v", doc)
	}
}

func TestScheduledNativeWaitSuppressesHintsAndClearsOnCompletion(t *testing.T) {
	var h TurnHooks
	ready := make(chan struct{})
	r := newRig(t, func(c *Config) {
		c.Timing.SilenceHint = 100 * time.Millisecond
		c.Timing.SilenceRepeat = 100 * time.Millisecond
		c.Timing.SilenceStuck = 200 * time.Millisecond
		a := c.Agent.(*fakeAgent)
		a.block["wait explicitly"] = make(chan struct{})
		a.block["parallel ordinary silence"] = make(chan struct{})
		a.during = func(_ TurnRequest, hooks TurnHooks) {
			h = hooks
			h.Waiting("sleep-1", time.Hour, true)
			h.Waiting("sleep-1", time.Hour, true)
			close(ready)
		}
	})
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "wait explicitly")
	<-ready
	r.wait("wait notice", func() bool { return len(r.own(work, "in-session wait")) == 1 })
	// The same engine must preserve ordinary behavior in another conversation.
	r.ag.mu.Lock()
	r.ag.during = nil
	r.ag.mu.Unlock()
	other := r.newWork(t.TempDir())
	r.ch.PostUser(other, "parallel ordinary silence")
	r.wait("parallel ordinary hint", func() bool { return len(r.own(other, "Still working")) >= 1 })
	r.ch.PostUser(other, "helper cancel running")
	r.wait("parallel ordinary cancel", func() bool { return len(r.own(other, "Codex request canceled.")) == 1 })
	// Cross both ordinary thresholds while the native wait is still valid.
	time.Sleep(400 * time.Millisecond)
	if len(r.own(work, "Still working"))+len(r.own(work, "may be stuck")) != 0 {
		t.Fatal("valid wait produced ordinary silence hints")
	}
	h.Waiting("unrelated", time.Hour, false)
	time.Sleep(150 * time.Millisecond)
	if len(r.own(work, "This run has not reported a result")) != 0 {
		t.Fatal("unrelated completion ended wait")
	}
	h.Waiting("sleep-1", time.Hour, false)
	// A repeated completed/start pair must not resurrect the same wait.
	h.Waiting("sleep-1", time.Hour, false)
	h.Waiting("sleep-1", time.Hour, true)
	r.wait("one post-wait hint", func() bool { return len(r.own(work, "This run has not reported a result")) == 1 })
	time.Sleep(350 * time.Millisecond)
	if len(r.own(work, "This run has not reported a result")) != 1 || len(r.own(work, "Still working")) != 0 {
		t.Fatal("post-wait silence repeated")
	}
	h.Waiting("sleep-2", time.Hour, true)
	h.Waiting("sleep-2", time.Hour, false)
	if len(r.own(work, "entered an in-session wait")) != 1 {
		t.Fatal("multiple native sleeps produced repeated wait notices")
	}
	r.ch.PostUser(work, "helper cancel running")
	r.wait("cancel", func() bool { return len(r.own(work, "Codex request canceled.")) == 1 })
	// New ordinary turns do not inherit the quieter policy.
	r.ag.mu.Lock()
	r.ag.during = nil
	r.ag.block["ordinary silence"] = make(chan struct{})
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "ordinary silence")
	r.wait("ordinary silence behavior", func() bool { return len(r.own(work, "Still working")) >= 1 })
	r.ch.PostUser(work, "helper cancel running")
	r.wait("second cancel", func() bool { return len(r.own(work, "Codex request canceled.")) == 2 })
}

func TestScheduledWaitResultAndNotificationKeepOrdinaryTurnsUnchanged(t *testing.T) {
	for _, failCard := range []bool{false, true} {
		t.Run(map[bool]string{false: "workflow", true: "mention-fallback"}[failCard], func(t *testing.T) {
			n := &noticeRecorder{}
			if failCard {
				n.err = errors.New("ambiguous Workflow response")
			}
			r := newRig(t, func(c *Config) {
				c.Notifier = n
				c.Agent.(*fakeAgent).during = func(_ TurnRequest, h TurnHooks) {
					h.Waiting("sleep-result", time.Second, true)
					h.Waiting("sleep-result", time.Second, false)
				}
			})
			work := r.newWork(t.TempDir())
			r.ch.PostUser(work, "等一下再回答")
			r.wait("answer after wait", func() bool { return len(r.own(work, "Codex answer after an in-session wait")) == 1 })
			r.wait("wait result card", func() bool { return n.count("🕒 In-session wait result") == 1 })
			if failCard {
				r.wait("wait result fallback", func() bool { return len(r.own(r.control(), "In-session wait result")) == 1 })
			}
			n.mu.Lock()
			for _, v := range n.notices {
				if v.Heading == "🕒 In-session wait result" && !strings.Contains(v.Hint, "does not confirm a saved schedule") {
					t.Errorf("misleading hint %+v", v)
				}
			}
			n.mu.Unlock()
			r.ag.mu.Lock()
			r.ag.during = nil
			r.ag.mu.Unlock()
			r.ch.PostUser(work, "普通问题")
			r.wait("ordinary final", func() bool { return len(r.own(work, "ANSWER to 普通问题")) == 1 })
			ordinary := r.own(work, "ANSWER to 普通问题")[0]
			if strings.Contains(ordinary.Text, "in-session wait") || !strings.Contains(ordinary.Text, "Codex finished responding") {
				t.Fatalf("ordinary answer changed %q", ordinary.Text)
			}
			r.wait("ordinary notification", func() bool { return n.count("✅ Codex finished") == 1 })
			r.stop()
			r.start()
			r.control()
			if n.count("🕒 In-session wait result") != 1 {
				t.Fatal("replayed wait notification")
			}
		})
	}
}

func TestScheduledNativeWaitExpiryRestoresAttention(t *testing.T) {
	r := newRig(t, func(c *Config) {
		c.Timing.SilenceHint = 100 * time.Millisecond
		c.Timing.SilenceRepeat = 100 * time.Millisecond
		a := c.Agent.(*fakeAgent)
		a.block["expire wait"] = make(chan struct{})
		a.during = func(_ TurnRequest, h TurnHooks) { h.Waiting("sleep-expire", 100*time.Millisecond, true) }
	})
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "expire wait")
	r.wait("expired wait hint", func() bool { return len(r.own(work, "This run has not reported a result")) == 1 })
	if len(r.own(work, "Still working"))+len(r.own(work, "may be stuck")) != 0 {
		t.Fatal("expired native wait used ordinary repeated hints")
	}
	r.ch.PostUser(work, "helper cancel running")
	r.wait("cancel", func() bool { return len(r.own(work, "Codex request canceled.")) == 1 })
}

func TestScheduledNativeWaitRejectsInvalidEvidence(t *testing.T) {
	e := &Engine{}
	rs := &runState{}
	for _, tc := range []struct {
		id       string
		duration time.Duration
	}{{"", time.Hour}, {"x", 0}, {"x", -1}, {"x", 13 * time.Hour}} {
		e.nativeWait(1, 1, rs, tc.id, tc.duration, true)
	}
	if rs.waitID != "" || !rs.waitUntil.IsZero() {
		t.Fatal("invalid evidence enabled wait policy")
	}
	// A callback from an execution which has already left the engine is stale.
	e.nativeWait(1, 1, rs, "late", time.Minute, true)
	if rs.waitID != "" || !rs.waitUntil.IsZero() {
		t.Fatal("stale callback changed a completed execution")
	}
}

func TestScheduledMirrorNotificationAndRestartIsolation(t *testing.T) {
	for _, mode := range []string{"mention", "workflow", "workflow-fallback"} {
		t.Run(mode, func(t *testing.T) { scheduledMirrorScenario(t, mode) })
	}
}

func scheduledMirrorScenario(t *testing.T, mode string) {
	t.Helper()
	var n *noticeRecorder
	if mode != "mention" {
		n = &noticeRecorder{}
		if mode == "workflow-fallback" {
			n.err = errors.New("ambiguous Workflow response")
		}
	}
	r, sessions := mirrorRig(t, func(c *Config) {
		if n != nil {
			c.Notifier = n
		}
	})
	ctl := r.control()
	r.mirrorInitialised()
	stage := t.TempDir()
	w, err := rollout.New(stage, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.AppendTurn(rollout.Turn{Prompt: "检查 PR", Answer: "自动检查结果"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(w.Path)
	if err != nil {
		t.Fatal(err)
	}
	var rewritten []string
	for _, ln := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var rec map[string]any
		if err = json.Unmarshal([]byte(ln), &rec); err != nil {
			t.Fatal(err)
		}
		p, _ := rec["payload"].(map[string]any)
		if p["type"] == "task_started" {
			trigger := "automation_cron_scheduled"
			if mode == "workflow" {
				trigger = "automation_heartbeat_scheduled"
			}
			p["turn_attribution"] = map[string]any{"turn_id": p["turn_id"], "turn_trigger": trigger}
		}
		if mode == "workflow" && p["role"] == "user" && p["id"] != nil {
			p["content"] = []any{map[string]any{"type": "input_text", "text": "<heartbeat>\n  <automation_id>pr-watch</automation_id>\n  <current_time_iso>2026-11-14T15:30:00+08:00</current_time_iso>\n  <instructions>\n检查 PR\n  </instructions>\n</heartbeat>\n"}}
			p["internal_chat_message_metadata_passthrough"] = map[string]any{"content_item_kinds": []string{"user.heartbeat"}}
		}
		b, _ := json.Marshal(rec)
		rewritten = append(rewritten, string(b))
	}
	rel, err := filepath.Rel(stage, w.Path)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(sessions, rel)
	if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(dest, []byte(strings.Join(rewritten, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	local := r.localChat()
	r.wait("scheduled result", func() bool { return len(r.own(local, "自动检查结果")) == 1 })
	m := r.own(local, "自动检查结果")[0]
	if !strings.HasPrefix(m.Text, "🕒 Scheduled task result") || strings.Contains(m.Text, "finished responding") {
		t.Fatalf("scheduled result %q", m.Text)
	}
	if mode == "workflow" {
		r.wait("heartbeat instructions", func() bool { return len(r.own(local, "Scheduled task instructions (pr-watch)")) == 1 })
		if instructions := r.own(local, "Scheduled task instructions (pr-watch)")[0]; !strings.Contains(instructions.Text, "检查 PR") || strings.Contains(instructions.Text, "<heartbeat>") {
			t.Fatalf("heartbeat instructions rewritten incorrectly %q", instructions.Text)
		}
	}
	if n != nil {
		r.wait("scheduled card", func() bool { return n.count("🕒 Scheduled task result") == 1 })
		n.mu.Lock()
		for _, v := range n.notices {
			if v.Heading == "🕒 Scheduled task result" {
				if v.Hint != scheduledResultHint || mode == "workflow" && !strings.Contains(v.Request, "pr-watch") || mode != "workflow" && v.Request != "" {
					t.Errorf("misleading or redundant scheduled card %+v", v)
				}
			}
		}
		n.mu.Unlock()
	}
	if mode != "workflow" {
		r.wait("scheduled notification", func() bool { return len(r.own(ctl, "Scheduled task")) == 1 })
	}
	r.stop()
	r.start()
	r.control()
	// A later source change also gives recovery a deterministic opportunity to replay.
	f, err := os.OpenFile(dest, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"manual"}}` + "\n" + `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","id":"manual-answer","content":[{"type":"output_text","text":"普通回答"}]}}` + "\n" + `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"manual"}}` + "\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	r.wait("ordinary result after restart", func() bool { return len(r.own(local, "普通回答")) == 1 })
	ordinary := r.own(local, "普通回答")[0]
	if strings.Contains(ordinary.Text, "Scheduled task") || !strings.Contains(ordinary.Text, "Codex finished responding") {
		t.Fatalf("ordinary result changed %q", ordinary.Text)
	}
	wantMention := 1
	if mode == "workflow" {
		wantMention = 0
	}
	if len(r.own(local, "自动检查结果")) != 1 || len(r.own(ctl, "Scheduled task")) != wantMention || n != nil && n.count("🕒 Scheduled task result") != 1 {
		t.Fatal("restart duplicated scheduled output")
	}
}
