package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type questionTestAgent struct {
	*steerAgent
	qm      sync.Mutex
	handler func(QuestionEvent) error
	pending map[string]bool
	replies []map[string][]string
	failure error
}

func newQuestionAgent() *questionTestAgent {
	return &questionTestAgent{steerAgent: &steerAgent{fakeAgent: newAgent()}, pending: map[string]bool{}}
}
func (a *questionTestAgent) SetQuestionHandler(fn func(QuestionEvent) error) {
	a.qm.Lock()
	a.handler = fn
	a.qm.Unlock()
}
func (a *questionTestAgent) QuestionPending(k string) bool {
	a.qm.Lock()
	defer a.qm.Unlock()
	return a.pending[k]
}
func (a *questionTestAgent) AnswerQuestions(_ context.Context, k string, m map[string][]string) error {
	a.qm.Lock()
	defer a.qm.Unlock()
	a.replies = append(a.replies, m)
	delete(a.pending, k)
	return a.failure
}
func (a *questionTestAgent) emit(g store.QuestionGroup) error {
	a.qm.Lock()
	a.pending[g.Key] = true
	fn := a.handler
	a.qm.Unlock()
	return fn(QuestionEvent{Group: g})
}
func (a *questionTestAgent) nreplies() int { a.qm.Lock(); defer a.qm.Unlock(); return len(a.replies) }

func questionRig(t *testing.T) (*rig, *questionTestAgent, string) {
	a := newQuestionAgent()
	r := newRig(t, func(c *Config) { c.Agent = a; c.Turns = 1; c.FilesDir = t.TempDir() })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "seed")
	r.wait("seed", func() bool { return len(r.own(work, "ANSWER to seed")) > 0 })
	return r, a, work
}
func questionGroup(async bool, n int) store.QuestionGroup {
	g := store.QuestionGroup{Key: "question-test", ThreadID: "thread-1", TurnID: "turn-1", Async: async, Blocking: true}
	for i := 0; i < n; i++ {
		g.Questions = append(g.Questions, store.Question{NativeID: fmt.Sprintf("q-%d", i), Title: fmt.Sprintf("使用哪个环境？ %d", i), Options: []store.QuestionOption{{Label: "测试", Description: "测试数据"}, {Label: "生产", Description: "真实数据"}}})
	}
	return g
}

func TestQuestionFormattingAndOperations(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprint(async), func(t *testing.T) {
			g := questionGroup(async, 1)
			g.State = "open"
			q := g.Questions[0]
			q.ID = 42
			d := questionDoc(g, q)
			for _, s := range []string{"使用哪个环境？", "\n\n**A. 测试**\n测试数据", "Quote this message", "helper answer Q42 <answer>", "helper answer Q42 --skip"} {
				if !strings.Contains(d.Markdown, s) {
					t.Errorf("missing %q: %s", s, d.Markdown)
				}
			}
			for _, s := range []string{"Cancel task", "New instructions", "Queue a task"} {
				if strings.Contains(d.Markdown, s) {
					t.Fatal(s)
				}
			}
			if strings.Contains(d.Markdown, "does not wait for your answer") != async {
				t.Fatal(d.Markdown)
			}
			q.Options = nil
			if strings.Contains(questionDoc(g, q).Markdown, "**A**") {
				t.Fatal("free text has options")
			}
			q.Secret = true
			if strings.Contains(questionDoc(g, q).Markdown, "<answer>") {
				t.Fatal("secret answer invited")
			}
		})
	}
}

func TestQuestionResolvedElsewhereDoesNotPresentDraftAsPendingOrSubmitted(t *testing.T) {
	g := questionGroup(false, 2)
	g.State = "resolved"
	q := g.Questions[0]
	q.ID = 42
	draft := "my draft"
	q.Answer = &draft
	d := questionDoc(g, q)
	if !strings.Contains(d.Label, "Resolved") || !strings.Contains(d.Markdown, q.Title) || !strings.Contains(d.Markdown, "Recorded answer") || strings.Contains(d.Markdown, "remaining") || strings.Contains(d.Label, "submitted") {
		t.Fatal(d)
	}
}

func TestQuestionSyncCollectsAndSubmitsOnce(t *testing.T) {
	r, a, work := questionRig(t)
	g := questionGroup(false, 2)
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.wait("two questions", func() bool { return len(r.own(work, "Question from Codex")) == 2 })
	r.ch.PostUser(work, "helper answer Q1 B")
	r.wait("first answer recorded", func() bool { return len(r.own(work, "Answer recorded")) > 0 })
	if a.nreplies() != 0 {
		t.Fatal("partial group submitted")
	}
	r.ch.PostUser(work, "helper answer Q1 A")
	r.ch.PostUser(work, "helper answer Q2 --skip")
	r.wait("group response", func() bool { return a.nreplies() == 1 })
	a.qm.Lock()
	reply := a.replies[0]
	a.qm.Unlock()
	if len(reply) != 1 || len(reply["q-0"]) != 1 || reply["q-0"][0] != "测试" {
		t.Fatalf("%v", reply)
	}
	r.ch.PostUser(work, "helper answer Q2 B")
	r.wait("closed answer refused", func() bool { return len(r.own(work, "already resolved")) > 0 })
	if a.nreplies() != 1 || len(a.runLog()) != 1 {
		t.Fatal("response replayed or created execution")
	}
}

func TestQuestionAsyncUsesNativeReplyAndSteer(t *testing.T) {
	r, a, work := questionRig(t)
	a.smu.Lock()
	a.current = "external-turn"
	a.smu.Unlock()
	g := questionGroup(true, 1)
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.ch.PostUser(work, "helper answer Q1 B")
	r.wait("async input", func() bool { return a.count() == 1 })
	a.smu.Lock()
	text := a.inputs[0].Text
	a.smu.Unlock()
	if !strings.HasPrefix(text, questionReplyStart) || !strings.HasSuffix(text, questionReplyEnd) || strings.Contains(text, "Teams helper safety") {
		t.Fatal(text)
	}
	var replies []map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSuffix(strings.TrimPrefix(text, questionReplyStart), questionReplyEnd)), &replies); err != nil {
		t.Fatal(err)
	}
	if replies[0]["answer"] != "生产" || replies[0]["questionItemId"] != "q-0" {
		t.Fatal(replies)
	}
	if a.nreplies() != 0 || len(a.runLog()) != 1 {
		t.Fatal("async used RPC or another execution")
	}
}

func TestQuestionAsyncIdleAndSkip(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprint(skip), func(t *testing.T) {
			r, a, work := questionRig(t)
			g := questionGroup(true, 1)
			if err := a.emit(g); err != nil {
				t.Fatal(err)
			}
			r.wait("question delivered", func() bool { return len(r.own(work, "Question from Codex")) == 1 })
			answer := "my custom answer"
			if skip {
				answer = "--skip"
			}
			r.ch.PostUser(work, "helper answer Q1 "+answer)
			if skip {
				r.wait("skipped", func() bool { return len(r.own(work, "Skipped")) > 0 })
				if len(a.runLog()) != 1 {
					t.Fatal("skip started task")
				}
			} else {
				r.wait("follow-up", func() bool { return len(a.runLog()) == 2 })
				if !strings.HasPrefix(a.runLog()[1].Text, questionReplyStart) {
					t.Fatal(a.runLog()[1].Text)
				}
			}
		})
	}
}

func TestQuestionFullConcurrencyDoesNotBlockAnswers(t *testing.T) {
	a := newQuestionAgent()
	release := make(chan struct{})
	a.block["blocked task"] = release
	r := newRig(t, func(c *Config) { c.Agent = a; c.Turns = 1 })
	r.ag = a.fakeAgent
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "blocked task")
	r.wait("thread binding committed", func() bool {
		r.e.mu.Lock()
		bound := r.e.byThread["thread-1"] != 0
		r.e.mu.Unlock()
		return bound && len(a.runLog()) == 1
	})
	if err := a.emit(questionGroup(false, 1)); err != nil {
		t.Fatal(err)
	}
	r.ch.PostUser(work, "helper queue later task")
	r.ch.PostUser(work, "helper answer Q1 A")
	r.wait("answer at full concurrency", func() bool { return a.nreplies() == 1 })
	close(release)
	r.wait("queue drained", func() bool { return len(a.runLog()) == 2 })
}

func TestQuestionUnknownDeliveryNeverReplays(t *testing.T) {
	r, a, work := questionRig(t)
	a.failure = errors.New("connection lost after write")
	if err := a.emit(questionGroup(false, 1)); err != nil {
		t.Fatal(err)
	}
	// The status edit requires a delivered message. Answering before publication
	// can intentionally suppress the entire question; that path is tested apart.
	r.wait("question delivered", func() bool { return len(r.own(work, "Question from Codex")) == 1 })
	r.ch.PostUser(work, "helper answer Q1 A")
	r.wait("submitted", func() bool { return a.nreplies() == 1 })
	r.wait("unknown notice", func() bool { return len(r.own(work, "Delivery unconfirmed")) > 0 })
	r.e.maintainQuestions(context.Background())
	r.ch.PostUser(work, "helper answer Q1 B")
	r.wait("not replayed", func() bool { return len(r.own(work, "already resolved")) > 0 })
	if a.nreplies() != 1 {
		t.Fatal("ambiguous response replayed")
	}
}

func TestQuestionDeadlineAndResolvedReplay(t *testing.T) {
	r, a, work := questionRig(t)
	g := questionGroup(false, 1)
	g.Blocking = false
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	if err := r.e.update(func(tx *store.Tx) error {
		cur, _, err := tx.QuestionGroup(g.Key)
		if err != nil {
			return err
		}
		_, err = txUpdateDeadline(tx, cur.Key)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	r.e.maintainQuestions(context.Background())
	r.wait("timeout response", func() bool { return a.nreplies() == 1 })
	a.qm.Lock()
	empty := len(a.replies[0]) == 0
	a.qm.Unlock()
	if !empty {
		t.Fatal("timeout chose option")
	}
	if err := a.emit(g); err != nil {
		t.Fatal(err)
	}
	r.ch.PostUser(work, "helper answer Q1 B")
	r.wait("expired reply refused", func() bool { return len(r.own(work, "already resolved")) > 0 })
	if a.nreplies() != 1 {
		t.Fatal("replay reopened request")
	}
}

// Rewrite the fixture's deadline through the store's explicit test clock API.
func txUpdateDeadline(tx *store.Tx, key string) (bool, error) {
	return tx.SetQuestionDeadline(key, time.Now().Add(-time.Minute).UnixMilli())
}
