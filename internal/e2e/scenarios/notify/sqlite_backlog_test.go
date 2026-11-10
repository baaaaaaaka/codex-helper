package notify

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	_ "modernc.org/sqlite"
)

// Exercise the real sender and notifier together. Store-level point-query
// tests alone cannot prove that a later turn wakes the right notification.
func TestNotifySQLiteUnknownBacklogDoesNotHideNewFinal(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "NOTIFY-SQLITE-BACKLOG", harness.Options{
		Title:  "retained unknown finals and queued progress do not starve a later answer and its owner notice",
		Budget: 100 * time.Second, WorkflowWebhook: true,
		Golden: harness.GoldenOptions{Skip: "assert exact POST, card and retained evidence counts for a synthetic backlog"},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "sqlite-backlog")
	// The existing resolver independently reads back every unknown send. This
	// fixture deliberately exceeds the normal per-chat read cap; keep the
	// exception confined to this chat and bound its reads below. Changing that
	// retry policy is outside the SQLite storage migration.
	s.Opts.Invariants.Except = append(s.Opts.Invariants.Except, harness.Exception{
		Invariant: harness.InvNoLoop,
		Match:     `reads of chat "💬 e2ehost - [^"\n]*(sqlite-backlog|E2E-NOTIFYSQLITEBACKLOG-new)[^"\n]*" within 1s`,
		Reason:    "1000 independently retained unknown sends use the unchanged read-back policy; request count is bounded explicitly",
	})
	s.Require("ready notification delivered", steps.CommandTimeout, func() (bool, string) {
		return len(s.Graph.WorkflowCards()) == 1, fmt.Sprint(len(s.Graph.WorkflowCards()))
	})
	path := s.StorePath()
	if path == "" {
		t.Fatal("no published store")
	}
	s.StopSUT()
	// The only writer has stopped. These SQL literals describe the independent
	// schema-six fixture; no product serializer creates the expected evidence.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	defer tx.Rollback()
	var conv, seq int64
	if err = tx.QueryRow(`SELECT id FROM conversations WHERE ext_chat_id=?`, work.ID).Scan(&conv); err == nil {
		err = tx.QueryRow(`SELECT COALESCE(MAX(seq),0) FROM outbox WHERE conv_id=?`, conv).Scan(&seq)
	}
	if err == nil {
		_, err = tx.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1000)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,attempt_token,created_at)
SELECT ?,'retained-unknown:'||i||':final',?+i,'final','retained answer '||i,'unknown','original-attempt-'||i,1 FROM n`, conv, seq)
	}
	if err == nil {
		_, err = tx.Exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1000)
INSERT INTO pending_notifications(id,turn_id,conv_id,key,kind,text,request,attempted)
SELECT 'retained-unknown:'||i||':',0,?,'retained-unknown:'||i||':','','',?,0 FROM n`, conv, strings.Repeat("retained request ", 64))
	}
	if err == nil {
		_, err = tx.Exec(`INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) VALUES
(?,'backlog-progress:1',?,'progress','earlier queued progress one','queued',1),
(?,'backlog-progress:2',?,'progress','earlier queued progress two','queued',1)`, conv, seq+1001, conv, seq+1002)
	}
	if err == nil {
		err = tx.Commit()
	}
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	before := len(s.Graph.WorkflowCards())
	beforeRequests := s.Graph.LastSeq()
	start := time.Now()
	s.StartSUT()
	token := s.Token("new-final")
	steps.MustPost(s, work.ID, "Answer after the retained backlog "+token)
	_, answered := steps.WaitAnswered(s, work.ID, token, steps.AnswerTimeout)
	s.Require("new final owner notice passes unknown backlog", steps.CommandTimeout, func() (bool, string) {
		return len(s.Graph.WorkflowCards()) == before+1, fmt.Sprint(len(s.Graph.WorkflowCards()) - before)
	})
	t.Logf("1000 retained notices plus queued progress: answer and card latency=%s", time.Since(start))
	s.Check("new turn executes and posts once", answered && len(s.Codex.CallsWithToken(token)) == 1 && len(steps.AnswerPosts(s, work.ID, token)) == 1, "one execution and final POST", fmt.Sprint(answered))
	s.StopSUT()
	reads := len(s.Graph.Requests(fakegraph.RequestFilter{Since: beforeRequests, Method: "GET", Route: "GET /chats/{chat}/messages", ChatID: work.ID}))
	// E2E resolve delay is 500 ms; even without exponential backoff there can
	// be at most one round per delay. Leave the ordinary poll budget separate.
	maxReads := 1000*(int(time.Since(start)/(500*time.Millisecond))+1) + 100
	s.Check("read-back work remains bounded by retained sends and elapsed rounds", reads <= maxReads, fmt.Sprintf("<= %d reads", maxReads), fmt.Sprint(reads))
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var unknown, notices, attempted int
	if err = db.QueryRow(`SELECT count(*) FROM outbox WHERE delivery_key GLOB 'retained-unknown:*' AND state='unknown' AND attempt_token GLOB 'original-attempt-*'`).Scan(&unknown); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*),COALESCE(SUM(attempted),0) FROM pending_notifications WHERE id GLOB 'retained-unknown:*'`).Scan(&notices, &attempted); err != nil {
		t.Fatal(err)
	}
	s.Check("unknown evidence remains pending and no card was tried for it", unknown == 1000 && notices == 1000 && attempted == 0, "1000 unknown, 1000 pending, no attempts", fmt.Sprintf("%d %d %d", unknown, notices, attempted))
	s.Check("retained finals were never reposted", len(s.SUTMessagesContaining(work.ID, "retained answer ")) == 0, "zero copies", fmt.Sprint(len(s.SUTMessagesContaining(work.ID, "retained answer "))))
}
