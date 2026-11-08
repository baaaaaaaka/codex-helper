package store

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func questionHistoryFixture(tb testing.TB, n int) *Store {
	tb.Helper()
	s, err := OpenWith(filepath.Join(tb.TempDir(), "questions.sqlite"), Options{Exclusive: true})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { s.Close() })
	if _, err = s.Acquire(testIdent(100)); err != nil {
		tb.Fatal(err)
	}
	err = s.Update(func(tx *Tx) error {
		conv, err := tx.CreateConversation(NewConversation{Label: "questions", Kind: KindWork, Channel: "teams", ExtChatID: "questions"})
		if err != nil {
			return err
		}
		_, err = tx.exec(`WITH RECURSIVE seq(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM seq WHERE i<?)
INSERT INTO question_groups(key,conv_id,thread_id,turn_id,async,blocking,deadline,state,created_at,updated_at)
SELECT 'history-'||i,?,'thread','turn',1,0,0,'resolved',?,? FROM seq`, n, conv, tx.now, tx.now)
		if err != nil {
			return err
		}
		_, err = tx.exec(`INSERT INTO questions(group_key,native_id,title,inbound_id) SELECT key,'q','Question',rowid FROM question_groups`)
		return err
	})
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

func TestQuestionsLargeHistoryUsesIndexesAndDoesNotRewrite(t *testing.T) {
	s := questionHistoryFixture(t, 10000)
	for _, tc := range []struct {
		query, index string
		args         []any
	}{
		{`SELECT 1 FROM questions WHERE inbound_id=?`, "questions_inbound", []any{9999}},
		{`SELECT key FROM question_groups WHERE state IN ('open','submitting')`, "question_groups_open", nil},
		{`SELECT key FROM question_groups WHERE thread_id=? AND state IN ('open','submitting')`, "question_groups_thread", []any{"thread"}},
		{`SELECT DISTINCT conv_id FROM question_groups WHERE state IN ('open','submitting') AND async=0`, "COVERING INDEX question_groups_open", nil},
		{`SELECT EXISTS(SELECT 1 FROM outbox WHERE delivery_key>=? AND delivery_key<?)`, "SEARCH outbox USING COVERING INDEX", []any{"question:", "question:\U0010FFFF"}},
		{`SELECT key FROM question_groups WHERE state NOT IN ('open','submitting') AND updated_at<?`, "question_groups_retention", []any{0}},
	} {
		rows, err := s.w.Query("EXPLAIN QUERY PLAN "+tc.query, tc.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plans []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plans = append(plans, detail)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(strings.Join(plans, "\n"), tc.index) {
			t.Fatal("growing history scan", tc.query, plans)
		}
	}
	var before, after int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	for i := 0; i < 10; i++ {
		mustT(t, s.Update(func(tx *Tx) error {
			groups, err := tx.OpenQuestionGroups()
			if err != nil {
				return err
			}
			if len(groups) != 0 {
				t.Fatal("terminal questions loaded")
			}
			if yes, err := tx.IsQuestionReply(99999); err != nil {
				return err
			} else if yes {
				t.Fatal("unexpected question")
			}
			return tx.PruneQuestions(0)
		}))
	}
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if before != after {
		t.Fatal("unchanged history wrote rows", before, after)
	}
	// A delayed maintenance snapshot must not overwrite a newer resolution.
	mustT(t, s.Update(func(tx *Tx) error {
		changed, err := tx.TransitionQuestion("history-1", "open", "expired")
		if changed {
			t.Fatal("terminal question regressed")
		}
		return err
	}))
	// An event in one thread must not load active questions of other threads;
	// waiting flags need only distinct conversations with unresolved RPCs.
	other := newWork(t, s, "other")
	mustT(t, s.Update(func(tx *Tx) error {
		for _, g := range []QuestionGroup{
			{Key: "active-async", ConvID: 1, ThreadID: "thread-a", Async: true},
			{Key: "active-rpc-1", ConvID: other, ThreadID: "thread-b"},
			{Key: "active-rpc-2", ConvID: other, ThreadID: "thread-b"},
		} {
			g.Questions = []Question{{NativeID: "q", Title: "Question", Options: []QuestionOption{{Label: "A"}}}}
			if _, err := tx.AddQuestionGroup(g); err != nil {
				return err
			}
		}
		groups, err := tx.ThreadQuestionGroups("thread-a")
		if err != nil {
			return err
		}
		if len(groups) != 1 || groups[0].Key != "active-async" || len(groups[0].Questions) != 1 {
			t.Fatalf("unrelated active questions loaded: %+v", groups)
		}
		convs, err := tx.WaitingQuestionConversations()
		if err != nil {
			return err
		}
		if len(convs) != 1 || convs[0] != other {
			t.Fatalf("waiting flags include async questions or duplicate chats: %v", convs)
		}
		if err := tx.SetQuestionState("active-async", "resolved"); err != nil {
			return err
		}
		groups, err = tx.ThreadQuestionGroups("thread-a")
		if len(groups) != 0 {
			t.Fatal("terminal question loaded for event")
		}
		return err
	}))
}

func BenchmarkQuestionsOrdinaryPromptLookup(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s := questionHistoryFixture(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Update(func(tx *Tx) error { _, err := tx.IsQuestionReply(999999); return err }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
