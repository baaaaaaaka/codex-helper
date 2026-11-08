package store

import (
	"fmt"
	"testing"
)

func TestQuestionsDurabilityIdentityAndNoRewrite(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "questions")
	g := QuestionGroup{Key: "rpc:backend:7", ConvID: conv, ThreadID: "thread", TurnID: "turn", Blocking: true, Questions: []Question{{NativeID: "native", Title: "Choose", Options: []QuestionOption{{Label: "A", Description: "First"}}}}}
	var saved QuestionGroup
	mustT(t, s.Update(func(tx *Tx) error {
		inserted, err := tx.AddQuestionGroup(g)
		if !inserted {
			t.Fatal("not inserted")
		}
		if err != nil {
			return err
		}
		saved, _, err = tx.QuestionGroup(g.Key)
		return err
	}))
	if saved.Questions[0].ID != 1 || saved.Questions[0].Options[0].Description != "First" {
		t.Fatal(saved)
	}
	var before, after int
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	for i := 0; i < 8; i++ {
		mustT(t, s.Update(func(tx *Tx) error {
			inserted, err := tx.AddQuestionGroup(g)
			if inserted {
				t.Fatal("replay inserted")
			}
			return err
		}))
	}
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if before != after {
		t.Fatal("unchanged replay wrote rows", before, after)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.RecordQuestionAnswer(1, 0, "custom", false); err != nil {
			return err
		}
		return tx.SetQuestionState(g.Key, "resolved")
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.PruneQuestions(tx.Now() + 1); err != nil {
			return err
		}
		g.Key = "rpc:backend:8"
		_, err := tx.AddQuestionGroup(g)
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		cur, _, err := tx.QuestionGroup(g.Key)
		if cur.Questions[0].ID <= 1 {
			t.Fatal("visible Q number reused")
		}
		return err
	}))
}

func TestQuestionsCrossChatAndBounds(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "one")
	other := newWork(t, s, "two")
	for i := 0; i < 64; i++ {
		mustT(t, s.Update(func(tx *Tx) error {
			_, err := tx.AddQuestionGroup(QuestionGroup{Key: fmt.Sprint(i), ConvID: conv, ThreadID: "thread", Questions: []Question{{NativeID: "a", Title: "Question"}}})
			return err
		}))
	}
	mustT(t, s.Update(func(tx *Tx) error {
		_, ok, err := tx.QuestionByID(other, 1)
		if ok {
			t.Fatal("cross-chat question exposed")
		}
		return err
	}))
	if err := s.Update(func(tx *Tx) error {
		_, err := tx.AddQuestionGroup(QuestionGroup{Key: "overflow", ConvID: conv, ThreadID: "thread"})
		return err
	}); err == nil {
		t.Fatal("unbounded active groups")
	}
}

func TestQuestionsRetentionKeepsQueuedReplyProvenance(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "retention")
	key := "async:retained"
	var inbound int64
	mustT(t, s.Update(func(tx *Tx) error {
		if _, err := tx.AddQuestionGroup(QuestionGroup{Key: key, ConvID: conv, ThreadID: "thread", Async: true, Questions: []Question{{NativeID: "native", Title: "Question"}}}); err != nil {
			return err
		}
		admitted, err := tx.AdmitWindow("questions", conv, []InboundMsg{{ExtMsgID: "answer", ExtTime: tx.Now(), Sender: SenderOwner, Kind: InCommand, Body: "helper answer Q1 A"}}, "cursor")
		if err != nil {
			return err
		}
		inbound = admitted[0].InboundID
		if err := tx.RecordQuestionAnswer(1, inbound, "A", false); err != nil {
			return err
		}
		if err := tx.QueueQuestionReply(conv, inbound, "native envelope"); err != nil {
			return err
		}
		return tx.SetQuestionState(key, "expired")
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.PruneQuestions(tx.Now() + 1); err != nil {
			return err
		}
		if _, ok, err := tx.QuestionGroup(key); err != nil {
			return err
		} else if !ok {
			t.Fatal("queued answer lost provenance")
		}
		if yes, err := tx.IsQuestionReply(inbound); err != nil {
			return err
		} else if !yes {
			t.Fatal("native envelope no longer recognized")
		}
		if thread, err := tx.QuestionInputThread(inbound); err != nil {
			return err
		} else if thread != "" {
			t.Fatal("expired input still has a dispatch target", thread)
		}
		if err := tx.SetInbound(inbound, InDone, 0); err != nil {
			return err
		}
		if err := tx.PruneQuestions(tx.Now() + 1); err != nil {
			return err
		}
		if _, ok, err := tx.QuestionGroup(key); err != nil {
			return err
		} else if ok {
			t.Fatal("terminal record not pruned")
		}
		return nil
	}))
}
