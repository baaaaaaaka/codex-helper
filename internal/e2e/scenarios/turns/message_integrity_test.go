package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestProgressIntegrityUnderThrottleAndRestart(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TURN-PROGRESS-INTEGRITY", harness.Options{Guarantees: []string{"R-1", "R-2", "F-TRN-15"}, Title: "120 progress records remain complete and ordered through 429, merging and restart", Budget: 100 * time.Second})
	defer s.Finish()
	tok := s.Token("integrity")
	answer := "Finished " + s.Token("integrityanswer")
	var notes []string
	for i := 0; i < 120; i++ {
		notes = append(notes, fmt.Sprintf("RECORD-%03d-%s", i, tok))
	}
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{MoreCommentary: notes, Answer: answer}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "integrity")
	if _, err := s.Graph.PostAsCoworker(work.ID, "A colleague joined this chat"); err != nil {
		t.Fatal(err)
	}
	fault := s.Graph.AddFault(fakegraph.Fault{Name: "progress-429", Kind: fakegraph.FaultStatus, Method: "POST", ChatID: work.ID, Status: 429, RetryAfter: time.Second, Times: 1, Match: func(r fakegraph.Request) bool { return strings.Contains(r.Body, "RECORD-") }})
	steps.MustPost(s, work.ID, "@codex exercise progress "+tok)
	s.Require("the final arrives after progress", 60*time.Second, func() (bool, string) { return len(s.SUTMessagesContaining(work.ID, answer)) == 1, "waiting for final" })
	s.Require("the helper completion notice is delivered", steps.AnswerTimeout, func() (bool, string) {
		n := len(s.SUTMessagesContaining(control.ID, "Codex finished"))
		return n == 1, fmt.Sprintf("%d completion notice(s)", n)
	})
	check := func(stage string) {
		var all strings.Builder
		posts := 0
		for _, m := range s.SUTMessages(work.ID) {
			all.WriteString(m.PlainText())
			all.WriteByte('\n')
			if strings.Contains(m.PlainText(), "RECORD-") {
				posts++
			}
		}
		text := all.String()
		ordered := true
		last := -1
		missing := []string{}
		for _, note := range notes {
			if strings.Count(text, note) != 1 {
				missing = append(missing, note)
			}
			at := strings.Index(text, note)
			ordered = ordered && at > last
			last = at
		}
		s.Check(stage+": every record once", len(missing) == 0, "120 unique records", fmt.Sprint(missing), s.Transcript(work.ID, 12)...)
		s.Check(stage+": source order before final", ordered && strings.Index(text, answer) > last, "progress in source order, then final", fmt.Sprint(ordered), s.Transcript(work.ID, 12)...)
		s.Check(stage+": complete merges reduce posts", posts > 0 && posts < 30, "1..29 progress POSTs", fmt.Sprint(posts))
	}
	check("live")
	s.Check("429 was exercised", fault.Fired() == 1, "1 throttle", fmt.Sprint(fault.Fired()))
	s.StopSUT()
	s.StartSUT()
	s.Settle(2*time.Second, 8*time.Second)
	check("restart")
}

func TestAcceptedProgressBatchCrashRecovery(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TURN-PROGRESS-BATCH-CRASH", harness.Options{Guarantees: []string{"R-1", "R-2", "R-5"}, Title: "a merged progress POST accepted before helper crash resolves once and keeps every member", Budget: 100 * time.Second})
	defer s.Finish()
	tok := s.Token("crashbatch")
	answer := "Done " + s.Token("crashfinal")
	var notes []string
	for i := 0; i < 100; i++ {
		notes = append(notes, fmt.Sprintf("BATCH-%03d-%s", i, tok))
	}
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{MoreCommentary: notes, Answer: answer}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "batch-crash")
	fault := s.Graph.AddFault(fakegraph.Fault{Name: "accepted-batch-held", Kind: fakegraph.FaultAcceptedHold, Method: "POST", ChatID: work.ID, Times: 1, Match: func(r fakegraph.Request) bool {
		return strings.Contains(r.Body, notes[1]) && strings.Contains(r.Body, notes[2])
	}})
	steps.MustPost(s, work.ID, "Run progress batch "+tok)
	s.Require("a merged POST was accepted and its response held", 30*time.Second, func() (bool, string) {
		return fault.Fired() == 1 && len(s.SUTMessagesContaining(work.ID, notes[2])) == 1, "no accepted batch yet"
	})
	s.CrashSUT()
	s.StartSUT()
	s.Require("delivery reaches the final after recovery", 45*time.Second, func() (bool, string) { return len(s.SUTMessagesContaining(work.ID, answer)) == 1, "final pending" })
	s.Settle(2*time.Second, 8*time.Second)
	var all strings.Builder
	for _, m := range s.SUTMessages(work.ID) {
		all.WriteString(m.PlainText())
		all.WriteByte('\n')
	}
	text := all.String()
	last := -1
	ok := true
	for _, note := range notes {
		at := strings.Index(text, note)
		ok = ok && strings.Count(text, note) == 1 && at > last
		last = at
	}
	s.Check("all members survive once in order", ok && strings.Index(text, answer) > last, "100 progress records exactly once, then final", fmt.Sprint(ok), s.Transcript(work.ID, 12)...)
}

func TestUnknownProgressBatchAllowsFinalAcrossRestart(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TURN-UNKNOWN-BATCH", harness.Options{Title: "an absent progress batch remains retained while the final and next task are delivered", Budget: 100 * time.Second, Golden: harness.GoldenOptions{Skip: "Explicit assertions check the exact failed batch, final, next task, and POST count."}})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "unknown-batch")
	tok := s.Token("unknownbatch")
	answer := "Completed " + s.Token("batchfinal")
	var notes []string
	for i := 0; i < 100; i++ {
		notes = append(notes, fmt.Sprintf("UNKNOWN-BATCH-%03d-%s", i, tok))
	}
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{MoreCommentary: notes, Answer: answer}
		}
		return harness.CodexBehavior{}
	})
	fault := s.Graph.AddFault(fakegraph.Fault{Name: "merged-batch-absent", Kind: fakegraph.FaultDropBeforeAccept, Method: "POST", ChatID: work.ID, Times: 1, Match: func(r fakegraph.Request) bool {
		return strings.Contains(r.Body, notes[1]) && strings.Contains(r.Body, notes[2])
	}})
	steps.MustPost(s, work.ID, "Produce progress "+tok)
	s.Require("final passes unknown batch", 60*time.Second, func() (bool, string) {
		return fault.Fired() == 1 && len(s.SUTMessagesContaining(work.ID, answer)) == 1, "final pending or merged fault not hit"
	})
	s.StopSUT()
	s.StartSUT()
	next := s.Token("afterbatch")
	steps.MustPost(s, work.ID, "Following task "+next)
	_, ok := steps.WaitAnswered(s, work.ID, next, steps.AnswerTimeout)
	s.Check("following task works once", ok && len(s.Codex.CallsWithToken(next)) == 1, "one following answer", fmt.Sprint(ok))
	failed := 0
	for _, p := range s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", ChatID: work.ID}) {
		if strings.Contains(p.Body, notes[1]) || strings.Contains(p.Body, notes[2]) {
			failed++
		}
	}
	s.Check("unknown batch is never replayed or split", failed == 1 && len(s.SUTMessagesContaining(work.ID, notes[1])) == 0 && len(s.SUTMessagesContaining(work.ID, notes[2])) == 0, "one merged attempt, absent members", fmt.Sprint(failed))
	s.Check("completed source task did not rerun", len(s.Codex.CallsWithToken(tok)) == 1, "one execution", fmt.Sprint(len(s.Codex.CallsWithToken(tok))))
}
