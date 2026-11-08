package codex

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestQuestionsRetainRPCDespiteAttachedTUI(t *testing.T) {
	seen := make(chan struct{}, 4)
	out := make(replySink, 4)
	s := &Server{in: out, cfg: Config{Log: io.Discard, QuestionRequest: func(*Server, json.RawMessage, json.RawMessage) bool { seen <- struct{}{}; return true }, IgnoreServerRequest: func(json.RawMessage) bool { return true }}}
	s.handleLine([]byte(`{"id":"request-7","method":"item/tool/requestUserInput","params":{"threadId":"thread"}}`))
	select {
	case <-seen:
	case <-time.After(time.Second):
		t.Fatal("no question")
	}
	if len(out) != 0 || !s.HasQuestion(json.RawMessage(`"request-7"`)) {
		t.Fatal("question was auto-answered")
	}
	if err := s.RespondQuestion(json.RawMessage(`"request-7"`), map[string][]string{"env": {"STAGING"}}); err != nil {
		t.Fatal(err)
	}
	reply := <-out
	if !strings.Contains(string(reply), `"answers":{"env":{"answers":["STAGING"]}}`) {
		t.Fatal(string(reply))
	}
	if err := s.RespondQuestion(json.RawMessage(`"request-7"`), nil); !errors.Is(err, ErrQuestionExpired) {
		t.Fatal(err)
	}
}

func TestQuestionsOtherClientResolutionAndApprovalIsolation(t *testing.T) {
	s := &Server{in: make(replySink, 2), cfg: Config{Log: io.Discard}, serverRequests: map[string]wireMsg{`1`: {Method: "item/tool/requestUserInput"}, `2`: {Method: "item/commandExecution/requestApproval"}}}
	s.handleLine([]byte(`{"method":"serverRequest/resolved","params":{"threadId":"thread","requestId":1}}`))
	if err := s.RespondQuestion(json.RawMessage(`1`), nil); !errors.Is(err, ErrQuestionExpired) {
		t.Fatal(err)
	}
	if err := s.RespondQuestion(json.RawMessage(`2`), nil); !errors.Is(err, ErrQuestionExpired) {
		t.Fatal("question path answered an approval")
	}
}

func TestQuestionsAsyncIsNeverFinalAnswer(t *testing.T) {
	q := Item{Type: "agentMessage", Delivery: "async", Phase: "final_answer", Text: "Question"}
	if got := (Turn{Items: []Item{q}}).Final(); got != "" {
		t.Fatal(got)
	}
	if got := (Turn{Items: []Item{{Type: "agentMessage", Phase: "final_answer", Text: "Final"}, q}}).Final(); got != "Final" {
		t.Fatal(got)
	}
}

func TestQuestionsBackendIdentityIsStableAndNeverReused(t *testing.T) {
	var a, b Server
	first := a.QuestionBackendID()
	if first == 0 || a.QuestionBackendID() != first || b.QuestionBackendID() == first {
		t.Fatal("backend identity reused or unstable")
	}
}
