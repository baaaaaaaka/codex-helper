package codex

import (
	"encoding/json"
	"errors"
	"testing"
)

func goalTestServer() *Server {
	return &Server{pending: map[int64]chan rpcResult{}, threads: map[string]*ThreadInfo{}, goalRPCs: map[int64]goalRPC{}}
}
func TestGoalSnapshotWireOrder(t *testing.T) {
	s := goalTestServer()
	s.pending[1] = make(chan rpcResult, 1)
	s.goalRPCs[1] = goalRPC{Thread: "th", Method: "thread/goal/get"}
	s.handleLine([]byte(`{"id":1,"result":{"goal":{"objective":"work","status":"active","tokensUsed":7}}}`))
	// The query's goroutine has not consumed its result. The newer clear must win.
	s.handleLine([]byte(`{"method":"thread/goal/cleared","params":{"threadId":"th"}}`))
	if i := s.Info("th"); !i.GoalKnown || i.Goal != nil {
		t.Fatalf("clear lost: %+v", i)
	}
	// Late unregistered results cannot resurrect the goal.
	s.handleLine([]byte(`{"id":1,"result":{"goal":{"objective":"stale","status":"active"}}}`))
	if i := s.Info("th"); !i.GoalKnown || i.Goal != nil {
		t.Fatalf("late response applied: %+v", i)
	}
}
func TestGoalSnapshotUnknownAndDisconnect(t *testing.T) {
	s := goalTestServer()
	if s.Info("th").GoalKnown {
		t.Fatal("unobserved goal is known")
	}
	s.track("thread/goal/updated", "th", json.RawMessage(`{"goal":{"objective":"work","status":"active","tokenBudget":100,"tokensUsed":9,"timeUsedSeconds":3}}`))
	i := s.Info("th")
	if !i.GoalKnown || i.Goal.TokensUsed != 9 || i.Goal.TimeUsedSeconds != 3 {
		t.Fatal(i)
	}
	*i.Goal.TokenBudget = 0
	if *s.Info("th").Goal.TokenBudget != 100 {
		t.Fatal("snapshot aliases native state")
	}
	s.failPending()
	if s.Info("th").GoalKnown {
		t.Fatal("disconnected snapshot is authoritative")
	}
}
func TestGoalSnapshotMalformedDoesNotMeanAbsent(t *testing.T) {
	for _, raw := range []string{`{}`, `{"goal":7}`, `{"goal":{}}`, `{"goal":{"status":"active"}}`} {
		s := goalTestServer()
		s.mu.Lock()
		s.goalResult(goalRPC{Thread: "th", Method: "thread/goal/get"}, json.RawMessage(raw), nil)
		s.mu.Unlock()
		if s.Info("th").GoalKnown {
			t.Fatalf("malformed %s accepted", raw)
		}
	}
	s := goalTestServer()
	s.mu.Lock()
	s.goalResult(goalRPC{Thread: "th", Method: "thread/goal/get"}, json.RawMessage(`{"goal":null}`), nil)
	s.mu.Unlock()
	if !s.Info("th").GoalKnown {
		t.Fatal("explicit absence not known")
	}
}

func TestGoalNotificationSupersedesOutstandingQuery(t *testing.T) {
	s := goalTestServer()
	s.pending[1] = make(chan rpcResult, 1)
	s.goalRPCs[1] = goalRPC{Thread: "th", Method: "thread/goal/get"}
	s.handleLine([]byte(`{"method":"thread/goal/cleared","params":{"threadId":"th"}}`))
	s.handleLine([]byte(`{"id":1,"result":{"goal":{"objective":"old snapshot","status":"active"}}}`))
	if i := s.Info("th"); !i.GoalKnown || i.Goal != nil {
		t.Fatalf("old query overrode notification: %+v", i)
	}
}

func TestGoalNewerRequestSupersedesOlderResponse(t *testing.T) {
	s := goalTestServer()
	s.threadLocked("th").goalRevision = 2
	s.pending[1] = make(chan rpcResult, 1)
	s.goalRPCs[1] = goalRPC{Thread: "th", Method: "thread/goal/get", Revision: 1}
	s.pending[2] = make(chan rpcResult, 1)
	s.goalRPCs[2] = goalRPC{Thread: "th", Method: "thread/goal/get", Revision: 2}
	s.handleLine([]byte(`{"id":2,"result":{"goal":null}}`))
	s.handleLine([]byte(`{"id":1,"result":{"goal":{"objective":"stale","status":"active"}}}`))
	if i := s.Info("th"); !i.GoalKnown || i.Goal != nil {
		t.Fatalf("stale query applied: %+v", i)
	}
}

func TestGoalCapabilityRejectionIsNarrow(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&RPCError{Code: -32601, Message: "method not found"}, true},
		{&RPCError{Code: -32600, Message: "goals feature is disabled"}, true},
		{&RPCError{Code: -32600, Message: "database unavailable"}, false},
		{&RPCError{Code: -32603, Message: "active writer"}, false},
		{errors.New("timeout"), false},
	} {
		if IsGoalsUnavailable(tc.err) != tc.want {
			t.Fatal(tc.err)
		}
	}
}
