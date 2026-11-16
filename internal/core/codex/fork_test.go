package codex

import (
	"encoding/json"
	"io"
	"testing"
)

func TestThreadForkContract(t *testing.T) {
	for _, id := range []string{"child", ""} {
		t.Run("id="+id, func(t *testing.T) {
			r, w := io.Pipe()
			defer r.Close()
			defer w.Close()
			s := &Server{in: w, done: make(chan struct{}), pending: map[int64]chan rpcResult{}}
			request := make(chan map[string]any, 1)
			go func() {
				var msg struct {
					ID     json.RawMessage
					Params map[string]any
				}
				if err := json.NewDecoder(r).Decode(&msg); err != nil {
					return
				}
				request <- msg.Params
				result, _ := json.Marshal(map[string]any{"thread": map[string]any{"id": id}})
				reply, _ := json.Marshal(wireMsg{ID: msg.ID, Result: result})
				s.handleLine(reply)
			}()
			th, err := s.ThreadFork(tctx(t), "parent", "cutoff", "workspace")
			if (id == "") != (err != nil) || th.ID != id {
				t.Fatalf("fork: %+v %v", th, err)
			}
			p := <-request
			if p["threadId"] != "parent" || p["lastTurnId"] != "cutoff" || p["cwd"] != "workspace" || p["ephemeral"] != false || p["excludeTurns"] != true || p["deferGoalContinuation"] != true {
				t.Fatalf("fork contract: %+v", p)
			}
		})
	}
}
