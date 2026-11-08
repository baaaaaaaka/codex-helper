package fakecodex

import (
	"io"
	"testing"
)

func TestQueueResponsesRemainUnchangedAfterDispatchAndDelete(t *testing.T) {
	for _, dispatch := range []string{"thread/queue/start", "automatic"} {
		t.Run(dispatch, func(t *testing.T) {
			const thread = "thread-1"
			first := map[string]any{"id": "q1", "threadId": thread}
			second := map[string]any{"id": "q2", "threadId": thread}
			s := &server{
				cfg:      Config{Log: io.Discard},
				queued:   map[string][]map[string]any{thread: {first, second}},
				loaded:   map[string]*Thread{thread: {ID: thread}},
				settings: map[string][2]string{thread: {"model", "low"}},
			}
			if dispatch == "automatic" {
				s.dispatchQueued(thread)
			} else {
				_, _ = s.queue(dispatch, map[string]any{"threadId": thread, "queuedSubmissionId": "q1"})
			}
			// Empty input deliberately rejects the execution before any model
			// call; preparing dispatch must still leave the returned input alone.
			if _, changed := first["model"]; changed {
				t.Fatal("dispatch mutated an already-returned queue submission")
			}
			// Reuse the queue backing array through deletion. A prior list
			// response must retain both of its original entries.
			s.queued[thread] = []map[string]any{first, second}
			result, _ := s.queue("thread/queue/list", map[string]any{"threadId": thread})
			list := result.(map[string]any)["data"].([]map[string]any)
			_, _ = s.queue("thread/queue/delete", map[string]any{"threadId": thread, "queuedSubmissionId": "q1"})
			if len(list) != 2 || list[0]["id"] != "q1" || list[1]["id"] != "q2" {
				t.Fatalf("delete changed an already-returned list: %v", list)
			}
		})
	}
}
