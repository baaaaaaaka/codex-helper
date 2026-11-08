package fakecodex

import "maps"

func (s *server) queue(method string, p map[string]any) (any, *rpcError) {
	thread, _ := p["threadId"].(string)
	id, _ := p["queuedSubmissionId"].(string)
	s.submitMu.Lock()
	defer s.submitMu.Unlock()
	switch method {
	case "thread/queue/add":
		q := map[string]any{"id": newID(), "clientUserMessageId": p["clientUserMessageId"], "input": p["input"], "threadId": thread}
		s.queued[thread] = append(s.queued[thread], q)
		go s.dispatchQueued(thread)
		return map[string]any{"queuedSubmission": q}, nil
	case "thread/queue/list":
		return map[string]any{"data": append([]map[string]any(nil), s.queued[thread]...), "nextCursor": nil}, nil
	case "thread/queue/delete":
		for i, q := range s.queued[thread] {
			if q["id"] == id {
				s.queued[thread] = append(s.queued[thread][:i], s.queued[thread][i+1:]...)
				return map[string]any{"deleted": true}, nil
			}
		}
		return map[string]any{"deleted": false}, nil
	case "thread/queue/start":
		s.mu.Lock()
		t := s.loaded[thread]
		busy := t != nil && t.Active()
		settings := s.settings[thread]
		s.mu.Unlock()
		if busy {
			return nil, invalid("thread has an active or pending turn")
		}
		if t == nil {
			return nil, invalid("resume the thread before starting a queued message")
		}
		if len(s.queued[thread]) == 0 {
			return nil, invalid("queued submission not found")
		}
		q := maps.Clone(s.queued[thread][0])
		s.queued[thread] = s.queued[thread][1:]
		q["model"] = settings[0]
		q["effort"] = settings[1]
		return s.startTurn(q)
	}
	return nil, invalid("unknown queue method")
}
func (s *server) dispatchQueued(thread string) {
	s.submitMu.Lock()
	defer s.submitMu.Unlock()
	s.mu.Lock()
	t := s.loaded[thread]
	busy := t != nil && t.Active()
	settings := s.settings[thread]
	s.mu.Unlock()
	if t == nil || busy || len(s.queued[thread]) == 0 || s.gone.Load() {
		return
	}
	// Queue responses are serialized after submitMu is released. Keep their
	// submission maps immutable while constructing the dispatched turn.
	q := maps.Clone(s.queued[thread][0])
	s.queued[thread] = s.queued[thread][1:]
	q["model"] = settings[0]
	q["effort"] = settings[1]
	_, _ = s.startTurn(q)
}
