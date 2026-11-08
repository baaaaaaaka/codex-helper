package fakecodex

import (
	"encoding/json"
)

type pendingQuestion struct {
	thread, turn string
	params       any
	reply        chan message
}

func (s *server) answerQuestion(m message) {
	s.mu.Lock()
	p := s.questions[string(m.ID)]
	if p != nil {
		delete(s.questions, string(m.ID))
	}
	s.mu.Unlock()
	if p != nil {
		p.reply <- m
		s.notify("serverRequest/resolved", map[string]any{"threadId": p.thread, "requestId": json.RawMessage(m.ID)})
	}
}

func (s *server) askQuestions(rt *runningTurn, t *Thread, turn *Turn) string {
	script := rt.plan.Questions
	if script == nil {
		return ""
	}
	itemID := newID()
	if script.Async {
		qs := []map[string]any{}
		for _, q := range script.Questions {
			opts := []string{}
			for _, o := range q.Options {
				opts = append(opts, o.Label)
			}
			m := map[string]any{"title": q.Question}
			if len(opts) > 0 {
				m["options"] = opts
			}
			qs = append(qs, m)
		}
		item := map[string]any{"type": "agentMessage", "id": itemID, "phase": "final_answer", "delivery": "async", "questions": qs, "text": "ASYNC_QUESTION_TEXT"}
		s.mu.Lock()
		turn.Items = append(turn.Items, item)
		_ = s.cfg.Store.Save(t)
		if rt.rollout != nil {
			_ = rt.rollout.AppendRecord("response_item", map[string]any{"type": "message", "id": itemID, "role": "assistant", "phase": "final_answer", "delivery": "async", "questions": qs, "content": []any{map[string]any{"type": "output_text", "text": "ASYNC_QUESTION_TEXT"}}})
		}
		s.mu.Unlock()
		s.notify("item/completed", map[string]any{"threadId": rt.threadID, "turnId": rt.turnID, "item": item})
		return ""
	}
	id, _ := json.Marshal(newID())
	params := map[string]any{"threadId": rt.threadID, "turnId": rt.turnID, "itemId": itemID, "isBlocking": script.Blocking, "questions": script.Questions}
	p := &pendingQuestion{thread: rt.threadID, turn: rt.turnID, params: params, reply: make(chan message, 1)}
	s.mu.Lock()
	s.questions[string(id)] = p
	s.mu.Unlock()
	s.send(message{ID: id, Method: "item/tool/requestUserInput", Params: mustJSON(params)})
	select {
	case reply := <-p.reply:
		raw, _ := json.Marshal(reply.Result)
		if reply.Error != nil {
			raw, _ = json.Marshal(reply.Error)
		}
		return string(raw)
	case <-rt.ctx.Done():
		s.mu.Lock()
		delete(s.questions, string(id))
		s.mu.Unlock()
		s.notify("serverRequest/resolved", map[string]any{"threadId": rt.threadID, "requestId": id})
		return ""
	}
}

func (s *server) replayQuestions(thread string) {
	s.mu.Lock()
	var pending []message
	for id, p := range s.questions {
		if p.thread == thread {
			pending = append(pending, message{ID: json.RawMessage(id), Method: "item/tool/requestUserInput", Params: mustJSON(p.params)})
		}
	}
	s.mu.Unlock()
	for _, m := range pending {
		s.send(m)
	}
}
