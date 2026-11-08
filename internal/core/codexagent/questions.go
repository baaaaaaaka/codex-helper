package codexagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type questionRequest struct {
	srv *codex.Server
	id  json.RawMessage
}

func (a *Agent) SetQuestionHandler(fn func(engine.QuestionEvent) error) {
	a.mu.Lock()
	a.questionHandler = fn
	a.mu.Unlock()
}
func (a *Agent) emitQuestion(ev engine.QuestionEvent) error {
	a.mu.Lock()
	fn := a.questionHandler
	a.mu.Unlock()
	if fn == nil {
		return errors.New("Teams question handling is unavailable")
	}
	return fn(ev)
}
func (a *Agent) requestKey(s *codex.Server, id json.RawMessage) string {
	return fmt.Sprintf("rpc:%s:%d:%s", a.questionEpoch, s.QuestionBackendID(), id)
}

func (a *Agent) onQuestion(s *codex.Server, id, raw json.RawMessage) bool {
	a.mu.Lock()
	enabled := a.questionHandler != nil
	a.mu.Unlock()
	if !enabled {
		return false
	}
	if !s.HasQuestion(id) {
		return true
	}
	var p struct {
		ThreadID  string `json:"threadId"`
		TurnID    string `json:"turnId"`
		Blocking  *bool  `json:"isBlocking"`
		Questions []struct {
			ID       string                 `json:"id"`
			Question string                 `json:"question"`
			Secret   bool                   `json:"isSecret"`
			Options  []store.QuestionOption `json:"options"`
		} `json:"questions"`
	}
	if len(raw) > 128<<10 || json.Unmarshal(raw, &p) != nil || p.ThreadID == "" || len(p.Questions) == 0 || len(p.Questions) > 16 {
		return false
	}
	g := store.QuestionGroup{Key: a.requestKey(s, id), ThreadID: p.ThreadID, TurnID: p.TurnID, Blocking: p.Blocking == nil || *p.Blocking}
	for _, q := range p.Questions {
		if q.ID == "" || strings.TrimSpace(q.Question) == "" || len(q.Options) > 32 {
			return false
		}
		g.Questions = append(g.Questions, store.Question{NativeID: q.ID, Title: q.Question, Secret: q.Secret, Options: q.Options})
	}
	a.mu.Lock()
	a.questionRequests[g.Key] = questionRequest{s, append(json.RawMessage(nil), id...)}
	a.mu.Unlock()
	if err := a.emitQuestion(engine.QuestionEvent{Group: g}); err != nil {
		fmt.Fprintf(a.cfg.Log, "codexagent: question could not be published: %v\n", err)
		a.mu.Lock()
		delete(a.questionRequests, g.Key)
		a.mu.Unlock()
		return false
	}
	if !s.HasQuestion(id) {
		_ = a.emitQuestion(engine.QuestionEvent{Resolved: g.Key})
	}
	return true
}

func (a *Agent) QuestionPending(key string) bool {
	a.mu.Lock()
	r, ok := a.questionRequests[key]
	a.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case <-r.srv.Done():
		return false
	default:
	}
	return r.srv.HasQuestion(r.id)
}

// Closed/reaped backends cannot resolve their retained RPCs. Release those
// references even when a turn ended before sending serverRequest/resolved.
func (a *Agent) pruneQuestionRequests() {
	a.mu.Lock()
	var stale []string
	for key, r := range a.questionRequests {
		dead := false
		select {
		case <-r.srv.Done():
			dead = true
		default:
		}
		if dead || !r.srv.HasQuestion(r.id) {
			delete(a.questionRequests, key)
			stale = append(stale, key)
		}
	}
	a.mu.Unlock()
	for _, key := range stale {
		_ = a.emitQuestion(engine.QuestionEvent{Resolved: key})
	}
}

func (a *Agent) AnswerQuestions(ctx context.Context, key string, answers map[string][]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	r, ok := a.questionRequests[key]
	delete(a.questionRequests, key)
	a.mu.Unlock()
	if !ok {
		return engine.ErrQuestionExpired
	}
	err := r.srv.RespondQuestion(r.id, answers)
	if errors.Is(err, codex.ErrQuestionExpired) {
		return engine.ErrQuestionExpired
	}
	return err
}

func (a *Agent) observeQuestion(s *codex.Server, n codex.Notification) {
	switch n.Method {
	case "item/completed", "serverRequest/resolved", "turn/completed":
	default:
		return
	}
	var p struct {
		ThreadID  string          `json:"threadId"`
		TurnID    string          `json:"turnId"`
		RequestID json.RawMessage `json:"requestId"`
		Item      codex.Item      `json:"item"`
		Turn      codex.Turn      `json:"turn"`
	}
	if json.Unmarshal(n.Params, &p) != nil {
		return
	}
	switch n.Method {
	case "serverRequest/resolved":
		key := a.requestKey(s, p.RequestID)
		a.mu.Lock()
		_, ours := a.questionRequests[key]
		delete(a.questionRequests, key)
		a.mu.Unlock()
		if !ours {
			return
		}
		_ = a.emitQuestion(engine.QuestionEvent{Resolved: key})
	case "turn/completed":
		_ = a.emitQuestion(engine.QuestionEvent{ThreadID: p.ThreadID, TurnID: p.Turn.ID, Finished: true, Interrupted: p.Turn.Status != "completed"})
	case "item/completed":
		if p.Item.Type == "agentMessage" && p.Item.Delivery == "async" && len(p.Item.Questions) > 0 {
			if p.ThreadID == "" || p.Item.ID == "" || len(p.Item.Questions) > 16 || len(n.Params) > 128<<10 {
				return
			}
			g := store.QuestionGroup{Key: "async:" + p.ThreadID + ":" + p.Item.ID, ThreadID: p.ThreadID, TurnID: p.TurnID, Async: true}
			for i, q := range p.Item.Questions {
				if strings.TrimSpace(q.Title) == "" || len(q.Options) > 32 {
					return
				}
				native, _ := json.Marshal([]any{"request_user_input_async", p.Item.ID, i})
				sq := store.Question{NativeID: string(native), Title: q.Title}
				for _, o := range q.Options {
					sq.Options = append(sq.Options, store.QuestionOption{Label: o})
				}
				g.Questions = append(g.Questions, sq)
			}
			if err := a.emitQuestion(engine.QuestionEvent{Group: g}); err != nil {
				fmt.Fprintf(a.cfg.Log, "codexagent: async question: %v\n", err)
			}
		}
		if p.Item.Type == "userMessage" {
			for _, c := range p.Item.Content {
				if ids := answeredIDs(c.Text); len(ids) > 0 {
					_ = a.emitQuestion(engine.QuestionEvent{ThreadID: p.ThreadID, Answered: ids})
				}
			}
		}
	}
}

func answeredIDs(text string) []string {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "<send_user_message_question_reply>") || !strings.HasSuffix(text, "</send_user_message_question_reply>") {
		return nil
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(text, "<send_user_message_question_reply>"), "</send_user_message_question_reply>")
	type reply struct {
		ID string `json:"questionItemId"`
	}
	var rs []reply
	if json.Unmarshal([]byte(raw), &rs) != nil {
		var r reply
		if json.Unmarshal([]byte(raw), &r) != nil {
			return nil
		}
		rs = []reply{r}
	}
	var ids []string
	for _, r := range rs {
		if r.ID != "" {
			ids = append(ids, r.ID)
		}
	}
	return ids
}
