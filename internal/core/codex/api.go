package codex

import (
	"context"
	"encoding/json"
	"strings"
)

// Turn statuses (app-server v2).
const (
	StatusInProgress  = "inProgress"
	StatusCompleted   = "completed"
	StatusFailed      = "failed"
	StatusInterrupted = "interrupted"
)

// Thread is the subset of the v2 Thread the helper uses.
type Thread struct {
	ID         string `json:"id"`
	Path       string `json:"path"`
	Cwd        string `json:"cwd"`
	Source     any    `json:"source"`
	Originator string `json:"originator"`
	Preview    string `json:"preview"`
	ForkedFrom string `json:"forkedFromId"`
	Model      string `json:"model"`
	Turns      []Turn `json:"turns"`
}

// Turn is a v2 Turn.
type Turn struct {
	ID     string     `json:"id"`
	Status string     `json:"status"`
	Items  []Item     `json:"items"`
	Error  *TurnError `json:"error"`
}

// Item is a v2 ThreadItem (only the fields the helper reads).
type Item struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	Text     string `json:"text"`
	Phase    string `json:"phase"`
	ClientID string `json:"clientId"`
}

// TurnError is the v2 TurnError.
type TurnError struct {
	Message           string          `json:"message"`
	CodexErrorInfo    json.RawMessage `json:"codexErrorInfo"`
	AdditionalDetails *string         `json:"additionalDetails"`
}

// Capacity reports the model-capacity failure that the helper retries
// (provisional shape: codexErrorInfo "serverOverloaded" or the message).
func (e *TurnError) Capacity() bool {
	if e == nil {
		return false
	}
	info := strings.ToLower(string(e.CodexErrorInfo))
	if strings.Contains(info, "serveroverloaded") || strings.Contains(info, "server_overloaded") {
		return true
	}
	// Rate limit (429): Codex does not retry it either (probe d).
	if strings.Contains(info, "responsetoomanyfailedattempts") && strings.Contains(info, "429") {
		return true
	}
	msg := strings.ToLower(e.Message)
	return strings.Contains(msg, "at capacity") || strings.Contains(msg, "429 too many requests")
}

// ClientID returns the clientId of the turn's user message.
func (t Turn) ClientID() string {
	for _, it := range t.Items {
		if it.Type == "userMessage" && it.ClientID != "" {
			return it.ClientID
		}
	}
	return ""
}

// Final returns the turn's final answer: the last agentMessage with phase
// final_answer, else the last agentMessage.
func (t Turn) Final() string {
	last, final := "", ""
	for _, it := range t.Items {
		if it.Type != "agentMessage" || strings.TrimSpace(it.Text) == "" {
			continue
		}
		last = it.Text
		if it.Phase == "final_answer" {
			final = it.Text
		}
	}
	if final != "" {
		return final
	}
	return last
}

// ThreadStartParams are the thread/start parameters the helper sets.
type ThreadStartParams struct {
	Cwd    string
	Model  string
	Config map[string]any
}

// ThreadStart starts a new thread (it is loaded in this server).
func (s *Server) ThreadStart(ctx context.Context, p ThreadStartParams) (Thread, error) {
	params := map[string]any{}
	if p.Cwd != "" {
		params["cwd"] = p.Cwd
	}
	if p.Model != "" {
		params["model"] = p.Model
	}
	if len(p.Config) > 0 {
		params["config"] = p.Config
	}
	var raw json.RawMessage
	if err := s.Call(ctx, "thread/start", params, &raw); err != nil {
		return Thread{}, err
	}
	var res struct {
		Thread Thread `json:"thread"`
	}
	err := json.Unmarshal(raw, &res)
	s.loadedResponse(res.Thread.ID, raw)
	return res.Thread, err
}

// ThreadResume loads an existing thread into this server (without turns).
func (s *Server) ThreadResume(ctx context.Context, threadID string) (Thread, error) {
	var raw json.RawMessage
	if err := s.Call(ctx, "thread/resume", map[string]any{"threadId": threadID, "excludeTurns": true}, &raw); err != nil {
		return Thread{}, err
	}
	var res struct {
		Thread Thread `json:"thread"`
	}
	err := json.Unmarshal(raw, &res)
	s.loadedResponse(threadID, raw)
	return res.Thread, err
}

// ThreadRead reads a thread, optionally with all turns.
func (s *Server) ThreadRead(ctx context.Context, threadID string, includeTurns bool) (Thread, error) {
	var res struct {
		Thread Thread `json:"thread"`
	}
	err := s.Call(ctx, "thread/read", map[string]any{"threadId": threadID, "includeTurns": includeTurns}, &res)
	return res.Thread, err
}

// RecentTurns lists the newest turns of a thread, newest first.
func (s *Server) RecentTurns(ctx context.Context, threadID string, limit int) ([]Turn, error) {
	var res struct {
		Data []Turn `json:"data"`
	}
	err := s.Call(ctx, "thread/turns/list", map[string]any{"threadId": threadID, "limit": limit, "sortDirection": "desc"}, &res)
	return res.Data, err
}

// FindTurn finds the turn whose user message carries clientID: first among
// the newest turns, then in the whole thread.
func (s *Server) FindTurn(ctx context.Context, threadID, clientID string) (Turn, bool, error) {
	if turns, err := s.RecentTurns(ctx, threadID, 20); err == nil {
		for _, t := range turns {
			if t.ClientID() == clientID {
				return t, true, nil
			}
		}
	}
	th, err := s.ThreadRead(ctx, threadID, true)
	if err != nil {
		return Turn{}, false, err
	}
	for i := len(th.Turns) - 1; i >= 0; i-- {
		if th.Turns[i].ClientID() == clientID {
			return th.Turns[i], true, nil
		}
	}
	return Turn{}, false, nil
}

// Input is one turn input element.
type Input struct {
	Text  string
	Image string // local image path
}

// TurnStartParams are the turn/start parameters.
type TurnStartParams struct {
	ThreadID string
	Input    []Input
	ClientID string // clientUserMessageId: the helper's turn key
	Cwd      string
	Model    string
	Effort   string
}

// TurnStart starts a turn and returns its id.
func (s *Server) TurnStart(ctx context.Context, p TurnStartParams) (string, error) {
	var input []any
	for _, in := range p.Input {
		if in.Image != "" {
			input = append(input, map[string]any{"type": "localImage", "path": in.Image})
		} else {
			input = append(input, map[string]any{"type": "text", "text": in.Text, "text_elements": []any{}})
		}
	}
	params := map[string]any{"threadId": p.ThreadID, "input": input}
	if p.ClientID != "" {
		params["clientUserMessageId"] = p.ClientID
	}
	if p.Cwd != "" {
		params["cwd"] = p.Cwd
	}
	if p.Model != "" {
		params["model"] = p.Model
	}
	if p.Effort != "" {
		params["effort"] = p.Effort
	}
	var res struct {
		Turn Turn `json:"turn"`
	}
	if err := s.Call(ctx, "turn/start", params, &res); err != nil {
		return "", err
	}
	return res.Turn.ID, nil
}

// TurnInterrupt asks Codex to stop a running turn.
func (s *Server) TurnInterrupt(ctx context.Context, threadID, turnID string) error {
	return s.Call(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, nil)
}

// ThreadFork forks a thread after lastTurnID ("" = all completed turns).
func (s *Server) ThreadFork(ctx context.Context, threadID, lastTurnID, cwd string) (Thread, error) {
	params := map[string]any{"threadId": threadID}
	if lastTurnID != "" {
		params["lastTurnId"] = lastTurnID
	}
	if cwd != "" {
		params["cwd"] = cwd
	}
	var res struct {
		Thread Thread `json:"thread"`
	}
	err := s.Call(ctx, "thread/fork", params, &res)
	return res.Thread, err
}

// Model is one model/list entry.
type Model struct {
	ID            string `json:"id"`
	Model         string `json:"model"`
	DisplayName   string `json:"displayName"`
	Description   string `json:"description"`
	IsDefault     bool   `json:"isDefault"`
	DefaultEffort string `json:"defaultReasoningEffort"`
	Efforts       []struct {
		Effort string `json:"reasoningEffort"`
	} `json:"supportedReasoningEfforts"`
}

// Models lists the models Codex offers.
func (s *Server) Models(ctx context.Context) ([]Model, error) {
	var res struct {
		Data []Model `json:"data"`
	}
	err := s.Call(ctx, "model/list", map[string]any{}, &res)
	return res.Data, err
}
