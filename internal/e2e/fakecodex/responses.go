// Fake Responses API shared by native protocol probes and client unit tests.
package fakecodex

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Markers placed in prompts select the fake model's behaviour.
//
//	probe:hold:<tag>      stream response.created, then wait for Release(tag)
//	probe:finalhold:<tag> emit a final answer item, then wait before completion
//	probe:http:<status>   answer the HTTP request with that status and an
//	                      OpenAI-style error body (429 rate limit, 503 overloaded…)
//	probe:failed-event    200 + SSE response.failed with server_is_overloaded
//
// Anything else gets an immediate answer "ANSWER(<first 40 chars>)".
var markerRE = regexp.MustCompile(`probe:(finalhold|hold|http|failed-event)(?::([A-Za-z0-9_-]+))?`)

// ResponsesRequest is one request the fake model received.
type ResponsesRequest struct {
	ToolNames []string
	Schema    json.RawMessage
	Effort    string
	UserText  string // input actually presented to the model, not just accepted by app-server
	Model     string
	At        time.Time
	Method    string
	Path      string
	Marker    string
	Status    int
	Cancelled bool // the client went away before the stream finished
	// CancelledAt is when the fake noticed the client going away.
	CancelledAt time.Time
	Finished    bool // the full stream (through response.completed) was written
	Note        string
}

// Responses is the fake Responses API.
type Responses struct {
	ts *httptest.Server

	mu       sync.Mutex
	reqs     []*ResponsesRequest
	holds    map[string]chan struct{}
	started  map[string]chan struct{}
	answers  map[string]string
	respSeq  int
	toolOnce *responseToolCall
}

type responseToolCall struct{ name, args string }

// ToolOnce makes the next offline model response call a native tool. Native
// contract tests then exercise Codex's own question events and response parser.
func (r *Responses) ToolOnce(name string, args any) {
	raw, _ := json.Marshal(args)
	r.mu.Lock()
	r.toolOnce = &responseToolCall{name, string(raw)}
	r.mu.Unlock()
}

// NewResponses starts the fake on a loopback port.
func NewResponses() *Responses {
	r := &Responses{holds: map[string]chan struct{}{}, started: map[string]chan struct{}{}, answers: map[string]string{}}
	r.ts = httptest.NewServer(http.HandlerFunc(r.serve))
	return r
}

// URL is the base URL to put in model_providers.<id>.base_url (with /v1).
func (r *Responses) URL() string { return r.ts.URL + "/v1" }

// Close stops the server (releasing every hold first).
func (r *Responses) Close() {
	r.mu.Lock()
	for tag, ch := range r.holds {
		select {
		case <-ch:
		default:
			close(ch)
		}
		delete(r.holds, tag)
	}
	r.mu.Unlock()
	r.ts.CloseClientConnections()
	r.ts.Close()
}

func (r *Responses) chans(tag string) (hold chan struct{}, started chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.holds[tag] == nil {
		r.holds[tag] = make(chan struct{})
		r.started[tag] = make(chan struct{})
	}
	return r.holds[tag], r.started[tag]
}

// Started returns a channel closed when the held request for tag arrives.
func (r *Responses) Started(tag string) <-chan struct{} {
	_, s := r.chans(tag)
	return s
}

// Release lets the held stream for tag finish.
func (r *Responses) Release(tag string) {
	h, _ := r.chans(tag)
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-h:
	default:
		close(h)
	}
}

// Requests returns a copy of every recorded request.
func (r *Responses) Requests() []ResponsesRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ResponsesRequest, 0, len(r.reqs))
	for _, q := range r.reqs {
		out = append(out, *q)
	}
	return out
}

// Answer is the text the fake model answers for a held tag.
func Answer(tag string) string { return "ANSWER-" + tag }

func (r *Responses) serve(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(req.Body, 32<<20))
	rec := &ResponsesRequest{At: time.Now(), Method: req.Method, Path: req.URL.Path, UserText: lastUserText(body)}
	var envelope struct {
		Model string `json:"model"`
		Tools []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"tools"`
		Reasoning struct {
			Effort string `json:"effort"`
		} `json:"reasoning"`
		Text struct {
			Format struct {
				Schema json.RawMessage `json:"schema"`
			} `json:"format"`
		} `json:"text"`
	}
	_ = json.Unmarshal(body, &envelope)
	rec.Model = envelope.Model
	rec.Effort = envelope.Reasoning.Effort
	rec.Schema = envelope.Text.Format.Schema
	for _, tool := range envelope.Tools {
		rec.ToolNames = append(rec.ToolNames, tool.Type+":"+tool.Name)
	}
	r.mu.Lock()
	r.reqs = append(r.reqs, rec)
	r.respSeq++
	respID := fmt.Sprintf("resp_probe_%d", r.respSeq)
	tool := r.toolOnce
	r.toolOnce = nil
	r.mu.Unlock()
	set := func(f func()) { r.mu.Lock(); f(); r.mu.Unlock() }

	if req.Method != http.MethodPost || !strings.HasSuffix(req.URL.Path, "/responses") {
		set(func() { rec.Status = 404; rec.Note = "unmodelled path" })
		http.Error(w, `{"error":{"message":"not found","type":"invalid_request_error"}}`, http.StatusNotFound)
		return
	}
	// Only the newest user text decides the behaviour (earlier turns of the
	// same thread are replayed in input).
	marker, arg := "", ""
	if ms := markerRE.FindAllStringSubmatch(lastUserText(body), -1); len(ms) > 0 {
		marker, arg = ms[len(ms)-1][1], ms[len(ms)-1][2]
	}
	set(func() { rec.Marker = strings.TrimSuffix(marker+":"+arg, ":") })

	switch marker {
	case "http":
		status := 503
		fmt.Sscanf(arg, "%d", &status)
		var payload string
		switch status {
		case 429:
			payload = `{"error":{"message":"Rate limit reached for requests","type":"requests","param":null,"code":"rate_limit_exceeded"}}`
			w.Header().Set("Retry-After", "1")
		default:
			payload = `{"error":{"message":"The server is overloaded. Please try again later.","type":"server_error","param":null,"code":"server_is_overloaded"}}`
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(payload))
		set(func() { rec.Status = status; rec.Finished = true })
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	set(func() { rec.Status = 200 })
	fl, _ := w.(http.Flusher)
	send := func(v map[string]any) bool {
		data, _ := json.Marshal(v)
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", v["type"], data); err != nil {
			return false
		}
		if fl != nil {
			fl.Flush()
		}
		return true
	}
	send(map[string]any{"type": "response.created", "response": map[string]any{"id": respID}})
	if tool != nil {
		item := map[string]any{"type": "function_call", "id": "fc_" + respID, "call_id": "call_" + respID, "name": tool.name, "arguments": tool.args, "status": "completed"}
		send(map[string]any{"type": "response.output_item.added", "output_index": 0, "item": item})
		send(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
		send(map[string]any{"type": "response.completed", "response": map[string]any{"id": respID, "status": "completed", "usage": map[string]any{"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}}})
		set(func() { rec.Finished = true })
		return
	}

	if marker == "failed-event" {
		send(map[string]any{"type": "response.failed", "response": map[string]any{
			"id": respID, "status": "failed",
			"error": map[string]any{"code": "server_is_overloaded", "message": "Selected model is at capacity. Please try a different model."},
		}})
		set(func() { rec.Finished = true })
		return
	}

	answer := "ANSWER(" + truncateResponse(lastUserText(body), 40) + ")"
	var structured struct {
		Text struct {
			Format struct {
				Name   string          `json:"name"`
				Schema json.RawMessage `json:"schema"`
			} `json:"format"`
		} `json:"text"`
	}
	_ = json.Unmarshal(body, &structured)
	if strings.Contains(string(structured.Text.Format.Schema), `"title"`) {
		answer = `{"title":"Native title contract"}`
	}
	if marker == "hold" {
		answer = Answer(arg)
		hold, started := r.chans(arg)
		r.mu.Lock()
		select {
		case <-started:
		default:
			close(started)
		}
		r.mu.Unlock()
		// Keep the stream alive with a reasoning-free in-progress event and
		// notice a client that goes away while held.
		select {
		case <-hold:
		case <-req.Context().Done():
			set(func() {
				rec.Cancelled = true
				rec.CancelledAt = time.Now()
				rec.Note = "client closed the stream while held"
			})
			return
		}
		if req.Context().Err() != nil {
			set(func() { rec.Cancelled = true; rec.Note = "client gone at release" })
			return
		}
	}
	msgID := "msg_" + respID
	var phase any
	if marker == "finalhold" {
		phase = "final_answer"
	}
	ok := send(map[string]any{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{
		"type": "message", "role": "assistant", "id": msgID, "status": "in_progress", "content": []any{}, "phase": phase,
	}})
	ok = ok && send(map[string]any{"type": "response.output_text.delta", "item_id": msgID, "output_index": 0, "content_index": 0, "delta": answer})
	ok = ok && send(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": map[string]any{
		"type": "message", "role": "assistant", "id": msgID, "status": "completed", "phase": phase,
		"content": []any{map[string]any{"type": "output_text", "text": answer, "annotations": []any{}}},
	}})
	// Hold after the answer item, before the response/turn finishes. This
	// distinguishes an answer being visible from a turn being complete.
	if marker == "finalhold" {
		hold, started := r.chans(arg)
		r.mu.Lock()
		select {
		case <-started:
		default:
			close(started)
		}
		r.mu.Unlock()
		select {
		case <-hold:
		case <-req.Context().Done():
			set(func() { rec.Cancelled = true; rec.CancelledAt = time.Now() })
			return
		}
	}
	ok = ok && send(map[string]any{"type": "response.completed", "response": map[string]any{
		"id": respID, "status": "completed",
		"usage": map[string]any{"input_tokens": 10, "input_tokens_details": map[string]any{"cached_tokens": 0}, "output_tokens": 5, "output_tokens_details": map[string]any{"reasoning_tokens": 0}, "total_tokens": 15},
	}})
	set(func() {
		rec.Finished = ok && req.Context().Err() == nil
		if !rec.Finished {
			rec.Cancelled = true
			rec.Note = "write failed after release"
		}
	})
}

// lastUserText returns the text of the last user message in a Responses
// request body.
func lastUserText(body []byte) string {
	var p struct {
		Input []struct {
			Type    string          `json:"type"`
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"input"`
	}
	if json.Unmarshal(body, &p) != nil {
		return ""
	}
	for i := len(p.Input) - 1; i >= 0; i-- {
		in := p.Input[i]
		if in.Role != "user" {
			continue
		}
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(in.Content, &parts) == nil {
			var b strings.Builder
			for _, part := range parts {
				b.WriteString(part.Text)
			}
			// Codex injects environment context as user messages too; skip
			// those.
			if t := b.String(); !strings.HasPrefix(strings.TrimSpace(t), "<") || strings.HasPrefix(strings.TrimSpace(t), "<send_user_message_question_reply>") {
				return t
			}
			continue
		}
		var s string
		if json.Unmarshal(in.Content, &s) == nil {
			return s
		}
	}
	return ""
}

func truncateResponse(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
