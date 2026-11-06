package fakecodex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

// Config configures one app-server process.
type Config struct {
	In          io.Reader
	Out         io.Writer
	Log         io.Writer
	Store       Store
	SessionsDir string // where rollouts are written ("" = none)
	Brain       string // base URL of the brain
	Kind        string
	Instance    string
	// CodexHome is where config.toml is read ("" = no config).
	CodexHome string
	// Exit terminates the process (os.Exit); replaceable for tests.
	Exit func(code int)
}

type server struct {
	cfg  Config
	http *http.Client

	outMu sync.Mutex
	out   *bufio.Writer

	mu     sync.Mutex
	loaded map[string]*Thread // threads started/resumed by this process
	// mcp is, per loaded thread, the MCP servers of config.toml when this
	// process loaded it: like real Codex, a loaded thread keeps the config
	// it was loaded with until the process ends.
	mcp      map[string][]string
	rollouts map[string]*rollout.Writer
	running  map[string]*runningTurn // by turn id
	wg       sync.WaitGroup
	gone     atomic.Bool // stdin closed: the helper went away
}

type runningTurn struct {
	threadID string
	turnID   string
	plan     Plan
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	rollout  *rollout.Writer
	open     rollout.OpenTurn
}

type message struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result any             `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Serve runs the protocol until stdin closes. On stdin EOF it abandons
// running turns (unless their plan says SurviveHelper, in which case it
// waits for them to finish first) and returns.
func Serve(cfg Config) error {
	if cfg.Exit == nil {
		cfg.Exit = os.Exit
	}
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	s := &server{
		cfg:      cfg,
		http:     &http.Client{Transport: &http.Transport{Proxy: nil}},
		out:      bufio.NewWriter(cfg.Out),
		loaded:   map[string]*Thread{},
		mcp:      map[string][]string{},
		rollouts: map[string]*rollout.Writer{},
		running:  map[string]*runningTurn{},
	}
	in := bufio.NewReaderSize(cfg.In, 1<<20)
	for {
		line, err := in.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var msg message
			if jerr := json.Unmarshal(line, &msg); jerr != nil {
				fmt.Fprintf(cfg.Log, "fakecodex: bad line: %v\n", jerr)
			} else if msg.Method != "" && len(msg.ID) > 0 {
				s.wg.Add(1)
				go func() {
					defer s.wg.Done()
					s.handle(msg)
				}()
			}
			// Notifications (initialized) and responses to server
			// requests need no handling.
		}
		if err != nil {
			break
		}
	}
	fmt.Fprintf(cfg.Log, "%s stdin closed\n", time.Now().Format("15:04:05.000"))
	s.gone.Store(true)
	s.onHelperGone()
	return nil
}

// onHelperGone handles stdin EOF: the helper exited or was killed.
func (s *server) onHelperGone() {
	s.mu.Lock()
	var survive, abandon []*runningTurn
	for _, rt := range s.running {
		if rt.plan.SurviveHelper {
			survive = append(survive, rt)
		} else {
			abandon = append(abandon, rt)
		}
	}
	s.mu.Unlock()
	for _, rt := range abandon {
		s.end(rt.plan.Seq, "abandoned", "app-server stdin closed (helper went away)")
		rt.cancel()
	}
	for _, rt := range survive {
		<-rt.done
	}
	for _, rt := range abandon {
		select {
		case <-rt.done:
		case <-time.After(5 * time.Second):
		}
	}
}

func (s *server) send(msg message) {
	data, err := json.Marshal(msg)
	if err != nil {
		fmt.Fprintf(s.cfg.Log, "fakecodex: marshal: %v\n", err)
		return
	}
	s.outMu.Lock()
	defer s.outMu.Unlock()
	_, _ = s.out.Write(append(data, '\n'))
	_ = s.out.Flush()
}

func (s *server) notify(method string, params any) {
	data := mustJSON(params)
	fmt.Fprintf(s.cfg.Log, "%s -> %s %s\n", time.Now().Format("15:04:05.000"), method, truncate(string(data), 200))
	s.send(message{Method: method, Params: data})
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func mustJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func (s *server) handle(msg message) {
	fmt.Fprintf(s.cfg.Log, "%s <- %s id=%s %s\n", time.Now().Format("15:04:05.000"), msg.Method, msg.ID, truncate(string(msg.Params), 300))
	result, rerr := s.dispatch(msg)
	if rerr != nil {
		fmt.Fprintf(s.cfg.Log, "%s -> error id=%s %s\n", time.Now().Format("15:04:05.000"), msg.ID, rerr.Message)
		s.send(message{ID: msg.ID, Error: rerr})
		return
	}
	if result == nil {
		result = map[string]any{}
	}
	s.send(message{ID: msg.ID, Result: result})
}

func invalid(format string, args ...any) *rpcError {
	return &rpcError{Code: -32600, Message: fmt.Sprintf(format, args...)}
}

func (s *server) dispatch(msg message) (any, *rpcError) {
	var p map[string]any
	if len(msg.Params) > 0 {
		_ = json.Unmarshal(msg.Params, &p)
	}
	str := func(k string) string { v, _ := p[k].(string); return strings.TrimSpace(v) }
	switch msg.Method {
	case "initialize":
		return map[string]any{"userAgent": "codex_vscode/0.58.0 (e2e fake)"}, nil
	case "thread/list":
		threads, err := s.cfg.Store.List()
		if err != nil {
			return nil, invalid("thread/list: %v", err)
		}
		limit := 25
		if n, ok := p["limit"].(float64); ok && n > 0 {
			limit = int(n)
		}
		data := []any{}
		for i, t := range threads {
			if i >= limit {
				break
			}
			data = append(data, threadJSON(t, false))
		}
		return map[string]any{"data": data, "nextCursor": nil}, nil
	case "model/list":
		efforts := []any{}
		for _, e := range []string{"low", "medium", "high"} {
			efforts = append(efforts, map[string]any{"reasoningEffort": e, "description": e})
		}
		// A second, smaller model (gap batch B: `model switch|fork`,
		// `default model set`, effort reconciliation).
		small := []any{}
		for _, e := range []string{"low", "medium"} {
			small = append(small, map[string]any{"reasoningEffort": e, "description": e})
		}
		return map[string]any{"data": []any{map[string]any{
			"id": "gpt-5-codex", "model": "gpt-5-codex", "displayName": "GPT-5-Codex", "description": "e2e",
			"isDefault": true, "supportedReasoningEfforts": efforts, "defaultReasoningEffort": "medium",
		}, map[string]any{
			"id": "gpt-5-mini", "model": "gpt-5-mini", "displayName": "GPT-5-Mini", "description": "e2e",
			"isDefault": false, "supportedReasoningEfforts": small, "defaultReasoningEffort": "low",
		}}, "nextCursor": nil}, nil
	case "thread/start":
		now := time.Now().Unix()
		t := &Thread{ID: newID(), Cwd: str("cwd"), CreatedAt: now, UpdatedAt: now, Turns: []*Turn{}}
		if err := s.cfg.Store.Save(t); err != nil {
			return nil, invalid("thread/start: %v", err)
		}
		mcp := s.mcpServers()
		s.mu.Lock()
		s.loaded[t.ID] = t
		s.mcp[t.ID] = mcp
		s.mu.Unlock()
		s.notify("thread/started", map[string]any{"thread": threadJSON(t, false)})
		return map[string]any{"thread": threadJSON(t, false), "model": "gpt-5-codex", "modelProvider": "openai", "cwd": t.Cwd}, nil
	case "thread/resume":
		id := str("threadId")
		t, err := s.cfg.Store.Load(id)
		if err != nil {
			return nil, invalid("no rollout found for thread id %s", id)
		}
		s.mu.Lock()
		_, loaded := s.loaded[id]
		s.mu.Unlock()
		if !loaded {
			// Real Codex 0.166 refuses to load a thread another process
			// holds (FIDELITY C5); the brain decides whether one does.
			dec, err := s.resumeCheck(ResumeRequest{Instance: s.cfg.Instance, Kind: s.cfg.Kind, Pid: os.Getpid(), ThreadID: id})
			if err != nil {
				return nil, &rpcError{Code: -32603, Message: "fake codex brain unavailable: " + err.Error()}
			}
			if dec.Reject != "" {
				return nil, invalid("%s", strings.ReplaceAll(dec.Reject, "{thread}", id))
			}
		}
		mcp := s.mcpServers()
		s.mu.Lock()
		if cur, ok := s.loaded[id]; ok {
			t = cur
		} else {
			s.loaded[id] = t
			s.mcp[id] = mcp
		}
		s.mu.Unlock()
		include := true
		if v, ok := p["excludeTurns"].(bool); ok && v {
			include = false
		}
		return map[string]any{"thread": s.threadView(t, include), "model": "gpt-5-codex", "modelProvider": "openai", "cwd": t.Cwd}, nil
	case "thread/read":
		t, err := s.current(str("threadId"))
		if err != nil {
			return nil, invalid("thread not found: %s", str("threadId"))
		}
		include, _ := p["includeTurns"].(bool)
		return map[string]any{"thread": s.threadView(t, include)}, nil
	case "thread/turns/list":
		t, err := s.current(str("threadId"))
		if err != nil {
			return nil, invalid("thread not found: %s", str("threadId"))
		}
		view := s.threadView(t, true)
		turns, _ := view["turns"].([]any)
		if str("sortDirection") == "desc" {
			for i, j := 0, len(turns)-1; i < j; i, j = i+1, j-1 {
				turns[i], turns[j] = turns[j], turns[i]
			}
		}
		if n, ok := p["limit"].(float64); ok && n > 0 && int(n) < len(turns) {
			turns = turns[:int(n)]
		}
		return map[string]any{"data": turns, "nextCursor": nil}, nil
	case "thread/fork":
		return s.fork(p)
	case "turn/start":
		return s.startTurn(p)
	case "turn/interrupt":
		return s.interrupt(str("threadId"), str("turnId"))
	}
	return nil, &rpcError{Code: -32601, Message: "method not found: " + msg.Method}
}

// current returns the in-process copy of a loaded thread, else the stored one.
func (s *server) current(id string) (*Thread, error) {
	s.mu.Lock()
	t, ok := s.loaded[id]
	s.mu.Unlock()
	if ok {
		return t, nil
	}
	return s.cfg.Store.Load(id)
}

// threadView renders a thread under the lock (turn goroutines mutate it).
func (s *server) threadView(t *Thread, includeTurns bool) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return threadJSON(t, includeTurns)
}

func threadJSON(t *Thread, includeTurns bool) map[string]any {
	status := map[string]any{"type": "idle"}
	if t.Active() {
		status = map[string]any{"type": "active", "activeFlags": []any{}}
	}
	preview := ""
	if len(t.Turns) > 0 {
		preview = userText(t.Turns[0])
	}
	var name any
	if t.Name != "" {
		name = t.Name
	}
	var path any
	if t.RolloutPath != "" {
		path = t.RolloutPath
	}
	m := map[string]any{
		"id": t.ID, "sessionId": t.ID, "projectId": nil, "preview": preview,
		"ephemeral": false, "modelProvider": "openai", "model": "gpt-5-codex",
		"createdAt": t.CreatedAt, "updatedAt": t.UpdatedAt,
		"path": path, "cwd": t.Cwd, "cliVersion": "0.58.0",
		"source": "vscode", "originator": "codex_vscode", "gitInfo": nil,
		"name": name, "status": status, "turns": []any{},
	}
	if t.ForkedFrom != "" {
		m["forkedFromId"] = t.ForkedFrom
	}
	if includeTurns {
		turns := []any{}
		for _, turn := range t.Turns {
			turns = append(turns, turnJSON(turn))
		}
		m["turns"] = turns
	}
	return m
}

func turnJSON(turn *Turn) map[string]any {
	items := []any{}
	for _, it := range turn.Items {
		items = append(items, it)
	}
	var terr any
	if turn.Error != nil {
		terr = turn.Error
	}
	return turnView(turn.ID, turn.Status, items, terr, turn.StartedAt, turn.CompletedAt)
}

// turnView renders the v2 Turn shape: id, status, items, startedAt,
// completedAt, durationMs, error.
func turnView(id, status string, items []any, terr any, startedAt, completedAt int64) map[string]any {
	var completed, duration any
	if completedAt > 0 {
		completed = completedAt
		duration = (completedAt - startedAt) * 1000
	}
	return map[string]any{
		"id": id, "status": status, "items": items, "error": terr,
		"startedAt": startedAt, "completedAt": completed, "durationMs": duration,
	}
}

func userText(turn *Turn) string {
	for _, it := range turn.Items {
		if it["type"] != "userMessage" {
			continue
		}
		content, _ := it["content"].([]any)
		for _, c := range content {
			cm, _ := c.(map[string]any)
			if cm["type"] == "text" {
				s, _ := cm["text"].(string)
				return s
			}
		}
	}
	return ""
}

func (s *server) startTurn(p map[string]any) (any, *rpcError) {
	threadID, _ := p["threadId"].(string)
	s.mu.Lock()
	t, ok := s.loaded[threadID]
	busy := ok && t.Active()
	mcp := s.mcp[threadID]
	s.mu.Unlock()
	if !ok {
		// Real Codex 0.166 text for a thread this process has not loaded
		// (FIDELITY C5).
		return nil, invalid("thread not found: %s", threadID)
	}
	if busy {
		// Real Codex 0.166 (probe c, internal/e2e/realcodex) does not reject
		// a second turn/start on a thread with a running turn: it steers the
		// new input into the running turn and answers with that turn's id.
		return s.steer(t, threadID, p)
	}
	var prompt string
	var images []string
	var content []any
	inputs, _ := p["input"].([]any)
	for _, raw := range inputs {
		in, _ := raw.(map[string]any)
		switch in["type"] {
		case "text":
			text, _ := in["text"].(string)
			prompt += text
			content = append(content, map[string]any{"type": "text", "text": text, "text_elements": []any{}})
		case "localImage":
			path, _ := in["path"].(string)
			images = append(images, path)
			content = append(content, map[string]any{"type": "localImage", "path": path})
		}
	}
	if strings.TrimSpace(prompt) == "" && len(images) == 0 {
		return nil, invalid("turn/start: empty input")
	}
	cwd, _ := p["cwd"].(string)
	if cwd == "" {
		cwd = t.Cwd
	}
	// clientUserMessageId is echoed as the userMessage item's clientId (and
	// as client_id in the rollout), exactly like real Codex.
	clientID, _ := p["clientUserMessageId"].(string)
	model, _ := p["model"].(string)
	effort, _ := p["effort"].(string)
	turnID := newID()
	plan, err := s.begin(BeginRequest{
		Instance: s.cfg.Instance, Kind: s.cfg.Kind, Pid: os.Getpid(),
		ThreadID: threadID, TurnID: turnID, ClientID: clientID, Cwd: cwd, Prompt: prompt, Images: images,
		Model: model, Effort: effort, Exe: executable(), MCPServers: mcp,
	})
	if err != nil {
		return nil, &rpcError{Code: -32603, Message: "fake codex brain unavailable: " + err.Error()}
	}
	if plan.RejectStart != "" {
		// Real Codex 0.166 answers a thread held by another process with
		// -32600 "thread <id> already has an active writer" (on thread/resume;
		// see FIDELITY.md C5).
		return nil, invalid("%s", strings.ReplaceAll(plan.RejectStart, "{thread}", threadID))
	}
	if s.gone.Load() {
		// The helper went away while turn/start was in flight: the turn
		// never starts.
		s.end(plan.Seq, "abandoned", "helper went away before the turn started")
		return nil, &rpcError{Code: -32603, Message: "client gone"}
	}
	now := time.Now().Unix()
	turn := &Turn{
		ID: turnID, Status: StatusInProgress, OwnerPID: os.Getpid(), StartedAt: now,
		Items: []map[string]any{{"type": "userMessage", "id": newID(), "clientId": nullable(clientID), "content": content}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt := &runningTurn{threadID: threadID, turnID: turnID, plan: plan, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	s.mu.Lock()
	t.Turns = append(t.Turns, turn)
	t.UpdatedAt = now
	saveErr := s.cfg.Store.Save(t)
	s.running[turnID] = rt
	// Like Codex, the rollout gets the turn start and the user message
	// right away; the rest is appended when the turn ends.
	if w := s.rolloutLocked(t); w != nil {
		open, werr := w.BeginTurn(rollout.Turn{ID: turnID, ClientID: clientID, Prompt: prompt})
		if werr != nil {
			fmt.Fprintf(s.cfg.Log, "fakecodex: rollout: %v\n", werr)
		} else {
			rt.rollout, rt.open = w, open
		}
	}
	s.mu.Unlock()
	if saveErr != nil {
		fmt.Fprintf(s.cfg.Log, "fakecodex: save: %v\n", saveErr)
	}
	view := turnView(turnID, StatusInProgress, []any{}, nil, now, 0)
	go func() {
		// Real Codex announces the turn right after accepting it.
		time.Sleep(5 * time.Millisecond)
		s.notify("turn/started", map[string]any{"threadId": threadID, "turn": view})
		s.runTurn(rt, t, turn)
	}()
	if plan.MalformedStart {
		return map[string]any{"turn": "malformed"}, nil
	}
	return map[string]any{"turn": view}, nil
}

// steer appends a turn/start input to the thread's running turn, the way
// real Codex 0.166 does. The brain is not told: the running turn's answer
// stands for both inputs.
func (s *server) steer(t *Thread, threadID string, p map[string]any) (any, *rpcError) {
	var content []any
	inputs, _ := p["input"].([]any)
	for _, raw := range inputs {
		in, _ := raw.(map[string]any)
		if in["type"] == "text" {
			text, _ := in["text"].(string)
			content = append(content, map[string]any{"type": "text", "text": text, "text_elements": []any{}})
		}
	}
	clientID, _ := p["clientUserMessageId"].(string)
	s.mu.Lock()
	var running *Turn
	for i := len(t.Turns) - 1; i >= 0; i-- {
		if t.Turns[i].Status == StatusInProgress {
			running = t.Turns[i]
			break
		}
	}
	if running == nil {
		s.mu.Unlock()
		return nil, invalid("thread %s has no running turn to steer", threadID)
	}
	running.Items = append(running.Items, map[string]any{"type": "userMessage", "id": newID(), "clientId": nullable(clientID), "content": content})
	saveErr := s.cfg.Store.Save(t)
	turnID := running.ID
	s.mu.Unlock()
	if saveErr != nil {
		fmt.Fprintf(s.cfg.Log, "fakecodex: save: %v\n", saveErr)
	}
	fmt.Fprintf(s.cfg.Log, "fakecodex: turn/start on busy thread %s steered into turn %s\n", threadID, turnID)
	view := turnView(turnID, StatusInProgress, []any{}, nil, 0, 0)
	view["startedAt"] = nil
	view["itemsView"] = "notLoaded"
	return map[string]any{"turn": view}, nil
}

func (s *server) runTurn(rt *runningTurn, t *Thread, turn *Turn) {
	defer func() {
		s.mu.Lock()
		delete(s.running, rt.turnID)
		s.mu.Unlock()
		rt.cancel()
		close(rt.done)
	}()
	if rt.plan.Crash {
		s.end(rt.plan.Seq, "crashed", "fake app-server crashed mid-turn")
		fmt.Fprintf(s.cfg.Log, "fakecodex: scripted crash in turn %s\n", rt.turnID)
		s.cfg.Exit(3)
		return
	}
	for i := 0; i < rt.plan.Compactions; i++ {
		s.notify("item/completed", map[string]any{"threadId": rt.threadID, "turnId": rt.turnID, "completedAtMs": time.Now().UnixMilli(), "item": map[string]any{
			"type": "contextCompaction", "id": newID(),
		}})
		s.notify("thread/compacted", map[string]any{"threadId": rt.threadID, "turnId": rt.turnID})
	}
	if rt.plan.Commentary != "" {
		s.notify("item/completed", map[string]any{"threadId": rt.threadID, "turnId": rt.turnID, "completedAtMs": time.Now().UnixMilli(), "item": map[string]any{
			"type": "agentMessage", "id": newID(), "text": rt.plan.Commentary, "phase": "commentary",
		}})
	}
	for _, r := range rt.plan.Retries {
		if r.Delay > 0 {
			timer := time.NewTimer(r.Delay)
			select {
			case <-rt.ctx.Done():
			case <-timer.C:
			}
			timer.Stop()
		}
		if rt.ctx.Err() != nil {
			break
		}
		var details any
		if r.Details != "" {
			details = r.Details
		}
		s.notify("error", map[string]any{"threadId": rt.threadID, "turnId": rt.turnID, "willRetry": true,
			"error": map[string]any{"message": r.Message, "codexErrorInfo": r.Info, "additionalDetails": details}})
	}
	res, err := s.await(rt.ctx, rt.plan.Seq)
	status := StatusCompleted
	var terr *TurnError
	var final map[string]any
	switch {
	case rt.ctx.Err() != nil || res.Outcome == "cancelled":
		status = StatusInterrupted
		// Tell the brain explicitly; it may not notice the closed request.
		s.end(rt.plan.Seq, "cancelled", "turn interrupted")
	case err != nil:
		status = StatusFailed
		terr = &TurnError{Message: "fake codex brain unavailable: " + err.Error()}
	case res.Outcome == "answered":
		final = map[string]any{"type": "agentMessage", "id": newID(), "text": res.Answer, "phase": "final_answer"}
	default:
		status = StatusFailed
		terr = &TurnError{Message: res.Error}
		// Shapes verified against real Codex 0.145/0.166 (probe d): a
		// provider 503 / response.failed server_is_overloaded and a 429
		// after retries.
		if res.Capacity {
			terr.CodexErrorInfo = "serverOverloaded"
		}
		if res.RateLimited {
			terr.CodexErrorInfo = map[string]any{"responseTooManyFailedAttempts": map[string]any{"httpStatusCode": 429}}
		}
	}
	s.mu.Lock()
	turn.Status = status
	turn.Error = terr
	turn.CompletedAt = time.Now().Unix()
	if final != nil {
		turn.Items = append(turn.Items, final)
	}
	t.UpdatedAt = turn.CompletedAt
	if err := s.cfg.Store.Save(t); err != nil {
		fmt.Fprintf(s.cfg.Log, "fakecodex: save: %v\n", err)
	}
	s.mu.Unlock()
	if rt.rollout != nil {
		answer := ""
		if final != nil {
			answer = res.Answer
		}
		werr := rt.rollout.FinishTurn(rt.open, rollout.Turn{Commentary: rt.plan.Commentary, Answer: answer, Aborted: status != StatusCompleted})
		if werr != nil {
			fmt.Fprintf(s.cfg.Log, "fakecodex: rollout: %v\n", werr)
		}
	}
	if status == StatusCompleted && rt.plan.HoldCompletion {
		fmt.Fprintf(s.cfg.Log, "fakecodex: turn %s completed; turn/completed withheld (scripted lost callback)\n", rt.turnID)
		return
	}
	if final != nil {
		s.notify("item/completed", map[string]any{"threadId": rt.threadID, "turnId": rt.turnID, "completedAtMs": time.Now().UnixMilli(), "item": final})
		s.notify("thread/tokenUsage/updated", map[string]any{"threadId": rt.threadID, "turnId": rt.turnID, "tokenUsage": map[string]any{
			"total": map[string]any{"inputTokens": 1200, "cachedInputTokens": 0, "outputTokens": 80, "reasoningOutputTokens": 20, "totalTokens": 1280},
			"last":  map[string]any{"inputTokens": 1200, "cachedInputTokens": 0, "outputTokens": 80, "reasoningOutputTokens": 20, "totalTokens": 1280},
		}})
	}
	var errView any
	if terr != nil {
		errView = terr
		// Real Codex sends an "error" notification before a failed
		// turn/completed.
		s.notify("error", map[string]any{"error": terr, "willRetry": false, "threadId": rt.threadID, "turnId": rt.turnID})
	}
	summary := []any{}
	if final != nil {
		summary = append(summary, final)
	}
	done := turnView(rt.turnID, status, summary, errView, turn.StartedAt, turn.CompletedAt)
	// Codex 0.166 sends the turn's agent messages as a summary on
	// turn/completed (0.145 sent items [] with itemsView notLoaded).
	done["itemsView"] = "summary"
	s.notify("turn/completed", map[string]any{"threadId": rt.threadID, "turn": done})
}

// rolloutLocked returns (creating on first use) the rollout writer of t.
func (s *server) rolloutLocked(t *Thread) *rollout.Writer {
	if s.cfg.SessionsDir == "" {
		return nil
	}
	if w, ok := s.rollouts[t.ID]; ok {
		return w
	}
	var w *rollout.Writer
	if t.RolloutPath != "" {
		if _, err := os.Stat(t.RolloutPath); err == nil {
			w = rollout.Open(t.RolloutPath, t.ID, t.Cwd)
		}
	}
	if w == nil {
		nw, err := rollout.New(s.cfg.SessionsDir, t.Cwd, rollout.Options{ID: t.ID, Source: "vscode", Originator: "codex_vscode"})
		if err != nil {
			fmt.Fprintf(s.cfg.Log, "fakecodex: create rollout: %v\n", err)
			return nil
		}
		w = nw
		t.RolloutPath = w.Path
		_ = s.cfg.Store.Save(t)
	}
	s.rollouts[t.ID] = w
	return w
}

func (s *server) interrupt(threadID, turnID string) (any, *rpcError) {
	s.mu.Lock()
	rt, ok := s.running[turnID]
	s.mu.Unlock()
	if ok {
		rt.cancel()
		select {
		case <-rt.done:
		case <-time.After(10 * time.Second):
			return nil, &rpcError{Code: -32603, Message: "interrupt timed out"}
		}
		return map[string]any{}, nil
	}
	t, err := s.current(threadID)
	if err != nil || t.TurnByID(turnID) == nil {
		return nil, invalid("no turn %s in thread %s", turnID, threadID)
	}
	return map[string]any{}, nil
}

func (s *server) fork(p map[string]any) (any, *rpcError) {
	parentID, _ := p["threadId"].(string)
	parent, err := s.current(parentID)
	if err != nil {
		return nil, invalid("no rollout found for thread id %s", parentID)
	}
	last, _ := p["lastTurnId"].(string)
	before, _ := p["beforeTurnId"].(string)
	cwd, _ := p["cwd"].(string)
	if cwd == "" {
		cwd = parent.Cwd
	}
	now := time.Now().Unix()
	child := &Thread{ID: newID(), Cwd: cwd, CreatedAt: now, UpdatedAt: now, ForkedFrom: parentID, Turns: []*Turn{}}
	s.mu.Lock()
	for _, turn := range parent.Turns {
		if before != "" && turn.ID == before {
			break
		}
		if turn.Status == StatusInProgress {
			continue
		}
		cp := *turn
		child.Turns = append(child.Turns, &cp)
		if last != "" && turn.ID == last {
			break
		}
	}
	// Codex 0.166 writes the child's rollout at once, with only its
	// session_meta (no copy of the parent's records: the history is read
	// through thread/read), and names it in the response (realcodex probe
	// TestRealCodexForkRollout).
	s.rolloutLocked(child)
	s.mu.Unlock()
	if err := s.cfg.Store.Save(child); err != nil {
		return nil, invalid("thread/fork: %v", err)
	}
	if ephemeral, _ := p["ephemeral"].(bool); !ephemeral {
		mcp := s.mcpServers()
		s.mu.Lock()
		s.loaded[child.ID] = child
		s.mcp[child.ID] = mcp
		s.mu.Unlock()
	}
	s.notify("thread/started", map[string]any{"thread": threadJSON(child, false)})
	return map[string]any{"thread": threadJSON(child, false)}, nil
}

// ---------------------------------------------------------------------------
// brain client

func (s *server) post(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.Brain+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("brain %s: %s: %s", path, resp.Status, strings.TrimSpace(string(msg)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (s *server) begin(req BeginRequest) (Plan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var plan Plan
	err := s.post(ctx, "/begin", req, &plan)
	return plan, err
}

func (s *server) resumeCheck(req ResumeRequest) (ResumeDecision, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var dec ResumeDecision
	err := s.post(ctx, "/resume", req, &dec)
	return dec, err
}

func (s *server) await(ctx context.Context, seq int) (AwaitResult, error) {
	var res AwaitResult
	err := s.post(ctx, "/await?seq="+strconv.Itoa(seq), struct{}{}, &res)
	if err != nil && ctx.Err() != nil {
		return AwaitResult{Outcome: "cancelled"}, nil
	}
	return res, err
}

func (s *server) end(seq int, outcome, msg string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.post(ctx, "/end?seq="+strconv.Itoa(seq), EndRequest{Outcome: outcome, Error: msg}, nil); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(s.cfg.Log, "fakecodex: end: %v\n", err)
	}
}

// executable is the path this app-server was started from ("" if unknown).
func executable() string {
	p, err := helperpath.RawExecutable()
	if err != nil {
		return ""
	}
	return p
}

// mcpServerRE matches a `[mcp_servers.<name>]` table header of config.toml.
var mcpServerRE = regexp.MustCompile(`(?m)^\s*\[mcp_servers\.("?)([^".\]]+)("?)\]\s*$`)

// mcpServers reads the MCP server names of $CODEX_HOME/config.toml now.
func (s *server) mcpServers() []string {
	if s.cfg.CodexHome == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(s.cfg.CodexHome, "config.toml"))
	if err != nil {
		return nil
	}
	var names []string
	for _, m := range mcpServerRE.FindAllStringSubmatch(string(data), -1) {
		names = append(names, m[2])
	}
	return names
}
