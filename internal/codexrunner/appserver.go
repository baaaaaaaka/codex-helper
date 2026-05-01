package codexrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	appServerMethodInitialize    = "initialize"
	appServerMethodInitialized   = "initialized"
	appServerMethodThreadStart   = "thread/start"
	appServerMethodThreadResume  = "thread/resume"
	appServerMethodThreadRead    = "thread/read"
	appServerMethodThreadList    = "thread/list"
	appServerMethodTurnStart     = "turn/start"
	appServerMethodTurnInterrupt = "turn/interrupt"
)

type AppServerLineTransport interface {
	WriteLine(ctx context.Context, line []byte) error
	ReadLine(ctx context.Context) ([]byte, error)
	Close() error
}

type AppServerTransportStarter interface {
	StartAppServer(ctx context.Context, req AppServerStartRequest) (AppServerLineTransport, error)
}

type AppServerTransportStarterFunc func(context.Context, AppServerStartRequest) (AppServerLineTransport, error)

func (f AppServerTransportStarterFunc) StartAppServer(ctx context.Context, req AppServerStartRequest) (AppServerLineTransport, error) {
	return f(ctx, req)
}

type AppServerStartRequest struct {
	Command    string
	Args       []string
	WorkingDir string
	Timeout    time.Duration
}

type AppServerRunner struct {
	Transport AppServerLineTransport
	Starter   AppServerTransportStarter
	Fallback  Runner

	Command       string
	AppServerArgs []string
	ExtraArgs     []string
	WorkingDir    string
	Timeout       time.Duration

	mu          sync.Mutex
	nextID      int64
	ready       bool
	unavailable bool
}

func NewAppServerRunner(transport AppServerLineTransport) *AppServerRunner {
	return &AppServerRunner{Transport: transport, Command: defaultCodexCommand}
}

func (r *AppServerRunner) StartThread(ctx context.Context, input TurnInput) (TurnResult, error) {
	if err := validatePrompt(input.Prompt); err != nil {
		return TurnResult{}, err
	}
	if err := r.validateAppServerArgs(input.ExtraArgs); err != nil {
		return r.fallbackStartThread(ctx, input, err)
	}
	ctx, cancel := withOptionalTimeout(ctx, firstDuration(input.Timeout, r.Timeout))
	defer cancel()

	r.mu.Lock()
	if err := r.ensureReadyLocked(ctx); err != nil {
		r.mu.Unlock()
		return r.fallbackStartThread(ctx, input, err)
	}
	threadID, err := r.startThreadLocked(ctx, input)
	if err != nil {
		r.mu.Unlock()
		return TurnResult{}, err
	}
	result, err := r.startTurnLocked(ctx, StartTurnInput{ThreadID: threadID, TurnInput: input})
	r.mu.Unlock()
	if result.ThreadID == "" {
		result.ThreadID = threadID
	}
	return result, err
}

func (r *AppServerRunner) ResumeThread(ctx context.Context, threadID string, input TurnInput) (TurnResult, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return TurnResult{}, &Error{Kind: ErrorInvalidRequest, Message: "thread id is required"}
	}
	if err := validatePrompt(input.Prompt); err != nil {
		return TurnResult{}, err
	}
	if err := r.validateAppServerArgs(input.ExtraArgs); err != nil {
		return r.fallbackResumeThread(ctx, threadID, input, err)
	}
	ctx, cancel := withOptionalTimeout(ctx, firstDuration(input.Timeout, r.Timeout))
	defer cancel()

	r.mu.Lock()
	if err := r.ensureReadyLocked(ctx); err != nil {
		r.mu.Unlock()
		return r.fallbackResumeThread(ctx, threadID, input, err)
	}
	resumedThreadID, err := r.resumeThreadLocked(ctx, threadID)
	if err != nil {
		r.mu.Unlock()
		return TurnResult{}, err
	}
	result, err := r.startTurnLocked(ctx, StartTurnInput{ThreadID: resumedThreadID, TurnInput: input})
	r.mu.Unlock()
	if result.ThreadID == "" {
		result.ThreadID = resumedThreadID
	}
	return result, err
}

func (r *AppServerRunner) StartTurn(ctx context.Context, input StartTurnInput) (TurnResult, error) {
	if strings.TrimSpace(input.ThreadID) == "" {
		return r.StartThread(ctx, input.TurnInput)
	}
	if err := validatePrompt(input.Prompt); err != nil {
		return TurnResult{}, err
	}
	if err := r.validateAppServerArgs(input.ExtraArgs); err != nil {
		return r.fallbackStartTurn(ctx, input, err)
	}
	ctx, cancel := withOptionalTimeout(ctx, firstDuration(input.Timeout, r.Timeout))
	defer cancel()

	r.mu.Lock()
	if err := r.ensureReadyLocked(ctx); err != nil {
		r.mu.Unlock()
		return r.fallbackStartTurn(ctx, input, err)
	}
	result, err := r.startTurnLocked(ctx, input)
	r.mu.Unlock()
	if result.ThreadID == "" {
		result.ThreadID = strings.TrimSpace(input.ThreadID)
	}
	return result, err
}

func (r *AppServerRunner) InterruptTurn(ctx context.Context, ref TurnRef) error {
	threadID := strings.TrimSpace(ref.ThreadID)
	turnID := strings.TrimSpace(ref.TurnID)
	if threadID == "" {
		return &Error{Kind: ErrorInvalidRequest, Message: "thread id is required"}
	}
	if turnID == "" {
		return &Error{Kind: ErrorInvalidRequest, Message: "turn id is required"}
	}
	ctx, cancel := withOptionalTimeout(ctx, r.Timeout)
	defer cancel()

	r.mu.Lock()
	if err := r.ensureReadyLocked(ctx); err != nil {
		r.mu.Unlock()
		if r.Fallback != nil {
			return r.Fallback.InterruptTurn(ctx, ref)
		}
		return err
	}
	_, err := r.requestLocked(ctx, appServerMethodTurnInterrupt, map[string]string{
		"threadId": threadID,
		"turnId":   turnID,
	}, nil)
	r.mu.Unlock()
	return err
}

func (r *AppServerRunner) ReadThread(ctx context.Context, threadID string) (Thread, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return Thread{}, &Error{Kind: ErrorInvalidRequest, Message: "thread id is required"}
	}
	ctx, cancel := withOptionalTimeout(ctx, r.Timeout)
	defer cancel()

	r.mu.Lock()
	if err := r.ensureReadyLocked(ctx); err != nil {
		r.mu.Unlock()
		if r.Fallback != nil {
			return r.Fallback.ReadThread(ctx, threadID)
		}
		return Thread{}, err
	}
	result, err := r.requestLocked(ctx, appServerMethodThreadRead, map[string]any{
		"threadId":     threadID,
		"includeTurns": true,
	}, nil)
	r.mu.Unlock()
	if err != nil {
		return Thread{}, err
	}
	thread, ok := decodeThread(result)
	if !ok || thread.ID == "" {
		return Thread{}, &Error{Kind: ErrorParse, Message: "thread/read response did not include a thread id"}
	}
	return thread, nil
}

func (r *AppServerRunner) ListThreads(ctx context.Context, opts ListThreadsOptions) ([]Thread, error) {
	ctx, cancel := withOptionalTimeout(ctx, r.Timeout)
	defer cancel()

	r.mu.Lock()
	if err := r.ensureReadyLocked(ctx); err != nil {
		r.mu.Unlock()
		if r.Fallback != nil {
			return r.Fallback.ListThreads(ctx, opts)
		}
		return nil, err
	}
	result, err := r.requestLocked(ctx, appServerMethodThreadList, appServerListThreadsParams(opts), nil)
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	threads, err := decodeThreads(result)
	if err != nil {
		return nil, err
	}
	return threads, nil
}

func (r *AppServerRunner) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Transport == nil {
		r.ready = false
		return nil
	}
	err := r.Transport.Close()
	r.Transport = nil
	r.ready = false
	return err
}

func (r *AppServerRunner) ensureReadyLocked(ctx context.Context) error {
	if r.ready {
		return nil
	}
	if r.unavailable {
		return &Error{Kind: ErrorUnsupported, Message: "codex app-server is unavailable"}
	}
	if r.Transport == nil {
		if r.Starter == nil {
			err := &Error{Kind: ErrorLaunch, Message: "codex app-server transport is not configured"}
			r.unavailable = true
			return err
		}
		transport, err := r.Starter.StartAppServer(ctx, AppServerStartRequest{
			Command:    firstNonEmpty(r.Command, defaultCodexCommand),
			Args:       append([]string{"app-server"}, r.AppServerArgs...),
			WorkingDir: r.WorkingDir,
			Timeout:    r.Timeout,
		})
		if err != nil {
			r.unavailable = true
			return classifyLaunchError(err)
		}
		r.Transport = transport
	}
	if err := r.initializeLocked(ctx); err != nil {
		r.closeTransportLocked()
		r.unavailable = true
		return err
	}
	r.ready = true
	return nil
}

func (r *AppServerRunner) initializeLocked(ctx context.Context) error {
	if _, err := r.requestLocked(ctx, appServerMethodInitialize, map[string]any{
		"clientInfo": map[string]string{
			"name":    "codex-helper",
			"version": "0",
		},
		"capabilities": nil,
	}, nil); err != nil {
		return err
	}
	if err := r.writeNotificationLocked(ctx, appServerMethodInitialized, map[string]any{}); err != nil {
		return err
	}
	if _, err := r.requestLocked(ctx, appServerMethodThreadList, map[string]any{"limit": 1}, nil); err != nil {
		return err
	}
	return nil
}

func (r *AppServerRunner) startThreadLocked(ctx context.Context, input TurnInput) (string, error) {
	params := map[string]any{}
	if workingDir := firstNonEmpty(input.WorkingDir, r.WorkingDir); workingDir != "" {
		params["cwd"] = workingDir
	}
	result, err := r.requestLocked(ctx, appServerMethodThreadStart, params, nil)
	if err != nil {
		return "", err
	}
	threadID := decodeThreadID(result)
	if threadID == "" {
		return "", &Error{Kind: ErrorParse, Message: "thread/start response did not include a thread id"}
	}
	return threadID, nil
}

func (r *AppServerRunner) resumeThreadLocked(ctx context.Context, threadID string) (string, error) {
	result, err := r.requestLocked(ctx, appServerMethodThreadResume, map[string]string{"threadId": threadID}, nil)
	if err != nil {
		return "", err
	}
	if resumedThreadID := decodeThreadID(result); resumedThreadID != "" {
		return resumedThreadID, nil
	}
	return threadID, nil
}

func (r *AppServerRunner) startTurnLocked(ctx context.Context, input StartTurnInput) (TurnResult, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	if threadID == "" {
		return TurnResult{}, &Error{Kind: ErrorInvalidRequest, Message: "thread id is required"}
	}
	params := map[string]any{
		"threadId": threadID,
		"input": []map[string]string{{
			"type": "text",
			"text": input.Prompt,
		}},
	}
	if workingDir := firstNonEmpty(input.WorkingDir, r.WorkingDir); workingDir != "" {
		params["cwd"] = workingDir
	}
	var result TurnResult
	raw, err := r.requestLocked(ctx, appServerMethodTurnStart, params, func(line []byte) error {
		if err := applyAppServerNotification(&result, line); err != nil {
			return err
		}
		emitAppServerStreamEvent(input.EventHandler, line)
		return nil
	})
	if err != nil {
		return result, err
	}
	if err := applyAppServerResult(&result, raw); err != nil {
		return result, err
	}
	if result.ThreadID == "" {
		result.ThreadID = threadID
	}
	if !isTerminalTurnStatus(result.Status) {
		if err := r.readTurnNotificationsUntilTerminalLocked(ctx, &result, input.EventHandler); err != nil {
			return result, err
		}
	}
	if result.Status == TurnStatusCompleted && strings.TrimSpace(result.FinalAgentMessage) == "" {
		if err := r.backfillTurnResultLocked(ctx, &result); err != nil {
			return result, err
		}
	}
	if result.Failure != nil || result.Status == TurnStatusFailed || result.Status == TurnStatusInterrupted {
		return result, turnResultError(result)
	}
	return result, nil
}

func (r *AppServerRunner) validateAppServerArgs(inputArgs []string) error {
	if len(r.ExtraArgs) == 0 && len(inputArgs) == 0 {
		return nil
	}
	return &Error{Kind: ErrorUnsupported, Message: "codex app-server runner cannot safely translate raw codex CLI arguments yet"}
}

func (r *AppServerRunner) backfillTurnResultLocked(ctx context.Context, result *TurnResult) error {
	threadID := strings.TrimSpace(result.ThreadID)
	if threadID == "" {
		return nil
	}
	raw, err := r.requestLocked(ctx, appServerMethodThreadRead, map[string]any{
		"threadId":     threadID,
		"includeTurns": true,
	}, nil)
	if err != nil {
		return err
	}
	if message := finalAgentMessageForTurn(raw, result.TurnID); strings.TrimSpace(message) != "" {
		result.FinalAgentMessage = message
	}
	return nil
}

func (r *AppServerRunner) readTurnNotificationsUntilTerminalLocked(ctx context.Context, result *TurnResult, handler EventHandler) error {
	for !isTerminalTurnStatus(result.Status) {
		line, err := r.readLineLocked(ctx)
		if err != nil {
			return err
		}
		var msg appServerMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			return &Error{Kind: ErrorParse, Message: "invalid app-server JSON line", Err: err}
		}
		if appServerMessageIsServerRequest(msg) {
			if err := r.writeUnsupportedServerRequestLocked(ctx, msg); err != nil {
				return err
			}
			continue
		}
		if len(bytes.TrimSpace(msg.ID)) > 0 {
			return &Error{Kind: ErrorParse, Message: fmt.Sprintf("unexpected app-server response id %s while waiting for turn completion", string(msg.ID))}
		}
		if err := applyAppServerNotification(result, line); err != nil {
			return err
		}
		emitAppServerStreamEvent(handler, line)
	}
	return nil
}

func (r *AppServerRunner) requestLocked(ctx context.Context, method string, params any, onNotify func([]byte) error) (json.RawMessage, error) {
	id := r.nextRequestIDLocked()
	request := appServerRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}
	line, err := json.Marshal(request)
	if err != nil {
		return nil, &Error{Kind: ErrorParse, Message: "failed to encode app-server request", Err: err}
	}
	if err := r.writeLineLocked(ctx, line); err != nil {
		return nil, err
	}
	for {
		line, err := r.readLineLocked(ctx)
		if err != nil {
			return nil, err
		}
		var msg appServerMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			return nil, &Error{Kind: ErrorParse, Message: "invalid app-server JSON line", Err: err}
		}
		if appServerMessageIsServerRequest(msg) {
			if err := r.writeUnsupportedServerRequestLocked(ctx, msg); err != nil {
				return nil, err
			}
			continue
		}
		if len(msg.ID) == 0 {
			if onNotify != nil {
				if err := onNotify(line); err != nil {
					return nil, err
				}
			}
			continue
		}
		if !sameAppServerID(msg.ID, id) {
			return nil, &Error{Kind: ErrorParse, Message: fmt.Sprintf("unexpected app-server response id %s while waiting for %d", string(msg.ID), id)}
		}
		if msg.Error != nil {
			return nil, appServerResponseError(msg.Error)
		}
		return msg.Result, nil
	}
}

func appServerMessageIsServerRequest(msg appServerMessage) bool {
	return strings.TrimSpace(msg.Method) != "" && len(bytes.TrimSpace(msg.ID)) > 0
}

func (r *AppServerRunner) writeUnsupportedServerRequestLocked(ctx context.Context, msg appServerMessage) error {
	method := strings.TrimSpace(msg.Method)
	if method == "" {
		method = "server request"
	}
	response := appServerErrorResponse{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Error: appServerErrorField{
			Code:    json.RawMessage(`-32601`),
			Message: method + " is not supported by codex-helper app-server runner",
		},
	}
	line, err := json.Marshal(response)
	if err != nil {
		return &Error{Kind: ErrorParse, Message: "failed to encode app-server error response", Err: err}
	}
	return r.writeLineLocked(ctx, line)
}

func (r *AppServerRunner) writeNotificationLocked(ctx context.Context, method string, params any) error {
	line, err := json.Marshal(appServerNotification{JSONRPC: "2.0", Method: method, Params: params})
	if err != nil {
		return &Error{Kind: ErrorParse, Message: "failed to encode app-server notification", Err: err}
	}
	return r.writeLineLocked(ctx, line)
}

func (r *AppServerRunner) writeLineLocked(ctx context.Context, line []byte) error {
	if r.Transport == nil {
		return &Error{Kind: ErrorLaunch, Message: "codex app-server transport is not configured"}
	}
	if err := r.Transport.WriteLine(ctx, line); err != nil {
		return classifyTransportError(err)
	}
	return nil
}

func (r *AppServerRunner) readLineLocked(ctx context.Context) ([]byte, error) {
	if r.Transport == nil {
		return nil, &Error{Kind: ErrorLaunch, Message: "codex app-server transport is not configured"}
	}
	line, err := r.Transport.ReadLine(ctx)
	if err != nil {
		return nil, classifyTransportError(err)
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, &Error{Kind: ErrorParse, Message: "empty app-server JSON line"}
	}
	return line, nil
}

func (r *AppServerRunner) nextRequestIDLocked() int64 {
	r.nextID++
	return r.nextID
}

func (r *AppServerRunner) closeTransportLocked() {
	if r.Transport != nil {
		_ = r.Transport.Close()
		r.Transport = nil
	}
	r.ready = false
}

func (r *AppServerRunner) fallbackStartThread(ctx context.Context, input TurnInput, cause error) (TurnResult, error) {
	if r.Fallback != nil {
		return r.Fallback.StartThread(ctx, input)
	}
	return TurnResult{}, cause
}

func (r *AppServerRunner) fallbackResumeThread(ctx context.Context, threadID string, input TurnInput, cause error) (TurnResult, error) {
	if r.Fallback != nil {
		return r.Fallback.ResumeThread(ctx, threadID, input)
	}
	return TurnResult{}, cause
}

func (r *AppServerRunner) fallbackStartTurn(ctx context.Context, input StartTurnInput, cause error) (TurnResult, error) {
	if r.Fallback != nil {
		return r.Fallback.StartTurn(ctx, input)
	}
	return TurnResult{}, cause
}

type appServerRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type appServerNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type appServerErrorResponse struct {
	JSONRPC string              `json:"jsonrpc"`
	ID      json.RawMessage     `json:"id"`
	Error   appServerErrorField `json:"error"`
}

type appServerMessage struct {
	ID     json.RawMessage      `json:"id"`
	Method string               `json:"method"`
	Params json.RawMessage      `json:"params"`
	Result json.RawMessage      `json:"result"`
	Error  *appServerErrorField `json:"error"`
}

type appServerErrorField struct {
	Code    json.RawMessage `json:"code"`
	Message string          `json:"message"`
}

func appServerResponseError(field *appServerErrorField) error {
	message := strings.TrimSpace(field.Message)
	if message == "" {
		message = "codex app-server returned an error response"
	}
	if code := appServerErrorCodeString(field.Code); code != "" {
		message = code + ": " + message
	}
	return &Error{Kind: ErrorCodex, Message: message}
}

func appServerErrorCodeString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return strings.TrimSpace(text)
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		return number.String()
	}
	return string(raw)
}

func classifyTransportError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Kind: ErrorTimeout, Err: err}
	case errors.Is(err, context.Canceled):
		return &Error{Kind: ErrorCanceled, Err: err}
	case errors.Is(err, io.EOF):
		return &Error{Kind: ErrorLaunch, Message: "codex app-server transport closed", Err: err}
	default:
		return &Error{Kind: ErrorLaunch, Err: err}
	}
}

func sameAppServerID(raw json.RawMessage, id int64) bool {
	var num json.Number
	if err := json.Unmarshal(raw, &num); err == nil {
		got, err := strconv.ParseInt(num.String(), 10, 64)
		return err == nil && got == id
	}
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		return str == strconv.FormatInt(id, 10)
	}
	return false
}

func appServerListThreadsParams(opts ListThreadsOptions) map[string]any {
	params := map[string]any{}
	if opts.WorkingDir != "" {
		params["cwd"] = opts.WorkingDir
	}
	if opts.Limit > 0 {
		params["limit"] = opts.Limit
	}
	return params
}

func decodeThreadID(raw json.RawMessage) string {
	thread, ok := decodeThread(raw)
	if ok {
		return thread.ID
	}
	return ""
}

func decodeThread(raw json.RawMessage) (Thread, bool) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return Thread{}, false
	}
	var envelope struct {
		ID            string `json:"id"`
		ThreadID      string `json:"thread_id"`
		ThreadIDCamel string `json:"threadId"`
		Thread        struct {
			ID            string `json:"id"`
			ThreadID      string `json:"thread_id"`
			ThreadIDCamel string `json:"threadId"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return Thread{}, false
	}
	id := firstNonEmpty(envelope.ThreadIDCamel, envelope.ThreadID, envelope.ID, envelope.Thread.ThreadIDCamel, envelope.Thread.ThreadID, envelope.Thread.ID)
	if id == "" {
		return Thread{}, false
	}
	return Thread{ID: id}, true
}

func decodeThreads(raw json.RawMessage) ([]Thread, error) {
	var direct []json.RawMessage
	if err := json.Unmarshal(raw, &direct); err == nil {
		return decodeThreadList(direct)
	}
	var envelope struct {
		Threads []json.RawMessage `json:"threads"`
		Data    []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, &Error{Kind: ErrorParse, Message: "thread/list response was not a thread list", Err: err}
	}
	if envelope.Data != nil {
		return decodeThreadList(envelope.Data)
	}
	return decodeThreadList(envelope.Threads)
}

func decodeThreadList(rawThreads []json.RawMessage) ([]Thread, error) {
	threads := make([]Thread, 0, len(rawThreads))
	for _, raw := range rawThreads {
		thread, ok := decodeThread(raw)
		if !ok {
			return nil, &Error{Kind: ErrorParse, Message: "thread/list response included a thread without an id"}
		}
		threads = append(threads, thread)
	}
	return threads, nil
}

func applyAppServerNotification(result *TurnResult, line []byte) error {
	var msg appServerMessage
	if err := json.Unmarshal(line, &msg); err != nil {
		return &Error{Kind: ErrorParse, Message: "invalid app-server notification", Err: err}
	}
	if applyAppServerProtocolNotification(result, msg) {
		return nil
	}
	if len(msg.Params) > 0 {
		if applyAppServerEventPayload(result, msg.Params) {
			return nil
		}
	}
	if appServerMethodLooksLikeEvent(msg.Method) {
		payload := appServerMethodEventPayload(msg.Method, msg.Params)
		if applyAppServerEventPayload(result, payload) {
			return nil
		}
	}
	if applyAppServerEventPayload(result, line) {
		return nil
	}
	return nil
}

func emitAppServerStreamEvent(handler EventHandler, line []byte) {
	if handler == nil {
		return
	}
	event, ok := appServerNotificationStreamEvent(line)
	if !ok {
		return
	}
	handler(event)
}

func appServerNotificationStreamEvent(line []byte) (StreamEvent, bool) {
	var msg appServerMessage
	if err := json.Unmarshal(line, &msg); err != nil {
		return StreamEvent{}, false
	}
	switch msg.Method {
	case "thread/started":
		var params struct {
			ThreadID string `json:"threadId"`
			Thread   struct {
				ID string `json:"id"`
			} `json:"thread"`
		}
		if json.Unmarshal(msg.Params, &params) != nil {
			return StreamEvent{}, false
		}
		return StreamEvent{
			Kind:     StreamEventThreadStarted,
			ThreadID: firstNonEmpty(params.ThreadID, params.Thread.ID),
			Raw:      append([]byte(nil), bytes.TrimSpace(line)...),
		}, true
	case "turn/started":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Turn     struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		if json.Unmarshal(msg.Params, &params) != nil {
			return StreamEvent{}, false
		}
		return StreamEvent{
			Kind:     StreamEventTurnStarted,
			ThreadID: params.ThreadID,
			TurnID:   firstNonEmpty(params.TurnID, params.Turn.ID),
			Raw:      append([]byte(nil), bytes.TrimSpace(line)...),
		}, true
	case "item/started", "item/completed":
		var params struct {
			ThreadID string    `json:"threadId"`
			TurnID   string    `json:"turnId"`
			Item     codexItem `json:"item"`
		}
		if json.Unmarshal(msg.Params, &params) != nil {
			return StreamEvent{}, false
		}
		event := StreamEvent{
			ThreadID:         params.ThreadID,
			TurnID:           params.TurnID,
			ItemID:           params.Item.ID,
			ItemType:         params.Item.Type,
			Text:             agentMessageText(params.Item),
			Command:          params.Item.Command,
			AggregatedOutput: commandOutputText(params.Item),
			Status:           params.Item.Status,
			ExitCode:         commandExitCode(params.Item),
			Raw:              append([]byte(nil), bytes.TrimSpace(line)...),
		}
		switch {
		case isAgentMessageItem(params.Item):
			if strings.TrimSpace(event.Text) == "" {
				return StreamEvent{}, false
			}
			event.Kind = StreamEventAgentMessage
		case isCommandExecutionItem(params.Item):
			if msg.Method == "item/started" {
				event.Kind = StreamEventCommandStarted
			} else {
				event.Kind = StreamEventCommandCompleted
			}
		default:
			return StreamEvent{}, false
		}
		return event, true
	case "turn/completed":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Turn     struct {
				ID     string       `json:"id"`
				Status TurnStatus   `json:"status"`
				Error  *TurnFailure `json:"error"`
			} `json:"turn"`
			Usage codexUsage `json:"usage"`
		}
		if json.Unmarshal(msg.Params, &params) != nil {
			return StreamEvent{}, false
		}
		kind := StreamEventTurnCompleted
		var failure *TurnFailure
		if params.Turn.Error != nil || params.Turn.Status == TurnStatusFailed || params.Turn.Status == TurnStatusInterrupted {
			kind = StreamEventTurnFailed
			failure = params.Turn.Error
			if failure == nil && params.Turn.Status == TurnStatusInterrupted {
				failure = &TurnFailure{Message: "Codex turn was interrupted"}
			}
		}
		return StreamEvent{
			Kind:     kind,
			ThreadID: params.ThreadID,
			TurnID:   firstNonEmpty(params.TurnID, params.Turn.ID),
			Failure:  failure,
			Usage:    usageFromEvent(codexEvent{Usage: params.Usage}),
			Raw:      append([]byte(nil), bytes.TrimSpace(line)...),
		}, true
	case "thread/tokenUsage/updated":
		var params struct {
			ThreadID    string              `json:"threadId"`
			TurnID      string              `json:"turnId"`
			TokenUsage  appServerTokenUsage `json:"tokenUsage"`
			TokenUsage2 appServerTokenUsage `json:"token_usage"`
		}
		if json.Unmarshal(msg.Params, &params) != nil {
			return StreamEvent{}, false
		}
		usagePayload := params.TokenUsage
		if usagePayload.isZero() {
			usagePayload = params.TokenUsage2
		}
		var usage Usage
		mergeAppServerUsage(&usage, usagePayload)
		return StreamEvent{
			Kind:     StreamEventUsage,
			ThreadID: params.ThreadID,
			TurnID:   params.TurnID,
			Usage:    usage,
			Raw:      append([]byte(nil), bytes.TrimSpace(line)...),
		}, true
	case "error":
		var params struct {
			ThreadID  string `json:"threadId"`
			TurnID    string `json:"turnId"`
			Message   string `json:"message"`
			Code      string `json:"code"`
			WillRetry bool   `json:"willRetry"`
			Error     struct {
				Message string `json:"message"`
				Code    string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(msg.Params, &params) != nil || params.WillRetry {
			return StreamEvent{}, false
		}
		return StreamEvent{
			Kind:     StreamEventTurnFailed,
			ThreadID: params.ThreadID,
			TurnID:   params.TurnID,
			Failure: &TurnFailure{
				Code:    firstNonEmpty(params.Error.Code, params.Code),
				Message: firstNonEmpty(params.Error.Message, params.Message, "Codex turn failed"),
			},
			Raw: append([]byte(nil), bytes.TrimSpace(line)...),
		}, true
	default:
		return StreamEvent{}, false
	}
}

func applyAppServerResult(result *TurnResult, raw json.RawMessage) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	if applyAppServerEventPayload(result, raw) {
		return nil
	}
	var envelope struct {
		ThreadID      string `json:"thread_id"`
		ThreadIDCamel string `json:"threadId"`
		TurnID        string `json:"turn_id"`
		TurnIDCamel   string `json:"turnId"`
		Turn          struct {
			ID     string       `json:"id"`
			Status TurnStatus   `json:"status"`
			Items  []codexItem  `json:"items"`
			Error  *TurnFailure `json:"error"`
		} `json:"turn"`
		Status            TurnStatus   `json:"status"`
		FinalAgentMessage string       `json:"final_agent_message"`
		Failure           *TurnFailure `json:"failure"`
		Usage             Usage        `json:"usage"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return &Error{Kind: ErrorParse, Message: "invalid turn/start result", Err: err}
	}
	if firstNonEmpty(envelope.ThreadIDCamel, envelope.ThreadID) != "" {
		result.ThreadID = firstNonEmpty(envelope.ThreadIDCamel, envelope.ThreadID)
	}
	if firstNonEmpty(envelope.TurnIDCamel, envelope.TurnID, envelope.Turn.ID) != "" {
		result.TurnID = firstNonEmpty(envelope.TurnIDCamel, envelope.TurnID, envelope.Turn.ID)
	}
	if envelope.Status != "" {
		result.Status = envelope.Status
	}
	if envelope.Turn.Status != "" {
		result.Status = envelope.Turn.Status
	}
	if envelope.FinalAgentMessage != "" {
		result.FinalAgentMessage = envelope.FinalAgentMessage
	}
	if message := finalAgentMessageFromItems(envelope.Turn.Items); strings.TrimSpace(message) != "" {
		result.FinalAgentMessage = message
	}
	if envelope.Failure != nil {
		result.Failure = envelope.Failure
		if result.Status == TurnStatusUnknown {
			result.Status = TurnStatusFailed
		}
	}
	if envelope.Turn.Error != nil {
		result.Failure = envelope.Turn.Error
		if result.Status == TurnStatusUnknown {
			result.Status = TurnStatusFailed
		}
	}
	if envelope.Usage != (Usage{}) {
		result.Usage = envelope.Usage
	}
	return nil
}

func applyAppServerEventPayload(result *TurnResult, raw json.RawMessage) bool {
	if len(bytes.TrimSpace(raw)) == 0 {
		return false
	}
	var event codexEvent
	if err := json.Unmarshal(raw, &event); err != nil || event.Type == "" {
		return false
	}
	applyEvent(result, event, bytes.TrimSpace(raw))
	return true
}

func appServerMethodLooksLikeEvent(method string) bool {
	return strings.HasPrefix(method, "thread.") ||
		strings.HasPrefix(method, "turn.") ||
		strings.HasPrefix(method, "item.") ||
		strings.HasPrefix(method, "thread/") ||
		strings.HasPrefix(method, "turn/") ||
		strings.HasPrefix(method, "item/")
}

func applyAppServerProtocolNotification(result *TurnResult, msg appServerMessage) bool {
	if len(msg.Params) == 0 {
		return false
	}
	switch msg.Method {
	case "thread/started":
		var params struct {
			ThreadID string `json:"threadId"`
			Thread   struct {
				ID string `json:"id"`
			} `json:"thread"`
		}
		if json.Unmarshal(msg.Params, &params) == nil {
			if id := firstNonEmpty(params.ThreadID, params.Thread.ID); id != "" {
				result.ThreadID = id
				return true
			}
		}
	case "turn/started":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Turn     struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		if json.Unmarshal(msg.Params, &params) == nil {
			if params.ThreadID != "" {
				result.ThreadID = params.ThreadID
			}
			if id := firstNonEmpty(params.TurnID, params.Turn.ID); id != "" {
				result.TurnID = id
			}
			result.Status = TurnStatusStarted
			return true
		}
	case "item/agentMessage/delta":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Delta    string `json:"delta"`
		}
		if json.Unmarshal(msg.Params, &params) == nil {
			if params.ThreadID != "" {
				result.ThreadID = params.ThreadID
			}
			if params.TurnID != "" {
				result.TurnID = params.TurnID
			}
			result.FinalAgentMessage += params.Delta
			return true
		}
	case "turn/completed":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Turn     struct {
				ID     string       `json:"id"`
				Status TurnStatus   `json:"status"`
				Items  []codexItem  `json:"items"`
				Error  *TurnFailure `json:"error"`
			} `json:"turn"`
			Usage codexUsage `json:"usage"`
		}
		if json.Unmarshal(msg.Params, &params) == nil {
			if params.ThreadID != "" {
				result.ThreadID = params.ThreadID
			}
			if id := firstNonEmpty(params.TurnID, params.Turn.ID); id != "" {
				result.TurnID = id
			}
			result.Status = firstTurnStatus(params.Turn.Status, TurnStatusCompleted)
			if message := finalAgentMessageFromItems(params.Turn.Items); strings.TrimSpace(message) != "" {
				result.FinalAgentMessage = message
			}
			if params.Turn.Error != nil {
				result.Failure = params.Turn.Error
			}
			mergeUsage(&result.Usage, params.Usage)
			return true
		}
	case "item/completed":
		var params struct {
			ThreadID string    `json:"threadId"`
			TurnID   string    `json:"turnId"`
			Item     codexItem `json:"item"`
		}
		if json.Unmarshal(msg.Params, &params) == nil {
			if params.ThreadID != "" {
				result.ThreadID = params.ThreadID
			}
			if params.TurnID != "" {
				result.TurnID = params.TurnID
			}
			if isAgentMessageItem(params.Item) {
				if text := agentMessageText(params.Item); strings.TrimSpace(text) != "" {
					result.FinalAgentMessage = text
				}
			}
			return true
		}
	case "thread/tokenUsage/updated":
		var params struct {
			ThreadID    string              `json:"threadId"`
			TurnID      string              `json:"turnId"`
			TokenUsage  appServerTokenUsage `json:"tokenUsage"`
			TokenUsage2 appServerTokenUsage `json:"token_usage"`
		}
		if json.Unmarshal(msg.Params, &params) == nil {
			if params.ThreadID != "" {
				result.ThreadID = params.ThreadID
			}
			if params.TurnID != "" {
				result.TurnID = params.TurnID
			}
			usage := params.TokenUsage
			if usage.isZero() {
				usage = params.TokenUsage2
			}
			mergeAppServerUsage(&result.Usage, usage)
			return true
		}
	case "error":
		var params struct {
			ThreadID  string `json:"threadId"`
			TurnID    string `json:"turnId"`
			Message   string `json:"message"`
			Code      string `json:"code"`
			WillRetry bool   `json:"willRetry"`
			Error     struct {
				Message string `json:"message"`
				Code    string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(msg.Params, &params) == nil {
			result.ThreadID = params.ThreadID
			result.TurnID = params.TurnID
			if params.WillRetry {
				return true
			}
			result.Status = TurnStatusFailed
			result.Failure = &TurnFailure{
				Code:    firstNonEmpty(params.Error.Code, params.Code),
				Message: firstNonEmpty(params.Error.Message, params.Message, "Codex turn failed"),
			}
			return true
		}
	}
	return false
}

func appServerMethodEventPayload(method string, params json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(params)) == 0 || bytes.Equal(bytes.TrimSpace(params), []byte("null")) {
		return json.RawMessage(`{"type":` + strconv.Quote(method) + `}`)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(params, &obj); err != nil {
		return params
	}
	if _, ok := obj["type"]; !ok {
		encodedType, _ := json.Marshal(method)
		obj["type"] = encodedType
		payload, err := json.Marshal(obj)
		if err == nil {
			return payload
		}
	}
	return params
}

type appServerTokenUsage struct {
	Total appServerTokenBreakdown `json:"total"`
	Last  appServerTokenBreakdown `json:"last"`
}

func (u appServerTokenUsage) isZero() bool {
	return u.Total.isZero() && u.Last.isZero()
}

type appServerTokenBreakdown struct {
	TotalTokens           int64 `json:"totalTokens"`
	InputTokens           int64 `json:"inputTokens"`
	CachedInputTokens     int64 `json:"cachedInputTokens"`
	OutputTokens          int64 `json:"outputTokens"`
	ReasoningOutputTokens int64 `json:"reasoningOutputTokens"`

	TotalTokensSnake       int64 `json:"total_tokens"`
	InputTokensSnake       int64 `json:"input_tokens"`
	CachedInputTokensSnake int64 `json:"cached_input_tokens"`
	OutputTokensSnake      int64 `json:"output_tokens"`
}

func (b appServerTokenBreakdown) isZero() bool {
	return b.TotalTokens == 0 &&
		b.InputTokens == 0 &&
		b.CachedInputTokens == 0 &&
		b.OutputTokens == 0 &&
		b.ReasoningOutputTokens == 0 &&
		b.TotalTokensSnake == 0 &&
		b.InputTokensSnake == 0 &&
		b.CachedInputTokensSnake == 0 &&
		b.OutputTokensSnake == 0
}

func mergeAppServerUsage(dst *Usage, src appServerTokenUsage) {
	breakdown := src.Last
	if breakdown.isZero() {
		breakdown = src.Total
	}
	if value := firstNonZeroInt64(breakdown.InputTokens, breakdown.InputTokensSnake); value != 0 {
		dst.InputTokens = value
	}
	if value := firstNonZeroInt64(breakdown.OutputTokens, breakdown.OutputTokensSnake); value != 0 {
		dst.OutputTokens = value
	}
	if value := firstNonZeroInt64(breakdown.TotalTokens, breakdown.TotalTokensSnake); value != 0 {
		dst.TotalTokens = value
	}
	if value := firstNonZeroInt64(breakdown.CachedInputTokens, breakdown.CachedInputTokensSnake); value != 0 {
		dst.CachedInputTokens = value
	}
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstTurnStatus(values ...TurnStatus) TurnStatus {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func isAgentMessageItem(item codexItem) bool {
	return item.Type == "agent_message" || item.Type == "agentMessage"
}

func isCommandExecutionItem(item codexItem) bool {
	return item.Type == "command_execution" || item.Type == "commandExecution"
}

func finalAgentMessageFromItems(items []codexItem) string {
	for i := len(items) - 1; i >= 0; i-- {
		if isAgentMessageItem(items[i]) {
			if text := agentMessageText(items[i]); strings.TrimSpace(text) != "" {
				return text
			}
		}
	}
	return ""
}

func finalAgentMessageForTurn(raw json.RawMessage, turnID string) string {
	var envelope struct {
		Thread struct {
			Turns []struct {
				ID    string      `json:"id"`
				Items []codexItem `json:"items"`
			} `json:"turns"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ""
	}
	for i := len(envelope.Thread.Turns) - 1; i >= 0; i-- {
		turn := envelope.Thread.Turns[i]
		if strings.TrimSpace(turnID) != "" && turn.ID != turnID {
			continue
		}
		if message := finalAgentMessageFromItems(turn.Items); strings.TrimSpace(message) != "" {
			return message
		}
	}
	return ""
}

func isTerminalTurnStatus(status TurnStatus) bool {
	return status == TurnStatusCompleted || status == TurnStatusFailed || status == TurnStatusInterrupted
}

func turnResultError(result TurnResult) error {
	if result.Failure != nil {
		message := firstNonEmpty(result.Failure.Message, string(result.Status), "Codex turn failed")
		return &Error{Kind: ErrorCodex, Message: message}
	}
	switch result.Status {
	case TurnStatusInterrupted:
		return &Error{Kind: ErrorCodex, Message: "Codex turn interrupted"}
	case TurnStatusFailed:
		return &Error{Kind: ErrorCodex, Message: "Codex turn failed"}
	default:
		return &Error{Kind: ErrorCodex, Message: "Codex turn did not complete"}
	}
}

func withOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
