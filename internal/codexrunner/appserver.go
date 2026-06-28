package codexrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	appServerMethodInitialize      = "initialize"
	appServerMethodInitialized     = "initialized"
	appServerMethodThreadStart     = "thread/start"
	appServerMethodThreadResume    = "thread/resume"
	appServerMethodThreadRead      = "thread/read"
	appServerMethodThreadList      = "thread/list"
	appServerMethodTurnStart       = "turn/start"
	appServerMethodTurnInterrupt   = "turn/interrupt"
	appServerCompletedRequestLimit = 256
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
	Command          string
	Args             []string
	WorkingDir       string
	ExtraEnv         []string
	Timeout          time.Duration
	ConfigureCommand func(*exec.Cmd) error
}

type AppServerRunner struct {
	Transport AppServerLineTransport
	Starter   AppServerTransportStarter
	// ServerRequestHandler handles app-server initiated requests. When nil, the
	// runner uses AutomaticApprovalHandler with DefaultApprovalDelay.
	ServerRequestHandler AppServerServerRequestHandler

	Command       string
	AppServerArgs []string
	ExtraArgs     []string
	ExtraEnv      []string
	WorkingDir    string
	Timeout       time.Duration
	// BackfillThreadName reads thread metadata after completed turns when the
	// completion stream did not carry a thread/name/updated notification.
	BackfillThreadName bool
	// CloseHook releases resources that must live exactly as long as this
	// runner (for example a local third-party Responses adapter).
	CloseHook func()

	mu      sync.Mutex
	writeMu sync.Mutex
	ready   bool

	protocolCtx             context.Context
	protocolCancel          context.CancelFunc
	protocolDone            chan struct{}
	protocolMu              sync.Mutex
	protocolErr             error
	nextID                  int64
	nextSubscriber          uint64
	pendingRequests         map[int64]chan appServerResponseDelivery
	turnSubscribers         map[uint64]*appServerTurnSubscriber
	threadTurnGates         map[string]*appServerThreadTurnGate
	serverWG                sync.WaitGroup
	serverRequests          map[string]*appServerServerRequestState
	completedServerRequests map[string][]byte
	completedServerOrder    []string
	closeHookOnce           sync.Once
}

type appServerResponseDelivery struct {
	result json.RawMessage
	err    error
}

type appServerTurnSubscriber struct {
	threadID string
	turnID   string
	ctx      context.Context
	frames   chan []byte
	done     <-chan struct{}
}

type appServerServerRequestState struct {
	done     chan struct{}
	response []byte
}

type appServerThreadTurnGate struct {
	mu   sync.Mutex
	refs int
}

func NewAppServerRunner(transport AppServerLineTransport) *AppServerRunner {
	return &AppServerRunner{Transport: transport, Command: defaultCodexCommand}
}

func (r *AppServerRunner) StartThread(ctx context.Context, input TurnInput) (TurnResult, error) {
	if err := validatePrompt(input.Prompt); err != nil {
		return TurnResult{}, err
	}
	if err := r.validateAppServerArgs(input.ExtraArgs); err != nil {
		return TurnResult{}, err
	}
	ctx, cancel := withOptionalTimeout(ctx, firstDuration(input.Timeout, r.Timeout))
	defer cancel()

	if err := r.ensureReady(ctx); err != nil {
		return TurnResult{}, err
	}
	threadID, err := r.startThread(ctx, input)
	if err != nil {
		return TurnResult{}, err
	}
	result, err := r.startTurn(ctx, StartTurnInput{ThreadID: threadID, TurnInput: input})
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
		return TurnResult{}, err
	}
	ctx, cancel := withOptionalTimeout(ctx, firstDuration(input.Timeout, r.Timeout))
	defer cancel()

	if err := r.ensureReady(ctx); err != nil {
		return TurnResult{}, err
	}
	resumedThreadID, err := r.resumeThread(ctx, threadID)
	if err != nil {
		return TurnResult{}, err
	}
	result, err := r.startTurn(ctx, StartTurnInput{ThreadID: resumedThreadID, TurnInput: input})
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
		return TurnResult{}, err
	}
	ctx, cancel := withOptionalTimeout(ctx, firstDuration(input.Timeout, r.Timeout))
	defer cancel()

	if err := r.ensureReady(ctx); err != nil {
		return TurnResult{}, err
	}
	result, err := r.startTurn(ctx, input)
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

	if err := r.ensureReady(ctx); err != nil {
		return err
	}
	_, err := r.request(ctx, appServerMethodTurnInterrupt, map[string]string{
		"threadId": threadID,
		"turnId":   turnID,
	})
	return err
}

func (r *AppServerRunner) ReadThread(ctx context.Context, threadID string) (Thread, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return Thread{}, &Error{Kind: ErrorInvalidRequest, Message: "thread id is required"}
	}
	ctx, cancel := withOptionalTimeout(ctx, r.Timeout)
	defer cancel()

	if err := r.ensureReady(ctx); err != nil {
		return Thread{}, err
	}
	result, err := r.request(ctx, appServerMethodThreadRead, map[string]any{
		"threadId":     threadID,
		"includeTurns": true,
	})
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

	if err := r.ensureReady(ctx); err != nil {
		return nil, err
	}
	result, err := r.request(ctx, appServerMethodThreadList, appServerListThreadsParams(opts))
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
	err := r.closeTransportLocked()
	r.mu.Unlock()
	r.closeHookOnce.Do(func() {
		if r.CloseHook != nil {
			r.CloseHook()
		}
	})
	return err
}

func (r *AppServerRunner) ensureReady(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ensureReadyLocked(ctx)
}

func (r *AppServerRunner) ensureReadyLocked(ctx context.Context) error {
	if r.ready {
		r.protocolMu.Lock()
		failed := r.protocolErr != nil
		r.protocolMu.Unlock()
		if !failed {
			return nil
		}
		// The previous protocol error is already the authoritative failure for
		// that turn. A diagnostic process-wait error while closing the dead
		// transport must not prevent a clean cold restart for the next turn.
		_ = r.closeTransportLocked()
	}
	for attempt := 0; attempt < 2; attempt++ {
		err := r.startAndInitializeLocked(ctx)
		if err == nil {
			return nil
		}
		if attempt == 1 || r.Starter == nil || !IsKind(err, ErrorLaunch) || ctx.Err() != nil {
			return err
		}
		// No thread or turn has been created at this point. One cold retry is
		// therefore safe and recovers transient process-start and handshake
		// failures without risking duplicate user work.
	}
	return &Error{Kind: ErrorLaunch, Message: "codex app-server initialization failed"}
}

func (r *AppServerRunner) startAndInitializeLocked(ctx context.Context) error {
	if r.Transport == nil {
		if r.Starter == nil {
			return &Error{Kind: ErrorLaunch, Message: "codex app-server transport is not configured"}
		}
		transport, err := r.Starter.StartAppServer(ctx, AppServerStartRequest{
			Command:    firstNonEmpty(r.Command, defaultCodexCommand),
			Args:       append([]string{"app-server"}, r.AppServerArgs...),
			WorkingDir: r.WorkingDir,
			ExtraEnv:   append([]string{}, r.ExtraEnv...),
			Timeout:    r.Timeout,
		})
		if err != nil {
			return classifyLaunchError(err)
		}
		r.Transport = transport
	}
	r.startProtocolLoopLocked()
	if err := r.initializeLocked(ctx); err != nil {
		_ = r.closeTransportLocked()
		return err
	}
	r.ready = true
	return nil
}

func (r *AppServerRunner) initializeLocked(ctx context.Context) error {
	if _, err := r.request(ctx, appServerMethodInitialize, map[string]any{
		"clientInfo": map[string]string{
			"name":    "codex-helper",
			"version": "0",
		},
		// runtimeWorkspaceRoots is still experimental in current Codex builds.
		// Advertising the capability is harmless for callers that do not use it
		// and is required for exact `codex --add-dir` compatibility.
		"capabilities": map[string]any{"experimentalApi": true},
	}); err != nil {
		return err
	}
	if err := r.writeNotificationLocked(ctx, appServerMethodInitialized, map[string]any{}); err != nil {
		return err
	}
	if _, err := r.request(ctx, appServerMethodThreadList, map[string]any{"limit": 1}); err != nil {
		return err
	}
	return nil
}

func (r *AppServerRunner) startThread(ctx context.Context, input TurnInput) (string, error) {
	params := map[string]any{}
	if workingDir := firstNonEmpty(input.WorkingDir, r.WorkingDir); workingDir != "" {
		params["cwd"] = workingDir
	}
	if len(input.AdditionalDirs) > 0 {
		params["runtimeWorkspaceRoots"] = append([]string(nil), input.AdditionalDirs...)
	}
	if input.Ephemeral {
		params["ephemeral"] = true
	}
	result, err := r.request(ctx, appServerMethodThreadStart, params)
	if err != nil {
		return "", err
	}
	threadID := decodeThreadID(result)
	if threadID == "" {
		return "", &Error{Kind: ErrorParse, Message: "thread/start response did not include a thread id"}
	}
	return threadID, nil
}

func (r *AppServerRunner) resumeThread(ctx context.Context, threadID string) (string, error) {
	result, err := r.request(ctx, appServerMethodThreadResume, map[string]string{"threadId": threadID})
	if err != nil {
		return "", err
	}
	if resumedThreadID := decodeThreadID(result); resumedThreadID != "" {
		return resumedThreadID, nil
	}
	return threadID, nil
}

func (r *AppServerRunner) startTurn(ctx context.Context, input StartTurnInput) (TurnResult, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	if threadID == "" {
		return TurnResult{}, &Error{Kind: ErrorInvalidRequest, Message: "thread id is required"}
	}
	unlockThread := r.lockThreadTurn(threadID)
	defer unlockThread()
	params := map[string]any{
		"threadId": threadID,
		"input":    appServerTurnInput(input.TurnInput),
	}
	if workingDir := firstNonEmpty(input.WorkingDir, r.WorkingDir); workingDir != "" {
		params["cwd"] = workingDir
	}
	if len(input.AdditionalDirs) > 0 {
		params["runtimeWorkspaceRoots"] = append([]string(nil), input.AdditionalDirs...)
	}
	if len(input.OutputSchema) > 0 {
		params["outputSchema"] = input.OutputSchema
	}
	subscription := r.subscribeTurn(ctx, threadID)
	defer r.unsubscribeTurn(subscription)

	var result TurnResult
	raw, err := r.request(ctx, appServerMethodTurnStart, params)
	if err != nil {
		return result, err
	}
	if err := applyAppServerResult(&result, raw); err != nil {
		return result, err
	}
	if result.ThreadID == "" {
		result.ThreadID = threadID
	}
	r.setTurnSubscriptionID(subscription, result.TurnID)
	if !isTerminalTurnStatus(result.Status) {
		if err := r.readTurnNotificationsUntilTerminal(ctx, subscription, &result, input.EventHandler); err != nil {
			return result, err
		}
	}
	needsBackfillMessage := strings.TrimSpace(result.FinalAgentMessage) == ""
	needsBackfillName := (r.BackfillThreadName || input.BackfillThreadName) && strings.TrimSpace(result.ThreadName) == ""
	if result.Status == TurnStatusCompleted && (needsBackfillMessage || needsBackfillName) {
		if err := r.backfillTurnResult(ctx, &result); err != nil {
			return result, err
		}
	}
	if result.Failure != nil || result.Status == TurnStatusFailed || result.Status == TurnStatusInterrupted {
		return result, turnResultError(result)
	}
	return result, nil
}

func appServerTurnInput(input TurnInput) []map[string]string {
	items := make([]map[string]string, 0, len(input.ImagePaths)+1)
	for _, path := range input.ImagePaths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		items = append(items, map[string]string{
			"type": "localImage",
			"path": path,
		})
	}
	items = append(items, map[string]string{
		"type": "text",
		"text": input.Prompt,
	})
	return items
}

func (r *AppServerRunner) validateAppServerArgs(inputArgs []string) error {
	if len(r.ExtraArgs) == 0 && len(inputArgs) == 0 {
		return nil
	}
	return &Error{Kind: ErrorUnsupported, Message: "codex app-server runner cannot safely translate raw codex CLI arguments yet"}
}

func (r *AppServerRunner) backfillTurnResult(ctx context.Context, result *TurnResult) error {
	threadID := strings.TrimSpace(result.ThreadID)
	if threadID == "" {
		return nil
	}
	includeTurns := strings.TrimSpace(result.FinalAgentMessage) == ""
	params := map[string]any{"threadId": threadID}
	if includeTurns {
		params["includeTurns"] = true
	}
	raw, err := r.request(ctx, appServerMethodThreadRead, params)
	if err != nil {
		return err
	}
	if thread, ok := decodeThread(raw); ok && strings.TrimSpace(result.ThreadName) == "" {
		result.ThreadName = thread.Name
	}
	if includeTurns {
		if message := finalAgentMessageForTurn(raw, result.TurnID); strings.TrimSpace(message) != "" {
			result.FinalAgentMessage = message
		}
	}
	return nil
}

func (r *AppServerRunner) readTurnNotificationsUntilTerminal(ctx context.Context, subscription *appServerTurnSubscriber, result *TurnResult, handler EventHandler) error {
	for !isTerminalTurnStatus(result.Status) {
		line, err := r.readTurnNotification(ctx, subscription)
		if err != nil {
			return err
		}
		if err := applyAppServerNotification(result, line); err != nil {
			return err
		}
		if subscription.turnID == "" && result.TurnID != "" {
			r.setTurnSubscriptionID(subscription, result.TurnID)
		}
		emitAppServerStreamEvent(handler, line)
	}
	return nil
}

func (r *AppServerRunner) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id, delivery := r.registerPendingRequest()
	request := appServerRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}
	line, err := json.Marshal(request)
	if err != nil {
		r.unregisterPendingRequest(id, delivery)
		return nil, &Error{Kind: ErrorParse, Message: "failed to encode app-server request", Err: err}
	}
	if err := r.writeLineLocked(ctx, line); err != nil {
		r.unregisterPendingRequest(id, delivery)
		r.setProtocolFailure(err)
		return nil, err
	}
	select {
	case response := <-delivery:
		if response.err != nil {
			return nil, response.err
		}
		return response.result, nil
	case <-ctx.Done():
		r.unregisterPendingRequest(id, delivery)
		return nil, classifyTransportError(ctx.Err())
	}
}

func appServerMessageIsServerRequest(msg appServerMessage) bool {
	return strings.TrimSpace(msg.Method) != "" && len(bytes.TrimSpace(msg.ID)) > 0
}

func (r *AppServerRunner) serverRequestResponse(ctx context.Context, msg appServerMessage) []byte {
	method := strings.TrimSpace(msg.Method)
	if method == "" {
		method = "server request"
	}
	handler := r.ServerRequestHandler
	if handler == nil {
		handler = AutomaticApprovalHandler{Delay: DefaultApprovalDelay}
	}
	result, handled, err := handler.HandleServerRequest(ctx, method, msg.Params)
	if err != nil {
		response := appServerErrorResponse{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error:   appServerErrorField{Code: json.RawMessage(`-32000`), Message: "approval request was rejected"},
		}
		line, _ := json.Marshal(response)
		return line
	}
	if handled {
		response := appServerResultResponse{JSONRPC: "2.0", ID: msg.ID, Result: result}
		line, err := json.Marshal(response)
		if err != nil {
			return nil
		}
		return line
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
		return nil
	}
	return line
}

func (r *AppServerRunner) writeNotificationLocked(ctx context.Context, method string, params any) error {
	line, err := json.Marshal(appServerNotification{JSONRPC: "2.0", Method: method, Params: params})
	if err != nil {
		return &Error{Kind: ErrorParse, Message: "failed to encode app-server notification", Err: err}
	}
	return r.writeLineLocked(ctx, line)
}

func (r *AppServerRunner) writeLineLocked(ctx context.Context, line []byte) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if r.Transport == nil {
		return &Error{Kind: ErrorLaunch, Message: "codex app-server transport is not configured"}
	}
	if err := r.Transport.WriteLine(ctx, line); err != nil {
		return classifyTransportError(err)
	}
	return nil
}

func (r *AppServerRunner) startProtocolLoopLocked() {
	if r.protocolDone != nil || r.Transport == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.protocolCtx = ctx
	r.protocolCancel = cancel
	r.protocolDone = make(chan struct{})
	r.protocolMu.Lock()
	r.protocolErr = nil
	r.pendingRequests = make(map[int64]chan appServerResponseDelivery)
	r.turnSubscribers = make(map[uint64]*appServerTurnSubscriber)
	r.threadTurnGates = make(map[string]*appServerThreadTurnGate)
	r.serverRequests = make(map[string]*appServerServerRequestState)
	r.completedServerRequests = make(map[string][]byte)
	r.completedServerOrder = nil
	r.protocolMu.Unlock()
	transport := r.Transport
	done := r.protocolDone
	go func() {
		defer close(done)
		for {
			line, err := transport.ReadLine(ctx)
			if err != nil {
				r.setProtocolFailure(err)
				return
			}
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				r.setProtocolFailure(&Error{Kind: ErrorParse, Message: "empty app-server JSON line"})
				return
			}
			var message appServerMessage
			if err := json.Unmarshal(line, &message); err != nil {
				r.setProtocolFailure(&Error{Kind: ErrorParse, Message: "invalid app-server JSON line", Err: err})
				return
			}
			if appServerMessageIsServerRequest(message) {
				r.dispatchServerRequest(ctx, message)
				continue
			}
			if len(bytes.TrimSpace(message.ID)) > 0 {
				r.dispatchResponse(message)
				continue
			}
			if err := r.dispatchNotification(ctx, line, message); err != nil {
				r.setProtocolFailure(err)
				return
			}
		}
	}()
}

func (r *AppServerRunner) registerPendingRequest() (int64, chan appServerResponseDelivery) {
	r.protocolMu.Lock()
	defer r.protocolMu.Unlock()
	r.nextID++
	id := r.nextID
	delivery := make(chan appServerResponseDelivery, 1)
	if r.protocolErr != nil {
		delivery <- appServerResponseDelivery{err: protocolTransportError(r.protocolErr)}
		return id, delivery
	}
	if r.pendingRequests == nil {
		r.pendingRequests = make(map[int64]chan appServerResponseDelivery)
	}
	r.pendingRequests[id] = delivery
	return id, delivery
}

func (r *AppServerRunner) unregisterPendingRequest(id int64, delivery chan appServerResponseDelivery) {
	r.protocolMu.Lock()
	if current, ok := r.pendingRequests[id]; ok && current == delivery {
		delete(r.pendingRequests, id)
	}
	r.protocolMu.Unlock()
}

func (r *AppServerRunner) dispatchResponse(message appServerMessage) {
	id, ok := appServerNumericID(message.ID)
	if !ok {
		return
	}
	r.protocolMu.Lock()
	delivery, ok := r.pendingRequests[id]
	if ok {
		delete(r.pendingRequests, id)
	}
	r.protocolMu.Unlock()
	if !ok {
		// A response may arrive after its caller was canceled. It is safe to
		// discard because request ids are never reused for this transport.
		return
	}
	response := appServerResponseDelivery{result: append(json.RawMessage(nil), message.Result...)}
	if message.Error != nil {
		response.err = appServerResponseError(message.Error)
	}
	delivery <- response
}

func appServerNumericID(raw json.RawMessage) (int64, bool) {
	var num json.Number
	if err := json.Unmarshal(raw, &num); err == nil {
		id, err := strconv.ParseInt(num.String(), 10, 64)
		return id, err == nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		id, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
		return id, err == nil
	}
	return 0, false
}

func (r *AppServerRunner) subscribeTurn(ctx context.Context, threadID string) *appServerTurnSubscriber {
	r.mu.Lock()
	done := r.protocolDone
	r.mu.Unlock()
	subscriber := &appServerTurnSubscriber{
		threadID: strings.TrimSpace(threadID),
		ctx:      ctx,
		frames:   make(chan []byte, 4096),
		done:     done,
	}
	r.protocolMu.Lock()
	r.nextSubscriber++
	subscriberID := r.nextSubscriber
	if r.turnSubscribers == nil {
		r.turnSubscribers = make(map[uint64]*appServerTurnSubscriber)
	}
	r.turnSubscribers[subscriberID] = subscriber
	r.protocolMu.Unlock()
	return subscriber
}

func (r *AppServerRunner) lockThreadTurn(threadID string) func() {
	threadID = strings.TrimSpace(threadID)
	r.protocolMu.Lock()
	if r.threadTurnGates == nil {
		r.threadTurnGates = make(map[string]*appServerThreadTurnGate)
	}
	gate := r.threadTurnGates[threadID]
	if gate == nil {
		gate = &appServerThreadTurnGate{}
		r.threadTurnGates[threadID] = gate
	}
	gate.refs++
	r.protocolMu.Unlock()

	gate.mu.Lock()
	return func() {
		gate.mu.Unlock()
		r.protocolMu.Lock()
		gate.refs--
		if gate.refs == 0 && r.threadTurnGates[threadID] == gate {
			delete(r.threadTurnGates, threadID)
		}
		r.protocolMu.Unlock()
	}
}

func (r *AppServerRunner) unsubscribeTurn(subscriber *appServerTurnSubscriber) {
	if subscriber == nil {
		return
	}
	r.protocolMu.Lock()
	for id, current := range r.turnSubscribers {
		if current == subscriber {
			delete(r.turnSubscribers, id)
			break
		}
	}
	r.protocolMu.Unlock()
}

func (r *AppServerRunner) setTurnSubscriptionID(subscriber *appServerTurnSubscriber, turnID string) {
	if subscriber == nil || strings.TrimSpace(turnID) == "" {
		return
	}
	r.protocolMu.Lock()
	subscriber.turnID = strings.TrimSpace(turnID)
	r.protocolMu.Unlock()
}

func (r *AppServerRunner) readTurnNotification(ctx context.Context, subscriber *appServerTurnSubscriber) ([]byte, error) {
	if subscriber == nil {
		return nil, &Error{Kind: ErrorLaunch, Message: "codex app-server turn subscription is not configured"}
	}
	protocolDone := subscriber.done
	if protocolDone == nil {
		return nil, r.protocolFailure()
	}
	select {
	case line := <-subscriber.frames:
		return line, nil
	case <-protocolDone:
		return nil, r.protocolFailure()
	case <-ctx.Done():
		return nil, classifyTransportError(ctx.Err())
	}
}

func (r *AppServerRunner) dispatchNotification(ctx context.Context, line []byte, message appServerMessage) error {
	threadID, turnID := appServerNotificationRoute(message.Params)
	r.protocolMu.Lock()
	subscribers := make([]*appServerTurnSubscriber, 0, len(r.turnSubscribers))
	for _, subscriber := range r.turnSubscribers {
		if threadID != "" && subscriber.threadID != threadID {
			continue
		}
		if turnID != "" && subscriber.turnID != "" && subscriber.turnID != turnID {
			continue
		}
		subscribers = append(subscribers, subscriber)
	}
	r.protocolMu.Unlock()
	for _, subscriber := range subscribers {
		select {
		case subscriber.frames <- append([]byte(nil), line...):
		case <-ctx.Done():
			return classifyTransportError(ctx.Err())
		default:
			return &Error{Kind: ErrorLaunch, Message: "codex app-server turn notification buffer exceeded"}
		}
	}
	return nil
}

func appServerNotificationRoute(raw json.RawMessage) (string, string) {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return "", ""
	}
	return appServerRouteValue(value)
}

func appServerRouteValue(value any) (string, string) {
	object, ok := value.(map[string]any)
	if !ok {
		return "", ""
	}
	threadID := firstStringValue(object, "threadId", "thread_id")
	turnID := firstStringValue(object, "turnId", "turn_id")
	if nested, ok := object["thread"].(map[string]any); ok && threadID == "" {
		threadID = firstStringValue(nested, "id", "threadId", "thread_id")
	}
	if nested, ok := object["turn"].(map[string]any); ok && turnID == "" {
		turnID = firstStringValue(nested, "id", "turnId", "turn_id")
	}
	for _, key := range []string{"event", "msg", "payload", "request"} {
		nestedThread, nestedTurn := appServerRouteValue(object[key])
		if threadID == "" {
			threadID = nestedThread
		}
		if turnID == "" {
			turnID = nestedTurn
		}
	}
	return threadID, turnID
}

func firstStringValue(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := object[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (r *AppServerRunner) dispatchServerRequest(ctx context.Context, message appServerMessage) {
	key := string(bytes.TrimSpace(message.ID)) + "\x00" + strings.TrimSpace(message.Method)
	r.protocolMu.Lock()
	if response, ok := r.completedServerRequests[key]; ok {
		response = append([]byte(nil), response...)
		r.protocolMu.Unlock()
		r.serverWG.Add(1)
		go func() {
			defer r.serverWG.Done()
			_ = r.writeLineLocked(ctx, response)
		}()
		return
	}
	if state, ok := r.serverRequests[key]; ok {
		r.protocolMu.Unlock()
		r.serverWG.Add(1)
		go func() {
			defer r.serverWG.Done()
			select {
			case <-ctx.Done():
				return
			case <-state.done:
				if len(state.response) > 0 {
					_ = r.writeLineLocked(ctx, state.response)
				}
			}
		}()
		return
	}
	state := &appServerServerRequestState{done: make(chan struct{})}
	r.serverRequests[key] = state
	r.protocolMu.Unlock()

	r.serverWG.Add(1)
	go func() {
		defer r.serverWG.Done()
		requestCtx, cancel := r.serverRequestContext(ctx, message.Params)
		defer cancel()
		response := r.serverRequestResponse(requestCtx, message)
		r.protocolMu.Lock()
		state.response = append([]byte(nil), response...)
		r.rememberCompletedServerRequestLocked(key, response)
		close(state.done)
		r.protocolMu.Unlock()
		if len(response) > 0 {
			_ = r.writeLineLocked(ctx, response)
		}
		r.protocolMu.Lock()
		if current, ok := r.serverRequests[key]; ok && current == state {
			delete(r.serverRequests, key)
		}
		r.protocolMu.Unlock()
	}()
}

func (r *AppServerRunner) rememberCompletedServerRequestLocked(key string, response []byte) {
	if len(response) == 0 {
		return
	}
	if r.completedServerRequests == nil {
		r.completedServerRequests = make(map[string][]byte)
	}
	if _, exists := r.completedServerRequests[key]; exists {
		return
	}
	r.completedServerRequests[key] = append([]byte(nil), response...)
	r.completedServerOrder = append(r.completedServerOrder, key)
	if len(r.completedServerOrder) <= appServerCompletedRequestLimit {
		return
	}
	oldest := r.completedServerOrder[0]
	r.completedServerOrder = r.completedServerOrder[1:]
	delete(r.completedServerRequests, oldest)
}

func (r *AppServerRunner) serverRequestContext(protocolCtx context.Context, params json.RawMessage) (context.Context, context.CancelFunc) {
	threadID, turnID := appServerNotificationRoute(params)
	if threadID == "" && turnID == "" {
		return context.WithCancel(protocolCtx)
	}
	r.protocolMu.Lock()
	var turnCtx context.Context
	for _, subscriber := range r.turnSubscribers {
		if threadID != "" && subscriber.threadID != threadID {
			continue
		}
		if turnID != "" && subscriber.turnID != "" && subscriber.turnID != turnID {
			continue
		}
		turnCtx = subscriber.ctx
		break
	}
	r.protocolMu.Unlock()
	requestCtx, cancel := context.WithCancel(protocolCtx)
	if turnCtx == nil {
		return requestCtx, cancel
	}
	stop := context.AfterFunc(turnCtx, cancel)
	return requestCtx, func() {
		stop()
		cancel()
	}
}

func (r *AppServerRunner) setProtocolFailure(err error) {
	r.protocolMu.Lock()
	if r.protocolErr == nil {
		r.protocolErr = err
	}
	failure := protocolTransportError(r.protocolErr)
	pending := r.pendingRequests
	r.pendingRequests = make(map[int64]chan appServerResponseDelivery)
	r.protocolMu.Unlock()
	for _, delivery := range pending {
		select {
		case delivery <- appServerResponseDelivery{err: failure}:
		default:
		}
	}
}

func (r *AppServerRunner) protocolFailure() error {
	r.protocolMu.Lock()
	err := r.protocolErr
	r.protocolMu.Unlock()
	if err == nil {
		err = io.EOF
	}
	return protocolTransportError(err)
}

func protocolTransportError(err error) error {
	var runnerErr *Error
	if errors.As(err, &runnerErr) {
		return runnerErr
	}
	return classifyTransportError(err)
}

func (r *AppServerRunner) closeTransportLocked() error {
	if r.protocolCancel != nil {
		r.protocolCancel()
	}
	if r.protocolDone != nil {
		<-r.protocolDone
	}
	r.serverWG.Wait()
	var err error
	r.writeMu.Lock()
	if r.Transport != nil {
		err = r.Transport.Close()
		r.Transport = nil
	}
	r.writeMu.Unlock()
	r.protocolCtx = nil
	r.protocolCancel = nil
	r.protocolDone = nil
	r.protocolMu.Lock()
	r.pendingRequests = nil
	r.turnSubscribers = nil
	r.threadTurnGates = nil
	r.serverRequests = nil
	r.completedServerRequests = nil
	r.completedServerOrder = nil
	r.protocolMu.Unlock()
	r.ready = false
	return err
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

type appServerResultResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
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
		Name          string `json:"name"`
		ThreadName    string `json:"thread_name"`
		ThreadName2   string `json:"threadName"`
		Title         string `json:"title"`
		Thread        struct {
			ID            string `json:"id"`
			ThreadID      string `json:"thread_id"`
			ThreadIDCamel string `json:"threadId"`
			Name          string `json:"name"`
			ThreadName    string `json:"thread_name"`
			ThreadName2   string `json:"threadName"`
			Title         string `json:"title"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return Thread{}, false
	}
	id := firstNonEmpty(envelope.ThreadIDCamel, envelope.ThreadID, envelope.ID, envelope.Thread.ThreadIDCamel, envelope.Thread.ThreadID, envelope.Thread.ID)
	if id == "" {
		return Thread{}, false
	}
	name := firstNonEmpty(envelope.Thread.Name, envelope.Thread.ThreadName2, envelope.Thread.ThreadName, envelope.Thread.Title, envelope.Name, envelope.ThreadName2, envelope.ThreadName, envelope.Title)
	return Thread{ID: id, Name: strings.TrimSpace(name)}, true
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
			Phase:            params.Item.Phase,
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
	case "thread/compacted", "context_compaction", "context_compacted":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Thread   struct {
				ID string `json:"id"`
			} `json:"thread"`
			Turn codexTurn `json:"turn"`
		}
		if json.Unmarshal(msg.Params, &params) != nil {
			return StreamEvent{}, false
		}
		return StreamEvent{
			Kind:     StreamEventContextCompacted,
			ThreadID: firstNonEmpty(params.ThreadID, params.Thread.ID),
			TurnID:   firstNonEmpty(params.TurnID, params.Turn.ID),
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
				Message           string          `json:"message"`
				Code              string          `json:"code"`
				AdditionalDetails string          `json:"additionalDetails"`
				CodexErrorInfo    json.RawMessage `json:"codexErrorInfo"`
			} `json:"error"`
		}
		if json.Unmarshal(msg.Params, &params) != nil {
			return StreamEvent{}, false
		}
		failure := &TurnFailure{
			Code:    firstNonEmpty(params.Error.Code, params.Code, codexErrorInfoCode(params.Error.CodexErrorInfo)),
			Message: firstNonEmpty(params.Error.Message, params.Message, params.Error.AdditionalDetails),
		}
		if failure.Message == "" {
			if params.WillRetry {
				failure.Message = "Codex stream disconnected; reconnecting"
			} else {
				failure.Message = "Codex turn failed"
			}
		}
		if params.WillRetry {
			return StreamEvent{
				Kind:      StreamEventStreamRetry,
				ThreadID:  params.ThreadID,
				TurnID:    params.TurnID,
				Failure:   failure,
				WillRetry: true,
				Raw:       append([]byte(nil), bytes.TrimSpace(line)...),
			}, true
		}
		return StreamEvent{
			Kind:     StreamEventTurnFailed,
			ThreadID: params.ThreadID,
			TurnID:   params.TurnID,
			Failure:  failure,
			Raw:      append([]byte(nil), bytes.TrimSpace(line)...),
		}, true
	default:
		if event, ok := appServerParamsStreamEvent(msg, line); ok {
			return event, true
		}
		return StreamEvent{}, false
	}
}

func appServerParamsStreamEvent(msg appServerMessage, line []byte) (StreamEvent, bool) {
	if len(bytes.TrimSpace(msg.Params)) == 0 {
		return StreamEvent{}, false
	}
	if event, ok, err := ParseStreamEventJSONL(msg.Params); err == nil && ok {
		event.Raw = append([]byte(nil), bytes.TrimSpace(line)...)
		return event, true
	}
	if !strings.EqualFold(strings.TrimSpace(msg.Method), "event_msg") &&
		!strings.EqualFold(strings.TrimSpace(msg.Method), "response_item") {
		return StreamEvent{}, false
	}
	wrapped, err := json.Marshal(codexEvent{Type: msg.Method, Payload: msg.Params})
	if err != nil {
		return StreamEvent{}, false
	}
	event, ok, err := ParseStreamEventJSONL(wrapped)
	if err != nil || !ok {
		return StreamEvent{}, false
	}
	event.Raw = append([]byte(nil), bytes.TrimSpace(line)...)
	return event, true
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
		ThreadName    string `json:"thread_name"`
		ThreadName2   string `json:"threadName"`
		Name          string `json:"name"`
		Title         string `json:"title"`
		TurnID        string `json:"turn_id"`
		TurnIDCamel   string `json:"turnId"`
		Thread        struct {
			ID          string `json:"id"`
			Name        string `json:"name"`
			ThreadName  string `json:"thread_name"`
			ThreadName2 string `json:"threadName"`
			Title       string `json:"title"`
		} `json:"thread"`
		Turn struct {
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
	if envelope.Thread.ID != "" && result.ThreadID == "" {
		result.ThreadID = envelope.Thread.ID
	}
	if name := firstNonEmpty(envelope.Thread.Name, envelope.Thread.ThreadName2, envelope.Thread.ThreadName, envelope.Thread.Title, envelope.ThreadName2, envelope.ThreadName, envelope.Name, envelope.Title); name != "" {
		result.ThreadName = strings.TrimSpace(name)
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
				ID          string `json:"id"`
				Name        string `json:"name"`
				ThreadName  string `json:"thread_name"`
				ThreadName2 string `json:"threadName"`
				Title       string `json:"title"`
			} `json:"thread"`
		}
		if json.Unmarshal(msg.Params, &params) == nil {
			if id := firstNonEmpty(params.ThreadID, params.Thread.ID); id != "" {
				result.ThreadID = id
			}
			if name := firstNonEmpty(params.Thread.Name, params.Thread.ThreadName2, params.Thread.ThreadName, params.Thread.Title); name != "" {
				result.ThreadName = strings.TrimSpace(name)
			}
			return result.ThreadID != "" || result.ThreadName != ""
		}
	case "thread/name/updated":
		var params struct {
			ThreadID    string `json:"threadId"`
			ThreadID2   string `json:"thread_id"`
			ThreadName  string `json:"threadName"`
			ThreadName2 string `json:"thread_name"`
			Name        string `json:"name"`
			Title       string `json:"title"`
			Thread      struct {
				ID          string `json:"id"`
				ThreadID    string `json:"thread_id"`
				ThreadID2   string `json:"threadId"`
				Name        string `json:"name"`
				ThreadName  string `json:"thread_name"`
				ThreadName2 string `json:"threadName"`
				Title       string `json:"title"`
			} `json:"thread"`
		}
		if json.Unmarshal(msg.Params, &params) == nil {
			if id := firstNonEmpty(params.ThreadID, params.ThreadID2, params.Thread.ThreadID2, params.Thread.ThreadID, params.Thread.ID); id != "" {
				result.ThreadID = id
			}
			if name := firstNonEmpty(params.ThreadName, params.ThreadName2, params.Name, params.Title, params.Thread.Name, params.Thread.ThreadName2, params.Thread.ThreadName, params.Thread.Title); name != "" {
				result.ThreadName = strings.TrimSpace(name)
			}
			return result.ThreadID != "" || result.ThreadName != ""
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

	TotalTokensSnake           int64 `json:"total_tokens"`
	InputTokensSnake           int64 `json:"input_tokens"`
	CachedInputTokensSnake     int64 `json:"cached_input_tokens"`
	OutputTokensSnake          int64 `json:"output_tokens"`
	ReasoningOutputTokensSnake int64 `json:"reasoning_output_tokens"`
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
		b.OutputTokensSnake == 0 &&
		b.ReasoningOutputTokensSnake == 0
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
	if value := firstNonZeroInt64(breakdown.ReasoningOutputTokens, breakdown.ReasoningOutputTokensSnake); value != 0 {
		dst.ReasoningOutputTokens = value
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
