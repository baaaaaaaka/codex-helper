package codexrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestAppServerRunnerParallelTurnsInterleavedResponsesAndApprovals(t *testing.T) {
	transport := newMultiplexAppServerTransport()
	handler := newParallelApprovalHandler(2)
	runner := &AppServerRunner{
		Transport:            transport,
		ServerRequestHandler: handler,
	}
	defer runner.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type turnOutcome struct {
		result TurnResult
		err    error
	}
	results := make(chan turnOutcome, 2)
	for _, threadID := range []string{"thread-a", "thread-b"} {
		threadID := threadID
		go func() {
			result, err := runner.StartTurn(ctx, StartTurnInput{
				ThreadID:  threadID,
				TurnInput: TurnInput{Prompt: "run " + threadID},
			})
			results <- turnOutcome{result: result, err: err}
		}()
	}

	got := make(map[string]TurnResult)
	for range 2 {
		outcome := <-results
		if outcome.err != nil {
			t.Fatalf("parallel turn failed: result=%#v err=%v", outcome.result, outcome.err)
		}
		got[outcome.result.ThreadID] = outcome.result
	}
	for _, threadID := range []string{"thread-a", "thread-b"} {
		result := got[threadID]
		if result.Status != TurnStatusCompleted || result.FinalAgentMessage != "done "+threadID {
			t.Fatalf("result for %s = %#v", threadID, result)
		}
	}
	if handler.maxConcurrent() != 2 {
		t.Fatalf("approval handlers max concurrency = %d, want 2", handler.maxConcurrent())
	}

	deadline := time.Now().Add(time.Second)
	for {
		runner.protocolMu.Lock()
		pending := len(runner.pendingRequests)
		subscribers := len(runner.turnSubscribers)
		serverRequests := len(runner.serverRequests)
		runner.protocolMu.Unlock()
		if pending == 0 && subscribers == 0 && serverRequests == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("protocol state leaked: pending=%d subscribers=%d serverRequests=%d", pending, subscribers, serverRequests)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAppServerRunnerCancelDuringApprovalDelayRejectsLateApproval(t *testing.T) {
	transport := newCancelApprovalTransport()
	handler := &cancelAwareApprovalHandler{started: make(chan struct{}), delay: time.Second}
	runner := &AppServerRunner{
		Transport:            transport,
		ServerRequestHandler: handler,
	}
	defer runner.Close()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(ctx, StartTurnInput{
			ThreadID:  "thread-cancel",
			TurnInput: TurnInput{Prompt: "cancel me"},
		})
		result <- err
	}()
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("approval request was not sent")
	}
	cancel()
	select {
	case err := <-result:
		if !IsKind(err, ErrorCanceled) {
			t.Fatalf("turn error = %v, want canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("turn did not stop after cancellation")
	}
	select {
	case raw := <-transport.approvalResponse:
		var response appServerMessage
		if json.Unmarshal(raw, &response) != nil || response.Error == nil || len(response.Result) != 0 {
			t.Fatalf("approval response = %s, want fail-closed error", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled approval did not receive a fail-closed response")
	}
}

func TestAppServerRunnerDisconnectDuringApprovalDelayFailsPendingTurn(t *testing.T) {
	transport := newCancelApprovalTransport()
	handler := &cancelAwareApprovalHandler{started: make(chan struct{}), delay: 10 * time.Second}
	runner := &AppServerRunner{Transport: transport, ServerRequestHandler: handler}
	result := make(chan error, 1)
	go func() {
		_, err := runner.StartTurn(context.Background(), StartTurnInput{
			ThreadID:  "thread-disconnect",
			TurnInput: TurnInput{Prompt: "disconnect me"},
		})
		result <- err
	}()
	select {
	case <-handler.started:
	case <-time.After(time.Second):
		t.Fatal("approval request was not sent")
	}
	closed := make(chan error, 1)
	go func() { closed <- runner.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("runner close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner close waited for the full approval delay")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("pending turn succeeded after app-server disconnect")
		}
	case <-time.After(time.Second):
		t.Fatal("pending turn was not failed after app-server disconnect")
	}
}

func TestAppServerRunnerSerializesTurnsOnSameThread(t *testing.T) {
	transport := newSameThreadTurnTransport()
	runner := &AppServerRunner{Transport: transport}
	defer runner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := runner.StartTurn(ctx, StartTurnInput{
				ThreadID:  "shared-thread",
				TurnInput: TurnInput{Prompt: "run"},
			})
			results <- err
		}()
	}

	if sequence := <-transport.started; sequence != 1 {
		t.Fatalf("first turn sequence = %d, want 1", sequence)
	}
	select {
	case sequence := <-transport.started:
		t.Fatalf("same-thread turn %d started before turn 1 completed", sequence)
	case <-time.After(50 * time.Millisecond):
	}
	transport.complete(ctx, 1)
	if err := <-results; err != nil {
		t.Fatalf("first same-thread turn failed: %v", err)
	}
	if sequence := <-transport.started; sequence != 2 {
		t.Fatalf("second turn sequence = %d, want 2", sequence)
	}
	transport.complete(ctx, 2)
	if err := <-results; err != nil {
		t.Fatalf("second same-thread turn failed: %v", err)
	}
}

func TestAppServerRunnerDuplicateApprovalRequestIsIdempotent(t *testing.T) {
	transport := &recordingAppServerTransport{}
	handler := &countingApprovalHandler{release: make(chan struct{})}
	runner := &AppServerRunner{
		Transport:               transport,
		ServerRequestHandler:    handler,
		serverRequests:          make(map[string]*appServerServerRequestState),
		completedServerRequests: make(map[string][]byte),
	}
	message := appServerMessage{
		ID:     json.RawMessage(`90`),
		Method: "item/commandExecution/requestApproval",
		Params: json.RawMessage(`{"threadId":"thread","turnId":"turn"}`),
	}
	runner.dispatchServerRequest(context.Background(), message)
	runner.dispatchServerRequest(context.Background(), message)
	handler.waitForCalls(t, 1)
	close(handler.release)
	runner.serverWG.Wait()

	// A duplicate that arrives after completion is answered from the bounded
	// response cache instead of applying the approval policy a second time.
	runner.dispatchServerRequest(context.Background(), message)
	runner.serverWG.Wait()
	if calls := handler.callCount(); calls != 1 {
		t.Fatalf("approval handler calls = %d, want 1", calls)
	}
	if writes := transport.writeCount(); writes != 3 {
		t.Fatalf("approval response writes = %d, want 3", writes)
	}
}

func TestAppServerRunnerApprovalStateSoakIsBounded(t *testing.T) {
	transport := &recordingAppServerTransport{}
	handler := &countingApprovalHandler{}
	runner := &AppServerRunner{
		Transport:               transport,
		ServerRequestHandler:    handler,
		serverRequests:          make(map[string]*appServerServerRequestState),
		completedServerRequests: make(map[string][]byte),
	}
	for id := 1; id <= 1000; id++ {
		runner.dispatchServerRequest(context.Background(), appServerMessage{
			ID:     json.RawMessage(fmt.Sprintf("%d", id)),
			Method: "item/commandExecution/requestApproval",
			Params: json.RawMessage(`{"threadId":"thread","turnId":"turn"}`),
		})
		runner.serverWG.Wait()
	}
	runner.protocolMu.Lock()
	active := len(runner.serverRequests)
	completed := len(runner.completedServerRequests)
	order := len(runner.completedServerOrder)
	runner.protocolMu.Unlock()
	if active != 0 {
		t.Fatalf("active server requests after soak = %d, want 0", active)
	}
	if completed != appServerCompletedRequestLimit || order != appServerCompletedRequestLimit {
		t.Fatalf("completed cache after soak = (%d,%d), want (%d,%d)", completed, order, appServerCompletedRequestLimit, appServerCompletedRequestLimit)
	}
}

type cancelAwareApprovalHandler struct {
	started chan struct{}
	once    sync.Once
	delay   time.Duration
}

type countingApprovalHandler struct {
	mu      sync.Mutex
	calls   int
	release chan struct{}
}

func (h *countingApprovalHandler) HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()
	if h.release != nil {
		select {
		case <-ctx.Done():
			return nil, true, ctx.Err()
		case <-h.release:
		}
	}
	return automaticApprovalResult(method, params)
}

func (h *countingApprovalHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func (h *countingApprovalHandler) waitForCalls(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for h.callCount() != want {
		if time.Now().After(deadline) {
			t.Fatalf("approval handler calls = %d, want %d", h.callCount(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

type recordingAppServerTransport struct {
	mu     sync.Mutex
	writes int
}

func (t *recordingAppServerTransport) WriteLine(context.Context, []byte) error {
	t.mu.Lock()
	t.writes++
	t.mu.Unlock()
	return nil
}

func (t *recordingAppServerTransport) ReadLine(context.Context) ([]byte, error) {
	return nil, io.EOF
}

func (t *recordingAppServerTransport) Close() error { return nil }

func (t *recordingAppServerTransport) writeCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.writes
}

func (h *cancelAwareApprovalHandler) HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	h.once.Do(func() { close(h.started) })
	return (AutomaticApprovalHandler{Delay: h.delay}).HandleServerRequest(ctx, method, params)
}

type parallelApprovalHandler struct {
	want    int
	mu      sync.Mutex
	active  int
	max     int
	arrived int
	release chan struct{}
}

func newParallelApprovalHandler(want int) *parallelApprovalHandler {
	return &parallelApprovalHandler{want: want, release: make(chan struct{})}
}

func (h *parallelApprovalHandler) HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	result, handled, err := automaticApprovalResult(method, params)
	if err != nil || !handled {
		return result, handled, err
	}
	h.mu.Lock()
	h.active++
	if h.active > h.max {
		h.max = h.active
	}
	h.arrived++
	if h.arrived == h.want {
		close(h.release)
	}
	h.mu.Unlock()
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-h.release:
	}
	h.mu.Lock()
	h.active--
	h.mu.Unlock()
	return result, true, err
}

func (h *parallelApprovalHandler) maxConcurrent() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.max
}

type multiplexTurnRequest struct {
	id       int64
	threadID string
}

type multiplexAppServerTransport struct {
	mu                sync.Mutex
	reads             chan []byte
	closed            chan struct{}
	closeOnce         sync.Once
	turns             []multiplexTurnRequest
	approvalResponses int
}

func newMultiplexAppServerTransport() *multiplexAppServerTransport {
	return &multiplexAppServerTransport{
		reads:  make(chan []byte, 32),
		closed: make(chan struct{}),
	}
}

func (t *multiplexAppServerTransport) WriteLine(ctx context.Context, line []byte) error {
	var message appServerMessage
	if err := json.Unmarshal(line, &message); err != nil {
		return err
	}
	if message.Method == "" {
		id, ok := appServerNumericID(message.ID)
		if ok && (id == 90 || id == 91) {
			t.mu.Lock()
			t.approvalResponses++
			complete := t.approvalResponses == 2
			turns := append([]multiplexTurnRequest(nil), t.turns...)
			t.mu.Unlock()
			if complete {
				for _, turn := range turns {
					t.send(ctx, fmt.Sprintf(`{"method":"future/unknown","params":{"threadId":%q,"turnId":%q,"newField":true}}`, turn.threadID, "turn-"+turn.threadID))
					t.send(ctx, fmt.Sprintf(`{"method":"item/completed","params":{"threadId":%q,"turnId":%q,"item":{"id":"item","type":"agentMessage","text":%q}}}`, turn.threadID, "turn-"+turn.threadID, "done "+turn.threadID))
					t.send(ctx, fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":%q,"turn":{"id":%q,"status":"completed","items":[]}}}`, turn.threadID, "turn-"+turn.threadID))
				}
			}
		}
		return nil
	}
	id, ok := appServerNumericID(message.ID)
	if !ok && message.Method != appServerMethodInitialized {
		return fmt.Errorf("request %s did not include numeric id", message.Method)
	}
	switch message.Method {
	case appServerMethodInitialized:
		return nil
	case appServerMethodInitialize:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{}}`, id))
	case appServerMethodThreadList:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"data":[]}}`, id))
	case appServerMethodTurnStart:
		var params struct {
			ThreadID string `json:"threadId"`
		}
		if err := json.Unmarshal(message.Params, &params); err != nil {
			return err
		}
		t.mu.Lock()
		t.turns = append(t.turns, multiplexTurnRequest{id: id, threadID: params.ThreadID})
		ready := len(t.turns) == 2
		turns := append([]multiplexTurnRequest(nil), t.turns...)
		t.mu.Unlock()
		if ready {
			for index := len(turns) - 1; index >= 0; index-- {
				turn := turns[index]
				t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":%q,"status":"inProgress","items":[]}}}`, turn.id, "turn-"+turn.threadID))
			}
			t.send(ctx, `{"jsonrpc":"2.0","id":90,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread-a","turnId":"turn-thread-a"}}`)
			t.send(ctx, `{"jsonrpc":"2.0","id":91,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread-b","turnId":"turn-thread-b"}}`)
		}
	default:
		return fmt.Errorf("unexpected method %q", message.Method)
	}
	return nil
}

func (t *multiplexAppServerTransport) send(ctx context.Context, line string) {
	select {
	case t.reads <- []byte(line):
	case <-ctx.Done():
	case <-t.closed:
	}
}

func (t *multiplexAppServerTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		return line, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *multiplexAppServerTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

type cancelApprovalTransport struct {
	reads            chan []byte
	closed           chan struct{}
	closeOnce        sync.Once
	approvalSent     chan struct{}
	approvalOnce     sync.Once
	approvalResponse chan []byte
}

func newCancelApprovalTransport() *cancelApprovalTransport {
	return &cancelApprovalTransport{
		reads:            make(chan []byte, 8),
		closed:           make(chan struct{}),
		approvalSent:     make(chan struct{}),
		approvalResponse: make(chan []byte, 1),
	}
}

func (t *cancelApprovalTransport) WriteLine(ctx context.Context, line []byte) error {
	var message appServerMessage
	if err := json.Unmarshal(line, &message); err != nil {
		return err
	}
	if message.Method == "" {
		if id, ok := appServerNumericID(message.ID); ok && id == 90 {
			t.approvalResponse <- append([]byte(nil), line...)
		}
		return nil
	}
	id, _ := appServerNumericID(message.ID)
	switch message.Method {
	case appServerMethodInitialized:
		return nil
	case appServerMethodInitialize:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{}}`, id))
	case appServerMethodThreadList:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"data":[]}}`, id))
	case appServerMethodTurnStart:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":"turn-cancel","status":"inProgress","items":[]}}}`, id))
		t.send(ctx, `{"jsonrpc":"2.0","id":90,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread-cancel","turnId":"turn-cancel"}}`)
		t.approvalOnce.Do(func() { close(t.approvalSent) })
	}
	return nil
}

func (t *cancelApprovalTransport) send(ctx context.Context, line string) {
	select {
	case t.reads <- []byte(line):
	case <-ctx.Done():
	case <-t.closed:
	}
}

func (t *cancelApprovalTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		return line, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *cancelApprovalTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

type sameThreadTurnTransport struct {
	mu        sync.Mutex
	reads     chan []byte
	closed    chan struct{}
	closeOnce sync.Once
	sequence  int
	started   chan int
}

func newSameThreadTurnTransport() *sameThreadTurnTransport {
	return &sameThreadTurnTransport{
		reads:   make(chan []byte, 16),
		closed:  make(chan struct{}),
		started: make(chan int, 2),
	}
}

func (t *sameThreadTurnTransport) WriteLine(ctx context.Context, line []byte) error {
	var message appServerMessage
	if err := json.Unmarshal(line, &message); err != nil {
		return err
	}
	if message.Method == appServerMethodInitialized {
		return nil
	}
	id, ok := appServerNumericID(message.ID)
	if !ok {
		return fmt.Errorf("request %s did not include numeric id", message.Method)
	}
	switch message.Method {
	case appServerMethodInitialize:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{}}`, id))
	case appServerMethodThreadList:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"data":[]}}`, id))
	case appServerMethodTurnStart:
		t.mu.Lock()
		t.sequence++
		sequence := t.sequence
		t.mu.Unlock()
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":"turn-%d","status":"inProgress","items":[]}}}`, id, sequence))
		select {
		case t.started <- sequence:
		case <-ctx.Done():
			return ctx.Err()
		case <-t.closed:
			return io.EOF
		}
	default:
		return fmt.Errorf("unexpected method %q", message.Method)
	}
	return nil
}

func (t *sameThreadTurnTransport) complete(ctx context.Context, sequence int) {
	t.send(ctx, fmt.Sprintf(`{"method":"item/completed","params":{"threadId":"shared-thread","turnId":"turn-%d","item":{"id":"item-%d","type":"agentMessage","text":"done-%d"}}}`, sequence, sequence, sequence))
	t.send(ctx, fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":"shared-thread","turn":{"id":"turn-%d","status":"completed","items":[]}}}`, sequence))
}

func (t *sameThreadTurnTransport) send(ctx context.Context, line string) {
	select {
	case t.reads <- []byte(line):
	case <-ctx.Done():
	case <-t.closed:
	}
}

func (t *sameThreadTurnTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		return line, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *sameThreadTurnTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}
