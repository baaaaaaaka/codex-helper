package codexrunner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAppServerRunnerInitializeHandshakeAndThreadListProbe(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{"userAgent":"codex-helper-test/0","codexHome":"/tmp/codex-home","platformFamily":"unix","platformOs":"linux"}}`,
		`{"method":"configWarning","params":{"message":"ignored warning"}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"data":[{"id":"thread-a"},{"id":"thread-b"}],"nextCursor":null,"backwardsCursor":"cursor-a"}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.ListThreads(context.Background(), ListThreadsOptions{WorkingDir: "/work", Limit: 2})
	if err != nil {
		t.Fatalf("ListThreads error: %v", err)
	}
	want := []Thread{{ID: "thread-a"}, {ID: "thread-b"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("threads = %#v, want %#v", got, want)
	}

	writes := transport.decodedWrites(t)
	assertMethod(t, writes[0], "initialize")
	assertJSONRPC(t, writes[0])
	assertParamNil(t, writes[0], "capabilities")
	assertMethod(t, writes[1], "initialized")
	assertJSONRPC(t, writes[1])
	assertNoID(t, writes[1])
	assertMethod(t, writes[2], "thread/list")
	assertParamNumber(t, writes[2], "limit", 1)
	assertMethod(t, writes[3], "thread/list")
	assertParamString(t, writes[3], "cwd", "/work")
	assertParamNumber(t, writes[3], "limit", 2)
	for _, write := range writes {
		assertJSONRPC(t, write)
	}
}

func TestAppServerRunnerStartThreadEncodesThreadStartAndTurnStart(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-new","turn":{"id":"turn-1"}}}`,
		`{"method":"item/agentMessage/delta","params":{"threadId":"thread-new","turnId":"turn-1","itemId":"item-1","delta":"do"}}`,
		`{"method":"item/agentMessage/delta","params":{"threadId":"thread-new","turnId":"turn-1","itemId":"item-1","delta":"ne"}}`,
		`{"method":"thread/tokenUsage/updated","params":{"threadId":"thread-new","turnId":"turn-1","tokenUsage":{"total":{"totalTokens":100,"inputTokens":80,"cachedInputTokens":20,"outputTokens":20,"reasoningOutputTokens":0},"last":{"totalTokens":30,"inputTokens":20,"cachedInputTokens":7,"outputTokens":10,"reasoningOutputTokens":0}}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turn":{"id":"turn-1","status":"completed","items":[]}}}`,
	)
	runner := &AppServerRunner{
		Transport:  transport,
		Command:    "/managed/codex",
		WorkingDir: "/work",
		Timeout:    time.Minute,
	}

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.ThreadID != "thread-new" || got.TurnID != "turn-1" || got.Status != TurnStatusCompleted {
		t.Fatalf("unexpected result: %#v", got)
	}
	if got.FinalAgentMessage != "done" || got.Usage.CachedInputTokens != 7 {
		t.Fatalf("notification result not parsed: %#v", got)
	}

	writes := transport.decodedWrites(t)
	assertMethod(t, writes[3], "thread/start")
	assertParamString(t, writes[3], "cwd", "/work")
	assertParamAbsent(t, writes[3], "extra_args")
	assertMethod(t, writes[4], "turn/start")
	assertParamString(t, writes[4], "threadId", "thread-new")
	assertTextInput(t, writes[4], "hello")
	assertParamString(t, writes[4], "cwd", "/work")
	assertJSONRPC(t, writes[4])
}

func TestAppServerRunnerResumeThreadEncodesResumeAndTurnStart(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-existing"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-resume","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-existing","turn":{"id":"turn-resume"}}}`,
		`{"method":"item/completed","params":{"threadId":"thread-existing","turnId":"turn-resume","item":{"id":"item-1","type":"agentMessage","text":"resumed"}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-existing","turn":{"id":"turn-resume","status":"completed","items":[]}}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.ResumeThread(context.Background(), "thread-existing", TurnInput{Prompt: "continue"})
	if err != nil {
		t.Fatalf("ResumeThread error: %v", err)
	}
	if got.ThreadID != "thread-existing" || got.TurnID != "turn-resume" || got.FinalAgentMessage != "resumed" {
		t.Fatalf("unexpected result: %#v", got)
	}

	writes := transport.decodedWrites(t)
	assertMethod(t, writes[3], "thread/resume")
	assertParamString(t, writes[3], "threadId", "thread-existing")
	assertMethod(t, writes[4], "turn/start")
	assertParamString(t, writes[4], "threadId", "thread-existing")
	assertTextInput(t, writes[4], "continue")
}

func TestAppServerRunnerStreamsNotifications(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-new","turnId":"turn-1"}}`,
		`{"method":"item/completed","params":{"threadId":"thread-new","turnId":"turn-1","item":{"id":"item-1","type":"agentMessage","text":"checking tests"}}}`,
		`{"method":"item/started","params":{"threadId":"thread-new","turnId":"turn-1","item":{"id":"item-2","type":"commandExecution","command":"go test ./..."}}}`,
		`{"method":"item/completed","params":{"threadId":"thread-new","turnId":"turn-1","item":{"id":"item-2","type":"commandExecution","command":"go test ./...","aggregatedOutput":"PASS\n","exitCode":0,"status":"completed"}}}`,
		`{"method":"item/completed","params":{"threadId":"thread-new","turnId":"turn-1","item":{"id":"item-3","type":"agentMessage","text":"done"}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turnId":"turn-1","turn":{"id":"turn-1","status":"completed","items":[]}}}`,
	)
	runner := NewAppServerRunner(transport)
	var events []StreamEvent

	got, err := runner.StartThread(context.Background(), TurnInput{
		Prompt: "hello",
		EventHandler: func(event StreamEvent) {
			events = append(events, event)
		},
	})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.FinalAgentMessage != "done" {
		t.Fatalf("final message = %q, want done", got.FinalAgentMessage)
	}
	if len(events) != 6 {
		t.Fatalf("events len = %d, want 6: %#v", len(events), events)
	}
	if events[0].Kind != StreamEventTurnStarted || events[0].TurnID != "turn-1" {
		t.Fatalf("turn event = %#v", events[0])
	}
	if events[1].Kind != StreamEventAgentMessage || events[1].Text != "checking tests" {
		t.Fatalf("agent event = %#v", events[1])
	}
	if events[2].Kind != StreamEventCommandStarted || events[2].Command != "go test ./..." {
		t.Fatalf("command start event = %#v", events[2])
	}
	if events[3].Kind != StreamEventCommandCompleted || events[3].AggregatedOutput != "PASS\n" || events[3].ExitCode == nil || *events[3].ExitCode != 0 {
		t.Fatalf("command completed event = %#v", events[3])
	}
	if events[4].Kind != StreamEventAgentMessage || events[4].Text != "done" {
		t.Fatalf("final agent event = %#v", events[4])
	}
	if events[5].Kind != StreamEventTurnCompleted || events[5].TurnID != "turn-1" {
		t.Fatalf("turn completed event = %#v", events[5])
	}
}

func TestAppServerRunnerStartTurnReturnsStructuredServerError(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"error":{"code":"bad_request","message":"missing model"}}`,
	)
	runner := NewAppServerRunner(transport)

	_, err := runner.StartTurn(context.Background(), StartTurnInput{
		ThreadID: "thread-1",
		TurnInput: TurnInput{
			Prompt: "hello",
		},
	})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("expected structured codex error, got %v", err)
	}
	if !strings.Contains(err.Error(), "bad_request") || !strings.Contains(err.Error(), "missing model") {
		t.Fatalf("error did not preserve server code/message: %v", err)
	}
}

func TestAppServerRunnerReturnsTurnCompletedFailure(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turn":{"id":"turn-1","status":"failed","error":{"code":"tool_error","message":"tool failed"},"items":[]}}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("expected codex error, got result=%#v err=%v", got, err)
	}
	if got.Status != TurnStatusFailed || got.Failure == nil || got.Failure.Message != "tool failed" {
		t.Fatalf("failure not preserved: %#v", got)
	}
}

func TestAppServerRunnerReturnsInterruptedTurn(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turn":{"id":"turn-1","status":"interrupted","items":[]}}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("expected codex error, got result=%#v err=%v", got, err)
	}
	if got.Status != TurnStatusInterrupted {
		t.Fatalf("status = %q, want interrupted", got.Status)
	}
}

func TestAppServerRunnerHandlesNestedErrorNotification(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"error","params":{"threadId":"thread-new","turnId":"turn-1","willRetry":true,"error":{"message":"temporary issue"}}}`,
		`{"method":"error","params":{"threadId":"thread-new","turnId":"turn-1","willRetry":false,"error":{"code":"model_error","message":"permanent issue"}}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("expected codex error, got result=%#v err=%v", got, err)
	}
	if got.Status != TurnStatusFailed || got.Failure == nil || got.Failure.Code != "model_error" || got.Failure.Message != "permanent issue" {
		t.Fatalf("nested error not preserved: %#v", got)
	}
}

func TestAppServerRunnerRejectsInterleavedServerRequestAndKeepsWaiting(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"jsonrpc":"2.0","id":99,"method":"item/commandExecution/requestApproval","params":{"threadId":"thread-new","turnId":"turn-1"}}`,
		`{"method":"item/completed","params":{"threadId":"thread-new","turnId":"turn-1","item":{"id":"item-1","type":"agentMessage","text":"after request"}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turn":{"id":"turn-1","status":"completed","items":[]}}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.FinalAgentMessage != "after request" {
		t.Fatalf("final message = %q", got.FinalAgentMessage)
	}

	writes := transport.decodedWrites(t)
	if got := writes[5]["id"]; got != float64(99) {
		t.Fatalf("server request response id = %#v, want 99 in %#v", got, writes[5])
	}
	if _, ok := writes[5]["error"].(map[string]any); !ok {
		t.Fatalf("server request response missing error: %#v", writes[5])
	}
}

func TestAppServerRunnerReadThreadUsesIncludeTurns(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-read","turns":[{"id":"turn-1","status":"completed","items":[{"id":"item-1","type":"agentMessage","text":"done"}]}]}}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.ReadThread(context.Background(), "thread-read")
	if err != nil {
		t.Fatalf("ReadThread error: %v", err)
	}
	if got.ID != "thread-read" {
		t.Fatalf("thread id = %q", got.ID)
	}

	writes := transport.decodedWrites(t)
	assertMethod(t, writes[3], "thread/read")
	assertParamString(t, writes[3], "threadId", "thread-read")
	assertParamBool(t, writes[3], "includeTurns", true)
}

func TestAppServerRunnerBackfillsCompletedTurnItems(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-new","turn":{"id":"turn-1"}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turn":{"id":"turn-1","status":"completed","items":[]}}}`,
		`{"id":5,"result":{"thread":{"id":"thread-new","turns":[{"id":"turn-1","status":"completed","items":[{"id":"item-1","type":"agentMessage","text":"backfilled"}]}]}}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.FinalAgentMessage != "backfilled" {
		t.Fatalf("final message = %q, want backfilled", got.FinalAgentMessage)
	}

	writes := transport.decodedWrites(t)
	assertMethod(t, writes[5], "thread/read")
	assertParamBool(t, writes[5], "includeTurns", true)
}

func TestAppServerRunnerFallsBackWhenExtraArgsCannotBeTranslated(t *testing.T) {
	transport := newFakeAppServerTransport()
	fallback := &fakeRunner{
		startResult: TurnResult{ThreadID: "fallback-thread", TurnID: "fallback-turn", Status: TurnStatusCompleted},
	}
	runner := &AppServerRunner{
		Transport: transport,
		Fallback:  fallback,
		ExtraArgs: []string{"--model", "gpt-5"},
	}

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread fallback error: %v", err)
	}
	if got.ThreadID != "fallback-thread" || !fallback.startCalled {
		t.Fatalf("fallback not used, result=%#v called=%v", got, fallback.startCalled)
	}
	if len(transport.writes) != 0 {
		t.Fatalf("app-server was used despite unsupported extra args: %q", transport.writes)
	}
}

func TestAppServerRunnerFallsBackWhenInitializationProbeFails(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"error":{"code":"unsupported","message":"thread list missing"}}`,
	)
	fallback := &fakeRunner{
		startResult: TurnResult{ThreadID: "fallback-thread", TurnID: "fallback-turn", Status: TurnStatusCompleted},
	}
	var startReq AppServerStartRequest
	runner := &AppServerRunner{
		Starter: AppServerTransportStarterFunc(func(_ context.Context, req AppServerStartRequest) (AppServerLineTransport, error) {
			startReq = req
			return transport, nil
		}),
		Fallback: fallback,
		Command:  "/managed/codex",
		Timeout:  time.Minute,
	}

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread fallback error: %v", err)
	}
	if got.ThreadID != "fallback-thread" || !fallback.startCalled {
		t.Fatalf("fallback not used, result=%#v called=%v", got, fallback.startCalled)
	}
	if startReq.Command != "/managed/codex" {
		t.Fatalf("starter command = %q", startReq.Command)
	}
	if !reflect.DeepEqual(startReq.Args, []string{"app-server"}) {
		t.Fatalf("starter args = %#v", startReq.Args)
	}
	if !transport.closed {
		t.Fatalf("transport was not closed after failed probe")
	}
}

func TestAppServerRunnerSurfacesTransportCrashWithoutRealCodex(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
	)
	runner := NewAppServerRunner(transport)

	_, err := runner.ListThreads(context.Background(), ListThreadsOptions{})
	if !IsKind(err, ErrorLaunch) {
		t.Fatalf("expected launch error for closed app-server stream, got %v", err)
	}
}

func TestAppServerRunnerCloseClosesTransport(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
	)
	runner := NewAppServerRunner(transport)
	if _, err := runner.ListThreads(context.Background(), ListThreadsOptions{Limit: 1}); err != nil {
		t.Fatalf("ListThreads error: %v", err)
	}
	if err := runner.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	if !transport.closed {
		t.Fatal("transport was not closed")
	}
	if runner.Transport != nil || runner.ready {
		t.Fatalf("runner still appears ready after close: transport=%#v ready=%v", runner.Transport, runner.ready)
	}
}

func TestProbeAppServerCompatibilityRunsColdProbeRepeatedly(t *testing.T) {
	starts := 0
	starter := AppServerTransportStarterFunc(func(_ context.Context, req AppServerStartRequest) (AppServerLineTransport, error) {
		starts++
		if req.Command != "/managed/codex" || req.WorkingDir != "/work" {
			t.Fatalf("start request = %#v", req)
		}
		time.Sleep(time.Millisecond)
		return newFakeAppServerTransport(
			`{"id":1,"result":{"userAgent":"codex-helper-test/0"}}`,
			`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
			`{"id":3,"result":{"data":[{"id":"thread-a"}],"nextCursor":null,"backwardsCursor":null}}`,
		), nil
	})

	got, err := ProbeAppServerCompatibility(context.Background(), AppServerProbeOptions{
		Starter:    starter,
		Command:    "/managed/codex",
		WorkingDir: "/work",
		Runs:       3,
		Limit:      1,
	})
	if err != nil {
		t.Fatalf("ProbeAppServerCompatibility error: %v", err)
	}
	if starts != 3 || len(got.Runs) != 3 {
		t.Fatalf("probe starts=%d runs=%#v, want 3", starts, got.Runs)
	}
	for i, run := range got.Runs {
		if run.Index != i+1 || run.ThreadCount != 1 || run.Duration <= 0 {
			t.Fatalf("run[%d] = %#v", i, run)
		}
	}
	if got.Min <= 0 || got.Max < got.Min || got.Total <= 0 {
		t.Fatalf("probe timing summary invalid: %#v", got)
	}
}

type fakeAppServerTransport struct {
	reads  [][]byte
	writes [][]byte
	closed bool
}

func newFakeAppServerTransport(reads ...string) *fakeAppServerTransport {
	transport := &fakeAppServerTransport{}
	for _, read := range reads {
		transport.reads = append(transport.reads, []byte(read))
	}
	return transport
}

func (t *fakeAppServerTransport) WriteLine(_ context.Context, line []byte) error {
	t.writes = append(t.writes, append([]byte{}, line...))
	return nil
}

func (t *fakeAppServerTransport) ReadLine(context.Context) ([]byte, error) {
	if len(t.reads) == 0 {
		return nil, io.EOF
	}
	line := t.reads[0]
	t.reads = t.reads[1:]
	return append([]byte{}, line...), nil
}

func (t *fakeAppServerTransport) Close() error {
	t.closed = true
	return nil
}

func (t *fakeAppServerTransport) decodedWrites(tb testing.TB) []map[string]any {
	tb.Helper()
	var writes []map[string]any
	for _, line := range t.writes {
		var decoded map[string]any
		if err := json.Unmarshal(line, &decoded); err != nil {
			tb.Fatalf("write is not JSON: %s: %v", string(line), err)
		}
		writes = append(writes, decoded)
	}
	return writes
}

type fakeRunner struct {
	startCalled bool
	startResult TurnResult
	startErr    error
}

func (r *fakeRunner) StartThread(context.Context, TurnInput) (TurnResult, error) {
	r.startCalled = true
	return r.startResult, r.startErr
}

func (r *fakeRunner) ResumeThread(context.Context, string, TurnInput) (TurnResult, error) {
	return TurnResult{}, errors.New("unexpected ResumeThread")
}

func (r *fakeRunner) StartTurn(context.Context, StartTurnInput) (TurnResult, error) {
	return TurnResult{}, errors.New("unexpected StartTurn")
}

func (r *fakeRunner) InterruptTurn(context.Context, TurnRef) error {
	return errors.New("unexpected InterruptTurn")
}

func (r *fakeRunner) ReadThread(context.Context, string) (Thread, error) {
	return Thread{}, errors.New("unexpected ReadThread")
}

func (r *fakeRunner) ListThreads(context.Context, ListThreadsOptions) ([]Thread, error) {
	return nil, errors.New("unexpected ListThreads")
}

func assertMethod(tb testing.TB, got map[string]any, want string) {
	tb.Helper()
	if got["method"] != want {
		tb.Fatalf("method = %#v, want %q in %#v", got["method"], want, got)
	}
}

func assertJSONRPC(tb testing.TB, got map[string]any) {
	tb.Helper()
	if got["jsonrpc"] != "2.0" {
		tb.Fatalf("jsonrpc = %#v, want 2.0 in %#v", got["jsonrpc"], got)
	}
}

func assertNoID(tb testing.TB, got map[string]any) {
	tb.Helper()
	if _, ok := got["id"]; ok {
		tb.Fatalf("notification included id: %#v", got)
	}
}

func assertParamString(tb testing.TB, got map[string]any, key string, want string) {
	tb.Helper()
	params := paramsMap(tb, got)
	if params[key] != want {
		tb.Fatalf("param %s = %#v, want %q in %#v", key, params[key], want, got)
	}
}

func assertParamNumber(tb testing.TB, got map[string]any, key string, want float64) {
	tb.Helper()
	params := paramsMap(tb, got)
	if params[key] != want {
		tb.Fatalf("param %s = %#v, want %v in %#v", key, params[key], want, got)
	}
}

func assertParamBool(tb testing.TB, got map[string]any, key string, want bool) {
	tb.Helper()
	params := paramsMap(tb, got)
	if params[key] != want {
		tb.Fatalf("param %s = %#v, want %v in %#v", key, params[key], want, got)
	}
}

func assertParamNil(tb testing.TB, got map[string]any, key string) {
	tb.Helper()
	params := paramsMap(tb, got)
	if value, ok := params[key]; !ok || value != nil {
		tb.Fatalf("param %s = %#v, want nil in %#v", key, value, got)
	}
}

func assertParamAbsent(tb testing.TB, got map[string]any, key string) {
	tb.Helper()
	params := paramsMap(tb, got)
	if value, ok := params[key]; ok {
		tb.Fatalf("param %s = %#v, want absent in %#v", key, value, got)
	}
}

func assertParamStrings(tb testing.TB, got map[string]any, key string, want []string) {
	tb.Helper()
	params := paramsMap(tb, got)
	raw, ok := params[key].([]any)
	if !ok {
		tb.Fatalf("param %s = %#v, want string list in %#v", key, params[key], got)
	}
	var gotStrings []string
	for _, value := range raw {
		gotStrings = append(gotStrings, value.(string))
	}
	if !reflect.DeepEqual(gotStrings, want) {
		tb.Fatalf("param %s = %#v, want %#v in %#v", key, gotStrings, want, got)
	}
}

func assertTextInput(tb testing.TB, got map[string]any, want string) {
	tb.Helper()
	params := paramsMap(tb, got)
	raw, ok := params["input"].([]any)
	if !ok || len(raw) != 1 {
		tb.Fatalf("input = %#v, want one text input in %#v", params["input"], got)
	}
	input, ok := raw[0].(map[string]any)
	if !ok {
		tb.Fatalf("input[0] = %#v, want object", raw[0])
	}
	if input["type"] != "text" || input["text"] != want {
		tb.Fatalf("input[0] = %#v, want text %q", input, want)
	}
}

func paramsMap(tb testing.TB, got map[string]any) map[string]any {
	tb.Helper()
	params, ok := got["params"].(map[string]any)
	if !ok {
		tb.Fatalf("params = %#v, want object in %#v", got["params"], got)
	}
	return params
}
