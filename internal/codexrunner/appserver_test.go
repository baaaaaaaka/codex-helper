package codexrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"sync"
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
	capabilities := writes[0]["params"].(map[string]any)["capabilities"].(map[string]any)
	if capabilities["experimentalApi"] != true {
		t.Fatalf("initialize capabilities = %#v", capabilities)
	}
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

func TestAppServerRunnerCloseHookRunsOnce(t *testing.T) {
	var calls int
	runner := &AppServerRunner{CloseHook: func() { calls++ }}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("close hook calls = %d, want 1", calls)
	}
}

func TestAppServerRunnerStartThreadEncodesThreadStartAndTurnStart(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-new","turn":{"id":"turn-1"}}}`,
		`{"method":"thread/name/updated","params":{"threadId":"thread-new","threadName":"Generated helper title"}}`,
		`{"method":"item/agentMessage/delta","params":{"threadId":"thread-new","turnId":"turn-1","itemId":"item-1","delta":"do"}}`,
		`{"method":"item/agentMessage/delta","params":{"threadId":"thread-new","turnId":"turn-1","itemId":"item-1","delta":"ne"}}`,
		`{"method":"thread/tokenUsage/updated","params":{"threadId":"thread-new","turnId":"turn-1","tokenUsage":{"total":{"totalTokens":100,"inputTokens":80,"cachedInputTokens":20,"outputTokens":20,"reasoningOutputTokens":4},"last":{"totalTokens":30,"inputTokens":20,"cachedInputTokens":7,"outputTokens":10,"reasoningOutputTokens":3}}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turn":{"id":"turn-1","status":"completed","items":[]}}}`,
	)
	runner := &AppServerRunner{
		Transport:  transport,
		Command:    "/managed/codex",
		WorkingDir: "/work",
		Timeout:    time.Minute,
	}

	got, err := runner.StartThread(context.Background(), TurnInput{
		Prompt:         "hello",
		AdditionalDirs: []string{"/extra-a", "/extra-b"},
		OutputSchema:   json.RawMessage(`{"type":"object"}`),
		Ephemeral:      true,
	})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.ThreadID != "thread-new" || got.TurnID != "turn-1" || got.Status != TurnStatusCompleted {
		t.Fatalf("unexpected result: %#v", got)
	}
	if got.FinalAgentMessage != "done" || got.Usage.CachedInputTokens != 7 || got.Usage.ReasoningOutputTokens != 3 {
		t.Fatalf("notification result not parsed: %#v", got)
	}
	if got.ThreadName != "Generated helper title" {
		t.Fatalf("thread name = %q, want generated title", got.ThreadName)
	}

	writes := transport.decodedWrites(t)
	assertMethod(t, writes[3], "thread/start")
	assertParamString(t, writes[3], "cwd", "/work")
	params := writes[3]["params"].(map[string]any)
	roots, ok := params["runtimeWorkspaceRoots"].([]any)
	if !ok || len(roots) != 2 || roots[0] != "/extra-a" || roots[1] != "/extra-b" {
		t.Fatalf("runtimeWorkspaceRoots = %#v", params["runtimeWorkspaceRoots"])
	}
	if params["ephemeral"] != true {
		t.Fatalf("ephemeral = %#v", params["ephemeral"])
	}
	assertParamAbsent(t, writes[3], "extra_args")
	assertMethod(t, writes[4], "turn/start")
	assertParamString(t, writes[4], "threadId", "thread-new")
	assertTextInput(t, writes[4], "hello")
	assertParamString(t, writes[4], "cwd", "/work")
	turnParams := writes[4]["params"].(map[string]any)
	if schema, ok := turnParams["outputSchema"].(map[string]any); !ok || schema["type"] != "object" {
		t.Fatalf("outputSchema = %#v", turnParams["outputSchema"])
	}
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

	got, err := runner.ResumeThread(context.Background(), "thread-existing", TurnInput{Prompt: "continue", AdditionalDirs: []string{"/resume-extra"}})
	if err != nil {
		t.Fatalf("ResumeThread error: %v", err)
	}
	if got.ThreadID != "thread-existing" || got.TurnID != "turn-resume" || got.FinalAgentMessage != "resumed" {
		t.Fatalf("unexpected result: %#v", got)
	}
	writes := transport.decodedWrites(t)
	turnParams := writes[4]["params"].(map[string]any)
	roots, ok := turnParams["runtimeWorkspaceRoots"].([]any)
	if !ok || len(roots) != 1 || roots[0] != "/resume-extra" {
		t.Fatalf("resume runtimeWorkspaceRoots = %#v", turnParams["runtimeWorkspaceRoots"])
	}

	assertMethod(t, writes[3], "thread/resume")
	assertParamString(t, writes[3], "threadId", "thread-existing")
	assertMethod(t, writes[4], "turn/start")
	assertParamString(t, writes[4], "threadId", "thread-existing")
	assertTextInput(t, writes[4], "continue")
}

func TestAppServerRunnerCapturesResumeThreadIDBeforeTurnStart(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-other"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-resume","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-other","turn":{"id":"turn-resume"}}}`,
		`{"method":"item/completed","params":{"threadId":"thread-other","turnId":"turn-resume","item":{"id":"item-1","type":"agentMessage","text":"resumed elsewhere"}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-other","turn":{"id":"turn-resume","status":"completed","items":[]}}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.ResumeThread(context.Background(), "thread-existing", TurnInput{Prompt: "continue"})
	if err != nil {
		t.Fatalf("ResumeThread error: %v", err)
	}
	if got.ThreadID != "thread-other" || got.TurnID != "turn-resume" || got.FinalAgentMessage != "resumed elsewhere" {
		t.Fatalf("unexpected result: %#v", got)
	}

	writes := transport.decodedWrites(t)
	assertMethod(t, writes[3], "thread/resume")
	assertParamString(t, writes[3], "threadId", "thread-existing")
	assertMethod(t, writes[4], "turn/start")
	assertParamString(t, writes[4], "threadId", "thread-other")
}

func TestAppServerRunnerEncodesLocalImageInputs(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"item/completed","params":{"threadId":"thread-new","turnId":"turn-1","item":{"id":"item-1","type":"agentMessage","text":"done"}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turn":{"id":"turn-1","status":"completed","items":[]}}}`,
	)
	runner := NewAppServerRunner(transport)

	_, err := runner.StartThread(context.Background(), TurnInput{
		Prompt:     "inspect",
		ImagePaths: []string{"/tmp/a.png", "", "/tmp/b.webp"},
	})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}

	writes := transport.decodedWrites(t)
	assertMethod(t, writes[4], "turn/start")
	assertInputItems(t, writes[4], []map[string]string{
		{"type": "localImage", "path": "/tmp/a.png"},
		{"type": "localImage", "path": "/tmp/b.webp"},
		{"type": "text", "text": "inspect"},
	})
}

func TestAppServerRunnerResumeThreadFailureKeepsExistingThreadIDOnly(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-existing"}}}`,
		`{"id":4,"error":{"code":"cloud_requirements","message":"Failed to load cloud requirements (workspace-managed policies)."}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.ResumeThread(context.Background(), "thread-existing", TurnInput{Prompt: "continue"})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("ResumeThread error = %v, want codex failure", err)
	}
	if got.ThreadID != "thread-existing" || got.TurnID != "" || got.Status != TurnStatusUnknown {
		t.Fatalf("unexpected result: %#v", got)
	}
	if !strings.Contains(err.Error(), "cloud_requirements") || !strings.Contains(err.Error(), "Failed to load cloud requirements") {
		t.Fatalf("error did not preserve app-server code/message: %v", err)
	}
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
		`{"method":"thread/compacted","params":{"thread":{"id":"thread-new"},"turn":{"id":"turn-1"}}}`,
		`{"method":"event_msg","params":{"type":"context_compacted","thread_id":"thread-new","turn_id":"turn-1"}}`,
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
	if len(events) != 8 {
		t.Fatalf("events len = %d, want 8: %#v", len(events), events)
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
	if events[4].Kind != StreamEventContextCompacted || events[4].ThreadID != "thread-new" || events[4].TurnID != "turn-1" {
		t.Fatalf("compact event = %#v", events[4])
	}
	if events[5].Kind != StreamEventContextCompacted || events[5].ThreadID != "thread-new" || events[5].TurnID != "turn-1" {
		t.Fatalf("event_msg compact event = %#v", events[5])
	}
	if events[6].Kind != StreamEventAgentMessage || events[6].Text != "done" {
		t.Fatalf("final agent event = %#v", events[6])
	}
	if events[7].Kind != StreamEventTurnCompleted || events[7].TurnID != "turn-1" {
		t.Fatalf("turn completed event = %#v", events[7])
	}
}

func TestAppServerRunnerStreamsRetryableStreamErrorNotification(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-new","turnId":"turn-1"}}`,
		`{"method":"error","params":{"threadId":"thread-new","turnId":"turn-1","willRetry":true,"error":{"message":"Reconnecting... 1/3","codexErrorInfo":{"responseStreamDisconnected":{"httpStatusCode":null}},"additionalDetails":"stream disconnected before completion"}}}`,
		`{"method":"item/completed","params":{"threadId":"thread-new","turnId":"turn-1","item":{"id":"item-1","type":"agentMessage","text":"done after reconnect"}}}`,
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
	if got.FinalAgentMessage != "done after reconnect" {
		t.Fatalf("final message = %q", got.FinalAgentMessage)
	}
	if len(events) != 4 {
		t.Fatalf("events len = %d, want 4: %#v", len(events), events)
	}
	retry := events[1]
	if retry.Kind != StreamEventStreamRetry || !retry.WillRetry {
		t.Fatalf("retry event = %#v", retry)
	}
	if retry.Failure == nil || retry.Failure.Message != "Reconnecting... 1/3" || retry.Failure.Code != "responseStreamDisconnected" {
		t.Fatalf("retry failure = %#v", retry.Failure)
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

func TestAppServerRunnerApprovesInterleavedServerRequestAndKeepsWaiting(t *testing.T) {
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
	runner.ServerRequestHandler = AutomaticApprovalHandler{}

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.FinalAgentMessage != "after request" {
		t.Fatalf("final message = %q", got.FinalAgentMessage)
	}
	runner.serverWG.Wait()

	writes := transport.decodedWrites(t)
	var response map[string]any
	for _, write := range writes {
		if write["id"] == float64(99) {
			response = write
			break
		}
	}
	if response == nil {
		t.Fatalf("missing approval response in %#v", writes)
	}
	if got := response["id"]; got != float64(99) {
		t.Fatalf("server request response id = %#v, want 99 in %#v", got, response)
	}
	result, ok := response["result"].(map[string]any)
	if !ok || result["decision"] != "accept" {
		t.Fatalf("server request response = %#v, want one-time accept", response)
	}
}

func TestAppServerRunnerReadThreadUsesIncludeTurns(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-read","name":"Read thread title","turns":[{"id":"turn-1","status":"completed","items":[{"id":"item-1","type":"agentMessage","text":"done"}]}]}}}`,
	)
	runner := NewAppServerRunner(transport)

	got, err := runner.ReadThread(context.Background(), "thread-read")
	if err != nil {
		t.Fatalf("ReadThread error: %v", err)
	}
	if got.ID != "thread-read" {
		t.Fatalf("thread id = %q", got.ID)
	}
	if got.Name != "Read thread title" {
		t.Fatalf("thread name = %q", got.Name)
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

func TestAppServerRunnerBackfillsThreadNameAfterCompletedTurn(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{"thread":{"id":"thread-new"}}}`,
		`{"id":4,"result":{"turn":{"id":"turn-1","status":"inProgress","items":[]}}}`,
		`{"method":"turn/started","params":{"threadId":"thread-new","turn":{"id":"turn-1"}}}`,
		`{"method":"item/completed","params":{"threadId":"thread-new","turnId":"turn-1","item":{"id":"item-1","type":"agentMessage","text":"done"}}}`,
		`{"method":"turn/completed","params":{"threadId":"thread-new","turn":{"id":"turn-1","status":"completed","items":[]}}}`,
		`{"id":5,"result":{"thread":{"id":"thread-new","name":"Backfilled generated title","turns":[{"id":"turn-1","status":"completed","items":[{"id":"item-1","type":"agentMessage","text":"done"}]}]}}}`,
	)
	runner := NewAppServerRunner(transport)
	runner.BackfillThreadName = true

	got, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartThread error: %v", err)
	}
	if got.FinalAgentMessage != "done" {
		t.Fatalf("final message = %q, want done", got.FinalAgentMessage)
	}
	if got.ThreadName != "Backfilled generated title" {
		t.Fatalf("thread name = %q, want backfilled generated title", got.ThreadName)
	}

	writes := transport.decodedWrites(t)
	assertMethod(t, writes[5], "thread/read")
	assertParamAbsent(t, writes[5], "includeTurns")
}

func TestAppServerRunnerFailsClosedWhenExtraArgsCannotBeTranslated(t *testing.T) {
	transport := newFakeAppServerTransport()
	runner := &AppServerRunner{
		Transport: transport,
		ExtraArgs: []string{"--model", "gpt-5"},
	}

	_, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorUnsupported) {
		t.Fatalf("StartThread error = %v, want unsupported", err)
	}
	if len(transport.writes) != 0 {
		t.Fatalf("app-server was used despite unsupported extra args: %q", transport.writes)
	}
}

func TestAppServerRunnerFailsClosedWhenInitializationProbeFails(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"error":{"code":"unsupported","message":"thread list missing"}}`,
	)
	var startReq AppServerStartRequest
	runner := &AppServerRunner{
		Starter: AppServerTransportStarterFunc(func(_ context.Context, req AppServerStartRequest) (AppServerLineTransport, error) {
			startReq = req
			return transport, nil
		}),
		Command:  "/managed/codex",
		ExtraEnv: []string{"CODEX_HELPER_TEAMS_CHILD=1"},
		Timeout:  time.Minute,
	}

	_, err := runner.StartThread(context.Background(), TurnInput{Prompt: "hello"})
	if !IsKind(err, ErrorCodex) {
		t.Fatalf("StartThread error = %v, want app-server probe failure", err)
	}
	if startReq.Command != "/managed/codex" {
		t.Fatalf("starter command = %q", startReq.Command)
	}
	if !reflect.DeepEqual(startReq.Args, []string{"app-server"}) {
		t.Fatalf("starter args = %#v", startReq.Args)
	}
	if !reflect.DeepEqual(startReq.ExtraEnv, []string{"CODEX_HELPER_TEAMS_CHILD=1"}) {
		t.Fatalf("starter extra env = %#v", startReq.ExtraEnv)
	}
	if !transport.closed {
		t.Fatalf("transport was not closed after failed probe")
	}
}

func TestAppServerRunnerRetriesAfterCanceledInitialization(t *testing.T) {
	first := newFakeAppServerTransport()
	second := newFakeAppServerTransport(
		`{"id":2,"result":{}}`,
		`{"id":3,"result":{"data":[]}}`,
		`{"id":4,"result":{"data":[{"id":"thread-after-retry"}]}}`,
	)
	starts := 0
	runner := &AppServerRunner{Starter: AppServerTransportStarterFunc(func(context.Context, AppServerStartRequest) (AppServerLineTransport, error) {
		starts++
		if starts == 1 {
			return first, nil
		}
		return second, nil
	})}
	defer runner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := runner.ListThreads(ctx, ListThreadsOptions{}); !IsKind(err, ErrorTimeout) {
		t.Fatalf("first ListThreads error = %v, want timeout", err)
	}
	threads, err := runner.ListThreads(context.Background(), ListThreadsOptions{})
	if err != nil {
		t.Fatalf("retry ListThreads error: %v", err)
	}
	if starts != 2 || len(threads) != 1 || threads[0].ID != "thread-after-retry" {
		t.Fatalf("retry starts=%d threads=%#v", starts, threads)
	}
	if !first.closed {
		t.Fatal("canceled initialization transport was not closed")
	}
}

func TestAppServerRunnerSurfacesTransportCrashWithoutRealCodex(t *testing.T) {
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
	)
	transport.eofWhenDrained = true
	runner := NewAppServerRunner(transport)

	_, err := runner.ListThreads(context.Background(), ListThreadsOptions{})
	if !IsKind(err, ErrorLaunch) {
		t.Fatalf("expected launch error for closed app-server stream, got %v", err)
	}
}

func TestAppServerRunnerRestartsOnNextRequestAfterTransportCrash(t *testing.T) {
	first := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[]}}`,
		`{"id":3,"result":{"data":[{"id":"thread-before-crash"}]}}`,
	)
	first.eofWhenDrained = true
	second := newFakeAppServerTransport(
		`{"id":4,"result":{}}`,
		`{"id":5,"result":{"data":[]}}`,
		`{"id":6,"result":{"data":[{"id":"thread-after-crash"}]}}`,
	)
	starts := 0
	runner := &AppServerRunner{Starter: AppServerTransportStarterFunc(func(context.Context, AppServerStartRequest) (AppServerLineTransport, error) {
		starts++
		if starts == 1 {
			return first, nil
		}
		return second, nil
	})}
	defer runner.Close()

	threads, err := runner.ListThreads(context.Background(), ListThreadsOptions{})
	if err != nil || len(threads) != 1 || threads[0].ID != "thread-before-crash" {
		t.Fatalf("first ListThreads threads=%#v err=%v", threads, err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		runner.protocolMu.Lock()
		failed := runner.protocolErr != nil
		runner.protocolMu.Unlock()
		if failed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("protocol crash was not recorded")
		}
		time.Sleep(time.Millisecond)
	}
	threads, err = runner.ListThreads(context.Background(), ListThreadsOptions{})
	if err != nil || starts != 2 || len(threads) != 1 || threads[0].ID != "thread-after-crash" {
		t.Fatalf("restarted ListThreads starts=%d threads=%#v err=%v", starts, threads, err)
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
	mu             sync.Mutex
	reads          [][]byte
	writes         [][]byte
	closed         bool
	eofWhenDrained bool
	notify         chan struct{}
}

func newFakeAppServerTransport(reads ...string) *fakeAppServerTransport {
	transport := &fakeAppServerTransport{notify: make(chan struct{}, 1)}
	for _, read := range reads {
		transport.reads = append(transport.reads, []byte(read))
	}
	return transport
}

func (t *fakeAppServerTransport) WriteLine(_ context.Context, line []byte) error {
	t.mu.Lock()
	t.writes = append(t.writes, append([]byte{}, line...))
	t.mu.Unlock()
	t.signal()
	return nil
}

func (t *fakeAppServerTransport) ReadLine(ctx context.Context) ([]byte, error) {
	for {
		t.mu.Lock()
		if len(t.reads) > 0 && t.readReadyLocked(t.reads[0]) {
			line := t.reads[0]
			t.reads = t.reads[1:]
			t.mu.Unlock()
			return append([]byte{}, line...), nil
		}
		if t.closed || (len(t.reads) == 0 && t.eofWhenDrained) {
			t.mu.Unlock()
			return nil, io.EOF
		}
		notify := t.notify
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

func (t *fakeAppServerTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.signal()
	return nil
}

func (t *fakeAppServerTransport) readReadyLocked(line []byte) bool {
	var message appServerMessage
	if json.Unmarshal(line, &message) != nil || len(bytes.TrimSpace(message.ID)) == 0 || strings.TrimSpace(message.Method) != "" {
		return true
	}
	id, ok := appServerNumericID(message.ID)
	if !ok {
		return true
	}
	for _, write := range t.writes {
		var request appServerRequest
		if json.Unmarshal(write, &request) == nil && request.ID == id {
			return true
		}
	}
	return false
}

func (t *fakeAppServerTransport) signal() {
	select {
	case t.notify <- struct{}{}:
	default:
	}
}

func (t *fakeAppServerTransport) decodedWrites(tb testing.TB) []map[string]any {
	tb.Helper()
	t.mu.Lock()
	defer t.mu.Unlock()
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

func assertInputItems(tb testing.TB, got map[string]any, want []map[string]string) {
	tb.Helper()
	params := paramsMap(tb, got)
	raw, ok := params["input"].([]any)
	if !ok || len(raw) != len(want) {
		tb.Fatalf("input = %#v, want %d item(s) in %#v", params["input"], len(want), got)
	}
	for i, wantItem := range want {
		item, ok := raw[i].(map[string]any)
		if !ok {
			tb.Fatalf("input[%d] = %#v, want object", i, raw[i])
		}
		for key, value := range wantItem {
			if item[key] != value {
				tb.Fatalf("input[%d][%s] = %#v, want %q in %#v", i, key, item[key], value, item)
			}
		}
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
