package codexrunner

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

type recordingPolicyStarter struct {
	request AppServerStartRequest
}

func (s *recordingPolicyStarter) StartAppServer(_ context.Context, request AppServerStartRequest) (AppServerLineTransport, error) {
	s.request = request
	return newFakeAppServerTransport(), nil
}

func TestPolicyTransportReadyHookRunsAfterSuccessfulInitializeResponse(t *testing.T) {
	base := newFakeAppServerTransport(
		`{"jsonrpc":"2.0","id":7,"result":{"userAgent":"test"}}`,
		`{"jsonrpc":"2.0","id":7,"result":{"duplicate":true}}`,
	)
	var calls atomic.Int32
	transport := &policyAppServerTransport{
		AppServerLineTransport: base,
		readyHook: func() error {
			calls.Add(1)
			return nil
		},
		initializeIDs: make(map[string]struct{}),
	}
	if err := transport.WriteLine(context.Background(), []byte(`{"jsonrpc":"2.0","id":7,"method":"initialize","params":{}}`)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("hook ran before initialize response")
	}
	if _, err := transport.ReadLine(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("hook calls = %d, want 1", calls.Load())
	}
	if _, err := transport.ReadLine(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("hook calls after duplicate = %d, want 1", calls.Load())
	}
}

func TestPolicyTransportReadyHookSkipsFailedInitialize(t *testing.T) {
	base := newFakeAppServerTransport(`{"jsonrpc":"2.0","id":"init","error":{"code":-1,"message":"no"}}`)
	var calls atomic.Int32
	transport := &policyAppServerTransport{
		AppServerLineTransport: base,
		readyHook: func() error {
			calls.Add(1)
			return nil
		},
		initializeIDs: make(map[string]struct{}),
	}
	if err := transport.WriteLine(context.Background(), []byte(`{"jsonrpc":"2.0","id":"init","method":"initialize","params":{}}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.ReadLine(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("hook calls = %d, want 0", calls.Load())
	}
}

func TestPolicyTransportReadyHookFailureRejectsInitialize(t *testing.T) {
	base := newFakeAppServerTransport(`{"jsonrpc":"2.0","id":7,"result":{}}`)
	transport := &policyAppServerTransport{
		AppServerLineTransport: base,
		readyHook: func() error {
			return errors.New("migration commit failed")
		},
		initializeIDs: make(map[string]struct{}),
	}
	if err := transport.WriteLine(context.Background(), []byte(`{"jsonrpc":"2.0","id":7,"method":"initialize","params":{}}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.ReadLine(context.Background()); err == nil || !strings.Contains(err.Error(), "migration commit failed") {
		t.Fatalf("ReadLine error = %v, want ready-hook failure", err)
	}
}

func TestAppServerOpenAIBaseURLOverrideUsesLastCLIValueAndIgnoresChatGPT(t *testing.T) {
	openAI := appServerOpenAIBaseURLOverride([]string{
		"app-server",
		"-c", `openai_base_url="https://first.example/v1"`,
		"--config", `chatgpt_base_url='https://chat.example/backend-api'`,
		"-c", `openai_base_url="https://last.example/v1"`,
	})
	if openAI != "https://last.example/v1" {
		t.Fatalf("openAI=%q", openAI)
	}
}

func TestPolicyStarterPreservesUserChatGPTOriginAndAddsOnlyResponsesGateway(t *testing.T) {
	base := &recordingPolicyStarter{}
	starter := PolicyAppServerStarter{Base: base}
	transport, err := starter.StartAppServer(context.Background(), AppServerStartRequest{
		Command: "codex",
		Args: []string{
			"app-server",
			"-c", `chatgpt_base_url="https://chat.example/backend-api"`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	var chatGPTValues, openAIValues []string
	for index := 0; index+1 < len(base.request.Args); index++ {
		if base.request.Args[index] != "-c" && base.request.Args[index] != "--config" {
			continue
		}
		value := base.request.Args[index+1]
		switch {
		case strings.HasPrefix(value, "chatgpt_base_url="):
			chatGPTValues = append(chatGPTValues, value)
		case strings.HasPrefix(value, "openai_base_url="):
			openAIValues = append(openAIValues, value)
		}
		index++
	}
	if len(chatGPTValues) != 1 || chatGPTValues[0] != `chatgpt_base_url="https://chat.example/backend-api"` {
		t.Fatalf("chatgpt_base_url values = %#v, want the untouched user value", chatGPTValues)
	}
	if len(openAIValues) != 1 || !strings.Contains(openAIValues[0], "http://127.0.0.1:") || !strings.Contains(openAIValues[0], "/gateway") {
		t.Fatalf("openai_base_url values = %#v, want one loopback Responses gateway", openAIValues)
	}
	joined := strings.Join(base.request.Args, " ")
	for _, want := range []string{`approval_policy="on-request"`, `approvals_reviewer="user"`, `sandbox_mode="read-only"`} {
		if !strings.Contains(joined, want) {
			t.Fatalf("policy args missing %q: %s", want, joined)
		}
	}
}
