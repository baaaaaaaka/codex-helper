package codexrunner

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestPolicyTransportReadyHookRunsAfterSuccessfulInitializeResponse(t *testing.T) {
	base := newFakeAppServerTransport(
		`{"jsonrpc":"2.0","id":7,"result":{"userAgent":"test"}}`,
		`{"jsonrpc":"2.0","id":7,"result":{"duplicate":true}}`,
	)
	var calls atomic.Int32
	transport := &policyAppServerTransport{
		AppServerLineTransport: base,
		readyHook:              func() { calls.Add(1) },
		initializeIDs:          make(map[string]struct{}),
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
		readyHook:              func() { calls.Add(1) },
		initializeIDs:          make(map[string]struct{}),
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

func TestAppServerBaseURLOverridesUsesLastCLIValue(t *testing.T) {
	openAI, chatGPT := appServerBaseURLOverrides([]string{
		"app-server",
		"-c", `openai_base_url="https://first.example/v1"`,
		"--config", `chatgpt_base_url='https://chat.example/backend-api'`,
		"-c", `openai_base_url="https://last.example/v1"`,
	})
	if openAI != "https://last.example/v1" || chatGPT != "https://chat.example/backend-api" {
		t.Fatalf("openAI=%q chatGPT=%q", openAI, chatGPT)
	}
}
