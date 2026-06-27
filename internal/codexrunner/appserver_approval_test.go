package codexrunner

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestAutomaticApprovalResultMatrix(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		params     string
		want       string
		wantHandle bool
	}{
		{name: "command", method: appServerMethodCommandExecutionApproval, params: `{}`, want: `{"decision":"accept"}`, wantHandle: true},
		{name: "file change", method: appServerMethodFileChangeApproval, params: `{}`, want: `{"decision":"accept"}`, wantHandle: true},
		{name: "permissions", method: appServerMethodPermissionsApproval, params: `{"permissions":{"network":{"enabled":true},"fileSystem":{"read":["/dev"]},"ignored":"value"}}`, want: `{"permissions":{"fileSystem":{"read":["/dev"]},"network":{"enabled":true}},"scope":"turn"}`, wantHandle: true},
		{name: "legacy exec", method: appServerMethodLegacyExecApproval, params: `{}`, want: `{"decision":"approved"}`, wantHandle: true},
		{name: "legacy patch", method: appServerMethodLegacyPatchApproval, params: `{}`, want: `{"decision":"approved"}`, wantHandle: true},
		{name: "MCP approval", method: appServerMethodMCPElicitation, params: `{"request":{"_meta":{"codex_approval_kind":"mcp_tool_call"}}}`, want: `{"_meta":null,"action":"accept","content":null}`, wantHandle: true},
		{name: "ordinary MCP elicitation", method: appServerMethodMCPElicitation, params: `{"request":{"_meta":{"purpose":"input"}}}`, wantHandle: false},
		{name: "unknown request", method: "item/tool/requestUserInput", params: `{}`, wantHandle: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, handled, err := automaticApprovalResult(tc.method, json.RawMessage(tc.params))
			if err != nil {
				t.Fatalf("automaticApprovalResult error: %v", err)
			}
			if handled != tc.wantHandle {
				t.Fatalf("handled = %v, want %v", handled, tc.wantHandle)
			}
			if tc.wantHandle {
				assertJSONEqual(t, got, []byte(tc.want))
			}
		})
	}
}

func TestAutomaticApprovalHandlerWaitsForConfiguredDelay(t *testing.T) {
	delay := 30 * time.Millisecond
	started := time.Now()
	result, handled, err := (AutomaticApprovalHandler{Delay: delay}).HandleServerRequest(context.Background(), appServerMethodCommandExecutionApproval, json.RawMessage(`{}`))
	if err != nil || !handled {
		t.Fatalf("HandleServerRequest result=%s handled=%v err=%v", result, handled, err)
	}
	if elapsed := time.Since(started); elapsed < delay {
		t.Fatalf("approval returned after %s, want at least %s", elapsed, delay)
	}
}

func TestAutomaticApprovalHandlerCancelsDelayWithTurnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, handled, err := (AutomaticApprovalHandler{Delay: time.Second}).HandleServerRequest(ctx, appServerMethodCommandExecutionApproval, json.RawMessage(`{}`))
	if !handled || !errors.Is(err, context.Canceled) {
		t.Fatalf("handled=%v err=%v, want canceled handled request", handled, err)
	}
}

func TestAutomaticApprovalRejectsMalformedPermissions(t *testing.T) {
	_, handled, err := automaticApprovalResult(appServerMethodPermissionsApproval, json.RawMessage(`{"reason":"missing permissions"}`))
	if !handled || err == nil {
		t.Fatalf("handled=%v err=%v, want fail-closed malformed request", handled, err)
	}
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var gotValue any
	var wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("decode got JSON %q: %v", got, err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("decode want JSON %q: %v", want, err)
	}
	gotCanonical, _ := json.Marshal(gotValue)
	wantCanonical, _ := json.Marshal(wantValue)
	if string(gotCanonical) != string(wantCanonical) {
		t.Fatalf("JSON = %s, want %s", gotCanonical, wantCanonical)
	}
}
