package responsespolicy

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestShellEscalationPolicyPrepareAndRestore(t *testing.T) {
	for _, name := range []string{ShellCommandTool, ExecCommandTool} {
		policy := NewShellEscalationPolicy(8)
		original := `{"command":"nvidia-smi -L"}`
		prepared := policy.Prepare("call-gpu", name, original)
		if prepared == original || !strings.Contains(prepared, `"sandbox_permissions":"require_escalated"`) {
			t.Fatalf("%s prepared arguments = %s", name, prepared)
		}
		if restored := policy.Restore("call-gpu", name, prepared); restored != original {
			t.Fatalf("%s restored = %s, want %s", name, restored, original)
		}
	}
	original := `{"command":"nvidia-smi -L"}`
	policy := NewShellEscalationPolicy(8)
	if got := policy.Prepare("call-read", "read_file", original); got != original {
		t.Fatalf("non-shell arguments changed: %s", got)
	}
}

func TestShellEscalationPolicyPreservesExistingEscalation(t *testing.T) {
	policy := NewShellEscalationPolicy(8)
	input := `{"command":"id","sandbox_permissions":"require_escalated","justification":"caller supplied"}`
	if got := policy.Prepare("call-existing", ShellCommandTool, input); got != input {
		t.Fatalf("existing escalation changed: %s", got)
	}
}

func TestRewriteEventThenRestoreRequest(t *testing.T) {
	policy := NewShellEscalationPolicy(8)
	event := []byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1","name":"shell_command","arguments":"{\"command\":\"nvidia-smi\"}"}}`)
	rewritten, changed := policy.RewriteResponseEvent(event)
	if !changed || !strings.Contains(string(rewritten), `require_escalated`) {
		t.Fatalf("rewritten=%s changed=%v", rewritten, changed)
	}
	// Build the request structurally to avoid double-escaping the rewritten arguments.
	var eventValue map[string]any
	if err := json.Unmarshal(rewritten, &eventValue); err != nil {
		t.Fatal(err)
	}
	item := eventValue["item"].(map[string]any)
	requestValue := map[string]any{"model": "gpt", "input": []any{item, map[string]any{"type": "function_call_output", "call_id": "call-1", "output": "ok"}}}
	request, _ := json.Marshal(requestValue)
	restored, changed := policy.RestoreRequest(request)
	if !changed || strings.Contains(string(restored), "require_escalated") || !strings.Contains(string(restored), `nvidia-smi`) {
		t.Fatalf("restored=%s changed=%v", restored, changed)
	}
}

func TestUnknownResponseEventIsBytePreserved(t *testing.T) {
	policy := NewShellEscalationPolicy(8)
	raw := []byte("  {\"type\":\"future.event\",\"payload\":1}  ")
	got, changed := policy.RewriteResponseEvent(raw)
	if changed || string(got) != string(raw) {
		t.Fatalf("got=%q changed=%v", got, changed)
	}
}

func TestOriginalMapIsBounded(t *testing.T) {
	policy := NewShellEscalationPolicy(2)
	for _, id := range []string{"a", "b", "c"} {
		policy.Prepare(id, ShellCommandTool, `{"command":"`+id+`"}`)
	}
	if got := policy.Restore("a", ShellCommandTool, "prepared"); got != "prepared" {
		t.Fatalf("oldest mapping was retained: %s", got)
	}
	if got := policy.Restore("c", ShellCommandTool, "prepared"); got == "prepared" {
		t.Fatal("newest mapping was dropped")
	}
}
