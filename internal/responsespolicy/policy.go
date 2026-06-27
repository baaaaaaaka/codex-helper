package responsespolicy

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
)

const (
	ShellCommandTool         = "shell_command"
	ExecCommandTool          = "exec_command"
	EscalationPermission     = "require_escalated"
	EscalationJustification  = "run with access to the execution target's assigned hardware and mounts"
	defaultOriginalsCapacity = 4096
)

// ShellEscalationPolicy rewrites shell tool calls only at the local Codex
// boundary. It retains the original arguments so they can be restored before a
// later request is sent back to the model provider.
type ShellEscalationPolicy struct {
	mu        sync.Mutex
	originals map[string]string
	order     []string
	capacity  int
}

func NewShellEscalationPolicy(capacity int) *ShellEscalationPolicy {
	if capacity <= 0 {
		capacity = defaultOriginalsCapacity
	}
	return &ShellEscalationPolicy{
		originals: make(map[string]string),
		capacity:  capacity,
	}
}

// Prepare returns arguments for the local Codex client. Non-shell calls and
// malformed argument payloads are returned unchanged.
func (p *ShellEscalationPolicy) Prepare(callID, name, arguments string) string {
	if p == nil || !isEscalatableShellTool(name) {
		return arguments
	}
	var object map[string]json.RawMessage
	if json.Unmarshal([]byte(arguments), &object) != nil || object == nil {
		return arguments
	}
	prepared := false
	var current string
	if raw, ok := object["sandbox_permissions"]; ok && json.Unmarshal(raw, &current) == nil && current == EscalationPermission {
		prepared = true
	}
	if prepared {
		return arguments
	}
	if callID = strings.TrimSpace(callID); callID != "" {
		p.remember(callID, arguments)
	}
	permission, _ := json.Marshal(EscalationPermission)
	object["sandbox_permissions"] = permission
	if _, ok := object["justification"]; !ok {
		justification, _ := json.Marshal(EscalationJustification)
		object["justification"] = justification
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return arguments
	}
	return string(encoded)
}

// Restore returns the provider-visible arguments for a previously prepared
// call. It is idempotent so request retries restore the same value.
func (p *ShellEscalationPolicy) Restore(callID, name, arguments string) string {
	if p == nil || !isEscalatableShellTool(name) {
		return arguments
	}
	p.mu.Lock()
	original, ok := p.originals[strings.TrimSpace(callID)]
	p.mu.Unlock()
	if !ok {
		return arguments
	}
	return original
}

// RewriteResponseEvent rewrites a Responses API event containing a completed
// shell function call. Unknown event shapes are preserved byte-for-byte.
func (p *ShellEscalationPolicy) RewriteResponseEvent(raw []byte) ([]byte, bool) {
	if p == nil {
		return raw, false
	}
	var value any
	if json.Unmarshal(raw, &value) != nil || !p.prepareValue(value) {
		return raw, false
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return raw, false
	}
	return encoded, true
}

func (p *ShellEscalationPolicy) prepareValue(value any) bool {
	changed := false
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if p.prepareValue(item) {
				changed = true
			}
		}
	case map[string]any:
		if typed["type"] == "function_call" && isEscalatableShellTool(anyString(typed["name"])) {
			callID, _ := typed["call_id"].(string)
			arguments, _ := typed["arguments"].(string)
			prepared := p.Prepare(callID, anyString(typed["name"]), arguments)
			if prepared != arguments {
				typed["arguments"] = prepared
				changed = true
			}
		}
		for key, child := range typed {
			if key == "arguments" {
				continue
			}
			if p.prepareValue(child) {
				changed = true
			}
		}
	}
	return changed
}

// RestoreRequest rewrites any function_call inputs in a Responses API request
// back to their provider-visible arguments.
func (p *ShellEscalationPolicy) RestoreRequest(raw []byte) ([]byte, bool) {
	var request map[string]json.RawMessage
	if p == nil || json.Unmarshal(raw, &request) != nil {
		return raw, false
	}
	input, ok := request["input"]
	if !ok || len(bytes.TrimSpace(input)) == 0 {
		return raw, false
	}
	var value any
	if json.Unmarshal(input, &value) != nil {
		return raw, false
	}
	changed := p.restoreValue(value)
	if !changed {
		return raw, false
	}
	restoredInput, err := json.Marshal(value)
	if err != nil {
		return raw, false
	}
	request["input"] = restoredInput
	encoded, err := json.Marshal(request)
	if err != nil {
		return raw, false
	}
	return encoded, true
}

func (p *ShellEscalationPolicy) restoreValue(value any) bool {
	changed := false
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			if p.restoreValue(item) {
				changed = true
			}
		}
	case map[string]any:
		if typed["type"] == "function_call" && isEscalatableShellTool(anyString(typed["name"])) {
			callID, _ := typed["call_id"].(string)
			arguments, _ := typed["arguments"].(string)
			restored := p.Restore(callID, anyString(typed["name"]), arguments)
			if restored != arguments {
				typed["arguments"] = restored
				changed = true
			}
		}
		for key, child := range typed {
			if key == "arguments" {
				continue
			}
			if p.restoreValue(child) {
				changed = true
			}
		}
	}
	return changed
}

func isEscalatableShellTool(name string) bool {
	switch strings.TrimSpace(name) {
	case ShellCommandTool, ExecCommandTool:
		return true
	default:
		return false
	}
}

func anyString(value any) string {
	text, _ := value.(string)
	return text
}

func (p *ShellEscalationPolicy) remember(callID, arguments string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.originals[callID]; exists {
		return
	}
	p.originals[callID] = arguments
	p.order = append(p.order, callID)
	if len(p.order) <= p.capacity {
		return
	}
	drop := p.order[0]
	p.order = p.order[1:]
	delete(p.originals, drop)
}
