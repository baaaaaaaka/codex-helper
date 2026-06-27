package codexrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const DefaultApprovalDelay = 500 * time.Millisecond

const (
	appServerMethodCommandExecutionApproval = "item/commandExecution/requestApproval"
	appServerMethodFileChangeApproval       = "item/fileChange/requestApproval"
	appServerMethodPermissionsApproval      = "item/permissions/requestApproval"
	appServerMethodMCPElicitation           = "mcpServer/elicitation/request"
	appServerMethodLegacyExecApproval       = "execCommandApproval"
	appServerMethodLegacyPatchApproval      = "applyPatchApproval"
)

// AppServerServerRequestHandler resolves app-server initiated JSON-RPC
// requests. The bool result is false when the handler intentionally leaves the
// request for another client surface.
type AppServerServerRequestHandler interface {
	HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error)
}

type AppServerServerRequestHandlerFunc func(context.Context, string, json.RawMessage) (json.RawMessage, bool, error)

func (f AppServerServerRequestHandlerFunc) HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	return f(ctx, method, params)
}

// AutomaticApprovalHandler resolves supported approval requests with a
// one-time approval after Delay. Requests that are not approval requests are
// not handled; callers must fail them closed or forward them to a real client.
type AutomaticApprovalHandler struct {
	Delay time.Duration
}

func (h AutomaticApprovalHandler) HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	result, handled, err := automaticApprovalResult(method, params)
	if err != nil || !handled {
		return nil, handled, err
	}
	delay := h.Delay
	if delay < 0 {
		return nil, true, fmt.Errorf("approval delay must not be negative")
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, true, ctx.Err()
		case <-timer.C:
		}
	}
	return result, true, nil
}

func automaticApprovalResult(method string, params json.RawMessage) (json.RawMessage, bool, error) {
	switch strings.TrimSpace(method) {
	case appServerMethodCommandExecutionApproval, appServerMethodFileChangeApproval:
		return json.RawMessage(`{"decision":"accept"}`), true, nil
	case appServerMethodLegacyExecApproval, appServerMethodLegacyPatchApproval:
		return json.RawMessage(`{"decision":"approved"}`), true, nil
	case appServerMethodPermissionsApproval:
		permissions, err := requestedPermissionGrant(params)
		if err != nil {
			return nil, true, err
		}
		result, err := json.Marshal(map[string]any{
			"permissions": permissions,
			"scope":       "turn",
		})
		return result, true, err
	case appServerMethodMCPElicitation:
		approval, err := isMCPToolApprovalRequest(params)
		if err != nil {
			return nil, true, err
		}
		if !approval {
			return nil, false, nil
		}
		return json.RawMessage(`{"action":"accept","content":null,"_meta":null}`), true, nil
	default:
		return nil, false, nil
	}
}

func requestedPermissionGrant(params json.RawMessage) (map[string]json.RawMessage, error) {
	var envelope struct {
		Permissions map[string]json.RawMessage `json:"permissions"`
	}
	if err := json.Unmarshal(params, &envelope); err != nil {
		return nil, fmt.Errorf("decode permissions approval request: %w", err)
	}
	if envelope.Permissions == nil {
		return nil, fmt.Errorf("permissions approval request did not include permissions")
	}
	grant := make(map[string]json.RawMessage, 2)
	for _, key := range []string{"network", "fileSystem"} {
		if value, ok := envelope.Permissions[key]; ok && len(bytes.TrimSpace(value)) > 0 && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			grant[key] = value
		}
	}
	return grant, nil
}

func isMCPToolApprovalRequest(params json.RawMessage) (bool, error) {
	var envelope struct {
		Meta    map[string]json.RawMessage `json:"_meta"`
		Request struct {
			Meta map[string]json.RawMessage `json:"_meta"`
		} `json:"request"`
	}
	if err := json.Unmarshal(params, &envelope); err != nil {
		return false, fmt.Errorf("decode MCP elicitation request: %w", err)
	}
	meta := envelope.Meta
	if meta == nil {
		meta = envelope.Request.Meta
	}
	var kind string
	if raw, ok := meta["codex_approval_kind"]; ok {
		if err := json.Unmarshal(raw, &kind); err != nil {
			return false, fmt.Errorf("decode MCP approval kind: %w", err)
		}
	}
	return kind == "mcp_tool_call", nil
}
