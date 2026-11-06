package codex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// Approval requests the app-server sends to its client. The helper runs
// turns unattended, so supported approvals are granted once (copied from
// codexrunner's automatic approval mode); anything else is answered with a
// JSON-RPC error so Codex does not wait forever.
const (
	methodCommandApproval     = "item/commandExecution/requestApproval"
	methodFileChangeApproval  = "item/fileChange/requestApproval"
	methodPermissionsApproval = "item/permissions/requestApproval"
	methodMCPElicitation      = "mcpServer/elicitation/request"
	methodLegacyExecApproval  = "execCommandApproval"
	methodLegacyPatchApproval = "applyPatchApproval"
)

// AutoApprove returns the response to a server request, or handled=false.
func AutoApprove(method string, params json.RawMessage) (result json.RawMessage, handled bool, err error) {
	switch strings.TrimSpace(method) {
	case methodCommandApproval, methodFileChangeApproval:
		return json.RawMessage(`{"decision":"accept"}`), true, nil
	case methodLegacyExecApproval, methodLegacyPatchApproval:
		return json.RawMessage(`{"decision":"approved"}`), true, nil
	case methodPermissionsApproval:
		grant, err := requestedPermissionGrant(params)
		if err != nil {
			return nil, true, err
		}
		out, err := json.Marshal(map[string]any{"permissions": grant, "scope": "turn"})
		return out, true, err
	case methodMCPElicitation:
		ok, err := isMCPToolApproval(params)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, false, nil
		}
		return json.RawMessage(`{"action":"accept","content":null,"_meta":null}`), true, nil
	}
	return nil, false, nil
}

func requestedPermissionGrant(params json.RawMessage) (map[string]json.RawMessage, error) {
	var env struct {
		Permissions map[string]json.RawMessage `json:"permissions"`
	}
	if err := json.Unmarshal(params, &env); err != nil {
		return nil, fmt.Errorf("decode permissions approval request: %w", err)
	}
	if env.Permissions == nil {
		return nil, fmt.Errorf("permissions approval request did not include permissions")
	}
	grant := make(map[string]json.RawMessage, 2)
	for _, k := range []string{"network", "fileSystem"} {
		if v, ok := env.Permissions[k]; ok && len(bytes.TrimSpace(v)) > 0 && !bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			grant[k] = v
		}
	}
	return grant, nil
}

func isMCPToolApproval(params json.RawMessage) (bool, error) {
	var env struct {
		Meta    map[string]json.RawMessage `json:"_meta"`
		Request struct {
			Meta map[string]json.RawMessage `json:"_meta"`
		} `json:"request"`
	}
	if err := json.Unmarshal(params, &env); err != nil {
		return false, fmt.Errorf("decode MCP elicitation request: %w", err)
	}
	meta := env.Meta
	if meta == nil {
		meta = env.Request.Meta
	}
	var kind string
	if raw, ok := meta["codex_approval_kind"]; ok {
		if err := json.Unmarshal(raw, &kind); err != nil {
			return false, fmt.Errorf("decode MCP approval kind: %w", err)
		}
	}
	return kind == "mcp_tool_call", nil
}
