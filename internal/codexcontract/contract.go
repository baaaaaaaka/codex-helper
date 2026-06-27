package codexcontract

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

var requiredClientMethods = map[string]string{
	"initialize":     "#/definitions/InitializeParams",
	"thread/list":    "#/definitions/ThreadListParams",
	"thread/read":    "#/definitions/ThreadReadParams",
	"thread/resume":  "#/definitions/ThreadResumeParams",
	"thread/start":   "#/definitions/ThreadStartParams",
	"turn/interrupt": "#/definitions/TurnInterruptParams",
	"turn/start":     "#/definitions/TurnStartParams",
}

var requiredServerMethods = map[string]string{
	"item/commandExecution/requestApproval": "#/definitions/CommandExecutionRequestApprovalParams",
	"item/fileChange/requestApproval":       "#/definitions/FileChangeRequestApprovalParams",
	"item/permissions/requestApproval":      "#/definitions/PermissionsRequestApprovalParams",
	"mcpServer/elicitation/request":         "#/definitions/McpServerElicitationRequestParams",
}

var requiredServerNotifications = map[string]string{
	"item/agentMessage/delta": "#/definitions/AgentMessageDeltaNotification",
	"item/completed":          "#/definitions/ItemCompletedNotification",
	"item/started":            "#/definitions/ItemStartedNotification",
	"turn/completed":          "#/definitions/TurnCompletedNotification",
	"turn/started":            "#/definitions/TurnStartedNotification",
}

type Report struct {
	Version             string
	ClientMethods       []string
	ServerMethods       []string
	ServerNotifications []string
}

// Probe validates the auth-free Codex CLI and app-server contracts used by
// the standard approval runtime. It intentionally does not start a model turn.
func Probe(ctx context.Context, command string) (Report, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		command = "codex"
	}
	version, err := commandOutput(ctx, command, "--version")
	if err != nil {
		return Report{}, fmt.Errorf("probe Codex version: %w", err)
	}
	help, err := commandOutput(ctx, command, "--help")
	if err != nil {
		return Report{}, fmt.Errorf("probe Codex help: %w", err)
	}
	if !strings.Contains(help, "--remote") {
		return Report{}, fmt.Errorf("Codex %s does not expose the required --remote TUI transport", strings.TrimSpace(version))
	}
	if err := requireHelpTokens("Codex top-level help", help, []string{
		"exec", "review", "app-server", "resume", "fork", "--add-dir",
	}); err != nil {
		return Report{}, err
	}
	execHelp, err := commandOutput(ctx, command, "exec", "--help")
	if err != nil {
		return Report{}, fmt.Errorf("probe Codex exec help: %w", err)
	}
	if err := requireHelpTokens("Codex exec help", execHelp, []string{
		"--ephemeral", "--output-schema", "--output-last-message", "--json", "--add-dir",
	}); err != nil {
		return Report{}, err
	}
	resumeHelp, err := commandOutput(ctx, command, "exec", "resume", "--help")
	if err != nil {
		return Report{}, fmt.Errorf("probe Codex exec resume help: %w", err)
	}
	if err := requireHelpTokens("Codex exec resume help", resumeHelp, []string{"--last", "--all", "--ephemeral"}); err != nil {
		return Report{}, err
	}
	appServerHelp, err := commandOutput(ctx, command, "app-server", "--help")
	if err != nil {
		return Report{}, fmt.Errorf("probe Codex app-server help: %w", err)
	}
	for _, required := range []string{"--analytics-default-enabled", "generate-json-schema"} {
		if !strings.Contains(appServerHelp, required) {
			return Report{}, fmt.Errorf("Codex %s app-server help does not expose %s", strings.TrimSpace(version), required)
		}
	}

	schemaDir, err := os.MkdirTemp("", "cxp-codex-contract-")
	if err != nil {
		return Report{}, fmt.Errorf("create Codex schema directory: %w", err)
	}
	defer os.RemoveAll(schemaDir)
	if _, err := commandOutput(ctx, command, "app-server", "generate-json-schema", "--experimental", "--out", schemaDir); err != nil {
		return Report{}, fmt.Errorf("generate Codex app-server schema: %w", err)
	}

	clientSchema, err := readSchema(filepath.Join(schemaDir, "ClientRequest.json"))
	if err != nil {
		return Report{}, err
	}
	serverSchema, err := readSchema(filepath.Join(schemaDir, "ServerRequest.json"))
	if err != nil {
		return Report{}, err
	}
	notificationSchema, err := readSchema(filepath.Join(schemaDir, "ServerNotification.json"))
	if err != nil {
		return Report{}, err
	}
	clientMethodParams := requestMethodParams(clientSchema)
	serverMethodParams := requestMethodParams(serverSchema)
	serverNotificationParams := requestMethodParams(notificationSchema)
	if err := requireMethodParams("client method", clientMethodParams, requiredClientMethods); err != nil {
		return Report{}, err
	}
	if err := requireMethodParams("server method", serverMethodParams, requiredServerMethods); err != nil {
		return Report{}, err
	}
	if err := requireMethodParams("server notification", serverNotificationParams, requiredServerNotifications); err != nil {
		return Report{}, err
	}
	threadStart, err := readSchema(filepath.Join(schemaDir, "v2", "ThreadStartParams.json"))
	if err != nil {
		return Report{}, err
	}
	if err := requireObjectShape("ThreadStartParams.json", threadStart, nil, []string{"cwd", "runtimeWorkspaceRoots", "ephemeral"}); err != nil {
		return Report{}, err
	}
	turnStart, err := readSchema(filepath.Join(schemaDir, "v2", "TurnStartParams.json"))
	if err != nil {
		return Report{}, err
	}
	if err := requireObjectShape("TurnStartParams.json", turnStart, nil, []string{"threadId", "cwd", "runtimeWorkspaceRoots", "outputSchema"}); err != nil {
		return Report{}, err
	}

	responses := []struct {
		file       string
		required   []string
		properties []string
		value      string
	}{
		{"CommandExecutionRequestApprovalResponse.json", []string{"decision"}, []string{"decision"}, "accept"},
		{"FileChangeRequestApprovalResponse.json", []string{"decision"}, []string{"decision"}, "accept"},
		{"McpServerElicitationRequestResponse.json", []string{"action"}, []string{"_meta", "action", "content"}, "accept"},
		{"PermissionsRequestApprovalResponse.json", []string{"permissions"}, []string{"permissions", "scope"}, "turn"},
	}
	for _, response := range responses {
		schema, err := readSchema(filepath.Join(schemaDir, response.file))
		if err != nil {
			return Report{}, err
		}
		if err := requireObjectShape(response.file, schema, response.required, response.properties); err != nil {
			return Report{}, err
		}
		if !contains(stringEnums(schema), response.value) {
			return Report{}, fmt.Errorf("%s no longer accepts %q", response.file, response.value)
		}
	}
	requests := []struct {
		file       string
		required   []string
		properties []string
	}{
		{"CommandExecutionRequestApprovalParams.json", []string{"itemId", "threadId", "turnId"}, []string{"itemId", "threadId", "turnId"}},
		{"FileChangeRequestApprovalParams.json", []string{"itemId", "threadId", "turnId"}, []string{"itemId", "threadId", "turnId"}},
		{"PermissionsRequestApprovalParams.json", []string{"itemId", "permissions", "threadId", "turnId"}, []string{"itemId", "permissions", "threadId", "turnId"}},
		{"McpServerElicitationRequestParams.json", []string{"serverName", "threadId"}, []string{"serverName", "threadId", "_meta"}},
	}
	for _, request := range requests {
		schema, err := readSchema(filepath.Join(schemaDir, request.file))
		if err != nil {
			return Report{}, err
		}
		if err := requireObjectShape(request.file, schema, request.required, request.properties); err != nil {
			return Report{}, err
		}
	}

	return Report{
		Version:             strings.TrimSpace(version),
		ClientMethods:       sortedKeys(clientMethodParams),
		ServerMethods:       sortedKeys(serverMethodParams),
		ServerNotifications: sortedKeys(serverNotificationParams),
	}, nil
}

func requireHelpTokens(label string, help string, tokens []string) error {
	for _, token := range tokens {
		if !strings.Contains(help, token) {
			return fmt.Errorf("%s no longer exposes required command or option %q", label, token)
		}
	}
	return nil
}

func commandOutput(ctx context.Context, command string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, command, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w: %s", command, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func readSchema(path string) (any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read generated schema %s: %w", filepath.Base(path), err)
	}
	var schema any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, fmt.Errorf("decode generated schema %s: %w", filepath.Base(path), err)
	}
	return schema, nil
}

func stringEnums(value any) []string {
	seen := map[string]struct{}{}
	var walk func(any)
	walk = func(current any) {
		switch typed := current.(type) {
		case []any:
			for _, child := range typed {
				walk(child)
			}
		case map[string]any:
			if values, ok := typed["enum"].([]any); ok {
				for _, value := range values {
					if text, ok := value.(string); ok {
						seen[text] = struct{}{}
					}
				}
			}
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(value)
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func requestMethodParams(value any) map[string]string {
	result := map[string]string{}
	root, ok := value.(map[string]any)
	if !ok {
		return result
	}
	branches, _ := root["oneOf"].([]any)
	for _, rawBranch := range branches {
		branch, _ := rawBranch.(map[string]any)
		properties, _ := branch["properties"].(map[string]any)
		methodSchema, _ := properties["method"].(map[string]any)
		paramsSchema, _ := properties["params"].(map[string]any)
		paramRef, _ := paramsSchema["$ref"].(string)
		for _, method := range stringEnums(methodSchema) {
			result[method] = paramRef
		}
	}
	return result
}

func requireMethodParams(kind string, got, required map[string]string) error {
	for method, wantedRef := range required {
		gotRef, ok := got[method]
		if !ok {
			return fmt.Errorf("generated app-server schema is missing required %s %q", kind, method)
		}
		if gotRef != wantedRef {
			return fmt.Errorf("generated app-server schema changed %s %q params binding: got %q want %q", kind, method, gotRef, wantedRef)
		}
	}
	return nil
}

func requireObjectShape(file string, value any, required, properties []string) error {
	root, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("%s is no longer an object schema", file)
	}
	requiredSet := map[string]struct{}{}
	if values, ok := root["required"].([]any); ok {
		for _, value := range values {
			if text, ok := value.(string); ok {
				requiredSet[text] = struct{}{}
			}
		}
	}
	for _, name := range required {
		if _, ok := requiredSet[name]; !ok {
			return fmt.Errorf("%s no longer requires top-level property %q", file, name)
		}
	}
	available := immediateProperties(root)
	for _, name := range properties {
		if _, ok := available[name]; !ok {
			return fmt.Errorf("%s no longer exposes flattened property %q", file, name)
		}
	}
	return nil
}

func immediateProperties(schema map[string]any) map[string]struct{} {
	result := map[string]struct{}{}
	var collect func(map[string]any)
	collect = func(current map[string]any) {
		if properties, ok := current["properties"].(map[string]any); ok {
			for name := range properties {
				result[name] = struct{}{}
			}
		}
		for _, combinator := range []string{"allOf", "anyOf", "oneOf"} {
			children, _ := current[combinator].([]any)
			for _, rawChild := range children {
				if child, ok := rawChild.(map[string]any); ok {
					collect(child)
				}
			}
		}
	}
	collect(schema)
	return result
}

func sortedKeys(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
