package codexcontract

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestHelpHasOptionRequiresExactFlag(t *testing.T) {
	help := "Options: --remote-auth-token-env <ENV_VAR>"
	if helpHasOption(help, "--remote") {
		t.Fatal("--remote-auth-token-env must not satisfy --remote")
	}
	if !helpHasOption(help, "--remote-auth-token-env") {
		t.Fatal("expected exact remote auth flag")
	}
}

func TestStringEnumsAndImmediateProperties(t *testing.T) {
	schema := map[string]any{
		"oneOf": []any{
			map[string]any{"properties": map[string]any{"method": map[string]any{"enum": []any{"thread/start"}}}},
			map[string]any{"properties": map[string]any{"_meta": true, "decision": map[string]any{"enum": []any{"accept"}}}},
		},
	}
	enums := stringEnums(schema)
	for _, wanted := range []string{"accept", "thread/start"} {
		if !contains(enums, wanted) {
			t.Fatalf("enums = %v, missing %q", enums, wanted)
		}
	}
	if _, ok := immediateProperties(schema)["_meta"]; !ok {
		t.Fatal("schema did not expose nested _meta property")
	}
}

func TestRequestMethodParamsDetectsBindingChange(t *testing.T) {
	schema := map[string]any{"oneOf": []any{
		map[string]any{"properties": map[string]any{
			"method": map[string]any{"enum": []any{"turn/start"}},
			"params": map[string]any{"$ref": "#/definitions/TurnStartParams"},
		}},
	}}
	methods := requestMethodParams(schema)
	if err := requireMethodParams("client method", methods, map[string]string{"turn/start": "#/definitions/TurnStartParams"}); err != nil {
		t.Fatal(err)
	}
	err := requireMethodParams("client method", methods, map[string]string{"turn/start": "#/definitions/RenamedTurnParams"})
	if err == nil || !strings.Contains(err.Error(), "params binding") {
		t.Fatalf("binding mutation error = %v", err)
	}
}

func TestRequireObjectShapeRejectsNestedApprovalMetadata(t *testing.T) {
	current := map[string]any{
		"required": []any{"serverName", "threadId"},
		"properties": map[string]any{
			"serverName": map[string]any{},
			"threadId":   map[string]any{},
		},
		"oneOf": []any{map[string]any{"properties": map[string]any{"_meta": true}}},
	}
	if err := requireObjectShape("McpServerElicitationRequestParams.json", current,
		[]string{"serverName", "threadId"}, []string{"serverName", "threadId", "_meta"}); err != nil {
		t.Fatal(err)
	}
	legacyNested := map[string]any{
		"required":   []any{"serverName", "threadId"},
		"properties": map[string]any{"serverName": map[string]any{}, "threadId": map[string]any{}, "request": map[string]any{}},
		"definitions": map[string]any{
			"NestedRequest": map[string]any{"properties": map[string]any{"_meta": true}},
		},
	}
	err := requireObjectShape("McpServerElicitationRequestParams.json", legacyNested,
		[]string{"serverName", "threadId"}, []string{"serverName", "threadId", "_meta"})
	if err == nil || !strings.Contains(err.Error(), `flattened property "_meta"`) {
		t.Fatalf("nested metadata mutation error = %v", err)
	}
}

func TestInstalledCodexRuntimeContract(t *testing.T) {
	if os.Getenv("CODEX_RUNTIME_CONTRACT_TEST") != "1" {
		t.Skip("set CODEX_RUNTIME_CONTRACT_TEST=1 to probe the installed Codex package")
	}
	command := os.Getenv("CXP_CONTRACT_CODEX")
	if command == "" {
		var err error
		command, err = exec.LookPath("codex")
		if err != nil {
			t.Fatalf("codex not found in PATH: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	report, err := Probe(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("validated %s: %d client methods, %d server methods, %d notifications", report.Version, len(report.ClientMethods), len(report.ServerMethods), len(report.ServerNotifications))
}
