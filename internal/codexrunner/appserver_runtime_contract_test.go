package codexrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexcontract"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

const runtimeContractMarker = "cxp-standard-approval-runtime"

type runtimeContractAdapter struct {
	mu                sync.Mutex
	calls             int
	sentinel          string
	workingDir        string
	originalArguments string
	toolName          string
	restoredHistory   bool
}

func (a *runtimeContractAdapter) Stream(_ context.Context, request responsesadapter.ProviderRequest) (<-chan responsesadapter.ProviderEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if a.calls == 1 {
		name, arguments, err := runtimeContractToolCall(request.Tools, a.sentinel, a.workingDir)
		if err != nil {
			return nil, err
		}
		a.toolName = name
		a.originalArguments = arguments
		return runtimeContractEvents(
			responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventToolCallDelta, ToolCall: &responsesadapter.ProviderToolCallDelta{
				Index: 0, ID: "call_cxp_contract", Name: name, ArgumentsDelta: arguments,
			}},
			responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventDone},
		), nil
	}

	for _, message := range request.Messages {
		for _, call := range message.ToolCalls {
			if call.ID != "call_cxp_contract" {
				continue
			}
			if call.Name != a.toolName {
				return nil, fmt.Errorf("provider history tool name = %q, want %q", call.Name, a.toolName)
			}
			if !sameJSONObject(call.Arguments, a.originalArguments) {
				return nil, fmt.Errorf("provider-visible tool arguments changed: got %s want %s", call.Arguments, a.originalArguments)
			}
			if strings.Contains(call.Arguments, "sandbox_permissions") || strings.Contains(call.Arguments, "justification") {
				return nil, fmt.Errorf("provider-visible history leaked local approval arguments: %s", call.Arguments)
			}
			a.restoredHistory = true
		}
	}
	if !a.restoredHistory {
		return nil, fmt.Errorf("provider request did not contain the completed contract tool call")
	}
	return runtimeContractEvents(
		responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventTextDelta, Delta: "contract complete"},
		responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventDone},
	), nil
}

func runtimeContractEvents(events ...responsesadapter.ProviderEvent) <-chan responsesadapter.ProviderEvent {
	result := make(chan responsesadapter.ProviderEvent, len(events))
	for _, event := range events {
		result <- event
	}
	close(result)
	return result
}

func runtimeContractToolCall(tools []responsesadapter.ChatTool, sentinel, workingDir string) (string, string, error) {
	byName := make(map[string]responsesadapter.ChatTool, len(tools))
	for _, tool := range tools {
		byName[tool.Function.Name] = tool
	}
	for _, name := range []string{responsespolicy.ExecCommandTool, responsespolicy.ShellCommandTool} {
		tool, ok := byName[name]
		if !ok {
			continue
		}
		var schema struct {
			Properties map[string]json.RawMessage `json:"properties"`
		}
		if err := json.Unmarshal(tool.Function.Parameters, &schema); err != nil {
			return "", "", fmt.Errorf("decode %s schema: %w", name, err)
		}
		if _, ok := schema.Properties["sandbox_permissions"]; !ok {
			return "", "", fmt.Errorf("%s schema no longer exposes sandbox_permissions", name)
		}
		command := runtimeContractCommand(sentinel)
		arguments := map[string]any{"workdir": workingDir}
		if name == responsespolicy.ExecCommandTool {
			arguments["cmd"] = command
		} else {
			arguments["command"] = command
		}
		encoded, err := json.Marshal(arguments)
		if err != nil {
			return "", "", err
		}
		return name, string(encoded), nil
	}
	return "", "", fmt.Errorf("Codex tool request exposed neither %s nor %s", responsespolicy.ExecCommandTool, responsespolicy.ShellCommandTool)
}

func runtimeContractCommand(sentinel string) string {
	if runtime.GOOS == "windows" {
		return "Set-Content -LiteralPath '" + strings.ReplaceAll(sentinel, "'", "''") + "' -Value '" + runtimeContractMarker + "' -NoNewline"
	}
	return "printf '%s' '" + runtimeContractMarker + "' > '" + strings.ReplaceAll(sentinel, "'", "'\"'\"'") + "'"
}

func sameJSONObject(left, right string) bool {
	var leftValue any
	var rightValue any
	if json.Unmarshal([]byte(left), &leftValue) != nil || json.Unmarshal([]byte(right), &rightValue) != nil {
		return left == right
	}
	leftJSON, _ := json.Marshal(leftValue)
	rightJSON, _ := json.Marshal(rightValue)
	return string(leftJSON) == string(rightJSON)
}

func TestInstalledCodexStandardApprovalRuntime(t *testing.T) {
	if os.Getenv("CODEX_RUNTIME_E2E_TEST") != "1" {
		t.Skip("set CODEX_RUNTIME_E2E_TEST=1 to exercise the installed Codex binary")
	}
	command := strings.TrimSpace(os.Getenv("CXP_CONTRACT_CODEX"))
	if command == "" {
		var err error
		command, err = exec.LookPath("codex")
		if err != nil {
			t.Fatalf("codex not found in PATH: %v", err)
		}
	}
	preflightCtx, preflightCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer preflightCancel()
	if _, err := codexcontract.Probe(preflightCtx, command); err != nil {
		t.Fatalf("Codex runtime contract preflight: %v", err)
	}

	root := t.TempDir()
	workingDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(root, "outside-workspace.txt")
	adapter := &runtimeContractAdapter{sentinel: sentinel, workingDir: workingDir}
	provider := httptest.NewServer(&responsesadapter.Facade{
		Adapter:      adapter,
		Store:        responsesadapter.NewMemoryStore(),
		DefaultModel: "gpt-5.4",
		Models:       []responsesadapter.ModelInfo{{ID: "gpt-5.4", OwnedBy: "cxp-contract"}},
	})
	defer provider.Close()

	codexHome := filepath.Join(root, "codex-home")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	runner := &AppServerRunner{
		Starter: PolicyAppServerStarter{ServerOptions: responsespolicy.ServerOptions{
			OpenAIUpstream: provider.URL + "/v1",
		}},
		Command: command,
		AppServerArgs: []string{
			"--analytics-default-enabled",
			"-c", `model="gpt-5.4"`,
			"-c", `model_provider="openai"`,
			"-c", `features.responses_websockets=false`,
			"-c", `features.responses_websockets_v2=false`,
		},
		ExtraEnv: []string{
			"CODEX_HOME=" + codexHome,
			"OPENAI_API_KEY=cxp-contract-key",
		},
		WorkingDir: workingDir,
		Timeout:    60 * time.Second,
	}
	defer runner.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	started := time.Now()
	result, err := runner.StartThread(ctx, TurnInput{
		Prompt:     "Use the available shell tool exactly once, then report completion.",
		WorkingDir: workingDir,
	})
	if err != nil {
		t.Fatalf("standard approval runtime: %v", err)
	}
	if elapsed := time.Since(started); elapsed < DefaultApprovalDelay {
		t.Fatalf("runtime completed in %s, shorter than approval delay %s", elapsed, DefaultApprovalDelay)
	}
	if result.Status != TurnStatusCompleted || !strings.Contains(result.FinalAgentMessage, "contract complete") {
		t.Fatalf("turn result = %#v", result)
	}
	raw, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("read unsandboxed sentinel: %v", err)
	}
	if string(raw) != runtimeContractMarker {
		t.Fatalf("sentinel = %q, want %q", raw, runtimeContractMarker)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.calls < 2 || !adapter.restoredHistory {
		t.Fatalf("provider adapter state = calls:%d restored:%v", adapter.calls, adapter.restoredHistory)
	}
}
