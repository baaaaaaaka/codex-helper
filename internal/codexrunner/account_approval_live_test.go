package codexrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

type accountApprovalAdapter struct {
	mu          sync.Mutex
	calls       int
	workingDir  string
	sentinel    string
	safeTool    string
	shellTool   string
	providerErr error
}

func (a *accountApprovalAdapter) Stream(_ context.Context, request responsesadapter.ProviderRequest) (<-chan responsesadapter.ProviderEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	switch a.calls {
	case 1:
		if !accountApprovalHasTool(request.Tools, "update_plan") {
			a.providerErr = fmt.Errorf("current account tool set did not expose update_plan")
			return nil, a.providerErr
		}
		a.safeTool = "update_plan"
		return runtimeContractEvents(
			responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventToolCallDelta, ToolCall: &responsesadapter.ProviderToolCallDelta{
				Index:          0,
				ID:             "call_without_approval",
				Name:           "update_plan",
				ArgumentsDelta: `{"explanation":"exercise a non-approval tool","plan":[{"step":"safe tool contract","status":"completed"}]}`,
			}},
			responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventDone},
		), nil
	case 2:
		command := "printf '%s' 'cxp-account-approval-ok' > '" + strings.ReplaceAll(a.sentinel, "'", "'\"'\"'") + "'"
		name, arguments, err := runtimeContractToolCall(request.Tools, command, a.workingDir)
		if err != nil {
			a.providerErr = err
			return nil, err
		}
		a.shellTool = name
		return runtimeContractEvents(
			responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventToolCallDelta, ToolCall: &responsesadapter.ProviderToolCallDelta{
				Index: 0, ID: "call_requiring_approval", Name: name, ArgumentsDelta: arguments,
			}},
			responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventDone},
		), nil
	default:
		return runtimeContractEvents(
			responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventTextDelta, Delta: "account approval matrix complete"},
			responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventDone},
		), nil
	}
}

func accountApprovalHasTool(tools []responsesadapter.ChatTool, name string) bool {
	for _, tool := range tools {
		if tool.Function.Name == name {
			return true
		}
	}
	return false
}

type accountApprovalRecord struct {
	Method   string
	Params   json.RawMessage
	Response json.RawMessage
}

type recordingAutomaticApprovalHandler struct {
	mu      sync.Mutex
	records []accountApprovalRecord
}

func (h *recordingAutomaticApprovalHandler) HandleServerRequest(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
	response, handled, err := (AutomaticApprovalHandler{Delay: DefaultApprovalDelay}).HandleServerRequest(ctx, method, params)
	if handled {
		h.mu.Lock()
		h.records = append(h.records, accountApprovalRecord{
			Method:   method,
			Params:   append(json.RawMessage(nil), params...),
			Response: append(json.RawMessage(nil), response...),
		})
		h.mu.Unlock()
	}
	return response, handled, err
}

func (h *recordingAutomaticApprovalHandler) snapshot() []accountApprovalRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]accountApprovalRecord(nil), h.records...)
}

// TestLiveAccountPolicyApprovesEveryRequiredRequestWithoutPromptingSafeTool
// combines the current account's real workspace-managed policy with a local,
// deterministic Responses provider. The official Codex client still fetches
// and enforces the account's cloud configuration; only model generation is
// made deterministic so the contract can compare one safe tool call with one
// shell call that CXP marks for ordinary user approval.
func TestLiveAccountPolicyApprovesEveryRequiredRequestWithoutPromptingSafeTool(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CXP_LIVE_ACCOUNT_APPROVAL_TEST")) != "1" {
		t.Skip("set CXP_LIVE_ACCOUNT_APPROVAL_TEST=1 to exercise current-account approval classification")
	}
	command := strings.TrimSpace(os.Getenv("CXP_LIVE_CODEX"))
	if command == "" {
		var err error
		command, err = exec.LookPath("codex")
		if err != nil {
			t.Fatalf("codex not found in PATH: %v", err)
		}
	}
	sourceHome := strings.TrimSpace(os.Getenv("CXP_LIVE_CODEX_HOME"))
	if sourceHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		sourceHome = filepath.Join(home, ".codex")
	}
	auth, err := os.ReadFile(filepath.Join(sourceHome, "auth.json"))
	if err != nil {
		t.Fatalf("read live auth: %v", err)
	}
	originalHash := runtimeContractFileHash(t, command)

	root := t.TempDir()
	codexHome := filepath.Join(root, "codex-home")
	workingDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(workingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexHome, "auth.json"), auth, 0o600); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(root, "approved-outside-workspace.txt")
	adapter := &accountApprovalAdapter{workingDir: workingDir, sentinel: sentinel}
	provider := httptest.NewServer(&responsesadapter.Facade{
		Adapter:      adapter,
		Store:        responsesadapter.NewMemoryStore(),
		DefaultModel: "gpt-5.4",
		Models:       []responsesadapter.ModelInfo{{ID: "gpt-5.4", OwnedBy: "cxp-live-account-contract"}},
	})
	defer provider.Close()

	proxy := strings.TrimSpace(os.Getenv("CXP_LIVE_HTTP_PROXY"))
	extraEnv := []string{
		"CODEX_HOME=" + codexHome,
		"CODEX_DIR=" + codexHome,
		"HTTP_PROXY=" + proxy,
		"HTTPS_PROXY=" + proxy,
		"ALL_PROXY=",
		"http_proxy=" + proxy,
		"https_proxy=" + proxy,
		"all_proxy=",
		"NO_PROXY=127.0.0.1,localhost,::1",
		"no_proxy=127.0.0.1,localhost,::1",
	}
	handler := &recordingAutomaticApprovalHandler{}
	runner := &AppServerRunner{
		Starter: PolicyAppServerStarter{ServerOptions: responsespolicy.ServerOptions{
			OpenAIUpstream:       provider.URL + "/v1",
			ChatGPTModelUpstream: provider.URL + "/v1",
		}},
		ServerRequestHandler: handler,
		Command:              command,
		AppServerArgs: []string{
			"-c", `model="gpt-5.4"`,
			"-c", `model_provider="openai"`,
		},
		ExtraEnv:   extraEnv,
		WorkingDir: workingDir,
		Timeout:    60 * time.Second,
	}
	defer runner.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	started := time.Now()
	result, err := runner.StartThread(ctx, TurnInput{
		Prompt:     "Run the deterministic current-account approval contract.",
		WorkingDir: workingDir,
		Ephemeral:  true,
	})
	if err != nil {
		adapter.mu.Lock()
		adapterState := fmt.Sprintf("calls:%d safe:%q shell:%q err:%v", adapter.calls, adapter.safeTool, adapter.shellTool, adapter.providerErr)
		adapter.mu.Unlock()
		t.Fatalf("current-account approval contract: %v (provider %s; approvals %#v)", err, adapterState, handler.snapshot())
	}
	if result.Status != TurnStatusCompleted || !strings.Contains(result.FinalAgentMessage, "account approval matrix complete") {
		t.Fatalf("turn result = %#v", result)
	}
	if time.Since(started) < DefaultApprovalDelay {
		t.Fatalf("turn completed before the fixed approval delay")
	}
	raw, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("approved shell command did not create sentinel: %v", err)
	}
	if string(raw) != "cxp-account-approval-ok" {
		t.Fatalf("sentinel = %q", raw)
	}
	records := handler.snapshot()
	if len(records) != 1 {
		t.Fatalf("approval records = %#v, want one command approval for two tool calls", records)
	}
	commandRecord := records[0]
	if commandRecord.Method != appServerMethodCommandExecutionApproval && commandRecord.Method != appServerMethodLegacyExecApproval {
		t.Fatalf("approval method = %q, want command approval", commandRecord.Method)
	}
	wantResponse := `{"decision":"accept"}`
	if commandRecord.Method == appServerMethodLegacyExecApproval {
		wantResponse = `{"decision":"approved"}`
	}
	var gotValue, wantValue any
	if json.Unmarshal(commandRecord.Response, &gotValue) != nil || json.Unmarshal([]byte(wantResponse), &wantValue) != nil {
		t.Fatalf("invalid approval response: %s", commandRecord.Response)
	}
	gotJSON, _ := json.Marshal(gotValue)
	wantJSON, _ := json.Marshal(wantValue)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("approval response = %s, want %s", gotJSON, wantJSON)
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if adapter.calls < 3 || adapter.safeTool != "update_plan" || adapter.shellTool == "" || adapter.providerErr != nil {
		t.Fatalf("provider state = calls:%d safe:%q shell:%q err:%v", adapter.calls, adapter.safeTool, adapter.shellTool, adapter.providerErr)
	}
	if currentHash := runtimeContractFileHash(t, command); currentHash != originalHash {
		t.Fatal("original Codex command changed during live account approval contract")
	}
}

// TestLiveAccountPolicyDefaultsApprovalFailClosedWithoutAAA exercises the same
// managed-account and deterministic-provider path without installing an
// automatic handler. The safe tool still runs, the approval-required command
// is rejected, and its filesystem side effect must not happen.
func TestLiveAccountPolicyDefaultsApprovalFailClosedWithoutAAA(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CXP_LIVE_ACCOUNT_APPROVAL_TEST")) != "1" {
		t.Skip("set CXP_LIVE_ACCOUNT_APPROVAL_TEST=1 to exercise current-account approval classification")
	}
	command := strings.TrimSpace(os.Getenv("CXP_LIVE_CODEX"))
	if command == "" {
		var err error
		command, err = exec.LookPath("codex")
		if err != nil {
			t.Fatalf("codex not found in PATH: %v", err)
		}
	}
	sourceHome := strings.TrimSpace(os.Getenv("CXP_LIVE_CODEX_HOME"))
	if sourceHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		sourceHome = filepath.Join(home, ".codex")
	}
	auth, err := os.ReadFile(filepath.Join(sourceHome, "auth.json"))
	if err != nil {
		t.Fatalf("read live auth: %v", err)
	}
	originalHash := runtimeContractFileHash(t, command)

	root := t.TempDir()
	codexHome := filepath.Join(root, "codex-home")
	workingDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(workingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexHome, "auth.json"), auth, 0o600); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(root, "must-not-exist.txt")
	adapter := &accountApprovalAdapter{workingDir: workingDir, sentinel: sentinel}
	provider := httptest.NewServer(&responsesadapter.Facade{
		Adapter:      adapter,
		Store:        responsesadapter.NewMemoryStore(),
		DefaultModel: "gpt-5.4",
		Models:       []responsesadapter.ModelInfo{{ID: "gpt-5.4", OwnedBy: "cxp-live-account-manual-contract"}},
	})
	defer provider.Close()
	proxy := strings.TrimSpace(os.Getenv("CXP_LIVE_HTTP_PROXY"))
	runner := &AppServerRunner{
		Starter: PolicyAppServerStarter{ServerOptions: responsespolicy.ServerOptions{
			OpenAIUpstream:       provider.URL + "/v1",
			ChatGPTModelUpstream: provider.URL + "/v1",
		}},
		Command: command,
		AppServerArgs: []string{
			"-c", `model="gpt-5.4"`,
			"-c", `model_provider="openai"`,
		},
		ExtraEnv: []string{
			"CODEX_HOME=" + codexHome,
			"CODEX_DIR=" + codexHome,
			"HTTP_PROXY=" + proxy,
			"HTTPS_PROXY=" + proxy,
			"ALL_PROXY=",
			"NO_PROXY=127.0.0.1,localhost,::1",
		},
		WorkingDir: workingDir,
		Timeout:    60 * time.Second,
	}
	defer runner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := runner.StartThread(ctx, TurnInput{
		Prompt:     "Run the deterministic current-account manual approval contract.",
		WorkingDir: workingDir,
		Ephemeral:  true,
	})
	if err != nil {
		t.Fatalf("current-account manual approval contract: %v", err)
	}
	if result.Status != TurnStatusCompleted || !strings.Contains(result.FinalAgentMessage, "account approval matrix complete") {
		t.Fatalf("turn result = %#v", result)
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("approval-required command produced a side effect while AAA was off: %v", err)
	}
	adapter.mu.Lock()
	calls, safeTool, shellTool, providerErr := adapter.calls, adapter.safeTool, adapter.shellTool, adapter.providerErr
	adapter.mu.Unlock()
	if calls < 3 || safeTool != "update_plan" || shellTool == "" || providerErr != nil {
		t.Fatalf("provider state = calls:%d safe:%q shell:%q err:%v", calls, safeTool, shellTool, providerErr)
	}
	if currentHash := runtimeContractFileHash(t, command); currentHash != originalHash {
		t.Fatal("original Codex command changed during manual approval contract")
	}
}
