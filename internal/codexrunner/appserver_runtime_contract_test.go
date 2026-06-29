package codexrunner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	"github.com/gorilla/websocket"
)

const runtimeContractMarker = "cxp-standard-approval-runtime"

type runtimeContractAdapter struct {
	mu                sync.Mutex
	calls             int
	sentinel          string
	workingDir        string
	command           string
	originalArguments string
	toolName          string
	restoredHistory   bool
}

func (a *runtimeContractAdapter) Stream(_ context.Context, request responsesadapter.ProviderRequest) (<-chan responsesadapter.ProviderEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if a.calls == 1 {
		command := strings.TrimSpace(a.command)
		if command == "" {
			command = runtimeContractCommand(a.sentinel)
		}
		name, arguments, err := runtimeContractToolCall(request.Tools, command, a.workingDir)
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

func runtimeContractToolCall(tools []responsesadapter.ChatTool, command, workingDir string) (string, string, error) {
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
	originalCommandHash := runtimeContractFileHash(t, command)
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
	var providerMu sync.Mutex
	var providerRequests, providerWebSockets, providerWebSocketMessages int
	var analyticsRequests [][]byte
	providerFacade := &responsesadapter.Facade{
		Adapter:      adapter,
		Store:        responsesadapter.NewMemoryStore(),
		DefaultModel: "gpt-5.4",
		Models:       []responsesadapter.ModelInfo{{ID: "gpt-5.4", OwnedBy: "cxp-contract"}},
		WebSocketRequestHook: func() {
			providerMu.Lock()
			providerWebSocketMessages++
			providerMu.Unlock()
		},
	}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/codex/analytics-events/events" {
			body, _ := io.ReadAll(r.Body)
			providerMu.Lock()
			analyticsRequests = append(analyticsRequests, append([]byte(nil), body...))
			providerMu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		providerMu.Lock()
		providerRequests++
		if websocket.IsWebSocketUpgrade(r) {
			providerWebSockets++
		}
		providerMu.Unlock()
		providerFacade.ServeHTTP(w, r)
	}))
	defer provider.Close()

	codexHome := filepath.Join(root, "codex-home")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	writeRuntimeContractChatGPTAuth(t, codexHome)
	runner := &AppServerRunner{
		Starter: PolicyAppServerStarter{ServerOptions: responsespolicy.ServerOptions{
			OpenAIUpstream:       provider.URL + "/v1",
			ChatGPTModelUpstream: provider.URL + "/v1",
		}},
		ApprovalMode: ApprovalModeAutomatic,
		Command:      command,
		AppServerArgs: []string{
			"--analytics-default-enabled",
			"-c", `model="gpt-5.4"`,
			"-c", `model_provider="openai"`,
			// The contract owns this synthetic ChatGPT origin. Production CXP no
			// longer overrides chatgpt_base_url, so the test supplies its own URL
			// explicitly to keep analytics local and deterministic.
			"-c", `chatgpt_base_url="` + provider.URL + `"`,
			"-c", `features.plugins=false`,
		},
		ExtraEnv: []string{
			"CODEX_HOME=" + codexHome,
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
		adapter.mu.Lock()
		state := fmt.Sprintf("calls:%d tool:%s restored:%v", adapter.calls, adapter.toolName, adapter.restoredHistory)
		adapter.mu.Unlock()
		providerMu.Lock()
		providerState := fmt.Sprintf("requests:%d websockets:%d websocket_messages:%d", providerRequests, providerWebSockets, providerWebSocketMessages)
		providerMu.Unlock()
		t.Fatalf("standard approval runtime: %v (adapter %s; provider %s)", err, state, providerState)
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
	// Codex sends analytics asynchronously. Older supported builds can finish
	// the turn before flushing the command-completion event even though the
	// review event has already arrived. Wait only for the exact event required
	// by this contract, with a short hard deadline so telemetry regressions still
	// fail deterministically instead of becoming timing-dependent flakes.
	analyticsDeadline := time.Now().Add(5 * time.Second)
	var analytics []byte
	for {
		providerMu.Lock()
		analytics = bytes.Join(analyticsRequests, []byte("\n"))
		providerMu.Unlock()
		if bytes.Contains(analytics, []byte(`"codex_command_execution_event"`)) || time.Now().After(analyticsDeadline) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(analytics) == 0 {
		t.Fatal("analytics was enabled but original Codex sent no analytics contract payload")
	}
	if !json.Valid(analyticsRequests[0]) || !bytes.Contains(analytics, []byte(`"events"`)) {
		t.Fatalf("invalid analytics contract payload: %s", analytics)
	}
	lowerAnalytics := bytes.ToLower(analytics)
	for _, forbidden := range [][]byte{
		[]byte(`yolo`),
		[]byte(`dangerously-bypass-approvals-and-sandbox`),
		[]byte(`danger-full-access`),
		[]byte(`bypasspermissions`),
		[]byte(`automatic_review`),
		[]byte(`automatic-review`),
		[]byte(`--aaa`),
		[]byte(`agent_auto_approve`),
		[]byte(`auto_approve`),
	} {
		if bytes.Contains(lowerAnalytics, forbidden) {
			t.Fatalf("analytics contains retired execution signal %q: %s", forbidden, analytics)
		}
	}
	if bytes.Contains(analytics, []byte("chatgpt-contract-token")) || bytes.Contains(analytics, []byte("refresh-contract-token")) {
		t.Fatalf("analytics payload leaked authentication material: %s", analytics)
	}
	var sawUserApproval, sawUserApprovedCommand bool
	for _, raw := range analyticsRequests {
		var payload struct {
			Events []struct {
				Type   string         `json:"event_type"`
				Params map[string]any `json:"event_params"`
			} `json:"events"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("decode analytics contract payload: %v", err)
		}
		for _, event := range payload.Events {
			switch event.Type {
			case "codex_review_event":
				if event.Params["reviewer"] == "user" && event.Params["status"] == "approved" {
					sawUserApproval = true
				}
			case "codex_command_execution_event":
				if event.Params["final_approval_outcome"] == "user_approved" && event.Params["terminal_status"] == "completed" {
					sawUserApprovedCommand = true
				}
			}
		}
	}
	if !sawUserApproval || !sawUserApprovedCommand {
		t.Fatalf("analytics does not describe an ordinary user approval and completed command: %s", analytics)
	}
	if currentHash := runtimeContractFileHash(t, command); currentHash != originalCommandHash {
		t.Fatal("original Codex command changed during the standard approval contract")
	}
}

func runtimeContractFileHash(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read original Codex command %s: %v", path, err)
	}
	return sha256.Sum256(raw)
}

func writeRuntimeContractChatGPTAuth(t *testing.T, codexHome string) {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "none", "typ": "JWT"})
	payload, _ := json.Marshal(map[string]any{
		"email": "cxp-contract@example.test",
		"https://api.openai.com/auth": map[string]string{
			"chatgpt_user_id":    "user-contract",
			"chatgpt_account_id": "account-contract",
		},
	})
	jwt := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload) + ".c2lnbmF0dXJl"
	auth, err := json.Marshal(map[string]any{
		"auth_mode":      "chatgpt",
		"OPENAI_API_KEY": nil,
		"tokens": map[string]any{
			"id_token":      jwt,
			"access_token":  "chatgpt-contract-token",
			"refresh_token": "refresh-contract-token",
			"account_id":    "account-contract",
		},
		"last_refresh": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexHome, "auth.json"), auth, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestInstalledCodexStandardApprovalRuntimeCUDA(t *testing.T) {
	if os.Getenv("CXP_GPU_RUNTIME_E2E_TEST") != "1" {
		t.Skip("set CXP_GPU_RUNTIME_E2E_TEST=1 on a CUDA host to exercise the approved hardware path")
	}
	if runtime.GOOS != "linux" {
		t.Skip("the CUDA runtime contract currently targets Linux NVIDIA hosts")
	}
	nvcc, err := exec.LookPath("nvcc")
	if err != nil {
		t.Fatal("nvcc is required for the CUDA runtime contract")
	}
	command := strings.TrimSpace(os.Getenv("CXP_CONTRACT_CODEX"))
	if command == "" {
		command, err = exec.LookPath("codex")
		if err != nil {
			t.Fatalf("codex not found in PATH: %v", err)
		}
	}
	root := t.TempDir()
	workingDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "cuda_probe.cu")
	probe := filepath.Join(root, "cuda_probe")
	program := []byte(`#include <cuda_runtime.h>
#include <cstdio>
__global__ void add_one(int *value) { *value += 1; }
int main() {
  int initial = 41;
  int result = 0;
  int *device = nullptr;
  if (cudaMalloc(&device, sizeof(int)) != cudaSuccess) return 2;
  if (cudaMemcpy(device, &initial, sizeof(int), cudaMemcpyHostToDevice) != cudaSuccess) return 3;
  add_one<<<1, 1>>>(device);
  if (cudaDeviceSynchronize() != cudaSuccess) return 4;
  if (cudaMemcpy(&result, device, sizeof(int), cudaMemcpyDeviceToHost) != cudaSuccess) return 5;
  cudaFree(device);
  if (result != 42) return 6;
  std::printf("cxp-cuda-ok:%d\n", result);
  return 0;
}
`)
	if err := os.WriteFile(source, program, 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(nvcc, "-O2", source, "-o", probe).CombinedOutput(); err != nil {
		t.Fatalf("compile CUDA contract: %v\n%s", err, output)
	}
	sentinel := filepath.Join(root, "cuda-output.txt")
	adapter := &runtimeContractAdapter{
		sentinel:   sentinel,
		workingDir: workingDir,
		command:    runtimeContractShellQuote(probe) + " > " + runtimeContractShellQuote(sentinel),
	}
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
		ApprovalMode: ApprovalModeAutomatic,
		Command:      command,
		AppServerArgs: []string{
			"--analytics-default-enabled",
			"-c", `model="gpt-5.4"`,
			"-c", `model_provider="openai"`,
			"-c", `features.plugins=false`,
		},
		ExtraEnv:   []string{"CODEX_HOME=" + codexHome, "OPENAI_API_KEY=cxp-contract-key"},
		WorkingDir: workingDir,
		Timeout:    60 * time.Second,
	}
	defer runner.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	started := time.Now()
	result, err := runner.StartThread(ctx, TurnInput{Prompt: "Run the supplied GPU contract tool exactly once.", WorkingDir: workingDir})
	if err != nil {
		t.Fatalf("CUDA standard approval runtime: %v", err)
	}
	if time.Since(started) < DefaultApprovalDelay {
		t.Fatal("CUDA command completed before the approval delay")
	}
	if result.Status != TurnStatusCompleted {
		t.Fatalf("CUDA turn result = %#v", result)
	}
	output, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("read CUDA sentinel: %v", err)
	}
	if strings.TrimSpace(string(output)) != "cxp-cuda-ok:42" {
		t.Fatalf("CUDA output = %q", output)
	}
}

func runtimeContractShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
