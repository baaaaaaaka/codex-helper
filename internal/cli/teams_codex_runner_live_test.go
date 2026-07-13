package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

const (
	syntheticOriginModel       = "gpt-cxp-synthetic-sol"
	syntheticThirdPartyModel   = "nvidia/cxp-synthetic-nemotron"
	syntheticThirdUpstream     = "nvidia/cxp-synthetic-upstream"
	syntheticReturnModel       = "gpt-cxp-synthetic-luna"
	syntheticOriginVisible     = "SYNTHETIC_ORIGIN_VISIBLE"
	syntheticThirdPartyVisible = "SYNTHETIC_THIRD_PARTY_VISIBLE"
	syntheticPrivateReasoning  = "SYNTHETIC_THIRD_PARTY_PRIVATE_REASONING"
	syntheticContextComplete   = "SYNTHETIC_CONTEXT_COMPLETE"
)

// TestInstalledCodexTeamsExecutorSyntheticModelSwitchPreservesContext is an
// actual-binary CI contract with no external account credentials. It launches
// the installed official Codex app-server, but routes every request to local
// synthetic official and NVIDIA-like Responses services. This catches the
// production boundary that unit tests previously missed: Teams session model
// and effort state must reach turn/start while a cross-provider resume keeps
// the exact thread and all portable visible history.
func TestInstalledCodexTeamsExecutorSyntheticModelSwitchPreservesContext(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_SYNTHETIC_TEAMS_MODEL_SWITCH")) != "1" {
		t.Skip("set CODEX_HELPER_SYNTHETIC_TEAMS_MODEL_SWITCH=1 to run the installed-Codex synthetic model-switch contract")
	}
	codexPath := strings.TrimSpace(os.Getenv("CXP_CONTRACT_CODEX"))
	var err error
	if codexPath == "" {
		codexPath, err = exec.LookPath("codex")
	}
	if err != nil || strings.TrimSpace(codexPath) == "" {
		t.Fatalf("installed Codex binary not found: %v", err)
	}

	marker := fmt.Sprintf("SYNTHETIC_CONTEXT_MARKER_%x", uint64(time.Now().UnixNano()))
	officialAdapter := &syntheticOfficialSwitchAdapter{marker: marker}
	thirdAdapter := &syntheticThirdPartySwitchAdapter{marker: marker}
	officialFacade := &responsesadapter.Facade{
		Adapter:      officialAdapter,
		Store:        responsesadapter.NewMemoryStore(),
		DefaultModel: syntheticOriginModel,
		Models: []responsesadapter.ModelInfo{
			{ID: syntheticOriginModel, OwnedBy: "cxp-synthetic"},
			{ID: syntheticReturnModel, OwnedBy: "cxp-synthetic"},
		},
	}
	thirdFacade := &responsesadapter.Facade{
		Adapter:      thirdAdapter,
		Store:        responsesadapter.NewMemoryStore(),
		DefaultModel: syntheticThirdUpstream,
		Models:       []responsesadapter.ModelInfo{{ID: syntheticThirdUpstream, OwnedBy: "cxp-synthetic-nvidia"}},
	}

	officialProvider := modelprofile.ProviderSpec{
		ID: "synthetic-official", DisplayName: "Synthetic official", DefaultModel: syntheticOriginModel,
		SupportsReason: true, DefaultReasoningEffort: "medium", SupportedReasoningEfforts: []string{"low", "medium", "high", "xhigh"},
		Models: []modelprofile.ModelSpec{
			{ID: syntheticOriginModel, DisplayName: "Synthetic Sol", SupportsReason: true, Priority: 20},
			{ID: syntheticReturnModel, DisplayName: "Synthetic Luna", SupportsReason: true, Priority: 10},
		},
	}
	officialCatalog, err := modelprofile.ThirdPartyCodexModelCatalogJSON([]modelprofile.ProviderSpec{officialProvider})
	if err != nil {
		t.Fatal(err)
	}
	officialServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(officialCatalog)
		case "/responses":
			officialFacade.ServeHTTP(w, r)
		default:
			// Codex analytics and cloud-config probes are kept local as part of
			// the no-external-credentials contract.
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer officialServer.Close()
	thirdServer := httptest.NewServer(thirdFacade)
	defer thirdServer.Close()

	thirdModel := modelprofile.ModelSpec{
		ID: syntheticThirdPartyModel, UpstreamID: syntheticThirdUpstream,
		DisplayName: "Synthetic NVIDIA Nemotron", SupportsReason: true, Priority: 5,
	}
	thirdProvider := modelprofile.ProviderSpec{
		ID: "synthetic-nvidia", DisplayName: "Synthetic NVIDIA", DefaultModel: syntheticThirdPartyModel,
		Models: []modelprofile.ModelSpec{thirdModel}, BaseURL: thirdServer.URL + "/v1",
		UsesAdapter: true, DirectResponses: true, SupportsReason: true,
		DefaultReasoningEffort: "high", SupportedReasoningEfforts: []string{"low", "medium", "high", "xhigh"},
	}
	mergedCatalog, err := modelprofile.MergeCodexModelCatalogJSON(officialCatalog, []modelprofile.ProviderSpec{thirdProvider})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	catalogPath := filepath.Join(root, "synthetic-model-catalog.json")
	if err := os.WriteFile(catalogPath, mergedCatalog, 0o600); err != nil {
		t.Fatal(err)
	}
	resolvedThird := modelprofile.Resolved{
		Name: "synthetic-nvidia",
		Profile: config.ModelProfile{
			Provider: thirdProvider.ID, Model: syntheticThirdPartyModel,
			BaseURL: thirdProvider.BaseURL, APIKeyRef: "env:SYNTHETIC_NVIDIA_KEY", Revision: 1,
		},
		Provider: thirdProvider,
		Model:    thirdModel,
	}
	gateway, gatewayCleanup, err := newUnifiedModelGateway(unifiedModelGatewayOptions{
		OfficialBaseURL: officialServer.URL,
		LocalKey:        "synthetic-local-gateway-key",
		CatalogPath:     catalogPath,
		Providers:       []modelprofile.Resolved{resolvedThird},
		APIKeys:         map[string]string{resolvedThird.Name: "synthetic-nvidia-key"},
		InstanceID:      "synthetic-model-switch",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer gatewayCleanup()
	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()

	codexHome := filepath.Join(root, "codex-home")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	writeSyntheticTeamsCodexAuth(t, codexHome)
	workDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		t.Fatal(err)
	}
	launch := codexModelProfileLaunch{
		Enabled: true, Unified: true, Name: "synthetic-unified",
		ProviderID: cxpUnifiedCodexModelProviderID,
		BaseURL:    gatewayServer.URL + "/v1", ProxyKey: "synthetic-local-gateway-key",
		CatalogPath: catalogPath, EnvKey: envCXPUnifiedGatewayKey,
	}
	profileArgs := appendCodexModelProfileArgs([]string{"codex"}, launch)
	appServerArgs := append([]string{}, profileArgs[1:]...)
	appServerArgs = append(appServerArgs,
		"-c", `chatgpt_base_url="`+tomlEscapeString(officialServer.URL)+`"`,
		"-c", `features.plugins=false`,
		"-c", `web_search="disabled"`,
	)
	newRunner := func() *codexrunner.AppServerRunner {
		return &codexrunner.AppServerRunner{
			Starter:              codexrunner.AppServerProcessStarter{},
			ApprovalMode:         codexrunner.ApprovalModeAutomatic,
			Command:              codexPath,
			AppServerArgs:        append([]string{}, appServerArgs...),
			ExtraEnv:             []string{"CODEX_HOME=" + codexHome, envCXPUnifiedGatewayKey + "=synthetic-local-gateway-key"},
			WorkingDir:           workDir,
			Timeout:              60 * time.Second,
			MetadataOnlyResume:   true,
			RequireCompleteFinal: true,
		}
	}
	originRunner := newRunner()
	thirdRunner := newRunner()
	returnRunner := newRunner()
	originSnapshot := syntheticModelSnapshot(syntheticOriginModel, modelprofile.DefaultProvider, "medium")
	thirdSnapshot := syntheticModelSnapshot(syntheticThirdPartyModel, thirdProvider.ID, "high")
	returnSnapshot := syntheticModelSnapshot(syntheticReturnModel, modelprofile.DefaultProvider, "xhigh")
	session := &teams.Session{
		ID: "synthetic-teams-model-switch", Cwd: workDir,
		ModelProfile: originSnapshot, ReasoningEffort: "medium",
	}
	executor := teamsCodexExecutor{
		runner: originRunner, workDir: workDir, timeout: 60 * time.Second,
		modelProfileSnapshot: originSnapshot,
		runnerCacheMu:        &sync.Mutex{},
		runnersByProfile: map[string]codexrunner.Runner{
			modelProfileRunnerSessionCacheKey(&teams.Session{ID: session.ID, ModelProfile: thirdSnapshot, ModelGeneration: 1}):  thirdRunner,
			modelProfileRunnerSessionCacheKey(&teams.Session{ID: session.ID, ModelProfile: returnSnapshot, ModelGeneration: 2}): returnRunner,
		},
		runnerKeyBySession: map[string]string{},
	}
	defer func() {
		if err := executor.Close(); err != nil {
			t.Errorf("close synthetic Teams executor: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	first, err := executor.Run(ctx, session, "Remember this marker across providers: "+marker)
	if err != nil {
		t.Fatalf("synthetic official origin: %v", err)
	}
	if first.Text != syntheticOriginVisible || strings.TrimSpace(first.CodexThreadID) == "" {
		t.Fatalf("synthetic origin result = %#v", first)
	}
	session.CodexThreadID = first.CodexThreadID
	session.ModelProfile = thirdSnapshot
	session.ModelGeneration = 1
	session.ReasoningEffort = "high"
	second, err := executor.Run(ctx, session, "Continue on the synthetic NVIDIA route and retain prior visible context.")
	if err != nil {
		t.Fatalf("synthetic third-party turn: %v", err)
	}
	if second.Text != syntheticThirdPartyVisible || second.CodexThreadID != first.CodexThreadID {
		t.Fatalf("synthetic third-party result = %#v, origin thread=%q", second, first.CodexThreadID)
	}
	session.ModelProfile = returnSnapshot
	session.ModelGeneration = 2
	session.ReasoningEffort = "xhigh"
	third, err := executor.Run(ctx, session, "Return to the synthetic official route and verify complete portable context.")
	if err != nil {
		t.Fatalf("synthetic official return: %v", err)
	}
	if third.Text != syntheticContextComplete || third.CodexThreadID != first.CodexThreadID {
		t.Fatalf("synthetic official return result = %#v, origin thread=%q", third, first.CodexThreadID)
	}
	if err := officialAdapter.verify(); err != nil {
		t.Fatal(err)
	}
	if err := thirdAdapter.verify(); err != nil {
		t.Fatal(err)
	}

	contexts, transcriptPath, err := waitForSyntheticTurnContexts(codexHome, first.CodexThreadID, 3, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	wantContexts := []liveTurnContext{
		{Model: syntheticOriginModel, Effort: "medium"},
		{Model: syntheticThirdPartyModel, Effort: "high"},
		{Model: syntheticReturnModel, Effort: "xhigh"},
	}
	contexts = contexts[len(contexts)-len(wantContexts):]
	for index := range wantContexts {
		if contexts[index] != wantContexts[index] {
			t.Fatalf("effective turn context %d = %#v, want %#v; transcript=%s", index, contexts[index], wantContexts[index], transcriptPath)
		}
	}
}

func syntheticModelSnapshot(model string, provider string, defaultEffort string) modelprofile.Snapshot {
	efforts, _ := json.Marshal([]string{"low", "medium", "high", "xhigh"})
	return modelprofile.Snapshot{
		Name: model, Provider: provider, Model: model, DefaultModel: model,
		DefaultReasoningEffort: defaultEffort, SupportedReasoningEffortsJSON: string(efforts), Revision: 1,
	}
}

type syntheticOfficialSwitchAdapter struct {
	mu     sync.Mutex
	marker string
	calls  int
}

func (a *syntheticOfficialSwitchAdapter) Stream(_ context.Context, request responsesadapter.ProviderRequest) (<-chan responsesadapter.ProviderEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	visible := syntheticProviderRequestText(request)
	switch a.calls {
	case 1:
		if request.Model != syntheticOriginModel || request.ReasoningEffort != "medium" || !strings.Contains(visible, a.marker) {
			return nil, fmt.Errorf("synthetic official origin request model=%q effort=%q context=%q", request.Model, request.ReasoningEffort, visible)
		}
		return syntheticProviderEvents(syntheticOriginVisible, ""), nil
	case 2:
		if request.Model != syntheticReturnModel || request.ReasoningEffort != "xhigh" {
			return nil, fmt.Errorf("synthetic official return model=%q effort=%q", request.Model, request.ReasoningEffort)
		}
		for _, required := range []string{a.marker, syntheticOriginVisible, syntheticThirdPartyVisible} {
			if !strings.Contains(visible, required) {
				return nil, fmt.Errorf("synthetic official return lost portable context %q: %s", required, visible)
			}
		}
		if strings.Contains(visible, syntheticPrivateReasoning) {
			return nil, fmt.Errorf("synthetic official return leaked third-party reasoning: %s", visible)
		}
		return syntheticProviderEvents(syntheticContextComplete, ""), nil
	default:
		return nil, fmt.Errorf("unexpected synthetic official call %d", a.calls)
	}
}

func (a *syntheticOfficialSwitchAdapter) verify() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.calls != 2 {
		return fmt.Errorf("synthetic official calls=%d, want 2", a.calls)
	}
	return nil
}

type syntheticThirdPartySwitchAdapter struct {
	mu     sync.Mutex
	marker string
	calls  int
}

func (a *syntheticThirdPartySwitchAdapter) Stream(_ context.Context, request responsesadapter.ProviderRequest) (<-chan responsesadapter.ProviderEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	visible := syntheticProviderRequestText(request)
	if a.calls != 1 || request.Model != syntheticThirdUpstream || request.ReasoningEffort != "high" {
		return nil, fmt.Errorf("synthetic third-party call=%d model=%q effort=%q", a.calls, request.Model, request.ReasoningEffort)
	}
	for _, required := range []string{a.marker, syntheticOriginVisible} {
		if !strings.Contains(visible, required) {
			return nil, fmt.Errorf("synthetic third-party route lost context %q: %s", required, visible)
		}
	}
	return syntheticProviderEvents(syntheticThirdPartyVisible, syntheticPrivateReasoning), nil
}

func (a *syntheticThirdPartySwitchAdapter) verify() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.calls != 1 {
		return fmt.Errorf("synthetic third-party calls=%d, want 1", a.calls)
	}
	return nil
}

func syntheticProviderRequestText(request responsesadapter.ProviderRequest) string {
	var out strings.Builder
	out.WriteString(request.InputText)
	for _, messages := range [][]responsesadapter.ProviderMessage{request.InputMessages, request.Messages} {
		for _, message := range messages {
			out.WriteByte('\n')
			out.WriteString(message.Role)
			out.WriteByte(':')
			out.WriteString(message.Content)
			out.WriteString(message.ReasoningContent)
			for _, part := range message.ContentParts {
				out.WriteString(part.Text)
			}
		}
	}
	return out.String()
}

func syntheticProviderEvents(text string, reasoning string) <-chan responsesadapter.ProviderEvent {
	count := 2
	if reasoning != "" {
		count++
	}
	events := make(chan responsesadapter.ProviderEvent, count)
	if reasoning != "" {
		events <- responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventReasoningDelta, Delta: reasoning}
	}
	events <- responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventTextDelta, Delta: text}
	events <- responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventDone}
	close(events)
	return events
}

func writeSyntheticTeamsCodexAuth(t *testing.T, codexHome string) {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "none", "typ": "JWT"})
	payload, _ := json.Marshal(map[string]any{
		"email": "synthetic-contract@example.test",
		"https://api.openai.com/auth": map[string]string{
			"chatgpt_user_id": "synthetic-user", "chatgpt_account_id": "synthetic-account",
		},
	})
	jwt := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload) + ".c3ludGhldGlj"
	auth, err := json.Marshal(map[string]any{
		"auth_mode": "chatgpt", "OPENAI_API_KEY": nil,
		"tokens": map[string]any{
			"id_token": jwt, "access_token": "synthetic-contract-access", "refresh_token": "synthetic-contract-refresh", "account_id": "synthetic-account",
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

type liveTurnContext struct {
	Model  string
	Effort string
}

func waitForSyntheticTurnContexts(codexHome string, threadID string, minimum int, timeout time.Duration) ([]liveTurnContext, string, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		contexts, path, err := syntheticTurnContexts(codexHome, threadID)
		if err == nil && len(contexts) >= minimum {
			return contexts, path, nil
		}
		if err != nil {
			lastErr = err
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return nil, "", lastErr
			}
			return nil, "", fmt.Errorf("thread %s did not persist %d turn_context records within %s", threadID, minimum, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func syntheticTurnContexts(codexHome string, threadID string) ([]liveTurnContext, string, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return nil, "", fmt.Errorf("thread id is required")
	}
	var transcriptPath string
	for _, root := range []string{filepath.Join(codexHome, "sessions"), filepath.Join(codexHome, "archived_sessions")} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if !entry.IsDir() && strings.Contains(entry.Name(), threadID) && strings.HasSuffix(entry.Name(), ".jsonl") {
				transcriptPath = path
				return fs.SkipAll
			}
			return nil
		})
		if err != nil {
			return nil, "", err
		}
		if transcriptPath != "" {
			break
		}
	}
	if transcriptPath == "" {
		return nil, "", fmt.Errorf("Codex transcript for thread %s was not found under %s", threadID, codexHome)
	}
	file, err := os.Open(transcriptPath)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	contexts := make([]liveTurnContext, 0, 3)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var record struct {
			Type    string `json:"type"`
			Payload struct {
				Model  string `json:"model"`
				Effort string `json:"effort"`
			} `json:"payload"`
		}
		if json.Unmarshal(scanner.Bytes(), &record) == nil && record.Type == "turn_context" {
			if model := strings.TrimSpace(record.Payload.Model); model != "" {
				contexts = append(contexts, liveTurnContext{Model: model, Effort: strings.TrimSpace(record.Payload.Effort)})
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, "", err
	}
	return contexts, transcriptPath, nil
}
