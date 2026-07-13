package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

func TestLiveNativeOfficialModelSwitchKeepsSameThreadAndContext(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_UNIFIED_GATEWAY")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_UNIFIED_GATEWAY=1 to run real Codex model-switch coverage")
	}
	models := splitLiveModelSequence(os.Getenv("CODEX_HELPER_LIVE_SWITCH_MODELS"))
	if len(models) < 2 {
		t.Skip("set CODEX_HELPER_LIVE_SWITCH_MODELS to at least two comma-separated official model slugs")
	}
	for _, model := range models {
		if strings.HasPrefix(strings.ToLower(model), "nvidia/") {
			t.Fatalf("native official model-switch test cannot route third-party model %q", model)
		}
	}
	codexPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_PATH"))
	codexHome := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_HOME"))
	if codexPath == "" || codexHome == "" {
		t.Fatal("native official model-switch test requires Codex path and Codex home")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	runner := &codexrunner.AppServerRunner{
		Starter:              codexrunner.AppServerProcessStarter{},
		ApprovalMode:         codexrunner.ApprovalModeAutomatic,
		Command:              codexPath,
		AppServerArgs:        []string{"--analytics-default-enabled"},
		ExtraEnv:             codexHomeEnv(codexHome),
		WorkingDir:           t.TempDir(),
		Timeout:              4 * time.Minute,
		MetadataOnlyResume:   true,
		RequireCompleteFinal: true,
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("close native app-server runner: %v", err)
		}
	}()
	listed, err := runner.ListModels(ctx)
	if err != nil {
		t.Fatalf("read native official model catalog: %v", err)
	}
	available := make(map[string]bool, len(listed))
	for _, model := range listed {
		available[strings.ToLower(firstNonEmptyCLI(model.Model, model.ID))] = true
	}
	for _, model := range models {
		if !available[strings.ToLower(model)] {
			t.Fatalf("native official model/list does not contain %q", model)
		}
	}
	runLiveModelSwitchSequence(t, ctx, runner, models, nil)
}

func TestLiveUnifiedModelSwitchKeepsSameThreadAndContext(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_UNIFIED_GATEWAY")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_UNIFIED_GATEWAY=1 to run real Codex unified gateway coverage")
	}
	models := splitLiveModelSequence(os.Getenv("CODEX_HELPER_LIVE_SWITCH_MODELS"))
	if len(models) < 2 {
		t.Skip("set CODEX_HELPER_LIVE_SWITCH_MODELS to at least two comma-separated model slugs")
	}
	codexPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_PATH"))
	configPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CONFIG_PATH"))
	templateProfileName := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_MODEL_PROFILE"))
	codexHome := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_HOME"))
	if codexPath == "" || configPath == "" || templateProfileName == "" || codexHome == "" {
		t.Fatal("live model switch test requires Codex path, config path, a template third-party profile, and Codex home")
	}
	if strings.TrimSpace(os.Getenv("NVIDIA_API_KEY")) == "" {
		t.Fatal("live model switch test requires NVIDIA_API_KEY in the process environment")
	}

	sourceStore, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := sourceStore.Load()
	if err != nil {
		t.Fatal(err)
	}
	template, ok := cfg.ModelProfiles[templateProfileName]
	if !ok {
		t.Fatalf("template model profile %q not found", templateProfileName)
	}
	profiles := make(map[string]config.ModelProfile, len(cfg.ModelProfiles)+len(models))
	for name, profile := range cfg.ModelProfiles {
		profiles[name] = profile
	}
	cfg.ModelProfiles = profiles
	for index, model := range models {
		if !strings.HasPrefix(strings.ToLower(model), "nvidia/") || configHasModelProfile(cfg, model) {
			continue
		}
		profile := template
		profile.Model = model
		profile.Revision = 1
		profile.Source = ""
		profile.VerifiedAt = time.Time{}
		profile.VerificationFingerprint = ""
		profile.VerificationError = ""
		cfg.ModelProfiles[fmt.Sprintf("live-switch-%02d", index)] = profile
	}
	testStore, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := testStore.Save(cfg); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	var gatewayLog bytes.Buffer
	sharedWorkingDir := t.TempDir()
	runner := &liveModelRoutingRunner{}
	for _, model := range models {
		key := strings.ToLower(strings.TrimSpace(model))
		profileName := config.DefaultModelProfileName
		if strings.HasPrefix(key, "nvidia/") {
			profileName = liveModelProfileName(cfg, model)
			if profileName == "" {
				t.Fatalf("no live model profile resolves third-party model %q", model)
			}
		}
		launch, cleanup, err := startModelProfileAdapterForCodex(
			withCodexLoginProbePath(ctx, codexPath),
			testStore,
			profileName,
			modelprofile.Snapshot{},
			"",
			true,
			&gatewayLog,
		)
		if err != nil {
			t.Fatalf("start unified gateway for model %q: %v\n%s", model, err, gatewayLog.String())
		}
		if !launch.Unified {
			cleanup()
			t.Fatalf("launch for model %q is not unified: %#v", model, launch)
		}
		catalog, err := os.ReadFile(launch.CatalogPath)
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
		for _, catalogModel := range models {
			if !bytes.Contains(catalog, []byte(`"slug": "`+catalogModel+`"`)) {
				cleanup()
				t.Fatalf("unified model catalog for %q does not contain %q", model, catalogModel)
			}
		}
		profileArgs := appendCodexModelProfileArgs([]string{"codex"}, launch)
		modelRunner := &codexrunner.AppServerRunner{
			Starter:              codexrunner.AppServerProcessStarter{},
			ApprovalMode:         codexrunner.ApprovalModeAutomatic,
			Command:              codexPath,
			AppServerArgs:        append([]string{"--analytics-default-enabled"}, profileArgs[1:]...),
			ExtraEnv:             append(codexHomeEnv(codexHome), launch.effectiveEnvKey()+"="+launch.ProxyKey),
			WorkingDir:           sharedWorkingDir,
			Timeout:              4 * time.Minute,
			MetadataOnlyResume:   true,
			RequireCompleteFinal: true,
			CloseHook:            cleanup,
		}
		runner.routes = append(runner.routes, liveModelRoute{model: key, runner: modelRunner})
		if runner.fallback == nil {
			runner.fallback = modelRunner
		}
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("close profile-routed app-server runners: %v", err)
		}
	}()

	runLiveModelSwitchSequence(t, ctx, runner, models, func() string { return gatewayLog.String() })
}

type liveModelRoutingRunner struct {
	mu       sync.Mutex
	fallback codexrunner.Runner
	routes   []liveModelRoute
	current  int
}

type liveModelRoute struct {
	model  string
	runner codexrunner.Runner
}

func (r *liveModelRoutingRunner) runner(model string) codexrunner.Runner {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	model = strings.ToLower(strings.TrimSpace(model))
	if r.current < len(r.routes) && r.routes[r.current].model == model {
		return r.routes[r.current].runner
	}
	if next := r.current + 1; next < len(r.routes) && r.routes[next].model == model {
		r.current = next
		return r.routes[r.current].runner
	}
	return r.fallback
}

func (r *liveModelRoutingRunner) StartThread(ctx context.Context, input codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	return r.runner(input.Model).StartThread(ctx, input)
}

func (r *liveModelRoutingRunner) ResumeThread(ctx context.Context, threadID string, input codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	return r.runner(input.Model).ResumeThread(ctx, threadID, input)
}

func (r *liveModelRoutingRunner) StartTurn(ctx context.Context, input codexrunner.StartTurnInput) (codexrunner.TurnResult, error) {
	return r.runner(input.Model).StartTurn(ctx, input)
}

func (r *liveModelRoutingRunner) InterruptTurn(ctx context.Context, ref codexrunner.TurnRef) error {
	return r.fallback.InterruptTurn(ctx, ref)
}

func (r *liveModelRoutingRunner) ReadThread(ctx context.Context, threadID string) (codexrunner.Thread, error) {
	return r.fallback.ReadThread(ctx, threadID)
}

func (r *liveModelRoutingRunner) ListThreads(ctx context.Context, opts codexrunner.ListThreadsOptions) ([]codexrunner.Thread, error) {
	return r.fallback.ListThreads(ctx, opts)
}

func (r *liveModelRoutingRunner) Close() error {
	seen := make(map[codexrunner.Runner]bool)
	var firstErr error
	for _, route := range r.routes {
		runner := route.runner
		if runner == nil || seen[runner] {
			continue
		}
		seen[runner] = true
		if closer, ok := runner.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func runLiveModelSwitchSequence(t *testing.T, ctx context.Context, runner codexrunner.Runner, models []string, diagnostic func() string) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CONTEXT_COMPLETENESS")) == "1" {
		runLiveContextCompletenessSequence(t, ctx, runner, models, diagnostic)
		return
	}
	diagnosticText := func() string {
		if diagnostic == nil {
			return ""
		}
		text := strings.TrimSpace(diagnostic())
		if text == "" {
			return ""
		}
		return "\n" + text
	}
	values := make([]string, len(models))
	var memory strings.Builder
	for index := range values {
		values[index] = fmt.Sprintf("SWITCH_CONTEXT_VALUE_%02d", index+1)
		_, _ = fmt.Fprintf(&memory, "key_%02d=%s\n", index+1, values[index])
	}
	stablePrefix := strings.Repeat(
		"This is stable context padding used to test model-switch prompt-prefix reuse. Keep it unchanged.\n",
		160,
	)
	firstPrompt := "Memorize every key/value pair below for later turns. Do not repeat any value yet. Reply exactly READY.\n\n" + memory.String() + "\n" + stablePrefix
	var sawToolHistory atomic.Bool
	var eventHandler codexrunner.EventHandler
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_SWITCH_TOOL_HISTORY")) == "1" {
		const sentinel = "MODEL_SWITCH_TOOL_HISTORY_SENTINEL"
		firstPrompt = "Before memorizing the values, use the shell tool to run exactly `printf MODEL_SWITCH_TOOL_HISTORY_SENTINEL`. " +
			"Wait for it to complete, then continue; do not include its output in your final reply.\n\n" + firstPrompt
		eventHandler = func(event codexrunner.StreamEvent) {
			if event.Kind == codexrunner.StreamEventCommandCompleted && strings.Contains(event.AggregatedOutput, sentinel) {
				sawToolHistory.Store(true)
			}
		}
	}
	result, err := runner.StartThread(ctx, codexrunner.TurnInput{
		Prompt:       firstPrompt,
		Model:        models[0],
		EventHandler: eventHandler,
	})
	if err != nil {
		t.Fatalf("initial model %q turn failed: %v%s", models[0], err, diagnosticText())
	}
	threadID := strings.TrimSpace(result.ThreadID)
	if threadID == "" {
		t.Fatal("initial turn returned an empty thread id")
	}
	if !strings.Contains(strings.ToUpper(result.FinalAgentMessage), "READY") {
		t.Fatalf("initial model %q did not acknowledge memory setup: %q", models[0], result.FinalAgentMessage)
	}
	if eventHandler != nil && !sawToolHistory.Load() {
		t.Fatalf("initial model %q did not produce the required completed shell-tool history", models[0])
	}
	logLiveModelSwitchUsage(t, 0, models[0], result)

	for index := 1; index < len(models); index++ {
		prompt := fmt.Sprintf("From the key/value map in the first turn, reply with only the value of key_%02d. Do not use tools and do not add punctuation or explanation.", index+1)
		result, err = runner.ResumeThread(ctx, threadID, codexrunner.TurnInput{
			Prompt: prompt,
			Model:  models[index],
		})
		if err != nil {
			t.Errorf("switch %d to model %q failed: %v%s", index, models[index], err, diagnosticText())
			continue
		}
		if result.ThreadID != threadID {
			t.Errorf("switch %d to model %q changed thread id from %q to %q", index, models[index], threadID, result.ThreadID)
		}
		if !strings.Contains(result.FinalAgentMessage, values[index]) {
			t.Errorf("switch %d to model %q lost first-turn context: got %q, want value %q", index, models[index], result.FinalAgentMessage, values[index])
		}
		logLiveModelSwitchUsage(t, index, models[index], result)
	}
}

type liveContextInventory struct {
	Phase1                  map[string]string `json:"phase_1"`
	Phase2                  map[string]string `json:"phase_2"`
	ToolOutput              string            `json:"tool_output"`
	PriorAssistantResponses []string          `json:"prior_assistant_responses"`
}

func runLiveContextCompletenessSequence(t *testing.T, ctx context.Context, runner codexrunner.Runner, models []string, diagnostic func() string) {
	t.Helper()
	if len(models) < 2 {
		t.Fatalf("context-completeness coverage requires an origin and at least one target model, got %d", len(models))
	}
	diagnosticText := func() string {
		if diagnostic == nil {
			return ""
		}
		text := strings.TrimSpace(diagnostic())
		if text == "" {
			return ""
		}
		return "\n" + text
	}

	seed := time.Now().UnixNano()
	phase1, phase1Text := liveContextLedger("phase_1", 24, seed)
	phase2, phase2Text := liveContextLedger("phase_2", 24, seed+1000)
	toolOutput := fmt.Sprintf("TOOL_ONLY_CONTEXT_%x", uint64(seed))
	toolContextPath := filepath.Join(t.TempDir(), "tool-context.txt")
	if err := os.WriteFile(toolContextPath, []byte(toolOutput), 0o600); err != nil {
		t.Fatal(err)
	}

	var sawExpectedToolOutput atomic.Bool
	phase1AssistantItems := make([]string, 0, 2)
	phase1Result, err := runner.StartThread(ctx, codexrunner.TurnInput{
		Model: models[0],
		Prompt: "Retain every phase_1 entry for a later model. Before replying, use the shell tool to run exactly `cat " + toolContextPath +
			"`. Do not repeat the command output or any ledger value in your reply. Choose a fresh nonce beginning with PHASE1_ASSISTANT_ and reply with only that nonce.\n\n" + phase1Text,
		EventHandler: func(event codexrunner.StreamEvent) {
			appendLiveAssistantItem(&phase1AssistantItems, event)
			if event.Kind == codexrunner.StreamEventCommandCompleted && strings.Contains(event.AggregatedOutput, toolOutput) {
				sawExpectedToolOutput.Store(true)
			}
		},
	})
	if err != nil {
		t.Fatalf("context seed turn on model %q failed: %v%s", models[0], err, diagnosticText())
	}
	threadID := strings.TrimSpace(phase1Result.ThreadID)
	if threadID == "" {
		t.Fatal("context seed turn returned an empty thread id")
	}
	phase1Assistant := strings.TrimSpace(phase1Result.FinalAgentMessage)
	if phase1Assistant == "" || !strings.HasPrefix(phase1Assistant, "PHASE1_ASSISTANT_") {
		t.Fatalf("phase 1 assistant response = %q, want a dynamic PHASE1_ASSISTANT_ nonce", phase1Assistant)
	}
	if strings.Contains(phase1Assistant, toolOutput) {
		t.Fatal("phase 1 assistant response leaked the tool-only context value")
	}
	if !sawExpectedToolOutput.Load() {
		t.Fatal("phase 1 did not produce the required completed shell-tool output")
	}
	phase1AssistantItems = completeLiveAssistantItems(phase1AssistantItems, phase1Assistant)

	var phase2UsedTool atomic.Bool
	phase2AssistantItems := make([]string, 0, 1)
	phase2Result, err := runner.ResumeThread(ctx, threadID, codexrunner.TurnInput{
		Model:  models[0],
		Prompt: "Retain every phase_2 entry alongside phase_1. Do not use tools and do not repeat any ledger value. Choose a fresh nonce beginning with PHASE2_ASSISTANT_ and reply with only that nonce.\n\n" + phase2Text,
		EventHandler: func(event codexrunner.StreamEvent) {
			appendLiveAssistantItem(&phase2AssistantItems, event)
			if event.Kind == codexrunner.StreamEventCommandStarted || event.Kind == codexrunner.StreamEventCommandCompleted {
				phase2UsedTool.Store(true)
			}
		},
	})
	if err != nil {
		t.Fatalf("second context seed turn on model %q failed: %v%s", models[0], err, diagnosticText())
	}
	if phase2Result.ThreadID != threadID {
		t.Fatalf("second context seed turn changed thread id from %q to %q", threadID, phase2Result.ThreadID)
	}
	if phase2UsedTool.Load() {
		t.Fatal("phase 2 unexpectedly used a tool")
	}
	phase2Assistant := strings.TrimSpace(phase2Result.FinalAgentMessage)
	if phase2Assistant == "" || !strings.HasPrefix(phase2Assistant, "PHASE2_ASSISTANT_") {
		t.Fatalf("phase 2 assistant response = %q, want a dynamic PHASE2_ASSISTANT_ nonce", phase2Assistant)
	}
	if strings.Contains(phase2Assistant, toolOutput) {
		t.Fatal("phase 2 assistant response leaked the tool-only context value")
	}
	phase2AssistantItems = completeLiveAssistantItems(phase2AssistantItems, phase2Assistant)
	priorAssistantResponses := append(append([]string(nil), phase1AssistantItems...), phase2AssistantItems...)

	for index := 1; index < len(models)-1; index++ {
		key := fmt.Sprintf("phase_1_key_%02d", index)
		want := phase1[key]
		var intermediateUsedTool atomic.Bool
		intermediateAssistantItems := make([]string, 0, 1)
		intermediateResult, err := runner.ResumeThread(ctx, threadID, codexrunner.TurnInput{
			Model: models[index],
			Prompt: fmt.Sprintf(
				"Without using tools, reply with only the exact value of %s from the earlier phase_1 ledger. Do not add punctuation or explanation.",
				key,
			),
			EventHandler: func(event codexrunner.StreamEvent) {
				appendLiveAssistantItem(&intermediateAssistantItems, event)
				if event.Kind == codexrunner.StreamEventCommandStarted || event.Kind == codexrunner.StreamEventCommandCompleted {
					intermediateUsedTool.Store(true)
				}
			},
		})
		if err != nil {
			t.Fatalf("intermediate switch %q -> %q failed: %v%s", models[index-1], models[index], err, diagnosticText())
		}
		if intermediateResult.ThreadID != threadID {
			t.Fatalf("intermediate switch to %q changed thread id from %q to %q", models[index], threadID, intermediateResult.ThreadID)
		}
		if intermediateUsedTool.Load() {
			t.Fatalf("intermediate model %q used a tool instead of retained context", models[index])
		}
		intermediateReply := strings.TrimSpace(intermediateResult.FinalAgentMessage)
		if intermediateReply != want {
			t.Fatalf("intermediate model %q returned %q for %s, want %q", models[index], intermediateReply, key, want)
		}
		intermediateAssistantItems = completeLiveAssistantItems(intermediateAssistantItems, intermediateReply)
		priorAssistantResponses = append(priorAssistantResponses, intermediateAssistantItems...)
	}

	var targetUsedTool atomic.Bool
	targetResult, err := runner.ResumeThread(ctx, threadID, codexrunner.TurnInput{
		Model: models[len(models)-1],
		Prompt: fmt.Sprintf("Without using tools, reconstruct all visible context that existed before this request. Reply with one JSON object containing exactly these fields: "+
			"phase_1 (all 24 key/value pairs), phase_2 (all 24 key/value pairs), tool_output (the exact output produced by the earlier shell command), "+
			"and prior_assistant_responses (all %d exact prior assistant replies in chronological order, including the two nonce replies and every intermediate model reply). "+
			"Do not omit a reply merely because it duplicates a ledger value. Do not omit, summarize, normalize, or invent values.", len(priorAssistantResponses)),
		EventHandler: func(event codexrunner.StreamEvent) {
			if event.Kind == codexrunner.StreamEventCommandStarted || event.Kind == codexrunner.StreamEventCommandCompleted {
				targetUsedTool.Store(true)
			}
		},
	})
	if err != nil {
		t.Fatalf("context reconstruction after switching %q -> %q failed: %v%s", models[len(models)-2], models[len(models)-1], err, diagnosticText())
	}
	if targetResult.ThreadID != threadID {
		t.Fatalf("context reconstruction changed thread id from %q to %q", threadID, targetResult.ThreadID)
	}
	if targetUsedTool.Load() {
		t.Fatalf("target model %q used a tool instead of relying on retained context", models[len(models)-1])
	}

	got, err := decodeLiveContextInventory(targetResult.FinalAgentMessage)
	if err != nil {
		t.Fatalf("decode context reconstruction from model %q: %v; response=%q", models[len(models)-1], err, targetResult.FinalAgentMessage)
	}
	assertLiveContextLedgerEqual(t, "phase_1", got.Phase1, phase1)
	assertLiveContextLedgerEqual(t, "phase_2", got.Phase2, phase2)
	if !strings.Contains(got.ToolOutput, toolOutput) {
		t.Errorf("tool-only context = %q, want it to contain %q", got.ToolOutput, toolOutput)
	}
	wantAssistant := priorAssistantResponses
	if len(got.PriorAssistantResponses) != len(wantAssistant) {
		t.Errorf("prior assistant response count = %d, want %d", len(got.PriorAssistantResponses), len(wantAssistant))
	} else {
		for index := range wantAssistant {
			if got.PriorAssistantResponses[index] != wantAssistant[index] {
				t.Errorf("prior assistant response %d = %q, want %q", index, got.PriorAssistantResponses[index], wantAssistant[index])
			}
		}
	}
	t.Logf("context_complete route=%q thread=%s user_values=%d assistant_responses=%d tool_outputs=1", strings.Join(models, " -> "), threadID, len(phase1)+len(phase2), len(wantAssistant))
}

func appendLiveAssistantItem(items *[]string, event codexrunner.StreamEvent) {
	if items == nil || event.Kind != codexrunner.StreamEventAgentMessage {
		return
	}
	if text := strings.TrimSpace(event.Text); text != "" {
		*items = append(*items, text)
	}
}

func completeLiveAssistantItems(items []string, final string) []string {
	final = strings.TrimSpace(final)
	if final == "" || (len(items) > 0 && items[len(items)-1] == final) {
		return items
	}
	return append(items, final)
}

func liveContextLedger(prefix string, count int, seed int64) (map[string]string, string) {
	values := make(map[string]string, count)
	var text strings.Builder
	for index := 1; index <= count; index++ {
		key := fmt.Sprintf("%s_key_%02d", prefix, index)
		value := fmt.Sprintf("%s_value_%02d_%x", prefix, index, uint64(seed)+uint64(index*7919))
		values[key] = value
		_, _ = fmt.Fprintf(&text, "%s=%s\n", key, value)
	}
	return values, text.String()
}

func decodeLiveContextInventory(raw string) (liveContextInventory, error) {
	var inventory liveContextInventory
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start < 0 || end < start {
		return inventory, fmt.Errorf("response does not contain a JSON object")
	}
	if err := json.Unmarshal([]byte(raw[start:end+1]), &inventory); err != nil {
		return inventory, err
	}
	return inventory, nil
}

func assertLiveContextLedgerEqual(t *testing.T, label string, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s entry count = %d, want %d", label, len(got), len(want))
	}
	for key, wantValue := range want {
		if gotValue, ok := got[key]; !ok {
			t.Errorf("%s is missing key %q", label, key)
		} else if gotValue != wantValue {
			t.Errorf("%s[%q] = %q, want %q", label, key, gotValue, wantValue)
		}
	}
}

func splitLiveModelSequence(raw string) []string {
	models := make([]string, 0)
	for _, value := range strings.Split(raw, ",") {
		model := strings.TrimSpace(value)
		if model == "" {
			continue
		}
		// Preserve deliberate repeats because they measure return-to-model and
		// same-model cache behavior.
		models = append(models, model)
	}
	return models
}

func configHasModelProfile(cfg config.Config, model string) bool {
	for _, profile := range cfg.ModelProfiles {
		if strings.EqualFold(strings.TrimSpace(profile.Model), strings.TrimSpace(model)) {
			return true
		}
	}
	return false
}

func liveModelProfileName(cfg config.Config, model string) string {
	for name, profile := range cfg.ModelProfiles {
		if strings.EqualFold(strings.TrimSpace(profile.Model), strings.TrimSpace(model)) {
			return name
		}
	}
	return ""
}

func logLiveModelSwitchUsage(t *testing.T, index int, model string, result codexrunner.TurnResult) {
	t.Helper()
	hitRate := 0.0
	if result.Usage.InputTokens > 0 {
		hitRate = 100 * float64(result.Usage.CachedInputTokens) / float64(result.Usage.InputTokens)
	}
	t.Logf(
		"switch_index=%d model=%q thread=%s input_tokens=%d cached_input_tokens=%d cache_hit_rate=%.1f%% output_tokens=%d",
		index,
		model,
		result.ThreadID,
		result.Usage.InputTokens,
		result.Usage.CachedInputTokens,
		hitRate,
		result.Usage.OutputTokens,
	)
}

func TestLiveUnifiedModelGatewayWithRealCodexOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_UNIFIED_GATEWAY")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_UNIFIED_GATEWAY=1 to run real Codex unified gateway coverage")
	}
	codexPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_PATH"))
	configPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CONFIG_PATH"))
	profileName := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_MODEL_PROFILE"))
	thirdModel := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_THIRD_MODEL"))
	officialModel := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_OFFICIAL_MODEL"))
	codexHome := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_HOME"))
	if codexPath == "" || configPath == "" || profileName == "" || thirdModel == "" || officialModel == "" || codexHome == "" {
		t.Fatal("live unified gateway test requires Codex path, config path, profile, third model, and official model")
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	launch, cleanup, err := startModelProfileAdapterForCodex(withCodexLoginProbePath(ctx, codexPath), store, profileName, modelprofile.Snapshot{}, "", true, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if !launch.Unified {
		t.Fatalf("launch is not unified: %#v", launch)
	}
	extraEnv := append(codexHomeEnv(codexHome), launch.effectiveEnvKey()+"="+launch.ProxyKey)
	catalog, err := os.ReadFile(launch.CatalogPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(catalog, []byte(`"slug": "`+officialModel+`"`)) || !bytes.Contains(catalog, []byte(`"slug": "`+thirdModel+`"`)) {
		t.Fatalf("merged catalog does not contain both target models: %s", catalog)
	}
	for _, test := range []struct {
		name   string
		model  string
		marker string
	}{
		{name: "official", model: officialModel, marker: "LIVE_UNIFIED_OFFICIAL_OK"},
		{name: "third-party", model: thirdModel, marker: "LIVE_UNIFIED_THIRD_OK"},
		{name: "official-return", model: officialModel, marker: "LIVE_UNIFIED_OFFICIAL_RETURN_OK"},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := appendCodexModelProfileArgs([]string{
				codexPath,
				"exec",
				"--ephemeral",
				"--ignore-user-config",
				"--json",
				"--sandbox", "read-only",
				"--skip-git-repo-check",
				"--model", test.model,
				"Reply with exactly " + test.marker + ".",
			}, launch)
			cmd := exec.CommandContext(ctx, args[0], args[1:]...)
			cmd.Env = mergeCLIEnvironment(os.Environ(), extraEnv)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("Codex failed: %v\n%s", err, output)
			}
			if !bytes.Contains(output, []byte(test.marker)) || !bytes.Contains(output, []byte(`"type":"turn.completed"`)) {
				t.Fatalf("Codex did not complete through expected route:\n%s", output)
			}
			cachedInputTokens := 0
			for _, line := range bytes.Split(output, []byte("\n")) {
				var event struct {
					Type  string `json:"type"`
					Usage struct {
						CachedInputTokens int `json:"cached_input_tokens"`
					} `json:"usage"`
				}
				if json.Unmarshal(line, &event) == nil && event.Type == "turn.completed" {
					cachedInputTokens = event.Usage.CachedInputTokens
				}
			}
			t.Logf("cached_input_tokens=%d", cachedInputTokens)
		})
	}
}

func TestLiveTeamsOfficialCatalogWithResolvedRuntimeOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_UNIFIED_GATEWAY")) != "1" {
		t.Skip("set CODEX_HELPER_LIVE_UNIFIED_GATEWAY=1 to run real Teams official catalog coverage")
	}
	codexPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_PATH"))
	configPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CONFIG_PATH"))
	officialModel := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_OFFICIAL_MODEL"))
	codexHome := strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_CODEX_HOME"))
	if codexPath == "" || configPath == "" || officialModel == "" || codexHome == "" {
		t.Fatal("live Teams catalog test requires Codex path, config path, official model, and Codex home")
	}
	// Package tests intentionally isolate HOME. Bind managed discovery to the
	// supplied live launcher instead of accidentally installing or selecting a
	// second Codex under the test HOME.
	managedPrefix := filepath.Dir(filepath.Dir(codexPath))
	t.Setenv("CODEX_NPM_PREFIX", managedPrefix)
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(cfg *config.Config) error {
		cfg.TeamsCodexPath.Mode = "service"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	root := &rootOptions{configPath: configPath}
	resolver := newTeamsCodexRuntimeResolver(root, "", t.TempDir(), io.Discard)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	contract, err := resolver(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resolvedCommand, err := filepath.EvalSymlinks(contract.Runtime.Command)
	if err != nil {
		t.Fatal(err)
	}
	wantCommand, err := filepath.EvalSymlinks(codexPath)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedCommand != wantCommand || envValue(contract.Runtime.Environment, "CODEX_HOME") != codexHome {
		t.Fatalf("resolved Teams runtime = command %q CODEX_HOME %q, want %q and %q", resolvedCommand, envValue(contract.Runtime.Environment, "CODEX_HOME"), wantCommand, codexHome)
	}
	manager := newTeamsModelProfileManagerWithRuntime(root, resolver)
	listed, err := manager.ListModelProfiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(listed, "Official Codex models") || !strings.Contains(listed, "(`"+officialModel+"`)") {
		t.Fatalf("Teams model list did not expose the authenticated official catalog:\n%s", listed)
	}
	snapshot, err := newTeamsModelProfileResolverWithRuntime(root, resolver)(ctx, "official:"+officialModel)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Provider != modelprofile.DefaultProvider || snapshot.Model != officialModel {
		t.Fatalf("official resolver snapshot = %#v", snapshot)
	}
}
