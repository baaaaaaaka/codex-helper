package teams

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestLiveBridgeThirdPartyAPIDeepSeekAndMimoOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_THIRDPARTY_API_STRESS")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_THIRDPARTY_API_STRESS=1 to run live Teams third-party API stress")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "thirdparty-api-work-chats")

	providers := liveThirdPartyProvidersFromEnv(t)
	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Minute)
	defer cancel()

	cfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(cfg.CachePath); err != nil {
		t.Fatalf("read Teams chat token cache %s: %v", cfg.CachePath, err)
	}
	readCfg, err := DefaultReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultReadAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(readCfg.CachePath); err != nil {
		t.Fatalf("read Teams read token cache %s: %v", readCfg.CachePath, err)
	}
	t.Setenv(envTeamsProfile, "thirdparty-"+nonce)

	auth := NewAuthManager(cfg)
	graph := NewGraphClient(auth, io.Discard)
	readGraph := NewGraphClient(NewAuthManager(readCfg), io.Discard)
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me failed: %v", err)
	}
	if normalizeLiveHumanName(me.DisplayName) != "jason wei" {
		t.Fatalf("logged-in user displayName %q is not Jason Wei", me.DisplayName)
	}

	tmp := t.TempDir()
	registryPath := filepath.Join(tmp, "registry.json")
	store, err := teamstore.Open(filepath.Join(tmp, "state.json"))
	if err != nil {
		t.Fatalf("open third-party live store: %v", err)
	}
	bridge, err := NewBridge(ctx, auth, registryPath, io.Discard)
	if err != nil {
		t.Fatalf("NewBridge error: %v", err)
	}
	bridge.readGraph = readGraph
	bridge.store = store
	controlChat, err := bridge.EnsureControlChat(ctx)
	if err != nil {
		t.Fatalf("EnsureControlChat live error: %v", err)
	}
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, controlChat.ID)
	t.Logf("LIVE_THIRDPARTY_CONTROL_CHAT_URL=%s", controlChat.WebURL)
	if _, err := store.RecordChatPollSuccess(ctx, controlChat.ID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed third-party control poll cursor failed: %v", err)
	}

	executor := &liveThirdPartyAPIExecutor{providers: providers}
	listenCtx, stopListen := context.WithCancel(ctx)
	listenErr := make(chan error, 1)
	go func() {
		listenErr <- bridge.Listen(listenCtx, BridgeOptions{
			RegistryPath:             registryPath,
			Store:                    store,
			HelperVersion:            "live-thirdparty-api-stress",
			Interval:                 2 * time.Second,
			Top:                      20,
			MaxWorkChatPollsPerCycle: len(providers),
			Executor:                 executor,
			ModelProfileResolver:     liveThirdPartyModelProfileResolver(providers),
		})
	}()
	defer func() {
		stopListen()
		select {
		case err := <-listenErr:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("third-party live bridge listener failed: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("timed out waiting for third-party live bridge listener to stop")
		}
	}()

	scenarios := []liveThirdPartyScenario{
		{Name: "deepseek-chat", ProviderID: "deepseek", Profile: providers["deepseek"].ProfileName, Dir: filepath.Join(tmp, "deepseek-"+nonce), Nonce: nonce},
		{Name: "mimo-chat", ProviderID: "mimo", Profile: providers["mimo"].ProfileName, Dir: filepath.Join(tmp, "mimo-"+nonce), Nonce: nonce},
	}
	for _, scenario := range scenarios {
		if err := os.MkdirAll(scenario.Dir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", scenario.Dir, err)
		}
		liveSendText(ctx, t, graph, controlChat.ID, "new "+scenario.Dir+" --model-profile "+scenario.Profile+" -- "+scenario.Name+" "+nonce)
	}
	for i := range scenarios {
		scenario := &scenarios[i]
		session := waitForLiveSessionByCwd(t, ctx, registryPath, scenario.Dir)
		scenario.Session = session
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, session.ChatID)
		waitForLiveOutbox(t, ctx, store, session.ChatID, nil, scenario.Name+" ready", 30, "Work chat is ready", filepath.Base(scenario.Dir), "model:"+scenario.Profile)
		if _, err := store.RecordChatPollSuccess(ctx, session.ChatID, time.Now().UTC(), true, false, 0); err != nil {
			t.Fatalf("seed third-party work poll cursor for %s failed: %v", scenario.Name, err)
		}
		t.Logf("LIVE_THIRDPARTY_WORK_CHAT name=%s chat=%s url=%s provider=%s model=%s", scenario.Name, session.ChatID, session.ChatURL, scenario.ProviderID, providers[scenario.ProviderID].Model)
	}

	rounds := liveThirdPartyRounds(t)
	for round := 0; round < rounds; round++ {
		afterSeq := make(map[string]int64, len(scenarios))
		for _, scenario := range scenarios {
			afterSeq[scenario.Name] = liveOutboxMaxSequence(t, ctx, store, scenario.Session.ChatID)
		}
		var wg sync.WaitGroup
		errs := make(chan error, len(scenarios))
		for _, scenario := range scenarios {
			scenario := scenario
			marker := scenario.marker(round)
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := graph.SendHTML(ctx, scenario.Session.ChatID, "<p>"+marker+" via "+scenario.ProviderID+" real third-party API</p>"); err != nil {
					errs <- fmt.Errorf("%s round %d send: %w", scenario.Name, round+1, err)
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, scenario := range scenarios {
			marker := scenario.marker(round)
			waitForLiveThirdPartyFinalAfter(t, ctx, store, scenario.Session.ChatID, fmt.Sprintf("%s third-party round %02d final", scenario.Name, round+1), 60, afterSeq[scenario.Name], marker, "THIRDPARTY-API-OK", "provider="+scenario.ProviderID)
		}
		t.Logf("LIVE_THIRDPARTY_ROUND_DONE round=%02d chats=%d", round+1, len(scenarios))
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load third-party live store failed: %v", err)
	}
	for _, scenario := range scenarios {
		for round := 0; round < rounds; round++ {
			marker := scenario.marker(round)
			if got := liveOutboxMarkerCount(state, scenario.Session.ChatID, marker); got != 1 {
				t.Fatalf("%s marker %s count = %d, want 1", scenario.Name, marker, got)
			}
		}
		if got := liveCompletedTurnCount(state, scenario.Session.ID); got < rounds {
			t.Fatalf("%s completed turns = %d, want at least %d", scenario.Name, got, rounds)
		}
	}
	if stuck := liveNonTerminalTurnSummary(state); stuck != "" {
		t.Fatalf("third-party live test left non-terminal turns: %s", stuck)
	}
	executor.assertProviders(t, map[string]string{
		scenarios[0].Session.ID: scenarios[0].ProviderID,
		scenarios[1].Session.ID: scenarios[1].ProviderID,
	})
}

type liveThirdPartyProvider struct {
	ProviderID  string
	ProfileName string
	Model       string
	BaseURL     string
	APIKey      string
	Revision    int
}

func liveThirdPartyProvidersFromEnv(t *testing.T) map[string]liveThirdPartyProvider {
	t.Helper()
	deepseekKey := strings.TrimSpace(os.Getenv("DEEPSEEK_API_KEY"))
	if deepseekKey == "" {
		t.Fatal("DEEPSEEK_API_KEY is required for live third-party Teams stress")
	}
	mimoKey := strings.TrimSpace(os.Getenv("MIMO_API_KEY"))
	if mimoKey == "" {
		t.Fatal("MIMO_API_KEY is required for live third-party Teams stress")
	}
	return map[string]liveThirdPartyProvider{
		"deepseek": {
			ProviderID:  "deepseek",
			ProfileName: "deepseek-live",
			Model:       "deepseek-v4-flash",
			BaseURL:     "https://api.deepseek.com/v1",
			APIKey:      deepseekKey,
			Revision:    1,
		},
		"mimo": {
			ProviderID:  "mimo",
			ProfileName: "mimo25-live",
			Model:       "mimo-v2.5",
			BaseURL:     "https://api.xiaomimimo.com/v1",
			APIKey:      mimoKey,
			Revision:    1,
		},
	}
}

func liveThirdPartyModelProfileResolver(providers map[string]liveThirdPartyProvider) ModelProfileResolver {
	now := time.Now().UTC()
	return func(_ context.Context, ref string) (modelprofile.Snapshot, error) {
		ref = strings.TrimSpace(ref)
		for _, provider := range providers {
			if ref == provider.ProfileName || ref == provider.ProviderID {
				return modelprofile.Snapshot{
					Name:       provider.ProfileName,
					Provider:   provider.ProviderID,
					APIKeyRef:  "env:" + strings.ToUpper(provider.ProviderID) + "_API_KEY",
					Revision:   provider.Revision,
					CapturedAt: now,
				}, nil
			}
		}
		return modelprofile.Snapshot{}, fmt.Errorf("unknown live third-party model profile %q", ref)
	}
}

type liveThirdPartyScenario struct {
	Name       string
	ProviderID string
	Profile    string
	Dir        string
	Nonce      string
	Session    Session
}

func (s liveThirdPartyScenario) marker(round int) string {
	prefix := "TP"
	switch s.ProviderID {
	case "deepseek":
		prefix += "DS"
	case "mimo":
		prefix += "MI"
	default:
		prefix += strings.ToUpper(shortStableID(s.ProviderID))[:2]
	}
	return prefix + strings.ToUpper(shortStableID(s.Nonce))[:6] + fmt.Sprintf("%02d", round+1)
}

func liveThirdPartyRounds(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_THIRDPARTY_API_STRESS_ROUNDS"))
	if raw == "" {
		return 1
	}
	rounds, err := strconv.Atoi(raw)
	if err != nil || rounds <= 0 || rounds > 4 {
		t.Fatalf("CODEX_HELPER_TEAMS_LIVE_THIRDPARTY_API_STRESS_ROUNDS=%q, want 1..4", raw)
	}
	return rounds
}

type liveThirdPartyStart struct {
	SessionID  string
	ProviderID string
	Marker     string
}

type liveThirdPartyAPIExecutor struct {
	providers map[string]liveThirdPartyProvider
	mu        sync.Mutex
	counts    map[string]int
	starts    []liveThirdPartyStart
}

func (e *liveThirdPartyAPIExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e *liveThirdPartyAPIExecutor) RunWithEventHandler(ctx context.Context, session *Session, prompt string, handler codexrunner.EventHandler) (ExecutionResult, error) {
	if session == nil {
		return ExecutionResult{}, fmt.Errorf("third-party live executor requires a Teams session")
	}
	providerID := strings.TrimSpace(session.ModelProfile.Provider)
	provider, ok := e.providers[providerID]
	if !ok {
		return ExecutionResult{}, fmt.Errorf("no live provider configured for %q", providerID)
	}
	marker := firstThirdPartyMarker(prompt)
	if marker == "" {
		return ExecutionResult{}, fmt.Errorf("third-party prompt missing marker: %q", StripHelperPromptEchoes(prompt))
	}
	e.mu.Lock()
	if e.counts == nil {
		e.counts = make(map[string]int)
	}
	e.counts[session.ID]++
	count := e.counts[session.ID]
	e.starts = append(e.starts, liveThirdPartyStart{SessionID: session.ID, ProviderID: providerID, Marker: marker})
	e.mu.Unlock()

	if handler != nil {
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Text: "calling " + providerID + " third-party model"})
	}
	text, err := callLiveThirdPartyAPI(ctx, provider, marker)
	if err != nil {
		return ExecutionResult{}, err
	}
	return ExecutionResult{
		Text:          fmt.Sprintf("THIRDPARTY-API-OK provider=%s model=%s marker=%s output=%s", providerID, provider.Model, marker, text),
		CodexThreadID: "thirdparty-thread-" + session.ID,
		CodexTurnID:   fmt.Sprintf("thirdparty-turn-%s-%02d", session.ID, count),
	}, nil
}

func callLiveThirdPartyAPI(ctx context.Context, provider liveThirdPartyProvider, marker string) (string, error) {
	maxTokens := 256
	temperature := 0.0
	adapter := responsesadapter.OpenAIChatAdapter{
		BaseURL:    provider.BaseURL,
		APIKey:     provider.APIKey,
		Profile:    responsesadapter.ProfileForProvider(provider.ProviderID),
		HTTPClient: &http.Client{Timeout: 90 * time.Second},
	}
	stream, err := adapter.Stream(ctx, responsesadapter.ProviderRequest{
		Model:           provider.Model,
		InputText:       "Reply with exactly this uppercase code and no other text: " + marker,
		MaxOutputTokens: &maxTokens,
		Temperature:     &temperature,
	})
	if err != nil {
		return "", fmt.Errorf("%s stream start: %w", provider.ProviderID, err)
	}
	return collectLiveThirdPartyStreamOutput(provider.ProviderID, marker, stream)
}

func collectLiveThirdPartyStreamOutput(providerID string, marker string, stream <-chan responsesadapter.ProviderEvent) (string, error) {
	var text strings.Builder
	var reasoning strings.Builder
	for event := range stream {
		switch event.Kind {
		case responsesadapter.ProviderEventTextDelta:
			text.WriteString(event.Delta)
		case responsesadapter.ProviderEventReasoningDelta:
			reasoning.WriteString(event.Delta)
		case responsesadapter.ProviderEventError:
			if event.Err != nil {
				return "", fmt.Errorf("%s stream error: %w", providerID, event.Err)
			}
			return "", fmt.Errorf("%s stream error", providerID)
		}
	}
	out := strings.TrimSpace(text.String())
	reasoningOut := strings.TrimSpace(reasoning.String())
	if !strings.Contains(out, marker) {
		if strings.Contains(reasoningOut, marker) {
			return reasoningOut, nil
		}
		return "", fmt.Errorf("%s output text=%q reasoning=%q did not contain marker %s", providerID, out, reasoningOut, marker)
	}
	return out, nil
}

func (e *liveThirdPartyAPIExecutor) assertProviders(t *testing.T, want map[string]string) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	seen := make(map[string]map[string]int)
	for _, start := range e.starts {
		if seen[start.SessionID] == nil {
			seen[start.SessionID] = make(map[string]int)
		}
		seen[start.SessionID][start.ProviderID]++
	}
	for sessionID, providerID := range want {
		if seen[sessionID][providerID] == 0 {
			t.Fatalf("session %s did not call provider %q; starts=%#v", sessionID, providerID, e.starts)
		}
		for gotProvider, count := range seen[sessionID] {
			if gotProvider != providerID && count > 0 {
				t.Fatalf("session %s also called unexpected provider %q; starts=%#v", sessionID, gotProvider, e.starts)
			}
		}
	}
}

func firstThirdPartyMarker(text string) string {
	for _, field := range strings.Fields(text) {
		cleaned := strings.Trim(field, ".,;:!?)(")
		if strings.HasPrefix(cleaned, "TP") {
			return cleaned
		}
	}
	return ""
}

func waitForLiveThirdPartyFinalAfter(t *testing.T, ctx context.Context, store *teamstore.Store, chatID string, label string, attempts int, afterSeq int64, parts ...string) {
	t.Helper()
	if attempts <= 0 {
		attempts = 1
	}
	deadline := time.Now().Add(8 * time.Minute)
	var lastErr error
	for attempt := 0; attempt < attempts && time.Now().Before(deadline); attempt++ {
		if _, ok, err := liveSentOutboxMessageContainsAfter(ctx, store, chatID, afterSeq, parts...); err != nil {
			lastErr = err
		} else if ok {
			return
		}
		if msg, ok, err := liveSentOutboxErrorAfter(ctx, store, chatID, afterSeq); err != nil {
			lastErr = err
		} else if ok {
			t.Fatalf("%s produced error outbox after seq %d: %s\n%s", label, afterSeq, msg.Body, liveOutboxDebug(ctx, store, chatID))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s canceled while waiting for live third-party outbox after seq %d %q: %v", label, afterSeq, parts, ctx.Err())
		case <-time.After(8 * time.Second):
		}
	}
	if lastErr != nil {
		t.Fatalf("%s did not produce expected live third-party outbox after seq %d %q: %v\n%s", label, afterSeq, parts, lastErr, liveOutboxDebug(ctx, store, chatID))
	}
	t.Fatalf("%s did not produce expected live third-party outbox after seq %d %q\n%s", label, afterSeq, parts, liveOutboxDebug(ctx, store, chatID))
}

func liveSentOutboxErrorAfter(ctx context.Context, store *teamstore.Store, chatID string, afterSeq int64) (teamstore.OutboxMessage, bool, error) {
	state, err := store.Load(ctx)
	if err != nil {
		return teamstore.OutboxMessage{}, false, err
	}
	for _, msg := range state.OutboxMessages {
		if msg.Sequence <= afterSeq || msg.TeamsChatID != chatID || msg.Status != teamstore.OutboxStatusSent || strings.TrimSpace(msg.TeamsMessageID) == "" {
			continue
		}
		if msg.Kind == "error" || strings.HasPrefix(strings.TrimSpace(msg.Body), "error:") {
			return msg, true, nil
		}
	}
	return teamstore.OutboxMessage{}, false, nil
}

func TestLiveThirdPartyMarkerShapeCI(t *testing.T) {
	t.Parallel()

	scenarios := []liveThirdPartyScenario{
		{Name: "deepseek", ProviderID: "deepseek", Nonce: "deepseek-live-nonce"},
		{Name: "mimo", ProviderID: "mimo", Nonce: "mimo-live-nonce"},
		{Name: "custom", ProviderID: "qwen", Nonce: "custom-live-nonce"},
	}
	for _, scenario := range scenarios {
		marker := scenario.marker(0)
		if len(marker) > 12 {
			t.Fatalf("%s marker %q length = %d, want <= 12 to avoid provider truncation", scenario.Name, marker, len(marker))
		}
		if got := firstThirdPartyMarker("please echo " + marker + " exactly"); got != marker {
			t.Fatalf("%s parsed marker = %q, want %q", scenario.Name, got, marker)
		}
		if strings.ContainsAny(marker, "_- ") {
			t.Fatalf("%s marker %q contains separator that providers may tokenize or trim unexpectedly", scenario.Name, marker)
		}
	}
	if got := firstThirdPartyMarker("old TP_DEEPSEEK_MARKER style"); got != "TP_DEEPSEEK_MARKER" {
		t.Fatalf("legacy TP_ marker parse = %q", got)
	}
}

func TestCollectLiveThirdPartyStreamOutputReasoningFallbackCI(t *testing.T) {
	t.Parallel()

	stream := make(chan responsesadapter.ProviderEvent, 3)
	stream <- responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventReasoningDelta, Delta: "think TPMIABC123"}
	stream <- responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventTextDelta, Delta: ""}
	stream <- responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventDone}
	close(stream)

	got, err := collectLiveThirdPartyStreamOutput("mimo", "TPMIABC123", stream)
	if err != nil {
		t.Fatalf("collect reasoning fallback error: %v", err)
	}
	if !strings.Contains(got, "TPMIABC123") {
		t.Fatalf("reasoning fallback output = %q, want marker", got)
	}
}

func TestCollectLiveThirdPartyStreamOutputRejectsMissingMarkerCI(t *testing.T) {
	t.Parallel()

	stream := make(chan responsesadapter.ProviderEvent, 2)
	stream <- responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventTextDelta, Delta: ""}
	stream <- responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventDone}
	close(stream)

	got, err := collectLiveThirdPartyStreamOutput("mimo", "TPMIEMPTY", stream)
	if err == nil {
		t.Fatalf("collect output = %q, want missing marker error", got)
	}
	if !strings.Contains(err.Error(), `text=""`) || !strings.Contains(err.Error(), `reasoning=""`) || !strings.Contains(err.Error(), "TPMIEMPTY") {
		t.Fatalf("missing marker error = %q, want text/reasoning debug and marker", err.Error())
	}
}

func TestCollectLiveThirdPartyStreamOutputPropagatesProviderErrorCI(t *testing.T) {
	t.Parallel()

	stream := make(chan responsesadapter.ProviderEvent, 1)
	stream <- responsesadapter.ProviderEvent{Kind: responsesadapter.ProviderEventError, Err: fmt.Errorf("provider blank response")}
	close(stream)

	_, err := collectLiveThirdPartyStreamOutput("deepseek", "TPDSERROR", stream)
	if err == nil || !strings.Contains(err.Error(), "deepseek stream error") || !strings.Contains(err.Error(), "provider blank response") {
		t.Fatalf("provider error = %v, want provider-prefixed stream error", err)
	}
}

func TestLiveSentOutboxErrorAfterDetectsSentErrorCI(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newBridgeTestStore(t)
	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:error",
		TeamsChatID: "chat-1",
		Kind:        "error",
		Body:        "provider error: empty response",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	sent, err := store.MarkOutboxSent(ctx, queued.ID, "teams-error")
	if err != nil {
		t.Fatalf("MarkOutboxSent error: %v", err)
	}
	if _, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:other-chat",
		TeamsChatID: "chat-2",
		Kind:        "error",
		Body:        "error: wrong chat",
	}); err != nil {
		t.Fatalf("QueueOutbox other chat error: %v", err)
	}

	msg, ok, err := liveSentOutboxErrorAfter(ctx, store, "chat-1", sent.Sequence-1)
	if err != nil {
		t.Fatalf("liveSentOutboxErrorAfter error: %v", err)
	}
	if !ok || msg.ID != sent.ID {
		t.Fatalf("liveSentOutboxErrorAfter = (%#v, %v), want sent error outbox", msg, ok)
	}
	if msg, ok, err := liveSentOutboxErrorAfter(ctx, store, "chat-1", sent.Sequence); err != nil {
		t.Fatalf("liveSentOutboxErrorAfter after sent seq error: %v", err)
	} else if ok {
		t.Fatalf("liveSentOutboxErrorAfter at sent seq = (%#v, true), want no newer error", msg)
	}
}
