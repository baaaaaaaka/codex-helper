package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestSimulatedTeamsConcurrentWorkChatsModelProfilesAndLongOutputCI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	graph, sent, created := newDeterministicStressGraph(t)
	store := newBridgeTestStore(t)
	executor := &deterministicStressExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.reg.Sessions = nil
	bridge.modelProfileResolver = deterministicStressModelProfileResolver("ci")

	scenarios := []deterministicLiveScenario{
		{Name: "alpha-chat", Profile: "alpha", Title: "CI alpha", Dir: filepath.Join(t.TempDir(), "alpha"), Nonce: "CI"},
		{Name: "beta-chat", Profile: "beta", Title: "CI beta", Dir: filepath.Join(t.TempDir(), "beta"), Nonce: "CI"},
	}
	for _, scenario := range scenarios {
		if err := os.MkdirAll(scenario.Dir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", scenario.Dir, err)
		}
		cmd := "new " + scenario.Dir + " --model-profile " + scenario.Profile + " -- " + scenario.Title
		if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("control-"+scenario.Profile, cmd), cmd); err != nil {
			t.Fatalf("create %s work chat: %v", scenario.Name, err)
		}
	}
	if got := len(*created); got != len(scenarios) {
		t.Fatalf("created work chats = %d, want %d; created=%#v", got, len(scenarios), *created)
	}

	for i := range scenarios {
		session := bridge.reg.Sessions[i]
		scenarios[i].Session = session
		if filepath.Clean(session.Cwd) != filepath.Clean(scenarios[i].Dir) {
			t.Fatalf("session %d cwd = %q, want %q", i, session.Cwd, scenarios[i].Dir)
		}
		if session.ModelProfile.Name != scenarios[i].Profile {
			t.Fatalf("session %d model profile = %#v, want %s", i, session.ModelProfile, scenarios[i].Profile)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(scenarios))
	for i := range scenarios {
		scenario := scenarios[i]
		prompt := scenario.marker(0) + " LONG_OUTPUT simulated concurrent work for " + scenario.Name
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := bridge.handleSessionMessage(ctx, scenario.Session.ChatID, bridgeTestMessageWithText("work-"+scenario.Name, prompt), prompt); err != nil {
				errs <- fmt.Errorf("%s handle session: %w", scenario.Name, err)
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

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	for _, scenario := range scenarios {
		if got := liveCompletedTurnCount(state, scenario.Session.ID); got != 1 {
			t.Fatalf("%s completed turns = %d, want 1; turns=%#v", scenario.Name, got, state.Turns)
		}
		marker := scenario.marker(0)
		if got := liveOutboxMarkerCount(state, scenario.Session.ChatID, marker); got != 1 {
			t.Fatalf("%s marker %s count = %d, want 1", scenario.Name, marker, got)
		}
		for _, other := range scenarios {
			if other.Session.ChatID == scenario.Session.ChatID {
				continue
			}
			if got := liveOutboxMarkerCount(state, other.Session.ChatID, marker); got != 0 {
				t.Fatalf("%s marker %s leaked into %s chat %d time(s)", scenario.Name, marker, other.Name, got)
			}
		}
		finalParts := 0
		for _, msg := range state.OutboxMessages {
			if msg.TeamsChatID == scenario.Session.ChatID && (msg.Kind == "final" || strings.HasPrefix(msg.Kind, "final-")) {
				finalParts++
			}
		}
		if finalParts < 2 {
			t.Fatalf("%s final parts = %d, want long output split into at least two parts", scenario.Name, finalParts)
		}
	}
	executor.assertProfiles(t, map[string]string{
		scenarios[0].Session.ID: scenarios[0].Profile,
		scenarios[1].Session.ID: scenarios[1].Profile,
	})

	joined := sentPlainJoined(*sent)
	for _, want := range []string{
		"model:alpha",
		"model:beta",
		scenarios[0].marker(0),
		scenarios[1].marker(0),
		"DETERMINISTIC-STRESS-OK",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("sent Teams output missing %q in:\n%s", want, joined)
		}
	}
}

func TestLiveBridgeDeterministicConcurrentWorkChatsOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_DETERMINISTIC_STRESS")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_DETERMINISTIC_STRESS=1 to run live deterministic Teams bridge stress")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "deterministic-work-chats")

	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
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
	t.Setenv(envTeamsProfile, "deterministic-"+nonce)

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
		t.Fatalf("open deterministic live store: %v", err)
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
	t.Logf("LIVE_DETERMINISTIC_CONTROL_CHAT_URL=%s", controlChat.WebURL)
	if _, err := store.RecordChatPollSuccess(ctx, controlChat.ID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed deterministic control poll cursor failed: %v", err)
	}

	executor := &deterministicStressExecutor{}
	resolver := deterministicStressModelProfileResolver(nonce)
	listenCtx, stopListen := context.WithCancel(ctx)
	listenErr := make(chan error, 1)
	go func() {
		listenErr <- bridge.Listen(listenCtx, BridgeOptions{
			RegistryPath:             registryPath,
			Store:                    store,
			HelperVersion:            "live-deterministic-stress",
			Interval:                 2 * time.Second,
			Top:                      20,
			MaxWorkChatPollsPerCycle: 4,
			Executor:                 executor,
			ModelProfileResolver:     resolver,
		})
	}()
	defer func() {
		stopListen()
		select {
		case err := <-listenErr:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("deterministic live bridge listener failed: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("timed out waiting for deterministic live bridge listener to stop")
		}
	}()

	scenarios := deterministicLiveScenarios(t, tmp, nonce)
	for _, scenario := range scenarios {
		liveSendText(ctx, t, graph, controlChat.ID, "new "+scenario.Dir+" --model-profile "+scenario.Profile+" -- "+scenario.Title)
	}
	for i := range scenarios {
		scenario := &scenarios[i]
		session := waitForLiveSessionByCwd(t, ctx, registryPath, scenario.Dir)
		scenario.Session = session
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, session.ChatID)
		waitForLiveOutbox(t, ctx, store, session.ChatID, nil, scenario.Name+" ready", 30, "Work chat is ready", filepath.Base(scenario.Dir), "model:"+scenario.Profile)
		if _, err := store.RecordChatPollSuccess(ctx, session.ChatID, time.Now().UTC(), true, false, 0); err != nil {
			t.Fatalf("seed deterministic work poll cursor for %s failed: %v", scenario.Name, err)
		}
		t.Logf("LIVE_DETERMINISTIC_WORK_CHAT name=%s chat=%s url=%s profile=%s", scenario.Name, session.ChatID, session.ChatURL, scenario.Profile)
	}

	rounds := deterministicLiveRounds(t)
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
				if _, err := graph.SendHTML(ctx, scenario.Session.ChatID, "<p>"+marker+" deterministic turn "+strconv.Itoa(round+1)+" for "+scenario.Name+"</p>"); err != nil {
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
			if retryTurnID := waitForLiveOutboxOrAmbiguousTurnAfter(t, ctx, store, scenario.Session.ChatID, nil, fmt.Sprintf("%s round %02d final", scenario.Name, round+1), 40, afterSeq[scenario.Name], marker, "DETERMINISTIC-STRESS-OK", "profile="+scenario.Profile); retryTurnID != "" {
				t.Fatalf("%s round %02d became ambiguous: %s", scenario.Name, round+1, retryTurnID)
			}
		}
		t.Logf("LIVE_DETERMINISTIC_ROUND_DONE round=%02d chats=%d", round+1, len(scenarios))
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load deterministic live store failed: %v", err)
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
		t.Fatalf("deterministic live test left non-terminal turns: %s", stuck)
	}
	executor.assertProfiles(t, map[string]string{
		scenarios[0].Session.ID: scenarios[0].Profile,
		scenarios[1].Session.ID: scenarios[1].Profile,
	})
}

type deterministicLiveScenario struct {
	Name    string
	Profile string
	Title   string
	Dir     string
	Session Session
	Nonce   string
}

func (s deterministicLiveScenario) marker(round int) string {
	return "DET_" + strings.ToUpper(strings.ReplaceAll(s.Name, "-", "_")) + "_" + s.Nonce + "_" + fmt.Sprintf("%02d", round+1)
}

func deterministicLiveScenarios(t *testing.T, baseDir string, nonce string) []deterministicLiveScenario {
	t.Helper()
	out := []deterministicLiveScenario{
		{Name: "alpha-chat", Profile: "alpha", Title: "Deterministic alpha " + nonce, Dir: filepath.Join(baseDir, "alpha-"+nonce), Nonce: nonce},
		{Name: "beta-chat", Profile: "beta", Title: "Deterministic beta " + nonce, Dir: filepath.Join(baseDir, "beta-"+nonce), Nonce: nonce},
	}
	for _, scenario := range out {
		if err := os.MkdirAll(scenario.Dir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", scenario.Dir, err)
		}
	}
	return out
}

func deterministicLiveRounds(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_DETERMINISTIC_STRESS_ROUNDS"))
	if raw == "" {
		return 2
	}
	rounds, err := strconv.Atoi(raw)
	if err != nil || rounds <= 0 || rounds > 6 {
		t.Fatalf("CODEX_HELPER_TEAMS_LIVE_DETERMINISTIC_STRESS_ROUNDS=%q, want 1..6", raw)
	}
	return rounds
}

func deterministicStressModelProfileResolver(nonce string) ModelProfileResolver {
	now := time.Now().UTC()
	return func(_ context.Context, ref string) (modelprofile.Snapshot, error) {
		switch strings.TrimSpace(ref) {
		case "", "default":
			return modelprofile.Snapshot{Name: "default", Provider: modelprofile.DefaultProvider, Revision: 1, CapturedAt: now}, nil
		case "alpha":
			return modelprofile.Snapshot{Name: "alpha", Provider: "deepseek", APIKeyRef: "env:DETERMINISTIC_ALPHA_" + nonce, SSHProxy: "ssh-alpha", Revision: 11, CapturedAt: now}, nil
		case "beta":
			return modelprofile.Snapshot{Name: "beta", Provider: "mimo", APIKeyRef: "env:DETERMINISTIC_BETA_" + nonce, SSHProxy: "ssh-beta", Revision: 22, CapturedAt: now}, nil
		default:
			return modelprofile.Snapshot{}, fmt.Errorf("unknown deterministic profile %q", ref)
		}
	}
}

type deterministicStressStart struct {
	SessionID string
	Marker    string
	Profile   modelprofile.Snapshot
}

type deterministicStressExecutor struct {
	mu     sync.Mutex
	counts map[string]int
	starts []deterministicStressStart
}

func (e *deterministicStressExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e *deterministicStressExecutor) RunWithEventHandler(ctx context.Context, session *Session, prompt string, handler codexrunner.EventHandler) (ExecutionResult, error) {
	select {
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	default:
	}
	sessionID := ""
	var snapshot modelprofile.Snapshot
	if session != nil {
		sessionID = session.ID
		snapshot = session.ModelProfile
	}
	visible := strings.TrimSpace(StripHelperPromptEchoes(prompt))
	marker := firstDeterministicMarker(visible)
	e.mu.Lock()
	if e.counts == nil {
		e.counts = make(map[string]int)
	}
	e.counts[sessionID]++
	count := e.counts[sessionID]
	e.starts = append(e.starts, deterministicStressStart{SessionID: sessionID, Marker: marker, Profile: snapshot})
	e.mu.Unlock()
	if handler != nil {
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Text: "deterministic progress"})
	}
	text := fmt.Sprintf("DETERMINISTIC-STRESS-OK %s profile=%s provider=%s turn=%02d", marker, snapshot.Name, snapshot.Provider, count)
	if strings.Contains(visible, "LONG_OUTPUT") {
		text += "\n" + strings.Repeat("deterministic long output filler for Teams chunking. ", 1800)
	}
	return ExecutionResult{
		Text:          text,
		CodexThreadID: "thread-" + sessionID,
		CodexTurnID:   fmt.Sprintf("turn-%s-%02d", sessionID, count),
	}, nil
}

func (e *deterministicStressExecutor) assertProfiles(t *testing.T, want map[string]string) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	seen := make(map[string]map[string]int)
	for _, start := range e.starts {
		if seen[start.SessionID] == nil {
			seen[start.SessionID] = make(map[string]int)
		}
		seen[start.SessionID][start.Profile.Name]++
	}
	for sessionID, profile := range want {
		if seen[sessionID][profile] == 0 {
			t.Fatalf("session %s did not run with profile %q; starts=%#v", sessionID, profile, e.starts)
		}
		for gotProfile, count := range seen[sessionID] {
			if gotProfile != profile && count > 0 {
				t.Fatalf("session %s also ran with unexpected profile %q; starts=%#v", sessionID, gotProfile, e.starts)
			}
		}
	}
}

func firstDeterministicMarker(text string) string {
	for _, field := range strings.Fields(text) {
		cleaned := strings.Trim(field, ".,;:!?)(")
		if strings.HasPrefix(cleaned, "DET_") {
			return cleaned
		}
	}
	return "DET_MISSING_MARKER"
}

func newDeterministicStressGraph(t *testing.T) (*GraphClient, *[]bridgeSentMessage, *[]string) {
	t.Helper()
	var mu sync.Mutex
	var sent []bridgeSentMessage
	var created []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			subject := decodeTestOnlineMeetingSubject(t, r)
			mu.Lock()
			chatID := fmt.Sprintf("work-chat-%02d", len(created)+1)
			created = append(created, chatID)
			mu.Unlock()
			writeTestOnlineMeeting(w, chatID, subject)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode Graph message request: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			mu.Lock()
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			id := len(sent)
			mu.Unlock()
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, id)
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(server.Close)
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &sent, &created
}
