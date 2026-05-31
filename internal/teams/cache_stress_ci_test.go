package teams

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

func TestTeamsThirdPartyCacheStressMultiChatLongConversationMockCI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	graph, _ := newBridgeAsyncQueueGraph(t)
	store := newBridgeTestStore(t)
	executor := &mockCacheStressExecutor{}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	now := time.Now()
	scenarios := []struct {
		ID       string
		ChatID   string
		Profile  modelprofile.Snapshot
		Revision int
	}{
		{ID: "cache-a", ChatID: "chat-cache-a", Profile: modelprofile.Snapshot{Name: "deepseek-live", Provider: "deepseek", APIKeyRef: "env:DEEPSEEK_API_KEY", Revision: 1, CapturedAt: now}},
		{ID: "cache-b", ChatID: "chat-cache-b", Profile: modelprofile.Snapshot{Name: "mimo25-live", Provider: "mimo", APIKeyRef: "env:MIMO_API_KEY", SSHProxy: "mimo-jump", Revision: 2, CapturedAt: now}},
		{ID: "cache-c", ChatID: "chat-cache-c", Profile: modelprofile.Snapshot{Name: "deepseek-alt", Provider: "deepseek", APIKeyRef: "env:DEEPSEEK_API_KEY_ALT", SSHProxy: "deepseek-jump", Revision: 3, CapturedAt: now}},
	}
	bridge.reg.Sessions = nil
	for _, scenario := range scenarios {
		bridge.reg.Sessions = append(bridge.reg.Sessions, Session{
			ID:            scenario.ID,
			ChatID:        scenario.ChatID,
			ChatURL:       "https://teams.example/" + scenario.ChatID,
			Topic:         "cache stress " + scenario.ID,
			Status:        "active",
			CodexThreadID: "thread-" + scenario.ID,
			ModelProfile:  scenario.Profile,
			CreatedAt:     now,
			UpdatedAt:     now,
		})
		if err := bridge.ensureDurableSession(ctx, &bridge.reg.Sessions[len(bridge.reg.Sessions)-1]); err != nil {
			t.Fatalf("ensure durable session %s: %v", scenario.ID, err)
		}
	}

	orders := [][]int{
		{0, 1, 2},
		{2, 0, 1},
		{1, 2, 0},
	}
	const rounds = 8
	for round := 0; round < rounds; round++ {
		roundNum := round
		var wg sync.WaitGroup
		errs := make(chan error, len(scenarios))
		for _, idx := range orders[roundNum%len(orders)] {
			scenario := scenarios[idx]
			session := bridge.reg.SessionByID(scenario.ID)
			marker := mockCacheStressMarker(scenario.ID, roundNum)
			prompt := fmt.Sprintf("%s continue long cache-sensitive work round %02d for provider %s", marker, roundNum+1, scenario.Profile.Provider)
			wg.Add(1)
			go func() {
				defer wg.Done()
				msg := bridgeTestMessageWithText(fmt.Sprintf("mock-cache-%s-%02d", scenario.ID, roundNum+1), prompt)
				if err := bridge.handleSessionMessage(ctx, session.ChatID, msg, prompt); err != nil {
					errs <- fmt.Errorf("%s round %02d: %w", scenario.ID, roundNum+1, err)
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
		waitForBridgeAsyncTurns(t, bridge)
		for _, scenario := range scenarios {
			waitForNoActiveTurnsOrOutbox(t, store, scenario.ID)
		}
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load cache stress state: %v", err)
	}
	for _, scenario := range scenarios {
		session := state.Sessions[scenario.ID]
		if session.CodexThreadID != "thread-"+scenario.ID {
			t.Fatalf("%s CodexThreadID = %q, want stable thread", scenario.ID, session.CodexThreadID)
		}
		if !modelProfileSnapshotsEqual(session.ModelProfile, scenario.Profile) {
			t.Fatalf("%s durable model profile = %#v, want %#v", scenario.ID, session.ModelProfile, scenario.Profile)
		}
		if got := liveCompletedTurnCount(state, scenario.ID); got != rounds {
			t.Fatalf("%s completed turns = %d, want %d", scenario.ID, got, rounds)
		}
		for round := 0; round < rounds; round++ {
			marker := mockCacheStressMarker(scenario.ID, round)
			if got := liveOutboxMarkerCount(state, scenario.ChatID, marker); got != 1 {
				t.Fatalf("%s marker %s count = %d, want 1 in its own chat", scenario.ID, marker, got)
			}
			for _, other := range scenarios {
				if other.ChatID == scenario.ChatID {
					continue
				}
				if got := liveOutboxMarkerCount(state, other.ChatID, marker); got != 0 {
					t.Fatalf("%s marker %s leaked into %s %d time(s)", scenario.ID, marker, other.ID, got)
				}
			}
		}
	}
	if stuck := liveNonTerminalTurnSummary(state); stuck != "" {
		t.Fatalf("mock cache stress left non-terminal turns: %s", stuck)
	}
	executor.assertStableCache(t, rounds)
}

func mockCacheStressMarker(sessionID string, round int) string {
	return "CACHE_MOCK_" + strings.ToUpper(strings.ReplaceAll(sessionID, "-", "_")) + fmt.Sprintf("_%02d", round+1)
}

type mockCacheStressObservation struct {
	SessionID string
	Provider  string
	Profile   modelprofile.Snapshot
	ThreadID  string
	Turn      int
	Input     int
	Cached    int
	Marker    string
}

type mockCacheStressExecutor struct {
	mu               sync.Mutex
	counts           map[string]int
	threadBySession  map[string]string
	profileBySession map[string]modelprofile.Snapshot
	observations     []mockCacheStressObservation
}

func (e *mockCacheStressExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, prompt, nil)
}

func (e *mockCacheStressExecutor) RunWithEventHandler(ctx context.Context, session *Session, prompt string, handler codexrunner.EventHandler) (ExecutionResult, error) {
	select {
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	default:
	}
	if session == nil {
		return ExecutionResult{}, fmt.Errorf("missing session")
	}
	visible := strings.TrimSpace(StripHelperPromptEchoes(prompt))
	marker := firstMockCacheStressMarker(visible)
	e.mu.Lock()
	if e.counts == nil {
		e.counts = make(map[string]int)
		e.threadBySession = make(map[string]string)
		e.profileBySession = make(map[string]modelprofile.Snapshot)
	}
	e.counts[session.ID]++
	count := e.counts[session.ID]
	threadID := e.threadBySession[session.ID]
	if threadID == "" {
		threadID = firstNonEmptyString(session.CodexThreadID, "thread-"+session.ID)
		e.threadBySession[session.ID] = threadID
	}
	if prev, ok := e.profileBySession[session.ID]; ok && !modelProfileSnapshotsEqual(prev, session.ModelProfile) {
		e.mu.Unlock()
		return ExecutionResult{}, fmt.Errorf("model profile changed within session %s: prev=%#v now=%#v", session.ID, prev, session.ModelProfile)
	}
	e.profileBySession[session.ID] = session.ModelProfile
	input := 1200 + count*111
	cached := 0
	if count > 1 {
		cached = input - 96
	}
	e.observations = append(e.observations, mockCacheStressObservation{
		SessionID: session.ID,
		Provider:  session.ModelProfile.Provider,
		Profile:   session.ModelProfile,
		ThreadID:  threadID,
		Turn:      count,
		Input:     input,
		Cached:    cached,
		Marker:    marker,
	})
	e.mu.Unlock()
	if handler != nil {
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Text: fmt.Sprintf("mock cache continuing %s turn %02d", session.ID, count)})
	}
	return ExecutionResult{
		Text:          fmt.Sprintf("MOCK_CACHE_STRESS_OK %s provider=%s turn=%02d input=%d cached=%d", marker, session.ModelProfile.Provider, count, input, cached),
		CodexThreadID: threadID,
		CodexTurnID:   fmt.Sprintf("turn-%s-%02d", session.ID, count),
	}, nil
}

func (e *mockCacheStressExecutor) assertStableCache(t *testing.T, rounds int) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.counts) == 0 {
		t.Fatal("mock cache executor did not run")
	}
	for sessionID, count := range e.counts {
		if count != rounds {
			t.Fatalf("%s executor turns = %d, want %d; observations=%#v", sessionID, count, rounds, e.observations)
		}
	}
	bySession := make(map[string][]mockCacheStressObservation)
	for _, obs := range e.observations {
		bySession[obs.SessionID] = append(bySession[obs.SessionID], obs)
	}
	for sessionID, observations := range bySession {
		threadID := ""
		provider := ""
		for _, obs := range observations {
			if threadID == "" {
				threadID = obs.ThreadID
			} else if obs.ThreadID != threadID {
				t.Fatalf("%s thread changed from %s to %s; observations=%#v", sessionID, threadID, obs.ThreadID, observations)
			}
			if provider == "" {
				provider = obs.Provider
			} else if obs.Provider != provider {
				t.Fatalf("%s provider changed from %s to %s; observations=%#v", sessionID, provider, obs.Provider, observations)
			}
			if obs.Turn > 1 && obs.Cached <= 0 {
				t.Fatalf("%s turn %02d did not simulate a cache hit: %#v", sessionID, obs.Turn, obs)
			}
			if obs.Turn > 1 && float64(obs.Cached)/float64(obs.Input) < 0.85 {
				t.Fatalf("%s turn %02d cache hit rate too low: input=%d cached=%d", sessionID, obs.Turn, obs.Input, obs.Cached)
			}
		}
	}
}

func firstMockCacheStressMarker(text string) string {
	for _, field := range strings.Fields(text) {
		cleaned := strings.Trim(field, ".,;:!?)(")
		if strings.HasPrefix(cleaned, "CACHE_MOCK_") {
			return cleaned
		}
	}
	return "CACHE_MOCK_MISSING"
}
