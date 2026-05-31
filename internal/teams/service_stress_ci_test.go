package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

func TestTeamsInstalledBackgroundServiceModelProfileRecoveryStressCI(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	graph, _, created := newDeterministicStressGraph(t)
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	resolver := newMutableServiceStressResolver(map[string]modelprofile.Snapshot{
		"alpha": {Name: "alpha", Provider: "deepseek", APIKeyRef: "env:DEEPSEEK_API_KEY_A", SSHProxy: "ssh-alpha", Revision: 11, CapturedAt: now},
		"beta":  {Name: "beta", Provider: "mimo", APIKeyRef: "env:MIMO_API_KEY_B", SSHProxy: "ssh-beta", Revision: 22, CapturedAt: now},
	})
	firstExecutor := &deterministicStressExecutor{}
	bridge := newBridgeTestBridge(graph, store, firstExecutor)
	bridge.reg.Sessions = nil
	bridge.modelProfileResolver = resolver.Resolve
	bridge.asyncTurns = true

	tmp := t.TempDir()
	scenarios := []serviceStressScenario{
		{Name: "alpha-old", Profile: "alpha", Dir: filepath.Join(tmp, "alpha-old")},
		{Name: "beta-old", Profile: "beta", Dir: filepath.Join(tmp, "beta-old")},
	}
	for i := range scenarios {
		if err := os.MkdirAll(scenarios[i].Dir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", scenarios[i].Dir, err)
		}
		cmd := "new " + scenarios[i].Dir + " --model-profile " + scenarios[i].Profile + " -- service stress " + scenarios[i].Name
		if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("service-create-"+scenarios[i].Name, cmd), cmd); err != nil {
			t.Fatalf("create %s: %v", scenarios[i].Name, err)
		}
		session := sessionByCwdForServiceStress(t, bridge, scenarios[i].Dir)
		scenarios[i].SessionID = session.ID
		scenarios[i].ChatID = session.ChatID
		scenarios[i].InitialRevision = session.ModelProfile.Revision
	}
	if got := len(*created); got != len(scenarios) {
		t.Fatalf("created initial work chats = %d, want %d; created=%#v", got, len(scenarios), *created)
	}
	runServiceStressRounds(t, ctx, bridge, scenarios, 0, 3)

	beforeRestart, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load before service restart: %v", err)
	}
	for _, scenario := range scenarios {
		if beforeRestart.Sessions[scenario.SessionID].CodexThreadID != "thread-"+scenario.SessionID {
			t.Fatalf("%s thread before restart = %#v", scenario.Name, beforeRestart.Sessions[scenario.SessionID])
		}
	}

	resolver.Set("alpha", modelprofile.Snapshot{Name: "alpha", Provider: "deepseek", APIKeyRef: "env:DEEPSEEK_API_KEY_NEW", SSHProxy: "ssh-alpha-new", Revision: 99, CapturedAt: now.Add(time.Minute)})
	restartedExecutor := &deterministicStressExecutor{}
	restarted := newBridgeTestBridge(graph, store, restartedExecutor)
	restarted.reg.Sessions = nil
	restarted.modelProfileResolver = resolver.Resolve
	restarted.asyncTurns = true
	restartState, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load for restarted bridge: %v", err)
	}
	for _, scenario := range scenarios {
		if session := restarted.sessionForIDState(restartState, scenario.SessionID); session == nil {
			t.Fatalf("restarted bridge did not recover durable session %s", scenario.SessionID)
		}
	}

	newScenario := serviceStressScenario{Name: "alpha-new", Profile: "alpha", Dir: filepath.Join(tmp, "alpha-new"), FirstRound: 3}
	if err := os.MkdirAll(newScenario.Dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", newScenario.Dir, err)
	}
	cmd := "new " + newScenario.Dir + " --model-profile alpha -- service stress alpha new"
	if err := restarted.handleControlMessage(ctx, bridgeTestMessageWithText("service-create-alpha-new", cmd), cmd); err != nil {
		t.Fatalf("create alpha-new after profile update: %v", err)
	}
	newSession := sessionByCwdForServiceStress(t, restarted, newScenario.Dir)
	newScenario.SessionID = newSession.ID
	newScenario.ChatID = newSession.ChatID
	newScenario.InitialRevision = newSession.ModelProfile.Revision
	if newScenario.InitialRevision != 99 {
		t.Fatalf("new alpha session revision = %d, want updated revision 99", newScenario.InitialRevision)
	}
	scenarios = append(scenarios, newScenario)
	runServiceStressRounds(t, ctx, restarted, scenarios, 3, 3)

	finalState, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load final service stress state: %v", err)
	}
	for _, scenario := range scenarios {
		session := finalState.Sessions[scenario.SessionID]
		if session.CodexThreadID != "thread-"+scenario.SessionID {
			t.Fatalf("%s final thread = %q, want stable thread-%s", scenario.Name, session.CodexThreadID, scenario.SessionID)
		}
		wantTurns := 6
		if scenario.Name == "alpha-new" {
			wantTurns = 3
		}
		if got := liveCompletedTurnCount(finalState, scenario.SessionID); got != wantTurns {
			t.Fatalf("%s completed turns = %d, want %d", scenario.Name, got, wantTurns)
		}
		if scenario.Name != "alpha-new" && session.ModelProfile.Revision != scenario.InitialRevision {
			t.Fatalf("%s profile revision changed from %d to %d", scenario.Name, scenario.InitialRevision, session.ModelProfile.Revision)
		}
		if scenario.Name == "alpha-new" && session.ModelProfile.Revision != 99 {
			t.Fatalf("alpha-new profile revision = %d, want 99", session.ModelProfile.Revision)
		}
		for round := scenario.FirstRound; round < scenario.FirstRound+wantTurns; round++ {
			marker := scenario.marker(round)
			if got := liveOutboxMarkerCount(finalState, scenario.ChatID, marker); got != 1 {
				t.Fatalf("%s marker %s count = %d, want 1", scenario.Name, marker, got)
			}
		}
	}
	if stuck := liveNonTerminalTurnSummary(finalState); stuck != "" {
		t.Fatalf("service model-profile stress left non-terminal turns: %s", stuck)
	}
}

type serviceStressScenario struct {
	Name            string
	Profile         string
	Dir             string
	SessionID       string
	ChatID          string
	InitialRevision int
	FirstRound      int
}

func (s serviceStressScenario) marker(round int) string {
	return "DET_SERVICE_STRESS_" + strings.ToUpper(strings.ReplaceAll(s.Name, "-", "_")) + fmt.Sprintf("_%02d", round+1)
}

func runServiceStressRounds(t *testing.T, ctx context.Context, bridge *Bridge, scenarios []serviceStressScenario, startRound int, rounds int) {
	t.Helper()
	for round := startRound; round < startRound+rounds; round++ {
		roundNum := round
		var wg sync.WaitGroup
		errs := make(chan error, len(scenarios))
		for _, scenario := range scenarios {
			session := bridge.reg.SessionByID(scenario.SessionID)
			if session == nil {
				errs <- fmt.Errorf("%s session %s missing from bridge registry", scenario.Name, scenario.SessionID)
				continue
			}
			marker := scenario.marker(roundNum)
			prompt := fmt.Sprintf("%s long installed-service stress round %02d for profile %s", marker, roundNum+1, scenario.Profile)
			wg.Add(1)
			go func(s serviceStressScenario) {
				defer wg.Done()
				msg := bridgeTestMessageWithText(fmt.Sprintf("service-%s-%02d", s.Name, roundNum+1), prompt)
				if err := bridge.handleSessionMessage(ctx, session.ChatID, msg, prompt); err != nil {
					errs <- fmt.Errorf("%s round %02d: %w", s.Name, roundNum+1, err)
				}
			}(scenario)
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
			waitForNoActiveTurnsOrOutbox(t, bridge.store, scenario.SessionID)
		}
	}
}

type mutableServiceStressResolver struct {
	mu       sync.Mutex
	profiles map[string]modelprofile.Snapshot
}

func newMutableServiceStressResolver(profiles map[string]modelprofile.Snapshot) *mutableServiceStressResolver {
	out := &mutableServiceStressResolver{profiles: make(map[string]modelprofile.Snapshot, len(profiles))}
	for key, value := range profiles {
		out.profiles[key] = value
	}
	return out
}

func (r *mutableServiceStressResolver) Resolve(_ context.Context, ref string) (modelprofile.Snapshot, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" || ref == "default" {
		return modelprofile.Snapshot{Name: "default", Provider: modelprofile.DefaultProvider, Revision: 1, CapturedAt: time.Now().UTC()}, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot, ok := r.profiles[ref]
	if !ok {
		return modelprofile.Snapshot{}, fmt.Errorf("unknown service stress model profile %q", ref)
	}
	return snapshot, nil
}

func (r *mutableServiceStressResolver) Set(ref string, snapshot modelprofile.Snapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.profiles[ref] = snapshot
}

func sessionByCwdForServiceStress(t *testing.T, bridge *Bridge, dir string) *Session {
	t.Helper()
	want := filepath.Clean(dir)
	for i := range bridge.reg.Sessions {
		if filepath.Clean(bridge.reg.Sessions[i].Cwd) == want {
			return &bridge.reg.Sessions[i]
		}
	}
	t.Fatalf("session for cwd %s not found: %#v", dir, bridge.reg.Sessions)
	return nil
}
