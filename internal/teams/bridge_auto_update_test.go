package teams

import (
	"context"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type fakeHelperAutoUpdater struct {
	decision   HelperAutoUpdateDecision
	applyCalls int
	applied    []HelperAutoUpdateCandidate
	err        error
}

func (f *fakeHelperAutoUpdater) Check(context.Context, HelperAutoUpdateCheck) (HelperAutoUpdateDecision, error) {
	return f.decision, f.err
}

func (f *fakeHelperAutoUpdater) Apply(_ context.Context, candidate HelperAutoUpdateCandidate) (HelperAutoUpdateApplyResult, error) {
	f.applyCalls++
	f.applied = append(f.applied, candidate)
	return HelperAutoUpdateApplyResult{Version: candidate.Version}, nil
}

func TestBridgeHelperAutoUpdateAppliesEligibleCandidate(t *testing.T) {
	st, bridge := newBridgeAutoUpdateTest(t)
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	updater := &fakeHelperAutoUpdater{decision: HelperAutoUpdateDecision{
		NextCheckAt: now.Add(30 * time.Minute),
		Candidate: &HelperAutoUpdateCandidate{
			TagName:     "v1.2.4",
			Version:     "1.2.4",
			Priority:    "p0",
			PublishedAt: now.Add(-time.Minute),
			EligibleAt:  now.Add(-time.Minute),
			Asset:       "codex-proxy_1.2.4_linux_amd64",
		},
	}}
	var restartCalls int
	err := bridge.maybeRunHelperAutoUpdate(context.Background(), BridgeOptions{
		HelperVersion:     "v1.2.3",
		HelperAutoUpdater: updater,
		HelperRestarter: func(context.Context) error {
			restartCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("maybeRunHelperAutoUpdate error: %v", err)
	}
	if updater.applyCalls != 1 || restartCalls != 1 {
		t.Fatalf("applyCalls=%d restartCalls=%d, want 1/1", updater.applyCalls, restartCalls)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.AutoUpdate.LastInstalledTag != "v1.2.4" {
		t.Fatalf("LastInstalledTag = %q, want v1.2.4", state.AutoUpdate.LastInstalledTag)
	}
	if state.Upgrade == nil || state.Upgrade.Phase != teamstore.UpgradePhaseCompleted {
		t.Fatalf("upgrade = %#v, want completed", state.Upgrade)
	}
	if state.ServiceControl.Draining {
		t.Fatalf("ServiceControl still draining after completed auto-update: %#v", state.ServiceControl)
	}
}

func TestBridgeHelperAutoUpdateWaitsForBlockingWorkThenAppliesPendingCandidate(t *testing.T) {
	st, bridge := newBridgeAutoUpdateTest(t)
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	if err := st.Update(context.Background(), func(state *teamstore.State) error {
		state.Turns["turn-running"] = teamstore.Turn{
			ID:        "turn-running",
			SessionID: "session-1",
			Status:    teamstore.TurnStatusRunning,
			StartedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed running turn: %v", err)
	}
	updater := &fakeHelperAutoUpdater{decision: HelperAutoUpdateDecision{
		NextCheckAt: now.Add(30 * time.Minute),
		Candidate: &HelperAutoUpdateCandidate{
			TagName:     "v1.2.4",
			Version:     "1.2.4",
			Priority:    "p0",
			PublishedAt: now.Add(-time.Minute),
			EligibleAt:  now.Add(-time.Minute),
			Asset:       "codex-proxy_1.2.4_linux_amd64",
		},
	}}
	if err := bridge.maybeRunHelperAutoUpdate(context.Background(), BridgeOptions{
		HelperVersion:     "v1.2.3",
		HelperAutoUpdater: updater,
	}); err != nil {
		t.Fatalf("first maybeRunHelperAutoUpdate error: %v", err)
	}
	if updater.applyCalls != 0 {
		t.Fatalf("applyCalls = %d, want 0 while running turn blocks", updater.applyCalls)
	}
	control, err := st.ReadControl(context.Background())
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if !control.Draining || control.Reason != teamstore.HelperUpgradeReason {
		t.Fatalf("control = %#v, want helper upgrade drain", control)
	}
	if err := st.Update(context.Background(), func(state *teamstore.State) error {
		turn := state.Turns["turn-running"]
		turn.Status = teamstore.TurnStatusCompleted
		turn.CompletedAt = now.Add(time.Second)
		state.Turns[turn.ID] = turn
		return nil
	}); err != nil {
		t.Fatalf("complete running turn: %v", err)
	}
	var restartCalls int
	if err := bridge.maybeRunHelperAutoUpdate(context.Background(), BridgeOptions{
		HelperVersion:     "v1.2.3",
		HelperAutoUpdater: updater,
		HelperRestarter: func(context.Context) error {
			restartCalls++
			return nil
		},
	}); err != nil {
		t.Fatalf("second maybeRunHelperAutoUpdate error: %v", err)
	}
	if updater.applyCalls != 1 || restartCalls != 1 {
		t.Fatalf("applyCalls=%d restartCalls=%d, want 1/1", updater.applyCalls, restartCalls)
	}
}

func TestBridgeHelperAutoUpdateRecordsCheckWithoutCandidate(t *testing.T) {
	st, bridge := newBridgeAutoUpdateTest(t)
	next := time.Date(2026, 5, 4, 0, 30, 0, 0, time.UTC)
	updater := &fakeHelperAutoUpdater{decision: HelperAutoUpdateDecision{NextCheckAt: next}}
	if err := bridge.maybeRunHelperAutoUpdate(context.Background(), BridgeOptions{
		HelperVersion:     "v1.2.3",
		HelperAutoUpdater: updater,
	}); err != nil {
		t.Fatalf("maybeRunHelperAutoUpdate error: %v", err)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.AutoUpdate.NextCheckAt.IsZero() {
		t.Fatal("NextCheckAt was not recorded")
	}
	if state.AutoUpdate.CandidateTag != "" {
		t.Fatalf("CandidateTag = %q, want empty", state.AutoUpdate.CandidateTag)
	}
}

func newBridgeAutoUpdateTest(t *testing.T) (*teamstore.Store, *Bridge) {
	t.Helper()
	st, err := teamstore.Open(t.TempDir() + "/state.json")
	if err != nil {
		t.Fatalf("Open store: %v", err)
	}
	bridge := &Bridge{store: st}
	return st, bridge
}
