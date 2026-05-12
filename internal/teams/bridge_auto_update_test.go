package teams

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type fakeHelperAutoUpdater struct {
	decision   HelperAutoUpdateDecision
	applyCalls int
	applied    []HelperAutoUpdateCandidate
	checks     []HelperAutoUpdateCheck
	checkErr   error
	applyErr   error
}

func (f *fakeHelperAutoUpdater) Check(_ context.Context, check HelperAutoUpdateCheck) (HelperAutoUpdateDecision, error) {
	f.checks = append(f.checks, check)
	return f.decision, f.checkErr
}

func (f *fakeHelperAutoUpdater) Apply(_ context.Context, candidate HelperAutoUpdateCandidate) (HelperAutoUpdateApplyResult, error) {
	f.applyCalls++
	f.applied = append(f.applied, candidate)
	if f.applyErr != nil {
		return HelperAutoUpdateApplyResult{}, f.applyErr
	}
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

func TestBridgeHelperAutoUpdatePassesPrereleaseOptIn(t *testing.T) {
	_, bridge := newBridgeAutoUpdateTest(t)
	next := time.Date(2026, 5, 4, 0, 30, 0, 0, time.UTC)
	updater := &fakeHelperAutoUpdater{decision: HelperAutoUpdateDecision{NextCheckAt: next}}
	if err := bridge.maybeRunHelperAutoUpdate(context.Background(), BridgeOptions{
		HelperVersion:              "v1.2.3",
		HelperAutoUpdater:          updater,
		HelperAutoUpdatePrerelease: true,
	}); err != nil {
		t.Fatalf("maybeRunHelperAutoUpdate error: %v", err)
	}
	if len(updater.checks) != 1 || !updater.checks[0].IncludePrerelease || updater.checks[0].Manual {
		t.Fatalf("auto-update check = %#v, want prerelease opt-in and not manual", updater.checks)
	}
}

func TestBridgeControlHelperUpdatePrereleaseRunsManualUpdate(t *testing.T) {
	st := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	bridge.helperVersion = "v1.2.3"
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	updater := &fakeHelperAutoUpdater{decision: HelperAutoUpdateDecision{
		NextCheckAt: now.Add(30 * time.Minute),
		Candidate: &HelperAutoUpdateCandidate{
			TagName:     "v1.2.4-rc.1",
			Version:     "1.2.4-rc.1",
			Priority:    "p2",
			PublishedAt: now.Add(-time.Hour),
			EligibleAt:  now.Add(-time.Hour),
			Asset:       "codex-proxy_1.2.4-rc.1_linux_amd64",
		},
	}}
	bridge.helperAutoUpdater = updater
	restarted := false
	bridge.helperRestarter = func(context.Context) error {
		restarted = true
		return nil
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-update-pre"), "helper update prerelease"); err != nil {
		t.Fatalf("handleControlMessage update prerelease error: %v", err)
	}
	if len(updater.checks) != 1 || !updater.checks[0].IncludePrerelease || !updater.checks[0].Manual {
		t.Fatalf("manual update check = %#v, want prerelease manual", updater.checks)
	}
	if updater.applyCalls != 1 || !restarted {
		t.Fatalf("applyCalls=%d restarted=%v, want update apply and restart", updater.applyCalls, restarted)
	}
	if !sentPlainContains(*sent, "Helper update scheduled") || !sentPlainContains(*sent, "v1.2.4-rc.1") {
		t.Fatalf("update response missing target: %#v", *sent)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.AutoUpdate.LastInstalledTag != "v1.2.4-rc.1" {
		t.Fatalf("LastInstalledTag = %q, want v1.2.4-rc.1", state.AutoUpdate.LastInstalledTag)
	}
	if state.Upgrade == nil || len(state.Upgrade.NotificationTargets) != 1 || state.Upgrade.NotificationTargets[0].TeamsChatID != "control-chat" {
		t.Fatalf("manual helper update notification targets = %#v, want control chat target", state.Upgrade)
	}
	restartedBridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	if err := restartedBridge.queuePendingHelperRestartNotice(context.Background()); err != nil {
		t.Fatalf("queuePendingHelperRestartNotice after helper update error: %v", err)
	}
	if err := restartedBridge.flushPendingOutbox(context.Background(), "", ""); err != nil {
		t.Fatalf("flush helper update complete notice error: %v", err)
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Helper update scheduled", "Helper update completed", "v1.2.4-rc.1", "updated code"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("manual helper update messages missing %q in:\n%s", want, joined)
		}
	}
	if got := strings.Count(joined, "Helper update completed"); got != 1 {
		t.Fatalf("manual helper update completion count = %d, want 1 in:\n%s", got, joined)
	}
}

func TestBridgeHelperAutoUpdateQueuesPlainCompletionNotice(t *testing.T) {
	st := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	updater := &fakeHelperAutoUpdater{decision: HelperAutoUpdateDecision{
		NextCheckAt: now.Add(30 * time.Minute),
		Candidate: &HelperAutoUpdateCandidate{
			TagName: "v1.2.4",
			Version: "1.2.4",
		},
	}}
	if err := bridge.maybeRunHelperAutoUpdate(context.Background(), BridgeOptions{
		HelperVersion:     "v1.2.3",
		HelperAutoUpdater: updater,
		HelperRestarter:   func(context.Context) error { return nil },
	}); err != nil {
		t.Fatalf("maybeRunHelperAutoUpdate error: %v", err)
	}
	restartedBridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	if err := restartedBridge.queuePendingHelperRestartNotice(context.Background()); err != nil {
		t.Fatalf("queuePendingHelperRestartNotice after auto update error: %v", err)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	var completion teamstore.OutboxMessage
	for _, outbox := range state.OutboxMessages {
		if strings.Contains(outbox.Kind, "upgrade-complete") {
			completion = outbox
			break
		}
	}
	if completion.ID == "" {
		t.Fatalf("auto helper update did not queue completion notice: %#v", state.OutboxMessages)
	}
	if completion.NotificationKind != "" || completion.MentionOwner {
		t.Fatalf("auto helper update completion = %#v, want plain Teams message without card/mention", completion)
	}
	if err := restartedBridge.flushPendingOutbox(context.Background(), "", ""); err != nil {
		t.Fatalf("flush auto helper update complete notice error: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Helper update completed") || !strings.Contains(joined, "v1.2.4") {
		t.Fatalf("auto helper update completion message missing in:\n%s", joined)
	}
}

func TestBridgeControlHelperUpdateWaitsForActiveWork(t *testing.T) {
	st, bridge := newBridgeAutoUpdateTest(t)
	graph, sent := newBridgeTestGraph(t)
	bridge.graph = graph
	bridge.reg.ControlChatID = "control-chat"
	bridge.helperVersion = "v1.2.3"
	if err := st.Update(context.Background(), func(state *teamstore.State) error {
		state.Turns["turn-running"] = teamstore.Turn{ID: "turn-running", SessionID: "session-1", Status: teamstore.TurnStatusRunning, StartedAt: time.Now()}
		return nil
	}); err != nil {
		t.Fatalf("seed running turn: %v", err)
	}
	updater := &fakeHelperAutoUpdater{decision: HelperAutoUpdateDecision{
		NextCheckAt: time.Now().Add(30 * time.Minute),
		Candidate: &HelperAutoUpdateCandidate{
			TagName: "v1.2.4",
			Version: "1.2.4",
		},
	}}
	bridge.helperAutoUpdater = updater
	restarted := false
	bridge.helperRestarter = func(context.Context) error {
		restarted = true
		return nil
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-update-active"), "helper update now"); err != nil {
		t.Fatalf("handleControlMessage update now error: %v", err)
	}
	if updater.applyCalls != 0 || restarted {
		t.Fatalf("applyCalls=%d restarted=%v, want no apply/restart while active work drains", updater.applyCalls, restarted)
	}
	if !sentPlainContains(*sent, "Helper update scheduled") {
		t.Fatalf("update scheduled response missing: %#v", *sent)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if !state.ServiceControl.Draining || state.ServiceControl.Reason != teamstore.HelperUpgradeReason {
		t.Fatalf("ServiceControl = %#v, want helper upgrade drain", state.ServiceControl)
	}
	if state.AutoUpdate.CandidateTag != "v1.2.4" {
		t.Fatalf("CandidateTag = %q, want v1.2.4", state.AutoUpdate.CandidateTag)
	}
}

func TestBridgeHelperAutoUpdateApplyFailureBacksOffAndAbortsDrain(t *testing.T) {
	st, bridge := newBridgeAutoUpdateTest(t)
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	if _, err := st.SetDraining(context.Background(), "maintenance"); err != nil {
		t.Fatalf("seed existing drain: %v", err)
	}
	updater := &fakeHelperAutoUpdater{
		decision: HelperAutoUpdateDecision{
			NextCheckAt: now.Add(30 * time.Minute),
			Candidate: &HelperAutoUpdateCandidate{
				TagName:     "v1.2.4",
				Version:     "1.2.4",
				Priority:    "p0",
				PublishedAt: now.Add(-time.Minute),
				EligibleAt:  now.Add(-time.Minute),
				Asset:       "codex-proxy_1.2.4_linux_amd64",
			},
		},
		applyErr: errors.New("download failed"),
	}
	var restartCalls int
	err := bridge.maybeRunHelperAutoUpdate(context.Background(), BridgeOptions{
		HelperVersion:     "v1.2.3",
		HelperAutoUpdater: updater,
		HelperRestarter: func(context.Context) error {
			restartCalls++
			return nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "download failed") {
		t.Fatalf("maybeRunHelperAutoUpdate error = %v, want download failure", err)
	}
	if updater.applyCalls != 1 || restartCalls != 0 {
		t.Fatalf("applyCalls=%d restartCalls=%d, want apply once and no restart", updater.applyCalls, restartCalls)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.AutoUpdate.LastError != "download failed" || !state.AutoUpdate.BackoffUntil.After(time.Now()) {
		t.Fatalf("auto-update failure state = %#v, want error and backoff", state.AutoUpdate)
	}
	if state.AutoUpdate.CandidateTag != "v1.2.4" || state.AutoUpdate.CandidateVersion != "1.2.4" {
		t.Fatalf("candidate context was not retained after failure: %#v", state.AutoUpdate)
	}
	if state.Upgrade == nil || state.Upgrade.Phase != teamstore.UpgradePhaseAborted || !strings.Contains(state.Upgrade.AbortReason, "download failed") {
		t.Fatalf("upgrade = %#v, want aborted download failure", state.Upgrade)
	}
	if !state.ServiceControl.Draining || state.ServiceControl.Reason != "maintenance" {
		t.Fatalf("previous drain was not restored after failed auto-update: %#v", state.ServiceControl)
	}
	if state.AutoUpdate.LastInstalledTag != "" {
		t.Fatalf("LastInstalledTag = %q, want empty after failed apply", state.AutoUpdate.LastInstalledTag)
	}
}

func TestBridgeHelperAutoUpdateRestarterFailureKeepsInstalledState(t *testing.T) {
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
	err := bridge.maybeRunHelperAutoUpdate(context.Background(), BridgeOptions{
		HelperVersion:     "v1.2.3",
		HelperAutoUpdater: updater,
		HelperRestarter: func(context.Context) error {
			return errors.New("restart failed")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "restart failed") {
		t.Fatalf("maybeRunHelperAutoUpdate error = %v, want restart failure", err)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.AutoUpdate.LastInstalledTag != "v1.2.4" {
		t.Fatalf("LastInstalledTag = %q, want v1.2.4", state.AutoUpdate.LastInstalledTag)
	}
	if state.Upgrade == nil || state.Upgrade.Phase != teamstore.UpgradePhaseCompleted {
		t.Fatalf("upgrade = %#v, want completed before restart failure is reported", state.Upgrade)
	}
	if state.ServiceControl.Draining {
		t.Fatalf("ServiceControl still draining after completed install: %#v", state.ServiceControl)
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
