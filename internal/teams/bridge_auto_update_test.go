package teams

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type fakeHelperAutoUpdater struct {
	decision    HelperAutoUpdateDecision
	applyCalls  int
	applied     []HelperAutoUpdateCandidate
	checks      []HelperAutoUpdateCheck
	checkErr    error
	applyErr    error
	applyResult HelperAutoUpdateApplyResult
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
	if strings.TrimSpace(f.applyResult.Version) == "" {
		f.applyResult.Version = candidate.Version
	}
	return f.applyResult, nil
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

func TestBridgeCompletedHelperUpgradeNoticeRecoversWithoutPendingFile(t *testing.T) {
	st := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	ctx := context.Background()

	req, err := st.BeginUpgrade(ctx, teamstore.HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if _, err := st.AddUpgradeNotificationTarget(ctx, req.ID, teamstore.UpgradeNotificationTarget{
		TurnID:      "control-message-1",
		TeamsChatID: "control-chat",
	}); err != nil {
		t.Fatalf("AddUpgradeNotificationTarget error: %v", err)
	}
	if _, err := st.RecordAutoUpdateInstalled(ctx, "v1.2.4-rc.1", time.Now()); err != nil {
		t.Fatalf("RecordAutoUpdateInstalled error: %v", err)
	}
	if _, err := st.CompleteUpgrade(ctx, req.ID, "v1.2.4-rc.1"); err != nil {
		t.Fatalf("CompleteUpgrade error: %v", err)
	}

	handled, err := bridge.queueCompletedHelperUpgradeNoticeIfNeeded(ctx)
	if err != nil {
		t.Fatalf("queueCompletedHelperUpgradeNoticeIfNeeded error: %v", err)
	}
	if !handled {
		t.Fatal("queueCompletedHelperUpgradeNoticeIfNeeded handled=false, want true")
	}
	if len(*sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(*sent))
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Helper update completed") || !strings.Contains(joined, "v1.2.4-rc.1") {
		t.Fatalf("completion notice missing expected text:\n%s", joined)
	}
	state, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.Upgrade == nil || state.Upgrade.CompletionNoticeID == "" || state.Upgrade.CompletionNoticeAt.IsZero() {
		t.Fatalf("upgrade completion notice was not marked durable: %#v", state.Upgrade)
	}
	outbox := state.OutboxMessages[state.Upgrade.CompletionNoticeID]
	if outbox.ID == "" || !outbox.MentionOwner || outbox.NotificationKind != "helper_upgrade_completed" {
		t.Fatalf("manual completion outbox = %#v, want owner mention and helper_upgrade_completed", outbox)
	}

	handled, err = bridge.queueCompletedHelperUpgradeNoticeIfNeeded(ctx)
	if err != nil {
		t.Fatalf("second queueCompletedHelperUpgradeNoticeIfNeeded error: %v", err)
	}
	if !handled {
		t.Fatal("second queueCompletedHelperUpgradeNoticeIfNeeded handled=false, want true")
	}
	if len(*sent) != 1 {
		t.Fatalf("completion notice sent %d times, want once", len(*sent))
	}
}

func TestBridgeCompletedHelperUpgradeNoticeAutoIsPlainAndDedupes(t *testing.T) {
	st := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	ctx := context.Background()

	req, err := st.BeginUpgrade(ctx, teamstore.HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if _, err := st.RecordAutoUpdateInstalled(ctx, "v1.2.4", time.Now()); err != nil {
		t.Fatalf("RecordAutoUpdateInstalled error: %v", err)
	}
	if _, err := st.CompleteUpgrade(ctx, req.ID, "v1.2.4"); err != nil {
		t.Fatalf("CompleteUpgrade error: %v", err)
	}

	for i := 0; i < 2; i++ {
		handled, err := bridge.queueCompletedHelperUpgradeNoticeIfNeeded(ctx)
		if err != nil {
			t.Fatalf("queueCompletedHelperUpgradeNoticeIfNeeded #%d error: %v", i+1, err)
		}
		if !handled {
			t.Fatalf("queueCompletedHelperUpgradeNoticeIfNeeded #%d handled=false, want true", i+1)
		}
	}
	if len(*sent) != 1 {
		t.Fatalf("auto completion notice sent %d times, want once", len(*sent))
	}
	state, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	outbox := state.OutboxMessages[state.Upgrade.CompletionNoticeID]
	if outbox.ID == "" {
		t.Fatalf("missing auto completion outbox: %#v", state.Upgrade)
	}
	if outbox.MentionOwner || outbox.NotificationKind != "" {
		t.Fatalf("auto completion outbox = %#v, want plain Teams message", outbox)
	}
}

func TestBridgePendingUpgradeNoticeDoesNotDuplicateDurableCompletionNotice(t *testing.T) {
	st := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	ctx := context.Background()

	req, err := st.BeginUpgrade(ctx, teamstore.HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if _, err := st.AddUpgradeNotificationTarget(ctx, req.ID, teamstore.UpgradeNotificationTarget{
		TurnID:      "control-message-1",
		TeamsChatID: "control-chat",
	}); err != nil {
		t.Fatalf("AddUpgradeNotificationTarget error: %v", err)
	}
	if _, err := st.CompleteUpgrade(ctx, req.ID, "v1.2.4"); err != nil {
		t.Fatalf("CompleteUpgrade error: %v", err)
	}
	if handled, err := bridge.queueCompletedHelperUpgradeNoticeIfNeeded(ctx); err != nil {
		t.Fatalf("queueCompletedHelperUpgradeNoticeIfNeeded error: %v", err)
	} else if !handled {
		t.Fatal("queueCompletedHelperUpgradeNoticeIfNeeded handled=false, want true")
	}
	if err := bridge.writePendingHelperUpgradeNotice("control-chat", "control-message-1", "v1.2.4", true); err != nil {
		t.Fatalf("writePendingHelperUpgradeNotice error: %v", err)
	}
	if err := bridge.queuePendingHelperRestartNotice(ctx); err != nil {
		t.Fatalf("queuePendingHelperRestartNotice error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("durable plus pending completion sent %d messages, want one", len(*sent))
	}
}

func TestBridgePendingUpgradeNoticeRepairsMissingDurableCompletionOutbox(t *testing.T) {
	st := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	ctx := context.Background()

	req, err := st.BeginUpgrade(ctx, teamstore.HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if _, err := st.CompleteUpgrade(ctx, req.ID, "v1.2.4"); err != nil {
		t.Fatalf("CompleteUpgrade error: %v", err)
	}
	const durableID = "outbox:control:helper-upgrade-complete:missing-durable"
	if _, err := st.MarkUpgradeCompletionNoticeQueued(ctx, req.ID, durableID); err != nil {
		t.Fatalf("MarkUpgradeCompletionNoticeQueued error: %v", err)
	}
	if err := bridge.writePendingHelperUpgradeNotice("control-chat", "control-message-1", "v1.2.4", true); err != nil {
		t.Fatalf("writePendingHelperUpgradeNotice error: %v", err)
	}

	if err := bridge.queuePendingHelperRestartNotice(ctx); err != nil {
		t.Fatalf("queuePendingHelperRestartNotice error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("pending repair sent %d completion messages, want one", len(*sent))
	}
	state, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if outbox := state.OutboxMessages[durableID]; outbox.ID != durableID {
		t.Fatalf("missing durable outbox was not repaired: %#v", state.OutboxMessages)
	}
	for id := range state.OutboxMessages {
		if strings.Contains(id, shortStableID("control-message-1")) {
			t.Fatalf("legacy pending completion outbox should not be created when durable record can be repaired: %s", id)
		}
	}
}

func TestBridgeCompletedHelperUpgradeNoticeAdoptsLegacyOutbox(t *testing.T) {
	st := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	ctx := context.Background()

	req, err := st.BeginUpgrade(ctx, teamstore.HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	if _, err := st.CompleteUpgrade(ctx, req.ID, "v1.2.4"); err != nil {
		t.Fatalf("CompleteUpgrade error: %v", err)
	}
	legacyID := "outbox:control:helper-upgrade-complete:legacy"
	if _, _, err := st.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          legacyID,
		TeamsChatID: "control-chat",
		Kind:        "control-upgrade-complete",
		Body:        helperLifecycleCompletedNoticeBody(helperRestartNoticeActionUpgrade, "v1.2.4"),
		Status:      teamstore.OutboxStatusSent,
	}); err != nil {
		t.Fatalf("QueueOutbox legacy completion error: %v", err)
	}

	handled, err := bridge.queueCompletedHelperUpgradeNoticeIfNeeded(ctx)
	if err != nil {
		t.Fatalf("queueCompletedHelperUpgradeNoticeIfNeeded error: %v", err)
	}
	if !handled {
		t.Fatal("queueCompletedHelperUpgradeNoticeIfNeeded handled=false, want true")
	}
	if len(*sent) != 0 {
		t.Fatalf("legacy sent completion should not be resent, sent=%#v", *sent)
	}
	state, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.Upgrade == nil || state.Upgrade.CompletionNoticeID != legacyID {
		t.Fatalf("CompletionNoticeID = %#v, want legacy id %q", state.Upgrade, legacyID)
	}
	var completionCount int
	for _, outbox := range state.OutboxMessages {
		if strings.EqualFold(strings.TrimSpace(outbox.Kind), "control-upgrade-complete") {
			completionCount++
		}
	}
	if completionCount != 1 {
		t.Fatalf("completion outbox count = %d, want 1", completionCount)
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

func TestBridgeHelperAutoUpdatePendingReplacementWaitsForVerifiedVersion(t *testing.T) {
	st := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	now := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	updater := &fakeHelperAutoUpdater{
		decision: HelperAutoUpdateDecision{
			NextCheckAt: now.Add(30 * time.Minute),
			Candidate: &HelperAutoUpdateCandidate{
				TagName: "v1.2.4",
				Version: "1.2.4",
			},
		},
		applyResult: HelperAutoUpdateApplyResult{
			Version:            "1.2.4",
			InstallPath:        "C:\\Users\\test\\.local\\bin\\codex-proxy.exe",
			RestartRequired:    true,
			PendingReplacePath: "C:\\Users\\test\\.local\\bin\\.codex-proxy_1.2.4.exe.tmp",
		},
	}
	bridge.helperAutoUpdater = updater
	var restartCalls int
	var pendingRestartPath string
	var pendingRestartInstallPath string
	bridge.helperRestarter = func(context.Context) error {
		restartCalls++
		return nil
	}
	bridge.helperPendingRestarter = func(_ context.Context, path string, installPath string) error {
		pendingRestartPath = path
		pendingRestartInstallPath = installPath
		return nil
	}

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessage("control-update-pending"), "helper update now"); err != nil {
		t.Fatalf("handleControlMessage update now error: %v", err)
	}
	if updater.applyCalls != 1 || restartCalls != 0 {
		t.Fatalf("applyCalls=%d restartCalls=%d, want 1/0", updater.applyCalls, restartCalls)
	}
	if pendingRestartPath != updater.applyResult.PendingReplacePath {
		t.Fatalf("pending restart path = %q, want %q", pendingRestartPath, updater.applyResult.PendingReplacePath)
	}
	if pendingRestartInstallPath != updater.applyResult.InstallPath {
		t.Fatalf("pending restart install path = %q, want %q", pendingRestartInstallPath, updater.applyResult.InstallPath)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if state.AutoUpdate.LastInstalledTag != "" {
		t.Fatalf("LastInstalledTag = %q, want empty before verified restart", state.AutoUpdate.LastInstalledTag)
	}
	if state.Upgrade == nil || state.Upgrade.Phase != teamstore.UpgradePhaseAborted || !strings.Contains(state.Upgrade.AbortReason, "replacement is pending") {
		t.Fatalf("upgrade = %#v, want aborted pending replacement", state.Upgrade)
	}

	restartedOld := newBridgeTestBridge(graph, st, &recordingExecutor{})
	restartedOld.helperVersion = "v1.2.3"
	if err := restartedOld.queuePendingHelperRestartNotice(context.Background()); err != nil {
		t.Fatalf("queuePendingHelperRestartNotice with old version error: %v", err)
	}
	if err := restartedOld.flushPendingOutbox(context.Background(), "", ""); err != nil {
		t.Fatalf("flush old pending outbox: %v", err)
	}
	if strings.Contains(sentPlainJoined(*sent), "Helper update completed") {
		t.Fatalf("completion notice sent before helper version matched:\n%s", sentPlainJoined(*sent))
	}
	state, err = st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after old restart error: %v", err)
	}
	if state.AutoUpdate.LastInstalledTag != "" {
		t.Fatalf("LastInstalledTag after old restart = %q, want empty", state.AutoUpdate.LastInstalledTag)
	}

	restartedNew := newBridgeTestBridge(graph, st, &recordingExecutor{})
	restartedNew.helperVersion = "v1.2.4 (abc123) 2026-05-04T00:00:00Z"
	if err := restartedNew.queuePendingHelperRestartNotice(context.Background()); err != nil {
		t.Fatalf("queuePendingHelperRestartNotice with new version error: %v", err)
	}
	if err := restartedNew.flushPendingOutbox(context.Background(), "", ""); err != nil {
		t.Fatalf("flush new pending outbox: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Helper update completed") || !strings.Contains(joined, "v1.2.4") {
		t.Fatalf("completion notice missing after verified restart:\n%s", joined)
	}
	if got := strings.Count(joined, "Helper update completed"); got != 1 {
		t.Fatalf("completion notice count = %d, want 1 in:\n%s", got, joined)
	}
	state, err = st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after new restart error: %v", err)
	}
	if state.AutoUpdate.LastInstalledTag != "v1.2.4" {
		t.Fatalf("LastInstalledTag after verified restart = %q, want v1.2.4", state.AutoUpdate.LastInstalledTag)
	}
	if state.Upgrade == nil || state.Upgrade.Phase != teamstore.UpgradePhaseCompleted || state.Upgrade.InstalledTag != "v1.2.4" {
		t.Fatalf("upgrade after verified restart = %#v, want completed v1.2.4", state.Upgrade)
	}
	if state.Upgrade.CompletionNoticeID == "" || state.Upgrade.CompletionNoticeAt.IsZero() {
		t.Fatalf("upgrade completion notice not durable after verified restart: %#v", state.Upgrade)
	}
}

func TestBridgeHelperAutoUpdatePendingReplacementFailureNotifiesOnce(t *testing.T) {
	st := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, st, &recordingExecutor{})
	tmp := t.TempDir()
	pendingPath := filepath.Join(tmp, ".codex-proxy_1.2.4_windows_amd64.exe.123")
	installPath := filepath.Join(tmp, "codex-proxy.exe")
	statusJSON := `{"version":1,"status":"failed","message":"move attempt 240 failed: file is locked","source":"` + strings.ReplaceAll(pendingPath, `\`, `\\`) + `","dest":"` + strings.ReplaceAll(installPath, `\`, `\\`) + `","want":"1.2.4","updated_at":"2026-05-04T00:00:00Z"}`
	statusData := append([]byte{0xef, 0xbb, 0xbf}, []byte(statusJSON)...)
	if err := os.WriteFile(helperActivationStatusPath(pendingPath), statusData, 0o600); err != nil {
		t.Fatalf("write activation status: %v", err)
	}
	if err := bridge.writePendingHelperUpgradeNoticeWithReplacement("control-chat", "cmd-1", "v1.2.4", true, pendingPath, installPath); err != nil {
		t.Fatalf("write pending upgrade notice: %v", err)
	}

	restartedOld := newBridgeTestBridge(graph, st, &recordingExecutor{})
	restartedOld.helperVersion = "v1.2.3"
	if err := restartedOld.queuePendingHelperRestartNotice(context.Background()); err != nil {
		t.Fatalf("queuePendingHelperRestartNotice with failed activation error: %v", err)
	}
	if err := restartedOld.flushPendingOutbox(context.Background(), "", ""); err != nil {
		t.Fatalf("flush pending failure outbox: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Helper update activation failed") || !strings.Contains(joined, "move attempt 240 failed") || !strings.Contains(joined, "v1.2.4") {
		t.Fatalf("activation failure notice missing details:\n%s", joined)
	}
	if got := strings.Count(joined, "Helper update activation failed"); got != 1 {
		t.Fatalf("activation failure notice count = %d, want 1 in:\n%s", got, joined)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after failure notice: %v", err)
	}
	var found bool
	for _, outbox := range state.OutboxMessages {
		if outbox.Kind != "failed-helper-upgrade-activation" {
			continue
		}
		found = true
		if !outbox.MentionOwner || outbox.NotificationKind != "needs_attention" {
			t.Fatalf("failure outbox notification fields = %#v, want owner attention", outbox)
		}
	}
	if !found {
		t.Fatalf("failure outbox not found in state: %#v", state.OutboxMessages)
	}

	if err := restartedOld.queuePendingHelperRestartNotice(context.Background()); err != nil {
		t.Fatalf("second queuePendingHelperRestartNotice with failed activation error: %v", err)
	}
	if err := restartedOld.flushPendingOutbox(context.Background(), "", ""); err != nil {
		t.Fatalf("second flush pending failure outbox: %v", err)
	}
	if got := strings.Count(sentPlainJoined(*sent), "Helper update activation failed"); got != 1 {
		t.Fatalf("activation failure notice duplicated, count = %d in:\n%s", got, sentPlainJoined(*sent))
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

func TestBridgeHelperUpgradeCompletionDoesNotUseLastAttemptAsInstalledVersion(t *testing.T) {
	st, bridge := newBridgeAutoUpdateTest(t)
	bridge.helperVersion = "v1.2.4"
	ctx := context.Background()
	req, err := st.BeginUpgrade(ctx, teamstore.HelperUpgradeReason, time.Minute)
	if err != nil {
		t.Fatalf("BeginUpgrade error: %v", err)
	}
	req, err = st.AddUpgradeNotificationTarget(ctx, req.ID, teamstore.UpgradeNotificationTarget{
		TeamsChatID: "control-chat",
		TurnID:      "command-1",
	})
	if err != nil {
		t.Fatalf("AddUpgradeNotificationTarget error: %v", err)
	}
	if _, err := st.RecordAutoUpdateAttempt(ctx, "v1.2.4", time.Now()); err != nil {
		t.Fatalf("RecordAutoUpdateAttempt error: %v", err)
	}
	req, err = st.CompleteUpgrade(ctx, req.ID)
	if err != nil {
		t.Fatalf("CompleteUpgrade error: %v", err)
	}
	state, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if msg, ok := bridge.completedHelperUpgradeNoticeMessage(state, req); ok {
		t.Fatalf("completion message should not be generated from last attempt tag: %#v", msg)
	}
	if _, err := st.RecordAutoUpdateInstalled(ctx, "v1.2.4", time.Now()); err != nil {
		t.Fatalf("RecordAutoUpdateInstalled error: %v", err)
	}
	state, err = st.Load(ctx)
	if err != nil {
		t.Fatalf("Load after installed error: %v", err)
	}
	if msg, ok := bridge.completedHelperUpgradeNoticeMessage(state, req); !ok || !strings.Contains(msg.Body, "v1.2.4") {
		t.Fatalf("completion message after verified install = %#v ok=%v, want v1.2.4", msg, ok)
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
