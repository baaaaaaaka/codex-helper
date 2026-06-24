package teams

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams/delegation"
	"github.com/baaaaaaaka/codex-helper/internal/teams/machineregistry"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBridgeMachineRegistryPublisherPatchesAndDrains(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		HelperVersion:            "v-test",
		MachineRegistryGraph:     graph,
		MachineRegistryCachePath: filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryInterval:  5 * time.Minute,
		MachineRegistryTTL:       15 * time.Minute,
		MachineRegistryNow:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	if graph.createCalls != 2 || graph.sendCount != 1 || graph.patchCount != 0 {
		t.Fatalf("initial calls create=%d send=%d patch=%d", graph.createCalls, graph.sendCount, graph.patchCount)
	}
	card := graph.latestCard(t)
	if card.MachineID != "machine-a" || !card.Accepting || card.Draining || card.CXPVersion != "v-test" {
		t.Fatalf("initial card = %#v", card)
	}
	if card.InboxRef == "" || card.InboxGeneration == "" {
		t.Fatalf("initial card missing inbox locator: %#v", card)
	}
	if card.InstanceID == "" || card.HostLabel == "" ||
		card.CapabilityFingerprint != machineregistry.CapabilityFingerprint([]string{"cxp", "codex", "teams-helper", "teams-registry"}) ||
		len(card.ProtocolVersions) != 1 || card.ProtocolVersions[0] != "cxp-delegation-v1" {
		t.Fatalf("initial card missing protocol metadata: %#v", card)
	}
	slot := publisher.cache.SlotMessageID
	if slot == "" {
		t.Fatal("publisher cache did not retain slot message id")
	}

	now = now.Add(5 * time.Minute)
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("patch publish: %v", err)
	}
	if graph.sendCount != 1 || graph.patchCount != 1 || publisher.cache.SlotMessageID != slot {
		t.Fatalf("patch calls send=%d patch=%d slot=%q", graph.sendCount, graph.patchCount, publisher.cache.SlotMessageID)
	}

	now = now.Add(time.Minute)
	if err := publisher.publish(context.Background(), true); err != nil {
		t.Fatalf("drain publish: %v", err)
	}
	card = graph.latestCard(t)
	if card.Accepting || !card.Draining || card.ExpiresAt == "" {
		t.Fatalf("drain card = %#v", card)
	}
	if graph.sendCount != 1 || graph.patchCount != 2 {
		t.Fatalf("drain should patch same slot: send=%d patch=%d", graph.sendCount, graph.patchCount)
	}
}

func TestBridgeMachineRegistryPublisherFailureBackoffDoesNotAppend(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:     graph,
		MachineRegistryCachePath: filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryInterval:  5 * time.Minute,
		MachineRegistryNow:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	graph.patchErr = &machineregistry.StatusError{StatusCode: 429, Err: errors.New("rate limited")}
	var logged []string
	backoff := time.Duration(0)
	delay := publisher.publishDelay(context.Background(), false, &backoff, func(err error) {
		logged = append(logged, err.Error())
	})
	if delay < 150*time.Second || delay > 5*time.Minute || backoff != 5*time.Minute {
		t.Fatalf("delay=%v backoff=%v, want random jitter in [2.5m, 5m] with stored 5m backoff", delay, backoff)
	}
	if graph.sendCount != 1 || graph.patchCount != 1 {
		t.Fatalf("transient patch failure must not append: send=%d patch=%d", graph.sendCount, graph.patchCount)
	}
	if len(logged) != 1 || !strings.Contains(logged[0], "rate limited") {
		t.Fatalf("logged = %#v", logged)
	}
	if next := machineRegistryNextBackoff(backoff, 5*time.Minute); next != 10*time.Minute {
		t.Fatalf("next backoff = %v, want 10m", next)
	}
}

func TestBridgeMachineRegistryPublisherFailureBackoffRespectsRetryAfter(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:     graph,
		MachineRegistryCachePath: filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryInterval:  5 * time.Minute,
		MachineRegistryNow:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("initial publish: %v", err)
	}
	retryAfter := 2 * time.Minute
	graph.patchErr = &machineregistry.StatusError{StatusCode: 429, RetryAfter: retryAfter, Err: errors.New("rate limited")}
	backoff := 5 * time.Minute
	delay := publisher.publishDelay(context.Background(), false, &backoff, nil)
	if delay < retryAfter || delay > retryAfter+12*time.Second {
		t.Fatalf("delay=%v, want Retry-After respected with random positive jitter in [%v, %v]", delay, retryAfter, retryAfter+12*time.Second)
	}
	if backoff != 0 {
		t.Fatalf("backoff=%v, want Retry-After to reset local exponential backoff", backoff)
	}
}

func TestMachineRegistryPublishRetryDelay(t *testing.T) {
	tests := []struct {
		name            string
		err             error
		current         time.Duration
		interval        time.Duration
		minDelay        time.Duration
		maxDelay        time.Duration
		wantNextBackoff time.Duration
	}{
		{
			name:            "registry status retry after",
			err:             &machineregistry.StatusError{StatusCode: 429, RetryAfter: 2 * time.Minute, Err: errors.New("rate limited")},
			current:         5 * time.Minute,
			interval:        5 * time.Minute,
			minDelay:        2 * time.Minute,
			maxDelay:        2*time.Minute + 12*time.Second,
			wantNextBackoff: 0,
		},
		{
			name:            "direct graph status retry after",
			err:             &GraphStatusError{StatusCode: 429, RetryAfter: 3 * time.Minute},
			current:         10 * time.Minute,
			interval:        5 * time.Minute,
			minDelay:        3 * time.Minute,
			maxDelay:        3*time.Minute + 18*time.Second,
			wantNextBackoff: 0,
		},
		{
			name:            "retry after jitter capped",
			err:             &machineregistry.StatusError{StatusCode: 429, RetryAfter: 10 * time.Minute},
			current:         10 * time.Minute,
			interval:        5 * time.Minute,
			minDelay:        10 * time.Minute,
			maxDelay:        10*time.Minute + machineRegistryRetryAfterJitterMax,
			wantNextBackoff: 0,
		},
		{
			name:            "429 without retry after keeps exponential backoff",
			err:             &machineregistry.StatusError{StatusCode: 429, Err: errors.New("rate limited")},
			current:         0,
			interval:        5 * time.Minute,
			minDelay:        150 * time.Second,
			maxDelay:        5 * time.Minute,
			wantNextBackoff: 5 * time.Minute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay, nextBackoff := machineRegistryPublishRetryDelay(tt.err, tt.current, tt.interval)
			if delay < tt.minDelay || delay > tt.maxDelay {
				t.Fatalf("delay=%v, want in [%v, %v]", delay, tt.minDelay, tt.maxDelay)
			}
			if nextBackoff != tt.wantNextBackoff {
				t.Fatalf("nextBackoff=%v, want %v", nextBackoff, tt.wantNextBackoff)
			}
		})
	}
}

func TestBridgeMachineRegistryHeartbeatDisabledForOnce(t *testing.T) {
	bridge := testBridgeForMachineRegistry()
	done := bridge.startMachineRegistryHeartbeat(context.Background(), BridgeOptions{
		Once:                   true,
		MachineRegistryEnabled: true,
		MachineRegistryGraph:   newFakeBridgeMachineRegistryGraph(),
	})
	if done != nil {
		t.Fatal("machine registry heartbeat should stay disabled for --once")
	}
}

func TestBridgeMachineRegistryHeartbeatDoesNotWriteDelegationThreadStore(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	statePath := filepath.Join(t.TempDir(), "delegation-worker.json")
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       newFakeBridgeMachineRegistryGraph(),
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryInterval:    5 * time.Minute,
		MachineRegistryTTL:         15 * time.Minute,
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: statePath,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delegation state file err = %v, want not created by heartbeat", err)
	}
}

func TestBridgeMachineDelegationWorkerClaimsExecutesAndPublishesResult(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	executor := &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "remote answer"}}
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryInterval:    5 * time.Minute,
		MachineRegistryTTL:         15 * time.Minute,
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: filepath.Join(t.TempDir(), "delegation-worker.json"),
		Interval:                   5 * time.Second,
		Executor:                   executor,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	req, err := delegation.NewRequestRecord(
		"session-a",
		"turn-1",
		"",
		[]string{"machine-source"},
		"machine-a",
		delegation.TaskSpec{Title: "remote test", Objective: "Inspect the remote fixture."},
		now,
	)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.InboxRef = publisher.cache.InboxExternalID
	req.InboxGeneration = publisher.cache.InboxGeneration
	if _, err := graph.SendHTML(context.Background(), publisher.cache.InboxChatID, delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	if err := publisher.pollDelegationInbox(context.Background()); err != nil {
		t.Fatalf("poll inbox: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var records []delegation.Record
	for time.Now().Before(deadline) {
		messages, _ := graph.ListMessages(context.Background(), publisher.cache.InboxChatID, 50)
		records = delegation.ObserveRecords(machineDelegationMessages(messages))
		if delegation.Reduce(delegation.RecordsForID(records, req.DelegationID), now).Status == delegation.StateComplete {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	state := delegation.Reduce(delegation.RecordsForID(records, req.DelegationID), now)
	if state.Status != delegation.StateComplete || state.Terminal == nil || state.Terminal.Body != "remote answer" {
		t.Fatalf("state = %#v records=%#v, want complete remote answer", state, records)
	}
	if !hasDelegationStatus(records, req.DelegationID, delegation.StateRunning) {
		t.Fatalf("records=%#v, want running status before terminal result", records)
	}
	if prompt := executor.promptText(); prompt == "" || !strings.Contains(prompt, "Inspect the remote fixture.") {
		t.Fatalf("executor prompt = %q, want delegated objective", prompt)
	}
	if state.WinningClaim == nil || state.WinningClaim.MachineID != "machine-a" {
		t.Fatalf("winning claim = %#v", state.WinningClaim)
	}
}

func TestBridgeMachineDelegationWorkerUsesRemoteThreadSessionID(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	executor := &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "thread answer"}}
	bridge := testBridgeForMachineRegistry()
	statePath := filepath.Join(t.TempDir(), "delegation-worker.json")
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: statePath,
		Executor:                   executor,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	req, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Title: "reuse thread", Objective: "Continue the remote analysis."}, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.InboxRef = publisher.cache.InboxExternalID
	req.InboxGeneration = publisher.cache.InboxGeneration
	req.RemoteThreadID = "rth-remote-thread"
	req.ThreadPolicy = delegation.ThreadPolicyReuse
	req.ThreadGeneration = "thread-gen"
	if _, err := graph.SendHTML(context.Background(), publisher.cache.InboxChatID, delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	if err := publisher.pollDelegationInbox(context.Background()); err != nil {
		t.Fatalf("poll inbox: %v", err)
	}
	records := waitForBridgeDelegationStatus(t, graph, publisher.cache.InboxChatID, req.DelegationID, delegation.StateComplete)
	state := delegation.Reduce(delegation.RecordsForID(records, req.DelegationID), now)
	if state.Terminal == nil || state.Terminal.ThreadUpdate == nil || state.Terminal.ThreadUpdate.LastResultSummary != "thread answer" {
		t.Fatalf("terminal = %#v, want thread update", state.Terminal)
	}
	if executor.sessionID() != "rth-remote-thread" {
		t.Fatalf("executor session id = %q, want remote thread id", executor.sessionID())
	}
	if prompt := executor.promptText(); !strings.Contains(prompt, "Thread policy: reuse") || !strings.Contains(prompt, delegationReuseRejectedPrefix) {
		t.Fatalf("prompt = %q, want reuse instructions", prompt)
	}
	if !waitPublishersIdle(time.Second, publisher) {
		t.Fatal("publisher did not finish delegation goroutine")
	}
	store, err := delegation.LoadStore(statePath)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	thread, ok := store.RemoteThreadForID("rth-remote-thread")
	if ok {
		t.Fatalf("thread = %#v, want completed worker remote thread removed", thread)
	}
}

func TestBridgeMachineDelegationWorkerUsesNewRemoteThreadSessionID(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	executor := &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "new thread answer"}}
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: filepath.Join(t.TempDir(), "delegation-worker.json"),
		Executor:                   executor,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	req, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Title: "new thread", Objective: "Start a new remote analysis."}, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.InboxRef = publisher.cache.InboxExternalID
	req.InboxGeneration = publisher.cache.InboxGeneration
	req.RemoteThreadID = "rth-new-thread"
	req.ThreadPolicy = delegation.ThreadPolicyNew
	req.ThreadGeneration = "thread-gen-new"
	if _, err := graph.SendHTML(context.Background(), publisher.cache.InboxChatID, delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	if err := publisher.pollDelegationInbox(context.Background()); err != nil {
		t.Fatalf("poll inbox: %v", err)
	}
	_ = waitForBridgeDelegationStatus(t, graph, publisher.cache.InboxChatID, req.DelegationID, delegation.StateComplete)
	if executor.sessionID() != "rth-new-thread" {
		t.Fatalf("executor session id = %q, want new remote thread id", executor.sessionID())
	}
}

func TestBridgeMachineDelegationWorkerPublishesReuseRejected(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	executor := &fakeMachineDelegationExecutor{result: ExecutionResult{Text: delegationReuseRejectedPrefix + " wrong remote context"}}
	bridge := testBridgeForMachineRegistry()
	statePath := filepath.Join(t.TempDir(), "delegation-worker.json")
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: statePath,
		Executor:                   executor,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	req, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Objective: "Try to reuse an old context."}, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.InboxRef = publisher.cache.InboxExternalID
	req.InboxGeneration = publisher.cache.InboxGeneration
	req.RemoteThreadID = "rth-wrong-context"
	req.ThreadPolicy = delegation.ThreadPolicyReuse
	req.ThreadGeneration = "thread-gen"
	if _, err := graph.SendHTML(context.Background(), publisher.cache.InboxChatID, delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	if err := publisher.pollDelegationInbox(context.Background()); err != nil {
		t.Fatalf("poll inbox: %v", err)
	}
	records := waitForBridgeDelegationStatus(t, graph, publisher.cache.InboxChatID, req.DelegationID, delegation.StateReuseRejected)
	state := delegation.Reduce(delegation.RecordsForID(records, req.DelegationID), now)
	if state.Status != delegation.StateReuseRejected || state.Terminal == nil || state.Terminal.Body != "wrong remote context" {
		t.Fatalf("state = %#v, want reuse_rejected body", state)
	}
	if !waitPublishersIdle(time.Second, publisher) {
		t.Fatal("publisher did not finish delegation goroutine")
	}
	store, err := delegation.LoadStore(statePath)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	fence, ok := store.ExecutionForID(req.DelegationID)
	if !ok || fence.Status != delegation.StateReuseRejected {
		t.Fatalf("fence = %#v ok=%v, want reuse_rejected", fence, ok)
	}
	thread, ok := store.RemoteThreadForID("rth-wrong-context")
	if ok {
		t.Fatalf("thread = %#v, want rejected worker remote thread removed", thread)
	}
}

func TestBridgeMachineDelegationWorkerPrunesOnlyWorkerIdleRemoteThreads(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       newFakeBridgeMachineRegistryGraph(),
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: filepath.Join(t.TempDir(), "delegation-worker.json"),
		Executor:                   &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "unused"}},
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	req, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Objective: "route preservation"}, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.RemoteThreadID = "rth-route"
	req.ThreadPolicy = delegation.ThreadPolicyReuse
	req.ThreadGeneration = "thread-gen"

	routeStore := delegation.Store{}
	routeStore.UpsertRoute(delegation.Route{DelegationID: req.DelegationID, MachineID: "machine-a", InboxRef: "inbox-ref"})
	routeStore.UpsertRemoteThread(delegation.RemoteThread{
		ThreadID:           req.RemoteThreadID,
		MachineID:          "machine-a",
		State:              delegation.RemoteThreadStateActive,
		ActiveDelegationID: req.DelegationID,
		Generation:         "thread-gen",
	})
	publisher.clearDelegationWorkerRemoteThread(&routeStore, req)
	thread, ok := routeStore.RemoteThreadForID(req.RemoteThreadID)
	if !ok || thread.State != delegation.RemoteThreadStateIdle || thread.ActiveDelegationID != "" {
		t.Fatalf("route-store thread = %#v ok=%v, want preserved idle thread", thread, ok)
	}

	workerStore := delegation.Store{}
	workerStore.UpsertRemoteThread(delegation.RemoteThread{
		ThreadID:  "rth-idle",
		MachineID: "machine-a",
		State:     delegation.RemoteThreadStateIdle,
	})
	workerStore.UpsertRemoteThread(delegation.RemoteThread{
		ThreadID:           "rth-active",
		MachineID:          "machine-a",
		State:              delegation.RemoteThreadStateActive,
		ActiveDelegationID: "del-active",
	})
	for i := 0; i < delegationWorkerTerminalExecutionLimit+5; i++ {
		workerStore.UpsertExecution(delegation.ExecutionFence{
			DelegationID: fmt.Sprintf("del-terminal-%03d", i),
			Status:       delegation.StateComplete,
			UpdatedAt:    now.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
		})
		workerStore.UpsertOutbox(delegation.OutboxRecord{
			RecordID:     fmt.Sprintf("record-visible-%03d", i),
			DelegationID: fmt.Sprintf("del-terminal-%03d", i),
			Status:       delegation.OutboxVisible,
			UpdatedAt:    now.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
		})
	}
	workerStore.UpsertExecution(delegation.ExecutionFence{DelegationID: "del-running", Status: delegation.StateRunning})
	workerStore.UpsertOutbox(delegation.OutboxRecord{RecordID: "record-pending", Status: delegation.OutboxPending})
	publisher.pruneDelegationWorkerState(&workerStore)
	if _, ok := workerStore.RemoteThreadForID("rth-idle"); ok {
		t.Fatalf("worker idle thread was not pruned: %#v", workerStore.RemoteThreads)
	}
	if _, ok := workerStore.RemoteThreadForID("rth-active"); !ok {
		t.Fatalf("worker active thread was pruned: %#v", workerStore.RemoteThreads)
	}
	if _, ok := workerStore.ExecutionForID("del-running"); !ok {
		t.Fatalf("running execution was pruned: %#v", workerStore.Executions)
	}
	if _, ok := workerStore.ExecutionForID("del-terminal-000"); ok {
		t.Fatalf("old terminal execution was not pruned: %#v", workerStore.Executions)
	}
	if _, ok := workerStore.ExecutionForID(fmt.Sprintf("del-terminal-%03d", delegationWorkerTerminalExecutionLimit+4)); !ok {
		t.Fatalf("new terminal execution was pruned: %#v", workerStore.Executions)
	}
	if _, ok := workerStore.Outbox["record-pending"]; !ok {
		t.Fatalf("pending outbox was pruned: %#v", workerStore.Outbox)
	}
	if _, ok := workerStore.Outbox["record-visible-000"]; ok {
		t.Fatalf("old visible outbox was not pruned: %#v", workerStore.Outbox)
	}
	if _, ok := workerStore.Outbox[fmt.Sprintf("record-visible-%03d", delegationWorkerTerminalOutboxLimit+4)]; !ok {
		t.Fatalf("new visible outbox was pruned: %#v", workerStore.Outbox)
	}
}

func TestBridgeMachineDelegationRemoteThreadFenceRejectsConcurrentReuse(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	statePath := filepath.Join(t.TempDir(), "delegation-worker.json")
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       newFakeBridgeMachineRegistryGraph(),
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: statePath,
		Executor:                   &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "unused"}},
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	firstReq, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Objective: "first shared thread task"}, now)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	secondReq, err := delegation.NewRequestRecord("session-a", "turn-2", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Objective: "second shared thread task"}, now)
	if err != nil {
		t.Fatalf("second request: %v", err)
	}
	for _, req := range []*delegation.Record{&firstReq, &secondReq} {
		req.RemoteThreadID = "rth-shared"
		req.ThreadPolicy = delegation.ThreadPolicyReuse
		req.ThreadGeneration = "thread-gen"
	}
	if !publisher.tryMarkDelegationActive(firstReq) {
		t.Fatal("first request should mark remote thread active")
	}
	if publisher.tryMarkDelegationActive(secondReq) {
		t.Fatal("second request should be rejected by in-memory remote thread fence")
	}
	publisher.clearDelegationActive(firstReq.DelegationID)

	firstClaim, err := delegation.NewClaimRecord(firstReq.DelegationID, "machine-a", publisher.workerInstanceID(), 1, now)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	secondClaim, err := delegation.NewClaimRecord(secondReq.DelegationID, "machine-a", publisher.workerInstanceID(), 1, now)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	started, err := publisher.tryStartDelegationExecution(firstReq, firstClaim)
	if err != nil || !started {
		t.Fatalf("first tryStart started=%v err=%v", started, err)
	}
	started, err = publisher.tryStartDelegationExecution(secondReq, secondClaim)
	if err != nil {
		t.Fatalf("second tryStart: %v", err)
	}
	if started {
		t.Fatal("second request should be rejected by durable remote thread fence")
	}
	store, err := delegation.LoadStore(statePath)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	thread, ok := store.RemoteThreadForID("rth-shared")
	if !ok || thread.State != delegation.RemoteThreadStateActive || thread.ActiveDelegationID != firstReq.DelegationID {
		t.Fatalf("thread = %#v ok=%v, want active first delegation", thread, ok)
	}
}

func TestBridgeMachineDelegationWorkerPagedDrainFindsRequestBehindFirstPage(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	bridge := testBridgeForMachineRegistry()
	statePath := filepath.Join(t.TempDir(), "delegation-worker.json")
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: statePath,
		Executor:                   &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "paged result"}},
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	oldHead := graph.addMessage(publisher.cache.InboxChatID, "message-old-head", "<p>old non-record</p>")
	store, err := delegation.LoadStore(statePath)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	store.UpsertInboxCursor(delegation.InboxCursor{
		ChatID:            publisher.cache.InboxChatID,
		LastHeadMessageID: oldHead.ID,
		UpdatedAt:         now.Format(time.RFC3339Nano),
	})
	if _, err := delegation.SaveStore(statePath, store); err != nil {
		t.Fatalf("SaveStore: %v", err)
	}
	req, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Objective: "request past first page"}, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.InboxRef = publisher.cache.InboxExternalID
	req.InboxGeneration = publisher.cache.InboxGeneration
	graph.addMessage(publisher.cache.InboxChatID, "message-request", delegation.RenderRecordHTML(req))
	for i := 0; i < delegationInboxDrainTop+5; i++ {
		graph.addMessage(publisher.cache.InboxChatID, fmt.Sprintf("message-noise-%02d", i), "<p>noise</p>")
	}
	if err := publisher.pollDelegationInbox(context.Background()); err != nil {
		t.Fatalf("poll inbox: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var records []delegation.Record
	for time.Now().Before(deadline) {
		messages, _ := graph.ListMessages(context.Background(), publisher.cache.InboxChatID, 100)
		records = delegation.ObserveRecords(machineDelegationMessages(messages))
		if delegation.Reduce(delegation.RecordsForID(records, req.DelegationID), now).Status == delegation.StateComplete {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	state := delegation.Reduce(delegation.RecordsForID(records, req.DelegationID), now)
	if state.Status != delegation.StateComplete || state.Terminal == nil || state.Terminal.Body != "paged result" {
		t.Fatalf("state = %#v records=%#v, want complete paged result", state, records)
	}
	store, err = delegation.LoadStore(statePath)
	if err != nil {
		t.Fatalf("LoadStore after poll: %v", err)
	}
	cursor, ok := store.InboxCursorForChat(publisher.cache.InboxChatID)
	if !ok || cursor.LastHeadMessageID == "" || cursor.LastHeadMessageID == oldHead.ID {
		t.Fatalf("cursor = %#v ok=%v, want advanced cursor", cursor, ok)
	}
}

func TestBridgeMachineDelegationWorkerRetriesSameHeadAfterDrainFailure(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	bridge := testBridgeForMachineRegistry()
	executor := &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "retried"}}
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: filepath.Join(t.TempDir(), "delegation-worker.json"),
		Executor:                   executor,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	req, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Objective: "retry after failed drain"}, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.InboxRef = publisher.cache.InboxExternalID
	req.InboxGeneration = publisher.cache.InboxGeneration
	if _, err := graph.SendHTML(context.Background(), publisher.cache.InboxChatID, delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	graph.windowErrOnce = errors.New("transient page failure")
	if err := publisher.pollDelegationInbox(context.Background()); err == nil || !strings.Contains(err.Error(), "transient page failure") {
		t.Fatalf("first poll err = %v, want transient page failure", err)
	}
	if publisher.lastInboxHeadID != "" {
		t.Fatalf("lastInboxHeadID = %q, want unchanged after failed drain", publisher.lastInboxHeadID)
	}
	if err := publisher.pollDelegationInbox(context.Background()); err != nil {
		t.Fatalf("retry poll: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	ran := false
	for time.Now().Before(deadline) {
		if executor.runCount() == 1 {
			ran = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ran {
		t.Fatalf("executor run count = %d, want retry to process request", executor.runCount())
	}
	if !waitPublishersIdle(time.Second, publisher) {
		t.Fatal("publisher did not finish delegation goroutine")
	}
}

func TestBridgeMachineDelegationWorkerRetryAfterBackoffSkipsPoll(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: filepath.Join(t.TempDir(), "delegation-worker.json"),
		Interval:                   5 * time.Second,
		Executor:                   &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "unused"}},
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	graph.exactErr = &machineregistry.StatusError{StatusCode: 429, RetryAfter: 2 * time.Minute, Err: errors.New("rate limited")}
	var logged []error
	delay := publisher.pollDelegationInboxDelay(context.Background(), func(err error) { logged = append(logged, err) })
	if delay != 2*time.Minute || len(logged) != 1 || graph.exactListCalls != 1 {
		t.Fatalf("delay=%v logged=%d exactCalls=%d, want Retry-After with one poll", delay, len(logged), graph.exactListCalls)
	}
	now = now.Add(time.Minute)
	delay = publisher.pollDelegationInboxDelay(context.Background(), nil)
	if delay != time.Minute || graph.exactListCalls != 1 {
		t.Fatalf("delay=%v exactCalls=%d, want stored backoff without Graph poll", delay, graph.exactListCalls)
	}
}

func TestBridgeMachineDelegationClaimRecheckDelayIsConfigurableAndCancellable(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	bridge := testBridgeForMachineRegistry()
	delay := 123 * time.Millisecond
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:               graph,
		MachineRegistryCachePath:           filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:                 func() time.Time { return now },
		MachineDelegationStatePath:         filepath.Join(t.TempDir(), "delegation-worker.json"),
		MachineDelegationClaimRecheckDelay: delay,
		Interval:                           5 * time.Second,
		Executor:                           &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "unused"}},
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if publisher.claimRecheckDelay != delay {
		t.Fatalf("claim recheck delay = %v, want %v", publisher.claimRecheckDelay, delay)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := publisher.waitBeforeDelegationClaimRecheck(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitBeforeDelegationClaimRecheck error = %v, want context canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("canceled claim recheck waited %v", elapsed)
	}
}

func TestBridgeMachineDelegationWorkerRaceExecutesOnlyWinningClaim(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	bridge := testBridgeForMachineRegistry()
	firstExec := &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "first"}}
	secondExec := &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "second"}}
	first, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry-a.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: filepath.Join(t.TempDir(), "delegation-worker-a.json"),
		Interval:                   5 * time.Second,
		Executor:                   firstExec,
	})
	if err != nil {
		t.Fatalf("new first publisher: %v", err)
	}
	second, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry-b.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: filepath.Join(t.TempDir(), "delegation-worker-b.json"),
		Interval:                   5 * time.Second,
		Executor:                   secondExec,
	})
	if err != nil {
		t.Fatalf("new second publisher: %v", err)
	}
	if err := first.publish(context.Background(), false); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	second.cache = first.cache
	req, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Objective: "race work"}, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.InboxRef = first.cache.InboxExternalID
	req.InboxGeneration = first.cache.InboxGeneration
	if _, err := graph.SendHTML(context.Background(), first.cache.InboxChatID, delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	var wg sync.WaitGroup
	for _, publisher := range []*bridgeMachineRegistryPublisher{first, second} {
		wg.Add(1)
		go func(p *bridgeMachineRegistryPublisher) {
			defer wg.Done()
			_ = p.pollDelegationInbox(context.Background())
		}(publisher)
	}
	wg.Wait()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if firstExec.runCount()+secondExec.runCount() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := firstExec.runCount() + secondExec.runCount(); got != 1 {
		t.Fatalf("executor run count = %d, want exactly one winner (first=%d second=%d)", got, firstExec.runCount(), secondExec.runCount())
	}
	if !waitPublishersIdle(time.Second, first, second) {
		t.Fatal("publishers did not finish delegation goroutines")
	}
}

func TestBridgeMachineDelegationWorkerCancelsRunningExecution(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	executor := newBlockingMachineDelegationExecutor()
	bridge := testBridgeForMachineRegistry()
	statePath := filepath.Join(t.TempDir(), "delegation-worker.json")
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: statePath,
		Executor:                   executor,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	req, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Objective: "cancel me"}, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.InboxRef = publisher.cache.InboxExternalID
	req.InboxGeneration = publisher.cache.InboxGeneration
	if _, err := graph.SendHTML(context.Background(), publisher.cache.InboxChatID, delegation.RenderRecordHTML(req)); err != nil {
		t.Fatalf("seed request: %v", err)
	}
	if err := publisher.pollDelegationInbox(context.Background()); err != nil {
		t.Fatalf("poll start: %v", err)
	}
	if !executor.waitStarted(time.Second) {
		t.Fatal("executor did not start")
	}
	cancel := delegation.NewTombstoneRecord(req.DelegationID, "user canceled", now.Add(time.Minute))
	if _, err := graph.SendHTML(context.Background(), publisher.cache.InboxChatID, delegation.RenderRecordHTML(cancel)); err != nil {
		t.Fatalf("send cancel: %v", err)
	}
	if err := publisher.pollDelegationInbox(context.Background()); err != nil {
		t.Fatalf("poll cancel: %v", err)
	}
	if !executor.waitDone(time.Second) {
		t.Fatal("executor did not observe cancellation")
	}
	deadline := time.Now().Add(time.Second)
	for {
		store, err := delegation.LoadStore(statePath)
		if err != nil {
			t.Fatalf("LoadStore: %v", err)
		}
		fence, ok := store.ExecutionForID(req.DelegationID)
		if ok && fence.Status == delegation.StateCanceled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fence = %#v ok=%v, want canceled", fence, ok)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBridgeMachineDelegationExecutionFenceSurvivesRestart(t *testing.T) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	statePath := filepath.Join(t.TempDir(), "delegation-worker.json")
	bridge := testBridgeForMachineRegistry()
	first, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       newFakeBridgeMachineRegistryGraph(),
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry-a.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: statePath,
		Executor:                   &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "unused"}},
	})
	if err != nil {
		t.Fatalf("new first publisher: %v", err)
	}
	req, err := delegation.NewRequestRecord("session-a", "turn-1", "", []string{"machine-source"}, "machine-a", delegation.TaskSpec{Objective: "restart fence"}, now)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	claim, err := delegation.NewClaimRecord(req.DelegationID, "machine-a", first.workerInstanceID(), 1, now)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	started, err := first.tryStartDelegationExecution(req, claim)
	if err != nil || !started {
		t.Fatalf("first tryStart started=%v err=%v", started, err)
	}
	restarted, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       newFakeBridgeMachineRegistryGraph(),
		MachineRegistryCachePath:   filepath.Join(t.TempDir(), "machine-registry-b.json"),
		MachineRegistryNow:         func() time.Time { return now.Add(time.Minute) },
		MachineDelegationStatePath: statePath,
		Executor:                   &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "unused"}},
	})
	if err != nil {
		t.Fatalf("new restarted publisher: %v", err)
	}
	started, err = restarted.tryStartDelegationExecution(req, claim)
	if err != nil {
		t.Fatalf("restart tryStart: %v", err)
	}
	if started {
		t.Fatal("restart should not start an already fenced delegation")
	}
	if err := first.finishDelegationExecution(req, claim, delegation.StateComplete); err != nil {
		t.Fatalf("finish fence: %v", err)
	}
	store, err := delegation.LoadStore(statePath)
	if err != nil {
		t.Fatalf("load store: %v", err)
	}
	fence, ok := store.ExecutionForID(req.DelegationID)
	if !ok || fence.Status != delegation.StateComplete {
		t.Fatalf("fence = %#v ok=%v, want complete", fence, ok)
	}
}

func BenchmarkBridgeMachineDelegationInboxIdleHeadCheck(b *testing.B) {
	now := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	graph := newFakeBridgeMachineRegistryGraph()
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		MachineRegistryGraph:       graph,
		MachineRegistryCachePath:   filepath.Join(b.TempDir(), "machine-registry.json"),
		MachineRegistryNow:         func() time.Time { return now },
		MachineDelegationStatePath: filepath.Join(b.TempDir(), "delegation-worker.json"),
		Interval:                   5 * time.Second,
		Executor:                   &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "unused"}},
	})
	if err != nil {
		b.Fatalf("new publisher: %v", err)
	}
	if err := publisher.publish(context.Background(), false); err != nil {
		b.Fatalf("publish: %v", err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := publisher.pollDelegationInbox(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBridgeMachineDelegationWorkerStateWriteAmplification(b *testing.B) {
	base := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	for _, threadCount := range []int{0, 1000, 10000} {
		b.Run(fmt.Sprintf("remote-threads-%d", threadCount), func(b *testing.B) {
			now := base
			statePath := filepath.Join(b.TempDir(), "delegation-worker.sqlite")
			if threadCount > 0 {
				store := delegation.Store{}
				for i := 0; i < threadCount; i++ {
					updatedAt := base.Add(-time.Duration(i%3600) * time.Second)
					store.UpsertRemoteThread(delegation.RemoteThread{
						ThreadID:             fmt.Sprintf("rth-%05d", i),
						MachineID:            "machine-a",
						SourceSessionID:      "session-a",
						WorkspaceFingerprint: "workspace-1",
						Title:                fmt.Sprintf("thread %05d", i),
						Summary:              "Prior remote worker context summary.",
						LastResultSummary:    "Prior useful result.",
						State:                delegation.RemoteThreadStateIdle,
						Generation:           fmt.Sprintf("gen-%05d", i),
						CreatedAt:            base.Add(-time.Hour).Format(time.RFC3339Nano),
						UpdatedAt:            updatedAt.Format(time.RFC3339Nano),
						LastUsedAt:           updatedAt.Format(time.RFC3339Nano),
						ExpiresAt:            base.Add(time.Hour).Format(time.RFC3339Nano),
					})
				}
				if _, err := delegation.SaveStore(statePath, store); err != nil {
					b.Fatalf("seed store: %v", err)
				}
			}
			graph := newFakeBridgeMachineRegistryGraph()
			bridge := testBridgeForMachineRegistry()
			publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
				MachineRegistryGraph:       graph,
				MachineRegistryCachePath:   filepath.Join(b.TempDir(), "machine-registry.json"),
				MachineRegistryNow:         func() time.Time { return now },
				MachineDelegationStatePath: statePath,
				Executor:                   &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "unused"}},
			})
			if err != nil {
				b.Fatalf("new publisher: %v", err)
			}
			threadID := func(i int) string {
				if threadCount <= 0 {
					return fmt.Sprintf("rth-new-%05d", i)
				}
				return fmt.Sprintf("rth-%05d", i%threadCount)
			}
			beforeIO, beforeOK := cxpPerfReadProcSelfIO()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				now = base.Add(time.Duration(i) * time.Second)
				req, err := delegation.NewRequestRecord(
					"session-a",
					fmt.Sprintf("turn-%05d", i),
					"",
					[]string{"machine-source"},
					"machine-a",
					delegation.TaskSpec{Objective: "benchmark remote worker state writes"},
					now,
				)
				if err != nil {
					b.Fatalf("request: %v", err)
				}
				req.RemoteThreadID = threadID(i)
				req.ThreadPolicy = delegation.ThreadPolicyReuse
				req.ThreadGeneration = "gen-bench"
				claim, err := delegation.NewClaimRecord(req.DelegationID, "machine-a", publisher.workerInstanceID(), 1, now)
				if err != nil {
					b.Fatalf("claim: %v", err)
				}
				started, err := publisher.tryStartDelegationExecution(req, claim)
				if err != nil {
					b.Fatalf("tryStartDelegationExecution: %v", err)
				}
				if !started {
					b.Fatal("tryStartDelegationExecution returned started=false")
				}
				if err := publisher.finishDelegationExecution(req, claim, delegation.StateComplete); err != nil {
					b.Fatalf("finishDelegationExecution: %v", err)
				}
			}
			b.StopTimer()
			cxpPerfReportProcIODelta(b, beforeIO, beforeOK, b.N)
			if info, err := os.Stat(statePath); err == nil {
				b.ReportMetric(float64(info.Size()), "store_file_B")
			}
		})
	}
}

func BenchmarkBridgeMachineDelegationWorkerInboxRoundTrip(b *testing.B) {
	base := time.Date(2026, 6, 21, 10, 0, 0, 0, time.UTC)
	for _, threadCount := range []int{0, 1000, 10000} {
		b.Run(fmt.Sprintf("remote-threads-%d", threadCount), func(b *testing.B) {
			now := base
			statePath := filepath.Join(b.TempDir(), "delegation-worker.sqlite")
			if threadCount > 0 {
				store := delegation.Store{}
				for i := 0; i < threadCount; i++ {
					updatedAt := base.Add(-time.Duration(i%3600) * time.Second)
					store.UpsertRemoteThread(delegation.RemoteThread{
						ThreadID:             fmt.Sprintf("rth-%05d", i),
						MachineID:            "machine-a",
						SourceSessionID:      "session-a",
						WorkspaceFingerprint: "workspace-1",
						Title:                fmt.Sprintf("thread %05d", i),
						Summary:              "Prior remote worker context summary.",
						LastResultSummary:    "Prior useful result.",
						State:                delegation.RemoteThreadStateIdle,
						Generation:           fmt.Sprintf("gen-%05d", i),
						CreatedAt:            base.Add(-time.Hour).Format(time.RFC3339Nano),
						UpdatedAt:            updatedAt.Format(time.RFC3339Nano),
						LastUsedAt:           updatedAt.Format(time.RFC3339Nano),
						ExpiresAt:            base.Add(time.Hour).Format(time.RFC3339Nano),
					})
				}
				if _, err := delegation.SaveStore(statePath, store); err != nil {
					b.Fatalf("seed store: %v", err)
				}
			}
			graph := newFakeBridgeMachineRegistryGraph()
			bridge := testBridgeForMachineRegistry()
			publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
				MachineRegistryGraph:               graph,
				MachineRegistryCachePath:           filepath.Join(b.TempDir(), "machine-registry.json"),
				MachineRegistryNow:                 func() time.Time { return now },
				MachineDelegationStatePath:         statePath,
				MachineDelegationClaimRecheckDelay: 0,
				Interval:                           5 * time.Second,
				Executor:                           &fakeMachineDelegationExecutor{result: ExecutionResult{Text: "round trip done"}},
			})
			if err != nil {
				b.Fatalf("new publisher: %v", err)
			}
			if err := publisher.publish(context.Background(), false); err != nil {
				b.Fatalf("publish: %v", err)
			}
			threadID := func(i int) string {
				if threadCount <= 0 {
					return fmt.Sprintf("rth-new-%05d", i)
				}
				return fmt.Sprintf("rth-%05d", i%threadCount)
			}
			beforeIO, beforeOK := cxpPerfReadProcSelfIO()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				now = base.Add(time.Duration(i) * time.Second)
				req, err := delegation.NewRequestRecord(
					"session-a",
					fmt.Sprintf("turn-%05d", i),
					"",
					[]string{"machine-source"},
					"machine-a",
					delegation.TaskSpec{Objective: "benchmark full worker inbox round trip"},
					now,
				)
				if err != nil {
					b.Fatalf("request: %v", err)
				}
				req.InboxRef = publisher.cache.InboxExternalID
				req.InboxGeneration = publisher.cache.InboxGeneration
				req.RemoteThreadID = threadID(i)
				req.ThreadPolicy = delegation.ThreadPolicyReuse
				req.ThreadGeneration = "gen-bench"
				if _, err := graph.SendHTML(context.Background(), publisher.cache.InboxChatID, delegation.RenderRecordHTML(req)); err != nil {
					b.Fatalf("seed request: %v", err)
				}
				if err := publisher.pollDelegationInbox(context.Background()); err != nil {
					b.Fatalf("pollDelegationInbox: %v", err)
				}
				if !waitPublishersIdle(2*time.Second, publisher) {
					b.Fatal("publisher did not finish delegation goroutine")
				}
			}
			b.StopTimer()
			cxpPerfReportProcIODelta(b, beforeIO, beforeOK, b.N)
			if info, err := os.Stat(statePath); err == nil {
				b.ReportMetric(float64(info.Size()), "store_file_B")
			}
		})
	}
}

func testBridgeForMachineRegistry() *Bridge {
	user := User{ID: "user-1", UserPrincipalName: "user@example.test"}
	scope := teamstore.ScopeIdentity{ID: "scope-1", Profile: "default", UserPrincipal: user.UserPrincipalName}
	return &Bridge{
		user:  user,
		scope: scope,
		machine: teamstore.MachineRecord{
			ID:            "machine-a",
			Label:         "Machine A",
			Hostname:      "host-a",
			UserPrincipal: user.UserPrincipalName,
			Profile:       "default",
		},
	}
}

type fakeMachineDelegationExecutor struct {
	result  ExecutionResult
	err     error
	mu      sync.Mutex
	prompt  string
	session string
	runs    int
}

type blockingMachineDelegationExecutor struct {
	started chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newBlockingMachineDelegationExecutor() *blockingMachineDelegationExecutor {
	return &blockingMachineDelegationExecutor{
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (e *blockingMachineDelegationExecutor) Run(ctx context.Context, _ *Session, _ string) (ExecutionResult, error) {
	e.once.Do(func() { close(e.started) })
	<-ctx.Done()
	close(e.done)
	return ExecutionResult{}, ctx.Err()
}

func (e *blockingMachineDelegationExecutor) waitStarted(timeout time.Duration) bool {
	select {
	case <-e.started:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (e *blockingMachineDelegationExecutor) waitDone(timeout time.Duration) bool {
	select {
	case <-e.done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (e *fakeMachineDelegationExecutor) Run(_ context.Context, session *Session, prompt string) (ExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if session != nil {
		e.session = session.ID
	}
	e.prompt = prompt
	e.runs++
	return e.result, e.err
}

func (e *fakeMachineDelegationExecutor) runCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runs
}

func (e *fakeMachineDelegationExecutor) promptText() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.prompt
}

func (e *fakeMachineDelegationExecutor) sessionID() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.session
}

func hasDelegationStatus(records []delegation.Record, delegationID string, status string) bool {
	for _, record := range records {
		if record.Kind == delegation.StatusKind && record.DelegationID == delegationID && record.Status == status {
			return true
		}
	}
	return false
}

func waitForBridgeDelegationStatus(t *testing.T, graph *fakeBridgeMachineRegistryGraph, chatID string, delegationID string, want string) []delegation.Record {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var records []delegation.Record
	for time.Now().Before(deadline) {
		messages, _ := graph.ListMessages(context.Background(), chatID, 100)
		records = delegation.ObserveRecords(machineDelegationMessages(messages))
		if delegation.Reduce(delegation.RecordsForID(records, delegationID), time.Now()).Status == want {
			return records
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("delegation %s did not reach %s; records=%#v", delegationID, want, records)
	return nil
}

func waitPublishersIdle(timeout time.Duration, publishers ...*bridgeMachineRegistryPublisher) bool {
	deadline := time.Now().Add(timeout)
	for {
		allIdle := true
		for _, publisher := range publishers {
			publisher.workerMu.Lock()
			active := len(publisher.activeDelegations)
			publisher.workerMu.Unlock()
			if active != 0 {
				allIdle = false
				break
			}
		}
		if allIdle {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type fakeBridgeMachineRegistryGraph struct {
	mu        sync.Mutex
	messages  map[string]machineregistry.ChatMessage
	order     []string
	chatOrder map[string][]string
	nextID    int

	createCalls    int
	sendCount      int
	patchCount     int
	patchErr       error
	exactErr       error
	windowErrOnce  error
	exactListCalls int
}

func newFakeBridgeMachineRegistryGraph() *fakeBridgeMachineRegistryGraph {
	return &fakeBridgeMachineRegistryGraph{
		messages:  map[string]machineregistry.ChatMessage{},
		chatOrder: map[string][]string{},
	}
}

func (g *fakeBridgeMachineRegistryGraph) CreateOrGetMeetingChatWindow(_ context.Context, topic string, externalID string, start time.Time, end time.Time) (machineregistry.Chat, machineregistry.OnlineMeeting, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.createCalls++
	chatID := "chat-registry"
	meetingID := "meeting-registry"
	if strings.Contains(externalID, "inbox") {
		chatID = "chat-inbox"
		meetingID = "meeting-inbox"
	}
	return machineregistry.Chat{ID: chatID, Topic: topic, ChatType: "meeting"}, machineregistry.OnlineMeeting{
		ID:            meetingID,
		Subject:       topic,
		StartDateTime: start.UTC().Format(time.RFC3339),
		EndDateTime:   end.UTC().Format(time.RFC3339),
		ChatThreadID:  chatID,
	}, nil
}

func (g *fakeBridgeMachineRegistryGraph) GetOnlineMeeting(_ context.Context, meetingID string) (machineregistry.OnlineMeeting, error) {
	threadID := "chat-registry"
	if strings.Contains(meetingID, "inbox") {
		threadID = "chat-inbox"
	}
	return machineregistry.OnlineMeeting{ID: meetingID, ChatThreadID: threadID}, nil
}

func (g *fakeBridgeMachineRegistryGraph) UpdateOnlineMeetingWindow(_ context.Context, meetingID string, start time.Time, end time.Time) (machineregistry.OnlineMeeting, error) {
	return machineregistry.OnlineMeeting{
		ID:            meetingID,
		StartDateTime: start.UTC().Format(time.RFC3339),
		EndDateTime:   end.UTC().Format(time.RFC3339),
		ChatThreadID:  "chat-registry",
	}, nil
}

func (g *fakeBridgeMachineRegistryGraph) SendHTML(_ context.Context, chatID string, html string) (machineregistry.ChatMessage, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sendCount++
	g.nextID++
	return g.addMessageLocked(chatID, fmt.Sprintf("message-%06d", g.nextID), html), nil
}

func (g *fakeBridgeMachineRegistryGraph) UpdateChatMessageHTML(_ context.Context, _ string, messageID string, html string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.patchCount++
	if g.patchErr != nil {
		err := g.patchErr
		g.patchErr = nil
		return err
	}
	msg, ok := g.messages[messageID]
	if !ok {
		return errors.New("message not found")
	}
	msg.Body.Content = html
	g.messages[messageID] = msg
	return nil
}

func (g *fakeBridgeMachineRegistryGraph) ListMessages(_ context.Context, chatID string, top int) ([]machineregistry.ChatMessage, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.listMessagesLocked(chatID, top), nil
}

func (g *fakeBridgeMachineRegistryGraph) listMessagesLocked(chatID string, top int) []machineregistry.ChatMessage {
	order := g.chatOrder[strings.TrimSpace(chatID)]
	if len(order) == 0 {
		order = g.order
	}
	if top <= 0 || top > len(order) {
		top = len(order)
	}
	out := make([]machineregistry.ChatMessage, 0, top)
	for i := 0; i < top; i++ {
		out = append(out, g.messages[order[i]])
	}
	return out
}

func (g *fakeBridgeMachineRegistryGraph) ListMessagesExactTopWithoutRateLimitRetry(ctx context.Context, chatID string, top int) ([]machineregistry.ChatMessage, error) {
	g.mu.Lock()
	g.exactListCalls++
	if g.exactErr != nil {
		err := g.exactErr
		g.exactErr = nil
		g.mu.Unlock()
		return nil, err
	}
	out := g.listMessagesLocked(chatID, top)
	g.mu.Unlock()
	return out, nil
}

func (g *fakeBridgeMachineRegistryGraph) ListMessagesWindow(ctx context.Context, chatID string, top int) (machineregistry.MessageWindow, error) {
	g.mu.Lock()
	if g.windowErrOnce != nil {
		err := g.windowErrOnce
		g.windowErrOnce = nil
		g.mu.Unlock()
		return machineregistry.MessageWindow{}, err
	}
	window := g.listMessagesWindowLocked(chatID, top, 0)
	g.mu.Unlock()
	return window, nil
}

func (g *fakeBridgeMachineRegistryGraph) ListMessagesWindowFromPath(ctx context.Context, path string) (machineregistry.MessageWindow, error) {
	var chatID string
	var top int
	var offset int
	if _, err := fmt.Sscanf(path, "fake-window:%s %d %d", &chatID, &top, &offset); err != nil {
		return machineregistry.MessageWindow{}, err
	}
	g.mu.Lock()
	window := g.listMessagesWindowLocked(chatID, top, offset)
	g.mu.Unlock()
	return window, nil
}

func (g *fakeBridgeMachineRegistryGraph) listMessagesWindowLocked(chatID string, top int, offset int) machineregistry.MessageWindow {
	messages := g.listMessagesLocked(chatID, 0)
	if top <= 0 {
		top = 50
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(messages) {
		return machineregistry.MessageWindow{}
	}
	end := offset + top
	if end > len(messages) {
		end = len(messages)
	}
	window := machineregistry.MessageWindow{Messages: append([]machineregistry.ChatMessage(nil), messages[offset:end]...)}
	if end < len(messages) {
		window.Truncated = true
		window.NextPath = fmt.Sprintf("fake-window:%s %d %d", strings.TrimSpace(chatID), top, end)
	}
	return window
}

func (g *fakeBridgeMachineRegistryGraph) addMessage(chatID string, id string, html string) machineregistry.ChatMessage {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.addMessageLocked(chatID, id, html)
}

func (g *fakeBridgeMachineRegistryGraph) addMessageLocked(chatID string, id string, html string) machineregistry.ChatMessage {
	msg := machineregistry.ChatMessage{ID: id}
	msg.Body.Content = html
	g.messages[id] = msg
	g.order = append([]string{id}, g.order...)
	chatID = strings.TrimSpace(chatID)
	g.chatOrder[chatID] = append([]string{id}, g.chatOrder[chatID]...)
	return msg
}

func (g *fakeBridgeMachineRegistryGraph) latestCard(t *testing.T) machineregistry.MachineCard {
	t.Helper()
	if len(g.order) == 0 {
		t.Fatal("no registry messages")
	}
	msg := g.messages[g.order[0]]
	card, ok := machineregistry.ParseCardMessage(msg.Body.Content)
	if !ok {
		t.Fatalf("registry message did not parse: %s", msg.Body.Content)
	}
	return card
}
