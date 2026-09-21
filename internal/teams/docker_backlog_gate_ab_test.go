package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// TestDockerBacklogGateRealSnapshot is an opt-in, no-Graph A/B experiment.
// baseline invokes the old all-maintenance paths; patched invokes the
// production mandatory backlog lanes already used by Listen. The store is a
// disposable copy supplied by the Docker runner and Codex sessions are read
// only. It intentionally reports timing/errors instead of turning a large
// real-data fixture into a smoke-sized pass/fail test.
func TestDockerBacklogGateRealSnapshot(t *testing.T) {
	storePath := strings.TrimSpace(os.Getenv("CXP_TEAMS_BACKLOG_GATE_STORE"))
	if storePath == "" {
		t.Skip("CXP_TEAMS_BACKLOG_GATE_STORE is not set")
	}
	mode := strings.TrimSpace(os.Getenv("CXP_TEAMS_BACKLOG_GATE_MODE"))
	if mode == "" {
		mode = "patched"
	}
	stateStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open Docker store: %v", err)
	}
	defer stateStore.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	schedule, err := stateStore.PollScheduleSnapshot(ctx)
	if err != nil {
		t.Fatalf("load schedule projection: %v", err)
	}
	backlog, err := stateStore.TeamsOperationalBacklog(ctx)
	if err != nil {
		t.Fatalf("load operational backlog: %v", err)
	}
	registry := Registry{Version: 1, ControlChatID: schedule.ControlChat.TeamsChatID, Chats: map[string]ChatState{}}
	for _, durable := range schedule.Sessions {
		registry.Sessions = append(registry.Sessions, registrySessionFromDurable(durable))
	}
	bridge := &Bridge{
		reg:                       registry,
		store:                     stateStore,
		scope:                     teamstore.ScopeIdentity{ID: "docker-backlog-gate", CodexHome: "/home/baka/.codex"},
		transcriptSyncInterval:    time.Nanosecond,
		transcriptSyncWorkerCount: 4,
	}
	var linkedJobs atomic.Int64
	var historyJobs atomic.Int64
	bridge.linkedTranscriptSessionHook = func(context.Context, Session) error {
		linkedJobs.Add(1)
		return nil
	}
	bridge.historyWatchPathHook = func(context.Context, string) error {
		historyJobs.Add(1)
		return nil
	}

	if mode != "baseline" && mode != "patched" {
		t.Fatalf("unsupported mode %q", mode)
	}
	result := map[string]any{
		"mode":               mode,
		"sessions":           len(schedule.Sessions),
		"chat_polls":         len(schedule.ChatPolls),
		"import_checkpoints": len(schedule.ImportCheckpoints),
		"history_watch":      historyWatchCount(ctx, stateStore),
		"active_turns":       backlog.ActiveTurns,
		"pending_inbound":    backlog.PendingInbound,
		"operational_poll":   backlog.OperationalPollFrontier,
	}
	started := time.Now()
	linkedCtx, linkedCancel := context.WithTimeout(context.Background(), 20*time.Second)
	var linkedErr error
	if mode == "baseline" {
		linkedErr = bridge.syncLinkedTranscriptsWithDiscoveryOptions(linkedCtx, false, time.Now(), true)
	} else {
		linkedErr = bridge.syncLinkedTranscriptsIfDueForBacklog(linkedCtx, time.Now())
	}
	linkedCancel()
	result["linked_ms"] = time.Since(started).Milliseconds()
	result["linked_jobs"] = linkedJobs.Load()
	result["linked_error"] = dockerBacklogGateErrorString(linkedErr)

	historyStarted := time.Now()
	historyCtx, historyCancel := context.WithTimeout(context.Background(), 20*time.Second)
	var historyErr error
	if mode == "baseline" {
		historyErr = bridge.syncCodexHistoryFinals(historyCtx, time.Now(), false)
	} else {
		historyErr = bridge.syncCodexHistoryFinalsForBacklog(historyCtx, time.Now())
	}
	historyCancel()
	result["history_ms"] = time.Since(historyStarted).Milliseconds()
	result["history_jobs"] = historyJobs.Load()
	result["history_error"] = dockerBacklogGateErrorString(historyErr)
	result["total_ms"] = time.Since(started).Milliseconds()
	encoded, _ := json.Marshal(result)
	fmt.Printf("CXP_DOCKER_BACKLOG_GATE_RESULT %s\n", encoded)
}

func historyWatchCount(ctx context.Context, stateStore *teamstore.Store) int {
	state, err := stateStore.HistoryWatchState(ctx)
	if err != nil {
		return -1
	}
	return len(state.HistoryWatch)
}

func dockerBacklogGateErrorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
