package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type cxpPerfProfile struct {
	Name            string
	Description     string
	WorkChats       int
	TurnsPerChat    int
	MessagesPerPoll int
	MessageBytes    int
	OutboxPerChat   int
	LookupPerCycle  int
	HistoryFiles    int
	HistoryLines    int
	RateLimited     bool
}

type cxpPerfProcIO struct {
	rchar               uint64
	wchar               uint64
	readBytes           uint64
	writeBytes          uint64
	cancelledWriteBytes uint64
}

func cxpPerfReadProcSelfIO() (cxpPerfProcIO, bool) {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return cxpPerfProcIO{}, false
	}
	var ioState cxpPerfProcIO
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "rchar":
			ioState.rchar = n
		case "wchar":
			ioState.wchar = n
		case "read_bytes":
			ioState.readBytes = n
		case "write_bytes":
			ioState.writeBytes = n
		case "cancelled_write_bytes":
			ioState.cancelledWriteBytes = n
		}
	}
	return ioState, true
}

func (ioState cxpPerfProcIO) delta(after cxpPerfProcIO) cxpPerfProcIO {
	return cxpPerfProcIO{
		rchar:               cxpPerfSaturatingSub(after.rchar, ioState.rchar),
		wchar:               cxpPerfSaturatingSub(after.wchar, ioState.wchar),
		readBytes:           cxpPerfSaturatingSub(after.readBytes, ioState.readBytes),
		writeBytes:          cxpPerfSaturatingSub(after.writeBytes, ioState.writeBytes),
		cancelledWriteBytes: cxpPerfSaturatingSub(after.cancelledWriteBytes, ioState.cancelledWriteBytes),
	}
}

func (ioState *cxpPerfProcIO) add(other cxpPerfProcIO) {
	ioState.rchar += other.rchar
	ioState.wchar += other.wchar
	ioState.readBytes += other.readBytes
	ioState.writeBytes += other.writeBytes
	ioState.cancelledWriteBytes += other.cancelledWriteBytes
}

func cxpPerfSaturatingSub(after, before uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

func cxpPerfReportProcIO(b *testing.B, total cxpPerfProcIO, n int) {
	b.Helper()
	if n <= 0 {
		return
	}
	denom := float64(n)
	b.ReportMetric(float64(total.readBytes)/denom, "disk_read_B/op")
	b.ReportMetric(float64(total.writeBytes)/denom, "disk_write_B/op")
	b.ReportMetric(float64(total.cancelledWriteBytes)/denom, "cancelled_write_B/op")
	b.ReportMetric(float64(total.rchar)/denom, "logical_read_B/op")
	b.ReportMetric(float64(total.wchar)/denom, "logical_write_B/op")
}

func cxpPerfReportProcIODelta(b *testing.B, before cxpPerfProcIO, beforeOK bool, n int) {
	b.Helper()
	if !beforeOK {
		return
	}
	after, afterOK := cxpPerfReadProcSelfIO()
	if !afterOK {
		return
	}
	cxpPerfReportProcIO(b, before.delta(after), n)
}

func cxpPerfMeasureProcIO(fn func() error) (cxpPerfProcIO, error) {
	before, beforeOK := cxpPerfReadProcSelfIO()
	err := fn()
	after, afterOK := cxpPerfReadProcSelfIO()
	if !beforeOK || !afterOK {
		return cxpPerfProcIO{}, err
	}
	return before.delta(after), err
}

func cxpPerfReportNamedProcIO(b *testing.B, prefix string, total cxpPerfProcIO, n int) {
	b.Helper()
	if n <= 0 {
		return
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return
	}
	denom := float64(n)
	b.ReportMetric(float64(total.writeBytes)/denom, prefix+"_disk_write_B/op")
	b.ReportMetric(float64(total.readBytes)/denom, prefix+"_disk_read_B/op")
	b.ReportMetric(float64(total.rchar)/denom, prefix+"_logical_read_B/op")
	b.ReportMetric(float64(total.wchar)/denom, prefix+"_logical_write_B/op")
}

var cxpPerfProfiles = []cxpPerfProfile{
	{
		Name:            "light-user",
		Description:     "one or two short chats, mostly idle",
		WorkChats:       2,
		TurnsPerChat:    6,
		MessagesPerPoll: 0,
		MessageBytes:    96,
		OutboxPerChat:   1,
		LookupPerCycle:  4,
		HistoryFiles:    2,
		HistoryLines:    20,
	},
	{
		Name:            "many-short-chats",
		Description:     "many short-lived chats with small prompts",
		WorkChats:       80,
		TurnsPerChat:    4,
		MessagesPerPoll: 1,
		MessageBytes:    128,
		OutboxPerChat:   1,
		LookupPerCycle:  48,
		HistoryFiles:    20,
		HistoryLines:    40,
	},
	{
		Name:            "few-very-long-chats",
		Description:     "one or two chats with very long accumulated history",
		WorkChats:       2,
		TurnsPerChat:    1500,
		MessagesPerPoll: 1,
		MessageBytes:    2048,
		OutboxPerChat:   8,
		LookupPerCycle:  64,
		HistoryFiles:    4,
		HistoryLines:    4000,
	},
	{
		Name:            "many-long-chats",
		Description:     "many active work chats, each with long history",
		WorkChats:       40,
		TurnsPerChat:    500,
		MessagesPerPoll: 1,
		MessageBytes:    1024,
		OutboxPerChat:   4,
		LookupPerCycle:  160,
		HistoryFiles:    40,
		HistoryLines:    1000,
	},
	{
		// This is a transition-storm profile: many chats are still active and
		// have not yet recorded a park notice, so a poll cycle can park and
		// notify all of them. It is intentionally harsher than the normal
		// steady state where old chats are already parked and notified.
		Name:            "idle-chat-hoarder",
		Description:     "hundreds of inactive but still active chats crossing the park threshold in one cycle",
		WorkChats:       240,
		TurnsPerChat:    2,
		MessagesPerPoll: 0,
		MessageBytes:    80,
		OutboxPerChat:   0,
		LookupPerCycle:  64,
		HistoryFiles:    10,
		HistoryLines:    20,
	},
	{
		Name:            "ci-burst-user",
		Description:     "short CI-like commands with frequent status/output messages",
		WorkChats:       24,
		TurnsPerChat:    80,
		MessagesPerPoll: 2,
		MessageBytes:    256,
		OutboxPerChat:   10,
		LookupPerCycle:  128,
		HistoryFiles:    24,
		HistoryLines:    200,
	},
	{
		Name:            "attachment-heavy-user",
		Description:     "moderate chats with large artifacts and attachment metadata",
		WorkChats:       12,
		TurnsPerChat:    120,
		MessagesPerPoll: 1,
		MessageBytes:    512,
		OutboxPerChat:   6,
		LookupPerCycle:  64,
		HistoryFiles:    12,
		HistoryLines:    200,
	},
	{
		Name:            "recovery-replay-user",
		Description:     "helper restart after downtime with duplicate message replay",
		WorkChats:       32,
		TurnsPerChat:    120,
		MessagesPerPoll: 3,
		MessageBytes:    256,
		OutboxPerChat:   3,
		LookupPerCycle:  192,
		HistoryFiles:    32,
		HistoryLines:    300,
	},
	{
		Name:            "rate-limited-tenant",
		Description:     "many chats under Graph 429/backoff pressure",
		WorkChats:       48,
		TurnsPerChat:    40,
		MessagesPerPoll: 0,
		MessageBytes:    160,
		OutboxPerChat:   2,
		LookupPerCycle:  96,
		HistoryFiles:    12,
		HistoryLines:    80,
		RateLimited:     true,
	},
	{
		Name:            "multi-workspace-power-user",
		Description:     "many workspaces, many chats, long local history",
		WorkChats:       64,
		TurnsPerChat:    240,
		MessagesPerPoll: 1,
		MessageBytes:    768,
		OutboxPerChat:   4,
		LookupPerCycle:  192,
		HistoryFiles:    80,
		HistoryLines:    600,
	},
}

type cxpPerfGraphMode string

const (
	cxpPerfGraphOK               cxpPerfGraphMode = ""
	cxpPerfGraphReadRateLimited  cxpPerfGraphMode = "graph-read-429"
	cxpPerfGraphReadUnauthorized cxpPerfGraphMode = "graph-read-401"
	cxpPerfGraphReadForbidden    cxpPerfGraphMode = "graph-read-403"
	cxpPerfGraphReadServerError  cxpPerfGraphMode = "graph-read-503"
	cxpPerfGraphReadNetworkDrop  cxpPerfGraphMode = "graph-read-network-drop"
	cxpPerfGraphReadMalformed    cxpPerfGraphMode = "graph-read-malformed-json"
	cxpPerfGraphSendRateLimited  cxpPerfGraphMode = "graph-send-429"
	cxpPerfGraphSendForbidden    cxpPerfGraphMode = "graph-send-403"
)

type cxpPerfCodexMode string

const (
	cxpPerfCodexSuccess      cxpPerfCodexMode = "codex-success"
	cxpPerfCodexStreaming    cxpPerfCodexMode = "codex-streaming"
	cxpPerfCodexFailure      cxpPerfCodexMode = "codex-error"
	cxpPerfCodexAmbiguous    cxpPerfCodexMode = "codex-ambiguous"
	cxpPerfCodexCanceled     cxpPerfCodexMode = "codex-canceled"
	cxpPerfCodexThreadSwitch cxpPerfCodexMode = "codex-thread-switch"
)

type cxpPerfServiceMode string

const (
	cxpPerfServiceIdle           cxpPerfServiceMode = ""
	cxpPerfServiceRestartCommand cxpPerfServiceMode = "service-restart-command"
	cxpPerfServiceReloadCommand  cxpPerfServiceMode = "service-reload-command"
)

type cxpPerfExternalScenario struct {
	Name          string
	Description   string
	GraphMode     cxpPerfGraphMode
	CodexMode     cxpPerfCodexMode
	ServiceMode   cxpPerfServiceMode
	ControlPrompt string
	QueueOutbox   bool
}

var cxpPerfExternalScenarios = []cxpPerfExternalScenario{
	{Name: "all-ok-streaming", Description: "Graph read/write succeeds and Codex streams status before a final answer", CodexMode: cxpPerfCodexStreaming},
	{Name: "codex-exec-error", Description: "Codex exits with a terminal execution error after accepting the prompt", CodexMode: cxpPerfCodexFailure},
	{Name: "codex-ambiguous-after-accept", Description: "Codex returns a turn id but the helper cannot confirm completion", CodexMode: cxpPerfCodexAmbiguous},
	{Name: "codex-canceled", Description: "Codex reports cancellation before a final result can be verified", CodexMode: cxpPerfCodexCanceled},
	{Name: "codex-thread-switch", Description: "Codex reports a different thread id than the resumed Teams session", CodexMode: cxpPerfCodexThreadSwitch},
	{Name: "graph-read-429", Description: "Graph read is throttled and the poll path must park/back off", GraphMode: cxpPerfGraphReadRateLimited, CodexMode: cxpPerfCodexSuccess},
	{Name: "graph-read-401", Description: "Graph read keeps returning unauthorized after token refresh", GraphMode: cxpPerfGraphReadUnauthorized, CodexMode: cxpPerfCodexSuccess},
	{Name: "graph-read-403", Description: "Graph read is forbidden for a chat", GraphMode: cxpPerfGraphReadForbidden, CodexMode: cxpPerfCodexSuccess},
	{Name: "graph-read-503", Description: "Graph read has a transient service failure", GraphMode: cxpPerfGraphReadServerError, CodexMode: cxpPerfCodexSuccess},
	{Name: "graph-read-network-drop", Description: "Graph transport fails before a response is available", GraphMode: cxpPerfGraphReadNetworkDrop, CodexMode: cxpPerfCodexSuccess},
	{Name: "graph-read-malformed-json", Description: "Graph returns HTTP 200 with invalid JSON", GraphMode: cxpPerfGraphReadMalformed, CodexMode: cxpPerfCodexSuccess},
	{Name: "graph-send-429", Description: "Pending outbox delivery is throttled by Graph", GraphMode: cxpPerfGraphSendRateLimited, CodexMode: cxpPerfCodexSuccess, QueueOutbox: true},
	{Name: "graph-send-403", Description: "Pending outbox delivery is rejected as forbidden", GraphMode: cxpPerfGraphSendForbidden, CodexMode: cxpPerfCodexSuccess, QueueOutbox: true},
	{Name: "service-helper-restart", Description: "Control chat asks the helper service layer to restart", CodexMode: cxpPerfCodexSuccess, ServiceMode: cxpPerfServiceRestartCommand, ControlPrompt: "helper restart now"},
	{Name: "service-helper-reload", Description: "Control chat asks the helper service layer to reload", CodexMode: cxpPerfCodexSuccess, ServiceMode: cxpPerfServiceReloadCommand, ControlPrompt: "helper reload now"},
}

func TestCXPPerfModelProfilesCanSeedStoreAndPoll(t *testing.T) {
	for _, profile := range cxpPerfProfiles {
		profile := cxpPerfSmokeProfile(profile)
		t.Run(profile.Name, func(t *testing.T) {
			t.Parallel()
			store := newCXPPerfStore(t, profile)
			ctx := context.Background()
			queries := cxpPerfLookupQueries(profile)
			if len(queries) == 0 {
				t.Fatal("no lookup queries")
			}
			if _, err := store.MessageLookup(ctx, queries[0].chatID, queries[0].messageID); err != nil {
				t.Fatalf("message lookup: %v", err)
			}
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil && !isGraphRateLimitError(err) {
				t.Fatalf("poll once: %v", err)
			}
			listenStore := newCXPPerfStore(t, profile)
			listenBridge := newCXPPerfBridge(listenStore, newCXPPerfGraph(profile), profile)
			if err := listenBridge.Listen(ctx, BridgeOptions{
				Store:                      listenStore,
				RegistryPath:               listenBridge.registryPath,
				Interval:                   time.Second,
				Once:                       true,
				Top:                        ownerPollMessageTop,
				MaxWorkChatPollsPerCycle:   DefaultMaxWorkChatPollsPerCycle,
				OwnerStaleAfter:            30 * time.Second,
				Executor:                   EchoExecutor{},
				ControlFallbackExecutor:    EchoExecutor{},
				ControlFallbackHelpContext: "perf",
				HelperVersion:              "perf-test",
			}); err != nil && !isGraphRateLimitError(err) {
				t.Fatalf("listen once: %v", err)
			}
		})
	}
}

func TestCXPPerfModelExternalScenariosCoverCommonPaths(t *testing.T) {
	cxpPerfWithImmediateHelperService(t)
	for _, scenario := range cxpPerfExternalScenarios {
		scenario := scenario
		t.Run(scenario.Name, func(t *testing.T) {
			t.Parallel()
			store, bridge, harness := newCXPPerfExternalBridge(t, scenario)
			if err := cxpPerfRunListenOnce(context.Background(), bridge, store, scenario, harness); err != nil && !cxpPerfExpectedListenError(err, scenario) {
				t.Fatalf("listen once external scenario error: %v", err)
			}
			switch scenario.ServiceMode {
			case cxpPerfServiceRestartCommand:
				harness.waitRestart(t)
			case cxpPerfServiceReloadCommand:
				harness.waitReload(t)
			}
		})
	}
}

func TestCXPPerfModelSQLiteProfilesCoverUpgradeOperations(t *testing.T) {
	for _, base := range cxpPerfProfiles {
		profile := cxpPerfSmokeProfile(base)
		for _, tc := range []struct {
			name string
			run  func(t *testing.T, profile cxpPerfProfile)
		}{
			{
				name: "poll-once",
				run: func(t *testing.T, profile cxpPerfProfile) {
					t.Helper()
					store := newCXPPerfStore(t, profile)
					cxpPerfMigrateStoreToSQLite(t, store)
					bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
					if err := bridge.pollOnce(context.Background(), ownerPollMessageTop); err != nil && !isGraphRateLimitError(err) {
						t.Fatalf("sqlite poll once: %v", err)
					}
				},
			},
			{
				name: "idle-cycle",
				run: func(t *testing.T, profile cxpPerfProfile) {
					t.Helper()
					profile.MessagesPerPoll = 0
					store := newCXPPerfStore(t, profile)
					cxpPerfMigrateStoreToSQLite(t, store)
					bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
					bridge.asyncTurns = true
					if err := bridge.pollOnce(context.Background(), ownerPollMessageTop); err != nil && !isGraphRateLimitError(err) {
						t.Fatalf("sqlite idle cycle: %v", err)
					}
				},
			},
			{
				name: "queued-drain",
				run: func(t *testing.T, profile cxpPerfProfile) {
					t.Helper()
					store := newCXPPerfStore(t, profile)
					cxpPerfSeedQueuedTurns(t, store, profile)
					cxpPerfMigrateStoreToSQLite(t, store)
					bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
					bridge.asyncTurns = true
					if err := bridge.processQueuedTurns(context.Background()); err != nil {
						t.Fatalf("sqlite queued drain: %v", err)
					}
					if err := cxpPerfDrainAsyncTurns(context.Background(), bridge); err != nil {
						t.Fatalf("sqlite queued drain wait: %v", err)
					}
				},
			},
			{
				name: "outbox-flush",
				run: func(t *testing.T, profile cxpPerfProfile) {
					t.Helper()
					store := newCXPPerfStore(t, profile)
					cxpPerfQueuePendingOutbox(t, store, profile)
					cxpPerfMigrateStoreToSQLite(t, store)
					bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
					if err := bridge.flushPendingOutboxMainLoop(context.Background()); err != nil && !isGraphRateLimitError(err) {
						t.Fatalf("sqlite outbox flush: %v", err)
					}
				},
			},
			{
				name: "deferred-inbound-no-deferred",
				run: func(t *testing.T, profile cxpPerfProfile) {
					t.Helper()
					store := newCXPPerfStore(t, profile)
					cxpPerfMigrateStoreToSQLite(t, store)
					deferred, err := store.DeferredInbound(context.Background())
					if err != nil {
						t.Fatalf("sqlite deferred inbound: %v", err)
					}
					if len(deferred) != 0 {
						t.Fatalf("sqlite deferred inbound returned %d events, want none", len(deferred))
					}
				},
			},
			{
				name: "history-watch-active-append",
				run: func(t *testing.T, profile cxpPerfProfile) {
					t.Helper()
					profile.MessagesPerPoll = 0
					store := newCXPPerfStore(t, profile)
					cxpPerfMigrateStoreToSQLite(t, store)
					bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
					cxpPerfSeedLinkedTranscriptFiles(t, store, bridge, profile)
					_, checkpoint := cxpPerfFirstHistoryWatchCheckpoint(t, store)
					now := time.Date(2026, 5, 23, 9, 45, 0, 0, time.UTC)
					cxpPerfAppendHistoryCommentary(t, checkpoint.Path, 0, now)
					bridge.lastHistoryWatchSync = time.Time{}
					bridge.lastHistoryWatchReconcile = now
					if err := bridge.syncCodexHistoryFinalsIfDue(context.Background(), now); err != nil {
						t.Fatalf("sqlite history watch active append: %v", err)
					}
				},
			},
		} {
			tc := tc
			t.Run(profile.Name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				tc.run(t, profile)
			})
		}
	}
}

func TestCXPPerfModelSQLiteExternalScenariosCoverCommonPaths(t *testing.T) {
	cxpPerfWithImmediateHelperService(t)
	for _, scenario := range cxpPerfExternalScenarios {
		scenario := scenario
		t.Run(scenario.Name, func(t *testing.T) {
			t.Parallel()
			store, bridge, harness := newCXPPerfExternalBridge(t, scenario)
			cxpPerfMigrateStoreToSQLite(t, store)
			if err := cxpPerfRunListenOnce(context.Background(), bridge, store, scenario, harness); err != nil && !cxpPerfExpectedListenError(err, scenario) {
				t.Fatalf("sqlite listen once external scenario error: %v", err)
			}
			switch scenario.ServiceMode {
			case cxpPerfServiceRestartCommand:
				harness.waitRestart(t)
			case cxpPerfServiceReloadCommand:
				harness.waitReload(t)
			}
		})
	}
}

func BenchmarkCXPPerfModelStoreProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			benchmarkCXPPerfStoreProfile(b, store, profile)
		})
	}
}

func benchmarkCXPPerfStoreProfile(b *testing.B, store *teamstore.Store, profile cxpPerfProfile) {
	ctx := context.Background()
	queries := cxpPerfLookupQueries(profile)
	base := time.Date(2026, 5, 23, 8, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chatIndex := i % max(1, profile.WorkChats)
		chatID := cxpPerfChatID(chatIndex)
		cursor := base.Add(time.Duration(i) * time.Second)
		if _, err := store.RecordChatPollSuccessWithContinuationAndSchedule(ctx, chatID, cursor, true, false, profile.MessagesPerPoll, "", func(teamstore.ChatPollState) (teamstore.ChatPollScheduleUpdate, error) {
			return teamstore.ChatPollScheduleUpdate{
				ChatID:            chatID,
				PollState:         "warm",
				NextPollAt:        cursor.Add(5 * time.Second),
				LastActivityAt:    cursor,
				ClearBlockedUntil: true,
				ResetFailures:     true,
			}, nil
		}); err != nil {
			b.Fatalf("poll success and schedule: %v", err)
		}
		for j := 0; j < profile.LookupPerCycle && j < len(queries); j++ {
			query := queries[(i+j)%len(queries)]
			if _, err := store.MessageLookup(ctx, query.chatID, query.messageID); err != nil {
				b.Fatalf("message lookup: %v", err)
			}
		}
		record := teamstore.MessageProvenanceRecord{
			TeamsChatID:    chatID,
			TeamsMessageID: cxpPerfInboundMessageID(chatIndex, i%max(1, profile.TurnsPerChat)),
			Origin:         teamstore.MessageOriginUserInbound,
			SessionID:      cxpPerfSessionID(chatIndex),
			CreatedAt:      cursor,
			UpdatedAt:      cursor,
		}
		if _, err := store.RecordMessageProvenance(ctx, record); err != nil {
			b.Fatalf("record provenance: %v", err)
		}
	}
}

func BenchmarkCXPPerfModelBridgePollProfiles(b *testing.B) {
	benchmarkCXPPerfModelSyncTotalCycleProfilesWithBackend(b, "")
}

func BenchmarkCXPPerfModelSyncTotalCycleProfiles(b *testing.B) {
	benchmarkCXPPerfModelSyncTotalCycleProfilesWithBackend(b, "")
}

func benchmarkCXPPerfModelSyncTotalCycleProfilesWithBackend(b *testing.B, backend string) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfMigrateStoreBackend(b, store, backend)
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil && !isGraphRateLimitError(err) {
					b.Fatalf("sync total cycle poll once: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelDaemonPollIngestProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonPollIngestProfilesWithBackend(b, "")
}

func BenchmarkCXPPerfModelSQLiteDaemonPollIngestProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonPollIngestProfilesWithBackend(b, "sqlite")
}

func benchmarkCXPPerfModelDaemonPollIngestProfilesWithBackend(b *testing.B, backend string) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedRunningTurns(b, store, profile)
			cxpPerfMigrateStoreBackend(b, store, backend)
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			bridge.asyncTurns = true
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil && !isGraphRateLimitError(err) {
					b.Fatalf("daemon poll ingest: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelDaemonTotalCycleProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonTotalCycleProfilesWithBackend(b, "")
}

func BenchmarkCXPPerfModelSQLiteDaemonTotalCycleProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonTotalCycleProfilesWithBackend(b, "sqlite")
}

func benchmarkCXPPerfModelDaemonTotalCycleProfilesWithBackend(b *testing.B, backend string) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfMigrateStoreBackend(b, store, backend)
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			bridge.asyncTurns = true
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil && !isGraphRateLimitError(err) {
					b.Fatalf("daemon total cycle poll: %v", err)
				}
				if err := cxpPerfDrainAsyncTurns(ctx, bridge); err != nil {
					b.Fatalf("daemon total cycle drain: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelDaemonIdleCycleProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonIdleCycleProfilesWithBackend(b, "")
}

func BenchmarkCXPPerfModelSQLiteDaemonIdleCycleProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonIdleCycleProfilesWithBackend(b, "sqlite")
}

func benchmarkCXPPerfModelDaemonIdleCycleProfilesWithBackend(b *testing.B, backend string) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		profile.MessagesPerPoll = 0
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfMigrateStoreBackend(b, store, backend)
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			bridge.asyncTurns = true
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil && !isGraphRateLimitError(err) {
					b.Fatalf("daemon idle cycle: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelDaemonQueuedTurnDrainProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonQueuedTurnDrainProfilesWithBackend(b, "")
}

func BenchmarkCXPPerfModelSQLiteDaemonQueuedTurnDrainProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonQueuedTurnDrainProfilesWithBackend(b, "sqlite")
}

func benchmarkCXPPerfModelDaemonQueuedTurnDrainProfilesWithBackend(b *testing.B, backend string) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			var ioTotal cxpPerfProcIO
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := newCXPPerfStore(b, profile)
				cxpPerfSeedQueuedTurns(b, store, profile)
				cxpPerfMigrateStoreBackend(b, store, backend)
				graph := newCXPPerfGraph(profile)
				bridge := newCXPPerfBridge(store, graph, profile)
				bridge.asyncTurns = true
				beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
				b.StartTimer()
				if err := bridge.processQueuedTurns(ctx); err != nil {
					b.Fatalf("daemon queued turn drain process: %v", err)
				}
				if err := cxpPerfDrainAsyncTurns(ctx, bridge); err != nil {
					b.Fatalf("daemon queued turn drain wait: %v", err)
				}
				b.StopTimer()
				if beforeIOOK {
					afterIO, afterIOOK := cxpPerfReadProcSelfIO()
					if afterIOOK {
						ioTotal.add(beforeIO.delta(afterIO))
					}
				}
			}
			cxpPerfReportProcIO(b, ioTotal, b.N)
		})
	}
}

func BenchmarkCXPPerfModelDaemonOutboxFlushProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonOutboxFlushProfilesWithBackend(b, "")
}

func BenchmarkCXPPerfModelSQLiteDaemonOutboxFlushProfiles(b *testing.B) {
	benchmarkCXPPerfModelDaemonOutboxFlushProfilesWithBackend(b, "sqlite")
}

func benchmarkCXPPerfModelDaemonOutboxFlushProfilesWithBackend(b *testing.B, backend string) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := newCXPPerfStore(b, profile)
				cxpPerfQueuePendingOutbox(b, store, profile)
				cxpPerfMigrateStoreBackend(b, store, backend)
				graph := newCXPPerfGraph(profile)
				bridge := newCXPPerfBridge(store, graph, profile)
				b.StartTimer()
				if err := bridge.flushPendingOutboxMainLoop(ctx); err != nil && !isGraphRateLimitError(err) {
					b.Fatalf("daemon outbox flush: %v", err)
				}
				b.StopTimer()
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteLiveProgressGuardProfiles(b *testing.B) {
	b.Run("large-history", func(b *testing.B) {
		benchmarkBridgeCanQueueLiveTurnOutboxSQLiteLargeHistory(b)
	})
}

func BenchmarkCXPPerfModelSQLiteMainLoopIdleTickProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		profile.MessagesPerPoll = 0
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			bridge.asyncTurns = true
			cxpPerfSeedLinkedTranscriptFiles(b, store, bridge, profile)
			cxpPerfPrepareActiveOwner(b, bridge)
			ctx := context.Background()
			now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
			opts := BridgeOptions{Top: ownerPollMessageTop, MaxWorkChatPollsPerCycle: DefaultMaxWorkChatPollsPerCycle, Interval: 5 * time.Second}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := cxpPerfRunMainLoopIdleTick(ctx, bridge, opts, now.Add(time.Duration(i)*11*time.Second)); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
					b.Fatalf("main loop idle tick: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteIdleWorkAutoParkSweepProfiles(b *testing.B) {
	var profile cxpPerfProfile
	for _, candidate := range cxpPerfProfiles {
		if candidate.Name == "idle-chat-hoarder" {
			profile = candidate
			break
		}
	}
	if profile.Name == "" {
		b.Fatal("idle-chat-hoarder profile not found")
	}
	profile.MessagesPerPoll = 0
	store := newCXPPerfStore(b, profile)
	graph := newCXPPerfGraph(profile)
	bridge := newCXPPerfBridge(store, graph, profile)
	bridge.asyncTurns = true
	cxpPerfSeedColdRuntimeMetadata(b, store, profile)
	cxpPerfSeedIdleAutoParkCandidates(b, store)
	cxpPerfMigrateStoreToSQLite(b, store)
	cxpPerfPrepareActiveOwner(b, bridge)
	ctx := context.Background()
	now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick := now.Add(time.Duration(i) * (autoParkSweepInterval + time.Second))
		if err := bridge.maybeRunIdleWorkChatAutoPark(ctx, tick); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
			b.Fatalf("idle work auto-park sweep: %v", err)
		}
	}
}

func BenchmarkCXPPerfModelSQLiteActiveParkedMainLoopProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		profile.MessagesPerPoll = 0
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			bridge.asyncTurns = true
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfSeedActiveParkedSessions(b, store, bridge)
			cxpPerfMigrateStoreToSQLite(b, store)
			cxpPerfSeedLinkedTranscriptFiles(b, store, bridge, profile)
			cxpPerfPrepareActiveOwner(b, bridge)
			ctx := context.Background()
			now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
			opts := BridgeOptions{Top: ownerPollMessageTop, MaxWorkChatPollsPerCycle: DefaultMaxWorkChatPollsPerCycle, Interval: 5 * time.Second}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := cxpPerfRunMainLoopIdleTick(ctx, bridge, opts, now.Add(time.Duration(i)*11*time.Second)); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
					b.Fatalf("active parked main loop idle tick: %v", err)
				}
			}
		})
	}
}

// BenchmarkCXPPerfModelSQLiteParkedChatHoarderMainLoop covers the realistic
// steady state where thousands of old Work chats remain in the registry but
// their poll rows are already parked and notified. It should not trigger the
// first-park freeze-notice fanout measured by idle-chat-hoarder.
func BenchmarkCXPPerfModelSQLiteParkedChatHoarderMainLoop(b *testing.B) {
	profile := cxpPerfProfile{
		Name:            "parked-chat-hoarder-2000",
		Description:     "thousands of already parked and notified work chats",
		WorkChats:       2000,
		TurnsPerChat:    0,
		MessagesPerPoll: 0,
		MessageBytes:    80,
		OutboxPerChat:   0,
		LookupPerCycle:  64,
		HistoryFiles:    0,
		HistoryLines:    0,
	}
	store := newCXPPerfStore(b, profile)
	graph := newCXPPerfGraph(profile)
	bridge := newCXPPerfBridge(store, graph, profile)
	bridge.asyncTurns = true
	cxpPerfSeedColdRuntimeMetadata(b, store, profile)
	cxpPerfSeedActiveParkedSessions(b, store, bridge)
	cxpPerfMigrateStoreToSQLite(b, store)
	cxpPerfPrepareActiveOwner(b, bridge)
	ctx := context.Background()
	now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
	opts := BridgeOptions{Top: ownerPollMessageTop, MaxWorkChatPollsPerCycle: DefaultMaxWorkChatPollsPerCycle, Interval: 5 * time.Second}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cxpPerfRunMainLoopIdleTick(ctx, bridge, opts, now.Add(time.Duration(i)*11*time.Second)); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
			b.Fatalf("parked chat hoarder main loop idle tick: %v", err)
		}
	}
}

// BenchmarkCXPPerfModelSQLiteRealisticMixedUser covers the expected large-user
// steady state: around 100 active chats with mixed history sizes, thousands of
// already parked chats, sparse user messages, and periodic live progress sends.
//
// Keep this as a dedicated benchmark instead of adding it to cxpPerfProfiles;
// otherwise every generic profile benchmark would inherit the 2100-chat fixture
// and make CI perf runs unreasonably slow.
func BenchmarkCXPPerfModelSQLiteRealisticMixedUser(b *testing.B) {
	b.Run("idle-main-loop", func(b *testing.B) {
		store, bridge := newCXPPerfRealisticMixedUserFixture(b)
		ctx := context.Background()
		now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
		opts := BridgeOptions{Top: ownerPollMessageTop, MaxWorkChatPollsPerCycle: DefaultMaxWorkChatPollsPerCycle, Interval: 5 * time.Second}
		cxpPerfMarkMainLoopMaintenanceFresh(bridge, now)
		b.ReportAllocs()
		beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := cxpPerfRunMainLoopSteadyIdleTick(ctx, bridge, opts, now.Add(time.Duration(i)*3*time.Second)); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
				b.Fatalf("realistic mixed idle tick: %v", err)
			}
		}
		b.StopTimer()
		cxpPerfReportProcIODelta(b, beforeIO, beforeIOOK, b.N)
		_ = store
	})
	b.Run("transcript-sync-due", func(b *testing.B) {
		_, bridge := newCXPPerfRealisticMixedUserFixture(b)
		ctx := context.Background()
		now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
		opts := BridgeOptions{Top: ownerPollMessageTop, MaxWorkChatPollsPerCycle: DefaultMaxWorkChatPollsPerCycle, Interval: 5 * time.Second}
		b.ReportAllocs()
		beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := cxpPerfRunMainLoopIdleTick(ctx, bridge, opts, now.Add(time.Duration(i)*time.Minute)); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
				b.Fatalf("realistic mixed transcript sync due tick: %v", err)
			}
		}
		b.StopTimer()
		cxpPerfReportProcIODelta(b, beforeIO, beforeIOOK, b.N)
	})
	b.Run("live-progress-update", func(b *testing.B) {
		_, bridge := newCXPPerfRealisticMixedUserFixture(b)
		ctx := context.Background()
		sessionID, chatID, turnID := cxpPerfSeedRealisticRunningTurn(b, bridge.store)
		b.ReportAllocs()
		beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !bridge.canQueueLiveTurnOutbox(ctx, sessionID, turnID) {
				b.Fatal("running turn unexpectedly rejected live progress")
			}
			kind := fmt.Sprintf("codex-progress-%03d", i+1)
			body := fmt.Sprintf("working on realistic mixed user update %d", i+1)
			if err := bridge.queueAndSendOutboxChunks(ctx, sessionID, turnID, chatID, kind, body); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
				b.Fatalf("realistic mixed live progress update: %v", err)
			}
		}
		b.StopTimer()
		cxpPerfReportProcIODelta(b, beforeIO, beforeIOOK, b.N)
	})
	b.Run("single-user-message-drain", func(b *testing.B) {
		_, bridge := newCXPPerfRealisticMixedUserFixture(b)
		ctx := context.Background()
		b.ReportAllocs()
		beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := cxpPerfHandleRealisticUserMessage(ctx, bridge, i); err != nil {
				b.Fatalf("realistic mixed user message: %v", err)
			}
			if err := cxpPerfDrainAsyncTurns(ctx, bridge); err != nil {
				b.Fatalf("realistic mixed queued turn drain: %v", err)
			}
		}
		b.StopTimer()
		cxpPerfReportProcIODelta(b, beforeIO, beforeIOOK, b.N)
	})
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserMessageDrainBreakdown(b *testing.B) {
	b.Run("handle-message-start", func(b *testing.B) {
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		var ioTotal cxpPerfProcIO
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			_, bridge := newCXPPerfRealisticMixedUserFixture(b)
			executor := newCXPPerfBlockingExecutor()
			bridge.executor = executor
			beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
			b.StartTimer()
			if err := cxpPerfHandleRealisticUserMessage(ctx, bridge, i); err != nil {
				b.Fatalf("realistic mixed user message: %v", err)
			}
			executor.waitStarted(b)
			b.StopTimer()
			if beforeIOOK {
				afterIO, afterIOOK := cxpPerfReadProcSelfIO()
				if afterIOOK {
					ioTotal.add(beforeIO.delta(afterIO))
				}
			}
			executor.release()
			if err := cxpPerfDrainAsyncTurns(ctx, bridge); err != nil {
				b.Fatalf("realistic mixed cleanup drain: %v", err)
			}
		}
		cxpPerfReportProcIO(b, ioTotal, b.N)
	})
	b.Run("complete-running-turn", func(b *testing.B) {
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		var ioTotal cxpPerfProcIO
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			_, bridge := newCXPPerfRealisticMixedUserFixture(b)
			executor := newCXPPerfBlockingExecutor()
			bridge.executor = executor
			if err := cxpPerfHandleRealisticUserMessage(ctx, bridge, i); err != nil {
				b.Fatalf("realistic mixed user message: %v", err)
			}
			executor.waitStarted(b)
			beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
			b.StartTimer()
			executor.release()
			if err := cxpPerfDrainAsyncTurns(ctx, bridge); err != nil {
				b.Fatalf("realistic mixed queued turn drain: %v", err)
			}
			b.StopTimer()
			if beforeIOOK {
				afterIO, afterIOOK := cxpPerfReadProcSelfIO()
				if afterIOOK {
					ioTotal.add(beforeIO.delta(afterIO))
				}
			}
		}
		cxpPerfReportProcIO(b, ioTotal, b.N)
	})
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserMessageDrainNoCodexDiscovery(b *testing.B) {
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(context.Context, string) ([]codexhistory.Project, error) {
		return nil, nil
	}
	b.Cleanup(func() {
		discoverCodexProjectsForTeams = prevDiscover
	})
	_, bridge := newCXPPerfRealisticMixedUserFixture(b)
	ctx := context.Background()
	b.ReportAllocs()
	beforeIO, beforeIOOK := cxpPerfReadProcSelfIO()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cxpPerfHandleRealisticUserMessage(ctx, bridge, i); err != nil {
			b.Fatalf("realistic mixed user message: %v", err)
		}
		if err := cxpPerfDrainAsyncTurns(ctx, bridge); err != nil {
			b.Fatalf("realistic mixed queued turn drain: %v", err)
		}
	}
	b.StopTimer()
	cxpPerfReportProcIODelta(b, beforeIO, beforeIOOK, b.N)
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserMessageDrainStageWrites(b *testing.B) {
	stages := []string{"importing", "service_control", "duplicate", "queue_state", "prepare_local", "ensure", "inbound", "turn", "ack", "start", "complete"}
	totals := make(map[string]cxpPerfProcIO, len(stages))
	durations := make(map[string]time.Duration, len(stages))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, bridge := newCXPPerfRealisticMixedUserFixture(b)
		executor := newCXPPerfBlockingExecutor()
		bridge.executor = executor
		chatID, msg, text := cxpPerfRealisticUserMessage(i)
		session := bridge.reg.SessionByChatID(chatID)
		if session == nil {
			b.Fatalf("realistic session for chat %s not found", chatID)
		}
		var inbound teamstore.InboundEvent
		var turn teamstore.Turn
		runStage := func(name string, fn func() error) {
			b.Helper()
			b.StartTimer()
			start := time.Now()
			delta, err := cxpPerfMeasureProcIO(fn)
			elapsed := time.Since(start)
			b.StopTimer()
			total := totals[name]
			total.add(delta)
			totals[name] = total
			durations[name] += elapsed
			if err != nil {
				b.Fatalf("%s: %v", name, err)
			}
		}
		runStage("importing", func() error {
			if importing, err := bridge.sessionTranscriptImportInProgress(ctx, session.ID); err != nil {
				return err
			} else if importing {
				return fmt.Errorf("session unexpectedly importing transcript")
			}
			return nil
		})
		runStage("service_control", func() error {
			if control, blocked, err := bridge.serviceControlBlocksNewWork(ctx); err != nil {
				return err
			} else if blocked {
				return fmt.Errorf("service control unexpectedly blocked work: %#v", control)
			}
			return nil
		})
		runStage("duplicate", func() error {
			if duplicate, err := bridge.ignoreRecentDuplicateSessionPrompt(ctx, session, msg, text); err != nil {
				return err
			} else if duplicate {
				return fmt.Errorf("message unexpectedly classified as duplicate")
			}
			return nil
		})
		runStage("queue_state", func() error {
			turns, err := bridge.sessionTurnQueueState(ctx, session.ID)
			if err != nil {
				return err
			}
			if turns.Running || turns.Queued != 0 {
				return fmt.Errorf("session unexpectedly has queued/running turns: %#v", turns)
			}
			return nil
		})
		runStage("prepare_local", func() error {
			gate, err := bridge.prepareLocalCodexBeforeTeamsTurn(ctx, session)
			if err != nil {
				return err
			}
			if gate.Block {
				return fmt.Errorf("local codex gate unexpectedly blocked: %s", gate.AckBody)
			}
			return nil
		})
		runStage("ensure", func() error {
			return bridge.ensureDurableSession(ctx, session)
		})
		runStage("inbound", func() error {
			var created bool
			var err error
			inbound, created, err = bridge.persistInbound(ctx, session, msg)
			if err != nil {
				return err
			}
			if !created {
				return fmt.Errorf("realistic inbound was not created")
			}
			return nil
		})
		runStage("turn", func() error {
			var created bool
			var err error
			turn, created, err = bridge.queueTurn(ctx, session, inbound)
			if err != nil {
				return err
			}
			if !created {
				return fmt.Errorf("realistic turn was not created")
			}
			session.UpdatedAt = time.Now()
			bridge.markRegistryProjectionDirty()
			return nil
		})
		runStage("ack", func() error {
			return bridge.queueTeamsPromptAckForMessage(ctx, session, turn, msg, false)
		})
		runStage("start", func() error {
			started, err := bridge.startQueuedTurn(ctx, session, turn.ID, func(runCtx context.Context, runSession *Session, claimed teamstore.Turn) error {
				return bridge.runPreparedQueuedTurnFromMessage(runCtx, runSession, claimed, runSession.ChatID, msg, text, bridge.executor)
			})
			if err != nil {
				return err
			}
			if !started {
				return fmt.Errorf("turn was not started")
			}
			executor.waitStarted(b)
			return nil
		})
		runStage("complete", func() error {
			executor.release()
			return cxpPerfDrainAsyncTurns(ctx, bridge)
		})
	}
	for _, stage := range stages {
		cxpPerfReportNamedProcIO(b, stage, totals[stage], b.N)
		b.ReportMetric(float64(durations[stage])/float64(b.N), stage+"_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserChatPollErrorStageWrites(b *testing.B) {
	ctx := context.Background()
	var total cxpPerfProcIO
	var duration time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, _ := newCXPPerfRealisticMixedUserFixture(b)
		chatID := cxpPerfChatID(i % 100)
		blockedUntil := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC).Add(time.Duration(i+1) * time.Minute)
		b.StartTimer()
		start := time.Now()
		delta, err := cxpPerfMeasureProcIO(func() error {
			return store.RecordChatPollErrorWithBlock(ctx, chatID, "Graph 429 Too Many Requests", blockedUntil)
		})
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatalf("RecordChatPollErrorWithBlock: %v", err)
		}
		total.add(delta)
		duration += elapsed
	}
	cxpPerfReportNamedProcIO(b, "chat_poll_error", total, b.N)
	if b.N > 0 {
		b.ReportMetric(float64(duration)/float64(b.N), "chat_poll_error_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserOutboxSendErrorStageWrites(b *testing.B) {
	ctx := context.Background()
	var total cxpPerfProcIO
	var duration time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, bridge := newCXPPerfRealisticMixedUserFixture(b)
		session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
		if session == nil {
			b.Fatalf("realistic session missing")
		}
		msg, created, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
			ID:                   fmt.Sprintf("realistic-send-error-outbox-%06d", i),
			SessionID:            session.ID,
			TeamsChatID:          session.ChatID,
			Kind:                 "status-progress",
			Body:                 "uploading artifact before send error",
			ArtifactIDs:          []string{fmt.Sprintf("artifact:send-error:%06d", i)},
			AttachmentName:       "report.txt",
			AttachmentUploadName: "report-upload.txt",
			DriveItemID:          "drive-item-1",
			Status:               teamstore.OutboxStatusSending,
		})
		if err != nil || !created {
			b.Fatalf("QueueOutbox setup created=%v err=%v", created, err)
		}
		b.StartTimer()
		start := time.Now()
		delta, err := cxpPerfMeasureProcIO(func() error {
			_, err := store.MarkOutboxSendError(ctx, msg.ID, "Graph 502 Bad Gateway while posting Teams message")
			return err
		})
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatalf("MarkOutboxSendError: %v", err)
		}
		total.add(delta)
		duration += elapsed
	}
	cxpPerfReportNamedProcIO(b, "outbox_send_error", total, b.N)
	if b.N > 0 {
		b.ReportMetric(float64(duration)/float64(b.N), "outbox_send_error_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserOutboxDriveItemStageWrites(b *testing.B) {
	ctx := context.Background()
	var total cxpPerfProcIO
	var duration time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, bridge := newCXPPerfRealisticMixedUserFixture(b)
		session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
		if session == nil {
			b.Fatalf("realistic session missing")
		}
		msg, created, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
			ID:                   fmt.Sprintf("realistic-drive-item-outbox-%06d", i),
			SessionID:            session.ID,
			TeamsChatID:          session.ChatID,
			Kind:                 "status-progress",
			Body:                 "uploading artifact before drive item update",
			ArtifactIDs:          []string{fmt.Sprintf("artifact:drive-item:%06d", i)},
			AttachmentName:       "report.txt",
			AttachmentUploadName: "report-upload.txt",
			Status:               teamstore.OutboxStatusSending,
		})
		if err != nil || !created {
			b.Fatalf("QueueOutbox setup created=%v err=%v", created, err)
		}
		b.StartTimer()
		start := time.Now()
		delta, err := cxpPerfMeasureProcIO(func() error {
			_, err := store.MarkOutboxDriveItem(ctx, msg.ID, "drive-item-1", "report.txt", "etag-1", "https://sharepoint/report", "dav://report")
			return err
		})
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatalf("MarkOutboxDriveItem: %v", err)
		}
		total.add(delta)
		duration += elapsed
	}
	cxpPerfReportNamedProcIO(b, "outbox_drive_item", total, b.N)
	if b.N > 0 {
		b.ReportMetric(float64(duration)/float64(b.N), "outbox_drive_item_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserArtifactUpsertStageWrites(b *testing.B) {
	ctx := context.Background()
	var total cxpPerfProcIO
	var duration time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, _ := newCXPPerfRealisticMixedUserFixture(b)
		artifactID := fmt.Sprintf("realistic-artifact-upsert-%06d", i)
		if _, err := store.UpsertArtifactRecord(ctx, teamstore.ArtifactRecord{
			ID:          artifactID,
			SessionID:   cxpPerfSessionID(i % 100),
			TurnID:      fmt.Sprintf("realistic-turn-%03d-%05d", i%100, 0),
			Path:        "artifact.txt",
			UploadName:  "codex-artifact.txt",
			OutboxID:    fmt.Sprintf("realistic-outbox-%03d-%04d", i%100, 0),
			DriveItemID: "drive-item-existing",
			Status:      "uploaded",
			UploadedAt:  time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC),
		}); err != nil {
			b.Fatalf("seed UpsertArtifactRecord: %v", err)
		}
		b.StartTimer()
		start := time.Now()
		delta, err := cxpPerfMeasureProcIO(func() error {
			_, err := store.UpsertArtifactRecord(ctx, teamstore.ArtifactRecord{
				ID:        artifactID,
				SessionID: cxpPerfSessionID(i % 100),
				Status:    "message_failed",
				Error:     "Graph post failed after upload",
			})
			return err
		})
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatalf("UpsertArtifactRecord: %v", err)
		}
		total.add(delta)
		duration += elapsed
	}
	cxpPerfReportNamedProcIO(b, "artifact_upsert", total, b.N)
	if b.N > 0 {
		b.ReportMetric(float64(duration)/float64(b.N), "artifact_upsert_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserTranscriptDeliveryStageWrites(b *testing.B) {
	ctx := context.Background()
	var total cxpPerfProcIO
	var duration time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, _ := newCXPPerfRealisticMixedUserFixture(b)
		sessionID := cxpPerfSessionID(i % 100)
		checkpointID := transcriptCheckpointID(sessionID)
		b.StartTimer()
		start := time.Now()
		delta, err := cxpPerfMeasureProcIO(func() error {
			_, _, err := store.RecordTranscriptDelivery(ctx, teamstore.TranscriptDeliveryRecord{
				ID:             fmt.Sprintf("realistic-transcript-delivery-%06d", i),
				SessionID:      sessionID,
				OutboxID:       fmt.Sprintf("realistic-outbox-%03d-%04d", i%100, 0),
				SourceRecordID: fmt.Sprintf("record-%06d", i),
				Status:         teamstore.TranscriptDeliveryStatusSkipped,
			}, teamstore.ImportCheckpoint{
				ID:             checkpointID,
				SessionID:      sessionID,
				SourcePath:     fmt.Sprintf("/tmp/cxp-perf/history/session-%03d.jsonl", i%100),
				LastRecordID:   fmt.Sprintf("record-%06d", i),
				LastSourceLine: 10_000 + i,
				LastOffset:     int64(1_000_000 + i),
				Status:         importCheckpointStatusComplete,
			})
			return err
		})
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatalf("RecordTranscriptDelivery: %v", err)
		}
		total.add(delta)
		duration += elapsed
	}
	cxpPerfReportNamedProcIO(b, "transcript_delivery", total, b.N)
	if b.N > 0 {
		b.ReportMetric(float64(duration)/float64(b.N), "transcript_delivery_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserTranscriptCheckpointStageWrites(b *testing.B) {
	ctx := context.Background()
	var total cxpPerfProcIO
	var duration time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, bridge := newCXPPerfRealisticMixedUserFixture(b)
		session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
		if session == nil {
			b.Fatalf("realistic session missing")
		}
		b.StartTimer()
		start := time.Now()
		delta, err := cxpPerfMeasureProcIO(func() error {
			return bridge.recordTranscriptCheckpointDetailedWithID(
				ctx,
				*session,
				fmt.Sprintf("/tmp/cxp-perf/history/session-%03d.jsonl", i%100),
				fmt.Sprintf("record-%06d", i),
				10_000+i,
				int64(1_000_000+i),
				transcriptCheckpointID(session.ID),
			)
		})
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatalf("recordTranscriptCheckpointDetailedWithID: %v", err)
		}
		total.add(delta)
		duration += elapsed
	}
	cxpPerfReportNamedProcIO(b, "transcript_checkpoint", total, b.N)
	if b.N > 0 {
		b.ReportMetric(float64(duration)/float64(b.N), "transcript_checkpoint_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserTranscriptQueueStageWrites(b *testing.B) {
	ctx := context.Background()
	var total cxpPerfProcIO
	var duration time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, bridge := newCXPPerfRealisticMixedUserFixture(b)
		session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
		if session == nil {
			b.Fatalf("realistic session missing")
		}
		b.StartTimer()
		start := time.Now()
		delta, err := cxpPerfMeasureProcIO(func() error {
			_, _, _, err := store.QueueTranscriptDeliveryOutbox(ctx, teamstore.TranscriptDeliveryQueueRequest{
				Message: teamstore.OutboxMessage{
					ID:              fmt.Sprintf("realistic-transcript-queue-outbox-%06d", i),
					SessionID:       session.ID,
					TurnID:          fmt.Sprintf("realistic-turn-%03d-%05d", i%100, 0),
					TeamsChatID:     session.ChatID,
					ScopeID:         bridge.scope.ID,
					MachineID:       bridge.machine.ID,
					LeaseGeneration: bridge.currentLeaseGeneration(),
					Kind:            "final",
					Body:            "final answer from transcript queue benchmark",
					SourceTextHash:  fmt.Sprintf("hash-%06d", i),
					RenderedBytes:   42,
				},
				Delivery: teamstore.TranscriptDeliveryRecord{
					ID:             fmt.Sprintf("realistic-transcript-queue-delivery-%06d", i),
					SessionID:      session.ID,
					SourcePath:     fmt.Sprintf("/tmp/cxp-perf/history/session-%03d.jsonl", i%100),
					SourceLine:     10_000 + i,
					SourceRecordID: fmt.Sprintf("record-%06d", i),
					Kind:           "final",
					TextHash:       fmt.Sprintf("hash-%06d", i),
					Status:         teamstore.TranscriptDeliveryStatusQueued,
				},
			})
			return err
		})
		elapsed := time.Since(start)
		b.StopTimer()
		if err != nil {
			b.Fatalf("QueueTranscriptDeliveryOutbox: %v", err)
		}
		total.add(delta)
		duration += elapsed
	}
	cxpPerfReportNamedProcIO(b, "transcript_queue", total, b.N)
	if b.N > 0 {
		b.ReportMetric(float64(duration)/float64(b.N), "transcript_queue_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteFullStateSaveSourceStageWrites(b *testing.B) {
	ctx := context.Background()
	type sourceCase struct {
		name  string
		setup func(testing.TB, *teamstore.Store, *Bridge, int) func() error
	}
	cases := []sourceCase{
		{
			name: "row_chat_poll_schedule",
			setup: func(tb testing.TB, store *teamstore.Store, _ *Bridge, i int) func() error {
				tb.Helper()
				chatID := cxpPerfChatID(i % 100)
				return func() error {
					_, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
						ChatID:         chatID,
						PollState:      inboundPollStateWarm,
						NextPollAt:     time.Date(2026, 5, 23, 11, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second),
						LastActivityAt: time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC),
					})
					return err
				}
			},
		},
		{
			name: "row_outbox_queue",
			setup: func(tb testing.TB, store *teamstore.Store, _ *Bridge, i int) func() error {
				tb.Helper()
				sessionID := cxpPerfSessionID(i % 100)
				chatID := cxpPerfChatID(i % 100)
				return func() error {
					_, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
						ID:          fmt.Sprintf("source-matrix-outbox-%06d", i),
						SessionID:   sessionID,
						TeamsChatID: chatID,
						Kind:        "status-progress",
						Body:        "source matrix outbox row update",
					})
					return err
				}
			},
		},
		{
			name: "full_update_session_raw",
			setup: func(tb testing.TB, store *teamstore.Store, _ *Bridge, i int) func() error {
				tb.Helper()
				sessionID := cxpPerfSessionID(i % 100)
				return func() error {
					return store.UpdateSession(ctx, sessionID, func(state *teamstore.State) error {
						session := state.Sessions[sessionID]
						session.Cwd = fmt.Sprintf("/workspace/source-matrix/raw-%06d", i)
						session.UpdatedAt = time.Now()
						state.Sessions[sessionID] = session
						return nil
					})
				}
			},
		},
		{
			name: "full_auto_title_from_result",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
				if session == nil {
					tb.Fatalf("realistic session missing")
				}
				session.TitleSource = sessionTitleSourceAuto
				result := ExecutionResult{
					Text:             "source matrix final",
					CodexThreadID:    firstNonEmptyString(session.CodexThreadID, fmt.Sprintf("source-matrix-thread-%06d", i)),
					CodexThreadTitle: fmt.Sprintf("Source Matrix Title %06d", i),
					CodexTurnID:      fmt.Sprintf("source-matrix-turn-%06d", i),
				}
				return func() error {
					updated, err := bridge.refreshWorkChatTitleFromExecutionResult(ctx, session, result)
					if err != nil {
						return err
					}
					if !updated {
						return fmt.Errorf("auto title update was not applied")
					}
					return err
				}
			},
		},
		{
			name: "full_checkpoint_position_backfill",
			setup: func(tb testing.TB, store *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
				if session == nil {
					tb.Fatalf("realistic session missing")
				}
				threadID := fmt.Sprintf("source-matrix-thread-%06d", i)
				session.CodexThreadID = threadID
				session.Cwd = filepath.Join(tb.TempDir(), "project")
				transcriptPath := filepath.Join(tb.TempDir(), "source-matrix-transcript.jsonl")
				data := cxpPerfTranscriptContent(threadID, 8, 96)
				if err := os.WriteFile(transcriptPath, []byte(data), 0o600); err != nil {
					tb.Fatalf("write checkpoint backfill transcript: %v", err)
				}
				checkpointID := transcriptCheckpointID(session.ID)
				lastRecordID := fmt.Sprintf("record-%s-%04d", threadID, 4)
				checkpoint, changed, err := store.UpdateImportCheckpoint(ctx, checkpointID, func(current teamstore.ImportCheckpoint, _ bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
					current.ID = checkpointID
					current.SessionID = session.ID
					current.SourcePath = transcriptPath
					current.LastRecordID = lastRecordID
					current.LastSourceLine = 0
					current.LastOffset = 0
					current.SourceSize = 0
					current.SourceModTime = time.Time{}
					current.Status = importCheckpointStatusComplete
					current.UpdatedAt = now
					return current, true, nil
				})
				if err != nil {
					tb.Fatalf("seed checkpoint backfill row: %v", err)
				}
				if !changed {
					tb.Fatalf("checkpoint backfill seed did not change")
				}
				local := codexhistory.Session{SessionID: threadID, FilePath: transcriptPath, ProjectPath: session.Cwd}
				return func() error {
					_, applied, err := bridge.backfillLinkedTranscriptCheckpointPosition(ctx, *session, local, checkpoint)
					if err != nil {
						return err
					}
					if !applied {
						return fmt.Errorf("checkpoint position backfill was not applied")
					}
					return nil
				}
			},
		},
		{
			name: "full_park_work_chat",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
				if session == nil {
					tb.Fatalf("realistic session missing")
				}
				return func() error {
					_, err := bridge.parkWorkChatSession(ctx, session)
					return err
				}
			},
		},
		{
			name: "full_resume_work_chat",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
				if session == nil {
					tb.Fatalf("realistic session missing")
				}
				if _, err := bridge.parkWorkChatSession(ctx, session); err != nil {
					tb.Fatalf("seed parked work chat: %v", err)
				}
				return func() error {
					return bridge.resumeWorkChat(ctx, session, time.Date(2026, 5, 23, 11, 30, 0, 0, time.UTC).Add(time.Duration(i)*time.Second))
				}
			},
		},
		{
			name: "full_control_dashboard_persist",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				dashboard := cxpPerfSourceMatrixDashboard(bridge, i)
				return func() error {
					return bridge.persistControlDashboard(ctx, dashboard)
				}
			},
		},
		{
			name: "full_helper_reload_begin",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, _ int) func() error {
				tb.Helper()
				return func() error {
					_, _, blocked, err := bridge.beginHelperReloadDrain(ctx, true)
					if blocked {
						return fmt.Errorf("helper reload drain unexpectedly blocked")
					}
					return err
				}
			},
		},
		{
			name: "full_helper_reload_restore",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, _ int) func() error {
				tb.Helper()
				previous, _, blocked, err := bridge.beginHelperReloadDrain(ctx, true)
				if err != nil {
					tb.Fatalf("seed helper reload drain: %v", err)
				}
				if blocked {
					tb.Fatalf("helper reload drain seed unexpectedly blocked")
				}
				return func() error {
					return bridge.restoreHelperReloadDrain(ctx, previous)
				}
			},
		},
		{
			name: "full_interrupted_restart_notice",
			setup: func(tb testing.TB, store *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				sessionID := cxpPerfSessionID(i % 100)
				turn := teamstore.Turn{
					ID:             fmt.Sprintf("source-matrix-interrupted-%06d", i),
					SessionID:      sessionID,
					Status:         teamstore.TurnStatusInterrupted,
					RecoveryReason: recoveryReasonAmbiguousAfterHelperRestart,
					CreatedAt:      time.Now(),
					UpdatedAt:      time.Now(),
				}
				if err := store.Update(ctx, func(state *teamstore.State) error {
					state.Turns[turn.ID] = turn
					return nil
				}); err != nil {
					tb.Fatalf("seed interrupted turn: %v", err)
				}
				return func() error {
					return bridge.markInterruptedAfterRestartNoticeSent(ctx, turn)
				}
			},
		},
		{
			name: "full_deferred_inbound_ignored",
			setup: func(tb testing.TB, store *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				inboundID := fmt.Sprintf("source-matrix-deferred-%06d", i)
				if err := store.Update(ctx, func(state *teamstore.State) error {
					state.InboundEvents[inboundID] = teamstore.InboundEvent{
						ID:             inboundID,
						SessionID:      cxpPerfSessionID(i % 100),
						TeamsChatID:    cxpPerfChatID(i % 100),
						TeamsMessageID: fmt.Sprintf("source-matrix-deferred-message-%06d", i),
						Text:           "deferred source matrix input",
						Status:         teamstore.InboundStatusDeferred,
						CreatedAt:      time.Now(),
						UpdatedAt:      time.Now(),
					}
					return nil
				}); err != nil {
					tb.Fatalf("seed deferred inbound: %v", err)
				}
				return func() error {
					return bridge.markDeferredInboundIgnored(ctx, inboundID, "source matrix unsupported input")
				}
			},
		},
		{
			name: "full_rename_session_chat",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
				if session == nil {
					tb.Fatalf("realistic session missing")
				}
				return func() error {
					return bridge.renameSessionChat(ctx, session, fmt.Sprintf("Source Matrix Rename %06d", i))
				}
			},
		},
		{
			name: "full_registry_projection_migration",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				chatID := fmt.Sprintf("source-matrix-registry-chat-%06d", i)
				if bridge.reg.Chats == nil {
					bridge.reg.Chats = map[string]ChatState{}
				}
				bridge.reg.Chats[chatID] = ChatState{
					SeenMessageIDs: []string{fmt.Sprintf("source-matrix-seen-%06d", i)},
					SentMessageIDs: []string{fmt.Sprintf("source-matrix-sent-%06d", i)},
				}
				return func() error {
					return bridge.migrateRegistryProjectionToStore(ctx)
				}
			},
		},
		{
			name: "full_control_chat_binding",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				chat := Chat{
					ID:     bridge.reg.ControlChatID,
					Topic:  fmt.Sprintf("source matrix control %06d", i),
					WebURL: fmt.Sprintf("https://teams.example/control-%06d", i),
				}
				return func() error {
					return bridge.recordControlChatBindingWithTitle(ctx, chat, fmt.Sprintf("Control %06d", i), sessionTitleSourceUser)
				}
			},
		},
		{
			name: "full_close_durable_session",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
				if session == nil {
					tb.Fatalf("realistic session missing")
				}
				session.UpdatedAt = time.Now()
				return func() error {
					return bridge.closeDurableSession(ctx, session)
				}
			},
		},
		{
			name: "full_clear_dashboard_view",
			setup: func(tb testing.TB, _ *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				if err := bridge.persistControlDashboard(ctx, cxpPerfSourceMatrixDashboard(bridge, i)); err != nil {
					tb.Fatalf("seed dashboard view: %v", err)
				}
				return func() error {
					return bridge.clearControlDashboardView(ctx)
				}
			},
		},
		{
			name: "full_model_key_intake_pending",
			setup: func(tb testing.TB, store *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				intake := teamstore.ModelProfileKeyIntake{
					ID:               fmt.Sprintf("source-matrix-model-key-%06d", i),
					ScopeID:          bridge.scope.ID,
					TeamsChatID:      bridge.reg.ControlChatID,
					RequestMessageID: fmt.Sprintf("source-matrix-model-key-message-%06d", i),
					AuthorUserID:     bridge.user.ID,
					ProfileName:      "source-matrix",
					Provider:         "openai",
					Model:            "gpt-5",
					Status:           teamstore.ModelProfileKeyIntakePending,
					CreatedAt:        time.Now(),
					UpdatedAt:        time.Now(),
					ExpiresAt:        time.Now().Add(time.Hour),
				}
				return func() error {
					return store.UpsertModelProfileKeyIntake(ctx, intake)
				}
			},
		},
		{
			name: "full_workflow_config",
			setup: func(tb testing.TB, store *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				webhookPath := filepath.Join(tb.TempDir(), "workflow-webhook-url")
				if err := os.WriteFile(webhookPath, []byte("https://workflow.example.test/source-matrix"), 0o600); err != nil {
					tb.Fatalf("write workflow webhook fixture: %v", err)
				}
				return func() error {
					_, err := store.UpdateWorkflowConfig(ctx, func(_ teamstore.WorkflowNotificationConfig, _ teamstore.ControlChatBinding, _ time.Time) (teamstore.WorkflowNotificationConfig, bool, error) {
						return teamstore.WorkflowNotificationConfig{
							Enabled:               true,
							ControlWebhookURLFile: webhookPath,
							ControlChatID:         bridge.reg.ControlChatID,
							UpdatedAt:             time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second),
						}, true, nil
					})
					return err
				}
			},
		},
		{
			name: "full_publish_session_metadata",
			setup: func(tb testing.TB, store *teamstore.Store, bridge *Bridge, i int) func() error {
				tb.Helper()
				session := bridge.reg.SessionByID(cxpPerfSessionID(i % 100))
				if session == nil {
					tb.Fatalf("realistic session missing")
				}
				localThreadID := fmt.Sprintf("source-matrix-publish-thread-%06d", i)
				projectPath := fmt.Sprintf("/workspace/source-matrix-publish-%06d", i)
				return func() error {
					_, _, err := store.UpdateSessionContext(ctx, session.ID, func(current teamstore.SessionContext, found bool, now time.Time) (teamstore.SessionContext, bool, error) {
						if !found {
							return current, false, fmt.Errorf("session %q not found", session.ID)
						}
						current.Cwd = projectPath
						current.CodexThreadID = localThreadID
						current.UpdatedAt = now
						return current, true, nil
					})
					return err
				}
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			var total cxpPerfProcIO
			var duration time.Duration
			var storeBytes int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store, bridge := newCXPPerfRealisticMixedUserFixture(b)
				op := tc.setup(b, store, bridge, i)
				storeBytes += cxpPerfSQLiteStoreBytes(b, store)
				b.StartTimer()
				start := time.Now()
				delta, err := cxpPerfMeasureProcIO(op)
				elapsed := time.Since(start)
				b.StopTimer()
				if err != nil {
					b.Fatalf("%s: %v", tc.name, err)
				}
				total.add(delta)
				duration += elapsed
			}
			cxpPerfReportNamedProcIO(b, "source", total, b.N)
			if b.N > 0 {
				b.ReportMetric(float64(duration)/float64(b.N), "source_ns/op")
				b.ReportMetric(float64(storeBytes)/float64(b.N), "sqlite_total_bytes/op")
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserWALSpikeBreakdown(b *testing.B) {
	const rowBurstUpdates = 512
	stages := []string{
		"row_poll_schedule_burst",
		"checkpoint_after_row_burst",
		"full_session_update",
		"checkpoint_after_full_session_update",
		"full_registry_projection_migration",
		"checkpoint_after_registry_migration",
	}
	totals := make(map[string]cxpPerfProcIO, len(stages))
	durations := make(map[string]time.Duration, len(stages))
	walBefore := make(map[string]int64, len(stages))
	walAfter := make(map[string]int64, len(stages))
	storeBytes := int64(0)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, bridge := newCXPPerfRealisticMixedUserFixture(b)
		storeBytes += cxpPerfSQLiteStoreBytes(b, store)
		base := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Hour)
		runStage := func(name string, fn func() error) {
			b.Helper()
			walBefore[name] += cxpPerfSQLiteWALBytes(b, store)
			b.StartTimer()
			start := time.Now()
			delta, err := cxpPerfMeasureProcIO(fn)
			elapsed := time.Since(start)
			b.StopTimer()
			walAfter[name] += cxpPerfSQLiteWALBytes(b, store)
			if err != nil {
				b.Fatalf("%s: %v", name, err)
			}
			total := totals[name]
			total.add(delta)
			totals[name] = total
			durations[name] += elapsed
		}
		runStage("row_poll_schedule_burst", func() error {
			for j := 0; j < rowBurstUpdates; j++ {
				chatID := cxpPerfChatID(j % 100)
				if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
					ChatID:         chatID,
					PollState:      inboundPollStateWarm,
					NextPollAt:     base.Add(time.Duration(j) * time.Second),
					LastActivityAt: base,
				}); err != nil {
					return err
				}
			}
			return nil
		})
		runStage("checkpoint_after_row_burst", func() error {
			_, err := store.CheckpointSQLiteWAL(ctx, 1)
			return err
		})
		runStage("full_session_update", func() error {
			sessionID := cxpPerfSessionID(0)
			return store.UpdateSession(ctx, sessionID, func(state *teamstore.State) error {
				session := state.Sessions[sessionID]
				session.Cwd = fmt.Sprintf("/workspace/wal-spike/full-session-%06d", i)
				session.UpdatedAt = time.Now()
				state.Sessions[sessionID] = session
				return nil
			})
		})
		runStage("checkpoint_after_full_session_update", func() error {
			bridge.lastSQLiteWALCheckpoint = time.Time{}
			return bridge.maybeCheckpointSQLiteWAL(ctx, base.Add(6*time.Minute))
		})
		runStage("full_registry_projection_migration", func() error {
			chatID := fmt.Sprintf("wal-spike-registry-chat-%06d", i)
			if bridge.reg.Chats == nil {
				bridge.reg.Chats = map[string]ChatState{}
			}
			bridge.reg.Chats[chatID] = ChatState{
				SeenMessageIDs: []string{fmt.Sprintf("wal-spike-seen-%06d", i)},
				SentMessageIDs: []string{fmt.Sprintf("wal-spike-sent-%06d", i)},
			}
			return bridge.migrateRegistryProjectionToStore(ctx)
		})
		runStage("checkpoint_after_registry_migration", func() error {
			bridge.lastSQLiteWALCheckpoint = time.Time{}
			return bridge.maybeCheckpointSQLiteWAL(ctx, base.Add(12*time.Minute))
		})
	}
	for _, stage := range stages {
		cxpPerfReportNamedProcIO(b, stage, totals[stage], b.N)
		if b.N > 0 {
			denom := float64(b.N)
			b.ReportMetric(float64(durations[stage])/denom, stage+"_ns/op")
			b.ReportMetric(float64(walBefore[stage])/denom, stage+"_wal_before_B/op")
			b.ReportMetric(float64(walAfter[stage])/denom, stage+"_wal_after_B/op")
		}
	}
	if b.N > 0 {
		b.ReportMetric(float64(storeBytes)/float64(b.N), "sqlite_total_bytes/op")
	}
}

func cxpPerfSourceMatrixDashboard(bridge *Bridge, index int) ControlDashboard {
	now := time.Date(2026, 5, 23, 11, 45, 0, 0, time.UTC).Add(time.Duration(index) * time.Second)
	workspaces := make([]DashboardWorkspaceInput, 0, 10)
	for workspace := 0; workspace < 10; workspace++ {
		input := DashboardWorkspaceInput{
			ID:        fmt.Sprintf("source-matrix-workspace-%02d", workspace),
			Path:      fmt.Sprintf("/workspace/source-matrix-%02d", workspace),
			UpdatedAt: now.Add(-time.Duration(workspace) * time.Minute),
		}
		for sessionIndex := workspace * 10; sessionIndex < (workspace+1)*10 && sessionIndex < len(bridge.reg.Sessions); sessionIndex++ {
			session := bridge.reg.Sessions[sessionIndex]
			input.Sessions = append(input.Sessions, DashboardSessionInput{
				ID:            session.ID,
				WorkspaceID:   input.ID,
				Cwd:           firstNonEmptyString(session.Cwd, fmt.Sprintf("/workspace/source-matrix-%02d", workspace)),
				Topic:         session.Topic,
				Status:        session.Status,
				TeamsChatID:   session.ChatID,
				TeamsChatURL:  session.ChatURL,
				CodexThreadID: session.CodexThreadID,
				CreatedAt:     session.CreatedAt,
				UpdatedAt:     session.UpdatedAt,
			})
		}
		workspaces = append(workspaces, input)
	}
	return BuildControlDashboard(ControlDashboard{}, ControlDashboardInput{
		Workspaces:          workspaces,
		ViewKind:            DashboardViewSessions,
		SelectedWorkspaceID: workspaces[0].ID,
	}, now)
}

func BenchmarkCXPPerfModelSQLiteNewChatFirstMessageStageWrites(b *testing.B) {
	stages := []string{"importing", "service_control", "duplicate", "queue_state", "prepare_local", "ensure", "inbound", "turn", "ack", "start", "complete"}
	totals := make(map[string]cxpPerfProcIO, len(stages))
	durations := make(map[string]time.Duration, len(stages))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, bridge := newCXPPerfRealisticMixedUserFixture(b)
		executor := newCXPPerfBlockingExecutor()
		bridge.executor = executor
		now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute)
		session := &Session{
			ID:        fmt.Sprintf("realistic-new-session-%06d", i),
			ChatID:    fmt.Sprintf("realistic-new-chat-%06d", i),
			ChatURL:   fmt.Sprintf("https://teams.example/realistic-new-chat-%06d", i),
			Topic:     "new chat",
			Status:    "active",
			Cwd:       "/workspace/new-chat",
			CreatedAt: now,
			UpdatedAt: now,
		}
		bridge.reg.Sessions = append(bridge.reg.Sessions, *session)
		msg := ChatMessage{
			ID:              fmt.Sprintf("realistic-new-message-%06d", i),
			ChatID:          session.ChatID,
			CreatedDateTime: now.Format(time.RFC3339),
		}
		msg.Body.ContentType = "html"
		msg.Body.Content = "<p>new chat user message</p>"
		text := "new chat user message"
		var inbound teamstore.InboundEvent
		var turn teamstore.Turn
		runStage := func(name string, fn func() error) {
			b.Helper()
			b.StartTimer()
			start := time.Now()
			delta, err := cxpPerfMeasureProcIO(fn)
			elapsed := time.Since(start)
			b.StopTimer()
			total := totals[name]
			total.add(delta)
			totals[name] = total
			durations[name] += elapsed
			if err != nil {
				b.Fatalf("%s: %v", name, err)
			}
		}
		runStage("importing", func() error {
			if importing, err := bridge.sessionTranscriptImportInProgress(ctx, session.ID); err != nil {
				return err
			} else if importing {
				return fmt.Errorf("session unexpectedly importing transcript")
			}
			return nil
		})
		runStage("service_control", func() error {
			if control, blocked, err := bridge.serviceControlBlocksNewWork(ctx); err != nil {
				return err
			} else if blocked {
				return fmt.Errorf("service control unexpectedly blocked work: %#v", control)
			}
			return nil
		})
		runStage("duplicate", func() error {
			if duplicate, err := bridge.ignoreRecentDuplicateSessionPrompt(ctx, session, msg, text); err != nil {
				return err
			} else if duplicate {
				return fmt.Errorf("message unexpectedly classified as duplicate")
			}
			return nil
		})
		runStage("queue_state", func() error {
			turns, err := bridge.sessionTurnQueueState(ctx, session.ID)
			if err != nil {
				return err
			}
			if turns.Running || turns.Queued != 0 {
				return fmt.Errorf("session unexpectedly has queued/running turns: %#v", turns)
			}
			return nil
		})
		runStage("prepare_local", func() error {
			gate, err := bridge.prepareLocalCodexBeforeTeamsTurn(ctx, session)
			if err != nil {
				return err
			}
			if gate.Block {
				return fmt.Errorf("local codex gate unexpectedly blocked: %s", gate.AckBody)
			}
			return nil
		})
		runStage("ensure", func() error {
			return bridge.ensureDurableSession(ctx, session)
		})
		runStage("inbound", func() error {
			var created bool
			var err error
			inbound, created, err = bridge.persistInbound(ctx, session, msg)
			if err != nil {
				return err
			}
			if !created {
				return fmt.Errorf("realistic new inbound was not created")
			}
			return nil
		})
		runStage("turn", func() error {
			var created bool
			var err error
			turn, created, err = bridge.queueTurn(ctx, session, inbound)
			if err != nil {
				return err
			}
			if !created {
				return fmt.Errorf("realistic new turn was not created")
			}
			session.UpdatedAt = time.Now()
			bridge.markRegistryProjectionDirty()
			return nil
		})
		runStage("ack", func() error {
			return bridge.queueTeamsPromptAckForMessage(ctx, session, turn, msg, false)
		})
		runStage("start", func() error {
			started, err := bridge.startQueuedTurn(ctx, session, turn.ID, func(runCtx context.Context, runSession *Session, claimed teamstore.Turn) error {
				return bridge.runPreparedQueuedTurnFromMessage(runCtx, runSession, claimed, runSession.ChatID, msg, text, bridge.executor)
			})
			if err != nil {
				return err
			}
			if !started {
				return fmt.Errorf("turn was not started")
			}
			executor.waitStarted(b)
			return nil
		})
		runStage("complete", func() error {
			executor.release()
			return cxpPerfDrainAsyncTurns(ctx, bridge)
		})
	}
	for _, stage := range stages {
		cxpPerfReportNamedProcIO(b, stage, totals[stage], b.N)
		b.ReportMetric(float64(durations[stage])/float64(b.N), stage+"_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteNewChatCompleteTurnBreakdown(b *testing.B) {
	stages := []string{
		"bind_thread",
		"transcript_final",
		"pre_final_status",
		"queue_final_outbox",
		"mark_completed",
		"flush_final_outbox",
		"title_from_result",
		"queue_state_fallback",
		"artifact_scan",
	}
	totals := make(map[string]cxpPerfProcIO, len(stages))
	durations := make(map[string]time.Duration, len(stages))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, bridge := newCXPPerfRealisticMixedUserFixture(b)
		now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute)
		session := &Session{
			ID:        fmt.Sprintf("realistic-new-complete-session-%06d", i),
			ChatID:    fmt.Sprintf("realistic-new-complete-chat-%06d", i),
			ChatURL:   fmt.Sprintf("https://teams.example/realistic-new-complete-chat-%06d", i),
			Topic:     "new chat",
			Status:    "active",
			Cwd:       "/workspace/new-chat",
			CreatedAt: now,
			UpdatedAt: now,
		}
		bridge.reg.Sessions = append(bridge.reg.Sessions, *session)
		msg := ChatMessage{
			ID:              fmt.Sprintf("realistic-new-complete-message-%06d", i),
			ChatID:          session.ChatID,
			CreatedDateTime: now.Format(time.RFC3339),
		}
		msg.Body.ContentType = "html"
		msg.Body.Content = "<p>new chat user message</p>"
		text := "new chat user message"
		if err := bridge.ensureDurableSession(ctx, session); err != nil {
			b.Fatalf("ensure durable session: %v", err)
		}
		inbound, created, err := bridge.persistInbound(ctx, session, msg)
		if err != nil {
			b.Fatalf("persist inbound: %v", err)
		}
		if !created {
			b.Fatal("new inbound was not created")
		}
		turn, turnCreated, err := bridge.queueTurn(ctx, session, inbound)
		if err != nil {
			b.Fatalf("queue turn: %v", err)
		}
		if !turnCreated {
			b.Fatal("new turn was not created")
		}
		if err := bridge.queueTeamsPromptAckForMessage(ctx, session, turn, msg, false); err != nil {
			b.Fatalf("queue ack: %v", err)
		}
		turn, err = bridge.store.MarkTurnRunning(ctx, turn.ID, session.CodexThreadID, "")
		if err != nil {
			b.Fatalf("mark running: %v", err)
		}
		result := ExecutionResult{
			Text:             "perf codex result for " + strings.TrimSpace(text),
			CodexThreadID:    firstNonEmptyString(session.CodexThreadID, "perf-thread-"+session.ID),
			CodexThreadTitle: "Perf Thread",
			CodexTurnID:      "perf-codex-turn-" + shortStableID(session.ID+":"+text),
		}
		runStage := func(name string, fn func() error) {
			b.Helper()
			b.StartTimer()
			start := time.Now()
			delta, err := cxpPerfMeasureProcIO(fn)
			elapsed := time.Since(start)
			b.StopTimer()
			total := totals[name]
			total.add(delta)
			totals[name] = total
			durations[name] += elapsed
			if err != nil {
				b.Fatalf("%s: %v", name, err)
			}
		}
		runStage("bind_thread", func() error {
			blocked, err := bridge.bindObservedCodexThreadOrInterrupt(ctx, session, turn, result.CodexThreadID, "runner_completed")
			if err != nil {
				return err
			}
			if blocked {
				return fmt.Errorf("thread bind unexpectedly blocked completion")
			}
			return nil
		})
		runStage("transcript_final", func() error {
			if transcriptResult, ok := bridge.completedTurnResultFromLinkedTranscript(ctx, session, turn, result); ok {
				result = executionResultWithTranscriptFinal(result, transcriptResult)
			}
			return nil
		})
		runStage("pre_final_status", func() error {
			_, err := bridge.queueActiveTurnTranscriptStatusBeforeFinal(ctx, session, turn)
			return err
		})
		runStage("queue_final_outbox", func() error {
			visibleText := StripOAIMemoryCitationBlocks(StripHelperPromptEchoes(StripArtifactManifestBlocks(result.Text)))
			if visibleText == "" && len(ExtractArtifactManifestBlocks(result.Text)) > 0 {
				visibleText = "artifact manifest received; uploading listed files."
			}
			_, err := bridge.queueOutboxChunksWithOptions(ctx, session.ID, turn.ID, session.ChatID, "final", visibleText, outboxQueueOptions{
				MentionOwner:     true,
				NotificationKind: "turn_completed",
			})
			return err
		})
		runStage("mark_completed", func() error {
			session.UpdatedAt = time.Now()
			bridge.markRegistryProjectionDirty()
			_, err := bridge.store.MarkTurnCompleted(ctx, turn.ID, firstNonEmptyString(result.CodexThreadID, session.CodexThreadID), result.CodexTurnID)
			return err
		})
		runStage("flush_final_outbox", func() error {
			if err := bridge.flushPendingOutboxForChat(ctx, session.ChatID); err != nil {
				if isOutboxDeliveryDeferred(err) || isGraphTransientServerError(err) {
					return nil
				}
				return err
			}
			bridge.boostPolling(time.Now())
			return nil
		})
		updatedTitle := false
		runStage("title_from_result", func() error {
			var err error
			updatedTitle, err = bridge.refreshWorkChatTitleFromExecutionResult(ctx, session, result)
			return err
		})
		runStage("queue_state_fallback", func() error {
			if updatedTitle {
				return nil
			}
			queueState, err := bridge.sessionTurnQueueState(ctx, session.ID)
			if err != nil {
				return err
			}
			if queueState.Queued == 0 {
				return bridge.refreshWorkChatTitleFromCodexHistory(ctx, session)
			}
			return nil
		})
		runStage("artifact_scan", func() error {
			return bridge.uploadArtifactsFromResult(ctx, session, turn, result.Text)
		})
	}
	for _, stage := range stages {
		cxpPerfReportNamedProcIO(b, stage, totals[stage], b.N)
		b.ReportMetric(float64(durations[stage])/float64(b.N), stage+"_ns/op")
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserCompleteTurnBreakdown(b *testing.B) {
	stages := []string{
		"bind_thread",
		"transcript_final",
		"pre_final_status",
		"queue_final_outbox",
		"mark_completed",
		"flush_final_outbox",
		"title_from_result",
		"queue_state_fallback",
		"artifact_scan",
	}
	totals := make(map[string]cxpPerfProcIO, len(stages))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, bridge := newCXPPerfRealisticMixedUserFixture(b)
		chatID, msg, text := cxpPerfRealisticUserMessage(i)
		session := bridge.reg.SessionByChatID(chatID)
		if session == nil {
			b.Fatalf("realistic session for chat %s not found", chatID)
		}
		if err := bridge.ensureDurableSession(ctx, session); err != nil {
			b.Fatalf("ensure durable session: %v", err)
		}
		inbound, created, err := bridge.persistInbound(ctx, session, msg)
		if err != nil {
			b.Fatalf("persist inbound: %v", err)
		}
		if !created {
			b.Fatal("realistic inbound was not created")
		}
		turn, turnCreated, err := bridge.queueTurn(ctx, session, inbound)
		if err != nil {
			b.Fatalf("queue turn: %v", err)
		}
		if !turnCreated {
			b.Fatal("realistic turn was not created")
		}
		if err := bridge.queueTeamsPromptAckForMessage(ctx, session, turn, msg, false); err != nil {
			b.Fatalf("queue ack: %v", err)
		}
		turn, err = bridge.store.MarkTurnRunning(ctx, turn.ID, session.CodexThreadID, "")
		if err != nil {
			b.Fatalf("mark running: %v", err)
		}
		result := ExecutionResult{
			Text:             "perf codex result for " + strings.TrimSpace(text),
			CodexThreadID:    firstNonEmptyString(session.CodexThreadID, "perf-thread-"+session.ID),
			CodexThreadTitle: "Perf Thread",
			CodexTurnID:      "perf-codex-turn-" + shortStableID(session.ID+":"+text),
		}
		runStage := func(name string, fn func() error) {
			b.Helper()
			b.StartTimer()
			delta, err := cxpPerfMeasureProcIO(fn)
			b.StopTimer()
			total := totals[name]
			total.add(delta)
			totals[name] = total
			if err != nil {
				b.Fatalf("%s: %v", name, err)
			}
		}
		runStage("bind_thread", func() error {
			blocked, err := bridge.bindObservedCodexThreadOrInterrupt(ctx, session, turn, result.CodexThreadID, "runner_completed")
			if err != nil {
				return err
			}
			if blocked {
				return fmt.Errorf("thread bind unexpectedly blocked completion")
			}
			return nil
		})
		runStage("transcript_final", func() error {
			if transcriptResult, ok := bridge.completedTurnResultFromLinkedTranscript(ctx, session, turn, result); ok {
				result = executionResultWithTranscriptFinal(result, transcriptResult)
			}
			return nil
		})
		runStage("pre_final_status", func() error {
			_, err := bridge.queueActiveTurnTranscriptStatusBeforeFinal(ctx, session, turn)
			return err
		})
		runStage("queue_final_outbox", func() error {
			visibleText := StripOAIMemoryCitationBlocks(StripHelperPromptEchoes(StripArtifactManifestBlocks(result.Text)))
			if visibleText == "" && len(ExtractArtifactManifestBlocks(result.Text)) > 0 {
				visibleText = "artifact manifest received; uploading listed files."
			}
			_, err := bridge.queueOutboxChunksWithOptions(ctx, session.ID, turn.ID, chatID, "final", visibleText, outboxQueueOptions{
				MentionOwner:     true,
				NotificationKind: "turn_completed",
			})
			return err
		})
		runStage("mark_completed", func() error {
			session.UpdatedAt = time.Now()
			bridge.markRegistryProjectionDirty()
			_, err := bridge.store.MarkTurnCompleted(ctx, turn.ID, firstNonEmptyString(result.CodexThreadID, session.CodexThreadID), result.CodexTurnID)
			return err
		})
		runStage("flush_final_outbox", func() error {
			if err := bridge.flushPendingOutboxForChat(ctx, chatID); err != nil {
				if isOutboxDeliveryDeferred(err) || isGraphTransientServerError(err) {
					return nil
				}
				return err
			}
			bridge.boostPolling(time.Now())
			return nil
		})
		updatedTitle := false
		runStage("title_from_result", func() error {
			var err error
			updatedTitle, err = bridge.refreshWorkChatTitleFromExecutionResult(ctx, session, result)
			return err
		})
		runStage("queue_state_fallback", func() error {
			if updatedTitle {
				return nil
			}
			queueState, err := bridge.sessionTurnQueueState(ctx, session.ID)
			if err != nil {
				return err
			}
			if queueState.Queued == 0 {
				return bridge.refreshWorkChatTitleFromCodexHistory(ctx, session)
			}
			return nil
		})
		runStage("artifact_scan", func() error {
			return bridge.uploadArtifactsFromResult(ctx, session, turn, result.Text)
		})
	}
	for _, stage := range stages {
		cxpPerfReportNamedProcIO(b, stage, totals[stage], b.N)
	}
}

// BenchmarkCXPPerfModelSQLiteLongTranscriptFinalArrival models the failure mode
// seen in large Teams sessions: the Codex JSONL already contains the final
// answer, but the helper still has to reconcile a very long linked transcript,
// a large SQLite store, many active/parked chats, and many live progress
// messages that were already delivered before the final can be sent.
func BenchmarkCXPPerfModelSQLiteLongTranscriptFinalArrival(b *testing.B) {
	stages := []string{
		"transcript_final",
		"pre_final_status",
		"queue_final_outbox",
		"mark_completed",
		"flush_final_outbox",
	}
	cases := []struct {
		name              string
		observedCodexTurn bool
	}{
		{name: "observed-turn-id", observedCodexTurn: true},
		{name: "missing-turn-id", observedCodexTurn: false},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			totals := make(map[string]cxpPerfProcIO, len(stages))
			durations := make(map[string]time.Duration, len(stages))
			var totalQueued int
			var totalSkipped int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				_, bridge, session, turn, result := newCXPPerfLongTranscriptFinalArrivalFixture(b, i)
				if !tc.observedCodexTurn {
					result.CodexTurnID = ""
				}
				preFinalQueued := 0
				runStage := func(name string, fn func() error) {
					b.Helper()
					b.StartTimer()
					start := time.Now()
					delta, err := cxpPerfMeasureProcIO(fn)
					elapsed := time.Since(start)
					b.StopTimer()
					total := totals[name]
					total.add(delta)
					totals[name] = total
					durations[name] += elapsed
					if err != nil {
						b.Fatalf("%s: %v", name, err)
					}
				}
				runStage("transcript_final", func() error {
					if transcriptResult, ok := bridge.completedTurnResultFromLinkedTranscript(ctx, session, turn, result); ok {
						result = executionResultWithTranscriptFinal(result, transcriptResult)
					}
					return nil
				})
				runStage("pre_final_status", func() error {
					queued, err := bridge.queueActiveTurnTranscriptStatusBeforeFinal(ctx, session, turn)
					if err != nil {
						return err
					}
					preFinalQueued = queued
					return nil
				})
				totalQueued += preFinalQueued
				totalSkipped += cxpPerfCountSessionTranscriptDeliveries(b, bridge.store, session.ID, teamstore.TranscriptDeliveryStatusSkipped)
				runStage("queue_final_outbox", func() error {
					visibleText := StripOAIMemoryCitationBlocks(StripHelperPromptEchoes(StripArtifactManifestBlocks(result.Text)))
					if visibleText == "" && len(ExtractArtifactManifestBlocks(result.Text)) > 0 {
						visibleText = "artifact manifest received; uploading listed files."
					}
					_, err := bridge.queueOutboxChunksWithOptions(ctx, session.ID, turn.ID, session.ChatID, "final", visibleText, outboxQueueOptions{
						MentionOwner:     true,
						NotificationKind: "turn_completed",
					})
					return err
				})
				runStage("mark_completed", func() error {
					session.UpdatedAt = time.Now()
					bridge.markRegistryProjectionDirty()
					_, err := bridge.store.MarkTurnCompleted(ctx, turn.ID, firstNonEmptyString(result.CodexThreadID, session.CodexThreadID), result.CodexTurnID)
					return err
				})
				runStage("flush_final_outbox", func() error {
					if err := bridge.flushPendingOutboxForChat(ctx, session.ChatID); err != nil {
						if isOutboxDeliveryDeferred(err) || isGraphTransientServerError(err) {
							return nil
						}
						return err
					}
					bridge.boostPolling(time.Now())
					return nil
				})
			}
			for _, stage := range stages {
				cxpPerfReportNamedProcIO(b, stage, totals[stage], b.N)
				b.ReportMetric(float64(durations[stage])/float64(b.N), stage+"_ns/op")
			}
			if b.N > 0 {
				b.ReportMetric(float64(totalQueued)/float64(b.N), "pre_final_status_queued/op")
				b.ReportMetric(float64(totalSkipped)/float64(b.N), "skipped_transcript_deliveries/op")
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteStatusOnlyFinalArrival(b *testing.B) {
	ctx := context.Background()
	var total cxpPerfProcIO
	var duration time.Duration
	var totalQueued int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, bridge, session, turn := newCXPPerfStatusOnlyFinalArrivalFixture(b, i)
		b.StartTimer()
		start := time.Now()
		delta, err := cxpPerfMeasureProcIO(func() error {
			queued, err := bridge.queueActiveTurnTranscriptStatusBeforeFinal(ctx, session, turn)
			totalQueued += queued
			return err
		})
		elapsed := time.Since(start)
		b.StopTimer()
		total.add(delta)
		duration += elapsed
		if err != nil {
			b.Fatalf("queueActiveTurnTranscriptStatusBeforeFinal: %v", err)
		}
		state, err := store.Load(ctx)
		if err != nil {
			b.Fatalf("Load state: %v", err)
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
		if checkpoint.LastRecordID != "perf-status-only-final" {
			b.Fatalf("checkpoint = %#v, want final", checkpoint)
		}
	}
	cxpPerfReportNamedProcIO(b, "pre_final_status", total, b.N)
	if b.N > 0 {
		b.ReportMetric(float64(duration)/float64(b.N), "pre_final_status_ns/op")
		b.ReportMetric(float64(totalQueued)/float64(b.N), "pre_final_status_queued/op")
	}
}

func BenchmarkCXPPerfModelSQLiteLongTranscriptLegacyPreFinalStatusBreakdown(b *testing.B) {
	// This intentionally exercises the old final-blocking status backfill path:
	// queue a handful of stale progress deliveries, then record each checkpoint
	// separately. The current completion path should be measured by
	// BenchmarkCXPPerfModelSQLiteLongTranscriptFinalArrival instead.
	stages := []string{
		"load_checkpoint",
		"dedupe_snapshot",
		"read_delta",
		"build_dedupe",
		"collect_first_8",
		"queue_delivery_outbox_8",
		"record_checkpoint_8",
	}
	type queuedStatusRecord struct {
		record TranscriptRecord
		line   int
		offset int64
		kind   string
		body   string
	}
	ctx := context.Background()
	totals := make(map[string]cxpPerfProcIO, len(stages))
	durations := make(map[string]time.Duration, len(stages))
	var totalQueued int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, bridge, session, turn, _ := newCXPPerfLongTranscriptFinalArrivalFixture(b, i)
		checkpointID := transcriptCheckpointID(session.ID)
		var checkpoint teamstore.ImportCheckpoint
		var state teamstore.State
		var local codexhistory.Session
		var transcript Transcript
		var teamsOriginHashes map[string]bool
		var teamsOriginDisplays map[string]string
		var known *knownTranscriptOutboxDedupeState
		var dedupe *transcriptDedupeState
		var selected []queuedStatusRecord
		runStage := func(name string, fn func() error) {
			b.Helper()
			b.StartTimer()
			start := time.Now()
			delta, err := cxpPerfMeasureProcIO(fn)
			elapsed := time.Since(start)
			b.StopTimer()
			total := totals[name]
			total.add(delta)
			totals[name] = total
			durations[name] += elapsed
			if err != nil {
				b.Fatalf("%s: %v", name, err)
			}
		}
		runStage("load_checkpoint", func() error {
			var err error
			var found bool
			checkpoint, found, err = bridge.store.ImportCheckpoint(ctx, checkpointID)
			if err != nil {
				return err
			}
			if !found || strings.TrimSpace(checkpoint.LastRecordID) == "" {
				return fmt.Errorf("long final checkpoint missing")
			}
			return nil
		})
		runStage("dedupe_snapshot", func() error {
			var err error
			state, err = bridge.store.SessionTranscriptDedupeSnapshot(ctx, session.ID, checkpointID)
			if err != nil {
				return err
			}
			checkpoint = state.ImportCheckpoints[checkpointID]
			return nil
		})
		runStage("read_delta", func() error {
			var ok bool
			local, ok = linkedTranscriptLocalFromCheckpoint(*session, checkpoint)
			if !ok {
				return fmt.Errorf("long final linked transcript missing")
			}
			var err error
			transcript, err = bridge.readLinkedTranscriptDelta(local.FilePath, checkpoint, firstNonEmptyString(local.SessionID, session.CodexThreadID), session.CodexThreadID)
			if err != nil {
				return err
			}
			if len(transcript.Records) == 0 {
				return fmt.Errorf("long final transcript delta is empty")
			}
			return nil
		})
		runStage("build_dedupe", func() error {
			teamsOriginHashes = teamsOriginTextHashes(state, session.ID)
			teamsOriginDisplays = teamsOriginDisplayTexts(state, session.ID)
			known = newKnownTranscriptOutboxDedupeState(state, session.ID, checkpoint.UpdatedAt)
			dedupe = newTranscriptDedupeState()
			return nil
		})
		runStage("collect_first_8", func() error {
			for i, record := range transcript.Records {
				checkpointLine, checkpointOffset := transcriptCheckpointPositionForRecord(transcript.Records, i)
				record.SourceLine = checkpointLine
				record.SourceOffset = checkpointOffset
				if record.Kind == TranscriptKindAssistant || transcriptRecordIsTerminal(record) {
					break
				}
				body := formatTranscriptRecordForTeams(record)
				body = teamsOriginTranscriptUserDisplayBody(record, body, teamsOriginDisplays)
				if strings.TrimSpace(body) == "" || shouldSkipBackgroundTranscriptRecord(record) {
					continue
				}
				if shouldSkipTeamsOriginTranscriptRecord(record, body, teamsOriginHashes) || dedupe.shouldSkip(record, body) {
					continue
				}
				kind := transcriptRecordOutboxKind("codex", record, i+1)
				if known.shouldSkip(record, body) {
					if record.Kind == TranscriptKindStatus || record.Kind == TranscriptKindCompact {
						if err := bridge.recordSkippedTranscriptDelivery(ctx, *session, local, record, checkpointLine, checkpointOffset, kind, body); err != nil {
							return err
						}
					}
					continue
				}
				switch record.Kind {
				case TranscriptKindStatus, TranscriptKindCompact:
					selected = append(selected, queuedStatusRecord{record: record, line: checkpointLine, offset: checkpointOffset, kind: kind, body: body})
					if len(selected) >= transcriptSyncMaxRecordsPerSessionPerCycle {
						return nil
					}
				}
			}
			return nil
		})
		runStage("queue_delivery_outbox_8", func() error {
			for _, item := range selected {
				if _, err := bridge.queueTranscriptDeliveryChunksWithOptions(ctx, *session, local, item.record, item.line, item.offset, item.kind, item.body, outboxQueueOptions{}, turn.ID); err != nil {
					return err
				}
			}
			return nil
		})
		runStage("record_checkpoint_8", func() error {
			for _, item := range selected {
				if err := bridge.recordTranscriptCheckpointDetailedWithID(ctx, *session, local.FilePath, transcriptRecordCheckpointKey(item.record), item.line, item.offset, transcriptCheckpointID(session.ID)); err != nil {
					return err
				}
			}
			return nil
		})
		totalQueued += len(selected)
	}
	for _, stage := range stages {
		cxpPerfReportNamedProcIO(b, stage, totals[stage], b.N)
		b.ReportMetric(float64(durations[stage])/float64(b.N), stage+"_ns/op")
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalQueued)/float64(b.N), "queued/op")
	}
}

func BenchmarkCXPPerfModelSQLiteRealisticMixedUserIdleLoopBreakdown(b *testing.B) {
	stages := []string{
		"refresh_lease",
		"flush_outbox",
		"flush_workflow",
		"poll_once",
		"auto_park",
		"transcript_sync_gate",
		"history_watch_gate",
		"helper_auto_update",
		"upgrade_notice",
		"codex_upgrade",
		"beacon_reconcile",
		"beacon_lease",
		"drain_complete",
		"deferred_inbound",
		"process_queued",
		"interrupted_notices",
		"registry_save",
	}
	totals := make(map[string]cxpPerfProcIO, len(stages))
	_, bridge := newCXPPerfRealisticMixedUserFixture(b)
	ctx := context.Background()
	now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
	opts := BridgeOptions{Top: ownerPollMessageTop, MaxWorkChatPollsPerCycle: DefaultMaxWorkChatPollsPerCycle, Interval: 5 * time.Second}
	cxpPerfMarkMainLoopMaintenanceFresh(bridge, now)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tick := now.Add(time.Duration(i) * 3 * time.Second)
		runStage := func(name string, fn func() error) {
			b.Helper()
			delta, err := cxpPerfMeasureProcIO(fn)
			total := totals[name]
			total.add(delta)
			totals[name] = total
			if err != nil {
				b.Fatalf("%s: %v", name, err)
			}
		}
		runStage("refresh_lease", func() error {
			active, err := bridge.refreshControlLease(ctx)
			if err != nil {
				return err
			}
			if !active {
				return teamstore.ErrControlLeaseNotHeld
			}
			return nil
		})
		runStage("flush_outbox", func() error {
			if err := bridge.flushPendingOutboxMainLoop(ctx); err != nil && !isOutboxDeliveryDeferred(err) {
				return err
			}
			return nil
		})
		runStage("flush_workflow", func() error {
			return bridge.flushPendingWorkflowNotificationsWithLimit(ctx, mainLoopWorkflowFlushMaxNotifications)
		})
		runStage("poll_once", func() error {
			if err := bridge.pollOnce(ctx, opts.Top); err != nil && !isGraphRateLimitError(err) {
				return err
			}
			return nil
		})
		runStage("auto_park", func() error {
			if err := bridge.maybeRunIdleWorkChatAutoPark(ctx, tick); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
				return err
			}
			return nil
		})
		runStage("transcript_sync_gate", func() error {
			return bridge.syncLinkedTranscriptsIfDue(ctx, tick)
		})
		runStage("history_watch_gate", func() error {
			return bridge.syncCodexHistoryFinalsIfDue(ctx, tick)
		})
		runStage("helper_auto_update", func() error {
			return bridge.maybeRunHelperAutoUpdate(ctx, opts)
		})
		runStage("upgrade_notice", func() error {
			_, err := bridge.queueCompletedHelperUpgradeNoticeIfNeeded(ctx)
			return err
		})
		runStage("codex_upgrade", func() error {
			return bridge.maybeRunPendingCodexUpgrade(ctx)
		})
		runStage("beacon_reconcile", func() error {
			return bridge.maybeRunBeaconReconcile(ctx, tick)
		})
		runStage("beacon_lease", func() error {
			return bridge.maybeRunBeaconLeaseMaintenance(ctx, tick)
		})
		runStage("drain_complete", func() error {
			_, err := bridge.drainComplete(ctx)
			return err
		})
		runStage("deferred_inbound", func() error {
			return bridge.processDeferredInbound(ctx)
		})
		runStage("process_queued", func() error {
			return bridge.processQueuedTurns(ctx)
		})
		runStage("interrupted_notices", func() error {
			return bridge.sendDeferredInterruptedTurnNotices(ctx)
		})
		runStage("registry_save", func() error {
			return bridge.Save()
		})
	}
	for _, stage := range stages {
		cxpPerfReportNamedProcIO(b, stage, totals[stage], b.N)
	}
}

func BenchmarkCXPPerfModelSQLiteInvalidWorkflowNotificationIdleTickProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		profile.MessagesPerPoll = 0
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			bridge.asyncTurns = true
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			cxpPerfSeedLinkedTranscriptFiles(b, store, bridge, profile)
			cxpPerfPrepareActiveOwner(b, bridge)
			cxpPerfEnableWorkflowNotifications(b, bridge)
			if _, _, err := store.UpdateNotification(context.Background(), "perf-invalid-workflow-notification", func(rec teamstore.NotificationRecord, found bool, now time.Time) (teamstore.NotificationRecord, bool, error) {
				if !found {
					rec.ID = "perf-invalid-workflow-notification"
					rec.CreatedAt = now
				}
				rec.Status = ""
				rec.Title = ""
				rec.ButtonURL = ""
				rec.UpdatedAt = now
				return rec, true, nil
			}); err != nil {
				b.Fatalf("seed invalid workflow notification: %v", err)
			}
			ctx := context.Background()
			now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
			opts := BridgeOptions{Top: ownerPollMessageTop, MaxWorkChatPollsPerCycle: DefaultMaxWorkChatPollsPerCycle, Interval: 5 * time.Second}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := cxpPerfRunMainLoopIdleTick(ctx, bridge, opts, now.Add(time.Duration(i)*11*time.Second)); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
					b.Fatalf("main loop invalid workflow notification tick: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteWorkflowNotificationNoPendingFlushProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			cxpPerfEnableWorkflowNotifications(b, bridge)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bridge.flushPendingWorkflowNotificationsWithLimit(ctx, mainLoopWorkflowFlushMaxNotifications); err != nil {
					b.Fatalf("workflow notification no-pending flush: %v", err)
				}
			}
		})
	}
}

func TestCXPPerfActiveParkedFixtureHasNoPendingWorkflowNotifications(t *testing.T) {
	t.Parallel()
	profile := cxpPerfProfileByNameForTest(t, "many-long-chats")
	profile.MessagesPerPoll = 0
	store := newCXPPerfStore(t, profile)
	bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
	cxpPerfSeedColdRuntimeMetadata(t, store, profile)
	cxpPerfSeedActiveParkedSessions(t, store, bridge)
	cxpPerfMigrateStoreToSQLite(t, store)
	hasPending, err := store.HasPendingWorkflowNotifications(context.Background())
	if err != nil {
		t.Fatalf("HasPendingWorkflowNotifications error: %v", err)
	}
	if hasPending {
		t.Fatal("active parked fixture unexpectedly has pending workflow notifications")
	}
}

func BenchmarkCXPPerfModelSQLiteLeaseOwnerHeartbeatProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
			cxpPerfPrepareActiveOwner(b, bridge)
			ctx := context.Background()
			b.Run("lease-refresh", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					active, err := bridge.refreshControlLease(ctx)
					if err != nil {
						b.Fatalf("refresh control lease: %v", err)
					}
					if !active {
						b.Fatal("control lease unexpectedly inactive")
					}
				}
			})
			b.Run("owner-heartbeat", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := bridge.recordCurrentOwnerHeartbeat(ctx); err != nil {
						b.Fatalf("owner heartbeat: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkCXPPerfModelSQLiteSelectedSnapshotLargeColdStateProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfQueuePendingOutbox(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			ctx := context.Background()
			benchState := func(name string, fn func(context.Context) (teamstore.State, error)) {
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := fn(ctx); err != nil {
							b.Fatalf("%s: %v", name, err)
						}
					}
				})
			}
			benchState("poll-schedule", store.PollScheduleSnapshot)
			benchState("queued-turns", store.QueuedTurnStateSnapshot)
			benchState("workflow-notifications", store.WorkflowNotificationStateSnapshot)
			benchState("upgrade-blocking", store.UpgradeBlockingStateSnapshot)
			b.Run("auto-update-control", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, _, err := store.ReadAutoUpdateControl(ctx); err != nil {
						b.Fatalf("auto-update control: %v", err)
					}
				}
			})
			if _, _, err := store.UpdateNotification(ctx, "perf-invalid-workflow-notification", func(rec teamstore.NotificationRecord, found bool, now time.Time) (teamstore.NotificationRecord, bool, error) {
				if !found {
					rec.ID = "perf-invalid-workflow-notification"
					rec.CreatedAt = now
				}
				rec.Status = ""
				rec.UpdatedAt = now
				return rec, true, nil
			}); err != nil {
				b.Fatalf("seed invalid workflow notification: %v", err)
			}
			b.Run("pending-workflow-notifications", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := store.PendingWorkflowNotifications(ctx); err != nil {
						b.Fatalf("pending workflow notifications: %v", err)
					}
				}
			})
			b.Run("has-pending-workflow-notifications", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := store.HasPendingWorkflowNotifications(ctx); err != nil {
						b.Fatalf("has pending workflow notifications: %v", err)
					}
				}
			})
			b.Run("pending-outbox", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := store.PendingOutbox(ctx); err != nil {
						b.Fatalf("pending outbox: %v", err)
					}
				}
			})
			b.Run("deferred-inbound", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := store.DeferredInbound(ctx); err != nil {
						b.Fatalf("deferred inbound: %v", err)
					}
				}
			})
			b.Run("read-owner", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, _, err := store.ReadOwner(ctx); err != nil {
						b.Fatalf("read owner: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkCXPPerfModelSQLiteHelperAutoUpdateNotDueProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			if err := store.Update(context.Background(), func(state *teamstore.State) error {
				state.AutoUpdate.NextCheckAt = time.Now().Add(time.Hour)
				state.AutoUpdate.LastCheckAt = time.Now().Add(-time.Minute)
				return nil
			}); err != nil {
				b.Fatalf("seed auto-update state: %v", err)
			}
			cxpPerfMigrateStoreToSQLite(b, store)
			graph := newCXPPerfGraph(profile)
			bridge := newCXPPerfBridge(store, graph, profile)
			opts := BridgeOptions{
				HelperVersion:     "v0.1.3",
				HelperAutoUpdater: &fakeHelperAutoUpdater{},
			}
			ctx := context.Background()
			b.Run("cold-state-refresh", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					bridge.clearHelperAutoUpdateProbeGate()
					if err := bridge.maybeRunHelperAutoUpdate(ctx, opts); err != nil {
						b.Fatalf("helper auto-update cold-state refresh: %v", err)
					}
				}
			})
			b.Run("cached-main-loop", func(b *testing.B) {
				bridge.clearHelperAutoUpdateProbeGate()
				if err := bridge.maybeRunHelperAutoUpdate(ctx, opts); err != nil {
					b.Fatalf("warm helper auto-update cache: %v", err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := bridge.maybeRunHelperAutoUpdate(ctx, opts); err != nil {
						b.Fatalf("helper auto-update cached main loop: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkCXPPerfModelSQLiteHelperAutoUpdateDueRecordWrites(b *testing.B) {
	profile := cxpPerfRealisticMixedUserProfile()
	b.Run(profile.Name, func(b *testing.B) {
		store := newCXPPerfStore(b, profile)
		cxpPerfSeedColdRuntimeMetadata(b, store, profile)
		cxpPerfMigrateStoreToSQLite(b, store)
		ctx := context.Background()
		now := time.Date(2026, 6, 17, 11, 0, 0, 0, time.UTC)
		benchRecord := func(name string, fn func(int) error) {
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				var total cxpPerfProcIO
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ioDelta, err := cxpPerfMeasureProcIO(func() error {
						return fn(i)
					})
					if err != nil {
						b.Fatalf("%s: %v", name, err)
					}
					total.add(ioDelta)
				}
				b.StopTimer()
				cxpPerfReportProcIO(b, total, b.N)
			})
		}
		benchRecord("check", func(i int) error {
			_, err := store.RecordAutoUpdateCheck(ctx, teamstore.AutoUpdateRecord{
				Now:              now.Add(time.Duration(i) * time.Minute),
				NextCheckAt:      now.Add(time.Duration(i+30) * time.Minute),
				CandidateTag:     "v9.9.9",
				CandidateVersion: "9.9.9",
				CandidateAsset:   "codex-proxy_9.9.9_linux_amd64",
			})
			return err
		})
		benchRecord("attempt", func(i int) error {
			_, err := store.RecordAutoUpdateAttempt(ctx, "v9.9.9", now.Add(time.Duration(i)*time.Minute))
			return err
		})
		benchRecord("installed", func(i int) error {
			_, err := store.RecordAutoUpdateInstalled(ctx, "v9.9.9", now.Add(time.Duration(i)*time.Minute))
			return err
		})
	})
}

func BenchmarkCXPPerfModelSQLiteCodexUpgradeNoPendingProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
			bridge.codexUpgrader = func(context.Context) (CodexUpgradeResult, error) {
				return CodexUpgradeResult{Path: "/managed/codex"}, nil
			}
			ctx := context.Background()
			b.Run("cold-state-refresh", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					bridge.clearPendingCodexUpgradeProbeGate()
					if err := bridge.maybeRunPendingCodexUpgrade(ctx); err != nil {
						b.Fatalf("codex upgrade cold-state refresh: %v", err)
					}
				}
			})
			b.Run("cached-main-loop", func(b *testing.B) {
				bridge.clearPendingCodexUpgradeProbeGate()
				if err := bridge.maybeRunPendingCodexUpgrade(ctx); err != nil {
					b.Fatalf("warm codex upgrade cache: %v", err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := bridge.maybeRunPendingCodexUpgrade(ctx); err != nil {
						b.Fatalf("codex upgrade cached main loop: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkCXPPerfModelSQLiteDeferredInboundNoDeferredProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				deferred, err := store.DeferredInbound(ctx)
				if err != nil {
					b.Fatalf("deferred inbound: %v", err)
				}
				if len(deferred) != 0 {
					b.Fatalf("deferred inbound returned %d events, want none", len(deferred))
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteHistoryWatchCheckpointUpdateProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		profile.MessagesPerPoll = 0
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
			cxpPerfSeedLinkedTranscriptFiles(b, store, bridge, profile)
			checkpointID, checkpoint := cxpPerfFirstHistoryWatchCheckpoint(b, store)
			ctx := context.Background()
			now := time.Date(2026, 5, 23, 10, 20, 0, 0, time.UTC)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bridge.recordHistoryWatchCheckpoint(ctx, checkpointID, historyTieredFileState{
					Path:        checkpoint.Path,
					Size:        checkpoint.Size + int64(i+1),
					ModTime:     now.Add(time.Duration(i) * time.Second),
					Offset:      checkpoint.Offset + int64(i+1),
					Line:        checkpoint.Line + i + 1,
					SessionID:   checkpoint.SessionID,
					ThreadID:    checkpoint.ThreadID,
					TurnID:      checkpoint.TurnID,
					LastFinalID: checkpoint.LastFinalID,
				}, now.Add(time.Duration(i)*time.Second)); err != nil {
					b.Fatalf("record history watch checkpoint: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteHistoryWatchActiveAppendProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		profile.MessagesPerPoll = 0
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
			cxpPerfSeedLinkedTranscriptFiles(b, store, bridge, profile)
			_, checkpoint := cxpPerfFirstHistoryWatchCheckpoint(b, store)
			ctx := context.Background()
			now := time.Date(2026, 5, 23, 10, 25, 0, 0, time.UTC)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				callNow := now.Add(time.Duration(i) * 11 * time.Second)
				b.StopTimer()
				cxpPerfAppendHistoryCommentary(b, checkpoint.Path, i, callNow)
				b.StartTimer()
				bridge.lastHistoryWatchSync = time.Time{}
				bridge.lastHistoryWatchReconcile = callNow
				if err := bridge.syncCodexHistoryFinalsIfDue(ctx, callNow); err != nil {
					b.Fatalf("history watch active append: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelLinkedTranscriptIdleManySessions(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		profile.MessagesPerPoll = 0
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
			cxpPerfSeedLinkedTranscriptFiles(b, store, bridge, profile)
			ctx := context.Background()
			now := time.Date(2026, 5, 23, 10, 30, 0, 0, time.UTC)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bridge.lastTranscriptSync = time.Time{}
				if err := bridge.syncLinkedTranscriptsIfDue(ctx, now.Add(time.Duration(i)*transcriptSyncMinInterval)); err != nil {
					b.Fatalf("linked transcript idle sync: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteBackgroundImportCheckpointOnly(b *testing.B) {
	ctx := context.Background()
	const backgroundRecords = 2000
	var totalSQLiteBytes int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		store, bridge, session, transcriptPath := newCXPPerfBackgroundImportCheckpointOnlyFixture(b, backgroundRecords, i)
		beforeBytes := cxpPerfSQLiteStoreBytes(b, store)
		b.StartTimer()

		result, err := bridge.importTranscriptRecordsToTeams(ctx, session, transcriptPath, "import-bg:"+session.ID, "import-bg", transcriptCheckpointID(session.ID), transcriptImportRunOptions{QueueOnly: true, MaxBatches: 1})
		if err != nil {
			b.Fatalf("background checkpoint-only import: %v", err)
		}
		if !result.Complete || result.Stats.SkippedBackground != backgroundRecords {
			b.Fatalf("background checkpoint-only result = %#v, want complete with %d skipped records", result, backgroundRecords)
		}

		b.StopTimer()
		afterBytes := cxpPerfSQLiteStoreBytes(b, store)
		totalSQLiteBytes += afterBytes - beforeBytes
		b.StartTimer()
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalSQLiteBytes)/float64(b.N), "sqlite_bytes/op")
	}
}

func BenchmarkCXPPerfModelSQLiteLegacyLinkedTranscriptBackfilledIdleProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		profile.MessagesPerPoll = 0
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
			cxpPerfSeedLinkedTranscriptFiles(b, store, bridge, profile)
			cxpPerfStripLinkedTranscriptCheckpointPositionMetadata(b, store)
			ctx := context.Background()
			now := time.Date(2026, 5, 23, 10, 35, 0, 0, time.UTC)
			bridge.lastTranscriptSync = time.Time{}
			if err := bridge.syncLinkedTranscriptsIfDue(ctx, now); err != nil {
				b.Fatalf("legacy linked transcript backfill: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bridge.lastTranscriptSync = time.Time{}
				if err := bridge.syncLinkedTranscriptsIfDue(ctx, now.Add(time.Duration(i+1)*transcriptSyncMinInterval)); err != nil {
					b.Fatalf("legacy linked transcript backfilled idle sync: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelSQLiteQueuedTurnsBlockedNoProgressProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			store := newCXPPerfStore(b, profile)
			cxpPerfSeedQueuedTurns(b, store, profile)
			cxpPerfSeedBlockedTranscriptImports(b, store, profile)
			cxpPerfSeedColdRuntimeMetadata(b, store, profile)
			cxpPerfMigrateStoreToSQLite(b, store)
			bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
			bridge.asyncTurns = true
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bridge.processQueuedTurns(ctx); err != nil {
					b.Fatalf("blocked queued turns: %v", err)
				}
			}
		})
	}
}

func BenchmarkCXPPerfModelListenOnceProfiles(b *testing.B) {
	for _, profile := range cxpPerfProfiles {
		profile := profile
		b.Run(profile.Name, func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := newCXPPerfStore(b, profile)
				graph := newCXPPerfGraph(profile)
				bridge := newCXPPerfBridge(store, graph, profile)
				b.StartTimer()
				err := bridge.Listen(ctx, BridgeOptions{
					Store:                      store,
					RegistryPath:               bridge.registryPath,
					Interval:                   time.Second,
					Once:                       true,
					Top:                        ownerPollMessageTop,
					MaxWorkChatPollsPerCycle:   DefaultMaxWorkChatPollsPerCycle,
					OwnerStaleAfter:            30 * time.Second,
					Executor:                   EchoExecutor{},
					ControlFallbackExecutor:    EchoExecutor{},
					ControlFallbackHelpContext: "perf",
					HelperVersion:              "perf-benchmark",
				})
				if err != nil && !isGraphRateLimitError(err) {
					b.Fatalf("listen once: %v", err)
				}
				b.StopTimer()
			}
		})
	}
}

func BenchmarkCXPPerfModelExternalScenarios(b *testing.B) {
	cxpPerfWithImmediateHelperService(b)
	for _, scenario := range cxpPerfExternalScenarios {
		scenario := scenario
		b.Run(scenario.Name, func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store, bridge, harness := newCXPPerfExternalBridge(b, scenario)
				b.StartTimer()
				err := cxpPerfRunListenOnce(ctx, bridge, store, scenario, harness)
				if err != nil && !cxpPerfExpectedListenError(err, scenario) {
					b.Fatalf("listen once external scenario: %v", err)
				}
				b.StopTimer()
			}
		})
	}
}

func cxpPerfSmokeProfile(profile cxpPerfProfile) cxpPerfProfile {
	profile.WorkChats = max(1, min(profile.WorkChats, 4))
	profile.TurnsPerChat = max(1, min(profile.TurnsPerChat, 8))
	profile.MessagesPerPoll = min(profile.MessagesPerPoll, 1)
	profile.MessageBytes = max(16, min(profile.MessageBytes, 128))
	profile.OutboxPerChat = min(profile.OutboxPerChat, 2)
	profile.LookupPerCycle = max(1, min(profile.LookupPerCycle, 8))
	profile.HistoryFiles = max(1, min(profile.HistoryFiles, 4))
	profile.HistoryLines = max(1, min(profile.HistoryLines, 16))
	return profile
}

func cxpPerfProfileByNameForTest(t testing.TB, name string) cxpPerfProfile {
	t.Helper()
	for _, profile := range cxpPerfProfiles {
		if profile.Name == name {
			return profile
		}
	}
	t.Fatalf("cxp perf profile %q not found", name)
	return cxpPerfProfile{}
}

func cxpPerfExternalBaseProfile() cxpPerfProfile {
	profile := cxpPerfSmokeProfile(cxpPerfProfiles[5])
	profile.MessagesPerPoll = 1
	profile.OutboxPerChat = 1
	return profile
}

func newCXPPerfExternalBridge(tb testing.TB, scenario cxpPerfExternalScenario) (*teamstore.Store, *Bridge, *cxpPerfServiceHarness) {
	tb.Helper()
	profile := cxpPerfExternalBaseProfile()
	store := newCXPPerfStore(tb, profile)
	if scenario.QueueOutbox {
		cxpPerfQueuePendingOutbox(tb, store, profile)
	}
	graph := newCXPPerfGraphWithScenario(profile, scenario)
	bridge := newCXPPerfBridge(store, graph, profile)
	harness := &cxpPerfServiceHarness{}
	bridge.executor = cxpPerfExecutor{mode: scenario.CodexMode}
	return store, bridge, harness
}

func cxpPerfRunListenOnce(ctx context.Context, bridge *Bridge, store *teamstore.Store, scenario cxpPerfExternalScenario, harness *cxpPerfServiceHarness) error {
	if err := bridge.Listen(ctx, cxpPerfBridgeOptions(bridge, store, scenario, harness)); err != nil {
		return err
	}
	if strings.TrimSpace(scenario.ControlPrompt) == "" {
		return nil
	}
	active, err := bridge.claimControlLease(ctx)
	if err != nil {
		return err
	}
	if !active {
		return fmt.Errorf("perf service scenario could not reclaim control lease")
	}
	msg := cxpPerfChatMessage("control-chat", "perf-direct-control-"+scenario.Name, time.Date(2026, 5, 23, 9, 1, 0, 0, time.UTC), scenario.ControlPrompt)
	return bridge.handleControlMessage(ctx, msg, scenario.ControlPrompt)
}

func cxpPerfBridgeOptions(bridge *Bridge, store *teamstore.Store, scenario cxpPerfExternalScenario, harness *cxpPerfServiceHarness) BridgeOptions {
	executor := cxpPerfExecutor{mode: scenario.CodexMode}
	return BridgeOptions{
		Store:                      store,
		RegistryPath:               bridge.registryPath,
		Interval:                   time.Second,
		Once:                       true,
		Top:                        ownerPollMessageTop,
		MaxWorkChatPollsPerCycle:   DefaultMaxWorkChatPollsPerCycle,
		OwnerStaleAfter:            30 * time.Second,
		Executor:                   executor,
		ControlFallbackExecutor:    executor,
		ControlFallbackHelpContext: "perf",
		HelperVersion:              "perf-external",
		HelperRestarter:            harness.restart,
		HelperReloader:             harness.reload,
		CodexUpgrader:              harness.upgradeCodex,
	}
}

func cxpPerfExpectedListenError(err error, scenario cxpPerfExternalScenario) bool {
	if err == nil {
		return true
	}
	return isGraphRateLimitError(err) ||
		scenario.GraphMode == cxpPerfGraphReadUnauthorized ||
		scenario.GraphMode == cxpPerfGraphReadForbidden ||
		scenario.GraphMode == cxpPerfGraphReadServerError ||
		scenario.GraphMode == cxpPerfGraphReadNetworkDrop ||
		scenario.GraphMode == cxpPerfGraphReadMalformed ||
		scenario.GraphMode == cxpPerfGraphSendForbidden
}

func cxpPerfWithImmediateHelperService(tb testing.TB) {
	tb.Helper()
	prevDelay := helperRestartDelay
	helperRestartDelay = 0
	tb.Cleanup(func() {
		helperRestartDelay = prevDelay
	})
}

func cxpPerfQueuePendingOutbox(tb testing.TB, store *teamstore.Store, profile cxpPerfProfile) {
	tb.Helper()
	now := time.Date(2026, 5, 23, 7, 45, 0, 0, time.UTC)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		sessionID := cxpPerfSessionID(0)
		chatID := cxpPerfChatID(0)
		state.OutboxMessages["perf-external-queued-outbox"] = teamstore.OutboxMessage{
			ID:          "perf-external-queued-outbox",
			SessionID:   sessionID,
			TeamsChatID: chatID,
			Kind:        "helper",
			Body:        cxpPerfText(profile.MessageBytes),
			Status:      teamstore.OutboxStatusQueued,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		return nil
	}); err != nil {
		tb.Fatalf("queue pending external outbox: %v", err)
	}
}

func cxpPerfMigrateStoreBackend(tb testing.TB, store *teamstore.Store, backend string) {
	tb.Helper()
	switch strings.TrimSpace(backend) {
	case "":
		return
	case "sqlite":
		cxpPerfMigrateStoreToSQLite(tb, store)
	default:
		tb.Fatalf("unknown perf store backend %q", backend)
	}
}

func cxpPerfMigrateStoreToSQLite(tb testing.TB, store *teamstore.Store) {
	tb.Helper()
	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		tb.Fatalf("migrate perf store to sqlite: %v", err)
	}
}

func cxpPerfSeedRunningTurns(tb testing.TB, store *teamstore.Store, profile cxpPerfProfile) {
	tb.Helper()
	now := time.Date(2026, 5, 23, 8, 30, 0, 0, time.UTC)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		for chat := 0; chat < profile.WorkChats; chat++ {
			sessionID := cxpPerfSessionID(chat)
			turnID := fmt.Sprintf("perf-running-%03d", chat)
			state.Turns[turnID] = teamstore.Turn{
				ID:        turnID,
				SessionID: sessionID,
				Status:    teamstore.TurnStatusRunning,
				QueuedAt:  now.Add(-time.Minute),
				StartedAt: now,
				CreatedAt: now.Add(-time.Minute),
				UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		tb.Fatalf("seed running turns: %v", err)
	}
}

func cxpPerfSeedQueuedTurns(tb testing.TB, store *teamstore.Store, profile cxpPerfProfile) {
	tb.Helper()
	now := time.Date(2026, 5, 23, 8, 45, 0, 0, time.UTC)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		for chat := 0; chat < profile.WorkChats; chat++ {
			sessionID := cxpPerfSessionID(chat)
			chatID := cxpPerfChatID(chat)
			inboundID := fmt.Sprintf("perf-drain-inbound-%03d", chat)
			turnID := fmt.Sprintf("perf-drain-turn-%03d", chat)
			state.InboundEvents[inboundID] = teamstore.InboundEvent{
				ID:             inboundID,
				SessionID:      sessionID,
				TeamsChatID:    chatID,
				TeamsMessageID: fmt.Sprintf("perf-drain-message-%03d", chat),
				Text:           cxpPerfText(profile.MessageBytes),
				Status:         teamstore.InboundStatusPersisted,
				CreatedAt:      now,
				UpdatedAt:      now,
			}
			state.Turns[turnID] = teamstore.Turn{
				ID:             turnID,
				SessionID:      sessionID,
				InboundEventID: inboundID,
				Status:         teamstore.TurnStatusQueued,
				QueuedAt:       now,
				CreatedAt:      now,
				UpdatedAt:      now,
			}
		}
		return nil
	}); err != nil {
		tb.Fatalf("seed queued turns: %v", err)
	}
}

func cxpPerfSeedBlockedTranscriptImports(tb testing.TB, store *teamstore.Store, profile cxpPerfProfile) {
	tb.Helper()
	now := time.Date(2026, 5, 23, 8, 50, 0, 0, time.UTC)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		for chat := 0; chat < profile.WorkChats; chat++ {
			sessionID := cxpPerfSessionID(chat)
			state.ImportCheckpoints[transcriptCheckpointID(sessionID)] = teamstore.ImportCheckpoint{
				ID:        transcriptCheckpointID(sessionID),
				SessionID: sessionID,
				Status:    importCheckpointStatusImporting,
				UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		tb.Fatalf("seed blocked transcript imports: %v", err)
	}
}

func cxpPerfSeedColdRuntimeMetadata(tb testing.TB, store *teamstore.Store, profile cxpPerfProfile) {
	tb.Helper()
	count := cxpPerfColdRuntimeRecordCount(profile)
	if count <= 0 {
		return
	}
	now := time.Date(2026, 5, 23, 8, 55, 0, 0, time.UTC)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		for i := 0; i < count; i++ {
			sessionID := cxpPerfSessionID(i % max(1, profile.WorkChats))
			chatID := cxpPerfChatID(i % max(1, profile.WorkChats))
			turnID := fmt.Sprintf("perf-cold-turn-%05d", i)
			sourcePath := fmt.Sprintf("/tmp/cxp-perf/history/session-%05d.jsonl", i%max(1, profile.HistoryFiles))
			text := cxpPerfText(max(32, min(profile.MessageBytes, 512)))
			hash := normalizedTextHash(text)
			state.TranscriptLedger[fmt.Sprintf("perf-ledger-%05d", i)] = teamstore.TranscriptLedgerRecord{
				ID:             fmt.Sprintf("perf-ledger-%05d", i),
				SessionID:      sessionID,
				CodexThreadID:  fmt.Sprintf("perf-thread-%03d", i%max(1, profile.WorkChats)),
				SourcePath:     sourcePath,
				SourceLine:     i + 1,
				SourceRecordID: fmt.Sprintf("perf-record-%05d", i),
				Kind:           "assistant",
				OutboxID:       fmt.Sprintf("perf-outbox-cold-%05d", i),
				ImportedAt:     now.Add(time.Duration(i) * time.Millisecond),
				CreatedAt:      now,
				UpdatedAt:      now,
			}
			state.TranscriptDeliveries[fmt.Sprintf("perf-delivery-%05d", i)] = teamstore.TranscriptDeliveryRecord{
				ID:             fmt.Sprintf("perf-delivery-%05d", i),
				SessionID:      sessionID,
				CodexThreadID:  fmt.Sprintf("perf-thread-%03d", i%max(1, profile.WorkChats)),
				SourcePath:     sourcePath,
				SourceLine:     i + 1,
				SourceOffset:   int64(i * 128),
				SourceRecordID: fmt.Sprintf("perf-record-%05d", i),
				Kind:           "assistant",
				TextHash:       hash,
				OutboxID:       fmt.Sprintf("perf-outbox-cold-%05d", i),
				Status:         teamstore.TranscriptDeliveryStatusSent,
				CreatedAt:      now,
				UpdatedAt:      now,
				SentAt:         now,
			}
			state.HelperDeliveries[fmt.Sprintf("perf-helper-delivery-%05d", i)] = teamstore.HelperDeliveryRecord{
				ID:             fmt.Sprintf("perf-helper-delivery-%05d", i),
				SessionID:      sessionID,
				TeamsChatID:    chatID,
				CodexThreadID:  fmt.Sprintf("perf-thread-%03d", i%max(1, profile.WorkChats)),
				TurnID:         turnID,
				Kind:           "final",
				KindFamily:     "answer",
				SourceTextHash: hash,
				RenderedHash:   hash,
				VisibleHash:    hash,
				OutboxID:       fmt.Sprintf("perf-outbox-cold-%05d", i),
				TeamsMessageID: fmt.Sprintf("perf-teams-helper-%05d", i),
				PartIndex:      1,
				PartCount:      1,
				Status:         teamstore.HelperDeliveryStatusSent,
				CreatedAt:      now,
				UpdatedAt:      now,
				SentAt:         now,
			}
			state.Notifications[fmt.Sprintf("perf-notification-%05d", i)] = teamstore.NotificationRecord{
				ID:             fmt.Sprintf("perf-notification-%05d", i),
				SessionID:      sessionID,
				TurnID:         turnID,
				Kind:           "turn_completed",
				OutboxID:       fmt.Sprintf("perf-outbox-cold-%05d", i),
				Status:         teamstore.NotificationStatusSent,
				Title:          "perf notification",
				ChatTitle:      "perf chat",
				RequestSummary: text,
				SentAt:         now,
				CreatedAt:      now,
				UpdatedAt:      now,
			}
			state.ArtifactRecords[fmt.Sprintf("perf-artifact-%05d", i)] = teamstore.ArtifactRecord{
				ID:             fmt.Sprintf("perf-artifact-%05d", i),
				SessionID:      sessionID,
				TurnID:         turnID,
				Path:           fmt.Sprintf("reports/perf-%05d.txt", i),
				UploadName:     fmt.Sprintf("perf-%05d.txt", i),
				DriveItemID:    fmt.Sprintf("drive-item-%05d", i),
				OutboxID:       fmt.Sprintf("perf-outbox-cold-%05d", i),
				TeamsMessageID: fmt.Sprintf("perf-teams-artifact-%05d", i),
				Status:         "uploaded",
				UploadedAt:     now,
				SentAt:         now,
				CreatedAt:      now,
				UpdatedAt:      now,
			}
		}
		state.HistoryWatchReady = now
		return nil
	}); err != nil {
		tb.Fatalf("seed cold runtime metadata: %v", err)
	}
}

func cxpPerfColdRuntimeRecordCount(profile cxpPerfProfile) int {
	fromTurns := profile.WorkChats * max(1, profile.TurnsPerChat) / 4
	fromHistory := profile.HistoryFiles * max(1, profile.HistoryLines) / 8
	count := max(profile.WorkChats*4, max(fromTurns, fromHistory))
	return max(0, min(count, 4096))
}

func cxpPerfSeedLinkedTranscriptFiles(tb testing.TB, store *teamstore.Store, bridge *Bridge, profile cxpPerfProfile) {
	tb.Helper()
	root := filepath.Join(tb.TempDir(), "codex-home")
	transcriptRoot := filepath.Join(root, "sessions", "2026", "05", "23")
	if err := os.MkdirAll(transcriptRoot, 0o700); err != nil {
		tb.Fatalf("mkdir transcript root: %v", err)
	}
	now := time.Date(2026, 5, 23, 9, 15, 0, 0, time.UTC)
	if bridge != nil {
		bridge.scope.CodexHome = root
	}
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.Scope.CodexHome = root
		lineCount := max(1, min(profile.HistoryLines, 256))
		for chat := 0; chat < profile.WorkChats; chat++ {
			sessionID := cxpPerfSessionID(chat)
			threadID := fmt.Sprintf("perf-thread-%03d", chat)
			path := filepath.Join(transcriptRoot, threadID+".jsonl")
			data := cxpPerfTranscriptContent(threadID, lineCount, profile.MessageBytes)
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				return err
			}
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			if session, ok := state.Sessions[sessionID]; ok {
				session.CodexThreadID = threadID
				session.CodexHome = root
				state.Sessions[sessionID] = session
			}
			if bridge != nil {
				for i := range bridge.reg.Sessions {
					if bridge.reg.Sessions[i].ID == sessionID {
						bridge.reg.Sessions[i].CodexThreadID = threadID
						break
					}
				}
			}
			state.ImportCheckpoints[transcriptCheckpointID(sessionID)] = teamstore.ImportCheckpoint{
				ID:             transcriptCheckpointID(sessionID),
				SessionID:      sessionID,
				SourcePath:     path,
				LastRecordID:   fmt.Sprintf("record-%s-%04d", threadID, lineCount-1),
				LastSourceLine: lineCount + 1,
				LastOffset:     int64(len(data)),
				SourceSize:     info.Size(),
				SourceModTime:  info.ModTime(),
				Status:         importCheckpointStatusComplete,
				UpdatedAt:      now,
			}
			state.HistoryWatch[historyWatchCheckpointID(path)] = teamstore.HistoryWatchCheckpoint{
				ID:        historyWatchCheckpointID(path),
				Path:      path,
				Size:      info.Size(),
				ModTime:   info.ModTime(),
				Offset:    int64(len(data)),
				Line:      lineCount + 1,
				SessionID: sessionID,
				ThreadID:  threadID,
				TurnID:    fmt.Sprintf("perf-watch-turn-%03d", chat),
				UpdatedAt: now,
			}
		}
		state.HistoryWatchReady = now
		return nil
	}); err != nil {
		tb.Fatalf("seed linked transcript files: %v", err)
	}
}

func newCXPPerfBackgroundImportCheckpointOnlyFixture(tb testing.TB, records int, index int) (*teamstore.Store, *Bridge, Session, string) {
	tb.Helper()
	profile := cxpPerfProfile{
		Name:          "background-import-checkpoint-only",
		WorkChats:     1,
		TurnsPerChat:  1,
		MessageBytes:  128,
		OutboxPerChat: 0,
	}
	store := newCXPPerfStore(tb, profile)
	cxpPerfMigrateStoreToSQLite(tb, store)
	bridge := newCXPPerfBridge(store, newCXPPerfGraph(profile), profile)
	session := bridge.reg.Sessions[0]
	threadID := fmt.Sprintf("perf-background-import-%06d", index)
	session.CodexThreadID = threadID
	bridge.reg.Sessions[0] = session
	if err := bridge.ensureDurableSession(context.Background(), &session); err != nil {
		tb.Fatalf("ensure durable perf session: %v", err)
	}
	transcriptPath := filepath.Join(tb.TempDir(), "background-import.jsonl")
	if err := os.WriteFile(transcriptPath, []byte(cxpPerfBackgroundOnlyTranscriptContent(threadID, records)), 0o600); err != nil {
		tb.Fatalf("write background-only transcript: %v", err)
	}
	return store, bridge, session, transcriptPath
}

func cxpPerfBackgroundOnlyTranscriptContent(threadID string, records int) string {
	var b strings.Builder
	b.WriteString(`{"type":"session_meta","payload":{"id":`)
	b.WriteString(strconv.Quote(threadID))
	b.WriteString("}}\n")
	for i := 0; i < records; i++ {
		b.WriteString(`{"type":"response_item","payload":{"id":`)
		b.WriteString(strconv.Quote(fmt.Sprintf("tool-%06d", i)))
		b.WriteString(`,"type":"function_call","name":"shell","arguments":`)
		b.WriteString(strconv.Quote(fmt.Sprintf(`{"cmd":"rg perf-%06d"}`, i)))
		b.WriteString("}}\n")
	}
	return b.String()
}

func cxpPerfSQLiteStoreBytes(tb testing.TB, store *teamstore.Store) int64 {
	tb.Helper()
	if store == nil {
		return 0
	}
	dir := filepath.Dir(store.Path())
	var total int64
	for _, name := range []string{"store.sqlite", "store.sqlite-wal", "store.sqlite-shm"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err == nil {
			total += info.Size()
			continue
		}
		if !os.IsNotExist(err) {
			tb.Fatalf("stat sqlite file %s: %v", name, err)
		}
	}
	return total
}

func cxpPerfSQLiteWALBytes(tb testing.TB, store *teamstore.Store) int64 {
	tb.Helper()
	if store == nil {
		return 0
	}
	path := filepath.Join(filepath.Dir(store.Path()), teamstore.SQLiteFileName+"-wal")
	info, err := os.Stat(path)
	if err == nil {
		return info.Size()
	}
	if os.IsNotExist(err) {
		return 0
	}
	tb.Fatalf("stat sqlite WAL %s: %v", path, err)
	return 0
}

func cxpPerfStripLinkedTranscriptCheckpointPositionMetadata(tb testing.TB, store *teamstore.Store) {
	tb.Helper()
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		for id, checkpoint := range state.ImportCheckpoints {
			if !strings.HasPrefix(id, "transcript:") || strings.Contains(id, ":subagent:") {
				continue
			}
			checkpoint.LastOffset = 0
			checkpoint.SourceSize = 0
			checkpoint.SourceModTime = time.Time{}
			state.ImportCheckpoints[id] = checkpoint
		}
		return nil
	}); err != nil {
		tb.Fatalf("strip linked transcript checkpoint metadata: %v", err)
	}
}

func cxpPerfFirstHistoryWatchCheckpoint(tb testing.TB, store *teamstore.Store) (string, teamstore.HistoryWatchCheckpoint) {
	tb.Helper()
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		tb.Fatalf("load history watch state: %v", err)
	}
	ids := make([]string, 0, len(state.HistoryWatch))
	for id := range state.HistoryWatch {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		checkpoint := state.HistoryWatch[id]
		if strings.TrimSpace(checkpoint.Path) != "" {
			return id, checkpoint
		}
	}
	tb.Fatal("perf store has no history watch checkpoint")
	return "", teamstore.HistoryWatchCheckpoint{}
}

func cxpPerfAppendHistoryCommentary(tb testing.TB, path string, index int, when time.Time) {
	tb.Helper()
	line := fmt.Sprintf(
		`{"timestamp":%q,"type":"event_msg","payload":{"type":"agent_message","id":%q,"turn_id":%q,"phase":"commentary","message":%q}}`+"\n",
		when.Format(time.RFC3339Nano),
		fmt.Sprintf("perf-status-%06d", index),
		fmt.Sprintf("perf-turn-%06d", index),
		"working",
	)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		tb.Fatalf("open history file for append: %v", err)
	}
	if _, err := f.WriteString(line); err != nil {
		_ = f.Close()
		tb.Fatalf("append history commentary: %v", err)
	}
	if err := f.Close(); err != nil {
		tb.Fatalf("close history commentary file: %v", err)
	}
}

func cxpPerfTranscriptContent(threadID string, lines int, messageBytes int) string {
	var b strings.Builder
	b.WriteString(`{"type":"session_meta","payload":{"id":`)
	b.WriteString(strconv.Quote(threadID))
	b.WriteString("}}\n")
	text := cxpPerfText(max(16, min(messageBytes, 512)))
	base := time.Date(2026, 5, 23, 9, 0, 0, 0, time.UTC)
	for i := 0; i < lines; i++ {
		b.WriteString(`{"timestamp":`)
		b.WriteString(strconv.Quote(base.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano)))
		b.WriteString(`,"type":"event_msg","payload":{"type":"agent_message","id":`)
		b.WriteString(strconv.Quote(fmt.Sprintf("record-%s-%04d", threadID, i)))
		b.WriteString(`,"turn_id":`)
		b.WriteString(strconv.Quote(fmt.Sprintf("turn-%s-%04d", threadID, i)))
		b.WriteString(`,"phase":"final_answer","message":`)
		b.WriteString(strconv.Quote(text))
		b.WriteString("}}\n")
	}
	return b.String()
}

func cxpPerfPrepareActiveOwner(tb testing.TB, bridge *Bridge) {
	tb.Helper()
	ctx := context.Background()
	active, err := bridge.claimControlLease(ctx)
	if err != nil {
		tb.Fatalf("claim control lease: %v", err)
	}
	if !active {
		tb.Fatal("control lease unexpectedly inactive")
	}
	owner, err := teamstore.CurrentOwner("", "", "", time.Date(2026, 5, 23, 9, 30, 0, 0, time.UTC))
	if err != nil {
		tb.Fatalf("current owner: %v", err)
	}
	bridge.setOwner(owner, 18*time.Second)
	if err := bridge.recordCurrentOwnerHeartbeat(ctx); err != nil {
		tb.Fatalf("initial owner heartbeat: %v", err)
	}
}

func cxpPerfEnableWorkflowNotifications(tb testing.TB, bridge *Bridge) {
	tb.Helper()
	tb.Setenv("XDG_CONFIG_HOME", tb.TempDir())
	path := filepath.Join(tb.TempDir(), "workflow-webhook-url")
	if err := os.WriteFile(path, []byte("https://workflow.example.test/hook"), 0o600); err != nil {
		tb.Fatalf("write workflow webhook url: %v", err)
	}
	if err := bridge.store.Update(context.Background(), func(state *teamstore.State) error {
		state.Workflow = teamstore.WorkflowNotificationConfig{
			Enabled:               true,
			ControlWebhookURLFile: path,
			ControlChatID:         bridge.reg.ControlChatID,
			UpdatedAt:             time.Now(),
		}
		return nil
	}); err != nil {
		tb.Fatalf("configure workflow notifications in perf store: %v", err)
	}
}

func cxpPerfRunMainLoopIdleTick(ctx context.Context, bridge *Bridge, opts BridgeOptions, now time.Time) error {
	bridge.lastTranscriptSync = time.Time{}
	bridge.lastHistoryWatchSync = time.Time{}
	bridge.lastHistoryWatchReconcile = now
	bridge.lastBeaconReconcile = time.Time{}
	bridge.lastBeaconLeaseMaintenance = time.Time{}
	return cxpPerfRunMainLoopSteadyIdleTick(ctx, bridge, opts, now)
}

func cxpPerfMarkMainLoopMaintenanceFresh(bridge *Bridge, now time.Time) {
	bridge.lastTranscriptSync = now
	bridge.lastHistoryWatchSync = now
	bridge.lastHistoryWatchReconcile = now
	bridge.lastBeaconReconcile = now
	bridge.lastBeaconLeaseMaintenance = now
}

func cxpPerfRunMainLoopSteadyIdleTick(ctx context.Context, bridge *Bridge, opts BridgeOptions, now time.Time) error {
	if active, err := bridge.refreshControlLease(ctx); err != nil {
		return err
	} else if !active {
		return teamstore.ErrControlLeaseNotHeld
	}
	if err := bridge.flushPendingOutboxMainLoop(ctx); err != nil && !isOutboxDeliveryDeferred(err) {
		return err
	}
	if err := bridge.flushPendingWorkflowNotificationsWithLimit(ctx, mainLoopWorkflowFlushMaxNotifications); err != nil {
		return err
	}
	if err := bridge.pollOnce(ctx, opts.Top); err != nil && !isGraphRateLimitError(err) {
		return err
	}
	if err := bridge.maybeRunIdleWorkChatAutoPark(ctx, now); err != nil && !isGraphRateLimitError(err) && !isOutboxDeliveryDeferred(err) {
		return err
	}
	if err := bridge.syncLinkedTranscriptsIfDue(ctx, now); err != nil {
		return err
	}
	if err := bridge.syncCodexHistoryFinalsIfDue(ctx, now); err != nil {
		return err
	}
	if err := bridge.maybeRunHelperAutoUpdate(ctx, opts); err != nil {
		return err
	}
	if _, err := bridge.queueCompletedHelperUpgradeNoticeIfNeeded(ctx); err != nil {
		return err
	}
	if err := bridge.maybeRunPendingCodexUpgrade(ctx); err != nil {
		return err
	}
	if err := bridge.maybeRunBeaconReconcile(ctx, now); err != nil {
		return err
	}
	if err := bridge.maybeRunBeaconLeaseMaintenance(ctx, now); err != nil {
		return err
	}
	if _, err := bridge.drainComplete(ctx); err != nil {
		return err
	}
	if err := bridge.processDeferredInbound(ctx); err != nil {
		return err
	}
	if err := bridge.processQueuedTurns(ctx); err != nil {
		return err
	}
	if err := bridge.sendDeferredInterruptedTurnNotices(ctx); err != nil {
		return err
	}
	return bridge.Save()
}

func cxpPerfDrainAsyncTurns(ctx context.Context, bridge *Bridge) error {
	for attempts := 0; attempts < 1000; attempts++ {
		bridge.asyncTurnWG.Wait()
		if err := bridge.processQueuedTurns(ctx); err != nil {
			return err
		}
		hasQueued, err := bridge.store.HasQueuedTurns(ctx)
		if err != nil {
			return err
		}
		if hasQueued {
			continue
		}
		running, err := bridge.store.RunningTurnSessionIDs(ctx)
		if err != nil {
			return err
		}
		if len(running) == 0 {
			return nil
		}
	}
	return fmt.Errorf("async Teams turns did not drain")
}

type cxpPerfExecutor struct {
	mode cxpPerfCodexMode
}

type cxpPerfBlockingExecutor struct {
	started     chan struct{}
	releaseChan chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

func newCXPPerfBlockingExecutor() *cxpPerfBlockingExecutor {
	return &cxpPerfBlockingExecutor{
		started:     make(chan struct{}),
		releaseChan: make(chan struct{}),
	}
}

func (e *cxpPerfBlockingExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunInput(ctx, session, ExecutionInput{Prompt: prompt})
}

func (e *cxpPerfBlockingExecutor) RunInput(ctx context.Context, session *Session, input ExecutionInput) (ExecutionResult, error) {
	e.startOnce.Do(func() {
		close(e.started)
	})
	select {
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	case <-e.releaseChan:
	}
	sessionID := "session"
	threadID := "perf-thread-" + sessionID
	if session != nil {
		sessionID = firstNonEmptyString(session.ID, sessionID)
		threadID = firstNonEmptyString(session.CodexThreadID, "perf-thread-"+sessionID)
	}
	return ExecutionResult{
		Text:             "perf codex result for " + strings.TrimSpace(input.Prompt),
		CodexThreadID:    threadID,
		CodexThreadTitle: "Perf Thread",
		CodexTurnID:      "perf-codex-turn-" + shortStableID(sessionID+":"+input.Prompt),
	}, nil
}

func (e *cxpPerfBlockingExecutor) waitStarted(tb testing.TB) {
	tb.Helper()
	select {
	case <-e.started:
	case <-time.After(5 * time.Second):
		tb.Fatal("perf blocking executor did not start")
	}
}

func (e *cxpPerfBlockingExecutor) release() {
	e.releaseOnce.Do(func() {
		close(e.releaseChan)
	})
}

func (e cxpPerfExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunInput(ctx, session, ExecutionInput{Prompt: prompt})
}

func (e cxpPerfExecutor) RunInput(ctx context.Context, session *Session, input ExecutionInput) (ExecutionResult, error) {
	return e.run(ctx, session, input, nil)
}

func (e cxpPerfExecutor) RunInputWithEventHandler(ctx context.Context, session *Session, input ExecutionInput, handler codexrunner.EventHandler) (ExecutionResult, error) {
	return e.run(ctx, session, input, handler)
}

func (e cxpPerfExecutor) run(ctx context.Context, session *Session, input ExecutionInput, handler codexrunner.EventHandler) (ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, err
	}
	sessionID := "control"
	threadID := "perf-thread-control"
	if session != nil {
		sessionID = firstNonEmptyString(session.ID, "session")
		threadID = firstNonEmptyString(session.CodexThreadID, "perf-thread-"+sessionID)
	}
	turnID := "perf-codex-turn-" + shortStableID(sessionID+":"+input.Prompt)
	if handler != nil && e.mode == cxpPerfCodexStreaming {
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventThreadStarted, ThreadID: threadID})
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventTurnStarted, ThreadID: threadID, TurnID: turnID})
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, ThreadID: threadID, TurnID: turnID, Text: "checking perf scenario"})
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventCommandStarted, ThreadID: threadID, TurnID: turnID, Command: "go test ./internal/teams"})
		exitCode := 0
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventCommandCompleted, ThreadID: threadID, TurnID: turnID, Command: "go test ./internal/teams", ExitCode: &exitCode, AggregatedOutput: "ok"})
	}
	result := ExecutionResult{
		Text:             "perf codex result for " + strings.TrimSpace(input.Prompt),
		CodexThreadID:    threadID,
		CodexThreadTitle: "Perf Thread",
		CodexTurnID:      turnID,
	}
	switch e.mode {
	case "", cxpPerfCodexSuccess, cxpPerfCodexStreaming:
		return result, nil
	case cxpPerfCodexFailure:
		return result, fmt.Errorf("simulated codex execution error")
	case cxpPerfCodexAmbiguous:
		return result, &AmbiguousExecutionError{ThreadID: threadID, TurnID: turnID, Err: fmt.Errorf("simulated codex disconnect after accept")}
	case cxpPerfCodexCanceled:
		return result, context.Canceled
	case cxpPerfCodexThreadSwitch:
		result.CodexThreadID = threadID + "-unexpected"
		return result, nil
	default:
		return result, nil
	}
}

type cxpPerfServiceHarness struct {
	restarts atomic.Int32
	reloads  atomic.Int32
	upgrades atomic.Int32
}

func (h *cxpPerfServiceHarness) restart(context.Context) error {
	h.restarts.Add(1)
	return nil
}

func (h *cxpPerfServiceHarness) reload(ctx context.Context, opts HelperReloadOptions) error {
	h.reloads.Add(1)
	if opts.BeforeRestart != nil {
		return opts.BeforeRestart(ctx)
	}
	return nil
}

func (h *cxpPerfServiceHarness) upgradeCodex(context.Context) (CodexUpgradeResult, error) {
	h.upgrades.Add(1)
	return CodexUpgradeResult{Path: "/tmp/cxp-perf/codex"}, nil
}

func (h *cxpPerfServiceHarness) waitRestart(t *testing.T) {
	t.Helper()
	h.waitCount(t, &h.restarts, "restart")
}

func (h *cxpPerfServiceHarness) waitReload(t *testing.T) {
	t.Helper()
	h.waitCount(t, &h.reloads, "reload")
}

func (h *cxpPerfServiceHarness) waitCount(t *testing.T, counter *atomic.Int32, label string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if counter.Load() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("service %s hook was not called", label)
}

func newCXPPerfStore(b testing.TB, profile cxpPerfProfile) *teamstore.Store {
	b.Helper()
	store, err := teamstore.Open(filepath.Join(b.TempDir(), "state.json"))
	if err != nil {
		b.Fatalf("open perf store: %v", err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Fatalf("close perf store: %v", err)
		}
	})
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		cxpPerfSeedState(state, profile)
		return nil
	}); err != nil {
		b.Fatalf("seed perf store: %v", err)
	}
	return store
}

func cxpPerfRealisticMixedUserProfile() cxpPerfProfile {
	return cxpPerfProfile{
		Name:            "realistic-mixed-user",
		Description:     "100 active mixed-length chats and 2000 parked chats",
		WorkChats:       2100,
		TurnsPerChat:    0,
		MessagesPerPoll: 0,
		MessageBytes:    256,
		OutboxPerChat:   0,
		LookupPerCycle:  128,
		HistoryFiles:    0,
		HistoryLines:    0,
	}
}

func newCXPPerfRealisticMixedUserFixture(tb testing.TB) (*teamstore.Store, *Bridge) {
	tb.Helper()
	profile := cxpPerfRealisticMixedUserProfile()
	store := newCXPPerfStore(tb, profile)
	graph := newCXPPerfGraph(profile)
	bridge := newCXPPerfBridge(store, graph, profile)
	bridge.asyncTurns = true
	cxpPerfSeedColdRuntimeMetadata(tb, store, profile)
	cxpPerfSeedRealisticMixedUserState(tb, store, bridge)
	cxpPerfMigrateStoreToSQLite(tb, store)
	cxpPerfPrepareActiveOwner(tb, bridge)
	return store, bridge
}

const (
	cxpPerfLongFinalHistoricLines = 40
	cxpPerfLongFinalHistoricBytes = 128
	cxpPerfLongFinalStatusRecords = 8
	cxpPerfLongFinalStatusBytes   = 128
)

func newCXPPerfLongTranscriptFinalArrivalFixture(tb testing.TB, index int) (*teamstore.Store, *Bridge, *Session, teamstore.Turn, ExecutionResult) {
	tb.Helper()
	store, bridge := newCXPPerfRealisticMixedUserFixture(tb)
	ctx := context.Background()
	sessionID := cxpPerfSessionID(0)
	session := bridge.reg.SessionByID(sessionID)
	if session == nil {
		tb.Fatalf("realistic session %s not found", sessionID)
	}
	threadID := fmt.Sprintf("perf-long-final-thread-%06d", index)
	codexTurnID := fmt.Sprintf("perf-long-final-codex-turn-%06d", index)
	turnID := fmt.Sprintf("perf-long-final-turn-%06d", index)
	root := filepath.Join(tb.TempDir(), "codex-home")
	transcriptPath, checkpointLine, checkpointOffset, finalText := cxpPerfWriteLongFinalArrivalTranscript(tb, root, threadID, codexTurnID, index)
	info, err := os.Stat(transcriptPath)
	if err != nil {
		tb.Fatalf("stat long final transcript: %v", err)
	}
	now := time.Date(2026, 5, 23, 10, 15, 0, 0, time.UTC).Add(time.Duration(index) * time.Second)
	session.CodexThreadID = threadID
	session.Cwd = filepath.Join(root, "project")
	session.UpdatedAt = now
	turn := teamstore.Turn{
		ID:            turnID,
		SessionID:     session.ID,
		Status:        teamstore.TurnStatusRunning,
		CodexThreadID: threadID,
		CodexTurnID:   codexTurnID,
		QueuedAt:      now.Add(-2 * time.Minute),
		StartedAt:     now.Add(-time.Minute),
		CreatedAt:     now.Add(-2 * time.Minute),
		UpdatedAt:     now,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		stored := state.Sessions[session.ID]
		stored.ID = session.ID
		stored.Status = teamstore.SessionStatusActive
		stored.TeamsChatID = session.ChatID
		stored.TeamsChatURL = session.ChatURL
		stored.TeamsTopic = session.Topic
		stored.CodexThreadID = threadID
		stored.CodexHome = root
		stored.Cwd = session.Cwd
		stored.UpdatedAt = now
		state.Sessions[session.ID] = stored
		state.Turns[turn.ID] = turn
		state.ImportCheckpoints[transcriptCheckpointID(session.ID)] = teamstore.ImportCheckpoint{
			ID:             transcriptCheckpointID(session.ID),
			SessionID:      session.ID,
			SourcePath:     transcriptPath,
			LastRecordID:   "perf-long-final-checkpoint",
			LastSourceLine: checkpointLine,
			LastOffset:     checkpointOffset,
			SourceSize:     checkpointOffset,
			SourceModTime:  info.ModTime(),
			Status:         importCheckpointStatusComplete,
			UpdatedAt:      now.Add(-30 * time.Second),
		}
		for i := 0; i < cxpPerfLongFinalStatusRecords; i++ {
			body := cxpPerfLongFinalStatusBody(i)
			created := now.Add(time.Duration(i-cxpPerfLongFinalStatusRecords) * time.Millisecond)
			outboxID := fmt.Sprintf("perf-long-final-live-progress-%04d", i)
			state.OutboxMessages[outboxID] = teamstore.OutboxMessage{
				ID:             outboxID,
				SessionID:      session.ID,
				TurnID:         turn.ID,
				CodexThreadID:  threadID,
				TeamsChatID:    session.ChatID,
				ScopeID:        bridge.scope.ID,
				MachineID:      bridge.machine.ID,
				Kind:           fmt.Sprintf("codex-progress-%03d", i+1),
				Body:           body,
				Sequence:       int64(10000 + i),
				PartIndex:      1,
				PartCount:      1,
				SourceTextHash: normalizedTextHash(body),
				RenderedHash:   normalizedTextHash(body),
				RenderedBytes:  len(body),
				Status:         teamstore.OutboxStatusSent,
				TeamsMessageID: fmt.Sprintf("teams-long-final-progress-%04d", i),
				CreatedAt:      created,
				UpdatedAt:      created,
				SentAt:         created,
			}
		}
		return nil
	}); err != nil {
		tb.Fatalf("seed long final arrival fixture: %v", err)
	}
	bridge.leaseDuration = time.Hour
	active, err := bridge.claimControlLease(ctx)
	if err != nil {
		tb.Fatalf("refresh long final fixture control lease: %v", err)
	}
	if !active {
		tb.Fatal("long final fixture control lease unexpectedly inactive")
	}
	result := ExecutionResult{
		Text:             finalText,
		CodexThreadID:    threadID,
		CodexThreadTitle: "Long Final Arrival",
		CodexTurnID:      codexTurnID,
	}
	return store, bridge, session, turn, result
}

func newCXPPerfStatusOnlyFinalArrivalFixture(tb testing.TB, index int) (*teamstore.Store, *Bridge, *Session, teamstore.Turn) {
	tb.Helper()
	store, err := teamstore.Open(filepath.Join(tb.TempDir(), "state.json"))
	if err != nil {
		tb.Fatalf("Open status-only store: %v", err)
	}
	tb.Cleanup(func() {
		if err := store.Close(); err != nil {
			tb.Fatalf("Close status-only store: %v", err)
		}
	})
	now := time.Now()
	user := User{ID: "user-1", UserPrincipalName: "user@example.test"}
	scope := ScopeIdentityForUser(user)
	scope.ID = "test-scope:" + shortStableID(store.Path())
	machine := MachineRecordForUser(user, scope)
	bridge := &Bridge{
		user:    user,
		scope:   scope,
		machine: machine,
		reg: Registry{
			Version:       1,
			UserID:        user.ID,
			ControlChatID: "control-chat",
			Sessions: []Session{{
				ID:        "s001",
				ChatID:    "chat-1",
				ChatURL:   "https://teams.example/chat-1",
				Topic:     "topic",
				Status:    "active",
				CreatedAt: now,
				UpdatedAt: now,
			}},
			Chats: map[string]ChatState{},
		},
		store: store,
	}
	ctx := context.Background()
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		tb.Fatal("missing test session s001")
	}
	threadID := fmt.Sprintf("perf-status-only-thread-%06d", index)
	codexTurnID := fmt.Sprintf("perf-status-only-codex-turn-%06d", index)
	turnID := fmt.Sprintf("perf-status-only-turn-%06d", index)
	root := filepath.Join(tb.TempDir(), "codex-home")
	transcriptPath, checkpointLine, checkpointOffset := cxpPerfWriteStatusOnlyFinalArrivalTranscript(tb, root, threadID, codexTurnID)
	info, err := os.Stat(transcriptPath)
	if err != nil {
		tb.Fatalf("stat status-only transcript: %v", err)
	}
	fixtureNow := time.Date(2026, 5, 23, 10, 15, 0, 0, time.UTC).Add(time.Duration(index) * time.Second)
	session.CodexThreadID = threadID
	session.Cwd = filepath.Join(root, "project")
	turn := teamstore.Turn{
		ID:            turnID,
		SessionID:     session.ID,
		Status:        teamstore.TurnStatusRunning,
		CodexThreadID: threadID,
		CodexTurnID:   codexTurnID,
		QueuedAt:      fixtureNow.Add(-2 * time.Minute),
		StartedAt:     fixtureNow.Add(-time.Minute),
		CreatedAt:     fixtureNow.Add(-2 * time.Minute),
		UpdatedAt:     fixtureNow,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		stored := state.Sessions[session.ID]
		stored.ID = session.ID
		stored.Status = teamstore.SessionStatusActive
		stored.TeamsChatID = session.ChatID
		stored.TeamsChatURL = session.ChatURL
		stored.TeamsTopic = session.Topic
		stored.CodexThreadID = threadID
		stored.CodexHome = root
		stored.Cwd = session.Cwd
		stored.UpdatedAt = fixtureNow
		state.Sessions[session.ID] = stored
		state.Turns[turn.ID] = turn
		state.ImportCheckpoints[transcriptCheckpointID(session.ID)] = teamstore.ImportCheckpoint{
			ID:             transcriptCheckpointID(session.ID),
			SessionID:      session.ID,
			SourcePath:     transcriptPath,
			LastRecordID:   "perf-status-only-checkpoint",
			LastSourceLine: checkpointLine,
			LastOffset:     checkpointOffset,
			SourceSize:     checkpointOffset,
			SourceModTime:  info.ModTime(),
			Status:         importCheckpointStatusComplete,
			UpdatedAt:      fixtureNow.Add(-30 * time.Second),
		}
		return nil
	}); err != nil {
		tb.Fatalf("seed status-only final fixture: %v", err)
	}
	cxpPerfMigrateStoreToSQLite(tb, store)
	return store, bridge, session, turn
}

func cxpPerfWriteLongFinalArrivalTranscript(tb testing.TB, root string, threadID string, codexTurnID string, index int) (string, int, int64, string) {
	tb.Helper()
	dir := filepath.Join(root, "sessions", "2026", "05", "23")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		tb.Fatalf("mkdir long final transcript dir: %v", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("rollout-2026-05-23T10-15-00-%s.jsonl", threadID))
	var builder strings.Builder
	builder.Grow(cxpPerfLongFinalHistoricLines*(cxpPerfLongFinalHistoricBytes+256) + cxpPerfLongFinalStatusRecords*(cxpPerfLongFinalStatusBytes+256))
	lineNo := 0
	var offset int64
	appendLine := func(line string) {
		lineNo++
		builder.WriteString(line)
		builder.WriteByte('\n')
		offset += int64(len(line) + 1)
	}
	appendLine(`{"type":"session_meta","payload":{"id":` + strconv.Quote(threadID) + `}}`)
	base := time.Date(2026, 5, 23, 9, 0, 0, 0, time.UTC)
	historicText := cxpPerfText(cxpPerfLongFinalHistoricBytes)
	for i := 0; i < cxpPerfLongFinalHistoricLines; i++ {
		appendLine(`{"timestamp":` + strconv.Quote(base.Add(time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano)) + `,"type":"event_msg","payload":{"type":"agent_message","id":` + strconv.Quote(fmt.Sprintf("perf-long-final-history-%05d", i)) + `,"thread_id":` + strconv.Quote(threadID) + `,"turn_id":` + strconv.Quote(fmt.Sprintf("perf-long-final-history-turn-%05d", i)) + `,"phase":"final_answer","message":` + strconv.Quote(historicText) + `}}`)
	}
	appendLine(`{"timestamp":` + strconv.Quote(base.Add(time.Hour).Format(time.RFC3339Nano)) + `,"type":"event_msg","payload":{"type":"agent_message","id":"perf-long-final-checkpoint","thread_id":` + strconv.Quote(threadID) + `,"turn_id":"perf-long-final-checkpoint-turn","phase":"final_answer","message":"checkpoint before live statuses"}}`)
	checkpointLine := lineNo
	checkpointOffset := offset
	for i := 0; i < cxpPerfLongFinalStatusRecords; i++ {
		appendLine(`{"timestamp":` + strconv.Quote(base.Add(time.Hour+time.Duration(i+1)*time.Second).Format(time.RFC3339Nano)) + `,"type":"event_msg","payload":{"type":"agent_message","id":` + strconv.Quote(fmt.Sprintf("perf-long-final-status-%04d", i)) + `,"thread_id":` + strconv.Quote(threadID) + `,"turn_id":` + strconv.Quote(codexTurnID) + `,"phase":"commentary","message":` + strconv.Quote(cxpPerfLongFinalStatusBody(i)) + `}}`)
	}
	finalText := "long transcript final answer " + cxpPerfText(4096)
	appendLine(`{"timestamp":` + strconv.Quote(base.Add(2*time.Hour).Format(time.RFC3339Nano)) + `,"type":"event_msg","payload":{"type":"agent_message","id":"perf-long-final-answer","thread_id":` + strconv.Quote(threadID) + `,"turn_id":` + strconv.Quote(codexTurnID) + `,"phase":"final_answer","message":` + strconv.Quote(finalText) + `}}`)
	if err := os.WriteFile(path, []byte(builder.String()), 0o600); err != nil {
		tb.Fatalf("write long final transcript: %v", err)
	}
	return path, checkpointLine, checkpointOffset, finalText
}

func cxpPerfWriteStatusOnlyFinalArrivalTranscript(tb testing.TB, root string, threadID string, codexTurnID string) (string, int, int64) {
	tb.Helper()
	dir := filepath.Join(root, "sessions", "2026", "05", "23")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		tb.Fatalf("mkdir status-only transcript dir: %v", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("rollout-2026-05-23T10-15-00-%s.jsonl", threadID))
	var builder strings.Builder
	builder.Grow(cxpPerfLongFinalStatusRecords * (cxpPerfLongFinalStatusBytes + 256))
	lineNo := 0
	var offset int64
	appendLine := func(line string) {
		lineNo++
		builder.WriteString(line)
		builder.WriteByte('\n')
		offset += int64(len(line) + 1)
	}
	base := time.Date(2026, 5, 23, 9, 0, 0, 0, time.UTC)
	appendLine(`{"type":"session_meta","payload":{"id":` + strconv.Quote(threadID) + `}}`)
	appendLine(`{"timestamp":` + strconv.Quote(base.Add(time.Hour).Format(time.RFC3339Nano)) + `,"type":"event_msg","payload":{"type":"agent_message","id":"perf-status-only-checkpoint","thread_id":` + strconv.Quote(threadID) + `,"turn_id":"perf-status-only-checkpoint-turn","phase":"final_answer","message":"checkpoint before live statuses"}}`)
	checkpointLine := lineNo
	checkpointOffset := offset
	for i := 0; i < cxpPerfLongFinalStatusRecords; i++ {
		appendLine(`{"timestamp":` + strconv.Quote(base.Add(time.Hour+time.Duration(i+1)*time.Second).Format(time.RFC3339Nano)) + `,"type":"event_msg","payload":{"type":"agent_message","id":` + strconv.Quote(fmt.Sprintf("perf-status-only-status-%04d", i)) + `,"thread_id":` + strconv.Quote(threadID) + `,"turn_id":` + strconv.Quote(codexTurnID) + `,"phase":"commentary","message":` + strconv.Quote(cxpPerfLongFinalStatusBody(i)) + `}}`)
	}
	appendLine(`{"timestamp":` + strconv.Quote(base.Add(2*time.Hour).Format(time.RFC3339Nano)) + `,"type":"event_msg","payload":{"type":"agent_message","id":"perf-status-only-final","thread_id":` + strconv.Quote(threadID) + `,"turn_id":` + strconv.Quote(codexTurnID) + `,"phase":"final_answer","message":"done"}}`)
	if err := os.WriteFile(path, []byte(builder.String()), 0o600); err != nil {
		tb.Fatalf("write status-only transcript: %v", err)
	}
	return path, checkpointLine, checkpointOffset
}

func cxpPerfLongFinalStatusBody(index int) string {
	return fmt.Sprintf("perf live progress status %04d %s", index, cxpPerfText(cxpPerfLongFinalStatusBytes))
}

func cxpPerfCountSessionTranscriptDeliveries(tb testing.TB, store *teamstore.Store, sessionID string, status teamstore.TranscriptDeliveryStatus) int {
	tb.Helper()
	state, err := store.SessionTranscriptDedupeSnapshot(context.Background(), sessionID, transcriptCheckpointID(sessionID))
	if err != nil {
		tb.Fatalf("load transcript delivery count snapshot: %v", err)
	}
	count := 0
	for _, delivery := range state.TranscriptDeliveries {
		if delivery.SessionID == sessionID && delivery.Status == status {
			count++
		}
	}
	return count
}

func cxpPerfSeedRealisticMixedUserState(tb testing.TB, store *teamstore.Store, bridge *Bridge) {
	tb.Helper()
	now := time.Date(2026, 5, 23, 9, 45, 0, 0, time.UTC)
	oldActivity := now.Add(-49 * time.Hour)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ChatPolls[bridge.reg.ControlChatID] = teamstore.ChatPollState{
			ChatID:               bridge.reg.ControlChatID,
			Seeded:               true,
			PollState:            inboundPollStateWarm,
			NextPollAt:           now.Add(time.Hour),
			LastActivityAt:       now.Add(-time.Minute),
			LastModifiedCursor:   now.Add(-time.Minute),
			LastSuccessfulPollAt: now.Add(-time.Minute),
			UpdatedAt:            now,
		}
		for chat := 0; chat < 2100; chat++ {
			sessionID := cxpPerfSessionID(chat)
			chatID := cxpPerfChatID(chat)
			session := state.Sessions[sessionID]
			session.ID = sessionID
			session.TeamsChatID = chatID
			session.TeamsChatURL = "https://teams.example/" + chatID
			session.TeamsTopic = "perf"
			session.Cwd = fmt.Sprintf("/workspace/project-%03d", chat)
			session.Status = teamstore.SessionStatusActive
			if chat < 100 {
				session.UpdatedAt = now.Add(-time.Duration(chat%90) * time.Second)
				session.CodexThreadID = fmt.Sprintf("perf-thread-%03d", chat)
				state.Sessions[sessionID] = session
				state.ChatPolls[chatID] = teamstore.ChatPollState{
					ChatID:               chatID,
					Seeded:               true,
					PollState:            inboundPollStateWarm,
					NextPollAt:           now.Add(-time.Duration(chat%30) * time.Second),
					LastActivityAt:       session.UpdatedAt,
					LastModifiedCursor:   session.UpdatedAt.Add(-time.Minute),
					LastSuccessfulPollAt: session.UpdatedAt.Add(-time.Minute),
					UpdatedAt:            session.UpdatedAt,
				}
				cxpPerfSeedRealisticChatHistory(state, chat, sessionID, chatID, session.CodexThreadID, now)
			} else {
				session.UpdatedAt = oldActivity
				state.Sessions[sessionID] = session
				state.ChatPolls[chatID] = teamstore.ChatPollState{
					ChatID:               chatID,
					Seeded:               true,
					PollState:            inboundPollStateParked,
					LastActivityAt:       oldActivity,
					LastModifiedCursor:   oldActivity,
					LastSuccessfulPollAt: oldActivity,
					ParkedAt:             oldActivity.Add(48 * time.Hour),
					ParkNoticeSentAt:     oldActivity.Add(48*time.Hour + time.Minute),
					UpdatedAt:            now.Add(-time.Hour),
				}
			}
		}
		return nil
	}); err != nil {
		tb.Fatalf("seed realistic mixed user state: %v", err)
	}
	if bridge != nil {
		for i := range bridge.reg.Sessions {
			if i < 100 {
				bridge.reg.Sessions[i].UpdatedAt = now.Add(-time.Duration(i%90) * time.Second)
				bridge.reg.Sessions[i].CodexThreadID = fmt.Sprintf("perf-thread-%03d", i)
			} else {
				bridge.reg.Sessions[i].UpdatedAt = oldActivity
			}
		}
	}
}

func cxpPerfSeedRealisticChatHistory(state *teamstore.State, chat int, sessionID string, chatID string, threadID string, now time.Time) {
	turns := 8
	outboxes := 2
	switch {
	case chat < 50:
		turns = 500
		outboxes = 50
	case chat < 80:
		turns = 120
		outboxes = 12
	}
	for turn := 0; turn < turns; turn++ {
		created := now.Add(-time.Duration(turns-turn) * time.Minute)
		inboundID := fmt.Sprintf("realistic-inbound-%03d-%05d", chat, turn)
		turnID := fmt.Sprintf("realistic-turn-%03d-%05d", chat, turn)
		messageID := fmt.Sprintf("realistic-user-message-%03d-%05d", chat, turn)
		state.InboundEvents[inboundID] = teamstore.InboundEvent{
			ID:             inboundID,
			SessionID:      sessionID,
			TeamsChatID:    chatID,
			TeamsMessageID: messageID,
			Text:           cxpPerfText(256),
			Status:         teamstore.InboundStatusPersisted,
			CreatedAt:      created,
			UpdatedAt:      created,
		}
		state.Turns[turnID] = teamstore.Turn{
			ID:             turnID,
			SessionID:      sessionID,
			InboundEventID: inboundID,
			Status:         teamstore.TurnStatusCompleted,
			CodexThreadID:  threadID,
			CodexTurnID:    fmt.Sprintf("realistic-codex-turn-%03d-%05d", chat, turn),
			QueuedAt:       created,
			StartedAt:      created.Add(time.Second),
			CompletedAt:    created.Add(2 * time.Second),
			CreatedAt:      created,
			UpdatedAt:      created.Add(2 * time.Second),
		}
		state.MessageProvenance[fmt.Sprintf("realistic-user-provenance-%03d-%05d", chat, turn)] = teamstore.MessageProvenanceRecord{
			ID:             fmt.Sprintf("realistic-user-provenance-%03d-%05d", chat, turn),
			TeamsChatID:    chatID,
			TeamsMessageID: messageID,
			Origin:         teamstore.MessageOriginUserInbound,
			SessionID:      sessionID,
			InboundID:      inboundID,
			CreatedAt:      created,
			UpdatedAt:      created,
		}
	}
	for outbox := 0; outbox < outboxes; outbox++ {
		created := now.Add(-time.Duration(outboxes-outbox) * time.Minute)
		outboxID := fmt.Sprintf("realistic-outbox-%03d-%04d", chat, outbox)
		messageID := fmt.Sprintf("realistic-helper-message-%03d-%04d", chat, outbox)
		state.OutboxMessages[outboxID] = teamstore.OutboxMessage{
			ID:             outboxID,
			SessionID:      sessionID,
			TeamsChatID:    chatID,
			Kind:           "final",
			Body:           cxpPerfText(256),
			Status:         teamstore.OutboxStatusSent,
			TeamsMessageID: messageID,
			SourceTextHash: normalizedTextHash(cxpPerfText(256)),
			CreatedAt:      created,
			UpdatedAt:      created,
			SentAt:         created,
		}
		state.MessageProvenance[fmt.Sprintf("realistic-helper-provenance-%03d-%04d", chat, outbox)] = teamstore.MessageProvenanceRecord{
			ID:             fmt.Sprintf("realistic-helper-provenance-%03d-%04d", chat, outbox),
			TeamsChatID:    chatID,
			TeamsMessageID: messageID,
			Origin:         teamstore.MessageOriginHelperOutbox,
			SessionID:      sessionID,
			OutboxID:       outboxID,
			CreatedAt:      created,
			UpdatedAt:      created,
		}
	}
}

func cxpPerfSeedRealisticRunningTurn(tb testing.TB, store *teamstore.Store) (string, string, string) {
	tb.Helper()
	sessionID := cxpPerfSessionID(0)
	chatID := cxpPerfChatID(0)
	turnID := "realistic-running-turn"
	now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.Turns[turnID] = teamstore.Turn{
			ID:            turnID,
			SessionID:     sessionID,
			Status:        teamstore.TurnStatusRunning,
			CodexThreadID: "perf-thread-000",
			CodexTurnID:   "realistic-codex-running-turn",
			QueuedAt:      now.Add(-time.Minute),
			StartedAt:     now,
			CreatedAt:     now.Add(-time.Minute),
			UpdatedAt:     now,
		}
		return nil
	}); err != nil {
		tb.Fatalf("seed realistic running turn: %v", err)
	}
	return sessionID, chatID, turnID
}

func cxpPerfHandleRealisticUserMessage(ctx context.Context, bridge *Bridge, index int) error {
	chatID, msg, text := cxpPerfRealisticUserMessage(index)
	return bridge.handleSessionMessage(ctx, chatID, msg, text)
}

func cxpPerfRealisticUserMessage(index int) (string, ChatMessage, string) {
	chat := index % 100
	chatID := cxpPerfChatID(chat)
	messageID := fmt.Sprintf("realistic-live-message-%06d", index)
	now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC).Add(time.Duration(index) * 90 * time.Second)
	msg := ChatMessage{
		ID:              messageID,
		ChatID:          chatID,
		CreatedDateTime: now.Format(time.RFC3339),
	}
	msg.Body.ContentType = "html"
	msg.Body.Content = "<p>realistic user message</p>"
	return chatID, msg, "realistic user message"
}

func cxpPerfSeedState(state *teamstore.State, profile cxpPerfProfile) {
	now := time.Date(2026, 5, 23, 7, 0, 0, 0, time.UTC)
	state.Scope = teamstore.ScopeIdentity{ID: "perf-scope", AccountID: "perf-user", OSUser: "perf", Profile: "perf", ConfigPath: "/tmp/cxp-perf/config.toml", CodexHome: "/tmp/cxp-perf/codex", CreatedAt: now, UpdatedAt: now}
	machine := teamstore.MachineRecord{ID: "perf-machine", ScopeID: state.Scope.ID, Hostname: "perf-host", OSUser: "perf", AccountID: state.Scope.AccountID, Profile: state.Scope.Profile, Kind: teamstore.MachineKindPrimary, Priority: teamstore.DefaultMachinePriority(teamstore.MachineKindPrimary), Status: teamstore.MachineStatusActive, LastSeen: now, CreatedAt: now, UpdatedAt: now}
	state.Machines[machine.ID] = machine
	state.MachineIdentity = teamstore.MachineIdentity{
		ID:            machine.ID,
		Label:         machine.Label,
		Hostname:      machine.Hostname,
		AccountID:     machine.AccountID,
		UserPrincipal: machine.UserPrincipal,
		Profile:       machine.Profile,
		ScopeID:       machine.ScopeID,
		Kind:          machine.Kind,
		Priority:      machine.Priority,
		CreatedAt:     machine.CreatedAt,
		UpdatedAt:     machine.UpdatedAt,
	}
	for chat := 0; chat < profile.WorkChats; chat++ {
		sessionID := cxpPerfSessionID(chat)
		chatID := cxpPerfChatID(chat)
		state.Sessions[sessionID] = teamstore.SessionContext{ID: sessionID, Status: teamstore.SessionStatusActive, TeamsChatID: chatID, Cwd: fmt.Sprintf("/workspace/project-%03d", chat), CreatedAt: now, UpdatedAt: now}
		state.ChatPolls[chatID] = teamstore.ChatPollState{ChatID: chatID, Seeded: true, PollState: "warm", NextPollAt: now.Add(-time.Second), LastActivityAt: now, LastModifiedCursor: now.Add(-time.Minute), LastSuccessfulPollAt: now.Add(-time.Minute), UpdatedAt: now}
		for turn := 0; turn < profile.TurnsPerChat; turn++ {
			created := now.Add(time.Duration(chat*profile.TurnsPerChat+turn) * time.Millisecond)
			inboundID := cxpPerfInboundID(chat, turn)
			messageID := cxpPerfInboundMessageID(chat, turn)
			provenanceID := fmt.Sprintf("perf-provenance-%03d-%06d", chat, turn)
			state.InboundEvents[inboundID] = teamstore.InboundEvent{ID: inboundID, SessionID: sessionID, TeamsChatID: chatID, TeamsMessageID: messageID, Text: cxpPerfText(profile.MessageBytes), Status: teamstore.InboundStatusPersisted, CreatedAt: created, UpdatedAt: created}
			state.MessageProvenance[provenanceID] = teamstore.MessageProvenanceRecord{ID: provenanceID, TeamsChatID: chatID, TeamsMessageID: messageID, Origin: teamstore.MessageOriginUserInbound, SessionID: sessionID, InboundID: inboundID, CreatedAt: created, UpdatedAt: created}
		}
		for outbox := 0; outbox < profile.OutboxPerChat; outbox++ {
			created := now.Add(time.Duration(outbox) * time.Second)
			outboxID := fmt.Sprintf("perf-outbox-%03d-%03d", chat, outbox)
			messageID := fmt.Sprintf("perf-helper-message-%03d-%03d", chat, outbox)
			state.OutboxMessages[outboxID] = teamstore.OutboxMessage{ID: outboxID, SessionID: sessionID, TeamsChatID: chatID, Kind: "answer", Body: cxpPerfText(profile.MessageBytes), Sequence: int64(outbox + 1), PartIndex: 1, PartCount: 1, RenderedHash: normalizedTextHash(cxpPerfText(profile.MessageBytes)), Status: teamstore.OutboxStatusSent, TeamsMessageID: messageID, CreatedAt: created, UpdatedAt: created, SentAt: created}
		}
	}
}

func cxpPerfSeedActiveParkedSessions(b testing.TB, store *teamstore.Store, bridge *Bridge) {
	b.Helper()
	now := time.Now().UTC()
	oldActivity := now.Add(-49 * time.Hour)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.ChatPolls[bridge.reg.ControlChatID] = teamstore.ChatPollState{
			ChatID:               bridge.reg.ControlChatID,
			Seeded:               true,
			PollState:            inboundPollStateWarm,
			NextPollAt:           now.Add(time.Hour),
			LastActivityAt:       now.Add(-time.Minute),
			LastModifiedCursor:   now.Add(-time.Minute),
			LastSuccessfulPollAt: now.Add(-time.Minute),
			UpdatedAt:            now,
		}
		for i := range bridge.reg.Sessions {
			session := &bridge.reg.Sessions[i]
			session.CreatedAt = oldActivity
			session.UpdatedAt = oldActivity
			state.Sessions[session.ID] = teamstore.SessionContext{
				ID:           session.ID,
				Status:       teamstore.SessionStatusActive,
				TeamsChatID:  session.ChatID,
				TeamsChatURL: session.ChatURL,
				TeamsTopic:   session.Topic,
				Cwd:          fmt.Sprintf("/workspace/%s", session.ID),
				CreatedAt:    oldActivity,
				UpdatedAt:    oldActivity,
			}
			state.ChatPolls[session.ChatID] = teamstore.ChatPollState{
				ChatID:               session.ChatID,
				Seeded:               true,
				PollState:            inboundPollStateParked,
				LastActivityAt:       oldActivity,
				LastModifiedCursor:   oldActivity,
				LastSuccessfulPollAt: oldActivity,
				ParkedAt:             oldActivity.Add(48 * time.Hour),
				ParkNoticeSentAt:     oldActivity.Add(48*time.Hour + time.Minute),
				UpdatedAt:            now.Add(-time.Minute),
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("seed active parked sessions: %v", err)
	}
}

func cxpPerfSeedIdleAutoParkCandidates(tb testing.TB, store *teamstore.Store) {
	tb.Helper()
	now := time.Date(2026, 5, 23, 7, 0, 0, 0, time.UTC)
	oldActivity := now.Add(-49 * time.Hour)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		for id, session := range state.Sessions {
			session.UpdatedAt = oldActivity
			state.Sessions[id] = session
		}
		for chatID, poll := range state.ChatPolls {
			poll.PollState = inboundPollStateCold
			poll.NextPollAt = now.Add(-time.Minute)
			poll.LastActivityAt = oldActivity
			poll.LastModifiedCursor = oldActivity
			poll.LastSuccessfulPollAt = oldActivity
			poll.UpdatedAt = now
			state.ChatPolls[chatID] = poll
		}
		return nil
	}); err != nil {
		tb.Fatalf("seed idle auto-park candidates: %v", err)
	}
}

func newCXPPerfBridge(store *teamstore.Store, graph *GraphClient, profile cxpPerfProfile) *Bridge {
	now := time.Date(2026, 5, 23, 7, 30, 0, 0, time.UTC)
	user := User{ID: "perf-user", UserPrincipalName: "perf@example.test"}
	scope := teamstore.ScopeIdentity{ID: "perf-scope", AccountID: user.ID, UserPrincipal: user.UserPrincipalName, OSUser: "perf", Profile: "perf", ConfigPath: "/tmp/cxp-perf/config.toml", CodexHome: "/tmp/cxp-perf/codex", CreatedAt: now, UpdatedAt: now}
	machine := teamstore.MachineRecord{ID: "perf-machine", ScopeID: scope.ID, Hostname: "perf-host", OSUser: "perf", AccountID: scope.AccountID, UserPrincipal: scope.UserPrincipal, Profile: scope.Profile, Kind: teamstore.MachineKindPrimary, Priority: teamstore.DefaultMachinePriority(teamstore.MachineKindPrimary), Status: teamstore.MachineStatusActive, LastSeen: now, CreatedAt: now, UpdatedAt: now}
	bridge := &Bridge{
		graph:        graph,
		readGraph:    graph,
		registryPath: filepath.Join(filepath.Dir(store.Path()), "registry.json"),
		user:         user,
		scope:        scope,
		machine:      machine,
		executor:     EchoExecutor{},
		store:        store,
		reg: Registry{
			Version:          1,
			UserID:           user.ID,
			UserPrincipal:    user.UserPrincipalName,
			ControlChatID:    "control-chat",
			ControlChatURL:   "https://teams.example/control-chat",
			ControlChatTopic: "perf control",
			Chats:            map[string]ChatState{},
		},
	}
	for chat := 0; chat < profile.WorkChats; chat++ {
		bridge.reg.Sessions = append(bridge.reg.Sessions, Session{ID: cxpPerfSessionID(chat), ChatID: cxpPerfChatID(chat), ChatURL: "https://teams.example/" + cxpPerfChatID(chat), Topic: "perf", Status: "active", CreatedAt: now, UpdatedAt: now})
	}
	return bridge
}

type cxpPerfGraphTransport struct {
	mu       sync.Mutex
	profile  cxpPerfProfile
	scenario cxpPerfExternalScenario
	seen     map[string]int
}

func newCXPPerfGraph(profile cxpPerfProfile) *GraphClient {
	return newCXPPerfGraphWithScenario(profile, cxpPerfExternalScenario{})
}

func newCXPPerfGraphWithScenario(profile cxpPerfProfile, scenario cxpPerfExternalScenario) *GraphClient {
	transport := &cxpPerfGraphTransport{profile: profile, scenario: scenario, seen: map[string]int{}}
	client := &http.Client{Transport: roundTripFunc(transport.roundTrip)}
	graph := newGraphClientWithHTTPClient(&fakeGraphAuth{token: "perf-token", refreshedToken: "perf-token"}, io.Discard, client)
	graph.baseURL = "https://graph.example/v1.0"
	graph.maxRetries = 0
	graph.sleep = func(context.Context, time.Duration) error { return nil }
	return graph
}

func (g *cxpPerfGraphTransport) roundTrip(req *http.Request) (*http.Response, error) {
	if g.profile.RateLimited && strings.Contains(req.URL.Path, "/messages") && req.Method == http.MethodGet {
		return cxpPerfJSONResponse(http.StatusTooManyRequests, map[string]any{"error": map[string]any{"code": "TooManyRequests", "message": "perf rate limit"}}, http.Header{"Retry-After": []string{"1"}}), nil
	}
	switch {
	case req.Method == http.MethodGet && req.URL.Path == "/v1.0/me":
		return cxpPerfJSONResponse(http.StatusOK, User{ID: "perf-user", UserPrincipalName: "perf@example.test"}, nil), nil
	case req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/messages"):
		if resp, err, ok := g.graphReadFault(); ok {
			return resp, err
		}
		chatID := cxpPerfChatIDFromGraphPath(req.URL.Path)
		messages := g.nextMessages(chatID)
		return cxpPerfJSONResponse(http.StatusOK, map[string]any{"value": messages}, nil), nil
	case req.Method == http.MethodPost && strings.Contains(req.URL.Path, "/messages"):
		if resp, err, ok := g.graphSendFault(); ok {
			return resp, err
		}
		chatID := cxpPerfChatIDFromGraphPath(req.URL.Path)
		msg := cxpPerfChatMessage(chatID, "sent-"+strconv.FormatInt(time.Now().UnixNano(), 10), time.Now(), "sent")
		return cxpPerfJSONResponse(http.StatusCreated, msg, nil), nil
	default:
		return cxpPerfJSONResponse(http.StatusOK, map[string]any{"value": []any{}}, nil), nil
	}
}

func (g *cxpPerfGraphTransport) graphReadFault() (*http.Response, error, bool) {
	switch g.scenario.GraphMode {
	case cxpPerfGraphReadRateLimited:
		return cxpPerfJSONResponse(http.StatusTooManyRequests, map[string]any{"error": map[string]any{"code": "TooManyRequests", "message": "perf read rate limit"}}, http.Header{"Retry-After": []string{"1"}}), nil, true
	case cxpPerfGraphReadUnauthorized:
		return cxpPerfJSONResponse(http.StatusUnauthorized, map[string]any{"error": map[string]any{"code": "InvalidAuthenticationToken", "message": "perf expired token"}}, nil), nil, true
	case cxpPerfGraphReadForbidden:
		return cxpPerfJSONResponse(http.StatusForbidden, map[string]any{"error": map[string]any{"code": "Forbidden", "message": "perf forbidden"}}, nil), nil, true
	case cxpPerfGraphReadServerError:
		return cxpPerfJSONResponse(http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"code": "ServiceUnavailable", "message": "perf unavailable"}}, nil), nil, true
	case cxpPerfGraphReadNetworkDrop:
		return nil, fmt.Errorf("simulated Graph network drop"), true
	case cxpPerfGraphReadMalformed:
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader("{")),
		}, nil, true
	default:
		return nil, nil, false
	}
}

func (g *cxpPerfGraphTransport) graphSendFault() (*http.Response, error, bool) {
	switch g.scenario.GraphMode {
	case cxpPerfGraphSendRateLimited:
		return cxpPerfJSONResponse(http.StatusTooManyRequests, map[string]any{"error": map[string]any{"code": "TooManyRequests", "message": "perf send rate limit"}}, http.Header{"Retry-After": []string{"1"}}), nil, true
	case cxpPerfGraphSendForbidden:
		return cxpPerfJSONResponse(http.StatusForbidden, map[string]any{"error": map[string]any{"code": "Forbidden", "message": "perf send forbidden"}}, nil), nil, true
	default:
		return nil, nil, false
	}
}

func (g *cxpPerfGraphTransport) nextMessages(chatID string) []ChatMessage {
	if chatID == "control-chat" {
		if strings.TrimSpace(g.scenario.ControlPrompt) == "" {
			return nil
		}
		return []ChatMessage{cxpPerfChatMessage(chatID, "perf-control-"+g.scenario.Name, time.Date(2026, 5, 23, 9, 0, 0, 0, time.UTC), g.scenario.ControlPrompt)}
	}
	if g.profile.MessagesPerPoll <= 0 {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	start := g.seen[chatID]
	g.seen[chatID] += g.profile.MessagesPerPoll
	out := make([]ChatMessage, 0, g.profile.MessagesPerPoll)
	base := time.Date(2026, 5, 23, 9, 0, 0, 0, time.UTC)
	for i := 0; i < g.profile.MessagesPerPoll; i++ {
		id := fmt.Sprintf("perf-graph-%s-%06d", chatID, start+i)
		out = append(out, cxpPerfChatMessage(chatID, id, base.Add(time.Duration(start+i)*time.Second), cxpPerfText(g.profile.MessageBytes)))
	}
	return out
}

func cxpPerfJSONResponse(status int, value any, header http.Header) *http.Response {
	data, _ := json.Marshal(value)
	if header == nil {
		header = http.Header{}
	}
	header.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(string(data))),
	}
}

func cxpPerfChatMessage(chatID string, id string, at time.Time, text string) ChatMessage {
	var msg ChatMessage
	msg.ID = id
	msg.ChatID = chatID
	msg.CreatedDateTime = at.Format(time.RFC3339Nano)
	msg.LastModifiedDateTime = at.Format(time.RFC3339Nano)
	msg.MessageType = "message"
	msg.From.User = &struct {
		ID          string `json:"id"`
		DisplayName string `json:"displayName"`
	}{ID: "perf-user", DisplayName: "Perf User"}
	msg.Body.ContentType = "text"
	msg.Body.Content = text
	return msg
}

func cxpPerfChatIDFromGraphPath(graphPath string) string {
	parts := strings.Split(graphPath, "/")
	for i, part := range parts {
		if part == "chats" && i+1 < len(parts) {
			unescaped, err := url.PathUnescape(parts[i+1])
			if err == nil {
				return unescaped
			}
			return parts[i+1]
		}
	}
	return ""
}

func cxpPerfLookupQueries(profile cxpPerfProfile) []struct {
	chatID    string
	messageID string
} {
	limit := max(1, min(profile.WorkChats, 32))
	out := make([]struct {
		chatID    string
		messageID string
	}, 0, limit)
	for chat := 0; chat < limit; chat++ {
		out = append(out, struct {
			chatID    string
			messageID string
		}{chatID: cxpPerfChatID(chat), messageID: cxpPerfInboundMessageID(chat, 0)})
	}
	return out
}

func cxpPerfText(size int) string {
	if size <= 0 {
		return "perf"
	}
	return strings.Repeat("x", size)
}

func cxpPerfSessionID(index int) string {
	return fmt.Sprintf("perf-session-%03d", index)
}

func cxpPerfChatID(index int) string {
	return fmt.Sprintf("perf-chat-%03d", index)
}

func cxpPerfInboundID(chat int, turn int) string {
	return fmt.Sprintf("perf-inbound-%03d-%06d", chat, turn)
}

func cxpPerfInboundMessageID(chat int, turn int) string {
	return fmt.Sprintf("perf-message-%03d-%06d", chat, turn)
}
