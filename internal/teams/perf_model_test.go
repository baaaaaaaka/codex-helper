package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		Name:            "idle-chat-hoarder",
		Description:     "hundreds of inactive chats that still need scheduling decisions",
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
		} {
			tc := tc
			t.Run(profile.Name+"/"+tc.name, func(t *testing.T) {
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
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := newCXPPerfStore(b, profile)
				cxpPerfSeedQueuedTurns(b, store, profile)
				cxpPerfMigrateStoreBackend(b, store, backend)
				graph := newCXPPerfGraph(profile)
				bridge := newCXPPerfBridge(store, graph, profile)
				bridge.asyncTurns = true
				b.StartTimer()
				if err := bridge.processQueuedTurns(ctx); err != nil {
					b.Fatalf("daemon queued turn drain process: %v", err)
				}
				if err := cxpPerfDrainAsyncTurns(ctx, bridge); err != nil {
					b.Fatalf("daemon queued turn drain wait: %v", err)
				}
				b.StopTimer()
			}
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

func cxpPerfDrainAsyncTurns(ctx context.Context, bridge *Bridge) error {
	for attempts := 0; attempts < 1000; attempts++ {
		bridge.asyncTurnWG.Wait()
		if err := bridge.processQueuedTurns(ctx); err != nil {
			return err
		}
		state, err := bridge.store.QueuedTurnStateSnapshot(ctx)
		if err != nil {
			return err
		}
		active := false
		for _, turn := range state.Turns {
			if turn.Status == teamstore.TurnStatusQueued || turn.Status == teamstore.TurnStatusRunning {
				active = true
				break
			}
		}
		if !active {
			return nil
		}
	}
	return fmt.Errorf("async Teams turns did not drain")
}

type cxpPerfExecutor struct {
	mode cxpPerfCodexMode
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
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		cxpPerfSeedState(state, profile)
		return nil
	}); err != nil {
		b.Fatalf("seed perf store: %v", err)
	}
	return store
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
