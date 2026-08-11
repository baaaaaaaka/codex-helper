package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// BenchmarkLinkedTranscriptProvenanceMatrix keeps the security-critical file
// checks measurable at the session counts that caused the original idle
// regression. JSON and SQLite are both exercised because the watcher obtains
// its durable HistoryWatch projection from the selected backend before doing
// the bounded source check.
func BenchmarkLinkedTranscriptProvenanceMatrix(b *testing.B) {
	for _, backend := range []string{"json", "sqlite"} {
		for _, sessionCount := range []int{1, 40, 240} {
			for _, scenario := range []string{"idle", "single-growth", "same-size-rewrite", "atomic-replacement"} {
				backend, sessionCount, scenario := backend, sessionCount, scenario
				b.Run(fmt.Sprintf("%s/%d/%s", backend, sessionCount, scenario), func(b *testing.B) {
					store := newCXPPerfStore(b, cxpPerfProfile{
						Name:         fmt.Sprintf("provenance-matrix-%s-%d", backend, sessionCount),
						WorkChats:    sessionCount,
						TurnsPerChat: 1,
						MessageBytes: 64,
						HistoryFiles: sessionCount,
						HistoryLines: 1,
					})
					if backend == "sqlite" {
						cxpPerfMigrateStoreToSQLite(b, store)
					}
					root := b.TempDir()
					const initial = `{"type":"session_meta","payload":{"id":"matrix-thread"}}
`
					paths := make([]string, sessionCount)
					checkpoints := make([]teamstore.ImportCheckpoint, sessionCount)
					if err := store.Update(context.Background(), func(state *teamstore.State) error {
						for i := 0; i < sessionCount; i++ {
							id := fmt.Sprintf("matrix-session-%03d", i)
							path := filepath.Join(root, id+".jsonl")
							if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
								return err
							}
							info, err := os.Stat(path)
							if err != nil {
								return err
							}
							offset := int64(len(initial))
							paths[i] = path
							checkpoints[i] = teamstore.ImportCheckpoint{ID: "transcript:" + id, SessionID: id, SourcePath: path, SourceFingerprint: transcriptCheckpointSourceFingerprint(path, offset), LastOffset: offset, LastOffsetKnown: true, SourceSize: info.Size(), SourceModTime: info.ModTime()}
							state.HistoryWatch[historyWatchCheckpointID(path)] = teamstore.HistoryWatchCheckpoint{ID: historyWatchCheckpointID(path), Path: path, Size: info.Size(), ModTime: info.ModTime(), SourceFingerprint: transcriptCheckpointSourceFingerprint(path, offset), Offset: offset, SessionID: id, ThreadID: "matrix-thread"}
						}
						return nil
					}); err != nil {
						b.Fatalf("seed matrix: %v", err)
					}
					if backend == "sqlite" {
						if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
							b.Fatalf("MigrateLargeStateToSQLite: %v", err)
						}
					}
					bridge := &Bridge{}
					ctx := context.Background()
					// Mutate the source once before timing. The old benchmark rewrote
					// the file inside every iteration and kept a stale checkpoint, so
					// "single-growth" measured repeated setup/rescan and disk writes
					// rather than an incremental poll. The timed loop now measures only
					// the watcher/provenance operation for a prepared source state.
					b.StopTimer()
					switch scenario {
					case "single-growth":
						if err := os.WriteFile(paths[0], []byte(initial+`{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"growth"}}
`), 0o600); err != nil {
							b.Fatalf("prepare growth source: %v", err)
						}
					case "same-size-rewrite":
						info, err := os.Stat(paths[0])
						if err != nil {
							b.Fatalf("stat rewrite file: %v", err)
						}
						rewritten := strings.Replace(initial, "matrix-thread", "matrix-threaD", 1)
						if len(rewritten) != len(initial) {
							b.Fatalf("same-size rewrite changed length: %d != %d", len(rewritten), len(initial))
						}
						if err := os.WriteFile(paths[0], []byte(rewritten), 0o600); err != nil {
							b.Fatalf("prepare rewrite file: %v", err)
						}
						if err := os.Chtimes(paths[0], info.ModTime(), info.ModTime()); err != nil {
							b.Fatalf("restore rewrite mtime: %v", err)
						}
					case "atomic-replacement":
						// Keep the old inode alive while installing the replacement.
						// Otherwise ext4 may immediately reuse it after the temporary
						// file is removed, making a repeated benchmark iteration look
						// like the original source even though the path was replaced.
						old := paths[0] + ".old-inode"
						if err := os.Rename(paths[0], old); err != nil {
							b.Fatalf("retain original replacement inode: %v", err)
						}
						tmp := paths[0] + ".replacement"
						if err := os.WriteFile(tmp, []byte(initial), 0o600); err != nil {
							b.Fatalf("prepare replacement: %v", err)
						}
						if err := os.Rename(tmp, paths[0]); err != nil {
							b.Fatalf("replace source: %v", err)
						}
					}
					b.StartTimer()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := store.HistoryWatchState(ctx); err != nil {
							b.Fatalf("HistoryWatchState: %v", err)
						}
						switch scenario {
						case "idle":
							for j := range paths {
								if !linkedCheckpointIdleNoGrowth(paths[j], checkpoints[j]) {
									b.Fatalf("idle checkpoint %d unexpectedly unsafe", j)
								}
							}
						case "single-growth":
							if _, err := bridge.readLinkedTranscriptDelta(paths[0], checkpoints[0], "matrix-thread", "matrix-thread"); err != nil {
								b.Fatalf("growth scan: %v", err)
							}
						case "same-size-rewrite":
							if linkedCheckpointFileUnchanged(paths[0], checkpoints[0]) {
								b.Fatalf("same-size rewrite passed strict provenance check")
							}
						case "atomic-replacement":
							if linkedCheckpointFileUnchanged(paths[0], checkpoints[0]) {
								b.Fatalf("atomic replacement passed strict identity check")
							}
						}
					}
				})
			}
		}
	}
}

// BenchmarkHistoryWatchUnchangedPartialTailCheckpointPersistence measures the
// storage cost of the checkpoint write made while a history file still has an
// incomplete trailing JSON record. The scanner deliberately keeps the logical
// cursor at the previous trusted position in that case, while the store must
// recognize the repeated checkpoint as a no-op. The write/read metrics make
// the no-op contract visible for both JSON and SQLite backends.
func BenchmarkHistoryWatchUnchangedPartialTailCheckpointPersistence(b *testing.B) {
	for _, backend := range []string{"json", "sqlite"} {
		backend := backend
		b.Run(backend, func(b *testing.B) {
			profile := cxpPerfProfile{
				Name:            "history-watch-partial-tail",
				WorkChats:       20,
				TurnsPerChat:    4,
				MessageBytes:    256,
				OutboxPerChat:   2,
				MessagesPerPoll: 0,
			}
			store := newCXPPerfStore(b, profile)
			if backend == "sqlite" {
				cxpPerfMigrateStoreToSQLite(b, store)
			}
			checkpointID := "history-watch:unchanged-partial-tail"
			checkpoint := teamstore.HistoryWatchCheckpoint{
				ID:        checkpointID,
				Path:      filepath.Join(b.TempDir(), "rollout-partial.jsonl"),
				Size:      4096,
				ModTime:   time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC),
				Offset:    4096,
				Line:      32,
				SessionID: "partial-tail-session",
				ThreadID:  "partial-tail-thread",
			}
			ctx := context.Background()
			if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
				history[checkpointID] = checkpoint
				return nil
			}); err != nil {
				b.Fatalf("seed history-watch checkpoint: %v", err)
			}

			// One warm-up write establishes the same state that the first partial
			// poll would have observed.  The timed loop represents subsequent
			// polls where the source size/modtime and logical cursor are unchanged.
			if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
				history[checkpointID] = checkpoint
				return nil
			}); err != nil {
				b.Fatalf("warm history-watch checkpoint: %v", err)
			}
			b.ReportAllocs()
			b.StopTimer()
			before, beforeOK := cxpPerfReadProcSelfIO()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
					history[checkpointID] = checkpoint
					return nil
				}); err != nil {
					b.Fatalf("persist unchanged partial-tail checkpoint: %v", err)
				}
			}
			b.StopTimer()
			if beforeOK {
				cxpPerfReportProcIODelta(b, before, beforeOK, b.N)
			}
		})
	}
}

// BenchmarkHistoryWatchUnchangedPartialTailPoll exercises the watcher entry
// point, not just Store.UpdateHistoryWatch.  The file is left at the same
// incomplete JSON tail for every iteration. The rollback checkpoint retains
// the trusted logical cursor, while UpdateHistoryWatch must avoid rewriting
// durable state after the first observation of the same source boundary.
func BenchmarkHistoryWatchUnchangedPartialTailPoll(b *testing.B) {
	for _, backend := range []string{"json", "sqlite"} {
		backend := backend
		b.Run(backend, func(b *testing.B) {
			profile := cxpPerfProfile{Name: "history-watch-partial-poll", WorkChats: 4, TurnsPerChat: 2, MessageBytes: 128, OutboxPerChat: 1}
			store := newCXPPerfStore(b, profile)
			if backend == "sqlite" {
				cxpPerfMigrateStoreToSQLite(b, store)
			}
			root := b.TempDir()
			path := filepath.Join(root, "sessions", "2026", "08", "09", "rollout-partial-poll.jsonl")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				b.Fatalf("mkdir history directory: %v", err)
			}
			complete := []byte(`{"type":"session_meta","payload":{"id":"partial-poll-thread"}}` + "\n" +
				`{"thread_id":"partial-poll-thread","turn_id":"turn-old","id":"old-final","role":"assistant","text":"old"}` + "\n")
			if err := os.WriteFile(path, complete, 0o600); err != nil {
				b.Fatalf("write complete history: %v", err)
			}
			info, err := os.Stat(path)
			if err != nil {
				b.Fatalf("stat complete history: %v", err)
			}
			partial := append(append([]byte(nil), complete...), []byte(`{"id":"partial","role":"assistant","text":"unfinished`)...)
			if err := os.WriteFile(path, partial, 0o600); err != nil {
				b.Fatalf("write incomplete history: %v", err)
			}
			checkpointID := historyWatchCheckpointID(path)
			checkpoint := teamstore.HistoryWatchCheckpoint{
				ID: checkpointID, Path: path, Size: info.Size(), ModTime: info.ModTime(), Offset: info.Size(), Line: 2,
				SessionID: "partial-poll-thread", ThreadID: "partial-poll-thread",
			}
			ctx := context.Background()
			if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
				history[checkpointID] = checkpoint
				*ready = time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
				return nil
			}); err != nil {
				b.Fatalf("seed partial-poll checkpoint: %v", err)
			}
			bridge := &Bridge{store: store, scope: teamstore.ScopeIdentity{CodexHome: root}}
			if err := bridge.syncCodexHistoryFinals(ctx, time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC), false); err != nil {
				b.Fatalf("warm partial-poll sync: %v", err)
			}
			b.ReportAllocs()
			b.StopTimer()
			before, beforeOK := cxpPerfReadProcSelfIO()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				if err := bridge.syncCodexHistoryFinals(ctx, time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC).Add(time.Duration(i+2)*10*time.Second), false); err != nil {
					b.Fatalf("unchanged partial-poll sync: %v", err)
				}
			}
			b.StopTimer()
			if beforeOK {
				cxpPerfReportProcIODelta(b, before, beforeOK, b.N)
			}
		})
	}
}
