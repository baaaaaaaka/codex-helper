package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

const hotPollAdmissionBenchmarkChats = 2100

// BenchmarkSQLiteHotPollAdmissionScalarVsLegacy compares the two admission
// lanes on the same durable shape: 2,100 active chat/session pairs with due
// operational frontiers and a 64-row quantum. The legacy sub-benchmark
// deliberately revokes the row-local trust bits after migration so it
// exercises the JSON compatibility oracle rather than accidentally measuring
// the scalar wrapper's fast path.
//
// This is diagnostic rather than a machine-specific pass/fail threshold. It
// is kept separate from normal tests because creating the realistic fixture is
// intentionally more expensive than a unit test.
func BenchmarkSQLiteHotPollAdmissionScalarVsLegacy(b *testing.B) {
	for _, tc := range []struct {
		name       string
		untrusted  bool
		readyQuery bool
	}{
		{name: "work-candidates/scalar", readyQuery: false},
		{name: "work-candidates/legacy-json", untrusted: true, readyQuery: false},
		{name: "ready-chat-ids/scalar", readyQuery: true},
		{name: "ready-chat-ids/legacy-json", untrusted: true, readyQuery: true},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			store, now := newHotPollAdmissionBenchmarkStore(b, tc.untrusted)
			if !tc.untrusted {
				// MigrateLargeStateToSQLite deliberately advances large-store
				// projection repair in bounded pages. Complete that setup before
				// timing the trusted lane; otherwise a benchmark named "scalar"
				// can accidentally measure the compatibility fallback while the
				// version marker is still incomplete.
				completeHotPollBenchmarkProjection(b, store)
			}
			ctx := context.Background()
			idleBefore := now.Add(-48 * time.Hour)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := store.withStateLock(ctx, func() error {
					pointer, ok, err := store.currentSQLitePointerUnlocked()
					if err != nil {
						return err
					}
					if !ok {
						return fmt.Errorf("sqlite pointer missing")
					}
					db, err := store.sqliteDBUnlocked(pointer)
					if err != nil {
						return err
					}
					if tc.readyQuery {
						var ids []string
						if tc.untrusted {
							ids, err = loadSQLiteHotPollReadyChatIDsLegacy(ctx, db, "", now, sqliteHotPollReadyLimit)
						} else {
							ids, err = loadSQLiteHotPollReadyChatIDsTrusted(ctx, db, "", now, sqliteHotPollReadyLimit)
						}
						if err == nil && len(ids) != sqliteHotPollReadyLimit {
							return fmt.Errorf("ready IDs=%d, want %d", len(ids), sqliteHotPollReadyLimit)
						}
						return err
					}
					var candidates []SessionContext
					if tc.untrusted {
						candidates, err = loadSQLiteHotPollWorkCandidatesLegacy(ctx, db, "", idleBefore, now, sqliteHotPollReadyLimit)
					} else {
						candidates, err = loadSQLiteHotPollWorkCandidatesTrusted(ctx, db, "", idleBefore, now, sqliteHotPollReadyLimit)
					}
					if err == nil && len(candidates) != sqliteHotPollReadyLimit {
						return fmt.Errorf("work candidates=%d, want %d", len(candidates), sqliteHotPollReadyLimit)
					}
					return err
				})
				if err != nil {
					if tc.untrusted && errors.Is(err, errSQLiteHotPollAdmissionIndeterminate) {
						// The compatibility oracle is deliberately fail-closed at
						// the production two-second budget. A large legacy fixture
						// may therefore be indeterminate rather than benchmarkable;
						// do not turn that expected safety decision into a CI red
						// build or weaken the runtime budget just for measurement.
						b.Skipf("compatibility JSON lane is indeterminate at the production budget: %v", err)
					}
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSQLiteHotPollCombinedAdmissionScalarVsLegacy measures the public
// combined admission APIs used by the listener. It includes the metadata
// probe, bounded schedule state, active-turn hints, and the one state-lock
// boundary; it is therefore a better local bound than calling an individual
// SQL loader directly. It still does not model Graph RTT, handler work, or
// durable completion throughput.
func BenchmarkSQLiteHotPollCombinedAdmissionScalarVsLegacy(b *testing.B) {
	for _, tc := range []struct {
		name      string
		optimized bool
		untrusted bool
	}{
		{name: "optimized-scalar", optimized: true},
		{name: "compatibility-json", untrusted: true},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			store, now := newHotPollAdmissionBenchmarkStore(b, tc.untrusted)
			completeHotPollBenchmarkProjection(b, store)
			ctx := context.Background()
			idleBefore := now.Add(-48 * time.Hour)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var candidates []SessionContext
				var handled bool
				var err error
				if tc.optimized {
					_, candidates, handled, err = store.HotPollScheduleAndWorkCandidatesOptimizedExcludingIdleAt(ctx, "", idleBefore, now)
				} else {
					_, candidates, handled, err = store.HotPollScheduleAndWorkCandidatesExcludingIdleAt(ctx, "", idleBefore, now)
				}
				if err != nil {
					if !tc.optimized && errors.Is(err, errSQLiteHotPollAdmissionIndeterminate) {
						// This is an intentional production safety result: the
						// compatibility oracle refused to spend more than its bounded
						// 2-second budget on this 2,100-row fixture. Do not turn that
						// refusal into a benchmark failure or weaken the runtime budget.
						b.Skipf("compatibility JSON lane is indeterminate at the production budget: %v", err)
					}
					b.Fatal(err)
				}
				if !handled || len(candidates) != sqliteHotPollReadyLimit {
					b.Fatalf("handled=%v candidates=%d, want %d", handled, len(candidates), sqliteHotPollReadyLimit)
				}
			}
		})
	}
}

func completeHotPollBenchmarkProjection(b *testing.B, store *Store) {
	b.Helper()
	// Each ensureSQLiteSchema invocation advances each keyset backfill by one
	// page. The fixture has 2,100 rows, so a few extra passes put the public
	// optimized API on the same completed-projection contract as a long-lived
	// store. This is setup, never part of the timed benchmark window.
	for i := 0; i < 16; i++ {
		if err := store.withStateLock(context.Background(), func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("sqlite pointer missing while completing benchmark projection")
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			return ensureSQLiteSchema(db)
		}); err != nil {
			b.Fatalf("complete benchmark projection pass %d: %v", i, err)
		}
	}
}

func newHotPollAdmissionBenchmarkStore(b *testing.B, untrusted bool) (*Store, time.Time) {
	b.Helper()
	store, err := Open(filepath.Join(b.TempDir(), "state.json"))
	if err != nil {
		b.Fatalf("open hot-poll benchmark store: %v", err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close hot-poll benchmark store: %v", err)
		}
	})
	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	if err := store.Update(context.Background(), func(state *State) error {
		for i := 0; i < hotPollAdmissionBenchmarkChats; i++ {
			sessionID := fmt.Sprintf("benchmark-session-%04d", i)
			chatID := fmt.Sprintf("benchmark-chat-%04d", i)
			state.Sessions[sessionID] = SessionContext{
				ID: sessionID, Status: SessionStatusActive, TeamsChatID: chatID,
				CreatedAt: now, UpdatedAt: now,
			}
			state.ChatPolls[chatID] = ChatPollState{
				ChatID: chatID, Seeded: true, PollState: chatPollStateWarm,
				NextPollAt: now.Add(-time.Minute), LastActivityAt: now,
				ContinuationPath:     "/chats/" + chatID + "/messages?$skiptoken=benchmark",
				LastSuccessfulPollAt: now.Add(-time.Minute), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("seed hot-poll benchmark store: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		b.Fatalf("migrate hot-poll benchmark store: %v", err)
	}
	if untrusted {
		if err := store.withStateLock(context.Background(), func() error {
			pointer, ok, err := store.currentSQLitePointerUnlocked()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("sqlite pointer missing after migration")
			}
			db, err := store.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			if _, err := db.ExecContext(context.Background(), `UPDATE sessions SET projection_trusted = 0`); err != nil {
				return err
			}
			_, err = db.ExecContext(context.Background(), `UPDATE chat_polls SET projection_trusted = 0`)
			return err
		}); err != nil {
			b.Fatalf("revoke hot-poll benchmark trust: %v", err)
		}
	}
	return store, now
}
