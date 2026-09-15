package teams

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBridgePollOnceDispositionsOnlyCorruptDurableSession(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store := newBridgeTestStore(t)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Sessions["s001"] = teamstore.SessionContext{
			ID: "s001", Status: teamstore.SessionStatusActive,
			TeamsChatID: "chat-only-corrupt", UpdatedAt: now,
		}
		state.ChatPolls["control-chat"] = teamstore.ChatPollState{
			ChatID: "control-chat", Seeded: true, PollState: inboundPollStateWarm,
			NextPollAt: now.Add(time.Hour), UpdatedAt: now,
		}
		state.ChatPolls["chat-only-corrupt"] = teamstore.ChatPollState{
			ChatID: "chat-only-corrupt", Seeded: true, PollState: inboundPollStateWarm,
			NextPollAt: now.Add(-time.Minute), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed durable-only-corrupt fixture: %v", err)
	}
	if result, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil || (!result.Migrated && !result.AlreadyDB) {
		t.Fatalf("migrate durable-only-corrupt fixture: result=%#v err=%v", result, err)
	}

	// Corrupt only the canonical session envelope after migration. The scalar
	// chat binding remains present, which is precisely the stale-registry trap:
	// it is enough to locate the row, but not enough to route a Teams message to
	// Codex safely.
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), teamstore.SQLiteFileName))
	if err != nil {
		t.Fatalf("open migrated SQLite fixture: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `UPDATE sessions SET json = ? WHERE id = ?`, []byte(`{"id":`), "s001"); err != nil {
		t.Fatalf("corrupt canonical session row: %v", err)
	}

	var graphRequests atomic.Int32
	graph := &GraphClient{
		auth: &fakeGraphAuth{token: "access"},
		client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			graphRequests.Add(1)
			return nil, errors.New("unexpected Graph request for corrupt durable session")
		})},
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil {
		t.Fatalf("pollOnce with corrupt durable session: %v", err)
	}
	if got := graphRequests.Load(); got != 0 {
		t.Fatalf("Graph requests for corrupt durable session = %d, want 0", got)
	}

	poll, found, err := store.ChatPoll(ctx, "chat-only-corrupt")
	if err != nil || !found {
		t.Fatalf("recovery chat poll found=%v err=%v", found, err)
	}
	if !poll.RecoveryRequired || poll.PollState != inboundPollStateBlocked || poll.BlockedUntil.IsZero() || !poll.NextPollAt.After(now) {
		t.Fatalf("corrupt session recovery disposition = %#v, want durable blocked gate", poll)
	}
	if poll.RecoverySourceHash == "" || poll.RecoveryReason == "" {
		t.Fatalf("corrupt session recovery evidence missing: %#v", poll)
	}

	// The durable gate must also suppress a second immediate probe. This checks
	// that the fix prevents both unsafe registry fallback and a local busy loop.
	if err := bridge.pollOnce(ctx, ownerPollMessageTop); err != nil {
		t.Fatalf("second gated pollOnce: %v", err)
	}
	if got := graphRequests.Load(); got != 0 {
		t.Fatalf("Graph requests after durable recovery gate = %d, want 0", got)
	}
}
