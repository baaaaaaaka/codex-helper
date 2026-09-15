package teams

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// A syntactically corrupt SQLite ChatPoll row must be repaired locally before
// the bridge can issue a head request.  This protects an established chat from
// silently baselining its next visible Teams message.
func TestBridgeSQLiteSyntaxCorruptPollRepairsWithoutGraphHead(t *testing.T) {
	ctx := context.Background()
	chatID := "chat-syntax-fence"
	store := newBridgeTestStore(t)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ChatPolls[chatID] = teamstore.ChatPollState{
			ChatID:     chatID,
			Seeded:     true,
			PollState:  inboundPollStateWarm,
			NextPollAt: time.Now().UTC().Add(-time.Minute),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed poll: %v", err)
	}
	migration, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("migrate poll: %v", err)
	}
	sqlitePath := migration.Path
	if sqlitePath == "" {
		sqlitePath = filepath.Join(filepath.Dir(store.Path()), teamstore.SQLiteFileName)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close before corruption: %v", err)
	}
	db, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("open SQLite for corruption: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, []byte(`{"chat_id":"chat-syntax-fence"`), chatID); err != nil {
		_ = db.Close()
		t.Fatalf("corrupt ChatPoll JSON: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close corrupted SQLite: %v", err)
	}
	reopened, err := teamstore.Open(store.Path())
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	poll, found, err := reopened.ChatPoll(ctx, chatID)
	if err != nil || !found {
		t.Fatalf("read corrupt poll: found=%v err=%v poll=%#v", found, err, poll)
	}
	if !teamstore.ChatPollHasOpaqueRecoveryEvidence(poll) {
		t.Fatalf("corrupt poll was not fenced as opaque: %#v", poll)
	}

	bridge := newBridgeTestBridge(nil, reopened, &recordingExecutor{})
	handled, err := bridge.pollChatWithRoleStateOptions(ctx, chatID, 20, inboundPollRoleWork, false, poll, true, pollChatWithRoleOptions{}, nil)
	if err != nil {
		t.Fatalf("repair corrupt poll: %v", err)
	}
	if handled {
		t.Fatal("syntax-only poll repair reported a handled Graph page")
	}
	repaired, found, err := reopened.ChatPoll(ctx, chatID)
	if err != nil || !found {
		t.Fatalf("read repaired poll: found=%v err=%v poll=%#v", found, err, repaired)
	}
	if teamstore.ChatPollHasOpaqueRecoveryEvidence(repaired) || repaired.Gap == nil {
		t.Fatalf("syntax-corrupt poll was not converted to a fenced gap: %#v", repaired)
	}
}
