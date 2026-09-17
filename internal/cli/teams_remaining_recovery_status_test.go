package cli

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	_ "modernc.org/sqlite"
)

func TestTeamsStatusReportsUnresolvedExecutionAndAmbiguousOutboxWithoutBodies(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	now := time.Now().UTC()
	store, err := openTeamsStore()
	if err != nil {
		t.Fatalf("open Teams store: %v", err)
	}
	if err := store.Update(context.Background(), func(state *teamsstore.State) error {
		state.Scope = teamsstore.ScopeIdentity{ID: "scope-recovery-status", Profile: "default"}
		state.Sessions["session-recovery-status"] = teamsstore.SessionContext{
			ID:          "session-recovery-status",
			Status:      teamsstore.SessionStatusActive,
			TeamsChatID: "chat-recovery-status",
		}
		state.Turns["turn-recovery-status"] = teamsstore.Turn{
			ID:             "turn-recovery-status",
			SessionID:      "session-recovery-status",
			Status:         teamsstore.TurnStatusRunning,
			RecoveryReason: "owner proof missing after restart",
			UpdatedAt:      now,
		}
		state.ImportCheckpoints["transcript:session-recovery-status"] = teamsstore.ImportCheckpoint{
			ID:        "transcript:session-recovery-status",
			SessionID: "session-recovery-status",
			UnresolvedExecution: &teamsstore.ExecutionAnchor{
				SessionID:   "session-recovery-status",
				OuterTurnID: "turn-recovery-status",
				Reason:      "owner proof missing after restart",
				Generation:  7,
				State:       "unresolved",
			},
		}
		state.ImportCheckpoints["transcript:resolved-status"] = teamsstore.ImportCheckpoint{
			ID:        "transcript:resolved-status",
			SessionID: "session-recovery-status",
			UnresolvedExecution: &teamsstore.ExecutionAnchor{
				State: "resolved",
			},
		}
		state.OutboxMessages["outbox:ambiguous-status"] = teamsstore.OutboxMessage{
			ID:            "outbox:ambiguous-status",
			SessionID:     "session-recovery-status",
			TurnID:        "turn-recovery-status",
			TeamsChatID:   "chat-recovery-status",
			Kind:          "helper",
			Body:          "this body must not be printed by status",
			Status:        teamsstore.OutboxStatusSending,
			LastSendError: "ambiguous Graph send; previous owner stopped before durable Graph identity",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed recovery status state: %v", err)
	}

	out := executeRootForTeamsTest(t, "teams", "status")
	for _, want := range []string{
		"Unresolved execution fences: 1 (automatic retry disabled)",
		"checkpoint=transcript:session-recovery-status",
		"session=session-recovery-status",
		"turn=turn-recovery-status",
		"turn_status=running",
		"chat=chat-recovery-status",
		"generation=7",
		`reason="owner proof missing after restart"`,
		"Ambiguous outbox: 1 rows across 1 chat(s) (exact Graph evidence required; no automatic replay)",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("Teams status output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "this body must not be printed") {
		t.Fatalf("Teams status exposed an outbox body:\n%s", out)
	}
}

func TestTeamsRecoverPreparesUnreadySQLiteBeforeOwnerRead(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	store := seedRecoverableTeamsState(t)
	ctx := context.Background()
	old := time.Now().UTC().Add(-time.Hour)
	owner, err := teamsstore.CurrentOwner("v-test", "s1", "turn:manual", old)
	if err != nil {
		t.Fatalf("create stale owner: %v", err)
	}
	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, old); err != nil {
		t.Fatalf("seed stale owner: %v", err)
	}
	storePath := store.Path()
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate recovery fixture to SQLite: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close migrated recovery fixture: %v", err)
	}

	databasePath := filepath.Join(filepath.Dir(storePath), teamsstore.SQLiteFileName)
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open recovery SQLite fixture: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM state_meta WHERE key = 'sqlite_schema_preparation_version'`); err != nil {
		_ = db.Close()
		t.Fatalf("make recovery SQLite fixture unprepared: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close unprepared recovery SQLite fixture: %v", err)
	}

	out := executeRootForTeamsTest(t, "teams", "recover", "--force")
	if !strings.Contains(out, "Recovered interrupted turns: 1") {
		t.Fatalf("recover did not prepare unready SQLite before owner read:\n%s", out)
	}
}
