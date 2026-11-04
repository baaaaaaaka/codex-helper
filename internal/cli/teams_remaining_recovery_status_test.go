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
