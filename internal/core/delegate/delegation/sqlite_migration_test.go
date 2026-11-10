package delegation

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func frozenWorkerDB(t *testing.T) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	raw, err := os.ReadFile("testdata/worker-v1.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(raw)); err != nil {
		t.Fatal(err)
	}
	return path, db
}

func TestWorkerSQLiteMigrationPreservesSafetyEvidence(t *testing.T) {
	path, old := frozenWorkerDB(t)
	if _, err := old.Exec(`PRAGMA journal_mode=WAL; INSERT INTO meta VALUES('scalar','kept')`); err != nil {
		t.Fatal(err)
	}
	s, err := OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	records, err := s.Records(ctx, "task-一")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ClaimEpoch != 9007199254740993 || fmt.Sprint(records[0].Path) != "[second first second]" || fmt.Sprint(records[0].Spec.Constraints) != "[二 一 二]" || records[0].Spec.Objective != "目标\nline" || records[0].ThreadUpdate == nil || records[0].ThreadUpdate.Summary != "increment" || len(records[0].Spec.ArtifactRefs) != 1 || records[0].Spec.ArtifactRefs[0].Name != "样例" {
		t.Fatalf("record fields lost: %+v", records)
	}
	fence, ok, err := s.Execution(ctx, "task-一")
	if err != nil || !ok || fence.ClaimEpoch != 9007199254740993 || fence.Status != StateRunning {
		t.Fatalf("fence=%+v ok=%v err=%v", fence, ok, err)
	}
	out, ok, err := s.Outbox(ctx, "request-一")
	if err != nil || !ok || out.Status != OutboxUnknown || out.Attempts != 2 || out.MessageID != "possible-message" {
		t.Fatalf("outbox=%+v ok=%v err=%v", out, ok, err)
	}
	if _, reserved, err := s.ReserveOutbox(ctx, records[0], "chat", time.Now()); err != nil || reserved {
		t.Fatalf("migration reopened POST: reserved=%v err=%v", reserved, err)
	}
	cursor, ok, err := s.InboxCursor(ctx, "chat")
	if err != nil || !ok || cursor.ContinuationPath != "page:next" || cursor.LastHeadMessageID != "head" {
		t.Fatalf("cursor=%+v ok=%v err=%v", cursor, ok, err)
	}
	gate, ok, err := s.InboxBackoff(ctx, "chat")
	if err != nil || !ok || gate.Reason != "429" {
		t.Fatalf("gate=%+v ok=%v err=%v", gate, ok, err)
	}
	for _, table := range delegationLegacyTables {
		var n int
		if err := s.db.QueryRow(`SELECT count(*) FROM pragma_table_info(?) WHERE name='json'`, table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("%s still contains a JSON column", table)
		}
	}
	var version int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("version=%d err=%v", version, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	// Residual legacy inputs cannot become an alternative authority.
	if err := os.WriteFile(strings.TrimSuffix(path, ".sqlite")+".json", []byte("poisoned legacy"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	out, ok, err = s.Outbox(ctx, "request-一")
	if err != nil || !ok || out.Attempts != 2 {
		t.Fatalf("reopen=%+v ok=%v err=%v", out, ok, err)
	}
}

func TestWorkerSQLiteMigrationRollsBack(t *testing.T) {
	for _, damage := range []string{
		`UPDATE outbox SET json='{"record_id":'`,
		`UPDATE inbox_cursors SET json='{"chat_id":"wrong"}'`,
		`UPDATE meta SET value='999' WHERE key='schema_version'`,
		`PRAGMA user_version=999`,
		`CREATE TRIGGER fail_version BEFORE UPDATE ON meta BEGIN SELECT RAISE(ABORT,'migration fault'); END`,
	} {
		t.Run(damage, func(t *testing.T) {
			path, old := frozenWorkerDB(t)
			if _, err := old.Exec(damage); err != nil {
				t.Fatal(err)
			}
			var oldVersion int
			if err := old.QueryRow(`PRAGMA user_version`).Scan(&oldVersion); err != nil {
				t.Fatal(err)
			}
			if s, err := OpenWorkerDB(path); err == nil {
				s.Close()
				t.Fatal("damaged upgrade succeeded")
			}
			var version, legacy, newTables int
			if err := old.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err := old.QueryRow(`SELECT count(*) FROM pragma_table_info('records') WHERE name='json'`).Scan(&legacy); err != nil {
				t.Fatal(err)
			}
			if err := old.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name LIKE 'legacy_%' OR name='record_path'`).Scan(&newTables); err != nil {
				t.Fatal(err)
			}
			if version != oldVersion || legacy != 1 || newTables != 0 {
				t.Fatalf("partial upgrade: version=%d legacy=%d new=%d", version, legacy, newTables)
			}
		})
	}
}

func TestWorkerSQLiteLegacyJSONFailureIsAtomic(t *testing.T) {
	for _, body := range []string{
		`{"schema_version":1,"records":[{"record_id":"one"},`,
		`{"schema_version":999,"records":[]}`,
		`{"records":[{"record_id":"one"},{"record_id":"one"}]}`,
		`{"records":[{"record_id":"one"}]} trailing`,
	} {
		t.Run(body, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.sqlite")
			if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			if s, err := OpenWorkerDB(path); err == nil {
				s.Close()
				t.Fatal("bad legacy input succeeded")
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var tables int
			if err = db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='table'`).Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if tables != 0 {
				t.Fatalf("incomplete import published %d tables", tables)
			}
		})
	}
}

func TestWorkerSQLiteUnchangedReadAndWriteAreQuiet(t *testing.T) {
	path, _ := frozenWorkerDB(t)
	s, err := OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	rows, err := s.Records(ctx, "task-一")
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`PRAGMA wal_checkpoint(PASSIVE)`)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	observer.SetMaxOpenConns(1)
	probe := func() (int64, int64, int64) {
		var changes, version int64
		if err := s.db.QueryRow(`SELECT total_changes()`).Scan(&changes); err != nil {
			t.Fatal(err)
		}
		if err := observer.QueryRow(`PRAGMA data_version`).Scan(&version); err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(path + "-wal")
		if err != nil {
			t.Fatal(err)
		}
		return changes, version, st.Size()
	}
	c0, v0, w0 := probe()
	for range 20 {
		if _, err := s.Append(ctx, rows[0], time.Date(2026, 11, 10, 4, 0, 0, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Outbox(ctx, "request-一"); err != nil {
			t.Fatal(err)
		}
	}
	c1, v1, w1 := probe()
	if c0 != c1 || v0 != v1 || w0 != w1 {
		t.Fatalf("unchanged I/O: changes %d->%d, version %d->%d, WAL %d->%d", c0, c1, v0, v1, w0, w1)
	}
}
