package delegation

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWorkerSQLiteRetentionMatchesExistingPolicy(t *testing.T) {
	now := time.Date(2026, 11, 10, 4, 0, 0, 0, time.UTC)
	fixture := newStore()
	for i := 0; i < 300; i++ {
		at := now.Add(-48 * time.Hour).Add(time.Duration(i) * time.Minute)
		req, err := NewRequestRecord("source", fmt.Sprint(i), "", nil, "machine", TaskSpec{Objective: "old work", Context: []string{strings.Repeat("context", 70)}}, at)
		if err != nil {
			t.Fatal(err)
		}
		claim, err := NewClaimRecord(req.DelegationID, "machine", "worker", 1, at.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		result, err := NewResultRecord(req.DelegationID, claim, StateComplete, strings.Repeat("answer", 1000), 1, at.Add(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		fixture.Records = append(fixture.Records, req, claim, result)
		fixture.UpsertRoute(Route{DelegationID: req.DelegationID, MachineID: "machine"})
		fixture.UpsertOutbox(OutboxRecord{RecordID: result.RecordID, DelegationID: req.DelegationID, Status: OutboxUnknown, Attempts: 1})
		fixture.UpsertExecution(ExecutionFence{DelegationID: req.DelegationID, Status: StateRunning})
	}
	// An old task with a deliberately future expiry remains live; its unknown
	// witness must survive alongside an unrelated orphan unknown witness.
	active, err := NewRequestRecord("source", "active", "", nil, "machine", TaskSpec{Objective: "live"}, now.Add(-72*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	active.ExpiresAt = now.Add(time.Hour).Format(time.RFC3339Nano)
	fixture.Records = append(fixture.Records, active)
	fixture.UpsertOutbox(OutboxRecord{RecordID: "live-witness", DelegationID: active.DelegationID, Status: OutboxUnknown, Attempts: 1})
	fixture.UpsertOutbox(OutboxRecord{RecordID: "orphan-witness", DelegationID: "unknown-orphan", Status: OutboxUnknown, Attempts: 1})
	fixture.UpsertOutbox(OutboxRecord{RecordID: "expired-visible", Status: OutboxVisible, UpdatedAt: now.Add(-48 * time.Hour).Format(time.RFC3339Nano)})
	fixture.UpsertRemoteThread(RemoteThread{ThreadID: "old-idle", State: RemoteThreadStateIdle, LastUsedAt: now.Add(-48 * time.Hour).Format(time.RFC3339Nano)})
	fixture.UpsertRemoteThread(RemoteThread{ThreadID: "old-active", State: RemoteThreadStateActive, LastUsedAt: now.Add(-48 * time.Hour).Format(time.RFC3339Nano)})
	fixture.UpsertInboxCursor(InboxCursor{ChatID: "old", UpdatedAt: now.Add(-48 * time.Hour).Format(time.RFC3339Nano)})
	fixture.UpsertInboxBackoff(InboxBackoff{ChatID: "old", BlockedUntil: now.Add(-48 * time.Hour).Format(time.RFC3339Nano)})
	path := filepath.Join(t.TempDir(), "state.sqlite")
	if _, err := SaveStore(path, fixture); err != nil {
		t.Fatal(err)
	}
	// Test-only copying keeps the historical in-memory implementation as an
	// independent oracle for which evidence is eligible for removal.
	raw, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	var expected Store
	if err = json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	newRecord := NewTombstoneRecord(active.DelegationID, "cancel now", now)
	expected.Records = append(expected.Records, newRecord)
	expected.Prune(now, DefaultStoreRetention)
	db, err := OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Append(context.Background(), newRecord, now); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	actual, err := LoadStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("native retention differs: actual records=%d expected=%d; actual=%+v expected=%+v", len(actual.Records), len(expected.Records), actual, expected)
	}
}

func TestWorkerSQLiteRetentionSeeksPastMissingTimestamps(t *testing.T) {
	s, err := OpenWorkerDB(filepath.Join(t.TempDir(), "worker.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Inspect the actual prepared plan. A SEARCH description alone is not
	// enough: seeking >= '' still visits every empty-time legacy record.
	for name, query := range map[string]struct {
		sql  string
		args []any
	}{
		"records":  {delegationRetentionPageSQL, []any{"2026-11-01T00:00:00.000000000Z", "", ""}},
		"backoffs": {delegationBackoffExpirySQL, []any{"2026-11-01T00:00:00.000000000Z"}},
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := s.db.Query(`EXPLAIN `+query.sql, query.args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			strictSeek := false
			for rows.Next() {
				var address, p1, p2, p3, p5 int
				var op string
				var p4, comment any
				if err = rows.Scan(&address, &op, &p1, &p2, &p3, &p4, &p5, &comment); err != nil {
					t.Fatal(err)
				}
				strictSeek = strictSeek || op == "SeekGT"
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !strictSeek {
				t.Fatal("retention does not seek strictly past the empty timestamp prefix")
			}
		})
	}
}
