package delegation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/testperf"
)

func TestWorkerSQLiteLargeMigrationProbe(t *testing.T) {
	if os.Getenv("CXP_SQLITE_PERF_PROBE") != "1" {
		t.Skip("nightly synthetic performance probe")
	}
	n, err := strconv.Atoi(os.Getenv("CXP_SQLITE_PERF_ROWS"))
	if err != nil || n < 1 {
		t.Fatal("positive CXP_SQLITE_PERF_ROWS required")
	}
	path := os.Getenv("CXP_SQLITE_PERF_INPUT")
	if path == "" {
		path, old := frozenWorkerDB(t)
		tx, err := old.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		stmt, err := tx.Prepare(`INSERT INTO records(id,json) VALUES(?,?)`)
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
		out, err := tx.Prepare(`INSERT INTO outbox(id,json) VALUES(?,?)`)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("history-%d", i)
			raw := fmt.Sprintf(`{"kind":"cxp.delegation.request.v1","record_id":%q,"delegation_id":%q,"spec":{"objective":%q,"constraints":["second","first","second"]}}`, id, id, strings.Repeat("x", 1024))
			if _, err = stmt.Exec(id, raw); err != nil {
				t.Fatal(err)
			}
			if _, err = out.Exec(id, fmt.Sprintf(`{"record_id":%q,"status":"unknown","attempts":1}`, id)); err != nil {
				t.Fatal(err)
			}
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = old.Close(); err != nil {
			t.Fatal(err)
		}
		testperf.RunFresh(t, path)
		return
	}
	testperf.Measure(t, filepath.Dir(path), "delegation-migration", n, func() {
		s, err := OpenWorkerDB(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		var records, outbox, children int
		if err = s.db.QueryRow(`SELECT count(*) FROM records`).Scan(&records); err != nil {
			t.Fatal(err)
		}
		if err = s.db.QueryRow(`SELECT count(*) FROM outbox WHERE status='unknown'`).Scan(&outbox); err != nil {
			t.Fatal(err)
		}
		if err = s.db.QueryRow(`SELECT count(*) FROM record_constraints WHERE record_id LIKE 'history-%'`).Scan(&children); err != nil {
			t.Fatal(err)
		}
		if records != n+1 || outbox != n+1 || children != 3*n {
			t.Fatal(records, outbox, children, n)
		}
	})
	s, err := OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, now := context.Background(), time.Now().UTC()
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// Complete storage path: requester deduplication, non-idempotent POST
	// reservation, claim, execution fence, result and visible POST evidence.
	// Network transport and actual worker execution are covered by DELEG-RT.
	testperf.Measure(t, filepath.Dir(path), "delegation-chain-100", n, func() {
		for i := 0; i < 100; i++ {
			req, err := NewRequestRecord("perf-source", fmt.Sprint(i), "", nil, "machine", TaskSpec{Objective: "measured request", Constraints: []string{"second", "first", "second"}}, now)
			check(err)
			req.RemoteThreadID = fmt.Sprintf("perf-thread-%d", i)
			thread := workerIdleRemoteThread(req, now)
			_, _, duplicate, err := s.Start(ctx, req, &thread, now)
			check(err)
			if duplicate {
				t.Fatal("unexpected duplicate request")
			}
			check(s.UpsertRoute(ctx, Route{DelegationID: req.DelegationID, MachineID: "machine"}))
			post := func(record Record) {
				_, won, err := s.ReserveOutbox(ctx, record, "inbox", now)
				check(err)
				if !won {
					t.Fatal("POST reservation lost")
				}
				check(s.UpsertOutbox(ctx, record, OutboxVisible, "inbox", "ref", record.RecordID, "", now, DefaultStoreRetention))
			}
			post(req)
			claim, err := NewClaimRecord(req.DelegationID, "machine", "worker", 1, now.Add(time.Second))
			check(err)
			_, err = s.Append(ctx, claim, now)
			check(err)
			started, err := s.TryStart(ctx, req, claim, now, WorkerStorePruneLimits{})
			check(err)
			if !started {
				t.Fatal("execution did not start")
			}
			result, err := NewResultRecord(req.DelegationID, claim, StateComplete, "complete result", 1, now.Add(2*time.Second))
			check(err)
			records, err := s.Append(ctx, result, now)
			check(err)
			post(result)
			if len(records) != 3 || Reduce(records, now.Add(3*time.Second)).Status != StateComplete {
				t.Fatal("incomplete delegated result", len(records))
			}
		}
	})
}
