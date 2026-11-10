package delegation

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWorkerSQLiteArchiveIsAfterAuthoritySwitch(t *testing.T) {
	for _, collision := range []bool{false, true} {
		t.Run(map[bool]string{false: "archive", true: "archive-failure"}[collision], func(t *testing.T) {
			dir := t.TempDir()
			path, legacy := filepath.Join(dir, "state.sqlite"), filepath.Join(dir, "state.json")
			if err := os.WriteFile(legacy, []byte(`{"schema_version":1,"outbox":{"once":{"record_id":"once","status":"unknown","attempts":7}}}`), 0600); err != nil {
				t.Fatal(err)
			}
			if collision {
				if err := os.WriteFile(legacy+".migrated", []byte("preserve previous archive"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			s, err := OpenWorkerDB(path)
			if collision {
				if err == nil || !strings.Contains(err.Error(), "migration committed; archive already exists") {
					t.Fatalf("archive collision=%v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				s.Close()
				if _, err := os.Stat(legacy); !os.IsNotExist(err) {
					t.Fatalf("source was not archived: %v", err)
				}
			}
			// A residual or replaced source cannot become authoritative again,
			// including after the explicit post-commit archive error.
			if err := os.WriteFile(legacy, []byte("broken residual"), 0600); err != nil {
				t.Fatal(err)
			}
			s, err = OpenWorkerDB(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			out, ok, err := s.Outbox(context.Background(), "once")
			if err != nil || !ok || out.Status != OutboxUnknown || out.Attempts != 7 {
				t.Fatalf("committed evidence lost: %+v %v %v", out, ok, err)
			}
		})
	}
}

func TestWorkerSQLiteConcurrentStartAndGenerationRollback(t *testing.T) {
	ctx, now := context.Background(), time.Now().UTC()
	path := filepath.Join(t.TempDir(), "state.sqlite")
	a, err := OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := OpenWorkerDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	original := RemoteThread{ThreadID: "thread", Generation: "new-generation", State: RemoteThreadStateIdle, Summary: "concurrent summary"}
	if err := a.UpsertRemoteThread(ctx, original); err != nil {
		t.Fatal(err)
	}
	request := Record{Kind: RequestKind, RecordID: "request", DelegationID: "task", SourceKey: "source", ThreadPolicy: ThreadPolicyReuse, ThreadGeneration: "stale-generation"}
	thread := original
	thread.State, thread.ActiveDelegationID = RemoteThreadStateActive, request.DelegationID
	if _, _, _, err := a.Start(ctx, request, &thread, now); err == nil {
		t.Fatal("stale generation admitted")
	}
	if records, err := a.Records(ctx, request.DelegationID); err != nil || len(records) != 0 {
		t.Fatalf("rejected start left a request: %v %v", records, err)
	}
	request.ThreadGeneration = original.Generation
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	created := make(chan bool, 2)
	for _, db := range []*WorkerDB{a, b} {
		wg.Add(1)
		go func(db *WorkerDB) {
			defer wg.Done()
			_, _, duplicate, err := db.Start(ctx, request, &thread, now)
			errs <- err
			created <- !duplicate
		}(db)
	}
	wg.Wait()
	close(errs)
	close(created)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for yes := range created {
		if yes {
			n++
		}
	}
	got, ok, err := a.RemoteThread(ctx, "thread")
	if n != 1 || err != nil || !ok || got.ActiveDelegationID != "task" || got.Summary != original.Summary {
		t.Fatalf("created=%d thread=%+v ok=%v err=%v", n, got, ok, err)
	}
	if err := a.ChangeThread(ctx, "thread", func(v *RemoteThread) error {
		v.Generation = ""
		v.Summary = strings.Repeat("界", MaxRemoteThreadSummaryRunes+10)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, _, err = a.RemoteThread(ctx, "thread")
	if err != nil || got.Generation != original.Generation || runeLen(got.Summary) > MaxRemoteThreadSummaryRunes {
		t.Fatalf("point update normalization: %+v %v", got, err)
	}
}
