package store

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

const pendingStateIndexSQL = `CREATE INDEX outbox_state_counts ON outbox(state) WHERE state IN ('queued','sending','unknown')`

const pendingCountAggregateSQL = pendingOutboxCountsSQL

func BenchmarkPendingCountAggregate(b *testing.B) {
	s := benchmarkEngineStore(b)
	if err := s.Update(func(tx *Tx) error {
		_, err := tx.exec(`WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<100000) INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT 1,'a-'||x,x,'progress',?,'queued',1 FROM n`, strings.Repeat("x", 1024))
		return err
	}); err != nil {
		b.Fatal(err)
	}
	for _, aggregate := range []bool{false, true} {
		b.Run(fmt.Sprint(aggregate), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if aggregate {
					var q, sending, u int
					if err := s.r.QueryRow(pendingCountAggregateSQL).Scan(&q, &sending, &u); err != nil || q != 100000 {
						b.Fatal(q, err)
					}
				} else {
					if _, err := s.countByState(`SELECT state,COUNT(*) FROM outbox WHERE state IN ('queued','sending','unknown') GROUP BY state`); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkPendingStateIndexWrites(b *testing.B) {
	for _, batch := range []int{1, 100} {
		for _, mode := range []string{"baseline", "state", "cover-pending"} {
			b.Run(fmt.Sprintf("batch-%d/%s", batch, mode), func(b *testing.B) {
				s := benchmarkEngineStore(b)
				restoreOldPendingIndex(b, s)
				if _, err := s.w.Exec(`DROP INDEX IF EXISTS outbox_state_counts`); err != nil {
					b.Fatal(err)
				}
				if mode == "state" {
					if _, err := s.w.Exec(pendingStateIndexSQL); err != nil {
						b.Fatal(err)
					}
				}
				if mode == "cover-pending" {
					if _, err := s.w.Exec(`DROP INDEX outbox_pending`); err != nil {
						b.Fatal(err)
					}
					if _, err := s.w.Exec(`CREATE INDEX outbox_pending ON outbox(conv_id,seq,state) WHERE state IN ('queued','sending','unknown')`); err != nil {
						b.Fatal(err)
					}
				}
				if err := s.Update(func(tx *Tx) error {
					_, err := tx.exec(`WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<1000) INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT 1,'w-'||x,x,'progress',?,'queued',1 FROM n`, strings.Repeat("x", 1024))
					return err
				}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					state := OutSending
					if i%2 == 1 {
						state = OutQueued
					}
					if err := s.Update(func(tx *Tx) error {
						_, err := tx.exec(`UPDATE outbox SET state=? WHERE id<=?`, state, batch)
						return err
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestPendingStateIndexBuildAndSize(t *testing.T) {
	s, _ := openTest(t)
	restoreOldPendingIndex(t, s)
	c := newWork(t, s, "index-cost")
	if _, err := s.w.Exec(`DROP INDEX IF EXISTS outbox_state_counts`); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Tx) error {
		_, err := tx.exec(`WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<100000) INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT ?,'s-'||x,x,'progress',?,'queued',1 FROM n`, c, strings.Repeat("x", 1024))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var before, after, pageSize int64
	if err := s.w.QueryRow(`PRAGMA page_count`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := s.w.Exec(pendingStateIndexSQL); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if err := s.w.QueryRow(`PRAGMA page_count`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if err := s.w.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	t.Logf("100000 rows: index build=%s extra-pages=%d extra-bytes=%d", elapsed, after-before, (after-before)*pageSize)
}

func restoreOldPendingIndex(t testing.TB, s *Store) {
	t.Helper()
	if _, err := s.w.Exec(`DROP INDEX outbox_pending; CREATE INDEX outbox_pending ON outbox(conv_id,seq) WHERE state IN ('queued','sending','unknown')`); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPendingIndexEnqueue(b *testing.B) {
	for _, mode := range []string{"baseline", "cover-pending"} {
		b.Run(mode, func(b *testing.B) {
			s := benchmarkEngineStore(b)
			if mode == "baseline" {
				restoreOldPendingIndex(b, s)
			}
			body := strings.Repeat("x", 1024)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Update(func(tx *Tx) error {
					_, err := tx.Enqueue(1, []OutMsg{{DeliveryKey: fmt.Sprint(i), Kind: OutProgress, Body: body}})
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPendingIndexReplacementBuildAndSize(t *testing.T) {
	s, _ := openTest(t)
	c := newWork(t, s, "replacement-cost")
	restoreOldPendingIndex(t, s)
	if err := s.Update(func(tx *Tx) error {
		_, err := tx.exec(`WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<100000) INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT ?,'r-'||x,x,'progress',?,'queued',1 FROM n`, c, strings.Repeat("x", 1024))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	used := func() int64 {
		var pages, free int64
		mustT(t, s.w.QueryRow(`PRAGMA page_count`).Scan(&pages))
		mustT(t, s.w.QueryRow(`PRAGMA freelist_count`).Scan(&free))
		return pages - free
	}
	before := used()
	start := time.Now()
	_, err := s.w.Exec(`BEGIN IMMEDIATE; DROP INDEX outbox_pending; CREATE INDEX outbox_pending ON outbox(conv_id,seq,state) WHERE state IN ('queued','sending','unknown'); COMMIT`)
	mustT(t, err)
	elapsed := time.Since(start)
	var size int64
	mustT(t, s.w.QueryRow(`PRAGMA page_size`).Scan(&size))
	t.Logf("100000 rows: replacement=%s extra-used-pages=%d extra-used-bytes=%d", elapsed, used()-before, (used()-before)*size)
}
