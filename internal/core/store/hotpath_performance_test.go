package store

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkHotSenderQueue(b *testing.B) {
	for _, n := range []int{1, 256, 10000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s := benchmarkEngineStore(b)
			if e := s.Update(func(tx *Tx) error {
				_, e := tx.exec(`WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<?) INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT 1,'q-'||x,x,'progress','payload','queued',1 FROM n`, n)
				return e
			}); e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if e := s.Read(func(tx *Tx) error {
					rows, e := tx.NextSendable(tx.Now())
					if e == nil && len(rows) != 1 {
						b.Fatal(len(rows))
					}
					return e
				}); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}
func TestHotQueryPlans(t *testing.T) {
	s, _ := openTest(t)
	c := newWork(t, s, "audit")
	if e := s.Update(func(tx *Tx) error {
		_, e := tx.Enqueue(c, []OutMsg{out("p", OutProgress)})
		return e
	}); e != nil {
		t.Fatal(e)
	}
	for _, q := range []string{`SELECT DISTINCT conv_id FROM outbox WHERE state IN ('queued','sending')`, `SELECT state,COUNT(*) FROM outbox WHERE state IN ('queued','sending','unknown') GROUP BY state`} {
		rows, e := s.r.Query("EXPLAIN QUERY PLAN " + q)
		if e != nil {
			t.Fatal(e)
		}
		for rows.Next() {
			var a, b, c int
			var detail string
			if e := rows.Scan(&a, &b, &c, &detail); e != nil {
				t.Fatal(e)
			}
			t.Log(q, detail)
		}
		rows.Close()
	}
}

func BenchmarkHotOutboxCounts(b *testing.B) {
	for _, n := range []int{256, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s := benchmarkEngineStore(b)
			restoreOldPendingIndex(b, s)
			if e := s.Update(func(tx *Tx) error {
				_, e := tx.exec(`WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<?) INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT 1,'c-'||x,x,'progress',?,'queued',1 FROM n`, n, strings.Repeat("x", 1024))
				return e
			}); e != nil {
				b.Fatal(e)
			}
			for _, mode := range []string{"baseline", "state", "cover-pending", "cover-pending-aggregate"} {
				name := mode
				if mode == "state" {
					name = "candidate-covering-index"
					if _, e := s.w.Exec(`CREATE INDEX audit_state ON outbox(state) WHERE state IN ('queued','sending','unknown')`); e != nil {
						b.Fatal(e)
					}
				}
				if mode == "cover-pending" {
					if _, err := s.w.Exec(`DROP INDEX audit_state`); err != nil {
						b.Fatal(err)
					}
					if _, err := s.w.Exec(`DROP INDEX outbox_pending`); err != nil {
						b.Fatal(err)
					}
					if _, err := s.w.Exec(`CREATE INDEX outbox_pending ON outbox(conv_id,seq,state) WHERE state IN ('queued','sending','unknown')`); err != nil {
						b.Fatal(err)
					}
				}
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						var counts map[string]int
						var e error
						if mode == "cover-pending-aggregate" {
							counts, e = s.OutboxCounts()
						} else {
							counts, e = s.countByState(`SELECT state,COUNT(*) FROM outbox WHERE state IN ('queued','sending','unknown') GROUP BY state`)
						}
						if e != nil || counts[OutQueued] != n {
							b.Fatal(counts, e)
						}
					}
				})
			}
		})
	}
}
func TestHotNoopUpdatePublishesChange(t *testing.T) {
	s, _ := openTest(t)
	for {
		select {
		case <-s.Changes():
			continue
		default:
			goto drained
		}
	}
drained:
	if e := s.Update(func(*Tx) error { return nil }); e != nil {
		t.Fatal(e)
	}
	select {
	case <-s.Changes():
		t.Log("no-op write transaction emits a publisher change signal")
	default:
		t.Fatal("missing expected signal")
	}
}
