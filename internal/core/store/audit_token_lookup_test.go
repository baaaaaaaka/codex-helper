package store

import (
	"fmt"
	"strings"
	"testing"
)

// Audit probe: retained sent history must be distinguished from active work.
// This records current query plans; it does not change the storage contract.
func TestAuditTokenLookupPlan(t *testing.T) {
	s, _ := openTest(t)
	_ = newWork(t, s, "audit")
	rows, err := s.r.Query(`EXPLAIN QUERY PLAN SELECT ` + outboxCols + ` FROM outbox WHERE conv_id=1 AND attempt_token='missing' ORDER BY seq LIMIT 1`)
	mustT(t, err)
	defer rows.Close()
	for rows.Next() {
		var id, parent, unused int
		var detail string
		mustT(t, rows.Scan(&id, &parent, &unused, &detail))
		t.Log(detail)
		if !strings.Contains(detail, "outbox_token") {
			t.Fatalf("token lookup is not indexed: %s", detail)
		}
	}
	mustT(t, rows.Err())
}

func BenchmarkAuditTokenLookup(b *testing.B) {
	for _, count := range []int{1000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			s := benchmarkEngineStore(b)
			if err := s.Update(func(tx *Tx) error {
				_, err := tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at,attempt_token)
SELECT 1,'audit:'||i,i,'final','body','sent',1,'token:'||i FROM n`, count)
				return err
			}); err != nil {
				b.Fatal(err)
			}
			if _, err := s.w.Exec(`DROP INDEX outbox_token`); err != nil {
				b.Fatal(err)
			}
			for _, indexed := range []bool{false, true} {
				if indexed {
					if _, err := s.w.Exec(`CREATE INDEX outbox_token ON outbox(conv_id,attempt_token,seq) WHERE attempt_token IS NOT NULL`); err != nil {
						b.Fatal(err)
					}
				}
				b.Run(fmt.Sprintf("indexed=%t", indexed), func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						if err := s.Read(func(tx *Tx) error {
							_, _, err := tx.OutboxByToken(1, "missing")
							return err
						}); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
