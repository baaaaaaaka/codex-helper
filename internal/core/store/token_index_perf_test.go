package store

import (
	"fmt"
	"testing"
	"time"
)

// Include index construction and space, and measure the added per-send write
// work independently of the indexed lookup benchmark.
func BenchmarkTokenIndexWriteCost(b *testing.B) {
	s := benchmarkEngineStore(b)
	if err := s.Update(func(tx *Tx) error {
		_, err := tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100000)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at,attempt_token)
SELECT 1,'seed:'||i,i,'final','body','sent',1,'token:'||i FROM n`)
		return err
	}); err != nil {
		b.Fatal(err)
	}
	if _, err := s.w.Exec(`DROP INDEX outbox_token`); err != nil {
		b.Fatal(err)
	}
	pages := func() int64 {
		var count, size, free int64
		for _, q := range []struct {
			sql string
			out *int64
		}{{`PRAGMA page_count`, &count}, {`PRAGMA page_size`, &size}, {`PRAGMA freelist_count`, &free}} {
			if err := s.w.QueryRow(q.sql).Scan(q.out); err != nil {
				b.Fatal(err)
			}
		}
		return (count - free) * size
	}
	var seq int64 = 100000
	for _, indexed := range []bool{false, true} {
		if indexed {
			before := pages()
			start := time.Now()
			if _, err := s.w.Exec(`CREATE INDEX outbox_token ON outbox(conv_id,attempt_token,seq) WHERE attempt_token IS NOT NULL`); err != nil {
				b.Fatal(err)
			}
			b.Logf("100k index construction: %s; used pages: %d -> %d bytes (delta %d)", time.Since(start), before, pages(), pages()-before)
		}
		b.Run(fmt.Sprintf("indexed=%t", indexed), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				seq++
				if err := s.Update(func(tx *Tx) error {
					_, err := tx.exec(`INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at,attempt_token) VALUES(1,?,?,'final','body','sent',1,?)`, fmt.Sprintf("new:%d", seq), seq, fmt.Sprintf("new-token:%d", seq))
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
