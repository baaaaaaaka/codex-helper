package store

import (
	"path/filepath"
	"testing"
)

// BenchmarkRecoveryAdmission compares the existing runnable lookup with the
// additional recovery checkpoint lookup on a conversation with long history.
func BenchmarkRecoveryAdmission(b *testing.B) {
	s, err := OpenWith(filepath.Join(b.TempDir(), "core.sqlite"), Options{PIDAlive: alwaysAlive})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Acquire(testIdent(100)); err != nil {
		b.Fatal(err)
	}
	var conv int64
	if err = s.Update(func(tx *Tx) error {
		var e error
		conv, e = tx.CreateConversation(NewConversation{Label: "bench", Kind: KindWork, Channel: "teams", ExtChatID: "bench"})
		if e != nil {
			return e
		}
		_, e = tx.exec(`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<10001)
INSERT INTO turns(id,conv_id,turn_key,state,queued_at) SELECT x,?, 'bench-'||x,CASE WHEN x=10001 THEN 'queued' ELSE 'completed' END,1 FROM n`, conv)
		return e
	}); err != nil {
		b.Fatal(err)
	}
	for _, extra := range []bool{false, true} {
		name := "baseline"
		if extra {
			name = "with_recovery_lookup"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := s.Update(func(tx *Tx) error {
					tr, ok, e := tx.NextRunnable(conv)
					if e != nil {
						return e
					}
					if !ok {
						b.Fatal("missing runnable turn")
					}
					if extra {
						_, _, e = tx.Recovery(tr.ID)
					}
					return e
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
