package store

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func benchmarkEngineStore(b *testing.B) *Store {
	b.Helper()
	s, err := Open(filepath.Join(b.TempDir(), "core.sqlite"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	if _, err = s.Acquire(testIdent(100)); err != nil {
		b.Fatal(err)
	}
	if err = s.Update(func(tx *Tx) error {
		_, err := tx.CreateConversation(NewConversation{Label: "work", Kind: KindWork, Channel: "teams", ExtChatID: "chat"})
		return err
	}); err != nil {
		b.Fatal(err)
	}
	return s
}

// These workloads separate historical growth from unresolved pending growth.
// The latter still requires visiting pending metadata: no ready-state mirror
// or undocumented eviction hides that cost.
func BenchmarkNotificationCandidates(b *testing.B) {
	for _, tc := range []struct{ history, unknown int }{{1, 0}, {1000, 0}, {100000, 0}, {100000, 1000}, {1, 100000}} {
		b.Run(fmt.Sprintf("history=%d/unknown=%d", tc.history, tc.unknown), func(b *testing.B) {
			s := benchmarkEngineStore(b)
			if err := s.Update(func(tx *Tx) error {
				if _, err := tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT 1,'history:'||i,i,'final',?,'sent',1 FROM n`, tc.history, strings.Repeat("x", 1024)); err != nil {
					return err
				}
				if tc.unknown > 0 {
					if _, err := tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT 1,'old:'||i||':part',?+i,'final',?,'unknown',1 FROM n`, tc.unknown, tc.history, strings.Repeat("x", 1024)); err != nil {
						return err
					}
					if _, err := tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
INSERT INTO pending_notifications(id,turn_id,conv_id,key,kind,text,request,attempted) SELECT 'old:'||i||':',0,1,'old:'||i||':','','','request',0 FROM n`, tc.unknown); err != nil {
						return err
					}
				}
				if _, err := tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT 1,'unrelated-progress:'||i,?+i,'progress',?,'queued',1 FROM n`, tc.history+tc.unknown, strings.Repeat("x", 1024)); err != nil {
					return err
				}
				return tx.AddNotification(PendingNotification{Conv: 1, Key: "new:", Request: "ready"})
			}); err != nil {
				b.Fatal(err)
			}
			for _, scope := range []NotificationScope{{}, {ID: "new:"}} {
				b.Run(fmt.Sprintf("event=%t", scope.ID != ""), func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if err := s.Read(func(tx *Tx) error {
							list, _, err := tx.NotificationPageFor(scope, 0, 128, false)
							if len(list) != 1 || list[0].Key != "new:" {
								b.Fatalf("wrong candidate: %+v", list)
							}
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

func BenchmarkCLIProgressLookup(b *testing.B) {
	for _, count := range []int{1, 1000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			s := benchmarkEngineStore(b)
			if err := s.Update(func(tx *Tx) error {
				if _, err := tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
INSERT INTO cli_requests(id,stage,conv_id,old_chat,chat,url,title,name,import_id,done,error) SELECT 'old:'||i,'done',1,'','','','','',0,1,'' FROM n`, count); err != nil {
					return err
				}
				return tx.SetCLIProgress("target", CLIProgress{Stage: "done", Result: &CLIResult{Lines: []string{"first", "second"}}})
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := s.Read(func(tx *Tx) error {
					p, ok, err := tx.CLIProgress("target")
					if !ok || p.Result == nil || len(p.Result.Lines) != 2 {
						b.Fatal(p)
					}
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
