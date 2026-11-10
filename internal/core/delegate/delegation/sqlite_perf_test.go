package delegation

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func BenchmarkWorkerSQLitePointOperations(b *testing.B) {
	for _, n := range []int{1, 1000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			s, err := OpenWorkerDB(filepath.Join(b.TempDir(), "worker.sqlite"))
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			ctx, now := context.Background(), time.Now().UTC()
			err = s.transaction(ctx, true, func(tx *sql.Tx) error {
				out, err := tx.PrepareContext(ctx, outboxInsertSQL)
				if err != nil {
					return err
				}
				defer out.Close()
				threads, err := tx.PrepareContext(ctx, remote_threadsInsertSQL)
				if err != nil {
					return err
				}
				defer threads.Close()
				for i := 0; i < n; i++ {
					if _, err = out.ExecContext(ctx, outboxArgs(OutboxRecord{RecordID: fmt.Sprint(i), Status: OutboxUnknown, Error: strings.Repeat("x", 1024)})...); err != nil {
						return err
					}
					if _, err = threads.ExecContext(ctx, remote_threadsArgs(RemoteThread{ThreadID: fmt.Sprint(i), MachineID: "target", State: RemoteThreadStateActive, Summary: strings.Repeat("x", 1024), LastUsedAt: now.Add(time.Hour).Format(time.RFC3339Nano)})...); err != nil {
						return err
					}
				}
				for i := 0; i < 3; i++ {
					if _, err := putRemoteThreadTx(ctx, tx, RemoteThread{ThreadID: fmt.Sprint("ready", i), MachineID: "target", State: RemoteThreadStateIdle, LastUsedAt: now.Format(time.RFC3339Nano)}); err != nil {
						return err
					}
				}
				_, err = putOutboxRecordTx(ctx, tx, OutboxRecord{RecordID: "target", Status: OutboxUnknown, Attempts: 1})
				return err
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, reserved, err := s.ReserveOutbox(ctx, Record{RecordID: "target"}, "chat", now); err != nil || reserved {
					b.Fatalf("reservation=%v err=%v", reserved, err)
				}
				list, err := s.ReusableThreads(ctx, "target", "", "", now, 3)
				if err != nil || len(list) != 3 {
					b.Fatalf("list=%d err=%v", len(list), err)
				}
			}
		})
	}
}
