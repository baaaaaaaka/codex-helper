package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/testperf"
)

// Measure the storage transactions of 100 complete turns, not just candidate
// lookup. Real Graph/Codex dispatch and wakeups have separate black-box E2E.
func TestEngineSQLiteSendChainProbe(t *testing.T) {
	if os.Getenv("CXP_SQLITE_PERF_PROBE") != "1" {
		t.Skip("nightly synthetic performance probe")
	}
	n, err := strconv.Atoi(os.Getenv("CXP_SQLITE_PERF_ROWS"))
	if err != nil || n < 1 {
		t.Fatal("positive CXP_SQLITE_PERF_ROWS required")
	}
	s, _ := openTest(t)
	conv := newWork(t, s, "send-chain")
	mustT(t, s.Update(func(tx *Tx) error {
		_, err := tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT ?,'old:'||i||':final',i,'final',?,'unknown',1 FROM n`, n, conv, strings.Repeat("x", 1024))
		if err != nil {
			return err
		}
		_, err = tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
INSERT INTO pending_notifications(id,turn_id,conv_id,key,kind,text,request,attempted) SELECT 'old:'||i||':',0,?,'old:'||i||':','','','retained request',0 FROM n`, n, conv)
		return err
	}))
	var path string
	var sequence int
	mustT(t, s.w.QueryRow(`PRAGMA database_list`).Scan(&sequence, new(string), &path))
	testperf.Measure(t, filepath.Dir(path), "send-chain-100", n, func() {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("prompt-%d", i)
			mustT(t, s.Update(func(tx *Tx) error {
				_, err := tx.AdmitWindow("send-chain", conv, []InboundMsg{prompt(key, int64(i+1))}, key)
				return err
			}))
			var turn Turn
			mustT(t, s.Update(func(tx *Tx) error {
				var ok bool
				var err error
				turn, ok, err = tx.NextRunnable(conv)
				if err != nil || !ok {
					return fmt.Errorf("next runnable: %t %v", ok, err)
				}
				return tx.MarkStarting(turn.ID, "thread")
			}))
			mustT(t, s.Update(func(tx *Tx) error { return tx.MarkRunning(turn.ID, key) }))
			notice := PendingNotification{Turn: turn.ID, Conv: conv, Request: key}
			mustT(t, s.Update(func(tx *Tx) error {
				if err := tx.FinishTurn(turn.ID, TurnCompleted, "", []OutMsg{out(fmt.Sprintf("turn:%d:final:1", turn.ID), OutFinal)}); err != nil {
					return err
				}
				return tx.AddNotification(notice)
			}))
			var send Outbox
			mustT(t, s.Read(func(tx *Tx) error {
				list, err := tx.NextSendable(tx.Now())
				if err != nil || len(list) != 1 {
					return fmt.Errorf("sendable rows=%d err=%v", len(list), err)
				}
				send = list[0]
				return nil
			}))
			mustT(t, s.Update(func(tx *Tx) error { _, err := tx.BeginSend(send.ID); return err }))
			mustT(t, s.Update(func(tx *Tx) error { return tx.MarkSent(send.ID, key) }))
			mustT(t, s.Read(func(tx *Tx) error {
				list, _, err := tx.NotificationPageFor(NotificationScope{ID: notice.ID()}, 0, 128, false)
				if err != nil || len(list) != 1 {
					return fmt.Errorf("ready notices=%d err=%v", len(list), err)
				}
				return nil
			}))
			mustT(t, s.Update(func(tx *Tx) error {
				won, err := tx.AttemptNotification(notice.ID())
				if err != nil || !won {
					return fmt.Errorf("card reservation: %t %v", won, err)
				}
				return nil
			}))
			mustT(t, s.Update(func(tx *Tx) error { return tx.DeleteNotification(notice.ID()) }))
		}
	})
	if got := countWhere(t, s, `SELECT count(*) FROM pending_notifications`); got != n {
		t.Fatalf("unknown evidence changed: %d want %d", got, n)
	}
}
