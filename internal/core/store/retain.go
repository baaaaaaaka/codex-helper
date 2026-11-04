package store

import (
	"context"
	"time"
)

// Retention windows (§6).
const (
	BodyRetention = 7 * 24 * time.Hour
	KeyRetention  = 120 * 24 * time.Hour
	retainBatch   = 5000
)

// retainSteps are bounded statements run repeatedly until they change no
// rows. ?1 = body cutoff, ?2 = key cutoff, ?3 = now, ?4 = batch size.
// Open rows (queued, sending, unknown, held, open turns) and every row of a
// 'hold' conversation are never touched.
var retainSteps = []string{
	// Bodies of terminal inbound rows older than 7 days.
	`UPDATE inbound SET body = NULL WHERE id IN (
  SELECT i.id FROM inbound i JOIN conversations c ON c.id = i.conv_id
  WHERE i.body IS NOT NULL AND i.state IN ('done','ignored') AND i.received_at < ?1 AND c.state <> 'hold' LIMIT ?4)`,
	// Bodies of terminal outbox rows older than 7 days.
	`UPDATE outbox SET body = NULL WHERE id IN (
  SELECT o.id FROM outbox o JOIN conversations c ON c.id = o.conv_id
  WHERE o.body IS NOT NULL AND o.state IN ('sent','dropped') AND o.created_at < ?1 AND c.state <> 'hold' LIMIT ?4)`,
	// Upload state of terminal outbox rows.
	`DELETE FROM outbox_files WHERE outbox_id IN (
  SELECT f.outbox_id FROM outbox_files f JOIN outbox o ON o.id = f.outbox_id JOIN conversations c ON c.id = o.conv_id
  WHERE o.state IN ('sent','dropped') AND o.created_at < ?1 AND c.state <> 'hold' LIMIT ?4)`,
	// Unlink inbound rows from terminal turns about to be deleted.
	`UPDATE inbound SET turn_id = NULL WHERE turn_id IN (
  SELECT t.id FROM turns t JOIN conversations c ON c.id = t.conv_id
  WHERE t.state NOT IN ('queued','starting','running') AND t.queued_at < ?2 AND c.state <> 'hold'
    AND t.id NOT IN (SELECT MAX(t2.id) FROM turns t2 WHERE t2.state NOT IN ('queued','starting','running') GROUP BY t2.conv_id)
  LIMIT ?4)`,
	// Terminal turns older than 120 days, except the latest terminal turn of
	// each conversation (needed for 'retry last').
	`DELETE FROM turns WHERE id IN (
  SELECT t.id FROM turns t JOIN conversations c ON c.id = t.conv_id
  WHERE t.state NOT IN ('queued','starting','running') AND t.queued_at < ?2 AND c.state <> 'hold'
    AND t.id NOT IN (SELECT MAX(t2.id) FROM turns t2 WHERE t2.state NOT IN ('queued','starting','running') GROUP BY t2.conv_id)
    AND NOT EXISTS (SELECT 1 FROM inbound i WHERE i.turn_id = t.id)
  LIMIT ?4)`,
	// Attachments of terminal inbound rows about to be deleted.
	`DELETE FROM inbound_files WHERE inbound_id IN (
  SELECT i.id FROM inbound i JOIN conversations c ON c.id = i.conv_id
  WHERE i.state IN ('done','ignored') AND i.received_at < ?2 AND c.state <> 'hold' LIMIT ?4)`,
	// Terminal inbound rows older than 120 days that no kept turn points at.
	`DELETE FROM inbound WHERE id IN (
  SELECT i.id FROM inbound i JOIN conversations c ON c.id = i.conv_id
  WHERE i.state IN ('done','ignored') AND i.received_at < ?2 AND c.state <> 'hold'
    AND NOT EXISTS (SELECT 1 FROM turns t WHERE t.inbound_id = i.id)
    AND NOT EXISTS (SELECT 1 FROM inbound_files f WHERE f.inbound_id = i.id)
  LIMIT ?4)`,
	// Terminal outbox rows older than 120 days.
	`DELETE FROM outbox WHERE id IN (
  SELECT o.id FROM outbox o JOIN conversations c ON c.id = o.conv_id
  WHERE o.state IN ('sent','dropped') AND o.created_at < ?2 AND c.state <> 'hold'
    AND NOT EXISTS (SELECT 1 FROM outbox_files f WHERE f.outbox_id = o.id)
  LIMIT ?4)`,
	// Terminal notifications older than 120 days.
	`DELETE FROM notifications WHERE id IN (
  SELECT id FROM notifications WHERE state IN ('sent','dropped') AND created_at < ?2 LIMIT ?4)`,
	// Expired gates.
	`DELETE FROM gates WHERE until < ?3`,
}

// Retain applies retention in bounded batches (each its own transaction)
// and then runs PRAGMA incremental_vacuum(1000). It returns the number of
// rows changed. It runs daily and at startup.
func (s *Store) Retain(now time.Time) (int, error) {
	bodyCut := now.Add(-BodyRetention).UnixMilli()
	keyCut := now.Add(-KeyRetention).UnixMilli()
	total := 0
	for _, step := range retainSteps {
		for {
			var n int64
			err := s.Update(func(t *Tx) error {
				var err error
				n, err = affected(t.exec(step, bodyCut, keyCut, now.UnixMilli(), retainBatch))
				return err
			})
			if err != nil {
				return total, err
			}
			total += int(n)
			if n < retainBatch {
				break
			}
		}
	}
	if _, err := s.w.ExecContext(context.Background(), `PRAGMA incremental_vacuum(1000)`); err != nil {
		return total, err
	}
	return total, nil
}
