package store

import "context"

// ReleaseHeldProgress appends a turn's final parts after the transcript prefix
// committed in this transaction. Until then they remain in the same outbox,
// excluded from transport selection and source backpressure. No payload copy
// or second queue is required. Delivery identities do not change.
func (t *Tx) ReleaseHeldProgress(conv, turn int64) error {
	rows, err := collect(func(sc scanner) (int64, error) { var id int64; err := sc.Scan(&id); return id, err })(t.rows(`SELECT id FROM outbox WHERE conv_id=? AND state IN ('queued','sending','unknown') AND hold_turn=? ORDER BY seq`, conv, turn))
	if err != nil || len(rows) == 0 {
		return err
	}
	last, err := t.LastSeq(conv)
	if err != nil {
		return err
	}
	for _, row := range rows {
		last++
		if _, err := t.exec(`UPDATE outbox SET hold_turn=NULL,seq=? WHERE id=? AND state='queued' AND hold_turn=?`, last, row, turn); err != nil {
			return err
		}
	}
	return nil
}

// HeldProgressConversations restores only outstanding publication ownership,
// including tasks whose chats were closed while their native turn was running.
func (s *Store) HeldProgressConversations() ([]int64, error) {
	return collect(func(sc scanner) (int64, error) { var id int64; err := sc.Scan(&id); return id, err })(s.r.QueryContext(context.Background(), `SELECT DISTINCT conv_id FROM outbox WHERE state IN ('queued','sending','unknown') AND hold_turn IS NOT NULL`))
}

// SettledProgressTurns is the interrupted-producer fallback. Completed native
// tasks require a terminal record. A broken interrupted producer may omit it;
// its held notice can follow a settled EOF, after the finish itself has aged.
func (s *Store) SettledProgressTurns(conv, before int64) ([]Turn, error) {
	return collect(scanTurn)(s.r.QueryContext(context.Background(), `SELECT `+turnCols+` FROM turns WHERE id IN (SELECT DISTINCT hold_turn FROM outbox WHERE conv_id=? AND state IN ('queued','sending','unknown') AND hold_turn IS NOT NULL AND created_at<=?) AND state IN ('cancelled','failed','interrupted','not_started')`, conv, before))
}
