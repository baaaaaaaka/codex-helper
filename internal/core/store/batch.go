package store

import "fmt"

const MaxProgressBatch = 16

// ProgressBatchCandidates reads a bounded adjacent window starting at the
// head. It never crosses a final, another execution, a split message, an
// attachment, a reply, a delay, or an already frozen batch.
func (t *Tx) ProgressBatchCandidates(head Outbox, now int64) ([]Outbox, error) {
	if head.ProgressGroup == "" || head.BatchHead != 0 {
		return nil, nil
	}
	rows, err := collect(scanOutbox)(t.rows(`SELECT `+outboxCols+` FROM outbox WHERE conv_id=? AND seq>=? ORDER BY seq LIMIT ?`, head.ConvID, head.Seq, MaxProgressBatch))
	if err != nil {
		return nil, err
	}
	var batch []Outbox
	for _, r := range rows {
		if r.State != OutQueued || r.HoldTurn != 0 || r.BatchHead != 0 || r.ProgressGroup != head.ProgressGroup || r.Kind != head.Kind || r.Part != 1 || r.Parts != 1 || r.Mention || r.ReplyTo != "" || r.NextAttemptAt > now || r.HashAlgo != "" {
			break
		}
		_, file, err := t.OutboxFile(r.ID)
		if err != nil {
			return nil, err
		}
		if file {
			break
		}
		batch = append(batch, r)
	}
	return batch, nil
}

// FreezeProgressBatch records the exact transport payload and membership
// before BeginSend, in the same transaction. Original source parts remain
// intact. A definite rejection retries this same batch, never a larger one.
func (t *Tx) FreezeProgressBatch(rows []Outbox, body string, hash []byte) error {
	if len(rows) < 2 || len(rows) > MaxProgressBatch || body == "" {
		return fmt.Errorf("store: invalid progress batch")
	}
	head := rows[0]
	candidates, err := t.ProgressBatchCandidates(head, t.now)
	if err != nil {
		return err
	}
	if len(candidates) < len(rows) {
		return fmt.Errorf("%w: progress batch changed", ErrState)
	}
	for i, r := range rows {
		if candidates[i].ID != r.ID {
			return fmt.Errorf("%w: nonadjacent progress batch", ErrState)
		}
		if _, err := t.exec(`UPDATE outbox SET batch_head=? WHERE id=?`, head.ID, r.ID); err != nil {
			return err
		}
	}
	_, err = t.exec(`UPDATE outbox SET batch_body=?,body_hash=? WHERE id=?`, body, hash, head.ID)
	return err
}
