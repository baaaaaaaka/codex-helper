package store

import (
	"fmt"
	"strings"
)

const MaxProgressBatch = 16
const MaxRecoveryBatch = 256

// Recovery groups share the existing outbox and frozen batch lifecycle. Their
// queued rows may yield to ordinary messages; sending rows never do.
const RecoveryGroupPrefix = "recovery:"

func (o Outbox) Recovery() bool {
	return o.Kind == OutMirror && strings.HasPrefix(o.ProgressGroup, RecoveryGroupPrefix)
}

func batchLimit(head Outbox) int {
	if head.Recovery() {
		return MaxRecoveryBatch
	}
	return MaxProgressBatch
}

// ProgressBatchCandidates reads a bounded adjacent window starting at the
// head. Ordinary progress never crosses a final or another execution.
// Recovery can combine complete mirror records within its source group. Neither
// crosses a split message, attachment, reply, delay, or already frozen batch.
func (t *Tx) ProgressBatchCandidates(head Outbox, now int64) ([]Outbox, error) {
	if head.ProgressGroup == "" || head.BatchHead != 0 {
		return nil, nil
	}
	rows, err := collect(scanOutbox)(t.rows(`SELECT `+outboxCols+` FROM outbox WHERE conv_id=? AND seq>=? ORDER BY seq LIMIT ?`, head.ConvID, head.Seq, batchLimit(head)))
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
	if len(rows) == 0 || (len(rows) == 1 && !rows[0].Recovery()) || len(rows) > batchLimit(rows[0]) || body == "" {
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
