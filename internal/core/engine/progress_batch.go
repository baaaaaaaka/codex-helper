package engine

import (
	"encoding/hex"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// No merge timer or extra queue: the existing sender packs what is already
// waiting when it obtains its normal write slot and token.
func (e *Engine) progressBatch(tx *store.Tx, head store.Outbox, now int64) (store.Outbox, error) {
	combiner, ok := e.cfg.Channel.(channel.PartCombiner)
	if !ok || head.ProgressGroup == "" || head.BatchHead != 0 {
		return head, nil
	}
	rows, err := tx.ProgressBatchCandidates(head, now)
	if err != nil {
		return head, err
	}
	var parts []channel.Part
	var merged channel.Part
	count := 1
	for i, r := range rows {
		parts = append(parts, channel.Part{Index: r.Part, Count: r.Parts, Body: r.Body, Hash: hex.EncodeToString(r.BodyHash), Mention: r.Mention, ReplyTo: r.ReplyTo, Size: len(r.Body)})
		if i == 0 {
			continue
		}
		p, ok := combiner.CombineProgress(parts)
		if !ok {
			break
		}
		merged, count = p, i+1
	}
	if count < 2 {
		return head, nil
	}
	if err := tx.FreezeProgressBatch(rows[:count], merged.Body, hashBytes(merged.Hash)); err != nil {
		return head, err
	}
	r, _, err := tx.Outbox(head.ID)
	return r, err
}
