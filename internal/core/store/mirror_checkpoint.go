package store

import (
	"context"
	"fmt"
)

// MirrorCheckpoint records the complete bytes seen when the source cursor was
// committed. The cursor itself can be earlier, to reconstruct an open turn.
type MirrorCheckpoint struct {
	Device, ReadEnd, AnchorEnd, AnchorLine int64
	Window, Context, Anchor                []byte
}

func (s *Store) MirrorCheckpoints() (map[int64]MirrorCheckpoint, error) {
	rows, err := s.r.QueryContext(context.Background(), `SELECT source_id,device,read_end,window_hash,context_hash,anchor_end,anchor_line,anchor_hash FROM mirror_checkpoints`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]MirrorCheckpoint{}
	for rows.Next() {
		var id int64
		var p MirrorCheckpoint
		if err := rows.Scan(&id, &p.Device, &p.ReadEnd, &p.Window, &p.Context, &p.AnchorEnd, &p.AnchorLine, &p.Anchor); err != nil {
			return nil, err
		}
		out[id] = p
	}
	return out, rows.Err()
}

func (t *Tx) SetMirrorCheckpoint(id int64, p MirrorCheckpoint) error {
	_, err := t.exec(`INSERT INTO mirror_checkpoints(source_id,device,read_end,window_hash,context_hash,anchor_end,anchor_line,anchor_hash)
VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET device=excluded.device,read_end=excluded.read_end,
window_hash=excluded.window_hash,context_hash=excluded.context_hash,anchor_end=excluded.anchor_end,
anchor_line=excluded.anchor_line,anchor_hash=excluded.anchor_hash`,
		id, p.Device, p.ReadEnd, p.Window, p.Context, p.AnchorEnd, p.AnchorLine, p.Anchor)
	return err
}

func (t *Tx) DeleteMirrorCheckpoint(id int64) error {
	_, err := t.exec(`DELETE FROM mirror_checkpoints WHERE source_id=?`, id)
	return err
}

// ApproveMirrorRecovery is called only for explicit full-history publication.
// A sending/unknown batch remains read-back-only.
func (t *Tx) ApproveMirrorRecovery(conv int64) error {
	_, err := t.exec(`UPDATE outbox SET state='queued',recovery_review=0
WHERE conv_id=? AND state='dropped' AND recovery_review=1`, conv)
	return err
}

func (t *Tx) MirrorRecoveryReviewCount(conv int64) (int, error) {
	var n int
	err := t.row(`SELECT count(*) FROM outbox WHERE conv_id=? AND state='dropped' AND recovery_review=1`, conv).Scan(&n)
	return n, err
}

// FinishProgressWithGap releases a stored outcome when its transcript cannot
// be verified. The distinct flag prevents a later scan from publishing progress
// for this execution after its final.
func (t *Tx) FinishProgressWithGap(turn int64, nativeID string) error {
	n, err := affected(t.exec(`UPDATE turns SET transcript_done=1,transcript_gap=1
WHERE id=? AND codex_turn_id=? AND transcript_progress=1 AND transcript_done=0
AND state IN ('completed','cancelled','failed','interrupted','not_started')`, turn, nativeID))
	if err != nil || n == 0 {
		return err
	}
	tr, ok, err := t.Turn(turn)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: missing held turn %d", ErrState, turn)
	}
	return t.ReleaseHeldProgress(tr.ConvID, turn)
}

func (t *Tx) TranscriptProgressGap(turn int64) (bool, error) {
	var gap bool
	err := t.row(`SELECT EXISTS(SELECT 1 FROM turns WHERE id=? AND transcript_gap=1)`, turn).Scan(&gap)
	return gap, err
}

func (s *Store) BlockedProgressTurns(conv, before int64) ([]Turn, error) {
	return collect(scanTurn)(s.r.QueryContext(context.Background(), `SELECT `+turnCols+` FROM turns
WHERE id IN (SELECT DISTINCT hold_turn FROM outbox WHERE conv_id=? AND hold_turn IS NOT NULL
AND state='queued' AND created_at<=?) AND transcript_done=0
AND state IN ('completed','cancelled','failed','interrupted','not_started')`, conv, before))
}
