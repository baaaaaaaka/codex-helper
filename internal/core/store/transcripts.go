package store

import (
	"context"
	"fmt"
)

// Source reads a transcript source by path.
func (t *Tx) Source(path string) (TranscriptSource, bool, error) {
	return one(t.row(`SELECT `+sourceCols+` FROM transcript_sources WHERE path = ?`, path), scanSource)
}

// SourceByID reads a transcript source by id.
func (t *Tx) SourceByID(id int64) (TranscriptSource, bool, error) {
	return one(t.row(`SELECT `+sourceCols+` FROM transcript_sources WHERE id = ?`, id), scanSource)
}

// UpsertSource inserts a source (with its initial offset) or updates the
// metadata of an existing one: thread, conversation, state and the observed
// size/mtime/inode. It never moves an existing offset; use AdvanceSource.
// Nothing is written when nothing changed.
func (t *Tx) UpsertSource(src TranscriptSource) (int64, error) {
	if src.Offset < 0 || src.Line < 0 {
		return 0, fmt.Errorf("%w: negative offset", ErrState)
	}
	var id int64
	err := t.row(`INSERT INTO transcript_sources(path,thread_id,conv_id,state,offset,line,size,mtime,inode,updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(path) DO UPDATE SET thread_id=excluded.thread_id, conv_id=excluded.conv_id, state=excluded.state,
  size=excluded.size, mtime=excluded.mtime, inode=excluded.inode, updated_at=excluded.updated_at
WHERE thread_id IS NOT excluded.thread_id OR conv_id IS NOT excluded.conv_id OR state IS NOT excluded.state
   OR size IS NOT excluded.size OR mtime IS NOT excluded.mtime OR inode IS NOT excluded.inode
RETURNING id`, src.Path, nullStr(src.ThreadID), nullInt(src.ConvID), src.State, src.Offset, src.Line,
		src.Size, src.MTime, nullInt(src.Inode), t.now).Scan(&id)
	if err == nil {
		return id, nil
	}
	// The conflict update was skipped (unchanged): RETURNING yields no row.
	cur, ok, err2 := t.Source(src.Path)
	if err2 != nil {
		return 0, err2
	}
	if !ok {
		return 0, err
	}
	return cur.ID, nil
}

// AdvanceSource moves a source's processed position and enqueues the
// messages produced from the records before it, in one transaction (the
// progress-with-output invariant, §3.9). Re-running from an older offset
// re-enqueues the same delivery keys, which are no-ops. An unchanged call
// writes nothing. The offset never goes back to 0 once it had progress.
func (t *Tx) AdvanceSource(id, offset, line, size, mtime, inode int64, outputs []OutMsg) error {
	cur, ok, err := t.SourceByID(id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: transcript source %d", ErrNotFound, id)
	}
	if offset < 0 || line < 0 || (offset == 0 && cur.Offset > 0) {
		return fmt.Errorf("%w: source %d offset %d -> %d", ErrState, id, cur.Offset, offset)
	}
	if len(outputs) > 0 {
		if cur.ConvID == 0 {
			return fmt.Errorf("%w: source %d has no conversation", ErrState, id)
		}
		if _, err := t.Enqueue(cur.ConvID, outputs); err != nil {
			return err
		}
	}
	if cur.Offset == offset && cur.Line == line && cur.Size == size && cur.MTime == mtime && cur.Inode == inode {
		return nil
	}
	_, err = t.exec(`UPDATE transcript_sources SET offset = ?, line = ?, size = ?, mtime = ?, inode = ?, updated_at = ? WHERE id = ?`,
		offset, line, size, mtime, nullInt(inode), t.now, id)
	return err
}

// Sources lists every transcript source (loaded once at start; unchanged
// files are then detected by stat against this copy, without DB access).
func (s *Store) Sources() ([]TranscriptSource, error) {
	return collect(scanSource)(s.r.QueryContext(context.Background(), `SELECT `+sourceCols+` FROM transcript_sources ORDER BY id`))
}

// SetGate records a throttling gate (Retry-After) that must survive restart.
func (t *Tx) SetGate(key string, until int64, reason string) error {
	_, err := t.exec(`INSERT INTO gates(key,until,reason) VALUES (?,?,?)
ON CONFLICT(key) DO UPDATE SET until = excluded.until, reason = excluded.reason
WHERE excluded.until > gates.until`, key, until, nullStr(reason))
	return err
}

// Gates returns the unexpired gates.
func (s *Store) Gates(now int64) ([]Gate, error) {
	rows, err := s.r.QueryContext(context.Background(), `SELECT key,until,COALESCE(reason,'') FROM gates WHERE until > ? ORDER BY key`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Gate
	for rows.Next() {
		var g Gate
		if err := rows.Scan(&g.Key, &g.Until, &g.Reason); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}
