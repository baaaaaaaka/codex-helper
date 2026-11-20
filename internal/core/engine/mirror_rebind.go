package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

const (
	mirrorProofWindow = 8 << 10
	mirrorMetaLimit   = 32 << 10
	mirrorGapWait     = 10 * time.Second
)

type mirrorHash struct {
	start, end int64
	h          hash.Hash
}

func (h mirrorHash) extend(f *os.File, start, end int64) (mirrorHash, error) {
	next := mirrorHash{start: start, end: end, h: sha256.New()}
	from := start
	if h.h != nil && h.start == start && h.end <= end {
		if h.end == end {
			return h, nil
		}
		state, err := h.h.(encoding.BinaryMarshaler).MarshalBinary()
		if err != nil {
			return next, err
		}
		if err := next.h.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
			return next, err
		}
		from = h.end
	}
	if from == end {
		return next, nil
	}
	if _, err := io.CopyN(next.h, io.NewSectionReader(f, from, end-from), end-from); err != nil {
		return next, err
	}
	return next, nil
}

func mirrorWindow(f *os.File, off int64) ([]byte, error) {
	var buf [mirrorProofWindow]byte
	n := min(off, mirrorProofWindow)
	if n > 0 {
		if _, err := f.ReadAt(buf[:n], off-n); err != nil {
			return nil, err
		}
	}
	sum := sha256.Sum256(buf[:n])
	return sum[:], nil
}

func (f *mirrorFile) checkpoint(fh *os.File, device, off, line, end int64) (store.MirrorCheckpoint, mirrorHash, error) {
	p := store.MirrorCheckpoint{Device: device, ReadEnd: end}
	if f.src.Offset == off && len(f.proof.Window) != 0 {
		p.Window = f.proof.Window
	} else {
		var err error
		p.Window, err = mirrorWindow(fh, off)
		if err != nil {
			return p, mirrorHash{}, err
		}
	}
	if a := f.tracker.TerminalAnchor(); a.End == off && off > 0 {
		p.AnchorEnd, p.AnchorLine, p.Anchor = off, line, a.Hash[:]
	} else if f.proof.AnchorEnd == off {
		p.AnchorEnd, p.AnchorLine, p.Anchor = off, f.proof.AnchorLine, f.proof.Anchor
	}
	context, err := f.proofHash.extend(fh, off, end)
	if err == nil {
		p.Context = context.h.Sum(nil)
	}
	return p, context, err
}

type rebindRequest struct {
	file  *mirrorFile // token only; worker uses the immutable fields below
	src   store.TranscriptSource
	info  transcript.FileInfo
	proof store.MirrorCheckpoint
}

type rebindResult struct {
	job       rebindRequest
	info      transcript.FileInfo
	off, line int64
	proof     store.MirrorCheckpoint
	err       error
	retry     bool
}

func mirrorIdentityChanged(p store.MirrorCheckpoint, s store.TranscriptSource, info transcript.FileInfo, readEnd int64) bool {
	return transcript.Rewritten(info, max(readEnd, p.ReadEnd, s.Offset), s.Inode) ||
		(p.Device != 0 && info.Device != 0 && p.Device != info.Device)
}

func sameMirrorIdentity(a, b transcript.FileInfo) bool {
	return a.Inode == b.Inode && a.Device == b.Device
}

func verifyMirrorBinding(ctx context.Context, j rebindRequest) rebindResult {
	res := rebindResult{job: j, off: j.src.Offset, line: j.src.Line, proof: j.proof}
	res.err = errors.New("no trusted checkpoint for the replacement")
	p := j.proof
	if j.src.Offset < 0 || len(p.Window) != sha256.Size || len(p.Context) != sha256.Size || p.ReadEnd < j.src.Offset {
		return res
	}
	f, err := os.Open(j.src.Path)
	if err != nil {
		res.err, res.retry = err, true
		return res
	}
	defer f.Close()
	before, err := transcript.StatOpen(f)
	if err != nil || !sameMirrorIdentity(before, j.info) || before.Size < j.info.Size {
		res.err, res.retry = errors.New("file changed before verification"), true
		return res
	}
	head, err := bufio.NewReaderSize(io.LimitReader(f, mirrorMetaLimit), mirrorMetaLimit).ReadSlice('\n')
	var meta transcript.Event
	if err == nil {
		meta = transcript.ParseLine(transcript.Line{Data: bytes.TrimSpace(head)})
	} else {
		lines, _, err := transcript.ReadLimited(f, 0, 1, 1)
		if err != nil || len(lines) != 1 {
			res.err, res.retry = errors.New("replacement has no complete session header"), err != nil
			return res
		}
		meta = transcript.ParseLine(lines[0])
	}
	if meta.Kind != transcript.KindSessionMeta || meta.Meta.ID != j.src.ThreadID || j.src.ThreadID == "" {
		res.err = errors.New("replacement belongs to a different or unknown session")
		return res
	}
	span := p.ReadEnd - j.src.Offset
	var window []byte
	if p.ReadEnd <= before.Size {
		window, err = mirrorWindow(f, j.src.Offset)
		if err != nil {
			res.err, res.retry = err, true
			return res
		}
	}
	if !bytes.Equal(window, p.Window) {
		if p.AnchorEnd != j.src.Offset || len(p.Anchor) != sha256.Size {
			return res
		}
		res.off, res.line, err = findMirrorAnchor(ctx, f, before.Size, p.Anchor)
		if err != nil {
			res.err, res.retry = err, !errors.Is(err, errAnchorMissing) && !errors.Is(err, errAnchorDuplicate)
			return res
		}
		window, err = mirrorWindow(f, res.off)
		if err != nil {
			res.err, res.retry = err, true
			return res
		}
	}
	if span > before.Size-res.off {
		res.err = errors.New("replacement lost part of the open turn")
		return res
	}
	h := sha256.New()
	r := io.NewSectionReader(f, res.off, span)
	var buf []byte
	if span > 0 {
		buf = make([]byte, min(span, 64<<10))
	}
	for left := span; left > 0; {
		if err := ctx.Err(); err != nil {
			res.err = err
			return res
		}
		n, err := io.ReadFull(r, buf[:min(int64(len(buf)), left)])
		if err != nil {
			res.err, res.retry = err, true
			return res
		}
		h.Write(buf[:n])
		left -= int64(n)
	}
	if !bytes.Equal(h.Sum(nil), p.Context) {
		res.err = errors.New("replacement changed the open turn")
		return res
	}
	after, err := transcript.StatOpen(f)
	if err != nil || !sameMirrorIdentity(before, after) || after.Size < before.Size ||
		(after.Size == before.Size && !after.MTime.Equal(before.MTime)) {
		res.err, res.retry = errors.New("file changed during verification"), true
		return res
	}
	res.proof.Device, res.proof.ReadEnd, res.proof.Window = after.Device, res.off+span, window
	if p.AnchorEnd == j.src.Offset {
		res.proof.AnchorEnd, res.proof.AnchorLine = res.off, res.line
	}
	res.info, res.err = after, nil
	return res
}

var (
	errAnchorMissing   = errors.New("replacement does not contain the last trusted turn boundary")
	errAnchorDuplicate = errors.New("replacement contains duplicate checkpoint anchors")
)

// Scan the snapshot once, with constant memory. Count newlines while searching
// so the legacy line-based delivery aliases are still correct after a rebase.
// Only parse turn boundary records. A duplicate anchor is ambiguous.
func findMirrorAnchor(ctx context.Context, f *os.File, size int64, want []byte) (int64, int64, error) {
	r := bufio.NewReaderSize(io.NewSectionReader(f, 0, size), 64<<10)
	var pos, line, off, foundLine int64
	var boundary transcript.BoundaryTracker
	big := false
	for {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		b, err := r.ReadSlice('\n')
		pos += int64(len(b))
		if errors.Is(err, bufio.ErrBufferFull) {
			big = true
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, 0, err
		}
		line++
		if !big && (bytes.Contains(b, []byte("task_started")) || bytes.Contains(b, []byte("task_complete")) ||
			bytes.Contains(b, []byte("turn_aborted")) || bytes.Contains(b, []byte("turn_context"))) {
			ev := transcript.ParseLine(transcript.Line{Data: bytes.TrimSpace(b)})
			boundary.Observe(ev, pos, line)
			if boundary.Last.End == pos && bytes.Equal(boundary.Last.Hash[:], want) {
				if off != 0 {
					return 0, 0, errAnchorDuplicate
				}
				off, foundLine = pos, line
			}
		}
		big = false
	}
	if off == 0 {
		return 0, 0, errAnchorMissing
	}
	return off, foundLine, nil
}

func (e *Engine) queueMirrorRebind(f *mirrorFile, info transcript.FileInfo) {
	e.mirrorBlocked(f, true)
	if f.rebindPending || (f.rebindTried && sameMirrorIdentity(info, f.rebindAt) &&
		((sameFile(info, f.rebindAt) && !f.rebindRetry) || e.now().Before(f.rebindNext))) {
		return
	}
	jobs := e.mirror.rebindJobs
	e.mu.Lock()
	if cs := e.convs[f.src.ConvID]; cs != nil && (cs.running != nil || cs.transcriptOpen) {
		jobs = e.mirror.rebindActive
	}
	e.mu.Unlock()
	select {
	case jobs <- rebindRequest{file: f, src: f.src, info: info, proof: f.proof}:
		f.rebindPending = true
	default: // Retry the bounded queue on the next poll.
	}
}

func (e *Engine) mirrorVerifyLoop(ctx context.Context, jobs <-chan rebindRequest) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobs:
			res := verifyMirrorBinding(ctx, job)
			select {
			case <-ctx.Done():
				return
			case e.mirror.rebindResults <- res:
			}
		}
	}
}

func sameRebindSource(a, b store.TranscriptSource) bool {
	a.UpdatedAt, b.UpdatedAt = 0, 0
	return a == b
}

func (e *Engine) applyMirrorRebind(r rebindResult) {
	f := r.job.file
	if e.mirror.files[r.job.src.Path] != f || f.src != r.job.src || !f.blocked || f.legacyRealign {
		f.rebindPending = false
		return
	}
	f.rebindPending = false
	if r.err == nil && f.historyEnd > 0 && (r.off != f.src.Offset || r.info.Size < f.historyEnd) {
		r.err = errors.New("replacement moved the boundary of an active history import")
	}
	if r.err != nil {
		// A growing copy or a changed target gets a new attempt; unchanged
		// unprovable files do not repeatedly scan the same bytes.
		if !f.rebindTried || !sameMirrorIdentity(f.rebindAt, r.job.info) {
			f.rebindDelay = e.t.MirrorStat
		} else {
			f.rebindDelay = min(f.rebindDelay*2, time.Minute)
		}
		f.rebindNext = e.now().Add(f.rebindDelay)
		f.rebindTried, f.rebindAt, f.rebindRetry = true, r.job.info, r.retry
		e.logf("mirror: %s awaiting history verification (chat %d): %v; restore the original rollout or review before running helper publish-history full", filepath.Base(f.src.Path), f.src.ConvID, r.err)
		return
	}
	current, err := e.statFile(f.src.Path)
	if err != nil || !sameMirrorIdentity(current, r.info) || current.Size < r.info.Size ||
		(current.Size == r.info.Size && !current.MTime.Equal(r.info.MTime)) {
		return
	}
	changed := !sameMirrorIdentity(r.info, f.info) || !bytes.Equal(f.proof.Window, r.proof.Window) || r.off != f.src.Offset
	err = e.update(func(tx *store.Tx) error {
		src, ok, err := tx.SourceByID(f.src.ID)
		if err != nil {
			return err
		}
		if !ok || !sameRebindSource(src, r.job.src) {
			return fmt.Errorf("%w: mirror checkpoint changed during verification", store.ErrState)
		}
		if err := tx.AdvanceSource(src.ID, r.off, r.line, r.info.Size, ms(r.info.MTime), r.info.Inode, nil); err != nil {
			return err
		}
		if changed {
			if err := tx.DeleteHistoryCheck(src.Path); err != nil {
				return err
			}
		}
		return tx.SetMirrorCheckpoint(src.ID, r.proof)
	})
	if err != nil {
		e.logf("mirror: commit verified replacement: %v", err)
		return
	}
	f.src.Offset, f.src.Line, f.src.Inode = r.off, r.line, r.info.Inode
	f.src.Size, f.src.MTime = r.info.Size, ms(r.info.MTime)
	// AdvanceSource may leave UpdatedAt intact for a no-op; use the durable
	// source on future comparisons by comparing only cursor/identity fields.
	f.proof, f.proofHash, f.openVerified = r.proof, mirrorHash{}, true
	f.info, f.nextStat = r.info, time.Time{}
	if changed {
		f.proven, f.held = 0, false // offsets proven for the previous file do not apply
	}
	if !f.catchUpSet {
		f.catchUpEnd, f.catchUpSet = r.info.Size, true
	}
	f.tracker = e.mirrorTracker(f)
	f.rebindTried = false
	e.mirrorBlocked(f, false)
	e.logf("mirror: verified replacement and resumed %s at %d", filepath.Base(f.src.Path), r.off)
}

func (e *Engine) mirrorBlocked(f *mirrorFile, blocked bool) {
	e.mirror.mu.Lock()
	defer e.mirror.mu.Unlock()
	e.setMirrorBlocked(f, blocked)
}

type blockedMirrorThread struct {
	files int
	since time.Time
}

func (e *Engine) setMirrorBlocked(f *mirrorFile, blocked bool) {
	if f.blocked == blocked {
		return
	}
	f.blocked = blocked
	conv, thread := f.src.ConvID, f.src.ThreadID
	threads := e.mirror.blocked[conv]
	state := threads[thread]
	if blocked {
		if threads == nil {
			threads = make(map[string]blockedMirrorThread)
			e.mirror.blocked[conv] = threads
		}
		if state.files == 0 {
			state.since = e.now()
		}
		state.files++
		threads[thread] = state
		return
	}
	state.files--
	if state.files > 0 {
		threads[thread] = state
		return
	}
	delete(threads, thread)
	if len(threads) == 0 {
		delete(e.mirror.blocked, conv)
	}
}

func (e *Engine) historyBlocked(conv int64, thread string) bool {
	if e.mirror == nil {
		return false
	}
	e.mirror.mu.Lock()
	defer e.mirror.mu.Unlock()
	threads := e.mirror.blocked[conv]
	return threads[thread].files > 0 || (thread == "" && len(threads) > 0)
}

func (e *Engine) historyBlockedBefore(conv int64, thread string, cutoff time.Time) bool {
	e.mirror.mu.Lock()
	defer e.mirror.mu.Unlock()
	for id, state := range e.mirror.blocked[conv] {
		if (thread == "" || thread == id) && !state.since.After(cutoff) {
			return true
		}
	}
	return false
}

func (e *Engine) releaseBlockedProgress() {
	// An old final held by backpressure can outlive a process. Even then,
	// give a newly observed obstruction time to verify before dropping
	// progress which the healthy source could still supply.
	cutoff := e.now().Add(-mirrorGapWait)
	e.mirror.mu.Lock()
	ids := make([]int64, 0, len(e.mirror.blocked))
	for id := range e.mirror.blocked {
		ids = append(ids, id)
	}
	e.mirror.mu.Unlock()
	for _, id := range ids {
		e.mu.Lock()
		cs := e.convs[id]
		active := cs != nil && cs.transcriptOpen
		e.mu.Unlock()
		if !active || !e.historyBlockedBefore(id, "", cutoff) {
			continue
		}
		rows, err := e.st.BlockedProgressTurns(id, ms(cutoff))
		if err != nil || len(rows) == 0 {
			continue
		}
		released := 0
		var stillHeld bool
		if err := e.update(func(tx *store.Tx) error {
			released = 0
			for _, tr := range rows {
				if !e.historyBlockedBefore(id, tr.ThreadID, cutoff) {
					continue
				}
				if err := tx.FinishProgressWithGap(tr.ID, tr.CodexTurnID); err != nil {
					return err
				}
				released++
			}
			if released == 0 {
				return nil
			}
			var err error
			stillHeld, err = tx.HasHeldProgress(id)
			return err
		}); err != nil {
			e.logf("mirror: release outcomes while history is unavailable: %v", err)
			continue
		}
		if released > 0 {
			if !stillHeld {
				e.mu.Lock()
				if cs := e.convs[id]; cs != nil && cs.running == nil {
					cs.transcriptOpen = false
				}
				e.mu.Unlock()
			}
			e.logf("mirror: released %d outcomes for chat %d with a transcript progress gap", released, id)
			e.wakeSender()
			e.wakeRunner(id)
		}
	}
}

func (e *Engine) mirrorGapLoop(ctx context.Context) {
	tick := time.NewTicker(e.t.MirrorStat)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			e.releaseBlockedProgress()
		}
	}
}
