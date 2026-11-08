package engine

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

func (e *Engine) newMirrorTracker(thread string, off, line int64, skip transcript.Skip) *transcript.Tracker {
	t := transcript.NewTracker(thread, off, line, skip)
	t.StableFallbackIDs()
	t.HelperProgressByClient(func(client, turn string) bool {
		e.mirror.mu.Lock()
		defer e.mirror.mu.Unlock()
		local := e.mirror.progressTurns[turn]
		if local == 0 {
			local = e.mirror.progressClients[client]
			if local != 0 && turn != "" {
				e.mirror.progressTurns[turn] = local
			}
		}
		return local != 0
	})
	return t
}

// thread/start can create a rollout before its binding callback runs. An
// exec-origin source discovered in that interval was ignored intentionally;
// reconsider only that thread's known path after binding, without a disk walk.
func (e *Engine) reconsiderBoundSources(ctx context.Context) {
	e.mirror.mu.Lock()
	threads := e.mirror.bound
	e.mirror.bound = nil
	var paths []string
	for _, id := range threads {
		for path, state := range e.mirror.known {
			if state == store.SrcIgnored && strings.HasSuffix(filepath.Base(path), "-"+id+".jsonl") {
				paths = append(paths, path)
			}
		}
	}
	e.mirror.mu.Unlock()
	for _, path := range paths {
		info, err := e.statFile(path)
		if err != nil {
			continue
		}
		e.newRollout(ctx, transcript.Found{Path: path, Info: info}, false)
	}
}

// EOF is a fallback only for a failed/interrupted native producer. The usual
// completed path releases held output at its exact terminal source position.
func (e *Engine) settleProgressAtEOF(f *mirrorFile, info transcript.FileInfo, now time.Time) {
	e.mu.Lock()
	cs := e.convs[f.src.ConvID]
	active := cs != nil && cs.transcriptOpen
	e.mu.Unlock()
	if !active || now.Sub(info.MTime) < e.t.MirrorStat {
		return
	}
	rows, err := e.st.SettledProgressTurns(f.src.ConvID, ms(now.Add(-e.t.MirrorStat)))
	if err != nil || len(rows) == 0 {
		return
	}
	if err := e.update(func(tx *store.Tx) error {
		for _, tr := range rows {
			if err := tx.CompleteTranscriptProgress(tr.ID, tr.CodexTurnID); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		e.logf("release interrupted output: %v", err)
		return
	}
	e.mu.Lock()
	if cs := e.convs[f.src.ConvID]; cs != nil {
		cs.transcriptOpen = false
	}
	e.mu.Unlock()
	e.wakeSender()
	e.wakeRunner(f.src.ConvID)
}
