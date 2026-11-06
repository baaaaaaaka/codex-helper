package engine

import (
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// `cxp teams run --once` (F-CLI-10): one poll, flush and turn cycle, then
// exit. The engine runs as usual; the command asks CycleDone until the
// cycle is over and then stops it. Nothing here changes what the engine
// does.

// onceState is the mark CycleDone measures the read pass from.
type onceState struct {
	mark time.Time
}

// CycleDone reports whether, since the engine loaded its conversations,
// every chat it reads has been read once (or failed once), and what that
// brought in is finished: no turn running or waiting to start, no command
// being handled and nothing left to send. Call it repeatedly; the first
// call after the engine is ready sets the mark.
func (e *Engine) CycleDone() bool {
	e.mu.Lock()
	if e.controlID == 0 {
		e.mu.Unlock()
		return false // not loaded yet
	}
	if ctl := e.convs[e.controlID]; ctl == nil || ctl.chat == "" {
		e.mu.Unlock()
		return false // the control chat is being created
	}
	now := e.now()
	if e.once.mark.IsZero() {
		e.once.mark = now
		e.mu.Unlock()
		return false
	}
	live, chats := map[int64]bool{}, map[int64]bool{}
	for id, cs := range e.convs {
		if cs.running != nil {
			e.mu.Unlock()
			return false
		}
		if cs.chat == "" || (cs.kind == store.KindWork && cs.state != store.ConvActive) || (cs.kind == store.KindControl && cs.state == store.ConvClosed) {
			continue
		}
		if cs.reading || !cs.nextRead.After(e.once.mark) {
			e.mu.Unlock()
			return false // not read since the mark
		}
		chats[id] = true
		if !cs.paused {
			live[id] = true
		}
	}
	e.mu.Unlock()
	e.cmdMu.Lock()
	busy := len(e.cmdQueue) > 0 || len(e.cmdPending) > 0
	e.cmdMu.Unlock()
	if busy {
		return false
	}
	queued, err := e.st.QueuedTurnConvs()
	if err != nil {
		return false
	}
	for _, id := range queued {
		if live[id] {
			return false // a turn that will start
		}
	}
	pending, err := e.st.PendingOutboxConvs()
	if err != nil {
		return false
	}
	for _, id := range pending {
		if chats[id] {
			return false
		}
	}
	return true
}
