package engine

import (
	"fmt"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

const nativeQuietPrefix = "native-quiet:"
const textNativeQuiet = "Codex is still running. No new progress yet. Send `helper status` to check."

type nativeQuietState struct {
	turn   string
	since  time.Time
	queued bool // the outbox accepted this turn's notice key
}

func nativeQuietKey(conv int64, turn string) string {
	return fmt.Sprintf("%s%d:%s", nativeQuietPrefix, conv, turn)
}

// nativeQuietOf requires positive evidence of a native turn and excludes
// work covered by the helper runner. It only reads the live app-server cache.
func (e *Engine) nativeQuietOf(id int64) (string, nativeQuietState, bool) {
	e.mu.Lock()
	cs := e.convs[id]
	if cs == nil {
		e.mu.Unlock()
		return "", nativeQuietState{}, false
	}
	if cs.kind != store.KindWork || cs.state != store.ConvActive || cs.chat == "" ||
		cs.threadID == "" || cs.running != nil || id == e.inboxID {
		quiet := cs.nativeQuiet
		e.mu.Unlock()
		return "", quiet, false
	}
	thread, quiet, waiting := cs.threadID, cs.nativeQuiet, e.questionWaiting[id]
	e.mu.Unlock()
	agent, ok := e.ag.(GoalAgent)
	if !ok {
		return "", nativeQuietState{}, false
	}
	return agent.Info(id, thread).NativeTurn, quiet, waiting
}

func (e *Engine) remindQuietNativeTurns() {
	if _, ok := e.ag.(GoalAgent); !ok || e.t.NativeQuietHint <= 0 || e.stopping.Load() {
		return
	}
	e.mu.Lock()
	var ids []int64
	for id, cs := range e.convs {
		if (cs.kind == store.KindWork && cs.state == store.ConvActive) || cs.nativeQuiet.turn != "" {
			ids = append(ids, id)
		}
	}
	e.mu.Unlock()
	for _, id := range ids {
		e.remindQuietNativeTurn(id)
	}
}

func (e *Engine) remindQuietNativeTurn(id int64) {
	turn, quiet, waiting := e.nativeQuietOf(id)
	now := e.now()
	if turn != quiet.turn {
		e.mu.Lock()
		if cs := e.convs[id]; cs != nil {
			cs.nativeQuiet = nativeQuietState{turn: turn, since: now}
		}
		e.mu.Unlock()
		return
	}
	if turn == "" || quiet.queued || waiting || now.Sub(quiet.since) < e.t.NativeQuietHint {
		return
	}

	accepted := false
	err := e.update(func(tx *store.Tx) error {
		accepted = false
		current, state, waiting := e.nativeQuietOf(id)
		if current != turn || state.turn != turn || waiting || e.now().Sub(state.since) < e.t.NativeQuietHint {
			return nil
		}
		pending, err := tx.HasOtherActiveOutbox(id, 0)
		if err != nil || pending {
			return err
		}
		if err := e.enqueueStatus(tx, id, nativeQuietKey(id, turn), textNativeQuiet); err != nil {
			return err
		}
		accepted = true
		return nil
	})
	if err != nil {
		e.logf("native quiet notice in %d: %v", id, err)
		return
	}
	if accepted {
		e.mu.Lock()
		if cs := e.convs[id]; cs != nil && cs.nativeQuiet.turn == turn {
			cs.nativeQuiet.queued = true
		}
		e.mu.Unlock()
		e.wakeSender()
	}
}

// Recheck just before claiming a row. A rejected send may re-enter this path;
// an ambiguously accepted send still uses the normal outbox reconciliation.
func (e *Engine) nativeQuietSendable(tx *store.Tx, row store.Outbox) (bool, error) {
	turn, quiet, waiting := e.nativeQuietOf(row.ConvID)
	if turn == "" || turn != quiet.turn || waiting || e.t.NativeQuietHint <= 0 ||
		e.now().Sub(quiet.since) < e.t.NativeQuietHint ||
		row.DeliveryKey != nativeQuietKey(row.ConvID, turn)+":1" {
		return false, nil
	}
	pending, err := tx.HasOtherActiveOutbox(row.ConvID, row.ID)
	return !pending, err
}
