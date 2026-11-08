package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// trySteer delivers ordinary input before reserving a new-turn slot.
// Native TUI work needs no helper execution observer: the existing mirror
// owns its output. A helper execution keeps its single existing observer.
func (e *Engine) trySteer(ctx context.Context, id int64) bool {
	agent, ok := e.ag.(SteeringAgent)
	if !ok {
		return false
	}
	e.mu.Lock()
	cs := e.convs[id]
	if cs == nil || (cs.kind != store.KindWork && cs.kind != store.KindControl) || cs.state != store.ConvActive || cs.paused || e.draining() || e.stopping.Load() || !e.readyLocked(cs) {
		e.mu.Unlock()
		return false
	}
	rs := cs.running
	if cs.steering {
		e.mu.Unlock()
		return true
	}
	if rs != nil && rs.cancelled {
		e.mu.Unlock()
		return true
	}
	thread := cs.threadID
	expected := agent.CurrentTurn(id, thread)
	if expected == "" && rs != nil && !rs.steerExpired {
		expected = rs.codexTurnID
	}
	if expected == "" {
		e.mu.Unlock()
		return rs != nil
	}
	// Additional input belongs to the native execution's output directory.
	// It must not replace a helper execution's existing artifact contract.
	outputKey := sharedOutputKey(expected)
	if rs != nil && rs.codexTurnID == expected {
		outputKey = rs.turnKey
	}
	cs.steering = true
	e.starting++
	e.mu.Unlock()
	cleanup := func() { e.mu.Lock(); cs.steering = false; e.starting--; e.mu.Unlock() }
	var tr store.Turn
	var req TurnRequest
	got := false
	err := e.update(func(tx *store.Tx) error {
		var err error
		tr, got, err = tx.NextSteer(id)
		if err != nil || !got {
			return err
		}
		in, _, err := tx.Inbound(tr.InboundID)
		if err != nil {
			return err
		}
		files, err := tx.InboundFiles(in.ID)
		if err != nil {
			return err
		}
		if text := voiceFailure(in.Body, files); text != "" {
			msgs, err := e.notice(fmt.Sprintf("turn:%d:voice", tr.ID), text)
			if err != nil {
				return err
			}
			got = false
			return tx.FinishTurn(tr.ID, store.TurnFailed, "voice transcription failed", msgs)
		}
		req = TurnRequest{ConvID: id, ThreadID: thread, TurnKey: "cxp-steer-" + tr.TurnKey}
		req.Text, req.Images = codexInput(in.Body, files, e.outboundDir(outputKey))
		if yes, x := tx.IsQuestionReply(in.ID); x != nil {
			return x
		} else if yes {
			origin, x := tx.QuestionInputThread(in.ID)
			if x != nil {
				return x
			}
			if origin != thread {
				got = false
				return e.rejectQuestionReply(tx, tr)
			}
			req.Text, req.Images = in.Body, nil
		} else if cs.kind == store.KindControl {
			// Control Q&A keeps its history envelope. Only native question
			// replies bypass it and may enter the running control turn.
			got = false
			return nil
		}
		req.Text = strings.Replace(req.Text, teamsRequestSafety, teamsSteerSafety, 1)
		tr.TurnKey = req.TurnKey
		return tx.MarkSteering(tr.ID, thread)
	})
	if err != nil || !got {
		cleanup()
		e.wakeSender()
		if err != nil {
			e.logf("claim steer in %d: %v", id, err)
		}
		return false
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() { cleanup(); e.wakeRunner(id) }()
		if dir := e.outboundDir(outputKey); dir != "" {
			if err := os.MkdirAll(dir, 0700); err != nil {
				e.logf("output directory of steer %d: %v", tr.ID, err)
			}
		}
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		err := agent.Steer(callCtx, req, expected)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, ErrSteerExpired) {
			e.mu.Lock()
			if rs != nil && cs.running == rs {
				rs.steerExpired = true
			}
			e.mu.Unlock()
			// A real rejection is safe to dispatch later. Do not spin against a
			// stale local task snapshot while its completion is being processed.
			if err = e.update(func(tx *store.Tx) error { return tx.Unsteer(tr.ID) }); err != nil {
				e.logf("release steer %d: %v", tr.ID, err)
			}
			return
		}
		var rejected *InputRejected
		if errors.As(err, &rejected) {
			if x := e.update(func(tx *store.Tx) error {
				msgs, x := e.notice(fmt.Sprintf("turn:%d:rejected", tr.ID), "Codex did not accept the additional input: "+userError(rejected.Reason))
				if x != nil {
					return x
				}
				return tx.FinishTurn(tr.ID, store.TurnNotStarted, rejected.Reason, msgs)
			}); x != nil {
				e.logf("record rejected steer %d: %v", tr.ID, x)
			}
			e.wakeSender()
			return
		}
		if err != nil {
			_, found, lookupErr := e.ag.Lookup(ctx, id, thread, tr.TurnKey)
			if lookupErr != nil || !found {
				e.logf("steer %d outcome unknown: %v; lookup: %v", tr.ID, err, lookupErr)
				e.mu.Lock()
				cs.paused = true
				e.mu.Unlock()
				_ = e.update(func(tx *store.Tx) error {
					if x := tx.Pause(id, "steer outcome unknown"); x != nil {
						return x
					}
					return e.enqueueNotice(tx, id, fmt.Sprintf("turn:%d:unknown", tr.ID), "The additional input may have reached Codex. This chat is paused to avoid sending it twice; the current task continues. Close its TUI windows before restarting the helper to reconcile it.")
				})
				e.wakeSender()
				return
			}
		}
		// The row records delivery; its execution's existing observer or the
		// history mirror publishes the answer. Never publish it a second time.
		if err = e.update(func(tx *store.Tx) error {
			if err := tx.MarkRunning(tr.ID, expected); err != nil {
				return err
			}
			return tx.FinishTurn(tr.ID, store.TurnCompleted, "", nil)
		}); err != nil {
			e.logf("record steer %d: %v", tr.ID, err)
		}
	}()
	return true
}
