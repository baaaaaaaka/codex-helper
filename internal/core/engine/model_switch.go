package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// pendingSettings uses durable admitted commands, including during catalog
// lookup and after restart. A slow validation never lets later input pass it.
func pendingSettings(tx *store.Tx, conv int64, control bool) (bool, error) {
	rows, err := tx.PendingCommands(conv)
	if err != nil {
		return false, err
	}
	for _, in := range rows {
		var p parsed
		if control {
			p = parseControl(in.Body)
		} else {
			text, _ := stripCodexMention(in.Body)
			p = parseWork(text)
		}
		if p.form != formCommand {
			continue
		}
		sub, rest := commandParts(p.arg)
		if p.name == cmdModelName {
			switch sub {
			case "use", "switch", "set", "reset":
				return true, nil
			case "effort", "reasoning-effort", "thinking-effort":
				sub, rest = commandParts(rest)
			default:
				continue
			}
		} else if p.name != cmdEffortName {
			continue
		}
		switch sub {
		case "", "status", "current", "list", "ls", "options", "choices", "help", "?":
			continue
		}
		_ = rest
		return true, nil
	}
	return false, nil
}

func (e *Engine) applyModelEffort(j cmdJob, c modelChat, model, modelSrc, effort, effortSrc, text string) {
	info := e.turnInfo(c.cs.id)
	if info.Goal != "" || info.SelfTurn {
		e.reply(j, "Model switching is unavailable while an independently continuing Codex goal or external turn is active.", nil)
		e.wakeRunner(c.cs.id)
		return
	}
	if guard, ok := e.ag.(interface{ ModelSwitchAllowed(int64) bool }); ok && !guard.ModelSwitchAllowed(c.cs.id) {
		e.reply(j, "Close the attached TUI before switching this chat's model settings.", nil)
		e.wakeRunner(c.cs.id)
		return
	}
	changed := false
	err := e.replyIn(j, func(tx *store.Tx) (string, error) {
		th, exists, err := tx.Thread(c.cs.id)
		if err != nil {
			return "", err
		}
		if !exists {
			th, err = e.newChatThread(tx, c.cs.id, c.th.Cwd)
			if err != nil {
				return "", err
			}
		}
		stored := th
		previousEffort := firstNonEmpty(th.PendingEffort, th.Effort)
		if c.control && previousEffort == "" {
			previousEffort = ctlEffortFallback
		}
		oldModel, oldEffort := TurnSettings(c.models, firstNonEmpty(th.PendingModel, th.Model), previousEffort)
		newModel, newEffort := TurnSettings(c.models, model, effort)
		changed = oldModel != newModel || oldEffort != newEffort
		if changed {
			th.SettingsVersion++
		}
		th.Model, th.ModelSource, th.PendingModel = model, modelSrc, ""
		th.Effort, th.EffortSource, th.PendingEffort = effort, effortSrc, ""
		if !exists || stored != th {
			if err = tx.Bind(th); err != nil {
				return "", err
			}
		}
		if changed {
			open, x := tx.OpenTurnsOf(c.cs.id)
			if x != nil {
				return "", x
			}
			for _, tr := range open {
				if tr.Steering {
					continue
				}
				if tr.State == store.TurnStarting || tr.State == store.TurnRunning || (tr.State == store.TurnQueued && tr.RetryDeadline != 0) {
					if x = tx.BeginModelSwitch(tr); x != nil {
						return "", x
					}
					return text + "\nThe active task will stop and continue automatically with these settings.", nil
				}
			}
		}
		return text + "\nThese settings apply when this chat next starts work.", nil
	})
	if err != nil {
		return
	}
	if changed {
		e.mu.Lock()
		c.cs.capWake = time.Time{}
		e.mu.Unlock()
	}
	e.wakeRunner(c.cs.id)
}

// tryModelSwitch interrupts only the pinned run. Unlike user cancellation it
// never kills the backend. The runner/observer owns terminal processing.
func (e *Engine) tryModelSwitch(ctx context.Context, id int64) bool {
	var sw store.ModelSwitch
	var source store.Turn
	switching := false
	err := e.st.Read(func(tx *store.Tx) error {
		var err error
		sw, switching, err = tx.ModelSwitch(id)
		if err != nil || !switching {
			return err
		}
		source, _, err = tx.Turn(sw.SourceTurn)
		return err
	})
	if err != nil {
		e.logf("read model switch: %v", err)
		return true
	}
	if !switching {
		return false
	}
	e.mu.Lock()
	cs := e.convs[id]
	if cs == nil || cs.paused {
		e.mu.Unlock()
		return true
	}
	rs := cs.running
	if rs == nil {
		e.mu.Unlock()
		return false
	} // tryStart prepares the continuation
	if rs.turnID != source.ID || source.Attempt != sw.SourceAttempt {
		e.mu.Unlock()
		return true
	}
	if cs.steering || rs.switching || rs.cancelled {
		e.mu.Unlock()
		return true
	}
	rs.switching = true
	e.mu.Unlock()
	if guard, ok := e.ag.(interface{ ModelSwitchAllowed(int64) bool }); ok && !guard.ModelSwitchAllowed(id) {
		e.pauseModelSwitch(cs, source, "an external client attached during the switch")
		return true
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		call, cancel := context.WithTimeout(ctx, e.t.CancelWait)
		defer cancel()
		var err error
		if targeted, ok := e.ag.(interface {
			InterruptExecution(context.Context, int64, string) error
		}); ok {
			err = targeted.InterruptExecution(call, id, source.TurnKey)
		} else {
			err = e.ag.Interrupt(call, id)
		}
		if err != nil {
			e.pauseModelSwitch(cs, source, "Codex did not confirm the interrupt")
			return
		}
		select {
		case <-rs.done:
		case <-ctx.Done():
		case <-call.Done():
			e.pauseModelSwitch(cs, source, "Codex has not stopped after the interrupt")
		}
	}()
	return true
}

func (e *Engine) pauseModelSwitch(cs *convState, tr store.Turn, reason string) {
	applied := false
	err := e.update(func(tx *store.Tx) error {
		sw, yes, err := tx.ModelSwitch(cs.id)
		if err != nil {
			return err
		}
		if !yes || sw.SourceTurn != tr.ID || sw.SourceAttempt != tr.Attempt {
			return nil
		}
		applied = true
		if err := tx.Pause(cs.id, "model switch: "+reason); err != nil {
			return err
		}
		return e.enqueueNotice(tx, cs.id, fmt.Sprintf("switch:%d:%d:paused", tr.ID, tr.Attempt), "Model switch paused: "+reason+". No continuation was sent. Check the session before retrying.")
	})
	if err != nil {
		e.logf("pause model switch: %v", err)
		e.fail(err)
	}
	if applied {
		e.mu.Lock()
		cs.paused = true
		e.mu.Unlock()
	}
	e.wakeSender()
}

// settleModelSwitch handles a terminal owned execution before ordinary retry
// handling. New rows, not a replay of the old prompt, continue accepted work.
func (e *Engine) settleModelSwitch(ctx context.Context, cs *convState, tr store.Turn, out TurnOutcome, cancelled bool) bool {
	var sw store.ModelSwitch
	var yes bool
	if err := e.st.Read(func(tx *store.Tx) error { var err error; sw, yes, err = tx.ModelSwitch(cs.id); return err }); err != nil {
		e.fail(err)
		return true
	}
	if !yes || sw.SourceTurn != tr.ID || sw.SourceAttempt != tr.Attempt {
		return false
	}
	if cancelled || out.Status == StatusCompleted {
		// Finish clears the handoff atomically with the terminal outcome below.
		return false
	}
	if out.Unconfirmed {
		e.pauseModelSwitch(cs, tr, "the start outcome could not be confirmed in Codex history")
		return true
	}
	if out.NotStarted {
		if err := e.update(func(tx *store.Tx) error { return tx.RetryUnsubmittedSwitch(tr) }); err != nil {
			e.fail(err)
		}
		return true
	}
	if out.Status != StatusInterrupted && !retryableFailure(out) {
		e.pauseModelSwitch(cs, tr, "the interrupted execution has no safe terminal outcome")
		return true
	}
	recorded, found, lookupErr := e.ag.Lookup(ctx, cs.id, tr.ThreadID, tr.TurnKey)
	if lookupErr != nil || !found || recorded.Status == StatusInProgress || (tr.CodexTurnID != "" && recorded.CodexTurnID != tr.CodexTurnID) {
		e.pauseModelSwitch(cs, tr, "the original input or terminal could not be confirmed in Codex history")
		return true
	}
	// A successful steer response alone does not prove it survived interruption.
	var inputs []store.Turn
	if err := e.st.Read(func(tx *store.Tx) error {
		var err error
		inputs, err = tx.SteeredInputs(cs.id, tr.CodexTurnID)
		return err
	}); err != nil {
		e.fail(err)
		return true
	}
	for _, in := range inputs {
		_, found, err := e.ag.Lookup(ctx, cs.id, tr.ThreadID, in.TurnKey)
		if err != nil || !found {
			e.pauseModelSwitch(cs, tr, "additional input could not be confirmed in Codex history")
			return true
		}
	}
	if err := e.update(func(tx *store.Tx) error {
		return tx.FinishTurn(tr.ID, store.TurnInterrupted, "model settings changed", nil)
	}); err != nil {
		e.fail(err)
	}
	return true
}

// prepareModelContinuation runs inside the execution-admission transaction.
// It can wait for input preparation without creating an empty continuation.
func (e *Engine) prepareModelContinuation(tx *store.Tx, cs *convState) (bool, error) {
	sw, yes, err := tx.ModelSwitch(cs.id)
	if err != nil || !yes {
		return !yes, err
	}
	source, ok, err := tx.Turn(sw.SourceTurn)
	if err != nil {
		return false, err
	}
	if !ok || source.Attempt != sw.SourceAttempt {
		return false, fmt.Errorf("model switch source changed")
	}
	if sw.Continuation != 0 {
		return true, nil
	}
	if source.State == store.TurnCompleted || source.State == store.TurnCancelled {
		return true, tx.EndModelSwitch(cs.id, source.ID, source.Attempt)
	}
	if source.State == store.TurnStarting || source.State == store.TurnRunning {
		return false, nil
	}
	if source.State != store.TurnInterrupted && !(source.State == store.TurnQueued && source.RetryDeadline != 0) {
		return false, nil
	}
	th, _, err := tx.Thread(cs.id)
	if err != nil {
		return false, err
	}
	if source.ThreadID == "" || source.ThreadID != th.ThreadID {
		return false, fmt.Errorf("model switch thread changed")
	}
	if source.State == store.TurnQueued {
		if source.CodexTurnID == "" {
			return true, tx.EndModelSwitch(cs.id, source.ID, source.Attempt)
		}
		if err = tx.FinishTurn(source.ID, store.TurnInterrupted, "model settings changed during retry", nil); err != nil {
			return false, err
		}
	}
	_, err = tx.NewContinuation(sw, source, nil)
	return err == nil, err
}

// claimContinuationInputs is called only once execution can actually start.
func (e *Engine) claimContinuationInputs(tx *store.Tx, cs *convState, tr store.Turn) (bool, error) {
	// Control questions have always been separate requests, not live steer.
	if cs.kind == store.KindControl {
		return true, nil
	}
	open, err := tx.OpenTurnsOf(cs.id)
	if err != nil {
		return false, err
	}
	var inputs []store.Turn
	for _, in := range open {
		if in.ID == tr.ID || in.Queue || in.State != store.TurnQueued || in.Steering || in.ResumeFrom != 0 {
			continue
		}
		question, x := tx.IsQuestionReply(in.InboundID)
		if x != nil {
			return false, x
		}
		if question {
			continue
		}
		files, x := tx.InboundFiles(in.InboundID)
		if x != nil {
			return false, x
		}
		for _, f := range files {
			if f.State == "pending" {
				return false, nil
			}
		}
		message, _, x := tx.Inbound(in.InboundID)
		if x != nil {
			return false, x
		}
		if failure := voiceFailure(message.Body, files); failure != "" {
			msgs, x := e.notice(fmt.Sprintf("turn:%d:voice", in.ID), failure)
			if x != nil {
				return false, x
			}
			if x = tx.FinishTurn(in.ID, store.TurnFailed, "voice transcription failed", msgs); x != nil {
				return false, x
			}
			continue
		}
		inputs = append(inputs, in)
	}
	for _, in := range inputs {
		if err = tx.AddContinuationInput(tr.ID, in.ID); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (e *Engine) continuationRequest(tx *store.Tx, tr store.Turn, req TurnRequest) (TurnRequest, error) {
	inputs, err := tx.ContinuationInputs(tr.ID)
	if err != nil {
		return req, err
	}
	req.Text, req.Images = continueText, nil
	if len(inputs) == 0 {
		return req, nil
	}
	var parts []string
	for _, input := range inputs {
		if input.State != store.TurnQueued {
			continue
		} // earlier accepted attempts already recorded it
		in, _, x := tx.Inbound(input.InboundID)
		if x != nil {
			return req, x
		}
		files, x := tx.InboundFiles(in.ID)
		if x != nil {
			return req, x
		}
		body, images := codexInput(in.Body, files, e.outboundDir(tr.OutputKey))
		if failure := voiceFailure(in.Body, files); failure != "" {
			body = "The following input was unavailable: " + failure
		}
		parts = append(parts, strings.Replace(body, teamsRequestSafety, teamsSteerSafety, 1))
		req.Images = append(req.Images, images...)
	}
	if len(parts) > 0 {
		req.Text = strings.Join(parts, "\n\n---\n\n")
	}
	return req, nil
}

func turnOutputKey(tr store.Turn) string {
	if tr.OutputKey != "" {
		return tr.OutputKey
	}
	return tr.TurnKey
}

// cancelModelHandoff also recognizes the interval with no native running turn.
// Queue-only cancellation never calls it.
func (e *Engine) cancelModelHandoff(tx *store.Tx, cs *convState, target store.Turn) (bool, error) {
	sw, yes, err := tx.ModelSwitch(cs.id)
	if err != nil {
		return false, err
	}
	e.mu.Lock()
	native := cs.running != nil && cs.running.turnID == target.ID
	if native {
		cs.running.cancelled = true
	}
	e.mu.Unlock()
	if !yes && target.ResumeFrom == 0 {
		validating, x := pendingSettings(tx, cs.id, cs.kind == store.KindControl)
		if x != nil {
			return false, x
		}
		if !validating {
			return native, nil
		}
	}
	if yes && target.ID != sw.SourceTurn && target.ID != sw.Continuation {
		return native, nil
	}
	if yes {
		if err = tx.EndModelSwitch(cs.id, sw.SourceTurn, sw.SourceAttempt); err != nil {
			return false, err
		}
	}
	if !native && (target.State == store.TurnQueued || target.State == store.TurnStarting || target.State == store.TurnRunning) {
		if err = tx.FinishTurn(target.ID, store.TurnCancelled, "cancelled by the user", nil); err != nil {
			return false, err
		}
	}
	if cs.kind == store.KindControl {
		return native, nil
	}
	// Unconsumed supplements belong to the cancelled task; explicit queued
	// requests and question replies keep their independent routing.
	open, err := tx.OpenTurnsOf(cs.id)
	if err != nil {
		return false, err
	}
	for _, in := range open {
		if in.State != store.TurnQueued || in.Queue || in.ID == target.ID {
			continue
		}
		q, x := tx.IsQuestionReply(in.InboundID)
		if x != nil {
			return false, x
		}
		if q {
			continue
		}
		if x = tx.FinishTurn(in.ID, store.TurnCancelled, "active task cancelled", nil); x != nil {
			return false, x
		}
	}
	return native, nil
}

func (e *Engine) pauseContinuation(cs *convState, tr store.Turn, reason string) {
	err := e.update(func(tx *store.Tx) error {
		if err := tx.Pause(cs.id, "model continuation: "+reason); err != nil {
			return err
		}
		return e.enqueueNotice(tx, cs.id, fmt.Sprintf("turn:%d:continuation-unknown", tr.ID), "Task continuation paused: "+reason+". No input was resubmitted. Check the session before retrying.")
	})
	if err != nil {
		e.fail(err)
		return
	}
	e.mu.Lock()
	cs.paused = true
	e.mu.Unlock()
	e.wakeSender()
}
