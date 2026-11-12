package store

// ModelSwitch is one unfinished handoff. Settings remain on threads; this
// record only identifies the execution that must settle before continuation.
type ModelSwitch struct {
	ConvID, SourceTurn int64
	SourceAttempt      int
	ThreadID           string
	Continuation       int64
}

func (t *Tx) ModelSwitch(conv int64) (ModelSwitch, bool, error) {
	return one(t.row(`SELECT conv_id,source_turn,source_attempt,thread_id,COALESCE(continuation,0) FROM model_switches WHERE conv_id=?`, conv), func(s scanner) (ModelSwitch, error) {
		var v ModelSwitch
		err := s.Scan(&v.ConvID, &v.SourceTurn, &v.SourceAttempt, &v.ThreadID, &v.Continuation)
		return v, err
	})
}

func (t *Tx) BeginModelSwitch(tr Turn) error {
	_, err := t.exec(`INSERT INTO model_switches(conv_id,source_turn,source_attempt,thread_id) VALUES(?,?,?,?) ON CONFLICT(conv_id) DO NOTHING`, tr.ConvID, tr.ID, tr.Attempt, tr.ThreadID)
	return err
}
func (t *Tx) EndModelSwitch(conv, source int64, attempt int) error {
	_, err := t.exec(`DELETE FROM model_switches WHERE conv_id=? AND source_turn=? AND source_attempt=?`, conv, source, attempt)
	return err
}

// PendingCommands is bounded by unprocessed inbound commands, not history.
func (t *Tx) PendingCommands(conv int64) ([]Inbound, error) {
	return collect(scanInbound)(t.rows(`SELECT `+inboundCols+` FROM inbound WHERE conv_id=? AND state IN ('queued','held') AND state='queued' AND kind='command' ORDER BY id`, conv))
}

func (t *Tx) SteeredInputs(conv int64, native string) ([]Turn, error) {
	return collect(scanTurn)(t.rows(`SELECT `+turnCols+` FROM turns WHERE conv_id=? AND codex_turn_id=? AND steering=1 ORDER BY id`, conv, native))
}

// NewContinuation reserves exactly one execution, preserving the logical
// task's output contract without copying the original user request.
func (t *Tx) NewContinuation(sw ModelSwitch, source Turn, inputs []Turn) (Turn, error) {
	if sw.Continuation != 0 {
		tr, _, err := t.Turn(sw.Continuation)
		return tr, err
	}
	id, err := t.newTurn(sw.ConvID, 0, false)
	if err != nil {
		return Turn{}, err
	}
	key := source.OutputKey
	if key == "" {
		key = source.TurnKey
	}
	if _, err = t.exec(`UPDATE turns SET resume_from=?,output_key=? WHERE id=?`, source.ID, key, id); err != nil {
		return Turn{}, err
	}
	if err = t.moveUnacceptedInputs(source.ID, id); err != nil {
		return Turn{}, err
	}
	for _, in := range inputs {
		if _, err = t.exec(`INSERT INTO continuation_inputs(execution_id,input_turn) VALUES(?,?)`, id, in.ID); err != nil {
			return Turn{}, err
		}
	}
	if _, err = t.exec(`UPDATE model_switches SET continuation=? WHERE conv_id=? AND source_turn=? AND source_attempt=?`, id, sw.ConvID, sw.SourceTurn, sw.SourceAttempt); err != nil {
		return Turn{}, err
	}
	tr, _, err := t.Turn(id)
	return tr, err
}

func (t *Tx) ContinuationInputs(execution int64) ([]Turn, error) {
	return collect(scanTurn)(t.rows(`SELECT `+turnCols+` FROM turns WHERE id IN (SELECT input_turn FROM continuation_inputs WHERE execution_id=?) ORDER BY id`, execution))
}
func (t *Tx) IsContinuationInput(id int64) (bool, error) {
	var yes bool
	err := t.row(`SELECT EXISTS(SELECT 1 FROM continuation_inputs WHERE input_turn=?)`, id).Scan(&yes)
	return yes, err
}
func (t *Tx) AcceptContinuationInputs(execution int64) error {
	rows, err := t.ContinuationInputs(execution)
	if err != nil {
		return err
	}
	for _, tr := range rows {
		if tr.State == TurnQueued {
			if err = t.FinishTurn(tr.ID, TurnCompleted, "included in continuation", nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Tx) RetryUnsubmittedSwitch(tr Turn) error {
	if err := t.turnMove(tr.ID, []string{TurnStarting, TurnRunning}, `state='queued',started_at=NULL,codex_turn_id=NULL,retry_deadline=NULL`); err != nil {
		return err
	}
	return t.EndModelSwitch(tr.ConvID, tr.ID, tr.Attempt)
}

func (t *Tx) AddContinuationInput(execution, input int64) error {
	_, err := t.exec(`INSERT INTO continuation_inputs(execution_id,input_turn) VALUES(?,?) ON CONFLICT(input_turn) DO NOTHING`, execution, input)
	return err
}

func (t *Tx) BindTurnSettings(id int64, thread, model, effort string) error {
	_, err := t.exec(`UPDATE turns SET thread_id=?,model=?,effort=? WHERE id=?`, nullStr(thread), nullStr(model), nullStr(effort), id)
	return err
}

// TaskSuccessor detects that an interrupted request already continued, even
// though later executions have no original inbound row. Never replay that root.
func (t *Tx) TaskSuccessor(tr Turn) (Turn, bool, error) {
	key := tr.OutputKey
	if key == "" {
		key = tr.TurnKey
	}
	return one(t.row(`SELECT `+turnCols+` FROM turns WHERE output_key=? AND id>? ORDER BY id DESC LIMIT 1`, key, tr.ID), scanTurn)
}

func (t *Tx) RetryContinuation(source Turn) (int64, error) {
	if _, yes, err := t.TaskSuccessor(source); err != nil {
		return 0, err
	} else if yes {
		return 0, ErrState
	}
	id, err := t.newTurn(source.ConvID, 0, false)
	if err != nil {
		return 0, err
	}
	if _, err = t.exec(`UPDATE turns SET resume_from=?,output_key=? WHERE id=?`, source.ID, source.OutputKey, id); err != nil {
		return 0, err
	}
	return id, t.moveUnacceptedInputs(source.ID, id)
}

func (t *Tx) moveUnacceptedInputs(from, to int64) error {
	_, err := t.exec(`UPDATE continuation_inputs SET execution_id=? WHERE execution_id=? AND input_turn IN (SELECT id FROM turns WHERE state IN ('queued','starting','running'))`, to, from)
	return err
}
