package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Cross-machine delegation, worker side (F-XM-2/3). Another machine's
// helper (`cxp delegate start`) appends a request record to this machine's
// inbox chat; this engine claims it, runs it as a Codex turn on a thread of
// its own, and appends the result record to the same inbox, where the
// requester reads it.
//
// Every request runs as a turn of the inbox conversation, so the turn
// machinery gives it its guarantees: admitted exactly once (the delegation
// id is recorded in the same transaction as the turn), never re-run after a
// restart (a turn found interrupted ends with a "blocked" result), and the
// claim and result records are outbox rows with idempotent delivery keys and
// read-back resolution.
//
// The inbox is not polled on its own: the activity scan reads it when a
// message arrives, and the running turn keeps it hot.

// Delegation record kinds (DelegRecord.Kind).
const (
	DelegRequest = "request"
	DelegClaim   = "claim"
	DelegStatus  = "status"
	DelegResult  = "result"
	DelegCancel  = "cancel"
)

// DelegRecord is a decoded inbox record, in the terms the engine needs.
type DelegRecord struct {
	Kind string
	// ID is the delegation id (all records of one delegation share it).
	ID string
	// Mine: a request addressed to this machine, or a claim, status or
	// result written for this machine.
	Mine bool
	// Worker is the claiming worker instance (claim, status, result).
	Worker string
	// Expired: a request past its expiry.
	Expired bool
	// Thread is the requester's remote thread id when the request continues
	// an earlier delegated thread ("" = a new thread).
	Thread string
}

// Delegations is the delegation wire protocol (internal/core/delegate).
// Records travel as plain text; the engine stores a request's text as the
// body of its inbound row and hands it back to build the replies.
type Delegations interface {
	// Decode reads one inbox message; ok is false for anything else.
	Decode(text string, now time.Time) (DelegRecord, bool)
	// Prompt is the Codex input for a request.
	Prompt(request string) (string, error)
	// Claim and Running are this worker's first records for a request.
	Claim(request, worker string, now time.Time) (string, error)
	Running(request, worker string, now time.Time) (string, error)
	// Result is the terminal record: the final answer when ok, else the
	// reason the task did not complete.
	Result(request, worker string, ok bool, body string, now time.Time) (string, error)
}

const (
	metaDelegPrefix       = "engine_deleg:"        // delegation id -> turn id, or a skip reason
	metaDelegCancelPrefix = "engine_deleg_cancel:" // delegation id -> "1" (requester cancelled)
	metaDelegThreadPrefix = "engine_deleg_thread:" // turn id -> Codex thread id
	metaDelegRemotePrefix = "engine_deleg_remote:" // requester's remote thread id -> Codex thread id
	metaDelegWorker       = "engine_deleg_worker"  // this engine's stable worker instance id
	metaDelegThreads      = "engine_deleg_threads" // JSON list of delegated Codex thread ids (never mirrored)
)

// maxDelegThreads bounds the remembered delegated threads (the oldest are
// dropped; their rollouts are long settled by then).
const maxDelegThreads = 1000

// loadDelegThreads reads the delegated thread ids at start.
func (e *Engine) loadDelegThreads() error {
	v, _, err := e.st.Meta(metaDelegThreads)
	if err != nil || v == "" {
		return err
	}
	var ids []string
	if json.Unmarshal([]byte(v), &ids) != nil {
		return nil
	}
	e.mu.Lock()
	for _, id := range ids {
		e.delegThreads[id] = true
	}
	e.mu.Unlock()
	return nil
}

// addDelegThread records a delegated thread in tx.
func addDelegThread(tx *store.Tx, threadID string) error {
	v, _, err := tx.Meta(metaDelegThreads)
	if err != nil {
		return err
	}
	var ids []string
	if v != "" {
		_ = json.Unmarshal([]byte(v), &ids)
	}
	for _, id := range ids {
		if id == threadID {
			return nil
		}
	}
	ids = append(ids, threadID)
	if len(ids) > maxDelegThreads {
		ids = ids[len(ids)-maxDelegThreads:]
	}
	b, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	return tx.SetMeta(metaDelegThreads, string(b))
}

// textDelegInterrupted is the result of a delegated task cut short by a
// helper restart (it is never re-run).
const textDelegInterrupted = "The delegated task was interrupted when the worker's helper restarted. It was not re-run; send it again if it is still needed."

func (e *Engine) delegations() Delegations {
	if !e.cfg.Registry {
		return nil
	}
	return e.cfg.Delegations
}

// delegWorker returns the stable worker instance id, created on first use.
func (e *Engine) delegWorker(tx *store.Tx) (string, error) {
	v, ok, err := tx.Meta(metaDelegWorker)
	if err != nil || (ok && v != "") {
		return v, err
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	v = "w-" + hex.EncodeToString(b[:])
	return v, tx.SetMeta(metaDelegWorker, v)
}

// inboxWindow pre-reads one inbox window: the delegations that some other
// worker already claimed or finished (taking those over could run them
// twice) and the ones the requester cancelled. A request in the same
// window as such a record is not run.
type inboxWindow struct {
	skip   map[string]string // delegation id -> reason
	cancel []string
	taken  map[string]bool // requests admitted in this window
}

func (e *Engine) scanInbox(tx *store.Tx, msgs []channel.InboundMsg, now time.Time) (*inboxWindow, error) {
	d := e.delegations()
	w := &inboxWindow{skip: map[string]string{}, taken: map[string]bool{}}
	if d == nil {
		return w, nil
	}
	worker, err := e.delegWorker(tx)
	if err != nil {
		return nil, err
	}
	for _, m := range msgs {
		if m.Kind != channel.KindMessage {
			continue
		}
		r, ok := d.Decode(m.Text, now)
		if !ok {
			continue
		}
		switch r.Kind {
		case DelegCancel:
			w.skip[r.ID] = "cancelled"
			w.cancel = append(w.cancel, r.ID)
		case DelegClaim, DelegStatus, DelegResult:
			if r.Mine && r.Worker != worker {
				w.skip[r.ID] = "handled by another worker"
			}
		}
	}
	return w, nil
}

// classifyInbox decides what an inbox message is: a request this engine
// takes becomes a prompt (and so a turn); everything else is recorded and
// ignored.
func (e *Engine) classifyInbox(tx *store.Tx, w *inboxWindow, c classified, now time.Time) (classified, error) {
	c.in.Kind, c.chatter = store.InCommand, true
	d := e.delegations()
	if d == nil || w == nil || c.in.Sender == store.SenderSelf {
		return c, nil
	}
	r, ok := d.Decode(c.msg.Text, now)
	if !ok || r.Kind != DelegRequest || !r.Mine {
		return c, nil
	}
	_, known, err := tx.Meta(metaDelegPrefix + r.ID)
	if err != nil {
		return c, err
	}
	reason := w.skip[r.ID]
	switch {
	case known || w.taken[r.ID]:
		return c, nil // a copy of a request already taken
	case r.Expired:
		reason = "expired"
	}
	if reason != "" {
		e.logf("delegation %s not run: %s", r.ID, reason)
		return c, tx.SetMeta(metaDelegPrefix+r.ID, reason)
	}
	w.taken[r.ID] = true
	c.in.Kind, c.chatter, c.delegation = store.InPrompt, false, r.ID
	c.in.Sender = store.SenderOwner
	return c, nil
}

// admitDelegation records a taken request and queues its claim and running
// records, in the admission transaction.
func (e *Engine) admitDelegation(tx *store.Tx, conv int64, a store.Admitted, c *classified) error {
	d := e.delegations()
	if err := tx.SetMeta(metaDelegPrefix+c.delegation, strconv.FormatInt(a.TurnID, 10)); err != nil {
		return err
	}
	worker, err := e.delegWorker(tx)
	if err != nil {
		return err
	}
	now := e.now()
	var msgs []store.OutMsg
	for _, step := range []struct {
		name  string
		build func(string, string, time.Time) (string, error)
	}{{"claim", d.Claim}, {"running", d.Running}} {
		text, err := step.build(c.in.Body, worker, now)
		if err != nil {
			return fmt.Errorf("delegation %s %s: %w", c.delegation, step.name, err)
		}
		m, err := e.delegRecord(text, store.OutControl, fmt.Sprintf("deleg:%s:%s", c.delegation, step.name))
		if err != nil {
			return err
		}
		msgs = append(msgs, m...)
	}
	_, err = tx.Enqueue(conv, msgs)
	e.logf("delegation %s claimed (turn %d)", c.delegation, a.TurnID)
	return err
}

// delegRecord renders a record as one plain inbox message: a code span, so
// that no Markdown rule touches the encoded payload.
func (e *Engine) delegRecord(text, kind, key string) ([]store.OutMsg, error) {
	msgs, err := e.render(context.Background(), channel.RichDoc{Markdown: "`" + text + "`"}, kind, key)
	if err == nil && len(msgs) != 1 {
		err = fmt.Errorf("delegation record does not fit one message (%d parts)", len(msgs))
	}
	return msgs, err
}

// cancelDelegations handles requester cancellations seen in an inbox window.
func (e *Engine) cancelDelegations(ctx context.Context, ids []string) {
	for _, id := range ids {
		var turnID int64
		err := e.update(func(tx *store.Tx) error {
			v, _, err := tx.Meta(metaDelegPrefix + id)
			if err != nil {
				return err
			}
			turnID, _ = strconv.ParseInt(v, 10, 64)
			if turnID == 0 {
				return nil
			}
			return tx.SetMeta(metaDelegCancelPrefix+id, "1")
		})
		if err != nil || turnID == 0 {
			continue
		}
		e.mu.Lock()
		cs := e.convs[e.inboxID]
		running := cs != nil && cs.running != nil && cs.running.turnID == turnID
		e.mu.Unlock()
		e.logf("delegation %s cancelled by the requester", id)
		if running {
			e.cancelRunning(ctx, e.inboxID)
		}
		e.wakeRunner(e.inboxID)
	}
}

// delegCancelled reports whether the requester cancelled the delegation of
// a request text.
func (e *Engine) delegCancelled(tx *store.Tx, request string) (bool, error) {
	d := e.delegations()
	if d == nil {
		return false, nil
	}
	r, ok := d.Decode(request, e.now())
	if !ok {
		return false, nil
	}
	_, ok, err := tx.Meta(metaDelegCancelPrefix + r.ID)
	return ok, err
}

// delegTurnRequest builds the Codex request of a delegated turn: a new
// thread, or the thread of the requester's earlier delegation it continues.
func (e *Engine) delegTurnRequest(tx *store.Tx, conv int64, tr store.Turn, request string) (TurnRequest, error) {
	d := e.delegations()
	if d == nil {
		return TurnRequest{}, fmt.Errorf("delegation is not enabled")
	}
	r, ok := d.Decode(request, e.now())
	if !ok {
		return TurnRequest{}, fmt.Errorf("not a delegation request")
	}
	prompt, err := d.Prompt(request)
	if err != nil {
		return TurnRequest{}, err
	}
	req := TurnRequest{ConvID: conv, Cwd: e.delegDir(), TurnKey: tr.TurnKey, Text: prompt}
	if r.Thread != "" {
		if v, ok, err := tx.Meta(metaDelegRemotePrefix + r.Thread); err != nil {
			return req, err
		} else if ok {
			req.ThreadID = v
		}
	}
	if req.ThreadID != "" {
		if err := tx.SetMeta(fmt.Sprintf("%s%d", metaDelegThreadPrefix, tr.ID), req.ThreadID); err != nil {
			return req, err
		}
	}
	return req, nil
}

func (e *Engine) delegDir() string {
	if e.cfg.DelegationDir != "" {
		return e.cfg.DelegationDir
	}
	if h, err := os.UserHomeDir(); err == nil {
		return h
	}
	return "."
}

// bindDelegThread makes a delegated turn's new thread durable before
// turn/start (the inbox conversation itself has no thread).
func (e *Engine) bindDelegThread(tr store.Turn, request, threadID string) error {
	// Known before the rollout can be discovered: the mirror ignores it.
	e.mu.Lock()
	e.delegThreads[threadID] = true
	e.mu.Unlock()
	return e.update(func(tx *store.Tx) error {
		if err := tx.SetMeta(fmt.Sprintf("%s%d", metaDelegThreadPrefix, tr.ID), threadID); err != nil {
			return err
		}
		if err := addDelegThread(tx, threadID); err != nil {
			return err
		}
		if d := e.delegations(); d != nil {
			if r, ok := d.Decode(request, e.now()); ok && r.Thread != "" {
				return tx.SetMeta(metaDelegRemotePrefix+r.Thread, threadID)
			}
		}
		return nil
	})
}

// delegThread is the Codex thread of a delegated turn ("" if none yet).
func (e *Engine) delegThread(turnID int64) string {
	var v string
	_ = e.update(func(tx *store.Tx) error {
		var err error
		v, _, err = tx.Meta(fmt.Sprintf("%s%d", metaDelegThreadPrefix, turnID))
		return err
	})
	return v
}

// finishDelegation ends a delegated turn with its result record (none when
// the requester cancelled it: the protocol ignores results after a cancel).
func (e *Engine) finishDelegation(ctx context.Context, cs *convState, tr store.Turn, out TurnOutcome, cancelled bool) {
	var state, errText string
	ok := false
	body := ""
	switch {
	case cancelled:
		state, errText = store.TurnCancelled, "cancelled by the requester"
	case out.Status == StatusCompleted:
		state, ok, body = store.TurnCompleted, true, strings.TrimSpace(out.Final)
	case out.NotStarted:
		state, errText = store.TurnNotStarted, out.Error
		body = "The delegated task did not start: " + userError(out.Error)
	case out.Status == StatusInterrupted:
		state, errText, body = store.TurnInterrupted, out.Error, textDelegInterrupted
	default:
		state, errText = store.TurnFailed, out.Error
		body = "The delegated task failed: " + userError(out.Error)
	}
	if !e.commitFinish(ctx, tr, func(tx *store.Tx) error {
		in, _, err := tx.Inbound(tr.InboundID)
		if err != nil {
			return err
		}
		if !cancelled {
			if cancelled, err = e.delegCancelled(tx, in.Body); err != nil {
				return err
			}
			if cancelled {
				state, errText = store.TurnCancelled, "cancelled by the requester"
			}
		}
		var msgs []store.OutMsg
		if d := e.delegations(); d != nil && !cancelled {
			worker, err := e.delegWorker(tx)
			if err != nil {
				return err
			}
			r, _ := d.Decode(in.Body, e.now())
			text, err := d.Result(in.Body, worker, ok, body, e.now())
			if err != nil {
				return err
			}
			if msgs, err = e.delegRecord(text, store.OutFinal, "deleg:"+r.ID+":result"); err != nil {
				return err
			}
		}
		if err := tx.FinishTurn(tr.ID, state, errText, msgs); err != nil {
			return err
		}
		return recordUsage(tx, cs.id, state, out.Usage)
	}) {
		return
	}
	e.logf("delegated turn %d finished: %s", tr.ID, state)
	if state == store.TurnCompleted {
		e.count(func(s *engineStats) { s.completed++ })
	}
	e.mu.Lock()
	cs.capWake = time.Time{}
	e.mu.Unlock()
}

// turnThread is the Codex thread a turn runs on: a delegated turn's own
// thread, else the conversation's.
func (e *Engine) turnThread(cs *convState, tr store.Turn) string {
	if cs.id == e.inboxID {
		return e.delegThread(tr.ID)
	}
	return e.threadOf(cs.id)
}

// startDelegation prepares a delegated turn in the runner's transaction. A
// request the requester cancelled ends without running (got=false).
func (e *Engine) startDelegation(tx *store.Tx, conv int64, tr store.Turn) (TurnRequest, string, bool, error) {
	in, _, err := tx.Inbound(tr.InboundID)
	if err != nil {
		return TurnRequest{}, "", false, err
	}
	cancelled, err := e.delegCancelled(tx, in.Body)
	if err != nil {
		return TurnRequest{}, "", false, err
	}
	if cancelled {
		return TurnRequest{}, "", false, tx.FinishTurn(tr.ID, store.TurnCancelled, "cancelled by the requester", nil)
	}
	req, err := e.delegTurnRequest(tx, conv, tr, in.Body)
	if err != nil {
		e.logf("delegated turn %d cannot run: %v", tr.ID, err)
		return TurnRequest{}, "", false, tx.FinishTurn(tr.ID, store.TurnFailed, err.Error(), nil)
	}
	return req, in.Body, true, tx.MarkStarting(tr.ID, req.ThreadID)
}
