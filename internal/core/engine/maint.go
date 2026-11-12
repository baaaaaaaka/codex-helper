package engine

import (
	"context"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// gate returns until when an account-wide gate is closed.
func (e *Engine) gate(key string) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.gates[key]
}

// setGate closes an account-wide gate (persisted so a restart honours it).
func (e *Engine) setGate(key string, until time.Time, reason string) {
	e.mu.Lock()
	if e.gates == nil {
		e.gates = map[string]time.Time{}
	}
	if !until.After(e.gates[key]) {
		e.mu.Unlock()
		return
	}
	e.gates[key] = until
	e.mu.Unlock()
	if err := e.update(func(tx *store.Tx) error { return tx.SetGate(key, ms(until), reason) }); err != nil {
		e.logf("set gate %s: %v", key, err)
	}
}

func (e *Engine) loadGates() error {
	gs, err := e.st.Gates(ms(e.now()))
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.gates == nil {
		e.gates = map[string]time.Time{}
	}
	for _, g := range gs {
		e.gates[g.Key] = fromMS(g.Until)
		// A per-chat gate (migrated Retry-After) delays that chat's reads
		// and sends.
		for _, cs := range e.convs {
			if cs.chat != "" && g.Key == store.ChatGate(e.cfg.ChannelName, cs.chat) {
				until := fromMS(g.Until)
				if until.After(cs.nextRead) {
					cs.nextRead = until
				}
				if last := until.Add(-e.t.ChatRate); last.After(cs.lastSend) {
					cs.lastSend = last
				}
			}
		}
	}
	return nil
}

// rehashLegacy replaces imported legacy body hashes with the adapter's
// hash once, at the first start after a migration.
// renderLegacy renders the unsent rows imported from the legacy helper,
// whose body is still the legacy Markdown text, into channel payloads
// before anything is sent. Rendering is local (no mention, no formula
// service). A row that does not fit one part keeps the first part and the
// rest is queued after it.
func (e *Engine) renderLegacy(ctx context.Context) error {
	var rows []store.Outbox
	if err := e.update(func(tx *store.Tx) error {
		var err error
		rows, err = tx.LegacyUnsentOutbox()
		return err
	}); err != nil || len(rows) == 0 {
		return err
	}
	// Render first, outside any transaction: math renders through a local
	// renderer, and no write transaction may span I/O.
	rendered := make([][]store.OutMsg, len(rows))
	for i, r := range rows {
		doc := channel.RichDoc{Label: labelHelper, Markdown: r.Body}
		switch r.Kind {
		case store.OutFinal:
			doc = answerDoc(r.Body)
		case store.OutProgress:
			doc = statusDoc(r.Body)
		case store.OutMirror:
			// The legacy kind (prompt or answer) is not kept.
			doc = channel.RichDoc{Label: labelAnswer, Markdown: r.Body, Math: true}
		}
		doc.ReplyTo = r.ReplyTo
		msgs, err := e.render(ctx, doc, r.Kind, r.DeliveryKey+":r")
		if err != nil {
			return err
		}
		rendered[i] = msgs
	}
	err := e.update(func(tx *store.Tx) error {
		for i, r := range rows {
			msgs := rendered[i]
			if err := tx.SetOutboxRendered(r.ID, msgs[0].Body, msgs[0].BodyHash); err != nil {
				return err
			}
			if len(msgs) > 1 {
				if _, err := tx.Enqueue(r.ConvID, msgs[1:]); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err == nil {
		e.logf("rendered %d imported unsent messages", len(rows))
	}
	return err
}

func (e *Engine) rehashLegacy() error {
	h, ok := e.cfg.Channel.(channel.BodyHasher)
	if !ok {
		return nil
	}
	if v, _, err := e.st.Meta(metaRehashed); err != nil || v == "1" {
		return err
	}
	var n int
	err := e.update(func(tx *store.Tx) error {
		var err error
		n, err = tx.RehashLegacy(func(body string) []byte { return hashBytes(h.BodyHash(body)) })
		if err != nil {
			return err
		}
		return tx.SetMeta(metaRehashed, "1")
	})
	if err == nil && n > 0 {
		e.logf("re-hashed %d imported messages", n)
	}
	return err
}

// maintenanceLoop: owner heartbeat, idle app-server reaping, chat creation
// retries and an occasional incremental vacuum. Nothing is deleted.
func (e *Engine) maintenanceLoop(ctx context.Context) {
	hb := time.NewTicker(e.t.Heartbeat)
	defer hb.Stop()
	mt := time.NewTicker(e.t.Maintenance)
	defer mt.Stop()
	lastVacuum := e.now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-hb.C:
			if err := e.st.Heartbeat(); err != nil {
				if err == store.ErrDisabled && e.stopping.Load() {
					continue // `shutdown` stops this process once its reply is out
				}
				e.logf("heartbeat: %v", err)
				if isFence(err) {
					e.fail(err)
					return
				}
			}
		case <-mt.C:
			e.maintainQuestions(ctx)
			e.ag.Maintain()
			e.remindQueued()
			e.retryCreating()
			e.announcePass()
			e.parkIdle()
			if e.now().Sub(lastVacuum) > time.Hour {
				lastVacuum = e.now()
				// Bounded: a large free list is returned over many hours.
				if err := e.st.VacuumStep(vacuumPages); err != nil {
					e.logf("vacuum: %v", err)
				}
			}
		}
	}
}

// vacuumPages bounds one incremental vacuum (1 MiB at 4 KiB pages).
const vacuumPages = 256

// isFence: errors that end this process's ownership (fenced by a newer
// owner, never acquired, or the kill-switch file appeared).
func isFence(err error) bool {
	return err == store.ErrFenced || err == store.ErrNotAcquired || err == store.ErrDisabled
}

// parkIdle parks work chats that have been cold for ParkAfter: no reads
// until a local rollout change or `r <label>` wakes them.
func (e *Engine) parkIdle() {
	if e.t.ParkAfter <= 0 {
		return
	}
	now := e.now()
	e.mu.Lock()
	var ids []int64
	for id, cs := range e.convs {
		if cs.kind == store.KindWork && cs.state == store.ConvActive && !cs.autoParkOff && cs.running == nil && id != e.inboxID &&
			now.Sub(cs.lastActivity) > e.t.ParkAfter {
			ids = append(ids, id)
		}
	}
	e.mu.Unlock()
	for _, id := range ids {
		var parkedAt int64
		err := e.update(func(tx *store.Tx) error {
			parkedAt = 0
			// The candidate may have changed since the initial scan. Policy is
			// checked again by AutoPark in this same write transaction.
			e.mu.Lock()
			cs := e.convs[id]
			eligible := cs != nil && cs.state == store.ConvActive && cs.running == nil && !cs.reading && now.Sub(cs.lastActivity) > e.t.ParkAfter
			e.mu.Unlock()
			if !eligible {
				return nil
			}
			open, err := tx.OpenTurnsOf(id)
			if err != nil || len(open) > 0 {
				return err
			}
			pending, err := tx.ActiveOutbox(id)
			if err != nil || len(pending) > 0 {
				return err
			}
			did, err := tx.AutoPark(id)
			if err != nil || !did {
				return err
			}
			c, err := e.refresh(tx, id)
			if err != nil {
				return err
			}
			// F-WRK-5: the freeze notice of this park (annotations.go).
			parkedAt = c.ParkedAt
			return owesFreeze(tx, id, parkedAt)
		})
		if err != nil {
			e.logf("park %d: %v", id, err)
		} else if parkedAt != 0 {
			e.queueFreeze(id, parkedAt)
		}
	}
}
