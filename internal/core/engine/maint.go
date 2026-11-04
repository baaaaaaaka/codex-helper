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
	err := e.update(func(tx *store.Tx) error {
		for _, r := range rows {
			label := labelHelper
			if r.Kind == store.OutFinal || r.Kind == store.OutProgress || r.Kind == store.OutMirror {
				label = labelCodex
			}
			msgs, err := e.render(ctx, channel.RichDoc{Label: label, Markdown: r.Body, ReplyTo: r.ReplyTo}, r.Kind, r.DeliveryKey+":r")
			if err != nil {
				return err
			}
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
				e.logf("heartbeat: %v", err)
				if isFence(err) {
					e.fail(err)
					return
				}
			}
		case <-mt.C:
			e.ag.Maintain()
			e.remindQueued()
			e.retryCreating(ctx)
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
		if cs.kind == store.KindWork && cs.state == store.ConvActive && cs.running == nil && id != e.inboxID &&
			now.Sub(cs.lastActivity) > e.t.ParkAfter {
			ids = append(ids, id)
		}
	}
	e.mu.Unlock()
	for _, id := range ids {
		err := e.update(func(tx *store.Tx) error {
			open, err := tx.OpenTurnsOf(id)
			if err != nil || len(open) > 0 {
				return err
			}
			pending, err := tx.PendingOutbox(id)
			if err != nil || len(pending) > 0 {
				return err
			}
			if err := tx.Park(id); err != nil {
				return err
			}
			_, err = e.refresh(tx, id)
			return err
		})
		if err != nil {
			e.logf("park %d: %v", id, err)
		}
	}
}
