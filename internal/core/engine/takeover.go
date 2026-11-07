package engine

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// metaOwnerHost is the host of the last helper that served the store.
const metaOwnerHost = "owner_host"

// textTakeoverLead starts the takeover notice; another copy's notice is
// recognised by it (supersededBy).
const textTakeoverLead = "🔁 The helper now runs on "

// announceTakeover tells the owner, once, that the helper now runs on
// another host of a shared home: the notice is enqueued in the same
// transaction that records the new host, so a crash cannot lose or repeat
// it. The first run records the host silently, and a restart on the same
// host writes nothing.
func (e *Engine) announceTakeover() error {
	host := strings.TrimSpace(e.cfg.Host)
	if host == "" {
		return nil
	}
	var from string
	err := e.update(func(tx *store.Tx) error {
		prev, _, err := tx.Meta(metaOwnerHost)
		if err != nil {
			return err
		}
		prev, from = strings.TrimSpace(prev), ""
		if strings.EqualFold(prev, host) {
			return nil
		}
		if prev != "" {
			ended := "stopped"
			if e.cfg.TakeoverWhy == "stale" {
				ended = "stopped answering"
			}
			text := fmt.Sprintf("%s%s: it took over from %s, whose helper %s.", textTakeoverLead, host, prev, ended)
			base := fmt.Sprintf("takeover:%d:%s", e.st.Generation(), host)
			if err := e.enqueueNotice(tx, e.controlID, base, text); err != nil {
				return err
			}
			from = prev
		}
		return tx.SetMeta(metaOwnerHost, host)
	})
	if err == nil {
		e.tookOver = from
	}
	return err
}

// Move guard (a home copied to another machine while its helper runs).
// Every copy shares the marker prefix, so a post with our prefix whose
// token is in none of our outbox rows was made by another copy of this
// store: a sibling post (classify).
//
//  1. The older copy stands down: a sibling's takeover notice in the
//     control chat, admitted for the first time, means another copy took
//     over. This helper records a stop (the kill-switch file, so the service
//     does not restart it until `cxp teams start`) and stops, posting
//     nothing.
//  2. The new copy does not redo the old copy's work: in the first read of
//     each chat after taking over by a copy, a request followed by a sibling
//     post was seen by the other copy. It is closed without running, as is
//     a turn the copied store left open in a chat with a sibling post, and
//     one notice in that chat lists what was skipped. Turns of a chat, and
//     the reconciling of the ones left open, wait for that read.

// supersededBy returns the host named by a sibling's takeover notice ("" for
// any other message, or a notice naming this host).
func (e *Engine) supersededBy(m channel.InboundMsg) string {
	_, rest, ok := strings.Cut(m.Text, textTakeoverLead)
	host, _, ok2 := strings.Cut(rest, ": it took over from ")
	host = strings.TrimSpace(host)
	if !ok || !ok2 || host == "" || strings.EqualFold(host, strings.TrimSpace(e.cfg.Host)) {
		return ""
	}
	return host
}

// standDown stops this helper for good after another copy took over (rule
// 1). A fenced store is a takeover of this very store (a shared home): the
// lease stops the helper and no stop is recorded, since the file would
// stop the new owner too.
func (e *Engine) standDown(host string) {
	if err := e.st.Fence(); err != nil {
		e.fail(err)
		return
	}
	rec := store.StopRecord{Source: store.StopBySuperseded, Host: host, At: e.now()}
	if _, err := store.WriteStop(filepath.Dir(e.st.Path()), rec); err != nil {
		e.logf("superseded by %s; the stop could not be recorded: %v", host, err)
	}
	e.logf("superseded: the helper now runs on %s (a copy of this state)", host)
	e.stopping.Store(true) // no turn starts: the control chat's first read is this one
	e.fail(store.ErrDisabled)
}

// readyLocked reports whether a turn of cs may start: not before this
// process read the control chat once, where another copy's takeover notice
// stops an old copy that starts again; after a takeover, not before it read
// cs once, where rule 2 closes what the other copy took (caller holds e.mu).
func (e *Engine) readyLocked(cs *convState) bool {
	return e.controlReadLocked() && (e.tookOver == "" || readOnceLocked(cs))
}

// controlReadLocked reports whether this process has read its control
// chat once, or has none to read. Until then it starts no turn and sends
// nothing (sender.go): a copy whose state was copied elsewhere learns of
// the sibling's takeover notice only from that read, and stands down
// having posted nothing (rule 1), not after an ACK for a request the
// other copy owns (caller holds e.mu).
func (e *Engine) controlReadLocked() bool {
	return readOnceLocked(e.convs[e.controlID])
}

// readOnceLocked reports whether cs was read once by this process, or is
// not in a state that is read (caller holds e.mu).
func readOnceLocked(c *convState) bool {
	return c == nil || c.readOnce || !readStateLocked(c)
}

// closeCopied closes a request the other copy already took (rule 2).
func (e *Engine) closeCopied(tx *store.Tx, a store.Admitted) error {
	if a.TurnID != 0 {
		return tx.FinishTurn(a.TurnID, store.TurnCancelled, "taken by the helper on "+e.tookOver, nil)
	}
	return tx.SetInbound(a.InboundID, store.InDone, 0)
}

// textCopySkipped lists the requests of one chat the other copy took.
func textCopySkipped(from string, bodies []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "⏭️ Not run here: the helper on %s already took these requests after its state was copied to this machine. If one of them got no answer, send it again.\n", from)
	for _, p := range bodies {
		p = strings.TrimPrefix(strings.TrimSpace(p), labelUser+":") // the other copy labelled it
		b.WriteString("\n- " + firstNonEmpty(promptLine(p), "(no text)"))
	}
	return b.String()
}
