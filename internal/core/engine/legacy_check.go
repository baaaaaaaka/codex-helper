package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Owner ruling 2026-11-05 (Q1): an answer of the previous helper whose send
// outcome was unknown is checked once against its chat's history at first
// start, and resent only when the history does not have it. The converter
// lists such answers in meta (store.LegacyFinalCheck); this loop decides
// each in the background. The resend and the removal from the list are one
// transaction, so a restart never checks or resends it again; an answer
// found stays listed with the id of its post (Found). An unfinished read
// (stopped, failed) is read again from the start.
//
// The read is paced: one read-pacer token per page, the account read gate
// honoured, one page per Hot interval, and it starts after ResolveDelay, so
// the first polls of the chats come first.
//
// The old helper posted such an answer without a marker when its outbox id
// had characters its marker did not allow ('@': every id of a real store),
// and an answer that landed while it stopped is newer than its stream
// cursor. The receiver therefore recognises a post of a listed answer as
// the helper's own (legacyOwn), from the first read on, and records it
// found: it is never taken for the owner's prompt or command.

// Bounds of a history check.
const (
	// legacyCheckSince reads the history from this long before the answer
	// was created: a message is never modified before it is created, so
	// this covers every message that can be the answer.
	legacyCheckSince = 24 * time.Hour
	// legacyCheckPages bounds one check's read (50 messages a page). A chat
	// whose history since the answer is longer is not decided: the answer is
	// not resent (no duplicate) and the owner is told.
	legacyCheckPages = 2000
	// legacyCheckResidue bounds the text a message may have besides the
	// answer's (labels, the completion footer, the @mention), in letters
	// and digits.
	legacyCheckResidue = 400
)

// errHistoryTooLong ends a check whose history exceeds legacyCheckPages.
var errHistoryTooLong = errors.New("history too long to check")

// legacyAnswer is a listed answer and its visible text (want: letters and
// digits, "" when it could not be rendered).
type legacyAnswer struct {
	c    store.LegacyFinalCheck
	want string
}

func readLegacyChecks(tx *store.Tx) ([]store.LegacyFinalCheck, error) {
	v, ok, err := tx.Meta(store.MetaLegacyFinalChecks)
	if err != nil || !ok || v == "" {
		return nil, err
	}
	var list []store.LegacyFinalCheck
	if err := json.Unmarshal([]byte(v), &list); err != nil {
		return nil, nil // unreadable: nothing can be done with it
	}
	return list, nil
}

func writeLegacyChecks(tx *store.Tx, list []store.LegacyFinalCheck) error {
	if len(list) == 0 {
		return tx.SetMeta(store.MetaLegacyFinalChecks, "")
	}
	b, err := json.Marshal(list)
	if err != nil {
		return err
	}
	return tx.SetMeta(store.MetaLegacyFinalChecks, string(b))
}

// loadLegacyAnswers reads the list before the loops start.
func (e *Engine) loadLegacyAnswers(ctx context.Context) {
	var list []store.LegacyFinalCheck
	if err := e.update(func(tx *store.Tx) error {
		var err error
		list, err = readLegacyChecks(tx)
		return err
	}); err != nil {
		e.logf("answer history check: %v", err)
		return
	}
	answers := make([]legacyAnswer, 0, len(list))
	for _, c := range list {
		a := legacyAnswer{c: c}
		if c.Found == "" {
			want, err := e.legacyText(ctx, c.Body)
			if err != nil {
				e.logf("answer history check of %s: %v", c.Key, err)
			}
			a.want = want
		}
		answers = append(answers, a)
	}
	e.legacyMu.Lock()
	e.legacy = answers
	e.legacyMu.Unlock()
}

// legacyText is the visible text a post of body carries without labels
// (the previous helper's may differ), as letters and digits.
func (e *Engine) legacyText(ctx context.Context, body string) (string, error) {
	parts, err := e.ch.Render(ctx, channel.RichDoc{Markdown: body, Math: true})
	if err != nil {
		return "", err
	}
	var text strings.Builder
	for _, p := range parts {
		if t, ok := e.cfg.Channel.(channel.PartTexter); ok {
			text.WriteString(t.PartText(p.Body))
		} else {
			text.WriteString(p.Body)
		}
	}
	return letters(text.String()), nil
}

// legacyOwn reports whether m, an owner-account message of conv without a
// marker that is none of this store's posts, is the post of a listed
// answer; a pending answer is then decided as found, in tx.
func (e *Engine) legacyOwn(tx *store.Tx, conv int64, m channel.InboundMsg) (bool, error) {
	e.legacyMu.Lock()
	defer e.legacyMu.Unlock()
	for i := range e.legacy {
		a := &e.legacy[i]
		if a.c.ConvID != conv {
			continue
		}
		if a.c.Found != "" {
			if a.c.Found == m.ID {
				return true, nil
			}
			continue
		}
		ok, err := legacyMatch(tx, a.c, a.want, m)
		if err != nil || !ok {
			if err != nil {
				return false, err
			}
			continue
		}
		list, err := readLegacyChecks(tx)
		if err != nil {
			return false, err
		}
		for j := range list {
			if list[j].Key == a.c.Key && list[j].Found == "" {
				list[j].Found = m.ID
				if err := writeLegacyChecks(tx, list); err != nil {
					return false, err
				}
				e.logf("answer of the previous helper from %s (%s): found in the chat's history (%s): not sent",
					time.UnixMilli(a.c.Created).UTC().Format(time.RFC3339), a.c.Key, m.ID)
			}
		}
		a.c.Found = m.ID
		return true, nil
	}
	return false, nil
}

// nextLegacy is the first listed answer not yet decided.
func (e *Engine) nextLegacy() (legacyAnswer, bool) {
	e.legacyMu.Lock()
	defer e.legacyMu.Unlock()
	for _, a := range e.legacy {
		if a.c.Found == "" {
			return a, true
		}
	}
	return legacyAnswer{}, false
}

// legacyCheckLoop decides the listed answers, one at a time.
func (e *Engine) legacyCheckLoop(ctx context.Context) {
	if _, ok := e.nextLegacy(); !ok || !sleepCtx(ctx, e.t.ResolveDelay) {
		return
	}
	for {
		a, ok := e.nextLegacy()
		if !ok {
			return
		}
		for failures := 0; ; failures++ {
			err := e.checkLegacyFinal(ctx, a)
			if err == nil || ctx.Err() != nil {
				break
			}
			back := min(e.t.Hot<<min(failures+1, 10), e.t.MaxBackoff)
			e.logf("answer history check of %s: %v; retrying in %s", a.c.Key, err, back)
			if !sleepCtx(ctx, back) {
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// checkLegacyFinal reads the history of a's chat and decides a: found
// (nothing is sent) or absent (resent). It returns an error when the read
// did not finish; a is then still pending.
func (e *Engine) checkLegacyFinal(ctx context.Context, a legacyAnswer) error {
	c := a.c
	e.mu.Lock()
	cs := e.convs[c.ConvID]
	var chat, state string
	if cs != nil {
		chat, state = cs.chat, cs.state
	}
	e.mu.Unlock()
	if cs == nil || chat == "" || state == store.ConvClosed {
		return e.decideLegacyFinal(c, nil, "", "its chat is closed or gone: not checked, not sent")
	}
	// The answer as it is sent now (if absent).
	doc := answerDoc(c.Body)
	doc.ReplyTo = c.ReplyTo
	resend, err := e.render(ctx, doc, store.OutFinal, "legacy-resend:"+c.Key)
	if err != nil {
		return err
	}
	want := a.want
	if want == "" {
		if want, err = e.legacyText(ctx, c.Body); err != nil {
			return err
		}
	}

	created := time.UnixMilli(c.Created)
	since := created.Add(-legacyCheckSince)
	found, err := e.readHistory(ctx, chat, since, func(tx *store.Tx, msgs []channel.InboundMsg) (string, error) {
		for _, m := range msgs {
			ok, err := legacyMatch(tx, c, want, m)
			if err != nil || ok {
				return m.ID, err
			}
		}
		return "", nil
	})
	switch {
	case errors.Is(err, errHistoryTooLong):
		return e.decideLegacyFinal(c, nil, "", fmt.Sprintf("the chat's history since %s is longer than %d pages: not sent", since.UTC().Format(time.DateOnly), legacyCheckPages))
	case err != nil:
		return err
	case found != "":
		return e.decideLegacyFinal(c, nil, found, "found in the chat's history ("+found+"): not sent")
	}
	return e.decideLegacyFinal(c, resend, "", "absent from the chat's history: sent now")
}

// decideLegacyFinal records the decision of c in one transaction: found
// (the entry stays, with the id of its post) or not (the entry is removed
// and resend, if any, enqueued). An entry the receiver decided meanwhile
// is left as it is.
func (e *Engine) decideLegacyFinal(c store.LegacyFinalCheck, resend []store.OutMsg, found, outcome string) error {
	decided, listed := false, true
	err := e.update(func(tx *store.Tx) error {
		list, err := readLegacyChecks(tx)
		if err != nil {
			return err
		}
		i := slices.IndexFunc(list, func(p store.LegacyFinalCheck) bool { return p.Key == c.Key })
		if i < 0 || list[i].Found != "" {
			decided, listed = true, i >= 0
			if listed {
				found = list[i].Found
			}
			return nil
		}
		if found != "" {
			list[i].Found = found
		} else {
			listed = false
			list = slices.Delete(list, i, i+1)
			if len(resend) > 0 {
				if _, err := tx.Enqueue(c.ConvID, resend); err != nil {
					return err
				}
			}
		}
		return writeLegacyChecks(tx, list)
	})
	if err != nil {
		return err
	}
	e.legacyMu.Lock()
	if i := slices.IndexFunc(e.legacy, func(a legacyAnswer) bool { return a.c.Key == c.Key }); i >= 0 {
		if listed {
			e.legacy[i].c.Found = found
		} else {
			e.legacy = slices.Delete(e.legacy, i, i+1)
		}
	}
	e.legacyMu.Unlock()
	if decided {
		return nil
	}
	e.logf("answer of the previous helper from %s (%s): %s", time.UnixMilli(c.Created).UTC().Format(time.RFC3339), c.Key, outcome)
	if len(resend) > 0 {
		e.wakeSender()
	}
	return nil
}

// readHistory pages through the messages of chat modified after since,
// paced, until match returns a message id or the history ends. The
// channel's ReadBack is the single-request fallback when it cannot page.
func (e *Engine) readHistory(ctx context.Context, chat string, since time.Time,
	match func(*store.Tx, []channel.InboundMsg) (string, error)) (string, error) {
	pager, paged := e.cfg.Channel.(channel.HistoryPager)
	pos := ""
	for pages := 0; ; {
		if pages >= legacyCheckPages {
			return "", errHistoryTooLong
		}
		if until := e.gate(e.gRead); e.now().Before(until) {
			if !sleepCtx(ctx, until.Sub(e.now())) {
				return "", ctx.Err()
			}
			continue
		}
		if e.readWaiters.Load() > 0 {
			// A chat edit waits for a read token: this background read
			// leaves it the next one (edits.go waitRead).
			if !sleepCtx(ctx, e.t.Tick) {
				return "", ctx.Err()
			}
			continue
		}
		if ok, wait := e.readTokens.take(); !ok {
			if !sleepCtx(ctx, max(wait, e.t.Tick)) {
				return "", ctx.Err()
			}
			continue
		}
		start := e.now()
		var (
			msgs []channel.InboundMsg
			next string
			err  error
		)
		rctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		if paged {
			msgs, next, err = pager.HistoryPage(rctx, chat, since, pos)
		} else {
			msgs, err = e.ch.ReadBack(rctx, chat, since)
		}
		cancel()
		e.count(func(s *engineStats) { s.reads++ })
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if th, ok := channel.AsThrottled(err); ok {
			until := e.now().Add(e.retryAfter(th))
			if th.Scope != channel.ScopeChat {
				e.readThrottled(start, until, "history check throttled")
			}
			if !sleepCtx(ctx, until.Sub(e.now())) {
				return "", ctx.Err()
			}
			continue
		}
		if err != nil {
			return "", err
		}
		pages++
		var found string
		if err := e.update(func(tx *store.Tx) error {
			var err error
			found, err = match(tx, msgs)
			return err
		}); err != nil || found != "" {
			return found, err
		}
		if !paged || next == "" {
			return "", nil
		}
		pos = next
		if !sleepCtx(ctx, e.t.Hot) {
			return "", ctx.Err()
		}
	}
}

// legacyMatch reports whether m is the post of c: by the Teams message id
// the old helper recorded, by its hidden marker (the old outbox id), or by
// its text: an owner-account message whose visible text contains the
// answer's (want: letters and digits) with little else (labels, footer,
// @mention).
//
// Owner ruling (canary 2026-11-06): an identical text already in the chat
// counts as present, whenever it was posted and whichever row of the old
// store it was recorded as (another answer's post, another old marker).
// The real control chat held 31 identical copies of the 2026-09-23 answer,
// each recorded as another row; resending it made a 32nd.
func legacyMatch(_ *store.Tx, c store.LegacyFinalCheck, want string, m channel.InboundMsg) (bool, error) {
	switch {
	case m.Kind != channel.KindMessage:
		return false, nil
	case c.ExtMsgID != "" && m.ID == c.ExtMsgID:
		return true, nil
	case m.Marker != "" && m.Marker == c.LegacyID:
		return true, nil
	case !m.ByOwner || want == "":
		return false, nil
	}
	got := letters(m.Text)
	return strings.Contains(got, want) && len([]rune(got))-len([]rune(want)) <= legacyCheckResidue, nil
}

// letters keeps the lower-cased letters and digits of s: the visible text
// of an answer compares the same whatever renderer posted it.
func letters(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// sleepCtx waits d; false when ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
