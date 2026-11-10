package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

// The resume check of converted rollouts (store.HistoryCheck; canary rerun
// brief 2026-11-06). The converter resumes a rollout at its delivery proof
// (STORE-DESIGN §9.4: the previous helper's positions are no proof, a
// watcher scanned without posting), so a rollout with no proof behind its
// old furthest position is caught up from there, and s001 re-posted at
// least 267 records its chat already had. The converter lists such
// rollouts; at first start, before a listed rollout's catch-up posts
// anything, its chat's history is read once (paced like the Q1 answer
// check, legacy_check.go) and compared with the rollout's prompts and
// finals:
//
//   - when the records just before the old position are in the chat, the
//     catch-up resumes there;
//   - else it resumes right after the newest record found in the chat;
//   - with nothing found (or no history to read) it starts at the converted
//     position: nothing is lost.
//
// The comparison uses the visible text (letters and digits, as Q1) of each
// record and of each message of the signed-in account that carries the
// mirror label of the same kind (prompt: "User", final: "Codex answer"),
// labels, "[part i/n]" and the completion footer removed. v0.1.20 and
// rc.59 posted the same records with different labels, footers, splits and
// truncation, so a record is found when the texts are equal, share their
// first historyPrefix letters, or one contains the other (at least
// historyContain letters), or the post starts with the record and carries
// little else (another footer). Short records are weak evidence: the newest
// record found with at least historyAnchor letters is the anchor, and a
// shorter record after it counts only when an equal text was posted after
// the anchor's.
//
// The decision is recorded in the list (Done, Resume) in one transaction;
// a restart does not read again. While a rollout's check is pending its
// catch-up waits; other rollouts and chats proceed.

const (
	historyPrefix  = 60 // letters two texts share at the start
	historyContain = 40 // letters of the contained text
	historyAnchor  = 12 // letters of a record that can anchor the resume point
	// historyResidue bounds what a post of a shorter record may carry
	// after its text (a footer, an @mention), in bytes of letters.
	historyResidue = 60
	// historySkew reads the history from this long before the oldest
	// compared record: a record is posted after it was written.
	historySkew = time.Hour
)

// mirror labels as plain text: the kind of the record a post carries.
var (
	reHistoryUser   = regexp.MustCompile(`^\W*User(?:\s*\[part \d+/\d+\])?\s*:`)
	reHistoryAnswer = regexp.MustCompile(`^\W*(?:Codex answer|Codex final)(?:\s*\[part \d+/\d+\])?\s*:`)
	reHistoryFooter = regexp.MustCompile(`\W*Helper\s*:\W*Codex finished responding\W*$`)
	// reHistoryNote: a closing line before the footer (render.go
	// infoNotes): the active goal, the model line.
	reHistoryNote = regexp.MustCompile(`^\W*(?:Active goal\s*:|Helper\s*:\s*model - .*, effort - .*, tier - )`)
)

// stripHistoryNotes removes the closing note lines at the end of text.
func stripHistoryNotes(text string) string {
	for {
		text = strings.TrimRight(text, " \t\r\n")
		i := strings.LastIndexByte(text, '\n')
		if i < 0 || !reHistoryNote.MatchString(text[i+1:]) {
			return text
		}
		text = text[:i]
	}
}

// historyRec is a compared record: its kind, visible text and end offset.
type historyRec struct {
	prompt bool
	text   string // letters and digits
	end    int64
	at     time.Time
}

// historyMsg is a labelled post of the signed-in account.
type historyMsg struct {
	prompt bool
	text   string // letters and digits, labels and footer removed
	at     time.Time
}

// historyMessage returns the labelled post m carries, if any.
func historyMessage(m channel.InboundMsg) (historyMsg, bool) {
	if m.Kind != channel.KindMessage || !m.ByOwner {
		return historyMsg{}, false
	}
	text := strings.TrimSpace(m.Text)
	var prompt bool
	switch {
	case reHistoryUser.MatchString(text):
		prompt, text = true, reHistoryUser.ReplaceAllString(text, "")
	case reHistoryAnswer.MatchString(text):
		text = stripHistoryNotes(reHistoryFooter.ReplaceAllString(reHistoryAnswer.ReplaceAllString(text, ""), ""))
	default:
		return historyMsg{}, false
	}
	t := letters(text)
	if t == "" {
		return historyMsg{}, false
	}
	return historyMsg{prompt: prompt, text: t, at: m.Created}, true
}

// prefixRunes is the first n runes of s ("" when s is shorter).
func prefixRunes(s string, n int) string {
	i := 0
	for k := range s {
		if i == n {
			return s[:k]
		}
		i++
	}
	if i == n {
		return s
	}
	return ""
}

// windowHashes calls f with the hash of every window of n runes of s and
// the byte offset where it starts (polynomial rolling hash).
func windowHashes(s string, n int, f func(h uint64, at int)) {
	const base = 1000003
	pow := uint64(1)
	for range n - 1 {
		pow *= base
	}
	rs := []rune(s)
	if len(rs) < n {
		return
	}
	offs := make([]int, 0, len(rs))
	for k := range s {
		offs = append(offs, k)
	}
	var h uint64
	for i, r := range rs {
		if i >= n {
			h -= uint64(rs[i-n]) * pow
		}
		h = h*base + uint64(r)
		if i >= n-1 {
			f(h, offs[i-n+1])
		}
	}
}

func hashRunes(s string) uint64 {
	var h uint64
	windowHashes(s, len([]rune(s)), func(x uint64, _ int) { h = x })
	return h
}

// historyMatches returns, per record, the newest post found of it (zero:
// none). A post is a record's when they are of the same kind and their
// texts are equal, share their first historyPrefix letters, or one
// contains the other with at least historyContain letters, or the post
// starts with a record of historyAnchor letters or more with at most
// historyResidue after it. Each direction
// of containment is one pass of rolling hashes over one side, looked up
// among the first historyContain letters of the other side, so the cost is
// linear in the text read.
func historyMatches(recs []historyRec, msgs []historyMsg) []time.Time {
	type key struct {
		prompt bool
		s      string
	}
	type hkey struct {
		prompt bool
		h      uint64
	}
	match := make([]time.Time, len(recs))
	seen := func(i int, at time.Time) {
		if at.After(match[i]) {
			match[i] = at
		}
	}
	exact, prefix, short := map[key][]int{}, map[key][]int{}, map[key][]int{}
	recHead := map[hkey][]int{} // first historyContain letters of each record
	for i, r := range recs {
		exact[key{r.prompt, r.text}] = append(exact[key{r.prompt, r.text}], i)
		if p := prefixRunes(r.text, historyPrefix); p != "" {
			prefix[key{r.prompt, p}] = append(prefix[key{r.prompt, p}], i)
		}
		if p := prefixRunes(r.text, historyContain); p != "" {
			k := hkey{r.prompt, hashRunes(p)}
			recHead[k] = append(recHead[k], i)
		} else if p := prefixRunes(r.text, historyAnchor); p != "" {
			short[key{r.prompt, p}] = append(short[key{r.prompt, p}], i)
		}
	}
	msgHead := map[hkey][]int{} // first historyContain letters of each post
	for j, m := range msgs {
		for _, i := range exact[key{m.prompt, m.text}] {
			seen(i, m.at)
		}
		if p := prefixRunes(m.text, historyPrefix); p != "" {
			for _, i := range prefix[key{m.prompt, p}] {
				seen(i, m.at)
			}
		}
		if p := prefixRunes(m.text, historyAnchor); p != "" {
			// A shorter record the post starts with, little else after
			// it (another footer, an @mention).
			for _, i := range short[key{m.prompt, p}] {
				if strings.HasPrefix(m.text, recs[i].text) && len(m.text)-len(recs[i].text) <= historyResidue {
					seen(i, m.at)
				}
			}
		}
		if p := prefixRunes(m.text, historyContain); p != "" {
			k := hkey{m.prompt, hashRunes(p)}
			msgHead[k] = append(msgHead[k], j)
		}
		// The post contains a record.
		windowHashes(m.text, historyContain, func(h uint64, at int) {
			for _, i := range recHead[hkey{m.prompt, h}] {
				if strings.HasPrefix(m.text[at:], recs[i].text) {
					seen(i, m.at)
				}
			}
		})
	}
	// A record contains a post (a part, a truncation).
	for i, r := range recs {
		windowHashes(r.text, historyContain, func(h uint64, at int) {
			for _, j := range msgHead[hkey{r.prompt, h}] {
				if strings.HasPrefix(r.text[at:], msgs[j].text) {
					seen(i, msgs[j].at)
				}
			}
		})
	}
	return match
}

// historyResume decides the resume point: the end of the newest record
// found (0: none), and how many records up to it were found. recs are in
// file order.
func historyResume(recs []historyRec, msgs []historyMsg) (resume int64, found int) {
	match := historyMatches(recs, msgs)
	anchor := -1
	for i, r := range recs {
		if !match[i].IsZero() && len([]rune(r.text)) >= historyAnchor {
			anchor = i
		}
	}
	if anchor < 0 {
		return 0, 0
	}
	// A short record after the anchor counts only when an equal text was
	// posted after the anchor's post.
	posted := map[historyMsg]bool{}
	for _, m := range msgs {
		if !m.at.Before(match[anchor]) {
			posted[historyMsg{prompt: m.prompt, text: m.text}] = true
		}
	}
	last := anchor
	short := map[int]bool{}
	for i := anchor + 1; i < len(recs); i++ {
		if posted[historyMsg{prompt: recs[i].prompt, text: recs[i].text}] {
			last, short[i] = i, true
		}
	}
	for i := range recs[:last+1] {
		if (i <= anchor && !match[i].IsZero()) || short[i] {
			found++
		}
	}
	return recs[last].end, found
}

// historyCheckLoop decides the listed rollouts, one at a time, after
// ResolveDelay (the first polls of the chats come first).
func (e *Engine) historyCheckLoop(ctx context.Context) {
	list, err := e.st.HistoryChecks()
	if err != nil {
		e.logf("resume history check: %v", err)
		return
	}
	pending := slices.DeleteFunc(list, func(c store.HistoryCheck) bool { return c.Done })
	if len(pending) == 0 || !sleepCtx(ctx, e.t.ResolveDelay) {
		return
	}
	for _, c := range pending {
		for failures := 0; ; failures++ {
			err := e.checkHistory(ctx, c)
			if err == nil || ctx.Err() != nil {
				break
			}
			back := min(e.t.Hot<<min(failures+1, 10), e.t.MaxBackoff)
			e.logf("resume history check of %s: %v; retrying in %s", c.Path, err, back)
			if !sleepCtx(ctx, back) {
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		wake(e.historyWake)
	}
}

// checkHistory reads the chat history of c's conversation and decides c.
// It returns an error when the read did not finish; c is then still
// pending.
func (e *Engine) checkHistory(ctx context.Context, c store.HistoryCheck) error {
	e.mu.Lock()
	cs := e.convs[c.ConvID]
	var chat, state string
	if cs != nil {
		chat, state = cs.chat, cs.state
	}
	e.mu.Unlock()
	if cs == nil || chat == "" || state == store.ConvClosed {
		return e.decideHistory(c, 0, "its chat is closed or gone: catch-up from the converted position")
	}
	recs, err := e.historyRecords(ctx, c)
	if err != nil {
		return err
	}
	if len(recs) == 0 {
		return e.decideHistory(c, 0, "no record to compare: catch-up from the converted position")
	}
	var since time.Time
	for _, r := range recs {
		if !r.at.IsZero() && (since.IsZero() || r.at.Before(since)) {
			since = r.at
		}
	}
	if !since.IsZero() {
		since = since.Add(-historySkew)
	}
	var msgs []historyMsg
	read := 0
	_, err = e.readHistory(ctx, chat, since, func(_ *store.Tx, page []channel.InboundMsg) (string, error) {
		read += len(page)
		for _, m := range page {
			if h, ok := historyMessage(m); ok {
				msgs = append(msgs, h)
			}
		}
		return "", nil
	})
	switch {
	case errors.Is(err, errHistoryTooLong):
		return e.decideHistory(c, 0, fmt.Sprintf("the chat's history is longer than %d pages: catch-up from the converted position", legacyCheckPages))
	case err != nil:
		return err
	}
	resume, found := historyResume(recs, msgs)
	where := "catch-up from the converted position"
	switch {
	case resume == 0:
	case resume >= recs[len(recs)-1].end:
		where = "the newest record is in the chat: nothing to catch up"
	case resume >= lastBefore(recs, c.To):
		where = "the records before the old position are in the chat: catch-up from there"
	default:
		where = "catch-up after the newest record found"
	}
	return e.decideHistory(c, resume, fmt.Sprintf("%d of %d records up to offset %d found among %d posts (%d messages read); %s",
		found, len(recs), max(resume, c.From), len(msgs), read, where))
}

// lastBefore is the end of the last record that starts before off (0: none).
func lastBefore(recs []historyRec, off int64) int64 {
	end := int64(0)
	for _, r := range recs {
		if r.end <= off {
			end = r.end
		}
	}
	return end
}

// historyRecords reads the prompts and finals of c's rollout in [From, End)
// as the mirror selects them (helper-run turns left out).
func (e *Engine) historyRecords(ctx context.Context, c store.HistoryCheck) ([]historyRec, error) {
	srcs, err := e.st.Sources()
	if err != nil {
		return nil, err
	}
	thread := ""
	for _, s := range srcs {
		if s.Path == c.Path {
			thread = s.ThreadID
		}
	}
	ids, err := e.st.CodexTurnIDs()
	if err != nil {
		return nil, err
	}
	ran := make(map[string]bool, len(ids))
	for _, id := range ids {
		ran[id] = true
	}
	skip := func(clientID, turnID string) bool {
		return strings.HasPrefix(clientID, "cxp-") || (turnID != "" && ran[turnID])
	}
	fh, err := os.Open(c.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer fh.Close()
	tr := transcript.NewTracker(thread, c.From, c.FromLine, skip)
	var recs []historyRec
	for tr.Offset() < c.End {
		lines, next, err := transcript.Read(fh, tr.Offset(), mirrorChunk)
		if err != nil {
			return nil, err
		}
		if len(lines) == 0 {
			break
		}
		for _, o := range tr.Feed(lines) {
			if o.End > c.End || (o.Kind != transcript.OutputPrompt && o.Kind != transcript.OutputFinal) {
				continue
			}
			prompt := o.User || o.Kind == transcript.OutputPrompt
			text, err := e.visibleText(ctx, strings.TrimSpace(o.Text), !prompt)
			if err != nil {
				return nil, err
			}
			recs = append(recs, historyRec{prompt: prompt, text: text, end: o.End, at: o.Time})
		}
		tr.Advance(next)
	}
	return recs, nil
}

// visibleText is the visible text of a post of text without labels, as
// letters and digits (legacyText; math only in an answer).
func (e *Engine) visibleText(ctx context.Context, text string, math bool) (string, error) {
	if math {
		return e.legacyText(ctx, text)
	}
	parts, err := e.ch.Render(ctx, channel.RichDoc{Markdown: text})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, p := range parts {
		if t, ok := e.cfg.Channel.(channel.PartTexter); ok {
			b.WriteString(t.PartText(p.Body))
		} else {
			b.WriteString(p.Body)
		}
	}
	return letters(b.String()), nil
}

// decideHistory records the decision of c in one transaction.
func (e *Engine) decideHistory(c store.HistoryCheck, resume int64, outcome string) error {
	if resume <= c.From {
		resume = 0
	}
	decided := false
	err := e.update(func(tx *store.Tx) error {
		saved, exists, err := tx.HistoryCheck(c.Path)
		if err != nil {
			return err
		}
		if !exists || saved.Done {
			decided = true
			return nil
		}
		saved.Done, saved.Resume, saved.Outcome = true, resume, outcome
		return tx.SetHistoryCheck(saved)
	})
	if err == nil && !decided {
		e.logf("resume history check of %s: %s", c.Path, outcome)
	}
	return err
}

// mirrorHistory applies the listed checks to the tracked rollouts (mirror
// goroutine): a pending one is held, a decided one skips the records up to
// its resume point.
func (e *Engine) mirrorHistory() {
	list, err := e.st.HistoryChecks()
	if err != nil {
		e.logf("mirror: %v", err)
		return
	}
	m := e.mirror
	for _, c := range list {
		m.mu.Lock()
		f := m.files[c.Path]
		m.mu.Unlock()
		if f == nil {
			continue
		}
		if f.held && c.Done {
			f.nextStat = e.now() // released: its catch-up starts at the next poll
		}
		f.held = !c.Done
		if c.Done && c.Resume > f.proven {
			f.proven = c.Resume
		}
	}
}
