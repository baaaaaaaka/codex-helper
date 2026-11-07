package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

// Chat titles and unread marks, as v0.1.20 made them (dashboard.go,
// bridge.go renameSessionChat / renameControlChat / renameMachineHostname /
// maybeUpdateWorkChatTitleFromLocalSession, markChatUnreadForUser):
//
//   - the control chat is "🏠 Codex Control - <machine>[ - <user title>]"
//     (F-CTL-1); `rename <title>` sets the user title, kept for a control
//     chat created again (F-CTL-8).
//   - a work chat is "💬 <machine> - <title>": after a completed turn (and a
//     live local final) its title follows the Codex thread name, else the
//     first prompt, until the owner renames it with `helper rename <title>`
//     (F-WRK-2).
//   - `rename hostname <name>` renames the machine in the control chat's
//     and every work chat's title, without nesting the old prefix, and
//     lists the chats it could not rename (F-CTL-8).
//   - a chat that got a final answer is marked unread (F-TRN-1); caught-up
//     and imported records never are.
//
// Every rename and unread mark is a Graph write under the account's write
// pacing: the owner's renames wait for a write token in the command
// worker, the automatic ones are chat edits made by the sender
// (edits.go).

const (
	controlMarker = "🏠 Codex Control"
	workMarker    = "💬"
	maxTitleRunes = 64

	// metaMachineLabel is the machine label set by `rename hostname`
	// (`cxp teams migrate` keeps v0.1.20's hostname override here too); it
	// wins over the configured label, as v0.1.20's override did.
	metaMachineLabel = "machine_label"
	// metaControlTitle is the user title of the control chat.
	metaControlTitle = "engine_control_user_title"
	// metaUserTitle + conversation id: the user title of a work chat.
	metaUserTitle = "engine_user_title:"

	sessionIndexMax = 8 << 20
)

type titleState struct {
	// run serialises the chat renames, so an automatic title never
	// overwrites a rename by the owner made meanwhile.
	run sync.Mutex
	// Under Engine.mu:
	label  string // the machine label in titles ("" until loaded)
	loaded bool
	unread map[string]time.Time // notifications whose chat was marked unread
	// lastFinal is when the channel created the newest answer post of a
	// conversation (the unread mark's reference).
	lastFinal map[int64]time.Time
	// retitling holds when a conversation's automatic title was queued
	// (until it is made; see retitling).
	retitling map[int64]time.Time
}

// machineLabel is the machine's name in chat titles and status.
func (e *Engine) machineLabel() string {
	e.mu.Lock()
	loaded, label := e.titles.loaded, e.titles.label
	e.mu.Unlock()
	if !loaded {
		v, _, err := e.st.Meta(metaMachineLabel)
		if err == nil {
			label = sanitizeTitle(v)
			e.mu.Lock()
			if !e.titles.loaded {
				e.titles.label, e.titles.loaded = label, true
			}
			label = e.titles.label
			e.mu.Unlock()
		}
	}
	if label == "" {
		label = strings.TrimSpace(e.cfg.Machine)
	}
	return label
}

// machinePart is the machine segment of a title (machineTitlePart).
func machinePart(label string) string {
	if p := sanitizeTitle(label); p != "" {
		return p
	}
	return "machine"
}

func (e *Engine) controlTitle() string {
	user, _, _ := e.st.Meta(metaControlTitle)
	return controlTitleFor(e.machineLabel(), user)
}

func controlTitleFor(label, user string) string {
	return joinTitle(controlMarker, machinePart(label), sanitizeTitle(user))
}

// workTitle is a work chat's title (WorkChatTitle): the user title, else
// the topic, else the directory's name.
func workTitle(label, user, topic, cwd string) string {
	display := sanitizeTitle(user)
	if display == "" {
		display = sanitizeTitle(topic)
	}
	if display == "" {
		if base := lastPathElement(cwd); base != "" {
			display = sanitizeTitle(base)
		}
	}
	if display == "" {
		display = "untitled"
	}
	return topicTitle(joinTitle(workMarker+" "+machinePart(label), display))
}

func joinTitle(parts ...string) string {
	kept := parts[:0:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, " - ")
}

// sanitizeTitle is SanitizeDashboardTitle: paths shown by their last
// element, no control or reserved characters, at most 64 runes.
func sanitizeTitle(title string) string {
	title = strings.TrimSpace(redactPaths(title))
	if title == "" {
		return ""
	}
	clean := cleanTitle(title)
	rs := []rune(clean)
	if len(rs) <= maxTitleRunes {
		return clean
	}
	return strings.TrimSpace(string(rs[:maxTitleRunes-3])) + "..."
}

// topicTitle is SanitizeTopic: a chat topic of at most 120 runes.
func topicTitle(topic string) string {
	clean := cleanTitle(topic)
	if clean == "" {
		clean = "codex session"
	}
	if rs := []rune(clean); len(rs) > 120 {
		clean = strings.TrimSpace(string(rs[:120]))
	}
	return clean
}

// promptTitleMarkers start the helper's own suffix of a prompt: v0.1.20's
// teamsHelperPromptSuffixMarkers, plus the earlier core's markers.
var promptTitleMarkers = []string{
	"Teams helper safety:",
	"If you need to return generated files or images to the Teams user,",
	"Then include this fenced manifest in your final answer:",
	"Helper safety:",
	"Attached files (saved locally):",
}

// promptTitle is the title text of a prompt as Codex received it
// (v0.1.20 firstPromptTitleText): the helper's suffix (safety notes,
// artifact instructions) and its "User message:" lead are cut, and a
// voice-only prompt is titled by its transcript, not by the ASR lead.
func promptTitle(text string) string {
	text = strings.TrimSpace(text)
	stripped := false
	for _, marker := range promptTitleMarkers {
		at := -1
		for _, prefix := range []string{"\n\n", "\n"} {
			if i := strings.LastIndex(text, prefix+marker); i > at {
				at = i
			}
		}
		if at >= 0 {
			text = strings.TrimSpace(text[:at])
			stripped = true
			continue
		}
		if strings.HasPrefix(text, marker) {
			return ""
		}
	}
	if stripped && len(text) >= len(userMessageLead) && strings.EqualFold(text[:len(userMessageLead)], userMessageLead) {
		text = strings.TrimSpace(text[len(userMessageLead):])
	}
	if lead := strings.TrimSpace(asrLead); strings.HasPrefix(text, lead) {
		text = strings.TrimSpace(text[len(lead):])
		// The clip header line ("Voice/video clip: name (attrs)") is not
		// the owner's words either.
		if strings.HasPrefix(text, "Voice/video clip") {
			if i := strings.Index(text, "\n"); i >= 0 {
				text = strings.TrimSpace(text[i+1:])
			} else {
				text = ""
			}
		}
	}
	return text
}

func cleanTitle(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsControl(r) {
			continue
		}
		switch r {
		case ':', '<', '>', '"', '/', '\\', '|', '?', '*':
			b.WriteByte('-')
		default:
			b.WriteRune(r)
		}
	}
	return strings.Trim(strings.Join(strings.Fields(b.String()), " "), " -.")
}

func redactPaths(text string) string {
	fields := strings.Fields(text)
	changed := false
	for i, f := range fields {
		start, end := 0, len(f)
		for start < end && strings.ContainsRune("'\"([{<", rune(f[start])) {
			start++
		}
		for end > start && strings.ContainsRune("'\".,;:)]}>", rune(f[end-1])) {
			end--
		}
		if core := f[start:end]; pathLike(core) {
			if base := lastPathElement(core); base != "" {
				fields[i] = f[:start] + base + f[end:]
				changed = true
			}
		}
	}
	if !changed {
		return text
	}
	return strings.Join(fields, " ")
}

func pathLike(t string) bool {
	if strings.HasPrefix(t, "/") || strings.HasPrefix(t, "~/") || strings.HasPrefix(t, `~\`) {
		return true
	}
	return len(t) >= 3 && (t[0] >= 'A' && t[0] <= 'Z' || t[0] >= 'a' && t[0] <= 'z') && t[1] == ':' && (t[2] == '\\' || t[2] == '/')
}

func lastPathElement(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	p = strings.TrimRight(strings.ReplaceAll(p, "\\", "/"), "/")
	if p == "" {
		return "root"
	}
	if i := strings.LastIndex(p, "/"); i >= 0 {
		p = p[i+1:]
	}
	if p = strings.TrimSpace(p); p == "." {
		return ""
	}
	return p
}

// shortLine is shortenTeamsLine.
func shortLine(text string, n int) string {
	text = strings.TrimSpace(text)
	rs := []rune(text)
	if len(rs) <= n {
		return text
	}
	return strings.TrimSpace(string(rs[:n-1])) + "…"
}

// renameChat renames chat under the account's write pacing. A throttle
// closes the write gate like a send's and the rename is tried again after
// it; an unknown outcome is tried again (the PATCH is idempotent).
func (e *Engine) renameChat(ctx context.Context, chat, title string) error {
	ctx, cancel := context.WithTimeout(ctx, editTimeout)
	defer cancel()
	var err error
	for try := 0; try < 3; try++ {
		if err = e.waitWrite(ctx); err != nil {
			return err
		}
		if err = e.renameOnce(ctx, chat, title); err == nil {
			return nil
		}
		if _, throttled := channel.AsThrottled(err); !throttled && !channel.IsAmbiguous(err) {
			return err
		}
	}
	return err
}

// renameOnce makes one rename with a write token already taken; a
// throttle closes the write gate for the account.
func (e *Engine) renameOnce(ctx context.Context, chat, title string) error {
	started := e.now()
	err := e.ch.RenameChat(ctx, chat, title)
	if th, ok := channel.AsThrottled(err); ok {
		next := e.now().Add(e.retryAfter(th))
		if th.Scope == channel.ScopeAccount || th.Scope == channel.ScopeUnknown {
			e.setGate(e.gWrite, next, "rename throttled")
			e.paceThrottled(started, next)
		} else {
			sleepCtx(ctx, next.Sub(e.now()))
		}
	}
	return err
}

// cmdRename is `rename <title>` / `rename hostname <name>` in the control
// chat and `helper rename …` in a work chat (cs != nil).
func (e *Engine) cmdRename(ctx context.Context, j cmdJob, cs *convState, arg string) {
	fields := strings.Fields(arg)
	if len(fields) > 0 && strings.EqualFold(fields[0], "hostname") {
		e.cmdRenameHostname(ctx, j, strings.Join(fields[1:], " "))
		return
	}
	title := sanitizeTitle(arg)
	if cs == nil {
		e.renameControl(ctx, j, title)
		return
	}
	if title == "" {
		e.reply(j, "usage: `helper rename <title>`", nil)
		return
	}
	e.titles.run.Lock()
	defer e.titles.run.Unlock()
	e.mu.Lock()
	chat, cwd, id := cs.chat, cs.cwd, cs.id
	e.mu.Unlock()
	topic := workTitle(e.machineLabel(), title, "", cwd)
	if err := e.renameChat(ctx, chat, topic); err != nil {
		e.reply(j, "rename failed: "+err.Error(), nil)
		return
	}
	e.reply(j, "Work chat renamed.\n\nNew title:\n"+shortLine(topic, 96)+"\n\nUse `helper details` to see the full Teams title and link.", func(tx *store.Tx) error {
		if err := tx.SetTitle(id, topic, "user"); err != nil {
			return err
		}
		if err := tx.SetMeta(fmt.Sprint(metaUserTitle, id), title); err != nil {
			return err
		}
		_, err := e.refresh(tx, id)
		return err
	})
}

func (e *Engine) renameControl(ctx context.Context, j cmdJob, title string) {
	if title == "" {
		e.reply(j, "usage: `helper rename <title>` or `rename <title>`", nil)
		return
	}
	e.titles.run.Lock()
	defer e.titles.run.Unlock()
	e.mu.Lock()
	id := e.controlID
	var chat string
	if cs := e.convs[id]; cs != nil {
		chat = cs.chat
	}
	e.mu.Unlock()
	if chat == "" {
		e.reply(j, "rename failed: control chat is not bound yet", nil)
		return
	}
	topic := topicTitle(controlTitleFor(e.machineLabel(), title))
	if err := e.renameChat(ctx, chat, topic); err != nil {
		e.reply(j, "rename failed: "+err.Error(), nil)
		return
	}
	e.reply(j, "Control chat renamed.\n\nNew title:\n"+shortLine(topic, 96), func(tx *store.Tx) error {
		if err := tx.SetTitle(id, topic, "user"); err != nil {
			return err
		}
		if err := tx.SetMeta(metaControlTitle, title); err != nil {
			return err
		}
		_, err := e.refresh(tx, id)
		return err
	})
}

// cmdRenameHostname renames the machine in every chat title
// (renameMachineHostname).
func (e *Engine) cmdRenameHostname(ctx context.Context, j cmdJob, raw string) {
	label := sanitizeTitle(raw)
	if label == "" {
		e.reply(j, "usage: `helper rename hostname <name>`", nil)
		return
	}
	e.titles.run.Lock()
	defer e.titles.run.Unlock()
	old := []string{}
	seen := map[string]bool{}
	for _, l := range []string{e.machineLabel(), e.cfg.Machine, e.cfg.Host, "machine"} {
		if l = sanitizeTitle(l); l != "" && !seen[strings.ToLower(l)] {
			seen[strings.ToLower(l)] = true
			old = append(old, l)
		}
	}
	type target struct {
		id                      int64
		label, chat, title, cwd string
		control                 bool
	}
	var targets []target
	e.mu.Lock()
	for id, cs := range e.convs {
		if id == e.inboxID || cs.chat == "" || cs.state == store.ConvClosed || cs.kind != store.KindWork && id != e.controlID {
			continue
		}
		targets = append(targets, target{id: id, label: cs.label, chat: cs.chat, title: cs.title, cwd: cs.cwd, control: id == e.controlID})
	}
	e.mu.Unlock()
	sort.Slice(targets, func(a, b int) bool {
		if targets[a].control != targets[b].control {
			return targets[a].control
		}
		return targets[a].id < targets[b].id
	})
	userTitles := map[int64]string{}
	controlUser := ""
	if err := e.update(func(tx *store.Tx) error {
		var err error
		if controlUser, _, err = tx.Meta(metaControlTitle); err != nil {
			return err
		}
		for _, t := range targets {
			v, _, err := tx.Meta(fmt.Sprint(metaUserTitle, t.id))
			if err != nil {
				return err
			}
			userTitles[t.id] = v
		}
		return nil
	}); err != nil {
		e.reply(j, "rename failed: "+err.Error(), nil)
		return
	}
	var failures []string
	controlBound, controlDone := false, false
	titles := map[int64]string{}
	for _, t := range targets {
		var topic string
		if t.control {
			controlBound = true
			topic = topicTitle(controlTitleFor(label, controlUser))
		} else {
			display := strings.TrimSpace(t.title)
			if userTitles[t.id] == "" {
				display = stripMachinePrefix(display, old)
			}
			topic = workTitle(label, userTitles[t.id], display, t.cwd)
		}
		if err := e.renameChat(ctx, t.chat, topic); err != nil {
			if ctx.Err() != nil {
				return
			}
			name := t.label
			if t.control {
				name = "control chat"
			}
			failures = append(failures, name+": "+err.Error())
			continue
		}
		titles[t.id] = topic
		if t.control {
			controlDone = true
		}
	}
	lines := []string{"Hostname renamed.\n\nNew hostname: " + label}
	switch {
	case controlDone:
		lines = append(lines, "Control chat: updated")
	case !controlBound:
		lines = append(lines, "Control chat: not bound")
	}
	work := len(titles)
	if controlDone {
		work--
	}
	lines = append(lines, fmt.Sprintf("Work chats: %d updated", work))
	if len(failures) > 0 {
		lines = append(lines, "", "Some chat titles were not updated:")
		for i, f := range failures {
			if i >= 4 {
				lines = append(lines, fmt.Sprintf("- and %d more", len(failures)-i))
				break
			}
			lines = append(lines, "- "+shortLine(f, 140))
		}
	}
	e.reply(j, strings.Join(lines, "\n"), func(tx *store.Tx) error {
		if err := tx.SetMeta(metaMachineLabel, label); err != nil {
			return err
		}
		for id, topic := range titles {
			c, ok, err := tx.Conversation(id)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if err := tx.SetTitle(id, topic, c.TitleSource); err != nil {
				return err
			}
			if _, err := e.refresh(tx, id); err != nil {
				return err
			}
		}
		return nil
	})
	e.mu.Lock()
	e.titles.label, e.titles.loaded = label, true
	e.mu.Unlock()
}

// stripMachinePrefix drops a work title's "💬 <machine> - " prefix for any
// of the old machine labels (stripWorkChatMachineTitlePrefix).
func stripMachinePrefix(topic string, old []string) string {
	topic = strings.TrimSpace(topic)
	for _, l := range old {
		prefix := workMarker + " " + machinePart(l)
		if rest, ok := strings.CutPrefix(topic, prefix+" - "); ok {
			return strings.TrimSpace(rest)
		}
		if strings.EqualFold(topic, prefix) {
			return ""
		}
	}
	return topic
}

// queueAutoTitle queues the automatic title of a work chat after an
// answer; it is made once the conversation has nothing left to send.
func (e *Engine) queueAutoTitle(conv int64) {
	e.mu.Lock()
	cs := e.convs[conv]
	ok := cs != nil && cs.kind == store.KindWork && conv != e.inboxID
	e.mu.Unlock()
	if !ok {
		return
	}
	e.mu.Lock()
	if e.titles.retitling == nil {
		e.titles.retitling = map[int64]time.Time{}
	}
	e.titles.retitling[conv] = e.now()
	e.mu.Unlock()
	e.queueEdit(conv, chatEdit{key: "title", what: "chat title", run: func(ctx context.Context, chat string) error {
		defer e.retitled(conv)
		return e.autoTitle(ctx, conv, chat)
	}})
}

// retitleWait bounds how long an owner notification waits for the
// automatic title of its chat (an edit that is dropped never clears it).
const retitleWait = 30 * time.Second

// retitling reports whether conv's automatic title is being made: the
// owner notification of the chat's next answer waits for it, so it names
// the chat by the title the owner sees (and does so deterministically).
func (e *Engine) retitling(conv int64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	at, ok := e.titles.retitling[conv]
	if ok && e.now().Sub(at) > retitleWait {
		delete(e.titles.retitling, conv)
		return false
	}
	return ok
}

// retitled ends retitling(conv) and lets a held notification go.
func (e *Engine) retitled(conv int64) {
	e.mu.Lock()
	delete(e.titles.retitling, conv)
	e.mu.Unlock()
	e.wakeNotify()
}

// autoAllowed is sessionAllowsAutoTitleUpdate.
func autoAllowed(source, title, user string) bool {
	if strings.TrimSpace(user) != "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "auto":
		return true
	case "":
		title = strings.TrimSpace(title)
		return title == "" || strings.Contains(title, "New message in ") || strings.Contains(strings.ToLower(title), "codex work")
	}
	return false
}

// autoTitle sets a work chat's title from its Codex thread name, else its
// first prompt (maybeUpdateWorkChatTitleFromLocalSession): the owner's
// first prompt in the chat, else the first prompt of its local rollout.
// The sender took its write token.
func (e *Engine) autoTitle(ctx context.Context, conv int64, chat string) error {
	e.titles.run.Lock()
	defer e.titles.run.Unlock()
	f, err := e.loadTitleFacts(conv)
	if err != nil || !f.found {
		return err
	}
	c, th := f.c, f.th
	if c.State == store.ConvClosed || !autoAllowed(c.TitleSource, c.Title, f.user) {
		return nil
	}
	name := e.autoName(f)
	if strings.TrimSpace(name) == "" {
		return nil
	}
	topic := workTitle(e.machineLabel(), "", name, th.Cwd)
	if strings.TrimSpace(c.Title) == topic {
		return nil
	}
	if err := e.renameOnce(ctx, chat, topic); err != nil {
		return err
	}
	return e.update(func(tx *store.Tx) error {
		if err := tx.SetTitle(conv, topic, "auto"); err != nil {
			return err
		}
		_, err := e.refresh(tx, conv)
		return err
	})
}

// titleFacts is what a work chat's title is made from.
type titleFacts struct {
	c     store.Conversation
	th    store.Thread
	user  string // the owner's title (`helper rename`)
	first string // the owner's first prompt in the chat
	path  string // the local rollout, when there is no such prompt
	found bool
}

func (e *Engine) loadTitleFacts(conv int64) (titleFacts, error) {
	var f titleFacts
	err := e.update(func(tx *store.Tx) error {
		f = titleFacts{}
		var err error
		if f.c, f.found, err = tx.Conversation(conv); err != nil || !f.found {
			return err
		}
		if f.th, _, err = tx.Thread(conv); err != nil {
			return err
		}
		if f.user, _, err = tx.Meta(fmt.Sprint(metaUserTitle, conv)); err != nil {
			return err
		}
		if f.first, err = tx.FirstPrompt(conv); err != nil {
			return err
		}
		// Batch A: a chat made by `continue` is titled from its local
		// session (v0.1.20), not from the first prompt sent in Teams.
		if v, _, err := tx.Meta(importedKey(conv)); err != nil {
			return err
		} else if v == "1" {
			f.first = ""
		}
		if f.first != "" {
			return nil
		}
		f.path, err = tx.SourcePath(conv)
		return err
	})
	return f, err
}

// autoName is the automatic title text of a work chat: its Codex thread
// name, else the owner's first prompt, else the first prompt of its local
// rollout ("" when none).
func (e *Engine) autoName(f titleFacts) string {
	name := ""
	if f.th.ThreadID != "" {
		home := f.th.CodexHome
		if home == "" && e.cfg.SessionsDir != "" {
			home = filepath.Dir(e.cfg.SessionsDir)
		}
		if home != "" {
			name = threadName(filepath.Join(home, "session_index.jsonl"), f.th.ThreadID)
		}
	}
	if name == "" {
		name = promptTitle(f.first)
	}
	if name == "" && f.path != "" {
		name = promptTitle(transcript.FirstPrompt(f.path, firstPromptMax))
	}
	return strings.TrimSpace(name)
}

// threadName is the newest thread name Codex recorded for thread in its
// session index ("" when none).
func threadName(path, thread string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	if info, err := f.Stat(); err == nil && info.Size() > sessionIndexMax {
		if _, err := f.Seek(info.Size()-sessionIndexMax, io.SeekStart); err != nil {
			return ""
		}
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	name := ""
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 || !strings.Contains(string(line), thread) {
			continue
		}
		var rec struct {
			ID   string `json:"id"`
			Name string `json:"thread_name"`
		}
		if json.Unmarshal(line, &rec) == nil && rec.ID == thread && strings.TrimSpace(rec.Name) != "" {
			name = strings.TrimSpace(rec.Name)
		}
	}
	return name
}

// noteFinalSent records when the channel created an answer post (the
// unread mark's reference).
func (e *Engine) noteFinalSent(conv int64, kind string, at time.Time) {
	if kind != store.OutFinal && kind != store.OutMirror || at.IsZero() {
		return
	}
	e.mu.Lock()
	if e.titles.lastFinal == nil {
		e.titles.lastFinal = map[int64]time.Time{}
	}
	if at.After(e.titles.lastFinal[conv]) {
		e.titles.lastFinal[conv] = at
	}
	e.mu.Unlock()
}

// afterAnswers follows the delivered answers pumpNotify announced, once
// per answer: the chat is marked unread for the owner from the newest
// answer post on (F-TRN-1), then its title follows the session
// (F-WRK-2). Called once the notifications named the chat by its title.
// notified keeps the entries of list whose notification has gone out
// (no longer pending): an answer is followed (unread mark, title) only
// after the notification that names its chat, also when that
// notification waits for a later pump or the next process.
func (e *Engine) notified(list []pendingNotify) []pendingNotify {
	if len(list) == 0 {
		return nil
	}
	v, _, err := e.st.Meta(metaNotify)
	if err != nil {
		return nil
	}
	var pending []pendingNotify
	if v != "" {
		if err := json.Unmarshal([]byte(v), &pending); err != nil {
			return nil
		}
	}
	still := map[string]bool{}
	for _, p := range pending {
		still[p.id()] = true
	}
	var out []pendingNotify
	for _, n := range list {
		if !still[n.id()] {
			out = append(out, n)
		}
	}
	return out
}

func (e *Engine) afterAnswers(list []pendingNotify) {
	for _, n := range list {
		if n.Kind != "" {
			continue // a discovered session's notice, not an answer
		}
		id := n.id()
		now := e.now()
		e.mu.Lock()
		if e.titles.unread == nil {
			e.titles.unread = map[string]time.Time{}
		}
		if _, done := e.titles.unread[id]; done {
			e.mu.Unlock()
			continue
		}
		for k, at := range e.titles.unread {
			if now.Sub(at) > time.Hour {
				delete(e.titles.unread, k)
			}
		}
		e.titles.unread[id] = now
		at := e.titles.lastFinal[n.Conv]
		e.mu.Unlock()
		if e.ch.Caps().MarkUnread {
			readAt := time.Time{}
			if !at.IsZero() {
				readAt = at.Add(-time.Millisecond)
			}
			e.queueEdit(n.Conv, chatEdit{key: "unread:" + id, what: "unread mark", run: func(ctx context.Context, chat string) error {
				return e.ch.MarkUnread(ctx, chat, readAt)
			}})
		}
		e.queueAutoTitle(n.Conv)
		e.wakeSender()
	}
}
