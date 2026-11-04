package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

const (
	channel             = "teams"
	controlLabel        = "control"
	controlFallbackID   = "__control_fallback__"
	accountGraphGateKey = "__cxp_account_graph_write__"
)

// StreamKey is the receive-stream key of a Teams chat.
func StreamKey(chatID string) string { return store.ChatStreamKey(channel, chatID) }

// LegacyTurnKey is the turn key of a migrated turn: short, stable, and in a
// namespace new keys ('cxp-...') never use.
func LegacyTurnKey(oldID string) string {
	h := sha256.Sum256([]byte(oldID))
	return "legacy-" + hex.EncodeToString(h[:8])
}

// cursor renders a Teams lastModifiedDateTime watermark.
func cursor(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

type sessInfo struct {
	s      legacySession
	conv   int64
	state  string
	paused bool
}

type compactTurn struct {
	t       legacyTurn
	conv    int64
	order   int64
	newID   int64
	keep    bool
	newStat string
}

type converter struct {
	l   *legacy
	im  *store.Importer
	rep *Report
	opt Options
	now time.Time

	control      int64
	controlChat  string
	scopeID      string
	sessions     map[string]*sessInfo
	convByChat   map[string]int64
	labelOf      map[int64]string
	forkParent   map[string]string // child session -> parent session (completed forks)
	holdChild    map[string]bool   // child sessions of unfinished forks
	polls        map[string]legacyPoll
	turns        map[string]*compactTurn
	extSeen      map[int64]map[string]bool // outbox ext ids per conversation
	oldestIn     map[int64]int64           // oldest inbound ext_time per conversation
	inboundOld   map[string]int64          // old inbound id -> new id (rows whose turn is kept)
	needPause    map[int64]string
	orphans      map[string]int64        // unknown chat (or "session:<id>") -> placeholder conversation
	lastAct      map[int64]int64         // newest activity per conversation (unix ms)
	unknownSrc   int                     // source rows with an unknown send outcome
	staleUnsent  int                     // unsent source rows dropped as stale
	anchors      map[string]legacyAnchor // by source path
	threadLinks  map[string]threadLink
	proofs       map[string]*proof // delivery-ledger proof per rollout path
	outboxMirror map[string]string // legacy outbox id -> mirror delivery key
	loserOf      map[int64]int64   // parked shared-thread conversation -> winner
	mirrorUsed   map[string]bool
	zeroNoProof  int // tracking sources resumed at 0 for lack of proof
	ledger       []legacyLedger
}

func (c *converter) ms() int64 { return c.now.UnixMilli() }

// touch records activity on a conversation (shared-thread winner choice).
func (c *converter) touch(conv, at int64) {
	if conv != 0 && at > c.lastAct[conv] {
		c.lastAct[conv] = at
	}
}

func (c *converter) convOf(sessionID, chatID string) (int64, bool) {
	if sessionID == controlFallbackID {
		return c.control, c.control != 0
	}
	if si, ok := c.sessions[sessionID]; ok && si.conv != 0 {
		return si.conv, true
	}
	if chatID != "" {
		if id, ok := c.convByChat[chatID]; ok {
			return id, true
		}
	}
	return 0, false
}

func (c *converter) run() error {
	steps := []struct {
		name string
		fn   func() error
	}{
		{"control", c.convertControl},
		{"forks (scan)", c.scanForks},
		{"chat polls (scan)", c.scanPolls},
		{"sessions", c.convertSessions},
		{"forks", c.convertForks},
		{"turns (scan)", c.scanTurns},
		{"inbound", c.convertInbound},
		{"turns", c.convertTurns},
		{"streams", c.convertStreams},
		{"delivery proofs (scan)", c.scanProofs},
		{"outbox", c.convertOutbox},
		{"deliveries", c.convertDeliveries},
		{"transcript sources", c.convertSources},
		{"notifications", c.convertNotifications},
		{"workflow config", c.convertWorkflow},
		{"artifacts", c.countArtifacts},
		{"gates", c.convertGates},
		{"meta", c.convertMeta},
	}
	for _, st := range steps {
		if err := st.fn(); err != nil {
			return fmt.Errorf("%s: %w", st.name, err)
		}
	}
	for conv, reason := range c.needPause {
		if err := c.im.PauseConversation(conv, c.ms(), reason); err != nil {
			return err
		}
	}
	return nil
}

func (c *converter) convertControl() error {
	var cc legacyControlChat
	_, _ = decode(c.l.section("control_chat"), &cc)
	var scope legacyScope
	_, _ = decode(c.l.section("scope"), &scope)
	c.scopeID = scope.ID
	if c.scopeID == "" {
		c.scopeID = cc.ScopeID
	}
	if cc.TeamsChatID == "" && c.opt.registry.ControlChatID != "" {
		cc.TeamsChatID = c.opt.registry.ControlChatID
		cc.TeamsChatURL = c.opt.registry.ControlChatURL
		cc.TeamsChatTopic = c.opt.registry.ControlChatTopic
		c.rep.note("control chat taken from registry.json (no binding in the store)")
	}
	e := c.rep.E("control chat")
	if cc.TeamsChatID == "" {
		e.Outcomes["absent"]++
		return nil
	}
	e.Source = 1
	c.controlChat = cc.TeamsChatID
	id, err := c.im.Conversation(store.Conversation{
		Label: controlLabel, Kind: store.KindControl, Channel: channel, ExtChatID: cc.TeamsChatID,
		ExtURL: cc.TeamsChatURL, Title: firstNonEmpty(cc.UserTitle, cc.TeamsChatTopic), TitleSource: titleSource(cc.UserTitle, cc.TeamsChatTopic),
		State: store.ConvActive, CreatedAt: ms(cc.BoundAt),
	})
	if err != nil {
		return err
	}
	c.control = id
	c.convByChat[cc.TeamsChatID] = id
	c.labelOf[id] = controlLabel
	e.Imported = 1
	return nil
}

func titleSource(user, topic string) string {
	switch {
	case user != "":
		return "user"
	case topic != "":
		return "legacy"
	}
	return ""
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

func completedFork(phase string) bool { return phase == "link_sent" || phase == "activated" }

func (c *converter) scanForks() error {
	return c.l.each("fork_operations", func(r row) error {
		var f legacyFork
		if _, err := decode(r.JSON, &f); err != nil {
			return nil // reported by convertForks
		}
		if completedFork(f.Phase) {
			if f.ChildSessionID != "" {
				c.forkParent[f.ChildSessionID] = f.ParentSessionID
			}
		} else if f.ChildSessionID != "" {
			c.holdChild[f.ChildSessionID] = true
		}
		return nil
	})
}

func (c *converter) scanPolls() error {
	e := c.rep.E("chat_polls")
	return c.l.each("chat_polls", func(r row) error {
		e.Source++
		var p legacyPoll
		if _, err := decode(r.JSON, &p); err != nil {
			e.Failed = append(e.Failed, r.ID)
			return nil
		}
		if p.ChatID == "" {
			p.ChatID = r.ID
		}
		c.polls[p.ChatID] = p
		return nil
	})
}

func (c *converter) convertSessions() error {
	e := c.rep.E("sessions")
	var list []legacySession
	var opaque []row
	err := c.l.each("sessions", func(r row) error {
		e.Source++
		var s legacySession
		if _, err := decode(r.JSON, &s); err != nil {
			e.Failed = append(e.Failed, r.ID)
			opaque = append(opaque, r)
			return nil
		}
		if s.ID == "" {
			s.ID = r.ID
		}
		list = append(list, s)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	for _, s := range list {
		if s.ID == controlFallbackID || s.RunnerKind == "control_fallback" {
			c.sessions[s.ID] = &sessInfo{s: s, conv: c.control, state: "control"}
			if c.control != 0 && s.CodexThreadID != "" {
				if err := c.im.Thread(threadOf(c.control, s)); err != nil {
					return err
				}
			}
			e.Imported++
			e.Outcomes["control fallback -> control conversation"]++
			continue
		}
		state, paused := store.ConvActive, false
		switch s.Status {
		case "active":
		case "archived":
			state = store.ConvParked
		case "closed":
			state = store.ConvClosed
		case "quarantined":
			paused = true
		case "staging", "awaiting_history":
			state = store.ConvHold
		default:
			state = store.ConvHold
			c.rep.note("session %s: unknown status %q -> hold", s.ID, s.Status)
		}
		if c.holdChild[s.ID] && state != store.ConvClosed {
			state = store.ConvHold
		}
		var parkedAt int64
		if p, ok := c.polls[s.TeamsChatID]; ok && s.TeamsChatID != "" && ms(p.ParkedAt) != 0 {
			parkedAt = ms(p.ParkedAt)
			if state == store.ConvActive {
				state = store.ConvParked
			}
		}
		if state == store.ConvParked && parkedAt == 0 {
			parkedAt = firstNonZero(ms(s.UpdatedAt), c.ms())
		}
		if state != store.ConvParked {
			parkedAt = 0
		}
		chat := s.TeamsChatID
		if chat != "" {
			if prev, dup := c.convByChat[chat]; dup {
				c.rep.note("session %s shares chat %s with %s: imported as hold without a chat", s.ID, chat, c.labelOf[prev])
				chat, state = "", store.ConvHold
			}
		}
		if chat == "" && state == store.ConvActive {
			state = store.ConvCreating
		}
		conv := store.Conversation{
			Label: s.ID, Kind: store.KindWork, Channel: channel, ExtChatID: chat, ExtURL: s.TeamsChatURL,
			Title: firstNonEmpty(s.UserTitle, s.TeamsTopic), TitleSource: titleSource(s.UserTitle, s.TeamsTopic),
			State: state, ParkedAt: parkedAt, CreatedAt: firstNonZero(ms(s.CreatedAt), c.ms()),
		}
		if paused {
			conv.PausedAt = firstNonZero(ms(s.QuarantinedAt), c.ms())
			conv.PausedReason = "migrated-quarantine"
			if s.QuarantineReason != "" {
				conv.PausedReason += ": " + s.QuarantineReason
			}
		}
		id, err := c.im.Conversation(conv)
		if err != nil {
			return err
		}
		c.sessions[s.ID] = &sessInfo{s: s, conv: id, state: state, paused: paused}
		c.labelOf[id] = s.ID
		if chat != "" {
			c.convByChat[chat] = id
		}
		// Thread binding; a newer thread-link journal entry is reported, and
		// folded only when the session has no thread at all.
		if tl, ok := c.threadLinks[s.ID]; ok && tl.CodexThreadID != "" && tl.CodexThreadID != s.CodexThreadID {
			if s.CodexThreadID == "" {
				s.CodexThreadID = tl.CodexThreadID
				c.rep.E("thread-link journal").Outcomes["folded into an empty binding"]++
			} else {
				c.rep.E("thread-link journal").Outcomes["differs; store binding kept"]++
				c.rep.note("session %s: thread-link journal says %s, store says %s (kept)", s.ID, tl.CodexThreadID, s.CodexThreadID)
			}
		}
		th := threadOf(id, s)
		if p, ok := c.forkParent[s.ID]; ok {
			if pi, ok := c.sessions[p]; ok {
				th.ForkParent = pi.conv
			}
		}
		if err := c.im.Thread(th); err != nil {
			return err
		}
		e.Imported++
		switch {
		case paused:
			e.Outcomes["quarantined -> active+paused"]++
		default:
			e.Outcomes[s.Status+" -> "+state]++
		}
	}
	// Rows that could not be parsed: hold, attributed by their columns.
	for _, r := range opaque {
		chat := r.Cols["teams_chat_id"]
		if _, dup := c.convByChat[chat]; dup {
			chat = ""
		}
		id, err := c.im.Conversation(store.Conversation{Label: r.ID, Kind: store.KindWork, Channel: channel, ExtChatID: chat, State: store.ConvHold})
		if err != nil {
			return err
		}
		c.sessions[r.ID] = &sessInfo{s: legacySession{ID: r.ID, TeamsChatID: chat}, conv: id, state: store.ConvHold}
		c.labelOf[id] = r.ID
		if chat != "" {
			c.convByChat[chat] = id
		}
		e.Imported++
		e.Outcomes["unparsable -> hold"]++
	}
	return nil
}

func threadOf(conv int64, s legacySession) store.Thread {
	effortSrc := s.ReasoningEffortSource
	return store.Thread{
		ConvID: conv, ThreadID: s.CodexThreadID, Cwd: s.Cwd, CodexHome: s.CodexHome, Profile: s.Profile,
		Model: firstNonEmpty(s.ModelProfile.Model, s.Model, s.ModelProfile.Name), ModelSource: s.ModelSelectionSource,
		Effort: s.ReasoningEffort, EffortSource: effortSrc, PendingModel: firstNonEmpty(s.PendingModelProfile.Model, s.PendingModelProfile.Name),
		PendingEffort: s.PendingReasoningEffort, Sandbox: s.Sandbox,
	}
}

func firstNonZero(v ...int64) int64 {
	for _, x := range v {
		if x != 0 {
			return x
		}
	}
	return 0
}

func (c *converter) convertForks() error {
	e := c.rep.E("fork_operations")
	return c.l.each("fork_operations", func(r row) error {
		e.Source++
		var f legacyFork
		if _, err := decode(r.JSON, &f); err != nil {
			e.Failed = append(e.Failed, r.ID)
			e.Outcomes["unparsable -> skipped"]++
			return nil
		}
		if completedFork(f.Phase) {
			e.Outcomes["completed -> fork_parent edge"]++
			return nil
		}
		parent, ok := c.convOf(f.ParentSessionID, "")
		if !ok {
			e.Outcomes["parent unknown -> skipped"]++
			return nil
		}
		child, _ := c.convOf(f.ChildSessionID, "")
		if _, err := c.im.Fork(store.Fork{ParentConv: parent, ChildConv: child, Phase: "hold", ChildThreadID: f.ChildThreadID, CreatedAt: firstNonZero(ms(f.CreatedAt), c.ms())}); err != nil {
			return err
		}
		e.Imported++
		e.Outcomes[f.Phase+" -> hold"]++
		return nil
	})
}

func terminalTurn(s string) bool { return s == "completed" || s == "failed" || s == "interrupted" }

// scanTurns indexes every turn. All of them are imported (no retention, §6):
// queued stays queued, running becomes interrupted, terminal rows keep their
// state, timestamps and model.
func (c *converter) scanTurns() error {
	e := c.rep.E("turns")
	err := c.l.each("turns", func(r row) error {
		e.Source++
		var t legacyTurn
		if _, err := decode(r.JSON, &t); err != nil {
			e.Failed = append(e.Failed, r.ID)
			e.Outcomes["unparsable -> skipped"]++
			return nil
		}
		if t.ID == "" {
			t.ID = r.ID
		}
		conv, ok := c.convOf(t.SessionID, "")
		ct := &compactTurn{t: t, conv: conv, order: firstNonZero(ms(t.QueuedAt), ms(t.CreatedAt))}
		c.turns[t.ID] = ct
		if !ok {
			e.Outcomes["session unknown -> dropped"]++
			return nil
		}
		ct.keep = true
		switch t.Status {
		case "queued":
			ct.newStat = store.TurnQueued
		case "running":
			ct.newStat = store.TurnInterrupted
		default:
			ct.newStat = t.Status
			if !terminalTurn(t.Status) {
				ct.newStat = store.TurnFailed
			}
		}
		return nil
	})
	return err
}

// inboundDisposition maps a legacy inbound row (§7.2).
func (c *converter) inboundDisposition(in legacyInbound, conv int64) (state, outcome string) {
	var turn *compactTurn
	if in.TurnID != "" {
		turn = c.turns[in.TurnID]
	}
	switch in.Status {
	case "ignored":
		return store.InIgnored, "ignored"
	case "manual_hold":
		if conv == c.control {
			return store.InIgnored, "manual_hold (control) -> ignored"
		}
		return store.InHeld, "manual_hold -> held"
	case "uncertain":
		return store.InHeld, "uncertain -> held"
	case "persisted", "queued", "deferred":
		if turn != nil {
			if turn.t.Status == "queued" {
				return store.InQueued, in.Status + " with an open turn -> queued"
			}
			return store.InDone, in.Status + " with a terminal turn -> done"
		}
		if in.TurnID != "" {
			return store.InDone, in.Status + " with a missing turn -> done"
		}
		if in.Status == "persisted" {
			if in.Source == "registry_migration" {
				return store.InDone, "registry_migration -> done"
			}
			return store.InDone, "persisted without a turn -> done"
		}
		// queued/deferred and never run: queued, in order (a prompt gets its
		// queued turn exactly as admission would create it).
		return store.InQueued, in.Status + " without a turn -> queued"
	}
	return store.InHeld, "status " + in.Status + " -> held"
}

// extTime derives a message time: Teams message ids are creation times in
// unix ms; otherwise the receive time.
func extTime(msgID string, fallback int64) int64 {
	if n, err := strconv.ParseInt(msgID, 10, 64); err == nil && n > 1_000_000_000_000 && n < 10_000_000_000_000 {
		return n
	}
	return fallback
}

func (c *converter) convertInbound() error {
	e := c.rep.E("inbound_events")
	var orphanRows []legacyInbound
	err := c.l.each("inbound_events", func(r row) error {
		e.Source++
		var in legacyInbound
		if _, err := decode(r.JSON, &in); err != nil {
			e.Failed = append(e.Failed, r.ID)
			in = legacyInbound{ID: r.ID, SessionID: r.Cols["session_id"], TeamsChatID: r.Cols["teams_chat_id"], TeamsMessageID: r.Cols["teams_message_id"], Status: r.Status}
			if in.Status != "ignored" && in.Status != "persisted" {
				in.Status = "uncertain" // non-terminal and unreadable: hold
			}
		}
		conv, ok := c.convOf(in.SessionID, in.TeamsChatID)
		if !ok {
			orphanRows = append(orphanRows, in)
			return nil
		}
		return c.importInbound(e, in, conv, "")
	})
	if err != nil {
		return err
	}
	// Rows of chats that map to no session: no data loss, so they go into
	// placeholder conversations (closed, never polled), one per chat.
	if err := c.createOrphans(orphanRows); err != nil {
		return err
	}
	for _, in := range orphanRows {
		if err := c.importInbound(e, in, c.orphanOf(in), "orphan chat: "); err != nil {
			return err
		}
	}
	return c.registrySeen()
}

// orphanKey identifies the placeholder conversation of a row whose chat is
// unknown: its chat id, or its session id when it has no chat.
func orphanKey(chat, session string) string {
	if chat != "" {
		return chat
	}
	return "session:" + session
}

func (c *converter) orphanOf(in legacyInbound) int64 {
	return c.orphans[orphanKey(in.TeamsChatID, in.SessionID)]
}

// createOrphans creates one closed placeholder conversation per unknown chat
// (inbound rows and registry seen ids), labelled orphan-<n> in chat order.
func (c *converter) createOrphans(rows []legacyInbound) error {
	keys := map[string]bool{}
	for _, in := range rows {
		keys[orphanKey(in.TeamsChatID, in.SessionID)] = true
	}
	for _, chat := range sortedKeys(c.opt.registry.Chats) {
		if _, known := c.convByChat[chat]; !known && len(c.opt.registry.Chats[chat].SeenMessageIDs) > 0 {
			keys[chat] = true
		}
	}
	e := c.rep.E("orphan conversations")
	for i, k := range sortedKeys(keys) {
		chat := k
		if strings.HasPrefix(k, "session:") {
			chat = ""
		}
		id, err := c.im.Conversation(store.Conversation{Label: fmt.Sprintf("orphan-%d", i+1), Kind: store.KindWork, Channel: channel,
			ExtChatID: chat, State: store.ConvClosed, CreatedAt: c.ms()})
		if err != nil {
			return err
		}
		c.orphans[k] = id
		c.labelOf[id] = fmt.Sprintf("orphan-%d", i+1)
		e.Source++
		e.Imported++
		e.Outcomes["closed placeholder (never polled)"]++
	}
	return nil
}

func (c *converter) importInbound(e *Entity, in legacyInbound, conv int64, prefix string) error {
	state, outcome := c.inboundDisposition(in, conv)
	outcome = prefix + outcome
	ext := firstNonEmpty(in.TeamsMessageID, "legacy:"+in.ID)
	kind := store.InPrompt
	if conv == c.control {
		kind = store.InCommand
	}
	sender := store.SenderOwner
	if state == store.InIgnored {
		sender = store.SenderOther
	}
	body := in.Text // kept for every row (§6: nothing is blanked)
	recv := firstNonZero(ms(in.ReceivedAt), ms(in.CreatedAt), c.ms())
	row := store.Inbound{ConvID: conv, ExtMsgID: ext, ExtTime: extTime(in.TeamsMessageID, recv), ReceivedAt: recv,
		Sender: sender, Kind: kind, State: state, Body: body}
	id, ok, err := c.im.Inbound(row)
	if err != nil {
		return fmt.Errorf("inbound %s: %w", in.ID, err)
	}
	if !ok {
		e.Outcomes[prefix+"duplicate message id -> merged"]++
		return nil
	}
	e.Imported++
	e.Outcomes[outcome]++
	if state == store.InHeld && conv != c.control && prefix == "" {
		if si := c.sessionOfConv(conv); si == nil || !si.paused {
			c.needPause[conv] = "migrated-held-messages"
		}
	}
	if old, seen := c.oldestIn[conv]; !seen || row.ExtTime < old {
		c.oldestIn[conv] = row.ExtTime
	}
	if t := c.turns[in.TurnID]; t != nil && t.keep && t.conv == conv {
		c.inboundOld[in.ID] = id
	} else if state == store.InQueued && kind == store.InPrompt && prefix == "" {
		key := "inbound:" + in.ID
		c.turns[key] = &compactTurn{t: legacyTurn{ID: key, SessionID: in.SessionID, InboundEventID: in.ID, Status: "queued"},
			conv: conv, order: row.ExtTime, keep: true, newStat: store.TurnQueued}
		c.inboundOld[in.ID] = id
	}
	c.touch(conv, row.ExtTime)
	return nil
}

func (c *converter) sessionOfConv(conv int64) *sessInfo {
	if si, ok := c.sessions[c.labelOf[conv]]; ok {
		return si
	}
	return nil
}

// registrySeen imports registry seen message ids for chats that have no
// inbound rows (dedupe only).
func (c *converter) registrySeen() error {
	e := c.rep.E("registry seen ids")
	for _, chat := range sortedKeys(c.opt.registry.Chats) {
		ids := c.opt.registry.Chats[chat].SeenMessageIDs
		e.Source += len(ids)
		conv, ok := c.convByChat[chat]
		prefix := ""
		if !ok {
			conv, prefix = c.orphans[chat], "orphan chat: "
		}
		if _, has := c.oldestIn[conv]; has {
			e.Outcomes[prefix+"chat has inbound rows -> covered"] += len(ids)
			continue
		}
		for _, id := range ids {
			t := extTime(id, c.ms())
			_, ok, err := c.im.Inbound(store.Inbound{ConvID: conv, ExtMsgID: id, ExtTime: t, ReceivedAt: t, Sender: store.SenderOther, Kind: store.InPrompt, State: store.InDone})
			if err != nil {
				return err
			}
			if ok {
				e.Imported++
				e.Outcomes[prefix+"imported (done)"]++
			}
		}
	}
	return nil
}

func (c *converter) convertTurns() error {
	e := c.rep.E("turns")
	ids := make([]string, 0, len(c.turns))
	for id, t := range c.turns {
		if t.keep {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := c.turns[ids[i]], c.turns[ids[j]]
		if a.order != b.order {
			return a.order < b.order
		}
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		ct := c.turns[id]
		t := ct.t
		inbound := c.inboundOld[t.InboundEventID]
		state := ct.newStat
		if state == store.TurnQueued && inbound == 0 {
			state = store.TurnNotStarted
			c.rep.note("turn %s was queued but its prompt row is missing: not_started", t.ID)
		}
		tr := store.Turn{
			ConvID: ct.conv, InboundID: inbound, TurnKey: LegacyTurnKey(t.ID), State: state, ThreadID: t.CodexThreadID,
			CodexTurnID: t.CodexTurnID, Model: firstNonEmpty(t.ModelProfile.Model, t.ModelProfile.Name), Effort: t.ReasoningEffort,
			QueuedAt: firstNonZero(ct.order, c.ms()), StartedAt: ms(t.StartedAt),
			FinishedAt: firstNonZero(ms(t.CompletedAt), ms(t.FailedAt), ms(t.InterruptedAt)), Error: t.FailureMessage,
		}
		if state == store.TurnInterrupted && t.Status == "running" {
			tr.FinishedAt, tr.Error = c.ms(), "interrupted by the migration to the core store"
		}
		if state != store.TurnQueued && tr.FinishedAt == 0 {
			tr.FinishedAt = tr.QueuedAt
		}
		nid, err := c.im.Turn(tr)
		if err != nil {
			return err
		}
		ct.newID = nid
		c.touch(ct.conv, firstNonZero(tr.FinishedAt, tr.StartedAt, tr.QueuedAt))
		if inbound != 0 {
			if err := c.im.LinkInbound(inbound, nid); err != nil {
				return err
			}
		}
		switch {
		case strings.HasPrefix(t.ID, "inbound:"):
			c.rep.E("inbound_events").Outcomes["queued turn created for a never-run prompt"]++
			continue
		case t.Status == "running":
			e.Outcomes["running -> interrupted (+notice)"]++
		default:
			e.Outcomes[firstNonEmpty(t.Status, "(empty)")+" -> "+state]++
		}
		e.Imported++
	}
	return nil
}

func (c *converter) convertStreams() error {
	e := c.rep.E("streams")
	e.Source = len(c.polls)
	done := map[int64]bool{}
	for _, chat := range sortedKeys(c.polls) {
		p := c.polls[chat]
		conv, ok := c.convByChat[chat]
		if !ok {
			e.Outcomes["chat unknown -> dropped"]++
			continue
		}
		var cur time.Time
		src := ""
		switch {
		case p.Gap != nil && !p.Gap.SafeCursor.IsZero():
			cur, src = p.Gap.SafeCursor.Time, "gap.safe_cursor"
		case p.ContinuationPath != "" && bool(p.ContinuationSafeCursorKnown) && !p.ContinuationSafeCursor.IsZero():
			cur, src = p.ContinuationSafeCursor.Time, "continuation_safe_cursor"
		default:
			cur, src = p.LastModifiedCursor.Time, "last_modified_cursor"
		}
		seeded := bool(p.Seeded) && !cur.IsZero()
		if !seeded {
			if oldest, has := c.oldestIn[conv]; has {
				cur, src, seeded = time.UnixMilli(oldest-1000), "unseeded: oldest inbound - 1s", true
			}
		}
		if p.PendingPage != nil && len(p.PendingPage.Records) > 0 {
			e.Outcomes["pending_page records (re-polled from the cursor)"] += len(p.PendingPage.Records)
		}
		if err := c.im.Stream(store.Stream{Key: StreamKey(chat), ConvID: conv, Cursor: cursor(cur), Seeded: seeded}); err != nil {
			return err
		}
		done[conv] = true
		e.Imported++
		e.Outcomes["cursor from "+src]++
	}
	// Initialised chats without a poll row: never rebaseline at the head
	// (hazard: rebaseline trap); recover from the oldest known inbound row.
	for _, label := range sortedKeys(c.sessions) {
		si := c.sessions[label]
		if si.conv == 0 || done[si.conv] || si.s.TeamsChatID == "" || c.convByChat[si.s.TeamsChatID] != si.conv {
			continue
		}
		oldest, has := c.oldestIn[si.conv]
		if !has && !bool(si.s.PollFrontierInitialized) {
			continue
		}
		var cur string
		if has {
			cur = cursor(time.UnixMilli(oldest - 1000))
		} else {
			c.rep.note("session %s: frontier initialised but no poll row and no inbound rows; stream left unseeded", label)
			continue
		}
		if err := c.im.Stream(store.Stream{Key: StreamKey(si.s.TeamsChatID), ConvID: si.conv, Cursor: cur, Seeded: true}); err != nil {
			return err
		}
		done[si.conv] = true
		e.Imported++
		e.Outcomes["no poll row: oldest inbound - 1s"]++
	}
	return nil
}

// outboxKind maps a legacy outbox kind to the new kind set.
func outboxKind(k string) string {
	switch {
	case k == "ack", strings.HasPrefix(k, "queued-status"), strings.HasPrefix(k, "queued-wait"):
		return store.OutAck
	case strings.HasPrefix(k, "codex-progress"), strings.HasPrefix(k, "codex-compact"), strings.HasPrefix(k, "codex-status"), k == "anchor":
		return store.OutProgress
	case k == "final":
		return store.OutFinal
	case strings.HasPrefix(k, "helper"), strings.HasPrefix(k, "control-"):
		return store.OutControl
	case k == "publish-full-title", k == "import-title":
		return store.OutTitle
	case strings.HasPrefix(k, "sync-"), strings.HasPrefix(k, "import-"), strings.HasPrefix(k, "publish-full-"),
		strings.HasPrefix(k, "fork-history-"), k == "local-session-started":
		return store.OutMirror
	}
	return store.OutNotice
}

type compactOut struct {
	o      legacyOutbox
	conv   int64
	state  string
	opaque bool
	stale  bool
}

func (c *converter) convertOutbox() error {
	e := c.rep.E("outbox_messages")
	byConv := map[int64][]*compactOut{}
	err := c.l.each("outbox_messages", func(r row) error {
		e.Source++
		var o legacyOutbox
		opaque := false
		if _, err := decode(r.JSON, &o); err != nil {
			// Unparsable: keep the key from the columns. A terminal row stays
			// terminal (its key still dedupes); anything else is unknown.
			e.Failed = append(e.Failed, r.ID)
			status := r.Status
			if status != "sent" && status != "skipped" {
				status = "sending"
			}
			o = legacyOutbox{ID: r.ID, SessionID: r.Cols["session_id"], TeamsChatID: r.Cols["teams_chat_id"], TeamsMessageID: r.Cols["teams_message_id"], Status: status}
			opaque = true
		}
		if o.ID == "" {
			o.ID = r.ID
		}
		unknown := o.unknownOutcome()
		// An unsent row older than Options.StaleUnsent is not sent (or resolved
		// and possibly re-sent) weeks later: it is dropped and counted in
		// the summary. An unparsable row has no age and stays unknown.
		created := ms(o.CreatedAt)
		stale := !opaque && o.Status != "sent" && o.Status != "skipped" && created > 0 &&
			c.opt.StaleUnsent > 0 && created < c.ms()-c.opt.StaleUnsent.Milliseconds()
		conv, ok := c.convOf(o.SessionID, o.TeamsChatID)
		if !ok && o.SessionID == "" && (o.TeamsChatID == "" || o.TeamsChatID == c.controlChat) {
			conv, ok = c.control, c.control != 0
		}
		if !ok {
			if (unknown || o.Status == "queued") && !stale {
				return fmt.Errorf("outbox %s (%s) has no conversation", o.ID, o.Status)
			}
			e.Outcomes["chat unknown -> dropped"]++
			return nil
		}
		st := ""
		switch {
		case stale:
			st = store.OutDropped
			c.staleUnsent++
		case unknown:
			st = store.OutUnknown
		case o.Status == "queued":
			st = store.OutQueued
		case o.Status == "sent":
			st = store.OutSent
		case o.Status == "skipped":
			st = store.OutDropped
		default:
			st = store.OutUnknown
		}
		if st == store.OutUnknown {
			c.unknownSrc++
		}
		byConv[conv] = append(byConv[conv], &compactOut{o: o, conv: conv, state: st, opaque: opaque, stale: stale})
		return nil
	})
	if err != nil {
		return err
	}
	convs := make([]int64, 0, len(byConv))
	for id := range byConv {
		convs = append(convs, id)
	}
	sort.Slice(convs, func(i, j int) bool { return convs[i] < convs[j] })
	for _, conv := range convs {
		rows := byConv[conv]
		sort.SliceStable(rows, func(i, j int) bool {
			a, b := rows[i].o, rows[j].o
			if ma, mb := ms(a.CreatedAt), ms(b.CreatedAt); ma != mb {
				return ma < mb
			}
			if a.Sequence != b.Sequence {
				return a.Sequence < b.Sequence
			}
			if a.PartIndex != b.PartIndex {
				return a.PartIndex < b.PartIndex
			}
			return a.ID < b.ID
		})
		var seq int64
		for _, co := range rows {
			o := co.o
			seq++
			row := store.Outbox{
				ConvID: conv, DeliveryKey: c.mirrorKeyOr(c.outboxMirror[o.ID], "legacy-outbox:"+o.ID), Seq: seq, Kind: outboxKind(o.Kind),
				Part: o.PartIndex, Parts: o.PartCount, Body: o.Body, Mention: bool(o.MentionOwner), ReplyTo: o.QuoteReplyToMessageID,
				State: co.state, ExtMsgID: o.TeamsMessageID, CreatedAt: firstNonZero(ms(o.CreatedAt), c.ms()), SentAt: ms(o.SentAt),
				BodyHash: renderedHash(o.RenderedHash),
			}
			if row.BodyHash != nil || row.State == store.OutQueued || row.State == store.OutUnknown {
				// Re-hashed (sent rows) or rendered from the legacy text
				// (unsent rows) by the core at first start.
				row.HashAlgo = store.HashLegacy
			}
			switch {
			case co.opaque && co.state != store.OutUnknown:
				row.Note = "migrated: unparsable legacy row (" + o.Status + "); key kept"
			case co.stale:
				row.Note = "migrated: unsent for more than " + c.opt.StaleUnsent.String() + " before the conversion (" + o.Status + "); not sent"
			case co.state == store.OutDropped:
				row.Note = "legacy skipped: " + firstNonEmpty(o.LastSendError, "(no reason recorded)")
			}
			if co.state == store.OutUnknown || co.state == store.OutQueued {
				row.AttemptToken = o.SendAttemptToken
				row.NextAttemptAt = ms(o.NextAttemptAt)
				if co.state == store.OutUnknown {
					row.Note = "migrated: send outcome unknown (" + o.Status + ")"
					if co.opaque {
						row.Note = "migrated: unparsable legacy row; resolve manually"
					}
				}
			}
			if row.Part <= 0 {
				row.Part = 1
			}
			if row.Parts < row.Part {
				row.Parts = row.Part
			}
			var f *store.OutFile
			if o.AttachmentPath != "" && (co.state == store.OutQueued || co.state == store.OutUnknown) {
				f = &store.OutFile{LocalPath: o.AttachmentPath, Size: o.AttachmentSize, UploadedBytes: o.AttachmentUploadOffset, ExtItemID: o.DriveItemID}
				if exp := ms(o.AttachmentUploadExpiry); o.AttachmentUploadURL != "" && exp > c.ms() {
					f.UploadURL, f.UploadExpiresAt = o.AttachmentUploadURL, exp
				} else {
					f.UploadedBytes = 0
				}
				c.rep.E("outbox files").Imported++
			}
			if _, ok, err := c.im.Outbox(row, f); err != nil {
				return err
			} else if !ok {
				return fmt.Errorf("duplicate outbox key %s", row.DeliveryKey)
			}
			e.Imported++
			if co.stale {
				e.Outcomes[o.Status+" -> dropped (stale)"]++
			} else {
				e.Outcomes[o.Status+" -> "+co.state]++
			}
			if row.State == store.OutSent {
				c.touch(conv, firstNonZero(row.SentAt, row.CreatedAt))
			}
			if o.TeamsMessageID != "" {
				c.markExt(conv, o.TeamsMessageID)
			}
		}
	}
	// One interruption notice per turn that was running (§7.2).
	for _, id := range sortedKeys(c.turns) {
		ct := c.turns[id]
		if ct.t.Status != "running" || ct.conv == 0 {
			continue
		}
		seq, err := c.im.MaxSeq(ct.conv)
		if err != nil {
			return err
		}
		if _, _, err := c.im.Outbox(store.Outbox{ConvID: ct.conv, DeliveryKey: "legacy-interrupted:" + id, Seq: seq + 1, Kind: store.OutNotice, HashAlgo: store.HashLegacy,
			Body:  "The previous request was interrupted when the helper stopped. It was not re-run; resend it if you still need it.",
			State: store.OutQueued, CreatedAt: c.ms()}, nil); err != nil {
			return err
		}
		c.rep.E("outbox_messages").Outcomes["interruption notice added"]++
	}
	return nil
}

// renderedHash decodes the legacy rendered_hash (hex sha256 of the rendered
// text) into body_hash; anything else is left NULL.
func renderedHash(h string) []byte {
	b, err := hex.DecodeString(strings.TrimSpace(h))
	if err != nil || len(b) != sha256.Size {
		return nil
	}
	return b
}

func (c *converter) markExt(conv int64, ext string) {
	m := c.extSeen[conv]
	if m == nil {
		m = map[string]bool{}
		c.extSeen[conv] = m
	}
	m[ext] = true
}

// convertDeliveries imports delivered-message ids that are not already in
// the outbox as sent mirror rows (keys and ext ids only, for echo/dedupe).
func (c *converter) convertDeliveries() error {
	for _, ent := range []string{"transcript_deliveries", "helper_deliveries"} {
		e := c.rep.E(ent)
		err := c.l.each(ent, func(r row) error {
			e.Source++
			var d legacyDelivery
			if _, err := decode(r.JSON, &d); err != nil {
				e.Failed = append(e.Failed, r.ID)
				return nil
			}
			if d.ID == "" {
				d.ID = r.ID
			}
			if d.Status != "sent" || d.TeamsMessageID == "" {
				e.Outcomes["not sent -> dropped"]++
				return nil
			}
			conv, ok := c.convOf(d.SessionID, d.TeamsChatID)
			if !ok {
				e.Outcomes["chat unknown -> dropped"]++
				return nil
			}
			if c.extSeen[conv][d.TeamsMessageID] {
				e.Outcomes["covered by outbox"]++
				return nil
			}
			key := "legacy-delivery:" + d.ID
			if ent == "transcript_deliveries" && d.CodexThreadID != "" && codexRecordID(d.SourceRecordID) {
				key = c.mirrorKeyOr(MirrorKey(d.CodexThreadID, d.SourceRecordID, max(d.PartIndex, 1)), key)
			}
			if err := c.sentMirror(conv, key, d.TeamsMessageID, firstNonZero(ms(d.SentAt), ms(d.CreatedAt), c.ms())); err != nil {
				return err
			}
			e.Imported++
			e.Outcomes["sent -> sent mirror key"]++
			return nil
		})
		if err != nil {
			return err
		}
	}
	e := c.rep.E("message_provenance")
	return c.l.each("message_provenance", func(r row) error {
		e.Source++
		var p struct {
			TeamsChatID    string `json:"teams_chat_id"`
			TeamsMessageID string `json:"teams_message_id"`
			Origin         string `json:"origin"`
			SessionID      string `json:"session_id"`
			CreatedAt      ltime  `json:"created_at"`
		}
		if _, err := decode(r.JSON, &p); err != nil {
			e.Failed = append(e.Failed, r.ID)
			return nil
		}
		if p.Origin != "helper_outbox" {
			e.Outcomes[p.Origin+" -> covered by inbound"]++
			return nil
		}
		conv, ok := c.convOf(p.SessionID, p.TeamsChatID)
		if !ok || p.TeamsMessageID == "" {
			e.Outcomes["chat unknown -> dropped"]++
			return nil
		}
		if c.extSeen[conv][p.TeamsMessageID] {
			e.Outcomes["covered by outbox"]++
			return nil
		}
		if err := c.sentMirror(conv, "legacy-provenance:"+r.ID, p.TeamsMessageID, firstNonZero(ms(p.CreatedAt), c.ms())); err != nil {
			return err
		}
		e.Imported++
		e.Outcomes["helper_outbox -> sent mirror key"]++
		return nil
	})
}

// mirrorKeyOr returns the mirror key (proof.go) when it is set and not yet
// taken, else the legacy key. A legacy row whose record identity is known
// thus dedupes against the new mirror per record.
func (c *converter) mirrorKeyOr(mirror, legacy string) string {
	if mirror == "" || c.mirrorUsed[mirror] {
		return legacy
	}
	c.mirrorUsed[mirror] = true
	c.rep.E("mirror keys").Imported++
	return mirror
}

func (c *converter) sentMirror(conv int64, key, ext string, at int64) error {
	seq, err := c.im.MaxSeq(conv)
	if err != nil {
		return err
	}
	_, ok, err := c.im.Outbox(store.Outbox{ConvID: conv, DeliveryKey: key, Seq: seq + 1, Kind: store.OutMirror, State: store.OutSent,
		ExtMsgID: ext, CreatedAt: at, SentAt: at}, nil)
	if err == nil && ok {
		c.markExt(conv, ext)
	}
	return err
}

func (c *converter) convertNotifications() error {
	e := c.rep.E("notifications")
	return c.l.each("notifications", func(r row) error {
		e.Source++
		var n legacyNotification
		if _, err := decode(r.JSON, &n); err != nil {
			e.Failed = append(e.Failed, r.ID)
			n = legacyNotification{ID: r.ID, Status: "unparsable"}
		}
		if n.ID == "" {
			n.ID = r.ID
		}
		// Every row is kept (§6). Cards are at-most-once, so anything that
		// is not queued or sent becomes dropped and is never re-sent. Most
		// rows are turn_completed cards: with a card available the old
		// helper suppressed the owner @mention on the final, and the core
		// keeps that rule (meta workflow_enabled).
		st := store.OutDropped
		switch n.Status {
		case "queued":
			st = store.OutQueued
		case "sent":
			st = store.OutSent
		}
		payload := strings.TrimSpace(strings.Join([]string{n.Title, n.ChatTitle, n.RequestSummary, n.ButtonURL}, "\n"))
		if err := c.im.Notification(store.Notification{Key: "legacy:" + n.ID, Kind: firstNonEmpty(n.Kind, "unknown"), Payload: payload, State: st,
			CreatedAt: firstNonZero(ms(n.CreatedAt), c.ms()), SentAt: ms(n.SentAt)}); err != nil {
			return err
		}
		e.Imported++
		e.Outcomes[firstNonEmpty(n.Kind, "(no kind)")+" "+firstNonEmpty(n.Status, "(empty)")+" -> "+st]++
		return nil
	})
}

func (c *converter) countArtifacts() error {
	e := c.rep.E("artifact_records")
	return c.l.each("artifact_records", func(r row) error {
		e.Source++
		var a legacyArtifact
		if _, err := decode(r.JSON, &a); err != nil {
			e.Failed = append(e.Failed, r.ID)
			return nil
		}
		// Upload state lives on the outbox row (outbox_files); the records
		// themselves are history.
		e.Outcomes[firstNonEmpty(a.Status, "(empty)")+" -> dropped (upload state kept on outbox rows)"]++
		return nil
	})
}

// maxImportedGate bounds a chat or account gate carried over from the old
// store.
const maxImportedGate = time.Hour

func (c *converter) convertGates() error {
	e := c.rep.E("chat_rate_limits")
	return c.l.each("chat_rate_limits", func(r row) error {
		e.Source++
		var g legacyRateLimit
		if _, err := decode(r.JSON, &g); err != nil {
			e.Failed = append(e.Failed, r.ID)
			return nil
		}
		chat := firstNonEmpty(g.ChatID, r.ID)
		until := ms(g.BlockedUntil)
		if until <= c.ms() {
			e.Outcomes["expired -> dropped"]++
			return nil
		}
		// Graph's Retry-After is seconds to minutes: a longer block is
		// stale state and would hold the chat's messages indefinitely.
		if limit := c.ms() + maxImportedGate.Milliseconds(); until > limit {
			until = limit
			e.Outcomes["capped to "+maxImportedGate.String()]++
		}
		// Keys the engine reads (store/keys.go): the legacy account-wide
		// Graph write gate becomes the account write gate, a chat gate
		// delays that chat.
		keys := []string{store.ChatGate(channel, chat)}
		if chat == accountGraphGateKey {
			keys = []string{store.AccountWriteGate(channel)}
		}
		for _, key := range keys {
			if err := c.im.Gate(store.Gate{Key: key, Until: until, Reason: firstNonEmpty(g.Reason, "migrated Retry-After")}); err != nil {
				return err
			}
		}
		e.Imported++
		return nil
	})
}

func (c *converter) convertMeta() error {
	var sc legacyServiceControl
	_, _ = decode(c.l.section("service_control"), &sc)
	var au legacyAutoUpdate
	_, _ = decode(c.l.section("auto_update"), &au)
	kv := map[string]string{
		store.MetaMigratedFrom: string(c.l.version),
		"migrated_at":          strconv.FormatInt(c.ms(), 10),
		"migrated_source_dir":  c.l.dir,
	}
	if c.scopeID != "" {
		kv["scope_id"] = c.scopeID
		scope8 := strings.TrimPrefix(c.scopeID, "scope:")
		if len(scope8) > 8 {
			scope8 = scope8[:8]
		}
		kv[store.MetaTurnKeyPrefix] = "cxp-" + scope8
	}
	if sc.Paused {
		kv["paused"] = "1"
		if sc.Reason != "" {
			kv["paused_reason"] = sc.Reason
		}
	}
	if au.LastInstalledTag != "" {
		kv["last_installed_tag"] = au.LastInstalledTag
	}
	if c.opt.registry.MachineHostnameOverride != "" {
		kv["machine_label"] = c.opt.registry.MachineHostnameOverride
	}
	// The registry machine id: the new core keeps it, so its card, inbox
	// chat and the requesters' routes stay the same (internal/core/delegate).
	var mid struct {
		ID string `json:"id"`
	}
	if _, err := decode(c.l.section("machine_identity"), &mid); err == nil && strings.TrimSpace(mid.ID) != "" {
		kv["machine_id"] = strings.TrimSpace(mid.ID)
	}
	for _, k := range sortedKeys(kv) {
		if err := c.im.SetMeta(k, kv[k]); err != nil {
			return err
		}
	}
	return nil
}
