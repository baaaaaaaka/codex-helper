package store

import (
	"context"
	"fmt"
)

// NewConversation describes a conversation to create.
type NewConversation struct {
	Label       string
	Kind        string
	Channel     string
	ExtChatID   string // optional; when set the conversation starts active
	ExtURL      string
	Title       string
	TitleSource string
}

// CreateConversation inserts a conversation (state creating, or active when
// the chat already exists).
func (t *Tx) CreateConversation(c NewConversation) (int64, error) {
	state := ConvCreating
	if c.ExtChatID != "" {
		state = ConvActive
	}
	res, err := t.exec(`INSERT INTO conversations(label,kind,channel,ext_chat_id,ext_url,title,title_source,state,created_at,updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?)`, c.Label, c.Kind, c.Channel, nullStr(c.ExtChatID), nullStr(c.ExtURL),
		nullStr(c.Title), nullStr(c.TitleSource), state, t.now, t.now)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// Conversation reads one conversation.
func (t *Tx) Conversation(id int64) (Conversation, bool, error) {
	return one(t.row(`SELECT `+convCols+` FROM conversations WHERE id = ?`, id), scanConv)
}

// ConversationByChat finds a conversation by its channel chat id.
func (t *Tx) ConversationByChat(channel, extChatID string) (Conversation, bool, error) {
	return one(t.row(`SELECT `+convCols+` FROM conversations WHERE channel = ? AND ext_chat_id = ?`, channel, extChatID), scanConv)
}

// SetChat records the created chat and activates a creating conversation.
func (t *Tx) SetChat(convID int64, extChatID, url string) error {
	return t.convUpdate(convID, `ext_chat_id = ?, ext_url = ?, state = CASE state WHEN 'creating' THEN 'active' ELSE state END`,
		nil, extChatID, nullStr(url))
}

// Park stops receive polling for an active conversation (N-10).
func (t *Tx) Park(convID int64) error {
	return t.convUpdate(convID, `state = 'parked', parked_at = ?`, []string{ConvActive}, t.now)
}

// Wake reactivates a parked conversation.
func (t *Tx) Wake(convID int64) error {
	return t.convUpdate(convID, `state = 'active', parked_at = NULL`, []string{ConvParked})
}

// Reactivate returns a held conversation to active (`cxp teams chat
// recreate --activate`, the v0.1.20 reactivation of a quarantined session).
func (t *Tx) Reactivate(convID int64) error {
	return t.convUpdate(convID, `state = 'active', parked_at = NULL`, []string{ConvHold})
}

// Close closes a conversation.
func (t *Tx) Close(convID int64) error {
	return t.convUpdate(convID, `state = 'closed'`, []string{ConvCreating, ConvActive, ConvParked, ConvHold})
}

// Pause sets the per-chat breaker: new prompts are held and no turn starts.
func (t *Tx) Pause(convID int64, reason string) error {
	return t.convUpdate(convID, `paused_at = COALESCE(paused_at, ?), paused_reason = ?, paused_notified = 0`, nil, t.now, reason)
}

// MarkPauseNotified records that the one pause notice was enqueued.
func (t *Tx) MarkPauseNotified(convID int64) error {
	return t.convUpdate(convID, `paused_notified = 1`, nil)
}

// Unpause clears the breaker and releases held prompts in order, creating
// one queued turn each. It returns the number of released prompts.
func (t *Tx) Unpause(convID int64) (int, error) {
	if err := t.convUpdate(convID, `paused_at = NULL, paused_reason = NULL, paused_notified = 0`, nil); err != nil {
		return 0, err
	}
	held, err := collect(scanInbound)(t.rows(`SELECT `+inboundCols+` FROM inbound
WHERE conv_id = ? AND state IN ('queued','held') ORDER BY ext_time, id`, convID))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, in := range held {
		if in.State != InHeld {
			continue
		}
		turnID := int64(0)
		if in.Kind == InPrompt {
			if turnID, err = t.newTurn(convID, in.ID); err != nil {
				return n, err
			}
		}
		if _, err := t.exec(`UPDATE inbound SET state = 'queued', turn_id = ? WHERE id = ?`, nullInt(turnID), in.ID); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// convUpdate applies set to one conversation, optionally requiring one of
// the given current states. args are bound before the id.
func (t *Tx) convUpdate(convID int64, set string, from []string, args ...any) error {
	q := `UPDATE conversations SET ` + set + `, updated_at = ? WHERE id = ?`
	args = append(args, t.now, convID)
	if len(from) > 0 {
		q += ` AND state IN (` + placeholders(len(from)) + `)`
		for _, s := range from {
			args = append(args, s)
		}
	}
	n, err := affected(t.exec(q, args...))
	if err != nil {
		return err
	}
	if n == 0 {
		c, ok, err := t.Conversation(convID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: conversation %d", ErrNotFound, convID)
		}
		return fmt.Errorf("%w: conversation %d is %s", ErrState, convID, c.State)
	}
	return nil
}

func placeholders(n int) string {
	s := "?"
	for i := 1; i < n; i++ {
		s += ",?"
	}
	return s
}

// Bind writes the Codex binding of a conversation. It is called for explicit
// new/fork and by migration; a restart never changes it.
func (t *Tx) Bind(th Thread) error {
	_, err := t.exec(`INSERT INTO threads(`+threadCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(conv_id) DO UPDATE SET thread_id=excluded.thread_id, cwd=excluded.cwd, codex_home=excluded.codex_home,
  profile=excluded.profile, model=excluded.model, model_source=excluded.model_source, effort=excluded.effort,
  effort_source=excluded.effort_source, pending_model=excluded.pending_model, pending_effort=excluded.pending_effort,
  sandbox=excluded.sandbox, model_pin=excluded.model_pin, fork_parent=excluded.fork_parent, updated_at=excluded.updated_at`,
		th.ConvID, nullStr(th.ThreadID), th.Cwd, nullStr(th.CodexHome), nullStr(th.Profile), nullStr(th.Model),
		nullStr(th.ModelSource), nullStr(th.Effort), nullStr(th.EffortSource), nullStr(th.PendingModel),
		nullStr(th.PendingEffort), nullStr(th.Sandbox), nullStr(th.ModelPin), nullInt(th.ForkParent), t.now)
	return err
}

// Thread reads the binding of a conversation.
func (t *Tx) Thread(convID int64) (Thread, bool, error) {
	return one(t.row(`SELECT `+threadCols+` FROM threads WHERE conv_id = ?`, convID), scanThread)
}

// Conversations lists all conversations (read pool; status and startup).
func (s *Store) Conversations() ([]Conversation, error) {
	return collect(scanConv)(s.r.QueryContext(context.Background(), `SELECT `+convCols+` FROM conversations ORDER BY id`))
}

// Threads lists all bindings (read pool).
func (s *Store) Threads() ([]Thread, error) {
	return collect(scanThread)(s.r.QueryContext(context.Background(), `SELECT `+threadCols+` FROM threads ORDER BY conv_id`))
}
