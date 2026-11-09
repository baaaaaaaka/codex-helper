package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Edit replaces a message this account posted with a rendered part
// (PATCH /chats/{chat}/messages/{id}). The part's mentions and reference
// attachments are sent along; a part that would add hosted images is refused
// (Graph does not accept new hostedContents on PATCH). PATCH is idempotent,
// so the core may repeat an Edit that ended Ambiguous.
func (a *Adapter) Edit(ctx context.Context, chat, id string, p channel.Part, token string) error {
	if strings.TrimSpace(id) == "" {
		return &channel.Rejected{Permanent: true, Err: errors.New("Edit needs a message id")}
	}
	raw, err := withMarker(p.Body, token)
	if err != nil {
		return &channel.Rejected{Permanent: true, Err: err}
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return &channel.Rejected{Permanent: true, Err: err}
	}
	var hosted []json.RawMessage
	_ = json.Unmarshal(m["hostedContents"], &hosted)
	if len(hosted) > 0 {
		return &channel.Rejected{Permanent: true, Err: errors.New("teams: an edit cannot add images; send a new message instead")}
	}
	delete(m, "hostedContents")
	return a.g.writeJSON(ctx, http.MethodPatch, messagePath(chat, id), m, nil)
}

// HTMLPart wraps ready Teams HTML (for example a status card built by the
// core) as a part for Send or Edit. It carries no mention, files or images.
func HTMLPart(html string) channel.Part {
	raw, _ := json.Marshal(map[string]any{"body": map[string]string{"contentType": "html", "content": html}})
	return channel.Part{Index: 1, Count: 1, Body: string(raw), Hash: channel.TextHash(plainText(html)), Size: len(html)}
}

// EditHTML rewrites the HTML of an existing message this account posted,
// keeping its attachments and mentions (Teams drops them when a PATCH omits
// them). rewrite receives the stored body (including any outbox marker) and
// returns the new body; returning ok=false leaves the message alone.
//
// This is the primitive behind annotating a user's own message (voice
// transcripts, freeze notices). Messages with inline hosted images keep
// their <img> references only if rewrite keeps them; Graph does not
// re-upload them.
func (a *Adapter) EditHTML(ctx context.Context, chat, id string, rewrite func(html string) (string, bool)) error {
	var cur struct {
		Body struct {
			ContentType string `json:"contentType"`
			Content     string `json:"content"`
		} `json:"body"`
		Attachments []json.RawMessage `json:"attachments"`
		Mentions    []json.RawMessage `json:"mentions"`
		Deleted     *string           `json:"deletedDateTime"`
	}
	if err := a.g.getJSON(ctx, messagePath(chat, id), smallBodyLimit, &cur); err != nil {
		return err
	}
	if cur.Deleted != nil && *cur.Deleted != "" {
		return &channel.Rejected{Permanent: true, Err: fmt.Errorf("message %s is deleted: %w", id, channel.ErrNotFound)}
	}
	html := cur.Body.Content
	if !strings.EqualFold(cur.Body.ContentType, "html") {
		html = esc(html)
	}
	next, ok := rewrite(html)
	if !ok || next == html {
		return nil
	}
	in := map[string]any{"body": map[string]string{"contentType": "html", "content": next}}
	if len(cur.Attachments) > 0 {
		in["attachments"] = cur.Attachments
	}
	if len(cur.Mentions) > 0 {
		in["mentions"] = cur.Mentions
	}
	return a.g.writeJSON(ctx, http.MethodPatch, messagePath(chat, id), in, nil)
}

// SetMessagePrefix adds or removes a separate first paragraph on one of this
// account's messages. It keeps attachments, mentions, and the existing body.
func (a *Adapter) SetMessagePrefix(ctx context.Context, chat, id, prefix string, present bool) error {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(prefix) == "" {
		return &channel.Rejected{Permanent: true, Err: fmt.Errorf("message prefix needs an id and text: %w", channel.ErrNotEditable)}
	}
	me, err := a.me(ctx)
	if err != nil {
		return err
	}
	var m annotatedMessage
	if err := a.g.getJSON(ctx, messagePath(chat, id), smallBodyLimit, &m); err != nil {
		return err
	}
	if m.Deleted != nil && *m.Deleted != "" || m.MessageType != "" && m.MessageType != "message" {
		return fmt.Errorf("message %s is not a visible message: %w", id, channel.ErrNotEditable)
	}
	if m.From == nil || m.From.User == nil || !strings.EqualFold(m.From.User.ID, me.ID) {
		return fmt.Errorf("message %s is not the signed-in account's: %w", id, channel.ErrNotEditable)
	}
	content := m.Body.Content
	isHTML := m.Body.ContentType == "" || strings.EqualFold(m.Body.ContentType, "html")
	if !isHTML {
		content = asHTML(content, false)
	}
	next, changed := messagePrefixEdit(content, prefix, present)
	if !changed {
		return nil
	}
	in := map[string]any{"body": map[string]string{"contentType": "html", "content": next}}
	if len(m.Attachments) > 0 {
		in["attachments"] = m.Attachments
	}
	if len(m.Mentions) > 0 {
		in["mentions"] = m.Mentions
	}
	return a.g.writeJSON(ctx, http.MethodPatch, messagePath(chat, id), in, nil)
}

func messagePrefixEdit(content, prefix string, present bool) (string, bool) {
	start := len(content) - len(strings.TrimLeft(content, " \t\r\n"))
	body := content[start:]
	first, _, _ := strings.Cut(strings.TrimSpace(plainText(body)), "\n")
	marked := strings.TrimSpace(first) == prefix
	if present {
		if marked {
			return content, false
		}
		return "<p><strong>" + esc(prefix) + "</strong></p>" + content, true
	}
	if !marked {
		return content, false
	}
	match := leadingParagraphRE.FindStringIndex(body)
	if match == nil || strings.TrimSpace(plainText(body[match[0]:match[1]])) != prefix {
		return content, false
	}
	return content[:start] + body[match[1]:], true
}

// MarkUnread marks chat unread for the owner (POST
// /chats/{chat}/markChatUnreadForUser) so a posted answer notifies like a
// message from someone else. Messages created after readAt count as unread;
// the old bridge passed the answer's createdDateTime minus 1 ms. A zero
// readAt omits lastMessageReadDateTime. Throttling of this call never
// gates sends: a *Throttled is returned with Scope == ScopeCall.
func (a *Adapter) MarkUnread(ctx context.Context, chat string, readAt time.Time) error {
	me, err := a.me(ctx)
	if err != nil {
		return err
	}
	user := map[string]string{"id": me.ID}
	if a.cfg.TenantID != "" {
		user["tenantId"] = a.cfg.TenantID
	}
	in := map[string]any{"user": user}
	if !readAt.IsZero() {
		in["lastMessageReadDateTime"] = readAt.UTC().Format(time.RFC3339Nano)
	}
	err = a.g.writeJSON(ctx, http.MethodPost, "/chats/"+url.PathEscape(chat)+"/markChatUnreadForUser", in, nil)
	if th, ok := channel.AsThrottled(err); ok {
		th.Scope = channel.ScopeCall
	}
	return err
}

func messagePath(chat, id string) string {
	return "/chats/" + url.PathEscape(chat) + "/messages/" + url.PathEscape(id)
}
