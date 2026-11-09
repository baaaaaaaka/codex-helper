package teams

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

const routeMarkUnread = "POST /chats/{chat}/markChatUnreadForUser"

func TestMarkUnreadAfterSentAnswer(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{}, func(c *Config) { c.TenantID = fakegraph.TenantID })
	chat := fg.CreateMeetingChatAsUser("unread").ID
	sent, err := a.Send(ctx, chat, renderOne(t, a, channel.RichDoc{Label: "🤖 ✅ Codex answer", Markdown: "done", Mention: true}), "a1")
	if err != nil {
		t.Fatal(err)
	}
	// Old bridge semantics: the answer itself (created at sent.Time) is unread.
	if err := a.MarkUnread(ctx, chat, sent.Time.Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	reqs := fg.Requests(fakegraph.RequestFilter{})
	var body struct {
		User struct {
			ID       string `json:"id"`
			TenantID string `json:"tenantId"`
		} `json:"user"`
		ReadAt string `json:"lastMessageReadDateTime"`
	}
	found := false
	for _, r := range reqs {
		if r.Route == routeMarkUnread && r.Status == 204 {
			found = true
			if err := json.Unmarshal([]byte(r.Body), &body); err != nil {
				t.Fatal(err)
			}
		}
	}
	readAt, _ := time.Parse(time.RFC3339Nano, body.ReadAt)
	if !found || body.User.ID != fg.Owner().ID || body.User.TenantID != fakegraph.TenantID || !readAt.Equal(sent.Time.Add(-time.Millisecond)) {
		t.Fatalf("mark unread request %+v (found %v)", body, found)
	}

	// Throttling of mark-unread is scoped to the call and never blocks sends.
	fg.AddFault(fakegraph.Fault{Name: "unread429", Kind: fakegraph.FaultStatus, Route: routeMarkUnread, Status: 429, RetryAfter: 7 * time.Second, Times: 1})
	err = a.MarkUnread(ctx, chat, time.Time{})
	th, ok := channel.AsThrottled(err)
	if !ok || th.Scope != channel.ScopeCall || th.RetryAfter != 7*time.Second {
		t.Fatalf("throttled mark unread: %v", err)
	}
	if countRequests(fg, routeMarkUnread) != 2 {
		t.Fatal("mark unread was retried internally")
	}
	if _, err := a.Send(ctx, chat, renderOne(t, a, channel.RichDoc{Label: "L", Markdown: "next"}), "a2"); err != nil {
		t.Fatalf("send after throttled mark unread: %v", err)
	}
	if err := a.MarkUnread(ctx, "19:missing@thread.v2", time.Time{}); !isPermanent(err) {
		t.Fatalf("unknown chat: %v", err)
	}
	noViolations(t, fg)
}

func TestEditMessage(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("edit").ID
	sent, err := a.Send(ctx, chat, renderOne(t, a, channel.RichDoc{Label: "Status", Markdown: "working", Mention: true}), "s1")
	if err != nil {
		t.Fatal(err)
	}
	p := renderOne(t, a, channel.RichDoc{Label: "Status", Markdown: "**done** 🎉", Mention: true})
	if err := a.Edit(ctx, chat, sent.ID, p, "s1"); err != nil {
		t.Fatal(err)
	}
	got, err := a.Get(ctx, chat, sent.ID)
	if err != nil || got.Marker != "s1" || got.Hash != p.Hash || !strings.Contains(got.Text, "done 🎉") || !got.MentionsOwner {
		t.Fatalf("edited message %+v %v", got, err)
	}
	if msgs := fg.Messages(chat); msgs[0].PatchCount != 1 {
		t.Fatalf("patches %d", msgs[0].PatchCount)
	}
	// Edits are idempotent and receive sees them again.
	w, _ := a.Receive(ctx, chat, "")
	if len(w.Msgs) != 1 || w.Msgs[0].ID != sent.ID {
		t.Fatalf("window %+v", w.Msgs)
	}
	// HTMLPart for core-built cards.
	if err := a.Edit(ctx, chat, sent.ID, HTMLPart("<p>card v2</p>"), ""); err != nil {
		t.Fatal(err)
	}
	if got, _ := a.Get(ctx, chat, sent.ID); got.Text != "card v2" || got.Marker != "" {
		t.Fatalf("card edit %+v", got)
	}
	// Someone else's message cannot be edited; images cannot be added.
	other, _ := fg.PostAsCoworker(chat, "hello")
	if err := a.Edit(ctx, chat, other.ID, HTMLPart("<p>x</p>"), ""); !isPermanent(err) {
		t.Fatalf("coworker edit: %v", err)
	}
	withImage := channel.Part{Body: `{"body":{"contentType":"html","content":"<img src=\"../hostedContents/1/$value\">"},"hostedContents":[{"@microsoft.graph.temporaryId":"1","contentBytes":"AA==","contentType":"image/png"}]}`}
	if err := a.Edit(ctx, chat, sent.ID, withImage, ""); !isPermanent(err) {
		t.Fatalf("edit adding images: %v", err)
	}
	fg.AddFault(fakegraph.Fault{Name: "patch503", Kind: fakegraph.FaultStatus, Route: "PATCH /chats/{chat}/messages/{message}", Status: 503, Times: 1})
	if err := a.Edit(ctx, chat, sent.ID, HTMLPart("<p>v3</p>"), ""); !channel.IsAmbiguous(err) {
		t.Fatalf("patch 503: %v", err)
	}
	noViolations(t, fg)
}

func TestEditHTMLPreservesAttachmentsAndMentions(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("annotate").ID
	src := filepath.Join(t.TempDir(), "a.pdf")
	if err := os.WriteFile(src, []byte("%PDF-1.4"), 0o600); err != nil {
		t.Fatal(err)
	}
	ref, err := a.UploadFile(ctx, src, "a.pdf", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := renderOne(t, a, channel.RichDoc{Label: "L", Markdown: "see file", Mention: true, Files: []channel.FileRef{ref}})
	sent, err := a.Send(ctx, chat, p, "")
	if err != nil {
		t.Fatal(err)
	}
	err = a.EditHTML(ctx, chat, sent.ID, func(html string) (string, bool) {
		return html + "<p><em>Transcript: hello</em></p>", true
	})
	if err != nil {
		t.Fatal(err)
	}
	m := fg.Messages(chat)[0]
	if m.PatchCount != 1 || len(m.Attachments) != 1 || len(m.Mentions) != 1 || !strings.Contains(m.Content, "Transcript: hello") {
		t.Fatalf("patched %+v", m)
	}
	// rewrite declining leaves the message alone.
	if err := a.EditHTML(ctx, chat, sent.ID, func(string) (string, bool) { return "", false }); err != nil || fg.Messages(chat)[0].PatchCount != 1 {
		t.Fatalf("declined rewrite patched: %v", err)
	}
	if err := a.EditHTML(ctx, chat, "999", func(h string) (string, bool) { return h + "x", true }); !errors.Is(err, channel.ErrNotFound) {
		t.Fatalf("missing message: %v", err)
	}
	noViolations(t, fg)
}

func TestSetMessagePrefixIsIdempotentAndPreservesBodyContext(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("question marker").ID
	src := filepath.Join(t.TempDir(), "question.txt")
	if err := os.WriteFile(src, []byte("question attachment"), 0o600); err != nil {
		t.Fatal(err)
	}
	ref, err := a.UploadFile(ctx, src, "question.txt", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := renderOne(t, a, channel.RichDoc{Label: "❓ Q1 · Question from Codex", Markdown: "Choose one", Mention: true, Files: []channel.FileRef{ref}})
	sent, err := a.Send(ctx, chat, p, "q1")
	if err != nil {
		t.Fatal(err)
	}
	original := fg.Messages(chat)[0].Content
	const prefix = "❓Pending agent question"
	if err := a.SetMessagePrefix(ctx, chat, sent.ID, prefix, true); err != nil {
		t.Fatal(err)
	}
	if err := a.SetMessagePrefix(ctx, chat, sent.ID, prefix, true); err != nil {
		t.Fatal(err)
	}
	message := fg.Messages(chat)[0]
	if message.PatchCount != 1 || !strings.HasPrefix(strings.TrimSpace(message.PlainText()), prefix) || !strings.Contains(message.PlainText(), "Choose one") || len(message.Attachments) != 1 || len(message.Mentions) != 1 {
		t.Fatalf("prefixed message %+v", message)
	}
	if err := a.SetMessagePrefix(ctx, chat, sent.ID, prefix, false); err != nil {
		t.Fatal(err)
	}
	message = fg.Messages(chat)[0]
	if message.PatchCount != 2 || message.Content != original || strings.HasPrefix(strings.TrimSpace(message.PlainText()), prefix) || !strings.Contains(message.PlainText(), "Choose one") || len(message.Attachments) != 1 || len(message.Mentions) != 1 {
		t.Fatalf("prefix removal lost content: %+v", message)
	}

	// Question status edits replace the body. Re-applying the prefix must keep
	// the updated status, then removing it must leave that status intact.
	plain, err := a.Send(ctx, chat, renderOne(t, a, channel.RichDoc{Label: "❓ Q2 · Question from Codex", Markdown: "Another question"}), "q2")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetMessagePrefix(ctx, chat, plain.ID, prefix, true); err != nil {
		t.Fatal(err)
	}
	if err := a.EditHTML(ctx, chat, plain.ID, func(string) (string, bool) { return "<p>Resolved</p>", true }); err != nil {
		t.Fatal(err)
	}
	if err := a.SetMessagePrefix(ctx, chat, plain.ID, prefix, true); err != nil {
		t.Fatal(err)
	}
	if err := a.SetMessagePrefix(ctx, chat, plain.ID, prefix, false); err != nil {
		t.Fatal(err)
	}
	message = fg.Messages(chat)[1]
	if message.PatchCount != 4 || strings.HasPrefix(strings.TrimSpace(message.PlainText()), prefix) || !strings.Contains(message.PlainText(), "Resolved") {
		t.Fatalf("updated message %+v", message)
	}
	other, _ := fg.PostAsCoworker(chat, "not mine")
	if err := a.SetMessagePrefix(ctx, chat, other.ID, prefix, true); !errors.Is(err, channel.ErrNotEditable) {
		t.Fatalf("prefix coworker message: %v", err)
	}
	noViolations(t, fg)
}
