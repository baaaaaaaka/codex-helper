package teams

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestPendingQuestionsDecorationPreservesContextAndInput(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("pending").ID
	p := renderOne(t, a, channel.RichDoc{Label: "Helper", Markdown: "A status", Mention: true})
	sent, err := a.Send(ctx, chat, p, "pending-test")
	if err != nil {
		t.Fatal(err)
	}
	original := fg.Messages(chat)[0].Content
	qs := []channel.PendingQuestion{{ID: "Q1", Title: "Choose <environment>", MessageID: sent.ID}, {ID: "Q2", Title: "Other?"}}
	if err := a.SetPendingQuestions(ctx, chat, sent.ID, channel.PendingQuestions{Items: qs}); err != nil {
		t.Fatal(err)
	}
	first := fg.Messages(chat)[0]
	if !strings.Contains(first.Content, esc(annotationLink(chat, sent.ID, ""))) || !strings.Contains(first.PlainText(), "Sending question") || !strings.Contains(first.PlainText(), "this question") || len(first.Mentions) != 1 {
		t.Fatal(first)
	}
	if err := a.SetPendingQuestions(ctx, chat, sent.ID, channel.PendingQuestions{Items: qs}); err != nil {
		t.Fatal(err)
	}
	if fg.Messages(chat)[0].PatchCount != first.PatchCount {
		t.Fatal("unchanged decoration wrote Graph")
	}
	// Service-normalized tags do not turn unchanged refreshes into PATCH loops.
	if err := a.EditHTML(ctx, chat, sent.ID, func(s string) (string, bool) {
		return strings.NewReplacer("<hr/>", "<hr>", "<br/>", "<br>").Replace(s), true
	}); err != nil {
		t.Fatal(err)
	}
	normalized := fg.Messages(chat)[0]
	if err := a.SetPendingQuestions(ctx, chat, sent.ID, channel.PendingQuestions{Items: qs}); err != nil {
		t.Fatal(err)
	}
	if fg.Messages(chat)[0].PatchCount != normalized.PatchCount {
		t.Fatal("normalized decoration wrote Graph")
	}
	// Helper decoration must not affect native input or echo-hash matching.
	var gm graphMessage
	gm.ID = sent.ID
	gm.Body.Content = first.Content
	gm.Body.ContentType = "html"
	got := a.convert(chat, gm, "")
	if got.Text != plainText(original) || len(got.Links) != 0 {
		t.Fatalf("decoration entered input: %+v", got)
	}
	// An ordinary append must stay above the dynamic tail.
	if err := a.Annotate(ctx, chat, sent.ID, channel.Annotation{Notice: &channel.RichDoc{Label: "Helper", Markdown: "Paused here"}, Key: "freeze:test"}); err != nil {
		t.Fatal(err)
	}
	updated := fg.Messages(chat)[0]
	if strings.Index(updated.Content, "Paused here") > strings.Index(updated.Content, "❓ Pending questions") {
		t.Fatal(updated.Content)
	}
	if err := a.SetPendingQuestions(ctx, chat, sent.ID, channel.PendingQuestions{Items: qs[:1]}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fg.Messages(chat)[0].PlainText(), "Q2") {
		t.Fatal("answered question retained")
	}
	if err := a.SetPendingQuestions(ctx, chat, sent.ID, channel.PendingQuestions{Items: nil}); err != nil {
		t.Fatal(err)
	}
	final := fg.Messages(chat)[0]
	if strings.Contains(final.PlainText(), "Pending") || !strings.Contains(final.PlainText(), "Paused here") || len(final.Mentions) != 1 {
		t.Fatal(final)
	}
	other, _ := fg.PostAsCoworker(chat, "coworker")
	if err := a.SetPendingQuestions(ctx, chat, other.ID, channel.PendingQuestions{Items: qs}); !errors.Is(err, channel.ErrNotEditable) {
		t.Fatalf("coworker: %v", err)
	}
	noViolations(t, fg)
}

func TestPendingQuestionsPreservesAttachmentsAndReceipt(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("receipt").ID
	m, _ := fg.PostImageAsUser(chat, "look", []byte{0x89, 'P', 'N', 'G'})
	original := fg.Messages(chat)[0]
	qs := []channel.PendingQuestion{{ID: "Q1", MessageID: m.ID}}
	if err := a.SetPendingQuestions(ctx, chat, m.ID, channel.PendingQuestions{Items: qs}); err != nil {
		t.Fatal(err)
	}
	p := renderOne(t, a, channel.RichDoc{Label: "🔧 Helper", Markdown: "Request accepted."})
	an := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:test"}
	if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
		t.Fatal(err)
	}
	if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
		t.Fatal(err)
	}
	got := fg.Messages(chat)[0]
	if len(got.Attachments) != len(original.Attachments) || !strings.Contains(got.Content, "<img") || strings.Count(got.PlainText(), "Request accepted.") != 1 || strings.Index(got.Content, "Request accepted.") > strings.Index(got.Content, "❓ Pending questions") {
		t.Fatal(got)
	}
	noViolations(t, fg)
}

func TestPendingQuestionsSectionNormalizationAndQuotes(t *testing.T) {
	qs := []channel.PendingQuestion{{ID: "Q1", MessageID: "question"}}
	section := pendingQuestionsHTML("chat", "message", channel.PendingQuestions{Items: qs}, true)
	for _, rule := range []string{"<hr/>", "<hr>", "<hr />"} {
		normalized := strings.ReplaceAll(section, "<hr/>", rule)
		source := "<p>body</p>" + normalized + "<p>later</p>"
		body, _, tail := splitPendingQuestions(source, "chat", "message")
		if body != "<p>body</p><p>later</p>" || tail != normalized {
			t.Fatalf("%q %q", body, tail)
		}
	}
	for _, wrap := range []string{"blockquote", "pre", "code"} {
		source := "<" + wrap + ">" + section + "</" + wrap + ">"
		body, _, tail := splitPendingQuestions(source, "chat", "message")
		if body != source || tail != "" {
			t.Fatal("removed quoted example")
		}
	}
	body, _, tail := splitPendingQuestions(section, "chat", "other-message")
	if body != section || tail != "" {
		t.Fatal("removed another message's section")
	}
}

func TestPendingQuestionsOversizeDirectory(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("large").ID
	sent, err := a.Send(ctx, chat, renderOne(t, a, channel.RichDoc{Markdown: "body"}), "large")
	if err != nil {
		t.Fatal(err)
	}
	qs := make([]channel.PendingQuestion, 1024)
	for i := range qs {
		qs[i] = channel.PendingQuestion{ID: fmt.Sprintf("Q%d", i+1), Title: strings.Repeat("题", 80), MessageID: fmt.Sprintf("%020d", i+1)}
	}
	if err := a.SetPendingQuestions(ctx, chat, sent.ID, channel.PendingQuestions{Items: qs}); !errors.Is(err, channel.ErrQuestionDecorationTooLarge) {
		t.Fatalf("overflow: %v", err)
	}
	parts, err := a.RenderQuestionDirectory(ctx, chat, qs)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) < 2 {
		t.Fatal("directory did not split")
	}
	total := 0
	for _, p := range parts {
		content := decodePart(t, p).Body.Content
		total += strings.Count(content, `<a href=`)
	}
	if total != len(qs) {
		t.Fatalf("directory lost links: %d", total)
	}
	noViolations(t, fg)
}

// A receipt is the primary operation; pending decoration piggybacks without
// adding another PATCH, including uncertain outcomes and independent refresh.
func TestPendingQuestionsCombinedReceipt(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint("lost=", lost), func(t *testing.T) {
			ctx := context.Background()
			fg, a := newFake(t, fakegraph.Options{})
			chat := fg.CreateMeetingChatAsUser("combined").ID
			m, err := fg.PostImageAsUser(chat, "look", []byte{0x89, 'P', 'N', 'G'})
			if err != nil {
				t.Fatal(err)
			}
			p := renderOne(t, a, channel.RichDoc{Label: "Helper", Markdown: "Request accepted."})
			pending := &channel.PendingQuestions{Items: []channel.PendingQuestion{{ID: "Q1", MessageID: "question"}}}
			an := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:combined", PendingQuestions: pending}
			if lost {
				fg.AddFault(fakegraph.Fault{Name: "lost", Kind: fakegraph.FaultAcceptedLost, Route: "PATCH /chats/{chat}/messages/{message}", Times: 1})
			}
			err = a.Annotate(ctx, chat, m.ID, an)
			if lost && !channel.IsAmbiguous(err) || !lost && err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
				t.Fatal(err)
			}
			if err := a.SetPendingQuestions(ctx, chat, m.ID, *pending); err != nil {
				t.Fatal(err)
			}
			got := fg.Messages(chat)[0]
			if got.PatchCount != 1 || !strings.Contains(got.PlainText(), "Q1") || !strings.Contains(got.Content, "<img") || strings.Count(got.PlainText(), "Request accepted.") != 1 {
				t.Fatalf("combined: %+v", got)
			}
			// An already-written receipt must not suppress changed question state.
			pending.Items[0].ID = "Q2"
			if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
				t.Fatal(err)
			}
			if got := fg.Messages(chat)[0]; got.PatchCount != 2 || strings.Contains(got.PlainText(), "Q1") || !strings.Contains(got.PlainText(), "Q2") {
				t.Fatalf("refresh: %+v", got)
			}
			an.PendingQuestions = nil
			if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
				t.Fatal(err)
			}
			if got := fg.Messages(chat)[0]; got.PatchCount != 2 || !strings.Contains(got.PlainText(), "Q2") {
				t.Fatalf("nil changed state: %+v", got)
			}
			an.PendingQuestions = &channel.PendingQuestions{}
			if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
				t.Fatal(err)
			}
			if got := fg.Messages(chat)[0]; got.PatchCount != 3 || strings.Contains(got.PlainText(), "Pending") || !strings.Contains(got.PlainText(), "Request accepted.") {
				t.Fatalf("clear: %+v", got)
			}
			noViolations(t, fg)
		})
	}
}

func TestPendingQuestionsOverflowDoesNotFailReceipt(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("overflow receipt").ID
	m, _ := fg.PostAsUser(chat, "hello")
	p := renderOne(t, a, channel.RichDoc{Label: "Helper", Markdown: "Request accepted."})
	pending := &channel.PendingQuestions{}
	for i := 0; i < 1024; i++ {
		pending.Items = append(pending.Items, channel.PendingQuestion{ID: fmt.Sprintf("Q%d", i), MessageID: fmt.Sprint(i)})
	}
	an := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:overflow", PendingQuestions: pending}
	for i := 0; i < 2; i++ {
		if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
			t.Fatal(err)
		}
	}
	got := fg.Messages(chat)[0]
	if got.PatchCount != 1 || !strings.Contains(got.PlainText(), "Request accepted.") || strings.Contains(got.PlainText(), "Pending") {
		t.Fatalf("overflow affected receipt: %+v", got)
	}
	if err := a.SetPendingQuestions(ctx, chat, m.ID, *pending); !errors.Is(err, channel.ErrQuestionDecorationTooLarge) {
		t.Fatalf("independent refresh: %v", err)
	}
	noViolations(t, fg)
}
