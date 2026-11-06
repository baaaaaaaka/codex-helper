package teams

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

const testLabel = "🧑‍💻 User"

func TestAnnotateLabel(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("label").ID
	m, _ := fg.PostAsUser(chat, "run the tests\nplease")
	if err := a.Annotate(ctx, chat, m.ID, channel.Annotation{Label: testLabel}); err != nil {
		t.Fatal(err)
	}
	got := fg.Messages(chat)[0]
	if got.PatchCount != 1 || !strings.HasPrefix(got.Content, "<p><strong>"+testLabel+":</strong></p>") ||
		!strings.Contains(got.PlainText(), "run the tests") {
		t.Fatalf("labelled %+v", got)
	}
	// Idempotent: a labelled message is left alone.
	if err := a.Annotate(ctx, chat, m.ID, channel.Annotation{Label: testLabel}); err != nil || fg.Messages(chat)[0].PatchCount != 1 {
		t.Fatalf("second label: %v, patches %d", err, fg.Messages(chat)[0].PatchCount)
	}
	// Another account's message is never edited.
	other, _ := fg.PostAsCoworker(chat, "hello")
	if err := a.Annotate(ctx, chat, other.ID, channel.Annotation{Label: testLabel}); !errors.Is(err, channel.ErrNotEditable) {
		t.Fatalf("coworker: %v", err)
	}
	// A pasted image could be lost: refused.
	img, _ := fg.PostImageAsUser(chat, "look", []byte{0x89, 'P', 'N', 'G'})
	if err := a.Annotate(ctx, chat, img.ID, channel.Annotation{Label: testLabel}); !errors.Is(err, channel.ErrNotEditable) {
		t.Fatalf("image: %v", err)
	}
	// A file reference is kept.
	f, _ := fg.PostFileAsUser(chat, "notes", "a.txt", []byte("x"))
	if err := a.Annotate(ctx, chat, f.ID, channel.Annotation{Label: testLabel}); err != nil {
		t.Fatal(err)
	}
	for _, mm := range fg.Messages(chat) {
		if mm.ID == f.ID && (mm.PatchCount != 1 || len(mm.Attachments) != 1 || !strings.Contains(mm.Content, "<attachment")) {
			t.Fatalf("file message %+v", mm)
		}
		if (mm.ID == other.ID || mm.ID == img.ID) && mm.PatchCount != 0 {
			t.Fatalf("refused message patched: %+v", mm)
		}
	}
	// A voice clip gets its transcript instead.
	v, _ := fg.PostVoiceAsUser(chat, "v.ogg", []byte("x"))
	if err := a.Annotate(ctx, chat, v.ID, channel.Annotation{Label: testLabel}); !errors.Is(err, channel.ErrNotEditable) {
		t.Fatalf("voice label: %v", err)
	}
	if err := a.Annotate(ctx, chat, "999", channel.Annotation{Label: testLabel}); !errors.Is(err, channel.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	noViolations(t, fg)
}

func TestAnnotateTranscript(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("voice").ID
	v, _ := fg.PostVoiceAsUser(chat, "v.ogg", []byte("x"))
	an := channel.Annotation{Label: testLabel, ClipsLabel: "🎙️ Transcript", Clips: []channel.Clip{{Name: "v.ogg", Text: "hello <world>\nsecond line"}}}
	if err := a.Annotate(ctx, chat, v.ID, an); err != nil {
		t.Fatal(err)
	}
	got := fg.Messages(chat)[0]
	text := got.PlainText()
	if got.PatchCount != 1 || len(got.Attachments) != 1 || !strings.Contains(got.Content, "<attachment") ||
		!strings.HasPrefix(strings.TrimSpace(text), testLabel+":") || !strings.Contains(text, "Clip: v.ogg") ||
		!strings.Contains(text, "hello <world>") || !strings.Contains(text, "second line") {
		t.Fatalf("transcript edit %+v\n%s", got, text)
	}
	if err := a.Annotate(ctx, chat, v.ID, an); err != nil || fg.Messages(chat)[0].PatchCount != 1 {
		t.Fatalf("second transcript: %v", err)
	}
	noViolations(t, fg)
}

func TestAnnotateNotice(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("freeze").ID
	sent, err := a.Send(ctx, chat, renderOne(t, a, channel.RichDoc{Label: "🤖 ✅ Codex answer", Markdown: "done", Mention: true}), "a1")
	if err != nil {
		t.Fatal(err)
	}
	an := channel.Annotation{Notice: &channel.RichDoc{Label: "🔧 Helper", Markdown: "🧊 This chat is paused\nSend: `r s001`"},
		Present: []string{"This chat is paused", "r s001"}}
	if err := a.Annotate(ctx, chat, sent.ID, an); err != nil {
		t.Fatal(err)
	}
	got := fg.Messages(chat)[0]
	if got.PatchCount != 1 || !strings.Contains(got.PlainText(), "done") || !strings.Contains(got.PlainText(), "This chat is paused") ||
		len(got.Mentions) != len(fg.Messages(chat)[0].Mentions) {
		t.Fatalf("notice edit %+v", got)
	}
	// Present: not added twice.
	if err := a.Annotate(ctx, chat, sent.ID, an); err != nil || fg.Messages(chat)[0].PatchCount != 1 {
		t.Fatalf("second notice: %v", err)
	}
	// A message with a pasted image is not edited.
	img, _ := fg.PostImageAsUser(chat, "look", []byte{0x89, 'P', 'N', 'G'})
	if err := a.Annotate(ctx, chat, img.ID, an); !errors.Is(err, channel.ErrNotEditable) {
		t.Fatalf("image notice: %v", err)
	}
	noViolations(t, fg)
}
