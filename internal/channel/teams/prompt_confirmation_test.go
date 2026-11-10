package teams

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestPromptConfirmationAppend(t *testing.T) {
	for _, stripped := range []bool{false, true} {
		t.Run(map[bool]string{false: "comments", true: "stripped"}[stripped], func(t *testing.T) {
			ctx := context.Background()
			fg, a := newFake(t, fakegraph.Options{StripHTMLComments: stripped})
			chat := fg.CreateMeetingChatAsUser("confirm").ID
			owner, _ := fg.PostAsUser(chat, "run the tests")
			voice, _ := fg.PostVoiceAsUser(chat, "voice.ogg", []byte("OggS"))
			file, _ := fg.PostFileAsUser(chat, "read it", "notes.txt", []byte("x"))
			p := renderOne(t, a, channel.RichDoc{Label: "🔧 Helper", Markdown: "⏳ Codex is working. Request accepted."})
			for _, id := range []string{owner.ID, voice.ID, file.ID} {
				an := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:" + id}
				if err := a.Annotate(ctx, chat, id, an); err != nil {
					t.Fatalf("append %s: %v", id, err)
				}
				// A later transcript edit must preserve both the media and the receipt.
				if id == voice.ID {
					err := a.Annotate(ctx, chat, id, channel.Annotation{ClipsLabel: "Transcript", Clips: []channel.Clip{{Name: "voice.ogg", Text: "run tests"}}})
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := a.Annotate(ctx, chat, id, an); err != nil {
					t.Fatal(err)
				}
			}
			msgs := fg.Messages(chat)
			if len(msgs) != 3 {
				t.Fatalf("created separate posts: %d", len(msgs))
			}
			for _, m := range msgs {
				wantPatches := 1
				if m.ID == voice.ID {
					wantPatches = 2
				}
				if m.PatchCount != wantPatches || strings.Count(m.PlainText(), "Request accepted.") != 1 || !strings.Contains(m.Content, "<hr") {
					t.Fatalf("confirmation: %+v", m)
				}
				got, err := a.Get(ctx, chat, m.ID)
				if err != nil || got.Marker != "" {
					t.Fatalf("user became helper post: %+v %v", got, err)
				}
				if m.ID != owner.ID && (len(m.Attachments) != 1 || !strings.Contains(m.Content, "<attachment")) {
					t.Fatalf("lost attachment: %+v", m)
				}
			}
			noViolations(t, fg)
		})
	}
}

func TestPromptConfirmationLostPatchAndRefusals(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("receipt").ID
	m, _ := fg.PostAsUser(chat, "do work")
	p := renderOne(t, a, channel.RichDoc{Label: "🔧 Helper", Markdown: "accepted"})
	an := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:one"}
	fg.AddFault(fakegraph.Fault{Name: "lost-patch", Kind: fakegraph.FaultAcceptedLost, Route: "PATCH /chats/{chat}/messages/{message}", Times: 1})
	if err := a.Annotate(ctx, chat, m.ID, an); !channel.IsAmbiguous(err) {
		t.Fatalf("expected ambiguous PATCH: %v", err)
	}
	if err := a.Annotate(ctx, chat, m.ID, an); err != nil {
		t.Fatal(err)
	}
	if fg.Messages(chat)[0].PatchCount != 1 {
		t.Fatal("PATCH was repeated after success")
	}
	other, _ := fg.PostAsCoworker(chat, "hello")
	img, _ := fg.PostImageAsUser(chat, "look", []byte{0x89, 'P', 'N', 'G'})
	long, _ := fg.PostAsUser(chat, strings.Repeat("x", maxPartBytes))
	for _, id := range []string{other.ID, img.ID, long.ID, "missing"} {
		err := a.Annotate(ctx, chat, id, an)
		if !errors.Is(err, channel.ErrNotEditable) && !errors.Is(err, channel.ErrNotFound) {
			t.Fatalf("unsafe edit %s: %v", id, err)
		}
	}
	fg.AddFault(fakegraph.Fault{Name: "refused", Kind: fakegraph.FaultStatus, Status: 403, Route: "PATCH /chats/{chat}/messages/{message}", Times: 1})
	fresh, _ := fg.PostAsUser(chat, "another request")
	if err := a.Annotate(ctx, chat, fresh.ID, an); !errors.Is(err, channel.ErrNotEditable) {
		t.Fatalf("definite refusal: %v", err)
	}
	noViolations(t, fg)
}
