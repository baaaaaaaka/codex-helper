package teams

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestTranscriptBeforePromptConfirmation(t *testing.T) {
	for _, receiptFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "confirmation-first", false: "transcript-first"}[receiptFirst], func(t *testing.T) {
			ctx := context.Background()
			fg, a := newFake(t, fakegraph.Options{StripHTMLComments: true})
			chat := fg.CreateMeetingChatAsUser("voice confirmation").ID
			voice, err := fg.PostVoiceAsUser(chat, "voice.ogg", []byte("OggS"))
			if err != nil {
				t.Fatal(err)
			}
			original := `<p>typed caption</p><hr class="user-rule"><p>keep this rule</p>` + voice.Content
			if err := fg.EditHTMLAsUser(chat, voice.ID, original); err != nil {
				t.Fatal(err)
			}
			p := renderOne(t, a, channel.RichDoc{Label: "🔧 Helper", Markdown: "Request accepted."})
			receipt := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:voice", Label: testLabel, At: time.Unix(1, 0)}
			transcript := channel.Annotation{Label: testLabel, ClipsLabel: "🎙️ Transcript", Clips: []channel.Clip{
				{Name: "voice.ogg", Text: "first clip text"},
				{Name: "second.ogg", Text: "second clip text"},
			}}
			first, second := receipt, transcript
			if !receiptFirst {
				first, second = second, first
			}
			for i, an := range []channel.Annotation{first, second} {
				if receiptFirst && i == 1 {
					fg.AddFault(fakegraph.Fault{Name: "lost-transcript-patch", Kind: fakegraph.FaultAcceptedLost,
						Route: "PATCH /chats/{chat}/messages/{message}", Times: 1})
				}
				err := a.Annotate(ctx, chat, voice.ID, an)
				if receiptFirst && i == 1 {
					if !channel.IsAmbiguous(err) {
						t.Fatalf("expected an uncertain transcript PATCH: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
			got := fg.Messages(chat)[0]
			user, helper, ok := strings.Cut(got.Content, "<hr/>")
			if !ok || !strings.HasPrefix(user, labelHTML(testLabel)+original) ||
				!strings.Contains(user, "first clip text") || !strings.Contains(user, "second clip text") ||
				strings.Contains(helper, "clip text") || !strings.Contains(helper, "Request accepted.") || !strings.Contains(helper, "Received at") {
				t.Fatalf("transcripts must precede the helper separator: %s", got.Content)
			}
			if !reflect.DeepEqual(got.Attachments, voice.Attachments) || !reflect.DeepEqual(got.Mentions, voice.Mentions) {
				t.Fatal("original attachments or mentions changed")
			}
			for _, an := range []channel.Annotation{transcript, receipt} {
				if err := a.Annotate(ctx, chat, voice.ID, an); err != nil {
					t.Fatal(err)
				}
			}
			if after := fg.Messages(chat)[0]; after.Content != got.Content || after.PatchCount != 2 {
				t.Fatal("annotation retry changed or duplicated the message")
			}
			noViolations(t, fg)
		})
	}
}

func TestTranscriptConfirmationOwnerBoundary(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("owner boundary").ID
	m, _ := fg.PostAsCoworker(chat, "voice from another member")
	err := a.Annotate(ctx, chat, m.ID, channel.Annotation{ClipsLabel: "Transcript", Clips: []channel.Clip{{Name: "clip", Text: "words"}}})
	if !errors.Is(err, channel.ErrNotEditable) || fg.Messages(chat)[0].PatchCount != 0 {
		t.Fatalf("another member's message was editable: %v", err)
	}
	noViolations(t, fg)
}

func TestTranscriptConfirmationBoundary(t *testing.T) {
	link := annotationLink("chat", "message", "")
	header := labelHTML("🔧 Helper") + "<p>Request accepted.</p>"
	stamp := timeStamp("Received at", link, time.Unix(1, 0))
	receipt := "<hr/>" + header + stamp
	for _, tc := range []struct {
		name, user, receipt string
	}{
		{"ordinary-rule", `<p>caption</p><hr/><p>more user text</p>`, receipt},
		{"quoted-receipt", "<blockquote>" + receipt + "</blockquote><p>caption</p>", receipt},
		{"code-receipt", "<pre><code>" + receipt + "</code></pre><p>caption</p>", receipt},
		{"other-message", "<hr/>" + header + timeStamp("Received at", annotationLink("chat", "other", ""), time.Unix(1, 0)), receipt},
		{"ordinary-self-link", "<hr/>" + stamp, receipt},
		{"normalized", `<div><p>caption</p>`, `<HR class="receipt"><div><p><span>🔧 Helper:</span></p></div>` + stamp + "</div>"},
		{"legacy", "<p>caption</p>", `<hr/><p><strong><a href="` + esc(link) + `">🔧 Helper:</a></strong></p><p>Request accepted.</p>`},
		{"only-quote", "<blockquote>" + receipt + "</blockquote>", ""},
		{"no-receipt", "<p>caption</p><hr/><p>more text</p>", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next, err := transcriptEdit("chat", "message", tc.user+tc.receipt, true, nil,
				channel.Annotation{ClipsLabel: "Transcript", Clips: []channel.Clip{{Name: "clip", Text: "transcribed words"}}})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(next, tc.user) || !strings.HasSuffix(next, tc.receipt) ||
				!strings.Contains(strings.TrimSuffix(next, tc.receipt)[len(tc.user):], "transcribed words") {
				t.Fatalf("transcript moved user HTML or entered the helper section: %s", next)
			}
		})
	}
}

func BenchmarkTranscriptEditLarge(b *testing.B) {
	content := strings.Repeat("<p>long user text &amp; notes</p><hr/>", 500) +
		"<hr/>" + labelHTML("🔧 Helper") + "<p>Request accepted.</p>" +
		timeStamp("Received at", annotationLink("chat", "message", ""), time.Unix(1, 0))
	an := channel.Annotation{ClipsLabel: "Transcript", Clips: []channel.Clip{{Name: "clip", Text: "transcribed words"}}}
	b.ReportAllocs()
	b.SetBytes(int64(len(content)))
	for b.Loop() {
		if _, err := transcriptEdit("chat", "message", content, true, nil, an); err != nil {
			b.Fatal(err)
		}
	}
}
