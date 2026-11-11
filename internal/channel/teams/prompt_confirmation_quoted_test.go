package teams

import (
	"context"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// Pasting a previous conversation must not count as acknowledging this input.
func TestPromptConfirmationDoesNotMistakeQuotedReceiptForCurrent(t *testing.T) {
	for _, quote := range []bool{false, true} {
		name := "pasted-conversation"
		if quote {
			name = "blockquote"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fg, a := newFake(t, fakegraph.Options{})
			chat := fg.CreateMeetingChatAsUser(name).ID
			const receipt = "⏳ Codex is working. Request accepted."
			const code = `tuiTail = []string{"resume", shared.Endpoint.ThreadID}`
			copied := "<p>Earlier conversation</p><hr/><p><strong>🔧 Helper:</strong> " + receipt + "</p><p>Earlier answer.</p>"
			if quote {
				copied = "<blockquote>" + copied + "</blockquote>"
			}
			body := "<p>Please investigate.</p>" + copied + "<pre><code>" + esc(code) + "</code></pre>"
			owner, err := fg.PostHTMLAsUser(chat, body)
			if err != nil {
				t.Fatal(err)
			}
			p := renderOne(t, a, channel.RichDoc{Label: "🔧 Helper", Markdown: receipt})
			an := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:new-input"}
			if err := a.Annotate(ctx, chat, owner.ID, an); err != nil {
				t.Fatal(err)
			}
			got := fg.Messages(chat)[0]
			if got.PatchCount != 1 || strings.Count(got.PlainText(), receipt) != 2 || strings.LastIndex(got.PlainText(), receipt) < strings.Index(got.PlainText(), code) {
				t.Fatalf("current receipt suppressed by copied content: patches=%d body=%s", got.PatchCount, got.Content)
			}
			if err := a.Annotate(ctx, chat, owner.ID, an); err != nil {
				t.Fatal(err)
			}
			if got := fg.Messages(chat)[0]; got.PatchCount != 1 {
				t.Fatalf("retry duplicated current receipt: %+v", got)
			}
		})
	}
}
