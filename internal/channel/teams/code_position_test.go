package teams

import (
	"context"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// Cover rendering and readback without claiming that
// the Graph fake models the Teams client or its clipboard behavior.
func TestCodePositionRenderAndReadback(t *testing.T) {
	const code = `tuiTail = []string{"resume", shared.Endpoint.ThreadID}`
	for _, inList := range []bool{false, true} {
		name, prefix, indent := "paragraph", "BEFORE", ""
		if inList {
			name, prefix, indent = "list", "1. BEFORE", "   "
		}
		t.Run(name, func(t *testing.T) {
			fg, a := newFake(t, fakegraph.Options{})
			ctx := context.Background()
			chat := fg.CreateMeetingChatAsUser(name).ID
			md := prefix + "\n\n" + indent + "```go\n" + indent + code + "\n" + indent + "```\n\nAFTER"
			parts, err := a.Render(ctx, channel.RichDoc{Label: "Codex answer", Markdown: md, Notes: []channel.Note{{Label: "Helper", Text: "MODEL-METADATA", SeparatorBefore: true}}, FooterLabel: "Helper", Footer: "FINISHED"})
			if err != nil {
				t.Fatal(err)
			}
			var html, plain, echo strings.Builder
			for _, p := range parts {
				html.WriteString(decodePart(t, p).Body.Content)
				plain.WriteString(a.PartText(p.Body))
				sent, err := a.Send(ctx, chat, p, "code-position-"+name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := a.Get(ctx, chat, sent.ID)
				if err != nil {
					t.Fatal(err)
				}
				echo.WriteString(got.Text)
			}
			for name, text := range map[string]string{"html": html.String(), "readback": plain.String(), "graph-fake-echo": echo.String()} {
				before, content, after, footer := strings.Index(text, "BEFORE"), strings.Index(text, "tuiTail"), strings.Index(text, "AFTER"), strings.Index(text, "FINISHED")
				metadata := strings.Index(text, "MODEL-METADATA")
				separator := strings.LastIndex(text, "———")
				if name == "html" {
					separator = strings.LastIndex(text, "<hr/>")
				}
				if before < 0 || content <= before || after <= content || separator <= after || metadata <= separator || footer <= metadata || strings.Count(text, "tuiTail") != 1 {
					t.Fatalf("%s reordered or lost code: %s", name, text)
				}
			}
		})
	}
}
