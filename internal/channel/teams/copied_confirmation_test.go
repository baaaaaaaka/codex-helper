package teams

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// Reduced from the incident's Graph HTML, retaining list breaks, old receipts,
// spans and the trailing codeblock. No private chat IDs or credentials remain.
func TestPromptConfirmationCopiedHTMLLostPatchRestart(t *testing.T) {
	raw, err := os.ReadFile("testdata/copied-conversation.html")
	if err != nil {
		t.Fatal(err)
	}
	original := strings.TrimSpace(string(raw))
	for _, stripped := range []bool{false, true} {
		t.Run(map[bool]string{false: "comments", true: "stripped"}[stripped], func(t *testing.T) {
			ctx := context.Background()
			fg, a := newFake(t, fakegraph.Options{StripHTMLComments: stripped})
			chat := fg.CreateMeetingChatAsUser("copy").ID
			owner, err := fg.PostHTMLAsUser(chat, original)
			if err != nil {
				t.Fatal(err)
			}
			p := renderOne(t, a, channel.RichDoc{Label: "🔧 Helper", Markdown: "⏳ Codex is working. Request accepted."})
			an := channel.Annotation{Confirmation: &p, ConfirmationKey: "ack:same-local-key"}
			fg.AddFault(fakegraph.Fault{Name: "lost", Kind: fakegraph.FaultAcceptedLost, Route: "PATCH /chats/{chat}/messages/{message}", Times: 1})
			if err := a.Annotate(ctx, chat, owner.ID, an); !channel.IsAmbiguous(err) {
				t.Fatalf("lost PATCH: %v", err)
			}
			got := fg.Messages(chat)[0]
			if got.PatchCount != 1 || !strings.HasPrefix(got.Content, original+"<hr/>") {
				t.Fatalf("original changed or receipt missing: %s", got.Content)
			}
			if strings.Count(got.PlainText(), "Request accepted.") != 2 {
				t.Fatal("old receipt suppressed new receipt")
			}
			// A later transcript/label edit and HTML reserialization must keep identity.
			normalized := strings.ReplaceAll(got.Content, `<a href="`, `<a title='request' href='`)
			normalized = strings.ReplaceAll(normalized, `"><strong>`, `'><strong>`)
			// Exercise the normalizing parser without relying on serialized attributes.
			next, err := confirmationEdit(chat, owner.ID, normalized, true, nil, an)
			if err != nil || next != "" {
				t.Fatalf("normalized link not recognized: %v %s", err, next)
			}
			if err := fg.EditHTMLAsUser(chat, owner.ID, normalized); err != nil {
				t.Fatal(err)
			}
			restarted, err := New(a.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.Annotate(ctx, chat, owner.ID, an); err != nil {
				t.Fatal(err)
			}
			if fg.Messages(chat)[0].PatchCount != 1 {
				t.Fatal("restart duplicated applied PATCH")
			}
			if fg.Messages(chat)[0].Content != normalized {
				t.Fatal("retry changed normalized original content")
			}
			// Copy a new-format receipt too. Local outbox IDs may repeat on another
			// machine: the target message, not the local key or visible text, owns it.
			other, err := fg.PostHTMLAsUser(chat, got.Content)
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.Annotate(ctx, chat, other.ID, an); err != nil {
				t.Fatal(err)
			}
			if fg.Messages(chat)[1].PatchCount != 1 {
				t.Fatal("copied new-format receipt suppressed acknowledgement")
			}
			noViolations(t, fg)
		})
	}
}

func TestCopiedHTMLParsingPreservesCodeOrder(t *testing.T) {
	raw, err := os.ReadFile("testdata/copied-conversation.html")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for name, body := range map[string]string{"source": source, "plain": plainText(source)} {
		if strings.Count(body, "tuiTail") != 1 || strings.Index(body, "tuiTail") < strings.LastIndex(body, "Codex finished responding.") {
			t.Fatalf("%s changed the captured code order", name)
		}
	}
	next, err := labelEdit(source, true, nil, "🧑‍💻 User")
	if err != nil || !strings.HasSuffix(next, source) {
		t.Fatalf("label changed original HTML: %v", err)
	}
}
