package harness

import (
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestLogicalProgressPreservesRepeatedBlocksAndSourceOrder(t *testing.T) {
	const heading = "<p><strong>🤖 ⏳ Codex status:</strong></p>"
	const marker = "<!-- codex-helper-outbox:token -->"
	a := heading + "<p>same</p>"
	b := heading + "<p>last</p>"
	got := logicalProgressMessages([]fakegraph.Message{
		{ID: "1", Origin: fakegraph.OriginSUT, Content: marker + a + "<hr>" + a + "<hr>" + b},
		{ID: "2", Origin: fakegraph.OriginSUT, Content: "final"},
	})
	if len(got) != 4 || got[0].Content != marker+a || got[1].Content != marker+a || got[2].Content != marker+b || got[3].Content != "final" {
		t.Fatalf("logical content/order changed: %+v", got)
	}
	if got[0].ID == got[1].ID || got[1].ID == got[2].ID {
		t.Fatal("repeated blocks lost their identity")
	}
}

func TestLogicalProgressDoesNotSplitOtherHTML(t *testing.T) {
	for _, origin := range []fakegraph.Origin{fakegraph.OriginSUT, fakegraph.OriginUser} {
		body := "<p>Codex answer</p><hr><p>More content</p>"
		got := logicalProgressMessages([]fakegraph.Message{{Origin: origin, Content: body}})
		if len(got) != 1 || !strings.Contains(got[0].Content, "<hr>") {
			t.Fatal("ordinary content was normalized as progress")
		}
	}
}
