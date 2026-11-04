package teams

import (
	"context"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestBodyHashMatchesRenderedPartHash(t *testing.T) {
	_, a := newFake(t, fakegraph.Options{})
	var _ channel.BodyHasher = a
	docs := []channel.RichDoc{
		{Label: "🔧 Helper", Markdown: "Request accepted. Queued behind 2 earlier requests."},
		{Label: "Codex", Markdown: "# Title\n\nSome *text* with `code` and a table:\n\n| a | b |\n|---|---|\n| 1 | 2 |\n", Mention: true},
		{Label: "Codex", Markdown: strings.Repeat("A long paragraph of words. ", 3000)},
	}
	for i, d := range docs {
		parts, err := a.Render(context.Background(), d)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range parts {
			if got := a.BodyHash(p.Body); got != p.Hash {
				t.Fatalf("doc %d part %d: BodyHash %s, Part.Hash %s", i, p.Index, got, p.Hash)
			}
		}
	}
	if a.BodyHash("plain legacy text") != channel.TextHash("plain legacy text") {
		t.Fatal("non-JSON body is not hashed as text")
	}
}
