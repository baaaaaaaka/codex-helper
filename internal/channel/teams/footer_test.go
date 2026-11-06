package teams

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// A footer closes the last part only, as v0.1.20 appended
// "<p><strong>🔧 Helper:</strong> ✅ Codex finished responding.</p>" to the
// last part of an answer, and its bytes are reserved so that part still fits.
func TestRenderFooterOnLastPart(t *testing.T) {
	ctx := context.Background()
	_, a := newFake(t, fakegraph.Options{})
	const want = "<p><strong>🔧 Helper:</strong> ✅ Codex finished &lt;responding&gt;.</p>"

	parts, err := a.Render(ctx, channel.RichDoc{Label: "🤖 ✅ Codex answer", Markdown: "Done.", FooterLabel: "🔧 Helper", Footer: "✅ Codex finished <responding>."})
	if err != nil {
		t.Fatal(err)
	}
	if c := decodePart(t, parts[0]).Body.Content; len(parts) != 1 || c != "<p><strong>🤖 ✅ Codex answer:</strong></p><p>Done.</p>"+want {
		t.Fatalf("one part: %d %q", len(parts), c)
	}

	var md strings.Builder
	for i := 0; i < 4000; i++ {
		fmt.Fprintf(&md, "line %04d with some words\n", i)
	}
	parts, err = a.Render(ctx, channel.RichDoc{Label: "L", Markdown: md.String(), Mention: true, FooterLabel: "🔧 Helper", Footer: "✅ Codex finished <responding>."})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) < 2 {
		t.Fatalf("%d parts", len(parts))
	}
	for i, c := range checkParts(t, parts) {
		if strings.HasSuffix(c, want) != (i == len(parts)-1) || strings.Count(c, "finished") > 1 {
			t.Fatalf("part %d footer: …%s", i+1, c[max(0, len(c)-120):])
		}
	}

	// No footer: nothing is appended.
	parts, err = a.Render(ctx, channel.RichDoc{Label: "L", Markdown: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if c := decodePart(t, parts[0]).Body.Content; strings.Contains(c, "Helper") {
		t.Fatalf("unexpected footer: %q", c)
	}
}
