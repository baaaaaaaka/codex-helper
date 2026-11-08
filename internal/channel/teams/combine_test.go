package teams

import (
	"context"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

func TestCombineProgressPreservesWholeRichBlocks(t *testing.T) {
	a := &Adapter{}
	texts := []string{"first **bold**\n\n```go\nprintln(1)\n```", "第二段\n\n| a | b |\n|---|---|\n| x | y |"}
	var parts []channel.Part
	for _, text := range texts {
		ps, err := a.Render(context.Background(), channel.RichDoc{Label: "Codex status", Markdown: text})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, ps...)
	}
	p, ok := a.CombineProgress(parts)
	if !ok {
		t.Fatal("safe combination declined")
	}
	text := a.PartText(p.Body)
	for _, want := range []string{"first bold", "println(1)", "第二段", "x"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %q", want, text)
		}
	}
	if p.Hash != a.BodyHash(p.Body) {
		t.Fatal("merged hash cannot resolve read-back")
	}
	for _, body := range []string{`{"body":{"contentType":"html","content":"<p>image</p>"},"hostedContents":[{}]}`, `{"body":{"contentType":"html","content":"<p>file</p>"},"attachments":[{}]}`, `{"body":{"contentType":"html","content":"<p>mention</p>"},"mentions":[{}]}`} {
		unsafe := parts[1]
		unsafe.Body = body
		if _, ok := a.CombineProgress([]channel.Part{parts[0], unsafe}); ok {
			t.Fatal("combined dependent attachment references")
		}
	}
	large := parts[1]
	large.Body = `{"body":{"contentType":"html","content":"` + strings.Repeat("x", targetPartBytes) + `"}}`
	if _, ok := a.CombineProgress([]channel.Part{parts[0], large}); ok {
		t.Fatal("oversize combined payload")
	}
}
