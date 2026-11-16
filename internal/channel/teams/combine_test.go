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

func TestCombineRecoveryPreservesContentAndCounts(t *testing.T) {
	a := &Adapter{}
	ctx := context.Background()
	render := func(text string) channel.Part {
		ps, err := a.Render(ctx, channel.RichDoc{Label: "Codex status", Markdown: text})
		if err != nil || len(ps) != 1 {
			t.Fatalf("render: %v %d", err, len(ps))
		}
		return ps[0]
	}
	repeated := render("Context compacted. Earlier turns were summarized.")
	var parts []channel.Part
	for i := 0; i < 256; i++ {
		parts = append(parts, repeated)
	}
	p, ok := a.CombineRecovery(parts)
	if !ok {
		t.Fatal("recovery declined")
	}
	text := a.PartText(p.Body)
	for _, want := range []string{"Recovered history", "Repeated 256 times:", "Context compacted."} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in %q", want, text)
		}
	}
	if p.Hash != a.BodyHash(p.Body) {
		t.Fatal("transport hash mismatch")
	}
	first, last := render("first **rich** block"), render("last ```literal``` block")
	p, ok = a.CombineRecovery([]channel.Part{first, repeated, repeated, last})
	if !ok {
		t.Fatal("mixed recovery declined")
	}
	text = a.PartText(p.Body)
	if !(strings.Index(text, "first rich") < strings.Index(text, "Repeated 2") && strings.Index(text, "Repeated 2") < strings.Index(text, "last")) {
		t.Fatal("order lost", text)
	}
	if _, ok = a.CombineRecovery([]channel.Part{first}); !ok {
		t.Fatal("single recovery declined")
	}
	unsafe := first
	unsafe.Body = `{"body":{"contentType":"html","content":"x"},"attachments":[{}]}`
	if _, ok = a.CombineRecovery([]channel.Part{unsafe, unsafe}); ok {
		t.Fatal("unsafe repeated attachment combined")
	}
	huge := first
	huge.Body = `{"body":{"contentType":"html","content":"` + strings.Repeat("x", targetPartBytes) + `"}}`
	if _, ok = a.CombineRecovery([]channel.Part{first, huge}); ok {
		t.Fatal("oversized recovery combined")
	}
}

func BenchmarkCombineRecovery256(b *testing.B) {
	a := &Adapter{}
	ps, err := a.Render(context.Background(), channel.RichDoc{Label: "Codex status", Markdown: "Context compacted. Earlier turns were summarized so Codex can continue the thread."})
	if err != nil {
		b.Fatal(err)
	}
	parts := make([]channel.Part, 256)
	for i := range parts {
		parts[i] = ps[0]
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := a.CombineRecovery(parts); !ok {
			b.Fatal("declined")
		}
	}
}
