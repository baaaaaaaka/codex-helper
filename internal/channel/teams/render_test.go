package teams

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image"
	"image/png"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

var update = flag.Bool("update", false, "rewrite golden files")

type fakeMath struct {
	fail  map[string]bool
	calls int
	srcs  []string
}

func tinyPNG(w, h int) []byte {
	var b bytes.Buffer
	_ = png.Encode(&b, image.NewGray(image.Rect(0, 0, w, h)))
	return b.Bytes()
}

func (f *fakeMath) Render(_ context.Context, srcs []string) ([]MathResult, error) {
	f.calls++
	f.srcs = append(f.srcs, srcs...)
	out := make([]MathResult, len(srcs))
	for i, s := range srcs {
		if f.fail[s] {
			out[i] = MathResult{Err: errors.New("mathjax_error")}
			continue
		}
		out[i] = MathResult{PNG: tinyPNG(40, 10), Width: 40, Height: 10}
	}
	return out, nil
}

var goldenCases = []struct {
	name string
	doc  channel.RichDoc
	fail map[string]bool
}{
	{name: "fences", doc: channel.RichDoc{Label: "🤖 ✅ Codex answer", Markdown: "Run:\n\n```go\nfunc main() {\n\tfmt.Println(\"<hi> & bye\")\n}\n```\n\n~~~\nunterminated *not em*"}},
	{name: "table", doc: channel.RichDoc{Label: "Codex answer", Markdown: "| a | b |\n|---|:-:|\n| `x|y` | **b** |\n| 1 \\| 2 | |\nafter"}},
	{name: "cjk", doc: channel.RichDoc{Label: "🧑‍💻 User", Markdown: "你好，世界！**粗体**和`代码`。\n\n- 列表项一\n- 列表项二\n\n| 名称 | 值 |\n| --- | --- |\n| 甲 | 乙 |"}},
	{name: "lists", doc: channel.RichDoc{Label: "L", Markdown: "1. one\n2. two\n   - nested *em*\n   - [x] done\n3. three\n   continued lazily\n\n5) five\n\n- [ ] todo\n* other list"}},
	{name: "inline", doc: channel.RichDoc{Label: "L", Markdown: "see [docs](https://example.com/a_b) and https://example.com/x). <https://a.b/c> [bad](javascript:alert(1)) ![img](https://i.example/p.png) snake_case_word *em* **strong** ***both*** ~~gone~~ \\*literal\\* <b>raw</b> 2*3*4 [same](https://same.example)\n    indented line kept\n  two-space indent"}},
	{name: "blocks", doc: channel.RichDoc{Label: "L", Markdown: "# Title\nSetext\n===\n## Sub\n### Third\n#### Four\n> quoted **text**\nlazy\n> > nested\n\n---\n\n    indented code\n\nend"}},
	{name: "math-ok", doc: channel.RichDoc{Label: "Codex answer", Math: true, Markdown: "Energy:\n\n<m>E = mc^2</m>\n\ninline <m>a+b</m> here\n\n<m>\n\\frac{1}{2}\n</m>\n\n- in list <m>x</m>\n\n<m>x</m> trailing text"}},
	{name: "math-fallback", fail: map[string]bool{`\broken{`: true, "z^2": true}, doc: channel.RichDoc{Label: "Codex answer", Math: true, Markdown: "<m>z^2</m>\n\n<m>\\href{https://x}{y}</m>\n\n<m>C:\\path</m>"}},
	{name: "math-off", doc: channel.RichDoc{Label: "🧑‍💻 User", Markdown: "<m>E=mc^2</m> stays text"}},
	{name: "mention", doc: channel.RichDoc{Label: "🤖 ✅ Codex answer", Mention: true, Markdown: "done"}},
	{name: "empty", doc: channel.RichDoc{Label: "🔧 Helper", Markdown: ""}},
}

func TestRenderGolden(t *testing.T) {
	ctx := context.Background()
	for _, c := range goldenCases {
		t.Run(c.name, func(t *testing.T) {
			fg, a := newFake(t, fakegraph.Options{}, func(cfg *Config) { cfg.Math = &fakeMath{fail: c.fail} })
			parts, err := a.Render(ctx, c.doc)
			if err != nil {
				t.Fatal(err)
			}
			var got strings.Builder
			for _, p := range parts {
				b := decodePart(t, p)
				fmt.Fprintf(&got, "== part %d/%d size=%d mention=%v images=%d\n%s\n", p.Index, p.Count, p.Size, p.Mention, len(b.HostedContents), b.Body.Content)
			}
			path := filepath.Join("testdata", c.name+".golden")
			if *update {
				if err := os.WriteFile(path, []byte(got.String()), 0o644); err != nil {
					// Read-only source tree (sandboxed runs): emit for the caller.
					fmt.Printf("GOLDEN-BEGIN %s\n%sGOLDEN-END\n", c.name, got.String())
					return
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v (run with -update)", err)
			}
			if got.String() != string(want) {
				t.Fatalf("golden mismatch for %s:\n--- got\n%s\n--- want\n%s", c.name, got.String(), want)
			}
			// Every rendered part is accepted by the Graph contract and its
			// echo hashes to the same value.
			chat := fg.CreateMeetingChatAsUser(c.name).ID
			for _, p := range parts {
				if _, err := a.Send(ctx, chat, p, "g-"+c.name); err != nil {
					t.Fatalf("send: %v", err)
				}
			}
			w, err := a.Receive(ctx, chat, "")
			if err != nil {
				t.Fatal(err)
			}
			for i, m := range w.Msgs {
				if m.Hash != parts[i].Hash || m.Marker != "g-"+c.name {
					t.Fatalf("echo %d hash %s marker %q, part hash %s", i, m.Hash, m.Marker, parts[i].Hash)
				}
			}
			noViolations(t, fg)
		})
	}
}

func TestRenderMathCalls(t *testing.T) {
	m := &fakeMath{}
	_, a := newFake(t, fakegraph.Options{}, func(c *Config) { c.Math = m })
	var md strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&md, "<m>x_{%d}</m>\n\n", i)
	}
	parts, err := a.Render(context.Background(), channel.RichDoc{Label: "L", Math: true, Markdown: md.String()})
	if err != nil {
		t.Fatal(err)
	}
	if m.calls != 1 || len(m.srcs) != maxMathFormulas {
		t.Fatalf("renderer calls=%d formulas=%d", m.calls, len(m.srcs))
	}
	images := 0
	for _, p := range parts {
		n := len(decodePart(t, p).HostedContents)
		if n > maxImagesPerPart {
			t.Fatalf("part %d has %d images", p.Index, n)
		}
		images += n
	}
	if images != maxMathFormulas {
		t.Fatalf("%d images, want %d (the rest as code)", images, maxMathFormulas)
	}
	// Renderer returning garbage: no image, code instead.
	bad := mathFunc(func([]string) []MathResult { return []MathResult{{PNG: []byte("not a png"), Width: 1, Height: 1}} })
	_, a2 := newFake(t, fakegraph.Options{}, func(c *Config) { c.Math = bad })
	p, _ := a2.Render(context.Background(), channel.RichDoc{Label: "L", Math: true, Markdown: "<m>y</m>"})
	if b := decodePart(t, p[0]); len(b.HostedContents) != 0 || !strings.Contains(b.Body.Content, "<pre><code>y</code></pre>") {
		t.Fatalf("garbage png used: %s", b.Body.Content)
	}
}

type mathFunc func([]string) []MathResult

func (f mathFunc) Render(_ context.Context, s []string) ([]MathResult, error) { return f(s), nil }

var partLabelRE = regexp.MustCompile(`^<p><strong>L \[part (\d+)/(\d+)\]:</strong>`)

func checkParts(t *testing.T, parts []channel.Part) []string {
	t.Helper()
	var contents []string
	for i, p := range parts {
		b := decodePart(t, p)
		c := b.Body.Content
		if p.Size != len(c) || p.Size+markerReserve > maxPartBytes || p.Size > targetPartBytes {
			t.Fatalf("part %d size %d", i+1, p.Size)
		}
		m := partLabelRE.FindStringSubmatch(c)
		if len(parts) > 1 && (m == nil || m[1] != fmt.Sprint(i+1) || m[2] != fmt.Sprint(len(parts))) {
			t.Fatalf("part %d label: %.80s", i+1, c)
		}
		if (len(b.Mentions) > 0) != (i == 0) || p.Mention != (i == 0) {
			t.Fatalf("part %d mention=%d", i+1, len(b.Mentions))
		}
		contents = append(contents, c)
	}
	return contents
}

func TestRenderSplitsLongDocuments(t *testing.T) {
	ctx := context.Background()
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("long").ID

	var para strings.Builder
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&para, "line %04d with some words & <markup>\n", i)
	}
	var table strings.Builder
	table.WriteString("| id | name |\n|---|---|\n")
	for i := 0; i < 1500; i++ {
		fmt.Fprintf(&table, "| %d | 名前 %d |\n", i, i)
	}
	var list strings.Builder
	for i := 1; i <= 900; i++ {
		fmt.Fprintf(&list, "%d. item %d with a `code` span\n   continued lazily\n", i, i)
	}
	code := "```\n" + strings.Repeat("x := y // 0123456789\n", 3000) + "```\n"
	longLine := strings.Repeat("長", 40000) // one 120 KB line

	for name, md := range map[string]string{"para": para.String(), "table": table.String(), "list": list.String(), "code": code, "longline": longLine} {
		parts, err := a.Render(ctx, channel.RichDoc{Label: "L", Markdown: md, Mention: true})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(parts) < 2 {
			t.Fatalf("%s: %d parts", name, len(parts))
		}
		contents := checkParts(t, parts)
		all := strings.Join(contents, "")
		switch name {
		case "table":
			for i, c := range contents {
				if !strings.Contains(c, "<table><tr><th>id</th><th>name</th></tr>") {
					t.Fatalf("table part %d lacks the header", i+1)
				}
			}
			if strings.Count(all, "<td>") != 3000 {
				t.Fatalf("table cells %d", strings.Count(all, "<td>"))
			}
		case "list":
			if strings.Count(all, "<li>item ") != 900 {
				t.Fatalf("list items %d", strings.Count(all, "<li>item "))
			}
			for i, c := range contents[1:] {
				// Each part opens its own <ol> (v0.1.20 wrote no start attribute).
				if !strings.Contains(c, "</p><ol><li>item ") || strings.Contains(c, "start=") {
					t.Fatalf("list part %d does not reopen the list: %.120s", i+2, c)
				}
				// No item is cut: every part closes all items it opens.
				if strings.Count(c, "<li>") != strings.Count(c, "</li>") {
					t.Fatalf("list part %d cuts an item", i+2)
				}
			}
		case "para":
			if strings.Count(all, "line ") != 2000 || !strings.Contains(all, "&amp; &lt;markup&gt;") {
				t.Fatal("paragraph lines lost")
			}
		case "code":
			if strings.Count(all, "x := y") != 3000 {
				t.Fatal("code lines lost")
			}
		case "longline":
			if strings.Count(all, "長") != 40000 {
				t.Fatalf("runes lost: %d", strings.Count(all, "長"))
			}
		}
		for _, p := range parts {
			if _, err := a.Send(ctx, chat, p, "s-"+name); err != nil {
				t.Fatalf("%s send: %v", name, err)
			}
		}
	}
	noViolations(t, fg)
}

func TestRenderAttachesFilesToLastPart(t *testing.T) {
	_, a := newFake(t, fakegraph.Options{})
	ref, _ := json.Marshal(driveRef{ItemID: "item", AttachmentID: "0a1b2c3d-0000-4000-8000-000000000001", URL: "https://t.sharepoint.com/f/report.pdf", Name: "report.pdf"})
	doc := channel.RichDoc{Label: "L", Markdown: strings.Repeat("word\n", 8000), Files: []channel.FileRef{{Name: "report.pdf", Ref: "drive:" + string(ref)}}}
	parts, err := a.Render(context.Background(), doc)
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range parts {
		b := decodePart(t, p)
		last := i == len(parts)-1
		if (len(b.Attachments) == 1) != last || strings.Contains(b.Body.Content, "<attachment id=") != last {
			t.Fatalf("part %d attachments %d", i+1, len(b.Attachments))
		}
	}
	if _, err := a.Render(context.Background(), channel.RichDoc{Files: []channel.FileRef{{Ref: "share:https://x"}}}); err == nil {
		t.Fatal("inbound ref accepted as outbound file")
	}
}

func TestSafeTeX(t *testing.T) {
	ok := []string{`E = mc^2`, `\frac{a}{b}`, `\$5`, `\text{if } x > 0`}
	bad := []string{"", `\href{x}{y}`, `a $ b`, `\(x\)`, `https://x`, `/home/u/x`, `C:\x`, `a@b.example`, `{`, `}{`, strings.Repeat("{", 65) + strings.Repeat("}", 65), strings.Repeat("x", 9000), strings.Repeat("&", 4000), `\newcommand{\a}{b}`}
	for _, s := range ok {
		if !safeTeX(s) {
			t.Errorf("rejected %q", s)
		}
	}
	for _, s := range bad {
		if safeTeX(s) {
			t.Errorf("accepted %.40q", s)
		}
	}
}

// A row with more cells than the header keeps the extra text in its last
// column (v0.1.20 behaviour). A prompt pasted as a table row followed by
// the question on the same line lost the question.
func TestRenderTableKeepsCellsPastTheHeader(t *testing.T) {
	_, a := newFake(t, fakegraph.Options{})
	md := "|  |  |  |\n|---|---|---|\n| 重写 | 修改已有行 | 缓存失效 |  什么情况下会出现这种 case？ | 另外\nafter"
	p := renderOne(t, a, channel.RichDoc{Label: "User", Markdown: md})
	c := decodePart(t, p).Body.Content
	want := "<td>重写</td><td>修改已有行</td><td>缓存失效 | 什么情况下会出现这种 case？ | 另外</td></tr></table>"
	if !strings.Contains(c, want) {
		t.Fatalf("extra cells lost:\n%s", c)
	}
	if !strings.Contains(c, "after") {
		t.Fatalf("text after the table lost:\n%s", c)
	}
}
