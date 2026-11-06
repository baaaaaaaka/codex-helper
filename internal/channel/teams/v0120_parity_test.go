package teams

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// v0120Cases are inputs from v0.1.20's own Teams Markdown tests (plus the
// formula and blank-line cases) with the exact body v0.1.20 rendered for
// them under the label "L" (formulas as code: no renderer).
var v0120Cases = []struct {
	name string
	md   string
	math bool
	want string
}{
	{
		name: "subset",
		md:   "## Summary\n\n- **fixed** *rendering* with `safe <tag>` and ~~old~~\n- link [docs](https://example.com/a?x=1&y=2)\n- local [fake](./internal/teams/render.go:12)\n\n```go\nfmt.Println(\"<safe>\")\n```\n\nRaw <b>html</b>",
		math: false,
		want: "<p><strong>L:</strong></p><p><strong>Summary</strong></p><ul><li><strong>fixed</strong> <em>rendering</em> with <code>safe &lt;tag&gt;</code> and <s>old</s></li><li>link <a href=\"https://example.com/a?x=1&amp;y=2\">docs</a> (https://example.com/a?x=1&amp;y=2)</li><li>local <code>./internal/teams/render.go:12</code></li></ul><pre><code>fmt.Println(&#34;&lt;safe&gt;&#34;)</code></pre><p>Raw &lt;b&gt;html&lt;/b&gt;</p>",
	},
	{
		name: "nested list",
		md:   "1. parent\n   - nested\n   - more",
		math: false,
		want: "<p><strong>L:</strong></p><ol><li>parent<br><ul><li>nested</li><li>more</li></ul></li></ol>",
	},
	{
		name: "rules and quotes",
		md:   "---\n**1. codex-helper**\n> `/home/baka/project/codex-helper`\n> Sessions: 1 active, 2 idle, last updated 2026-05-12 14:08\n>\n> **Next:** send `1` to select this project\n---",
		math: false,
		want: "<p><strong>L:</strong></p><hr/><p><strong>1. codex-helper</strong></p><blockquote><p><code>/home/baka/project/codex-helper</code><br>Sessions: 1 active, 2 idle, last updated 2026-05-12 14:08</p><p><strong>Next:</strong> send <code>1</code> to select this project</p></blockquote><hr/>",
	},
	{
		name: "fenced blank lines",
		md:   "before\n\n```\nline 1\n\nline 3\n```\n\nafter",
		math: false,
		want: "<p><strong>L:</strong></p><p>before</p><pre><code>line 1\n\nline 3</code></pre><p>after</p>",
	},
	{
		name: "headings",
		md:   "# H1\n## H2 **bold** ##\n### H3 `x < y`\n#### H4\n###### H6\n####### literal\n#literal",
		math: false,
		want: "<p><strong>L:</strong></p><p><strong>H1</strong></p><p><strong>H2 <strong>bold</strong></strong></p><p><strong><em>H3 <code>x &lt; y</code></em></strong></p><p><em>H4</em></p><p><em>H6</em></p><p>####### literal<br>#literal</p>",
	},
	{
		name: "tilde fences",
		md:   "~~~~markdown\n```not end\n~~~\nstill code\n~~~~\nafter",
		math: false,
		want: "<p><strong>L:</strong></p><pre><code>```not end\n~~~\nstill code</code></pre><p>after</p>",
	},
	{
		name: "fence in list item",
		md:   "1. **Control Chat**\n   一个固定 chat，比如：\n\n   ```text\n   codex-helper control\n   ```\n\n   你在里面发管理命令：\n\n   ```text\n   new fix installer bug\n   list\n   status\n   ```\n\n  最小 Python 实现骨架\n\n  import time, json, os, requests\n\n  def graph(method, path, **kwargs):\n      t = token()[\"access_token\"]\n      return r.json() if r.text else {}\n\n  Graph Teams API 对应 MIAO 的调用",
		math: false,
		want: "<p><strong>L:</strong></p><ol><li><strong>Control Chat</strong><br>&nbsp;&nbsp;&nbsp;一个固定 chat，比如：<br><pre><code>codex-helper control</code></pre><br>&nbsp;&nbsp;&nbsp;你在里面发管理命令：<br><pre><code>new fix installer bug\nlist\nstatus</code></pre><br>&nbsp;&nbsp;最小 Python 实现骨架<br><pre><code>import time, json, os, requests\n\ndef graph(method, path, **kwargs):\n    t = token()[&#34;access_token&#34;]\n    return r.json() if r.text else {}</code></pre><br>&nbsp;&nbsp;Graph Teams API 对应 MIAO 的调用</li></ol>",
	},
	{
		name: "indented code",
		md:   "Intro\n\n    line 1\n\n    line 3\n\nDone",
		math: false,
		want: "<p><strong>L:</strong></p><p>Intro</p><pre><code>line 1\n\nline 3</code></pre><p>Done</p>",
	},
	{
		name: "nested list after blank",
		md:   "- parent\n\n    - child after blank\n    continuation",
		math: false,
		want: "<p><strong>L:</strong></p><ul><li>parent<br><ul><li>child after blank<br>continuation</li></ul></li></ul>",
	},
	{
		name: "quote markers",
		md:   "> quoted **bold**\n> \n> - item\n>> nested\nnormal",
		math: false,
		want: "<p><strong>L:</strong></p><blockquote><p>quoted <strong>bold</strong></p><ul><li>item</li></ul><blockquote><p>nested</p></blockquote></blockquote><p>normal</p>",
	},
	{
		name: "rules and non rules",
		md:   "before\n\n---\nafter\n\n* * *\n___\n- - -\nnot --- rule",
		math: false,
		want: "<p><strong>L:</strong></p><p>before</p><hr/><p>after</p><hr/><hr/><hr/><p>not --- rule</p>",
	},
	{
		name: "inline",
		md:   "**outer ~~strike **inner**~~ end** Keep snake_case and mid_word_value, but _em_ and __strong__ render. Unclosed **bold and ~~strike and *em\nBefore `**not bold** <tag> [x](javascript:alert(1))` after; Use ``code with ` tick and <x>`` ok\nEscaped \\[not link\\]\\(x\\), \\# heading, \\*not em\\*, \\`not code\\`",
		math: false,
		want: "<p><strong>L:</strong></p><p><strong>outer <s>strike <strong>inner</strong></s> end</strong> Keep snake_case and mid_word_value, but <em>em</em> and <strong>strong</strong> render. Unclosed **bold and ~~strike and *em<br>Before <code>**not bold** &lt;tag&gt; [x](javascript:alert(1))</code> after; Use <code>code with ` tick and &lt;x&gt;</code> ok<br>Escaped [not link](x), # heading, *not em*, `not code`</p>",
	},
	{
		name: "links",
		md:   "See <https://example.com/a?x=1&y=2> and <mailto:user+tag@example.com>.\nBad <javascript:alert(1)> <ftp://example.com/file>\n[spec](https://example.com/path(foo)/bar?q=(x)&ok=1) [docs]( <https://example.com/a(b)?q=1&x=2> \"ignored title\" )\n[rel](./internal/teams/render.go:12) [unc](\\\\server\\share\\report.md) [js](javascript:alert(1)) [relative](notes.md) [same](https://same.example)",
		math: false,
		want: "<p><strong>L:</strong></p><p>See <a href=\"https://example.com/a?x=1&amp;y=2\">https://example.com/a?x=1&amp;y=2</a> and <a href=\"mailto:user+tag@example.com\">mailto:user+tag@example.com</a>.<br>Bad &lt;javascript:alert(1)&gt; &lt;ftp://example.com/file&gt;<br><a href=\"https://example.com/path(foo)/bar?q=(x)&amp;ok=1\">spec</a> (https://example.com/path(foo)/bar?q=(x)&amp;ok=1) <a href=\"https://example.com/a(b)?q=1&amp;x=2\">docs</a> (https://example.com/a(b)?q=1&amp;x=2)<br><code>./internal/teams/render.go:12</code> <code>//server/share/report.md</code> js (javascript:alert(1)) relative (notes.md) <a href=\"https://same.example\">same</a> (https://same.example)</p>",
	},
	{
		name: "lists and bare urls",
		md:   "## Tasks ##\n\n- **inspect** renderer\n- link https://example.com/a?x=1&y=2.\n  wrapped continuation\n1. first\n2) second [docs](https://example.com/docs)\n\n[broken](https://example.com\n(https://example.org/wrapped).",
		math: false,
		want: "<p><strong>L:</strong></p><p><strong>Tasks</strong></p><ul><li><strong>inspect</strong> renderer</li><li>link <a href=\"https://example.com/a?x=1&amp;y=2\">https://example.com/a?x=1&amp;y=2</a>.<br>&nbsp;&nbsp;wrapped continuation</li></ul><ol><li>first</li><li>second <a href=\"https://example.com/docs\">docs</a> (https://example.com/docs)</li></ol><p>[broken](https://example.com<br>(<a href=\"https://example.org/wrapped\">https://example.org/wrapped</a>).</p>",
	},
	{
		name: "tasks and images",
		md:   "- [ ] write tests\n- [x] fix renderer\n1. [X] ship safely\n\n![plot <x>](https://example.com/plot.png)\n![local](file:///tmp/plot.png)\n![bad](javascript:alert(1))",
		math: false,
		want: "<p><strong>L:</strong></p><ul><li>☐ write tests</li><li>☑ fix renderer</li></ul><ol><li>☑ ship safely</li></ol><p>Image: <a href=\"https://example.com/plot.png\">plot &lt;x&gt;</a> (https://example.com/plot.png)<br>Image: <code>/tmp/plot.png</code><br>Image: bad (javascript:alert(1))</p>",
	},
	{
		name: "tables",
		md:   "| Name | Result | Link |\n| --- | :---: | ---: |\n| **build** | `ok|pass` | [docs](https://example.com/a?x=1&y=2) |\n| raw | <b>x</b> | [bad](javascript:alert(1)) |\n\na \\| b | img | extra\n- | - | -\n`x|y` | ![plot](https://example.com/plot.png) | one | two\nmissing | only\n\n| a | b |\n| --- | nope |\n| c | d |",
		math: false,
		want: "<p><strong>L:</strong></p><table><tr><th>Name</th><th>Result</th><th>Link</th></tr><tr><td><strong>build</strong></td><td><code>ok|pass</code></td><td><a href=\"https://example.com/a?x=1&amp;y=2\">docs</a> (https://example.com/a?x=1&amp;y=2)</td></tr><tr><td>raw</td><td>&lt;b&gt;x&lt;/b&gt;</td><td>bad (javascript:alert(1))</td></tr></table><table><tr><th>a | b</th><th>img</th><th>extra</th></tr><tr><td><code>x|y</code></td><td>Image: <a href=\"https://example.com/plot.png\">plot</a> (https://example.com/plot.png)</td><td>one | two</td></tr><tr><td>missing</td><td>only</td><td></td></tr></table><p>| a | b |<br>| --- | nope |<br>| c | d |</p>",
	},
	{
		name: "chat link",
		md:   `**Chat:** [▶ Open chat](https://teams.microsoft.com/l/chat/19%3Aabc%40thread.v2/0?tenantId=t)`,
		math: false,
		want: "<p><strong>L:</strong></p><p><strong>Chat:</strong> <a href=\"https://teams.microsoft.com/l/chat/19%3Aabc%40thread.v2/0?tenantId=t\">▶ Open chat</a></p>",
	},
	{
		name: "blank runs",
		md:   "\n\n  first\n\n\n\nsecond   \n\n\n- a\n\n\n- b\n",
		math: false,
		want: "<p><strong>L:</strong></p><p>&nbsp;&nbsp;first</p><p>second   </p><ul><li>a</li><li>b</li></ul>",
	},
	{
		name: "math",
		md:   "Energy:\n\n<m>E = mc^2</m>\n\nratio <m>\\frac{a}{b}</m> inline <m>a+b</m> here\n\n<m>\n\\frac{1}{2}\n</m>\n\n- in list <m>x</m>\n\n> quoted <m>y</m>\n\n| a | <m>z</m> |\n|---|---|\n\n`<m>code</m>` and <m>\\href{x}{y}</m>",
		math: true,
		want: "<p><strong>L:</strong></p><p>Energy:</p><pre><code>E = mc^2</code></pre><p>ratio </p><pre><code>\\frac{a}{b}</code></pre><p>&nbsp;inline <code>a+b</code> here</p><pre><code>\n\\frac{1}{2}\n</code></pre><ul><li>in list <code>x</code></li></ul><blockquote><p>quoted <code>y</code></p></blockquote><table><tr><th>a</th><th><code>z</code></th></tr></table><p><code>&lt;m&gt;code&lt;/m&gt;</code> and &lt;m&gt;\\href{x}{y}&lt;/m&gt;</p>",
	},
	{
		name: "math off",
		md:   `<m>E=mc^2</m> stays text`,
		math: false,
		want: "<p><strong>L:</strong></p><p>&lt;m&gt;E=mc^2&lt;/m&gt; stays text</p>",
	},
}

// A single message renders exactly as v0.1.20 rendered it.
func TestRenderMatchesV0120(t *testing.T) {
	_, a := newFake(t, fakegraph.Options{})
	for _, c := range v0120Cases {
		t.Run(c.name, func(t *testing.T) {
			p := renderOne(t, a, channel.RichDoc{Label: "L", Markdown: c.md, Math: c.math})
			if got := decodePart(t, p).Body.Content; got != c.want {
				t.Fatalf("body differs from v0.1.20:\n got: %s\nwant: %s", got, c.want)
			}
		})
	}
}

// A line too long for one part is cut, but never through a formula's
// placeholder: every formula still renders as code.
func TestRenderLongLineKeepsFormulasWhole(t *testing.T) {
	_, a := newFake(t, fakegraph.Options{})
	const n = 4000
	md := "x " + strings.Repeat("<m>a</m>", n)
	parts, err := a.Render(context.Background(), channel.RichDoc{Label: "L", Markdown: md, Math: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) < 2 {
		t.Fatalf("got %d parts, want the line cut", len(parts))
	}
	codes := 0
	for _, p := range parts {
		c := decodePart(t, p).Body.Content
		if strings.ContainsAny(c, "") || strings.Contains(c, "cxp-math") {
			t.Fatalf("part %d holds a cut formula placeholder", p.Index)
		}
		codes += strings.Count(c, "<code>a</code>")
	}
	if codes != n {
		t.Fatalf("%d formulas rendered, want %d", codes, n)
	}
}

// Long lines render in linear time. Searching for a closer that does not
// exist (an unclosed "*" or "_" before every word, "[" without "]") and
// lower-casing the rest of the line at every position made v0.1.20's
// inline renderer quadratic: these took from seconds to minutes.
func TestRenderLongLinesAreFast(t *testing.T) {
	_, a := newFake(t, fakegraph.Options{})
	for _, unit := range []string{"*a ", "_x ", "*a _x ", "[a ", "[a](", "![a](", "数据"} {
		md := strings.Repeat(unit, 200000/len(unit))
		start := time.Now()
		if _, err := a.Render(context.Background(), channel.RichDoc{Label: "L", Markdown: md}); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Errorf("%q x %d took %v", unit, 200000/len(unit), d)
		}
	}
}
