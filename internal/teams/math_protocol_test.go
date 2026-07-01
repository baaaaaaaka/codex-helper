package teams

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type fakeTeamsMathRenderer struct {
	assets []teamsMathAsset
}

func freezeTestOutboxMath(outbox teamstore.OutboxMessage) teamstore.OutboxMessage {
	outbox.TrustedMath = true
	outbox.MathPlanVersion = teamsMathPlanVersion
	outbox.MathSpans = storeTeamsMathPlan(parseTrustedTeamsMath(outbox.Body))
	return outbox
}

func (r fakeTeamsMathRenderer) Render(_ context.Context, spans []teamsMathSpan) []teamsMathAsset {
	if r.assets != nil {
		return append([]teamsMathAsset(nil), r.assets...)
	}
	out := make([]teamsMathAsset, 0, len(spans))
	for _, span := range spans {
		out = append(out, teamsMathAsset{Index: span.Index, PNG: testMathPNG()})
	}
	return out
}

func TestParseTrustedTeamsMathCorpus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		text string
		want []string
	}{
		{"inline", `结果是 <m>x_i+y_i</m>。`, []string{"x_i+y_i"}},
		{"multiple", `<m>x</m> plain <m>y</m>`, []string{"x", "y"}},
		{"multiline", "before <m>\\begin{aligned}\nx&=1\\\\y&=2\n\\end{aligned}</m> after", []string{"\\begin{aligned}\nx&=1\\\\y&=2\n\\end{aligned}"}},
		{"inline code", "`<m>x</m>` and <m>y</m>", []string{"y"}},
		{"indented code", "    <m>x</m>\n<m>y</m>", []string{"y"}},
		{"escaped marker", `\<m>x</m> <m>y</m>`, []string{"y"}},
		{"fenced code", "```tex\n<m>x</m>\n```\n<m>y</m>", []string{"y"}},
		{"tilde fence", "~~~xml\n<m>x</m>\n~~~\n<m>y</m>", []string{"y"}},
		{"link destination", `[x](https://e.test/<m>x</m>) <m>y</m>`, []string{"y"}},
		{"link label", `[<m>x</m>](https://e.test) <m>y</m>`, []string{"y"}},
		{"image label", `![<m>x</m>](https://e.test/x.png) <m>y</m>`, []string{"y"}},
		{"html attribute", `<span data-x="<m>x</m>">text</span> <m>y</m>`, []string{"y"}},
		{"html content", `<span><m>x</m></span> <m>y</m>`, []string{"y"}},
		{"script content", `<script><m>x</m></script> <m>y</m>`, []string{"y"}},
		{"custom html content", `<formula-note><m>x</m></formula-note> <m>y</m>`, []string{"y"}},
		{"unterminated html attribute", `<span data-x="<m>x</m>"`, nil},
		{"multiline raw html", "<section>\n<m>x</m>\n</section>\n<m>y</m>", []string{"y"}},
		{"html comment", `<!-- <m>x</m> --> <m>y</m>`, []string{"y"}},
		{"multiline html comment", "<!--\n<m>x</m>\n--> <m>y</m>", []string{"y"}},
		{"bare URL", `https://e.test/<m>x</m> <m>y</m>`, []string{"y"}},
		{"literal regex and path", `\[A-Z\] \(foo\) C:\tmp\[draft\]\x <m>A-Z</m>`, []string{"A-Z"}},
		{"empty", `<m></m><m> </m>`, nil},
		{"unclosed", `<m>x`, nil},
		{"closing only", `x</m>`, nil},
		{"nested", `<m>outer <m>x</m> tail</m>`, nil},
		{"shell", `<m>${HOME}</m> <m>$(date)</m> <m>$20</m>`, nil},
		{"paths", `<m>C:\Users\x</m> <m>/home/u/x</m>`, nil},
		{"url", `<m>https://example.com/x</m>`, nil},
		{"email", `<m>a@example.com</m>`, nil},
		{"other delimiters", `<m>\(x\)</m><m>\[y\]</m>`, nil},
		{"unsafe commands", `<m>\includegraphics{x}</m><m>\require{html}</m><m>\def\x{x}\x</m><m>\input{file}</m>`, nil},
		{"unbalanced", `<m>{x</m><m>x}</m>`, nil},
		{"escaped currency", `<m>p=\$5</m>`, []string{`p=\$5`}},
		{"less than", `<m>x<y</m>`, []string{"x<y"}},
		{"comparison tag shape", `<m>x<y>0</m>`, []string{"x<y>0"}},
		{"unicode", `<m>α+β=γ</m>`, []string{"α+β=γ"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := parseTrustedTeamsMath(tc.text)
			got := make([]string, 0, len(plan.Spans))
			for _, span := range plan.Spans {
				got = append(got, span.Source)
				if tc.text[span.Start:span.End] != teamsMathOpenTag+span.Source+teamsMathCloseTag {
					t.Fatalf("invalid span %#v", span)
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("sources = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestRenderTrustedTeamsMathShowsExactCodeBeforeImage(t *testing.T) {
	t.Parallel()
	source := `A=\operatorname{softmax}(QK^T/\sqrt{d})V`
	got := renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak("Codex", "before <m>"+source+"</m> after", []teamsMathAsset{{
		Index:       1,
		TemporaryID: "math-001",
		PNG:         []byte("png"),
	}})
	code := "<pre><code>" + source + "</code></pre>"
	image := `<p><img src="../hostedContents/math-001/$value" alt="Rendered TeX formula"></p>`
	if !strings.Contains(got, code) || !strings.Contains(got, image) {
		t.Fatalf("rendered math missing source/image:\n%s", got)
	}
	if strings.Index(got, code) > strings.Index(got, image) {
		t.Fatalf("source code must be before image:\n%s", got)
	}
	if strings.Contains(got, teamsMathOpenTag) || strings.Contains(got, teamsMathCloseTag) {
		t.Fatalf("protocol markers leaked:\n%s", got)
	}
}

func TestRenderTrustedTeamsMathEscapesSourceWithoutChangingIt(t *testing.T) {
	t.Parallel()
	source := `x<y \& y>0`
	got := renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak("Codex", "<m>"+source+"</m>", nil)
	if !strings.Contains(got, `<pre><code>x&lt;y \&amp; y&gt;0</code></pre>`) {
		t.Fatalf("escaped source = %q", got)
	}
}

func TestRenderTrustedTeamsMathDoesNotReplaceLiteralPlaceholderLikeText(t *testing.T) {
	t.Parallel()
	literal := "\ue000cxp-math-000001\ue001"
	got := renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak("Codex", literal+" <m>x_i</m>", nil)
	if !strings.Contains(got, literal) || !strings.Contains(got, `<pre><code>x_i</code></pre>`) {
		t.Fatalf("placeholder-like text was lost or replaced: %q", got)
	}
}

func TestRenderTrustedTeamsMathPreservesMarkdownContainers(t *testing.T) {
	t.Parallel()
	asset := []teamsMathAsset{{Index: 1, TemporaryID: "math-001", PNG: testMathPNG()}}
	cases := []struct {
		name string
		text string
		tag  string
	}{
		{name: "table", text: "| Formula | Note |\n|---|---|\n| <m>x_i</m> | ok |", tag: "<table>"},
		{name: "list", text: "- <m>x_i</m>", tag: "<ul>"},
		{name: "blockquote", text: "> <m>x_i</m>", tag: "<blockquote>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak("Codex", tc.text, asset)
			if !strings.Contains(got, tc.tag) || !strings.Contains(got, `<pre><code>x_i</code></pre>`) || !strings.Contains(got, `<img src="../hostedContents/math-001/$value"`) {
				t.Fatalf("container was not preserved:\n%s", got)
			}
		})
	}
}

func TestTrustedOutboxMathPlanIsFrozenAndFailClosed(t *testing.T) {
	t.Parallel()
	outbox := teamstore.OutboxMessage{
		Kind: "final", Body: `<m>x_i</m>`, TrustedMath: true, MathPlanVersion: teamsMathPlanVersion,
	}
	if got := renderOutboxHTML(outbox); strings.Contains(got, `<pre><code>`) || strings.Contains(got, `<img`) {
		t.Fatalf("empty frozen plan was rescanned: %s", got)
	}
	outbox.MathSpans = []teamstore.OutboxMathSpan{{Start: 0, End: len(outbox.Body), Index: 1, Source: "different"}}
	if got := renderOutboxHTML(outbox); strings.Contains(got, `<pre><code>`) || strings.Contains(got, `<img`) {
		t.Fatalf("corrupt frozen plan was rendered: %s", got)
	}
	outbox = freezeTestOutboxMath(teamstore.OutboxMessage{Kind: "final", Body: strings.Repeat(`<m>x</m>`, maxTeamsMathPerMessage+1)})
	if got := renderOutboxHTML(outbox); strings.Contains(got, `<pre><code>`) || strings.Contains(got, `<img`) {
		t.Fatalf("oversized frozen plan was rendered: %s", got)
	}
}

func TestRenderTrustedTeamsMathFailureKeepsCodeOnly(t *testing.T) {
	t.Parallel()
	got := RenderTeamsHTML(TeamsRenderInput{
		Kind:        TeamsRenderAssistant,
		Text:        `result <m>x_i^2</m>`,
		TrustedMath: true,
	})
	if !strings.Contains(got, `<pre><code>x_i^2</code></pre>`) || strings.Contains(got, `<img`) {
		t.Fatalf("code-only fallback = %q", got)
	}
}

func TestUntrustedTeamsSurfacesNeverInterpretMathMarkers(t *testing.T) {
	t.Parallel()
	for _, kind := range []TeamsRenderKind{TeamsRenderUser, TeamsRenderHelper, TeamsRenderStatus, TeamsRenderAssistant} {
		got := RenderTeamsHTML(TeamsRenderInput{Kind: kind, Text: `<m>x_i</m>`})
		if strings.Contains(got, `<pre><code>x_i</code></pre>`) || strings.Contains(got, `<img`) {
			t.Fatalf("kind %s interpreted untrusted marker: %s", kind, got)
		}
		if !strings.Contains(got, `&lt;m&gt;x_i&lt;/m&gt;`) {
			t.Fatalf("kind %s did not preserve escaped marker: %s", kind, got)
		}
	}
}

func TestOutboxMathTrustIsRestrictedToCodexOutputKinds(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"final", "final-002", "codex-progress", "codex-progress-003", "import-assistant-abc", "sync-assistant-abc", "codex-assistant-abc"} {
		if !outboxKindTrustsMath(kind) {
			t.Fatalf("kind %q should trust math", kind)
		}
	}
	for _, kind := range []string{"user", "helper", "status", "progress", "progress-003", "artifact", "queued-status", "import-user-abc", "user-assistant-note"} {
		if outboxKindTrustsMath(kind) {
			t.Fatalf("kind %q unexpectedly trusts math", kind)
		}
	}
}

func TestQueueOutboxFreezesTrustedMathPlan(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	bridge := &Bridge{store: store}
	queued, err := bridge.queueOutboxChunksWithOptions(context.Background(), "", "turn-math-plan", "chat-1", "final", `result <m>x_i</m>`, outboxQueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 || !queued[0].TrustedMath || queued[0].MathPlanVersion != teamsMathPlanVersion || len(queued[0].MathSpans) != 1 {
		t.Fatalf("queued math plan = %#v", queued)
	}
	span := queued[0].MathSpans[0]
	if span.Source != "x_i" || queued[0].Body[span.Start:span.End] != `<m>x_i</m>` {
		t.Fatalf("frozen span = %#v body=%q", span, queued[0].Body)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.OutboxStateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	persisted := state.OutboxMessages[queued[0].ID]
	if persisted.MathPlanVersion != teamsMathPlanVersion || !equalOutboxMathSpans(persisted.MathSpans, queued[0].MathSpans) {
		t.Fatalf("persisted math plan = %#v", persisted)
	}
}

func TestBridgeRendersTrustedOutboxAsCodeThenHostedImage(t *testing.T) {
	t.Parallel()
	bridge := &Bridge{mathRenderer: fakeTeamsMathRenderer{}}
	outbox := freezeTestOutboxMath(teamstore.OutboxMessage{
		ID:               "outbox:turn-1:final",
		Kind:             "final",
		Body:             `result <m>x_i^2</m>`,
		NotificationKind: "turn_completed",
		PartIndex:        1,
		PartCount:        1,
	})
	assets, hosted := bridge.renderOutboxMathAssets(context.Background(), outbox)
	if len(assets) != 1 || len(hosted) != 1 || assets[0].TemporaryID == "" {
		t.Fatalf("assets=%#v hosted=%#v", assets, hosted)
	}
	rendered := renderOutboxHTMLWithMathAssets(outbox, assets)
	code := `<pre><code>x_i^2</code></pre>`
	image := `<img src="../hostedContents/` + assets[0].TemporaryID + `/$value" alt="Rendered TeX formula">`
	if !strings.Contains(rendered, code) || !strings.Contains(rendered, image) || strings.Index(rendered, code) > strings.Index(rendered, image) {
		t.Fatalf("rendered outbox = %q", rendered)
	}
}

func TestBridgeDeduplicatesIdenticalMathHostedContent(t *testing.T) {
	t.Parallel()
	bridge := &Bridge{mathRenderer: fakeTeamsMathRenderer{}}
	outbox := freezeTestOutboxMath(teamstore.OutboxMessage{
		ID: "outbox:duplicate-math", Kind: "final", Body: `<m>x_i</m> and <m>x_i</m>`,
	})
	assets, hosted := bridge.renderOutboxMathAssets(context.Background(), outbox)
	if len(assets) != 2 || len(hosted) != 1 {
		t.Fatalf("assets=%d hosted=%d, want 2 references to 1 hosted item", len(assets), len(hosted))
	}
	if assets[0].TemporaryID == "" || assets[0].TemporaryID != assets[1].TemporaryID {
		t.Fatalf("duplicate sources did not share temporary id: %#v", assets)
	}
	rendered := renderOutboxHTMLWithMathAssets(outbox, assets)
	imageRef := `../hostedContents/` + assets[0].TemporaryID + `/$value`
	if strings.Count(rendered, imageRef) != 2 {
		t.Fatalf("rendered duplicate image references = %d, want 2: %s", strings.Count(rendered, imageRef), rendered)
	}
}

func TestBridgeMathMediaFallbackNeverInvokesRenderer(t *testing.T) {
	t.Parallel()
	bridge := &Bridge{mathRenderer: fakeTeamsMathRenderer{assets: []teamsMathAsset{{Index: 1, PNG: testMathPNG()}}}}
	outbox := freezeTestOutboxMath(teamstore.OutboxMessage{ID: "o1", Kind: "final", Body: `<m>x</m>`, MathMediaFallback: true})
	assets, hosted := bridge.renderOutboxMathAssets(context.Background(), outbox)
	if len(assets) != 0 || len(hosted) != 0 {
		t.Fatalf("fallback rendered media: assets=%#v hosted=%#v", assets, hosted)
	}
	if rendered := renderOutboxHTMLWithMathAssets(outbox, nil); !strings.Contains(rendered, `<pre><code>x</code></pre>`) || strings.Contains(rendered, `<img`) {
		t.Fatalf("fallback render = %q", rendered)
	}
}

func TestTeamsMathMediaFallbackOnlyForPayloadRejections(t *testing.T) {
	t.Parallel()
	for _, status := range []int{400, 413, 415, 422} {
		if !shouldFallbackTeamsMathMediaError(teamsMathHostedSendError{err: &GraphStatusError{StatusCode: status}}) {
			t.Fatalf("status %d should use code-only fallback", status)
		}
	}
	for _, status := range []int{401, 403, 404, 429, 500, 503} {
		if shouldFallbackTeamsMathMediaError(teamsMathHostedSendError{err: &GraphStatusError{StatusCode: status}}) {
			t.Fatalf("status %d must preserve media for retry", status)
		}
	}
}

func TestPlanTeamsHTMLChunksKeepsMathAtomicAndCapsImages(t *testing.T) {
	t.Parallel()
	var lines []string
	for i := 0; i < maxTeamsMathPerMessage+2; i++ {
		lines = append(lines, fmt.Sprintf("formula %d <m>x_%d</m>", i, i))
	}
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Kind:        TeamsRenderAssistant,
		Text:        strings.Join(lines, "\n"),
		TrustedMath: true,
	}, TeamsRenderOptions{HardLimitBytes: 32 * 1024, TargetLimitBytes: 24 * 1024})
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(chunks))
	}
	total := 0
	for i, chunk := range chunks {
		plan := parseTrustedTeamsMath(chunk.Text)
		if len(plan.Spans) > maxTeamsMathPerMessage {
			t.Fatalf("chunk %d formulas = %d", i, len(plan.Spans))
		}
		if strings.Count(chunk.Text, teamsMathOpenTag) != strings.Count(chunk.Text, teamsMathCloseTag) {
			t.Fatalf("chunk %d split a formula: %q", i, chunk.Text)
		}
		total += len(plan.Spans)
		plannedAssets := plannedTeamsMathAssets(plan)
		actualLength := len(renderTeamsHTMLPartWithMathAssets(TeamsRenderInput{
			Kind: TeamsRenderAssistant, Text: chunk.Text, TrustedMath: true,
		}, chunk.PartIndex, chunk.PartCount, plannedAssets))
		if chunk.ByteLength != actualLength {
			t.Fatalf("chunk %d planned bytes = %d, actual hosted HTML = %d", i, chunk.ByteLength, actualLength)
		}
	}
	if total != len(lines) {
		t.Fatalf("total formulas = %d, want %d", total, len(lines))
	}
}

func FuzzParseTrustedTeamsMath(f *testing.F) {
	for _, seed := range []string{
		`<m>x_i</m>`, "```\n<m>x</m>\n```", `` + "`<m>x</m>`", `<m><m>x</m></m>`,
		`C:\tmp\[draft\]\x`, `\[A-Z\]`, `<m>${HOME}</m>`, `<m>{x</m>`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		plan := parseTrustedTeamsMath(text)
		last := 0
		for _, span := range plan.Spans {
			if span.Start < last || span.Start < 0 || span.End > len(text) || span.Start >= span.End {
				t.Fatalf("invalid ordered span %#v for %q", span, text)
			}
			if text[span.Start:span.End] != teamsMathOpenTag+span.Source+teamsMathCloseTag {
				t.Fatalf("span/source mismatch %#v for %q", span, text)
			}
			last = span.End
		}
	})
}

func TestParseTrustedTeamsMathDeterministicStress(t *testing.T) {
	formulas := []string{"x_i", `\frac{a}{b}`, `\sum_{i=1}^n x_i`, `A=\operatorname{softmax}(QK^T/\sqrt{d})V`, "α+β=γ"}
	plain := []string{`\[A-Z\]`, `\(foo\)`, `C:\tmp\[draft\]\x`, `$PATH`, `${HOME}`, `$(date)`, `$20`, `https://e.test/<m>x</m>`, `foo_bar`, `[docs](https://e.test)`}
	rng := rand.New(rand.NewSource(0xC0DE))
	for round := 0; round < 50_000; round++ {
		var parts []string
		var want []string
		for i := 0; i < 1+rng.Intn(10); i++ {
			switch rng.Intn(6) {
			case 0, 1:
				parts = append(parts, plain[rng.Intn(len(plain))])
			case 2, 3:
				source := formulas[rng.Intn(len(formulas))]
				parts = append(parts, teamsMathOpenTag+source+teamsMathCloseTag)
				want = append(want, source)
			case 4:
				source := formulas[rng.Intn(len(formulas))]
				parts = append(parts, "`"+teamsMathOpenTag+source+teamsMathCloseTag+"`")
			default:
				parts = append(parts, `<span data-x="<m>x</m>">html</span>`)
			}
		}
		text := strings.Join(parts, []string{" ", "\n", "，", " | "}[rng.Intn(4)])
		plan := parseTrustedTeamsMath(text)
		got := make([]string, 0, len(plan.Spans))
		for _, span := range plan.Spans {
			got = append(got, span.Source)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("round %d: got=%#v want=%#v text=%q", round, got, want, text)
		}
	}

	nearMiss := []string{`<mm>x</mm>`, `<m->x</m->`, `<M>x</M>`, `<m attr=x>y</m>`, `<math>x</math>`, "`<m>x</m>`", `<m><m>x</m></m>`, `\[A-Z\]`, `\(foo\)`, `$PATH`, `$(date)`}
	for round := 0; round < 50_000; round++ {
		var parts []string
		for i := 0; i < 1+rng.Intn(10); i++ {
			parts = append(parts, nearMiss[rng.Intn(len(nearMiss))])
		}
		text := strings.Join(parts, []string{" ", "\n", " | "}[rng.Intn(3)])
		if got := parseTrustedTeamsMath(text).Spans; len(got) != 0 {
			t.Fatalf("near miss %d: got=%#v text=%q", round, got, text)
		}
	}

	alphabet := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789<>/m`~[](){}\\$:_- \n")
	for round := 0; round < 50_000; round++ {
		value := make([]rune, rng.Intn(200))
		for i := range value {
			value[i] = alphabet[rng.Intn(len(alphabet))]
		}
		text := string(value)
		last := 0
		for _, span := range parseTrustedTeamsMath(text).Spans {
			if span.Start < last || span.Start < 0 || span.End > len(text) || text[span.Start:span.End] != teamsMathOpenTag+span.Source+teamsMathCloseTag {
				t.Fatalf("malformed %d produced invalid span %#v for %q", round, span, text)
			}
			last = span.End
		}
	}
}
