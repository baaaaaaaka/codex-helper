package turns

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/png"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// D13: display formulas in a Codex answer are posted as the TeX code plus a
// rendered image (the v0.1.20 look), by the helper's managed renderer.
//
// Stand-in `npm` and `node` on PATH play the renderer runtime (the real one
// needs the network to install): npm "installs" once released, node returns
// a small PNG. While the install runs the answer is not held: the formula
// is shown as code. After it, the formula is code + image. Graph refusing
// the image (413) does not drop the answer: it is posted with the code.
func TestFormulaRenderedAsCodeAndImage(t *testing.T) {
	harness.Parallel(t)
	bin := t.TempDir()
	release, npmLog := filepath.Join(bin, "release"), filepath.Join(bin, "npm.log")
	writeStandIn(t, filepath.Join(bin, "npm"), `echo started >> "`+npmLog+`"
while [ ! -f "`+release+`" ]; do sleep 0.1; done
mkdir -p node_modules/mathjax node_modules/@resvg/resvg-js
echo {} > node_modules/mathjax/package.json
echo {} > node_modules/@resvg/resvg-js/package.json
echo done >> "`+npmLog+`"
`)
	var img bytes.Buffer
	_ = png.Encode(&img, image.NewGray(image.Rect(0, 0, 40, 20)))
	writeStandIn(t, filepath.Join(bin, "node"), `cat > /dev/null
printf '%s' '{"results":[{"index":0,"png":"`+base64.StdEncoding.EncodeToString(img.Bytes())+`","width":40,"height":20}]}'
`)
	s := harness.NewScenario(t, "FORMULA", harness.Options{
		Guarantees: []string{"D13"},
		Title:      "a display formula is posted as TeX code plus a rendered image; during the renderer install and when Graph refuses the image it is code only, never held or dropped",
		Budget:     150 * time.Second,
		UserEnv:    map[string]string{"PATH": bin + string(os.PathListSeparator) + os.Getenv("PATH")},
	})
	defer s.Finish()
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		return harness.CodexBehavior{Answer: "Result for " + strings.Join(c.Tokens, " ") + ":\n\n<m>\\frac{a}{b}</m>\n\nDone."}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "formula")
	const code = `<pre><code>\frac{a}{b}</code></pre>`

	ask := func(token string) (fakegraph.Message, bool) {
		steps.MustPost(s, work.ID, "Show a fraction "+token)
		var final fakegraph.Message
		ok := s.WaitFor("formula answer for "+token, steps.AnswerTimeout, func() (bool, string) {
			for _, m := range s.SUTMessagesContaining(work.ID, "Result for "+token) {
				final = m
				return true, ""
			}
			return false, "no answer"
		})
		return final, ok
	}

	during, ok := ask(s.Token("during"))
	started, _ := os.ReadFile(npmLog)
	s.Check("while the renderer installs, the answer is posted at once with the formula as code",
		ok && during.Hosted == 0 && strings.Contains(during.Content, code) && strings.Contains(string(started), "started"),
		"answered, code block, no image, install started", fmt.Sprintf("answered=%v hosted=%d code=%v npm=%q", ok, during.Hosted, strings.Contains(during.Content, code), started))

	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s.Require("renderer installed", 30*time.Second, func() (bool, string) {
		b, _ := os.ReadFile(npmLog)
		return strings.Contains(string(b), "done"), string(b)
	})
	time.Sleep(time.Second) // the helper moves the staged install into place

	after, ok := ask(s.Token("after"))
	s.Check("after the install the formula is TeX code plus a rendered image (v0.1.20 look)",
		ok && after.Hosted == 1 && strings.Contains(after.Content, code) && strings.Contains(after.Content, `alt="Rendered TeX formula"`) &&
			strings.Index(after.Content, code) < strings.Index(after.Content, "<img"),
		"code block, then 1 hosted image", fmt.Sprintf("answered=%v hosted=%d content=%.400s", ok, after.Hosted, after.Content))

	s.Graph.AddFault(fakegraph.Fault{Name: "image-refused", Kind: fakegraph.FaultStatus, Status: http.StatusRequestEntityTooLarge, Method: http.MethodPost,
		Match: func(r fakegraph.Request) bool { return strings.Contains(r.Body, "hostedContents/") }})
	token := s.Token("refused")
	refused, ok := ask(token)
	s.Settle(2*time.Second, 8*time.Second)
	n := len(s.SUTMessagesContaining(work.ID, "Result for "+token))
	s.Check("Graph refusing the image does not drop the answer: it is posted once with the code",
		ok && n == 1 && refused.Hosted == 0 && strings.Contains(refused.Content, code),
		"answered once, code block, no image", fmt.Sprintf("answered=%v posts=%d hosted=%d", ok, n, refused.Hosted), s.Transcript(work.ID, 12)...)
}

func writeStandIn(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
}
