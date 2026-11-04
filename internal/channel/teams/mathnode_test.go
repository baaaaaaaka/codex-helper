package teams

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func writeScript(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
}

// TestNodeMathInstallAndRender drives NodeMath with stand-in npm and node
// binaries: the real renderer is covered by the documented manual check.
func TestNodeMathInstallAndRender(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stand-ins")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	os.MkdirAll(bin, 0o700)
	npm := filepath.Join(bin, "npm")
	writeScript(t, npm, `echo ran >> "`+filepath.Join(dir, "npm.log")+`"
test "$1 $2" = "ci --ignore-scripts" || exit 3
test -f package-lock.json || exit 4
mkdir -p node_modules/mathjax node_modules/@resvg/resvg-js
echo {} > node_modules/mathjax/package.json
echo {} > node_modules/@resvg/resvg-js/package.json
`)
	png := base64.StdEncoding.EncodeToString(tinyPNG(4, 2))
	out := `{"results":[{"index":0,"png":"` + png + `","width":4,"height":2},` +
		`{"index":1,"error":{"code":"tex","message":"Undefined control sequence \\bad"}},` +
		`{"index":2,"png":"` + png + `","text":"中文"},{"index":9,"png":"x"}]}`
	os.WriteFile(filepath.Join(dir, "out.json"), []byte(out), 0o600)
	node := filepath.Join(bin, "node")
	writeScript(t, node, `cat > "`+filepath.Join(dir, "in.json")+`"; cat "`+filepath.Join(dir, "out.json")+`"`)

	cache := filepath.Join(dir, "cache")
	m := &NodeMath{Dir: cache, Node: node, Npm: npm, Fonts: []FontAsset{}}
	// The first render starts the install in the background and does not
	// wait for it; the formulas show as code meanwhile.
	if _, err := m.Render(context.Background(), []string{"x"}); !errors.Is(err, errMathInstalling) {
		t.Fatalf("first render: %v, want errMathInstalling", err)
	}
	<-m.installDone()
	res, err := m.Render(context.Background(), []string{`E=mc^2`, `\bad`, `\text{中文}`, `x`})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 4 || res[0].Err != nil || len(res[0].PNG) == 0 || res[0].Width != 4 {
		t.Fatalf("ok formula: %+v", res[0])
	}
	if res[1].Err == nil || !strings.Contains(res[1].Err.Error(), "Undefined control") {
		t.Fatalf("tex error: %+v", res[1])
	}
	if res[2].Err == nil || res[2].PNG != nil {
		t.Fatalf("missing glyphs must fail closed: %+v", res[2])
	}
	if res[3].Err == nil {
		t.Fatal("formula without a result must fail")
	}
	var req struct {
		Items []struct {
			Index  int
			Source string
		}
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "in.json"))
	if json.Unmarshal(raw, &req) != nil || len(req.Items) != 4 || req.Items[2].Source != `\text{中文}` {
		t.Fatalf("request %s", raw)
	}
	if _, err := os.Stat(filepath.Join(cache, mathRuntimeName, "renderer.mjs")); err != nil {
		t.Fatal("runtime not installed")
	}

	// A second instance reuses the installed runtime without npm.
	os.Remove(npm)
	m2 := &NodeMath{Dir: cache, Node: node, Npm: npm}
	if _, err := m2.Render(context.Background(), []string{"x"}); err != nil {
		t.Fatalf("reuse: %v", err)
	}
	if log, _ := os.ReadFile(filepath.Join(dir, "npm.log")); strings.Count(string(log), "ran") != 1 {
		t.Fatalf("npm ran %q", log)
	}

	// A legacy runtime with an identical renderer is reused as is.
	cache2 := filepath.Join(dir, "cache2")
	legacy := filepath.Join(cache2, mathRuntimeLegacy)
	os.MkdirAll(filepath.Join(legacy, "node_modules", "mathjax"), 0o700)
	os.MkdirAll(filepath.Join(legacy, "node_modules", "@resvg", "resvg-js"), 0o700)
	os.WriteFile(filepath.Join(legacy, "node_modules", "mathjax", "package.json"), []byte("{}"), 0o600)
	os.WriteFile(filepath.Join(legacy, "node_modules", "@resvg", "resvg-js", "package.json"), []byte("{}"), 0o600)
	r, _ := mathjsFiles.ReadFile("mathjs/renderer.mjs")
	os.WriteFile(filepath.Join(legacy, "renderer.mjs"), r, 0o600)
	m3 := &NodeMath{Dir: cache2, Node: node, Npm: npm}
	if root, err := m3.runtime(context.Background()); err != nil || root != legacy {
		t.Fatalf("legacy reuse: %q %v", root, err)
	}
}

func TestParseMathOutputErrors(t *testing.T) {
	if _, _, err := parseMathOutput([]byte("not json"), 1); err == nil {
		t.Fatal("bad json accepted")
	}
	if _, _, err := parseMathOutput([]byte(`{"error":{"code":"startup"}}`), 1); err == nil {
		t.Fatal("top-level error accepted")
	}
	res, _, err := parseMathOutput([]byte(`{"results":[{"index":0,"png":"%%%"}]}`), 1)
	if err != nil || res[0].Err == nil {
		t.Fatalf("bad base64: %+v %v", res, err)
	}
}

func TestNodeMathFailingRendererFallsBackInRender(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stand-ins")
	}
	dir := t.TempDir()
	node := filepath.Join(dir, "node")
	writeScript(t, node, "echo boom >&2; exit 1\n")
	root := filepath.Join(dir, mathRuntimeName)
	os.MkdirAll(filepath.Join(root, "node_modules", "mathjax"), 0o700)
	os.MkdirAll(filepath.Join(root, "node_modules", "@resvg", "resvg-js"), 0o700)
	os.WriteFile(filepath.Join(root, "node_modules", "mathjax", "package.json"), []byte("{}"), 0o600)
	os.WriteFile(filepath.Join(root, "node_modules", "@resvg", "resvg-js", "package.json"), []byte("{}"), 0o600)
	r, _ := mathjsFiles.ReadFile("mathjs/renderer.mjs")
	os.WriteFile(filepath.Join(root, "renderer.mjs"), r, 0o600)
	m := &NodeMath{Dir: dir, Node: node}
	if _, err := m.Render(context.Background(), []string{"x"}); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
	_, a := newFake(t, fakegraph.Options{}, func(c *Config) { c.Math = m })
	parts, err := a.Render(context.Background(), channel.RichDoc{Label: "L", Markdown: "<m>\n\\frac{a}{b}\n</m>", Math: true})
	if err != nil {
		t.Fatal(err)
	}
	if body := decodePart(t, parts[0]).Body.Content; !strings.Contains(body, `\frac{a}{b}`) || strings.Contains(body, "<img") {
		t.Fatalf("renderer failure must show the TeX as code: %s", body)
	}
}

// The install never holds a render: one that does not finish keeps every
// render returning at once (code fallback). A failed install is not retried
// on every render: the next try waits 10 min, doubling to 24 h.
func TestNodeMathInstallNeverBlocksAndBacksOff(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stand-ins")
	}
	dir := t.TempDir()
	release := filepath.Join(dir, "release")
	slowNpm := filepath.Join(dir, "slow-npm")
	writeScript(t, slowNpm, `while [ ! -f "`+release+`" ]; do sleep 0.05; done; exit 1`)
	m := &NodeMath{Dir: filepath.Join(dir, "slow"), Npm: slowNpm, Node: "/nonexistent/node"}
	_, a := newFake(t, fakegraph.Options{}, func(c *Config) { c.Math = m })
	for i := 0; i < 3; i++ {
		start := time.Now()
		parts, err := a.Render(context.Background(), channel.RichDoc{Label: "L", Markdown: "<m>\\frac{a}{b}</m>", Math: true})
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("render %d waited %s for the install", i, d)
		}
		if b := decodePart(t, parts[0]); len(b.HostedContents) != 0 || !strings.Contains(b.Body.Content, "<pre><code>\\frac{a}{b}</code></pre>") {
			t.Fatalf("formula during install: %s", b.Body.Content)
		}
	}
	os.WriteFile(release, nil, 0o600)
	<-m.installDone()

	now := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	prev := mathNow
	mathNow = func() time.Time { return now }
	t.Cleanup(func() { mathNow = prev })
	log := filepath.Join(dir, "npm.log")
	failNpm := filepath.Join(dir, "fail-npm")
	writeScript(t, failNpm, `echo ran >> "`+log+`"; echo offline >&2; exit 1`)
	f := &NodeMath{Dir: filepath.Join(dir, "fail"), Npm: failNpm, Node: "/nonexistent/node"}
	runs := func() int { b, _ := os.ReadFile(log); return strings.Count(string(b), "ran") }
	try := func() error {
		_, err := f.Render(context.Background(), []string{"x"})
		<-f.installDone()
		return err
	}
	if err := try(); !errors.Is(err, errMathInstalling) || runs() != 1 {
		t.Fatalf("first render: %v, npm runs %d", err, runs())
	}
	for _, wait := range []time.Duration{10 * time.Minute, 20 * time.Minute, 40 * time.Minute} {
		now = now.Add(wait - time.Second)
		n := runs()
		if err := try(); err == nil || !strings.Contains(err.Error(), "offline") || runs() != n {
			t.Fatalf("within %s of a failed install: err=%v, npm ran again=%v", wait, err, runs() != n)
		}
		now = now.Add(time.Second)
		if err := try(); !errors.Is(err, errMathInstalling) || runs() != n+1 {
			t.Fatalf("after %s: err=%v, npm runs %d (want %d)", wait, err, runs(), n+1)
		}
	}
	want := map[int]time.Duration{1: 10 * time.Minute, 2: 20 * time.Minute, 5: 160 * time.Minute, 8: 1280 * time.Minute, 9: 24 * time.Hour, 30: 24 * time.Hour}
	for n, d := range want {
		if got := mathInstallBackoff(n); got != d {
			t.Errorf("mathInstallBackoff(%d) = %s, want %s", n, got, d)
		}
	}
}

// Graph refusing the hosted formula images (400/413/415/422: nothing was
// stored) does not drop the answer: it is posted again with the TeX code
// only, as v0.1.20 did.
func TestSendRetriesWithoutFormulaImagesWhenGraphRefusesThem(t *testing.T) {
	png := tinyPNG(8, 4)
	m := mathFunc(func(s []string) []MathResult { return []MathResult{{PNG: png, Width: 8, Height: 4}} })
	for _, status := range []int{http.StatusRequestEntityTooLarge, http.StatusBadRequest} {
		fg, a := newFake(t, fakegraph.Options{}, func(c *Config) { c.Math = m })
		chat := fg.CreateMeetingChatAsUser("math").ID
		parts, err := a.Render(context.Background(), channel.RichDoc{Label: "L", Markdown: "Result:\n\n<m>\\frac{a}{b}</m>", Math: true})
		if err != nil || len(decodePart(t, parts[0]).HostedContents) != 1 {
			t.Fatalf("render: %v", err)
		}
		fg.AddFault(fakegraph.Fault{Name: "no-images", Kind: fakegraph.FaultStatus, Status: status, Method: http.MethodPost,
			Match: func(r fakegraph.Request) bool { return strings.Contains(r.Body, "hostedContents") }})
		if _, err := a.Send(context.Background(), chat, parts[0], "tok1"); err != nil {
			t.Fatalf("%d: send: %v", status, err)
		}
		msgs := fg.Messages(chat)
		var got []fakegraph.Message
		for _, msg := range msgs {
			if msg.Origin == fakegraph.OriginSUT {
				got = append(got, msg)
			}
		}
		if len(got) != 1 || got[0].Hosted != 0 || strings.Contains(got[0].Content, "<img") ||
			!strings.Contains(got[0].Content, "<pre><code>\\frac{a}{b}</code></pre>") || !strings.Contains(got[0].Content, "Result:") {
			t.Fatalf("%d: posted %s", status, fmt.Sprint(got))
		}
	}
	// Other refusals are not retried.
	fg, a := newFake(t, fakegraph.Options{}, func(c *Config) { c.Math = m })
	chat := fg.CreateMeetingChatAsUser("math").ID
	parts, _ := a.Render(context.Background(), channel.RichDoc{Label: "L", Markdown: "<m>x</m>", Math: true})
	fg.AddFault(fakegraph.Fault{Name: "forbidden", Kind: fakegraph.FaultStatus, Status: http.StatusForbidden, Method: http.MethodPost})
	if _, err := a.Send(context.Background(), chat, parts[0], "tok2"); err == nil {
		t.Fatal("403 accepted")
	}
	if n := len(fg.Requests(fakegraph.RequestFilter{Method: http.MethodPost})); n != 1 {
		t.Fatalf("403 retried: %d POSTs", n)
	}
}
