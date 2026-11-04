package teams

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

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
	m := &NodeMath{Dir: cache, Node: node, Npm: npm}
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
	if _, err := parseMathOutput([]byte("not json"), 1); err == nil {
		t.Fatal("bad json accepted")
	}
	if _, err := parseMathOutput([]byte(`{"error":{"code":"startup"}}`), 1); err == nil {
		t.Fatal("top-level error accepted")
	}
	res, err := parseMathOutput([]byte(`{"results":[{"index":0,"png":"%%%"}]}`), 1)
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
	parts, err := a.Render(context.Background(), channel.RichDoc{Label: "L", Markdown: "$$\n\\frac{a}{b}\n$$", Math: true})
	if err != nil {
		t.Fatal(err)
	}
	if body := decodePart(t, parts[0]).Body.Content; !strings.Contains(body, `\frac{a}{b}`) || strings.Contains(body, "<img") {
		t.Fatalf("renderer failure must show the TeX as code: %s", body)
	}
}
