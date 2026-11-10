package teams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"unicode"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// testFont builds a minimal SFNT with a format 12 Unicode cmap covering runes.
func testFont(runes ...rune) []byte {
	sort.Slice(runes, func(i, j int) bool { return runes[i] < runes[j] })
	var sub bytes.Buffer
	w := func(v any) { _ = binary.Write(&sub, binary.BigEndian, v) }
	w(uint16(12))
	w(uint16(0))
	w(uint32(16 + 12*len(runes)))
	w(uint32(0))
	w(uint32(len(runes)))
	for i, r := range runes {
		w(uint32(r))
		w(uint32(r))
		w(uint32(i + 1))
	}
	var cmap bytes.Buffer
	_ = binary.Write(&cmap, binary.BigEndian, []uint16{0, 1, 3, 10})
	_ = binary.Write(&cmap, binary.BigEndian, uint32(12))
	cmap.Write(sub.Bytes())
	var font bytes.Buffer
	_ = binary.Write(&font, binary.BigEndian, []uint32{0x00010000})
	_ = binary.Write(&font, binary.BigEndian, []uint16{1, 16, 0, 0})
	font.WriteString("cmap")
	_ = binary.Write(&font, binary.BigEndian, []uint32{0, 12 + 16, uint32(cmap.Len())})
	font.Write(cmap.Bytes())
	return font.Bytes()
}

type fontServer struct {
	srv  *httptest.Server
	hits atomic.Int32
}

func newFontServer(t *testing.T, data []byte) *fontServer {
	fs := &fontServer{}
	fs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs.hits.Add(1)
		w.Write(data)
	}))
	t.Cleanup(fs.srv.Close)
	return fs
}

func asset(url string, data []byte, sha string) FontAsset {
	if sha == "" {
		sum := sha256.Sum256(data)
		sha = hex.EncodeToString(sum[:])
	}
	return FontAsset{ID: "test-cjk", Version: "1.0", Family: "Test CJK", Filename: "TestCJK.ttf", URL: url,
		Size: int64(len(data)), SHA256: sha, License: "NotoSerifSC-LICENSE.txt",
		Matches: func(r rune) bool { return unicode.Is(unicode.Han, r) }}
}

// fontNode is a stand-in renderer: every formula yields a PNG with the
// given text; requests that carry fonts are logged to fonts.log.
func fontNode(t *testing.T, dir, text string) (node, root string) {
	png := base64.StdEncoding.EncodeToString(tinyPNG(4, 2))
	out := `{"results":[{"index":0,"png":"` + png + `","width":4,"height":2,"text":"` + text + `"}]}`
	os.WriteFile(filepath.Join(dir, "out.json"), []byte(out), 0o600)
	node = filepath.Join(dir, "node")
	writeScript(t, node, `in=$(cat); case "$in" in *'"path"'*) echo "$in" >> "`+filepath.Join(dir, "fonts.log")+`";; esac; cat "`+filepath.Join(dir, "out.json")+`"`)
	root = filepath.Join(dir, "cache", mathRuntimeName)
	os.MkdirAll(filepath.Join(root, "node_modules", "mathjax"), 0o700)
	os.MkdirAll(filepath.Join(root, "node_modules", "@resvg", "resvg-js"), 0o700)
	os.WriteFile(filepath.Join(root, "node_modules", "mathjax", "package.json"), []byte("{}"), 0o600)
	os.WriteFile(filepath.Join(root, "node_modules", "@resvg", "resvg-js", "package.json"), []byte("{}"), 0o600)
	r, _ := mathjsFiles.ReadFile("mathjs/renderer.mjs")
	os.WriteFile(filepath.Join(root, "renderer.mjs"), r, 0o600)
	return node, root
}

func TestMathFontDownloadedOnDemand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stand-ins")
	}
	ctx := context.Background()
	data := testFont('中', '文', '字')
	fs := newFontServer(t, data)
	dir := t.TempDir()
	node, root := fontNode(t, dir, "中文")
	a := asset(fs.srv.URL+"/font.ttf", data, "")
	m := &NodeMath{Dir: filepath.Join(dir, "cache"), Node: node, Fonts: []FontAsset{a}}
	res, err := m.Render(ctx, []string{`\text{中文}`})
	if err != nil || res[0].Err != nil || len(res[0].PNG) == 0 {
		t.Fatalf("render with downloaded font: %+v %v", res, err)
	}
	if fs.hits.Load() != 1 {
		t.Fatalf("font fetched %d times", fs.hits.Load())
	}
	log, _ := os.ReadFile(filepath.Join(dir, "fonts.log"))
	if !strings.Contains(string(log), `"family":"Test CJK"`) {
		t.Fatalf("renderer was not re-run with the font: %s", log)
	}
	fd := fontDir(root, a)
	for _, f := range []string{a.Filename, fontDatabaseName, "LICENSE.txt"} {
		if _, err := os.Stat(filepath.Join(fd, f)); err != nil {
			t.Fatalf("installed font lacks %s", f)
		}
	}
	// A new process reuses the verified font without downloading.
	m2 := &NodeMath{Dir: filepath.Join(dir, "cache"), Node: node, Fonts: []FontAsset{a}}
	if res, _ := m2.Render(ctx, []string{`\text{中文}`}); res[0].Err != nil || fs.hits.Load() != 1 {
		t.Fatalf("reuse: %+v, hits %d", res[0], fs.hits.Load())
	}
	// A tampered font file is not trusted.
	os.WriteFile(filepath.Join(fd, a.Filename), bytes.Repeat([]byte{0}, len(data)), 0o600)
	m3 := &NodeMath{Dir: filepath.Join(dir, "cache"), Node: node, Fonts: []FontAsset{a}}
	if fonts := m3.installedFonts(root); len(fonts) != 0 {
		t.Fatalf("tampered font accepted: %+v", fonts)
	}
}

func TestMathFontNotFetchedWithoutNeed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stand-ins")
	}
	data := testFont('中')
	fs := newFontServer(t, data)
	dir := t.TempDir()
	node, _ := fontNode(t, dir, "")
	m := &NodeMath{Dir: filepath.Join(dir, "cache"), Node: node, Fonts: []FontAsset{asset(fs.srv.URL, data, "")}}
	if res, err := m.Render(context.Background(), []string{`E=mc^2`}); err != nil || res[0].Err != nil {
		t.Fatalf("%+v %v", res, err)
	}
	// Text no asset matches (Hangul) is refused without a download.
	node2, _ := fontNode(t, t.TempDir(), "한")
	m.Node = node2
	if res, _ := m.Render(context.Background(), []string{`\text{한}`}); res[0].Err == nil || res[0].PNG != nil {
		t.Fatalf("uncovered text accepted: %+v", res[0])
	}
	if fs.hits.Load() != 0 {
		t.Fatalf("font fetched %d times", fs.hits.Load())
	}
}

func TestMathFontFailuresFallBack(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stand-ins")
	}
	ctx := context.Background()
	data := testFont('中')
	fs := newFontServer(t, data)
	cases := map[string]FontAsset{
		"checksum": asset(fs.srv.URL, data, strings.Repeat("ab", 32)),
		"partial":  asset(fs.srv.URL, data, ""), // covers 中 but not 文
		"offline":  asset(closedURL(t), data, ""),
	}
	for name, a := range cases {
		dir := t.TempDir()
		node, root := fontNode(t, dir, "中文")
		m := &NodeMath{Dir: filepath.Join(dir, "cache"), Node: node, Fonts: []FontAsset{a}}
		res, err := m.Render(ctx, []string{`\text{中文}`})
		if err != nil || res[0].Err == nil || res[0].PNG != nil {
			t.Fatalf("%s: %+v %v", name, res, err)
		}
		_, statErr := os.Stat(filepath.Join(fontDir(root, a), a.Filename))
		if (name == "partial") != (statErr == nil) {
			t.Fatalf("%s: installed=%v", name, statErr == nil)
		}
		if leftovers, _ := filepath.Glob(filepath.Join(root, "fonts", ".install-*")); len(leftovers) != 0 {
			t.Fatalf("%s: staging left behind: %v", name, leftovers)
		}
	}

	// End to end: the TeX stays visible as code when the font cannot be had.
	dir := t.TempDir()
	node, _ := fontNode(t, dir, "中文")
	m := &NodeMath{Dir: filepath.Join(dir, "cache"), Node: node, Fonts: []FontAsset{cases["checksum"]}}
	_, ad := newFake(t, fakegraph.Options{}, func(c *Config) { c.Math = m })
	parts, err := ad.Render(ctx, channel.RichDoc{Label: "L", Markdown: "<m>\n\\text{中文}\n</m>", Math: true})
	if err != nil {
		t.Fatal(err)
	}
	if body := decodePart(t, parts[0]).Body.Content; !strings.Contains(body, `\text{中文}`) || strings.Contains(body, "<img") {
		t.Fatalf("font failure must show the TeX as code: %s", body)
	}
	good := testFont('中', '文')
	gs := newFontServer(t, good)
	dir = t.TempDir()
	node, _ = fontNode(t, dir, "中文")
	m = &NodeMath{Dir: filepath.Join(dir, "cache"), Node: node, Fonts: []FontAsset{asset(gs.srv.URL, good, "")}}
	_, ad = newFake(t, fakegraph.Options{}, func(c *Config) { c.Math = m })
	parts, err = ad.Render(ctx, channel.RichDoc{Label: "L", Markdown: "<m>\n\\text{中文}\n</m>", Math: true})
	if err != nil || !strings.Contains(decodePart(t, parts[0]).Body.Content, "<img") {
		t.Fatalf("covered CJK formula must render as an image: %v", err)
	}
}

func TestCmapCoverage(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f.ttf")
	os.WriteFile(p, testFont('a', '中', 0x20000), 0o600)
	cm, err := loadFontCmaps(p)
	if err != nil {
		t.Fatal(err)
	}
	got := cmapCoverage(cm, []rune{'a', 'b', '中', 0x20000, 0x20001})
	if !got['a'] || got['b'] || !got['中'] || !got[0x20000] || got[0x20001] {
		t.Fatalf("coverage %v", got)
	}
	os.WriteFile(p, []byte("not a font at all"), 0o600)
	if _, err := loadFontCmaps(p); err == nil {
		t.Fatal("garbage parsed as a font")
	}
}
