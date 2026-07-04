package teams

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"image"
	"image/png"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type offlineTeamsMathHTTPClient struct {
	calls int
}

func (c *offlineTeamsMathHTTPClient) Do(*http.Request) (*http.Response, error) {
	c.calls++
	return nil, errors.New("network must not be used")
}

func TestManagedTeamsMathRendererLive(t *testing.T) {
	if os.Getenv("CODEX_HELPER_TEST_TEAMS_MATH_RUNTIME") != "1" {
		t.Skip("set CODEX_HELPER_TEST_TEAMS_MATH_RUNTIME=1 to exercise npm/MathJax/resvg")
	}
	renderer := &managedTeamsMathRenderer{cacheRoot: filepath.Join(t.TempDir(), "runtime")}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	spans := []teamsMathSpan{
		{Index: 1, Source: `N_1=\operatorname{RMSNorm}(X)`},
		{Index: 2, Source: `A_h=\frac{\widetilde{\exp}(\alpha S_h)}{\sum_j\widetilde{\exp}(\alpha S_{h,ij})}`},
		{Index: 3, Source: `N_1=\operatorname{RMSNorm}(X)`},
		{Index: 4, Source: `\text{中文}+x`},
		{Index: 5, Source: `\phantom{\text{中文}}+x`},
	}
	assets := renderer.Render(ctx, spans)
	if len(assets) != len(spans) {
		t.Fatalf("assets = %d, want %d", len(assets), len(spans))
	}
	for i, asset := range assets {
		if asset.Error != "" || !validTeamsMathPNG(asset.PNG) {
			t.Fatalf("asset %d failed: error=%q bytes=%d", i, asset.Error, len(asset.PNG))
		}
		decoded, err := png.Decode(bytes.NewReader(asset.PNG))
		if err != nil || decoded.Bounds().Dx() <= 0 || decoded.Bounds().Dy() <= 0 {
			t.Fatalf("asset %d is not a decodable non-empty PNG: %v", i, err)
		}
		nonWhiteBounds := image.Rectangle{}
		nonWhite := 0
		for y := decoded.Bounds().Min.Y; y < decoded.Bounds().Max.Y; y++ {
			for x := decoded.Bounds().Min.X; x < decoded.Bounds().Max.X; x++ {
				r, g, b, a := decoded.At(x, y).RGBA()
				if a != 0 && (r < 0xffff || g < 0xffff || b < 0xffff) {
					pixel := image.Rect(x, y, x+1, y+1)
					if nonWhite == 0 {
						nonWhiteBounds = pixel
					} else {
						nonWhiteBounds = nonWhiteBounds.Union(pixel)
					}
					nonWhite++
				}
			}
		}
		if nonWhite < 10 {
			t.Fatalf("asset %d appears blank: non-white pixels=%d", i, nonWhite)
		}
		topMargin := nonWhiteBounds.Min.Y - decoded.Bounds().Min.Y
		bottomMargin := decoded.Bounds().Max.Y - nonWhiteBounds.Max.Y
		if decoded.Bounds().Dy() < 128 {
			t.Fatalf("asset %d is too low-resolution: bounds=%v", i, decoded.Bounds())
		}
		if topMargin < 24 || bottomMargin < 24 {
			t.Fatalf("asset %d has insufficient vertical padding: top=%d bottom=%d bounds=%v content=%v", i, topMargin, bottomMargin, decoded.Bounds(), nonWhiteBounds)
		}
	}
	if bytes.Equal(assets[0].PNG, assets[1].PNG) {
		t.Fatal("different TeX sources rendered to identical PNGs")
	}
	if !bytes.Equal(assets[0].PNG, assets[2].PNG) {
		t.Fatal("duplicate TeX sources did not share the same rendered PNG")
	}
	if bytes.Equal(assets[3].PNG, assets[4].PNG) {
		t.Fatal("Chinese glyphs rendered identically to a phantom; the managed font was not applied")
	}
	fontAsset := defaultTeamsMathFontAssets[0]
	fontPath := filepath.Join(managedTeamsMathFontDir(renderer.cacheRoot, fontAsset), fontAsset.Filename)
	fontInfo, err := os.Stat(fontPath)
	if err != nil {
		t.Fatalf("managed Chinese font was not installed: %v", err)
	}
	if fontInfo.Size() != fontAsset.Size || fontInfo.Size() > 12*1024*1024 {
		t.Fatalf("managed Chinese font size=%d, want exact %d and no more than 12 MiB", fontInfo.Size(), fontAsset.Size)
	}
	licenseInfo, err := os.Stat(filepath.Join(managedTeamsMathFontDir(renderer.cacheRoot, fontAsset), "LICENSE.txt"))
	if err != nil || !licenseInfo.Mode().IsRegular() || licenseInfo.Size() == 0 {
		t.Fatalf("managed Chinese font license is missing: info=%v err=%v", licenseInfo, err)
	}
	entries, err := os.ReadDir(filepath.Join(renderer.cacheRoot, "png"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("cached PNG files = %d, want 4 unique sources", len(entries))
	}
	second := renderer.Render(ctx, spans)
	for i, asset := range second {
		if asset.Error != "" || !validTeamsMathPNG(asset.PNG) {
			t.Fatalf("cached asset %d failed: error=%q bytes=%d", i, asset.Error, len(asset.PNG))
		}
	}
	offlineClient := &offlineTeamsMathHTTPClient{}
	restarted := &managedTeamsMathRenderer{cacheRoot: renderer.cacheRoot, fontHTTPClient: offlineClient}
	offline := restarted.Render(ctx, []teamsMathSpan{{Index: 7, Source: `\text{中文离线}+y`}})
	if len(offline) != 1 || offline[0].Error != "" || !validTeamsMathPNG(offline[0].PNG) || offlineClient.calls != 0 {
		t.Fatalf("restarted offline font reuse = %#v network_calls=%d", offline, offlineClient.calls)
	}
	invalid := renderer.Render(ctx, []teamsMathSpan{{Index: 3, Source: `\definitelyUnknownCommand{}`}})
	if len(invalid) != 1 || invalid[0].Error == "" || len(invalid[0].PNG) != 0 {
		t.Fatalf("invalid TeX result = %#v", invalid)
	}
	unsupported := renderer.Render(ctx, []teamsMathSpan{{Index: 6, Source: `\text{العربية}+x`}})
	if len(unsupported) != 1 || unsupported[0].Error == "" || len(unsupported[0].PNG) != 0 {
		t.Fatalf("unsupported external text must fail without a tofu PNG: %#v", unsupported)
	}
}

func TestManagedTeamsMathRendererMissingNodeFallsBack(t *testing.T) {
	t.Parallel()
	renderer := &managedTeamsMathRenderer{
		cacheRoot: filepath.Join(t.TempDir(), "runtime"),
		lookPath:  func(string) (string, error) { return "", os.ErrNotExist },
	}
	assets := renderer.Render(context.Background(), []teamsMathSpan{{Index: 1, Source: "x_i"}})
	if len(assets) != 1 || assets[0].Error == "" || len(assets[0].PNG) != 0 {
		t.Fatalf("missing-node fallback = %#v", assets)
	}
}

func TestManagedTeamsMathRendererRejectsTooManyFormulasBeforeRuntimeLookup(t *testing.T) {
	t.Parallel()
	lookups := 0
	renderer := &managedTeamsMathRenderer{
		cacheRoot: filepath.Join(t.TempDir(), "runtime"),
		lookPath: func(string) (string, error) {
			lookups++
			return "", os.ErrNotExist
		},
	}
	spans := make([]teamsMathSpan, maxTeamsMathImagesPerMessage+1)
	for i := range spans {
		spans[i] = teamsMathSpan{Index: i + 1, Source: "x"}
	}
	assets := renderer.Render(context.Background(), spans)
	if len(assets) != len(spans) || lookups != 0 {
		t.Fatalf("assets=%d runtime lookups=%d", len(assets), lookups)
	}
	for _, asset := range assets {
		if asset.Error == "" || len(asset.PNG) != 0 {
			t.Fatalf("unbounded formula batch was not rejected: %#v", assets)
		}
	}
}

func TestTeamsMathCacheKeyChangesWithSource(t *testing.T) {
	t.Parallel()
	if teamsMathCacheKey("x") == teamsMathCacheKey("y") {
		t.Fatal("cache key ignored source")
	}
}

func TestManagedTeamsMathRendererCacheHitDoesNotTouchMTime(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "runtime")
	renderer := &managedTeamsMathRenderer{cacheRoot: root}
	data, ok := validateTeamsMathPNG(testMathPNG())
	if !ok {
		t.Fatal("test PNG did not validate")
	}
	if err := renderer.storeCachedPNG("x_i", data); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "png", teamsMathCacheKey("x_i")+".png")
	old := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if cached, ok := renderer.cachedPNG("x_i"); !ok || !bytes.Equal(cached, data) {
		t.Fatal("cache hit failed")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(old) {
		t.Fatalf("cache hit changed mtime: got %s want %s", info.ModTime(), old)
	}
}

func TestManagedTeamsMathRendererReplacesCorruptCacheEntry(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "runtime")
	dir := filepath.Join(root, "png")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, teamsMathCacheKey("x_i")+".png")
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, ok := validateTeamsMathPNG(testMathPNG())
	if !ok {
		t.Fatal("test PNG did not validate")
	}
	renderer := &managedTeamsMathRenderer{cacheRoot: root}
	if err := renderer.storeCachedPNG("x_i", data); err != nil {
		t.Fatal(err)
	}
	got, ok := renderer.cachedPNG("x_i")
	if !ok || !bytes.Equal(got, data) {
		t.Fatal("corrupt cache entry was not replaced with the validated PNG")
	}
}

func TestManagedTeamsMathRendererCacheAccountingScansOnceAndPrunesWithHysteresis(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "runtime")
	data, ok := validateTeamsMathPNG(testMathPNG())
	if !ok {
		t.Fatal("test PNG did not validate")
	}
	size := int64(len(data))
	renderer := &managedTeamsMathRenderer{
		cacheRoot:           root,
		maxPNGCacheBytes:    size*2 + 1,
		targetPNGCacheBytes: size + 1,
	}
	for _, source := range []string{"x_1", "x_2"} {
		if err := renderer.storeCachedPNG(source, data); err != nil {
			t.Fatal(err)
		}
	}
	firstPath := filepath.Join(root, "png", teamsMathCacheKey("x_1")+".png")
	secondPath := filepath.Join(root, "png", teamsMathCacheKey("x_2")+".png")
	first := renderer.pngCacheEntries[firstPath]
	first.modTime = time.Unix(1_700_000_000, 0)
	renderer.pngCacheEntries[firstPath] = first
	second := renderer.pngCacheEntries[secondPath]
	second.modTime = time.Unix(1_700_000_001, 0)
	renderer.pngCacheEntries[secondPath] = second
	if err := renderer.storeCachedPNG("x_3", data); err != nil {
		t.Fatal(err)
	}
	if renderer.pngCacheScans != 1 {
		t.Fatalf("cache directory scans = %d, want 1", renderer.pngCacheScans)
	}
	if renderer.pngCacheBytes > renderer.targetPNGCacheBytes {
		t.Fatalf("cache bytes = %d, target = %d", renderer.pngCacheBytes, renderer.targetPNGCacheBytes)
	}
	entries, err := os.ReadDir(filepath.Join(root, "png"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || len(renderer.pngCacheEntries) != 1 {
		t.Fatalf("disk entries=%d indexed entries=%d, want 1", len(entries), len(renderer.pngCacheEntries))
	}
	thirdPath := filepath.Join(root, "png", teamsMathCacheKey("x_3")+".png")
	if entries[0].Name() != filepath.Base(thirdPath) {
		t.Fatalf("FIFO pruning kept %q, want newest %q", entries[0].Name(), filepath.Base(thirdPath))
	}
}

func TestCleanupObsoleteTeamsMathRuntimesKeepsCurrentPreviousAndUnrelated(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	current := filepath.Join(parent, "current")
	previous := filepath.Join(parent, "previous")
	old := filepath.Join(parent, "old")
	unrelated := filepath.Join(parent, "unrelated")
	foreign := filepath.Join(parent, "foreign-runtime")
	for _, dir := range []string{current, previous, old, unrelated, foreign} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeMarker := func(dir string, version string, installed time.Time) {
		t.Helper()
		data, err := json.Marshal(teamsMathRuntimeMarker{Kind: teamsMathRuntimeKind, Version: version, Installed: installed.UTC().Format(time.RFC3339)})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "runtime.json"), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeMarker(current, teamsMathRuntimeVersion, time.Now())
	writeMarker(previous, "previous-version", time.Now().Add(-time.Hour))
	writeMarker(old, "old-version", time.Now().Add(-2*time.Hour))
	foreignMarker, err := json.Marshal(teamsMathRuntimeMarker{Version: "foreign-version", Installed: time.Now().Add(-3 * time.Hour).UTC().Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "runtime.json"), foreignMarker, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unrelated, "keep.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	removed, err := cleanupObsoleteTeamsMathRuntimes(parent, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || filepath.Clean(removed[0]) != filepath.Clean(old) {
		t.Fatalf("removed = %#v, want only %q", removed, old)
	}
	for _, kept := range []string{current, previous, unrelated, foreign} {
		if _, err := os.Stat(kept); err != nil {
			t.Fatalf("kept directory %q missing: %v", kept, err)
		}
	}
}

func TestScanTeamsMathPNGCacheIgnoresForeignFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	managed := teamsMathCacheKey("x") + ".png"
	for _, name := range []string{managed, "notes.png", strings.Repeat("g", 64) + ".png", "README.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), testMathPNG(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	entries, total, err := scanTeamsMathPNGCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || total != int64(len(testMathPNG())) {
		t.Fatalf("managed cache entries=%d bytes=%d, want 1/%d", len(entries), total, len(testMathPNG()))
	}
	if _, ok := entries[filepath.Join(dir, managed)]; !ok {
		t.Fatalf("managed cache entry missing: %#v", entries)
	}
}

func TestValidateTeamsMathPNGRejectsExcessiveDecodedArea(t *testing.T) {
	t.Parallel()
	data := append([]byte(nil), testMathPNG()...)
	binary.BigEndian.PutUint32(data[16:20], 4096)
	binary.BigEndian.PutUint32(data[20:24], 4096)
	binary.BigEndian.PutUint32(data[29:33], crc32.ChecksumIEEE(data[12:29]))
	if _, ok := validateTeamsMathPNG(data); ok {
		t.Fatal("accepted PNG whose decoded pixel area exceeds the resource limit")
	}
}

func TestTeamsMathBoundedBufferDrainsWithoutGrowingPastLimit(t *testing.T) {
	t.Parallel()
	buffer := newTeamsMathBoundedBuffer(4)
	if n, err := buffer.Write([]byte("abcdefgh")); err != nil || n != 8 {
		t.Fatalf("Write returned n=%d err=%v", n, err)
	}
	if got := buffer.String(); got != "abcd" || !buffer.Truncated() {
		t.Fatalf("buffer=%q truncated=%v", got, buffer.Truncated())
	}
}
