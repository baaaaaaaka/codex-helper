package teams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTeamsMathCmapDetectsPresentAndMissingGlyphs(t *testing.T) {
	t.Parallel()
	font := testTeamsMathSFNT([]testTeamsMathCmapGroup{
		{start: 'A', end: 'A'},
		{start: '中', end: '中'},
		{start: '文', end: '文'},
	})
	path := filepath.Join(t.TempDir(), "test.ttf")
	if err := os.WriteFile(path, font, 0o600); err != nil {
		t.Fatal(err)
	}
	coverage, err := mathFontRuneCoverage(path, []rune{'A', 'B', '中', '文', '한'})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []rune{'A', '中', '文'} {
		if !coverage[value] {
			t.Fatalf("expected U+%04X to be covered: %#v", value, coverage)
		}
	}
	for _, value := range []rune{'B', '한'} {
		if coverage[value] {
			t.Fatalf("unexpected coverage for U+%04X: %#v", value, coverage)
		}
	}
}

func TestTeamsMathCmapFormat4HandlesDeltaAndMissingGlyph(t *testing.T) {
	t.Parallel()
	data := make([]byte, 32)
	binary.BigEndian.PutUint16(data[0:2], 4)
	binary.BigEndian.PutUint16(data[2:4], uint16(len(data)))
	binary.BigEndian.PutUint16(data[6:8], 4)
	binary.BigEndian.PutUint16(data[14:16], 'A')
	binary.BigEndian.PutUint16(data[16:18], 0xffff)
	binary.BigEndian.PutUint16(data[20:22], 'A')
	binary.BigEndian.PutUint16(data[22:24], 0xffff)
	binary.BigEndian.PutUint16(data[24:26], 0xffc0)
	binary.BigEndian.PutUint16(data[26:28], 1)
	cmap := teamsMathCmapSubtable{format: 4, data: data}
	if !teamsMathCmapHasRune(cmap, 'A') || teamsMathCmapHasRune(cmap, 'B') || teamsMathCmapHasRune(cmap, 0xffff) {
		t.Fatal("format 4 cmap lookup returned the wrong coverage")
	}
}

func TestTeamsMathCmapParserRejectsEveryTruncatedPrefix(t *testing.T) {
	t.Parallel()
	font := testTeamsMathSFNT([]testTeamsMathCmapGroup{{start: '中', end: '文'}})
	for length := 0; length < len(font); length++ {
		reader := bytes.NewReader(font[:length])
		offsets, err := teamsMathFontFaceOffsets(reader, int64(length))
		if err != nil {
			continue
		}
		accepted := true
		for _, offset := range offsets {
			if _, err := loadTeamsMathFaceCmaps(reader, int64(length), offset); err != nil {
				accepted = false
				break
			}
		}
		if accepted {
			t.Fatalf("accepted truncated font prefix of %d/%d bytes", length, len(font))
		}
	}
}

func TestManagedMathFontDownloadsOnceAndReusesVerifiedCache(t *testing.T) {
	t.Parallel()
	fontData := testTeamsMathSFNT([]testTeamsMathCmapGroup{{start: '中', end: '中'}, {start: '文', end: '文'}})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = writer.Write(fontData)
	}))
	defer server.Close()
	asset := testTeamsMathFontAsset(server.URL, fontData, func(value rune) bool { return value == '中' || value == '文' })
	renderer := &managedTeamsMathRenderer{cacheRoot: t.TempDir(), fontAssets: []teamsMathFontAsset{asset}, fontHTTPClient: server.Client()}

	fonts, added, err := renderer.ensureManagedMathFonts(context.Background(), renderer.cacheRoot, nil, []string{"中文"})
	if err != nil || !added || len(fonts) != 1 {
		t.Fatalf("first ensure fonts=%#v added=%v err=%v", fonts, added, err)
	}
	if requests.Load() != 1 {
		t.Fatalf("font downloads=%d, want 1", requests.Load())
	}
	if missing, err := renderer.missingManagedMathGlyphs(fonts, []string{"中文"}); err != nil || len(missing) != 0 {
		t.Fatalf("downloaded font coverage missing=%v err=%v", missing, err)
	}
	secondRenderer := &managedTeamsMathRenderer{cacheRoot: renderer.cacheRoot, fontAssets: []teamsMathFontAsset{asset}, fontHTTPClient: server.Client()}
	cached := secondRenderer.installedManagedMathFonts(renderer.cacheRoot)
	fonts, added, err = secondRenderer.ensureManagedMathFonts(context.Background(), renderer.cacheRoot, cached, []string{"中文"})
	if err != nil || added || len(fonts) != 1 || requests.Load() != 1 {
		t.Fatalf("cached ensure fonts=%#v added=%v downloads=%d err=%v", fonts, added, requests.Load(), err)
	}
}

func TestManagedMathFontReplacesSameSizeCorruptCache(t *testing.T) {
	t.Parallel()
	fontData := testTeamsMathSFNT([]testTeamsMathCmapGroup{{start: '中', end: '中'}})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = writer.Write(fontData)
	}))
	defer server.Close()
	asset := testTeamsMathFontAsset(server.URL, fontData, func(value rune) bool { return value == '中' })
	root := t.TempDir()
	first := &managedTeamsMathRenderer{cacheRoot: root, fontAssets: []teamsMathFontAsset{asset}, fontHTTPClient: server.Client()}
	if _, err := first.ensureManagedMathFont(context.Background(), root, asset, []rune{'中'}); err != nil {
		t.Fatal(err)
	}
	fontPath := filepath.Join(managedTeamsMathFontDir(root, asset), asset.Filename)
	corrupt := append([]byte(nil), fontData...)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := os.WriteFile(fontPath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}

	second := &managedTeamsMathRenderer{cacheRoot: root, fontAssets: []teamsMathFontAsset{asset}, fontHTTPClient: server.Client()}
	font, err := second.ensureManagedMathFont(context.Background(), root, asset, []rune{'中'})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(font.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, fontData) || requests.Load() != 2 {
		t.Fatalf("corrupt cache restored=%v downloads=%d, want true and 2", bytes.Equal(restored, fontData), requests.Load())
	}
}

func TestManagedMathFontRevalidatesChangedFileInSameRenderer(t *testing.T) {
	t.Parallel()
	fontData := testTeamsMathSFNT([]testTeamsMathCmapGroup{{start: '中', end: '中'}})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = writer.Write(fontData)
	}))
	defer server.Close()
	asset := testTeamsMathFontAsset(server.URL, fontData, func(value rune) bool { return value == '中' })
	root := t.TempDir()
	renderer := &managedTeamsMathRenderer{cacheRoot: root, fontAssets: []teamsMathFontAsset{asset}, fontHTTPClient: server.Client()}
	font, err := renderer.ensureManagedMathFont(context.Background(), root, asset, []rune{'中'})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(font.Path)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), fontData...)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := os.WriteFile(font.Path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	changed := info.ModTime().Add(2 * time.Second)
	if err := os.Chtimes(font.Path, changed, changed); err != nil {
		t.Fatal(err)
	}

	font, err = renderer.ensureManagedMathFont(context.Background(), root, asset, []rune{'中'})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(font.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, fontData) || requests.Load() != 2 {
		t.Fatalf("same-renderer corrupt cache restored=%v downloads=%d, want true and 2", bytes.Equal(restored, fontData), requests.Load())
	}
}

func TestManagedMathFontConcurrentInstallIsSingleDownload(t *testing.T) {
	fontData := testTeamsMathSFNT([]testTeamsMathCmapGroup{{start: '中', end: '中'}})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		time.Sleep(30 * time.Millisecond)
		_, _ = writer.Write(fontData)
	}))
	defer server.Close()
	root := t.TempDir()
	asset := testTeamsMathFontAsset(server.URL, fontData, func(value rune) bool { return value == '中' })
	const workers = 12
	start := make(chan struct{})
	errors := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			renderer := &managedTeamsMathRenderer{cacheRoot: root, fontAssets: []teamsMathFontAsset{asset}, fontHTTPClient: server.Client()}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := renderer.ensureManagedMathFont(ctx, root, asset, []rune{'中'})
			errors <- err
		}()
	}
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("concurrent font downloads=%d, want 1", requests.Load())
	}
}

func TestManagedMathFontRejectsWrongHashWithoutPublishing(t *testing.T) {
	t.Parallel()
	fontData := testTeamsMathSFNT([]testTeamsMathCmapGroup{{start: '中', end: '中'}})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = writer.Write(fontData)
	}))
	defer server.Close()
	asset := testTeamsMathFontAsset(server.URL, fontData, func(value rune) bool { return value == '中' })
	asset.SHA256 = strings.Repeat("0", 64)
	root := t.TempDir()
	renderer := &managedTeamsMathRenderer{cacheRoot: root, fontAssets: []teamsMathFontAsset{asset}, fontHTTPClient: server.Client()}
	_, _, err := renderer.ensureManagedMathFonts(context.Background(), root, nil, []string{"中"})
	if err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Fatalf("wrong-hash error=%v", err)
	}
	if requests.Load() != 2 {
		t.Fatalf("integrity retry downloads=%d, want 2", requests.Load())
	}
	if _, statErr := os.Stat(managedTeamsMathFontDir(root, asset)); !os.IsNotExist(statErr) {
		t.Fatalf("invalid font was published: %v", statErr)
	}
}

func TestManagedMathFontRejectsValidFontWithoutRequestedGlyph(t *testing.T) {
	t.Parallel()
	fontData := testTeamsMathSFNT([]testTeamsMathCmapGroup{{start: 'A', end: 'Z'}})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = writer.Write(fontData)
	}))
	defer server.Close()
	asset := testTeamsMathFontAsset(server.URL, fontData, func(value rune) bool { return value == '中' })
	root := t.TempDir()
	renderer := &managedTeamsMathRenderer{cacheRoot: root, fontAssets: []teamsMathFontAsset{asset}, fontHTTPClient: server.Client()}
	_, _, err := renderer.ensureManagedMathFonts(context.Background(), root, nil, []string{"中"})
	if err == nil || !strings.Contains(err.Error(), "does not cover") {
		t.Fatalf("wrong-coverage error=%v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("wrong-coverage downloads=%d, want 1", requests.Load())
	}
	if _, statErr := os.Stat(managedTeamsMathFontDir(root, asset)); !os.IsNotExist(statErr) {
		t.Fatalf("wrong-coverage font was published: %v", statErr)
	}
}

func TestManagedMathFontRejectsOversizedAssetBeforeNetwork(t *testing.T) {
	t.Parallel()
	asset := teamsMathFontAsset{
		ID: "huge", Version: "1", Family: "Huge", Filename: "huge.otf",
		URL: "https://invalid.example/font.otf", Size: maxTeamsMathFontDownloadBytes + 1,
		SHA256: strings.Repeat("0", 64), Matches: func(rune) bool { return true },
	}
	if err := validateTeamsMathFontAsset(asset); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized asset error=%v", err)
	}
}

func TestManagedMathFontUnsupportedScriptFailsWithoutDownload(t *testing.T) {
	t.Parallel()
	fontData := testTeamsMathSFNT([]testTeamsMathCmapGroup{{start: '中', end: '中'}})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = writer.Write(fontData)
	}))
	defer server.Close()
	asset := testTeamsMathFontAsset(server.URL, fontData, func(value rune) bool { return value == '中' })
	renderer := &managedTeamsMathRenderer{cacheRoot: t.TempDir(), fontAssets: []teamsMathFontAsset{asset}, fontHTTPClient: server.Client()}
	_, added, err := renderer.ensureManagedMathFonts(context.Background(), renderer.cacheRoot, nil, []string{"العربية"})
	if err == nil || added || requests.Load() != 0 {
		t.Fatalf("unsupported-script added=%v downloads=%d err=%v", added, requests.Load(), err)
	}
}

func TestTeamsMathVisibleRunesIgnoreNonGlyphFormatting(t *testing.T) {
	t.Parallel()
	got := teamsMathVisibleRunes([]string{" 中\u200d\ufe0f\n文 "})
	if string(got) != "中文" {
		t.Fatalf("visible runes=%U, want Chinese glyphs only", got)
	}
}

type testTeamsMathCmapGroup struct {
	start rune
	end   rune
}

func testTeamsMathSFNT(groups []testTeamsMathCmapGroup) []byte {
	subtable := make([]byte, 16+len(groups)*12)
	binary.BigEndian.PutUint16(subtable[0:2], 12)
	binary.BigEndian.PutUint32(subtable[4:8], uint32(len(subtable)))
	binary.BigEndian.PutUint32(subtable[12:16], uint32(len(groups)))
	for index, group := range groups {
		position := 16 + index*12
		binary.BigEndian.PutUint32(subtable[position:position+4], uint32(group.start))
		binary.BigEndian.PutUint32(subtable[position+4:position+8], uint32(group.end))
		binary.BigEndian.PutUint32(subtable[position+8:position+12], 1)
	}
	cmap := make([]byte, 12+len(subtable))
	binary.BigEndian.PutUint16(cmap[2:4], 1)
	binary.BigEndian.PutUint16(cmap[4:6], 3)
	binary.BigEndian.PutUint16(cmap[6:8], 10)
	binary.BigEndian.PutUint32(cmap[8:12], 12)
	copy(cmap[12:], subtable)
	font := make([]byte, 28+len(cmap))
	binary.BigEndian.PutUint32(font[0:4], 0x00010000)
	binary.BigEndian.PutUint16(font[4:6], 1)
	copy(font[12:16], []byte("cmap"))
	binary.BigEndian.PutUint32(font[20:24], 28)
	binary.BigEndian.PutUint32(font[24:28], uint32(len(cmap)))
	copy(font[28:], cmap)
	return font
}

func testTeamsMathFontAsset(url string, data []byte, matches func(rune) bool) teamsMathFontAsset {
	hash := sha256.Sum256(data)
	return teamsMathFontAsset{
		ID:       "test-serif",
		Version:  "1.0",
		Family:   "Test Serif",
		Filename: "TestSerif-Regular.ttf",
		URL:      url,
		Size:     int64(len(data)),
		SHA256:   hex.EncodeToString(hash[:]),
		Matches:  matches,
	}
}
