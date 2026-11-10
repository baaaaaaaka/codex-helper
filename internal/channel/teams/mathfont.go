package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
)

// Math fonts.
//
// MathJax draws formulas with its own fonts; text it cannot draw (CJK in
// \text{}, for example) comes back from the renderer as "text". Such a
// formula is only accepted when an installed font has a glyph for every one
// of those runes (checked in the font's cmap), otherwise it falls back to a
// TeX code block. Pinned fonts are downloaded on demand, only when a
// formula needs a rune they cover, verified by size and SHA-256, and
// installed atomically next to the runtime as
// fonts/<id>-<version>/{<file>,font.sqlite,LICENSE.txt} (older metadata is migrated once).

// FontAsset is a pinned font file the math renderer may download.
type FontAsset struct {
	ID       string
	Version  string
	Family   string
	Filename string
	URL      string
	Size     int64
	SHA256   string
	License  string          // file under mathjs/fonts/ installed as LICENSE.txt
	Matches  func(rune) bool // runes worth downloading this font for
}

// DefaultMathFonts is the font set NodeMath uses when Fonts is nil.
var DefaultMathFonts = []FontAsset{{
	ID:       "noto-serif-sc",
	Version:  "2.003",
	Family:   "Noto Serif SC",
	Filename: "NotoSerifSC-Regular.otf",
	URL:      "https://raw.githubusercontent.com/notofonts/noto-cjk/Serif2.003/Serif/SubsetOTF/SC/NotoSerifSC-Regular.otf",
	Size:     11_625_800,
	SHA256:   "e8f396decc1f0963a016a989c3d8852e863d1350996f573860a80767c83a1cd3",
	License:  "NotoSerifSC-LICENSE.txt",
	Matches:  func(r rune) bool { return unicode.Is(unicode.Han, r) },
}}

const (
	fontMarkerKind = "codex-helper-teams-math-font"
	maxFontBytes   = 16 << 20
	fontInstallTO  = 2 * time.Minute
)

// renderFont is what renderer.mjs expects in request.fonts.
type renderFont struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Path    string `json:"path"`
	Family  string `json:"family"`
}

type fontMarker struct {
	Kind      string `json:"kind"`
	ID        string `json:"id"`
	Version   string `json:"version"`
	Filename  string `json:"filename"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256"`
	Installed string `json:"installed_at"`
}

type fontStamp struct {
	size  int64
	mtime time.Time
	cmaps []cmapSubtable
}

func (n *NodeMath) fontAssets() []FontAsset {
	if n.Fonts != nil {
		return n.Fonts
	}
	return DefaultMathFonts
}

func fontDir(root string, a FontAsset) string {
	return filepath.Join(root, "fonts", a.ID+"-"+a.Version)
}

// installedFonts returns the verified installed fonts.
func (n *NodeMath) installedFonts(root string) []renderFont {
	var out []renderFont
	for _, a := range n.fontAssets() {
		if f, err := n.verifyFont(root, a); err == nil {
			out = append(out, f)
		}
	}
	return out
}

func validFontAsset(a FontAsset) error {
	for _, v := range []string{a.ID, a.Version, a.Filename} {
		if !safeFontName(v) {
			return fmt.Errorf("math font name %q is unsafe", v)
		}
	}
	if a.URL == "" || a.Family == "" || a.Size <= 0 || a.Size > maxFontBytes {
		return fmt.Errorf("math font %s metadata is incomplete", a.ID)
	}
	if b, err := hex.DecodeString(a.SHA256); err != nil || len(b) != sha256.Size {
		return fmt.Errorf("math font %s SHA-256 is invalid", a.ID)
	}
	return nil
}

func safeFontName(v string) bool {
	if v == "" || v == "." || v == ".." {
		return false
	}
	for _, c := range v {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

// verifyFont checks marker, size, SHA-256 (once per file version) and cmap.
func (n *NodeMath) verifyFont(root string, a FontAsset) (renderFont, error) {
	if err := validFontAsset(a); err != nil {
		return renderFont{}, err
	}
	dir := fontDir(root, a)
	path := filepath.Join(dir, a.Filename)
	rf := renderFont{ID: a.ID, Version: a.Version, Path: path, Family: a.Family}
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Size() != a.Size {
		return renderFont{}, errors.New("math font file is missing or has the wrong size")
	}
	n.fontMu.Lock()
	stamp, ok := n.fonts[path]
	n.fontMu.Unlock()
	if ok && stamp.size == st.Size() && stamp.mtime.Equal(st.ModTime()) {
		return rf, nil
	}
	mk, err := readFontMarker(dir)
	if err != nil {
		return renderFont{}, err
	}
	if mk.Kind != fontMarkerKind || mk.ID != a.ID || mk.Version != a.Version || mk.Filename != a.Filename || mk.Size != a.Size || !strings.EqualFold(mk.SHA256, a.SHA256) {
		return renderFont{}, errors.New("math font marker does not match")
	}
	if a.License != "" {
		if li, err := os.Stat(filepath.Join(dir, "LICENSE.txt")); err != nil || li.Size() == 0 {
			return renderFont{}, errors.New("math font license is missing")
		}
	}
	if err := checkFileSHA(path, a.Size, a.SHA256); err != nil {
		return renderFont{}, err
	}
	cmaps, err := loadFontCmaps(path)
	if err != nil {
		return renderFont{}, err
	}
	n.fontMu.Lock()
	if n.fonts == nil {
		n.fonts = map[string]fontStamp{}
	}
	n.fonts[path] = fontStamp{size: st.Size(), mtime: st.ModTime(), cmaps: cmaps}
	n.fontMu.Unlock()
	return rf, nil
}

func checkFileSHA(path string, size int64, want string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	got, err := io.Copy(h, io.LimitReader(f, size+1))
	if err != nil {
		return err
	}
	if got != size || !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), want) {
		return errors.New("math font integrity check failed")
	}
	return nil
}

// needsGlyph reports whether a rune of renderer "text" must be drawn.
func needsGlyph(r rune) bool {
	if unicode.IsSpace(r) || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
		return false
	}
	return !(r >= 0xfe00 && r <= 0xfe0f) && !(r >= 0xe0100 && r <= 0xe01ef)
}

// missingGlyphs returns the runes of texts no font in fonts can draw.
func (n *NodeMath) missingGlyphs(fonts []renderFont, texts ...string) []rune {
	need := map[rune]bool{}
	for _, t := range texts {
		for _, r := range t {
			if needsGlyph(r) {
				need[r] = true
			}
		}
	}
	n.fontMu.Lock()
	defer n.fontMu.Unlock()
	var missing []rune
	for r := range need {
		covered := false
		for _, f := range fonts {
			if cmapCoverage(n.fonts[f.Path].cmaps, []rune{r})[r] {
				covered = true
				break
			}
		}
		if !covered {
			missing = append(missing, r)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return missing
}

// ensureFonts installs the assets that cover missing runes of texts.
// It returns the (possibly extended) font list and whether it changed.
func (n *NodeMath) ensureFonts(ctx context.Context, root string, fonts []renderFont, texts []string) ([]renderFont, bool, error) {
	missing := n.missingGlyphs(fonts, texts...)
	if len(missing) == 0 {
		return fonts, false, nil
	}
	have := map[string]bool{}
	for _, f := range fonts {
		have[f.ID] = true
	}
	added := false
	for _, a := range n.fontAssets() {
		if have[a.ID] || a.Matches == nil || !matchesAny(a, missing) {
			continue
		}
		f, err := n.installFont(ctx, root, a, missing)
		if err != nil {
			return fonts, added, err
		}
		fonts, added = append(fonts, f), true
		if missing = n.missingGlyphs(fonts, texts...); len(missing) == 0 {
			return fonts, true, nil
		}
	}
	return fonts, added, fmt.Errorf("no math font covers %s", runeList(missing))
}

func matchesAny(a FontAsset, rs []rune) bool {
	for _, r := range rs {
		if a.Matches(r) {
			return true
		}
	}
	return false
}

func runeList(rs []rune) string {
	var parts []string
	for i, r := range rs {
		if i == 8 {
			parts = append(parts, fmt.Sprintf("and %d more", len(rs)-8))
			break
		}
		parts = append(parts, fmt.Sprintf("U+%04X", r))
	}
	return strings.Join(parts, ", ")
}

func (n *NodeMath) installFont(ctx context.Context, root string, a FontAsset, want []rune) (renderFont, error) {
	if err := validFontAsset(a); err != nil {
		return renderFont{}, err
	}
	if f, err := n.verifyFont(root, a); err == nil {
		return f, nil
	}
	timeout := n.FontTimeout
	if timeout <= 0 {
		timeout = fontInstallTO
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	fontsRoot := filepath.Join(root, "fonts")
	unlock, err := lockFile(ctx, filepath.Join(fontsRoot, "install.lock"))
	if err != nil {
		return renderFont{}, fmt.Errorf("lock math font install: %w", err)
	}
	defer unlock()
	if f, err := n.verifyFont(root, a); err == nil {
		return f, nil // installed by another process meanwhile
	}
	stage, err := os.MkdirTemp(fontsRoot, ".install-"+a.ID+"-")
	if err != nil {
		return renderFont{}, err
	}
	defer os.RemoveAll(stage)
	path := filepath.Join(stage, a.Filename)
	var derr error
	for attempt := 0; attempt < 2; attempt++ {
		if derr = n.downloadFont(ctx, a, path); derr == nil {
			break
		}
	}
	if derr != nil {
		return renderFont{}, derr
	}
	cmaps, err := loadFontCmaps(path)
	if err != nil {
		return renderFont{}, fmt.Errorf("downloaded math font: %w", err)
	}
	covers := false
	for _, ok := range cmapCoverage(cmaps, want) {
		covers = covers || ok
	}
	if !covers {
		return renderFont{}, fmt.Errorf("downloaded math font %s covers none of %s", a.ID, runeList(want))
	}
	if a.License != "" {
		lic, err := mathjsFiles.ReadFile("mathjs/fonts/" + a.License)
		if err != nil {
			return renderFont{}, err
		}
		if err := os.WriteFile(filepath.Join(stage, "LICENSE.txt"), lic, 0o600); err != nil {
			return renderFont{}, err
		}
	}
	if err := writeFontMarker(stage, fontMarker{Kind: fontMarkerKind, ID: a.ID, Version: a.Version, Filename: a.Filename, Size: a.Size, SHA256: strings.ToLower(a.SHA256), Installed: time.Now().UTC().Format(time.RFC3339)}); err != nil {
		return renderFont{}, err
	}
	target := fontDir(root, a)
	if err := os.RemoveAll(target); err != nil {
		return renderFont{}, err
	}
	if err := os.Rename(stage, target); err != nil {
		return renderFont{}, err
	}
	return n.verifyFont(root, a)
}

func (n *NodeMath) downloadFont(ctx context.Context, a FontAsset, path string) error {
	client := n.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.URL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download math font: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download math font: HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > 0 && resp.ContentLength != a.Size {
		return fmt.Errorf("download math font: %d bytes, want %d", resp.ContentLength, a.Size)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	h := sha256.New()
	got, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(resp.Body, a.Size+1))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("download math font: %w", err)
	}
	if got != a.Size || !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), a.SHA256) {
		return errors.New("download math font: size or SHA-256 mismatch")
	}
	return nil
}
