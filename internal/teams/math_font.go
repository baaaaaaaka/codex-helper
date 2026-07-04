package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	"github.com/gofrs/flock"
)

const (
	teamsMathFontKind             = "codex-helper-teams-math-font"
	maxTeamsMathFontDownloadBytes = 16 * 1024 * 1024
)

type teamsMathHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type teamsMathRenderFont struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Path    string `json:"path"`
	Family  string `json:"family"`
}

type teamsMathFontAsset struct {
	ID          string
	Version     string
	Family      string
	Filename    string
	URL         string
	Size        int64
	SHA256      string
	LicenseFile string
	Matches     func(rune) bool
}

type teamsMathFontMarker struct {
	Kind      string `json:"kind"`
	ID        string `json:"id"`
	Version   string `json:"version"`
	Filename  string `json:"filename"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256"`
	Installed string `json:"installed_at"`
}

type teamsMathVerifiedFont struct {
	Size    int64
	ModTime time.Time
}

var defaultTeamsMathFontAssets = []teamsMathFontAsset{
	{
		ID:          "noto-serif-sc",
		Version:     "2.003",
		Family:      "Noto Serif SC",
		Filename:    "NotoSerifSC-Regular.otf",
		URL:         "https://raw.githubusercontent.com/notofonts/noto-cjk/Serif2.003/Serif/SubsetOTF/SC/NotoSerifSC-Regular.otf",
		Size:        11_625_800,
		SHA256:      "e8f396decc1f0963a016a989c3d8852e863d1350996f573860a80767c83a1cd3",
		LicenseFile: "NotoSerifSC-LICENSE.txt",
		Matches: func(r rune) bool {
			return unicode.Is(unicode.Han, r)
		},
	},
}

func (r *managedTeamsMathRenderer) managedMathFontAssets() []teamsMathFontAsset {
	if r.fontAssets != nil {
		return r.fontAssets
	}
	return defaultTeamsMathFontAssets
}

func (r *managedTeamsMathRenderer) installedManagedMathFonts(runtimeRoot string) []teamsMathRenderFont {
	assets := r.managedMathFontAssets()
	fonts := make([]teamsMathRenderFont, 0, len(assets))
	for _, asset := range assets {
		font, err := r.verifyManagedMathFont(runtimeRoot, asset)
		if err == nil {
			fonts = append(fonts, font)
		}
	}
	return fonts
}

func (r *managedTeamsMathRenderer) ensureManagedMathFonts(ctx context.Context, runtimeRoot string, fonts []teamsMathRenderFont, texts []string) ([]teamsMathRenderFont, bool, error) {
	missing, err := r.missingManagedMathGlyphs(fonts, texts)
	if err != nil {
		return fonts, false, err
	}
	if len(missing) == 0 {
		return fonts, false, nil
	}
	installed := make(map[string]bool, len(fonts))
	for _, font := range fonts {
		installed[font.ID] = true
	}
	added := false
	for _, asset := range r.managedMathFontAssets() {
		if installed[asset.ID] || !assetMatchesAnyMathRune(asset, missing) {
			continue
		}
		font, installErr := r.ensureManagedMathFont(ctx, runtimeRoot, asset, missing)
		if installErr != nil {
			return fonts, added, installErr
		}
		fonts = append(fonts, font)
		installed[asset.ID] = true
		added = true
		missing, err = r.missingManagedMathGlyphs(fonts, texts)
		if err != nil {
			return fonts, added, err
		}
		if len(missing) == 0 {
			return fonts, added, nil
		}
	}
	return fonts, added, fmt.Errorf("no managed math font covers %s", formatTeamsMathRunes(missing))
}

func assetMatchesAnyMathRune(asset teamsMathFontAsset, runes []rune) bool {
	if asset.Matches == nil {
		return false
	}
	for _, value := range runes {
		if asset.Matches(value) {
			return true
		}
	}
	return false
}

func (r *managedTeamsMathRenderer) ensureManagedMathFont(ctx context.Context, runtimeRoot string, asset teamsMathFontAsset, requested []rune) (teamsMathRenderFont, error) {
	if err := validateTeamsMathFontAsset(asset); err != nil {
		return teamsMathRenderFont{}, err
	}
	if font, err := r.verifyManagedMathFont(runtimeRoot, asset); err == nil {
		return font, nil
	}
	fontsRoot := filepath.Join(runtimeRoot, "fonts")
	if err := os.MkdirAll(fontsRoot, 0o700); err != nil {
		return teamsMathRenderFont{}, fmt.Errorf("create managed math font directory: %w", err)
	}
	lock := flock.New(filepath.Join(fontsRoot, "install.lock"))
	locked, err := lock.TryLockContext(ctx, 250*time.Millisecond)
	if err != nil {
		return teamsMathRenderFont{}, fmt.Errorf("lock managed math font install: %w", err)
	}
	if !locked {
		return teamsMathRenderFont{}, fmt.Errorf("managed math font setup is already in progress")
	}
	defer func() { _ = lock.Unlock() }()
	if font, err := r.verifyManagedMathFont(runtimeRoot, asset); err == nil {
		return font, nil
	}

	stage, err := os.MkdirTemp(fontsRoot, ".install-"+asset.ID+"-")
	if err != nil {
		return teamsMathRenderFont{}, fmt.Errorf("create managed math font staging directory: %w", err)
	}
	defer os.RemoveAll(stage)
	fontPath := filepath.Join(stage, asset.Filename)
	client := r.fontHTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	if err := downloadManagedMathFont(ctx, client, asset, fontPath); err != nil {
		return teamsMathRenderFont{}, err
	}
	stagedCmaps, coverageErr := loadTeamsMathFontCmaps(fontPath)
	if coverageErr != nil {
		return teamsMathRenderFont{}, fmt.Errorf("validate downloaded managed math font: %w", coverageErr)
	}
	covered := false
	for _, supported := range teamsMathCmapCoverage(stagedCmaps, requested) {
		if supported {
			covered = true
			break
		}
	}
	if !covered {
		return teamsMathRenderFont{}, fmt.Errorf("downloaded managed math font %s does not cover the requested text", asset.ID)
	}
	if asset.LicenseFile != "" {
		license, readErr := teamsMathRuntimeFiles.ReadFile("mathruntime/fonts/" + asset.LicenseFile)
		if readErr != nil {
			return teamsMathRenderFont{}, fmt.Errorf("read managed math font license: %w", readErr)
		}
		if writeErr := os.WriteFile(filepath.Join(stage, "LICENSE.txt"), license, 0o600); writeErr != nil {
			return teamsMathRenderFont{}, fmt.Errorf("write managed math font license: %w", writeErr)
		}
	}
	marker, err := json.Marshal(teamsMathFontMarker{
		Kind:      teamsMathFontKind,
		ID:        asset.ID,
		Version:   asset.Version,
		Filename:  asset.Filename,
		Size:      asset.Size,
		SHA256:    strings.ToLower(asset.SHA256),
		Installed: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return teamsMathRenderFont{}, err
	}
	if err := os.WriteFile(filepath.Join(stage, "font.json"), marker, 0o600); err != nil {
		return teamsMathRenderFont{}, fmt.Errorf("write managed math font marker: %w", err)
	}
	target := managedTeamsMathFontDir(runtimeRoot, asset)
	if err := os.RemoveAll(target); err != nil && !errors.Is(err, os.ErrNotExist) {
		return teamsMathRenderFont{}, fmt.Errorf("remove stale managed math font: %w", err)
	}
	if err := os.Rename(stage, target); err != nil {
		return teamsMathRenderFont{}, fmt.Errorf("publish managed math font: %w", err)
	}
	path := filepath.Join(target, asset.Filename)
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != asset.Size {
		return teamsMathRenderFont{}, fmt.Errorf("published managed math font is missing or has the wrong size")
	}
	if r.verifiedFonts == nil {
		r.verifiedFonts = make(map[string]teamsMathVerifiedFont)
	}
	r.verifiedFonts[path] = teamsMathVerifiedFont{Size: info.Size(), ModTime: info.ModTime()}
	if r.fontGlyphCoverage == nil {
		r.fontGlyphCoverage = make(map[string]map[rune]bool)
	}
	r.fontGlyphCoverage[path] = make(map[rune]bool)
	if r.fontCmaps == nil {
		r.fontCmaps = make(map[string][]teamsMathCmapSubtable)
	}
	r.fontCmaps[path] = stagedCmaps
	return teamsMathRenderFont{ID: asset.ID, Version: asset.Version, Path: path, Family: asset.Family}, nil
}

func managedTeamsMathFontDir(runtimeRoot string, asset teamsMathFontAsset) string {
	return filepath.Join(runtimeRoot, "fonts", asset.ID+"-"+asset.Version)
}

func validateTeamsMathFontAsset(asset teamsMathFontAsset) error {
	for label, value := range map[string]string{
		"id": asset.ID, "version": asset.Version, "filename": asset.Filename,
	} {
		if !safeTeamsMathFontName(value) {
			return fmt.Errorf("managed math font %s is unsafe", label)
		}
	}
	if strings.TrimSpace(asset.Family) == "" || strings.TrimSpace(asset.URL) == "" {
		return fmt.Errorf("managed math font metadata is incomplete")
	}
	if asset.Size <= 0 || asset.Size > maxTeamsMathFontDownloadBytes {
		return fmt.Errorf("managed math font size %d exceeds the %d-byte limit", asset.Size, maxTeamsMathFontDownloadBytes)
	}
	if len(asset.SHA256) != 64 {
		return fmt.Errorf("managed math font SHA-256 is invalid")
	}
	if _, err := hex.DecodeString(asset.SHA256); err != nil {
		return fmt.Errorf("managed math font SHA-256 is invalid: %w", err)
	}
	return nil
}

func safeTeamsMathFontName(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func (r *managedTeamsMathRenderer) verifyManagedMathFont(runtimeRoot string, asset teamsMathFontAsset) (teamsMathRenderFont, error) {
	if err := validateTeamsMathFontAsset(asset); err != nil {
		return teamsMathRenderFont{}, err
	}
	dir := managedTeamsMathFontDir(runtimeRoot, asset)
	path := filepath.Join(dir, asset.Filename)
	if cached, ok := r.verifiedFonts[path]; ok {
		info, statErr := os.Stat(path)
		if statErr == nil && info.Mode().IsRegular() && info.Size() == cached.Size && info.ModTime().Equal(cached.ModTime) {
			return teamsMathRenderFont{ID: asset.ID, Version: asset.Version, Path: path, Family: asset.Family}, nil
		}
		delete(r.verifiedFonts, path)
		delete(r.fontGlyphCoverage, path)
		delete(r.fontCmaps, path)
	}
	markerData, err := os.ReadFile(filepath.Join(dir, "font.json"))
	if err != nil {
		return teamsMathRenderFont{}, err
	}
	var marker teamsMathFontMarker
	if json.Unmarshal(markerData, &marker) != nil || marker.Kind != teamsMathFontKind || marker.ID != asset.ID || marker.Version != asset.Version || marker.Filename != asset.Filename || marker.Size != asset.Size || !strings.EqualFold(marker.SHA256, asset.SHA256) {
		return teamsMathRenderFont{}, fmt.Errorf("managed math font marker is invalid")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != asset.Size {
		return teamsMathRenderFont{}, fmt.Errorf("managed math font file is missing or has the wrong size")
	}
	file, err := os.Open(path)
	if err != nil {
		return teamsMathRenderFont{}, err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(hash, io.LimitReader(file, asset.Size+1))
	closeErr := file.Close()
	if copyErr != nil {
		return teamsMathRenderFont{}, fmt.Errorf("read managed math font: %w", copyErr)
	}
	if closeErr != nil {
		return teamsMathRenderFont{}, closeErr
	}
	if written != asset.Size || !strings.EqualFold(hex.EncodeToString(hash.Sum(nil)), asset.SHA256) {
		return teamsMathRenderFont{}, fmt.Errorf("managed math font integrity check failed")
	}
	cmaps, parseErr := loadTeamsMathFontCmaps(path)
	if parseErr != nil {
		return teamsMathRenderFont{}, fmt.Errorf("parse managed math font cmap: %w", parseErr)
	}
	if asset.LicenseFile != "" {
		if licenseInfo, licenseErr := os.Stat(filepath.Join(dir, "LICENSE.txt")); licenseErr != nil || !licenseInfo.Mode().IsRegular() || licenseInfo.Size() == 0 {
			return teamsMathRenderFont{}, fmt.Errorf("managed math font license is missing")
		}
	}
	if r.verifiedFonts == nil {
		r.verifiedFonts = make(map[string]teamsMathVerifiedFont)
	}
	r.verifiedFonts[path] = teamsMathVerifiedFont{Size: info.Size(), ModTime: info.ModTime()}
	if r.fontCmaps == nil {
		r.fontCmaps = make(map[string][]teamsMathCmapSubtable)
	}
	r.fontCmaps[path] = cmaps
	return teamsMathRenderFont{ID: asset.ID, Version: asset.Version, Path: path, Family: asset.Family}, nil
}

func downloadManagedMathFont(ctx context.Context, client teamsMathHTTPClient, asset teamsMathFontAsset, path string) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		_ = os.Remove(path)
		lastErr = downloadManagedMathFontOnce(ctx, client, asset, path)
		if lastErr == nil {
			return nil
		}
		if attempt == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return lastErr
}

func downloadManagedMathFontOnce(ctx context.Context, client teamsMathHTTPClient, asset teamsMathFontAsset, path string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, asset.URL, nil)
	if err != nil {
		return fmt.Errorf("create managed math font request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("download managed math font: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download managed math font: HTTP %s", response.Status)
	}
	if response.ContentLength > 0 && response.ContentLength != asset.Size {
		return fmt.Errorf("download managed math font: content length %d, want %d", response.ContentLength, asset.Size)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create managed math font download: %w", err)
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, asset.Size+1))
	if copyErr == nil {
		copyErr = file.Sync()
	}
	closeErr := file.Close()
	if copyErr != nil {
		return fmt.Errorf("write managed math font download: %w", copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close managed math font download: %w", closeErr)
	}
	if written != asset.Size {
		return fmt.Errorf("download managed math font: received %d bytes, want %d", written, asset.Size)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); !strings.EqualFold(got, asset.SHA256) {
		return fmt.Errorf("download managed math font: SHA-256 mismatch")
	}
	return nil
}

func (r *managedTeamsMathRenderer) missingManagedMathGlyphs(fonts []teamsMathRenderFont, texts []string) ([]rune, error) {
	needed := teamsMathVisibleRunes(texts)
	if len(needed) == 0 {
		return nil, nil
	}
	supported := make(map[rune]bool, len(needed))
	for _, font := range fonts {
		if strings.TrimSpace(font.Path) == "" {
			continue
		}
		coverage := r.fontGlyphCoverage[font.Path]
		if coverage == nil {
			coverage = make(map[rune]bool)
			if r.fontGlyphCoverage == nil {
				r.fontGlyphCoverage = make(map[string]map[rune]bool)
			}
			r.fontGlyphCoverage[font.Path] = coverage
		}
		unknown := make([]rune, 0, len(needed))
		for _, value := range needed {
			if supported[value] {
				continue
			}
			if valueSupported, ok := coverage[value]; ok {
				supported[value] = valueSupported
				continue
			}
			unknown = append(unknown, value)
		}
		if len(unknown) == 0 {
			continue
		}
		cmaps := r.fontCmaps[font.Path]
		if cmaps == nil {
			loaded, loadErr := loadTeamsMathFontCmaps(font.Path)
			if loadErr != nil {
				return nil, loadErr
			}
			if r.fontCmaps == nil {
				r.fontCmaps = make(map[string][]teamsMathCmapSubtable)
			}
			r.fontCmaps[font.Path] = loaded
			cmaps = loaded
		}
		fontCoverage := teamsMathCmapCoverage(cmaps, unknown)
		for value, valueSupported := range fontCoverage {
			coverage[value] = valueSupported
			if valueSupported {
				supported[value] = true
			}
		}
	}
	missing := make([]rune, 0, len(needed))
	for _, value := range needed {
		if !supported[value] {
			missing = append(missing, value)
		}
	}
	return missing, nil
}

func teamsMathVisibleRunes(texts []string) []rune {
	unique := make(map[rune]bool)
	for _, text := range texts {
		for _, value := range text {
			if teamsMathRuneNeedsGlyph(value) {
				unique[value] = true
			}
		}
	}
	values := make([]rune, 0, len(unique))
	for value := range unique {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func teamsMathRuneNeedsGlyph(value rune) bool {
	if unicode.IsSpace(value) || unicode.IsControl(value) || unicode.Is(unicode.Cf, value) {
		return false
	}
	return !(value >= 0xfe00 && value <= 0xfe0f) && !(value >= 0xe0100 && value <= 0xe01ef)
}

func mathFontRuneCoverage(path string, values []rune) (map[rune]bool, error) {
	cmaps, err := loadTeamsMathFontCmaps(path)
	if err != nil {
		return nil, err
	}
	return teamsMathCmapCoverage(cmaps, values), nil
}

func formatTeamsMathMissingGlyphError(missing []rune, coverageErr error, setupErr error) string {
	parts := []string{"math font coverage failed"}
	if len(missing) > 0 {
		parts = append(parts, "missing "+formatTeamsMathRunes(missing))
	}
	if coverageErr != nil {
		parts = append(parts, coverageErr.Error())
	}
	if setupErr != nil {
		parts = append(parts, setupErr.Error())
	}
	return strings.Join(parts, ": ")
}

func formatTeamsMathRunes(values []rune) string {
	if len(values) == 0 {
		return "no code points"
	}
	const limit = 8
	formatted := make([]string, 0, min(len(values), limit)+1)
	for index, value := range values {
		if index == limit {
			formatted = append(formatted, fmt.Sprintf("and %d more", len(values)-limit))
			break
		}
		formatted = append(formatted, fmt.Sprintf("U+%04X", value))
	}
	return strings.Join(formatted, ", ")
}
