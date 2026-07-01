package teams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

const (
	teamsMathRuntimeKind         = "codex-helper-teams-math-runtime"
	teamsMathRuntimeVersion      = "mathjax-4.1.2_resvg-js-2.6.2_v1"
	teamsMathInstallTimeout      = 2 * time.Minute
	teamsMathRenderTimeout       = 20 * time.Second
	maxTeamsMathPNGBytes         = 4 * 1024 * 1024
	maxTeamsMathPNGDimension     = 4096
	maxTeamsMathPNGPixelCount    = 8 * 1024 * 1024
	maxTeamsMathRenderOutput     = maxTeamsMathPerMessage*(maxTeamsMathPNGBytes*4/3+64*1024) + 64*1024
	maxTeamsMathDiagnosticOutput = 64 * 1024
	maxTeamsMathInstallOutput    = 512 * 1024
	maxTeamsMathPNGCacheBytes    = 128 * 1024 * 1024
	targetTeamsMathPNGCacheBytes = 96 * 1024 * 1024
)

//go:embed mathruntime/package.json mathruntime/package-lock.json mathruntime/renderer.mjs
var teamsMathRuntimeFiles embed.FS

type teamsMathPNGRenderer interface {
	Render(context.Context, []teamsMathSpan) []teamsMathAsset
}

type validatedTeamsMathPNG []byte

type managedTeamsMathRenderer struct {
	mu                  sync.Mutex
	cacheRoot           string
	lookPath            func(string) (string, error)
	pngCacheInitialized bool
	pngCacheEntries     map[string]teamsMathPNGCacheEntry
	pngCacheBytes       int64
	pngCacheScans       int
	maxPNGCacheBytes    int64
	targetPNGCacheBytes int64
	runtimeCleanupOnce  sync.Once
}

type teamsMathRuntimeMarker struct {
	Kind      string `json:"kind"`
	Version   string `json:"version"`
	Installed string `json:"installed_at"`
}

type teamsMathRenderRequest struct {
	Items []teamsMathRenderRequestItem `json:"items"`
}

type teamsMathRenderRequestItem struct {
	Index  int    `json:"index"`
	Source string `json:"source"`
}

type teamsMathRenderResponse struct {
	Results []teamsMathRenderResponseItem `json:"results"`
	Error   *teamsMathRuntimeError        `json:"error,omitempty"`
}

type teamsMathRenderResponseItem struct {
	Index  int                    `json:"index"`
	PNG    string                 `json:"png,omitempty"`
	Width  int                    `json:"width,omitempty"`
	Height int                    `json:"height,omitempty"`
	Error  *teamsMathRuntimeError `json:"error,omitempty"`
}

type teamsMathRuntimeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

var (
	defaultTeamsMathRendererOnce sync.Once
	defaultTeamsMathRenderer     teamsMathPNGRenderer
)

func teamsMathRenderer() teamsMathPNGRenderer {
	defaultTeamsMathRendererOnce.Do(func() {
		root, err := defaultTeamsMathCacheRoot()
		if err != nil {
			defaultTeamsMathRenderer = errorTeamsMathRenderer{err: err}
			return
		}
		defaultTeamsMathRenderer = &managedTeamsMathRenderer{cacheRoot: root, lookPath: exec.LookPath}
	})
	return defaultTeamsMathRenderer
}

type errorTeamsMathRenderer struct{ err error }

func (r errorTeamsMathRenderer) Render(_ context.Context, spans []teamsMathSpan) []teamsMathAsset {
	assets := make([]teamsMathAsset, 0, len(spans))
	for _, span := range spans {
		assets = append(assets, teamsMathAsset{Index: span.Index, Error: r.err.Error()})
	}
	return assets
}

func defaultTeamsMathCacheRoot() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams-math", teamsMathRuntimeVersion), nil
}

func (r *managedTeamsMathRenderer) Render(ctx context.Context, spans []teamsMathSpan) []teamsMathAsset {
	assets := make([]teamsMathAsset, len(spans))
	if len(spans) == 0 {
		return assets
	}
	for i, span := range spans {
		assets[i] = teamsMathAsset{Index: span.Index}
	}
	if len(spans) > maxTeamsMathPerMessage {
		setTeamsMathAssetErrors(assets, fmt.Errorf("too many formulas in one Teams message: %d", len(spans)))
		return assets
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	positions := make(map[string][]int, len(spans))
	unique := make([]teamsMathSpan, 0, len(spans))
	for i, span := range spans {
		if _, ok := positions[span.Source]; !ok {
			unique = append(unique, span)
		}
		positions[span.Source] = append(positions[span.Source], i)
	}
	missing := make([]teamsMathSpan, 0, len(unique))
	for _, span := range unique {
		if png, ok := r.cachedPNG(span.Source); ok {
			for _, position := range positions[span.Source] {
				assets[position].PNG = png
			}
			continue
		}
		missing = append(missing, span)
	}
	if len(missing) == 0 {
		return assets
	}

	runtimeRoot, nodePath, err := r.ensureRuntime(ctx)
	if err != nil {
		setTeamsMathAssetErrors(assets, err)
		return assets
	}
	request := teamsMathRenderRequest{Items: make([]teamsMathRenderRequestItem, 0, len(missing))}
	for _, span := range missing {
		request.Items = append(request.Items, teamsMathRenderRequestItem{Index: span.Index, Source: span.Source})
	}
	payload, err := json.Marshal(request)
	if err != nil {
		setTeamsMathAssetErrors(assets, err)
		return assets
	}
	renderCtx, cancel := context.WithTimeout(ctx, teamsMathRenderTimeout)
	defer cancel()
	cmd := exec.CommandContext(renderCtx, nodePath, filepath.Join(runtimeRoot, "renderer.mjs"))
	cmd.Dir = runtimeRoot
	cmd.Stdin = bytes.NewReader(payload)
	stdout := newTeamsMathBoundedBuffer(maxTeamsMathRenderOutput)
	stderr := newTeamsMathBoundedBuffer(maxTeamsMathDiagnosticOutput)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if renderCtx.Err() != nil {
			err = renderCtx.Err()
		} else if stderr.Len() > 0 {
			err = fmt.Errorf("math renderer failed: %w: %s", err, truncateTeamsMathError(stderr.String()))
		}
		setTeamsMathAssetErrors(assets, err)
		return assets
	}
	if stdout.Truncated() {
		setTeamsMathAssetErrors(assets, fmt.Errorf("math renderer response exceeded %d bytes", maxTeamsMathRenderOutput))
		return assets
	}
	var response teamsMathRenderResponse
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		setTeamsMathAssetErrors(assets, fmt.Errorf("decode math renderer response: %w", err))
		return assets
	}
	if response.Error != nil {
		setTeamsMathAssetErrors(assets, fmt.Errorf("%s: %s", response.Error.Code, response.Error.Message))
		return assets
	}
	byIndex := make(map[int]teamsMathRenderResponseItem, len(response.Results))
	for _, item := range response.Results {
		byIndex[item.Index] = item
	}
	for _, span := range missing {
		item, ok := byIndex[span.Index]
		if !ok {
			for _, position := range positions[span.Source] {
				assets[position].Error = "math renderer omitted result"
			}
			continue
		}
		if item.Error != nil {
			message := strings.TrimSpace(item.Error.Code + ": " + item.Error.Message)
			for _, position := range positions[span.Source] {
				assets[position].Error = message
			}
			continue
		}
		data, err := base64.StdEncoding.DecodeString(item.PNG)
		png, valid := validateTeamsMathPNG(data)
		if err != nil || !valid {
			for _, position := range positions[span.Source] {
				assets[position].Error = "math renderer returned invalid PNG"
			}
			continue
		}
		for _, position := range positions[span.Source] {
			assets[position].PNG = png
		}
		_ = r.storeCachedPNG(span.Source, png)
	}
	return assets
}

func (r *managedTeamsMathRenderer) ensureRuntime(ctx context.Context) (string, string, error) {
	root := strings.TrimSpace(r.cacheRoot)
	if root == "" {
		return "", "", fmt.Errorf("math runtime cache root is empty")
	}
	lookPath := r.lookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	nodePath, err := lookPath("node")
	if err != nil {
		return "", "", fmt.Errorf("Node.js is required for Teams math rendering: %w", err)
	}
	if teamsMathRuntimeReady(root) {
		r.cleanupObsoleteRuntimeVersions(root)
		return root, nodePath, nil
	}
	npmPath, err := lookPath("npm")
	if err != nil {
		return "", "", fmt.Errorf("npm is required for on-demand Teams math setup: %w", err)
	}
	parent := filepath.Dir(root)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", "", err
	}
	lock := flock.New(filepath.Join(parent, "install.lock"))
	locked, err := lock.TryLockContext(ctx, 250*time.Millisecond)
	if err != nil {
		return "", "", err
	}
	if !locked {
		return "", "", fmt.Errorf("Teams math runtime setup is already in progress")
	}
	defer func() { _ = lock.Unlock() }()
	if teamsMathRuntimeReady(root) {
		return root, nodePath, nil
	}
	stage, err := os.MkdirTemp(parent, ".install-")
	if err != nil {
		return "", "", err
	}
	defer os.RemoveAll(stage)
	for _, name := range []string{"package.json", "package-lock.json", "renderer.mjs"} {
		data, readErr := teamsMathRuntimeFiles.ReadFile("mathruntime/" + name)
		if readErr != nil {
			return "", "", readErr
		}
		if writeErr := os.WriteFile(filepath.Join(stage, name), data, 0o600); writeErr != nil {
			return "", "", writeErr
		}
	}
	installCtx, cancel := context.WithTimeout(ctx, teamsMathInstallTimeout)
	defer cancel()
	cmd := exec.CommandContext(installCtx, npmPath, "ci", "--ignore-scripts", "--no-audit", "--no-fund", "--include=optional")
	cmd.Dir = stage
	installOutput := newTeamsMathBoundedBuffer(maxTeamsMathInstallOutput)
	cmd.Stdout = installOutput
	cmd.Stderr = installOutput
	if err := cmd.Run(); err != nil {
		if installCtx.Err() != nil {
			err = installCtx.Err()
		}
		return "", "", fmt.Errorf("install Teams math runtime: %w: %s", err, truncateTeamsMathError(installOutput.String()))
	}
	marker, _ := json.Marshal(teamsMathRuntimeMarker{Kind: teamsMathRuntimeKind, Version: teamsMathRuntimeVersion, Installed: time.Now().UTC().Format(time.RFC3339)})
	if err := os.WriteFile(filepath.Join(stage, "runtime.json"), marker, 0o600); err != nil {
		return "", "", err
	}
	if err := os.RemoveAll(root); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	if err := os.Rename(stage, root); err != nil {
		return "", "", err
	}
	if !teamsMathRuntimeReady(root) {
		return "", "", fmt.Errorf("Teams math runtime installation did not pass validation")
	}
	r.cleanupObsoleteRuntimeVersions(root)
	return root, nodePath, nil
}

func (r *managedTeamsMathRenderer) cleanupObsoleteRuntimeVersions(currentRoot string) {
	r.runtimeCleanupOnce.Do(func() {
		parent := filepath.Dir(filepath.Clean(currentRoot))
		lock := flock.New(filepath.Join(parent, "cleanup.lock"))
		locked, err := lock.TryLock()
		if err != nil || !locked {
			return
		}
		defer func() { _ = lock.Unlock() }()
		_, _ = cleanupObsoleteTeamsMathRuntimes(parent, currentRoot, 1)
	})
}

type teamsMathRuntimeCandidate struct {
	path      string
	installed time.Time
	modTime   time.Time
}

func cleanupObsoleteTeamsMathRuntimes(parent string, currentRoot string, keepPrevious int) ([]string, error) {
	children, err := os.ReadDir(parent)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	currentRoot = filepath.Clean(currentRoot)
	candidates := make([]teamsMathRuntimeCandidate, 0, len(children))
	for _, child := range children {
		if !child.IsDir() || strings.HasPrefix(child.Name(), ".") {
			continue
		}
		path := filepath.Join(parent, child.Name())
		if filepath.Clean(path) == currentRoot {
			continue
		}
		markerData, readErr := os.ReadFile(filepath.Join(path, "runtime.json"))
		if readErr != nil {
			continue
		}
		var marker teamsMathRuntimeMarker
		if json.Unmarshal(markerData, &marker) != nil || marker.Kind != teamsMathRuntimeKind || strings.TrimSpace(marker.Version) == "" {
			continue
		}
		info, infoErr := child.Info()
		if infoErr != nil {
			continue
		}
		installed, _ := time.Parse(time.RFC3339, strings.TrimSpace(marker.Installed))
		candidates = append(candidates, teamsMathRuntimeCandidate{path: path, installed: installed, modTime: info.ModTime()})
	}
	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i].installed
		right := candidates[j].installed
		if left.IsZero() {
			left = candidates[i].modTime
		}
		if right.IsZero() {
			right = candidates[j].modTime
		}
		if left.Equal(right) {
			return candidates[i].path > candidates[j].path
		}
		return left.After(right)
	})
	if keepPrevious < 0 {
		keepPrevious = 0
	}
	if keepPrevious > len(candidates) {
		keepPrevious = len(candidates)
	}
	removed := make([]string, 0, len(candidates))
	for _, candidate := range candidates[keepPrevious:] {
		if err := os.RemoveAll(candidate.path); err != nil {
			return removed, err
		}
		removed = append(removed, candidate.path)
	}
	return removed, nil
}

func teamsMathRuntimeReady(root string) bool {
	markerData, err := os.ReadFile(filepath.Join(root, "runtime.json"))
	if err != nil {
		return false
	}
	var marker teamsMathRuntimeMarker
	if json.Unmarshal(markerData, &marker) != nil || marker.Kind != teamsMathRuntimeKind || marker.Version != teamsMathRuntimeVersion {
		return false
	}
	for _, path := range []string{
		filepath.Join(root, "renderer.mjs"),
		filepath.Join(root, "node_modules", "mathjax", "node-main.mjs"),
		filepath.Join(root, "node_modules", "@resvg", "resvg-js", "index.js"),
	} {
		if info, err := os.Stat(path); err != nil || info.IsDir() {
			return false
		}
	}
	return true
}

func (r *managedTeamsMathRenderer) cachedPNG(source string) (validatedTeamsMathPNG, bool) {
	path := filepath.Join(r.cacheRoot, "png", teamsMathCacheKey(source)+".png")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	return validateTeamsMathPNG(data)
}

func (r *managedTeamsMathRenderer) storeCachedPNG(source string, png validatedTeamsMathPNG) error {
	if !plausibleTeamsMathPNG(png) {
		return fmt.Errorf("invalid math PNG")
	}
	dir := filepath.Join(r.cacheRoot, "png")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, teamsMathCacheKey(source)+".png")
	tmp, err := os.CreateTemp(dir, ".png-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(png); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		if existing, readErr := os.ReadFile(path); readErr == nil {
			if _, valid := validateTeamsMathPNG(existing); valid {
				return r.recordExistingCachedPNG(dir, path)
			}
		}
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return err
		}
		if retryErr := os.Rename(tmpName, path); retryErr != nil {
			if existing, readErr := os.ReadFile(path); readErr == nil {
				if _, valid := validateTeamsMathPNG(existing); valid {
					return r.recordExistingCachedPNG(dir, path)
				}
			}
			return retryErr
		}
	}
	return r.recordExistingCachedPNG(dir, path)
}

func (r *managedTeamsMathRenderer) recordExistingCachedPNG(dir string, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return r.recordCachedPNG(dir, path, info.Size(), info.ModTime())
}

func (r *managedTeamsMathRenderer) recordCachedPNG(dir string, path string, size int64, modTime time.Time) error {
	if !r.pngCacheInitialized {
		entries, total, err := scanTeamsMathPNGCache(dir)
		if err != nil {
			return err
		}
		r.pngCacheEntries = entries
		r.pngCacheBytes = total
		r.pngCacheInitialized = true
		r.pngCacheScans++
	} else {
		if existing, ok := r.pngCacheEntries[path]; ok {
			r.pngCacheBytes -= existing.size
		}
		r.pngCacheEntries[path] = teamsMathPNGCacheEntry{path: path, size: size, modTime: modTime}
		r.pngCacheBytes += size
	}
	maxBytes, targetBytes := r.pngCacheLimits()
	if r.pngCacheBytes <= maxBytes {
		return nil
	}
	return r.pruneIndexedTeamsMathPNGCache(targetBytes)
}

func (r *managedTeamsMathRenderer) pngCacheLimits() (int64, int64) {
	maxBytes := r.maxPNGCacheBytes
	if maxBytes <= 0 {
		maxBytes = maxTeamsMathPNGCacheBytes
	}
	targetBytes := r.targetPNGCacheBytes
	if targetBytes <= 0 && maxBytes == maxTeamsMathPNGCacheBytes {
		targetBytes = targetTeamsMathPNGCacheBytes
	}
	if targetBytes <= 0 || targetBytes >= maxBytes {
		targetBytes = maxBytes * 3 / 4
	}
	return maxBytes, targetBytes
}

func (r *managedTeamsMathRenderer) pruneIndexedTeamsMathPNGCache(target int64) error {
	files := make([]teamsMathPNGCacheEntry, 0, len(r.pngCacheEntries))
	for _, file := range r.pngCacheEntries {
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].modTime.Equal(files[j].modTime) {
			return files[i].path < files[j].path
		}
		return files[i].modTime.Before(files[j].modTime)
	})
	for _, file := range files {
		if r.pngCacheBytes <= target {
			return nil
		}
		if err := os.Remove(file.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		delete(r.pngCacheEntries, file.path)
		r.pngCacheBytes -= file.size
	}
	return nil
}

type teamsMathPNGCacheEntry struct {
	path    string
	size    int64
	modTime time.Time
}

func scanTeamsMathPNGCache(dir string) (map[string]teamsMathPNGCacheEntry, int64, error) {
	children, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]teamsMathPNGCacheEntry{}, 0, nil
		}
		return nil, 0, err
	}
	entries := make(map[string]teamsMathPNGCacheEntry, len(children))
	var total int64
	for _, child := range children {
		if child.IsDir() || !isTeamsMathCacheFilename(child.Name()) {
			continue
		}
		info, infoErr := child.Info()
		if infoErr != nil || !info.Mode().IsRegular() {
			continue
		}
		path := filepath.Join(dir, child.Name())
		entries[path] = teamsMathPNGCacheEntry{path: path, size: info.Size(), modTime: info.ModTime()}
		total += info.Size()
	}
	return entries, total, nil
}

func isTeamsMathCacheFilename(name string) bool {
	if len(name) != 64+len(".png") || !strings.HasSuffix(name, ".png") {
		return false
	}
	for _, c := range name[:64] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func teamsMathCacheKey(source string) string {
	sum := sha256.Sum256([]byte(teamsMathRuntimeVersion + "\x00" + source))
	return hex.EncodeToString(sum[:])
}

func plausibleTeamsMathPNG(data []byte) bool {
	if len(data) < 8 || len(data) > maxTeamsMathPNGBytes || !bytes.Equal(data[:8], []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}) {
		return false
	}
	return true
}

func validateTeamsMathPNG(data []byte) (validatedTeamsMathPNG, bool) {
	if !plausibleTeamsMathPNG(data) {
		return nil, false
	}
	config, err := png.DecodeConfig(bytes.NewReader(data))
	if err != nil || config.Width <= 0 || config.Height <= 0 || config.Width > maxTeamsMathPNGDimension || config.Height > maxTeamsMathPNGDimension || int64(config.Width)*int64(config.Height) > maxTeamsMathPNGPixelCount {
		return nil, false
	}
	_, err = png.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, false
	}
	return validatedTeamsMathPNG(data), true
}

type teamsMathBoundedBuffer struct {
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func newTeamsMathBoundedBuffer(limit int) *teamsMathBoundedBuffer {
	return &teamsMathBoundedBuffer{limit: limit}
}

func (b *teamsMathBoundedBuffer) Write(data []byte) (int, error) {
	written := len(data)
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		if remaining > len(data) {
			remaining = len(data)
		}
		_, _ = b.buffer.Write(data[:remaining])
	}
	if remaining < len(data) {
		b.truncated = true
	}
	return written, nil
}

func (b *teamsMathBoundedBuffer) Bytes() []byte   { return b.buffer.Bytes() }
func (b *teamsMathBoundedBuffer) String() string  { return b.buffer.String() }
func (b *teamsMathBoundedBuffer) Len() int        { return b.buffer.Len() }
func (b *teamsMathBoundedBuffer) Truncated() bool { return b.truncated }

func validTeamsMathPNG(data []byte) bool {
	_, valid := validateTeamsMathPNG(data)
	return valid
}

func setTeamsMathAssetErrors(assets []teamsMathAsset, err error) {
	message := "math rendering unavailable"
	if err != nil {
		message = truncateTeamsMathError(err.Error())
	}
	for i := range assets {
		if len(assets[i].PNG) == 0 && assets[i].Error == "" {
			assets[i].Error = message
		}
	}
}

func truncateTeamsMathError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 500 {
		return value[:500]
	}
	return value
}
