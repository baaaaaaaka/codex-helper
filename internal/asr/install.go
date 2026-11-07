package asr

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

// Cache layout (safe to share between hosts, e.g. an NFS home):
//
//	<root>/<asset>-<sha12>/            one verified install per pinned asset
//	<root>/<asset>-<sha12>/.asr-install.json   manifest, written before publish
//	<root>/tmp/                        downloads, staging dirs, quarantined installs
//	<root>/locks/                      advisory locks (best effort; NFS may lack them)
//
// An install directory only ever appears through one rename of a fully
// verified staging directory, so readers never see a partial install, and
// two hosts racing on the same asset both end up with the same content.

const (
	manifestName   = ".asr-install.json"
	hashOnUseLimit = 128 << 20 // files up to this size are re-hashed on first use per process
	staleTempAge   = 24 * time.Hour
	maxExtract     = 4 << 30
	downloadTries  = 3
)

type installManifest struct {
	Asset     string         `json:"asset"`
	SHA256    string         `json:"sha256"`
	Installed string         `json:"installed"`
	Files     []manifestFile `json:"files"`
}

type manifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type installer struct {
	root     string
	client   *http.Client
	backoff  time.Duration
	stall    time.Duration // abort a download attempt that receives nothing for this long
	progress func()        // optional; called when bytes arrive or an install step finishes
	noLock   bool          // tests: behave like an NFS mount without working locks
	mu       sync.Mutex
	verified map[string]bool // install dirs verified by this process
}

func newInstaller(root string, client *http.Client) *installer {
	if client == nil {
		client = http.DefaultClient
	}
	return &installer{root: root, client: client, backoff: time.Second, stall: time.Minute, verified: map[string]bool{}}
}

// installed reports whether a verified install of a exists (no download).
func (in *installer) installed(a Asset) bool {
	dir := filepath.Join(in.root, a.dirName())
	in.mu.Lock()
	ok := in.verified[dir]
	in.mu.Unlock()
	if ok {
		return true
	}
	if verifyInstall(dir, a) != nil {
		return false
	}
	in.markVerified(dir)
	return true
}

// forget drops the in-memory verification of a, so the next ensure re-checks
// it (used when the runtime reports that an installed file is unusable).
func (in *installer) forget(a Asset, quarantine bool) {
	dir := filepath.Join(in.root, a.dirName())
	in.mu.Lock()
	delete(in.verified, dir)
	in.mu.Unlock()
	if quarantine {
		in.quarantine(dir)
	}
}

// ensure returns the directory of a verified install of a, downloading and
// publishing it first if needed. validate (optional) runs on the staging
// directory before publish; a failing install is never published.
func (in *installer) ensure(ctx context.Context, a Asset, validate func(dir string) error) (string, error) {
	dir := filepath.Join(in.root, a.dirName())
	if in.installed(a) {
		return dir, nil
	}
	in.quarantine(dir) // half-written, corrupted or from an older layout (no-op if absent)

	unlock := in.lock(ctx, a.dirName())
	defer unlock()
	if verifyInstall(dir, a) == nil { // another process/host finished meanwhile
		return in.markVerified(dir), nil
	}
	if err := ctx.Err(); err != nil {
		return "", ctxError(ctx, "install "+a.Name)
	}

	file, err := in.download(ctx, a)
	if err != nil {
		return "", err
	}
	defer os.Remove(file)
	stage, err := os.MkdirTemp(in.tmp(), "stage-")
	if err != nil {
		return "", newError(KindCache, "create staging dir", err, "")
	}
	defer os.RemoveAll(stage)
	if err := unpack(file, a, stage); err != nil {
		return "", newError(KindIntegrity, "unpack "+a.Name, err, "")
	}
	if validate != nil {
		if err := validate(stage); err != nil {
			return "", err
		}
	}
	in.report()
	if err := writeManifest(stage, a); err != nil {
		return "", newError(KindCache, "write install manifest", err, "")
	}
	if err := publish(stage, dir); err != nil {
		if verifyInstall(dir, a) == nil { // lost a race to an identical install
			return in.markVerified(dir), nil
		}
		return "", newError(KindCache, "publish "+a.Name, err, "")
	}
	return in.markVerified(dir), nil
}

func (in *installer) markVerified(dir string) string {
	in.mu.Lock()
	in.verified[dir] = true
	in.mu.Unlock()
	return dir
}

func (in *installer) tmp() string { return filepath.Join(in.root, "tmp") }

// quarantine moves a broken install aside atomically (so concurrent readers
// see either the old directory or none) and deletes it best effort.
func (in *installer) quarantine(dir string) {
	if _, err := os.Lstat(dir); err != nil {
		return
	}
	aside := filepath.Join(in.tmp(), "broken-"+filepath.Base(dir)+"-"+randomHex(4))
	if os.Rename(dir, aside) == nil {
		_ = os.RemoveAll(aside) // may fail on Windows while files are open; cleanTemp retries later
	}
}

// lock takes the per-asset advisory lock, waiting until ctx ends. Lock
// errors (e.g. NFS without lock support) are ignored: correctness does not
// depend on the lock, it only avoids duplicate downloads.
func (in *installer) lock(ctx context.Context, name string) func() {
	dir := filepath.Join(in.root, "locks")
	if in.noLock || os.MkdirAll(dir, 0o700) != nil {
		return func() {}
	}
	l := flock.New(filepath.Join(dir, name+".lock"))
	ok, err := l.TryLockContext(ctx, 250*time.Millisecond)
	if err != nil || !ok {
		return func() {}
	}
	return func() { _ = l.Unlock() }
}

// cleanTemp removes leftovers (interrupted downloads, staging dirs,
// quarantined installs) older than staleTempAge.
func (in *installer) cleanTemp(now time.Time) {
	entries, _ := os.ReadDir(in.tmp())
	for _, e := range entries {
		if info, err := e.Info(); err == nil && now.Sub(info.ModTime()) > staleTempAge {
			_ = os.RemoveAll(filepath.Join(in.tmp(), e.Name()))
		}
	}
}

// download fetches a into a temp file and verifies size and SHA-256. Each
// URL (a.URL, then the mirrors) gets downloadTries attempts; a missing file
// (HTTP 4xx) or wrong bytes move on to the next URL at once. The result is
// permanent only when every URL failed permanently.
func (in *installer) download(ctx context.Context, a Asset) (string, error) {
	if err := os.MkdirAll(in.tmp(), 0o700); err != nil {
		return "", newError(KindCache, "create cache dir", err, "")
	}
	var lastErr, transient *Error
	for _, url := range a.urls() {
		for attempt := 1; attempt <= downloadTries; attempt++ {
			file, err := in.downloadOnce(ctx, a, url)
			if err == nil {
				return file, nil
			}
			lastErr = err
			if err.Kind != KindDownload || errors.Is(err, errPermanent) || ctx.Err() != nil {
				break
			}
			transient = err
			select {
			case <-ctx.Done():
			case <-time.After(in.backoff * time.Duration(attempt)):
			}
		}
		if ctx.Err() != nil {
			return "", ctxError(ctx, "download "+a.Name)
		}
	}
	if lastErr.Kind == KindDownload && transient != nil {
		return "", transient // one URL may work later: keep the error retryable
	}
	return "", lastErr
}

func (in *installer) downloadOnce(parent context.Context, a Asset, url string) (string, *Error) {
	op := "download " + a.Name
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stalled := time.AfterFunc(in.stall, cancel) // reset on every read below
	defer stalled.Stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", newError(KindDownload, op, err, "")
	}
	req.Header.Set("User-Agent", "codex-helper-asr")
	resp, err := in.client.Do(req)
	if err != nil {
		if parent.Err() == nil && ctx.Err() != nil {
			return "", newError(KindDownload, op, err, fmt.Sprintf("no response for %s", in.stall))
		}
		return "", newError(KindDownload, op, err, "")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests {
			err = fmt.Errorf("%w: %w", errPermanent, err) // the pinned file is missing or forbidden: retrying will not help
		}
		return "", newError(KindDownload, op, err, "")
	}
	f, err := os.CreateTemp(in.tmp(), "dl-")
	if err != nil {
		return "", newError(KindCache, op, err, "")
	}
	h := sha256.New()
	body := &stallReader{r: resp.Body, t: stalled, d: in.stall, on: in.report}
	n, copyErr := io.Copy(io.MultiWriter(f, h), io.LimitReader(body, a.Size+1))
	closeErr := f.Close()
	fail := func(e *Error) (string, *Error) { _ = os.Remove(f.Name()); return "", e }
	switch {
	case copyErr != nil && parent.Err() == nil && ctx.Err() != nil:
		return fail(newError(KindDownload, op, copyErr, fmt.Sprintf("no data received for %s", in.stall)))
	case copyErr != nil:
		return fail(newError(KindDownload, op, copyErr, ""))
	case closeErr != nil:
		return fail(newError(KindCache, op, closeErr, ""))
	case n != a.Size:
		return fail(newError(KindIntegrity, op, fmt.Errorf("got %d bytes, pinned size is %d", n, a.Size), ""))
	}
	if got := hex.EncodeToString(h.Sum(nil)); !strings.EqualFold(got, a.SHA256) {
		return fail(newError(KindIntegrity, op, fmt.Errorf("sha256 %s, pinned %s", got, a.SHA256), ""))
	}
	return f.Name(), nil
}

// errPermanent marks download failures that retrying cannot fix (HTTP 4xx).
var errPermanent = errors.New("permanent download failure")

func (in *installer) report() {
	if in.progress != nil {
		in.progress()
	}
}

// stallReader re-arms the stall timer and reports progress whenever bytes
// arrive.
type stallReader struct {
	r  io.Reader
	t  *time.Timer
	d  time.Duration
	on func()
}

func (s *stallReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if n > 0 {
		s.t.Reset(s.d)
		s.on()
	}
	return n, err
}

// verifyInstall checks dir against its manifest and the pin: every file is
// present with the recorded size, and files up to hashOnUseLimit (the
// binaries) are re-hashed. Large model files were hashed at download time;
// a corrupted model is caught when llama-server fails to load it.
func verifyInstall(dir string, a Asset) error {
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		return err
	}
	var m installManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("install manifest unreadable: %w", err)
	}
	if !strings.EqualFold(m.SHA256, a.SHA256) || len(m.Files) == 0 {
		return errors.New("install manifest does not match the pinned asset")
	}
	for _, f := range m.Files {
		p := filepath.Join(dir, filepath.FromSlash(f.Path))
		info, err := os.Stat(p)
		if err != nil {
			return err
		}
		if info.Size() != f.Size {
			return fmt.Errorf("%s: size %d, manifest %d", f.Path, info.Size(), f.Size)
		}
		if f.Size <= hashOnUseLimit {
			if sum, err := fileSHA256(p); err != nil || sum != f.SHA256 {
				return fmt.Errorf("%s: checksum mismatch", f.Path)
			}
		}
	}
	return nil
}

func writeManifest(stage string, a Asset) error {
	m := installManifest{Asset: a.Name, SHA256: a.SHA256, Installed: time.Now().UTC().Format(time.RFC3339)}
	err := filepath.WalkDir(stage, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(stage, p)
		sum := a.SHA256 // a FormatFile asset was hashed while downloading
		if a.Format != FormatFile || rel != a.Name {
			if sum, err = fileSHA256(p); err != nil {
				return err
			}
		}
		m.Files = append(m.Files, manifestFile{Path: filepath.ToSlash(rel), Size: info.Size(), SHA256: sum})
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	data, _ := json.MarshalIndent(m, "", " ")
	return writeFile(filepath.Join(stage, manifestName), bytes.NewReader(data), 0o644)
}

// publish renames stage to dir. A rename onto an existing directory fails,
// which is how a lost race is detected. Windows scanners briefly lock fresh
// files, so sharing violations are retried.
func publish(stage, dir string) error {
	var err error
	for i := 0; i < 8; i++ {
		if err = os.Rename(stage, dir); err == nil {
			syncDir(filepath.Dir(dir))
			return nil
		}
		if _, statErr := os.Stat(dir); statErr == nil || runtime.GOOS != "windows" {
			return err
		}
		time.Sleep(time.Duration(i+1) * 250 * time.Millisecond)
	}
	return err
}

// unpack places the downloaded file into stage according to a.Format.
func unpack(file string, a Asset, stage string) error {
	switch a.Format {
	case FormatFile: // same filesystem: move, don't copy, the (large) model file
		return os.Rename(file, filepath.Join(stage, a.Name))
	case FormatTarGz:
		return untar(file, stage)
	}
	return fmt.Errorf("unknown asset format %q", a.Format)
}

// stripTop removes the single top-level directory of an archive entry and
// rejects entries that would escape the destination.
func stripTop(name string) (string, error) {
	name = path.Clean(strings.ReplaceAll(name, "\\", "/"))
	if strings.HasPrefix(name, "/") || name == ".." || strings.HasPrefix(name, "../") {
		return "", fmt.Errorf("unsafe archive path %q", name)
	}
	_, rest, ok := strings.Cut(name, "/")
	if !ok {
		return "", nil // the top-level directory itself
	}
	return rest, nil
}

func untar(file, stage string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	tr := tar.NewReader(gz)
	var total int64
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rel, err := stripTop(h.Name)
		if err != nil {
			return err
		}
		if rel == "" || h.Typeflag != tar.TypeReg {
			continue // directories are implied; links are never shipped
		}
		if total += h.Size; total > maxExtract {
			return errors.New("archive too large")
		}
		if err := writeFile(filepath.Join(stage, filepath.FromSlash(rel)), tr, os.FileMode(h.Mode)&0o755|0o600); err != nil {
			return err
		}
	}
}

func writeFile(p string, r io.Reader, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, io.LimitReader(r, maxExtract))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}

func fileSHA256(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func exeName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}
