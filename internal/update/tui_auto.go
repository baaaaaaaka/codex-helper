package update

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
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
	"github.com/gofrs/flock"
	"golang.org/x/mod/semver"
)

const TUIManifestName = "tui-update.json"
const maxTUIAssetSize = 256 << 20
const tuiDiskHeadroom = 64 << 20

var tuiDiskFreeBytes = platformTUIDiskFreeBytes

func checkTUIDiskSpace(root string, needed int64) error {
	available, err := tuiDiskFreeBytes(root)
	if err != nil {
		return fmt.Errorf("check update disk space: %w", err)
	}
	if available < uint64(needed)+tuiDiskHeadroom {
		return fmt.Errorf("not enough free space for update")
	}
	return nil
}

type TUIManifest struct {
	Schema      int                 `json:"schema"`
	Repo        string              `json:"repo"`
	Version     string              `json:"version"`
	Protocol    int                 `json:"protocol"`
	Priority    AutoUpdatePriority  `json:"priority"`
	PublishedAt time.Time           `json:"published_at"`
	Assets      map[string]TUIAsset `json:"assets"`
}

type TUIAsset struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// The manifest and binary use GitHub's authenticated HTTPS publishing path.
// This is the same publisher trust as the installer, with the additional
// post-CI gate and mandatory digest binding. It is not an offline signature.
func VerifyTUIManifest(raw []byte, repo string, now time.Time) (TUIManifest, error) {
	var m TUIManifest
	if len(raw) > 128<<10 {
		return m, fmt.Errorf("update manifest too large")
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	v, valid := helperruntime.NormalizeVersion(m.Version)
	if m.Schema != 1 || m.Protocol != helperruntime.BootProtocol || !strings.EqualFold(m.Repo, repo) || !valid || v != m.Version || semver.Prerelease(v) != "" || m.PublishedAt.IsZero() || m.PublishedAt.After(now) {
		return TUIManifest{}, fmt.Errorf("update manifest is incompatible")
	}
	return m, nil
}

type tuiCheckState = updatestate.Check

type tuiRateLimit struct {
	next   time.Time
	status int
}

func (e *tuiRateLimit) Error() string { return fmt.Sprintf("update request: HTTP %d", e.status) }

// CheckAndStageTUI runs only in the outer interactive process, never the draw
// loop. A shared nonblocking lock and durable cooldown cover all open windows.
// The post-CI manifest is the only source: no Release API fallback.
func CheckAndStageTUI(ctx context.Context, current helperruntime.Context) error {
	return checkAndStageTUI(ctx, current, "", time.Now())
}

func checkAndStageTUI(ctx context.Context, current helperruntime.Context, endpoint string, now time.Time) error {
	l := flock.New(filepath.Join(current.Root, "auto-update.lock"))
	locked, err := l.TryLock()
	if err != nil || !locked {
		return err
	}
	defer l.Unlock()
	store, err := updatestate.Open(current.Root)
	if err != nil {
		return err
	}
	defer store.Close()
	state, err := store.Check()
	if err != nil {
		return err
	}
	if now.Before(state.Next) && state.Next.Before(now.Add(25*time.Hour)) {
		return nil
	}
	base, generation, err := helperruntime.BootSnapshot(current.Root)
	if err != nil {
		return err
	}
	// Persist the cooldown before sending any request. A read-only/full state
	// directory must not turn many old windows into a retry storm.
	state.Next = now.Add(DefaultAutoUpdateCheckInterval)
	save := func() error { return store.SaveCheck(state) }
	if err := save(); err != nil {
		return err
	}
	err = stageTUIRelease(ctx, current, base, generation, endpoint, now)
	if err != nil {
		state.Failures++
		if state.Failures > 6 {
			state.Failures = 6
		}
		delay := DefaultAutoUpdateCheckInterval * time.Duration(1<<uint(state.Failures-1))
		if delay > 24*time.Hour {
			delay = 24 * time.Hour
		}
		state.Next = now.Add(delay)
		var rate *tuiRateLimit
		if errors.As(err, &rate) && rate.next.After(state.Next) {
			state.Next = rate.next
		}
		state.LastError = err.Error()
		if len(state.LastError) > 300 {
			state.LastError = state.LastError[:300]
		}
	} else {
		state.Failures, state.LastError = 0, ""
	}
	_ = save()
	return err
}

func stageTUIRelease(ctx context.Context, current helperruntime.Context, base string, generation uint64, endpoint string, now time.Time) error {
	repo := ResolveRepo("")
	if endpoint == "" {
		endpoint = fmt.Sprintf("%s/%s/auto-update-index/%s", strings.TrimRight(githubRawBase, "/"), repo, TUIManifestName)
	}
	client := &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("too many update redirects")
		}
		if len(via) > 0 && via[0].URL.Scheme == "https" && req.URL.Scheme != "https" {
			return fmt.Errorf("insecure update redirect")
		}
		return nil
	}}
	request := func(url string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "cxp-tui-update")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
				next, _ := http.ParseTime(resp.Header.Get("Retry-After"))
				if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 && seconds <= 86400 {
					next = now.Add(time.Duration(seconds) * time.Second)
				}
				if next.After(now.Add(24 * time.Hour)) {
					next = now.Add(24 * time.Hour)
				}
				return nil, &tuiRateLimit{next, resp.StatusCode}
			}
			return nil, fmt.Errorf("update request: HTTP %d", resp.StatusCode)
		}
		return resp, nil
	}
	resp, err := request(endpoint)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (128<<10)+1))
	_ = resp.Body.Close()
	if err != nil {
		return err
	}
	m, err := VerifyTUIManifest(raw, repo, now)
	if err != nil {
		return err
	}
	eligible, ok := autoUpdateEligibleAt(m.Priority, m.PublishedAt, false)
	if !ok || now.Before(eligible) || semver.Compare(m.Version, base) <= 0 {
		return nil
	}
	if boot, err := helperruntime.ReadBoot(current.Root); err == nil && boot.Candidate != nil && semver.Compare(m.Version, boot.Candidate.Version) <= 0 {
		return nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	asset, ok := m.Assets[runtime.GOOS+"/"+runtime.GOARCH]
	expectedName, nameErr := assetName(strings.TrimPrefix(m.Version, "v"), runtime.GOOS, runtime.GOARCH)
	hash, hashErr := hex.DecodeString(asset.SHA256)
	if !ok || nameErr != nil || asset.Name != expectedName || hashErr != nil || len(hash) != 32 || asset.Size <= 0 || asset.Size > maxTUIAssetSize {
		return fmt.Errorf("update asset invalid or unsupported")
	}
	if err := helperruntime.CheckAutoStorage(current.Root, m.Version); err != nil {
		return err
	}
	// One owned cache path, under auto-update.lock. A killed download cannot
	// accumulate a new partial file every time a window retries.
	path := filepath.Join(current.Root, ".tui-download")
	if runtime.GOOS == "windows" {
		path += ".exe"
	}
	if info, statErr := os.Lstat(path); statErr == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("invalid update cache")
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	// Keep room for both the download and immutable installed copy, plus a
	// small reserve for live sessions. This is a best-effort preflight, not a
	// reservation against other processes writing to the same filesystem.
	if err := checkTUIDiskSpace(current.Root, 2*asset.Size); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer os.Remove(path)
	defer f.Close()
	resp, err = request(buildReleaseURL(repo, m.Version, asset.Name))
	if err != nil {
		return err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), io.LimitReader(resp.Body, asset.Size+1))
	_ = resp.Body.Close()
	if copyErr != nil {
		return copyErr
	}
	if n != asset.Size || !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), asset.SHA256) {
		return fmt.Errorf("update asset size or digest mismatch")
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0755); err != nil {
			return err
		}
	}
	if err := validateDownloadedBinary(ctx, path, strings.TrimPrefix(m.Version, "v"), 10*time.Second); err != nil {
		return err
	}
	if err := helperruntime.ProbeProtocol(ctx, path); err != nil {
		return err
	}
	if err := helperruntime.ProbeProtocol(ctx, current.EntryPath); err != nil {
		return fmt.Errorf("safe boot requires a current installed launcher: %w", err)
	}
	if _, err := helperruntime.ReadBoot(current.Root); err != nil {
		return err
	}
	if err := checkTUIDiskSpace(current.Root, asset.Size); err != nil {
		return err
	}
	return helperruntime.StageBootCandidate(ctx, current, base, generation, path, m.Version, asset.SHA256)
}
