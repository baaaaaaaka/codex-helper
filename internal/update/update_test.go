package update

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestCheckForUpdateAvailable(t *testing.T) {
	requireRuntimeAsset(t)
	tag := "v1.2.3"
	ver := "1.2.3"
	asset, err := assetName(ver, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName error: %v", err)
	}

	server := newReleaseServer(t, tag, asset, []byte("payload"))
	defer server.Close()

	restore := overrideGitHubBases(server.URL)
	defer restore()

	st := CheckForUpdate(context.Background(), CheckOptions{
		Repo:             "owner/name",
		InstalledVersion: "1.0.0",
		Timeout:          time.Second,
	})

	if !st.Supported {
		t.Fatalf("expected supported update check, got error=%q", st.Error)
	}
	if !st.UpdateAvailable {
		t.Fatalf("expected update available")
	}
	if st.RemoteVersion != ver {
		t.Fatalf("expected remote version %s, got %s", ver, st.RemoteVersion)
	}
	if st.RemoteTag != tag {
		t.Fatalf("expected remote tag %s, got %s", tag, st.RemoteTag)
	}
}

func TestCheckForUpdateFallbackRedirect(t *testing.T) {
	requireRuntimeAsset(t)
	tag := "v1.4.0"
	ver := "1.4.0"

	server := newRedirectReleaseServer(t, "owner/name", tag)
	defer server.Close()

	restore := overrideGitHubBases(server.URL)
	defer restore()

	st := CheckForUpdate(context.Background(), CheckOptions{
		Repo:             "owner/name",
		InstalledVersion: "1.0.0",
		Timeout:          time.Second,
	})

	if !st.Supported {
		t.Fatalf("expected supported update check, got error=%q", st.Error)
	}
	if st.RemoteVersion != ver {
		t.Fatalf("expected remote version %s, got %s", ver, st.RemoteVersion)
	}
	if st.RemoteTag != tag {
		t.Fatalf("expected remote tag %s, got %s", tag, st.RemoteTag)
	}
}

func TestCheckForUpdateIncludePrereleaseTreatsStableSameVersionAsNewer(t *testing.T) {
	requireRuntimeAsset(t)
	tag := "v1.2.4"
	ver := "1.2.4"
	asset, err := assetName(ver, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName error: %v", err)
	}
	payload := []byte("stable-binary")
	server := newReleaseListServer(t, []testReleaseAsset{{Tag: tag, Asset: asset, Payload: payload, Prerelease: false}})
	defer server.Close()
	restore := overrideGitHubBases(server.URL)
	defer restore()

	st := CheckForUpdate(context.Background(), CheckOptions{
		Repo:              "owner/name",
		InstalledVersion:  "1.2.4-rc.1",
		Timeout:           time.Second,
		IncludePrerelease: true,
	})
	if !st.Supported {
		t.Fatalf("expected supported update check, got error=%q", st.Error)
	}
	if !st.UpdateAvailable || st.RemoteVersion != "1.2.4" {
		t.Fatalf("status = %#v, want stable 1.2.4 newer than 1.2.4-rc.1", st)
	}
}

func TestCheckForUpdateRejectsDev(t *testing.T) {
	st := CheckForUpdate(context.Background(), CheckOptions{InstalledVersion: "dev"})
	if st.Supported {
		t.Fatalf("expected unsupported when local version is dev")
	}
}

func TestCheckForUpdateUnknownLocalVersion(t *testing.T) {
	st := CheckForUpdate(context.Background(), CheckOptions{InstalledVersion: " "})
	if st.Supported {
		t.Fatalf("expected unsupported when local version unknown")
	}
	if !strings.Contains(st.Error, "unknown local version") {
		t.Fatalf("expected unknown local version error, got %q", st.Error)
	}
}

func TestResolveInstallPathUsesStableSiblingForNFSSillyRename(t *testing.T) {
	t.Setenv(EnvInstallDir, "")
	prevExecutablePath := executablePath
	t.Cleanup(func() { executablePath = prevExecutablePath })

	dir := t.TempDir()
	stable := filepath.Join(dir, binaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable binary: %v", err)
	}
	running := filepath.Join(dir, ".nfs802014de01c482a800000492")
	executablePath = func() (string, error) {
		return running, nil
	}

	got, err := ResolveInstallPath("")
	if err != nil {
		t.Fatalf("ResolveInstallPath error: %v", err)
	}
	if got != stable {
		t.Fatalf("ResolveInstallPath = %q, want stable sibling %q", got, stable)
	}
}

func TestResolveInstallPathFallsBackToStableArgv0Last(t *testing.T) {
	t.Setenv(EnvInstallDir, "")
	prevExecutablePath := executablePath
	prevArgv0Path := argv0Path
	t.Cleanup(func() {
		executablePath = prevExecutablePath
		argv0Path = prevArgv0Path
	})

	dir := t.TempDir()
	stable := filepath.Join(dir, binaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable binary: %v", err)
	}
	executablePath = func() (string, error) {
		return filepath.Join(t.TempDir(), ".nfs802014de01c482a800000492"), nil
	}
	argv0Path = func() string { return stable }

	got, err := ResolveInstallPath("")
	if err != nil {
		t.Fatalf("ResolveInstallPath error: %v", err)
	}
	if got != stable {
		t.Fatalf("ResolveInstallPath = %q, want argv0 stable %q", got, stable)
	}
}

func TestResolveInstallPathRejectsExplicitNFSSillyRename(t *testing.T) {
	dir := t.TempDir()
	stable := filepath.Join(dir, binaryName())
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable binary: %v", err)
	}
	running := filepath.Join(dir, ".nfs802014de01c482a800000492")
	_, err := ResolveInstallPath(running)
	if err == nil || !strings.Contains(err.Error(), "transient") {
		t.Fatalf("ResolveInstallPath explicit .nfs error = %v, want transient rejection", err)
	}
}

func TestStableInstallPathKeepsNFSSillyRenameWhenSiblingMissing(t *testing.T) {
	running := filepath.Join(t.TempDir(), ".nfs802014de01c482a800000492")
	if got := StableInstallPathFromExecutable(running); got != running {
		t.Fatalf("StableInstallPathFromExecutable = %q, want original %q", got, running)
	}
}

func TestPerformUpdateExplicitVersion(t *testing.T) {
	requireRuntimeAsset(t)
	tag := "v1.2.3"
	ver := "1.2.3"
	asset, err := assetName(ver, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName error: %v", err)
	}
	payload := []byte("binary-payload")

	server := newReleaseServer(t, tag, asset, payload)
	defer server.Close()

	restore := overrideGitHubBases(server.URL)
	defer restore()

	tmpDir := t.TempDir()
	dest := filepath.Join(tmpDir, "codex-proxy")
	if err := os.WriteFile(dest, []byte("old"), 0o755); err != nil {
		t.Fatalf("write dest: %v", err)
	}

	res, err := PerformUpdate(context.Background(), UpdateOptions{
		Repo:        "owner/name",
		Version:     tag,
		InstallPath: dest,
		Timeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("PerformUpdate error: %v", err)
	}
	if res.Version != ver {
		t.Fatalf("expected version %s, got %s", ver, res.Version)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("expected payload to be installed")
	}
}

func TestPerformUpdateUsesStableInstallTargetForTransientRuntimePath(t *testing.T) {
	requireRuntimeAsset(t)
	t.Setenv(EnvInstallDir, "")
	tag := "v1.2.5"
	ver := "1.2.5"
	asset, err := assetName(ver, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName error: %v", err)
	}
	payload := []byte("binary-payload")
	server := newReleaseServer(t, tag, asset, payload)
	defer server.Close()
	restore := overrideGitHubBases(server.URL)
	defer restore()

	prevExecutablePath := executablePath
	prevArgv0Path := argv0Path
	t.Cleanup(func() {
		executablePath = prevExecutablePath
		argv0Path = prevArgv0Path
	})

	tmpDir := t.TempDir()
	dest := filepath.Join(tmpDir, binaryName())
	if err := os.WriteFile(dest, []byte("old"), 0o755); err != nil {
		t.Fatalf("write dest: %v", err)
	}
	raw := filepath.Join(tmpDir, ".nfs802014de01c482a800000492")
	executablePath = func() (string, error) { return raw, nil }
	argv0Path = func() string { return "" }

	res, err := PerformUpdate(context.Background(), UpdateOptions{
		Repo:    "owner/name",
		Version: tag,
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("PerformUpdate error: %v", err)
	}
	if res.InstallPath != dest {
		t.Fatalf("install path = %q, want stable %q", res.InstallPath, dest)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("expected payload to be installed at stable target, got %q", got)
	}
	if _, err := os.Stat(raw); !os.IsNotExist(err) {
		t.Fatalf("transient raw path should not be created or replaced, stat err=%v", err)
	}
}

func TestPerformUpdateChecksumMismatch(t *testing.T) {
	requireRuntimeAsset(t)
	tag := "v2.0.0"
	ver := "2.0.0"
	asset, err := assetName(ver, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName error: %v", err)
	}
	payload := []byte("binary-payload")

	server := newReleaseServerWithChecksum(t, tag, asset, payload, strings.Repeat("0", 64))
	defer server.Close()

	restore := overrideGitHubBases(server.URL)
	defer restore()

	dest := filepath.Join(t.TempDir(), "codex-proxy")
	_, err = PerformUpdate(context.Background(), UpdateOptions{
		Repo:        "owner/name",
		Version:     tag,
		InstallPath: dest,
		Timeout:     time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got %v", err)
	}
}

func TestPerformUpdateValidateBinaryRejectsInvalidPayload(t *testing.T) {
	requireRuntimeAsset(t)
	tag := "v2.1.0"
	ver := "2.1.0"
	asset, err := assetName(ver, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName error: %v", err)
	}
	server := newReleaseServer(t, tag, asset, []byte("not a helper binary"))
	defer server.Close()
	restore := overrideGitHubBases(server.URL)
	defer restore()

	dest := filepath.Join(t.TempDir(), binaryName())
	if err := os.WriteFile(dest, []byte("old"), 0o755); err != nil {
		t.Fatalf("write dest: %v", err)
	}
	_, err = PerformUpdate(context.Background(), UpdateOptions{
		Repo:           "owner/name",
		Version:        tag,
		InstallPath:    dest,
		Timeout:        time.Second,
		ValidateBinary: true,
	})
	if err == nil || !strings.Contains(err.Error(), "validate downloaded binary") {
		t.Fatalf("expected validation error, got %v", err)
	}
	got, readErr := os.ReadFile(dest)
	if readErr != nil {
		t.Fatalf("read dest: %v", readErr)
	}
	if string(got) != "old" {
		t.Fatalf("dest was replaced despite validation failure: %q", got)
	}
}

func TestValidateDownloadedBinaryRejectsRCWhenFinalRequested(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script validation fixture is POSIX-only")
	}
	path := filepath.Join(t.TempDir(), "codex-proxy")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho 'codex-proxy version 0.1.0-rc.133 (abc) 2026-05-13T00:00:00Z'\n"), 0o700); err != nil {
		t.Fatalf("write validation fixture: %v", err)
	}
	err := validateDownloadedBinary(context.Background(), path, "v0.1.0", time.Second)
	if err == nil {
		t.Fatal("validateDownloadedBinary accepted rc output for final target")
	}
	if !strings.Contains(err.Error(), `parsed as "0.1.0-rc.133"`) || !strings.Contains(err.Error(), `want "0.1.0"`) {
		t.Fatalf("validation error = %v, want parsed exact-version mismatch", err)
	}
}

func TestPerformUpdateLatest(t *testing.T) {
	requireRuntimeAsset(t)
	tag := "v3.1.0"
	ver := "3.1.0"
	asset, err := assetName(ver, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName error: %v", err)
	}
	payload := []byte("latest-binary")

	server := newReleaseServer(t, tag, asset, payload)
	defer server.Close()

	restore := overrideGitHubBases(server.URL)
	defer restore()

	dest := filepath.Join(t.TempDir(), "codex-proxy")
	_, err = PerformUpdate(context.Background(), UpdateOptions{
		Repo:        "owner/name",
		Version:     "latest",
		InstallPath: dest,
		Timeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("PerformUpdate latest error: %v", err)
	}
}

func TestPerformUpdateLatestIncludePrerelease(t *testing.T) {
	requireRuntimeAsset(t)
	stableVer := "3.1.0"
	stableAsset, err := assetName(stableVer, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName stable error: %v", err)
	}
	preTag := "v3.2.0-rc.10"
	preVer := "3.2.0-rc.10"
	preAsset, err := assetName(preVer, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName prerelease error: %v", err)
	}
	payload := []byte("latest-prerelease-binary")
	server := newReleaseListServer(t, []testReleaseAsset{
		{Tag: "v" + stableVer, Asset: stableAsset, Payload: []byte("stable-binary"), Prerelease: false},
		{Tag: preTag, Asset: preAsset, Payload: payload, Prerelease: true},
	})
	defer server.Close()
	restore := overrideGitHubBases(server.URL)
	defer restore()

	dest := filepath.Join(t.TempDir(), "codex-proxy")
	res, err := PerformUpdate(context.Background(), UpdateOptions{
		Repo:              "owner/name",
		Version:           "latest",
		InstallPath:       dest,
		Timeout:           time.Second,
		IncludePrerelease: true,
	})
	if err != nil {
		t.Fatalf("PerformUpdate latest prerelease error: %v", err)
	}
	if res.Version != preVer {
		t.Fatalf("result version = %q, want %s", res.Version, preVer)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("installed payload = %q, want prerelease payload", got)
	}
}

func TestPerformUpdateInvalidVersion(t *testing.T) {
	_, err := PerformUpdate(context.Background(), UpdateOptions{
		Version: "v",
	})
	if err == nil || !strings.Contains(err.Error(), "invalid version") {
		t.Fatalf("expected invalid version error, got %v", err)
	}
}

func TestPerformUpdateDownloadFailure(t *testing.T) {
	requireRuntimeAsset(t)
	tag := "v9.9.9"
	ver := "9.9.9"
	asset, err := assetName(ver, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatalf("assetName error: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/checksums.txt"):
			_, _ = fmt.Fprintf(w, "%s  %s\n", strings.Repeat("0", 64), asset)
		case strings.Contains(r.URL.Path, "/"+asset):
			http.Error(w, "nope", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	restore := overrideGitHubBases(server.URL)
	defer restore()

	dest := filepath.Join(t.TempDir(), "codex-proxy")
	_, err = PerformUpdate(context.Background(), UpdateOptions{
		Repo:        "owner/name",
		Version:     tag,
		InstallPath: dest,
		Timeout:     time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "download failed") {
		t.Fatalf("expected download failed error, got %v", err)
	}
}

func TestAssetNameErrors(t *testing.T) {
	if _, err := assetName("1.0.0", "solaris", "amd64"); err == nil {
		t.Fatalf("expected error for unsupported os")
	}
	if _, err := assetName("1.0.0", "linux", "arm64"); err == nil {
		t.Fatalf("expected error for linux arm64 asset")
	}
}

func requireRuntimeAsset(t *testing.T) {
	t.Helper()
	if _, err := assetName("1.0.0", runtime.GOOS, runtime.GOARCH); err != nil {
		t.Skipf("runtime asset unsupported: %v", err)
	}
}

func newReleaseServer(t *testing.T, tag, asset string, payload []byte) *httptest.Server {
	sum := sha256.Sum256(payload)
	checksum := hex.EncodeToString(sum[:])
	return newReleaseServerWithChecksum(t, tag, asset, payload, checksum)
}

func newReleaseServerWithChecksum(t *testing.T, tag, asset string, payload []byte, checksum string) *httptest.Server {
	t.Helper()
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/releases/latest"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"tag_name": "%s"}`, tag)
		case strings.Contains(r.URL.Path, "/checksums.txt"):
			w.Header().Set("Content-Type", "text/plain")
			_, _ = fmt.Fprintf(w, "%s  %s\n", checksum, asset)
		case strings.HasSuffix(r.URL.Path, "/"+asset) || strings.Contains(r.URL.Path, "/"+asset):
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(payload)
		default:
			http.NotFound(w, r)
		}
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

type testReleaseAsset struct {
	Tag        string
	Asset      string
	Payload    []byte
	Prerelease bool
}

func newReleaseListServer(t *testing.T, releases []testReleaseAsset) *httptest.Server {
	t.Helper()
	payloadByAsset := make(map[string][]byte)
	checksumByAsset := make(map[string]string)
	for _, rel := range releases {
		payloadByAsset[rel.Asset] = rel.Payload
		sum := sha256.Sum256(rel.Payload)
		checksumByAsset[rel.Asset] = hex.EncodeToString(sum[:])
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/repos/") && strings.Contains(r.URL.Path, "/releases"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `[`)
			for i, rel := range releases {
				if i > 0 {
					_, _ = fmt.Fprint(w, `,`)
				}
				_, _ = fmt.Fprintf(w, `{"tag_name":%q,"prerelease":%t,"published_at":"2026-05-04T00:00:00Z","assets":[{"name":%q}]}`, rel.Tag, rel.Prerelease, rel.Asset)
			}
			_, _ = fmt.Fprint(w, `]`)
		case strings.Contains(r.URL.Path, "/checksums.txt"):
			w.Header().Set("Content-Type", "text/plain")
			for _, rel := range releases {
				_, _ = fmt.Fprintf(w, "%s  %s\n", checksumByAsset[rel.Asset], rel.Asset)
			}
		default:
			for asset, payload := range payloadByAsset {
				if strings.HasSuffix(r.URL.Path, "/"+asset) || strings.Contains(r.URL.Path, "/"+asset) {
					w.Header().Set("Content-Type", "application/octet-stream")
					_, _ = w.Write(payload)
					return
				}
			}
			http.NotFound(w, r)
		}
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

func newRedirectReleaseServer(t *testing.T, repo, tag string) *httptest.Server {
	t.Helper()
	tagPath := "/" + repo + "/releases/tag/" + tag
	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/repos/") && strings.Contains(r.URL.Path, "/releases/latest"):
			http.Error(w, "api unavailable", http.StatusServiceUnavailable)
		case strings.Contains(r.URL.Path, "/releases/latest"):
			w.Header().Set("Location", tagPath)
			w.WriteHeader(http.StatusFound)
		case strings.Contains(r.URL.Path, tagPath):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		default:
			http.NotFound(w, r)
		}
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

func overrideGitHubBases(base string) func() {
	prevAPI := githubAPIBase
	prevRelease := githubReleaseBase
	githubAPIBase = base
	githubReleaseBase = base
	return func() {
		githubAPIBase = prevAPI
		githubReleaseBase = prevRelease
	}
}
