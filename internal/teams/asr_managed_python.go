package teams

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	managedASRStandalonePythonRuntimeVersion = "python-build-standalone-cpython-v1"
	managedASRStandalonePythonReleaseTag     = "20260510"
	managedASRStandalonePythonVersion        = "3.10.20"
	managedASRStandalonePythonDownloadBase   = "https://github.com/astral-sh/python-build-standalone/releases/download"
	managedASRStandalonePythonDirName        = "standalone-python"
)

var (
	managedASRStandalonePythonHTTPClient = http.DefaultClient
)

type managedASRStandalonePythonAsset struct {
	ReleaseTag string
	Name       string
	URL        string
	Size       int64
	Target     string
}

type managedASRStandalonePythonMarker struct {
	Version    string `json:"version"`
	PythonRel  string `json:"python_rel"`
	ReleaseTag string `json:"release_tag"`
	AssetName  string `json:"asset_name"`
	AssetURL   string `json:"asset_url"`
	SHA256     string `json:"sha256"`
	UpdatedAt  string `json:"updated_at"`
}

func ensureManagedASRStandalonePython(ctx context.Context, cacheRoot string) (managedASRBootstrapPython, error) {
	cacheRoot = strings.TrimSpace(cacheRoot)
	if cacheRoot == "" {
		return managedASRBootstrapPython{}, fmt.Errorf("managed ASR cache root is empty")
	}
	installRoot := filepath.Join(cacheRoot, "python", managedASRStandalonePythonDirName)
	if python, ok := managedASRStandalonePythonFromMarker(installRoot); ok {
		return python, nil
	}
	assets, err := managedASRStandalonePythonAssets(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return managedASRBootstrapPython{}, err
	}
	var installErrors []string
	for _, asset := range assets {
		python, err := installManagedASRStandalonePython(ctx, installRoot, asset)
		if err == nil {
			return python, nil
		}
		installErrors = append(installErrors, asset.Name+": "+err.Error())
	}
	return managedASRBootstrapPython{}, fmt.Errorf("managed Python download/install failed: %s", strings.Join(installErrors, "; "))
}

func managedASRStandalonePythonFromMarker(installRoot string) (managedASRBootstrapPython, bool) {
	markerPath := filepath.Join(installRoot, "runtime.json")
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return managedASRBootstrapPython{}, false
	}
	var marker managedASRStandalonePythonMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return managedASRBootstrapPython{}, false
	}
	if marker.Version != managedASRStandalonePythonRuntimeVersion || strings.TrimSpace(marker.PythonRel) == "" {
		return managedASRBootstrapPython{}, false
	}
	pythonPath := filepath.Join(installRoot, filepath.FromSlash(marker.PythonRel))
	python := managedASRBootstrapPython{
		Command: pythonPath,
		Display: "managed Python (" + firstNonEmptyString(marker.ReleaseTag, marker.AssetName, "python-build-standalone") + ")",
	}
	if err := validateManagedASRBootstrapPythonFn(python); err != nil {
		return managedASRBootstrapPython{}, false
	}
	return python, true
}

func installManagedASRStandalonePython(ctx context.Context, installRoot string, asset managedASRStandalonePythonAsset) (managedASRBootstrapPython, error) {
	parent := filepath.Dir(installRoot)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return managedASRBootstrapPython{}, err
	}
	staging := filepath.Join(parent, ".python-staging-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	archivePath := filepath.Join(parent, ".python-"+safePathPart(asset.Name)+".tar.gz")
	defer func() {
		_ = os.RemoveAll(staging)
		_ = os.Remove(archivePath)
	}()
	if err := os.RemoveAll(staging); err != nil {
		return managedASRBootstrapPython{}, err
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return managedASRBootstrapPython{}, err
	}
	sum, err := downloadManagedASRStandalonePythonArchive(ctx, asset.URL, archivePath)
	if err != nil {
		return managedASRBootstrapPython{}, err
	}
	if err := extractManagedASRTarGz(archivePath, staging); err != nil {
		return managedASRBootstrapPython{}, fmt.Errorf("extract managed Python archive: %w", err)
	}
	pythonPath, err := findManagedASRStandalonePythonExecutable(staging)
	if err != nil {
		return managedASRBootstrapPython{}, err
	}
	rel, err := filepath.Rel(staging, pythonPath)
	if err != nil {
		return managedASRBootstrapPython{}, err
	}
	python := managedASRBootstrapPython{
		Command: pythonPath,
		Display: "managed Python (" + firstNonEmptyString(asset.ReleaseTag, asset.Name) + ")",
	}
	if err := validateManagedASRBootstrapPythonFn(python); err != nil {
		return managedASRBootstrapPython{}, fmt.Errorf("downloaded managed Python is not usable: %w", err)
	}
	marker := managedASRStandalonePythonMarker{
		Version:    managedASRStandalonePythonRuntimeVersion,
		PythonRel:  filepath.ToSlash(rel),
		ReleaseTag: asset.ReleaseTag,
		AssetName:  asset.Name,
		AssetURL:   asset.URL,
		SHA256:     sum,
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	}
	markerData, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return managedASRBootstrapPython{}, err
	}
	if err := writePrivateFileReplacing(filepath.Join(staging, "runtime.json"), append(markerData, '\n'), 0o600); err != nil {
		return managedASRBootstrapPython{}, err
	}
	if err := os.RemoveAll(installRoot); err != nil {
		return managedASRBootstrapPython{}, err
	}
	if err := os.Rename(staging, installRoot); err != nil {
		return managedASRBootstrapPython{}, err
	}
	finalPython := filepath.Join(installRoot, rel)
	return managedASRBootstrapPython{
		Command: finalPython,
		Display: "managed Python (" + firstNonEmptyString(asset.ReleaseTag, asset.Name) + ")",
	}, nil
}

func downloadManagedASRStandalonePythonArchive(ctx context.Context, url string, path string) (string, error) {
	url = strings.TrimSpace(url)
	if url == "" {
		return "", fmt.Errorf("managed Python download URL is empty")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("User-Agent", "codex-helper-managed-asr")
	client := managedASRStandalonePythonHTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("download managed Python archive: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("download managed Python archive: HTTP %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".python-download-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	hash := sha256.New()
	if _, err := io.Copy(tmp, io.TeeReader(resp.Body, hash)); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return "", err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func resolveManagedASRStandalonePythonAsset(_ context.Context, goos string, goarch string) (managedASRStandalonePythonAsset, error) {
	assets, err := managedASRStandalonePythonAssets(goos, goarch)
	if err != nil {
		return managedASRStandalonePythonAsset{}, err
	}
	return assets[0], nil
}

func managedASRStandalonePythonAssets(goos string, goarch string) ([]managedASRStandalonePythonAsset, error) {
	target, err := managedASRStandalonePythonTarget(goos, goarch)
	if err != nil {
		return nil, err
	}
	var out []managedASRStandalonePythonAsset
	for _, suffix := range []string{"install_only_stripped", "install_only"} {
		name := fmt.Sprintf("cpython-%s+%s-%s-%s.tar.gz", managedASRStandalonePythonVersion, managedASRStandalonePythonReleaseTag, target, suffix)
		out = append(out, managedASRStandalonePythonAsset{
			ReleaseTag: managedASRStandalonePythonReleaseTag,
			Name:       name,
			URL:        managedASRStandalonePythonDownloadBase + "/" + managedASRStandalonePythonReleaseTag + "/" + name,
			Target:     target,
		})
	}
	return out, nil
}

func managedASRStandalonePythonTarget(goos string, goarch string) (string, error) {
	switch goos {
	case "linux":
		switch goarch {
		case "amd64":
			return "x86_64-unknown-linux-gnu", nil
		case "arm64":
			return "aarch64-unknown-linux-gnu", nil
		}
	case "darwin":
		switch goarch {
		case "amd64":
			return "x86_64-apple-darwin", nil
		case "arm64":
			return "aarch64-apple-darwin", nil
		}
	case "windows":
		switch goarch {
		case "amd64":
			return "x86_64-pc-windows-msvc", nil
		}
	}
	return "", fmt.Errorf("managed Teams speech recognition Python is not available for %s/%s", goos, goarch)
}

func extractManagedASRTarGz(archivePath string, dest string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		target, err := safeManagedASRExtractPath(dest, header.Name)
		if err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)&0o777); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode)&0o777)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(out, tr)
			closeErr := out.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		case tar.TypeSymlink:
			if err := safeManagedASRSymlink(dest, header.Linkname, target); err != nil {
				return err
			}
		case tar.TypeLink:
			linkTarget, err := safeManagedASRExtractPath(dest, header.Linkname)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Link(linkTarget, target); err != nil {
				return err
			}
		default:
			continue
		}
	}
}

func safeManagedASRExtractPath(dest string, name string) (string, error) {
	name = filepath.Clean(filepath.FromSlash(strings.TrimSpace(name)))
	if name == "." || filepath.IsAbs(name) || strings.HasPrefix(name, ".."+string(os.PathSeparator)) || name == ".." {
		return "", fmt.Errorf("unsafe managed Python archive path: %s", name)
	}
	target := filepath.Join(dest, name)
	rel, err := filepath.Rel(dest, target)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("unsafe managed Python archive path: %s", name)
	}
	return target, nil
}

func safeManagedASRSymlink(dest string, linkName string, target string) error {
	linkName = filepath.Clean(filepath.FromSlash(strings.TrimSpace(linkName)))
	if linkName == "." || filepath.IsAbs(linkName) {
		return fmt.Errorf("unsafe managed Python symlink target: %s", linkName)
	}
	resolved := filepath.Join(filepath.Dir(target), linkName)
	rel, err := filepath.Rel(dest, resolved)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("unsafe managed Python symlink target: %s", linkName)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	_ = os.Remove(target)
	return os.Symlink(linkName, target)
}

func findManagedASRStandalonePythonExecutable(root string) (string, error) {
	var preferred []string
	if runtime.GOOS == "windows" {
		preferred = []string{
			filepath.Join(root, "python", "python.exe"),
			filepath.Join(root, "python.exe"),
		}
	} else {
		preferred = []string{
			filepath.Join(root, "python", "bin", "python3"),
			filepath.Join(root, "python", "bin", "python"),
			filepath.Join(root, "bin", "python3"),
			filepath.Join(root, "bin", "python"),
		}
	}
	for _, path := range preferred {
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path, nil
		}
	}
	var found string
	want := map[string]bool{"python3": true, "python": true}
	if runtime.GOOS == "windows" {
		want = map[string]bool{"python.exe": true}
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil || entry.IsDir() || found != "" {
			return err
		}
		if want[strings.ToLower(entry.Name())] {
			found = path
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("managed Python archive did not contain a Python executable")
	}
	return found, nil
}
