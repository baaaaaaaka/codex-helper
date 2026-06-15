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
	SHA256     string
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
	sum, err := downloadManagedASRStandalonePythonArchive(ctx, asset, archivePath)
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
	if err := managedASRPublishDir("managed Python runtime", staging, installRoot); err != nil {
		return managedASRBootstrapPython{}, err
	}
	finalPython := filepath.Join(installRoot, rel)
	return managedASRBootstrapPython{
		Command: finalPython,
		Display: "managed Python (" + firstNonEmptyString(asset.ReleaseTag, asset.Name) + ")",
	}, nil
}

func downloadManagedASRStandalonePythonArchive(ctx context.Context, asset managedASRStandalonePythonAsset, path string) (string, error) {
	url := strings.TrimSpace(asset.URL)
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
	written, err := io.Copy(tmp, io.TeeReader(resp.Body, hash))
	if err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if asset.Size > 0 && written != asset.Size {
		return "", managedASRDownloadIntegrityError{Label: "managed Python archive", Err: fmt.Errorf("downloaded %d bytes, want %d", written, asset.Size)}
	}
	gotSHA := hex.EncodeToString(hash.Sum(nil))
	if strings.TrimSpace(asset.SHA256) != "" && !strings.EqualFold(gotSHA, asset.SHA256) {
		return "", managedASRDownloadIntegrityError{Label: "managed Python archive", Err: fmt.Errorf("sha256 %s, want %s", gotSHA, asset.SHA256)}
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return "", err
	}
	if err := durableReplaceFile(tmpPath, path); err != nil {
		return "", managedASRCacheError{Op: "publish downloaded managed Python archive", Path: path, Err: err}
	}
	return gotSHA, nil
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
		size, sha256, ok := managedASRStandalonePythonAssetIntegrity(name)
		if !ok {
			return nil, fmt.Errorf("managed Teams speech recognition Python asset %q has no pinned integrity metadata", name)
		}
		out = append(out, managedASRStandalonePythonAsset{
			ReleaseTag: managedASRStandalonePythonReleaseTag,
			Name:       name,
			URL:        managedASRStandalonePythonDownloadBase + "/" + managedASRStandalonePythonReleaseTag + "/" + name,
			SHA256:     sha256,
			Size:       size,
			Target:     target,
		})
	}
	return out, nil
}

func managedASRStandalonePythonAssetIntegrity(name string) (int64, string, bool) {
	type integrity struct {
		size   int64
		sha256 string
	}
	assets := map[string]integrity{
		"cpython-3.10.20+20260510-aarch64-apple-darwin-install_only.tar.gz":               {size: 25921421, sha256: "22f02aa2458efa28029f91800c3d85a270ae308a2d8450f3f6cef49f56abfa48"},
		"cpython-3.10.20+20260510-aarch64-apple-darwin-install_only_stripped.tar.gz":      {size: 25826007, sha256: "36b7364a5cd75e5b8591c4dc6cc30d84d9112b62a2b8199c406d63f2ca2f981f"},
		"cpython-3.10.20+20260510-aarch64-unknown-linux-gnu-install_only.tar.gz":          {size: 43843105, sha256: "e204550daa63afd113519feb71732f77b5ff56e4a383acf26d0b5aa2c79dadff"},
		"cpython-3.10.20+20260510-aarch64-unknown-linux-gnu-install_only_stripped.tar.gz": {size: 29734163, sha256: "9f0becaa10fff71a455dcc9f4343cd784ee506a63d770d87fe92030cb376feee"},
		"cpython-3.10.20+20260510-x86_64-apple-darwin-install_only.tar.gz":                {size: 25634496, sha256: "9a48464592efe7a1d8a0df9a1868f0b1bf36cbf72997f503b4bbdca26ff9d96a"},
		"cpython-3.10.20+20260510-x86_64-apple-darwin-install_only_stripped.tar.gz":       {size: 25559363, sha256: "e7ce16965714c05b2cc4515f20cd0c1f8d4fef037bd34daf18d924677f6545b8"},
		"cpython-3.10.20+20260510-x86_64-pc-windows-msvc-install_only.tar.gz":             {size: 39591215, sha256: "b64ea8bb067d9dbcaf197818dd57e56173033d302dcd0be29020088827529224"},
		"cpython-3.10.20+20260510-x86_64-pc-windows-msvc-install_only_stripped.tar.gz":    {size: 22259051, sha256: "d1e8fb30cba04e6bb5a703e0186da77f833957de027562fa4df9fd0424ae5f7e"},
		"cpython-3.10.20+20260510-x86_64-unknown-linux-gnu-install_only.tar.gz":           {size: 43672750, sha256: "338ae9e6916c85a354ba0258ac0eaaef63c0389b30d82d2467e90a1a32b1789b"},
		"cpython-3.10.20+20260510-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz":  {size: 29324796, sha256: "dc734bdd388975c0b093fe730b272af741a2e192475d38bc6845a687b6405922"},
	}
	item, ok := assets[name]
	return item.size, item.sha256, ok
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
