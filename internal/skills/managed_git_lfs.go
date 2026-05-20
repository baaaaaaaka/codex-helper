package skills

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

const (
	managedGitLFSVersion    = "3.7.1"
	managedSkillToolsEnvOff = "CODEX_PROXY_DISABLE_MANAGED_SKILL_TOOLS"
	maxGitLFSArchiveBytes   = 64 << 20
)

var (
	managedGitLFSInstaller      = ensureManagedGitLFS
	managedGitLFSHTTPClient     = http.DefaultClient
	managedGitLFSReleaseBaseURL = "https://github.com/git-lfs/git-lfs/releases/download"
)

type gitLFSArchive struct {
	Filename string
	SHA256   string
	Format   string
	Platform string
}

func ensureManagedGitLFS(ctx context.Context, toolsRoot string) (string, error) {
	if strings.TrimSpace(os.Getenv(managedSkillToolsEnvOff)) != "" {
		return "", fmt.Errorf("managed skill tool installation disabled by %s", managedSkillToolsEnvOff)
	}
	toolsRoot = filepath.Clean(strings.TrimSpace(toolsRoot))
	if toolsRoot == "" || toolsRoot == "." {
		return "", fmt.Errorf("empty managed skill tools root")
	}
	archive, err := gitLFSArchiveForRuntime(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return "", err
	}
	root := filepath.Join(toolsRoot, "git-lfs")
	installDir := filepath.Join(root, "v"+managedGitLFSVersion+"-"+archive.Platform)
	binaryPath := filepath.Join(installDir, gitLFSBinaryName())
	if probeGitLFSBinary(ctx, binaryPath) == nil {
		return installDir, nil
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", fmt.Errorf("create managed git-lfs root: %w", err)
	}
	lock := flock.New(filepath.Join(root, ".install.lock"))
	if err := lock.Lock(); err != nil {
		return "", fmt.Errorf("lock managed git-lfs install: %w", err)
	}
	defer func() { _ = lock.Unlock() }()
	if probeGitLFSBinary(ctx, binaryPath) == nil {
		return installDir, nil
	}
	if err := installManagedGitLFS(ctx, root, installDir, archive); err != nil {
		return "", err
	}
	return installDir, nil
}

func gitLFSArchiveForRuntime(goos, goarch string) (gitLFSArchive, error) {
	arch, ok := gitLFSArchiveArch(goarch)
	if !ok {
		return gitLFSArchive{}, fmt.Errorf("unsupported git-lfs architecture %s/%s", goos, goarch)
	}
	format := "tar.gz"
	if goos == "darwin" || goos == "windows" {
		format = "zip"
	}
	filename := fmt.Sprintf("git-lfs-%s-%s-v%s.%s", goos, arch, managedGitLFSVersion, format)
	sum, ok := gitLFSArchiveChecksums()[filename]
	if !ok {
		return gitLFSArchive{}, fmt.Errorf("unsupported git-lfs platform %s/%s", goos, goarch)
	}
	return gitLFSArchive{
		Filename: filename,
		SHA256:   sum,
		Format:   format,
		Platform: goos + "-" + arch,
	}, nil
}

func gitLFSArchiveArch(goarch string) (string, bool) {
	switch goarch {
	case "386", "amd64", "arm", "arm64", "loong64", "ppc64le", "riscv64", "s390x":
		return goarch, true
	default:
		return "", false
	}
}

func gitLFSArchiveChecksums() map[string]string {
	return map[string]string{
		"git-lfs-darwin-amd64-v3.7.1.zip":     "b5b1b641c0648c83661fa9eda991cd3eff945264dabc2cdf411a80dfe7ec0970",
		"git-lfs-darwin-arm64-v3.7.1.zip":     "76260fb34f4ee622ff0a66b857e5954aa49c7e343a92e57a1ec4a760618c94b2",
		"git-lfs-freebsd-386-v3.7.1.tar.gz":   "811cf7b7d459ba507e01d01172b05f5bfea2fce9b6b9a22a98f8de87dfd4d1da",
		"git-lfs-freebsd-amd64-v3.7.1.tar.gz": "50931d36415a80f5bd427cbb1e283d4c825a1b24fa6da0481c9fa1b5f5803c6f",
		"git-lfs-linux-386-v3.7.1.tar.gz":     "a49eed4612d9a33db848db8cb9079b15d5f3116bbca2c1a11cb89a70e3218921",
		"git-lfs-linux-amd64-v3.7.1.tar.gz":   "1c0b6ee5200ca708c5cebebb18fdeb0e1c98f1af5c1a9cba205a4c0ab5a5ec08",
		"git-lfs-linux-arm-v3.7.1.tar.gz":     "567002d2735ceb0e876e326736f1b72895931d5ac156002cc8561b072a4ce9a3",
		"git-lfs-linux-arm64-v3.7.1.tar.gz":   "73a9c90eeb4312133a63c3eaee0c38c019ea7bfa0953d174809d25b18588dd8d",
		"git-lfs-linux-loong64-v3.7.1.tar.gz": "10c300a81968b070e331d36abcf21da18e478b17f4a61c009eb9d2b50374132c",
		"git-lfs-linux-ppc64le-v3.7.1.tar.gz": "100fbefdd86722dafd56737121510289ece9574c7bb8ec01b4633f8892acc427",
		"git-lfs-linux-riscv64-v3.7.1.tar.gz": "4e17b28e64416b680a68cb2ac3e3514cecb86548603c78774519b26686683928",
		"git-lfs-linux-s390x-v3.7.1.tar.gz":   "d4b68db5d7cc34395b8d6c392326aeff98a297bde2053625560df6c76eb97c69",
		"git-lfs-windows-386-v3.7.1.zip":      "06c05c06523abf3930301b3022527ad881b1a7f8bf036ed6d93c8e68569041bb",
		"git-lfs-windows-amd64-v3.7.1.zip":    "8683cdc3d6c029b49393dcebbaa6265bd6efd9abdcf837be855b4cd42e5e80b6",
		"git-lfs-windows-arm64-v3.7.1.zip":    "9441383a3928a7f387223711929292a46ace95580ceed443d61e7b8a4d9615c3",
	}
}

func installManagedGitLFS(ctx context.Context, root string, installDir string, archive gitLFSArchive) error {
	downloads := filepath.Join(root, ".downloads")
	if err := os.MkdirAll(downloads, 0o700); err != nil {
		return fmt.Errorf("create managed git-lfs download dir: %w", err)
	}
	archivePath := filepath.Join(downloads, archive.Filename)
	url := strings.TrimRight(managedGitLFSReleaseBaseURL, "/") + "/v" + managedGitLFSVersion + "/" + archive.Filename
	if err := downloadManagedGitLFSArchive(ctx, url, archivePath); err != nil {
		return err
	}
	if err := verifyFileSHA256(archivePath, archive.SHA256); err != nil {
		return err
	}
	staging := installDir + ".staging-" + fmt.Sprint(os.Getpid()) + "-" + time.Now().UTC().Format("20060102150405")
	if err := os.RemoveAll(staging); err != nil {
		return fmt.Errorf("remove stale managed git-lfs staging dir: %w", err)
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		return fmt.Errorf("create managed git-lfs staging dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(staging) }()
	if err := extractGitLFSBinary(archivePath, archive.Format, staging); err != nil {
		return err
	}
	binaryPath := filepath.Join(staging, gitLFSBinaryName())
	if err := os.Chmod(binaryPath, 0o755); err != nil {
		return fmt.Errorf("chmod managed git-lfs: %w", err)
	}
	if err := probeGitLFSBinary(ctx, binaryPath); err != nil {
		return fmt.Errorf("probe managed git-lfs: %w", err)
	}
	if err := os.RemoveAll(installDir); err != nil {
		return fmt.Errorf("remove previous managed git-lfs install: %w", err)
	}
	if err := os.Rename(staging, installDir); err != nil {
		return fmt.Errorf("publish managed git-lfs install: %w", err)
	}
	return nil
}

func downloadManagedGitLFSArchive(ctx context.Context, url string, archivePath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create managed git-lfs download request: %w", err)
	}
	req.Header.Set("User-Agent", "codex-helper managed git-lfs")
	resp, err := managedGitLFSHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("download managed git-lfs: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download managed git-lfs: unexpected HTTP status %s", resp.Status)
	}
	tmp := archivePath + ".tmp"
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale managed git-lfs download: %w", err)
	}
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create managed git-lfs download: %w", err)
	}
	n, copyErr := io.Copy(out, io.LimitReader(resp.Body, maxGitLFSArchiveBytes+1))
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write managed git-lfs download: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close managed git-lfs download: %w", closeErr)
	}
	if n > maxGitLFSArchiveBytes {
		_ = os.Remove(tmp)
		return fmt.Errorf("managed git-lfs archive is too large")
	}
	if err := os.Rename(tmp, archivePath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("publish managed git-lfs download: %w", err)
	}
	return nil
}

func verifyFileSHA256(path string, want string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open managed git-lfs archive: %w", err)
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return fmt.Errorf("hash managed git-lfs archive: %w", err)
	}
	got := hex.EncodeToString(sum.Sum(nil))
	if got != strings.ToLower(strings.TrimSpace(want)) {
		return fmt.Errorf("managed git-lfs checksum mismatch for %s: expected %s, got %s", filepath.Base(path), want, got)
	}
	return nil
}

func extractGitLFSBinary(archivePath string, format string, destDir string) error {
	switch format {
	case "tar.gz":
		return extractGitLFSBinaryFromTarGZ(archivePath, destDir)
	case "zip":
		return extractGitLFSBinaryFromZip(archivePath, destDir)
	default:
		return fmt.Errorf("unsupported managed git-lfs archive format %q", format)
	}
}

func extractGitLFSBinaryFromTarGZ(archivePath string, destDir string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open managed git-lfs archive: %w", err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("read managed git-lfs archive: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read managed git-lfs tar entry: %w", err)
		}
		if header.Typeflag != tar.TypeReg || filepath.Base(header.Name) != gitLFSBinaryName() {
			continue
		}
		return writeGitLFSBinary(destDir, tr)
	}
	return fmt.Errorf("managed git-lfs archive did not contain %s", gitLFSBinaryName())
}

func extractGitLFSBinaryFromZip(archivePath string, destDir string) error {
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("open managed git-lfs archive: %w", err)
	}
	defer reader.Close()
	for _, file := range reader.File {
		if file.FileInfo().IsDir() || filepath.Base(file.Name) != gitLFSBinaryName() {
			continue
		}
		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("open managed git-lfs zip entry: %w", err)
		}
		err = writeGitLFSBinary(destDir, rc)
		closeErr := rc.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return fmt.Errorf("close managed git-lfs zip entry: %w", closeErr)
		}
		return nil
	}
	return fmt.Errorf("managed git-lfs archive did not contain %s", gitLFSBinaryName())
}

func writeGitLFSBinary(destDir string, src io.Reader) error {
	target := filepath.Join(destDir, gitLFSBinaryName())
	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("create managed git-lfs binary: %w", err)
	}
	_, copyErr := io.Copy(out, src)
	closeErr := out.Close()
	if copyErr != nil {
		return fmt.Errorf("write managed git-lfs binary: %w", copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close managed git-lfs binary: %w", closeErr)
	}
	return nil
}

func probeGitLFSBinary(ctx context.Context, binaryPath string) error {
	if _, err := os.Stat(binaryPath); err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, binaryPath, "version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	if !strings.Contains(strings.ToLower(string(out)), "git-lfs") {
		return fmt.Errorf("unexpected git-lfs version output: %s", strings.TrimSpace(string(out)))
	}
	return nil
}

func gitLFSBinaryName() string {
	if runtime.GOOS == "windows" {
		return "git-lfs.exe"
	}
	return "git-lfs"
}

func envWithPathPrefix(dir string) []string {
	key := "PATH"
	for _, kv := range os.Environ() {
		name, _, ok := strings.Cut(kv, "=")
		if ok && strings.EqualFold(name, "PATH") {
			key = name
			break
		}
	}
	value := dir
	if current := os.Getenv("PATH"); current != "" {
		value += string(os.PathListSeparator) + current
	}
	return []string{key + "=" + value}
}
