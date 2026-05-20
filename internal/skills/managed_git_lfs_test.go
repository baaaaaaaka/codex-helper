package skills

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitLFSArchiveForRuntimeCommonPlatforms(t *testing.T) {
	for _, tc := range []struct {
		goos     string
		goarch   string
		filename string
		format   string
	}{
		{goos: "linux", goarch: "amd64", filename: "git-lfs-linux-amd64-v3.7.1.tar.gz", format: "tar.gz"},
		{goos: "linux", goarch: "arm64", filename: "git-lfs-linux-arm64-v3.7.1.tar.gz", format: "tar.gz"},
		{goos: "darwin", goarch: "arm64", filename: "git-lfs-darwin-arm64-v3.7.1.zip", format: "zip"},
		{goos: "windows", goarch: "amd64", filename: "git-lfs-windows-amd64-v3.7.1.zip", format: "zip"},
	} {
		t.Run(tc.goos+"-"+tc.goarch, func(t *testing.T) {
			archive, err := gitLFSArchiveForRuntime(tc.goos, tc.goarch)
			if err != nil {
				t.Fatalf("gitLFSArchiveForRuntime: %v", err)
			}
			if archive.Filename != tc.filename || archive.Format != tc.format {
				t.Fatalf("archive = %#v, want %s/%s", archive, tc.filename, tc.format)
			}
			if len(archive.SHA256) != 64 {
				t.Fatalf("checksum len = %d, want 64", len(archive.SHA256))
			}
		})
	}
}

func TestEnvWithPathPrefixIncludesManagedDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "git-lfs")
	env := envWithPathPrefix(dir)
	if !envHasPathDir(env, dir) {
		t.Fatalf("PATH env %v does not include %s", env, dir)
	}
}

func TestEnsureManagedGitLFSRespectsDisableEnv(t *testing.T) {
	t.Setenv(managedSkillToolsEnvOff, "1")
	_, err := ensureManagedGitLFS(t.Context(), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("ensureManagedGitLFS disabled error = %v", err)
	}
}

func TestEnsureManagedGitLFSIntegrationDownload(t *testing.T) {
	if os.Getenv("CODEX_HELPER_GIT_LFS_DOWNLOAD_TEST") != "1" {
		t.Skip("set CODEX_HELPER_GIT_LFS_DOWNLOAD_TEST=1 to download and verify managed git-lfs")
	}
	dir, err := ensureManagedGitLFS(t.Context(), t.TempDir())
	if err != nil {
		t.Fatalf("ensureManagedGitLFS: %v", err)
	}
	if err := probeGitLFSBinary(t.Context(), filepath.Join(dir, gitLFSBinaryName())); err != nil {
		t.Fatalf("probe downloaded git-lfs: %v", err)
	}
}
