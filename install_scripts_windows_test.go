//go:build windows

package installtest

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallPs1LatestViaRedirect(t *testing.T) {
	runInstallPs1(t, false, false)
}

func TestInstallPs1LatestSurvivesAPIUnavailable(t *testing.T) {
	runInstallPs1(t, true, false)
}

func TestInstallPs1KeepsPathSetupWhenInstallDirAlreadySet(t *testing.T) {
	runInstallPs1(t, false, true)
}

func TestInstallPs1CurrentPowerShellPathAllowsBareCxp(t *testing.T) {
	if _, err := exec.LookPath("powershell"); err != nil {
		t.Skip("powershell not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.ps1")

	repo := "owner/name"
	tag := "v1.2.3"
	verNoV := strings.TrimPrefix(tag, "v")
	asset := fmt.Sprintf("codex-proxy_%s_windows_amd64.exe", verNoV)
	assetData := buildWindowsCodexProxyAsset(t)
	checksum := sha256.Sum256(assetData)

	server := newInstallServer(t, repo, tag, asset, assetData, false, checksum)
	defer server.Close()

	installDir := t.TempDir()
	managedPrefix := t.TempDir()
	tempDir := t.TempDir()
	userProfile := t.TempDir()
	appData := filepath.Join(userProfile, "AppData", "Roaming")
	localAppData := filepath.Join(userProfile, "AppData", "Local")
	profilePath := filepath.Join(t.TempDir(), "profile.ps1")

	basePath := os.Getenv("SystemRoot")
	if basePath == "" {
		basePath = `C:\Windows`
	}
	pathValue := filepath.Join(basePath, "System32")

	psScript := `
$ErrorActionPreference = "Stop"
& $env:TEST_INSTALL_SCRIPT -Repo $env:TEST_REPO -Version latest -InstallDir $env:TEST_INSTALL_DIR
$cmd = Get-Command cxp -ErrorAction Stop
$expected = [IO.Path]::GetFullPath((Join-Path $env:TEST_INSTALL_DIR "cxp.exe"))
$actual = [IO.Path]::GetFullPath($cmd.Source)
if ($actual -ine $expected) {
  throw "cxp resolved to $actual, expected $expected"
}
$version = (& cxp --version | Out-String).Trim()
if ($version -ne "codex-proxy 1.2.3") {
  throw "unexpected cxp version output: $version"
}
`
	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", psScript)
	cmd.Env = filterEnvWithoutKeys(os.Environ(), "Path", "USERPROFILE", "APPDATA", "LOCALAPPDATA")
	cmd.Env = append(cmd.Env,
		"CODEX_PROXY_API_BASE="+server.URL,
		"CODEX_PROXY_RELEASE_BASE="+server.URL,
		"CODEX_PROXY_PROFILE_PATH="+profilePath,
		"CODEX_PROXY_SKIP_PATH_UPDATE=1",
		"CODEX_NPM_PREFIX="+managedPrefix,
		"Path="+pathValue,
		"TEMP="+tempDir,
		"USERPROFILE="+userProfile,
		"APPDATA="+appData,
		"LOCALAPPDATA="+localAppData,
		"TEST_INSTALL_SCRIPT="+scriptPath,
		"TEST_REPO="+repo,
		"TEST_INSTALL_DIR="+installDir,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.ps1 should make cxp available in the current PowerShell process: %v\n%s", err, string(output))
	}
}

func TestInstallPs1ProfileScriptAllowsBareCxpInNewPowerShell(t *testing.T) {
	if _, err := exec.LookPath("powershell"); err != nil {
		t.Skip("powershell not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.ps1")

	repo := "owner/name"
	tag := "v1.2.3"
	verNoV := strings.TrimPrefix(tag, "v")
	asset := fmt.Sprintf("codex-proxy_%s_windows_amd64.exe", verNoV)
	assetData := buildWindowsCodexProxyAsset(t)
	checksum := sha256.Sum256(assetData)

	server := newInstallServer(t, repo, tag, asset, assetData, false, checksum)
	defer server.Close()

	installDir := t.TempDir()
	managedPrefix := t.TempDir()
	tempDir := t.TempDir()
	userProfile := t.TempDir()
	appData := filepath.Join(userProfile, "AppData", "Roaming")
	localAppData := filepath.Join(userProfile, "AppData", "Local")
	profilePath := filepath.Join(t.TempDir(), "profile.ps1")

	basePath := os.Getenv("SystemRoot")
	if basePath == "" {
		basePath = `C:\Windows`
	}
	pathValue := filepath.Join(basePath, "System32")

	baseEnv := filterEnvWithoutKeys(os.Environ(), "Path", "USERPROFILE", "APPDATA", "LOCALAPPDATA")
	baseEnv = append(baseEnv,
		"CODEX_PROXY_API_BASE="+server.URL,
		"CODEX_PROXY_RELEASE_BASE="+server.URL,
		"CODEX_PROXY_PROFILE_PATH="+profilePath,
		"CODEX_PROXY_SKIP_PATH_UPDATE=1",
		"CODEX_NPM_PREFIX="+managedPrefix,
		"Path="+pathValue,
		"TEMP="+tempDir,
		"USERPROFILE="+userProfile,
		"APPDATA="+appData,
		"LOCALAPPDATA="+localAppData,
	)

	installCmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", scriptPath,
		"-Repo", repo,
		"-Version", "latest",
		"-InstallDir", installDir,
	)
	installCmd.Env = baseEnv
	output, err := installCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.ps1 failed before profile startup smoke: %v\n%s", err, string(output))
	}

	psScript := `
$ErrorActionPreference = "Stop"
. $env:TEST_PROFILE_PATH
$cmd = Get-Command cxp -ErrorAction Stop
$version = (& cxp --version | Out-String).Trim()
if ($version -ne "codex-proxy 1.2.3") {
  throw "unexpected cxp version output: $version"
}
`
	profileCmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", psScript)
	profileCmd.Env = append([]string{}, baseEnv...)
	profileCmd.Env = append(profileCmd.Env, "TEST_PROFILE_PATH="+profilePath)
	output, err = profileCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("installer-written PowerShell profile should make cxp available in a new process: %v\n%s", err, string(output))
	}
}

func TestInstallPs1ChecksumDownloadFailureRemainsBestEffort(t *testing.T) {
	if _, err := exec.LookPath("powershell"); err != nil {
		t.Skip("powershell not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.ps1")

	repo := "owner/name"
	tag := "v1.2.3"
	verNoV := strings.TrimPrefix(tag, "v")
	asset := fmt.Sprintf("codex-proxy_%s_windows_amd64.exe", verNoV)
	assetData := []byte("fake-binary")
	checksum := sha256.Sum256(assetData)

	server := newInstallServerWithChecksumStatus(t, repo, tag, asset, assetData, false, checksum, http.StatusNotFound)
	defer server.Close()

	installDir := t.TempDir()
	managedPrefix := t.TempDir()
	tempDir := t.TempDir()
	profilePath := filepath.Join(t.TempDir(), "profile.ps1")

	basePath := os.Getenv("SystemRoot")
	if basePath == "" {
		basePath = `C:\Windows`
	}
	pathValue := filepath.Join(basePath, "System32")

	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", scriptPath,
		"-Repo", repo,
		"-Version", "latest",
		"-InstallDir", installDir,
	)
	cmd.Env = isolatedWindowsInstallEnv(t, filterEnvWithoutKey(os.Environ(), "Path"))
	cmd.Env = append(cmd.Env,
		"CODEX_PROXY_API_BASE="+server.URL,
		"CODEX_PROXY_RELEASE_BASE="+server.URL,
		"CODEX_PROXY_PROFILE_PATH="+profilePath,
		"CODEX_PROXY_SKIP_PATH_UPDATE=1",
		"CODEX_NPM_PREFIX="+managedPrefix,
		"Path="+pathValue,
		"TEMP="+tempDir,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.ps1 should ignore checksum download failure: %v\n%s", err, string(output))
	}
	text := string(output)
	if !strings.Contains(text, "CODEX-PROXY INSTALL SUCCESS") {
		t.Fatalf("expected success banner, got %s", text)
	}
	installed := filepath.Join(installDir, "codex-proxy.exe")
	got, err := os.ReadFile(installed)
	if err != nil {
		t.Fatalf("read installed binary: %v", err)
	}
	if !bytes.Equal(got, assetData) {
		t.Fatalf("installed payload mismatch")
	}
}

func TestInstallPs1ChecksumMismatchFailsBeforeReplacingBinaries(t *testing.T) {
	assetData := []byte("new-fake-binary")
	wrongChecksum := sha256.Sum256([]byte("different-binary"))
	server := newInstallServer(t, "owner/name", "v1.2.3", "codex-proxy_1.2.3_windows_amd64.exe", assetData, false, wrongChecksum)
	defer server.Close()

	installDir := t.TempDir()
	tempDir := t.TempDir()
	oldCodex := []byte("old-codex-proxy")
	oldCXP := []byte("old-cxp")
	for name, data := range map[string][]byte{"codex-proxy.exe": oldCodex, "cxp.exe": oldCXP} {
		if err := os.WriteFile(filepath.Join(installDir, name), data, 0o600); err != nil {
			t.Fatalf("write old %s: %v", name, err)
		}
	}

	output, err := runInstallPs1ForChecksumTest(t, server.URL, installDir, tempDir)
	if err == nil {
		t.Fatalf("install.ps1 unexpectedly accepted an incorrect checksum:\n%s", output)
	}
	if strings.Contains(output, "CODEX-PROXY INSTALL SUCCESS") || !strings.Contains(output, "Checksum mismatch") {
		t.Fatalf("unexpected checksum failure output:\n%s", output)
	}
	for name, want := range map[string][]byte{"codex-proxy.exe": oldCodex, "cxp.exe": oldCXP} {
		got, readErr := os.ReadFile(filepath.Join(installDir, name))
		if readErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s changed after checksum mismatch: got=%q err=%v", name, got, readErr)
		}
	}
	assertInstallTempDirEmpty(t, tempDir)
}

func TestInstallPs1ChecksumWithoutTargetAssetRowRemainsBestEffort(t *testing.T) {
	asset := "codex-proxy_1.2.3_windows_amd64.exe"
	assetData := []byte("fake-binary")
	otherChecksum := sha256.Sum256([]byte("other-asset"))
	server := newInstallServerWithChecksumManifest(t, "owner/name", "v1.2.3", asset, assetData, false, fmt.Sprintf("%x  another-asset.exe\n", otherChecksum), http.StatusOK)
	defer server.Close()

	installDir := t.TempDir()
	tempDir := t.TempDir()
	output, err := runInstallPs1ForChecksumTest(t, server.URL, installDir, tempDir)
	if err != nil {
		t.Fatalf("install.ps1 should keep a missing target row best-effort: %v\n%s", err, output)
	}
	if !strings.Contains(output, "CODEX-PROXY INSTALL SUCCESS") {
		t.Fatalf("expected success banner, got:\n%s", output)
	}
	got, err := os.ReadFile(filepath.Join(installDir, "codex-proxy.exe"))
	if err != nil || !bytes.Equal(got, assetData) {
		t.Fatalf("installed payload = %q err=%v", got, err)
	}
	assertInstallTempDirEmpty(t, tempDir)
}

func TestInstallPs1ConcurrentDownloadsUseIsolatedTemporaryPaths(t *testing.T) {
	assetData := []byte("concurrent-fake-binary")
	checksum := sha256.Sum256(assetData)
	server := newInstallServer(t, "owner/name", "v1.2.3", "codex-proxy_1.2.3_windows_amd64.exe", assetData, false, checksum)
	defer server.Close()

	tempDir := t.TempDir()
	firstInstallDir := t.TempDir()
	secondInstallDir := t.TempDir()
	firstCmd := newInstallPs1ChecksumCommand(t, server.URL, firstInstallDir, tempDir)
	secondCmd := newInstallPs1ChecksumCommand(t, server.URL, secondInstallDir, tempDir)
	type result struct {
		output []byte
		err    error
	}
	results := make(chan result, 2)
	go func() {
		output, err := firstCmd.CombinedOutput()
		results <- result{output: output, err: err}
	}()
	go func() {
		output, err := secondCmd.CombinedOutput()
		results <- result{output: output, err: err}
	}()
	for index := 0; index < 2; index++ {
		got := <-results
		if got.err != nil || !strings.Contains(string(got.output), "CODEX-PROXY INSTALL SUCCESS") {
			t.Fatalf("concurrent install failed: %v\n%s", got.err, got.output)
		}
	}
	for _, installDir := range []string{firstInstallDir, secondInstallDir} {
		got, err := os.ReadFile(filepath.Join(installDir, "codex-proxy.exe"))
		if err != nil || !bytes.Equal(got, assetData) {
			t.Fatalf("installed payload in %s = %q err=%v", installDir, got, err)
		}
	}
	assertInstallTempDirEmpty(t, tempDir)
}

func runInstallPs1ForChecksumTest(t *testing.T, serverURL, installDir, tempDir string) (string, error) {
	t.Helper()
	cmd := newInstallPs1ChecksumCommand(t, serverURL, installDir, tempDir)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func newInstallPs1ChecksumCommand(t *testing.T, serverURL, installDir, tempDir string) *exec.Cmd {
	t.Helper()
	if _, err := exec.LookPath("powershell"); err != nil {
		t.Skip("powershell not available")
	}
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	profilePath := filepath.Join(t.TempDir(), "profile.ps1")
	basePath := os.Getenv("SystemRoot")
	if basePath == "" {
		basePath = `C:\Windows`
	}
	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", filepath.Join(repoRoot, "install.ps1"),
		"-Repo", "owner/name", "-Version", "latest", "-InstallDir", installDir)
	cmd.Env = isolatedWindowsInstallEnv(t, filterEnvWithoutKey(os.Environ(), "Path"))
	cmd.Env = append(cmd.Env,
		"CODEX_PROXY_API_BASE="+serverURL,
		"CODEX_PROXY_RELEASE_BASE="+serverURL,
		"CODEX_PROXY_PROFILE_PATH="+profilePath,
		"CODEX_PROXY_SKIP_PATH_UPDATE=1",
		"CODEX_NPM_PREFIX="+t.TempDir(),
		"Path="+filepath.Join(basePath, "System32"),
		"TEMP="+tempDir,
	)
	return cmd
}

func assertInstallTempDirEmpty(t *testing.T, tempDir string) {
	t.Helper()
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read installer temporary directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("installer temporary files remain after completion: %v", entries)
	}
}

func TestInstallPs1DiskSpaceFailureBanner(t *testing.T) {
	if _, err := exec.LookPath("powershell"); err != nil {
		t.Skip("powershell not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.ps1")

	installDir := t.TempDir()
	managedPrefix := t.TempDir()
	tempDir := t.TempDir()
	profilePath := filepath.Join(t.TempDir(), "profile.ps1")

	basePath := os.Getenv("SystemRoot")
	if basePath == "" {
		basePath = `C:\Windows`
	}
	pathValue := filepath.Join(basePath, "System32")

	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", scriptPath,
		"-Repo", "owner/name",
		"-Version", "v1.2.3",
		"-InstallDir", installDir,
	)
	cmd.Env = isolatedWindowsInstallEnv(t, filterEnvWithoutKey(os.Environ(), "Path"))
	cmd.Env = append(cmd.Env,
		"CODEX_PROXY_PROFILE_PATH="+profilePath,
		"CODEX_PROXY_SKIP_PATH_UPDATE=1",
		"CODEX_NPM_PREFIX="+managedPrefix,
		"CODEX_PROXY_INSTALL_MIN_FREE_KB=999999999999",
		"Path="+pathValue,
		"TEMP="+tempDir,
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected disk space failure, got success:\n%s", string(output))
	}
	text := string(output)
	if !strings.Contains(text, "CODEX-PROXY INSTALL FAILED") {
		t.Fatalf("expected failure banner, got %s", text)
	}
	if !strings.Contains(text, "Not enough disk space") {
		t.Fatalf("expected disk space reason, got %s", text)
	}
}

func TestInstallPs1RemovesLegacyCodexClpExe(t *testing.T) {
	if _, err := exec.LookPath("powershell"); err != nil {
		t.Skip("powershell not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.ps1")

	repo := "owner/name"
	tag := "v1.2.3"
	verNoV := strings.TrimPrefix(tag, "v")
	asset := fmt.Sprintf("codex-proxy_%s_windows_amd64.exe", verNoV)
	assetData := []byte("fake-binary")
	checksum := sha256.Sum256(assetData)

	server := newInstallServer(t, repo, tag, asset, assetData, false, checksum)
	defer server.Close()

	installDir := t.TempDir()
	managedPrefix := t.TempDir()
	tempDir := t.TempDir()
	profilePath := filepath.Join(t.TempDir(), "profile.ps1")

	legacyClpExe := filepath.Join(installDir, "clp.exe")
	legacyClpExeData := []byte("codex-proxy legacy clp exe")
	if err := os.WriteFile(legacyClpExe, legacyClpExeData, 0o644); err != nil {
		t.Fatalf("write clp.exe: %v", err)
	}
	legacyClpCmd := filepath.Join(installDir, "clp.cmd")
	legacyClpCmdData := []byte("@echo off\r\n\"%~dp0codex-proxy.exe\" %*\r\n")
	if err := os.WriteFile(legacyClpCmd, legacyClpCmdData, 0o644); err != nil {
		t.Fatalf("write clp.cmd: %v", err)
	}

	basePath := os.Getenv("SystemRoot")
	if basePath == "" {
		basePath = `C:\Windows`
	}
	pathValue := filepath.Join(basePath, "System32")

	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", scriptPath,
		"-Repo", repo,
		"-Version", "latest",
		"-InstallDir", installDir,
	)
	cmd.Env = isolatedWindowsInstallEnv(t, filterEnvWithoutKey(os.Environ(), "Path"))
	cmd.Env = append(cmd.Env,
		"CODEX_PROXY_API_BASE="+server.URL,
		"CODEX_PROXY_RELEASE_BASE="+server.URL,
		"CODEX_PROXY_PROFILE_PATH="+profilePath,
		"CODEX_PROXY_SKIP_PATH_UPDATE=1",
		"CODEX_NPM_PREFIX="+managedPrefix,
		"Path="+pathValue,
		"TEMP="+tempDir,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.ps1 failed: %v\n%s", err, string(output))
	}

	if _, err := os.Stat(legacyClpExe); !os.IsNotExist(err) {
		t.Fatalf("expected legacy clp.exe removed, stat err=%v", err)
	}
	if _, err := os.Stat(legacyClpCmd); !os.IsNotExist(err) {
		t.Fatalf("expected legacy clp.cmd removed, stat err=%v", err)
	}
}

func TestInstallPs1RemovesLegacyCodexClaudeProxyAndClpCmd(t *testing.T) {
	if _, err := exec.LookPath("powershell"); err != nil {
		t.Skip("powershell not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.ps1")

	repo := "owner/name"
	tag := "v1.2.3"
	verNoV := strings.TrimPrefix(tag, "v")
	asset := fmt.Sprintf("codex-proxy_%s_windows_amd64.exe", verNoV)
	assetData := []byte("fake-binary")
	checksum := sha256.Sum256(assetData)

	server := newInstallServer(t, repo, tag, asset, assetData, false, checksum)
	defer server.Close()

	installDir := t.TempDir()
	managedPrefix := t.TempDir()
	tempDir := t.TempDir()
	profilePath := filepath.Join(t.TempDir(), "profile.ps1")

	legacyClaudeProxyExe := filepath.Join(installDir, "claude-proxy.exe")
	legacyClaudeProxyExeData := []byte("github.com/baaaaaaaka/codex-helper claude-proxy legacy exe")
	if err := os.WriteFile(legacyClaudeProxyExe, legacyClaudeProxyExeData, 0o644); err != nil {
		t.Fatalf("write claude-proxy.exe: %v", err)
	}
	legacyClpCmd := filepath.Join(installDir, "clp.cmd")
	legacyClpCmdData := []byte("@echo off\r\n\"%~dp0claude-proxy.exe\" %*\r\n")
	if err := os.WriteFile(legacyClpCmd, legacyClpCmdData, 0o644); err != nil {
		t.Fatalf("write clp.cmd: %v", err)
	}

	basePath := os.Getenv("SystemRoot")
	if basePath == "" {
		basePath = `C:\Windows`
	}
	pathValue := filepath.Join(basePath, "System32")

	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", scriptPath,
		"-Repo", repo,
		"-Version", "latest",
		"-InstallDir", installDir,
	)
	cmd.Env = isolatedWindowsInstallEnv(t, filterEnvWithoutKey(os.Environ(), "Path"))
	cmd.Env = append(cmd.Env,
		"CODEX_PROXY_API_BASE="+server.URL,
		"CODEX_PROXY_RELEASE_BASE="+server.URL,
		"CODEX_PROXY_PROFILE_PATH="+profilePath,
		"CODEX_PROXY_SKIP_PATH_UPDATE=1",
		"CODEX_NPM_PREFIX="+managedPrefix,
		"Path="+pathValue,
		"TEMP="+tempDir,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.ps1 failed: %v\n%s", err, string(output))
	}

	if _, err := os.Stat(legacyClaudeProxyExe); !os.IsNotExist(err) {
		t.Fatalf("expected legacy claude-proxy.exe removed, stat err=%v", err)
	}
	if _, err := os.Stat(legacyClpCmd); !os.IsNotExist(err) {
		t.Fatalf("expected legacy clp.cmd removed, stat err=%v", err)
	}
}

func TestInstallPs1PreservesExternalClpCmdReferencingClaudeProxy(t *testing.T) {
	if _, err := exec.LookPath("powershell"); err != nil {
		t.Skip("powershell not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.ps1")

	repo := "owner/name"
	tag := "v1.2.3"
	verNoV := strings.TrimPrefix(tag, "v")
	asset := fmt.Sprintf("codex-proxy_%s_windows_amd64.exe", verNoV)
	assetData := []byte("fake-binary")
	checksum := sha256.Sum256(assetData)

	server := newInstallServer(t, repo, tag, asset, assetData, false, checksum)
	defer server.Close()

	installDir := t.TempDir()
	managedPrefix := t.TempDir()
	tempDir := t.TempDir()
	profilePath := filepath.Join(t.TempDir(), "profile.ps1")

	legacyClaudeProxyExe := filepath.Join(installDir, "claude-proxy.exe")
	legacyClaudeProxyExeData := []byte("external claude-proxy exe")
	if err := os.WriteFile(legacyClaudeProxyExe, legacyClaudeProxyExeData, 0o644); err != nil {
		t.Fatalf("write claude-proxy.exe: %v", err)
	}
	legacyClpCmd := filepath.Join(installDir, "clp.cmd")
	legacyClpCmdData := []byte("@echo off\r\n\"%~dp0claude-proxy.exe\" %*\r\n")
	if err := os.WriteFile(legacyClpCmd, legacyClpCmdData, 0o644); err != nil {
		t.Fatalf("write clp.cmd: %v", err)
	}

	basePath := os.Getenv("SystemRoot")
	if basePath == "" {
		basePath = `C:\Windows`
	}
	pathValue := filepath.Join(basePath, "System32")

	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", scriptPath,
		"-Repo", repo,
		"-Version", "latest",
		"-InstallDir", installDir,
	)
	cmd.Env = isolatedWindowsInstallEnv(t, filterEnvWithoutKey(os.Environ(), "Path"))
	cmd.Env = append(cmd.Env,
		"CODEX_PROXY_API_BASE="+server.URL,
		"CODEX_PROXY_RELEASE_BASE="+server.URL,
		"CODEX_PROXY_PROFILE_PATH="+profilePath,
		"CODEX_PROXY_SKIP_PATH_UPDATE=1",
		"CODEX_NPM_PREFIX="+managedPrefix,
		"Path="+pathValue,
		"TEMP="+tempDir,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.ps1 failed: %v\n%s", err, string(output))
	}

	claudeProxyExeData, err := os.ReadFile(legacyClaudeProxyExe)
	if err != nil {
		t.Fatalf("read claude-proxy.exe: %v", err)
	}
	if !bytes.Equal(claudeProxyExeData, legacyClaudeProxyExeData) {
		t.Fatalf("expected external claude-proxy.exe preserved")
	}
	clpCmdData, err := os.ReadFile(legacyClpCmd)
	if err != nil {
		t.Fatalf("read clp.cmd: %v", err)
	}
	if !bytes.Equal(clpCmdData, legacyClpCmdData) {
		t.Fatalf("expected clp.cmd referencing external claude-proxy.exe preserved")
	}
}

func buildWindowsCodexProxyAsset(t *testing.T) []byte {
	t.Helper()
	workDir := t.TempDir()
	sourcePath := filepath.Join(workDir, "main.go")
	exePath := filepath.Join(workDir, "codex-proxy.exe")
	source := `package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("codex-proxy 1.2.3")
	}
}
`
	if err := os.WriteFile(sourcePath, []byte(source), 0o644); err != nil {
		t.Fatalf("write fake codex-proxy source: %v", err)
	}
	cmd := exec.Command("go", "build", "-o", exePath, sourcePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build fake codex-proxy.exe: %v\n%s", err, string(output))
	}
	data, err := os.ReadFile(exePath)
	if err != nil {
		t.Fatalf("read fake codex-proxy.exe: %v", err)
	}
	return data
}

func runInstallPs1(t *testing.T, apiFail bool, pathAlreadySet bool) {
	t.Helper()
	if _, err := exec.LookPath("powershell"); err != nil {
		t.Skip("powershell not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.ps1")

	repo := "owner/name"
	tag := "v1.2.3"
	verNoV := strings.TrimPrefix(tag, "v")
	asset := fmt.Sprintf("codex-proxy_%s_windows_amd64.exe", verNoV)
	assetData := []byte("fake-binary")
	checksum := sha256.Sum256(assetData)

	server := newInstallServer(t, repo, tag, asset, assetData, apiFail, checksum)
	defer server.Close()

	installDir := t.TempDir()
	managedPrefix := t.TempDir()
	tempDir := t.TempDir()
	userProfile := t.TempDir()
	appData := filepath.Join(userProfile, "AppData", "Roaming")
	localAppData := filepath.Join(userProfile, "AppData", "Local")
	profilePath := filepath.Join(t.TempDir(), "profile.ps1")
	legacyClaudeProxyExe := filepath.Join(installDir, "claude-proxy.exe")
	legacyClaudeProxyExeData := []byte("external claude-proxy exe")
	if err := os.WriteFile(legacyClaudeProxyExe, legacyClaudeProxyExeData, 0o644); err != nil {
		t.Fatalf("write claude-proxy.exe: %v", err)
	}
	legacyClpExe := filepath.Join(installDir, "clp.exe")
	legacyClpExeData := []byte("external clp exe")
	if err := os.WriteFile(legacyClpExe, legacyClpExeData, 0o644); err != nil {
		t.Fatalf("write clp.exe: %v", err)
	}
	legacyClpCmd := filepath.Join(installDir, "clp.cmd")
	legacyClpCmdData := []byte("@echo off\r\necho external clp\r\n")
	if err := os.WriteFile(legacyClpCmd, legacyClpCmdData, 0o644); err != nil {
		t.Fatalf("write clp.cmd: %v", err)
	}
	basePath := os.Getenv("SystemRoot")
	if basePath == "" {
		basePath = `C:\Windows`
	}
	pathValue := filepath.Join(basePath, "System32")
	if pathAlreadySet {
		pathValue = installDir + ";" + pathValue
	}
	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", scriptPath,
		"-Repo", repo,
		"-Version", "latest",
		"-InstallDir", installDir,
	)
	cmd.Env = filterEnvWithoutKeys(os.Environ(), "Path", "USERPROFILE", "APPDATA", "LOCALAPPDATA")
	cmd.Env = append(cmd.Env,
		"CODEX_PROXY_API_BASE="+server.URL,
		"CODEX_PROXY_RELEASE_BASE="+server.URL,
		"CODEX_PROXY_PROFILE_PATH="+profilePath,
		"CODEX_PROXY_SKIP_PATH_UPDATE=1",
		"CODEX_NPM_PREFIX="+managedPrefix,
		"Path="+pathValue,
		"TEMP="+tempDir,
		"USERPROFILE="+userProfile,
		"APPDATA="+appData,
		"LOCALAPPDATA="+localAppData,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.ps1 failed: %v\n%s", err, string(output))
	}
	if !strings.Contains(string(output), "CODEX-PROXY INSTALL SUCCESS") {
		t.Fatalf("expected success banner, got %s", string(output))
	}
	installDirResolved := resolvePathViaPowerShell(t, cmd.Env, installDir)
	managedPrefixResolved := resolvePathViaPowerShell(t, cmd.Env, managedPrefix)
	managedBinResolved := resolvePathViaPowerShell(t, cmd.Env, filepath.Join(managedPrefix, "bin"))

	installed := filepath.Join(installDir, "codex-proxy.exe")
	got, err := os.ReadFile(installed)
	if err != nil {
		t.Fatalf("read installed: %v", err)
	}
	if !bytes.Equal(got, assetData) {
		t.Fatalf("installed payload mismatch")
	}
	cxpExe := filepath.Join(installDir, "cxp.exe")
	cxpExeData, err := os.ReadFile(cxpExe)
	if err != nil {
		t.Fatalf("read cxp.exe: %v", err)
	}
	if !bytes.Equal(cxpExeData, assetData) {
		t.Fatalf("cxp.exe payload mismatch")
	}
	cxpCmd := filepath.Join(installDir, "cxp.cmd")
	cmdData, err := os.ReadFile(cxpCmd)
	if err != nil {
		t.Fatalf("read cxp.cmd: %v", err)
	}
	cmdText := strings.ReplaceAll(string(cmdData), "\r\n", "\n")
	cmdText = strings.TrimRight(cmdText, "\n")
	wantCxpCmd := "@echo off\n\"%~dp0cxp.exe\" %*"
	if cmdText != wantCxpCmd {
		t.Fatalf("cxp.cmd content mismatch\ngot:\n%q\nwant:\n%q", cmdText, wantCxpCmd)
	}
	recordData, err := os.ReadFile(filepath.Join(appData, "codex-helper", "install.json"))
	if err != nil {
		t.Fatalf("read install record: %v", err)
	}
	if len(recordData) >= 3 && recordData[0] == 0xef && recordData[1] == 0xbb && recordData[2] == 0xbf {
		t.Fatalf("install record should be UTF-8 without BOM")
	}
	var record struct {
		SchemaVersion int      `json:"schema_version"`
		TargetPath    string   `json:"target_path"`
		TargetSource  string   `json:"target_source"`
		TargetState   string   `json:"target_state"`
		Repo          string   `json:"repo"`
		Version       string   `json:"version"`
		GOOS          string   `json:"goos"`
		GOARCH        string   `json:"goarch"`
		Shims         []string `json:"shims"`
	}
	if err := json.Unmarshal(recordData, &record); err != nil {
		t.Fatalf("parse install record: %v\n%s", err, recordData)
	}
	expectedTarget := filepath.Join(installDirResolved, "codex-proxy.exe")
	expectedShim := filepath.Join(installDirResolved, "cxp.exe")
	if record.SchemaVersion != 1 ||
		record.TargetPath != expectedTarget ||
		record.TargetSource != "installer" ||
		record.TargetState != "managed" ||
		record.Repo != "owner/name" ||
		record.Version != "v1.2.3" ||
		record.GOOS != "windows" ||
		record.GOARCH != "amd64" ||
		!stringSliceContains(record.Shims, expectedShim) {
		t.Fatalf("unexpected install record: %#v", record)
	}
	claudeProxyExeData, err := os.ReadFile(legacyClaudeProxyExe)
	if err != nil {
		t.Fatalf("read claude-proxy.exe: %v", err)
	}
	if !bytes.Equal(claudeProxyExeData, legacyClaudeProxyExeData) {
		t.Fatalf("expected claude-proxy.exe preserved")
	}
	clpExeData, err := os.ReadFile(legacyClpExe)
	if err != nil {
		t.Fatalf("read clp.exe: %v", err)
	}
	if !bytes.Equal(clpExeData, legacyClpExeData) {
		t.Fatalf("expected clp.exe preserved")
	}
	clpCmdData, err := os.ReadFile(legacyClpCmd)
	if err != nil {
		t.Fatalf("read clp.cmd: %v", err)
	}
	if !bytes.Equal(clpCmdData, legacyClpCmdData) {
		t.Fatalf("expected clp.cmd preserved")
	}

	profile, err := os.ReadFile(profilePath)
	if err != nil {
		t.Fatalf("read profile: %v", err)
	}
	profileText := string(profile)
	if !strings.Contains(profileText, "Set-Alias -Name cxp -Value cxp.exe") {
		t.Fatalf("missing cxp alias in profile")
	}
	if strings.Contains(strings.ToLower(profileText), "clp") {
		t.Fatalf("unexpected clp reference in profile")
	}
	for _, dir := range []string{installDirResolved, managedPrefixResolved, managedBinResolved} {
		if !hasPathLineForDir(profileText, dir) {
			t.Fatalf("missing PATH update in profile for %s", dir)
		}
	}
}

func hasPathLineForDir(profileText, installDir string) bool {
	for _, line := range strings.Split(profileText, "\n") {
		if !strings.Contains(line, "$env:Path") {
			continue
		}
		if strings.Contains(strings.ToLower(line), strings.ToLower(installDir)) {
			return true
		}
	}
	return false
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func resolvePathViaPowerShell(t *testing.T, env []string, installDir string) string {
	t.Helper()
	script := `[IO.Path]::GetFullPath($env:TEST_INSTALL_DIR)`
	cmd := exec.Command("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script)
	cmd.Env = append([]string{}, env...)
	cmd.Env = append(cmd.Env, "TEST_INSTALL_DIR="+installDir)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("resolve path: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func isolatedWindowsInstallEnv(t *testing.T, env []string) []string {
	t.Helper()
	userProfile := t.TempDir()
	return append(filterEnvWithoutKeys(env, "USERPROFILE", "APPDATA", "LOCALAPPDATA"),
		"USERPROFILE="+userProfile,
		"APPDATA="+filepath.Join(userProfile, "AppData", "Roaming"),
		"LOCALAPPDATA="+filepath.Join(userProfile, "AppData", "Local"),
	)
}

func filterEnvWithoutKey(env []string, key string) []string {
	return filterEnvWithoutKeys(env, key)
}

func filterEnvWithoutKeys(env []string, keys ...string) []string {
	blocked := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		blocked[strings.ToLower(key)] = struct{}{}
	}
	out := make([]string, 0, len(env))
	for _, kv := range env {
		k, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if _, ok := blocked[strings.ToLower(k)]; ok {
			continue
		}
		out = append(out, kv)
	}
	return out
}

func newInstallServer(
	t *testing.T,
	repo string,
	tag string,
	asset string,
	assetData []byte,
	apiFail bool,
	checksum [32]byte,
) *httptest.Server {
	return newInstallServerWithChecksumStatus(t, repo, tag, asset, assetData, apiFail, checksum, http.StatusOK)
}

func newInstallServerWithChecksumStatus(
	t *testing.T,
	repo string,
	tag string,
	asset string,
	assetData []byte,
	apiFail bool,
	checksum [32]byte,
	checksumStatus int,
) *httptest.Server {
	return newInstallServerWithChecksumManifest(t, repo, tag, asset, assetData, apiFail, fmt.Sprintf("%x  %s\n", checksum, asset), checksumStatus)
}

func newInstallServerWithChecksumManifest(
	t *testing.T,
	repo string,
	tag string,
	asset string,
	assetData []byte,
	apiFail bool,
	checksumManifest string,
	checksumStatus int,
) *httptest.Server {
	t.Helper()
	apiPath := "/repos/" + repo + "/releases/latest"
	latestPath := "/" + repo + "/releases/latest"
	tagPath := "/" + repo + "/releases/tag/" + tag
	assetPath := "/" + repo + "/releases/download/" + tag + "/" + asset
	checksumsPath := "/" + repo + "/releases/download/" + tag + "/checksums.txt"

	handler := func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == apiPath:
			if apiFail {
				http.Error(w, "api unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"tag_name": "%s"}`, tag)
		case r.URL.Path == latestPath:
			http.Redirect(w, r, tagPath, http.StatusFound)
		case r.URL.Path == tagPath:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		case r.URL.Path == assetPath:
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(assetData)
		case r.URL.Path == checksumsPath:
			if checksumStatus != http.StatusOK {
				http.Error(w, "checksums unavailable", checksumStatus)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			_, _ = fmt.Fprint(w, checksumManifest)
		default:
			http.NotFound(w, r)
		}
	}

	return httptest.NewServer(http.HandlerFunc(handler))
}
