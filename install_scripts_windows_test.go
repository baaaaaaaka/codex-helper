//go:build windows

package installtest

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallPs1LatestViaAPI(t *testing.T) {
	runInstallPs1(t, false, false)
}

func TestInstallPs1LatestViaRedirect(t *testing.T) {
	runInstallPs1(t, true, false)
}

func TestInstallPs1KeepsPathSetupWhenInstallDirAlreadySet(t *testing.T) {
	runInstallPs1(t, false, true)
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
	cmd.Env = append([]string{}, filterEnvWithoutKey(os.Environ(), "Path")...)
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
	cmd.Env = append([]string{}, filterEnvWithoutKey(os.Environ(), "Path")...)
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
	cmd.Env = append([]string{}, filterEnvWithoutKey(os.Environ(), "Path")...)
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
	cmd.Env = append([]string{}, filterEnvWithoutKey(os.Environ(), "Path")...)
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
	cxpCmd := filepath.Join(installDir, "cxp.cmd")
	cmdData, err := os.ReadFile(cxpCmd)
	if err != nil {
		t.Fatalf("read cxp.cmd: %v", err)
	}
	if !strings.Contains(strings.ToLower(string(cmdData)), "codex-proxy.exe") {
		t.Fatalf("cxp.cmd does not reference codex-proxy.exe")
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
	if !strings.Contains(profileText, "Set-Alias -Name cxp -Value codex-proxy") {
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

func filterEnvWithoutKey(env []string, key string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		k, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if strings.EqualFold(k, key) {
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
			w.Header().Set("Content-Type", "text/plain")
			_, _ = fmt.Fprintf(w, "%x  %s\n", checksum, asset)
		default:
			http.NotFound(w, r)
		}
	}

	return httptest.NewServer(http.HandlerFunc(handler))
}
