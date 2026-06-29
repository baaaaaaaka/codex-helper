//go:build !windows

package installtest

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestInstallShLatestUsesRedirectBeforeAPI(t *testing.T) {
	run := newInstallShRun(t, false, false)
	apiHitsPath := filepath.Join(t.TempDir(), "api-hits.txt")
	run.env = overrideEnv(run.env, "CODEX_PROXY_TEST_API_HITS", apiHitsPath)

	runInstallShCommand(t, run)

	assertFileMissingOrEmpty(t, apiHitsPath)
}

func TestInstallShLatestFallsBackToAPIWhenRedirectUnavailable(t *testing.T) {
	run := newInstallShRun(t, false, false)
	apiHitsPath := filepath.Join(t.TempDir(), "api-hits.txt")
	run.env = overrideEnv(run.env, "CODEX_PROXY_TEST_API_HITS", apiHitsPath)
	run.env = overrideEnv(run.env, "CODEX_PROXY_TEST_LATEST_URL", "https://github.com/owner/name/releases/latest")

	runInstallShCommand(t, run)

	assertFileContains(t, apiHitsPath, "api\n")
}

func TestInstallShKeepsPathSetupWhenInstallDirAlreadySet(t *testing.T) {
	runInstallSh(t, false, true)
}

func TestInstallShChecksumMismatch(t *testing.T) {
	if _, err := exec.LookPath("sha256sum"); err != nil {
		if _, err := exec.LookPath("shasum"); err != nil {
			t.Skip("no checksum tool available")
		}
	}

	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "CODEX_PROXY_TEST_CHECKSUMS", strings.Repeat("0", 64)+"  "+run.asset+"\n")

	cmd := exec.Command("sh", run.scriptPath)
	cmd.Dir = run.repoRoot
	cmd.Env = run.env
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected checksum mismatch error")
	}
	if !strings.Contains(string(output), "Checksum mismatch") {
		t.Fatalf("expected checksum mismatch output, got %s", string(output))
	}
	if !strings.Contains(string(output), "CODEX-PROXY INSTALL FAILED") {
		t.Fatalf("expected failure banner, got %s", string(output))
	}
}

func TestInstallShSuccessBanner(t *testing.T) {
	run := newInstallShRun(t, false, false)

	cmd := exec.Command("sh", run.scriptPath)
	cmd.Dir = run.repoRoot
	cmd.Env = run.env
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh failed: %v\n%s", err, string(output))
	}
	text := string(output)
	if !strings.Contains(text, "CODEX-PROXY INSTALL SUCCESS") {
		t.Fatalf("expected success banner, got %s", text)
	}
	if !strings.Contains(text, "Installed: "+filepath.Join(run.installDir, "codex-proxy")) {
		t.Fatalf("expected installed path in success output, got %s", text)
	}
	if strings.Contains(text, "reload attempted") {
		t.Fatalf("success output should not claim shell config reload was attempted, got %s", text)
	}
	if !strings.Contains(text, "current shell PATH was not reloaded automatically") {
		t.Fatalf("expected current-shell activation guidance, got %s", text)
	}
}

func TestInstallShPreservesDirectoryAndMultiHopFileSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer symlink topology")
	}
	run := newInstallShRun(t, false, false)
	logicalInstallDir := run.installDir
	physicalInstallDir := filepath.Join(t.TempDir(), "physical install with spaces")
	externalStorage := filepath.Join(t.TempDir(), "external payloads")
	if err := os.RemoveAll(logicalInstallDir); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{physicalInstallDir, externalStorage} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(physicalInstallDir, logicalInstallDir); err != nil {
		t.Fatal(err)
	}
	realCXP := filepath.Join(externalStorage, "cxp-payload")
	realLegacy := filepath.Join(externalStorage, "legacy-payload")
	for _, path := range []string{realCXP, realLegacy} {
		if err := os.WriteFile(path, []byte("#!/bin/sh\necho old\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cxpHop := filepath.Join(physicalInstallDir, "cxp-hop")
	legacyHop := filepath.Join(physicalInstallDir, "legacy-hop")
	if err := os.Symlink(realCXP, cxpHop); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realLegacy, legacyHop); err != nil {
		t.Fatal(err)
	}
	cxpEntry := filepath.Join(physicalInstallDir, "cxp")
	legacyEntry := filepath.Join(physicalInstallDir, "codex-proxy")
	if err := os.Symlink("cxp-hop", cxpEntry); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("legacy-hop", legacyEntry); err != nil {
		t.Fatal(err)
	}

	links := []string{logicalInstallDir, cxpEntry, cxpHop, legacyEntry, legacyHop}
	before := make(map[string]string, len(links))
	for _, path := range links {
		target, err := os.Readlink(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = target
	}

	runInstallShCommand(t, run)

	for _, path := range links {
		target, err := os.Readlink(path)
		if err != nil {
			t.Fatalf("link %s was replaced: %v", path, err)
		}
		if target != before[path] {
			t.Fatalf("link %s changed from %q to %q", path, before[path], target)
		}
	}
	for _, path := range []string{filepath.Join(logicalInstallDir, "cxp"), filepath.Join(logicalInstallDir, "codex-proxy")} {
		out, err := exec.Command(path, "--version").CombinedOutput()
		if err != nil {
			t.Fatalf("%s --version: %v\n%s", path, err, out)
		}
		if !strings.Contains(string(out), "1.2.3") {
			t.Fatalf("%s did not receive new payload: %s", path, out)
		}
	}
}

func TestInstallShRejectsSymlinkLoopWithoutChangingLinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX installer symlink topology")
	}
	run := newInstallShRun(t, false, false)
	cxp := filepath.Join(run.installDir, "cxp")
	hop := filepath.Join(run.installDir, "cxp-loop-hop")
	if err := os.Symlink("cxp-loop-hop", cxp); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("cxp", hop); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("sh", run.scriptPath)
	cmd.Dir = run.repoRoot
	cmd.Env = run.env
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("installer accepted symlink loop:\n%s", output)
	}
	if !strings.Contains(string(output), "symlink loop") {
		t.Fatalf("installer did not explain symlink loop:\n%s", output)
	}
	for path, want := range map[string]string{cxp: "cxp-loop-hop", hop: "cxp"} {
		got, readErr := os.Readlink(path)
		if readErr != nil || got != want {
			t.Fatalf("link %s changed: got %q err=%v, want %q", path, got, readErr, want)
		}
	}
}

func TestInstallShBuiltinSkillFailureWarnsButInstallSucceeds(t *testing.T) {
	run := newInstallShRun(t, false, false)
	assetData := []byte("#!/bin/sh\nif [ \"$1\" = \"skills\" ]; then\n  exit 42\nfi\nif [ \"$1\" = \"--version\" ]; then\n  echo codex-proxy 1.2.3\n  exit 0\nfi\nexit 0\n")
	checksum := sha256.Sum256(assetData)
	run.env = overrideEnv(run.env, "CODEX_PROXY_TEST_ASSET_DATA", string(assetData))
	run.env = overrideEnv(run.env, "CODEX_PROXY_TEST_CHECKSUMS", fmt.Sprintf("%x  %s\n", checksum, run.asset))

	cmd := exec.Command("sh", run.scriptPath)
	cmd.Dir = run.repoRoot
	cmd.Env = run.env
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh should not fail when built-in skill install fails: %v\n%s", err, string(output))
	}
	text := string(output)
	if !strings.Contains(text, "CODEX-PROXY INSTALL SUCCESS") {
		t.Fatalf("expected success banner, got %s", text)
	}
	if !strings.Contains(text, "Warning: failed to install built-in cxp skill") {
		t.Fatalf("expected built-in skill warning, got %s", text)
	}
}

func TestInstallShChecksumDownloadFailureRemainsBestEffort(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "CODEX_PROXY_TEST_CHECKSUMS_FAIL", "1")

	cmd := exec.Command("sh", run.scriptPath)
	cmd.Dir = run.repoRoot
	cmd.Env = run.env
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh should ignore checksum download failure: %v\n%s", err, string(output))
	}
	text := string(output)
	if !strings.Contains(text, "CODEX-PROXY INSTALL SUCCESS") {
		t.Fatalf("expected success banner, got %s", text)
	}
	installed := filepath.Join(run.installDir, "codex-proxy")
	got, err := os.ReadFile(installed)
	if err != nil {
		t.Fatalf("read installed binary: %v", err)
	}
	if string(got) != string(run.assetData) {
		t.Fatalf("installed payload mismatch")
	}
}

func TestInstallShAssetDownloadFailureReportsDownloadFailure(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "CODEX_PROXY_TEST_ASSET_FAIL", "1")

	cmd := exec.Command("sh", run.scriptPath)
	cmd.Dir = run.repoRoot
	cmd.Env = run.env
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected release asset download failure")
	}
	text := string(output)
	if !strings.Contains(text, "Failed to download release asset") {
		t.Fatalf("expected download failure reason, got %s", text)
	}
	if strings.Contains(text, "Checksum mismatch") {
		t.Fatalf("download failure must not be masked as checksum mismatch, got %s", text)
	}
}

func TestInstallShDiskSpaceFailureBanner(t *testing.T) {
	run := newInstallShRun(t, false, false)

	dfDir := t.TempDir()
	writeStubDf(t, dfDir, 1)
	pathValue := dfDir + string(os.PathListSeparator) + envValueForTest(run.env, "PATH")
	run.env = overrideEnv(run.env, "PATH", pathValue)
	run.env = overrideEnv(run.env, "CODEX_PROXY_INSTALL_MIN_FREE_KB", "2048")

	cmd := exec.Command("sh", run.scriptPath)
	cmd.Dir = run.repoRoot
	cmd.Env = run.env
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected disk space failure")
	}
	text := string(output)
	if !strings.Contains(text, "CODEX-PROXY INSTALL FAILED") {
		t.Fatalf("expected failure banner, got %s", text)
	}
	if !strings.Contains(text, "Not enough disk space") {
		t.Fatalf("expected disk space reason, got %s", text)
	}
}

func TestInstallShDiskSpaceUnknownWarnsButContinues(t *testing.T) {
	run := newInstallShRun(t, false, false)

	dfDir := t.TempDir()
	writeBrokenDf(t, dfDir)
	pathValue := dfDir + string(os.PathListSeparator) + envValueForTest(run.env, "PATH")
	run.env = overrideEnv(run.env, "PATH", pathValue)

	cmd := exec.Command("sh", run.scriptPath)
	cmd.Dir = run.repoRoot
	cmd.Env = run.env
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh should continue when disk space cannot be checked reliably: %v\n%s", err, string(output))
	}
	text := string(output)
	if !strings.Contains(strings.ToLower(text), "warning: could not reliably check free disk space") {
		t.Fatalf("expected unreliable disk check warning, got %s", text)
	}
	if !strings.Contains(text, "CODEX-PROXY INSTALL SUCCESS") {
		t.Fatalf("expected success banner, got %s", text)
	}
}

func TestInstallPs1ChecksumDownloadRemainsBestEffort(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.ps1")
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read install.ps1: %v", err)
	}
	text := string(data)
	checksumBlockStart := strings.Index(text, "# Optional checksum verification.")
	if checksumBlockStart < 0 {
		t.Fatal("install.ps1 missing optional checksum block")
	}
	checksumBlockEnd := strings.Index(text[checksumBlockStart:], "$dst = Join-Path")
	if checksumBlockEnd < 0 {
		t.Fatal("install.ps1 checksum block end marker not found")
	}
	checksumBlock := text[checksumBlockStart : checksumBlockStart+checksumBlockEnd]
	if strings.Contains(checksumBlock, "Invoke-DiskWrite -Label \"checksum download\"") {
		t.Fatalf("checksum download must stay best-effort for non-disk failures, got:\n%s", checksumBlock)
	}
	if !strings.Contains(checksumBlock, "Test-DiskSpaceError") {
		t.Fatalf("checksum block should still promote disk-space failures, got:\n%s", checksumBlock)
	}
	if !strings.Contains(text, "Get-CodexProxySHA256Hex") || !strings.Contains(text, "System.Security.Cryptography.SHA256") {
		t.Fatalf("install.ps1 should not depend solely on Get-FileHash for checksum verification")
	}
}

func TestInstallPs1DoesNotDotSourceProfile(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(repoRoot, "install.ps1"))
	if err != nil {
		t.Fatalf("read install.ps1: %v", err)
	}
	text := string(data)
	for _, forbidden := range []string{
		". $profilePath",
		"$profileUpdated",
		"Failed to reload profile",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("install.ps1 should not dot-source or reload the user profile, found %q", forbidden)
		}
	}
	if !strings.Contains(text, "$env:Path = Prepend-PathEntries -pathText $env:Path -pathValues $pathEntries") {
		t.Fatalf("install.ps1 should still refresh the current PowerShell process PATH")
	}
}

func TestReadmeWindowsInstallAvoidsDownloadAndExecuteOneLiner(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(repoRoot, "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	text := string(data)
	lower := strings.ToLower(text)
	for _, forbidden := range []string{
		"install.ps1 | iex",
		"install.ps1 | invoke-expression",
		"invoke-expression",
		"-executionpolicy bypass",
	} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("README should not recommend Defender-prone PowerShell download-and-execute pattern %q", forbidden)
		}
	}
	for _, required := range []string{
		`$u="https://raw.githubusercontent.com/baaaaaaaka/codex-helper/main/install.ps1"`,
		"Invoke-WebRequest -UseBasicParsing $u -OutFile $p",
		"Unblock-File -LiteralPath $p",
		"& powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File $p",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("README missing safer Windows install step %q", required)
		}
	}
}

func TestReadmeTeamsSetupDoesNotRequirePrerelease(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(repoRoot, "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	text := string(data)
	lower := strings.ToLower(text)
	for _, forbidden := range []string{
		"pre-release builds only",
		"pre-release-only",
		"v0.1.0-rc",
	} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("README should not tell stable Teams users to move to prerelease-only setup, found %q", forbidden)
		}
	}
	for _, required := range []string{
		"Teams helper is available in stable releases.",
		"codex-proxy upgrade",
		"helper update now",
		"Use `helper update prerelease` only when you intentionally want the newest\npre-release.",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("README missing stable Teams setup guidance %q", required)
		}
	}
}

func TestInstallShUsesProfileWhenShellMissing(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "SHELL", "")

	runInstallShCommand(t, run)
	profilePath := filepath.Join(run.homeDir, ".profile")
	contents, err := os.ReadFile(profilePath)
	if err != nil {
		t.Fatalf("read profile: %v", err)
	}
	text := string(contents)
	sourceLine := expectedPosixSourceLine(run.homeDir)
	if !strings.Contains(text, sourceLine) {
		t.Fatalf("missing PATH source line in profile")
	}
	if strings.Contains(text, "alias cxp='codex-proxy'") {
		t.Fatalf("legacy cxp alias remained in profile")
	}
	assertUnixPathSnippet(t, run.homeDir, run.installDir)
}

func TestInstallShPathSnippetActivatesWhenHomeContainsSpaces(t *testing.T) {
	run := newInstallShRun(t, false, false)

	root := t.TempDir()
	run.homeDir = filepath.Join(root, "home dir")
	run.installDir = filepath.Join(root, "install dir")
	run.env = overrideEnv(run.env, "HOME", run.homeDir)
	run.env = overrideEnv(run.env, "XDG_CONFIG_HOME", filepath.Join(run.homeDir, ".config"))
	run.env = overrideEnv(run.env, "CODEX_PROXY_INSTALL_DIR", run.installDir)
	run.env = overrideEnv(run.env, "SHELL", "")

	runInstallShCommand(t, run)

	outFile := filepath.Join(t.TempDir(), "path.txt")
	cmd := exec.Command("sh", "-c", `. "$PATH_SNIPPET"; printf '%s' "$PATH" > "$OUT_FILE"`)
	cmd.Dir = run.repoRoot
	cmd.Env = append([]string{}, run.env...)
	cmd.Env = append(cmd.Env,
		"PATH_SNIPPET="+expectedPosixPathSnippetPath(run.homeDir),
		"OUT_FILE="+outFile,
	)
	runInstallShCommandWithCmd(t, cmd)

	pathData, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read PATH capture: %v", err)
	}
	pathValue := string(pathData)
	if !containsPathEntry(pathValue, run.installDir) {
		t.Fatalf("expected PATH to include install dir %q, got %q", run.installDir, pathValue)
	}
	if !containsPathEntry(pathValue, defaultManagedBinDir(run.homeDir)) {
		t.Fatalf("expected PATH to include managed CLI dir %q, got %q", defaultManagedBinDir(run.homeDir), pathValue)
	}
}

func TestInstallShDoesNotSourceUserShellConfigsDuringInstall(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "SHELL", "/bin/bash")

	markerPath := filepath.Join(t.TempDir(), "profile-sourced")
	markerLine := fmt.Sprintf("printf sourced > \"%s\"\n", strings.ReplaceAll(markerPath, `"`, `\"`))
	for _, path := range []string{
		filepath.Join(run.homeDir, ".bash_profile"),
		filepath.Join(run.homeDir, ".bashrc"),
	} {
		if err := os.WriteFile(path, []byte(markerLine), 0o644); err != nil {
			t.Fatalf("write poison shell config %s: %v", path, err)
		}
	}

	runInstallShCommand(t, run)

	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("installer should not source user shell configs during install, stat err=%v", err)
	}
}

func TestInstallShRemovesLegacyCodexClp(t *testing.T) {
	run := newInstallShRun(t, false, false)

	legacyClpPath := filepath.Join(run.installDir, "clp")
	legacyClpData := []byte("#!/bin/sh\necho codex-proxy 0.0.0\n")
	if err := os.WriteFile(legacyClpPath, legacyClpData, 0o755); err != nil {
		t.Fatalf("write legacy clp: %v", err)
	}

	runInstallShCommand(t, run)

	if _, err := os.Stat(legacyClpPath); !os.IsNotExist(err) {
		t.Fatalf("expected legacy clp removed, stat err=%v", err)
	}
}

func TestInstallShRemovesLegacyCodexClaudeProxyAndClpSymlink(t *testing.T) {
	run := newInstallShRun(t, false, false)

	legacyClaudeProxyPath := filepath.Join(run.installDir, "claude-proxy")
	legacyClaudeProxyData := []byte("#!/bin/sh\n# github.com/baaaaaaaka/codex-helper\nif [ \"$1\" = \"--version\" ]; then\n  echo claude-proxy 0.0.0\n  exit 0\nfi\nexit 0\n")
	if err := os.WriteFile(legacyClaudeProxyPath, legacyClaudeProxyData, 0o755); err != nil {
		t.Fatalf("write legacy claude-proxy: %v", err)
	}
	legacyClpPath := filepath.Join(run.installDir, "clp")
	if err := os.Symlink("claude-proxy", legacyClpPath); err != nil {
		t.Fatalf("symlink clp: %v", err)
	}

	runInstallShCommand(t, run)

	if _, err := os.Stat(legacyClaudeProxyPath); !os.IsNotExist(err) {
		t.Fatalf("expected legacy claude-proxy removed, stat err=%v", err)
	}
	if _, err := os.Lstat(legacyClpPath); !os.IsNotExist(err) {
		t.Fatalf("expected legacy clp symlink removed, stat err=%v", err)
	}
}

func TestInstallShRemovesCodexOwnedClaudeProxyWithoutVersionOutput(t *testing.T) {
	run := newInstallShRun(t, false, false)

	legacyClaudeProxyPath := filepath.Join(run.installDir, "claude-proxy")
	legacyClaudeProxyData := []byte("#!/bin/sh\n# github.com/baaaaaaaka/codex-helper\nexit 0\n")
	if err := os.WriteFile(legacyClaudeProxyPath, legacyClaudeProxyData, 0o755); err != nil {
		t.Fatalf("write legacy claude-proxy: %v", err)
	}

	runInstallShCommand(t, run)

	if _, err := os.Stat(legacyClaudeProxyPath); !os.IsNotExist(err) {
		t.Fatalf("expected codex-owned claude-proxy removed without version output, stat err=%v", err)
	}
}

func TestInstallShPreservesExternalClpSymlinkToClaudeProxy(t *testing.T) {
	run := newInstallShRun(t, false, false)

	legacyClaudeProxyPath := filepath.Join(run.installDir, "claude-proxy")
	legacyClaudeProxyData := []byte("#!/bin/sh\necho external claude-proxy\n")
	if err := os.WriteFile(legacyClaudeProxyPath, legacyClaudeProxyData, 0o755); err != nil {
		t.Fatalf("write external claude-proxy: %v", err)
	}
	legacyClpPath := filepath.Join(run.installDir, "clp")
	if err := os.Symlink("claude-proxy", legacyClpPath); err != nil {
		t.Fatalf("symlink clp: %v", err)
	}

	runInstallShCommand(t, run)

	claudeProxyData, err := os.ReadFile(legacyClaudeProxyPath)
	if err != nil {
		t.Fatalf("read external claude-proxy: %v", err)
	}
	if string(claudeProxyData) != string(legacyClaudeProxyData) {
		t.Fatalf("expected installer to preserve external claude-proxy, got %q", string(claudeProxyData))
	}

	info, err := os.Lstat(legacyClpPath)
	if err != nil {
		t.Fatalf("lstat clp symlink: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected clp to remain a symlink, mode=%v", info.Mode())
	}
	linkTarget, err := os.Readlink(legacyClpPath)
	if err != nil {
		t.Fatalf("readlink clp: %v", err)
	}
	if linkTarget != "claude-proxy" {
		t.Fatalf("expected clp to keep pointing at claude-proxy, got %q", linkTarget)
	}
}

func TestInstallShWritesZshPathSources(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "SHELL", "/bin/zsh")

	runInstallShCommand(t, run)

	sourceLine := expectedPosixSourceLine(run.homeDir)
	zprofilePath := filepath.Join(run.homeDir, ".zprofile")
	zprofile, err := os.ReadFile(zprofilePath)
	if err != nil {
		t.Fatalf("read zprofile: %v", err)
	}
	if !strings.Contains(string(zprofile), sourceLine) {
		t.Fatalf("missing PATH source line in zprofile")
	}

	zshrcPath := filepath.Join(run.homeDir, ".zshrc")
	zshrc, err := os.ReadFile(zshrcPath)
	if err != nil {
		t.Fatalf("read zshrc: %v", err)
	}
	zshrcText := string(zshrc)
	if !strings.Contains(zshrcText, sourceLine) {
		t.Fatalf("missing PATH source line in zshrc")
	}
	if strings.Contains(zshrcText, "alias cxp='codex-proxy'") {
		t.Fatalf("legacy cxp alias remained in zshrc")
	}

	assertUnixPathSnippet(t, run.homeDir, run.installDir)
}

func TestInstallShRemovesLegacyBashAliasFromLoginAndInteractiveConfigs(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "SHELL", "/bin/bash")
	bashProfilePath := filepath.Join(run.homeDir, ".bash_profile")
	if err := os.WriteFile(bashProfilePath, []byte("# existing bash profile\nalias cxp='codex-proxy'\n"), 0o644); err != nil {
		t.Fatalf("write bash_profile: %v", err)
	}
	bashrcPath := filepath.Join(run.homeDir, ".bashrc")
	if err := os.WriteFile(bashrcPath, []byte("# existing bashrc\nalias cxp='codex-proxy'\n"), 0o644); err != nil {
		t.Fatalf("write bashrc: %v", err)
	}

	runInstallShCommand(t, run)

	for _, path := range []string{bashProfilePath, bashrcPath} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(contents)
		if strings.Contains(text, "alias cxp='codex-proxy'") {
			t.Fatalf("legacy cxp alias remained in %s", path)
		}
	}
}

func TestInstallShUsesBashLoginWhenPresent(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "SHELL", "/bin/bash")
	bashLoginPath := filepath.Join(run.homeDir, ".bash_login")
	if err := os.WriteFile(bashLoginPath, []byte("# existing bash login\n"), 0o644); err != nil {
		t.Fatalf("write bash_login: %v", err)
	}

	runInstallShCommand(t, run)

	contents, err := os.ReadFile(bashLoginPath)
	if err != nil {
		t.Fatalf("read bash_login: %v", err)
	}
	text := string(contents)
	if !strings.Contains(text, expectedPosixSourceLine(run.homeDir)) {
		t.Fatalf("missing PATH source line in bash_login")
	}
	if strings.Contains(text, "alias cxp='codex-proxy'") {
		t.Fatalf("legacy cxp alias remained in bash_login")
	}
}

func TestInstallShWritesFishPathSnippet(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "SHELL", "/usr/bin/fish")
	fishConfigPath := filepath.Join(run.homeDir, ".config", "fish", "config.fish")
	if err := os.MkdirAll(filepath.Dir(fishConfigPath), 0o755); err != nil {
		t.Fatalf("mkdir fish config dir: %v", err)
	}
	if err := os.WriteFile(fishConfigPath, []byte("alias cxp \"codex-proxy\"\n"), 0o644); err != nil {
		t.Fatalf("write fish config: %v", err)
	}

	runInstallShCommand(t, run)

	fishSnippetPath := expectedFishPathSnippetPath(run.homeDir)
	fishSnippet, err := os.ReadFile(fishSnippetPath)
	if err != nil {
		t.Fatalf("read fish PATH snippet: %v", err)
	}
	fishText := string(fishSnippet)
	if !strings.Contains(fishText, run.installDir) {
		t.Fatalf("missing install dir in fish PATH snippet")
	}
	if !strings.Contains(fishText, defaultManagedBinDir(run.homeDir)) {
		t.Fatalf("missing managed CLI dir in fish PATH snippet")
	}

	fishConfig, err := os.ReadFile(fishConfigPath)
	if err != nil {
		t.Fatalf("read fish config: %v", err)
	}
	fishConfigText := string(fishConfig)
	if strings.Contains(fishConfigText, "alias cxp \"codex-proxy\"") {
		t.Fatalf("legacy cxp alias remained in fish config")
	}
	if strings.Contains(fishConfigText, fmt.Sprintf("set -gx PATH \"%s\" $PATH", run.installDir)) {
		t.Fatalf("unexpected legacy PATH update in fish config")
	}
}

func TestInstallShWritesCshPathSnippet(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "SHELL", "/bin/csh")

	runInstallShCommand(t, run)

	cshrcPath := filepath.Join(run.homeDir, ".cshrc")
	cshrc, err := os.ReadFile(cshrcPath)
	if err != nil {
		t.Fatalf("read cshrc: %v", err)
	}
	cshrcText := string(cshrc)
	if !strings.Contains(cshrcText, expectedCshSourceLine(run.homeDir)) {
		t.Fatalf("missing PATH source line in cshrc")
	}
	if strings.Contains(cshrcText, "alias cxp codex-proxy") {
		t.Fatalf("legacy cxp alias remained in cshrc")
	}

	assertCshPathSnippet(t, run.homeDir, run.installDir)
}

func TestInstallShWritesTcshPathSnippetToCshrcWhenTcshrcMissing(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "SHELL", "/bin/tcsh")

	runInstallShCommand(t, run)

	cshrcPath := filepath.Join(run.homeDir, ".cshrc")
	cshrc, err := os.ReadFile(cshrcPath)
	if err != nil {
		t.Fatalf("read cshrc: %v", err)
	}
	cshrcText := string(cshrc)
	if !strings.Contains(cshrcText, expectedCshSourceLine(run.homeDir)) {
		t.Fatalf("missing PATH source line in cshrc for tcsh without tcshrc")
	}
	if strings.Contains(cshrcText, "alias cxp codex-proxy") {
		t.Fatalf("legacy cxp alias remained in cshrc for tcsh without tcshrc")
	}
}

func TestInstallShWritesTcshPathSnippetWithoutChangingTcshrcPrecedence(t *testing.T) {
	run := newInstallShRun(t, false, false)
	run.env = overrideEnv(run.env, "SHELL", "/bin/tcsh")
	tcshrcPath := filepath.Join(run.homeDir, ".tcshrc")
	if err := os.WriteFile(tcshrcPath, []byte("# existing tcshrc\n"), 0o644); err != nil {
		t.Fatalf("write tcshrc: %v", err)
	}

	runInstallShCommand(t, run)

	cshrcPath := filepath.Join(run.homeDir, ".cshrc")
	cshrc, err := os.ReadFile(cshrcPath)
	if err != nil {
		t.Fatalf("read cshrc: %v", err)
	}
	if !strings.Contains(string(cshrc), expectedCshSourceLine(run.homeDir)) {
		t.Fatalf("missing PATH source line in cshrc for tcsh")
	}

	tcshrc, err := os.ReadFile(tcshrcPath)
	if err != nil {
		t.Fatalf("read tcshrc: %v", err)
	}
	tcshrcText := string(tcshrc)
	if !strings.Contains(tcshrcText, expectedCshSourceLine(run.homeDir)) {
		t.Fatalf("missing PATH source line in tcshrc")
	}
	if strings.Contains(tcshrcText, "alias cxp codex-proxy") {
		t.Fatalf("legacy cxp alias remained in tcshrc")
	}

	assertCshPathSnippet(t, run.homeDir, run.installDir)
}

func TestInstallShRejectsUnknownArg(t *testing.T) {
	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.sh")
	cmd := exec.Command("sh", scriptPath, "--unknown")
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected unknown arg error")
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError, got %T", err)
	}
	if exitErr.ExitCode() != 2 {
		t.Fatalf("expected exit code 2, got %d\n%s", exitErr.ExitCode(), string(output))
	}
}

func runInstallSh(t *testing.T, apiFail bool, pathAlreadySet bool) {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.sh")

	binDir := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	writeStubCurl(t, binDir)

	homeDir := t.TempDir()
	installDir := t.TempDir()
	version := "v1.2.3"
	verNoV := strings.TrimPrefix(version, "v")
	asset := fmt.Sprintf("codex-proxy_%s_%s_%s", verNoV, runtime.GOOS, runtime.GOARCH)
	assetData := []byte("#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo codex-proxy 1.2.3\n  exit 0\nfi\nexit 0\n")
	checksum := sha256.Sum256(assetData)
	checksums := fmt.Sprintf("%x  %s\n", checksum, asset)
	apiJSON := fmt.Sprintf("{\"tag_name\":\"%s\"}", version)
	latestURL := "https://github.com/owner/name/releases/tag/" + version

	pathValue := binDir + string(os.PathListSeparator) + os.Getenv("PATH")
	if pathAlreadySet {
		pathValue = installDir + string(os.PathListSeparator) + pathValue
	}
	env := append([]string{}, os.Environ()...)
	env = append(env,
		"PATH="+pathValue,
		"HOME="+homeDir,
		"XDG_CONFIG_HOME="+filepath.Join(homeDir, ".config"),
		"SHELL=/bin/bash",
		"CODEX_PROXY_REPO=owner/name",
		"CODEX_PROXY_VERSION=latest",
		"CODEX_PROXY_INSTALL_DIR="+installDir,
		"CODEX_PROXY_TEST_API_FAIL="+boolEnv(apiFail),
		"CODEX_PROXY_TEST_API_JSON="+apiJSON,
		"CODEX_PROXY_TEST_LATEST_URL="+latestURL,
		"CODEX_PROXY_TEST_ASSET="+asset,
		"CODEX_PROXY_TEST_ASSET_DATA="+string(assetData),
		"CODEX_PROXY_TEST_CHECKSUMS="+checksums,
	)

	preexistingClpPath := filepath.Join(installDir, "clp")
	preexistingClpData := []byte("#!/bin/sh\necho external clp\n")
	if err := os.WriteFile(preexistingClpPath, preexistingClpData, 0o755); err != nil {
		t.Fatalf("write preexisting clp: %v", err)
	}
	preexistingClaudeProxyPath := filepath.Join(installDir, "claude-proxy")
	preexistingClaudeProxyData := []byte("#!/bin/sh\necho external claude-proxy\n")
	if err := os.WriteFile(preexistingClaudeProxyPath, preexistingClaudeProxyData, 0o755); err != nil {
		t.Fatalf("write preexisting claude-proxy: %v", err)
	}

	cmd := exec.Command("sh", scriptPath)
	cmd.Dir = repoRoot
	cmd.Env = env
	runInstallShCommandWithCmd(t, cmd)

	installed := filepath.Join(installDir, "codex-proxy")
	got, err := os.ReadFile(installed)
	if err != nil {
		t.Fatalf("read installed: %v", err)
	}
	if string(got) != string(assetData) {
		t.Fatalf("installed payload mismatch")
	}

	cxpPath := filepath.Join(installDir, "cxp")
	cxpData, err := os.ReadFile(cxpPath)
	if err != nil {
		t.Fatalf("read cxp: %v", err)
	}
	if string(cxpData) != string(assetData) {
		t.Fatalf("cxp payload mismatch")
	}
	cxpVersion, err := exec.Command(cxpPath, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("cxp --version failed: %v\n%s", err, string(cxpVersion))
	}
	if !strings.Contains(string(cxpVersion), "codex-proxy 1.2.3") {
		t.Fatalf("unexpected cxp --version output: %s", string(cxpVersion))
	}
	recordData, err := os.ReadFile(filepath.Join(homeDir, ".config", "codex-helper", "install.json"))
	if err != nil {
		t.Fatalf("read install record: %v", err)
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
	if record.SchemaVersion != 1 ||
		record.TargetPath != installed ||
		record.TargetSource != "installer" ||
		record.TargetState != "managed" ||
		record.Repo != "owner/name" ||
		record.Version != "v1.2.3" ||
		record.GOOS != runtime.GOOS ||
		record.GOARCH != runtime.GOARCH ||
		!stringSliceContains(record.Shims, cxpPath) {
		t.Fatalf("unexpected install record: %#v", record)
	}
	clpPath := filepath.Join(installDir, "clp")
	clpData, err := os.ReadFile(clpPath)
	if err != nil {
		t.Fatalf("read preexisting clp: %v", err)
	}
	if string(clpData) != string(preexistingClpData) {
		t.Fatalf("expected installer to preserve unrelated clp, got %q", string(clpData))
	}
	claudeProxyPath := filepath.Join(installDir, "claude-proxy")
	claudeProxyData, err := os.ReadFile(claudeProxyPath)
	if err != nil {
		t.Fatalf("read preexisting claude-proxy: %v", err)
	}
	if string(claudeProxyData) != string(preexistingClaudeProxyData) {
		t.Fatalf("expected installer to preserve unrelated claude-proxy, got %q", string(claudeProxyData))
	}

	bashrcPath := filepath.Join(homeDir, ".bashrc")
	bashrc, err := os.ReadFile(bashrcPath)
	if err != nil {
		t.Fatalf("read bashrc: %v", err)
	}
	bashrcText := string(bashrc)
	sourceLine := expectedPosixSourceLine(homeDir)
	if !strings.Contains(bashrcText, sourceLine) {
		t.Fatalf("missing PATH source line in bashrc")
	}
	if strings.Contains(bashrcText, fmt.Sprintf("export PATH=\"%s:$PATH\"", installDir)) {
		t.Fatalf("unexpected legacy PATH update in bashrc")
	}
	if strings.Contains(bashrcText, "alias cxp='codex-proxy'") {
		t.Fatalf("legacy cxp alias remained in bashrc")
	}

	profilePath := filepath.Join(homeDir, ".profile")
	profile, err := os.ReadFile(profilePath)
	if err != nil {
		t.Fatalf("read profile: %v", err)
	}
	profileText := string(profile)
	if !strings.Contains(profileText, sourceLine) {
		t.Fatalf("missing PATH source line in profile")
	}
	if strings.Contains(profileText, fmt.Sprintf("export PATH=\"%s:$PATH\"", installDir)) {
		t.Fatalf("unexpected legacy PATH update in profile")
	}
	if strings.Contains(profileText, "alias cxp='codex-proxy'") {
		t.Fatalf("legacy cxp alias remained in profile")
	}

	assertUnixPathSnippet(t, homeDir, installDir)
}

type installShRun struct {
	repoRoot   string
	scriptPath string
	homeDir    string
	installDir string
	asset      string
	assetData  []byte
	env        []string
}

func newInstallShRun(t *testing.T, apiFail bool, pathAlreadySet bool) installShRun {
	t.Helper()
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	scriptPath := filepath.Join(repoRoot, "install.sh")

	binDir := filepath.Join(t.TempDir(), "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	writeStubCurl(t, binDir)

	homeDir := t.TempDir()
	installDir := t.TempDir()
	version := "v1.2.3"
	verNoV := strings.TrimPrefix(version, "v")
	asset := fmt.Sprintf("codex-proxy_%s_%s_%s", verNoV, runtime.GOOS, runtime.GOARCH)
	assetData := []byte("#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo codex-proxy 1.2.3\n  exit 0\nfi\nexit 0\n")
	checksum := sha256.Sum256(assetData)
	checksums := fmt.Sprintf("%x  %s\n", checksum, asset)
	apiJSON := fmt.Sprintf("{\"tag_name\":\"%s\"}", version)
	latestURL := "https://github.com/owner/name/releases/tag/" + version

	pathValue := binDir + string(os.PathListSeparator) + os.Getenv("PATH")
	if pathAlreadySet {
		pathValue = installDir + string(os.PathListSeparator) + pathValue
	}
	env := append([]string{}, os.Environ()...)
	env = append(env,
		"PATH="+pathValue,
		"HOME="+homeDir,
		"XDG_CONFIG_HOME="+filepath.Join(homeDir, ".config"),
		"SHELL=/bin/bash",
		"CODEX_PROXY_REPO=owner/name",
		"CODEX_PROXY_VERSION=latest",
		"CODEX_PROXY_INSTALL_DIR="+installDir,
		"CODEX_PROXY_TEST_API_FAIL="+boolEnv(apiFail),
		"CODEX_PROXY_TEST_API_JSON="+apiJSON,
		"CODEX_PROXY_TEST_LATEST_URL="+latestURL,
		"CODEX_PROXY_TEST_ASSET="+asset,
		"CODEX_PROXY_TEST_ASSET_DATA="+string(assetData),
		"CODEX_PROXY_TEST_CHECKSUMS="+checksums,
	)

	return installShRun{
		repoRoot:   repoRoot,
		scriptPath: scriptPath,
		homeDir:    homeDir,
		installDir: installDir,
		asset:      asset,
		assetData:  assetData,
		env:        env,
	}
}

func overrideEnv(env []string, key, value string) []string {
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		k, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if k == key {
			continue
		}
		out = append(out, kv)
	}
	return append(out, key+"="+value)
}

func boolEnv(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func envValueForTest(env []string, key string) string {
	for i := len(env) - 1; i >= 0; i-- {
		k, v, ok := strings.Cut(env[i], "=")
		if ok && k == key {
			return v
		}
	}
	return ""
}

func containsPathEntry(pathValue, target string) bool {
	target = normalizeComparablePath(target)
	for _, entry := range filepath.SplitList(pathValue) {
		if normalizeComparablePath(entry) == target {
			return true
		}
	}
	return false
}

func normalizeComparablePath(pathValue string) string {
	cleaned := filepath.Clean(pathValue)
	if runtime.GOOS == "darwin" {
		cleaned = strings.TrimPrefix(cleaned, "/private")
		if cleaned == "" {
			cleaned = string(filepath.Separator)
		}
	}
	resolved, err := filepath.EvalSymlinks(cleaned)
	if err == nil {
		cleaned = filepath.Clean(resolved)
		if runtime.GOOS == "darwin" {
			cleaned = strings.TrimPrefix(cleaned, "/private")
			if cleaned == "" {
				cleaned = string(filepath.Separator)
			}
		}
	}
	return cleaned
}

func writeStubCurl(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "curl")
	script := `#!/usr/bin/env sh
set -e
out=""
write_effective=""
url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o)
      out="$2"
      shift 2
      ;;
    -w)
      write_effective="$2"
      shift 2
      ;;
    -*)
      shift
      ;;
    *)
      url="$1"
      shift
      ;;
  esac
done

if [ -n "$write_effective" ]; then
  if [ -z "${CODEX_PROXY_TEST_LATEST_URL:-}" ]; then
    exit 1
  fi
  printf "%s" "$CODEX_PROXY_TEST_LATEST_URL"
  exit 0
fi

if [ -z "$out" ]; then
  exit 1
fi

case "$url" in
  *"/repos/"*"/releases/latest")
    if [ -n "${CODEX_PROXY_TEST_API_HITS:-}" ]; then
      printf "api\n" >> "$CODEX_PROXY_TEST_API_HITS"
    fi
    if [ "${CODEX_PROXY_TEST_API_FAIL:-}" = "1" ]; then
      exit 22
    fi
    printf "%s" "${CODEX_PROXY_TEST_API_JSON:-}" > "$out"
    ;;
  *"/checksums.txt")
    if [ "${CODEX_PROXY_TEST_CHECKSUMS_FAIL:-}" = "1" ]; then
      exit 22
    fi
    printf "%s" "${CODEX_PROXY_TEST_CHECKSUMS:-}" > "$out"
    ;;
  *"/${CODEX_PROXY_TEST_ASSET}")
    if [ "${CODEX_PROXY_TEST_ASSET_FAIL:-}" = "1" ]; then
      printf "partial download" > "$out"
      exit 16
    fi
    printf "%s" "${CODEX_PROXY_TEST_ASSET_DATA:-}" > "$out"
    ;;
  *)
    exit 22
    ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub curl: %v", err)
	}
}

func writeStubDf(t *testing.T, dir string, availableKB int) {
	t.Helper()
	path := filepath.Join(dir, "df")
	script := fmt.Sprintf(`#!/usr/bin/env sh
printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n'
printf 'stub 4096 4095 %d 99%% /\n'
`, availableKB)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub df: %v", err)
	}
}

func writeBrokenDf(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "df")
	script := `#!/usr/bin/env sh
printf 'df unavailable\n' >&2
exit 1
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write broken df: %v", err)
	}
}

func runInstallShCommand(t *testing.T, run installShRun) {
	t.Helper()
	cmd := exec.Command("sh", run.scriptPath)
	cmd.Dir = run.repoRoot
	cmd.Env = run.env
	runInstallShCommandWithCmd(t, cmd)
}

func runInstallShCommandWithCmd(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh failed: %v\n%s", err, string(output))
	}
}

func assertFileMissingOrEmpty(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(data) != 0 {
		t.Fatalf("expected %s to be empty or absent, got %q", path, string(data))
	}
}

func assertFileContains(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(data), want) {
		t.Fatalf("expected %s to contain %q, got %q", path, want, string(data))
	}
}

func expectedPosixPathSnippetPath(home string) string {
	return filepath.Join(home, ".config", "codex-proxy", "shell", "path.sh")
}

func expectedPosixSourceLine(home string) string {
	snippet := expectedPosixPathSnippetPath(home)
	return fmt.Sprintf("[ -f \"%s\" ] && . \"%s\"", snippet, snippet)
}

func expectedFishPathSnippetPath(home string) string {
	return filepath.Join(home, ".config", "fish", "conf.d", "codex-proxy-path.fish")
}

func expectedCshPathSnippetPath(home string) string {
	return filepath.Join(home, ".config", "codex-proxy", "shell", "path.csh")
}

func expectedCshSourceLine(home string) string {
	return fmt.Sprintf("source '%s'", expectedCshPathSnippetPath(home))
}

func defaultManagedBinDir(home string) string {
	return filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin")
}

func assertUnixPathSnippet(t *testing.T, home, installDir string) {
	t.Helper()
	snippetPath := expectedPosixPathSnippetPath(home)
	contents, err := os.ReadFile(snippetPath)
	if err != nil {
		t.Fatalf("read PATH snippet: %v", err)
	}
	text := string(contents)
	if !strings.Contains(text, installDir) {
		t.Fatalf("missing install dir in PATH snippet")
	}
	if !strings.Contains(text, defaultManagedBinDir(home)) {
		t.Fatalf("missing managed CLI dir in PATH snippet")
	}
	if strings.Count(text, installDir) != 1 {
		t.Fatalf("expected install dir once in PATH snippet, got %q", text)
	}
	if !strings.Contains(text, "hash -r 2>/dev/null || true") {
		t.Fatalf("missing POSIX command cache refresh in PATH snippet")
	}
	if !strings.Contains(text, "rehash 2>/dev/null || true") {
		t.Fatalf("missing zsh command cache refresh in PATH snippet")
	}
}

func assertCshPathSnippet(t *testing.T, home, installDir string) {
	t.Helper()
	snippetPath := expectedCshPathSnippetPath(home)
	contents, err := os.ReadFile(snippetPath)
	if err != nil {
		t.Fatalf("read csh PATH snippet: %v", err)
	}
	text := string(contents)
	if !strings.Contains(text, installDir) {
		t.Fatalf("missing install dir in csh PATH snippet")
	}
	if !strings.Contains(text, defaultManagedBinDir(home)) {
		t.Fatalf("missing managed CLI dir in csh PATH snippet")
	}
	for _, forbidden := range []string{
		`":$PATH:"`,
		`":$_path_entry:"`,
		`setenv PATH`,
		`!~`,
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("csh PATH snippet should avoid PATH string/glob matching, found %q in:\n%s", forbidden, text)
		}
	}
	for _, want := range []string{
		`set path = ( "$_path_entry" $path:q )`,
		`set path = ( "$_path_entry" )`,
		"rehash",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in csh PATH snippet:\n%s", want, text)
		}
	}
}
