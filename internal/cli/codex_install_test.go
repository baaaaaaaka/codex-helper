package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/cloudgate"
)

func containsPath(paths []string, target string) bool {
	target = filepath.Clean(target)
	for _, path := range paths {
		if filepath.Clean(path) == target {
			return true
		}
	}
	return false
}

func writeProbeableCodex(t *testing.T, dir string, ok bool) string {
	t.Helper()

	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, "codex.cmd")
		script := "@echo off\r\nexit /b 0\r\n"
		if !ok {
			script = "@echo off\r\nexit /b 1\r\n"
		}
		if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
			t.Fatalf("write codex cmd: %v", err)
		}
		return path
	}

	path := filepath.Join(dir, "codex")
	script := "#!/bin/sh\nexit 0\n"
	if !ok {
		script = "#!/bin/sh\nexit 1\n"
	}
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}
	return path
}

func writeExecutable(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}

func TestCodexInstallerCandidatesLinux(t *testing.T) {
	cmds := codexInstallerCandidates("linux")
	if len(cmds) != 2 {
		t.Fatalf("expected 2 linux installers, got %d", len(cmds))
	}
	if cmds[0].path != "bash" || cmds[1].path != "sh" {
		t.Fatalf("expected bash then sh installers, got %q then %q", cmds[0].path, cmds[1].path)
	}
	for i, cmd := range cmds {
		if len(cmd.args) < 2 {
			t.Fatalf("expected shell command args for candidate %d, got %v", i, cmd.args)
		}
		if cmd.args[0] != "-c" {
			t.Fatalf("expected non-login shell (-c) for candidate %d, got %q", i, cmd.args[0])
		}
		if !strings.Contains(cmd.args[1], "@openai/codex") {
			t.Fatalf("expected codex npm install in candidate %d", i)
		}
	}
}

func TestCodexInstallerCandidatesWindows(t *testing.T) {
	cmds := codexInstallerCandidates("windows")
	if len(cmds) != 2 {
		t.Fatalf("expected 2 windows installers, got %d", len(cmds))
	}
	if cmds[0].path != "powershell" || cmds[1].path != "pwsh" {
		t.Fatalf("expected powershell then pwsh installers, got %q then %q", cmds[0].path, cmds[1].path)
	}
	for i, cmd := range cmds {
		if len(cmd.args) < 5 {
			t.Fatalf("expected powershell args for candidate %d, got %v", i, cmd.args)
		}
		if cmd.args[3] != "-Command" {
			t.Fatalf("expected -Command for candidate %d, got %q", i, cmd.args[3])
		}
		if !strings.Contains(cmd.args[4], "@openai/codex") {
			t.Fatalf("expected codex npm install in candidate %d", i)
		}
	}
}

func TestCodexInstallerCandidatesUnsupported(t *testing.T) {
	if cmds := codexInstallerCandidates("plan9"); len(cmds) != 0 {
		t.Fatalf("expected no installers for unsupported OS, got %v", cmds)
	}
}

func TestInstallerAttemptLabel(t *testing.T) {
	if got := installerAttemptLabel(codexInstallCmd{path: "bash"}); got != "bash" {
		t.Fatalf("expected bare path label, got %q", got)
	}
	if got := installerAttemptLabel(codexInstallCmd{path: "bash", args: []string{"-c", "echo hi"}}); got != "bash -c" {
		t.Fatalf("expected first arg label, got %q", got)
	}
}

func TestIsCodexCommandRecognizesShimAndScriptNames(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"codex",
		"/tmp/codex",
		`C:\tools\codex.cmd`,
		"/tmp/codex.ps1",
		"/tmp/codex.js",
		"/tmp/codex.mjs",
		"/tmp/codex.cjs",
	} {
		if !isCodexCommand(path) {
			t.Fatalf("expected %q to be recognized as codex command", path)
		}
	}

	for _, path := range []string{
		"",
		"/tmp/node",
		"/tmp/codex-linux-x64",
		"/tmp/not-codex.js",
	} {
		if isCodexCommand(path) {
			t.Fatalf("expected %q to not be recognized as codex command", path)
		}
	}
}

func TestEnsureCodexInstalledWithMissingPath(t *testing.T) {
	_, err := ensureCodexInstalled(context.Background(), filepath.Join(t.TempDir(), "missing"), io.Discard)
	if err == nil {
		t.Fatalf("expected error for missing codex path")
	}
}

func TestEnsureCodexInstalledUsesProvidedPath(t *testing.T) {
	path := writeProbeableCodex(t, t.TempDir(), true)
	got, err := ensureCodexInstalled(context.Background(), path, io.Discard)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v", err)
	}
	if got != path {
		t.Fatalf("expected path %q, got %q", path, got)
	}
}

func TestEnsureCodexInstalledRejectsBrokenProvidedPath(t *testing.T) {
	path := writeProbeableCodex(t, t.TempDir(), false)
	_, err := ensureCodexInstalled(context.Background(), path, io.Discard)
	if err == nil {
		t.Fatal("expected error for broken codex path")
	}
	if !strings.Contains(err.Error(), "not functional") {
		t.Fatalf("expected not-functional error, got %q", err.Error())
	}
}

func TestEnsureCodexInstalledUsesCachedPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip cache path test on windows")
	}

	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir())

	cached := writeProbeableCodex(t, t.TempDir(), true)
	writeCachedCodexPath(cached)

	var out bytes.Buffer
	got, err := ensureCodexInstalled(context.Background(), "", &out)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v", err)
	}
	if got != cached {
		t.Fatalf("expected cached codex path %q, got %q", cached, got)
	}
	if strings.Contains(out.String(), "installing") {
		t.Fatalf("expected cached path to skip install log, got %q", out.String())
	}
}

func TestEnsureCodexInstalledPrefersPathOverCachedPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip cache precedence test on windows")
	}

	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	pathDir := t.TempDir()
	pathCodex := writeProbeableCodex(t, pathDir, true)
	cachedCodex := writeProbeableCodex(t, t.TempDir(), true)
	writeCachedCodexPath(cachedCodex)
	t.Setenv("PATH", pathDir)

	got, err := ensureCodexInstalled(context.Background(), "", io.Discard)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v", err)
	}
	if got != pathCodex {
		t.Fatalf("expected PATH codex %q, got %q", pathCodex, got)
	}
	if cached := readCachedCodexPath(); cached != pathCodex {
		t.Fatalf("expected cache to update to PATH codex %q, got %q", pathCodex, cached)
	}
}

func TestEnsureCodexInstalledUsesCachedPathWhenPathIsBroken(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip cache fallback test on windows")
	}

	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	brokenDir := t.TempDir()
	_ = writeProbeableCodex(t, brokenDir, false)
	cachedCodex := writeProbeableCodex(t, t.TempDir(), true)
	writeCachedCodexPath(cachedCodex)
	t.Setenv("PATH", brokenDir)

	got, err := ensureCodexInstalled(context.Background(), "", io.Discard)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v", err)
	}
	if got != cachedCodex {
		t.Fatalf("expected cached codex %q, got %q", cachedCodex, got)
	}
}

func TestEnsureCodexInstalledClearsBrokenCachedPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip cache path test on windows")
	}

	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir())

	writeCachedCodexPath(filepath.Join(t.TempDir(), "missing-codex"))

	_, err := ensureCodexInstalled(context.Background(), "", io.Discard)
	if err == nil {
		t.Fatal("expected install error with empty PATH and broken cache")
	}
	if got := readCachedCodexPath(); got != "" {
		t.Fatalf("expected broken cache to be cleared, got %q", got)
	}
}

func TestEnsureCodexInstalledInstallsWhenMissing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	home := t.TempDir()
	installDir := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin")
	codexPath := filepath.Join(installDir, "codex")

	binDir := t.TempDir()
	installer := filepath.Join(binDir, "bash")
	script := "#!/bin/sh\n" +
		"mkdir -p \"" + installDir + "\"\n" +
		"cat > \"" + codexPath + "\" <<'EOF'\n" +
		"#!/bin/sh\n" +
		"exit 0\n" +
		"EOF\n" +
		"chmod +x \"" + codexPath + "\"\n"
	if err := os.WriteFile(installer, []byte(script), 0o700); err != nil {
		t.Fatalf("write installer: %v", err)
	}

	t.Setenv("HOME", home)
	t.Setenv("PATH", strings.Join([]string{binDir, "/usr/bin", "/bin"}, string(os.PathListSeparator)))

	var out bytes.Buffer
	got, err := ensureCodexInstalled(context.Background(), "", &out)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v", err)
	}
	if got != codexPath {
		t.Fatalf("expected installed path %q, got %q", codexPath, got)
	}
	if !strings.Contains(out.String(), "codex not found; installing...") {
		t.Fatalf("expected install log, got %q", out.String())
	}
}

func TestEnsureCodexInstalledReportsNonfunctionalAfterInstall(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	home := t.TempDir()
	installDir := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin")
	codexPath := filepath.Join(installDir, "codex")

	binDir := t.TempDir()
	installer := filepath.Join(binDir, "bash")
	script := "#!/bin/sh\n" +
		"mkdir -p \"" + installDir + "\"\n" +
		"cat > \"" + codexPath + "\" <<'EOF'\n" +
		"#!/bin/sh\n" +
		"echo probe boom >&2\n" +
		"exit 7\n" +
		"EOF\n" +
		"chmod +x \"" + codexPath + "\"\n"
	if err := os.WriteFile(installer, []byte(script), 0o700); err != nil {
		t.Fatalf("write installer: %v", err)
	}

	t.Setenv("HOME", home)
	t.Setenv("PATH", strings.Join([]string{binDir, "/usr/bin", "/bin"}, string(os.PathListSeparator)))

	_, err := ensureCodexInstalled(context.Background(), "", io.Discard)
	if err == nil {
		t.Fatal("expected install error")
	}
	if !strings.Contains(err.Error(), "codex installation finished but installed binary is not functional") {
		t.Fatalf("expected nonfunctional install error, got %v", err)
	}
	if !strings.Contains(err.Error(), "probe boom") {
		t.Fatalf("expected probe output in error, got %v", err)
	}
}

func TestDetectCodexUpgradeSourceManaged(t *testing.T) {
	home := t.TempDir()
	prefix := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global")
	codexDir := filepath.Join(prefix, "bin")
	if runtime.GOOS == "windows" {
		codexDir = prefix
	}
	if err := os.MkdirAll(codexDir, 0o755); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	codexPath := writeProbeableCodex(t, codexDir, true)

	t.Setenv("HOME", home)
	t.Setenv("PATH", codexDir)

	source, err := detectCodexUpgradeSource(context.Background(), nil)
	if err != nil {
		t.Fatalf("detectCodexUpgradeSource error: %v", err)
	}
	if source.origin != codexInstallOriginManaged {
		t.Fatalf("expected managed origin, got %q", source.origin)
	}
	if source.codexPath != codexPath {
		t.Fatalf("expected codex path %q, got %q", codexPath, source.codexPath)
	}
	if source.npmPrefix != prefix {
		t.Fatalf("expected npm prefix %q, got %q", prefix, source.npmPrefix)
	}
}

func TestDetectCodexUpgradeSourceSystemNpm(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based source detection test on windows")
	}

	root := t.TempDir()
	globalPrefix := filepath.Join(root, "system-global")
	globalBin := filepath.Join(globalPrefix, "bin")
	if err := os.MkdirAll(globalBin, 0o755); err != nil {
		t.Fatalf("mkdir global bin: %v", err)
	}
	codexPath := writeProbeableCodex(t, globalBin, true)

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + globalPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{globalBin, binDir}, string(os.PathListSeparator)))

	source, err := detectCodexUpgradeSource(context.Background(), nil)
	if err != nil {
		t.Fatalf("detectCodexUpgradeSource error: %v", err)
	}
	if source.origin != codexInstallOriginSystem {
		t.Fatalf("expected system origin, got %q", source.origin)
	}
	if source.codexPath != codexPath {
		t.Fatalf("expected codex path %q, got %q", codexPath, source.codexPath)
	}
	if source.npmPrefix != globalPrefix {
		t.Fatalf("expected npm prefix %q, got %q", globalPrefix, source.npmPrefix)
	}
}

func TestDetectCodexUpgradeSourceUnknown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based source detection test on windows")
	}

	codexDir := t.TempDir()
	_ = writeProbeableCodex(t, codexDir, true)

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + filepath.Join(t.TempDir(), "unrelated-prefix") + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{codexDir, binDir}, string(os.PathListSeparator)))

	source, err := detectCodexUpgradeSource(context.Background(), nil)
	if err != nil {
		t.Fatalf("detectCodexUpgradeSource error: %v", err)
	}
	if source.origin != codexInstallOriginUnknown {
		t.Fatalf("expected unknown origin, got %q", source.origin)
	}
}

func TestUpgradeCodexInstalledWithOptionsRequiresInstalledCodex(t *testing.T) {
	setMissingCodexEnv(t)

	_, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{upgradeCodex: true})
	if err == nil {
		t.Fatal("expected error when codex is not installed")
	}
	if !strings.Contains(err.Error(), "cannot upgrade") {
		t.Fatalf("expected cannot-upgrade error, got %q", err.Error())
	}
}

func TestUpgradeCodexInstalledWithOptionsSkipsProxySetupWhenPrecheckFails(t *testing.T) {
	setMissingCodexEnv(t)

	called := false
	_, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{
		upgradeCodex: true,
		withInstallerEnv: func(context.Context, func([]string) error) error {
			called = true
			return fmt.Errorf("unexpected withInstallerEnv call")
		},
	})
	if err == nil {
		t.Fatal("expected error when codex is not installed")
	}
	if called {
		t.Fatal("expected withInstallerEnv not to run when precheck fails")
	}
	if !strings.Contains(err.Error(), "cannot upgrade") {
		t.Fatalf("expected cannot-upgrade error, got %q", err.Error())
	}
}

func setMissingCodexEnv(t *testing.T) {
	t.Helper()

	homeDir := t.TempDir()
	t.Setenv("PATH", t.TempDir())
	t.Setenv("HOME", homeDir)
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("CODEX_NPM_PREFIX", t.TempDir())
	t.Setenv("CODEX_NODE_INSTALL_ROOT", t.TempDir())

	if runtime.GOOS == "windows" {
		t.Setenv("USERPROFILE", homeDir)
		t.Setenv("LOCALAPPDATA", t.TempDir())
		t.Setenv("APPDATA", t.TempDir())
	}
}

func TestUpgradeCodexInstalledWithOptionsSystemUsesNpmInstall(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}

	root := t.TempDir()
	globalPrefix := filepath.Join(root, "system-global")
	globalBin := filepath.Join(globalPrefix, "bin")
	if err := os.MkdirAll(globalBin, 0o755); err != nil {
		t.Fatalf("mkdir global bin: %v", err)
	}
	codexPath := writeProbeableCodex(t, globalBin, true)
	marker := filepath.Join(root, "npm-install-hit")

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + globalPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"if [ \"$1\" = \"install\" ] && [ \"$2\" = \"-g\" ] && [ \"$3\" = \"@openai/codex\" ]; then\n" +
		"  echo hit > \"" + marker + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{globalBin, binDir}, string(os.PathListSeparator)))
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	got, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{upgradeCodex: true})
	if err != nil {
		t.Fatalf("upgradeCodexInstalledWithOptions error: %v", err)
	}
	if got != codexPath {
		t.Fatalf("expected codex path %q, got %q", codexPath, got)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected npm install marker: %v", err)
	}
}

func TestUpgradeCodexInstalledWithOptionsUsesWithInstallerEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}

	root := t.TempDir()
	globalPrefix := filepath.Join(root, "system-global")
	globalBin := filepath.Join(globalPrefix, "bin")
	if err := os.MkdirAll(globalBin, 0o755); err != nil {
		t.Fatalf("mkdir global bin: %v", err)
	}
	codexPath := writeProbeableCodex(t, globalBin, true)
	marker := filepath.Join(root, "npm-install-hit")

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + globalPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"if [ \"$1\" = \"install\" ] && [ \"$2\" = \"-g\" ] && [ \"$3\" = \"@openai/codex\" ]; then\n" +
		"  if [ \"$TEST_INSTALLER_ENV\" != \"1\" ]; then\n" +
		"    echo \"missing TEST_INSTALLER_ENV\" >&2\n" +
		"    exit 1\n" +
		"  fi\n" +
		"  echo hit > \"" + marker + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{globalBin, binDir}, string(os.PathListSeparator)))
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	called := false
	got, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{
		upgradeCodex: true,
		withInstallerEnv: func(_ context.Context, runUpgrade func([]string) error) error {
			called = true
			return runUpgrade([]string{"TEST_INSTALLER_ENV=1"})
		},
	})
	if err != nil {
		t.Fatalf("upgradeCodexInstalledWithOptions error: %v", err)
	}
	if !called {
		t.Fatal("expected withInstallerEnv to run")
	}
	if got != codexPath {
		t.Fatalf("expected codex path %q, got %q", codexPath, got)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected npm install marker: %v", err)
	}
}

func TestUpgradeCodexInstalledWithOptionsPropagatesWithInstallerEnvError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}

	root := t.TempDir()
	globalPrefix := filepath.Join(root, "system-global")
	globalBin := filepath.Join(globalPrefix, "bin")
	if err := os.MkdirAll(globalBin, 0o755); err != nil {
		t.Fatalf("mkdir global bin: %v", err)
	}
	_ = writeProbeableCodex(t, globalBin, true)

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + globalPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"echo unexpected >&2\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{globalBin, binDir}, string(os.PathListSeparator)))
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	called := false
	_, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{
		upgradeCodex: true,
		withInstallerEnv: func(context.Context, func([]string) error) error {
			called = true
			return fmt.Errorf("proxy bootstrap failed")
		},
	})
	if err == nil {
		t.Fatal("expected withInstallerEnv error")
	}
	if !called {
		t.Fatal("expected withInstallerEnv to run")
	}
	if !strings.Contains(err.Error(), "proxy bootstrap failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCodexRetirePathMatchesNpmArborist(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip unix-style arborist retire path assertion on windows")
	}

	path := "/home/baka/.npm-global/lib/node_modules/@openai/codex"
	got := codexRetirePath(path)
	want := "/home/baka/.npm-global/lib/node_modules/@openai/.codex-WofhFpbS"
	if got != want {
		t.Fatalf("expected retire path %q, got %q", want, got)
	}
}

func TestCodexRetirePathEmpty(t *testing.T) {
	if got := codexRetirePath(""); got != "" {
		t.Fatalf("expected empty retire path, got %q", got)
	}
}

func TestCodexPackageDirForPrefixForOS(t *testing.T) {
	prefix := t.TempDir()

	if got := codexPackageDirForPrefixForOS("linux", prefix); got != filepath.Join(prefix, "lib", "node_modules", "@openai", "codex") {
		t.Fatalf("unexpected linux package dir: %q", got)
	}
	if got := codexPackageDirForPrefixForOS("windows", prefix); got != filepath.Join(prefix, "node_modules", "@openai", "codex") {
		t.Fatalf("unexpected windows package dir: %q", got)
	}
	if got := codexPackageDirForPrefixForOS("linux", ""); got != "" {
		t.Fatalf("expected empty package dir for empty prefix, got %q", got)
	}
}

func TestCodexRetiredPathTargetsIgnoreEmptyPath(t *testing.T) {
	got := codexRetiredPathTargets(codexUpgradeSource{})
	if len(got) != 0 {
		t.Fatalf("expected no targets, got %v", got)
	}
}

func TestDetectCodexUpgradeSourceForPathUsesExplicitManagedPath(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "managed-prefix")
	codexPath := filepath.Join(prefix, "bin", "codex")

	source, err := detectCodexUpgradeSourceForPath(context.Background(), codexPath, []string{"CODEX_NPM_PREFIX=" + prefix})
	if err != nil {
		t.Fatalf("detectCodexUpgradeSourceForPath error: %v", err)
	}
	if source.origin != codexInstallOriginManaged {
		t.Fatalf("expected managed origin, got %q", source.origin)
	}
	if source.codexPath != codexPath {
		t.Fatalf("expected codex path %q, got %q", codexPath, source.codexPath)
	}
	if source.npmPrefix != prefix {
		t.Fatalf("expected npm prefix %q, got %q", prefix, source.npmPrefix)
	}
}

func TestCleanupStaleCodexRetiredPathsForSourceInspectError(t *testing.T) {
	source := codexUpgradeSource{codexPath: "\x00codex"}

	err := cleanupStaleCodexRetiredPathsForSource(io.Discard, source)
	if err == nil {
		t.Fatal("expected inspect error")
	}
	if !strings.Contains(err.Error(), "inspect stale npm retired path") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCleanupStaleCodexRetiredPathsForSourceRemoveError(t *testing.T) {
	lockCLITestHooks(t)
	prevRemoveAll := codexRemoveAll
	t.Cleanup(func() { codexRemoveAll = prevRemoveAll })
	codexRemoveAll = func(string) error { return errors.New("boom") }

	root := t.TempDir()
	codexPath := filepath.Join(root, "codex")
	retired := codexRetirePath(codexPath)
	if err := os.WriteFile(retired, []byte("stale\n"), 0o600); err != nil {
		t.Fatalf("write stale file: %v", err)
	}

	err := cleanupStaleCodexRetiredPathsForSource(io.Discard, codexUpgradeSource{codexPath: codexPath})
	if err == nil {
		t.Fatal("expected remove error")
	}
	if !strings.Contains(err.Error(), "remove stale npm retired path") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureCodexInstalledLeavesStaleSystemNpmRetiredPathsAlone(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based system npm test on windows")
	}

	root := t.TempDir()
	globalPrefix := filepath.Join(root, "system-global")
	globalBin := filepath.Join(globalPrefix, "bin")
	if err := os.MkdirAll(globalBin, 0o755); err != nil {
		t.Fatalf("mkdir global bin: %v", err)
	}
	codexPath := writeProbeableCodex(t, globalBin, true)

	packageDir := codexPackageDirForPrefix(globalPrefix)
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatalf("mkdir package dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packageDir, "package.json"), []byte("{\"name\":\"@openai/codex\"}\n"), 0o600); err != nil {
		t.Fatalf("write package.json: %v", err)
	}

	stalePackageDir := codexRetirePath(packageDir)
	if err := os.MkdirAll(stalePackageDir, 0o755); err != nil {
		t.Fatalf("mkdir stale package dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stalePackageDir, "README.md"), []byte("stale\n"), 0o600); err != nil {
		t.Fatalf("write stale package file: %v", err)
	}

	staleBinPath := codexRetirePath(filepath.Join(globalBin, "codex"))
	if err := os.WriteFile(staleBinPath, []byte("stale\n"), 0o700); err != nil {
		t.Fatalf("write stale bin file: %v", err)
	}

	t.Setenv("PATH", globalBin)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	got, err := ensureCodexInstalled(context.Background(), "", io.Discard)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v", err)
	}
	if got != codexPath {
		t.Fatalf("expected codex path %q, got %q", codexPath, got)
	}
	if _, err := os.Stat(stalePackageDir); err != nil {
		t.Fatalf("expected stale package dir to remain, stat err=%v", err)
	}
	if _, err := os.Stat(staleBinPath); err != nil {
		t.Fatalf("expected stale bin path to remain, stat err=%v", err)
	}
	if _, err := os.Stat(packageDir); err != nil {
		t.Fatalf("expected active package dir to remain: %v", err)
	}
	if _, err := os.Stat(codexPath); err != nil {
		t.Fatalf("expected active codex to remain: %v", err)
	}
}

func TestCodexBinCandidatesForPrefixForOSWindowsIncludesAllNpmShims(t *testing.T) {
	prefix := t.TempDir()
	got := codexBinCandidatesForPrefixForOS("windows", prefix)
	for _, want := range []string{
		filepath.Join(prefix, "codex"),
		filepath.Join(prefix, "codex.cmd"),
		filepath.Join(prefix, "codex.ps1"),
		filepath.Join(prefix, "bin", "codex"),
		filepath.Join(prefix, "bin", "codex.cmd"),
		filepath.Join(prefix, "bin", "codex.ps1"),
	} {
		if !containsPath(got, want) {
			t.Fatalf("expected windows shim candidate %q in %v", want, got)
		}
	}
}

func TestCodexBinCandidatesForPrefixForOSEmptyPrefix(t *testing.T) {
	got := codexBinCandidatesForPrefixForOS("windows", "")
	if got != nil {
		t.Fatalf("expected nil candidates for empty prefix, got %v", got)
	}
}

func TestRunCodexUpgradeBySourceRejectsManagedWithoutPrefix(t *testing.T) {
	err := runCodexUpgradeBySource(context.Background(), io.Discard, nil, codexUpgradeSource{origin: codexInstallOriginManaged})
	if err == nil {
		t.Fatal("expected managed prefix error")
	}
	if !strings.Contains(err.Error(), "missing npm prefix") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunCodexUpgradeBySourceRejectsUnknownSource(t *testing.T) {
	err := runCodexUpgradeBySource(context.Background(), io.Discard, nil, codexUpgradeSource{origin: codexInstallOriginUnknown})
	if err == nil {
		t.Fatal("expected unknown-origin error")
	}
	if !strings.Contains(err.Error(), "cannot determine codex installation origin") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunCodexUpgradeBySourceSystemIgnoresManagedDiskTargets(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}
	lockCLITestHooks(t)

	root := t.TempDir()
	systemPrefix := filepath.Join(root, "system-prefix")
	managedPrefix := filepath.Join(root, "managed-prefix")
	managedNodeRoot := filepath.Join(root, "managed-node")
	tmpDir := filepath.Join(root, "tmp")
	for _, dir := range []string{systemPrefix, managedPrefix, managedNodeRoot, tmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	binDir := t.TempDir()
	marker := filepath.Join(root, "system-install-hit")
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + systemPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"if [ \"$1\" = \"install\" ] && [ \"$2\" = \"-g\" ] && [ \"$3\" = \"@openai/codex\" ]; then\n" +
		"  echo hit > \"" + marker + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{binDir, os.Getenv("PATH")}, string(os.PathListSeparator)))

	var checked []string
	prevDiskFree := codexInstallDiskFreeBytes
	t.Cleanup(func() { codexInstallDiskFreeBytes = prevDiskFree })
	codexInstallDiskFreeBytes = func(path string) (uint64, error) {
		checked = append(checked, filepath.Clean(path))
		if filepath.Clean(path) == filepath.Clean(managedPrefix) || filepath.Clean(path) == filepath.Clean(managedNodeRoot) {
			return 1024, nil
		}
		return 1024 * 1024 * 1024, nil
	}

	installerEnv := append(os.Environ(),
		"TMPDIR="+tmpDir,
		"CODEX_NPM_PREFIX="+managedPrefix,
		"CODEX_NODE_INSTALL_ROOT="+managedNodeRoot,
	)
	err := runCodexUpgradeBySource(context.Background(), io.Discard, installerEnv, codexUpgradeSource{
		origin:    codexInstallOriginSystem,
		npmPrefix: systemPrefix,
	})
	if err != nil {
		t.Fatalf("runCodexUpgradeBySource error: %v; checked paths: %v", err, checked)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected system npm install marker: %v", err)
	}
	for _, path := range checked {
		if path == filepath.Clean(managedPrefix) || path == filepath.Clean(managedNodeRoot) {
			t.Fatalf("system npm upgrade should not check managed disk target %q; checked paths: %v", path, checked)
		}
	}
}

func TestEnsureCodexInstallDiskSpaceWarnsAndContinuesWhenUnknown(t *testing.T) {
	lockCLITestHooks(t)

	prevDiskFree := codexInstallDiskFreeBytes
	t.Cleanup(func() { codexInstallDiskFreeBytes = prevDiskFree })
	codexInstallDiskFreeBytes = func(string) (uint64, error) {
		return 0, errors.New("statfs unavailable")
	}

	root := t.TempDir()
	installerEnv := []string{
		"TMPDIR=" + filepath.Join(root, "tmp"),
		"HOME=" + filepath.Join(root, "home"),
		"CODEX_NPM_PREFIX=" + filepath.Join(root, "npm-prefix"),
		"CODEX_NODE_INSTALL_ROOT=" + filepath.Join(root, "node-root"),
		envCodexInstallMinFreeKB + "=2048",
	}
	var out bytes.Buffer
	err := ensureCodexInstallDiskSpace(&out, installerEnv, []codexInstallDiskTarget{
		{label: "npm cache", path: filepath.Join(root, "npm-cache")},
	})
	if err != nil {
		t.Fatalf("disk check should continue when free space cannot be checked reliably: %v", err)
	}
	if !strings.Contains(out.String(), "warning: could not reliably check free disk space") {
		t.Fatalf("expected unreliable disk warning, got %q", out.String())
	}
}

func TestEnsureCodexInstallDiskSpaceDeduplicatesTargets(t *testing.T) {
	lockCLITestHooks(t)

	root := t.TempDir()
	prevDiskFree := codexInstallDiskFreeBytes
	t.Cleanup(func() { codexInstallDiskFreeBytes = prevDiskFree })
	calls := 0
	codexInstallDiskFreeBytes = func(path string) (uint64, error) {
		calls++
		if filepath.Clean(path) != filepath.Clean(root) {
			t.Fatalf("unexpected disk check path %q", path)
		}
		return 1024 * 1024 * 1024, nil
	}

	installerEnv := []string{
		"TMPDIR=" + root,
		"CODEX_NPM_PREFIX=" + root,
		"CODEX_NODE_INSTALL_ROOT=" + root,
		envCodexInstallMinFreeKB + "=2048",
	}
	err := ensureCodexInstallDiskSpace(io.Discard, installerEnv, []codexInstallDiskTarget{
		{label: "duplicate extra target", path: root},
	})
	if err != nil {
		t.Fatalf("ensureCodexInstallDiskSpace error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected duplicate disk targets to be checked once, got %d calls", calls)
	}
}

func TestUpgradeCodexInstalledWithOptionsManagedUsesManagedPrefix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}

	root := t.TempDir()
	prefix := filepath.Join(root, "custom-managed-prefix")
	codexDir := filepath.Join(prefix, "bin")
	if err := os.MkdirAll(codexDir, 0o755); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	codexPath := writeProbeableCodex(t, codexDir, true)
	marker := filepath.Join(root, "managed-install-hit")

	binDir := t.TempDir()
	bashPath := filepath.Join(binDir, "bash")
	script := "#!/bin/sh\n" +
		"if [ \"$CODEX_NPM_PREFIX\" != \"" + prefix + "\" ]; then\n" +
		"  echo \"unexpected CODEX_NPM_PREFIX=$CODEX_NPM_PREFIX\" >&2\n" +
		"  exit 1\n" +
		"fi\n" +
		"echo hit > \"" + marker + "\"\n" +
		"exit 0\n"
	writeExecutable(t, bashPath, script)

	t.Setenv("CODEX_NPM_PREFIX", prefix)
	t.Setenv("PATH", strings.Join([]string{codexDir, binDir}, string(os.PathListSeparator)))
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	got, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{upgradeCodex: true})
	if err != nil {
		t.Fatalf("upgradeCodexInstalledWithOptions error: %v", err)
	}
	if got != codexPath {
		t.Fatalf("expected codex path %q, got %q", codexPath, got)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected managed install marker: %v", err)
	}
}

func TestUpgradeCodexInstalledWithOptionsRemovesStaleSystemRetiredPaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}

	root := t.TempDir()
	globalPrefix := filepath.Join(root, "system-global")
	globalBin := filepath.Join(globalPrefix, "bin")
	if err := os.MkdirAll(globalBin, 0o755); err != nil {
		t.Fatalf("mkdir global bin: %v", err)
	}
	codexPath := writeProbeableCodex(t, globalBin, true)
	marker := filepath.Join(root, "npm-install-hit")

	packageDir := codexPackageDirForPrefix(globalPrefix)
	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		t.Fatalf("mkdir package dir: %v", err)
	}
	stalePackageDir := codexRetirePath(packageDir)
	if err := os.MkdirAll(stalePackageDir, 0o755); err != nil {
		t.Fatalf("mkdir stale package dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stalePackageDir, "README.md"), []byte("stale\n"), 0o600); err != nil {
		t.Fatalf("write stale package file: %v", err)
	}

	staleBinPath := codexRetirePath(filepath.Join(globalBin, "codex"))
	if err := os.WriteFile(staleBinPath, []byte("stale\n"), 0o700); err != nil {
		t.Fatalf("write stale bin file: %v", err)
	}

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + globalPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"if [ \"$1\" = \"install\" ] && [ \"$2\" = \"-g\" ] && [ \"$3\" = \"@openai/codex\" ]; then\n" +
		"  if [ -e \"" + stalePackageDir + "\" ] || [ -e \"" + staleBinPath + "\" ]; then\n" +
		"    echo \"stale retired path still present\" >&2\n" +
		"    exit 1\n" +
		"  fi\n" +
		"  echo hit > \"" + marker + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{globalBin, binDir}, string(os.PathListSeparator)))
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	got, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{upgradeCodex: true})
	if err != nil {
		t.Fatalf("upgradeCodexInstalledWithOptions error: %v", err)
	}
	if got != codexPath {
		t.Fatalf("expected codex path %q, got %q", codexPath, got)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected npm install marker: %v", err)
	}
	if _, err := os.Stat(stalePackageDir); !os.IsNotExist(err) {
		t.Fatalf("expected stale package dir to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(staleBinPath); !os.IsNotExist(err) {
		t.Fatalf("expected stale bin path to be removed, stat err=%v", err)
	}
}

func TestUpgradeCodexInstalledWithOptionsPropagatesUpgradeFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}

	root := t.TempDir()
	globalPrefix := filepath.Join(root, "system-global")
	globalBin := filepath.Join(globalPrefix, "bin")
	if err := os.MkdirAll(globalBin, 0o755); err != nil {
		t.Fatalf("mkdir global bin: %v", err)
	}
	_ = writeProbeableCodex(t, globalBin, true)

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + globalPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"if [ \"$1\" = \"install\" ] && [ \"$2\" = \"-g\" ] && [ \"$3\" = \"@openai/codex\" ]; then\n" +
		"  exit 7\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{globalBin, binDir}, string(os.PathListSeparator)))
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	_, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{upgradeCodex: true})
	if err == nil {
		t.Fatal("expected upgrade failure")
	}
	if !strings.Contains(err.Error(), "system npm codex upgrade failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpgradeCodexInstalledWithOptionsErrorsWhenCodexDisappearsAfterUpgrade(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}

	root := t.TempDir()
	globalPrefix := filepath.Join(root, "system-global")
	globalBin := filepath.Join(globalPrefix, "bin")
	if err := os.MkdirAll(globalBin, 0o755); err != nil {
		t.Fatalf("mkdir global bin: %v", err)
	}
	codexPath := writeProbeableCodex(t, globalBin, true)

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + globalPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"if [ \"$1\" = \"install\" ] && [ \"$2\" = \"-g\" ] && [ \"$3\" = \"@openai/codex\" ]; then\n" +
		"  cat > \"" + codexPath + "\" <<'EOF'\n" +
		"#!/bin/sh\n" +
		"exit 1\n" +
		"EOF\n" +
		"  chmod 700 \"" + codexPath + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{globalBin, binDir}, string(os.PathListSeparator)))
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	_, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{upgradeCodex: true})
	if err == nil {
		t.Fatal("expected nonfunctional-binary error")
	}
	if !strings.Contains(err.Error(), "codex upgrade finished but installed binary is not functional") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpgradeCodexInstalledWithOptionsFailsWhenCleanupFails(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}

	lockCLITestHooks(t)
	prevRemoveAll := codexRemoveAll
	t.Cleanup(func() { codexRemoveAll = prevRemoveAll })
	codexRemoveAll = func(string) error { return errors.New("boom") }

	root := t.TempDir()
	globalPrefix := filepath.Join(root, "system-global")
	globalBin := filepath.Join(globalPrefix, "bin")
	if err := os.MkdirAll(globalBin, 0o755); err != nil {
		t.Fatalf("mkdir global bin: %v", err)
	}
	_ = writeProbeableCodex(t, globalBin, true)

	staleBinPath := codexRetirePath(filepath.Join(globalBin, "codex"))
	if err := os.WriteFile(staleBinPath, []byte("stale\n"), 0o700); err != nil {
		t.Fatalf("write stale bin file: %v", err)
	}

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + globalPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"echo unexpected >&2\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{globalBin, binDir}, string(os.PathListSeparator)))
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	called := false
	_, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{
		upgradeCodex: true,
		withInstallerEnv: func(context.Context, func([]string) error) error {
			called = true
			return nil
		},
	})
	if err == nil {
		t.Fatal("expected cleanup failure")
	}
	if !strings.Contains(err.Error(), "remove stale npm retired path") {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Fatal("expected withInstallerEnv not to run when stale cleanup fails")
	}
}

func TestUpgradeCodexInstalledWithOptionsRejectsUnknownSource(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based upgrade test on windows")
	}

	codexDir := t.TempDir()
	_ = writeProbeableCodex(t, codexDir, true)

	binDir := t.TempDir()
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + filepath.Join(t.TempDir(), "different-prefix") + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)

	t.Setenv("PATH", strings.Join([]string{codexDir, binDir}, string(os.PathListSeparator)))
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	_, err := upgradeCodexInstalledWithOptions(context.Background(), io.Discard, codexInstallOptions{upgradeCodex: true})
	if err == nil {
		t.Fatal("expected unknown-source error")
	}
	if !strings.Contains(err.Error(), "cannot determine codex installation origin") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunCodexInstallerFallsBackToNextCandidate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell fallback test on windows")
	}
	binDir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "fallback-hit")

	bashPath := filepath.Join(binDir, "bash")
	if err := os.WriteFile(bashPath, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatalf("write fake bash: %v", err)
	}

	shPath := filepath.Join(binDir, "sh")
	shScript := "#!/bin/sh\n" +
		"echo ok > \"" + marker + "\"\n" +
		"exit 0\n"
	if err := os.WriteFile(shPath, []byte(shScript), 0o700); err != nil {
		t.Fatalf("write fake sh: %v", err)
	}

	t.Setenv("PATH", binDir)
	if err := runCodexInstaller(context.Background(), io.Discard, nil); err != nil {
		t.Fatalf("runCodexInstaller error: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected fallback marker file: %v", err)
	}
}

func TestRunCodexInstallerReturnsCombinedErrors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell fallback test on windows")
	}
	binDir := t.TempDir()
	failScript := []byte("#!/bin/sh\nexit 1\n")
	if err := os.WriteFile(filepath.Join(binDir, "bash"), failScript, 0o700); err != nil {
		t.Fatalf("write fake bash: %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "sh"), failScript, 0o700); err != nil {
		t.Fatalf("write fake sh: %v", err)
	}

	t.Setenv("PATH", binDir)
	err := runCodexInstaller(context.Background(), io.Discard, nil)
	if err == nil {
		t.Fatal("expected install failure")
	}
	msg := err.Error()
	if !strings.Contains(msg, "bash -c:") {
		t.Fatalf("expected bash attempt in error, got %q", msg)
	}
	if !strings.Contains(msg, "sh -c:") {
		t.Fatalf("expected sh attempt in error, got %q", msg)
	}
}

func TestRunCodexInstallerDiskSpacePrecheckFailsBeforeShell(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell installer test on windows")
	}
	lockCLITestHooks(t)
	prevDiskFree := codexInstallDiskFreeBytes
	t.Cleanup(func() { codexInstallDiskFreeBytes = prevDiskFree })
	codexInstallDiskFreeBytes = func(string) (uint64, error) {
		return 1024, nil
	}

	binDir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "installer-ran")
	script := "#!/bin/sh\n" +
		"echo ran > \"" + marker + "\"\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "bash"), []byte(script), 0o700); err != nil {
		t.Fatalf("write fake bash: %v", err)
	}

	t.Setenv("PATH", binDir)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("TMPDIR", t.TempDir())

	var out bytes.Buffer
	err := runCodexInstaller(context.Background(), &out, nil)
	if err == nil {
		t.Fatal("expected disk space error")
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("installer should not run after disk precheck failure, stat err=%v", statErr)
	}
	text := out.String()
	if !strings.Contains(text, "CODEX CLI INSTALL FAILED") {
		t.Fatalf("expected failure banner, got %q", text)
	}
	if !strings.Contains(text, "Not enough disk space") {
		t.Fatalf("expected disk space reason, got %q", text)
	}
}

func TestRunCodexInstallerRechecksDiskSpaceAfterShellFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell installer test on windows")
	}
	lockCLITestHooks(t)

	binDir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "installer-ran")
	script := "#!/bin/sh\n" +
		"echo ran > \"" + marker + "\"\n" +
		"exit 1\n"
	if err := os.WriteFile(filepath.Join(binDir, "bash"), []byte(script), 0o700); err != nil {
		t.Fatalf("write fake bash: %v", err)
	}

	prevDiskFree := codexInstallDiskFreeBytes
	t.Cleanup(func() { codexInstallDiskFreeBytes = prevDiskFree })
	codexInstallDiskFreeBytes = func(string) (uint64, error) {
		if _, err := os.Stat(marker); err == nil {
			return 1024, nil
		}
		return 1024 * 1024 * 1024, nil
	}

	t.Setenv("PATH", binDir)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("TMPDIR", t.TempDir())

	var out bytes.Buffer
	err := runCodexInstaller(context.Background(), &out, nil)
	if err == nil {
		t.Fatal("expected disk space error")
	}
	text := out.String()
	if !strings.Contains(text, "CODEX CLI INSTALL FAILED") {
		t.Fatalf("expected failure banner, got %q", text)
	}
	if !strings.Contains(text, "Not enough disk space") {
		t.Fatalf("expected disk space reason, got %q", text)
	}
	if strings.Contains(err.Error(), "bash -c") {
		t.Fatalf("expected disk error to replace shell attempt error, got %v", err)
	}
}

func TestRunSystemNpmCodexUpgradeDiskPrecheckFailsBeforeInstall(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based npm test on windows")
	}
	lockCLITestHooks(t)

	root := t.TempDir()
	systemPrefix := filepath.Join(root, "system-prefix")
	tmpDir := filepath.Join(root, "tmp")
	if err := os.MkdirAll(systemPrefix, 0o755); err != nil {
		t.Fatalf("mkdir system prefix: %v", err)
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatalf("mkdir tmp dir: %v", err)
	}

	binDir := t.TempDir()
	marker := filepath.Join(root, "npm-install-ran")
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + systemPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"if [ \"$1\" = \"install\" ]; then\n" +
		"  echo ran > \"" + marker + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)
	t.Setenv("PATH", binDir)

	prevDiskFree := codexInstallDiskFreeBytes
	t.Cleanup(func() { codexInstallDiskFreeBytes = prevDiskFree })
	codexInstallDiskFreeBytes = func(path string) (uint64, error) {
		if filepath.Clean(path) == filepath.Clean(systemPrefix) {
			return 1024, nil
		}
		return 1024 * 1024 * 1024, nil
	}

	var out bytes.Buffer
	err := runSystemNpmCodexUpgrade(context.Background(), &out, []string{
		"PATH=" + binDir,
		"TMPDIR=" + tmpDir,
		envCodexInstallMinFreeKB + "=2048",
	})
	if err == nil {
		t.Fatal("expected disk space error")
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("npm install should not run after disk precheck failure, stat err=%v", statErr)
	}
	if !strings.Contains(out.String(), "CODEX CLI INSTALL FAILED") || !strings.Contains(err.Error(), "Not enough disk space") {
		t.Fatalf("expected explicit disk failure, err=%v output=%q", err, out.String())
	}
}

func TestRunSystemNpmCodexUpgradeReportsDiskSpaceAfterNpmFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based npm test on windows")
	}
	lockCLITestHooks(t)

	root := t.TempDir()
	systemPrefix := filepath.Join(root, "system-prefix")
	tmpDir := filepath.Join(root, "tmp")
	if err := os.MkdirAll(systemPrefix, 0o755); err != nil {
		t.Fatalf("mkdir system prefix: %v", err)
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatalf("mkdir tmp dir: %v", err)
	}

	binDir := t.TempDir()
	marker := filepath.Join(root, "npm-install-ran")
	npmPath := filepath.Join(binDir, "npm")
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"prefix\" ] && [ \"$2\" = \"-g\" ]; then\n" +
		"  echo \"" + systemPrefix + "\"\n" +
		"  exit 0\n" +
		"fi\n" +
		"if [ \"$1\" = \"install\" ] && [ \"$2\" = \"-g\" ] && [ \"$3\" = \"@openai/codex\" ]; then\n" +
		"  echo ran > \"" + marker + "\"\n" +
		"  exit 17\n" +
		"fi\n" +
		"exit 1\n"
	writeExecutable(t, npmPath, script)
	t.Setenv("PATH", binDir)

	prevDiskFree := codexInstallDiskFreeBytes
	t.Cleanup(func() { codexInstallDiskFreeBytes = prevDiskFree })
	codexInstallDiskFreeBytes = func(path string) (uint64, error) {
		if _, err := os.Stat(marker); err == nil && filepath.Clean(path) == filepath.Clean(systemPrefix) {
			return 1024, nil
		}
		return 1024 * 1024 * 1024, nil
	}

	var out bytes.Buffer
	err := runSystemNpmCodexUpgrade(context.Background(), &out, []string{
		"PATH=" + binDir,
		"TMPDIR=" + tmpDir,
		envCodexInstallMinFreeKB + "=2048",
	})
	if err == nil {
		t.Fatal("expected disk space error after npm failure")
	}
	if !strings.Contains(out.String(), "CODEX CLI INSTALL FAILED") || !strings.Contains(err.Error(), "Not enough disk space") {
		t.Fatalf("expected explicit disk failure, err=%v output=%q", err, out.String())
	}
	if strings.Contains(err.Error(), "system npm codex upgrade failed") {
		t.Fatalf("expected disk error to replace npm exit error, got %v", err)
	}
}

func TestResolveRunCommandIgnoresNonCodex(t *testing.T) {
	args := []string{"echo", "hello"}
	got, err := resolveRunCommand(context.Background(), args, io.Discard)
	if err != nil {
		t.Fatalf("resolveRunCommand error: %v", err)
	}
	if len(got) != len(args) || got[0] != args[0] || got[1] != args[1] {
		t.Fatalf("expected unchanged args, got %v", got)
	}
}

func TestResolveRunCommandInstallsCodexWhenMissing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	home := t.TempDir()
	installDir := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin")
	codexPath := filepath.Join(installDir, "codex")

	binDir := t.TempDir()
	installer := filepath.Join(binDir, "bash")
	script := "#!/bin/sh\n" +
		"mkdir -p \"" + installDir + "\"\n" +
		"cat > \"" + codexPath + "\" <<'EOF'\n" +
		"#!/bin/sh\n" +
		"exit 0\n" +
		"EOF\n" +
		"chmod +x \"" + codexPath + "\"\n"
	if err := os.WriteFile(installer, []byte(script), 0o700); err != nil {
		t.Fatalf("write installer: %v", err)
	}

	t.Setenv("HOME", home)
	t.Setenv("PATH", strings.Join([]string{binDir, "/usr/bin", "/bin"}, string(os.PathListSeparator)))

	got, err := resolveRunCommand(context.Background(), []string{"codex", "--help"}, io.Discard)
	if err != nil {
		t.Fatalf("resolveRunCommand error: %v", err)
	}
	if len(got) != 2 || got[0] != codexPath || got[1] != "--help" {
		t.Fatalf("expected installed codex command, got %v", got)
	}
}

func TestResolveRunCommandResolvesCodexOnPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell PATH test on windows")
	}
	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write fake codex: %v", err)
	}
	marker := filepath.Join(t.TempDir(), "installer-called")
	bashPath := filepath.Join(binDir, "bash")
	bashScript := "#!/bin/sh\n" +
		"echo called > \"" + marker + "\"\n" +
		"exit 0\n"
	if err := os.WriteFile(bashPath, []byte(bashScript), 0o700); err != nil {
		t.Fatalf("write fake bash: %v", err)
	}

	t.Setenv("PATH", binDir)
	args := []string{"codex", "--help"}
	got, err := resolveRunCommand(context.Background(), args, io.Discard)
	if err != nil {
		t.Fatalf("resolveRunCommand error: %v", err)
	}
	if len(got) != len(args) || got[0] != codexPath || got[1] != args[1] {
		t.Fatalf("expected resolved codex path, got %v", got)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("installer should not run when codex is already on PATH")
	}
}

func TestResolveRunCommandFailsForMissingExplicitCodexPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "codex")
	_, err := resolveRunCommand(context.Background(), []string{missing, "--help"}, io.Discard)
	if err == nil {
		t.Fatal("expected error for missing explicit codex path")
	}
	if !strings.Contains(err.Error(), "codex not found at") {
		t.Fatalf("expected missing-path error, got %q", err.Error())
	}
}

func TestFindInstalledCodexUsesFallbackCandidates(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip unix fallback path test on windows")
	}
	home := t.TempDir()
	candidate := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex")
	if err := os.MkdirAll(filepath.Dir(candidate), 0o755); err != nil {
		t.Fatalf("mkdir candidate dir: %v", err)
	}
	if err := os.WriteFile(candidate, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write candidate codex: %v", err)
	}

	t.Setenv("HOME", home)
	t.Setenv("PATH", t.TempDir())
	got, err := findInstalledCodex(context.Background())
	if err != nil {
		t.Fatalf("findInstalledCodex error: %v", err)
	}
	if got != candidate {
		t.Fatalf("expected candidate %q, got %q", candidate, got)
	}
}

func TestFindInstalledCodexUsesPathLookupFirst(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip unix fallback path test on windows")
	}
	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}

	t.Setenv("HOME", t.TempDir())
	t.Setenv("PATH", binDir)
	got, err := findInstalledCodex(context.Background())
	if err != nil {
		t.Fatalf("findInstalledCodex error: %v", err)
	}
	if got != codexPath {
		t.Fatalf("expected PATH codex %q, got %q", codexPath, got)
	}
}

func TestFindInstalledCodexReturnsErrorWhenMissing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip unix fallback path test on windows")
	}
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("PATH", t.TempDir())
	_, err := findInstalledCodex(context.Background())
	if err == nil {
		t.Fatal("expected error when codex is unavailable")
	}
	if !strings.Contains(err.Error(), "codex binary not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFindInstalledCodexUsesCustomPrefixCandidate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip unix custom prefix candidate test on windows")
	}

	prefix := t.TempDir()
	binDir := filepath.Join(prefix, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir prefix bin: %v", err)
	}
	candidate := writeProbeableCodex(t, binDir, true)

	t.Setenv("CODEX_NPM_PREFIX", prefix)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir())

	got, err := findInstalledCodex(context.Background())
	if err != nil {
		t.Fatalf("findInstalledCodex error: %v", err)
	}
	if got != candidate {
		t.Fatalf("expected custom-prefix codex %q, got %q", candidate, got)
	}
}

func TestCodexBinaryCandidatesIncludeCustomPrefix(t *testing.T) {
	prefix := t.TempDir()
	t.Setenv("CODEX_NPM_PREFIX", prefix)

	candidates := codexBinaryCandidates()

	var expected string
	if runtime.GOOS == "windows" {
		expected = filepath.Join(prefix, "codex.cmd")
	} else {
		expected = filepath.Join(prefix, "bin", "codex")
	}
	if !containsPath(candidates, expected) {
		t.Fatalf("expected custom prefix candidate %q in %v", expected, candidates)
	}
}

func TestManagedNodeBinCandidatesForEnvUnixDefaultRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix-only test")
	}
	arch := nodeRuntimeArch(runtime.GOARCH)
	if arch == "" {
		t.Skip("unsupported runtime arch for managed-node test")
	}

	home := t.TempDir()
	installDir := filepath.Join(home, ".cache", "codex-proxy", "node", "v22-linux-"+arch)
	nodeBin := filepath.Join(installDir, "bin")
	if err := os.MkdirAll(nodeBin, 0o755); err != nil {
		t.Fatalf("mkdir node bin: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nodeBin, "node"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write node: %v", err)
	}

	candidates := managedNodeBinCandidatesForEnv("linux", runtime.GOARCH, home, "", "", "", "")
	if !containsPath(candidates, nodeBin) {
		t.Fatalf("expected managed node bin candidate %q in %v", nodeBin, candidates)
	}
}

func TestManagedNodeBinCandidatesForEnvWindowsDefaultRoot(t *testing.T) {
	arch := nodeRuntimeArch(runtime.GOARCH)
	if arch == "" {
		t.Skip("unsupported runtime arch for managed-node test")
	}

	localAppData := t.TempDir()
	installDir := filepath.Join(localAppData, "codex-proxy", "node", "v22-win-"+arch)
	if err := os.MkdirAll(installDir, 0o755); err != nil {
		t.Fatalf("mkdir node dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(installDir, "node.exe"), []byte("node"), 0o700); err != nil {
		t.Fatalf("write node.exe: %v", err)
	}

	candidates := managedNodeBinCandidatesForEnv("windows", runtime.GOARCH, "", "", "", localAppData, "")
	if !containsPath(candidates, installDir) {
		t.Fatalf("expected managed node install dir candidate %q in %v", installDir, candidates)
	}
}

func TestEnsureCodexInstalledUsesManagedNodeForCandidateProbe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	arch := nodeRuntimeArch(runtime.GOARCH)
	if arch == "" {
		t.Skip("unsupported runtime arch for managed-node probe test")
	}

	home := t.TempDir()
	nodeBin := filepath.Join(home, ".cache", "codex-proxy", "node", "v22-"+runtime.GOOS+"-"+arch, "bin")
	if err := os.MkdirAll(nodeBin, 0o755); err != nil {
		t.Fatalf("mkdir node bin: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nodeBin, "node"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write node: %v", err)
	}

	codexPath := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex")
	if err := os.MkdirAll(filepath.Dir(codexPath), 0o755); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	codexScript := "#!/bin/sh\n" +
		"command -v node >/dev/null 2>&1 || exit 1\n" +
		"exit 0\n"
	if err := os.WriteFile(codexPath, []byte(codexScript), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}

	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir())

	got, err := ensureCodexInstalled(context.Background(), "", io.Discard)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v", err)
	}
	if got != codexPath {
		t.Fatalf("expected candidate codex %q, got %q", codexPath, got)
	}
	if !containsPath(filepath.SplitList(os.Getenv("PATH")), nodeBin) {
		t.Fatalf("expected PATH to include managed node bin %q, got %q", nodeBin, os.Getenv("PATH"))
	}
}

func TestCodexBinaryCandidatesForWindowsIncludeCustomPrefix(t *testing.T) {
	prefix := "/custom/prefix"
	candidates := codexBinaryCandidatesForEnv("windows", "", prefix, "", "", "")
	if !containsPath(candidates, filepath.Join(prefix, "codex.cmd")) {
		t.Fatalf("expected windows custom-prefix cmd candidate in %v", candidates)
	}
	if !containsPath(candidates, filepath.Join(prefix, "bin", "codex.cmd")) {
		t.Fatalf("expected windows custom-prefix bin cmd candidate in %v", candidates)
	}
}

func TestCodexBinaryCandidatesForWindowsIncludeAppDataCandidates(t *testing.T) {
	localAppData := "/localapp"
	appData := "/appdata"
	candidates := codexBinaryCandidatesForEnv("windows", "", "", localAppData, appData, "")
	if !containsPath(candidates, filepath.Join(localAppData, "codex-proxy", "npm-global", "codex.cmd")) {
		t.Fatalf("expected local appdata codex candidate in %v", candidates)
	}
	if !containsPath(candidates, filepath.Join(appData, "npm", "codex.cmd")) {
		t.Fatalf("expected roaming appdata codex candidate in %v", candidates)
	}
}

func TestCodexBinaryCandidatesForWindowsIncludeTempFallback(t *testing.T) {
	tempDir := "/temp-fallback"
	candidates := codexBinaryCandidatesForEnv("windows", "", "", "", "", tempDir)
	want := filepath.Join(tempDir, "codex-proxy", "npm-global", "codex.cmd")
	if !containsPath(candidates, want) {
		t.Fatalf("expected temp fallback candidate %q in %v", want, candidates)
	}
}

func TestEnsureCodexInstalledClearsRelativeCachedPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip cache path test on windows")
	}

	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv("PATH", t.TempDir())

	cacheFile := cachedCodexPathFile()
	if err := os.MkdirAll(filepath.Dir(cacheFile), 0o755); err != nil {
		t.Fatalf("mkdir cache dir: %v", err)
	}
	if err := os.WriteFile(cacheFile, []byte("./relative/codex\n"), 0o600); err != nil {
		t.Fatalf("write cache file: %v", err)
	}

	_, _ = ensureCodexInstalled(context.Background(), "", io.Discard)
	if got := readCachedCodexPath(); got != "" {
		t.Fatalf("expected relative cached path to be cleared, got %q", got)
	}
}

func TestCodexInstallLockIsContended(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "codex_install.lock")

	if codexInstallLockIsContended(lockPath, nil) {
		t.Fatal("nil error should not be treated as lock contention")
	}
	if !codexInstallLockIsContended(lockPath, os.ErrPermission) {
		t.Fatal("permission error should be treated as lock contention")
	}
	if codexInstallLockIsContended(lockPath, io.EOF) {
		t.Fatal("unexpected non-lock error should not be treated as contention")
	}

	if err := os.Mkdir(lockPath, 0o700); err != nil {
		t.Fatalf("create lock dir: %v", err)
	}
	if !codexInstallLockIsContended(lockPath, io.EOF) {
		t.Fatal("existing lock path should be treated as contention")
	}
}

func TestWithCodexInstallLockFallsBackAfterTimeout(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	if runtime.GOOS == "windows" {
		t.Setenv("LOCALAPPDATA", t.TempDir())
		t.Setenv("APPDATA", t.TempDir())
	}

	lockPath := codexInstallLockPath()
	if lockPath == "" {
		t.Fatal("expected non-empty lock path")
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	if err := os.Mkdir(lockPath, 0o700); err != nil {
		t.Fatalf("create held lock: %v", err)
	}
	defer os.Remove(lockPath)

	prevPoll := codexInstallLockPollDelay
	prevWait := codexInstallLockMaxWait
	codexInstallLockPollDelay = 10 * time.Millisecond
	codexInstallLockMaxWait = 40 * time.Millisecond
	defer func() {
		codexInstallLockPollDelay = prevPoll
		codexInstallLockMaxWait = prevWait
	}()

	var out bytes.Buffer
	called := false
	if err := withCodexInstallLock(context.Background(), &out, func() error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("withCodexInstallLock error: %v", err)
	}
	if !called {
		t.Fatal("expected function to run after lock timeout fallback")
	}
	log := out.String()
	if !strings.Contains(log, "waiting up to") {
		t.Fatalf("expected wait log, got %q", log)
	}
	if !strings.Contains(log, "continuing without lock") {
		t.Fatalf("expected fallback log, got %q", log)
	}
}

func TestWithCodexInstallLockAcquiresAfterRelease(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	if runtime.GOOS == "windows" {
		t.Setenv("LOCALAPPDATA", t.TempDir())
		t.Setenv("APPDATA", t.TempDir())
	}

	lockPath := codexInstallLockPath()
	if lockPath == "" {
		t.Fatal("expected non-empty lock path")
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	if err := os.Mkdir(lockPath, 0o700); err != nil {
		t.Fatalf("create held lock: %v", err)
	}
	defer os.Remove(lockPath)

	prevPoll := codexInstallLockPollDelay
	prevWait := codexInstallLockMaxWait
	codexInstallLockPollDelay = 10 * time.Millisecond
	codexInstallLockMaxWait = 2 * time.Second
	defer func() {
		codexInstallLockPollDelay = prevPoll
		codexInstallLockMaxWait = prevWait
	}()

	removeDone := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(1500 * time.Millisecond)
		for {
			time.Sleep(25 * time.Millisecond)
			err := os.Remove(lockPath)
			if err == nil || os.IsNotExist(err) {
				removeDone <- nil
				return
			}
			if time.Now().After(deadline) {
				removeDone <- err
				return
			}
		}
	}()

	var out bytes.Buffer
	called := false
	if err := withCodexInstallLock(context.Background(), &out, func() error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("withCodexInstallLock error: %v", err)
	}
	if !called {
		t.Fatal("expected function to run after lock release")
	}
	select {
	case err := <-removeDone:
		if err != nil {
			t.Fatalf("remove held lock: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for held lock to be released")
	}
	if strings.Contains(out.String(), "continuing without lock") {
		t.Fatalf("unexpected fallback log when lock should be acquired: %q", out.String())
	}
}

func TestProbeCodexSuccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell probe test on windows")
	}
	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}
	if !probeCodex(context.Background(), codexPath) {
		t.Fatal("expected probe to succeed for working binary")
	}
}

func TestProbeCodexFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell probe test on windows")
	}
	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}
	if probeCodex(context.Background(), codexPath) {
		t.Fatal("expected probe to fail for broken binary")
	}
}

func TestEnsureCodexInstalledSkipsBrokenInPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	home := t.TempDir()
	installDir := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin")
	goodCodex := filepath.Join(installDir, "codex")

	// Create a broken codex on PATH (simulates WSL finding Windows codex).
	brokenDir := t.TempDir()
	brokenCodex := filepath.Join(brokenDir, "codex")
	if err := os.WriteFile(brokenCodex, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatalf("write broken codex: %v", err)
	}

	// Installer that creates a working codex at the known candidate path.
	installerDir := t.TempDir()
	installer := filepath.Join(installerDir, "bash")
	script := "#!/bin/sh\n" +
		"mkdir -p \"" + installDir + "\"\n" +
		"cat > \"" + goodCodex + "\" <<'SCRIPT'\n" +
		"#!/bin/sh\nexit 0\n" +
		"SCRIPT\n" +
		"chmod +x \"" + goodCodex + "\"\n"
	if err := os.WriteFile(installer, []byte(script), 0o700); err != nil {
		t.Fatalf("write installer: %v", err)
	}

	t.Setenv("HOME", home)
	// Broken codex is first in PATH; installer bash is also available.
	t.Setenv("PATH", strings.Join([]string{brokenDir, installerDir, "/usr/bin", "/bin"}, string(os.PathListSeparator)))

	var out bytes.Buffer
	got, err := ensureCodexInstalled(context.Background(), "", &out)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v", err)
	}
	if got != goodCodex {
		t.Fatalf("expected installed path %q, got %q", goodCodex, got)
	}
	if !strings.Contains(out.String(), "not functional") {
		t.Fatalf("expected 'not functional' log, got %q", out.String())
	}
}

func TestFindInstalledCodexSkipsBrokenInPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell test on windows")
	}
	home := t.TempDir()

	// Broken codex on PATH.
	brokenDir := t.TempDir()
	brokenCodex := filepath.Join(brokenDir, "codex")
	if err := os.WriteFile(brokenCodex, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatalf("write broken codex: %v", err)
	}

	// Working codex at known candidate path.
	candidate := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex")
	if err := os.MkdirAll(filepath.Dir(candidate), 0o755); err != nil {
		t.Fatalf("mkdir candidate dir: %v", err)
	}
	if err := os.WriteFile(candidate, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write candidate codex: %v", err)
	}

	t.Setenv("HOME", home)
	t.Setenv("PATH", brokenDir)

	got, err := findInstalledCodex(context.Background())
	if err != nil {
		t.Fatalf("findInstalledCodex error: %v", err)
	}
	if got != candidate {
		t.Fatalf("expected candidate %q, got broken %q", candidate, got)
	}
}

func TestBootstrapScriptContainsWSLDetection(t *testing.T) {
	if !strings.Contains(codexInstallBootstrap, "is_wsl") {
		t.Fatal("bootstrap script missing WSL detection")
	}
	if !strings.Contains(codexInstallBootstrap, "/mnt/") {
		t.Fatal("bootstrap script missing /mnt/ path check for WSL")
	}
}

func TestBootstrapScriptContainsHomeFallback(t *testing.T) {
	if !strings.Contains(codexInstallBootstrap, "cd ~") {
		t.Fatal("bootstrap script missing home-dir fallback via shell expansion")
	}
}

func TestBootstrapScriptContainsDiskSpacePreflight(t *testing.T) {
	for _, want := range []string{
		"CODEX_PROXY_CODEX_INSTALL_MIN_FREE_KB",
		"check_disk_space \"temporary directory\"",
		"check_disk_space \"managed npm prefix\"",
		"check_disk_space \"npm cache\"",
		"fail_write_or_disk",
		"CODEX CLI INSTALL FAILED",
	} {
		if !strings.Contains(codexInstallBootstrap, want) {
			t.Fatalf("bootstrap script missing %q", want)
		}
	}
}

func TestBootstrapScriptChecksNpmUsability(t *testing.T) {
	for _, want := range []string{
		"npm_usable_with_path",
		"system npm is not usable",
		"managed Node.js/npm install is missing or broken; reinstalling",
		"prefix -g",
	} {
		if !strings.Contains(codexInstallBootstrap, want) {
			t.Fatalf("bootstrap script missing %q", want)
		}
	}
}

func TestBootstrapScriptContainsSelfValidation(t *testing.T) {
	if !strings.Contains(codexInstallBootstrap, "used_system_node") {
		t.Fatal("bootstrap script missing used_system_node flag")
	}
	if !strings.Contains(codexInstallBootstrap, "download_local_node") {
		t.Fatal("bootstrap script missing download_local_node function")
	}
	if !strings.Contains(codexInstallBootstrap, "--version") {
		t.Fatal("bootstrap script missing post-install probe")
	}
}

func TestBootstrapScriptContainsLegacyGlibcFallback(t *testing.T) {
	if !strings.Contains(codexInstallBootstrap, "unofficial-builds.nodejs.org") {
		t.Fatal("bootstrap script missing unofficial-builds mirror for legacy glibc")
	}
	if !strings.Contains(codexInstallBootstrap, "glibc-217") {
		t.Fatal("bootstrap script missing glibc-217 tarball selection")
	}
	if !strings.Contains(codexInstallBootstrap, "download/release/index.tab") {
		t.Fatal("bootstrap script missing unofficial-builds index lookup")
	}
	if !strings.Contains(codexInstallBootstrap, "getconf GNU_LIBC_VERSION") {
		t.Fatal("bootstrap script missing glibc detection")
	}
}

func TestBootstrapWindowsScriptContainsSelfValidation(t *testing.T) {
	if !strings.Contains(codexInstallBootstrapWindows, "$usedSystemNode") {
		t.Fatal("Windows bootstrap script missing usedSystemNode flag")
	}
	if !strings.Contains(codexInstallBootstrapWindows, "Install-LocalNode") {
		t.Fatal("Windows bootstrap script missing Install-LocalNode function")
	}
	if !strings.Contains(codexInstallBootstrapWindows, "Set-CodexManagedNodeShims") {
		t.Fatal("Windows bootstrap script missing managed Node shim patch")
	}
	if !strings.Contains(codexInstallBootstrapWindows, "codex.cmd") {
		t.Fatal("Windows bootstrap script missing codex.cmd shim patch")
	}
	if !strings.Contains(codexInstallBootstrapWindows, "codex.ps1") {
		t.Fatal("Windows bootstrap script missing codex.ps1 shim patch")
	}
	if !strings.Contains(codexInstallBootstrapWindows, "CODEX_NODE_INSTALL_ROOT") {
		t.Fatal("Windows bootstrap script missing managed Node root lookup in shim")
	}
	if strings.Contains(codexInstallBootstrapWindows, "Join-Path $npmPrefix 'node.cmd'") {
		t.Fatal("Windows bootstrap script must not publish a generic node.cmd in the npm prefix")
	}
	if !strings.Contains(codexInstallBootstrapWindows, "--version") {
		t.Fatal("Windows bootstrap script missing post-install probe")
	}
	if !strings.Contains(codexInstallBootstrapWindows, "codex installation finished but") {
		t.Fatal("Windows bootstrap script missing final probe failure")
	}
}

func TestBootstrapWindowsScriptContainsDiskAndNpmChecks(t *testing.T) {
	for _, want := range []string{
		"CODEX_PROXY_CODEX_INSTALL_MIN_FREE_KB",
		"Assert-DiskSpace \"temporary directory\"",
		"Test-NpmUsable",
		"Invoke-DiskWrite",
		"Fail-IfDiskSpaceLow",
		"Assert-DiskSpace \"npm cache\"",
		"Get-CodexSHA256Hex",
		"System.Security.Cryptography.SHA256",
		"system npm is not usable",
		"managed Node.js/npm install is missing or broken; reinstalling",
	} {
		if !strings.Contains(codexInstallBootstrapWindows, want) {
			t.Fatalf("Windows bootstrap script missing %q", want)
		}
	}
}

func TestProbeCodexIntegration(t *testing.T) {
	if os.Getenv("CODEX_PATCH_TEST") != "1" {
		t.Skip("skipping: set CODEX_PATCH_TEST=1 to run against real codex")
	}

	path, err := exec.LookPath("codex")
	if err != nil {
		t.Fatalf("codex not found in PATH: %v", err)
	}
	t.Logf("codex found at: %s", path)

	if !probeCodex(context.Background(), path) {
		t.Fatalf("probeCodex returned false for %s", path)
	}

	found, err := findInstalledCodex(context.Background())
	if err != nil {
		t.Fatalf("findInstalledCodex error: %v", err)
	}
	t.Logf("findInstalledCodex returned: %s", found)
}

func TestEnsureCodexInstalledIntegrationManagedNode(t *testing.T) {
	if os.Getenv("CODEX_INSTALL_TEST") != "1" {
		t.Skip("skipping: set CODEX_INSTALL_TEST=1 to run installer integration")
	}

	arch := nodeRuntimeArch(runtime.GOARCH)
	if arch == "" {
		t.Skip("unsupported runtime arch for managed-node integration")
	}

	root := t.TempDir()
	home := filepath.Join(root, "home")
	cacheDir := filepath.Join(root, "cache")
	npmPrefix := filepath.Join(root, "npm-global")
	nodeRoot := filepath.Join(root, "node")
	npmCache := filepath.Join(root, "npm-cache")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatalf("mkdir home: %v", err)
	}

	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CACHE_HOME", cacheDir)
	t.Setenv("npm_config_cache", npmCache)
	t.Setenv("CODEX_NPM_PREFIX", npmPrefix)
	t.Setenv("CODEX_NODE_INSTALL_ROOT", nodeRoot)
	t.Setenv("CODEX_NODE_MIN_MAJOR", "999")
	t.Setenv("CODEX_NODE_MAJOR", "22")
	t.Setenv("PATH", managedNodeIntegrationPath(t))
	clearCachedCodexPath()

	var out bytes.Buffer
	got, err := ensureCodexInstalled(context.Background(), "", &out)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v\ninstaller output:\n%s", err, out.String())
	}
	if !probeCodex(context.Background(), got) {
		t.Fatalf("installed codex is not functional: %s", got)
	}
	if !strings.HasPrefix(filepath.Clean(got), filepath.Clean(npmPrefix)+string(os.PathSeparator)) {
		t.Fatalf("expected installed codex under npm prefix %q, got %q", npmPrefix, got)
	}

	nodeBin, nodePath := managedNodeIntegrationNodePaths(nodeRoot, arch)
	if !executableExists(nodePath) {
		t.Fatalf("expected managed node binary at %q, installer output:\n%s", nodePath, out.String())
	}
	if !containsPath(filepath.SplitList(os.Getenv("PATH")), nodeBin) {
		t.Fatalf("expected PATH to include managed node bin %q, got %q", nodeBin, os.Getenv("PATH"))
	}

	if runtime.GOOS == "windows" {
		assertWindowsManagedCodexInstall(t, npmPrefix, out.String())
	}
}

func managedNodeIntegrationPath(t *testing.T) string {
	t.Helper()

	if runtime.GOOS != "windows" {
		// Keep PATH minimal so no usable system node/npm is discovered.
		return "/usr/bin:/bin"
	}

	pathDirs := make([]string, 0, 2)
	if systemRoot := os.Getenv("SystemRoot"); systemRoot != "" {
		system32 := filepath.Join(systemRoot, "System32")
		if !containsPath(pathDirs, system32) {
			pathDirs = append(pathDirs, system32)
		}
	}
	for _, name := range []string{"powershell", "pwsh"} {
		path, err := exec.LookPath(name)
		if err != nil {
			continue
		}
		dir := filepath.Dir(path)
		if !containsPath(pathDirs, dir) {
			pathDirs = append(pathDirs, dir)
		}
	}
	if len(pathDirs) == 0 {
		t.Skip("powershell/pwsh not available for Windows managed-node integration")
	}
	return strings.Join(pathDirs, string(os.PathListSeparator))
}

func managedNodeIntegrationNodePaths(nodeRoot string, arch string) (nodeBin string, nodePath string) {
	if runtime.GOOS == "windows" {
		nodeBin = filepath.Join(nodeRoot, "v22-win-"+arch)
		nodePath = filepath.Join(nodeBin, "node.exe")
		return nodeBin, nodePath
	}
	nodeBin = filepath.Join(nodeRoot, "v22-"+runtime.GOOS+"-"+arch, "bin")
	nodePath = filepath.Join(nodeBin, "node")
	return nodeBin, nodePath
}

func assertWindowsManagedCodexInstall(t *testing.T, npmPrefix string, installerOutput string) {
	t.Helper()

	for _, forbidden := range []string{
		filepath.Join(npmPrefix, "node.cmd"),
		filepath.Join(npmPrefix, "node.exe"),
		filepath.Join(npmPrefix, "bin", "node.cmd"),
		filepath.Join(npmPrefix, "bin", "node.exe"),
	} {
		if executableExists(forbidden) {
			t.Fatalf("managed Codex install must not publish generic node command %q; installer output:\n%s", forbidden, installerOutput)
		}
	}

	codexCmd := filepath.Join(npmPrefix, "codex.cmd")
	if !executableExists(codexCmd) {
		t.Fatalf("expected managed codex shim at %q; installer output:\n%s", codexCmd, installerOutput)
	}
	shim, err := os.ReadFile(codexCmd)
	if err != nil {
		t.Fatalf("read codex.cmd: %v", err)
	}
	shimText := string(shim)
	if !strings.Contains(shimText, "CODEX_NODE_INSTALL_ROOT") {
		t.Fatalf("expected codex.cmd to resolve private managed Node, got:\n%s", shimText)
	}
	if strings.Contains(strings.ToLower(shimText), "node.cmd") {
		t.Fatalf("codex.cmd must not depend on a public node.cmd shim, got:\n%s", shimText)
	}

	nativeBin, _, err := cloudgate.FindNativeBinary(codexCmd)
	if err != nil {
		t.Fatalf("managed codex shim must remain compatible with native binary discovery: %v\nshim:\n%s", err, shimText)
	}
	if !executableExists(nativeBin) {
		t.Fatalf("native codex binary discovered from managed shim does not exist: %s", nativeBin)
	}
}
