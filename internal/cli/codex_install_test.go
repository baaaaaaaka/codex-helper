package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
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

func TestResolveRunCommandKeepsCodexWhenOnPath(t *testing.T) {
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
	if len(got) != len(args) || got[0] != args[0] || got[1] != args[1] {
		t.Fatalf("expected unchanged args, got %v", got)
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

func TestWithCodexInstallLockFallsBackAfterTimeout(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

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
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

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

	prevPoll := codexInstallLockPollDelay
	prevWait := codexInstallLockMaxWait
	codexInstallLockPollDelay = 10 * time.Millisecond
	codexInstallLockMaxWait = 300 * time.Millisecond
	defer func() {
		codexInstallLockPollDelay = prevPoll
		codexInstallLockMaxWait = prevWait
	}()

	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = os.Remove(lockPath)
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

func TestBootstrapWindowsScriptContainsSelfValidation(t *testing.T) {
	if !strings.Contains(codexInstallBootstrapWindows, "$usedSystemNode") {
		t.Fatal("Windows bootstrap script missing usedSystemNode flag")
	}
	if !strings.Contains(codexInstallBootstrapWindows, "Install-LocalNode") {
		t.Fatal("Windows bootstrap script missing Install-LocalNode function")
	}
	if !strings.Contains(codexInstallBootstrapWindows, "--version") {
		t.Fatal("Windows bootstrap script missing post-install probe")
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
