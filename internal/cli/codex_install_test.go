package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

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
	path := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(path, []byte("x"), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}
	got, err := ensureCodexInstalled(context.Background(), path, io.Discard)
	if err != nil {
		t.Fatalf("ensureCodexInstalled error: %v", err)
	}
	if got != path {
		t.Fatalf("expected path %q, got %q", path, got)
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
	if err := runCodexInstaller(context.Background(), io.Discard); err != nil {
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
	err := runCodexInstaller(context.Background(), io.Discard)
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
	got, err := findInstalledCodex()
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
	got, err := findInstalledCodex()
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
	_, err := findInstalledCodex()
	if err == nil {
		t.Fatal("expected error when codex is unavailable")
	}
	if !strings.Contains(err.Error(), "codex binary not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}
