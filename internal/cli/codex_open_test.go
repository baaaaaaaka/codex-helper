package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestBuildCodexResumeCommandUsesSessionPath(t *testing.T) {
	dir := t.TempDir()
	session := codexhistory.Session{SessionID: "abc", ProjectPath: dir}
	project := codexhistory.Project{Path: "/tmp/other"}

	path, args, cwd, err := buildCodexResumeCommand("/bin/codex", session, project, false)
	if err != nil {
		t.Fatalf("buildCodexResumeCommand error: %v", err)
	}
	if path != "/bin/codex" {
		t.Fatalf("expected path /bin/codex, got %s", path)
	}
	if len(args) != 2 || args[0] != "resume" || args[1] != "abc" {
		t.Fatalf("unexpected args: %#v", args)
	}
	if cwd != dir {
		t.Fatalf("expected cwd %s, got %s", dir, cwd)
	}
}

func TestBuildCodexResumeCommandUsesProjectPath(t *testing.T) {
	dir := t.TempDir()
	session := codexhistory.Session{SessionID: "abc"}
	project := codexhistory.Project{Path: dir}

	_, _, cwd, err := buildCodexResumeCommand("/bin/codex", session, project, false)
	if err != nil {
		t.Fatalf("buildCodexResumeCommand error: %v", err)
	}
	if cwd != dir {
		t.Fatalf("expected cwd %s, got %s", dir, cwd)
	}
}

func TestBuildCodexResumeCommandAddsYoloArgs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	session := codexhistory.Session{SessionID: "abc"}
	project := codexhistory.Project{Path: dir}

	// Create a fake codex that responds to --help with --ask-for-approval.
	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\necho 'usage codex --ask-for-approval <POLICY>'\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	_, args, _, err := buildCodexResumeCommand(scriptPath, session, project, true)
	if err != nil {
		t.Fatalf("buildCodexResumeCommand error: %v", err)
	}
	want := []string{"--ask-for-approval", "never", "resume", "abc"}
	if len(args) != len(want) {
		t.Fatalf("expected args %v, got %v", want, args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("expected args %v, got %v", want, args)
		}
	}
}

func TestBuildCodexResumeCommandRejectsMissingSession(t *testing.T) {
	dir := t.TempDir()
	session := codexhistory.Session{}
	project := codexhistory.Project{Path: dir}

	_, _, _, err := buildCodexResumeCommand("/bin/codex", session, project, false)
	if err == nil {
		t.Fatalf("expected error for missing session id")
	}
}

func TestBuildCodexResumeCommandRejectsMissingCwd(t *testing.T) {
	session := codexhistory.Session{SessionID: "abc", ProjectPath: filepath.Join(t.TempDir(), "missing")}
	project := codexhistory.Project{}

	_, _, _, err := buildCodexResumeCommand("/bin/codex", session, project, false)
	if err == nil {
		t.Fatalf("expected error for missing cwd")
	}
}

func TestNormalizeWorkingDirResolvesRelative(t *testing.T) {
	dir := t.TempDir()
	rel := filepath.Base(dir)
	abs := filepath.Dir(dir)
	old, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer func() { _ = os.Chdir(old) }()
	if err := os.Chdir(abs); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	got, err := normalizeWorkingDir(rel)
	if err != nil {
		t.Fatalf("normalizeWorkingDir error: %v", err)
	}
	if canonicalPath(t, got) != canonicalPath(t, dir) {
		t.Fatalf("expected %s, got %s", dir, got)
	}
}

func TestNormalizeWorkingDirRejectsMissing(t *testing.T) {
	_, err := normalizeWorkingDir(filepath.Join(t.TempDir(), "missing"))
	if err == nil {
		t.Fatalf("expected error for missing cwd")
	}
}

func TestNormalizeWorkingDirRejectsFile(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := normalizeWorkingDir(file); err == nil {
		t.Fatalf("expected error for non-directory cwd")
	}
}

func TestRunCodexSessionSuccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script execution on windows")
	}
	codexPath := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}
	store := newTempStore(t)
	root := &rootOptions{configPath: store.Path()}

	projectDir := t.TempDir()
	session := codexhistory.Session{SessionID: "sess-1", ProjectPath: projectDir}
	project := codexhistory.Project{Path: projectDir}

	if err := runCodexSession(context.Background(), root, store, nil, nil, session, project, codexPath, "", false, false, io.Discard); err != nil {
		t.Fatalf("runCodexSession error: %v", err)
	}
}

func TestRunCodexNewSessionSuccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script execution on windows")
	}
	codexPath := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}
	store := newTempStore(t)
	root := &rootOptions{configPath: store.Path()}

	projectDir := t.TempDir()
	if err := runCodexNewSession(context.Background(), root, store, nil, nil, projectDir, codexPath, "", false, false, io.Discard); err != nil {
		t.Fatalf("runCodexNewSession error: %v", err)
	}
}

func TestRunCodexSessionRequiresProfileWhenProxyEnabled(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script execution on windows")
	}
	codexPath := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}
	store := newTempStore(t)
	root := &rootOptions{configPath: store.Path()}

	projectDir := t.TempDir()
	session := codexhistory.Session{SessionID: "sess-1", ProjectPath: projectDir}
	project := codexhistory.Project{Path: projectDir}

	if err := runCodexSession(context.Background(), root, store, nil, nil, session, project, codexPath, "", true, false, io.Discard); err == nil {
		t.Fatalf("expected proxy mode error")
	}
}

func TestRunCodexNewSessionRequiresProfileWhenProxyEnabled(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script execution on windows")
	}
	codexPath := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(codexPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write codex: %v", err)
	}
	store := newTempStore(t)
	root := &rootOptions{configPath: store.Path()}

	projectDir := t.TempDir()
	if err := runCodexNewSession(context.Background(), root, store, nil, nil, projectDir, codexPath, "", true, false, io.Discard); err == nil {
		t.Fatalf("expected proxy mode error")
	}
}

func TestRunCodexNewSessionUsesCwdDirect(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	outFile := filepath.Join(t.TempDir(), "pwd.txt")
	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := fmt.Sprintf("#!/bin/sh\npwd > %q\n", outFile)
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = runCodexNewSession(
		context.Background(),
		&rootOptions{},
		store,
		nil,
		nil,
		dir,
		scriptPath,
		"",
		false,
		false,
		io.Discard,
	)
	if err != nil {
		t.Fatalf("runCodexNewSession error: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if strings.TrimSpace(string(got)) != dir {
		if canonicalPath(t, strings.TrimSpace(string(got))) != canonicalPath(t, dir) {
			t.Fatalf("expected cwd %s, got %q", dir, strings.TrimSpace(string(got)))
		}
	}
}

func TestRunCodexNewSessionAddsYoloArgs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	outFile := filepath.Join(t.TempDir(), "args.txt")
	scriptPath := filepath.Join(t.TempDir(), "codex")
	// The script must respond to --help with a string containing --ask-for-approval
	// so that codexYoloArgs detects yolo support. For any other invocation it
	// records the arguments.
	script := fmt.Sprintf("#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --ask-for-approval <POLICY>' ;; *) printf '%%s\\n' \"$@\" > %q ;; esac\n", outFile)
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = runCodexNewSession(
		context.Background(),
		&rootOptions{},
		store,
		nil,
		nil,
		dir,
		scriptPath,
		"",
		false,
		true,
		io.Discard,
	)
	if err != nil {
		t.Fatalf("runCodexNewSession error: %v", err)
	}
	got, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	// Should have --ask-for-approval never as the first two args.
	if len(lines) < 2 || lines[0] != "--ask-for-approval" || lines[1] != "never" {
		t.Fatalf("expected yolo args [--ask-for-approval never ...], got %v", lines)
	}
}

func TestRunCodexNewSessionRejectsProxyWithoutProfile(t *testing.T) {
	dir := t.TempDir()
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = runCodexNewSession(
		context.Background(),
		&rootOptions{},
		store,
		nil,
		nil,
		dir,
		"/bin/codex",
		"",
		true,
		false,
		io.Discard,
	)
	if err == nil {
		t.Fatalf("expected error when proxy enabled without profile")
	}
}
