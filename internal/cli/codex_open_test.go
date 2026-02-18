package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// testTargetTriple mirrors cloudgate.targetTriple() for test setup.
func testTargetTriple() string {
	switch runtime.GOOS {
	case "linux":
		switch runtime.GOARCH {
		case "amd64":
			return "x86_64-unknown-linux-musl"
		case "arm64":
			return "aarch64-unknown-linux-musl"
		}
	case "darwin":
		switch runtime.GOARCH {
		case "amd64":
			return "x86_64-apple-darwin"
		case "arm64":
			return "aarch64-apple-darwin"
		}
	case "windows":
		switch runtime.GOARCH {
		case "amd64":
			return "x86_64-pc-windows-msvc"
		case "arm64":
			return "aarch64-pc-windows-msvc"
		}
	}
	return ""
}

func testNativeBinaryName() string {
	if runtime.GOOS == "windows" {
		return "codex.exe"
	}
	return "codex"
}

// buildTestBinary creates a synthetic binary containing the given marker strings.
func buildTestBinary(markers ...string) []byte {
	var buf bytes.Buffer
	buf.WriteString("HEADER_PADDING_000")
	for _, m := range markers {
		buf.WriteString(m)
		buf.WriteString("\x00PADDING\x00")
	}
	buf.WriteString("TRAILER_PADDING")
	return buf.Bytes()
}

// setupMockCodexInstall creates a mock Codex installation directory structure
// with a wrapper and a native binary containing patchable byte sequences.
// Returns (wrapperPath, nativePath).
func setupMockCodexInstall(t *testing.T) (string, string) {
	t.Helper()
	triple := testTargetTriple()
	if triple == "" {
		t.Skip("unsupported platform for this test")
	}

	dir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}

	// <dir>/bin/codex.js  (wrapper)
	// <dir>/vendor/<triple>/codex/codex[.exe]  (native binary)
	binDir := filepath.Join(dir, "bin")
	nativeDir := filepath.Join(dir, "vendor", triple, "codex")
	for _, d := range []string{binDir, nativeDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

	wrapperPath := filepath.Join(binDir, "codex.js")
	if err := os.WriteFile(wrapperPath, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}

	nativePath := filepath.Join(nativeDir, testNativeBinaryName())
	data := buildTestBinary(
		"/api/codex/config/requirements",
		"/wham/config/requirements",
	)
	if err := os.WriteFile(nativePath, data, 0o755); err != nil {
		t.Fatalf("write native: %v", err)
	}

	return wrapperPath, nativePath
}

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

	// Create a fake codex that responds to --help with --ask-for-approval and --sandbox.
	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\necho 'usage codex --ask-for-approval <POLICY> --sandbox <MODE>'\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	_, args, _, err := buildCodexResumeCommand(scriptPath, session, project, true)
	if err != nil {
		t.Fatalf("buildCodexResumeCommand error: %v", err)
	}
	want := []string{"--ask-for-approval", "never", "--sandbox", "danger-full-access", "resume", "abc"}
	if len(args) != len(want) {
		t.Fatalf("expected args %v, got %v", want, args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("expected args %v, got %v", want, args)
		}
	}
}

func TestBuildCodexResumeCommandPrefersDangerouslyBypass(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	session := codexhistory.Session{SessionID: "abc"}
	project := codexhistory.Project{Path: dir}

	// Simulate Codex ≥0.104 where --help includes both --ask-for-approval
	// and --dangerously-bypass-approvals-and-sandbox.
	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\necho 'usage codex --ask-for-approval <POLICY> --sandbox <MODE> --dangerously-bypass-approvals-and-sandbox'\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	_, args, _, err := buildCodexResumeCommand(scriptPath, session, project, true)
	if err != nil {
		t.Fatalf("buildCodexResumeCommand error: %v", err)
	}
	want := []string{"--dangerously-bypass-approvals-and-sandbox", "resume", "abc"}
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
	// and --sandbox so that codexYoloArgs detects yolo support. For any other
	// invocation it records the arguments.
	script := fmt.Sprintf("#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --ask-for-approval <POLICY> --sandbox <MODE>' ;; *) printf '%%s\\n' \"$@\" > %q ;; esac\n", outFile)
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
	// Should have --ask-for-approval never --sandbox danger-full-access as the first args.
	if len(lines) < 4 || lines[0] != "--ask-for-approval" || lines[1] != "never" ||
		lines[2] != "--sandbox" || lines[3] != "danger-full-access" {
		t.Fatalf("expected yolo args [--ask-for-approval never --sandbox danger-full-access ...], got %v", lines)
	}
}

func TestRunCodexNewSessionPrefersDangerouslyBypass(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	outFile := filepath.Join(t.TempDir(), "args.txt")
	scriptPath := filepath.Join(t.TempDir(), "codex")
	// Simulate Codex ≥0.104 where --help includes both --ask-for-approval
	// and --dangerously-bypass-approvals-and-sandbox.
	script := fmt.Sprintf("#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --ask-for-approval <POLICY> --sandbox <MODE> --dangerously-bypass-approvals-and-sandbox' ;; *) printf '%%s\\n' \"$@\" > %q ;; esac\n", outFile)
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
	// Should prefer --dangerously-bypass-approvals-and-sandbox over individual flags.
	if len(lines) < 1 || lines[0] != "--dangerously-bypass-approvals-and-sandbox" {
		t.Fatalf("expected yolo args [--dangerously-bypass-approvals-and-sandbox ...], got %v", lines)
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

// TestPreparePatchedBinaryRePatchesAfterCleanup verifies that
// preparePatchedBinary always re-patches even when patch history records a
// previous successful patch. This is critical because Cleanup() deletes the
// patched binary after each session — if we skip on "already patched", the
// next session would run the unpatched original binary.
func TestPreparePatchedBinaryRePatchesAfterCleanup(t *testing.T) {
	wrapperPath, _ := setupMockCodexInstall(t)
	configDir := filepath.Join(t.TempDir(), "config")
	defer os.RemoveAll("/tmp/cxreq") // clean up permissive requirements

	// --- First call: should produce a patched binary ---
	result1, _, info1, skipped1 := preparePatchedBinary(wrapperPath, configDir)
	if skipped1 {
		t.Fatal("first call should not be skipped")
	}
	if result1 == nil || result1.PatchedBinary == "" {
		t.Fatal("first call should produce a patched binary")
	}
	if info1 == nil {
		t.Fatal("first call should produce patch run info")
	}

	// Verify patched binary exists on disk.
	if _, err := os.Stat(result1.PatchedBinary); err != nil {
		t.Fatalf("patched binary should exist: %v", err)
	}

	// Verify patch history recorded this as a successful patch.
	phs, err := config.NewPatchHistoryStore(configDir)
	if err != nil {
		t.Fatalf("open patch history: %v", err)
	}
	patched, err := phs.IsPatched(wrapperPath, info1.OrigSHA256)
	if err != nil {
		t.Fatalf("IsPatched: %v", err)
	}
	if !patched {
		t.Fatal("patch history should record successful patch after first call")
	}

	// Simulate Cleanup() (which runs via defer after each session ends).
	result1.Cleanup()

	// Verify patched binary is gone.
	if _, err := os.Stat(result1.PatchedBinary); err == nil {
		t.Fatal("patched binary should be removed after Cleanup")
	}

	// --- Second call: must re-patch despite history saying "already patched" ---
	result2, _, info2, skipped2 := preparePatchedBinary(wrapperPath, configDir)
	if skipped2 {
		t.Fatal("second call should NOT be skipped — this was the bug (IsPatched caused skip)")
	}
	if result2 == nil || result2.PatchedBinary == "" {
		t.Fatal("second call should produce a new patched binary")
	}
	defer result2.Cleanup()
	if info2 == nil {
		t.Fatal("second call should produce patch run info")
	}

	// Verify the new patched binary exists and is a real file.
	fi, err := os.Stat(result2.PatchedBinary)
	if err != nil {
		t.Fatalf("second patched binary should exist: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("second patched binary should not be empty")
	}
}

// TestPreparePatchedBinarySkipsOnFailed verifies that preparePatchedBinary
// correctly skips patching when a previous patch was recorded as failed.
func TestPreparePatchedBinarySkipsOnFailed(t *testing.T) {
	wrapperPath, _ := setupMockCodexInstall(t)
	configDir := filepath.Join(t.TempDir(), "config")
	defer os.RemoveAll("/tmp/cxreq")

	// Compute wrapper hash to pre-populate failure in history.
	origHash, err := hashFileSHA256(wrapperPath)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}

	// Record a failed patch in history.
	phs, err := config.NewPatchHistoryStore(configDir)
	if err != nil {
		t.Fatalf("open patch history: %v", err)
	}
	if err := phs.Upsert(config.PatchHistoryEntry{
		Path:          wrapperPath,
		OrigSHA256:    origHash,
		PatchedSHA256: "dummy",
		ProxyVersion:  "test",
		PatchedAt:     time.Now(),
		Failed:        true,
		FailureReason: "test: simulated crash",
	}); err != nil {
		t.Fatalf("upsert failed entry: %v", err)
	}

	// Call preparePatchedBinary — should be skipped due to failure history.
	result, _, _, skipped := preparePatchedBinary(wrapperPath, configDir)
	if !skipped {
		t.Fatal("should be skipped when history records a failed patch")
	}
	if result != nil {
		t.Fatal("skipped call should return nil result")
	}
}

// TestPreparePatchedBinaryRecordsHistory verifies that a successful patch
// is recorded in patch history with correct metadata.
func TestPreparePatchedBinaryRecordsHistory(t *testing.T) {
	wrapperPath, _ := setupMockCodexInstall(t)
	configDir := filepath.Join(t.TempDir(), "config")
	defer os.RemoveAll("/tmp/cxreq")

	result, _, info, skipped := preparePatchedBinary(wrapperPath, configDir)
	if skipped || result == nil || result.PatchedBinary == "" {
		t.Fatal("expected successful patch")
	}
	defer result.Cleanup()

	// Verify history entry was created.
	phs, err := config.NewPatchHistoryStore(configDir)
	if err != nil {
		t.Fatalf("open patch history: %v", err)
	}
	entry, err := phs.Find(wrapperPath, info.OrigSHA256)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if entry == nil {
		t.Fatal("expected history entry after successful patch")
	}
	if entry.Failed {
		t.Fatal("entry should not be marked as failed")
	}
	if entry.PatchedSHA256 == "" {
		t.Fatal("entry should have patched SHA256")
	}
	if entry.ProxyVersion == "" {
		t.Fatal("entry should have proxy version")
	}
}

// ---------------------------------------------------------------------------
// Cloud requirements cache deletion tests
// ---------------------------------------------------------------------------

// writeFakeCache creates a dummy cloud-requirements-cache.json in dir and
// returns its path.
func writeFakeCache(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "cloud-requirements-cache.json")
	if err := os.WriteFile(p, []byte(`{"signed_payload":{"contents":"allowed_approval_policies = [\"on-request\"]"}}`), 0o644); err != nil {
		t.Fatalf("write cache: %v", err)
	}
	return p
}

// cacheExists returns true if the cloud requirements cache file exists.
func cacheExists(t *testing.T, dir string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, "cloud-requirements-cache.json"))
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	t.Fatalf("stat cache: %v", err)
	return false
}

// TestRunCodexNewSessionDeletesCacheOnYolo verifies that the cloud
// requirements cache is always deleted when yolo mode is enabled, even when
// binary patching fails (e.g. no native binary found for the fake script).
func TestRunCodexNewSessionDeletesCacheOnYolo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()

	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; *) exit 0 ;; esac\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	codexDir := t.TempDir()
	writeFakeCache(t, codexDir)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = runCodexNewSession(
		context.Background(),
		&rootOptions{},
		store,
		nil, nil,
		dir,
		scriptPath,
		codexDir,
		false,
		true, // yolo
		io.Discard,
	)
	if err != nil {
		t.Fatalf("runCodexNewSession error: %v", err)
	}

	if cacheExists(t, codexDir) {
		t.Fatal("cloud requirements cache should be deleted when yolo is enabled")
	}
}

// TestRunCodexNewSessionPreservesCacheWithoutYolo verifies that the cache is
// NOT deleted when yolo mode is disabled.
func TestRunCodexNewSessionPreservesCacheWithoutYolo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()

	scriptPath := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	codexDir := t.TempDir()
	writeFakeCache(t, codexDir)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	err = runCodexNewSession(
		context.Background(),
		&rootOptions{},
		store,
		nil, nil,
		dir,
		scriptPath,
		codexDir,
		false,
		false, // yolo off
		io.Discard,
	)
	if err != nil {
		t.Fatalf("runCodexNewSession error: %v", err)
	}

	if !cacheExists(t, codexDir) {
		t.Fatal("cloud requirements cache should be preserved when yolo is disabled")
	}
}

// TestRunCodexNewSessionDeletesCacheWhenPatchSkipped verifies that the cache
// is deleted even when binary patching is skipped because a previous patch was
// recorded as failed. This is the core regression test for the fix that moved
// RemoveCloudRequirementsCache outside the patch-success block.
func TestRunCodexNewSessionDeletesCacheWhenPatchSkipped(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()

	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; *) exit 0 ;; esac\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	// Create a config store in a specific directory so we can populate
	// patch history in the same location.
	configDir := t.TempDir()
	store, err := config.NewStore(filepath.Join(configDir, "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	// Record a failed patch in history for this script's hash so that
	// preparePatchedBinary will skip patching entirely.
	origHash, err := hashFileSHA256(scriptPath)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	phs, err := config.NewPatchHistoryStore(configDir)
	if err != nil {
		t.Fatalf("patch history: %v", err)
	}
	if err := phs.Upsert(config.PatchHistoryEntry{
		Path:          scriptPath,
		OrigSHA256:    origHash,
		Failed:        true,
		FailureReason: "test: simulated crash",
		PatchedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	codexDir := t.TempDir()
	writeFakeCache(t, codexDir)

	err = runCodexNewSession(
		context.Background(),
		&rootOptions{},
		store,
		nil, nil,
		dir,
		scriptPath,
		codexDir,
		false,
		true, // yolo
		io.Discard,
	)
	if err != nil {
		t.Fatalf("runCodexNewSession error: %v", err)
	}

	if cacheExists(t, codexDir) {
		t.Fatal("cloud requirements cache should be deleted even when patching is skipped due to failure history")
	}
}

// TestRunCodexSessionDeletesCacheOnYolo mirrors the new-session test for the
// resume (runCodexSession) path.
func TestRunCodexSessionDeletesCacheOnYolo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; *) exit 0 ;; esac\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	codexDir := t.TempDir()
	writeFakeCache(t, codexDir)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	projectDir := t.TempDir()
	session := codexhistory.Session{SessionID: "sess-cache", ProjectPath: projectDir}
	project := codexhistory.Project{Path: projectDir}

	err = runCodexSession(
		context.Background(),
		&rootOptions{configPath: store.Path()},
		store,
		nil, nil,
		session,
		project,
		scriptPath,
		codexDir,
		false,
		true, // yolo
		io.Discard,
	)
	if err != nil {
		t.Fatalf("runCodexSession error: %v", err)
	}

	if cacheExists(t, codexDir) {
		t.Fatal("cloud requirements cache should be deleted when yolo is enabled (resume path)")
	}
}

// TestRunCodexSessionPreservesCacheWithoutYolo verifies cache is preserved
// when yolo is off on the resume path.
func TestRunCodexSessionPreservesCacheWithoutYolo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	scriptPath := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	codexDir := t.TempDir()
	writeFakeCache(t, codexDir)

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	projectDir := t.TempDir()
	session := codexhistory.Session{SessionID: "sess-nocache", ProjectPath: projectDir}
	project := codexhistory.Project{Path: projectDir}

	err = runCodexSession(
		context.Background(),
		&rootOptions{configPath: store.Path()},
		store,
		nil, nil,
		session,
		project,
		scriptPath,
		codexDir,
		false,
		false, // yolo off
		io.Discard,
	)
	if err != nil {
		t.Fatalf("runCodexSession error: %v", err)
	}

	if !cacheExists(t, codexDir) {
		t.Fatal("cloud requirements cache should be preserved when yolo is disabled (resume path)")
	}
}

// TestRunCodexSessionDeletesCacheWhenPatchSkipped mirrors the new-session
// test: cache is deleted even when patching is skipped from failure history.
func TestRunCodexSessionDeletesCacheWhenPatchSkipped(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\ncase \"$1\" in --help) echo 'usage codex --dangerously-bypass-approvals-and-sandbox' ;; *) exit 0 ;; esac\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	configDir := t.TempDir()
	store, err := config.NewStore(filepath.Join(configDir, "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	origHash, err := hashFileSHA256(scriptPath)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	phs, err := config.NewPatchHistoryStore(configDir)
	if err != nil {
		t.Fatalf("patch history: %v", err)
	}
	if err := phs.Upsert(config.PatchHistoryEntry{
		Path:          scriptPath,
		OrigSHA256:    origHash,
		Failed:        true,
		FailureReason: "test: simulated crash",
		PatchedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	codexDir := t.TempDir()
	writeFakeCache(t, codexDir)

	projectDir := t.TempDir()
	session := codexhistory.Session{SessionID: "sess-skip", ProjectPath: projectDir}
	project := codexhistory.Project{Path: projectDir}

	err = runCodexSession(
		context.Background(),
		&rootOptions{configPath: store.Path()},
		store,
		nil, nil,
		session,
		project,
		scriptPath,
		codexDir,
		false,
		true, // yolo
		io.Discard,
	)
	if err != nil {
		t.Fatalf("runCodexSession error: %v", err)
	}

	if cacheExists(t, codexDir) {
		t.Fatal("cloud requirements cache should be deleted even when patching is skipped (resume path)")
	}
}
