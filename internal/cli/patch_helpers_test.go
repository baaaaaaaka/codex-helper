package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestIsYoloFailure(t *testing.T) {
	cases := []struct {
		output string
		want   bool
	}{
		{"", false},
		{"unknown flag: --yolo", true},
		{"yolo unknown", true},
		{"yolo not supported", true},
		{"yolo invalid", true},
		{"yolo flag provided but not defined", true},
		{"unrelated error", false},
	}
	for _, tc := range cases {
		got := isYoloFailure(os.ErrInvalid, tc.output)
		if got != tc.want {
			t.Fatalf("output %q: expected %v, got %v", tc.output, tc.want, got)
		}
	}
	if isYoloFailure(nil, "yolo unknown") {
		t.Fatalf("expected nil error to return false")
	}
}

func TestIsYoloFailure_ApprovalPolicyRejection(t *testing.T) {
	if isYoloFailure(os.ErrInvalid, "approval_policy value not in the allowed set") != true {
		t.Fatal("expected true for approval_policy rejection")
	}
}

func TestIsYoloFailure_AskForApproval(t *testing.T) {
	if isYoloFailure(os.ErrInvalid, "ask-for-approval unknown flag") != true {
		t.Fatal("expected true for ask-for-approval unknown")
	}
	if isYoloFailure(os.ErrInvalid, "ask-for-approval unrecognized") != true {
		t.Fatal("expected true for ask-for-approval unrecognized")
	}
}

func TestStripYoloArgs(t *testing.T) {
	in := []string{"codex", "--yolo", "resume", "abc"}
	out := stripYoloArgs(in)
	want := []string{"codex", "resume", "abc"}
	if len(out) != len(want) {
		t.Fatalf("expected %v, got %v", want, out)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, out)
		}
	}
}

func TestStripYoloArgsEmpty(t *testing.T) {
	out := stripYoloArgs(nil)
	if out != nil {
		t.Fatalf("expected nil, got %v", out)
	}
}

func TestStripYoloArgsNoMatch(t *testing.T) {
	in := []string{"codex", "resume", "abc"}
	out := stripYoloArgs(in)
	if len(out) != len(in) {
		t.Fatalf("expected %v, got %v", in, out)
	}
}

func TestStripYoloArgs_AskForApproval(t *testing.T) {
	in := []string{"codex", "--ask-for-approval", "never", "--sandbox", "danger-full-access", "resume", "abc"}
	out := stripYoloArgs(in)
	want := []string{"codex", "resume", "abc"}
	if len(out) != len(want) {
		t.Fatalf("expected %v, got %v", want, out)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, out)
		}
	}
}

func TestStripYoloArgs_DangerouslyBypass(t *testing.T) {
	in := []string{"codex", "--dangerously-bypass-approvals-and-sandbox", "resume"}
	out := stripYoloArgs(in)
	want := []string{"codex", "resume"}
	if len(out) != len(want) {
		t.Fatalf("expected %v, got %v", want, out)
	}
}

func TestStripYoloArgs_AskForApprovalAtEnd(t *testing.T) {
	// --ask-for-approval without a value at end of args.
	in := []string{"codex", "--ask-for-approval"}
	out := stripYoloArgs(in)
	want := []string{"codex"}
	if len(out) != len(want) {
		t.Fatalf("expected %v, got %v", want, out)
	}
}

func TestExtractVersion(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"Codex CLI 1.2.3", "1.2.3"},
		{"v2.0", "2.0"},
		{"version", "version"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := extractVersion(tc.input); got != tc.want {
			t.Fatalf("extractVersion(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestResolveCodexVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	writeStub(t, dir, "codex", "#!/bin/sh\necho \"Codex CLI 1.2.3\"\n", "")
	path := dir + "/codex"
	if got := resolveCodexVersion(path); got != "1.2.3" {
		t.Fatalf("expected version 1.2.3, got %q", got)
	}
}

func TestIsCodexExecutable(t *testing.T) {
	if !isCodexExecutable("codex", "/usr/local/bin/codex") {
		t.Fatalf("expected resolved path to identify codex binary")
	}
	if !isCodexExecutable("codex.exe", "C:\\Users\\user\\codex.exe") {
		t.Fatalf("expected cmd arg to identify codex.exe binary")
	}
	if isCodexExecutable("not-codex", "/tmp/other") {
		t.Fatalf("expected non-codex to be rejected")
	}
}

// --- isPatchedBinaryFailure tests ---

func TestIsPatchedBinaryFailure(t *testing.T) {
	cases := []struct {
		output string
		want   bool
	}{
		{"", false},
		{"normal output", false},
		{"not a valid executable", true},
		{"exec format error", true},
		{"cannot execute binary file", true},
		{"bad cpu type in executable", true},
		{"Killed", true},
		{"Segmentation fault (core dumped)", true},
		{"Bus error", true},
		{"Illegal instruction", true},
		{"Abort trap: 6", true},
	}
	for _, tc := range cases {
		got := isPatchedBinaryFailure(os.ErrInvalid, tc.output)
		if got != tc.want {
			t.Errorf("isPatchedBinaryFailure(err, %q) = %v, want %v", tc.output, got, tc.want)
		}
	}
	// nil error always returns false.
	if isPatchedBinaryFailure(nil, "segmentation fault") {
		t.Error("expected false for nil error")
	}
}

func TestIsPatchedBinaryFailure_CaseInsensitive(t *testing.T) {
	// Patterns should match case-insensitively.
	cases := []string{
		"SEGMENTATION FAULT",
		"Bus Error",
		"EXEC FORMAT ERROR",
		"NOT A VALID EXECUTABLE",
		"ABORT",
		"ILLEGAL INSTRUCTION",
	}
	for _, output := range cases {
		if !isPatchedBinaryFailure(os.ErrInvalid, output) {
			t.Errorf("expected true for %q", output)
		}
	}
}

func TestIsPatchedBinaryFailure_Substrings(t *testing.T) {
	// Patterns should match as substrings within longer output.
	cases := []string{
		"Error: not a valid executable (binary corrupted)",
		"./codex-patched: exec format error\n",
		"zsh: abort      ./codex-patched",
		"/tmp/codex-patched: cannot execute binary file: Exec format error",
	}
	for _, output := range cases {
		if !isPatchedBinaryFailure(os.ErrInvalid, output) {
			t.Errorf("expected true for %q", output)
		}
	}
}

func TestIsPatchedBinaryFailure_NoFalsePositives(t *testing.T) {
	benign := []string{
		"Usage: codex [options]",
		"Error: missing required argument",
		"Connection refused",
		"timeout waiting for server",
		"file not found: config.json",
	}
	for _, output := range benign {
		if isPatchedBinaryFailure(os.ErrInvalid, output) {
			t.Errorf("unexpected true for %q", output)
		}
	}
}

// --- isPatchedBinaryStartupFailure tests ---

func TestIsPatchedBinaryStartupFailure(t *testing.T) {
	// nil error.
	if isPatchedBinaryStartupFailure(nil, "") {
		t.Error("expected false for nil error")
	}
	// PathError.
	pathErr := &os.PathError{Op: "exec", Path: "/tmp/codex", Err: os.ErrNotExist}
	if !isPatchedBinaryStartupFailure(pathErr, "") {
		t.Error("expected true for PathError")
	}
	// exec.Error.
	execErr := &exec.Error{Name: "codex", Err: os.ErrNotExist}
	if !isPatchedBinaryStartupFailure(execErr, "") {
		t.Error("expected true for exec.Error")
	}
	// Output-based detection.
	if !isPatchedBinaryStartupFailure(os.ErrInvalid, "segmentation fault") {
		t.Error("expected true for segfault output")
	}
	// Normal exit error with no matching output.
	if isPatchedBinaryStartupFailure(fmt.Errorf("exit status 1"), "usage info") {
		t.Error("expected false for normal exit error")
	}
}

func TestIsPatchedBinaryStartupFailure_WrappedPathError(t *testing.T) {
	inner := &os.PathError{Op: "exec", Path: "/tmp/codex", Err: os.ErrNotExist}
	wrapped := fmt.Errorf("start failed: %w", inner)
	if !isPatchedBinaryStartupFailure(wrapped, "") {
		t.Error("expected true for wrapped PathError")
	}
}

func TestIsPatchedBinaryStartupFailure_WrappedExecError(t *testing.T) {
	inner := &exec.Error{Name: "codex", Err: os.ErrNotExist}
	wrapped := fmt.Errorf("start failed: %w", inner)
	if !isPatchedBinaryStartupFailure(wrapped, "") {
		t.Error("expected true for wrapped exec.Error")
	}
}

func TestIsPatchedBinaryStartupFailure_PermissionDenied(t *testing.T) {
	pathErr := &os.PathError{Op: "exec", Path: "/tmp/codex", Err: os.ErrPermission}
	if !isPatchedBinaryStartupFailure(pathErr, "") {
		t.Error("expected true for permission denied PathError")
	}
}

func TestIsPatchedBinaryStartupFailure_OnlyOutputMatch(t *testing.T) {
	// Error is generic but output indicates a crash.
	if !isPatchedBinaryStartupFailure(fmt.Errorf("exit status 134"), "Abort trap: 6") {
		t.Error("expected true for abort output with generic error")
	}
}

func TestIsPatchedBinaryStartupFailure_NilOutputEmpty(t *testing.T) {
	// Generic error with empty output — should be false.
	if isPatchedBinaryStartupFailure(fmt.Errorf("exit status 1"), "") {
		t.Error("expected false for generic error with empty output")
	}
}

func TestIsPatchedBinaryStartupFailure_FatalSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals not applicable on Windows")
	}
	cmd := exec.Command("sh", "-c", "kill -SEGV $$")
	err := cmd.Run()
	if err == nil {
		t.Skip("command unexpectedly succeeded")
	}
	if !isPatchedBinaryStartupFailure(err, "") {
		t.Error("expected true for SIGSEGV via exitDueToFatalSignal")
	}
}

// --- hashFileSHA256 tests ---

func TestHashFileSHA256(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")
	content := []byte("hello world")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := hashFileSHA256(path)
	if err != nil {
		t.Fatalf("hashFileSHA256: %v", err)
	}

	sum := sha256.Sum256(content)
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
}

func TestHashFileSHA256_NotFound(t *testing.T) {
	_, err := hashFileSHA256("/nonexistent/file")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestHashFileSHA256_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty")
	if err := os.WriteFile(path, []byte{}, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := hashFileSHA256(path)
	if err != nil {
		t.Fatalf("hashFileSHA256: %v", err)
	}

	// SHA-256 of empty data.
	sum := sha256.Sum256(nil)
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
}

func TestHashFileSHA256_LargeFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.bin")
	data := make([]byte, 1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := hashFileSHA256(path)
	if err != nil {
		t.Fatalf("hashFileSHA256: %v", err)
	}

	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("hash mismatch for large file")
	}
}

func TestHashFileSHA256_DifferentContent(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "a.bin")
	path2 := filepath.Join(dir, "b.bin")
	if err := os.WriteFile(path1, []byte("aaa"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(path2, []byte("bbb"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	h1, err := hashFileSHA256(path1)
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	h2, err := hashFileSHA256(path2)
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	if h1 == h2 {
		t.Fatal("different content should produce different hashes")
	}
}

func TestHashFileSHA256_SameContent(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "a.bin")
	path2 := filepath.Join(dir, "b.bin")
	content := []byte("same content")
	if err := os.WriteFile(path1, content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(path2, content, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	h1, _ := hashFileSHA256(path1)
	h2, _ := hashFileSHA256(path2)
	if h1 != h2 {
		t.Fatal("same content should produce same hashes")
	}
}

func TestHashFileSHA256_HexFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := hashFileSHA256(path)
	if err != nil {
		t.Fatalf("hashFileSHA256: %v", err)
	}
	// SHA-256 hex digest is always 64 characters.
	if len(got) != 64 {
		t.Fatalf("hash length = %d, want 64", len(got))
	}
	// Should only contain hex characters.
	for _, c := range got {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Fatalf("non-hex character in hash: %c", c)
		}
	}
}

// --- formatFailureReason tests ---

func TestFormatFailureReason(t *testing.T) {
	// Short reason.
	reason := formatFailureReason(fmt.Errorf("exit status 1"), "bad output")
	if reason != "exit status 1: bad output" {
		t.Fatalf("unexpected reason: %q", reason)
	}

	// Nil error.
	reason = formatFailureReason(nil, "output only")
	if reason != "output only" {
		t.Fatalf("unexpected reason: %q", reason)
	}

	// Empty output.
	reason = formatFailureReason(fmt.Errorf("err"), "")
	if reason != "err" {
		t.Fatalf("unexpected reason: %q", reason)
	}

	// Truncation.
	long := ""
	for i := 0; i < 300; i++ {
		long += "x"
	}
	reason = formatFailureReason(fmt.Errorf("%s", long), "")
	if len(reason) > maxFailureReasonLen {
		t.Fatalf("reason too long: %d > %d", len(reason), maxFailureReasonLen)
	}
}

func TestFormatFailureReason_BothEmpty(t *testing.T) {
	reason := formatFailureReason(nil, "")
	if reason != "" {
		t.Fatalf("expected empty reason, got %q", reason)
	}
}

func TestFormatFailureReason_WhitespaceOnlyOutput(t *testing.T) {
	reason := formatFailureReason(nil, "   \n\t  ")
	if reason != "" {
		t.Fatalf("expected empty reason for whitespace-only output, got %q", reason)
	}
}

func TestFormatFailureReason_TruncationWithBoth(t *testing.T) {
	longErr := strings.Repeat("e", 200)
	longOut := strings.Repeat("o", 200)
	reason := formatFailureReason(fmt.Errorf("%s", longErr), longOut)
	if len(reason) > maxFailureReasonLen {
		t.Fatalf("reason too long: %d > %d", len(reason), maxFailureReasonLen)
	}
	if !strings.HasSuffix(reason, "...") {
		t.Fatalf("truncated reason should end with ..., got %q", reason[len(reason)-10:])
	}
}

func TestFormatFailureReason_ExactlyMaxLen(t *testing.T) {
	exact := strings.Repeat("x", maxFailureReasonLen)
	reason := formatFailureReason(fmt.Errorf("%s", exact), "")
	if len(reason) > maxFailureReasonLen {
		t.Fatalf("reason too long: %d > %d", len(reason), maxFailureReasonLen)
	}
}

func TestFormatFailureReason_JustUnderMaxLen(t *testing.T) {
	under := strings.Repeat("x", maxFailureReasonLen-1)
	reason := formatFailureReason(fmt.Errorf("%s", under), "")
	if reason != under {
		t.Fatalf("expected no truncation")
	}
}

func TestFormatFailureReason_MultilineOutput(t *testing.T) {
	reason := formatFailureReason(fmt.Errorf("exit status 134"), "line1\nline2\nline3")
	if !strings.Contains(reason, "exit status 134") {
		t.Fatal("missing error in reason")
	}
	if !strings.Contains(reason, "line1") {
		t.Fatal("missing output in reason")
	}
}

// --- currentProxyVersion tests ---

func TestCurrentProxyVersion(t *testing.T) {
	origVersion := version
	t.Cleanup(func() { version = origVersion })

	version = ""
	if got := currentProxyVersion(); got != "dev" {
		t.Fatalf("expected %q, got %q", "dev", got)
	}

	version = "v1.0.0"
	if got := currentProxyVersion(); got != "v1.0.0" {
		t.Fatalf("expected %q, got %q", "v1.0.0", got)
	}
}

func TestCurrentProxyVersion_WhitespaceOnly(t *testing.T) {
	origVersion := version
	t.Cleanup(func() { version = origVersion })

	version = "   "
	if got := currentProxyVersion(); got != "dev" {
		t.Fatalf("expected %q for whitespace version, got %q", "dev", got)
	}
}

// --- recordPatchFailure tests ---

func TestRecordPatchFailure_NilInfo(t *testing.T) {
	// Should not panic.
	recordPatchFailure(nil, fmt.Errorf("err"), "output")
}

func TestRecordPatchFailure_EmptyConfigDir(t *testing.T) {
	info := &patchRunInfo{
		OrigBinaryPath: "/bin/codex",
		OrigSHA256:     "aaa",
		ConfigDir:      "",
	}
	// Should not panic, just return.
	recordPatchFailure(info, fmt.Errorf("err"), "output")
}

func TestRecordPatchFailure_WritesToStore(t *testing.T) {
	dir := t.TempDir()
	info := &patchRunInfo{
		OrigBinaryPath: "/bin/codex",
		OrigSHA256:     "abc123",
		ConfigDir:      dir,
	}

	recordPatchFailure(info, fmt.Errorf("signal: segmentation fault"), "Segmentation fault (core dumped)")

	// Verify the failure was recorded.
	phs, err := config.NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	failed, err := phs.IsFailed("/bin/codex", "abc123")
	if err != nil {
		t.Fatalf("IsFailed: %v", err)
	}
	if !failed {
		t.Fatal("expected failure to be recorded")
	}

	// Verify failure reason.
	entry, err := phs.Find("/bin/codex", "abc123")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if entry == nil {
		t.Fatal("expected entry")
	}
	if !entry.Failed {
		t.Fatal("expected Failed=true")
	}
	if entry.FailureReason == "" {
		t.Fatal("expected non-empty failure reason")
	}
	if !strings.Contains(entry.FailureReason, "segmentation fault") {
		t.Fatalf("expected 'segmentation fault' in reason, got %q", entry.FailureReason)
	}
}

func TestRecordPatchFailure_OverwritesPreviousSuccess(t *testing.T) {
	dir := t.TempDir()

	// First, record a success.
	phs, err := config.NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	_ = phs.Upsert(config.PatchHistoryEntry{
		Path:          "/bin/codex",
		OrigSHA256:    "abc123",
		PatchedSHA256: "def456",
		Failed:        false,
	})

	// Now record a failure.
	info := &patchRunInfo{
		OrigBinaryPath: "/bin/codex",
		OrigSHA256:     "abc123",
		ConfigDir:      dir,
	}
	recordPatchFailure(info, fmt.Errorf("crash"), "boom")

	// Should now be failed.
	phs2, _ := config.NewPatchHistoryStore(dir)
	failed, _ := phs2.IsFailed("/bin/codex", "abc123")
	if !failed {
		t.Fatal("expected failure to overwrite success")
	}
}

func TestRecordPatchFailure_SetsProxyVersion(t *testing.T) {
	dir := t.TempDir()
	info := &patchRunInfo{
		OrigBinaryPath: "/bin/codex",
		OrigSHA256:     "xxx",
		ConfigDir:      dir,
	}

	origVersion := version
	t.Cleanup(func() { version = origVersion })
	version = "v0.0.5-test"

	recordPatchFailure(info, fmt.Errorf("err"), "")

	phs, _ := config.NewPatchHistoryStore(dir)
	entry, _ := phs.Find("/bin/codex", "xxx")
	if entry == nil {
		t.Fatal("expected entry")
	}
	if entry.ProxyVersion != "v0.0.5-test" {
		t.Fatalf("ProxyVersion = %q, want v0.0.5-test", entry.ProxyVersion)
	}
}
