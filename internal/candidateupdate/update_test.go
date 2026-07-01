package candidateupdate

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
)

func TestSupportsProtocolStartsAtRC36(t *testing.T) {
	for _, tc := range []struct {
		version string
		want    bool
	}{
		{version: "v0.1.13-rc.35"},
		{version: "0.1.13-rc.36", want: true},
		{version: "v0.1.13-rc.37", want: true},
		{version: "v0.1.13", want: true},
		{version: "invalid"},
	} {
		if got := SupportsProtocol(tc.version); got != tc.want {
			t.Fatalf("SupportsProtocol(%q) = %v, want %v", tc.version, got, tc.want)
		}
	}
}

func TestApplyRejectsCandidateHashMismatchBeforeWritingState(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	_, err := Apply(context.Background(), Context{
		Schema:          ProtocolVersion,
		CandidateSHA256: strings.Repeat("0", 64),
		TargetVersion:   "v9.8.7",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RequestID:       "wrong-hash",
	}, "v9.8.7")
	if err == nil || !strings.Contains(err.Error(), "candidate sha256") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Stat(root); !os.IsNotExist(statErr) {
		t.Fatalf("runtime state was created after hash rejection: %v", statErr)
	}
}

func TestHandleInternalCommandAppliesCurrentCandidate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX launcher fixture")
	}
	executable, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	hash, err := fileSHA256(executable)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	recordPath := filepath.Join(dir, "config", "install.json")
	writeDynamicLauncher(t, entry, root)
	request := Context{
		Schema:          ProtocolVersion,
		CandidatePath:   executable,
		CandidateSHA256: hash,
		TargetVersion:   "v9.8.7",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RecordPath:      recordPath,
		RequestID:       "handler-request",
	}
	contextPath := filepath.Join(dir, "context.json")
	data, _ := json.Marshal(request)
	if err := os.WriteFile(contextPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code, handled := HandleInternalCommand([]string{
		"candidate",
		InternalCommand,
		"--protocol=1",
		"--context-file=" + contextPath,
	}, "v9.8.7", &stdout, &stderr)
	if !handled || code != 0 {
		t.Fatalf("handled=%v code=%d stderr=%q", handled, code, stderr.String())
	}
	var result Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Version != "9.8.7" || result.RuntimePath == "" {
		t.Fatalf("result = %#v", result)
	}
}

func TestReconcileInstallIsIdempotentAndTracksPrevious(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX launcher fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	recordPath := filepath.Join(dir, "config", "install.json")
	writeDynamicLauncher(t, entry, root)

	v1 := filepath.Join(dir, "candidate-v1")
	writeExecutable(t, v1, "candidate-v1")
	v1Hash, err := fileSHA256(v1)
	if err != nil {
		t.Fatal(err)
	}
	req1 := Context{
		Schema:          ProtocolVersion,
		CandidatePath:   v1,
		CandidateSHA256: v1Hash,
		TargetVersion:   "v1.0.0",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RecordPath:      recordPath,
		RequestID:       "request-v1",
	}
	if err := helperruntime.WithRootLock(context.Background(), root, func() error {
		_, err := reconcileLocked(req1, v1, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if active, err := helperruntime.ReadActive(root); err != nil || active != "v1.0.0" {
		t.Fatalf("active = %q, %v", active, err)
	}

	v2 := filepath.Join(dir, "candidate-v2")
	writeExecutable(t, v2, "candidate-v2")
	v2Hash, err := fileSHA256(v2)
	if err != nil {
		t.Fatal(err)
	}
	req2 := Context{
		Schema:          ProtocolVersion,
		CandidatePath:   v2,
		CandidateSHA256: v2Hash,
		SourceVersion:   "1.0.0",
		TargetVersion:   "v2.0.0",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RecordPath:      recordPath,
		RequestID:       "request-v2",
	}
	apply := func(resuming bool, source string) error {
		return helperruntime.WithRootLock(context.Background(), root, func() error {
			_, err := reconcileLocked(req2, source, resuming)
			return err
		})
	}
	if err := apply(false, v2); err != nil {
		t.Fatal(err)
	}
	if err := apply(true, helperruntime.VersionPath(root, "v2.0.0", runtime.GOOS)); err != nil {
		t.Fatalf("idempotent reconcile: %v", err)
	}
	if active, err := helperruntime.ReadActive(root); err != nil || active != "v2.0.0" {
		t.Fatalf("active = %q, %v", active, err)
	}
	if previous, err := helperruntime.ReadPrevious(root); err != nil || previous != "v1.0.0" {
		t.Fatalf("previous = %q, %v", previous, err)
	}
	record, err := managedinstall.LoadRecord(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	if record.Version != "2.0.0" {
		t.Fatalf("record version = %q", record.Version)
	}
	if _, err := os.Stat(filepath.Join(root, PendingFileName)); err != nil {
		t.Fatalf("pending update missing before startup recovery: %v", err)
	}
}

func TestReconcileRejectsStaleSourceAfterConcurrentActivation(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	current := filepath.Join(dir, "current")
	candidate := filepath.Join(dir, "candidate")
	writeExecutable(t, current, "current")
	writeExecutable(t, candidate, "candidate")
	if _, err := helperruntime.InstallVersion(root, current, "v2.0.0", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v2.0.0"); err != nil {
		t.Fatal(err)
	}
	hash, err := fileSHA256(candidate)
	if err != nil {
		t.Fatal(err)
	}
	request := Context{
		Schema:          ProtocolVersion,
		CandidatePath:   candidate,
		CandidateSHA256: hash,
		SourceVersion:   "v1.0.0",
		TargetVersion:   "v3.0.0",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RequestID:       "stale-source",
	}
	err = helperruntime.WithRootLock(context.Background(), root, func() error {
		_, reconcileErr := reconcileLocked(request, candidate, false)
		return reconcileErr
	})
	if err == nil || !strings.Contains(err.Error(), "active runtime changed") {
		t.Fatalf("error = %v", err)
	}
	if active, readErr := helperruntime.ReadActive(root); readErr != nil || active != "v2.0.0" {
		t.Fatalf("active = %q, %v", active, readErr)
	}
	if _, statErr := os.Stat(helperruntime.VersionPath(root, "v3.0.0", runtime.GOOS)); !os.IsNotExist(statErr) {
		t.Fatalf("stale candidate was published: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(root, PendingFileName)); !os.IsNotExist(statErr) {
		t.Fatalf("stale candidate wrote pending state: %v", statErr)
	}
}

func TestReconcileRejectsDisappearedSourceRuntime(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	candidate := filepath.Join(dir, "candidate")
	writeExecutable(t, candidate, "candidate")
	hash, err := fileSHA256(candidate)
	if err != nil {
		t.Fatal(err)
	}
	request := Context{
		Schema:          ProtocolVersion,
		CandidatePath:   candidate,
		CandidateSHA256: hash,
		SourceVersion:   "v1.0.0",
		TargetVersion:   "v2.0.0",
		RuntimeRoot:     root,
		EntryPath:       filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS)),
		RequestID:       "missing-source",
	}
	err = helperruntime.WithRootLock(context.Background(), root, func() error {
		_, reconcileErr := reconcileLocked(request, candidate, false)
		return reconcileErr
	})
	if err == nil || !strings.Contains(err.Error(), "disappeared") {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Stat(helperruntime.VersionPath(root, "v2.0.0", runtime.GOOS)); !os.IsNotExist(statErr) {
		t.Fatalf("candidate was published without its source runtime: %v", statErr)
	}
}

func TestResumePendingRemovesMarkerAfterConvergence(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX launcher fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	recordPath := filepath.Join(dir, "config", "install.json")
	writeDynamicLauncher(t, entry, root)
	source := filepath.Join(dir, "candidate")
	writeExecutable(t, source, "candidate")
	hash, _ := fileSHA256(source)
	request := Context{
		Schema:          ProtocolVersion,
		CandidatePath:   source,
		CandidateSHA256: hash,
		TargetVersion:   "v2.0.0",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RecordPath:      recordPath,
		RequestID:       "resume-request",
	}
	if err := helperruntime.WithRootLock(context.Background(), root, func() error {
		_, err := reconcileLocked(request, source, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v2.0.0")
	t.Setenv(helperruntime.EnvEntryPath, entry)
	if err := ResumePending(context.Background(), "v2.0.0"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, PendingFileName)); !os.IsNotExist(err) {
		t.Fatalf("pending marker still exists: %v", err)
	}
}

func TestResumePendingPreviousRuntimeCompletesPublishBeforeActivationCrash(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX launcher fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	recordPath := filepath.Join(dir, "config", "install.json")
	writeDynamicLauncher(t, entry, root)
	v1 := filepath.Join(dir, "v1")
	v2 := filepath.Join(dir, "v2")
	writeExecutable(t, v1, "v1")
	writeExecutable(t, v2, "v2")
	if _, err := helperruntime.InstallVersion(root, v1, "v1.0.0", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	target, err := helperruntime.InstallVersion(root, v2, "v2.0.0", runtime.GOOS, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	hash, _ := fileSHA256(target)
	if err := writePending(filepath.Join(root, PendingFileName), Pending{
		Schema:         ProtocolVersion,
		TargetVersion:  "v2.0.0",
		TargetSHA256:   hash,
		PreviousActive: "v1.0.0",
		TargetActive:   target,
		RuntimeRoot:    root,
		EntryPath:      entry,
		RecordPath:     recordPath,
		RequestID:      "publish-before-activate",
	}); err != nil {
		t.Fatal(err)
	}
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v1.0.0")
	t.Setenv(helperruntime.EnvEntryPath, entry)
	if err := ResumePending(context.Background(), "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	if active, err := helperruntime.ReadActive(root); err != nil || active != "v2.0.0" {
		t.Fatalf("active = %q, %v", active, err)
	}
	if previous, err := helperruntime.ReadPrevious(root); err != nil || previous != "v1.0.0" {
		t.Fatalf("previous = %q, %v", previous, err)
	}
	if _, err := os.Stat(filepath.Join(root, PendingFileName)); !os.IsNotExist(err) {
		t.Fatalf("pending marker still exists: %v", err)
	}
}

func TestResumePendingRejectsMissingPublishedTarget(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	pendingPath := filepath.Join(root, PendingFileName)
	if err := writePending(pendingPath, Pending{
		Schema:         ProtocolVersion,
		TargetVersion:  "v2.0.0",
		TargetSHA256:   strings.Repeat("a", 64),
		PreviousActive: "v1.0.0",
		TargetActive:   helperruntime.VersionPath(root, "v2.0.0", runtime.GOOS),
		RuntimeRoot:    root,
		EntryPath:      entry,
		RequestID:      "missing-target",
	}); err != nil {
		t.Fatal(err)
	}
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v1.0.0")
	t.Setenv(helperruntime.EnvEntryPath, entry)
	err := ResumePending(context.Background(), "v1.0.0")
	if err == nil || !strings.Contains(err.Error(), "hash pending update target") {
		t.Fatalf("error = %v", err)
	}
	if active, readErr := helperruntime.ReadActive(root); readErr != nil || active != "v1.0.0" {
		t.Fatalf("active = %q, %v", active, readErr)
	}
	if _, statErr := os.Stat(pendingPath); statErr != nil {
		t.Fatalf("pending recovery state was removed after failure: %v", statErr)
	}
}

func TestResumePendingRejectsUnsafePermissions(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	pendingPath := filepath.Join(root, PendingFileName)
	if err := os.WriteFile(pendingPath, []byte(`{"schema":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v2.0.0")
	t.Setenv(helperruntime.EnvEntryPath, entry)
	err := ResumePending(context.Background(), "v2.0.0")
	if runtime.GOOS != "windows" {
		if err == nil || !strings.Contains(err.Error(), "permissions") {
			t.Fatalf("error = %v", err)
		}
	}
}

func TestWritePendingAtomicallyReplacesExistingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), PendingFileName)
	first := Pending{Schema: ProtocolVersion, TargetVersion: "v1.0.0", RequestID: "first"}
	second := Pending{Schema: ProtocolVersion, TargetVersion: "v2.0.0", RequestID: "second"}
	if err := writePending(path, first); err != nil {
		t.Fatal(err)
	}
	if err := writePending(path, second); err != nil {
		t.Fatal(err)
	}
	got, err := readPending(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.RequestID != "second" || got.TargetVersion != "v2.0.0" {
		t.Fatalf("pending = %#v", got)
	}
}

func TestValidateContextRejectsUnownedRuntimeRoot(t *testing.T) {
	dir := t.TempDir()
	err := validateContext(Context{
		Schema:          ProtocolVersion,
		CandidateSHA256: strings.Repeat("a", 64),
		TargetVersion:   "v2.0.0",
		RuntimeRoot:     filepath.Join(dir, "other-runtime"),
		EntryPath:       filepath.Join(dir, "cxp"),
		RequestID:       "request",
	}, "v2.0.0")
	if err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("validateContext error = %v", err)
	}
}

func TestSamePathResolvesMultiHopRuntimeSymlinks(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink fixture")
	}
	dir := t.TempDir()
	storage := filepath.Join(dir, "storage")
	hop := filepath.Join(dir, "runtime-hop")
	logical := filepath.Join(dir, "install", ".cxp-runtime")
	physical := filepath.Join(storage, "versions", "v2.0.0")
	if err := os.MkdirAll(physical, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(logical), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(storage, hop); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(hop, logical); err != nil {
		t.Fatal(err)
	}
	if !samePath(filepath.Join(logical, "versions", "v2.0.0"), physical) {
		t.Fatal("multi-hop logical runtime path did not match its physical path")
	}
}

func TestLegacyRuntimeNeedsBridgeUsesExactAffectedRange(t *testing.T) {
	for _, tc := range []struct {
		version string
		want    bool
	}{
		{version: "v0.1.13-rc.27"},
		{version: "v0.1.13-rc.28", want: true},
		{version: "v0.1.13-rc.31", want: true},
		{version: "v0.1.13-rc.35", want: true},
		{version: "v0.1.13-rc.36"},
		{version: "v0.1.13"},
	} {
		if got := legacyRuntimeNeedsBridge(tc.version); got != tc.want {
			t.Fatalf("legacyRuntimeNeedsBridge(%q) = %v, want %v", tc.version, got, tc.want)
		}
	}
}

func TestStableReplacementTargetPreservesManagedSymlinkLeaf(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	running := filepath.Join(root, "versions", "v0.1.13-rc.31", "cxp")
	managed := filepath.Join(dir, "managed-launcher")
	entry := filepath.Join(dir, "cxp")
	if err := os.MkdirAll(filepath.Dir(running), 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, running, "old-runtime")
	writeExecutable(t, managed, "old-runtime")
	if err := os.Symlink(filepath.Base(managed), entry); err != nil {
		t.Fatal(err)
	}
	target, expectedHash, err := stableReplacementTarget(entry, running, root)
	if err != nil {
		t.Fatal(err)
	}
	if target != managed {
		t.Fatalf("target = %q, want %q", target, managed)
	}
	newCandidate := filepath.Join(dir, "candidate")
	writeExecutable(t, newCandidate, "new-runtime")
	if err := copyExecutableAtomically(newCandidate, target, expectedHash); err != nil {
		t.Fatal(err)
	}
	if link, err := os.Readlink(entry); err != nil || link != filepath.Base(managed) {
		t.Fatalf("entry symlink changed: %q, %v", link, err)
	}
	data, err := os.ReadFile(managed)
	if err != nil || string(data) != "new-runtime" {
		t.Fatalf("managed launcher = %q, %v", data, err)
	}
}

func TestStableReplacementTargetRejectsImmutableRuntimeSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	running := filepath.Join(root, "versions", "v0.1.13-rc.31", "cxp")
	entry := filepath.Join(dir, "cxp")
	if err := os.MkdirAll(filepath.Dir(running), 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, running, "old-runtime")
	if err := os.Symlink(running, entry); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stableReplacementTarget(entry, running, root); err == nil || !strings.Contains(err.Error(), "immutable runtime") {
		t.Fatalf("error = %v", err)
	}
}

func TestStableReplacementTargetRejectsImmutableRuntimeThroughSymlinkedRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink fixture")
	}
	dir := t.TempDir()
	storage := filepath.Join(dir, "runtime-storage")
	root := filepath.Join(dir, "install", ".cxp-runtime")
	running := filepath.Join(root, "versions", "v0.1.13-rc.31", "cxp")
	physicalRunning := filepath.Join(storage, "versions", "v0.1.13-rc.31", "cxp")
	entry := filepath.Join(dir, "install", "cxp")
	if err := os.MkdirAll(filepath.Dir(physicalRunning), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(root), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(storage, root); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, physicalRunning, "old-runtime")
	if err := os.Symlink(physicalRunning, entry); err != nil {
		t.Fatal(err)
	}
	if _, _, err := stableReplacementTarget(entry, running, root); err == nil || !strings.Contains(err.Error(), "immutable runtime") {
		t.Fatalf("error = %v", err)
	}
}

func TestStableReplacementRejectsTargetChangedAfterValidation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX replacement fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	running := filepath.Join(root, "versions", "v0.1.13-rc.31", "cxp")
	entry := filepath.Join(dir, "cxp")
	if err := os.MkdirAll(filepath.Dir(running), 0o755); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, running, "old-runtime")
	writeExecutable(t, entry, "old-runtime")
	target, expectedHash, err := stableReplacementTarget(entry, running, root)
	if err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, entry, "changed-runtime")
	candidate := filepath.Join(dir, "candidate")
	writeExecutable(t, candidate, "new-runtime")
	if err := copyExecutableAtomically(candidate, target, expectedHash); err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("error = %v", err)
	}
}

func writeDynamicLauncher(t *testing.T, path string, root string) {
	t.Helper()
	quotedRoot := strings.ReplaceAll(root, "'", "'\\''")
	script := "#!/bin/sh\nactive=$(cat '" + quotedRoot + "/active')\necho \"codex-proxy version ${active#v}\"\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
}

func writeExecutable(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
}
