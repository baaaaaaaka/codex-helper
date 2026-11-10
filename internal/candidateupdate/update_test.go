package candidateupdate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
)

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

func TestApplyVerificationFailureLeavesEntryAndDefaultUntouched(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("inert POSIX version fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	candidate := filepath.Join(dir, "candidate")
	writeVersionScript(t, entry, "1.0.0")
	writeVersionScript(t, candidate, "9.9.9")
	before, err := os.ReadFile(entry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := helperruntime.InstallVersion(root, entry, "v1.0.0", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	hash, err := fileSHA256(candidate)
	if err != nil {
		t.Fatal(err)
	}
	old := candidateExecutablePath
	candidateExecutablePath = func() (string, error) { return candidate, nil }
	t.Cleanup(func() { candidateExecutablePath = old })
	_, err = Apply(context.Background(), Context{Schema: ProtocolVersion, CandidateSHA256: hash, SourceVersion: "v1.0.0", TargetVersion: "v2.0.0", RuntimeRoot: root, EntryPath: entry, RecordPath: filepath.Join(dir, "install.json"), RequestID: "bad-version"}, "v2.0.0")
	if err == nil {
		t.Fatal("mismatched immutable runtime accepted")
	}
	if got, err := helperruntime.ReadActive(root); err != nil || got != "v1.0.0" {
		t.Fatalf("%s %v", got, err)
	}
	if after, err := os.ReadFile(entry); err != nil || !bytes.Equal(before, after) {
		t.Fatal("stable entry changed before verification")
	}
	if _, err := os.Stat(filepath.Join(root, "pending-update.json")); !os.IsNotExist(err) {
		t.Fatalf("pending activation after verification error: %v", err)
	}
}

func TestApplyVerifiesPublishedRuntimeInsteadOfStableEntry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX executable fixture")
	}

	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	oldRuntime := filepath.Join(dir, "old-runtime")
	candidate := filepath.Join(dir, "candidate-runtime")
	writeVersionScript(t, oldRuntime, "0.1.14")
	writeVersionScript(t, candidate, "0.1.14-rc.20")
	if _, err := helperruntime.InstallVersion(root, oldRuntime, "v0.1.14", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v0.1.14"); err != nil {
		t.Fatal(err)
	}
	oldEntry, err := os.ReadFile(oldRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry, oldEntry, 0o700); err != nil {
		t.Fatal(err)
	}
	candidateHash, err := fileSHA256(candidate)
	if err != nil {
		t.Fatal(err)
	}

	previousCandidateExecutablePath := candidateExecutablePath
	t.Cleanup(func() {
		candidateExecutablePath = previousCandidateExecutablePath
	})
	candidateExecutablePath = func() (string, error) { return candidate, nil }
	// This models a split install whose stable launcher is intentionally kept
	// by the owner of the running process. Apply must still validate and return
	// the newly published immutable runtime instead of rejecting its version
	// because the preserved launcher reports the old stable version.

	result, err := Apply(context.Background(), Context{
		Schema:          ProtocolVersion,
		CandidateSHA256: candidateHash,
		SourceVersion:   "v0.1.14",
		TargetVersion:   "0.1.14-rc.20",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RecordPath:      filepath.Join(dir, "install.json"),
		RequestID:       "split-entry-rc20",
	}, "v0.1.14-rc.20")
	if err != nil {
		t.Fatalf("Apply split-entry update: %v", err)
	}
	wantRuntime := helperruntime.VersionPath(root, "v0.1.14-rc.20", runtime.GOOS)
	if result.RuntimePath != wantRuntime || result.Version != "0.1.14-rc.20" {
		t.Fatalf("result = %#v, want runtime %q", result, wantRuntime)
	}
	if active, readErr := helperruntime.ReadActive(root); readErr != nil || active != "v0.1.14-rc.20" {
		t.Fatalf("active = %q, %v", active, readErr)
	}
	if err := verifyImmutableRuntime(context.Background(), result.RuntimePath, "0.1.14-rc.20"); err != nil {
		t.Fatalf("published immutable runtime verification: %v", err)
	}
	// The preserved stable entry is deliberately still the old physical
	// launcher. Its output must not participate in candidate-owned success.
	out, err := exec.Command(entry, "--version").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "0.1.14") || strings.Contains(string(out), "rc.20") {
		t.Fatalf("preserved stable entry output = %q, %v", out, err)
	}
}

func TestSafeBootManualApplyPreservesBootstrapAndUsesSingleRecord(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("inert POSIX version fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	candidate := filepath.Join(dir, "candidate")
	writeVersionScript(t, entry, "1.0.0")
	writeVersionScript(t, candidate, "2.0.0")
	if _, err := helperruntime.InstallVersion(root, entry, "v1.0.0", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	hash, _ := fileSHA256(candidate)
	current := helperruntime.Context{Root: root, EntryPath: entry, Version: "v1.0.0"}
	if err := helperruntime.StageBootCandidate(context.Background(), current, "v1.0.0", 1, candidate, "v3.0.0", hash); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(entry)
	hash, _ = fileSHA256(candidate)
	old := candidateExecutablePath
	candidateExecutablePath = func() (string, error) { return candidate, nil }
	t.Cleanup(func() { candidateExecutablePath = old })
	_, err := Apply(context.Background(), Context{Schema: 1, CandidateSHA256: hash, SourceVersion: "v1.0.0", TargetVersion: "v2.0.0", RuntimeRoot: root, EntryPath: entry, RecordPath: filepath.Join(dir, "install.json"), RequestID: "safe-manual"}, "v2.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if after, _ := os.ReadFile(entry); !bytes.Equal(before, after) {
		t.Fatal("manual update replaced the protected bootstrap")
	}
	got, err := helperruntime.ReadBoot(root)
	if err != nil || got.Good != "v2.0.0" || got.Previous != "v1.0.0" || got.Candidate != nil || got.Generation != 3 {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(root, updatestate.Filename)); !os.IsNotExist(err) {
		t.Fatalf("legacy pending marker was written: %v", err)
	}
	// Even a malformed pre-existing legacy marker cannot drive safe startup.
	if err := os.WriteFile(filepath.Join(root, "pending-update.json"), []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v2.0.0")
	t.Setenv(helperruntime.EnvEntryPath, entry)
	if err := CheckNativeBootstrap(); err != nil {
		t.Fatal(err)
	}
}

func TestSafeBootInvokeRejectsLegacyCandidateBeforeAnyMutation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("inert POSIX protocol fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"schema":1,"generation":1,"good":"v1.0.0"}`)
	if err := os.WriteFile(filepath.Join(root, helperruntime.BootFile), raw, 0600); err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(dir, "legacy-candidate")
	writeVersionScript(t, candidate, "2.0.0")
	if _, err := Invoke(context.Background(), candidate, Context{RuntimeRoot: root}, 0); err == nil {
		t.Fatal("candidate without safe-boot support was invoked")
	}
	after, _ := os.ReadFile(filepath.Join(root, helperruntime.BootFile))
	if !bytes.Equal(raw, after) {
		t.Fatal("legacy candidate changed startup record")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 2 {
		t.Fatalf("invocation context left behind: %v %v", files, err)
	}
}

func TestHandleInternalCommandAppliesCurrentCandidate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX launcher fixture")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	candidate := filepath.Join(dir, "candidate")
	recordPath := filepath.Join(dir, "config", "install.json")
	writeVersionScript(t, entry, "1.0.0")
	if err := helperruntime.Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	writeVersionScript(t, candidate, "9.8.7")
	hash, err := fileSHA256(candidate)
	if err != nil {
		t.Fatal(err)
	}
	previousCandidateExecutablePath := candidateExecutablePath
	candidateExecutablePath = func() (string, error) { return candidate, nil }
	t.Cleanup(func() { candidateExecutablePath = previousCandidateExecutablePath })
	request := Context{
		Schema:          ProtocolVersion,
		CandidatePath:   candidate,
		CandidateSHA256: hash,
		TargetVersion:   "v9.8.7",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RecordPath:      recordPath,
		RequestID:       "handler-request",
	}
	contextPath, err := writeInvocationContext(candidate, request)
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code, handled := HandleInternalCommand([]string{
		"candidate",
		InternalCommand,
		"--protocol=2",
		"--state-db=" + contextPath,
		"--request-id=" + request.RequestID,
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
	if err := helperruntime.Activate(root, "v0.9.0"); err != nil {
		t.Fatal(err)
	}

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
		_, err := reconcileLocked(req1, v1)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if active, err := helperruntime.ReadActive(root); err != nil || active != "v1.0.0" {
		t.Fatalf("active = %q, %v", active, err)
	}
	if err := os.WriteFile(entry, []byte("candidate-v1"), 0o700); err != nil {
		t.Fatal(err)
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
	apply := func(source string) error {
		return helperruntime.WithRootLock(context.Background(), root, func() error {
			_, err := reconcileLocked(req2, source)
			return err
		})
	}
	if err := apply(v2); err != nil {
		t.Fatal(err)
	}
	if err := apply(helperruntime.VersionPath(root, "v2.0.0", runtime.GOOS)); err != nil {
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
	if _, err := os.Stat(filepath.Join(root, updatestate.Filename)); !os.IsNotExist(err) {
		t.Fatalf("native update created obsolete pending intent: %v", err)
	}
}

func TestReconcilePreservesBootstrapAndShim(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink topology is covered by native Windows CI")
	}
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	canonical := filepath.Join(dir, helperpath.BinaryName(runtime.GOOS))
	entry := filepath.Join(dir, "cxp")
	oldRuntime := filepath.Join(dir, "old-runtime")
	candidate := filepath.Join(dir, "candidate")
	writeExecutable(t, canonical, "old-runtime")
	writeExecutable(t, oldRuntime, "old-runtime")
	writeExecutable(t, candidate, "new-runtime")
	if err := os.Symlink(filepath.Base(canonical), entry); err != nil {
		t.Fatal(err)
	}
	if _, err := helperruntime.InstallVersion(root, oldRuntime, "v0.1.14", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v0.1.14"); err != nil {
		t.Fatal(err)
	}
	candidateHash, err := fileSHA256(candidate)
	if err != nil {
		t.Fatal(err)
	}
	request := Context{
		Schema:          ProtocolVersion,
		CandidateSHA256: candidateHash,
		SourceVersion:   "v0.1.14",
		TargetVersion:   "v0.1.14-rc.19",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RecordPath:      filepath.Join(dir, "install.json"),
		RequestID:       "managed-entry-refresh",
	}
	if err := helperruntime.WithRootLock(context.Background(), root, func() error {
		_, err := reconcileLocked(request, candidate)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if link, err := os.Readlink(entry); err != nil || link != filepath.Base(canonical) {
		t.Fatalf("stable shim changed: %q, %v", link, err)
	}
	if data, err := os.ReadFile(canonical); err != nil || string(data) != "old-runtime" {
		t.Fatalf("managed stable target = %q, %v", data, err)
	}
	if active, err := helperruntime.ReadActive(root); err != nil || active != "v0.1.14-rc.19" {
		t.Fatalf("active runtime = %q, %v", active, err)
	}
	if previous, err := helperruntime.ReadPrevious(root); err != nil || previous != "v0.1.14" {
		t.Fatalf("previous runtime = %q, %v", previous, err)
	}
}

func TestReconcilePreservesBootstrap(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	oldRuntime := filepath.Join(dir, "old-runtime")
	candidate := filepath.Join(dir, "candidate")
	writeExecutable(t, entry, "old-runtime")
	writeExecutable(t, oldRuntime, "old-runtime")
	writeExecutable(t, candidate, "new-runtime")
	if _, err := helperruntime.InstallVersion(root, oldRuntime, "v0.1.14", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v0.1.14"); err != nil {
		t.Fatal(err)
	}
	candidateHash, err := fileSHA256(candidate)
	if err != nil {
		t.Fatal(err)
	}
	request := Context{
		Schema:          ProtocolVersion,
		CandidateSHA256: candidateHash,
		SourceVersion:   "v0.1.14",
		TargetVersion:   "v0.1.14-rc.19",
		RuntimeRoot:     root,
		EntryPath:       entry,
		RecordPath:      filepath.Join(dir, "install.json"),
		RequestID:       "managed-entry-refresh-regular",
	}
	if err := helperruntime.WithRootLock(context.Background(), root, func() error {
		_, err := reconcileLocked(request, candidate)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(entry); err != nil || string(data) != "old-runtime" {
		t.Fatalf("managed stable entry = %q, %v", data, err)
	}
	if active, err := helperruntime.ReadActive(root); err != nil || active != "v0.1.14-rc.19" {
		t.Fatalf("active runtime = %q, %v", active, err)
	}
}

func TestReconcileRequiresPresentBootstrapBeforeActivation(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	oldRuntime := filepath.Join(dir, "old-runtime")
	candidate := filepath.Join(dir, "candidate")
	// A missing dispatcher requires the explicit installer.
	writeExecutable(t, oldRuntime, "old-runtime")
	writeExecutable(t, candidate, "new-runtime")
	if _, err := helperruntime.InstallVersion(root, oldRuntime, "v0.1.14", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v0.1.14"); err != nil {
		t.Fatal(err)
	}
	candidateHash, err := fileSHA256(candidate)
	if err != nil {
		t.Fatal(err)
	}
	err = helperruntime.WithRootLock(context.Background(), root, func() error {
		_, err := reconcileLocked(Context{
			Schema:          ProtocolVersion,
			CandidateSHA256: candidateHash,
			SourceVersion:   "v0.1.14",
			TargetVersion:   "v0.1.14-rc.19",
			RuntimeRoot:     root,
			EntryPath:       entry,
			RecordPath:      filepath.Join(dir, "install.json"),
			RequestID:       "unmanaged-entry",
		}, candidate)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "safe boot entry") {
		t.Fatalf("reconcile error = %v, want managed-entry refusal", err)
	}
	if active, readErr := helperruntime.ReadActive(root); readErr != nil || active != "v0.1.14" {
		t.Fatalf("active runtime changed after refusal: %q, %v", active, readErr)
	}
	if _, readErr := os.Stat(entry); !os.IsNotExist(readErr) {
		t.Fatal("created missing entry", readErr)
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
		_, reconcileErr := reconcileLocked(request, candidate)
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
	if _, statErr := os.Stat(filepath.Join(root, updatestate.Filename)); !os.IsNotExist(statErr) {
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
		_, reconcileErr := reconcileLocked(request, candidate)
		return reconcileErr
	})
	if err == nil || !errors.Is(err, helperruntime.ErrBootstrapState) {
		t.Fatalf("error = %v", err)
	}
	if _, statErr := os.Stat(helperruntime.VersionPath(root, "v2.0.0", runtime.GOOS)); !os.IsNotExist(statErr) {
		t.Fatalf("candidate was published without its source runtime: %v", statErr)
	}
}

func TestSafeBootNeverReplaysLegacyPending(t *testing.T) {
	for _, mode := range []string{"unpublished-target", "already-active", "corrupt-intent", "lost-native"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			root := filepath.Join(dir, ".cxp-runtime")
			entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
			if err := helperruntime.Activate(root, "v1.0.0"); err != nil {
				t.Fatal(err)
			}
			if mode == "already-active" {
				if err := helperruntime.Activate(root, "v2.0.0"); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := os.ReadFile(filepath.Join(root, helperruntime.BootFile))
			legacy := []byte(`{"schema":1,"target_version":"v2.0.0","previous_active":"v1.0.0","request_id":"old-in-flight"}`)
			if mode == "corrupt-intent" {
				legacy = []byte("{broken")
			}
			path := filepath.Join(root, "pending-update.json")
			if err := os.WriteFile(path, legacy, 0644); err != nil {
				t.Fatal(err)
			}
			if mode == "lost-native" {
				if err := os.Remove(filepath.Join(root, helperruntime.BootFile)); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv(helperruntime.EnvRuntime, "1")
			t.Setenv(helperruntime.EnvRuntimeRoot, root)
			t.Setenv(helperruntime.EnvRuntimeVersion, "v1.0.0")
			t.Setenv(helperruntime.EnvEntryPath, entry)
			err := CheckNativeBootstrap()
			if mode == "lost-native" {
				if !errors.Is(err, helperruntime.ErrBootstrapState) {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if after, _ := os.ReadFile(filepath.Join(root, helperruntime.BootFile)); !bytes.Equal(before, after) {
					t.Fatal("legacy intent changed native selection")
				}
			}
			if got, _ := os.ReadFile(path); !bytes.Equal(got, legacy) {
				t.Fatal("changed legacy evidence during startup")
			}
			if _, err := os.Stat(filepath.Join(root, updatestate.Filename)); !os.IsNotExist(err) {
				t.Fatal("startup created update database")
			}
		})
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

func writeDynamicLauncher(t *testing.T, path string, root string) {
	t.Helper()
	quotedRoot := strings.ReplaceAll(root, "'", "'\\''")
	script := "#!/bin/sh\nactive=$(cat '" + quotedRoot + "/active')\necho \"codex-proxy version ${active#v}\"\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
}

func writeVersionScript(t *testing.T, path string, version string) {
	t.Helper()
	script := "#!/bin/sh\n" +
		"if [ \"${1-}\" = \"--cxp-boot-protocol\" ]; then echo 2; exit 0; fi\n" +
		"if [ \"${1-}\" = \"--version\" ]; then\n" +
		"  echo 'codex-proxy version " + version + "'\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 2\n"
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
