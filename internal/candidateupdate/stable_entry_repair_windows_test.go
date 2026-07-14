//go:build windows

package candidateupdate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"golang.org/x/sys/windows"
)

func TestWindowsCandidateOwnedUpdateDefersLockedStableEntry(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, "cxp.exe")
	oldRuntime := filepath.Join(dir, "old-runtime.exe")
	candidate := filepath.Join(dir, "candidate.exe")
	writePostParentTestFile(t, entry, []byte("old-runtime"))
	writePostParentTestFile(t, oldRuntime, []byte("old-runtime"))
	writePostParentTestFile(t, candidate, []byte("new-runtime"))
	if _, err := helperruntime.InstallVersion(root, oldRuntime, "v0.1.14", "windows", false); err != nil {
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
		RequestID:       "windows-locked-entry",
	}
	previousReplace := replaceManagedStableEntryFn
	replaceManagedStableEntryFn = func(string, string, string, string) error {
		return windows.ERROR_SHARING_VIOLATION
	}
	t.Cleanup(func() { replaceManagedStableEntryFn = previousReplace })

	var result Result
	if err := helperruntime.WithRootLock(context.Background(), root, func() error {
		var err error
		result, err = reconcileLocked(request, candidate, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if result.stableEntryRepair == nil {
		t.Fatal("locked stable entry did not produce deferred repair state")
	}
	repair := result.stableEntryRepair
	if !samePath(repair.TargetPath, entry) {
		t.Fatalf("deferred target = %q, want %q", repair.TargetPath, entry)
	}
	wantSource := helperruntime.VersionPath(root, "v0.1.14-rc.19", "windows")
	if !samePath(repair.SourcePath, wantSource) {
		t.Fatalf("deferred source = %q, want %q", repair.SourcePath, wantSource)
	}
	if repair.SourceSHA256 != candidateHash || repair.ExpectedCurrentSHA256 == "" {
		t.Fatalf("deferred hashes = %#v", repair)
	}
	if active, err := helperruntime.ReadActive(root); err != nil || active != "v0.1.14-rc.19" {
		t.Fatalf("active = %q, %v", active, err)
	}
	if data, err := os.ReadFile(entry); err != nil || string(data) != "old-runtime" {
		t.Fatalf("locked stable entry changed before worker ran: %q, %v", data, err)
	}
}

func TestWindowsDeferredStableEntryRepairArmsOnlyOneWorker(t *testing.T) {
	previousParent := managedStableEntryParentPID
	previousSchedule := managedStableEntryScheduleRepair
	t.Cleanup(func() {
		managedStableEntryParentPID = previousParent
		managedStableEntryScheduleRepair = previousSchedule
	})
	managedStableEntryParentPID = func(string) (int, error) { return 4242, nil }
	var calls int
	var gotSource string
	var gotRequest postParentRepairRequest
	managedStableEntryScheduleRepair = func(source string, request postParentRepairRequest) error {
		calls++
		gotSource = source
		gotRequest = request
		return nil
	}

	err := scheduleDeferredStableEntryRepair(Result{stableEntryRepair: &stableEntryRepair{
		SourcePath:            `C:\install\.cxp-runtime\versions\v0.1.14-rc.19\cxp.exe`,
		TargetPath:            `C:\install\cxp.exe`,
		ExpectedCurrentSHA256: strings.Repeat("a", 64),
		SourceSHA256:          strings.Repeat("b", 64),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("worker schedule calls = %d, want 1", calls)
	}
	if gotSource == "" || gotRequest.ParentPID != 4242 || !gotRequest.AllowUncommittedManagedTarget {
		t.Fatalf("scheduled repair = source %q request %#v", gotSource, gotRequest)
	}
}

func TestWindowsDeferredStableEntryRepairTreatsMissingLauncherAsNoop(t *testing.T) {
	previousParent := managedStableEntryParentPID
	t.Cleanup(func() { managedStableEntryParentPID = previousParent })
	managedStableEntryParentPID = func(string) (int, error) { return 0, nil }
	if err := scheduleDeferredStableEntryRepair(Result{stableEntryRepair: &stableEntryRepair{SourcePath: "source", TargetPath: "target"}}); err != nil {
		t.Fatal(err)
	}
}
