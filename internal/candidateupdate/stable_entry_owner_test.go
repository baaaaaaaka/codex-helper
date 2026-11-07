package candidateupdate

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
)

func saveManagedTestRecord(t *testing.T, recordPath string, target string, goos string, state managedinstall.TargetState) {
	t.Helper()
	if err := managedinstall.SaveRecord(recordPath, managedinstall.Record{
		TargetPath:   target,
		TargetSource: string(managedinstall.SourceRecord),
		TargetState:  string(state),
		Version:      "0.1.22-rc.66",
		GOOS:         goos,
		GOARCH:       "amd64",
	}); err != nil {
		t.Fatal(err)
	}
}

// The install record names codex-proxy; the cxp beside it (a copy on
// Windows) belongs to the same install. Nothing else does.
func TestRecordOwnsStableEntryBesideTheRecordedTarget(t *testing.T) {
	for _, goos := range []string{"linux", "windows"} {
		t.Run(goos, func(t *testing.T) {
			dir := t.TempDir()
			recordPath := filepath.Join(dir, "install.json")
			bin := filepath.Join(dir, "bin")
			saveManagedTestRecord(t, recordPath, filepath.Join(bin, helperpath.BinaryName(goos)), goos, managedinstall.StateManaged)
			cases := []struct {
				path string
				want bool
			}{
				{filepath.Join(bin, helperpath.BinaryName(goos)), true},
				{filepath.Join(bin, helperruntime.BinaryName(goos)), true},
				{filepath.Join(dir, "other", helperruntime.BinaryName(goos)), false},
				{filepath.Join(bin, "tool"), false},
			}
			for _, c := range cases {
				if got, err := recordOwnsStableEntry(recordPath, c.path, goos); err != nil || got != c.want {
					t.Errorf("recordOwnsStableEntry(%s) = %v, %v; want %v", c.path, got, err, c.want)
				}
			}
			saveManagedTestRecord(t, recordPath, filepath.Join(bin, helperpath.BinaryName(goos)), goos, managedinstall.StateExplicit)
			if got, err := recordOwnsStableEntry(recordPath, filepath.Join(bin, helperruntime.BinaryName(goos)), goos); err != nil || got {
				t.Errorf("an explicit (user-owned) record owns the cxp entry: %v, %v", got, err)
			}
			if got, err := recordOwnsStableEntry(filepath.Join(dir, "missing.json"), filepath.Join(bin, helperruntime.BinaryName(goos)), goos); err != nil || got {
				t.Errorf("a missing record owns the cxp entry: %v, %v", got, err)
			}
		})
	}
}

// A field state on Windows (2026-11-07): an earlier update could not replace
// the copied cxp entry while the Teams service held it, so the entry still
// holds an older helper while the active runtime moved on. The next update
// refused it ("is not a recorded managed helper") and every later one did
// too. The entry beside the recorded codex-proxy is the install's own and
// is refreshed.
func TestReconcileRefreshesAStaleCopiedStableEntry(t *testing.T) {
	previousVerify := verifyPhysicalHelper
	verifyPhysicalHelper = func(string) (string, error) { return "v0.1.20", nil }
	t.Cleanup(func() { verifyPhysicalHelper = previousVerify })

	run := func(t *testing.T, recorded bool) (string, error) {
		dir := t.TempDir()
		root := filepath.Join(dir, ".cxp-runtime")
		entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
		canonical := filepath.Join(dir, helperpath.BinaryName(runtime.GOOS))
		oldRuntime := filepath.Join(dir, "old-runtime")
		candidate := filepath.Join(dir, "candidate")
		recordPath := filepath.Join(dir, "config", "install.json")
		writeExecutable(t, entry, "older-helper") // the copy the deferred repair never replaced
		writeExecutable(t, canonical, "old-runtime")
		writeExecutable(t, oldRuntime, "old-runtime")
		writeExecutable(t, candidate, "new-runtime")
		if recorded {
			saveManagedTestRecord(t, recordPath, canonical, runtime.GOOS, managedinstall.StateManaged)
		}
		if _, err := helperruntime.InstallVersion(root, oldRuntime, "v0.1.22-rc.66", runtime.GOOS, false); err != nil {
			t.Fatal(err)
		}
		if err := helperruntime.Activate(root, "v0.1.22-rc.66"); err != nil {
			t.Fatal(err)
		}
		hash, err := fileSHA256(candidate)
		if err != nil {
			t.Fatal(err)
		}
		request := Context{
			Schema:          ProtocolVersion,
			CandidateSHA256: hash,
			TargetVersion:   "v0.1.22-rc.69",
			RuntimeRoot:     root,
			EntryPath:       entry,
			RecordPath:      recordPath,
			RequestID:       "stale-copied-entry",
		}
		err = helperruntime.WithRootLock(context.Background(), root, func() error {
			_, err := reconcileLocked(request, candidate, false)
			return err
		})
		data, _ := os.ReadFile(entry)
		return string(data), err
	}

	t.Run("beside the recorded target", func(t *testing.T) {
		data, err := run(t, true)
		if err != nil {
			t.Fatalf("update refused the stale cxp entry: %v", err)
		}
		if data != "new-runtime" {
			t.Fatalf("cxp entry = %q, want the candidate", data)
		}
	})
	t.Run("without an install record", func(t *testing.T) {
		data, err := run(t, false)
		if err == nil || !strings.Contains(err.Error(), "is not a recorded managed helper") {
			t.Fatalf("an unowned stale entry was not refused: %v", err)
		}
		if data != "older-helper" {
			t.Fatalf("an unowned entry was changed: %q", data)
		}
	})
}
