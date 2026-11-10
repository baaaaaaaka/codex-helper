package helperruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gofrs/flock"
)

func bootFixture(t *testing.T) (Context, string, string) {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	old := filepath.Join(dir, "old")
	if err := os.WriteFile(old, []byte("known good original bytes"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := InstallVersion(root, old, "v1.0.0", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "candidate")
	if err := os.WriteFile(source, []byte("verified candidate bytes"), 0755); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256([]byte("verified candidate bytes"))
	return Context{Root: root, EntryPath: filepath.Join(dir, BinaryName(runtime.GOOS)), Version: "v1.0.0", RuntimePath: VersionPath(root, "v1.0.0", runtime.GOOS)}, source, hex.EncodeToString(h[:])
}

func stageBootFixture(t *testing.T) (Context, BootState) {
	t.Helper()
	c, source, digest := bootFixture(t)
	if err := StageBootCandidate(context.Background(), c, "v1.0.0", 1, source, "v2.0.0", digest); err != nil {
		t.Fatal(err)
	}
	s, err := ReadBoot(c.Root)
	if err != nil {
		t.Fatal(err)
	}
	return c, s
}

func TestSafeBootStageDoesNotChangeEntryOrLegacyActive(t *testing.T) {
	c, source, digest := bootFixture(t)
	if err := os.WriteFile(filepath.Join(c.Root, "active"), []byte("v1.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.EntryPath, []byte("independent recovery entry"), 0755); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(c.RuntimePath)
	if err := StageBootCandidate(context.Background(), c, "v1.0.0", 1, source, "v2.0.0", digest); err != nil {
		t.Fatal(err)
	}
	if v, _ := ReadActive(c.Root); v != "v1.0.0" {
		t.Fatal(v)
	}
	if v, _ := readVersionPointer(c.Root, "active"); v != "v1.0.0" {
		t.Fatal(v)
	}
	if entry, _ := os.ReadFile(c.EntryPath); string(entry) != "independent recovery entry" {
		t.Fatal("entry replaced")
	}
	if after, _ := os.ReadFile(c.RuntimePath); string(before) != string(after) {
		t.Fatal("good runtime changed")
	}
}

func TestSafeBootRecordReadersAllowAtomicReplacement(t *testing.T) {
	c, s := stageBootFixture(t)
	reader, err := openVersionPointerFile(filepath.Join(c.Root, BootFile))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	s.Generation++
	if err := writeBoot(c.Root, s); err != nil {
		t.Fatal(err)
	}
	old, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	// The retained reader sees the old complete native database. Query its
	// generation independently; do not inspect encoded SQLite bytes as text.
	oldRoot := t.TempDir()
	if err = os.WriteFile(filepath.Join(oldRoot, BootFile), old, 0600); err != nil {
		t.Fatal(err)
	}
	if oldState, err := ReadBoot(oldRoot); err != nil || oldState.Generation != s.Generation-1 {
		t.Fatalf("old snapshot generation %d: %v", oldState.Generation, err)
	}

	got, err := ReadBoot(c.Root)
	if err != nil || got.Generation != s.Generation {
		t.Fatalf("new reader did not get new complete record: %+v %v", got, err)
	}
}

func TestSafeBootConsumesOnceAndSurvivesLostConfirmation(t *testing.T) {
	c, s := stageBootFixture(t)
	claim, ok := consumeBootCandidate(c.Root, s)
	if !ok || claim.Candidate.Attempt == "" {
		t.Fatal("no durable claim")
	}
	for i := 0; i < 3; i++ {
		s, err := ReadBoot(c.Root)
		if err != nil || s.Good != "v1.0.0" {
			t.Fatalf("next start: %v %v", s, err)
		}
		if _, ok := consumeBootCandidate(c.Root, s); ok {
			t.Fatal("unconfirmed candidate retried")
		}
	}
	// The updater also cannot re-arm this same failed release.
	if err := StageBootCandidate(context.Background(), c, s.Good, claim.Generation, VersionPath(c.Root, "v2.0.0", runtime.GOOS), "v2.0.0", claim.Candidate.SHA256); err != nil {
		t.Fatal(err)
	}
	s, _ = ReadBoot(c.Root)
	if s.Candidate.Attempt != claim.Candidate.Attempt {
		t.Fatal("trial was re-armed")
	}
}

func TestSafeBootConcurrentWindowsClaimOnlyOnce(t *testing.T) {
	c, s := stageBootFixture(t)
	var count atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := consumeBootCandidate(c.Root, s); ok {
				count.Add(1)
			}
		}()
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatalf("claims = %d", count.Load())
	}
}

func TestSafeBootCommitFailureStillSelectsGood(t *testing.T) {
	c, s := stageBootFixture(t)
	prev := commitBootFile
	t.Cleanup(func() { commitBootFile = prev })
	commitBootFile = func(string, string) error { return errors.New("disk full / access denied") }
	if _, ok := consumeBootCandidate(c.Root, s); ok {
		t.Fatal("launched without durable claim")
	}
	if got, _ := ReadBoot(c.Root); got.Good != "v1.0.0" || got.Candidate.Attempt != "" {
		t.Fatal(got)
	}
	commitBootFile = prev
	claim, ok := consumeBootCandidate(c.Root, s)
	if !ok {
		t.Fatal("claim failed")
	}
	commitBootFile = func(string, string) error { return errors.New("read-only state") }
	confirmBootCandidate(c.Root, claim)
	if got, _ := ReadBoot(c.Root); got.Good != "v1.0.0" || got.Candidate.Attempt == "" {
		t.Fatal(got)
	}
}

func TestSafeBootLateConfirmationCannotUndoManualRollback(t *testing.T) {
	c, s := stageBootFixture(t)
	claim, ok := consumeBootCandidate(c.Root, s)
	if !ok {
		t.Fatal("claim failed")
	}
	if err := WithRootLock(context.Background(), c.Root, func() error { return Activate(c.Root, "v1.0.0") }); err != nil {
		t.Fatal(err)
	}
	confirmBootCandidate(c.Root, claim)
	s, _ = ReadBoot(c.Root)
	if s.Good != "v1.0.0" || s.Candidate != nil {
		t.Fatal(s)
	}
}

func TestSafeBootPromotionRetainsGoodAndInvalidatesOldSnapshot(t *testing.T) {
	c, s := stageBootFixture(t)
	claim, ok := consumeBootCandidate(c.Root, s)
	if !ok {
		t.Fatal("claim failed")
	}
	confirmBootCandidate(c.Root, claim)
	s, _ = ReadBoot(c.Root)
	if s.Good != "v2.0.0" || s.Previous != "v1.0.0" || s.Candidate != nil {
		t.Fatal(s)
	}
	confirmBootCandidate(c.Root, claim)
	if again, _ := ReadBoot(c.Root); again.Generation != s.Generation {
		t.Fatal("duplicate confirmation wrote state")
	}
}

func TestSafeBootConfirmedFailureCannotUndoConcurrentExplicitSelection(t *testing.T) {
	c, s := stageBootFixture(t)
	claim, ok := consumeBootCandidate(c.Root, s)
	if !ok {
		t.Fatal("claim failed")
	}
	confirmBootCandidate(c.Root, claim)
	s, _ = ReadBoot(c.Root)
	if err := WithRootLock(context.Background(), c.Root, func() error { return Activate(c.Root, "v3.0.0") }); err != nil {
		t.Fatal(err)
	}
	rejectBootGood(c.Root, s)
	got, err := ReadBoot(c.Root)
	if err != nil || got.Good != "v3.0.0" || got.Previous != "v2.0.0" {
		t.Fatalf("stale failure changed explicit choice: %+v %v", got, err)
	}
}

func TestSafeBootBusyLockDoesNotWaitOrWrite(t *testing.T) {
	c, s := stageBootFixture(t)
	l := flock.New(filepath.Join(c.Root, "runtime.lock"))
	if err := l.Lock(); err != nil {
		t.Fatal(err)
	}
	defer l.Unlock()
	if _, ok := consumeBootCandidate(c.Root, s); ok {
		t.Fatal("claimed while busy")
	}
	if got, _ := ReadBoot(c.Root); got.Generation != s.Generation {
		t.Fatal("state changed")
	}
}

func TestSafeBootRejectsMalformedRecords(t *testing.T) {
	for _, raw := range []string{`{`, `{}`, `{"schema":99,"generation":1,"good":"v1.0.0"}`, `{"schema":1,"generation":1,"good":"../../bin/sh"}`, `{"schema":1,"generation":1,"good":"v1.0.0"} {}`} {
		t.Run(raw, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, BootFile), []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadBoot(root); err == nil {
				t.Fatal("accepted corrupt state")
			}
		})
	}
}

func TestSafeBootMarkersDoNotLeakToWorkloads(t *testing.T) {
	got := WithoutRuntimeMarkers([]string{envBootAddress + "=127.0.0.1:1", envBootToken + "=secret", EnvBootManaged + "=1", EnvProbeProtocol + "=2", "KEEP=yes"})
	if len(got) != 1 || got[0] != "KEEP=yes" {
		t.Fatal(got)
	}
}

func BenchmarkSafeBootRead(b *testing.B) {
	root := b.TempDir()
	if err := writeBoot(root, BootState{Schema: BootProtocol, Generation: 1, Good: "v1.0.0"}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ReadBoot(root); err != nil {
			b.Fatal(err)
		}
	}
}

func FuzzSafeBootRecord(f *testing.F) {
	f.Add([]byte(`{"schema":1,"generation":1,"good":"v1.0.0"}`))
	f.Add([]byte(`{"schema":1,"generation":1,"good":"../../bin/sh"}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 8193 {
			return
		}
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, BootFile), raw, 0600); err != nil {
			t.Fatal(err)
		}
		if s, err := ReadBoot(root); err == nil {
			if err := validateBoot(s); err != nil {
				t.Fatal(err)
			}
			if filepath.Dir(VersionPath(root, s.Good, runtime.GOOS)) != filepath.Join(root, "versions", s.Good) {
				t.Fatal("escaped root")
			}
		}
	})
}

type failingStateFile struct {
	stateFile
	phase string
}

func (f failingStateFile) Write(b []byte) (int, error) {
	if f.phase == "short-write" {
		return len(b) - 1, nil
	}
	if f.phase == "write" {
		return 0, errors.New("write failed")
	}
	return f.stateFile.Write(b)
}
func (f failingStateFile) Sync() error {
	if f.phase == "sync" {
		return errors.New("fsync failed")
	}
	return f.stateFile.Sync()
}
func (f failingStateFile) Close() error {
	err := f.stateFile.Close()
	if f.phase == "close" {
		return errors.New("close failed")
	}
	return err
}

func TestSafeBootEveryCommitFailurePreservesLaunchableDefault(t *testing.T) {
	for _, phase := range []string{"create", "write", "short-write", "sync", "close", "replace", "dir-sync"} {
		t.Run(phase, func(t *testing.T) {
			c, s := stageBootFixture(t)
			create, commit, syncDirectory := createStateFile, commitBootFile, syncBootDir
			t.Cleanup(func() { createStateFile, commitBootFile, syncBootDir = create, commit, syncDirectory })
			createStateFile = func(root, pattern string) (stateFile, error) {
				if phase == "create" {
					return nil, errors.New("create failed")
				}
				f, err := create(root, pattern)
				return failingStateFile{f, phase}, err
			}
			if phase == "replace" {
				commitBootFile = func(string, string) error { return errors.New("replace failed") }
			}
			if phase == "dir-sync" {
				syncBootDir = func(string) error { return errors.New("directory sync failed") }
			}
			if _, ok := consumeBootCandidate(c.Root, s); ok {
				t.Fatal("executed after failed durable commit")
			}
			after, err := ReadBoot(c.Root)
			if err != nil || after.Good != "v1.0.0" {
				t.Fatalf("%+v %v", after, err)
			}
			if _, err := checkedRuntime(c.Root, after.Good); err != nil {
				t.Fatal(err)
			}
			// Simulates a new invocation, requiring no lock/repair write.
			if good, _, err := BootSnapshot(c.Root); err != nil || good != "v1.0.0" {
				t.Fatalf("%s %v", good, err)
			}
		})
	}
}

func TestSafeBootMissingOrTamperedCandidateIsNeverRetried(t *testing.T) {
	for _, mode := range []string{"missing", "changed"} {
		t.Run(mode, func(t *testing.T) {
			c, s := stageBootFixture(t)
			p := VersionPath(c.Root, "v2.0.0", runtime.GOOS)
			if mode == "missing" {
				if err := os.Remove(p); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(p, []byte("different bytes"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			if _, ok := consumeBootCandidate(c.Root, s); ok {
				t.Fatal("executed invalid candidate")
			}
			after, err := ReadBoot(c.Root)
			if err != nil || after.Good != "v1.0.0" || after.Candidate.Attempt == "" {
				t.Fatalf("%+v %v", after, err)
			}
			if _, ok := consumeBootCandidate(c.Root, after); ok {
				t.Fatal("retried invalid candidate")
			}
		})
	}
}

func TestSafeBootLegacyDownloadCannotOverwriteConcurrentBootRecord(t *testing.T) {
	c, source, digest := bootFixture(t)
	if err := StageBootCandidate(context.Background(), c, "v1.0.0", 1, source, "v2.0.0", digest); err != nil {
		t.Fatal(err)
	}
	if err := StageBootCandidate(context.Background(), c, "v1.0.0", 1, source, "v3.0.0", digest); err == nil {
		t.Fatal("stale legacy generation accepted")
	}
}

func TestSafeBootRetentionLimitDoesNotDeleteRunningVersions(t *testing.T) {
	c, source, digest := bootFixture(t)
	for i := 2; i <= 8; i++ {
		if _, err := InstallVersion(c.Root, source, fmt.Sprintf("v%d.0.0", i), runtime.GOOS, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := StageBootCandidate(context.Background(), c, "v1.0.0", 1, source, "v9.0.0", digest); err == nil {
		t.Fatal("unbounded retained versions")
	}
	if baseline, err := ReadBoot(c.Root); err != nil || baseline.Good != "v1.0.0" || baseline.Candidate != nil || baseline.Generation != 1 {
		t.Fatal("changed boot selection", err)
	}
	for i := 1; i <= 8; i++ {
		if _, err := checkedRuntime(c.Root, fmt.Sprintf("v%d.0.0", i)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSafeBootCorruptRecordFailsClosedWithoutRepair(t *testing.T) {
	c, _ := stageBootFixture(t)
	if err := os.WriteFile(c.EntryPath, []byte("stable base"), 0755); err != nil {
		t.Fatal(err)
	}
	raw := []byte("{broken")
	if err := os.WriteFile(filepath.Join(c.Root, BootFile), raw, 0600); err != nil {
		t.Fatal(err)
	}
	old := executablePath
	t.Cleanup(func() { executablePath = old })
	executablePath = func() (string, error) { return c.EntryPath, nil }
	t.Setenv(EnvDisable, "")
	t.Setenv(EnvRuntime, "")
	t.Setenv(EnvForce, "1")
	if code, handled, err := Launch("v1.0.0", []string{c.EntryPath, "tui"}); !errors.Is(err, ErrBootstrapState) || handled || code != 0 {
		t.Fatalf("%d %v %v", code, handled, err)
	}
	if after, _ := os.ReadFile(filepath.Join(c.Root, BootFile)); string(after) != string(raw) {
		t.Fatal("startup attempted repair")
	}
}

func TestSafeBootInvocationAndOptOut(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{nil, true}, {[]string{"work-profile"}, true}, {[]string{"tui", "--profile", "work"}, true}, {[]string{"--config=a", "history", "tui"}, true},
		{[]string{"history"}, false}, {[]string{"--version"}, false}, {[]string{"tui", "--help"}, false}, {[]string{"teams", "run"}, false}, {[]string{"--upgrade-codex"}, false}, {[]string{"--unknown"}, false}, {[]string{"__complete", "tui"}, false},
	} {
		if got := historyTUIArgs(tc.args); got != tc.want {
			t.Fatalf("%v: %v", tc.args, got)
		}
	}
	for _, setting := range []string{"0", "off", "FALSE"} {
		t.Setenv("CXP_AUTO_UPDATE", setting)
		if AutoUpdateAllowed() {
			t.Fatal(setting)
		}
	}
	t.Setenv("CXP_AUTO_UPDATE", "1")
	t.Setenv("CODEX_PROXY_VERSION", "v1.0.0")
	if AutoUpdateAllowed() {
		t.Fatal("pin ignored")
	}
	t.Setenv("CODEX_PROXY_VERSION", "latest")
	if !AutoUpdateAllowed() {
		t.Fatal("default disabled")
	}
}

func TestSafeBootCannotPublishThroughEscapingSymlink(t *testing.T) {
	c, source, digest := bootFixture(t)
	outside := t.TempDir()
	path := filepath.Join(outside, BinaryName(runtime.GOOS))
	if err := os.WriteFile(path, []byte("preserved outside entry"), 0755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(c.Root, "versions", "v2.0.0")
	if err := os.Symlink(outside, link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink capability: %v", err)
		}
		t.Fatal(err)
	}
	if err := StageBootCandidate(context.Background(), c, "v1.0.0", 1, source, "v2.0.0", digest); err == nil {
		t.Fatal("published outside managed root")
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "preserved outside entry" {
		t.Fatal("outside entry changed")
	}
	if baseline, err := ReadBoot(c.Root); err != nil || baseline.Good != "v1.0.0" || baseline.Candidate != nil || baseline.Generation != 1 {
		t.Fatal("published unsafe candidate", err)
	}
}
