package helperruntime

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLegacyUpgradeReceiptKeepsHistoricalReadbackUntilAdmission(t *testing.T) {
	root := t.TempDir()
	legacy := []byte(`{"schema":1,"generation":7,"good":"v1.0.0","previous":"v0.9.0"}`)
	if err := os.WriteFile(filepath.Join(root, "boot.json"), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	old, err := LegacyUpgradeSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = WriteUpgradeReceipt(root, old, "v2.0.0"); err != nil {
		t.Fatal(err)
	}
	got, err := LegacyUpgradeSnapshot(root)
	if err != nil || got.Schema != 1 || got.Good != "v2.0.0" || got.Previous != "" || got.Candidate != nil || got.Generation != 8 {
		t.Fatalf("%+v %v", got, err)
	}
	if !HasUpgradeForwarder(root, "v2.0.0") {
		t.Fatal("old dispatcher cannot reach verified runtime")
	}
	if _, err := os.Stat(filepath.Join(root, BootFile)); !os.IsNotExist(err) {
		t.Fatal("admitted before old caller's last writes")
	}
	if err := RemoveUpgradeForwarder(root, "v1.0.0"); err == nil {
		t.Fatal("removed another forwarder")
	}
	if err := RemoveUpgradeForwarder(root, "v2.0.0"); err != nil {
		t.Fatal(err)
	}
	if err := RemoveUpgradeForwarder(root, "v2.0.0"); err != nil {
		t.Fatal("cleanup retry", err)
	}
}

func TestLegacyUpgradeReceiptExhaustionWritesNothing(t *testing.T) {
	root := t.TempDir()
	if err := WriteUpgradeReceipt(root, BootState{Generation: ^uint64(0)}, "v2.0.0"); err == nil {
		t.Fatal("generation overflow")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
}

func TestLegacyUpgradeAdmissionCannotReplaceNewerNativeSelection(t *testing.T) {
	c, source, _ := bootFixture(t)
	before, err := os.ReadFile(filepath.Join(c.Root, BootFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := AdmitUpgradeBootstrap(context.Background(), c.Root, source, "v2.0.0"); err == nil {
		t.Fatal("stale migration changed native Good")
	}
	after, err := os.ReadFile(filepath.Join(c.Root, BootFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("native selection changed", err)
	}
	if _, err := LegacyUpgradeSnapshot(c.Root); err == nil {
		t.Fatal("native bootstrap entered old updater")
	}
	if err := os.Remove(filepath.Join(c.Root, BootFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := LegacyUpgradeSnapshot(c.Root); err == nil {
		t.Fatal("lost published bootstrap admitted legacy")
	}
	if err := AdmitUpgradeBootstrap(context.Background(), c.Root, source, "v2.0.0"); err == nil {
		t.Fatal("recreated lost publication")
	}
}

func TestLegacyUpgradeForwarderUsesNativeSelectionAndEnvironment(t *testing.T) {
	c, _, _ := bootFixture(t)
	old := launchRuntimeFn
	t.Cleanup(func() { launchRuntimeFn = old })
	called := false
	launchRuntimeFn = func(path string, args, env []string) (int, bool, error) {
		called = true
		if path != c.RuntimePath || len(args) != 2 || args[1] != "--version" {
			t.Fatal(path, args)
		}
		found := false
		for _, e := range env {
			found = found || e == EnvRuntimeVersion+"=v1.0.0"
		}
		if !found {
			t.Fatal("missing pinned native version")
		}
		return 7, true, nil
	}
	if code, err := ForwardUpgradeInvocation(c.Root, c.EntryPath, "v1.0.0", []string{"old", "--version"}); err != nil || code != 7 || !called {
		t.Fatal(code, err, called)
	}
	called = false
	if _, err := ForwardUpgradeInvocation(c.Root, c.EntryPath, "v2.0.0", []string{"old", "--version"}); err == nil || called {
		t.Fatal("stale forwarder ran", err)
	}
}

func TestLegacyUpgradeAdmissionRetryPreservesNativeTrial(t *testing.T) {
	c, before := stageBootFixture(t)
	if err := AdmitUpgradeBootstrap(context.Background(), c.Root, c.RuntimePath, c.Version); err != nil {
		t.Fatal(err)
	}
	after, err := ReadBoot(c.Root)
	if err != nil || after.Generation != before.Generation || after.Good != before.Good || after.Candidate == nil || *after.Candidate != *before.Candidate {
		t.Fatal("migration retry changed native trial", before, after, err)
	}
}
