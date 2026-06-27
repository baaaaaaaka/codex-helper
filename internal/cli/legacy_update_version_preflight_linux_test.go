//go:build linux

package cli

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
)

type legacyPreflightFixture struct {
	root          string
	home          string
	physicalLocal string
	target        string
	temporary     string
	defaultTarget string
	shim          string
	recordPath    string
	opts          legacyUpdateVersionPreflightOptions
}

func newLegacyPreflightFixture(t *testing.T, layout string) legacyPreflightFixture {
	t.Helper()
	root := t.TempDir()
	home := filepath.Join(root, "home")
	physicalLocal := filepath.Join(root, "overflow", "local")
	physicalBin := filepath.Join(physicalLocal, "bin")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	switch layout {
	case "local-symlink":
		if err := os.MkdirAll(physicalBin, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(physicalLocal, filepath.Join(home, ".local")); err != nil {
			t.Fatal(err)
		}
	case "local-relative-symlink":
		if err := os.MkdirAll(physicalBin, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..", "overflow", "local"), filepath.Join(home, ".local")); err != nil {
			t.Fatal(err)
		}
	case "local-symlink-chain":
		if err := os.MkdirAll(physicalBin, 0o755); err != nil {
			t.Fatal(err)
		}
		firstHop := filepath.Join(root, "local-hop")
		if err := os.Symlink(physicalLocal, firstHop); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..", filepath.Base(firstHop)), filepath.Join(home, ".local")); err != nil {
			t.Fatal(err)
		}
	case "bin-symlink":
		if err := os.MkdirAll(filepath.Join(home, ".local"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(physicalBin, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(physicalBin, filepath.Join(home, ".local", "bin")); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown layout %q", layout)
	}
	target := filepath.Join(physicalBin, "codex-proxy")
	temporary := filepath.Join(physicalBin, ".codex-proxy_0.1.13-rc.17_linux_"+runtime.GOARCH+".123456")
	writeExecutableForLegacyPreflightTest(t, target, "legacy-helper")
	writeExecutableForLegacyPreflightTest(t, temporary, "downloaded-helper")
	defaultTarget := filepath.Join(home, ".local", "bin", "codex-proxy")
	recordPath := filepath.Join(home, ".config", "codex-helper", "install.json")
	return legacyPreflightFixture{
		root:          root,
		home:          home,
		physicalLocal: physicalLocal,
		target:        target,
		temporary:     temporary,
		defaultTarget: defaultTarget,
		shim:          filepath.Join(physicalBin, "cxp"),
		recordPath:    recordPath,
		opts: legacyUpdateVersionPreflightOptions{
			args:             []string{temporary, "--version"},
			executable:       temporary,
			parentExecutable: target,
			defaultTarget:    defaultTarget,
			recordPath:       recordPath,
			inspectParent: func(string, string) (bool, error) {
				return true, nil
			},
		},
	}
}

func writeExecutableForLegacyPreflightTest(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyUpdaterVersionPreflightPreparesSymlinkedLocalLayouts(t *testing.T) {
	for _, layout := range []string{"local-symlink", "local-relative-symlink", "local-symlink-chain", "bin-symlink"} {
		t.Run(layout, func(t *testing.T) {
			fixture := newLegacyPreflightFixture(t, layout)
			if err := managedinstall.SaveRecord(fixture.recordPath, managedinstall.Record{
				TargetPath: fixture.target,
				Version:    "0.1.12",
			}); err != nil {
				t.Fatal(err)
			}

			if err := legacyUpdaterVersionPreflightWithOptions(fixture.opts); err != nil {
				t.Fatalf("preflight failed: %v", err)
			}

			targetInfo, err := os.Lstat(fixture.target)
			if err != nil {
				t.Fatal(err)
			}
			if !targetInfo.Mode().IsRegular() {
				t.Fatalf("target mode = %s, want regular file", targetInfo.Mode())
			}
			shimTarget, err := os.Readlink(fixture.shim)
			if err != nil {
				t.Fatalf("read cxp guard: %v", err)
			}
			if shimTarget != "codex-proxy" {
				t.Fatalf("cxp target = %q, want relative codex-proxy", shimTarget)
			}
			record, err := managedinstall.LoadRecord(fixture.recordPath)
			if err != nil {
				t.Fatal(err)
			}
			if record.TargetPath != fixture.target {
				t.Fatalf("record target = %q, want %q", record.TargetPath, fixture.target)
			}
			if len(record.Shims) == 0 || record.Shims[0] != fixture.shim {
				t.Fatalf("record shims = %#v, want guard %q first", record.Shims, fixture.shim)
			}
			if err := verifyLegacyUpdaterAliasPlan(fixture.target, fixture.opts, record); err != nil {
				t.Fatalf("saved alias plan is unsafe: %v", err)
			}
		})
	}
}

func TestLegacyUpdaterVersionPreflightOrdinaryVersionIsReadOnly(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	record := managedinstall.Record{TargetPath: fixture.target, Version: "0.1.12"}
	if err := managedinstall.SaveRecord(fixture.recordPath, record); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(fixture.recordPath)
	if err != nil {
		t.Fatal(err)
	}
	oldTime := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(fixture.recordPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	opts := fixture.opts
	opts.args = []string{fixture.target, "--version"}
	opts.executable = fixture.target
	if err := legacyUpdaterVersionPreflightWithOptions(opts); err != nil {
		t.Fatalf("ordinary version preflight: %v", err)
	}
	after, err := os.ReadFile(fixture.recordPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("ordinary --version changed install record\nbefore: %s\nafter: %s", before, after)
	}
	info, err := os.Stat(fixture.recordPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(oldTime) {
		t.Fatalf("ordinary --version changed record mtime: %s", info.ModTime())
	}
	if _, err := os.Lstat(fixture.shim); !os.IsNotExist(err) {
		t.Fatalf("ordinary --version created cxp shim, err=%v", err)
	}
}

func TestLegacyUpdaterVersionPreflightRejectsDangerousEnvironmentAlias(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	opts := fixture.opts
	opts.envInstallPath = fixture.defaultTarget
	err := legacyUpdaterVersionPreflightWithOptions(opts)
	if err == nil || !strings.Contains(err.Error(), "unsafe environment alias") {
		t.Fatalf("error = %v, want unsafe environment alias", err)
	}
	if _, statErr := os.Lstat(fixture.shim); !os.IsNotExist(statErr) {
		t.Fatalf("rejected preflight created cxp shim, err=%v", statErr)
	}
	if _, statErr := os.Stat(fixture.recordPath); !os.IsNotExist(statErr) {
		t.Fatalf("rejected preflight created install record, err=%v", statErr)
	}
}

func TestLegacyUpdaterVersionPreflightRejectsDangerousEnvironmentDirectory(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	opts := fixture.opts
	opts.envInstallDir = filepath.Dir(fixture.defaultTarget)
	err := legacyUpdaterVersionPreflightWithOptions(opts)
	if err == nil || !strings.Contains(err.Error(), "unsafe environment alias") {
		t.Fatalf("error = %v, want unsafe environment alias", err)
	}
}

func TestLegacyUpdaterVersionPreflightAllowsExactPhysicalEnvironmentTarget(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	opts := fixture.opts
	opts.envInstallPath = fixture.target
	if err := legacyUpdaterVersionPreflightWithOptions(opts); err != nil {
		t.Fatalf("physical environment target preflight: %v", err)
	}
	if _, err := os.Readlink(fixture.shim); err != nil {
		t.Fatalf("physical environment target did not prepare guard: %v", err)
	}
}

func TestLegacyUpdaterVersionPreflightRequiresVerifiedParent(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	opts := fixture.opts
	opts.inspectParent = func(string, string) (bool, error) { return false, errors.New("not the updater") }
	err := legacyUpdaterVersionPreflightWithOptions(opts)
	if err == nil || !strings.Contains(err.Error(), "not the updater") {
		t.Fatalf("error = %v, want parent verification failure", err)
	}
	if _, statErr := os.Lstat(fixture.shim); !os.IsNotExist(statErr) {
		t.Fatalf("unverified preflight created cxp shim, err=%v", statErr)
	}
}

func TestLegacyUpdaterVersionPreflightSkipsKnownSafeParent(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	opts := fixture.opts
	opts.inspectParent = func(string, string) (bool, error) { return false, nil }
	if err := legacyUpdaterVersionPreflightWithOptions(opts); err != nil {
		t.Fatalf("safe parent preflight: %v", err)
	}
	if _, err := os.Lstat(fixture.shim); !os.IsNotExist(err) {
		t.Fatalf("safe parent created cxp shim, err=%v", err)
	}
	if _, err := os.Stat(fixture.recordPath); !os.IsNotExist(err) {
		t.Fatalf("safe parent created install record, err=%v", err)
	}
}

func TestLegacyUpdaterVersionNeedsBridge(t *testing.T) {
	tests := []struct {
		version string
		want    bool
		known   bool
	}{
		{version: "v0.1.10-rc.23", known: true},
		{version: "v0.1.10-rc.24", want: true, known: true},
		{version: "v0.1.10", want: true, known: true},
		{version: "v0.1.11", want: true, known: true},
		{version: "v0.1.12", want: true, known: true},
		{version: "v0.1.13-rc.6", want: true, known: true},
		{version: "v0.1.13-rc.7", known: true},
		{version: "v0.1.13-rc.15", known: true},
		{version: "v0.1.13-rc.16", known: true},
		{version: "v0.1.13", known: true},
		{version: "(devel)"},
	}
	for _, tc := range tests {
		t.Run(tc.version, func(t *testing.T) {
			got, known := legacyUpdaterVersionNeedsBridge(tc.version)
			if got != tc.want || known != tc.known {
				t.Fatalf("legacyUpdaterVersionNeedsBridge(%q) = (%v, %v), want (%v, %v)", tc.version, got, known, tc.want, tc.known)
			}
		})
	}
}

func TestLegacyUpdaterInspectParentRejectsUnknownBinary(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	if _, err := inspectLegacyUpdaterParent(fixture.target, fixture.target); err == nil || !strings.Contains(err.Error(), "build info") {
		t.Fatalf("error = %v, want missing build info", err)
	}
}

func TestLegacyUpdaterVersionPreflightRewritesLogicalRecordAndPoisonedShims(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	logicalShim := filepath.Join(filepath.Dir(fixture.defaultTarget), "cxp")
	if err := os.Symlink("codex-proxy", fixture.shim); err != nil {
		t.Fatal(err)
	}
	if err := managedinstall.SaveRecord(fixture.recordPath, managedinstall.Record{
		TargetPath: fixture.defaultTarget,
		Version:    "0.1.12",
		Shims:      []string{fixture.defaultTarget, logicalShim},
	}); err != nil {
		t.Fatal(err)
	}
	if err := legacyUpdaterVersionPreflightWithOptions(fixture.opts); err != nil {
		t.Fatalf("preflight failed: %v", err)
	}
	record, err := managedinstall.LoadRecord(fixture.recordPath)
	if err != nil {
		t.Fatal(err)
	}
	if record.TargetPath != fixture.target {
		t.Fatalf("record target = %q, want physical %q", record.TargetPath, fixture.target)
	}
	if len(record.Shims) == 0 || record.Shims[0] != fixture.shim {
		t.Fatalf("record shims = %#v, want physical guard first", record.Shims)
	}
	if err := verifyLegacyUpdaterAliasPlan(fixture.target, fixture.opts, record); err != nil {
		t.Fatalf("rewritten record remains unsafe: %v", err)
	}
}

func TestLegacyUpdaterVersionPreflightRejectsUserOwnedCXP(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	writeExecutableForLegacyPreflightTest(t, fixture.shim, "not-the-helper")
	err := legacyUpdaterVersionPreflightWithOptions(fixture.opts)
	if err == nil || !strings.Contains(err.Error(), "user-owned regular file") {
		t.Fatalf("error = %v, want user-owned cxp rejection", err)
	}
	contents, readErr := os.ReadFile(fixture.shim)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(contents) != "not-the-helper" {
		t.Fatalf("user-owned cxp was changed: %q", contents)
	}
}

func TestLegacyUpdaterVersionPreflightRejectsUserOwnedCXPSymlinkWithHelperBasename(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	userTarget := filepath.Join(fixture.root, "user-bin", "codex-proxy")
	writeExecutableForLegacyPreflightTest(t, userTarget, "not-the-helper")
	if err := os.Symlink(userTarget, fixture.shim); err != nil {
		t.Fatal(err)
	}

	err := legacyUpdaterVersionPreflightWithOptions(fixture.opts)
	if err == nil || !strings.Contains(err.Error(), "user-owned or unverifiable symlink") {
		t.Fatalf("error = %v, want user-owned symlink rejection", err)
	}
	got, readErr := os.Readlink(fixture.shim)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if got != userTarget {
		t.Fatalf("user-owned cxp symlink changed from %q to %q", userTarget, got)
	}
}

func TestLegacyUpdaterVersionPreflightConvertsIdenticalCXPBinary(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	contents, err := os.ReadFile(fixture.target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.shim, contents, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := legacyUpdaterVersionPreflightWithOptions(fixture.opts); err != nil {
		t.Fatalf("preflight failed: %v", err)
	}
	if info, err := os.Lstat(fixture.shim); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("cxp was not converted to a symlink: info=%v err=%v", info, err)
	}
}

func TestLegacyUpdaterVersionPreflightIsIdempotent(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	if err := legacyUpdaterVersionPreflightWithOptions(fixture.opts); err != nil {
		t.Fatal(err)
	}
	firstBytes, err := os.ReadFile(fixture.recordPath)
	if err != nil {
		t.Fatal(err)
	}
	oldTime := time.Unix(1_700_000_000, 0)
	if err := os.Chtimes(fixture.recordPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	firstRecord, err := managedinstall.LoadRecord(fixture.recordPath)
	if err != nil {
		t.Fatal(err)
	}
	firstShim, err := os.Readlink(fixture.shim)
	if err != nil {
		t.Fatal(err)
	}
	retryOpts := fixture.opts
	retryOpts.inspectParent = func(string, string) (bool, error) {
		return false, errors.New("safe retry should not inspect parent")
	}
	if err := legacyUpdaterVersionPreflightWithOptions(retryOpts); err != nil {
		t.Fatal(err)
	}
	secondRecord, err := managedinstall.LoadRecord(fixture.recordPath)
	if err != nil {
		t.Fatal(err)
	}
	secondShim, err := os.Readlink(fixture.shim)
	if err != nil {
		t.Fatal(err)
	}
	if firstRecord.TargetPath != secondRecord.TargetPath || strings.Join(firstRecord.Shims, "\x00") != strings.Join(secondRecord.Shims, "\x00") {
		t.Fatalf("record changed across retries: first=%#v second=%#v", firstRecord, secondRecord)
	}
	if firstShim != secondShim {
		t.Fatalf("shim changed across retries: %q -> %q", firstShim, secondShim)
	}
	secondBytes, err := os.ReadFile(fixture.recordPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstBytes) != string(secondBytes) {
		t.Fatalf("record contents changed across retries\nfirst: %s\nsecond: %s", firstBytes, secondBytes)
	}
	info, err := os.Stat(fixture.recordPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(oldTime) {
		t.Fatalf("record mtime changed across retry: %s", info.ModTime())
	}
}

func TestLegacyUpdaterVersionPreflightDoesNotTreatHardlinkAsSameDirectoryEntry(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	otherDefault := filepath.Join(fixture.root, "other-home", ".local", "bin", "codex-proxy")
	if err := os.MkdirAll(filepath.Dir(otherDefault), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(fixture.target, otherDefault); err != nil {
		t.Fatal(err)
	}
	opts := fixture.opts
	opts.defaultTarget = otherDefault
	if err := legacyUpdaterVersionPreflightWithOptions(opts); err != nil {
		t.Fatalf("hardlink preflight: %v", err)
	}
	if _, err := os.Lstat(fixture.shim); !os.IsNotExist(err) {
		t.Fatalf("hardlink-only topology created cxp shim, err=%v", err)
	}
}

func TestLegacyUpdaterVersionPreflightInvalidRecordFailsBeforeMutation(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	if err := os.MkdirAll(filepath.Dir(fixture.recordPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.recordPath, []byte("{invalid"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := legacyUpdaterVersionPreflightWithOptions(fixture.opts)
	if err == nil || !strings.Contains(err.Error(), "load managed install record") {
		t.Fatalf("error = %v, want invalid record rejection", err)
	}
	if _, statErr := os.Lstat(fixture.shim); !os.IsNotExist(statErr) {
		t.Fatalf("invalid record preflight changed cxp, err=%v", statErr)
	}
}

func TestLegacyUpdaterVersionPreflightRecordSaveFailureLeavesRunnableGuard(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	if err := managedinstall.SaveRecord(fixture.recordPath, managedinstall.Record{
		TargetPath: fixture.defaultTarget,
		Version:    "0.1.12",
	}); err != nil {
		t.Fatal(err)
	}
	recordDir := filepath.Dir(fixture.recordPath)
	if err := os.Chmod(recordDir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(recordDir, 0o700) })
	err := legacyUpdaterVersionPreflightWithOptions(fixture.opts)
	if err == nil || !strings.Contains(err.Error(), "save managed install record guard") {
		t.Fatalf("error = %v, want record save failure", err)
	}
	shimTarget, readErr := os.Readlink(fixture.shim)
	if readErr != nil {
		t.Fatalf("partial failure did not leave a runnable cxp guard: %v", readErr)
	}
	if shimTarget != "codex-proxy" {
		t.Fatalf("partial failure cxp target = %q", shimTarget)
	}
	if info, statErr := os.Lstat(fixture.target); statErr != nil || !info.Mode().IsRegular() {
		t.Fatalf("partial failure changed stable target: info=%v err=%v", info, statErr)
	}
}

func TestLegacyUpdaterTempTargetRejectsMalformedOrNonExecutableCandidates(t *testing.T) {
	fixture := newLegacyPreflightFixture(t, "local-symlink")
	malformed := filepath.Join(filepath.Dir(fixture.temporary), ".codex-proxy_bad-name")
	writeExecutableForLegacyPreflightTest(t, malformed, "downloaded-helper")
	if _, candidate, err := legacyUpdaterTempTarget(malformed); !candidate || err == nil {
		t.Fatalf("malformed candidate = %v err=%v, want candidate error", candidate, err)
	}
	if err := os.Chmod(fixture.temporary, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, candidate, err := legacyUpdaterTempTarget(fixture.temporary); !candidate || err == nil {
		t.Fatalf("non-executable candidate = %v err=%v, want candidate error", candidate, err)
	}
}
