package candidateupdate

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
)

func TestHistoricalVersionProbeIsSuccessfulAndInert(t *testing.T) {
	t.Setenv(helperruntime.EnvDisable, "1")
	before := candidateExecutablePath
	t.Cleanup(func() { candidateExecutablePath = before })
	candidateExecutablePath = func() (string, error) { t.Fatal("version probe inspected installation"); return "", nil }
	for _, marker := range []string{"", "1", "2"} {
		t.Setenv(helperruntime.EnvProbeProtocol, marker)
		var out, err bytes.Buffer
		code, handled := HandleLegacyMigration([]string{".codex-proxy_1.2.3", "--version"}, "v1.2.3", &out, &err)
		if !handled || code != 0 || !strings.Contains(out.String(), "1.2.3") || err.Len() != 0 {
			t.Fatalf("%d %v %q %q", code, handled, out.String(), err.String())
		}
	}
}

func TestLegacyUpdateIdentityFailureDoesNotCreateState(t *testing.T) {
	dir := t.TempDir()
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	root := filepath.Join(dir, ".cxp-runtime")
	source := filepath.Join(dir, "candidate")
	if err := os.WriteFile(source, []byte("candidate bytes"), 0700); err != nil {
		t.Fatal(err)
	}
	hash, err := fileSHA256(source)
	if err != nil {
		t.Fatal(err)
	}
	before := candidateExecutablePath
	t.Cleanup(func() { candidateExecutablePath = before })
	candidateExecutablePath = func() (string, error) { return source, nil }
	r := Context{Schema: 1, CandidatePath: source, CandidateSHA256: hash, TargetVersion: "v2.0.0", SourceVersion: "v1.0.0", EntryPath: entry, RuntimeRoot: root, RequestID: "old"}
	for _, kind := range []string{"hash", "path", "build", "root"} {
		t.Run(kind, func(t *testing.T) {
			x := r
			version := "v2.0.0"
			switch kind {
			case "hash":
				x.CandidateSHA256 = strings.Repeat("0", 64)
			case "path":
				x.CandidatePath = filepath.Join(dir, "other")
			case "build":
				version = "v3.0.0"
			case "root":
				x.RuntimeRoot = dir
			}
			if _, err := applyLegacyUpgrade(context.Background(), x, version); err == nil {
				t.Fatal("identity accepted")
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatal("identity failure wrote state", err)
			}
		})
	}
}

func TestLegacyMigrationCannotMaskNativeCorruption(t *testing.T) {
	dir := t.TempDir()
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	root := filepath.Join(dir, ".cxp-runtime")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	source := helperruntime.VersionPath(root, "v2.0.0", runtime.GOOS)
	if err := os.MkdirAll(filepath.Dir(source), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("verified immutable"), 0700); err != nil {
		t.Fatal(err)
	}
	hash, _ := fileSHA256(source)
	s, err := updatestate.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	err = s.SavePending(updatestate.Pending{Schema: 1, TargetVersion: "v2.0.0", TargetSHA256: hash, TargetActive: source, EntryPath: entry, RuntimeRoot: root, RequestID: legacyUpgradePrefix + "old"})
	s.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "active"), []byte("v2.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvEntryPath, entry)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v2.0.0")
	t.Setenv(helperruntime.EnvDisable, "")
	for _, name := range []string{helperruntime.BootFile, helperruntime.BootFile + ".published"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(root, name)
			if err := os.WriteFile(path, []byte("corrupt publication"), 0600); err != nil {
				t.Fatal(err)
			}
			defer os.Remove(path)
			var out, stderr bytes.Buffer
			code, handled := HandleLegacyMigration([]string{entry, "--version"}, "v2.0.0", &out, &stderr)
			if !handled || code != 1 || out.Len() != 0 {
				t.Fatal(code, handled, out.String(), stderr.String())
			}
		})
	}
}

func TestLegacyMigrationNativeStartupDoesNotOpenUpdaterState(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	entry := filepath.Join(dir, "cxp")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	// Opening this database would fail. A normal native start must not open
	// updater state or hash/copy binaries merely to check for legacy migration.
	if err := os.WriteFile(filepath.Join(root, updatestate.Filename), []byte("unreadable updater payload"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvEntryPath, entry)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v2.0.0")
	t.Setenv(helperruntime.EnvBootManaged, "1")
	var out, stderr bytes.Buffer
	if _, handled := HandleLegacyMigration([]string{entry, "proxy", "list"}, "v2.0.0", &out, &stderr); handled {
		t.Fatal(stderr.String())
	}
}

func BenchmarkNativeMigrationCheck(b *testing.B) {
	for _, entered := range []bool{false, true} {
		b.Run(map[bool]string{false: "physical", true: "runtime"}[entered], func(b *testing.B) {
			dir := b.TempDir()
			root := filepath.Join(dir, ".cxp-runtime")
			entry := filepath.Join(dir, "cxp")
			if err := os.Mkdir(root, 0700); err != nil {
				b.Fatal(err)
			}
			before := candidateExecutablePath
			b.Cleanup(func() { candidateExecutablePath = before })
			candidateExecutablePath = func() (string, error) { return entry, nil }
			b.Setenv(helperruntime.EnvRuntime, "")
			b.Setenv(helperruntime.EnvBootManaged, "")
			if entered {
				b.Setenv(helperruntime.EnvRuntime, "1")
				b.Setenv(helperruntime.EnvRuntimeRoot, root)
				b.Setenv(helperruntime.EnvEntryPath, entry)
				b.Setenv(helperruntime.EnvRuntimeVersion, "v2.0.0")
				b.Setenv(helperruntime.EnvBootManaged, "1")
			}
			args := []string{entry, "proxy", "list"}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, handled := HandleLegacyMigration(args, "v2.0.0", io.Discard, io.Discard); handled {
					b.Fatal("native steady startup entered migration")
				}
			}
		})
	}
}

func TestLegacyInvocationIsPrivateBoundedAndExact(t *testing.T) {
	for _, tc := range []struct {
		name, content string
		mode          os.FileMode
		valid         bool
	}{
		{"valid", `{"schema":1,"request_id":"old"}`, 0600, true},
		{"trailing", `{"schema":1} {}`, 0600, false},
		{"unknown", `{"schema":1,"unexpected":true}`, 0600, false},
		{"large", strings.Repeat(" ", 65537), 0600, false},
		{"public", `{"schema":1}`, 0644, runtime.GOOS == "windows"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "request.json")
			if err := os.WriteFile(path, []byte(tc.content), tc.mode); err != nil {
				t.Fatal(err)
			}
			if _, err := readLegacyInvocation(path); (err == nil) != tc.valid {
				t.Fatal(err)
			}
		})
	}
	if _, err := readLegacyInvocation(t.TempDir()); err == nil {
		t.Fatal("directory accepted")
	}
	if runtime.GOOS != "windows" {
		path := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink("missing", path); err != nil {
			t.Fatal(err)
		}
		if _, err := readLegacyInvocation(path); err == nil {
			t.Fatal("symlink accepted")
		}
	}
}

func TestLegacyProtocolDoesNotAcceptAmbiguousFlags(t *testing.T) {
	for _, flags := range [][]string{{"--protocol=1"}, {"--context-file=x", "--protocol=1"}, {"--protocol=1", "--context-file=x", "--protocol=2"}} {
		var out, err bytes.Buffer
		code, handled := HandleInternalCommand(append([]string{"candidate", InternalCommand}, flags...), "v1.2.3", &out, &err)
		if !handled || code != 2 || out.Len() != 0 {
			t.Fatalf("%v: %d %v", flags, code, handled)
		}
	}
}

func TestLegacyMigrationLostUpdaterPublicationFailsClosed(t *testing.T) {
	root := t.TempDir()
	s, err := updatestate.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err = os.Remove(filepath.Join(root, updatestate.Filename)); err != nil {
		t.Fatal(err)
	}
	if _, err = migrationPending(root); err == nil || os.IsNotExist(err) {
		t.Fatalf("lost publication treated as fresh migration: %v", err)
	}
}

func TestLegacyTrialRequiresClaimedValidBootstrap(t *testing.T) {
	for _, legacy := range []string{`broken`, `{"schema":1,"generation":1,"good":"v1.0.0"}`, `{"schema":1,"generation":1,"good":"v1.0.0","candidate":{"version":"v2.0.0","sha256":"` + strings.Repeat("a", 64) + `"}}`} {
		t.Run(legacy[:6], func(t *testing.T) {
			root := t.TempDir()
			raw := helperruntime.VersionPath(root, "v2.0.0", runtime.GOOS)
			t.Setenv(helperruntime.EnvRuntime, "1")
			t.Setenv(helperruntime.EnvRuntimeRoot, root)
			t.Setenv(helperruntime.EnvRuntimeVersion, "v2.0.0")
			t.Setenv(helperruntime.EnvEntryPath, filepath.Join(filepath.Dir(root), "cxp"))
			t.Setenv(helperruntime.EnvBootManaged, "1")
			if err := os.WriteFile(filepath.Join(root, "boot.json"), []byte(legacy), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := adoptLegacyTrial(context.Background(), root, raw, "v2.0.0"); err == nil || os.IsNotExist(err) {
				t.Fatalf("unclaimed legacy trial admitted: %v", err)
			}
			if _, err := os.Stat(filepath.Join(root, helperruntime.BootFile)); !os.IsNotExist(err) {
				t.Fatal("invalid trial published native bootstrap", err)
			}
		})
	}
}

func TestLegacyReceiptDoesNotInterceptSubsequentNativeTrial(t *testing.T) {
	root := filepath.Join(t.TempDir(), ".cxp-runtime")
	source := filepath.Join(t.TempDir(), "migration-runtime")
	if err := os.WriteFile(source, []byte("not executed by admission"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.AdmitUpgradeBootstrap(context.Background(), root, source, "v2.0.0"); err != nil {
		t.Fatal(err)
	}
	target := helperruntime.VersionPath(root, "v2.0.0", runtime.GOOS)
	hash, err := fileSHA256(target)
	if err != nil {
		t.Fatal(err)
	}
	entry := filepath.Join(filepath.Dir(root), helperruntime.BinaryName(runtime.GOOS))
	s, err := updatestate.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	err = s.SavePending(updatestate.Pending{Schema: 1, TargetVersion: "v2.0.0", TargetSHA256: hash, TargetActive: target, EntryPath: entry, RuntimeRoot: root, RequestID: legacyUpgradePrefix + "prior"})
	s.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "active"), []byte("v2.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	boot, err := helperruntime.ReadBoot(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.StageBootCandidate(context.Background(), helperruntime.Context{Root: root}, boot.Good, boot.Generation, source, "v3.0.0", hash); err != nil {
		t.Fatal(err)
	}
	before := candidateExecutablePath
	t.Cleanup(func() { candidateExecutablePath = before })
	candidateExecutablePath = func() (string, error) { return helperruntime.VersionPath(root, "v3.0.0", runtime.GOOS), nil }
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, root)
	t.Setenv(helperruntime.EnvRuntimeVersion, "v3.0.0")
	t.Setenv(helperruntime.EnvEntryPath, entry)
	t.Setenv(helperruntime.EnvBootManaged, "1")
	var stderr bytes.Buffer
	if _, handled := HandleLegacyMigration([]string{entry, "profile"}, "v3.0.0", io.Discard, &stderr); handled {
		t.Fatal("native trial intercepted", stderr.String())
	}
	after, err := helperruntime.ReadBoot(root)
	if err != nil || after.Good != "v2.0.0" || after.Candidate == nil || after.Candidate.Version != "v3.0.0" {
		t.Fatal("native trial changed", after, err)
	}
}
