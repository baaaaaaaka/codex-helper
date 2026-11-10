//go:build cxp_boot_e2e

package candidateupdate

// Real source binaries: run only in the isolated Docker harness or native CI.
import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
)

func TestCandidateNativeInstallUpdateAndRecovery(t *testing.T) {
	// Keep Go's caches outside the synthetic account. Module cache directories
	// are read-only and cannot be removed by TempDir cleanup on native CI.
	cacheOutput, err := exec.Command("go", "env", "GOMODCACHE", "GOCACHE").Output()
	if err != nil {
		t.Fatal(err)
	}
	// Cache paths may contain spaces (notably on Windows).
	caches := strings.Split(strings.TrimSpace(strings.ReplaceAll(string(cacheOutput), "\r\n", "\n")), "\n")
	if len(caches) != 2 {
		t.Fatalf("unexpected Go cache paths: %q", cacheOutput)
	}
	t.Setenv("GOMODCACHE", caches[0])
	t.Setenv("GOCACHE", caches[1])
	dir := t.TempDir()
	home := filepath.Join(dir, "home")
	if err := os.MkdirAll(home, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
	t.Setenv("CODEX_PROXY_SKIP_BUILTIN_SKILLS", "1")
	build := func(version string) string {
		path := filepath.Join(dir, "candidate-"+version+"-"+helperruntime.BinaryName(runtime.GOOS))
		cmd := exec.Command("go", "build", "-trimpath", "-ldflags", "-X github.com/baaaaaaaka/codex-helper/internal/cli.version="+version, "-o", path, "../../cmd/codex-proxy")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v: %s", version, err, out)
		}
		return path
	}
	first, second := build("v1.0.0"), build("v2.0.0")
	install := filepath.Join(dir, "install")
	if err := os.MkdirAll(install, 0700); err != nil {
		t.Fatal(err)
	}
	entry := filepath.Join(install, helperruntime.BinaryName(runtime.GOOS))
	entryBytes, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry, entryBytes, 0755); err != nil {
		t.Fatal(err)
	}
	run := func(binary string, args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = append(helperruntime.WithoutRuntimeMarkers(os.Environ()), helperruntime.EnvForce+"=1", "CXP_AUTO_UPDATE=0")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v: %s", binary, args, err, out)
		}
		return out
	}
	root := filepath.Join(install, ".cxp-runtime")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	legacy := []byte(`{"schema":1,"generation":8,"good":"v0.9.0","previous":"v0.8.0"}`)
	if err := os.WriteFile(filepath.Join(root, "boot.json"), legacy, 0600); err != nil {
		t.Fatal(err)
	}
	run(entry, "--cxp-install-bootstrap", "--target", entry)
	boot, err := helperruntime.ReadBoot(root)
	if err != nil || boot.Good != "v1.0.0" || boot.Previous != "" || boot.Generation != 9 {
		t.Fatalf("bootstrap %+v: %v", boot, err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "boot.json.migrated")); err != nil || !bytes.Equal(got, legacy) {
		t.Fatal("legacy evidence", err)
	}
	assertVersion := func(want string) {
		t.Helper()
		if out := run(entry, "--version"); !strings.Contains(string(out), want) {
			t.Fatalf("want %s: %s", want, out)
		}
	}
	assertVersion("1.0.0")
	record := filepath.Join(dir, "record", "install.sqlite")
	request := Context{SourceVersion: "v1.0.0", TargetVersion: "v2.0.0", RuntimeRoot: root, EntryPath: entry, RecordPath: record, RequestID: "native-update"}
	unlock := holdNativeCandidateEntry(t, entry)
	result, err := Invoke(context.Background(), second, request, 30*time.Second)
	unlock()
	if err != nil || result.Version != "2.0.0" {
		t.Fatalf("update %+v: %v", result, err)
	}
	boot, err = helperruntime.ReadBoot(root)
	if err != nil || boot.Good != "v2.0.0" || boot.Previous != "v1.0.0" {
		t.Fatalf("updated %+v: %v", boot, err)
	}
	if got, _ := os.ReadFile(entry); !bytes.Equal(got, entryBytes) {
		t.Fatal("update replaced physical bootstrap")
	}
	assertVersion("2.0.0")
	stored, err := managedinstall.LoadRecord(record)
	if err != nil || stored.Version != "2.0.0" {
		t.Fatalf("install record %+v: %v", stored, err)
	}
	state, err := updatestate.OpenRead(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Invocation(request.RequestID); !os.IsNotExist(err) {
		t.Fatal("invocation leaked", err)
	}
	state.Close()
	result, err = Invoke(context.Background(), second, request, 30*time.Second)
	if err != nil || !result.Noop {
		t.Fatalf("retry %+v: %v", result, err)
	}
	run(entry, "--recover-previous")
	assertVersion("1.0.0")
	boot, err = helperruntime.ReadBoot(root)
	if err != nil || boot.Good != "v1.0.0" || boot.Previous != "v2.0.0" {
		t.Fatalf("recovery %+v: %v", boot, err)
	}
	cmd := exec.Command(second, InternalCommand, "--protocol=1", "--context-file="+filepath.Join(dir, "not-present.json"))
	cmd.Env = append(helperruntime.WithoutRuntimeMarkers(os.Environ()), helperruntime.EnvDisable+"=1")
	if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "unsupported argument") {
		t.Fatalf("legacy transport admitted: %v %s", err, out)
	}
}
