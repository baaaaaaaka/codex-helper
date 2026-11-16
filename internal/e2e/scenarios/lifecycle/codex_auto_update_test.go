package lifecycle

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

// Only the external npm registry and Codex model are fakes. Production CLI
// wiring, install-source detection, installer shell, npm, binary replacement,
// version probes, durable restart and explicit retry all execute normally.
func TestManagedCodexAutomaticUpgrade(t *testing.T) {
	harness.Parallel(t)
	registry, env, downloads := offlineCodexRegistry(t, false)
	s := harness.NewScenario(t, "CODEX-AUTO-UPGRADE", harness.Options{
		Guarantees: []string{"codex-auto-upgrade"},
		Title:      "too-old managed Codex is really replaced through npm; restart announces once and only explicit retry runs the request",
		Budget:     150 * time.Second, NoStart: true, Factory: sut.InstalledCodexFactory(), UserEnv: env,
		Golden: harness.GoldenOptions{Skip: "assert installed bytes, old/new executable identities, instance transition and exact request/notice counts"},
	})
	defer s.Finish()
	managed := filepath.Join(s.Env.Home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex")
	oldExecutable := installManagedCodexPackage(t, managed)
	// A disposable service account may have no discoverable login shell.
	// Configure the supported user-facing override, as installed services can.
	if out, code := s.RunCLI(20*time.Second, "teams", "path", "shell", "/bin/bash"); code != 0 {
		t.Fatalf("configure upgrade login shell: %s", out)
	}
	tok := s.Token("old")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) && c.Exe == oldExecutable {
			return harness.CodexBehavior{Err: errors.New(tooOldCodexFixture)}
		}
		return harness.CodexBehavior{}
	})
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "auto-upgrade")
	oldInstance := s.SUT.Instance()
	steps.MustPost(s, work.ID, "Use the newest model "+tok)
	if !s.WaitFor("actual upgrade and restart completion", 90*time.Second, func() (bool, string) {
		n := len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded"))
		return n > 0 && s.SUT.Instance() != oldInstance, fmt.Sprintf("notices=%d instance=%s downloads=%d registry=%s", n, s.SUT.Instance(), downloads.Load(), registry.URL)
	}) {
		return
	}
	if !s.Check("upgrade messages finish draining", s.Settle(2*time.Second, 10*time.Second), "settled", "upgrade drain") {
		return
	}
	s.Check("one real package download", downloads.Load() == 1, "1", fmt.Sprint(downloads.Load()))
	installed, err := os.ReadFile(managed)
	replacement, buildErr := sut.VersionedFakeCodex("0.59.0")
	expected, readErr := os.ReadFile(replacement)
	s.Check("npm installed the new executable bytes", err == nil && buildErr == nil && readErr == nil && bytes.Equal(installed, expected), "new binary", fmt.Sprintf("read=%v build=%v expected=%v", err, buildErr, readErr))
	s.Check("failed request is not retried automatically", len(s.Codex.CallsWithToken(tok)) == 1, "1 call", fmt.Sprint(len(s.Codex.CallsWithToken(tok))))
	s.Check("one completed upgrade announcement", len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded")) == 1, "1", fmt.Sprint(len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded"))))
	steps.MustPost(s, work.ID, "helper retry last")
	if _, ok := steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout); !ok {
		return
	}
	if !s.Check("retry finishes draining", s.Settle(2*time.Second, 10*time.Second), "settled", "retry drain") {
		return
	}
	calls := s.Codex.CallsWithToken(tok)
	upgraded := filepath.Join(filepath.Dir(filepath.Dir(managed)), "lib", "node_modules", "@openai", "codex", "bin", "codex")
	s.Check("explicit retry runs the installed new binary in a new process", len(calls) == 2 && calls[0].Exe == oldExecutable && calls[1].Exe == upgraded && calls[0].Pid != calls[1].Pid, "old managed binary then installed npm binary, two processes", fmt.Sprintf("%+v", calls))
	s.Check("upgrade announcement stays once after retry", len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded")) == 1, "1", fmt.Sprint(len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded"))))
}

func TestManagedCodexUpgradeDownloadFailure(t *testing.T) {
	harness.Parallel(t)
	_, env, downloads := offlineCodexRegistry(t, true)
	s := harness.NewScenario(t, "CODEX-AUTO-UPGRADE-FAIL", harness.Options{
		Guarantees: []string{"codex-auto-upgrade"},
		Title:      "failed npm package download preserves the usable managed executable and helper; no implicit retry",
		Budget:     120 * time.Second, NoStart: true, Factory: sut.InstalledCodexFactory(), UserEnv: env,
		Golden: harness.GoldenOptions{Skip: "assert actual failed package request, unchanged binary, unchanged process and subsequent successful work"},
	})
	defer s.Finish()
	managed := filepath.Join(s.Env.Home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex")
	oldExecutable := installManagedCodexPackage(t, managed)
	// A disposable service account may have no discoverable login shell.
	// Configure the supported user-facing override, as installed services can.
	if out, code := s.RunCLI(20*time.Second, "teams", "path", "shell", "/bin/bash"); code != 0 {
		t.Fatalf("configure upgrade login shell: %s", out)
	}
	before, err := os.ReadFile(managed)
	if err != nil {
		t.Fatal(err)
	}
	tok := s.Token("failed")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{Err: errors.New(tooOldCodexFixture)}
		}
		return harness.CodexBehavior{}
	})
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "failed-upgrade")
	oldInstance := s.SUT.Instance()
	steps.MustPost(s, work.ID, "Use the newest model "+tok)
	if !s.WaitFor("actual download failure notice", 60*time.Second, func() (bool, string) {
		n := len(s.SUTMessagesContaining(work.ID, "Codex CLI upgrade failed"))
		return n > 0, fmt.Sprintf("notices=%d downloads=%d", n, downloads.Load())
	}) {
		return
	}
	if !s.Check("failure drains", s.Settle(2*time.Second, 10*time.Second), "settled", "failure drain") {
		return
	}
	after, err := os.ReadFile(managed)
	s.Check("failed download leaves old executable unchanged", err == nil && bytes.Equal(before, after), "unchanged bytes", fmt.Sprint(err))
	s.Check("real package download failed without restart", downloads.Load() > 0 && s.SUT.Instance() == oldInstance, "download attempted; same instance", fmt.Sprintf("downloads=%d instance=%s", downloads.Load(), s.SUT.Instance()))
	s.Check("failed request is not retried automatically", len(s.Codex.CallsWithToken(tok)) == 1, "1 call", fmt.Sprint(len(s.Codex.CallsWithToken(tok))))
	warm := s.Token("still-usable")
	steps.MustPost(s, work.ID, "Continue using the existing install "+warm)
	if _, ok := steps.WaitAnswered(s, work.ID, warm, steps.AnswerTimeout); !ok {
		return
	}
	calls := s.Codex.CallsWithToken(warm)
	s.Check("old install still answers work", len(calls) == 1 && calls[0].Exe == oldExecutable, "one call using old managed binary", fmt.Sprintf("%+v", calls))
	s.Check("no successful-upgrade announcement", len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded")) == 0, "0", fmt.Sprint(len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded"))))
}

// The manual command has its own positive case; the explicit-path refusal
// scenario must not stand in for a successful managed-install update.
func TestManagedCodexManualUpgrade(t *testing.T) {
	harness.Parallel(t)
	_, env, downloads := offlineCodexRegistry(t, false)
	s := harness.NewScenario(t, "CODEX-MANUAL-UPGRADE", harness.Options{
		Guarantees: []string{"F-SVC-12"},
		Title:      "codex update now replaces the managed binary, restarts once and serves new work",
		Budget:     120 * time.Second, NoStart: true, Factory: sut.InstalledCodexFactory(), UserEnv: env,
		Golden: harness.GoldenOptions{Skip: "assert real installed bytes, restart instance, one completion notice and new executable handling work"},
	})
	defer s.Finish()
	managed := filepath.Join(s.Env.Home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex")
	installManagedCodexPackage(t, managed)
	if out, code := s.RunCLI(20*time.Second, "teams", "path", "shell", "/bin/bash"); code != 0 {
		t.Fatalf("configure upgrade login shell: %s", out)
	}
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "manual-upgrade")
	oldInstance := s.SUT.Instance()
	steps.MustPost(s, control.ID, "codex update now")
	if !s.WaitFor("manual upgrade completes and restarts", 90*time.Second, func() (bool, string) {
		n := len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded"))
		return n > 0 && s.SUT.Instance() != oldInstance, fmt.Sprintf("notices=%d downloads=%d instance=%s", n, downloads.Load(), s.SUT.Instance())
	}) {
		return
	}
	installed, err := os.ReadFile(managed)
	replacement, buildErr := sut.VersionedFakeCodex("0.59.0")
	expected, readErr := os.ReadFile(replacement)
	s.Check("manual command really installs the package", downloads.Load() == 1 && err == nil && buildErr == nil && readErr == nil && bytes.Equal(installed, expected), "one download and new executable bytes", fmt.Sprintf("downloads=%d read=%v build=%v expected=%v", downloads.Load(), err, buildErr, readErr))
	tok := s.Token("new-work")
	steps.MustPost(s, work.ID, "Use the upgraded install "+tok)
	if _, ok := steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout); !ok {
		return
	}
	calls := s.Codex.CallsWithToken(tok)
	upgraded := filepath.Join(filepath.Dir(filepath.Dir(managed)), "lib", "node_modules", "@openai", "codex", "bin", "codex")
	s.Check("new work uses the installed binary", len(calls) == 1 && calls[0].Exe == upgraded, "one call using new npm executable", fmt.Sprintf("%+v", calls))
	s.Check("one manual upgrade completion", len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded")) == 1, "1", fmt.Sprint(len(s.SUTMessagesContaining(control.ID, "Codex CLI upgraded"))))
}

const tooOldCodexFixture = "The 'gpt-9' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again."

// Match an actual npm global installation: npm refuses to overwrite an
// unrelated executable placed directly at prefix/bin/codex. Keeping distinct
// old/new bin targets also lets the app-server report which bytes it ran.
func installManagedCodexPackage(t *testing.T, managed string) string {
	t.Helper()
	pkg := filepath.Join(filepath.Dir(filepath.Dir(managed)), "lib", "node_modules", "@openai", "codex")
	old := filepath.Join(pkg, "bin", "old-codex")
	installCodexCopy(t, old)
	manifest := []byte(`{"name":"@openai/codex","version":"0.58.0","bin":{"codex":"bin/old-codex"}}`)
	if err := os.WriteFile(filepath.Join(pkg, "package.json"), manifest, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(managed), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..", "lib", "node_modules", "@openai", "codex", "bin", "old-codex"), managed); err != nil {
		t.Fatal(err)
	}
	return old
}

func offlineCodexRegistry(t *testing.T, failDownload bool) (*httptest.Server, map[string]string, *atomic.Int32) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX managed npm installer; exercised by Linux E2E CI")
	}
	for _, cmd := range []string{"node", "npm"} {
		if _, err := exec.LookPath(cmd); err != nil {
			t.Fatalf("offline managed upgrade requires %s (CI installs Node 22): %v", cmd, err)
		}
	}
	binary, err := sut.VersionedFakeCodex("0.59.0")
	if err != nil {
		t.Fatal(err)
	}
	native, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	pkg := map[string]any{"name": "@openai/codex", "version": "0.59.0", "bin": map[string]string{"codex": "bin/codex"}}
	manifest, err := json.Marshal(pkg)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, entry := range []struct {
		name string
		body []byte
		mode int64
	}{{"package/package.json", manifest, 0644}, {"package/bin/codex", native, 0755}} {
		if err := tw.WriteHeader(&tar.Header{Name: entry.name, Mode: entry.mode, Size: int64(len(entry.body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(entry.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	archive := buf.Bytes()
	sum := sha1.Sum(archive)
	downloads := new(atomic.Int32)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/@openai/codex":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"name": "@openai/codex", "dist-tags": map[string]string{"latest": "0.59.0"}, "versions": map[string]any{"0.59.0": map[string]any{"name": "@openai/codex", "version": "0.59.0", "bin": pkg["bin"], "dist": map[string]string{"tarball": server.URL + "/codex.tgz", "shasum": hex.EncodeToString(sum[:])}}}})
		case r.Method == http.MethodGet && r.URL.Path == "/codex.tgz":
			downloads.Add(1)
			if failDownload {
				http.Error(w, "injected package unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(archive)
		default:
			http.Error(w, "unexpected npm request: "+r.Method+" "+r.URL.Path, http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	scratch := t.TempDir()
	return server, map[string]string{"npm_config_registry": server.URL, "npm_config_fetch_retries": "0", "npm_config_audit": "false", "npm_config_ignore_scripts": "true", "npm_config_update_notifier": "false", "npm_config_userconfig": filepath.Join(scratch, "npmrc"), "npm_config_cache": filepath.Join(scratch, "cache")}, downloads
}
