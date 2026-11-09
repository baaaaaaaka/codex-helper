//go:build cxp_boot_e2e

package update

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

// This is a native executable, including on Windows. The fake endpoint replaces
// GitHub only; production download, verification, publication and boot all run.
func TestTUIAutoNativeDownloadThenRealNextInvocation(t *testing.T) {
	fixture := os.Getenv("CXP_BOOT_FIXTURE")
	if fixture == "" {
		fixture = filepath.Join(t.TempDir(), helperruntime.BinaryName(runtime.GOOS))
		cmd := exec.Command("go", "build", "-o", fixture, "../helperruntime/testdata/bootfixture")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build native fixture: %v %s", err, out)
		}
	}
	payload, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	c := tuiUpdateFixture(t)
	// Replace the inert unit fixture with a real stable entry and good runtime.
	if err := os.WriteFile(c.EntryPath, payload, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.RuntimePath, payload, 0755); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	m := tuiManifestFixture(t, payload, now)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/manifest" {
			_ = json.NewEncoder(w).Encode(m)
			return
		}
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	restore := overrideGitHubBases(server.URL)
	defer restore()
	if err := checkAndStageTUI(context.Background(), c, server.URL+"/manifest", now); err != nil {
		t.Fatal(err)
	}
	s, err := helperruntime.ReadBoot(c.Root)
	if err != nil || s.Good != "v1.0.0" || s.Candidate.Version != "v2.0.0" {
		t.Fatalf("%+v %v", s, err)
	}
	cmd := exec.Command(c.EntryPath, "tui")
	cmd.Env = append(helperruntime.WithoutRuntimeMarkers(os.Environ()), helperruntime.EnvForce+"=1", "CXP_BOOT_TEST_DIR="+filepath.Dir(c.Root), "CXP_AUTO_UPDATE=1", "CODEX_PROXY_VERSION=latest")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("real invocation: %v %s", err, out)
	}
	s, err = helperruntime.ReadBoot(c.Root)
	if err != nil || s.Good != "v2.0.0" || s.Previous != "v1.0.0" {
		t.Fatalf("%+v %v", s, err)
	}
	cmd = exec.Command(c.EntryPath, "--version")
	cmd.Env = append(helperruntime.WithoutRuntimeMarkers(os.Environ()), helperruntime.EnvForce+"=1")
	if out, err := cmd.CombinedOutput(); err != nil || !strings.Contains(string(out), "2.0.0") {
		t.Fatalf("fresh launch: %v %s", err, out)
	}
}
