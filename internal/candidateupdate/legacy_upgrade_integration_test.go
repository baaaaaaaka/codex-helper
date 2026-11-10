//go:build cxp_boot_e2e

package candidateupdate

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

func legacyEnvironment(t *testing.T) string {
	t.Helper()
	fixture := os.Getenv("CXP_LEGACY_FIXTURE")
	if fixture == "" {
		t.Fatal("CXP_LEGACY_FIXTURE is required; use scripts/ci/test_legacy_upgrade.sh")
	}
	caches, err := exec.Command("go", "env", "GOMODCACHE", "GOCACHE").Output()
	if err != nil {
		t.Fatal(err)
	}
	paths := strings.Split(strings.TrimSpace(strings.ReplaceAll(string(caches), "\r\n", "\n")), "\n")
	if len(paths) != 2 {
		t.Fatalf("cache paths %q", caches)
	}
	t.Setenv("GOMODCACHE", paths[0])
	t.Setenv("GOCACHE", paths[1])
	home := t.TempDir()
	for key, value := range map[string]string{"HOME": home, "USERPROFILE": home, "APPDATA": filepath.Join(home, "AppData", "Roaming"), "LOCALAPPDATA": filepath.Join(home, "AppData", "Local"), "XDG_CONFIG_HOME": filepath.Join(home, "config"), "XDG_STATE_HOME": filepath.Join(home, "state"), "XDG_CACHE_HOME": filepath.Join(home, "cache"), "CODEX_HOME": filepath.Join(home, ".codex"), "CXP_AUTO_UPDATE": "0", "CODEX_PROXY_SKIP_BUILTIN_SKILLS": "1"} {
		t.Setenv(key, value)
	}
	return fixture
}

func buildUpgradeCandidate(t *testing.T, version string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), helperruntime.BinaryName(runtime.GOOS))
	cmd := exec.Command("go", "build", "-trimpath", "-ldflags", "-X github.com/baaaaaaaka/codex-helper/internal/cli.version="+version, "-o", binary, "../../cmd/codex-proxy")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	return binary
}

func runUpgradeCLI(t *testing.T, binary string, allowFailure bool, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(helperruntime.WithoutRuntimeMarkers(os.Environ()), helperruntime.EnvForce+"=1")
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil || (err != nil && !allowFailure) {
		t.Fatalf("%s %v: %v %s", binary, args, err, out)
	}
	return out
}

func copyUpgradeBinary(t *testing.T, source, target string) {
	t.Helper()
	b, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(target, b, 0755); err != nil {
		t.Fatal(err)
	}
}

// f.Stat captures the Windows file ID immediately; os.Stat can defer that
// lookup until SameFile, after the path has already been replaced.
func upgradeEntryIdentity(t *testing.T, path string) os.FileInfo {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestCandidateLegacyOriginalUpdaterCompletesSQLiteCutover(t *testing.T) {
	fixture := legacyEnvironment(t)
	candidate := buildUpgradeCandidate(t, "v9.99.0")
	b, err := os.ReadFile(candidate)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(b)
	asset := "codex-proxy_9.99.0_" + runtime.GOOS + "_" + runtime.GOARCH
	if runtime.GOOS == "windows" {
		asset += ".exe"
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch filepath.Base(r.URL.Path) {
		case asset:
			w.Write(b)
		case "checksums.txt":
			fmt.Fprintf(w, "%x  %s\n", digest, asset)
		default:
			t.Errorf("unexpected download %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("CXP_LEGACY_RELEASE_ORIGIN", server.URL)
	nativeFixture := os.Getenv("CXP_NATIVE_FIXTURE")
	if nativeFixture == "" {
		t.Fatal("CXP_NATIVE_FIXTURE is required; use scripts/ci/test_legacy_upgrade.sh")
	}
	for _, tc := range []struct {
		name, source, version string
		safeBoot, native      bool
	}{
		{"legacy-active", fixture, "v0.1.22-rc.96", false, false},
		{"protocol-one", fixture, "v0.1.22-rc.96", true, false},
		{"native-predecessor", nativeFixture, "v0.1.22-rc.104", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.safeBoot {
				t.Setenv("CODEX_PROXY_SKIP_BUILTIN_SKILLS", "")
			}
			dir := t.TempDir()
			entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
			root := filepath.Join(dir, ".cxp-runtime")
			copyUpgradeBinary(t, tc.source, entry)
			originalEntry := upgradeEntryIdentity(t, entry)
			out := runUpgradeCLI(t, entry, false, "--version")
			if !strings.Contains(string(out), strings.TrimPrefix(tc.version, "v")) {
				t.Fatal(string(out))
			}
			if tc.safeBoot {
				if err := os.WriteFile(filepath.Join(root, "boot.json"), []byte(`{"schema":1,"generation":8,"good":"v0.1.22-rc.96"}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.native {
				runUpgradeCLI(t, entry, false, "--cxp-install-bootstrap", "--target", entry)
			}
			config := filepath.Join(dir, "config.json")
			raw := []byte(`{"version":6,"minReader":1,"runtimeGeneration":1,"profiles":[{"id":"preserved","name":"preserved-profile","sshArgs":[]}],"instances":[]}`)
			if err := os.WriteFile(config, raw, 0600); err != nil {
				t.Fatal(err)
			}
			var held *exec.Cmd
			var release io.WriteCloser
			if runtime.GOOS == "windows" && tc.safeBoot {
				held = exec.Command(entry, "__fixture-hold-entry")
				held.Env = helperruntime.WithoutRuntimeMarkers(os.Environ())
				release, err = held.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				ready, pipeErr := held.StdoutPipe()
				if pipeErr != nil {
					t.Fatal(pipeErr)
				}
				if err = held.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { release.Close(); held.Process.Kill() })
				if line, err := bufio.NewReader(ready).ReadString('\n'); err != nil || line != "mapped\n" {
					t.Fatal("hold entry did not map", line, err)
				}
			}
			runUpgradeCLI(t, entry, false, "--config", config, "upgrade", "--repo", "baaaaaaaka/codex-helper", "--version", "v9.99.0", "--install-path", entry)
			if _, err := os.Stat(filepath.Join(root, helperruntime.BootFile)); !tc.native && !os.IsNotExist(err) {
				t.Fatalf("native admission preceded old caller's final writes: %v", err)
			}
			core := filepath.Join(dir, "core.sqlite")
			if !tc.native {
				runUpgradeCLI(t, fixture, false, "__fixture-last-core-write", core)
			}
			runUpgradeCLI(t, entry, false, "--config", config, "proxy", "prune")
			boot, err := helperruntime.ReadBoot(root)
			previous := ""
			if tc.native {
				previous = tc.version
			}
			if err != nil || boot.Good != "v9.99.0" || boot.Previous != previous {
				t.Fatalf("boot %+v %v", boot, err)
			}
			if !tc.native {
				s, err := store.Open(core)
				if err != nil {
					t.Fatal(err)
				}
				err = s.Read(func(tx *store.Tx) error {
					pending, ok, err := tx.UpdatePending()
					if err == nil && (!ok || pending.Tag != "v9.99.0" || pending.From != "v0.1.22-rc.96") {
						return fmt.Errorf("last old-core write lost: %+v %v", pending, ok)
					}
					return err
				})
				s.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if held != nil {
				// Native admission must work while a different terminal/service
				// still maps the old image. Repeated calls reuse one worker.
				for repeat := 0; repeat < 3; repeat++ {
					runUpgradeCLI(t, entry, false, "--config", config, "proxy", "prune")
				}
				if got, _ := fileSHA256(entry); got == hex.EncodeToString(digest[:]) {
					t.Fatal("mapped entry unexpectedly overwritten")
				}
				if _, err := migrationPending(root); err != nil {
					t.Fatal("lost migration while launcher still mapped", err)
				}
				release.Close()
				if err := held.Wait(); err != nil {
					t.Fatal(err)
				}
			}
			if !tc.native {
				// Allow quiescence: launching the old image on every poll
				// would keep creating the mappings the Windows worker awaits.
				deadline := time.Now().Add(15 * time.Second)
				for {
					now := upgradeEntryIdentity(t, entry)
					if !os.SameFile(originalEntry, now) {
						break
					}
					if time.Now().After(deadline) {
						log, _ := os.ReadFile(filepath.Join(root, "bootstrap-migration.log"))
						t.Fatalf("physical bootstrap did not converge after launcher exit; worker: %s", log)
					}
					time.Sleep(100 * time.Millisecond)
				}
				runUpgradeCLI(t, entry, false, "--config", config, "proxy", "prune")
			}
			if _, err := os.Stat(filepath.Join(root, "active")); !os.IsNotExist(err) {
				t.Fatal("migration cleanup did not converge", err)
			}
			wantHash := hex.EncodeToString(digest[:])
			if tc.native {
				wantHash, err = fileSHA256(tc.source)
				if err != nil {
					t.Fatal(err)
				}
			}
			if got, err := fileSHA256(entry); err != nil || got != wantHash {
				t.Fatalf("physical entry %s %v", got, err)
			}
			if _, err := migrationPending(root); !os.IsNotExist(err) {
				t.Fatalf("migration receipt retained: %v", err)
			}
			if !tc.native {
				// Simulate a crash after receipt deletion, before scalar cleanup.
				if err := os.WriteFile(filepath.Join(root, "active"), []byte("v9.99.0\n"), 0600); err != nil {
					t.Fatal(err)
				}
				runUpgradeCLI(t, entry, false, "--config", config, "proxy", "prune")
				if _, err := os.Stat(filepath.Join(root, "active")); !os.IsNotExist(err) {
					t.Fatal("orphan forwarding receipt retained", err)
				}
			}
			if tc.safeBoot {
				if _, err := os.Stat(filepath.Join(os.Getenv("HOME"), ".agents", "skills", "cxp", "SKILL.md")); err != nil {
					t.Fatal("deferred builtin refresh was lost", err)
				}
			}
			if err := os.WriteFile(config, []byte("poisoned legacy"), 0600); err != nil {
				t.Fatal(err)
			}
			runUpgradeCLI(t, entry, false, "--config", config, "proxy", "list")
		})
	}
}

func TestCandidateLegacyTUITrialTransfersBeforeNativeWrites(t *testing.T) {
	fixture := legacyEnvironment(t)
	candidate := buildUpgradeCandidate(t, "v9.99.0")
	dir := t.TempDir()
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	root := filepath.Join(dir, ".cxp-runtime")
	copyUpgradeBinary(t, fixture, entry)
	runUpgradeCLI(t, entry, false, "--version")
	immutable := helperruntime.VersionPath(root, "v9.99.0", runtime.GOOS)
	copyUpgradeBinary(t, candidate, immutable)
	hash, err := fileSHA256(immutable)
	if err != nil {
		t.Fatal(err)
	}
	record, err := json.Marshal(helperruntime.BootState{Schema: 1, Generation: 8, Good: "v0.1.22-rc.96", Candidate: &helperruntime.BootCandidate{Version: "v9.99.0", SHA256: hash}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "boot.json"), record, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CXP_AUTO_UPDATE", "1")
	// An invalid profile exits after the migration handshake. The historical
	// supervisor must not replay that failure on its incompatible old Good.
	runUpgradeCLI(t, entry, true, "--profile", "missing-upgrade-fixture-profile")
	boot, err := helperruntime.ReadBoot(root)
	if err != nil || boot.Good != "v9.99.0" || boot.Previous != "" {
		t.Fatalf("trial did not migrate safely: %+v %v", boot, err)
	}
	runUpgradeCLI(t, entry, false, "proxy", "prune")
}
