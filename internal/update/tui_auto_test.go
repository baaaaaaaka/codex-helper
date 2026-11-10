package update

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
	"github.com/gofrs/flock"
)

func tuiUpdateFixture(t *testing.T) helperruntime.Context {
	t.Helper()
	t.Setenv(EnvRepo, DefaultRepo)
	dir := t.TempDir()
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	root := filepath.Join(dir, ".cxp-runtime")
	if err := os.WriteFile(entry, []byte("stable recovery entry"), 0755); err != nil {
		t.Fatal(err)
	}
	path, err := helperruntime.InstallVersion(root, entry, "v1.0.0", runtime.GOOS, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := helperruntime.Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	return helperruntime.Context{Root: root, EntryPath: entry, Version: "v1.0.0", RuntimePath: path}
}

func tuiManifestFixture(t *testing.T, payload []byte, now time.Time) TUIManifest {
	t.Helper()
	name, err := assetName("2.0.0", runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Skip(err)
	}
	h := sha256.Sum256(payload)
	return TUIManifest{Schema: 1, Protocol: helperruntime.BootProtocol, Repo: DefaultRepo, Version: "v2.0.0", PublishedAt: now.Add(-72 * time.Hour), Priority: AutoUpdatePriorityP0, Assets: map[string]TUIAsset{runtime.GOOS + "/" + runtime.GOARCH: {Name: name, SHA256: hex.EncodeToString(h[:]), Size: int64(len(payload))}}}
}

func TestTUIAutoManifestRejectsIncompatibleMetadata(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name   string
		change func(*TUIManifest)
	}{
		{"schema", func(m *TUIManifest) { m.Schema = 99 }}, {"protocol", func(m *TUIManifest) { m.Protocol++ }},
		{"repository", func(m *TUIManifest) { m.Repo = "attacker/repo" }}, {"prerelease", func(m *TUIManifest) { m.Version = "v2.0.0-rc.1" }},
		{"path", func(m *TUIManifest) { m.Version = "../../bin/sh" }}, {"noncanonical", func(m *TUIManifest) { m.Version = "2.0.0" }},
		{"missing publication", func(m *TUIManifest) { m.PublishedAt = time.Time{} }}, {"future release", func(m *TUIManifest) { m.PublishedAt = now.Add(time.Hour) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := tuiManifestFixture(t, []byte("binary"), now)
			tc.change(&m)
			raw, _ := json.Marshal(m)
			if _, err := VerifyTUIManifest(raw, DefaultRepo, now); err == nil {
				t.Fatal("unsafe manifest accepted")
			}
		})
	}
	if _, err := VerifyTUIManifest([]byte(strings.Repeat(" ", (128<<10)+1)), DefaultRepo, now); err == nil {
		t.Fatal("unbounded manifest accepted")
	}
}

func TestTUIAutoReadsPostCIPublisherContract(t *testing.T) {
	raw, err := os.ReadFile("testdata/tui-update.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, year := range []int{2026, 2028} {
		// A long-offline installation can still use the last tested stable
		// release. Static metadata must not require a renewal service.
		now := time.Date(year, 11, 9, 0, 0, 0, 0, time.UTC)
		if m, err := VerifyTUIManifest(raw, "owner/name", now); err != nil || m.Version != "v2.0.0" {
			t.Fatalf("%+v %v", m, err)
		}
	}
}

func TestTUIAutoBadDownloadNeverChangesDefaultOrExecutes(t *testing.T) {
	for _, mode := range []string{"missing digest", "wrong digest", "wrong platform", "oversize", "truncated", "http failure", "wrong version", "old protocol"} {
		t.Run(mode, func(t *testing.T) {
			c := tuiUpdateFixture(t)
			now := time.Now().UTC()
			marker := filepath.Join(filepath.Dir(c.Root), "executed")
			payload := []byte("#!/bin/sh\nprintf touched > '" + marker + "'\nprintf 'cxp version 2.0.0\\n'\n")
			m := tuiManifestFixture(t, payload, now)
			platform := runtime.GOOS + "/" + runtime.GOARCH
			a := m.Assets[platform]
			switch mode {
			case "missing digest":
				a.SHA256 = ""
			case "wrong digest":
				a.SHA256 = strings.Repeat("0", 64)
			case "wrong platform":
				a.Name = "codex-proxy_2.0.0_wrong_platform"
			case "oversize":
				a.Size = maxTUIAssetSize + 1
			case "wrong version":
				payload = []byte("#!/bin/sh\nprintf 'cxp version 99.0.0\\n'\n")
			case "old protocol":
				payload = []byte("#!/bin/sh\nprintf 'cxp version 2.0.0\\n'\n")
				h := sha256.Sum256(payload)
				a.SHA256 = hex.EncodeToString(h[:])
				a.Size = int64(len(payload))
			}
			m.Assets[platform] = a
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/manifest" {
					_ = json.NewEncoder(w).Encode(m)
					return
				}
				if mode == "http failure" {
					w.WriteHeader(503)
					return
				}
				if mode == "truncated" {
					_, _ = w.Write(payload[:len(payload)/2])
					return
				}
				_, _ = w.Write(payload)
			}))
			defer server.Close()
			restore := overrideGitHubBases(server.URL)
			defer restore()
			if err := checkAndStageTUI(context.Background(), c, server.URL+"/manifest", now); err == nil {
				t.Fatal("bad candidate accepted")
			}
			if active, err := helperruntime.ReadActive(c.Root); err != nil || active != "v1.0.0" {
				t.Fatalf("%s %v", active, err)
			}
			if b, err := os.ReadFile(c.EntryPath); err != nil || string(b) != "stable recovery entry" {
				t.Fatalf("entry changed %s %v", b, err)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("unverified executable ran: %v", err)
			}
			if baseline, err := helperruntime.ReadBoot(c.Root); err != nil || baseline.Good != "v1.0.0" || baseline.Candidate != nil || baseline.Generation != 1 {
				t.Fatalf("failed download published boot record: %v", err)
			}
		})
	}
}

func TestTUIAutoInsufficientOrUnknownDiskSpaceDefersBeforeDownload(t *testing.T) {
	oldFree := tuiDiskFreeBytes
	t.Cleanup(func() { tuiDiskFreeBytes = oldFree })
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprintf("unknown=%v", unknown), func(t *testing.T) {
			c := tuiUpdateFixture(t)
			now := time.Now().UTC()
			m := tuiManifestFixture(t, []byte("never downloaded"), now)
			a := m.Assets[runtime.GOOS+"/"+runtime.GOARCH]
			tuiDiskFreeBytes = func(path string) (uint64, error) {
				if path != c.Root {
					t.Errorf("checked wrong filesystem: %s", path)
				}
				if unknown {
					return 0, errors.New("disk information unavailable")
				}
				return uint64(2*a.Size) + tuiDiskHeadroom - 1, nil
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/manifest" {
					t.Error("download attempted without enough disk space")
				}
				_ = json.NewEncoder(w).Encode(m)
			}))
			defer server.Close()
			restore := overrideGitHubBases(server.URL)
			defer restore()
			if err := checkAndStageTUI(context.Background(), c, server.URL+"/manifest", now); err == nil {
				t.Fatal("update did not defer")
			}
			if baseline, err := helperruntime.ReadBoot(c.Root); err != nil || baseline.Good != "v1.0.0" || baseline.Candidate != nil || baseline.Generation != 1 {
				t.Fatalf("deferred update changed boot record: %v", err)
			}
			if active, err := helperruntime.ReadActive(c.Root); err != nil || active != "v1.0.0" {
				t.Fatalf("deferred update changed default: %s %v", active, err)
			}
			if got, err := os.ReadFile(c.EntryPath); err != nil || string(got) != "stable recovery entry" {
				t.Fatalf("deferred update changed launcher: %q %v", got, err)
			}
		})
	}
}

func TestTUIAutoCheckIsSharedAndPersistsBackoffBeforeNetwork(t *testing.T) {
	c := tuiUpdateFixture(t)
	now := time.Now().UTC()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		state, err := readUpdateCheckForTest(c.Root)
		if err != nil || !state.Next.After(now) {
			t.Error("network before durable cooldown")
		}
		w.Header().Set("Retry-After", "7200")
		w.WriteHeader(429)
	}))
	defer server.Close()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = checkAndStageTUI(context.Background(), c, server.URL+"/manifest", now) }()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("request storm: %d", calls.Load())
	}
	state, err := readUpdateCheckForTest(c.Root)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(c.Root, updatestate.Filename))
	if err != nil {
		t.Fatal(err)
	}
	if state.Failures != 1 || state.Next.Before(now.Add(2*time.Hour)) {
		t.Fatal(state)
	}
	before := string(b)
	if err := checkAndStageTUI(context.Background(), c, server.URL+"/manifest", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(c.Root, updatestate.Filename))
	if string(after) != before || calls.Load() != 1 {
		t.Fatal("unchanged cooldown rewrote state or requested network")
	}
}

func TestTUIAutoMissingManifestNeverFallsBackToReleaseAPI(t *testing.T) {
	c := tuiUpdateFixture(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/manifest" {
			t.Errorf("unguarded fallback: %s", r.URL.Path)
		}
		w.WriteHeader(404)
	}))
	defer server.Close()
	restore := overrideGitHubBases(server.URL)
	defer restore()
	if err := checkAndStageTUI(context.Background(), c, server.URL+"/manifest", time.Now()); err == nil {
		t.Fatal("missing authorization accepted")
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}

func TestTUIAutoBusyOrUnwritableStateSendsNoRequest(t *testing.T) {
	for _, mode := range []string{"busy", "unwritable"} {
		t.Run(mode, func(t *testing.T) {
			c := tuiUpdateFixture(t)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(404) }))
			defer server.Close()
			if mode == "busy" {
				l := flock.New(filepath.Join(c.Root, "auto-update.lock"))
				if err := l.Lock(); err != nil {
					t.Fatal(err)
				}
				defer l.Unlock()
			} else {
				if err := os.Mkdir(filepath.Join(c.Root, updatestate.Filename), 0700); err != nil {
					t.Fatal(err)
				}
			}
			_ = checkAndStageTUI(context.Background(), c, server.URL+"/manifest", time.Now())
			if calls.Load() != 0 {
				t.Fatal("network after state failure")
			}
		})
	}
}

func TestTUIAutoStagesOnceAndKeepsStableEntry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("inert POSIX --version fixture; native boot E2E covers Windows executables")
	}
	c := tuiUpdateFixture(t)
	now := time.Now().UTC()
	payload := []byte("#!/bin/sh\nif [ \"$1\" = \"--cxp-boot-protocol\" ]; then printf '2\\n'; else printf 'cxp version 2.0.0\\n'; fi\n")
	if err := os.WriteFile(c.EntryPath, []byte("#!/bin/sh\nprintf '2\\n'\n"), 0755); err != nil {
		t.Fatal(err)
	}
	m := tuiManifestFixture(t, payload, now)
	var downloads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/manifest" {
			_ = json.NewEncoder(w).Encode(m)
			return
		}
		downloads.Add(1)
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	restore := overrideGitHubBases(server.URL)
	defer restore()
	for _, when := range []time.Time{now, now.Add(time.Hour)} {
		if err := checkAndStageTUI(context.Background(), c, server.URL+"/manifest", when); err != nil {
			t.Fatal(err)
		}
	}
	s, err := helperruntime.ReadBoot(c.Root)
	if err != nil || s.Good != "v1.0.0" || s.Candidate.Version != "v2.0.0" || s.Candidate.Attempt != "" {
		t.Fatalf("%+v %v", s, err)
	}
	if downloads.Load() != 1 {
		t.Fatal("redownloaded staged release", downloads.Load())
	}
}

func TestTUIAutoOlderPhysicalLauncherCannotEnableSafeBoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("inert POSIX protocol fixture")
	}
	c := tuiUpdateFixture(t)
	now := time.Now().UTC()
	payload := []byte("#!/bin/sh\nif [ \"$1\" = \"--cxp-boot-protocol\" ]; then printf '2\\n'; else printf 'cxp version 2.0.0\\n'; fi\n")
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
	if err := checkAndStageTUI(context.Background(), c, server.URL+"/manifest", now); err == nil || !strings.Contains(err.Error(), "current installed launcher") {
		t.Fatal(err)
	}
	if baseline, err := helperruntime.ReadBoot(c.Root); err != nil || baseline.Good != "v1.0.0" || baseline.Candidate != nil || baseline.Generation != 1 {
		t.Fatalf("old bootstrap received a new boot record: %v", err)
	}
	if v, err := helperruntime.ReadActive(c.Root); err != nil || v != "v1.0.0" {
		t.Fatalf("%s %v", v, err)
	}
}

func TestSafeBootExplicitLegacyUpdateCannotBypassProtocolCheck(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("inert POSIX version fixture")
	}
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprint(owned), func(t *testing.T) {
			c := tuiUpdateFixture(t)
			raw := []byte(`{"schema":1,"generation":1,"good":"v1.0.0"}`)
			if err := os.WriteFile(filepath.Join(c.Root, helperruntime.BootFile), raw, 0600); err != nil {
				t.Fatal(err)
			}
			asset, err := assetName("0.1.12", runtime.GOOS, runtime.GOARCH)
			if err != nil {
				t.Fatal(err)
			}
			server := newReleaseServer(t, "v0.1.12", asset, []byte("#!/bin/sh\nprintf 'cxp version 0.1.12\\n'\n"))
			defer server.Close()
			restore := overrideGitHubBases(server.URL)
			defer restore()
			_, err = PerformUpdate(context.Background(), UpdateOptions{Repo: "owner/name", Version: "v0.1.12", ValidateBinary: true, RuntimeRoot: c.Root, RuntimeEntryPath: c.EntryPath, CandidateOwnedApply: owned, Timeout: 5 * time.Second})
			if err == nil || !strings.Contains(err.Error(), "incompatible boot protocol") {
				t.Fatal(err)
			}
			after, _ := os.ReadFile(filepath.Join(c.Root, helperruntime.BootFile))
			if string(raw) != string(after) {
				t.Fatal("legacy update changed safe boot selection")
			}
		})
	}
}

func TestTUIAutoOldWindowComparesInstalledGoodAndHonorsPriority(t *testing.T) {
	for _, mode := range []string{"installed", "p2", "p1 delay"} {
		t.Run(mode, func(t *testing.T) {
			c := tuiUpdateFixture(t)
			now := time.Now().UTC()
			m := tuiManifestFixture(t, []byte("unused"), now)
			if mode == "installed" {
				if _, err := helperruntime.InstallVersion(c.Root, c.EntryPath, "v2.0.0", runtime.GOOS, false); err != nil {
					t.Fatal(err)
				}
				if err := helperruntime.Activate(c.Root, "v2.0.0"); err != nil {
					t.Fatal(err)
				}
			} else if mode == "p2" {
				m.Priority = AutoUpdatePriorityP2
			} else {
				m.Priority = AutoUpdatePriorityP1
				m.PublishedAt = now.Add(-time.Hour)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/manifest" {
					t.Errorf("ineligible asset downloaded %s", r.URL.Path)
				}
				_ = json.NewEncoder(w).Encode(m)
			}))
			defer server.Close()
			if err := checkAndStageTUI(context.Background(), c, server.URL+"/manifest", now); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTUIAutoNetworkCancellationLeavesOldVersionUsable(t *testing.T) {
	c := tuiUpdateFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := checkAndStageTUI(ctx, c, server.URL+"/manifest", time.Now()); err == nil {
		t.Fatal("timeout not returned")
	}
	if v, err := helperruntime.ReadActive(c.Root); err != nil || v != "v1.0.0" {
		t.Fatalf("%s %v", v, err)
	}
}

func readUpdateCheckForTest(root string) (tuiCheckState, error) {
	s, err := updatestate.OpenRead(root)
	if err != nil {
		return tuiCheckState{}, err
	}
	defer s.Close()
	return s.Check()
}
