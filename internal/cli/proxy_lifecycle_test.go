package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
)

func TestProxyStartForegroundRecordsInstanceAndRunsDaemon(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:   "p1",
			Name: "dev",
			Host: "host",
			Port: 22,
			User: "alice",
		}},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	prevRunDaemon := runProxyDaemonFunc
	t.Cleanup(func() { runProxyDaemonFunc = prevRunDaemon })

	var gotInstanceID string
	runProxyDaemonFunc = func(_ context.Context, gotStore *config.Store, instanceID string) error {
		gotInstanceID = instanceID
		if gotStore.Path() != store.Path() {
			t.Fatalf("expected store path %q, got %q", store.Path(), gotStore.Path())
		}
		return nil
	}

	cmd := newProxyStartCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--foreground"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute proxy start: %v", err)
	}
	if gotInstanceID == "" {
		t.Fatal("expected daemon launch to receive an instance id")
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 1 {
		t.Fatalf("expected 1 recorded instance, got %+v", cfg.Instances)
	}
	if cfg.Instances[0].ID != gotInstanceID || cfg.Instances[0].ProfileID != "p1" {
		t.Fatalf("unexpected recorded instance: %+v", cfg.Instances[0])
	}
}

func TestRunProxyDaemonRemovesInstanceOnFatal(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	inst := config.Instance{ID: "inst-1", ProfileID: "p1"}
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:   "p1",
			Name: "dev",
			Host: "host",
			Port: 22,
			User: "alice",
		}},
		Instances: []config.Instance{inst},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	prevStart := proxyStartStack
	prevNow := proxyNow
	prevRecord := proxyRecordInstance
	prevRemove := proxyRemoveInstance
	t.Cleanup(func() {
		proxyStartStack = prevStart
		proxyNow = prevNow
		proxyRecordInstance = prevRecord
		proxyRemoveInstance = prevRemove
	})

	fixedNow := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	proxyNow = func() time.Time { return fixedNow }

	fatalErr := errors.New("stack failed")
	fatalCh := make(chan error, 1)
	fatalCh <- fatalErr
	closed := false
	proxyStartStack = func(profile config.Profile, instanceID string, opts stack.Options) (proxyStartedStack, error) {
		if profile.ID != "p1" || instanceID != "inst-1" {
			t.Fatalf("unexpected start args: profile=%+v instanceID=%q", profile, instanceID)
		}
		if opts.SocksPort != 0 || opts.HTTPListenAddr != "" {
			t.Fatalf("unexpected stack options: %+v", opts)
		}
		return proxyStartedStack{
			httpPort:  18080,
			socksPort: 19080,
			fatalCh:   fatalCh,
			close: func(context.Context) error {
				closed = true
				return nil
			},
		}, nil
	}

	var recorded config.Instance
	proxyRecordInstance = func(s *config.Store, inst config.Instance) error {
		recorded = inst
		return manager.RecordInstance(s, inst)
	}

	removed := []string{}
	proxyRemoveInstance = func(s *config.Store, instanceID string) error {
		removed = append(removed, instanceID)
		return manager.RemoveInstance(s, instanceID)
	}

	err := runProxyDaemon(context.Background(), store, "inst-1")
	if !errors.Is(err, fatalErr) {
		t.Fatalf("expected fatal error, got %v", err)
	}
	if !closed {
		t.Fatal("expected started stack to be closed")
	}
	if recorded.HTTPPort != 18080 || recorded.SocksPort != 19080 {
		t.Fatalf("expected recorded ports to be updated, got %+v", recorded)
	}
	if recorded.LastSeenAt != fixedNow {
		t.Fatalf("expected fixed heartbeat time, got %v", recorded.LastSeenAt)
	}
	if len(removed) != 1 || removed[0] != "inst-1" {
		t.Fatalf("expected instance removal, got %v", removed)
	}

	cfg, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("load config: %v", loadErr)
	}
	if len(cfg.Instances) != 0 {
		t.Fatalf("expected instance removal after fatal error, got %+v", cfg.Instances)
	}
}

func TestRunProxyDaemonRemovesInstanceOnContextCancel(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:   "p1",
			Name: "dev",
			Host: "host",
			Port: 22,
			User: "alice",
		}},
		Instances: []config.Instance{{ID: "inst-1", ProfileID: "p1"}},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	prevStart := proxyStartStack
	prevRemove := proxyRemoveInstance
	t.Cleanup(func() {
		proxyStartStack = prevStart
		proxyRemoveInstance = prevRemove
	})

	closed := false
	proxyStartStack = func(config.Profile, string, stack.Options) (proxyStartedStack, error) {
		return proxyStartedStack{
			httpPort:  18080,
			socksPort: 19080,
			fatalCh:   make(chan error),
			close: func(context.Context) error {
				closed = true
				return nil
			},
		}, nil
	}

	removed := []string{}
	proxyRemoveInstance = func(s *config.Store, instanceID string) error {
		removed = append(removed, instanceID)
		return manager.RemoveInstance(s, instanceID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runProxyDaemon(ctx, store, "inst-1"); err != nil {
		t.Fatalf("expected nil on canceled context, got %v", err)
	}
	if !closed {
		t.Fatal("expected started stack to be closed")
	}
	if len(removed) != 1 || removed[0] != "inst-1" {
		t.Fatalf("expected instance removal, got %v", removed)
	}
}

func TestRunProxyDaemonReturnsErrorWhenProfileMissing(t *testing.T) {
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version:   config.CurrentVersion,
		Instances: []config.Instance{{ID: "inst-1", ProfileID: "missing"}},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	err := runProxyDaemon(context.Background(), store, "inst-1")
	if err == nil || !strings.Contains(err.Error(), `profile "missing" not found`) {
		t.Fatalf("expected missing-profile error, got %v", err)
	}
}

func TestProxyListReportsAliveUnhealthyAndDead(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	ts := time.Date(2026, 3, 10, 13, 0, 0, 0, time.UTC)
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Profiles: []config.Profile{{
			ID:   "p1",
			Name: "dev",
		}},
		Instances: []config.Instance{
			{ID: "inst-alive", ProfileID: "p1", DaemonPID: 11, HTTPPort: 18081, LastSeenAt: ts},
			{ID: "inst-bad", ProfileID: "p1", DaemonPID: 22, HTTPPort: 18082, LastSeenAt: ts},
			{ID: "inst-dead", ProfileID: "missing", DaemonPID: 33, HTTPPort: 18083, LastSeenAt: ts},
		},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	prevAlive := proxyProcessAlive
	prevCheck := proxyCheckHTTPProxy
	t.Cleanup(func() {
		proxyProcessAlive = prevAlive
		proxyCheckHTTPProxy = prevCheck
	})

	proxyProcessAlive = func(pid int) bool { return pid != 33 }
	proxyCheckHTTPProxy = func(_ manager.HealthClient, port int, expectedInstanceID string) error {
		if port == 18082 {
			return errors.New("health failed")
		}
		if expectedInstanceID == "" {
			t.Fatal("expected instance id for health check")
		}
		return nil
	}

	cmd := newProxyListCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute proxy list: %v", err)
	}

	text := out.String()
	assertLineContains(t, text, "inst-alive", "alive")
	assertLineContains(t, text, "inst-bad", "unhealthy")
	assertLineContains(t, text, "inst-dead", "dead")
	assertLineContains(t, text, "inst-alive", "dev")
	assertLineContains(t, text, "inst-dead", "missing")
}

func TestProxyPruneRemovesDeadAndUnhealthyInstances(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version: config.CurrentVersion,
		Instances: []config.Instance{
			{ID: "inst-ok", DaemonPID: 11, HTTPPort: 18081},
			{ID: "inst-bad", DaemonPID: 22, HTTPPort: 18082},
			{ID: "inst-dead", DaemonPID: 0, HTTPPort: 18083},
		},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	prevAlive := proxyProcessAlive
	prevCheck := proxyCheckHTTPProxy
	t.Cleanup(func() {
		proxyProcessAlive = prevAlive
		proxyCheckHTTPProxy = prevCheck
	})

	proxyProcessAlive = func(pid int) bool { return pid != 0 }
	proxyCheckHTTPProxy = func(_ manager.HealthClient, port int, _ string) error {
		if port == 18082 {
			return errors.New("health failed")
		}
		return nil
	}

	cmd := newProxyPruneCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute proxy prune: %v", err)
	}
	if !strings.Contains(out.String(), "Pruned 2 instances") {
		t.Fatalf("expected prune summary, got %q", out.String())
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 1 || cfg.Instances[0].ID != "inst-ok" {
		t.Fatalf("expected only healthy instance to remain, got %+v", cfg.Instances)
	}
}

func TestProxyStopTerminatesAliveInstanceAndRemovesConfigEntry(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version:   config.CurrentVersion,
		Instances: []config.Instance{{ID: "inst-1", DaemonPID: 4242}},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	prevAlive := proxyProcessAlive
	prevFind := proxyFindProcess
	prevTerminate := proxyTerminate
	prevRemove := proxyRemoveInstance
	t.Cleanup(func() {
		proxyProcessAlive = prevAlive
		proxyFindProcess = prevFind
		proxyTerminate = prevTerminate
		proxyRemoveInstance = prevRemove
	})

	proxyProcessAlive = func(int) bool { return true }
	proxyFindProcess = func(pid int) (*os.Process, error) {
		return &os.Process{Pid: pid}, nil
	}

	terminatedPID := 0
	proxyTerminate = func(p *os.Process, _ time.Duration) error {
		if p == nil {
			t.Fatal("expected process")
		}
		terminatedPID = p.Pid
		return nil
	}
	proxyRemoveInstance = func(s *config.Store, instanceID string) error {
		return manager.RemoveInstance(s, instanceID)
	}

	cmd := newProxyStopCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"inst-1"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute proxy stop: %v", err)
	}
	if terminatedPID != 4242 {
		t.Fatalf("expected terminate pid 4242, got %d", terminatedPID)
	}
	if !strings.Contains(out.String(), "Stopped instance inst-1") {
		t.Fatalf("unexpected output: %q", out.String())
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 0 {
		t.Fatalf("expected instance removal, got %+v", cfg.Instances)
	}
}

func TestProxyDoctorReportsMissingTools(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	prevLookPath := proxyLookPath
	t.Cleanup(func() { proxyLookPath = prevLookPath })

	proxyLookPath = func(name string) (string, error) {
		if name == "ssh" {
			return "/usr/bin/ssh", nil
		}
		return "", errors.New("missing")
	}

	cmd := newProxyDoctorCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute proxy doctor: %v", err)
	}

	text := out.String()
	if !strings.Contains(text, "Issues found:") {
		t.Fatalf("expected doctor issues output, got %q", text)
	}
	if !strings.Contains(text, "missing `codex`") || !strings.Contains(text, "missing `node`") {
		t.Fatalf("expected missing tool hints, got %q", text)
	}
	if !strings.Contains(text, "Install hints:") {
		t.Fatalf("expected install hints, got %q", text)
	}
}

func TestProxyDaemonCmdUsesInstanceFlag(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	prevRunDaemon := runProxyDaemonFunc
	t.Cleanup(func() { runProxyDaemonFunc = prevRunDaemon })

	gotID := ""
	runProxyDaemonFunc = func(_ context.Context, gotStore *config.Store, instanceID string) error {
		gotID = instanceID
		if gotStore.Path() != store.Path() {
			t.Fatalf("expected store path %q, got %q", store.Path(), gotStore.Path())
		}
		return nil
	}

	cmd := newProxyDaemonCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--instance-id", "inst-daemon"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute proxy daemon cmd: %v", err)
	}
	if gotID != "inst-daemon" {
		t.Fatalf("expected instance flag to propagate, got %q", gotID)
	}
}

func assertLineContains(t *testing.T, text string, fragments ...string) {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		matched := true
		for _, fragment := range fragments {
			if !strings.Contains(line, fragment) {
				matched = false
				break
			}
		}
		if matched {
			return
		}
	}
	t.Fatalf("expected a line containing %v in output:\n%s", fragments, text)
}
