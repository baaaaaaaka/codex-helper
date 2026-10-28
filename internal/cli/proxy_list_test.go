package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appgateway"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/localproxy"
)

func proxyListTestCmd(path string, ctx context.Context, out io.Writer) *cobra.Command {
	cmd := newProxyListCmd(&rootOptions{configPath: path})
	cmd.SetArgs([]string{})
	cmd.SetContext(ctx)
	cmd.SetOut(out)
	cmd.SetErr(io.Discard)
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	return cmd
}

func TestProxyListIncludesGatewayWithoutReinterpretingDeadLegacyRecord(t *testing.T) {
	lockCLITestHooks(t)
	dir := t.TempDir()
	oldDir, oldProbe, oldAlive := appGatewayRegistryDirFn, proxyListProbe, proxyProcessStatus
	t.Cleanup(func() { appGatewayRegistryDirFn, proxyListProbe, proxyProcessStatus = oldDir, oldProbe, oldAlive })
	appGatewayRegistryDirFn = func() (string, error) { return dir, nil }
	registry, err := appgateway.NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.EnsureWithPort(context.Background(), "p1", "fingerprint", 46843)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Ensure(context.Background(), "other-config", "other"); err != nil {
		t.Fatal(err)
	}
	proxyProcessStatus = func(int) (bool, error) { return false, nil }
	var probes atomic.Int32
	proxyListProbe = func(ctx context.Context, port int, timeout time.Duration) (appgateway.Health, error) {
		probes.Add(1)
		if port != reg.HTTPPort || timeout < time.Second {
			t.Errorf("probe port/timeout = %d/%s", port, timeout)
		}
		return appgateway.Health{OK: true, InstanceID: reg.ID}, nil
	}
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion,
		Profiles:  []config.Profile{{ID: "p1", Name: "ssh-proxy-pdx"}},
		Instances: []config.Instance{{ID: "old", ProfileID: "p1", Kind: "daemon", DaemonPID: 834, HTTPPort: 46843}},
	}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := proxyListTestCmd(store.Path(), context.Background(), &out).Execute(); err != nil {
		t.Fatal(err)
	}
	assertLineContains(t, out.String(), "old", "dead")
	assertLineContains(t, out.String(), reg.ID, "app-gateway", "ssh-proxy-pdx", "alive")
	if probes.Load() != 1 || strings.Contains(out.String(), "other-config") {
		t.Fatalf("unexpected probes/output: %d/%s", probes.Load(), &out)
	}
}

func TestProxyListStatesAndModelNames(t *testing.T) {
	lockCLITestHooks(t)
	now := time.Now()
	oldNow, oldAlive := proxyNow, proxyProcessStatus
	t.Cleanup(func() { proxyNow, proxyProcessStatus = oldNow, oldAlive })
	proxyNow = func() time.Time { return now }
	proxyProcessStatus = func(pid int) (bool, error) { return pid == 11, nil }
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion, Instances: []config.Instance{
		{ID: "placeholder", StartedAt: now},
		{ID: "startup-process", DaemonPID: 11, StartedAt: now},
		{ID: "old-unverified-pid", DaemonPID: 11},
		{ID: "expired-placeholder", StartedAt: now.Add(-time.Hour)},
		{ID: "blocked", RecoveryBudget: config.ProxyRecoveryBudget{Blocked: true, LastReason: "restart budget exceeded"}},
		{ID: "model", Kind: config.InstanceKindModelAdapter, ProfileID: "model-adapter:hash", ModelProfileName: "my-model"},
		{ID: "unified", Kind: config.InstanceKindModelAdapter, ProfileID: "model-adapter:catalog-hash", ModelUnified: true, ModelProfileName: "initial-model"},
	}}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := proxyListTestCmd(store.Path(), context.Background(), &out).Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range [][]string{
		{"placeholder", "starting"}, {"startup-process", "starting"}, {"old-unverified-pid", "unknown"},
		{"expired-placeholder", "dead"}, {"blocked", "blocked", "restart budget exceeded"}, {"model", "model-adapter", "my-model"},
		{"unified", "model-adapter", "unified models"},
	} {
		assertLineContains(t, out.String(), want...)
	}
	if strings.Contains(out.String(), "model-adapter:hash") || strings.Contains(out.String(), "initial-model") || strings.Contains(out.String(), "0001-01-01") {
		t.Fatalf("internal key/zero timestamp leaked: %s", &out)
	}
}

func TestProxyListPreservesHealthDetailsAndIdentity(t *testing.T) {
	lockCLITestHooks(t)
	oldProbe := proxyListProbe
	t.Cleanup(func() { proxyListProbe = oldProbe })
	for _, tc := range []struct {
		name           string
		health         appgateway.Health
		err            error
		status, detail string
	}{
		{name: "healthy", health: appgateway.Health{OK: true, InstanceID: "expected"}, status: "alive"},
		{name: "recovering", health: appgateway.Health{InstanceID: "expected", Recovery: "pending", Error: "waiting for VPN"}, status: "unhealthy", detail: "pending: waiting for VPN"},
		{name: "live block", health: appgateway.Health{InstanceID: "expected", Recovery: "blocked", Error: "restart budget exceeded"}, status: "blocked", detail: "blocked: restart budget exceeded"},
		{name: "other owner", health: appgateway.Health{OK: true, InstanceID: "replacement"}, status: "stale", detail: "replacement"},
		{name: "probe timeout", err: context.DeadlineExceeded, status: "unknown", detail: "deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxyListProbe = func(context.Context, int, time.Duration) (appgateway.Health, error) { return tc.health, tc.err }
			row := proxyListRow{id: "expected", http: 12345}
			inspectProxyListRow(context.Background(), &row)
			if row.status != tc.status || !strings.Contains(row.detail, tc.detail) {
				t.Fatalf("row = %+v", row)
			}
		})
	}
}

func TestProxyListReadOnlyAndDoesNotWaitForConfigLock(t *testing.T) {
	lockCLITestHooks(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := []byte(`{"version":6,"profiles":[]}`)
	if err := os.WriteFile(path, content, 0400); err != nil {
		t.Fatal(err)
	}
	// The diagnostic should not create a lock in the first place.
	if err := proxyListTestCmd(path, context.Background(), io.Discard).Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lock created: %v", err)
	}
	lock := flock.New(path + ".lock")
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := proxyListTestCmd(path, ctx, io.Discard).Execute(); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(dir, 0500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0700) })
		if err := proxyListTestCmd(path, ctx, io.Discard).Execute(); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, content) {
		t.Fatalf("config changed: %s/%v", after, err)
	}
	missing := filepath.Join(t.TempDir(), "not-created", "config.json")
	if err := proxyListTestCmd(missing, context.Background(), io.Discard).Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(missing)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing directory was created: %v", err)
	}
}

type proxyListFailWriter struct{}

func (proxyListFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestProxyListReturnsOutputErrors(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := proxyListTestCmd(store.Path(), context.Background(), proxyListFailWriter{}).Execute(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("error = %v", err)
	}
}

func TestProxyListBoundedConcurrencyAndCancellation(t *testing.T) {
	lockCLITestHooks(t)
	oldProbe, oldAlive := proxyListProbe, proxyProcessStatus
	t.Cleanup(func() { proxyListProbe, proxyProcessStatus = oldProbe, oldAlive })
	proxyProcessStatus = func(int) (bool, error) { return true, nil }
	entered := make(chan struct{}, proxyListWorkers)
	var calls atomic.Int32
	proxyListProbe = func(ctx context.Context, port int, timeout time.Duration) (appgateway.Health, error) {
		calls.Add(1)
		select {
		case entered <- struct{}{}:
		case <-ctx.Done():
			return appgateway.Health{}, ctx.Err()
		}
		<-ctx.Done()
		return appgateway.Health{}, ctx.Err()
	}
	cfg := config.Config{Version: config.CurrentVersion}
	for range 20 {
		cfg.Instances = append(cfg.Instances, config.Instance{ID: "slow", DaemonPID: 11, HTTPPort: 12345})
	}
	store := newTempStore(t)
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	cmd := proxyListTestCmd(store.Path(), ctx, io.Discard)
	go func() { done <- cmd.Execute() }()
	for range proxyListWorkers {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("independent probes did not start concurrently")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("list ignored cancellation")
	}
	if calls.Load() != proxyListWorkers {
		t.Fatalf("probes started after cancellation: %d", calls.Load())
	}
}

func TestProxyListAllowsServerHealthProbeBudget(t *testing.T) {
	lockCLITestHooks(t)
	p := localproxy.NewHTTPProxy(&net.Dialer{}, localproxy.Options{
		InstanceID: "slow-healthy",
		HealthProbe: func(ctx context.Context) error {
			select {
			case <-time.After(600 * time.Millisecond):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	addr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(context.Background())
	_, portString, _ := net.SplitHostPort(addr)
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion,
		Profiles: []config.Profile{{ID: "p1", Name: "profile", RouteTargetHost: "127.0.0.1", RouteTargetPort: 1}},
		Instances: []config.Instance{
			{ID: "slow-healthy", ProfileID: "p1", DaemonPID: os.Getpid(), HTTPPort: port},
		}}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := proxyListTestCmd(store.Path(), context.Background(), &out).Execute(); err != nil {
		t.Fatal(err)
	}
	assertLineContains(t, out.String(), "slow-healthy", "alive")
}

func TestProxyPrunePreservesStartupLeaseAndRecoveryFenceWithoutRewrite(t *testing.T) {
	lockCLITestHooks(t)
	now := time.Now()
	store := newTempStore(t)
	cfg := config.Config{Version: config.CurrentVersion, Instances: []config.Instance{
		{ID: "live-unhealthy", DaemonPID: os.Getpid(), HTTPPort: 1},
		{ID: "starting", StartedAt: now},
		{ID: "clock-moved-back", StartedAt: now.Add(time.Minute)},
		{ID: "leased", OwnerLeaseExpiresAt: now.Add(time.Minute)},
		{ID: "blocked", RecoveryBudget: config.ProxyRecoveryBudget{Blocked: true}},
	}}
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	cmd := newProxyPruneCmd(&rootOptions{configPath: store.Path()})
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("unchanged config was rewritten")
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Instances) != len(cfg.Instances) {
		t.Fatalf("protected instances pruned: %+v", got.Instances)
	}
}

func TestProxyListReportsRegistrationErrorsAlongsideOtherRows(t *testing.T) {
	lockCLITestHooks(t)
	dir := t.TempDir()
	oldDir := appGatewayRegistryDirFn
	t.Cleanup(func() { appGatewayRegistryDirFn = oldDir })
	appGatewayRegistryDirFn = func() (string, error) { return dir, nil }
	registry, err := appgateway.NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.Ensure(context.Background(), "p1", "fp")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, reg.ID+".json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{{ID: "p1", Name: "profile"}}, Instances: []config.Instance{{ID: "dead"}}}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = proxyListTestCmd(store.Path(), context.Background(), &out).Execute()
	if err == nil || !strings.Contains(err.Error(), "read app gateway") {
		t.Fatalf("error = %v", err)
	}
	assertLineContains(t, out.String(), "dead", "dead")
}

func TestProxyListDoesNotCreateMissingGatewayRegistry(t *testing.T) {
	lockCLITestHooks(t)
	dir := filepath.Join(t.TempDir(), "missing")
	oldDir := appGatewayRegistryDirFn
	t.Cleanup(func() { appGatewayRegistryDirFn = oldDir })
	appGatewayRegistryDirFn = func() (string, error) { return dir, nil }
	path := filepath.Join(t.TempDir(), "config.json")
	content, _ := json.Marshal(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{{ID: "p1", Name: "profile"}}})
	if err := os.WriteFile(path, content, 0400); err != nil {
		t.Fatal(err)
	}
	if err := proxyListTestCmd(path, context.Background(), io.Discard).Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("registry created: %v", err)
	}
}

func TestProxyListProcessInspectionFailureIsUnknownAndPrunePreservesIt(t *testing.T) {
	lockCLITestHooks(t)
	oldStatus := proxyProcessStatus
	t.Cleanup(func() { proxyProcessStatus = oldStatus })
	proxyProcessStatus = func(int) (bool, error) { return false, os.ErrPermission }
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion, Instances: []config.Instance{
		{ID: "cannot-inspect", DaemonPID: 11, HTTPPort: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := proxyListTestCmd(store.Path(), context.Background(), &out).Execute(); err != nil {
		t.Fatal(err)
	}
	assertLineContains(t, out.String(), "cannot-inspect", "unknown", "permission denied")
	prune := newProxyPruneCmd(&rootOptions{configPath: store.Path()})
	prune.SetArgs([]string{})
	prune.SetOut(io.Discard)
	if err := prune.Execute(); err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil || len(cfg.Instances) != 1 {
		t.Fatalf("uncertain record removed: %+v/%v", cfg, err)
	}
}

func TestProxyListTotalBudgetBoundsQueuedProbes(t *testing.T) {
	lockCLITestHooks(t)
	oldProbe, oldStatus := proxyListProbe, proxyProcessStatus
	t.Cleanup(func() { proxyListProbe, proxyProcessStatus = oldProbe, oldStatus })
	proxyProcessStatus = func(int) (bool, error) { return true, nil }
	var calls atomic.Int32
	proxyListProbe = func(ctx context.Context, _ int, timeout time.Duration) (appgateway.Health, error) {
		calls.Add(1)
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		<-probeCtx.Done()
		return appgateway.Health{}, probeCtx.Err()
	}
	cfg := config.Config{Version: config.CurrentVersion}
	for range 100 {
		cfg.Instances = append(cfg.Instances, config.Instance{ID: "slow", DaemonPID: 11, HTTPPort: 12345})
	}
	store := newTempStore(t)
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := proxyListTestCmd(store.Path(), context.Background(), io.Discard).Execute(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > proxyListTimeout+3*time.Second {
		t.Fatalf("overall budget exceeded: %s", elapsed)
	}
	if calls.Load() >= 100 {
		t.Fatalf("started all %d probes despite overall budget", calls.Load())
	}
}

func TestProxyListOutputCannotInjectTableRows(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{Version: config.CurrentVersion, Instances: []config.Instance{
		{ID: "bad\nrow", Kind: "future\ttype", RecoveryBudget: config.ProxyRecoveryBudget{Blocked: true, LastReason: "reason\nnext\x1b[2J"}},
	}}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := proxyListTestCmd(store.Path(), context.Background(), &out).Execute(); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "\n") != 2 || strings.Contains(out.String(), "\x1b") {
		t.Fatalf("unsafe table output: %q", out.String())
	}
}

func BenchmarkProxyListLargeRegistry(b *testing.B) {
	lockCLITestHooks(b)
	oldStatus, oldAlive, oldDir := proxyProcessStatus, proxyProcessAlive, appGatewayRegistryDirFn
	b.Cleanup(func() { proxyProcessStatus, proxyProcessAlive, appGatewayRegistryDirFn = oldStatus, oldAlive, oldDir })
	proxyProcessStatus = func(int) (bool, error) { return false, nil }
	proxyProcessAlive = func(int) bool { return false }
	dir := filepath.Join(b.TempDir(), "absent-gateways")
	appGatewayRegistryDirFn = func() (string, error) { return dir, nil }
	cfg := config.Config{Version: config.CurrentVersion}
	for i := range 100 {
		cfg.Profiles = append(cfg.Profiles, config.Profile{ID: fmt.Sprintf("p-%d", i), Name: fmt.Sprintf("profile-%d", i)})
	}
	for i := range 10000 {
		cfg.Instances = append(cfg.Instances, config.Instance{ID: fmt.Sprintf("instance-%d", i), Kind: config.InstanceKindDaemon,
			ProfileID: cfg.Profiles[i%len(cfg.Profiles)].ID, DaemonPID: 1, HTTPPort: 12345,
			LastSeenAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
	}
	store, err := config.NewStore(filepath.Join(b.TempDir(), "config.json"))
	if err != nil {
		b.Fatal(err)
	}
	if err := store.Save(cfg); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := proxyListTestCmd(store.Path(), context.Background(), io.Discard).Execute(); err != nil {
			b.Fatal(err)
		}
	}
}
