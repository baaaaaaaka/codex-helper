package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appgateway"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
)

func TestRunAppGatewayDaemonKeepsStableFrontendWhileBackendRuns(t *testing.T) {
	stateDir := t.TempDir()
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	profile := config.Profile{ID: "profile-a", Name: "profile-a", Host: "127.0.0.1", Port: 1}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{profile}}); err != nil {
		t.Fatal(err)
	}

	oldProbe := appGatewayProbeHostNetwork
	oldStart := proxyStartStack
	t.Cleanup(func() {
		appGatewayProbeHostNetwork = oldProbe
		proxyStartStack = oldStart
	})
	appGatewayProbeHostNetwork = func(context.Context, config.Profile) error { return nil }
	fatalCh := make(chan error, 1)
	closeCalls := make(chan struct{}, 1)
	optionsCh := make(chan stack.Options, 1)
	proxyStartStack = func(_ config.Profile, _ string, opts stack.Options) (proxyStartedStack, error) {
		select {
		case optionsCh <- opts:
		default:
		}
		return proxyStartedStack{
			socksPort:        40123,
			currentSocksPort: func() int { return 40123 },
			fatalCh:          fatalCh,
			close: func(context.Context) error {
				select {
				case closeCalls <- struct{}{}:
				default:
				}
				return nil
			},
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runAppGatewayDaemon(ctx, store, profile.ID, stateDir) }()

	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	var reg appgateway.Registration
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		reg, err = registry.Load(profile.ID)
		if err == nil && reg.State == appgateway.StateReady {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || reg.State != appgateway.StateReady {
		t.Fatalf("gateway did not become ready: reg=%#v err=%v", reg, err)
	}
	stablePort := reg.HTTPPort
	health, err := appgateway.Probe(context.Background(), stablePort, time.Second)
	if err != nil || health.InstanceID != reg.ID {
		t.Fatalf("stable frontend probe = %#v/%v", health, err)
	}
	select {
	case opts := <-optionsCh:
		if opts.MaxRestarts != 3 || opts.MaxRequestFailureRecoveries != 3 || opts.RestartBackoff != 2*time.Second {
			t.Fatalf("child retry policy = %#v", opts)
		}
	case <-time.After(time.Second):
		t.Fatal("backend options were not captured")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon exit = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop")
	}
	select {
	case <-closeCalls:
	case <-time.After(time.Second):
		t.Fatal("backend close was not called")
	}
	latest, err := registry.Load(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	if latest.HTTPPort != stablePort || latest.OwnerPID != 0 || latest.State != appgateway.StatePending {
		t.Fatalf("shutdown registration = %#v", latest)
	}
}

func TestRunAppGatewayDaemonDoesNotConsumeLegacyBlockedBudget(t *testing.T) {
	stateDir := t.TempDir()
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	profile := config.Profile{ID: "profile-a", Name: "profile-a", Host: "127.0.0.1", Port: 1}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{profile}, Instances: []config.Instance{{ID: "old", ProfileID: profile.ID, RecoveryBudget: config.ProxyRecoveryBudget{Blocked: true}}}}); err != nil {
		t.Fatal(err)
	}
	oldProbe := appGatewayProbeHostNetwork
	oldStart := proxyStartStack
	t.Cleanup(func() {
		appGatewayProbeHostNetwork = oldProbe
		proxyStartStack = oldStart
	})
	appGatewayProbeHostNetwork = func(context.Context, config.Profile) error { return errors.New("DNS not ready") }
	proxyStartStack = func(config.Profile, string, stack.Options) (proxyStartedStack, error) {
		t.Fatal("backend must not start while host admission is failing")
		return proxyStartedStack{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runAppGatewayDaemon(ctx, store, profile.ID, stateDir) }()
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var reg appgateway.Registration
	for time.Now().Before(deadline) {
		reg, err = registry.Load(profile.ID)
		if err == nil && reg.RecoveryAttempts > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon exit = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop")
	}
	if err != nil {
		t.Fatal(err)
	}
	if reg.RecoveryBlocked {
		t.Fatalf("gateway inherited a blocked legacy budget: %#v", reg)
	}
}

func TestRunAppGatewayDaemonModernStandbyDNSGapThenRecoveryKeepsClientPort(t *testing.T) {
	stateDir := t.TempDir()
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	profile := config.Profile{ID: "profile-a", Name: "profile-a", Host: "corp-alias", Port: 22}
	legacyBudget := config.ProxyRecoveryBudget{Blocked: true, RequestAttempts: 7}
	if err := store.Save(config.Config{
		Version:  config.CurrentVersion,
		Profiles: []config.Profile{profile},
		Instances: []config.Instance{{
			ID:             "legacy-instance",
			ProfileID:      profile.ID,
			HTTPPort:       3804,
			RecoveryBudget: legacyBudget,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	var probeCalls atomic.Int32
	firstProbeStarted := make(chan struct{})
	releaseFirstProbe := make(chan struct{})
	var firstProbeOnce atomic.Bool
	secondRecoveryProbeStarted := make(chan struct{})
	releaseSecondRecoveryProbe := make(chan struct{})
	var secondRecoveryProbeOnce atomic.Bool
	probe := func(ctx context.Context, _ config.Profile) error {
		call := probeCalls.Add(1)
		if call == 1 {
			if firstProbeOnce.CompareAndSwap(false, true) {
				close(firstProbeStarted)
			}
			select {
			case <-releaseFirstProbe:
			case <-ctx.Done():
				return ctx.Err()
			}
			return errors.New("enterprise DNS is still recovering")
		}
		if call == 3 {
			if secondRecoveryProbeOnce.CompareAndSwap(false, true) {
				close(secondRecoveryProbeStarted)
			}
			select {
			case <-releaseSecondRecoveryProbe:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}

	var startCalls atomic.Int32
	var firstFatal chan error
	var firstClose atomic.Int32
	var secondClose atomic.Int32
	startStack := func(_ config.Profile, _ string, opts stack.Options) (proxyStartedStack, error) {
		if opts.MaxRestarts != 3 || opts.MaxRequestFailureRecoveries != 3 {
			t.Fatalf("backend retry policy = %#v", opts)
		}
		call := startCalls.Add(1)
		fatal := make(chan error, 1)
		if call == 1 {
			firstFatal = fatal
		}
		closeFn := func(context.Context) error {
			if call == 1 {
				firstClose.Add(1)
			} else {
				secondClose.Add(1)
			}
			return nil
		}
		return proxyStartedStack{
			socksPort:        40123 + int(call) - 1,
			currentSocksPort: func() int { return 40123 + int(call) - 1 },
			fatalCh:          fatal,
			close:            closeFn,
		}, nil
	}

	deps := appGatewayDaemonDeps{
		probeHostNetwork: probe,
		startStack:       startStack,
		sleep: func(ctx context.Context, _ time.Duration) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runAppGatewayDaemonWithDeps(ctx, store, profile.ID, stateDir, deps)
	}()

	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstProbeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the DNS-gap probe")
	}
	reg, err := registry.Load(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	stablePort := reg.HTTPPort
	if stablePort <= 0 {
		t.Fatalf("stable port = %d", stablePort)
	}
	health, err := appgateway.Probe(context.Background(), stablePort, time.Second)
	if err != nil || health.OK || health.InstanceID != reg.ID {
		t.Fatalf("DNS-gap health = %#v/%v, want identified 503", health, err)
	}
	if startCalls.Load() != 0 {
		t.Fatalf("backend started before DNS recovery: %d", startCalls.Load())
	}

	close(releaseFirstProbe)
	waitForAppGatewayRegistration(t, registry, profile.ID, func(got appgateway.Registration) bool {
		return got.State == appgateway.StateReady && got.BackendGeneration != ""
	})
	if startCalls.Load() != 1 || firstFatal == nil {
		t.Fatalf("first backend start/fatal channel = %d/%v", startCalls.Load(), firstFatal != nil)
	}
	ready, err := registry.Load(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ready.HTTPPort != stablePort {
		t.Fatalf("port changed after DNS recovery: %d -> %d", stablePort, ready.HTTPPort)
	}
	health = waitForAppGatewayHealth(t, stablePort, true)
	err = nil
	if err != nil || !health.OK || health.InstanceID != ready.ID {
		t.Fatalf("ready health = %#v/%v", health, err)
	}

	firstFatal <- errors.New("SSH connection reset")
	waitForAppGatewayRegistration(t, registry, profile.ID, func(got appgateway.Registration) bool {
		return got.State == appgateway.StatePending && got.LastError != ""
	})
	health = waitForAppGatewayHealth(t, stablePort, false)
	err = nil
	if err != nil || health.OK || health.InstanceID != ready.ID {
		t.Fatalf("post-reset health = %#v/%v, want identified 503 on same port", health, err)
	}
	select {
	case <-secondRecoveryProbeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second recovery probe")
	}
	close(releaseSecondRecoveryProbe)

	waitForAppGatewayRegistration(t, registry, profile.ID, func(got appgateway.Registration) bool {
		return got.State == appgateway.StateReady && got.BackendGeneration != ready.BackendGeneration
	})
	latest, err := registry.Load(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	if latest.HTTPPort != stablePort || startCalls.Load() != 2 {
		t.Fatalf("post-reset registration/start count = %#v/%d", latest, startCalls.Load())
	}
	health = waitForAppGatewayHealth(t, stablePort, true)
	err = nil
	if err != nil || !health.OK {
		t.Fatalf("recovered health = %#v/%v", health, err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon exit = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not stop")
	}
	if firstClose.Load() != 1 || secondClose.Load() != 1 {
		t.Fatalf("backend close counts = first %d second %d", firstClose.Load(), secondClose.Load())
	}
	finalCfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(finalCfg.Instances) != 1 || finalCfg.Instances[0].RecoveryBudget != legacyBudget {
		t.Fatalf("legacy budget/instance changed: %#v", finalCfg.Instances)
	}
	eventFiles, err := filepath.Glob(filepath.Join(stateDir, "*.jsonl"))
	if err != nil || len(eventFiles) != 1 {
		t.Fatalf("event files = %v/%v", eventFiles, err)
	}
	events, err := os.ReadFile(eventFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"network-not-ready", "backend-ready", "backend-exited"} {
		if !strings.Contains(string(events), `"event":"`+want+`"`) {
			t.Fatalf("event log missing %q: %s", want, events)
		}
	}
}

func TestRunAppGatewayDaemonBoundsBackendRecoveryBeforeCooldown(t *testing.T) {
	stateDir := t.TempDir()
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	profile := config.Profile{ID: "profile-a", Name: "profile-a", Host: "corp-alias", Port: 22}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{profile}}); err != nil {
		t.Fatal(err)
	}
	var starts atomic.Int32
	var probes atomic.Int32
	var sleeps atomic.Int32
	blockedReached := make(chan struct{})
	var blockedOnce atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps := appGatewayDaemonDeps{
		probeHostNetwork: func(context.Context, config.Profile) error {
			probes.Add(1)
			return nil
		},
		startStack: func(config.Profile, string, stack.Options) (proxyStartedStack, error) {
			starts.Add(1)
			return proxyStartedStack{}, errors.New("SSH backend unavailable")
		},
		sleep: func(sleepCtx context.Context, _ time.Duration) error {
			if sleeps.Add(1) == appGatewayBackendMaxAttempts+1 {
				if blockedOnce.CompareAndSwap(false, true) {
					close(blockedReached)
				}
				<-sleepCtx.Done()
			}
			return sleepCtx.Err()
		},
	}
	done := make(chan error, 1)
	go func() { done <- runAppGatewayDaemonWithDeps(ctx, store, profile.ID, stateDir, deps) }()
	select {
	case <-blockedReached:
		registry, registryErr := appgateway.NewRegistry(stateDir)
		if registryErr != nil {
			t.Fatal(registryErr)
		}
		blocked := waitForAppGatewayRegistration(t, registry, profile.ID, func(reg appgateway.Registration) bool {
			return reg.State == appgateway.StateBlocked && reg.RecoveryBlocked
		})
		health, healthErr := appgateway.Probe(context.Background(), blocked.HTTPPort, time.Second)
		if healthErr != nil || health.OK || health.InstanceID != blocked.ID {
			t.Fatalf("blocked gateway health = %#v/%v, want identified 503", health, healthErr)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("daemon exit = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("blocked daemon did not stop")
		}
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon exit = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recovery loop did not reach cooldown")
	}
	if starts.Load() != appGatewayBackendMaxAttempts || probes.Load() != appGatewayBackendMaxAttempts || sleeps.Load() != appGatewayBackendMaxAttempts+1 {
		t.Fatalf("recovery bounds starts/probes/sleeps = %d/%d/%d", starts.Load(), probes.Load(), sleeps.Load())
	}
	files, err := filepath.Glob(filepath.Join(stateDir, "*.jsonl"))
	if err != nil || len(files) != 1 {
		t.Fatalf("event files = %v/%v", files, err)
	}
	events, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(events), `"event":"backend-start-failed"`); got != appGatewayBackendMaxAttempts {
		t.Fatalf("backend failure events = %d, want %d", got, appGatewayBackendMaxAttempts)
	}
	if got := strings.Count(string(events), `"event":"recovery-blocked"`); got != 1 {
		t.Fatalf("recovery blocked events = %d, want one", got)
	}
}

func TestRunAppGatewayDaemonBackendSwapKeepsFrontendPort(t *testing.T) {
	stateDir := t.TempDir()
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	profile := config.Profile{ID: "profile-a", Name: "profile-a", Host: "corp-alias", Port: 22}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{profile}}); err != nil {
		t.Fatal(err)
	}
	var currentPort atomic.Int32
	currentPort.Store(40123)
	fatal := make(chan error, 1)
	tick := make(chan time.Time, 1)
	tickerStopped := make(chan struct{})
	deps := appGatewayDaemonDeps{
		probeHostNetwork: func(context.Context, config.Profile) error { return nil },
		startStack: func(config.Profile, string, stack.Options) (proxyStartedStack, error) {
			return proxyStartedStack{
				socksPort:        int(currentPort.Load()),
				currentSocksPort: func() int { return int(currentPort.Load()) },
				fatalCh:          fatal,
				close:            func(context.Context) error { return nil },
			}, nil
		},
		sleep: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
		newTicker: func(time.Duration) appGatewayTicker {
			return appGatewayTicker{C: tick, stop: func() {
				select {
				case <-tickerStopped:
				default:
					close(tickerStopped)
				}
			}}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runAppGatewayDaemonWithDeps(ctx, store, profile.ID, stateDir, deps) }()
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	ready := waitForAppGatewayRegistration(t, registry, profile.ID, func(reg appgateway.Registration) bool {
		return reg.State == appgateway.StateReady
	})
	stablePort := ready.HTTPPort
	firstGeneration := ready.BackendGeneration
	currentPort.Store(40124)
	tick <- time.Now()
	updated := waitForAppGatewayRegistration(t, registry, profile.ID, func(reg appgateway.Registration) bool {
		return reg.State == appgateway.StateReady && reg.BackendGeneration != firstGeneration
	})
	if updated.HTTPPort != stablePort {
		t.Fatalf("backend swap changed client port: %d -> %d", stablePort, updated.HTTPPort)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon exit = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not stop")
	}
}

func TestRunAppGatewayDaemonRestartReusesStablePort(t *testing.T) {
	stateDir := t.TempDir()
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	profile := config.Profile{ID: "profile-a", Name: "profile-a", Host: "corp-alias", Port: 22}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Profiles: []config.Profile{profile}}); err != nil {
		t.Fatal(err)
	}
	deps := appGatewayDaemonDeps{
		probeHostNetwork: func(context.Context, config.Profile) error { return nil },
		startStack: func(config.Profile, string, stack.Options) (proxyStartedStack, error) {
			return proxyStartedStack{
				socksPort:        40123,
				currentSocksPort: func() int { return 40123 },
				fatalCh:          make(chan error),
				close:            func(context.Context) error { return nil },
			}, nil
		},
		sleep: func(ctx context.Context, _ time.Duration) error { return ctx.Err() },
	}
	registry, err := appgateway.NewRegistry(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	runOnce := func() int {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- runAppGatewayDaemonWithDeps(ctx, store, profile.ID, stateDir, deps) }()
		ready := waitForAppGatewayRegistration(t, registry, profile.ID, func(reg appgateway.Registration) bool {
			return reg.State == appgateway.StateReady
		})
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("daemon restart run exit = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("daemon restart run did not stop")
		}
		return ready.HTTPPort
	}
	firstPort := runOnce()
	secondPort := runOnce()
	if firstPort != secondPort {
		t.Fatalf("daemon restart changed stable port: first=%d second=%d", firstPort, secondPort)
	}
}

func waitForAppGatewayRegistration(t *testing.T, registry *appgateway.Registry, profileID string, predicate func(appgateway.Registration) bool) appgateway.Registration {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last appgateway.Registration
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = registry.Load(profileID)
		if lastErr == nil && predicate(last) {
			return last
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for registration: %#v/%v", last, lastErr)
	return appgateway.Registration{}
}

func waitForAppGatewayHealth(t *testing.T, port int, wantOK bool) appgateway.Health {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last appgateway.Health
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = appgateway.Probe(context.Background(), port, time.Second)
		if lastErr == nil && last.OK == wantOK {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for health=%t: %#v/%v", wantOK, last, lastErr)
	return appgateway.Health{}
}
