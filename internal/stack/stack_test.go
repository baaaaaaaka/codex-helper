package stack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/localproxy"
	"github.com/baaaaaaaka/codex-helper/internal/ssh"
)

func TestPickFreePort_IsBindable(t *testing.T) {
	port, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}

	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", intToString(port)))
	if err != nil {
		t.Fatalf("listen on picked port %d: %v", port, err)
	}
	_ = ln.Close()
}

func TestWaitForTCP_SucceedsWhenListening(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	if err := waitForTCP(ln.Addr().String(), 1*time.Second); err != nil {
		t.Fatalf("waitForTCP: %v", err)
	}
}

func TestWaitForTCP_TimesOutAndIncludesCause(t *testing.T) {
	addr := "127.0.0.1:0"
	err := waitForTCP(addr, 150*time.Millisecond)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "timeout waiting for") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), addr) {
		t.Fatalf("expected addr in error: %v", err)
	}
}

func TestWaitForTCPTunnel_TimesOutWhenNoTunnelAndNotListening(t *testing.T) {
	addr := "127.0.0.1:0"
	err := waitForTCPTunnel(addr, 150*time.Millisecond, nil)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "timeout waiting for") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), addr) {
		t.Fatalf("expected addr in error: %v", err)
	}
}

func TestWaitForTCPTunnel_ReturnsWhenTunnelExits(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "ssh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}
	t.Setenv("PATH", dir)

	port, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	tun, err := ssh.NewTunnel(ssh.TunnelConfig{
		Host:      "example.com",
		Port:      22,
		User:      "alice",
		SocksPort: port,
	})
	if err != nil {
		t.Fatalf("NewTunnel error: %v", err)
	}
	if err := tun.Start(); err != nil {
		t.Fatalf("Start error: %v", err)
	}

	err = waitForTCPTunnel(addr, 500*time.Millisecond, tun)
	if err == nil {
		t.Fatalf("expected early tunnel exit error")
	}
	if !strings.Contains(err.Error(), "ssh tunnel exited") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForSOCKS5_SucceedsAfterGreeting(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		greeting := make([]byte, 3)
		if _, err := io.ReadFull(conn, greeting); err == nil {
			_, _ = conn.Write([]byte{5, 0})
		}
	}()

	if err := waitForSOCKS5(ln.Addr().String(), time.Second, nil); err != nil {
		t.Fatalf("waitForSOCKS5: %v", err)
	}
	_ = ln.Close()
	<-done
}

func TestNotifyNetworkResumeCoalesces(t *testing.T) {
	s := &Stack{
		fatalCh: make(chan error, 1),
		stopCh:  make(chan struct{}),
		probeCh: make(chan struct{}, 1),
	}
	for i := 0; i < 256; i++ {
		s.NotifyNetworkResume()
	}
	select {
	case <-s.probeCh:
	default:
		t.Fatal("expected one pending network resume probe")
	}
	select {
	case <-s.probeCh:
		t.Fatal("network resume notifications were not coalesced")
	default:
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRequestFailureRecoveryBudgetIsBounded(t *testing.T) {
	states := make(chan string, 4)
	s := &Stack{
		fatalCh: make(chan error, 1),
		stopCh:  make(chan struct{}),
		setRecoveryState: func(state string, _ error) {
			select {
			case states <- state:
			default:
			}
		},
	}
	attempts := []time.Time{}
	opts := Options{
		MaxRequestFailureRecoveries: 2,
		RequestFailureWindow:        time.Minute,
	}
	if !s.allowRequestFailureRecovery(opts, &attempts, errors.New("request-1"), errors.New("probe-1")) {
		t.Fatal("first request failure recovery was unexpectedly blocked")
	}
	if !s.allowRequestFailureRecovery(opts, &attempts, errors.New("request-2"), errors.New("probe-2")) {
		t.Fatal("second request failure recovery was unexpectedly blocked")
	}
	if s.allowRequestFailureRecovery(opts, &attempts, errors.New("request-3"), errors.New("probe-3")) {
		t.Fatal("third request failure recovery exceeded the configured budget")
	}
	select {
	case err := <-s.fatalCh:
		if !strings.Contains(err.Error(), "request failure recovery budget exceeded") {
			t.Fatalf("fatal error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("budget exhaustion did not report a fatal error")
	}
	select {
	case state := <-states:
		if state != "blocked" {
			t.Fatalf("final recovery state = %q, want blocked", state)
		}
	case <-time.After(time.Second):
		t.Fatal("budget exhaustion did not publish blocked state")
	}
}

func TestRequestFailureGateRequiresRequestAndProbeEvidence(t *testing.T) {
	var gate requestFailureGate
	now := time.Unix(10, 0)
	if got := gate.observe(now, time.Minute); got != 1 {
		t.Fatalf("first observation = %d, want 1", got)
	}
	if got := gate.observe(now.Add(time.Second), time.Minute); got != 2 {
		t.Fatalf("second observation = %d, want 2", got)
	}
	gate.reset()
	if got := gate.observe(now.Add(2*time.Minute), time.Minute); got != 1 {
		t.Fatalf("observation after admission window = %d, want 1", got)
	}
}

func TestClassifyRequestFailureKeepsTargetErrorsOutOfSharedRecovery(t *testing.T) {
	if got := classifyRequestFailure(errors.New("HTTP 403 forbidden")); got != requestFailureTarget {
		t.Fatalf("target failure kind = %q, want %q", got, requestFailureTarget)
	}
	if got := classifyRequestFailure(errors.New("connection reset by peer")); got != requestFailureTransient {
		t.Fatalf("transport failure kind = %q, want %q", got, requestFailureTransient)
	}
	if got := classifyRequestFailure(context.Canceled); got != requestFailureIgnored {
		t.Fatalf("canceled failure kind = %q, want %q", got, requestFailureIgnored)
	}
}

func TestMonitorRecoversTunnelAndSwapsProxyGeneration(t *testing.T) {
	port, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	initial := newProbeTestTunnel(port, errors.New("initial tunnel failure"))
	close(initial.done)

	previousFactory := newStackTunnel
	created := make(chan *probeTestTunnel, 1)
	newStackTunnel = func(_ config.Profile, socksPort int) (tunnelProcess, error) {
		tun := newProbeTestTunnel(socksPort, nil)
		created <- tun
		return tun, nil
	}
	t.Cleanup(func() { newStackTunnel = previousFactory })

	router, err := localproxy.NewGenerationRouter(stackTestDialer{})
	if err != nil {
		t.Fatalf("NewGenerationRouter: %v", err)
	}
	defer router.Close(context.Background())
	oldGeneration := router.CurrentGeneration()
	s := &Stack{
		Profile:   config.Profile{Host: "example.com", Port: 22, User: "alice"},
		SocksPort: port,
		tunnel:    initial,
		router:    router,
		fatalCh:   make(chan error, 1),
		stopCh:    make(chan struct{}),
		probeCh:   make(chan struct{}, 1),
	}

	go s.monitor(Options{
		MaxRestarts:       1,
		RestartBackoff:    time.Millisecond,
		TunnelStopGrace:   100 * time.Millisecond,
		SocksReadyTimeout: 500 * time.Millisecond,
		ProbeInterval:     time.Hour,
	})

	var replacement *probeTestTunnel
	select {
	case replacement = <-created:
	case err := <-s.fatalCh:
		t.Fatalf("unexpected recovery fatal: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replacement tunnel")
	}
	select {
	case <-replacement.started:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement tunnel did not start")
	}
	deadline := time.Now().Add(2 * time.Second)
	for s.currentTunnel() != replacement && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.currentTunnel() != replacement {
		t.Fatal("monitor did not install replacement tunnel")
	}
	if replacement.port == port {
		t.Fatalf("candidate reused active SOCKS port %d; recovery must use a temporary port", port)
	}
	if got := s.CurrentSocksPort(); got != replacement.port {
		t.Fatalf("active SOCKS port = %d, want candidate port %d", got, replacement.port)
	}
	if router.CurrentGeneration() == oldGeneration {
		t.Fatal("monitor did not swap the proxy generation")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = initial.Stop(time.Millisecond)
}

func TestMonitorRetriesCandidateAfterDeadTunnel(t *testing.T) {
	port, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	initial := newProbeTestTunnel(port, errors.New("initial tunnel failure"))
	close(initial.done)

	previousFactory := newStackTunnel
	firstAttempt := true
	created := make(chan *probeTestTunnel, 1)
	newStackTunnel = func(_ config.Profile, socksPort int) (tunnelProcess, error) {
		if firstAttempt {
			firstAttempt = false
			return nil, errors.New("candidate temporarily unavailable")
		}
		tun := newProbeTestTunnel(socksPort, nil)
		created <- tun
		return tun, nil
	}
	t.Cleanup(func() { newStackTunnel = previousFactory })

	router, err := localproxy.NewGenerationRouter(stackTestDialer{})
	if err != nil {
		t.Fatalf("NewGenerationRouter: %v", err)
	}
	defer router.Close(context.Background())
	s := &Stack{
		Profile:   config.Profile{Host: "example.com", Port: 22, User: "alice"},
		SocksPort: port,
		tunnel:    initial,
		router:    router,
		fatalCh:   make(chan error, 1),
		stopCh:    make(chan struct{}),
		probeCh:   make(chan struct{}, 1),
	}
	go s.monitor(Options{
		MaxRestarts:       2,
		RestartBackoff:    time.Millisecond,
		TunnelStopGrace:   100 * time.Millisecond,
		SocksReadyTimeout: 500 * time.Millisecond,
		ProbeInterval:     time.Hour,
	})

	var replacement *probeTestTunnel
	select {
	case replacement = <-created:
	case err := <-s.fatalCh:
		t.Fatalf("transient candidate failure became fatal: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for candidate retry")
	}
	if replacement == nil {
		t.Fatal("candidate retry returned no replacement tunnel")
	}
	deadline := time.Now().Add(2 * time.Second)
	for s.currentTunnel() != replacement && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.currentTunnel() != replacement {
		t.Fatal("monitor did not install replacement after candidate retry")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_ = initial.Stop(time.Millisecond)
}

func TestMonitorNetworkResumeRecoversFailedRemoteProbe(t *testing.T) {
	port, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	initial := newProbeTestTunnel(port, nil)
	if err := initial.Start(); err != nil {
		t.Fatalf("start initial tunnel: %v", err)
	}

	previousFactory := newStackTunnel
	created := make(chan *probeTestTunnel, 1)
	newStackTunnel = func(_ config.Profile, socksPort int) (tunnelProcess, error) {
		tun := newProbeTestTunnel(socksPort, nil)
		created <- tun
		return tun, nil
	}
	t.Cleanup(func() { newStackTunnel = previousFactory })

	router, err := localproxy.NewGenerationRouter(stackTestDialer{})
	if err != nil {
		t.Fatalf("NewGenerationRouter: %v", err)
	}
	defer router.Close(context.Background())
	s := &Stack{
		Profile: config.Profile{
			Host:            "example.com",
			Port:            22,
			User:            "alice",
			RouteTargetHost: "api.example.com",
			RouteTargetPort: 443,
		},
		SocksPort: port,
		tunnel:    initial,
		router:    router,
		fatalCh:   make(chan error, 1),
		stopCh:    make(chan struct{}),
		probeCh:   make(chan struct{}, 1),
	}
	probeCount := 0
	go s.monitor(Options{
		MaxRestarts:       1,
		RestartBackoff:    time.Millisecond,
		TunnelStopGrace:   100 * time.Millisecond,
		SocksReadyTimeout: 500 * time.Millisecond,
		ProbeInterval:     time.Hour,
		RouteProbe: func(context.Context, string, string, int, time.Duration) error {
			probeCount++
			if probeCount == 1 {
				return errors.New("remote route unavailable")
			}
			return nil
		},
	})

	s.NotifyNetworkResume()
	var replacement *probeTestTunnel
	select {
	case replacement = <-created:
	case err := <-s.fatalCh:
		t.Fatalf("unexpected recovery fatal: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for resume recovery")
	}
	deadline := time.Now().Add(2 * time.Second)
	for s.currentTunnel() != replacement && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.currentTunnel() != replacement {
		t.Fatal("resume recovery did not install replacement tunnel")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRouteProbeUsesExplicitTargetForSSHConfigAlias(t *testing.T) {
	port, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	initial := newProbeTestTunnel(port, errors.New("initial tunnel failure"))
	close(initial.done)
	previousFactory := newStackTunnel
	defer func() { newStackTunnel = previousFactory }()
	var gotHost string
	var gotPort int
	newStackTunnel = func(_ config.Profile, socksPort int) (tunnelProcess, error) {
		return newProbeTestTunnel(socksPort, nil), nil
	}
	s := &Stack{
		Profile: config.Profile{
			Host:            "work-alias",
			Port:            22,
			User:            "alice",
			SSHArgs:         []string{"-F", "/tmp/ssh_config"},
			RouteTargetHost: "api.example.com",
			RouteTargetPort: 443,
		},
		SocksPort: port,
		tunnel:    initial,
		fatalCh:   make(chan error, 1),
		stopCh:    make(chan struct{}),
	}
	if !s.recoverTunnelAt(Options{
		MaxRestarts:       1,
		RestartBackoff:    0,
		RestartWindow:     time.Minute,
		TunnelStopGrace:   100 * time.Millisecond,
		SocksReadyTimeout: 500 * time.Millisecond,
		RouteProbe: func(_ context.Context, _ string, host string, port int, _ time.Duration) error {
			gotHost, gotPort = host, port
			return nil
		},
	}, &[]time.Time{}, errors.New("resume"), time.Now()) {
		t.Fatal("config-alias recovery was unexpectedly rejected")
	}
	if gotHost != "api.example.com" || gotPort != 443 {
		t.Fatalf("route probe target = %s:%d, want api.example.com:443", gotHost, gotPort)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRouteProbeSkipsSSHConfigAliasWithoutExplicitTarget(t *testing.T) {
	port, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	initial := newProbeTestTunnel(port, errors.New("initial tunnel failure"))
	close(initial.done)
	previousFactory := newStackTunnel
	defer func() { newStackTunnel = previousFactory }()
	var probeCalls int
	newStackTunnel = func(_ config.Profile, socksPort int) (tunnelProcess, error) {
		return newProbeTestTunnel(socksPort, nil), nil
	}
	s := &Stack{
		Profile: config.Profile{
			Host:    "pdx-ssh-session",
			Port:    4081,
			User:    "alice",
			SSHArgs: []string{"-F", "/home/example/.ssh/config"},
		},
		SocksPort: port,
		tunnel:    initial,
		fatalCh:   make(chan error, 1),
		stopCh:    make(chan struct{}),
	}
	if !s.recoverTunnelAt(Options{
		MaxRestarts:       1,
		RestartBackoff:    0,
		RestartWindow:     time.Minute,
		TunnelStopGrace:   100 * time.Millisecond,
		SocksReadyTimeout: 500 * time.Millisecond,
		RouteProbe: func(context.Context, string, string, int, time.Duration) error {
			probeCalls++
			return errors.New("the SSH alias must not be probed as a remote target")
		},
	}, &[]time.Time{}, errors.New("resume"), time.Now()) {
		t.Fatal("SSH config alias recovery was unexpectedly rejected")
	}
	if probeCalls != 0 {
		t.Fatalf("route probe calls = %d, want 0 without an explicit route target", probeCalls)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRouteTargetRequiresExplicitDestination(t *testing.T) {
	tests := []struct {
		name        string
		profile     config.Profile
		options     Options
		wantHost    string
		wantPort    int
		wantPresent bool
	}{
		{
			name: "ssh config alias endpoint is not inferred",
			profile: config.Profile{
				Host:    "pdx-ssh-session",
				Port:    4081,
				SSHArgs: []string{"-F", "/home/example/.ssh/config"},
			},
		},
		{
			name: "ProxyJump endpoint is not inferred",
			profile: config.Profile{
				Host:    "target.internal",
				Port:    22,
				SSHArgs: []string{"-J", "jump-alias"},
			},
		},
		{
			name: "profile target is preserved",
			profile: config.Profile{
				Host:            "ssh-alias",
				Port:            22,
				RouteTargetHost: "api.example.com",
				RouteTargetPort: 443,
			},
			wantHost:    "api.example.com",
			wantPort:    443,
			wantPresent: true,
		},
		{
			name: "options target overrides profile target",
			profile: config.Profile{
				Host:            "ssh-alias",
				Port:            22,
				RouteTargetHost: "profile.example.com",
				RouteTargetPort: 443,
			},
			options: Options{
				RouteTargetHost: "options.example.com",
				RouteTargetPort: 8443,
			},
			wantHost:    "options.example.com",
			wantPort:    8443,
			wantPresent: true,
		},
		{
			name: "invalid explicit port is rejected",
			profile: config.Profile{
				Host:            "ssh-alias",
				Port:            22,
				RouteTargetHost: "api.example.com",
				RouteTargetPort: 65536,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, present := routeTarget(tt.profile, tt.options)
			if present != tt.wantPresent || host != tt.wantHost || port != tt.wantPort {
				t.Fatalf("routeTarget = %q:%d present=%t, want %q:%d present=%t", host, port, present, tt.wantHost, tt.wantPort, tt.wantPresent)
			}
		})
	}
}

func TestRecoveryBudgetPersistsBlockedAcrossStackReplacement(t *testing.T) {
	var persisted config.ProxyRecoveryBudget
	s := &Stack{
		fatalCh: make(chan error, 1),
		stopCh:  make(chan struct{}),
		persistRecoveryBudget: func(budget config.ProxyRecoveryBudget) error {
			persisted = budget
			return nil
		},
	}
	attempts := []time.Time{}
	opts := Options{MaxRequestFailureRecoveries: 1, RequestFailureWindow: time.Minute}
	if !s.allowRequestFailureRecovery(opts, &attempts, errors.New("first"), errors.New("probe")) {
		t.Fatal("first recovery was unexpectedly blocked")
	}
	if s.allowRequestFailureRecovery(opts, &attempts, errors.New("second"), errors.New("probe")) {
		t.Fatal("second recovery exceeded the budget")
	}
	if !persisted.Blocked || persisted.RequestAttempts != 2 {
		t.Fatalf("persisted budget = %#v, want blocked after two attempts", persisted)
	}
	profile := config.Profile{Host: "example.com", Port: 22, User: "alice"}
	_, err := Start(profile, "replacement", Options{RecoveryBudget: persisted})
	if !errors.Is(err, ErrRecoveryBudgetBlocked) {
		t.Fatalf("replacement Start error = %v, want ErrRecoveryBudgetBlocked", err)
	}
}

func TestMonitorDetectsProbeGapAsResume(t *testing.T) {
	port, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	initial := newProbeTestTunnel(port, nil)
	if err := initial.Start(); err != nil {
		t.Fatalf("start initial tunnel: %v", err)
	}

	router, err := localproxy.NewGenerationRouter(stackTestDialer{})
	if err != nil {
		t.Fatalf("NewGenerationRouter: %v", err)
	}
	defer router.Close(context.Background())

	var clockMu sync.Mutex
	clock := time.Unix(100, 0)
	readClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	}
	advanceClock := func(delta time.Duration) {
		clockMu.Lock()
		clock = clock.Add(delta)
		clockMu.Unlock()
	}
	clockReady := make(chan struct{})
	var clockReadyOnce sync.Once
	readMonitorClock := func() time.Time {
		clockReadyOnce.Do(func() { close(clockReady) })
		return readClock()
	}
	remoteProbes := make(chan struct{}, 1)
	s := &Stack{
		Profile: config.Profile{
			Host:            "example.com",
			Port:            22,
			User:            "alice",
			RouteTargetHost: "api.example.com",
			RouteTargetPort: 443,
		},
		SocksPort: port,
		tunnel:    initial,
		router:    router,
		fatalCh:   make(chan error, 1),
		stopCh:    make(chan struct{}),
		probeCh:   make(chan struct{}, 1),
	}
	go s.monitor(Options{
		MaxRestarts:       1,
		RestartBackoff:    time.Millisecond,
		TunnelStopGrace:   100 * time.Millisecond,
		SocksReadyTimeout: 500 * time.Millisecond,
		ProbeInterval:     10 * time.Millisecond,
		Now:               readMonitorClock,
		RouteProbe: func(context.Context, string, string, int, time.Duration) error {
			select {
			case remoteProbes <- struct{}{}:
			default:
			}
			return nil
		},
	})

	select {
	case <-clockReady:
	case <-time.After(time.Second):
		t.Fatal("monitor did not initialize its clock")
	}
	// Let one ordinary periodic tick perform only the cheap local greeting.
	time.Sleep(25 * time.Millisecond)
	advanceClock(100 * time.Millisecond)
	select {
	case <-remoteProbes:
	case fatal := <-s.fatalCh:
		t.Fatalf("probe gap caused fatal recovery: %v", fatal)
	case <-time.After(time.Second):
		t.Fatal("monitor did not perform a remote probe after the simulated sleep gap")
	}
	if s.currentTunnel() != initial {
		t.Fatal("healthy tunnel was replaced during simulated sleep/resume probe")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMonitorCandidateFailureKeepsLiveActiveTunnel(t *testing.T) {
	port, err := pickFreePort()
	if err != nil {
		t.Fatalf("pickFreePort: %v", err)
	}
	initial := newProbeTestTunnel(port, nil)
	if err := initial.Start(); err != nil {
		t.Fatalf("start initial tunnel: %v", err)
	}

	previousFactory := newStackTunnel
	attempted := make(chan struct{}, 1)
	newStackTunnel = func(config.Profile, int) (tunnelProcess, error) {
		attempted <- struct{}{}
		return nil, errors.New("candidate unavailable")
	}
	t.Cleanup(func() { newStackTunnel = previousFactory })

	router, err := localproxy.NewGenerationRouter(stackTestDialer{})
	if err != nil {
		t.Fatalf("NewGenerationRouter: %v", err)
	}
	defer router.Close(context.Background())
	s := &Stack{
		Profile: config.Profile{
			Host:            "example.com",
			Port:            22,
			User:            "alice",
			RouteTargetHost: "api.example.com",
			RouteTargetPort: 443,
		},
		SocksPort: port,
		tunnel:    initial,
		router:    router,
		fatalCh:   make(chan error, 1),
		stopCh:    make(chan struct{}),
		probeCh:   make(chan struct{}, 1),
	}
	go s.monitor(Options{
		MaxRestarts:       1,
		RestartBackoff:    time.Millisecond,
		TunnelStopGrace:   100 * time.Millisecond,
		SocksReadyTimeout: 500 * time.Millisecond,
		ProbeInterval:     time.Hour,
		RouteProbe: func(context.Context, string, string, int, time.Duration) error {
			return errors.New("remote route unavailable")
		},
	})

	s.NotifyNetworkResume()
	select {
	case <-attempted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for candidate attempt")
	}
	if s.currentTunnel() != initial {
		t.Fatal("candidate failure replaced the active tunnel")
	}
	if got := s.CurrentSocksPort(); got != port {
		t.Fatalf("active SOCKS port changed after failed candidate: got %d want %d", got, port)
	}
	select {
	case fatal := <-s.fatalCh:
		t.Fatalf("candidate failure killed the active tunnel: %v", fatal)
	default:
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

type stackTestDialer struct{}

func (stackTestDialer) Dial(string, string) (net.Conn, error) {
	client, peer := net.Pipe()
	_ = peer.Close()
	return client, nil
}

type probeTestTunnel struct {
	port    int
	waitErr error
	done    chan struct{}
	started chan struct{}

	mu        sync.Mutex
	listener  net.Listener
	startOnce sync.Once
	stopOnce  sync.Once
}

func newProbeTestTunnel(port int, waitErr error) *probeTestTunnel {
	return &probeTestTunnel{
		port:    port,
		waitErr: waitErr,
		done:    make(chan struct{}),
		started: make(chan struct{}),
	}
}

func (t *probeTestTunnel) Start() error {
	var startErr error
	t.startOnce.Do(func() {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", t.port))
		if err != nil {
			startErr = err
			close(t.started)
			return
		}
		t.mu.Lock()
		t.listener = ln
		t.mu.Unlock()
		close(t.started)
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func(conn net.Conn) {
					defer conn.Close()
					greeting := make([]byte, 3)
					if _, err := io.ReadFull(conn, greeting); err == nil {
						_, _ = conn.Write([]byte{5, 0})
					}
				}(conn)
			}
		}()
	})
	return startErr
}

func (t *probeTestTunnel) Stop(time.Duration) error {
	t.stopOnce.Do(func() {
		t.mu.Lock()
		ln := t.listener
		t.listener = nil
		t.mu.Unlock()
		if ln != nil {
			_ = ln.Close()
		}
		select {
		case <-t.done:
		default:
			close(t.done)
		}
	})
	return t.waitErr
}

func (t *probeTestTunnel) Done() <-chan struct{} { return t.done }

func (t *probeTestTunnel) Wait() error {
	<-t.done
	return t.waitErr
}

func intToString(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + (n % 10))
		n /= 10
	}
	return string(buf[i:])
}
