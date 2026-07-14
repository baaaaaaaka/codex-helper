//go:build linux

package stack

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestStackIntegrationHTTPProxyConnectThroughSSHTunnel(t *testing.T) {
	if os.Getenv("SSH_TEST_ENABLED") != "1" {
		t.Skip("SSH integration tests disabled")
	}
	if os.Getenv("SSH_STACK_INTEGRATION_TEST") != "1" {
		t.Skip("SSH stack integration test disabled")
	}
	host := os.Getenv("SSH_TEST_HOST")
	portStr := os.Getenv("SSH_TEST_PORT")
	user := os.Getenv("SSH_TEST_USER")
	key := os.Getenv("SSH_TEST_KEY")
	if host == "" || portStr == "" || user == "" || key == "" {
		t.Skip("missing SSH_TEST_* env vars")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("invalid SSH_TEST_PORT: %v", err)
	}

	echoAddr, closeEcho := startStackIntegrationEcho(t)
	defer closeEcho()

	st, err := Start(config.Profile{
		ID:   "ssh-test",
		Name: "ssh-test",
		Host: host,
		Port: port,
		User: user,
		SSHArgs: []string{
			"-i", key,
			"-o", "StrictHostKeyChecking=no",
			"-o", "UserKnownHostsFile=/dev/null",
			"-o", "IdentitiesOnly=yes",
			"-o", "GSSAPIAuthentication=no",
		},
	}, "stack-integration", Options{
		SocksReadyTimeout: 5 * time.Second,
		RestartBackoff:    100 * time.Millisecond,
		TunnelStopGrace:   500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Start stack: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = st.Close(context.Background())
		}
	}()

	resp, err := http.Get(st.HTTPProxyURL() + "/_codex_proxy/health")
	if err != nil {
		t.Fatalf("GET health: %v", err)
	}
	defer resp.Body.Close()
	var health struct {
		OK         bool   `json:"ok"`
		InstanceID string `json:"instanceId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if !health.OK || health.InstanceID != "stack-integration" {
		t.Fatalf("unexpected health response: %+v", health)
	}

	conn, err := net.DialTimeout("tcp", st.HTTPAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial HTTP proxy: %v", err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", echoAddr, echoAddr)
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read CONNECT status: %v", err)
	}
	if status != "HTTP/1.1 200 Connection Established\r\n" {
		t.Fatalf("unexpected CONNECT status: %q", status)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read CONNECT headers: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatalf("write echo payload: %v", err)
	}
	echo, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read echo payload: %v", err)
	}
	if echo != "ping\n" {
		t.Fatalf("expected echo payload, got %q", echo)
	}

	if err := st.Close(context.Background()); err != nil {
		t.Logf("Close stack returned after proxy verification: %v", err)
	}
	closed = true
	if c, err := net.DialTimeout("tcp", st.HTTPAddr, 100*time.Millisecond); err == nil {
		_ = c.Close()
		t.Fatalf("expected HTTP proxy listener to be closed after stack Close")
	}
}

func TestStackIntegrationDockerNetworkRecovery(t *testing.T) {
	if os.Getenv("STACK_DOCKER_RECOVERY_TEST") != "1" {
		t.Skip("Docker network recovery test disabled")
	}
	if os.Getenv("SSH_TEST_ENABLED") != "1" || os.Getenv("SSH_STACK_INTEGRATION_TEST") != "1" {
		t.Fatal("Docker network recovery test requires SSH integration test flags")
	}
	host := os.Getenv("SSH_TEST_HOST")
	portStr := os.Getenv("SSH_TEST_PORT")
	user := os.Getenv("SSH_TEST_USER")
	key := os.Getenv("SSH_TEST_KEY")
	readyDir := os.Getenv("STACK_DOCKER_READY_DIR")
	doneDir := os.Getenv("STACK_DOCKER_DONE_DIR")
	roundsStr := os.Getenv("STACK_DOCKER_FAULT_ROUNDS")
	requireGenerationRaw := os.Getenv("STACK_DOCKER_REQUIRE_GENERATION_ROUNDS")
	if host == "" || portStr == "" || user == "" || key == "" || readyDir == "" || doneDir == "" || roundsStr == "" || requireGenerationRaw == "" {
		t.Fatal("Docker network recovery test requires SSH, round, and marker env vars")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("invalid SSH_TEST_PORT: %v", err)
	}
	rounds, err := strconv.Atoi(roundsStr)
	if err != nil || rounds <= 0 {
		t.Fatalf("invalid STACK_DOCKER_FAULT_ROUNDS: %q", roundsStr)
	}
	requireGenerationParts := strings.Split(requireGenerationRaw, ",")
	if len(requireGenerationParts) != rounds {
		t.Fatalf("STACK_DOCKER_REQUIRE_GENERATION_ROUNDS has %d entries, want %d", len(requireGenerationParts), rounds)
	}
	requireGeneration := make([]bool, rounds)
	for i, part := range requireGenerationParts {
		switch strings.TrimSpace(part) {
		case "1":
			requireGeneration[i] = true
		case "0":
		default:
			t.Fatalf("invalid generation requirement %q at round %d", part, i)
		}
	}
	targetHost := os.Getenv("STACK_DOCKER_TARGET_HOST")
	if targetHost == "" {
		targetHost = host
	}
	targetPort := os.Getenv("STACK_DOCKER_TARGET_PORT")
	if targetPort == "" {
		targetPort = portStr
	}

	st, err := Start(config.Profile{
		ID:   "docker-recovery",
		Name: "docker-recovery",
		Host: host,
		Port: port,
		User: user,
		SSHArgs: []string{
			"-i", key,
			"-o", "StrictHostKeyChecking=no",
			"-o", "UserKnownHostsFile=/dev/null",
			"-o", "IdentitiesOnly=yes",
			"-o", "GSSAPIAuthentication=no",
		},
	}, "docker-recovery", Options{
		SocksReadyTimeout: 3 * time.Second,
		// Four Docker fault rounds can include short DNS convergence after SSH
		// container reattach. Keep recovery finite while avoiding a tight
		// restart loop that exhausts the budget before DNS settles.
		MaxRestarts:     32,
		RestartBackoff:  250 * time.Millisecond,
		TunnelStopGrace: 500 * time.Millisecond,
		RestartWindow:   30 * time.Second,
		ProbeInterval:   100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Start Docker recovery stack: %v", err)
	}
	defer func() {
		if err := st.Close(context.Background()); err != nil {
			t.Logf("Close Docker recovery stack: %v", err)
		}
	}()

	target := net.JoinHostPort(targetHost, targetPort)
	previousHealth := waitForDockerStackHealth(t, st, 10*time.Second, 1)
	assertDockerStackConnect(t, st, target)
	if err := os.MkdirAll(readyDir, 0700); err != nil {
		t.Fatalf("create ready marker directory: %v", err)
	}
	if err := os.MkdirAll(doneDir, 0700); err != nil {
		t.Fatalf("create done marker directory: %v", err)
	}

	baselineGoroutines := runtime.NumGoroutine()
	for round := 0; round < rounds; round++ {
		beforeGeneration := previousHealth.ActiveGeneration
		readyFile := filepath.Join(readyDir, strconv.Itoa(round))
		doneFile := filepath.Join(doneDir, strconv.Itoa(round))
		if err := os.WriteFile(readyFile, []byte("ready\n"), 0600); err != nil {
			t.Fatalf("write ready marker for round %d: %v", round, err)
		}

		stopStorm := startDockerConnectStorm(st, target)
		t.Cleanup(stopStorm)
		markerErr := waitForDockerMarker(st, doneFile, 60*time.Second)
		stopStorm()
		if markerErr != nil {
			t.Fatalf("Docker fault round %d failed: %v", round, markerErr)
		}

		// Exercise the explicit resume path as a burst. The stack's one-slot
		// signal channel must coalesce these calls into bounded probe/recovery
		// work even while the CONNECT storm is producing failures.
		for i := 0; i < 32; i++ {
			st.NotifyNetworkResume()
		}
		previousHealth = waitForDockerRecovery(t, st, target, beforeGeneration, requireGeneration[round])
	}

	dockerStackHealthClient.CloseIdleConnections()
	time.Sleep(time.Second)
	runtime.GC()
	if got := runtime.NumGoroutine(); got > baselineGoroutines+24 {
		t.Fatalf("goroutine count grew across Docker fault rounds: baseline=%d got=%d", baselineGoroutines, got)
	}
}

func TestStackIntegrationDockerHealthySteadyState(t *testing.T) {
	if os.Getenv("STACK_DOCKER_HEALTHY_TEST") != "1" {
		t.Skip("Docker healthy steady-state test disabled")
	}
	if os.Getenv("SSH_TEST_ENABLED") != "1" || os.Getenv("SSH_STACK_INTEGRATION_TEST") != "1" {
		t.Fatal("Docker healthy steady-state test requires SSH integration test flags")
	}
	host := os.Getenv("SSH_TEST_HOST")
	portStr := os.Getenv("SSH_TEST_PORT")
	user := os.Getenv("SSH_TEST_USER")
	key := os.Getenv("SSH_TEST_KEY")
	readyFile := os.Getenv("STACK_DOCKER_HEALTHY_READY_FILE")
	startFile := os.Getenv("STACK_DOCKER_HEALTHY_START_FILE")
	doneFile := os.Getenv("STACK_DOCKER_HEALTHY_DONE_FILE")
	if host == "" || portStr == "" || user == "" || key == "" || readyFile == "" || startFile == "" || doneFile == "" {
		t.Fatal("Docker healthy steady-state test requires SSH and marker env vars")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("invalid SSH_TEST_PORT: %v", err)
	}
	targetHost := os.Getenv("STACK_DOCKER_TARGET_HOST")
	if targetHost == "" {
		targetHost = host
	}
	targetPort := os.Getenv("STACK_DOCKER_TARGET_PORT")
	if targetPort == "" {
		targetPort = portStr
	}
	loadDuration := 2 * time.Second
	if raw := os.Getenv("STACK_DOCKER_HEALTHY_LOAD_MS"); raw != "" {
		ms, parseErr := strconv.Atoi(raw)
		if parseErr != nil || ms <= 0 {
			t.Fatalf("invalid STACK_DOCKER_HEALTHY_LOAD_MS: %q", raw)
		}
		loadDuration = time.Duration(ms) * time.Millisecond
	}
	probeInterval := 30 * time.Second
	if raw := os.Getenv("STACK_DOCKER_HEALTHY_PROBE_INTERVAL_MS"); raw != "" {
		ms, parseErr := strconv.Atoi(raw)
		if parseErr != nil || ms <= 0 {
			t.Fatalf("invalid STACK_DOCKER_HEALTHY_PROBE_INTERVAL_MS: %q", raw)
		}
		probeInterval = time.Duration(ms) * time.Millisecond
	}

	st, err := Start(config.Profile{
		ID:   "docker-healthy",
		Name: "docker-healthy",
		Host: host,
		Port: port,
		User: user,
		SSHArgs: []string{
			"-i", key,
			"-o", "StrictHostKeyChecking=no",
			"-o", "UserKnownHostsFile=/dev/null",
			"-o", "IdentitiesOnly=yes",
			"-o", "GSSAPIAuthentication=no",
		},
	}, "docker-healthy", Options{
		SocksReadyTimeout: 3 * time.Second,
		// The default keeps request-path measurements below the periodic probe;
		// CI can set this explicitly to the production five-second interval when
		// measuring background probe cost.
		ProbeInterval: probeInterval,
	})
	if err != nil {
		t.Fatalf("Start Docker healthy stack: %v", err)
	}
	defer func() {
		if err := st.Close(context.Background()); err != nil {
			t.Logf("Close Docker healthy stack: %v", err)
		}
	}()

	target := net.JoinHostPort(targetHost, targetPort)
	before := waitForDockerStackHealth(t, st, 10*time.Second, 1)
	assertDockerStackConnect(t, st, target)
	if err := os.MkdirAll(filepath.Dir(readyFile), 0700); err != nil {
		t.Fatalf("create healthy ready marker directory: %v", err)
	}
	if err := os.WriteFile(readyFile, []byte("ready\n"), 0600); err != nil {
		t.Fatalf("write healthy ready marker: %v", err)
	}
	if err := waitForDockerMarker(st, startFile, 30*time.Second); err != nil {
		t.Fatalf("wait for healthy measurement start: %v", err)
	}

	var successes atomic.Uint64
	var failures atomic.Uint64
	loadStarted := time.Now()
	deadline := time.Now().Add(loadDuration)
	for time.Now().Before(deadline) {
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := tryDockerStackConnect(st, target); err != nil {
					failures.Add(1)
					return
				}
				successes.Add(1)
			}()
		}
		wg.Wait()
	}
	if failures.Load() != 0 {
		t.Fatalf("healthy CONNECT workload had %d failures out of %d attempts", failures.Load(), successes.Load()+failures.Load())
	}
	if successes.Load() == 0 {
		t.Fatal("healthy CONNECT workload made no successful requests")
	}
	loadElapsed := time.Since(loadStarted)
	t.Logf("healthy CONNECT workload: successes=%d failures=%d elapsed=%s rate=%.1f/s", successes.Load(), failures.Load(), loadElapsed, float64(successes.Load())/loadElapsed.Seconds())

	after := dockerStackHealthResponse{}
	var healthErr error
	for attempt := 0; attempt < 20; attempt++ {
		after, healthErr = dockerStackHealth(st)
		if healthErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if healthErr != nil {
		t.Fatalf("healthy stack health after workload: %v", healthErr)
	}
	if after.ActiveGeneration != before.ActiveGeneration {
		t.Fatalf("healthy workload changed active generation: before=%d after=%d", before.ActiveGeneration, after.ActiveGeneration)
	}
	if after.RecoveryCount != before.RecoveryCount {
		t.Fatalf("healthy workload changed recovery count: before=%d after=%d", before.RecoveryCount, after.RecoveryCount)
	}
	if after.BackendFailures != before.BackendFailures {
		t.Fatalf("healthy workload recorded backend failures: before=%d after=%d", before.BackendFailures, after.BackendFailures)
	}
	if after.Recovery != "ready" {
		t.Fatalf("healthy workload left recovery state %q", after.Recovery)
	}
	if err := os.WriteFile(doneFile, []byte(fmt.Sprintf("successes=%d\n", successes.Load())), 0600); err != nil {
		t.Fatalf("write healthy done marker: %v", err)
	}
	t.Logf("healthy health counters: generation=%d probeCount=%d recoveryCount=%d backendFailures=%d", after.ActiveGeneration, after.ProbeCount, after.RecoveryCount, after.BackendFailures)
}

type dockerStackHealthResponse struct {
	OK               bool   `json:"ok"`
	ActiveGeneration uint64 `json:"activeGeneration"`
	Recovery         string `json:"recovery"`
	Error            string `json:"error"`
	LastProbeError   string `json:"lastProbeError"`
	ProbeCount       uint64 `json:"probeCount"`
	RecoveryCount    uint64 `json:"recoveryCount"`
	BackendFailures  uint64 `json:"backendFailures"`
}

var dockerStackHealthClient = &http.Client{
	Timeout:   time.Second,
	Transport: &http.Transport{Proxy: nil},
}

func dockerStackHealth(st *Stack) (dockerStackHealthResponse, error) {
	resp, err := dockerStackHealthClient.Get(st.HTTPProxyURL() + "/_codex_proxy/health")
	if err != nil {
		return dockerStackHealthResponse{}, err
	}
	defer resp.Body.Close()
	var health dockerStackHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return health, err
	}
	if resp.StatusCode != http.StatusOK || !health.OK {
		detail := health.Error
		if detail == "" {
			detail = health.LastProbeError
		}
		return health, fmt.Errorf("health status %d recovery=%q: %s", resp.StatusCode, health.Recovery, detail)
	}
	return health, nil
}

func waitForDockerStackHealth(t *testing.T, st *Stack, timeout time.Duration, minGeneration uint64) dockerStackHealthResponse {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		health, err := dockerStackHealth(st)
		if err == nil && health.ActiveGeneration >= minGeneration {
			return health
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("proxy health did not become ready: %v", lastErr)
	return dockerStackHealthResponse{}
}

func waitForDockerMarker(st *Stack, path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("check marker %s: %w", path, err)
		}
		select {
		case err := <-st.Fatal():
			return fmt.Errorf("stack became fatal before marker %s: %w", path, err)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("marker %s was not written within %s", path, timeout)
}

func waitForDockerRecovery(t *testing.T, st *Stack, target string, previousGeneration uint64, requireGeneration bool) dockerStackHealthResponse {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		health, err := dockerStackHealth(st)
		if err != nil {
			lastErr = err
		} else if health.OK && health.ActiveGeneration >= previousGeneration && (!requireGeneration || health.ActiveGeneration > previousGeneration) {
			if err := tryDockerStackConnect(st, target); err == nil {
				return health
			} else {
				lastErr = err
			}
		}
		select {
		case err := <-st.Fatal():
			t.Fatalf("stack became fatal during Docker recovery: %v", err)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Docker proxy did not recover after generation %d (require new=%t): %v", previousGeneration, requireGeneration, lastErr)
	return dockerStackHealthResponse{}
}

func startDockerConnectStorm(st *Stack, target string) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = tryDockerStackConnect(st, target)
				}()
			}
			wg.Wait()
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}
}

func assertDockerStackConnect(t *testing.T, st *Stack, target string) {
	t.Helper()
	if err := tryDockerStackConnect(st, target); err != nil {
		t.Fatalf("HTTP CONNECT through Docker SSH/SOCKS stack: %v", err)
	}
}

func tryDockerStackConnect(st *Stack, target string) error {
	conn, err := net.DialTimeout("tcp", st.HTTPAddr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(status, "HTTP/1.1 200 ") {
		return fmt.Errorf("unexpected CONNECT status %q", status)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if line == "\r\n" {
			return nil
		}
	}
}

func startStackIntegrationEcho(t *testing.T) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}
