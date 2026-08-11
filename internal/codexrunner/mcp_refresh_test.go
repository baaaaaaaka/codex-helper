package codexrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMCPRefreshCoordinatorCoalescesChangesWhileReloadIsInFlight(t *testing.T) {
	configPath := writeMCPRefreshTestConfigAt(t, t.TempDir(), "initial")
	started := make(chan int, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	var calls atomic.Int32

	coordinator := newMCPRefreshCoordinator(configPath, 5*time.Millisecond, func(ctx context.Context) error {
		call := int(calls.Add(1))
		started <- call
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	coordinator.start(context.Background())
	defer func() {
		releaseAll()
		coordinator.stopAndWait()
	}()

	writeMCPRefreshTestConfig(t, configPath, "change-one")
	waitForMCPRefreshCall(t, started, 1)
	for i := 0; i < 8; i++ {
		writeMCPRefreshTestConfig(t, configPath, fmt.Sprintf("change-%02d-with-a-distinct-size", i))
	}
	// Give the watcher time to observe the final state while the first reload
	// remains blocked. The trigger channel must still contain only one item.
	time.Sleep(30 * time.Millisecond)

	// All changes observed while the first reload is blocked collapse into one
	// pending trigger rather than one request per file event.
	releaseAll()
	waitForMCPRefreshCall(t, started, 2)
	select {
	case call := <-started:
		t.Fatalf("unexpected third reload call %d", call)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestMCPRefreshCoordinatorDoesNotRefreshStableConfig(t *testing.T) {
	configPath := writeMCPRefreshTestConfigAt(t, t.TempDir(), "initial")
	started := make(chan struct{}, 1)
	coordinator := newMCPRefreshCoordinator(configPath, 5*time.Millisecond, func(context.Context) error {
		started <- struct{}{}
		return nil
	})
	coordinator.start(context.Background())
	defer coordinator.stopAndWait()

	select {
	case <-started:
		t.Fatal("stable config triggered an unexpected MCP refresh")
	case <-time.After(40 * time.Millisecond):
	}
}

func TestMCPRefreshCoordinatorDoesNotRetryUnchangedFailure(t *testing.T) {
	configPath := writeMCPRefreshTestConfigAt(t, t.TempDir(), "initial")
	started := make(chan int, 4)
	var calls atomic.Int32
	coordinator := newMCPRefreshCoordinator(configPath, 5*time.Millisecond, func(context.Context) error {
		started <- int(calls.Add(1))
		return fmt.Errorf("synthetic reload failure")
	})
	coordinator.start(context.Background())
	defer coordinator.stopAndWait()

	writeMCPRefreshTestConfig(t, configPath, "first-change")
	waitForMCPRefreshCall(t, started, 1)
	select {
	case call := <-started:
		t.Fatalf("unchanged failed config was retried as call %d", call)
	case <-time.After(40 * time.Millisecond):
	}

	writeMCPRefreshTestConfig(t, configPath, "second-change")
	waitForMCPRefreshCall(t, started, 2)
}

func TestMCPRefreshCoordinatorRefreshesAfterConfigRecreated(t *testing.T) {
	codexHome := t.TempDir()
	configPath := writeMCPRefreshTestConfigAt(t, codexHome, "initial")
	started := make(chan int, 4)
	var calls atomic.Int32
	coordinator := newMCPRefreshCoordinator(configPath, 5*time.Millisecond, func(context.Context) error {
		started <- int(calls.Add(1))
		return nil
	})
	coordinator.start(context.Background())
	defer coordinator.stopAndWait()

	if err := os.Remove(configPath); err != nil {
		t.Fatalf("remove config: %v", err)
	}
	waitForMCPRefreshCall(t, started, 1)
	select {
	case call := <-started:
		t.Fatalf("missing config was retried without a new state: call %d", call)
	case <-time.After(40 * time.Millisecond):
	}

	writeMCPRefreshTestConfig(t, configPath, "recreated-config-with-distinct-size")
	waitForMCPRefreshCall(t, started, 2)
}

func TestMCPRefreshCoordinatorDetectsAtomicReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("replacing an existing file with os.Rename is not portable on Windows")
	}
	codexHome := t.TempDir()
	configPath := writeMCPRefreshTestConfigAt(t, codexHome, "same-size")
	started := make(chan struct{}, 1)
	coordinator := newMCPRefreshCoordinator(configPath, 5*time.Millisecond, func(context.Context) error {
		started <- struct{}{}
		return nil
	})
	coordinator.start(context.Background())
	defer coordinator.stopAndWait()

	oldInfo, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("stat original config: %v", err)
	}
	replacement := filepath.Join(codexHome, "config.toml.replacement")
	if err := os.WriteFile(replacement, []byte("new-value"), 0o600); err != nil {
		t.Fatalf("write replacement config: %v", err)
	}
	if err := os.Chtimes(replacement, oldInfo.ModTime(), oldInfo.ModTime()); err != nil {
		t.Fatalf("preserve replacement mtime: %v", err)
	}
	if err := os.Rename(replacement, configPath); err != nil {
		t.Fatalf("atomically replace config: %v", err)
	}
	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("atomic replacement was not observed")
	}
}

func TestMCPRefreshCoordinatorStopCancelsBlockedReload(t *testing.T) {
	configPath := writeMCPRefreshTestConfigAt(t, t.TempDir(), "initial")
	started := make(chan struct{})
	coordinator := newMCPRefreshCoordinator(configPath, 5*time.Millisecond, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	coordinator.start(context.Background())

	writeMCPRefreshTestConfig(t, configPath, "change")
	waitForMCPRefreshStarted(t, started)

	stopped := make(chan struct{})
	go func() {
		coordinator.stopAndWait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("coordinator stop waited for blocked reload")
	}
}

func TestAppServerRunnerRefreshesConfigAfterStartup(t *testing.T) {
	codexHome := t.TempDir()
	configPath := writeMCPRefreshTestConfigAt(t, codexHome, "initial")
	transport := newFakeAppServerTransport(
		`{"id":1,"result":{}}`,
		`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
		`{"id":3,"result":{}}`,
	)
	runner := &AppServerRunner{Transport: transport, CodexHome: codexHome}
	if err := runner.ensureReady(context.Background()); err != nil {
		t.Fatalf("ensureReady error: %v", err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Fatalf("runner close error: %v", err)
		}
	}()

	if err := os.WriteFile(configPath, []byte("changed-config"), 0o600); err != nil {
		t.Fatalf("write changed config: %v", err)
	}
	waitForAppServerMethod(t, transport, appServerMethodMCPServerReload, 3*time.Second)
}

func TestAppServerRunnerMCPRefreshThroughJSONLProcess(t *testing.T) {
	codexHome := t.TempDir()
	configPath := writeMCPRefreshTestConfigAt(t, codexHome, "initial")
	markerDir := t.TempDir()
	reloadCountPath := filepath.Join(markerDir, "reload-count")
	reloadCompletePath := filepath.Join(markerDir, "reload-complete")
	releasePath := filepath.Join(markerDir, "release")
	transport := startProcessHelper(t, AppServerProcessStarter{}, "mcp-jsonl", reloadCountPath, reloadCompletePath, releasePath)
	runner := &AppServerRunner{Transport: transport, CodexHome: codexHome}
	defer func() { _ = runner.Close() }()
	if err := runner.ensureReady(context.Background()); err != nil {
		t.Fatalf("ensureReady error: %v", err)
	}

	if err := os.WriteFile(configPath, []byte("first-change"), 0o600); err != nil {
		t.Fatalf("write first config change: %v", err)
	}
	waitForMCPRefreshFileValue(t, reloadCountPath, "1", 3*time.Second)
	for i := 0; i < 100; i++ {
		writeMCPRefreshTestConfig(t, configPath, fmt.Sprintf("burst-change-%03d-%s", i, strings.Repeat("x", i)))
	}
	assertMCPRefreshFileValueRemains(t, reloadCountPath, "1", 200*time.Millisecond)

	const promptCount = 64
	turnDone := make(chan error, promptCount)
	var prompts sync.WaitGroup
	for i := 0; i < promptCount; i++ {
		threadID := fmt.Sprintf("jsonl-thread-%02d", i)
		prompts.Add(1)
		go func(threadID string) {
			defer prompts.Done()
			_, err := runner.StartTurn(context.Background(), StartTurnInput{
				ThreadID:  threadID,
				TurnInput: TurnInput{Prompt: "prompt while child-process reload is blocked"},
			})
			turnDone <- err
		}(threadID)
	}
	allPromptsDone := make(chan struct{})
	go func() {
		prompts.Wait()
		close(allPromptsDone)
	}()
	select {
	case <-allPromptsDone:
	case <-time.After(3 * time.Second):
		t.Fatal("JSONL child-process prompts waited for blocked reload")
	}
	for i := 0; i < promptCount; i++ {
		if err := <-turnDone; err != nil {
			t.Fatalf("JSONL child-process prompt %d failed: %v", i, err)
		}
	}

	if err := os.WriteFile(releasePath, []byte("release"), 0o600); err != nil {
		t.Fatalf("release child-process reload: %v", err)
	}
	waitForMCPRefreshFileValue(t, reloadCompletePath, "2", 3*time.Second)
	if _, err := runner.StartTurn(context.Background(), StartTurnInput{
		ThreadID:  "jsonl-after-refresh",
		TurnInput: TurnInput{Prompt: "prompt after child-process reload completed"},
	}); err != nil {
		t.Fatalf("prompt after JSONL refresh failed: %v", err)
	}
}

func TestAppServerRunnerDetectsConfigChangeDuringStartup(t *testing.T) {
	codexHome := t.TempDir()
	configPath := writeMCPRefreshTestConfigAt(t, codexHome, "initial")
	transport := newPromptMCPRefreshTransport()
	transport.initializeStarted = make(chan struct{})
	transport.initializeRelease = make(chan struct{})
	runner := &AppServerRunner{Transport: transport, CodexHome: codexHome}
	var releaseInitializeOnce sync.Once
	releaseInitialize := func() { releaseInitializeOnce.Do(func() { close(transport.initializeRelease) }) }
	defer func() {
		releaseInitialize()
		_ = runner.Close()
	}()

	ready := make(chan error, 1)
	go func() { ready <- runner.ensureReady(context.Background()) }()
	select {
	case <-transport.initializeStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for app-server initialization")
	}
	if err := os.WriteFile(configPath, []byte("changed-during-startup"), 0o600); err != nil {
		t.Fatalf("write changed config: %v", err)
	}
	releaseInitialize()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("ensureReady error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("app-server startup did not complete")
	}
	select {
	case <-transport.reloadStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for MCP reload after startup change")
	}
}

func TestAppServerRunnerParallelPromptsAreNotBlockedByMCPReload(t *testing.T) {
	codexHome := t.TempDir()
	writeMCPRefreshTestConfigAt(t, codexHome, "initial")
	transport := newPromptMCPRefreshTransport()
	runner := &AppServerRunner{Transport: transport, CodexHome: codexHome}
	if err := runner.ensureReady(context.Background()); err != nil {
		t.Fatalf("ensureReady error: %v", err)
	}
	defer func() {
		close(transport.reloadRelease)
		if err := runner.Close(); err != nil {
			t.Fatalf("runner close error: %v", err)
		}
	}()

	writeMCPRefreshTestConfig(t, filepath.Join(codexHome, "config.toml"), "changed-config")
	select {
	case <-transport.reloadStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for blocked MCP reload")
	}

	const promptCount = 32
	turnDone := make(chan error, promptCount)
	var prompts sync.WaitGroup
	for i := 0; i < promptCount; i++ {
		threadID := fmt.Sprintf("thread-%02d", i)
		prompts.Add(1)
		go func(threadID string) {
			defer prompts.Done()
			_, err := runner.StartTurn(context.Background(), StartTurnInput{
				ThreadID:  threadID,
				TurnInput: TurnInput{Prompt: "prompt while MCP reload is blocked"},
			})
			turnDone <- err
		}(threadID)
	}
	allPromptsDone := make(chan struct{})
	go func() {
		prompts.Wait()
		close(allPromptsDone)
	}()
	select {
	case <-allPromptsDone:
	case <-time.After(3 * time.Second):
		t.Fatal("parallel prompts waited for blocked MCP reload")
	}
	for i := 0; i < promptCount; i++ {
		if err := <-turnDone; err != nil {
			t.Fatalf("prompt %d failed while reload was blocked: %v", i, err)
		}
	}
}

func TestAppServerRunnerCloseCancelsMCPReload(t *testing.T) {
	codexHome := t.TempDir()
	writeMCPRefreshTestConfigAt(t, codexHome, "initial")
	transport := newPromptMCPRefreshTransport()
	runner := &AppServerRunner{Transport: transport, CodexHome: codexHome}
	defer func() { _ = runner.Close() }()
	if err := runner.ensureReady(context.Background()); err != nil {
		t.Fatalf("ensureReady error: %v", err)
	}

	writeMCPRefreshTestConfig(t, filepath.Join(codexHome, "config.toml"), "changed-config")
	select {
	case <-transport.reloadStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for blocked MCP reload")
	}

	closed := make(chan error, 1)
	go func() { closed <- runner.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("runner close error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runner close waited for blocked MCP reload")
	}
}

func TestAppServerRunnerRestartStopsOldMCPRefreshWorker(t *testing.T) {
	codexHome := t.TempDir()
	writeMCPRefreshTestConfigAt(t, codexHome, "initial")
	first := newPromptMCPRefreshTransport()
	second := newPromptMCPRefreshTransport()
	var starts atomic.Int32
	runner := &AppServerRunner{
		Starter: AppServerTransportStarterFunc(func(context.Context, AppServerStartRequest) (AppServerLineTransport, error) {
			if starts.Add(1) == 1 {
				return first, nil
			}
			return second, nil
		}),
		CodexHome: codexHome,
	}
	defer func() { _ = runner.Close() }()
	if err := runner.ensureReady(context.Background()); err != nil {
		t.Fatalf("initial ensureReady error: %v", err)
	}

	writeMCPRefreshTestConfig(t, filepath.Join(codexHome, "config.toml"), "first-change")
	select {
	case <-first.reloadStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for first MCP reload")
	}
	if err := runner.Restart(); err != nil {
		t.Fatalf("runner restart error: %v", err)
	}
	if got := first.reloadCalls.Load(); got != 1 {
		t.Fatalf("first transport reload calls after restart = %d, want 1", got)
	}
	if err := runner.ensureReady(context.Background()); err != nil {
		t.Fatalf("recovery ensureReady error: %v", err)
	}

	writeMCPRefreshTestConfig(t, filepath.Join(codexHome, "config.toml"), "second-change")
	select {
	case <-second.reloadStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for second MCP reload")
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("starter calls = %d, want 2", got)
	}
}

func TestAppServerRunnerMCPRefreshUsesRunnerSpecificCodexHome(t *testing.T) {
	homeA := t.TempDir()
	homeB := t.TempDir()
	writeMCPRefreshTestConfigAt(t, homeA, "initial-a")
	writeMCPRefreshTestConfigAt(t, homeB, "initial-b")
	transportA := newPromptMCPRefreshTransport()
	transportB := newPromptMCPRefreshTransport()
	runnerA := &AppServerRunner{Transport: transportA, CodexHome: homeA}
	runnerB := &AppServerRunner{Transport: transportB, CodexHome: homeB}
	defer func() {
		_ = runnerA.Close()
		_ = runnerB.Close()
	}()
	if err := runnerA.ensureReady(context.Background()); err != nil {
		t.Fatalf("runner A ensureReady error: %v", err)
	}
	if err := runnerB.ensureReady(context.Background()); err != nil {
		t.Fatalf("runner B ensureReady error: %v", err)
	}

	writeMCPRefreshTestConfig(t, filepath.Join(homeA, "config.toml"), "changed-a-with-distinct-size")
	select {
	case <-transportA.reloadStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("runner A did not refresh its config")
	}
	select {
	case <-transportB.reloadStarted:
		t.Fatal("runner B refreshed for runner A config change")
	case <-time.After(2 * mcpRefreshPollInterval):
	}
}

func TestAppServerRunnerCloseHookRunsOutsideRefreshLifecycleLock(t *testing.T) {
	runner := &AppServerRunner{}
	hookDone := make(chan struct{})
	runner.CloseHook = func() {
		if err := runner.Restart(); err != nil {
			t.Errorf("reentrant Restart error: %v", err)
		}
		close(hookDone)
	}
	closed := make(chan error, 1)
	go func() { closed <- runner.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("runner close error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runner close deadlocked while CloseHook re-entered Restart")
	}
	select {
	case <-hookDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CloseHook did not complete")
	}
}

type promptMCPRefreshTransport struct {
	reads             chan []byte
	closed            chan struct{}
	closeOnce         sync.Once
	initializeStarted chan struct{}
	initializeRelease chan struct{}
	initializeOnce    sync.Once
	reloadStarted     chan struct{}
	reloadOnce        sync.Once
	reloadRelease     chan struct{}
	reloadCalls       atomic.Int32
}

func newPromptMCPRefreshTransport() *promptMCPRefreshTransport {
	return &promptMCPRefreshTransport{
		reads:         make(chan []byte, 128),
		closed:        make(chan struct{}),
		reloadStarted: make(chan struct{}),
		reloadRelease: make(chan struct{}),
	}
}

func (t *promptMCPRefreshTransport) WriteLine(ctx context.Context, line []byte) error {
	var request appServerRequest
	if err := json.Unmarshal(line, &request); err != nil {
		return err
	}
	switch request.Method {
	case appServerMethodInitialize:
		if t.initializeStarted != nil {
			t.initializeOnce.Do(func() { close(t.initializeStarted) })
			select {
			case <-t.initializeRelease:
			case <-ctx.Done():
				return ctx.Err()
			case <-t.closed:
				return io.EOF
			}
		}
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{}}`, request.ID))
	case appServerMethodInitialized:
	case appServerMethodThreadList:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"data":[]}}`, request.ID))
	case appServerMethodMCPServerReload:
		t.reloadCalls.Add(1)
		t.reloadOnce.Do(func() { close(t.reloadStarted) })
		go func(id int64) {
			select {
			case <-t.reloadRelease:
				t.send(context.Background(), fmt.Sprintf(`{"id":%d,"result":{}}`, id))
			case <-t.closed:
			case <-ctx.Done():
			}
		}(request.ID)
	case appServerMethodTurnStart:
		t.send(ctx, fmt.Sprintf(`{"id":%d,"result":{"turn":{"id":"turn-1","status":"completed","items":[{"type":"agentMessage","text":"done"}]}}}`, request.ID))
	default:
		return fmt.Errorf("unexpected app-server method %q", request.Method)
	}
	return nil
}

func (t *promptMCPRefreshTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		return line, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *promptMCPRefreshTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (t *promptMCPRefreshTransport) send(ctx context.Context, line string) {
	select {
	case t.reads <- []byte(line):
	case <-ctx.Done():
	case <-t.closed:
	}
}

func writeMCPRefreshTestConfig(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

func writeMCPRefreshTestConfigAt(t *testing.T, codexHome, contents string) string {
	t.Helper()
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatalf("create Codex home: %v", err)
	}
	path := filepath.Join(codexHome, "config.toml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func waitForMCPRefreshCall(t *testing.T, calls <-chan int, want int) {
	t.Helper()
	select {
	case got := <-calls:
		if got != want {
			t.Fatalf("reload call = %d, want %d", got, want)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for reload call %d", want)
	}
}

func waitForMCPRefreshStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for blocked reload")
	}
}

func waitForAppServerMethod(t *testing.T, transport *fakeAppServerTransport, method string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, write := range transport.decodedWrites(t) {
			if got, _ := write["method"].(string); got == method {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for app-server method %q", method)
}

func waitForMCPRefreshFileValue(t *testing.T, path, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(raw)) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to contain %q", path, want)
}

func assertMCPRefreshFileValueRemains(t *testing.T, path, want string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err != nil || strings.TrimSpace(string(raw)) != want {
			t.Fatalf("%s changed from %q while it should have remained stable: %q (err=%v)", path, want, strings.TrimSpace(string(raw)), err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
