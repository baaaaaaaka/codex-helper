package codexrunner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

const appServerProcessHelperMarker = "--appserver-process-helper"

func TestAppServerProcessStarterLaunchesCommandArgsAndWorkingDir(t *testing.T) {
	workingDir := t.TempDir()
	command, args := appServerProcessHelperCommand("meta", "arg-one", "arg-two")

	transport, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{
		Command:    command,
		Args:       args,
		WorkingDir: workingDir,
	})
	if err != nil {
		t.Fatalf("StartAppServer error: %v", err)
	}
	defer transport.Close()

	line := readProcessTestLine(t, transport)
	var got struct {
		Cwd  string   `json:"cwd"`
		Args []string `json:"args"`
	}
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("metadata line is not JSON: %s: %v", string(line), err)
	}
	gotDir, err := os.Stat(got.Cwd)
	if err != nil {
		t.Fatalf("stat reported working dir %q: %v", got.Cwd, err)
	}
	wantDir, err := os.Stat(workingDir)
	if err != nil {
		t.Fatalf("stat requested working dir %q: %v", workingDir, err)
	}
	if !os.SameFile(gotDir, wantDir) {
		t.Fatalf("working dir = %q, want same directory as %q", got.Cwd, workingDir)
	}
	if want := []string{"meta", "arg-one", "arg-two"}; !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("helper args = %#v, want %#v", got.Args, want)
	}
}

func TestAppServerProcessStarterPassesExtraEnv(t *testing.T) {
	command, args := appServerProcessHelperCommand("meta")

	transport, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{
		Command:  command,
		Args:     args,
		ExtraEnv: []string{"CODEX_HELPER_TEAMS_CHILD=1", "CODEX_HELPER_TEAMS_PARENT_PID=1234"},
	})
	if err != nil {
		t.Fatalf("StartAppServer error: %v", err)
	}
	defer transport.Close()

	line := readProcessTestLine(t, transport)
	var got struct {
		Env map[string]string `json:"env"`
	}
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("metadata line is not JSON: %s: %v", string(line), err)
	}
	if got.Env["CODEX_HELPER_TEAMS_CHILD"] != "1" || got.Env["CODEX_HELPER_TEAMS_PARENT_PID"] != "1234" {
		t.Fatalf("extra env not passed to app-server process: %#v", got.Env)
	}
}

func TestMergeAppServerProcessEnvEmitsUniqueKeysAndOverlayWins(t *testing.T) {
	previousGOOS := appServerProcessRuntimeGOOS
	appServerProcessRuntimeGOOS = func() string { return "linux" }
	t.Cleanup(func() { appServerProcessRuntimeGOOS = previousGOOS })

	got := mergeAppServerProcessEnv(
		[]string{"PATH=/base", "HOME=/home/base", "PATH=/base-last"},
		[]string{"PATH=/overlay", "HOME=/home/overlay"},
	)
	counts := map[string]int{}
	values := map[string]string{}
	for _, entry := range got {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			counts[key]++
			values[key] = value
		}
	}
	if counts["PATH"] != 1 || counts["HOME"] != 1 {
		t.Fatalf("environment contains duplicate keys: %#v", got)
	}
	if values["PATH"] != "/overlay" || values["HOME"] != "/home/overlay" {
		t.Fatalf("overlay did not win: %#v", got)
	}
}

func TestMergeAppServerProcessEnvTreatsWindowsKeysCaseInsensitively(t *testing.T) {
	previousGOOS := appServerProcessRuntimeGOOS
	appServerProcessRuntimeGOOS = func() string { return "windows" }
	t.Cleanup(func() { appServerProcessRuntimeGOOS = previousGOOS })

	got := mergeAppServerProcessEnv([]string{"Path=C:\\base"}, []string{"PATH=C:\\overlay"})
	if len(got) != 1 || got[0] != "PATH=C:\\overlay" {
		t.Fatalf("Windows environment = %#v", got)
	}
}

func TestAppServerProcessStarterConfiguresCommandBeforeStart(t *testing.T) {
	command, args := appServerProcessHelperCommand("meta")

	transport, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{
		Command:  command,
		Args:     args,
		ExtraEnv: []string{"CODEX_HELPER_BEFORE_CONFIGURE=before"},
		ConfigureCommand: func(cmd *exec.Cmd) error {
			cmd.Env = append(cmd.Env, "CODEX_HELPER_CONFIGURED=after")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("StartAppServer error: %v", err)
	}
	defer transport.Close()

	line := readProcessTestLine(t, transport)
	var got struct {
		Env map[string]string `json:"env"`
	}
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("metadata line is not JSON: %s: %v", string(line), err)
	}
	if got.Env["CODEX_HELPER_BEFORE_CONFIGURE"] != "before" || got.Env["CODEX_HELPER_CONFIGURED"] != "after" {
		t.Fatalf("configure command env not passed to app-server process: %#v", got.Env)
	}
}

func TestAppServerProcessStarterConfigureCommandInheritsParentEnv(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_CHILD", "parent")
	command, args := appServerProcessHelperCommand("meta")

	transport, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{
		Command: command,
		Args:    args,
		ConfigureCommand: func(cmd *exec.Cmd) error {
			cmd.Env = append(cmd.Env, "CODEX_HELPER_CONFIGURED=after")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("StartAppServer error: %v", err)
	}
	defer transport.Close()

	line := readProcessTestLine(t, transport)
	var got struct {
		Env map[string]string `json:"env"`
	}
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("metadata line is not JSON: %s: %v", string(line), err)
	}
	if got.Env["CODEX_HELPER_TEAMS_CHILD"] != "parent" || got.Env["CODEX_HELPER_CONFIGURED"] != "after" {
		t.Fatalf("configure command did not inherit parent env before appending: %#v", got.Env)
	}
}

func TestAppServerProcessStarterRemovesInheritedRuntimeMarkers(t *testing.T) {
	for _, name := range appServerRuntimeEnvironmentNames() {
		t.Setenv(name, "inherited")
	}
	t.Setenv("CODEX_HELPER_ENV_KEEP", "parent")
	command, args := appServerProcessHelperCommand("meta")

	transport, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{
		Command: command,
		Args:    args,
	})
	if err != nil {
		t.Fatalf("StartAppServer error: %v", err)
	}
	defer transport.Close()

	env := readAppServerProcessTestEnvironment(t, transport)
	for _, name := range appServerRuntimeEnvironmentNames() {
		if got := env[name]; got != "" {
			t.Fatalf("%s leaked to app-server process: %q", name, got)
		}
	}
	if got := env["CODEX_HELPER_ENV_KEEP"]; got != "parent" {
		t.Fatalf("unrelated parent environment = %q, want parent", got)
	}
}

func TestAppServerProcessStarterRemovesRuntimeMarkersAfterConfiguration(t *testing.T) {
	command, args := appServerProcessHelperCommand("meta")
	transport, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{
		Command: command,
		Args:    args,
		ExtraEnv: []string{
			helperruntime.EnvRuntime + "=1",
			helperruntime.EnvRuntimeRoot + "=/runtime",
			helperruntime.EnvRuntimeVersion + "=v1.2.3",
			helperruntime.EnvEntryPath + "=/entry/cxp",
			"CODEX_HELPER_ENV_KEEP=overlay",
		},
		ConfigureCommand: func(cmd *exec.Cmd) error {
			cmd.Env = append(cmd.Env,
				helperruntime.EnvDisable+"=1",
				helperruntime.EnvForce+"=1",
			)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("StartAppServer error: %v", err)
	}
	defer transport.Close()

	env := readAppServerProcessTestEnvironment(t, transport)
	for _, name := range appServerRuntimeEnvironmentNames() {
		if got := env[name]; got != "" {
			t.Fatalf("configured %s leaked to app-server process: %q", name, got)
		}
	}
	if got := env["CODEX_HELPER_ENV_KEEP"]; got != "overlay" {
		t.Fatalf("unrelated configured environment = %q, want overlay", got)
	}
}

func TestAppServerProcessStarterNilConfiguredEnvironmentKeepsSanitizedParent(t *testing.T) {
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv("CODEX_HELPER_ENV_KEEP", "parent")
	command, args := appServerProcessHelperCommand("meta")
	transport, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{
		Command: command,
		Args:    args,
		ConfigureCommand: func(cmd *exec.Cmd) error {
			cmd.Env = nil
			return nil
		},
	})
	if err != nil {
		t.Fatalf("StartAppServer error: %v", err)
	}
	defer transport.Close()

	env := readAppServerProcessTestEnvironment(t, transport)
	if got := env[helperruntime.EnvRuntime]; got != "" {
		t.Fatalf("%s leaked to app-server process: %q", helperruntime.EnvRuntime, got)
	}
	if got := env["CODEX_HELPER_ENV_KEEP"]; got != "parent" {
		t.Fatalf("unrelated parent environment = %q, want parent", got)
	}
}

func TestAppServerProcessExecutablePrefersWindowsCmdShimOverPowerShellShim(t *testing.T) {
	previousGOOS := appServerProcessRuntimeGOOS
	appServerProcessRuntimeGOOS = func() string { return "windows" }
	t.Cleanup(func() { appServerProcessRuntimeGOOS = previousGOOS })

	dir := t.TempDir()
	ps1Path := filepath.Join(dir, "codex.ps1")
	cmdPath := filepath.Join(dir, "codex.cmd")
	if err := os.WriteFile(ps1Path, []byte("pwsh shim"), 0o600); err != nil {
		t.Fatalf("write ps1 shim: %v", err)
	}
	if got := appServerProcessExecutable(ps1Path); got != ps1Path {
		t.Fatalf("executable without .cmd = %q, want %q", got, ps1Path)
	}
	if err := os.WriteFile(cmdPath, []byte("@echo off\r\n"), 0o600); err != nil {
		t.Fatalf("write cmd shim: %v", err)
	}
	if got := appServerProcessExecutable(ps1Path); got != cmdPath {
		t.Fatalf("executable with .cmd = %q, want %q", got, cmdPath)
	}

	appServerProcessRuntimeGOOS = func() string { return "linux" }
	if got := appServerProcessExecutable(ps1Path); got != ps1Path {
		t.Fatalf("non-Windows executable = %q, want %q", got, ps1Path)
	}
}

func TestAppServerProcessTransportWriteLineAndReadLine(t *testing.T) {
	transport := startProcessHelper(t, AppServerProcessStarter{}, "echo")
	defer transport.Close()

	if err := transport.WriteLine(context.Background(), []byte(`{"hello":"world"}`)); err != nil {
		t.Fatalf("WriteLine error: %v", err)
	}
	line := readProcessTestLine(t, transport)
	var got map[string]string
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("echo line is not JSON: %s: %v", string(line), err)
	}
	if got["echo"] != `{"hello":"world"}` {
		t.Fatalf("echo = %q", got["echo"])
	}
}

func TestAppServerProcessTransportCloseTerminatesAndWaits(t *testing.T) {
	transport := startProcessHelper(t, AppServerProcessStarter{}, "ready-block")
	processTransport := transport.(*appServerProcessTransport)
	_ = readProcessTestLine(t, transport)

	if err := transport.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	select {
	case <-processTransport.waitDone:
	case <-time.After(time.Second):
		t.Fatal("process was not waited after Close")
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("second Close error: %v", err)
	}
}

func TestAppServerProcessTransportReadFailureIncludesLimitedStderr(t *testing.T) {
	transport := startProcessHelper(t, AppServerProcessStarter{StderrLimit: 64}, "stderr-exit")
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := transport.ReadLine(ctx)
	if err == nil {
		t.Fatal("ReadLine unexpectedly succeeded")
	}
	message := err.Error()
	if !strings.Contains(message, "tail-marker") {
		t.Fatalf("stderr diagnostic missing tail marker: %v", err)
	}
	if !strings.Contains(message, "[truncated]") {
		t.Fatalf("stderr diagnostic did not report truncation: %v", err)
	}
	if len(message) > 220 {
		t.Fatalf("stderr diagnostic grew too large (%d bytes): %v", len(message), err)
	}
}

func TestAppServerProcessTransportRejectsOversizedStdoutLine(t *testing.T) {
	oldLimit := appServerProcessStdoutLineLimit
	appServerProcessStdoutLineLimit = 256
	t.Cleanup(func() { appServerProcessStdoutLineLimit = oldLimit })

	transport := startProcessHelper(t, AppServerProcessStarter{}, "long-stdout-line", "1024")
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := transport.ReadLine(ctx)
	if err == nil {
		t.Fatal("ReadLine unexpectedly succeeded for oversized stdout line")
	}
	if !strings.Contains(err.Error(), "token too long") {
		t.Fatalf("ReadLine error = %v, want scanner token-too-long error", err)
	}
}

func TestAppServerProcessDiagnosticWaitsForStderrDrainAfterExit(t *testing.T) {
	transport := &appServerProcessTransport{
		stderrBuffer: newLimitedStderrBuffer(1024),
		stderrDone:   make(chan struct{}),
		waitDone:     make(chan struct{}),
	}
	close(transport.waitDone)

	go func() {
		time.Sleep(10 * time.Millisecond)
		_, _ = transport.stderrBuffer.Write([]byte("delayed-stderr-marker"))
		close(transport.stderrDone)
	}()

	err := transport.diagnosticError(errors.New("stdout closed"), "read app-server stdout")
	if err == nil {
		t.Fatal("diagnosticError unexpectedly returned nil")
	}
	if !strings.Contains(err.Error(), "delayed-stderr-marker") {
		t.Fatalf("stderr diagnostic did not wait for drain: %v", err)
	}
}

func TestAppServerProcessTransportWriteFailureIncludesStderr(t *testing.T) {
	transport := startProcessHelper(t, AppServerProcessStarter{}, "close-stdin")
	defer transport.Close()
	processTransport := transport.(*appServerProcessTransport)

	select {
	case <-processTransport.waitDone:
	case <-time.After(time.Second):
		t.Fatal("helper did not exit")
	}

	err := transport.WriteLine(context.Background(), []byte(`{"after":"exit"}`))
	if err == nil {
		t.Fatal("WriteLine unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "write-marker") {
		t.Fatalf("stderr diagnostic missing write marker: %v", err)
	}
}

func TestAppServerProcessTransportReadTimeoutCleansUpProcess(t *testing.T) {
	transport := startProcessHelper(t, AppServerProcessStarter{}, "slow")
	processTransport := transport.(*appServerProcessTransport)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := transport.ReadLine(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	select {
	case <-processTransport.waitDone:
	case <-time.After(time.Second):
		t.Fatal("process was not cleaned up after ReadLine timeout")
	}
}

func TestAppServerProcessStarterStartFailureAndCanceledContext(t *testing.T) {
	missingCommand := filepath.Join(t.TempDir(), "missing-codex")
	_, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{
		Command: missingCommand,
	})
	if err == nil ||
		!strings.Contains(err.Error(), "start app-server process") ||
		!strings.Contains(err.Error(), filepath.Base(missingCommand)) {
		t.Fatalf("start failure did not include command path: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (AppServerProcessStarter{}).StartAppServer(ctx, AppServerStartRequest{
		Command: os.Args[0],
		Args:    []string{"-test.run=TestAppServerProcessHelper", "--", appServerProcessHelperMarker, "ready-block"},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context, got %v", err)
	}
}

func TestAppServerProcessHelper(t *testing.T) {
	args, ok := appServerProcessHelperArgs()
	if !ok {
		return
	}
	os.Exit(runAppServerProcessHelper(args))
}

func startProcessHelper(t *testing.T, starter AppServerProcessStarter, args ...string) AppServerLineTransport {
	t.Helper()
	command, commandArgs := appServerProcessHelperCommand(args...)
	transport, err := starter.StartAppServer(context.Background(), AppServerStartRequest{
		Command: command,
		Args:    commandArgs,
	})
	if err != nil {
		t.Fatalf("StartAppServer error: %v", err)
	}
	return transport
}

func appServerProcessHelperCommand(args ...string) (string, []string) {
	commandArgs := []string{"-test.run=TestAppServerProcessHelper", "--", appServerProcessHelperMarker}
	commandArgs = append(commandArgs, args...)
	return os.Args[0], commandArgs
}

func appServerProcessHelperArgs() ([]string, bool) {
	for i, arg := range os.Args {
		if arg == appServerProcessHelperMarker {
			return os.Args[i+1:], true
		}
	}
	return nil, false
}

func readProcessTestLine(t *testing.T, transport AppServerLineTransport) []byte {
	t.Helper()
	// Windows hosted runners can take a few seconds to create a PowerShell
	// fixture before its first line is available. Keep this bounded, but avoid
	// turning runner startup variance into a false process-lifecycle failure.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	line, err := transport.ReadLine(ctx)
	if err != nil {
		t.Fatalf("ReadLine error: %v", err)
	}
	return line
}

func readAppServerProcessTestEnvironment(t *testing.T, transport AppServerLineTransport) map[string]string {
	t.Helper()
	line := readProcessTestLine(t, transport)
	var got struct {
		Env map[string]string `json:"env"`
	}
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("metadata line is not JSON: %s: %v", string(line), err)
	}
	return got.Env
}

func appServerRuntimeEnvironmentNames() []string {
	return []string{
		helperruntime.EnvRuntime,
		helperruntime.EnvRuntimeRoot,
		helperruntime.EnvRuntimeVersion,
		helperruntime.EnvEntryPath,
		helperruntime.EnvDisable,
		helperruntime.EnvForce,
	}
}

func runAppServerProcessHelper(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "missing helper mode")
		return 2
	}
	switch args[0] {
	case "meta":
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "getwd: %v\n", err)
			return 2
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"cwd":  cwd,
			"args": args,
			"env": map[string]string{
				"CODEX_HELPER_TEAMS_CHILD":      os.Getenv("CODEX_HELPER_TEAMS_CHILD"),
				"CODEX_HELPER_TEAMS_PARENT_PID": os.Getenv("CODEX_HELPER_TEAMS_PARENT_PID"),
				"CODEX_HELPER_BEFORE_CONFIGURE": os.Getenv("CODEX_HELPER_BEFORE_CONFIGURE"),
				"CODEX_HELPER_CONFIGURED":       os.Getenv("CODEX_HELPER_CONFIGURED"),
				"CODEX_HELPER_ENV_KEEP":         os.Getenv("CODEX_HELPER_ENV_KEEP"),
				helperruntime.EnvRuntime:        os.Getenv(helperruntime.EnvRuntime),
				helperruntime.EnvRuntimeRoot:    os.Getenv(helperruntime.EnvRuntimeRoot),
				helperruntime.EnvRuntimeVersion: os.Getenv(helperruntime.EnvRuntimeVersion),
				helperruntime.EnvEntryPath:      os.Getenv(helperruntime.EnvEntryPath),
				helperruntime.EnvDisable:        os.Getenv(helperruntime.EnvDisable),
				helperruntime.EnvForce:          os.Getenv(helperruntime.EnvForce),
			},
		})
		time.Sleep(24 * time.Hour)
		return 0
	case "echo":
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"echo": scanner.Text()})
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "scan stdin: %v\n", err)
			return 3
		}
		return 0
	case "mcp-jsonl":
		return runMCPJSONLProcessHelper(args[1:])
	case "ready-block":
		fmt.Fprintln(os.Stdout, `{"ready":true}`)
		time.Sleep(24 * time.Hour)
		return 0
	case "stderr-exit":
		fmt.Fprint(os.Stderr, strings.Repeat("prefix-", 80), "tail-marker")
		return 4
	case "long-stdout-line":
		size := appServerProcessStdoutLineLimit + 512
		if len(args) > 1 {
			if parsed, err := strconv.Atoi(args[1]); err == nil && parsed > 0 {
				size = parsed
			}
		}
		fmt.Fprint(os.Stdout, strings.Repeat("x", size))
		time.Sleep(24 * time.Hour)
		return 0
	case "close-stdin":
		fmt.Fprint(os.Stderr, "write-marker")
		return 5
	case "slow":
		fmt.Fprint(os.Stderr, "slow-marker")
		time.Sleep(24 * time.Hour)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", args[0])
		return 2
	}
}

// runMCPJSONLProcessHelper is a real child-process JSONL harness for the
// runner/coordinator tests. It deliberately models only the app-server
// methods needed by the test; it does not require a model, network, or MCP
// credentials. Each request is handled independently so a blocked reload
// cannot prevent a turn/start response from being emitted.
func runMCPJSONLProcessHelper(args []string) int {
	if len(args) < 3 {
		fmt.Fprintln(os.Stderr, "mcp-jsonl requires reload-count, reload-complete, and release paths")
		return 2
	}
	reloadCountPath := args[0]
	reloadCompletePath := args[1]
	releasePath := args[2]
	var outputMu sync.Mutex
	var reloadMu sync.Mutex
	var workers sync.WaitGroup
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var request appServerRequest
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			fmt.Fprintf(os.Stderr, "decode mcp-jsonl request: %v\n", err)
			return 3
		}
		if request.Method == appServerMethodInitialized {
			continue
		}
		requestCopy := request
		workers.Add(1)
		go func() {
			defer workers.Done()
			switch requestCopy.Method {
			case appServerMethodInitialize, appServerMethodThreadList:
				writeMCPJSONLResult(&outputMu, requestCopy.ID, map[string]any{"data": []any{}})
			case appServerMethodTurnStart:
				writeMCPJSONLResult(&outputMu, requestCopy.ID, map[string]any{
					"turn": map[string]any{
						"id":     fmt.Sprintf("turn-%d", requestCopy.ID),
						"status": "completed",
						"items":  []any{map[string]any{"type": "agentMessage", "text": "done"}},
					},
				})
			case appServerMethodMCPServerReload:
				reloadMu.Lock()
				count := 0
				if raw, err := os.ReadFile(reloadCountPath); err == nil {
					count, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
				}
				_ = os.WriteFile(reloadCountPath, []byte(strconv.Itoa(count+1)), 0o600)
				reloadMu.Unlock()
				for {
					if _, err := os.Stat(releasePath); err == nil {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				writeMCPJSONLResult(&outputMu, requestCopy.ID, map[string]any{})
				_ = os.WriteFile(reloadCompletePath, []byte(strconv.Itoa(count+1)), 0o600)
			}
		}()
	}
	workers.Wait()
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan mcp-jsonl stdin: %v\n", err)
		return 4
	}
	return 0
}

func writeMCPJSONLResult(outputMu *sync.Mutex, id int64, result any) {
	outputMu.Lock()
	defer outputMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}
