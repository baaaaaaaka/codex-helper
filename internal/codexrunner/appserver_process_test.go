package codexrunner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
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
	if got.Cwd != workingDir {
		t.Fatalf("working dir = %q, want %q", got.Cwd, workingDir)
	}
	if want := []string{"meta", "arg-one", "arg-two"}; !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("helper args = %#v, want %#v", got.Args, want)
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
	if err == nil || !strings.Contains(err.Error(), missingCommand) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	line, err := transport.ReadLine(ctx)
	if err != nil {
		t.Fatalf("ReadLine error: %v", err)
	}
	return line
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
	case "ready-block":
		fmt.Fprintln(os.Stdout, `{"ready":true}`)
		time.Sleep(24 * time.Hour)
		return 0
	case "stderr-exit":
		fmt.Fprint(os.Stderr, strings.Repeat("prefix-", 80), "tail-marker")
		return 4
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
