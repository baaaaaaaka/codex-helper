//go:build windows

package codexrunner

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAppServerProcessCloseTerminatesWindowsDescendants(t *testing.T) {
	transport, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{
		Command: "powershell.exe",
		Args: []string{
			"-NoProfile",
			"-NonInteractive",
			"-Command",
			`$child = Start-Process ping.exe -ArgumentList '-t','127.0.0.1' -PassThru; [Console]::Out.WriteLine($child.Id); while ($true) { Start-Sleep -Seconds 1 }`,
		},
	})
	if err != nil {
		t.Fatalf("start Windows descendant fixture: %v", err)
	}
	pidRaw := strings.TrimSpace(string(readProcessTestLine(t, transport)))
	pid, err := strconv.Atoi(pidRaw)
	if err != nil || pid <= 0 {
		_ = transport.Close()
		t.Fatalf("descendant PID = %q, want positive integer", pidRaw)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("close app-server transport: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		output, queryErr := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/NH").CombinedOutput()
		if queryErr == nil && !strings.Contains(string(output), strconv.Itoa(pid)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("app-server descendant PID %d still running after Close: %s", pid, strings.TrimSpace(string(output)))
		}
		time.Sleep(50 * time.Millisecond)
	}
}
