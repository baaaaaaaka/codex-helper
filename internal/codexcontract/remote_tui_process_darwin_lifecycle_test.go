//go:build darwin

package codexcontract

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDarwinRemoteTUIProcessKeepsSupervisorStdinOpen(t *testing.T) {
	root := t.TempDir()
	command := filepath.Join(root, "codex-fixture")
	fixture := `#!/bin/sh
if IFS= read -r value; then
  exit 43
fi
exit 42
`
	if err := os.WriteFile(command, []byte(fixture), 0o755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	process, err := startRemoteTUIProcess(ctx, command, "ws://127.0.0.1:1", "", "", filepath.Join(root, "codex-home"))
	if err != nil {
		t.Fatalf("start PTY fixture: %v", err)
	}
	defer process.Stop()

	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("PTY fixture observed synthetic stdin EOF: %v\n%s", err, process.Output())
	case <-time.After(1 * time.Second):
	}

	process.Stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("PTY fixture did not exit after Stop")
	}
	if output := process.Output(); strings.Contains(output, "^D") {
		t.Fatalf("PTY supervisor emitted synthetic Ctrl-D: %q", output)
	}
}
