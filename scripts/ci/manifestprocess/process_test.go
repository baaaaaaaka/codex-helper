package manifestprocess

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

const (
	manifestProcessTreeHelperEnv = "CODEX_HELPER_MANIFEST_PROCESS_TREE_HELPER"
	manifestProcessTreeChildEnv  = "CODEX_HELPER_MANIFEST_PROCESS_TREE_CHILD"
	manifestProcessTreeMarkerEnv = "CODEX_HELPER_MANIFEST_PROCESS_TREE_MARKER"
)

// TestRunTerminatesDescendants protects the watchdog's most important
// cleanup guarantee: killing the go wrapper must also kill a test binary and
// a descendant started by that binary. The descendant exits on its own after
// a short safety bound so a failed implementation cannot leave a permanent
// process behind in a developer or CI environment.
func TestRunTerminatesDescendants(t *testing.T) {
	if os.Getenv(manifestProcessTreeHelperEnv) == "1" || os.Getenv(manifestProcessTreeChildEnv) == "1" {
		return
	}

	marker := t.TempDir() + string(os.PathSeparator) + "grandchild.marker"
	cmd := exec.Command(os.Args[0], "-test.run=^TestManifestProcessTreeHelper$")
	cmd.Env = append(os.Environ(),
		manifestProcessTreeHelperEnv+"=1",
		manifestProcessTreeMarkerEnv+"="+marker,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cmd) }()

	deadline := time.Now().Add(3 * time.Second)
	var before []byte
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(marker)
		if err == nil && len(data) > 0 {
			before = append([]byte(nil), data...)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(before) == 0 {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("Run() did not return after failed process-tree startup")
		}
		t.Fatal("manifest process grandchild did not start")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() after cancellation = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return after terminating the process tree")
	}

	// A kill can race with one final in-flight marker write. Give a correctly
	// terminated tree a small scheduling window, then require that output has
	// remained stable; a direct-only kill keeps changing the marker every 10ms.
	deadline = time.Now().Add(time.Second)
	stableSince := time.Now()
	last := before
	for time.Now().Before(deadline) {
		after, err := os.ReadFile(marker)
		if err != nil {
			t.Fatalf("read process-tree marker after cancellation: %v", err)
		}
		if !bytes.Equal(last, after) {
			last = append(last[:0], after...)
			stableSince = time.Now()
		}
		if time.Since(stableSince) >= 200*time.Millisecond {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("process-tree marker did not stabilize after Run returned: last=%q", last)
}

// TestManifestProcessTreeHelper is executed in the child process spawned by
// TestRunTerminatesDescendants. It creates its descendant immediately, which
// also exercises the small Windows Job Object attachment race. The bounded
// lifetime prevents a failed cleanup implementation from leaking a permanent
// helper process in a developer or CI environment.
func TestManifestProcessTreeHelper(t *testing.T) {
	if os.Getenv(manifestProcessTreeHelperEnv) != "1" {
		return
	}
	marker := os.Getenv(manifestProcessTreeMarkerEnv)
	child := exec.Command(os.Args[0], "-test.run=^TestManifestProcessGrandchild$")
	child.Env = append(os.Environ(),
		manifestProcessTreeChildEnv+"=1",
		manifestProcessTreeMarkerEnv+"="+marker,
	)
	if err := child.Start(); err != nil {
		os.Exit(2)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

// TestManifestProcessGrandchild keeps a monotonic marker alive long enough
// for the parent test to distinguish a killed tree from a direct-only kill.
func TestManifestProcessGrandchild(t *testing.T) {
	if os.Getenv(manifestProcessTreeChildEnv) != "1" {
		return
	}
	marker := os.Getenv(manifestProcessTreeMarkerEnv)
	deadline := time.Now().Add(2 * time.Second)
	for count := 0; time.Now().Before(deadline); count++ {
		if err := os.WriteFile(marker, []byte(strconv.Itoa(count)), 0o600); err != nil {
			os.Exit(3)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
