//go:build !windows

package cli

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

const (
	detachedLifecycleModeEnv  = "CODEX_HELPER_DETACHED_LIFECYCLE_MODE"
	detachedLifecycleReadyEnv = "CODEX_HELPER_DETACHED_LIFECYCLE_READY"
)

// TestDetachedDaemonSurvivesParentSessionHangup models closing the terminal
// that launched a long-lived daemon. The launcher is placed in its own process
// group, starts the daemon through the production detached configuration, and
// then sends SIGHUP to its own group. A daemon that did not call setsid would
// receive the same signal and fail the health check below.
func TestDetachedDaemonSurvivesParentSessionHangup(t *testing.T) {
	switch os.Getenv(detachedLifecycleModeEnv) {
	case "child":
		runDetachedLifecycleChild()
		return
	case "parent":
		runDetachedLifecycleParent()
		return
	}

	root := t.TempDir()
	readyPath := filepath.Join(root, "ready")
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	launcher := exec.Command(exe, "-test.run=^TestDetachedDaemonSurvivesParentSessionHangup$")
	launcher.Env = append(os.Environ(),
		detachedLifecycleModeEnv+"=parent",
		detachedLifecycleReadyEnv+"="+readyPath,
	)
	launcher.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := launcher.Start(); err != nil {
		t.Fatalf("start detached lifecycle launcher: %v", err)
	}

	pid, port, err := waitForDetachedLifecycleReady(readyPath, 5*time.Second)
	if err != nil {
		_ = launcher.Process.Kill()
		_ = launcher.Wait()
		t.Fatal(err)
	}
	cleanup := func() {
		if pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	t.Cleanup(cleanup)

	if err := launcher.Wait(); err == nil {
		t.Fatal("launcher survived the simulated terminal hangup")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := checkDetachedLifecycleHealth(port); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("detached daemon pid %d on port %d did not survive parent SIGHUP", pid, port)
}

func runDetachedLifecycleParent() {
	readyPath := strings.TrimSpace(os.Getenv(detachedLifecycleReadyEnv))
	if readyPath == "" {
		lifecycleHelperFatal("ready path is required")
	}
	exe, err := helperpath.RawExecutable()
	if err != nil {
		lifecycleHelperFatal("resolve helper executable: %v", err)
	}
	child := exec.Command(exe, "-test.run=^TestDetachedDaemonSurvivesParentSessionHangup$")
	child.Env = append(os.Environ(),
		detachedLifecycleModeEnv+"=child",
		detachedLifecycleReadyEnv+"="+readyPath,
	)
	configureTeamsServiceDetachedCommand(child)
	if _, err := startDetachedProcess(child); err != nil {
		lifecycleHelperFatal("start detached child: %v", err)
	}
	if _, _, err := waitForDetachedLifecycleReady(readyPath, 5*time.Second); err != nil {
		lifecycleHelperFatal("wait for detached child: %v", err)
	}
	if err := syscall.Kill(0, syscall.SIGHUP); err != nil {
		lifecycleHelperFatal("send launcher SIGHUP: %v", err)
	}
	// The default SIGHUP action terminates this launcher. Keep the process alive
	// only long enough to make an unexpected ignored signal observable.
	time.Sleep(5 * time.Second)
	lifecycleHelperFatal("launcher ignored SIGHUP")
}

func runDetachedLifecycleChild() {
	readyPath := strings.TrimSpace(os.Getenv(detachedLifecycleReadyEnv))
	if readyPath == "" {
		lifecycleHelperFatal("ready path is required")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		lifecycleHelperFatal("listen health endpoint: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := os.WriteFile(readyPath, []byte(strconv.Itoa(os.Getpid())+"\n"+strconv.Itoa(port)+"\n"), 0o600); err != nil {
		_ = listener.Close()
		lifecycleHelperFatal("write readiness: %v", err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})}
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		lifecycleHelperFatal("serve health endpoint: %v", err)
	}
}

func waitForDetachedLifecycleReady(path string, timeout time.Duration) (int, int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			fields := strings.Fields(string(data))
			if len(fields) == 2 {
				pid, pidErr := strconv.Atoi(fields[0])
				port, portErr := strconv.Atoi(fields[1])
				if pidErr == nil && portErr == nil && pid > 0 && port > 0 {
					return pid, port, nil
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return 0, 0, fmt.Errorf("timed out waiting for detached lifecycle readiness at %s", path)
}

func checkDetachedLifecycleHealth(port int) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health status = %s", resp.Status)
	}
	return nil
}

func lifecycleHelperFatal(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
