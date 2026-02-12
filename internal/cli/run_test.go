package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
	"github.com/spf13/cobra"
)

func TestSelectProfile(t *testing.T) {
	cfg := config.Config{
		Profiles: []config.Profile{
			{ID: "one", Name: "first"},
			{ID: "two", Name: "second"},
		},
	}

	if _, err := selectProfile(cfg, "one"); err != nil {
		t.Fatalf("expected profile by ID, got error %v", err)
	}
	if _, err := selectProfile(cfg, "second"); err != nil {
		t.Fatalf("expected profile by name, got error %v", err)
	}
	if _, err := selectProfile(cfg, "missing"); err == nil {
		t.Fatalf("expected missing profile error")
	}
	if _, err := selectProfile(cfg, ""); err == nil {
		t.Fatalf("expected error when multiple profiles exist without ref")
	}
}

func TestSelectProfileSingleDefault(t *testing.T) {
	cfg := config.Config{
		Profiles: []config.Profile{
			{ID: "only", Name: "only-profile"},
		},
	}
	p, err := selectProfile(cfg, "")
	if err != nil {
		t.Fatalf("expected single profile to be selected by default, got error %v", err)
	}
	if p.ID != "only" {
		t.Fatalf("expected profile ID %q, got %q", "only", p.ID)
	}
}

func TestRunTargetSupervisedSuccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "ok.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	if err := runTargetSupervised(context.Background(), []string{script}, "", nil, nil); err != nil {
		t.Fatalf("runTargetSupervised error: %v", err)
	}
}

func TestRunTargetOnceWithOptionsNoProxyKeepsEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	outFile := filepath.Join(dir, "env.txt")
	script := filepath.Join(dir, "print.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf \"%s\" \"$HTTP_PROXY\" > \"$OUT_FILE\"\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	t.Setenv("HTTP_PROXY", "http://example.com")
	opts := runTargetOptions{
		ExtraEnv: []string{"OUT_FILE=" + outFile},
		UseProxy: false,
	}

	if err := runTargetOnceWithOptions(context.Background(), []string{script}, "http://127.0.0.1:9999", nil, nil, &bytes.Buffer{}, &bytes.Buffer{}, opts); err != nil {
		t.Fatalf("runTargetOnceWithOptions error: %v", err)
	}
	content, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read env file: %v", err)
	}
	if got := string(content); got != "http://example.com" {
		t.Fatalf("expected HTTP_PROXY preserved, got %q", got)
	}
}

func TestTerminateProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip process signal test on windows")
	}
	cmd := exec.Command("sleep", "5")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	if err := terminateProcess(cmd.Process, 100*time.Millisecond); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("terminateProcess error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected process to exit")
	}
}

func TestRunTargetWithFallbackDisablesYolo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "yolo.sh")
	content := "#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = \"--yolo\" ]; then\n    echo \"unknown flag: --yolo\" >&2\n    exit 2\n  fi\ndone\nexit 0\n"
	if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	disabled := false
	opts := runTargetOptions{
		UseProxy:    false,
		PreserveTTY: false,
		YoloEnabled: true,
		OnYoloFallback: func() error {
			disabled = true
			return nil
		},
	}
	cmdArgs := []string{script, "--yolo"}
	if err := runTargetWithFallbackWithOptions(context.Background(), cmdArgs, "", nil, nil, opts); err != nil {
		t.Fatalf("runTargetWithFallbackWithOptions error: %v", err)
	}
	if !disabled {
		t.Fatalf("expected yolo to be disabled on failure")
	}
}

func TestLimitedBufferWrite(t *testing.T) {
	buf := &limitedBuffer{max: 5}
	if _, err := buf.Write([]byte("abc")); err != nil {
		t.Fatalf("write error: %v", err)
	}
	if got := buf.String(); got != "abc" {
		t.Fatalf("expected %q, got %q", "abc", got)
	}
	if _, err := buf.Write([]byte("def")); err != nil {
		t.Fatalf("write error: %v", err)
	}
	if got := buf.String(); got != "bcdef" {
		t.Fatalf("expected %q, got %q", "bcdef", got)
	}

	buf = &limitedBuffer{max: 5}
	_, _ = buf.Write([]byte("0123456789"))
	if got := buf.String(); got != "56789" {
		t.Fatalf("expected %q, got %q", "56789", got)
	}

	buf = &limitedBuffer{max: 0}
	_, _ = buf.Write([]byte("abc"))
	if got := buf.String(); got != "" {
		t.Fatalf("expected empty buffer, got %q", got)
	}
}

func TestRunLikeRejectsMultipleProfiles(t *testing.T) {
	cmd := &cobra.Command{}
	if err := cmd.Flags().Parse([]string{"a", "b"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	root := &rootOptions{}
	if err := runLike(cmd, root, false); err == nil {
		t.Fatalf("expected error for multiple profile args")
	}
}

// startHealthServer starts an HTTP server that responds to the codex-proxy
// health endpoint for the given instanceID. Returns the port and a cleanup function.
func startHealthServer(t *testing.T, instanceID string) int {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/_codex_proxy/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":         true,
			"instanceId": instanceID,
		})
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().(*net.TCPAddr).Port
}

func TestRunWithProfileOptionsUsesSnapshotFirst(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	instanceID := "test-inst-snapshot"
	httpPort := startHealthServer(t, instanceID)

	// Fresh store with NO instances on disk.
	store := newTempStore(t)

	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		t.Fatal("stackStart should not be called when snapshot already has instance")
		return nil, nil
	}

	dir := t.TempDir()
	script := filepath.Join(dir, "ok.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	now := time.Now()
	instances := []config.Instance{{
		ID:         instanceID,
		ProfileID:  "prof-1",
		HTTPPort:   httpPort,
		SocksPort:  0,
		DaemonPID:  os.Getpid(),
		StartedAt:  now,
		LastSeenAt: now,
	}}

	profile := config.Profile{ID: "prof-1", Name: "test"}
	err := runWithProfileOptions(
		context.Background(),
		store,
		profile,
		instances,
		[]string{script},
		defaultRunTargetOptions(),
	)
	if err != nil {
		t.Fatalf("runWithProfileOptions error: %v", err)
	}
}

func TestRunWithProfileOptionsCreatesNewStack(t *testing.T) {
	store := newTempStore(t)

	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	sentinel := errors.New("mock: stackStart called")
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		return nil, sentinel
	}

	profile := config.Profile{ID: "prof-1", Name: "test"}
	// Both snapshot and disk are empty → must fall through to new stack.
	err := runWithProfileOptions(
		context.Background(),
		store,
		profile,
		nil,
		[]string{"true"},
		defaultRunTargetOptions(),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel from stackStart, got: %v", err)
	}
}

func TestRunWithProfileOptionsLoadErrorFallsThrough(t *testing.T) {
	// Create a store backed by corrupt JSON so store.Load() fails.
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, []byte("not valid json"), 0o600); err != nil {
		t.Fatalf("write corrupt config: %v", err)
	}
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	sentinel := errors.New("mock: stackStart after load error")
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		return nil, sentinel
	}

	profile := config.Profile{ID: "prof-1", Name: "test"}
	// Snapshot empty, store.Load fails → should still fall through to new stack.
	err = runWithProfileOptions(
		context.Background(),
		store,
		profile,
		nil,
		[]string{"true"},
		defaultRunTargetOptions(),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel from stackStart, got: %v", err)
	}
}

func TestRunWithProfileOptionsSkipsWrongProfile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	// Health server for a different profile's instance.
	instanceID := "inst-wrong-prof"
	httpPort := startHealthServer(t, instanceID)

	store := newTempStore(t)
	now := time.Now()
	// Write instance with profileID "other" to disk.
	if err := store.Update(func(cfg *config.Config) error {
		cfg.UpsertInstance(config.Instance{
			ID:         instanceID,
			ProfileID:  "other",
			HTTPPort:   httpPort,
			SocksPort:  0,
			DaemonPID:  os.Getpid(),
			StartedAt:  now,
			LastSeenAt: now,
		})
		return nil
	}); err != nil {
		t.Fatalf("record instance: %v", err)
	}

	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	sentinel := errors.New("mock: stackStart for correct profile")
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		return nil, sentinel
	}

	// Request profile "prof-1" — neither snapshot nor disk has a match.
	profile := config.Profile{ID: "prof-1", Name: "test"}
	err := runWithProfileOptions(
		context.Background(),
		store,
		profile,
		nil,
		[]string{"true"},
		defaultRunTargetOptions(),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel (wrong profile should not match), got: %v", err)
	}
}

func TestRunWithProfileOptionsRefreshesInstances(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	instanceID := "test-inst-refresh"
	httpPort := startHealthServer(t, instanceID)

	// Create a store and record the instance on disk.
	store := newTempStore(t)
	now := time.Now()
	inst := config.Instance{
		ID:         instanceID,
		ProfileID:  "prof-1",
		HTTPPort:   httpPort,
		SocksPort:  0,
		DaemonPID:  os.Getpid(),
		StartedAt:  now,
		LastSeenAt: now,
	}
	if err := store.Update(func(cfg *config.Config) error {
		cfg.UpsertInstance(inst)
		return nil
	}); err != nil {
		t.Fatalf("record instance: %v", err)
	}

	// Override stackStart so we can detect if it's called (it shouldn't be).
	origStackStart := stackStart
	defer func() { stackStart = origStackStart }()
	stackStart = func(_ config.Profile, _ string, _ stack.Options) (*stack.Stack, error) {
		t.Fatal("stackStart should not be called when refresh finds an instance")
		return nil, nil
	}

	// Create a simple script that exits 0.
	dir := t.TempDir()
	script := filepath.Join(dir, "ok.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	profile := config.Profile{ID: "prof-1", Name: "test"}
	// Pass nil instances (simulating a stale/empty snapshot).
	// runWithProfileOptions should reload from disk and find the instance.
	err := runWithProfileOptions(
		context.Background(),
		store,
		profile,
		nil,
		[]string{script},
		defaultRunTargetOptions(),
	)
	if err != nil {
		t.Fatalf("runWithProfileOptions error: %v", err)
	}
}
