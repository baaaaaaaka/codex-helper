package codex

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Configure sees the command with its environment before it starts and can
// change it; its error fails Start (and nothing is left running).
func TestStartConfigureHook(t *testing.T) {
	_, url := newBrain(t)
	cfg := fakeConfig(t, url, t.TempDir())
	var env []string
	cfg.Configure = func(cmd *exec.Cmd) error {
		env = append([]string(nil), cmd.Env...)
		cmd.Env = append(cmd.Env, "CXP_CONFIGURED=1")
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.Close(5 * time.Second)
	if !strings.Contains(strings.Join(env, "\n"), envFake+"=1") {
		t.Fatalf("Configure did not see the configured environment")
	}
	cfg.Configure = func(*exec.Cmd) error { return errors.New("no identity") }
	if _, err := Start(ctx, cfg); err == nil || !strings.Contains(err.Error(), "no identity") {
		t.Fatalf("Start = %v", err)
	}
}
