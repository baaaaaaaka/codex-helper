package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/userpath"
)

func TestTeamsPathExplicitCaptureAndModeCommandsPersistPolicy(t *testing.T) {
	lockCLITestHooks(t)
	configPath := filepath.Join(t.TempDir(), "config.json")

	execute := func(args ...string) string {
		t.Helper()
		cmd := newRootCmd()
		cmd.SetArgs(append([]string{"--config", configPath, "teams", "path"}, args...))
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("execute %v: %v\n%s", args, err, out.String())
		}
		return out.String()
	}

	execute("explicit", "/custom/bin:/usr/bin")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TeamsCodexPath.Mode != "explicit" || cfg.TeamsCodexPath.ExplicitPath != "/custom/bin:/usr/bin" {
		t.Fatalf("explicit policy = %#v", cfg.TeamsCodexPath)
	}

	t.Setenv("PATH", "/terminal/venv/bin:/usr/bin")
	execute("capture-terminal")
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TeamsCodexPath.Mode != "captured-terminal" || cfg.TeamsCodexPath.ExplicitPath != "/terminal/venv/bin:/usr/bin" {
		t.Fatalf("captured policy = %#v", cfg.TeamsCodexPath)
	}

	execute("mode", "service")
	cfg, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TeamsCodexPath.Mode != "service" {
		t.Fatalf("mode policy = %#v", cfg.TeamsCodexPath)
	}
}

func TestTeamsPathStatusRedactsCompletePathByDefault(t *testing.T) {
	lockCLITestHooks(t)
	previous := teamsUserPathResolver
	teamsUserPathResolver = cliTestUserPathResolverFunc(func(context.Context, userpath.Request) (userpath.Result, error) {
		return userpath.Result{
			Path: "/private/project/bin:/usr/bin", Mode: userpath.ModeAccountDefault,
			Source: "account-shell", AccountShell: "/bin/zsh", Adapter: "zsh",
			BaselineSource: "unix-login-default", Fingerprint: "0123456789abcdef", EntryCount: 2,
		}, nil
	})
	t.Cleanup(func() { teamsUserPathResolver = previous })

	configPath := filepath.Join(t.TempDir(), "config.json")
	cmd := newRootCmd()
	cmd.SetArgs([]string{"--config", configPath, "teams", "path", "status"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "/private/project") || !strings.Contains(out.String(), "PATH fingerprint: 0123456789abcdef") {
		t.Fatalf("redacted status output:\n%s", out.String())
	}

	cmd = newRootCmd()
	cmd.SetArgs([]string{"--config", configPath, "teams", "path", "status", "--show-path"})
	out.Reset()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "/private/project/bin:/usr/bin") {
		t.Fatalf("show-path output:\n%s", out.String())
	}
}

func TestTeamsPathShellRequiresAbsolutePath(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	cmd := newRootCmd()
	cmd.SetArgs([]string{"--config", configPath, "teams", "path", "shell", "zsh"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("relative shell override succeeded")
	}
	if _, err := os.Stat(configPath); !os.IsNotExist(err) {
		t.Fatalf("invalid command should not create config: %v", err)
	}
}

func TestTeamsPathShellDefaultClearsOverride(t *testing.T) {
	lockCLITestHooks(t)
	configPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{TeamsCodexPath: config.TeamsCodexPathPolicy{Mode: "account-default", ShellOverride: "/bin/zsh"}}); err != nil {
		t.Fatal(err)
	}
	cmd := newRootCmd()
	cmd.SetArgs([]string{"--config", configPath, "teams", "path", "shell", "default"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TeamsCodexPath.Mode != "account-default" || cfg.TeamsCodexPath.ShellOverride != "" {
		t.Fatalf("shell policy = %#v", cfg.TeamsCodexPath)
	}
}
