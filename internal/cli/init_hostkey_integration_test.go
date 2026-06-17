//go:build linux

package cli

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitProfileHostKeyRetryIntegration(t *testing.T) {
	if os.Getenv("SSH_TEST_ENABLED") != "1" {
		t.Skip("SSH integration tests disabled")
	}
	if os.Getenv("SSH_HOSTKEY_RETRY_TEST") != "1" {
		t.Skip("SSH host-key retry integration test disabled")
	}
	host := os.Getenv("SSH_TEST_HOST")
	port := os.Getenv("SSH_TEST_PORT")
	user := os.Getenv("SSH_TEST_USER")
	key := os.Getenv("SSH_TEST_KEY")
	if host == "" || port == "" || user == "" || key == "" {
		t.Skip("missing SSH_TEST_* env vars")
	}
	if _, err := os.Stat(key); err != nil {
		t.Skipf("SSH test key unavailable: %v", err)
	}

	home := t.TempDir()
	sshDir := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDir, 0o700); err != nil {
		t.Fatalf("mkdir .ssh: %v", err)
	}
	configPath := filepath.Join(sshDir, "config")
	configText := strings.Join([]string{
		"Host " + host,
		"  IdentityFile " + key,
		"  IdentitiesOnly yes",
		"  StrictHostKeyChecking ask",
		"  UserKnownHostsFile " + filepath.Join(sshDir, "known_hosts"),
		"  GlobalKnownHostsFile /dev/null",
		"  GSSAPIAuthentication no",
		"",
	}, "\n")
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatalf("write ssh config: %v", err)
	}
	t.Setenv("HOME", home)

	store := newTempStore(t)
	input := strings.Join([]string{
		"n",
		host,
		port,
		user,
		"y",
		"yes",
		"",
	}, "\n")
	var out bytes.Buffer
	prof, err := initProfileInteractiveWithDeps(
		context.Background(),
		store,
		bufio.NewReader(strings.NewReader(input)),
		defaultSSHOps{},
		&out,
	)
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v\noutput:\n%s", err, out.String())
	}
	if prof.Host != host || prof.User != user {
		t.Fatalf("unexpected profile: %+v", prof)
	}
	if prof.SSHArgs != nil {
		t.Fatalf("expected direct SSH profile without managed key args, got %v", prof.SSHArgs)
	}
	if _, err := os.Stat(filepath.Join(sshDir, "known_hosts")); err != nil {
		t.Fatalf("expected interactive host-key check to write known_hosts: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].ID != prof.ID {
		t.Fatalf("expected saved profile, got %+v", cfg.Profiles)
	}
}

func TestInitProfileSSHConfigProfileIntegration(t *testing.T) {
	if os.Getenv("SSH_TEST_ENABLED") != "1" {
		t.Skip("SSH integration tests disabled")
	}
	if os.Getenv("SSH_CONFIG_PROFILE_INIT_TEST") != "1" {
		t.Skip("SSH config profile init integration test disabled")
	}
	host := os.Getenv("SSH_TEST_HOST")
	port := os.Getenv("SSH_TEST_PORT")
	user := os.Getenv("SSH_TEST_USER")
	key := os.Getenv("SSH_TEST_KEY")
	if host == "" || port == "" || user == "" || key == "" {
		t.Skip("missing SSH_TEST_* env vars")
	}
	if _, err := os.Stat(key); err != nil {
		t.Skipf("SSH test key unavailable: %v", err)
	}

	home := t.TempDir()
	sshDir := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDir, 0o700); err != nil {
		t.Fatalf("mkdir .ssh: %v", err)
	}
	configPath := filepath.Join(sshDir, "config")
	knownHosts := filepath.Join(sshDir, "known_hosts")
	configText := strings.Join([]string{
		"Host cxp-live",
		"  HostName " + host,
		"  User " + user,
		"  Port " + port,
		"  IdentityFile " + key,
		"  IdentitiesOnly yes",
		"  UserKnownHostsFile " + knownHosts,
		"  GlobalKnownHostsFile /dev/null",
		"  GSSAPIAuthentication no",
		"",
	}, "\n")
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatalf("write ssh config: %v", err)
	}
	t.Setenv("HOME", home)

	store := newTempStore(t)
	var out bytes.Buffer
	prof, err := initProfileInteractiveWithDeps(
		context.Background(),
		store,
		bufio.NewReader(strings.NewReader("y\n1\n")),
		defaultSSHOps{},
		&out,
	)
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v\noutput:\n%s", err, out.String())
	}
	if prof.Name != "cxp-live" || prof.Host != "cxp-live" || prof.User != user {
		t.Fatalf("unexpected ssh config profile: %+v", prof)
	}
	if got := prof.SSHArgs; len(got) != 2 || got[0] != "-F" || got[1] != configPath {
		t.Fatalf("SSHArgs = %#v, want -F config", got)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].ID != prof.ID {
		t.Fatalf("expected saved profile, got %+v", cfg.Profiles)
	}
	if !strings.Contains(out.String(), "Found 1 SSH config host entries") {
		t.Fatalf("expected ssh config discovery output, got %q", out.String())
	}
}
