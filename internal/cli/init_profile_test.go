package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

type fakeSSHOps struct {
	probeErrs   []error
	probes      []config.Profile
	generated   []config.Profile
	keyPath     string
	generateErr error
	installErr  error
	installed   []string
}

func (f *fakeSSHOps) probe(_ context.Context, prof config.Profile, _ bool) error {
	f.probes = append(f.probes, prof)
	if len(f.probeErrs) == 0 {
		return nil
	}
	err := f.probeErrs[0]
	f.probeErrs = f.probeErrs[1:]
	return err
}

func (f *fakeSSHOps) generateKeypair(_ context.Context, _ *config.Store, prof config.Profile) (string, error) {
	f.generated = append(f.generated, prof)
	if f.generateErr != nil {
		return "", f.generateErr
	}
	return f.keyPath, nil
}

func (f *fakeSSHOps) installPublicKey(_ context.Context, _ config.Profile, pubKeyPath string) error {
	f.installed = append(f.installed, pubKeyPath)
	return f.installErr
}

func TestInitProfileInteractiveWithDepsDirectSSHSuccess(t *testing.T) {
	store := newTempStore(t)
	reader := bufio.NewReader(strings.NewReader("\nexample.com\n0\n70000\n2222\n\nalice\n"))
	ops := &fakeSSHOps{}
	var out bytes.Buffer

	prof, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, &out)
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v", err)
	}

	if prof.Host != "example.com" || prof.Port != 2222 || prof.User != "alice" {
		t.Fatalf("unexpected profile: %+v", prof)
	}
	if prof.Name != "alice@example.com" {
		t.Fatalf("unexpected profile name %q", prof.Name)
	}
	if prof.ID == "" || prof.CreatedAt.IsZero() {
		t.Fatalf("expected generated id and creation time, got %+v", prof)
	}
	if len(ops.probes) != 1 {
		t.Fatalf("expected 1 probe, got %d", len(ops.probes))
	}
	if len(ops.installed) != 0 {
		t.Fatalf("expected no key installation, got %v", ops.installed)
	}
	if !strings.Contains(out.String(), "Proxy mode uses an SSH tunnel") {
		t.Fatalf("expected intro text, got %q", out.String())
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].ID != prof.ID {
		t.Fatalf("expected saved profile, got %+v", cfg.Profiles)
	}
}

func TestInitProfileInteractiveWithDepsFallsBackToManagedKey(t *testing.T) {
	store := newTempStore(t)
	reader := bufio.NewReader(strings.NewReader("host.example\n22\ncarol\n"))
	ops := &fakeSSHOps{
		probeErrs: []error{&sshProbeError{kind: sshProbeFailureAuth, err: errors.New("direct ssh failed")}, nil},
		keyPath:   filepath.Join(t.TempDir(), "id_ed25519_test"),
	}
	var out bytes.Buffer

	prof, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, &out)
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v", err)
	}

	if len(ops.probes) != 2 {
		t.Fatalf("expected 2 probes, got %d", len(ops.probes))
	}
	if len(ops.installed) != 1 || ops.installed[0] != ops.keyPath+".pub" {
		t.Fatalf("expected installed pubkey %q, got %v", ops.keyPath+".pub", ops.installed)
	}
	if len(prof.SSHArgs) != 2 || prof.SSHArgs[0] != "-i" || prof.SSHArgs[1] != ops.keyPath {
		t.Fatalf("expected SSHArgs to use generated key, got %v", prof.SSHArgs)
	}
	if !strings.Contains(out.String(), "creating a dedicated codex-proxy key") {
		t.Fatalf("expected fallback message, got %q", out.String())
	}
}

func TestInitProfileInteractiveWithDepsReturnsWrappedKeyProbeError(t *testing.T) {
	store := newTempStore(t)
	reader := bufio.NewReader(strings.NewReader("host.example\n22\ndana\n"))
	ops := &fakeSSHOps{
		probeErrs: []error{
			&sshProbeError{kind: sshProbeFailureAuth, err: errors.New("direct ssh failed")},
			&sshProbeError{kind: sshProbeFailureOther, err: errors.New("still blocked")},
		},
		keyPath: filepath.Join(t.TempDir(), "id_ed25519_test"),
	}

	_, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, ioDiscard{})
	if err == nil {
		t.Fatal("expected key-based probe error")
	}
	if !strings.Contains(err.Error(), "key-based ssh probe failed") {
		t.Fatalf("expected wrapped key probe error, got %v", err)
	}

	cfg, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("load config: %v", loadErr)
	}
	if len(cfg.Profiles) != 0 {
		t.Fatalf("expected no saved profiles after failure, got %+v", cfg.Profiles)
	}
}

func TestInitProfileInteractiveWithDepsDoesNotInstallManagedKeyForNonAuthProbeErrors(t *testing.T) {
	store := newTempStore(t)
	reader := bufio.NewReader(strings.NewReader("host.example\n22\nerin\n"))
	ops := &fakeSSHOps{
		probeErrs: []error{
			&sshProbeError{kind: sshProbeFailureOther, err: errors.New("forwarding disabled"), output: "administratively prohibited"},
		},
		keyPath: filepath.Join(t.TempDir(), "id_ed25519_test"),
	}

	_, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, ioDiscard{})
	if err == nil {
		t.Fatal("expected probe error")
	}
	if len(ops.generated) != 0 {
		t.Fatalf("expected no key generation, got %d", len(ops.generated))
	}
	if len(ops.installed) != 0 {
		t.Fatalf("expected no key installation, got %v", ops.installed)
	}
	if !strings.Contains(err.Error(), "administratively prohibited") {
		t.Fatalf("expected original probe error, got %v", err)
	}
}

func TestSSHProbeUsesTunnelReadinessForNonInteractiveChecks(t *testing.T) {
	lockCLITestHooks(t)

	dir := t.TempDir()
	helperBin, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test binary: %v", err)
	}
	writeStub(
		t,
		dir,
		"ssh",
		fmt.Sprintf("#!/bin/sh\nexec %q -test.run=TestSSHProbeHelperProcess -- \"$@\"\n", helperBin),
		fmt.Sprintf("@echo off\r\n\"%s\" -test.run=TestSSHProbeHelperProcess -- %%*\r\n", helperBin),
	)
	t.Setenv("PATH", dir)
	t.Setenv("GO_WANT_SSH_HELPER_PROCESS", "1")
	t.Setenv("SSH_HELPER_MODE", "socks-ready")

	err = sshProbe(context.Background(), config.Profile{
		Host: "example.com",
		Port: 3211,
		User: "starh",
	}, false)
	if err != nil {
		t.Fatalf("sshProbe error: %v", err)
	}
}

func TestSSHProbeOmitsTrailingColonWhenSSHReturnsNoOutput(t *testing.T) {
	lockCLITestHooks(t)

	dir := t.TempDir()
	helperBin, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test binary: %v", err)
	}
	writeStub(
		t,
		dir,
		"ssh",
		fmt.Sprintf("#!/bin/sh\nexec %q -test.run=TestSSHProbeHelperProcess -- \"$@\"\n", helperBin),
		fmt.Sprintf("@echo off\r\n\"%s\" -test.run=TestSSHProbeHelperProcess -- %%*\r\n", helperBin),
	)
	t.Setenv("PATH", dir)
	t.Setenv("GO_WANT_SSH_HELPER_PROCESS", "1")
	t.Setenv("SSH_HELPER_MODE", "empty-fail")

	err = sshProbe(context.Background(), config.Profile{
		Host: "example.com",
		Port: 3211,
		User: "starh",
	}, false)
	if err == nil {
		t.Fatal("expected sshProbe to fail")
	}
	if strings.HasSuffix(err.Error(), ":") {
		t.Fatalf("expected error without trailing colon, got %q", err.Error())
	}
	if strings.Contains(err.Error(), "exit status 1:") {
		t.Fatalf("expected empty stderr not to add a dangling separator, got %q", err.Error())
	}
}

func TestSSHProbeHonorsContextCancellation(t *testing.T) {
	lockCLITestHooks(t)

	dir := t.TempDir()
	helperBin, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test binary: %v", err)
	}
	writeStub(
		t,
		dir,
		"ssh",
		fmt.Sprintf("#!/bin/sh\nexec %q -test.run=TestSSHProbeHelperProcess -- \"$@\"\n", helperBin),
		fmt.Sprintf("@echo off\r\n\"%s\" -test.run=TestSSHProbeHelperProcess -- %%*\r\n", helperBin),
	)
	t.Setenv("PATH", dir)
	t.Setenv("GO_WANT_SSH_HELPER_PROCESS", "1")
	t.Setenv("SSH_HELPER_MODE", "block")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err = sshProbe(ctx, config.Profile{
		Host: "example.com",
		Port: 3211,
		User: "starh",
	}, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("expected prompt cancellation, took %s", time.Since(start))
	}
}

func TestEnsureProfileAutoInitUsesInteractiveInitializer(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	prevInit := initProfileInteractiveFunc
	t.Cleanup(func() { initProfileInteractiveFunc = prevInit })

	initProfileInteractiveFunc = func(_ context.Context, s *config.Store) (config.Profile, error) {
		prof := config.Profile{ID: "p-auto", Name: "dev", Host: "host", Port: 22, User: "alice"}
		if err := s.Update(func(cfg *config.Config) error {
			cfg.UpsertProfile(prof)
			return nil
		}); err != nil {
			return config.Profile{}, err
		}
		return prof, nil
	}

	prof, cfg, err := ensureProfile(context.Background(), store, "", true, nil)
	if err != nil {
		t.Fatalf("ensureProfile error: %v", err)
	}
	if prof.ID != "p-auto" {
		t.Fatalf("expected created profile, got %+v", prof)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].ID != "p-auto" {
		t.Fatalf("expected loaded config to include created profile, got %+v", cfg.Profiles)
	}
}

func TestEnsureProfileReturnsCreatedProfileWhenRequestedRefStillMissing(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	prevInit := initProfileInteractiveFunc
	t.Cleanup(func() { initProfileInteractiveFunc = prevInit })

	initProfileInteractiveFunc = func(context.Context, *config.Store) (config.Profile, error) {
		return config.Profile{ID: "created", Name: "new"}, nil
	}

	prof, _, err := ensureProfile(context.Background(), store, "missing", true, nil)
	if err != nil {
		t.Fatalf("ensureProfile error: %v", err)
	}
	if prof.ID != "created" {
		t.Fatalf("expected created profile fallback, got %+v", prof)
	}
}

func TestNewInitCmdUsesInteractiveInitializer(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	prevInit := initProfileInteractiveFunc
	t.Cleanup(func() { initProfileInteractiveFunc = prevInit })

	initProfileInteractiveFunc = func(context.Context, *config.Store) (config.Profile, error) {
		return config.Profile{ID: "p1", Name: "dev@host"}, nil
	}

	cmd := newInitCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{})
	var out bytes.Buffer
	cmd.SetOut(&out)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute init cmd: %v", err)
	}
	if !strings.Contains(out.String(), `Saved profile "dev@host" (p1)`) {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

func TestSSHProbeHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_SSH_HELPER_PROCESS") != "1" {
		return
	}

	args := os.Args
	sep := -1
	for i, arg := range args {
		if arg == "--" {
			sep = i
			break
		}
	}
	if sep < 0 {
		return
	}
	args = args[sep+1:]

	switch os.Getenv("SSH_HELPER_MODE") {
	case "socks-ready":
		for _, arg := range args {
			if arg == "exit" {
				os.Exit(1)
			}
		}

		spec := ""
		for i := 0; i < len(args)-1; i++ {
			if args[i] == "-D" {
				spec = args[i+1]
				break
			}
		}
		if spec == "" {
			_, _ = fmt.Fprint(os.Stderr, "missing -D")
			os.Exit(1)
		}

		host, port, err := net.SplitHostPort(spec)
		if err != nil {
			_, _ = fmt.Fprint(os.Stderr, err.Error())
			os.Exit(1)
		}
		ln, err := net.Listen("tcp", net.JoinHostPort(host, port))
		if err != nil {
			_, _ = fmt.Fprint(os.Stderr, err.Error())
			os.Exit(1)
		}
		defer ln.Close()

		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}

	case "empty-fail":
		os.Exit(1)

	case "block":
		time.Sleep(30 * time.Second)
	}
}
