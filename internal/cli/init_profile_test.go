package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	internalssh "github.com/baaaaaaaka/codex-helper/internal/ssh"
)

type fakeSSHOps struct {
	probeErrs        []error
	probes           []config.Profile
	probeInteractive []bool
	probeStdin       []io.Reader
	generated        []config.Profile
	keyPath          string
	generateErr      error
	installErr       error
	installed        []string
}

func (f *fakeSSHOps) probe(_ context.Context, prof config.Profile, interactive bool, stdin io.Reader) error {
	f.probes = append(f.probes, prof)
	f.probeInteractive = append(f.probeInteractive, interactive)
	f.probeStdin = append(f.probeStdin, stdin)
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

func withSSHConfigInitTestHooks(t *testing.T, path string, user string) {
	t.Helper()
	prevPath := sshConfigPathForInit
	prevUser := sshConfigCurrentUserName
	prevResolver := resolveSSHConfigProfileForInit
	t.Cleanup(func() {
		sshConfigPathForInit = prevPath
		sshConfigCurrentUserName = prevUser
		resolveSSHConfigProfileForInit = prevResolver
	})
	sshConfigPathForInit = func() (string, error) { return path, nil }
	sshConfigCurrentUserName = func() string { return user }
	resolveSSHConfigProfileForInit = func(prof sshConfigProfile) (sshConfigProfile, error) {
		return prof, nil
	}
}

func writeSSHConfigForInitTest(t *testing.T, text string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), ".ssh")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir ssh dir: %v", err)
	}
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatalf("write ssh config: %v", err)
	}
	return path
}

func withoutSSHConfigProfilesForInitTest(t *testing.T) {
	t.Helper()
	withSSHConfigInitTestHooks(t, filepath.Join(t.TempDir(), ".ssh", "missing-config"), "fallback-user")
}

func TestParseSSHConfigProfilesListsConcreteHosts(t *testing.T) {
	prevUser := sshConfigCurrentUserName
	t.Cleanup(func() { sshConfigCurrentUserName = prevUser })
	sshConfigCurrentUserName = func() string { return "fallback-user" }

	got := parseSSHConfigProfiles(`
Host shared-defaults
  Port 2022
  User global-user

Host work *.internal !blocked
  HostName work.example.com
  User alice
  Port 2222

Host no-user
  HostName no-user.example.com

Host *
  User star-user
  Port 2023

Match host special
  User ignored

Host work
  User duplicate
`, "/tmp/ssh-config")

	if len(got) != 3 {
		t.Fatalf("profiles = %#v, want 3 concrete deduped hosts", got)
	}
	if got[0].Alias != "shared-defaults" || got[0].User != "global-user" || got[0].Port != 2022 {
		t.Fatalf("first profile = %#v", got[0])
	}
	if got[1].Alias != "work" || got[1].User != "alice" || got[1].Port != 2222 {
		t.Fatalf("work profile = %#v", got[1])
	}
	if got[2].Alias != "no-user" || got[2].User != "star-user" || got[2].Port != 2023 {
		t.Fatalf("no-user profile = %#v", got[2])
	}
}

func TestParseSSHConfigProfilesMatchesOpenSSHFirstWinsAndNegation(t *testing.T) {
	prevUser := sshConfigCurrentUserName
	t.Cleanup(func() { sshConfigCurrentUserName = prevUser })
	sshConfigCurrentUserName = func() string { return "fallback-user" }

	got := parseSSHConfigProfiles(`
Host repeat
  User first-user
  User second-user
  Port 2201
  Port 2202

Host good blocked !blocked
  User visible-user

Host=equals
  User=equals-user
  Port=2203
`, "/tmp/ssh-config")

	if len(got) != 3 {
		t.Fatalf("profiles = %#v, want repeat, good, and equals only", got)
	}
	if got[0].Alias != "repeat" || got[0].User != "first-user" || got[0].Port != 2201 {
		t.Fatalf("repeat profile = %#v, want first values", got[0])
	}
	if got[1].Alias != "good" {
		t.Fatalf("second profile = %#v, want good and not blocked", got[1])
	}
	if got[2].Alias != "equals" || got[2].User != "equals-user" || got[2].Port != 2203 {
		t.Fatalf("equals profile = %#v, want keyword=value support", got[2])
	}
}

func TestInitProfileInteractiveWithDepsUsesSSHConfigProfile(t *testing.T) {
	store := newTempStore(t)
	sshConfigPath := writeSSHConfigForInitTest(t, `
Host work
  HostName work.example.com
  User alice
  Port 2222
  IdentityFile ~/.ssh/id_work
`)
	withSSHConfigInitTestHooks(t, sshConfigPath, "fallback-user")
	reader := bufio.NewReader(strings.NewReader("y\n1\n"))
	ops := &fakeSSHOps{}
	var out bytes.Buffer

	prof, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, &out)
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v", err)
	}

	if prof.Name != "work" || prof.Host != "work" || prof.User != "alice" || prof.Port != 2222 {
		t.Fatalf("unexpected ssh config profile: %+v", prof)
	}
	if got := prof.SSHArgs; len(got) != 2 || got[0] != "-F" || got[1] != sshConfigPath {
		t.Fatalf("SSHArgs = %#v, want -F config", got)
	}
	if len(ops.probes) != 1 || ops.probes[0].Name != "work" {
		t.Fatalf("probes = %#v, want selected ssh config profile probe", ops.probes)
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].Name != "work" {
		t.Fatalf("saved profiles = %#v", cfg.Profiles)
	}
	if !strings.Contains(out.String(), "Found 1 SSH config host entries") {
		t.Fatalf("expected ssh config discovery output, got %q", out.String())
	}
}

func TestInitProfileInteractiveWithDepsRetriesSSHConfigProfileAfterProbeFailure(t *testing.T) {
	store := newTempStore(t)
	sshConfigPath := writeSSHConfigForInitTest(t, `
Host bad
  User alice

Host good
  User bob
  Port 2202
`)
	withSSHConfigInitTestHooks(t, sshConfigPath, "fallback-user")
	reader := bufio.NewReader(strings.NewReader("y\n1\n2\n"))
	ops := &fakeSSHOps{probeErrs: []error{errors.New("connection refused")}}
	var out bytes.Buffer

	prof, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, &out)
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v", err)
	}

	if prof.Name != "good" || prof.User != "bob" || prof.Port != 2202 {
		t.Fatalf("profile = %+v, want good ssh config profile", prof)
	}
	if len(ops.probes) != 2 || ops.probes[0].Name != "bad" || ops.probes[1].Name != "good" {
		t.Fatalf("probes = %#v, want bad then good", ops.probes)
	}
	if !strings.Contains(out.String(), "is not reachable") || !strings.Contains(out.String(), "Choose another SSH config host") {
		t.Fatalf("expected retry guidance, got %q", out.String())
	}
}

func TestInitProfileInteractiveWithDepsSSHConfigProbeFailureThenEOFDoesNotSave(t *testing.T) {
	store := newTempStore(t)
	sshConfigPath := writeSSHConfigForInitTest(t, `
Host bad
  User alice
`)
	withSSHConfigInitTestHooks(t, sshConfigPath, "fallback-user")
	reader := bufio.NewReader(strings.NewReader("y\n1\n"))
	ops := &fakeSSHOps{probeErrs: []error{errors.New("connection refused")}}
	var out bytes.Buffer

	_, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, &out)
	if err == nil {
		t.Fatal("expected EOF after failed ssh config probe")
	}
	if !strings.Contains(err.Error(), "input ended while reading Choose SSH config host") {
		t.Fatalf("expected EOF prompt error, got %v", err)
	}

	cfg, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("load config: %v", loadErr)
	}
	if len(cfg.Profiles) != 0 {
		t.Fatalf("expected no saved profiles after failed probe and EOF, got %+v", cfg.Profiles)
	}
}

func TestInitProfileInteractiveWithDepsProbeFailureThenManualDoesNotSaveFailedConfigProfile(t *testing.T) {
	store := newTempStore(t)
	sshConfigPath := writeSSHConfigForInitTest(t, `
Host bad
  User alice
`)
	withSSHConfigInitTestHooks(t, sshConfigPath, "fallback-user")
	reader := bufio.NewReader(strings.NewReader("y\n1\nmanual\nmanual.example.com\n2222\ncarol\n"))
	ops := &fakeSSHOps{probeErrs: []error{errors.New("connection refused")}}

	prof, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, ioDiscard{})
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v", err)
	}
	if prof.Host != "manual.example.com" || prof.User != "carol" || prof.Port != 2222 {
		t.Fatalf("profile = %+v, want manual profile after failed config probe", prof)
	}

	cfg, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("load config: %v", loadErr)
	}
	if len(cfg.Profiles) != 1 || cfg.Profiles[0].Name != "carol@manual.example.com" {
		t.Fatalf("saved profiles = %#v, want only manual profile", cfg.Profiles)
	}
	if len(ops.probes) != 2 || ops.probes[0].Name != "bad" || ops.probes[1].Host != "manual.example.com" {
		t.Fatalf("probes = %#v, want failed config then manual", ops.probes)
	}
}

func TestInitProfileInteractiveWithDepsSSHConfigEOFDoesNotAcceptDefaults(t *testing.T) {
	store := newTempStore(t)
	sshConfigPath := writeSSHConfigForInitTest(t, `
Host work
  User alice
`)
	withSSHConfigInitTestHooks(t, sshConfigPath, "fallback-user")
	reader := bufio.NewReader(strings.NewReader(""))
	ops := &fakeSSHOps{}

	_, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, ioDiscard{})
	if err == nil {
		t.Fatal("expected EOF before ssh config prompt choice")
	}
	if len(ops.probes) != 0 {
		t.Fatalf("expected no probe when input ends before explicit choice, got %#v", ops.probes)
	}

	cfg, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("load config: %v", loadErr)
	}
	if len(cfg.Profiles) != 0 {
		t.Fatalf("expected no saved profiles after EOF, got %+v", cfg.Profiles)
	}
}

func TestInitProfileInteractiveWithDepsUsesResolvedSSHConfigUserAndPort(t *testing.T) {
	store := newTempStore(t)
	sshConfigPath := writeSSHConfigForInitTest(t, `
Host work
  HostName work.example.com
`)
	withSSHConfigInitTestHooks(t, sshConfigPath, "fallback-user")
	resolveSSHConfigProfileForInit = func(prof sshConfigProfile) (sshConfigProfile, error) {
		if prof.Alias != "work" {
			t.Fatalf("resolver got profile %#v, want work", prof)
		}
		prof.User = "resolved-user"
		prof.Port = 2209
		return prof, nil
	}
	reader := bufio.NewReader(strings.NewReader("y\n1\n"))
	ops := &fakeSSHOps{}

	prof, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, ioDiscard{})
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v", err)
	}
	if prof.User != "resolved-user" || prof.Port != 2209 {
		t.Fatalf("profile = %+v, want resolved user/port", prof)
	}
	if len(ops.probes) != 1 || ops.probes[0].User != "resolved-user" || ops.probes[0].Port != 2209 {
		t.Fatalf("probes = %#v, want resolved user/port", ops.probes)
	}
}

func TestInitProfileInteractiveWithDepsCanDeclineSSHConfigProfile(t *testing.T) {
	store := newTempStore(t)
	sshConfigPath := writeSSHConfigForInitTest(t, `
Host work
  User alice
`)
	withSSHConfigInitTestHooks(t, sshConfigPath, "fallback-user")
	reader := bufio.NewReader(strings.NewReader("n\nmanual.example.com\n22\ncarol\n"))
	ops := &fakeSSHOps{}

	prof, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, ioDiscard{})
	if err != nil {
		t.Fatalf("initProfileInteractiveWithDeps error: %v", err)
	}
	if prof.Host != "manual.example.com" || prof.User != "carol" || prof.Name != "carol@manual.example.com" {
		t.Fatalf("profile = %+v, want manual profile", prof)
	}
	if len(ops.probes) != 1 || ops.probes[0].Host != "manual.example.com" {
		t.Fatalf("probes = %#v, want manual probe only", ops.probes)
	}
}

func TestInitProfileInteractiveWithDepsDirectSSHSuccess(t *testing.T) {
	withoutSSHConfigProfilesForInitTest(t)
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
	withoutSSHConfigProfilesForInitTest(t)
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
	if got := ops.probeInteractive; len(got) != 2 || got[0] || got[1] {
		t.Fatalf("expected non-interactive managed-key probes, got %v", got)
	}
	if got := ops.probeStdin; len(got) != 2 || got[0] != nil || got[1] != nil {
		t.Fatalf("expected managed-key probes without stdin override, got %v", got)
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
	withoutSSHConfigProfilesForInitTest(t)
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
	withoutSSHConfigProfilesForInitTest(t)
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

func TestInitProfileInteractiveWithDepsDoesNotPromptForHostKeyConfirmation(t *testing.T) {
	withoutSSHConfigProfilesForInitTest(t)
	store := newTempStore(t)
	reader := bufio.NewReader(strings.NewReader("host.example\n22\nfrank\n"))
	ops := &fakeSSHOps{
		probeErrs: []error{
			&sshProbeError{kind: sshProbeFailureHostKey, err: errors.New("host key failed"), output: "Host key verification failed"},
		},
	}
	var out bytes.Buffer

	_, err := initProfileInteractiveWithDeps(context.Background(), store, reader, ops, &out)
	if err == nil {
		t.Fatal("expected host key error")
	}
	if !strings.Contains(err.Error(), "Host key verification failed") {
		t.Fatalf("expected original host key error, got %v", err)
	}
	if got := ops.probeInteractive; len(got) != 1 || got[0] {
		t.Fatalf("expected only the initial non-interactive probe, got %v", got)
	}
	if got := ops.probeStdin; len(got) != 1 || got[0] != nil {
		t.Fatalf("expected host-key flow to avoid interactive stdin override, got %v", got)
	}
	if strings.Contains(out.String(), "Open interactive SSH host key check") {
		t.Fatalf("did not expect an interactive host-key prompt, got %q", out.String())
	}

	cfg, loadErr := store.Load()
	if loadErr != nil {
		t.Fatalf("load config: %v", loadErr)
	}
	if len(cfg.Profiles) != 0 {
		t.Fatalf("expected no saved profiles after decline, got %+v", cfg.Profiles)
	}
}

func TestSSHProbeUsesTunnelReadinessForNonInteractiveChecks(t *testing.T) {
	lockCLITestHooks(t)
	prevNewSSHTunnel := newSSHTunnel
	t.Cleanup(func() { newSSHTunnel = prevNewSSHTunnel })

	var gotCfg internalssh.TunnelConfig
	newSSHTunnel = func(cfg internalssh.TunnelConfig) (sshTunnel, error) {
		gotCfg = cfg
		return newFakeReadyTunnel(cfg)
	}

	err := sshProbe(context.Background(), config.Profile{
		Host: "example.com",
		Port: 3211,
		User: "starh",
	}, false, nil)
	if err != nil {
		t.Fatalf("sshProbe error: %v", err)
	}
	if gotCfg.Host != "example.com" || gotCfg.Port != 3211 || gotCfg.User != "starh" {
		t.Fatalf("unexpected tunnel config: %+v", gotCfg)
	}
	if gotCfg.SocksPort == 0 {
		t.Fatalf("expected probe to allocate a socks port, got %+v", gotCfg)
	}
	if !gotCfg.BatchMode {
		t.Fatalf("expected batch mode for probe tunnel, got %+v", gotCfg)
	}
	if gotCfg.ConfigTarget {
		t.Fatalf("expected regular profile probe not to use config target, got %+v", gotCfg)
	}
}

func TestSSHProbeUsesConfigTargetForSSHConfigProfile(t *testing.T) {
	lockCLITestHooks(t)
	prevNewSSHTunnel := newSSHTunnel
	t.Cleanup(func() { newSSHTunnel = prevNewSSHTunnel })

	var gotCfg internalssh.TunnelConfig
	newSSHTunnel = func(cfg internalssh.TunnelConfig) (sshTunnel, error) {
		gotCfg = cfg
		return newFakeReadyTunnel(cfg)
	}

	err := sshProbe(context.Background(), config.Profile{
		Host:    "work",
		Port:    2222,
		User:    "alice",
		SSHArgs: []string{"-F", "/tmp/ssh_config"},
	}, false, nil)
	if err != nil {
		t.Fatalf("sshProbe error: %v", err)
	}
	if !gotCfg.ConfigTarget {
		t.Fatalf("expected ssh config profile probe to use config target, got %+v", gotCfg)
	}
}

func TestInstallPublicKeyAcceptsNewHostKeysByDefault(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell-based ssh argument test on windows")
	}

	dir := t.TempDir()
	sshPath := filepath.Join(dir, "ssh")
	writeExecutable(t, sshPath, "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$SSH_ARGS_FILE\"\ncat > \"$SSH_STDIN_FILE\"\n")

	argsFile := filepath.Join(dir, "ssh-args")
	stdinFile := filepath.Join(dir, "ssh-stdin")
	t.Setenv("SSH_ARGS_FILE", argsFile)
	t.Setenv("SSH_STDIN_FILE", stdinFile)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	pubKeyPath := filepath.Join(dir, "id.pub")
	if err := os.WriteFile(pubKeyPath, []byte("ssh-ed25519 test-key"), 0o600); err != nil {
		t.Fatalf("write pubkey: %v", err)
	}

	err := installPublicKey(context.Background(), config.Profile{
		Host:    "host.example",
		Port:    2319,
		User:    "erin",
		SSHArgs: []string{"-J", "jump.example"},
	}, pubKeyPath)
	if err != nil {
		t.Fatalf("installPublicKey error: %v", err)
	}

	rawArgs, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("read ssh args: %v", err)
	}
	joined := "\x00" + strings.ReplaceAll(strings.TrimSpace(string(rawArgs)), "\n", "\x00") + "\x00"
	for _, want := range []string{
		"\x00-o\x00StrictHostKeyChecking=accept-new\x00",
		"\x00-J\x00jump.example\x00",
		"\x00erin@host.example\x00",
		"\x00umask 077; mkdir -p ~/.ssh; cat >> ~/.ssh/authorized_keys\x00",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ssh args missing %q: %q", want, joined)
		}
	}

	stdin, err := os.ReadFile(stdinFile)
	if err != nil {
		t.Fatalf("read ssh stdin: %v", err)
	}
	if string(stdin) != "ssh-ed25519 test-key\n" {
		t.Fatalf("expected public key with appended newline, got %q", string(stdin))
	}
}

func TestSSHProbeOmitsTrailingColonWhenSSHReturnsNoOutput(t *testing.T) {
	lockCLITestHooks(t)
	prevNewSSHTunnel := newSSHTunnel
	t.Cleanup(func() { newSSHTunnel = prevNewSSHTunnel })
	newSSHTunnel = func(internalssh.TunnelConfig) (sshTunnel, error) {
		return &fakeProbeTunnel{startErr: errors.New("exit status 1"), done: make(chan struct{})}, nil
	}

	err := sshProbe(context.Background(), config.Profile{
		Host: "example.com",
		Port: 3211,
		User: "starh",
	}, false, nil)
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
	prevNewSSHTunnel := newSSHTunnel
	t.Cleanup(func() { newSSHTunnel = prevNewSSHTunnel })
	newSSHTunnel = func(internalssh.TunnelConfig) (sshTunnel, error) {
		return newFakeBlockingTunnel(), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := sshProbe(ctx, config.Profile{
		Host: "example.com",
		Port: 3211,
		User: "starh",
	}, false, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("expected prompt cancellation, took %s", time.Since(start))
	}
}

func TestInitProfileInteractiveRejectsTeamsServiceModeWithoutReading(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	prevStdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = prevStdin
		_ = reader.Close()
		_ = writer.Close()
	})

	store := newTempStore(t)
	done := make(chan error, 1)
	go func() {
		_, err := initProfileInteractive(context.Background(), store)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "interactive SSH profile setup requires a terminal") {
			t.Fatalf("initProfileInteractive error = %v, want non-interactive terminal error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("initProfileInteractive blocked on Teams service stdin")
	}
}

func TestInitProfileInteractiveRejectsNonTerminalStdinWithoutBlocking(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "")

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	prevStdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = prevStdin
		_ = reader.Close()
		_ = writer.Close()
	})

	store := newTempStore(t)
	done := make(chan error, 1)
	go func() {
		_, err := initProfileInteractive(context.Background(), store)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "interactive SSH profile setup requires a terminal") {
			t.Fatalf("initProfileInteractive error = %v, want non-interactive terminal error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("initProfileInteractive blocked on non-terminal stdin")
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

func TestClassifySSHProbeFailureDoesNotTreatBannerOffendingAsHostKey(t *testing.T) {
	output := "WARNING: offending users will be prosecuted.\nPermission denied (publickey)."
	if got := classifySSHProbeFailure(output); got != sshProbeFailureAuth {
		t.Fatalf("expected auth failure classification, got %v", got)
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

type fakeProbeTunnel struct {
	startErr error
	waitErr  error
	startFn  func() error
	stopFn   func() error

	done chan struct{}
	once sync.Once
}

func newFakeReadyTunnel(cfg internalssh.TunnelConfig) (*fakeProbeTunnel, error) {
	tun := &fakeProbeTunnel{done: make(chan struct{})}

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(cfg.SocksPort))
	var ln net.Listener
	tun.startFn = func() error {
		var err error
		ln, err = net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				_ = conn.Close()
			}
		}()
		return nil
	}
	tun.stopFn = func() error {
		if ln != nil {
			_ = ln.Close()
		}
		return nil
	}
	return tun, nil
}

func newFakeBlockingTunnel() *fakeProbeTunnel {
	return &fakeProbeTunnel{done: make(chan struct{})}
}

func (t *fakeProbeTunnel) Start() error {
	if t.startFn != nil {
		if err := t.startFn(); err != nil {
			return err
		}
	}
	return t.startErr
}

func (t *fakeProbeTunnel) Stop(time.Duration) error {
	if t.stopFn != nil {
		if err := t.stopFn(); err != nil {
			return err
		}
	}
	t.once.Do(func() { close(t.done) })
	return t.waitErr
}

func (t *fakeProbeTunnel) Done() <-chan struct{} { return t.done }

func (t *fakeProbeTunnel) Wait() error {
	<-t.done
	return t.waitErr
}
