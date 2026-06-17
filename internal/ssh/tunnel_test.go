package ssh

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBuildArgs_IncludesRequiredOptions(t *testing.T) {
	cfg := TunnelConfig{
		Host:      "example.com",
		Port:      2222,
		User:      "alice",
		SocksPort: 12345,
		ExtraArgs: []string{"-i", "/tmp/key"},
		BatchMode: true,
	}

	args, err := BuildArgs(cfg)
	if err != nil {
		t.Fatalf("BuildArgs error: %v", err)
	}

	wantPrefix := []string{
		"-N",
		"-o", "ExitOnForwardFailure=yes",
		"-o", "ConnectTimeout=15",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=3",
		"-o", "TCPKeepAlive=yes",
		"-o", "StrictHostKeyChecking=accept-new",
		"-p", "2222",
		"-D", "127.0.0.1:12345",
		"-o", "BatchMode=yes",
		"-i", "/tmp/key",
		"alice@example.com",
	}

	if !reflect.DeepEqual(args, wantPrefix) {
		t.Fatalf("args mismatch\n got: %#v\nwant: %#v", args, wantPrefix)
	}
}

func TestBuildArgs_UsesHostDestinationWithoutUserOrBatchMode(t *testing.T) {
	args, err := BuildArgs(TunnelConfig{
		Host:      "example.com",
		Port:      22,
		SocksPort: 12345,
	})
	if err != nil {
		t.Fatalf("BuildArgs error: %v", err)
	}
	if args[len(args)-1] != "example.com" {
		t.Fatalf("expected host-only destination, got %q", args[len(args)-1])
	}
	for i := 0; i < len(args); i++ {
		if args[i] == "BatchMode=yes" {
			t.Fatalf("expected BatchMode option to be absent, got %#v", args)
		}
	}
}

func TestBuildArgs_UsesConfigTargetWithoutOverridingUserOrPort(t *testing.T) {
	args, err := BuildArgs(TunnelConfig{
		Host:         "work",
		Port:         2222,
		User:         "alice",
		SocksPort:    12345,
		ExtraArgs:    []string{"-F", "/tmp/ssh_config"},
		ConfigTarget: true,
		BatchMode:    true,
	})
	if err != nil {
		t.Fatalf("BuildArgs error: %v", err)
	}
	if args[len(args)-1] != "work" {
		t.Fatalf("expected config alias destination, got %q in %#v", args[len(args)-1], args)
	}
	for i, arg := range args {
		if arg == "-p" {
			t.Fatalf("config target should not add command-line port, got %#v", args[i:])
		}
		if strings.Contains(arg, "@work") {
			t.Fatalf("config target should not add command-line user, got %#v", args)
		}
	}
}

func TestBuildArgs_ValidatesPorts(t *testing.T) {
	_, err := BuildArgs(TunnelConfig{
		Host:      "h",
		Port:      0,
		User:      "u",
		SocksPort: 1,
	})
	if err == nil {
		t.Fatalf("expected error for invalid ssh port")
	}

	_, err = BuildArgs(TunnelConfig{
		Host:      "h",
		Port:      22,
		User:      "u",
		SocksPort: 0,
	})
	if err == nil {
		t.Fatalf("expected error for invalid socks port")
	}

	_, err = BuildArgs(TunnelConfig{
		Host:      "  \t",
		Port:      22,
		User:      "u",
		SocksPort: 1,
	})
	if err == nil {
		t.Fatalf("expected error for blank host")
	}
}

func TestOpenSSHAcceptNewOptionSupported(t *testing.T) {
	if os.Getenv("SSH_ACCEPT_NEW_OPTION_TEST") != "1" {
		t.Skip("OpenSSH accept-new option test disabled")
	}
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Fatalf("ssh not available: %v", err)
	}
	out, err := exec.Command("ssh", "-G", "-o", "StrictHostKeyChecking=accept-new", "example.com").CombinedOutput()
	if err != nil {
		t.Fatalf("ssh client does not accept StrictHostKeyChecking=accept-new: %v\n%s", err, out)
	}
}

func TestHostKeyArgsForTargetLegacySeedsKnownHosts(t *testing.T) {
	setAcceptNewSupportForTest(t, false)

	dir := t.TempDir()
	knownHosts := filepath.Join(dir, "known_hosts")
	var scannedArgs []string
	setSSHKeyCommandsForTest(t,
		func(path, host string) bool {
			if path != knownHosts {
				t.Fatalf("ssh-keygen known_hosts = %q, want %q", path, knownHosts)
			}
			if host != "[legacy.example]:2222" {
				t.Fatalf("ssh-keygen host = %q, want [legacy.example]:2222", host)
			}
			return false
		},
		func(args []string) ([]byte, error) {
			scannedArgs = append([]string(nil), args...)
			return []byte("[legacy.example]:2222 ssh-ed25519 AAAAlegacy\n"), nil
		},
	)

	args, err := HostKeyArgsForTarget("legacy.example", 2222, []string{"-o", "UserKnownHostsFile=" + knownHosts})
	if err != nil {
		t.Fatalf("HostKeyArgsForTarget error: %v", err)
	}
	want := []string{"-o", "StrictHostKeyChecking=yes"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("host key args = %#v, want %#v", args, want)
	}
	wantScanArgs := []string{"-T", "5", "-H", "-p", "2222", "legacy.example"}
	if !reflect.DeepEqual(scannedArgs, wantScanArgs) {
		t.Fatalf("ssh-keyscan args = %#v, want %#v", scannedArgs, wantScanArgs)
	}
	data, err := os.ReadFile(knownHosts)
	if err != nil {
		t.Fatalf("read known_hosts: %v", err)
	}
	if !strings.Contains(string(data), "[legacy.example]:2222 ssh-ed25519 AAAAlegacy") {
		t.Fatalf("known_hosts missing scanned key: %q", string(data))
	}
}

func TestNewTunnelConfigTargetSkipsLegacyKnownHostScan(t *testing.T) {
	setAcceptNewSupportForTest(t, false)
	setSSHKeyCommandsForTest(t,
		func(path, host string) bool {
			t.Fatalf("ssh-keygen should not inspect config alias target path=%q host=%q", path, host)
			return false
		},
		func(args []string) ([]byte, error) {
			t.Fatalf("ssh-keyscan should not run for config alias target: %#v", args)
			return nil, nil
		},
	)

	tun, err := NewTunnel(TunnelConfig{
		Host:         "work",
		Port:         2222,
		User:         "alice",
		SocksPort:    12345,
		ExtraArgs:    []string{"-F", "/tmp/ssh_config"},
		ConfigTarget: true,
		BatchMode:    true,
	})
	if err != nil {
		t.Fatalf("NewTunnel error: %v", err)
	}
	if tun == nil {
		t.Fatal("expected tunnel")
	}
}

func TestHostKeyArgsForTargetLegacyHonorsExplicitStrictHostKeyChecking(t *testing.T) {
	setAcceptNewSupportForTest(t, false)
	args, err := HostKeyArgsForTarget("legacy.example", 22, []string{"-o", "StrictHostKeyChecking=no"})
	if err != nil {
		t.Fatalf("HostKeyArgsForTarget error: %v", err)
	}
	if len(args) != 0 {
		t.Fatalf("expected explicit StrictHostKeyChecking to suppress default args, got %#v", args)
	}
}

func TestHostKeyArgsForTargetHonorsExplicitStrictHostKeyCheckingBeforeDetection(t *testing.T) {
	setAcceptNewSupportDetectorForTest(t, func() bool {
		t.Fatal("ssh accept-new detector should not run when StrictHostKeyChecking is explicit")
		return false
	})

	args, err := HostKeyArgsForTarget("modern.example", 22, []string{"-oStrictHostKeyChecking=no"})
	if err != nil {
		t.Fatalf("HostKeyArgsForTarget error: %v", err)
	}
	if len(args) != 0 {
		t.Fatalf("expected explicit StrictHostKeyChecking to suppress default args, got %#v", args)
	}
}

func TestHostKeyArgsForTargetLegacySkipsScanWhenKnownHostExists(t *testing.T) {
	setAcceptNewSupportForTest(t, false)

	dir := t.TempDir()
	knownHosts := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(knownHosts, []byte("legacy.example ssh-ed25519 AAAAexisting\n"), 0o600); err != nil {
		t.Fatalf("write known_hosts: %v", err)
	}
	setSSHKeyCommandsForTest(t,
		func(path, host string) bool {
			if path != knownHosts || host != "legacy.example" {
				t.Fatalf("unexpected ssh-keygen lookup path=%q host=%q", path, host)
			}
			return true
		},
		func(args []string) ([]byte, error) {
			t.Fatal("ssh-keyscan ran even though ssh-keygen found the known host")
			return nil, nil
		},
	)

	args, err := HostKeyArgsForTarget("legacy.example", 22, []string{"-oUserKnownHostsFile=" + knownHosts})
	if err != nil {
		t.Fatalf("HostKeyArgsForTarget error: %v", err)
	}
	want := []string{"-o", "StrictHostKeyChecking=yes"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("host key args = %#v, want %#v", args, want)
	}
}

func TestHostKeyArgsForTargetLegacyRetriesWithoutHashFlag(t *testing.T) {
	setAcceptNewSupportForTest(t, false)

	dir := t.TempDir()
	knownHosts := filepath.Join(dir, "known_hosts")
	var calls [][]string
	setSSHKeyCommandsForTest(t,
		func(path, host string) bool { return false },
		func(args []string) ([]byte, error) {
			calls = append(calls, append([]string(nil), args...))
			if len(calls) == 1 {
				return nil, errors.New("unknown option -- H")
			}
			return []byte("legacy.example ssh-ed25519 AAAAlegacy\n"), nil
		},
	)

	_, err := HostKeyArgsForTarget("legacy.example", 22, []string{"-o", "UserKnownHostsFile=" + knownHosts})
	if err != nil {
		t.Fatalf("HostKeyArgsForTarget error: %v", err)
	}
	wantCalls := [][]string{
		{"-T", "5", "-H", "legacy.example"},
		{"-T", "5", "legacy.example"},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("ssh-keyscan calls = %#v, want %#v", calls, wantCalls)
	}
}

func TestHostKeyArgsForTargetLegacyRejectsDiscardKnownHostsFile(t *testing.T) {
	setAcceptNewSupportForTest(t, false)
	paths := []string{"none", os.DevNull}
	if os.DevNull != "/dev/null" {
		paths = append(paths, "/dev/null")
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			_, err := HostKeyArgsForTarget("legacy.example", 22, []string{"-o", "UserKnownHostsFile=" + path})
			if err == nil {
				t.Fatalf("expected error for UserKnownHostsFile=%s", path)
			}
			if !strings.Contains(err.Error(), "cannot store the first host key") {
				t.Fatalf("expected actionable known_hosts error, got %v", err)
			}
		})
	}
}

func TestHostKeyArgsForTargetValidatesTargetBeforeDetection(t *testing.T) {
	setAcceptNewSupportDetectorForTest(t, func() bool {
		t.Fatal("ssh accept-new detector should not run for invalid targets")
		return false
	})

	tests := []struct {
		name string
		host string
		port int
	}{
		{name: "empty host", host: "", port: 22},
		{name: "blank host", host: "  \t", port: 22},
		{name: "zero port", host: "legacy.example", port: 0},
		{name: "too large port", host: "legacy.example", port: 65536},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := HostKeyArgsForTarget(tt.host, tt.port, nil); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestUserKnownHostsFileExpandsHomeShortcut(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	got, err := userKnownHostsFile([]string{"-o", "UserKnownHostsFile=~/custom_known_hosts"})
	if err != nil {
		t.Fatalf("userKnownHostsFile error: %v", err)
	}
	want := filepath.Join(home, "custom_known_hosts")
	if got != want {
		t.Fatalf("known_hosts path = %q, want %q", got, want)
	}
}

func TestDetectSSHAcceptNewSupportRejectsUnsupportedOption(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	writeTestExecutable(t, filepath.Join(dir, "ssh"), "#!/bin/sh\necho 'Bad configuration option: StrictHostKeyChecking=accept-new' >&2\nexit 255\n")
	t.Setenv("PATH", dir)
	if detectSSHAcceptNewSupport() {
		t.Fatal("expected unsupported accept-new option to be detected")
	}
}

func TestDetectSSHAcceptNewSupportRejectsOldSSHWithoutConfigDump(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	writeTestExecutable(t, filepath.Join(dir, "ssh"), "#!/bin/sh\necho 'ssh: unknown option -- G' >&2\nexit 255\n")
	t.Setenv("PATH", dir)
	if detectSSHAcceptNewSupport() {
		t.Fatal("expected ssh clients without -G support to use legacy host-key fallback")
	}
}

func TestDetectSSHAcceptNewSupportIgnoresUnrelatedSSHFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}
	dir := t.TempDir()
	writeTestExecutable(t, filepath.Join(dir, "ssh"), "#!/bin/sh\nexit 1\n")
	t.Setenv("PATH", dir)
	if !detectSSHAcceptNewSupport() {
		t.Fatal("expected unrelated ssh -G failure not to trigger legacy host-key fallback")
	}
}

func setAcceptNewSupportForTest(t *testing.T, supported bool) {
	t.Helper()
	setAcceptNewSupportDetectorForTest(t, func() bool { return supported })
}

func setAcceptNewSupportDetectorForTest(t *testing.T, detector func() bool) {
	t.Helper()
	oldDetector := sshAcceptNewSupportDetector
	sshAcceptNewSupportDetector = detector
	sshAcceptNewSupportValue = false
	sshAcceptNewSupportOnce = sync.Once{}
	t.Cleanup(func() {
		sshAcceptNewSupportDetector = oldDetector
		sshAcceptNewSupportValue = false
		sshAcceptNewSupportOnce = sync.Once{}
	})
}

func setSSHKeyCommandsForTest(t *testing.T, keygenFindHost func(string, string) bool, keyscan func([]string) ([]byte, error)) {
	t.Helper()
	oldKeygenFindHost := sshKeygenFindHost
	oldRunSSHKeyscan := runSSHKeyscan
	if keygenFindHost != nil {
		sshKeygenFindHost = keygenFindHost
	}
	if keyscan != nil {
		runSSHKeyscan = keyscan
	}
	t.Cleanup(func() {
		sshKeygenFindHost = oldKeygenFindHost
		runSSHKeyscan = oldRunSSHKeyscan
	})
}

func writeTestExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}

func TestTunnelLifecycleFailures(t *testing.T) {
	t.Run("BuildArgs requires host", func(t *testing.T) {
		_, err := BuildArgs(TunnelConfig{
			Host:      "",
			Port:      22,
			User:      "u",
			SocksPort: 1,
		})
		if err == nil {
			t.Fatalf("expected error for missing host")
		}
	})

	t.Run("NewTunnel validates before host-key detection", func(t *testing.T) {
		setAcceptNewSupportDetectorForTest(t, func() bool {
			t.Fatal("ssh accept-new detector should not run for invalid tunnel config")
			return false
		})
		_, err := NewTunnel(TunnelConfig{
			Host:      "",
			Port:      22,
			User:      "alice",
			SocksPort: 12345,
		})
		if err == nil {
			t.Fatalf("expected NewTunnel validation error")
		}
	})

	t.Run("Start fails when ssh missing", func(t *testing.T) {
		t.Setenv("PATH", "")
		cfg := TunnelConfig{
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			SocksPort: 12345,
		}
		tun, err := NewTunnel(cfg)
		if err != nil {
			t.Fatalf("NewTunnel error: %v", err)
		}
		if err := tun.Start(); err == nil {
			t.Fatalf("expected Start to fail without ssh")
		}
	})

	t.Run("Stop before Start is a no-op", func(t *testing.T) {
		cfg := TunnelConfig{
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			SocksPort: 12345,
		}
		tun, err := NewTunnel(cfg)
		if err != nil {
			t.Fatalf("NewTunnel error: %v", err)
		}
		if err := tun.Stop(10 * time.Millisecond); err != nil {
			t.Fatalf("expected Stop to return nil, got %v", err)
		}
	})

	t.Run("Start handles immediate exit", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("skip shell script test on windows")
		}
		dir := t.TempDir()
		script := filepath.Join(dir, "ssh")
		if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
			t.Fatalf("write script: %v", err)
		}
		t.Setenv("PATH", dir)

		cfg := TunnelConfig{
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			SocksPort: 12345,
		}
		tun, err := NewTunnel(cfg)
		if err != nil {
			t.Fatalf("NewTunnel error: %v", err)
		}
		if err := tun.Start(); err != nil {
			t.Fatalf("Start error: %v", err)
		}
		if err := tun.Wait(); err == nil {
			t.Fatalf("expected Wait to report exit error")
		}
	})

	t.Run("Stop forces kill after grace", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("skip shell script test on windows")
		}
		dir := t.TempDir()
		script := filepath.Join(dir, "ssh")
		content := "#!/bin/sh\ntrap '' INT\nsleep 5\n"
		if err := os.WriteFile(script, []byte(content), 0o700); err != nil {
			t.Fatalf("write script: %v", err)
		}
		t.Setenv("PATH", dir)

		cfg := TunnelConfig{
			Host:      "example.com",
			Port:      22,
			User:      "alice",
			SocksPort: 12345,
		}
		tun, err := NewTunnel(cfg)
		if err != nil {
			t.Fatalf("NewTunnel error: %v", err)
		}
		if err := tun.Start(); err != nil {
			t.Fatalf("Start error: %v", err)
		}
		if err := tun.Stop(50 * time.Millisecond); err == nil {
			t.Fatalf("expected Stop to report forced kill")
		}
	})
}
