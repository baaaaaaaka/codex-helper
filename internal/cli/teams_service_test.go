package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func isolateTeamsUserDirsForTest(t *testing.T, tmp string) (string, string) {
	t.Helper()
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)
	t.Setenv("APPDATA", filepath.Join(tmp, "AppData", "Roaming"))
	t.Setenv("LOCALAPPDATA", filepath.Join(tmp, "AppData", "Local"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "tenant")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "chat-client")
	t.Setenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID", "read-client")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID", "file-client")
	configBase, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("os.UserConfigDir: %v", err)
	}
	cacheBase, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("os.UserCacheDir: %v", err)
	}
	return configBase, cacheBase
}

func setTeamsAuthIDsForCLITest(t *testing.T) {
	t.Helper()
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "tenant")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "chat-client")
	t.Setenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID", "read-client")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID", "file-client")
}

func teamsServiceTestAbsPath(t *testing.T, path string) string {
	t.Helper()
	out, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("filepath.Abs(%q): %v", path, err)
	}
	return out
}

func teamsServiceTestRegistryPath(cwd string, registryPath string) string {
	registryPath = strings.TrimSpace(registryPath)
	if filepath.IsAbs(registryPath) {
		return registryPath
	}
	return filepath.Join(cwd, registryPath)
}

func TestTeamsServiceInstallWritesSystemdUserUnitWithoutEnabling(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd user")
	exePath := filepath.Join(tmp, "bin", "codex proxy")
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     cwd,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	unitPath := filepath.Join(unitDir, teamsServiceUnitName)
	data, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		"[Unit]",
		"Description=Codex Helper Teams bridge",
		"WorkingDirectory=" + strconv.Quote(cwd),
		"ExecStart=" + systemdQuoteArg(exePath) + " teams run --owner-stale-after 18s --registry " + strconv.Quote(registryPath),
		"Restart=on-failure",
		"RestartSec=10s",
		"Environment=NO_COLOR=1",
		"Environment=CODEX_HELPER_TEAMS_SERVICE=1",
		"[Install]",
		"WantedBy=default.target",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing %q:\n%s", want, unit)
		}
	}

	wantCalls := []teamsServiceCommandCall{{
		name: "systemctl",
		args: []string{"--user", "daemon-reload"},
	}}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("systemctl calls = %#v, want %#v", runner.calls, wantCalls)
	}
	for _, call := range runner.calls {
		joined := strings.Join(call.args, " ")
		if strings.Contains(joined, "enable") || strings.Contains(joined, "start") {
			t.Fatalf("install should not enable or start service, calls=%#v", runner.calls)
		}
	}
	if !strings.Contains(out.String(), "not enabled or started automatically") {
		t.Fatalf("install output should state no auto enable/start:\n%s", out.String())
	}
}

func TestTeamsServiceInstallWithoutRegistryLetsBridgeUseScopedDefaults(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	cwd := filepath.Join(tmp, "work")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     cwd,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	if strings.Contains(unit, "--registry") {
		t.Fatalf("default service unit should not force a shared legacy registry:\n%s", unit)
	}
	if !strings.Contains(unit, "ExecStart="+systemdQuoteArg(exePath)+" teams run") {
		t.Fatalf("unit missing teams run ExecStart:\n%s", unit)
	}
}

func TestTeamsServiceInstallRejectsGoRunTemporaryExecutable(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "go-build123", "b001", "exe", "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "temporary go run binary") {
		t.Fatalf("expected go run executable rejection, got %v", err)
	}
}

func TestTeamsServiceAuthPreflightRequiresForegroundLogin(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	err := defaultTeamsServiceAuthPreflight()
	if err == nil || !strings.Contains(err.Error(), "teams auth") {
		t.Fatalf("expected missing auth preflight error, got %v", err)
	}
}

func TestTeamsServiceStartRequiresAuthPreflight(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("started\n")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})
	teamsServiceAuthPreflight = func() error { return errors.New("auth missing") }

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"start"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "auth missing") {
		t.Fatalf("expected auth preflight error, got %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("service start should not reach supervisor when auth preflight fails: %#v", runner.calls)
	}
}

func TestTeamsServiceDoctorReportsAuthAndExecutableWithoutFailingFast(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "go-build123", "b001", "exe", "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})
	teamsServiceAuthPreflight = func() error { return errors.New("auth missing") }

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"doctor"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("service doctor should report auth/executable issues without failing fast: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Teams service backend: systemd-user",
		"Teams service executable: not installable",
		"temporary go run binary",
		"Teams service auth: not ready",
		"auth missing",
		"Linux: systemd --user keeps the Teams bridge independent of the terminal",
		"if lingering is disabled",
		"no user service can guarantee survival",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
}

func TestTeamsServiceInstallPreservesScopedEnvironment(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex home"))
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "work-profile")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "1")
	t.Setenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", filepath.Join(tmp, "read-token.json"))
	t.Setenv("HTTPS_PROXY", "http://proxy.example.test:8080")
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     tmp,
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		"Environment=" + systemdQuoteArg("CODEX_HOME="+filepath.Join(tmp, "codex home")),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_PROFILE=work-profile"),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1"),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE="+filepath.Join(tmp, "read-token.json")),
		"Environment=" + systemdQuoteArg("HTTPS_PROXY=http://proxy.example.test:8080"),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_SERVICE_MODE=background"),
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing preserved env %q:\n%s", want, unit)
		}
	}
}

func TestTeamsServiceInstallDefaultsCodexHomeForScope(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	home := filepath.Join(tmp, "home")
	prevUserHome := effectivePathsUserHomeDir
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { effectivePathsUserHomeDir = prevUserHome })
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "")
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     filepath.Join(tmp, "work"),
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	want := filepath.Join(home, ".codex")
	unit := string(data)
	for _, envLine := range []string{
		"Environment=" + systemdQuoteArg("CODEX_HOME="+want),
		"Environment=" + systemdQuoteArg("CODEX_DIR="+want),
	} {
		if !strings.Contains(unit, envLine) {
			t.Fatalf("unit missing default Codex home env %q:\n%s", envLine, unit)
		}
	}
}

func TestTeamsServiceInstallDoesNotFailWhenDefaultCodexHomeCannotBeDerived(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	prevUserHome := effectivePathsUserHomeDir
	effectivePathsUserHomeDir = func() (string, error) { return "", errors.New("home unavailable") }
	t.Cleanup(func() { effectivePathsUserHomeDir = prevUserHome })
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "")
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     filepath.Join(tmp, "work"),
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("service install should not fail only because default Codex home cannot be derived: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	if strings.Contains(unit, "CODEX_HOME=") || strings.Contains(unit, "CODEX_DIR=") {
		t.Fatalf("unit should omit default Codex home env when it cannot be derived:\n%s", unit)
	}
	if !strings.Contains(unit, "CODEX_HELPER_TEAMS_SERVICE=1") {
		t.Fatalf("unit missing required helper service env:\n%s", unit)
	}
}

func TestTeamsServiceEnvironmentPreservesLoopbackProxyByDefault(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:38471")
	t.Setenv("HTTPS_PROXY", "http://localhost:38471")
	t.Setenv("ALL_PROXY", "socks5://[::1]:38471")
	t.Setenv("http_proxy", "http://127.0.0.1:38471")
	t.Setenv("https_proxy", "http://localhost:38471")
	t.Setenv("all_proxy", "socks5://[::1]:38471")
	t.Setenv("NO_PROXY", "localhost,127.0.0.1,::1")
	t.Setenv("no_proxy", "localhost,127.0.0.1,::1")

	env := teamsServiceEnvironment()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		if value := env[name]; value == "" {
			t.Fatalf("%s should be preserved for the background service, got %#v", name, env)
		}
	}
	if env["NO_PROXY"] == "" || env["no_proxy"] == "" {
		t.Fatalf("NO_PROXY values should be preserved, got %#v", env)
	}
}

func TestTeamsServiceEnvironmentMirrorsSingleCodexHomeEnv(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex home"))
	t.Setenv("CODEX_DIR", "")

	env, err := teamsServiceEnvironmentForWorkingDir(tmp)
	if err != nil {
		t.Fatalf("teamsServiceEnvironmentForWorkingDir: %v", err)
	}
	want := filepath.Join(tmp, "codex home")
	if env["CODEX_HOME"] != want || env["CODEX_DIR"] != want {
		t.Fatalf("Codex home env = CODEX_HOME:%q CODEX_DIR:%q, want both %q", env["CODEX_HOME"], env["CODEX_DIR"], want)
	}
}

func TestTeamsServiceEnvironmentResolvesRelativeCodexDirEnv(t *testing.T) {
	tmp := t.TempDir()
	work := filepath.Join(tmp, "work")
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "relative-codex")

	env, err := teamsServiceEnvironmentForWorkingDir(work)
	if err != nil {
		t.Fatalf("teamsServiceEnvironmentForWorkingDir: %v", err)
	}
	want := filepath.Join(work, "relative-codex")
	if env["CODEX_HOME"] != want || env["CODEX_DIR"] != want {
		t.Fatalf("Codex home env = CODEX_HOME:%q CODEX_DIR:%q, want both %q", env["CODEX_HOME"], env["CODEX_DIR"], want)
	}
}

func TestTeamsServiceEnvironmentPreservesConflictingExplicitCodexHomes(t *testing.T) {
	tmp := t.TempDir()
	home := filepath.Join(tmp, "codex-home")
	dir := filepath.Join(tmp, "codex-dir")
	t.Setenv("CODEX_HOME", home)
	t.Setenv("CODEX_DIR", dir)

	env, err := teamsServiceEnvironmentForWorkingDir(tmp)
	if err != nil {
		t.Fatalf("teamsServiceEnvironmentForWorkingDir: %v", err)
	}
	if env["CODEX_HOME"] != home || env["CODEX_DIR"] != dir {
		t.Fatalf("explicit Codex env should be preserved, got CODEX_HOME:%q CODEX_DIR:%q", env["CODEX_HOME"], env["CODEX_DIR"])
	}
}

func TestTeamsServiceEnvironmentCanDropLoopbackProxyWhenExplicit(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_DROP_LOCAL_PROXY", "1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:38471")
	t.Setenv("HTTPS_PROXY", "http://localhost:38471")
	t.Setenv("ALL_PROXY", "socks5://[::1]:38471")
	t.Setenv("http_proxy", "http://127.0.0.1:38471")
	t.Setenv("https_proxy", "http://localhost:38471")
	t.Setenv("all_proxy", "socks5://[::1]:38471")
	t.Setenv("NO_PROXY", "localhost,127.0.0.1,::1")
	t.Setenv("no_proxy", "localhost,127.0.0.1,::1")

	env := teamsServiceEnvironment()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		if value := env[name]; value != "" {
			t.Fatalf("%s = %q, want dropped loopback proxy", name, value)
		}
	}
	if env["NO_PROXY"] == "" || env["no_proxy"] == "" {
		t.Fatalf("NO_PROXY values should be preserved, got %#v", env)
	}
}

func TestTeamsServiceEnvironmentCanKeepLoopbackProxyWhenExplicit(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_DROP_LOCAL_PROXY", "1")
	t.Setenv("CODEX_HELPER_TEAMS_KEEP_LOCAL_PROXY", "1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:38471")

	env := teamsServiceEnvironment()
	if got := env["HTTPS_PROXY"]; got != "http://127.0.0.1:38471" {
		t.Fatalf("HTTPS_PROXY = %q, want explicit loopback proxy preserved", got)
	}
}

func TestTeamsServiceWSLArgumentsUseExecToBypassLoginShell(t *testing.T) {
	lockCLITestHooks(t)

	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		wslDistro:    "Debian",
		wslLinuxUser: "baka",
	})
	spec := teamsServiceSpec{
		Executable: "/home/baka/.local/bin/codex-proxy",
		WorkingDir: "/home/baka",
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE": "1",
			"NO_PROXY":                   "*.nvidia.com,nvidia.com,<local>",
		},
	}
	args := buildTeamsServiceWSLArguments(spec)
	joined := strings.Join(args, "\x00")
	if strings.Contains(joined, "\x00--\x00env\x00") {
		t.Fatalf("WSL service args should not route env through the login shell: %#v", args)
	}
	if !strings.Contains(joined, "\x00--exec\x00env\x00") {
		t.Fatalf("WSL service args should use --exec env to preserve glob proxy values: %#v", args)
	}
	if !slices.Contains(args, "NO_PROXY=*.nvidia.com,nvidia.com,<local>") {
		t.Fatalf("WSL service args should preserve raw NO_PROXY value for env: %#v", args)
	}
}

func TestTeamsServiceWSLTaskIdentityStableAcrossWorkingConfigAndCodexHome(t *testing.T) {
	lockCLITestHooks(t)

	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:         "linux",
		isWSL:        true,
		wslDistro:    "Debian",
		wslLinuxUser: "baka",
	})
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "default")
	t.Setenv("CODEX_HELPER_TEAMS_MACHINE_ID", "")
	t.Setenv("CODEX_HELPER_CONFIG", "/home/baka/project/a/config.json")
	t.Setenv("CODEX_HOME", "/home/baka/project/a/.codex")
	first := teamsServiceWSLTaskIdentity()

	t.Setenv("CODEX_HELPER_CONFIG", "/home/baka/project/b/config.json")
	t.Setenv("CODEX_HOME", "/home/baka/project/b/.codex")
	second := teamsServiceWSLTaskIdentity()

	if first.Suffix != second.Suffix || first.Display != second.Display {
		t.Fatalf("WSL task identity changed across cwd/config/codex home: first=%#v second=%#v", first, second)
	}
	if !strings.Contains(first.Display, "Debian baka default") {
		t.Fatalf("display = %q, want distro/user/profile", first.Display)
	}
}

func TestTeamsServiceWSLTaskNamePrefixIncludesTrailingSpaceBeforeSuffix(t *testing.T) {
	got := teamsServiceWSLTaskNamePrefix("Codex Helper Teams Bridge (WSL Debian baka default bd54a914bb9b)")
	want := "Codex Helper Teams Bridge (WSL Debian baka default "
	if got != want {
		t.Fatalf("prefix = %q, want %q", got, want)
	}
}

func TestTeamsServiceSystemctlSubcommandsUseUserService(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("active\n")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	for _, tc := range []struct {
		name      string
		wantCalls []teamsServiceCommandCall
	}{
		{name: "enable", wantCalls: []teamsServiceCommandCall{{name: "systemctl", args: []string{"--user", "enable", teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName}}}},
		{name: "disable", wantCalls: []teamsServiceCommandCall{{name: "systemctl", args: []string{"--user", "disable", teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName}}}},
		{name: "status", wantCalls: []teamsServiceCommandCall{
			{name: "systemctl", args: []string{"--user", "status", "--no-pager", teamsServiceUnitName}},
			{name: "systemctl", args: []string{"--user", "status", "--no-pager", teamsServiceWatchdogUnitName}},
			{name: "systemctl", args: []string{"--user", "status", "--no-pager", teamsServiceWatchdogTimerName}},
		}},
		{name: "start", wantCalls: []teamsServiceCommandCall{{name: "systemctl", args: []string{"--user", "start", teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName}}}},
		{name: "stop", wantCalls: []teamsServiceCommandCall{
			{name: "systemctl", args: []string{"--user", "stop", teamsServiceWatchdogTimerName}},
			{name: "systemctl", args: []string{"--user", "stop", teamsServiceWatchdogUnitName}},
			{name: "systemctl", args: []string{"--user", "stop", teamsServiceUnitName}},
		}},
		{name: "restart", wantCalls: []teamsServiceCommandCall{{name: "systemctl", args: []string{"--user", "restart", teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName}}}},
	} {
		runner.calls = nil
		cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
		cmd.SetArgs([]string{tc.name})
		var out bytes.Buffer
		cmd.SetOut(&out)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("execute service %s: %v", tc.name, err)
		}
		if !reflect.DeepEqual(runner.calls, tc.wantCalls) {
			t.Fatalf("%s calls = %#v, want %#v", tc.name, runner.calls, tc.wantCalls)
		}
		if tc.name == "status" && !strings.Contains(out.String(), "active") {
			t.Fatalf("status should print systemctl output:\n%s", out.String())
		}
	}
}

func TestTeamsServiceSystemctlStopIgnoresMissingWatchdogUnits(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			[]byte("Unit codex-helper-teams-watchdog.timer not loaded.\n"),
			nil,
			[]byte("stopped main\n"),
		},
		errs: []error{
			errors.New("exit status 5"),
			errors.New("systemctl --user stop failed: unit could not be found"),
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"stop"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service stop: %v\n%s", err, out.String())
	}
	wantCalls := []teamsServiceCommandCall{
		{name: "systemctl", args: []string{"--user", "stop", teamsServiceWatchdogTimerName}},
		{name: "systemctl", args: []string{"--user", "stop", teamsServiceWatchdogUnitName}},
		{name: "systemctl", args: []string{"--user", "stop", teamsServiceUnitName}},
	}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("stop calls = %#v, want %#v", runner.calls, wantCalls)
	}
	if strings.Contains(out.String(), "not loaded") || strings.Contains(out.String(), "could not be found") {
		t.Fatalf("missing watchdog errors should be suppressed:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "Stopped Teams service") {
		t.Fatalf("stop success output missing:\n%s", out.String())
	}
}

func TestTeamsServiceSystemctlStopStillFailsWhenPrimaryStopFails(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		errs: []error{nil, nil, errors.New("exit status 5")},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"stop"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "exit status 5") {
		t.Fatalf("execute service stop error = %v, want primary stop failure\n%s", err, out.String())
	}
}

func TestTeamsServiceUninstallRemovesUnitAndReloads(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	unitPath := filepath.Join(unitDir, teamsServiceUnitName)
	if err := os.WriteFile(unitPath, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale unit: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"uninstall"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service uninstall: %v", err)
	}
	if _, err := os.Stat(unitPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected unit to be removed, stat err=%v", err)
	}
	wantCalls := []teamsServiceCommandCall{{
		name: "systemctl",
		args: []string{"--user", "daemon-reload"},
	}}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("systemctl calls = %#v, want %#v", runner.calls, wantCalls)
	}
}

func TestTeamsServiceUnsupportedPlatformInjection(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "freebsd",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"install"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected unsupported platform error")
	}
	if !strings.Contains(err.Error(), `unsupported platform "freebsd"`) ||
		!strings.Contains(err.Error(), "Linux systemd --user, macOS LaunchAgent, and Windows per-user Task Scheduler") {
		t.Fatalf("unexpected unsupported error: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("unsupported platform should not invoke systemctl, calls=%#v", runner.calls)
	}
}

func TestTeamsServiceInstallWritesMacOSLaunchAgentPlist(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	agentDir := filepath.Join(tmp, "LaunchAgents")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "darwin",
		exe:            exePath,
		cwd:            cwd,
		launchAgentDir: agentDir,
		userID:         "501",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	plistPath := filepath.Join(agentDir, teamsServiceLaunchAgentPlistName)
	data, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	plist := string(data)
	for _, want := range []string{
		"<string>" + teamsServiceLaunchAgentLabel + "</string>",
		"<key>Disabled</key>",
		"<key>ProgramArguments</key>",
		"<string>" + exePath + "</string>",
		"<string>teams</string>",
		"<string>run</string>",
		"<string>--owner-stale-after</string>",
		"<string>18s</string>",
		"<string>--registry</string>",
		"<string>" + registryPath + "</string>",
		"<key>KeepAlive</key>",
		"<key>SuccessfulExit</key>",
		"<false/>",
		"<key>CODEX_HELPER_TEAMS_SERVICE</key>",
	} {
		if !strings.Contains(plist, want) {
			t.Fatalf("plist missing %q:\n%s", want, plist)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatalf("install should only write plist, calls=%#v", runner.calls)
	}
	watchdogPlistPath := filepath.Join(agentDir, teamsServiceLaunchAgentWatchdogPlistName)
	watchdogData, err := os.ReadFile(watchdogPlistPath)
	if err != nil {
		t.Fatalf("read watchdog plist: %v", err)
	}
	watchdogPlist := string(watchdogData)
	for _, want := range []string{
		"<string>" + teamsServiceLaunchAgentWatchdogLabel + "</string>",
		"<string>teams</string>",
		"<string>service</string>",
		"<string>watchdog</string>",
		"<string>--loop</string>",
		"<string>--interval</string>",
		"<string>10s</string>",
		"<string>--quiet</string>",
		"<key>KeepAlive</key>",
		"<key>SuccessfulExit</key>",
		"<false/>",
	} {
		if !strings.Contains(watchdogPlist, want) {
			t.Fatalf("watchdog plist missing %q:\n%s", want, watchdogPlist)
		}
	}

	runner.calls = nil
	cmd = newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"start"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service start: %v", err)
	}
	wantCalls := []teamsServiceCommandCall{{
		name: "launchctl",
		args: []string{"bootstrap", "gui/501", plistPath},
	}, {
		name: "launchctl",
		args: []string{"bootstrap", "gui/501", watchdogPlistPath},
	}}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("launchctl start calls = %#v, want %#v", runner.calls, wantCalls)
	}
}

func TestTeamsServiceInstallWritesWindowsTaskXMLAndRegistersTask(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	xmlDir := filepath.Join(tmp, "config")
	exePath := filepath.Join(tmp, "bin", "codex-proxy.exe")
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            exePath,
		cwd:            cwd,
		windowsTaskDir: xmlDir,
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	xmlPath := filepath.Join(xmlDir, teamsServiceWindowsTaskXMLName)
	data, err := os.ReadFile(xmlPath)
	if err != nil {
		t.Fatalf("read task xml: %v", err)
	}
	taskXML := string(data)
	for _, want := range []string{
		"<LogonType>InteractiveToken</LogonType>",
		"<RunLevel>LeastPrivilege</RunLevel>",
		"<Enabled>false</Enabled>",
		"<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"<Command>powershell.exe</Command>",
		"-WindowStyle Hidden",
		"<WorkingDirectory>" + cwd + "</WorkingDirectory>",
		`&amp; &apos;` + exePath + `&apos; &apos;teams&apos; &apos;run&apos; &apos;--owner-stale-after&apos; &apos;18s&apos; &apos;--registry&apos; &apos;` + registryPath + `&apos;`,
		`$code = $LASTEXITCODE`,
		`exit $code`,
		`$env:CODEX_HELPER_TEAMS_SERVICE = &apos;1&apos;`,
		"<RestartOnFailure>",
		"<Count>999</Count>",
	} {
		if !strings.Contains(taskXML, want) {
			t.Fatalf("task xml missing %q:\n%s", want, taskXML)
		}
	}
	watchdogXMLPath := filepath.Join(xmlDir, teamsServiceWindowsWatchdogTaskXMLName)
	watchdogData, err := os.ReadFile(watchdogXMLPath)
	if err != nil {
		t.Fatalf("read watchdog task xml: %v", err)
	}
	watchdogXML := string(watchdogData)
	for _, want := range []string{
		"<Description>Codex Helper Teams service watchdog</Description>",
		"<CalendarTrigger>",
		"<Interval>PT1M</Interval>",
		"<RestartOnFailure>",
		"<Interval>PT1M</Interval>",
		"<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"<Command>powershell.exe</Command>",
		`&amp; &apos;` + exePath + `&apos; &apos;teams&apos; &apos;service&apos; &apos;watchdog&apos; &apos;--loop&apos; &apos;--interval&apos; &apos;10s&apos; &apos;--quiet&apos;`,
		`$env:CODEX_HELPER_TEAMS_SERVICE = &apos;1&apos;`,
	} {
		if !strings.Contains(watchdogXML, want) {
			t.Fatalf("watchdog task xml missing %q:\n%s", want, watchdogXML)
		}
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Register-ScheduledTask") {
		t.Fatalf("install should register task with powershell, calls=%#v", runner.calls)
	}
	if joined := strings.Join(runner.calls[0].args, " "); !strings.Contains(joined, teamsServiceWindowsWatchdogTaskName) {
		t.Fatalf("install should register watchdog task too, call=%s", joined)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Start-ScheduledTask", "Enable-ScheduledTask")

	runner.calls = nil
	cmd = newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"enable"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service enable: %v", err)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Enable-ScheduledTask") {
		t.Fatalf("enable should call powershell scheduled task command, calls=%#v", runner.calls)
	}
	if joined := strings.Join(runner.calls[0].args, " "); !strings.Contains(joined, teamsServiceWindowsTaskName) || !strings.Contains(joined, teamsServiceWindowsWatchdogTaskName) {
		t.Fatalf("enable should include main and watchdog tasks, call=%s", joined)
	}
}

func TestTeamsServiceWindowsPowerShellPropagatesChildExitCodeCI(t *testing.T) {
	shell, err := exec.LookPath("pwsh")
	if err != nil {
		shell, err = exec.LookPath("powershell.exe")
	}
	if err != nil {
		t.Skip("PowerShell is not available")
	}

	tmp := t.TempDir()
	child := filepath.Join(tmp, "exit-37")
	if runtime.GOOS == "windows" {
		child += ".cmd"
		if err := os.WriteFile(child, []byte("@exit /b 37\r\n"), 0o700); err != nil {
			t.Fatalf("write child command: %v", err)
		}
	} else {
		if err := os.WriteFile(child, []byte("#!/bin/sh\nexit 37\n"), 0o700); err != nil {
			t.Fatalf("write child command: %v", err)
		}
	}
	script := buildTeamsServiceWindowsPowerShell(teamsServiceSpec{
		Executable:  child,
		Environment: map[string]string{"CODEX_HELPER_TEAMS_SERVICE": "1"},
	}, []string{"teams", "run"})
	cmd := exec.Command(shell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	err = cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("PowerShell error = %T %v, want exit error", err, err)
	}
	if got := exitErr.ExitCode(); got != 37 {
		t.Fatalf("PowerShell exit code = %d, want child exit code 37", got)
	}
}

func TestTeamsServiceBootstrapShowsControlChatInForeground(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	var openValues []bool
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            cwd,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, gotRegistry *string, openControl bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			openValues = append(openValues, openControl)
			if gotRegistry == nil || *gotRegistry != registryPath {
				got := "<nil>"
				if gotRegistry != nil {
					got = *gotRegistry
				}
				t.Fatalf("registry path = %q, want %q", got, registryPath)
			}
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
				Opened: openControl,
			}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	if len(openValues) != 1 || !openValues[0] {
		t.Fatalf("bootstrap control open values = %#v, want one true", openValues)
	}
	got := out.String()
	for _, want := range []string{
		"BOOTSTRAP COMPLETE",
		"Teams service bootstrap ready: wsl-windows-task-scheduler",
		"NEXT STEP: OPEN THE TEAMS CONTROL CHAT",
		"Open:",
		"https://teams.microsoft.com/l/chat/control",
		"Then send:",
		"help",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("bootstrap output missing %q:\n%s", want, got)
		}
	}
	completeIdx := strings.Index(got, "BOOTSTRAP COMPLETE")
	nextIdx := strings.Index(got, "NEXT STEP: OPEN THE TEAMS CONTROL CHAT")
	if completeIdx < 0 || nextIdx < 0 || completeIdx > nextIdx {
		t.Fatalf("bootstrap should put the user's next step last:\n%s", got)
	}
	for _, unwanted := range []string{
		"Title: 🏠 Codex Control - workstation",
		"I also tried to open it automatically.",
	} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("bootstrap next-step output should not mix in secondary info %q:\n%s", unwanted, got)
		}
	}
}

func TestTeamsServiceBootstrapPreparesControlChatWithServiceCodexHomeEnv(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	home := filepath.Join(tmp, "home")
	prevUserHome := effectivePathsUserHomeDir
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { effectivePathsUserHomeDir = prevUserHome })
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "")

	runner := &recordingTeamsServiceRunner{}
	var gotHome string
	var gotDir string
	var gotService string
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:     filepath.Join(tmp, "work"),
		unitDir: filepath.Join(tmp, "systemd-user"),
		runner:  runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, _ *string, _ bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			gotHome = os.Getenv("CODEX_HOME")
			gotDir = os.Getenv("CODEX_DIR")
			gotService = os.Getenv("CODEX_HELPER_TEAMS_SERVICE")
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
			}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	wantCodexHome := filepath.Join(home, ".codex")
	if gotHome != wantCodexHome || gotDir != wantCodexHome {
		t.Fatalf("bootstrap control chat Codex env = CODEX_HOME:%q CODEX_DIR:%q, want both %q", gotHome, gotDir, wantCodexHome)
	}
	if gotService != "1" {
		t.Fatalf("bootstrap control chat CODEX_HELPER_TEAMS_SERVICE = %q, want 1", gotService)
	}
	if os.Getenv("CODEX_HOME") != "" || os.Getenv("CODEX_DIR") != "" {
		t.Fatalf("bootstrap control chat should restore caller Codex env, got CODEX_HOME:%q CODEX_DIR:%q", os.Getenv("CODEX_HOME"), os.Getenv("CODEX_DIR"))
	}
}

func TestTeamsServiceBootstrapPreparesControlChatWithRelativeCodexDirEnv(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cwd := filepath.Join(tmp, "work")
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "relative-codex")

	runner := &recordingTeamsServiceRunner{}
	var gotHome string
	var gotDir string
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:     cwd,
		unitDir: filepath.Join(tmp, "systemd-user"),
		runner:  runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, _ *string, _ bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			gotHome = os.Getenv("CODEX_HOME")
			gotDir = os.Getenv("CODEX_DIR")
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
			}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	wantCodexHome := filepath.Join(cwd, "relative-codex")
	if gotHome != wantCodexHome || gotDir != wantCodexHome {
		t.Fatalf("bootstrap control chat Codex env = CODEX_HOME:%q CODEX_DIR:%q, want both %q", gotHome, gotDir, wantCodexHome)
	}
	if os.Getenv("CODEX_HOME") != "" || os.Getenv("CODEX_DIR") != "relative-codex" {
		t.Fatalf("bootstrap control chat should restore caller Codex env, got CODEX_HOME:%q CODEX_DIR:%q", os.Getenv("CODEX_HOME"), os.Getenv("CODEX_DIR"))
	}
}

func TestTeamsServiceBootstrapPreparesControlChatWithResolvedRegistryPath(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cwd := filepath.Join(tmp, "work dir")
	registryPath := "teams registry.json"
	runner := &recordingTeamsServiceRunner{}
	var gotRegistry string
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:     cwd,
		unitDir: filepath.Join(tmp, "systemd-user"),
		runner:  runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, gotRegistryPath *string, _ bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			if gotRegistryPath != nil {
				gotRegistry = *gotRegistryPath
			}
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
			}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	wantRegistry := filepath.Join(cwd, registryPath)
	if gotRegistry != wantRegistry {
		t.Fatalf("bootstrap control chat registry path = %q, want %q", gotRegistry, wantRegistry)
	}
}

func TestTeamsServiceBootstrapSuppressesControlChatDiagnosticOutput(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	var sawDiscardedDiagnostic bool
	registryPath := "/home/alice/teams registry.json"
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		bootstrapControlChat: func(ctx context.Context, root *rootOptions, gotRegistryPath *string, openControl bool, errOut io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			_, _ = fmt.Fprintln(errOut, "Teams Graph proxy: using http://127.0.0.1:44438")
			sawDiscardedDiagnostic = true
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
				Opened: true,
			}, nil
		},
	})

	var out strings.Builder
	var errOut strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s\nstderr:\n%s", err, out.String(), errOut.String())
	}
	if !sawDiscardedDiagnostic {
		t.Fatal("bootstrap control chat hook was not called")
	}
	if got := out.String() + errOut.String(); strings.Contains(got, "Teams Graph proxy: using") {
		t.Fatalf("bootstrap should suppress control chat diagnostics, got:\n%s", got)
	}
}

func TestTeamsServiceBootstrapPrintsControlChatRecoveryAsFinalStep(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	registryPath := "/home/alice/teams registry.json"
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		bootstrapControlChat: func(ctx context.Context, root *rootOptions, gotRegistryPath *string, openControl bool, errOut io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			return teamsServiceBootstrapControlChatResult{}, errors.New("Teams Graph proxy is enabled but no unambiguous proxy profile is configured")
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	for _, want := range []string{
		"BOOTSTRAP COMPLETE",
		"NEXT STEP: PRINT THE TEAMS CONTROL CHAT LINK",
		"Reason: Teams Graph proxy is enabled",
		"codex-proxy teams control",
		"Then open the printed link and send:",
		"help",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("bootstrap output missing %q:\n%s", want, got)
		}
	}
	completeIdx := strings.Index(got, "BOOTSTRAP COMPLETE")
	nextIdx := strings.Index(got, "NEXT STEP: PRINT THE TEAMS CONTROL CHAT LINK")
	if completeIdx < 0 || nextIdx < 0 || completeIdx > nextIdx {
		t.Fatalf("control chat recovery should be the final next-step block:\n%s", got)
	}
}

func TestTeamsServiceBootstrapErrorSummaryKeepsFailuresReadable(t *testing.T) {
	longPowerShellError := strings.Repeat("Register-ScheduledTask failed with noisy diagnostic details. ", 20)
	got := teamsServiceBootstrapErrorSummary(errors.New(longPowerShellError))
	if strings.Contains(got, "\n") {
		t.Fatalf("summary should be single-line, got %q", got)
	}
	if len(got) > 323 {
		t.Fatalf("summary should be bounded, got len=%d text=%q", len(got), got)
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("long summary should be truncated, got %q", got)
	}

	accessDenied := teamsServiceBootstrapErrorSummary(errors.New("Register-ScheduledTask : Access is denied.\nAt line:1 char:1\nlarge command"))
	if strings.Contains(accessDenied, "Register-ScheduledTask") || !strings.Contains(accessDenied, "Windows denied permission") {
		t.Fatalf("access denied should be summarized without raw PowerShell noise, got %q", accessDenied)
	}
}

func TestTeamsServiceBootstrapCanSkipAutomaticControlChatOpen(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	var openValues []bool
	registryPath := "/home/alice/teams registry.json"
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, _ *string, openControl bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			openValues = append(openValues, openControl)
			return teamsServiceBootstrapControlChatResult{URL: "https://teams.microsoft.com/l/chat/control"}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	if len(openValues) != 1 || openValues[0] {
		t.Fatalf("bootstrap control open values = %#v, want one false", openValues)
	}
	if got := out.String(); !strings.Contains(got, "NEXT STEP: OPEN THE TEAMS CONTROL CHAT") || !strings.Contains(got, "https://teams.microsoft.com/l/chat/control") || strings.Contains(got, "I also tried to open it automatically") {
		t.Fatalf("bootstrap --no-open-control output is wrong:\n%s", got)
	}
}

func TestTeamsServiceBootstrapSchedulesPendingHelperActivationBeforeStartingOldWindowsEntry(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.73_windows_amd64.exe.123")
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
		teamsServiceStartDetached = prevDetached
	})
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		if filepath.Clean(path) != filepath.Clean(exe) || goos != "windows" {
			t.Fatalf("FindPendingReplacements path/goos = %q/%q, want %q/windows", path, goos, exe)
		}
		return []update.PendingReplacement{{Path: pending, Version: "0.1.0-rc.73", ModTime: time.Now()}}, nil
	}
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch filepath.Clean(path) {
		case filepath.Clean(exe):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.68"}, nil
		case filepath.Clean(pending):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.73"}, nil
		default:
			t.Fatalf("unexpected probe path %q", path)
			return update.BinaryVersion{}, nil
		}
	}
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string(nil), args...)
		return nil
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            exe,
		windowsTaskDir: filepath.Join(tmp, "tasks"),
		runner:         runner,
	})

	result, err := bootstrapTeamsService(context.Background(), nil, teamsServiceBootstrapOptions{})
	if err != nil {
		t.Fatalf("bootstrapTeamsService error: %v", err)
	}
	if result.Mode != "windows-pending-helper-activation" || !strings.HasSuffix(result.Path, teamsServiceWindowsTaskXMLName) {
		t.Fatalf("bootstrap result = %#v, want pending activation", result)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("PowerShell calls = %#v, want install/register and enable only before activation", runner.calls)
	}
	if joinedCalls := strings.Join([]string{strings.Join(runner.calls[0].args, " "), strings.Join(runner.calls[1].args, " ")}, "\n"); !strings.Contains(joinedCalls, "Register-ScheduledTask") || !strings.Contains(joinedCalls, "Enable-ScheduledTask") {
		t.Fatalf("bootstrap should repair and enable tasks before pending activation, calls=%#v", runner.calls)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Start-ScheduledTask")
	if detachedName == "" {
		t.Fatal("pending activation did not schedule detached PowerShell")
	}
	joined := strings.Join(detachedArgs, " ")
	for _, want := range []string{
		pending,
		exe,
		teamsServiceWindowsTaskName,
		teamsServiceWindowsWatchdogTaskName,
		"$want='0.1.0-rc.73'",
		"Move-Item -Force",
		"if (Test-DestVersion) { $ready=$true }",
		"if ($ready) { foreach",
		"Start-ScheduledTask",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("activation command missing %q:\nname=%s\nargs=%s", want, detachedName, joined)
		}
	}
}

func TestTeamsServiceOpenURLCommandMatrix(t *testing.T) {
	lockCLITestHooks(t)

	tests := []struct {
		name       string
		goos       string
		isWSL      bool
		wantExe    string
		wantArg    string
		wantErr    string
		wantCmdExe bool
	}{
		{name: "windows", goos: "windows", wantExe: "rundll32.exe", wantArg: "url.dll,FileProtocolHandler"},
		{name: "macos", goos: "darwin", wantExe: "open"},
		{name: "linux desktop", goos: "linux", wantExe: "xdg-open"},
		{name: "wsl", goos: "linux", isWSL: true, wantCmdExe: true, wantArg: "start"},
		{name: "unsupported", goos: "freebsd", wantErr: "not supported"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:      tc.goos,
				exe:       "/tmp/codex-proxy",
				cwd:       "/tmp",
				isWSL:     tc.isWSL,
				runner:    &recordingTeamsServiceRunner{},
				unitDir:   filepath.Join(t.TempDir(), "systemd"),
				userID:    "501",
				wslDistro: "Ubuntu",
			})
			name, args, err := teamsServiceOpenURLCommand("https://teams.microsoft.com/l/chat/control")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("open URL error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("open URL command error: %v", err)
			}
			if tc.wantCmdExe {
				if filepath.Base(name) != "cmd.exe" {
					t.Fatalf("WSL opener = %q, want cmd.exe or absolute cmd.exe", name)
				}
			} else if name != tc.wantExe {
				t.Fatalf("opener = %q, want %q", name, tc.wantExe)
			}
			joined := strings.Join(args, " ")
			if !strings.Contains(joined, "https://teams.microsoft.com/l/chat/control") {
				t.Fatalf("opener args missing URL: %#v", args)
			}
			if tc.wantArg != "" && !strings.Contains(joined, tc.wantArg) {
				t.Fatalf("opener args missing %q: %#v", tc.wantArg, args)
			}
		})
	}
}

func TestTeamsServiceDoctorReportsWSLHint(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		unitDir:        filepath.Join(tmp, "systemd", "user"),
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"doctor"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service doctor: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Teams service backend: wsl-windows-task-scheduler",
		"Codex Helper Teams Bridge (WSL Ubuntu alice default ",
		"WSL: detected",
		"wsl.exe",
		"WSL supervisor readiness",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Get-Command wsl.exe") || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Get-ScheduledTask") {
		t.Fatalf("doctor should run a non-destructive WSL readiness probe, calls=%#v", runner.calls)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Register-ScheduledTask", "Start-ScheduledTask", "Enable-ScheduledTask", "Disable-ScheduledTask")
}

func TestTeamsServiceRunPowerShellIncludesCombinedOutputOnFailure(t *testing.T) {
	lockCLITestHooks(t)

	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:  "linux",
		exe:   "/tmp/codex-proxy",
		cwd:   "/tmp",
		isWSL: true,
		runner: &recordingTeamsServiceRunner{
			output: []byte("Register-ScheduledTask : Access is denied.\n"),
			err:    errors.New("exit status 1"),
		},
	})

	_, err := teamsServiceRunPowerShell(context.Background(), "Register-ScheduledTask -TaskName 'Codex Helper Teams Bridge'")
	if err == nil {
		t.Fatal("teamsServiceRunPowerShell error = nil, want failure")
	}
	if !strings.Contains(err.Error(), "exit status 1") || !strings.Contains(err.Error(), "Access is denied") {
		t.Fatalf("PowerShell error did not preserve combined output:\n%v", err)
	}
	if !isTeamsServiceWindowsAccessDeniedError(err) {
		t.Fatalf("PowerShell access denied output should be classified as access denied:\n%v", err)
	}
}

func TestTeamsServiceDoctorReportsWSLReadinessFailure(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{err: errors.New("powershell missing")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		unitDir:        filepath.Join(tmp, "systemd", "user"),
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"doctor"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "WSL Windows Scheduled Task readiness check failed") {
		t.Fatalf("doctor error = %v, want WSL readiness failure", err)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
		t.Fatalf("doctor readiness calls=%#v", runner.calls)
	}
}

func TestTeamsServiceRunPowerShellCanUseExplicitExecutablePath(t *testing.T) {
	lockCLITestHooks(t)

	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  "/home/alice/bin/codex-proxy",
		cwd:                  "/home/alice/work",
		isWSL:                true,
		runner:               runner,
		powerShellExecutable: "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
	})

	if _, err := teamsServiceRunPowerShell(context.Background(), "Get-Command wsl.exe"); err != nil {
		t.Fatalf("teamsServiceRunPowerShell error: %v", err)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe" {
		t.Fatalf("powershell executable call = %#v", runner.calls)
	}
}

func TestTeamsServiceInstallWritesWSLWindowsTask(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex-home"))
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu-22.04",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/registry.json"))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(tmp, "wsl-task", "codex-helper-teams-wsl-task-*.txt"))
	if err != nil {
		t.Fatalf("glob WSL task config: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("WSL task config files = %#v, want one", files)
	}
	configPath := files[0]
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read WSL task config: %v", err)
	}
	config := string(data)
	wantCWD := teamsServiceTestAbsPath(t, "/home/alice/work dir")
	wantExe := teamsServiceTestAbsPath(t, "/home/alice/bin/codex-proxy")
	wantRegistry := teamsServiceTestRegistryPath(wantCWD, "/home/alice/registry.json")
	for _, want := range []string{
		"TaskName=Codex Helper Teams Bridge (WSL Ubuntu-22.04 alice default ",
		"Command=wsl.exe",
		"-d Ubuntu-22.04",
		"-u alice",
		"--cd ",
		"--exec env",
		wantCWD,
		"CODEX_HOME=" + filepath.Join(tmp, "codex-home"),
		wantExe + " teams run --owner-stale-after 18s --auto-service=false --registry",
		wantRegistry,
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("WSL config missing %q:\n%s", want, config)
		}
	}
	if len(runner.calls) != 1 {
		t.Fatalf("install should register WSL scheduled task, calls=%#v", runner.calls)
	}
	call := strings.Join(runner.calls[0].args, " ")
	if runner.calls[0].name != "powershell.exe" || !strings.Contains(call, "Register-ScheduledTask") || !strings.Contains(call, "wsl.exe") {
		t.Fatalf("install should register WSL scheduled task, calls=%#v", runner.calls)
	}
	for _, want := range []string{
		"$expectedActionExecute = 'wscript.exe'",
		"$expectedActionArgument = '//B //Nologo",
		"WScript.Shell",
		"shell.Run(cmd, 0, True)",
		"WScript.Quit code",
		"-WindowStyle Hidden",
		"$wslArgumentLine",
		"Start-Process -FilePath ''wsl.exe''",
		"-RedirectStandardOutput $stdoutLog",
		"-RedirectStandardError $stderrLog",
	} {
		if !strings.Contains(call, want) {
			t.Fatalf("WSL scheduled task should run wsl.exe through a hidden child process while preserving task lifetime; missing %q, calls=%#v", want, runner.calls)
		}
	}
	if strings.Contains(call, "& wsl.exe @wslArgs") {
		t.Fatalf("WSL scheduled task should not launch wsl.exe directly in a visible console path, calls=%#v", runner.calls)
	}
	if strings.Contains(call, "RepetitionInterval") || strings.Contains(call, "Trigger @($logon, $watchdog)") {
		t.Fatalf("WSL scheduled task should not use repeated Task Scheduler triggers, calls=%#v", runner.calls)
	}
	if !strings.Contains(call, "RestartCount 999") {
		t.Fatalf("WSL task should use high restart count for background keepalive, calls=%#v", runner.calls)
	}
	if !strings.Contains(call, "Disable-ScheduledTask") {
		t.Fatalf("install should leave WSL task disabled, calls=%#v", runner.calls)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Start-ScheduledTask", "Enable-ScheduledTask")
}

func TestTeamsServiceWSLTaskNameSeparatesUsersAndProfiles(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "research/profile")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu/Dev",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	aliceResearch := teamsServiceWSLWindowsTaskBackend{}.Name()

	teamsServiceWSLLinuxUserName = func() string { return "bob" }
	bobResearch := teamsServiceWSLWindowsTaskBackend{}.Name()
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "default")
	bobDefault := teamsServiceWSLWindowsTaskBackend{}.Name()

	if aliceResearch == bobResearch || bobResearch == bobDefault || aliceResearch == bobDefault {
		t.Fatalf("WSL task names should be isolated by Linux user and profile: alice=%q bob=%q default=%q", aliceResearch, bobResearch, bobDefault)
	}
	for _, name := range []string{aliceResearch, bobResearch, bobDefault} {
		if strings.ContainsAny(name, `\/:*?"<>|`) {
			t.Fatalf("WSL task name contains Windows-unsafe character: %q", name)
		}
	}
}

func TestTeamsServiceActiveQueriesSupervisorWithoutGeneratedConfig(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	for _, tc := range []struct {
		name     string
		goos     string
		runner   string
		wantName string
	}{
		{name: "darwin", goos: "darwin", runner: "launchctl", wantName: "print"},
		{name: "windows", goos: "windows", runner: "powershell.exe", wantName: "Get-ScheduledTask"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &recordingTeamsServiceRunner{}
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:           tc.goos,
				exe:            filepath.Join(tmp, "codex-proxy"),
				cwd:            tmp,
				launchAgentDir: filepath.Join(tmp, "missing-launch-agents"),
				windowsTaskDir: filepath.Join(tmp, "missing-task-xml"),
				userID:         "501",
				runner:         runner,
			})
			active, err := teamsServiceActive(context.Background())
			if err != nil {
				t.Fatalf("teamsServiceActive error: %v", err)
			}
			if !active {
				t.Fatal("teamsServiceActive = false, want true from supervisor")
			}
			if len(runner.calls) != 1 || runner.calls[0].name != tc.runner || !strings.Contains(strings.Join(runner.calls[0].args, " "), tc.wantName) {
				t.Fatalf("unexpected supervisor calls: %#v", runner.calls)
			}
		})
	}
}

type teamsServiceCommandCall struct {
	name string
	args []string
}

type recordingTeamsServiceRunner struct {
	calls  []teamsServiceCommandCall
	output []byte
	err    error
}

func (r *recordingTeamsServiceRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, teamsServiceCommandCall{
		name: name,
		args: append([]string{}, args...),
	})
	return append([]byte{}, r.output...), r.err
}

func teamsServiceCallSeen(calls []teamsServiceCommandCall, action string) bool {
	for _, call := range calls {
		for _, arg := range call.args {
			if arg == action {
				return true
			}
		}
	}
	return false
}

func assertTeamsServiceCallsDoNotContain(t *testing.T, calls []teamsServiceCommandCall, forbidden ...string) {
	t.Helper()
	for _, call := range calls {
		joined := call.name + " " + strings.Join(call.args, " ")
		for _, value := range forbidden {
			if strings.Contains(joined, value) {
				t.Fatalf("service command contained forbidden %q: %#v", value, calls)
			}
		}
	}
}

type teamsServiceTestHooks struct {
	goos                 string
	exe                  string
	cwd                  string
	unitDir              string
	launchAgentDir       string
	windowsTaskDir       string
	userID               string
	isWSL                bool
	wslDistro            string
	wslLinuxUser         string
	powerShellExecutable string
	runner               teamsServiceCommandRunner
	bootstrapControlChat func(context.Context, *rootOptions, *string, bool, io.Writer) (teamsServiceBootstrapControlChatResult, error)
}

func withTeamsServiceTestHooks(t *testing.T, hooks teamsServiceTestHooks) {
	t.Helper()
	t.Setenv(envTeamsCodexChild, "")
	t.Setenv(envTeamsCodexParentPID, "")
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "")
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE_MODE", "")
	t.Setenv("CODEX_HELPER_TEAMS_AUTO_SERVICE", "")
	prevGOOS := teamsServiceGOOS
	prevExecutable := teamsServiceExecutable
	prevGetwd := teamsServiceGetwd
	prevSystemdUserDir := teamsServiceSystemdUserDir
	prevLaunchAgentDir := teamsServiceLaunchAgentDir
	prevWindowsTaskXMLDir := teamsServiceWindowsTaskXMLDir
	prevUserID := teamsServiceUserID
	prevIsWSL := teamsServiceIsWSL
	prevWSLDistroName := teamsServiceWSLDistroName
	prevWSLLinuxUserName := teamsServiceWSLLinuxUserName
	prevPowerShellExecutable := teamsServicePowerShellExecutable
	prevSystemctl := teamsServiceSystemctl
	prevAuthPreflight := teamsServiceAuthPreflight
	prevBootstrapControlChat := teamsServiceBootstrapControlChat
	prevListLocalProcesses := teamsServiceListLocalProcesses
	prevTerminateLocalProcess := teamsServiceTerminateLocalProcess
	prevLocalProcessGraceDelay := teamsServiceLocalProcessGraceDelay
	teamsServiceGOOS = func() string { return hooks.goos }
	teamsServiceExecutable = func() (string, error) { return hooks.exe, nil }
	teamsServiceGetwd = func() (string, error) { return hooks.cwd, nil }
	teamsServiceSystemdUserDir = func() (string, error) { return hooks.unitDir, nil }
	teamsServiceLaunchAgentDir = func() (string, error) { return hooks.launchAgentDir, nil }
	teamsServiceWindowsTaskXMLDir = func() (string, error) { return hooks.windowsTaskDir, nil }
	teamsServiceUserID = func() string { return hooks.userID }
	teamsServiceIsWSL = func() bool { return hooks.isWSL }
	teamsServiceWSLDistroName = func() string { return hooks.wslDistro }
	teamsServiceWSLLinuxUserName = func() string { return hooks.wslLinuxUser }
	teamsServicePowerShellExecutable = func() string {
		if hooks.powerShellExecutable != "" {
			return hooks.powerShellExecutable
		}
		return "powershell.exe"
	}
	teamsServiceSystemctl = hooks.runner
	teamsServiceAuthPreflight = func() error { return nil }
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) { return nil, nil }
	teamsServiceTerminateLocalProcess = func(int, time.Duration) error { return nil }
	teamsServiceLocalProcessGraceDelay = 0
	teamsServiceBootstrapControlChat = func(ctx context.Context, root *rootOptions, registryPath *string, openControl bool, errOut io.Writer) (teamsServiceBootstrapControlChatResult, error) {
		if hooks.bootstrapControlChat != nil {
			return hooks.bootstrapControlChat(ctx, root, registryPath, openControl, errOut)
		}
		return teamsServiceBootstrapControlChatResult{}, nil
	}
	t.Cleanup(func() {
		teamsServiceGOOS = prevGOOS
		teamsServiceExecutable = prevExecutable
		teamsServiceGetwd = prevGetwd
		teamsServiceSystemdUserDir = prevSystemdUserDir
		teamsServiceLaunchAgentDir = prevLaunchAgentDir
		teamsServiceWindowsTaskXMLDir = prevWindowsTaskXMLDir
		teamsServiceUserID = prevUserID
		teamsServiceIsWSL = prevIsWSL
		teamsServiceWSLDistroName = prevWSLDistroName
		teamsServiceWSLLinuxUserName = prevWSLLinuxUserName
		teamsServicePowerShellExecutable = prevPowerShellExecutable
		teamsServiceSystemctl = prevSystemctl
		teamsServiceAuthPreflight = prevAuthPreflight
		teamsServiceBootstrapControlChat = prevBootstrapControlChat
		teamsServiceListLocalProcesses = prevListLocalProcesses
		teamsServiceTerminateLocalProcess = prevTerminateLocalProcess
		teamsServiceLocalProcessGraceDelay = prevLocalProcessGraceDelay
	})
}

func stringPtr(s string) *string {
	return &s
}
