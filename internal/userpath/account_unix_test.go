//go:build !windows

package userpath

import (
	"context"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

func TestAccountDefaultLiveShellMatrix(t *testing.T) {
	if os.Getenv("CODEX_HELPER_USER_PATH_LIVE") != "1" {
		t.Skip("set CODEX_HELPER_USER_PATH_LIVE=1 to modify and restore real account profiles")
	}
	target, helper := currentProbeTestIdentity(t)
	target.Groups = currentProcessGroups()
	target.GroupsKnown = true

	type shellCase struct {
		name     string
		commands []string
		marker   string
	}
	newMarker := func(name string) string {
		marker := filepath.Join(t.TempDir(), name+" user path", "bin")
		if err := os.MkdirAll(marker, 0o700); err != nil {
			t.Fatal(err)
		}
		return marker
	}
	shMarker := newMarker("sh")
	bashMarker := newMarker("bash")
	zshMarker := newMarker("zsh")
	fishMarker := newMarker("fish")
	cshMarker := newMarker("csh")
	pwshMarker := newMarker("pwsh")
	nuMarker := newMarker("nu")

	appendLiveProfile(t, filepath.Join(target.Home, ".profile"), "export PATH="+quotePOSIX(shMarker)+":\"$PATH\"")
	appendLiveProfile(t, filepath.Join(target.Home, ".bash_profile"), "export PATH="+quotePOSIX(bashMarker)+":\"$PATH\"")
	appendLiveProfile(t, filepath.Join(target.Home, ".bashrc"), "export PATH="+quotePOSIX(bashMarker)+":\"$PATH\"")
	appendLiveProfile(t, filepath.Join(target.Home, ".zprofile"), "export PATH="+quotePOSIX(zshMarker)+":\"$PATH\"")
	appendLiveProfile(t, filepath.Join(target.Home, ".zshrc"), "export PATH="+quotePOSIX(zshMarker)+":\"$PATH\"")
	appendLiveProfile(t, filepath.Join(target.Home, ".cshrc"), "setenv PATH "+quotePOSIX(cshMarker)+":$PATH")
	appendLiveProfile(t, filepath.Join(target.Home, ".login"), "setenv PATH "+quotePOSIX(cshMarker)+":$PATH")
	appendLiveProfile(t, filepath.Join(target.Home, ".config", "fish", "config.fish"), "set -gx PATH "+quotePOSIX(fishMarker)+" $PATH")
	appendLiveProfile(t, filepath.Join(target.Home, ".config", "powershell", "Microsoft.PowerShell_profile.ps1"), "$env:PATH = "+quotePowerShell(pwshMarker)+" + [IO.Path]::PathSeparator + $env:PATH")
	appendLiveProfile(t, filepath.Join(target.Home, ".config", "nushell", "env.nu"), "$env.PATH = ($env.PATH | prepend "+quotePOSIX(nuMarker)+")")

	cases := []shellCase{
		{name: "sh", commands: []string{"sh"}, marker: shMarker},
		{name: "bash", commands: []string{"bash"}, marker: bashMarker},
		{name: "zsh", commands: []string{"zsh"}, marker: zshMarker},
		{name: "fish", commands: []string{"fish"}, marker: fishMarker},
		{name: "csh", commands: []string{"csh"}, marker: cshMarker},
		{name: "tcsh", commands: []string{"tcsh"}, marker: cshMarker},
		{name: "pwsh", commands: []string{"pwsh", "powershell"}, marker: pwshMarker},
		{name: "nu", commands: []string{"nu"}, marker: nuMarker},
	}
	required := make(map[string]bool)
	for _, name := range strings.FieldsFunc(os.Getenv("CODEX_HELPER_USER_PATH_LIVE_SHELLS"), func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	}) {
		required[strings.ToLower(name)] = true
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			shell := ""
			for _, command := range test.commands {
				if found, err := exec.LookPath(command); err == nil {
					shell, _ = filepath.Abs(found)
					break
				}
			}
			if shell == "" {
				if required[test.name] {
					t.Fatalf("required live shell %s is not installed", test.name)
				}
				t.Skip("shell is not installed")
			}
			result, err := resolveAccountDefault(context.Background(), Request{
				Policy: Policy{Mode: ModeAccountDefault, ShellOverride: shell},
				Target: target,
				ServiceEnvironment: []string{
					"PATH=/service-only",
					"LANG=C.UTF-8",
					"TERM=dumb",
					"XDG_CONFIG_HOME=" + filepath.Join(target.Home, ".config"),
				},
				HelperExecutable: helper,
				Timeout:          15 * time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !pathListContains(result.Path, test.marker) {
				t.Fatalf("%s PATH did not source its account profile marker %q: %q", test.name, test.marker, result.Path)
			}
			if pathListContains(result.Path, "/service-only") {
				t.Fatalf("%s PATH leaked service-only baseline: %q", test.name, result.Path)
			}
		})
	}
}

func appendLiveProfile(t *testing.T, path string, line string) {
	t.Helper()
	data, readErr := os.ReadFile(path)
	info, statErr := os.Stat(path)
	existed := readErr == nil && statErr == nil
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatal(readErr)
	}
	if statErr != nil && !os.IsNotExist(statErr) {
		t.Fatal(statErr)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	mode := os.FileMode(0o600)
	if existed {
		mode = info.Mode().Perm()
	}
	updated := append(append([]byte(nil), data...), []byte("\n# codex-helper live user PATH test\n"+line+"\n")...)
	if err := os.WriteFile(path, updated, mode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if existed {
			if err := os.WriteFile(path, data, mode); err != nil {
				t.Errorf("restore %s: %v", path, err)
			}
			return
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Errorf("remove %s: %v", path, err)
		}
	})
}

func pathListContains(pathValue string, want string) bool {
	for _, entry := range filepath.SplitList(pathValue) {
		if entry == want {
			return true
		}
	}
	return false
}

func TestResolveAccountDefaultRunsFramedProbeThroughAccountShell(t *testing.T) {
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	uid, err := strconv.ParseUint(current.Uid, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	gid, err := strconv.ParseUint(current.Gid, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	helper, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	result, err := resolveAccountDefault(context.Background(), Request{
		Policy: Policy{Mode: ModeAccountDefault, ShellOverride: "/bin/sh"},
		Target: TargetIdentity{
			UID: uint32(uid), GID: uint32(gid), Username: current.Username, Home: current.HomeDir,
		},
		ServiceEnvironment: []string{"PATH=/service-only", "LANG=C"},
		HelperExecutable:   helper,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Path == "" || result.AccountShell != "/bin/sh" || result.Adapter != "sh" {
		t.Fatalf("result = %#v", result)
	}
	if strings.Contains(result.Path, "/service-only") {
		t.Fatalf("service PATH leaked through account shell: %q", result.Path)
	}
}

func TestProbeSocketIsPrivateFramedAndCleanedUp(t *testing.T) {
	target, _ := currentProbeTestIdentity(t)
	listener, socketPath, cleanup, err := newProbeSocket(target)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Dir(socketPath)
	for path, wantMode := range map[string]os.FileMode{directory: 0o700, socketPath: 0o600} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			cleanup()
			t.Fatal(statErr)
		}
		if info.Mode().Perm() != wantMode {
			cleanup()
			t.Fatalf("%s mode = %o, want %o", path, info.Mode().Perm(), wantMode)
		}
	}
	nonce := strings.Repeat("d", probeNonceBytes*2)
	writeDone := make(chan error, 1)
	go func() { writeDone <- WriteProbeSocket(socketPath, nonce) }()
	connection, err := listener.AcceptUnix()
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	payload, err := readProbePayload(connection, nonce)
	_ = connection.Close()
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		cleanup()
		t.Fatal(err)
	}
	if payload.Path == "" || payload.UID != target.UID {
		cleanup()
		t.Fatalf("socket payload = %#v", payload)
	}
	cleanup()
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("probe directory still exists after cleanup: %v", err)
	}
}

func TestShellProbeIgnoresStartupNoise(t *testing.T) {
	target, helper := currentProbeTestIdentity(t)
	wrapper := filepath.Join(t.TempDir(), "noisy-probe")
	script := "#!/bin/sh\n" +
		"echo startup-banner\n" +
		"echo startup-warning >&2\n" +
		"exec " + quotePOSIX(helper) + " \"$@\"\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, err := resolveAccountDefault(context.Background(), Request{
		Policy: Policy{Mode: ModeAccountDefault, ShellOverride: "/bin/sh"},
		Target: target, ServiceEnvironment: []string{"PATH=/service-only", "LANG=C"},
		HelperExecutable: wrapper, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Path == "" || time.Since(started) > 3*time.Second {
		t.Fatalf("noisy/background probe result=%#v duration=%s", result, time.Since(started))
	}
}

func TestShellProbeDoesNotWaitForBackgroundStdoutOrStderr(t *testing.T) {
	target, helper := currentProbeTestIdentity(t)
	shell := filepath.Join(t.TempDir(), "sh")
	// A startup file can legitimately launch an agent in the background. That
	// process inherits the shell's diagnostic descriptors unless it redirects
	// them, but it must not keep Cmd.Wait blocked after the shell and framed
	// probe have both completed.
	script := "#!/bin/sh\n" +
		"sleep 5 &\n" +
		"exec /bin/sh \"$@\"\n"
	if err := os.WriteFile(shell, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, err := resolveAccountDefault(context.Background(), Request{
		Policy: Policy{Mode: ModeAccountDefault, ShellOverride: shell},
		Target: target, ServiceEnvironment: []string{"PATH=/service-only", "LANG=C"},
		HelperExecutable: helper, Timeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Path == "" || time.Since(started) > 2500*time.Millisecond {
		t.Fatalf("background-output probe result=%#v duration=%s", result, time.Since(started))
	}
}

func TestShellProbeAcceptsStartupDirectoryChange(t *testing.T) {
	target, helper := currentProbeTestIdentity(t)
	shell := filepath.Join(t.TempDir(), "sh")
	script := "#!/bin/sh\ncd /\nexec /bin/sh \"$@\"\n"
	if err := os.WriteFile(shell, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	result, err := resolveAccountDefault(context.Background(), Request{
		Policy: Policy{Mode: ModeAccountDefault, ShellOverride: shell},
		Target: target, ServiceEnvironment: []string{"PATH=/service-only", "LANG=C"},
		HelperExecutable: helper, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Path == "" {
		t.Fatal("startup directory change lost PATH")
	}
}

func TestShellProbeAcceptsSameHomeThroughSymlinkAlias(t *testing.T) {
	target, helper := currentProbeTestIdentity(t)
	realHome := target.Home
	aliasHome := filepath.Join(t.TempDir(), "home-alias")
	if err := os.Symlink(realHome, aliasHome); err != nil {
		t.Fatal(err)
	}
	target.Home = aliasHome
	shell := filepath.Join(t.TempDir(), "sh")
	script := "#!/bin/sh\nHOME=" + quotePOSIX(realHome) + "\nexport HOME\nexec /bin/sh \"$@\"\n"
	if err := os.WriteFile(shell, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	result, err := resolveAccountDefault(context.Background(), Request{
		Policy: Policy{Mode: ModeAccountDefault, ShellOverride: shell},
		Target: target, ServiceEnvironment: []string{"PATH=/service-only", "LANG=C"},
		HelperExecutable: helper, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Path == "" {
		t.Fatal("same-file HOME alias lost PATH")
	}
}

func TestShellProbeRunsCshLoginCommandThroughStdin(t *testing.T) {
	target, helper := currentProbeTestIdentity(t)
	shell := filepath.Join(t.TempDir(), "tcsh")
	script := "#!/bin/sh\n" +
		"test \"$#\" -eq 1 || exit 71\n" +
		"test \"$1\" = -l || exit 72\n" +
		"exec /bin/sh\n"
	if err := os.WriteFile(shell, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	result, err := resolveAccountDefault(context.Background(), Request{
		Policy: Policy{Mode: ModeAccountDefault, ShellOverride: shell},
		Target: target, ServiceEnvironment: []string{"PATH=/service-only", "LANG=C"},
		HelperExecutable: helper, Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Adapter != "tcsh" || result.Path == "" {
		t.Fatalf("tcsh probe result = %#v", result)
	}
}

func TestShellProbeTimesOutAfterFrameWhenShellDoesNotExit(t *testing.T) {
	target, helper := currentProbeTestIdentity(t)
	wrapper := filepath.Join(t.TempDir(), "hanging-probe")
	script := "#!/bin/sh\n" + quotePOSIX(helper) + " \"$@\"\nsleep 30\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err := resolveAccountDefault(context.Background(), Request{
		Policy: Policy{Mode: ModeAccountDefault, ShellOverride: "/bin/sh"},
		Target: target, ServiceEnvironment: []string{"PATH=/service-only", "LANG=C"},
		HelperExecutable: wrapper, Timeout: 100 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("timeout error = %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("timeout took too long: %s", time.Since(started))
	}
}

func TestShellProbeRejectsShellFailureAfterValidFrame(t *testing.T) {
	target, helper := currentProbeTestIdentity(t)
	wrapper := filepath.Join(t.TempDir(), "failing-probe")
	script := "#!/bin/sh\n" + quotePOSIX(helper) + " \"$@\"\nexit 7\n"
	if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := resolveAccountDefault(context.Background(), Request{
		Policy: Policy{Mode: ModeAccountDefault, ShellOverride: "/bin/sh"},
		Target: target, ServiceEnvironment: []string{"PATH=/service-only", "LANG=C"},
		HelperExecutable: wrapper, Timeout: 2 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "exited after probe") {
		t.Fatalf("shell failure error = %v", err)
	}
}

func currentProbeTestIdentity(t *testing.T) (TargetIdentity, string) {
	t.Helper()
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	uid, err := strconv.ParseUint(current.Uid, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	gid, err := strconv.ParseUint(current.Gid, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	helper, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	return TargetIdentity{UID: uint32(uid), GID: uint32(gid), Username: current.Username, Home: current.HomeDir}, helper
}

func TestShellFromPasswdRequiresExactIdentity(t *testing.T) {
	data := "alice:x:1000:100:Alice:/home/alice:/bin/zsh\n" +
		"other:x:1001:100:Other:/home/other:/bin/bash\n"
	target := TargetIdentity{UID: 1000, Username: "alice", Home: "/home/alice"}
	if got := shellFromPasswd(data, target); got != "/bin/zsh" {
		t.Fatalf("shell = %q", got)
	}
	for _, changed := range []TargetIdentity{
		{UID: 1001, Username: "alice", Home: "/home/alice"},
		{UID: 1000, Username: "mallory", Home: "/home/alice"},
		{UID: 1000, Username: "alice", Home: "/root"},
	} {
		if got := shellFromPasswd(data, changed); got != "" {
			t.Fatalf("mismatched identity %#v resolved shell %q", changed, got)
		}
	}
}

func TestShellFromPasswdAcceptsSameHomeThroughSymlinkAlias(t *testing.T) {
	realHome := t.TempDir()
	aliasHome := filepath.Join(t.TempDir(), "home-alias")
	if err := os.Symlink(realHome, aliasHome); err != nil {
		t.Fatal(err)
	}
	data := "alice:x:1000:100:Alice:" + realHome + ":/bin/zsh\n"
	target := TargetIdentity{UID: 1000, Username: "alice", Home: aliasHome}
	if got := shellFromPasswd(data, target); got != "/bin/zsh" {
		t.Fatalf("shell through same-file home alias = %q", got)
	}
}

func TestSameAccountHomeRejectsEmptyAndRelativePaths(t *testing.T) {
	for _, test := range [][2]string{{"", "/home/alice"}, {".", "/home/alice"}, {"home/alice", "/home/alice"}} {
		if sameAccountHome(test[0], test[1]) {
			t.Fatalf("sameAccountHome(%q, %q) = true", test[0], test[1])
		}
	}
}

func TestShellFromDSCLOutput(t *testing.T) {
	if got := shellFromDSCLOutput("UserShell: /bin/zsh\n"); got != "/bin/zsh" {
		t.Fatalf("DSCL shell = %q", got)
	}
	for _, output := range []string{"", "UserShell: zsh", "not-a-record"} {
		if got := shellFromDSCLOutput(output); got != "" {
			t.Fatalf("invalid DSCL output %q resolved %q", output, got)
		}
	}
}

func TestAdapterForSupportedShellsAndRejectsUnknown(t *testing.T) {
	tests := []struct {
		shell         string
		wantArgs      []string
		scriptOnStdin bool
	}{
		{shell: "bash", wantArgs: []string{"-lic", "probe"}},
		{shell: "zsh", wantArgs: []string{"-lic", "probe"}},
		{shell: "dash", wantArgs: []string{"-lc", "probe"}},
		{shell: "fish", wantArgs: []string{"--login", "--interactive", "--command", "probe"}},
		{shell: "tcsh", wantArgs: []string{"-l"}, scriptOnStdin: true},
		{shell: "pwsh", wantArgs: []string{"-Login", "-NoLogo", "-Command", "probe"}},
		{shell: "nu", wantArgs: []string{"--login", "--commands", "probe"}},
	}
	for _, test := range tests {
		adapter, err := adapterForShell(filepath.Join("/opt/shells", test.shell))
		if err != nil || adapter.name == "" {
			t.Fatalf("adapter %s = %#v, %v", test.shell, adapter, err)
		}
		got := adapter.args("probe")
		if strings.Join(got, "\x00") != strings.Join(test.wantArgs, "\x00") || adapter.scriptOnStdin != test.scriptOnStdin {
			t.Fatalf("adapter %s args=%#v stdin=%t, want args=%#v stdin=%t", test.shell, got, adapter.scriptOnStdin, test.wantArgs, test.scriptOnStdin)
		}
	}
	if _, err := adapterForShell("/usr/bin/tmux"); err == nil {
		t.Fatal("tmux must not be accepted merely because it may appear in /etc/shells")
	}
}

func TestSupplementaryGroupComparisonIgnoresOrderDuplicatesAndPrimary(t *testing.T) {
	if !sameSupplementaryGroups([]uint32{30, 100, 20, 30}, []uint32{20, 30}, 100) {
		t.Fatal("equivalent supplementary groups did not match")
	}
	if sameSupplementaryGroups([]uint32{20}, []uint32{20, 30}, 100) {
		t.Fatal("different supplementary groups matched")
	}
}

func TestAccountProbeEnvironmentUsesDeterministicBaselineAndStableXDG(t *testing.T) {
	home := t.TempDir()
	xdg := filepath.Join(home, ".config")
	environment, source := accountProbeEnvironment([]string{
		"PATH=/service/private:/usr/bin",
		"VIRTUAL_ENV=/project/.venv",
		"XDG_CONFIG_HOME=" + xdg,
		"ZDOTDIR=/outside",
		"LANG=en_US.UTF-8",
	}, TargetIdentity{Username: "alice", Home: home}, "/bin/zsh")
	pathValue, _ := EnvironmentValue(environment, "PATH", false)
	if strings.Contains(pathValue, "/service/private") || source != "unix-login-default" && source != "darwin-login-default" {
		t.Fatalf("baseline PATH=%q source=%q", pathValue, source)
	}
	if _, ok := EnvironmentValue(environment, "VIRTUAL_ENV", false); ok {
		t.Fatalf("terminal-only VIRTUAL_ENV leaked: %#v", environment)
	}
	if got, _ := EnvironmentValue(environment, "XDG_CONFIG_HOME", false); got != xdg {
		t.Fatalf("XDG_CONFIG_HOME=%q", got)
	}
	if _, ok := EnvironmentValue(environment, "ZDOTDIR", false); ok {
		t.Fatalf("outside-home ZDOTDIR leaked: %#v", environment)
	}
}

func TestWSLBaselinePreservesInteropAndStripsHelperRuntime(t *testing.T) {
	separator := string(os.PathListSeparator)
	helperDir := "/home/alice/.local/bin"
	nodeRoot := "/home/alice/.cache/codex-proxy/node/v22"
	runtimeRoot := "/home/alice/.local/bin/.cxp-runtime"
	pathValue := strings.Join([]string{helperDir, runtimeRoot + "/versions/v1", nodeRoot + "/bin", "/usr/bin", "/usr/games", "/mnt/c/Windows/System32", "/usr/bin"}, separator)
	got, source := defaultBaselinePath([]string{
		"WSL_DISTRO_NAME=Ubuntu",
		"CODEX_HELPER_CLI_DIR=" + helperDir,
		"CODEX_NODE_INSTALL_ROOT=" + nodeRoot,
		"CXP_RUNTIME_ROOT=" + runtimeRoot,
		"PATH=" + pathValue,
	}, TargetIdentity{WSLDistro: "Ubuntu"})
	if source != "wsl-login-default+interop" || strings.Contains(got, helperDir) || strings.Contains(got, runtimeRoot) || strings.Contains(got, nodeRoot) {
		t.Fatalf("WSL baseline=%q source=%q", got, source)
	}
	if !strings.Contains(got, "/mnt/c/Windows/System32") || !strings.Contains(got, "/usr/games") || strings.Count(got, "/usr/bin") != 1 {
		t.Fatalf("WSL interop/order not preserved: %q", got)
	}
}

func TestWSLBaselinePreservesCustomAutomountRoot(t *testing.T) {
	previous := baselineReadFile
	baselineReadFile = func(string) ([]byte, error) {
		return []byte("[automount]\nroot = /windir/\n"), nil
	}
	t.Cleanup(func() { baselineReadFile = previous })
	got, source := defaultBaselinePath([]string{
		"WSL_DISTRO_NAME=Ubuntu",
		"PATH=/usr/bin:/windir/c/Windows/System32:/mnt/c/should-not-be-assumed",
	}, TargetIdentity{WSLDistro: "Ubuntu"})
	if source != "wsl-login-default+interop" || !strings.Contains(got, "/windir/c/Windows/System32") {
		t.Fatalf("custom automount PATH=%q source=%q", got, source)
	}
	if strings.Contains(got, "/mnt/c/should-not-be-assumed") {
		t.Fatalf("default automount root leaked into custom-root PATH: %q", got)
	}
}

func TestWSLBaselineParsesQuotedAutomountRootWithInlineComment(t *testing.T) {
	previous := baselineReadFile
	baselineReadFile = func(string) ([]byte, error) {
		return []byte("[automount]\nroot = \"/windir/\" ; enterprise policy\n"), nil
	}
	t.Cleanup(func() { baselineReadFile = previous })
	got, _ := defaultBaselinePath([]string{
		"WSL_DISTRO_NAME=Ubuntu",
		"PATH=/usr/bin:/windir/c/Windows/System32:/mnt/wsl-tools/bin",
	}, TargetIdentity{WSLDistro: "Ubuntu"})
	if !strings.Contains(got, "/windir/c/Windows/System32") || strings.Contains(got, "/mnt/wsl-tools/bin") {
		t.Fatalf("inline-comment/custom-root PATH = %q", got)
	}
}

func TestSanitizeResolvedPathRemovesOnlyCodexVolatileDirectories(t *testing.T) {
	separator := string(os.PathListSeparator)
	pathValue := strings.Join([]string{
		"/home/alice/.local/bin",
		"/home/alice/.npm-global/bin",
		"/home/alice/.npm-global/lib/node_modules/@openai/codex/node_modules/@openai/codex-linux-x64/vendor/x86_64-unknown-linux-musl/codex-path",
		"/home/alice/.codex/tmp/arg0/codex-arg0ABC",
		"/home/alice/.local/bin/.cxp-runtime/versions/v1",
		"/usr/bin",
		"/usr/bin",
	}, separator)
	got := sanitizeResolvedPath(pathValue, nil)
	if strings.Contains(got, "codex-path") || strings.Contains(got, ".codex/tmp") || strings.Contains(got, ".cxp-runtime") {
		t.Fatalf("volatile directories remain: %q", got)
	}
	if !strings.Contains(got, "/home/alice/.local/bin") || !strings.Contains(got, "/home/alice/.npm-global/bin") || strings.Count(got, "/usr/bin") != 2 {
		t.Fatalf("user PATH entries/order were not preserved: %q", got)
	}
}

func TestSanitizeResolvedPathPreservesEmptyAndWhitespaceEntries(t *testing.T) {
	separator := string(os.PathListSeparator)
	want := strings.Join([]string{"/usr/bin", "", " /path with edge spaces ", "/bin", ""}, separator)
	if got := sanitizeResolvedPath(want, nil); got != want {
		t.Fatalf("sanitized PATH = %q, want byte-preserving %q", got, want)
	}
}
