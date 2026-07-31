package cli

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/userpath"
)

type recordingUserPathResolver struct {
	request userpath.Request
	result  userpath.Result
	err     error
}

func (r *recordingUserPathResolver) Resolve(_ context.Context, request userpath.Request) (userpath.Result, error) {
	r.request = request
	return r.result, r.err
}

func TestResolveTeamsCodexUserPathCarriesPolicyAndTargetIdentity(t *testing.T) {
	previous := teamsUserPathResolver
	fake := &recordingUserPathResolver{result: userpath.Result{Path: "/home/alice/.local/bin:/usr/bin", Mode: userpath.ModeAccountDefault}}
	teamsUserPathResolver = fake
	t.Cleanup(func() { teamsUserPathResolver = previous })

	paths := effectivePaths{
		Home: "/home/alice",
		ExecIdentity: &execIdentity{
			UID: 1000, GID: 100, Groups: []uint32{20, 30}, GroupsKnown: true,
			Username: "alice", Home: "/home/alice",
		},
	}
	result, err := resolveTeamsCodexUserPath(context.Background(), config.Config{TeamsCodexPath: config.TeamsCodexPathPolicy{
		Mode: "account-default", ShellOverride: "/bin/zsh",
	}}, paths, []string{envTeamsHelperCLIPath + "=/opt/cxp", "PATH=/service/bin", "WSL_DISTRO_NAME=Ubuntu"}, "/workspace")
	if err != nil {
		t.Fatal(err)
	}
	if result.Path != fake.result.Path {
		t.Fatalf("result = %#v", result)
	}
	if fake.request.Target.UID != 1000 || fake.request.Target.GID != 100 || fake.request.Target.Username != "alice" || !fake.request.Target.GroupsKnown {
		t.Fatalf("target = %#v", fake.request.Target)
	}
	if fake.request.Policy.ShellOverride != "/bin/zsh" || fake.request.HelperExecutable != "/opt/cxp" || fake.request.Target.WSLDistro != "Ubuntu" {
		t.Fatalf("request = %#v", fake.request)
	}
	cmd := exec.Command("ignored")
	cmd.Env = []string{"PATH=/baseline"}
	if err := fake.request.ConfigureCommand(cmd); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		attribute := reflect.ValueOf(cmd.SysProcAttr)
		if !attribute.IsValid() || attribute.IsNil() {
			t.Fatalf("probe command identity was not applied: %#v", cmd.SysProcAttr)
		}
		credential := attribute.Elem().FieldByName("Credential")
		if !credential.IsValid() || credential.IsNil() {
			t.Fatalf("probe command credential was not applied: %#v", cmd.SysProcAttr)
		}
	}
}

func TestResolveTeamsCodexUpgradePathUsesManagedPrefixNotTargetAccountPATH(t *testing.T) {
	previousProbe := probeManagedCodexUpgradeCandidateForRun
	probeManagedCodexUpgradeCandidateForRun = func(context.Context, string, []string, *execIdentity) error { return nil }
	t.Cleanup(func() { probeManagedCodexUpgradeCandidateForRun = previousProbe })
	previous := teamsUserPathResolver
	targetBin := t.TempDir()
	managedPrefix := t.TempDir()
	t.Setenv("CODEX_NPM_PREFIX", managedPrefix)
	codexName := "codex"
	if runtime.GOOS == "windows" {
		codexName = "codex.exe"
	}
	codexPath := filepath.Join(managedPrefix, "bin", codexName)
	if runtime.GOOS == "windows" {
		codexPath = filepath.Join(managedPrefix, codexName)
	}
	if err := os.MkdirAll(filepath.Dir(codexPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(codexPath, []byte("test"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeProbeableCodex(t, targetBin, true)
	targetPath := targetBin + string(os.PathListSeparator) + t.TempDir()
	teamsUserPathResolver = &recordingUserPathResolver{result: userpath.Result{
		Path: targetPath,
		Mode: userpath.ModeAccountDefault,
	}}
	t.Cleanup(func() { teamsUserPathResolver = previous })

	got, err := resolveTeamsCodexUpgradeTarget(context.Background(), config.Config{}, effectivePaths{Home: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if got.path != codexPath {
		t.Fatalf("upgrade path = %q, want %q", got.path, codexPath)
	}
	if path := envValue(got.environment, "PATH"); path != targetPath {
		t.Fatalf("upgrade environment PATH = %q, want %q", path, targetPath)
	}
}

func TestResolveTeamsCodexUpgradePathReturnsInstallTargetInsteadOfFallingBackToServicePATH(t *testing.T) {
	previous := teamsUserPathResolver
	teamsUserPathResolver = &recordingUserPathResolver{result: userpath.Result{
		Path: t.TempDir(),
		Mode: userpath.ModeAccountDefault,
	}}
	t.Cleanup(func() { teamsUserPathResolver = previous })

	target, err := resolveTeamsCodexUpgradeTarget(context.Background(), config.Config{}, effectivePaths{Home: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if target.path != "" {
		t.Fatalf("upgrade target = %q, want empty managed-install target", target.path)
	}
}

func TestResolveTeamsCodexUpgradeTargetServicePrefersManagedInstallOverSystemNPMAndUnknownCache(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX npm wrapper fixture")
	}
	home := t.TempDir()
	cacheRoot := t.TempDir()
	serviceBin := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", cacheRoot)
	t.Setenv("PATH", serviceBin)

	managedPrefix := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global")
	managedBin := filepath.Join(managedPrefix, "bin")
	if err := os.MkdirAll(managedBin, 0o755); err != nil {
		t.Fatal(err)
	}
	managedCodex := writeProbeableCodex(t, managedBin, true)
	systemNPMBin := filepath.Join(home, ".npm-global", "bin")
	if err := os.MkdirAll(systemNPMBin, 0o755); err != nil {
		t.Fatal(err)
	}
	systemNPMCodex := writeProbeableCodex(t, systemNPMBin, true)
	writeExecutable(t, filepath.Join(serviceBin, "npm"), "#!/bin/sh\nif [ \"$1\" = prefix ] && [ \"$2\" = -g ]; then echo \"$HOME/.npm-global\"; exit 0; fi\nexit 99\n")
	unknownCachedCodex := writeProbeableCodex(t, t.TempDir(), true)
	writeCachedCodexPath(unknownCachedCodex)

	previous := teamsUserPathResolver
	teamsUserPathResolver = &recordingUserPathResolver{result: userpath.Result{
		Path:   os.Getenv("PATH"),
		Mode:   userpath.ModeService,
		Source: "service-environment",
	}}
	t.Cleanup(func() { teamsUserPathResolver = previous })

	target, err := resolveTeamsCodexUpgradeTarget(context.Background(), config.Config{
		TeamsCodexPath: config.TeamsCodexPathPolicy{Mode: string(userpath.ModeService)},
	}, effectivePaths{Home: home})
	if err != nil {
		t.Fatal(err)
	}
	if target.path != managedCodex {
		t.Fatalf("upgrade target = %q, want managed install %q instead of system npm %q or unknown cache %q", target.path, managedCodex, systemNPMCodex, unknownCachedCodex)
	}
}

func TestTeamsCodexUpgradeLiveTargetPATH(t *testing.T) {
	if os.Getenv("CODEX_HELPER_TEAMS_UPGRADE_LIVE") != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_UPGRADE_LIVE=1 to run the real npm upgrade")
	}
	codexDir := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_UPGRADE_CODEX_DIR"))
	if codexDir == "" {
		t.Fatal("CODEX_HELPER_TEAMS_UPGRADE_CODEX_DIR is required")
	}
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	if runtime.GOOS == "windows" {
		callerProfile := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_UPGRADE_CALLER_PROFILE"))
		if callerProfile == "" {
			t.Fatal("CODEX_HELPER_TEAMS_UPGRADE_CALLER_PROFILE is required on Windows; TestMain must not recover the ambient profile implicitly")
		}
		if !filepath.IsAbs(callerProfile) {
			t.Fatalf("CODEX_HELPER_TEAMS_UPGRADE_CALLER_PROFILE must be absolute, got %q", callerProfile)
		}
		if samePath(callerProfile, os.Getenv("USERPROFILE")) {
			t.Fatalf("live shard restored caller profile globally instead of passing it through the explicit test-only channel: %q", callerProfile)
		}
		t.Setenv("LOCALAPPDATA", t.TempDir())
	}
	servicePath := os.Getenv("PATH")
	if path, ok := teamsCodexExecutableOnPath(servicePath); ok && samePath(filepath.Dir(path), codexDir) {
		t.Fatalf("test precondition failed: service PATH already exposes target Codex %s", path)
	}
	targetPath := codexDir + string(os.PathListSeparator) + servicePath
	mode := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_UPGRADE_PATH_MODE"))
	if mode == "" {
		mode = string(userpath.ModeExplicit)
	}
	pathPolicy := config.TeamsCodexPathPolicy{Mode: mode}
	if mode == string(userpath.ModeExplicit) || mode == string(userpath.ModeCapturedTerminal) {
		pathPolicy.ExplicitPath = targetPath
	}
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{
		Version:        config.CurrentVersion,
		TeamsCodexPath: pathPolicy,
		ProxyEnabled:   boolPtr(false),
	}); err != nil {
		t.Fatal(err)
	}

	result, err := runTeamsCodexUpgradeFromBridge(context.Background(), &rootOptions{configPath: cfgPath}, io.Discard, "")
	if err != nil {
		t.Fatal(err)
	}
	if !samePath(filepath.Dir(result.Path), codexDir) {
		t.Fatalf("upgraded path = %q, want directory %q", result.Path, codexDir)
	}
	if err := probeCodexVersion(context.Background(), result.Path); err != nil {
		t.Fatalf("upgraded Codex is not functional: %v", err)
	}
	runtimeContract := applyTeamsUserPathRuntime(resolvedCodexRuntime{
		Command:     result.Path,
		Environment: []string{"PATH=" + servicePath},
	}, userpath.Result{Path: servicePath, Mode: userpath.ModeService}, io.Discard)
	rgPath, err := lookPathInEnvironment("rg", runtimeContract.Environment)
	if err != nil {
		t.Fatalf("service-mode Codex runtime did not expose bundled rg: %v; command=%s wrapper=%s vendor=%s PATH=%s", err, runtimeContract.Command, runtimeContract.WrapperCommand, runtimeContract.VendorPathDir, envValue(runtimeContract.Environment, "PATH"))
	}
	rgProbe := exec.Command(rgPath, "--version")
	rgProbe.Env = runtimeContract.Environment
	if output, err := rgProbe.CombinedOutput(); err != nil {
		t.Fatalf("bundled rg is not functional: %v: %s", err, output)
	}
}

func TestApplyTeamsUserPathRuntimeUsesNativeBinaryWithoutManagedNodePATH(t *testing.T) {
	previous := teamsFindNativeCodex
	previousRoot := teamsCodexPackageRoot
	teamsFindNativeCodex = func(wrapper string) (string, string, error) {
		if wrapper != "/managed/codex" {
			t.Fatalf("wrapper = %q", wrapper)
		}
		return "/managed/vendor/codex", "/managed/vendor/codex-path", nil
	}
	teamsCodexPackageRoot = func(string) (string, error) { return "/managed/package", nil }
	t.Cleanup(func() {
		teamsFindNativeCodex = previous
		teamsCodexPackageRoot = previousRoot
	})

	got := applyTeamsUserPathRuntime(resolvedCodexRuntime{
		Command:     "/managed/codex",
		Environment: []string{"PATH=/managed/node/bin:/service/bin", "HOME=/home/alice", "CODEX_NODE_INSTALL_ROOT=/managed/node", "CODEX_NPM_PREFIX=/managed/npm", "NPM_CONFIG_CACHE=/managed/cache", "npm_config_cache=/managed/lower-cache"},
	}, userpath.Result{Path: "/home/alice/.local/bin:/usr/bin"}, io.Discard)
	if got.Command != "/managed/vendor/codex" || got.WrapperCommand != "/managed/codex" {
		t.Fatalf("runtime = %#v", got)
	}
	wantPath := "/managed/vendor/codex-path" + string(os.PathListSeparator) + "/home/alice/.local/bin:/usr/bin"
	if pathValue := envValue(got.Environment, "PATH"); pathValue != wantPath || strings.Contains(pathValue, "/managed/node/bin") {
		t.Fatalf("user PATH = %q, want %q", pathValue, wantPath)
	}
	if envValue(got.Environment, "CODEX_MANAGED_BY_NPM") != "1" {
		t.Fatalf("native runtime lost npm management marker: %#v", got.Environment)
	}
	if envValue(got.Environment, "CODEX_MANAGED_PACKAGE_ROOT") != "/managed/package" {
		t.Fatalf("native runtime package root = %q", envValue(got.Environment, "CODEX_MANAGED_PACKAGE_ROOT"))
	}
	for _, key := range []string{"CODEX_NODE_INSTALL_ROOT", "CODEX_NPM_PREFIX", "NPM_CONFIG_CACHE", "npm_config_cache"} {
		if _, ok := sliceEnvValue(got.Environment, key); ok {
			t.Fatalf("launcher-only %s leaked into native runtime: %#v", key, got.Environment)
		}
	}
}

func TestApplyTeamsUserPathRuntimeServiceModePreservesLegacyWrapper(t *testing.T) {
	previous := teamsFindNativeCodex
	teamsFindNativeCodex = func(wrapper string) (string, string, error) {
		if wrapper != "/managed/codex" {
			t.Fatalf("wrapper = %q", wrapper)
		}
		return "/managed/vendor/codex", "/managed/vendor/codex-path", nil
	}
	t.Cleanup(func() { teamsFindNativeCodex = previous })
	want := resolvedCodexRuntime{
		Command: "/managed/codex",
		Environment: []string{
			"PATH=/managed/node/bin:/service/bin",
			"CODEX_NODE_INSTALL_ROOT=/managed/node",
		},
	}
	got := applyTeamsUserPathRuntime(want, userpath.Result{
		Path: "/service/bin",
		Mode: userpath.ModeService,
	}, io.Discard)
	if got.Command != want.Command || got.WrapperCommand != want.Command {
		t.Fatalf("service compatibility runtime bypassed wrapper: %#v", got)
	}
	wantPath := "/managed/vendor/codex-path" + string(os.PathListSeparator) + "/managed/node/bin:/service/bin"
	if path := envValue(got.Environment, "PATH"); path != wantPath {
		t.Fatalf("service compatibility PATH = %q, want bundled tool path %q", path, wantPath)
	}
	if got.VendorPathDir != "/managed/vendor/codex-path" || envValue(got.Environment, "CODEX_NODE_INSTALL_ROOT") != "/managed/node" {
		t.Fatalf("service compatibility runtime = %#v", got)
	}
}

func TestApplyTeamsUserPathRuntimeNativeOverrideGetsUserPATH(t *testing.T) {
	if os.PathSeparator != '/' {
		t.Skip("ELF fixture")
	}
	previous := teamsFindNativeCodex
	previousRoot := teamsCodexPackageRoot
	teamsFindNativeCodex = func(string) (string, string, error) { return "", "", os.ErrNotExist }
	t.Cleanup(func() {
		teamsFindNativeCodex = previous
		teamsCodexPackageRoot = previousRoot
	})
	native := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(native, []byte("\x7fELFpayload"), 0o700); err != nil {
		t.Fatal(err)
	}
	got := applyTeamsUserPathRuntime(resolvedCodexRuntime{
		Command: native, Environment: []string{"PATH=/managed/node/bin"},
	}, userpath.Result{Path: "/user/bin:/usr/bin"}, io.Discard)
	if pathValue := envValue(got.Environment, "PATH"); pathValue != "/user/bin:/usr/bin" {
		t.Fatalf("PATH = %q", pathValue)
	}
}

func TestCommandLooksNativeRecognizesAllMachOHeaders(t *testing.T) {
	for name, header := range map[string][]byte{
		"thin-32-big":    {0xfe, 0xed, 0xfa, 0xce},
		"thin-32-little": {0xce, 0xfa, 0xed, 0xfe},
		"thin-64-big":    {0xfe, 0xed, 0xfa, 0xcf},
		"thin-64-little": {0xcf, 0xfa, 0xed, 0xfe},
		"fat-32-big":     {0xca, 0xfe, 0xba, 0xbe},
		"fat-32-little":  {0xbe, 0xba, 0xfe, 0xca},
		"fat-64-big":     {0xca, 0xfe, 0xba, 0xbf},
		"fat-64-little":  {0xbf, 0xba, 0xfe, 0xca},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "codex")
			if err := os.WriteFile(path, append(header, []byte("payload")...), 0o700); err != nil {
				t.Fatal(err)
			}
			if !commandLooksNative(path) {
				t.Fatalf("Mach-O header %x was not recognized", header)
			}
		})
	}
}

func TestTeamsAppServerReceivesUserPathInsteadOfServiceOrManagedRuntime(t *testing.T) {
	if os.PathSeparator != '/' {
		t.Skip("POSIX app-server fixture")
	}
	lockCLITestHooks(t)
	previousResolver := teamsUserPathResolver
	teamsUserPathResolver = cliTestUserPathResolverFunc(func(context.Context, userpath.Request) (userpath.Result, error) {
		return userpath.Result{Path: "/user/bin:/usr/bin:/bin", Mode: userpath.ModeAccountDefault, Source: "test-account"}, nil
	})
	previousNative := teamsFindNativeCodex
	previousRoot := teamsCodexPackageRoot
	t.Cleanup(func() {
		teamsUserPathResolver = previousResolver
		teamsFindNativeCodex = previousNative
		teamsCodexPackageRoot = previousRoot
	})

	rootDir := t.TempDir()
	setTestCodexHomeEnv(t, filepath.Join(rootDir, "codex-home"))
	pathFile := filepath.Join(rootDir, "appserver.path")
	codexPath := filepath.Join(rootDir, "codex")
	script := `#!/bin/sh
case "${1:-}" in
  --version) echo 'codex-cli 0.142.3'; exit 0 ;;
  --help) echo 'Options: --remote <ADDR> --remote-auth-token-env <ENV_VAR>'; exit 0 ;;
  app-server)
    printf '%s' "$PATH" > ` + shellSingleQuoteForBeaconCLITest(pathFile) + `
    while IFS= read -r line; do
      id=$(printf %s "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
      case "$line" in
        *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
        *'"method":"thread/list"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"data":[]}}\n' "$id" ;;
        *'"method":"thread/start"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-user-path"}}}\n' "$id" ;;
        *'"method":"thread/read"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"thread-user-path","name":"path"}}}\n' "$id" ;;
        *'"method":"turn/start"'*)
          printf '{"jsonrpc":"2.0","id":%s,"result":{"turn":{"id":"turn-user-path","status":"inProgress","items":[]}}}\n' "$id"
          printf '%s\n' '{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-user-path","turnId":"turn-user-path","item":{"id":"final","type":"agentMessage","text":"ok"}}}'
          printf '%s\n' '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-user-path","turn":{"id":"turn-user-path","status":"completed","items":[]}}}' ;;
      esac
    done ;;
  *) exit 64 ;;
esac
`
	if err := os.WriteFile(codexPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	vendorPath := filepath.Join(rootDir, "vendor", "codex-path")
	teamsFindNativeCodex = func(wrapper string) (string, string, error) {
		return wrapper, vendorPath, nil
	}
	teamsCodexPackageRoot = func(string) (string, error) { return filepath.Join(rootDir, "package"), nil }
	t.Setenv("PATH", "/service/only")
	store, err := config.NewStore(filepath.Join(rootDir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, RuntimeGeneration: currentRuntimeGeneration}); err != nil {
		t.Fatal(err)
	}
	executor, err := newManagedTeamsCodexExecutor(&rootOptions{configPath: store.Path()}, "appserver", codexPath, rootDir, nil, "", 5*time.Second, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	managed := executor.(teamsCodexExecutor)
	defer managed.Close()
	result, err := managed.Run(context.Background(), &teams.Session{Cwd: rootDir}, "check path")
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "ok" {
		t.Fatalf("result = %#v", result)
	}
	raw, err := os.ReadFile(pathFile)
	if err != nil {
		t.Fatal(err)
	}
	want := vendorPath + string(os.PathListSeparator) + "/user/bin:/usr/bin:/bin"
	if got := string(raw); got != want || strings.Contains(got, "/service/only") || strings.Contains(got, "codex-proxy/node") {
		t.Fatalf("app-server PATH = %q, want %q", got, want)
	}
}
