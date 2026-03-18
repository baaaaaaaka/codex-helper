package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func sliceEnvValue(env []string, key string) (string, bool) {
	for i := len(env) - 1; i >= 0; i-- {
		k, v, ok := strings.Cut(env[i], "=")
		if ok && envKeyEqual(k, key) {
			return v, true
		}
	}
	return "", false
}

func TestPrepareCodexSelfUpdateGuardEnvPrependsWrapper(t *testing.T) {
	lockCLITestHooks(t)

	prevDetect := codexSelfUpdateDetectSource
	prevLookPath := codexSelfUpdateLookPath
	prevExecutable := codexSelfUpdateExecutable
	t.Cleanup(func() {
		codexSelfUpdateDetectSource = prevDetect
		codexSelfUpdateLookPath = prevLookPath
		codexSelfUpdateExecutable = prevExecutable
	})

	codexSelfUpdateDetectSource = func(context.Context, string, []string) (codexUpgradeSource, error) {
		return codexUpgradeSource{
			origin:    codexInstallOriginSystem,
			codexPath: "/tmp/codex",
			npmPrefix: "/tmp/npm-global",
		}, nil
	}
	privateNpmPath := "/tmp/codex-proxy/node/v22-linux-x64/bin/npm"
	codexSelfUpdateLookPath = func(string) (string, error) {
		return privateNpmPath, nil
	}
	codexSelfUpdateExecutable = func() (string, error) {
		return "/usr/bin/codex-proxy", nil
	}

	updated, cleanup, err := prepareCodexSelfUpdateGuardEnv(
		context.Background(),
		"/tmp/codex",
		[]string{"PATH=/usr/bin:/bin", "FOO=bar"},
	)
	if err != nil {
		t.Fatalf("prepare guard env: %v", err)
	}
	t.Cleanup(cleanup)

	if got := envValue(updated, envCodexProxyRealNPM); got != privateNpmPath {
		t.Fatalf("expected real npm path, got %q", got)
	}
	if got := envValue(updated, envCodexProxyUpdateNPMPrefix); got != "/tmp/npm-global" {
		t.Fatalf("expected npm prefix, got %q", got)
	}

	pathParts := filepath.SplitList(envValue(updated, "PATH"))
	if len(pathParts) < 2 {
		t.Fatalf("expected wrapper PATH prefix, got %q", envValue(updated, "PATH"))
	}
	wrapperDir := pathParts[0]
	if !strings.HasPrefix(filepath.Base(wrapperDir), "codex-proxy-npm-") {
		t.Fatalf("expected temp wrapper dir, got %q", wrapperDir)
	}

	wrapperName := "npm"
	if runtime.GOOS == "windows" {
		wrapperName = "npm.cmd"
	}
	if _, err := os.Stat(filepath.Join(wrapperDir, wrapperName)); err != nil {
		t.Fatalf("wrapper missing: %v", err)
	}

	cleanup()
	if _, err := os.Stat(wrapperDir); !os.IsNotExist(err) {
		t.Fatalf("expected wrapper dir removed, got %v", err)
	}
}

func TestPrepareCodexSelfUpdateGuardEnvSkipsUnknownSource(t *testing.T) {
	lockCLITestHooks(t)

	prevDetect := codexSelfUpdateDetectSource
	t.Cleanup(func() { codexSelfUpdateDetectSource = prevDetect })

	codexSelfUpdateDetectSource = func(context.Context, string, []string) (codexUpgradeSource, error) {
		return codexUpgradeSource{origin: codexInstallOriginUnknown}, nil
	}

	input := []string{"PATH=/usr/bin:/bin"}
	updated, cleanup, err := prepareCodexSelfUpdateGuardEnv(context.Background(), "/tmp/codex", input)
	if err != nil {
		t.Fatalf("prepare guard env: %v", err)
	}
	t.Cleanup(cleanup)

	if len(updated) != len(input) || updated[0] != input[0] {
		t.Fatalf("expected unchanged env, got %#v", updated)
	}
}

func TestIsNpmGlobalCodexInstallArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "global long flag", args: []string{"install", "--global", "@openai/codex"}, want: true},
		{name: "global location equals", args: []string{"install", "--location=global", "@openai/codex"}, want: true},
		{name: "global location separate", args: []string{"install", "--location", "global", "@openai/codex"}, want: true},
		{name: "global location before command", args: []string{"--location", "global", "install", "@openai/codex"}, want: true},
		{name: "global latest tag", args: []string{"install", "--global", "@openai/codex@latest"}, want: true},
		{name: "global pinned version", args: []string{"install", "--global", "@openai/codex@0.115.0"}, want: true},
		{name: "global short flag first", args: []string{"-g", "install", "@openai/codex"}, want: true},
		{name: "global short flag last", args: []string{"install", "@openai/codex", "-g"}, want: true},
		{name: "alias command", args: []string{"i", "-g", "@openai/codex"}, want: true},
		{name: "update command", args: []string{"update", "-g", "@openai/codex"}, want: true},
		{name: "update alias command", args: []string{"up", "-g", "@openai/codex"}, want: true},
		{name: "missing global", args: []string{"install", "@openai/codex"}, want: false},
		{name: "non-global location", args: []string{"install", "--location=project", "@openai/codex"}, want: false},
		{name: "wrong package", args: []string{"install", "-g", "lodash"}, want: false},
		{name: "wrong scoped package", args: []string{"install", "-g", "@openai/codex-linux-x64"}, want: false},
		{name: "wrong command", args: []string{"remove", "-g", "@openai/codex"}, want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isNpmGlobalCodexInstallArgs(tc.args); got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestSanitizeCodexSelfUpdateEnvRestoresPath(t *testing.T) {
	t.Parallel()

	env := []string{
		"PATH=/tmp/wrapper:/usr/bin:/bin",
		"npm_config_prefix=/tmp/inherited",
		"NPM_CONFIG_PREFIX=/tmp/inherited-uppercase",
		envCodexProxyOriginalPath + "=/usr/bin:/bin",
		envCodexProxyRealNPM + "=/usr/bin/npm",
		envCodexProxyUpdateOrigin + "=system-npm",
		envCodexProxyUpdateCodexPath + "=/tmp/codex",
		envCodexProxyUpdateNPMPrefix + "=/tmp/prefix",
		envCodexProxyWrapperExe + "=/tmp/codex-proxy",
		"KEEP=1",
	}
	sanitized := sanitizeCodexSelfUpdateEnv(env)

	if got := envValue(sanitized, "PATH"); got != "/usr/bin:/bin" {
		t.Fatalf("expected restored PATH, got %q", got)
	}
	if got := envValue(sanitized, "KEEP"); got != "1" {
		t.Fatalf("expected KEEP env preserved, got %q", got)
	}
	if got, ok := sliceEnvValue(sanitized, "npm_config_prefix"); ok {
		t.Fatalf("expected inherited npm_config_prefix removed, got %q", got)
	}
	if got, ok := sliceEnvValue(sanitized, "NPM_CONFIG_PREFIX"); ok {
		t.Fatalf("expected inherited NPM_CONFIG_PREFIX removed, got %q", got)
	}
	for _, key := range []string{
		envCodexProxyOriginalPath,
		envCodexProxyRealNPM,
		envCodexProxyUpdateOrigin,
		envCodexProxyUpdateCodexPath,
		envCodexProxyUpdateNPMPrefix,
		envCodexProxyWrapperExe,
	} {
		if got := envValue(sanitized, key); got != "" {
			t.Fatalf("expected %s removed, got %q", key, got)
		}
	}
}

func TestRunInternalNpmWrapperCleansBeforeGlobalInstall(t *testing.T) {
	lockCLITestHooks(t)

	prevCleanup := codexSelfUpdateCleanupStale
	prevRun := codexSelfUpdateRunRealNpm
	t.Cleanup(func() {
		codexSelfUpdateCleanupStale = prevCleanup
		codexSelfUpdateRunRealNpm = prevRun
	})

	t.Setenv(envCodexProxyRealNPM, "/usr/bin/npm")
	t.Setenv(envCodexProxyOriginalPath, "/usr/bin:/bin")
	t.Setenv(envCodexProxyUpdateOrigin, string(codexInstallOriginSystem))
	t.Setenv(envCodexProxyUpdateCodexPath, "/tmp/codex")
	t.Setenv(envCodexProxyUpdateNPMPrefix, "/tmp/prefix")
	t.Setenv(envNpmConfigPrefix, "/tmp/inherited-prefix")

	cleaned := false
	codexSelfUpdateCleanupStale = func(_ io.Writer, source codexUpgradeSource) error {
		cleaned = true
		if source.codexPath != "/tmp/codex" || source.npmPrefix != "/tmp/prefix" {
			t.Fatalf("unexpected cleanup source: %+v", source)
		}
		return nil
	}

	var ranPath string
	var ranArgs []string
	var ranEnv []string
	codexSelfUpdateRunRealNpm = func(_ context.Context, npmPath string, args []string, env []string) error {
		ranPath = npmPath
		ranArgs = append([]string{}, args...)
		ranEnv = append([]string{}, env...)
		return nil
	}

	code := runInternalNpmWrapper(context.Background(), []string{"install", "--global", "@openai/codex"}, io.Discard)
	if code != 0 {
		t.Fatalf("expected success, got %d", code)
	}
	if !cleaned {
		t.Fatal("expected stale cleanup to run")
	}
	if ranPath != "/usr/bin/npm" {
		t.Fatalf("expected real npm path, got %q", ranPath)
	}
	if strings.Join(ranArgs, " ") != "install --global @openai/codex" {
		t.Fatalf("unexpected args: %#v", ranArgs)
	}
	if got := envValue(ranEnv, "PATH"); got != "/usr/bin:/bin" {
		t.Fatalf("expected restored PATH, got %q", got)
	}
	if got, ok := sliceEnvValue(ranEnv, "npm_config_prefix"); ok {
		t.Fatalf("expected system npm env to avoid prefix override, got %q", got)
	}
	if got, ok := sliceEnvValue(ranEnv, envCodexProxyRealNPM); ok {
		t.Fatalf("expected wrapper env removed, got %q", got)
	}
}

func TestRunInternalNpmWrapperManagedInstallForcesManagedPrefixWithPrivateNpm(t *testing.T) {
	lockCLITestHooks(t)

	prevCleanup := codexSelfUpdateCleanupStale
	prevRun := codexSelfUpdateRunRealNpm
	t.Cleanup(func() {
		codexSelfUpdateCleanupStale = prevCleanup
		codexSelfUpdateRunRealNpm = prevRun
	})

	privateNpmPath := "/tmp/codex-proxy/node/v22-linux-x64/bin/npm"
	t.Setenv(envCodexProxyRealNPM, privateNpmPath)
	t.Setenv(envCodexProxyOriginalPath, "/usr/bin:/bin")
	t.Setenv(envCodexProxyUpdateOrigin, string(codexInstallOriginManaged))
	t.Setenv(envCodexProxyUpdateCodexPath, "/tmp/managed/bin/codex")
	t.Setenv(envCodexProxyUpdateNPMPrefix, "/tmp/managed")

	cleaned := false
	codexSelfUpdateCleanupStale = func(_ io.Writer, source codexUpgradeSource) error {
		cleaned = true
		if source.origin != codexInstallOriginManaged || source.npmPrefix != "/tmp/managed" {
			t.Fatalf("unexpected cleanup source: %+v", source)
		}
		return nil
	}

	var ranEnv []string
	codexSelfUpdateRunRealNpm = func(_ context.Context, npmPath string, args []string, env []string) error {
		if npmPath != privateNpmPath {
			t.Fatalf("expected real npm path, got %q", npmPath)
		}
		if strings.Join(args, " ") != "install --global @openai/codex@latest" {
			t.Fatalf("unexpected args: %#v", args)
		}
		ranEnv = append([]string{}, env...)
		return nil
	}

	code := runInternalNpmWrapper(context.Background(), []string{"install", "--global", "@openai/codex@latest"}, io.Discard)
	if code != 0 {
		t.Fatalf("expected success, got %d", code)
	}
	if !cleaned {
		t.Fatal("expected stale cleanup to run")
	}
	if got := envValue(ranEnv, "PATH"); got != "/usr/bin:/bin" {
		t.Fatalf("expected restored PATH, got %q", got)
	}
	if got := envValue(ranEnv, "npm_config_prefix"); got != "/tmp/managed" {
		t.Fatalf("expected managed npm prefix override, got %q", got)
	}
}

func TestRunInternalNpmWrapperSkipsCleanupForOtherCommands(t *testing.T) {
	lockCLITestHooks(t)

	prevCleanup := codexSelfUpdateCleanupStale
	prevRun := codexSelfUpdateRunRealNpm
	t.Cleanup(func() {
		codexSelfUpdateCleanupStale = prevCleanup
		codexSelfUpdateRunRealNpm = prevRun
	})

	t.Setenv(envCodexProxyRealNPM, "/usr/bin/npm")
	t.Setenv(envCodexProxyOriginalPath, "/usr/bin:/bin")

	cleaned := false
	codexSelfUpdateCleanupStale = func(io.Writer, codexUpgradeSource) error {
		cleaned = true
		return nil
	}
	codexSelfUpdateRunRealNpm = func(context.Context, string, []string, []string) error {
		return nil
	}

	code := runInternalNpmWrapper(context.Background(), []string{"install", "-g", "lodash"}, io.Discard)
	if code != 0 {
		t.Fatalf("expected success, got %d", code)
	}
	if cleaned {
		t.Fatal("expected stale cleanup to be skipped")
	}
}

func TestRunInternalNpmWrapperReturnsFailureOnCleanupError(t *testing.T) {
	lockCLITestHooks(t)

	prevCleanup := codexSelfUpdateCleanupStale
	prevRun := codexSelfUpdateRunRealNpm
	t.Cleanup(func() {
		codexSelfUpdateCleanupStale = prevCleanup
		codexSelfUpdateRunRealNpm = prevRun
	})

	t.Setenv(envCodexProxyRealNPM, "/usr/bin/npm")
	t.Setenv(envCodexProxyOriginalPath, "/usr/bin:/bin")
	t.Setenv(envCodexProxyUpdateOrigin, string(codexInstallOriginSystem))

	ran := false
	codexSelfUpdateCleanupStale = func(io.Writer, codexUpgradeSource) error {
		return errors.New("boom")
	}
	codexSelfUpdateRunRealNpm = func(context.Context, string, []string, []string) error {
		ran = true
		return nil
	}

	code := runInternalNpmWrapper(context.Background(), []string{"install", "-g", "@openai/codex"}, io.Discard)
	if code != 1 {
		t.Fatalf("expected failure exit code, got %d", code)
	}
	if ran {
		t.Fatal("expected npm execution skipped after cleanup failure")
	}
}

func TestRunInternalNpmWrapperManagedInstallRequiresPrefix(t *testing.T) {
	lockCLITestHooks(t)

	prevCleanup := codexSelfUpdateCleanupStale
	prevRun := codexSelfUpdateRunRealNpm
	t.Cleanup(func() {
		codexSelfUpdateCleanupStale = prevCleanup
		codexSelfUpdateRunRealNpm = prevRun
	})

	t.Setenv(envCodexProxyRealNPM, "/usr/bin/npm")
	t.Setenv(envCodexProxyOriginalPath, "/usr/bin:/bin")
	t.Setenv(envCodexProxyUpdateOrigin, string(codexInstallOriginManaged))
	t.Setenv(envCodexProxyUpdateCodexPath, "/tmp/managed/bin/codex")

	cleaned := false
	codexSelfUpdateCleanupStale = func(_ io.Writer, source codexUpgradeSource) error {
		cleaned = true
		if source.origin != codexInstallOriginManaged {
			t.Fatalf("unexpected cleanup source: %+v", source)
		}
		return nil
	}

	ran := false
	codexSelfUpdateRunRealNpm = func(context.Context, string, []string, []string) error {
		ran = true
		return nil
	}

	code := runInternalNpmWrapper(context.Background(), []string{"install", "-g", "@openai/codex"}, io.Discard)
	if code != 1 {
		t.Fatalf("expected failure exit code, got %d", code)
	}
	if !cleaned {
		t.Fatal("expected cleanup to run before prefix validation failure")
	}
	if ran {
		t.Fatal("expected npm execution skipped when managed prefix is missing")
	}
}
