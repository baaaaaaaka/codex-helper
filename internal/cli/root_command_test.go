package cli

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestRootCommandWiresExpectedSubcommandsAndFlags(t *testing.T) {
	cmd := newRootCmd()

	var names []string
	for _, sub := range cmd.Commands() {
		names = append(names, sub.Name())
	}
	sort.Strings(names)

	want := []string{"__internal-npm-wrapper", "history", "init", "proxy", "run", "tui", "upgrade"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("unexpected root subcommands\n got: %#v\nwant: %#v", names, want)
	}
	if cmd.PersistentFlags().Lookup("config") == nil {
		t.Fatal("expected persistent --config flag")
	}
	if cmd.Flags().Lookup("upgrade-codex") == nil {
		t.Fatal("expected --upgrade-codex flag")
	}
}

func TestInternalNpmWrapperCommandPassesArgsThroughRoot(t *testing.T) {
	lockCLITestHooks(t)

	prevExit := internalNpmWrapperExit
	prevRun := codexSelfUpdateRunRealNpm
	prevCleanup := codexSelfUpdateCleanupStale
	t.Cleanup(func() {
		internalNpmWrapperExit = prevExit
		codexSelfUpdateRunRealNpm = prevRun
		codexSelfUpdateCleanupStale = prevCleanup
	})

	t.Setenv(envCodexProxyRealNPM, "/usr/bin/npm")
	t.Setenv(envCodexProxyOriginalPath, "/usr/bin:/bin")

	var exitCode int
	internalNpmWrapperExit = func(code int) {
		exitCode = code
	}
	var gotArgs []string
	codexSelfUpdateRunRealNpm = func(_ context.Context, npmPath string, args []string, env []string) error {
		if npmPath != "/usr/bin/npm" {
			t.Fatalf("expected real npm path, got %q", npmPath)
		}
		gotArgs = append([]string{}, args...)
		if got := envValue(env, "PATH"); got != "/usr/bin:/bin" {
			t.Fatalf("expected sanitized PATH, got %q", got)
		}
		return nil
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"__internal-npm-wrapper", "install", "--", "-g", "lodash"})
	var errOut bytes.Buffer
	cmd.SetErr(&errOut)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute internal wrapper: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exit code 0, got %d, stderr=%q", exitCode, errOut.String())
	}
	if strings.Join(gotArgs, " ") != "install -- -g lodash" {
		t.Fatalf("unexpected forwarded args: %#v", gotArgs)
	}
}

func TestInternalNpmWrapperCommandReportsFailureExit(t *testing.T) {
	lockCLITestHooks(t)

	prevExit := internalNpmWrapperExit
	t.Cleanup(func() { internalNpmWrapperExit = prevExit })

	var exitCode int
	internalNpmWrapperExit = func(code int) {
		exitCode = code
	}
	t.Setenv(envCodexProxyRealNPM, "")

	cmd := newRootCmd()
	cmd.SetArgs([]string{"__internal-npm-wrapper", "install", "-g", "@openai/codex"})
	var errOut bytes.Buffer
	cmd.SetErr(&errOut)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute internal wrapper: %v", err)
	}
	if exitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", exitCode)
	}
	if !strings.Contains(errOut.String(), "missing real npm path") {
		t.Fatalf("expected missing npm message, got %q", errOut.String())
	}
}
