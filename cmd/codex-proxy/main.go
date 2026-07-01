package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/candidateupdate"
	"github.com/baaaaaaaka/codex-helper/internal/cli"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

var (
	runLegacyUpdaterVersionPreflight = cli.LegacyUpdaterVersionPreflight
	launchHelperRuntime              = helperruntime.Launch
	executeCLI                       = cli.Execute
)

func main() {
	os.Exit(runMain(os.Args))
}

func runMain(args []string) int {
	if len(args) == 2 && args[1] == "--recover-previous" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		version, err := helperruntime.RecoverPrevious(ctx)
		cancel()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error: recover previous cxp runtime: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintf(os.Stdout, "Recovered previous cxp runtime %s. Restart the helper to use it.\n", version)
		return 0
	}
	if exitCode, handled := candidateupdate.HandleInternalCommand(args, cli.RuntimeVersion(), os.Stdout, os.Stderr); handled {
		return exitCode
	}
	if err := candidateupdate.LegacyVersionBridge(args, cli.RuntimeVersion()); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: legacy runtime update bridge failed: %v\n", err)
		return 1
	}
	// This compatibility check intentionally precedes immutable-runtime
	// dispatch. v0.1.12 validates downloaded candidates without setting
	// CXP_RUNTIME_DISABLE, and dispatching first would hide the candidate's
	// temporary path and bypass the repair entirely.
	if err := runLegacyUpdaterVersionPreflight(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: helper update compatibility check failed: %v\n", err)
		return 1
	}
	if exitCode, handled, err := launchHelperRuntime(cli.RuntimeVersion(), args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Warning: cxp runtime activation failed; continuing with the current executable: %v\n", err)
	} else if handled {
		return exitCode
	}
	if _, ok := helperruntime.Current(); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := candidateupdate.ResumePending(ctx, cli.RuntimeVersion())
		cancel()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Warning: pending cxp update recovery failed; continuing with the active runtime: %v\n", err)
		}
	}
	return executeCLI()
}
