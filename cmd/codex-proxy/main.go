package main

import (
	"fmt"
	"os"

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
	return executeCLI()
}
