package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/candidateupdate"
	"github.com/baaaaaaaka/codex-helper/internal/cli"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
	"github.com/baaaaaaaka/codex-helper/internal/updatestate"
)

var (
	launchHelperRuntime = helperruntime.Launch
	executeCLI          = cli.Execute
)

func main() {
	os.Exit(runMain(os.Args))
}

func runMain(args []string) int {
	if code, handled := helperruntime.HandleBootstrapInstall(args, cli.RuntimeVersion(), os.Stderr); handled {
		return code
	}
	if code, handled := updatestate.HandleActivationCommand(args, os.Stderr); handled {
		return code
	}
	if code, handled := managedinstall.HandleInstallerCommand(args, os.Stdout, os.Stderr); handled {
		return code
	}
	if code, handled := helperruntime.HandleProtocolProbe(args, os.Stdout); handled {
		return code
	}
	if exitCode, handled := cli.HandleWindowsAppInstallWorker(args, os.Stdin, os.Stdout, os.Stderr); handled {
		return exitCode
	}
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
	if code, handled := candidateupdate.HandleUpgradeRepair(args, os.Stdout, os.Stderr); handled {
		return code
	}
	if code, handled := candidateupdate.HandleLegacyMigration(args, cli.RuntimeVersion(), os.Stdout, os.Stderr); handled {
		return code
	}
	if exitCode, handled, err := launchHelperRuntime(cli.RuntimeVersion(), args); err != nil {
		if errors.Is(err, helperruntime.ErrBootstrapState) {
			fmt.Fprintf(os.Stderr, "Error: runtime startup: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintf(os.Stderr, "Warning: cxp runtime activation failed; continuing with the current executable: %v\n", err)
	} else if handled {

		return exitCode
	}
	if err := helperruntime.PrepareBoot(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: runtime startup: %v\n", err)
		return 1
	}
	if _, ok := helperruntime.Current(); ok && os.Getenv(helperruntime.EnvBootManaged) != "1" {
		err := candidateupdate.CheckNativeBootstrap()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error: cxp runtime admission failed: %v\n", err)
			return 1
		}
	}
	return executeCLI()
}
