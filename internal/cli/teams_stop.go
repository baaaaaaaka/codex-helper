package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// One concept, "stopped everywhere until started" (owner ruling
// 2026-11-05): `cxp teams stop` (or `shutdown` in the control chat)
// creates the kill-switch file DISABLED next to the store, recording who
// stopped the helper, where and when. The owner stops at its next
// heartbeat; standbys and services on every machine sharing the directory
// stay idle (no Graph traffic) until `cxp teams start` removes the file.
// `cxp teams service stop` stops this machine's service only.

// teamsStopWait bounds how long `cxp teams stop` waits for the running
// helper to stop: it notices the file at its next heartbeat (30 s).
var teamsStopWait = 75 * time.Second

// teamsStopDir is the directory of the store (and of its kill-switch
// file), created if missing so a helper never started here stays stopped.
func teamsStopDir() (anchor string, err error) {
	stateDir, err := appdirs.StateDir()
	if err != nil {
		return "", fmt.Errorf("state directory: %w", err)
	}
	anchor, _, err = planTeamsCoreStore(stateDir)
	if err != nil {
		return "", err
	}
	return anchor, os.MkdirAll(filepath.Dir(anchor), 0o700)
}

// teamsStopState is the stop record when the helper is stopped everywhere
// (ok false otherwise), and its one-line description.
func teamsStopState(anchor string) (desc string, ok bool) {
	dir := filepath.Dir(anchor)
	rec, ok, err := corestore.ReadStop(dir)
	if err != nil || !ok {
		return "", false
	}
	return rec.Describe(corestore.DisabledPath(dir)), true
}

// teamsStoppedLine is the status line of a helper stopped everywhere, or
// "" when it is not stopped (or the store cannot be found).
func teamsStoppedLine() string {
	stateDir, err := appdirs.StateDir()
	if err != nil {
		return ""
	}
	anchor, _, err := planTeamsCoreStore(stateDir)
	if err != nil {
		return ""
	}
	desc, _ := teamsStopState(anchor)
	return desc
}

// errTeamsStoppedEverywhere refuses a service start or restart while the
// helper is stopped on all machines: the service would only stand by.
func errTeamsStoppedEverywhere(action string) error {
	desc := teamsStoppedLine()
	if desc == "" {
		return nil
	}
	return fmt.Errorf("%s\nThe Teams service was not %s: it would only stand by until then", desc, action)
}

func newTeamsStopCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stop the Teams helper on all machines until `cxp teams start`",
		Long: "Stop the Teams helper on all machines that share this state directory, until `cxp teams start`.\n\n" +
			"It creates the kill-switch file DISABLED next to the helper's store. The running helper stops at its next " +
			"heartbeat (within 30 s); helpers standing by and services on every machine stay idle and do not read Teams. " +
			"Messages sent meanwhile are read when the helper starts again. Running it again changes nothing.\n\n" +
			"`cxp teams service stop` stops only this machine's service: a helper on another machine may then take over.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("stop the Teams helper", "shutdown"); err != nil {
				return err
			}
			return runTeamsStop(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

// newTeamsPauseCmd and newTeamsResumeCmd keep v0.1.20's `cxp teams pause
// [reason]` and `cxp teams resume` as hidden aliases of `cxp teams stop`
// and `cxp teams start` (F-TRN-17 / R-40, coordinator ruling): there is
// one stop concept. The reason v0.1.20 recorded is accepted and not kept;
// the stop record says who stopped the helper, where and when.
func newTeamsPauseCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "pause [reason]",
		Short:  "Alias of `cxp teams stop`",
		Hidden: true,
		Args:   cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectTeamsHelperSelfManagementFromChild("stop the Teams helper", "shutdown"); err != nil {
				return err
			}
			return runTeamsStop(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

func newTeamsResumeCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "resume",
		Short:  "Alias of `cxp teams start`",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTeamsStart(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

func runTeamsStop(ctx context.Context, out io.Writer) error {
	anchor, err := teamsStopDir()
	if err != nil {
		return err
	}
	dir := filepath.Dir(anchor)
	host, _ := teamsHostname()
	created, err := corestore.WriteStop(dir, corestore.StopRecord{Source: corestore.StopByCLI, Host: host, At: time.Now()})
	if err != nil {
		return fmt.Errorf("stop the Teams helper: %w", err)
	}
	desc, _ := teamsStopState(anchor)
	if !created {
		_, _ = fmt.Fprintf(out, "Already stopped; nothing changed.\n%s\n", desc)
		return nil
	}
	_, _ = fmt.Fprintf(out, "Stopping the Teams helper on all machines (created %s).\n", corestore.DisabledPath(dir))
	_, _ = fmt.Fprintln(out, teamsStopOwnerOutcome(ctx, anchor, out))
	_, _ = fmt.Fprintln(out, "Helpers standing by and services on every machine stay idle and do not read Teams. Messages sent meanwhile are read when the helper starts again.")
	_, _ = fmt.Fprintln(out, "To start it again: cxp teams start")
	return nil
}

// teamsStopOwnerOutcome waits (bounded) for the helper that holds the
// store to stop, and says how that went.
func teamsStopOwnerOutcome(ctx context.Context, anchor string, out io.Writer) string {
	in, err := lease.Read(anchor)
	if err != nil || !in.HeadOK || !in.Live(time.Now(), lease.ProductionStale) {
		return "No helper was running."
	}
	who := fmt.Sprintf("%s (pid %d)", firstNonEmptyString(in.Head.Host, "unknown host"), in.Head.PID)
	gen := in.HeadGen
	_, _ = fmt.Fprintf(out, "Waiting for the helper on %s to stop...\n", who)
	deadline := time.Now().Add(teamsStopWait)
	for time.Now().Before(deadline) {
		if err := sleepContext(ctx, 500*time.Millisecond); err != nil {
			break
		}
		in, err := lease.Read(anchor)
		if err != nil {
			continue
		}
		if in.HeadGen != gen || !in.HeadOK || in.Head.State == lease.Released {
			return fmt.Sprintf("The helper on %s stopped.", who)
		}
	}
	return fmt.Sprintf("The helper on %s has not stopped yet; it stops at its next heartbeat. Check with `cxp teams status`.", who)
}

func newTeamsStartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the Teams helper again after `cxp teams stop`",
		Long: "Start the Teams helper again after `cxp teams stop` (or `shutdown` in the control chat): it removes the " +
			"kill-switch file DISABLED next to the helper's store. If the Teams service is installed on this machine and " +
			"not running, it is started; otherwise the command says which machine picks up. Running it again changes nothing.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTeamsStart(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

func runTeamsStart(ctx context.Context, out io.Writer) error {
	anchor, err := teamsStopDir()
	if err != nil {
		return err
	}
	dir := filepath.Dir(anchor)
	desc, _ := teamsStopState(anchor)
	removed, err := corestore.RemoveStop(dir)
	if err != nil {
		return fmt.Errorf("start the Teams helper: %w", err)
	}
	if removed {
		desc = strings.TrimSuffix(strings.TrimSuffix(desc, " Start again with `cxp teams start`."), ".")
		_, _ = fmt.Fprintf(out, "Started the Teams helper again on all machines (it was: %s; removed %s).\n", desc, corestore.DisabledPath(dir))
	} else {
		_, _ = fmt.Fprintln(out, "The Teams helper was not stopped; nothing to remove.")
	}
	_, _ = fmt.Fprintln(out, teamsStartPickup(ctx, anchor, out))
	return nil
}

// teamsStartPickup starts this machine's installed service when it is not
// running, else says which helper picks up or how to run one.
func teamsStartPickup(ctx context.Context, anchor string, out io.Writer) string {
	if installed, err := teamsServiceInstalled(); err == nil && installed {
		active, aerr := teamsServiceActive(ctx)
		if aerr == nil && active {
			return "The Teams service on this machine is running; it resumes within a minute."
		}
		if runningInsideTeamsCodexChild() {
			return "The Teams service on this machine is not running; start it with `cxp teams service start`."
		}
		if err := startTeamsServiceWithOutput(ctx, false, out); err != nil {
			return fmt.Sprintf("The Teams service on this machine is not running and could not be started (%v); start it with `cxp teams service start`, or run `cxp teams run`.", err)
		}
		return "Started the Teams service on this machine."
	}
	in, err := lease.Read(anchor)
	last := ""
	if err == nil && in.HeadOK {
		last = firstNonEmptyString(in.Head.Host, "unknown host")
		if in.Live(time.Now(), lease.ProductionStale) && in.Head.State != lease.Released {
			return fmt.Sprintf("The helper on %s is running; it resumes within a minute.", last)
		}
		last = " (the last one ran on " + last + ")"
	}
	return "No Teams service is installed on this machine. A helper standing by on any machine" + last +
		" resumes within a minute; if none is, run `cxp teams run`, or install the service with `cxp teams service install`."
}
