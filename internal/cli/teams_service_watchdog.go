package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

const (
	teamsServiceWatchdogActionNoop    = "noop"
	teamsServiceWatchdogActionStart   = "start"
	teamsServiceWatchdogActionRestart = "restart"

	defaultTeamsServiceWatchdogOwnerStaleAfter    = corestore.OwnerStaleAfter
	defaultTeamsServiceWatchdogCooldown           = 2 * time.Minute
	defaultTeamsServiceWatchdogConsecutiveStale   = 3
	defaultTeamsServiceWatchdogMissingStateReason = "no Teams state evidence yet"
)

// The watchdog (and the local supervisor's child health check) judge the
// helper only from local evidence: whether the service is installed and
// running, and the owner heartbeat in the Teams store (F-SVC-6). It never
// calls Graph, so a Graph or proxy outage never restarts a helper whose
// heartbeat is fresh. A stale or missing owner restarts the service after
// three consecutive stale checks, at most once per cooldown.
type teamsServiceWatchdogOptions struct {
	Now                 time.Time
	OwnerStaleAfter     time.Duration
	Cooldown            time.Duration
	MinConsecutiveStale int
	DryRun              bool
}

type teamsServiceWatchdogSnapshot struct {
	Installed              bool
	Active                 bool
	StateFiles             int
	AuthoritativeStorePath string
	OwnerFound             bool
	OwnerFresh             bool
	LastOwnerHeartbeat     time.Time
	// ManagedChildStarting is true when the local supervisor has a live
	// child that does not own the store yet (it is starting, or converting
	// an old store at first start).
	ManagedChildStarting  bool
	ManagedChildStartedAt time.Time
	// Disabled is true when the kill-switch file is next to the store: the
	// helper must stay stopped (or stand by), so nothing is started.
	Disabled bool
	// BackoffUntil is set while this host's helper waits before starting
	// again after repeated fast restarts (teams_service_backoff.go).
	BackoffUntil time.Time
}

type teamsServiceWatchdogState struct {
	ConsecutiveStale int       `json:"consecutive_stale,omitempty"`
	LastReason       string    `json:"last_reason,omitempty"`
	LastAction       string    `json:"last_action,omitempty"`
	LastActionAt     time.Time `json:"last_action_at,omitempty"`
	UpdatedAt        time.Time `json:"updated_at,omitempty"`
}

type teamsServiceWatchdogDecision struct {
	Action           string
	Reason           string
	Stale            bool
	ConsecutiveStale int
	CooldownUntil    time.Time
}

type teamsServiceWatchdogResult struct {
	Snapshot teamsServiceWatchdogSnapshot
	Decision teamsServiceWatchdogDecision
	State    teamsServiceWatchdogState
	DryRun   bool
}

type teamsServiceWatchdogManagedChildIdentity struct {
	PID                 int
	StartedAt           time.Time
	ManagedBySupervisor bool
}

var (
	teamsServiceWatchdogNow             = time.Now
	teamsServiceWatchdogStatePath       = defaultTeamsServiceWatchdogStatePath
	teamsServiceWatchdogCollectSnapshot = collectTeamsServiceWatchdogSnapshot
	teamsServiceWatchdogStorePath       = defaultTeamsServiceWatchdogStorePath
	teamsServiceWatchdogInstalled       = teamsServiceInstalled
	teamsServiceWatchdogActive          = teamsServiceActive
	teamsServiceWatchdogStartService    = startTeamsPrimaryService
	teamsServiceWatchdogManagedChild    = defaultTeamsServiceWatchdogManagedChild
)

func newTeamsServiceWatchdogCmd() *cobra.Command {
	var dryRun bool
	var loop bool
	var quiet bool
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "watchdog",
		Short: "Run one low-cost Teams service watchdog check",
		Long:  "Run one low-cost local Teams service watchdog check. This command does not call Microsoft Graph; it reads local service and helper state, then starts or restarts the service only when the local evidence is stale.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if loop {
				return runTeamsServiceWatchdogLoop(cmd.Context(), teamsServiceWatchdogOptions{DryRun: dryRun}, interval, quiet, cmd.OutOrStdout(), cmd.ErrOrStderr())
			}
			result, err := runTeamsServiceWatchdogOnce(cmd.Context(), teamsServiceWatchdogOptions{DryRun: dryRun})
			if err != nil {
				return err
			}
			if !quiet {
				printTeamsServiceWatchdogResult(cmd.OutOrStdout(), result)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Evaluate watchdog state without starting or restarting the service")
	cmd.Flags().BoolVar(&loop, "loop", false, "Run watchdog checks continuously until stopped")
	cmd.Flags().DurationVar(&interval, "interval", teamsServiceExternalWatchdogInterval, "Loop interval for --loop")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "Print nothing when the watchdog check succeeds")
	return cmd
}

func runTeamsServiceWatchdogLoop(ctx context.Context, opts teamsServiceWatchdogOptions, interval time.Duration, quiet bool, out io.Writer, errOut io.Writer) error {
	if interval <= 0 {
		interval = teamsServiceExternalWatchdogInterval
	}
	for {
		checkCtx, cancel := context.WithTimeout(ctx, teamsServiceExternalWatchdogCheckTimeout)
		result, err := runTeamsServiceWatchdogOnce(checkCtx, opts)
		cancel()
		if err != nil {
			if errOut != nil {
				_, _ = fmt.Fprintf(errOut, "Teams service watchdog error: %v\n", err)
			}
		} else if !quiet {
			printTeamsServiceWatchdogResult(out, result)
		}
		if err := sleepContext(ctx, interval); err != nil {
			return err
		}
	}
}

func runTeamsServiceWatchdogOnce(ctx context.Context, opts teamsServiceWatchdogOptions) (teamsServiceWatchdogResult, error) {
	opts = normalizeTeamsServiceWatchdogOptions(opts)
	snapshot, err := teamsServiceWatchdogCollectSnapshot(ctx, opts)
	if err != nil {
		return teamsServiceWatchdogResult{}, err
	}
	state, err := loadTeamsServiceWatchdogState()
	if err != nil {
		return teamsServiceWatchdogResult{}, err
	}
	decision := evaluateTeamsServiceWatchdog(snapshot, state, opts)
	next := nextTeamsServiceWatchdogState(state, decision, opts.Now)
	result := teamsServiceWatchdogResult{Snapshot: snapshot, Decision: decision, State: next, DryRun: opts.DryRun}
	if opts.DryRun {
		return result, nil
	}
	if decision.Action == teamsServiceWatchdogActionStart || decision.Action == teamsServiceWatchdogActionRestart {
		pending := teamsServiceWatchdogPendingActionState(next)
		if err := saveTeamsServiceWatchdogState(pending); err != nil {
			return teamsServiceWatchdogResult{}, err
		}
		if err := teamsServiceWatchdogStartService(ctx, decision.Action == teamsServiceWatchdogActionRestart); err != nil {
			result.State = pending
			return result, err
		}
		result.State = next
		if err := saveTeamsServiceWatchdogState(next); err != nil {
			return teamsServiceWatchdogResult{}, err
		}
		return result, nil
	}
	if teamsServiceWatchdogStateSemanticallyEqual(state, next) {
		result.State = state
		return result, nil
	}
	if err := saveTeamsServiceWatchdogState(next); err != nil {
		return teamsServiceWatchdogResult{}, err
	}
	return result, nil
}

func normalizeTeamsServiceWatchdogOptions(opts teamsServiceWatchdogOptions) teamsServiceWatchdogOptions {
	if opts.Now.IsZero() {
		opts.Now = teamsServiceWatchdogNow()
	}
	if opts.OwnerStaleAfter <= 0 {
		opts.OwnerStaleAfter = defaultTeamsServiceWatchdogOwnerStaleAfter
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = defaultTeamsServiceWatchdogCooldown
	}
	if opts.MinConsecutiveStale <= 0 {
		opts.MinConsecutiveStale = defaultTeamsServiceWatchdogConsecutiveStale
	}
	return opts
}

// defaultTeamsServiceWatchdogStorePath is the store the helper runs on
// ("" when there is none yet, or it cannot be determined).
func defaultTeamsServiceWatchdogStorePath() (string, error) {
	stateDir, err := appdirs.StateDir()
	if err != nil {
		return "", err
	}
	path, err := resolveTeamsCoreStorePath(stateDir)
	if err != nil {
		return "", nil // the helper refuses to start and says why
	}
	return path, nil
}

func collectTeamsServiceWatchdogSnapshot(ctx context.Context, opts teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
	var snapshot teamsServiceWatchdogSnapshot
	installed, err := teamsServiceWatchdogInstalled()
	if err != nil {
		return snapshot, err
	}
	snapshot.Installed = installed
	if installed {
		active, err := teamsServiceWatchdogActive(ctx)
		if err != nil {
			return snapshot, err
		}
		snapshot.Active = active
	}
	path, err := teamsServiceWatchdogStorePath()
	if err != nil || path == "" {
		return snapshot, err
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(path), corestore.DisabledFile)); err == nil {
		snapshot.Disabled = true
	}
	if host, err := os.Hostname(); err == nil {
		snapshot.BackoffUntil = readTeamsServiceRestartRecord(teamsServiceRestartRecordPath(filepath.Dir(path), host)).WaitUntil
	}
	if info, err := os.Lstat(path); err != nil || !info.Mode().IsRegular() {
		return snapshot, nil
	}
	snapshot.StateFiles = 1
	snapshot.AuthoritativeStorePath = path
	st, err := corestore.OpenWith(path, corestore.Options{ReadOnly: true})
	if err != nil {
		return snapshot, err
	}
	defer st.Close()
	owner, ok, err := st.OwnerInfo()
	if err != nil {
		return snapshot, err
	}
	if ok && owner.HeartbeatAt > 0 {
		snapshot.OwnerFound = true
		snapshot.LastOwnerHeartbeat = time.UnixMilli(owner.HeartbeatAt)
		age := opts.Now.Sub(snapshot.LastOwnerHeartbeat)
		snapshot.OwnerFresh = age < opts.OwnerStaleAfter
	}
	if child, found := teamsServiceWatchdogManagedChild(); found && child.PID > 0 && !child.StartedAt.IsZero() {
		snapshot.ManagedChildStartedAt = child.StartedAt
		owns := snapshot.OwnerFresh && owner.PID == child.PID && !snapshot.LastOwnerHeartbeat.Before(child.StartedAt)
		snapshot.ManagedChildStarting = !owns
	}
	return snapshot, nil
}

func defaultTeamsServiceWatchdogManagedChild() (teamsServiceWatchdogManagedChildIdentity, bool) {
	status, ok, err := readTeamsServiceLocalSupervisorStatus()
	if err != nil || !ok || status.ChildPID <= 0 || status.ChildIdentity == nil || status.LastChildStartAt.IsZero() {
		return teamsServiceWatchdogManagedChildIdentity{}, false
	}
	if !teamsLocalSupervisorProcessAlive(status.ChildPID) {
		return teamsServiceWatchdogManagedChildIdentity{}, false
	}
	if err := teamsServiceLocalSupervisorVerifyRecordedIdentity(status.ChildPID, status.ChildIdentity, "local supervisor child"); err != nil {
		return teamsServiceWatchdogManagedChildIdentity{}, false
	}
	return teamsServiceWatchdogManagedChildIdentity{PID: status.ChildPID, StartedAt: status.LastChildStartAt, ManagedBySupervisor: true}, true
}

func evaluateTeamsServiceWatchdog(snapshot teamsServiceWatchdogSnapshot, state teamsServiceWatchdogState, opts teamsServiceWatchdogOptions) teamsServiceWatchdogDecision {
	opts = normalizeTeamsServiceWatchdogOptions(opts)
	if !snapshot.Installed {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: "service is not installed"}
	}
	if snapshot.Disabled {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: "the kill-switch file " + corestore.DisabledFile + " exists; the helper stays stopped"}
	}
	if opts.Now.Before(snapshot.BackoffUntil) {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: "the helper is pausing after repeated fast restarts until " + snapshot.BackoffUntil.Format(time.RFC3339)}
	}
	if !snapshot.Active {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionStart, Reason: "service is installed but not running"}
	}
	if snapshot.StateFiles == 0 {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: defaultTeamsServiceWatchdogMissingStateReason}
	}
	stale, reason := teamsServiceWatchdogStaleReason(snapshot)
	if !stale {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: reason}
	}
	consecutive := state.ConsecutiveStale + 1
	if consecutive < opts.MinConsecutiveStale {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: reason, Stale: true, ConsecutiveStale: consecutive}
	}
	if until, ok := teamsServiceWatchdogCooldownUntil(state, opts); ok {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: reason + "; waiting for restart cooldown", Stale: true, ConsecutiveStale: consecutive, CooldownUntil: until}
	}
	return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionRestart, Reason: reason, Stale: true, ConsecutiveStale: consecutive}
}

func teamsServiceWatchdogStaleReason(snapshot teamsServiceWatchdogSnapshot) (bool, string) {
	switch {
	case snapshot.ManagedChildStarting:
		return false, "managed Teams child is alive; waiting for owner heartbeat"
	case snapshot.OwnerFresh:
		return false, "helper owner heartbeat is fresh"
	case snapshot.OwnerFound:
		return true, "helper owner heartbeat is stale"
	}
	return true, "helper owner is missing"
}

func teamsServiceWatchdogCooldownUntil(state teamsServiceWatchdogState, opts teamsServiceWatchdogOptions) (time.Time, bool) {
	if state.LastActionAt.IsZero() || opts.Cooldown <= 0 {
		return time.Time{}, false
	}
	until := state.LastActionAt.Add(opts.Cooldown)
	if opts.Now.Before(until) {
		return until, true
	}
	return time.Time{}, false
}

func nextTeamsServiceWatchdogState(prev teamsServiceWatchdogState, decision teamsServiceWatchdogDecision, now time.Time) teamsServiceWatchdogState {
	next := prev
	next.UpdatedAt = now
	next.LastReason = strings.TrimSpace(decision.Reason)
	next.ConsecutiveStale = 0
	if decision.Stale {
		next.ConsecutiveStale = decision.ConsecutiveStale
	}
	if decision.Action == teamsServiceWatchdogActionStart || decision.Action == teamsServiceWatchdogActionRestart {
		next.LastAction = decision.Action
		next.LastActionAt = now
	}
	return next
}

func teamsServiceWatchdogPendingActionState(state teamsServiceWatchdogState) teamsServiceWatchdogState {
	state.LastAction = ""
	state.LastActionAt = time.Time{}
	return state
}

func teamsServiceWatchdogStateSemanticallyEqual(left, right teamsServiceWatchdogState) bool {
	left.UpdatedAt = time.Time{}
	right.UpdatedAt = time.Time{}
	return left == right
}

func loadTeamsServiceWatchdogState() (teamsServiceWatchdogState, error) {
	path, err := teamsServiceWatchdogStatePath()
	if err != nil {
		return teamsServiceWatchdogState{}, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return teamsServiceWatchdogState{}, nil
	}
	if err != nil {
		return teamsServiceWatchdogState{}, err
	}
	var state teamsServiceWatchdogState
	if err := json.Unmarshal(data, &state); err != nil {
		return teamsServiceWatchdogState{}, nil
	}
	return state, nil
}

func saveTeamsServiceWatchdogState(state teamsServiceWatchdogState) error {
	path, err := teamsServiceWatchdogStatePath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

func defaultTeamsServiceWatchdogStatePath() (string, error) {
	path, err := appdirs.StatePath("teams", "service", "service-watchdog.json")
	if err != nil {
		return "", err
	}
	var legacyPaths []string
	if legacyPath, legacyErr := appdirs.LegacyCachePath("teams", "service-watchdog.json"); legacyErr == nil {
		legacyPaths = append(legacyPaths, legacyPath)
	}
	if legacyPath, legacyErr := appdirs.LegacyConfigPath("teams", "service-watchdog.json"); legacyErr == nil {
		legacyPaths = append(legacyPaths, legacyPath)
	}
	return appdirs.ResolveMigratedFile(path, legacyPaths...)
}

func printTeamsServiceWatchdogResult(out io.Writer, result teamsServiceWatchdogResult) {
	if out == nil {
		return
	}
	decision := result.Decision
	dryRun := ""
	if result.DryRun {
		dryRun = " dry_run=true"
	}
	parts := []string{
		"Teams service watchdog:",
		"action=" + firstNonEmptyCLI(decision.Action, teamsServiceWatchdogActionNoop),
		"reason=" + strconvQuote(decision.Reason),
		fmt.Sprintf("installed=%t", result.Snapshot.Installed),
		fmt.Sprintf("running=%t", result.Snapshot.Active),
		fmt.Sprintf("state_files=%d", result.Snapshot.StateFiles),
	}
	if decision.ConsecutiveStale > 0 {
		parts = append(parts, fmt.Sprintf("consecutive_stale=%d", decision.ConsecutiveStale))
	}
	if !decision.CooldownUntil.IsZero() {
		parts = append(parts, "cooldown_until="+decision.CooldownUntil.Format(time.RFC3339))
	}
	if dryRun != "" {
		parts = append(parts, strings.TrimSpace(dryRun))
	}
	_, _ = fmt.Fprintln(out, strings.Join(parts, " "))
}

func strconvQuote(s string) string {
	return strconv.Quote(strings.TrimSpace(s))
}
