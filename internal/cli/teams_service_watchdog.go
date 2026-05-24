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

	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	teamsServiceWatchdogActionNoop    = "noop"
	teamsServiceWatchdogActionStart   = "start"
	teamsServiceWatchdogActionRestart = "restart"

	defaultTeamsServiceWatchdogOwnerStaleAfter    = 90 * time.Second
	defaultTeamsServiceWatchdogPollStaleAfter     = 2 * time.Minute
	defaultTeamsServiceWatchdogCooldown           = 2 * time.Minute
	defaultTeamsServiceWatchdogConsecutiveStale   = 3
	defaultTeamsServiceWatchdogMissingStateReason = "no Teams state evidence yet"
	defaultTeamsServiceWatchdogReloadStaleAfter   = 6 * time.Minute
)

type teamsServiceWatchdogOptions struct {
	Now                 time.Time
	OwnerStaleAfter     time.Duration
	PollStaleAfter      time.Duration
	Cooldown            time.Duration
	MinConsecutiveStale int
	DryRun              bool
}

type teamsServiceWatchdogSnapshot struct {
	Installed                          bool
	Active                             bool
	StateFiles                         int
	ServicePaused                      bool
	ServiceDraining                    bool
	HelperUpgradeDrainExpired          bool
	HelperUpgradeDrainLocalOwnerFresh  bool
	HelperUpgradeDrainRemoteOwnerFresh bool
	HelperReloadDrainStale             bool
	HelperReloadDrainLocalOwnerFresh   bool
	HelperReloadDrainRemoteOwnerFresh  bool
	OwnerFound                         bool
	OwnerFresh                         bool
	OwnerActiveTurn                    bool
	LastOwnerHeartbeat                 time.Time
	FreshOwnerStartedAt                time.Time
	PollActivityFound                  bool
	PollActivityAt                     time.Time
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

var (
	teamsServiceWatchdogNow             = time.Now
	teamsServiceWatchdogStatePath       = defaultTeamsServiceWatchdogStatePath
	teamsServiceWatchdogCollectSnapshot = collectTeamsServiceWatchdogSnapshot
	teamsServiceWatchdogStartService    = startTeamsPrimaryService
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
	if opts.PollStaleAfter <= 0 {
		opts.PollStaleAfter = defaultTeamsServiceWatchdogPollStaleAfter
	}
	if opts.Cooldown <= 0 {
		opts.Cooldown = defaultTeamsServiceWatchdogCooldown
	}
	if opts.MinConsecutiveStale <= 0 {
		opts.MinConsecutiveStale = defaultTeamsServiceWatchdogConsecutiveStale
	}
	return opts
}

func collectTeamsServiceWatchdogSnapshot(ctx context.Context, opts teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
	var snapshot teamsServiceWatchdogSnapshot
	installed, err := teamsServiceInstalled()
	if err != nil {
		return snapshot, err
	}
	snapshot.Installed = installed
	if installed {
		active, err := teamsServiceActive(ctx)
		if err != nil {
			return snapshot, err
		}
		snapshot.Active = active
	}
	paths, err := existingTeamsStorePaths()
	if err != nil {
		return snapshot, err
	}
	for _, path := range paths {
		st, err := teamsstore.Open(path)
		if err != nil {
			return snapshot, err
		}
		state, err := st.Load(ctx)
		if err != nil {
			return snapshot, err
		}
		snapshot.StateFiles++
		mergeTeamsServiceWatchdogState(&snapshot, state, opts)
	}
	return snapshot, nil
}

func mergeTeamsServiceWatchdogState(snapshot *teamsServiceWatchdogSnapshot, state teamsstore.State, opts teamsServiceWatchdogOptions) {
	if snapshot == nil {
		return
	}
	if state.ServiceControl.Paused {
		snapshot.ServicePaused = true
	}
	if state.ServiceControl.Draining {
		snapshot.ServiceDraining = true
	}
	owner, hasOwner := rawTeamsStateOwner(state)
	if hasOwner {
		snapshot.OwnerFound = true
		if owner.LastHeartbeat.After(snapshot.LastOwnerHeartbeat) {
			snapshot.LastOwnerHeartbeat = owner.LastHeartbeat
		}
		fresh := !teamsstore.IsStale(owner, opts.OwnerStaleAfter, opts.Now) && !teamsstore.OwnerAppearsLocallyDead(owner)
		if fresh {
			snapshot.OwnerFresh = true
			if strings.TrimSpace(owner.ActiveTurnID) != "" {
				snapshot.OwnerActiveTurn = true
			}
			if !owner.StartedAt.IsZero() && (snapshot.FreshOwnerStartedAt.IsZero() || owner.StartedAt.Before(snapshot.FreshOwnerStartedAt)) {
				snapshot.FreshOwnerStartedAt = owner.StartedAt
			}
		}
	}
	if !state.ServiceControl.Paused && teamsstore.HelperUpgradeDrainExpired(state, opts.Now) {
		snapshot.HelperUpgradeDrainExpired = true
		if hasOwner {
			fresh := !teamsstore.IsStale(owner, opts.OwnerStaleAfter, opts.Now) && !teamsstore.OwnerAppearsLocallyDead(owner)
			if fresh {
				if teamsstore.OwnerAppearsLocal(owner) {
					snapshot.HelperUpgradeDrainLocalOwnerFresh = true
				} else {
					snapshot.HelperUpgradeDrainRemoteOwnerFresh = true
				}
			}
		}
	}
	if !state.ServiceControl.Paused && teamsstore.HelperReloadDrainStale(state, opts.Now, defaultTeamsServiceWatchdogReloadStaleAfter) {
		snapshot.HelperReloadDrainStale = true
		if hasOwner {
			fresh := !teamsstore.IsStale(owner, opts.OwnerStaleAfter, opts.Now) && !teamsstore.OwnerAppearsLocallyDead(owner)
			if fresh {
				if teamsstore.OwnerAppearsLocal(owner) {
					snapshot.HelperReloadDrainLocalOwnerFresh = true
				} else {
					snapshot.HelperReloadDrainRemoteOwnerFresh = true
				}
			}
		}
	}
	if activity, ok := teamsServiceWatchdogPollActivity(state, opts.Now); ok {
		snapshot.PollActivityFound = true
		if activity.After(snapshot.PollActivityAt) {
			snapshot.PollActivityAt = activity
		}
	}
}

func rawTeamsStateOwner(state teamsstore.State) (teamsstore.OwnerMetadata, bool) {
	if state.ServiceOwner != nil {
		return *state.ServiceOwner, true
	}
	if state.LockOwner != nil {
		return *state.LockOwner, true
	}
	return teamsstore.OwnerMetadata{}, false
}

func teamsServiceWatchdogPollActivity(state teamsstore.State, now time.Time) (time.Time, bool) {
	if len(state.ChatPolls) == 0 {
		return time.Time{}, false
	}
	controlChatID := strings.TrimSpace(state.ControlChat.TeamsChatID)
	if controlChatID != "" {
		poll, ok := state.ChatPolls[controlChatID]
		if !ok {
			return time.Time{}, false
		}
		return teamsServiceWatchdogPollStateActivity(poll, now)
	}
	var out time.Time
	found := false
	for _, poll := range state.ChatPolls {
		if activity, ok := teamsServiceWatchdogPollStateActivity(poll, now); ok {
			found = true
			if activity.After(out) {
				out = activity
			}
		}
	}
	return out, found
}

func teamsServiceWatchdogPollStateActivity(poll teamsstore.ChatPollState, now time.Time) (time.Time, bool) {
	out := poll.LastSuccessfulPollAt
	if poll.LastErrorAt.After(out) {
		out = poll.LastErrorAt
	}
	if !poll.BlockedUntil.IsZero() && poll.BlockedUntil.After(now) && now.After(out) {
		out = now
	}
	if out.IsZero() {
		return time.Time{}, false
	}
	return out, true
}

func evaluateTeamsServiceWatchdog(snapshot teamsServiceWatchdogSnapshot, state teamsServiceWatchdogState, opts teamsServiceWatchdogOptions) teamsServiceWatchdogDecision {
	opts = normalizeTeamsServiceWatchdogOptions(opts)
	if !snapshot.Installed {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: "service is not installed"}
	}
	if !snapshot.Active {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionStart, Reason: "service is installed but not running"}
	}
	if snapshot.ServiceDraining {
		if snapshot.HelperUpgradeDrainExpired {
			return evaluateTeamsServiceWatchdogExpiredHelperUpgradeDrain(snapshot, state, opts)
		}
		if snapshot.HelperReloadDrainStale {
			return evaluateTeamsServiceWatchdogStaleHelperReloadDrain(snapshot, state, opts)
		}
	}
	if snapshot.ServicePaused {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: "service is paused"}
	}
	if snapshot.ServiceDraining {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: "service is draining"}
	}
	if snapshot.StateFiles == 0 {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: defaultTeamsServiceWatchdogMissingStateReason}
	}

	stale, reason := teamsServiceWatchdogStaleReason(snapshot, opts)
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

func evaluateTeamsServiceWatchdogExpiredHelperUpgradeDrain(snapshot teamsServiceWatchdogSnapshot, state teamsServiceWatchdogState, opts teamsServiceWatchdogOptions) teamsServiceWatchdogDecision {
	reason := "helper upgrade drain expired"
	if snapshot.OwnerFresh && snapshot.OwnerActiveTurn {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: reason + "; active turn is still heartbeating"}
	}
	if snapshot.HelperUpgradeDrainRemoteOwnerFresh {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: reason + "; owner is fresh on another machine"}
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

func evaluateTeamsServiceWatchdogStaleHelperReloadDrain(snapshot teamsServiceWatchdogSnapshot, state teamsServiceWatchdogState, opts teamsServiceWatchdogOptions) teamsServiceWatchdogDecision {
	reason := "helper reload drain is stale"
	if snapshot.OwnerFresh && snapshot.OwnerActiveTurn {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: reason + "; active turn is still heartbeating"}
	}
	if snapshot.HelperReloadDrainRemoteOwnerFresh {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: reason + "; owner is fresh on another machine"}
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

func teamsServiceWatchdogStaleReason(snapshot teamsServiceWatchdogSnapshot, opts teamsServiceWatchdogOptions) (bool, string) {
	lastActivity := snapshot.LastOwnerHeartbeat
	if snapshot.PollActivityAt.After(lastActivity) {
		lastActivity = snapshot.PollActivityAt
	}
	if !snapshot.OwnerFresh {
		if !lastActivity.IsZero() && !lastActivity.After(opts.Now) && opts.Now.Sub(lastActivity) <= opts.OwnerStaleAfter {
			return false, "no fresh owner yet, but local activity is recent"
		}
		if snapshot.OwnerFound {
			return true, "helper owner heartbeat is stale"
		}
		return true, "helper owner is missing"
	}
	if snapshot.OwnerActiveTurn {
		return false, "helper owner is fresh and a turn is active"
	}
	if snapshot.PollActivityFound {
		if !snapshot.PollActivityAt.After(opts.Now) && opts.Now.Sub(snapshot.PollActivityAt) > opts.PollStaleAfter {
			return true, "control chat polling is stale"
		}
		return false, "helper owner and control chat polling are fresh"
	}
	if !snapshot.FreshOwnerStartedAt.IsZero() && !snapshot.FreshOwnerStartedAt.After(opts.Now) && opts.Now.Sub(snapshot.FreshOwnerStartedAt) > opts.PollStaleAfter {
		return true, "control chat polling never became active"
	}
	return false, "helper owner is fresh; waiting for first control chat poll"
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
	base, err := os.UserCacheDir()
	if err != nil || strings.TrimSpace(base) == "" {
		base, err = os.UserConfigDir()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(base, "codex-helper", "teams", "service-watchdog.json"), nil
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
