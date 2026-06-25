package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/teams/delegation"
	"github.com/baaaaaaaka/codex-helper/internal/teams/machineregistry"
	"github.com/spf13/cobra"
)

type delegateOptions struct {
	root                 *rootOptions
	ctx                  context.Context
	storePath            string
	routeStorePath       string
	registryCache        string
	jsonOutput           bool
	query                string
	candidateFile        string
	top                  int
	candidateToken       string
	newThreadToken       string
	threadToken          string
	taskFile             string
	sourceSession        string
	sourceTurn           string
	workspaceFingerprint string
	parentID             string
	path                 []string
	delegationID         string
	machineID            string
	workerID             string
	claimID              string
	claimEpoch           int
	resultStatus         string
	body                 string
	machineLabel         string
	aliases              []string
	capabilities         []string
	accepting            bool
	draining             bool
	heartbeat            time.Duration
	ttl                  time.Duration
	timeout              time.Duration
	waitUntil            string
	reason               string
	now                  func() time.Time
	loadCandidates       func(*delegateOptions) ([]delegation.Candidate, error)
	openRegistry         func(*delegateOptions) (*delegateRegistrySession, error)
}

const defaultDelegateClaimRecheckDelay = 5 * time.Second

const (
	delegateWaitUntilTerminal           = "terminal"
	delegateWaitUntilTerminalOrQuestion = "terminal-or-question"
)

func newDelegateCmd(root *rootOptions) *cobra.Command {
	opts := &delegateOptions{root: root, now: time.Now}
	cmd := &cobra.Command{
		Use:   "delegate",
		Short: "Resolve and manage cross-machine agent delegation",
	}
	cmd.PersistentFlags().StringVar(&opts.storePath, "store", "", "Override local delegation state file")
	cmd.PersistentFlags().BoolVar(&opts.jsonOutput, "json", false, "Print JSON output")
	cmd.AddCommand(
		newDelegateResolveCmd(opts),
		newDelegateStartCmd(opts),
		newDelegateStatusCmd(opts),
		newDelegateWaitCmd(opts),
		newDelegateCancelCmd(opts),
		newDelegateClaimCmd(opts),
		newDelegateProgressCmd(opts),
		newDelegateQuestionCmd(opts),
		newDelegateResultCmd(opts),
		newDelegateMachineCmd(opts),
	)
	return cmd
}

func newDelegateResolveCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resolve",
		Short: "Resolve candidate machines for a natural-language delegation request",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateResolve(opts)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	cmd.Flags().StringVar(&opts.query, "query", "", "Natural-language delegation need")
	cmd.Flags().StringVar(&opts.candidateFile, "candidate-file", "", "JSON file containing resolver candidates")
	cmd.Flags().StringVar(&opts.registryCache, "registry-cache", "", "Override Teams machine registry cache file")
	cmd.Flags().StringVar(&opts.sourceSession, "source-session", "", "Source Codex session id for remote thread reuse")
	cmd.Flags().StringVar(&opts.workspaceFingerprint, "workspace-fingerprint", "", "Workspace or topic fingerprint for remote thread reuse")
	cmd.Flags().IntVar(&opts.top, "top", 50, "Recent registry messages to inspect")
	_ = cmd.MarkFlagRequired("query")
	return cmd
}

func newDelegateStartCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Create an idempotent delegation request",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateStart(opts)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	cmd.Flags().StringVar(&opts.candidateToken, "candidate-token", "", "Candidate token returned by delegate resolve")
	cmd.Flags().StringVar(&opts.newThreadToken, "new-thread-token", "", "New remote thread token returned by delegate resolve")
	cmd.Flags().StringVar(&opts.threadToken, "thread-token", "", "Reusable remote thread token returned by delegate resolve")
	cmd.Flags().StringVar(&opts.taskFile, "task-file", "", "JSON task spec file")
	cmd.Flags().StringVar(&opts.sourceSession, "source-session", "", "Source Codex session id")
	cmd.Flags().StringVar(&opts.sourceTurn, "source-turn", "", "Source Codex turn id")
	cmd.Flags().StringVar(&opts.workspaceFingerprint, "workspace-fingerprint", "", "Workspace or topic fingerprint for remote thread reuse")
	cmd.Flags().StringVar(&opts.parentID, "parent", "", "Parent delegation id")
	cmd.Flags().StringSliceVar(&opts.path, "path", nil, "Delegation path machine id; repeat or comma-separate")
	_ = cmd.MarkFlagRequired("candidate-token")
	_ = cmd.MarkFlagRequired("task-file")
	return cmd
}

func newDelegateStatusCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show a delegation state",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateStatus(opts)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	cmd.Flags().StringVar(&opts.delegationID, "id", "", "Delegation id")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

func newDelegateWaitCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wait",
		Short: "Wait for a delegation to reach a terminal state",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateWait(opts)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	cmd.Flags().StringVar(&opts.delegationID, "id", "", "Delegation id")
	cmd.Flags().DurationVar(&opts.timeout, "timeout", 30*time.Second, "Maximum wait duration")
	cmd.Flags().StringVar(&opts.waitUntil, "until", delegateWaitUntilTerminalOrQuestion, "Return condition: terminal or terminal-or-question")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

func newDelegateCancelCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel",
		Short: "Cancel an open delegation",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateCancel(opts)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	cmd.Flags().StringVar(&opts.delegationID, "id", "", "Delegation id")
	cmd.Flags().StringVar(&opts.reason, "reason", "canceled", "Cancel reason")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

func newDelegateClaimCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "claim",
		Short: "Claim an open delegation for a worker",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateClaim(opts)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	cmd.Flags().StringVar(&opts.delegationID, "id", "", "Delegation id")
	cmd.Flags().StringVar(&opts.machineID, "machine-id", "", "Claiming machine id")
	cmd.Flags().StringVar(&opts.workerID, "worker-instance", "", "Worker instance id")
	cmd.Flags().IntVar(&opts.claimEpoch, "claim-epoch", 1, "Claim epoch")
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("machine-id")
	_ = cmd.MarkFlagRequired("worker-instance")
	return cmd
}

func newDelegateResultCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "result",
		Short: "Publish a terminal delegation result for a winning claim",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateResult(opts)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	cmd.Flags().StringVar(&opts.delegationID, "id", "", "Delegation id")
	cmd.Flags().StringVar(&opts.machineID, "machine-id", "", "Claiming machine id")
	cmd.Flags().StringVar(&opts.workerID, "worker-instance", "", "Worker instance id")
	cmd.Flags().StringVar(&opts.claimID, "claim-id", "", "Claim id")
	cmd.Flags().IntVar(&opts.claimEpoch, "claim-epoch", 1, "Claim epoch")
	cmd.Flags().StringVar(&opts.resultStatus, "status", delegation.StateComplete, "Terminal status: complete or blocked")
	cmd.Flags().StringVar(&opts.body, "body", "", "Terminal result body")
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("machine-id")
	_ = cmd.MarkFlagRequired("worker-instance")
	_ = cmd.MarkFlagRequired("claim-id")
	return cmd
}

func newDelegateProgressCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "progress",
		Short: "Publish an intermediate delegation progress status for a winning claim",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateStatusRecord(opts, delegation.StateRunning)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	addDelegateStatusRecordFlags(cmd, opts)
	return cmd
}

func newDelegateQuestionCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "question",
		Short: "Publish an intermediate delegation question for the source agent",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateStatusRecord(opts, delegation.StateQuestion)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	addDelegateStatusRecordFlags(cmd, opts)
	return cmd
}

func addDelegateStatusRecordFlags(cmd *cobra.Command, opts *delegateOptions) {
	cmd.Flags().StringVar(&opts.delegationID, "id", "", "Delegation id")
	cmd.Flags().StringVar(&opts.machineID, "machine-id", "", "Claiming machine id")
	cmd.Flags().StringVar(&opts.workerID, "worker-instance", "", "Worker instance id")
	cmd.Flags().StringVar(&opts.claimID, "claim-id", "", "Claim id")
	cmd.Flags().IntVar(&opts.claimEpoch, "claim-epoch", 1, "Claim epoch")
	cmd.Flags().StringVar(&opts.body, "body", "", "Progress or question body")
	_ = cmd.MarkFlagRequired("id")
	_ = cmd.MarkFlagRequired("machine-id")
	_ = cmd.MarkFlagRequired("worker-instance")
	_ = cmd.MarkFlagRequired("claim-id")
	_ = cmd.MarkFlagRequired("body")
}

func newDelegateMachineCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "machine",
		Short: "Publish or inspect this machine in the cross-machine registry",
	}
	cmd.AddCommand(newDelegateMachinePublishOnceCmd(opts))
	return cmd
}

func newDelegateMachinePublishOnceCmd(opts *delegateOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "publish-once",
		Short: "Publish one machine capability heartbeat to the Teams registry",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.ctx = cmd.Context()
			result, err := runDelegateMachinePublishOnce(opts)
			if err != nil {
				return err
			}
			return printDelegateJSON(cmd, opts, result)
		},
	}
	cmd.Flags().StringVar(&opts.machineID, "machine-id", "", "Machine id to publish")
	cmd.Flags().StringVar(&opts.machineLabel, "label", "", "Human-readable machine label")
	cmd.Flags().StringSliceVar(&opts.aliases, "alias", nil, "Machine alias; repeat or comma-separate")
	cmd.Flags().StringSliceVar(&opts.capabilities, "capability", nil, "Machine capability; repeat or comma-separate")
	cmd.Flags().BoolVar(&opts.accepting, "accepting", true, "Whether this machine accepts new delegations")
	cmd.Flags().BoolVar(&opts.draining, "draining", false, "Publish draining state")
	cmd.Flags().DurationVar(&opts.heartbeat, "heartbeat", machineregistry.DefaultHeartbeatInterval, "Heartbeat interval")
	cmd.Flags().DurationVar(&opts.ttl, "ttl", machineregistry.DefaultOnlineTTL, "Online TTL")
	return cmd
}

type delegateStartResult struct {
	DelegationID   string           `json:"delegation_id"`
	Status         string           `json:"status"`
	MachineID      string           `json:"machine_id"`
	RemoteThreadID string           `json:"remote_thread_id,omitempty"`
	ThreadPolicy   string           `json:"thread_policy,omitempty"`
	Idempotent     bool             `json:"idempotent"`
	State          delegation.State `json:"state"`
	StorePath      string           `json:"store_path,omitempty"`
	Transport      string           `json:"transport"`
}

type delegateClaimResult struct {
	DelegationID        string            `json:"delegation_id"`
	Status              string            `json:"status"`
	Claim               delegation.Record `json:"claim"`
	Winning             bool              `json:"winning"`
	ShouldExecute       bool              `json:"should_execute"`
	RecheckAfterSeconds int               `json:"recheck_after_seconds,omitempty"`
	State               delegation.State  `json:"state"`
	Transport           string            `json:"transport"`
}

type delegateResultResult struct {
	DelegationID string            `json:"delegation_id"`
	Status       string            `json:"status"`
	Result       delegation.Record `json:"result"`
	State        delegation.State  `json:"state"`
	Transport    string            `json:"transport"`
}

type delegateStatusRecordResult struct {
	DelegationID string            `json:"delegation_id"`
	Status       string            `json:"status"`
	Record       delegation.Record `json:"record"`
	State        delegation.State  `json:"state"`
	Transport    string            `json:"transport"`
}

type delegateMachinePublishResult struct {
	MachineID   string `json:"machine_id"`
	Mode        string `json:"mode"`
	Transport   string `json:"transport"`
	SlotMessage string `json:"slot_message_id,omitempty"`
}

func runDelegateResolve(opts *delegateOptions) (delegation.ResolveResult, error) {
	now := opts.now().UTC()
	candidates, err := loadDelegateCandidates(opts)
	if err != nil {
		return delegation.ResolveResult{}, err
	}
	threadStore, _ := loadDelegateRouteStore(opts)
	query := strings.TrimSpace(opts.query)
	for i := range candidates {
		scoreCandidate(query, &candidates[i])
		if candidates[i].State == "" {
			candidates[i].State = "unknown"
		}
		if candidates[i].State == "online" && candidates[i].Accepting && candidates[i].CandidateToken == "" {
			token, validUntil, err := delegation.NewCandidateTokenForCandidate(candidates[i], now, delegation.DefaultCandidateTokenTTL)
			if err != nil {
				candidates[i].NotStartableReasons = append(candidates[i].NotStartableReasons, err.Error())
			} else {
				candidates[i].CandidateToken = token
				candidates[i].ValidUntil = validUntil
			}
			threadToken, _, threadErr := delegation.NewThreadTokenForCandidate(candidates[i], opts.sourceSession, opts.workspaceFingerprint, now, delegation.DefaultThreadTokenTTL)
			if threadErr == nil {
				candidates[i].NewThreadToken = threadToken
			} else {
				candidates[i].NotStartableReasons = append(candidates[i].NotStartableReasons, threadErr.Error())
			}
			candidates[i].ThreadCandidates = reusableThreadCandidates(threadStore, candidates[i], opts, now, 3)
		}
		if candidates[i].State != "online" {
			candidates[i].NotStartableReasons = appendMissing(candidates[i].NotStartableReasons, "machine is not online")
		}
		if !candidates[i].Accepting {
			candidates[i].NotStartableReasons = appendMissing(candidates[i].NotStartableReasons, "machine is not accepting delegation")
		}
	}
	sortCandidates(candidates)
	startable := make([]delegation.Candidate, 0, len(candidates))
	available := make([]delegation.Candidate, 0, len(candidates))
	var reasons []string
	for _, candidate := range candidates {
		if candidate.State == "online" && candidate.Accepting && candidate.Confidence >= 0.8 && candidate.CandidateToken != "" {
			startable = append(startable, candidate)
		}
		if candidate.State == "online" && candidate.Accepting && candidate.CandidateToken != "" {
			available = append(available, candidate)
		} else if len(candidate.NotStartableReasons) > 0 {
			reasons = append(reasons, candidate.MachineID+": "+strings.Join(candidate.NotStartableReasons, "; "))
		}
	}
	result := delegation.ResolveResult{Query: query, Candidates: candidates}
	switch len(startable) {
	case 0:
		if len(available) > 0 {
			result.Action = delegation.ActionAskUser
			result.Reason = "online_candidates_available_no_confident_match"
		} else {
			result.Action = delegation.ActionDoNotDelegate
			result.Reason = "no_online_matching_candidate"
			result.NotStartableReasons = reasons
		}
	case 1:
		result.Action = delegation.ActionStart
		result.Reason = "single_online_matching_candidate"
		result.CandidateToken = startable[0].CandidateToken
		result.NewThreadToken = startable[0].NewThreadToken
		result.ThreadCandidates = startable[0].ThreadCandidates
		result.ValidUntil = startable[0].ValidUntil
	default:
		result.Action = delegation.ActionAskUser
		result.Reason = "multiple_matching_candidates"
	}
	return result, nil
}

func runDelegateStart(opts *delegateOptions) (delegateStartResult, error) {
	now := opts.now().UTC()
	token, err := delegation.DecodeCandidateToken(opts.candidateToken, now)
	if err != nil {
		return delegateStartResult{}, err
	}
	spec, err := readTaskSpec(opts.taskFile)
	if err != nil {
		return delegateStartResult{}, err
	}
	record, err := delegation.NewRequestRecord(opts.sourceSession, opts.sourceTurn, opts.parentID, opts.path, token.MachineID, spec, now)
	if err != nil {
		return delegateStartResult{}, err
	}
	record = delegation.BindRequestToCandidate(record, token)
	threadPayload, hasThread, err := resolveDelegateStartThread(opts, token, spec, now)
	if err != nil {
		return delegateStartResult{}, err
	}
	if hasThread {
		record = delegation.BindRequestToThread(record, threadPayload)
	}
	if strings.TrimSpace(opts.storePath) == "" {
		return runDelegateStartInbox(opts, token, record, now)
	}
	path, err := delegateStorePath(opts.storePath)
	if err != nil {
		return delegateStartResult{}, err
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return delegateStartResult{}, err
	}
	idempotent := false
	for _, existing := range store.Records {
		if existing.Kind == delegation.RequestKind && existing.SourceKey == record.SourceKey {
			record = existing
			idempotent = true
			break
		}
	}
	if !idempotent {
		store.Records = append(store.Records, record)
		if hasThread {
			upsertStartedRemoteThread(&store, record, threadPayload, now)
		}
		store.Prune(opts.now().UTC(), delegation.DefaultStoreRetention)
		if _, err := delegation.SaveStore(path, store); err != nil {
			return delegateStartResult{}, err
		}
	}
	state := delegation.Reduce(recordsForID(store.Records, record.DelegationID), now)
	return delegateStartResult{
		DelegationID:   record.DelegationID,
		Status:         state.Status,
		MachineID:      record.MachineID,
		RemoteThreadID: record.RemoteThreadID,
		ThreadPolicy:   record.ThreadPolicy,
		Idempotent:     idempotent,
		State:          state,
		StorePath:      path,
		Transport:      "local-store",
	}, nil
}

func runDelegateStatus(opts *delegateOptions) (delegation.State, error) {
	if strings.TrimSpace(opts.storePath) == "" {
		records, err := readDelegateInboxRecordsForID(opts, opts.delegationID)
		if err != nil {
			return delegation.State{}, err
		}
		state := delegation.Reduce(records, opts.now().UTC())
		_ = syncDelegateRemoteThreadFromState(opts, state)
		return state, nil
	}
	path, err := delegateStorePath(opts.storePath)
	if err != nil {
		return delegation.State{}, err
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return delegation.State{}, err
	}
	state := delegation.Reduce(recordsForID(store.Records, opts.delegationID), opts.now().UTC())
	_ = syncDelegateRemoteThreadFromState(opts, state)
	return state, nil
}

func runDelegateWait(opts *delegateOptions) (delegation.State, error) {
	deadline := opts.now().Add(opts.timeout)
	delay := time.Second
	for {
		state, err := runDelegateStatus(opts)
		if err != nil {
			return delegation.State{}, err
		}
		if delegateWaitShouldReturn(opts.waitUntil, state.Status) || !opts.now().Before(deadline) {
			return state, nil
		}
		if remaining := time.Until(deadline); remaining > 0 && delay > remaining {
			delay = remaining
		}
		time.Sleep(delay)
		if delay < 5*time.Second {
			delay *= 2
			if delay > 5*time.Second {
				delay = 5 * time.Second
			}
		}
	}
}

func runDelegateCancel(opts *delegateOptions) (delegation.State, error) {
	if strings.TrimSpace(opts.storePath) == "" {
		record := delegation.NewTombstoneRecord(opts.delegationID, opts.reason, opts.now().UTC())
		records, err := appendDelegateInboxRecordForID(opts, opts.delegationID, record)
		if err != nil {
			return delegation.State{}, err
		}
		return delegation.Reduce(delegation.RecordsForID(records, opts.delegationID), opts.now().UTC()), nil
	}
	path, err := delegateStorePath(opts.storePath)
	if err != nil {
		return delegation.State{}, err
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return delegation.State{}, err
	}
	record := delegation.NewTombstoneRecord(opts.delegationID, opts.reason, opts.now().UTC())
	store.Records = append(store.Records, record)
	if _, err := delegation.SaveStore(path, store); err != nil {
		return delegation.State{}, err
	}
	return delegation.Reduce(recordsForID(store.Records, opts.delegationID), opts.now().UTC()), nil
}

func runDelegateClaim(opts *delegateOptions) (delegateClaimResult, error) {
	now := opts.now().UTC()
	claim, err := delegation.NewClaimRecord(opts.delegationID, opts.machineID, opts.workerID, opts.claimEpoch, now)
	if err != nil {
		return delegateClaimResult{}, err
	}
	var records []delegation.Record
	transport := "registry"
	if strings.TrimSpace(opts.storePath) == "" {
		transport = "inbox"
		records, err = appendDelegateInboxRecordForClaim(opts, claim)
	} else {
		transport = "local-store"
		records, err = appendDelegateLocalRecord(opts, claim)
	}
	if err != nil {
		return delegateClaimResult{}, err
	}
	state := delegation.Reduce(delegation.RecordsForID(records, opts.delegationID), now)
	winning := state.WinningClaim != nil && state.WinningClaim.RecordID == claim.RecordID
	shouldExecute := winning && len(state.ConflictRecordIDs) == 0 && !isTerminalDelegationState(state.Status)
	recheckAfter := 0
	if winning {
		recheckAfter = int(defaultDelegateClaimRecheckDelay.Seconds())
	}
	return delegateClaimResult{
		DelegationID:        opts.delegationID,
		Status:              state.Status,
		Claim:               claim,
		Winning:             winning,
		ShouldExecute:       shouldExecute,
		RecheckAfterSeconds: recheckAfter,
		State:               state,
		Transport:           transport,
	}, nil
}

func runDelegateResult(opts *delegateOptions) (delegateResultResult, error) {
	now := opts.now().UTC()
	claim := delegation.Record{
		Kind:             delegation.ClaimKind,
		DelegationID:     strings.TrimSpace(opts.delegationID),
		MachineID:        strings.TrimSpace(opts.machineID),
		ClaimID:          strings.TrimSpace(opts.claimID),
		ClaimEpoch:       opts.claimEpoch,
		WorkerInstanceID: strings.TrimSpace(opts.workerID),
	}
	result, err := delegation.NewResultRecord(opts.delegationID, claim, opts.resultStatus, opts.body, 1, now)
	if err != nil {
		return delegateResultResult{}, err
	}
	var records []delegation.Record
	transport := "registry"
	if strings.TrimSpace(opts.storePath) == "" {
		transport = "inbox"
		records, err = appendDelegateInboxRecordForClaim(opts, result)
	} else {
		transport = "local-store"
		records, err = appendDelegateLocalRecord(opts, result)
	}
	if err != nil {
		return delegateResultResult{}, err
	}
	state := delegation.Reduce(delegation.RecordsForID(records, opts.delegationID), now)
	_ = syncDelegateRemoteThreadFromState(opts, state)
	return delegateResultResult{
		DelegationID: opts.delegationID,
		Status:       state.Status,
		Result:       result,
		State:        state,
		Transport:    transport,
	}, nil
}

func runDelegateStatusRecord(opts *delegateOptions, status string) (delegateStatusRecordResult, error) {
	now := opts.now().UTC()
	claim := delegation.Record{
		Kind:             delegation.ClaimKind,
		DelegationID:     strings.TrimSpace(opts.delegationID),
		MachineID:        strings.TrimSpace(opts.machineID),
		ClaimID:          strings.TrimSpace(opts.claimID),
		ClaimEpoch:       opts.claimEpoch,
		WorkerInstanceID: strings.TrimSpace(opts.workerID),
	}
	var (
		record delegation.Record
		err    error
	)
	if status == delegation.StateQuestion {
		record, err = delegation.NewQuestionRecord(opts.delegationID, claim, opts.body, now)
	} else {
		record, err = delegation.NewStatusRecord(opts.delegationID, claim, status, opts.body, now)
	}
	if err != nil {
		return delegateStatusRecordResult{}, err
	}
	var records []delegation.Record
	transport := "registry"
	if strings.TrimSpace(opts.storePath) == "" {
		transport = "inbox"
		records, err = appendDelegateInboxRecordForClaim(opts, record)
	} else {
		transport = "local-store"
		records, err = appendDelegateLocalRecord(opts, record)
	}
	if err != nil {
		return delegateStatusRecordResult{}, err
	}
	state := delegation.Reduce(delegation.RecordsForID(records, opts.delegationID), now)
	_ = syncDelegateRemoteThreadFromState(opts, state)
	return delegateStatusRecordResult{
		DelegationID: opts.delegationID,
		Status:       state.Status,
		Record:       record,
		State:        state,
		Transport:    transport,
	}, nil
}

func runDelegateMachinePublishOnce(opts *delegateOptions) (delegateMachinePublishResult, error) {
	session, err := openDelegateRegistry(opts)
	if err != nil {
		return delegateMachinePublishResult{}, err
	}
	defer session.close(opts.context())
	card, err := buildDelegateMachineCard(opts, session.cache)
	if err != nil {
		return delegateMachinePublishResult{}, err
	}
	nextCache, _, _, _, err := session.store.EnsureInbox(opts.context(), session.cache, card.MachineID)
	if err != nil {
		return delegateMachinePublishResult{}, err
	}
	session.cache = nextCache
	card.InboxRef = strings.TrimSpace(nextCache.InboxExternalID)
	card.InboxGeneration = strings.TrimSpace(nextCache.InboxGeneration)
	next, result, _, err := session.store.Publish(opts.context(), session.cache, card)
	if err != nil {
		return delegateMachinePublishResult{}, err
	}
	session.cache = next
	return delegateMachinePublishResult{
		MachineID:   card.MachineID,
		Mode:        result.Mode,
		Transport:   "registry",
		SlotMessage: result.SlotMessageID,
	}, nil
}

func readTaskSpec(path string) (delegation.TaskSpec, error) {
	raw, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return delegation.TaskSpec{}, err
	}
	var spec delegation.TaskSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return delegation.TaskSpec{}, err
	}
	if err := spec.Validate(); err != nil {
		return delegation.TaskSpec{}, err
	}
	return spec, nil
}

func loadDelegateCandidates(opts *delegateOptions) ([]delegation.Candidate, error) {
	if opts.loadCandidates != nil {
		return opts.loadCandidates(opts)
	}
	if strings.TrimSpace(opts.candidateFile) == "" {
		return loadDelegateCandidatesFromTeamsRegistry(opts)
	}
	raw, err := os.ReadFile(opts.candidateFile)
	if err != nil {
		return nil, err
	}
	var direct []delegation.Candidate
	if err := json.Unmarshal(raw, &direct); err == nil {
		return direct, nil
	}
	var wrapped struct {
		Candidates []delegation.Candidate `json:"candidates"`
	}
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		return nil, err
	}
	return wrapped.Candidates, nil
}

func loadDelegateCandidatesFromTeamsRegistry(opts *delegateOptions) ([]delegation.Candidate, error) {
	session, err := openDelegateRegistry(opts)
	if err != nil {
		return nil, err
	}
	defer session.close(opts.context())
	top := opts.top
	if top <= 0 {
		top = 50
	}
	statuses, err := session.store.Observe(opts.context(), session.cache, top, opts.now().UTC())
	if err != nil {
		return nil, err
	}
	candidates := candidatesFromMachineStatuses(statuses)
	for i := range candidates {
		candidates[i].RegistryGeneration = strings.TrimSpace(session.cache.RegistryGeneration)
	}
	return candidates, nil
}

type delegateRegistryRecordGraph interface {
	SendHTML(ctx context.Context, chatID string, html string) (teams.ChatMessage, error)
	ListMessages(ctx context.Context, chatID string, top int) ([]teams.ChatMessage, error)
	ListMessagesExactTopWithoutRateLimitRetry(ctx context.Context, chatID string, top int) ([]teams.ChatMessage, error)
	CreateOrGetMeetingChatWindow(ctx context.Context, topic string, externalID string, start time.Time, end time.Time) (teams.Chat, teams.OnlineMeeting, error)
}

type delegateRegistryRecordWindowLister interface {
	ListMessagesWindow(ctx context.Context, chatID string, top int, modifiedAfter time.Time) (teams.MessageWindow, error)
	ListMessagesWindowFromPath(ctx context.Context, path string) (teams.MessageWindow, error)
}

type delegateRegistrySession struct {
	graph  delegateRegistryRecordGraph
	store  machineregistry.Store
	cache  machineregistry.Cache
	chatID string
	close  func(context.Context) error
}

func openDelegateRegistry(opts *delegateOptions) (*delegateRegistrySession, error) {
	if opts.openRegistry != nil {
		return opts.openRegistry(opts)
	}
	ctx := opts.context()
	httpClient, err := newTeamsGraphHTTPClientLease(ctx, opts.root, io.Discard)
	if err != nil {
		return nil, err
	}
	closeSession := func(ctx context.Context) error {
		return httpClient.Close(ctx)
	}
	cfg, err := teams.DefaultEffectiveAuthConfig()
	if err != nil {
		_ = closeSession(context.Background())
		return nil, err
	}
	auth := teams.NewAuthManagerWithHTTPClient(cfg, httpClient.Client)
	graph := teams.NewGraphClientWithHTTPClient(auth, io.Discard, httpClient.Client)
	me, err := graph.Me(ctx)
	if err != nil {
		_ = closeSession(context.Background())
		return nil, err
	}
	userID := strings.TrimSpace(me.ID)
	if userID == "" {
		userID = strings.TrimSpace(me.UserPrincipalName)
	}
	if userID == "" {
		_ = closeSession(context.Background())
		return nil, errDelegateMissingTeamsUserID
	}
	cachePath, err := delegateRegistryCachePath(opts.registryCache)
	if err != nil {
		_ = closeSession(context.Background())
		return nil, err
	}
	store := machineregistry.Store{
		Graph:     teams.NewMachineRegistryGraphAdapter(graph),
		CachePath: cachePath,
		Now:       opts.now,
	}
	ensured, err := store.Ensure(ctx, cfg.TenantID, userID)
	if err != nil {
		_ = closeSession(context.Background())
		return nil, err
	}
	return &delegateRegistrySession{
		graph:  graph,
		store:  store,
		cache:  ensured.Cache,
		chatID: ensured.Cache.RegistryChatID,
		close:  closeSession,
	}, nil
}

var errDelegateMissingTeamsUserID = &delegateStaticError{"Teams Graph /me did not include a user id"}

type delegateStaticError struct {
	message string
}

func (e *delegateStaticError) Error() string {
	return e.message
}

func delegateStorePath(override string) (string, error) {
	if strings.TrimSpace(override) != "" {
		return override, nil
	}
	return appdirs.StatePath("delegation", "state.sqlite")
}

func delegateRegistryCachePath(override string) (string, error) {
	if strings.TrimSpace(override) != "" {
		return override, nil
	}
	return machineregistry.DefaultCachePath()
}

func runDelegateStartRegistry(opts *delegateOptions, record delegation.Record, now time.Time) (delegateStartResult, error) {
	session, err := openDelegateRegistry(opts)
	if err != nil {
		return delegateStartResult{}, err
	}
	defer session.close(opts.context())
	records, err := readDelegateRegistryRecords(opts, session)
	if err != nil {
		return delegateStartResult{}, err
	}
	idempotent := false
	for _, existing := range records {
		if existing.Kind == delegation.RequestKind && existing.SourceKey == record.SourceKey {
			record = existing
			idempotent = true
			break
		}
	}
	if !idempotent {
		if _, err := session.graph.SendHTML(opts.context(), session.chatID, delegation.RenderRecordHTML(record)); err != nil {
			return delegateStartResult{}, err
		}
		records = append(records, record)
	}
	state := delegation.Reduce(delegation.RecordsForID(records, record.DelegationID), now)
	return delegateStartResult{
		DelegationID: record.DelegationID,
		Status:       state.Status,
		MachineID:    record.MachineID,
		Idempotent:   idempotent,
		State:        state,
		Transport:    "registry",
	}, nil
}

func runDelegateStartInbox(opts *delegateOptions, token delegation.CandidateTokenPayload, record delegation.Record, now time.Time) (delegateStartResult, error) {
	if strings.TrimSpace(token.InboxRef) == "" {
		return delegateStartResult{}, fmt.Errorf("candidate token missing inbox_ref; run delegate resolve again with an active machine registry")
	}
	session, err := openDelegateRegistry(opts)
	if err != nil {
		return delegateStartResult{}, err
	}
	defer session.close(opts.context())
	if err := validateDelegateCandidateToken(opts, session, token); err != nil {
		return delegateStartResult{}, err
	}
	chatID, err := openDelegateInboxRef(opts, session, token.InboxRef)
	if err != nil {
		return delegateStartResult{}, err
	}
	records, err := readDelegateInboxRecords(opts, session, chatID)
	if err != nil {
		return delegateStartResult{}, err
	}
	idempotent := false
	for _, existing := range records {
		if existing.Kind == delegation.RequestKind && existing.SourceKey == record.SourceKey {
			record = existing
			idempotent = true
			break
		}
	}
	if !idempotent {
		var nextRecords []delegation.Record
		nextRecords, err = appendDelegateInboxRecordWithOutbox(opts, session, chatID, token.InboxRef, record)
		if err != nil {
			return delegateStartResult{}, err
		}
		records = nextRecords
	}
	if err := saveDelegateRoute(opts, delegation.Route{
		DelegationID:     record.DelegationID,
		SourceKey:        record.SourceKey,
		MachineID:        record.MachineID,
		InboxRef:         token.InboxRef,
		InboxGeneration:  token.InboxGeneration,
		RemoteThreadID:   record.RemoteThreadID,
		ThreadPolicy:     record.ThreadPolicy,
		ThreadGeneration: record.ThreadGeneration,
		CreatedAt:        record.CreatedAt,
		UpdatedAt:        now.UTC().Format(time.RFC3339Nano),
	}); err != nil {
		return delegateStartResult{}, err
	}
	if strings.TrimSpace(record.RemoteThreadID) != "" {
		if err := saveDelegateStartedRemoteThread(opts, record, now); err != nil {
			return delegateStartResult{}, err
		}
	}
	state := delegation.Reduce(delegation.RecordsForID(records, record.DelegationID), now)
	return delegateStartResult{
		DelegationID:   record.DelegationID,
		Status:         state.Status,
		MachineID:      record.MachineID,
		RemoteThreadID: record.RemoteThreadID,
		ThreadPolicy:   record.ThreadPolicy,
		Idempotent:     idempotent,
		State:          state,
		Transport:      "inbox",
	}, nil
}

func validateDelegateCandidateToken(opts *delegateOptions, session *delegateRegistrySession, token delegation.CandidateTokenPayload) error {
	if session == nil || session.store.Graph == nil {
		return nil
	}
	statuses, err := session.store.Observe(opts.context(), session.cache, opts.top, opts.now().UTC())
	if err != nil {
		return err
	}
	for _, status := range statuses {
		if strings.TrimSpace(status.MachineID) != strings.TrimSpace(token.MachineID) {
			continue
		}
		if status.State != "online" || !status.Accepting {
			return fmt.Errorf("candidate machine %s is no longer online and accepting; run delegate resolve again", token.MachineID)
		}
		if strings.TrimSpace(token.InboxGeneration) != "" && strings.TrimSpace(status.InboxGeneration) != strings.TrimSpace(token.InboxGeneration) {
			return fmt.Errorf("candidate machine %s inbox generation changed from %q to %q; run delegate resolve again", token.MachineID, token.InboxGeneration, status.InboxGeneration)
		}
		if strings.TrimSpace(token.CapabilityFingerprint) != "" && strings.TrimSpace(status.CapabilityFingerprint) != strings.TrimSpace(token.CapabilityFingerprint) {
			return fmt.Errorf("candidate machine %s capabilities changed; run delegate resolve again", token.MachineID)
		}
		if token.CardRevision != 0 && status.Revision != 0 && status.Revision < token.CardRevision {
			return fmt.Errorf("candidate machine %s registry card moved backwards; run delegate resolve again", token.MachineID)
		}
		return nil
	}
	return fmt.Errorf("candidate machine %s is no longer present in the registry; run delegate resolve again", token.MachineID)
}

func readDelegateRegistryRecordsForID(opts *delegateOptions, delegationID string) ([]delegation.Record, error) {
	session, err := openDelegateRegistry(opts)
	if err != nil {
		return nil, err
	}
	defer session.close(opts.context())
	records, err := readDelegateRegistryRecords(opts, session)
	if err != nil {
		return nil, err
	}
	return delegation.RecordsForID(records, delegationID), nil
}

func readDelegateInboxRecordsForID(opts *delegateOptions, delegationID string) ([]delegation.Record, error) {
	session, route, chatID, err := openDelegateInboxForID(opts, delegationID)
	if err != nil {
		return nil, err
	}
	defer session.close(opts.context())
	records, err := readDelegateInboxRecords(opts, session, chatID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(route.InboxGeneration) != "" {
		for _, record := range records {
			if record.DelegationID == delegationID && strings.TrimSpace(record.InboxGeneration) != "" && record.InboxGeneration != route.InboxGeneration {
				return nil, fmt.Errorf("delegation %s uses stale inbox generation %q, current route is %q", delegationID, record.InboxGeneration, route.InboxGeneration)
			}
		}
	}
	return delegation.RecordsForID(records, delegationID), nil
}

func appendDelegateRegistryRecord(opts *delegateOptions, record delegation.Record) ([]delegation.Record, error) {
	session, err := openDelegateRegistry(opts)
	if err != nil {
		return nil, err
	}
	defer session.close(opts.context())
	records, err := readDelegateRegistryRecords(opts, session)
	if err != nil {
		return nil, err
	}
	if _, err := session.graph.SendHTML(opts.context(), session.chatID, delegation.RenderRecordHTML(record)); err != nil {
		return nil, err
	}
	return append(records, record), nil
}

func appendDelegateInboxRecordForID(opts *delegateOptions, delegationID string, record delegation.Record) ([]delegation.Record, error) {
	session, route, chatID, err := openDelegateInboxForID(opts, delegationID)
	if err != nil {
		return nil, err
	}
	defer session.close(opts.context())
	return appendDelegateInboxRecordWithOutbox(opts, session, chatID, route.InboxRef, record)
}

func appendDelegateInboxRecordForClaim(opts *delegateOptions, record delegation.Record) ([]delegation.Record, error) {
	session, route, chatID, err := openDelegateInboxForClaim(opts)
	if err != nil {
		return nil, err
	}
	defer session.close(opts.context())
	if strings.TrimSpace(route.InboxGeneration) != "" {
		record.InboxRef = route.InboxRef
		record.InboxGeneration = route.InboxGeneration
	}
	return appendDelegateInboxRecordWithOutbox(opts, session, chatID, route.InboxRef, record)
}

func appendDelegateInboxRecordWithOutbox(opts *delegateOptions, session *delegateRegistrySession, chatID string, inboxRef string, record delegation.Record) ([]delegation.Record, error) {
	if err := saveDelegateOutbox(opts, record, delegation.OutboxPending, chatID, inboxRef, "", ""); err != nil {
		return nil, err
	}
	msg, sendErr := session.graph.SendHTML(opts.context(), chatID, delegation.RenderRecordHTML(record))
	if sendErr != nil {
		records, readErr := readDelegateInboxRecords(opts, session, chatID)
		if readErr == nil && containsDelegateRecordID(records, record.RecordID) {
			if err := saveDelegateOutbox(opts, record, delegation.OutboxVisible, chatID, inboxRef, "", ""); err != nil {
				return nil, err
			}
			return records, nil
		}
		_ = saveDelegateOutbox(opts, record, delegation.OutboxFailed, chatID, inboxRef, "", sendErr.Error())
		if readErr != nil {
			return nil, fmt.Errorf("send delegation record %s: %w; visibility check failed: %v", record.RecordID, sendErr, readErr)
		}
		return nil, sendErr
	}
	records, err := readDelegateInboxRecords(opts, session, chatID)
	if err != nil {
		_ = saveDelegateOutbox(opts, record, delegation.OutboxSent, chatID, inboxRef, msg.ID, "")
		return nil, err
	}
	if !containsDelegateRecordID(records, record.RecordID) {
		_ = saveDelegateOutbox(opts, record, delegation.OutboxFailed, chatID, inboxRef, msg.ID, "sent record was not visible in inbox reread")
		return nil, fmt.Errorf("delegation record %s sent as message %s but was not visible in inbox reread", record.RecordID, msg.ID)
	}
	if err := saveDelegateOutbox(opts, record, delegation.OutboxVisible, chatID, inboxRef, msg.ID, ""); err != nil {
		return nil, err
	}
	return records, nil
}

func saveDelegateOutbox(opts *delegateOptions, record delegation.Record, status string, chatID string, inboxRef string, messageID string, errText string) error {
	path, err := delegateRouteStorePath(opts)
	if err != nil {
		return err
	}
	if delegation.StorePathUsesSQLite(path) {
		return delegation.UpsertOutboxSQLite(path, record, status, chatID, inboxRef, messageID, errText, opts.now().UTC(), delegation.DefaultStoreRetention)
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return err
	}
	now := opts.now().UTC().Format(time.RFC3339Nano)
	existing, _ := store.OutboxForRecordID(record.RecordID)
	attempts := existing.Attempts
	if status == delegation.OutboxPending {
		attempts++
	}
	createdAt := existing.CreatedAt
	if strings.TrimSpace(createdAt) == "" {
		createdAt = now
	}
	store.UpsertOutbox(delegation.OutboxRecord{
		RecordID:     record.RecordID,
		DelegationID: record.DelegationID,
		ChatID:       strings.TrimSpace(chatID),
		InboxRef:     strings.TrimSpace(inboxRef),
		Status:       strings.TrimSpace(status),
		MessageID:    strings.TrimSpace(firstNonEmptyString(messageID, existing.MessageID)),
		Attempts:     attempts,
		Error:        strings.TrimSpace(errText),
		CreatedAt:    createdAt,
		UpdatedAt:    now,
	})
	store.Prune(opts.now().UTC(), delegation.DefaultStoreRetention)
	_, err = delegation.SaveStore(path, store)
	return err
}

func containsDelegateRecordID(records []delegation.Record, recordID string) bool {
	recordID = strings.TrimSpace(recordID)
	for _, record := range records {
		if strings.TrimSpace(record.RecordID) == recordID {
			return true
		}
	}
	return false
}

func readDelegateRegistryRecords(opts *delegateOptions, session *delegateRegistrySession) ([]delegation.Record, error) {
	top := opts.top
	if top <= 0 {
		top = 100
	}
	if windowLister, ok := session.graph.(delegateRegistryRecordWindowLister); ok {
		var messages []teams.ChatMessage
		nextPath := ""
		for page := 0; page < 20; page++ {
			var (
				window teams.MessageWindow
				err    error
			)
			if page == 0 {
				window, err = windowLister.ListMessagesWindow(opts.context(), session.chatID, top, time.Time{})
			} else {
				window, err = windowLister.ListMessagesWindowFromPath(opts.context(), nextPath)
			}
			if err != nil {
				return nil, err
			}
			messages = append(messages, window.Messages...)
			if !window.Truncated || strings.TrimSpace(window.NextPath) == "" {
				return delegation.ObserveRecords(delegateMessages(messages)), nil
			}
			nextPath = window.NextPath
		}
		return delegation.ObserveRecords(delegateMessages(messages)), nil
	}
	messages, err := session.graph.ListMessages(opts.context(), session.chatID, top)
	if err != nil {
		return nil, err
	}
	return delegation.ObserveRecords(delegateMessages(messages)), nil
}

func readDelegateInboxRecords(opts *delegateOptions, session *delegateRegistrySession, chatID string) ([]delegation.Record, error) {
	top := opts.top
	if top <= 0 {
		top = 50
	}
	if windowLister, ok := session.graph.(delegateRegistryRecordWindowLister); ok {
		var messages []teams.ChatMessage
		nextPath := ""
		for page := 0; page < 20; page++ {
			var (
				window teams.MessageWindow
				err    error
			)
			if page == 0 {
				window, err = windowLister.ListMessagesWindow(opts.context(), chatID, top, time.Time{})
			} else {
				window, err = windowLister.ListMessagesWindowFromPath(opts.context(), nextPath)
			}
			if err != nil {
				return nil, err
			}
			messages = append(messages, window.Messages...)
			if !window.Truncated || strings.TrimSpace(window.NextPath) == "" {
				return delegation.ObserveRecords(delegateMessages(messages)), nil
			}
			nextPath = window.NextPath
		}
		return delegation.ObserveRecords(delegateMessages(messages)), nil
	}
	messages, err := session.graph.ListMessages(opts.context(), chatID, top)
	if err != nil {
		return nil, err
	}
	return delegation.ObserveRecords(delegateMessages(messages)), nil
}

func openDelegateInboxForID(opts *delegateOptions, delegationID string) (*delegateRegistrySession, delegation.Route, string, error) {
	route, err := loadDelegateRoute(opts, delegationID)
	if err != nil {
		return nil, delegation.Route{}, "", err
	}
	if strings.TrimSpace(route.InboxRef) == "" {
		return nil, delegation.Route{}, "", fmt.Errorf("delegation %s has no inbox route; start it again with an inbox-backed candidate token", delegationID)
	}
	session, err := openDelegateRegistry(opts)
	if err != nil {
		return nil, delegation.Route{}, "", err
	}
	chatID, err := openDelegateInboxRef(opts, session, route.InboxRef)
	if err != nil {
		_ = session.close(context.Background())
		return nil, delegation.Route{}, "", err
	}
	return session, route, chatID, nil
}

func openDelegateInboxForClaim(opts *delegateOptions) (*delegateRegistrySession, delegation.Route, string, error) {
	if route, err := loadDelegateRoute(opts, opts.delegationID); err == nil && strings.TrimSpace(route.InboxRef) != "" {
		session, err := openDelegateRegistry(opts)
		if err != nil {
			return nil, delegation.Route{}, "", err
		}
		chatID, err := openDelegateInboxRef(opts, session, route.InboxRef)
		if err != nil {
			_ = session.close(context.Background())
			return nil, delegation.Route{}, "", err
		}
		return session, route, chatID, nil
	}
	session, err := openDelegateRegistry(opts)
	if err != nil {
		return nil, delegation.Route{}, "", err
	}
	next, chat, _, _, err := session.store.EnsureInbox(opts.context(), session.cache, opts.machineID)
	if err != nil {
		_ = session.close(context.Background())
		return nil, delegation.Route{}, "", err
	}
	session.cache = next
	route := delegation.Route{
		DelegationID:    opts.delegationID,
		MachineID:       opts.machineID,
		InboxRef:        next.InboxExternalID,
		InboxGeneration: next.InboxGeneration,
		UpdatedAt:       opts.now().UTC().Format(time.RFC3339Nano),
	}
	return session, route, strings.TrimSpace(chat.ID), nil
}

func openDelegateInboxRef(opts *delegateOptions, session *delegateRegistrySession, inboxRef string) (string, error) {
	now := opts.now().UTC()
	chat, _, err := session.graph.CreateOrGetMeetingChatWindow(opts.context(), "CXP Machine Inbox", strings.TrimSpace(inboxRef), now.Add(-5*time.Minute), now.Add(machineregistry.DefaultWindowDuration))
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(chat.ID) == "" {
		return "", fmt.Errorf("delegation inbox chat id is empty")
	}
	return strings.TrimSpace(chat.ID), nil
}

func loadDelegateRoute(opts *delegateOptions, delegationID string) (delegation.Route, error) {
	path, err := delegateRouteStorePath(opts)
	if err != nil {
		return delegation.Route{}, err
	}
	if delegation.StorePathUsesSQLite(path) {
		route, ok, err := delegation.RouteSQLite(path, delegationID)
		if err != nil {
			return delegation.Route{}, err
		}
		if !ok {
			return delegation.Route{}, fmt.Errorf("delegation %s route not found in %s", delegationID, path)
		}
		return route, nil
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return delegation.Route{}, err
	}
	route, ok := store.RouteForID(delegationID)
	if !ok {
		return delegation.Route{}, fmt.Errorf("delegation %s route not found in %s", delegationID, path)
	}
	return route, nil
}

func saveDelegateRoute(opts *delegateOptions, route delegation.Route) error {
	path, err := delegateRouteStorePath(opts)
	if err != nil {
		return err
	}
	if delegation.StorePathUsesSQLite(path) {
		return delegation.UpsertRouteSQLite(path, route)
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return err
	}
	store.UpsertRoute(route)
	store.Prune(opts.now().UTC(), delegation.DefaultStoreRetention)
	_, err = delegation.SaveStore(path, store)
	return err
}

func loadDelegateRouteStore(opts *delegateOptions) (delegation.Store, error) {
	path, err := delegateRouteStorePath(opts)
	if err != nil {
		return delegation.Store{}, err
	}
	return delegation.LoadStore(path)
}

func resolveDelegateStartThread(opts *delegateOptions, candidate delegation.CandidateTokenPayload, spec delegation.TaskSpec, now time.Time) (delegation.ThreadTokenPayload, bool, error) {
	_ = spec
	if strings.TrimSpace(opts.threadToken) != "" && strings.TrimSpace(opts.newThreadToken) != "" {
		return delegation.ThreadTokenPayload{}, false, fmt.Errorf("use either --thread-token or --new-thread-token, not both")
	}
	raw := strings.TrimSpace(firstNonEmptyString(opts.threadToken, opts.newThreadToken))
	if raw == "" {
		return delegation.ThreadTokenPayload{}, false, nil
	}
	payload, err := delegation.DecodeThreadToken(raw, now)
	if err != nil {
		return delegation.ThreadTokenPayload{}, false, err
	}
	if strings.TrimSpace(payload.MachineID) != strings.TrimSpace(candidate.MachineID) {
		return delegation.ThreadTokenPayload{}, false, fmt.Errorf("thread token machine %s does not match candidate machine %s", payload.MachineID, candidate.MachineID)
	}
	if strings.TrimSpace(payload.InboxGeneration) != "" && strings.TrimSpace(candidate.InboxGeneration) != "" && strings.TrimSpace(payload.InboxGeneration) != strings.TrimSpace(candidate.InboxGeneration) {
		return delegation.ThreadTokenPayload{}, false, fmt.Errorf("thread token inbox generation changed from %q to %q; run delegate resolve again", payload.InboxGeneration, candidate.InboxGeneration)
	}
	if strings.TrimSpace(opts.workspaceFingerprint) != "" && strings.TrimSpace(payload.WorkspaceFingerprint) != "" && strings.TrimSpace(opts.workspaceFingerprint) != strings.TrimSpace(payload.WorkspaceFingerprint) {
		return delegation.ThreadTokenPayload{}, false, fmt.Errorf("thread token workspace %q does not match requested workspace %q", payload.WorkspaceFingerprint, opts.workspaceFingerprint)
	}
	if payload.Policy == delegation.ThreadPolicyReuse {
		store, err := loadDelegateRouteStore(opts)
		if err != nil {
			return delegation.ThreadTokenPayload{}, false, err
		}
		thread, ok := store.RemoteThreadForID(payload.ThreadID)
		if !ok {
			return delegation.ThreadTokenPayload{}, false, fmt.Errorf("remote thread %s is not known locally; run delegate resolve again or start a new thread", payload.ThreadID)
		}
		if err := validateReusableThread(thread, candidate.MachineID, opts, now); err != nil {
			return delegation.ThreadTokenPayload{}, false, err
		}
		if strings.TrimSpace(payload.ThreadGeneration) != "" && strings.TrimSpace(thread.Generation) != strings.TrimSpace(payload.ThreadGeneration) {
			return delegation.ThreadTokenPayload{}, false, fmt.Errorf("remote thread %s generation changed; run delegate resolve again", payload.ThreadID)
		}
	}
	return payload, true, nil
}

func upsertStartedRemoteThread(store *delegation.Store, record delegation.Record, payload delegation.ThreadTokenPayload, now time.Time) {
	if store == nil || strings.TrimSpace(record.RemoteThreadID) == "" {
		return
	}
	existing, _ := store.RemoteThreadForID(record.RemoteThreadID)
	thread := existing
	thread.ThreadID = strings.TrimSpace(record.RemoteThreadID)
	thread.MachineID = strings.TrimSpace(record.MachineID)
	thread.SourceSessionID = strings.TrimSpace(firstNonEmptyString(record.SourceSessionID, payload.SourceSessionID, existing.SourceSessionID))
	thread.WorkspaceFingerprint = strings.TrimSpace(firstNonEmptyString(payload.WorkspaceFingerprint, existing.WorkspaceFingerprint))
	thread.State = delegation.RemoteThreadStateActive
	thread.ActiveDelegationID = strings.TrimSpace(record.DelegationID)
	thread.Generation = strings.TrimSpace(firstNonEmptyString(payload.ThreadGeneration, existing.Generation, delegation.NewThreadGeneration(record.RemoteThreadID, now)))
	thread.Title = firstNonEmptyString(existing.Title, record.Spec.Title, record.Spec.Objective)
	thread.LastTerminalRecordID = ""
	thread.UpdatedAt = now.UTC().Format(time.RFC3339Nano)
	thread.LastUsedAt = thread.UpdatedAt
	if strings.TrimSpace(thread.CreatedAt) == "" {
		thread.CreatedAt = thread.UpdatedAt
	}
	if strings.TrimSpace(thread.ExpiresAt) == "" {
		thread.ExpiresAt = now.Add(delegation.DefaultStoreRetention).UTC().Format(time.RFC3339Nano)
	}
	store.UpsertRemoteThread(thread)
}

func saveDelegateStartedRemoteThread(opts *delegateOptions, record delegation.Record, now time.Time) error {
	path, err := delegateRouteStorePath(opts)
	if err != nil {
		return err
	}
	if delegation.StorePathUsesSQLite(path) {
		store := delegation.Store{}
		if existing, ok, err := delegation.RemoteThreadSQLite(path, record.RemoteThreadID); err != nil {
			return err
		} else if ok {
			store.UpsertRemoteThread(existing)
		}
		upsertStartedRemoteThread(&store, record, delegation.ThreadTokenPayload{
			Policy:           record.ThreadPolicy,
			ThreadID:         record.RemoteThreadID,
			MachineID:        record.MachineID,
			ThreadGeneration: record.ThreadGeneration,
		}, now)
		thread, ok := store.RemoteThreadForID(record.RemoteThreadID)
		if !ok {
			return nil
		}
		return delegation.UpsertRemoteThreadSQLite(path, thread)
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return err
	}
	upsertStartedRemoteThread(&store, record, delegation.ThreadTokenPayload{
		Policy:           record.ThreadPolicy,
		ThreadID:         record.RemoteThreadID,
		MachineID:        record.MachineID,
		ThreadGeneration: record.ThreadGeneration,
	}, now)
	store.Prune(now, delegation.DefaultStoreRetention)
	_, err = delegation.SaveStore(path, store)
	return err
}

func syncDelegateRemoteThreadFromState(opts *delegateOptions, state delegation.State) error {
	if state.Request == nil || strings.TrimSpace(state.Request.RemoteThreadID) == "" {
		return nil
	}
	if !isTerminalDelegationState(state.Status) {
		return nil
	}
	path, err := delegateRouteStorePath(opts)
	if err != nil {
		return err
	}
	if delegation.StorePathUsesSQLite(path) {
		thread, _, err := delegation.RemoteThreadSQLite(path, state.Request.RemoteThreadID)
		if err != nil {
			return err
		}
		terminalRecordID := ""
		if state.Terminal != nil {
			terminalRecordID = strings.TrimSpace(state.Terminal.RecordID)
		}
		if terminalRecordID != "" &&
			strings.TrimSpace(thread.LastTerminalRecordID) == terminalRecordID &&
			thread.State == delegation.RemoteThreadStateIdle &&
			strings.TrimSpace(thread.ActiveDelegationID) == "" {
			return nil
		}
		now := opts.now().UTC()
		thread.ThreadID = strings.TrimSpace(state.Request.RemoteThreadID)
		thread.MachineID = strings.TrimSpace(state.Request.MachineID)
		thread.SourceSessionID = strings.TrimSpace(firstNonEmptyString(thread.SourceSessionID, state.Request.SourceSessionID))
		thread.State = delegation.RemoteThreadStateIdle
		thread.ActiveDelegationID = ""
		thread.Generation = strings.TrimSpace(firstNonEmptyString(thread.Generation, state.Request.ThreadGeneration, delegation.NewThreadGeneration(thread.ThreadID, now)))
		thread.Title = firstNonEmptyString(thread.Title, state.Request.Spec.Title, state.Request.Spec.Objective)
		thread.UpdatedAt = now.Format(time.RFC3339Nano)
		thread.LastUsedAt = thread.UpdatedAt
		if strings.TrimSpace(thread.CreatedAt) == "" {
			thread.CreatedAt = state.Request.CreatedAt
		}
		if state.Terminal != nil {
			applyThreadUpdateFromTerminal(&thread, *state.Terminal)
			thread.LastTerminalRecordID = strings.TrimSpace(state.Terminal.RecordID)
		}
		if strings.TrimSpace(thread.ExpiresAt) == "" {
			thread.ExpiresAt = now.Add(delegation.DefaultStoreRetention).UTC().Format(time.RFC3339Nano)
		}
		return delegation.UpsertRemoteThreadSQLite(path, thread)
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return err
	}
	thread, _ := store.RemoteThreadForID(state.Request.RemoteThreadID)
	terminalRecordID := ""
	if state.Terminal != nil {
		terminalRecordID = strings.TrimSpace(state.Terminal.RecordID)
	}
	if terminalRecordID != "" &&
		strings.TrimSpace(thread.LastTerminalRecordID) == terminalRecordID &&
		thread.State == delegation.RemoteThreadStateIdle &&
		strings.TrimSpace(thread.ActiveDelegationID) == "" {
		return nil
	}
	now := opts.now().UTC()
	thread.ThreadID = strings.TrimSpace(state.Request.RemoteThreadID)
	thread.MachineID = strings.TrimSpace(state.Request.MachineID)
	thread.SourceSessionID = strings.TrimSpace(firstNonEmptyString(thread.SourceSessionID, state.Request.SourceSessionID))
	thread.State = delegation.RemoteThreadStateIdle
	thread.ActiveDelegationID = ""
	thread.Generation = strings.TrimSpace(firstNonEmptyString(thread.Generation, state.Request.ThreadGeneration, delegation.NewThreadGeneration(thread.ThreadID, now)))
	thread.Title = firstNonEmptyString(thread.Title, state.Request.Spec.Title, state.Request.Spec.Objective)
	thread.UpdatedAt = now.Format(time.RFC3339Nano)
	thread.LastUsedAt = thread.UpdatedAt
	if strings.TrimSpace(thread.CreatedAt) == "" {
		thread.CreatedAt = state.Request.CreatedAt
	}
	if state.Terminal != nil {
		applyThreadUpdateFromTerminal(&thread, *state.Terminal)
		thread.LastTerminalRecordID = strings.TrimSpace(state.Terminal.RecordID)
	}
	if strings.TrimSpace(thread.ExpiresAt) == "" {
		thread.ExpiresAt = now.Add(delegation.DefaultStoreRetention).UTC().Format(time.RFC3339Nano)
	}
	store.UpsertRemoteThread(thread)
	store.Prune(now, delegation.DefaultStoreRetention)
	_, err = delegation.SaveStore(path, store)
	return err
}

func applyThreadUpdateFromTerminal(thread *delegation.RemoteThread, terminal delegation.Record) {
	if thread == nil {
		return
	}
	if terminal.Status == delegation.StateReuseRejected {
		thread.LastResultSummary = truncateForDelegateThread(terminal.Body)
		return
	}
	if terminal.ThreadUpdate != nil {
		if strings.TrimSpace(terminal.ThreadUpdate.Title) != "" {
			thread.Title = terminal.ThreadUpdate.Title
		}
		if strings.TrimSpace(terminal.ThreadUpdate.Summary) != "" {
			thread.Summary = terminal.ThreadUpdate.Summary
		} else if strings.TrimSpace(terminal.ThreadUpdate.SummaryDelta) != "" {
			thread.Summary = firstNonEmptyString(thread.Summary, terminal.ThreadUpdate.SummaryDelta)
			if !strings.Contains(thread.Summary, terminal.ThreadUpdate.SummaryDelta) {
				thread.Summary = strings.TrimSpace(thread.Summary + "\n" + terminal.ThreadUpdate.SummaryDelta)
			}
		}
		if strings.TrimSpace(terminal.ThreadUpdate.LastResultSummary) != "" {
			thread.LastResultSummary = terminal.ThreadUpdate.LastResultSummary
		}
	}
	if strings.TrimSpace(thread.LastResultSummary) == "" {
		thread.LastResultSummary = truncateForDelegateThread(terminal.Body)
	}
}

func reusableThreadCandidates(store delegation.Store, candidate delegation.Candidate, opts *delegateOptions, now time.Time, limit int) []delegation.RemoteThreadCandidate {
	if limit <= 0 {
		limit = 3
	}
	threads := store.RemoteThreadsForMachine(candidate.MachineID)
	out := make([]delegation.RemoteThreadCandidate, 0, limit)
	for _, thread := range threads {
		if err := validateReusableThread(thread, candidate.MachineID, opts, now); err != nil {
			continue
		}
		score, reasons := scoreReusableThread(thread, opts, now)
		token, payload, err := delegation.NewThreadTokenForThread(thread, now, delegation.DefaultThreadTokenTTL)
		if err != nil {
			continue
		}
		out = append(out, delegation.RemoteThreadCandidate{
			ThreadID:             thread.ThreadID,
			MachineID:            thread.MachineID,
			Title:                thread.Title,
			Summary:              thread.Summary,
			LastResultSummary:    thread.LastResultSummary,
			WorkspaceFingerprint: thread.WorkspaceFingerprint,
			SourceSessionID:      thread.SourceSessionID,
			State:                thread.State,
			LastUsedAt:           thread.LastUsedAt,
			ReuseConfidence:      score,
			ReuseReasons:         reasons,
			ThreadToken:          token,
			ValidUntil:           payload.ValidUntil,
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func validateReusableThread(thread delegation.RemoteThread, machineID string, opts *delegateOptions, now time.Time) error {
	if strings.TrimSpace(thread.ThreadID) == "" {
		return fmt.Errorf("remote thread id is empty")
	}
	if strings.TrimSpace(thread.MachineID) != strings.TrimSpace(machineID) {
		return fmt.Errorf("remote thread machine mismatch")
	}
	if thread.State == delegation.RemoteThreadStateClosed || thread.State == delegation.RemoteThreadStateStale {
		return fmt.Errorf("remote thread is %s", thread.State)
	}
	if thread.State == delegation.RemoteThreadStateActive || strings.TrimSpace(thread.ActiveDelegationID) != "" {
		return fmt.Errorf("remote thread is active")
	}
	if strings.TrimSpace(opts.sourceSession) != "" && strings.TrimSpace(thread.SourceSessionID) != "" && strings.TrimSpace(opts.sourceSession) != strings.TrimSpace(thread.SourceSessionID) {
		return fmt.Errorf("remote thread source session mismatch")
	}
	if strings.TrimSpace(opts.workspaceFingerprint) != "" && strings.TrimSpace(thread.WorkspaceFingerprint) != "" && strings.TrimSpace(opts.workspaceFingerprint) != strings.TrimSpace(thread.WorkspaceFingerprint) {
		return fmt.Errorf("remote thread workspace mismatch")
	}
	expiresAt := parseDelegateTime(thread.ExpiresAt)
	if !expiresAt.IsZero() && now.After(expiresAt) {
		return fmt.Errorf("remote thread expired")
	}
	return nil
}

func scoreReusableThread(thread delegation.RemoteThread, opts *delegateOptions, now time.Time) (float64, []string) {
	score := 0.45
	var reasons []string
	if strings.TrimSpace(opts.sourceSession) != "" && strings.TrimSpace(thread.SourceSessionID) == strings.TrimSpace(opts.sourceSession) {
		score += 0.25
		reasons = append(reasons, "same source session")
	}
	if strings.TrimSpace(opts.workspaceFingerprint) != "" && strings.TrimSpace(thread.WorkspaceFingerprint) == strings.TrimSpace(opts.workspaceFingerprint) {
		score += 0.2
		reasons = append(reasons, "same workspace")
	}
	lastUsed := parseDelegateTime(firstNonEmptyString(thread.LastUsedAt, thread.UpdatedAt, thread.CreatedAt))
	if !lastUsed.IsZero() && now.Sub(lastUsed) <= 2*time.Hour {
		score += 0.1
		reasons = append(reasons, "recent remote thread")
	}
	if score > 0.99 {
		score = 0.99
	}
	return score, reasons
}

func parseDelegateTime(value string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	return t
}

func truncateForDelegateThread(value string) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= delegation.MaxRemoteThreadSummaryRunes {
		return value
	}
	return string(runes[:delegation.MaxRemoteThreadSummaryRunes])
}

func delegateWaitShouldReturn(until string, status string) bool {
	if isTerminalDelegationState(status) {
		return true
	}
	switch strings.TrimSpace(until) {
	case "", delegateWaitUntilTerminalOrQuestion:
		return status == delegation.StateQuestion
	case delegateWaitUntilTerminal:
		return false
	default:
		return status == delegation.StateQuestion
	}
}

func delegateRouteStorePath(opts *delegateOptions) (string, error) {
	if opts != nil && strings.TrimSpace(opts.routeStorePath) != "" {
		return opts.routeStorePath, nil
	}
	if opts != nil && strings.TrimSpace(opts.storePath) != "" {
		return delegateStorePath(opts.storePath)
	}
	return delegateStorePath("")
}

func delegateMessages(messages []teams.ChatMessage) []delegation.ChatMessage {
	out := make([]delegation.ChatMessage, 0, len(messages))
	for _, msg := range messages {
		out = append(out, delegation.ChatMessage{
			ID: msg.ID,
			Body: delegation.ChatMessageBody{
				Content: msg.Body.Content,
			},
		})
	}
	return out
}

func appendDelegateLocalRecord(opts *delegateOptions, record delegation.Record) ([]delegation.Record, error) {
	path, err := delegateStorePath(opts.storePath)
	if err != nil {
		return nil, err
	}
	store, err := delegation.LoadStore(path)
	if err != nil {
		return nil, err
	}
	store.Records = append(store.Records, record)
	store.Prune(opts.now().UTC(), delegation.DefaultStoreRetention)
	if _, err := delegation.SaveStore(path, store); err != nil {
		return nil, err
	}
	return store.Records, nil
}

func buildDelegateMachineCard(opts *delegateOptions, cache machineregistry.Cache) (machineregistry.MachineCard, error) {
	now := opts.now().UTC()
	machineID := strings.TrimSpace(opts.machineID)
	if machineID == "" {
		host, _ := os.Hostname()
		machineID = strings.TrimSpace(host)
	}
	if machineID == "" {
		return machineregistry.MachineCard{}, fmt.Errorf("machine id is required")
	}
	heartbeat := opts.heartbeat
	if heartbeat <= 0 {
		heartbeat = machineregistry.DefaultHeartbeatInterval
	}
	ttl := opts.ttl
	if ttl <= 0 {
		ttl = machineregistry.DefaultOnlineTTL
	}
	if ttl <= heartbeat {
		return machineregistry.MachineCard{}, fmt.Errorf("ttl must be greater than heartbeat")
	}
	capabilities := append([]string(nil), opts.capabilities...)
	if len(capabilities) == 0 {
		capabilities = []string{"cxp", "codex", "teams-registry"}
	}
	label := strings.TrimSpace(opts.machineLabel)
	if label == "" {
		label = machineID
	}
	return machineregistry.MachineCard{
		Kind:                     machineregistry.CardKind,
		RegistryKey:              cache.RegistryKey,
		MachineID:                machineID,
		InstanceID:               "manual_" + machineID,
		MachineLabel:             label,
		HostLabel:                label,
		Aliases:                  compactStrings(opts.aliases),
		Platform:                 machineregistry.MachinePlatform{OS: runtime.GOOS, Arch: runtime.GOARCH},
		Capabilities:             compactStrings(capabilities),
		CapabilityFingerprint:    machineregistry.CapabilityFingerprint(capabilities),
		ProtocolVersions:         []string{"cxp-delegation-v1"},
		InboxRef:                 strings.TrimSpace(cache.InboxExternalID),
		InboxGeneration:          strings.TrimSpace(cache.InboxGeneration),
		Accepting:                opts.accepting && !opts.draining,
		Draining:                 opts.draining,
		Sequence:                 int(now.Unix()),
		Revision:                 int(now.Unix()),
		HeartbeatIntervalSeconds: int(heartbeat.Seconds()),
		TTLSeconds:               int(ttl.Seconds()),
		PublishedAt:              now.Format(time.RFC3339Nano),
		ExpiresAt:                now.Add(ttl).Format(time.RFC3339Nano),
	}, nil
}

func compactStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func printDelegateJSON(cmd *cobra.Command, opts *delegateOptions, value any) error {
	if !opts.jsonOutput {
		opts.jsonOutput = true
	}
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func recordsForID(records []delegation.Record, id string) []delegation.Record {
	id = strings.TrimSpace(id)
	out := make([]delegation.Record, 0, len(records))
	for _, record := range records {
		if record.DelegationID == id {
			out = append(out, record)
		}
	}
	return out
}

func isTerminalDelegationState(status string) bool {
	switch status {
	case delegation.StateComplete, delegation.StateBlocked, delegation.StateCanceled, delegation.StateExpired, delegation.StateConflict, delegation.StateReuseRejected:
		return true
	default:
		return false
	}
}

func scoreCandidate(query string, candidate *delegation.Candidate) {
	query = strings.ToLower(query)
	if candidate.Confidence > 0 {
		return
	}
	if query == "" {
		return
	}
	var score float64
	var reasons []string
	needles := append([]string{candidate.MachineID, candidate.Label, candidate.HostLabel}, candidate.Aliases...)
	for _, needle := range needles {
		needle = strings.ToLower(strings.TrimSpace(needle))
		if needle != "" && strings.Contains(query, needle) {
			score = 0.95
			reasons = append(reasons, "query matched machine alias or label")
			break
		}
	}
	if score == 0 {
		haystack := strings.ToLower(strings.Join(append(append([]string{candidate.Label, candidate.HostLabel}, candidate.Aliases...), append(candidate.Capabilities, candidate.Skills...)...), " "))
		for _, item := range append(candidate.Capabilities, candidate.Skills...) {
			item = strings.ToLower(strings.TrimSpace(item))
			if item != "" && strings.Contains(query, item) {
				score = 0.86
				reasons = append(reasons, "query matched machine capability or skill")
				break
			}
		}
		if score == 0 && strings.Contains(query, "gpu") && strings.Contains(haystack, "gpu") {
			score = 0.82
			reasons = append(reasons, "query matched GPU hint")
		}
	}
	candidate.Confidence = score
	candidate.MatchedReasons = append(candidate.MatchedReasons, reasons...)
}

func sortCandidates(candidates []delegation.Candidate) {
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].Confidence > candidates[i].Confidence ||
				(candidates[j].Confidence == candidates[i].Confidence && candidates[j].MachineID < candidates[i].MachineID) {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}
}

func appendMissing(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func candidatesFromMachineStatuses(statuses []machineregistry.MachineStatus) []delegation.Candidate {
	out := make([]delegation.Candidate, 0, len(statuses))
	for _, status := range statuses {
		candidate := delegation.Candidate{
			MachineID:             status.MachineID,
			InstanceID:            status.InstanceID,
			Label:                 status.MachineLabel,
			HostLabel:             status.HostLabel,
			Aliases:               append([]string(nil), status.Aliases...),
			State:                 status.State,
			Accepting:             status.Accepting && !status.Draining,
			InboxRef:              status.InboxRef,
			InboxGeneration:       status.InboxGeneration,
			CardRevision:          status.Revision,
			CapabilityFingerprint: status.CapabilityFingerprint,
			Capabilities:          append([]string(nil), status.Capabilities...),
			Skills:                append([]string(nil), status.Skills...),
			ProtocolVersions:      append([]string(nil), status.ProtocolVersions...),
			Confidence:            0,
			MatchedReasons:        nil,
		}
		if status.State != "online" {
			candidate.NotStartableReasons = append(candidate.NotStartableReasons, "machine is not online")
		}
		if !candidate.Accepting {
			candidate.NotStartableReasons = append(candidate.NotStartableReasons, "machine is not accepting delegation")
		}
		out = append(out, candidate)
	}
	return out
}

func (o *delegateOptions) context() context.Context {
	if o != nil && o.ctx != nil {
		return o.ctx
	}
	return context.Background()
}
