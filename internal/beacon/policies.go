package beacon

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func IdempotencyKey(messageID, normalizedCommand, confirmationToken string) string {
	return strings.TrimSpace(messageID) + "\x00" + strings.TrimSpace(normalizedCommand) + "\x00" + strings.TrimSpace(confirmationToken)
}

func ApplyIdempotent(st *State, messageID, normalizedCommand, confirmationToken string, now time.Time, fn func() (string, error)) (string, bool, error) {
	if st == nil {
		return "", false, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	key := IdempotencyKey(messageID, normalizedCommand, confirmationToken)
	if rec, ok := st.Idempotency[key]; ok {
		return rec.Result, false, nil
	}
	result, err := fn()
	if err != nil {
		return "", false, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	st.Idempotency[key] = IdempotencyRecord{Key: key, Result: result, CreatedAt: now}
	return result, true, nil
}

type ReleaseInput struct {
	MachineID     string
	LeaseID       string
	JobID         string
	HardKill      bool
	ExactID       string
	ConfirmToken  string
	ProvidedToken string
}

type ReleasePreview struct {
	MachineID     string
	LeaseID       string
	ProviderJobID string
	Chats         []string
	Jobs          []string
	Confirmation  string
	ExternalOwned bool
}

type ReleaseResult struct {
	Action  string
	Preview ReleasePreview
}

type AllocationReleasePreview struct {
	Scope                string
	Ref                  string
	Force                bool
	Allocations          []AllocationReleaseItem
	AffectedChats        []string
	QueuedTurns          []string
	RunningTurns         []string
	RequiresConfirmation bool
	Confirmation         string
}

type AllocationReleaseItem struct {
	AllocationID string
	Profile      string
	State        AllocationState
	Provider     Provider
	ProviderJob  string
	Action       string
	Machines     []string
	Chats        []string
	QueuedTurns  []string
	RunningTurns []string
}

type ConversationDetachResult struct {
	ConversationID string
	Detached       []string
	Draining       []string
	Escalated      []string
	Skipped        []string
}

func PreviewRelease(m Machine) ReleasePreview {
	token := "KILL-" + strings.TrimSpace(m.LeaseID)
	return ReleasePreview{
		MachineID:     m.ID,
		LeaseID:       m.LeaseID,
		ProviderJobID: m.ProviderJobID,
		Chats:         append([]string(nil), m.Chats...),
		Jobs:          append([]string(nil), m.Jobs...),
		Confirmation:  token,
		ExternalOwned: m.ExternalOwned,
	}
}

func DecideRelease(m Machine, in ReleaseInput) (ReleaseResult, error) {
	preview := PreviewRelease(m)
	if !in.HardKill {
		if len(m.Jobs) > 0 {
			return ReleaseResult{Action: "drain", Preview: preview}, nil
		}
		return ReleaseResult{Action: "release", Preview: preview}, nil
	}
	if m.ExternalOwned {
		return ReleaseResult{Action: "reject_external", Preview: preview}, nil
	}
	if strings.TrimSpace(in.ExactID) != m.LeaseID && strings.TrimSpace(in.ExactID) != m.ID && strings.TrimSpace(in.ExactID) != strings.TrimSpace(in.JobID) {
		return ReleaseResult{Action: "reject_exact_id", Preview: preview}, nil
	}
	if strings.TrimSpace(in.ProvidedToken) == "" || strings.TrimSpace(in.ProvidedToken) != strings.TrimSpace(in.ConfirmToken) {
		return ReleaseResult{Action: "reject_confirmation", Preview: preview}, nil
	}
	return ReleaseResult{Action: "kill_quarantine", Preview: preview}, nil
}

func PreviewAllocationRelease(st State, scope string, ref string, allocations []AllocationRequest, force bool) AllocationReleasePreview {
	st.normalize()
	preview := AllocationReleasePreview{
		Scope: strings.TrimSpace(scope),
		Ref:   strings.TrimSpace(ref),
		Force: force,
	}
	for _, req := range allocations {
		item := AllocationReleaseItem{
			AllocationID: strings.TrimSpace(req.ID),
			Profile:      strings.TrimSpace(req.Profile),
			State:        req.State,
			Provider:     req.Provider,
			ProviderJob:  strings.TrimSpace(firstNonEmpty(req.ProviderIdentity.ProviderJobID, req.Target.ProviderJobID)),
			Action:       allocationReleasePreviewAction(st, req, force),
		}
		item.Machines = allocationReleaseMachines(st, req)
		item.Chats = allocationReleaseChats(st, req)
		item.QueuedTurns, item.RunningTurns = allocationReleaseTurns(st, req)
		preview.Allocations = append(preview.Allocations, item)
		preview.AffectedChats = append(preview.AffectedChats, item.Chats...)
		preview.QueuedTurns = append(preview.QueuedTurns, item.QueuedTurns...)
		preview.RunningTurns = append(preview.RunningTurns, item.RunningTurns...)
		if force || len(item.Chats) > 1 {
			preview.RequiresConfirmation = true
		}
	}
	preview.AffectedChats = uniqueSortedStrings(preview.AffectedChats)
	preview.QueuedTurns = uniqueSortedStrings(preview.QueuedTurns)
	preview.RunningTurns = uniqueSortedStrings(preview.RunningTurns)
	if len(preview.AffectedChats) > 1 {
		preview.RequiresConfirmation = true
	}
	if preview.RequiresConfirmation {
		preview.Confirmation = AllocationReleaseConfirmationToken(preview)
	}
	return preview
}

func AllocationReleaseConfirmationToken(preview AllocationReleasePreview) string {
	var parts []string
	parts = append(parts,
		"scope="+preview.Scope,
		"ref="+preview.Ref,
		"force="+strconv.FormatBool(preview.Force),
	)
	for i, item := range preview.Allocations {
		prefix := "allocation[" + strconv.Itoa(i) + "]."
		parts = append(parts,
			prefix+"id="+item.AllocationID,
			prefix+"profile="+item.Profile,
			prefix+"state="+string(item.State),
			prefix+"provider="+string(item.Provider),
			prefix+"provider_job="+item.ProviderJob,
			prefix+"action="+item.Action,
			prefix+"machines="+strings.Join(item.Machines, ","),
			prefix+"chats="+strings.Join(item.Chats, ","),
			prefix+"queued="+strings.Join(item.QueuedTurns, ","),
			prefix+"running="+strings.Join(item.RunningTurns, ","),
		)
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "RELEASE-" + strings.ToUpper(fmt.Sprintf("%x", sum[:6]))
}

func allocationReleasePreviewAction(st State, req AllocationRequest, force bool) string {
	if allocationStateTerminal(req.State) {
		return "none"
	}
	if allocationHasActiveStartedJob(st, req.ID) && !force {
		return "drain"
	}
	if strings.TrimSpace(firstNonEmpty(req.ProviderIdentity.ProviderJobID, req.Target.ProviderJobID)) != "" && req.Provider != ProviderLocal {
		return "cancel_provider_job"
	}
	return "cancel_allocation"
}

func allocationReleaseMachines(st State, req AllocationRequest) []string {
	providerJobID, leaseID, ok := allocationMachineMatchKeys(req)
	if !ok {
		return nil
	}
	var out []string
	for _, machine := range st.Machines {
		if strings.TrimSpace(providerJobID) != "" && strings.TrimSpace(machine.ProviderJobID) != strings.TrimSpace(providerJobID) {
			continue
		}
		if strings.TrimSpace(providerJobID) == "" && strings.TrimSpace(machine.LeaseID) != leaseID {
			continue
		}
		out = append(out, firstNonEmpty(machine.ID, machine.LeaseID, machine.ProviderJobID))
	}
	return uniqueSortedStrings(out)
}

func allocationReleaseChats(st State, req AllocationRequest) []string {
	out := []string{strings.TrimSpace(req.ConversationID)}
	providerJobID, leaseID, ok := allocationMachineMatchKeys(req)
	if ok {
		for _, machine := range st.Machines {
			if strings.TrimSpace(providerJobID) != "" && strings.TrimSpace(machine.ProviderJobID) != strings.TrimSpace(providerJobID) {
				continue
			}
			if strings.TrimSpace(providerJobID) == "" && strings.TrimSpace(machine.LeaseID) != leaseID {
				continue
			}
			out = append(out, machine.Chats...)
		}
	}
	return uniqueSortedStrings(out)
}

func allocationReleaseTurns(st State, req AllocationRequest) (queued []string, running []string) {
	for _, job := range st.JobAttempts {
		if strings.TrimSpace(job.RequestID) != strings.TrimSpace(req.ID) {
			continue
		}
		id := firstNonEmpty(job.TurnID, job.ID)
		switch job.Phase {
		case JobQueued, JobClaimed, JobStartIntent:
			queued = append(queued, id)
		case JobStarted, JobAmbiguous:
			running = append(running, id)
		}
	}
	for _, conv := range st.Conversations {
		for _, turn := range conv.Queued {
			if strings.TrimSpace(turn.Snapshot.Profile) == strings.TrimSpace(req.Profile) {
				queued = append(queued, turn.ID)
			}
		}
	}
	return uniqueSortedStrings(queued), uniqueSortedStrings(running)
}

func DetachConversationDemand(st *State, conversationID string, reason string, now time.Time) ConversationDetachResult {
	if st == nil {
		return ConversationDetachResult{}
	}
	st.normalize()
	conversationID = strings.TrimSpace(conversationID)
	if now.IsZero() {
		now = time.Now()
	}
	reason = firstNonEmpty(reason, "detached from shared beacon resource")
	result := ConversationDetachResult{ConversationID: conversationID}
	for id, req := range st.Allocations {
		if strings.TrimSpace(req.ConversationID) != conversationID || allocationStateTerminal(req.State) {
			continue
		}
		if !allocationHasOtherChats(*st, req, conversationID) {
			result.Skipped = append(result.Skipped, id)
			continue
		}
		req.DetachRequestedAt = now
		req.DetachReason = reason
		req.UpdatedAt = now
		if allocationHasActiveStartedJob(*st, req.ID) {
			req.ProviderReason = firstNonEmpty(req.ProviderReason, "release requested while this chat may still have running work; detaching after the turn drains")
			st.Allocations[id] = req
			result.Draining = append(result.Draining, id)
			_, _ = AppendAudit(st, "allocation_detach_drain_started", id, now)
			continue
		}
		tombstoneAllocationJobs(st, req, reason, now)
		req.State = AllocationCanceled
		req.ProviderReason = firstNonEmpty(req.ProviderReason, reason)
		req.DetachRequestedAt = time.Time{}
		req.DetachReason = ""
		st.Allocations[id] = req
		removeConversationFromAllocationMachines(st, req, conversationID, now)
		result.Detached = append(result.Detached, id)
		_, _ = AppendAudit(st, "allocation_detach_chat", id, now)
	}
	sort.Strings(result.Detached)
	sort.Strings(result.Draining)
	sort.Strings(result.Escalated)
	sort.Strings(result.Skipped)
	return result
}

func FinalizeConversationDetachIntents(st *State, now time.Time) ConversationDetachResult {
	if st == nil {
		return ConversationDetachResult{}
	}
	st.normalize()
	if now.IsZero() {
		now = time.Now()
	}
	result := ConversationDetachResult{}
	for id, req := range st.Allocations {
		if allocationStateTerminal(req.State) || req.DetachRequestedAt.IsZero() {
			continue
		}
		if result.ConversationID == "" {
			result.ConversationID = strings.TrimSpace(req.ConversationID)
		}
		if allocationHasActiveStartedJob(*st, req.ID) {
			result.Draining = append(result.Draining, id)
			continue
		}
		reason := firstNonEmpty(req.DetachReason, "detached from shared beacon resource")
		if !allocationHasOtherChats(*st, req, req.ConversationID) {
			req.CancelRequestedAt = now
			req.CancelReason = reason
			req.DetachRequestedAt = time.Time{}
			req.DetachReason = ""
			req.ProviderReason = firstNonEmpty(req.ProviderReason, "shared detach became a full release after other chats drained")
			req.UpdatedAt = now
			st.Allocations[id] = req
			result.Escalated = append(result.Escalated, id)
			_, _ = AppendAudit(st, "allocation_detach_escalated_release", id, now)
			continue
		}
		tombstoneAllocationJobs(st, req, reason, now)
		req.State = AllocationCanceled
		req.ProviderReason = firstNonEmpty(req.ProviderReason, reason)
		req.DetachRequestedAt = time.Time{}
		req.DetachReason = ""
		req.UpdatedAt = now
		st.Allocations[id] = req
		removeConversationFromAllocationMachines(st, req, req.ConversationID, now)
		result.Detached = append(result.Detached, id)
		_, _ = AppendAudit(st, "allocation_detach_chat", id, now)
	}
	sort.Strings(result.Detached)
	sort.Strings(result.Draining)
	sort.Strings(result.Escalated)
	return result
}

func allocationHasOtherChats(st State, req AllocationRequest, conversationID string) bool {
	conversationID = strings.TrimSpace(conversationID)
	for _, chat := range allocationReleaseChats(st, req) {
		if strings.TrimSpace(chat) != "" && strings.TrimSpace(chat) != conversationID {
			return true
		}
	}
	return false
}

func removeConversationFromAllocationMachines(st *State, req AllocationRequest, conversationID string, now time.Time) {
	if st == nil {
		return
	}
	providerJobID, leaseID, ok := allocationMachineMatchKeys(req)
	if !ok {
		return
	}
	for key, machine := range st.Machines {
		if strings.TrimSpace(providerJobID) != "" && strings.TrimSpace(machine.ProviderJobID) != strings.TrimSpace(providerJobID) {
			continue
		}
		if strings.TrimSpace(providerJobID) == "" && strings.TrimSpace(machine.LeaseID) != strings.TrimSpace(leaseID) {
			continue
		}
		machine.Chats = removeString(machine.Chats, conversationID)
		machine.UpdatedAt = now
		st.Machines[key] = machine
	}
}

func removeString(values []string, target string) []string {
	target = strings.TrimSpace(target)
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || value == target {
			continue
		}
		out = append(out, value)
	}
	return uniqueSortedStrings(out)
}

func uniqueSortedStrings(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

type UpgradeOperation string

const (
	UpgradeHelperReload       UpgradeOperation = "helper_reload"
	UpgradeHelperRestart      UpgradeOperation = "helper_restart"
	UpgradePendingReplacement UpgradeOperation = "pending_replacement"
	UpgradePrelistenCodex     UpgradeOperation = "prelisten_codex"
	UpgradeBeaconCodexTarget  UpgradeOperation = "beacon_codex_target"
)

type UpgradeInput struct {
	Operation               UpgradeOperation
	QueuedTeamsTurns        int
	RunningTeamsTurns       int
	ActiveBeaconSameTarget  int
	ActiveBeaconOtherTarget int
	QueuedSameCodexTarget   int
	ProtectedOutbox         int
	UnreconciledMarkers     int
	Force                   bool
}

type UpgradeBlocker struct {
	Kind           string
	ID             string
	ConversationID string
	MachineID      string
	Status         string
	Detail         string
}

func UpgradeBlockers(in UpgradeInput) []string {
	var blockers []string
	if in.RunningTeamsTurns > 0 {
		blockers = append(blockers, "running_teams_turn")
	}
	if in.ProtectedOutbox > 0 {
		blockers = append(blockers, "protected_outbox")
	}
	if in.UnreconciledMarkers > 0 {
		blockers = append(blockers, "unreconciled_beacon_marker")
	}
	switch in.Operation {
	case UpgradeHelperReload, UpgradeHelperRestart, UpgradePendingReplacement:
		if in.ActiveBeaconSameTarget+in.ActiveBeaconOtherTarget > 0 {
			blockers = append(blockers, "active_beacon_job")
		}
	case UpgradePrelistenCodex:
		if in.QueuedTeamsTurns > 0 {
			blockers = append(blockers, "queued_teams_turn")
		}
		if in.ActiveBeaconSameTarget+in.ActiveBeaconOtherTarget+in.QueuedSameCodexTarget > 0 {
			blockers = append(blockers, "codex_target_work")
		}
	case UpgradeBeaconCodexTarget:
		if in.ActiveBeaconSameTarget+in.QueuedSameCodexTarget > 0 {
			blockers = append(blockers, "codex_target_work")
		}
	default:
		blockers = append(blockers, "unknown_upgrade_operation")
	}
	if in.Force {
		filtered := blockers[:0]
		for _, blocker := range blockers {
			if blocker != "stale_owner_marker" {
				filtered = append(filtered, blocker)
			}
		}
		blockers = filtered
	}
	return blockers
}

func UpgradeBlockersForState(st State, op UpgradeOperation, codexTargetSignature string) []UpgradeBlocker {
	st.normalize()
	target := strings.TrimSpace(codexTargetSignature)
	var blockers []UpgradeBlocker
	for _, m := range st.Machines {
		state := strings.ToLower(strings.TrimSpace(m.State))
		if !machineStateMayBlockUpgrade(state, len(m.Jobs) > 0) {
			continue
		}
		detail := strings.TrimSpace(m.Profile)
		if strings.TrimSpace(m.ProviderJobID) != "" {
			if detail != "" {
				detail += " "
			}
			detail += "provider_job=" + strings.TrimSpace(m.ProviderJobID)
		}
		if len(m.Jobs) == 0 {
			blockers = append(blockers, UpgradeBlocker{
				Kind:      "beacon_marker",
				ID:        firstNonEmpty(m.LeaseID, m.ID),
				MachineID: m.ID,
				Status:    firstNonEmpty(m.State, "unknown"),
				Detail:    detail,
			})
			continue
		}
		for _, jobID := range m.Jobs {
			blockers = append(blockers, UpgradeBlocker{
				Kind:      "beacon_job",
				ID:        strings.TrimSpace(jobID),
				MachineID: m.ID,
				Status:    firstNonEmpty(m.State, "unknown"),
				Detail:    detail,
			})
		}
	}

	for _, req := range st.Allocations {
		if !allocationMayBlockUpgrade(st, req, op, target) {
			continue
		}
		blockers = append(blockers, UpgradeBlocker{
			Kind:           "beacon_allocation",
			ID:             strings.TrimSpace(req.ID),
			ConversationID: strings.TrimSpace(req.ConversationID),
			MachineID:      strings.TrimSpace(req.Target.MachineID),
			Status:         string(req.State),
			Detail:         beaconAllocationBlockerDetail(req),
		})
	}

	if op == UpgradePrelistenCodex || op == UpgradeBeaconCodexTarget {
		for _, conv := range st.Conversations {
			for _, queued := range conv.Queued {
				snap := queued.Snapshot
				if snap.Target != TargetBeacon {
					continue
				}
				if target != "" && strings.TrimSpace(snap.Signature) != target {
					continue
				}
				blockers = append(blockers, UpgradeBlocker{
					Kind:           "beacon_queued_turn",
					ID:             strings.TrimSpace(queued.ID),
					ConversationID: conv.ID,
					MachineID:      strings.TrimSpace(snap.MachineID),
					Status:         "queued",
					Detail:         strings.TrimSpace(snap.Profile),
				})
			}
		}
	}

	sort.Slice(blockers, func(i, j int) bool {
		if blockers[i].Kind != blockers[j].Kind {
			return blockers[i].Kind < blockers[j].Kind
		}
		if blockers[i].ConversationID != blockers[j].ConversationID {
			return blockers[i].ConversationID < blockers[j].ConversationID
		}
		if blockers[i].MachineID != blockers[j].MachineID {
			return blockers[i].MachineID < blockers[j].MachineID
		}
		return blockers[i].ID < blockers[j].ID
	})
	return blockers
}

func allocationMayBlockUpgrade(st State, req AllocationRequest, op UpgradeOperation, target string) bool {
	switch req.State {
	case AllocationCanceled, AllocationExpired, AllocationFailed:
		return false
	}
	if target != "" && strings.TrimSpace(req.Execution.Hash) != target && strings.TrimSpace(req.Target.Signature) != target {
		return false
	}
	if !allocationHasUpgradeBlockingResource(st, req) {
		return false
	}
	switch op {
	case UpgradeHelperReload, UpgradeHelperRestart, UpgradePendingReplacement:
		return true
	case UpgradePrelistenCodex:
		return true
	case UpgradeBeaconCodexTarget:
		return target == "" || strings.TrimSpace(req.Execution.Hash) == target || strings.TrimSpace(req.Target.Signature) == target
	default:
		return true
	}
}

func allocationHasUpgradeBlockingResource(st State, req AllocationRequest) bool {
	if strings.TrimSpace(firstNonEmpty(req.ProviderIdentity.ProviderJobID, req.Target.ProviderJobID)) != "" {
		return true
	}
	if strings.TrimSpace(req.Target.MachineID) != "" || strings.TrimSpace(req.Target.LeaseID) != "" {
		return true
	}
	if AllocationHasStartedJob(st, req.ID) {
		return true
	}
	if !req.RenewStartedAt.IsZero() && (req.RenewCompletedAt.IsZero() || req.RenewCompletedAt.Before(req.RenewStartedAt)) {
		return true
	}
	if !req.CancelRequestedAt.IsZero() && req.State != AllocationCanceled {
		return true
	}
	return false
}

func beaconAllocationBlockerDetail(req AllocationRequest) string {
	var parts []string
	if strings.TrimSpace(req.Profile) != "" {
		parts = append(parts, strings.TrimSpace(req.Profile))
	}
	if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" {
		parts = append(parts, "provider_job="+strings.TrimSpace(req.ProviderIdentity.ProviderJobID))
	}
	if req.RenewEpoch > 0 {
		parts = append(parts, fmt.Sprintf("renew_epoch=%d", req.RenewEpoch))
	}
	if strings.TrimSpace(req.RenewError) != "" {
		parts = append(parts, "renew_error="+strings.TrimSpace(req.RenewError))
	}
	if strings.TrimSpace(req.ReplacementID) != "" {
		parts = append(parts, "replacement="+strings.TrimSpace(req.ReplacementID))
	}
	if req.ReplacementEpoch > 0 {
		parts = append(parts, fmt.Sprintf("replacement_epoch=%d", req.ReplacementEpoch))
	}
	return strings.Join(parts, " ")
}

func machineStateMayBlockUpgrade(state string, hasJobs bool) bool {
	switch state {
	case "", "idle", "accepting", "drained", "expired":
		return hasJobs
	case "draining":
		return hasJobs
	case "lost", "incompatible", "ambiguous", "needs_attention", "protocol_mismatch", "finalizing", "running", "starting", "claimed":
		return true
	default:
		return hasJobs
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

type ArtifactRef struct {
	Root                 string
	Path                 string
	DeclaredHash         string
	ActualHash           string
	Size                 int64
	Limit                int64
	OpenedNoFollow       bool
	FstatStable          bool
	HashFromOpenedFile   bool
	StagedFromOpenedFile bool
	HardlinkCount        int
	WorkerDeliveryField  bool
	Missing              bool
	UploadOK             bool
}

func ValidateArtifact(ref ArtifactRef) error {
	if ref.Missing {
		return fmt.Errorf("artifact missing")
	}
	if ref.WorkerDeliveryField {
		return fmt.Errorf("worker-provided delivery field is not allowed")
	}
	if !ref.OpenedNoFollow || !ref.FstatStable || !ref.HashFromOpenedFile || !ref.StagedFromOpenedFile {
		return fmt.Errorf("artifact was not safely staged from opened file")
	}
	if ref.HardlinkCount > 1 {
		return fmt.Errorf("artifact hardlinks are not allowed")
	}
	if ref.Size <= 0 || ref.Size > ref.Limit {
		return fmt.Errorf("artifact size out of range")
	}
	if strings.TrimSpace(ref.DeclaredHash) == "" || ref.DeclaredHash != ref.ActualHash {
		return fmt.Errorf("artifact hash mismatch")
	}
	root := filepath.Clean(ref.Root)
	path := filepath.Clean(ref.Path)
	if path == root || !strings.HasPrefix(path, root+string(filepath.Separator)) {
		return fmt.Errorf("artifact path outside root")
	}
	if !ref.UploadOK {
		return fmt.Errorf("artifact upload failed")
	}
	return nil
}

type TerminalResult struct {
	Action       string
	OutboxQueued bool
}

type WorkerTerminalEnvelope struct {
	JobID            string           `json:"job_id"`
	RequestID        string           `json:"request_id"`
	TurnID           string           `json:"turn_id"`
	WorkerID         string           `json:"worker_id,omitempty"`
	LeaseID          string           `json:"lease_id,omitempty"`
	LeaseEpoch       int              `json:"lease_epoch,omitempty"`
	ClaimEpoch       int              `json:"claim_epoch,omitempty"`
	ProviderIdentity ProviderIdentity `json:"provider_identity,omitempty"`
	Payload          []byte           `json:"payload,omitempty"`
}

type WorkerTerminalDecision struct {
	Integrity    TerminalIntegrity `json:"integrity"`
	Result       TerminalResult    `json:"result"`
	Reason       string            `json:"reason,omitempty"`
	OutboxQueued bool              `json:"outbox_queued"`
}

func AcceptTerminal(st *State, jobID string, envelope []byte, now time.Time) (TerminalResult, error) {
	if st == nil {
		return TerminalResult{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return TerminalResult{}, fmt.Errorf("job id is required")
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(envelope))
	if existing, ok := st.Terminals[jobID]; ok {
		if existing.EnvelopeHash != hash {
			return TerminalResult{Action: "quarantine"}, nil
		}
		if !existing.OutboxQueued {
			existing.OutboxQueued = true
			st.Terminals[jobID] = existing
			return TerminalResult{Action: "complete", OutboxQueued: true}, nil
		}
		return TerminalResult{Action: "complete", OutboxQueued: false}, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	st.Terminals[jobID] = TerminalRecord{JobID: jobID, EnvelopeHash: hash, OutboxQueued: true, AcceptedAt: now}
	return TerminalResult{Action: "complete", OutboxQueued: true}, nil
}

func AcceptWorkerTerminal(st *State, envelope WorkerTerminalEnvelope, now time.Time) (WorkerTerminalDecision, error) {
	if st == nil {
		return WorkerTerminalDecision{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	normalizeWorkerTerminalEnvelope(&envelope)
	if envelope.JobID == "" {
		return WorkerTerminalDecision{}, fmt.Errorf("job id is required")
	}
	attempt, ok := st.JobAttempts[envelope.JobID]
	if !ok {
		return WorkerTerminalDecision{Integrity: TerminalHMACBad, Result: TerminalResult{Action: "quarantine"}, Reason: "unknown job attempt"}, nil
	}
	if attempt.Phase == JobTombstoned {
		return WorkerTerminalDecision{Integrity: TerminalLateWrite, Result: TerminalResult{Action: "ignored"}, Reason: firstNonEmpty(attempt.Reason, "job attempt was tombstoned")}, nil
	}
	if attempt.Phase == JobQuarantined {
		return WorkerTerminalDecision{Integrity: TerminalDuplicateConflict, Result: TerminalResult{Action: "quarantine"}, Reason: firstNonEmpty(attempt.Reason, "job attempt is quarantined")}, nil
	}
	if reason := workerTerminalMismatchReason(attempt, envelope); reason != "" {
		attempt.Phase = JobQuarantined
		if now.IsZero() {
			now = time.Now()
		}
		attempt.UpdatedAt = now
		st.JobAttempts[envelope.JobID] = attempt
		return WorkerTerminalDecision{Integrity: TerminalDuplicateConflict, Result: TerminalResult{Action: "quarantine"}, Reason: reason}, nil
	}
	canonical, err := json.Marshal(envelope)
	if err != nil {
		return WorkerTerminalDecision{}, fmt.Errorf("marshal worker terminal envelope: %w", err)
	}
	result, err := AcceptTerminal(st, envelope.JobID, canonical, now)
	if err != nil {
		return WorkerTerminalDecision{}, err
	}
	if record, ok := st.Terminals[envelope.JobID]; ok && result.Action != "quarantine" {
		record.Payload = string(envelope.Payload)
		st.Terminals[envelope.JobID] = record
	}
	if result.Action == "quarantine" {
		attempt.Phase = JobQuarantined
	} else {
		attempt.Phase = JobTerminal
		removeJobFromMachines(st, envelope.JobID)
	}
	if now.IsZero() {
		now = time.Now()
	}
	attempt.UpdatedAt = now
	st.JobAttempts[envelope.JobID] = attempt
	integrity := TerminalValid
	if result.Action == "quarantine" {
		integrity = TerminalDuplicateConflict
	} else if !result.OutboxQueued {
		integrity = TerminalDuplicateSame
	}
	return WorkerTerminalDecision{Integrity: integrity, Result: result, OutboxQueued: result.OutboxQueued}, nil
}

func removeJobFromMachines(st *State, jobID string) {
	if st == nil {
		return
	}
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return
	}
	for key, machine := range st.Machines {
		machine.Jobs = removeStringValue(machine.Jobs, jobID)
		st.Machines[key] = machine
	}
}

func removeStringValue(values []string, value string) []string {
	out := values[:0]
	for _, current := range values {
		if strings.TrimSpace(current) != value {
			out = append(out, current)
		}
	}
	return out
}

func normalizeWorkerTerminalEnvelope(envelope *WorkerTerminalEnvelope) {
	if envelope == nil {
		return
	}
	envelope.JobID = strings.TrimSpace(envelope.JobID)
	envelope.RequestID = strings.TrimSpace(envelope.RequestID)
	envelope.TurnID = strings.TrimSpace(envelope.TurnID)
	envelope.WorkerID = strings.TrimSpace(envelope.WorkerID)
	envelope.LeaseID = strings.TrimSpace(envelope.LeaseID)
	envelope.ProviderIdentity.ProviderJobID = strings.TrimSpace(envelope.ProviderIdentity.ProviderJobID)
	envelope.ProviderIdentity.AllocationID = strings.TrimSpace(envelope.ProviderIdentity.AllocationID)
	envelope.ProviderIdentity.StepID = strings.TrimSpace(envelope.ProviderIdentity.StepID)
	envelope.ProviderIdentity.RunIncarnation = strings.TrimSpace(envelope.ProviderIdentity.RunIncarnation)
	envelope.ProviderIdentity.Host = strings.TrimSpace(envelope.ProviderIdentity.Host)
	envelope.ProviderIdentity.MembershipProof = strings.TrimSpace(envelope.ProviderIdentity.MembershipProof)
}

func workerTerminalMismatchReason(attempt JobAttempt, envelope WorkerTerminalEnvelope) string {
	if strings.TrimSpace(envelope.RequestID) == "" || strings.TrimSpace(envelope.RequestID) != strings.TrimSpace(attempt.RequestID) {
		return "request id mismatch"
	}
	if strings.TrimSpace(envelope.TurnID) == "" || strings.TrimSpace(envelope.TurnID) != strings.TrimSpace(attempt.TurnID) {
		return "turn id mismatch"
	}
	if strings.TrimSpace(attempt.WorkerID) != "" && strings.TrimSpace(envelope.WorkerID) != strings.TrimSpace(attempt.WorkerID) {
		return "worker id mismatch"
	}
	if strings.TrimSpace(attempt.LeaseID) != "" && strings.TrimSpace(envelope.LeaseID) != strings.TrimSpace(attempt.LeaseID) {
		return "lease id mismatch"
	}
	if attempt.LeaseEpoch != 0 && envelope.LeaseEpoch != attempt.LeaseEpoch {
		return "lease epoch mismatch"
	}
	if attempt.ClaimEpoch != 0 && envelope.ClaimEpoch != attempt.ClaimEpoch {
		return "claim epoch mismatch"
	}
	if strings.TrimSpace(attempt.ProviderIdentity.ProviderJobID) != "" && strings.TrimSpace(envelope.ProviderIdentity.ProviderJobID) != strings.TrimSpace(attempt.ProviderIdentity.ProviderJobID) {
		return "provider job mismatch"
	}
	if attempt.Phase == JobTombstoned || attempt.Phase == JobQuarantined {
		return "job attempt is not accepting terminal output"
	}
	return ""
}
