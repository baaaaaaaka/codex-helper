package beacon

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	NoticeProviderAdapterNotConfigured = "BEACON_PROVIDER_ADAPTER_NOT_CONFIGURED"
	NoticeProviderQueryFailed          = "BEACON_PROVIDER_QUERY_FAILED"
	NoticeAllocationNeedsAttention     = "BEACON_ALLOCATION_NEEDS_ATTENTION"
	NoticeSchedulerPending             = "BEACON_SCHEDULER_PENDING"
	NoticeWaitingForWorker             = "BEACON_WORKER_NOT_CONNECTED"
	NoticeReady                        = "BEACON_READY"
	NoticeLocalTarget                  = "BEACON_LOCAL_TARGET"
	NoticeUnknown                      = "BEACON_STATUS_UNKNOWN"
)

type NoticeDetail struct {
	Key   string
	Value string
}

type BeaconNotice struct {
	Title        string
	WhatHappened []string
	State        []NoticeDetail
	Doing        []string
	Next         []string
	Details      []NoticeDetail
}

func (n BeaconNotice) Render() string {
	var lines []string
	title := strings.TrimSpace(n.Title)
	if title == "" {
		title = "Beacon status"
	}
	lines = append(lines, title)
	if len(n.WhatHappened) > 0 {
		lines = append(lines, "", "Summary:")
		for _, line := range n.WhatHappened {
			line = strings.TrimSpace(line)
			if line != "" {
				lines = append(lines, "- "+line)
			}
		}
	}
	state := n.State
	if len(state) == 0 {
		state = n.Details
	}
	if len(state) > 0 {
		lines = append(lines, "", "State:")
		for _, detail := range state {
			key := strings.TrimSpace(detail.Key)
			if key == "" {
				continue
			}
			lines = append(lines, "- "+key+": `"+detailValue(detail.Value)+"`")
		}
	}
	if len(n.Doing) > 0 {
		lines = append(lines, "", "What cxp is doing:")
		for _, line := range n.Doing {
			line = strings.TrimSpace(line)
			if line != "" {
				lines = append(lines, "- "+line)
			}
		}
	}
	lines = append(lines, "", "Action needed:")
	if len(n.Next) > 0 {
		for _, line := range n.Next {
			line = strings.TrimSpace(line)
			if line != "" {
				lines = append(lines, "- "+line)
			}
		}
	} else {
		lines = append(lines, "- None.")
	}
	if len(n.State) > 0 && len(n.Details) > 0 {
		lines = append(lines, "", "Details:")
		for _, detail := range n.Details {
			key := strings.TrimSpace(detail.Key)
			if key == "" {
				continue
			}
			lines = append(lines, "- "+key+": `"+detailValue(detail.Value)+"`")
		}
	}
	return strings.Join(lines, "\n")
}

func detailValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "<none>"
	}
	return value
}

func providerLabel(provider Provider) string {
	text := strings.TrimSpace(string(provider))
	switch strings.ToLower(text) {
	case "slurm":
		return "Slurm"
	case "lsf":
		return "LSF"
	case "local":
		return "local"
	case "":
		return "provider"
	default:
		return text
	}
}

func targetLabel(snapshot TargetSnapshot) string {
	label := targetSnapshotLabel(snapshot)
	if label == "" {
		return TargetLocal
	}
	return label
}

func TurnStartFailureNotice(plan TurnExecutionPlan, cause error) BeaconNotice {
	code := NoticeUnknown
	title := "Beacon cannot start yet."
	var notConfigured ProviderCommandNotConfiguredError
	switch {
	case errors.As(cause, &notConfigured):
		code = NoticeProviderAdapterNotConfigured
		title = fmt.Sprintf("Beacon cannot start: %s provider adapter is not configured.", providerLabel(notConfigured.Provider))
	case strings.Contains(strings.ToLower(plan.ProviderReason), "not configured"):
		code = NoticeProviderAdapterNotConfigured
		title = "Beacon cannot start: provider adapter is not configured."
	case plan.AllocationState == AllocationNeedsAttention || plan.SubmitAction == AllocationSubmitAttention:
		code = NoticeAllocationNeedsAttention
		title = "Beacon needs attention before Codex can start."
	case strings.TrimSpace(plan.ProviderJobID) != "" && strings.TrimSpace(plan.ProviderState) != "":
		code = NoticeSchedulerPending
		title = "Beacon is waiting for the scheduler provider."
	case strings.TrimSpace(plan.AllocationRequestID) != "":
		code = NoticeWaitingForWorker
		title = "Beacon is waiting for a worker machine."
	}

	what := []string{
		fmt.Sprintf("This Work chat targets `%s`.", targetLabel(plan.Snapshot)),
	}
	if strings.TrimSpace(plan.AllocationRequestID) != "" {
		what = append(what, fmt.Sprintf("cxp recorded allocation `%s`, but no ready beacon lease is available for this turn.", plan.AllocationRequestID))
	}
	what = append(what, "Codex did not run locally because explicit beacon targets disable local fallback.")
	if strings.TrimSpace(plan.ProviderReason) != "" && !strings.EqualFold(strings.TrimSpace(plan.ProviderReason), strings.TrimSpace(causeString(cause))) {
		what = append(what, "Provider reason: "+strings.TrimSpace(plan.ProviderReason))
	} else if strings.TrimSpace(causeString(cause)) != "" && !errors.As(cause, &notConfigured) {
		what = append(what, "Provider reason: "+strings.TrimSpace(causeString(cause)))
	}

	next := turnStartFailureNextSteps(code, notConfigured, plan)
	return BeaconNotice{
		Title:        title,
		WhatHappened: what,
		State:        turnPlanStateDetails(plan),
		Doing:        noticeDoing(code),
		Next:         next,
		Details:      turnPlanDetails(code, plan, cause),
	}
}

func ProviderAdapterConfigurationNotice(notConfigured ProviderCommandNotConfiguredError) BeaconNotice {
	profile := strings.TrimSpace(notConfigured.ProfileName)
	operation := strings.TrimSpace(notConfigured.Operation)
	if operation == "" {
		operation = "provider"
	}
	what := []string{
		fmt.Sprintf("The %s adapter command for %s is missing.", operation, providerLabel(notConfigured.Provider)),
	}
	if profile != "" {
		what = append(what, fmt.Sprintf("Profile `%s` does not define `%s`, and the helper environment fallback is not set.", profile, detailValue(notConfigured.ProfileFlag)))
	}
	plan := TurnExecutionPlan{Snapshot: TargetSnapshot{Target: TargetBeacon, Profile: profile}}
	return BeaconNotice{
		Title:        fmt.Sprintf("Beacon command failed: %s provider adapter is not configured.", providerLabel(notConfigured.Provider)),
		WhatHappened: what,
		State: []NoticeDetail{
			{"profile", profile},
			{"provider", string(notConfigured.Provider)},
			{"operation", operation},
			{"env", notConfigured.EnvName},
		},
		Doing: []string{"Waiting for a profile-stored adapter or helper service environment setting before touching the scheduler."},
		Next:  turnStartFailureNextSteps(NoticeProviderAdapterNotConfigured, notConfigured, plan),
		Details: []NoticeDetail{
			{"error_code", NoticeProviderAdapterNotConfigured},
			{"profile", profile},
			{"provider", string(notConfigured.Provider)},
			{"operation", operation},
			{"profile_flag", notConfigured.ProfileFlag},
			{"env", notConfigured.EnvName},
		},
	}
}

func causeString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func turnStartFailureNextSteps(code string, notConfigured ProviderCommandNotConfiguredError, plan TurnExecutionPlan) []string {
	switch code {
	case NoticeProviderAdapterNotConfigured:
		envName := strings.TrimSpace(notConfigured.EnvName)
		provider := notConfigured.Provider
		profile := strings.TrimSpace(firstNonEmpty(notConfigured.ProfileName, plan.Snapshot.Profile))
		flag := strings.TrimSpace(notConfigured.ProfileFlag)
		if envName == "" {
			provider, envName = providerCommandHintFromPlan(plan)
		}
		if envName == "" {
			envName = "CODEX_HELPER_BEACON_<PROVIDER>_QUERY"
		}
		next := []string{}
		if profile != "" && flag != "" && provider != "" {
			next = append(next, fmt.Sprintf("Run `beacon profile update %s --provider %s ... %s <adapter-script>` in the control chat to store the adapter on the profile. This does not require a helper reload.", profile, strings.ToLower(string(provider)), flag))
		}
		next = append(next, fmt.Sprintf("Or configure `%s` in the Teams helper service environment, not only in an interactive shell.", envName))
		if provider != "" {
			next = append(next, fmt.Sprintf("For a starter script, run `cxp beacon provider template %s` on the helper host.", strings.ToLower(string(provider))))
		} else {
			next = append(next, "For a starter script, run `cxp beacon provider template slurm` or `cxp beacon provider template lsf` on the helper host.")
		}
		next = append(next, "Only the service-environment path requires `helper reload now`; profile-stored adapter changes apply to future turns directly.")
		return next
	case NoticeSchedulerPending:
		return []string{
			"No action is needed yet. cxp will keep reconciling this allocation.",
			"Use `beacon allocation status " + detailValue(plan.AllocationRequestID) + "` in the control chat for provider details.",
		}
	case NoticeAllocationNeedsAttention:
		return []string{
			"Inspect the allocation from the control chat with `beacon allocation status " + detailValue(plan.AllocationRequestID) + "`.",
			"Fix the provider or worker issue, then send a new request or retry after confirming the previous turn did not start.",
		}
	default:
		return []string{
			"Wait for the beacon worker to become ready, or inspect the allocation from the control chat with `beacon allocation status " + detailValue(plan.AllocationRequestID) + "`.",
		}
	}
}

func providerCommandHintFromPlan(plan TurnExecutionPlan) (Provider, string) {
	text := strings.ToLower(plan.ProviderReason)
	switch {
	case strings.Contains(text, "slurm") || strings.Contains(text, strings.ToLower(BeaconSlurmQueryCommandEnv)):
		return ProviderSlurm, BeaconSlurmQueryCommandEnv
	case strings.Contains(text, "lsf") || strings.Contains(text, strings.ToLower(BeaconLSFQueryCommandEnv)):
		return ProviderLSF, BeaconLSFQueryCommandEnv
	default:
		return "", ""
	}
}

func turnPlanDetails(code string, plan TurnExecutionPlan, cause error) []NoticeDetail {
	details := []NoticeDetail{
		{"error_code", code},
		{"phase", "allocation"},
		{"target", targetLabel(plan.Snapshot)},
		{"profile", plan.Snapshot.Profile},
		{"allocation", plan.AllocationRequestID},
		{"allocation_state", string(plan.AllocationState)},
		{"turn_action", string(plan.Action)},
		{"submit_action", string(plan.SubmitAction)},
		{"provider_job", plan.ProviderJobID},
		{"provider_state", plan.ProviderState},
		{"machine", plan.MachineID},
		{"lease", plan.LeaseID},
	}
	if strings.TrimSpace(plan.ProviderReason) != "" {
		details = append(details, NoticeDetail{"provider_reason", plan.ProviderReason})
	}
	if strings.TrimSpace(causeString(cause)) != "" {
		details = append(details, NoticeDetail{"raw_error", causeString(cause)})
	}
	return details
}

func turnPlanStateDetails(plan TurnExecutionPlan) []NoticeDetail {
	return []NoticeDetail{
		{"target", targetLabel(plan.Snapshot)},
		{"profile", plan.Snapshot.Profile},
		{"allocation", plan.AllocationRequestID},
		{"allocation_state", string(plan.AllocationState)},
		{"provider_job", plan.ProviderJobID},
		{"provider_state", plan.ProviderState},
		{"machine", plan.MachineID},
		{"lease", plan.LeaseID},
	}
}

func noticeDoing(code string) []string {
	switch code {
	case NoticeProviderAdapterNotConfigured:
		return []string{"Waiting for the provider adapter configuration before starting Codex."}
	case NoticeSchedulerPending:
		return []string{"Checking scheduler status and waiting for a beacon worker to register."}
	case NoticeAllocationNeedsAttention:
		return []string{"Keeping the allocation visible while it avoids local fallback for this explicit beacon target."}
	case NoticeReady:
		return []string{"Using the ready beacon worker for new work."}
	case NoticeLocalTarget:
		return []string{"Running future turns locally until this Work chat is switched to a beacon profile."}
	default:
		return []string{"Tracking beacon state and waiting for a safe next step."}
	}
}

func ConversationStatusNotice(st State, conversationID string) BeaconNotice {
	st.normalize()
	conversationID = strings.TrimSpace(conversationID)
	conv := st.Conversations[conversationID]
	current := conv.Current
	if current.Target == "" {
		current.Target = TargetLocal
	}
	pending := ""
	if conv.Pending != nil {
		pending = targetSnapshotLabel(*conv.Pending)
	}
	turnSnapshot := ""
	if len(conv.Queued) > 0 {
		turnSnapshot = targetSnapshotLabel(conv.Queued[0].Snapshot)
	}
	req, hasAllocation := latestStatusAllocation(st, conversationID, conv)
	if hasAllocation && current.ProviderJobID == "" {
		current.ProviderJobID = req.ProviderIdentity.ProviderJobID
	}

	title := "Beacon status: this chat runs locally."
	code := NoticeLocalTarget
	switch strings.TrimSpace(current.Target) {
	case TargetBeacon:
		code = statusNoticeCode(req, hasAllocation)
		switch code {
		case NoticeReady:
			title = "Beacon status: ready."
		case NoticeAllocationNeedsAttention:
			title = "Beacon status: needs attention."
		case NoticeSchedulerPending:
			title = fmt.Sprintf("Beacon status: waiting for %s.", providerLabel(req.Provider))
		default:
			title = "Beacon status: waiting for a worker."
		}
	}

	what := []string{"Current target: `" + targetSnapshotLabel(current) + "`."}
	if pending != "" {
		what = append(what, "Pending target: `"+pending+"`.")
	}
	if turnSnapshot != "" {
		what = append(what, "Next queued turn keeps snapshot `"+turnSnapshot+"`.")
	}
	if hasAllocation {
		what = append(what, allocationSentence(req))
	} else if strings.TrimSpace(current.Target) == TargetBeacon {
		what = append(what, "No managed allocation has been recorded for this chat yet.")
	}

	return BeaconNotice{
		Title:        title,
		WhatHappened: what,
		State:        statusNoticeStateDetails(conversationID, current, pending, turnSnapshot, req, hasAllocation),
		Doing:        noticeDoing(code),
		Next:         statusNoticeNext(code, req, hasAllocation),
		Details:      statusNoticeDetails(code, conversationID, current, pending, turnSnapshot, req, hasAllocation),
	}
}

func latestStatusAllocation(st State, conversationID string, conv Conversation) (AllocationRequest, bool) {
	if len(conv.Queued) > 0 {
		reqID := ManagedRequestID(conversationID, conv.Queued[0].ID)
		if req, ok := st.Allocations[reqID]; ok {
			return req, true
		}
	}
	return latestAllocationForConversation(st, conversationID)
}

func statusNoticeCode(req AllocationRequest, hasAllocation bool) string {
	if !hasAllocation {
		return NoticeWaitingForWorker
	}
	switch req.State {
	case AllocationRunning:
		return NoticeReady
	case AllocationNeedsAttention, AllocationFailed, AllocationExpired, AllocationCanceled:
		return NoticeAllocationNeedsAttention
	case AllocationSubmitted, AllocationPending:
		return NoticeSchedulerPending
	default:
		return NoticeWaitingForWorker
	}
}

func statusNoticeNext(code string, req AllocationRequest, hasAllocation bool) []string {
	switch code {
	case NoticeLocalTarget:
		return []string{"To use a remote worker, switch this Work chat to a ready beacon profile with `beacon switch <profile>`."}
	case NoticeReady:
		return []string{"Send a task message in this Work chat. Codex will run through the ready beacon lease."}
	case NoticeSchedulerPending:
		return []string{"No action is needed yet. cxp will keep waiting for the scheduler job and worker.", "Use `beacon allocation status " + detailValue(req.ID) + "` in the control chat for provider details."}
	case NoticeAllocationNeedsAttention:
		if hasAllocation {
			return []string{"Use `beacon allocation status " + detailValue(req.ID) + "` in the control chat, then fix the provider or worker issue before retrying."}
		}
		return []string{"Use `beacon list` in the control chat to inspect profiles, allocations, and machines."}
	default:
		return []string{"Wait for the beacon worker to register, or use `beacon list` in the control chat to inspect global state."}
	}
}

func statusNoticeDetails(code string, conversationID string, current TargetSnapshot, pending string, turnSnapshot string, req AllocationRequest, hasAllocation bool) []NoticeDetail {
	details := []NoticeDetail{
		{"status_code", code},
		{"conversation", conversationID},
		{"current_target", targetSnapshotLabel(current)},
		{"profile", current.Profile},
		{"pending_target", pending},
		{"turn_snapshot", turnSnapshot},
		{"proxy", current.ProxyRoute},
		{"isolation", string(current.Isolation)},
		{"lease", current.LeaseID},
		{"machine", current.MachineID},
		{"provider_job", current.ProviderJobID},
	}
	if hasAllocation {
		details = append(details,
			NoticeDetail{"allocation", req.ID},
			NoticeDetail{"allocation_state", string(req.State)},
			NoticeDetail{"provider_state", req.RawProviderState},
		)
		if strings.TrimSpace(req.ProviderReason) != "" {
			details = append(details, NoticeDetail{"provider_reason", req.ProviderReason})
		}
	}
	return details
}

func statusNoticeStateDetails(conversationID string, current TargetSnapshot, pending string, turnSnapshot string, req AllocationRequest, hasAllocation bool) []NoticeDetail {
	details := []NoticeDetail{
		{"conversation", conversationID},
		{"current_target", targetSnapshotLabel(current)},
		{"profile", current.Profile},
		{"pending_target", pending},
		{"turn_snapshot", turnSnapshot},
	}
	if hasAllocation {
		details = append(details,
			NoticeDetail{"allocation", req.ID},
			NoticeDetail{"allocation_state", string(req.State)},
			NoticeDetail{"provider_job", req.ProviderIdentity.ProviderJobID},
			NoticeDetail{"provider_state", req.RawProviderState},
		)
	}
	return details
}

func AllocationStatusNotice(req AllocationRequest) BeaconNotice {
	code := statusNoticeCode(req, true)
	title := "Beacon allocation: " + detailValue(req.ID)
	switch code {
	case NoticeReady:
		title += " is running."
	case NoticeSchedulerPending:
		title += " is waiting for " + providerLabel(req.Provider) + "."
	case NoticeAllocationNeedsAttention:
		title += " needs attention."
	}
	return BeaconNotice{
		Title: title,
		WhatHappened: []string{
			allocationSentence(req),
		},
		State:   allocationStateDetails(req),
		Doing:   noticeDoing(code),
		Next:    allocationNext(code, req),
		Details: allocationDetails(code, req),
	}
}

func allocationSentence(req AllocationRequest) string {
	parts := []string{
		"Allocation `" + detailValue(req.ID) + "`",
		"profile `" + detailValue(req.Profile) + "`",
		"provider " + providerLabel(req.Provider),
		"state `" + detailValue(string(req.State)) + "`",
	}
	if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" {
		parts = append(parts, "provider job `"+req.ProviderIdentity.ProviderJobID+"`")
	}
	if strings.TrimSpace(req.RawProviderState) != "" {
		parts = append(parts, "provider state `"+req.RawProviderState+"`")
	}
	if strings.TrimSpace(req.ProviderReason) != "" {
		parts = append(parts, "reason `"+req.ProviderReason+"`")
	}
	return strings.Join(parts, ", ") + "."
}

func allocationNext(code string, req AllocationRequest) []string {
	switch code {
	case NoticeReady:
		return []string{"No action is needed. The worker should be able to accept beacon jobs."}
	case NoticeSchedulerPending:
		return []string{"No action is needed yet. cxp will keep reconciling this allocation.", "If it stays pending too long, check the scheduler reason and site quota."}
	case NoticeAllocationNeedsAttention:
		return []string{"Fix the provider or worker issue shown in Details before retrying this turn."}
	default:
		return []string{"Wait for the scheduler job and worker to become ready."}
	}
}

func allocationDetails(code string, req AllocationRequest) []NoticeDetail {
	details := []NoticeDetail{
		{"status_code", code},
		{"allocation", req.ID},
		{"conversation", req.ConversationID},
		{"turn", req.TurnID},
		{"profile", req.Profile},
		{"provider", string(req.Provider)},
		{"isolation", string(req.Isolation)},
		{"allocation_state", string(req.State)},
		{"deterministic_name", req.DeterministicName},
		{"provider_job", req.ProviderIdentity.ProviderJobID},
		{"provider_state", req.RawProviderState},
	}
	if strings.TrimSpace(req.ProviderReason) != "" {
		details = append(details, NoticeDetail{"provider_reason", req.ProviderReason})
	}
	if strings.TrimSpace(req.RenewError) != "" {
		details = append(details, NoticeDetail{"renew_error", req.RenewError})
	}
	return details
}

func allocationStateDetails(req AllocationRequest) []NoticeDetail {
	return []NoticeDetail{
		{"allocation", req.ID},
		{"profile", req.Profile},
		{"provider", string(req.Provider)},
		{"allocation_state", string(req.State)},
		{"provider_job", req.ProviderIdentity.ProviderJobID},
		{"provider_state", req.RawProviderState},
	}
}

func MachineStatusNotice(m Machine) BeaconNotice {
	p := PreviewRelease(m)
	title := "Beacon machine: " + detailValue(firstNonEmpty(m.ID, p.MachineID))
	if strings.TrimSpace(m.State) != "" {
		title += " is " + strings.TrimSpace(m.State) + "."
	}
	what := []string{
		fmt.Sprintf("Lease `%s` is attached to profile `%s`.", detailValue(p.LeaseID), detailValue(m.Profile)),
	}
	if strings.TrimSpace(p.ProviderJobID) != "" {
		what = append(what, "Provider job: `"+p.ProviderJobID+"`.")
	}
	if len(p.Jobs) > 0 {
		what = append(what, fmt.Sprintf("Jobs currently associated with this machine: %s.", strings.Join(p.Jobs, ", ")))
	}
	return BeaconNotice{
		Title:        title,
		WhatHappened: what,
		State: []NoticeDetail{
			{"machine", firstNonEmpty(p.MachineID, m.ID)},
			{"lease", p.LeaseID},
			{"profile", m.Profile},
			{"state", m.State},
			{"provider_job", p.ProviderJobID},
			{"host", m.Host},
		},
		Doing: []string{"Keeping release safe by draining by default; hard kill requires explicit confirmation."},
		Next:  []string{"Release drains by default. Hard kill requires the confirmation token shown in Details."},
		Details: []NoticeDetail{
			{"machine", firstNonEmpty(p.MachineID, m.ID)},
			{"lease", p.LeaseID},
			{"provider_job", p.ProviderJobID},
			{"profile", m.Profile},
			{"state", m.State},
			{"host", m.Host},
			{"chats", strings.Join(p.Chats, ",")},
			{"jobs", strings.Join(p.Jobs, ",")},
			{"kill_confirmation", p.Confirmation},
		},
	}
}

func AllocationSummaryLines(st State) []string {
	st.normalize()
	allocations := make([]AllocationRequest, 0, len(st.Allocations))
	for _, req := range st.Allocations {
		allocations = append(allocations, req)
	}
	sort.Slice(allocations, func(i, j int) bool {
		if allocations[i].UpdatedAt.Equal(allocations[j].UpdatedAt) {
			return allocations[i].ID < allocations[j].ID
		}
		return allocations[i].UpdatedAt.Before(allocations[j].UpdatedAt)
	})
	if len(allocations) == 0 {
		return []string{"- none"}
	}
	lines := make([]string, 0, len(allocations))
	for _, req := range allocations {
		status := string(req.State)
		if status == "" {
			status = "unknown"
		}
		line := fmt.Sprintf("- %s: %s on %s (%s)", detailValue(req.ID), status, detailValue(req.Profile), providerLabel(req.Provider))
		var facts []string
		if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" {
			facts = append(facts, "provider_job="+req.ProviderIdentity.ProviderJobID)
		}
		if strings.TrimSpace(req.RawProviderState) != "" {
			facts = append(facts, "provider_state="+req.RawProviderState)
		}
		if strings.TrimSpace(req.ProviderReason) != "" {
			facts = append(facts, "reason="+req.ProviderReason)
		}
		if len(facts) > 0 {
			line += " - " + strings.Join(facts, ", ")
		}
		lines = append(lines, line)
	}
	return lines
}

func MachineSummaryLines(st State) []string {
	st.normalize()
	machines := make([]Machine, 0, len(st.Machines))
	for _, m := range st.Machines {
		machines = append(machines, m)
	}
	sort.Slice(machines, func(i, j int) bool { return machines[i].ID < machines[j].ID })
	if len(machines) == 0 {
		return []string{"- none"}
	}
	lines := make([]string, 0, len(machines))
	for _, m := range machines {
		status := firstNonEmpty(m.State, "unknown")
		line := fmt.Sprintf("- %s: %s on %s", detailValue(firstNonEmpty(m.ID, m.LeaseID)), status, detailValue(m.Profile))
		var facts []string
		if strings.TrimSpace(m.LeaseID) != "" {
			facts = append(facts, "lease="+m.LeaseID)
		}
		if strings.TrimSpace(m.ProviderJobID) != "" {
			facts = append(facts, "provider_job="+m.ProviderJobID)
		}
		if len(m.Jobs) > 0 {
			facts = append(facts, fmt.Sprintf("jobs=%d", len(m.Jobs)))
		}
		if len(m.Chats) > 0 {
			facts = append(facts, fmt.Sprintf("chats=%d", len(m.Chats)))
		}
		if len(facts) > 0 {
			line += " - " + strings.Join(facts, ", ")
		}
		lines = append(lines, line)
	}
	return lines
}
