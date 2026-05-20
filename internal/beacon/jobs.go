package beacon

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func ManagedJobID(requestID string) string {
	requestID = sanitizeJobName(strings.TrimSpace(requestID))
	if requestID == "" {
		requestID = "request"
	}
	return "job-" + requestID
}

type WorkerRegistrationInput struct {
	MachineID       string
	LeaseID         string
	ProviderJobID   string
	WorkerID        string
	Host            string
	State           LeaseState
	Doctor          WorkerDoctor
	Bootstrap       BootstrapDiagnostics
	MembershipProof string
}

type CancelTurnResult struct {
	ConversationID string
	TurnID         string
	Reason         string
	Allocations    []AllocationRequest
	Jobs           []JobAttempt
	Ambiguous      bool
}

func RegisterWorkerMachineForAllocation(st *State, requestID string, in WorkerRegistrationInput, now time.Time) (Machine, error) {
	if st == nil {
		return Machine{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return Machine{}, fmt.Errorf("allocation request id is required")
	}
	req, ok := st.Allocations[requestID]
	if !ok {
		return Machine{}, fmt.Errorf("allocation request %q not found", requestID)
	}
	if allocationStateTerminal(req.State) || !req.CancelRequestedAt.IsZero() {
		return Machine{}, fmt.Errorf("allocation request %q is not accepting worker registration: state=%s", requestID, req.State)
	}
	expectedProviderJobID := firstNonEmpty(req.ProviderIdentity.ProviderJobID, req.Target.ProviderJobID)
	inputProviderJobID := strings.TrimSpace(in.ProviderJobID)
	if expectedProviderJobID != "" && inputProviderJobID != "" && inputProviderJobID != expectedProviderJobID {
		return Machine{}, fmt.Errorf("allocation request %q is bound to provider job %q, got worker provider job %q", requestID, expectedProviderJobID, inputProviderJobID)
	}
	if staleProviderJobID := strings.TrimSpace(req.ReplacementID); staleProviderJobID != "" && inputProviderJobID == staleProviderJobID {
		return Machine{}, fmt.Errorf("allocation request %q already replaced stale provider job %q", requestID, staleProviderJobID)
	}
	providerJobID := firstNonEmpty(expectedProviderJobID, inputProviderJobID)
	if providerJobID == "" && req.Provider != ProviderLocal {
		return Machine{}, fmt.Errorf("allocation request %q requires a scheduler provider job id for provider %s", requestID, req.Provider)
	}
	machineID := strings.TrimSpace(in.MachineID)
	if machineID == "" {
		machineID = defaultManagedMachineID(req, providerJobID, in.Host)
	}
	leaseID := strings.TrimSpace(in.LeaseID)
	if leaseID == "" {
		leaseID = defaultManagedLeaseID(providerJobID, machineID)
	}
	if machineID == "" || leaseID == "" {
		return Machine{}, fmt.Errorf("machine id and lease id are required")
	}
	state := in.State
	if state == "" {
		state = LeaseAccepting
	}
	isolation := req.Isolation
	if isolation == "" {
		isolation = req.ProfileSnapshot.IsolationDefault
	}
	if isolation == "" {
		isolation = IsolationShared
	}
	host := strings.TrimSpace(in.Host)
	machine := st.Machines[machineID]
	machine.ID = machineID
	machine.LeaseID = leaseID
	machine.ProviderJobID = providerJobID
	machine.WorkerID = strings.TrimSpace(in.WorkerID)
	machine.Profile = req.Profile
	machine.Host = host
	machine.Isolation = isolation
	machine.State = string(state)
	machine.Reason = ""
	machine.Doctor = in.Doctor
	machine.Bootstrap = in.Bootstrap
	machine.DoctorBlockers = nil
	if ManagedProviderRequiresSharedPath(req.Provider) && strings.TrimSpace(req.ProfileSnapshot.SharedPath) != "" {
		if !workerSharedStoreMatchesProfile(req.ProfileSnapshot.SharedPath, machine.Bootstrap.SharedStorePath) {
			machine.State = string(LeaseNeedsAttention)
			machine.Reason = "worker shared store path does not match profile shared_path"
			machine.DoctorBlockers = appendUniqueString(machine.DoctorBlockers, machine.Reason)
		}
	}
	if !workerDoctorIsZero(in.Doctor) {
		for _, blocker := range WorkerDoctorBlockers(in.Doctor) {
			machine.DoctorBlockers = appendUniqueString(machine.DoctorBlockers, blocker)
		}
		if len(machine.DoctorBlockers) > 0 {
			machine.State = string(LeaseNeedsAttention)
			machine.Reason = firstNonEmpty(machine.Reason, "worker doctor failed: "+strings.Join(machine.DoctorBlockers, "; "))
		}
	}
	machine.MembershipProof = strings.TrimSpace(in.MembershipProof)
	machine.Execution = req.Execution
	machine.ProviderState = ProviderJobRunning
	machine.LeaseExpiresAt = req.ProviderDeadline
	if !workerMembershipProofOK(req, providerJobID, machine.MembershipProof) {
		machine.State = string(LeaseNeedsAttention)
		machine.DoctorBlockers = appendUniqueString(machine.DoctorBlockers, "provider membership proof failed")
		machine.Reason = firstNonEmpty(machine.Reason, "provider membership proof failed")
	}
	if now.IsZero() {
		now = time.Now()
	}
	if machine.StartedAt.IsZero() {
		machine.StartedAt = now
	}
	machine.LastHeartbeat = now
	machine.UpdatedAt = now
	machine.Chats = appendUniqueString(machine.Chats, req.ConversationID)
	st.Machines[machineID] = machine
	req.Target.MachineID = machineID
	req.Target.LeaseID = leaseID
	req.Target.ProviderJobID = providerJobID
	if providerJobID != "" {
		req.ProviderIdentity.ProviderJobID = providerJobID
	}
	if machine.State == string(LeaseAccepting) {
		req.State = AllocationRunning
	}
	req.UpdatedAt = now
	st.Allocations[req.ID] = req
	return machine, nil
}

func CancelTurn(st *State, conversationID string, turnID string, reason string, now time.Time) CancelTurnResult {
	if st == nil {
		return CancelTurnResult{}
	}
	st.normalize()
	conversationID = strings.TrimSpace(conversationID)
	turnID = strings.TrimSpace(turnID)
	reason = firstNonEmpty(reason, "canceled by user")
	if turnID == "" {
		return CancelTurnResult{}
	}
	if now.IsZero() {
		now = time.Now()
	}
	result := CancelTurnResult{ConversationID: conversationID, TurnID: turnID, Reason: reason}
	for id, req := range st.Allocations {
		if strings.TrimSpace(req.TurnID) != turnID {
			continue
		}
		if conversationID != "" && strings.TrimSpace(req.ConversationID) != conversationID {
			continue
		}
		req.CancelRequestedAt = now
		req.CancelReason = reason
		if !allocationStateTerminal(req.State) {
			req.State = AllocationCanceled
		}
		req.ProviderReason = firstNonEmpty(req.ProviderReason, reason)
		req.UpdatedAt = now
		st.Allocations[id] = req
		result.Allocations = append(result.Allocations, req)
	}
	for id, attempt := range st.JobAttempts {
		if strings.TrimSpace(attempt.TurnID) != turnID {
			continue
		}
		if jobAttemptTerminal(attempt.Phase) {
			continue
		}
		if attempt.Phase == JobStartIntent || attempt.Phase == JobStarted {
			result.Ambiguous = true
		}
		attempt.Phase = JobTombstoned
		attempt.Reason = reason
		attempt.UpdatedAt = now
		st.JobAttempts[id] = attempt
		removeJobFromMachines(st, attempt.ID)
		result.Jobs = append(result.Jobs, attempt)
	}
	RemoveTurnSnapshot(st, conversationID, turnID, now)
	sort.Slice(result.Allocations, func(i, j int) bool { return result.Allocations[i].ID < result.Allocations[j].ID })
	sort.Slice(result.Jobs, func(i, j int) bool { return result.Jobs[i].ID < result.Jobs[j].ID })
	return result
}

func TombstoneJobsForMachine(st *State, machine Machine, reason string, now time.Time) []JobAttempt {
	if st == nil {
		return nil
	}
	st.normalize()
	if now.IsZero() {
		now = time.Now()
	}
	reason = firstNonEmpty(reason, "machine was killed")
	jobIDs := map[string]bool{}
	for _, id := range machine.Jobs {
		if strings.TrimSpace(id) != "" {
			jobIDs[strings.TrimSpace(id)] = true
		}
	}
	for id, attempt := range st.JobAttempts {
		if strings.TrimSpace(attempt.Target.MachineID) == strings.TrimSpace(machine.ID) ||
			strings.TrimSpace(attempt.LeaseID) == strings.TrimSpace(machine.LeaseID) ||
			strings.TrimSpace(attempt.ProviderIdentity.ProviderJobID) == strings.TrimSpace(machine.ProviderJobID) {
			jobIDs[id] = true
		}
	}
	var tombstoned []JobAttempt
	for id := range jobIDs {
		attempt, ok := st.JobAttempts[id]
		if !ok || jobAttemptTerminal(attempt.Phase) {
			continue
		}
		attempt.Phase = JobTombstoned
		attempt.Reason = reason
		attempt.UpdatedAt = now
		st.JobAttempts[id] = attempt
		removeJobFromMachines(st, id)
		tombstoned = append(tombstoned, attempt)
	}
	sort.Slice(tombstoned, func(i, j int) bool { return tombstoned[i].ID < tombstoned[j].ID })
	return tombstoned
}

func RecordWorkerHeartbeat(st *State, machineID string, workerID string, now time.Time) (Machine, error) {
	if st == nil {
		return Machine{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	machineID = strings.TrimSpace(machineID)
	if machineID == "" {
		return Machine{}, fmt.Errorf("machine id is required")
	}
	machine, ok := st.Machines[machineID]
	if !ok {
		return Machine{}, fmt.Errorf("beacon machine %q not found", machineID)
	}
	workerID = strings.TrimSpace(workerID)
	if workerID != "" && strings.TrimSpace(machine.WorkerID) != "" && workerID != strings.TrimSpace(machine.WorkerID) {
		return Machine{}, fmt.Errorf("worker id mismatch for machine %q", machineID)
	}
	if workerID != "" {
		machine.WorkerID = workerID
	}
	if now.IsZero() {
		now = time.Now()
	}
	machine.LastHeartbeat = now
	machine.UpdatedAt = now
	st.Machines[machineID] = machine
	return machine, nil
}

func DrainStaleWorkerMachines(st *State, staleAfter time.Duration, now time.Time) []Machine {
	if st == nil {
		return nil
	}
	st.normalize()
	if staleAfter <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	var drained []Machine
	for key, machine := range st.Machines {
		if strings.ToLower(strings.TrimSpace(machine.State)) != string(LeaseAccepting) {
			continue
		}
		if machine.LastHeartbeat.IsZero() || now.Sub(machine.LastHeartbeat) <= staleAfter {
			continue
		}
		machine.State = string(LeaseDraining)
		machine.Reason = "worker heartbeat stale"
		machine.UpdatedAt = now
		st.Machines[key] = machine
		drained = append(drained, machine)
	}
	sort.Slice(drained, func(i, j int) bool { return drained[i].ID < drained[j].ID })
	return drained
}

func DrainIdleWorkerMachines(st *State, idleAfter time.Duration, now time.Time) []Machine {
	if st == nil {
		return nil
	}
	st.normalize()
	if idleAfter <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	var drained []Machine
	for key, machine := range st.Machines {
		if strings.ToLower(strings.TrimSpace(machine.State)) != string(LeaseAccepting) {
			continue
		}
		if machine.ExternalOwned {
			continue
		}
		if len(machine.Chats) > 0 || len(machine.Jobs) > 0 {
			continue
		}
		lastActive := machine.UpdatedAt
		if lastActive.IsZero() || (!machine.LastHeartbeat.IsZero() && machine.LastHeartbeat.After(lastActive)) {
			lastActive = machine.LastHeartbeat
		}
		if lastActive.IsZero() || (!machine.StartedAt.IsZero() && machine.StartedAt.After(lastActive)) {
			lastActive = machine.StartedAt
		}
		if lastActive.IsZero() || now.Sub(lastActive) <= idleAfter {
			continue
		}
		machine.State = string(LeaseDraining)
		machine.Reason = "worker idle timeout"
		machine.UpdatedAt = now
		st.Machines[key] = machine
		drained = append(drained, machine)
	}
	sort.Slice(drained, func(i, j int) bool { return drained[i].ID < drained[j].ID })
	return drained
}

func RecoverStaleJobAttempts(st *State, staleAfter time.Duration, now time.Time) []JobAttempt {
	if st == nil {
		return nil
	}
	st.normalize()
	if staleAfter <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	var recovered []JobAttempt
	for id, attempt := range st.JobAttempts {
		if attempt.UpdatedAt.IsZero() || now.Sub(attempt.UpdatedAt) <= staleAfter {
			continue
		}
		switch attempt.Phase {
		case JobClaimed:
			removeJobFromMachines(st, attempt.ID)
			attempt.Phase = JobQueued
			attempt.WorkerID = ""
			attempt.LeaseID = ""
			attempt.ProviderIdentity = ProviderIdentity{}
			attempt.ClaimEpoch++
			attempt.UpdatedAt = now
		case JobStartIntent, JobStarted:
			attempt.Phase = JobAmbiguous
			attempt.UpdatedAt = now
		default:
			continue
		}
		st.JobAttempts[id] = attempt
		recovered = append(recovered, attempt)
	}
	sort.Slice(recovered, func(i, j int) bool { return recovered[i].ID < recovered[j].ID })
	return recovered
}

func workerDoctorIsZero(doctor WorkerDoctor) bool {
	return doctor == WorkerDoctor{}
}

func workerMembershipProofOK(req AllocationRequest, providerJobID string, proof string) bool {
	proof = strings.TrimSpace(proof)
	if proof == "" {
		return true
	}
	providerJobID = strings.TrimSpace(providerJobID)
	if providerJobID == "" {
		return false
	}
	switch req.Provider {
	case ProviderSlurm:
		return proof == providerJobID || proof == "slurm:"+providerJobID
	case ProviderLSF:
		return proof == providerJobID || proof == "lsf:"+providerJobID
	default:
		return true
	}
}

func workerSharedStoreMatchesProfile(sharedPath string, workerStorePath string) bool {
	sharedPath = NormalizeSharedPath(sharedPath)
	workerStorePath = strings.TrimSpace(workerStorePath)
	if sharedPath == "" || workerStorePath == "" {
		return false
	}
	workerStorePath = filepath.Clean(workerStorePath)
	if workerStorePath == SharedStorePath(sharedPath) {
		return true
	}
	rel, err := filepath.Rel(sharedPath, workerStorePath)
	if err != nil {
		return false
	}
	return rel != "." && rel != "" && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".."
}

func defaultManagedMachineID(req AllocationRequest, providerJobID string, host string) string {
	seed := firstNonEmpty(providerJobID, strings.TrimSpace(host), req.ID)
	name := sanitizeJobName("machine-" + string(req.Provider) + "-" + seed)
	if name == "" {
		return "machine-request"
	}
	if len(name) > 96 {
		name = strings.TrimRight(name[:96], "-_.")
	}
	return name
}

func defaultManagedLeaseID(providerJobID string, machineID string) string {
	seed := sanitizeJobName(firstNonEmpty(providerJobID, machineID))
	if seed == "" {
		seed = "machine"
	}
	name := "lease-" + seed
	if len(name) > 96 {
		name = strings.TrimRight(name[:96], "-_.")
	}
	return name
}

func EnqueueJobAttempt(st *State, requestID string, machine Machine, payload JobPayload, now time.Time) (JobAttempt, bool, error) {
	if st == nil {
		return JobAttempt{}, false, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return JobAttempt{}, false, fmt.Errorf("request id is required")
	}
	req, ok := st.Allocations[requestID]
	if !ok {
		return JobAttempt{}, false, fmt.Errorf("allocation request %q not found", requestID)
	}
	jobID := ManagedJobID(requestID)
	if existing, ok := st.JobAttempts[jobID]; ok {
		return existing, false, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	target := req.Target
	target.MachineID = firstNonEmpty(strings.TrimSpace(target.MachineID), strings.TrimSpace(machine.ID))
	target.LeaseID = firstNonEmpty(strings.TrimSpace(target.LeaseID), strings.TrimSpace(machine.LeaseID))
	target.ProviderJobID = firstNonEmpty(strings.TrimSpace(target.ProviderJobID), strings.TrimSpace(machine.ProviderJobID), strings.TrimSpace(req.ProviderIdentity.ProviderJobID))
	payload.Prompt = strings.TrimSpace(payload.Prompt)
	payload.WorkingDir = strings.TrimSpace(payload.WorkingDir)
	payload.CodexThreadID = strings.TrimSpace(payload.CodexThreadID)
	payload.ImagePaths = trimNonEmptyStrings(payload.ImagePaths)
	attempt := JobAttempt{
		ID:        jobID,
		RequestID: req.ID,
		TurnID:    req.TurnID,
		Attempt:   1,
		Payload:   payload,
		LeaseID:   target.LeaseID,
		Phase:     JobQueued,
		Target:    target,
		ProviderIdentity: ProviderIdentity{
			ProviderJobID: target.ProviderJobID,
		},
		Execution: req.Execution,
		StartedAt: now,
		UpdatedAt: now,
	}
	st.JobAttempts[jobID] = attempt
	machineKey, current, ok := findMachineEntry(*st, machine.ID, machine.LeaseID, machine.ProviderJobID)
	if ok {
		current.Jobs = appendUniqueString(current.Jobs, jobID)
		current.Chats = appendUniqueString(current.Chats, req.ConversationID)
		st.Machines[machineKey] = current
	}
	return attempt, true, nil
}

func ClaimNextJobForMachine(st *State, machineID string, workerID string, now time.Time) (JobAttempt, bool, error) {
	if st == nil {
		return JobAttempt{}, false, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	machineID = strings.TrimSpace(machineID)
	if machineID == "" {
		return JobAttempt{}, false, fmt.Errorf("machine id is required")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return JobAttempt{}, false, fmt.Errorf("worker id is required")
	}
	machine, ok := st.Machines[machineID]
	if !ok {
		return JobAttempt{}, false, fmt.Errorf("beacon machine %q not found", machineID)
	}
	if strings.ToLower(strings.TrimSpace(machine.State)) != string(LeaseAccepting) {
		return JobAttempt{}, false, fmt.Errorf("beacon machine %q is not accepting jobs: state=%s", machineID, machine.State)
	}
	var queued []JobAttempt
	for _, attempt := range st.JobAttempts {
		if attempt.Phase != JobQueued {
			continue
		}
		if !machineCanClaimJob(machine, attempt) {
			continue
		}
		queued = append(queued, attempt)
	}
	sort.Slice(queued, func(i, j int) bool {
		if !queued[i].StartedAt.Equal(queued[j].StartedAt) {
			return queued[i].StartedAt.Before(queued[j].StartedAt)
		}
		return queued[i].ID < queued[j].ID
	})
	if len(queued) == 0 {
		return JobAttempt{}, false, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	claimed := queued[0]
	claimed.WorkerID = workerID
	claimed.LeaseID = machine.LeaseID
	claimed.ClaimEpoch++
	claimed.Phase = JobClaimed
	claimed.UpdatedAt = now
	st.JobAttempts[claimed.ID] = claimed
	return claimed, true, nil
}

func MarkJobStarted(st *State, jobID string, now time.Time) (JobAttempt, error) {
	if st == nil {
		return JobAttempt{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	jobID = strings.TrimSpace(jobID)
	attempt, ok := st.JobAttempts[jobID]
	if !ok {
		return JobAttempt{}, fmt.Errorf("job attempt %q not found", jobID)
	}
	if attempt.Phase != JobClaimed && attempt.Phase != JobStartIntent {
		return JobAttempt{}, fmt.Errorf("job attempt %q cannot start from phase %s", jobID, attempt.Phase)
	}
	if now.IsZero() {
		now = time.Now()
	}
	attempt.Phase = JobStarted
	attempt.UpdatedAt = now
	st.JobAttempts[jobID] = attempt
	return attempt, nil
}

func machineCanClaimJob(machine Machine, attempt JobAttempt) bool {
	if strings.TrimSpace(attempt.Target.MachineID) != "" && strings.TrimSpace(attempt.Target.MachineID) != strings.TrimSpace(machine.ID) {
		return false
	}
	if strings.TrimSpace(attempt.LeaseID) != "" && strings.TrimSpace(attempt.LeaseID) != strings.TrimSpace(machine.LeaseID) {
		return false
	}
	if strings.TrimSpace(attempt.ProviderIdentity.ProviderJobID) != "" && strings.TrimSpace(attempt.ProviderIdentity.ProviderJobID) != strings.TrimSpace(machine.ProviderJobID) {
		return false
	}
	return true
}

func findMachineEntry(st State, ids ...string) (string, Machine, bool) {
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if m, ok := st.Machines[id]; ok {
			return id, m, true
		}
	}
	for key, machine := range st.Machines {
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			if machine.ID == id || machine.LeaseID == id || machine.ProviderJobID == id {
				return key, machine, true
			}
		}
	}
	return "", Machine{}, false
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if strings.TrimSpace(existing) == value {
			return values
		}
	}
	return append(values, value)
}

func trimNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}
