package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
)

var beaconJobPollInterval = 200 * time.Millisecond
var beaconAllocationPollInterval = time.Second
var beaconJobStreamDrainInterval = 50 * time.Millisecond

type BeaconJobExecutor struct {
	Plan beacon.TurnExecutionPlan
}

func (e BeaconJobExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunInput(ctx, session, ExecutionInput{Prompt: prompt})
}

func (e BeaconJobExecutor) RunInput(ctx context.Context, session *Session, input ExecutionInput) (ExecutionResult, error) {
	return e.RunInputWithEventHandler(ctx, session, input, nil)
}

func (e BeaconJobExecutor) RunInputWithEventHandler(ctx context.Context, session *Session, input ExecutionInput, handler codexrunner.EventHandler) (ExecutionResult, error) {
	plan := e.Plan
	store, err := beacon.NewStore(plan.StorePath)
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("beacon state unavailable: %w", err)
	}
	if plan.Action == beacon.TurnWaitAllocation {
		plan, err = waitBeaconAllocationReady(ctx, store, plan)
		if err != nil {
			return ExecutionResult{}, err
		}
	}
	if plan.Action != beacon.TurnRunBeacon {
		return ExecutionResult{}, fmt.Errorf("beacon execution cannot run from plan action %q", plan.Action)
	}
	job, err := enqueueBeaconJobForTurn(ctx, store, plan, session, input)
	if err != nil {
		return ExecutionResult{}, err
	}
	return waitBeaconJobTerminalWithEventHandler(ctx, store, job.ID, handler)
}

func waitBeaconAllocationReady(ctx context.Context, store *beacon.Store, initial beacon.TurnExecutionPlan) (beacon.TurnExecutionPlan, error) {
	interval := beaconAllocationPollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plan := initial
	for {
		next, err := refreshBeaconAllocationPlan(ctx, store, plan)
		if err != nil {
			return next, err
		}
		switch next.Action {
		case beacon.TurnRunBeacon:
			return next, nil
		case beacon.TurnWaitAllocation:
			if beaconAllocationCannotProgress(next) {
				return next, fmt.Errorf("%s", beacon.TurnStartFailureNotice(next, nil).Render())
			}
			plan = next
		case beacon.TurnReject:
			return next, fmt.Errorf("beacon execution rejected for this turn: %s", next.Reason)
		case beacon.TurnRunLocal:
			return next, fmt.Errorf("beacon execution unexpectedly resolved to local target while local fallback is disabled")
		default:
			return next, fmt.Errorf("unknown beacon execution plan action %q", next.Action)
		}
		select {
		case <-ctx.Done():
			return plan, ctx.Err()
		case <-ticker.C:
		}
	}
}

func refreshBeaconAllocationPlan(ctx context.Context, store *beacon.Store, previous beacon.TurnExecutionPlan) (beacon.TurnExecutionPlan, error) {
	plan := previous
	var reconcileErr error
	err := store.Update(func(st *beacon.State) error {
		next, err := beacon.PlanTurnExecution(st, previous.ConversationID, previous.TurnID, time.Now())
		if err != nil {
			return err
		}
		next.StorePath = previous.StorePath
		plan = next
		return nil
	})
	if err != nil {
		return plan, err
	}
	if plan.Action == beacon.TurnWaitAllocation && strings.TrimSpace(plan.AllocationRequestID) != "" {
		var req beacon.AllocationRequest
		var action beacon.AllocationSubmitAction
		req, action, reconcileErr = beacon.ReconcileAllocationSubmitOutsideLock(ctx, store, plan.AllocationRequestID, beacon.NewCommandProviderAdapterFromEnv(nil), time.Now())
		updateBeaconTurnPlanFromAllocation(&plan, req, action)
	}
	return plan, reconcileErr
}

func enqueueBeaconJobForTurn(ctx context.Context, store *beacon.Store, plan beacon.TurnExecutionPlan, session *Session, input ExecutionInput) (beacon.JobAttempt, error) {
	var job beacon.JobAttempt
	err := store.Update(func(st *beacon.State) error {
		machine, ok := beaconMachineForPlan(*st, plan, time.Now())
		if !ok {
			return fmt.Errorf("beacon machine is no longer accepting this turn: machine=%s lease=%s provider_job=%s", plan.MachineID, plan.LeaseID, plan.ProviderJobID)
		}
		payload := beacon.JobPayload{
			Prompt:     input.Prompt,
			ImagePaths: append([]string(nil), input.ImagePaths...),
		}
		if session != nil {
			payload.WorkingDir = session.Cwd
			payload.CodexThreadID = session.CodexThreadID
		}
		var err error
		job, _, err = beacon.EnqueueJobAttempt(st, plan.AllocationRequestID, machine, payload, time.Now())
		return err
	})
	if err != nil {
		return beacon.JobAttempt{}, err
	}
	return job, ctx.Err()
}

func waitBeaconJobTerminal(ctx context.Context, store *beacon.Store, jobID string) (ExecutionResult, error) {
	return waitBeaconJobTerminalWithEventHandler(ctx, store, jobID, nil)
}

func waitBeaconJobTerminalWithEventHandler(ctx context.Context, store *beacon.Store, jobID string, handler codexrunner.EventHandler) (ExecutionResult, error) {
	interval := beaconJobPollInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var streamReader *beacon.JobStreamReader
	if handler != nil {
		// Read from the beginning and rely on the request/turn/worker/lease/claim
		// metadata filter below to reject stale attempts. Starting at the current
		// end races a fast worker: events written before this goroutine is first
		// scheduled would otherwise be skipped permanently.
		reader, err := beacon.NewJobStreamReader(store.Path(), jobID, beacon.JobStreamReaderOptions{})
		if err != nil {
			return ExecutionResult{}, err
		}
		streamReader = reader
	}
	for {
		result, done, attempt, attemptOK, err := readBeaconJobTerminalAndAttempt(store, jobID)
		if err == nil {
			dispatchBeaconJobStreamEvents(streamReader, jobID, attempt, attemptOK, handler)
		}
		if done || err != nil {
			drainBeaconJobStream(ctx, store, streamReader, jobID, handler)
			return result, err
		}
		select {
		case <-ctx.Done():
			return ExecutionResult{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func dispatchBeaconJobStreamEvents(reader *beacon.JobStreamReader, jobID string, attempt beacon.JobAttempt, attemptOK bool, handler codexrunner.EventHandler) int {
	if reader == nil || handler == nil {
		return 0
	}
	records, err := reader.ReadAvailable()
	if err != nil {
		return 0
	}
	dispatched := 0
	jobID = strings.TrimSpace(jobID)
	for _, record := range records {
		if !beaconJobStreamRecordMatchesAttempt(record, jobID, attempt, attemptOK) {
			continue
		}
		event := record.Event.StreamEvent()
		if strings.TrimSpace(string(event.Kind)) == "" {
			continue
		}
		handler(event)
		dispatched++
	}
	return dispatched
}

func beaconJobStreamRecordMatchesAttempt(record beacon.JobStreamRecord, jobID string, attempt beacon.JobAttempt, attemptOK bool) bool {
	if strings.TrimSpace(record.JobID) != strings.TrimSpace(jobID) {
		return false
	}
	if !attemptOK {
		return false
	}
	if !beaconJobStreamFieldMatches(record.RequestID, attempt.RequestID) {
		return false
	}
	if !beaconJobStreamFieldMatches(record.TurnID, attempt.TurnID) {
		return false
	}
	if !beaconJobStreamFieldMatches(record.WorkerID, attempt.WorkerID) {
		return false
	}
	if !beaconJobStreamFieldMatches(record.LeaseID, attempt.LeaseID) {
		return false
	}
	if !beaconJobStreamFieldMatches(record.ProviderJobID, attempt.ProviderIdentity.ProviderJobID) {
		return false
	}
	if attempt.ClaimEpoch > 0 && record.ClaimEpoch != attempt.ClaimEpoch {
		return false
	}
	return true
}

func beaconJobStreamFieldMatches(recordValue string, attemptValue string) bool {
	attemptValue = strings.TrimSpace(attemptValue)
	if attemptValue == "" {
		return true
	}
	return strings.TrimSpace(recordValue) == attemptValue
}

func drainBeaconJobStream(ctx context.Context, store *beacon.Store, reader *beacon.JobStreamReader, jobID string, handler codexrunner.EventHandler) {
	if reader == nil || handler == nil {
		return
	}
	interval := beaconJobStreamDrainInterval
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	for stableReads := 0; stableReads < 2; {
		_, _, attempt, attemptOK, err := readBeaconJobTerminalAndAttempt(store, jobID)
		if err != nil {
			stableReads++
			continue
		}
		if dispatchBeaconJobStreamEvents(reader, jobID, attempt, attemptOK, handler) == 0 {
			stableReads++
		} else {
			stableReads = 0
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}
}

func readBeaconJobTerminal(store *beacon.Store, jobID string) (ExecutionResult, bool, error) {
	result, done, _, _, err := readBeaconJobTerminalAndAttempt(store, jobID)
	return result, done, err
}

func readBeaconJobTerminalAndAttempt(store *beacon.Store, jobID string) (ExecutionResult, bool, beacon.JobAttempt, bool, error) {
	st, err := store.Load()
	if err != nil {
		return ExecutionResult{}, false, beacon.JobAttempt{}, false, err
	}
	jobID = strings.TrimSpace(jobID)
	attempt, attemptOK := st.JobAttempts[jobID]
	if rec, ok := st.Terminals[jobID]; ok && strings.TrimSpace(rec.Payload) != "" {
		var payload beacon.JobTerminalPayload
		if err := json.Unmarshal([]byte(rec.Payload), &payload); err != nil {
			return ExecutionResult{}, true, attempt, attemptOK, fmt.Errorf("parse beacon terminal payload for %s: %w", jobID, err)
		}
		result := ExecutionResult{
			Text:             payload.Text,
			CodexThreadID:    payload.CodexThreadID,
			CodexThreadTitle: payload.CodexThreadTitle,
			CodexTurnID:      payload.CodexTurnID,
		}
		if strings.TrimSpace(payload.Error) != "" {
			return result, true, attempt, attemptOK, fmt.Errorf("beacon worker failed: %s", strings.TrimSpace(payload.Error))
		}
		return result, true, attempt, attemptOK, nil
	}
	if attemptOK {
		switch attempt.Phase {
		case beacon.JobQuarantined, beacon.JobTombstoned:
			return ExecutionResult{}, true, attempt, true, fmt.Errorf("beacon job %s cannot complete from phase %s", jobID, attempt.Phase)
		}
	}
	return ExecutionResult{}, false, attempt, attemptOK, nil
}

func beaconMachineForPlan(st beacon.State, plan beacon.TurnExecutionPlan, now time.Time) (beacon.Machine, bool) {
	req, ok := st.Allocations[strings.TrimSpace(plan.AllocationRequestID)]
	if !ok {
		return beacon.Machine{}, false
	}
	for _, machine := range st.Machines {
		if strings.TrimSpace(machine.ID) != strings.TrimSpace(plan.MachineID) {
			continue
		}
		if strings.TrimSpace(machine.LeaseID) != strings.TrimSpace(plan.LeaseID) {
			continue
		}
		if strings.TrimSpace(machine.ProviderJobID) != strings.TrimSpace(plan.ProviderJobID) {
			continue
		}
		if strings.ToLower(strings.TrimSpace(machine.State)) != string(beacon.LeaseAccepting) {
			continue
		}
		if !beacon.MachineCanAcceptAllocation(machine, req, now) {
			continue
		}
		return machine, true
	}
	return beacon.Machine{}, false
}
