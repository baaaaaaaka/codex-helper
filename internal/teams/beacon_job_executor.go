package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
)

var beaconJobPollInterval = 200 * time.Millisecond
var beaconAllocationPollInterval = time.Second

type BeaconJobExecutor struct {
	Plan beacon.TurnExecutionPlan
}

func (e BeaconJobExecutor) Run(ctx context.Context, session *Session, prompt string) (ExecutionResult, error) {
	return e.RunInput(ctx, session, ExecutionInput{Prompt: prompt})
}

func (e BeaconJobExecutor) RunInput(ctx context.Context, session *Session, input ExecutionInput) (ExecutionResult, error) {
	store, err := beacon.NewStore("")
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("beacon state unavailable: %w", err)
	}
	plan := e.Plan
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
	return waitBeaconJobTerminal(ctx, store, job.ID)
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
	interval := beaconJobPollInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, done, err := readBeaconJobTerminal(store, jobID)
		if done || err != nil {
			return result, err
		}
		select {
		case <-ctx.Done():
			return ExecutionResult{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func readBeaconJobTerminal(store *beacon.Store, jobID string) (ExecutionResult, bool, error) {
	st, err := store.Load()
	if err != nil {
		return ExecutionResult{}, false, err
	}
	if rec, ok := st.Terminals[strings.TrimSpace(jobID)]; ok && strings.TrimSpace(rec.Payload) != "" {
		var payload beacon.JobTerminalPayload
		if err := json.Unmarshal([]byte(rec.Payload), &payload); err != nil {
			return ExecutionResult{}, true, fmt.Errorf("parse beacon terminal payload for %s: %w", jobID, err)
		}
		result := ExecutionResult{
			Text:             payload.Text,
			CodexThreadID:    payload.CodexThreadID,
			CodexThreadTitle: payload.CodexThreadTitle,
			CodexTurnID:      payload.CodexTurnID,
		}
		if strings.TrimSpace(payload.Error) != "" {
			return result, true, fmt.Errorf("beacon worker failed: %s", strings.TrimSpace(payload.Error))
		}
		return result, true, nil
	}
	if attempt, ok := st.JobAttempts[strings.TrimSpace(jobID)]; ok {
		switch attempt.Phase {
		case beacon.JobQuarantined, beacon.JobTombstoned:
			return ExecutionResult{}, true, fmt.Errorf("beacon job %s cannot complete from phase %s", jobID, attempt.Phase)
		}
	}
	return ExecutionResult{}, false, nil
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
