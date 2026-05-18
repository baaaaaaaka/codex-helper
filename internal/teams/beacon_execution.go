package teams

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

var beaconReconcileInterval = 30 * time.Second
var beaconLeaseMaintenanceInterval = 30 * time.Second
var beaconReconcileStaleWorkerAfter = beacon.DefaultWorkerHeartbeatStaleAfter
var beaconReconcileStaleJobAfter = 10 * time.Minute

func (b *Bridge) prepareBeaconTurnExecution(ctx context.Context, session *Session, turn teamstore.Turn) (beacon.TurnExecutionPlan, bool, error) {
	if b == nil || session == nil || strings.TrimSpace(turn.ID) == "" {
		return beacon.TurnExecutionPlan{}, false, nil
	}
	store, err := beacon.NewStore("")
	if err != nil {
		return beacon.TurnExecutionPlan{}, true, fmt.Errorf("beacon state unavailable: %w", err)
	}
	var plan beacon.TurnExecutionPlan
	err = store.Update(func(st *beacon.State) error {
		conv, convOK := st.Conversations[session.ID]
		_, turnOK := beacon.TargetSnapshotForTurn(*st, turn.ID)
		if !turnOK && (!convOK || (conv.Current.Target == "" && conv.Pending == nil && len(conv.Queued) == 0)) {
			plan = beacon.TurnExecutionPlan{
				Action:         beacon.TurnRunLocal,
				ConversationID: session.ID,
				TurnID:         turn.ID,
				Snapshot:       beacon.TargetSnapshot{Target: beacon.TargetLocal},
				Reason:         "no beacon target state",
			}
			return nil
		}
		next, err := beacon.PlanTurnExecution(st, session.ID, turn.ID, time.Now())
		if err != nil {
			return err
		}
		plan = next
		return nil
	})
	if err != nil {
		return beacon.TurnExecutionPlan{}, true, err
	}
	switch plan.Action {
	case beacon.TurnRunLocal:
		return plan, false, nil
	case beacon.TurnWaitAllocation:
		plan, err = b.reconcileBeaconTurnAllocation(ctx, store, plan)
		if err != nil {
			return plan, true, fmt.Errorf("beacon allocation is not ready for this turn: allocation=%s profile=%s action=%s provider_reason=%s reason=%s", plan.AllocationRequestID, plan.Snapshot.Profile, plan.SubmitAction, firstNonEmptyString(plan.ProviderReason, err.Error()), plan.Reason)
		}
		if beaconAllocationCannotProgress(plan) {
			return plan, true, fmt.Errorf("beacon allocation is not ready for this turn: allocation=%s profile=%s action=%s allocation_state=%s provider_job=%s provider_state=%s provider_reason=%s reason=%s", plan.AllocationRequestID, plan.Snapshot.Profile, plan.SubmitAction, plan.AllocationState, plan.ProviderJobID, plan.ProviderState, plan.ProviderReason, plan.Reason)
		}
		return plan, false, nil
	case beacon.TurnRunBeacon:
		return plan, false, nil
	case beacon.TurnReject:
		return plan, true, fmt.Errorf("beacon execution rejected for this turn: %s", plan.Reason)
	default:
		return plan, true, fmt.Errorf("unknown beacon execution plan action %q", plan.Action)
	}
}

func (b *Bridge) maybeRunBeaconReconcile(ctx context.Context, now time.Time) error {
	if b == nil || beaconReconcileInterval <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	if !b.lastBeaconReconcile.IsZero() && now.Sub(b.lastBeaconReconcile) < beaconReconcileInterval {
		return nil
	}
	b.lastBeaconReconcile = now
	store, err := beacon.NewStore("")
	if err != nil {
		return fmt.Errorf("beacon state unavailable: %w", err)
	}
	return b.reconcileBeaconState(ctx, store, beacon.NewCommandProviderAdapterFromEnv(nil), now)
}

func (b *Bridge) reconcileBeaconState(ctx context.Context, store *beacon.Store, adapter beacon.AllocationAdapter, now time.Time) error {
	if store == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	st, err := store.Load()
	if err != nil {
		return err
	}
	var allocations []beacon.AllocationRequest
	for _, req := range st.Allocations {
		if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) == "" {
			continue
		}
		allocations = append(allocations, req)
	}
	for _, req := range allocations {
		query, err := adapter.QueryAllocation(ctx, req)
		if err != nil {
			if beacon.IsProviderCommandNotConfigured(err) {
				continue
			}
			query.QueryError = true
			query.Reason = err.Error()
		}
		if strings.TrimSpace(query.RawState) == "" && strings.TrimSpace(query.Reason) == "" && query.ProviderDeadline.IsZero() {
			continue
		}
		if err := store.Update(func(st *beacon.State) error {
			current, ok := st.Allocations[req.ID]
			if !ok {
				return nil
			}
			if strings.TrimSpace(query.RawState) != "" {
				current.RawProviderState = strings.TrimSpace(query.RawState)
			}
			if strings.TrimSpace(query.Reason) != "" {
				current.ProviderReason = strings.TrimSpace(query.Reason)
			}
			if !query.ProviderDeadline.IsZero() {
				current.ProviderDeadline = query.ProviderDeadline
			}
			current.UpdatedAt = now
			st.Allocations[current.ID] = current
			beacon.ApplyAllocationDeadlineToMachines(st, current, now)
			if strings.TrimSpace(current.RawProviderState) != "" {
				projection := beacon.ProjectRawProviderState(current.Provider, current.RawProviderState, current.ProviderReason, beaconAllocationHasStartedJobState(*st, current.ID), beaconAllocationHasEverRunState(current))
				_, _ = beacon.UpdateAllocationProjection(st, current.ID, projection, now)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return store.Update(func(st *beacon.State) error {
		beacon.DrainStaleWorkerMachines(st, beaconReconcileStaleWorkerAfter, now)
		beacon.RecoverStaleJobAttempts(st, beaconReconcileStaleJobAfter, now)
		return nil
	})
}

func (b *Bridge) maybeRunBeaconLeaseMaintenance(ctx context.Context, now time.Time) error {
	if b == nil || beaconLeaseMaintenanceInterval <= 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	if !b.lastBeaconLeaseMaintenance.IsZero() && now.Sub(b.lastBeaconLeaseMaintenance) < beaconLeaseMaintenanceInterval {
		return nil
	}
	b.lastBeaconLeaseMaintenance = now
	store, err := beacon.NewStore("")
	if err != nil {
		return fmt.Errorf("beacon state unavailable: %w", err)
	}
	opts := beacon.AllocationRenewOptions{}
	if control, blocked, err := b.serviceControlBlocksNewWork(ctx); err != nil {
		return err
	} else if blocked {
		if control.Draining {
			opts.StartedOnly = true
		} else {
			return nil
		}
	}
	return b.renewDueBeaconAllocations(ctx, store, beacon.NewCommandProviderAdapterFromEnv(nil), opts, now)
}

func (b *Bridge) renewDueBeaconAllocations(ctx context.Context, store *beacon.Store, adapter beacon.AllocationRenewAdapter, opts beacon.AllocationRenewOptions, now time.Time) error {
	if store == nil || adapter == nil {
		return nil
	}
	st, err := store.Load()
	if err != nil {
		return err
	}
	var requestIDs []string
	for _, req := range st.Allocations {
		if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) == "" {
			continue
		}
		if beacon.AllocationLeaseDeadline(st, req).IsZero() {
			continue
		}
		requestIDs = append(requestIDs, req.ID)
	}
	sort.Strings(requestIDs)
	var errorsOut []string
	for _, requestID := range requestIDs {
		req, action, renewErr := beacon.ReconcileAllocationRenewOutsideLock(ctx, store, requestID, adapter, opts, now)
		if renewErr != nil {
			if beacon.IsProviderCommandNotConfigured(renewErr) {
				continue
			}
			errorsOut = append(errorsOut, fmt.Sprintf("%s: %v", requestID, renewErr))
			continue
		}
		if action == beacon.AllocationRenewNow && b.out != nil {
			_, _ = fmt.Fprintf(b.out, "Teams beacon lease renewed: allocation=%s provider_job=%s deadline=%s\n", req.ID, req.ProviderIdentity.ProviderJobID, req.ProviderDeadline.Format(time.RFC3339))
		}
	}
	if len(errorsOut) > 0 {
		return fmt.Errorf("beacon renewal errors: %s", strings.Join(errorsOut, "; "))
	}
	return nil
}

func beaconAllocationHasStartedJobState(st beacon.State, allocationID string) bool {
	return beacon.AllocationHasStartedJob(st, allocationID)
}

func beaconAllocationHasEverRunState(req beacon.AllocationRequest) bool {
	switch req.State {
	case beacon.AllocationRunning, beacon.AllocationExpired, beacon.AllocationFailed:
		return true
	default:
		return false
	}
}

func beaconAllocationCannotProgress(plan beacon.TurnExecutionPlan) bool {
	if plan.AllocationState == beacon.AllocationNeedsAttention {
		return true
	}
	if plan.SubmitAction == beacon.AllocationSubmitAttention {
		return true
	}
	if strings.TrimSpace(plan.ProviderJobID) == "" && strings.Contains(strings.ToLower(plan.ProviderReason), "not configured") {
		return true
	}
	return false
}

func (b *Bridge) reconcileBeaconTurnAllocation(ctx context.Context, store *beacon.Store, plan beacon.TurnExecutionPlan) (beacon.TurnExecutionPlan, error) {
	if strings.TrimSpace(plan.AllocationRequestID) == "" {
		return plan, nil
	}
	var req beacon.AllocationRequest
	var action beacon.AllocationSubmitAction
	var reconcileErr error
	req, action, reconcileErr = beacon.ReconcileAllocationSubmitOutsideLock(ctx, store, plan.AllocationRequestID, beacon.NewCommandProviderAdapterFromEnv(nil), time.Now())
	updateBeaconTurnPlanFromAllocation(&plan, req, action)
	return plan, reconcileErr
}

func (b *Bridge) cancelBeaconTurn(ctx context.Context, session *Session, turn teamstore.Turn, reason string) error {
	if b == nil || session == nil || strings.TrimSpace(turn.ID) == "" {
		return nil
	}
	store, err := beacon.NewStore("")
	if err != nil {
		return fmt.Errorf("beacon state unavailable: %w", err)
	}
	var result beacon.CancelTurnResult
	now := time.Now()
	if err := store.Update(func(st *beacon.State) error {
		result = beacon.CancelTurn(st, session.ID, turn.ID, reason, now)
		return nil
	}); err != nil {
		return err
	}
	if len(result.Allocations) == 0 {
		return nil
	}
	adapter := beacon.NewCommandProviderAdapterFromEnv(nil)
	for _, req := range result.Allocations {
		if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) == "" {
			continue
		}
		cancelResult, cancelErr := adapter.CancelAllocation(ctx, req)
		cancelReason := ""
		rawState := ""
		if cancelErr != nil {
			if beacon.IsProviderCommandNotConfigured(cancelErr) {
				continue
			}
			cancelReason = cancelErr.Error()
		} else {
			cancelReason = cancelResult.Reason
			rawState = cancelResult.RawState
		}
		if cancelReason == "" && rawState == "" {
			continue
		}
		_ = store.Update(func(st *beacon.State) error {
			current, ok := st.Allocations[req.ID]
			if !ok {
				return nil
			}
			if strings.TrimSpace(rawState) != "" {
				current.RawProviderState = strings.TrimSpace(rawState)
			}
			if strings.TrimSpace(cancelReason) != "" {
				current.ProviderReason = strings.TrimSpace(cancelReason)
			}
			current.UpdatedAt = time.Now()
			st.Allocations[current.ID] = current
			return nil
		})
	}
	return nil
}

func updateBeaconTurnPlanFromAllocation(plan *beacon.TurnExecutionPlan, req beacon.AllocationRequest, action beacon.AllocationSubmitAction) {
	if plan == nil || strings.TrimSpace(req.ID) == "" {
		return
	}
	plan.AllocationRequestID = req.ID
	plan.AllocationState = req.State
	plan.ProviderJobID = req.ProviderIdentity.ProviderJobID
	plan.ProviderState = req.RawProviderState
	plan.ProviderReason = req.ProviderReason
	plan.SubmitAction = action
}

func beaconTurnExecutionStatus(plan beacon.TurnExecutionPlan) string {
	parts := []string{
		"target=" + strings.TrimSpace(plan.Snapshot.Target),
		"profile=" + strings.TrimSpace(plan.Snapshot.Profile),
		"allocation=" + strings.TrimSpace(plan.AllocationRequestID),
		"allocation_state=" + strings.TrimSpace(string(plan.AllocationState)),
		"submit_action=" + strings.TrimSpace(string(plan.SubmitAction)),
		"machine=" + strings.TrimSpace(plan.MachineID),
		"lease=" + strings.TrimSpace(plan.LeaseID),
		"provider_job=" + strings.TrimSpace(plan.ProviderJobID),
		"provider_state=" + strings.TrimSpace(plan.ProviderState),
		"provider_reason=" + strings.TrimSpace(plan.ProviderReason),
	}
	return strings.Join(parts, " ")
}

func (b *Bridge) recordBeaconTurnStartFailure(ctx context.Context, session *Session, turn teamstore.Turn, plan beacon.TurnExecutionPlan, reason string) error {
	if b == nil || session == nil || strings.TrimSpace(turn.ID) == "" {
		return nil
	}
	store, err := beacon.NewStore("")
	if err != nil {
		return fmt.Errorf("beacon state unavailable: %w", err)
	}
	return store.Update(func(st *beacon.State) error {
		if strings.TrimSpace(plan.AllocationRequestID) != "" {
			req, ok := st.Allocations[plan.AllocationRequestID]
			if ok {
				req.State = beacon.AllocationNeedsAttention
				if strings.TrimSpace(req.ProviderReason) == "" {
					req.ProviderReason = strings.TrimSpace(reason)
				}
				req.UpdatedAt = time.Now()
				st.Allocations[plan.AllocationRequestID] = req
			}
		}
		beacon.RemoveTurnSnapshot(st, session.ID, turn.ID, time.Now())
		return nil
	})
}

func (b *Bridge) recordBeaconTurnFinish(ctx context.Context, session *Session, turn teamstore.Turn, plan beacon.TurnExecutionPlan, reason string) error {
	if b == nil || session == nil || strings.TrimSpace(turn.ID) == "" || (plan.Action != beacon.TurnRunBeacon && plan.Action != beacon.TurnWaitAllocation) {
		return nil
	}
	store, err := beacon.NewStore("")
	if err != nil {
		return fmt.Errorf("beacon state unavailable: %w", err)
	}
	return store.Update(func(st *beacon.State) error {
		if strings.TrimSpace(reason) != "" && strings.TrimSpace(plan.AllocationRequestID) != "" {
			req, ok := st.Allocations[plan.AllocationRequestID]
			if ok {
				req.ProviderReason = strings.TrimSpace(reason)
				req.UpdatedAt = time.Now()
				st.Allocations[plan.AllocationRequestID] = req
			}
		}
		beacon.RemoveTurnSnapshot(st, session.ID, turn.ID, time.Now())
		return nil
	})
}
