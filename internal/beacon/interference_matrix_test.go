package beacon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The generated matrix tests are driven by a small safety oracle rather than by
// the current implementation's incidental output:
//
//   - provider loss before possible start may be replaced; provider loss after
//     claim/start must not replay the user task automatically;
//   - cancel before provider reconciliation is terminal for the allocation and
//     later provider data may only update raw diagnostics;
//   - provider loss before cancel preserves the diagnostic job state. Claimed
//     jobs stay failed/tombstoned; possible-start jobs stay quarantined while
//     the allocation records the later cancel intent;
//   - renewal is protective only for known provider jobs and must be skipped
//     during drain unless the job may already have started;
//   - stale renew results lose to newer cancel or provider-job changes.
//
// Those rules come from the beacon interference matrix and from the "no silent
// replay after possible execution" guarantee. If product semantics change,
// update this oracle first and then adjust the generated expectations.

type generatedJobPhase struct {
	name    string
	phase   JobPhase
	present bool
}

func generatedJobPhases() []generatedJobPhase {
	return []generatedJobPhase{
		{name: "no-job"},
		{name: "queued", phase: JobQueued, present: true},
		{name: "claimed", phase: JobClaimed, present: true},
		{name: "start-intent", phase: JobStartIntent, present: true},
		{name: "started", phase: JobStarted, present: true},
	}
}

func TestGeneratedProviderLossReplacementMatrix(t *testing.T) {
	now := time.Unix(1779090000, 0)
	for _, phase := range generatedJobPhases() {
		t.Run(phase.name, func(t *testing.T) {
			st := generatedAllocationState(phase, now)
			started := AllocationHasStartedJob(st, "req-1")
			projection := ProjectRawProviderState(ProviderSlurm, "F", "matrix provider failure", started, true)
			req, err := UpdateAllocationProjection(&st, "req-1", projection, now.Add(time.Second))
			if err != nil {
				t.Fatalf("UpdateAllocationProjection: %v", err)
			}

			switch phase.phase {
			case "":
				assertGeneratedReplacement(t, st, req, phase)
			case JobQueued:
				assertGeneratedReplacement(t, st, req, phase)
				if len(st.Machines["machine-1"].Jobs) != 0 {
					t.Fatalf("replacement should remove queued job from old machine: %#v", st.Machines["machine-1"])
				}
			case JobClaimed:
				if req.State != AllocationFailed || st.JobAttempts["job-1"].Phase != JobTombstoned {
					t.Fatalf("claimed job must not auto-replace: req=%#v job=%#v", req, st.JobAttempts["job-1"])
				}
			case JobStartIntent, JobStarted:
				if req.State != AllocationNeedsAttention || st.JobAttempts["job-1"].Phase != JobQuarantined {
					t.Fatalf("possible-start job must quarantine instead of replaying: req=%#v job=%#v", req, st.JobAttempts["job-1"])
				}
			default:
				t.Fatalf("unhandled generated phase %s", phase.phase)
			}
		})
	}
}

func assertGeneratedReplacement(t *testing.T, st State, req AllocationRequest, phase generatedJobPhase) {
	t.Helper()
	if req.State != AllocationRequestPersisted ||
		strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" ||
		strings.TrimSpace(req.Target.ProviderJobID) != "" ||
		strings.TrimSpace(req.Target.MachineID) != "" ||
		strings.TrimSpace(req.Target.LeaseID) != "" ||
		req.SubmitAttempts != 0 ||
		req.ReplacementID != "slurm-1" ||
		req.ReplacementEpoch != 1 {
		t.Fatalf("pre-start loss should reset allocation for replacement: %#v", req)
	}
	if phase.present {
		job := st.JobAttempts["job-1"]
		if job.Phase != JobQueued ||
			strings.TrimSpace(job.ProviderIdentity.ProviderJobID) != "" ||
			strings.TrimSpace(job.Target.ProviderJobID) != "" ||
			strings.TrimSpace(job.Target.MachineID) != "" ||
			strings.TrimSpace(job.Target.LeaseID) != "" {
			t.Fatalf("pre-start replacement should clear old job binding: %#v", job)
		}
	}
}

type generatedDeadlineCase struct {
	name     string
	deadline time.Time
}

func generatedDeadlineCases(now time.Time) []generatedDeadlineCase {
	return []generatedDeadlineCase{
		{name: "no-deadline"},
		{name: "far", deadline: now.Add(time.Hour)},
		{name: "due", deadline: now.Add(time.Minute)},
		{name: "urgent", deadline: now.Add(10 * time.Second)},
	}
}

func TestGeneratedRenewalServiceModeMatrix(t *testing.T) {
	now := time.Unix(1779090000, 0)
	for _, serviceDraining := range []bool{false, true} {
		for _, phase := range generatedJobPhases() {
			for _, deadlineCase := range generatedDeadlineCases(now) {
				name := strings.Join([]string{
					boolName("drain", serviceDraining),
					phase.name,
					deadlineCase.name,
				}, "/")
				t.Run(name, func(t *testing.T) {
					store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
					if err != nil {
						t.Fatalf("NewStore: %v", err)
					}
					st := generatedAllocationState(phase, now)
					req := st.Allocations["req-1"]
					req.ProviderDeadline = deadlineCase.deadline
					st.Allocations[req.ID] = req
					if machine, ok := st.Machines["machine-1"]; ok {
						machine.LeaseExpiresAt = deadlineCase.deadline
						st.Machines[machine.ID] = machine
					}
					if err := store.Save(st); err != nil {
						t.Fatalf("Save: %v", err)
					}

					adapter := &fakeAllocationAdapter{renew: SchedulerQueryResult{RawState: "R", Reason: "renewed", ProviderDeadline: now.Add(time.Hour)}}
					_, action, err := ReconcileAllocationRenewOutsideLock(context.Background(), store, "req-1", adapter, AllocationRenewOptions{StartedOnly: serviceDraining}, now)
					if err != nil {
						t.Fatalf("ReconcileAllocationRenewOutsideLock: %v", err)
					}
					wantRenew := !deadlineCase.deadline.IsZero() &&
						!deadlineCase.deadline.After(now.Add(DefaultRenewBeforeDeadline)) &&
						(!serviceDraining || phase.phase == JobStartIntent || phase.phase == JobStarted)
					if wantRenew {
						if action != AllocationRenewNow || adapter.renews != 1 {
							t.Fatalf("expected renewal, action=%s renews=%d", action, adapter.renews)
						}
					} else if adapter.renews != 0 || action == AllocationRenewNow {
						t.Fatalf("unexpected renewal, action=%s renews=%d", action, adapter.renews)
					}
				})
			}
		}
	}
}

func TestGeneratedRenewalRaceMatrix(t *testing.T) {
	now := time.Unix(1779090000, 0)
	tests := []struct {
		name  string
		hook  func(*Store)
		check func(*testing.T, State)
	}{
		{
			name: "cancel-before-renew-result",
			hook: func(store *Store) {
				_ = store.Update(func(st *State) error {
					CancelTurn(st, "conv", "turn-1", "matrix cancel", now.Add(time.Second))
					return nil
				})
			},
			check: func(t *testing.T, st State) {
				if req := st.Allocations["req-1"]; req.State != AllocationCanceled || !req.ProviderDeadline.Equal(now.Add(time.Minute)) {
					t.Fatalf("cancel must win over stale renew result: %#v", req)
				}
			},
		},
		{
			name: "provider-job-changed-before-renew-result",
			hook: func(store *Store) {
				_ = store.Update(func(st *State) error {
					req := st.Allocations["req-1"]
					req.ProviderIdentity.ProviderJobID = "slurm-2"
					st.Allocations[req.ID] = req
					return nil
				})
			},
			check: func(t *testing.T, st State) {
				if req := st.Allocations["req-1"]; req.ProviderIdentity.ProviderJobID != "slurm-2" || !req.ProviderDeadline.Equal(now.Add(time.Minute)) {
					t.Fatalf("new provider job must win over stale renew result: %#v", req)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
			if err != nil {
				t.Fatalf("NewStore: %v", err)
			}
			st := generatedAllocationState(generatedJobPhase{name: "started", phase: JobStarted, present: true}, now)
			req := st.Allocations["req-1"]
			req.ProviderDeadline = now.Add(time.Minute)
			st.Allocations[req.ID] = req
			if err := store.Save(st); err != nil {
				t.Fatalf("Save: %v", err)
			}
			adapter := &hookRenewAdapter{
				result: SchedulerQueryResult{RawState: "R", Reason: "renewed", ProviderDeadline: now.Add(time.Hour)},
				hook:   func() { tt.hook(store) },
			}
			if _, action, err := ReconcileAllocationRenewOutsideLock(context.Background(), store, "req-1", adapter, AllocationRenewOptions{}, now); err != nil || action != AllocationRenewNow {
				t.Fatalf("renew race call: action=%s err=%v", action, err)
			}
			next, err := store.Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			tt.check(t, next)
		})
	}
}

func TestGeneratedCancelTombstoneMatrix(t *testing.T) {
	now := time.Unix(1779090000, 0)
	phases := []JobPhase{JobQueued, JobClaimed, JobStartIntent, JobStarted, JobTerminal, JobQuarantined, JobTombstoned}
	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			st := generatedAllocationState(generatedJobPhase{name: string(phase), phase: phase, present: true}, now)
			CancelTurn(&st, "conv", "turn-1", "matrix cancel", now.Add(time.Second))
			job := st.JobAttempts["job-1"]
			switch phase {
			case JobTerminal, JobQuarantined, JobTombstoned:
				if job.Phase != phase {
					t.Fatalf("terminal job phase should be preserved after cancel: before=%s after=%s", phase, job.Phase)
				}
			default:
				if job.Phase != JobTombstoned || len(st.Machines["machine-1"].Jobs) != 0 {
					t.Fatalf("non-terminal job should tombstone and unbind: before=%s job=%#v machine=%#v", phase, job, st.Machines["machine-1"])
				}
			}
		})
	}
}

type generatedInterferenceOp string

const (
	generatedOpCancel       generatedInterferenceOp = "cancel"
	generatedOpProviderLoss generatedInterferenceOp = "provider-loss"
	generatedOpRenew        generatedInterferenceOp = "renew"
	generatedOpTerminal     generatedInterferenceOp = "terminal"
)

func TestGeneratedInterferenceOrderMatrixCancelDominates(t *testing.T) {
	now := time.Unix(1779090000, 0)
	phases := []generatedJobPhase{
		{name: "queued", phase: JobQueued, present: true},
		{name: "claimed", phase: JobClaimed, present: true},
		{name: "start-intent", phase: JobStartIntent, present: true},
		{name: "started", phase: JobStarted, present: true},
	}
	for _, phase := range phases {
		for _, ops := range generatedPermutations([]generatedInterferenceOp{generatedOpCancel, generatedOpProviderLoss, generatedOpRenew}) {
			t.Run(phase.name+"/"+generatedOpName(ops), func(t *testing.T) {
				store, adapter := generatedSequenceStore(t, phase, now)
				for _, op := range ops {
					runGeneratedInterferenceOp(t, store, adapter, op, now)
				}
				st, err := store.Load()
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				req := st.Allocations["req-1"]
				job := st.JobAttempts["job-1"]
				cancelBeforeProviderLoss := generatedOpIndex(ops, generatedOpCancel) < generatedOpIndex(ops, generatedOpProviderLoss)
				if cancelBeforeProviderLoss || phase.phase == JobQueued {
					if req.State != AllocationCanceled || job.Phase != JobTombstoned {
						t.Fatalf("cancel before provider loss should dominate later reconcile, and pre-start replacement should still be cancelable: ops=%v req=%#v job=%#v", ops, req, job)
					}
					return
				}
				if req.CancelRequestedAt.IsZero() {
					t.Fatalf("cancel after provider loss should still record cancel intent: ops=%v req=%#v", ops, req)
				}
				switch phase.phase {
				case JobClaimed:
					if req.State != AllocationFailed || job.Phase != JobTombstoned {
						t.Fatalf("provider loss before cancel should preserve claimed-job failure diagnostic: ops=%v req=%#v job=%#v", ops, req, job)
					}
				case JobStartIntent, JobStarted:
					if req.State != AllocationCanceled || job.Phase != JobQuarantined || !strings.Contains(req.ProviderReason, "matrix provider failure") {
						t.Fatalf("provider loss before cancel should keep job quarantine while recording cancel intent: ops=%v req=%#v job=%#v", ops, req, job)
					}
				default:
					t.Fatalf("unhandled cancel/provider order phase %s", phase.phase)
				}
			})
		}
	}
}

func TestGeneratedInterferenceOrderMatrixRenewAndProviderLoss(t *testing.T) {
	now := time.Unix(1779090000, 0)
	phases := []generatedJobPhase{
		{name: "queued", phase: JobQueued, present: true},
		{name: "start-intent", phase: JobStartIntent, present: true},
		{name: "started", phase: JobStarted, present: true},
	}
	for _, phase := range phases {
		for _, ops := range generatedPermutations([]generatedInterferenceOp{generatedOpProviderLoss, generatedOpRenew}) {
			t.Run(phase.name+"/"+generatedOpName(ops), func(t *testing.T) {
				store, adapter := generatedSequenceStore(t, phase, now)
				for _, op := range ops {
					runGeneratedInterferenceOp(t, store, adapter, op, now)
				}
				st, err := store.Load()
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				req := st.Allocations["req-1"]
				job := st.JobAttempts["job-1"]
				switch phase.phase {
				case JobQueued:
					if req.State != AllocationRequestPersisted || req.ProviderIdentity.ProviderJobID != "" || job.Phase != JobQueued || job.ProviderIdentity.ProviderJobID != "" {
						t.Fatalf("pre-start provider loss should prepare replacement regardless of renew order: ops=%v req=%#v job=%#v", ops, req, job)
					}
				case JobStartIntent, JobStarted:
					if req.State != AllocationNeedsAttention || job.Phase != JobQuarantined {
						t.Fatalf("possible-start provider loss must not replay regardless of renew order: ops=%v req=%#v job=%#v", ops, req, job)
					}
				}
			})
		}
	}
}

func TestGeneratedInterferenceOrderMatrixTerminalAndCancel(t *testing.T) {
	now := time.Unix(1779090000, 0)
	phase := generatedJobPhase{name: "started", phase: JobStarted, present: true}
	for _, ops := range generatedPermutations([]generatedInterferenceOp{generatedOpTerminal, generatedOpCancel}) {
		t.Run(generatedOpName(ops), func(t *testing.T) {
			store, adapter := generatedSequenceStore(t, phase, now)
			for _, op := range ops {
				runGeneratedInterferenceOp(t, store, adapter, op, now)
			}
			st, err := store.Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			job := st.JobAttempts["job-1"]
			terminalFirst := ops[0] == generatedOpTerminal
			if terminalFirst {
				if job.Phase != JobTerminal || len(st.Terminals) != 1 {
					t.Fatalf("terminal before cancel should remain delivered: ops=%v job=%#v terminals=%#v", ops, job, st.Terminals)
				}
			} else if job.Phase != JobTombstoned || len(st.Terminals) != 0 {
				t.Fatalf("terminal after cancel should be ignored as late write: ops=%v job=%#v terminals=%#v", ops, job, st.Terminals)
			}
		})
	}
}

func TestGeneratedMachineReadinessIsolationAndHealthMatrix(t *testing.T) {
	now := time.Unix(1779090000, 0)
	baseReq := AllocationRequest{
		ID:             "req-1",
		ConversationID: "conv",
		Profile:        "gpu",
		Provider:       ProviderSlurm,
		Isolation:      IsolationShared,
		ProviderIdentity: ProviderIdentity{
			ProviderJobID: "slurm-1",
		},
		Execution: ExecutionSignature{Hash: "sig-a"},
	}
	baseMachine := Machine{
		ID:              "machine-1",
		LeaseID:         "lease-1",
		ProviderJobID:   "slurm-1",
		Profile:         "gpu",
		Isolation:       IsolationShared,
		State:           string(LeaseAccepting),
		ProviderState:   ProviderJobRunning,
		Doctor:          healthyWorkerDoctor(),
		MembershipProof: "slurm:slurm-1",
		Execution:       ExecutionSignature{Hash: "sig-a"},
		LeaseExpiresAt:  now.Add(time.Hour),
		LastHeartbeat:   now.Add(-time.Second),
	}
	tests := []struct {
		name    string
		req     func(*AllocationRequest)
		machine func(*Machine)
		want    bool
	}{
		{name: "shared request accepts shared idle machine", want: true},
		{name: "shared request accepts shared busy machine", machine: func(m *Machine) { m.Jobs = []string{"other-job"} }, want: true},
		{name: "exclusive request accepts exclusive empty machine", req: func(r *AllocationRequest) { r.Isolation = IsolationExclusive }, machine: func(m *Machine) { m.Isolation = IsolationExclusive }, want: true},
		{name: "exclusive request accepts exclusive machine already scoped to same chat", req: func(r *AllocationRequest) { r.Isolation = IsolationExclusive }, machine: func(m *Machine) { m.Isolation = IsolationExclusive; m.Chats = []string{"conv"} }, want: true},
		{name: "exclusive request rejects exclusive busy machine", req: func(r *AllocationRequest) { r.Isolation = IsolationExclusive }, machine: func(m *Machine) { m.Isolation = IsolationExclusive; m.Jobs = []string{"other-job"} }},
		{name: "exclusive request rejects shared machine", req: func(r *AllocationRequest) { r.Isolation = IsolationExclusive }},
		{name: "shared request rejects exclusive machine", machine: func(m *Machine) { m.Isolation = IsolationExclusive }},
		{name: "exclusive request rejects machine scoped to another chat", req: func(r *AllocationRequest) { r.Isolation = IsolationExclusive }, machine: func(m *Machine) { m.Isolation = IsolationExclusive; m.Chats = []string{"other-conv"} }},
		{name: "profile mismatch", machine: func(m *Machine) { m.Profile = "cpu" }},
		{name: "provider job mismatch", machine: func(m *Machine) { m.ProviderJobID = "slurm-2" }},
		{name: "external owned machine", machine: func(m *Machine) { m.ExternalOwned = true }},
		{name: "stale heartbeat", machine: func(m *Machine) { m.LastHeartbeat = now.Add(-10 * time.Minute) }},
		{name: "doctor failure", machine: func(m *Machine) { m.Doctor.CXPAvailable = false }},
		{name: "pending provider state", machine: func(m *Machine) { m.ProviderState = ProviderJobPending }},
		{name: "signature mismatch", machine: func(m *Machine) { m.Execution.Hash = "sig-b" }},
		{name: "short ttl", machine: func(m *Machine) { m.LeaseExpiresAt = now.Add(DefaultMinimumLeaseTTL - time.Second) }},
		{name: "membership proof mismatch", machine: func(m *Machine) { m.MembershipProof = "slurm:slurm-2" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := baseReq
			machine := baseMachine
			if tt.req != nil {
				tt.req(&req)
			}
			if tt.machine != nil {
				tt.machine(&machine)
			}
			st := State{Machines: map[string]Machine{machine.ID: machine}}
			got, ok := readyMachineForAllocation(st, req, now)
			if ok != tt.want {
				t.Fatalf("acceptance=%v want=%v machine=%#v req=%#v selected=%#v", ok, tt.want, machine, req, got)
			}
			if ok && got.ID != machine.ID {
				t.Fatalf("selected wrong machine: %#v", got)
			}
		})
	}
}

type hookRenewAdapter struct {
	result SchedulerQueryResult
	hook   func()
}

func (a *hookRenewAdapter) RenewAllocation(context.Context, AllocationRequest) (SchedulerQueryResult, error) {
	if a.hook != nil {
		a.hook()
	}
	return a.result, nil
}

func generatedAllocationState(phase generatedJobPhase, now time.Time) State {
	st := State{
		Allocations: map[string]AllocationRequest{
			"req-1": {
				ID:                "req-1",
				ConversationID:    "conv",
				TurnID:            "turn-1",
				Profile:           "gpu",
				ProfileSnapshot:   readyProfile("gpu"),
				Provider:          ProviderSlurm,
				State:             AllocationRunning,
				SubmitAttempts:    1,
				DeterministicName: "cxp-req-1",
				ProviderIdentity:  ProviderIdentity{ProviderJobID: "slurm-1"},
				Target:            TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProviderJobID: "slurm-1", MachineID: "machine-1", LeaseID: "lease-1"},
				CreatedAt:         now,
				UpdatedAt:         now,
			},
		},
		Machines: map[string]Machine{
			"machine-1": {
				ID:            "machine-1",
				LeaseID:       "lease-1",
				ProviderJobID: "slurm-1",
				Profile:       "gpu",
				State:         string(LeaseAccepting),
				Jobs:          []string{},
				UpdatedAt:     now,
			},
		},
	}
	if phase.present {
		st.JobAttempts = map[string]JobAttempt{
			"job-1": {
				ID:               "job-1",
				RequestID:        "req-1",
				TurnID:           "turn-1",
				Phase:            phase.phase,
				ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"},
				Target:           TargetSnapshot{Target: TargetBeacon, Profile: "gpu", ProviderJobID: "slurm-1", MachineID: "machine-1", LeaseID: "lease-1"},
				LeaseID:          "lease-1",
				UpdatedAt:        now,
			},
		}
		machine := st.Machines["machine-1"]
		machine.Jobs = []string{"job-1"}
		st.Machines[machine.ID] = machine
	}
	st.normalize()
	return st
}

func generatedSequenceStore(t *testing.T, phase generatedJobPhase, now time.Time) (*Store, *fakeAllocationAdapter) {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "beacon.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	st := generatedAllocationState(phase, now)
	req := st.Allocations["req-1"]
	req.ProviderDeadline = now.Add(time.Minute)
	st.Allocations[req.ID] = req
	if machine, ok := st.Machines["machine-1"]; ok {
		machine.LeaseExpiresAt = now.Add(time.Minute)
		st.Machines[machine.ID] = machine
	}
	if err := store.Save(st); err != nil {
		t.Fatalf("Save: %v", err)
	}
	adapter := &fakeAllocationAdapter{renew: SchedulerQueryResult{RawState: "R", Reason: "renewed", ProviderDeadline: now.Add(time.Hour)}}
	return store, adapter
}

func runGeneratedInterferenceOp(t *testing.T, store *Store, adapter *fakeAllocationAdapter, op generatedInterferenceOp, now time.Time) {
	t.Helper()
	switch op {
	case generatedOpCancel:
		if err := store.Update(func(st *State) error {
			CancelTurn(st, "conv", "turn-1", "matrix cancel", now.Add(time.Second))
			return nil
		}); err != nil {
			t.Fatalf("cancel op: %v", err)
		}
	case generatedOpProviderLoss:
		if err := store.Update(func(st *State) error {
			projection := ProjectRawProviderState(ProviderSlurm, "F", "matrix provider failure", AllocationHasStartedJob(*st, "req-1"), true)
			_, err := UpdateAllocationProjection(st, "req-1", projection, now.Add(2*time.Second))
			return err
		}); err != nil {
			t.Fatalf("provider-loss op: %v", err)
		}
	case generatedOpRenew:
		if _, _, err := ReconcileAllocationRenewOutsideLock(context.Background(), store, "req-1", adapter, AllocationRenewOptions{}, now.Add(3*time.Second)); err != nil {
			t.Fatalf("renew op: %v", err)
		}
	case generatedOpTerminal:
		if err := store.Update(func(st *State) error {
			_, err := AcceptWorkerTerminal(st, WorkerTerminalEnvelope{
				JobID:     "job-1",
				RequestID: "req-1",
				TurnID:    "turn-1",
				LeaseID:   "lease-1",
				ProviderIdentity: ProviderIdentity{
					ProviderJobID: "slurm-1",
				},
				Payload: []byte(`{"text":"matrix terminal"}`),
			}, now.Add(4*time.Second))
			return err
		}); err != nil {
			t.Fatalf("terminal op: %v", err)
		}
	default:
		t.Fatalf("unknown generated op %q", op)
	}
}

func generatedPermutations(in []generatedInterferenceOp) [][]generatedInterferenceOp {
	var out [][]generatedInterferenceOp
	var walk func([]generatedInterferenceOp, []generatedInterferenceOp)
	walk = func(prefix []generatedInterferenceOp, rest []generatedInterferenceOp) {
		if len(rest) == 0 {
			cp := append([]generatedInterferenceOp(nil), prefix...)
			out = append(out, cp)
			return
		}
		for i, op := range rest {
			nextRest := append([]generatedInterferenceOp(nil), rest[:i]...)
			nextRest = append(nextRest, rest[i+1:]...)
			walk(append(prefix, op), nextRest)
		}
	}
	walk(nil, in)
	return out
}

func generatedOpName(ops []generatedInterferenceOp) string {
	parts := make([]string, 0, len(ops))
	for _, op := range ops {
		parts = append(parts, string(op))
	}
	return strings.Join(parts, "-then-")
}

func generatedOpIndex(ops []generatedInterferenceOp, target generatedInterferenceOp) int {
	for i, op := range ops {
		if op == target {
			return i
		}
	}
	return len(ops)
}

func boolName(name string, value bool) string {
	if value {
		return name
	}
	return "no-" + name
}
