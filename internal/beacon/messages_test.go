package beacon

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestTurnStartFailureNoticeProviderAdapterMissingIsActionable(t *testing.T) {
	plan := TurnExecutionPlan{
		Action:              TurnWaitAllocation,
		Snapshot:            TargetSnapshot{Target: TargetBeacon, Profile: "fgx_dev"},
		AllocationRequestID: "req-b7ae",
		AllocationState:     AllocationRequestPersisted,
		SubmitAction:        AllocationSubmitWait,
		Reason:              "explicit beacon target requires managed allocation; local fallback is disabled",
	}
	err := fmt.Errorf("reconcile: %w", ProviderCommandNotConfiguredError{Provider: ProviderSlurm, EnvName: BeaconSlurmQueryCommandEnv})
	msg := TurnStartFailureNotice(plan, err).Render()
	for _, want := range []string{
		"Beacon cannot start: Slurm provider adapter is not configured.",
		"Summary:",
		"This Work chat targets `beacon:fgx_dev`.",
		"explicit beacon targets disable local fallback",
		"State:",
		"allocation: `req-b7ae`",
		"What cxp is doing:",
		"Action needed:",
		"CODEX_HELPER_BEACON_SLURM_QUERY",
		"helper reload now",
		"Details:",
		"error_code: `BEACON_PROVIDER_ADAPTER_NOT_CONFIGURED`",
		"provider_job: `<none>`",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("notice missing %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "beacon allocation is not ready") {
		t.Fatalf("notice should not use vague legacy wording:\n%s", msg)
	}
	if strings.Index(msg, "Action needed:") > strings.Index(msg, "Details:") {
		t.Fatalf("action steps should appear before technical details:\n%s", msg)
	}
}

func TestConversationStatusNoticePendingSchedulerSeparatesNextAndDetails(t *testing.T) {
	st := State{
		Conversations: map[string]Conversation{
			"conv": {ID: "conv", Current: TargetSnapshot{Target: TargetBeacon, Profile: "gpu"}},
		},
		Allocations: map[string]AllocationRequest{
			"req-1": {
				ID:               "req-1",
				ConversationID:   "conv",
				TurnID:           "turn-1",
				Profile:          "gpu",
				Provider:         ProviderSlurm,
				State:            AllocationSubmitted,
				ProviderIdentity: ProviderIdentity{ProviderJobID: "slurm-1"},
				RawProviderState: "PD",
				ProviderReason:   "Resources",
				UpdatedAt:        time.Unix(1, 0),
			},
		},
	}
	msg := ConversationStatusNotice(st, "conv").Render()
	for _, want := range []string{
		"Beacon status: waiting for Slurm.",
		"Current target: `beacon:gpu`.",
		"Allocation `req-1`, profile `gpu`, provider Slurm, state `submitted`, provider job `slurm-1`, provider state `PD`, reason `Resources`.",
		"No action is needed yet.",
		"status_code: `BEACON_SCHEDULER_PENDING`",
		"provider_reason: `Resources`",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("status notice missing %q:\n%s", want, msg)
		}
	}
}

func TestMachineStatusNoticeShowsBootstrapDiagnostics(t *testing.T) {
	msg := MachineStatusNotice(Machine{
		ID:            "machine-1",
		LeaseID:       "lease-1",
		ProviderJobID: "slurm-1",
		Profile:       "gpu",
		Host:          "node-a",
		State:         string(LeaseAccepting),
		LastHeartbeat: time.Unix(10, 0).UTC(),
		Bootstrap: BootstrapDiagnostics{
			NodeList:        "node-a",
			StdoutPath:      "/logs/slurm.out",
			StderrPath:      "/logs/slurm.err",
			SharedStorePath: "/shared/beacon/state.json",
			CodexPath:       "/usr/bin/codex",
			CXPPath:         "/usr/bin/cxp",
			ProtocolVersion: "1",
		},
	}).Render()
	for _, want := range []string{
		"Scheduler node list: `node-a`.",
		"node_list: `node-a`",
		"bootstrap_stdout: `/logs/slurm.out`",
		"shared_store: `/shared/beacon/state.json`",
		"codex_path: `/usr/bin/codex`",
		"cxp_path: `/usr/bin/cxp`",
		"worker_protocol: `1`",
		"last_heartbeat: `1970-01-01T00:00:10Z`",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("machine status missing %q:\n%s", want, msg)
		}
	}
}

func TestRenderBeaconErrorUsesNoticeShape(t *testing.T) {
	msg := RenderBeaconError(BeaconErrorContext{
		Phase:          "bootstrap",
		Target:         "beacon:gpu",
		ProviderJobID:  "slurm-123",
		ProviderState:  "R",
		ProviderReason: "missing shared root",
		ConversationID: "conv",
		JobID:          "job-1",
		Retry:          "unsafe",
		Next:           "beacon status --job job-1",
	})
	for _, want := range []string{
		"Beacon needs attention: bootstrap.",
		"Summary:",
		"Action needed:",
		"phase: `bootstrap`",
		"target: `beacon:gpu`",
		"provider_reason: `missing shared root`",
		"retry: `unsafe`",
		"next: `beacon status --job job-1`",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error notice missing %q:\n%s", want, msg)
		}
	}
}
