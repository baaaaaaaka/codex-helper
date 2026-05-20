package teams

import (
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
)

func TestPendingBeaconLifecycleEventsClassifiesFailuresAndExpiry(t *testing.T) {
	st := beacon.State{
		Allocations: map[string]beacon.AllocationRequest{
			"req-failed": {
				ID:               "req-failed",
				ConversationID:   "s001",
				TurnID:           "turn-1",
				Profile:          "gpu",
				Provider:         beacon.ProviderSlurm,
				State:            beacon.AllocationFailed,
				ProviderIdentity: beacon.ProviderIdentity{ProviderJobID: "slurm-1"},
				RawProviderState: "F",
				ProviderReason:   "node failed",
			},
			"req-expired": {
				ID:               "req-expired",
				Profile:          "gpu",
				Provider:         beacon.ProviderSlurm,
				State:            beacon.AllocationExpired,
				ProviderIdentity: beacon.ProviderIdentity{ProviderJobID: "slurm-2"},
				RawProviderState: "CD",
			},
		},
		Machines: map[string]beacon.Machine{
			"machine-stale": {
				ID:            "machine-stale",
				LeaseID:       "lease-stale",
				ProviderJobID: "slurm-1",
				Profile:       "gpu",
				State:         string(beacon.LeaseDraining),
				Reason:        "worker heartbeat stale",
			},
			"machine-idle": {
				ID:            "machine-idle",
				LeaseID:       "lease-idle",
				ProviderJobID: "slurm-3",
				Profile:       "gpu",
				State:         string(beacon.LeaseDraining),
				Reason:        "worker idle timeout",
			},
		},
	}

	events := pendingBeaconLifecycleEvents(st)
	joined := lifecycleEventKinds(events)
	for _, want := range []string{
		"allocation_failed:req-failed:alert",
		"allocation_expired:req-expired:info",
		"machine_failed:req-failed:alert",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("lifecycle events missing %q: %s", want, joined)
		}
	}
	if strings.Contains(joined, "machine-idle") {
		t.Fatalf("idle drain should not alert as a machine failure: %s", joined)
	}
}

func TestPendingBeaconLifecycleEventsDeduplicatesByResource(t *testing.T) {
	st := beacon.State{
		Allocations: map[string]beacon.AllocationRequest{
			"req-failed": {
				ID:             "req-failed",
				Profile:        "gpu",
				Provider:       beacon.ProviderSlurm,
				State:          beacon.AllocationNeedsAttention,
				ProviderReason: "new scheduler reason",
			},
		},
		Machines: map[string]beacon.Machine{
			"machine-failed": {
				ID:     "machine-failed",
				State:  string(beacon.LeaseLost),
				Reason: "new node reason",
			},
		},
		Notifications: map[string]beacon.LifecycleNotificationRecord{},
	}
	beacon.RecordLifecycleNotification(&st, beacon.LifecycleNotificationRecord{
		ID:           "old-allocation-notification",
		Kind:         beacon.LifecycleNotificationAllocationFailed,
		AllocationID: "req-failed",
		Reason:       "old scheduler reason",
	}, timeNowForLifecycleTest())
	beacon.RecordLifecycleNotification(&st, beacon.LifecycleNotificationRecord{
		ID:        "old-machine-notification",
		Kind:      beacon.LifecycleNotificationMachineFailed,
		MachineID: "machine-failed",
		Reason:    "old node reason",
	}, timeNowForLifecycleTest())

	if events := pendingBeaconLifecycleEvents(st); len(events) != 0 {
		t.Fatalf("already reported resources should not produce duplicate lifecycle events: %#v", events)
	}
}

func TestMachineLifecycleDemandDoesNotMatchEmptyFences(t *testing.T) {
	st := beacon.State{
		Allocations: map[string]beacon.AllocationRequest{
			"req-empty": {
				ID:             "req-empty",
				ConversationID: "wrong-session",
				TurnID:         "wrong-turn",
			},
		},
		JobAttempts: map[string]beacon.JobAttempt{
			"job-empty": {
				ID:        "job-empty",
				RequestID: "req-empty",
				TurnID:    "wrong-turn",
			},
		},
	}
	if conv, turn, req := machineLifecycleDemand(st, beacon.Machine{ID: "machine-a"}); conv != "" || turn != "" || req != "" {
		t.Fatalf("empty lease/provider fences should not associate a machine with unrelated demand: conv=%q turn=%q req=%q", conv, turn, req)
	}

	st.JobAttempts["job-match"] = beacon.JobAttempt{
		ID:        "job-match",
		RequestID: "req-empty",
		TurnID:    "turn-match",
	}
	conv, turn, req := machineLifecycleDemand(st, beacon.Machine{ID: "machine-a", Jobs: []string{"job-match"}})
	if conv != "wrong-session" || turn != "turn-match" || req != "req-empty" {
		t.Fatalf("explicit job binding should still associate demand: conv=%q turn=%q req=%q", conv, turn, req)
	}
}

func timeNowForLifecycleTest() time.Time {
	return time.Unix(1, 0)
}

func lifecycleEventKinds(events []beacon.LifecycleNotificationRecord) string {
	var parts []string
	for _, event := range events {
		id := event.AllocationID
		if id == "" {
			id = event.MachineID
		}
		parts = append(parts, string(event.Kind)+":"+id+":"+event.Severity)
	}
	return strings.Join(parts, ",")
}
