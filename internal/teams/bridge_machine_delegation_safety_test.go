package teams

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams/delegation"
)

func newDelegationOutboxSafetyPublisher(t *testing.T, graph *fakeBridgeMachineRegistryGraph) *bridgeMachineRegistryPublisher {
	t.Helper()
	bridge := testBridgeForMachineRegistry()
	publisher, err := bridge.newBridgeMachineRegistryPublisher(BridgeOptions{
		HelperVersion:            "v-test",
		MachineRegistryGraph:     graph,
		MachineRegistryCachePath: filepath.Join(t.TempDir(), "machine-registry.json"),
		MachineRegistryNow:       func() time.Time { return time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	publisher.delegationStatePath = filepath.Join(t.TempDir(), "delegation-worker-state.json")
	return publisher
}

func newDelegationOutboxSafetyRecord(t *testing.T) delegation.Record {
	t.Helper()
	record, err := delegation.NewClaimRecord("delegation-safety", "machine-a", "worker-a", 1, time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("new claim: %v", err)
	}
	return record
}

func loadDelegationOutboxSafetyRecord(t *testing.T, path string, recordID string) delegation.OutboxRecord {
	t.Helper()
	store, err := delegation.LoadStore(path)
	if err != nil {
		t.Fatalf("load delegation store: %v", err)
	}
	outbox, ok := store.OutboxForRecordID(recordID)
	if !ok {
		t.Fatalf("outbox %q is missing", recordID)
	}
	return outbox
}

func TestMachineDelegationUnknownPOSTAcceptedButVisibilityReadFailsIsNotReplayed(t *testing.T) {
	graph := newFakeBridgeMachineRegistryGraph()
	publisher := newDelegationOutboxSafetyPublisher(t, graph)
	record := newDelegationOutboxSafetyRecord(t)
	graph.sendErr = errors.New("transport lost after Graph accepted POST")
	graph.sendErrAfterAdd = true
	graph.listErr = errors.New("visibility read unavailable")
	graph.listErrOnce = true

	if err := publisher.sendDelegationInboxRecord(context.Background(), "chat-inbox", record); err == nil {
		t.Fatal("first send unexpectedly succeeded")
	}
	outbox := loadDelegationOutboxSafetyRecord(t, publisher.delegationStatePath, record.RecordID)
	if outbox.Status != delegation.OutboxUnknown || outbox.Attempts != 1 {
		t.Fatalf("outbox after ambiguous accepted POST = %#v, want unknown/one attempt", outbox)
	}
	if graph.sendCount != 1 {
		t.Fatalf("send count after first attempt = %d, want 1", graph.sendCount)
	}

	if err := publisher.sendDelegationInboxRecord(context.Background(), "chat-inbox", record); err != nil {
		t.Fatalf("reconcile accepted POST: %v", err)
	}
	if graph.sendCount != 1 {
		t.Fatalf("send count after reconcile = %d, want no replay", graph.sendCount)
	}
	outbox = loadDelegationOutboxSafetyRecord(t, publisher.delegationStatePath, record.RecordID)
	if outbox.Status != delegation.OutboxVisible {
		t.Fatalf("outbox after reconcile = %#v, want visible", outbox)
	}
}

func TestMachineDelegationUnknownPOSTWithoutVisibilityEvidenceSuppressesReplay(t *testing.T) {
	graph := newFakeBridgeMachineRegistryGraph()
	publisher := newDelegationOutboxSafetyPublisher(t, graph)
	record := newDelegationOutboxSafetyRecord(t)
	graph.sendErr = errors.New("ambiguous transport failure")
	graph.listErr = errors.New("visibility read unavailable")
	graph.listErrOnce = true

	if err := publisher.sendDelegationInboxRecord(context.Background(), "chat-inbox", record); err == nil {
		t.Fatal("first send unexpectedly succeeded")
	}
	if outbox := loadDelegationOutboxSafetyRecord(t, publisher.delegationStatePath, record.RecordID); outbox.Status != delegation.OutboxUnknown {
		t.Fatalf("outbox after unknown POST = %#v, want unknown", outbox)
	}

	err := publisher.sendDelegationInboxRecord(context.Background(), "chat-inbox", record)
	if err == nil {
		t.Fatal("replay unexpectedly succeeded without visibility evidence")
	}
	if graph.sendCount != 1 {
		t.Fatalf("send count after suppressed retry = %d, want 1", graph.sendCount)
	}
}

func TestMachineDelegationDurableSentOrVisibleWitnessSuppressesPOST(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		messageID  string
		addVisible bool
		wantErr    bool
	}{
		{name: "sent-not-visible", status: delegation.OutboxSent, messageID: "message-existing", wantErr: true},
		{name: "visible", status: delegation.OutboxVisible, messageID: "message-existing", addVisible: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			graph := newFakeBridgeMachineRegistryGraph()
			publisher := newDelegationOutboxSafetyPublisher(t, graph)
			record := newDelegationOutboxSafetyRecord(t)
			if tt.addVisible {
				graph.addMessage("chat-inbox", tt.messageID, delegation.RenderRecordHTML(record))
			}
			if err := publisher.updateDelegationOutbox(record, tt.status, "chat-inbox", tt.messageID, "pre-existing durable witness"); err != nil {
				t.Fatalf("seed outbox: %v", err)
			}
			err := publisher.sendDelegationInboxRecord(context.Background(), "chat-inbox", record)
			if (err != nil) != tt.wantErr {
				t.Fatalf("send error = %v, wantErr=%v", err, tt.wantErr)
			}
			if graph.sendCount != 0 {
				t.Fatalf("send count = %d, want 0", graph.sendCount)
			}
			outbox := loadDelegationOutboxSafetyRecord(t, publisher.delegationStatePath, record.RecordID)
			if tt.addVisible && outbox.Status != delegation.OutboxVisible {
				t.Fatalf("visible witness status = %#v", outbox)
			}
		})
	}
}

func TestMachineDelegationPrePOSTReservationConservativelySuppressesReplay(t *testing.T) {
	for _, suffix := range []string{"json", "sqlite"} {
		t.Run(suffix, func(t *testing.T) {
			graph := newFakeBridgeMachineRegistryGraph()
			publisher := newDelegationOutboxSafetyPublisher(t, graph)
			if suffix == "sqlite" {
				publisher.delegationStatePath = filepath.Join(t.TempDir(), "delegation-worker-state.sqlite")
			}
			record := newDelegationOutboxSafetyRecord(t)
			if _, reserved, err := delegation.ReserveOutbox(context.Background(), publisher.delegationStatePath, record, "chat-inbox", time.Date(2026, 9, 14, 12, 0, 1, 0, time.UTC)); err != nil || !reserved {
				t.Fatalf("pre-POST reservation reserved=%v err=%v", reserved, err)
			}

			// Model a process dying after the durable reservation and before the
			// network call. Absence from the fake inbox is not proof that a real
			// POST did not happen, so the safety policy must refuse an automatic
			// replay and leave the witness available for explicit reconciliation.
			err := publisher.sendDelegationInboxRecord(context.Background(), "chat-inbox", record)
			if err == nil || !strings.Contains(err.Error(), "refusing automatic replay") {
				t.Fatalf("pre-POST reservation retry error = %v, want conservative replay refusal", err)
			}
			if graph.sendCount != 0 {
				t.Fatalf("pre-POST reservation retry send count = %d, want 0", graph.sendCount)
			}
			outbox := loadDelegationOutboxSafetyRecord(t, publisher.delegationStatePath, record.RecordID)
			if outbox.Status != delegation.OutboxPending || outbox.Attempts != 1 {
				t.Fatalf("pre-POST reservation witness = %#v, want pending/one attempt", outbox)
			}
		})
	}
}

func TestMachineDelegationRefusesGraphPOSTWithoutDurableState(t *testing.T) {
	graph := newFakeBridgeMachineRegistryGraph()
	publisher := newDelegationOutboxSafetyPublisher(t, graph)
	publisher.delegationStatePath = ""
	record := newDelegationOutboxSafetyRecord(t)

	err := publisher.sendDelegationInboxRecord(context.Background(), "chat-inbox", record)
	if err == nil || !strings.Contains(err.Error(), "durable state path is required") {
		t.Fatalf("send without durable state error = %v, want fail-closed error", err)
	}
	if graph.sendCount != 0 {
		t.Fatalf("send without durable state count = %d, want zero", graph.sendCount)
	}
}
