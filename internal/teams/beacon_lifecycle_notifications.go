package teams

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func (b *Bridge) queueBeaconLifecycleNotifications(ctx context.Context, store *beacon.Store, now time.Time) error {
	if b == nil || store == nil || b.store == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	st, err := store.Load()
	if err != nil {
		return err
	}
	events := pendingBeaconLifecycleEvents(st)
	for _, event := range events {
		if err := b.queueBeaconLifecycleNotification(ctx, store, event, now); err != nil {
			return err
		}
	}
	return nil
}

func pendingBeaconLifecycleEvents(st beacon.State) []beacon.LifecycleNotificationRecord {
	var events []beacon.LifecycleNotificationRecord
	for _, req := range st.Allocations {
		switch req.State {
		case beacon.AllocationFailed, beacon.AllocationNeedsAttention:
			kind := beacon.LifecycleNotificationAllocationFailed
			if beacon.NotificationRecordedForAllocation(st, req.ID, kind) {
				continue
			}
			id := beacon.LifecycleNotificationID(kind, req.ID, "", req.ProviderIdentity.ProviderJobID, string(req.State), req.ProviderReason)
			if beacon.LifecycleNotificationExists(st, id) {
				continue
			}
			events = append(events, beacon.LifecycleNotificationRecord{
				ID:               id,
				Kind:             kind,
				Severity:         "alert",
				AllocationID:     req.ID,
				ProviderJobID:    firstNonEmptyString(req.ProviderIdentity.ProviderJobID, req.Target.ProviderJobID),
				ConversationID:   req.ConversationID,
				TurnID:           req.TurnID,
				Profile:          req.Profile,
				State:            string(req.State),
				RawProviderState: req.RawProviderState,
				Reason:           firstNonEmptyString(req.ProviderReason, "beacon allocation needs attention"),
			})
		case beacon.AllocationExpired:
			kind := beacon.LifecycleNotificationAllocationExpired
			if beacon.NotificationRecordedForAllocation(st, req.ID, kind) {
				continue
			}
			id := beacon.LifecycleNotificationID(kind, req.ID, "", req.ProviderIdentity.ProviderJobID, string(req.State), req.ProviderReason)
			if beacon.LifecycleNotificationExists(st, id) {
				continue
			}
			events = append(events, beacon.LifecycleNotificationRecord{
				ID:               id,
				Kind:             kind,
				Severity:         "info",
				AllocationID:     req.ID,
				ProviderJobID:    firstNonEmptyString(req.ProviderIdentity.ProviderJobID, req.Target.ProviderJobID),
				ConversationID:   req.ConversationID,
				TurnID:           req.TurnID,
				Profile:          req.Profile,
				State:            string(req.State),
				RawProviderState: req.RawProviderState,
				Reason:           firstNonEmptyString(req.ProviderReason, "beacon allocation expired"),
			})
		}
	}
	for _, machine := range st.Machines {
		state := strings.ToLower(strings.TrimSpace(machine.State))
		convID, turnID, allocationID := machineLifecycleDemand(st, machine)
		switch state {
		case string(beacon.LeaseLost), string(beacon.LeaseNeedsAttention), string(beacon.LeaseAmbiguous):
			kind := beacon.LifecycleNotificationMachineFailed
			if beacon.NotificationRecordedForMachine(st, machine.ID, kind) {
				continue
			}
			id := beacon.LifecycleNotificationID(kind, "", machine.ID, machine.ProviderJobID, state, machine.Reason)
			if beacon.LifecycleNotificationExists(st, id) {
				continue
			}
			events = append(events, beacon.LifecycleNotificationRecord{
				ID:             id,
				Kind:           kind,
				Severity:       "alert",
				AllocationID:   allocationID,
				MachineID:      machine.ID,
				ProviderJobID:  machine.ProviderJobID,
				ConversationID: convID,
				TurnID:         turnID,
				Profile:        machine.Profile,
				State:          state,
				Reason:         firstNonEmptyString(machine.Reason, "beacon machine needs attention"),
			})
		case string(beacon.LeaseDraining):
			if strings.Contains(strings.ToLower(machine.Reason), "idle") {
				continue
			}
			kind := beacon.LifecycleNotificationMachineFailed
			if beacon.NotificationRecordedForMachine(st, machine.ID, kind) {
				continue
			}
			id := beacon.LifecycleNotificationID(kind, "", machine.ID, machine.ProviderJobID, state, machine.Reason)
			if beacon.LifecycleNotificationExists(st, id) {
				continue
			}
			events = append(events, beacon.LifecycleNotificationRecord{
				ID:             id,
				Kind:           kind,
				Severity:       "alert",
				AllocationID:   allocationID,
				MachineID:      machine.ID,
				ProviderJobID:  machine.ProviderJobID,
				ConversationID: convID,
				TurnID:         turnID,
				Profile:        machine.Profile,
				State:          state,
				Reason:         firstNonEmptyString(machine.Reason, "beacon machine is draining"),
			})
		case string(beacon.LeaseExpired), string(beacon.LeaseDrained):
			kind := beacon.LifecycleNotificationMachineExpired
			if beacon.NotificationRecordedForMachine(st, machine.ID, kind) {
				continue
			}
			id := beacon.LifecycleNotificationID(kind, "", machine.ID, machine.ProviderJobID, state, machine.Reason)
			if beacon.LifecycleNotificationExists(st, id) {
				continue
			}
			events = append(events, beacon.LifecycleNotificationRecord{
				ID:             id,
				Kind:           kind,
				Severity:       "info",
				AllocationID:   allocationID,
				MachineID:      machine.ID,
				ProviderJobID:  machine.ProviderJobID,
				ConversationID: convID,
				TurnID:         turnID,
				Profile:        machine.Profile,
				State:          state,
				Reason:         firstNonEmptyString(machine.Reason, "beacon machine expired"),
			})
		}
	}
	sort.Slice(events, func(i, j int) bool { return events[i].ID < events[j].ID })
	return events
}

func machineLifecycleDemand(st beacon.State, machine beacon.Machine) (conversationID string, turnID string, allocationID string) {
	jobIDs := map[string]bool{}
	for _, id := range machine.Jobs {
		if strings.TrimSpace(id) != "" {
			jobIDs[strings.TrimSpace(id)] = true
		}
	}
	for _, attempt := range st.JobAttempts {
		if matchNonEmpty(attempt.Target.MachineID, machine.ID) ||
			matchNonEmpty(attempt.LeaseID, machine.LeaseID) ||
			matchNonEmpty(attempt.ProviderIdentity.ProviderJobID, machine.ProviderJobID) ||
			jobIDs[strings.TrimSpace(attempt.ID)] {
			if strings.TrimSpace(attempt.TurnID) != "" {
				turnID = strings.TrimSpace(attempt.TurnID)
			}
			if strings.TrimSpace(attempt.RequestID) != "" {
				allocationID = strings.TrimSpace(attempt.RequestID)
				if req, ok := st.Allocations[allocationID]; ok {
					conversationID = strings.TrimSpace(req.ConversationID)
					if turnID == "" {
						turnID = strings.TrimSpace(req.TurnID)
					}
				}
			}
			if conversationID != "" || turnID != "" || allocationID != "" {
				return conversationID, turnID, allocationID
			}
		}
	}
	for _, req := range st.Allocations {
		providerJobID := firstNonEmptyString(req.ProviderIdentity.ProviderJobID, req.Target.ProviderJobID)
		if matchNonEmpty(req.Target.MachineID, machine.ID) ||
			matchNonEmpty(req.Target.LeaseID, machine.LeaseID) ||
			matchNonEmpty(providerJobID, machine.ProviderJobID) {
			return strings.TrimSpace(req.ConversationID), strings.TrimSpace(req.TurnID), strings.TrimSpace(req.ID)
		}
	}
	return "", "", ""
}

func matchNonEmpty(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return left != "" && right != "" && left == right
}

func (b *Bridge) queueBeaconLifecycleNotification(ctx context.Context, store *beacon.Store, event beacon.LifecycleNotificationRecord, now time.Time) error {
	controlChatID := strings.TrimSpace(b.reg.ControlChatID)
	if controlChatID == "" {
		return nil
	}
	event.ControlOutboxID = "outbox:" + event.ID + ":control"
	controlMsg := teamstore.OutboxMessage{
		ID:               event.ControlOutboxID,
		TeamsChatID:      controlChatID,
		Kind:             "helper",
		Body:             formatBeaconLifecycleControlMessage(event),
		NotificationKind: beaconLifecycleNotificationKind(event),
		MentionOwner:     event.Severity == "alert",
	}
	if _, err := b.queueOutbox(ctx, controlMsg); err != nil {
		return err
	}
	if event.Severity == "alert" {
		if workChatID, active := b.beaconLifecycleWorkChat(ctx, event); active && workChatID != "" {
			event.WorkOutboxID = "outbox:" + event.ID + ":work"
			workMsg := teamstore.OutboxMessage{
				ID:               event.WorkOutboxID,
				SessionID:        event.ConversationID,
				TurnID:           event.TurnID,
				TeamsChatID:      workChatID,
				Kind:             "helper",
				Body:             formatBeaconLifecycleWorkMessage(event),
				NotificationKind: "needs_attention",
			}
			if _, err := b.queueOutbox(ctx, workMsg); err != nil {
				return err
			}
		}
	}
	return store.Update(func(st *beacon.State) error {
		beacon.RecordLifecycleNotification(st, event, now)
		return nil
	})
}

func (b *Bridge) beaconLifecycleWorkChat(ctx context.Context, event beacon.LifecycleNotificationRecord) (string, bool) {
	if strings.TrimSpace(event.ConversationID) == "" || strings.TrimSpace(event.TurnID) == "" || b.store == nil {
		return "", false
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return "", false
	}
	turn, ok := state.Turns[strings.TrimSpace(event.TurnID)]
	if !ok {
		return "", false
	}
	if turn.Status != teamstore.TurnStatusQueued && turn.Status != teamstore.TurnStatusRunning {
		return "", false
	}
	session := state.Sessions[strings.TrimSpace(event.ConversationID)]
	return strings.TrimSpace(session.TeamsChatID), strings.TrimSpace(session.TeamsChatID) != ""
}

func beaconLifecycleNotificationKind(event beacon.LifecycleNotificationRecord) string {
	if event.Severity == "alert" {
		return "needs_attention"
	}
	return ""
}

func formatBeaconLifecycleControlMessage(event beacon.LifecycleNotificationRecord) string {
	switch event.Kind {
	case beacon.LifecycleNotificationMachineFailed:
		return beaconLifecycleMessage("Beacon machine failed", event, true)
	case beacon.LifecycleNotificationAllocationFailed:
		return beaconLifecycleMessage("Beacon allocation needs attention", event, true)
	case beacon.LifecycleNotificationMachineExpired:
		return beaconLifecycleMessage("Beacon machine expired", event, false)
	case beacon.LifecycleNotificationAllocationExpired:
		return beaconLifecycleMessage("Beacon allocation expired", event, false)
	default:
		return beaconLifecycleMessage("Beacon lifecycle update", event, event.Severity == "alert")
	}
}

func formatBeaconLifecycleWorkMessage(event beacon.LifecycleNotificationRecord) string {
	return beaconLifecycleMessage("Beacon machine for this Codex request is unavailable", event, true)
}

func beaconLifecycleMessage(title string, event beacon.LifecycleNotificationRecord, actionNeeded bool) string {
	var lines []string
	lines = append(lines, "🔧 Helper:")
	lines = append(lines, "")
	lines = append(lines, title+".")
	lines = append(lines, "")
	lines = append(lines, "Details:")
	if strings.TrimSpace(event.MachineID) != "" {
		lines = append(lines, "- machine: `"+event.MachineID+"`")
	}
	if strings.TrimSpace(event.AllocationID) != "" {
		lines = append(lines, "- allocation: `"+event.AllocationID+"`")
	}
	if strings.TrimSpace(event.ProviderJobID) != "" {
		lines = append(lines, "- provider_job: `"+event.ProviderJobID+"`")
	}
	if strings.TrimSpace(event.Profile) != "" {
		lines = append(lines, "- profile: `"+event.Profile+"`")
	}
	if strings.TrimSpace(event.State) != "" {
		lines = append(lines, "- state: `"+event.State+"`")
	}
	if strings.TrimSpace(event.RawProviderState) != "" {
		lines = append(lines, "- provider_state: `"+event.RawProviderState+"`")
	}
	if strings.TrimSpace(event.Reason) != "" {
		lines = append(lines, "- reason: "+event.Reason)
	}
	if actionNeeded {
		lines = append(lines, "", "Action needed:")
		lines = append(lines, "- Check the scheduler job and retry the request after fixing the worker or provider issue.")
	}
	return strings.Join(lines, "\n")
}
