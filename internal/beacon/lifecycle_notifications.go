package beacon

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"
)

func LifecycleNotificationID(kind LifecycleNotificationKind, allocationID string, machineID string, providerJobID string, state string, reason string) string {
	seed := strings.Join([]string{
		string(kind),
		strings.TrimSpace(allocationID),
		strings.TrimSpace(machineID),
		strings.TrimSpace(providerJobID),
		strings.TrimSpace(state),
		strings.TrimSpace(reason),
	}, "\x00")
	sum := sha256.Sum256([]byte(seed))
	return "beacon-notify-" + fmt.Sprintf("%x", sum[:12])
}

func RecordLifecycleNotification(st *State, rec LifecycleNotificationRecord, now time.Time) bool {
	if st == nil || strings.TrimSpace(rec.ID) == "" {
		return false
	}
	st.normalize()
	if _, exists := st.Notifications[rec.ID]; exists {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	rec.QueuedAt = now
	st.Notifications[rec.ID] = rec
	return true
}

func LifecycleNotificationExists(st State, id string) bool {
	st.normalize()
	_, ok := st.Notifications[strings.TrimSpace(id)]
	return ok
}

func NotificationRecordedForAllocation(st State, allocationID string, kinds ...LifecycleNotificationKind) bool {
	st.normalize()
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return false
	}
	kindSet := map[LifecycleNotificationKind]bool{}
	for _, kind := range kinds {
		kindSet[kind] = true
	}
	for _, rec := range st.Notifications {
		if strings.TrimSpace(rec.AllocationID) != allocationID {
			continue
		}
		if len(kindSet) == 0 || kindSet[rec.Kind] {
			return true
		}
	}
	return false
}

func NotificationRecordedForMachine(st State, machineID string, kinds ...LifecycleNotificationKind) bool {
	st.normalize()
	machineID = strings.TrimSpace(machineID)
	if machineID == "" {
		return false
	}
	kindSet := map[LifecycleNotificationKind]bool{}
	for _, kind := range kinds {
		kindSet[kind] = true
	}
	for _, rec := range st.Notifications {
		if strings.TrimSpace(rec.MachineID) != machineID {
			continue
		}
		if len(kindSet) == 0 || kindSet[rec.Kind] {
			return true
		}
	}
	return false
}
