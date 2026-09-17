package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestOutboxGraphPreflightFailureReleasesStartedAttachmentBoundaries(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			now := time.Now().UTC()
			owner := testOwner("preflight-session", "preflight-turn", now)
			owner.ScopeID = "preflight-scope"
			owner.MachineID = "preflight-machine"
			owner.LeaseGeneration = 11
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = ScopeIdentity{ID: owner.ScopeID, AccountID: "preflight-account", Profile: "default"}
				state.ServiceOwner = &owner
				state.LockOwner = &owner
				state.ControlLease = ControlLease{
					ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
					Generation: owner.LeaseGeneration, Status: ControlLeaseStatusActive,
					LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed owner: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			msg, created, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
				ID: "outbox:preflight-attachment", TeamsChatID: "preflight-chat",
				Kind: "attachment", AttachmentPath: "/private/preflight.bin", DriveItemID: "drive:preflight",
				AttachmentMessagePostState: "pending", AttachmentUploadSessionPostState: "pending",
			}, owner.MachineID, owner.LeaseGeneration)
			if err != nil || !created {
				t.Fatalf("QueueOutboxForOwner created=%v err=%v", created, err)
			}
			claimed, err := store.MarkOutboxSendAttemptForOwner(ctx, msg.ID, owner.MachineID, owner.LeaseGeneration)
			if err != nil {
				t.Fatalf("claim attachment: %v", err)
			}
			started, err := store.MarkOutboxAttachmentMessagePostStartedForAttempt(ctx, msg.ID, claimed.SendAttemptToken)
			if err != nil {
				t.Fatalf("mark attachment POST started: %v", err)
			}
			if started.AttachmentMessagePostAttemptToken != claimed.SendAttemptToken {
				t.Fatalf("attachment POST boundary token = %q, want %q", started.AttachmentMessagePostAttemptToken, claimed.SendAttemptToken)
			}
			started, err = store.MarkOutboxUploadSessionPostStartedForAttempt(ctx, msg.ID, claimed.SendAttemptToken)
			if err != nil {
				t.Fatalf("mark upload-session POST started: %v", err)
			}
			if started.AttachmentUploadSessionPostAttemptToken != claimed.SendAttemptToken {
				t.Fatalf("upload-session POST boundary token = %q, want %q", started.AttachmentUploadSessionPostAttemptToken, claimed.SendAttemptToken)
			}

			released, err := store.ReleaseControlLeaseIfHolder(ctx, owner.MachineID, owner.LeaseGeneration)
			if err != nil || !released {
				t.Fatalf("release owner before preflight callback: released=%v err=%v", released, err)
			}
			replacement := owner
			replacement.MachineID = owner.MachineID + "-replacement"
			replacement.InstanceID = "preflight-replacement"
			replacement.LeaseGeneration = owner.LeaseGeneration + 1
			claimNow := time.Now().UTC()
			decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope:   ScopeIdentity{ID: owner.ScopeID, AccountID: "preflight-account", Profile: "default"},
				Machine: MachineRecord{ID: replacement.MachineID, ScopeID: owner.ScopeID, Status: MachineStatusActive},
				Owner:   replacement, Duration: time.Hour, Now: claimNow,
			})
			if err != nil || decision.Mode != LeaseModeActive {
				t.Fatalf("replacement lease decision = %#v err=%v", decision, err)
			}
			bound, err := store.BindOutboxRecoveryAttemptForOwner(ctx, msg.ID, replacement.MachineID, decision.Lease.Generation)
			if err != nil {
				t.Fatalf("bind replacement recovery attempt: %v", err)
			}
			if bound.SendAttemptToken == claimed.SendAttemptToken {
				t.Fatalf("replacement recovery did not rotate send token: old=%q new=%q", claimed.SendAttemptToken, bound.SendAttemptToken)
			}
			reset, err := store.MarkOutboxGraphPreflightFailureForAttempt(ctx, msg.ID, claimed.SendAttemptToken, "owner fence before HTTP")
			if err != nil {
				t.Fatalf("preflight failure reducer after takeover: %v", err)
			}
			if reset.Status != OutboxStatusQueued || reset.AttachmentMessagePostState != "pending" || reset.AttachmentUploadSessionPostState != "pending" || reset.AttachmentUploadURL != "" {
				t.Fatalf("preflight reset = %#v, want queued replayable attachment", reset)
			}
			if reset.AttachmentMessagePostAttemptToken != "" || reset.AttachmentUploadSessionPostAttemptToken != "" {
				t.Fatalf("preflight reset retained stale boundary tokens: %#v", reset)
			}
			reclaimed, err := store.MarkOutboxSendAttemptForOwner(ctx, msg.ID, replacement.MachineID, decision.Lease.Generation)
			if err != nil {
				t.Fatalf("replacement owner could not reclaim preflight row: %v", err)
			}
			if reclaimed.Status != OutboxStatusSending || reclaimed.SendAttemptToken == claimed.SendAttemptToken {
				t.Fatalf("replacement claim = %#v, want new sending attempt", reclaimed)
			}
		})
	}
}

func TestOutboxGraphPreflightFailureRejectsWrongAttemptToken(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	owner := testOwner("preflight-token-session", "preflight-token-turn", now)
	owner.ScopeID = "preflight-token-scope"
	owner.MachineID = "preflight-token-machine"
	owner.LeaseGeneration = 1
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = ScopeIdentity{ID: owner.ScopeID}
		state.ControlLease = ControlLease{ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID, Generation: owner.LeaseGeneration, Status: ControlLeaseStatusActive, LeaseUntil: now.Add(time.Hour)}
		return nil
	}); err != nil {
		t.Fatalf("seed preflight token owner: %v", err)
	}
	msg, _, err := store.QueueOutboxForOwner(ctx, OutboxMessage{ID: "outbox:preflight-token", TeamsChatID: "preflight-token-chat", Kind: "helper-status"}, owner.MachineID, owner.LeaseGeneration)
	if err != nil {
		t.Fatalf("queue preflight token row: %v", err)
	}
	claimed, err := store.MarkOutboxSendAttemptForOwner(ctx, msg.ID, owner.MachineID, owner.LeaseGeneration)
	if err != nil {
		t.Fatalf("claim preflight token row: %v", err)
	}
	if _, err := store.MarkOutboxGraphPreflightFailureForAttempt(ctx, msg.ID, "wrong-token", "must be fenced"); !errors.Is(err, ErrOutboxSendNotClaimed) {
		t.Fatalf("wrong-token preflight reducer error = %v, want ErrOutboxSendNotClaimed", err)
	}
	current, err := store.OutboxMessageByID(ctx, msg.ID)
	if err != nil {
		t.Fatalf("read row after wrong-token reducer: %v", err)
	}
	if current.Status != OutboxStatusSending || current.SendAttemptToken != claimed.SendAttemptToken {
		t.Fatalf("wrong-token reducer changed row = %#v", current)
	}
}

func TestOutboxGraphPreflightFailureRejectsUnwitnessedStartedBoundary(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		for _, boundary := range []string{"message", "upload-session"} {
			for _, witness := range []string{"", "foreign-attempt"} {
				name := map[bool]string{false: "json", true: "sqlite"}[useSQLite] + "/" + boundary + "/"
				if witness == "" {
					name += "missing-witness"
				} else {
					name += "foreign-witness"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					store := newTestStore(t)
					now := time.Now().UTC()
					owner := testOwner("preflight-unwitnessed-session", "preflight-unwitnessed-turn", now)
					owner.ScopeID = "preflight-unwitnessed-scope"
					owner.MachineID = "preflight-unwitnessed-machine"
					owner.LeaseGeneration = 1
					if err := store.Update(ctx, func(state *State) error {
						state.Scope = ScopeIdentity{ID: owner.ScopeID, AccountID: "preflight-unwitnessed-account"}
						state.ServiceOwner = &owner
						state.LockOwner = &owner
						state.ControlLease = ControlLease{
							ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
							Generation: owner.LeaseGeneration, Status: ControlLeaseStatusActive,
							LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
						}
						return nil
					}); err != nil {
						t.Fatalf("seed owner: %v", err)
					}
					if useSQLite {
						migrateStoreToSQLiteForTest(t, store)
					}
					msg, created, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
						ID: "outbox:preflight-unwitnessed", TeamsChatID: "preflight-unwitnessed-chat",
						Kind: "attachment", AttachmentPath: "/private/preflight.bin", DriveItemID: "drive:preflight",
						AttachmentMessagePostState: "pending", AttachmentUploadSessionPostState: "pending",
					}, owner.MachineID, owner.LeaseGeneration)
					if err != nil || !created {
						t.Fatalf("QueueOutboxForOwner created=%v err=%v", created, err)
					}
					claimed, err := store.MarkOutboxSendAttemptForOwner(ctx, msg.ID, owner.MachineID, owner.LeaseGeneration)
					if err != nil {
						t.Fatalf("claim outbox: %v", err)
					}
					if err := store.Update(ctx, func(state *State) error {
						row := state.OutboxMessages[msg.ID]
						if boundary == "message" {
							row.AttachmentMessagePostState = "started"
							row.AttachmentMessagePostAttemptToken = witness
						} else {
							row.AttachmentUploadSessionPostState = "started"
							row.AttachmentUploadSessionPostAttemptToken = witness
						}
						state.OutboxMessages[msg.ID] = row
						return nil
					}); err != nil {
						t.Fatalf("inject unwitnessed boundary: %v", err)
					}

					if _, err := store.MarkOutboxGraphPreflightFailureForAttempt(ctx, msg.ID, claimed.SendAttemptToken, "must fail closed"); !errors.Is(err, ErrOutboxSendNotClaimed) {
						t.Fatalf("unwitnessed %s preflight reducer error = %v, want ErrOutboxSendNotClaimed", boundary, err)
					}
					current, err := store.OutboxMessageByID(ctx, msg.ID)
					if err != nil {
						t.Fatalf("reload unwitnessed outbox: %v", err)
					}
					if current.Status != OutboxStatusSending || current.SendAttemptToken != claimed.SendAttemptToken {
						t.Fatalf("unwitnessed boundary changed send claim: %#v", current)
					}
					if boundary == "message" && (!strings.EqualFold(current.AttachmentMessagePostState, "started") || current.AttachmentMessagePostAttemptToken != witness) {
						t.Fatalf("unwitnessed message boundary changed: %#v", current)
					}
					if boundary == "upload-session" && (!strings.EqualFold(current.AttachmentUploadSessionPostState, "started") || current.AttachmentUploadSessionPostAttemptToken != witness) {
						t.Fatalf("unwitnessed upload-session boundary changed: %#v", current)
					}
				})
			}
		}
	}
}

func TestOutboxMissingDriveItemRequeuesOnlyPendingAttachmentAttempt(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			now := time.Now().UTC()
			owner := testOwner("missing-drive-session", "missing-drive-turn", now)
			owner.ScopeID = "missing-drive-scope"
			owner.MachineID = "missing-drive-machine"
			owner.LeaseGeneration = 7
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = ScopeIdentity{ID: owner.ScopeID, AccountID: "missing-drive-account"}
				state.ServiceOwner = &owner
				state.LockOwner = &owner
				state.ControlLease = ControlLease{
					ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
					Generation: owner.LeaseGeneration, Status: ControlLeaseStatusActive,
					LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed owner: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			msg, created, err := store.QueueOutboxForOwner(ctx, OutboxMessage{
				ID: "outbox:missing-drive-item", TeamsChatID: "missing-drive-chat",
				Kind: "attachment", AttachmentPath: "/private/missing-drive.bin",
				DriveItemID: "drive:stale", AttachmentUploadURL: "https://upload.example/session",
				AttachmentUploadSessionPostState: "ready", AttachmentMessagePostState: "pending",
			}, owner.MachineID, owner.LeaseGeneration)
			if err != nil || !created {
				t.Fatalf("QueueOutboxForOwner created=%v err=%v", created, err)
			}
			claimed, err := store.MarkOutboxSendAttemptForOwner(ctx, msg.ID, owner.MachineID, owner.LeaseGeneration)
			if err != nil {
				t.Fatalf("claim missing-drive row: %v", err)
			}
			reset, err := store.RequeueOutboxAfterMissingDriveItemForAttempt(ctx, msg.ID, claimed.SendAttemptToken, "GET /me/drive/items/drive:stale returned 404")
			if err != nil {
				t.Fatalf("requeue missing-drive row: %v", err)
			}
			if reset.Status != OutboxStatusQueued || reset.DriveItemID != "" || reset.AttachmentUploadURL != "" ||
				reset.AttachmentUploadSessionPostState != "pending" || reset.AttachmentMessagePostState != "pending" ||
				reset.SendAttemptToken != "" || reset.MachineID != "" || reset.LeaseGeneration != 0 || !reset.NextAttemptAt.IsZero() {
				t.Fatalf("missing-drive requeue = %#v, want replayable upload row", reset)
			}
			if !strings.Contains(reset.LastSendError, "returned 404") {
				t.Fatalf("missing-drive diagnostic = %q, want source error", reset.LastSendError)
			}

			// A stale callback must not clear a replacement attempt, even though
			// the old worker may still hold the 404 response in memory.
			reclaimed, err := store.MarkOutboxSendAttemptForOwner(ctx, msg.ID, owner.MachineID, owner.LeaseGeneration)
			if err != nil {
				t.Fatalf("reclaim reset row: %v", err)
			}
			if _, err := store.RequeueOutboxAfterMissingDriveItemForAttempt(ctx, msg.ID, claimed.SendAttemptToken, "stale callback"); !errors.Is(err, ErrOutboxSendNotClaimed) {
				t.Fatalf("stale missing-drive callback error = %v, want ErrOutboxSendNotClaimed", err)
			}
			current, err := store.OutboxMessageByID(ctx, msg.ID)
			if err != nil {
				t.Fatalf("reload replacement missing-drive row: %v", err)
			}
			if current.Status != OutboxStatusSending || current.SendAttemptToken != reclaimed.SendAttemptToken || current.DriveItemID != "" {
				t.Fatalf("stale callback changed replacement row = %#v", current)
			}

			// Once the final attachment POST boundary is started, a missing
			// DriveItem is not proof that the earlier Teams POST was absent. Keep
			// that row fenced instead of making it replayable.
			reclaimed, err = store.MarkOutboxDriveItemForAttempt(ctx, msg.ID, reclaimed.SendAttemptToken, "drive:replacement", "replacement.bin", `"{replacement},1"`, "", "")
			if err != nil {
				t.Fatalf("persist replacement DriveItem: %v", err)
			}
			started, err := store.MarkOutboxAttachmentMessagePostStartedForAttempt(ctx, msg.ID, reclaimed.SendAttemptToken)
			if err != nil {
				t.Fatalf("mark final attachment boundary: %v", err)
			}
			if _, err := store.RequeueOutboxAfterMissingDriveItemForAttempt(ctx, msg.ID, started.SendAttemptToken, "must retain started boundary"); !errors.Is(err, ErrOutboxSendNotClaimed) {
				t.Fatalf("started missing-drive requeue error = %v, want ErrOutboxSendNotClaimed", err)
			}
			current, err = store.OutboxMessageByID(ctx, msg.ID)
			if err != nil {
				t.Fatalf("reload started missing-drive row: %v", err)
			}
			if current.Status != OutboxStatusSending || current.DriveItemID != "drive:replacement" || current.AttachmentMessagePostState != "started" {
				t.Fatalf("started missing-drive row changed = %#v", current)
			}
		})
	}
}
