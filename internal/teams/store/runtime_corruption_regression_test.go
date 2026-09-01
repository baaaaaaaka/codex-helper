package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// An owner projection is written to two compatibility rows. A torn write in
// one row must not make the live capability-bound heartbeat fail forever when
// the other row and the control lease still identify the same owner. The raw
// bytes remain in an opaque audit record instead of being silently discarded.
func TestSQLiteOwnerHeartbeatRepairsCorruptOwnerProjection(t *testing.T) {
	ctx := context.Background()
	for _, method := range []string{"legacy", "lease-bound"} {
		t.Run(method, func(t *testing.T) {
			for _, corruptKey := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
				t.Run(corruptKey, func(t *testing.T) {
					store := newTestStore(t)
					now := testOwnerStart()
					owner := testOwner("runtime-corrupt-session", "runtime-corrupt-turn", now)
					owner.ScopeID = "scope-runtime-corrupt-owner"
					owner.MachineID = "machine-runtime-corrupt-owner"
					owner.LeaseGeneration = 9
					lease := ControlLease{
						ScopeID:         owner.ScopeID,
						HolderMachineID: owner.MachineID,
						Generation:      owner.LeaseGeneration,
						Status:          ControlLeaseStatusActive,
						LeaseUntil:      now.Add(time.Hour),
						LastHeartbeat:   now,
						UpdatedAt:       now,
					}
					if err := store.Update(ctx, func(state *State) error {
						state.Scope = ScopeIdentity{ID: owner.ScopeID}
						state.ServiceOwner = &owner
						state.LockOwner = &owner
						state.ControlLease = lease
						return nil
					}); err != nil {
						t.Fatalf("seed owner projection: %v", err)
					}
					migrateStoreToSQLiteForTest(t, store)
					corruptRaw := []byte(`{"owner-projection-is-torn"`)
					withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
						result, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, corruptRaw, corruptKey)
						if err != nil {
							return err
						}
						affected, err := result.RowsAffected()
						if err != nil {
							return err
						}
						if affected != 1 {
							return fmt.Errorf("updated %d %s rows, want 1", affected, corruptKey)
						}
						return nil
					})

					path := store.Path()
					if err := store.Close(); err != nil {
						t.Fatalf("close store: %v", err)
					}
					reopened, err := Open(path)
					if err != nil {
						t.Fatalf("reopen store: %v", err)
					}
					t.Cleanup(func() { _ = reopened.Close() })

					next := now.Add(time.Minute)
					var updated OwnerMetadata
					if method == "lease-bound" {
						updated, err = reopened.RecordOwnerHeartbeatForLease(ctx, owner, time.Minute, time.Hour, next)
					} else {
						updated, err = reopened.RecordOwnerHeartbeat(ctx, owner, time.Minute, next)
					}
					if err != nil {
						t.Fatalf("owner heartbeat with corrupt %s row: %v", corruptKey, err)
					}
					if !updated.LastHeartbeat.Equal(next) || updated.MachineID != owner.MachineID {
						t.Fatalf("updated owner = %#v, want heartbeat=%s machine=%q", updated, next, owner.MachineID)
					}

					var opaque []byte
					withSQLiteTxForTest(t, reopened, func(tx *sql.Tx) error {
						return tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key LIKE ? ORDER BY key LIMIT 1`, sqliteRuntimeOpaqueMetaPrefix+corruptKey+":%").Scan(&opaque)
					})
					if string(opaque) != string(corruptRaw) {
						t.Fatalf("opaque %s row = %q, want %q", corruptKey, opaque, corruptRaw)
					}
					if method == "lease-bound" {
						if _, err := reopened.ValidateControlLease(ctx, owner.MachineID, owner.LeaseGeneration, next); err != nil {
							t.Fatalf("validate lease after heartbeat repair: %v", err)
						}
					}
				})
			}
		})
	}
}

// If both compatibility owner rows are opaque, the unscoped legacy heartbeat
// has no ownership witness and must not overwrite an unknown live owner. The
// capability-bound API remains the explicit recovery path because it validates
// the current control-lease generation independently.
func TestSQLiteUnscopedOwnerHeartbeatFailsClosedWithOpaqueOwnerProjection(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := testOwnerStart()
	owner := testOwner("opaque-owner-session", "opaque-owner-turn", now)
	owner.ScopeID = "scope-opaque-owner"
	owner.MachineID = "machine-opaque-owner"
	owner.LeaseGeneration = 13
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = ScopeIdentity{ID: owner.ScopeID}
		state.ServiceOwner = &owner
		state.LockOwner = &owner
		state.ControlLease = ControlLease{
			ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
			Generation: owner.LeaseGeneration, Status: ControlLeaseStatusActive,
			LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed owner projection: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	corruptRaw := []byte(`{"opaque-owner"`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
			if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, corruptRaw, key); err != nil {
				return err
			}
		}
		return nil
	})

	_, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now.Add(time.Minute))
	if err == nil {
		t.Fatal("unscoped heartbeat unexpectedly repaired owner without a witness")
	}
	if errors.Is(err, ErrControlLeaseNotHeld) {
		t.Fatalf("unscoped opaque-owner failure used the wrong safety boundary: %v", err)
	}
	if !strings.Contains(err.Error(), "invalid sqlite owner runtime row") {
		t.Fatalf("unscoped opaque-owner error = %v, want invalid runtime diagnostic", err)
	}
}

// An unknown lease status is syntactically valid JSON but semantically
// untrusted. Every ownership operation must fail closed for it; otherwise a
// future helper could renew, release, or replace a lease whose meaning it does
// not understand. Keep this parity check on both storage backends because the
// JSON fallback and the materialized runtime projection have historically
// diverged at exactly these boundaries.
func TestUnknownControlLeaseStatusFailsClosedAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			now := testOwnerStart()
			scope := ScopeIdentity{ID: "scope-unknown-lease-" + name, AccountID: "user-unknown-lease", OSUser: "tester", Profile: "default"}
			machine := MachineRecord{ID: "machine-unknown-lease-" + name, ScopeID: scope.ID, Kind: MachineKindPrimary}
			claimed, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Hour, Now: now})
			if err != nil {
				t.Fatalf("initial lease claim: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			if err := store.Update(ctx, func(state *State) error {
				state.ControlLease.Status = ControlLeaseStatus("future-control-lease-state")
				return nil
			}); err != nil {
				t.Fatalf("persist unknown lease status: %v", err)
			}
			owner := testOwner("unknown-lease-session", "unknown-lease-turn", now)
			owner.ScopeID = scope.ID
			owner.MachineID = machine.ID
			owner.LeaseGeneration = claimed.Lease.Generation

			assertUnknown := func(operation string, err error) {
				t.Helper()
				if !errors.Is(err, ErrControlLeaseStatusUnknown) {
					t.Fatalf("%s error = %v, want ErrControlLeaseStatusUnknown", operation, err)
				}
			}
			_, err = store.RecordOwnerHeartbeatForLease(ctx, owner, time.Minute, time.Hour, now.Add(time.Minute))
			assertUnknown("lease-bound heartbeat", err)
			_, err = store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now.Add(time.Minute))
			assertUnknown("legacy heartbeat", err)
			_, err = store.ValidateControlLease(ctx, machine.ID, claimed.Lease.Generation, now.Add(time.Minute))
			assertUnknown("lease validation", err)
			_, err = store.ReleaseControlLeaseIfHolder(ctx, machine.ID, claimed.Lease.Generation)
			assertUnknown("lease release", err)
			replacement := MachineRecord{ID: "machine-unknown-lease-replacement-" + name, ScopeID: scope.ID, Kind: MachineKindPrimary}
			_, err = store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: replacement, Duration: time.Hour, Now: now.Add(2 * time.Hour)})
			assertUnknown("replacement lease claim", err)

			if useSQLite {
				raw := sqliteRuntimeRawForTest(t, store, sqliteRuntimeKeyControlLease)
				var lease ControlLease
				if err := json.Unmarshal(raw, &lease); err != nil {
					t.Fatalf("decode unknown lease after rejected operations: %v", err)
				}
				if lease.Status != ControlLeaseStatus("future-control-lease-state") || lease.HolderMachineID != machine.ID {
					t.Fatalf("unknown lease was changed by a rejected operation: %#v", lease)
				}
			} else {
				state, err := store.Load(ctx)
				if err != nil {
					t.Fatalf("load state after rejected operations: %v", err)
				}
				if state.ControlLease.Status != ControlLeaseStatus("future-control-lease-state") || state.ControlLease.HolderMachineID != machine.ID {
					t.Fatalf("unknown lease was changed by a rejected operation: %#v", state.ControlLease)
				}
			}
		})
	}
}

// A syntactically valid lease row is not necessarily usable ownership proof.
// The claimant must reject partial active tuples on both backends, while still
// accepting the complete generation-zero tuple emitted by pre-generation
// helpers and a deliberately released lease with no holder fields.
func TestControlLeaseShapeFailsClosedAcrossBackends(t *testing.T) {
	ctx := context.Background()
	now := testOwnerStart()
	scope := ScopeIdentity{ID: "scope-lease-shape", AccountID: "account-lease-shape", OSUser: "tester", Profile: "default"}
	claimMachine := MachineRecord{ID: "machine-lease-shape-claimant", ScopeID: scope.ID, Kind: MachineKindPrimary}
	cases := []struct {
		name    string
		lease   ControlLease
		wantErr bool
	}{
		{
			name: "active missing holder",
			lease: ControlLease{ScopeID: scope.ID, Generation: 3, Status: ControlLeaseStatusActive,
				LeaseUntil: now.Add(time.Hour), LastHeartbeat: now},
			wantErr: true,
		},
		{
			name: "active missing expiry",
			lease: ControlLease{ScopeID: scope.ID, HolderMachineID: "machine-old", Generation: 3,
				Status: ControlLeaseStatusActive, LastHeartbeat: now},
			wantErr: true,
		},
		{
			name:    "legacy partial holder",
			lease:   ControlLease{ScopeID: scope.ID, HolderMachineID: "machine-old", Generation: 0},
			wantErr: true,
		},
		{
			name:    "legacy partial expiry",
			lease:   ControlLease{ScopeID: scope.ID, Generation: 3, LeaseUntil: now.Add(time.Hour)},
			wantErr: true,
		},
		{
			name:    "negative released generation",
			lease:   ControlLease{ScopeID: scope.ID, Generation: -1},
			wantErr: true,
		},
		{
			name:  "complete legacy expired lease",
			lease: ControlLease{ScopeID: scope.ID, HolderMachineID: "machine-old", LeaseUntil: now.Add(-time.Minute)},
		},
		{
			name:  "released lease",
			lease: ControlLease{ScopeID: scope.ID, Generation: 7},
		},
	}
	for _, useSQLite := range []bool{false, true} {
		backend := "json"
		if useSQLite {
			backend = "sqlite"
		}
		for _, tc := range cases {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				store := newTestStore(t)
				if err := store.Update(ctx, func(state *State) error {
					state.Scope = scope
					// SQLite migration itself must be allowed to complete before
					// this test mutates the runtime projection. The JSON backend
					// has no separate projection, so it can be seeded directly.
					state.ControlLease = ControlLease{ScopeID: scope.ID}
					if !useSQLite {
						state.ControlLease = tc.lease
					}
					return nil
				}); err != nil {
					t.Fatalf("seed lease shape: %v", err)
				}
				if useSQLite {
					migrateStoreToSQLiteForTest(t, store)
					raw, err := json.Marshal(tc.lease)
					if err != nil {
						t.Fatalf("encode lease shape: %v", err)
					}
					withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
						_, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, raw, sqliteRuntimeKeyControlLease)
						return err
					})
				}
				decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
					Scope: scope, Machine: claimMachine, Duration: time.Hour, Now: now,
				})
				if tc.wantErr {
					if !errors.Is(err, ErrControlLeaseStateUntrusted) {
						t.Fatalf("claim error = %v, want ErrControlLeaseStateUntrusted", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("claim valid legacy/released lease: %v", err)
				}
				if decision.Mode != LeaseModeActive || decision.Lease.HolderMachineID != claimMachine.ID {
					t.Fatalf("claim decision = %#v, want active claimant", decision)
				}
			})
		}
	}
}

// Once a SQLite runtime projection has been materialized, deleting just its
// control-lease row must not make the claimant fall back to a stale cold copy.
// The missing row is an ownership-unknown hold, not proof that takeover is
// safe.
func TestSQLiteMissingControlLeaseProjectionFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := testOwnerStart()
	scope := ScopeIdentity{ID: "scope-missing-control-lease", AccountID: "account-missing-control-lease", OSUser: "tester", Profile: "default"}
	oldMachine := MachineRecord{ID: "machine-missing-control-lease-old", ScopeID: scope.ID, Kind: MachineKindPrimary}
	claimed, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: oldMachine, Duration: time.Hour, Now: now})
	if err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyControlLease)
		return err
	})
	newMachine := MachineRecord{ID: "machine-missing-control-lease-new", ScopeID: scope.ID, Kind: MachineKindPrimary}
	_, err = store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: newMachine, Duration: time.Hour, Now: now.Add(2 * time.Hour)})
	if !errors.Is(err, ErrControlLeaseStateUntrusted) {
		t.Fatalf("replacement claim error = %v, want ErrControlLeaseStateUntrusted", err)
	}
	if _, err := store.ValidateControlLease(ctx, oldMachine.ID, claimed.Lease.Generation, now); !errors.Is(err, ErrControlLeaseStateUntrusted) {
		t.Fatalf("lease validation error = %v, want ErrControlLeaseStateUntrusted", err)
	}
}

// A partially materialized runtime projection must not make the hot poll path
// resurrect a stale control binding from state_json.  The ready scheduler can
// use the caller's registry control-chat ID and the bounded chat-poll rows even
// when one runtime row is missing; using the old cold document here would both
// reintroduce O(size-of-history) work and potentially poll the wrong chat.
func TestSQLiteHotPollPartialRuntimeProjectionDoesNotFallbackToStaleColdState(t *testing.T) {
	ctx := context.Background()
	for _, missingKey := range []string{sqliteRuntimeKeyScope, sqliteRuntimeKeyControlChat} {
		t.Run(missingKey, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.ControlChat = ControlChatBinding{TeamsChatID: "runtime-control-chat"}
				return nil
			}); err != nil {
				t.Fatalf("seed control binding: %v", err)
			}
			migrateStoreToSQLiteForTest(t, store)

			// Simulate an older/torn cold writer after the runtime projection was
			// materialized. The cold binding must never be used as a fallback for
			// this hot admission read.
			coldRaw := sqliteRawStateJSONForTest(t, store)
			var cold State
			if err := json.Unmarshal(coldRaw, &cold); err != nil {
				t.Fatalf("decode cold state: %v", err)
			}
			cold.ControlChat.TeamsChatID = "stale-cold-control-chat"
			staleRaw, err := json.Marshal(cold)
			if err != nil {
				t.Fatalf("encode stale cold state: %v", err)
			}
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = 'state_json'`, staleRaw); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `DELETE FROM runtime_state WHERE key = ?`, missingKey)
				return err
			})

			schedule, err := store.HotPollReadyScheduleState(ctx, "runtime-control-chat", time.Now().UTC())
			if err != nil {
				t.Fatalf("hot ready schedule with partial runtime projection: %v", err)
			}
			if got := schedule.ControlChat.TeamsChatID; got == "stale-cold-control-chat" {
				t.Fatalf("hot admission resurrected stale cold control binding: %#v", schedule.ControlChat)
			}
			if got := schedule.SchemaVersion; got != SchemaVersion {
				t.Fatalf("partial runtime projection schema version = %d, want %d", got, SchemaVersion)
			}
			if missingKey == sqliteRuntimeKeyScope && schedule.ControlChat.TeamsChatID != "runtime-control-chat" {
				t.Fatalf("valid runtime control binding was lost with missing %q row: %#v", missingKey, schedule.ControlChat)
			}
		})
	}
}

// Once the SQLite runtime projection has been materialized, an empty runtime
// table is corruption rather than the old pre-projection compatibility shape.
// Falling back to state_json in that case can resurrect a stale control-chat
// binding and make the listener poll the wrong Graph chat.
func TestSQLiteEmptyMaterializedRuntimeProjectionDoesNotFallbackToStaleColdState(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.ControlChat = ControlChatBinding{TeamsChatID: "runtime-control-chat"}
		return nil
	}); err != nil {
		t.Fatalf("seed control binding: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	coldRaw := sqliteRawStateJSONForTest(t, store)
	var cold State
	if err := json.Unmarshal(coldRaw, &cold); err != nil {
		t.Fatalf("decode cold state: %v", err)
	}
	cold.ControlChat.TeamsChatID = "stale-cold-control-chat"
	staleRaw, err := json.Marshal(cold)
	if err != nil {
		t.Fatalf("encode stale cold state: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = 'state_json'`, staleRaw); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM runtime_state`)
		return err
	})

	schedule, err := store.HotPollReadyScheduleState(ctx, "runtime-control-chat", time.Now().UTC())
	if err != nil {
		t.Fatalf("hot ready schedule with empty materialized runtime projection: %v", err)
	}
	if schedule.SchemaVersion != SchemaVersion {
		t.Fatalf("empty materialized runtime projection schema version = %d, want %d", schedule.SchemaVersion, SchemaVersion)
	}
	if got := schedule.ControlChat.TeamsChatID; got == "stale-cold-control-chat" {
		t.Fatalf("empty materialized runtime projection resurrected stale cold binding: %#v", schedule.ControlChat)
	}
}

// A full runtime-state mutation must not use a stale cold snapshot to fill a
// missing required projection row after the runtime projection has already
// been materialized.  The safe result is a fenced error; targeted liveness
// repair paths remain responsible for independently proving an expired lease.
func TestSQLitePartialMaterializedRuntimeProjectionRejectsFullStateMutation(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = ScopeIdentity{ID: "runtime-scope"}
		state.ControlChat = ControlChatBinding{TeamsChatID: "runtime-control-chat"}
		return nil
	}); err != nil {
		t.Fatalf("seed runtime state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)

	coldRaw := sqliteRawStateJSONForTest(t, store)
	var cold State
	if err := json.Unmarshal(coldRaw, &cold); err != nil {
		t.Fatalf("decode cold state: %v", err)
	}
	cold.Scope = ScopeIdentity{ID: "stale-cold-scope"}
	staleRaw, err := json.Marshal(cold)
	if err != nil {
		t.Fatalf("encode stale cold state: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = 'state_json'`, staleRaw); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyScope)
		return err
	})

	if _, err := store.SetDraining(ctx, "partial-runtime-projection"); !errors.Is(err, ErrSQLiteRuntimeProjectionIncomplete) {
		t.Fatalf("full runtime mutation error = %v, want ErrSQLiteRuntimeProjectionIncomplete", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyScope).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("missing runtime scope row was unexpectedly recreated: count=%d", count)
		}
		return nil
	})
}

func TestSQLiteNarrowRuntimeWriterMarksProjectionMaterialized(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := testOwnerStart()
	owner := testOwner("narrow-marker-session", "narrow-marker-turn", now)
	owner.ScopeID = "scope-narrow-marker"
	owner.MachineID = "machine-narrow-marker"
	owner.LeaseGeneration = 4
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = ScopeIdentity{ID: owner.ScopeID}
		state.ServiceOwner = &owner
		state.LockOwner = &owner
		state.ControlLease = ControlLease{
			ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
			Generation: owner.LeaseGeneration, Status: ControlLeaseStatusActive,
			LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed narrow marker state: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM state_meta WHERE key = ?`, sqliteRuntimeProjectionMaterializedKey); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM runtime_state`)
		return err
	})

	if _, err := store.RecordOwnerHeartbeat(ctx, owner, time.Minute, now.Add(time.Minute)); err != nil {
		t.Fatalf("legacy narrow heartbeat: %v", err)
	}
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		var marker string
		if err := tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteRuntimeProjectionMaterializedKey).Scan(&marker); err != nil {
			return err
		}
		if marker != sqliteRuntimeProjectionMaterializedValue {
			return fmt.Errorf("runtime projection marker = %q, want %q", marker, sqliteRuntimeProjectionMaterializedValue)
		}
		return nil
	})
}

// Thread binding is a pre-dispatch ownership boundary too. It must not treat
// an unknown lease status as an active lease merely because the holder,
// generation, and expiry fields happen to look plausible.
func TestCodexThreadBindingRejectsUnknownControlLeaseStatusAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			now := testOwnerStart()
			owner := testOwner("session-binding", "turn-binding", now)
			owner.ScopeID = "scope-thread-unknown-lease-" + name
			owner.MachineID = "machine-thread-unknown-lease-" + name
			owner.LeaseGeneration = 21
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = ScopeIdentity{ID: owner.ScopeID}
				state.ServiceOwner = &owner
				state.LockOwner = &owner
				state.ControlLease = ControlLease{
					ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
					Generation: owner.LeaseGeneration,
					Status:     ControlLeaseStatusActive,
					LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
				}
				state.Sessions["session-binding"] = SessionContext{ID: "session-binding", Status: SessionStatusActive, ModelGeneration: 4, TeamsChatID: "chat-binding"}
				state.Turns["turn-binding"] = Turn{ID: "turn-binding", SessionID: "session-binding", Status: TurnStatusRunning, ModelGeneration: 4, MachineID: owner.MachineID, LeaseGeneration: owner.LeaseGeneration, CreatedAt: now}
				return nil
			}); err != nil {
				t.Fatalf("seed thread binding state: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
				unknownRaw := []byte(`{"status":"future-thread-lease-state","holder_machine_id":"` + owner.MachineID + `","generation":21,"lease_until":"2026-05-01T12:00:00Z"}`)
				withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, unknownRaw, sqliteRuntimeKeyControlLease)
					return err
				})
			} else if err := store.Update(ctx, func(state *State) error {
				state.ControlLease.Status = ControlLeaseStatus("future-thread-lease-state")
				return nil
			}); err != nil {
				t.Fatalf("persist unknown thread lease status: %v", err)
			}

			_, err := store.BindCodexThreadForRunningTurn(ctx, CodexThreadStartBindingRequest{
				SessionID: "session-binding", TurnID: "turn-binding", ThreadID: "thread-unknown-lease",
				ModelGeneration: 4, MachineID: owner.MachineID, LeaseGeneration: owner.LeaseGeneration, Owner: owner,
			})
			if !errors.Is(err, ErrControlLeaseStatusUnknown) {
				t.Fatalf("thread binding error = %v, want ErrControlLeaseStatusUnknown", err)
			}
		})
	}
}

// Releasing a valid lease is a liveness operation. An unrelated torn runtime
// projection must not strand that lease, but the release must preserve the
// opaque bytes for explicit repair instead of replacing them with a value
// reconstructed from the cold state.
func TestSQLiteLeaseReleasePreservesMalformedUnrelatedRuntimeRow(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := testOwnerStart()
	scope := ScopeIdentity{ID: "scope-release-opaque", AccountID: "account-release-opaque", OSUser: "tester", Profile: "default"}
	machine := MachineRecord{ID: "machine-release-opaque", ScopeID: scope.ID, Kind: MachineKindPrimary}
	claimed, err := store.ClaimControlLease(ctx, ControlLeaseClaim{Scope: scope, Machine: machine, Duration: time.Hour, Now: now})
	if err != nil {
		t.Fatalf("initial lease claim: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	corruptRaw := []byte(`{"machines":"torn"`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, corruptRaw, sqliteRuntimeKeyMachines)
		return err
	})

	released, err := store.ReleaseControlLeaseIfHolder(ctx, machine.ID, claimed.Lease.Generation)
	if err != nil || !released {
		t.Fatalf("release with unrelated malformed runtime row = released:%v err:%v, want successful release", released, err)
	}
	if got := sqliteRuntimeRawForTest(t, store, sqliteRuntimeKeyMachines); string(got) != string(corruptRaw) {
		t.Fatalf("malformed machines row after release = %q, want %q", got, corruptRaw)
	}
	leaseRaw := sqliteRuntimeRawForTest(t, store, sqliteRuntimeKeyControlLease)
	var lease ControlLease
	if err := json.Unmarshal(leaseRaw, &lease); err != nil {
		t.Fatalf("decode released lease: %v", err)
	}
	if lease.HolderMachineID != "" || lease.Generation != claimed.Lease.Generation {
		t.Fatalf("released lease = %#v, want empty holder at generation %d", lease, claimed.Lease.Generation)
	}
	var opaque []byte
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key LIKE ? ORDER BY key LIMIT 1`, sqliteRuntimeOpaqueMetaPrefix+sqliteRuntimeKeyMachines+":%").Scan(&opaque)
	})
	if string(opaque) != string(corruptRaw) {
		t.Fatalf("opaque machines row = %q, want %q", opaque, corruptRaw)
	}
}

// clearOwnerIfSame may run while a stale cold snapshot is being rewritten.
// It may clear a proven matching owner despite an unrelated malformed scope
// row, but must retain that row and refuse to make it part of a synthetic
// replacement state.
func TestSQLiteClearOwnerPreservesMalformedUnrelatedRuntimeRow(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	now := testOwnerStart()
	owner := testOwner("opaque-clear-session", "opaque-clear-turn", now)
	owner.ScopeID = "scope-clear-opaque"
	owner.MachineID = "machine-clear-opaque"
	owner.LeaseGeneration = 17
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = ScopeIdentity{ID: owner.ScopeID}
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
	migrateStoreToSQLiteForTest(t, store)
	corruptRaw := []byte(`{"scope":`)
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, corruptRaw, sqliteRuntimeKeyScope)
		return err
	})

	cleared, err := store.ClearOwnerIfSame(ctx, owner)
	if err != nil || !cleared {
		t.Fatalf("clear matching owner with unrelated malformed row = cleared:%v err:%v, want successful clear", cleared, err)
	}
	for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
		raw := sqliteRuntimeRawForTest(t, store, key)
		if string(raw) != "null" {
			t.Fatalf("%s after clear = %q, want null", key, raw)
		}
	}
	if got := sqliteRuntimeRawForTest(t, store, sqliteRuntimeKeyScope); string(got) != string(corruptRaw) {
		t.Fatalf("malformed scope row after clear = %q, want %q", got, corruptRaw)
	}
}

// A pre-generation store may have durable owner rows but no control-lease
// history.  The absence of a lease tuple is not proof that the old process is
// gone: a restart or a torn legacy write can leave exactly this shape while
// the writer is still alive.  A different process must stay standby until the
// owner becomes stale; otherwise the first post-upgrade claim can create two
// writers.
func TestClaimControlLeaseDoesNotOverwriteFreshLegacyOwnerWithoutLeaseHistory(t *testing.T) {
	prevHostname := ownerHostname
	prevAlive := ownerProcessAlive
	t.Cleanup(func() {
		ownerHostname = prevHostname
		ownerProcessAlive = prevAlive
	})
	ownerHostname = func() (string, error) { return "host-a", nil }
	ownerProcessAlive = func(pid int) bool { return pid == 4242 || pid == 7777 }

	ctx := context.Background()
	now := testOwnerStart()
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			scope := ScopeIdentity{ID: "scope-legacy-owner-" + name}
			oldMachine := MachineRecord{ID: "machine-legacy-owner-old-" + name, ScopeID: scope.ID, Kind: MachineKindPrimary}
			oldOwner := testOwner("legacy-session", "legacy-turn", now)
			oldOwner.PID = 4242
			oldOwner.MachineID = oldMachine.ID
			oldOwner.ScopeID = scope.ID
			oldOwner.LastHeartbeat = now
			// Scope-only control-lease state is a valid released/never-claimed
			// shape and therefore models the actual missing-history boundary.
			legacyLease := ControlLease{ScopeID: scope.ID}
			if err := store.Update(ctx, func(state *State) error {
				state.Scope = scope
				state.Machines[oldMachine.ID] = oldMachine
				state.ServiceOwner = &oldOwner
				state.LockOwner = &oldOwner
				state.ControlLease = legacyLease
				return nil
			}); err != nil {
				t.Fatalf("seed legacy owner: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			newMachine := MachineRecord{ID: "machine-legacy-owner-new-" + name, ScopeID: scope.ID, Kind: MachineKindPrimary}
			newOwner := testOwner("new-session", "new-turn", now)
			newOwner.PID = 7777
			newOwner.MachineID = newMachine.ID
			newOwner.ScopeID = scope.ID
			newOwner.LastHeartbeat = now
			decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
				Scope: scope, Machine: newMachine, Owner: newOwner,
				Duration: time.Minute, Now: now.Add(10 * time.Second),
			})
			if err != nil {
				t.Fatalf("fresh legacy owner claim: %v", err)
			}
			if decision.Mode != LeaseModeStandby {
				t.Fatalf("fresh legacy owner claim mode = %q, want standby: %#v", decision.Mode, decision)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after fresh legacy owner claim: %v", err)
			}
			owner, ok := state.readOwner()
			if !ok || !sameOwnerProcess(owner, oldOwner) || !owner.LastHeartbeat.Equal(oldOwner.LastHeartbeat) {
				t.Fatalf("fresh legacy owner was overwritten: owner=%#v ok=%v want=%#v", owner, ok, oldOwner)
			}
			if state.ControlLease != legacyLease {
				t.Fatalf("fresh legacy owner claim changed lease history: got=%#v want=%#v", state.ControlLease, legacyLease)
			}
		})
	}
}

// When the current control-lease row is corrupt, an expired cold snapshot is
// usable only if the current owner projection independently proves that the
// old holder is stale.  A fresh runtime heartbeat after the cold snapshot must
// keep a replacement claimant fail-closed.
func TestSQLiteCorruptControlLeaseDoesNotTrustStaleColdCopy(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	coldAt := testOwnerStart()
	runtimeAt := coldAt.Add(time.Hour)
	scope := ScopeIdentity{ID: "scope-corrupt-lease-cold-copy"}
	oldMachine := MachineRecord{ID: "machine-corrupt-lease-old", ScopeID: scope.ID, Kind: MachineKindPrimary}
	oldOwner := testOwner("cold-session", "cold-turn", coldAt)
	oldOwner.MachineID = oldMachine.ID
	oldOwner.ScopeID = scope.ID
	oldOwner.LeaseGeneration = 7
	oldOwner.LastHeartbeat = coldAt
	coldLease := ControlLease{
		ScopeID: scope.ID, HolderMachineID: oldMachine.ID, Generation: 7,
		Status: ControlLeaseStatusActive, LeaseUntil: coldAt.Add(time.Minute),
		LastHeartbeat: coldAt, UpdatedAt: coldAt,
	}
	if err := store.Update(ctx, func(state *State) error {
		state.Scope = scope
		state.Machines[oldMachine.ID] = oldMachine
		state.ServiceOwner = &oldOwner
		state.LockOwner = &oldOwner
		state.ControlLease = coldLease
		return nil
	}); err != nil {
		t.Fatalf("seed cold lease: %v", err)
	}
	migrateStoreToSQLiteForTest(t, store)
	freshOwner := oldOwner
	freshOwner.LastHeartbeat = runtimeAt
	freshOwner.ActiveTurnID = "fresh-runtime-turn"
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
			raw, err := json.Marshal(freshOwner)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, raw, key); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, []byte("{\"control-lease-is-torn\""), sqliteRuntimeKeyControlLease)
		return err
	})

	newMachine := MachineRecord{ID: "machine-corrupt-lease-new", ScopeID: scope.ID, Kind: MachineKindPrimary}
	_, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
		Scope: scope, Machine: newMachine, Duration: time.Minute, Now: runtimeAt.Add(time.Second),
	})
	if !errors.Is(err, ErrControlLeaseStateUntrusted) || !strings.Contains(err.Error(), "expiry is not provable") {
		t.Fatalf("corrupt lease with fresh runtime owner error = %v, want fail-closed expiry diagnostic", err)
	}
	if got := sqliteRuntimeRawForTest(t, store, sqliteRuntimeKeyControlLease); string(got) != "{\"control-lease-is-torn\"" {
		t.Fatalf("corrupt control lease row changed after rejected claim: %q", got)
	}
}

// A runtime owner projection can be syntactically valid while still being
// unusable as ownership evidence.  In particular, a torn write may leave the
// service-owner copy as {} while the compatibility lock-owner copy still has
// the current lease binding.  The latter must win; otherwise a same-machine
// replacement can take over an unexpired lease.  If both copies are
// semantically empty, the active lease remains a hold rather than becoming an
// accidental takeover opportunity.
func TestClaimControlLeaseRejectsUntrustedActiveOwnerProjection(t *testing.T) {
	ctx := context.Background()
	now := testOwnerStart()
	for _, useSQLite := range []bool{false, true} {
		backend := "json"
		if useSQLite {
			backend = "sqlite"
		}
		t.Run(backend, func(t *testing.T) {
			for _, corruption := range []string{"service-empty", "service-legacy", "both-empty"} {
				t.Run(corruption, func(t *testing.T) {
					store := newTestStore(t)
					scope := ScopeIdentity{ID: "scope-owner-projection-" + backend + "-" + corruption}
					machine := MachineRecord{ID: "machine-owner-projection-" + backend + "-" + corruption, ScopeID: scope.ID, Kind: MachineKindPrimary}
					ownerA := testOwner("owner-projection-session", "owner-projection-turn", now)
					ownerA.ScopeID = scope.ID
					ownerA.MachineID = machine.ID
					first, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
						Scope: scope, Machine: machine, Owner: ownerA, Duration: time.Minute, Now: now,
					})
					if err != nil || first.Mode != LeaseModeActive {
						t.Fatalf("initial claim = %#v err=%v", first, err)
					}
					ownerA.LeaseGeneration = first.Lease.Generation
					if _, err := store.RecordOwnerHeartbeatForLease(ctx, ownerA, time.Minute, time.Minute, now); err != nil {
						t.Fatalf("initial owner heartbeat = %v", err)
					}
					if useSQLite {
						migrateStoreToSQLiteForTest(t, store)
					}

					legacyOwner := ownerA
					legacyOwner.MachineID = ""
					legacyOwner.LeaseGeneration = 0
					if useSQLite {
						withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
							if corruption == "both-empty" {
								for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
									if _, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = '{}' WHERE key = ?`, key); err != nil {
										return err
									}
								}
								return nil
							}
							raw := []byte(`{}`)
							if corruption == "service-legacy" {
								var err error
								raw, err = json.Marshal(legacyOwner)
								if err != nil {
									return err
								}
							}
							_, err := tx.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, raw, sqliteRuntimeKeyServiceOwner)
							return err
						})
					} else if err := store.Update(ctx, func(state *State) error {
						if corruption == "both-empty" {
							state.ServiceOwner = &OwnerMetadata{}
							state.LockOwner = &OwnerMetadata{}
						} else {
							serviceOwner := OwnerMetadata{}
							if corruption == "service-legacy" {
								serviceOwner = legacyOwner
							}
							state.ServiceOwner = &serviceOwner
							lockOwner := ownerA
							state.LockOwner = &lockOwner
						}
						return nil
					}); err != nil {
						t.Fatalf("seed owner projection = %v", err)
					}

					ownerB := ownerA
					ownerB.PID++
					ownerB.StartedAt = now.Add(time.Second)
					ownerB.LastHeartbeat = ownerB.StartedAt
					decision, err := store.ClaimControlLease(ctx, ControlLeaseClaim{
						Scope: scope, Machine: machine, Owner: ownerB, Duration: time.Minute, Now: now.Add(time.Second),
					})
					if err != nil {
						t.Fatalf("replacement claim = %v", err)
					}
					if decision.Mode != LeaseModeStandby {
						t.Fatalf("replacement claim mode = %q, want standby: %#v", decision.Mode, decision)
					}
					state, err := store.Load(ctx)
					if err != nil {
						t.Fatalf("load after replacement claim = %v", err)
					}
					if state.ControlLease.HolderMachineID != machine.ID || state.ControlLease.Generation != first.Lease.Generation {
						t.Fatalf("active lease changed after untrusted owner claim: %#v", state.ControlLease)
					}
					if corruption == "service-empty" || corruption == "service-legacy" {
						got, found, err := store.ReadOwner(ctx)
						if err != nil || !found {
							t.Fatalf("ReadOwner fallback = %#v found=%v err=%v", got, found, err)
						}
						if got.MachineID != ownerA.MachineID || got.LeaseGeneration != ownerA.LeaseGeneration {
							t.Fatalf("ReadOwner fallback = %#v, want current lock owner %#v", got, ownerA)
						}
					}
				})
			}
		})
	}
}

func sqliteRuntimeRawForTest(t *testing.T, store *Store, key string) []byte {
	t.Helper()
	ctx := context.Background()
	var raw []byte
	withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT json FROM runtime_state WHERE key = ?`, key).Scan(&raw)
	})
	return append([]byte(nil), raw...)
}
