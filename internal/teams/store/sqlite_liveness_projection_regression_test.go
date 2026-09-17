package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// Once the runtime projection marker is durable, liveness cleanup must not
// reconstruct ownership from state_json when one required runtime row is
// missing.  The cold document is an older compatibility snapshot and may
// contain a different owner from the one that last held the durable lease.
func TestSQLiteMaterializedIncompleteProjectionRejectsColdOwnershipForLivenessCleanup(t *testing.T) {
	ctx := context.Background()
	for _, operation := range []string{"read-owner", "release-lease", "clear-owner"} {
		t.Run(operation, func(t *testing.T) {
			store, owner, _ := seedSQLiteSchemaFenceHeartbeatStore(t)
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `DELETE FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyServiceOwner)
				return err
			})

			switch operation {
			case "read-owner":
				if _, found, err := store.ReadOwner(ctx); !errors.Is(err, ErrSQLiteRuntimeProjectionIncomplete) || found {
					t.Fatalf("ReadOwner with a missing materialized owner row = found=%t err=%v, want incomplete projection", found, err)
				}
			case "release-lease":
				released, err := store.ReleaseControlLeaseIfHolder(ctx, owner.MachineID, owner.LeaseGeneration)
				if !errors.Is(err, ErrSQLiteRuntimeProjectionIncomplete) || released {
					t.Fatalf("ReleaseControlLeaseIfHolder with a missing materialized owner row = released=%t err=%v, want incomplete projection", released, err)
				}
			case "clear-owner":
				cleared, err := store.ClearOwnerIfSame(ctx, owner)
				if !errors.Is(err, ErrSQLiteRuntimeProjectionIncomplete) || cleared {
					t.Fatalf("ClearOwnerIfSame with a missing materialized owner row = cleared=%t err=%v, want incomplete projection", cleared, err)
				}
			}

			var count int
			withSQLiteTxForTest(t, store, func(tx *sql.Tx) error {
				if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyServiceOwner).Scan(&count); err != nil {
					return err
				}
				return nil
			})
			if count != 0 {
				t.Fatalf("rejected liveness cleanup recreated the missing owner row: count=%d", count)
			}
			lease, err := readSQLiteControlLeaseForTest(t, store)
			if err != nil {
				t.Fatalf("read control lease after rejected liveness cleanup: %v", err)
			}
			if lease.HolderMachineID != owner.MachineID || lease.Generation != owner.LeaseGeneration {
				t.Fatalf("rejected liveness cleanup changed control lease: %#v", lease)
			}
		})
	}
}
