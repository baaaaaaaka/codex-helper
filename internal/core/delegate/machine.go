package delegate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Meta keys of the machine identity.
const (
	// MetaMachineID is the registry machine id; `cxp teams migrate` copies
	// the old store's machine identity into it.
	MetaMachineID = "machine_id"
	// metaScopeID is the old store's scope (copied by the migration).
	metaScopeID = "scope_id"
)

// ResolveMachineID returns this machine's registry id. It is, in order:
// CODEX_HELPER_TEAMS_MACHINE_ID; the id kept in the store (machine_id, also
// imported from the old store); the id the old core derived for this host
// from the migrated scope; else a new id derived from the account and this
// host. A derived id is kept in the store, so it does not change when the
// host name or user does. Keeping the old id keeps the inbox chat and the
// requesters' routes.
func ResolveMachineID(st *store.Store, tenantID, ownerID string) (string, error) {
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_MACHINE_ID")); v != "" {
		return v, nil
	}
	if v, ok, err := st.Meta(MetaMachineID); err != nil {
		return "", err
	} else if ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v), nil
	}
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "local"
	}
	scope, _, err := st.Meta(metaScopeID)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(scope) == "" {
		// No old store: a scope of this account (the old core's scope id is
		// not derivable here, and nothing refers to one yet).
		scope = "core:" + strings.TrimSpace(tenantID) + ":" + strings.TrimSpace(ownerID)
	}
	id := stableID("machine", []string{"v1", strings.TrimSpace(scope), host, osUser(), machineKind()})
	err = st.Update(func(tx *store.Tx) error { return tx.SetMeta(MetaMachineID, id) })
	return id, err
}

// stableID is the old core's id derivation (internal/teams/scope.go).
func stableID(prefix string, parts []string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + ":" + hex.EncodeToString(sum[:])[:16]
}

func osUser() string {
	for _, k := range []string{"USER", "LOGNAME", "USERNAME"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return home
	}
	return "unknown"
}

// machineKind is the old core's machine kind (primary, or ephemeral in a
// batch job).
func machineKind() string {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_MACHINE_KIND"))) {
	case "primary", "long", "long-lived", "persistent":
		return "primary"
	case "ephemeral", "temporary", "temp":
		return "ephemeral"
	}
	for _, k := range []string{"SLURM_JOB_ID", "PBS_JOBID", "LSB_JOBID", "JOB_ID", "KUBERNETES_SERVICE_HOST"} {
		if strings.TrimSpace(os.Getenv(k)) != "" {
			return "ephemeral"
		}
	}
	return "primary"
}

// metaOwnerID keeps the account id the registry is keyed by, so a start
// without network still knows the inbox key.
const metaOwnerID = "registry_owner_id"

// Identity is this machine's registry identity.
type Identity struct {
	MachineID, OwnerID, InboxKey string
}

// ResolveIdentity returns the machine id (ResolveMachineID) and the inbox
// key; the account id is read once from the channel and kept.
func ResolveIdentity(ctx context.Context, st *store.Store, ch MeetingChannel, tenantID string) (Identity, error) {
	owner, ok, err := st.Meta(metaOwnerID)
	if err != nil {
		return Identity{}, err
	}
	if !ok || strings.TrimSpace(owner) == "" {
		if owner, _, err = ch.Owner(ctx); err != nil {
			return Identity{}, err
		}
		if err := st.Update(func(tx *store.Tx) error { return tx.SetMeta(metaOwnerID, owner) }); err != nil {
			return Identity{}, err
		}
	}
	id, err := ResolveMachineID(st, tenantID, owner)
	if err != nil {
		return Identity{}, err
	}
	return Identity{MachineID: id, OwnerID: owner, InboxKey: InboxKey(tenantID, owner, id)}, nil
}
