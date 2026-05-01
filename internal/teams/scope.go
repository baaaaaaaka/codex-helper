package teams

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	envTeamsMachineID       = "CODEX_HELPER_TEAMS_MACHINE_ID"
	envTeamsMachineLabel    = "CODEX_HELPER_TEAMS_MACHINE_LABEL"
	envTeamsMachineKind     = "CODEX_HELPER_TEAMS_MACHINE_KIND"
	envTeamsMachinePriority = "CODEX_HELPER_TEAMS_MACHINE_PRIORITY"
	envTeamsProfile         = "CODEX_HELPER_TEAMS_PROFILE"
)

func ScopeIdentityForUser(user User) teamstore.ScopeIdentity {
	scope := teamstore.ScopeIdentity{
		AccountID:     strings.TrimSpace(user.ID),
		UserPrincipal: strings.TrimSpace(user.UserPrincipalName),
		OSUser:        localOSUser(),
		Profile:       strings.TrimSpace(os.Getenv(envTeamsProfile)),
		ConfigPath:    strings.TrimSpace(os.Getenv("CODEX_HELPER_CONFIG")),
		CodexHome:     strings.TrimSpace(firstNonEmptyString(os.Getenv("CODEX_HOME"), os.Getenv("CODEX_DIR"))),
	}
	if scope.Profile == "" {
		scope.Profile = "default"
	}
	scope.ID = stableID("scope", []string{
		"v1",
		scope.OSUser,
		scope.AccountID,
		scope.UserPrincipal,
		scope.Profile,
		scope.ConfigPath,
		scope.CodexHome,
	})
	return scope
}

func DefaultStorePathForScope(scopeID string) (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams", "scopes", safeScopePathPart(scopeID), "state.json"), nil
}

func DefaultRegistryPathForScope(scopeID string) (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams", "scopes", safeScopePathPart(scopeID), "registry.json"), nil
}

func MachineRecordForUser(user User, scope teamstore.ScopeIdentity) teamstore.MachineRecord {
	hostname := machineLabel()
	label := strings.TrimSpace(os.Getenv(envTeamsMachineLabel))
	if label == "" {
		label = hostname
	}
	kind := machineKindFromEnv()
	priority := machinePriorityFromEnv(kind)
	machineID := strings.TrimSpace(os.Getenv(envTeamsMachineID))
	if machineID == "" {
		machineID = stableID("machine", []string{
			"v1",
			scope.ID,
			hostname,
			localOSUser(),
			string(kind),
		})
	}
	return teamstore.MachineRecord{
		ID:            machineID,
		ScopeID:       scope.ID,
		Label:         label,
		Hostname:      hostname,
		OSUser:        localOSUser(),
		AccountID:     user.ID,
		UserPrincipal: user.UserPrincipalName,
		Profile:       scope.Profile,
		Kind:          kind,
		Priority:      priority,
	}
}

func machineKindFromEnv() teamstore.MachineKind {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envTeamsMachineKind))) {
	case "primary", "long", "long-lived", "persistent":
		return teamstore.MachineKindPrimary
	case "ephemeral", "temporary", "temp":
		return teamstore.MachineKindEphemeral
	default:
		if looksLikeBatchJob() {
			return teamstore.MachineKindEphemeral
		}
		return teamstore.MachineKindPrimary
	}
}

func machinePriorityFromEnv(kind teamstore.MachineKind) int {
	raw := strings.TrimSpace(os.Getenv(envTeamsMachinePriority))
	if raw == "" {
		return teamstore.DefaultMachinePriority(kind)
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return teamstore.DefaultMachinePriority(kind)
	}
	return value
}

func looksLikeBatchJob() bool {
	for _, name := range []string{"SLURM_JOB_ID", "PBS_JOBID", "LSB_JOBID", "JOB_ID", "KUBERNETES_SERVICE_HOST"} {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			return true
		}
	}
	return false
}

func localOSUser() string {
	for _, name := range []string{"USER", "LOGNAME", "USERNAME"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return home
	}
	return "unknown"
}

func stableID(prefix string, parts []string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return prefix + ":" + hex.EncodeToString(sum[:])[:16]
}

func safeScopePathPart(scopeID string) string {
	scopeID = strings.TrimSpace(scopeID)
	if scopeID == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range scopeID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" {
		return "default"
	}
	return out
}
