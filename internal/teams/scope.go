package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

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
		"v2",
		scope.AccountID,
		scope.UserPrincipal,
		scope.Profile,
	})
	return scope
}

func ResolveStorePathForScope(scope teamstore.ScopeIdentity) (teamstore.ScopeIdentity, string, error) {
	scope = normalizeScopeForResolution(scope)
	currentPath, err := DefaultStorePathForScope(scope.ID)
	if err != nil {
		return scope, "", err
	}
	resolved, path, ok, err := resolveExistingScopeStore(scope, currentPath)
	if err != nil {
		return scope, "", err
	}
	if ok {
		scope.ID = resolved.ID
		if !resolved.CreatedAt.IsZero() {
			scope.CreatedAt = resolved.CreatedAt
		}
		return scope, path, nil
	}
	return scope, currentPath, nil
}

func ResolveRegistryPathForScope(scope teamstore.ScopeIdentity) (teamstore.ScopeIdentity, string, error) {
	resolved, _, err := ResolveStorePathForScope(scope)
	if err != nil {
		return scope, "", err
	}
	path, err := DefaultRegistryPathForScope(resolved.ID)
	if err != nil {
		return resolved, "", err
	}
	return resolved, path, nil
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

type resolvedScopeStoreCandidate struct {
	scope   teamstore.ScopeIdentity
	path    string
	score   int
	updated time.Time
}

func resolveExistingScopeStore(scope teamstore.ScopeIdentity, currentPath string) (teamstore.ScopeIdentity, string, bool, error) {
	paths, err := candidateScopeStorePaths(currentPath)
	if err != nil {
		return teamstore.ScopeIdentity{}, "", false, err
	}
	now := time.Now()
	var candidates []resolvedScopeStoreCandidate
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return teamstore.ScopeIdentity{}, "", false, err
			}
		}
		st, err := teamstore.Open(path)
		if err != nil {
			return teamstore.ScopeIdentity{}, "", false, err
		}
		state, err := st.Load(context.Background())
		closeErr := st.Close()
		if err != nil {
			return teamstore.ScopeIdentity{}, "", false, err
		}
		if closeErr != nil {
			return teamstore.ScopeIdentity{}, "", false, closeErr
		}
		if !scopeStateMatches(scope, state) {
			continue
		}
		candidateScope := state.Scope
		if strings.TrimSpace(candidateScope.ID) == "" {
			candidateScope = scope
		}
		candidate := resolvedScopeStoreCandidate{
			scope:   candidateScope,
			path:    path,
			score:   scopeStoreResolutionScore(state, path, currentPath, now),
			updated: candidateScope.UpdatedAt,
		}
		if info, err := os.Stat(path); err == nil && info.ModTime().After(candidate.updated) {
			candidate.updated = info.ModTime()
		}
		candidates = append(candidates, candidate)
	}
	if len(candidates) == 0 {
		return teamstore.ScopeIdentity{}, "", false, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		if !candidates[i].updated.Equal(candidates[j].updated) {
			return candidates[i].updated.After(candidates[j].updated)
		}
		return candidates[i].path < candidates[j].path
	})
	best := candidates[0]
	return best.scope, best.path, true, nil
}

func candidateScopeStorePaths(currentPath string) ([]string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}
	matches, err := filepath.Glob(filepath.Join(base, "codex-helper", "teams", "scopes", "*", "state.json"))
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var paths []string
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" || seen[path] {
			return
		}
		seen[path] = true
		paths = append(paths, path)
	}
	add(currentPath)
	sort.Strings(matches)
	for _, path := range matches {
		add(path)
	}
	return paths, nil
}

func scopeStoreResolutionScore(state teamstore.State, path string, currentPath string, now time.Time) int {
	score := 0
	if path == currentPath {
		score += 10
	}
	if teamstore.HasUpgradeBlockingWork(state, now) {
		score += 1000
	}
	if state.ServiceOwner != nil || state.LockOwner != nil {
		score += 900
	}
	if state.ControlLease.HolderMachineID != "" {
		score += 800
	}
	if strings.TrimSpace(state.ControlChat.TeamsChatID) != "" {
		score += 500
	}
	score += len(state.Sessions) * 10
	score += len(state.OutboxMessages)
	return score
}

func scopeStateMatches(current teamstore.ScopeIdentity, state teamstore.State) bool {
	if scopeIdentityMatches(current, state.Scope) {
		return true
	}
	control := state.ControlChat
	if strings.TrimSpace(control.ScopeID) == "" && strings.TrimSpace(control.AccountID) == "" && strings.TrimSpace(control.Profile) == "" {
		return false
	}
	return scopeIdentityMatches(current, teamstore.ScopeIdentity{
		ID:        control.ScopeID,
		AccountID: control.AccountID,
		Profile:   control.Profile,
	})
}

func scopeIdentityMatches(current teamstore.ScopeIdentity, existing teamstore.ScopeIdentity) bool {
	current = normalizeScopeForResolution(current)
	existing = normalizeScopeForResolution(existing)
	if current.ID != "" && current.ID == existing.ID {
		return true
	}
	if current.Profile != existing.Profile {
		return false
	}
	accountMatches := current.AccountID != "" && existing.AccountID != "" && current.AccountID == existing.AccountID
	principalMatches := current.UserPrincipal != "" && existing.UserPrincipal != "" && strings.EqualFold(current.UserPrincipal, existing.UserPrincipal)
	return accountMatches || principalMatches
}

func normalizeScopeForResolution(scope teamstore.ScopeIdentity) teamstore.ScopeIdentity {
	scope.ID = strings.TrimSpace(scope.ID)
	scope.AccountID = strings.TrimSpace(scope.AccountID)
	scope.UserPrincipal = strings.TrimSpace(scope.UserPrincipal)
	scope.OSUser = strings.TrimSpace(scope.OSUser)
	scope.Profile = strings.TrimSpace(scope.Profile)
	if scope.Profile == "" {
		scope.Profile = "default"
	}
	scope.ConfigPath = strings.TrimSpace(scope.ConfigPath)
	scope.CodexHome = strings.TrimSpace(scope.CodexHome)
	return scope
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
