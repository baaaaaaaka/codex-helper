package beacon

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func IdempotencyKey(messageID, normalizedCommand, confirmationToken string) string {
	return strings.TrimSpace(messageID) + "\x00" + strings.TrimSpace(normalizedCommand) + "\x00" + strings.TrimSpace(confirmationToken)
}

func ApplyIdempotent(st *State, messageID, normalizedCommand, confirmationToken string, now time.Time, fn func() (string, error)) (string, bool, error) {
	if st == nil {
		return "", false, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	key := IdempotencyKey(messageID, normalizedCommand, confirmationToken)
	if rec, ok := st.Idempotency[key]; ok {
		return rec.Result, false, nil
	}
	result, err := fn()
	if err != nil {
		return "", false, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	st.Idempotency[key] = IdempotencyRecord{Key: key, Result: result, CreatedAt: now}
	return result, true, nil
}

type ReleaseInput struct {
	MachineID     string
	LeaseID       string
	JobID         string
	HardKill      bool
	ExactID       string
	ConfirmToken  string
	ProvidedToken string
}

type ReleasePreview struct {
	MachineID     string
	LeaseID       string
	ProviderJobID string
	Chats         []string
	Jobs          []string
	Confirmation  string
	ExternalOwned bool
}

type ReleaseResult struct {
	Action  string
	Preview ReleasePreview
}

func PreviewRelease(m Machine) ReleasePreview {
	token := "KILL-" + strings.TrimSpace(m.LeaseID)
	return ReleasePreview{
		MachineID:     m.ID,
		LeaseID:       m.LeaseID,
		ProviderJobID: m.ProviderJobID,
		Chats:         append([]string(nil), m.Chats...),
		Jobs:          append([]string(nil), m.Jobs...),
		Confirmation:  token,
		ExternalOwned: m.ExternalOwned,
	}
}

func DecideRelease(m Machine, in ReleaseInput) (ReleaseResult, error) {
	preview := PreviewRelease(m)
	if !in.HardKill {
		if len(m.Jobs) > 0 {
			return ReleaseResult{Action: "drain", Preview: preview}, nil
		}
		return ReleaseResult{Action: "release", Preview: preview}, nil
	}
	if m.ExternalOwned {
		return ReleaseResult{Action: "reject_external", Preview: preview}, nil
	}
	if strings.TrimSpace(in.ExactID) != m.LeaseID && strings.TrimSpace(in.ExactID) != m.ID && strings.TrimSpace(in.ExactID) != strings.TrimSpace(in.JobID) {
		return ReleaseResult{Action: "reject_exact_id", Preview: preview}, nil
	}
	if strings.TrimSpace(in.ProvidedToken) == "" || strings.TrimSpace(in.ProvidedToken) != strings.TrimSpace(in.ConfirmToken) {
		return ReleaseResult{Action: "reject_confirmation", Preview: preview}, nil
	}
	return ReleaseResult{Action: "kill_quarantine", Preview: preview}, nil
}

type UpgradeOperation string

const (
	UpgradeHelperReload       UpgradeOperation = "helper_reload"
	UpgradeHelperRestart      UpgradeOperation = "helper_restart"
	UpgradePendingReplacement UpgradeOperation = "pending_replacement"
	UpgradePrelistenCodex     UpgradeOperation = "prelisten_codex"
	UpgradeBeaconCodexTarget  UpgradeOperation = "beacon_codex_target"
)

type UpgradeInput struct {
	Operation               UpgradeOperation
	QueuedTeamsTurns        int
	RunningTeamsTurns       int
	ActiveBeaconSameTarget  int
	ActiveBeaconOtherTarget int
	QueuedSameCodexTarget   int
	ProtectedOutbox         int
	UnreconciledMarkers     int
	Force                   bool
}

type UpgradeBlocker struct {
	Kind           string
	ID             string
	ConversationID string
	MachineID      string
	Status         string
	Detail         string
}

func UpgradeBlockers(in UpgradeInput) []string {
	var blockers []string
	if in.RunningTeamsTurns > 0 {
		blockers = append(blockers, "running_teams_turn")
	}
	if in.ProtectedOutbox > 0 {
		blockers = append(blockers, "protected_outbox")
	}
	if in.UnreconciledMarkers > 0 {
		blockers = append(blockers, "unreconciled_beacon_marker")
	}
	switch in.Operation {
	case UpgradeHelperReload, UpgradeHelperRestart, UpgradePendingReplacement:
		if in.ActiveBeaconSameTarget+in.ActiveBeaconOtherTarget > 0 {
			blockers = append(blockers, "active_beacon_job")
		}
	case UpgradePrelistenCodex:
		if in.QueuedTeamsTurns > 0 {
			blockers = append(blockers, "queued_teams_turn")
		}
		if in.ActiveBeaconSameTarget+in.ActiveBeaconOtherTarget+in.QueuedSameCodexTarget > 0 {
			blockers = append(blockers, "codex_target_work")
		}
	case UpgradeBeaconCodexTarget:
		if in.ActiveBeaconSameTarget+in.QueuedSameCodexTarget > 0 {
			blockers = append(blockers, "codex_target_work")
		}
	default:
		blockers = append(blockers, "unknown_upgrade_operation")
	}
	if in.Force {
		filtered := blockers[:0]
		for _, blocker := range blockers {
			if blocker != "stale_owner_marker" {
				filtered = append(filtered, blocker)
			}
		}
		blockers = filtered
	}
	return blockers
}

func UpgradeBlockersForState(st State, op UpgradeOperation, codexTargetSignature string) []UpgradeBlocker {
	st.normalize()
	target := strings.TrimSpace(codexTargetSignature)
	var blockers []UpgradeBlocker
	for _, m := range st.Machines {
		state := strings.ToLower(strings.TrimSpace(m.State))
		if !machineStateMayBlockUpgrade(state, len(m.Jobs) > 0) {
			continue
		}
		detail := strings.TrimSpace(m.Profile)
		if strings.TrimSpace(m.ProviderJobID) != "" {
			if detail != "" {
				detail += " "
			}
			detail += "provider_job=" + strings.TrimSpace(m.ProviderJobID)
		}
		if len(m.Jobs) == 0 {
			blockers = append(blockers, UpgradeBlocker{
				Kind:      "beacon_marker",
				ID:        firstNonEmpty(m.LeaseID, m.ID),
				MachineID: m.ID,
				Status:    firstNonEmpty(m.State, "unknown"),
				Detail:    detail,
			})
			continue
		}
		for _, jobID := range m.Jobs {
			blockers = append(blockers, UpgradeBlocker{
				Kind:      "beacon_job",
				ID:        strings.TrimSpace(jobID),
				MachineID: m.ID,
				Status:    firstNonEmpty(m.State, "unknown"),
				Detail:    detail,
			})
		}
	}

	if op == UpgradePrelistenCodex || op == UpgradeBeaconCodexTarget {
		for _, conv := range st.Conversations {
			for _, queued := range conv.Queued {
				snap := queued.Snapshot
				if snap.Target != TargetBeacon {
					continue
				}
				if target != "" && strings.TrimSpace(snap.Signature) != target {
					continue
				}
				blockers = append(blockers, UpgradeBlocker{
					Kind:           "beacon_queued_turn",
					ID:             strings.TrimSpace(queued.ID),
					ConversationID: conv.ID,
					MachineID:      strings.TrimSpace(snap.MachineID),
					Status:         "queued",
					Detail:         strings.TrimSpace(snap.Profile),
				})
			}
		}
	}

	sort.Slice(blockers, func(i, j int) bool {
		if blockers[i].Kind != blockers[j].Kind {
			return blockers[i].Kind < blockers[j].Kind
		}
		if blockers[i].ConversationID != blockers[j].ConversationID {
			return blockers[i].ConversationID < blockers[j].ConversationID
		}
		if blockers[i].MachineID != blockers[j].MachineID {
			return blockers[i].MachineID < blockers[j].MachineID
		}
		return blockers[i].ID < blockers[j].ID
	})
	return blockers
}

func machineStateMayBlockUpgrade(state string, hasJobs bool) bool {
	switch state {
	case "", "idle", "accepting", "drained", "expired":
		return hasJobs
	case "draining":
		return hasJobs
	case "lost", "incompatible", "ambiguous", "needs_attention", "protocol_mismatch", "finalizing", "running", "starting", "claimed":
		return true
	default:
		return hasJobs
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

type ArtifactRef struct {
	Root                 string
	Path                 string
	DeclaredHash         string
	ActualHash           string
	Size                 int64
	Limit                int64
	OpenedNoFollow       bool
	FstatStable          bool
	HashFromOpenedFile   bool
	StagedFromOpenedFile bool
	HardlinkCount        int
	WorkerDeliveryField  bool
	Missing              bool
	UploadOK             bool
}

func ValidateArtifact(ref ArtifactRef) error {
	if ref.Missing {
		return fmt.Errorf("artifact missing")
	}
	if ref.WorkerDeliveryField {
		return fmt.Errorf("worker-provided delivery field is not allowed")
	}
	if !ref.OpenedNoFollow || !ref.FstatStable || !ref.HashFromOpenedFile || !ref.StagedFromOpenedFile {
		return fmt.Errorf("artifact was not safely staged from opened file")
	}
	if ref.HardlinkCount > 1 {
		return fmt.Errorf("artifact hardlinks are not allowed")
	}
	if ref.Size <= 0 || ref.Size > ref.Limit {
		return fmt.Errorf("artifact size out of range")
	}
	if strings.TrimSpace(ref.DeclaredHash) == "" || ref.DeclaredHash != ref.ActualHash {
		return fmt.Errorf("artifact hash mismatch")
	}
	root := filepath.Clean(ref.Root)
	path := filepath.Clean(ref.Path)
	if path == root || !strings.HasPrefix(path, root+string(filepath.Separator)) {
		return fmt.Errorf("artifact path outside root")
	}
	if !ref.UploadOK {
		return fmt.Errorf("artifact upload failed")
	}
	return nil
}

type TerminalResult struct {
	Action       string
	OutboxQueued bool
}

func AcceptTerminal(st *State, jobID string, envelope []byte, now time.Time) (TerminalResult, error) {
	if st == nil {
		return TerminalResult{}, fmt.Errorf("nil beacon state")
	}
	st.normalize()
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return TerminalResult{}, fmt.Errorf("job id is required")
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(envelope))
	if existing, ok := st.Terminals[jobID]; ok {
		if existing.EnvelopeHash != hash {
			return TerminalResult{Action: "quarantine"}, nil
		}
		if !existing.OutboxQueued {
			existing.OutboxQueued = true
			st.Terminals[jobID] = existing
			return TerminalResult{Action: "complete", OutboxQueued: true}, nil
		}
		return TerminalResult{Action: "complete", OutboxQueued: false}, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	st.Terminals[jobID] = TerminalRecord{JobID: jobID, EnvelopeHash: hash, OutboxQueued: true, AcceptedAt: now}
	return TerminalResult{Action: "complete", OutboxQueued: true}, nil
}
