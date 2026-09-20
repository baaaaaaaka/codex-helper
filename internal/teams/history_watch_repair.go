package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// historyWatchWakeCandidate is the only input that the local missing-history
// repair is allowed to turn into durable state. It deliberately contains no
// session, import, frontier, or outbox fields: a wake repair only makes the
// existing source scanner consider an explicitly named file.
type historyWatchWakeCandidate struct {
	Path    string
	ModTime time.Time
}

type historyWatchWakeReport struct {
	Added    []string
	Existing []string
}

// validateHistoryWatchWakeCandidates validates an operator-supplied, bounded
// path list before the SQLite transaction starts. Requiring regular files
// below the resolved Codex sessions directory prevents a typo, symlink, or
// arbitrary path from becoming a durable history source.
func validateHistoryWatchWakeCandidates(codexHome string, paths []string) ([]historyWatchWakeCandidate, error) {
	root, err := codexhistory.ResolveCodexDir(codexHome)
	if err != nil {
		return nil, fmt.Errorf("resolve Codex history directory: %w", err)
	}
	sessionsRoot := cleanComparablePath(filepath.Join(root, "sessions"))
	if sessionsRoot == "" {
		return nil, fmt.Errorf("resolved Codex sessions directory is empty")
	}
	prefix := sessionsRoot + string(filepath.Separator)
	seen := make(map[string]struct{}, len(paths))
	candidates := make([]historyWatchWakeCandidate, 0, len(paths))
	for _, raw := range paths {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, fmt.Errorf("history wake path is empty")
		}
		if !filepath.IsAbs(raw) {
			return nil, fmt.Errorf("history wake path is not absolute: %q", raw)
		}
		path := cleanComparablePath(raw)
		if path == "" || (path != sessionsRoot && !strings.HasPrefix(path, prefix)) {
			return nil, fmt.Errorf("history wake path is outside Codex sessions: %q", raw)
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("stat history wake path %q: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("history wake path is not a regular non-symlink file: %q", path)
		}
		candidates = append(candidates, historyWatchWakeCandidate{Path: path, ModTime: info.ModTime()})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Path < candidates[j].Path })
	return candidates, nil
}

// seedHistoryWatchWakeCheckpoints adds only absent checkpoints for explicitly
// validated Codex JSONL files. Existing checkpoints are never rewritten,
// even if their cursor is blocked or stale. The operation is idempotent and
// does not touch sessions, import checkpoints, frontiers, outbox rows, or
// Graph state; the normal history scanner performs all later work. It is valid
// only after the normal history-watch baseline exists: an uninitialized store
// must perform its complete baseline instead of treating an operator-supplied
// subset as the complete source set.
func seedHistoryWatchWakeCheckpoints(ctx context.Context, store *teamstore.Store, codexHome string, paths []string, now time.Time) (historyWatchWakeReport, error) {
	if store == nil {
		return historyWatchWakeReport{}, fmt.Errorf("history wake repair requires a store")
	}
	candidates, err := validateHistoryWatchWakeCandidates(codexHome, paths)
	if err != nil {
		return historyWatchWakeReport{}, err
	}
	if len(candidates) == 0 {
		return historyWatchWakeReport{}, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	report := historyWatchWakeReport{}
	err = store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		if ready == nil || ready.IsZero() {
			return fmt.Errorf("history-watch wake repair requires an initialized history-watch baseline")
		}
		byPath := make(map[string]string, len(history))
		for id, checkpoint := range history {
			path := cleanComparablePath(checkpoint.Path)
			if path == "" {
				continue
			}
			if previous, ok := byPath[path]; ok && previous != id {
				return fmt.Errorf("history-watch contains duplicate path %q under %q and %q", path, previous, id)
			}
			byPath[path] = id
		}
		for _, candidate := range candidates {
			id := historyWatchCheckpointID(candidate.Path)
			if existing, ok := history[id]; ok {
				if cleanComparablePath(existing.Path) != candidate.Path {
					return fmt.Errorf("history-watch checkpoint id collision for %q", candidate.Path)
				}
				report.Existing = append(report.Existing, candidate.Path)
				continue
			}
			if existingID, ok := byPath[candidate.Path]; ok && existingID != id {
				return fmt.Errorf("history-watch path %q already exists under unexpected id %q", candidate.Path, existingID)
			}
			history[id] = teamstore.HistoryWatchCheckpoint{
				ID:        id,
				Path:      candidate.Path,
				Size:      0,
				ModTime:   candidate.ModTime,
				Offset:    0,
				Line:      0,
				UpdatedAt: now,
			}
			byPath[candidate.Path] = id
			report.Added = append(report.Added, candidate.Path)
		}
		return nil
	})
	if err != nil {
		return historyWatchWakeReport{}, err
	}
	sort.Strings(report.Added)
	sort.Strings(report.Existing)
	return report, nil
}
