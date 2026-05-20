package skills

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	Store     *Store
	ConfigDir string
	CacheDir  string
	CodexDir  string
	HomeDir   string
	Git       GitRunner
	Out       io.Writer
}

type ManagerOptions struct {
	ConfigDir string
	CacheDir  string
	CodexDir  string
	HomeDir   string
	Git       GitRunner
	Out       io.Writer
}

func NewManager(opts ManagerOptions) (*Manager, error) {
	configDir := filepath.Clean(strings.TrimSpace(opts.ConfigDir))
	if configDir == "" || configDir == "." {
		return nil, fmt.Errorf("empty skill subscription config dir")
	}
	cacheDir := filepath.Clean(strings.TrimSpace(opts.CacheDir))
	if cacheDir == "" || cacheDir == "." {
		base, err := os.UserCacheDir()
		if err != nil {
			return nil, fmt.Errorf("get user cache dir: %w", err)
		}
		cacheDir = filepath.Join(base, "codex-proxy", "skill-subscriptions")
	}
	store, err := NewStore(configDir)
	if err != nil {
		return nil, err
	}
	home := strings.TrimSpace(opts.HomeDir)
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	codexDir := strings.TrimSpace(opts.CodexDir)
	if codexDir == "" && home != "" {
		codexDir = filepath.Join(home, ".codex")
	}
	if home != "" {
		home = filepath.Clean(home)
	}
	if codexDir != "" {
		codexDir = filepath.Clean(codexDir)
	}
	git := opts.Git
	if git == nil {
		git = ExecGitRunner{Timeout: 2 * time.Minute}
	}
	out := opts.Out
	if out == nil {
		out = io.Discard
	}
	return &Manager{
		Store:     store,
		ConfigDir: configDir,
		CacheDir:  cacheDir,
		CodexDir:  codexDir,
		HomeDir:   home,
		Git:       git,
		Out:       out,
	}, nil
}

func (m *Manager) Add(ctx context.Context, rawURL string, opts AddOptions) (Source, SyncResult, error) {
	known := []string(nil)
	if strings.Contains(rawURL, "github.com") || strings.Contains(rawURL, "gitlab") {
		rough, _ := ParseURL(rawURL, URLParseOptions{Name: opts.Name, Ref: opts.Ref, Path: opts.Path})
		if rough.RemoteURL != "" {
			known = knownRemoteRefs(ctx, m.Git, rough.RemoteURL)
		}
	}
	info, err := ParseURL(rawURL, URLParseOptions{Name: opts.Name, Ref: opts.Ref, Path: opts.Path, KnownRefs: known})
	if err != nil {
		return Source{}, SyncResult{}, err
	}
	targetKind := strings.TrimSpace(opts.TargetKind)
	if targetKind == "" {
		targetKind = TargetCodexHome
	}
	autoSync := true
	if opts.AutoSync != nil {
		autoSync = *opts.AutoSync
	}
	targetRoot, err := m.TargetRoot(targetKind)
	if err != nil {
		return Source{}, SyncResult{}, err
	}
	now := nowUTC()
	source := Source{
		ID:         stableID(info.Name, info.RemoteURL, info.Path),
		Name:       info.Name,
		URL:        strings.TrimSpace(rawURL),
		RemoteURL:  info.RemoteURL,
		Provider:   info.Provider,
		Ref:        info.Ref,
		Path:       info.Path,
		TargetKind: targetKind,
		TargetRoot: targetRoot,
		AutoSync:   autoSync,
		AddedAt:    now,
		UpdatedAt:  now,
	}
	var existingAdded time.Time
	if cfg, err := m.Store.LoadConfig(); err == nil {
		for _, existing := range cfg.Sources {
			if existing.ID == source.ID {
				existingAdded = existing.AddedAt
				break
			}
		}
	}
	if !existingAdded.IsZero() {
		source.AddedAt = existingAdded
	}
	if err := m.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = upsertSource(cfg.Sources, source)
		st.Sources = upsertState(st.Sources, SourceState{ID: source.ID, Status: StatusSyncing})
		return nil
	}); err != nil {
		return Source{}, SyncResult{}, err
	}
	result := m.syncOne(ctx, source, false)
	if result.Error != nil {
		return source, result, result.Error
	}
	return source, result, nil
}

func (m *Manager) Remove(ctx context.Context, idOrName string) (Source, error) {
	var removed *Source
	if err := m.Store.Update(func(cfg *Config, st *State) error {
		var next []Source
		next, removed = removeSource(cfg.Sources, idOrName)
		if removed == nil {
			return fmt.Errorf("skill source %q not found", idOrName)
		}
		if err := removeManagedSkills(removed.TargetRoot, removed.ID); err != nil {
			return err
		}
		cfg.Sources = next
		st.Sources = removeState(st.Sources, removed.ID)
		return nil
	}); err != nil {
		return Source{}, err
	}
	if removed == nil {
		return Source{}, fmt.Errorf("skill source %q not found", idOrName)
	}
	_ = ctx
	return *removed, nil
}

func (m *Manager) List(ctx context.Context) ([]StatusEntry, error) {
	cfg, err := m.Store.LoadConfig()
	if err != nil {
		return nil, err
	}
	st, err := m.Store.LoadState()
	if err != nil {
		return nil, err
	}
	entries := make([]StatusEntry, 0, len(cfg.Sources))
	for _, source := range cfg.Sources {
		source = normalizeSourceRemoteForGit(source)
		state, ok := sourceStateByID(st, source.ID)
		if !ok {
			state = SourceState{ID: source.ID, Status: StatusReady}
		}
		if changes, err := m.LocalChangesForSource(source, state); err == nil && len(changes) > 0 {
			state.Status = StatusModified
		}
		entries = append(entries, StatusEntry{Source: source, State: state})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Source.Name < entries[j].Source.Name })
	_ = ctx
	return entries, nil
}

func (m *Manager) Sync(ctx context.Context, opts SyncOptions) ([]SyncResult, error) {
	cfg, err := m.Store.LoadConfig()
	if err != nil {
		return nil, err
	}
	var selected []Source
	for _, source := range cfg.Sources {
		if opts.All || strings.TrimSpace(opts.Name) == "" || sourceMatches(source, opts.Name) {
			selected = append(selected, normalizeSourceRemoteForGit(source))
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("skill source %q not found", opts.Name)
	}
	results := make([]SyncResult, len(selected))
	var wg sync.WaitGroup
	for i, source := range selected {
		wg.Add(1)
		go func(i int, source Source) {
			defer wg.Done()
			results[i] = m.syncOne(ctx, source, false)
		}(i, source)
	}
	wg.Wait()
	var failures []string
	for _, result := range results {
		if result.Error != nil {
			failures = append(failures, result.Source.Name+": "+result.Error.Error())
		}
	}
	if len(failures) > 0 {
		return results, errors.New(strings.Join(failures, "\n"))
	}
	return results, nil
}

func (m *Manager) syncOne(ctx context.Context, source Source, auto bool) SyncResult {
	result := SyncResult{Source: source}
	_ = m.Store.UpdateState(func(st *State) error {
		current, _ := sourceStateByID(*st, source.ID)
		current.ID = source.ID
		current.Status = StatusSyncing
		current.LastError = ""
		st.Sources = upsertState(st.Sources, current)
		return nil
	})
	if st, err := m.Store.LoadState(); err == nil {
		if current, ok := sourceStateByID(st, source.ID); ok {
			changes, err := m.LocalChangesForSource(source, current)
			if err != nil {
				return m.recordSyncFailure(source, result, err, auto)
			}
			if len(changes) > 0 {
				return m.recordSyncFailure(source, result, fmt.Errorf("local modifications detected in %s; run `cxp skills push` before syncing", source.Name), auto)
			}
		}
	}
	mirror, err := ensureBareMirror(ctx, m.Git, m.CacheDir, source)
	if err != nil {
		return m.recordSyncFailure(source, result, err, auto)
	}
	commit, err := fetchSource(ctx, m.Git, mirror, source)
	if err != nil {
		return m.recordSyncFailure(source, result, err, auto)
	}
	trees, err := scanSkillsFromGitTree(ctx, m.Git, mirror, source, commit, filepath.Join(m.CacheDir, "tools"))
	if err != nil {
		return m.recordSyncFailure(source, result, err, auto)
	}
	installed, err := publishSkills(source.TargetRoot, source, commit, trees)
	if err != nil {
		return m.recordSyncFailure(source, result, err, auto)
	}
	state := SourceState{
		ID:              source.ID,
		Status:          StatusReady,
		LastSyncAt:      nowUTC(),
		LastCommit:      commit,
		InstalledSkills: installed,
	}
	if auto {
		state.LastAutoSyncDay = todayString()
	}
	if err := m.Store.UpdateState(func(st *State) error {
		previous, ok := sourceStateByID(*st, source.ID)
		if ok && state.LastAutoSyncDay == "" {
			state.LastAutoSyncDay = previous.LastAutoSyncDay
		}
		st.Sources = upsertState(st.Sources, state)
		return nil
	}); err != nil {
		result.Error = err
		return result
	}
	result.Commit = commit
	result.Installed = installed
	result.State = state
	return result
}

func (m *Manager) recordSyncFailure(source Source, result SyncResult, err error, auto bool) SyncResult {
	status := StatusSyncFailed
	if strings.Contains(strings.ToLower(err.Error()), "authentication hint") {
		status = StatusAuthRequired
	}
	if strings.Contains(strings.ToLower(err.Error()), "skill.md") || strings.Contains(strings.ToLower(err.Error()), "invalid") {
		status = StatusInvalid
	}
	if strings.Contains(strings.ToLower(err.Error()), "local modifications") {
		status = StatusModified
	}
	state := SourceState{
		ID:         source.ID,
		Status:     status,
		LastError:  err.Error(),
		LastSyncAt: time.Time{},
	}
	if auto {
		state.LastAutoSyncDay = todayString()
	}
	_ = m.Store.UpdateState(func(st *State) error {
		previous, ok := sourceStateByID(*st, source.ID)
		if ok {
			state.LastCommit = previous.LastCommit
			state.InstalledSkills = previous.InstalledSkills
			if state.LastAutoSyncDay == "" {
				state.LastAutoSyncDay = previous.LastAutoSyncDay
			}
		}
		st.Sources = upsertState(st.Sources, state)
		return nil
	})
	result.State = state
	result.Error = err
	return result
}

func (m *Manager) TargetRoot(kind string) (string, error) {
	switch strings.TrimSpace(kind) {
	case "", TargetCodexHome:
		if strings.TrimSpace(m.CodexDir) == "" {
			return "", fmt.Errorf("cannot resolve Codex home; pass --codex-dir")
		}
		return filepath.Join(m.CodexDir, "skills"), nil
	case TargetAgents:
		if strings.TrimSpace(m.HomeDir) == "" {
			return "", fmt.Errorf("cannot resolve user home for ~/.agents/skills")
		}
		return filepath.Join(m.HomeDir, ".agents", "skills"), nil
	default:
		return "", fmt.Errorf("unknown skill target %q", kind)
	}
}

func (m *Manager) LocalChanges(ctx context.Context, name string) ([]LocalChange, error) {
	entries, err := m.List(ctx)
	if err != nil {
		return nil, err
	}
	var changes []LocalChange
	for _, entry := range entries {
		if strings.TrimSpace(name) != "" && !sourceMatches(entry.Source, name) {
			continue
		}
		sourceChanges, err := m.LocalChangesForSource(entry.Source, entry.State)
		if err != nil {
			return nil, err
		}
		changes = append(changes, sourceChanges...)
	}
	if strings.TrimSpace(name) != "" && len(changes) == 0 {
		found := false
		for _, entry := range entries {
			if sourceMatches(entry.Source, name) {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("skill source %q not found", name)
		}
	}
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].Source.Name != changes[j].Source.Name {
			return changes[i].Source.Name < changes[j].Source.Name
		}
		return changes[i].RelPath < changes[j].RelPath
	})
	return changes, nil
}

func (m *Manager) LocalChangesForSource(source Source, state SourceState) ([]LocalChange, error) {
	var all []LocalChange
	for _, skill := range state.InstalledSkills {
		manifest, ok, err := readExportManifest(filepath.Join(skill.TargetPath, manifestFilename))
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		changes, err := localChangesForManifest(skill.TargetPath, manifest)
		if err != nil {
			return nil, err
		}
		for i := range changes {
			changes[i].Source = source
			changes[i].Skill = skill
			changes[i].SourcePath = pathJoinSlash(skill.SourcePath, changes[i].RelPath)
			changes[i].Commit = manifest.Commit
		}
		all = append(all, changes...)
	}
	return all, nil
}

func (m *Manager) StartDailyAutoSync(ctx context.Context) {
	cfg, err := m.Store.LoadConfig()
	if err != nil {
		return
	}
	st, err := m.Store.LoadState()
	if err != nil {
		return
	}
	today := todayString()
	var selected []Source
	for _, source := range cfg.Sources {
		if !source.AutoSync {
			continue
		}
		source = normalizeSourceRemoteForGit(source)
		state, _ := sourceStateByID(st, source.ID)
		if state.LastAutoSyncDay == today {
			continue
		}
		selected = append(selected, source)
	}
	if len(selected) == 0 {
		return
	}
	go func() {
		var wg sync.WaitGroup
		for _, source := range selected {
			source := source
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = m.syncOne(ctx, source, true)
			}()
		}
		wg.Wait()
	}()
}

func todayString() string {
	return time.Now().Local().Format("2006-01-02")
}

func pathJoinSlash(base, rel string) string {
	if strings.TrimSpace(base) == "" {
		return filepath.ToSlash(rel)
	}
	return strings.Trim(strings.ReplaceAll(base, "\\", "/"), "/") + "/" + strings.Trim(strings.ReplaceAll(rel, "\\", "/"), "/")
}
