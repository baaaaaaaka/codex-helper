package skills

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const MigrationStatusFilename = "skill-migration-status.json"

const (
	MigrationStatusDone          = "done"
	MigrationStatusFailed        = "failed"
	MigrationStatusSkipped       = "skipped"
	MigrationStatusMigrated      = "migrated"
	MigrationStatusRetargeted    = "retargeted"
	MigrationStatusBackedUp      = "backed_up"
	MigrationStatusLocalModified = "local_modified"
	MigrationStatusConflict      = "conflict"
	MigrationStatusDryRun        = "dry_run"
)

type MigrationOptions struct {
	DryRun          bool
	IncludeBuiltins bool
}

type MigrationReport struct {
	Status    string                  `json:"status"`
	StartedAt time.Time               `json:"started_at"`
	EndedAt   time.Time               `json:"ended_at"`
	OldRoot   string                  `json:"old_root"`
	NewRoot   string                  `json:"new_root"`
	DryRun    bool                    `json:"dry_run"`
	Changed   bool                    `json:"changed"`
	Failed    bool                    `json:"failed"`
	Results   []MigrationSourceResult `json:"results"`
}

type MigrationSourceResult struct {
	ID      string                 `json:"id"`
	Name    string                 `json:"name"`
	Kind    string                 `json:"kind"`
	Status  string                 `json:"status"`
	Message string                 `json:"message,omitempty"`
	Skills  []MigrationSkillResult `json:"skills,omitempty"`
}

type MigrationSkillResult struct {
	ExportName string `json:"export_name"`
	OldPath    string `json:"old_path,omitempty"`
	NewPath    string `json:"new_path,omitempty"`
	Status     string `json:"status"`
	Message    string `json:"message,omitempty"`
}

type migrationSkillCandidate struct {
	exportName string
	oldPath    string
	newPath    string
	manifest   exportManifest
}

func (m *Manager) MigrationStatusPath() string {
	return filepath.Join(m.ConfigDir, MigrationStatusFilename)
}

func (m *Manager) MigrateLegacySkills(ctx context.Context, opts MigrationOptions) (MigrationReport, error) {
	var report MigrationReport
	err := m.withOperationLock(ctx, func() error {
		var err error
		report, err = m.migrateLegacySkillsUnlocked(ctx, opts)
		return err
	})
	return report, err
}

func (m *Manager) migrateLegacySkillsUnlocked(ctx context.Context, opts MigrationOptions) (MigrationReport, error) {
	started := nowUTC()
	codexRoot, err := m.TargetRoot(TargetCodexHome)
	if err != nil {
		return MigrationReport{}, err
	}
	agentsRoot, err := m.TargetRoot(TargetAgents)
	if err != nil {
		return MigrationReport{}, err
	}
	report := MigrationReport{
		Status:    MigrationStatusDone,
		StartedAt: started,
		OldRoot:   codexRoot,
		NewRoot:   agentsRoot,
		DryRun:    opts.DryRun,
	}
	cfg, err := m.Store.LoadConfig()
	if err != nil {
		return report, err
	}
	st, err := m.Store.LoadState()
	if err != nil {
		return report, err
	}
	for _, source := range cfg.Sources {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		state, _ := sourceStateByID(st, source.ID)
		result, err := m.migrateSourceUnlocked(source, state, codexRoot, agentsRoot, opts)
		if err != nil {
			result.Status = MigrationStatusFailed
			result.Message = err.Error()
			report.Failed = true
		}
		if migrationResultChanged(result) {
			report.Changed = true
		}
		report.Results = append(report.Results, result)
	}
	if opts.IncludeBuiltins {
		result, err := m.migrateBuiltinUnlocked(codexRoot, agentsRoot, opts)
		if err != nil {
			result.Status = MigrationStatusFailed
			result.Message = err.Error()
			report.Failed = true
		}
		if result.Status != MigrationStatusSkipped {
			if migrationResultChanged(result) {
				report.Changed = true
			}
			report.Results = append(report.Results, result)
		}
	}
	report.EndedAt = nowUTC()
	if report.Failed {
		report.Status = MigrationStatusFailed
	}
	if opts.DryRun {
		report.Status = MigrationStatusDryRun
	}
	if err := writeJSONFile(m.MigrationStatusPath(), report); err != nil {
		return report, err
	}
	return report, nil
}

func (m *Manager) migrateSourceUnlocked(source Source, state SourceState, codexRoot, agentsRoot string, opts MigrationOptions) (MigrationSourceResult, error) {
	result := MigrationSourceResult{
		ID:     source.ID,
		Name:   source.Name,
		Kind:   "subscription",
		Status: MigrationStatusSkipped,
	}
	oldRoot := legacyRootForSource(source, codexRoot)
	needsRetarget := sourceNeedsLegacyMigration(source, oldRoot, codexRoot, agentsRoot)
	cleanupLegacyDuplicate := sourceUsesCurrentAgentsTarget(source, agentsRoot)
	if !needsRetarget && !cleanupLegacyDuplicate {
		return result, nil
	}
	candidates, err := migrationCandidatesForSource(oldRoot, agentsRoot, source, state)
	if err != nil {
		return result, err
	}
	if len(candidates) == 0 {
		if needsRetarget {
			result.Status = MigrationStatusRetargeted
			result.Message = "no managed legacy skill directories found; retargeted source for future syncs"
			if opts.DryRun {
				result.Status = MigrationStatusDryRun
				return result, nil
			}
			if err := m.retargetSource(source.ID, agentsRoot, nil); err != nil {
				return result, err
			}
		}
		return result, nil
	}
	for _, c := range candidates {
		if modified, err := exportHasLocalChanges(c.oldPath, c.manifest); err != nil {
			return result, err
		} else if modified {
			result.Status = MigrationStatusLocalModified
			result.Message = "legacy managed skill has local modifications; leaving source unchanged"
			result.Skills = append(result.Skills, MigrationSkillResult{
				ExportName: c.exportName,
				OldPath:    c.oldPath,
				NewPath:    c.newPath,
				Status:     MigrationStatusLocalModified,
			})
			return result, nil
		}
		status, message, err := migrationDestinationStatus(c.newPath, c.manifest)
		if err != nil {
			return result, err
		}
		if status == MigrationStatusConflict {
			result.Status = MigrationStatusConflict
			result.Message = message
			result.Skills = append(result.Skills, MigrationSkillResult{
				ExportName: c.exportName,
				OldPath:    c.oldPath,
				NewPath:    c.newPath,
				Status:     status,
				Message:    message,
			})
			return result, nil
		}
	}
	if opts.DryRun {
		result.Status = MigrationStatusDryRun
		for _, c := range candidates {
			result.Skills = append(result.Skills, MigrationSkillResult{
				ExportName: c.exportName,
				OldPath:    c.oldPath,
				NewPath:    c.newPath,
				Status:     MigrationStatusDryRun,
			})
		}
		return result, nil
	}
	for _, c := range candidates {
		if err := copyManagedSkillForMigration(c.oldPath, c.newPath, c.manifest); err != nil {
			return result, err
		}
	}
	installed := mergeInstalledSkillsForMigration(state.InstalledSkills, candidates)
	if err := m.retargetSource(source.ID, agentsRoot, installed); err != nil {
		return result, err
	}
	result.Status = MigrationStatusMigrated
	for _, c := range candidates {
		skillResult := MigrationSkillResult{
			ExportName: c.exportName,
			OldPath:    c.oldPath,
			NewPath:    c.newPath,
			Status:     MigrationStatusMigrated,
		}
		if backup, err := moveLegacySkillToBackup(oldRoot, c.oldPath); err != nil {
			skillResult.Status = MigrationStatusConflict
			skillResult.Message = "migrated but failed to hide old directory: " + err.Error()
			result.Status = MigrationStatusConflict
			result.Message = "one or more old directories remain visible"
		} else {
			skillResult.Message = "legacy copy moved to " + backup
		}
		result.Skills = append(result.Skills, skillResult)
	}
	return result, nil
}

func (m *Manager) migrateBuiltinUnlocked(codexRoot, agentsRoot string, opts MigrationOptions) (MigrationSourceResult, error) {
	source := Source{ID: builtinCxpSourceID, Name: BuiltinCxpSkillName}
	state := SourceState{ID: builtinCxpSourceID}
	result := MigrationSourceResult{
		ID:     builtinCxpSourceID,
		Name:   BuiltinCxpSkillName,
		Kind:   "builtin",
		Status: MigrationStatusSkipped,
	}
	candidates, err := migrationCandidatesForSource(codexRoot, agentsRoot, source, state)
	if err != nil {
		return result, err
	}
	if len(candidates) == 0 {
		return result, nil
	}
	for _, c := range candidates {
		if modified, err := exportHasLocalChanges(c.oldPath, c.manifest); err != nil {
			return result, err
		} else if modified {
			result.Status = MigrationStatusLocalModified
			result.Message = "legacy built-in skill has local modifications"
			result.Skills = append(result.Skills, MigrationSkillResult{ExportName: c.exportName, OldPath: c.oldPath, NewPath: c.newPath, Status: MigrationStatusLocalModified})
			return result, nil
		}
		status, message, err := migrationDestinationStatus(c.newPath, c.manifest)
		if err != nil {
			return result, err
		}
		if status == MigrationStatusConflict {
			result.Status = MigrationStatusConflict
			result.Message = message
			result.Skills = append(result.Skills, MigrationSkillResult{ExportName: c.exportName, OldPath: c.oldPath, NewPath: c.newPath, Status: status, Message: message})
			return result, nil
		}
	}
	if opts.DryRun {
		result.Status = MigrationStatusDryRun
		return result, nil
	}
	for _, c := range candidates {
		if err := copyManagedSkillForMigration(c.oldPath, c.newPath, c.manifest); err != nil {
			return result, err
		}
		if backup, err := moveLegacySkillToBackup(codexRoot, c.oldPath); err != nil {
			result.Status = MigrationStatusConflict
			result.Message = "migrated but failed to hide old built-in directory: " + err.Error()
			result.Skills = append(result.Skills, MigrationSkillResult{ExportName: c.exportName, OldPath: c.oldPath, NewPath: c.newPath, Status: MigrationStatusConflict, Message: err.Error()})
			return result, nil
		} else {
			result.Skills = append(result.Skills, MigrationSkillResult{ExportName: c.exportName, OldPath: c.oldPath, NewPath: c.newPath, Status: MigrationStatusMigrated, Message: "legacy copy moved to " + backup})
		}
	}
	result.Status = MigrationStatusMigrated
	return result, nil
}

func (m *Manager) retargetSource(sourceID, agentsRoot string, installed []InstalledSkill) error {
	return m.Store.Update(func(cfg *Config, st *State) error {
		for i := range cfg.Sources {
			if cfg.Sources[i].ID == sourceID {
				cfg.Sources[i].TargetKind = TargetAgents
				cfg.Sources[i].TargetRoot = agentsRoot
				cfg.Sources[i].UpdatedAt = nowUTC()
			}
		}
		state, ok := sourceStateByID(*st, sourceID)
		if !ok {
			state = SourceState{ID: sourceID, Status: StatusReady}
		}
		if len(installed) > 0 {
			state.InstalledSkills = installed
		} else {
			for i := range state.InstalledSkills {
				state.InstalledSkills[i].TargetPath = filepath.Join(agentsRoot, state.InstalledSkills[i].ExportName)
			}
		}
		if state.Status == "" || state.Status == StatusSyncing {
			state.Status = StatusReady
		}
		st.Sources = upsertState(st.Sources, state)
		return nil
	})
}

func migrationCandidatesForSource(oldRoot, newRoot string, source Source, state SourceState) ([]migrationSkillCandidate, error) {
	byExport := map[string]string{}
	stateExports := map[string]bool{}
	for _, skill := range state.InstalledSkills {
		exportName := strings.TrimSpace(skill.ExportName)
		if exportName == "" {
			continue
		}
		if _, err := safeMigrationExportName(exportManifest{ExportName: exportName}, filepath.Join(oldRoot, exportName)); err != nil {
			return nil, err
		}
		oldPath := strings.TrimSpace(skill.TargetPath)
		if oldPath == "" || !pathInsideRoot(oldRoot, oldPath) {
			oldPath = filepath.Join(oldRoot, exportName)
		}
		if !pathInsideRoot(oldRoot, oldPath) {
			return nil, fmt.Errorf("legacy skill state path %q escapes legacy root %q", oldPath, oldRoot)
		}
		key := strings.ToLower(exportName)
		stateExports[key] = true
		byExport[key] = oldPath
	}
	entries, err := os.ReadDir(oldRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read legacy skill root: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		dir := filepath.Join(oldRoot, entry.Name())
		manifest, ok, err := readExportManifest(filepath.Join(dir, manifestFilename))
		if err != nil {
			return nil, err
		}
		if ok && manifest.SourceID == source.ID {
			entryKey := strings.ToLower(entry.Name())
			if len(stateExports) > 0 && !stateExports[entryKey] {
				exportName, err := safeMigrationExportName(manifest, dir)
				if err != nil {
					continue
				}
				exportKey := strings.ToLower(exportName)
				if !stateExports[exportKey] {
					continue
				}
				byExport[exportKey] = dir
				continue
			}
			exportName, err := safeMigrationExportName(manifest, dir)
			if err != nil {
				return nil, err
			}
			exportKey := strings.ToLower(exportName)
			if len(stateExports) == 0 || stateExports[exportKey] || stateExports[entryKey] {
				byExport[exportKey] = dir
			}
		}
	}
	var candidates []migrationSkillCandidate
	for _, oldPath := range byExport {
		if !pathInsideRoot(oldRoot, oldPath) {
			return nil, fmt.Errorf("legacy skill path %q escapes legacy root %q", oldPath, oldRoot)
		}
		manifest, ok, err := readExportManifest(filepath.Join(oldPath, manifestFilename))
		if err != nil {
			return nil, err
		}
		if !ok || manifest.SourceID != source.ID {
			continue
		}
		exportName, err := safeMigrationExportName(manifest, oldPath)
		if err != nil {
			return nil, err
		}
		newPath, err := migrationDestinationPath(newRoot, exportName)
		if err != nil {
			return nil, err
		}
		manifest.ExportName = exportName
		candidates = append(candidates, migrationSkillCandidate{
			exportName: exportName,
			oldPath:    oldPath,
			newPath:    newPath,
			manifest:   manifest,
		})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].exportName < candidates[j].exportName })
	return candidates, nil
}

func safeMigrationExportName(manifest exportManifest, oldPath string) (string, error) {
	exportName := strings.TrimSpace(firstNonEmpty(manifest.ExportName, filepath.Base(oldPath)))
	if exportName == "" || exportName == "." || exportName == ".." || filepath.IsAbs(exportName) ||
		strings.ContainsAny(exportName, `/\`) || filepath.Clean(exportName) != exportName {
		return "", fmt.Errorf("legacy skill %s has unsafe export name %q", oldPath, exportName)
	}
	return exportName, nil
}

func migrationDestinationPath(root, exportName string) (string, error) {
	dest := filepath.Join(root, exportName)
	if sameSkillPath(dest, root) || !pathInsideRoot(root, dest) {
		return "", fmt.Errorf("legacy skill export name %q escapes agents skills root", exportName)
	}
	return dest, nil
}

func migrationDestinationStatus(dest string, oldManifest exportManifest) (string, string, error) {
	manifest, ok, err := readExportManifest(filepath.Join(dest, manifestFilename))
	if err != nil {
		return "", "", err
	}
	if !ok {
		if _, err := os.Stat(dest); err == nil {
			return MigrationStatusConflict, "destination exists but is not managed by cxp skills", nil
		} else if err != nil && !os.IsNotExist(err) {
			return "", "", fmt.Errorf("stat destination %s: %w", dest, err)
		}
		return MigrationStatusMigrated, "", nil
	}
	if manifest.SourceID != oldManifest.SourceID {
		return MigrationStatusConflict, "destination is managed by another source", nil
	}
	if modified, err := exportHasLocalChanges(dest, manifest); err != nil {
		return "", "", err
	} else if modified {
		return MigrationStatusConflict, "destination has local modifications", nil
	}
	if !manifestsEquivalentForMigration(manifest, oldManifest) {
		return MigrationStatusConflict, "destination has a different clean managed version", nil
	}
	return MigrationStatusMigrated, "", nil
}

func copyManagedSkillForMigration(oldPath, newPath string, manifest exportManifest) error {
	if existing, ok, err := readExportManifest(filepath.Join(newPath, manifestFilename)); err != nil {
		return err
	} else if ok && manifestsEquivalentForMigration(existing, manifest) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(newPath), 0o700); err != nil {
		return fmt.Errorf("create agents skill root: %w", err)
	}
	staging := filepath.Join(filepath.Dir(newPath), ".cxp-migrate-staging-"+filepath.Base(newPath)+"-"+time.Now().UTC().Format("20060102150405"))
	if err := os.RemoveAll(staging); err != nil {
		return err
	}
	if err := copyDirForMigration(oldPath, staging); err != nil {
		_ = os.RemoveAll(staging)
		return err
	}
	if changes, err := localChangesForManifest(staging, manifest); err != nil {
		_ = os.RemoveAll(staging)
		return err
	} else if len(changes) > 0 {
		_ = os.RemoveAll(staging)
		return fmt.Errorf("copied skill %s did not match manifest", oldPath)
	}
	if err := renameSkillPublishPath(staging, newPath); err != nil {
		_ = os.RemoveAll(staging)
		return fmt.Errorf("publish migrated skill %s: %w", filepath.Base(newPath), err)
	}
	return nil
}

func copyDirForMigration(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			return os.MkdirAll(target, info.Mode().Perm())
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			_ = in.Close()
			return err
		}
		out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			_ = in.Close()
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = in.Close()
			_ = out.Close()
			return err
		}
		if err := in.Close(); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
}

func moveLegacySkillToBackup(oldRoot, oldPath string) (string, error) {
	manifest, ok, err := readExportManifest(filepath.Join(oldPath, manifestFilename))
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("legacy skill %s is missing managed manifest", oldPath)
	}
	if modified, err := exportHasLocalChanges(oldPath, manifest); err != nil {
		return "", err
	} else if modified {
		return "", fmt.Errorf("legacy skill %s has local modifications", oldPath)
	}
	backupRoot := filepath.Join(oldRoot, ".cxp-migrated-backups", time.Now().UTC().Format("20060102150405"))
	if err := os.MkdirAll(backupRoot, 0o700); err != nil {
		return "", err
	}
	backup := filepath.Join(backupRoot, filepath.Base(oldPath))
	if err := renameSkillPublishPath(oldPath, backup); err != nil {
		return "", err
	}
	return backup, nil
}

func manifestsEquivalentForMigration(a, b exportManifest) bool {
	a.ExportedAt = time.Time{}
	b.ExportedAt = time.Time{}
	adata, _ := json.Marshal(a)
	bdata, _ := json.Marshal(b)
	return string(adata) == string(bdata)
}

func installedSkillFromManifest(manifest exportManifest, targetPath string) InstalledSkill {
	return InstalledSkill{
		Name:       manifest.SkillName,
		ExportName: manifest.ExportName,
		SourcePath: manifest.SourcePath,
		TargetPath: targetPath,
		Files:      manifest.Files,
	}
}

func mergeInstalledSkillsForMigration(existing []InstalledSkill, candidates []migrationSkillCandidate) []InstalledSkill {
	merged := append([]InstalledSkill(nil), existing...)
	byExport := make(map[string]int, len(merged))
	for i, skill := range merged {
		exportName := strings.ToLower(strings.TrimSpace(skill.ExportName))
		if exportName == "" {
			continue
		}
		byExport[exportName] = i
	}
	for _, c := range candidates {
		installed := installedSkillFromManifest(c.manifest, c.newPath)
		exportName := strings.ToLower(strings.TrimSpace(installed.ExportName))
		if i, ok := byExport[exportName]; ok {
			merged[i] = installed
			continue
		}
		byExport[exportName] = len(merged)
		merged = append(merged, installed)
	}
	return merged
}

func migrationResultChanged(result MigrationSourceResult) bool {
	switch result.Status {
	case MigrationStatusMigrated, MigrationStatusRetargeted, MigrationStatusBackedUp, MigrationStatusDryRun:
		return true
	default:
		return false
	}
}

func legacyRootForSource(source Source, codexRoot string) string {
	targetKind := strings.TrimSpace(source.TargetKind)
	targetRoot := strings.TrimSpace(source.TargetRoot)
	if targetRoot != "" && targetKind == TargetCodexHome {
		return filepath.Clean(source.TargetRoot)
	}
	if targetRoot != "" && targetKind == "" && sameSkillPath(targetRoot, codexRoot) {
		return filepath.Clean(targetRoot)
	}
	return codexRoot
}

func sourceNeedsLegacyMigration(source Source, _ string, codexRoot, agentsRoot string) bool {
	targetKind := strings.TrimSpace(source.TargetKind)
	targetRoot := filepath.Clean(strings.TrimSpace(source.TargetRoot))
	if targetKind == "" {
		return strings.TrimSpace(source.TargetRoot) == "" || sameSkillPath(targetRoot, codexRoot)
	}
	if targetKind == TargetCodexHome {
		return true
	}
	if targetKind == TargetAgents {
		return !sameSkillPath(targetRoot, agentsRoot)
	}
	return false
}

func sourceUsesCurrentAgentsTarget(source Source, agentsRoot string) bool {
	return strings.TrimSpace(source.TargetKind) == TargetAgents && sameSkillPath(source.TargetRoot, agentsRoot)
}

func pathInsideRoot(root, p string) bool {
	root = filepath.Clean(root)
	p = filepath.Clean(p)
	if sameSkillPath(root, p) {
		return true
	}
	rel, err := filepath.Rel(root, p)
	return err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".."
}

func sameSkillPath(a, b string) bool {
	a = filepath.Clean(strings.TrimSpace(a))
	b = filepath.Clean(strings.TrimSpace(b))
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}
