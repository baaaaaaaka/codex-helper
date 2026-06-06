package skills

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMigrateLegacySkillsMovesCleanCodexHomeSubscriptionToAgents(t *testing.T) {
	repo := initSkillRepo(t)
	mgr := newTestManager(t)
	ctx := context.Background()

	source, result, err := mgr.Add(ctx, repo, AddOptions{Name: "acme", Ref: "HEAD", TargetKind: TargetCodexHome})
	if err != nil {
		t.Fatalf("add legacy source: %v", err)
	}
	if source.TargetKind != TargetCodexHome {
		t.Fatalf("source target = %q, want codex-home", source.TargetKind)
	}
	oldPath := result.Installed[0].TargetPath
	if _, err := os.Stat(filepath.Join(oldPath, "SKILL.md")); err != nil {
		t.Fatalf("old installed skill missing: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: true})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if report.Status != MigrationStatusDone || !report.Changed {
		t.Fatalf("report = %#v, want changed done", report)
	}
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	newPath := filepath.Join(agentsRoot, "acme__review")
	if got := readFile(t, filepath.Join(newPath, "SKILL.md")); got == "" {
		t.Fatal("migrated SKILL.md empty")
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old direct skill path still visible: %v", err)
	}
	entries, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries len = %d, want 1", len(entries))
	}
	if entries[0].Source.TargetKind != TargetAgents || entries[0].Source.TargetRoot != agentsRoot {
		t.Fatalf("source target = %q %q, want agents %q", entries[0].Source.TargetKind, entries[0].Source.TargetRoot, agentsRoot)
	}
	if entries[0].State.InstalledSkills[0].TargetPath != newPath {
		t.Fatalf("state target path = %q, want %q", entries[0].State.InstalledSkills[0].TargetPath, newPath)
	}
}

func TestMigrateLegacySkillsMovesHandwrittenOldShapeSubscription(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	oldRoot, err := mgr.TargetRoot(TargetCodexHome)
	if err != nil {
		t.Fatalf("old root: %v", err)
	}
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	source := Source{
		ID:        "old-source-1",
		Name:      "oldshape",
		URL:       "https://example.invalid/old/skills.git",
		RemoteURL: "https://example.invalid/old/skills.git",
		Ref:       "HEAD",
		AutoSync:  true,
		AddedAt:   nowUTC(),
		UpdatedAt: nowUTC(),
	}
	oldPath := filepath.Join(oldRoot, "oldshape__review")
	body := []byte("---\nname: review\ndescription: Old shape\n---\nbody\n")
	writeFile(t, filepath.Join(oldPath, "SKILL.md"), string(body), 0o644)
	sum := sha256.Sum256(body)
	manifest := exportManifest{
		Version:    1,
		SourceID:   source.ID,
		SourceName: source.Name,
		RemoteURL:  source.RemoteURL,
		Ref:        source.Ref,
		Commit:     "commit-old",
		SkillName:  "review",
		SourcePath: "skills/review",
		ExportName: "oldshape__review",
		ExportedAt: nowUTC(),
		Files: []FileManifest{{
			RelPath: "SKILL.md",
			SHA256:  hex.EncodeToString(sum[:]),
			Size:    int64(len(body)),
			Mode:    0o644,
		}},
	}
	if err := writeManifest(filepath.Join(oldPath, manifestFilename), manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = append(cfg.Sources, source)
		st.Sources = append(st.Sources, SourceState{
			ID:         source.ID,
			Status:     StatusReady,
			LastCommit: "commit-old",
			InstalledSkills: []InstalledSkill{{
				Name:       "review",
				ExportName: "oldshape__review",
				SourcePath: "skills/review",
				TargetPath: oldPath,
				Files:      manifest.Files,
			}},
		})
		return nil
	}); err != nil {
		t.Fatalf("seed old-shape store: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if report.Status != MigrationStatusDone || !report.Changed {
		t.Fatalf("report = %#v, want changed done", report)
	}
	if _, err := os.Stat(filepath.Join(agentsRoot, "oldshape__review", "SKILL.md")); err != nil {
		t.Fatalf("agents skill missing: %v", err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("legacy direct skill still visible: %v", err)
	}
}

func TestMigrateLegacySkillsLeavesLocalModifiedSubscriptionUntouched(t *testing.T) {
	repo := initSkillRepo(t)
	mgr := newTestManager(t)
	ctx := context.Background()

	source, result, err := mgr.Add(ctx, repo, AddOptions{Name: "acme", Ref: "HEAD", TargetKind: TargetCodexHome})
	if err != nil {
		t.Fatalf("add legacy source: %v", err)
	}
	oldPath := result.Installed[0].TargetPath
	writeFile(t, filepath.Join(oldPath, "SKILL.md"), "---\nname: review\ndescription: Local edit\n---\nlocal\n", 0o644)

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if len(report.Results) != 1 || report.Results[0].Status != MigrationStatusLocalModified {
		t.Fatalf("report = %#v, want local_modified", report)
	}
	if _, err := os.Stat(filepath.Join(oldPath, "SKILL.md")); err != nil {
		t.Fatalf("old modified skill missing: %v", err)
	}
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(agentsRoot, "acme__review")); !os.IsNotExist(err) {
		t.Fatalf("agents skill should not exist after local-modified skip: %v", err)
	}
	cfg, err := mgr.Store.LoadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Sources) != 1 || cfg.Sources[0].ID != source.ID || cfg.Sources[0].TargetKind != TargetCodexHome {
		t.Fatalf("config source = %#v, want legacy target unchanged", cfg.Sources)
	}
}

func TestMigrateLegacySkillsRejectsUnsafeLegacyExportName(t *testing.T) {
	repo := initSkillRepo(t)
	mgr := newTestManager(t)
	ctx := context.Background()

	_, result, err := mgr.Add(ctx, repo, AddOptions{Name: "acme", Ref: "HEAD", TargetKind: TargetCodexHome})
	if err != nil {
		t.Fatalf("add legacy source: %v", err)
	}
	oldPath := result.Installed[0].TargetPath
	manifestPath := filepath.Join(oldPath, manifestFilename)
	manifest, ok, err := readExportManifest(manifestPath)
	if err != nil || !ok {
		t.Fatalf("read manifest ok=%v err=%v", ok, err)
	}
	manifest.ExportName = "../escaped"
	if err := writeJSONFile(manifestPath, manifest); err != nil {
		t.Fatalf("tamper manifest: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if report.Status != MigrationStatusFailed || !report.Failed {
		t.Fatalf("report = %#v, want failed", report)
	}
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(agentsRoot, "..", "escaped")); !os.IsNotExist(err) {
		t.Fatalf("unsafe escaped destination exists or stat failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(oldPath, "SKILL.md")); err != nil {
		t.Fatalf("legacy skill should remain untouched: %v", err)
	}
}

func TestMigrateLegacySkillsRejectsUnsafeStateExportName(t *testing.T) {
	repo := initSkillRepo(t)
	mgr := newTestManager(t)
	ctx := context.Background()

	source, result, err := mgr.Add(ctx, repo, AddOptions{Name: "acme", Ref: "HEAD", TargetKind: TargetCodexHome})
	if err != nil {
		t.Fatalf("add legacy source: %v", err)
	}
	if len(result.Installed) != 1 {
		t.Fatalf("installed len = %d, want 1", len(result.Installed))
	}
	if err := mgr.Store.UpdateState(func(st *State) error {
		state, ok := sourceStateByID(*st, source.ID)
		if !ok || len(state.InstalledSkills) != 1 {
			t.Fatalf("state = %#v, want one installed skill", st.Sources)
		}
		state.InstalledSkills[0].ExportName = "../escaped"
		state.InstalledSkills[0].TargetPath = ""
		st.Sources = upsertState(st.Sources, state)
		return nil
	}); err != nil {
		t.Fatalf("tamper state: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if report.Status != MigrationStatusFailed || !report.Failed {
		t.Fatalf("report = %#v, want failed", report)
	}
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(agentsRoot, "..", "escaped")); !os.IsNotExist(err) {
		t.Fatalf("unsafe state destination exists or stat failed: %v", err)
	}
}

func TestMigrateLegacySkillsRejectsUnsafeManifestRelPath(t *testing.T) {
	repo := initSkillRepo(t)
	mgr := newTestManager(t)
	ctx := context.Background()

	_, result, err := mgr.Add(ctx, repo, AddOptions{Name: "acme", Ref: "HEAD", TargetKind: TargetCodexHome})
	if err != nil {
		t.Fatalf("add legacy source: %v", err)
	}
	oldPath := result.Installed[0].TargetPath
	manifestPath := filepath.Join(oldPath, manifestFilename)
	manifest, ok, err := readExportManifest(manifestPath)
	if err != nil || !ok {
		t.Fatalf("read manifest ok=%v err=%v", ok, err)
	}
	if len(manifest.Files) == 0 {
		t.Fatal("manifest has no files")
	}
	manifest.Files[0].RelPath = "../outside"
	if err := writeJSONFile(manifestPath, manifest); err != nil {
		t.Fatalf("tamper manifest: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if report.Status != MigrationStatusFailed || !report.Failed {
		t.Fatalf("report = %#v, want failed", report)
	}
}

func TestMigrateLegacySkillsMigratesCleanBuiltinCxp(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	oldRoot, err := mgr.TargetRoot(TargetCodexHome)
	if err != nil {
		t.Fatalf("old root: %v", err)
	}
	if _, err := mgr.installBuiltin(BuiltinCxpSkillName, oldRoot); err != nil {
		t.Fatalf("install old builtin: %v", err)
	}
	oldPath := filepath.Join(oldRoot, BuiltinCxpSkillName)
	if _, err := os.Stat(filepath.Join(oldPath, "SKILL.md")); err != nil {
		t.Fatalf("old builtin missing: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: true})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	found := false
	for _, result := range report.Results {
		if result.Kind == "builtin" {
			found = true
			if result.Status != MigrationStatusMigrated {
				t.Fatalf("builtin result = %#v, want migrated", result)
			}
		}
	}
	if !found {
		t.Fatalf("builtin migration result missing: %#v", report)
	}
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(agentsRoot, BuiltinCxpSkillName, "SKILL.md")); err != nil {
		t.Fatalf("agents builtin missing: %v", err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old builtin direct path still visible: %v", err)
	}
}

func TestMigrateLegacySkillsDoesNotResurrectOldSkillMissingFromState(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	oldRoot, err := mgr.TargetRoot(TargetCodexHome)
	if err != nil {
		t.Fatalf("old root: %v", err)
	}
	source := Source{
		ID:         stableID("acme", "https://example.invalid/acme/skills.git"),
		Name:       "acme",
		URL:        "https://example.invalid/acme/skills.git",
		RemoteURL:  "https://example.invalid/acme/skills.git",
		Ref:        "HEAD",
		TargetKind: TargetAgents,
		TargetRoot: agentsRoot,
		AutoSync:   true,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	allTrees := []skillTree{
		{
			Name:       "review",
			SourceDir:  "skills/review",
			ExportName: "acme__review",
			Files: []treeFile{{
				RepoPath: "skills/review/SKILL.md",
				RelPath:  "SKILL.md",
				Mode:     "100644",
				Data:     []byte("---\nname: review\ndescription: Review code\n---\nbody\n"),
			}},
		},
		{
			Name:       "debug",
			SourceDir:  "skills/debug",
			ExportName: "acme__debug",
			Files: []treeFile{{
				RepoPath: "skills/debug/SKILL.md",
				RelPath:  "SKILL.md",
				Mode:     "100644",
				Data:     []byte("---\nname: debug\ndescription: Debug code\n---\nbody\n"),
			}},
		},
	}
	installed, err := publishSkills(agentsRoot, source, "commit-one", allTrees[:1])
	if err != nil {
		t.Fatalf("publish agents skill: %v", err)
	}
	if _, err := publishSkills(oldRoot, source, "commit-one", allTrees); err != nil {
		t.Fatalf("publish legacy skills: %v", err)
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = append(cfg.Sources, source)
		st.Sources = append(st.Sources, SourceState{
			ID:              source.ID,
			Status:          StatusReady,
			LastCommit:      "commit-one",
			InstalledSkills: installed,
		})
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if report.Failed {
		t.Fatalf("report failed: %#v", report)
	}
	entries, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 || len(entries[0].State.InstalledSkills) != 1 || entries[0].State.InstalledSkills[0].ExportName != "acme__review" {
		t.Fatalf("installed skills = %#v, want only review from state", entries)
	}
	if _, err := os.Stat(filepath.Join(agentsRoot, "acme__debug")); !os.IsNotExist(err) {
		t.Fatalf("debug should not be resurrected into agents: %v", err)
	}
}

func TestMigrateLegacySkillsPreservesAgentsStateForPartialLegacyDuplicate(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	oldRoot, err := mgr.TargetRoot(TargetCodexHome)
	if err != nil {
		t.Fatalf("old root: %v", err)
	}
	source := Source{
		ID:         stableID("acme", "https://example.invalid/acme/skills.git"),
		Name:       "acme",
		URL:        "https://example.invalid/acme/skills.git",
		RemoteURL:  "https://example.invalid/acme/skills.git",
		Ref:        "HEAD",
		TargetKind: TargetAgents,
		TargetRoot: agentsRoot,
		AutoSync:   true,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	allTrees := []skillTree{
		{
			Name:       "review",
			SourceDir:  "skills/review",
			ExportName: "acme__review",
			Files: []treeFile{{
				RepoPath: "skills/review/SKILL.md",
				RelPath:  "SKILL.md",
				Mode:     "100644",
				Data:     []byte("---\nname: review\ndescription: Review code\n---\nbody\n"),
			}},
		},
		{
			Name:       "debug",
			SourceDir:  "skills/debug",
			ExportName: "acme__debug",
			Files: []treeFile{{
				RepoPath: "skills/debug/SKILL.md",
				RelPath:  "SKILL.md",
				Mode:     "100644",
				Data:     []byte("---\nname: debug\ndescription: Debug code\n---\nbody\n"),
			}},
		},
	}
	installed, err := publishSkills(agentsRoot, source, "commit-one", allTrees)
	if err != nil {
		t.Fatalf("publish agents skills: %v", err)
	}
	if _, err := publishSkills(oldRoot, source, "commit-one", allTrees[:1]); err != nil {
		t.Fatalf("publish partial legacy duplicate: %v", err)
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = append(cfg.Sources, source)
		st.Sources = append(st.Sources, SourceState{
			ID:              source.ID,
			Status:          StatusReady,
			LastCommit:      "commit-one",
			InstalledSkills: installed,
		})
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if !report.Changed {
		t.Fatalf("report changed = false, want true: %#v", report)
	}
	entries, err := mgr.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries len = %d, want 1", len(entries))
	}
	if len(entries[0].State.InstalledSkills) != 2 {
		t.Fatalf("installed skills = %#v, want both agents skills preserved", entries[0].State.InstalledSkills)
	}
	byExport := map[string]InstalledSkill{}
	for _, skill := range entries[0].State.InstalledSkills {
		byExport[skill.ExportName] = skill
	}
	if byExport["acme__debug"].TargetPath != filepath.Join(agentsRoot, "acme__debug") {
		t.Fatalf("debug target path = %q, want preserved agents path", byExport["acme__debug"].TargetPath)
	}
	if _, err := os.Stat(filepath.Join(oldRoot, "acme__review")); !os.IsNotExist(err) {
		t.Fatalf("legacy duplicate still visible: %v", err)
	}
}

func TestMigrateLegacySkillsDoesNotRetargetUnknownCustomSource(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	customRoot := filepath.Join(t.TempDir(), "shared-skills")
	source := Source{
		ID:         stableID("custom", "https://example.invalid/custom/skills.git"),
		Name:       "custom",
		URL:        "https://example.invalid/custom/skills.git",
		RemoteURL:  "https://example.invalid/custom/skills.git",
		Ref:        "HEAD",
		TargetKind: "custom",
		TargetRoot: customRoot,
		AutoSync:   true,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = append(cfg.Sources, source)
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if report.Changed || len(report.Results) != 1 || report.Results[0].Status != MigrationStatusSkipped {
		t.Fatalf("report = %#v, want skipped unchanged", report)
	}
	cfg, err := mgr.Store.LoadConfig()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Sources) != 1 || cfg.Sources[0].TargetKind != "custom" || cfg.Sources[0].TargetRoot != customRoot {
		t.Fatalf("source retargeted unexpectedly: %#v", cfg.Sources)
	}
}

func TestMigrateLegacySkillsDoesNotMigrateUnknownCustomSourceWithCandidates(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	customRoot := filepath.Join(t.TempDir(), "shared-skills")
	source := Source{
		ID:         stableID("custom", "https://example.invalid/custom/skills.git"),
		Name:       "custom",
		URL:        "https://example.invalid/custom/skills.git",
		RemoteURL:  "https://example.invalid/custom/skills.git",
		Ref:        "HEAD",
		TargetKind: "custom",
		TargetRoot: customRoot,
		AutoSync:   true,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	trees := []skillTree{{
		Name:       "review",
		SourceDir:  "skills/review",
		ExportName: "custom__review",
		Files: []treeFile{{
			RepoPath: "skills/review/SKILL.md",
			RelPath:  "SKILL.md",
			Mode:     "100644",
			Data:     []byte("---\nname: review\ndescription: Review code\n---\nbody\n"),
		}},
	}}
	installed, err := publishSkills(customRoot, source, "commit-one", trees)
	if err != nil {
		t.Fatalf("publish custom skill: %v", err)
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = append(cfg.Sources, source)
		st.Sources = append(st.Sources, SourceState{
			ID:              source.ID,
			Status:          StatusReady,
			LastCommit:      "commit-one",
			InstalledSkills: installed,
		})
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if report.Changed || len(report.Results) != 1 || report.Results[0].Status != MigrationStatusSkipped {
		t.Fatalf("report = %#v, want skipped unchanged", report)
	}
	if _, err := os.Stat(filepath.Join(customRoot, "custom__review", "SKILL.md")); err != nil {
		t.Fatalf("custom skill should remain in custom root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(agentsRoot, "custom__review")); !os.IsNotExist(err) {
		t.Fatalf("custom skill should not be migrated to agents: %v", err)
	}
}

func TestMigrateLegacySkillsDoesNotMigrateUnknownKindEvenAtCodexRoot(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	agentsRoot, err := mgr.TargetRoot(TargetAgents)
	if err != nil {
		t.Fatalf("agents root: %v", err)
	}
	oldRoot, err := mgr.TargetRoot(TargetCodexHome)
	if err != nil {
		t.Fatalf("old root: %v", err)
	}
	source := Source{
		ID:         stableID("future", "https://example.invalid/future/skills.git"),
		Name:       "future",
		URL:        "https://example.invalid/future/skills.git",
		RemoteURL:  "https://example.invalid/future/skills.git",
		Ref:        "HEAD",
		TargetKind: "future-target",
		TargetRoot: oldRoot,
		AutoSync:   true,
		AddedAt:    nowUTC(),
		UpdatedAt:  nowUTC(),
	}
	trees := []skillTree{{
		Name:       "review",
		SourceDir:  "skills/review",
		ExportName: "future__review",
		Files: []treeFile{{
			RepoPath: "skills/review/SKILL.md",
			RelPath:  "SKILL.md",
			Mode:     "100644",
			Data:     []byte("---\nname: review\ndescription: Review code\n---\nbody\n"),
		}},
	}}
	installed, err := publishSkills(oldRoot, source, "commit-one", trees)
	if err != nil {
		t.Fatalf("publish future skill: %v", err)
	}
	if err := mgr.Store.Update(func(cfg *Config, st *State) error {
		cfg.Sources = append(cfg.Sources, source)
		st.Sources = append(st.Sources, SourceState{ID: source.ID, Status: StatusReady, LastCommit: "commit-one", InstalledSkills: installed})
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	report, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if report.Changed || len(report.Results) != 1 || report.Results[0].Status != MigrationStatusSkipped {
		t.Fatalf("report = %#v, want skipped unchanged", report)
	}
	if _, err := os.Stat(filepath.Join(oldRoot, "future__review", "SKILL.md")); err != nil {
		t.Fatalf("future skill should remain in original root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(agentsRoot, "future__review")); !os.IsNotExist(err) {
		t.Fatalf("future skill should not be migrated to agents: %v", err)
	}
}

func TestMigrationDestinationStatusDetectsConflicts(t *testing.T) {
	source := Source{
		ID:        "source-1",
		Name:      "acme",
		RemoteURL: "https://example.invalid/acme/skills.git",
		Ref:       "HEAD",
	}
	tree := skillTree{
		Name:       "review",
		SourceDir:  "skills/review",
		ExportName: "acme__review",
		Files: []treeFile{{
			RepoPath: "skills/review/SKILL.md",
			RelPath:  "SKILL.md",
			Mode:     "100644",
			Data:     []byte("---\nname: review\ndescription: Review code\n---\nbody\n"),
		}},
	}
	oldRoot := filepath.Join(t.TempDir(), "old")
	installed, err := publishSkills(oldRoot, source, "commit-old", []skillTree{tree})
	if err != nil {
		t.Fatalf("publish old skill: %v", err)
	}
	oldManifest, ok, err := readExportManifest(filepath.Join(installed[0].TargetPath, manifestFilename))
	if err != nil || !ok {
		t.Fatalf("read old manifest ok=%v err=%v", ok, err)
	}

	for _, tc := range []struct {
		name    string
		setup   func(t *testing.T, root string)
		wantMsg string
	}{
		{
			name: "unmanaged destination",
			setup: func(t *testing.T, root string) {
				writeFile(t, filepath.Join(root, "acme__review", "SKILL.md"), "---\nname: review\ndescription: User\n---\nuser\n", 0o644)
			},
			wantMsg: "not managed",
		},
		{
			name: "managed by another source",
			setup: func(t *testing.T, root string) {
				other := source
				other.ID = "source-2"
				if _, err := publishSkills(root, other, "commit-old", []skillTree{tree}); err != nil {
					t.Fatalf("publish other source: %v", err)
				}
			},
			wantMsg: "another source",
		},
		{
			name: "local modified destination",
			setup: func(t *testing.T, root string) {
				if _, err := publishSkills(root, source, "commit-old", []skillTree{tree}); err != nil {
					t.Fatalf("publish same source: %v", err)
				}
				writeFile(t, filepath.Join(root, "acme__review", "SKILL.md"), "---\nname: review\ndescription: Local\n---\nlocal\n", 0o644)
			},
			wantMsg: "local modifications",
		},
		{
			name: "different clean managed version",
			setup: func(t *testing.T, root string) {
				updated := tree
				updated.Files = []treeFile{{
					RepoPath: "skills/review/SKILL.md",
					RelPath:  "SKILL.md",
					Mode:     "100644",
					Data:     []byte("---\nname: review\ndescription: Review code\n---\nupdated\n"),
				}}
				if _, err := publishSkills(root, source, "commit-new", []skillTree{updated}); err != nil {
					t.Fatalf("publish different version: %v", err)
				}
			},
			wantMsg: "different clean managed version",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			tc.setup(t, root)
			status, message, err := migrationDestinationStatus(filepath.Join(root, "acme__review"), oldManifest)
			if err != nil {
				t.Fatalf("destination status: %v", err)
			}
			if status != MigrationStatusConflict || !strings.Contains(message, tc.wantMsg) {
				t.Fatalf("status=%q message=%q, want conflict containing %q", status, message, tc.wantMsg)
			}
		})
	}
}
