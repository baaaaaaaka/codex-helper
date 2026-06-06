package skills

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const (
	BuiltinCxpSkillName = "cxp"

	builtinCxpRoot     = "builtin/cxp"
	builtinCxpSourceID = "builtin:cxp"
	builtinProvider    = "builtin"
)

//go:embed builtin/cxp/**
var builtinSkillFS embed.FS

type BuiltinInstallOptions struct {
	Name string
}

type BuiltinInstallResult struct {
	Source    Source
	Commit    string
	Installed []InstalledSkill
}

func BuiltinSkillNames() []string {
	return []string{BuiltinCxpSkillName}
}

func (m *Manager) InstallBuiltins(ctx context.Context, opts BuiltinInstallOptions) ([]BuiltinInstallResult, error) {
	var results []BuiltinInstallResult
	err := m.withOperationLock(ctx, func() error {
		var err error
		results, err = m.installBuiltinsUnlocked(ctx, opts)
		return err
	})
	return results, err
}

func (m *Manager) installBuiltinsUnlocked(ctx context.Context, opts BuiltinInstallOptions) ([]BuiltinInstallResult, error) {
	targetRoot, err := m.TargetRoot(TargetAgents)
	if err != nil {
		return nil, err
	}
	names, err := selectedBuiltinSkillNames(opts.Name)
	if err != nil {
		return nil, err
	}
	results := make([]BuiltinInstallResult, 0, len(names))
	for _, name := range names {
		select {
		case <-ctx.Done():
			return results, ctx.Err()
		default:
		}
		result, err := m.installBuiltin(name, targetRoot)
		if err != nil {
			return results, err
		}
		results = append(results, result)
	}
	return results, nil
}

func selectedBuiltinSkillNames(name string) ([]string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	switch name {
	case "", "all":
		return BuiltinSkillNames(), nil
	case BuiltinCxpSkillName:
		return []string{BuiltinCxpSkillName}, nil
	default:
		return nil, fmt.Errorf("unknown builtin skill %q", name)
	}
}

func (m *Manager) installBuiltin(name string, targetRoot string) (BuiltinInstallResult, error) {
	tree, commit, err := builtinSkillTree(name)
	if err != nil {
		return BuiltinInstallResult{}, err
	}
	now := nowUTC()
	source := Source{
		ID:         builtinCxpSourceID,
		Name:       BuiltinCxpSkillName,
		URL:        "builtin://" + BuiltinCxpSkillName,
		RemoteURL:  "builtin://" + BuiltinCxpSkillName,
		Provider:   builtinProvider,
		Ref:        commit,
		TargetKind: TargetAgents,
		TargetRoot: targetRoot,
		AutoSync:   false,
		AddedAt:    now,
		UpdatedAt:  now,
	}
	installed, err := publishSkills(targetRoot, source, commit, []skillTree{tree})
	if err != nil {
		return BuiltinInstallResult{}, builtinInstallError(name, targetRoot, err)
	}
	return BuiltinInstallResult{Source: source, Commit: commit, Installed: installed}, nil
}

func builtinInstallError(name string, targetRoot string, err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "local modifications") {
		return fmt.Errorf("refuse to install builtin skill %q because %s has local modifications; move that directory aside, remove it, or restore the managed files before reinstalling", name, filepath.Join(targetRoot, name))
	}
	return err
}

func builtinSkillTree(name string) (skillTree, string, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case BuiltinCxpSkillName:
	default:
		return skillTree{}, "", fmt.Errorf("unknown builtin skill %q", name)
	}
	files, err := readBuiltinSkillFiles(builtinCxpRoot)
	if err != nil {
		return skillTree{}, "", err
	}
	if err := detectCaseFoldCollisions(files); err != nil {
		return skillTree{}, "", err
	}
	var skillName string
	for _, file := range files {
		if file.RelPath == "SKILL.md" {
			skillName, err = parseSkillName(file.Data, ".")
			if err != nil {
				return skillTree{}, "", err
			}
			break
		}
	}
	if skillName == "" {
		return skillTree{}, "", fmt.Errorf("builtin skill %q has no SKILL.md", name)
	}
	if skillName != BuiltinCxpSkillName {
		return skillTree{}, "", fmt.Errorf("builtin skill name %q does not match %q", skillName, BuiltinCxpSkillName)
	}
	tree := skillTree{
		Name:       skillName,
		SourceDir:  ".",
		ExportName: BuiltinCxpSkillName,
		Files:      files,
	}
	return tree, builtinCommit(skillName, files), nil
}

func readBuiltinSkillFiles(root string) ([]treeFile, error) {
	var files []treeFile
	err := fs.WalkDir(builtinSkillFS, root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel := strings.TrimPrefix(p, root+"/")
		if rel == "" || strings.HasPrefix(rel, "../") || path.IsAbs(rel) {
			return fmt.Errorf("unsafe builtin skill path %q", p)
		}
		data, err := builtinSkillFS.ReadFile(p)
		if err != nil {
			return err
		}
		mode := "100644"
		if info, err := d.Info(); err == nil && info.Mode().Perm()&0o111 != 0 {
			mode = "100755"
		}
		files = append(files, treeFile{
			RepoPath: filepath.ToSlash(rel),
			RelPath:  filepath.ToSlash(rel),
			Mode:     mode,
			Data:     data,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].RelPath < files[j].RelPath })
	return files, nil
}

func builtinCommit(name string, files []treeFile) string {
	h := sha256.New()
	_, _ = h.Write([]byte(name))
	_, _ = h.Write([]byte{0})
	sorted := append([]treeFile(nil), files...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].RelPath < sorted[j].RelPath })
	for _, file := range sorted {
		_, _ = h.Write([]byte(file.RelPath))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(file.Mode))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(file.Data)
		_, _ = h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	return "builtin:" + name + ":" + hex.EncodeToString(sum[:])[:12]
}
