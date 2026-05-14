package skills

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

const manifestFilename = ".cxp-skill-manifest.json"

type treeFile struct {
	RepoPath string
	RelPath  string
	Mode     string
	OID      string
	Data     []byte
}

type skillTree struct {
	Name       string
	SourceDir  string
	ExportName string
	Files      []treeFile
}

type exportManifest struct {
	Version    int            `json:"version"`
	SourceID   string         `json:"source_id"`
	SourceName string         `json:"source_name"`
	RemoteURL  string         `json:"remote_url"`
	Ref        string         `json:"ref,omitempty"`
	Commit     string         `json:"commit"`
	SkillName  string         `json:"skill_name"`
	SourcePath string         `json:"source_path"`
	ExportName string         `json:"export_name"`
	ExportedAt time.Time      `json:"exported_at"`
	Files      []FileManifest `json:"files"`
}

func scanSkillsFromGitTree(ctx context.Context, git GitRunner, mirror string, source Source, commit string) ([]skillTree, error) {
	prefix := strings.Trim(strings.ReplaceAll(source.Path, "\\", "/"), "/")
	args := []string{"ls-tree", "-rz", "-r", commit}
	if prefix != "" {
		args = append(args, "--", prefix)
	}
	out, err := git.Run(ctx, mirror, nil, args...)
	if err != nil {
		return nil, err
	}
	files, err := parseTreeListing(out, prefix)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no files found under %q at %s", firstNonEmpty(prefix, "."), commit)
	}
	if err := detectCaseFoldCollisions(files); err != nil {
		return nil, err
	}
	for i := range files {
		if files[i].Mode != "100644" && files[i].Mode != "100755" {
			return nil, fmt.Errorf("unsupported git mode %s at %s; symlinks and submodules are not installed", files[i].Mode, files[i].RepoPath)
		}
		if strings.EqualFold(path.Base(files[i].RepoPath), ".gitmodules") {
			return nil, fmt.Errorf(".gitmodules is not allowed in skill source %s", source.Name)
		}
		data, err := git.Run(ctx, mirror, nil, "cat-file", "blob", files[i].OID)
		if err != nil {
			return nil, err
		}
		files[i].Data = data
	}
	return discoverSkillTrees(source, files)
}

func parseTreeListing(data []byte, prefix string) ([]treeFile, error) {
	entries := bytes.Split(data, []byte{0})
	files := make([]treeFile, 0, len(entries))
	for _, entry := range entries {
		if len(bytes.TrimSpace(entry)) == 0 {
			continue
		}
		head, repoPathBytes, ok := bytes.Cut(entry, []byte{'\t'})
		if !ok {
			return nil, fmt.Errorf("invalid git tree entry %q", string(entry))
		}
		fields := bytes.Fields(head)
		if len(fields) < 3 {
			return nil, fmt.Errorf("invalid git tree header %q", string(head))
		}
		repoPath := string(repoPathBytes)
		if err := validateRepoRelPath(repoPath); err != nil {
			return nil, fmt.Errorf("reject git path %q: %w", repoPath, err)
		}
		rel := repoPath
		if prefix != "" {
			rel = strings.TrimPrefix(repoPath, prefix)
			rel = strings.TrimPrefix(rel, "/")
		}
		if rel == "" {
			continue
		}
		files = append(files, treeFile{
			RepoPath: repoPath,
			RelPath:  rel,
			Mode:     string(fields[0]),
			OID:      string(fields[2]),
		})
	}
	return files, nil
}

func detectCaseFoldCollisions(files []treeFile) error {
	seen := map[string]string{}
	for _, f := range files {
		key := strings.ToLower(f.RelPath)
		if prev, ok := seen[key]; ok && prev != f.RelPath {
			return fmt.Errorf("case-insensitive path collision: %s and %s", prev, f.RelPath)
		}
		seen[key] = f.RelPath
	}
	return nil
}

func discoverSkillTrees(source Source, files []treeFile) ([]skillTree, error) {
	byRepoPath := map[string]treeFile{}
	var skillDirs []string
	for _, f := range files {
		byRepoPath[f.RepoPath] = f
		if path.Base(f.RepoPath) == "SKILL.md" {
			skillDirs = append(skillDirs, path.Dir(f.RepoPath))
		}
	}
	sort.Strings(skillDirs)
	if len(skillDirs) == 0 {
		return nil, fmt.Errorf("no SKILL.md found under %q", firstNonEmpty(source.Path, "."))
	}
	trees := make([]skillTree, 0, len(skillDirs))
	for _, dir := range skillDirs {
		skillFile := byRepoPath[path.Join(dir, "SKILL.md")]
		name, err := parseSkillName(skillFile.Data, dir)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", skillFile.RepoPath, err)
		}
		tree := skillTree{
			Name:       name,
			SourceDir:  dir,
			ExportName: exportNameForSkill(source, name, dir),
		}
		for _, f := range files {
			if dir == "." || f.RepoPath == dir || strings.HasPrefix(f.RepoPath, dir+"/") {
				rel := f.RepoPath
				if dir != "." {
					rel = strings.TrimPrefix(f.RepoPath, dir)
					rel = strings.TrimPrefix(rel, "/")
				}
				if rel == "" {
					continue
				}
				if err := validateRepoRelPath(rel); err != nil {
					return nil, fmt.Errorf("reject skill sidecar path %q: %w", rel, err)
				}
				copy := f
				copy.RelPath = rel
				tree.Files = append(tree.Files, copy)
			}
		}
		sort.Slice(tree.Files, func(i, j int) bool { return tree.Files[i].RelPath < tree.Files[j].RelPath })
		trees = append(trees, tree)
	}
	return trees, nil
}

func parseSkillName(data []byte, sourceDir string) (string, error) {
	text := string(data)
	if !strings.HasPrefix(text, "---\n") && !strings.HasPrefix(text, "---\r\n") {
		return "", fmt.Errorf("SKILL.md must start with YAML frontmatter")
	}
	body := strings.TrimPrefix(strings.TrimPrefix(text, "---\r\n"), "---\n")
	end := strings.Index(body, "\n---")
	if end < 0 {
		return "", fmt.Errorf("SKILL.md frontmatter is not closed")
	}
	front := body[:end]
	name := ""
	description := ""
	for _, line := range strings.Split(front, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "name":
			name = value
		case "description":
			description = value
		}
	}
	if strings.TrimSpace(description) == "" {
		return "", fmt.Errorf("SKILL.md frontmatter must contain a non-empty description")
	}
	if strings.TrimSpace(name) == "" {
		name = path.Base(sourceDir)
	}
	if safeName(name) == "" {
		return "", fmt.Errorf("skill name %q is not usable as an export name", name)
	}
	return strings.TrimSpace(name), nil
}

func exportNameForSkill(source Source, skillName string, sourceDir string) string {
	base := safeName(source.Name)
	skill := safeName(skillName)
	if base == "" {
		base = "source"
	}
	if skill == "" {
		skill = safeName(path.Base(sourceDir))
	}
	name := base + "__" + skill
	if len(name) > 80 {
		name = name[:80]
		name = strings.TrimRight(name, ".-_")
	}
	return name
}

func publishSkills(targetRoot string, source Source, commit string, trees []skillTree) ([]InstalledSkill, error) {
	if targetRoot == "" {
		return nil, fmt.Errorf("empty skill target root")
	}
	if err := os.MkdirAll(targetRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create skill target root: %w", err)
	}
	lock := flock.New(filepath.Join(targetRoot, ".cxp-skill-publish.lock"))
	if err := lock.Lock(); err != nil {
		return nil, fmt.Errorf("lock skill target root: %w", err)
	}
	defer func() { _ = lock.Unlock() }()
	if err := refuseLocalChangesForSource(targetRoot, source.ID); err != nil {
		return nil, err
	}

	installed := make([]InstalledSkill, 0, len(trees))
	seenExport := map[string]bool{}
	for _, tree := range trees {
		if seenExport[strings.ToLower(tree.ExportName)] {
			return nil, fmt.Errorf("duplicate export directory %s", tree.ExportName)
		}
		seenExport[strings.ToLower(tree.ExportName)] = true
		target := filepath.Join(targetRoot, tree.ExportName)
		if err := ensureManagedOrAbsent(target, source.ID); err != nil {
			return nil, err
		}
		staging := filepath.Join(targetRoot, ".cxp-skill-staging-"+tree.ExportName+"-"+time.Now().UTC().Format("20060102150405"))
		if err := os.RemoveAll(staging); err != nil {
			return nil, fmt.Errorf("remove stale staging dir: %w", err)
		}
		if err := writeSkillTree(staging, tree); err != nil {
			_ = os.RemoveAll(staging)
			return nil, err
		}
		manifest := manifestForTree(source, commit, tree, target)
		if err := writeManifest(filepath.Join(staging, manifestFilename), manifest); err != nil {
			_ = os.RemoveAll(staging)
			return nil, err
		}
		backup := target + ".cxp-backup-" + time.Now().UTC().Format("20060102150405")
		hadTarget := false
		if _, err := os.Stat(target); err == nil {
			hadTarget = true
			if err := os.Rename(target, backup); err != nil {
				_ = os.RemoveAll(staging)
				return nil, fmt.Errorf("backup existing skill %s: %w", tree.ExportName, err)
			}
		} else if err != nil && !os.IsNotExist(err) {
			_ = os.RemoveAll(staging)
			return nil, fmt.Errorf("stat existing skill %s: %w", tree.ExportName, err)
		}
		if err := os.Rename(staging, target); err != nil {
			if hadTarget {
				_ = os.Rename(backup, target)
			}
			_ = os.RemoveAll(staging)
			return nil, fmt.Errorf("publish skill %s: %w", tree.ExportName, err)
		}
		if hadTarget {
			_ = os.RemoveAll(backup)
		}
		installed = append(installed, InstalledSkill{
			Name:       tree.Name,
			ExportName: tree.ExportName,
			SourcePath: tree.SourceDir,
			TargetPath: target,
			Files:      manifest.Files,
		})
	}
	if err := pruneRemovedManagedSkills(targetRoot, source.ID, seenExport); err != nil {
		return nil, err
	}
	return installed, nil
}

func refuseLocalChangesForSource(targetRoot string, sourceID string) error {
	entries, err := os.ReadDir(targetRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read skill target root: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		dir := filepath.Join(targetRoot, entry.Name())
		manifest, ok, err := readExportManifest(filepath.Join(dir, manifestFilename))
		if err != nil {
			return err
		}
		if !ok || manifest.SourceID != sourceID {
			continue
		}
		modified, err := exportHasLocalChanges(dir, manifest)
		if err != nil {
			return err
		}
		if modified {
			return fmt.Errorf("refuse to sync %s because it has local modifications; run `cxp skills push` before syncing", dir)
		}
	}
	return nil
}

func ensureManagedOrAbsent(target string, sourceID string) error {
	manifest, ok, err := readExportManifest(filepath.Join(target, manifestFilename))
	if err != nil {
		return err
	}
	if !ok {
		if _, err := os.Stat(target); err == nil {
			return fmt.Errorf("target %s already exists and is not managed by cxp skills", target)
		} else if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("stat target %s: %w", target, err)
		}
		return nil
	}
	if manifest.SourceID != sourceID {
		return fmt.Errorf("target %s is managed by source %s, not %s", target, manifest.SourceID, sourceID)
	}
	return nil
}

func writeSkillTree(root string, tree skillTree) error {
	for _, f := range tree.Files {
		if err := validateRepoRelPath(f.RelPath); err != nil {
			return fmt.Errorf("reject export path %q: %w", f.RelPath, err)
		}
		target := filepath.Join(root, filepath.FromSlash(f.RelPath))
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(root)+string(filepath.Separator)) && filepath.Clean(target) != filepath.Clean(root) {
			return fmt.Errorf("export path escapes target root: %s", f.RelPath)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return fmt.Errorf("create export dir: %w", err)
		}
		mode := fs.FileMode(0o644)
		if f.Mode == "100755" {
			mode = 0o755
		}
		if err := os.WriteFile(target, f.Data, mode); err != nil {
			return fmt.Errorf("write export file %s: %w", f.RelPath, err)
		}
	}
	return nil
}

func manifestForTree(source Source, commit string, tree skillTree, targetPath string) exportManifest {
	files := make([]FileManifest, 0, len(tree.Files))
	for _, f := range tree.Files {
		sum := sha256.Sum256(f.Data)
		mode := uint32(0o644)
		if f.Mode == "100755" {
			mode = 0o755
		}
		files = append(files, FileManifest{
			RelPath: f.RelPath,
			SHA256:  hex.EncodeToString(sum[:]),
			Size:    int64(len(f.Data)),
			Mode:    mode,
		})
	}
	return exportManifest{
		Version:    1,
		SourceID:   source.ID,
		SourceName: source.Name,
		RemoteURL:  source.RemoteURL,
		Ref:        source.Ref,
		Commit:     commit,
		SkillName:  tree.Name,
		SourcePath: tree.SourceDir,
		ExportName: tree.ExportName,
		ExportedAt: nowUTC(),
		Files:      files,
	}
}

func writeManifest(path string, manifest exportManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal skill manifest: %w", err)
	}
	data = append(data, '\n')
	return atomicWriteFile(path, data, 0o600)
}

func readExportManifest(path string) (exportManifest, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return exportManifest{}, false, nil
		}
		return exportManifest{}, false, fmt.Errorf("read skill manifest %s: %w", path, err)
	}
	var manifest exportManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return exportManifest{}, false, fmt.Errorf("parse skill manifest %s: %w", path, err)
	}
	return manifest, true, nil
}

func pruneRemovedManagedSkills(targetRoot string, sourceID string, keep map[string]bool) error {
	entries, err := os.ReadDir(targetRoot)
	if err != nil {
		return fmt.Errorf("read skill target root: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		if keep[strings.ToLower(entry.Name())] {
			continue
		}
		dir := filepath.Join(targetRoot, entry.Name())
		manifest, ok, err := readExportManifest(filepath.Join(dir, manifestFilename))
		if err != nil {
			return err
		}
		if ok && manifest.SourceID == sourceID {
			if err := os.RemoveAll(dir); err != nil {
				return fmt.Errorf("prune removed skill %s: %w", entry.Name(), err)
			}
		}
	}
	return nil
}

func removeManagedSkills(targetRoot string, sourceID string) error {
	entries, err := os.ReadDir(targetRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read skill target root: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		dir := filepath.Join(targetRoot, entry.Name())
		manifest, ok, err := readExportManifest(filepath.Join(dir, manifestFilename))
		if err != nil {
			return err
		}
		if ok && manifest.SourceID == sourceID {
			if modified, err := exportHasLocalChanges(dir, manifest); err != nil {
				return err
			} else if modified {
				return fmt.Errorf("refuse to remove %s because it has local modifications; run `cxp skills push` or remove it manually", dir)
			}
			if err := os.RemoveAll(dir); err != nil {
				return fmt.Errorf("remove managed skill %s: %w", entry.Name(), err)
			}
		}
	}
	return nil
}

func exportHasLocalChanges(root string, manifest exportManifest) (bool, error) {
	changes, err := localChangesForManifest(root, manifest)
	return len(changes) > 0, err
}

func localChangesForManifest(root string, manifest exportManifest) ([]LocalChange, error) {
	known := map[string]FileManifest{}
	for _, file := range manifest.Files {
		known[file.RelPath] = file
	}
	var changes []LocalChange
	for _, file := range manifest.Files {
		filePath := filepath.Join(root, filepath.FromSlash(file.RelPath))
		data, err := os.ReadFile(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				changes = append(changes, LocalChange{Kind: ChangeDeleted, RelPath: file.RelPath, OldSHA256: file.SHA256, OldMode: canonicalSkillFileMode(file.Mode)})
				continue
			}
			return nil, fmt.Errorf("read %s: %w", filePath, err)
		}
		info, err := os.Stat(filePath)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", filePath, err)
		}
		sum := sha256.Sum256(data)
		got := hex.EncodeToString(sum[:])
		oldMode := canonicalSkillFileMode(file.Mode)
		newMode := localSkillFileMode(info.Mode(), oldMode)
		if got != file.SHA256 || newMode != oldMode {
			changes = append(changes, LocalChange{
				Kind:      ChangeModified,
				RelPath:   file.RelPath,
				OldSHA256: file.SHA256,
				NewSHA256: got,
				OldMode:   oldMode,
				NewMode:   newMode,
				Size:      int64(len(data)),
			})
		}
	}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == manifestFilename {
			return nil
		}
		if _, ok := known[rel]; ok {
			return nil
		}
		if err := validateRepoRelPath(rel); err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		changes = append(changes, LocalChange{
			Kind:      ChangeAdded,
			RelPath:   rel,
			NewSHA256: hex.EncodeToString(sum[:]),
			NewMode:   localSkillFileMode(info.Mode(), 0),
			Size:      int64(len(data)),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan local changes under %s: %w", root, err)
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].RelPath < changes[j].RelPath })
	return changes, nil
}

func canonicalSkillFileMode(mode uint32) uint32 {
	if mode&0o111 != 0 {
		return 0o755
	}
	return 0o644
}

func localSkillFileMode(mode fs.FileMode, fallback uint32) uint32 {
	if runtime.GOOS == "windows" {
		if fallback != 0 {
			return canonicalSkillFileMode(fallback)
		}
		return 0o644
	}
	if mode.Perm()&0o111 != 0 {
		return 0o755
	}
	return 0o644
}
