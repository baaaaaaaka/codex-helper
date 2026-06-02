package skills

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestParseSkillNameRequiresFrontmatterDescription(t *testing.T) {
	if _, err := parseSkillName([]byte("no frontmatter"), "skills/review"); err == nil {
		t.Fatal("parseSkillName without frontmatter = nil, want error")
	}
	if _, err := parseSkillName([]byte("---\nname: review\n---\nbody"), "skills/review"); err == nil {
		t.Fatal("parseSkillName without description = nil, want error")
	}
	name, err := parseSkillName([]byte("---\nname: review\ndescription: Review code\n---\nbody"), "skills/review")
	if err != nil {
		t.Fatalf("parseSkillName: %v", err)
	}
	if name != "review" {
		t.Fatalf("name = %q", name)
	}
}

func TestParseSkillNameRejectsInvalidYAMLFrontmatter(t *testing.T) {
	data := []byte("---\nname: review\ndescription: Use when operations: proxy profiles\n---\nbody")
	_, err := parseSkillName(data, "skills/review")
	if err == nil || !strings.Contains(err.Error(), "invalid YAML") {
		t.Fatalf("parseSkillName invalid YAML error = %v, want invalid YAML", err)
	}
}

func TestParseSkillNameSupportsQuotedDescriptionWithColon(t *testing.T) {
	data := []byte("---\nname: review\ndescription: \"Use when operations: proxy profiles\"\n---\nbody")
	name, err := parseSkillName(data, "skills/review")
	if err != nil {
		t.Fatalf("parseSkillName quoted description: %v", err)
	}
	if name != "review" {
		t.Fatalf("name = %q", name)
	}
}

func TestParseTreeListingRejectsUnsafePath(t *testing.T) {
	data := []byte("100644 blob abcdef\t../SKILL.md\x00")
	if _, err := parseTreeListing(data, ""); err == nil {
		t.Fatal("parseTreeListing unsafe path = nil, want error")
	}
}

func TestDiscoverSkillTreesKeepsSidecars(t *testing.T) {
	files := []treeFile{
		{RepoPath: "skills/review/SKILL.md", RelPath: "skills/review/SKILL.md", Mode: "100644", Data: []byte("---\nname: review\ndescription: Review code\n---\nbody")},
		{RepoPath: "skills/review/agents/openai.yaml", RelPath: "skills/review/agents/openai.yaml", Mode: "100644", Data: []byte("version: 1\n")},
		{RepoPath: "skills/review/scripts/check.sh", RelPath: "skills/review/scripts/check.sh", Mode: "100755", Data: []byte("#!/bin/sh\n")},
	}
	trees, err := discoverSkillTrees(Source{Name: "acme"}, files)
	if err != nil {
		t.Fatalf("discoverSkillTrees: %v", err)
	}
	if len(trees) != 1 {
		t.Fatalf("trees len = %d", len(trees))
	}
	if len(trees[0].Files) != 3 {
		t.Fatalf("files len = %d", len(trees[0].Files))
	}
	if trees[0].ExportName != "acme__review" {
		t.Fatalf("export name = %q", trees[0].ExportName)
	}
}

func TestDiscoverSkillTreesSupportsRepoRootSkill(t *testing.T) {
	files := []treeFile{
		{RepoPath: "SKILL.md", RelPath: "SKILL.md", Mode: "100644", Data: []byte("---\nname: review\ndescription: Review code\n---\nbody")},
		{RepoPath: "scripts/check.sh", RelPath: "scripts/check.sh", Mode: "100755", Data: []byte("#!/bin/sh\n")},
	}
	trees, err := discoverSkillTrees(Source{Name: "acme"}, files)
	if err != nil {
		t.Fatalf("discoverSkillTrees: %v", err)
	}
	if len(trees) != 1 {
		t.Fatalf("trees len = %d", len(trees))
	}
	if trees[0].SourceDir != "." {
		t.Fatalf("source dir = %q, want .", trees[0].SourceDir)
	}
	if len(trees[0].Files) != 2 {
		t.Fatalf("files len = %d", len(trees[0].Files))
	}
	if trees[0].Files[0].RelPath != "SKILL.md" || trees[0].Files[1].RelPath != "scripts/check.sh" {
		t.Fatalf("files = %#v", trees[0].Files)
	}
}

func TestExportNameForSkillPreservesSkillSuffixWhenSourceNameIsLong(t *testing.T) {
	source := Source{Name: strings.Repeat("very-long-source-name-", 8)}
	name := exportNameForSkill(source, "review", "skills/review")
	if len(name) > 80 {
		t.Fatalf("export name len = %d, want <= 80: %q", len(name), name)
	}
	if !strings.HasSuffix(name, "__review") {
		t.Fatalf("export name = %q, want __review suffix preserved", name)
	}
}

func TestDetectCaseFoldCollisionsRejectsCrossPlatformAmbiguity(t *testing.T) {
	err := detectCaseFoldCollisions([]treeFile{
		{RepoPath: "skills/review/SKILL.md", RelPath: "skills/review/SKILL.md"},
		{RepoPath: "skills/review/skill.md", RelPath: "skills/review/skill.md"},
	})
	if err == nil {
		t.Fatal("detectCaseFoldCollisions = nil, want error")
	}
}

func TestScanSkillsFromGitTreeRejectsUnsafeGitTreeEntries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		listing string
		want    string
	}{
		{
			name:    "symlink",
			listing: "120000 blob symlinkoid\tskills/review/SKILL.md\x00",
			want:    "unsupported git mode 120000",
		},
		{
			name:    "gitlink",
			listing: "160000 commit gitlinkoid\tskills/review/vendor\x00",
			want:    "unsupported git mode 160000",
		},
		{
			name:    "gitmodules",
			listing: "100644 blob modulesoid\tskills/review/.gitmodules\x00",
			want:    ".gitmodules is not allowed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := scanSkillsFromGitTree(context.Background(), treeListingGitRunner{listing: []byte(tc.listing)}, "mirror", Source{Name: "acme", Path: "skills"}, "commit", t.TempDir())
			if err == nil || !containsString(err.Error(), tc.want) {
				t.Fatalf("scan error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestScanSkillsFromGitTreeMaterializesGitLFSFiles(t *testing.T) {
	payload := []byte("MZ real executable bytes")
	sum := sha256.Sum256(payload)
	oid := hex.EncodeToString(sum[:])
	mirror := t.TempDir()
	runner := &lfsTreeGitRunner{
		listing: []byte("100644 blob skilloid\tskills/flash/SKILL.md\x00" +
			"100644 blob assetoid\tskills/flash/assets/tools/nvflash_eng.exe\x00"),
		blobs: map[string][]byte{
			"skilloid": []byte("---\nname: flash\ndescription: Flash vBIOS\n---\nbody\n"),
			"assetoid": []byte(fmt.Sprintf("%s\noid sha256:%s\nsize %d\n", gitLFSPointerVersion, oid, len(payload))),
		},
		lfsOID:  oid,
		lfsData: payload,
	}

	trees, err := scanSkillsFromGitTree(context.Background(), runner, mirror, Source{Name: "fgx", RemoteURL: "ssh://git@gitlab.example.com/acme/skills.git"}, "commit", t.TempDir())
	if err != nil {
		t.Fatalf("scanSkillsFromGitTree: %v", err)
	}
	if runner.lfsFetches != 1 {
		t.Fatalf("lfs fetches = %d, want 1", runner.lfsFetches)
	}
	if len(trees) != 1 {
		t.Fatalf("trees len = %d, want 1", len(trees))
	}
	var got []byte
	for _, file := range trees[0].Files {
		if file.RelPath == "assets/tools/nvflash_eng.exe" {
			got = file.Data
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("LFS asset data = %q, want %q", got, payload)
	}
}

func TestScanSkillsFromGitTreeInstallsManagedGitLFSWhenMissing(t *testing.T) {
	payload := []byte("MZ real executable bytes")
	sum := sha256.Sum256(payload)
	oid := hex.EncodeToString(sum[:])
	managedDir := t.TempDir()
	runner := &lfsTreeGitRunner{
		listing: []byte("100644 blob skilloid\tskills/flash/SKILL.md\x00" +
			"100644 blob assetoid\tskills/flash/assets/tools/nvflash_eng.exe\x00"),
		blobs: map[string][]byte{
			"skilloid": []byte("---\nname: flash\ndescription: Flash vBIOS\n---\nbody\n"),
			"assetoid": []byte(fmt.Sprintf("%s\noid sha256:%s\nsize %d\n", gitLFSPointerVersion, oid, len(payload))),
		},
		lfsErr: &GitError{
			Args:   []string{"lfs", "fetch", skillsLFSRemoteName, "commit"},
			Output: "git: 'lfs' is not a git command. See 'git --help'.",
			Err:    fmt.Errorf("exit status 1"),
		},
		managedDir: managedDir,
		lfsOID:     oid,
		lfsData:    payload,
	}
	restore := stubManagedGitLFSInstaller(t, managedDir, nil)
	defer restore()

	trees, err := scanSkillsFromGitTree(context.Background(), runner, t.TempDir(), Source{Name: "fgx", RemoteURL: "ssh://git@gitlab.example.com/acme/skills.git"}, "commit", t.TempDir())
	if err != nil {
		t.Fatalf("scanSkillsFromGitTree: %v", err)
	}
	if runner.lfsFetches != 2 {
		t.Fatalf("lfs fetches = %d, want initial failure plus managed retry", runner.lfsFetches)
	}
	if !runner.usedManagedEnv {
		t.Fatal("managed git-lfs dir was not added to retry PATH")
	}
	var got []byte
	for _, file := range trees[0].Files {
		if file.RelPath == "assets/tools/nvflash_eng.exe" {
			got = file.Data
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("LFS asset data = %q, want %q", got, payload)
	}
}

func TestScanSkillsFromGitTreeReportsManagedGitLFSInstallFailure(t *testing.T) {
	payload := []byte("MZ real executable bytes")
	sum := sha256.Sum256(payload)
	oid := hex.EncodeToString(sum[:])
	runner := &lfsTreeGitRunner{
		listing: []byte("100644 blob skilloid\tskills/flash/SKILL.md\x00" +
			"100644 blob assetoid\tskills/flash/assets/tools/nvflash_eng.exe\x00"),
		blobs: map[string][]byte{
			"skilloid": []byte("---\nname: flash\ndescription: Flash vBIOS\n---\nbody\n"),
			"assetoid": []byte(fmt.Sprintf("%s\noid sha256:%s\nsize %d\n", gitLFSPointerVersion, oid, len(payload))),
		},
		lfsErr: &GitError{
			Args:   []string{"lfs", "fetch", skillsLFSRemoteName, "commit"},
			Output: "git: 'lfs' is not a git command. See 'git --help'.",
			Err:    fmt.Errorf("exit status 1"),
		},
	}
	restore := stubManagedGitLFSInstaller(t, "", fmt.Errorf("download failed"))
	defer restore()

	_, err := scanSkillsFromGitTree(context.Background(), runner, t.TempDir(), Source{Name: "fgx", RemoteURL: "ssh://git@gitlab.example.com/acme/skills.git"}, "commit", t.TempDir())
	if err == nil {
		t.Fatal("scanSkillsFromGitTree with unavailable git-lfs succeeded, want error")
	}
	for _, want := range []string{
		"skills/flash/assets/tools/nvflash_eng.exe is a Git LFS pointer",
		"install managed git-lfs",
		"download failed",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %v, want %q", err, want)
		}
	}
}

func TestScanSkillsFromGitTreeRejectsMalformedGitLFSPointer(t *testing.T) {
	runner := &lfsTreeGitRunner{
		listing: []byte("100644 blob skilloid\tskills/flash/SKILL.md\x00" +
			"100644 blob assetoid\tskills/flash/assets/tools/nvflash_eng.exe\x00"),
		blobs: map[string][]byte{
			"skilloid": []byte("---\nname: flash\ndescription: Flash vBIOS\n---\nbody\n"),
			"assetoid": []byte(gitLFSPointerVersion + "\noid sha256:not-a-real-sha\nsize 12\n"),
		},
	}
	_, err := scanSkillsFromGitTree(context.Background(), runner, t.TempDir(), Source{Name: "fgx", RemoteURL: "ssh://git@gitlab.example.com/acme/skills.git"}, "commit", t.TempDir())
	if err == nil {
		t.Fatal("scanSkillsFromGitTree with malformed LFS pointer succeeded, want error")
	}
	for _, want := range []string{
		"skills/flash/assets/tools/nvflash_eng.exe has an invalid Git LFS pointer",
		"invalid oid",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %v, want %q", err, want)
		}
	}
}

func TestPublishSkillsRejectsDuplicateExportNames(t *testing.T) {
	source := Source{ID: "source", Name: "acme", RemoteURL: "repo"}
	trees := []skillTree{
		{Name: "review", SourceDir: "skills/one", ExportName: "acme__review", Files: []treeFile{{RelPath: "SKILL.md", Mode: "100644", Data: []byte("---\nname: review\ndescription: One\n---\nbody\n")}}},
		{Name: "review", SourceDir: "skills/two", ExportName: "acme__review", Files: []treeFile{{RelPath: "SKILL.md", Mode: "100644", Data: []byte("---\nname: review\ndescription: Two\n---\nbody\n")}}},
	}
	if _, err := publishSkills(t.TempDir(), source, "commit", trees); err == nil || !containsString(err.Error(), "duplicate export directory") {
		t.Fatalf("publish duplicate export error = %v", err)
	}
}

func TestPublishSkillsRetriesTransientRenameFailure(t *testing.T) {
	root := t.TempDir()
	source := Source{ID: "source", Name: "acme", RemoteURL: "repo"}
	initial := []skillTree{{
		Name:       "review",
		SourceDir:  "skills/review",
		ExportName: "acme__review",
		Files:      []treeFile{{RelPath: "SKILL.md", Mode: "100644", Data: []byte("---\nname: review\ndescription: Review\n---\ninitial\n")}},
	}}
	if _, err := publishSkills(root, source, "commit-1", initial); err != nil {
		t.Fatalf("initial publishSkills: %v", err)
	}

	previousRename := skillPublishRename
	previousSleep := skillPublishSleep
	failures := 0
	t.Cleanup(func() {
		skillPublishRename = previousRename
		skillPublishSleep = previousSleep
	})
	skillPublishSleep = func(time.Duration) {}
	skillPublishRename = func(oldPath, newPath string) error {
		if failures == 0 && strings.Contains(newPath, ".cxp-backup-") {
			failures++
			return &os.PathError{Op: "rename", Path: oldPath, Err: fs.ErrPermission}
		}
		return os.Rename(oldPath, newPath)
	}

	updated := []skillTree{{
		Name:       "review",
		SourceDir:  "skills/review",
		ExportName: "acme__review",
		Files:      []treeFile{{RelPath: "SKILL.md", Mode: "100644", Data: []byte("---\nname: review\ndescription: Review\n---\nupdated\n")}},
	}}
	if _, err := publishSkills(root, source, "commit-2", updated); err != nil {
		t.Fatalf("publishSkills after transient rename failure: %v", err)
	}
	if failures != 1 {
		t.Fatalf("transient rename failures = %d, want 1", failures)
	}
	data, err := os.ReadFile(filepath.Join(root, "acme__review", "SKILL.md"))
	if err != nil {
		t.Fatalf("read updated skill: %v", err)
	}
	if !strings.Contains(string(data), "updated") {
		t.Fatalf("updated skill = %q, want updated content", data)
	}
}

func TestLocalChangesForManifestDetectsExecutableModeOnlyChange(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows file modes do not reliably expose executable bits")
	}
	root := t.TempDir()
	data := []byte("#!/bin/sh\necho ok\n")
	scriptPath := filepath.Join(root, "scripts", "check.sh")
	if err := os.MkdirAll(filepath.Dir(scriptPath), 0o755); err != nil {
		t.Fatalf("mkdir script dir: %v", err)
	}
	if err := os.WriteFile(scriptPath, data, 0o644); err != nil {
		t.Fatalf("write script: %v", err)
	}
	sum := sha256.Sum256(data)
	manifest := exportManifest{Files: []FileManifest{{
		RelPath: "scripts/check.sh",
		SHA256:  hex.EncodeToString(sum[:]),
		Size:    int64(len(data)),
		Mode:    0o644,
	}}}
	if err := os.Chmod(scriptPath, 0o755); err != nil {
		t.Fatalf("chmod script: %v", err)
	}
	changes, err := localChangesForManifest(root, manifest)
	if err != nil {
		t.Fatalf("localChangesForManifest: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("changes len = %d, want 1: %#v", len(changes), changes)
	}
	change := changes[0]
	if change.Kind != ChangeModified || change.OldMode != 0o644 || change.NewMode != 0o755 {
		t.Fatalf("change = %#v, want modified 0644 -> 0755", change)
	}
	if change.OldSHA256 != change.NewSHA256 {
		t.Fatalf("mode-only change should preserve sha, got old=%s new=%s", change.OldSHA256, change.NewSHA256)
	}
}

type treeListingGitRunner struct {
	listing []byte
	blobs   map[string][]byte
}

func (r treeListingGitRunner) Run(_ context.Context, _ string, _ []string, args ...string) ([]byte, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("empty args")
	}
	switch args[0] {
	case "ls-tree":
		return r.listing, nil
	case "cat-file":
		if len(args) >= 3 {
			return r.blobs[args[2]], nil
		}
	}
	return nil, fmt.Errorf("unexpected args: %v", args)
}

type lfsTreeGitRunner struct {
	listing        []byte
	blobs          map[string][]byte
	lfsOID         string
	lfsData        []byte
	lfsErr         error
	managedDir     string
	lfsFetches     int
	usedManagedEnv bool
}

func (r *lfsTreeGitRunner) Run(_ context.Context, dir string, env []string, args ...string) ([]byte, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("empty args")
	}
	switch args[0] {
	case "ls-tree":
		return r.listing, nil
	case "cat-file":
		if len(args) >= 3 {
			return r.blobs[args[2]], nil
		}
	case "config":
		return nil, nil
	case "lfs":
		if len(args) >= 2 && args[1] == "fetch" {
			r.lfsFetches++
			managed := r.managedDir != "" && envHasPathDir(env, r.managedDir)
			if managed {
				r.usedManagedEnv = true
			}
			if r.lfsErr != nil && !managed {
				return nil, r.lfsErr
			}
			objectPath, err := gitLFSObjectPath(dir, r.lfsOID)
			if err != nil {
				return nil, err
			}
			if err := os.MkdirAll(filepath.Dir(objectPath), 0o700); err != nil {
				return nil, err
			}
			return nil, os.WriteFile(objectPath, r.lfsData, 0o600)
		}
	}
	return nil, fmt.Errorf("unexpected args: %v", args)
}

func stubManagedGitLFSInstaller(t *testing.T, managedDir string, installErr error) func() {
	t.Helper()
	previous := managedGitLFSInstaller
	managedGitLFSInstaller = func(_ context.Context, toolsRoot string) (string, error) {
		if strings.TrimSpace(toolsRoot) == "" {
			t.Fatal("managed git-lfs installer got empty tools root")
		}
		return managedDir, installErr
	}
	return func() {
		managedGitLFSInstaller = previous
	}
}

func envHasPathDir(env []string, want string) bool {
	want = filepath.Clean(want)
	for _, kv := range env {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !strings.EqualFold(name, "PATH") {
			continue
		}
		for _, dir := range filepath.SplitList(value) {
			if filepath.Clean(dir) == want {
				return true
			}
		}
	}
	return false
}

func containsString(s string, sub string) bool {
	return strings.Contains(s, sub)
}
