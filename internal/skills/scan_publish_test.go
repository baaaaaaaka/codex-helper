package skills

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
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
			_, err := scanSkillsFromGitTree(context.Background(), treeListingGitRunner{listing: []byte(tc.listing)}, "mirror", Source{Name: "acme", Path: "skills"}, "commit")
			if err == nil || !containsString(err.Error(), tc.want) {
				t.Fatalf("scan error = %v, want %q", err, tc.want)
			}
		})
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

func containsString(s string, sub string) bool {
	return strings.Contains(s, sub)
}
