package skills

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestApplyLocalChangeToRepoPreservesExecutableMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows git worktrees do not reliably preserve executable bits")
	}
	repo := t.TempDir()
	target := t.TempDir()
	writePushTestFile(t, filepath.Join(target, "scripts", "check.sh"), "#!/bin/sh\necho changed\n", 0o755)
	change := LocalChange{
		Kind:       ChangeModified,
		RelPath:    "scripts/check.sh",
		SourcePath: "skills/review/scripts/check.sh",
		NewSHA256:  "",
		Skill: InstalledSkill{
			TargetPath: target,
			Files: []FileManifest{{
				RelPath: "scripts/check.sh",
				Mode:    0o755,
			}},
		},
	}
	if err := applyLocalChangeToRepo(change, repo); err != nil {
		t.Fatalf("applyLocalChangeToRepo: %v", err)
	}
	info, err := os.Stat(filepath.Join(repo, "skills", "review", "scripts", "check.sh"))
	if err != nil {
		t.Fatalf("stat applied file: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("applied mode = %v, want executable bit", info.Mode().Perm())
	}
}

func TestApplyLocalChangeToRepoUsesReviewedLocalMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows git worktrees do not reliably preserve executable bits")
	}
	repo := t.TempDir()
	target := t.TempDir()
	writePushTestFile(t, filepath.Join(target, "scripts", "check.sh"), "#!/bin/sh\necho changed\n", 0o755)
	change := LocalChange{
		Kind:       ChangeModified,
		RelPath:    "scripts/check.sh",
		SourcePath: "skills/review/scripts/check.sh",
		NewMode:    0o755,
		Skill: InstalledSkill{
			TargetPath: target,
			Files: []FileManifest{{
				RelPath: "scripts/check.sh",
				Mode:    0o644,
			}},
		},
	}
	if err := applyLocalChangeToRepo(change, repo); err != nil {
		t.Fatalf("applyLocalChangeToRepo: %v", err)
	}
	info, err := os.Stat(filepath.Join(repo, "skills", "review", "scripts", "check.sh"))
	if err != nil {
		t.Fatalf("stat applied file: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("applied mode = %v, want reviewed executable bit", info.Mode().Perm())
	}
}

func writePushTestFile(t *testing.T, path string, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
