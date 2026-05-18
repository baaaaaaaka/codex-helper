package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/skills"
)

func TestSkillsCommandAddListDoctorSyncRemoveAgentsTargetCombo(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	home := t.TempDir()
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	effectivePathsRunningAsRoot = func() bool { return false }
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("CODEX_DIR", "")
	t.Setenv("CODEX_HOME", "")

	repo := initCLISkillRepo(t)
	configPath := filepath.Join(t.TempDir(), "config.json")
	codexDir := filepath.Join(t.TempDir(), "codex")

	out, err := runSkillsRootCommand(t,
		"--config", configPath,
		"skills", "--codex-dir", codexDir,
		"add", repo,
		"--name", "acme",
		"--ref", "HEAD",
		"--path", "skills/review",
		"--target", skills.TargetAgents,
		"--no-auto-sync",
		"--yes",
	)
	if err != nil {
		t.Fatalf("skills add: %v\n%s", err, out)
	}
	agentSkill := filepath.Join(home, ".agents", "skills", "acme__review")
	if _, err := os.Stat(filepath.Join(agentSkill, "SKILL.md")); err != nil {
		t.Fatalf("agents target SKILL.md missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(codexDir, "skills", "acme__review")); !os.IsNotExist(err) {
		t.Fatalf("codex target should not contain agents skill, stat err=%v", err)
	}

	listOut, err := runSkillsRootCommand(t,
		"--config", configPath,
		"skills", "--codex-dir", codexDir,
		"list", "--json",
	)
	if err != nil {
		t.Fatalf("skills list --json: %v\n%s", err, listOut)
	}
	var entries []skills.StatusEntry
	if err := json.Unmarshal([]byte(listOut), &entries); err != nil {
		t.Fatalf("parse list json: %v\n%s", err, listOut)
	}
	if len(entries) != 1 || entries[0].Source.TargetKind != skills.TargetAgents || entries[0].Source.AutoSync {
		t.Fatalf("list entries = %#v", entries)
	}

	doctorOut, err := runSkillsRootCommand(t,
		"--config", configPath,
		"skills", "--codex-dir", codexDir,
		"doctor", "acme",
	)
	if err != nil {
		t.Fatalf("skills doctor: %v\n%s", err, doctorOut)
	}
	if !strings.Contains(doctorOut, filepath.Join(home, ".agents", "skills")) {
		t.Fatalf("doctor output missing agents root:\n%s", doctorOut)
	}

	syncOut, err := runSkillsRootCommand(t,
		"--config", configPath,
		"skills", "--codex-dir", codexDir,
		"sync", "acme",
	)
	if err != nil {
		t.Fatalf("skills sync acme: %v\n%s", err, syncOut)
	}
	if !strings.Contains(syncOut, "acme: synced 1 skill") {
		t.Fatalf("sync output = %s", syncOut)
	}

	writeCLIFile(t, filepath.Join(agentSkill, "notes.md"), "local note\n", 0o644)
	removeOut, err := runSkillsRootCommand(t,
		"--config", configPath,
		"skills", "--codex-dir", codexDir,
		"remove", "acme", "--yes",
	)
	if err == nil {
		t.Fatalf("skills remove with local changes succeeded, want error\n%s", removeOut)
	}
	if !strings.Contains(err.Error(), "local modifications") {
		t.Fatalf("remove error missing local modifications: %v", err)
	}
	if _, err := os.Stat(filepath.Join(agentSkill, "SKILL.md")); err != nil {
		t.Fatalf("agents skill was removed despite local changes: %v", err)
	}
}

func TestNewSkillsManagerUsesEffectivePaths(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	root := t.TempDir()
	home := filepath.Join(root, "home")
	cache := filepath.Join(root, "cache")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CACHE_HOME", cache)
	t.Setenv("LOCALAPPDATA", cache)
	t.Setenv("CODEX_DIR", "")
	t.Setenv("CODEX_HOME", "")
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	effectivePathsRunningAsRoot = func() bool { return false }

	configPath := filepath.Join(root, "config", "custom.json")
	codexDir := filepath.Join(root, "codex-home")
	mgr, err := newSkillsManager(&rootOptions{configPath: configPath}, codexDir, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("newSkillsManager: %v", err)
	}
	wantCache, err := defaultSkillsCacheDir()
	if err != nil {
		t.Fatalf("default skills cache dir: %v", err)
	}
	if mgr.ConfigDir != filepath.Dir(configPath) {
		t.Fatalf("ConfigDir = %q, want %q", mgr.ConfigDir, filepath.Dir(configPath))
	}
	if mgr.CacheDir != wantCache {
		t.Fatalf("CacheDir = %q, want %q", mgr.CacheDir, wantCache)
	}
	if mgr.CodexDir != filepath.Clean(codexDir) {
		t.Fatalf("CodexDir = %q, want %q", mgr.CodexDir, filepath.Clean(codexDir))
	}
	if mgr.HomeDir != filepath.Clean(home) {
		t.Fatalf("HomeDir = %q, want %q", mgr.HomeDir, filepath.Clean(home))
	}
}

func TestSkillsCommandInstallBuiltinCxp(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	home := t.TempDir()
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	effectivePathsRunningAsRoot = func() bool { return false }
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("CODEX_DIR", "")
	t.Setenv("CODEX_HOME", "")

	configPath := filepath.Join(t.TempDir(), "config.json")
	codexDir := filepath.Join(t.TempDir(), "codex")
	out, err := runSkillsRootCommand(t,
		"--config", configPath,
		"skills", "--codex-dir", codexDir,
		"install-builtin", "cxp",
		"--yes",
	)
	if err != nil {
		t.Fatalf("skills install-builtin: %v\n%s", err, out)
	}
	installed := filepath.Join(codexDir, "skills", "cxp")
	if _, err := os.Stat(filepath.Join(installed, "SKILL.md")); err != nil {
		t.Fatalf("builtin SKILL.md missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(installed, "references", "commands.md")); err != nil {
		t.Fatalf("builtin command reference missing: %v", err)
	}
	if !strings.Contains(out, "Installed bundled skill cxp -> "+installed) {
		t.Fatalf("install-builtin output = %s", out)
	}

	listOut, err := runSkillsRootCommand(t,
		"--config", configPath,
		"skills", "--codex-dir", codexDir,
		"list",
	)
	if err != nil {
		t.Fatalf("skills list: %v\n%s", err, listOut)
	}
	if !strings.Contains(listOut, "No skill subscriptions.") {
		t.Fatalf("builtin skill should not appear as a git subscription:\n%s", listOut)
	}
}

func TestRunSkillsPushPushesOnlyConfirmedChangesToReviewBranch(t *testing.T) {
	repo := initCLISkillRepo(t)
	mgr := newCLISkillsManager(t)
	ctx := context.Background()
	_, result, err := mgr.Add(ctx, repo, skills.AddOptions{Name: "acme", Ref: "HEAD", Path: "skills/review"})
	if err != nil {
		t.Fatalf("add source: %v", err)
	}
	if len(result.Installed) != 1 {
		t.Fatalf("installed len = %d, want 1", len(result.Installed))
	}
	target := result.Installed[0].TargetPath
	writeCLIFile(t, filepath.Join(target, "SKILL.md"), "---\nname: review\ndescription: Updated review\n---\nupdated body\n", 0o644)
	writeCLIFile(t, filepath.Join(target, "scripts", "check.sh"), "#!/bin/sh\necho changed\n", 0o755)

	var out bytes.Buffer
	input := strings.NewReader("y\nn\ny\ny\n")
	if err := runSkillsPush(ctx, mgr, "", false, input, &out); err != nil {
		t.Fatalf("runSkillsPush: %v\n%s", err, out.String())
	}
	branch := singleReviewBranch(t, repo)
	gotSkill := cliGitRun(t, repo, "show", branch+":skills/review/SKILL.md")
	if !strings.Contains(gotSkill, "updated body") {
		t.Fatalf("review branch SKILL.md was not updated:\n%s", gotSkill)
	}
	gotScript := cliGitRun(t, repo, "show", branch+":skills/review/scripts/check.sh")
	if strings.Contains(gotScript, "changed") {
		t.Fatalf("unconfirmed script change was pushed:\n%s", gotScript)
	}
	if !strings.Contains(out.String(), "Pushed HEAD:refs/heads/skill/") {
		t.Fatalf("push output missing review branch:\n%s", out.String())
	}
}

func TestRunSkillsPushAddedDeletedAndFinalDeclineFlows(t *testing.T) {
	repo := initCLISkillRepo(t)
	mgr := newCLISkillsManager(t)
	ctx := context.Background()
	_, result, err := mgr.Add(ctx, repo, skills.AddOptions{Name: "acme", Ref: "HEAD", Path: "skills/review"})
	if err != nil {
		t.Fatalf("add source: %v", err)
	}
	target := result.Installed[0].TargetPath
	writeCLIFile(t, filepath.Join(target, "agents", "openai.yaml"), "version: 1\n", 0o644)
	if err := os.Remove(filepath.Join(target, "scripts", "check.sh")); err != nil {
		t.Fatalf("remove local script: %v", err)
	}

	var declineOut bytes.Buffer
	if err := runSkillsPush(ctx, mgr, "", false, strings.NewReader("y\ny\nn\n"), &declineOut); err != nil {
		t.Fatalf("runSkillsPush final decline: %v\n%s", err, declineOut.String())
	}
	if branches := reviewBranches(t, repo); len(branches) != 0 {
		t.Fatalf("review branches after declined push = %v, want none", branches)
	}

	var pushOut bytes.Buffer
	if err := runSkillsPush(ctx, mgr, "", false, strings.NewReader("y\ny\ny\ny\n"), &pushOut); err != nil {
		t.Fatalf("runSkillsPush added/deleted: %v\n%s", err, pushOut.String())
	}
	branch := singleReviewBranch(t, repo)
	gotAgent := cliGitRun(t, repo, "show", branch+":skills/review/agents/openai.yaml")
	if gotAgent != "version: 1\n" {
		t.Fatalf("pushed agent sidecar = %q", gotAgent)
	}
	if _, err := cliGitRunErr(repo, "show", branch+":skills/review/scripts/check.sh"); err == nil {
		t.Fatal("deleted script still exists on review branch")
	}
}

func TestRunSkillsPushNoConfirmedChangesDoesNotCreateBranch(t *testing.T) {
	repo := initCLISkillRepo(t)
	mgr := newCLISkillsManager(t)
	ctx := context.Background()
	_, result, err := mgr.Add(ctx, repo, skills.AddOptions{Name: "acme", Ref: "HEAD", Path: "skills/review"})
	if err != nil {
		t.Fatalf("add source: %v", err)
	}
	writeCLIFile(t, filepath.Join(result.Installed[0].TargetPath, "SKILL.md"), "---\nname: review\ndescription: Updated\n---\nupdated\n", 0o644)

	var out bytes.Buffer
	if err := runSkillsPush(ctx, mgr, "", false, strings.NewReader("n\n"), &out); err != nil {
		t.Fatalf("runSkillsPush: %v\n%s", err, out.String())
	}
	if branches := reviewBranches(t, repo); len(branches) != 0 {
		t.Fatalf("review branches = %v, want none", branches)
	}
	if !strings.Contains(out.String(), "Skipped acme") {
		t.Fatalf("missing skipped output:\n%s", out.String())
	}
}

func TestDirectPushRefSpecRequiresExistingBranch(t *testing.T) {
	ctx := context.Background()
	runner := directRefGitRunner{heads: map[string]bool{"main": true, "feature/work": true}}
	source := skills.Source{Name: "acme", RemoteURL: "repo", Ref: "main"}
	refspec, err := directPushRefSpec(ctx, runner, source)
	if err != nil {
		t.Fatalf("directPushRefSpec main: %v", err)
	}
	if refspec != "HEAD:refs/heads/main" {
		t.Fatalf("refspec = %q", refspec)
	}

	for _, ref := range []string{"", "HEAD", "refs/tags/v1.0.0", "0123456789abcdef0123456789abcdef01234567", "-bad", "missing"} {
		source.Ref = ref
		if _, err := directPushRefSpec(ctx, runner, source); err == nil {
			t.Fatalf("directPushRefSpec(%q) succeeded, want error", ref)
		}
	}
}

func TestRunSkillsTextMenuAddListSyncAndBackCombo(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	home := t.TempDir()
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	effectivePathsRunningAsRoot = func() bool { return false }
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("CODEX_DIR", "")
	t.Setenv("CODEX_HOME", "")

	repo := initCLISkillRepo(t)
	configPath := filepath.Join(t.TempDir(), "config.json")
	codexDir := filepath.Join(t.TempDir(), "codex")
	root := &rootOptions{configPath: configPath}
	var out bytes.Buffer
	input := strings.NewReader("2\n" + repo + "\n1\n3\n6\n")
	if err := runSkillsTextMenu(context.Background(), root, codexDir, input, &out); err != nil {
		t.Fatalf("runSkillsTextMenu: %v\n%s", err, out.String())
	}
	text := out.String()
	for _, want := range []string{"Installed 1 skill", "review ->", "synced 1 skill"} {
		if !strings.Contains(text, want) {
			t.Fatalf("menu output missing %q:\n%s", want, text)
		}
	}
	matches, err := installedReviewSkillManifests(filepath.Join(codexDir, "skills"))
	if err != nil {
		t.Fatalf("list installed skills: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("menu-installed skill matches = %v, want 1", matches)
	}
}

func TestApplySkillChangeToRepoPreservesExecutableMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows git worktrees do not reliably preserve executable bits")
	}
	repo := t.TempDir()
	target := t.TempDir()
	writeCLIFile(t, filepath.Join(target, "scripts", "check.sh"), "#!/bin/sh\necho changed\n", 0o755)
	change := skills.LocalChange{
		Kind:       skills.ChangeModified,
		RelPath:    "scripts/check.sh",
		SourcePath: "skills/review/scripts/check.sh",
		NewSHA256:  "",
		Skill: skills.InstalledSkill{
			TargetPath: target,
			Files: []skills.FileManifest{{
				RelPath: "scripts/check.sh",
				Mode:    0o755,
			}},
		},
	}
	if err := applySkillChangeToRepo(change, repo); err != nil {
		t.Fatalf("applySkillChangeToRepo: %v", err)
	}
	info, err := os.Stat(filepath.Join(repo, "skills", "review", "scripts", "check.sh"))
	if err != nil {
		t.Fatalf("stat applied file: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("applied mode = %v, want executable bit", info.Mode().Perm())
	}
}

func TestApplySkillChangeToRepoUsesReviewedLocalMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows git worktrees do not reliably preserve executable bits")
	}
	repo := t.TempDir()
	target := t.TempDir()
	writeCLIFile(t, filepath.Join(target, "scripts", "check.sh"), "#!/bin/sh\necho changed\n", 0o755)
	change := skills.LocalChange{
		Kind:       skills.ChangeModified,
		RelPath:    "scripts/check.sh",
		SourcePath: "skills/review/scripts/check.sh",
		NewMode:    0o755,
		Skill: skills.InstalledSkill{
			TargetPath: target,
			Files: []skills.FileManifest{{
				RelPath: "scripts/check.sh",
				Mode:    0o644,
			}},
		},
	}
	if err := applySkillChangeToRepo(change, repo); err != nil {
		t.Fatalf("applySkillChangeToRepo: %v", err)
	}
	info, err := os.Stat(filepath.Join(repo, "skills", "review", "scripts", "check.sh"))
	if err != nil {
		t.Fatalf("stat applied file: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("applied mode = %v, want reviewed executable bit", info.Mode().Perm())
	}
}

func runSkillsRootCommand(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func newCLISkillsManager(t *testing.T) *skills.Manager {
	t.Helper()
	root := t.TempDir()
	mgr, err := skills.NewManager(skills.ManagerOptions{
		ConfigDir: filepath.Join(root, "config"),
		CacheDir:  filepath.Join(root, "cache"),
		CodexDir:  filepath.Join(root, "codex"),
		HomeDir:   filepath.Join(root, "home"),
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr
}

func initCLISkillRepo(t *testing.T) string {
	t.Helper()
	requireCLIGit(t)
	repo := t.TempDir()
	cliGitRun(t, repo, "init")
	cliGitRun(t, repo, "config", "user.name", "Skill Test")
	cliGitRun(t, repo, "config", "user.email", "skill-test@example.invalid")
	writeCLIFile(t, filepath.Join(repo, "skills", "review", "SKILL.md"), "---\nname: review\ndescription: Review code\n---\nbody\n", 0o644)
	writeCLIFile(t, filepath.Join(repo, "skills", "review", "scripts", "check.sh"), "#!/bin/sh\necho ok\n", 0o755)
	cliGitRun(t, repo, "add", "-A")
	cliGitRun(t, repo, "commit", "-m", "initial skill")
	return repo
}

func singleReviewBranch(t *testing.T, repo string) string {
	t.Helper()
	lines := reviewBranches(t, repo)
	if len(lines) != 1 {
		t.Fatalf("review branches = %v, want 1", lines)
	}
	return lines[0]
}

func reviewBranches(t *testing.T, repo string) []string {
	t.Helper()
	out := cliGitRun(t, repo, "for-each-ref", "--format=%(refname:short)", "refs/heads/skill")
	lines := strings.Fields(out)
	return lines
}

func cliGitRun(t *testing.T, repo string, args ...string) string {
	t.Helper()
	out, err := cliGitRunErr(repo, args...)
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, string(out))
	}
	return string(out)
}

func cliGitRunErr(repo string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = repo
	return cmd.CombinedOutput()
}

func requireCLIGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
}

func writeCLIFile(t *testing.T, path string, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func installedReviewSkillManifests(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var matches []string
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasSuffix(entry.Name(), "__review") {
			continue
		}
		manifest := filepath.Join(root, entry.Name(), "SKILL.md")
		if _, err := os.Stat(manifest); err == nil {
			matches = append(matches, manifest)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	return matches, nil
}

type directRefGitRunner struct {
	heads map[string]bool
}

func (r directRefGitRunner) Run(_ context.Context, _ string, _ []string, args ...string) ([]byte, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("empty git args")
	}
	switch args[0] {
	case "check-ref-format":
		if len(args) >= 3 && args[1] == "--branch" && !strings.Contains(args[2], "..") {
			return []byte(args[2] + "\n"), nil
		}
		return nil, fmt.Errorf("invalid branch")
	case "ls-remote":
		branch := args[len(args)-1]
		if r.heads[branch] {
			return []byte("0123456789abcdef\trefs/heads/" + branch + "\n"), nil
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected git args: %v", args)
	}
}
