package skills

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestInstallBuiltinsPublishesCxpSkill(t *testing.T) {
	mgr := newBuiltinTestManager(t)

	results, err := mgr.InstallBuiltins(context.Background(), BuiltinInstallOptions{Name: BuiltinCxpSkillName})
	if err != nil {
		t.Fatalf("InstallBuiltins: %v", err)
	}
	if len(results) != 1 || len(results[0].Installed) != 1 {
		t.Fatalf("results = %#v, want one installed skill", results)
	}
	installed := results[0].Installed[0]
	if installed.Name != BuiltinCxpSkillName || installed.ExportName != BuiltinCxpSkillName {
		t.Fatalf("installed skill = %#v", installed)
	}
	if _, err := os.Stat(filepath.Join(installed.TargetPath, "SKILL.md")); err != nil {
		t.Fatalf("installed SKILL.md missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(installed.TargetPath, "references", "commands.md")); err != nil {
		t.Fatalf("installed command reference missing: %v", err)
	}
	manifest, ok, err := readExportManifest(filepath.Join(installed.TargetPath, manifestFilename))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !ok {
		t.Fatal("manifest missing")
	}
	if manifest.SourceID != builtinCxpSourceID || manifest.SourceName != BuiltinCxpSkillName || manifest.RemoteURL != "builtin://cxp" {
		t.Fatalf("manifest source fields = %#v", manifest)
	}
	if !strings.HasPrefix(manifest.Commit, "builtin:cxp:") {
		t.Fatalf("manifest commit = %q", manifest.Commit)
	}
}

func TestInstallBuiltinsRefusesLocalModifications(t *testing.T) {
	mgr := newBuiltinTestManager(t)
	results, err := mgr.InstallBuiltins(context.Background(), BuiltinInstallOptions{})
	if err != nil {
		t.Fatalf("initial InstallBuiltins: %v", err)
	}
	target := results[0].Installed[0].TargetPath
	if err := os.WriteFile(filepath.Join(target, "SKILL.md"), []byte("---\nname: cxp\ndescription: local\n---\nlocal edit\n"), 0o644); err != nil {
		t.Fatalf("write local modification: %v", err)
	}

	_, err = mgr.InstallBuiltins(context.Background(), BuiltinInstallOptions{})
	if err == nil || !strings.Contains(err.Error(), "local modifications") {
		t.Fatalf("InstallBuiltins with local edit error = %v, want local modifications", err)
	}
	if strings.Contains(err.Error(), "cxp skills push") {
		t.Fatalf("builtin local edit guidance should not suggest git-backed skill push: %v", err)
	}
	data, readErr := os.ReadFile(filepath.Join(target, "SKILL.md"))
	if readErr != nil {
		t.Fatalf("read local SKILL.md: %v", readErr)
	}
	if !strings.Contains(string(data), "local edit") {
		t.Fatalf("local edit was overwritten:\n%s", string(data))
	}
}

func TestInstallBuiltinsRefusesUnmanagedExistingSkill(t *testing.T) {
	mgr := newBuiltinTestManager(t)
	target := filepath.Join(mgr.HomeDir, ".agents", "skills", BuiltinCxpSkillName)
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatalf("mkdir unmanaged target: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "SKILL.md"), []byte("---\nname: cxp\ndescription: user\n---\nuser skill\n"), 0o644); err != nil {
		t.Fatalf("write unmanaged SKILL.md: %v", err)
	}

	_, err := mgr.InstallBuiltins(context.Background(), BuiltinInstallOptions{})
	if err == nil || !strings.Contains(err.Error(), "already exists and is not managed") {
		t.Fatalf("InstallBuiltins unmanaged target error = %v, want unmanaged target refusal", err)
	}
	data, readErr := os.ReadFile(filepath.Join(target, "SKILL.md"))
	if readErr != nil {
		t.Fatalf("read unmanaged SKILL.md: %v", readErr)
	}
	if !strings.Contains(string(data), "user skill") {
		t.Fatalf("unmanaged skill was overwritten:\n%s", string(data))
	}
}

func TestBuiltinCxpSkillDocumentsCommandMapAndDisruptiveHandoffs(t *testing.T) {
	files := builtinCxpSkillTestFiles(t)
	skill := files["SKILL.md"]
	reference := files["references/commands.md"]
	for _, want := range []string{
		"$CODEX_HELPER_CLI_PATH",
		"cxp beacon switch-profile <profile> --session <session-id> --after-current-turn",
		"helper reload now",
		"helper restart now",
		"do not restart, reload, update, kill, replace, or background the helper from a child Codex turn",
		"source-checkout development reload",
	} {
		if !strings.Contains(skill, want) {
			t.Fatalf("SKILL.md missing %q:\n%s", want, skill)
		}
	}
	for _, want := range []string{
		"cxp app [profile]",
		"cxp app auth [profile]",
		"cxp app --model-profile <name>",
		"cxp run [profile] -- <cmd args...>",
		"cxp run --yolo -- codex",
		"cxp run --model-profile <name> -- codex",
		"cxp init",
		"cxp model list",
		"cxp model-profile setup [name]",
		"cxp responses serve",
		"cxp proxy doctor",
		"cxp proxy reset",
		"cxp beacon profile create <name> --provider slurm",
		"cxp beacon switch-profile <name> --session <session-id> --after-current-turn",
		"cxp teams service bootstrap",
		"cxp teams service restart --force",
		"cxp teams workflow status|enable|disable|test",
		"cxp teams send-file <path> --session <session-id>",
		"cxp teams probe-chat --chat <teams-chat-id-or-link>",
		"cxp teams pause",
		"cxp teams chat recreate <session-id> --yes",
		"new <directory> --model <profile>",
		"model status",
		"helper update prerelease",
		"cxp skills install-builtin",
		"cxp skills migrate",
		"cxp history tui",
		"cxp upgrade --include-prerelease",
		"Built-in skills are installed into `$HOME/.agents/skills`, but they are not git subscriptions",
	} {
		if !strings.Contains(reference, want) {
			t.Fatalf("commands.md missing %q:\n%s", want, reference)
		}
	}
}

func TestBuiltinCxpSkillTriggerDescriptionCoversImplicitRequestsWithoutTokenBloat(t *testing.T) {
	files := builtinCxpSkillTestFiles(t)
	skill := files["SKILL.md"]
	description := builtinSkillDescriptionForTest(t, skill)
	for _, want := range []string{
		"codex-helper",
		"proxy/SSH profiles",
		"model profiles/Responses adapter/YOLO mode",
		"Teams helper/control/work chats",
		"Codex history or skills",
		"upgrades",
		"beacon/execution target/profile switching",
		"Slurm/LSF/GPU/local execution",
		"interrupt Codex",
	} {
		if !strings.Contains(description, want) {
			t.Fatalf("frontmatter description missing trigger %q:\n%s", want, description)
		}
	}
	for _, want := range []string{
		"even when the user does not say `cxp`",
		"model profiles",
		"Responses adapter",
		"YOLO mode",
		"execution target/profile switching",
		"GPU/Slurm/LSF/local execution",
		"For the command map and workflows, load `references/commands.md`",
	} {
		if !strings.Contains(skill, want) {
			t.Fatalf("SKILL.md missing progressive trigger guidance %q:\n%s", want, skill)
		}
	}
	if got, max := len([]rune(skill)), 2800; got > max {
		t.Fatalf("SKILL.md length = %d runes, want <= %d so the trigger stays token-efficient", got, max)
	}
}

func TestBuiltinCxpSkillScenarioMatrix(t *testing.T) {
	files := builtinCxpSkillTestFiles(t)
	all := files["SKILL.md"] + "\n" + files["references/commands.md"]
	scenarios := []struct {
		name     string
		required []string
	}{
		{
			name: "teams child with cxp missing from path",
			required: []string{
				"$CODEX_HELPER_CLI_PATH",
				"`cxp` is not visible in the current process PATH",
				"`type cxp`, `command -v cxp`, and `echo $PATH`",
			},
		},
		{
			name: "beacon profile setup is not proxy setup",
			required: []string{
				"Proxy profiles are SSH/network profiles",
				"They are not beacon execution profiles",
				"Use `cxp proxy` only when the user is asking about SSH/network routing",
				"If the user asks for beacon mode or beacon profiles, use `cxp beacon ...`",
			},
		},
		{
			name: "model profiles responses and yolo mode",
			required: []string{
				"cxp run --yolo -- codex",
				"cxp run --model-profile <name> -- codex",
				"cxp model list",
				"cxp model-profile setup [name]",
				"cxp responses serve",
				"new <directory> --model <profile>",
				"model status",
				"model switch <profile>",
				"model fork <profile>",
				"Ctrl+Y",
				"Ctrl+K",
			},
		},
		{
			name: "slurm lsf and local beacon profile creation",
			required: []string{
				"cxp beacon profile create <name> --provider slurm",
				"cxp beacon profile create <name> --provider lsf",
				"cxp beacon profile create <name> --provider local",
				"cxp beacon profile doctor <name>",
				"cxp beacon profile confirm <name>",
			},
		},
		{
			name: "beacon worker adapter troubleshooting",
			required: []string{
				"--shared-store",
				"--codex-path <codex-or-wrapper>",
				"--no-yolo",
				"--skip-git-repo-check",
				"CXP_BEACON_CODEX_BIN",
				"launch Codex in yolo mode by default",
				"Keep exactly one `exec`",
				"`--adapter-shell direct`",
				"user-shell capture is incompatible",
			},
		},
		{
			name: "active codex beacon switch uses deferred pending target",
			required: []string{
				"Do not run these inline from an active Codex turn",
				"cxp beacon switch-profile <name> --session <session-id> --after-current-turn",
				"the current Codex turn stays on its existing target and future turns use the new profile",
			},
		},
		{
			name: "incompatible signature requires user-approved fork",
			required: []string{
				"If the command reports an incompatible execution signature, ask whether to fork before using `--fork`",
				"cxp beacon switch-profile <name> --session <session-id> --fork",
			},
		},
		{
			name: "teams helper lifecycle is control chat handoff",
			required: []string{
				"helper reload now",
				"helper restart now",
				"helper update now",
				"do not restart, reload, update, kill, replace, or background the helper from a child Codex turn",
				"source-checkout development",
			},
		},
		{
			name: "auth destructive and skill push stay local",
			required: []string{
				"For auth prompts, destructive confirmations, and skill pushes",
				"direct the user to run the local `cxp ...` command in their terminal",
				"cxp skills push [name]",
			},
		},
		{
			name: "machine hard kill requires exact confirmation token",
			required: []string{
				"cxp beacon machine status <machine-or-lease>",
				"cxp beacon machine kill <machine-or-lease-or-job> --confirm <token>",
				"hard kill only after using the exact token from status",
			},
		},
		{
			name: "upgrade in teams child is not self managed",
			required: []string{
				"cxp upgrade --include-prerelease",
				"From a Teams child turn, do not self-manage the running helper process",
				"Use control chat update or restart commands for installed helpers",
			},
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			for _, want := range scenario.required {
				if !strings.Contains(all, want) {
					t.Fatalf("scenario %q missing %q", scenario.name, want)
				}
			}
		})
	}
}

func TestInstallBuiltinsRejectsUnknownName(t *testing.T) {
	mgr := newBuiltinTestManager(t)
	if _, err := mgr.InstallBuiltins(context.Background(), BuiltinInstallOptions{Name: "missing"}); err == nil {
		t.Fatal("InstallBuiltins unknown name succeeded, want error")
	}
}

func newBuiltinTestManager(t *testing.T) *Manager {
	t.Helper()
	root := t.TempDir()
	mgr, err := NewManager(ManagerOptions{
		ConfigDir: filepath.Join(root, "config"),
		CacheDir:  filepath.Join(root, "cache"),
		CodexDir:  filepath.Join(root, "codex"),
		HomeDir:   filepath.Join(root, "home"),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

func builtinCxpSkillTestFiles(t *testing.T) map[string]string {
	t.Helper()
	tree, _, err := builtinSkillTree(BuiltinCxpSkillName)
	if err != nil {
		t.Fatalf("builtinSkillTree: %v", err)
	}
	files := map[string]string{}
	for _, file := range tree.Files {
		files[file.RelPath] = string(file.Data)
	}
	if files["SKILL.md"] == "" || files["references/commands.md"] == "" {
		t.Fatalf("builtin cxp skill files = %v, want SKILL.md and references/commands.md", sortedBuiltinTestKeys(files))
	}
	return files
}

func builtinSkillDescriptionForTest(t *testing.T, skill string) string {
	t.Helper()
	if !strings.HasPrefix(skill, "---\n") {
		t.Fatalf("skill missing YAML frontmatter:\n%s", skill)
	}
	rest := strings.TrimPrefix(skill, "---\n")
	frontmatter, _, ok := strings.Cut(rest, "\n---")
	if !ok {
		t.Fatalf("skill frontmatter is not closed:\n%s", skill)
	}
	for _, line := range strings.Split(frontmatter, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "description:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "description:"))
		}
	}
	t.Fatalf("skill frontmatter has no description:\n%s", frontmatter)
	return ""
}

func sortedBuiltinTestKeys(files map[string]string) []string {
	keys := make([]string, 0, len(files))
	for key := range files {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
