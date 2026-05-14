package skills

import "testing"

func TestParseURLGitHubFolder(t *testing.T) {
	info, err := ParseURL("https://github.com/acme/codex-skills/tree/main/skills/review", URLParseOptions{})
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if info.Provider != "github" {
		t.Fatalf("provider = %q", info.Provider)
	}
	if info.RemoteURL != "https://github.com/acme/codex-skills.git" {
		t.Fatalf("remote = %q", info.RemoteURL)
	}
	if info.Ref != "main" || info.Path != "skills/review" {
		t.Fatalf("ref/path = %q/%q", info.Ref, info.Path)
	}
	if info.Name != "acme-codex-skills" {
		t.Fatalf("name = %q", info.Name)
	}
}

func TestParseURLGitHubBranchWithSlashUsesKnownRefs(t *testing.T) {
	info, err := ParseURL("https://github.com/acme/codex-skills/tree/feature/skills-pack/skills", URLParseOptions{
		KnownRefs: []string{"main", "feature/skills-pack"},
	})
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if info.Ref != "feature/skills-pack" || info.Path != "skills" {
		t.Fatalf("ref/path = %q/%q", info.Ref, info.Path)
	}
}

func TestParseURLGitLabFolder(t *testing.T) {
	info, err := ParseURL("https://gitlab.com/acme/platform/codex-skills/-/tree/main/skills", URLParseOptions{})
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if info.Provider != "gitlab" {
		t.Fatalf("provider = %q", info.Provider)
	}
	if info.RemoteURL != "https://gitlab.com/acme/platform/codex-skills.git" {
		t.Fatalf("remote = %q", info.RemoteURL)
	}
	if info.Ref != "main" || info.Path != "skills" {
		t.Fatalf("ref/path = %q/%q", info.Ref, info.Path)
	}
}

func TestParseURLSCPStyleGitRemoteAndOverrides(t *testing.T) {
	info, err := ParseURL("git@github.com:acme/codex-skills.git", URLParseOptions{
		Name: "Team Skills",
		Ref:  "release/v1",
		Path: "/skills/review/",
	})
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if info.Provider != "github" {
		t.Fatalf("provider = %q", info.Provider)
	}
	if info.RemoteURL != "git@github.com:acme/codex-skills.git" {
		t.Fatalf("remote = %q", info.RemoteURL)
	}
	if info.Name != "team-skills" || info.Ref != "release/v1" || info.Path != "skills/review" {
		t.Fatalf("name/ref/path = %q/%q/%q", info.Name, info.Ref, info.Path)
	}
}

func TestValidateRepoRelPathRejectsUnsafePaths(t *testing.T) {
	for _, path := range []string{
		"../SKILL.md",
		"/tmp/SKILL.md",
		`skills\review\SKILL.md`,
		"skills/CON/SKILL.md",
		"skills/a:stream/SKILL.md",
	} {
		if err := validateRepoRelPath(path); err == nil {
			t.Fatalf("validateRepoRelPath(%q) = nil, want error", path)
		}
	}
}
