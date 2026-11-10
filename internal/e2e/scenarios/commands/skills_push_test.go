package commands

import (
	"database/sql"
	"fmt"
	_ "modernc.org/sqlite"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// skillRemote creates a local bare Git repository holding one skill (the
// subscription's remote; nothing leaves the container). Its commit is
// fixed, so the review branch names it with the same short SHA every run.
func skillRemote(t *testing.T, root string) (bare string, git func(args ...string) string) {
	t.Helper()
	env := append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME=e2e", "GIT_AUTHOR_EMAIL=e2e@example.invalid", "GIT_COMMITTER_NAME=e2e", "GIT_COMMITTER_EMAIL=e2e@example.invalid",
		"GIT_AUTHOR_DATE=2026-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2026-01-01T00:00:00Z")
	git = func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	work := filepath.Join(root, "skill-src")
	if err := os.MkdirAll(filepath.Join(work, "skills", "demo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "skills", "demo", "SKILL.md"),
		[]byte("---\nname: demo\ndescription: E2E demo skill\n---\n\nSay hello.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("-C", work, "init", "-q", "-b", "main")
	git("-C", work, "add", "-A")
	git("-C", work, "commit", "-q", "-m", "demo skill")
	bare = filepath.Join(root, "remote", "demo.git")
	git("clone", "-q", "--bare", work, bare)
	return bare, git
}

// SKILLS-PUSH (F-CTL-17): v0.1.20's `helper skills push` from Teams: the
// review lists the local edits of a subscribed skill and is kept per chat
// (across a restart); `helper skills push confirm` commits and pushes
// exactly the reviewed edits to a review branch, once; a second confirm
// finds no review; a review the edits no longer match is refused as
// stale and pushes nothing.
func TestSkillsPushFromTeams(t *testing.T) {
	harness.NeedsCLI(t) // the skills backend is wired in the cxp entry point
	harness.Parallel(t)
	s := harness.NewScenario(t, "SKILLS-PUSH", harness.Options{
		Guarantees: []string{"F-CTL-17"},
		Title:      "`helper skills push` reviews local skill edits in Teams, `helper skills push confirm` pushes them once to a review branch",
		Budget:     200 * time.Second,
	})
	defer s.Finish()
	bare, git := skillRemote(t, s.Env.Root)
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "skillpush")

	expect(s, control.ID, "helper skills add file://"+bare, "Added demo", "Installed 1 skill(s).")
	expect(s, control.ID, "helper skills push", "No local skill changes to push.")

	installed, _ := filepath.Glob(filepath.Join(s.Env.Home, ".agents", "skills", "*", "SKILL.md"))
	skillFile := ""
	for _, f := range installed {
		if raw, err := os.ReadFile(f); err == nil && strings.Contains(string(raw), "E2E demo skill") {
			skillFile = f
		}
	}
	if skillFile == "" {
		t.Fatalf("installed demo skill not found among %v", installed)
	}
	edit := func(line string) {
		f, err := os.OpenFile(skillFile, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString(line + "\n")
		_ = f.Close()
	}
	branches := func() []string {
		out := git("--git-dir", bare, "for-each-ref", "--format=%(refname)", "refs/heads/skill/")
		if out == "" {
			return nil
		}
		return strings.Split(out, "\n")
	}

	edit("Also say goodbye.")
	expect(s, control.ID, "helper skills push", "Skills Push Review", "A review-branch push is pending.",
		"then send helper skills push confirm to commit and push from Teams.", "Expires:", "demo",
		"review target: HEAD:refs/heads/skill/demo-", "MODIFIED skills/demo/SKILL.md")
	s.Check("the review pushes nothing", len(branches()) == 0, "no branch", strings.Join(branches(), ","))
	expect(s, work.ID, "helper skills push confirm", "No pending skills push review. Run helper skills push [name] first.")

	// The review survives a restart.
	s.StopSUT()
	dbPath := filepath.Join(s.Env.ConfigDir, "codex-proxy", "skills.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM push_reviews WHERE chat=?`, control.ID).Scan(&count); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	s.Check("review is a native SQLite row before restart", count == 1, "one review", fmt.Sprint(count))
	if err = os.WriteFile(filepath.Join(filepath.Dir(dbPath), "teams-skill-push-reviews.json"), []byte("poisoned legacy review"), 0600); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	expect(s, control.ID, "helper skills push confirm", "Skills Push", "Mode: review branch",
		"demo: pushed HEAD:refs/heads/skill/demo-", "Final staged diff for demo:", "Pushed HEAD:refs/heads/skill/demo-")
	got := branches()
	content := ""
	if len(got) == 1 {
		content = git("--git-dir", bare, "show", got[0]+":skills/demo/SKILL.md")
	}
	s.Check("confirm pushed the reviewed edit to one review branch", len(got) == 1 && strings.Contains(content, "Also say goodbye."),
		"1 skill/ branch with the edit", fmt.Sprintf("%v: %q", got, clip(content, 120)))
	s.Check("the subscribed branch is unchanged", !strings.Contains(git("--git-dir", bare, "show", "main:skills/demo/SKILL.md"), "goodbye"),
		"main without the edit", "")
	expect(s, control.ID, "helper skills push confirm", "No pending skills push review.")
	s.Check("a second confirm pushes nothing", len(branches()) == 1, "1 branch", strings.Join(branches(), ","))

	// A review the local edits no longer match is stale.
	edit("And wave.")
	expect(s, work.ID, "helper skills push", "Skills Push Review", "MODIFIED skills/demo/SKILL.md")
	edit("And smile.")
	expect(s, work.ID, "helper skills push confirm", "Pending skills push review is stale:", "Run helper skills push [name] again.")
	s.Check("a stale review pushes nothing", len(branches()) == 1, "1 branch", strings.Join(branches(), ","))

	calls := 0
	for _, c := range s.Codex.Calls() {
		if strings.Contains(c.Prompt, "skills") {
			calls++
		}
	}
	s.Check("skills commands start no Codex turn", calls == 0, "0 turns", fmt.Sprint(calls))
}
