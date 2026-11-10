package commands

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	_ "modernc.org/sqlite"
)

func TestSkillsSQLiteLegacyUpgradeFromCLI(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "SKILLS-SQLITE-UPGRADE", harness.Options{
		Guarantees: []string{"F-CTL-17", "entry-point"}, Title: "legacy skill subscriptions import once; cold CLI reopens native state", NoStart: true, Budget: 60 * time.Second,
		Golden: harness.GoldenOptions{Skip: "native migration, archive and authoritative cold reopen asserted directly"},
	})
	defer s.Finish()
	dir := filepath.Join(s.Env.ConfigDir, "codex-proxy")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// Handwritten pre-upgrade data, independent of the current Go encoders.
	input := `{"version":1,"sources":[{"id":"old-id","name":"legacy-review","url":"file:///unused","remote_url":"file:///unused","target_kind":"agents","target_root":%q,"auto_sync":false,"added_at":"2026-11-01T00:00:00Z"}]}`
	legacy := filepath.Join(dir, "skill-subscriptions.json")
	if err := os.WriteFile(legacy, []byte(fmt.Sprintf(input, filepath.Join(s.Env.Home, ".agents", "skills"))), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "skill-subscriptions-state.json"), []byte(`{"version":1,"sources":[{"id":"old-id","status":"ready","last_commit":"original-commit","last_auto_sync_day":"2026-11-01"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		output, code := s.RunCLI(20*time.Second, "skills", "list", "--json")
		s.Check(fmt.Sprintf("cold CLI %d reads the original subscription", i+1), code == 0 && strings.Contains(output, "legacy-review") && strings.Contains(output, "original-commit"), "original name and commit", output)
		if i == 0 {
			if _, err := os.Stat(legacy + ".migrated"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(legacy, []byte("invalid leftover legacy"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "skills.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	var commit string
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT last_commit FROM skill_states WHERE id='old-id'`).Scan(&commit); err != nil {
		t.Fatal(err)
	}
	s.Check("migration retained typed native state", version == 1 && commit == "original-commit", "version 1; original commit", fmt.Sprintf("v%d %s", version, commit))
	s.Check("listing starts no Codex task", len(s.Codex.Calls()) == 0, "zero tasks", fmt.Sprint(len(s.Codex.Calls())))
}
