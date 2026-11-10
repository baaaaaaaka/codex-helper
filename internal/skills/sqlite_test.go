package skills

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSkillsSQLiteMigrationAndQuietWrites(t *testing.T) {
	dir := t.TempDir()
	// Frozen legacy documents: no current encoder or SQL implementation builds
	// the fixture. Order, >2^53 integers, Unicode, and exact time all matter.
	old := map[string]string{
		ConfigFilename:                  `{"version":1,"sources":[{"id":"s1","name":"审阅","remote_url":"file:///repo","target_kind":"agents","target_root":"/skills","auto_sync":true,"added_at":"2026-11-01T02:03:04.123456789+08:00"}]}`,
		StateFilename:                   `{"version":1,"sources":[{"id":"s1","status":"ready","last_commit":"abc","last_auto_sync_day":"2026-11-01","installed_skills":[{"name":"demo","export_name":"demo","target_path":"/skills/demo","files":[{"rel_path":"b","sha256":"bhash","size":9007199254740993,"mode":493},{"rel_path":"a","sha256":"ahash","size":2,"mode":420}]}]}]}`,
		"teams-skill-push-reviews.json": `{"chat":{"chat":"chat","name":"审阅","created_at":"2026-11-01T02:03:04.123456789+08:00","expires_at":"2026-11-01T02:33:04.123456789+08:00","sources":[{"source_id":"s1","source_name":"审阅","base_commit":"abc","review_refspec":"HEAD:refs/heads/review","changes":[{"kind":"modified","rel_path":"b","source_path":"demo/b","size":9007199254740993},{"kind":"added","rel_path":"a"}]}]}}`,
		MigrationStatusFilename:         `{"status":"done","started_at":"2026-11-01T02:03:04.123456789+08:00","results":[{"id":"s1","name":"审阅","kind":"subscription","status":"migrated","skills":[{"export_name":"demo","old_path":"/old/demo","new_path":"/skills/demo","status":"migrated"}]}]}`,
	}
	for name, input := range old {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
	}
	db, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg, err := db.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Sources) != 1 || cfg.Sources[0].Name != "审阅" || skillTime(cfg.Sources[0].AddedAt) != "2026-11-01T02:03:04.123456789+08:00" {
		t.Fatalf("config %#v", cfg)
	}
	st, ok, err := db.SourceState("s1")
	if err != nil || !ok {
		t.Fatalf("state %v %v", ok, err)
	}
	if len(st.InstalledSkills) != 1 || len(st.InstalledSkills[0].Files) != 2 || st.InstalledSkills[0].Files[0].Size != 9007199254740993 || st.InstalledSkills[0].Files[1].RelPath != "a" {
		t.Fatalf("state %#v", st)
	}
	now := cfg.Sources[0].AddedAt
	review, ok, err := db.Review("chat", now)
	if err != nil || !ok {
		t.Fatalf("review %v %v", ok, err)
	}
	if len(review.Sources) != 1 || len(review.Sources[0].Changes) != 2 || review.Sources[0].Changes[0].Size != 9007199254740993 {
		t.Fatalf("review %#v", review)
	}
	report, ok, err := db.MigrationReport()
	if err != nil || !ok || len(report.Results) != 1 || report.Results[0].Skills[0].NewPath != "/skills/demo" {
		t.Fatalf("report %#v %v", report, err)
	}
	for name := range old {
		if _, err := os.Stat(filepath.Join(dir, name+".migrated")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte("broken leftover"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	other, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	changes := func() int64 {
		var n int64
		if err := db.db.QueryRow(`SELECT total_changes()`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	version := func() int64 {
		var n int64
		if err := other.db.QueryRow(`PRAGMA data_version`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	wal := func() int64 {
		st, err := os.Stat(db.path + "-wal")
		if os.IsNotExist(err) {
			return 0
		}
		if err != nil {
			t.Fatal(err)
		}
		return st.Size()
	}
	c, d, w := changes(), version(), wal()
	for i := 0; i < 20; i++ {
		if err = db.PutSubscription(cfg.Sources[0], st); err != nil {
			t.Fatal(err)
		}
		if err = db.SaveReview(review); err != nil {
			t.Fatal(err)
		}
		if err = db.SaveMigrationReport(report); err != nil {
			t.Fatal(err)
		}
		if _, _, err = db.SourceState("s1"); err != nil {
			t.Fatal(err)
		}
		if _, _, err = db.Review("chat", now); err != nil {
			t.Fatal(err)
		}
	}
	if changes() != c || version() != d || wal() != w {
		t.Fatalf("unchanged state wrote: changes %d/%d data_version %d/%d WAL %d/%d", c, changes(), d, version(), w, wal())
	}
	if err = db.SaveFailure(SourceState{ID: "s1", Status: StatusSyncFailed, LastError: "offline"}); err != nil {
		t.Fatal(err)
	}
	failed, _, err := db.SourceState("s1")
	if err != nil || failed.LastCommit != "abc" || len(failed.InstalledSkills[0].Files) != 2 || failed.LastAutoSyncDay != "2026-11-01" {
		t.Fatalf("failure discarded evidence %#v %v", failed, err)
	}
	if _, ok, err := db.Review("chat", review.ExpiresAt.Add(time.Nanosecond)); err != nil || ok {
		t.Fatalf("expired review %v %v", ok, err)
	}
}

func TestSkillsSQLiteMigrationRollsBack(t *testing.T) {
	for _, tc := range []struct{ name, file, input string }{
		{"duplicate", ConfigFilename, `{"sources":[{"id":"same"},{"id":"same"}]}`},
		{"late-state", StateFilename, `{"sources":[{"id":"first"},{`},
		{"key-mismatch", "teams-skill-push-reviews.json", `{"wrong":{"chat":"other"}}`},
		{"late-review", "teams-skill-push-reviews.json", `{"ok":{"chat":"ok"},"bad":`},
		{"future", ConfigFilename, `{"version":99}`},
		{"trailing", ConfigFilename, `{"sources":[]} {}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, ConfigFilename), []byte(`{"version":1,"sources":[{"id":"first"}]}`), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, tc.file), []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			if db, err := NewStore(dir); err == nil {
				db.Close()
				t.Fatal("accepted malformed input")
			}
			db, err := sql.Open("sqlite", filepath.Join(dir, "skills.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var version, tables int
			if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if version != 0 || tables != 0 {
				t.Fatalf("partial migration version=%d tables=%d", version, tables)
			}
			if _, err = os.Stat(filepath.Join(dir, tc.file)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSkillsSQLiteSubscriptionRollbackAndReviewIsolation(t *testing.T) {
	db, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.db.Exec(`CREATE TRIGGER reject_state BEFORE INSERT ON skill_states WHEN NEW.id='bad' BEGIN SELECT RAISE(ABORT,'injected');END`); err != nil {
		t.Fatal(err)
	}
	if err = db.PutSubscription(Source{ID: "bad"}, SourceState{ID: "bad"}); err == nil {
		t.Fatal("expected injected failure")
	}
	cfg, err := db.LoadConfig()
	if err != nil || len(cfg.Sources) != 0 {
		t.Fatalf("subscription partially committed %#v %v", cfg, err)
	}
	now := time.Now()
	for _, chat := range []string{"a", "b"} {
		if err = db.SaveReview(PushReview{Chat: chat, CreatedAt: now, ExpiresAt: now.Add(time.Hour), Sources: []ReviewSource{{SourceID: "s1"}, {SourceID: "s2"}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.DeleteReviewSource("a", "s1"); err != nil {
		t.Fatal(err)
	}
	a, _, err := db.Review("a", now)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := db.Review("b", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Sources) != 1 || a.Sources[0].SourceID != "s2" || len(b.Sources) != 2 {
		t.Fatalf("review isolation a=%#v b=%#v", a, b)
	}
	if err = db.DeleteReviewSource("a", "s2"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.Review("a", now); err != nil || ok {
		t.Fatalf("empty review %v %v", ok, err)
	}
	rows, err := db.db.Query(`SELECT name,sql FROM sqlite_schema WHERE type='table'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, ddl string
		if err = rows.Scan(&name, &ddl); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(strings.ToLower(ddl), " json") {
			t.Fatalf("JSON column in %s: %s", name, ddl)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestSkillsSQLiteManifestMigrationAndPublication(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, legacyManifestFilename)
	native := filepath.Join(dir, manifestFilename)
	input := `{"version":1,"source_id":"old","commit":"original","export_name":"demo","exported_at":"2026-11-01T02:03:04.123456789+08:00","files":[{"rel_path":"b","size":9007199254740993},{"rel_path":"a"}]}`
	if err := os.WriteFile(legacy, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	v, ok, err := readExportManifest(native)
	if err != nil || !ok || v.SourceID != "old" || v.Files[0].Size != 9007199254740993 {
		t.Fatalf("manifest %#v %v %v", v, ok, err)
	}
	if _, err = os.Stat(legacy + ".migrated"); err != nil {
		t.Fatal(err)
	}
	v.Commit = "new"
	if err = writeManifest(native, v); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(legacy, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	if err = migrateExportManifest(native); err != nil {
		t.Fatal(err)
	} // cannot replace a concurrent native publication
	got, ok, err := readExportManifest(native)
	if err != nil || !ok || got.Commit != "new" {
		t.Fatalf("native overwritten %#v %v", got, err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err = os.Stat(native + suffix); !os.IsNotExist(err) {
			t.Fatalf("portable manifest has sidecar %s: %v", suffix, err)
		}
	}
	info, err := os.Stat(native)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("portable manifest with two file records: %d bytes", info.Size())
}

func BenchmarkSkillsSQLiteSourceState(b *testing.B) {
	for _, n := range []int{1, 1000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			db, err := NewStore(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			if err = db.transaction(true, func(tx *sql.Tx) error {
				st, err := tx.Prepare(skill_statesPut)
				if err != nil {
					return err
				}
				defer st.Close()
				for i := 0; i < n; i++ {
					if _, err = st.Exec(skill_statesArgs(SourceState{ID: fmt.Sprint(i), Status: StatusReady})...); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := db.SourceState("0"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
