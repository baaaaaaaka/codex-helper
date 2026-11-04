package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// migrateFixture copies a converter fixture (real code of v0.1.20 and
// rc.59 wrote it: internal/core/migrate/testdata) into a scope directory
// under a fresh state root.
func migrateFixture(t *testing.T, version, form string) (root, scope string) {
	t.Helper()
	root = t.TempDir()
	scope = filepath.Join(root, "teams", "scopes", "scope_"+strings.ReplaceAll(version, ".", ""))
	src := filepath.Join("..", "core", "migrate", "testdata", version, form)
	ents, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		raw, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(scope, e.Name()), string(raw))
	}
	return root, scope
}

func dirHashes(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		out[e.Name()] = hex.EncodeToString(sum[:])
	}
	return out
}

// The first start of this version on a v0.1.20 or rc.59 store converts it
// into core.sqlite next to it: the old files stay byte-identical (the
// backup), only the new store, its report and the marker are added, and
// the new store carries the one-time summary for the control chat (no
// paths or internal ids in it). A second start finds the converted store.
func TestFirstStartConvertsOldStore(t *testing.T) {
	t.Setenv(envTeamsCoreStore, "")
	for _, tc := range []struct{ version, form string }{
		{"v0120", "sqlite-wal"}, {"rc59", "sqlite-wal"}, {"v0120", "json"}, {"rc59", "json"},
	} {
		t.Run(tc.version+"/"+tc.form, func(t *testing.T) {
			root, scope := migrateFixture(t, tc.version, tc.form)
			// An intermediate-release scope used earlier does not block it.
			old := filepath.Join(root, "teams", "scopes", "scope_old")
			writeFile(t, filepath.Join(old, "state.json"), `{"schema_version":8}`)
			past := time.Now().Add(-90 * 24 * time.Hour)
			_ = os.Chtimes(filepath.Join(old, "state.json"), past, past)

			path, from, err := planTeamsCoreStore(root)
			if err != nil || from != scope || path != filepath.Join(scope, migrate.TargetName) {
				t.Fatalf("plan = %q from %q, %v", path, from, err)
			}
			if _, err := resolveTeamsCoreStorePath(root); err == nil || !strings.Contains(err.Error(), "converts it when it starts") {
				t.Fatalf("read-only commands before the conversion: %v", err)
			}
			before := dirHashes(t, scope)

			var log strings.Builder
			if err := migrateTeamsStoreAtStart(context.Background(), from, t.TempDir(), &log); err != nil {
				t.Fatalf("convert: %v\n%s", err, log.String())
			}
			after := dirHashes(t, scope)
			var added []string
			for name, h := range after {
				if b, ok := before[name]; !ok {
					added = append(added, name)
				} else if b != h {
					t.Errorf("old store file %s changed", name)
				}
			}
			for name := range before {
				if _, ok := after[name]; !ok {
					t.Errorf("old store file %s removed", name)
				}
			}
			sort.Strings(added)
			// Besides the result: the old helper's lock file and SQLite's
			// shared-memory index of the online backup (no data).
			allowed := map[string]bool{migrate.MarkerName: true, migrate.TargetName: true, migrate.ReportName: true,
				"state.json.lock": true, "store.sqlite-shm": true}
			for _, name := range added {
				if !allowed[name] {
					t.Errorf("unexpected file %s added next to the old store", name)
				}
			}
			for _, name := range []string{migrate.MarkerName, migrate.TargetName, migrate.ReportName} {
				if _, ok := after[name]; !ok {
					t.Errorf("%s missing", name)
				}
			}
			if _, err := os.Stat(filepath.Join(scope, "migrate.tmp")); !os.IsNotExist(err) {
				t.Errorf("snapshot left behind: %v", err)
			}

			st, err := corestore.OpenWith(path, corestore.Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			summary, _, err := st.Meta(corestore.MetaMigrationSummary)
			st.Close()
			if err != nil {
				t.Fatal(err)
			}
			for _, sub := range []string{"converted from v0.1.", "Work chats:", migrate.ReportName} {
				if !strings.Contains(summary, sub) {
					t.Errorf("summary lacks %q:\n%s", sub, summary)
				}
			}
			if !strings.Contains(summary, "Catch-up of local Codex sessions:") && !strings.Contains(summary, "Local Codex sessions are caught up") {
				t.Errorf("summary lacks the catch-up of local Codex sessions:\n%s", summary)
			}
			if strings.ContainsAny(summary, `/\`) || strings.Contains(summary, "scope_") {
				t.Errorf("summary shows a path:\n%s", summary)
			}

			if got, from, err := planTeamsCoreStore(root); err != nil || from != "" || got != path {
				t.Fatalf("second start plan = %q from %q, %v", got, from, err)
			}
			if err := migrateTeamsStoreAtStart(context.Background(), scope, t.TempDir(), io.Discard); err != nil {
				t.Fatalf("a second conversion attempt is a no-op: %v", err)
			}
		})
	}
}

// Stores the first start refuses to pick by itself.
func TestFirstStartRefusesAmbiguousOldStores(t *testing.T) {
	t.Setenv(envTeamsCoreStore, "")
	t.Run("intermediate release used after the supported one", func(t *testing.T) {
		root, _ := migrateFixture(t, "rc59", "json")
		writeFile(t, filepath.Join(root, "teams", "scopes", "scope_new", "state.json"), `{"schema_version":8}`)
		future := time.Now().Add(time.Hour)
		_ = os.Chtimes(filepath.Join(root, "teams", "scopes", "scope_new", "state.json"), future, future)
		if _, _, err := planTeamsCoreStore(root); err == nil || !strings.Contains(err.Error(), "used more recently") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("two supported stores", func(t *testing.T) {
		root, _ := migrateFixture(t, "rc59", "json")
		raw, err := os.ReadFile(filepath.Join("..", "core", "migrate", "testdata", "v0120", "json", "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(root, "teams", "scopes", "scope_b", "state.json"), string(raw))
		if _, _, err := planTeamsCoreStore(root); err == nil || !strings.Contains(err.Error(), "cxp teams migrate --source") {
			t.Fatalf("err = %v", err)
		}
	})
}

// A conversion that fails is retried in-process with a growing wait, never
// at the service manager's restart rate (a large store is copied each
// attempt), and the wait ends with the process context.
func TestFirstStartConversionFailureBacksOff(t *testing.T) {
	scope := filepath.Join(t.TempDir(), "scope")
	// rc.59 pointer to a database that is not there: every attempt fails.
	writeFile(t, filepath.Join(scope, "state.json"), `{"schema_version":12,"storage_backend":"store-sqlite","storage_version":1,"path":"store.sqlite"}`)
	oldRetry, oldMax := teamsMigrateRetry, teamsMigrateRetryMax
	teamsMigrateRetry, teamsMigrateRetryMax = 40*time.Millisecond, 160*time.Millisecond
	t.Cleanup(func() { teamsMigrateRetry, teamsMigrateRetryMax = oldRetry, oldMax })

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	var log strings.Builder
	start := time.Now()
	err := migrateTeamsStoreAtStart(ctx, scope, "", &log)
	if err == nil {
		t.Fatal("conversion of a broken store succeeded")
	}
	if time.Since(start) < 600*time.Millisecond {
		t.Fatalf("returned before the context ended: %v", err)
	}
	// 40+80+160+160+160 ms of waits fit in 700 ms: at most 6 attempts.
	if n := strings.Count(log.String(), "conversion failed"); n < 2 || n > 6 {
		t.Fatalf("%d attempts in 700ms:\n%s", n, log.String())
	}
	if _, err := os.Stat(filepath.Join(scope, migrate.TargetName)); !os.IsNotExist(err) {
		t.Fatalf("core.sqlite after a failed conversion: %v", err)
	}
}
