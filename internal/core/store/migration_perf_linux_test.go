package store

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/testperf"
)

func TestEngineSQLiteLargeMigrationProbe(t *testing.T) {
	if os.Getenv("CXP_SQLITE_PERF_PROBE") != "1" {
		t.Skip("nightly synthetic performance probe")
	}
	n, err := strconv.Atoi(os.Getenv("CXP_SQLITE_PERF_ROWS"))
	if err != nil || n < 1 {
		t.Fatal("positive CXP_SQLITE_PERF_ROWS required")
	}
	path := os.Getenv("CXP_SQLITE_PERF_INPUT")
	if path == "" {
		path, old := legacyEngineDB(t)
		fixture := filepath.Join(filepath.Dir(path), "fixture.json")
		f, err := os.Create(fixture)
		mustT(t, err)
		w := bufio.NewWriter(f)
		fmt.Fprint(w, "[")
		for i := 0; i < n; i++ {
			if i > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprintf(w, `{"conv":1,"key":"key:%d:","request":%q,"attempted":true}`, i, strings.Repeat("x", 1024))
		}
		fmt.Fprint(w, "]")
		mustT(t, w.Flush())
		mustT(t, f.Close())
		body, err := os.ReadFile(fixture)
		mustT(t, err)
		_, err = old.Exec(`INSERT INTO meta(key,value) VALUES('engine_notify',?)`, string(body))
		mustT(t, err)
		body = nil
		mustT(t, os.Remove(fixture))
		mustT(t, old.Close())
		testperf.RunFresh(t, path)
		return
	}
	testperf.Measure(t, filepath.Dir(path), "core-migration", n, func() {
		s, err := Open(path)
		mustT(t, err)
		defer s.Close()
		var count int
		mustT(t, s.w.QueryRow(`SELECT count(*) FROM pending_notifications`).Scan(&count))
		if count != n {
			t.Fatalf("migrated rows=%d want=%d", count, n)
		}
		var version int
		mustT(t, s.w.QueryRow(`PRAGMA user_version`).Scan(&version))
		t.Logf("native_version=%d", version)
	})
}
