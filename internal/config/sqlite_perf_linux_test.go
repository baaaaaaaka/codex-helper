package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/testperf"
)

// Opt-in measured process, not part of each unit run. CI runs one fresh
// executable per scale and observes its actual syscalls as well as proc I/O.
func TestConfigSQLitePerformanceProbe(t *testing.T) {
	if os.Getenv("CXP_SQLITE_PERF_PROBE") != "1" {
		t.Skip("nightly synthetic performance probe")
	}
	n, err := strconv.Atoi(os.Getenv("CXP_SQLITE_PERF_ROWS"))
	if err != nil || n < 1 {
		t.Fatal("positive CXP_SQLITE_PERF_ROWS required")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	fmt.Fprint(w, `{"version":6,"minReader":1,"profiles":[],"instances":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprintf(w, `{"id":"%d","profileId":"p","ownerToken":"owner","brokerEpoch":"epoch","lastSeenAt":"2026-11-01T00:00:00Z"}`, i)
	}
	fmt.Fprint(w, `]}`)
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	phase := func(name string, work func()) {
		testperf.Measure(t, dir, name, n, work)
	}
	st, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	phase("migration", func() {
		if _, ok, err := st.Instance("0"); err != nil || !ok {
			t.Fatal("migration point read", ok, err)
		}
	})
	phase("read-only", func() {
		for i := 0; i < 100; i++ {
			if _, ok, err := st.Instance("0"); err != nil || !ok {
				t.Fatal(ok, err)
			}
		}
	})
	phase("unchanged", func() {
		for i := 0; i < 100; i++ {
			if err := st.ChangeInstance("0", func(*Instance) error { return nil }); err != nil {
				t.Fatal(err)
			}
		}
	})
	phase("heartbeat", func() {
		now := time.Now()
		for i := 0; i < 100; i++ {
			now = now.Add(time.Second)
			if err := st.ChangeInstance("0", func(v *Instance) error { v.LastSeenAt = now; return nil }); err != nil {
				t.Fatal(err)
			}
		}
	})
	phase("settings", func() {
		for i := 0; i < 100; i++ {
			enabled := i%2 == 0
			if err := st.ChangeSettings(func(v *Config) error { v.AgentAutoApproveEnabled = &enabled; return nil }); err != nil {
				t.Fatal(err)
			}
		}
	})
	phase("cold-open", func() {
		for i := 0; i < 20; i++ {
			other, err := NewStore(st.Path())
			if err != nil {
				t.Fatal(err)
			}
			_, ok, err := other.Instance("0")
			other.Close()
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
		}
	})
}
