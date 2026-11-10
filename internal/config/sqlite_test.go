package config

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigSQLiteMigrationAndQuietWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	raw := `{"version":6,"minReader":1,"runtimeGeneration":1,"runtimeMigrationId":"original","proxyEnabled":false,"agentAutoApproveEnabled":true,"defaults":{"model":"official:test","reasoningEffort":"high"},"teamsCodexPath":{"mode":"explicit","explicitPath":"/usr/bin","shellOverride":"/bin/zsh"},"profiles":[{"id":"p","name":"配置","sshArgs":["-o","a=b","-o","c=d"],"createdAt":"2026-11-01T02:03:04.123456789+08:00"}],"instances":[{"id":"i","profileId":"p","ownerToken":"holder","brokerEpoch":"epoch","daemonPid":9007199254740993,"lastSeenAt":"2026-11-01T02:03:04.123456789+08:00","recoveryBudget":{"restartAttempts":7,"requestAttempts":11,"blocked":true,"lastReason":"原始故障"}}]}`
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	st, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg, err := st.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Profiles) != 1 || len(cfg.Profiles[0].SSHArgs) != 4 || cfg.Profiles[0].SSHArgs[2] != "-o" || configTime(cfg.Profiles[0].CreatedAt) != "2026-11-01T02:03:04.123456789+08:00" {
		t.Fatalf("profile %#v", cfg.Profiles)
	}
	if cfg.ProxyEnabled == nil || *cfg.ProxyEnabled || cfg.AgentAutoApproveEnabled == nil || !*cfg.AgentAutoApproveEnabled {
		t.Fatalf("nullable booleans %#v", cfg)
	}
	if len(cfg.Instances) != 1 || cfg.Instances[0].DaemonPID != 9007199254740993 || !cfg.Instances[0].RecoveryBudget.Blocked || cfg.Instances[0].RecoveryBudget.RequestAttempts != 11 {
		t.Fatalf("instance %#v", cfg.Instances)
	}
	if archived, err := os.ReadFile(path + ".migrated"); err != nil || string(archived) != raw {
		t.Fatalf("archive %v", err)
	}
	if err = os.WriteFile(path, []byte("poisoned legacy"), 0600); err != nil {
		t.Fatal(err)
	}
	other, err := NewStore(st.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err = other.Load(); err != nil {
		t.Fatal(err)
	}
	changes := func() int64 {
		var n int64
		if err = st.db.QueryRow(`SELECT total_changes()`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	version := func() int64 {
		var n int64
		if err = other.db.QueryRow(`PRAGMA data_version`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	wal := func() int64 {
		info, err := os.Stat(st.Path() + "-wal")
		if os.IsNotExist(err) {
			return 0
		}
		if err != nil {
			t.Fatal(err)
		}
		return info.Size()
	}
	c, d, w := changes(), version(), wal()
	for i := 0; i < 20; i++ {
		if err = st.Save(cfg); err != nil {
			t.Fatal(err)
		}
		if err = st.ChangeInstance("i", func(*Instance) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err = st.WriteDefaults(cfg.Defaults); err != nil {
			t.Fatal(err)
		}
		if _, _, err = st.Instance("i"); err != nil {
			t.Fatal(err)
		}
		if _, err = LoadPathReadOnly(st.Path()); err != nil {
			t.Fatal(err)
		}
	}
	if changes() != c || version() != d || wal() != w {
		t.Fatalf("quiet path wrote: changes=%d/%d version=%d/%d WAL=%d/%d", c, changes(), d, version(), w, wal())
	}
	if _, err = st.db.Exec(`CREATE TRIGGER reject_arg BEFORE INSERT ON ssh_args WHEN NEW.value='bad' BEGIN SELECT RAISE(ABORT,'injected');END`); err != nil {
		t.Fatal(err)
	}
	cfg.Defaults.Model = "must roll back"
	cfg.Profiles[0].SSHArgs = append(cfg.Profiles[0].SSHArgs, "bad")
	if err = st.Save(cfg); err == nil {
		t.Fatal("accepted injected child failure")
	}
	got, err := other.Load()
	if err != nil || got.Defaults.Model != "official:test" || len(got.Profiles[0].SSHArgs) != 4 {
		t.Fatalf("partial commit %#v %v", got, err)
	}
	// A late old heartbeat cannot clear a replacement's durable block.
	if err = other.ChangeInstance("i", func(v *Instance) error { v.OwnerToken = "replacement"; return nil }); err != nil {
		t.Fatal(err)
	}
	if err = st.DeleteInstanceWhen("i", func(v Instance) error {
		if v.OwnerToken != "holder" {
			return fmt.Errorf("fenced")
		}
		return nil
	}); err == nil {
		t.Fatal("stale holder deleted replacement")
	}
}

func TestConfigSQLiteMigrationRollsBack(t *testing.T) {
	for _, raw := range []string{
		`{"version":6,"profiles":[{"id":"p"},{"id":"p"}]}`,
		`{"version":6,"profiles":[{"id":"p"}],"instances":[{"id":"i"},{`,
		`{"version":99,"profiles":[{"id":"p"}]}`,
		`{"version":6,"profiles":[]} {}`,
		`{"version":6,"profiles":[],"profiles":[]}`,
		`null`,
	} {
		t.Run(fmt.Sprint(len(raw)), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			st, err := NewStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			if _, err = st.Load(); err == nil {
				t.Fatal("accepted invalid migration input")
			}
			db, err := sql.Open("sqlite", st.Path())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var version, tables int
			if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='table'`).Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if version != 0 || tables != 0 {
				t.Fatalf("partial migration v%d tables%d", version, tables)
			}
			got, err := os.ReadFile(path)
			if err != nil || string(got) != raw {
				t.Fatalf("modified failed source: %v", err)
			}
		})
	}
}

func TestConfigSQLitePointQueryUsesIndex(t *testing.T) {
	st, err := NewStore(filepath.Join(t.TempDir(), "config.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err = st.PutInstance(Instance{ID: "i"}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.db.Query(`EXPLAIN QUERY PLAN SELECT ` + instanceColumns + ` FROM instances WHERE id='i'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var a, b, c int
		var detail string
		if err = rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "SCAN instances") {
			t.Fatalf("point lookup scans instances: %s", detail)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkConfigSQLiteHeartbeat(b *testing.B) {
	for _, n := range []int{1, 1000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			st, err := NewStore(filepath.Join(b.TempDir(), "config.sqlite"))
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()
			if err = st.write(func(tx *sql.Tx) error {
				stmt, err := tx.Prepare(instancePut)
				if err != nil {
					return err
				}
				defer stmt.Close()
				for i := 0; i < n; i++ {
					if _, err = stmt.Exec(append([]any{i}, instanceArgs(Instance{ID: fmt.Sprint(i), OwnerToken: "owner", BrokerEpoch: "epoch"})...)...); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				now = now.Add(time.Second)
				if err = st.ChangeInstance("0", func(v *Instance) error { v.LastSeenAt = now; return nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
