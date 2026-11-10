package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestConfigSQLitePointEditsPreserveUnrelatedCollections(t *testing.T) {
	s, err := NewStore(filepath.Join(t.TempDir(), "config.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Save(Config{Profiles: []Profile{{ID: "target", SSHArgs: []string{"a"}}, {ID: "untouched", SSHArgs: []string{"second", "first"}}}, Instances: []Instance{{ID: "i", OwnerToken: "owner"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`CREATE TRIGGER untouched_profile BEFORE UPDATE ON profiles WHEN old.id='untouched' BEGIN SELECT RAISE(ABORT,'unrelated profile write'); END;
CREATE TRIGGER untouched_args BEFORE DELETE ON ssh_args WHEN old.profile_id='untouched' BEGIN SELECT RAISE(ABORT,'unrelated args write'); END;
CREATE TRIGGER untouched_instance BEFORE UPDATE ON instances BEGIN SELECT RAISE(ABORT,'unrelated instance write'); END`); err != nil {
		t.Fatal(err)
	}
	change := func(c *Config) error { v := true; c.AgentAutoApproveEnabled = &v; return nil }
	p := Profile{ID: "target", Name: "updated", SSHArgs: []string{"c", "b", "c"}}
	if err = s.ChangeSettings(change); err != nil {
		t.Fatal(err)
	}
	if err = s.PutProfile(p); err != nil {
		t.Fatal(err)
	}
	var before, after int64
	if err = s.db.QueryRow(`SELECT total_changes()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err = s.ChangeSettings(change); err != nil {
		t.Fatal(err)
	}
	if err = s.PutProfile(p); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatal("same-value settings/profile wrote", before, after)
	}
	cfg, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Profiles) != 2 || cfg.Profiles[0].ID != "target" || len(cfg.Profiles[0].SSHArgs) != 3 || cfg.Profiles[1].SSHArgs[0] != "second" || len(cfg.Instances) != 1 || cfg.Instances[0].OwnerToken != "owner" {
		t.Fatal(cfg)
	}
	if err = s.ChangeSettings(func(c *Config) error { c.Profiles = []Profile{{ID: "bad"}}; return nil }); err == nil {
		t.Fatal("settings callback edited a collection")
	}
}

func TestConfigSQLitePrunePreservesConcurrentProgress(t *testing.T) {
	s, err := NewStore(filepath.Join(t.TempDir(), "config.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	dead := Instance{ID: "dead", OwnerToken: "original"}
	active := Instance{ID: "active", OwnerToken: "owner"}
	if err = s.Save(Config{Instances: []Instance{dead, active}}); err != nil {
		t.Fatal(err)
	}
	// A second process makes progress between PID inspection and deletion.
	other, err := NewStore(s.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err = other.ChangeInstance(active.ID, func(v *Instance) error { v.LastSeenAt = time.Now(); v.OwnerToken = "replacement"; return nil }); err != nil {
		t.Fatal(err)
	}
	removed, err := s.DeleteUnchangedInstances([]Instance{dead, active})
	if err != nil || removed != 1 {
		t.Fatal(removed, err)
	}
	if _, ok, err := s.Instance(dead.ID); err != nil || ok {
		t.Fatal(ok, err)
	}
	if got, ok, err := s.Instance(active.ID); err != nil || !ok || got.OwnerToken != "replacement" {
		t.Fatal(got, ok, err)
	}
}

func TestConfigSQLitePruneBatchRollsBack(t *testing.T) {
	s, err := NewStore(filepath.Join(t.TempDir(), "config.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first, second := Instance{ID: "first"}, Instance{ID: "second"}
	if err = s.Save(Config{Instances: []Instance{first, second}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`CREATE TRIGGER deny_second_delete BEFORE DELETE ON instances WHEN old.id='second' BEGIN SELECT RAISE(ABORT,'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if removed, err := s.DeleteUnchangedInstances([]Instance{first, second}); err == nil || removed != 0 {
		t.Fatal("failed batch reported success", removed, err)
	}
	for _, id := range []string{first.ID, second.ID} {
		if _, ok, err := s.Instance(id); err != nil || !ok {
			t.Fatal("failed batch lost instance", id, ok, err)
		}
	}
}
