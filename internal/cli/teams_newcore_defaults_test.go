package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// The Teams helper's global defaults are the cxp config's `defaults`, in
// the selector format of the releases before the rebuilt core.
func TestTeamsConfigDefaultsReadWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	write := func(defaults string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(`{"version":1,"profiles":[],"defaults":`+defaults+`}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	st, err := config.NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	d := teamsConfigDefaults{st}
	for _, c := range []struct{ defaults, model, effort string }{
		{`{"model":"official:gpt-5.6-luna","reasoningEffort":"max"}`, "gpt-5.6-luna", "max"},
		{`{"model":"default"}`, "default", ""},
		{`{"model":"profile:old","reasoningEffort":"high"}`, "", "high"}, // model profiles are gone
		{`null`, "", ""},
	} {
		write(c.defaults)
		m, e, err := d.ReadDefaults()
		if err != nil || m != c.model || e != c.effort {
			t.Fatalf("%s: read %q %q %v, want %q %q", c.defaults, m, e, err, c.model, c.effort)
		}
	}

	write(`{"model":"default","other":"kept"}`)
	if err := d.WriteDefaults("gpt-5-mini", "low"); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Defaults map[string]string `json:"defaults"`
	}
	raw, _ := os.ReadFile(path)
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Defaults["model"] != "official:gpt-5-mini" || got.Defaults["reasoningEffort"] != "low" || got.Defaults["other"] != "kept" {
		t.Fatalf("written defaults %v", got.Defaults)
	}
	if err := d.WriteDefaults("", ""); err != nil {
		t.Fatal(err)
	}
	raw, _ = os.ReadFile(path)
	got.Defaults = nil
	_ = json.Unmarshal(raw, &got)
	if len(got.Defaults) != 1 || got.Defaults["other"] != "kept" {
		t.Fatalf("after reset %v", got.Defaults)
	}
}
