package cli

import (
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"os"
	"path/filepath"
	"testing"
)

func TestTeamsConfigDefaultsReadWrite(t *testing.T) {
	for _, tc := range []struct{ raw, model, effort string }{
		{`{"model":"official:gpt-5.6-luna","reasoningEffort":"max"}`, "gpt-5.6-luna", "max"},
		{`{"model":"default"}`, "default", ""},
		{`{"model":"profile:old","reasoningEffort":"high"}`, "", "high"},
		{`null`, "", ""},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			raw := `{"version":1,"profiles":[],"defaults":` + tc.raw + `}`
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			st, err := config.NewStore(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { st.Close() })
			defer st.Close()
			d := teamsConfigDefaults{st}
			m, e, err := d.ReadDefaults()
			if err != nil || m != tc.model || e != tc.effort {
				t.Fatalf("got %q %q %v", m, e, err)
			}
			if err = d.WriteDefaults("gpt-5-mini", "low"); err != nil {
				t.Fatal(err)
			}
			v, err := st.ReadDefaults()
			if err != nil || v.Model != "official:gpt-5-mini" || v.ReasoningEffort != "low" {
				t.Fatalf("written %#v %v", v, err)
			}
			if err = d.WriteDefaults("", ""); err != nil {
				t.Fatal(err)
			}
			v, err = st.ReadDefaults()
			if err != nil || v != (config.GlobalDefaults{}) {
				t.Fatalf("reset %#v %v", v, err)
			}
		})
	}
}
