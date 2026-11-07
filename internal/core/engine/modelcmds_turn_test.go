package engine

import (
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// What `model status` shows is what a turn sends (TurnSettings): the
// account default and its default effort for a chat without a model of its
// own, a chosen model's default effort, a chosen effort as it is.
func TestTurnSettingsMatchModelStatus(t *testing.T) {
	models := []Model{
		{ID: "model-a", DisplayName: "Model A", Default: true, Effort: "medium", Efforts: []string{"low", "medium", "high"}},
		{ID: "model-b", DisplayName: "Model B", Effort: "low", Efforts: []string{"low", "medium"}},
	}
	cases := []struct {
		name                  string
		th                    store.Thread
		wantModel, wantEffort string
	}{
		{"account default", store.Thread{}, "model-a", "medium"},
		{"v0.1.20 default selector", store.Thread{Model: "default"}, "model-a", "medium"},
		{"chosen model", store.Thread{Model: "model-b"}, "model-b", "low"},
		{"display name", store.Thread{Model: "Model B"}, "model-b", "low"},
		{"chosen effort", store.Thread{Effort: "high", EffortSource: effortSrcExplicit}, "model-a", "high"},
		{"pending switch", store.Thread{Model: "model-a", PendingModel: "model-b"}, "model-b", "low"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			chat := modelChat{kind: "Work", th: c.th, models: models}
			m, e := TurnSettings(models, firstNonEmpty(c.th.PendingModel, c.th.Model), firstNonEmpty(c.th.PendingEffort, c.th.Effort))
			if m != c.wantModel || e != c.wantEffort {
				t.Fatalf("TurnSettings = %s/%s, want %s/%s", m, e, c.wantModel, c.wantEffort)
			}
			status := chat.modelStatusText()
			if !strings.Contains(status, "Effective: Codex Official (`"+m+"`)") || !strings.Contains(status, "Effective effort: `"+e+"`") {
				t.Fatalf("model status does not show %s/%s:\n%s", m, e, status)
			}
		})
	}
	if m, e := TurnSettings(nil, "", ""); m != "" || e != "" {
		t.Fatalf("without a model list the turn leaves the choice to Codex: %q %q", m, e)
	}
	if m, e := TurnSettings(models, "model-gone", ""); m != "model-gone" || e != "" {
		t.Fatalf("an unlisted model is sent as it is: %q %q", m, e)
	}
}
