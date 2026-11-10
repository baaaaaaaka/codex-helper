package cli

import (
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"strings"
)

type teamsConfigDefaults struct{ store *config.Store }

func (d teamsConfigDefaults) ReadDefaults() (model, effort string, err error) {
	v, err := d.store.ReadDefaults()
	if err != nil {
		return "", "", err
	}
	sel := strings.TrimSpace(v.Model)
	effort = strings.TrimSpace(v.ReasoningEffort)
	switch {
	case strings.EqualFold(sel, "default"):
		model = "default"
	case strings.HasPrefix(sel, "official:"):
		model = strings.TrimSpace(strings.TrimPrefix(sel, "official:"))
	}
	return model, effort, nil
}
func (d teamsConfigDefaults) WriteDefaults(model, effort string) error {
	if model != "" && model != "default" {
		model = "official:" + model
	}
	return d.store.WriteDefaults(config.GlobalDefaults{Model: model, ReasoningEffort: effort})
}
