package cli

import (
	"encoding/json"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// teamsConfigDefaults keeps the Teams helper's global default model and
// effort in the cxp config's `defaults`, where releases before the
// rebuilt core kept them (`default model|effort` in the control chat):
// `model` is a selector ("default" or "official:<model>"), and
// `reasoningEffort` an effort. Other keys of `defaults` are kept as they
// are. A selector the helper no longer supports ("profile:<name>": model
// profiles were removed with third-party models) reads as unset.
type teamsConfigDefaults struct{ store *config.Store }

func (d teamsConfigDefaults) ReadDefaults() (model, effort string, err error) {
	cfg, err := d.store.Load()
	if err != nil {
		return "", "", err
	}
	fields, err := teamsDefaultsFields(cfg.Defaults)
	if err != nil {
		return "", "", err
	}
	sel, effort := teamsDefaultsString(fields, "model"), teamsDefaultsString(fields, "reasoningEffort")
	switch {
	case strings.EqualFold(sel, "default"):
		model = "default"
	case strings.HasPrefix(sel, "official:"):
		model = strings.TrimSpace(strings.TrimPrefix(sel, "official:"))
	}
	return model, effort, nil
}

func (d teamsConfigDefaults) WriteDefaults(model, effort string) error {
	return d.store.Update(func(cfg *config.Config) error {
		fields, err := teamsDefaultsFields(cfg.Defaults)
		if err != nil {
			return err
		}
		set := func(key, v string) {
			if v == "" {
				delete(fields, key)
				return
			}
			b, _ := json.Marshal(v)
			fields[key] = b
		}
		sel := model
		if model != "" && model != "default" {
			sel = "official:" + model
		}
		set("model", sel)
		set("reasoningEffort", effort)
		if len(fields) == 0 {
			cfg.Defaults = nil
			return nil
		}
		b, err := json.Marshal(fields)
		cfg.Defaults = b
		return err
	})
}

func teamsDefaultsFields(raw json.RawMessage) (map[string]json.RawMessage, error) {
	fields := map[string]json.RawMessage{}
	if s := strings.TrimSpace(string(raw)); s == "" || s == "null" {
		return fields, nil
	}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func teamsDefaultsString(fields map[string]json.RawMessage, key string) string {
	var v string
	_ = json.Unmarshal(fields[key], &v)
	return strings.TrimSpace(v)
}
