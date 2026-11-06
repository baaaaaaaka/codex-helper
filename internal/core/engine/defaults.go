package engine

import (
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Global defaults (gap batch B, F-CTL-15): the model and reasoning effort
// new chats start with, set from the control chat with `default model …`
// and `default effort …`. "" is "unset": Codex's own default (the model it
// advertises as default, and that model's default effort).
const (
	metaDefaultModel  = "engine_default_model"
	metaDefaultEffort = "engine_default_effort"
)

type globalDefaults struct{ Model, Effort string }

func readGlobalDefaults(tx *store.Tx) (globalDefaults, error) {
	var d globalDefaults
	var err error
	if d.Model, _, err = tx.Meta(metaDefaultModel); err != nil {
		return d, err
	}
	d.Effort, _, err = tx.Meta(metaDefaultEffort)
	return d, err
}
