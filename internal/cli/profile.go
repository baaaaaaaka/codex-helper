package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

func ensureProfile(
	ctx context.Context,
	store *config.Store,
	profileRef string,
	autoInit bool,
	out io.Writer,
) (config.Profile, config.Config, error) {
	cfg, err := store.Load()
	if err != nil {
		return config.Profile{}, cfg, err
	}

	var created *config.Profile
	if len(cfg.Profiles) == 0 && autoInit {
		if err := helperruntime.BeforeInteractiveSetup(); err != nil {
			return config.Profile{}, cfg, err
		}
		p, err := initProfileInteractiveFunc(ctx, store)
		if err != nil {
			return config.Profile{}, cfg, err
		}
		if out != nil {
			_, _ = fmt.Fprintf(out, "Saved profile %q (%s)\n", p.Name, p.ID)
		}
		created = &p

		cfg, err = store.Load()
		if err != nil {
			return config.Profile{}, cfg, err
		}
	}

	if created != nil && profileRef == "" {
		return *created, cfg, nil
	}

	p, err := selectProfile(cfg, profileRef)
	if err != nil {
		if created != nil {
			return *created, cfg, nil
		}
		return config.Profile{}, cfg, err
	}
	return p, cfg, nil
}
