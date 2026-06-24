package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

var proxyPreferencePromptAllowed = func() bool {
	return proxyPreferencePromptAllowedFor(os.Getenv("CODEX_HELPER_TEAMS_SERVICE"), os.Stdin)
}

func proxyPreferencePromptAllowedFor(serviceEnv string, stdin *os.File) bool {
	if strings.TrimSpace(serviceEnv) != "" {
		return false
	}
	return isTerminalFile(stdin)
}

func ensureProxyPreference(ctx context.Context, store *config.Store, profileRef string, out io.Writer) (bool, config.Config, error) {
	return ensureProxyPreferenceWithReaderMode(ctx, store, profileRef, out, bufio.NewReader(os.Stdin), proxyPreferencePromptAllowed())
}

func ensureProxyPreferenceWithReader(
	ctx context.Context,
	store *config.Store,
	profileRef string,
	out io.Writer,
	reader *bufio.Reader,
) (bool, config.Config, error) {
	return ensureProxyPreferenceWithReaderMode(ctx, store, profileRef, out, reader, true)
}

func ensureProxyPreferenceWithReaderMode(
	_ context.Context,
	store *config.Store,
	profileRef string,
	out io.Writer,
	reader *bufio.Reader,
	allowPrompt bool,
) (bool, config.Config, error) {
	cfg, err := store.Load()
	if err != nil {
		return false, cfg, err
	}

	if cfg.ProxyEnabled != nil {
		// If proxy was enabled but no profile was ever created, treat as
		// incomplete setup — clear the flag and re-ask from scratch.
		if *cfg.ProxyEnabled && len(cfg.Profiles) == 0 {
			_ = store.Update(func(c *config.Config) error {
				c.ProxyEnabled = nil
				return nil
			})
		} else {
			return *cfg.ProxyEnabled, cfg, nil
		}
	}

	if len(cfg.Profiles) > 0 {
		enabled := true
		if err := persistProxyPreference(store, enabled); err != nil {
			return false, cfg, err
		}
		cfg.ProxyEnabled = &enabled
		return enabled, cfg, nil
	}

	if !allowPrompt {
		if strings.TrimSpace(profileRef) != "" {
			return false, cfg, fmt.Errorf("proxy profile %q requested but no SSH profiles are configured; cannot prompt in non-interactive mode", profileRef)
		}
		enabled := false
		if err := persistProxyPreference(store, enabled); err != nil {
			return false, cfg, err
		}
		cfg.ProxyEnabled = &enabled
		if out != nil {
			_, _ = fmt.Fprintln(out, "codex-proxy SSH preference is unset; non-interactive launch defaults to direct mode.")
		}
		return enabled, cfg, nil
	}

	if out != nil {
		_, _ = fmt.Fprintln(out, "codex-proxy can route Codex traffic through an SSH tunnel when your network requires it.")
		_, _ = fmt.Fprintln(out, "If you don't need a proxy, choose \"no\" to connect directly.")
	}
	defaultYes := profileRef != ""
	enabled := promptYesNo(reader, "Use SSH proxy for Codex?", defaultYes)
	// Do not persist ProxyEnabled=true until proxy setup is complete
	// (i.e. at least one profile has been created successfully).
	if !enabled {
		if err := persistProxyPreference(store, enabled); err != nil {
			return false, cfg, err
		}
	}
	cfg.ProxyEnabled = &enabled
	return enabled, cfg, nil
}

func persistProxyPreference(store *config.Store, enabled bool) error {
	return store.Update(func(cfg *config.Config) error {
		cfg.ProxyEnabled = boolPtr(enabled)
		return nil
	})
}

func boolPtr(v bool) *bool { return &v }
