package cli

import (
	"bufio"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestEnsureProxyPreferenceRespectsExistingValue(t *testing.T) {
	store := newTempStore(t)
	enabled := true
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &enabled,
		Profiles:     []config.Profile{{ID: "p1", Name: "p1"}},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	got, cfg, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("n\n")))
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if !got || cfg.ProxyEnabled == nil || !*cfg.ProxyEnabled {
		t.Fatalf("expected proxy enabled from config")
	}
}

func TestEnsureProxyPreferenceResetsIncompleteState(t *testing.T) {
	store := newTempStore(t)
	enabled := true
	// ProxyEnabled=true but no profiles → incomplete setup.
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &enabled}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	// Provide "n" so the re-prompt returns false.
	got, _, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("n\n")))
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if got {
		t.Fatalf("expected proxy disabled after re-prompt with 'n'")
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ProxyEnabled == nil || *cfg.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=false persisted, got %v", cfg.ProxyEnabled)
	}
}

func TestEnsureProxyPreferenceResetsIncompleteStateAcceptsYes(t *testing.T) {
	store := newTempStore(t)
	enabled := true
	// ProxyEnabled=true but no profiles → incomplete setup.
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &enabled}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	// Provide "y" so the re-prompt returns true.
	got, _, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("y\n")))
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if !got {
		t.Fatalf("expected proxy enabled after re-prompt with 'y'")
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ProxyEnabled == nil || !*cfg.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=true persisted, got %v", cfg.ProxyEnabled)
	}
}

func TestEnsureProxyPreferenceDisabledNoProfilesNotIncomplete(t *testing.T) {
	store := newTempStore(t)
	disabled := false
	// ProxyEnabled=false with no profiles is a valid final state, not incomplete.
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &disabled}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	got, cfg, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("y\n")))
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if got {
		t.Fatalf("expected proxy disabled; ProxyEnabled=false should be respected")
	}
	if cfg.ProxyEnabled == nil || *cfg.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=false unchanged in returned config")
	}
}

func TestEnsureProxyPreferenceDisabledWithProfilesNotIncomplete(t *testing.T) {
	store := newTempStore(t)
	disabled := false
	if err := store.Save(config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: &disabled,
		Profiles:     []config.Profile{{ID: "p1", Name: "p1"}},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	got, cfg, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("y\n")))
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if got {
		t.Fatalf("expected proxy disabled; ProxyEnabled=false with profiles should be respected")
	}
	if cfg.ProxyEnabled == nil || *cfg.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=false unchanged in returned config")
	}
}

func TestEnsureProxyPreferenceIncompleteStateClearsFile(t *testing.T) {
	store := newTempStore(t)
	enabled := true
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &enabled}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	// Run with "n" to trigger reset and re-prompt.
	ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("n\n")))

	// The re-prompt answered "n", so ProxyEnabled should now be false.
	// Simulate what a second launch would do: load the config and check.
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ProxyEnabled == nil {
		t.Fatalf("expected ProxyEnabled to be set after re-prompt (false), got nil")
	}
	if *cfg.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=false after re-prompt with 'n'")
	}

	// A second call with the now-clean state should return false immediately
	// without prompting (ProxyEnabled=false is a valid final state).
	got, _, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("y\n")))
	if err != nil {
		t.Fatalf("second call error: %v", err)
	}
	if got {
		t.Fatalf("expected second call to return false without re-prompting")
	}
}

func TestEnsureProxyPreferencePromptsWhenNoProfiles(t *testing.T) {
	store := newTempStore(t)

	got, _, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("y\n")))
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if !got {
		t.Fatalf("expected proxy enabled from prompt")
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ProxyEnabled == nil || !*cfg.ProxyEnabled {
		t.Fatalf("expected proxy enabled in config")
	}
}

func TestEnsureProxyPreferenceDefaultsToProxyWhenProfilesExist(t *testing.T) {
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version:  config.CurrentVersion,
		Profiles: []config.Profile{{ID: "p1", Name: "p1"}},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	got, cfg, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("n\n")))
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if !got || cfg.ProxyEnabled == nil || !*cfg.ProxyEnabled {
		t.Fatalf("expected proxy enabled when profiles exist")
	}
}

func TestEnsureProxyPreferenceWriteFailure(t *testing.T) {
	store := newTempStore(t)
	if err := os.MkdirAll(store.Path(), 0o700); err != nil {
		t.Fatalf("mkdir config path: %v", err)
	}

	reader := bufio.NewReader(strings.NewReader("y\n"))
	_, _, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, reader)
	if err == nil {
		t.Fatalf("expected error when config dir is read-only")
	}
}

func TestEnsureProxyPreferenceUsesStdin(t *testing.T) {
	store := newTempStore(t)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	prevStdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() { os.Stdin = prevStdin })

	if _, err := writer.Write([]byte("y\n")); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	_ = writer.Close()

	enabled, _, err := ensureProxyPreference(context.Background(), store, "", io.Discard)
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if !enabled {
		t.Fatalf("expected proxy enabled from stdin input")
	}
}
