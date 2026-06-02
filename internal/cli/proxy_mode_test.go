package cli

import (
	"bufio"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

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
	if cfg.ProxyEnabled != nil {
		t.Fatalf("expected ProxyEnabled to remain unset until profile setup completes, got %v", cfg.ProxyEnabled)
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
	if cfg.ProxyEnabled != nil {
		t.Fatalf("expected proxy preference to remain unset before profile setup, got %v", cfg.ProxyEnabled)
	}
}

func TestEnsureProxyPreferenceServiceDefaultsToDirectWithoutPrompt(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	store := newTempStore(t)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	prevStdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = prevStdin
		_ = reader.Close()
		_ = writer.Close()
	})

	got, cfg, err := runEnsureProxyPreferenceWithTimeout(t, store)
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if got {
		t.Fatalf("expected service mode to default to direct mode")
	}
	if cfg.ProxyEnabled == nil || *cfg.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=false in returned config, got %v", cfg.ProxyEnabled)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.ProxyEnabled == nil || *loaded.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=false persisted, got %v", loaded.ProxyEnabled)
	}
}

func TestEnsureProxyPreferenceNonTerminalDefaultsToDirectWithoutPrompt(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "")
	store := newTempStore(t)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	prevStdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = prevStdin
		_ = reader.Close()
		_ = writer.Close()
	})

	got, cfg, err := runEnsureProxyPreferenceWithTimeout(t, store)
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if got {
		t.Fatalf("expected non-terminal stdin to default to direct mode")
	}
	if cfg.ProxyEnabled == nil || *cfg.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=false, got %v", cfg.ProxyEnabled)
	}
}

func TestEnsureProxyPreferenceNonInteractiveUsesConfiguredProfiles(t *testing.T) {
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version:  config.CurrentVersion,
		Profiles: []config.Profile{{ID: "p1", Name: "p1"}},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	got, cfg, err := ensureProxyPreferenceWithReaderMode(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("n\n")), false)
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if !got || cfg.ProxyEnabled == nil || !*cfg.ProxyEnabled {
		t.Fatalf("expected configured profile to enable proxy in non-interactive mode")
	}
}

func TestEnsureProxyPreferenceNonInteractiveResetsIncompleteStateToDirect(t *testing.T) {
	store := newTempStore(t)
	enabled := true
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &enabled}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	got, cfg, err := ensureProxyPreferenceWithReaderMode(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("y\n")), false)
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if got {
		t.Fatalf("expected incomplete non-interactive state to default to direct mode")
	}
	if cfg.ProxyEnabled == nil || *cfg.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=false in returned config, got %v", cfg.ProxyEnabled)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if loaded.ProxyEnabled == nil || *loaded.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=false persisted, got %v", loaded.ProxyEnabled)
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

	reader := bufio.NewReader(strings.NewReader("n\n"))
	_, _, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, reader)
	if err == nil {
		t.Fatalf("expected error when config dir is read-only")
	}
}

func TestEnsureProxyPreferenceWithReaderUsesProvidedInput(t *testing.T) {
	store := newTempStore(t)

	enabled, _, err := ensureProxyPreferenceWithReader(context.Background(), store, "", io.Discard, bufio.NewReader(strings.NewReader("y\n")))
	if err != nil {
		t.Fatalf("ensureProxyPreference error: %v", err)
	}
	if !enabled {
		t.Fatalf("expected proxy enabled from provided input")
	}
}

func runEnsureProxyPreferenceWithTimeout(t *testing.T, store *config.Store) (bool, config.Config, error) {
	t.Helper()
	type result struct {
		enabled bool
		cfg     config.Config
		err     error
	}
	done := make(chan result, 1)
	go func() {
		enabled, cfg, err := ensureProxyPreference(context.Background(), store, "", io.Discard)
		done <- result{enabled: enabled, cfg: cfg, err: err}
	}()
	select {
	case got := <-done:
		return got.enabled, got.cfg, got.err
	case <-time.After(time.Second):
		t.Fatal("ensureProxyPreference blocked on stdin")
		return false, config.Config{}, nil
	}
}
