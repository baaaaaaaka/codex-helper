package cli

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/tui"
)

func TestRunHistoryTuiPersistsProxyEnabledAfterProfileSetup(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	prevEnsureProxy := ensureProxyPreferenceFunc
	prevEnsureProfile := ensureProfileFunc
	prevPersist := persistProxyPreferenceFunc
	prevSelect := selectSession
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		ensureProfileFunc = prevEnsureProfile
		persistProxyPreferenceFunc = prevPersist
		selectSession = prevSelect
	})

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return true, config.Config{Version: config.CurrentVersion}, nil
	}
	ensureProfileFunc = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		p := config.Profile{ID: "p1", Name: "p1"}
		return p, config.Config{
			Version:  config.CurrentVersion,
			Profiles: []config.Profile{p},
		}, nil
	}

	persistCalls := 0
	persistProxyPreferenceFunc = func(s *config.Store, enabled bool) error {
		persistCalls++
		if !enabled {
			t.Fatalf("expected persist true after profile setup")
		}
		return persistProxyPreference(s, enabled)
	}

	selectSession = func(context.Context, tui.Options) (*tui.Selection, error) {
		return nil, nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "", "", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}
	if persistCalls != 1 {
		t.Fatalf("expected 1 persist call, got %d", persistCalls)
	}

	updated, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if updated.ProxyEnabled == nil || !*updated.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=true persisted, got %v", updated.ProxyEnabled)
	}
}

func TestRunHistoryTuiDoesNotPersistProxyEnabledWhenProfileSetupFails(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	prevEnsureProxy := ensureProxyPreferenceFunc
	prevEnsureProfile := ensureProfileFunc
	prevPersist := persistProxyPreferenceFunc
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		ensureProfileFunc = prevEnsureProfile
		persistProxyPreferenceFunc = prevPersist
	})

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return true, config.Config{Version: config.CurrentVersion}, nil
	}
	profileErr := errors.New("profile setup failed")
	ensureProfileFunc = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		return config.Profile{}, config.Config{}, profileErr
	}

	persistCalls := 0
	persistProxyPreferenceFunc = func(*config.Store, bool) error {
		persistCalls++
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err = runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "", "", 0)
	if !errors.Is(err, profileErr) {
		t.Fatalf("expected profile setup error, got %v", err)
	}
	if persistCalls != 0 {
		t.Fatalf("expected no persist calls on profile setup failure, got %d", persistCalls)
	}

	updated, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if updated.ProxyEnabled != nil {
		t.Fatalf("expected ProxyEnabled to remain unset, got %v", updated.ProxyEnabled)
	}
}

func TestRunHistoryTuiToggleEnablePersistsAfterInitSuccess(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	disabled := false
	if err := persistProxyPreference(store, disabled); err != nil {
		t.Fatalf("seed proxy preference: %v", err)
	}

	prevEnsureProxy := ensureProxyPreferenceFunc
	prevInit := initProfileInteractiveFunc
	prevPersist := persistProxyPreferenceFunc
	prevSelect := selectSession
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		initProfileInteractiveFunc = prevInit
		persistProxyPreferenceFunc = prevPersist
		selectSession = prevSelect
	})

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{
			Version:      config.CurrentVersion,
			ProxyEnabled: boolPtr(false),
		}, nil
	}

	order := []string{}
	initProfileInteractiveFunc = func(context.Context, *config.Store) (config.Profile, error) {
		order = append(order, "init")
		return config.Profile{ID: "p1", Name: "p1"}, nil
	}
	persistProxyPreferenceFunc = func(s *config.Store, enabled bool) error {
		if enabled {
			order = append(order, "persist:true")
		} else {
			order = append(order, "persist:false")
		}
		return persistProxyPreference(s, enabled)
	}

	selectCalls := 0
	selectSession = func(context.Context, tui.Options) (*tui.Selection, error) {
		selectCalls++
		if selectCalls == 1 {
			return nil, tui.ProxyToggleRequested{Enable: true, RequireConfig: true}
		}
		return nil, nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "", "", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}

	if len(order) != 2 || order[0] != "init" || order[1] != "persist:true" {
		t.Fatalf("expected init then persist:true, got %v", order)
	}

	updated, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if updated.ProxyEnabled == nil || !*updated.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=true persisted, got %v", updated.ProxyEnabled)
	}
}

func TestRunHistoryTuiToggleEnableDoesNotPersistWhenInitFails(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	disabled := false
	if err := persistProxyPreference(store, disabled); err != nil {
		t.Fatalf("seed proxy preference: %v", err)
	}

	prevEnsureProxy := ensureProxyPreferenceFunc
	prevInit := initProfileInteractiveFunc
	prevPersist := persistProxyPreferenceFunc
	prevSelect := selectSession
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		initProfileInteractiveFunc = prevInit
		persistProxyPreferenceFunc = prevPersist
		selectSession = prevSelect
	})

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{
			Version:      config.CurrentVersion,
			ProxyEnabled: boolPtr(false),
		}, nil
	}

	initErr := errors.New("init failed")
	initProfileInteractiveFunc = func(context.Context, *config.Store) (config.Profile, error) {
		return config.Profile{}, initErr
	}

	persistCalls := 0
	persistProxyPreferenceFunc = func(*config.Store, bool) error {
		persistCalls++
		return nil
	}

	selectSession = func(context.Context, tui.Options) (*tui.Selection, error) {
		return nil, tui.ProxyToggleRequested{Enable: true, RequireConfig: true}
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	err = runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "", "", 0)
	if !errors.Is(err, initErr) {
		t.Fatalf("expected init error, got %v", err)
	}
	if persistCalls != 0 {
		t.Fatalf("expected no persist calls when init fails, got %d", persistCalls)
	}

	updated, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if updated.ProxyEnabled == nil || *updated.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled to stay false, got %v", updated.ProxyEnabled)
	}
}

func TestHistoryOpenPersistsProxyEnabledAfterProfileSetup(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	prevEnsureProxy := ensureProxyPreferenceFunc
	prevEnsureProfile := ensureProfileFunc
	prevPersist := persistProxyPreferenceFunc
	prevFind := findSessionWithProjectFunc
	prevRun := runCodexSessionFunc
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		ensureProfileFunc = prevEnsureProfile
		persistProxyPreferenceFunc = prevPersist
		findSessionWithProjectFunc = prevFind
		runCodexSessionFunc = prevRun
	})

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return true, config.Config{Version: config.CurrentVersion}, nil
	}
	ensureProfileFunc = func(context.Context, *config.Store, string, bool, io.Writer) (config.Profile, config.Config, error) {
		p := config.Profile{ID: "p1", Name: "p1"}
		return p, config.Config{
			Version:  config.CurrentVersion,
			Profiles: []config.Profile{p},
		}, nil
	}

	persistCalls := 0
	persistProxyPreferenceFunc = func(s *config.Store, enabled bool) error {
		persistCalls++
		if !enabled {
			t.Fatalf("expected persist true after profile setup")
		}
		return persistProxyPreference(s, enabled)
	}

	findSessionWithProjectFunc = func(string, string) (*codexhistory.Session, *codexhistory.Project, error) {
		s := &codexhistory.Session{SessionID: "sid"}
		p := &codexhistory.Project{Path: t.TempDir()}
		return s, p, nil
	}
	runCodexSessionFunc = func(
		context.Context,
		*rootOptions,
		*config.Store,
		*config.Profile,
		[]config.Instance,
		codexhistory.Session,
		codexhistory.Project,
		string,
		string,
		bool,
		bool,
		io.Writer,
	) error {
		return nil
	}

	root := &rootOptions{configPath: cfgPath}
	codexDir := ""
	codexPath := ""
	profileRef := ""
	cmd := newHistoryOpenCmd(root, &codexDir, &codexPath, &profileRef)
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"sid"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute history open: %v", err)
	}

	if persistCalls != 1 {
		t.Fatalf("expected 1 persist call, got %d", persistCalls)
	}
	updated, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if updated.ProxyEnabled == nil || !*updated.ProxyEnabled {
		t.Fatalf("expected ProxyEnabled=true persisted, got %v", updated.ProxyEnabled)
	}
}
