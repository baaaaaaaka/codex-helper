package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/tui"
)

func TestRunHistoryTuiHidesYoloToggleWithoutPatchHistory(t *testing.T) {
	lockCLITestHooks(t)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevSelect := selectSession
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		selectSession = prevSelect
	})

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion}, nil
	}

	called := false
	selectSession = func(_ context.Context, opts tui.Options) (*tui.Selection, error) {
		called = true
		if opts.ShowYoloToggle {
			t.Fatalf("expected yolo toggle to be hidden")
		}
		return nil, nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "", "", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}
	if !called {
		t.Fatal("expected selectSession to be called")
	}
}

func TestRunHistoryTuiShowsYoloToggleWithPatchHistory(t *testing.T) {
	lockCLITestHooks(t)
	configDir := t.TempDir()
	cfgPath := filepath.Join(configDir, "config.json")
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevSelect := selectSession
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		selectSession = prevSelect
	})

	phs, err := config.NewPatchHistoryStore(configDir)
	if err != nil {
		t.Fatalf("patch history: %v", err)
	}
	if err := phs.Upsert(config.PatchHistoryEntry{
		Path:       "/tmp/codex",
		OrigSHA256: "abc123",
		PatchedAt:  time.Now(),
	}); err != nil {
		t.Fatalf("upsert patch history: %v", err)
	}

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion}, nil
	}

	called := false
	selectSession = func(_ context.Context, opts tui.Options) (*tui.Selection, error) {
		called = true
		if !opts.ShowYoloToggle {
			t.Fatalf("expected yolo toggle to be visible")
		}
		return nil, nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "", "", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}
	if !called {
		t.Fatal("expected selectSession to be called")
	}
}

func TestRunHistoryTuiShowsYoloToggleWithPersistedFalse(t *testing.T) {
	lockCLITestHooks(t)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevSelect := selectSession
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		selectSession = prevSelect
	})

	disabled := false
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{
			Version:     config.CurrentVersion,
			YoloEnabled: &disabled,
		}, nil
	}

	called := false
	selectSession = func(_ context.Context, opts tui.Options) (*tui.Selection, error) {
		called = true
		if !opts.ShowYoloToggle {
			t.Fatalf("expected yolo toggle to stay visible when persisted false")
		}
		return nil, nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "", "", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}
	if !called {
		t.Fatal("expected selectSession to be called")
	}
}

func TestShouldShowYoloToggleReturnsTrueWhenPatchHistoryStoreInitFails(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}

	got := shouldShowYoloToggle(config.Config{Version: config.CurrentVersion}, filepath.Join(blocker, "config.json"))
	if !got {
		t.Fatal("expected yolo toggle to remain visible when patch history store init fails")
	}
}

func TestShouldShowYoloToggleReturnsTrueWhenPatchHistoryLoadFails(t *testing.T) {
	configDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(configDir, "patch_history.json"), []byte("{invalid json"), 0o600); err != nil {
		t.Fatalf("write patch history: %v", err)
	}

	got := shouldShowYoloToggle(config.Config{Version: config.CurrentVersion}, filepath.Join(configDir, "config.json"))
	if !got {
		t.Fatal("expected yolo toggle to remain visible when patch history load fails")
	}
}

func TestRunHistoryTuiOpensNewSessionSelection(t *testing.T) {
	lockCLITestHooks(t)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevSelect := selectSession
	prevRunNew := runCodexNewSessionFn
	prevRunSession := runCodexSessionFunc
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		selectSession = prevSelect
		runCodexNewSessionFn = prevRunNew
		runCodexSessionFunc = prevRunSession
	})

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion}, nil
	}
	selectSession = func(_ context.Context, _ tui.Options) (*tui.Selection, error) {
		return &tui.Selection{
			Cwd:      "/tmp/project",
			UseProxy: true,
			UseYolo:  true,
		}, nil
	}

	called := false
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
		t.Fatal("expected new-session path, not existing session")
		return nil
	}
	runCodexNewSessionFn = func(
		_ context.Context,
		_ *rootOptions,
		_ *config.Store,
		_ *config.Profile,
		_ []config.Instance,
		cwd string,
		codexPath string,
		codexDir string,
		useProxy bool,
		useYolo bool,
		_ io.Writer,
	) error {
		called = true
		if cwd != "/tmp/project" || codexPath != "codex-bin" || codexDir != "codex-home" {
			t.Fatalf("unexpected selection args: cwd=%q codexPath=%q codexDir=%q", cwd, codexPath, codexDir)
		}
		if !useProxy || !useYolo {
			t.Fatalf("expected selection flags to propagate, got useProxy=%v useYolo=%v", useProxy, useYolo)
		}
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "codex-home", "codex-bin", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}
	if !called {
		t.Fatal("expected runCodexNewSessionFn to be called")
	}
}

func TestRunHistoryTuiOpensExistingSessionSelection(t *testing.T) {
	lockCLITestHooks(t)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevSelect := selectSession
	prevRunNew := runCodexNewSessionFn
	prevRunSession := runCodexSessionFunc
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		selectSession = prevSelect
		runCodexNewSessionFn = prevRunNew
		runCodexSessionFunc = prevRunSession
	})

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion}, nil
	}
	selectSession = func(_ context.Context, _ tui.Options) (*tui.Selection, error) {
		return &tui.Selection{
			Session:  codexhistory.Session{SessionID: "sid"},
			Project:  codexhistory.Project{Path: "/repo"},
			UseProxy: false,
			UseYolo:  true,
		}, nil
	}

	runCodexNewSessionFn = func(
		context.Context,
		*rootOptions,
		*config.Store,
		*config.Profile,
		[]config.Instance,
		string,
		string,
		string,
		bool,
		bool,
		io.Writer,
	) error {
		t.Fatal("expected existing-session path")
		return nil
	}

	called := false
	runCodexSessionFunc = func(
		_ context.Context,
		_ *rootOptions,
		_ *config.Store,
		_ *config.Profile,
		_ []config.Instance,
		session codexhistory.Session,
		project codexhistory.Project,
		codexPath string,
		codexDir string,
		useProxy bool,
		useYolo bool,
		_ io.Writer,
	) error {
		called = true
		if session.SessionID != "sid" || project.Path != "/repo" {
			t.Fatalf("unexpected session/project: %+v %+v", session, project)
		}
		if codexPath != "codex-bin" || codexDir != "codex-home" {
			t.Fatalf("unexpected codex args: %q %q", codexPath, codexDir)
		}
		if useProxy || !useYolo {
			t.Fatalf("expected useProxy=false useYolo=true, got %v %v", useProxy, useYolo)
		}
		return nil
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "codex-home", "codex-bin", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}
	if !called {
		t.Fatal("expected runCodexSessionFunc to be called")
	}
}

func TestHistoryOpenReturnsSessionNotFound(t *testing.T) {
	lockCLITestHooks(t)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevFind := findSessionWithProjectFunc
	prevRun := runCodexSessionFunc
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		findSessionWithProjectFunc = prevFind
		runCodexSessionFunc = prevRun
	})

	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion}, nil
	}
	findSessionWithProjectFunc = func(string, string) (*codexhistory.Session, *codexhistory.Project, error) {
		return nil, nil, nil
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
		t.Fatal("expected history open to stop before launching codex")
		return nil
	}

	root := &rootOptions{configPath: cfgPath}
	codexDir := ""
	codexPath := ""
	profileRef := ""
	cmd := newHistoryOpenCmd(root, &codexDir, &codexPath, &profileRef)
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"missing"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected session-not-found error")
	}
	if !errors.Is(err, context.Canceled) && err.Error() != `session "missing" not found` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHistoryListCmdPrintsDiscoveredProjects(t *testing.T) {
	codexDir := setupCodexHistoryDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	projectDir := t.TempDir()
	writeCodexSessionFile(t, codexDir, sessionID, projectDir, "build the release")

	cmd := newHistoryListCmd(&rootOptions{}, &codexDir)
	cmd.SetContext(context.Background())
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--pretty"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute history list: %v", err)
	}

	var payload struct {
		Projects []codexhistory.Project `json:"projects"`
	}
	if err := json.Unmarshal([]byte(out.String()), &payload); err != nil {
		t.Fatalf("unmarshal history list output: %v\noutput: %s", err, out.String())
	}
	if len(payload.Projects) != 1 || len(payload.Projects[0].Sessions) != 1 {
		t.Fatalf("unexpected history list payload: %+v", payload)
	}
	if payload.Projects[0].Sessions[0].SessionID != sessionID {
		t.Fatalf("unexpected session id in payload: %+v", payload.Projects[0].Sessions[0])
	}
	gotProjectDir := canonicalPath(t, payload.Projects[0].Path)
	wantProjectDir := canonicalPath(t, projectDir)
	if gotProjectDir != wantProjectDir {
		t.Fatalf("unexpected project path: got %q want %q", gotProjectDir, wantProjectDir)
	}
}

func TestHistoryShowCmdPrintsFormattedSession(t *testing.T) {
	codexDir := setupCodexHistoryDir(t)
	sessionID := "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	projectDir := t.TempDir()
	writeCodexSessionFile(t, codexDir, sessionID, projectDir, "open the dashboard")

	cmd := newHistoryShowCmd(&rootOptions{}, &codexDir)
	cmd.SetContext(context.Background())
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetArgs([]string{sessionID})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute history show: %v", err)
	}

	text := out.String()
	projectLine := ""
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "Project: ") {
			projectLine = strings.TrimPrefix(line, "Project: ")
			break
		}
	}
	if canonicalPath(t, projectLine) != canonicalPath(t, projectDir) {
		t.Fatalf("unexpected project line: got %q want %q", projectLine, projectDir)
	}
	if !strings.Contains(text, "Session: "+sessionID) || !strings.Contains(text, "User:") {
		t.Fatalf("unexpected history show output: %q", text)
	}
}

func setupCodexHistoryDir(t *testing.T) string {
	t.Helper()
	codexhistory.ResetCache()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0o755); err != nil {
		t.Fatalf("create sessions dir: %v", err)
	}
	return root
}

func writeCodexSessionFile(t *testing.T, codexDir string, sessionID string, projectDir string, prompt string) string {
	t.Helper()
	sessionsDir := filepath.Join(codexDir, "sessions")
	filename := "rollout-2026-03-10T10-00-00-" + sessionID + ".jsonl"
	path := filepath.Join(sessionsDir, filename)
	jsonProjectDir := strings.ReplaceAll(projectDir, `\`, `\\`)
	content := strings.Join([]string{
		`{"timestamp":"2026-03-10T10:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonProjectDir + `","source":"cli"}}`,
		`{"timestamp":"2026-03-10T10:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"` + prompt + `"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write session file: %v", err)
	}
	return path
}
