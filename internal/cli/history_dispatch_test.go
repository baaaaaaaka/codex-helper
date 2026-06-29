package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/skills"
	"github.com/baaaaaaaka/codex-helper/internal/tui"
)

func TestRunHistoryTuiSkillsMenuReturnsToTuiLoop(t *testing.T) {
	lockCLITestHooks(t)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	codexDir := filepath.Join(t.TempDir(), "codex")
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevSelect := selectSession
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		selectSession = prevSelect
	})

	ensureCalls := 0
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		ensureCalls++
		return false, config.Config{Version: config.CurrentVersion}, nil
	}

	selectCalls := 0
	selectSession = func(_ context.Context, _ tui.Options) (*tui.Selection, error) {
		selectCalls++
		switch selectCalls {
		case 1:
			return nil, tui.SkillsRequested{}
		case 2:
			return nil, nil
		default:
			t.Fatalf("selectSession called %d times, want 2", selectCalls)
			return nil, nil
		}
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	cmd.SetIn(strings.NewReader("6\n"))
	var out strings.Builder
	cmd.SetOut(&out)
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", codexDir, "", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}
	if selectCalls != 2 {
		t.Fatalf("selectSession calls = %d, want 2", selectCalls)
	}
	if ensureCalls != 2 {
		t.Fatalf("ensureProxyPreference calls = %d, want 2", ensureCalls)
	}
	for _, want := range []string{"Skills", "Back to TUI"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("skills menu output missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunHistoryTuiLoadsAndPersistsAAA(t *testing.T) {
	lockCLITestHooks(t)
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	enabled := true
	proxyDisabled := false
	if err := store.Save(config.Config{ProxyEnabled: &proxyDisabled, AgentAutoApproveEnabled: &enabled}); err != nil {
		t.Fatal(err)
	}
	previousEnsure := ensureProxyPreferenceFunc
	previousSelect := selectSession
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = previousEnsure
		selectSession = previousSelect
	})
	ensureProxyPreferenceFunc = func(_ context.Context, gotStore *config.Store, _ string, _ io.Writer) (bool, config.Config, error) {
		cfg, loadErr := gotStore.Load()
		return false, cfg, loadErr
	}
	selectSession = func(_ context.Context, opts tui.Options) (*tui.Selection, error) {
		if !opts.AAAEnabled || opts.PersistAAA == nil {
			t.Fatalf("AAA options = enabled:%t persist:%v", opts.AAAEnabled, opts.PersistAAA != nil)
		}
		if err := opts.PersistAAA(false); err != nil {
			t.Fatal(err)
		}
		return nil, nil
	}
	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", t.TempDir(), "", 0); err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AgentAutoApproveEnabled == nil || *cfg.AgentAutoApproveEnabled {
		t.Fatalf("persisted AAA preference = %#v, want false", cfg.AgentAutoApproveEnabled)
	}
}

func TestRunHistoryTuiStartsDailyAutoSyncWithoutBlockingSelection(t *testing.T) {
	lockCLITestHooks(t)
	setEffectivePathsHooksForTest(t)
	requireCLIGit(t)
	root := t.TempDir()
	home := filepath.Join(root, "home")
	cache := filepath.Join(root, "cache")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CACHE_HOME", cache)
	t.Setenv("LOCALAPPDATA", cache)
	t.Setenv("CODEX_DIR", "")
	t.Setenv("CODEX_HOME", "")
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	effectivePathsRunningAsRoot = func() bool { return false }

	repo := initCLISkillRepo(t)
	cfgPath := filepath.Join(root, "config", "config.json")
	codexDir := filepath.Join(root, "codex")
	mgr, err := newSkillsManager(&rootOptions{configPath: cfgPath}, codexDir, io.Discard)
	if err != nil {
		t.Fatalf("new skills manager: %v", err)
	}
	source, result, err := mgr.Add(context.Background(), repo, skills.AddOptions{Name: "acme", Ref: "HEAD", Path: "skills/review"})
	if err != nil {
		t.Fatalf("add skill source: %v", err)
	}
	if len(result.Installed) != 1 {
		t.Fatalf("installed skills = %d, want 1", len(result.Installed))
	}
	targetSkill := result.Installed[0].TargetPath
	writeCLIFile(t, filepath.Join(repo, "skills", "review", "SKILL.md"), "---\nname: review\ndescription: Review code\n---\nauto update\n", 0o644)
	cliGitRun(t, repo, "add", "-A")
	cliGitRun(t, repo, "commit", "-m", "auto update")

	prevEnsureProxy := ensureProxyPreferenceFunc
	prevSelect := selectSession
	t.Cleanup(func() {
		ensureProxyPreferenceFunc = prevEnsureProxy
		selectSession = prevSelect
	})
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion}, nil
	}
	selectSession = func(_ context.Context, _ tui.Options) (*tui.Selection, error) {
		deadline := time.Now().Add(historyDispatchTestTimeout(3 * time.Second))
		for {
			data, _ := os.ReadFile(filepath.Join(targetSkill, "SKILL.md"))
			st, _ := mgr.Store.LoadState()
			var state skills.SourceState
			for _, candidate := range st.Sources {
				if candidate.ID == source.ID {
					state = candidate
					break
				}
			}
			if strings.Contains(string(data), "auto update") && state.LastAutoSyncDay != "" {
				return nil, nil
			}
			if time.Now().After(deadline) {
				t.Fatalf("auto-sync did not update installed skill; state=%#v skill=%q", state, string(data))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", codexDir, "", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}
}

func historyDispatchTestTimeout(base time.Duration) time.Duration {
	if runtime.GOOS == "windows" {
		return 30 * time.Second
	}
	return base
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
		_ io.Writer,
	) error {
		called = true
		if cwd != "/tmp/project" || codexPath != "codex-bin" || codexDir != "codex-home" {
			t.Fatalf("unexpected selection args: cwd=%q codexPath=%q codexDir=%q", cwd, codexPath, codexDir)
		}
		if !useProxy {
			t.Fatalf("expected proxy selection to propagate")
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
		_ io.Writer,
	) error {
		called = true
		if session.SessionID != "sid" || project.Path != "/repo" {
			t.Fatalf("unexpected session/project: %+v %+v", session, project)
		}
		if codexPath != "codex-bin" || codexDir != "codex-home" {
			t.Fatalf("unexpected codex args: %q %q", codexPath, codexDir)
		}
		if useProxy {
			t.Fatalf("expected direct selection")
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
	helperSessionID := "aaaaaaaa-bbbb-cccc-dddd-ffffffffffff"
	tempHelperSessionID := "aaaaaaaa-bbbb-cccc-dddd-999999999999"
	projectDir := t.TempDir()
	tempHelperProjectDir := filepath.Join(os.TempDir(), "codex-helper-real-probe")
	writeCodexSessionFile(t, codexDir, sessionID, projectDir, "build the release")
	writeCodexSessionFile(t, codexDir, helperSessionID, projectDir, "[codex-helper-control] helper control prompt")
	writeCodexSessionFile(t, codexDir, tempHelperSessionID, tempHelperProjectDir, "Reply exactly DEFAULT-PROBE-OK")

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
	for _, project := range payload.Projects {
		if strings.Contains(project.Path, "codex-helper-real-probe") {
			t.Fatalf("helper temp debug project remained visible: %+v", project)
		}
	}

	cmd = newHistoryListCmd(&rootOptions{}, &codexDir)
	cmd.SetContext(context.Background())
	out.Reset()
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--pretty", "--include-helper"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute history list --include-helper: %v", err)
	}
	payload = struct {
		Projects []codexhistory.Project `json:"projects"`
	}{}
	if err := json.Unmarshal([]byte(out.String()), &payload); err != nil {
		t.Fatalf("unmarshal include-helper history list output: %v\noutput: %s", err, out.String())
	}
	gotSessions := map[string]bool{}
	for _, project := range payload.Projects {
		for _, session := range project.Sessions {
			gotSessions[session.SessionID] = true
		}
	}
	if len(payload.Projects) != 2 || len(gotSessions) != 3 ||
		!gotSessions[sessionID] || !gotSessions[helperSessionID] || !gotSessions[tempHelperSessionID] {
		t.Fatalf("unexpected include-helper history list payload: %+v", payload)
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
