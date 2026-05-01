package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestTeamsStatusReportsLocalStateWithoutCreatingDefaultState(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)

	registryPath := filepath.Join(tmp, "teams-registry.json")
	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "--registry", registryPath, "status"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute teams status: %v", err)
	}

	got := out.String()
	for _, want := range []string{
		"Teams status",
		"Control chat: unavailable",
		"Sessions: 0 total, 0 active",
		"State summary: unavailable",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("teams status output missing %q:\n%s", want, got)
		}
	}
	if _, err := os.Stat(filepath.Join(configBase, "codex-helper", "teams", "state.json")); !os.IsNotExist(err) {
		t.Fatalf("teams status should not create state file, stat err = %v", err)
	}
	if _, err := os.Stat(registryPath); !os.IsNotExist(err) {
		t.Fatalf("teams status should not create registry file, stat err = %v", err)
	}
}

func TestTeamsStatusFindsScopedControlChatState(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	scopedStatePath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope-a", "state.json")
	st, err := teamsstore.Open(scopedStatePath)
	if err != nil {
		t.Fatalf("Open scoped store: %v", err)
	}
	now := time.Now()
	if err := st.Update(context.Background(), func(state *teamsstore.State) error {
		state.ControlChat = teamsstore.ControlChatBinding{
			TeamsChatID:    "scoped-control",
			TeamsChatURL:   "https://teams.example/scoped-control",
			TeamsChatTopic: "🏠 Codex Control - host",
			BoundAt:        now,
			UpdatedAt:      now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed scoped state: %v", err)
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "status"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute teams status: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Control chat: configured",
		"Control chat URL: https://teams.example/scoped-control",
		"Control chat source: " + scopedStatePath,
		"Teams listener: stopped - Teams messages are not being read.",
		"messages sent from your phone cannot start a stopped local listener",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("teams status output missing %q:\n%s", want, got)
		}
	}
}

func TestTeamsSetupPrintsSafeChecklistWithoutState(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)

	out := executeRootForTeamsTest(t, "teams", "setup")
	for _, want := range []string{
		"Teams setup checklist",
		"teams auth",
		"teams doctor --live",
		"teams control",
		"may create a Teams chat and send a ready message",
		"codex-proxy teams service enable",
		"codex-proxy teams service start",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("setup output missing %q:\n%s", want, out)
		}
	}
	if _, err := os.Stat(filepath.Join(configBase, "codex-helper", "teams", "state.json")); !os.IsNotExist(err) {
		t.Fatalf("teams setup should not create state file, stat err = %v", err)
	}
}

func TestTeamsStatusAndControlPrintShowControlChatDetails(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	registryPath := filepath.Join(tmp, "teams-registry.json")
	controlTopic := teams.ControlChatTitle(teams.ChatTitleOptions{MachineLabel: "host"})
	reg := teams.Registry{
		Version:          1,
		ControlChatID:    "control-chat",
		ControlChatTopic: controlTopic,
		ControlChatURL:   "https://teams.example/control",
		Chats:            map[string]teams.ChatState{},
	}
	if err := teams.SaveRegistry(registryPath, reg); err != nil {
		t.Fatalf("SaveRegistry error: %v", err)
	}

	status := executeRootForTeamsTest(t, "teams", "--registry", registryPath, "status")
	if !strings.Contains(status, "Control chat title: "+controlTopic) || !strings.Contains(status, "Control chat URL: https://teams.example/control") {
		t.Fatalf("status did not print control details:\n%s", status)
	}
	printed := executeRootForTeamsTest(t, "teams", "--registry", registryPath, "control", "--print")
	if !strings.Contains(printed, "Teams control chat: https://teams.example/control") || !strings.Contains(printed, "Title: "+controlTopic) || !strings.Contains(printed, "Chat ID: control-chat") {
		t.Fatalf("control --print output mismatch:\n%s", printed)
	}
	if !strings.Contains(printed, "new <directory> -- <title>") || !strings.Contains(printed, "Teams messages are not read after the local listener stops") {
		t.Fatalf("control --print examples are not mobile-safe:\n%s", printed)
	}
}

func TestTeamsAuthStatusDoesNotPrintCachedSecrets(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cachePath := filepath.Join(tmp, "token.json")
	t.Setenv("CODEX_HELPER_TEAMS_TOKEN_CACHE", cachePath)
	t.Setenv("CODEX_HELPER_TEAMS_SCOPES", "")
	secretAccess := "access-secret-value"
	secretRefresh := "refresh-secret-value"
	data := `{"access_token":"` + secretAccess + `","refresh_token":"` + secretRefresh + `","expires_at":` + unixString(time.Now().Add(time.Hour).Unix()) + `}`
	if err := os.WriteFile(cachePath, []byte(data), 0o600); err != nil {
		t.Fatalf("write token cache: %v", err)
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "auth", "status"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute teams auth status: %v", err)
	}

	got := out.String()
	if !strings.Contains(got, "Teams auth cache: present") {
		t.Fatalf("expected present auth cache, got:\n%s", got)
	}
	if strings.Contains(got, secretAccess) || strings.Contains(got, secretRefresh) {
		t.Fatalf("teams auth status printed a cached secret:\n%s", got)
	}
}

func TestTeamsPauseDrainResumePersistServiceControl(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	out := executeRootForTeamsTest(t, "teams", "pause", "upgrade")
	if !strings.Contains(out, "Teams processing paused: upgrade") {
		t.Fatalf("pause output mismatch:\n%s", out)
	}
	out = executeRootForTeamsTest(t, "teams", "status")
	if !strings.Contains(out, "Service control: paused (upgrade)") {
		t.Fatalf("status did not show paused control:\n%s", out)
	}

	out = executeRootForTeamsTest(t, "teams", "drain", "restart")
	if !strings.Contains(out, "Teams processing draining: restart") {
		t.Fatalf("drain output mismatch:\n%s", out)
	}
	out = executeRootForTeamsTest(t, "teams", "status")
	if !strings.Contains(out, "Service control: paused, draining (restart)") {
		t.Fatalf("status did not show paused+draining control:\n%s", out)
	}

	out = executeRootForTeamsTest(t, "teams", "resume")
	if !strings.Contains(out, "Teams processing resumed") {
		t.Fatalf("resume output mismatch:\n%s", out)
	}
	out = executeRootForTeamsTest(t, "teams", "status")
	if !strings.Contains(out, "Service control: running") {
		t.Fatalf("status did not show running control:\n%s", out)
	}
}

func TestTeamsStatusReportsPollDiagnostics(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("openTeamsStore error: %v", err)
	}
	if _, err := st.RecordChatPollSuccess(context.Background(), "chat-1", time.Now(), true, true, 50); err != nil {
		t.Fatalf("RecordChatPollSuccess error: %v", err)
	}
	if err := st.RecordChatPollError(context.Background(), "chat-2", "Graph unavailable"); err != nil {
		t.Fatalf("RecordChatPollError error: %v", err)
	}

	out := executeRootForTeamsTest(t, "teams", "status")
	if !strings.Contains(out, "Poll summary: 2 chats") || !strings.Contains(out, "1 errors") || !strings.Contains(out, "1 window warnings") {
		t.Fatalf("status did not show poll diagnostics:\n%s", out)
	}
}

func TestTeamsDoctorLiveUsesExplicitOptIn(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	prevLive := runTeamsDoctorLiveCheck
	prevProbe := runTeamsAppServerProbe
	liveCalled := false
	probeCalled := false
	runTeamsDoctorLiveCheck = func(cmd *cobra.Command, _ string) error {
		liveCalled = true
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Graph: ok as Test <test@example.com>")
		return nil
	}
	runTeamsAppServerProbe = func(cmd *cobra.Command, opts teamsAppServerProbeOptions) error {
		probeCalled = true
		if opts.CodexPath != "/tmp/codex" || opts.WorkDir != tmp || opts.Timeout != time.Second || opts.Runs != 2 {
			t.Fatalf("probe args = %#v", opts)
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Codex app-server: ok (2 cold probe(s), min 1ms, max 2ms, total 3ms)")
		return nil
	}
	t.Cleanup(func() {
		runTeamsDoctorLiveCheck = prevLive
		runTeamsAppServerProbe = prevProbe
	})

	out := executeRootForTeamsTest(t, "teams", "doctor")
	if liveCalled {
		t.Fatal("doctor should not run live checks without --live")
	}
	if probeCalled {
		t.Fatal("doctor should not run app-server probe without --appserver-probe")
	}
	if !strings.Contains(out, "Graph: not checked") {
		t.Fatalf("doctor missing local-only graph message:\n%s", out)
	}
	if !strings.Contains(out, "Codex app-server: not checked") {
		t.Fatalf("doctor missing local-only appserver message:\n%s", out)
	}
	if !strings.Contains(out, "Teams auth cache: missing") || !strings.Contains(out, "Auth next step") {
		t.Fatalf("doctor missing auth guidance:\n%s", out)
	}

	out = executeRootForTeamsTest(t, "teams", "doctor", "--live")
	if !liveCalled {
		t.Fatal("doctor --live did not call live check")
	}
	if !strings.Contains(out, "Graph: ok as Test") {
		t.Fatalf("doctor --live missing live output:\n%s", out)
	}

	out = executeRootForTeamsTest(t, "teams", "doctor", "--appserver-probe", "--codex-path", "/tmp/codex", "--workdir", tmp, "--probe-timeout", "1s", "--appserver-probe-runs", "2")
	if !probeCalled {
		t.Fatal("doctor --appserver-probe did not call app-server probe")
	}
	if !strings.Contains(out, "Codex app-server: ok") {
		t.Fatalf("doctor --appserver-probe missing probe output:\n%s", out)
	}
}

func TestTeamsLogoutRemovesOnlyLocalAuthCache(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cachePath := filepath.Join(tmp, "token.json")
	t.Setenv("CODEX_HELPER_TEAMS_TOKEN_CACHE", cachePath)
	t.Setenv("CODEX_HELPER_TEAMS_SCOPES", "")
	if err := os.WriteFile(cachePath, []byte(`{"access_token":"secret"}`), 0o600); err != nil {
		t.Fatalf("write token cache: %v", err)
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "auth", "logout"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute teams auth logout: %v", err)
	}
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatalf("expected auth cache to be removed, stat err = %v", err)
	}
	if strings.Contains(out.String(), "secret") {
		t.Fatalf("teams auth logout printed token contents:\n%s", out.String())
	}
}

func TestTeamsLogoutRefusesToRemoveNonTokenCacheOverride(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cachePath := filepath.Join(tmp, "ordinary.json")
	t.Setenv("CODEX_HELPER_TEAMS_TOKEN_CACHE", cachePath)
	t.Setenv("CODEX_HELPER_TEAMS_SCOPES", "")
	if err := os.WriteFile(cachePath, []byte(`{"hello":"world"}`), 0o600); err != nil {
		t.Fatalf("write ordinary json: %v", err)
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "auth", "logout"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "does not look like") {
		t.Fatalf("expected non-token cache error, got %v", err)
	}
	if _, statErr := os.Stat(cachePath); statErr != nil {
		t.Fatalf("ordinary file should not be removed, stat err = %v", statErr)
	}
}

func TestTeamsFileWriteAuthStatusAndLogoutUseSeparateCache(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cachePath := filepath.Join(tmp, "file-write-token.json")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE", cachePath)
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_SCOPES", "")
	secretAccess := "file-write-access-secret"
	if err := os.WriteFile(cachePath, []byte(`{"access_token":"`+secretAccess+`","expires_at":`+unixString(time.Now().Add(time.Hour).Unix())+`}`), 0o600); err != nil {
		t.Fatalf("write file-write token cache: %v", err)
	}

	out := executeRootForTeamsTest(t, "teams", "auth", "file-write-status")
	if !strings.Contains(out, "Teams file-write auth cache: present") || !strings.Contains(out, cachePath) {
		t.Fatalf("file-write status output mismatch:\n%s", out)
	}
	if strings.Contains(out, secretAccess) {
		t.Fatalf("file-write status printed token contents:\n%s", out)
	}

	out = executeRootForTeamsTest(t, "teams", "auth", "file-write-logout")
	if !strings.Contains(out, "Removed Teams file-write auth cache") {
		t.Fatalf("file-write logout output mismatch:\n%s", out)
	}
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatalf("expected file-write auth cache to be removed, stat err=%v", err)
	}
}

func TestResolveTeamsSendFileChatIDRequiresExplicitTargetWhenAmbiguous(t *testing.T) {
	tmp := t.TempDir()
	registryPath := filepath.Join(tmp, "teams-registry.json")
	reg := teams.Registry{Version: 1, Sessions: []teams.Session{
		{ID: "s001", ChatID: "chat-1", Status: "active", UpdatedAt: time.Now()},
		{ID: "s002", ChatID: "chat-2", Status: "active", UpdatedAt: time.Now().Add(time.Second)},
	}}
	if err := teams.SaveRegistry(registryPath, reg); err != nil {
		t.Fatalf("SaveRegistry error: %v", err)
	}
	if _, err := resolveTeamsSendFileChatID(registryPath, "", ""); err == nil {
		t.Fatal("expected ambiguous target error")
	}
	got, err := resolveTeamsSendFileChatID(registryPath, "s001", "")
	if err != nil {
		t.Fatalf("resolve by session error: %v", err)
	}
	if got != "chat-1" {
		t.Fatalf("resolved chat = %q, want chat-1", got)
	}
	got, err = resolveTeamsSendFileChatID(registryPath, "", "explicit-chat")
	if err != nil {
		t.Fatalf("resolve explicit chat error: %v", err)
	}
	if got != "explicit-chat" {
		t.Fatalf("resolved explicit chat = %q", got)
	}
}

func TestResolveTeamsSendFileChatIDDoesNotImplicitlyPickSingleActiveSession(t *testing.T) {
	tmp := t.TempDir()
	registryPath := filepath.Join(tmp, "teams-registry.json")
	reg := teams.Registry{Version: 1, Sessions: []teams.Session{
		{ID: "s001", ChatID: "chat-1", Status: "active", UpdatedAt: time.Now()},
	}}
	if err := teams.SaveRegistry(registryPath, reg); err != nil {
		t.Fatalf("SaveRegistry error: %v", err)
	}
	if _, err := resolveTeamsSendFileChatID(registryPath, "", ""); err == nil || !strings.Contains(err.Error(), "--session or --chat-id") {
		t.Fatalf("resolve without explicit target error = %v, want explicit target requirement", err)
	}
}

func TestTeamsSendFileRequiresYesForExplicitChatID(t *testing.T) {
	lockCLITestHooks(t)

	out, err := executeRootForTeamsTestAllowError(t, "teams", "send-file", "--chat-id", "chat-1", "report.txt")
	if err == nil {
		t.Fatalf("send-file explicit chat-id succeeded without --yes, output:\n%s", out)
	}
	if !strings.Contains(err.Error(), "refusing explicit --chat-id without --yes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTeamsSendFileMissingTokenFailsClosedWithoutDeviceLogin(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	file := filepath.Join(tmp, "report.txt")
	if err := os.WriteFile(file, []byte("report"), 0o600); err != nil {
		t.Fatalf("write report: %v", err)
	}
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE", filepath.Join(tmp, "missing-file-token.json"))

	out, err := executeRootForTeamsTestAllowError(t, "teams", "send-file", "--chat-id", "chat-1", "--yes", "--allow-local-path", file)
	if err == nil {
		t.Fatalf("send-file succeeded without file-write token, output:\n%s", out)
	}
	if !strings.Contains(err.Error(), "auth cache is missing") || !strings.Contains(err.Error(), "teams auth file-write") {
		t.Fatalf("unexpected send-file error: %v\noutput:\n%s", err, out)
	}
	if strings.Contains(out, "login.microsoft") || strings.Contains(out, "device login") {
		t.Fatalf("send-file should not start interactive device login, output:\n%s", out)
	}
}

func TestTeamsRecoverMissingStateIsLocalNoop(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)

	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "recover"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute teams recover: %v", err)
	}
	if !strings.Contains(out.String(), "Teams state unavailable") {
		t.Fatalf("expected missing state message, got:\n%s", out.String())
	}
	if _, err := os.Stat(filepath.Join(configBase, "codex-helper", "teams", "state.json")); !os.IsNotExist(err) {
		t.Fatalf("teams recover should not create missing state file, stat err = %v", err)
	}
}

func TestTeamsRecoverRefusesLiveOwnerUnlessForced(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	st := seedRecoverableTeamsState(t)
	owner, err := teamsstore.CurrentOwner("v-test", "s1", "turn:manual", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, time.Now()); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	out, err := executeRootForTeamsTestAllowError(t, "teams", "recover")
	if err == nil {
		t.Fatalf("teams recover succeeded with live owner, output:\n%s", out)
	}
	if !strings.Contains(err.Error(), "Teams bridge owner is active") {
		t.Fatalf("unexpected recover error: %v", err)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns["turn:manual"].Status; got != teamsstore.TurnStatusQueued {
		t.Fatalf("turn status = %q, want queued after refused recover", got)
	}

	out = executeRootForTeamsTest(t, "teams", "recover", "--force")
	if !strings.Contains(out, "Recovered interrupted turns: 1") {
		t.Fatalf("force recover output mismatch:\n%s", out)
	}
	if _, ok, err := st.ReadOwner(context.Background()); err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	} else if ok {
		t.Fatal("owner should be cleared after forced recover")
	}
}

func TestTeamsRecoverAllowsStaleOwner(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	st := seedRecoverableTeamsState(t)
	old := time.Now().Add(-time.Hour)
	owner, err := teamsstore.CurrentOwner("v-test", "s1", "turn:manual", old)
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, old); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	out := executeRootForTeamsTest(t, "teams", "recover", "--stale-after", "1ms")
	if !strings.Contains(out, "Recovered interrupted turns: 1") {
		t.Fatalf("stale recover output mismatch:\n%s", out)
	}
	if _, ok, err := st.ReadOwner(context.Background()); err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	} else if ok {
		t.Fatal("owner should be cleared after stale recover")
	}
}

func seedRecoverableTeamsState(t *testing.T) *teamsstore.Store {
	t.Helper()
	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("openTeamsStore error: %v", err)
	}
	ctx := context.Background()
	if _, _, err := st.CreateSession(ctx, teamsstore.SessionContext{
		ID:          "s1",
		Status:      teamsstore.SessionStatusActive,
		TeamsChatID: "chat-1",
	}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	if _, _, err := st.QueueTurn(ctx, teamsstore.Turn{ID: "turn:manual", SessionID: "s1"}); err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}
	return st
}

func unixString(v int64) string {
	return strconv.FormatInt(v, 10)
}

func executeRootForTeamsTest(t *testing.T, args ...string) string {
	t.Helper()
	cmd := newRootCmd()
	cmd.SetArgs(args)
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute %v: %v\n%s", args, err, out.String())
	}
	return out.String()
}

func executeRootForTeamsTestAllowError(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := newRootCmd()
	cmd.SetArgs(args)
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	return out.String(), err
}
