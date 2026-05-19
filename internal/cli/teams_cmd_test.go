package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestTeamsStatusReportsLocalStateWithoutCreatingDefaultState(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "codex-proxy")
	if runtime.GOOS == "windows" {
		exe = filepath.Join(tmp, "codex-proxy.exe")
	}
	if err := os.WriteFile(exe, []byte("#!/bin/sh\necho 'codex-proxy version 0.1.0-test'\n"), 0o700); err != nil {
		t.Fatalf("write fake helper: %v", err)
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    runtime.GOOS,
		exe:     exe,
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd"),
		runner:  &recordingTeamsServiceRunner{output: []byte("inactive\n")},
	})

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
		"OS service:",
		"Helper raw executable: " + exe,
		"Helper stable executable: " + exe,
		"Helper version:",
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

func TestRunTeamsServiceRetryLoopRetriesRecoverableErrors(t *testing.T) {
	lockCLITestHooks(t)

	prevDelay := teamsRunServiceRetryDelay
	prevSleep := teamsRunServiceSleep
	t.Cleanup(func() {
		teamsRunServiceRetryDelay = prevDelay
		teamsRunServiceSleep = prevSleep
	})
	teamsRunServiceRetryDelay = 123 * time.Millisecond
	var sleeps []time.Duration
	teamsRunServiceSleep = func(_ context.Context, delay time.Duration) error {
		sleeps = append(sleeps, delay)
		return nil
	}

	attempts := 0
	var errOut bytes.Buffer
	err := runTeamsServiceRetryLoop(context.Background(), &errOut, func() error {
		attempts++
		if attempts == 1 {
			return &teams.GraphStatusError{Method: "GET", Path: "/me", StatusCode: 502}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runTeamsServiceRetryLoop error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if len(sleeps) != 1 || sleeps[0] != 123*time.Millisecond {
		t.Fatalf("sleeps = %v, want one 123ms sleep", sleeps)
	}
	if !strings.Contains(errOut.String(), "recoverable error") || !strings.Contains(errOut.String(), "retrying in 123ms") {
		t.Fatalf("retry output mismatch:\n%s", errOut.String())
	}
}

func TestRunTeamsServiceRetryLoopDoesNotRetryPermanentErrors(t *testing.T) {
	lockCLITestHooks(t)

	prevSleep := teamsRunServiceSleep
	t.Cleanup(func() { teamsRunServiceSleep = prevSleep })
	teamsRunServiceSleep = func(context.Context, time.Duration) error {
		t.Fatal("permanent errors should not sleep or retry")
		return nil
	}

	attempts := 0
	permanent := fmt.Errorf("invalid Teams configuration")
	err := runTeamsServiceRetryLoop(context.Background(), nil, func() error {
		attempts++
		return permanent
	})
	if err != permanent {
		t.Fatalf("error = %v, want permanent error", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestTeamsRunShouldRetryInProcessOnlyForServiceMode(t *testing.T) {
	lockCLITestHooks(t)

	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "")
	if teamsRunShouldRetryInProcess(false) {
		t.Fatal("foreground teams run should not retry internally")
	}
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	if !teamsRunShouldRetryInProcess(false) {
		t.Fatal("background service teams run should retry recoverable errors internally")
	}
	if teamsRunShouldRetryInProcess(true) {
		t.Fatal("teams run --once should not enter service retry loop")
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

func TestFormatTeamsHelperVersionStatusReportsOwnerEntryAndPending(t *testing.T) {
	lockCLITestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell fixture is not executable on Windows")
	}
	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy")
	if err := os.WriteFile(exe, []byte("#!/bin/sh\necho 'codex-proxy version 0.1.0-rc.68 (old)'\n"), 0o700); err != nil {
		t.Fatalf("write fake helper: %v", err)
	}
	pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.73_linux_"+runtime.GOARCH+".123")
	if err := os.WriteFile(pending, []byte("new"), 0o600); err != nil {
		t.Fatalf("write pending helper: %v", err)
	}
	failedPending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.74_linux_"+runtime.GOARCH+".123")
	if err := os.WriteFile(failedPending, []byte("failed"), 0o600); err != nil {
		t.Fatalf("write failed pending helper: %v", err)
	}
	failedStatus := append([]byte{0xef, 0xbb, 0xbf}, []byte(`{"version":1,"status":"failed","message":"file is locked"}`)...)
	if err := os.WriteFile(failedPending+".activation.json", failedStatus, 0o600); err != nil {
		t.Fatalf("write failed pending status: %v", err)
	}
	stalePending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.67_linux_"+runtime.GOARCH+".123")
	if err := os.WriteFile(stalePending, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale pending helper: %v", err)
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exe,
		unitDir: filepath.Join(tmp, "systemd"),
	})

	got := formatTeamsHelperVersionStatus(context.Background(), []teamsstore.OwnerMetadata{{
		HelperVersion: "v0.1.0-rc.68",
	}})
	for _, want := range []string{
		"owner=v0.1.0-rc.68",
		"entry=v0.1.0-rc.68",
		"pending=failed:v0.1.0-rc.74, newer:v0.1.0-rc.73, stale:v0.1.0-rc.67",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("helper version status missing %q: %s", want, got)
		}
	}
}

func TestResolveTeamsHelperExecutableStatusReportsRawStableAndActivationPending(t *testing.T) {
	lockCLITestHooks(t)
	if runtime.GOOS == "windows" {
		t.Skip("NFS-style raw path fixture is Unix-focused")
	}
	tmp := t.TempDir()
	stable := filepath.Join(tmp, "codex-proxy")
	if err := os.WriteFile(stable, []byte("#!/bin/sh\necho 'codex-proxy version 0.1.0-rc.70'\n"), 0o700); err != nil {
		t.Fatalf("write stable helper: %v", err)
	}
	raw := filepath.Join(tmp, ".nfs802014de01c482a800000492")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos: "linux",
		exe:  raw,
		cwd:  tmp,
	})

	got := resolveTeamsHelperExecutableStatus()
	if got.Raw != raw || got.Stable != stable || !got.ActivationPending || got.Source != string(helperpath.SourceExecutable) {
		t.Fatalf("path status = %#v, want raw %q stable %q pending executable source", got, raw, stable)
	}
}

func TestTeamsWorkflowStatusRedactsWebhookURL(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	secretURL := "https://workflow.example.test/secret-token"
	secretPath := filepath.Join(tmp, "workflow-url")
	if err := os.WriteFile(secretPath, []byte(secretURL), 0o600); err != nil {
		t.Fatalf("write secret URL file: %v", err)
	}
	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("openTeamsStore error: %v", err)
	}
	if err := st.Update(context.Background(), func(state *teamsstore.State) error {
		state.Workflow = teamsstore.WorkflowNotificationConfig{
			Enabled:               true,
			ControlWebhookURLFile: secretPath,
			ControlChatID:         "control-chat",
			UpdatedAt:             time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed workflow state: %v", err)
	}

	got := executeRootForTeamsTest(t, "teams", "workflow", "status")
	for _, want := range []string{
		"Teams workflow notifications: enabled",
		"Bound control chat ID: control-chat",
		"Webhook URL file: configured (ok)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("teams workflow status missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, secretURL) {
		t.Fatalf("teams workflow status leaked raw webhook URL:\n%s", got)
	}
}

func TestTeamsWorkflowStatusReadsSidecarConfig(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	scopeID := "scope-workflow-sidecar"
	statePath, err := teams.DefaultStorePathForScope(scopeID)
	if err != nil {
		t.Fatalf("DefaultStorePathForScope: %v", err)
	}
	st, err := teamsstore.Open(statePath)
	if err != nil {
		t.Fatalf("Open scoped store: %v", err)
	}
	if err := st.Update(context.Background(), func(state *teamsstore.State) error {
		state.Scope = teamsstore.ScopeIdentity{ID: scopeID}
		return nil
	}); err != nil {
		t.Fatalf("seed scoped state: %v", err)
	}
	secretURL := "https://workflow.example.test/sidecar-token"
	secretPath := filepath.Join(tmp, "workflow-sidecar-url")
	if err := os.WriteFile(secretPath, []byte(secretURL), 0o600); err != nil {
		t.Fatalf("write secret URL file: %v", err)
	}
	sidecarPath, err := teams.WorkflowNotificationConfigFilePathForScope(scopeID)
	if err != nil {
		t.Fatalf("WorkflowNotificationConfigFilePathForScope: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(sidecarPath), 0o700); err != nil {
		t.Fatalf("mkdir sidecar dir: %v", err)
	}
	sidecar := fmt.Sprintf(`{"version":1,"workflow":{"enabled":true,"control_webhook_url_file":%q,"control_chat_id":"control-sidecar"}}`, secretPath)
	if err := os.WriteFile(sidecarPath, []byte(sidecar), 0o600); err != nil {
		t.Fatalf("write sidecar config: %v", err)
	}

	got := executeRootForTeamsTest(t, "teams", "workflow", "status")
	for _, want := range []string{
		"Teams workflow notifications: enabled",
		"Bound control chat ID: control-sidecar",
		"Webhook URL file: configured (ok)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("teams workflow sidecar status missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, secretURL) {
		t.Fatalf("teams workflow sidecar status leaked raw webhook URL:\n%s", got)
	}
}

func TestTeamsStatusPrefersScopedDrainingServiceControl(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	legacyStatePath := filepath.Join(configBase, "codex-helper", "teams", "state.json")
	legacyStore, err := teamsstore.Open(legacyStatePath)
	if err != nil {
		t.Fatalf("Open legacy store: %v", err)
	}
	if _, err := legacyStore.ClearDrain(context.Background()); err != nil {
		t.Fatalf("ClearDrain legacy: %v", err)
	}

	scopedStatePath := filepath.Join(configBase, "codex-helper", "teams", "scopes", "scope-a", "state.json")
	scopedStore, err := teamsstore.Open(scopedStatePath)
	if err != nil {
		t.Fatalf("Open scoped store: %v", err)
	}
	if _, err := scopedStore.SetDraining(context.Background(), teamsstore.HelperReloadReason); err != nil {
		t.Fatalf("SetDraining scoped: %v", err)
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "status"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute teams status: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "Service control: draining (codex-proxy reload)") {
		t.Fatalf("teams status did not surface scoped drain:\n%s", got)
	}
}

func TestTeamsStatusCountsScopedRegistrySessions(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	scopedRegistryPath := filepath.Join(cacheBase, "codex-helper", "teams", "scopes", "scope-a", "registry.json")
	if err := teams.SaveRegistry(scopedRegistryPath, teams.Registry{
		Version:          1,
		ControlChatID:    "scoped-control",
		ControlChatURL:   "https://teams.example/scoped-control",
		ControlChatTopic: "🏠 Codex Control - host",
		Sessions: []teams.Session{{
			ID:        "s001",
			ChatID:    "work-chat",
			ChatURL:   "https://teams.example/work-chat",
			Topic:     "💬 Codex Work - s001 - host",
			Status:    "active",
			UpdatedAt: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("SaveRegistry scoped registry: %v", err)
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
		"Control chat: configured, listener stopped",
		"Control chat URL: https://teams.example/scoped-control",
		"Sessions: 1 total, 1 active",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("teams status output missing %q:\n%s", want, got)
		}
	}
}

func TestTeamsStatusTreatsDeadLocalOwnerAsStopped(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("openTeamsStore error: %v", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname error: %v", err)
	}
	now := time.Now()
	owner := teamsstore.OwnerMetadata{
		PID:            2147483647,
		Hostname:       hostname,
		ExecutablePath: "/missing/codex-proxy",
		HelperVersion:  "v-test",
		StartedAt:      now,
		LastHeartbeat:  now,
	}
	if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, time.Hour, now); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	if err := st.Update(context.Background(), func(state *teamsstore.State) error {
		state.ControlChat = teamsstore.ControlChatBinding{
			TeamsChatID:    "control-chat",
			TeamsChatTopic: "🏠 Codex Control - host",
			TeamsChatURL:   "https://teams.example/control",
			BoundAt:        now,
			UpdatedAt:      now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed control chat: %v", err)
	}

	got := executeRootForTeamsTest(t, "teams", "status")
	for _, want := range []string{
		"Control chat: configured, listener stopped",
		"Bridge: not running",
		"Teams listener: stopped",
		"Owner: none",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("teams status output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Bridge: running") {
		t.Fatalf("teams status reported dead owner as running:\n%s", got)
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
		"may create a Teams chat and send an @mention plus a ready message",
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

func TestTeamsProbeChatHelpIsExplicitlySideEffectSafe(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)

	out := executeRootForTeamsTest(t, "teams", "probe-chat", "--help")
	for _, want := range []string{
		"Probe an external Teams chat without binding helper state",
		"--send-test",
		"--webhook-url-file",
		"By default it is read-only",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("probe-chat help missing %q:\n%s", want, out)
		}
	}
	if _, err := os.Stat(filepath.Join(configBase, "codex-helper", "teams", "state.json")); !os.IsNotExist(err) {
		t.Fatalf("teams probe-chat --help should not create state file, stat err = %v", err)
	}
}

func TestTeamsAuthConfigWritesLocalClientIDs(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)

	out := executeRootForTeamsTest(t,
		"teams", "auth", "config",
		"--tenant-id", "tenant-config",
		"--read-client-id", "read-client-config",
		"--chat-client-id", "chat-client-config",
	)
	configPath := filepath.Join(configBase, "codex-helper", "teams-auth.json")
	for _, want := range []string{
		"Saved Teams auth config: " + configPath,
		"Tenant ID: configured",
		"Read client ID: configured",
		"Chat write client ID: configured",
		"File write client ID: using chat write client",
		"Full client ID: using chat write client",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("auth config output missing %q:\n%s", want, out)
		}
	}
	cfg, err := teams.LoadTeamsAuthConfigFile(configPath)
	if err != nil {
		t.Fatalf("LoadTeamsAuthConfigFile error: %v", err)
	}
	if cfg.TenantID != "tenant-config" || cfg.Read.ClientID != "read-client-config" || cfg.ChatWrite.ClientID != "chat-client-config" {
		t.Fatalf("saved auth config = %#v", cfg)
	}
	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("stat auth config: %v", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("auth config permissions = %03o, want private", info.Mode().Perm())
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
	if !strings.Contains(printed, "new <directory>") || !strings.Contains(printed, "Teams messages are not read after the local listener stops") {
		t.Fatalf("control --print examples are not mobile-safe:\n%s", printed)
	}
}

func TestTeamsAuthStatusDoesNotPrintCachedSecrets(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cachePath := filepath.Join(tmp, "token.json")
	setTeamsAuthIDsForCLITest(t)
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
	runTeamsDoctorLiveCheck = func(cmd *cobra.Command, _ *rootOptions, _ string) error {
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

func TestTeamsDoctorAppServerProbeWithoutAuthConfigStaysLocalOnly(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	for _, name := range []string{
		"CODEX_HELPER_TEAMS_TENANT_ID",
		"CODEX_HELPER_TEAMS_CLIENT_ID",
		"CODEX_HELPER_TEAMS_READ_CLIENT_ID",
		"CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID",
		"CODEX_HELPER_TEAMS_FULL_CLIENT_ID",
	} {
		t.Setenv(name, "")
	}

	prevProbe := runTeamsAppServerProbe
	runTeamsAppServerProbe = func(cmd *cobra.Command, _ teamsAppServerProbeOptions) error {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Codex app-server: ok (2 cold probe(s), min 1ms, max 2ms, total 3ms)")
		return nil
	}
	t.Cleanup(func() { runTeamsAppServerProbe = prevProbe })

	out := executeRootForTeamsTest(t, "teams", "doctor", "--appserver-probe")
	for _, want := range []string{
		"Graph: not checked",
		"Codex app-server: ok",
		"Teams read auth cache: not configured",
		"Teams auth cache: not configured",
		"Teams file-write auth cache: not configured",
		"Next steps: run `codex-proxy teams setup`",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, out)
		}
	}
}

func TestTeamsLogoutRemovesOnlyLocalAuthCache(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cachePath := filepath.Join(tmp, "token.json")
	setTeamsAuthIDsForCLITest(t)
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
	setTeamsAuthIDsForCLITest(t)
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
	setTeamsAuthIDsForCLITest(t)
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
	if !strings.Contains(out, "Cleared stale owners: 1") {
		t.Fatalf("stale recover should report cleared owner:\n%s", out)
	}
	if !strings.Contains(out, "Recovered interrupted turns: 1") {
		t.Fatalf("stale recover output mismatch:\n%s", out)
	}
	if _, ok, err := st.ReadOwner(context.Background()); err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	} else if ok {
		t.Fatal("owner should be cleared after stale recover")
	}
}

func TestTeamsRecoverReportsRemainingUpgradeBlockers(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
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
	if _, _, err := st.QueueOutbox(ctx, teamsstore.OutboxMessage{
		ID:          "outbox:blocking",
		SessionID:   "s1",
		TeamsChatID: "chat-1",
		Kind:        "answer",
		Body:        "pending answer",
	}); err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}

	out := executeRootForTeamsTest(t, "teams", "recover")
	if !strings.Contains(out, "Recovered interrupted turns: 0") {
		t.Fatalf("recover should report no interrupted turns:\n%s", out)
	}
	if !strings.Contains(out, "Preserved protected outbox: 1") {
		t.Fatalf("recover should report protected outbox preservation:\n%s", out)
	}
	if !strings.Contains(out, "Remaining upgrade blockers: 1") ||
		!strings.Contains(out, "outbox s1 outbox:blocking status=queued kind=answer") {
		t.Fatalf("recover should report remaining outbox blocker:\n%s", out)
	}
}

func TestTeamsRecoverDoesNotReportNotificationOutboxAsUpgradeBlockers(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
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
	for _, msg := range []teamsstore.OutboxMessage{
		{
			ID:          "outbox:turn-1:codex-status-001",
			SessionID:   "s1",
			TeamsChatID: "chat-1",
			Kind:        "codex-status-001",
			Body:        "still running",
			Status:      teamsstore.OutboxStatusQueued,
		},
		{
			ID:          "outbox:turn-1:interrupted",
			SessionID:   "s1",
			TeamsChatID: "chat-1",
			Kind:        "interrupted",
			Body:        "ambiguous after restart",
			Status:      teamsstore.OutboxStatusQueued,
		},
	} {
		if _, _, err := st.QueueOutbox(ctx, msg); err != nil {
			t.Fatalf("QueueOutbox %s error: %v", msg.ID, err)
		}
	}

	out := executeRootForTeamsTest(t, "teams", "recover")
	if !strings.Contains(out, "Recovered interrupted turns: 0") {
		t.Fatalf("recover should report no interrupted turns:\n%s", out)
	}
	if !strings.Contains(out, "Superseded transient outbox: 2") {
		t.Fatalf("recover should report transient outbox reconciliation:\n%s", out)
	}
	if strings.Contains(out, "Remaining upgrade blockers") {
		t.Fatalf("notification outbox should not remain upgrade-blocking:\n%s", out)
	}
	state, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	for _, id := range []string{"outbox:turn-1:codex-status-001", "outbox:turn-1:interrupted"} {
		if got := state.OutboxMessages[id].Status; got != teamsstore.OutboxStatusSkipped {
			t.Fatalf("%s status = %q, want skipped", id, got)
		}
	}
}

func TestDrainTeamsBridgeForChatRecreateWaitsForOwnerAndClearsDrain(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("openTeamsStore error: %v", err)
	}
	owner, err := teamsstore.CurrentOwner("v-test", "", "", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, time.Now()); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	prevPollInterval := teamsUpgradePollInterval
	teamsUpgradePollInterval = time.Millisecond
	t.Cleanup(func() { teamsUpgradePollInterval = prevPollInterval })

	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.After(2 * time.Second)
		for {
			control, err := st.ReadControl(context.Background())
			if err == nil && control.Draining && control.Reason == "chat recreate" {
				_ = st.ClearOwner(context.Background())
				return
			}
			select {
			case <-deadline:
				return
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()

	var out bytes.Buffer
	if err := drainTeamsBridgeForChatRecreate(context.Background(), &out, time.Second); err != nil {
		t.Fatalf("drainTeamsBridgeForChatRecreate error: %v\noutput:\n%s", err, out.String())
	}
	<-done
	control, err := st.ReadControl(context.Background())
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if control.Draining {
		t.Fatalf("drain flag should be cleared after successful recreate drain: %#v", control)
	}
	if !strings.Contains(out.String(), "Waiting for active Teams listener") || !strings.Contains(out.String(), "Teams listener drained.") {
		t.Fatalf("unexpected drain output:\n%s", out.String())
	}
}

func TestDrainTeamsBridgeForChatRecreateTimeoutClearsDrain(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("openTeamsStore error: %v", err)
	}
	owner, err := teamsstore.CurrentOwner("v-test", "", "", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, time.Now()); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	prevPollInterval := teamsUpgradePollInterval
	teamsUpgradePollInterval = time.Millisecond
	t.Cleanup(func() { teamsUpgradePollInterval = prevPollInterval })

	err = drainTeamsBridgeForChatRecreate(context.Background(), nil, 5*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for Teams listener to drain") {
		t.Fatalf("drain timeout error = %v", err)
	}
	control, readErr := st.ReadControl(context.Background())
	if readErr != nil {
		t.Fatalf("ReadControl error: %v", readErr)
	}
	if control.Draining {
		t.Fatalf("drain flag should be cleared after timeout: %#v", control)
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
