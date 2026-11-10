package cli

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

type cliReadOnlyFileSnapshot struct {
	Mode    os.FileMode
	Size    int64
	ModTime int64
	Data    string
}

func assertCLIFileContent(t *testing.T, path string, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("content for %s = %q, want %q", path, got, want)
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
		"creates this machine's control chat when it first starts",
		"teams run",
		"teams service doctor",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("setup output missing %q:\n%s", want, out)
		}
	}
	if _, err := os.Stat(filepath.Join(configBase, "codex-helper", "teams", "state.json")); !os.IsNotExist(err) {
		t.Fatalf("teams setup should not create state file, stat err = %v", err)
	}
}

func TestTeamsAuthStatusDoesNotPrintCachedSecrets(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cachePath := filepath.Join(tmp, "token.json")
	setTeamsAuthIDsForCLITest(t)
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", cachePath)
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
	if !strings.Contains(got, "Teams auth: logged in") {
		t.Fatalf("expected present auth cache, got:\n%s", got)
	}
	if strings.Contains(got, secretAccess) || strings.Contains(got, secretRefresh) {
		t.Fatalf("teams auth status printed a cached secret:\n%s", got)
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
		"Auth: not ready",
		"Next steps: run `cxp teams setup`",
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
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", cachePath)
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
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", cachePath)
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

// `teams auth config` writes the tenant and the one Graph client id to the
// private user config, keeping fields written by earlier releases.
func TestTeamsAuthConfigWritesLocalClientIDs(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	configPath := filepath.Join(configBase, "codex-helper", "teams-auth.json")
	writeFile(t, configPath, `{"read":{"client_id":"old-read"},"full":{"scopes":"User.Read"}}`)

	out := executeRootForTeamsTest(t,
		"teams", "auth", "config",
		"--tenant-id", "tenant-config",
		"--chat-client-id", "chat-client-config",
	)
	for _, want := range []string{
		"Saved Teams auth config: " + strings.TrimSuffix(configPath, ".json") + ".sqlite",
		"Tenant ID: configured",
		"Client ID: configured",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("auth config output missing %q:\n%s", want, out)
		}
	}
	doc := readAuthConfigRow(t, configPath)
	if doc.TenantID != "tenant-config" || doc.FullClient != "chat-client-config" || doc.FullScopes != "User.Read" || doc.ReadClient != "old-read" {
		t.Fatalf("saved auth config = %#v", doc)
	}
	configPath = strings.TrimSuffix(configPath, ".json") + ".sqlite"

	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("stat auth config: %v", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("auth config permissions = %03o, want private", info.Mode().Perm())
	}
}

func TestTeamsAuthConfigWritesSeparateReadClient(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	configPath := filepath.Join(configBase, "codex-helper", "teams-auth.json")
	out := executeRootForTeamsTest(t,
		"teams", "auth", "config",
		"--tenant-id", "tenant-config",
		"--read-client-id", "read-client-config",
		"--read-scopes", "User.Read Chat.Read Files.Read",
		"--full-client-id", "write-client-config",
		"--client-id", "write-client-config",
	)
	if !strings.Contains(out, "Read client ID: configured") {
		t.Fatalf("auth config did not report split read client:\n%s", out)
	}
	doc := readAuthConfigRow(t, configPath)
	if doc.ReadClient != "read-client-config" || doc.ReadScopes != "User.Read Chat.Read Files.Read" || doc.FullClient != "write-client-config" {
		t.Fatalf("auth config = %#v", doc)
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
	runTeamsDoctorLiveCheck = func(cmd *cobra.Command, _ *rootOptions) error {
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
	if !strings.Contains(out, "Auth: not logged in (run `cxp teams auth`)") {
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

type authConfigFixtureRow struct{ TenantID, ReadClient, ReadScopes, FullClient, FullScopes string }

func readAuthConfigRow(t *testing.T, path string) authConfigFixtureRow {
	t.Helper()
	path = strings.TrimSuffix(path, ".json") + ".sqlite"
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var row authConfigFixtureRow
	if err = db.QueryRow(`SELECT tenant_id,read_client_id,read_scopes,full_client_id,full_scopes FROM auth_profiles WHERE profile=''`).Scan(&row.TenantID, &row.ReadClient, &row.ReadScopes, &row.FullClient, &row.FullScopes); err != nil {
		t.Fatal(err)
	}
	return row
}
