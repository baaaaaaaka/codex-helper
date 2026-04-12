package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func encodeTestJWT(t *testing.T, payload map[string]any) string {
	t.Helper()
	headerBytes, err := json.Marshal(map[string]string{
		"alg": "none",
		"typ": "JWT",
	})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(headerBytes) + "." +
		base64.RawURLEncoding.EncodeToString(payloadBytes) + "." +
		base64.RawURLEncoding.EncodeToString([]byte("sig"))
}

func writeTestAuthJSONWithPlans(t *testing.T, codexDir string, idTokenPlan bool, accessTokenPlan bool) []byte {
	t.Helper()

	buildAuthClaims := func(includePlan bool) map[string]any {
		authClaims := map[string]any{
			"chatgpt_account_id": "org_test",
			"chatgpt_user_id":    "user_test",
		}
		if includePlan {
			authClaims["chatgpt_plan_type"] = "business"
		}
		return authClaims
	}

	idToken := encodeTestJWT(t, map[string]any{
		"email":                       "user@example.com",
		"https://api.openai.com/auth": buildAuthClaims(idTokenPlan),
	})
	accessToken := encodeTestJWT(t, map[string]any{
		"scope":                       "openid profile email",
		"https://api.openai.com/auth": buildAuthClaims(accessTokenPlan),
	})
	doc := map[string]any{
		"auth_mode": "chatgpt",
		"tokens": map[string]any{
			"id_token":      idToken,
			"access_token":  accessToken,
			"refresh_token": "refresh",
			"account_id":    "org_test",
		},
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("marshal auth json: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(codexDir, "auth.json"), data, 0o600); err != nil {
		t.Fatalf("write auth.json: %v", err)
	}
	return data
}

func writeTestAuthJSON(t *testing.T, codexDir string, includePlan bool) []byte {
	t.Helper()
	return writeTestAuthJSONWithPlans(t, codexDir, includePlan, includePlan)
}

func rewriteAuthToken(t *testing.T, authPath string, tokenKey string, tokenValue string) []byte {
	t.Helper()

	data, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatalf("read auth json: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse auth json: %v", err)
	}
	tokens, ok := doc["tokens"].(map[string]any)
	if !ok {
		t.Fatalf("missing tokens object: %#v", doc["tokens"])
	}
	tokens[tokenKey] = tokenValue
	updated, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("marshal auth json: %v", err)
	}
	updated = append(updated, '\n')
	if err := os.WriteFile(authPath, updated, 0o600); err != nil {
		t.Fatalf("write auth json: %v", err)
	}
	return updated
}

func authJSONHasPlanClaim(t *testing.T, data []byte) bool {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parse auth json: %v", err)
	}
	tokens, ok := doc["tokens"].(map[string]any)
	if !ok {
		t.Fatalf("missing tokens object: %#v", doc["tokens"])
	}
	idToken, ok := tokens["id_token"].(string)
	if !ok || idToken == "" {
		return false
	}
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		t.Fatalf("invalid jwt: %q", idToken)
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		t.Fatalf("parse payload: %v", err)
	}
	authClaims, ok := payload["https://api.openai.com/auth"].(map[string]any)
	if !ok {
		return false
	}
	_, ok = authClaims["chatgpt_plan_type"]
	return ok
}

func TestPrepareYoloAuthOverrideMasksPlanAndRestores(t *testing.T) {
	codexDir := t.TempDir()
	original := writeTestAuthJSON(t, codexDir, true)

	override, err := prepareYoloAuthOverride(codexDir, nil)
	if err != nil {
		t.Fatalf("prepareYoloAuthOverride: %v", err)
	}
	if override == nil {
		t.Fatal("expected auth override")
	}

	sanitized, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read sanitized auth.json: %v", err)
	}
	if authJSONHasPlanClaim(t, sanitized) {
		t.Fatal("sanitized auth.json should not contain chatgpt_plan_type")
	}

	override.Cleanup()

	restored, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read restored auth.json: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("auth.json should be restored after cleanup")
	}
}

func TestPrepareYoloAuthOverrideIgnoresAccessTokenOnlyPlan(t *testing.T) {
	codexDir := t.TempDir()
	original := writeTestAuthJSONWithPlans(t, codexDir, false, true)

	override, err := prepareYoloAuthOverride(codexDir, nil)
	if err != nil {
		t.Fatalf("prepareYoloAuthOverride: %v", err)
	}
	if override != nil {
		t.Fatal("expected no auth override when only access_token contains the plan claim")
	}

	current, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read auth.json: %v", err)
	}
	if !bytes.Equal(current, original) {
		t.Fatal("auth.json should remain unchanged")
	}
}

func TestPrepareYoloAuthOverrideIgnoresOpaqueThreePartAccessToken(t *testing.T) {
	codexDir := t.TempDir()
	authPath := filepath.Join(codexDir, "auth.json")
	_ = writeTestAuthJSONWithPlans(t, codexDir, true, false)
	original := rewriteAuthToken(t, authPath, "access_token", "opaque.token.value")

	override, err := prepareYoloAuthOverride(codexDir, nil)
	if err != nil {
		t.Fatalf("prepareYoloAuthOverride: %v", err)
	}
	if override == nil {
		t.Fatal("expected auth override")
	}

	sanitized, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatalf("read sanitized auth.json: %v", err)
	}
	if authJSONHasPlanClaim(t, sanitized) {
		t.Fatal("sanitized auth.json should not contain chatgpt_plan_type in id_token")
	}

	var doc map[string]any
	if err := json.Unmarshal(sanitized, &doc); err != nil {
		t.Fatalf("parse sanitized auth json: %v", err)
	}
	tokens, ok := doc["tokens"].(map[string]any)
	if !ok {
		t.Fatalf("missing tokens object: %#v", doc["tokens"])
	}
	if got := tokens["access_token"]; got != "opaque.token.value" {
		t.Fatalf("access_token = %#v, want opaque token unchanged", got)
	}

	override.Cleanup()

	restored, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatalf("read restored auth.json: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("auth.json should be restored after cleanup")
	}
}

func TestPrepareYoloAuthOverrideRecoversMaskedAuthFromBackupAfterStaleLease(t *testing.T) {
	codexDir := t.TempDir()
	original := writeTestAuthJSON(t, codexDir, true)
	authPath := filepath.Join(codexDir, "auth.json")
	backupPath := yoloAuthBackupPath(authPath)
	if err := os.WriteFile(backupPath, original, 0o600); err != nil {
		t.Fatalf("write backup: %v", err)
	}

	sanitized, changed, err := sanitizeAuthJSONPlanClaim(original)
	if err != nil {
		t.Fatalf("sanitizeAuthJSONPlanClaim: %v", err)
	}
	if !changed {
		t.Fatal("expected sanitized auth to differ from original")
	}
	if err := os.WriteFile(authPath, sanitized, 0o600); err != nil {
		t.Fatalf("write sanitized auth: %v", err)
	}

	staleLease := filepath.Join(codexDir, yoloAuthLeasePrefix(authPath)+"stale")
	if err := os.WriteFile(staleLease, nil, 0o600); err != nil {
		t.Fatalf("write stale lease: %v", err)
	}
	staleAt := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(staleLease, staleAt, staleAt); err != nil {
		t.Fatalf("chtimes stale lease: %v", err)
	}

	override, err := prepareYoloAuthOverride(codexDir, nil)
	if err != nil {
		t.Fatalf("prepareYoloAuthOverride: %v", err)
	}
	if override == nil {
		t.Fatal("expected auth override to preserve and restore the original backup")
	}
	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("expected backup to be preserved, stat err=%v", err)
	}

	override.Cleanup()

	restored, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatalf("read restored auth.json: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("auth.json should be restored from backup after cleanup")
	}
}

func TestPrepareYoloAuthOverrideDropsStaleLeaseAndBackupWhenAuthAlreadyClean(t *testing.T) {
	codexDir := t.TempDir()
	original := writeTestAuthJSONWithPlans(t, codexDir, false, false)
	authPath := filepath.Join(codexDir, "auth.json")
	backupDir := t.TempDir()
	staleBackup := writeTestAuthJSONWithPlans(t, backupDir, true, true)
	backupPath := yoloAuthBackupPath(authPath)
	if err := os.WriteFile(backupPath, staleBackup, 0o600); err != nil {
		t.Fatalf("write stale backup: %v", err)
	}

	staleLease := filepath.Join(codexDir, yoloAuthLeasePrefix(authPath)+"stale")
	if err := os.WriteFile(staleLease, nil, 0o600); err != nil {
		t.Fatalf("write stale lease: %v", err)
	}
	staleAt := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(staleLease, staleAt, staleAt); err != nil {
		t.Fatalf("chtimes stale lease: %v", err)
	}

	override, err := prepareYoloAuthOverride(codexDir, nil)
	if err != nil {
		t.Fatalf("prepareYoloAuthOverride: %v", err)
	}
	if override != nil {
		t.Fatal("expected no auth override when auth is already clean")
	}

	current, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatalf("read auth.json: %v", err)
	}
	if !bytes.Equal(current, original) {
		t.Fatal("auth.json should remain unchanged")
	}
	if _, err := os.Stat(backupPath); !os.IsNotExist(err) {
		t.Fatalf("stale backup should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(staleLease); !os.IsNotExist(err) {
		t.Fatalf("stale lease should be removed, stat err=%v", err)
	}
}

func TestYoloAuthOverrideCleanupPreservesChangedFile(t *testing.T) {
	codexDir := t.TempDir()
	writeTestAuthJSON(t, codexDir, true)

	override, err := prepareYoloAuthOverride(codexDir, nil)
	if err != nil {
		t.Fatalf("prepareYoloAuthOverride: %v", err)
	}
	if override == nil {
		t.Fatal("expected auth override")
	}

	changed := []byte("{\"auth_mode\":\"chatgpt\"}\n")
	if err := os.WriteFile(filepath.Join(codexDir, "auth.json"), changed, 0o600); err != nil {
		t.Fatalf("write changed auth.json: %v", err)
	}

	override.Cleanup()

	current, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read auth.json after cleanup: %v", err)
	}
	if !bytes.Equal(current, changed) {
		t.Fatal("cleanup should not overwrite auth.json when codex changed it")
	}
}

func TestPrepareYoloAuthOverrideWaitsForLastLeaseBeforeRestore(t *testing.T) {
	codexDir := t.TempDir()
	original := writeTestAuthJSON(t, codexDir, true)

	first, err := prepareYoloAuthOverride(codexDir, nil)
	if err != nil {
		t.Fatalf("prepareYoloAuthOverride first: %v", err)
	}
	if first == nil {
		t.Fatal("expected first auth override")
	}

	second, err := prepareYoloAuthOverride(codexDir, nil)
	if err != nil {
		t.Fatalf("prepareYoloAuthOverride second: %v", err)
	}
	if second == nil {
		t.Fatal("expected second auth override to join existing lease")
	}

	first.Cleanup()

	stillSanitized, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read auth.json after first cleanup: %v", err)
	}
	if authJSONHasPlanClaim(t, stillSanitized) {
		t.Fatal("auth.json should stay sanitized while another lease is active")
	}

	second.Cleanup()

	restored, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read auth.json after last cleanup: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("auth.json should be restored after the last lease is released")
	}
}

func TestResolveCodexHomeUsesWorkingDirAndCODEXDIREnv(t *testing.T) {
	projectDir := t.TempDir()
	t.Setenv(envCodexHome, "")
	t.Setenv(codexhistory.EnvCodexDir, ".codex-rel")

	got, err := resolveCodexHome("", projectDir)
	if err != nil {
		t.Fatalf("resolveCodexHome: %v", err)
	}

	want := filepath.Join(projectDir, ".codex-rel")
	if got != want {
		t.Fatalf("resolveCodexHome = %q, want %q", got, want)
	}
}

func TestRunCodexNewSessionSanitizesAuthForChildProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	projectDir := t.TempDir()
	codexDir := t.TempDir()
	original := writeTestAuthJSON(t, codexDir, true)

	outHome := filepath.Join(t.TempDir(), "codex-home.txt")
	outAuth := filepath.Join(t.TempDir(), "auth-snapshot.json")
	t.Setenv("OUT_CODEX_HOME", outHome)
	t.Setenv("OUT_AUTH_SNAPSHOT", outAuth)

	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\n" +
		"case \"$1\" in\n" +
		"  --version)\n" +
		"    echo 'codex 0.112.0'\n" +
		"    exit 0\n" +
		"    ;;\n" +
		"  --help)\n" +
		"    echo 'usage codex --dangerously-bypass-approvals-and-sandbox'\n" +
		"    exit 0\n" +
		"    ;;\n" +
		"esac\n" +
		"printf '%s' \"$CODEX_HOME\" > \"$OUT_CODEX_HOME\"\n" +
		"cp \"$CODEX_HOME/auth.json\" \"$OUT_AUTH_SNAPSHOT\"\n" +
		"exit 0\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	if err := runCodexNewSession(
		context.Background(),
		&rootOptions{},
		store,
		nil, nil,
		projectDir,
		scriptPath,
		codexDir,
		false,
		true,
		io.Discard,
	); err != nil {
		t.Fatalf("runCodexNewSession: %v", err)
	}

	gotHome, err := os.ReadFile(outHome)
	if err != nil {
		t.Fatalf("read child CODEX_HOME: %v", err)
	}
	if string(gotHome) != codexDir {
		t.Fatalf("child CODEX_HOME = %q, want %q", string(gotHome), codexDir)
	}

	snapshot, err := os.ReadFile(outAuth)
	if err != nil {
		t.Fatalf("read child auth snapshot: %v", err)
	}
	if authJSONHasPlanClaim(t, snapshot) {
		t.Fatal("child process should see auth.json without chatgpt_plan_type")
	}

	restored, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read restored auth.json: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("auth.json should be restored after child exit")
	}
}

func TestRunCodexNewSessionSanitizesAuthForRelativeCodexDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	projectDir := t.TempDir()
	relativeCodexDir := ".codex-rel"
	absoluteCodexDir := filepath.Join(projectDir, relativeCodexDir)
	if err := os.MkdirAll(absoluteCodexDir, 0o700); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	original := writeTestAuthJSON(t, absoluteCodexDir, true)

	outHome := filepath.Join(t.TempDir(), "codex-home.txt")
	outAuth := filepath.Join(t.TempDir(), "auth-snapshot.json")
	t.Setenv("OUT_CODEX_HOME", outHome)
	t.Setenv("OUT_AUTH_SNAPSHOT", outAuth)

	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\n" +
		"case \"$1\" in\n" +
		"  --version)\n" +
		"    echo 'codex 0.112.0'\n" +
		"    exit 0\n" +
		"    ;;\n" +
		"  --help)\n" +
		"    echo 'usage codex --dangerously-bypass-approvals-and-sandbox'\n" +
		"    exit 0\n" +
		"    ;;\n" +
		"esac\n" +
		"printf '%s' \"$CODEX_HOME\" > \"$OUT_CODEX_HOME\"\n" +
		"cp \"$CODEX_HOME/auth.json\" \"$OUT_AUTH_SNAPSHOT\"\n" +
		"exit 0\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	if err := runCodexNewSession(
		context.Background(),
		&rootOptions{},
		store,
		nil, nil,
		projectDir,
		scriptPath,
		relativeCodexDir,
		false,
		true,
		io.Discard,
	); err != nil {
		t.Fatalf("runCodexNewSession: %v", err)
	}

	gotHome, err := os.ReadFile(outHome)
	if err != nil {
		t.Fatalf("read child CODEX_HOME: %v", err)
	}
	if string(gotHome) != absoluteCodexDir {
		t.Fatalf("child CODEX_HOME = %q, want %q", string(gotHome), absoluteCodexDir)
	}

	snapshot, err := os.ReadFile(outAuth)
	if err != nil {
		t.Fatalf("read child auth snapshot: %v", err)
	}
	if authJSONHasPlanClaim(t, snapshot) {
		t.Fatal("child process should see auth.json without chatgpt_plan_type")
	}

	restored, err := os.ReadFile(filepath.Join(absoluteCodexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read restored auth.json: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("auth.json should be restored after child exit")
	}
}

func TestRunCodexSessionSanitizesAuthForChildProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	projectDir := t.TempDir()
	codexDir := t.TempDir()
	original := writeTestAuthJSON(t, codexDir, true)

	outHome := filepath.Join(t.TempDir(), "codex-home.txt")
	outAuth := filepath.Join(t.TempDir(), "auth-snapshot.json")
	t.Setenv("OUT_CODEX_HOME", outHome)
	t.Setenv("OUT_AUTH_SNAPSHOT", outAuth)

	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\n" +
		"case \"$1\" in\n" +
		"  --version)\n" +
		"    echo 'codex 0.112.0'\n" +
		"    exit 0\n" +
		"    ;;\n" +
		"  --help)\n" +
		"    echo 'usage codex --dangerously-bypass-approvals-and-sandbox'\n" +
		"    exit 0\n" +
		"    ;;\n" +
		"esac\n" +
		"printf '%s' \"$CODEX_HOME\" > \"$OUT_CODEX_HOME\"\n" +
		"cp \"$CODEX_HOME/auth.json\" \"$OUT_AUTH_SNAPSHOT\"\n" +
		"exit 0\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	session := codexhistory.Session{
		SessionID:   "session-123",
		ProjectPath: projectDir,
	}
	project := codexhistory.Project{Path: projectDir}

	if err := runCodexSession(
		context.Background(),
		&rootOptions{},
		store,
		nil, nil,
		session,
		project,
		scriptPath,
		codexDir,
		false,
		true,
		io.Discard,
	); err != nil {
		t.Fatalf("runCodexSession: %v", err)
	}

	gotHome, err := os.ReadFile(outHome)
	if err != nil {
		t.Fatalf("read child CODEX_HOME: %v", err)
	}
	if string(gotHome) != codexDir {
		t.Fatalf("child CODEX_HOME = %q, want %q", string(gotHome), codexDir)
	}

	snapshot, err := os.ReadFile(outAuth)
	if err != nil {
		t.Fatalf("read child auth snapshot: %v", err)
	}
	if authJSONHasPlanClaim(t, snapshot) {
		t.Fatal("child process should see auth.json without chatgpt_plan_type")
	}

	restored, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read restored auth.json: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("auth.json should be restored after child exit")
	}
}

func TestRunCodexNewSessionRestoresAuthOnContextCancel(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skip shell script test on windows")
	}

	projectDir := t.TempDir()
	codexDir := t.TempDir()
	original := writeTestAuthJSON(t, codexDir, true)

	outAuth := filepath.Join(t.TempDir(), "auth-snapshot.json")
	t.Setenv("OUT_AUTH_SNAPSHOT", outAuth)

	scriptPath := filepath.Join(t.TempDir(), "codex")
	script := "#!/bin/sh\n" +
		"case \"$1\" in\n" +
		"  --version)\n" +
		"    echo 'codex 0.112.0'\n" +
		"    exit 0\n" +
		"    ;;\n" +
		"  --help)\n" +
		"    echo 'usage codex --dangerously-bypass-approvals-and-sandbox'\n" +
		"    exit 0\n" +
		"    ;;\n" +
		"esac\n" +
		"cp \"$CODEX_HOME/auth.json\" \"$OUT_AUTH_SNAPSHOT\"\n" +
		"trap 'exit 0' INT TERM\n" +
		"while :; do sleep 1; done\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write script: %v", err)
	}

	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runCodexNewSession(
			ctx,
			&rootOptions{},
			store,
			nil, nil,
			projectDir,
			scriptPath,
			codexDir,
			false,
			true,
			io.Discard,
		)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(outAuth); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for child auth snapshot")
		}
		time.Sleep(50 * time.Millisecond)
	}

	snapshot, err := os.ReadFile(outAuth)
	if err != nil {
		t.Fatalf("read child auth snapshot: %v", err)
	}
	if authJSONHasPlanClaim(t, snapshot) {
		t.Fatal("child process should see auth.json without chatgpt_plan_type")
	}

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runCodexNewSession error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runCodexNewSession to return")
	}

	restored, err := os.ReadFile(filepath.Join(codexDir, "auth.json"))
	if err != nil {
		t.Fatalf("read restored auth.json: %v", err)
	}
	if !bytes.Equal(restored, original) {
		t.Fatal("auth.json should be restored after context cancellation")
	}
}
