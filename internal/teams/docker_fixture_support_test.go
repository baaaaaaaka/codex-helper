package teams

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	dockerFixtureDirEnv          = "CXP_TEAMS_DOCKER_FIXTURE_DIR"
	dockerRuntimeDirEnv          = "CXP_TEAMS_DOCKER_RUNTIME_DIR"
	dockerRuntimeReuseEnv        = "CXP_TEAMS_DOCKER_RUNTIME_REUSE"
	dockerCodexMountEnv          = "CXP_TEAMS_DOCKER_CODEX_MOUNTED"
	dockerCodexSourceEnv         = "CXP_TEAMS_DOCKER_CODEX_SOURCE_PREFIX"
	dockerSourceProofManifestEnv = "CXP_TEAMS_DOCKER_SOURCE_PROOF_MANIFEST"
	dockerFixtureCodexDir        = "/home/baka/.codex/"
)

// dockerTeamsFixtureRoot is intentionally opt-in. The fixture contains a
// point-in-time copy of user state and must never become normal package or CI
// test data.
func dockerTeamsFixtureRoot(t *testing.T) string {
	t.Helper()
	root := strings.TrimSpace(os.Getenv(dockerFixtureDirEnv))
	if root == "" {
		t.Skipf("set %s to run the copied SQLite/Codex Docker fixture", dockerFixtureDirEnv)
	}
	for _, name := range []string{
		"teams/state.json",
		"teams/store.sqlite",
		"teams/registry.json",
		"codex/history.jsonl",
		"codex/session_index.jsonl",
	} {
		path := filepath.Join(root, filepath.FromSlash(name))
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			t.Fatalf("Docker fixture file %q: %v", name, err)
		}
		if err != nil {
			t.Fatalf("Docker fixture file %q: %v", name, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			t.Fatalf("Docker fixture file %q is not a regular file", name)
		}
	}
	// A copied Codex session necessarily has a new inode/path.  When the
	// Docker runner opts into source-proof rebinding, require the immutable
	// manifest to live inside the same read-only fixture tree.  Accepting an
	// arbitrary host path here would make a source-proof result depend on a
	// mutable file outside the snapshot and would turn a missing manifest into
	// an accidental fail-open diagnostic.
	if manifestPath := strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)); manifestPath != "" {
		manifestInfo, err := os.Lstat(manifestPath)
		if err != nil {
			t.Fatalf("Docker source-proof manifest: %v", err)
		}
		if manifestInfo.Mode()&os.ModeSymlink != 0 || !manifestInfo.Mode().IsRegular() {
			t.Fatalf("Docker source-proof manifest is not a regular file: %q", manifestPath)
		}
		rootAbs, err := filepath.Abs(root)
		if err != nil {
			t.Fatalf("resolve Docker fixture root: %v", err)
		}
		manifestAbs, err := filepath.Abs(manifestPath)
		if err != nil {
			t.Fatalf("resolve Docker source-proof manifest: %v", err)
		}
		relative, err := filepath.Rel(rootAbs, manifestAbs)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			t.Fatalf("Docker source-proof manifest must be inside fixture root %q: %q", rootAbs, manifestAbs)
		}
	}
	sessionsPath := filepath.Join(root, "codex", "sessions")
	sessionsInfo, err := os.Lstat(sessionsPath)
	if err != nil {
		t.Fatalf("Docker fixture sessions directory: %v", err)
	}
	if sessionsInfo.Mode()&os.ModeSymlink != 0 || !sessionsInfo.IsDir() {
		t.Fatalf("Docker fixture sessions path is not a real directory: %q", sessionsPath)
	}
	return root
}

func dockerTeamsFixtureRuntimeRoot(t *testing.T) string {
	t.Helper()
	root := strings.TrimSpace(os.Getenv(dockerRuntimeDirEnv))
	if root == "" {
		return t.TempDir()
	}
	testName := strings.NewReplacer("/", "-", "\\", "-", " ", "-").Replace(t.Name())
	root = filepath.Join(root, testName)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("create Docker fixture runtime root: %v", err)
	}
	return root
}

func copyDockerFixtureFile(t *testing.T, sourcePath string, destinationPath string) {
	t.Helper()
	info, err := os.Lstat(sourcePath)
	if err != nil {
		t.Fatalf("stat Docker fixture source %q: %v", sourcePath, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		t.Fatalf("Docker fixture source is not a regular file: %q", sourcePath)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		t.Fatalf("open Docker fixture source %q: %v", sourcePath, err)
	}
	defer source.Close()
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o700); err != nil {
		t.Fatalf("create Docker fixture destination directory: %v", err)
	}
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("create Docker fixture destination %q: %v", destinationPath, err)
	}
	_, copyErr := io.Copy(destination, source)
	syncErr := destination.Sync()
	closeErr := destination.Close()
	if copyErr != nil {
		t.Fatalf("copy Docker fixture %q to %q: %v", sourcePath, destinationPath, copyErr)
	}
	if syncErr != nil {
		t.Fatalf("sync Docker fixture copy %q: %v", destinationPath, syncErr)
	}
	if closeErr != nil {
		t.Fatalf("close Docker fixture copy %q: %v", destinationPath, closeErr)
	}
}

func validateDockerFixtureRuntime(t *testing.T, runtimeRoot string) {
	t.Helper()
	stateRoot := filepath.Join(runtimeRoot, "state")
	for _, name := range []string{"state.json", "store.sqlite", "registry.json"} {
		path := filepath.Join(stateRoot, name)
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("reused Docker fixture runtime file %q: %v", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			t.Fatalf("reused Docker fixture runtime file is not a regular file: %q", path)
		}
	}
	err := filepath.Walk(runtimeRoot, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("reused Docker fixture runtime contains a symlink: %q", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate reused Docker fixture runtime: %v", err)
	}
}

func prepareDockerFixtureStore(t *testing.T, fixtureRoot string) (*teamstore.Store, string) {
	t.Helper()
	runtimeRoot := dockerTeamsFixtureRuntimeRoot(t)
	stateRoot := filepath.Join(runtimeRoot, "state")
	statePath := filepath.Join(stateRoot, "state.json")
	if strings.TrimSpace(os.Getenv(dockerRuntimeReuseEnv)) == "1" {
		validateDockerFixtureRuntime(t, runtimeRoot)
	} else {
		copyDockerFixtureFile(t, filepath.Join(fixtureRoot, "teams", "state.json"), statePath)
		copyDockerFixtureFile(t, filepath.Join(fixtureRoot, "teams", "store.sqlite"), filepath.Join(stateRoot, "store.sqlite"))
		// The listener reloads the registry projection at every disposable restart.
		// Keep it beside the copied state rather than relying on the source fixture
		// path, which is read-only and is not mounted at the runtime location.
		copyDockerFixtureFile(t, filepath.Join(fixtureRoot, "teams", "registry.json"), filepath.Join(stateRoot, "registry.json"))
		for _, name := range []string{"global-inbound-ledger", "global-outbound-ledger"} {
			for _, suffix := range []string{".json", ".sqlite"} {
				sourcePath := filepath.Join(fixtureRoot, "teams", name+suffix)
				if _, err := os.Stat(sourcePath); err == nil {
					copyDockerFixtureFile(t, sourcePath, filepath.Join(stateRoot, "teams-"+name+suffix))
				} else if !os.IsNotExist(err) {
					t.Fatalf("stat optional Docker fixture shared ledger %q: %v", sourcePath, err)
				}
			}
		}
		for _, name := range []string{
			"control-chat-history.jsonl",
			"control-chat-history.sqlite",
			"control-chat-history.sqlite-wal",
			"control-chat-history.sqlite-shm",
			"helper-restart-pending.json",
			"workflow-notifications.json",
		} {
			sourcePath := filepath.Join(fixtureRoot, "teams", name)
			info, err := os.Lstat(sourcePath)
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				t.Fatalf("stat optional Docker fixture sidecar %q: %v", sourcePath, err)
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				t.Fatalf("optional Docker fixture sidecar is not a regular file: %q", sourcePath)
			}
			copyDockerFixtureFile(t, sourcePath, filepath.Join(stateRoot, name))
		}
		threadLinksRoot := filepath.Join(fixtureRoot, "teams", "thread-links")
		if _, err := os.Lstat(threadLinksRoot); err == nil {
			err = filepath.Walk(threadLinksRoot, func(sourcePath string, info os.FileInfo, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if info.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("Docker fixture thread-link tree contains a symlink: %q", sourcePath)
				}
				if info.IsDir() || filepath.Ext(info.Name()) != ".jsonl" {
					return nil
				}
				relative, relativeErr := filepath.Rel(threadLinksRoot, sourcePath)
				if relativeErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
					return fmt.Errorf("Docker fixture thread-link path escapes its root: %q", sourcePath)
				}
				copyDockerFixtureFile(t, sourcePath, filepath.Join(stateRoot, "thread-links", relative))
				return nil
			})
			if err != nil {
				t.Fatalf("copy optional Docker fixture thread-links: %v", err)
			}
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat optional Docker fixture thread-links: %v", err)
		}
	}
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("open copied Docker fixture store: %v", err)
	}
	// A live source may be copied while its schema marker and derived objects
	// are between two fenced preparation steps.  The disposable fixture must
	// follow the same startup boundary before applying its path/workflow safety
	// rewrites; otherwise the first owner-scoped update correctly fails closed
	// with ErrSQLiteSchemaPreparationRequired and hides the actual experiment.
	if err := store.PrepareSQLiteSchemaBeforeOwner(context.Background()); err != nil {
		t.Fatalf("prepare copied Docker fixture SQLite schema before safety rewrites: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close copied Docker fixture store: %v", err)
		}
	})
	return store, statePath
}

// dockerFixtureRemapCodexPaths rewrites only the copied fixture's persisted
// Codex-home/source paths.  The live state deliberately keeps the host path;
// the container has no access to that path and must never be given the whole
// host Codex home merely to make a replay pass.  Source paths that claim to be
// under the configured Codex home but escape the copied sessions tree fail
// closed instead of being silently dropped or redirected to a sibling.
func dockerFixtureRemapCodexPaths(t *testing.T, store *teamstore.Store) {
	t.Helper()
	if store == nil {
		t.Fatal("cannot remap Codex paths on a nil Docker fixture store")
	}
	sourcePrefix := strings.TrimSpace(os.Getenv(dockerCodexSourceEnv))
	if sourcePrefix == "" {
		sourcePrefix = dockerFixtureCodexDir
	}
	canonicalHome := strings.TrimSuffix(filepath.Clean(filepath.FromSlash(dockerFixtureCodexDir)), string(filepath.Separator))
	if dockerFixtureCodexHomeIsMounted(sourcePrefix, canonicalHome) {
		// The runner mounts the copied Codex tree at the exact persisted path.
		// Rewriting every State row here would only change paths to identical
		// values, but store.Update would first load and then rewrite the entire
		// 864MB SQLite projection. Keep the safety mutations narrow and make the
		// path-preserving case explicit instead of silently weakening the generic
		// remapper below.
		dockerFixtureSanitizeMountedStore(t, store)
		scope, err := store.ReadScope(context.Background())
		if err != nil {
			t.Fatalf("read mounted Docker fixture scope: %v", err)
		}
		if strings.TrimSpace(scope.ConfigPath) != "" {
			scope.ConfigPath = ""
			if err := store.RebindScopeForMigration(context.Background(), scope); err != nil {
				t.Fatalf("clear mounted Docker fixture scope config path: %v", err)
			}
		}
		return
	}
	mapPath := func(path string, allowHome bool) (string, error) {
		path = strings.TrimSpace(path)
		if path == "" {
			return "", nil
		}
		sourceRoot := filepath.Clean(filepath.FromSlash(sourcePrefix))
		candidate := filepath.Clean(filepath.FromSlash(path))
		if candidate == sourceRoot {
			if allowHome {
				return canonicalHome, nil
			}
			return "", fmt.Errorf("Codex source path is the home directory, not a copied session: %q", path)
		}
		relative, err := filepath.Rel(sourceRoot, candidate)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || !filepath.IsAbs(candidate) {
			return "", fmt.Errorf("Codex source path is outside the configured Codex home: %q", path)
		}
		sessionsRoot := filepath.FromSlash("sessions")
		if relative != sessionsRoot && !strings.HasPrefix(relative, sessionsRoot+string(filepath.Separator)) {
			return "", fmt.Errorf("Codex source path is outside the copied sessions tree: %q", path)
		}
		return filepath.Join(canonicalHome, relative), nil
	}
	mapRequired := func(label string, value *string, allowHome bool) error {
		mapped, err := mapPath(*value, allowHome)
		if err != nil {
			return fmt.Errorf("remap %s: %w", label, err)
		}
		*value = mapped
		return nil
	}
	mapOptionalCodexPath := func(label string, value *string) error {
		if strings.TrimSpace(*value) == "" {
			return nil
		}
		candidate := filepath.Clean(filepath.FromSlash(strings.TrimSpace(*value)))
		sourceRoot := filepath.Clean(filepath.FromSlash(sourcePrefix))
		relative, err := filepath.Rel(sourceRoot, candidate)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			// Attachment/artifact paths normally belong to the Teams outbox tree,
			// not Codex sessions. Leave those paths alone; only a path that is
			// actually under Codex is subject to the strict copied-tree check.
			return nil
		}
		mapped, err := mapPath(*value, false)
		if err != nil {
			return fmt.Errorf("remap %s: %w", label, err)
		}
		*value = mapped
		return nil
	}

	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.Scope.CodexHome = canonicalHome
		// No copied fixture may read a real config or webhook secret. Graph and
		// execution are replaced by the test boundary; workflow notifications are
		// therefore disabled in the disposable state as an explicit safety fence.
		state.Scope.ConfigPath = ""
		state.Workflow.Enabled = false
		state.Workflow.ControlWebhookURLFile = ""
		for id, session := range state.Sessions {
			if err := mapRequired("session "+id+" CodexHome", &session.CodexHome, true); err != nil {
				return err
			}
			// A worktree is not part of the fixture mount. Clear the source
			// workspace instead of leaving an absolute host path that a future
			// executor or attachment helper could accidentally open.
			session.Cwd = ""
			state.Sessions[id] = session
		}
		for id, workspace := range state.Workspaces {
			workspace.Path = ""
			state.Workspaces[id] = workspace
		}
		for id, checkpoint := range state.ImportCheckpoints {
			if err := mapRequired("import checkpoint "+id+" source", &checkpoint.SourcePath, false); err != nil {
				return err
			}
			if checkpoint.PendingHistoryRange != nil {
				if err := mapRequired("import checkpoint "+id+" pending range", &checkpoint.PendingHistoryRange.SourcePath, false); err != nil {
					return err
				}
			}
			if checkpoint.TranscriptQuarantine != nil {
				if err := mapRequired("import checkpoint "+id+" quarantine", &checkpoint.TranscriptQuarantine.SourcePath, false); err != nil {
					return err
				}
			}
			if checkpoint.UnresolvedExecution != nil {
				if err := mapRequired("import checkpoint "+id+" execution anchor", &checkpoint.UnresolvedExecution.SourcePath, false); err != nil {
					return err
				}
			}
			state.ImportCheckpoints[id] = checkpoint
		}
		for id, checkpoint := range state.HistoryWatch {
			if err := mapRequired("history checkpoint "+id, &checkpoint.Path, false); err != nil {
				return err
			}
			if checkpoint.PendingHistoryRange != nil {
				if err := mapRequired("history checkpoint "+id+" pending range", &checkpoint.PendingHistoryRange.SourcePath, false); err != nil {
					return err
				}
			}
			if checkpoint.TranscriptQuarantine != nil {
				if err := mapRequired("history checkpoint "+id+" quarantine", &checkpoint.TranscriptQuarantine.SourcePath, false); err != nil {
					return err
				}
			}
			state.HistoryWatch[id] = checkpoint
		}
		for id, outbox := range state.OutboxMessages {
			if err := mapOptionalCodexPath("outbox "+id+" transcript source", &outbox.TranscriptSourcePath); err != nil {
				return err
			}
			if err := mapOptionalCodexPath("outbox "+id+" attachment", &outbox.AttachmentPath); err != nil {
				return err
			}
			state.OutboxMessages[id] = outbox
		}
		for id, record := range state.TranscriptLedger {
			if err := mapRequired("transcript ledger "+id, &record.SourcePath, false); err != nil {
				return err
			}
			state.TranscriptLedger[id] = record
		}
		for id, record := range state.TranscriptDeliveries {
			if err := mapRequired("transcript delivery "+id, &record.SourcePath, false); err != nil {
				return err
			}
			state.TranscriptDeliveries[id] = record
		}
		for id, record := range state.ArtifactRecords {
			if err := mapOptionalCodexPath("artifact "+id, &record.Path); err != nil {
				return err
			}
			state.ArtifactRecords[id] = record
		}
		return nil
	}); err != nil {
		t.Fatalf("remap copied Docker fixture Codex paths: %v", err)
	}
}

func dockerFixtureCodexHomeIsMounted(sourcePrefix string, canonicalHome string) bool {
	if os.Getenv(dockerCodexMountEnv) != "1" {
		return false
	}
	return filepath.Clean(filepath.FromSlash(strings.TrimSpace(sourcePrefix))) == filepath.Clean(filepath.FromSlash(canonicalHome))
}

// dockerFixtureSanitizeMountedStore applies only the disposable safety fences
// that are still needed when persisted Codex paths already match the container
// mount. It intentionally avoids Store.Update: that API materializes every
// split table before writing it back and is unsuitable for a real-data fixture.
func dockerFixtureSanitizeMountedStore(t *testing.T, store *teamstore.Store) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.UpdateWorkflowConfig(ctx, func(current teamstore.WorkflowNotificationConfig, _ teamstore.ControlChatBinding, _ time.Time) (teamstore.WorkflowNotificationConfig, bool, error) {
		next := current
		changed := next.Enabled || strings.TrimSpace(next.ControlWebhookURLFile) != ""
		next.Enabled = false
		next.ControlWebhookURLFile = ""
		return next, changed, nil
	}); err != nil {
		t.Fatalf("disable workflow in mounted Docker fixture: %v", err)
	}
	if err := store.UpdateDashboardRecords(ctx, func(records *teamstore.DashboardStoreRecords, _ time.Time) (bool, error) {
		changed := false
		for id, workspace := range records.Workspaces {
			if workspace.Path == "" {
				continue
			}
			workspace.Path = ""
			records.Workspaces[id] = workspace
			changed = true
		}
		return changed, nil
	}); err != nil {
		t.Fatalf("clear workspaces in mounted Docker fixture: %v", err)
	}

	dbPath := filepath.Join(filepath.Dir(store.Path()), teamstore.SQLiteFileName)
	query := url.Values{}
	query.Set("mode", "rw")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(dbPath, query))
	if err != nil {
		t.Fatalf("open mounted Docker fixture SQLite session projection: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close mounted Docker fixture SQLite session projection: %v", err)
		}
	})
	if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("configure mounted Docker fixture SQLite session projection: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin mounted Docker fixture session sanitization: %v", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, json FROM sessions`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("read mounted Docker fixture sessions: %v", err)
	}
	type sessionJSONUpdate struct {
		id  string
		raw []byte
	}
	updates := make([]sessionJSONUpdate, 0)
	for rows.Next() {
		var update sessionJSONUpdate
		if err := rows.Scan(&update.id, &update.raw); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			t.Fatalf("scan mounted Docker fixture session: %v", err)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(update.raw, &object); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			t.Fatalf("decode mounted Docker fixture session %q: %v", update.id, err)
		}
		if _, ok := object["cwd"]; !ok {
			continue
		}
		delete(object, "cwd")
		update.raw, err = json.Marshal(object)
		if err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			t.Fatalf("encode mounted Docker fixture session %q: %v", update.id, err)
		}
		updates = append(updates, update)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		_ = tx.Rollback()
		t.Fatalf("read mounted Docker fixture sessions: %v", err)
	}
	if err := rows.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatalf("close mounted Docker fixture sessions: %v", err)
	}
	for _, update := range updates {
		// Removing cwd is a fixture-only safety rewrite.  It does not change any
		// session admission field, so publish it like a normal canonical SQLite
		// write: advance both sides of the row-local generation fence and retain
		// trust only for rows that were trusted before the rewrite.  A bare JSON
		// UPDATE would intentionally revoke projection_trusted for every row and
		// force the real-data experiment through the two-second JSON compatibility
		// oracle, measuring the fixture sanitization rather than the service.
		if _, err := tx.ExecContext(ctx, `UPDATE sessions
SET json = ?,
    canonical_revision = COALESCE(canonical_revision, 0) + 1,
    projection_revision = COALESCE(canonical_revision, 0) + 1,
    projection_trusted = CASE WHEN projection_trusted = 1 THEN 1 ELSE 0 END
WHERE id = ?`, update.raw, update.id); err != nil {
			_ = tx.Rollback()
			t.Fatalf("clear mounted Docker fixture session %q workspace: %v", update.id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit mounted Docker fixture session sanitization: %v", err)
	}
}

func TestDockerFixtureSanitizeMountedStorePreservesTrustedSessionAdmission(t *testing.T) {
	store := newBridgeTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.Sessions["fixture-mounted-session"] = teamstore.SessionContext{
			ID:          "fixture-mounted-session",
			Status:      teamstore.SessionStatusActive,
			TeamsChatID: "fixture-mounted-chat",
			Cwd:         "/home/baka/.codex/sessions/fixture-mounted-session",
			UpdatedAt:   now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed mounted fixture session: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("migrate mounted fixture session: %v", err)
	}

	dockerFixtureSanitizeMountedStore(t, store)

	dbPath := filepath.Join(filepath.Dir(store.Path()), teamstore.SQLiteFileName)
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(dbPath, url.Values{"mode": []string{"rw"}}))
	if err != nil {
		t.Fatalf("open sanitized mounted fixture database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(context.Background(), `PRAGMA query_only = ON`); err != nil {
		t.Fatalf("configure sanitized mounted fixture database: %v", err)
	}
	var trusted, canonicalRevision, projectionRevision int64
	var raw []byte
	if err := db.QueryRowContext(context.Background(), `SELECT projection_trusted, canonical_revision, projection_revision, json FROM sessions WHERE id = ?`, "fixture-mounted-session").Scan(&trusted, &canonicalRevision, &projectionRevision, &raw); err != nil {
		t.Fatalf("read sanitized mounted fixture session: %v", err)
	}
	if trusted != 1 || canonicalRevision <= 0 || projectionRevision != canonicalRevision {
		t.Fatalf("sanitized mounted fixture session lost admission trust: trusted=%d canonical=%d projection=%d", trusted, canonicalRevision, projectionRevision)
	}
	var cwd sql.NullString
	if err := db.QueryRowContext(context.Background(), `SELECT json_extract(json, '$.cwd') FROM sessions WHERE id = ?`, "fixture-mounted-session").Scan(&cwd); err != nil {
		t.Fatalf("inspect sanitized mounted fixture cwd: %v", err)
	}
	if cwd.Valid && strings.TrimSpace(cwd.String) != "" {
		t.Fatalf("sanitized mounted fixture retained cwd %q", cwd.String)
	}

	candidates, authoritative, err := store.HotPollWorkCandidatesExcludingIdleAt(context.Background(), "control-chat", now.Add(-time.Hour), now.Add(time.Second))
	if err != nil {
		t.Fatalf("admit sanitized mounted fixture session: %v", err)
	}
	if !authoritative || len(candidates) != 1 || candidates[0].ID != "fixture-mounted-session" {
		t.Fatalf("sanitized mounted fixture admission = authoritative:%t candidates:%#v, want one trusted candidate", authoritative, candidates)
	}
}

// dockerFixtureSourceProofContentMatches verifies a bounded proof against the
// immutable source-proof manifest assembled by the shell runner. Production
// transcript fingerprints intentionally include physical file identity; a
// copied Docker fixture necessarily has a different inode, so the manifest
// proves the source bytes before this test helper replaces the physical
// identity with the destination identity. If no manifest is configured, the
// historical strict fingerprint comparison remains in force for unit tests.
func dockerFixtureSourceProofContentMatches(path string, start, end int64) (bool, error) {
	manifestPath := strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv))
	if manifestPath == "" {
		return false, nil
	}
	if start < 0 || end < start {
		return false, fmt.Errorf("invalid source proof range [%d,%d)", start, end)
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return false, fmt.Errorf("read Docker source-proof manifest %q: %w", manifestPath, err)
	}
	key := filepath.ToSlash(filepath.Clean(path))
	var expected string
	found := false
	fullFileDigests := make(map[int64]string)
	type coveredRange struct {
		start  int64
		end    int64
		digest string
	}
	var coveredRanges []coveredRange
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 4 {
			return false, fmt.Errorf("invalid Docker source-proof manifest line %q", line)
		}
		if filepath.ToSlash(filepath.Clean(fields[0])) != key {
			continue
		}
		manifestStart, startErr := strconv.ParseInt(fields[1], 10, 64)
		manifestEnd, endErr := strconv.ParseInt(fields[2], 10, 64)
		if startErr != nil || endErr != nil || manifestStart < 0 || manifestEnd < manifestStart {
			return false, fmt.Errorf("invalid Docker source-proof manifest range for %q", fields[0])
		}
		candidate := strings.TrimSpace(fields[3])
		candidate = strings.TrimPrefix(candidate, "sha256:")
		if len(candidate) != sha256.Size*2 {
			return false, fmt.Errorf("invalid Docker source-proof digest for %q [%d,%d)", key, start, end)
		}
		if _, decodeErr := hex.DecodeString(candidate); decodeErr != nil {
			return false, fmt.Errorf("invalid Docker source-proof digest for %q [%d,%d): %w", key, start, end, decodeErr)
		}
		if manifestStart == 0 {
			if previous, exists := fullFileDigests[manifestEnd]; exists && previous != candidate {
				return false, fmt.Errorf("conflicting Docker full-file source-proof manifest entries for %q size %d", key, manifestEnd)
			}
			fullFileDigests[manifestEnd] = candidate
		}
		if manifestStart <= start && manifestEnd >= end && manifestEnd > manifestStart {
			coveredRanges = append(coveredRanges, coveredRange{start: manifestStart, end: manifestEnd, digest: candidate})
		}
		if manifestStart != start || manifestEnd != end {
			continue
		}
		if found && expected != candidate {
			return false, fmt.Errorf("conflicting Docker source-proof manifest entries for %q [%d,%d)", key, start, end)
		}
		expected = candidate
		found = true
	}
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	if info.IsDir() || info.Size() < end {
		return false, fmt.Errorf("source proof range [%d,%d) exceeds copied file %q size %d", start, end, path, info.Size())
	}
	if !found || expected == "" {
		// Recent session files are copied in full before a listener starts. A
		// resumed listener may discover a new checkpoint offset in such a file,
		// so the exact bounded range was not knowable when the manifest was built.
		// A whole-file or copied-tail digest is a stronger proof than the requested
		// prefix/range: verify the complete containing copied range, then accept the
		// bounded subrange only after that witness matches. This also covers sparse
		// proof-tail files whose fixture manifest intentionally starts at a non-zero
		// offset.
		for _, covered := range coveredRanges {
			if info.Size() < covered.end {
				return false, fmt.Errorf("source proof range [%d,%d) exceeds copied file %q size %d", covered.start, covered.end, key, info.Size())
			}
			f, openErr := os.Open(path)
			if openErr != nil {
				return false, openErr
			}
			h := sha256.New()
			_, copyErr := io.Copy(h, io.NewSectionReader(f, covered.start, covered.end-covered.start))
			closeErr := f.Close()
			if copyErr != nil {
				return false, copyErr
			}
			if closeErr != nil {
				return false, closeErr
			}
			actual := hex.EncodeToString(h.Sum(nil))
			if actual != covered.digest {
				return false, fmt.Errorf("copied source proof differs for %q [%d,%d)", key, covered.start, covered.end)
			}
			// A verified containing range proves every requested subrange. This is
			// used for sparse history copies whose durable cursor can advance after
			// fixture assembly; the source bytes from the copied tail through EOF
			// are still authenticated as one immutable range.
			return true, nil
		}
		return false, fmt.Errorf("Docker source-proof manifest has no entry for %q [%d,%d)", key, start, end)
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, io.NewSectionReader(f, start, end-start))
	closeErr := f.Close()
	if copyErr != nil {
		return false, copyErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	actual := hex.EncodeToString(h.Sum(nil))
	return actual == expected, nil
}

func dockerFixtureSourceProofPrefixRange(offset int64) (int64, int64) {
	start := offset - transcriptCheckpointFingerprintBytes
	if start < 0 {
		start = 0
	}
	return start, offset
}

// dockerFixtureSourceFileProof rebuilds a bounded cursor proof against the
// copied file. The fixture runner copies the bytes to a new inode, so keeping
// the source identity from the host would intentionally force a source-rewrite
// recovery. The immutable manifest is the source-byte witness; the caller
// still has to verify every range that it rebinds.
func dockerFixtureSourceFileProof(path string, offset int64) (string, string, os.FileInfo, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", "", nil, fmt.Errorf("source path is empty")
	}
	if offset < 0 {
		return "", "", nil, fmt.Errorf("source proof offset is negative: %d", offset)
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", "", nil, err
	}
	if info.IsDir() || info.Size() < offset {
		return "", "", nil, fmt.Errorf("source proof offset %d exceeds file %q size %d", offset, path, info.Size())
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, info)
	if err != nil {
		return "", "", nil, err
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return "", "", nil, fmt.Errorf("physical source identity is unavailable for %q", path)
	}
	fingerprint := strings.TrimSpace(transcriptCheckpointSourceFingerprint(path, offset))
	if fingerprint == "" && offset > 0 {
		return "", "", nil, fmt.Errorf("bounded source proof could not be rebuilt at offset %d for %q", offset, path)
	}
	// An offset of zero has no consumed source bytes.  Legacy/initial
	// checkpoints may omit an explicit offset entry from the fixture manifest;
	// requiring a synthetic [0,0) manifest row would turn a valid empty-prefix
	// checkpoint into a false fixture failure.  Non-zero offsets still require
	// the immutable byte-range witness below.
	if manifestPath := strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)); manifestPath != "" && offset > 0 {
		start, end := dockerFixtureSourceProofPrefixRange(offset)
		matched, err := dockerFixtureSourceProofContentMatches(path, start, end)
		if err != nil {
			return "", "", nil, err
		}
		if !matched {
			return "", "", nil, fmt.Errorf("copied source proof bytes differ from manifest for %q [%d,%d)", path, start, end)
		}
	}
	return identity, fingerprint, info, nil
}

func dockerFixtureRebindRangeProof(path string, start, end int64, existing string) (string, error) {
	if start < 0 || end < 0 {
		return "", fmt.Errorf("invalid source proof range [%d,%d)", start, end)
	}
	existing = strings.TrimSpace(existing)
	// A zero end is how an in-progress quarantine records that it has found a
	// frontier but has not yet observed the terminating newline.  It is not a
	// proof range and must remain an explicit diagnostic fence in the fixture;
	// accepting it as a normal range would either hash the wrong bytes or turn a
	// real recovery state into fabricated evidence.
	if end == 0 {
		return existing, nil
	}
	if end < start {
		return "", fmt.Errorf("invalid source proof range [%d,%d)", start, end)
	}
	// An empty range carries no source bytes. Preserve the legacy behavior for
	// checkpoints which intentionally have no proof at that boundary. Every
	// non-empty range must be rebuilt and, when the Docker manifest is present,
	// authenticated against the immutable source-byte witness even if the
	// original checkpoint omitted its fingerprint.
	if start == end && existing == "" {
		return "", nil
	}
	fingerprint := strings.TrimSpace(transcriptSourceRangeFingerprint(path, start, end))
	if fingerprint == "" {
		return "", fmt.Errorf("source proof range [%d,%d) is not present in copied file %q", start, end, path)
	}
	if manifestPath := strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)); manifestPath != "" {
		matched, manifestErr := dockerFixtureSourceProofContentMatches(path, start, end)
		if manifestErr != nil {
			return "", manifestErr
		}
		if !matched {
			return "", fmt.Errorf("copied source proof range [%d,%d) differs from the immutable manifest for %q", start, end, path)
		}
	} else if existing != "" && fingerprint != existing {
		return "", fmt.Errorf("copied source proof range [%d,%d) differs from the original proof for %q", start, end, path)
	}
	return fingerprint, nil
}

func dockerFixtureRebindHistoryWatchCheckpoint(checkpoint *teamstore.HistoryWatchCheckpoint) error {
	if checkpoint == nil {
		return nil
	}
	// Store loading deliberately preserves malformed optional recovery proofs and
	// marks them RecoveryProofUnusable.  They are diagnostic fences, not bytes
	// that this fixture may reconstruct.  Rebinding one as if it were a valid
	// range would either invent a proof or reject a real production database
	// before the listener gets a chance to exercise its fail-closed path.
	if checkpoint.RecoveryProofUnusable {
		return nil
	}
	path := strings.TrimSpace(checkpoint.Path)
	var identity string
	var info os.FileInfo
	var err error
	if path != "" {
		var fingerprint string
		identity, fingerprint, info, err = dockerFixtureSourceFileProof(path, maxInt64(0, checkpoint.Offset))
		if err != nil {
			return err
		}
		if !checkpoint.LegacySourceUnverified {
			if existing := strings.TrimSpace(checkpoint.SourceFingerprint); existing != "" && fingerprint != existing && strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) == "" {
				return fmt.Errorf("copied history checkpoint proof differs from the original proof for %q", path)
			}
			checkpoint.SourceGeneration = identity
			if strings.TrimSpace(checkpoint.SourceFingerprint) != "" {
				checkpoint.SourceFingerprint = fingerprint
			}
			checkpoint.SourceChangeTime = teamstore.SourceFileChangeTime(path, info)
		}
		if strings.TrimSpace(checkpoint.PartialSourceIdentity) != "" {
			checkpoint.PartialSourceIdentity = identity
			checkpoint.PartialSourceChangeTime = teamstore.SourceFileChangeTime(path, info)
		}
		if strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity) != "" {
			checkpoint.SourceRewriteRecoveryIdentity = identity
		}
	}
	if checkpoint.PendingHistoryRange != nil {
		r := checkpoint.PendingHistoryRange
		rangePath := firstNonEmptyString(r.SourcePath, path)
		if rangePath == "" {
			return fmt.Errorf("history pending range has no source path")
		}
		rangeIdentity, _, _, rangeErr := dockerFixtureSourceFileProof(rangePath, r.StartOffset)
		if rangeErr != nil {
			return rangeErr
		}
		r.SourceGeneration = rangeIdentity
		r.RangeFingerprint, err = dockerFixtureRebindRangeProof(rangePath, r.StartOffset, r.ExclusiveEnd, r.RangeFingerprint)
		if err != nil {
			return err
		}
	}
	if checkpoint.ContextGap != nil {
		if path == "" {
			return fmt.Errorf("history context gap has no checkpoint source path")
		}
		gap := checkpoint.ContextGap
		gap.SourceGeneration = identity
		gap.RangeFingerprint, err = dockerFixtureRebindRangeProof(path, gap.StartOffset, gap.ExclusiveEndOffset, gap.RangeFingerprint)
		if err != nil {
			return err
		}
	}
	if checkpoint.TranscriptQuarantine != nil {
		q := checkpoint.TranscriptQuarantine
		qPath := firstNonEmptyString(q.SourcePath, path)
		if qPath == "" {
			return fmt.Errorf("history quarantine has no source path")
		}
		qIdentity, qFingerprint, _, qErr := dockerFixtureSourceFileProof(qPath, q.FrontierOffset)
		if qErr != nil {
			return qErr
		}
		q.SourceGeneration = qIdentity
		if strings.TrimSpace(q.SourceFingerprint) != "" && strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) == "" {
			if strings.TrimSpace(q.SourceFingerprint) != qFingerprint {
				return fmt.Errorf("copied history quarantine proof differs from the original proof for %q", qPath)
			}
			q.SourceFingerprint = qFingerprint
		}
		q.RangeFingerprint, err = dockerFixtureRebindRangeProof(qPath, q.FrontierOffset, q.ExclusiveEndOffset, q.RangeFingerprint)
		if err != nil {
			return err
		}
	}
	if checkpoint.TerminalBoundary != nil {
		if path == "" {
			return fmt.Errorf("history terminal boundary has no checkpoint source path")
		}
		boundary := checkpoint.TerminalBoundary
		boundary.SourceGeneration = identity
		boundary.RangeFingerprint, err = dockerFixtureRebindRangeProof(path, boundary.StartOffset, boundary.ExclusiveEndOffset, boundary.RangeFingerprint)
		if err != nil {
			return err
		}
	}
	return nil
}

func dockerFixtureRebindImportCheckpoint(checkpoint *teamstore.ImportCheckpoint) error {
	if checkpoint == nil {
		return nil
	}
	// See the HistoryWatch counterpart above.  An unusable proof must remain an
	// unusable proof in the disposable copy; only an explicit recovery operation
	// may replace it.  In particular, an in-progress quarantine can legitimately
	// have a frontier without an exclusive end yet (for example [offset, 0)).
	if checkpoint.RecoveryProofUnusable {
		return nil
	}
	path := strings.TrimSpace(checkpoint.SourcePath)
	var identity string
	if path != "" {
		var fingerprint string
		var info os.FileInfo
		var err error
		identity, fingerprint, info, err = dockerFixtureSourceFileProof(path, maxInt64(0, checkpoint.LastOffset))
		if err != nil {
			return err
		}
		if !checkpoint.LegacySourceUnverified {
			if existing := strings.TrimSpace(checkpoint.SourceFingerprint); existing != "" && fingerprint != existing && strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) == "" {
				return fmt.Errorf("copied import checkpoint proof differs from the original proof for %q", path)
			}
			checkpoint.SourceGeneration = identity
			if strings.TrimSpace(checkpoint.SourceFingerprint) != "" {
				checkpoint.SourceFingerprint = fingerprint
			}
			checkpoint.SourceChangeTime = teamstore.SourceFileChangeTime(path, info)
		}
		if strings.TrimSpace(checkpoint.PartialSourceIdentity) != "" {
			checkpoint.PartialSourceIdentity = identity
			checkpoint.PartialSourceChangeTime = teamstore.SourceFileChangeTime(path, info)
		}
		if strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity) != "" {
			checkpoint.SourceRewriteRecoveryIdentity = identity
		}
	}
	if checkpoint.PendingHistoryRange != nil {
		r := checkpoint.PendingHistoryRange
		rangePath := firstNonEmptyString(r.SourcePath, path)
		rangeIdentity, _, _, err := dockerFixtureSourceFileProof(rangePath, r.StartOffset)
		if err != nil {
			return err
		}
		r.SourceGeneration = rangeIdentity
		r.RangeFingerprint, err = dockerFixtureRebindRangeProof(rangePath, r.StartOffset, r.ExclusiveEnd, r.RangeFingerprint)
		if err != nil {
			return err
		}
	}
	if checkpoint.ContextGap != nil {
		if path == "" {
			return fmt.Errorf("context gap has no checkpoint source path")
		}
		checkpoint.ContextGap.SourceGeneration = identity
		var err error
		checkpoint.ContextGap.RangeFingerprint, err = dockerFixtureRebindRangeProof(path, checkpoint.ContextGap.StartOffset, checkpoint.ContextGap.ExclusiveEndOffset, checkpoint.ContextGap.RangeFingerprint)
		if err != nil {
			return err
		}
	}
	if checkpoint.TranscriptQuarantine != nil {
		q := checkpoint.TranscriptQuarantine
		qPath := firstNonEmptyString(q.SourcePath, path)
		qIdentity, qFingerprint, _, err := dockerFixtureSourceFileProof(qPath, q.FrontierOffset)
		if err != nil {
			return err
		}
		q.SourceGeneration = qIdentity
		if strings.TrimSpace(q.SourceFingerprint) != "" && strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) == "" {
			if strings.TrimSpace(q.SourceFingerprint) != qFingerprint {
				return fmt.Errorf("copied import quarantine proof differs from the original proof for %q", qPath)
			}
			q.SourceFingerprint = qFingerprint
		}
		q.RangeFingerprint, err = dockerFixtureRebindRangeProof(qPath, q.FrontierOffset, q.ExclusiveEndOffset, q.RangeFingerprint)
		if err != nil {
			return err
		}
	}
	if checkpoint.TerminalBoundary != nil {
		if path == "" {
			return fmt.Errorf("terminal boundary has no checkpoint source path")
		}
		checkpoint.TerminalBoundary.SourceGeneration = identity
		var err error
		checkpoint.TerminalBoundary.RangeFingerprint, err = dockerFixtureRebindRangeProof(path, checkpoint.TerminalBoundary.StartOffset, checkpoint.TerminalBoundary.ExclusiveEndOffset, checkpoint.TerminalBoundary.RangeFingerprint)
		if err != nil {
			return err
		}
	}
	if checkpoint.UnresolvedExecution != nil {
		anchor := checkpoint.UnresolvedExecution
		anchorPath := firstNonEmptyString(anchor.SourcePath, path)
		if strings.TrimSpace(anchor.SourceFingerprint) != "" {
			if anchorPath == "" {
				return fmt.Errorf("unresolved execution anchor has no source path")
			}
			_, anchorFingerprint, _, err := dockerFixtureSourceFileProof(anchorPath, anchor.CutoffOffset)
			if err != nil {
				return err
			}
			// A copied Docker fixture cannot retain the production inode/path
			// fingerprint. dockerFixtureSourceFileProof nevertheless validates the
			// bounded bytes against the immutable manifest when one is configured.
			// Without a manifest, retain the strict historical fingerprint check.
			if strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) == "" && strings.TrimSpace(anchor.SourceFingerprint) != anchorFingerprint {
				return fmt.Errorf("copied import execution anchor proof differs from the original proof for %q", anchorPath)
			}
			if strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) == "" {
				anchor.SourceFingerprint = anchorFingerprint
			}
		}
	}
	return nil
}

// dockerFixtureVerifyOutboxSourceProofs checks source-bound outbox rows after
// the copied Codex files have been assembled. Outbox proofs contain bounded
// content fingerprints rather than a physical inode, so a correct copy keeps
// the JSON unchanged; a sparse hole, truncated range, or wrong path must make
// the fixture fail closed instead of being silently rebound to invented bytes.
func dockerFixtureVerifyOutboxSourceProofs(t *testing.T, store *teamstore.Store) {
	t.Helper()
	if store == nil {
		t.Fatal("cannot verify outbox source proofs on a nil Docker fixture store")
	}
	dbPath := filepath.Join(filepath.Dir(store.Path()), teamstore.SQLiteFileName)
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(dbPath, query))
	if err != nil {
		t.Fatalf("open copied Docker fixture for outbox source-proof verification: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close copied Docker fixture outbox source-proof reader: %v", err)
		}
	})
	rows, err := db.QueryContext(context.Background(), `SELECT id, json FROM outbox_messages WHERE instr(CAST(json AS TEXT), '"transcript_source_path"') > 0 ORDER BY id`)
	if err != nil {
		t.Fatalf("read copied outbox source-proof rows: %v", err)
	}
	defer rows.Close()
	verified := 0
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			t.Fatalf("scan copied outbox source-proof row: %v", err)
		}
		var message teamstore.OutboxMessage
		if err := json.Unmarshal(raw, &message); err != nil {
			t.Fatalf("decode source-bound copied outbox %q: %v", id, err)
		}
		path := strings.TrimSpace(message.TranscriptSourcePath)
		if path == "" {
			t.Fatalf("source-bound copied outbox %q has an empty transcript source path", id)
		}
		if proof := strings.TrimSpace(message.TranscriptSourceProofFingerprint); proof != "" {
			if !message.TranscriptSourceProofOffsetKnown || message.TranscriptSourceProofOffset < 0 {
				t.Fatalf("source-bound copied outbox %q has an unusable prefix proof", id)
			}
			_, actual, _, err := dockerFixtureSourceFileProof(path, message.TranscriptSourceProofOffset)
			if err != nil {
				t.Fatalf("verify copied outbox %q prefix proof: %v", id, err)
			}
			if actual != proof && strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) == "" {
				t.Fatalf("copied outbox %q prefix proof differs from source: got %q want %q", id, actual, proof)
			}
		}
		if message.TranscriptSourceReadProofFingerprint != "" || message.TranscriptSourceReadProofRangeKnown {
			if !message.TranscriptSourceReadProofRangeKnown || message.TranscriptSourceReadProofStartOffset < 0 ||
				message.TranscriptSourceReadProofEndOffset < message.TranscriptSourceReadProofStartOffset ||
				strings.TrimSpace(message.TranscriptSourceReadProofFingerprint) == "" {
				t.Fatalf("source-bound copied outbox %q has an unusable read-range proof", id)
			}
			actual := strings.TrimSpace(transcriptSourceRangeFingerprint(path, message.TranscriptSourceReadProofStartOffset, message.TranscriptSourceReadProofEndOffset))
			if actual == "" {
				t.Fatalf("copied outbox %q read-range proof could not be rebuilt", id)
			}
			if actual != strings.TrimSpace(message.TranscriptSourceReadProofFingerprint) && strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) == "" {
				t.Fatalf("copied outbox %q read-range proof differs from source: got %q want %q", id, actual, message.TranscriptSourceReadProofFingerprint)
			}
			if strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) != "" {
				matched, err := dockerFixtureSourceProofContentMatches(path, message.TranscriptSourceReadProofStartOffset, message.TranscriptSourceReadProofEndOffset)
				if err != nil || !matched {
					t.Fatalf("copied outbox %q read-range bytes differ from immutable source manifest: matched=%t err=%v", id, matched, err)
				}
			}
		}
		verified++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate copied outbox source-proof rows: %v", err)
	}
	if verified == 0 {
		t.Log("copied fixture contains no source-bound outbox rows")
	}
}

// dockerFixtureRebindSourceProofs applies the destination file identity to
// source-bound metadata in the disposable store. All byte ranges were included
// in the shell fixture inventory before this helper runs; a missing range is an
// error rather than a reason to manufacture a new proof over sparse zeros.
func dockerFixtureRebindSourceProofs(t *testing.T, store *teamstore.Store) {
	t.Helper()
	if store == nil {
		t.Fatal("cannot rebind source proofs on a nil Docker fixture store")
	}
	ctx := context.Background()
	if err := store.UpdateHistoryWatch(ctx, func(history map[string]teamstore.HistoryWatchCheckpoint, _ *time.Time) error {
		for id, checkpoint := range history {
			if err := dockerFixtureRebindHistoryWatchCheckpoint(&checkpoint); err != nil {
				return fmt.Errorf("history checkpoint %q: %w", id, err)
			}
			history[id] = checkpoint
		}
		return nil
	}); err != nil {
		t.Fatalf("rebind copied HistoryWatch source proofs: %v", err)
	}
	sessions, err := store.SessionContexts(ctx)
	if err != nil {
		t.Fatalf("load copied session IDs for source-proof rebind: %v", err)
	}
	sessionIDs := make([]string, 0, len(sessions))
	for id := range sessions {
		sessionIDs = append(sessionIDs, id)
	}
	checkpoints, err := store.ImportCheckpointsForSessions(ctx, sessionIDs)
	if err != nil {
		t.Fatalf("load copied ImportCheckpoint source proofs: %v", err)
	}
	for id, checkpoint := range checkpoints {
		updated := checkpoint
		if err := dockerFixtureRebindImportCheckpoint(&updated); err != nil {
			t.Fatalf("rebind ImportCheckpoint %q source proofs: %v", id, err)
		}
		before, err := json.Marshal(checkpoint)
		if err != nil {
			t.Fatalf("encode original ImportCheckpoint %q: %v", id, err)
		}
		after, err := json.Marshal(updated)
		if err != nil {
			t.Fatalf("encode rebound ImportCheckpoint %q: %v", id, err)
		}
		if bytes.Equal(before, after) {
			continue
		}
		if _, found, err := store.UpdateImportCheckpoint(ctx, id, func(current teamstore.ImportCheckpoint, found bool, _ time.Time) (teamstore.ImportCheckpoint, bool, error) {
			if !found {
				return current, false, nil
			}
			return updated, true, nil
		}); err != nil {
			t.Fatalf("persist rebound ImportCheckpoint %q: %v", id, err)
		} else if !found {
			t.Fatalf("ImportCheckpoint %q disappeared during source-proof rebind", id)
		}
	}
}

func dockerFixtureSanitizeRegistryWorkspacePaths(registry *Registry) {
	if registry == nil {
		return
	}
	for i := range registry.Sessions {
		registry.Sessions[i].Cwd = ""
	}
}

func dockerFixtureSourcePath(fixtureRoot string, persistedPath string) string {
	rawPersistedPath := strings.TrimSpace(persistedPath)
	if rawPersistedPath == "" {
		return ""
	}
	// Persisted Codex paths use the host's native separator, while this test
	// also exercises the stable slash form used by the Docker fixture. Normalize
	// both before prefix matching so the containment rule behaves identically on
	// Unix and Windows.
	persistedPath = filepath.Clean(filepath.FromSlash(rawPersistedPath))
	sourcePrefix := strings.TrimSpace(os.Getenv(dockerCodexSourceEnv))
	if sourcePrefix == "" {
		sourcePrefix = dockerFixtureCodexDir
	}
	sourcePrefix = filepath.Clean(filepath.FromSlash(sourcePrefix))
	if !strings.HasSuffix(sourcePrefix, string(filepath.Separator)) {
		sourcePrefix += string(filepath.Separator)
	}
	prefixes := []string{sourcePrefix}
	canonicalPrefix := filepath.Clean(filepath.FromSlash(dockerFixtureCodexDir))
	if !strings.HasSuffix(canonicalPrefix, string(filepath.Separator)) {
		canonicalPrefix += string(filepath.Separator)
	}
	if canonicalPrefix != sourcePrefix {
		prefixes = append(prefixes, canonicalPrefix)
	}
	for _, prefix := range prefixes {
		if !strings.HasPrefix(persistedPath, prefix) {
			continue
		}
		relative := filepath.Clean(filepath.FromSlash(strings.TrimPrefix(persistedPath, prefix)))
		sessionsRoot := filepath.FromSlash("sessions")
		if relative != sessionsRoot && !strings.HasPrefix(relative, sessionsRoot+string(filepath.Separator)) {
			// The persisted path is inside the Codex home prefix but not inside
			// the copied sessions tree. Never let a fixture test fall back to
			// auth/config data or a sibling directory.
			return ""
		}
		if os.Getenv(dockerCodexMountEnv) == "1" {
			return dockerFixtureContainedRegularPath(filepath.Dir(sourcePrefix), persistedPath)
		}
		return dockerFixtureContainedRegularPath(filepath.Join(fixtureRoot, "codex"), filepath.Join(fixtureRoot, "codex", relative))
	}
	if strings.TrimSpace(os.Getenv(dockerFixtureDirEnv)) != "" {
		// Docker fixture mode must fail closed for relative, missing-prefix, or
		// traversal paths. The shell runner validates the source projection too;
		// this second boundary prevents a future test helper from accidentally
		// bypassing that check.
		return ""
	}
	return rawPersistedPath
}

func dockerFixtureContainedRegularPath(root string, candidate string) string {
	rootAbs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return ""
	}
	candidateAbs, err := filepath.Abs(filepath.Clean(candidate))
	if err != nil {
		return ""
	}
	relative, err := filepath.Rel(rootAbs, candidateAbs)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return ""
	}
	info, err := os.Lstat(candidateAbs)
	if os.IsNotExist(err) {
		// Preserve a missing path so dockerFixtureHistoryLag can report an
		// incomplete fixture instead of silently ignoring the checkpoint.
		return candidateAbs
	}
	if err != nil || !info.Mode().IsRegular() {
		return ""
	}
	resolvedRoot, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return ""
	}
	resolvedCandidate, err := filepath.EvalSymlinks(candidateAbs)
	if err != nil {
		return ""
	}
	resolvedRelative, err := filepath.Rel(resolvedRoot, resolvedCandidate)
	if err != nil || resolvedRelative == ".." || strings.HasPrefix(resolvedRelative, ".."+string(filepath.Separator)) {
		return ""
	}
	return candidateAbs
}

func dockerFixtureHistoryLag(t *testing.T, fixtureRoot string, state teamstore.State) (int, int64, int64) {
	t.Helper()
	threadIDs := make(map[string]struct{}, len(state.Sessions))
	for _, session := range state.Sessions {
		if threadID := strings.TrimSpace(session.CodexThreadID); threadID != "" {
			threadIDs[threadID] = struct{}{}
		}
	}
	paths := make(map[string]struct{})
	missing := 0
	ahead := 0
	var extraBytes int64
	var maxExtraBytes int64
	for _, checkpoint := range state.HistoryWatch {
		if _, ok := threadIDs[strings.TrimSpace(checkpoint.ThreadID)]; !ok {
			continue
		}
		filePath := dockerFixtureSourcePath(fixtureRoot, checkpoint.Path)
		if strings.TrimSpace(filePath) == "" {
			t.Fatalf("copied Codex history path cannot be mapped into the read-only sessions fixture: %q", checkpoint.Path)
		}
		if _, ok := paths[filePath]; ok {
			continue
		}
		paths[filePath] = struct{}{}
		info, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				missing++
				continue
			}
			t.Fatalf("stat copied Codex history %q: %v", filePath, err)
		}
		if info.IsDir() {
			t.Fatalf("copied Codex history path is a directory: %q", filePath)
		}
		if checkpoint.Offset < 0 {
			t.Fatalf("negative durable Codex history offset for %q: %d", filePath, checkpoint.Offset)
		}
		if expected := strings.TrimSpace(checkpoint.SourceFingerprint); expected != "" &&
			!checkpoint.LegacySourceUnverified && !checkpoint.RecoveryProofUnusable {
			_, actual, _, proofErr := dockerFixtureSourceFileProof(filePath, checkpoint.Offset)
			if proofErr != nil {
				t.Fatalf("verify copied Codex history proof for %q: %v", filePath, proofErr)
			}
			if actual != expected {
				t.Fatalf("copied Codex history proof for %q = %q, want %q", filePath, actual, expected)
			}
		}
		if extra := info.Size() - checkpoint.Offset; extra > 0 {
			ahead++
			extraBytes += extra
			if extra > maxExtraBytes {
				maxExtraBytes = extra
			}
		}
	}
	t.Logf("copied Teams-linked Codex history: total_history_watch_rows=%d unique_files=%d missing=%d ahead=%d extra_bytes=%d max_extra_bytes=%d tail_budget=%d", len(state.HistoryWatch), len(paths), missing, ahead, extraBytes, maxExtraBytes, historyTieredMaxTailBytes)
	if len(paths) == 0 {
		t.Fatal("copied fixture contains no Codex history files linked to Teams sessions")
	}
	if missing != 0 {
		t.Fatalf("copied fixture is incomplete: %d Teams-linked Codex history files are missing", missing)
	}
	if ahead == 0 || extraBytes == 0 {
		t.Fatal("copied fixture no longer demonstrates the observed SQLite cursor lag")
	}
	if maxExtraBytes <= historyTieredMaxTailBytes {
		t.Fatalf("maximum Codex cursor lag = %d bytes, want more than one bounded tail pass (%d bytes)", maxExtraBytes, historyTieredMaxTailBytes)
	}
	return ahead, extraBytes, maxExtraBytes
}

func dockerFixtureChatIDFromMessagesPath(path string) (string, bool) {
	const prefix = "/chats/"
	const suffix = "/messages"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return "", false
	}
	encoded := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	if encoded == "" {
		return "", false
	}
	chatID, err := url.PathUnescape(encoded)
	if err != nil || strings.TrimSpace(chatID) == "" {
		return "", false
	}
	return chatID, true
}

func TestDockerFixtureSourcePathFailsClosedOutsideCopiedSessions(t *testing.T) {
	fixtureRoot := t.TempDir()
	sessionsRoot := filepath.Join(fixtureRoot, "codex", "sessions", "2026", "09", "07")
	if err := os.MkdirAll(sessionsRoot, 0o700); err != nil {
		t.Fatalf("create copied sessions root: %v", err)
	}
	valid := filepath.Join(sessionsRoot, "thread.jsonl")
	if err := os.WriteFile(valid, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write copied session: %v", err)
	}
	previousFixture, fixtureWasSet := os.LookupEnv(dockerFixtureDirEnv)
	previousPrefix, prefixWasSet := os.LookupEnv(dockerCodexSourceEnv)
	t.Cleanup(func() {
		if fixtureWasSet {
			_ = os.Setenv(dockerFixtureDirEnv, previousFixture)
		} else {
			_ = os.Unsetenv(dockerFixtureDirEnv)
		}
		if prefixWasSet {
			_ = os.Setenv(dockerCodexSourceEnv, previousPrefix)
		} else {
			_ = os.Unsetenv(dockerCodexSourceEnv)
		}
	})
	if err := os.Setenv(dockerFixtureDirEnv, fixtureRoot); err != nil {
		t.Fatalf("set fixture root: %v", err)
	}
	if err := os.Setenv(dockerCodexSourceEnv, "/source/.codex/"); err != nil {
		t.Fatalf("set source prefix: %v", err)
	}
	if got := dockerFixtureSourcePath(fixtureRoot, "/source/.codex/sessions/2026/09/07/thread.jsonl"); got != valid {
		t.Fatalf("valid copied session path = %q, want %q", got, valid)
	}
	for _, path := range []string{
		"/source/.codex/sessions-evil/thread.jsonl",
		"/source/.codex/sessions/../secrets.jsonl",
		"/other/.codex/sessions/thread.jsonl",
		"relative/sessions/thread.jsonl",
	} {
		if got := dockerFixtureSourcePath(fixtureRoot, path); got != "" {
			t.Fatalf("unsafe copied session path %q mapped to %q", path, got)
		}
	}
}

func TestDockerFixtureRebindSourceProofRequiresMatchingCopiedBytes(t *testing.T) {
	root := t.TempDir()
	original := filepath.Join(root, "original.jsonl")
	copied := filepath.Join(root, "copied.jsonl")
	body := []byte("prefix record\nsecond record\n")
	if err := os.WriteFile(original, body, 0o600); err != nil {
		t.Fatalf("write original transcript: %v", err)
	}
	if err := os.WriteFile(copied, body, 0o600); err != nil {
		t.Fatalf("write copied transcript: %v", err)
	}
	offset := int64(len("prefix record\n"))
	originalIdentity, err := teamstore.SourceFileIdentity(original)
	if err != nil {
		t.Fatalf("read original transcript identity: %v", err)
	}
	originalInfo, err := os.Stat(original)
	if err != nil {
		t.Fatalf("stat original transcript: %v", err)
	}
	checkpoint := teamstore.HistoryWatchCheckpoint{
		Path:              copied,
		Offset:            offset,
		SourceGeneration:  originalIdentity,
		SourceFingerprint: transcriptCheckpointSourceFingerprint(original, offset),
		Size:              originalInfo.Size(),
	}
	if checkpoint.SourceFingerprint == "" {
		t.Fatal("original transcript proof is empty")
	}
	manifestPath := filepath.Join(root, "source-proof-manifest.tsv")
	prefixDigest := sha256.Sum256(body[:offset])
	manifest := fmt.Sprintf("%s\t0\t%d\t%s\n", filepath.ToSlash(filepath.Clean(copied)), offset, hex.EncodeToString(prefixDigest[:]))
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write source-proof manifest: %v", err)
	}
	previousManifest, manifestWasSet := os.LookupEnv(dockerSourceProofManifestEnv)
	if err := os.Setenv(dockerSourceProofManifestEnv, manifestPath); err != nil {
		t.Fatalf("set source-proof manifest: %v", err)
	}
	t.Cleanup(func() {
		if manifestWasSet {
			_ = os.Setenv(dockerSourceProofManifestEnv, previousManifest)
		} else {
			_ = os.Unsetenv(dockerSourceProofManifestEnv)
		}
	})
	if err := dockerFixtureRebindHistoryWatchCheckpoint(&checkpoint); err != nil {
		t.Fatalf("rebind matching copied transcript proof: %v", err)
	}
	copiedIdentity, err := teamstore.SourceFileIdentity(copied)
	if err != nil {
		t.Fatalf("read copied transcript identity: %v", err)
	}
	if copiedIdentity == "" || checkpoint.SourceGeneration != copiedIdentity {
		t.Fatalf("rebound source generation = %q, want copied identity %q", checkpoint.SourceGeneration, copiedIdentity)
	}
	if !historyWatchSourcePrefixMatches(copied, historyTieredFileStateFromHistoryWatch(checkpoint)) {
		t.Fatal("rebound checkpoint did not pass the real source-prefix verifier")
	}

	if err := os.WriteFile(copied, []byte("rewritten prefix\nsecond record\n"), 0o600); err != nil {
		t.Fatalf("rewrite copied transcript: %v", err)
	}
	unchanged := teamstore.HistoryWatchCheckpoint{
		Path:              copied,
		Offset:            offset,
		SourceGeneration:  originalIdentity,
		SourceFingerprint: checkpoint.SourceFingerprint,
		Size:              int64(len("rewritten prefix\nsecond record\n")),
	}
	if err := dockerFixtureRebindHistoryWatchCheckpoint(&unchanged); err == nil {
		t.Fatal("rebind accepted copied transcript with a changed bounded prefix")
	}
}

func TestDockerFixtureRebindSourceProofAcceptsVerifiedFullRecentFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "recent.jsonl")
	body := []byte("first record\nsecond record\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write recent transcript: %v", err)
	}
	digest := sha256.Sum256(body)
	manifestPath := filepath.Join(root, "source-proof-manifest.tsv")
	manifest := fmt.Sprintf("%s\t0\t%d\t%s\n", filepath.ToSlash(filepath.Clean(path)), len(body), hex.EncodeToString(digest[:]))
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write full-file source-proof manifest: %v", err)
	}
	previousManifest, manifestWasSet := os.LookupEnv(dockerSourceProofManifestEnv)
	if err := os.Setenv(dockerSourceProofManifestEnv, manifestPath); err != nil {
		t.Fatalf("set source-proof manifest: %v", err)
	}
	t.Cleanup(func() {
		if manifestWasSet {
			_ = os.Setenv(dockerSourceProofManifestEnv, previousManifest)
		} else {
			_ = os.Unsetenv(dockerSourceProofManifestEnv)
		}
	})

	checkpoint := teamstore.HistoryWatchCheckpoint{
		Path:              path,
		Offset:            int64(len("first record\n")),
		SourceFingerprint: transcriptCheckpointSourceFingerprint(path, int64(len("first record\n"))),
		Size:              int64(len(body)),
	}
	if err := dockerFixtureRebindHistoryWatchCheckpoint(&checkpoint); err != nil {
		t.Fatalf("full recent-file proof rejected a bounded checkpoint: %v", err)
	}

	if err := os.WriteFile(path, []byte("rewritten data\nsecond record\n"), 0o600); err != nil {
		t.Fatalf("rewrite recent transcript: %v", err)
	}
	changed := checkpoint
	if err := dockerFixtureRebindHistoryWatchCheckpoint(&changed); err == nil {
		t.Fatal("full recent-file proof accepted changed copied bytes")
	}
}

func TestDockerFixtureRebindRangeProofDoesNotSkipEmptyOriginalFingerprint(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "range.jsonl")
	body := []byte("trusted range\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write range transcript: %v", err)
	}
	manifestPath := filepath.Join(root, "source-proof-manifest.tsv")
	wrongDigest := sha256.Sum256([]byte("different range\n"))
	manifest := fmt.Sprintf("%s\t0\t%d\t%s\n", filepath.ToSlash(filepath.Clean(path)), len(body), hex.EncodeToString(wrongDigest[:]))
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write mismatching source-proof manifest: %v", err)
	}
	previousManifest, manifestWasSet := os.LookupEnv(dockerSourceProofManifestEnv)
	if err := os.Setenv(dockerSourceProofManifestEnv, manifestPath); err != nil {
		t.Fatalf("set source-proof manifest: %v", err)
	}
	t.Cleanup(func() {
		if manifestWasSet {
			_ = os.Setenv(dockerSourceProofManifestEnv, previousManifest)
		} else {
			_ = os.Unsetenv(dockerSourceProofManifestEnv)
		}
	})

	if _, err := dockerFixtureRebindRangeProof(path, 0, int64(len(body)), ""); err == nil {
		t.Fatal("non-empty range with an empty original fingerprint bypassed the immutable manifest")
	}

	digest := sha256.Sum256(body)
	manifest = fmt.Sprintf("%s\t0\t%d\t%s\n", filepath.ToSlash(filepath.Clean(path)), len(body), hex.EncodeToString(digest[:]))
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("rewrite matching source-proof manifest: %v", err)
	}
	got, err := dockerFixtureRebindRangeProof(path, 0, int64(len(body)), "")
	if err != nil {
		t.Fatalf("matching immutable manifest rejected empty original fingerprint: %v", err)
	}
	if got == "" {
		t.Fatal("matching non-empty range returned an empty rebound fingerprint")
	}
}

func TestDockerFixtureRebindRangeProofPreservesIncompleteQuarantine(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "incomplete.jsonl")
	if err := os.WriteFile(path, []byte("complete prefix\npartial"), 0o600); err != nil {
		t.Fatalf("write incomplete transcript: %v", err)
	}
	const existing = "sha256:durable-incomplete-witness"
	got, err := dockerFixtureRebindRangeProof(path, 17, 0, existing)
	if err != nil {
		t.Fatalf("incomplete quarantine range should remain a diagnostic fence: %v", err)
	}
	if got != existing {
		t.Fatalf("incomplete quarantine proof = %q, want original diagnostic %q", got, existing)
	}
}

func TestDockerFixtureSourceProofAcceptsVerifiedSparseContainingRange(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sparse.jsonl")
	body := []byte("unavailable prefix\ntrusted copied tail\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("write sparse transcript: %v", err)
	}
	coverStart := int64(len("unavailable prefix\n"))
	digest := sha256.Sum256(body[coverStart:])
	manifestPath := filepath.Join(root, "source-proof-manifest.tsv")
	manifest := fmt.Sprintf("%s\t%d\t%d\t%s\n", filepath.ToSlash(filepath.Clean(path)), coverStart, len(body), hex.EncodeToString(digest[:]))
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write sparse source-proof manifest: %v", err)
	}
	previousManifest, manifestWasSet := os.LookupEnv(dockerSourceProofManifestEnv)
	if err := os.Setenv(dockerSourceProofManifestEnv, manifestPath); err != nil {
		t.Fatalf("set sparse source-proof manifest: %v", err)
	}
	t.Cleanup(func() {
		if manifestWasSet {
			_ = os.Setenv(dockerSourceProofManifestEnv, previousManifest)
		} else {
			_ = os.Unsetenv(dockerSourceProofManifestEnv)
		}
	})

	matched, err := dockerFixtureSourceProofContentMatches(path, coverStart+1, int64(len(body)-1))
	if err != nil || !matched {
		t.Fatalf("verified sparse containing range = matched:%t err:%v, want true", matched, err)
	}
	if err := os.WriteFile(path, []byte("unavailable prefix\ntrusted changed tail\n"), 0o600); err != nil {
		t.Fatalf("rewrite sparse transcript: %v", err)
	}
	matched, err = dockerFixtureSourceProofContentMatches(path, coverStart+1, int64(len(body)-1))
	if err == nil || matched {
		t.Fatalf("changed sparse containing range = matched:%t err:%v, want rejection", matched, err)
	}
}

func TestDockerFixtureRebindZeroOffsetAllowsMissingEmptyPrefixManifest(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "copied.jsonl")
	if err := os.WriteFile(path, []byte("record\n"), 0o600); err != nil {
		t.Fatalf("write copied transcript: %v", err)
	}
	manifestPath := filepath.Join(root, "source-proof-manifest.tsv")
	if err := os.WriteFile(manifestPath, []byte("# intentionally no zero-length row\n"), 0o600); err != nil {
		t.Fatalf("write source-proof manifest: %v", err)
	}
	previousManifest, manifestWasSet := os.LookupEnv(dockerSourceProofManifestEnv)
	if err := os.Setenv(dockerSourceProofManifestEnv, manifestPath); err != nil {
		t.Fatalf("set source-proof manifest: %v", err)
	}
	t.Cleanup(func() {
		if manifestWasSet {
			_ = os.Setenv(dockerSourceProofManifestEnv, previousManifest)
		} else {
			_ = os.Unsetenv(dockerSourceProofManifestEnv)
		}
	})
	checkpoint := teamstore.HistoryWatchCheckpoint{Path: path, Offset: 0}
	if err := dockerFixtureRebindHistoryWatchCheckpoint(&checkpoint); err != nil {
		t.Fatalf("zero-offset checkpoint should not require an empty-prefix manifest row: %v", err)
	}
	if strings.TrimSpace(checkpoint.SourceGeneration) == "" {
		t.Fatal("zero-offset checkpoint did not receive the copied source identity")
	}
}

func TestDockerFixtureRebindPreservesUnusableRecoveryProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unusable.jsonl")
	if err := os.WriteFile(path, []byte("record\n"), 0o600); err != nil {
		t.Fatalf("write unusable-proof transcript: %v", err)
	}
	history := teamstore.HistoryWatchCheckpoint{
		Path:                  path,
		Offset:                42,
		SourceGeneration:      "host-generation",
		RecoveryProofUnusable: true,
		TranscriptQuarantine: &teamstore.TranscriptQuarantine{
			Kind:               "incomplete-range",
			SourcePath:         path,
			SourceGeneration:   "host-generation",
			FrontierRecordID:   "record-1",
			FrontierLine:       7,
			FrontierOffset:     42,
			ExclusiveEndOffset: 0,
			RangeFingerprint:   "stale-or-incomplete",
		},
	}
	historyBefore := history
	if err := dockerFixtureRebindHistoryWatchCheckpoint(&history); err != nil {
		t.Fatalf("unusable history proof should remain diagnostic: %v", err)
	}
	if !reflect.DeepEqual(history, historyBefore) {
		t.Fatalf("unusable history proof was rebound: before=%#v after=%#v", historyBefore, history)
	}

	importCheckpoint := teamstore.ImportCheckpoint{
		ID:                    "transcript:unusable",
		SourcePath:            path,
		LastOffset:            42,
		RecoveryProofUnusable: true,
		TranscriptQuarantine: &teamstore.TranscriptQuarantine{
			Kind:               "incomplete-range",
			SourcePath:         path,
			SourceGeneration:   "host-generation",
			FrontierRecordID:   "record-1",
			FrontierLine:       7,
			FrontierOffset:     42,
			ExclusiveEndOffset: 0,
			RangeFingerprint:   "stale-or-incomplete",
		},
	}
	importBefore := importCheckpoint
	if err := dockerFixtureRebindImportCheckpoint(&importCheckpoint); err != nil {
		t.Fatalf("unusable import proof should remain diagnostic: %v", err)
	}
	if !reflect.DeepEqual(importCheckpoint, importBefore) {
		t.Fatalf("unusable import proof was rebound: before=%#v after=%#v", importBefore, importCheckpoint)
	}
}

func TestDockerFixtureRebindImportExecutionAnchorVerifiesManifestBytes(t *testing.T) {
	root := t.TempDir()
	original := filepath.Join(root, "original.jsonl")
	copied := filepath.Join(root, "copied.jsonl")
	body := []byte("prefix record\nsecond record\n")
	if err := os.WriteFile(original, body, 0o600); err != nil {
		t.Fatalf("write original transcript: %v", err)
	}
	if err := os.WriteFile(copied, body, 0o600); err != nil {
		t.Fatalf("write copied transcript: %v", err)
	}
	offset := int64(len("prefix record\n"))
	originalProof := transcriptCheckpointSourceFingerprint(original, offset)
	if originalProof == "" {
		t.Fatal("original execution-anchor proof is empty")
	}
	prefixDigest := sha256.Sum256(body[:offset])
	manifestPath := filepath.Join(root, "source-proof-manifest.tsv")
	manifest := fmt.Sprintf("%s\t0\t%d\t%s\n", filepath.ToSlash(filepath.Clean(copied)), offset, hex.EncodeToString(prefixDigest[:]))
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatalf("write source-proof manifest: %v", err)
	}
	previousManifest, manifestWasSet := os.LookupEnv(dockerSourceProofManifestEnv)
	if err := os.Setenv(dockerSourceProofManifestEnv, manifestPath); err != nil {
		t.Fatalf("set source-proof manifest: %v", err)
	}
	t.Cleanup(func() {
		if manifestWasSet {
			_ = os.Setenv(dockerSourceProofManifestEnv, previousManifest)
		} else {
			_ = os.Unsetenv(dockerSourceProofManifestEnv)
		}
	})

	checkpoint := teamstore.ImportCheckpoint{
		SourcePath:        copied,
		SourceFingerprint: originalProof,
		LastOffset:        offset,
		LastOffsetKnown:   true,
		UnresolvedExecution: &teamstore.ExecutionAnchor{
			SourcePath:        copied,
			SourceFingerprint: originalProof,
			CutoffOffset:      offset,
			State:             "unresolved",
		},
	}
	if err := dockerFixtureRebindImportCheckpoint(&checkpoint); err != nil {
		t.Fatalf("matching copied unresolved execution anchor rejected: %v", err)
	}

	if err := os.WriteFile(copied, []byte("rewritten prefix\nsecond record\n"), 0o600); err != nil {
		t.Fatalf("rewrite copied transcript: %v", err)
	}
	if err := dockerFixtureRebindImportCheckpoint(&checkpoint); err == nil {
		t.Fatal("unresolved execution anchor accepted copied bytes that differ from immutable manifest")
	}
}

func TestDockerFixtureSourceProofManifestRejectsInvalidDigest(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "session.jsonl")
	if err := os.WriteFile(path, []byte("record\n"), 0o600); err != nil {
		t.Fatalf("write source-proof file: %v", err)
	}
	manifestPath := filepath.Join(root, "source-proof-manifest.tsv")
	line := fmt.Sprintf("%s\t0\t1\tnot-a-sha256-digest\n", filepath.ToSlash(path))
	if err := os.WriteFile(manifestPath, []byte(line), 0o600); err != nil {
		t.Fatalf("write invalid source-proof manifest: %v", err)
	}
	previous, wasSet := os.LookupEnv(dockerSourceProofManifestEnv)
	if err := os.Setenv(dockerSourceProofManifestEnv, manifestPath); err != nil {
		t.Fatalf("set source-proof manifest: %v", err)
	}
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv(dockerSourceProofManifestEnv, previous)
		} else {
			_ = os.Unsetenv(dockerSourceProofManifestEnv)
		}
	})
	if matched, err := dockerFixtureSourceProofContentMatches(path, 0, 1); err == nil || matched {
		t.Fatalf("invalid source-proof digest accepted: matched=%v err=%v", matched, err)
	}
}
