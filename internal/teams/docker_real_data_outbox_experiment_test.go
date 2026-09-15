package teams

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	dockerRealDataOutboxExperimentEnv = "CXP_TEAMS_DOCKER_REAL_DATA_OUTBOX_EXPERIMENT"
	dockerRealDataOutboxDurationEnv   = "CXP_TEAMS_DOCKER_REAL_DATA_OUTBOX_DURATION"
	dockerRealDataOutboxDefaultWindow = 60 * time.Second
	dockerRealDataOutboxMinimumWindow = 10 * time.Second
)

type dockerRealDataOutboxCounts struct {
	Queued   int64
	Sending  int64
	Accepted int64
	Sent     int64
	Skipped  int64
}

func dockerRealDataOutboxGraphRequestTimingSummary(records []dockerRealDataGraphRequest) string {
	if len(records) == 0 {
		return "count=0"
	}
	type aggregate struct {
		count     int
		total     time.Duration
		max       time.Duration
		statuses  map[int]int
		throttled int
		accepted  int
	}
	byOperation := make(map[string]*aggregate)
	for _, record := range records {
		operation := strings.TrimSpace(record.Operation)
		if operation == "" {
			operation = "unknown"
		}
		item := byOperation[operation]
		if item == nil {
			item = &aggregate{statuses: make(map[int]int)}
			byOperation[operation] = item
		}
		elapsed := record.CompletedAt.Sub(record.StartedAt)
		if elapsed < 0 {
			elapsed = 0
		}
		item.count++
		item.total += elapsed
		if elapsed > item.max {
			item.max = elapsed
		}
		item.statuses[record.StatusCode]++
		if record.StatusCode == 429 {
			item.throttled++
		}
		if record.RemoteAccepted {
			item.accepted++
		}
	}
	operations := make([]string, 0, len(byOperation))
	for operation := range byOperation {
		operations = append(operations, operation)
	}
	sort.Strings(operations)
	parts := make([]string, 0, len(operations))
	for _, operation := range operations {
		item := byOperation[operation]
		parts = append(parts, fmt.Sprintf("%s{n=%d,total=%s,avg=%s,max=%s,statuses=%v,429=%d,accepted=%d}",
			operation, item.count, item.total, item.total/time.Duration(item.count), item.max, item.statuses, item.throttled, item.accepted))
	}
	return fmt.Sprintf("count=%d %s", len(records), strings.Join(parts, " "))
}

func dockerRealDataOutboxFixtureRoot(t *testing.T) string {
	t.Helper()
	root := strings.TrimSpace(os.Getenv(dockerFixtureDirEnv))
	if root == "" {
		t.Skipf("set %s to run the copied SQLite outbox Docker experiment", dockerFixtureDirEnv)
	}
	for _, name := range []string{"teams/state.json", "teams/store.sqlite", "teams/registry.json"} {
		path := filepath.Join(root, filepath.FromSlash(name))
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("Docker outbox fixture file %q: %v", name, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			t.Fatalf("Docker outbox fixture file %q is not a regular file", name)
		}
	}
	if os.Getenv(dockerCodexMountEnv) == "1" {
		sessionsPath := filepath.Join(root, "codex", "sessions")
		info, err := os.Lstat(sessionsPath)
		if err != nil {
			t.Fatalf("Docker outbox Codex sessions fixture: %v", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			t.Fatalf("Docker outbox Codex sessions fixture is not a real directory: %q", sessionsPath)
		}
		manifestPath := filepath.Join(root, "source-proof-manifest.tsv")
		manifestInfo, err := os.Lstat(manifestPath)
		if err != nil {
			t.Fatalf("Docker outbox source-proof manifest: %v", err)
		}
		if manifestInfo.Mode()&os.ModeSymlink != 0 || !manifestInfo.Mode().IsRegular() {
			t.Fatalf("Docker outbox source-proof manifest is not a regular file: %q", manifestPath)
		}
	}
	return root
}

func dockerRealDataOutboxDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(dockerRealDataOutboxDurationEnv))
	if raw == "" {
		return dockerRealDataOutboxDefaultWindow
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < dockerRealDataOutboxMinimumWindow {
		t.Fatalf("%s must be at least %s: %q", dockerRealDataOutboxDurationEnv, dockerRealDataOutboxMinimumWindow, raw)
	}
	return duration
}

func dockerRealDataOutboxCountsFromSQLite(ctx context.Context, path string) (dockerRealDataOutboxCounts, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return dockerRealDataOutboxCounts{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var counts dockerRealDataOutboxCounts
	err = db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN status = ? THEN 1 ELSE 0 END), 0)
		FROM outbox_messages`,
		string(teamstore.OutboxStatusQueued),
		string(teamstore.OutboxStatusSending),
		string(teamstore.OutboxStatusAccepted),
		string(teamstore.OutboxStatusSent),
		string(teamstore.OutboxStatusSkipped),
	).Scan(&counts.Queued, &counts.Sending, &counts.Accepted, &counts.Sent, &counts.Skipped)
	return counts, err
}

func dockerRealDataPendingOutboxChatIDsFromSQLite(ctx context.Context, path string) ([]string, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT teams_chat_id
		FROM outbox_messages
		WHERE status IN (?, ?, ?)
		  AND trim(COALESCE(teams_chat_id, '')) <> ''
		  AND teams_chat_id = trim(teams_chat_id)`,
		string(teamstore.OutboxStatusQueued),
		string(teamstore.OutboxStatusSending),
		string(teamstore.OutboxStatusAccepted),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var chatID string
		if err := rows.Scan(&chatID); err != nil {
			return nil, err
		}
		if chatID = strings.TrimSpace(chatID); chatID != "" {
			ids = append(ids, chatID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(ids)
	return ids, nil
}

func dockerRealDataOutboxSkippedReasonsFromSQLite(ctx context.Context, path string) (map[string]string, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	rows, err := db.QueryContext(ctx, `
		SELECT id, COALESCE(json_extract(json, '$.last_send_error'), '')
		FROM outbox_messages
		WHERE status = ?
		ORDER BY id`, string(teamstore.OutboxStatusSkipped))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	reasons := make(map[string]string)
	for rows.Next() {
		var id, reason string
		if err := rows.Scan(&id, &reason); err != nil {
			return nil, err
		}
		reasons[strings.TrimSpace(id)] = strings.TrimSpace(reason)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return reasons, rows.Close()
}

// dockerRealDataRebindOutboxProjectionProvenance changes only the disposable
// runtime copy's physical-file witness. A byte-for-byte SQLite backup retains
// the source inode witness in state_meta, so the production marker correctly
// becomes non-native when the copy is opened. The real source snapshot was
// already trusted; this test-only rebind preserves that audited capability for
// the copy after checking that all three projection markers/provenance records
// are present and internally consistent. It never runs against the live store.
func dockerRealDataRebindOutboxProjectionProvenance(ctx context.Context, path string) error {
	physicalRevision, err := teamstore.SourceFileIdentity(path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(physicalRevision) == "" {
		return fmt.Errorf("copied SQLite database has no physical identity")
	}
	query := url.Values{}
	query.Set("mode", "rw")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout = 5000`); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var databaseIdentity string
	if err := tx.QueryRowContext(ctx, `SELECT CAST(value AS TEXT) FROM state_meta WHERE key = ?`, "outbox_database_identity").Scan(&databaseIdentity); err != nil {
		return err
	}
	databaseIdentity = strings.TrimSpace(databaseIdentity)
	if databaseIdentity == "" {
		return fmt.Errorf("copied SQLite outbox database identity is empty")
	}
	var generation int64
	if err := tx.QueryRowContext(ctx, `SELECT CAST(value AS INTEGER) FROM state_meta WHERE key = ?`, "outbox_generation").Scan(&generation); err != nil {
		return err
	}
	var schemaVersion int64
	if err := tx.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
		return err
	}
	type provenance struct {
		Version          string `json:"version"`
		DatabaseIdentity string `json:"database_identity"`
		PhysicalRevision string `json:"physical_revision"`
		Generation       int64  `json:"generation"`
		SchemaVersion    *int64 `json:"schema_version"`
	}
	for _, key := range []string{
		"outbox_projection_trust",
		"outbox_session_projection_trust",
		"outbox_turn_projection_trust",
	} {
		var marker string
		if err := tx.QueryRowContext(ctx, `SELECT CAST(value AS TEXT) FROM state_meta WHERE key = ?`, key).Scan(&marker); err != nil {
			return err
		}
		if strings.TrimSpace(marker) != "trusted-v1" {
			return fmt.Errorf("copied SQLite projection marker %q is %q, want trusted-v1", key, marker)
		}
		var raw string
		if err := tx.QueryRowContext(ctx, `SELECT CAST(value AS TEXT) FROM state_meta WHERE key = ?`, strings.Replace(key, "_trust", "_provenance", 1)).Scan(&raw); err != nil {
			return err
		}
		var value provenance
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return fmt.Errorf("decode copied SQLite projection provenance %q: %w", key, err)
		}
		if strings.TrimSpace(value.Version) == "" || strings.TrimSpace(value.DatabaseIdentity) != databaseIdentity || value.Generation > generation || value.SchemaVersion == nil || *value.SchemaVersion != schemaVersion {
			return fmt.Errorf("copied SQLite projection provenance %q is inconsistent: %#v", key, value)
		}
		value.DatabaseIdentity = databaseIdentity
		value.PhysicalRevision = physicalRevision
		value.Generation = generation
		value.SchemaVersion = &schemaVersion
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE state_meta SET value = ? WHERE key = ?`, string(encoded), strings.Replace(key, "_trust", "_provenance", 1))
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("copied SQLite projection provenance %q disappeared", key)
		}
	}
	return tx.Commit()
}

// dockerRealDataRebindPendingOutboxSourceProofs adopts the copied Codex
// transcript inode for pending source-bound outbox rows.  A Docker copy cannot
// retain the production inode identity embedded in the original proof, so the
// test must explicitly rebind that physical witness.  The source-proof
// manifest is the immutable content witness: dockerFixtureSourceFileProof and
// dockerFixtureRebindRangeProof verify every bounded byte range before this
// helper changes only the disposable SQLite JSON.  No production store API is
// used to weaken the sender-side proof or to rewrite the live database.
func dockerRealDataRebindPendingOutboxSourceProofs(t *testing.T, fixtureRoot string, path string) {
	t.Helper()
	if os.Getenv(dockerCodexMountEnv) != "1" {
		return
	}
	if strings.TrimSpace(os.Getenv(dockerSourceProofManifestEnv)) == "" {
		t.Fatal("mounted Docker Codex fixture requires an immutable source-proof manifest")
	}
	query := url.Values{}
	query.Set("mode", "rw")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		t.Fatalf("open copied outbox SQLite for source-proof rebind: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatalf("set copied outbox source-proof rebind busy timeout: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin copied outbox source-proof rebind: %v", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
		SELECT id, json
		FROM outbox_messages
		WHERE status IN (?, ?, ?)
		  AND instr(CAST(json AS TEXT), '"transcript_source_path"') > 0
		ORDER BY id`,
		string(teamstore.OutboxStatusQueued),
		string(teamstore.OutboxStatusSending),
		string(teamstore.OutboxStatusAccepted),
	)
	if err != nil {
		t.Fatalf("read copied pending source-bound outbox rows: %v", err)
	}
	type update struct {
		id  string
		raw []byte
	}
	updates := make([]update, 0)
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			_ = rows.Close()
			t.Fatalf("scan copied pending source-bound outbox row: %v", err)
		}
		var message teamstore.OutboxMessage
		if err := json.Unmarshal(raw, &message); err != nil {
			_ = rows.Close()
			t.Fatalf("decode copied pending source-bound outbox %q: %v", id, err)
		}
		sourcePath := strings.TrimSpace(message.TranscriptSourcePath)
		if sourcePath == "" {
			continue
		}
		copiedPath := dockerFixtureSourcePath(fixtureRoot, sourcePath)
		if copiedPath == "" || copiedPath != sourcePath {
			_ = rows.Close()
			t.Fatalf("pending source-bound outbox %q cannot resolve copied transcript %q", id, sourcePath)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			_ = rows.Close()
			t.Fatalf("decode copied source-bound outbox JSON %q: %v", id, err)
		}
		changed := false
		if strings.TrimSpace(message.TranscriptSourceProofFingerprint) != "" {
			if !message.TranscriptSourceProofOffsetKnown || message.TranscriptSourceProofOffset < 0 {
				_ = rows.Close()
				t.Fatalf("pending source-bound outbox %q has an unusable prefix proof", id)
			}
			_, rebound, _, err := dockerFixtureSourceFileProof(copiedPath, message.TranscriptSourceProofOffset)
			if err != nil {
				_ = rows.Close()
				t.Fatalf("verify copied pending outbox %q prefix proof: %v", id, err)
			}
			encoded, err := json.Marshal(rebound)
			if err != nil {
				_ = rows.Close()
				t.Fatalf("encode copied pending outbox %q prefix proof: %v", id, err)
			}
			object["transcript_source_proof_fingerprint"] = encoded
			changed = true
		}
		if strings.TrimSpace(message.TranscriptSourceReadProofFingerprint) != "" || message.TranscriptSourceReadProofRangeKnown {
			if !message.TranscriptSourceReadProofRangeKnown || message.TranscriptSourceReadProofStartOffset < 0 ||
				message.TranscriptSourceReadProofEndOffset < message.TranscriptSourceReadProofStartOffset ||
				strings.TrimSpace(message.TranscriptSourceReadProofFingerprint) == "" {
				_ = rows.Close()
				t.Fatalf("pending source-bound outbox %q has an unusable read-range proof", id)
			}
			rebound, err := dockerFixtureRebindRangeProof(copiedPath, message.TranscriptSourceReadProofStartOffset, message.TranscriptSourceReadProofEndOffset, message.TranscriptSourceReadProofFingerprint)
			if err != nil {
				_ = rows.Close()
				t.Fatalf("verify copied pending outbox %q read-range proof: %v", id, err)
			}
			encoded, err := json.Marshal(rebound)
			if err != nil {
				_ = rows.Close()
				t.Fatalf("encode copied pending outbox %q read-range proof: %v", id, err)
			}
			object["transcript_source_read_proof_fingerprint"] = encoded
			changed = true
		}
		if !changed {
			continue
		}
		updated, err := json.Marshal(object)
		if err != nil {
			_ = rows.Close()
			t.Fatalf("encode copied pending source-bound outbox %q: %v", id, err)
		}
		updates = append(updates, update{id: id, raw: updated})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate copied pending source-bound outbox rows: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close copied pending source-bound outbox rows: %v", err)
	}
	for _, update := range updates {
		result, err := tx.ExecContext(ctx, `UPDATE outbox_messages SET json = ? WHERE id = ?`, update.raw, update.id)
		if err != nil {
			t.Fatalf("rebind copied pending outbox source proof %q: %v", update.id, err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			t.Fatalf("rebind copied pending outbox source proof %q affected=%d err=%v", update.id, affected, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit copied pending outbox source-proof rebind: %v", err)
	}
	t.Logf("rebound copied pending outbox source proofs: rows=%d", len(updates))
}

// TestDockerRealDataOutboxDrain is an opt-in, no-token experiment for the
// inherited real SQLite outbox. Unlike the full listener replay experiment it
// does not inject synthetic Teams messages or require Codex history files. It
// runs the production main-loop outbox lane against the copied durable rows,
// records only local fake-Graph POSTs, and measures the before/after SQLite
// terminal delta. The fixture is disposable and the Graph server is local.
func TestDockerRealDataOutboxDrain(t *testing.T) {
	if os.Getenv(dockerRealDataOutboxExperimentEnv) != "1" {
		t.Skipf("set %s=1 to run the real-data outbox Docker experiment", dockerRealDataOutboxExperimentEnv)
	}
	fixtureRoot := dockerRealDataOutboxFixtureRoot(t)
	store, statePath := prepareDockerFixtureStore(t, fixtureRoot)
	storePath := filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName)
	dockerRealDataRebindPendingOutboxSourceProofs(t, fixtureRoot, storePath)
	ctx, cancel := context.WithTimeout(context.Background(), dockerRealDataOutboxDuration(t)+2*time.Minute)
	defer cancel()
	setupStarted := time.Now()
	if err := dockerRealDataRebindOutboxProjectionProvenance(ctx, storePath); err != nil {
		t.Fatalf("rebind copied outbox projection provenance: %v", err)
	}
	if err := store.PrepareSQLiteSchemaBeforeOwner(ctx); err != nil {
		t.Fatalf("prepare copied outbox SQLite schema: %v", err)
	}

	before, err := dockerRealDataOutboxCountsFromSQLite(ctx, storePath)
	if err != nil {
		t.Fatalf("read real-data outbox baseline: %v", err)
	}
	if before.Queued+before.Sending+before.Accepted == 0 {
		t.Fatalf("real-data outbox fixture has no pending durable messages: %#v", before)
	}
	chatIDs, err := dockerRealDataPendingOutboxChatIDsFromSQLite(ctx, storePath)
	if err != nil {
		t.Fatalf("read real-data pending outbox chat IDs: %v", err)
	}
	if len(chatIDs) == 0 {
		t.Fatalf("real-data outbox fixture has pending rows but no canonical chat IDs: %#v", before)
	}
	skippedBefore, err := dockerRealDataOutboxSkippedReasonsFromSQLite(ctx, storePath)
	if err != nil {
		t.Fatalf("read real-data skipped baseline: %v", err)
	}

	scope, err := store.ReadScope(ctx)
	if err != nil {
		t.Fatalf("read copied outbox scope: %v", err)
	}
	control, err := store.ReadControlChat(ctx)
	if err != nil {
		t.Fatalf("read copied outbox control binding: %v", err)
	}
	startupState, err := store.PollStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("read copied outbox lease state: %v", err)
	}
	if priorLease := startupState.ControlLease; priorLease.Generation > 0 && strings.TrimSpace(priorLease.HolderMachineID) != "" {
		if _, err := store.ReleaseControlLeaseIfHolder(ctx, priorLease.HolderMachineID, priorLease.Generation); err != nil {
			t.Fatalf("release copied source lease: %v", err)
		}
	}
	machine := teamstore.MachineRecord{
		ID:            "docker-real-data-outbox-machine",
		ScopeID:       scope.ID,
		AccountID:     scope.AccountID,
		UserPrincipal: scope.UserPrincipal,
		Profile:       scope.Profile,
		Kind:          teamstore.MachineKindEphemeral,
		Status:        teamstore.MachineStatusActive,
	}
	owner, err := teamstore.CurrentOwner("docker-real-data-outbox", "", "", time.Now().UTC())
	if err != nil {
		t.Fatalf("create copied outbox owner: %v", err)
	}
	owner.ScopeID = scope.ID
	owner.MachineID = machine.ID
	decision, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
		Scope: scope, Machine: machine, Owner: owner, Duration: 10 * time.Minute, Now: time.Now().UTC(),
	})
	if err != nil || decision.Mode != teamstore.LeaseModeActive {
		t.Fatalf("claim copied outbox owner: mode=%v err=%v", decision.Mode, err)
	}
	owner.LeaseGeneration = decision.Lease.Generation
	if err := store.PrepareOutboxProjectionForOwner(ctx, owner); err != nil {
		t.Fatalf("prepare copied outbox projection for owner: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := store.ReleaseControlLeaseIfHolder(cleanupCtx, machine.ID, decision.Lease.Generation); err != nil {
			t.Errorf("release copied outbox owner: %v", err)
		}
	})
	t.Logf("real-data outbox setup: elapsed=%s", time.Since(setupStarted))
	registryPath := filepath.Join(filepath.Dir(statePath), "registry.json")
	registry, err := LoadRegistry(registryPath)
	if err != nil {
		t.Fatalf("load copied outbox registry: %v", err)
	}
	user := User{
		ID:                scope.AccountID,
		UserPrincipalName: scope.UserPrincipal,
		DisplayName:       firstNonEmptyString(scope.UserPrincipal, "Docker real-data user"),
	}
	serverState := newDockerRealDataGraphServer("docker-real-data-outbox-token", user, nil)
	serverState.controlChatID = control.TeamsChatID
	// This lane is measuring normal known-outcome delivery. Reserve the fake's
	// deliberate connection-drop fault for the separate unknown-POST experiment;
	// an empty marker intentionally makes the first POST ambiguous.
	serverState.setUnknownPostMarker("__docker_real_data_outbox_unknown_post_disabled__")
	serverState.setKnownChats(chatIDs...)
	server := httptest.NewServer(serverState)
	defer server.Close()
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "docker-real-data-outbox-token"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(delay time.Duration) time.Duration { return delay },
	}
	if strings.TrimSpace(user.ID) == "" {
		user.ID = "docker-real-data-user"
	}
	trace := &dockerRealDataTraceWriter{}
	store.SetTimingObserver(trace.recordStoreTiming)
	t.Cleanup(func() { store.SetTimingObserver(nil) })
	bridge := &Bridge{
		graph:            graph,
		readGraph:        graph,
		registryPath:     registryPath,
		reg:              registry,
		user:             user,
		scope:            scope,
		machine:          machine,
		out:              io.Discard,
		store:            store,
		pollWorkerBudget: mainLoopPollWorkerBudget,
	}
	bridge.setControlLease(decision.Lease)
	bridge.outboxPhaseTraceHook = func(name string, duration time.Duration, phaseErr error) {
		trace.recordTiming("outbox.phase."+strings.TrimSpace(name), duration, phaseErr)
	}
	bridge.outboxSendTraceHook = func(outboxID, stage string, duration time.Duration, phaseErr error) {
		trace.recordOutboxSendStage(outboxID, stage, duration, phaseErr)
	}

	duration := dockerRealDataOutboxDuration(t)
	started := time.Now()
	cycles := 0
	var firstErr error
	for time.Since(started) < duration {
		cycles++
		cycleCtx, cycleCancel := context.WithTimeout(ctx, 15*time.Second)
		cycleCtx = withTeamsOwnerCapability(cycleCtx, owner)
		err := bridge.flushPendingOutboxMainLoop(cycleCtx)
		cycleCancel()
		if err != nil && firstErr == nil && !isOutboxDeliveryDeferred(err) && !isGraphRateLimitError(err) {
			firstErr = err
		}
		if time.Since(started) >= duration {
			break
		}
	}
	elapsed := time.Since(started)
	after, err := dockerRealDataOutboxCountsFromSQLite(ctx, storePath)
	if err != nil {
		t.Fatalf("read real-data outbox final counters: %v", err)
	}
	skippedAfter, err := dockerRealDataOutboxSkippedReasonsFromSQLite(ctx, storePath)
	if err != nil {
		t.Fatalf("read real-data skipped final rows: %v", err)
	}
	newSkipped := make([]string, 0)
	for id, reason := range skippedAfter {
		if _, existed := skippedBefore[id]; existed {
			continue
		}
		newSkipped = append(newSkipped, id+"="+reason)
	}
	sort.Strings(newSkipped)
	if len(newSkipped) > 16 {
		newSkipped = append(newSkipped[:8], newSkipped[len(newSkipped)-8:]...)
	}
	sentDelta := after.Sent - before.Sent
	skippedDelta := after.Skipped - before.Skipped
	terminalDelta := sentDelta + skippedDelta
	postAttempts := serverState.messagePostAttempts.Load()
	postAccepts := serverState.messagePostAccepts.Load()
	duplicateKeys := 0
	serverState.mu.Lock()
	for _, attempts := range serverState.postAttempts {
		if attempts > 1 {
			duplicateKeys++
		}
	}
	serverState.mu.Unlock()
	t.Logf("real-data outbox drain: window=%s cycles=%d pending_chats=%d before=%#v after=%#v durable_sent_delta=%d durable_skipped_delta=%d durable_terminal_delta=%d sent_per_sec=%.3f graph_post_attempts=%d graph_post_accepts=%d duplicate_post_keys=%d graph_429=%d graph_requests=%d", elapsed, cycles, len(chatIDs), before, after, sentDelta, skippedDelta, terminalDelta, float64(sentDelta)/elapsed.Seconds(), postAttempts, postAccepts, duplicateKeys, serverState.status429.Load(), len(serverState.graphRequestsSnapshot()))
	t.Logf("real-data outbox phase timings: %s", trace.timingAggregatesSummaryFor("outbox.phase."))
	t.Logf("real-data outbox graph request timings: %s", dockerRealDataOutboxGraphRequestTimingSummary(serverState.graphRequestsSnapshot()))
	t.Logf("real-data outbox send timings: %s", trace.timingAggregatesSummaryFor("outbox.send."))
	t.Logf("real-data outbox store timings: %s", trace.timingAggregatesSummaryFor("store."))
	if len(newSkipped) > 0 {
		t.Logf("real-data outbox newly skipped rows: count=%d sample=%v", after.Skipped-before.Skipped, newSkipped)
	}
	if firstErr != nil {
		t.Fatalf("real-data outbox drain encountered unexpected error: %v", firstErr)
	}
	if postAttempts != postAccepts {
		t.Fatalf("fake Graph outbox POST attempts=%d accepts=%d, want every normal POST response accepted", postAttempts, postAccepts)
	}
	if duplicateKeys != 0 {
		t.Fatalf("fake Graph observed duplicate POST payload keys: %d", duplicateKeys)
	}
	if sentDelta <= 0 {
		t.Fatalf("real-data outbox drain made no durable Sent progress: before=%#v after=%#v", before, after)
	}
	if sentDelta != postAccepts {
		t.Fatalf("durable Sent delta=%d differs from fake Graph accepted POSTs=%d", sentDelta, postAccepts)
	}
	if terminalDelta <= 0 {
		t.Fatalf("real-data outbox drain made no durable terminal progress: before=%#v after=%#v", before, after)
	}
}
