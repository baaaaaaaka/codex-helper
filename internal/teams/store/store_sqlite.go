package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	storeSQLiteBackend              = "store-sqlite"
	storeSQLiteVersion              = 1
	storeSQLiteFileName             = "store.sqlite"
	storeSQLitePointerSchemaVersion = SchemaVersion + 1
	// Pointer schema 6 was emitted while the JSON state schema was 5. Pointer
	// schemas 6 through 10 remain readable for migration, but the current
	// pointer schema (11) is an upgrade-only boundary because schema-9 helpers
	// cannot preserve the post-send replay marker and other durable safety data.
	storeSQLiteMinPointerSchemaVersion = 6
	sqliteImportCheckpointImporting    = "importing"
	sqliteWALAutocheckpointPages       = 0

	DefaultSQLiteStateMigrationMinSize int64 = 1 << 20
)

// History-watch is kept in the existing cold state document for backwards
// compatibility.  This projection is an additional state_meta value, not a
// schema/table change, and lets the high-frequency watcher update only its own
// JSON without repeatedly rewriting the large cold document.
const (
	sqliteHistoryWatchProjectionKey = "history_watch_projection"
	// stateJSONRevisionKey is incremented by a durable SQLite trigger whenever
	// any writer (including an older helper) replaces state_json.  The history
	// watch projection records the revision it was based on, so a mixed-version
	// full-state write cannot silently get overwritten by a stale projection.
	sqliteStateJSONRevisionKey = "state_json_revision"
)

// Keep the listener's durable admission bounded. The bridge applies its own
// smaller work-quantum limit after decoding these rows; this headroom lets the
// SQL ordering survive a few rows that are later filtered as control/closed
// sessions without materializing the entire store.
const (
	sqliteHotPollReadyLimit = 64
	// A malformed poll is a chat-local recovery candidate. Reserve one slot
	// for it, but never let an unbounded collection of corrupt rows fill the
	// admission quantum and starve healthy chats.
	sqliteHotPollMalformedLimit = 1
	// Echo recovery is an exceptional path used only when a provider accepted
	// a rendered message before its local receipt was persisted. Page through a
	// bounded number of rows so one malformed canonical row cannot hide the next
	// usable candidate, while a pathological pile of opaque rows cannot turn
	// this lookup into an unbounded scan.
	sqliteOutboxEchoMaxPages = 8
)

// Keep a small ordinary lane in every hot-poll admission quantum.  A large
// number of due continuation/gap rows must not be able to hide a newly active
// chat forever.  The two lanes are deliberately derived from the same total
// limit used by both the schedule and session-candidate queries.
const (
	sqliteHotPollOperationalShare = 3
	sqliteHotPollOrdinaryShare    = 1
)

func sqliteHotPollLaneLimits(limit int) (operational, ordinary int) {
	if limit <= 0 {
		limit = sqliteHotPollReadyLimit
	}
	if limit == 1 {
		return 0, 1
	}
	ordinary = limit * sqliteHotPollOrdinaryShare / (sqliteHotPollOperationalShare + sqliteHotPollOrdinaryShare)
	if ordinary < 1 {
		ordinary = 1
	}
	// The operational CTE may use the full capacity.  The final admission
	// LIMIT trims it after the ordinary lane has been placed ahead of it; when
	// no ordinary row exists this preserves the historical full operational
	// batch size.
	return limit, ordinary
}

// sqliteChatPollOperationalFrontierSQL mirrors chatPollHasOperationalFrontier
// for the admission query. The materialized frontier_active column is an
// index hint, not authority: an older helper or a partial repair can leave it
// stale while the JSON row is still the durable source of truth. The caller
// must pair this expression with json_valid(column).
func sqliteChatPollOperationalFrontierSQL(column string) string {
	// Keep every JSON1 call behind json_valid. SQLite is free to reorder AND
	// terms, so a separate `json_valid(...) AND json_type(...)` predicate is not
	// a sufficient guard for a syntax-corrupt per-chat row.
	return `(CASE WHEN json_valid(` + column + `) THEN COALESCE((
    json_type(` + column + `, '$.pending_page') IN ('object', 'array')
    OR (json_type(` + column + `, '$.continuation_path') = 'text' AND COALESCE(json_extract(` + column + `, '$.continuation_path'), '') <> '')
    OR (json_type(` + column + `, '$.deferred_continuation_path') = 'text' AND COALESCE(json_extract(` + column + `, '$.deferred_continuation_path'), '') <> '')
    OR json_type(` + column + `, '$.gap') IN ('object', 'array')
  ), 0) ELSE 0 END)`
}

func sqliteChatPollValidJSONSQL(jsonColumn, chatIDColumn string) string {
	return `json_valid(` + jsonColumn + `)
  AND (CASE WHEN json_valid(` + jsonColumn + `) THEN json_type(` + jsonColumn + `, '$.chat_id') END) = 'text'
  AND (CASE WHEN json_valid(` + jsonColumn + `) THEN json_extract(` + jsonColumn + `, '$.chat_id') END) = ` + chatIDColumn
}

// sqliteChatPollAdmissionValidJSONSQL is the stricter form used only by the
// bounded hot-poll admission queries.  A syntactically valid JSON object can
// still make encoding/json reject the whole poll row (for example
// {"next_poll_at":17} or {"attempt":[]}).  If many such rows pass the SQL
// LIMIT and are discarded after decoding, they can starve healthy chats.  Keep
// those rows in the reserved malformed lane instead.
//
// The checks intentionally cover every field whose Go decoder can fail at the
// top level, plus the typed fields in the three nested recovery envelopes.
// Missing and explicit null values are accepted for optional/omitempty
// fields, matching encoding/json's compatibility behavior for old rows.
func sqliteChatPollAdmissionValidJSONSQL(jsonColumn, chatIDColumn string) string {
	base := sqliteChatPollValidJSONSQL(jsonColumn, chatIDColumn)
	typeAllowed := func(path string, allowed ...string) string {
		typeExpr := sqliteSafeJSONType(jsonColumn, path)
		parts := []string{typeExpr + " IS NULL", typeExpr + " = 'null'"}
		for _, typ := range allowed {
			parts = append(parts, typeExpr+" = '"+typ+"'")
		}
		return "(" + strings.Join(parts, " OR ") + ")"
	}
	arrayElementsAllowed := func(path, elementType string) string {
		typeExpr := sqliteSafeJSONType(jsonColumn, path)
		// json_each is fed a safe JSON value so SQLite cannot evaluate it on a
		// truncated row while reordering WHERE terms.
		safeJSON := "CASE WHEN json_valid(" + jsonColumn + ") THEN " + jsonColumn + " ELSE 'null' END"
		return "(" + typeExpr + " IS NULL OR " + typeExpr + " = 'null' OR (" + typeExpr + " = 'array' AND NOT EXISTS (SELECT 1 FROM json_each(" + safeJSON + ", '" + path + "') WHERE type <> '" + elementType + "')) )"
	}
	parts := []string{base}
	for _, path := range []string{
		"$.recovery_reason", "$.recovery_source_hash", "$.state", "$.previous_state",
		"$.continuation_path", "$.deferred_continuation_path", "$.last_error",
		"$.last_window_full_message", "$.continuation_last_path",
	} {
		parts = append(parts, typeAllowed(path, "text"))
	}
	for _, path := range []string{
		"$.next_poll_at", "$.last_activity_at", "$.blocked_until", "$.parked_at",
		"$.park_notice_sent_at", "$.last_modified_cursor", "$.continuation_safe_cursor",
		"$.last_successful_poll_at", "$.last_error_at", "$.last_window_full_at",
		"$.continuation_first_failure_at", "$.continuation_last_failure_at", "$.updated_at",
	} {
		parts = append(parts, typeAllowed(path, "text"))
	}
	for _, path := range []string{
		"$.seeded", "$.recovery_required", "$.head_probe_pending",
	} {
		parts = append(parts, typeAllowed(path, "true", "false"))
	}
	for _, path := range []string{
		"$.failure_count", "$.continuation_failure_count", "$.continuation_no_progress_count",
		"$.continuation_page_count", "$.poll_revision", "$.schedule_revision", "$.frontier_epoch",
	} {
		parts = append(parts, typeAllowed(path, "integer"))
	}
	for _, path := range []string{"$.pending_page", "$.gap", "$.attempt"} {
		parts = append(parts, typeAllowed(path, "object"))
	}
	for _, path := range []string{"$.continuation_path_history", "$.continuation_page_fingerprint_history", "$.quarantined_record_ids"} {
		parts = append(parts, arrayElementsAllowed(path, "text"))
	}
	// The nested envelopes are optional pointers.  Validate their scalar
	// members so a row with a malformed pending/attempt/gap object is isolated
	// before it consumes an operational or ordinary slot.
	for _, path := range []string{
		"$.pending_page.receipt_id", "$.pending_page.chat_id", "$.pending_page.request_path",
		"$.pending_page.request_fingerprint", "$.pending_page.frontier", "$.pending_page.next_path", "$.pending_page.boundary_reason",
		"$.gap.kind", "$.gap.reason", "$.gap.evidence", "$.gap.frontier_path", "$.gap.recovery_path",
		"$.attempt.id", "$.attempt.owner", "$.attempt.process_incarnation", "$.attempt.expected_frontier",
		"$.attempt.expected_receipt_id",
	} {
		parts = append(parts, typeAllowed(path, "text"))
	}
	for _, path := range []string{
		"$.pending_page.frontier_epoch", "$.gap.epoch", "$.gap.notice_epoch",
		"$.attempt.expected_poll_revision", "$.attempt.expected_schedule_revision",
	} {
		parts = append(parts, typeAllowed(path, "integer"))
	}
	for _, path := range []string{"$.pending_page.baseline_only", "$.gap.head_probe_pending"} {
		parts = append(parts, typeAllowed(path, "true", "false"))
	}
	for _, path := range []string{
		"$.pending_page.received_at", "$.gap.safe_cursor", "$.gap.recovery_cursor", "$.gap.opened_at",
		"$.gap.last_progress_at", "$.attempt.started_at", "$.attempt.expires_at",
	} {
		parts = append(parts, typeAllowed(path, "text"))
	}
	for _, path := range []string{"$.pending_page.record_ids", "$.pending_page.record_hashes", "$.pending_page.dispositions"} {
		parts = append(parts, arrayElementsAllowed(path, "text"))
	}
	parts = append(parts, arrayElementsAllowed("$.pending_page.refetch_failures", "integer"))
	return strings.Join(parts, "\n  AND ")
}

func sqliteSessionValidJSONSQL(jsonColumn, idColumn, chatIDColumn string) string {
	return `json_valid(` + jsonColumn + `)
  AND (CASE WHEN json_valid(` + jsonColumn + `) THEN json_type(` + jsonColumn + `, '$.id') END) = 'text'
  AND (CASE WHEN json_valid(` + jsonColumn + `) THEN json_extract(` + jsonColumn + `, '$.id') END) = ` + idColumn + `
  AND (CASE WHEN json_valid(` + jsonColumn + `) THEN json_type(` + jsonColumn + `, '$.teams_chat_id') END) = 'text'
  AND (CASE WHEN json_valid(` + jsonColumn + `) THEN json_extract(` + jsonColumn + `, '$.teams_chat_id') END) = ` + chatIDColumn + `
  AND ((CASE WHEN json_valid(` + jsonColumn + `) THEN json_type(` + jsonColumn + `, '$.status') END) IS NULL OR
       (CASE WHEN json_valid(` + jsonColumn + `) THEN json_type(` + jsonColumn + `, '$.status') END) IN ('null', 'text'))`
}

// sqliteSessionActiveJSONSQL treats the canonical JSON session status as the
// source of truth when it is present, while retaining the indexed SQL status
// for old rows that omitted status. This prevents a stale/partial SQL
// projection from hiding an otherwise active chat after a mixed-version write.
func sqliteSessionActiveJSONSQL(jsonColumn, sqlStatusColumn string) string {
	statusType := sqliteSafeJSONType(jsonColumn, "$.status")
	status := sqliteSafeJSONExtract(jsonColumn, "$.status")
	return "(((" + statusType + " IN ('null') OR " + statusType + " IS NULL) AND (" + sqlStatusColumn + " IS NULL OR " + sqlStatusColumn + " = '' OR " + sqlStatusColumn + " = '" + string(SessionStatusActive) + "'))" +
		" OR (" + statusType + " = 'text' AND (trim(COALESCE(" + status + ", '')) = '' OR trim(" + status + ") = '" + string(SessionStatusActive) + "')))"
}

// sqliteSafeJSONExtract must be used for JSON1 expressions over a persisted
// row that may have been truncated. SQLite may reorder ordinary AND terms, so
// `json_valid(column) AND json_extract(...)` is not a reliable error barrier;
// CASE is. The returned NULL makes malformed rows ineligible for the local
// query while leaving their raw bytes available to the opaque-row quarantine.
func sqliteSafeJSONExtract(column, path string) string {
	return "(CASE WHEN json_valid(" + column + ") THEN json_extract(" + column + ", '" + path + "') ELSE NULL END)"
}

func sqliteSafeJSONType(column, path string) string {
	return "(CASE WHEN json_valid(" + column + ") THEN json_type(" + column + ", '" + path + "') ELSE NULL END)"
}

// sqliteOutboxProjectionValidSQL is the admission boundary for an outbox row.
// SQLite keeps a small set of indexed columns beside the canonical JSON.  A
// partial/manual write can leave those two representations contradictory even
// when the JSON is syntactically valid.  Never decode such a row as runnable
// work: filtering it before LIMIT keeps one damaged row from hiding healthy
// rows, while the raw row remains available to the opaque-row diagnostics.
//
// Optional JSON fields are absent in older rows, so their absent/null forms
// match the zero-valued compatibility columns.  Fields with a non-zero value
// must have the type that encoding/json expects; otherwise a type error would
// otherwise abort an entire pending page.
func sqliteOutboxProjectionValidSQL(alias string) string {
	jsonColumn := alias + ".json"
	id := sqliteSafeJSONExtract(jsonColumn, "$.id")
	chatID := sqliteSafeJSONExtract(jsonColumn, "$.teams_chat_id")
	messageID := sqliteSafeJSONExtract(jsonColumn, "$.teams_message_id")
	status := sqliteSafeJSONExtract(jsonColumn, "$.status")
	sequence := sqliteSafeJSONExtract(jsonColumn, "$.sequence")
	pending := sqliteSafeJSONExtract(jsonColumn, "$.post_send_effects_pending")
	optionalText := func(path, column string) string {
		typeExpr := sqliteSafeJSONType(jsonColumn, path)
		valueExpr := sqliteSafeJSONExtract(jsonColumn, path)
		// A NULL indexed column is an unknown value on an older SQLite schema
		// (or on a row written before that projection existed), not evidence of
		// an identity mismatch. The JSON payload remains canonical in that case.
		return "(" + column + " IS NULL OR ((" + typeExpr + " IS NULL OR " + typeExpr + " = 'text') AND trim(COALESCE(" + valueExpr + ", '')) = trim(COALESCE(" + column + ", ''))))"
	}
	optionalTime := func(path string) string {
		typeExpr := sqliteSafeJSONType(jsonColumn, path)
		return "(" + typeExpr + " IS NULL OR " + typeExpr + " = 'text')"
	}
	return "json_valid(" + jsonColumn + ")" +
		" AND " + sqliteSafeJSONType(jsonColumn, "$.id") + " = 'text'" +
		" AND trim(" + id + ") = trim(" + alias + ".id)" +
		" AND " + sqliteSafeJSONType(jsonColumn, "$.teams_chat_id") + " = 'text'" +
		" AND trim(" + chatID + ") = trim(COALESCE(" + alias + ".teams_chat_id, ''))" +
		" AND (" + sqliteSafeJSONType(jsonColumn, "$.teams_message_id") + " IS NULL OR " + sqliteSafeJSONType(jsonColumn, "$.teams_message_id") + " = 'text')" +
		" AND (" + alias + ".teams_message_id IS NULL OR trim(COALESCE(" + messageID + ", '')) = trim(COALESCE(" + alias + ".teams_message_id, '')))" +
		" AND " + sqliteSafeJSONType(jsonColumn, "$.status") + " = 'text'" +
		" AND " + status + " = COALESCE(" + alias + ".status, '')" +
		" AND (" + sqliteSafeJSONType(jsonColumn, "$.sequence") + " IS NULL OR " + sqliteSafeJSONType(jsonColumn, "$.sequence") + " = 'integer')" +
		" AND COALESCE(" + sequence + ", 0) = COALESCE(" + alias + ".sequence, 0)" +
		" AND " + optionalText("$.session_id", alias+".session_id") +
		" AND " + optionalText("$.turn_id", alias+".turn_id") +
		" AND (" + sqliteSafeJSONType(jsonColumn, "$.post_send_effects_pending") + " IS NULL OR " + sqliteSafeJSONType(jsonColumn, "$.post_send_effects_pending") + " IN ('true', 'false'))" +
		" AND COALESCE(" + pending + ", 0) = COALESCE(" + alias + ".post_send_effects_pending, 0)" +
		" AND " + optionalTime("$.created_at") +
		" AND " + optionalTime("$.updated_at") +
		" AND " + optionalTime("$.sent_at") +
		" AND " + optionalTime("$.last_send_attempt") +
		" AND " + optionalTime("$.next_attempt_at")
}

type sqliteHistoryWatchProjection struct {
	HistoryWatch      map[string]HistoryWatchCheckpoint `json:"history_watch,omitempty"`
	HistoryWatchReady time.Time                         `json:"history_watch_ready,omitempty"`
	StateJSONRevision int64                             `json:"state_json_revision,omitempty"`
}

const SQLiteFileName = storeSQLiteFileName

const (
	sqliteMigrationStageAfterBackup       = "after-backup"
	sqliteMigrationStageAfterTempVerified = "after-temp-verified"
	sqliteMigrationStageAfterDBReplace    = "after-db-replace"
)

// sqliteMigrationTestHook is nil in production; tests use it to inject failures
// at durable migration boundaries.
var sqliteMigrationTestHook func(stage string) error

// sqliteRuntimeMetadataConnectionTestHook is nil in production. It lets tests
// verify that one metadata attempt acquires exactly one physical connection.
var sqliteRuntimeMetadataConnectionTestHook func()

// IsSQLiteBusyError reports both SQLITE_BUSY and its extended variants (for
// example SQLITE_BUSY_SNAPSHOT). A deferred SQLite transaction can observe a
// stale WAL snapshot and fail immediately while upgrading a read to a write;
// callers that own a liveness loop should retry the whole short transaction
// from a fresh snapshot instead of terminating the service.
func IsSQLiteBusyError(err error) bool {
	return sqliteErrorHasCode(err, sqlite3.SQLITE_BUSY)
}

// IsSQLiteProcessWideError reports SQLite failures that make the store unsafe
// to treat as a chat-local error.  In particular, FULL/READONLY/IOERR and
// corruption errors mean that a phase must stop making further durable
// mutations until the store becomes writable/healthy again.  The bridge uses
// this at phase boundaries; it deliberately does not classify ordinary query
// or decode errors as process-wide so one bad row can remain isolated.
func IsSQLiteProcessWideError(err error) bool {
	for _, code := range []int{
		sqlite3.SQLITE_BUSY,
		sqlite3.SQLITE_FULL,
		sqlite3.SQLITE_READONLY,
		sqlite3.SQLITE_IOERR,
		sqlite3.SQLITE_CORRUPT,
		sqlite3.SQLITE_NOTADB,
	} {
		if sqliteErrorHasCode(err, code) {
			return true
		}
	}
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"database or disk is full",
		"disk is full",
		"no space left on device",
		"disk quota exceeded",
		"readonly database",
		"read-only database",
		"read-only file system",
		"database disk image is malformed",
		"input/output error",
		"i/o error",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func sqliteErrorHasCode(err error, code int) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	return sqliteErr.Code()&0xff == code
}

// SQLite defaults to 999 host parameters. Keep headroom for fixed query
// arguments (for example a status predicate) and avoid making a large linked
// transcript poll fail just because many sessions are registered.
const sqliteQueryParameterBatchSize = 400

type storeSQLitePointer struct {
	SchemaVersion       int       `json:"schema_version"`
	StorageBackend      string    `json:"storage_backend"`
	StorageVersion      int       `json:"storage_version"`
	Path                string    `json:"path"`
	MigrationID         string    `json:"migration_id,omitempty"`
	SourceSchemaVersion int       `json:"source_schema_version,omitempty"`
	SourceSHA256        string    `json:"source_sha256,omitempty"`
	CreatedAt           time.Time `json:"created_at,omitempty"`
}

type StoreSQLiteMigrationResult struct {
	Path        string
	MigrationID string
	AlreadyDB   bool
	Migrated    bool
	State       State
}

func (s *Store) MigrateLargeStateToSQLite(ctx context.Context, minSourceSize int64) (StoreSQLiteMigrationResult, error) {
	if minSourceSize < 0 {
		minSourceSize = 0
	}
	var out StoreSQLiteMigrationResult
	err := s.withStateLock(ctx, func() error {
		// A runtime heartbeat may use the shared SQLite handle without the
		// business state-file lock. Quiesce and close it before a migration can
		// replace the SQLite inode or rewrite the pointer; the next liveness
		// operation will reopen the validated current database.
		if err := lockMutexContext(ctx, &s.sqliteRuntimeMu); err != nil {
			return err
		}
		defer s.sqliteRuntimeMu.Unlock()
		if err := s.closeSQLiteDBLocked(); err != nil {
			return err
		}
		source, err := os.ReadFile(s.path)
		if errors.Is(err, os.ErrNotExist) {
			out.State = newState()
			return nil
		}
		if err != nil {
			return err
		}
		if pointer, ok, err := storeSQLitePointerFromData(source); err != nil {
			return err
		} else if ok {
			state, err := s.loadSQLiteStateWithSQLiteLock(ctx, pointer)
			if err != nil {
				return err
			}
			// A pointer emitted by the previous helper is readable for the
			// migration step, but it must not remain writable under the old
			// format.  Publish the current pointer schema before returning the
			// already-migrated database so an older helper rejects the store
			// instead of overwriting safety fields it does not understand.
			if pointer.SchemaVersion < storeSQLitePointerSchemaVersion {
				if err := s.writeSQLitePointerUnlocked(pointer); err != nil {
					return err
				}
				pointer.SchemaVersion = storeSQLitePointerSchemaVersion
			}
			dbPath, err := s.storeSQLitePath(pointer)
			if err != nil {
				return err
			}
			out = StoreSQLiteMigrationResult{Path: dbPath, MigrationID: pointer.MigrationID, AlreadyDB: true, State: state}
			return nil
		}
		if minSourceSize > 0 && int64(len(source)) < minSourceSize {
			return nil
		}
		sourceSchemaVersion := SchemaVersion
		if parsed, ok := stateSchemaVersionFromData(source); ok {
			sourceSchemaVersion = parsed
		}
		state, err := s.loadUnlocked(ctx)
		if err != nil {
			return err
		}
		sum := sha256Bytes(source)
		migrationID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
		pointer := storeSQLitePointer{
			SchemaVersion:       storeSQLitePointerSchemaVersion,
			StorageBackend:      storeSQLiteBackend,
			StorageVersion:      storeSQLiteVersion,
			Path:                storeSQLiteFileName,
			MigrationID:         migrationID,
			SourceSchemaVersion: sourceSchemaVersion,
			SourceSHA256:        sum,
			CreatedAt:           time.Now().UTC(),
		}
		dbPath, err := s.storeSQLitePath(pointer)
		if err != nil {
			return err
		}
		tmpPath := dbPath + ".tmp." + migrationID
		backup := s.path + ".bak.sqlite." + migrationID
		if err := os.MkdirAll(filepath.Dir(dbPath), dirMode); err != nil {
			return err
		}
		if err := atomicWriteFile(backup, source, fileMode); err != nil {
			return err
		}
		if err := runSQLiteMigrationTestHook(sqliteMigrationStageAfterBackup); err != nil {
			return err
		}
		_ = os.Remove(tmpPath)
		if err := s.writeSQLiteStateFile(tmpPath, state); err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
		got, err := loadSQLiteStateFile(tmpPath)
		if err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
		if !sqliteMigrationStateLogicalEqual(state, got) {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("sqlite migration verification failed: %s", sqliteStateSummaryDiff(state, got))
		}
		if err := runSQLiteMigrationTestHook(sqliteMigrationStageAfterTempVerified); err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
		if err := removeSQLiteSidecarFiles(dbPath); err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
		if err := durableReplaceFile(tmpPath, dbPath); err != nil {
			_ = os.Remove(tmpPath)
			return err
		}
		if err := runSQLiteMigrationTestHook(sqliteMigrationStageAfterDBReplace); err != nil {
			return err
		}
		if err := removeSQLiteSidecarFiles(tmpPath); err != nil {
			return err
		}
		_ = os.Chmod(dbPath, fileMode)
		if _, err := loadSQLiteStateFile(dbPath); err != nil {
			return err
		}
		if err := s.writeSQLitePointerUnlocked(pointer); err != nil {
			return err
		}
		out = StoreSQLiteMigrationResult{Path: dbPath, MigrationID: migrationID, Migrated: true, State: state}
		return nil
	})
	return out, err
}

func runSQLiteMigrationTestHook(stage string) error {
	if sqliteMigrationTestHook == nil {
		return nil
	}
	return sqliteMigrationTestHook(stage)
}

func sqliteStateSummaryDiff(left State, right State) string {
	summary := fmt.Sprintf("sessions %d/%d turns %d/%d inbound %d/%d outbox %d/%d provenance %d/%d polls %d/%d rates %d/%d",
		len(left.Sessions), len(right.Sessions),
		len(left.Turns), len(right.Turns),
		len(left.InboundEvents), len(right.InboundEvents),
		len(left.OutboxMessages), len(right.OutboxMessages),
		len(left.MessageProvenance), len(right.MessageProvenance),
		len(left.ChatPolls), len(right.ChatPolls),
		len(left.ChatRateLimits), len(right.ChatRateLimits)) +
		fmt.Sprintf(" helper %d/%d deliveries %d/%d ledger %d/%d checkpoints %d/%d",
			len(left.HelperDeliveries), len(right.HelperDeliveries),
			len(left.TranscriptDeliveries), len(right.TranscriptDeliveries),
			len(left.TranscriptLedger), len(right.TranscriptLedger),
			len(left.ImportCheckpoints), len(right.ImportCheckpoints))
	ldata, lerr := json.Marshal(left)
	rdata, rerr := json.Marshal(right)
	if lerr != nil || rerr != nil {
		return summary
	}
	limit := len(ldata)
	if len(rdata) < limit {
		limit = len(rdata)
	}
	for i := 0; i < limit; i++ {
		if ldata[i] == rdata[i] {
			continue
		}
		start := i - 80
		if start < 0 {
			start = 0
		}
		end := i + 160
		if end > limit {
			end = limit
		}
		return fmt.Sprintf("%s first_diff=%d left=%q right=%q", summary, i, string(ldata[start:end]), string(rdata[start:end]))
	}
	return fmt.Sprintf("%s json_len %d/%d", summary, len(ldata), len(rdata))
}

func stateLogicalEqual(left State, right State) bool {
	normalizeLoadedState(&left)
	normalizeLoadedState(&right)
	ldata, lerr := json.Marshal(left)
	rdata, rerr := json.Marshal(right)
	if lerr != nil || rerr != nil {
		return false
	}
	return string(ldata) == string(rdata)
}

// sqliteMigrationStateLogicalEqual compares the state projection that SQLite
// can safely expose after migration.  A legacy JSON state can contain a
// checkpoint whose canonical key and session owner disagree.  The source
// row is still copied to SQLite (and remains available for an explicit
// repair), but the normal SQLite readers intentionally omit that untrusted
// row instead of returning it as a typed checkpoint.  Treating that expected
// row-local omission as a migration failure would make the safety isolation
// incompatible with otherwise valid legacy stores.
func sqliteMigrationStateLogicalEqual(left State, right State) bool {
	left = sqliteMigrationComparableState(left)
	return stateLogicalEqual(left, right)
}

func sqliteMigrationComparableState(state State) State {
	normalizeLoadedState(&state)
	knownSessionIDs := sqliteKnownSessionIDs(state.Sessions)
	checkpoints := make(map[string]ImportCheckpoint, len(state.ImportCheckpoints))
	for key, checkpoint := range state.ImportCheckpoints {
		key = strings.TrimSpace(key)
		if !sqliteMigrationCheckpointRepresentable(key, checkpoint, knownSessionIDs) {
			continue
		}
		// ImportCheckpoint.UnmarshalJSON applies this marker as well.  Keep the
		// projection stable for callers that construct a State directly in a
		// migration test or future migration entry point.
		checkpoint.RecoveryProofUnusable = checkpoint.RecoveryProofUnusable || !importCheckpointOptionalProofUsable(checkpoint)
		checkpoints[key] = checkpoint
	}
	state.ImportCheckpoints = checkpoints
	return state
}

func sqliteKnownSessionIDs(sessions map[string]SessionContext) map[string]struct{} {
	known := make(map[string]struct{}, len(sessions))
	for id, session := range sessions {
		id = strings.TrimSpace(id)
		if id == "" {
			id = strings.TrimSpace(session.ID)
		}
		if id != "" {
			known[id] = struct{}{}
		}
	}
	return known
}

func sqliteMigrationCheckpointRepresentable(key string, checkpoint ImportCheckpoint, knownSessionIDs map[string]struct{}) bool {
	key = strings.TrimSpace(key)
	if key == "" || strings.TrimSpace(checkpoint.ID) != key || strings.TrimSpace(checkpoint.SessionID) == "" {
		return false
	}
	if canonicalSessionID, canonical := canonicalCheckpointSessionID(key); canonical {
		if _, known := knownSessionIDs[canonicalSessionID]; known && canonicalSessionID != strings.TrimSpace(checkpoint.SessionID) {
			return false
		}
	}
	return true
}

func sha256Bytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func storeSQLitePointerFromData(data []byte) (storeSQLitePointer, bool, error) {
	if len(data) > maxStatePointerSize {
		return storeSQLitePointer{}, false, nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return storeSQLitePointer{}, false, nil
	}
	backendRaw, ok := raw["storage_backend"]
	if !ok {
		return storeSQLitePointer{}, false, nil
	}
	var backend string
	if err := json.Unmarshal(backendRaw, &backend); err != nil {
		return storeSQLitePointer{}, false, err
	}
	if backend != storeSQLiteBackend {
		return storeSQLitePointer{}, false, nil
	}
	var pointer storeSQLitePointer
	if err := json.Unmarshal(data, &pointer); err != nil {
		return storeSQLitePointer{}, false, err
	}
	if pointer.SchemaVersion < storeSQLiteMinPointerSchemaVersion || pointer.SchemaVersion > storeSQLitePointerSchemaVersion {
		return storeSQLitePointer{}, false, &UnsupportedSchemaVersionError{Version: pointer.SchemaVersion}
	}
	if pointer.StorageVersion != storeSQLiteVersion {
		return storeSQLitePointer{}, false, fmt.Errorf("unsupported sqlite store version %d", pointer.StorageVersion)
	}
	return pointer, true, nil
}

func stateSchemaVersionFromData(data []byte) (int, bool) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return 0, false
	}
	value, ok := raw["schema_version"]
	if !ok {
		return 0, false
	}
	var version int
	if err := json.Unmarshal(value, &version); err != nil {
		return 0, false
	}
	return version, true
}

func (s *Store) currentSQLitePointerUnlocked() (storeSQLitePointer, bool, error) {
	info, err := os.Stat(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.clearSQLitePointerCacheUnlocked()
		return storeSQLitePointer{}, false, nil
	}
	if err != nil {
		return storeSQLitePointer{}, false, err
	}
	if pointer, ok := s.cachedSQLitePointerUnlocked(info); ok {
		return pointer, true, nil
	}
	s.clearSQLitePointerCacheUnlocked()
	if info.Size() > maxStatePointerSize {
		return storeSQLitePointer{}, false, nil
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.clearSQLitePointerCacheUnlocked()
		return storeSQLitePointer{}, false, nil
	}
	if err != nil {
		return storeSQLitePointer{}, false, err
	}
	pointer, ok, err := s.sqlitePointerFromDataUnlocked(data, info)
	if err != nil || !ok || pointer.SchemaVersion >= storeSQLitePointerSchemaVersion {
		return pointer, ok, err
	}
	pointer, err = s.upgradeSQLitePointerUnlocked(pointer)
	if err != nil {
		return storeSQLitePointer{}, false, err
	}
	return pointer, true, nil
}

func (s *Store) upgradeSQLitePointerUnlocked(pointer storeSQLitePointer) (storeSQLitePointer, error) {
	if pointer.SchemaVersion >= storeSQLitePointerSchemaVersion {
		return pointer, nil
	}
	// Pointer schema upgrades are metadata-only and must happen before any
	// normal writer can touch the database. Validate the target path first so a
	// malformed pointer remains fail-closed and byte-for-byte unchanged. Once
	// published, older helpers reject the store instead of rewriting rows with
	// a struct that predates the durable poll frontier fields.
	if _, err := s.storeSQLitePath(pointer); err != nil {
		return pointer, err
	}
	pointer.SchemaVersion = storeSQLitePointerSchemaVersion
	if err := s.writeSQLitePointerUnlocked(pointer); err != nil {
		return storeSQLitePointer{}, err
	}
	return pointer, nil
}

func (s *Store) currentSQLitePointerReadOnly() (storeSQLitePointer, bool, error) {
	info, err := os.Lstat(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return storeSQLitePointer{}, false, nil
	}
	if err != nil {
		return storeSQLitePointer{}, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > maxStatePointerSize {
		return storeSQLitePointer{}, false, nil
	}
	file, err := openRuntimeStateFile(s.path)
	if err != nil {
		return storeSQLitePointer{}, false, err
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil {
		return storeSQLitePointer{}, false, readErr
	}
	if closeErr != nil {
		return storeSQLitePointer{}, false, closeErr
	}
	return storeSQLitePointerFromData(data)
}

func (s *Store) writeSQLitePointerUnlocked(pointer storeSQLitePointer) error {
	if strings.TrimSpace(pointer.Path) == "" {
		pointer.Path = storeSQLiteFileName
	}
	pointer.SchemaVersion = storeSQLitePointerSchemaVersion
	pointer.StorageBackend = storeSQLiteBackend
	pointer.StorageVersion = storeSQLiteVersion
	data, err := json.Marshal(pointer)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := atomicWriteFile(s.path, data, fileMode); err != nil {
		return err
	}
	if info, err := os.Stat(s.path); err == nil {
		s.cacheSQLitePointerUnlocked(pointer, info, false)
		s.sqlitePointerFingerprint = sha256Bytes(data)
	} else {
		s.clearSQLitePointerCacheUnlocked()
	}
	return nil
}

func (s *Store) cacheSQLitePointerUnlocked(pointer storeSQLitePointer, info os.FileInfo, trusted bool) {
	s.sqlitePointer = pointer
	s.sqlitePointerCached = true
	s.sqlitePointerTrusted = trusted
	s.sqlitePointerSize = info.Size()
	s.sqlitePointerMod = info.ModTime()
	s.sqlitePointerChange = fileInfoChangeTimeUnixNano(info)
}

func (s *Store) cachedSQLitePointerUnlocked(info os.FileInfo) (storeSQLitePointer, bool) {
	if !s.sqlitePointerCached {
		return storeSQLitePointer{}, false
	}
	if !s.sqlitePointerTrusted {
		return storeSQLitePointer{}, false
	}
	if info.Size() != s.sqlitePointerSize || !info.ModTime().Equal(s.sqlitePointerMod) {
		return storeSQLitePointer{}, false
	}
	if change := fileInfoChangeTimeUnixNano(info); change != 0 || s.sqlitePointerChange != 0 {
		if change == 0 || s.sqlitePointerChange == 0 || change != s.sqlitePointerChange {
			return storeSQLitePointer{}, false
		}
	}
	return s.sqlitePointer, true
}

func (s *Store) sqlitePointerFromDataUnlocked(data []byte, info os.FileInfo) (storeSQLitePointer, bool, error) {
	pointer, ok, err := storeSQLitePointerFromData(data)
	if err != nil || !ok {
		return pointer, ok, err
	}
	s.cacheSQLitePointerUnlocked(pointer, info, true)
	s.sqlitePointerFingerprint = sha256Bytes(data)
	return pointer, true, nil
}

func (s *Store) clearSQLitePointerCacheUnlocked() {
	s.sqlitePointer = storeSQLitePointer{}
	s.sqlitePointerCached = false
	s.sqlitePointerTrusted = false
	s.sqlitePointerSize = 0
	s.sqlitePointerMod = time.Time{}
	s.sqlitePointerChange = 0
	s.sqlitePointerFingerprint = ""
}

func fileInfoChangeTimeUnixNano(info os.FileInfo) int64 {
	if info == nil || info.Sys() == nil {
		return 0
	}
	v := reflect.ValueOf(info.Sys())
	if !v.IsValid() {
		return 0
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return 0
	}
	for _, fieldName := range []string{"Ctim", "Ctimespec"} {
		ts := v.FieldByName(fieldName)
		if unixNano, ok := reflectedTimespecUnixNano(ts); ok {
			return unixNano
		}
	}
	return 0
}

func reflectedTimespecUnixNano(v reflect.Value) (int64, bool) {
	if !v.IsValid() {
		return 0, false
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, false
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return 0, false
	}
	sec, ok := reflectedInt64Field(v, "Sec")
	if !ok {
		return 0, false
	}
	nsec, ok := reflectedInt64Field(v, "Nsec")
	if !ok {
		return 0, false
	}
	return sec*int64(time.Second) + nsec, true
}

func reflectedInt64Field(v reflect.Value, name string) (int64, bool) {
	field := v.FieldByName(name)
	if !field.IsValid() {
		return 0, false
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		value := field.Uint()
		if value > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	default:
		return 0, false
	}
}

func (s *Store) storeSQLitePath(pointer storeSQLitePointer) (string, error) {
	path := strings.TrimSpace(pointer.Path)
	if path == "" {
		path = storeSQLiteFileName
	}
	if filepath.IsAbs(path) {
		return "", fmt.Errorf("invalid sqlite store path %q: absolute paths are not supported", pointer.Path)
	}
	path = filepath.Clean(path)
	if path != storeSQLiteFileName {
		return "", fmt.Errorf("invalid sqlite store path %q: expected %q", pointer.Path, storeSQLiteFileName)
	}
	return filepath.Join(filepath.Dir(s.path), path), nil
}

func (s *Store) loadSQLiteStateUnlocked(ctx context.Context, pointer storeSQLitePointer) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	return loadSQLiteState(ctx, db)
}

// loadSQLiteStateWithSQLiteLock is used by migration while it already holds
// sqliteRuntimeMu. All ordinary callers use loadSQLiteStateUnlocked, which
// acquires the handle lock through sqliteDBUnlocked.
func (s *Store) loadSQLiteStateWithSQLiteLock(ctx context.Context, pointer storeSQLitePointer) (State, error) {
	db, err := s.sqliteDBUnlockedLocked(pointer)
	if err != nil {
		return State{}, err
	}
	return loadSQLiteState(ctx, db)
}

func (s *Store) saveSQLiteStateUnlocked(pointer storeSQLitePointer, state State) error {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return err
	}
	if err := ensureSQLiteSchema(db); err != nil {
		return err
	}
	return writeSQLiteState(context.Background(), db, state)
}

func (s *Store) loadSQLiteSelectedStateFieldsUnlocked(ctx context.Context, pointer storeSQLitePointer, wanted map[string]struct{}) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	return loadSQLiteSelectedState(ctx, db, wanted)
}

func (s *Store) hotPollScheduleStateSQLite(ctx context.Context) (State, bool, error) {
	state, _, handled, err := s.hotPollScheduleSQLite(ctx, false)
	return state, handled, err
}

func (s *Store) hotPollScheduleSnapshotSQLite(ctx context.Context) (State, map[string]bool, bool, error) {
	return s.hotPollScheduleSQLite(ctx, true)
}

// hotPollReadyScheduleStateSQLite is the bounded SQLite admission path used by
// the listener.  The compatibility HotPollScheduleState API intentionally
// keeps its historical full schedule semantics for callers that need a
// snapshot, while the hot listener only materializes rows that can run in the
// current quantum.  This prevents a large number of idle chats from turning a
// heartbeat interval into an O(N) JSON decode pass.
func (s *Store) hotPollReadyScheduleStateSQLite(ctx context.Context, controlChatID string, now time.Time) (State, bool, error) {
	if now.IsZero() {
		now = time.Now()
	}
	state, _, handled, err := s.hotPollScheduleSQLiteReady(ctx, controlChatID, now)
	return state, handled, err
}

func (s *Store) hotPollScheduleSQLite(ctx context.Context, includeParkedSkip bool) (State, map[string]bool, bool, error) {
	return s.hotPollScheduleSQLiteWithOptions(ctx, includeParkedSkip, "", time.Time{}, false)
}

func (s *Store) hotPollScheduleSQLiteReady(ctx context.Context, controlChatID string, now time.Time) (State, map[string]bool, bool, error) {
	return s.hotPollScheduleSQLiteWithOptions(ctx, false, controlChatID, now, true)
}

// loadSQLiteHotPollReadyChatIDs performs ready-only admission using the
// indexed scalar projection and bounded Go decoding. The old ready query
// validated every optional field with JSON1 before applying LIMIT, so a large
// operational backlog made each listener tick proportional to the number of
// chats rather than to the small work quantum. A syntax-corrupt row gets its
// own bounded recovery slot; semantically malformed rows found in a due lane
// use that same slot and cannot consume the healthy-chat quota.
func loadSQLiteHotPollReadyChatIDs(ctx context.Context, db *sql.DB, controlChatID string, now time.Time, limit int) ([]string, error) {
	controlChatID = strings.TrimSpace(controlChatID)
	if limit <= 0 {
		limit = sqliteHotPollReadyLimit
	}
	if now.IsZero() {
		now = time.Now()
	}
	operationalLimit, ordinaryLimit := sqliteHotPollLaneLimits(limit)
	ids := make([]string, 0, limit)
	seen := make(map[string]struct{}, limit)
	malformed := 0
	// frontier_active is a materialized admission hint.  It is normally kept
	// in the same transaction as json, but an older writer or a partial repair
	// can leave it stale.  Keep a small reconciliation lane for rows whose
	// decoded canonical poll disagrees with the hint; otherwise those rows can
	// consume the ordinary quota and hide a healthy chat indefinitely.
	deferredOperational := make([]string, 0, limit)
	deferredOrdinary := make([]string, 0, limit)
	deferredSeen := make(map[string]struct{}, limit)
	appendRow := func(chatID string, valid bool) {
		chatID = strings.TrimSpace(chatID)
		if chatID == "" || len(ids) >= limit {
			return
		}
		if _, ok := seen[chatID]; ok {
			return
		}
		if !valid {
			if malformed >= sqliteHotPollMalformedLimit {
				return
			}
			malformed++
		}
		seen[chatID] = struct{}{}
		ids = append(ids, chatID)
	}

	if controlChatID != "" {
		var raw []byte
		err := db.QueryRowContext(ctx, `SELECT json FROM chat_polls WHERE chat_id = ?`, controlChatID).Scan(&raw)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if err == nil {
			var typed ChatPollState
			valid := json.Unmarshal(raw, &typed) == nil && strings.TrimSpace(typed.ChatID) == controlChatID && chatPollAdmissionValid(typed)
			appendRow(controlChatID, valid)
		}
	}

	// Syntax-corrupt rows have no trustworthy schedule and must be surfaced for
	// local recovery even if stale derived deadlines happen to be in the future.
	if len(ids) < limit && malformed < sqliteHotPollMalformedLimit {
		rows, err := db.QueryContext(ctx, `SELECT chat_id, json FROM chat_polls
WHERE (? = '' OR chat_id != ?) AND json_valid(json) = 0
ORDER BY updated_at, next_poll_at, last_activity_at, chat_id
LIMIT ?`, controlChatID, controlChatID, sqliteHotPollMalformedLimit-malformed)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var chatID string
			var raw []byte
			if err := rows.Scan(&chatID, &raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			appendRow(chatID, false)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}

	type cursor struct {
		updated, next, activity int64
		id                      string
		set                     bool
	}
	type readyRow struct {
		chatID                  string
		raw                     []byte
		updated, next, activity int64
	}
	queryLane := func(operational bool, after cursor, pageSize int) ([]readyRow, error) {
		where := `(? = '' OR chat_id != ?) AND json_valid(json) = 1`
		args := []any{controlChatID, controlChatID}
		if operational {
			where += `
  AND COALESCE(frontier_active, 0) != 0
  AND COALESCE(next_poll_at, 0) <= ?
  AND COALESCE(blocked_until, 0) <= ?`
			args = append(args, sqliteTime(now), sqliteTime(now))
		} else {
			where += `
  AND COALESCE(frontier_active, 0) = 0
  AND COALESCE(next_poll_at, 0) <= ?
  AND COALESCE(blocked_until, 0) <= ?
  AND (COALESCE(parked_skip_eligible, 0) = 0
       OR (poll_state = ? AND COALESCE(next_poll_at, 0) <= ?))`
			args = append(args, sqliteTime(now), sqliteTime(now), chatPollStateParked, sqliteTime(now))
		}
		if after.set {
			where += `
  AND (COALESCE(updated_at, 0) > ?
       OR (COALESCE(updated_at, 0) = ? AND COALESCE(next_poll_at, 0) > ?)
       OR (COALESCE(updated_at, 0) = ? AND COALESCE(next_poll_at, 0) = ? AND COALESCE(last_activity_at, 0) > ?)
       OR (COALESCE(updated_at, 0) = ? AND COALESCE(next_poll_at, 0) = ? AND COALESCE(last_activity_at, 0) = ? AND chat_id > ?))`
			args = append(args,
				after.updated, after.updated, after.next,
				after.updated, after.next, after.activity,
				after.updated, after.next, after.activity, after.id)
		}
		args = append(args, pageSize)
		rows, err := db.QueryContext(ctx, `SELECT chat_id, json,
       COALESCE(updated_at, 0), COALESCE(next_poll_at, 0), COALESCE(last_activity_at, 0)
FROM chat_polls
WHERE `+where+`
ORDER BY COALESCE(updated_at, 0), COALESCE(next_poll_at, 0), COALESCE(last_activity_at, 0), chat_id
LIMIT ?`, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]readyRow, 0, pageSize)
		for rows.Next() {
			var row readyRow
			if err := rows.Scan(&row.chatID, &row.raw, &row.updated, &row.next, &row.activity); err != nil {
				return nil, err
			}
			out = append(out, row)
		}
		return out, rows.Err()
	}

	appendLane := func(operational bool, quota int) error {
		valid := 0
		after := cursor{}
		const pageSize = 64
		for page := 0; valid < quota && len(ids) < limit; page++ {
			// Keep scanning the keyset until the lane quota is satisfied or the
			// table is exhausted. A fixed eight-page cap made a semantically
			// corrupt prefix permanently starve a healthy chat behind it. Context
			// cancellation remains the per-cycle budget for pathological stores.
			if err := ctx.Err(); err != nil {
				return err
			}
			rows, err := queryLane(operational, after, pageSize)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				if err := ctx.Err(); err != nil {
					return err
				}
				after = cursor{updated: row.updated, next: row.next, activity: row.activity, id: row.chatID, set: true}
				before := len(ids)
				var typed ChatPollState
				if json.Unmarshal(row.raw, &typed) != nil || strings.TrimSpace(typed.ChatID) != strings.TrimSpace(row.chatID) {
					appendRow(row.chatID, false)
					continue
				}
				// A typed row can still be structurally unusable (for example
				// pending_page:{}). Keep it in the bounded malformed lane, but do
				// not count it toward the healthy lane quota; otherwise a long
				// malformed operational prefix can hide a healthy chat from the
				// ready-schedule fast path indefinitely.
				if !chatPollAdmissionValid(typed) {
					appendRow(row.chatID, false)
					continue
				}
				actualOperational := chatPollHasOperationalFrontier(typed)
				if actualOperational != operational {
					if _, ok := deferredSeen[row.chatID]; !ok {
						deferredSeen[row.chatID] = struct{}{}
						if actualOperational {
							deferredOperational = append(deferredOperational, row.chatID)
						} else {
							deferredOrdinary = append(deferredOrdinary, row.chatID)
						}
					}
					continue
				}
				appendRow(row.chatID, true)
				if len(ids) == before {
					continue
				}
				valid++
				if valid >= quota || len(ids) >= limit {
					break
				}
			}
			if len(rows) < pageSize {
				break
			}
		}
		return nil
	}
	if err := appendLane(false, ordinaryLimit); err != nil {
		return nil, err
	}
	// Rows discovered in the wrong hint lane are still useful work.  Admit the
	// canonical operational subset before querying the indexed operational lane
	// so stale ordinary hints cannot disappear for every future cycle.
	for _, chatID := range deferredOperational {
		if len(ids) >= limit {
			break
		}
		if _, ok := seen[chatID]; ok {
			continue
		}
		seen[chatID] = struct{}{}
		ids = append(ids, chatID)
	}
	if err := appendLane(true, operationalLimit); err != nil {
		return nil, err
	}
	for _, chatID := range deferredOrdinary {
		if len(ids) >= limit {
			break
		}
		if _, ok := seen[chatID]; ok {
			continue
		}
		seen[chatID] = struct{}{}
		ids = append(ids, chatID)
	}
	return ids, nil
}

func sqliteChatPollSelectionQuery(ids []string) (string, []any) {
	if len(ids) == 0 {
		return `SELECT chat_id, json FROM chat_polls WHERE 0`, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	return `SELECT chat_id, json FROM chat_polls WHERE chat_id IN (` + strings.Join(placeholders, ",") + `)`, args
}

func (s *Store) hotPollScheduleSQLiteReadyOptimized(ctx context.Context, includeParkedSkip bool, controlChatID string, now time.Time) (State, map[string]bool, bool, error) {
	var state State
	parkedSkip := map[string]bool{}
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		if now.IsZero() {
			now = time.Now()
		}
		ids, err := loadSQLiteHotPollReadyChatIDs(ctx, db, controlChatID, now, sqliteHotPollReadyLimit)
		if err != nil {
			return err
		}
		chatPollQuery, chatPollArgs := sqliteChatPollSelectionQuery(ids)
		selected, err := loadSQLiteHotPollSelectedStateWithChatPollQuery(ctx, db, hotPollScheduleBaseFields, chatPollQuery, chatPollArgs...)
		if err != nil {
			return err
		}
		if err := loadSQLiteHotPollActiveTurns(ctx, db, selected, true); err != nil {
			return err
		}
		if err := loadSQLiteHotPollImportingCheckpoints(ctx, db, selected, true); err != nil {
			return err
		}
		if includeParkedSkip {
			skipped, err := loadSQLiteParkedNoticeChatIDs(ctx, db)
			if err != nil {
				return err
			}
			parkedSkip = skipped
		}
		state = selected
		handled = true
		return nil
	})
	return state, parkedSkip, handled, err
}

func (s *Store) hotPollScheduleSQLiteWithOptions(ctx context.Context, includeParkedSkip bool, controlChatID string, now time.Time, readyOnly bool) (State, map[string]bool, bool, error) {
	if readyOnly {
		return s.hotPollScheduleSQLiteReadyOptimized(ctx, includeParkedSkip, controlChatID, now)
	}
	var state State
	parkedSkip := map[string]bool{}
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		if now.IsZero() {
			now = time.Now()
		}
		parkProbeDueAt := sqliteTime(now)
		validPoll := sqliteChatPollValidJSONSQL("json", "chat_id")
		frontier := sqliteChatPollOperationalFrontierSQL("json")
		validPollAlias := sqliteChatPollValidJSONSQL("p.json", "p.chat_id")
		if readyOnly {
			validPoll = sqliteChatPollAdmissionValidJSONSQL("json", "chat_id")
			validPollAlias = sqliteChatPollAdmissionValidJSONSQL("p.json", "p.chat_id")
		}
		malformedPoll := `NOT (` + validPoll + `)`
		malformedPollAlias := `NOT (` + validPollAlias + `)`
		chatPollQuery := `SELECT chat_id, json FROM chat_polls
WHERE (` + validPoll + ` OR ` + malformedPoll + `)
  AND (` + malformedPoll + ` OR COALESCE(parked_skip_eligible, 0) = 0
   OR (poll_state = ? AND COALESCE(next_poll_at, 0) <= ?))`
		chatPollArgs := []any{chatPollStateParked, parkProbeDueAt}
		if readyOnly {
			controlChatID = strings.TrimSpace(controlChatID)
			operationalLimit, ordinaryLimit := sqliteHotPollLaneLimits(sqliteHotPollReadyLimit)
			// The listener admits one control row, then independent due
			// operational and ordinary lanes.  A future retry deadline applies to
			// both lanes; an operational marker is not permission to hammer Graph
			// before Retry-After.  Keeping the lane limits in this query avoids a
			// LIMIT 64 operational prefix starving a newly active ordinary chat.
			chatPollQuery = `WITH control AS (
    SELECT chat_id, 0 AS lane, COALESCE(updated_at, 0) AS sort_updated,
           COALESCE(next_poll_at, 0) AS sort_next,
           COALESCE(last_activity_at, 0) AS sort_activity
    FROM chat_polls
    WHERE chat_id = ? AND (` + validPoll + ` OR ` + malformedPoll + `)
    LIMIT 1
), operational AS (
	SELECT chat_id, 2 AS lane, COALESCE(updated_at, 0) AS sort_updated,
           COALESCE(next_poll_at, 0) AS sort_next,
           COALESCE(last_activity_at, 0) AS sort_activity
    FROM chat_polls
    WHERE chat_id != ?
      AND ((` + validPoll + `
        AND ` + frontier + `
        AND COALESCE(next_poll_at, 0) <= ?
        AND COALESCE(blocked_until, 0) <= ?)
      )
    ORDER BY sort_updated, sort_next, sort_activity, chat_id
    LIMIT ?
), ordinary AS (
	SELECT chat_id, 1 AS lane, COALESCE(updated_at, 0) AS sort_updated,
           COALESCE(next_poll_at, 0) AS sort_next,
           COALESCE(last_activity_at, 0) AS sort_activity
    FROM chat_polls
    WHERE chat_id != ?
      AND (` + validPoll + `
        AND NOT ` + frontier + `
        AND COALESCE(next_poll_at, 0) <= ?
        AND COALESCE(blocked_until, 0) <= ?
        AND (COALESCE(parked_skip_eligible, 0) = 0
             OR (poll_state = ? AND COALESCE(next_poll_at, 0) <= ?)))
    ORDER BY sort_updated, sort_next, sort_activity, chat_id
    LIMIT ?
), malformed AS (
	SELECT chat_id, 1 AS lane, COALESCE(updated_at, 0) AS sort_updated,
           COALESCE(next_poll_at, 0) AS sort_next,
           COALESCE(last_activity_at, 0) AS sort_activity
	FROM chat_polls
	WHERE chat_id != ?
	  AND ` + malformedPoll + `
	ORDER BY sort_updated, sort_next, sort_activity, chat_id
	LIMIT ?
), admitted AS (
    SELECT * FROM control
    UNION ALL SELECT * FROM ordinary
    UNION ALL SELECT * FROM malformed
    UNION ALL SELECT * FROM operational
)
SELECT p.chat_id, p.json
FROM chat_polls p
JOIN admitted a ON a.chat_id = p.chat_id
WHERE (` + validPollAlias + ` OR ` + malformedPollAlias + `)
ORDER BY a.lane, a.sort_updated, a.sort_next, a.sort_activity, a.chat_id`
			chatPollQuery += ` LIMIT ?`
			chatPollArgs = []any{controlChatID, controlChatID, parkProbeDueAt, parkProbeDueAt, operationalLimit, controlChatID, parkProbeDueAt, parkProbeDueAt, chatPollStateParked, parkProbeDueAt, ordinaryLimit, controlChatID, sqliteHotPollMalformedLimit, sqliteHotPollReadyLimit}
		}
		selected, err := loadSQLiteHotPollSelectedStateWithChatPollQuery(ctx, db, hotPollScheduleBaseFields,
			chatPollQuery, chatPollArgs...)
		if err != nil {
			return err
		}
		if err := loadSQLiteHotPollActiveTurns(ctx, db, selected, readyOnly); err != nil {
			return err
		}
		if err := loadSQLiteHotPollImportingCheckpoints(ctx, db, selected, readyOnly); err != nil {
			return err
		}
		if includeParkedSkip {
			skipped, err := loadSQLiteParkedNoticeChatIDs(ctx, db)
			if err != nil {
				return err
			}
			parkedSkip = skipped
		}
		state = selected
		handled = true
		return nil
	})
	return state, parkedSkip, handled, err
}

func (s *Store) hotPollWorkCandidatesSQLite(ctx context.Context, controlChatID string, idleBefore time.Time, now time.Time) ([]SessionContext, bool, error) {
	var out []SessionContext
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		// The candidate loader below deliberately does its semantic session
		// validation in Go, one bounded page at a time.  Do not preflight with
		// sqliteSessionValidJSONSQL here: that predicate expands into many
		// JSON1 calls and parses every row before the scheduler has selected a
		// work quantum.  A non-empty sessions table is enough to keep the
		// SQLite projection authoritative; malformed/unknown rows are isolated
		// by loadSQLiteHotPollWorkCandidates and cannot make the registry
		// fallback silently recreate a second durable session.
		hasSessions, err := loadSQLiteHasSessions(ctx, db)
		if err != nil {
			return err
		}
		if !hasSessions {
			return nil
		}
		sessions, err := loadSQLiteHotPollWorkCandidates(ctx, db, controlChatID, idleBefore, now, sqliteHotPollReadyLimit)
		if err != nil {
			return err
		}
		out = sessions
		handled = true
		return nil
	})
	return out, handled, err
}

func (s *Store) idleWorkChatParkCandidatesSQLite(ctx context.Context, controlChatID string, idleBefore time.Time, limit int) ([]IdleWorkChatParkCandidate, bool, error) {
	var out []IdleWorkChatParkCandidate
	handled := false
	if idleBefore.IsZero() || limit <= 0 {
		return nil, false, nil
	}
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		candidates, err := loadSQLiteIdleWorkChatParkCandidates(ctx, db, controlChatID, idleBefore, limit)
		if err != nil {
			return err
		}
		out = candidates
		handled = true
		return nil
	})
	return out, handled, err
}

func (s *Store) sessionsByIDSQLite(ctx context.Context, ids []string) (map[string]SessionContext, bool, error) {
	out := make(map[string]SessionContext, len(ids))
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		const batchSize = 500
		for start := 0; start < len(ids); start += batchSize {
			end := start + batchSize
			if end > len(ids) {
				end = len(ids)
			}
			if err := loadSQLiteSessionsByID(ctx, db, ids[start:end], out); err != nil {
				return err
			}
		}
		handled = true
		return nil
	})
	return out, handled, err
}

func (s *Store) hasSessionsSQLite(ctx context.Context) (bool, bool, error) {
	hasSessions := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		hasSessions, err = loadSQLiteHasValidSessions(ctx, db)
		if err != nil {
			return err
		}
		handled = true
		return nil
	})
	return hasSessions, handled, err
}

func (s *Store) sqliteDBUnlocked(pointer storeSQLitePointer) (*sql.DB, error) {
	s.sqliteRuntimeMu.Lock()
	defer s.sqliteRuntimeMu.Unlock()
	return s.sqliteDBUnlockedLocked(pointer)
}

func (s *Store) sqliteDBUnlockedLocked(pointer storeSQLitePointer) (*sql.DB, error) {
	path, err := s.storeSQLitePath(pointer)
	if err != nil {
		return nil, err
	}
	if s.sqliteDB != nil && s.sqliteDBPath == path {
		return s.sqliteDB, nil
	}
	if s.sqliteDB != nil {
		_ = s.sqliteDB.Close()
		s.sqliteDB = nil
		s.sqliteDBPath = ""
	}
	db, err := openExistingSQLiteStore(path)
	if err != nil {
		return nil, err
	}
	if err := ensureSQLiteSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	s.sqliteDB = db
	s.sqliteDBPath = path
	return db, nil
}

// withSQLiteRuntimeDB runs a liveness-only operation without acquiring
// Store.mu. Full Store operations serialize on Store.mu and the state file
// lock while they execute user callbacks; heartbeats must not inherit either
// of those potentially long waits. The operation shares sqliteDB, whose
// single physical connection serializes its transaction with normal store
// work, and sqliteRuntimeMu makes closing/rebinding the handle safe.
func (s *Store) withSQLiteRuntimeDB(ctx context.Context, fn func(*sql.DB) error) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := lockMutexContext(ctx, &s.sqliteRuntimeMu); err != nil {
		return false, err
	}
	defer s.sqliteRuntimeMu.Unlock()
	pointer, ok, err := s.currentSQLitePointerReadOnly()
	if err != nil || !ok {
		return false, err
	}
	path, err := s.storeSQLitePath(pointer)
	if err != nil {
		return true, err
	}
	if s.sqliteDB != nil && s.sqliteDBPath != path {
		if err := s.sqliteDB.Close(); err != nil {
			return true, err
		}
		s.sqliteDB = nil
		s.sqliteDBPath = ""
	}
	if s.sqliteDB == nil {
		db, err := openExistingSQLiteRuntimeStore(path)
		if err != nil {
			return true, err
		}
		s.sqliteDB = db
		s.sqliteDBPath = path
	}
	return true, fn(s.sqliteDB)
}

func (s *Store) closeSQLiteDBLocked() error {
	if s.sqliteDB == nil {
		return nil
	}
	err := s.sqliteDB.Close()
	s.sqliteDB = nil
	s.sqliteDBPath = ""
	return err
}

type SQLiteWALCheckpointResult struct {
	SQLite             bool
	Attempted          bool
	WALPath            string
	WALSize            int64
	Busy               int
	LogFrames          int
	CheckpointedFrames int
}

func (s *Store) CheckpointSQLiteWAL(ctx context.Context, minSizeBytes int64) (SQLiteWALCheckpointResult, error) {
	if minSizeBytes < 0 {
		minSizeBytes = 0
	}
	var result SQLiteWALCheckpointResult
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		dbPath, err := s.storeSQLitePath(pointer)
		if err != nil {
			return err
		}
		walPath := dbPath + "-wal"
		result.SQLite = true
		result.WALPath = walPath
		info, err := os.Stat(walPath)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		result.WALSize = info.Size()
		if result.WALSize < minSizeBytes {
			return nil
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		result.Attempted = true
		return db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(
			&result.Busy,
			&result.LogFrames,
			&result.CheckpointedFrames,
		)
	})
	return result, err
}

func loadSQLiteStateFile(path string) (State, error) {
	db, err := openExistingSQLiteStore(path)
	if err != nil {
		return State{}, err
	}
	defer db.Close()
	return loadSQLiteState(context.Background(), db)
}

func loadSQLiteStateFileReadOnly(ctx context.Context, path string) (State, error) {
	return loadSQLiteStateFileReadOnlyWithHook(ctx, path, nil)
}

func loadSQLiteRuntimeMetadataFileReadOnly(ctx context.Context, path string) (RuntimeMetadata, error) {
	const maxAttempts = 3
	var changedErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		dbBefore, err := sqliteReadOnlyFileIdentityForPath(path)
		if err != nil {
			return RuntimeMetadata{}, err
		}
		if !dbBefore.Exists {
			return RuntimeMetadata{}, os.ErrNotExist
		}
		walBefore, err := sqliteReadOnlyFileIdentityForPath(path + "-wal")
		if err != nil {
			return RuntimeMetadata{}, err
		}
		immutable := !walBefore.Exists || walBefore.Size == 0
		if !immutable {
			if err := requireSQLiteReadOnlySHM(path); err != nil {
				return RuntimeMetadata{}, err
			}
		}
		metadata, err := loadSQLiteRuntimeMetadataFileReadOnlyAttempt(ctx, path, immutable)
		if err != nil {
			return RuntimeMetadata{}, err
		}
		if !immutable {
			return metadata, nil
		}
		dbAfter, err := sqliteReadOnlyFileIdentityForPath(path)
		if err != nil {
			return RuntimeMetadata{}, err
		}
		walAfter, err := sqliteReadOnlyFileIdentityForPath(path + "-wal")
		if err != nil {
			return RuntimeMetadata{}, err
		}
		if dbBefore == dbAfter && walBefore == walAfter {
			return metadata, nil
		}
		changedErr = fmt.Errorf("database or WAL changed during immutable metadata attempt %d", attempt+1)
	}
	return RuntimeMetadata{}, fmt.Errorf("read stable sqlite runtime metadata after %d attempts: %w", maxAttempts, changedErr)
}

func loadSQLiteRuntimeMetadataFileReadOnlyAttempt(ctx context.Context, path string, immutable bool) (RuntimeMetadata, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	if immutable {
		query.Set("immutable", "1")
	}
	db, err := sql.Open("sqlite", sqliteFileURI(path, query))
	if err != nil {
		return RuntimeMetadata{}, err
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return RuntimeMetadata{}, err
	}
	defer conn.Close()
	return loadSQLiteRuntimeMetadataConn(ctx, conn)
}

func loadSQLiteRuntimeMetadataFileOfflineRecoveryReadOnly(ctx context.Context, path string) (RuntimeMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	recoveryNeeded, err := sqliteOfflineRecoveryNeeded(path)
	if err != nil {
		return RuntimeMetadata{}, err
	}
	if !recoveryNeeded {
		return loadSQLiteRuntimeMetadataFileReadOnly(ctx, path)
	}
	reader, err := openSQLiteOfflineRecoveryReader(ctx, path)
	if err != nil {
		return RuntimeMetadata{}, err
	}
	metadata, loadErr := loadSQLiteRuntimeMetadataConn(ctx, reader.readConn)
	closeErr := reader.Close()
	if loadErr != nil {
		return RuntimeMetadata{}, loadErr
	}
	if closeErr != nil {
		return RuntimeMetadata{}, closeErr
	}
	return metadata, nil
}

func loadSQLiteRuntimeMetadataConn(ctx context.Context, conn *sql.Conn) (RuntimeMetadata, error) {
	if sqliteRuntimeMetadataConnectionTestHook != nil {
		sqliteRuntimeMetadataConnectionTestHook()
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA query_only = ON`); err != nil {
		return RuntimeMetadata{}, err
	}
	if err := validateSQLiteRuntimeMetadataTablesContext(ctx, conn); err != nil {
		return RuntimeMetadata{}, err
	}

	var metadata RuntimeMetadata
	var controlChatJSON string
	if err := conn.QueryRowContext(
		ctx,
		`SELECT CASE WHEN json_valid(value) THEN COALESCE(json_extract(value, '$.control_chat'), '{}') ELSE '{}' END FROM state_meta WHERE key = 'state_json'`,
	).Scan(&controlChatJSON); err != nil {
		return RuntimeMetadata{}, err
	}
	if err := json.Unmarshal([]byte(controlChatJSON), &metadata.ControlChat); err != nil {
		return RuntimeMetadata{}, err
	}

	rows, err := conn.QueryContext(ctx, `SELECT key, json FROM runtime_state WHERE key IN (?, ?, ?, ?, ?)`,
		sqliteRuntimeKeyScope,
		sqliteRuntimeKeyControlLease,
		sqliteRuntimeKeyServiceOwner,
		sqliteRuntimeKeyLockOwner,
		sqliteRuntimeKeyServiceControl,
	)
	if err != nil {
		return RuntimeMetadata{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			return RuntimeMetadata{}, err
		}
		switch key {
		case sqliteRuntimeKeyScope:
			if err := json.Unmarshal(raw, &metadata.Scope); err != nil {
				return RuntimeMetadata{}, err
			}
		case sqliteRuntimeKeyControlLease:
			if err := json.Unmarshal(raw, &metadata.ControlLease); err != nil {
				return RuntimeMetadata{}, err
			}
		case sqliteRuntimeKeyServiceOwner:
			if err := json.Unmarshal(raw, &metadata.ServiceOwner); err != nil {
				return RuntimeMetadata{}, err
			}
		case sqliteRuntimeKeyLockOwner:
			if err := json.Unmarshal(raw, &metadata.LockOwner); err != nil {
				return RuntimeMetadata{}, err
			}
		case sqliteRuntimeKeyServiceControl:
			if err := json.Unmarshal(raw, &metadata.ServiceControl); err != nil {
				return RuntimeMetadata{}, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return RuntimeMetadata{}, err
	}
	return metadata, nil
}

func loadSQLiteWatchdogStateFileReadOnly(ctx context.Context, path string) (State, error) {
	const maxAttempts = 3
	var changedErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		dbBefore, err := sqliteReadOnlyFileIdentityForPath(path)
		if err != nil {
			return State{}, err
		}
		if !dbBefore.Exists {
			return State{}, os.ErrNotExist
		}
		walBefore, err := sqliteReadOnlyFileIdentityForPath(path + "-wal")
		if err != nil {
			return State{}, err
		}
		immutable := !walBefore.Exists || walBefore.Size == 0
		if !immutable {
			if err := requireSQLiteReadOnlySHM(path); err != nil {
				return State{}, err
			}
		}
		state, err := loadSQLiteWatchdogStateFileReadOnlyAttempt(ctx, path, immutable)
		if err != nil {
			return State{}, err
		}
		if !immutable {
			return state, nil
		}
		dbAfter, err := sqliteReadOnlyFileIdentityForPath(path)
		if err != nil {
			return State{}, err
		}
		walAfter, err := sqliteReadOnlyFileIdentityForPath(path + "-wal")
		if err != nil {
			return State{}, err
		}
		if dbBefore == dbAfter && walBefore == walAfter {
			return state, nil
		}
		changedErr = fmt.Errorf("database or WAL changed during immutable watchdog attempt %d", attempt+1)
	}
	return State{}, fmt.Errorf("read stable sqlite watchdog state after %d attempts: %w", maxAttempts, changedErr)
}

func loadSQLiteWatchdogStateFileReadOnlyAttempt(ctx context.Context, path string, immutable bool) (State, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	if immutable {
		query.Set("immutable", "1")
	}
	db, err := sql.Open("sqlite", sqliteFileURI(path, query))
	if err != nil {
		return State{}, err
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return State{}, err
	}
	defer conn.Close()
	if sqliteRuntimeMetadataConnectionTestHook != nil {
		sqliteRuntimeMetadataConnectionTestHook()
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA query_only = ON`); err != nil {
		return State{}, err
	}
	if err := validateSQLiteWatchdogTablesContext(ctx, conn); err != nil {
		return State{}, err
	}

	state := State{ChatPolls: make(map[string]ChatPollState)}
	var controlChatJSON string
	if err := conn.QueryRowContext(
		ctx,
		`SELECT CASE WHEN json_valid(value) THEN COALESCE(json_extract(value, '$.control_chat'), '{}') ELSE '{}' END FROM state_meta WHERE key = 'state_json'`,
	).Scan(&controlChatJSON); err != nil {
		return State{}, err
	}
	if err := json.Unmarshal([]byte(controlChatJSON), &state.ControlChat); err != nil {
		return State{}, err
	}

	rows, err := conn.QueryContext(ctx, `SELECT key, json FROM runtime_state WHERE key IN (?, ?, ?, ?, ?)`,
		sqliteRuntimeKeyScope,
		sqliteRuntimeKeyServiceOwner,
		sqliteRuntimeKeyLockOwner,
		sqliteRuntimeKeyServiceControl,
		sqliteRuntimeKeyUpgrade,
	)
	if err != nil {
		return State{}, err
	}
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			_ = rows.Close()
			return State{}, err
		}
		switch key {
		case sqliteRuntimeKeyScope:
			err = json.Unmarshal(raw, &state.Scope)
		case sqliteRuntimeKeyServiceOwner:
			err = json.Unmarshal(raw, &state.ServiceOwner)
		case sqliteRuntimeKeyLockOwner:
			err = json.Unmarshal(raw, &state.LockOwner)
		case sqliteRuntimeKeyServiceControl:
			err = json.Unmarshal(raw, &state.ServiceControl)
		case sqliteRuntimeKeyUpgrade:
			err = json.Unmarshal(raw, &state.Upgrade)
		}
		if err != nil {
			_ = rows.Close()
			return State{}, err
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return State{}, err
	}
	if err := rows.Close(); err != nil {
		return State{}, err
	}

	controlChatID := strings.TrimSpace(state.ControlChat.TeamsChatID)
	if controlChatID == "" {
		return state, nil
	}
	pollRows, err := conn.QueryContext(ctx, `SELECT json FROM chat_polls WHERE chat_id = ?`, controlChatID)
	if err != nil {
		return State{}, err
	}
	defer pollRows.Close()
	for pollRows.Next() {
		var raw []byte
		if err := pollRows.Scan(&raw); err != nil {
			return State{}, err
		}
		var poll ChatPollState
		if err := json.Unmarshal(raw, &poll); err != nil {
			// The watchdog must retain owner/lifecycle evidence even when a
			// business poll projection was written by an older or corrupt helper.
			// A missing control-poll hint is safer than aborting the entire
			// watchdog read and accidentally treating the service as unreadable.
			return state, nil
		}
		if strings.TrimSpace(poll.ChatID) == "" {
			poll.ChatID = controlChatID
		}
		state.ChatPolls[poll.ChatID] = poll
	}
	if err := pollRows.Err(); err != nil {
		return State{}, err
	}
	return state, nil
}

func validateSQLiteRuntimeMetadataTablesContext(ctx context.Context, conn *sql.Conn) error {
	var count int
	if err := conn.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('state_meta', 'runtime_state')`,
	).Scan(&count); err != nil {
		return err
	}
	if count != 2 {
		return fmt.Errorf("sqlite teams store is missing runtime metadata tables")
	}
	return nil
}

func validateSQLiteWatchdogTablesContext(ctx context.Context, conn *sql.Conn) error {
	var count int
	if err := conn.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('state_meta', 'runtime_state', 'chat_polls')`,
	).Scan(&count); err != nil {
		return err
	}
	if count != 3 {
		return fmt.Errorf("sqlite teams store is missing watchdog state tables")
	}
	return nil
}

type sqliteReadOnlyFileIdentity struct {
	Exists  bool
	Size    int64
	ModTime int64
}

func sqliteReadOnlyFileIdentityForPath(path string) (sqliteReadOnlyFileIdentity, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return sqliteReadOnlyFileIdentity{}, nil
	}
	if err != nil {
		return sqliteReadOnlyFileIdentity{}, err
	}
	reparse, err := sqliteStorePathIsReparsePoint(path, info)
	if err != nil {
		return sqliteReadOnlyFileIdentity{}, err
	}
	if reparse || !info.Mode().IsRegular() {
		return sqliteReadOnlyFileIdentity{}, fmt.Errorf("sqlite store path is not a regular file: %s", path)
	}
	return sqliteReadOnlyFileIdentity{Exists: true, Size: info.Size(), ModTime: info.ModTime().UnixNano()}, nil
}

func requireSQLiteReadOnlySHM(path string) error {
	identity, err := sqliteReadOnlyFileIdentityForPath(path + "-shm")
	if err != nil {
		return err
	}
	if !identity.Exists {
		return fmt.Errorf("read live sqlite WAL without creating SHM: %w", os.ErrNotExist)
	}
	return nil
}

func loadSQLiteStateFileReadOnlyWithHook(ctx context.Context, path string, afterSnapshot func(attempt int, immutable bool)) (State, error) {
	const maxAttempts = 3
	var changedErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		dbBefore, err := sqliteReadOnlyFileIdentityForPath(path)
		if err != nil {
			return State{}, err
		}
		if !dbBefore.Exists {
			return State{}, os.ErrNotExist
		}
		walBefore, err := sqliteReadOnlyFileIdentityForPath(path + "-wal")
		if err != nil {
			return State{}, err
		}
		immutable := !walBefore.Exists || walBefore.Size == 0
		if !immutable {
			if err := requireSQLiteReadOnlySHM(path); err != nil {
				return State{}, err
			}
		}
		if afterSnapshot != nil {
			afterSnapshot(attempt, immutable)
		}
		state, err := loadSQLiteStateFileReadOnlyAttempt(ctx, path, immutable)
		if err != nil {
			return State{}, err
		}
		if !immutable {
			return state, nil
		}
		dbAfter, err := sqliteReadOnlyFileIdentityForPath(path)
		if err != nil {
			return State{}, err
		}
		walAfter, err := sqliteReadOnlyFileIdentityForPath(path + "-wal")
		if err != nil {
			return State{}, err
		}
		if dbBefore == dbAfter && walBefore == walAfter {
			return state, nil
		}
		changedErr = fmt.Errorf("database or WAL changed during immutable attempt %d", attempt+1)
	}
	return State{}, fmt.Errorf("read stable sqlite diagnostic snapshot after %d attempts: %w", maxAttempts, changedErr)
}

func loadSQLiteStateFileReadOnlyAttempt(ctx context.Context, path string, immutable bool) (State, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	if immutable {
		query.Set("immutable", "1")
	}
	db, err := sql.Open("sqlite", sqliteFileURI(path, query))
	if err != nil {
		return State{}, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	defer db.Close()
	if _, err := db.ExecContext(ctx, `PRAGMA query_only = ON`); err != nil {
		return State{}, err
	}
	if err := validateSQLiteRequiredTables(db); err != nil {
		return State{}, err
	}
	return loadSQLiteStateRows(ctx, db)
}

func loadSQLiteStateFileOfflineRecoveryReadOnly(ctx context.Context, path string) (State, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	recoveryNeeded, err := sqliteOfflineRecoveryNeeded(path)
	if err != nil {
		return State{}, err
	}
	if !recoveryNeeded {
		return loadSQLiteStateFileReadOnly(ctx, path)
	}
	reader, err := openSQLiteOfflineRecoveryReader(ctx, path)
	if err != nil {
		return State{}, err
	}
	if err := validateSQLiteRequiredTablesContext(ctx, reader.readConn); err != nil {
		_ = reader.Close()
		return State{}, err
	}
	state, loadErr := loadSQLiteStateRows(ctx, reader.readConn)
	closeErr := reader.Close()
	if loadErr != nil {
		return State{}, loadErr
	}
	if closeErr != nil {
		return State{}, closeErr
	}
	return state, nil
}

func sqliteOfflineRecoveryNeeded(path string) (bool, error) {
	if err := validateExistingSQLiteStorePath(path); err != nil {
		return false, err
	}
	walInfo, err := sqliteReadOnlyFileIdentityForPath(path + "-wal")
	if err != nil {
		return false, err
	}
	if !walInfo.Exists {
		return false, nil
	}
	if walInfo.Size == 0 {
		return false, nil
	}
	shmInfo, err := sqliteReadOnlyFileIdentityForPath(path + "-shm")
	if err != nil {
		return false, err
	}
	if !shmInfo.Exists {
		return true, nil
	}
	return false, nil
}

func configureSQLiteOfflineRecoveryConnection(ctx context.Context, conn *sql.Conn) error {
	if err := conn.Raw(func(driverConn any) error {
		control, ok := driverConn.(sqlite.FileControl)
		if !ok {
			return errors.New("sqlite driver does not support persistent WAL control")
		}
		_, err := control.FileControlPersistWAL("main", 1)
		return err
	}); err != nil {
		return err
	}
	_, err := conn.ExecContext(ctx, `PRAGMA query_only = ON`)
	return err
}

type sqliteOfflineRecoveryReader struct {
	anchorDB   *sql.DB
	anchorConn *sql.Conn
	readDB     *sql.DB
	readConn   *sql.Conn
}

func openSQLiteOfflineRecoveryReader(ctx context.Context, path string) (*sqliteOfflineRecoveryReader, error) {
	if err := validateExistingSQLiteStorePath(path); err != nil {
		return nil, err
	}
	reader := &sqliteOfflineRecoveryReader{}
	var err error
	reader.anchorDB, err = openSQLiteHandle(path, false)
	if err != nil {
		return nil, err
	}
	reader.anchorConn, err = reader.anchorDB.Conn(ctx)
	if err != nil {
		_ = reader.anchorDB.Close()
		return nil, err
	}
	if err := configureSQLiteOfflineRecoveryConnection(ctx, reader.anchorConn); err != nil {
		_ = reader.anchorConn.Close()
		_ = reader.anchorDB.Close()
		return nil, err
	}

	query := url.Values{}
	query.Set("mode", "ro")
	reader.readDB, err = sql.Open("sqlite", sqliteFileURI(path, query))
	if err != nil {
		_ = reader.anchorConn.Close()
		_ = reader.anchorDB.Close()
		return nil, err
	}
	reader.readDB.SetMaxOpenConns(1)
	reader.readDB.SetMaxIdleConns(0)
	reader.readConn, err = reader.readDB.Conn(ctx)
	if err != nil {
		_ = reader.readDB.Close()
		_ = reader.anchorConn.Close()
		_ = reader.anchorDB.Close()
		return nil, err
	}
	if _, err := reader.readConn.ExecContext(ctx, `PRAGMA query_only = ON`); err != nil {
		_ = reader.Close()
		return nil, err
	}
	return reader, nil
}

func (r *sqliteOfflineRecoveryReader) Close() error {
	if r == nil {
		return nil
	}
	var first error
	for _, closeFn := range []func() error{
		func() error {
			if r.anchorConn == nil {
				return nil
			}
			return r.anchorConn.Close()
		},
		func() error {
			if r.anchorDB == nil {
				return nil
			}
			return r.anchorDB.Close()
		},
		func() error {
			if r.readConn == nil {
				return nil
			}
			return r.readConn.Close()
		},
		func() error {
			if r.readDB == nil {
				return nil
			}
			return r.readDB.Close()
		},
	} {
		if err := closeFn(); first == nil && err != nil {
			first = err
		}
	}
	return first
}

func (s *Store) writeSQLiteStateFile(path string, state State) error {
	db, err := openSQLiteStore(path, true)
	if err != nil {
		return err
	}
	if err := ensureSQLiteSchema(db); err != nil {
		_ = db.Close()
		return err
	}
	if err := writeSQLiteState(context.Background(), db, state); err != nil {
		_ = db.Close()
		return err
	}
	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	if err := removeSQLiteSidecarFiles(path); err != nil {
		return err
	}
	return os.Chmod(path, fileMode)
}

func removeSQLiteSidecarFiles(path string) error {
	for _, suffix := range []string{"-wal", "-shm"} {
		sidecar := path + suffix
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove sqlite sidecar %q: %w", sidecar, err)
		}
	}
	return nil
}

func openExistingSQLiteStore(path string) (*sql.DB, error) {
	if err := validateExistingSQLiteStorePath(path); err != nil {
		return nil, err
	}
	db, err := openSQLiteHandle(path, false)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		db.Close()
		return nil, err
	}
	if err := validateSQLiteStoreInitialized(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := validateSQLiteRequiredTables(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := configureSQLiteStore(db, path); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// openExistingSQLiteRuntimeStore opens the liveness handle without changing
// journal mode, synchronous mode, permissions, or other database settings.
// Those setup operations belong to migration/open, not to a heartbeat racing
// with normal business writes.
func openExistingSQLiteRuntimeStore(path string) (*sql.DB, error) {
	if err := validateExistingSQLiteStorePath(path); err != nil {
		return nil, err
	}
	db, err := openSQLiteHandle(path, false)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		db.Close()
		return nil, err
	}
	if err := validateSQLiteStoreInitialized(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := validateSQLiteRequiredTables(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func validateExistingSQLiteStorePath(path string) error {
	identity, err := sqliteReadOnlyFileIdentityForPath(path)
	if err != nil {
		return err
	}
	if !identity.Exists {
		return fmt.Errorf("sqlite store %q does not exist", path)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := sqliteReadOnlyFileIdentityForPath(path + suffix); err != nil {
			return err
		}
	}
	return nil
}

func openSQLiteStore(path string, create bool) (*sql.DB, error) {
	db, err := openSQLiteHandle(path, create)
	if err != nil {
		return nil, err
	}
	if err := configureSQLiteStore(db, path); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func openSQLiteHandle(path string, create bool) (*sql.DB, error) {
	query := url.Values{}
	if create {
		query.Set("mode", "rwc")
	} else {
		query.Set("mode", "rw")
	}
	dsn := sqliteFileURI(path, query)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func sqliteFileURI(path string, query url.Values) string {
	u := url.URL{Scheme: "file"}
	if runtime.GOOS == "windows" {
		u = sqliteWindowsFileURL(path)
	} else {
		u.Path = path
	}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	return u.String()
}

func sqliteWindowsFileURL(path string) url.URL {
	slash := strings.ReplaceAll(path, `\`, `/`)
	if strings.HasPrefix(slash, "//") {
		trimmed := strings.TrimLeft(slash, "/")
		host, rest, ok := strings.Cut(trimmed, "/")
		if ok {
			return url.URL{Scheme: "file", Host: host, Path: "/" + rest}
		}
		return url.URL{Scheme: "file", Path: slash}
	}
	if len(slash) >= 2 && slash[1] == ':' {
		slash = "/" + slash
	}
	return url.URL{Scheme: "file", Path: slash}
}

func configureSQLiteStore(db *sql.DB, path string) error {
	for _, stmt := range []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		fmt.Sprintf(`PRAGMA wal_autocheckpoint = %d`, sqliteWALAutocheckpointPages),
		`PRAGMA temp_store = MEMORY`,
		`PRAGMA busy_timeout = 5000`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	chmodSQLiteStoreFiles(path)
	return nil
}

func chmodSQLiteStoreFiles(path string) {
	_ = os.Chmod(path, fileMode)
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); err == nil {
			_ = os.Chmod(path+suffix, fileMode)
		}
	}
}

func validateSQLiteStoreInitialized(db *sql.DB) error {
	var raw []byte
	err := db.QueryRow(`SELECT value FROM state_meta WHERE key = 'state_json'`).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("sqlite teams store is missing state metadata")
		}
		return fmt.Errorf("sqlite teams store is not initialized: %w", err)
	}
	if len(raw) == 0 {
		return errors.New("sqlite teams store has empty state metadata")
	}
	return nil
}

var sqliteRequiredTables = []string{
	"state_meta",
	"sessions",
	"inbound_events",
	"turns",
	"outbox_messages",
	"message_provenance",
	"chat_polls",
	"chat_rate_limits",
}

func validateSQLiteRequiredTables(db *sql.DB) error {
	return validateSQLiteRequiredTablesContext(context.Background(), db)
}

func validateSQLiteRequiredTablesContext(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	for _, table := range sqliteRequiredTables {
		var name string
		err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("sqlite teams store is missing required table %q", table)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func sqliteTableExists(db *sql.DB, table string) (bool, error) {
	table = strings.TrimSpace(table)
	if table == "" {
		return false, nil
	}
	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return name != "", nil
}

func sqliteChatSequencesEmpty(db *sql.DB) (bool, error) {
	var one int
	err := db.QueryRow(`SELECT 1 FROM chat_sequences LIMIT 1`).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func ensureSQLiteSchema(db *sql.DB) error {
	chatSequencesExisted, err := sqliteTableExists(db, "chat_sequences")
	if err != nil {
		return err
	}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS state_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS runtime_state (key TEXT PRIMARY KEY, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, teams_chat_id TEXT, status TEXT, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS sessions_chat_idx ON sessions(teams_chat_id)`,
		`CREATE TABLE IF NOT EXISTS inbound_events (id TEXT PRIMARY KEY, session_id TEXT, teams_chat_id TEXT, teams_message_id TEXT, status TEXT, created_at INTEGER, updated_at INTEGER, received_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS inbound_session_idx ON inbound_events(session_id, status, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS inbound_status_idx ON inbound_events(status, teams_chat_id, created_at, teams_message_id)`,
		`CREATE INDEX IF NOT EXISTS inbound_message_idx ON inbound_events(teams_chat_id, teams_message_id)`,
		`CREATE TABLE IF NOT EXISTS turns (id TEXT PRIMARY KEY, session_id TEXT, status TEXT, queued_at INTEGER, created_at INTEGER, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS turns_ready_idx ON turns(status, session_id, queued_at, id)`,
		`CREATE INDEX IF NOT EXISTS turns_session_status_idx ON turns(session_id, status, queued_at, id)`,
		`CREATE TABLE IF NOT EXISTS outbox_messages (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, teams_chat_id TEXT, teams_message_id TEXT, status TEXT, sequence INTEGER, created_at INTEGER, deliver_after INTEGER, post_send_effects_pending INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_messages(status, teams_chat_id, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_pending_due_idx ON outbox_messages(status, deliver_after, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_session_idx ON outbox_messages(session_id, status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS message_provenance (id TEXT PRIMARY KEY, teams_chat_id TEXT, teams_message_id TEXT, origin TEXT, session_id TEXT, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS message_provenance_lookup_idx ON message_provenance(teams_chat_id, teams_message_id, origin)`,
		`CREATE TABLE IF NOT EXISTS chat_polls (chat_id TEXT PRIMARY KEY, next_poll_at INTEGER, blocked_until INTEGER, poll_state TEXT, last_activity_at INTEGER, park_notice_sent_at INTEGER, parked_skip_eligible INTEGER, frontier_active INTEGER, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS chat_sequences (chat_id TEXT PRIMARY KEY, next_sequence INTEGER, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS chat_rate_limits (chat_id TEXT PRIMARY KEY, blocked_until INTEGER, json BLOB NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS import_checkpoints (id TEXT PRIMARY KEY, session_id TEXT, status TEXT, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS import_checkpoints_session_idx ON import_checkpoints(session_id, status, updated_at, id)`,
		`CREATE TABLE IF NOT EXISTS transcript_ledger (id TEXT PRIMARY KEY, session_id TEXT, imported_at INTEGER, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS transcript_ledger_session_idx ON transcript_ledger(session_id, imported_at, id)`,
		`CREATE TABLE IF NOT EXISTS transcript_deliveries (id TEXT PRIMARY KEY, session_id TEXT, outbox_id TEXT, status TEXT, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS transcript_deliveries_session_idx ON transcript_deliveries(session_id, status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS helper_deliveries (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, outbox_id TEXT, status TEXT, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS helper_deliveries_session_idx ON helper_deliveries(session_id, status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS artifact_records (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, outbox_id TEXT, status TEXT, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS artifact_records_session_idx ON artifact_records(session_id, status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS notifications (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, status TEXT, created_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS notifications_session_idx ON notifications(session_id, status, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS notifications_status_idx ON notifications(status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS fork_operations (id TEXT PRIMARY KEY, parent_session_id TEXT, child_session_id TEXT, phase TEXT, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS fork_operations_parent_idx ON fork_operations(parent_session_id, phase, updated_at, id)`,
		`CREATE INDEX IF NOT EXISTS fork_operations_phase_idx ON fork_operations(phase, updated_at, id)`,
		`CREATE TABLE IF NOT EXISTS fork_history_items (id TEXT PRIMARY KEY, operation_id TEXT, ordinal INTEGER, delivery_status TEXT, updated_at INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS fork_history_operation_idx ON fork_history_items(operation_id, ordinal, id)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	// Keep a tiny durable epoch for the cold state row. The trigger is part of
	// the SQLite file, so an older helper that only knows state_json still bumps
	// the epoch and cannot silently leave a newer history-watch projection in
	// front of its write.
	for _, stmt := range []string{
		`CREATE TRIGGER IF NOT EXISTS state_json_revision_insert
AFTER INSERT ON state_meta
WHEN NEW.key = 'state_json'
BEGIN
  INSERT INTO state_meta(key, value) VALUES ('state_json_revision', '1')
  ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(COALESCE(value, '0') AS INTEGER) + 1 AS TEXT);
END`,
		`CREATE TRIGGER IF NOT EXISTS state_json_revision_update
AFTER UPDATE OF value ON state_meta
WHEN NEW.key = 'state_json'
BEGIN
  INSERT INTO state_meta(key, value) VALUES ('state_json_revision', '1')
  ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(COALESCE(value, '0') AS INTEGER) + 1 AS TEXT);
END`,
		`INSERT OR IGNORE INTO state_meta(key, value) VALUES ('state_json_revision', '1')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	// frontier_active is an indexed admission hint, never an independent
	// source of truth.  Current writers update it atomically with the canonical
	// poll JSON, but older helpers and narrow repair tools may update only one
	// side.  Repair the hint at the SQLite boundary so a stale value cannot hide
	// a due ordinary chat behind the operational lane.  The trigger is inert for
	// a coherent write and therefore does not add JSON work to the listener's
	// read path; it only runs when a row is inserted or one of these two columns
	// is directly changed.
	frontierHint := sqliteChatPollOperationalFrontierSQL("NEW.json")
	for _, stmt := range []string{
		`CREATE TRIGGER IF NOT EXISTS chat_polls_frontier_hint_repair_insert
AFTER INSERT ON chat_polls
WHEN json_valid(NEW.json)
 AND COALESCE(NEW.frontier_active, 0) != ` + frontierHint + `
BEGIN
  UPDATE chat_polls
  SET frontier_active = ` + frontierHint + `
  WHERE chat_id = NEW.chat_id;
END`,
		`CREATE TRIGGER IF NOT EXISTS chat_polls_frontier_hint_repair_update
AFTER UPDATE OF json, frontier_active ON chat_polls
WHEN json_valid(NEW.json)
 AND COALESCE(NEW.frontier_active, 0) != ` + frontierHint + `
BEGIN
  UPDATE chat_polls
  SET frontier_active = ` + frontierHint + `
  WHERE chat_id = NEW.chat_id;
END`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`ALTER TABLE outbox_messages ADD COLUMN teams_message_id TEXT`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE outbox_messages ADD COLUMN turn_id TEXT`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE outbox_messages ADD COLUMN post_send_effects_pending INTEGER`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if _, err := db.Exec(`UPDATE outbox_messages
SET post_send_effects_pending = CASE
  WHEN CASE WHEN json_valid(json) THEN COALESCE(json_extract(json, '$.post_send_effects_pending'), 0) ELSE 0 END = 1 THEN 1
  ELSE 0
END
WHERE post_send_effects_pending IS NULL`); err != nil {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE inbound_events ADD COLUMN received_at INTEGER`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE chat_polls ADD COLUMN park_notice_sent_at INTEGER`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE chat_polls ADD COLUMN parked_skip_eligible INTEGER`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE chat_polls ADD COLUMN last_activity_at INTEGER`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE chat_polls ADD COLUMN frontier_active INTEGER`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE chat_polls ADD COLUMN blocked_until INTEGER`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if err := backfillSQLiteChatPollDerivedColumns(db); err != nil {
		return err
	}
	if err := ensureSQLiteRuntimeProjectionMarker(db); err != nil {
		return err
	}
	if err := backfillSQLiteInboundDerivedColumns(db); err != nil {
		return err
	}
	shouldBackfillChatSequences := !chatSequencesExisted
	if !shouldBackfillChatSequences {
		chatSequencesEmpty, err := sqliteChatSequencesEmpty(db)
		if err != nil {
			return err
		}
		shouldBackfillChatSequences = chatSequencesEmpty
	}
	if shouldBackfillChatSequences {
		if err := backfillSQLiteChatSequences(db); err != nil {
			return err
		}
	}
	for _, stmt := range []string{
		`CREATE INDEX IF NOT EXISTS inbound_session_created_idx ON inbound_events(session_id, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS inbound_session_received_idx ON inbound_events(session_id, received_at, id) WHERE received_at > 0`,
		`CREATE INDEX IF NOT EXISTS outbox_turn_idx ON outbox_messages(turn_id, status, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_message_lookup_idx ON outbox_messages(teams_chat_id, teams_message_id, status)`,
		`CREATE INDEX IF NOT EXISTS outbox_chat_sequence_idx ON outbox_messages(teams_chat_id, sequence, status, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_side_effects_idx ON outbox_messages(post_send_effects_pending, status, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_side_effects_due_idx ON outbox_messages(post_send_effects_pending, status, deliver_after, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS chat_polls_parked_skip_idx ON chat_polls(parked_skip_eligible, chat_id)`,
		`CREATE INDEX IF NOT EXISTS chat_polls_auto_park_idx ON chat_polls(last_activity_at, chat_id) WHERE parked_skip_eligible = 0 AND last_activity_at > 0 AND poll_state IN ('cold', 'parked')`,
		`CREATE INDEX IF NOT EXISTS chat_polls_frontier_due_idx ON chat_polls(frontier_active, next_poll_at, blocked_until, updated_at, chat_id)`,
		`CREATE INDEX IF NOT EXISTS chat_polls_ordinary_due_idx ON chat_polls(parked_skip_eligible, next_poll_at, updated_at, chat_id) WHERE frontier_active = 0`,
		`CREATE INDEX IF NOT EXISTS transcript_deliveries_outbox_idx ON transcript_deliveries(outbox_id)`,
		`CREATE INDEX IF NOT EXISTS helper_deliveries_outbox_idx ON helper_deliveries(outbox_id)`,
		`CREATE INDEX IF NOT EXISTS artifact_records_outbox_idx ON artifact_records(outbox_id)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func coldSQLiteState(state State) State {
	cold := state
	cold.Sessions = nil
	cold.Turns = nil
	cold.InboundEvents = nil
	cold.OutboxMessages = nil
	cold.MessageProvenance = nil
	cold.ChatPolls = nil
	cold.ChatSequences = nil
	cold.ChatRateLimits = nil
	cold.TranscriptLedger = nil
	cold.TranscriptDeliveries = nil
	cold.HelperDeliveries = nil
	cold.ImportCheckpoints = nil
	cold.ArtifactRecords = nil
	cold.Notifications = nil
	cold.ForkOperations = nil
	cold.ForkHistoryItems = nil
	return cold
}

func writeSQLiteState(ctx context.Context, db *sql.DB, state State) error {
	state.ensure(time.Time{})
	for key, checkpoint := range state.ImportCheckpoints {
		checkpointKey := strings.TrimSpace(key)
		checkpointID := strings.TrimSpace(checkpoint.ID)
		if checkpointKey != "" && checkpointID != "" && checkpointKey != checkpointID {
			return fmt.Errorf("%w: checkpoint row id %q is keyed as %q", ErrSessionStateProvenanceMismatch, checkpointID, checkpointKey)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	opaqueCheckpoints, err := captureOpaqueSQLiteCheckpointRows(ctx, tx)
	if err != nil {
		return err
	}
	opaqueOutbox, err := captureOpaqueSQLiteOutboxRows(ctx, tx)
	if err != nil {
		return err
	}
	opaqueSessions, err := captureOpaqueSQLiteSessionRows(ctx, tx)
	if err != nil {
		return err
	}
	opaqueChatPolls, err := captureOpaqueSQLiteChatPollRows(ctx, tx)
	if err != nil {
		return err
	}
	// A runtime heartbeat uses a dedicated handle and may commit after this
	// State snapshot was loaded but before the full-state writer reaches the
	// runtime projection. Preserve newer liveness rows so an unrelated cold
	// update cannot roll the watchdog evidence back. Explicit owner/lease
	// mutations use their targeted APIs; this merge is for stale snapshots.
	if err := preserveNewerSQLiteLivenessRows(ctx, tx, &state); err != nil {
		return err
	}
	for _, table := range []string{"state_meta", "runtime_state", "sessions", "inbound_events", "turns", "outbox_messages", "message_provenance", "chat_polls", "chat_sequences", "chat_rate_limits", "import_checkpoints", "transcript_ledger", "transcript_deliveries", "helper_deliveries", "artifact_records", "notifications", "fork_operations", "fork_history_items"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return err
		}
	}
	cold, err := json.Marshal(coldSQLiteState(state))
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES ('state_json', ?)`, cold); err != nil {
		return err
	}
	stateJSONRevision, err := sqliteStateJSONRevision(ctx, tx)
	if err != nil {
		return err
	}
	if err := upsertSQLiteHistoryWatchProjectionTx(ctx, tx, state.HistoryWatch, state.HistoryWatchReady, stateJSONRevision); err != nil {
		return err
	}
	if err := saveSQLiteRuntimeStateTx(ctx, tx, state); err != nil {
		return err
	}
	if err := writeSQLiteSessionsPreservingOpaque(ctx, tx, state.Sessions, opaqueSessions); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO inbound_events(id, session_id, teams_chat_id, teams_message_id, status, created_at, updated_at, received_at, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, state.InboundEvents, func(v InboundEvent) []any {
		return []any{v.ID, v.SessionID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), string(v.Status), sqliteTime(v.CreatedAt), sqliteTime(v.UpdatedAt), sqliteTime(v.ReceivedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO turns(id, session_id, status, queued_at, created_at, updated_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)`, state.Turns, func(v Turn) []any {
		return []any{v.ID, v.SessionID, string(v.Status), sqliteTime(queuedTurnSortTime(v)), sqliteTime(v.CreatedAt), sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteOutboxPreservingOpaque(ctx, tx, state.OutboxMessages, opaqueOutbox); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO message_provenance(id, teams_chat_id, teams_message_id, origin, session_id, json) VALUES (?, ?, ?, ?, ?, ?)`, state.MessageProvenance, func(v MessageProvenanceRecord) []any {
		return []any{v.ID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), v.Origin, v.SessionID}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO chat_polls(chat_id, next_poll_at, blocked_until, poll_state, last_activity_at, park_notice_sent_at, parked_skip_eligible, frontier_active, updated_at, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, state.ChatPolls, func(v ChatPollState) []any {
		return []any{v.ChatID, sqliteTime(v.NextPollAt), sqliteTime(v.BlockedUntil), v.PollState, sqliteTime(v.LastActivityAt), sqliteTime(v.ParkNoticeSentAt), sqliteBool(chatPollParkedSkipEligible(v)), sqliteBool(chatPollHasOperationalFrontier(v)), sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteChatPollsPreservingOpaque(ctx, tx, state.ChatPolls, opaqueChatPolls); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO chat_sequences(chat_id, next_sequence, updated_at, json) VALUES (?, ?, ?, ?)`, state.ChatSequences, func(v ChatSequenceState) []any {
		return []any{v.ChatID, v.Next, sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO chat_rate_limits(chat_id, blocked_until, json) VALUES (?, ?, ?)`, state.ChatRateLimits, func(v ChatRateLimitState) []any {
		return []any{v.ChatID, sqliteTime(v.BlockedUntil)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteImportCheckpointsPreservingOpaque(ctx, tx, state.ImportCheckpoints, opaqueCheckpoints); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO transcript_ledger(id, session_id, imported_at, created_at, json) VALUES (?, ?, ?, ?, ?)`, state.TranscriptLedger, func(v TranscriptLedgerRecord) []any {
		return []any{v.ID, v.SessionID, sqliteTime(v.ImportedAt), sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO transcript_deliveries(id, session_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?)`, state.TranscriptDeliveries, func(v TranscriptDeliveryRecord) []any {
		return []any{v.ID, v.SessionID, v.OutboxID, string(v.Status), sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO helper_deliveries(id, session_id, turn_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)`, state.HelperDeliveries, func(v HelperDeliveryRecord) []any {
		return []any{v.ID, v.SessionID, v.TurnID, v.OutboxID, string(v.Status), sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO artifact_records(id, session_id, turn_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)`, state.ArtifactRecords, func(v ArtifactRecord) []any {
		return []any{v.ID, v.SessionID, v.TurnID, v.OutboxID, v.Status, sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO notifications(id, session_id, turn_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?)`, state.Notifications, func(v NotificationRecord) []any {
		return []any{v.ID, v.SessionID, v.TurnID, string(v.Status), sqliteTime(v.CreatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO fork_operations(id, parent_session_id, child_session_id, phase, updated_at, json) VALUES (?, ?, ?, ?, ?, ?)`, state.ForkOperations, func(v ForkOperation) []any {
		return []any{v.ID, v.ParentSessionID, v.ChildSessionID, string(v.Phase), sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO fork_history_items(id, operation_id, ordinal, delivery_status, updated_at, json) VALUES (?, ?, ?, ?, ?, ?)`, state.ForkHistoryItems, func(v ForkHistoryItem) []any {
		return []any{v.ID, v.OperationID, v.Ordinal, string(v.DeliveryStatus), sqliteTime(v.UpdatedAt)}
	}); err != nil {
		return err
	}
	return tx.Commit()
}

func preserveNewerSQLiteLivenessRows(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, state *State) error {
	if state == nil {
		return nil
	}
	if err := preserveNewerSQLiteChatPollRows(ctx, q, state); err != nil {
		return err
	}
	var currentOwners [2]*OwnerMetadata
	ownerRowPresent := false
	nullOwnerRows := 0
	for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
		var ownerRaw []byte
		err := q.QueryRowContext(ctx, `SELECT json FROM runtime_state WHERE key = ?`, key).Scan(&ownerRaw)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		ownerRowPresent = true
		var current *OwnerMetadata
		if err := json.Unmarshal(ownerRaw, &current); err != nil {
			return err
		}
		if current == nil {
			nullOwnerRows++
			continue
		}
		if key == sqliteRuntimeKeyServiceOwner {
			currentOwners[0] = current
		} else {
			currentOwners[1] = current
		}
	}
	var leaseRaw []byte
	err := q.QueryRowContext(ctx, `SELECT json FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyControlLease).Scan(&leaseRaw)
	leasePresent := err == nil
	var currentLease ControlLease
	if err == nil {
		if bytes.Equal(bytes.TrimSpace(leaseRaw), []byte("null")) {
			return fmt.Errorf("%w: invalid sqlite control lease row: null", ErrControlLeaseStateUntrusted)
		}
		if err := json.Unmarshal(leaseRaw, &currentLease); err != nil {
			return err
		}
		if err := validateControlLeaseShape(currentLease); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	var latestOwner *OwnerMetadata
	var currentOwner *OwnerMetadata
	for _, candidate := range currentOwners {
		if candidate == nil {
			continue
		}
		if latestOwner == nil || candidate.LastHeartbeat.After(latestOwner.LastHeartbeat) {
			latestOwner = candidate
		}
	}
	if leasePresent && strings.TrimSpace(currentLease.HolderMachineID) != "" && !currentLease.LeaseUntil.IsZero() {
		// An active lease is the authority. Prefer the owner projection that
		// proves this exact holder/generation; the other compatibility copy may
		// be stale and will be repaired by the next coherent write.
		for _, candidate := range currentOwners {
			if candidate == nil || !ownerMatchesControlLease(*candidate, currentLease) {
				continue
			}
			if currentOwner == nil || candidate.LastHeartbeat.After(currentOwner.LastHeartbeat) {
				currentOwner = candidate
			}
		}
	}
	activeLease := leasePresent && strings.TrimSpace(currentLease.HolderMachineID) != "" && !currentLease.LeaseUntil.IsZero()
	if currentOwner == nil && !activeLease {
		// With no active lease, owner rows retain their legacy diagnostic meaning.
		// Pick the freshest copy only after the active-lease pairing attempt above;
		// this preserves released/pre-generation owner state without inventing a
		// new owner for an active lease it cannot prove.
		currentOwner = latestOwner
	}
	stateOwner, stateHasOwner := state.readOwner()
	ownerChanged := false
	ownerForComparison := currentOwner
	if ownerForComparison == nil {
		// For an active lease with no matching owner witness, retain the latest
		// row only as contradiction evidence. It must not be used for the state
		// overlay below, but it must prevent a cold writer from accepting a
		// mixed-generation tuple.
		ownerForComparison = latestOwner
	}
	if ownerForComparison != nil {
		ownerChanged = !stateHasOwner || ownerForComparison.LastHeartbeat.After(stateOwner.LastHeartbeat) ||
			!reflect.DeepEqual(*ownerForComparison, stateOwner)
	}
	ownerCleared := ownerRowPresent && nullOwnerRows == 2
	leaseChanged := leasePresent && (currentLease.Generation > state.ControlLease.Generation ||
		currentLease.UpdatedAt.After(state.ControlLease.UpdatedAt))

	if leasePresent && strings.TrimSpace(currentLease.HolderMachineID) != "" && !currentLease.LeaseUntil.IsZero() {
		if ownerCleared {
			// ClearOwner writes both NULL owner rows deliberately. Preserve that
			// tombstone even if a lease heartbeat was committed separately.
			if leaseChanged {
				state.ControlLease = currentLease
			}
			state.ServiceOwner = nil
			state.LockOwner = nil
		} else if ownerChanged || leaseChanged {
			if currentOwner == nil {
				return fmt.Errorf("%w: SQLite owner and control lease projections do not form a coherent liveness tuple", ErrControlLeaseStateUntrusted)
			}
			state.ControlLease = currentLease
			state.writeOwner(*currentOwner)
		}
	} else if leasePresent {
		if leaseChanged {
			state.ControlLease = currentLease
		}
		if ownerCleared {
			state.ServiceOwner = nil
			state.LockOwner = nil
		} else if ownerChanged && currentOwner != nil {
			state.writeOwner(*currentOwner)
		}
	} else if ownerCleared {
		state.ServiceOwner = nil
		state.LockOwner = nil
	} else if ownerChanged && currentOwner != nil {
		// A missing lease row is a supported legacy shape only when the cold
		// snapshot itself has no active lease. Never combine a new owner with a
		// stale active lease from state_json.
		if strings.TrimSpace(state.ControlLease.HolderMachineID) != "" || !state.ControlLease.LeaseUntil.IsZero() {
			return fmt.Errorf("%w: SQLite control lease projection is missing while owner projection changed", ErrControlLeaseStateUntrusted)
		}
		state.writeOwner(*currentOwner)
	}
	return nil
}

// preserveNewerSQLiteChatPollRows prevents a stale full-state snapshot from
// erasing a targeted frontier/page/attempt update. Chat-poll rows are kept in
// a split table because they are updated on the polling hot path, while cold
// Store.Update still rewrites the state projection. The store-wide UpdatedAt
// is the snapshot watermark: a row newer than it was written after the
// snapshot was read. Equal-revision, equal-time conflicts are ambiguous and
// fail closed instead of choosing a cursor or pending page arbitrarily.
func preserveNewerSQLiteChatPollRows(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, state *State) error {
	if state == nil {
		return nil
	}
	if state.ChatPolls == nil {
		state.ChatPolls = make(map[string]ChatPollState)
	}
	rows, err := q.QueryContext(ctx, `SELECT chat_id, json FROM chat_polls`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var rowChatID string
		var raw []byte
		if err := rows.Scan(&rowChatID, &raw); err != nil {
			return err
		}
		rowChatID = strings.TrimSpace(rowChatID)
		if rowChatID == "" {
			return fmt.Errorf("%w: SQLite chat poll row has no chat id", ErrChatPollStateConflict)
		}
		var current ChatPollState
		if err := json.Unmarshal(raw, &current); err != nil {
			// The row is held as opaque evidence by the surrounding full-state
			// writer. It cannot be used to merge a stale snapshot, but it must
			// not make an unrelated healthy-chat update fail globally.
			continue
		}
		if strings.TrimSpace(current.ChatID) == "" {
			current.ChatID = rowChatID
		}
		if strings.TrimSpace(current.ChatID) != rowChatID {
			continue
		}
		existing, present := state.ChatPolls[rowChatID]
		if !present {
			// A DB-only row is normally a targeted write made after the stale
			// snapshot. Preserve it only when its own watermark proves that
			// ordering; a current full-state deletion has a newer global
			// UpdatedAt and therefore remains authoritative. Operational-looking
			// fields are not sufficient evidence: using them here would resurrect
			// a row that the current snapshot intentionally deleted.
			if current.UpdatedAt.After(state.UpdatedAt) {
				state.ChatPolls[rowChatID] = current
			}
			continue
		}
		if current.PollRevision > existing.PollRevision || current.UpdatedAt.After(existing.UpdatedAt) && current.PollRevision >= existing.PollRevision {
			state.ChatPolls[rowChatID] = current
			continue
		}
		if current.PollRevision == existing.PollRevision && current.UpdatedAt.Equal(existing.UpdatedAt) && !reflect.DeepEqual(current, existing) {
			return fmt.Errorf("%w: chat %q has revision %d with two payloads", ErrChatPollStateConflict, rowChatID, current.PollRevision)
		}
	}
	return rows.Err()
}

func clearSQLiteOwnerRuntimeTx(ctx context.Context, tx *sql.Tx) error {
	for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_state(key, json) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET json = excluded.json`, key, []byte("null")); err != nil {
			return err
		}
	}
	return markSQLiteRuntimeProjectionMaterializedTx(ctx, tx)
}

func (s *Store) clearOwnerSQLite(ctx context.Context) (bool, error) {
	return s.withSQLiteRuntimeDB(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := clearSQLiteOwnerRuntimeTx(ctx, tx); err != nil {
			return err
		}
		return tx.Commit()
	})
}

type opaqueSQLiteCheckpointRow struct {
	ID        string
	IDValid   bool
	SessionID sql.NullString
	Status    sql.NullString
	UpdatedAt sql.NullInt64
	Raw       []byte
}

type opaqueSQLiteChatPollRow struct {
	ChatID string
	Raw    []byte
}

// A session row with invalid JSON or contradictory SQL identity is not a
// runnable SessionContext, but it is still durable evidence.  Keep its exact
// payload across a full-state rewrite so a bad row cannot disappear merely
// because another chat updated its poll schedule.
type opaqueSQLiteSessionRow struct {
	ID          string
	TeamsChatID sql.NullString
	Status      sql.NullString
	UpdatedAt   sql.NullInt64
	Raw         []byte
}

func captureOpaqueSQLiteSessionRows(ctx context.Context, tx *sql.Tx) ([]opaqueSQLiteSessionRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, teams_chat_id, status, updated_at, json FROM sessions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]opaqueSQLiteSessionRow, 0)
	for rows.Next() {
		var row opaqueSQLiteSessionRow
		if err := rows.Scan(&row.ID, &row.TeamsChatID, &row.Status, &row.UpdatedAt, &row.Raw); err != nil {
			return nil, err
		}
		var session SessionContext
		if json.Unmarshal(row.Raw, &session) == nil && sqliteSessionProjectionMatches(row, session) {
			continue
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, rows.Close()
}

func sqliteSessionProjectionMatches(row opaqueSQLiteSessionRow, session SessionContext) bool {
	if strings.TrimSpace(session.ID) == "" || strings.TrimSpace(session.ID) != strings.TrimSpace(row.ID) {
		return false
	}
	if !sqliteNullableStringMatches(row.TeamsChatID, session.TeamsChatID) ||
		!sqliteNullableStringMatches(row.Status, string(session.Status)) ||
		!sqliteNullableTimeMatches(row.UpdatedAt, session.UpdatedAt) {
		return false
	}
	return true
}

// An outbox row with invalid JSON cannot be represented in State, but it must
// not make the store unreadable or disappear during an unrelated full-state
// rewrite. Keep its SQL projection and exact payload as opaque evidence. Hot
// outbox queries explicitly exclude invalid rows, so they are isolated while
// healthy rows in the same chat remain eligible for delivery.
type opaqueSQLiteOutboxRow struct {
	ID                     string
	SessionID              sql.NullString
	TurnID                 sql.NullString
	TeamsChatID            sql.NullString
	TeamsMessageID         sql.NullString
	Status                 sql.NullString
	Sequence               sql.NullInt64
	CreatedAt              sql.NullInt64
	DeliverAfter           sql.NullInt64
	PostSendEffectsPending sql.NullInt64
	Raw                    []byte
}

func captureOpaqueSQLiteOutboxRows(ctx context.Context, tx *sql.Tx) ([]opaqueSQLiteOutboxRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json FROM outbox_messages`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]opaqueSQLiteOutboxRow, 0)
	for rows.Next() {
		var row opaqueSQLiteOutboxRow
		if err := rows.Scan(&row.ID, &row.SessionID, &row.TurnID, &row.TeamsChatID, &row.TeamsMessageID, &row.Status, &row.Sequence, &row.CreatedAt, &row.DeliverAfter, &row.PostSendEffectsPending, &row.Raw); err != nil {
			return nil, err
		}
		var message OutboxMessage
		if json.Unmarshal(row.Raw, &message) == nil && sqliteOutboxProjectionMatches(row, message) {
			continue
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, rows.Close()
}

func writeSQLiteSessionsPreservingOpaque(ctx context.Context, tx *sql.Tx, values map[string]SessionContext, opaque []opaqueSQLiteSessionRow) error {
	keepOpaque := make(map[string]struct{}, len(opaque))
	for _, row := range opaque {
		if id := strings.TrimSpace(row.ID); id != "" {
			keepOpaque[id] = struct{}{}
		}
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO sessions(id, teams_chat_id, status, updated_at, json) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, value := range values {
		id := strings.TrimSpace(value.ID)
		if id == "" {
			continue
		}
		if _, keep := keepOpaque[id]; keep {
			continue
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, value.ID, value.TeamsChatID, string(value.Status), sqliteTime(value.UpdatedAt), data); err != nil {
			return err
		}
	}
	for _, row := range opaque {
		if _, err := stmt.ExecContext(ctx, row.ID, nullableSQLiteString(row.TeamsChatID), nullableSQLiteString(row.Status), nullableSQLiteInt64(row.UpdatedAt), row.Raw); err != nil {
			return err
		}
	}
	return nil
}

func sqliteOutboxProjectionMatches(row opaqueSQLiteOutboxRow, message OutboxMessage) bool {
	if strings.TrimSpace(message.ID) != strings.TrimSpace(row.ID) ||
		!sqliteNullableStringMatches(row.SessionID, message.SessionID) ||
		!sqliteNullableStringMatches(row.TurnID, message.TurnID) ||
		!sqliteNullableStringMatches(row.TeamsChatID, message.TeamsChatID) ||
		!sqliteNullableStringMatches(row.TeamsMessageID, message.TeamsMessageID) ||
		!sqliteNullableStringMatches(row.Status, string(message.Status)) ||
		!sqliteNullableInt64Matches(row.Sequence, message.Sequence) ||
		!sqliteNullableTimeMatches(row.CreatedAt, message.CreatedAt) ||
		!sqliteNullableTimeMatches(row.DeliverAfter, message.NextAttemptAt) {
		return false
	}
	pending := int64(0)
	if row.PostSendEffectsPending.Valid {
		pending = row.PostSendEffectsPending.Int64
	}
	if pending != boolInt64(message.PostSendEffectsPending) {
		return false
	}
	return true
}

func sqliteNullableStringMatches(column sql.NullString, value string) bool {
	if !column.Valid {
		// A nullable projection may be absent on a legacy SQLite schema.  The
		// canonical JSON value is still authoritative; absence is not a
		// contradiction that should quarantine an otherwise valid row.
		return true
	}
	return strings.TrimSpace(column.String) == strings.TrimSpace(value)
}

func sqliteNullableInt64Matches(column sql.NullInt64, value int64) bool {
	if !column.Valid {
		return true
	}
	return column.Int64 == value
}

func sqliteNullableTimeMatches(column sql.NullInt64, value time.Time) bool {
	if !column.Valid {
		return true
	}
	// SQLite uses zero for a present-but-empty optional timestamp.  Treat that
	// as the canonical zero time instead of classifying the row as opaque.  A
	// false mismatch here makes a full JSON/SQLite rewrite preserve the old raw
	// row, so later Store.Update changes appear to succeed but are lost on the
	// next read.
	if value.IsZero() {
		return column.Int64 == 0
	}
	return column.Int64 == value.UnixNano()
}

func boolInt64(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func writeSQLiteOutboxPreservingOpaque(ctx context.Context, tx *sql.Tx, values map[string]OutboxMessage, opaque []opaqueSQLiteOutboxRow) error {
	keepOpaque := make(map[string]struct{}, len(opaque))
	for _, row := range opaque {
		if id := strings.TrimSpace(row.ID); id != "" {
			keepOpaque[id] = struct{}{}
		}
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, value := range values {
		if _, keep := keepOpaque[strings.TrimSpace(value.ID)]; keep {
			continue
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, value.ID, value.SessionID, value.TurnID, strings.TrimSpace(value.TeamsChatID), strings.TrimSpace(value.TeamsMessageID), string(value.Status), value.Sequence, sqliteTime(value.CreatedAt), sqliteTime(value.NextAttemptAt), sqliteBool(value.PostSendEffectsPending), data); err != nil {
			return err
		}
	}
	for _, row := range opaque {
		if strings.TrimSpace(row.ID) == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, row.ID, nullableSQLiteString(row.SessionID), nullableSQLiteString(row.TurnID), nullableSQLiteString(row.TeamsChatID), nullableSQLiteString(row.TeamsMessageID), nullableSQLiteString(row.Status), nullableSQLiteInt64(row.Sequence), nullableSQLiteInt64(row.CreatedAt), nullableSQLiteInt64(row.DeliverAfter), nullableSQLiteInt64(row.PostSendEffectsPending), row.Raw); err != nil {
			return err
		}
	}
	return nil
}

func captureOpaqueSQLiteChatPollRows(ctx context.Context, tx *sql.Tx) ([]opaqueSQLiteChatPollRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT chat_id, json FROM chat_polls`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]opaqueSQLiteChatPollRow, 0)
	for rows.Next() {
		var chatID string
		var raw []byte
		if err := rows.Scan(&chatID, &raw); err != nil {
			return nil, err
		}
		var poll ChatPollState
		if json.Unmarshal(raw, &poll) != nil || strings.TrimSpace(chatID) == "" || strings.TrimSpace(poll.ChatID) != strings.TrimSpace(chatID) || !chatPollAdmissionValid(poll) {
			out = append(out, opaqueSQLiteChatPollRow{ChatID: strings.TrimSpace(chatID), Raw: raw})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, rows.Close()
}

func writeSQLiteChatPollsPreservingOpaque(ctx context.Context, tx *sql.Tx, values map[string]ChatPollState, opaque []opaqueSQLiteChatPollRow) error {
	for _, row := range opaque {
		chatID := strings.TrimSpace(row.ChatID)
		if chatID == "" {
			continue
		}
		if replacement, replaced := values[chatID]; replaced {
			if !replacement.RecoveryRequired && chatPollAdmissionValid(replacement) {
				continue
			}
			// Keep the original opaque bytes while the chat-local recovery
			// marker is still active.  The next successful poll writes a typed
			// row and permanently retires this raw projection.
			if _, err := tx.ExecContext(ctx, `DELETE FROM chat_polls WHERE chat_id = ?`, chatID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO chat_polls(chat_id, next_poll_at, blocked_until, poll_state, last_activity_at, park_notice_sent_at, parked_skip_eligible, frontier_active, updated_at, json) VALUES (?, 0, 0, '', 0, 0, 0, 0, 0, ?)`, chatID, row.Raw); err != nil {
			return err
		}
	}
	return nil
}

// captureOpaqueSQLiteCheckpointRows runs before the full-state rewrite.  A
// typed State cannot represent malformed JSON, so writing it back directly
// would silently replace the forensic row with a synthetic fallback. Keep the
// exact raw row and SQL metadata in the same transaction and restore it after
// healthy typed rows have been written.
func captureOpaqueSQLiteCheckpointRows(ctx context.Context, tx *sql.Tx) ([]opaqueSQLiteCheckpointRow, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]opaqueSQLiteCheckpointRow, 0)
	for rows.Next() {
		row, err := scanSQLiteCheckpointRow(rows)
		if err != nil {
			return nil, err
		}
		var checkpoint ImportCheckpoint
		opaque := json.Unmarshal(row.Raw, &checkpoint) != nil
		if !opaque {
			opaque = !row.IDValid || strings.TrimSpace(checkpoint.ID) != strings.TrimSpace(row.ID) ||
				strings.TrimSpace(checkpoint.SessionID) != strings.TrimSpace(row.SessionID.String) ||
				!importCheckpointOptionalProofUsable(checkpoint)
		}
		if opaque {
			out = append(out, opaqueSQLiteCheckpointRow{
				ID:        row.ID,
				IDValid:   row.IDValid,
				SessionID: row.SessionID,
				Status:    row.Status,
				UpdatedAt: row.UpdatedAt,
				Raw:       row.Raw,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, rows.Close()
}

func writeSQLiteImportCheckpointsPreservingOpaque(ctx context.Context, tx *sql.Tx, values map[string]ImportCheckpoint, opaque []opaqueSQLiteCheckpointRow) error {
	for _, value := range values {
		id := strings.TrimSpace(value.ID)
		keepRaw := false
		for _, row := range opaque {
			if row.IDValid && strings.TrimSpace(row.ID) == id {
				keepRaw = true
				break
			}
		}
		if keepRaw {
			continue
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_checkpoints(id, session_id, status, updated_at, json) VALUES (?, ?, ?, ?, ?)`, value.ID, value.SessionID, value.Status, sqliteTime(value.UpdatedAt), data); err != nil {
			return err
		}
	}
	for _, row := range opaque {
		var id any
		if row.IDValid {
			id = row.ID
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_checkpoints(id, session_id, status, updated_at, json) VALUES (?, ?, ?, ?, ?)`, id, nullableSQLiteString(row.SessionID), nullableSQLiteString(row.Status), nullableSQLiteInt64(row.UpdatedAt), row.Raw); err != nil {
			return err
		}
	}
	return nil
}

func nullableSQLiteString(value sql.NullString) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

func nullableSQLiteInt64(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func writeSQLiteMap[T any](ctx context.Context, tx *sql.Tx, stmtText string, values map[string]T, keys func(T) []any) error {
	if len(values) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, stmtText)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, value := range values {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		args := append(keys(value), data)
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return err
		}
	}
	return nil
}

func backfillSQLiteChatPollDerivedColumns(db *sql.DB) error {
	rows, err := db.Query(`SELECT chat_id, json FROM chat_polls WHERE park_notice_sent_at IS NULL OR parked_skip_eligible IS NULL OR frontier_active IS NULL OR last_activity_at IS NULL OR blocked_until IS NULL`)
	if err != nil {
		return err
	}
	type chatPollDerivedUpdate struct {
		ChatID             string
		BlockedUntil       int64
		LastActivityAt     int64
		ParkNoticeSentAt   int64
		ParkedSkipEligible int64
		FrontierActive     int64
	}
	var updates []chatPollDerivedUpdate
	for rows.Next() {
		var chatID string
		var raw []byte
		if err := rows.Scan(&chatID, &raw); err != nil {
			_ = rows.Close()
			return err
		}
		var poll ChatPollState
		if err := json.Unmarshal(raw, &poll); err != nil {
			// A malformed poll projection is a chat-local corruption. Leave the
			// raw bytes for explicit repair, but make sure its derived row cannot
			// be admitted as an operational frontier or abort startup for every
			// other chat.
			chatID = strings.TrimSpace(chatID)
			if chatID != "" {
				updates = append(updates, chatPollDerivedUpdate{ChatID: chatID})
			}
			continue
		}
		chatID = strings.TrimSpace(chatID)
		if chatID == "" {
			chatID = strings.TrimSpace(poll.ChatID)
		}
		if chatID == "" {
			continue
		}
		updates = append(updates, chatPollDerivedUpdate{
			ChatID:             chatID,
			BlockedUntil:       sqliteTime(poll.BlockedUntil),
			LastActivityAt:     sqliteTime(poll.LastActivityAt),
			ParkNoticeSentAt:   sqliteTime(poll.ParkNoticeSentAt),
			ParkedSkipEligible: sqliteBool(chatPollParkedSkipEligible(poll)),
			FrontierActive:     sqliteBool(chatPollHasOperationalFrontier(poll)),
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(updates) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`UPDATE chat_polls SET blocked_until = ?, last_activity_at = ?, park_notice_sent_at = ?, parked_skip_eligible = ?, frontier_active = ? WHERE chat_id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, update := range updates {
		if _, err := stmt.Exec(update.BlockedUntil, update.LastActivityAt, update.ParkNoticeSentAt, update.ParkedSkipEligible, update.FrontierActive, update.ChatID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func backfillSQLiteInboundDerivedColumns(db *sql.DB) error {
	_, err := db.Exec(`UPDATE inbound_events
SET received_at = CASE WHEN json_valid(json) THEN COALESCE(CAST(strftime('%s', json_extract(json, '$.received_at')) AS INTEGER) * 1000000000, 0) ELSE 0 END
WHERE received_at IS NULL`)
	return err
}

// ensureSQLiteRuntimeProjectionMarker distinguishes a genuinely legacy
// SQLite store (no runtime rows yet) from a store whose runtime projection was
// started by an older helper but is now partial. The latter must not silently
// fall back to state_json, because that cold snapshot may predate a newer
// owner/lease write. New full runtime writes record the same marker explicitly
// in their transaction; this one-time compatibility step covers old stores
// opened after the marker was introduced.
func ensureSQLiteRuntimeProjectionMarker(db *sql.DB) error {
	var marker []byte
	err := db.QueryRow(`SELECT value FROM state_meta WHERE key = ?`, sqliteRuntimeProjectionMaterializedKey).Scan(&marker)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var one int
	err = db.QueryRow(`SELECT 1 FROM runtime_state LIMIT 1`).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO NOTHING`, sqliteRuntimeProjectionMaterializedKey, sqliteRuntimeProjectionMaterializedValue)
	return err
}

func backfillSQLiteChatSequences(db *sql.DB) error {
	var raw []byte
	if err := db.QueryRow(`SELECT value FROM state_meta WHERE key = 'state_json'`).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	state, err := loadStateData(raw)
	if err != nil {
		return err
	}
	if len(state.ChatSequences) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO chat_sequences(chat_id, next_sequence, updated_at, json) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, seq := range state.ChatSequences {
		seq.ChatID = strings.TrimSpace(seq.ChatID)
		if seq.ChatID == "" {
			continue
		}
		data, err := json.Marshal(seq)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(seq.ChatID, seq.Next, sqliteTime(seq.UpdatedAt), data); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func loadSQLiteState(ctx context.Context, db *sql.DB) (State, error) {
	if err := ensureSQLiteSchema(db); err != nil {
		return State{}, err
	}
	return loadSQLiteStateRows(ctx, db)
}

func loadSQLiteStateRows(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, error) {
	state, err := loadSQLiteColdState(ctx, db)
	if err != nil {
		return State{}, err
	}
	if err := overlaySQLiteRuntimeState(ctx, db, &state); err != nil {
		return State{}, err
	}
	if err := loadSQLiteSessionMap(ctx, db, `SELECT id, teams_chat_id, status, updated_at, json FROM sessions`, state.Sessions); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM inbound_events`, state.InboundEvents, func(v InboundEvent) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteTurnMap(ctx, db, `SELECT id, session_id, status, json FROM turns`, state.Turns, true); err != nil {
		return State{}, err
	}
	if err := loadSQLiteOutboxMap(ctx, db, `SELECT o.json FROM outbox_messages o WHERE `+sqliteOutboxProjectionValidSQL("o"), state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM message_provenance`, state.MessageProvenance, func(v MessageProvenanceRecord) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteChatPollMap(ctx, db, `SELECT chat_id, json FROM chat_polls`, state.ChatPolls); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM chat_rate_limits`, state.ChatRateLimits, func(v ChatRateLimitState) string { return v.ChatID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteCheckpointMapWithCanonicalSessions(ctx, db, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints`, state.ImportCheckpoints, sqliteKnownSessionIDs(state.Sessions)); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM transcript_ledger`, state.TranscriptLedger, func(v TranscriptLedgerRecord) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM transcript_deliveries`, state.TranscriptDeliveries, func(v TranscriptDeliveryRecord) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM helper_deliveries`, state.HelperDeliveries, func(v HelperDeliveryRecord) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM artifact_records`, state.ArtifactRecords, func(v ArtifactRecord) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM notifications`, state.Notifications, func(v NotificationRecord) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM fork_operations`, state.ForkOperations, func(v ForkOperation) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM fork_history_items`, state.ForkHistoryItems, func(v ForkHistoryItem) string { return v.ID }); err != nil {
		return State{}, err
	}
	normalizeLoadedState(&state)
	return state, nil
}

func loadSQLiteSelectedState(ctx context.Context, db *sql.DB, wanted map[string]struct{}) (State, error) {
	return loadSQLiteSelectedStateWithChatPollQuery(ctx, db, wanted, `SELECT chat_id, json FROM chat_polls`)
}

func loadSQLiteSelectedStateWithChatPollQuery(ctx context.Context, db *sql.DB, wanted map[string]struct{}, chatPollQuery string, chatPollArgs ...any) (State, error) {
	return loadSQLiteSelectedStateWithChatPollQueryMode(ctx, db, wanted, chatPollQuery, false, chatPollArgs...)
}

func loadSQLiteHotPollSelectedStateWithChatPollQuery(ctx context.Context, db *sql.DB, wanted map[string]struct{}, chatPollQuery string, chatPollArgs ...any) (State, error) {
	return loadSQLiteSelectedStateWithChatPollQueryMode(ctx, db, wanted, chatPollQuery, true, chatPollArgs...)
}

func loadSQLiteSelectedStateWithChatPollQueryMode(ctx context.Context, db *sql.DB, wanted map[string]struct{}, chatPollQuery string, hotBase bool, chatPollArgs ...any) (State, error) {
	// Chat sequences are needed by message publication, not by poll admission.
	// Loading the full sequence map here made every listener tick decode one row
	// per chat before it had selected a single due candidate.
	var state State
	var err error
	if hotBase {
		state, err = loadSQLiteHotPollBaseState(ctx, db)
		if err != nil {
			return State{}, err
		}
		if state.SchemaVersion == 0 {
			state, err = loadSQLiteColdStateWithoutChatSequences(ctx, db)
			if err != nil {
				return State{}, err
			}
			if err := overlaySQLiteRuntimeStateBestEffort(ctx, db, &state); err != nil {
				return State{}, err
			}
		}
	} else {
		state, err = loadSQLiteColdStateWithoutChatSequences(ctx, db)
		if err != nil {
			return State{}, err
		}
		if err := overlaySQLiteRuntimeState(ctx, db, &state); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["sessions"]; ok {
		if err := loadSQLiteSessionMap(ctx, db, `SELECT id, teams_chat_id, status, updated_at, json FROM sessions`, state.Sessions); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["inbound_events"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM inbound_events`, state.InboundEvents, func(v InboundEvent) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["turns"]; ok {
		if err := loadSQLiteTurnMap(ctx, db, `SELECT id, session_id, status, json FROM turns`, state.Turns, true); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["outbox_messages"]; ok {
		if err := loadSQLiteOutboxMap(ctx, db, `SELECT o.json FROM outbox_messages o WHERE `+sqliteOutboxProjectionValidSQL("o"), state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["message_provenance"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM message_provenance`, state.MessageProvenance, func(v MessageProvenanceRecord) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["chat_polls"]; ok {
		if strings.TrimSpace(chatPollQuery) == "" {
			chatPollQuery = `SELECT chat_id, json FROM chat_polls`
		}
		if err := loadSQLiteChatPollMapBestEffort(ctx, db, chatPollQuery, state.ChatPolls, chatPollArgs...); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["chat_rate_limits"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM chat_rate_limits`, state.ChatRateLimits, func(v ChatRateLimitState) string { return v.ChatID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["import_checkpoints"]; ok {
		var canonicalSessionIDs map[string]struct{}
		if _, sessionsLoaded := wanted["sessions"]; sessionsLoaded {
			canonicalSessionIDs = sqliteKnownSessionIDs(state.Sessions)
		}
		if err := loadSQLiteCheckpointMapWithCanonicalSessions(ctx, db, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints`, state.ImportCheckpoints, canonicalSessionIDs); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["transcript_ledger"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM transcript_ledger`, state.TranscriptLedger, func(v TranscriptLedgerRecord) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["transcript_deliveries"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM transcript_deliveries`, state.TranscriptDeliveries, func(v TranscriptDeliveryRecord) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["helper_deliveries"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM helper_deliveries`, state.HelperDeliveries, func(v HelperDeliveryRecord) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["artifact_records"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM artifact_records`, state.ArtifactRecords, func(v ArtifactRecord) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["notifications"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM notifications`, state.Notifications, func(v NotificationRecord) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["fork_operations"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM fork_operations`, state.ForkOperations, func(v ForkOperation) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["fork_history_items"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM fork_history_items`, state.ForkHistoryItems, func(v ForkHistoryItem) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	normalizeLoadedState(&state)
	return state, nil
}

// loadSQLiteHotPollBaseState reads only the small runtime projection needed to
// construct a bounded poll-admission snapshot. New stores persist ControlChat
// there as well as in state_json, so a large/corrupt cold document cannot put
// the listener back on an O(size-of-history) decode path. Once a runtime
// projection has any rows, an incomplete projection is deliberately treated as
// authoritative-but-partial: falling back to state_json at that point could
// resurrect a stale owner/control binding. A completely empty runtime table is
// the only compatibility case that returns a zero SchemaVersion for older
// SQLite stores that predate this projection.
func loadSQLiteHotPollBaseState(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, error) {
	runtimeState, seen, err := loadSQLiteRuntimeStateBestEffort(ctx, q)
	if err != nil {
		return State{}, err
	}
	materialized, err := sqliteRuntimeProjectionMaterialized(ctx, q)
	if err != nil {
		return State{}, err
	}
	projectionStarted := len(seen) > 0
	if !projectionStarted {
		projectionStarted = materialized
	}
	if !projectionStarted {
		rows, queryErr := q.QueryContext(ctx, `SELECT 1 FROM runtime_state LIMIT 1`)
		if queryErr != nil {
			return State{}, queryErr
		}
		projectionStarted = rows.Next()
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			return State{}, rowsErr
		}
		if closeErr := rows.Close(); closeErr != nil {
			return State{}, closeErr
		}
	}
	if !projectionStarted {
		return State{}, nil
	}
	state := State{
		SchemaVersion: runtimeState.SchemaVersion,
	}
	if seen[sqliteRuntimeKeyControlChat] {
		state.ControlChat = runtimeState.ControlChat
	}
	if seen[sqliteRuntimeKeyServiceOwner] {
		state.ServiceOwner = runtimeState.ServiceOwner
	}
	state.ensure(time.Time{})
	return state, nil
}

func hotPollSelectedChatIDs(state State) []string {
	seen := make(map[string]struct{}, len(state.ChatPolls)+1)
	for chatID := range state.ChatPolls {
		chatID = strings.TrimSpace(chatID)
		if chatID != "" {
			seen[chatID] = struct{}{}
		}
	}
	if chatID := strings.TrimSpace(state.ControlChat.TeamsChatID); chatID != "" {
		seen[chatID] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for chatID := range seen {
		ids = append(ids, chatID)
	}
	sort.Strings(ids)
	return ids
}

// loadSQLiteHotPollActiveTurns and loadSQLiteHotPollImportingCheckpoints keep
// the hot schedule's auxiliary rows aligned with the bounded poll admission.
// Loading all queued/running turns or importing checkpoints defeats the point
// of limiting chat_polls because those maps are decoded before the scheduler
// has selected a work quantum.
func loadSQLiteHotPollActiveTurns(ctx context.Context, db *sql.DB, state State, readyOnly bool) error {
	if !readyOnly {
		args := sqliteTurnActiveStatusArgs()
		return loadSQLiteTurnMap(ctx, db, `SELECT id, session_id, status, json FROM turns WHERE `+sqliteTurnActiveStatusSQL("status"), state.Turns, true, args...)
	}
	chatIDs := hotPollSelectedChatIDs(state)
	if len(chatIDs) == 0 {
		return nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(chatIDs)), ",")
	args := make([]any, 0, len(chatIDs)+5)
	args = append(args, sqliteTurnActiveStatusArgs()...)
	for _, chatID := range chatIDs {
		args = append(args, chatID)
	}
	return loadSQLiteTurnMap(ctx, db,
		`SELECT t.id, t.session_id, t.status, t.json
FROM turns t
JOIN sessions s ON s.id = t.session_id
WHERE `+sqliteTurnActiveStatusSQL("t.status")+`
  AND s.teams_chat_id IN (`+placeholders+`)`,
		state.Turns, true, args...)
}

func loadSQLiteHotPollImportingCheckpoints(ctx context.Context, db *sql.DB, state State, readyOnly bool) error {
	if !readyOnly {
		return loadSQLiteCheckpointMap(ctx, db, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE status = ?`, state.ImportCheckpoints, sqliteImportCheckpointImporting)
	}
	chatIDs := hotPollSelectedChatIDs(state)
	if len(chatIDs) == 0 {
		return nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(chatIDs)), ",")
	args := make([]any, 0, len(chatIDs)+1)
	args = append(args, sqliteImportCheckpointImporting)
	for _, chatID := range chatIDs {
		args = append(args, chatID)
	}
	return loadSQLiteCheckpointMap(ctx, db,
		`SELECT i.id, i.session_id, i.status, i.updated_at, i.json
FROM import_checkpoints i
JOIN sessions s ON s.id = i.session_id
WHERE i.status = ?
  AND s.teams_chat_id IN (`+placeholders+`)`,
		state.ImportCheckpoints, args...)
}

func loadSQLiteParkedNoticeChatIDs(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT chat_id FROM chat_polls WHERE COALESCE(parked_skip_eligible, 0) != 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var chatID string
		if err := rows.Scan(&chatID); err != nil {
			return nil, err
		}
		chatID = strings.TrimSpace(chatID)
		if chatID != "" {
			out[chatID] = true
		}
	}
	return out, rows.Err()
}

func loadSQLiteHasSessions(ctx context.Context, db *sql.DB) (bool, error) {
	var one int
	if err := db.QueryRowContext(ctx, `SELECT 1 FROM sessions LIMIT 1`).Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func loadSQLiteHasValidSessions(ctx context.Context, db *sql.DB) (bool, error) {
	var one int
	validSession := sqliteSessionValidJSONSQL("json", "id", "teams_chat_id")
	if err := db.QueryRowContext(ctx, `SELECT 1 FROM sessions WHERE `+validSession+` LIMIT 1`).Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func loadSQLiteHotPollWorkCandidates(ctx context.Context, db *sql.DB, controlChatID string, idleBefore time.Time, now time.Time, limit int) ([]SessionContext, error) {
	controlChatID = strings.TrimSpace(controlChatID)
	if limit <= 0 {
		limit = sqliteHotPollReadyLimit
	}
	operationalLimit, ordinaryLimit := sqliteHotPollLaneLimits(limit)
	if now.IsZero() {
		now = time.Now()
	}

	// The SQL projection supplies only cheap, indexed scheduling predicates.
	// Canonical session/poll JSON is decoded once per admitted page in Go. The
	// old query validated every optional field with JSON1 before applying its
	// LIMIT; on a large but healthy backlog that turned one poll tick into a
	// full JSON parse of every candidate. Keyset pages retain the important
	// safety property: a malformed prefix cannot consume the lane LIMIT and
	// hide healthy rows behind it.
	type cursor struct {
		updated, next, activity, sessionUpdated int64
		id                                      string
		set                                     bool
	}
	type candidateRow struct {
		sessionID, sessionRaw, chatID string
		pollRaw                       []byte
		updated, next, activity       int64
		sessionUpdated                int64
	}
	queryRows := func(operational bool, after cursor, pageSize int) ([]candidateRow, error) {
		where := `COALESCE(s.teams_chat_id, '') != ''
  AND (? = '' OR s.teams_chat_id != ?)`
		args := []any{controlChatID, controlChatID}
		if operational {
			where += `
  AND p.chat_id IS NOT NULL
  AND COALESCE(p.frontier_active, 0) != 0
  AND COALESCE(p.next_poll_at, 0) <= ?
  AND COALESCE(p.blocked_until, 0) <= ?`
		} else {
			where += `
  AND (p.chat_id IS NULL OR json_valid(p.json) = 0 OR (
    COALESCE(p.frontier_active, 0) = 0
    AND COALESCE(p.next_poll_at, 0) <= ?
    AND COALESCE(p.blocked_until, 0) <= ?
    AND (COALESCE(p.parked_skip_eligible, 0) = 0
         OR (p.poll_state = ? AND COALESCE(p.next_poll_at, 0) <= ?))
  ))`
			args = append(args, sqliteTime(now), sqliteTime(now), chatPollStateParked, sqliteTime(now))
		}
		if operational {
			args = append(args, sqliteTime(now), sqliteTime(now))
		}
		if !idleBefore.IsZero() {
			idle := `
    COALESCE(p.last_activity_at, 0) > 0
    AND COALESCE(p.last_activity_at, 0) <= ?
    AND COALESCE(s.updated_at, 0) <= ?
    AND COALESCE(p.parked_skip_eligible, 0) = 0
    AND p.poll_state IN (?)
    AND COALESCE(p.frontier_active, 0) = 0
    AND CASE WHEN json_valid(p.json) THEN
      CASE WHEN json_type(p.json, '$.attempt') IS NULL THEN 1 ELSE 0 END
      ELSE 0 END = 1
    AND NOT EXISTS (
      SELECT 1 FROM turns t
      WHERE t.session_id = s.id
        AND ` + sqliteTurnActiveStatusSQL("t.status") + `
    )
  `
			// Syntax-corrupt rows are recovery candidates regardless of stale
			// schedule columns. A malformed row must not be parked by the idle
			// optimization before the listener has a chance to isolate it.
			where += `
  AND (p.chat_id IS NULL OR json_valid(p.json) = 0 OR NOT (` + idle + `))`
			idleBeforeUnix := sqliteTime(idleBefore)
			args = append(args, idleBeforeUnix, idleBeforeUnix, chatPollStateCold)
			args = append(args, sqliteTurnActiveStatusArgs()...)
		}
		if after.set {
			where += `
  AND (COALESCE(p.updated_at, 0) > ?
       OR (COALESCE(p.updated_at, 0) = ? AND COALESCE(p.next_poll_at, 0) > ?)
       OR (COALESCE(p.updated_at, 0) = ? AND COALESCE(p.next_poll_at, 0) = ? AND COALESCE(p.last_activity_at, 0) > ?)
       OR (COALESCE(p.updated_at, 0) = ? AND COALESCE(p.next_poll_at, 0) = ? AND COALESCE(p.last_activity_at, 0) = ? AND COALESCE(s.updated_at, 0) > ?)
       OR (COALESCE(p.updated_at, 0) = ? AND COALESCE(p.next_poll_at, 0) = ? AND COALESCE(p.last_activity_at, 0) = ? AND COALESCE(s.updated_at, 0) = ? AND s.id > ?))`
			args = append(args,
				after.updated, after.updated, after.next,
				after.updated, after.next, after.activity,
				after.updated, after.next, after.activity, after.sessionUpdated,
				after.updated, after.next, after.activity, after.sessionUpdated, after.id)
		}
		query := `SELECT s.id, s.json, s.teams_chat_id,
       COALESCE(p.json, ''), COALESCE(p.updated_at, 0),
       COALESCE(p.next_poll_at, 0), COALESCE(p.last_activity_at, 0),
       COALESCE(s.updated_at, 0)
FROM sessions s
LEFT JOIN chat_polls p ON p.chat_id = s.teams_chat_id
WHERE ` + where + `
ORDER BY COALESCE(p.updated_at, 0), COALESCE(p.next_poll_at, 0),
         COALESCE(p.last_activity_at, 0), COALESCE(s.updated_at, 0), s.id
LIMIT ?`
		args = append(args, pageSize)
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]candidateRow, 0, pageSize)
		for rows.Next() {
			var item candidateRow
			if err := rows.Scan(&item.sessionID, &item.sessionRaw, &item.chatID, &item.pollRaw,
				&item.updated, &item.next, &item.activity, &item.sessionUpdated); err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, rows.Err()
	}

	result := make([]SessionContext, 0, limit)
	seenSessions := make(map[string]struct{}, limit)
	malformedPolls := 0
	deferredOperational := make([]SessionContext, 0, limit)
	deferredOrdinary := make([]SessionContext, 0, limit)
	deferredSeen := make(map[string]struct{}, limit)
	appendLane := func(operational bool, quota int) error {
		if quota <= 0 || len(result) >= limit {
			return nil
		}
		valid := 0
		after := cursor{}
		const pageSize = 64
		for page := 0; valid < quota && len(result) < limit; page++ {
			// The keyset cursor makes this scan finite when the table is finite;
			// do not impose an arbitrary page count that can hide a healthy chat
			// behind more than 512 locally-corrupt rows. The caller's phase
			// context remains the resource boundary for an unusually large
			// corrupt table.
			if err := ctx.Err(); err != nil {
				return err
			}
			items, err := queryRows(operational, after, pageSize)
			if err != nil {
				return err
			}
			if len(items) == 0 {
				break
			}
			for _, item := range items {
				if err := ctx.Err(); err != nil {
					return err
				}
				after = cursor{updated: item.updated, next: item.next, activity: item.activity, sessionUpdated: item.sessionUpdated, id: item.sessionID, set: true}
				var session SessionContext
				if json.Unmarshal([]byte(item.sessionRaw), &session) != nil ||
					strings.TrimSpace(session.ID) == "" || strings.TrimSpace(session.ID) != strings.TrimSpace(item.sessionID) ||
					strings.TrimSpace(session.TeamsChatID) == "" ||
					(item.chatID != "" && strings.TrimSpace(session.TeamsChatID) != strings.TrimSpace(item.chatID)) ||
					(session.Status != "" && session.Status != SessionStatusActive) {
					continue
				}
				if _, exists := seenSessions[session.ID]; exists {
					continue
				}
				pollMalformed := false
				var poll ChatPollState
				if item.chatID != "" {
					if err := json.Unmarshal(item.pollRaw, &poll); err != nil || strings.TrimSpace(poll.ChatID) != strings.TrimSpace(item.chatID) || !chatPollAdmissionValid(poll) {
						pollMalformed = true
					}
				}
				if pollMalformed {
					if malformedPolls >= sqliteHotPollMalformedLimit {
						continue
					}
					malformedPolls++
				} else if actualOperational := chatPollHasOperationalFrontier(poll); actualOperational != operational {
					if _, ok := deferredSeen[session.ID]; !ok {
						deferredSeen[session.ID] = struct{}{}
						if actualOperational {
							deferredOperational = append(deferredOperational, session)
						} else {
							deferredOrdinary = append(deferredOrdinary, session)
						}
					}
					continue
				} else {
					valid++
				}
				seenSessions[session.ID] = struct{}{}
				result = append(result, session)
				if valid >= quota || len(result) >= limit {
					break
				}
			}
			if len(items) < pageSize {
				break
			}
		}
		return nil
	}
	if err := appendLane(false, ordinaryLimit); err != nil {
		return nil, err
	}
	for _, session := range deferredOperational {
		if len(result) >= limit {
			break
		}
		if _, ok := seenSessions[session.ID]; ok {
			continue
		}
		seenSessions[session.ID] = struct{}{}
		result = append(result, session)
	}
	if err := appendLane(true, operationalLimit); err != nil {
		return nil, err
	}
	for _, session := range deferredOrdinary {
		if len(result) >= limit {
			break
		}
		if _, ok := seenSessions[session.ID]; ok {
			continue
		}
		seenSessions[session.ID] = struct{}{}
		result = append(result, session)
	}
	return result, nil
}

func loadSQLiteHotPollWorkCandidatesLegacy(ctx context.Context, db *sql.DB, controlChatID string, idleBefore time.Time, now time.Time, limit int) ([]SessionContext, error) {
	controlChatID = strings.TrimSpace(controlChatID)
	if limit <= 0 {
		limit = sqliteHotPollReadyLimit
	}
	operationalLimit, ordinaryLimit := sqliteHotPollLaneLimits(limit)
	if now.IsZero() {
		now = time.Now()
	}
	validSession := sqliteSessionValidJSONSQL("s.json", "s.id", "s.teams_chat_id")
	activeSession := sqliteSessionActiveJSONSQL("s.json", "s.status")
	// The bounded candidate query performs the final typed decode in Go.  Keep
	// SQL admission to the cheap identity/syntax projection here; the full
	// semantic validator expands into dozens of JSON1 calls per row and made a
	// large but healthy operational backlog dominate the listener hot path.
	validPoll := sqliteChatPollValidJSONSQL("p.json", "p.chat_id")
	malformedPoll := `NOT (` + validPoll + `)`
	frontier := sqliteChatPollOperationalFrontierSQL("p.json")
	excludeIdle := ""
	operationalIdle := ""
	ordinaryIdle := ""
	// Keep the arguments in the same order as the CTEs below.  In particular,
	// the idle predicate is appended after the operational/ordinary lane's
	// readiness arguments; putting it first makes every placeholder after the
	// control row receive the wrong value and silently removes work chats from
	// the candidate set.
	args := []any{
		controlChatID,
		controlChatID, controlChatID, sqliteTime(now), sqliteTime(now),
	}
	if !idleBefore.IsZero() {
		// A malformed poll is admitted as a chat-local recovery candidate and
		// therefore bypasses schedule/idle predicates that depend on untrusted
		// JSON.  For valid rows the derived frontier flag replaces the previous
		// JSON1 probes; an attempt is still checked with a guarded CASE because
		// it is a short-lived field that is not worth another column.
		excludeIdle = `  AND NOT (
    COALESCE(p.last_activity_at, 0) > 0
    AND COALESCE(p.last_activity_at, 0) <= ?
    AND COALESCE(s.updated_at, 0) <= ?
    AND COALESCE(p.parked_skip_eligible, 0) = 0
    AND p.poll_state IN (?)
    AND ` + validPoll + `
    AND NOT ` + frontier + `
    AND CASE WHEN json_valid(p.json) THEN
      CASE WHEN json_type(p.json, '$.attempt') IS NULL THEN 1 ELSE 0 END
      ELSE 0 END = 1
    AND NOT EXISTS (
      SELECT 1 FROM turns t
      WHERE t.session_id = s.id
        AND ` + sqliteTurnActiveStatusSQL("t.status") + `
    )
  )
		`
		operationalIdle, ordinaryIdle = excludeIdle, excludeIdle
		idleBeforeUnix := sqliteTime(idleBefore)
		args = append(args, idleBeforeUnix, idleBeforeUnix, chatPollStateCold)
		args = append(args, sqliteTurnActiveStatusArgs()...)
	}
	args = append(args, operationalLimit)
	args = append(args, controlChatID, controlChatID, sqliteTime(now), sqliteTime(now), chatPollStateParked, sqliteTime(now))
	if !idleBefore.IsZero() {
		idleBeforeUnix := sqliteTime(idleBefore)
		args = append(args, idleBeforeUnix, idleBeforeUnix, chatPollStateCold)
		args = append(args, sqliteTurnActiveStatusArgs()...)
	}
	args = append(args, ordinaryLimit)
	args = append(args, controlChatID, controlChatID, sqliteHotPollMalformedLimit)
	args = append(args, limit, controlChatID)
	query := `WITH control AS (
    SELECT s.json, s.id, s.teams_chat_id AS chat_id, 0 AS lane,
           COALESCE(p.updated_at, 0) AS sort_updated,
           COALESCE(p.next_poll_at, 0) AS sort_next,
           COALESCE(p.last_activity_at, 0) AS sort_activity
    FROM sessions s
    JOIN chat_polls p ON p.chat_id = s.teams_chat_id
    WHERE s.teams_chat_id = ?
      AND ` + validSession + `
      AND ` + activeSession + `
      AND (` + validPoll + ` OR ` + malformedPoll + `)
    LIMIT 1
), operational AS (
    SELECT s.json, s.id, s.teams_chat_id AS chat_id, 2 AS lane,
           COALESCE(p.updated_at, 0) AS sort_updated,
           COALESCE(p.next_poll_at, 0) AS sort_next,
           COALESCE(p.last_activity_at, 0) AS sort_activity
    FROM sessions s
    LEFT JOIN chat_polls p ON p.chat_id = s.teams_chat_id
    WHERE COALESCE(s.teams_chat_id, '') != ''
      AND (? = '' OR s.teams_chat_id != ?)
      AND ` + validSession + `
      AND ` + activeSession + `
      AND p.chat_id IS NOT NULL
      AND ((` + validPoll + `
        AND ` + frontier + `
        AND COALESCE(p.next_poll_at, 0) <= ?
        AND COALESCE(p.blocked_until, 0) <= ?)
      )
` + operationalIdle + `
    ORDER BY sort_updated, sort_next, sort_activity, s.updated_at, s.id
    LIMIT ?
), ordinary AS (
    SELECT s.json, s.id, s.teams_chat_id AS chat_id, 1 AS lane,
           COALESCE(p.updated_at, 0) AS sort_updated,
           COALESCE(p.next_poll_at, 0) AS sort_next,
           COALESCE(p.last_activity_at, 0) AS sort_activity
    FROM sessions s
    LEFT JOIN chat_polls p ON p.chat_id = s.teams_chat_id
    WHERE COALESCE(s.teams_chat_id, '') != ''
      AND (? = '' OR s.teams_chat_id != ?)
      AND ` + validSession + `
      AND ` + activeSession + `
      AND (
        p.chat_id IS NULL
        OR (
          ` + validPoll + `
          AND NOT ` + frontier + `
          AND COALESCE(p.next_poll_at, 0) <= ?
          AND COALESCE(p.blocked_until, 0) <= ?
          AND (COALESCE(p.parked_skip_eligible, 0) = 0
               OR (p.poll_state = ? AND COALESCE(p.next_poll_at, 0) <= ?))
        )
      )
` + ordinaryIdle + `
    ORDER BY sort_updated, sort_next, sort_activity, s.updated_at, s.id
    LIMIT ?
), malformed AS (
    SELECT s.json, s.id, s.teams_chat_id AS chat_id, 1 AS lane,
           COALESCE(p.updated_at, 0) AS sort_updated,
           COALESCE(p.next_poll_at, 0) AS sort_next,
           COALESCE(p.last_activity_at, 0) AS sort_activity
    FROM sessions s
    JOIN chat_polls p ON p.chat_id = s.teams_chat_id
    WHERE COALESCE(s.teams_chat_id, '') != ''
      AND (? = '' OR s.teams_chat_id != ?)
      AND ` + validSession + `
      AND ` + activeSession + `
      AND ` + malformedPoll + `
    ORDER BY sort_updated, sort_next, sort_activity, s.updated_at, s.id
    LIMIT ?
), admitted AS (
    SELECT * FROM control
    UNION ALL SELECT * FROM ordinary
    UNION ALL SELECT * FROM malformed
    UNION ALL SELECT * FROM operational
), limited AS (
    SELECT * FROM admitted
    ORDER BY lane, sort_updated, sort_next, sort_activity, id
    LIMIT ?
)
SELECT id, json FROM limited
WHERE chat_id != ?
ORDER BY lane, sort_updated, sort_next, sort_activity, id`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionContext
	for rows.Next() {
		var sessionID string
		var raw []byte
		if err := rows.Scan(&sessionID, &raw); err != nil {
			return nil, err
		}
		var session SessionContext
		if err := json.Unmarshal(raw, &session); err != nil {
			continue
		}
		if strings.TrimSpace(session.ID) == "" || strings.TrimSpace(session.ID) != strings.TrimSpace(sessionID) || strings.TrimSpace(session.TeamsChatID) == "" {
			continue
		}
		out = append(out, session)
	}
	return out, rows.Err()
}

func loadSQLiteIdleWorkChatParkCandidates(ctx context.Context, db *sql.DB, controlChatID string, idleBefore time.Time, limit int) ([]IdleWorkChatParkCandidate, error) {
	controlChatID = strings.TrimSpace(controlChatID)
	if idleBefore.IsZero() || limit <= 0 {
		return nil, nil
	}
	validSession := sqliteSessionValidJSONSQL("s.json", "s.id", "s.teams_chat_id")
	validPoll := sqliteChatPollValidJSONSQL("p.json", "p.chat_id")
	rows, err := db.QueryContext(ctx, `SELECT s.json, p.json
FROM chat_polls p
JOIN sessions s ON s.teams_chat_id = p.chat_id
WHERE p.last_activity_at > 0
  AND p.last_activity_at <= ?
  AND COALESCE(s.updated_at, 0) <= ?
  AND (s.status IS NULL OR s.status = '' OR s.status = ?)
  AND COALESCE(s.teams_chat_id, '') != ''
  AND (? = '' OR s.teams_chat_id != ?)
  AND `+validSession+`
  AND `+validPoll+`
  AND COALESCE(p.blocked_until, 0) <= ?
  AND COALESCE(p.parked_skip_eligible, 0) = 0
  AND p.poll_state IN (?, ?)
  AND NOT EXISTS (
    SELECT 1 FROM turns t
    WHERE t.session_id = s.id
      AND `+sqliteTurnActiveStatusSQL("t.status")+`
  )
ORDER BY p.last_activity_at ASC, s.updated_at ASC, s.id
LIMIT ?`, append([]any{sqliteTime(idleBefore), sqliteTime(idleBefore), string(SessionStatusActive), controlChatID, controlChatID, sqliteTime(time.Now()), chatPollStateCold, chatPollStateParked}, append(sqliteTurnActiveStatusArgs(), limit)...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IdleWorkChatParkCandidate
	for rows.Next() {
		var sessionRaw []byte
		var pollRaw []byte
		if err := rows.Scan(&sessionRaw, &pollRaw); err != nil {
			return nil, err
		}
		var candidate IdleWorkChatParkCandidate
		if err := json.Unmarshal(sessionRaw, &candidate.Session); err != nil {
			// A malformed session is local scheduler state.  Do not let it abort
			// auto-park discovery for every other chat; the SQL identity remains
			// held out by the admission query and the next repair/startup pass can
			// report it independently.
			continue
		}
		if err := json.Unmarshal(pollRaw, &candidate.Poll); err != nil {
			continue
		}
		if strings.TrimSpace(candidate.Session.ID) == "" || strings.TrimSpace(candidate.Poll.ChatID) == "" || strings.TrimSpace(candidate.Session.TeamsChatID) != strings.TrimSpace(candidate.Poll.ChatID) {
			continue
		}
		annotateUnknownLoadedSession(&candidate.Session)
		out = append(out, candidate)
	}
	return out, rows.Err()
}

func loadSQLiteColdState(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, error) {
	return loadSQLiteColdStateWithChatSequences(ctx, q, true)
}

func loadSQLiteColdStateWithoutChatSequences(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, error) {
	return loadSQLiteColdStateWithChatSequences(ctx, q, false)
}

func loadSQLiteColdStateWithChatSequences(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, includeChatSequences bool) (State, error) {
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = 'state_json'`).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return State{}, errors.New("sqlite teams store is missing state metadata")
		}
		return State{}, err
	}
	if len(raw) == 0 {
		return State{}, errors.New("sqlite teams store has empty state metadata")
	}
	state, err := loadStateData(raw)
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	if err := overlaySQLiteHistoryWatchProjection(ctx, q, &state); err != nil {
		return State{}, err
	}
	state.ChatSequences = map[string]ChatSequenceState{}
	if includeChatSequences {
		if err := loadSQLiteJSONMap(ctx, q, `SELECT json FROM chat_sequences`, state.ChatSequences, func(v ChatSequenceState) string { return v.ChatID }); err != nil {
			return State{}, err
		}
	}
	return state, nil
}

func loadSQLiteHistoryWatchProjection(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (map[string]HistoryWatchCheckpoint, time.Time, int64, int64, bool, error) {
	var raw []byte
	var stateJSONRevision int64
	// Read the projection and the epoch in one scoped query.  The epoch is
	// required for mixed-version safety, but a second state_meta round trip on
	// every HistoryWatch poll was a measurable cost in the SQLite hot path.
	if err := q.QueryRowContext(ctx, `
SELECT value,
       COALESCE((SELECT CAST(value AS INTEGER) FROM state_meta WHERE key = ?), 0)
FROM state_meta WHERE key = ?`, sqliteStateJSONRevisionKey, sqliteHistoryWatchProjectionKey).Scan(&raw, &stateJSONRevision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, time.Time{}, 0, 0, false, nil
		}
		return nil, time.Time{}, 0, 0, false, err
	}
	var projection sqliteHistoryWatchProjection
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &projection); err != nil {
			// The projection is a rebuildable cache of the canonical state_json
			// row. A torn/manual write here must not make the entire SQLite store
			// unreadable; return "not found" so callers fall back to state_json
			// and can rematerialize a clean projection on the next update.
			return nil, time.Time{}, 0, 0, false, nil
		}
	}
	if projection.HistoryWatch == nil {
		projection.HistoryWatch = make(map[string]HistoryWatchCheckpoint)
	}
	return projection.HistoryWatch, projection.HistoryWatchReady, projection.StateJSONRevision, stateJSONRevision, true, nil
}

func sqliteStateJSONRevision(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (int64, error) {
	var revision int64
	if err := q.QueryRowContext(ctx, `SELECT CAST(value AS INTEGER) FROM state_meta WHERE key = ?`, sqliteStateJSONRevisionKey).Scan(&revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	if revision < 0 {
		return 0, fmt.Errorf("invalid sqlite state_json revision %d", revision)
	}
	return revision, nil
}

func overlaySQLiteHistoryWatchProjection(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, state *State) error {
	history, ready, projectionRevision, stateJSONRevision, found, err := loadSQLiteHistoryWatchProjection(ctx, q)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	// A projection written before the epoch existed is treated as legacy. If a
	// full-state writer has since touched state_json, the revision differs and
	// the cold JSON is authoritative until the current helper rematerializes the
	// projection. This is deliberately fail-closed for mixed-version stores.
	if projectionRevision <= 0 || stateJSONRevision <= 0 || projectionRevision != stateJSONRevision {
		return nil
	}
	state.HistoryWatch = history
	state.HistoryWatchReady = ready
	return nil
}

func loadSQLiteJSONMap[T any](ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, out map[string]T, key func(T) string, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		out[key(value)] = value
	}
	return rows.Err()
}

// loadSQLiteJSONMapBestEffort is used only for chat-local admission
// projections. A malformed row is held out of the current selection, while
// other rows remain available to the listener. Strict loading remains the
// default for execution, delivery, and ownership records where silently
// omitting a row could change work semantics.
func loadSQLiteJSONMapBestEffort[T any](ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, out map[string]T, key func(T) string, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			continue
		}
		if strings.TrimSpace(key(value)) == "" {
			continue
		}
		out[key(value)] = value
	}
	return rows.Err()
}

// Outbox JSON is an independently persisted delivery record. A single
// truncated/corrupt row must be held out of a hot snapshot so healthy rows can
// continue, while full-state writes preserve the raw row as opaque evidence.
func loadSQLiteOutboxMap[T any](ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, out map[string]T, key func(T) string, args ...any) error {
	return loadSQLiteJSONMapBestEffort(ctx, q, query, out, key, args...)
}

func loadSQLiteSessionMap(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, out map[string]SessionContext, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var sqlID string
		var chatID, status sql.NullString
		var updatedAt sql.NullInt64
		var raw []byte
		if err := rows.Scan(&sqlID, &chatID, &status, &updatedAt, &raw); err != nil {
			return err
		}
		var session SessionContext
		if err := json.Unmarshal(raw, &session); err != nil {
			// A corrupt session is held out of the scheduler. The SQL identity
			// is intentionally not promoted into a runnable SessionContext: it
			// does not prove a Codex thread or workspace binding.
			continue
		}
		if strings.TrimSpace(sqlID) == "" || strings.TrimSpace(session.ID) != strings.TrimSpace(sqlID) {
			continue
		}
		// The SQL chat binding is an indexed identity, not merely a sort
		// column.  Do not let a valid JSON payload from another chat become a
		// runnable session after a partial/cross-row repair.  Empty values are
		// retained for old non-Teams/session-only rows; hot admission already
		// requires a non-empty binding.
		if chatID.Valid && strings.TrimSpace(chatID.String) != "" &&
			strings.TrimSpace(session.TeamsChatID) != "" &&
			strings.TrimSpace(session.TeamsChatID) != strings.TrimSpace(chatID.String) {
			continue
		}
		// Preserve an explicit diagnostic even in the bounded hot projection.
		// The scheduler still holds unknown session statuses out of runnable
		// candidates; annotating here prevents the fast path from making the
		// row look like an ordinary inactive chat when it is later surfaced by
		// a diagnostic or repair snapshot.
		annotateUnknownLoadedSession(&session)
		out[session.ID] = session
	}
	return rows.Err()
}

func loadSQLiteTurnMap(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, out map[string]Turn, holdMalformedActive bool, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var sqlID, sqlSessionID string
		var sqlStatus string
		var raw []byte
		if err := rows.Scan(&sqlID, &sqlSessionID, &sqlStatus, &raw); err != nil {
			return err
		}
		var turn Turn
		if err := json.Unmarshal(raw, &turn); err == nil &&
			strings.TrimSpace(turn.ID) == strings.TrimSpace(sqlID) &&
			strings.TrimSpace(turn.SessionID) == strings.TrimSpace(sqlSessionID) {
			normalizeLoadedTurnStatus(&turn)
			out[turn.ID] = turn
			continue
		}
		if holdMalformedActive && sqliteTurnStatusIsSafetyActive(TurnStatus(sqlStatus)) &&
			strings.TrimSpace(sqlID) != "" && strings.TrimSpace(sqlSessionID) != "" {
			// Preserve the safety-relevant state from the indexed SQL columns
			// when the payload is malformed. This holds the affected chat from
			// starting a second execution without trusting any cursor/output
			// fields from the bad JSON.
			held := Turn{ID: sqlID, SessionID: sqlSessionID, Status: TurnStatus(sqlStatus)}
			normalizeLoadedTurnStatus(&held)
			out[sqlID] = held
		}
	}
	return rows.Err()
}

func sqliteTurnStatusIsSafetyActive(status TurnStatus) bool {
	trimmed := TurnStatus(strings.TrimSpace(string(status)))
	if trimmed == TurnStatusQueued || trimmed == TurnStatusRunning {
		return true
	}
	return strings.TrimSpace(string(trimmed)) != "" && !knownTurnStatus(trimmed)
}

func sqliteTurnActiveStatusSQL(column string) string {
	return "(" + column + " IN (?, ?) OR (" + column + " <> '' AND " + column + " NOT IN (?, ?, ?)))"
}

func sqliteTurnActiveStatusArgs() []any {
	return []any{string(TurnStatusQueued), string(TurnStatusRunning), string(TurnStatusCompleted), string(TurnStatusFailed), string(TurnStatusInterrupted)}
}

func sqliteTurnUnresolvedStatusSQL(column string) string {
	return "(" + column + " IN (?, ?) OR (" + column + " <> '' AND " + column + " NOT IN (?, ?, ?, ?)))"
}

func sqliteTurnUnresolvedStatusArgs() []any {
	return []any{string(TurnStatusRunning), string(TurnStatusInterrupted), string(TurnStatusQueued), string(TurnStatusCompleted), string(TurnStatusFailed), string(TurnStatusInterrupted)}
}

// decodeSQLiteTurnExecutionMetadata validates only the small ownership
// surface needed by the linked-transcript probes. The SQL status/session
// columns are the indexed row identity; the JSON payload must agree when it
// supplies those fields. A malformed or contradictory payload is not an
// infrastructure failure, but it is not safe evidence that an interrupted
// execution has finished either.
func decodeSQLiteTurnExecutionMetadata(raw []byte, indexedSessionID string, indexedStatus TurnStatus) (struct {
	SessionID      string     `json:"session_id"`
	Status         TurnStatus `json:"status"`
	RecoveryReason string     `json:"recovery_reason"`
}, bool) {
	var meta struct {
		SessionID      string     `json:"session_id"`
		Status         TurnStatus `json:"status"`
		RecoveryReason string     `json:"recovery_reason"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return meta, false
	}
	if strings.TrimSpace(indexedSessionID) == "" || meta.Status != indexedStatus {
		return meta, false
	}
	if embeddedSessionID := strings.TrimSpace(meta.SessionID); embeddedSessionID != "" && embeddedSessionID != strings.TrimSpace(indexedSessionID) {
		return meta, false
	}
	return meta, true
}

func loadSQLiteChatPollMap(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, out map[string]ChatPollState, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var sqlChatID string
		var raw []byte
		if err := rows.Scan(&sqlChatID, &raw); err != nil {
			return err
		}
		if strings.TrimSpace(sqlChatID) == "" {
			continue
		}
		poll, ok := decodeChatPollState(sqlChatID, raw)
		if !ok {
			continue
		}
		out[sqlChatID] = poll
	}
	return rows.Err()
}

// loadSQLiteChatPollMapBestEffort is used by bounded hot admission.  The SQL
// identity is authoritative for a malformed poll row: turning it into a
// recovery placeholder prevents a missing typed projection from becoming a
// baseline-only first observation that silently consumes the current Graph
// head.  Other rows remain independently usable.
func loadSQLiteChatPollMapBestEffort(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, out map[string]ChatPollState, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var sqlChatID string
		var raw []byte
		if err := rows.Scan(&sqlChatID, &raw); err != nil {
			return err
		}
		poll, ok := decodeChatPollStateForAdmission(sqlChatID, raw)
		if !ok {
			continue
		}
		out[strings.TrimSpace(sqlChatID)] = poll
	}
	return rows.Err()
}

func loadSQLiteRegisteredSessionIDs(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (map[string]struct{}, error) {
	rows, err := q.QueryContext(ctx, `SELECT id FROM sessions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	known := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id = strings.TrimSpace(id); id != "" {
			known[id] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return known, rows.Close()
}

// sqliteCheckpointDisposition describes the result of decoding a checkpoint
// row without conflating a corrupt row with an absent row.  Checkpoint rows
// are the one SQLite table whose JSON is untrusted input: older versions and
// interrupted manual repairs have left rows whose typed payload is not
// decodable even though the SQL identity columns are intact.
type sqliteCheckpointDisposition string

const (
	sqliteCheckpointValid              sqliteCheckpointDisposition = "valid"
	sqliteCheckpointMalformedCanonical sqliteCheckpointDisposition = "malformed-canonical"
	sqliteCheckpointIdentityConflict   sqliteCheckpointDisposition = "identity-conflict"
	sqliteCheckpointForeign            sqliteCheckpointDisposition = "foreign/noncanonical"
	sqliteCheckpointMissing            sqliteCheckpointDisposition = "missing"
	sqliteCheckpointProvenanceInvalid  sqliteCheckpointDisposition = "provenance-invalid"
	sqliteCheckpointInfrastructure     sqliteCheckpointDisposition = "infrastructure-error"
)

type sqliteCheckpointRow struct {
	ID        string
	IDValid   bool
	SessionID sql.NullString
	Status    sql.NullString
	UpdatedAt sql.NullInt64
	Raw       []byte
}

type sqliteCheckpointRowScanner interface {
	Scan(...any) error
}

// scanSQLiteCanonicalCheckpointFastRow is used only by the linked-transcript
// snapshot, whose query is restricted to non-NULL primary-key values.  The
// COALESCE expressions keep nullable legacy metadata fail-closed while letting
// the normal row path scan concrete Go values instead of allocating through
// sql.Null* conversions on every poll.  A zero/empty value remains invalid to
// the decoder; this is an allocation optimization, not a trust downgrade.
func scanSQLiteCanonicalCheckpointFastRow(scanner sqliteCheckpointRowScanner) (sqliteCheckpointRow, error) {
	var id string
	var sessionID string
	var status string
	var updatedAt int64
	var raw []byte
	if err := scanner.Scan(&id, &sessionID, &status, &updatedAt, &raw); err != nil {
		return sqliteCheckpointRow{}, err
	}
	return sqliteCheckpointRow{
		ID:        id,
		IDValid:   strings.TrimSpace(id) != "",
		SessionID: sql.NullString{String: sessionID, Valid: strings.TrimSpace(sessionID) != ""},
		Status:    sql.NullString{String: status, Valid: strings.TrimSpace(status) != ""},
		UpdatedAt: sql.NullInt64{Int64: updatedAt, Valid: updatedAt != 0},
		Raw:       raw,
	}, nil
}

// scanSQLiteCheckpointIdentityFastRow is the no-change linked-transcript read
// path.  The caller already selected canonical IDs and only needs the SQL
// identity plus payload to validate a normal checkpoint.  Status and
// updated_at are needed only when preserving/repairing an opaque row, so the
// full nullable row decoder remains available for those paths without adding
// two extra driver conversions to every idle poll.
func scanSQLiteCheckpointIdentityFastRow(scanner sqliteCheckpointRowScanner) (sqliteCheckpointRow, error) {
	var id string
	var sessionID string
	var raw []byte
	if err := scanner.Scan(&id, &sessionID, &raw); err != nil {
		return sqliteCheckpointRow{}, err
	}
	return sqliteCheckpointRow{
		ID:        id,
		IDValid:   strings.TrimSpace(id) != "",
		SessionID: sql.NullString{String: sessionID, Valid: strings.TrimSpace(sessionID) != ""},
		Raw:       raw,
	}, nil
}

func scanSQLiteCheckpointRow(scanner sqliteCheckpointRowScanner) (sqliteCheckpointRow, error) {
	var id sql.NullString
	var row sqliteCheckpointRow
	if err := scanner.Scan(&id, &row.SessionID, &row.Status, &row.UpdatedAt, &row.Raw); err != nil {
		return sqliteCheckpointRow{}, err
	}
	row.ID = id.String
	row.IDValid = id.Valid
	return row, nil
}

// loadSQLiteCheckpointRow reads all SQL identity metadata with the raw JSON in
// one query.  Callers must use this instead of loadSQLiteJSONRow for
// ImportCheckpoint: a malformed checkpoint is still an addressable session
// local fence, not a missing row and not a reason to abort unrelated reads.
func loadSQLiteCheckpointRow(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, query string, args ...any) (sqliteCheckpointRow, bool, error) {
	row, err := scanSQLiteCheckpointRow(q.QueryRowContext(ctx, query, args...))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sqliteCheckpointRow{}, false, nil
		}
		return sqliteCheckpointRow{}, false, err
	}
	return row, true, nil
}

func decodeSQLiteCheckpointRow(row sqliteCheckpointRow, requestedID string, requestedSessionID string, requireCanonical bool) (ImportCheckpoint, bool, sqliteCheckpointDisposition, error) {
	return decodeSQLiteCheckpointRowWithCanonicalIdentity(row, requestedID, requestedSessionID, requireCanonical, requireCanonical)
}

func decodeSQLiteCheckpointRowWithCanonicalIdentity(row sqliteCheckpointRow, requestedID string, requestedSessionID string, requireCanonical bool, enforceCanonicalIdentity bool) (ImportCheckpoint, bool, sqliteCheckpointDisposition, error) {
	requestedID = strings.TrimSpace(requestedID)
	requestedSessionID = strings.TrimSpace(requestedSessionID)
	rowID := strings.TrimSpace(row.ID)
	rowSessionID := strings.TrimSpace(row.SessionID.String)
	if requestedID == "" || !row.IDValid || rowID == "" || rowID != requestedID {
		return ImportCheckpoint{}, false, sqliteCheckpointIdentityConflict,
			fmt.Errorf("%w: checkpoint row id %q does not match requested %q", ErrSessionStateProvenanceMismatch, rowID, requestedID)
	}
	if requestedSessionID != "" && rowSessionID != "" && requestedSessionID != rowSessionID {
		return ImportCheckpoint{}, false, sqliteCheckpointIdentityConflict,
			fmt.Errorf("%w: checkpoint %q SQL session %q does not match requested %q", ErrSessionStateProvenanceMismatch, requestedID, rowSessionID, requestedSessionID)
	}
	if rowSessionID == "" {
		if requestedSessionID != "" {
			return ImportCheckpoint{}, false, sqliteCheckpointIdentityConflict,
				fmt.Errorf("%w: checkpoint %q has no SQL session identity", ErrSessionStateProvenanceMismatch, requestedID)
		}
		return ImportCheckpoint{}, false, sqliteCheckpointIdentityConflict,
			fmt.Errorf("%w: checkpoint %q has no SQL session identity", ErrSessionStateProvenanceMismatch, requestedID)
	}
	if requestedSessionID == "" {
		requestedSessionID = rowSessionID
	}
	canonicalSessionID, canonical := canonicalCheckpointSessionID(requestedID)
	if enforceCanonicalIdentity && canonical && canonicalSessionID != rowSessionID {
		// A canonical transcript:<session> key cannot be reinterpreted as an
		// operation-specific row merely because its SQL and JSON payload agree
		// with a different session.  This check is enabled when the caller has
		// independently established that the suffix names a registered session;
		// older stores also contain operation-local IDs in the transcript:*
		// namespace whose suffix is not a session ID.
		return ImportCheckpoint{}, false, sqliteCheckpointIdentityConflict,
			fmt.Errorf("%w: canonical checkpoint %q SQL session %q does not match key session %q", ErrSessionStateProvenanceMismatch, requestedID, rowSessionID, canonicalSessionID)
	}
	if requireCanonical && (!canonical || canonicalSessionID != rowSessionID || canonicalSessionID != requestedSessionID) {
		return ImportCheckpoint{}, false, sqliteCheckpointForeign,
			fmt.Errorf("%w: checkpoint %q is not the canonical row for session %q", ErrSessionStateProvenanceMismatch, requestedID, requestedSessionID)
	}

	var checkpoint ImportCheckpoint
	trimmed := bytes.TrimSpace(row.Raw)
	unmarshalErr := json.Unmarshal(row.Raw, &checkpoint)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		unmarshalErr = fmt.Errorf("JSON is null or empty")
	}
	if unmarshalErr == nil {
		// SQL identity and any non-empty embedded JSON identity must agree before
		// an optional-proof downgrade is considered. A valid JSON object carrying
		// another checkpoint/session remains a hard provenance error. An object
		// with no identity at all (or only one half of it) is just as opaque as an
		// empty/null payload, but a canonical SQL row can still provide a safe,
		// session-local fence for it.
		embeddedID := strings.TrimSpace(checkpoint.ID)
		embeddedSessionID := strings.TrimSpace(checkpoint.SessionID)
		if (embeddedID != "" && embeddedID != requestedID) ||
			(embeddedSessionID != "" && embeddedSessionID != requestedSessionID) {
			return ImportCheckpoint{}, false, sqliteCheckpointIdentityConflict,
				fmt.Errorf("%w: checkpoint %q embedded identity is id=%q session=%q", ErrSessionStateProvenanceMismatch, requestedID, checkpoint.ID, checkpoint.SessionID)
		}
		if embeddedID == "" || embeddedSessionID == "" {
			if canonical && canonicalSessionID == rowSessionID {
				return opaqueCanonicalImportCheckpointFromRow(row, requestedID, rowSessionID, invalidCanonicalCheckpointKind), true, sqliteCheckpointIdentityConflict, nil
			}
			return ImportCheckpoint{}, false, sqliteCheckpointIdentityConflict,
				fmt.Errorf("%w: checkpoint %q has incomplete embedded identity id=%q session=%q", ErrSessionStateProvenanceMismatch, requestedID, checkpoint.ID, checkpoint.SessionID)
		}
		if !importCheckpointOptionalProofUsable(checkpoint) {
			// The payload is readable and its identity is proven.  Preserve the
			// legacy fields for diagnosis and migration compatibility, but keep the
			// explicit marker that prevents automatic source use.  A later ordinary
			// write must still retain the raw row; the caller receives the
			// provenance-invalid disposition for that purpose.
			checkpoint.RecoveryProofUnusable = true
			return checkpoint, true, sqliteCheckpointProvenanceInvalid, nil
		}
		if err := validateImportCheckpointProvenance(checkpoint, requestedSessionID, requestedID); err != nil {
			// SQL id/session columns are the only trusted identity for an opaque
			// canonical row.  Do not return a partially decoded JSON value: it may
			// contain a foreign cursor, path, or execution owner.  Keep the row
			// addressable as a deterministic session-local fence instead.
			return ImportCheckpoint{}, false, sqliteCheckpointProvenanceInvalid, err
		}
		return checkpoint, true, sqliteCheckpointValid, nil
	} else {
		// A malformed row is recoverable as a deterministic fence only when
		// the SQL identity itself proves that it is the canonical per-session
		// checkpoint.  General readers also need this fallback: passing
		// requireCanonical=false means valid non-canonical operation rows are
		// allowed, not that an opaque canonical row should become an error.
		if !canonical || canonicalSessionID != rowSessionID {
			return ImportCheckpoint{}, false, sqliteCheckpointMalformedCanonical,
				fmt.Errorf("malformed non-canonical import checkpoint %q: JSON cannot be decoded", requestedID)
		}
		// A canonical row with trusted SQL identity is retained as an explicit
		// unresolved fallback. The raw payload is never written back by this
		// decoder; ordinary reads must not repair or erase evidence.
		return opaqueCanonicalImportCheckpointFromRow(row, requestedID, rowSessionID, malformedCanonicalCheckpointKind), true, sqliteCheckpointMalformedCanonical, nil
	}
}

func loadSQLiteCheckpointMap(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, out map[string]ImportCheckpoint, args ...any) error {
	return loadSQLiteCheckpointMapWithCanonicalSessions(ctx, q, query, out, nil, args...)
}

func loadSQLiteCheckpointMapWithCanonicalSessions(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query string, out map[string]ImportCheckpoint, canonicalSessionIDs map[string]struct{}, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	canonicalCandidates := make(map[string]struct{})
	for rows.Next() {
		row, err := scanSQLiteCheckpointRow(rows)
		if err != nil {
			return err
		}
		enforceCanonicalIdentity := false
		if canonicalSessionID, canonical := canonicalCheckpointSessionID(row.ID); canonical {
			if canonicalSessionIDs != nil {
				_, enforceCanonicalIdentity = canonicalSessionIDs[canonicalSessionID]
			} else if canonicalSessionID != strings.TrimSpace(row.SessionID.String) {
				canonicalCandidates[row.ID] = struct{}{}
			}
		}
		checkpoint, found, disposition, err := decodeSQLiteCheckpointRowWithCanonicalIdentity(row, row.ID, row.SessionID.String, false, enforceCanonicalIdentity)
		if err != nil {
			if sqliteCheckpointDispositionIsRowLocal(disposition) {
				// A malformed/foreign operation row cannot be safely represented in
				// the typed map, but it must not make unrelated sessions unreadable.
				continue
			}
			return err
		}
		if found {
			out[checkpoint.ID] = checkpoint
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(canonicalCandidates) == 0 || canonicalSessionIDs != nil {
		return rows.Close()
	}
	registered, err := loadSQLiteRegisteredSessionIDs(ctx, q)
	if err != nil {
		return err
	}
	for id := range canonicalCandidates {
		canonicalSessionID, canonical := canonicalCheckpointSessionID(id)
		if canonical {
			if _, known := registered[canonicalSessionID]; known {
				delete(out, id)
			}
		}
	}
	return rows.Close()
}

func sqliteCheckpointDispositionIsRowLocal(disposition sqliteCheckpointDisposition) bool {
	switch disposition {
	case sqliteCheckpointMalformedCanonical, sqliteCheckpointIdentityConflict, sqliteCheckpointForeign, sqliteCheckpointProvenanceInvalid:
		return true
	default:
		return false
	}
}

func loadSQLiteCheckpointForID(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, query string, args ...any) (ImportCheckpoint, bool, error) {
	checkpoint, found, _, err := loadSQLiteCheckpointForIDWithDisposition(ctx, q, query, args...)
	return checkpoint, found, err
}

func loadSQLiteCheckpointForIDWithDisposition(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, query string, args ...any) (ImportCheckpoint, bool, sqliteCheckpointDisposition, error) {
	row, found, err := loadSQLiteCheckpointRow(ctx, q, query, args...)
	if err != nil || !found {
		return ImportCheckpoint{}, found, sqliteCheckpointMissing, err
	}
	checkpoint, found, disposition, err := decodeSQLiteCheckpointRowWithCanonicalIdentity(row, row.ID, row.SessionID.String, false, false)
	if err != nil {
		return checkpoint, found, disposition, err
	}
	canonicalSessionID, canonical := canonicalCheckpointSessionID(row.ID)
	if canonical && canonicalSessionID != strings.TrimSpace(row.SessionID.String) {
		enforceCanonicalIdentity, err := sqliteCheckpointCanonicalIdentityIsRegistered(ctx, q, row.ID)
		if err != nil {
			return ImportCheckpoint{}, false, sqliteCheckpointInfrastructure, err
		}
		if enforceCanonicalIdentity {
			return decodeSQLiteCheckpointRowWithCanonicalIdentity(row, row.ID, row.SessionID.String, false, true)
		}
	}
	return checkpoint, found, disposition, nil
}

func sqliteCheckpointCanonicalIdentityIsRegistered(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id string) (bool, error) {
	sessionID, canonical := canonicalCheckpointSessionID(id)
	if !canonical {
		return false, nil
	}
	var one int
	err := q.QueryRowContext(ctx, `SELECT 1 FROM sessions WHERE id = ? LIMIT 1`, sessionID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return one != 0, nil
}

func loadSQLiteSessionsByID(ctx context.Context, db *sql.DB, ids []string, out map[string]SessionContext) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	query := `SELECT json FROM sessions WHERE id IN (` + strings.Join(placeholders, ",") + `)`
	return loadSQLiteJSONMap(ctx, db, query, out, func(v SessionContext) string { return v.ID }, args...)
}

// loadSQLiteForkPollingState is the scoped SQLite projection for the staged
// child poller.  fork_operations is an indexed, exceptional table; sessions
// and chat_polls are not.  Loading those two tables in full on every listener
// tick made one staged child impose the cost of the entire Teams state.  The
// operation row is the only discovery input, after which child session and
// poll rows are fetched by their durable identities in bounded batches.
func loadSQLiteForkPollingState(ctx context.Context, db *sql.DB) (State, error) {
	state := newState()
	rows, err := db.QueryContext(ctx, `SELECT id, json FROM fork_operations
WHERE phase IN (?, ?, ?)
ORDER BY updated_at, id`,
		string(ForkPhaseChildChatStaged),
		string(ForkPhaseHistoryPublishing),
		string(ForkPhaseHistoryVerified),
	)
	if err != nil {
		return State{}, err
	}
	var childSessionIDs []string
	childSessionsSeen := make(map[string]struct{})
	var childChatIDs []string
	childChatsSeen := make(map[string]struct{})
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			_ = rows.Close()
			return State{}, err
		}
		var operation ForkOperation
		if err := json.Unmarshal(raw, &operation); err != nil ||
			strings.TrimSpace(id) == "" || strings.TrimSpace(operation.ID) != strings.TrimSpace(id) {
			// A malformed fork operation is not a reason to materialize all
			// unrelated chats. It remains in SQLite for the normal explicit
			// repair/diagnostic path.
			continue
		}
		switch operation.Phase {
		case ForkPhaseChildChatStaged, ForkPhaseHistoryPublishing, ForkPhaseHistoryVerified:
		default:
			// The indexed phase is only an admission hint. The canonical JSON
			// phase must agree before a child becomes runnable.
			continue
		}
		state.ForkOperations[operation.ID] = operation
		if sessionID := strings.TrimSpace(operation.ChildSessionID); sessionID != "" {
			if _, seen := childSessionsSeen[sessionID]; !seen {
				childSessionsSeen[sessionID] = struct{}{}
				childSessionIDs = append(childSessionIDs, sessionID)
			}
		}
		if chatID := strings.TrimSpace(operation.ChildChatID); chatID != "" {
			if _, seen := childChatsSeen[chatID]; !seen {
				childChatsSeen[chatID] = struct{}{}
				childChatIDs = append(childChatIDs, chatID)
			}
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return State{}, err
	}
	if err := rows.Close(); err != nil {
		return State{}, err
	}

	const batchSize = 400
	for start := 0; start < len(childSessionIDs); start += batchSize {
		end := min(start+batchSize, len(childSessionIDs))
		placeholders := strings.TrimRight(strings.Repeat("?,", end-start), ",")
		args := make([]any, 0, end-start)
		for _, id := range childSessionIDs[start:end] {
			args = append(args, id)
		}
		if err := loadSQLiteSessionMap(ctx, db, `SELECT id, teams_chat_id, status, updated_at, json FROM sessions WHERE id IN (`+placeholders+`)`, state.Sessions, args...); err != nil {
			return State{}, err
		}
	}
	for _, session := range state.Sessions {
		if chatID := strings.TrimSpace(session.TeamsChatID); chatID != "" {
			if _, seen := childChatsSeen[chatID]; !seen {
				childChatsSeen[chatID] = struct{}{}
				childChatIDs = append(childChatIDs, chatID)
			}
		}
	}
	for start := 0; start < len(childChatIDs); start += batchSize {
		end := min(start+batchSize, len(childChatIDs))
		placeholders := strings.TrimRight(strings.Repeat("?,", end-start), ",")
		args := make([]any, 0, end-start)
		for _, id := range childChatIDs[start:end] {
			args = append(args, id)
		}
		if err := loadSQLiteChatPollMapBestEffort(ctx, db, `SELECT chat_id, json FROM chat_polls WHERE chat_id IN (`+placeholders+`)`, state.ChatPolls, args...); err != nil {
			return State{}, err
		}
	}
	return state, nil
}

func loadSQLiteJSONMapTx[T any](ctx context.Context, tx *sql.Tx, query string, args []any, out map[string]T, key func(T) string) error {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			return err
		}
		out[key(value)] = value
	}
	return rows.Err()
}

func loadSQLiteOutboxMapTx(ctx context.Context, tx *sql.Tx, query string, args []any, out map[string]OutboxMessage, key func(OutboxMessage) string) error {
	return loadSQLiteJSONMapBestEffort(ctx, tx, query, out, key, args...)
}

func loadSQLiteJSONRow[T any](ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, query string, args ...any) (T, bool, error) {
	var raw []byte
	if err := q.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			var zero T
			return zero, false, nil
		}
		var zero T
		return zero, false, err
	}
	// A chat-poll row is an operational projection, not execution proof. Use
	// the SQL chat identity as a recovery placeholder for syntax/identity
	// failures, but retain a syntactically valid typed row here even when its
	// nested envelope is structurally unusable. The bridge must see that
	// envelope so it can move the exact pending receipt into gap evidence rather
	// than silently replacing it with a new baseline.
	var zero T
	if _, isChatPoll := any(zero).(ChatPollState); isChatPoll && len(args) == 1 {
		if chatID, ok := args[0].(string); ok {
			if poll, recoverable := decodeChatPollState(chatID, raw); recoverable {
				markChatPollRecoveryEvidence(&poll, raw)
				return any(poll).(T), true, nil
			}
		}
	}
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return zero, false, err
	}
	return value, true, nil
}

func saveSQLiteColdStateTx(ctx context.Context, tx *sql.Tx, state State) error {
	if err := upsertSQLiteSplitStateTx(ctx, tx, state); err != nil {
		return err
	}
	if err := replaceSQLiteChatSequencesTx(ctx, tx, state.ChatSequences); err != nil {
		return err
	}
	cold, err := json.Marshal(coldSQLiteState(state))
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES ('state_json', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, cold)
	if err != nil {
		return err
	}
	// HistoryWatch has a dedicated projection update path.  Do not rewrite that
	// projection on every unrelated cold-state/outbox update: it adds a JSON
	// marshal and state_meta write to the hot queue path and can overwrite a
	// newer watcher projection when a legacy full-state writer races with it.
	// The state_json revision trigger invalidates a stale projection; the next
	// HistoryWatch read/update will rebuild it from the canonical cold state.
	return nil
}

func upsertSQLiteHistoryWatchProjectionTx(ctx context.Context, tx *sql.Tx, history map[string]HistoryWatchCheckpoint, ready time.Time, stateJSONRevision int64) error {
	if history == nil {
		history = make(map[string]HistoryWatchCheckpoint)
	}
	raw, err := json.Marshal(sqliteHistoryWatchProjection{HistoryWatch: history, HistoryWatchReady: ready, StateJSONRevision: stateJSONRevision})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteHistoryWatchProjectionKey, raw)
	return err
}

const (
	sqliteRuntimeKeyScope                    = "scope"
	sqliteRuntimeKeyMachineIdentity          = "machine_identity"
	sqliteRuntimeKeyMachines                 = "machines"
	sqliteRuntimeKeyControlLease             = "control_lease"
	sqliteRuntimeKeyServiceOwner             = "service_owner"
	sqliteRuntimeKeyLockOwner                = "lock_owner"
	sqliteRuntimeKeyServiceControl           = "service_control"
	sqliteRuntimeKeyUpgrade                  = "upgrade"
	sqliteRuntimeKeyAutoUpdate               = "auto_update"
	sqliteRuntimeKeyControlChat              = "control_chat"
	sqliteRuntimeProjectionMaterializedKey   = "runtime_state_materialized"
	sqliteRuntimeProjectionMaterializedValue = "1"
)

// ErrSQLiteRuntimeProjectionIncomplete means a store that has already
// materialized its runtime projection is missing one or more required rows.
// Falling back to state_json in this state could overwrite a newer owner or
// lease with an older cold snapshot, so full-state runtime mutations fail
// closed until the projection is repaired by an explicit recovery path.
var ErrSQLiteRuntimeProjectionIncomplete = errors.New("SQLite runtime projection is incomplete")

func sqliteRuntimeProjectionIncompleteError(detail string) error {
	return fmt.Errorf("%w: %w: %s", ErrSQLiteRuntimeProjectionIncomplete, ErrControlLeaseStateUntrusted, detail)
}

var sqliteRuntimeRequiredKeys = []string{
	sqliteRuntimeKeyScope,
	sqliteRuntimeKeyMachineIdentity,
	sqliteRuntimeKeyMachines,
	sqliteRuntimeKeyControlLease,
	sqliteRuntimeKeyServiceOwner,
	sqliteRuntimeKeyLockOwner,
}

var sqliteRuntimePersistedKeys = append(append([]string{}, sqliteRuntimeRequiredKeys...),
	sqliteRuntimeKeyServiceControl,
	sqliteRuntimeKeyUpgrade,
	sqliteRuntimeKeyAutoUpdate,
	sqliteRuntimeKeyControlChat,
)

func saveSQLiteRuntimeStateTx(ctx context.Context, tx *sql.Tx, state State) error {
	values := map[string]any{
		sqliteRuntimeKeyScope:           state.Scope,
		sqliteRuntimeKeyMachineIdentity: state.MachineIdentity,
		sqliteRuntimeKeyMachines:        state.Machines,
		sqliteRuntimeKeyControlLease:    state.ControlLease,
		sqliteRuntimeKeyServiceOwner:    state.ServiceOwner,
		sqliteRuntimeKeyLockOwner:       state.LockOwner,
		sqliteRuntimeKeyServiceControl:  state.ServiceControl,
		sqliteRuntimeKeyUpgrade:         state.Upgrade,
		sqliteRuntimeKeyAutoUpdate:      state.AutoUpdate,
		sqliteRuntimeKeyControlChat:     state.ControlChat,
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO runtime_state(key, json) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET json = excluded.json`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, key := range sqliteRuntimePersistedKeys {
		data, err := json.Marshal(values[key])
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, key, data); err != nil {
			return err
		}
	}
	return markSQLiteRuntimeProjectionMaterializedTx(ctx, tx)
}

func markSQLiteRuntimeProjectionMaterializedTx(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteRuntimeProjectionMaterializedKey, sqliteRuntimeProjectionMaterializedValue)
	return err
}

func loadSQLiteRuntimeState(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, map[string]bool, error) {
	return loadSQLiteRuntimeStateWithMode(ctx, q, true)
}

// loadSQLiteRuntimeStateBestEffort is used only by bounded liveness/admission
// paths.  A malformed optional projection row must not make the listener
// repeatedly decode the cold state or stop healthy chats from being admitted.
// The full Store.Load and migration paths continue to use the strict loader.
func loadSQLiteRuntimeStateBestEffort(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, map[string]bool, error) {
	return loadSQLiteRuntimeStateWithMode(ctx, q, false)
}

func loadSQLiteRuntimeStateWithMode(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, strict bool) (State, map[string]bool, error) {
	state := State{
		SchemaVersion: SchemaVersion,
		Machines:      map[string]MachineRecord{},
	}
	seen := make(map[string]bool)
	rows, err := q.QueryContext(ctx, `SELECT key, json FROM runtime_state`)
	if err != nil {
		return State{}, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			return State{}, nil, err
		}
		var decodeErr error
		switch key {
		case sqliteRuntimeKeyScope:
			decodeErr = json.Unmarshal(raw, &state.Scope)
		case sqliteRuntimeKeyMachineIdentity:
			decodeErr = json.Unmarshal(raw, &state.MachineIdentity)
		case sqliteRuntimeKeyMachines:
			decodeErr = json.Unmarshal(raw, &state.Machines)
			if decodeErr == nil && state.Machines == nil {
				state.Machines = map[string]MachineRecord{}
			}
		case sqliteRuntimeKeyControlLease:
			decodeErr = json.Unmarshal(raw, &state.ControlLease)
			if decodeErr == nil {
				decodeErr = validateControlLeaseShape(state.ControlLease)
			}
		case sqliteRuntimeKeyServiceOwner:
			decodeErr = json.Unmarshal(raw, &state.ServiceOwner)
		case sqliteRuntimeKeyLockOwner:
			decodeErr = json.Unmarshal(raw, &state.LockOwner)
		case sqliteRuntimeKeyServiceControl:
			decodeErr = json.Unmarshal(raw, &state.ServiceControl)
		case sqliteRuntimeKeyUpgrade:
			decodeErr = json.Unmarshal(raw, &state.Upgrade)
		case sqliteRuntimeKeyAutoUpdate:
			decodeErr = json.Unmarshal(raw, &state.AutoUpdate)
		case sqliteRuntimeKeyControlChat:
			decodeErr = json.Unmarshal(raw, &state.ControlChat)
		default:
			continue
		}
		if decodeErr != nil {
			if strict {
				return State{}, nil, decodeErr
			}
			continue
		}
		seen[key] = true
	}
	if err := rows.Err(); err != nil {
		return State{}, nil, err
	}
	state.ensure(time.Time{})
	return state, seen, nil
}

func sqliteRuntimeStateUsable(seen map[string]bool) bool {
	for _, key := range sqliteRuntimeRequiredKeys {
		if !seen[key] {
			return false
		}
	}
	return true
}

func sqliteRuntimeProjectionMaterialized(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT value FROM state_meta WHERE key = ?`, sqliteRuntimeProjectionMaterializedKey)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, err
		}
		return false, nil
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		return false, err
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	// Presence is the authority. Treat an unknown marker value as materialized
	// too, so a torn/manual marker write cannot reopen the unsafe cold fallback.
	return true, nil
}

func sqliteRuntimeProjectionStarted(seen map[string]bool, invalid map[string][]byte) bool {
	for _, key := range sqliteRuntimeRequiredKeys {
		if seen[key] || invalid[key] != nil {
			return true
		}
	}
	return false
}

func overlaySQLiteRuntimeState(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, state *State) error {
	return overlaySQLiteRuntimeStateWithMode(ctx, q, state, true)
}

func overlaySQLiteRuntimeStateBestEffort(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, state *State) error {
	return overlaySQLiteRuntimeStateWithMode(ctx, q, state, false)
}

func overlaySQLiteRuntimeStateWithMode(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, state *State, strict bool) error {
	var runtimeState State
	var seen map[string]bool
	var err error
	if strict {
		runtimeState, seen, err = loadSQLiteRuntimeState(ctx, q)
	} else {
		runtimeState, seen, err = loadSQLiteRuntimeStateBestEffort(ctx, q)
	}
	if err != nil {
		return err
	}
	if !sqliteRuntimeStateUsable(seen) {
		materialized, err := sqliteRuntimeProjectionMaterialized(ctx, q)
		if err != nil {
			return err
		}
		if materialized && strict {
			return sqliteRuntimeProjectionIncompleteError("missing required runtime rows")
		}
		if materialized {
			overlaySQLiteRuntimeStateValues(state, runtimeState, seen)
			state.ensure(time.Time{})
		}
		return nil
	}
	overlaySQLiteRuntimeStateValues(state, runtimeState, seen)
	state.ensure(time.Time{})
	return nil
}

func overlaySQLiteRuntimeStateValues(state *State, runtimeState State, seen map[string]bool) {
	if state == nil {
		return
	}
	state.Scope = runtimeState.Scope
	state.MachineIdentity = runtimeState.MachineIdentity
	state.Machines = runtimeState.Machines
	state.ControlLease = runtimeState.ControlLease
	state.ServiceOwner = runtimeState.ServiceOwner
	state.LockOwner = runtimeState.LockOwner
	if seen[sqliteRuntimeKeyServiceControl] {
		state.ServiceControl = runtimeState.ServiceControl
	}
	if seen[sqliteRuntimeKeyUpgrade] {
		state.Upgrade = runtimeState.Upgrade
	}
	if seen[sqliteRuntimeKeyAutoUpdate] {
		state.AutoUpdate = runtimeState.AutoUpdate
	}
	if seen[sqliteRuntimeKeyControlChat] {
		state.ControlChat = runtimeState.ControlChat
	}
}

// readUpgradeSQLite reads the bounded runtime projection used by the main-loop
// upgrade notice check. A complete runtime projection is authoritative for
// SQLite stores and avoids decoding the growing cold state document and chat
// sequences on every poll. Older stores with an incomplete projection fall
// back to the compatibility loader in ReadUpgrade.
func (s *Store) readUpgradeSQLiteUnlocked(ctx context.Context) (UpgradeRequest, bool, bool, error) {
	var out UpgradeRequest
	found := false
	handled := false
	pointer, ok, err := s.currentSQLitePointerUnlocked()
	if err != nil || !ok {
		return out, found, handled, err
	}
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return out, found, handled, err
	}
	runtimeState, seen, err := loadSQLiteRuntimeState(ctx, db)
	if err != nil {
		return out, found, handled, err
	}
	if !sqliteRuntimeStateUsable(seen) {
		materialized, materializedErr := sqliteRuntimeProjectionMaterialized(ctx, db)
		if materializedErr != nil {
			return out, found, handled, materializedErr
		}
		if materialized {
			return out, found, handled, sqliteRuntimeProjectionIncompleteError("missing required runtime rows")
		}
		return out, found, handled, nil
	}
	if !seen[sqliteRuntimeKeyUpgrade] {
		return out, found, handled, nil
	}
	handled = true
	if runtimeState.Upgrade == nil || runtimeState.Upgrade.ID == "" {
		return out, found, handled, nil
	}
	out = *runtimeState.Upgrade
	found = true
	return out, found, handled, nil
}

func seedMissingSQLiteRuntimeOptionalState(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, state *State, seen map[string]bool) error {
	if seen[sqliteRuntimeKeyServiceControl] && seen[sqliteRuntimeKeyUpgrade] && seen[sqliteRuntimeKeyAutoUpdate] && seen[sqliteRuntimeKeyControlChat] {
		return nil
	}
	cold, err := loadSQLiteColdState(ctx, q)
	if err != nil {
		return err
	}
	if !seen[sqliteRuntimeKeyServiceControl] {
		state.ServiceControl = cold.ServiceControl
	}
	if !seen[sqliteRuntimeKeyUpgrade] {
		state.Upgrade = cold.Upgrade
	}
	if !seen[sqliteRuntimeKeyAutoUpdate] {
		state.AutoUpdate = cold.AutoUpdate
	}
	if !seen[sqliteRuntimeKeyControlChat] {
		state.ControlChat = cold.ControlChat
	}
	return nil
}

func (s *Store) updateSQLiteRuntimeState(ctx context.Context, fn func(*State) error) (bool, error) {
	return s.updateSQLiteRuntimeStateWithTx(ctx, fn, nil)
}

// updateSQLiteRuntimeStateWithTx applies a runtime-state mutation without
// decoding unrelated hot rows. An optional afterTx callback can update a
// small, explicitly selected set of split-table rows in the same transaction.
// This is used by lifecycle migrations that need runtime-state atomicity while
// still preserving the runtime fast path for stores with a damaged/unrelated
// hot row.
func (s *Store) updateSQLiteRuntimeStateWithTx(ctx context.Context, fn func(*State) error, afterTx func(context.Context, *sql.Tx) error) (bool, error) {
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state, seen, err := loadSQLiteRuntimeState(ctx, tx)
		if err != nil {
			return err
		}
		materialized, err := sqliteRuntimeProjectionMaterialized(ctx, tx)
		if err != nil {
			return err
		}
		if materialized && !sqliteRuntimeStateUsable(seen) {
			return sqliteRuntimeProjectionIncompleteError("missing required runtime rows")
		}
		seedRuntime := !sqliteRuntimeStateUsable(seen)
		seedOptional := !seen[sqliteRuntimeKeyServiceControl] || !seen[sqliteRuntimeKeyUpgrade] || !seen[sqliteRuntimeKeyAutoUpdate] || !seen[sqliteRuntimeKeyControlChat]
		if seedRuntime {
			state, err = loadSQLiteColdState(ctx, tx)
			if err != nil {
				return err
			}
		} else if err := seedMissingSQLiteRuntimeOptionalState(ctx, tx, &state, seen); err != nil {
			return err
		}
		if err := fn(&state); err != nil {
			if errors.Is(err, errStoreNoChange) && (seedRuntime || seedOptional) {
				state.ensure(time.Now())
				if afterTx != nil {
					if err := afterTx(ctx, tx); err != nil {
						return err
					}
				}
				if saveErr := saveSQLiteRuntimeStateTx(ctx, tx, state); saveErr != nil {
					return saveErr
				}
				return tx.Commit()
			}
			return err
		}
		state.ensure(time.Now())
		if afterTx != nil {
			if err := afterTx(ctx, tx); err != nil {
				return err
			}
		}
		if err := saveSQLiteRuntimeStateTx(ctx, tx, state); err != nil {
			return err
		}
		return tx.Commit()
	})
	if errors.Is(err, errStoreNoChange) {
		return handled, nil
	}
	return handled, err
}

// retireLegacyHistoryGateOutboxSQLiteTx retires only obsolete history-gate
// notices while the caller owns the store transaction. It deliberately reads
// raw outbox rows instead of the full state document: a malformed unrelated
// hot row must not prevent an upgrade from establishing its drain fence.
func retireLegacyHistoryGateOutboxSQLiteTx(ctx context.Context, tx *sql.Tx, pageSize int, _ int, includeActiveSending bool) (int, error) {
	if pageSize <= 0 {
		pageSize = 128
	}
	type candidate struct {
		id        string
		createdAt int64
		message   OutboxMessage
	}
	var retired int
	var afterCreatedAt int64
	var afterID string
	for {
		clauses := []string{"status IN (?, ?)"}
		args := []any{string(OutboxStatusQueued), string(OutboxStatusSending)}
		if afterID != "" || afterCreatedAt != 0 {
			clauses = append(clauses, "(created_at > ? OR (created_at = ? AND id > ?))")
			args = append(args, afterCreatedAt, afterCreatedAt, afterID)
		}
		args = append(args, pageSize+1)
		rows, err := tx.QueryContext(ctx, `SELECT id, created_at, json
FROM outbox_messages
WHERE `+strings.Join(clauses, " AND ")+`
ORDER BY created_at, id
LIMIT ?`, args...)
		if err != nil {
			return retired, err
		}
		rowsRead := 0
		candidates := make([]candidate, 0, pageSize)
		for rows.Next() {
			var rowID string
			var createdAt int64
			var raw []byte
			if err := rows.Scan(&rowID, &createdAt, &raw); err != nil {
				_ = rows.Close()
				return retired, err
			}
			rowsRead++
			afterCreatedAt = createdAt
			afterID = rowID
			var msg OutboxMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			if strings.TrimSpace(msg.ID) == "" || msg.ID != rowID {
				continue
			}
			candidates = append(candidates, candidate{id: rowID, createdAt: createdAt, message: msg})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return retired, err
		}
		if err := rows.Close(); err != nil {
			return retired, err
		}
		for _, item := range candidates {
			if !legacyHistoryGateNoticeRetirable(item.message, time.Now(), includeActiveSending) {
				continue
			}
			state := newState()
			state.OutboxMessages[item.id] = item.message
			if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, item.id); err != nil {
				return retired, err
			}
			updated, err := markOutboxSkippedLocked(&state, item.message, "obsolete automatic history-gate notice", time.Now())
			if err != nil {
				return retired, err
			}
			if updated.Status == item.message.Status {
				continue
			}
			if err := upsertSQLiteOutboxTx(ctx, tx, updated); err != nil {
				return retired, err
			}
			if err := upsertSQLiteOutboxLinkedRecordsTx(ctx, tx, state); err != nil {
				return retired, err
			}
			retired++
		}
		if rowsRead <= pageSize {
			return retired, nil
		}
	}
}

func (s *Store) retireLegacyHistoryGateOutboxSQLite(ctx context.Context, pageSize int, maxPages int, includeActiveSending bool) (int, bool, error) {
	retired := 0
	handled, err := s.updateSQLiteRuntimeStateWithTx(ctx, func(_ *State) error {
		return nil
	}, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		retired, err = retireLegacyHistoryGateOutboxSQLiteTx(ctx, tx, pageSize, maxPages, includeActiveSending)
		if err != nil {
			return err
		}
		if retired == 0 {
			return errStoreNoChange
		}
		return nil
	})
	return retired, handled, err
}

// rebindSQLiteScopeForMigration updates only the bounded runtime projection and
// the small state_meta document. It deliberately leaves all business-data
// tables untouched.
func (s *Store) rebindSQLiteScopeForMigration(ctx context.Context, fn func(*State) error) (bool, error) {
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		runtimeState, seen, err := loadSQLiteRuntimeState(ctx, tx)
		if err != nil {
			return err
		}
		if !sqliteRuntimeStateUsable(seen) {
			return errors.New("sqlite teams store is missing required runtime state")
		}
		coldState, err := loadSQLiteColdStateWithoutChatSequences(ctx, tx)
		if err != nil {
			return err
		}
		runtimeState.ControlChat = coldState.ControlChat
		if err := fn(&runtimeState); err != nil {
			return err
		}
		coldState.ControlChat = runtimeState.ControlChat
		if err := saveSQLiteRuntimeStateTx(ctx, tx, runtimeState); err != nil {
			return err
		}
		cold, err := json.Marshal(coldSQLiteState(coldState))
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES ('state_json', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, cold); err != nil {
			return err
		}
		return tx.Commit()
	})
	return handled, err
}

func (s *Store) importCheckpointSQLite(ctx context.Context, id string) (ImportCheckpoint, bool, bool, error) {
	var out ImportCheckpoint
	found := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		out, found, _, err = loadSQLiteCheckpointForIDWithDisposition(ctx, db, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, id)
		return err
	})
	return out, found, handled, err
}

// RepairOpaqueImportCheckpoint replaces one opaque canonical SQLite row only
// when its exact raw payload is still the one the caller inspected.  This is
// intentionally a narrow, explicit operation: ordinary reads, queue claims,
// and unrelated state updates must never turn a forensic fallback into a new
// typed row.  The raw compare is kept in the UPDATE predicate as well as the
// transaction-local read so an external/manual writer cannot be overwritten
// by a stale repair decision.
func (s *Store) RepairOpaqueImportCheckpoint(ctx context.Context, id string, expectedRaw []byte, replacement ImportCheckpoint) error {
	if ctx == nil {
		ctx = context.Background()
	}
	id = strings.TrimSpace(id)
	if id == "" || expectedRaw == nil {
		return ErrCheckpointRepairConflict
	}
	expectedSessionID, canonical := canonicalCheckpointSessionID(id)
	if !canonical {
		return fmt.Errorf("%w: checkpoint %q is not a canonical session checkpoint", ErrCheckpointRepairConflict, id)
	}
	if replacement.ID != "" && strings.TrimSpace(replacement.ID) != id {
		return fmt.Errorf("%w: replacement id %q does not match %q", ErrSessionStateProvenanceMismatch, replacement.ID, id)
	}
	if replacement.SessionID != "" && strings.TrimSpace(replacement.SessionID) != expectedSessionID {
		return fmt.Errorf("%w: replacement session %q does not match %q", ErrSessionStateProvenanceMismatch, replacement.SessionID, expectedSessionID)
	}
	replacement.ID = id
	replacement.SessionID = expectedSessionID
	if replacement.RecoveryProofUnusable || !importCheckpointOptionalProofUsable(replacement) {
		return fmt.Errorf("%w: replacement recovery proof is not usable", ErrSessionStateProvenanceMismatch)
	}
	if err := validateImportCheckpointProvenance(replacement, expectedSessionID, id); err != nil {
		return err
	}
	replacementRaw, err := json.Marshal(replacement)
	if err != nil {
		return err
	}
	return s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil {
			return err
		}
		if !ok {
			return ErrSQLiteCheckpointRepairUnavailable
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		row, found, err := loadSQLiteCheckpointRow(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, id)
		if err != nil {
			return err
		}
		if !found || strings.TrimSpace(row.ID) != id || !row.SessionID.Valid || strings.TrimSpace(row.SessionID.String) != expectedSessionID {
			return ErrCheckpointRepairConflict
		}
		if !bytes.Equal(row.Raw, expectedRaw) {
			// A client may lose the commit response after SQLite has committed.
			// If the row already contains the exact requested replacement, the
			// retry is idempotently complete; any other raw value is a conflict.
			if bytes.Equal(row.Raw, replacementRaw) {
				return tx.Commit()
			}
			return ErrCheckpointRepairConflict
		}
		result, err := tx.ExecContext(ctx, `UPDATE import_checkpoints SET session_id = ?, status = ?, updated_at = ?, json = ? WHERE id = ? AND session_id = ? AND json = ?`,
			replacement.SessionID, replacement.Status, sqliteTime(replacement.UpdatedAt), replacementRaw, id, expectedSessionID, expectedRaw)
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return ErrCheckpointRepairConflict
		}
		return tx.Commit()
	})
}

// promoteProofBackedImportCheckpointSQLite is the SQLite half of the narrow
// compatibility promotion API.  The typed row is loaded and compared inside
// the same transaction that writes the replacement, so a scanner cannot move
// a cursor or install a new quarantine between validation and promotion.
func (s *Store) promoteProofBackedImportCheckpointSQLite(ctx context.Context, expected ImportCheckpoint, replacement ImportCheckpoint) (bool, bool, error) {
	changed := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		current, found, _, err := loadSQLiteCanonicalCheckpointRow(ctx, tx, expected.ID, expected.SessionID)
		if err != nil {
			return err
		}
		if !found || !proofBackedCheckpointCASMatches(current, expected) || !current.LegacySourceUnverified && !current.RecoveryProofUnusable {
			return ErrProofBackedCheckpointPromotionConflict
		}
		replacement.UpdatedAt = time.Now()
		if err := upsertSQLiteImportCheckpointTx(ctx, tx, replacement); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		changed = true
		return nil
	})
	return changed, handled, err
}

func (s *Store) loadSQLiteImportCheckpointsByIDsUnlocked(ctx context.Context, pointer storeSQLitePointer, requested map[string]string) (map[string]ImportCheckpoint, error) {
	out := make(map[string]ImportCheckpoint, len(requested))
	if len(requested) == 0 {
		return out, nil
	}
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(requested))
	for id := range requested {
		ids = append(ids, id)
	}
	for start := 0; start < len(ids); start += sqliteQueryParameterBatchSize {
		end := start + sqliteQueryParameterBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		args := make([]any, 0, len(batch))
		for _, id := range batch {
			args = append(args, id)
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(batch)), ",")
		rows, err := db.QueryContext(ctx, `SELECT id, COALESCE(session_id, ''), json FROM import_checkpoints WHERE id IN (`+placeholders+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			row, err := scanSQLiteCheckpointIdentityFastRow(rows)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			sessionID := strings.TrimSpace(requested[row.ID])
			checkpoint, _, _, err := decodeSQLiteCheckpointRowWithCanonicalIdentity(row, row.ID, sessionID, false, true)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			out[row.ID] = checkpoint
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) updateImportCheckpointSQLite(ctx context.Context, parentSessionID string, id string, fn func(ImportCheckpoint, bool, time.Time) (ImportCheckpoint, bool, error), capability storeOwnerCapability) (ImportCheckpoint, bool, bool, error) {
	var out ImportCheckpoint
	changed := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if capability.bound() {
			lease, loadErr := loadSQLiteControlLease(ctx, tx)
			if loadErr != nil {
				return loadErr
			}
			if err := validateStoreOwnerCapability(&State{ControlLease: lease}, capability); err != nil {
				return err
			}
		}
		if err := ensureSQLiteParentUnfencedTx(ctx, tx, parentSessionID); err != nil {
			return err
		}
		current, found, opaque, err := loadSQLiteCanonicalCheckpointRow(ctx, tx, id, "")
		if err != nil {
			return err
		}
		now := time.Now()
		next, updateChanged, err := fn(current, found, now)
		if err != nil {
			return err
		}
		if err := validateImportCheckpointUpdateProvenance(id, current, found, next); err != nil {
			return err
		}
		if opaque && updateChanged {
			return ErrOpaqueCheckpoint
		}
		out = next
		handled = true
		if !updateChanged {
			return tx.Commit()
		}
		next.ID = id
		if err := upsertSQLiteImportCheckpointTx(ctx, tx, next); err != nil {
			return err
		}
		out = next
		changed = true
		return tx.Commit()
	})
	return out, changed, handled, err
}

func (s *Store) recordTranscriptCheckpointSQLite(ctx context.Context, parentSessionID string, checkpoint ImportCheckpoint, ledger TranscriptLedgerRecord, capability storeOwnerCapability) (bool, error) {
	handled := false
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if capability.bound() {
				lease, loadErr := loadSQLiteControlLease(ctx, tx)
				if loadErr != nil {
					return loadErr
				}
				if err := validateStoreOwnerCapability(&State{ControlLease: lease}, capability); err != nil {
					return err
				}
			}
			if err := ensureSQLiteParentUnfencedTx(ctx, tx, parentSessionID); err != nil {
				return err
			}
			state := State{
				SchemaVersion:            SchemaVersion,
				ImportCheckpoints:        map[string]ImportCheckpoint{},
				TranscriptLedger:         map[string]TranscriptLedgerRecord{},
				legacyUnresolvedSessions: map[string]bool{},
			}
			if existing, ok, disposition, err := loadSQLiteCheckpointForIDWithDisposition(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, checkpoint.ID); err != nil {
				return err
			} else if ok {
				if disposition == sqliteCheckpointMalformedCanonical {
					return ErrOpaqueCheckpoint
				}
				if err := validateLoadedTranscriptCheckpointRow(existing, checkpoint.ID, checkpoint.SessionID); err != nil {
					return err
				}
				state.ImportCheckpoints[existing.ID] = existing
			}
			if err := loadSQLiteSessionTranscriptCheckpointTx(ctx, tx, &state, checkpoint.SessionID); err != nil {
				return err
			}
			if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, checkpoint.SessionID); err != nil {
				return err
			}
			if err := validateTranscriptCheckpointRecordProvenance(&state, checkpoint); err != nil {
				return err
			}
			previous, _ := state.ImportCheckpoints[checkpoint.ID]
			if stateHasUnresolvedExecution(&state, checkpoint.SessionID) &&
				!importCheckpointIsExplicitHistoryRun(previous) {
				return ErrUnresolvedExecution
			}
			nextCheckpoint, nextLedger := applyRecordTranscriptCheckpointLocked(&state, checkpoint, ledger, time.Now())
			if err := upsertSQLiteImportCheckpointTx(ctx, tx, nextCheckpoint); err != nil {
				return err
			}
			if err := upsertSQLiteTranscriptLedgerTx(ctx, tx, nextLedger); err != nil {
				return err
			}
			if err := pruneSQLiteTranscriptLedgerTx(ctx, tx, maxRetainedTranscriptLedgerRecords); err != nil {
				return err
			}
			if err := pruneSQLiteTranscriptDeliveriesTx(ctx, tx, maxRetainedTranscriptDeliveries); err != nil {
				return err
			}
			handled = true
			return tx.Commit()
		})
	}
	if sessionID := strings.TrimSpace(checkpoint.SessionID); sessionID != "" {
		err := s.withSessionLock(ctx, sessionID, run)
		return handled, err
	}
	err := run()
	return handled, err
}

func (s *Store) updateSQLiteColdState(ctx context.Context, fn func(*State) error) (bool, error) {
	return s.updateSQLiteColdStateIfChanged(ctx, func(state *State) (bool, error) {
		if err := fn(state); err != nil {
			return false, err
		}
		return true, nil
	})
}

// updateSQLiteColdStateIfChanged keeps the transaction and cold-state lock
// narrow while allowing callers that can prove a no-op to avoid rewriting the
// state_json row and all split metadata. In particular, history-watch polls
// must not rewrite SQLite for an unchanged incomplete tail.
func (s *Store) updateSQLiteColdStateIfChanged(ctx context.Context, fn func(*State) (bool, error)) (bool, error) {
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state, err := loadSQLiteColdState(ctx, tx)
		if err != nil {
			return err
		}
		changed, err := fn(&state)
		if err != nil {
			return err
		}
		if !changed {
			return tx.Commit()
		}
		state.ensure(time.Now())
		if err := saveSQLiteColdStateTx(ctx, tx, state); err != nil {
			return err
		}
		return tx.Commit()
	})
	if errors.Is(err, errStoreNoChange) {
		return handled, nil
	}
	return handled, err
}

// updateSQLiteHistoryWatchIfChanged updates only the history-watch projection.
// Older pointers that lack the projection are read through JSON1 once and are
// materialized lazily on the first changed update. The projection lives in the
// existing state_meta table, so no schema migration is required; each update
// remains atomic under the same store lock and SQLite transaction.
func (s *Store) updateSQLiteHistoryWatchIfChanged(ctx context.Context, fn func(map[string]HistoryWatchCheckpoint, *time.Time) error) (bool, error) {
	return s.updateSQLiteHistoryWatchIfChangedWithCapability(ctx, fn, storeOwnerCapability{})
}

func (s *Store) updateSQLiteHistoryWatchIfChangedWithCapability(ctx context.Context, fn func(map[string]HistoryWatchCheckpoint, *time.Time) error, capability storeOwnerCapability) (bool, error) {
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if capability.bound() {
			lease, loadErr := loadSQLiteControlLease(ctx, tx)
			if loadErr != nil {
				return loadErr
			}
			if err := validateStoreOwnerCapability(&State{ControlLease: lease}, capability); err != nil {
				return err
			}
		}

		history, ready, projectionRevision, stateJSONRevision, found, err := loadSQLiteHistoryWatchProjection(ctx, tx)
		if err != nil {
			return err
		}
		// A missing/legacy projection or a revision mismatch means an older
		// full-state writer may have changed state_json. Start from that JSON and
		// rematerialize a current projection after applying this update.
		if found && (projectionRevision <= 0 || stateJSONRevision <= 0 || projectionRevision != stateJSONRevision) {
			found = false
		}
		if !found {
			// Old SQLite pointers have only state_json.  Read just the two
			// history-watch members once, then materialize the projection on
			// the first changed update below.
			var raw []byte
			var readyRaw sql.NullString
			if err := tx.QueryRowContext(ctx, `
SELECT CASE WHEN json_valid(value) THEN COALESCE(json_extract(value, '$.history_watch'), '{}') ELSE '{}' END,
       CASE WHEN json_valid(value) THEN json_extract(value, '$.history_watch_ready') ELSE NULL END
FROM state_meta WHERE key = 'state_json'`).Scan(&raw, &readyRaw); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return errors.New("sqlite teams store is missing state metadata")
				}
				return err
			}
			history = make(map[string]HistoryWatchCheckpoint)
			if len(raw) > 0 && string(raw) != "null" {
				var rows map[string]json.RawMessage
				if err := json.Unmarshal(raw, &rows); err != nil {
					return err
				}
				if err := decodeJSONHistoryWatchRows(rows, history); err != nil {
					return err
				}
			}
			if readyRaw.Valid && strings.TrimSpace(readyRaw.String) != "" {
				ready, err = time.Parse(time.RFC3339Nano, readyRaw.String)
				if err != nil {
					// This is only a scheduling hint. Ignore a malformed value and
					// let the next successful history update establish it again.
					ready = time.Time{}
				}
			}
			// A legacy SQLite pointer has no projection row, so the loader
			// cannot supply the state_json revision.  Fetch it before
			// materializing the projection; revision zero is intentionally
			// rejected by the overlay path and would otherwise force every
			// subsequent update back through JSON1.
			stateJSONRevision, err = sqliteStateJSONRevision(ctx, tx)
			if err != nil {
				return err
			}
		}
		before := cloneHistoryWatchCheckpoints(history)
		beforeReady := ready
		if err := fn(history, &ready); err != nil {
			return err
		}
		if !historyWatchCheckpointsEqual(before, history) || !beforeReady.Equal(ready) {
			if err := upsertSQLiteHistoryWatchProjectionTx(ctx, tx, history, ready, stateJSONRevision); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	if errors.Is(err, errStoreNoChange) {
		return handled, nil
	}
	return handled, err
}

func (s *Store) recordControlChatBindingSQLite(ctx context.Context, update ControlChatBindingUpdate) (bool, bool, error) {
	changed := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state, err := loadSQLiteColdState(ctx, tx)
		if err != nil {
			return err
		}
		if err := overlaySQLiteRuntimeState(ctx, tx, &state); err != nil {
			return err
		}
		changed = applyControlChatBindingUpdate(&state, update, time.Now())
		if !changed {
			return tx.Commit()
		}
		state.ensure(time.Now())
		if err := saveSQLiteRuntimeStateTx(ctx, tx, state); err != nil {
			return err
		}
		if err := saveSQLiteColdStateTx(ctx, tx, state); err != nil {
			return err
		}
		return tx.Commit()
	})
	return changed, handled, err
}

func (s *Store) historyWatchStateSQLite(ctx context.Context) (State, bool, error) {
	var state State
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		state, err = loadSQLiteColdState(ctx, db)
		handled = true
		return err
	})
	return state, handled, err
}

func (s *Store) historyWatchOriginStateSQLite(ctx context.Context, threadID string) (State, bool, error) {
	state := State{
		SchemaVersion: SchemaVersion,
		Sessions:      make(map[string]SessionContext),
		Turns:         make(map[string]Turn),
		InboundEvents: make(map[string]InboundEvent),
	}
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		if err := loadSQLiteSessionMap(ctx, db, `SELECT id, teams_chat_id, status, updated_at, json FROM sessions`, state.Sessions); err != nil {
			return err
		}
		if err := loadSQLiteTurnMap(ctx, db, `SELECT id, session_id, status, json FROM turns`, state.Turns, true); err != nil {
			return err
		}

		threadID = strings.TrimSpace(threadID)
		sessionIDs := make(map[string]struct{})
		inboundIDs := make(map[string]struct{})
		for _, session := range state.Sessions {
			if strings.TrimSpace(session.CodexThreadID) == threadID {
				if sessionID := strings.TrimSpace(session.ID); sessionID != "" {
					sessionIDs[sessionID] = struct{}{}
				}
			}
		}
		for _, turn := range state.Turns {
			if strings.TrimSpace(turn.CodexThreadID) != threadID {
				continue
			}
			if sessionID := strings.TrimSpace(turn.SessionID); sessionID != "" {
				sessionIDs[sessionID] = struct{}{}
			}
			if inboundID := strings.TrimSpace(turn.InboundEventID); inboundID != "" {
				inboundIDs[inboundID] = struct{}{}
			}
		}
		if err := loadSQLiteHistoryWatchInboundRows(ctx, db, "session_id", sortedMapKeys(sessionIDs), state.InboundEvents); err != nil {
			return err
		}
		return loadSQLiteHistoryWatchInboundRows(ctx, db, "id", sortedMapKeys(inboundIDs), state.InboundEvents)
	})
	return state, handled, err
}

func loadSQLiteHistoryWatchInboundRows(ctx context.Context, db *sql.DB, column string, values []string, out map[string]InboundEvent) error {
	if len(values) == 0 {
		return nil
	}
	switch column {
	case "id", "session_id":
	default:
		return fmt.Errorf("unsupported history-watch inbound lookup column %q", column)
	}
	const batchSize = 400
	for start := 0; start < len(values); start += batchSize {
		end := min(start+batchSize, len(values))
		placeholders := make([]string, 0, end-start)
		args := make([]any, 0, end-start)
		for _, value := range values[start:end] {
			placeholders = append(placeholders, "?")
			args = append(args, value)
		}
		query := `SELECT json FROM inbound_events WHERE ` + column + ` IN (` + strings.Join(placeholders, ",") + `)`
		if err := loadSQLiteJSONMapBestEffort(ctx, db, query, out, func(v InboundEvent) string { return v.ID }, args...); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) claimControlLeaseSQLite(ctx context.Context, claim ControlLeaseClaim) (ControlLeaseDecision, bool, error) {
	var out ControlLeaseDecision
	handled, err := s.withSQLiteRuntimeDB(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state, seen, invalid, err := loadSQLiteRequiredRuntimeStateDetailed(ctx, tx)
		if err != nil {
			return err
		}
		materialized, err := sqliteRuntimeProjectionMaterialized(ctx, tx)
		if err != nil {
			return err
		}
		if len(invalid) > 0 {
			// An unknown control-lease status is not an expired lease proof. It
			// is a semantically valid row whose meaning this binary does not
			// understand, so never fall back to the older cold copy and claim
			// over a possibly live writer.
			if err := invalidSQLiteRequiredRuntimeError(invalid); errors.Is(err, ErrControlLeaseStatusUnknown) {
				return err
			}
			// A malformed required runtime row is different from a legacy missing
			// projection.  It may be repaired only when an independently decoded
			// lease proves that the previous owner is no longer live.  Otherwise an
			// unknown row could hide a live writer and a replacement claim would
			// split ownership.
			cold, loadErr := loadSQLiteColdState(ctx, tx)
			if loadErr != nil {
				return loadErr
			}
			lease := cold.ControlLease
			if seen[sqliteRuntimeKeyControlLease] {
				lease = state.ControlLease
			} else if !sqliteCorruptControlLeaseExpiryProven(cold.ControlLease, state, claim) {
				// The cold control-lease copy is only a fallback snapshot.  When
				// the current lease row itself is corrupt, an expired cold copy
				// cannot establish that the current writer is gone: a valid
				// runtime owner row may have renewed the lease after the cold
				// snapshot was written.  Require a matching, stale/dead runtime
				// owner witness before allowing recovery.
				return fmt.Errorf("%w: sqlite Teams runtime state is corrupt; control lease expiry is not provable", ErrControlLeaseStateUntrusted)
			}
			if !sqliteControlLeaseDefinitelyExpired(lease, claim.Now) {
				return fmt.Errorf("%w: sqlite Teams runtime state is corrupt; control lease expiry is not provable", ErrControlLeaseStateUntrusted)
			}
			overlaySQLiteRequiredRuntimeState(&cold, state, seen)
			return claimControlLeaseSQLiteWithRecoveredRuntime(ctx, tx, claim, cold, invalid, &out)
		}
		if !sqliteRuntimeStateUsable(seen) {
			if materialized {
				return sqliteRuntimeProjectionIncompleteError("missing required runtime rows")
			}
			if !seen[sqliteRuntimeKeyControlLease] && sqliteRuntimeProjectionStarted(seen, invalid) {
				return fmt.Errorf("%w: sqlite control lease projection is missing", ErrControlLeaseStateUntrusted)
			}
			state, err = loadSQLiteLivenessState(ctx, tx)
			if err != nil {
				return err
			}
		}
		decision, err := claimControlLeaseInState(&state, claim)
		out = decision
		if err != nil && !errors.Is(err, errStoreNoChange) {
			return err
		}
		if !errors.Is(err, errStoreNoChange) {
			if err := saveSQLiteRequiredRuntimeStateTx(ctx, tx, state); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return out, handled, err
}

// claimControlLeaseSQLiteWithRecoveredRuntime completes the expired-lease
// repair after the caller has established an independent liveness proof.  It
// is kept separate from the normal path so the common heartbeat/claim case
// does not allocate or write an opaque-row record.
func claimControlLeaseSQLiteWithRecoveredRuntime(ctx context.Context, tx *sql.Tx, claim ControlLeaseClaim, cold State, invalid map[string][]byte, out *ControlLeaseDecision) error {
	// The caller passes the cold state as the base.  Valid runtime rows are
	// authoritative where present, while invalid rows deliberately do not
	// overwrite the independently decoded cold values.
	decision, err := claimControlLeaseInState(&cold, claim)
	if out != nil {
		*out = decision
	}
	if err != nil && !errors.Is(err, errStoreNoChange) {
		return err
	}
	if !errors.Is(err, errStoreNoChange) {
		if err := quarantineSQLiteRuntimeRowsTx(ctx, tx, invalid); err != nil {
			return err
		}
		if err := saveSQLiteRequiredRuntimeStateTx(ctx, tx, cold); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func overlaySQLiteRequiredRuntimeState(state *State, runtime State, seen map[string]bool) {
	if state == nil {
		return
	}
	use := func(key string) bool { return seen == nil || seen[key] }
	if use(sqliteRuntimeKeyScope) {
		state.Scope = runtime.Scope
	}
	if use(sqliteRuntimeKeyMachineIdentity) {
		state.MachineIdentity = runtime.MachineIdentity
	}
	if use(sqliteRuntimeKeyMachines) {
		state.Machines = runtime.Machines
	}
	if use(sqliteRuntimeKeyControlLease) {
		state.ControlLease = runtime.ControlLease
	}
	if use(sqliteRuntimeKeyServiceOwner) {
		state.ServiceOwner = runtime.ServiceOwner
	}
	if use(sqliteRuntimeKeyLockOwner) {
		state.LockOwner = runtime.LockOwner
	}
	state.ensure(time.Time{})
}

func sqliteControlLeaseDefinitelyExpired(lease ControlLease, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	if strings.TrimSpace(lease.HolderMachineID) == "" || lease.LeaseUntil.IsZero() {
		return false
	}
	if lease.Status != "" && lease.Status != ControlLeaseStatusActive {
		return false
	}
	return !lease.LeaseUntil.After(now)
}

// sqliteCorruptControlLeaseExpiryProven allows the cold lease snapshot to be
// used only when the current runtime owner projection independently confirms
// that the holder represented by that snapshot is no longer live.  A cold
// lease can be older than a valid runtime heartbeat, so checking its expiry
// alone would let a claimant split ownership after a torn control_lease write.
func sqliteCorruptControlLeaseExpiryProven(cold ControlLease, runtime State, claim ControlLeaseClaim) bool {
	if !sqliteControlLeaseDefinitelyExpired(cold, claim.Now) || strings.TrimSpace(cold.HolderMachineID) == "" {
		return false
	}
	foundMatchingWitness := false
	for _, owner := range []*OwnerMetadata{runtime.ServiceOwner, runtime.LockOwner} {
		if owner == nil || strings.TrimSpace(owner.MachineID) == "" {
			continue
		}
		if !IsStale(*owner, claim.Duration, claim.Now) && !OwnerAppearsLocallyDead(*owner) {
			return false
		}
		if strings.TrimSpace(owner.MachineID) == strings.TrimSpace(cold.HolderMachineID) &&
			(cold.Generation <= 0 || owner.LeaseGeneration == cold.Generation) {
			foundMatchingWitness = true
		}
	}
	return foundMatchingWitness
}

const sqliteRuntimeOpaqueMetaPrefix = "runtime_opaque:"

func quarantineSQLiteRuntimeRowsTx(ctx context.Context, tx *sql.Tx, invalid map[string][]byte) error {
	keys := make([]string, 0, len(invalid))
	for key := range invalid {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		raw := invalid[key]
		metaKey := sqliteRuntimeOpaqueMetaPrefix + key + ":" + sha256Bytes(raw)
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO state_meta(key, value) VALUES (?, ?)`, metaKey, raw); err != nil {
			return err
		}
	}
	return nil
}

func loadSQLiteRequiredRuntimeState(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, map[string]bool, error) {
	state, seen, invalid, err := loadSQLiteRequiredRuntimeStateDetailed(ctx, q)
	if err != nil {
		return State{}, nil, err
	}
	if len(invalid) != 0 {
		return State{}, nil, invalidSQLiteRequiredRuntimeError(invalid)
	}
	return state, seen, nil
}

// loadSQLiteRequiredRuntimeStateDetailed decodes required runtime rows one at
// a time.  Liveness callers need to distinguish a missing/invalid row from a
// query failure: a malformed projection row must not make an expired lease
// impossible to recover, but it must never be treated as valid ownership
// evidence either.  The normal/full-state loader above remains strict so a
// diagnostic or migration cannot silently hide corrupt state.
func loadSQLiteRequiredRuntimeStateDetailed(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, map[string]bool, map[string][]byte, error) {
	state := State{SchemaVersion: SchemaVersion, Machines: map[string]MachineRecord{}}
	seen := make(map[string]bool)
	invalid := make(map[string][]byte)
	keys := make([]string, len(sqliteRuntimeRequiredKeys))
	args := make([]any, len(sqliteRuntimeRequiredKeys))
	for i, key := range sqliteRuntimeRequiredKeys {
		keys[i] = "?"
		args[i] = key
	}
	rows, err := q.QueryContext(ctx, `SELECT key, json FROM runtime_state WHERE key IN (`+strings.Join(keys, ",")+`)`, args...)
	if err != nil {
		return State{}, nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			return State{}, nil, nil, err
		}
		valid := true
		switch key {
		case sqliteRuntimeKeyScope:
			if err := json.Unmarshal(raw, &state.Scope); err != nil {
				valid = false
			}
		case sqliteRuntimeKeyMachineIdentity:
			if err := json.Unmarshal(raw, &state.MachineIdentity); err != nil {
				valid = false
			}
		case sqliteRuntimeKeyMachines:
			if err := json.Unmarshal(raw, &state.Machines); err != nil {
				valid = false
			}
			if valid && state.Machines == nil {
				state.Machines = map[string]MachineRecord{}
			}
		case sqliteRuntimeKeyControlLease:
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				valid = false
			} else if err := json.Unmarshal(raw, &state.ControlLease); err != nil {
				valid = false
			}
			if valid {
				if err := validateControlLeaseShape(state.ControlLease); err != nil {
					valid = false
				}
			}
		case sqliteRuntimeKeyServiceOwner:
			var owner *OwnerMetadata
			if err := json.Unmarshal(raw, &owner); err != nil {
				valid = false
			}
			if valid {
				state.ServiceOwner = owner
			}
		case sqliteRuntimeKeyLockOwner:
			var owner *OwnerMetadata
			if err := json.Unmarshal(raw, &owner); err != nil {
				valid = false
			}
			if valid {
				state.LockOwner = owner
			}
		default:
			continue
		}
		if valid {
			seen[key] = true
		} else {
			invalid[key] = append([]byte(nil), raw...)
		}
	}
	if err := rows.Err(); err != nil {
		return State{}, nil, nil, err
	}
	state.ensure(time.Time{})
	return state, seen, invalid, nil
}

func saveSQLiteRequiredRuntimeStateTx(ctx context.Context, tx *sql.Tx, state State) error {
	values := map[string]any{
		sqliteRuntimeKeyScope:           state.Scope,
		sqliteRuntimeKeyMachineIdentity: state.MachineIdentity,
		sqliteRuntimeKeyMachines:        state.Machines,
		sqliteRuntimeKeyControlLease:    state.ControlLease,
		sqliteRuntimeKeyServiceOwner:    state.ServiceOwner,
		sqliteRuntimeKeyLockOwner:       state.LockOwner,
	}
	for _, key := range sqliteRuntimeRequiredKeys {
		raw, err := json.Marshal(values[key])
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_state(key, json) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET json = excluded.json`, key, raw); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO state_meta(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, sqliteRuntimeProjectionMaterializedKey, sqliteRuntimeProjectionMaterializedValue)
	return err
}

// saveSQLiteRequiredRuntimeStatePreservingInvalidTx updates the rows that can
// be decoded while leaving an opaque required row byte-for-byte intact. This
// is used by low-risk owner cleanup/release operations: a malformed scope or
// machine projection must not strand a valid lease, but the cleanup must not
// replace evidence that still requires explicit repair. The invalid raw bytes
// are copied to state_meta by the caller before commit.
func saveSQLiteRequiredRuntimeStatePreservingInvalidTx(ctx context.Context, tx *sql.Tx, state State, invalid map[string][]byte) error {
	values := map[string]any{
		sqliteRuntimeKeyScope:           state.Scope,
		sqliteRuntimeKeyMachineIdentity: state.MachineIdentity,
		sqliteRuntimeKeyMachines:        state.Machines,
		sqliteRuntimeKeyControlLease:    state.ControlLease,
		sqliteRuntimeKeyServiceOwner:    state.ServiceOwner,
		sqliteRuntimeKeyLockOwner:       state.LockOwner,
	}
	for _, key := range sqliteRuntimeRequiredKeys {
		if _, opaque := invalid[key]; opaque {
			continue
		}
		raw, err := json.Marshal(values[key])
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_state(key, json) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET json = excluded.json`, key, raw); err != nil {
			return err
		}
	}
	return markSQLiteRuntimeProjectionMaterializedTx(ctx, tx)
}

func invalidSQLiteRequiredRuntimeError(invalid map[string][]byte) error {
	if raw, ok := invalid[sqliteRuntimeKeyControlLease]; ok {
		var lease ControlLease
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("%w: invalid sqlite control lease row: null", ErrControlLeaseStateUntrusted)
		}
		if err := json.Unmarshal(raw, &lease); err != nil {
			return fmt.Errorf("%w: invalid sqlite control lease row: %v", ErrControlLeaseStateUntrusted, err)
		}
		if err := validateControlLeaseShape(lease); err != nil {
			return err
		}
		return fmt.Errorf("%w: invalid sqlite control lease row", ErrControlLeaseStateUntrusted)
	}
	keys := make([]string, 0, len(invalid))
	for key := range invalid {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return nil
	}
	return fmt.Errorf("invalid sqlite required runtime row %q", keys[0])
}

// loadSQLiteLivenessState is the compatibility fallback for an incomplete
// runtime projection. It starts from the cold state document but overlays only
// the ownership/lease rows that are required for a liveness decision. Optional
// lifecycle rows must not be decoded here: a damaged upgrade or auto-update
// row must not prevent a replacement owner from claiming the service.
func loadSQLiteLivenessState(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, error) {
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT value FROM state_meta WHERE key = 'state_json'`).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return State{}, errors.New("sqlite teams store is missing state metadata")
		}
		return State{}, err
	}
	if len(raw) == 0 {
		return State{}, errors.New("sqlite teams store has empty state metadata")
	}
	state, err := loadStateData(raw)
	if err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	runtimeState, seen, invalid, err := loadSQLiteRequiredRuntimeStateDetailed(ctx, q)
	if err != nil {
		return State{}, err
	}
	if !seen[sqliteRuntimeKeyControlLease] && sqliteRuntimeProjectionStarted(seen, invalid) {
		return State{}, fmt.Errorf("%w: sqlite control lease projection is missing", ErrControlLeaseStateUntrusted)
	}
	// Overlay each required row independently. A partially materialized
	// projection is expected during older migrations or narrow repairs; rows
	// that are present (including an explicit NULL owner) are still stronger
	// evidence than the cold document. Missing rows continue to use the cold
	// compatibility value. Optional runtime rows are intentionally never read.
	if seen[sqliteRuntimeKeyScope] {
		state.Scope = runtimeState.Scope
	}
	if seen[sqliteRuntimeKeyMachineIdentity] {
		state.MachineIdentity = runtimeState.MachineIdentity
	}
	if seen[sqliteRuntimeKeyMachines] {
		state.Machines = runtimeState.Machines
	}
	if seen[sqliteRuntimeKeyControlLease] {
		state.ControlLease = runtimeState.ControlLease
	}
	if seen[sqliteRuntimeKeyServiceOwner] {
		state.ServiceOwner = runtimeState.ServiceOwner
	}
	if seen[sqliteRuntimeKeyLockOwner] {
		state.LockOwner = runtimeState.LockOwner
	}
	// Invalid required rows are deliberately treated as absent here.  This
	// fallback is used only to make a liveness decision; the claimant path keeps
	// their raw bytes in an opaque audit record after independently proving that
	// the old lease is expired.  Do not let a malformed row become a replacement
	// owner's evidence.
	_ = invalid
	state.ensure(time.Time{})
	if err := validateControlLeaseShape(state.ControlLease); err != nil {
		return State{}, err
	}
	return state, nil
}

// loadSQLiteControlLease reads only the owner capability needed by an
// attempt-scoped outbox CAS. Keeping this narrower than loadSQLiteLivenessState
// avoids decoding the cold state document on every normal send transition,
// while retaining the legacy fallback for stores created before the runtime
// projection was materialized.
func loadSQLiteControlLease(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (ControlLease, error) {
	var raw []byte
	err := q.QueryRowContext(ctx, `SELECT json FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyControlLease).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		// A completely empty runtime table is the pre-migration legacy shape and
		// may safely fall back to state_json. Once any required projection row
		// exists, however, a missing lease row is an incomplete ownership record;
		// using the cold copy could claim over a writer whose lease update was
		// torn. Hold until the projection is explicitly repaired.
		materialized, materializedErr := sqliteRuntimeProjectionMaterialized(ctx, q)
		if materializedErr != nil {
			return ControlLease{}, materializedErr
		}
		if materialized {
			return ControlLease{}, sqliteRuntimeProjectionIncompleteError("control lease row is missing")
		}
		var runtimeKey string
		projectionErr := q.QueryRowContext(ctx, `SELECT key FROM runtime_state WHERE key IN (?, ?, ?, ?, ?, ?) LIMIT 1`,
			sqliteRuntimeRequiredKeys[0], sqliteRuntimeRequiredKeys[1], sqliteRuntimeRequiredKeys[2],
			sqliteRuntimeRequiredKeys[3], sqliteRuntimeRequiredKeys[4], sqliteRuntimeRequiredKeys[5]).Scan(&runtimeKey)
		if projectionErr == nil {
			return ControlLease{}, fmt.Errorf("%w: sqlite control lease projection is missing", ErrControlLeaseStateUntrusted)
		}
		if !errors.Is(projectionErr, sql.ErrNoRows) {
			return ControlLease{}, projectionErr
		}
		state, loadErr := loadSQLiteLivenessState(ctx, q)
		if loadErr != nil {
			return ControlLease{}, loadErr
		}
		if err := validateControlLeaseShape(state.ControlLease); err != nil {
			return ControlLease{}, err
		}
		return state.ControlLease, nil
	}
	if err != nil {
		return ControlLease{}, err
	}
	var lease ControlLease
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ControlLease{}, fmt.Errorf("%w: invalid sqlite control lease row: null", ErrControlLeaseStateUntrusted)
	}
	if err := json.Unmarshal(raw, &lease); err != nil {
		return ControlLease{}, fmt.Errorf("%w: invalid sqlite control lease row: %v", ErrControlLeaseStateUntrusted, err)
	}
	if err := validateControlLeaseShape(lease); err != nil {
		return ControlLease{}, err
	}
	return lease, nil
}

func sqliteStoreOwnerCapabilityMatchesTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, machineID string, leaseGeneration int64) (bool, error) {
	if leaseGeneration <= 0 {
		return true, nil
	}
	lease, err := loadSQLiteControlLease(ctx, q)
	if err != nil {
		return false, err
	}
	state := State{ControlLease: lease}
	return storeOwnerCapabilityMatchesState(&state, machineID, leaseGeneration), nil
}

func sqliteStoreActiveOwnerCapabilityMatchesTx(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, machineID string, leaseGeneration int64) (bool, error) {
	if leaseGeneration <= 0 {
		return true, nil
	}
	lease, err := loadSQLiteControlLease(ctx, q)
	if err != nil {
		return false, err
	}
	state := State{ControlLease: lease}
	return storeOwnerCapabilityMatchesActiveLease(&state, machineID, leaseGeneration), nil
}

func (s *Store) validateControlLeaseSQLite(ctx context.Context, machineID string, generation int64, now time.Time) (ControlLease, bool, error) {
	var out ControlLease
	handled, err := s.withSQLiteRuntimeDB(ctx, func(db *sql.DB) error {
		var raw []byte
		err := db.QueryRowContext(ctx, `SELECT json FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyControlLease).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			state, loadErr := loadSQLiteLivenessState(ctx, db)
			if loadErr != nil {
				return loadErr
			}
			out = state.ControlLease
		} else if err != nil {
			return err
		} else if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("%w: invalid sqlite control lease row: null", ErrControlLeaseStateUntrusted)
		} else if err := json.Unmarshal(raw, &out); err != nil {
			return fmt.Errorf("%w: invalid sqlite control lease row: %v", ErrControlLeaseStateUntrusted, err)
		}
		if err := validateControlLeaseShape(out); err != nil {
			return err
		}
		if out.HolderMachineID != machineID || out.Generation != generation || !out.LeaseUntil.After(now) {
			return ErrControlLeaseNotHeld
		}
		return nil
	})
	return out, handled, err
}

func (s *Store) releaseControlLeaseSQLite(ctx context.Context, machineID string, generation int64) (bool, bool, error) {
	released := false
	handled, err := s.withSQLiteRuntimeDB(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state, seen, invalid, err := loadSQLiteRequiredRuntimeStateDetailed(ctx, tx)
		if err != nil {
			return err
		}
		if _, opaque := invalid[sqliteRuntimeKeyControlLease]; opaque {
			return invalidSQLiteRequiredRuntimeError(invalid)
		}
		if !sqliteRuntimeStateUsable(seen) {
			state, err = loadSQLiteLivenessState(ctx, tx)
			if err != nil {
				return err
			}
		}
		lease := state.ControlLease
		if err := validateKnownControlLeaseStatus(lease); err != nil {
			return err
		}
		if lease.HolderMachineID != machineID || lease.Generation != generation {
			return tx.Commit()
		}
		if strings.TrimSpace(lease.ScopeID) == "" {
			lease.ScopeID = state.Scope.ID
		}
		lease.HolderMachineID = ""
		lease.HolderKind = ""
		lease.Priority = 0
		lease.Status = ""
		lease.LeaseUntil = time.Time{}
		lease.LastHeartbeat = time.Time{}
		lease.UpdatedAt = time.Now()
		state.ControlLease = lease
		if machine := state.Machines[machineID]; machine.ID != "" {
			machine.Status = MachineStatusStandby
			machine.UpdatedAt = time.Now()
			state.Machines[machineID] = machine
		}
		if err := quarantineSQLiteRuntimeRowsTx(ctx, tx, invalid); err != nil {
			return err
		}
		if err := saveSQLiteRequiredRuntimeStatePreservingInvalidTx(ctx, tx, state, invalid); err != nil {
			return err
		}
		released = true
		return tx.Commit()
	})
	return released, handled, err
}

func (s *Store) recordOwnerHeartbeatSQLite(ctx context.Context, owner OwnerMetadata, staleAfter time.Duration, now time.Time) (OwnerMetadata, bool, error) {
	var out OwnerMetadata
	handled, err := s.withSQLiteRuntimeDB(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state, _, invalid, present, err := loadSQLiteOwnerRuntimeDetailed(ctx, tx)
		if err != nil {
			return err
		}
		if len(invalid) != 0 {
			// An untrusted owner row may conceal a live process. The unscoped
			// compatibility heartbeat has no capability with which to repair an
			// entirely opaque owner projection, so require at least one valid
			// owner witness and let the normal conflict check decide whether the
			// caller is that owner.
			if _, ok := state.readOwner(); !ok {
				return invalidSQLiteOwnerRuntimeError(invalid)
			}
		}
		if !present {
			state, err = loadSQLiteLivenessState(ctx, tx)
			if err != nil {
				return err
			}
		}
		// The owner-only projection intentionally avoids optional runtime rows,
		// but the control lease is part of the safety boundary even for the
		// legacy heartbeat API. Load it explicitly so an unknown lease status
		// cannot be overwritten by an unscoped owner heartbeat just because the
		// owner rows themselves are valid.
		state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
		if err != nil {
			return err
		}
		next, err := recordOwnerHeartbeatInState(&state, owner, staleAfter, now)
		if err != nil {
			return err
		}
		out = next
		if err := saveSQLiteOwnerRuntimeTx(ctx, tx, next); err != nil {
			return err
		}
		if err := quarantineSQLiteRuntimeRowsTx(ctx, tx, invalid); err != nil {
			return err
		}
		return tx.Commit()
	})
	return out, handled, err
}

// recordOwnerHeartbeatForLeaseSQLite is deliberately narrower than a full
// state write. It reads the owner rows and the control-lease row in one
// transaction, validates the captured generation, then persists both before
// commit. A delayed callback from a retired listener therefore cannot renew
// or replace the current owner.
func (s *Store) recordOwnerHeartbeatForLeaseSQLite(ctx context.Context, owner OwnerMetadata, staleAfter time.Duration, leaseDuration time.Duration, now time.Time) (OwnerMetadata, bool, error) {
	var out OwnerMetadata
	handled, err := s.withSQLiteRuntimeDB(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state, _, invalid, present, err := loadSQLiteOwnerRuntimeDetailed(ctx, tx)
		if err != nil {
			return err
		}
		// A capability-bound heartbeat can repair an opaque owner projection,
		// but only after the control-lease row independently proves that this
		// callback owns the current generation. If a valid peer owner exists,
		// retain it as an additional conflict witness; otherwise use the cold
		// state only as compatibility evidence and let the capability check run
		// before any write.
		if len(invalid) != 0 {
			if _, ok := state.readOwner(); !ok {
				state, err = loadSQLiteLivenessState(ctx, tx)
				if err != nil {
					return err
				}
			}
		} else if !present {
			state, err = loadSQLiteLivenessState(ctx, tx)
			if err != nil {
				return err
			}
		}
		state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
		if err != nil {
			return err
		}
		next, err := recordOwnerHeartbeatForLeaseInState(&state, owner, staleAfter, leaseDuration, now)
		if err != nil {
			return err
		}
		out = next
		if err := saveSQLiteOwnerRuntimeTx(ctx, tx, next); err != nil {
			return err
		}
		if err := saveSQLiteControlLeaseTx(ctx, tx, state.ControlLease); err != nil {
			return err
		}
		if err := quarantineSQLiteRuntimeRowsTx(ctx, tx, invalid); err != nil {
			return err
		}
		return tx.Commit()
	})
	return out, handled, err
}

func saveSQLiteControlLeaseTx(ctx context.Context, tx *sql.Tx, lease ControlLease) error {
	raw, err := json.Marshal(lease)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO runtime_state(key, json) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET json = excluded.json`, sqliteRuntimeKeyControlLease, raw); err != nil {
		return err
	}
	return markSQLiteRuntimeProjectionMaterializedTx(ctx, tx)
}

// loadSQLiteOwnerRuntime intentionally reads only the two owner rows. An
// unrelated optional runtime row (upgrade/service-control/auto-update) is not
// part of liveness proof and must not be able to stop a heartbeat or owner
// diagnostic.
func loadSQLiteOwnerRuntime(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (State, bool, bool, error) {
	state, _, invalid, present, err := loadSQLiteOwnerRuntimeDetailed(ctx, q)
	if err != nil {
		return State{}, false, false, err
	}
	if len(invalid) != 0 {
		return State{}, false, false, invalidSQLiteOwnerRuntimeError(invalid)
	}
	return state, state.ServiceOwner != nil || state.LockOwner != nil, present, nil
}

func invalidSQLiteOwnerRuntimeError(invalid map[string][]byte) error {
	keys := make([]string, 0, len(invalid))
	for key := range invalid {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return nil
	}
	return fmt.Errorf("invalid sqlite owner runtime row %q", keys[0])
}

// loadSQLiteOwnerRuntimeDetailed reads the two owner rows independently. A
// targeted heartbeat only needs a valid owner witness and an independently
// validated control lease; an unrelated malformed compatibility copy should
// not force a healthy listener into a restart loop. The strict wrapper above
// remains available to callers that require a complete owner projection.
func loadSQLiteOwnerRuntimeDetailed(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (State, map[string]bool, map[string][]byte, bool, error) {
	state := State{}
	seen := make(map[string]bool)
	invalid := make(map[string][]byte)
	present := false
	for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
		var raw []byte
		err := q.QueryRowContext(ctx, `SELECT json FROM runtime_state WHERE key = ?`, key).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return State{}, nil, nil, false, err
		}
		present = true
		var owner *OwnerMetadata
		if err := json.Unmarshal(raw, &owner); err != nil {
			invalid[key] = append([]byte(nil), raw...)
			continue
		}
		seen[key] = true
		if owner == nil {
			continue
		}
		if key == sqliteRuntimeKeyServiceOwner {
			state.ServiceOwner = owner
		} else {
			state.LockOwner = owner
		}
	}
	return state, seen, invalid, present, nil
}

func saveSQLiteOwnerRuntimeTx(ctx context.Context, tx *sql.Tx, owner OwnerMetadata) error {
	raw, err := json.Marshal(owner)
	if err != nil {
		return err
	}
	for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_state(key, json) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET json = excluded.json`, key, raw); err != nil {
			return err
		}
	}
	return markSQLiteRuntimeProjectionMaterializedTx(ctx, tx)
}

func (s *Store) readOwnerSQLite(ctx context.Context) (OwnerMetadata, bool, bool, error) {
	var out OwnerMetadata
	found := false
	handled, err := s.withSQLiteRuntimeDB(ctx, func(db *sql.DB) error {
		state, _, present, err := loadSQLiteOwnerRuntime(ctx, db)
		if err != nil {
			return err
		}
		if present {
			// Owner rows are compatibility copies.  Load the lease alongside
			// them so State.readOwner can prefer a valid lock-owner witness when
			// the service-owner copy is stale or semantically incomplete.
			state.ControlLease, err = loadSQLiteControlLease(ctx, db)
			if err != nil {
				return err
			}
		} else {
			state, err = loadSQLiteLivenessState(ctx, db)
			if err != nil {
				return err
			}
		}
		out, found = state.readOwner()
		return nil
	})
	return out, found, handled, err
}

func (s *Store) clearOwnerIfSameSQLite(ctx context.Context, owner OwnerMetadata) (bool, bool, error) {
	cleared := false
	handled, err := s.withSQLiteRuntimeDB(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state, seen, invalid, err := loadSQLiteRequiredRuntimeStateDetailed(ctx, tx)
		if err != nil {
			return err
		}
		if _, opaque := invalid[sqliteRuntimeKeyControlLease]; opaque {
			return invalidSQLiteRequiredRuntimeError(invalid)
		}
		for _, key := range []string{sqliteRuntimeKeyServiceOwner, sqliteRuntimeKeyLockOwner} {
			if _, opaque := invalid[key]; opaque {
				return invalidSQLiteRequiredRuntimeError(invalid)
			}
		}
		if !sqliteRuntimeStateUsable(seen) {
			state, err = loadSQLiteLivenessState(ctx, tx)
			if err != nil {
				return err
			}
		}
		existing, ok := state.readOwner()
		if !ok || !sameOwnerInstance(existing, owner) {
			return tx.Commit()
		}
		state.ServiceOwner = nil
		state.LockOwner = nil
		if err := quarantineSQLiteRuntimeRowsTx(ctx, tx, invalid); err != nil {
			return err
		}
		if err := saveSQLiteRequiredRuntimeStatePreservingInvalidTx(ctx, tx, state, invalid); err != nil {
			return err
		}
		cleared = true
		return tx.Commit()
	})
	return cleared, handled, err
}

func upsertSQLiteSessionTx(ctx context.Context, tx *sql.Tx, v SessionContext) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sessions(id, teams_chat_id, status, updated_at, json) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET teams_chat_id = excluded.teams_chat_id, status = excluded.status, updated_at = excluded.updated_at, json = excluded.json`,
		v.ID, v.TeamsChatID, string(v.Status), sqliteTime(v.UpdatedAt), data)
	return err
}

func upsertSQLiteInboundTx(ctx context.Context, tx *sql.Tx, v InboundEvent) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO inbound_events(id, session_id, teams_chat_id, teams_message_id, status, created_at, updated_at, received_at, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, teams_chat_id = excluded.teams_chat_id, teams_message_id = excluded.teams_message_id, status = excluded.status, created_at = excluded.created_at, updated_at = excluded.updated_at, received_at = excluded.received_at, json = excluded.json`,
		v.ID, v.SessionID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), string(v.Status), sqliteTime(v.CreatedAt), sqliteTime(v.UpdatedAt), sqliteTime(v.ReceivedAt), data)
	return err
}

func upsertSQLiteTurnTx(ctx context.Context, tx *sql.Tx, v Turn) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO turns(id, session_id, status, queued_at, created_at, updated_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, status = excluded.status, queued_at = excluded.queued_at, created_at = excluded.created_at, updated_at = excluded.updated_at, json = excluded.json`,
		v.ID, v.SessionID, string(v.Status), sqliteTime(queuedTurnSortTime(v)), sqliteTime(v.CreatedAt), sqliteTime(v.UpdatedAt), data)
	return err
}

func upsertSQLiteOutboxTx(ctx context.Context, tx *sql.Tx, v OutboxMessage) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, post_send_effects_pending, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, turn_id = excluded.turn_id, teams_chat_id = excluded.teams_chat_id, teams_message_id = excluded.teams_message_id, status = excluded.status, sequence = excluded.sequence, created_at = excluded.created_at, deliver_after = excluded.deliver_after, post_send_effects_pending = excluded.post_send_effects_pending, json = excluded.json`,
		v.ID, v.SessionID, v.TurnID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), string(v.Status), v.Sequence, sqliteTime(v.CreatedAt), sqliteTime(v.NextAttemptAt), sqliteBool(v.PostSendEffectsPending), data)
	return err
}

func upsertSQLiteProvenanceTx(ctx context.Context, tx *sql.Tx, v MessageProvenanceRecord) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO message_provenance(id, teams_chat_id, teams_message_id, origin, session_id, json) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET teams_chat_id = excluded.teams_chat_id, teams_message_id = excluded.teams_message_id, origin = excluded.origin, session_id = excluded.session_id, json = excluded.json`,
		v.ID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), v.Origin, v.SessionID, data)
	return err
}

func upsertSQLiteChatPollTx(ctx context.Context, tx *sql.Tx, v ChatPollState) error {
	// A semantically recovery-marked value may be a typed view over an opaque
	// row. Preserve that raw evidence during ordinary retry or scheduling
	// mutations. A syntax/type-corrupt row has no usable durable receipt and is
	// deliberately allowed to be replaced by the first successful Graph poll;
	// otherwise a recoverable bad projection would remain stuck forever. An
	// explicit gap/frontier repair also clears the semantic marker before this
	// function is called.
	if chatPollHasOpaqueRecoveryEvidence(v) {
		var raw []byte
		err := tx.QueryRowContext(ctx, `SELECT json FROM chat_polls WHERE chat_id = ?`, v.ChatID).Scan(&raw)
		if err == nil {
			var existing ChatPollState
			if json.Unmarshal(raw, &existing) != nil ||
				strings.TrimSpace(existing.ChatID) != strings.TrimSpace(v.ChatID) ||
				!chatPollAdmissionValid(existing) {
				return nil
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO chat_polls(chat_id, next_poll_at, blocked_until, poll_state, last_activity_at, park_notice_sent_at, parked_skip_eligible, frontier_active, updated_at, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(chat_id) DO UPDATE SET next_poll_at = excluded.next_poll_at, blocked_until = excluded.blocked_until, poll_state = excluded.poll_state, last_activity_at = excluded.last_activity_at, park_notice_sent_at = excluded.park_notice_sent_at, parked_skip_eligible = excluded.parked_skip_eligible, frontier_active = excluded.frontier_active, updated_at = excluded.updated_at, json = excluded.json`,
		v.ChatID, sqliteTime(v.NextPollAt), sqliteTime(v.BlockedUntil), v.PollState, sqliteTime(v.LastActivityAt), sqliteTime(v.ParkNoticeSentAt), sqliteBool(chatPollParkedSkipEligible(v)), sqliteBool(chatPollHasOperationalFrontier(v)), sqliteTime(v.UpdatedAt), data)
	return err
}

func upsertSQLiteChatSequenceTx(ctx context.Context, tx *sql.Tx, v ChatSequenceState) error {
	v.ChatID = strings.TrimSpace(v.ChatID)
	if v.ChatID == "" {
		return fmt.Errorf("chat sequence chat id is required")
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO chat_sequences(chat_id, next_sequence, updated_at, json) VALUES (?, ?, ?, ?)
ON CONFLICT(chat_id) DO UPDATE SET next_sequence = excluded.next_sequence, updated_at = excluded.updated_at, json = excluded.json`,
		v.ChatID, v.Next, sqliteTime(v.UpdatedAt), data)
	return err
}

func replaceSQLiteChatSequencesTx(ctx context.Context, tx *sql.Tx, values map[string]ChatSequenceState) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM chat_sequences`); err != nil {
		return err
	}
	for _, seq := range values {
		if strings.TrimSpace(seq.ChatID) == "" {
			continue
		}
		if err := upsertSQLiteChatSequenceTx(ctx, tx, seq); err != nil {
			return err
		}
	}
	return nil
}

func allocateSQLiteChatSequenceTx(ctx context.Context, tx *sql.Tx, state *State, chatID string, now time.Time) (int64, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return 0, fmt.Errorf("Teams chat id is required")
	}
	state.ensure(time.Time{})
	seq, ok, err := loadSQLiteJSONRow[ChatSequenceState](ctx, tx, `SELECT json FROM chat_sequences WHERE chat_id = ?`, chatID)
	if err != nil {
		return 0, err
	}
	if !ok {
		seq = state.ChatSequences[chatID]
	}
	if seq.ChatID == "" {
		seq.ChatID = chatID
	}
	if !ok || seq.Next <= 0 {
		nextFromOutbox, err := nextSQLiteOutboxSequenceFromMessagesTx(ctx, tx, chatID)
		if err != nil {
			return 0, err
		}
		if seq.Next <= 0 || nextFromOutbox > seq.Next {
			seq.Next = nextFromOutbox
		}
	}
	value := seq.Next
	seq.Next++
	if !now.IsZero() {
		seq.UpdatedAt = now
	}
	if err := upsertSQLiteChatSequenceTx(ctx, tx, seq); err != nil {
		return 0, err
	}
	state.ChatSequences[chatID] = seq
	return value, nil
}

func nextSQLiteOutboxSequenceFromMessagesTx(ctx context.Context, tx *sql.Tx, chatID string) (int64, error) {
	var maxSequence sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(sequence) FROM outbox_messages WHERE teams_chat_id = ?`, chatID).Scan(&maxSequence); err != nil {
		return 0, err
	}
	if maxSequence.Valid && maxSequence.Int64 >= 1 {
		return maxSequence.Int64 + 1, nil
	}
	return 1, nil
}

func upsertSQLiteChatRateLimitTx(ctx context.Context, tx *sql.Tx, v ChatRateLimitState) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO chat_rate_limits(chat_id, blocked_until, json) VALUES (?, ?, ?)
ON CONFLICT(chat_id) DO UPDATE SET blocked_until = excluded.blocked_until, json = excluded.json`,
		v.ChatID, sqliteTime(v.BlockedUntil), data)
	return err
}

func upsertSQLiteImportCheckpointTx(ctx context.Context, tx *sql.Tx, v ImportCheckpoint) error {
	if importCheckpointHasOpaqueRaw(v) {
		// This value is the typed view of an opaque SQL row.  Writing it through
		// the normal upsert would replace the original bytes with a synthetic
		// fallback and destroy the evidence needed for an explicit repair.  The
		// full-state writer has a separate raw-preserving path; all ordinary
		// transactional writers must fail instead.
		return ErrOpaqueCheckpoint
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO import_checkpoints(id, session_id, status, updated_at, json) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, status = excluded.status, updated_at = excluded.updated_at, json = excluded.json`,
		v.ID, v.SessionID, v.Status, sqliteTime(v.UpdatedAt), data)
	return err
}

func upsertSQLiteTranscriptLedgerTx(ctx context.Context, tx *sql.Tx, v TranscriptLedgerRecord) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO transcript_ledger(id, session_id, imported_at, created_at, json) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, imported_at = excluded.imported_at, created_at = excluded.created_at, json = excluded.json`,
		v.ID, v.SessionID, sqliteTime(v.ImportedAt), sqliteTime(v.CreatedAt), data)
	return err
}

func upsertSQLiteTranscriptDeliveryTx(ctx context.Context, tx *sql.Tx, v TranscriptDeliveryRecord) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO transcript_deliveries(id, session_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, outbox_id = excluded.outbox_id, status = excluded.status, created_at = excluded.created_at, json = excluded.json`,
		v.ID, v.SessionID, v.OutboxID, string(v.Status), sqliteTime(v.CreatedAt), data)
	return err
}

func upsertSQLiteHelperDeliveryTx(ctx context.Context, tx *sql.Tx, v HelperDeliveryRecord) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO helper_deliveries(id, session_id, turn_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, turn_id = excluded.turn_id, outbox_id = excluded.outbox_id, status = excluded.status, created_at = excluded.created_at, json = excluded.json`,
		v.ID, v.SessionID, v.TurnID, v.OutboxID, string(v.Status), sqliteTime(v.CreatedAt), data)
	return err
}

func upsertSQLiteArtifactRecordTx(ctx context.Context, tx *sql.Tx, v ArtifactRecord) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO artifact_records(id, session_id, turn_id, outbox_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, turn_id = excluded.turn_id, outbox_id = excluded.outbox_id, status = excluded.status, created_at = excluded.created_at, json = excluded.json`,
		v.ID, v.SessionID, v.TurnID, v.OutboxID, v.Status, sqliteTime(v.CreatedAt), data)
	return err
}

func upsertSQLiteNotificationTx(ctx context.Context, tx *sql.Tx, v NotificationRecord) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO notifications(id, session_id, turn_id, status, created_at, json) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, turn_id = excluded.turn_id, status = excluded.status, created_at = excluded.created_at, json = excluded.json`,
		v.ID, v.SessionID, v.TurnID, string(v.Status), sqliteTime(v.CreatedAt), data)
	return err
}

func upsertSQLiteSplitStateTx(ctx context.Context, tx *sql.Tx, state State) error {
	for _, checkpoint := range state.ImportCheckpoints {
		if err := upsertSQLiteImportCheckpointTx(ctx, tx, checkpoint); err != nil {
			return err
		}
	}
	for _, record := range state.TranscriptLedger {
		if err := upsertSQLiteTranscriptLedgerTx(ctx, tx, record); err != nil {
			return err
		}
	}
	for _, delivery := range state.TranscriptDeliveries {
		if err := upsertSQLiteTranscriptDeliveryTx(ctx, tx, delivery); err != nil {
			return err
		}
	}
	for _, delivery := range state.HelperDeliveries {
		if err := upsertSQLiteHelperDeliveryTx(ctx, tx, delivery); err != nil {
			return err
		}
	}
	for _, record := range state.ArtifactRecords {
		if err := upsertSQLiteArtifactRecordTx(ctx, tx, record); err != nil {
			return err
		}
	}
	for _, notification := range state.Notifications {
		if err := upsertSQLiteNotificationTx(ctx, tx, notification); err != nil {
			return err
		}
	}
	return nil
}

func loadSQLiteOutboxLinkedRecordsTx(ctx context.Context, tx *sql.Tx, state *State, outboxID string) error {
	outboxID = strings.TrimSpace(outboxID)
	if outboxID == "" {
		return nil
	}
	if state.TranscriptDeliveries == nil {
		state.TranscriptDeliveries = map[string]TranscriptDeliveryRecord{}
	}
	if state.HelperDeliveries == nil {
		state.HelperDeliveries = map[string]HelperDeliveryRecord{}
	}
	if state.ArtifactRecords == nil {
		state.ArtifactRecords = map[string]ArtifactRecord{}
	}
	if err := loadSQLiteOutboxLinkedJSONMapTx(ctx, tx, state, "transcript_deliveries", `SELECT id, json FROM transcript_deliveries WHERE outbox_id = ?`, []any{outboxID}, state.TranscriptDeliveries, func(v TranscriptDeliveryRecord) string { return v.ID }, func(v TranscriptDeliveryRecord) bool { return strings.TrimSpace(v.OutboxID) == outboxID }); err != nil {
		return err
	}
	if err := loadSQLiteOutboxLinkedJSONMapTx(ctx, tx, state, "helper_deliveries", `SELECT id, json FROM helper_deliveries WHERE outbox_id = ?`, []any{outboxID}, state.HelperDeliveries, func(v HelperDeliveryRecord) string { return v.ID }, func(v HelperDeliveryRecord) bool { return strings.TrimSpace(v.OutboxID) == outboxID }); err != nil {
		return err
	}
	if err := loadSQLiteOutboxLinkedJSONMapTx(ctx, tx, state, "artifact_records", `SELECT id, json FROM artifact_records WHERE outbox_id = ?`, []any{outboxID}, state.ArtifactRecords, func(v ArtifactRecord) string { return v.ID }, func(v ArtifactRecord) bool { return strings.TrimSpace(v.OutboxID) == outboxID }); err != nil {
		return err
	}
	return nil
}

// loadSQLiteOutboxLinkedJSONMapTx reads optional post-send projections on a
// row-local basis. These records are useful bookkeeping, but they are not the
// external Graph delivery identity. A malformed or SQL/JSON identity-conflict
// row must therefore remain opaque while allowing the core outbox receipt to
// settle and preventing one bad attachment/delivery row from blocking a chat.
func loadSQLiteOutboxLinkedJSONMapTx[T any](ctx context.Context, tx *sql.Tx, state *State, table string, query string, args []any, out map[string]T, key func(T) string, valid func(T) bool) error {
	if state != nil && state.opaqueOutboxLinkedRecords == nil {
		state.opaqueOutboxLinkedRecords = make(map[string]struct{})
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var sqlID string
		var raw []byte
		if err := rows.Scan(&sqlID, &raw); err != nil {
			return err
		}
		var value T
		if err := json.Unmarshal(raw, &value); err != nil {
			if state != nil && strings.TrimSpace(sqlID) != "" {
				state.opaqueOutboxLinkedRecords[table+":"+strings.TrimSpace(sqlID)] = struct{}{}
			}
			continue
		}
		id := strings.TrimSpace(key(value))
		if id == "" || id != strings.TrimSpace(sqlID) || valid != nil && !valid(value) {
			if state != nil && strings.TrimSpace(sqlID) != "" {
				state.opaqueOutboxLinkedRecords[table+":"+strings.TrimSpace(sqlID)] = struct{}{}
			}
			continue
		}
		out[id] = value
	}
	return rows.Err()
}

func loadSQLiteArtifactRecordsByIDTx(ctx context.Context, tx *sql.Tx, state *State, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if state.ArtifactRecords == nil {
		state.ArtifactRecords = map[string]ArtifactRecord{}
	}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := state.ArtifactRecords[id]; ok {
			continue
		}
		var sqlID string
		var raw []byte
		err := tx.QueryRowContext(ctx, `SELECT id, json FROM artifact_records WHERE id = ?`, id).Scan(&sqlID, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		var record ArtifactRecord
		if err := json.Unmarshal(raw, &record); err != nil || strings.TrimSpace(record.ID) != id {
			if state.opaqueOutboxLinkedRecords == nil {
				state.opaqueOutboxLinkedRecords = make(map[string]struct{})
			}
			state.opaqueOutboxLinkedRecords["artifact_records:"+strings.TrimSpace(sqlID)] = struct{}{}
			continue
		}
		state.ArtifactRecords[record.ID] = record
	}
	return nil
}

func pruneSQLiteTranscriptLedgerTx(ctx context.Context, tx *sql.Tx, keep int) error {
	return pruneSQLiteRowsByNewestTx(ctx, tx, "transcript_ledger", "imported_at DESC, created_at DESC, id DESC", keep)
}

func pruneSQLiteTranscriptDeliveriesTx(ctx context.Context, tx *sql.Tx, keep int) error {
	return pruneSQLiteRowsByNewestTx(ctx, tx, "transcript_deliveries", "created_at DESC, id DESC", keep)
}

func pruneSQLiteRowsByNewestTx(ctx context.Context, tx *sql.Tx, table string, orderBy string, keep int) error {
	if keep <= 0 {
		return nil
	}
	query := fmt.Sprintf(`DELETE FROM %s WHERE id NOT IN (SELECT id FROM %s ORDER BY %s LIMIT ?)`, table, table, orderBy)
	_, err := tx.ExecContext(ctx, query, keep)
	return err
}

func upsertSQLiteOutboxLinkedRecordsTx(ctx context.Context, tx *sql.Tx, state State) error {
	for _, delivery := range state.TranscriptDeliveries {
		if err := upsertSQLiteTranscriptDeliveryTx(ctx, tx, delivery); err != nil {
			return err
		}
	}
	for _, delivery := range state.HelperDeliveries {
		if err := upsertSQLiteHelperDeliveryTx(ctx, tx, delivery); err != nil {
			return err
		}
	}
	for _, record := range state.ArtifactRecords {
		if err := upsertSQLiteArtifactRecordTx(ctx, tx, record); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) turnByIDSQLite(ctx context.Context, turnID string) (Turn, bool, bool, error) {
	var out Turn
	var found bool
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		out, found, err = loadSQLiteJSONRow[Turn](ctx, db, `SELECT json FROM turns WHERE id = ?`, turnID)
		handled = true
		return err
	})
	return out, found, handled, err
}

func (s *Store) InboundEventByID(ctx context.Context, inboundID string) (InboundEvent, bool, error) {
	inboundID = strings.TrimSpace(inboundID)
	if inboundID == "" {
		return InboundEvent{}, false, nil
	}
	if out, ok, handled, err := s.inboundEventByIDSQLite(ctx, inboundID); handled || err != nil {
		return out, ok, err
	}
	state, err := s.loadStateFieldsOrFull(ctx, deferredInboundStateFields)
	if err != nil {
		return InboundEvent{}, false, err
	}
	event, ok := state.InboundEvents[inboundID]
	return event, ok, nil
}

func (s *Store) inboundEventByIDSQLite(ctx context.Context, inboundID string) (InboundEvent, bool, bool, error) {
	var out InboundEvent
	var found bool
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		out, found, err = loadSQLiteJSONRow[InboundEvent](ctx, db, `SELECT json FROM inbound_events WHERE id = ?`, inboundID)
		handled = true
		return err
	})
	return out, found, handled, err
}

func (s *Store) deferredInboundSQLite(ctx context.Context) ([]InboundEvent, bool, error) {
	var out []InboundEvent
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		rows, err := db.QueryContext(ctx, `SELECT json FROM inbound_events WHERE status = ? ORDER BY teams_chat_id, created_at, teams_message_id`, string(InboundStatusDeferred))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				return err
			}
			var event InboundEvent
			if err := json.Unmarshal(raw, &event); err != nil {
				return err
			}
			out = append(out, event)
		}
		return rows.Err()
	})
	return out, handled, err
}

func (s *Store) hasQueuedTurnsSQLite(ctx context.Context) (bool, bool, error) {
	hasQueued := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		var exists int
		err = db.QueryRowContext(ctx, `SELECT 1 FROM turns WHERE status = ? LIMIT 1`, string(TurnStatusQueued)).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		hasQueued = exists == 1
		return nil
	})
	return hasQueued, handled, err
}

func (s *Store) runningTurnSessionIDsSQLite(ctx context.Context) (map[string]bool, bool, error) {
	running := make(map[string]bool)
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		rows, err := db.QueryContext(ctx, `SELECT DISTINCT session_id FROM turns WHERE status = ? AND session_id != ''`, string(TurnStatusRunning))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var sessionID string
			if err := rows.Scan(&sessionID); err != nil {
				return err
			}
			sessionID = strings.TrimSpace(sessionID)
			if sessionID != "" {
				running[sessionID] = true
			}
		}
		return rows.Err()
	})
	return running, handled, err
}

func (s *Store) hasPendingWorkflowNotificationsSQLite(ctx context.Context) (bool, bool, error) {
	hasPending := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		var exists int
		err = db.QueryRowContext(ctx, `SELECT 1 FROM notifications WHERE status IS NULL OR status = '' OR status IN (?, ?, ?) LIMIT 1`,
			string(NotificationStatusQueued), string(NotificationStatusFailed), string(NotificationStatusSending)).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		hasPending = exists == 1
		return nil
	})
	return hasPending, handled, err
}

func (s *Store) pendingWorkflowNotificationsSQLite(ctx context.Context) ([]NotificationRecord, bool, error) {
	var out []NotificationRecord
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		rows, err := db.QueryContext(ctx, `SELECT json FROM notifications
WHERE status IS NULL OR status = '' OR status IN (?, ?, ?)
ORDER BY created_at, id`,
			string(NotificationStatusQueued), string(NotificationStatusFailed), string(NotificationStatusSending))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				return err
			}
			var rec NotificationRecord
			if err := json.Unmarshal(raw, &rec); err != nil {
				return err
			}
			if isPendingWorkflowNotification(rec) {
				out = append(out, rec)
			}
		}
		return rows.Err()
	})
	return out, handled, err
}

func (s *Store) updateNotificationSQLite(ctx context.Context, id string, fn func(NotificationRecord, bool, time.Time) (NotificationRecord, bool, error)) (NotificationRecord, bool, bool, error) {
	return s.updateNotificationSQLiteWithCapability(ctx, id, fn, storeOwnerCapability{})
}

func (s *Store) updateNotificationSQLiteWithCapability(ctx context.Context, id string, fn func(NotificationRecord, bool, time.Time) (NotificationRecord, bool, error), capability storeOwnerCapability) (NotificationRecord, bool, bool, error) {
	var out NotificationRecord
	changed := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		current, found, err := loadSQLiteJSONRow[NotificationRecord](ctx, tx, `SELECT json FROM notifications WHERE id = ?`, id)
		if err != nil {
			return err
		}
		if capability.bound() {
			lease, err := loadSQLiteControlLease(ctx, tx)
			if err != nil {
				return err
			}
			if err := validateStoreOwnerCapability(&State{ControlLease: lease}, capability); err != nil {
				return err
			}
		}
		now := time.Now()
		next, updateChanged, err := fn(current, found, now)
		if err != nil {
			return err
		}
		out = next
		handled = true
		if !updateChanged {
			return tx.Commit()
		}
		next.ID = id
		if capability.bound() {
			next.MachineID = capability.machineID
			next.LeaseGeneration = capability.leaseGeneration
		}
		if err := upsertSQLiteNotificationTx(ctx, tx, next); err != nil {
			return err
		}
		out = next
		changed = true
		return tx.Commit()
	})
	return out, changed, handled, err
}

func (s *Store) upsertArtifactRecordSQLite(ctx context.Context, record ArtifactRecord) (ArtifactRecord, bool, error) {
	var out ArtifactRecord
	handled := false
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			state := State{SchemaVersion: SchemaVersion, ArtifactRecords: map[string]ArtifactRecord{}}
			if existing, ok, err := loadSQLiteJSONRow[ArtifactRecord](ctx, tx, `SELECT json FROM artifact_records WHERE id = ?`, strings.TrimSpace(record.ID)); err != nil {
				return err
			} else if ok {
				state.ArtifactRecords[existing.ID] = existing
			}
			out = applyUpsertArtifactRecordLocked(&state, record, time.Now())
			if err := upsertSQLiteArtifactRecordTx(ctx, tx, out); err != nil {
				return err
			}
			handled = true
			return tx.Commit()
		})
	}
	if sessionID := strings.TrimSpace(record.SessionID); sessionID != "" {
		err := s.withSessionLock(ctx, sessionID, run)
		return out, handled, err
	}
	err := run()
	return out, handled, err
}

func (s *Store) recordTranscriptDeliverySQLite(ctx context.Context, parentSessionID string, delivery TranscriptDeliveryRecord, checkpoint ImportCheckpoint, capability storeOwnerCapability) (TranscriptDeliveryRecord, bool, bool, error) {
	var out TranscriptDeliveryRecord
	created := false
	handled := false
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if capability.bound() {
				lease, loadErr := loadSQLiteControlLease(ctx, tx)
				if loadErr != nil {
					return loadErr
				}
				if err := validateStoreOwnerCapability(&State{ControlLease: lease}, capability); err != nil {
					return err
				}
			}
			if err := ensureSQLiteParentUnfencedTx(ctx, tx, parentSessionID); err != nil {
				return err
			}
			state := State{
				SchemaVersion:            SchemaVersion,
				TranscriptDeliveries:     map[string]TranscriptDeliveryRecord{},
				ImportCheckpoints:        map[string]ImportCheckpoint{},
				legacyUnresolvedSessions: map[string]bool{},
			}
			if existing, ok, err := loadSQLiteJSONRow[TranscriptDeliveryRecord](ctx, tx, `SELECT json FROM transcript_deliveries WHERE id = ?`, strings.TrimSpace(delivery.ID)); err != nil {
				return err
			} else if ok {
				state.TranscriptDeliveries[existing.ID] = existing
			}
			checkpointID := strings.TrimSpace(checkpoint.ID)
			if checkpointID == "" {
				checkpointID = sessionTranscriptCheckpointID(delivery.SessionID)
			}
			if checkpointID != "" {
				if existing, ok, err := loadSQLiteCheckpointForID(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, checkpointID); err != nil {
					return err
				} else if ok {
					if err := validateLoadedTranscriptCheckpointRow(existing, checkpointID, delivery.SessionID); err != nil {
						return err
					}
					state.ImportCheckpoints[existing.ID] = existing
				}
			}
			if err := loadSQLiteSessionTranscriptCheckpointTx(ctx, tx, &state, delivery.SessionID); err != nil {
				return err
			}
			if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, delivery.SessionID); err != nil {
				return err
			}
			if err := validateTranscriptDeliveryCheckpointProvenance(delivery, checkpoint); err != nil {
				return err
			}
			if err := validateTranscriptCheckpointRecordProvenance(&state, checkpoint); err != nil {
				return err
			}
			beforeCheckpoint, hadCheckpoint := state.ImportCheckpoints[checkpointID]
			if stateHasUnresolvedExecution(&state, delivery.SessionID) &&
				!importCheckpointIsExplicitHistoryRun(beforeCheckpoint) {
				return ErrUnresolvedExecution
			}
			out, created = applyRecordTranscriptDeliveryLocked(&state, delivery, checkpoint, time.Now())
			if created {
				if err := upsertSQLiteTranscriptDeliveryTx(ctx, tx, out); err != nil {
					return err
				}
			}
			if afterCheckpoint, ok := state.ImportCheckpoints[checkpointID]; ok && (!hadCheckpoint || afterCheckpoint != beforeCheckpoint) {
				if err := upsertSQLiteImportCheckpointTx(ctx, tx, afterCheckpoint); err != nil {
					return err
				}
			}
			handled = true
			return tx.Commit()
		})
	}
	if sessionID := strings.TrimSpace(delivery.SessionID); sessionID != "" {
		err := s.withSessionLock(ctx, sessionID, run)
		return out, created, handled, err
	}
	err := run()
	return out, created, handled, err
}

func (s *Store) bindSessionCodexThreadSQLite(ctx context.Context, sessionID string, turnID string, threadID string) (SessionContext, bool, bool, error) {
	var out SessionContext
	changed := false
	handled := false
	err := s.withSessionLock(ctx, sessionID, func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, sessionID)
			if err != nil {
				return err
			}
			handled = true
			if !ok || session.ID == "" {
				return fmt.Errorf("session %q not found", sessionID)
			}
			if existing := strings.TrimSpace(session.CodexThreadID); existing != "" && existing != threadID {
				return CodexThreadBindingConflictError{SessionID: sessionID, Existing: existing, Observed: threadID}
			}
			var turn Turn
			turnOK := false
			if turnID != "" {
				loaded, ok, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, turnID)
				if err != nil {
					return err
				}
				if ok && strings.TrimSpace(loaded.SessionID) == sessionID {
					if existing := strings.TrimSpace(loaded.CodexThreadID); existing != "" && existing != threadID {
						return CodexThreadBindingConflictError{SessionID: sessionID, Existing: existing, Observed: threadID}
					}
					turn = loaded
					turnOK = true
				}
			}
			sessionNeedsUpdate := strings.TrimSpace(session.CodexThreadID) != threadID
			turnNeedsUpdate := turnOK && strings.TrimSpace(turn.CodexThreadID) != threadID
			out = session
			if !sessionNeedsUpdate && !turnNeedsUpdate {
				return tx.Commit()
			}
			now := time.Now()
			session.CodexThreadID = threadID
			session.UpdatedAt = now
			if err := upsertSQLiteSessionTx(ctx, tx, session); err != nil {
				return err
			}
			if turnNeedsUpdate {
				turn.CodexThreadID = threadID
				turn.UpdatedAt = now
				if err := upsertSQLiteTurnTx(ctx, tx, turn); err != nil {
					return err
				}
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			out = session
			changed = true
			return nil
		})
	})
	return out, changed, handled, err
}

func (s *Store) createSessionSQLite(ctx context.Context, session SessionContext) (SessionContext, bool, bool, error) {
	var out SessionContext
	created := false
	handled := false
	err := s.withSessionLock(ctx, session.ID, func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if existing, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, session.ID); err != nil {
				return err
			} else if ok {
				out = existing
				handled = true
				return tx.Commit()
			}
			now := time.Now()
			if session.Status == "" {
				session.Status = SessionStatusActive
			}
			if session.CreatedAt.IsZero() {
				session.CreatedAt = now
			}
			if session.UpdatedAt.IsZero() {
				session.UpdatedAt = session.CreatedAt
			}
			if err := upsertSQLiteSessionTx(ctx, tx, session); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			out = session
			created = true
			handled = true
			return nil
		})
	})
	return out, created, handled, err
}

func (s *Store) updateSessionContextSQLite(ctx context.Context, sessionID string, fn func(SessionContext, bool, time.Time) (SessionContext, bool, error)) (SessionContext, bool, bool, error) {
	return s.updateSessionContextSQLiteWithCapability(ctx, sessionID, fn, storeOwnerCapability{})
}

func (s *Store) updateSessionContextSQLiteWithCapability(ctx context.Context, sessionID string, fn func(SessionContext, bool, time.Time) (SessionContext, bool, error), capability storeOwnerCapability) (SessionContext, bool, bool, error) {
	var out SessionContext
	changed := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		current, found, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, sessionID)
		if err != nil {
			return err
		}
		if capability.bound() {
			lease, err := loadSQLiteControlLease(ctx, tx)
			if err != nil {
				return err
			}
			if err := validateStoreOwnerCapability(&State{ControlLease: lease}, capability); err != nil {
				return err
			}
		}
		now := time.Now()
		next, updateChanged, err := fn(current, found, now)
		if err != nil {
			return err
		}
		out = next
		handled = true
		if !updateChanged {
			return tx.Commit()
		}
		if strings.TrimSpace(next.ID) == "" {
			next.ID = sessionID
		}
		if next.UpdatedAt.IsZero() {
			next.UpdatedAt = now
		}
		if err := upsertSQLiteSessionTx(ctx, tx, next); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		out = next
		changed = true
		return nil
	})
	return out, changed, handled, err
}

func (s *Store) quarantineSessionSQLite(ctx context.Context, req SessionQuarantineRequest) (SessionQuarantineReport, bool, error) {
	var report SessionQuarantineReport
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state := newState()
		session, found, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, req.SessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("session %q not found", req.SessionID)
		}
		state.Sessions[session.ID] = session
		turnArgs := []any{req.SessionID}
		turnArgs = append(turnArgs, sqliteTurnActiveStatusArgs()...)
		if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM turns WHERE session_id = ? AND `+sqliteTurnActiveStatusSQL("status"), turnArgs, state.Turns, func(v Turn) string { return v.ID }); err != nil {
			return err
		}
		if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM inbound_events WHERE session_id = ? AND status IN (?, ?, ?)`, []any{req.SessionID, string(InboundStatusPersisted), string(InboundStatusQueued), string(InboundStatusDeferred)}, state.InboundEvents, func(v InboundEvent) string { return v.ID }); err != nil {
			return err
		}
		if err := loadSQLiteOutboxMapTx(ctx, tx, `SELECT o.json FROM outbox_messages o WHERE o.session_id = ? AND o.status IN (?, ?) AND `+sqliteOutboxProjectionValidSQL("o"), []any{req.SessionID, string(OutboxStatusQueued), string(OutboxStatusSending)}, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
			return err
		}
		linkedArgs := []any{req.SessionID, string(OutboxStatusQueued), string(OutboxStatusSending)}
		if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT d.json FROM transcript_deliveries d JOIN outbox_messages o ON o.id = d.outbox_id WHERE o.session_id = ? AND o.status IN (?, ?)`, linkedArgs, state.TranscriptDeliveries, func(v TranscriptDeliveryRecord) string { return v.ID }); err != nil {
			return err
		}
		if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT d.json FROM helper_deliveries d JOIN outbox_messages o ON o.id = d.outbox_id WHERE o.session_id = ? AND o.status IN (?, ?)`, linkedArgs, state.HelperDeliveries, func(v HelperDeliveryRecord) string { return v.ID }); err != nil {
			return err
		}
		if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT d.json FROM artifact_records d JOIN outbox_messages o ON o.id = d.outbox_id WHERE o.session_id = ? AND o.status IN (?, ?)`, linkedArgs, state.ArtifactRecords, func(v ArtifactRecord) string { return v.ID }); err != nil {
			return err
		}
		if chatID := strings.TrimSpace(session.TeamsChatID); chatID != "" {
			if poll, ok, err := loadSQLiteJSONRow[ChatPollState](ctx, tx, `SELECT json FROM chat_polls WHERE chat_id = ?`, chatID); err != nil {
				return err
			} else if ok {
				state.ChatPolls[chatID] = poll
			}
		}
		report, err = applySessionQuarantine(&state, req)
		if err != nil {
			return err
		}
		if !report.Changed {
			return tx.Commit()
		}
		if err := upsertSQLiteSessionTx(ctx, tx, report.Session); err != nil {
			return err
		}
		for _, id := range report.InterruptedTurnIDs {
			if err := upsertSQLiteTurnTx(ctx, tx, state.Turns[id]); err != nil {
				return err
			}
		}
		for _, id := range report.IgnoredInboundIDs {
			if err := upsertSQLiteInboundTx(ctx, tx, state.InboundEvents[id]); err != nil {
				return err
			}
		}
		for _, id := range report.SkippedOutboxIDs {
			if err := upsertSQLiteOutboxTx(ctx, tx, state.OutboxMessages[id]); err != nil {
				return err
			}
		}
		if err := upsertSQLiteOutboxLinkedRecordsTx(ctx, tx, state); err != nil {
			return err
		}
		if poll, ok := state.ChatPolls[strings.TrimSpace(report.Session.TeamsChatID)]; ok {
			if err := upsertSQLiteChatPollTx(ctx, tx, poll); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return report, handled, err
}

func (s *Store) unquarantineSessionSQLite(ctx context.Context, req SessionUnquarantineRequest) (SessionUnquarantineReport, bool, error) {
	var report SessionUnquarantineReport
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state := newState()
		session, found, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, req.SessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("session %q not found", req.SessionID)
		}
		state.Sessions[session.ID] = session
		if chatID := strings.TrimSpace(session.TeamsChatID); chatID != "" {
			if poll, ok, err := loadSQLiteJSONRow[ChatPollState](ctx, tx, `SELECT json FROM chat_polls WHERE chat_id = ?`, chatID); err != nil {
				return err
			} else if ok {
				state.ChatPolls[chatID] = poll
			}
		}
		report, err = applySessionUnquarantine(&state, req)
		if err != nil {
			return err
		}
		if err := upsertSQLiteSessionTx(ctx, tx, report.Session); err != nil {
			return err
		}
		if poll, ok := state.ChatPolls[strings.TrimSpace(report.Session.TeamsChatID)]; ok {
			if err := upsertSQLiteChatPollTx(ctx, tx, poll); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return report, handled, err
}

func (s *Store) persistInboundSQLite(ctx context.Context, event InboundEvent) (InboundEvent, bool, bool, error) {
	var out InboundEvent
	created := false
	handled := false
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if existing, ok, err := loadSQLiteJSONRow[InboundEvent](ctx, tx, `SELECT json FROM inbound_events WHERE id = ?`, event.ID); err != nil {
				return err
			} else if ok {
				out = existing
				handled = true
				return tx.Commit()
			}
			lookupChatID := strings.TrimSpace(event.TeamsChatID)
			lookupMessageID := strings.TrimSpace(event.TeamsMessageID)
			if existing, ok, err := loadSQLiteJSONRow[InboundEvent](ctx, tx, `SELECT json FROM inbound_events WHERE teams_chat_id = ? AND teams_message_id = ? LIMIT 1`, lookupChatID, lookupMessageID); err != nil {
				return err
			} else if ok {
				out = existing
				handled = true
				return tx.Commit()
			}
			if event.LeaseGeneration > 0 {
				matches, err := sqliteStoreOwnerCapabilityMatchesTx(ctx, tx, event.MachineID, event.LeaseGeneration)
				if err != nil {
					return err
				}
				if !matches {
					handled = true
					return ErrControlLeaseNotHeld
				}
			}
			if provenanceID := messageProvenanceID(event.TeamsChatID, event.TeamsMessageID); provenanceID != "" {
				if record, ok, err := loadSQLiteJSONRow[MessageProvenanceRecord](ctx, tx, `SELECT json FROM message_provenance WHERE id = ?`, provenanceID); err != nil {
					return err
				} else if ok && strings.TrimSpace(record.Origin) == MessageOriginHelperOutbox {
					handled = true
					return ErrInboundMessageFromHelperOutbox
				}
			}
			var delivered int
			if err := tx.QueryRowContext(ctx, `SELECT 1 FROM outbox_messages WHERE teams_chat_id = ? AND teams_message_id = ? AND status IN (?, ?) LIMIT 1`, lookupChatID, lookupMessageID, string(OutboxStatusAccepted), string(OutboxStatusSent)).Scan(&delivered); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			} else if delivered == 1 {
				handled = true
				return ErrInboundMessageFromHelperOutbox
			}
			if fenced, err := sqliteForkParentFencedTx(ctx, tx, event.SessionID); err != nil {
				return err
			} else if fenced && event.Status != InboundStatusDeferred {
				handled = true
				return ErrForkParentFenced
			}
			now := time.Now()
			if event.Status == "" {
				event.Status = InboundStatusPersisted
			}
			if event.ReceivedAt.IsZero() {
				event.ReceivedAt = now
			}
			if event.CreatedAt.IsZero() {
				event.CreatedAt = now
			}
			if event.UpdatedAt.IsZero() {
				event.UpdatedAt = event.CreatedAt
			}
			state := State{SchemaVersion: SchemaVersion, MessageProvenance: map[string]MessageProvenanceRecord{}}
			if provenanceID := messageProvenanceID(event.TeamsChatID, event.TeamsMessageID); provenanceID != "" {
				if existing, ok, err := loadSQLiteJSONRow[MessageProvenanceRecord](ctx, tx, `SELECT json FROM message_provenance WHERE id = ?`, provenanceID); err != nil {
					return err
				} else if ok {
					state.MessageProvenance[provenanceID] = existing
				}
			}
			provenance := recordMessageProvenanceLocked(&state, MessageProvenanceRecord{
				TeamsChatID:    event.TeamsChatID,
				TeamsMessageID: event.TeamsMessageID,
				Origin:         MessageOriginUserInbound,
				SessionID:      event.SessionID,
				TurnID:         event.TurnID,
				InboundID:      event.ID,
				Kind:           string(event.Status),
				RenderedHash:   event.TextHash,
				CreatedAt:      event.CreatedAt,
				UpdatedAt:      event.UpdatedAt,
			}, now)
			if err := upsertSQLiteInboundTx(ctx, tx, event); err != nil {
				return err
			}
			if provenance.ID != "" {
				if err := upsertSQLiteProvenanceTx(ctx, tx, provenance); err != nil {
					return err
				}
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			s.invalidateMessageLookupCacheLocked()
			out = event
			created = true
			handled = true
			return nil
		})
	}
	if event.SessionID != "" {
		err := s.withSessionLock(ctx, event.SessionID, run)
		return out, created, handled, err
	}
	err := run()
	return out, created, handled, err
}

func sqliteForkParentFencedTx(ctx context.Context, tx *sql.Tx, sessionID string) (bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false, nil
	}
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM fork_operations
	WHERE parent_session_id = ?
	  AND phase NOT IN (?, ?, ?)
	LIMIT 1`, sessionID,
		string(ForkPhaseLinkSent), string(ForkPhaseFailed), string(ForkPhaseAbandoned)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return one == 1, nil
}

func ensureSQLiteParentUnfencedTx(ctx context.Context, tx *sql.Tx, sessionID string) error {
	fenced, err := sqliteForkParentFencedTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if fenced {
		return ErrForkParentFenced
	}
	return nil
}

func (s *Store) updateInboundEventSQLite(ctx context.Context, inboundID string, fn func(InboundEvent, bool, time.Time) (InboundEvent, bool, error)) (InboundEvent, bool, bool, error) {
	return s.updateInboundEventSQLiteWithCapability(ctx, inboundID, fn, storeOwnerCapability{})
}

func (s *Store) updateInboundEventSQLiteWithCapability(ctx context.Context, inboundID string, fn func(InboundEvent, bool, time.Time) (InboundEvent, bool, error), capability storeOwnerCapability) (InboundEvent, bool, bool, error) {
	var out InboundEvent
	changed := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		current, found, err := loadSQLiteJSONRow[InboundEvent](ctx, tx, `SELECT json FROM inbound_events WHERE id = ?`, inboundID)
		if err != nil {
			return err
		}
		lease, err := loadSQLiteControlLease(ctx, tx)
		if err != nil {
			return err
		}
		state := State{ControlLease: lease}
		if err := validateStoreOwnerCapability(&state, capability); err != nil {
			return err
		}
		if err := validateInboundOwnerCapability(&state, current, found, capability); err != nil {
			return err
		}
		now := time.Now()
		next, updateChanged, err := fn(current, found, now)
		if err != nil {
			return err
		}
		out = next
		handled = true
		if !updateChanged {
			return tx.Commit()
		}
		if strings.TrimSpace(next.ID) == "" {
			next.ID = inboundID
		}
		if next.UpdatedAt.IsZero() {
			next.UpdatedAt = now
		}
		if capability.bound() && (!found || strings.TrimSpace(current.MachineID) == "" && current.LeaseGeneration <= 0 || current.Status == InboundStatusDeferred && (strings.TrimSpace(current.MachineID) != capability.machineID || current.LeaseGeneration != capability.leaseGeneration)) {
			next.MachineID = capability.machineID
			next.LeaseGeneration = capability.leaseGeneration
		}
		if err := upsertSQLiteInboundTx(ctx, tx, next); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		s.invalidateMessageLookupCacheLocked()
		out = next
		changed = true
		return nil
	})
	return out, changed, handled, err
}

func (s *Store) queueTurnSQLite(ctx context.Context, turn Turn) (Turn, bool, bool, error) {
	var out Turn
	created := false
	handled := false
	err := s.withSessionLock(ctx, turn.SessionID, func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if strings.TrimSpace(turn.ID) == "" {
				turn.ID = turnID(turn.InboundEventID)
			}
			if strings.TrimSpace(turn.ID) == "" {
				return fmt.Errorf("turn id or inbound event id is required")
			}
			if existing, ok, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, turn.ID); err != nil {
				return err
			} else if ok {
				out = existing
				handled = true
				return tx.Commit()
			}
			var inbound InboundEvent
			var hasInbound bool
			if turn.InboundEventID != "" {
				if existingInbound, ok, err := loadSQLiteJSONRow[InboundEvent](ctx, tx, `SELECT json FROM inbound_events WHERE id = ?`, turn.InboundEventID); err != nil {
					return err
				} else if ok {
					inbound = existingInbound
					hasInbound = true
					if inbound.TurnID != "" {
						if existing, ok, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, inbound.TurnID); err != nil {
							return err
						} else if ok {
							out = existing
							handled = true
							return tx.Commit()
						}
					}
				}
			}
			if turn.LeaseGeneration > 0 {
				matches, err := sqliteStoreOwnerCapabilityMatchesTx(ctx, tx, turn.MachineID, turn.LeaseGeneration)
				if err != nil {
					return err
				}
				if !matches {
					handled = true
					return ErrControlLeaseNotHeld
				}
			}
			session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, turn.SessionID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("session %q not found", turn.SessionID)
			}
			if session.Status == SessionStatusQuarantined {
				return fmt.Errorf("session %q is quarantined", turn.SessionID)
			}
			if !sessionStatusIsActive(session.Status) {
				return fmt.Errorf("session %q is not active", turn.SessionID)
			}
			if fenced, err := sqliteForkParentFencedTx(ctx, tx, turn.SessionID); err != nil {
				return err
			} else if fenced {
				handled = true
				return ErrForkParentFenced
			}
			now := time.Now()
			if turn.Status == "" {
				turn.Status = TurnStatusQueued
			}
			if turn.ModelProfile.IsZero() {
				turn.ModelProfile = session.ModelProfile
			}
			if turn.ModelGeneration == 0 {
				turn.ModelGeneration = session.ModelGeneration
			}
			if strings.TrimSpace(turn.ReasoningEffort) == "" && strings.TrimSpace(turn.ReasoningEffortSource) == "" {
				turn.ReasoningEffort = strings.TrimSpace(session.ReasoningEffort)
				turn.ReasoningEffortSource = strings.TrimSpace(session.ReasoningEffortSource)
			}
			if turn.QueuedAt.IsZero() {
				turn.QueuedAt = now
			}
			if turn.CreatedAt.IsZero() {
				turn.CreatedAt = now
			}
			if turn.UpdatedAt.IsZero() {
				turn.UpdatedAt = turn.CreatedAt
			}
			session.LatestTurnID = turn.ID
			session.UpdatedAt = now
			if hasInbound {
				inbound.TurnID = turn.ID
				inbound.Status = InboundStatusQueued
				inbound.UpdatedAt = now
			}
			if err := upsertSQLiteTurnTx(ctx, tx, turn); err != nil {
				return err
			}
			if err := upsertSQLiteSessionTx(ctx, tx, session); err != nil {
				return err
			}
			if hasInbound {
				if err := upsertSQLiteInboundTx(ctx, tx, inbound); err != nil {
					return err
				}
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			out = turn
			created = true
			handled = true
			return nil
		})
	})
	return out, created, handled, err
}

func (s *Store) loadSQLiteSessionTurnQueueStateUnlocked(pointer storeSQLitePointer, sessionID string, includeSession bool) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	ctx := context.Background()
	state := State{
		SchemaVersion: SchemaVersion,
		Sessions:      map[string]SessionContext{},
		Turns:         map[string]Turn{},
		InboundEvents: map[string]InboundEvent{},
	}
	if includeSession {
		loaded := make(map[string]SessionContext)
		if err := loadSQLiteSessionMap(ctx, db, `SELECT id, teams_chat_id, status, updated_at, json FROM sessions WHERE id = ?`, loaded, sessionID); err != nil {
			return State{}, err
		}
		if session, ok := loaded[strings.TrimSpace(sessionID)]; ok {
			state.Sessions[session.ID] = session
		}
	}
	if err := loadSQLiteTurnMap(ctx, db, `SELECT id, session_id, status, json FROM turns WHERE session_id = ?`, state.Turns, true, sessionID); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM inbound_events WHERE session_id = ?`, state.InboundEvents, func(v InboundEvent) string { return v.ID }, sessionID); err != nil {
		return State{}, err
	}
	for _, turn := range state.Turns {
		inboundID := strings.TrimSpace(turn.InboundEventID)
		if inboundID == "" {
			continue
		}
		if _, ok := state.InboundEvents[inboundID]; ok {
			continue
		}
		if inbound, ok, err := loadSQLiteJSONRow[InboundEvent](ctx, db, `SELECT json FROM inbound_events WHERE id = ?`, inboundID); err != nil {
			return State{}, err
		} else if ok {
			state.InboundEvents[inbound.ID] = inbound
		}
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) loadSQLiteRecentSessionInboundTurnStateUnlocked(pointer storeSQLitePointer, sessionID string, since time.Time) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	ctx := context.Background()
	state := State{
		SchemaVersion: SchemaVersion,
		Turns:         map[string]Turn{},
		InboundEvents: map[string]InboundEvent{},
	}
	query := `SELECT json FROM inbound_events WHERE session_id = ?`
	args := []any{sessionID}
	if !since.IsZero() {
		sinceSQLite := sqliteTime(since)
		receivedSinceSQLite := sqliteTime(since.Add(-time.Second))
		query = `SELECT json FROM inbound_events WHERE session_id = ? AND created_at >= ?
UNION ALL SELECT json FROM inbound_events WHERE session_id = ? AND received_at > 0 AND received_at >= ?`
		args = []any{sessionID, sinceSQLite, sessionID, receivedSinceSQLite}
	}
	if err := loadSQLiteJSONMap(ctx, db, query, state.InboundEvents, func(v InboundEvent) string { return v.ID }, args...); err != nil {
		return State{}, err
	}
	if !since.IsZero() {
		filtered := make(map[string]InboundEvent, len(state.InboundEvents))
		for id, inbound := range state.InboundEvents {
			activity := inboundStoreActivityTime(inbound)
			if !activity.IsZero() && !activity.Before(since) {
				filtered[id] = inbound
			}
		}
		state.InboundEvents = filtered
	}
	for _, inbound := range state.InboundEvents {
		turnID := strings.TrimSpace(inbound.TurnID)
		if turnID == "" {
			continue
		}
		if _, ok := state.Turns[turnID]; ok {
			continue
		}
		if turn, ok, err := loadSQLiteJSONRow[Turn](ctx, db, `SELECT json FROM turns WHERE id = ?`, turnID); err != nil {
			return State{}, err
		} else if ok {
			state.Turns[turn.ID] = turn
		}
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) loadSQLiteSessionWorkflowEventForTurnUnlocked(pointer storeSQLitePointer, sessionID string, turnID string) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	ctx := context.Background()
	state := State{
		SchemaVersion: SchemaVersion,
		Sessions:      map[string]SessionContext{},
		Turns:         map[string]Turn{},
		InboundEvents: map[string]InboundEvent{},
	}
	if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, db, `SELECT json FROM sessions WHERE id = ?`, sessionID); err != nil {
		return State{}, err
	} else if ok {
		state.Sessions[session.ID] = session
	}
	turn, ok, err := loadSQLiteJSONRow[Turn](ctx, db, `SELECT json FROM turns WHERE id = ?`, turnID)
	if err != nil {
		return State{}, err
	}
	if ok && strings.TrimSpace(turn.SessionID) == sessionID {
		state.Turns[turn.ID] = turn
		if inboundID := strings.TrimSpace(turn.InboundEventID); inboundID != "" {
			if inbound, ok, err := loadSQLiteJSONRow[InboundEvent](ctx, db, `SELECT json FROM inbound_events WHERE id = ?`, inboundID); err != nil {
				return State{}, err
			} else if ok {
				state.InboundEvents[inbound.ID] = inbound
			}
		}
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) loadSQLiteSessionThreadResolutionStateUnlocked(pointer storeSQLitePointer, sessionID string) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	ctx := context.Background()
	state := State{
		SchemaVersion: SchemaVersion,
		Sessions:      map[string]SessionContext{},
		Turns:         map[string]Turn{},
	}
	loadedSessions := make(map[string]SessionContext)
	if err := loadSQLiteSessionMap(ctx, db, `SELECT id, teams_chat_id, status, updated_at, json FROM sessions WHERE id = ?`, loadedSessions, sessionID); err != nil {
		return State{}, err
	}
	if session, ok := loadedSessions[strings.TrimSpace(sessionID)]; ok {
		state.Sessions[session.ID] = session
	}
	if err := loadSQLiteTurnMap(ctx, db, `SELECT id, session_id, status, json FROM turns WHERE session_id = ?`, state.Turns, true, sessionID); err != nil {
		return State{}, err
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) loadSQLiteSessionTranscriptDedupeStateUnlocked(pointer storeSQLitePointer, sessionID string, checkpointID string) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	ctx := context.Background()
	state := State{
		SchemaVersion:        SchemaVersion,
		Turns:                map[string]Turn{},
		InboundEvents:        map[string]InboundEvent{},
		OutboxMessages:       map[string]OutboxMessage{},
		TranscriptDeliveries: map[string]TranscriptDeliveryRecord{},
		HelperDeliveries:     map[string]HelperDeliveryRecord{},
		ImportCheckpoints:    map[string]ImportCheckpoint{},
	}
	if runtimeState, seen, err := loadSQLiteRuntimeState(ctx, db); err != nil {
		return State{}, err
	} else if sqliteRuntimeStateUsable(seen) {
		state.ServiceOwner = runtimeState.ServiceOwner
	}
	if err := loadSQLiteTurnMap(ctx, db, `SELECT id, session_id, status, json FROM turns WHERE session_id = ?`, state.Turns, true, sessionID); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM inbound_events WHERE session_id = ?`, state.InboundEvents, func(v InboundEvent) string { return v.ID }, sessionID); err != nil {
		return State{}, err
	}
	if err := loadSQLiteOutboxMap(ctx, db, `SELECT o.json FROM outbox_messages o WHERE o.session_id = ? AND `+sqliteOutboxProjectionValidSQL("o"), state.OutboxMessages, func(v OutboxMessage) string { return v.ID }, sessionID); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM transcript_deliveries WHERE session_id = ?`, state.TranscriptDeliveries, func(v TranscriptDeliveryRecord) string { return v.ID }, sessionID); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM helper_deliveries WHERE session_id = ?`, state.HelperDeliveries, func(v HelperDeliveryRecord) string { return v.ID }, sessionID); err != nil {
		return State{}, err
	}
	if err := loadSQLiteCheckpointMapWithCanonicalSessions(ctx, db, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE session_id = ?`, state.ImportCheckpoints, map[string]struct{}{sessionID: {}}, sessionID); err != nil {
		return State{}, err
	}
	if checkpointID != "" {
		if checkpoint, ok, err := loadSQLiteCheckpointForID(ctx, db, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, checkpointID); err != nil {
			return State{}, err
		} else if ok {
			if err := validateImportCheckpointProvenance(checkpoint, sessionID, checkpointID); err != nil {
				return State{}, err
			}
			state.ImportCheckpoints[checkpoint.ID] = checkpoint
		}
	}
	for _, delivery := range state.HelperDeliveries {
		outboxID := strings.TrimSpace(delivery.OutboxID)
		if outboxID == "" {
			continue
		}
		if _, ok := state.OutboxMessages[outboxID]; ok {
			continue
		}
		if outbox, ok, err := loadSQLiteJSONRow[OutboxMessage](ctx, db, `SELECT json FROM outbox_messages WHERE id = ?`, outboxID); err != nil {
			return State{}, err
		} else if ok {
			state.OutboxMessages[outbox.ID] = outbox
		}
	}
	for _, delivery := range state.TranscriptDeliveries {
		outboxID := strings.TrimSpace(delivery.OutboxID)
		if outboxID == "" {
			continue
		}
		if _, ok := state.OutboxMessages[outboxID]; ok {
			continue
		}
		if outbox, ok, err := loadSQLiteJSONRow[OutboxMessage](ctx, db, `SELECT json FROM outbox_messages WHERE id = ?`, outboxID); err != nil {
			return State{}, err
		} else if ok {
			state.OutboxMessages[outbox.ID] = outbox
		}
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) loadSQLiteSessionExecutionStateUnlocked(ctx context.Context, pointer storeSQLitePointer, sessionID string, checkpointID string) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	state := State{
		SchemaVersion:     SchemaVersion,
		Turns:             map[string]Turn{},
		ImportCheckpoints: map[string]ImportCheckpoint{},
	}
	if err := loadSQLiteTurnMap(ctx, db, `SELECT id, session_id, status, json FROM turns WHERE session_id = ?`, state.Turns, true, sessionID); err != nil {
		return State{}, err
	}
	if err := loadSQLiteCheckpointMapWithCanonicalSessions(ctx, db, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE session_id = ?`, state.ImportCheckpoints, map[string]struct{}{sessionID: {}}, sessionID); err != nil {
		return State{}, err
	}
	if checkpointID != "" {
		if checkpoint, ok, err := loadSQLiteCheckpointForID(ctx, db, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, checkpointID); err != nil {
			return State{}, err
		} else if ok {
			if err := validateImportCheckpointProvenance(checkpoint, sessionID, checkpointID); err != nil {
				return State{}, err
			}
			state.ImportCheckpoints[checkpoint.ID] = checkpoint
		}
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) loadSQLiteSessionExecutionOwnershipProbeUnlocked(ctx context.Context, pointer storeSQLitePointer, sessionID string, checkpointID string) (bool, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return false, err
	}
	if checkpointID != "" {
		if checkpoint, ok, _, err := loadSQLiteCanonicalCheckpointRow(ctx, db, checkpointID, sessionID); err != nil {
			// A scoped provenance failure belongs to this chat. Hold only this
			// session from automatic execution; an unrelated malformed row must
			// not make the listener abandon the whole admission cycle.
			if errors.Is(err, ErrSessionStateProvenanceMismatch) {
				return true, nil
			}
			return false, err
		} else if ok {
			if importCheckpointHasUnresolvedExecution(checkpoint) {
				return true, nil
			}
		}
	}
	rows, err := db.QueryContext(ctx, `SELECT session_id, json FROM turns WHERE session_id = ? AND status = ?`, sessionID, string(TurnStatusInterrupted))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var indexedSessionID string
		var raw []byte
		if err := rows.Scan(&indexedSessionID, &raw); err != nil {
			return false, err
		}
		meta, valid := decodeSQLiteTurnExecutionMetadata(raw, indexedSessionID, TurnStatusInterrupted)
		if !valid {
			// The indexed status is safety-relevant even when the payload is
			// unreadable. Do not start a replacement turn from an unknown
			// interrupted row, but keep the failure local to this session.
			return true, nil
		}
		if isLegacyUnresolvedTurn(Turn{Status: meta.Status, RecoveryReason: meta.RecoveryReason}) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func (s *Store) loadSQLiteSessionExecutionOwnershipProbesUnlocked(ctx context.Context, pointer storeSQLitePointer, requested map[string]struct{}) (map[string]bool, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(requested))
	for sessionID := range requested {
		out[sessionID] = false
	}
	ids := make([]string, 0, len(requested))
	checkpointIDToSession := make(map[string]string, len(requested))
	for sessionID := range requested {
		ids = append(ids, sessionID)
		checkpointIDToSession[sessionTranscriptCheckpointID(sessionID)] = sessionID
	}
	for start := 0; start < len(ids); start += sqliteQueryParameterBatchSize {
		end := start + sqliteQueryParameterBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		args := make([]any, 0, len(batch))
		for _, sessionID := range batch {
			args = append(args, sessionTranscriptCheckpointID(sessionID))
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(batch)), ",")
		checkpointRows, err := db.QueryContext(ctx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id IN (`+placeholders+`)`, args...)
		if err != nil {
			return nil, err
		}
		for checkpointRows.Next() {
			row, err := scanSQLiteCheckpointRow(checkpointRows)
			if err != nil {
				_ = checkpointRows.Close()
				return nil, err
			}
			sessionID, wanted := checkpointIDToSession[row.ID]
			if !wanted {
				continue
			}
			checkpoint, _, disposition, err := decodeSQLiteCheckpointRow(row, row.ID, sessionID, true)
			if err != nil {
				// A malformed optional proof is a local history-only hold, but a
				// syntactically valid foreign identity is a store corruption that
				// must remain visible to the scoped ownership caller. Silently
				// converting the latter into "unresolved" would hide a provenance
				// violation and make the JSON/SQLite contracts diverge.
				if disposition == sqliteCheckpointMalformedCanonical || disposition == sqliteCheckpointProvenanceInvalid {
					out[sessionID] = true
					continue
				}
				_ = checkpointRows.Close()
				return nil, err
			}
			if importCheckpointHasUnresolvedExecution(checkpoint) {
				out[sessionID] = true
			}
		}
		if err := checkpointRows.Err(); err != nil {
			_ = checkpointRows.Close()
			return nil, err
		}
		if err := checkpointRows.Close(); err != nil {
			return nil, err
		}
	}
	for start := 0; start < len(ids); start += sqliteQueryParameterBatchSize {
		end := start + sqliteQueryParameterBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		turnArgs := make([]any, 0, len(batch)+1)
		turnArgs = append(turnArgs, string(TurnStatusInterrupted))
		for _, id := range batch {
			turnArgs = append(turnArgs, id)
		}
		turnPlaceholders := strings.TrimRight(strings.Repeat("?,", len(batch)), ",")
		rows, err := db.QueryContext(ctx, `SELECT session_id, json FROM turns WHERE status = ? AND session_id IN (`+turnPlaceholders+`)`, turnArgs...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var sessionID string
			var raw []byte
			if err := rows.Scan(&sessionID, &raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if out[sessionID] {
				continue
			}
			meta, valid := decodeSQLiteTurnExecutionMetadata(raw, sessionID, TurnStatusInterrupted)
			if !valid {
				// SQL says the row is interrupted, so an unreadable payload is
				// conservatively an unresolved owner for just this session.
				out[sessionID] = true
				continue
			}
			if isLegacyUnresolvedTurn(Turn{Status: meta.Status, RecoveryReason: meta.RecoveryReason}) {
				out[sessionID] = true
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// loadSQLiteLinkedTranscriptExecutionSnapshotUnlocked loads only the
// active/interrupted turn rows used by the linked-transcript bridge. The
// caller already holds the state lock; keeping this query separate lets the
// idle path avoid it entirely when every checkpoint is a trusted no-op.
func (s *Store) loadSQLiteLinkedTranscriptExecutionSnapshotUnlocked(ctx context.Context, pointer storeSQLitePointer, requested map[string]struct{}) (LinkedTranscriptExecutionSnapshot, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return LinkedTranscriptExecutionSnapshot{}, err
	}
	out := LinkedTranscriptExecutionSnapshot{
		Running:   make(map[string]bool, len(requested)),
		Ownership: make(map[string]bool, len(requested)),
	}
	for sessionID := range requested {
		out.Running[sessionID] = false
		out.Ownership[sessionID] = false
	}
	if len(requested) == 0 {
		return out, nil
	}
	sessionIDs := make([]string, 0, len(requested))
	for sessionID := range requested {
		sessionIDs = append(sessionIDs, sessionID)
	}
	// Six arguments are reserved for the status predicates. Unknown non-empty
	// statuses are included as unresolved safety fences rather than being
	// silently omitted from the ownership probe.
	const statusArgs = 6
	for start := 0; start < len(sessionIDs); start += sqliteQueryParameterBatchSize - statusArgs {
		end := start + sqliteQueryParameterBatchSize - statusArgs
		if end > len(sessionIDs) {
			end = len(sessionIDs)
		}
		batch := sessionIDs[start:end]
		placeholders := strings.TrimRight(strings.Repeat("?,", len(batch)), ",")
		args := make([]any, 0, len(batch)+statusArgs)
		args = append(args, sqliteTurnUnresolvedStatusArgs()...)
		for _, sessionID := range batch {
			args = append(args, sessionID)
		}
		rows, err := db.QueryContext(ctx, `SELECT session_id, status, json FROM turns WHERE `+sqliteTurnUnresolvedStatusSQL("status")+` AND session_id IN (`+placeholders+`)`, args...)
		if err != nil {
			return LinkedTranscriptExecutionSnapshot{}, err
		}
		for rows.Next() {
			var sessionID string
			var indexedStatus string
			var raw []byte
			if err := rows.Scan(&sessionID, &indexedStatus, &raw); err != nil {
				_ = rows.Close()
				return LinkedTranscriptExecutionSnapshot{}, err
			}
			sessionID = strings.TrimSpace(sessionID)
			if _, wanted := requested[sessionID]; !wanted {
				continue
			}
			indexedTurnStatus := TurnStatus(indexedStatus)
			meta, valid := decodeSQLiteTurnExecutionMetadata(raw, sessionID, indexedTurnStatus)
			switch indexedTurnStatus {
			case TurnStatusRunning:
				out.Running[sessionID] = true
			case TurnStatusInterrupted:
				if !valid || isLegacyUnresolvedTurn(Turn{Status: meta.Status, RecoveryReason: meta.RecoveryReason}) {
					out.Ownership[sessionID] = true
				}
			default:
				// An unknown status cannot prove that the old execution ended.
				out.Running[sessionID] = true
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return LinkedTranscriptExecutionSnapshot{}, err
		}
		if err := rows.Close(); err != nil {
			return LinkedTranscriptExecutionSnapshot{}, err
		}
	}
	return out, nil
}

// loadSQLiteLinkedTranscriptSessionSnapshotUnlocked combines the checkpoint
// and active/interrupted-turn probes used by the linked-transcript bridge.
// The caller already holds the state lock, so this avoids both repeated lock
// acquisition and decoding the same checkpoint JSON once for the checkpoint
// map and again for the ownership probe.
func (s *Store) loadSQLiteLinkedTranscriptSessionSnapshotUnlocked(ctx context.Context, pointer storeSQLitePointer, requested map[string]struct{}) (LinkedTranscriptSessionSnapshot, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return LinkedTranscriptSessionSnapshot{}, err
	}
	out := LinkedTranscriptSessionSnapshot{
		Running:     make(map[string]bool, len(requested)),
		Checkpoints: make(map[string]ImportCheckpoint, len(requested)),
		Ownership:   make(map[string]bool, len(requested)),
	}
	checkpointIDToSession := make(map[string]string, len(requested))
	ids := make([]string, 0, len(requested))
	for sessionID := range requested {
		out.Running[sessionID] = false
		out.Ownership[sessionID] = false
		checkpointID := sessionTranscriptCheckpointID(sessionID)
		checkpointIDToSession[checkpointID] = sessionID
		ids = append(ids, checkpointID)
	}
	for start := 0; start < len(ids); start += sqliteQueryParameterBatchSize {
		end := start + sqliteQueryParameterBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		placeholders := strings.TrimRight(strings.Repeat("?,", len(batch)), ",")
		args := make([]any, len(batch))
		for i, id := range batch {
			args[i] = id
		}
		rows, err := db.QueryContext(ctx, `SELECT COALESCE(id, ''), COALESCE(session_id, ''), COALESCE(status, ''), COALESCE(updated_at, 0), json FROM import_checkpoints WHERE id IN (`+placeholders+`)`, args...)
		if err != nil {
			return LinkedTranscriptSessionSnapshot{}, err
		}
		for rows.Next() {
			row, err := scanSQLiteCanonicalCheckpointFastRow(rows)
			if err != nil {
				_ = rows.Close()
				return LinkedTranscriptSessionSnapshot{}, err
			}
			sessionID, wanted := checkpointIDToSession[row.ID]
			if !wanted {
				continue
			}
			checkpoint, _, _, err := decodeSQLiteCheckpointRow(row, row.ID, sessionID, true)
			if err != nil {
				_ = rows.Close()
				return LinkedTranscriptSessionSnapshot{}, err
			}
			out.Checkpoints[row.ID] = checkpoint
			out.Ownership[sessionID] = importCheckpointHasUnresolvedExecution(checkpoint)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return LinkedTranscriptSessionSnapshot{}, err
		}
		if err := rows.Close(); err != nil {
			return LinkedTranscriptSessionSnapshot{}, err
		}
	}
	// One query per bounded session batch covers both active execution and the
	// legacy interrupted-turn fallback. This preserves the old fail-closed
	// behavior without a second pass over checkpoint rows.
	execution, err := s.loadSQLiteLinkedTranscriptExecutionSnapshotUnlocked(ctx, pointer, requested)
	if err != nil {
		return LinkedTranscriptSessionSnapshot{}, err
	}
	out.Running = execution.Running
	for sessionID, owned := range execution.Ownership {
		if owned {
			out.Ownership[sessionID] = true
		}
	}
	return out, nil
}

func (s *Store) loadSQLiteSessionActiveTurnQueueStateUnlocked(ctx context.Context, pointer storeSQLitePointer, sessionID string) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	state := State{
		SchemaVersion: SchemaVersion,
		Turns:         map[string]Turn{},
		InboundEvents: map[string]InboundEvent{},
	}
	turnArgs := []any{sessionID}
	turnArgs = append(turnArgs, sqliteTurnActiveStatusArgs()...)
	if err := loadSQLiteTurnMap(ctx, db, `SELECT id, session_id, status, json FROM turns WHERE session_id = ? AND `+sqliteTurnActiveStatusSQL("status"), state.Turns, true, turnArgs...); err != nil {
		return State{}, err
	}
	for _, turn := range state.Turns {
		inboundID := strings.TrimSpace(turn.InboundEventID)
		if inboundID == "" {
			continue
		}
		if _, ok := state.InboundEvents[inboundID]; ok {
			continue
		}
		if inbound, ok, err := loadSQLiteJSONRow[InboundEvent](ctx, db, `SELECT json FROM inbound_events WHERE id = ?`, inboundID); err != nil {
			return State{}, err
		} else if ok {
			state.InboundEvents[inbound.ID] = inbound
		}
	}
	state.ensure(time.Time{})
	return state, nil
}

func (s *Store) messageLookupSQLite(ctx context.Context, chatID string, teamsMessageID string) (MessageLookup, bool, error) {
	var out MessageLookup
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		out, err = messageLookupSQLiteDirect(ctx, db, chatID, teamsMessageID)
		handled = true
		return err
	})
	return out, handled, err
}

func messageLookupSQLiteDirect(ctx context.Context, db *sql.DB, chatID string, teamsMessageID string) (MessageLookup, error) {
	chatID = strings.TrimSpace(chatID)
	teamsMessageID = strings.TrimSpace(teamsMessageID)
	if chatID == "" || teamsMessageID == "" {
		return MessageLookup{}, nil
	}
	var out MessageLookup
	provenanceID := messageProvenanceID(chatID, teamsMessageID)
	if provenanceID != "" {
		if record, ok, err := loadSQLiteJSONRow[MessageProvenanceRecord](ctx, db, `SELECT json FROM message_provenance WHERE id = ?`, provenanceID); err != nil {
			return MessageLookup{}, err
		} else if ok {
			out.Provenance = record
			out.HasProvenance = true
		}
	}
	if !out.HasProvenance {
		if record, ok, err := loadSQLiteJSONRow[MessageProvenanceRecord](ctx, db, `SELECT json FROM message_provenance WHERE teams_chat_id = ? AND teams_message_id = ? LIMIT 1`, chatID, teamsMessageID); err != nil {
			return MessageLookup{}, err
		} else if ok {
			out.Provenance = record
			out.HasProvenance = true
		}
	}
	if out.HasProvenance {
		switch strings.TrimSpace(out.Provenance.Origin) {
		case MessageOriginUserInbound:
			out.HasInbound = true
		case MessageOriginHelperOutbox:
			out.HasDeliveredOutbox = true
		}
	}
	var inboundRaw []byte
	if err := db.QueryRowContext(ctx, `SELECT json FROM inbound_events WHERE teams_chat_id = ? AND teams_message_id = ? LIMIT 1`, chatID, teamsMessageID).Scan(&inboundRaw); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return MessageLookup{}, err
		}
	} else {
		var event InboundEvent
		if err := json.Unmarshal(inboundRaw, &event); err != nil {
			return MessageLookup{}, err
		}
		out.HasInbound = true
		out.InboundNeedsQueue = inboundEventNeedsQueue(event)
	}
	var exists int
	exists = 0
	if err := db.QueryRowContext(ctx, `SELECT 1 FROM outbox_messages WHERE teams_chat_id = ? AND teams_message_id = ? AND status IN (?, ?) LIMIT 1`, chatID, teamsMessageID, string(OutboxStatusAccepted), string(OutboxStatusSent)).Scan(&exists); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return MessageLookup{}, err
		}
	} else if exists == 1 {
		out.HasDeliveredOutbox = true
	}
	return out, nil
}

func sqliteMessageLookupStamp(path string) (stateFileStamp, error) {
	stamp, err := stateFileStampForPath(path)
	if err != nil {
		return stateFileStamp{}, err
	}
	wal, err := stateFileStampForPath(path + "-wal")
	if err != nil {
		return stateFileStamp{}, err
	}
	if wal.Exists {
		stamp.Exists = true
		stamp.Size += wal.Size
		if wal.ModTime.After(stamp.ModTime) {
			stamp.ModTime = wal.ModTime
		}
	}
	return stamp, nil
}

func buildSQLiteMessageLookupCache(ctx context.Context, db *sql.DB, stamp stateFileStamp) (messageLookupCache, error) {
	cache := messageLookupCache{
		Valid:               true,
		Stamp:               stamp,
		Provenance:          map[string]MessageProvenanceRecord{},
		ProvenanceCanonical: map[string]bool{},
		Inbound:             map[string]bool{},
		InboundNeedsQueue:   map[string]bool{},
		DeliveredOutbox:     map[string]bool{},
	}
	rows, err := db.QueryContext(ctx, `SELECT id, json FROM message_provenance`)
	if err != nil {
		return messageLookupCache{}, err
	}
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return messageLookupCache{}, err
		}
		var record MessageProvenanceRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			rows.Close()
			return messageLookupCache{}, err
		}
		key := messageLookupKey(record.TeamsChatID, record.TeamsMessageID)
		if key == "" {
			continue
		}
		canonical := id == messageProvenanceID(record.TeamsChatID, record.TeamsMessageID)
		if _, ok := cache.Provenance[key]; !ok || (canonical && !cache.ProvenanceCanonical[key]) {
			cache.Provenance[key] = record
			cache.ProvenanceCanonical[key] = canonical
		}
	}
	if err := rows.Close(); err != nil {
		return messageLookupCache{}, err
	}
	for key, record := range cache.Provenance {
		switch strings.TrimSpace(record.Origin) {
		case MessageOriginUserInbound:
			cache.Inbound[key] = true
		case MessageOriginHelperOutbox:
			cache.DeliveredOutbox[key] = true
		}
	}
	rows, err = db.QueryContext(ctx, `SELECT teams_chat_id, teams_message_id, json FROM inbound_events WHERE teams_message_id <> ''`)
	if err != nil {
		return messageLookupCache{}, err
	}
	for rows.Next() {
		var chatID, teamsMessageID string
		var raw []byte
		if err := rows.Scan(&chatID, &teamsMessageID, &raw); err != nil {
			rows.Close()
			return messageLookupCache{}, err
		}
		if key := messageLookupKey(chatID, teamsMessageID); key != "" {
			cache.Inbound[key] = true
			var event InboundEvent
			if err := json.Unmarshal(raw, &event); err != nil {
				rows.Close()
				return messageLookupCache{}, err
			}
			if inboundEventNeedsQueue(event) {
				cache.InboundNeedsQueue[key] = true
			}
		}
	}
	if err := rows.Close(); err != nil {
		return messageLookupCache{}, err
	}
	rows, err = db.QueryContext(ctx, `SELECT teams_chat_id, teams_message_id FROM outbox_messages WHERE teams_message_id <> '' AND status IN (?, ?)`, string(OutboxStatusAccepted), string(OutboxStatusSent))
	if err != nil {
		return messageLookupCache{}, err
	}
	for rows.Next() {
		var chatID, teamsMessageID string
		if err := rows.Scan(&chatID, &teamsMessageID); err != nil {
			rows.Close()
			return messageLookupCache{}, err
		}
		if key := messageLookupKey(chatID, teamsMessageID); key != "" {
			cache.DeliveredOutbox[key] = true
		}
	}
	if err := rows.Close(); err != nil {
		return messageLookupCache{}, err
	}
	return cache, nil
}

func (s *Store) recordMessageProvenanceSQLite(ctx context.Context, record MessageProvenanceRecord) (MessageProvenanceRecord, bool, error) {
	var out MessageProvenanceRecord
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		record.TeamsChatID = strings.TrimSpace(record.TeamsChatID)
		record.TeamsMessageID = strings.TrimSpace(record.TeamsMessageID)
		id := messageProvenanceID(record.TeamsChatID, record.TeamsMessageID)
		if id == "" {
			handled = true
			return nil
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state := State{
			SchemaVersion:     SchemaVersion,
			MessageProvenance: map[string]MessageProvenanceRecord{},
			InboundEvents:     map[string]InboundEvent{},
			Turns:             map[string]Turn{},
		}
		if existing, ok, err := loadSQLiteJSONRow[MessageProvenanceRecord](ctx, tx, `SELECT json FROM message_provenance WHERE id = ?`, id); err != nil {
			return err
		} else if ok {
			state.MessageProvenance[id] = existing
			if strings.TrimSpace(existing.Origin) == MessageOriginUserInbound && strings.TrimSpace(record.Origin) == MessageOriginHelperOutbox {
				if inboundID := strings.TrimSpace(existing.InboundID); inboundID != "" {
					if inbound, ok, err := loadSQLiteJSONRow[InboundEvent](ctx, tx, `SELECT json FROM inbound_events WHERE id = ?`, inboundID); err != nil {
						return err
					} else if ok {
						state.InboundEvents[inboundID] = inbound
						if turnID := strings.TrimSpace(inbound.TurnID); turnID != "" {
							if turn, ok, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, turnID); err != nil {
								return err
							} else if ok {
								state.Turns[turnID] = turn
							}
						}
					}
				}
			}
		}
		before, hadBefore := state.MessageProvenance[id]
		out = recordMessageProvenanceLocked(&state, record, time.Now())
		handled = true
		if out.ID == "" {
			return nil
		}
		if hadBefore {
			if after, ok := state.MessageProvenance[out.ID]; ok && messageProvenanceRecordEqual(after, before) {
				return nil
			}
		}
		if err := upsertSQLiteProvenanceTx(ctx, tx, out); err != nil {
			return err
		}
		for _, inbound := range state.InboundEvents {
			if err := upsertSQLiteInboundTx(ctx, tx, inbound); err != nil {
				return err
			}
		}
		for _, turn := range state.Turns {
			if err := upsertSQLiteTurnTx(ctx, tx, turn); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		s.invalidateMessageLookupCacheLocked()
		return nil
	})
	return out, handled, err
}

func markSQLiteLegacyUnresolvedSessionTx(ctx context.Context, tx *sql.Tx, state *State, sessionID string) error {
	if tx == nil || state == nil {
		return nil
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	if state.legacyUnresolvedSessions == nil {
		state.legacyUnresolvedSessions = map[string]bool{}
	}
	if _, probed := state.legacyUnresolvedSessions[sessionID]; probed {
		// Multiple operations can share one SQLite transaction. A negative probe
		// is authoritative for that transaction just like a materialized anchor.
		return nil
	}
	if state.ImportCheckpoints == nil {
		state.ImportCheckpoints = map[string]ImportCheckpoint{}
	}
	checkpointID := sessionTranscriptCheckpointID(sessionID)
	if _, loaded := state.ImportCheckpoints[checkpointID]; !loaded {
		if err := loadSQLiteSessionTranscriptCheckpointTx(ctx, tx, state, sessionID); err != nil {
			return err
		}
	}
	if checkpoint, ok := state.ImportCheckpoints[checkpointID]; ok && importCheckpointHasUnresolvedExecution(checkpoint) && !importCheckpointHasMalformedCanonicalFence(checkpoint) {
		// Once the compatibility probe has materialized the canonical anchor,
		// every subsequent transaction can use this single indexed checkpoint
		// row instead of decoding all interrupted turns again.
		state.legacyUnresolvedSessions[sessionID] = true
		return nil
	}
	if checkpoint, ok := state.ImportCheckpoints[checkpointID]; ok && importCheckpointHasMalformedCanonicalFence(checkpoint) {
		// The fallback is intentionally a permanent session-local fence until an
		// explicit raw-row repair. Do not run the legacy negative probe and then
		// write its synthetic checkpoint back over the opaque payload.
		state.legacyUnresolvedSessions[sessionID] = true
		return nil
	}
	if checkpoint, ok := state.ImportCheckpoints[checkpointID]; ok && checkpoint.RecoveryProofUnusable && !importCheckpointOptionalProofUsable(checkpoint) {
		// A readable but incomplete optional proof is history-only. It must not
		// participate in live execution ownership, and caching the negative
		// legacy-turn probe must not rewrite the exact raw payload.
		state.legacyUnresolvedSessions[sessionID] = false
		return nil
	}
	legacyRevision, err := sqliteLegacyInterruptedRevisionTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if checkpoint, ok := state.ImportCheckpoints[checkpointID]; ok && strings.TrimSpace(legacyRevision) != "" && checkpoint.LegacyProbeRevision == legacyRevision {
		// A persisted negative probe is valid until the indexed interrupted set
		// changes. Do not scan/decode the same candidate rows again in every
		// transaction.
		state.legacyUnresolvedSessions[sessionID] = false
		return nil
	}
	// This is a compatibility probe for pre-anchor state. It intentionally
	// checks only the interrupted record itself: a later durable terminal is
	// not app-server ownership proof and must never discharge the fence. Keep
	// the SQL predicate limited to the indexed session/status columns, then
	// decode the small candidate rows in Go. SQLite's json_extract on every
	// candidate was a measurable polling regression and could not use the
	// existing turns indexes.
	rows, err := tx.QueryContext(ctx, `SELECT json FROM turns WHERE session_id = ? AND status = ?`, sessionID, string(TurnStatusInterrupted))
	if err != nil {
		return err
	}
	legacyCandidates := make([]Turn, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return err
		}
		var turn Turn
		if err := json.Unmarshal(raw, &turn); err != nil {
			_ = rows.Close()
			return err
		}
		if isLegacyUnresolvedTurn(turn) {
			legacyCandidates = append(legacyCandidates, turn)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	sort.SliceStable(legacyCandidates, func(i, j int) bool {
		leftTime := firstStoreNonZeroTime(legacyCandidates[i].InterruptedAt, legacyCandidates[i].UpdatedAt, legacyCandidates[i].CreatedAt)
		rightTime := firstStoreNonZeroTime(legacyCandidates[j].InterruptedAt, legacyCandidates[j].UpdatedAt, legacyCandidates[j].CreatedAt)
		if !leftTime.Equal(rightTime) {
			return leftTime.After(rightTime)
		}
		return strings.TrimSpace(legacyCandidates[i].ID) > strings.TrimSpace(legacyCandidates[j].ID)
	})
	if len(legacyCandidates) > 0 {
		legacyTurn := legacyCandidates[0]
		state.legacyUnresolvedSessions[sessionID] = true
		checkpoint := state.ImportCheckpoints[checkpointID]
		checkpoint.ID = checkpointID
		checkpoint.SessionID = sessionID
		checkpoint.LegacyProbeRevision = legacyRevision
		state.ImportCheckpoints[checkpointID] = checkpoint
		if err := materializeSQLiteLegacyExecutionAnchorTx(ctx, tx, state, sessionID, legacyTurn); err != nil {
			return err
		}
		return nil
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	checkpoint.ID = checkpointID
	checkpoint.SessionID = sessionID
	checkpoint.LegacyProbeRevision = legacyRevision
	state.ImportCheckpoints[checkpointID] = checkpoint
	if err := upsertSQLiteImportCheckpointTx(ctx, tx, checkpoint); err != nil {
		return err
	}
	state.legacyUnresolvedSessions[sessionID] = false
	return nil
}

func sqliteLegacyInterruptedRevisionTx(ctx context.Context, tx *sql.Tx, sessionID string) (string, error) {
	if tx == nil || strings.TrimSpace(sessionID) == "" {
		return "", nil
	}
	var maxUpdated int64
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(updated_at), 0), COUNT(*) FROM turns WHERE session_id = ? AND status = ?`, sessionID, string(TurnStatusInterrupted)).Scan(&maxUpdated, &count); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d:%d", maxUpdated, count), nil
}

func materializeSQLiteLegacyExecutionAnchorTx(ctx context.Context, tx *sql.Tx, state *State, sessionID string, turn Turn) error {
	if tx == nil || state == nil || strings.TrimSpace(sessionID) == "" {
		return nil
	}
	checkpointID := sessionTranscriptCheckpointID(sessionID)
	checkpoint, ok := state.ImportCheckpoints[checkpointID]
	if !ok {
		checkpoint = ImportCheckpoint{ID: checkpointID, SessionID: sessionID}
		state.ImportCheckpoints[checkpointID] = checkpoint
	}
	if importCheckpointHasUnresolvedExecution(checkpoint) && !importCheckpointHasMalformedCanonicalFence(checkpoint) {
		return nil
	}
	generation := checkpoint.ExecutionAnchorGeneration + 1
	if generation <= 0 {
		generation = 1
	}
	anchor := ExecutionAnchor{
		SessionID:         sessionID,
		ThreadID:          strings.TrimSpace(firstStoreNonEmptyString(turn.CodexThreadID, state.Sessions[sessionID].CodexThreadID)),
		OuterTurnID:       strings.TrimSpace(turn.ID),
		CodexTurnID:       strings.TrimSpace(turn.CodexTurnID),
		SourcePath:        strings.TrimSpace(checkpoint.SourcePath),
		SourceFingerprint: strings.TrimSpace(checkpoint.SourceFingerprint),
		Reason:            strings.TrimSpace(turn.RecoveryReason),
		Provenance:        ExecutionAnchorProvenanceLegacy,
		State:             "unresolved",
		Generation:        generation,
		CreatedAt:         firstStoreNonZeroTime(turn.InterruptedAt, turn.UpdatedAt, turn.CreatedAt, time.Now()),
		UpdatedAt:         time.Now(),
	}
	// If the checkpoint was written after the interruption, it is not a safe
	// cutoff: use the conservative beginning-of-source boundary instead.
	if turn.InterruptedAt.IsZero() || checkpoint.UpdatedAt.IsZero() || !checkpoint.UpdatedAt.After(turn.InterruptedAt) {
		anchor.CutoffRecordID = strings.TrimSpace(checkpoint.LastRecordID)
		anchor.CutoffLine = checkpoint.LastSourceLine
		anchor.CutoffOffset = checkpoint.LastOffset
	}
	checkpoint.ExecutionAnchorGeneration = generation
	checkpoint.UnresolvedExecution = &anchor
	state.ImportCheckpoints[checkpointID] = checkpoint
	return upsertSQLiteImportCheckpointTx(ctx, tx, checkpoint)
}

const malformedCanonicalCheckpointKind = "malformed_canonical_checkpoint"
const invalidCanonicalCheckpointKind = "invalid_canonical_checkpoint"

func opaqueCanonicalImportCheckpointFromRow(row sqliteCheckpointRow, id string, sessionID string, kind string) ImportCheckpoint {
	updatedAt := time.Time{}
	if row.UpdatedAt.Valid && row.UpdatedAt.Int64 > 0 {
		updatedAt = time.Unix(0, row.UpdatedAt.Int64).UTC()
	}
	status := strings.TrimSpace(row.Status.String)
	if status == "" {
		status = importCheckpointStatusComplete
	}
	return ImportCheckpoint{
		ID:                     strings.TrimSpace(id),
		SessionID:              strings.TrimSpace(sessionID),
		Status:                 status,
		LegacySourceUnverified: true,
		RecoveryProofUnusable:  true,
		UnresolvedExecution: &ExecutionAnchor{
			SessionID: strings.TrimSpace(sessionID),
			Reason:    "malformed canonical transcript checkpoint",
			// A malformed optional checkpoint payload is a history-only
			// diagnostic fence, not proof of a live Codex writer.
			Provenance: ExecutionAnchorProvenanceHistoryOnly,
			State:      "unresolved",
			Generation: 1,
			CreatedAt:  updatedAt,
			UpdatedAt:  updatedAt,
		},
		TranscriptQuarantine: &TranscriptQuarantine{
			Kind: strings.TrimSpace(kind),
		},
	}
}

func canonicalCheckpointSessionID(id string) (string, bool) {
	const prefix = "transcript:"
	id = strings.TrimSpace(id)
	if !strings.HasPrefix(id, prefix) {
		return "", false
	}
	sessionID := strings.TrimSpace(strings.TrimPrefix(id, prefix))
	// This decoder is only for the one canonical per-session checkpoint. A
	// subagent operation has the explicit `:subagent:` delimiter; ordinary
	// session IDs are opaque and may themselves contain colons.
	return sessionID, sessionID != "" && !strings.Contains(sessionID, ":subagent:")
}

func loadSQLiteCanonicalCheckpointRow(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id string, sessionID string) (ImportCheckpoint, bool, bool, error) {
	// The caller name is historical: UpdateImportCheckpoint is also used for
	// valid operation-specific rows such as transcript:<parent>:subagent:<id>.
	// Let the decoder accept those rows when their SQL/embedded identities are
	// complete, while its malformed fallback remains restricted to the one
	// canonical transcript:<session> namespace.
	row, found, err := loadSQLiteCheckpointRow(ctx, q,
		`SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, strings.TrimSpace(id))
	if err != nil || !found {
		return ImportCheckpoint{}, found, false, err
	}
	checkpoint, _, disposition, err := decodeSQLiteCheckpointRow(row, id, sessionID, false)
	return checkpoint, found, disposition == sqliteCheckpointMalformedCanonical || disposition == sqliteCheckpointIdentityConflict || disposition == sqliteCheckpointProvenanceInvalid, err
}

// loadSQLiteSessionTranscriptCheckpointTx loads the canonical per-session
// transcript checkpoint in addition to any operation-specific checkpoint the
// caller requested.  An explicit/full-history operation may use a different
// checkpoint ID, but the session anchor remains the authoritative execution
// fence for every automatic delivery path.
func loadSQLiteSessionTranscriptCheckpointTx(ctx context.Context, tx *sql.Tx, state *State, sessionID string) error {
	if tx == nil || state == nil {
		return nil
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	id := sessionTranscriptCheckpointID(sessionID)
	if checkpoint, alreadyLoaded := state.ImportCheckpoints[id]; alreadyLoaded {
		// Callers may have loaded an operation-specific row into the same
		// transaction before asking for the canonical session row.  Never trust
		// the map key alone: an embedded foreign ID/session must fail closed.
		return validateImportCheckpointProvenance(checkpoint, sessionID, id)
	}
	checkpoint, ok, _, err := loadSQLiteQueueClaimCheckpointTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if ok {
		state.ImportCheckpoints[checkpoint.ID] = checkpoint
	}
	return nil
}

// loadSQLiteQueueClaimCheckpointTx is deliberately narrower than the generic
// JSON loader. A legacy/corrupt canonical checkpoint is unknown execution
// ownership, not proof that the old thread is safe to reuse. It is converted
// into an unresolved fence for this transaction, but the raw row is never
// rewritten by the claim path. A valid row with the wrong embedded identity
// still fails closed through the normal provenance check.
func loadSQLiteQueueClaimCheckpointTx(ctx context.Context, tx *sql.Tx, sessionID string) (ImportCheckpoint, bool, sqliteCheckpointDisposition, error) {
	sessionID = strings.TrimSpace(sessionID)
	id := sessionTranscriptCheckpointID(sessionID)
	return loadSQLiteCheckpointForIDWithDisposition(ctx, tx,
		`SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, id)
}

func (s *Store) claimNextQueuedTurnSQLite(ctx context.Context, sessionID string) (Turn, bool, bool, error) {
	return s.claimNextQueuedTurnSQLiteWithOwner(ctx, sessionID, storeOwnerCapability{})
}

func (s *Store) claimNextQueuedTurnSQLiteWithOwner(ctx context.Context, sessionID string, capability storeOwnerCapability) (Turn, bool, bool, error) {
	var out Turn
	claimed := false
	handled := false
	err := s.withSessionLock(ctx, sessionID, func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			var ownerLease ControlLease
			if capability.bound() {
				ownerLease, err = loadSQLiteControlLease(ctx, tx)
				if err != nil {
					return err
				}
				if err := validateStoreOwnerCapability(&State{ControlLease: ownerLease}, capability); err != nil {
					return err
				}
			}
			session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, sessionID)
			if err != nil {
				return err
			}
			handled = true
			if !ok || !sessionStatusIsActive(session.Status) {
				return tx.Commit()
			}
			if fenced, err := sqliteForkParentFencedTx(ctx, tx, sessionID); err != nil {
				return err
			} else if fenced {
				return tx.Commit()
			}
			checkpoint, checkpointFound, _, err := loadSQLiteQueueClaimCheckpointTx(ctx, tx, sessionID)
			if err != nil {
				return err
			}
			legacyState := State{
				legacyUnresolvedSessions: map[string]bool{},
				ImportCheckpoints:        map[string]ImportCheckpoint{},
			}
			if checkpointFound {
				// The canonical row was already loaded for the fast unresolved
				// check above.  Seed the compatibility probe with it so a queued
				// turn claim does not issue the same indexed JSON query again.
				legacyState.ImportCheckpoints[checkpoint.ID] = checkpoint
			}
			if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &legacyState, sessionID); err != nil {
				return err
			}
			var running int
			if err := tx.QueryRowContext(ctx, `SELECT 1 FROM turns WHERE session_id = ? AND status = ? LIMIT 1`, sessionID, string(TurnStatusRunning)).Scan(&running); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if running == 1 {
				return tx.Commit()
			}
			turn, ok, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE session_id = ? AND status = ? ORDER BY queued_at, id LIMIT 1`, sessionID, string(TurnStatusQueued))
			if err != nil || !ok {
				if err != nil {
					return err
				}
				return tx.Commit()
			}
			state := State{
				SchemaVersion:            SchemaVersion,
				Sessions:                 map[string]SessionContext{sessionID: session},
				Turns:                    map[string]Turn{turn.ID: turn},
				ImportCheckpoints:        map[string]ImportCheckpoint{},
				ControlLease:             ownerLease,
				legacyUnresolvedSessions: legacyState.legacyUnresolvedSessions,
			}
			if checkpointFound {
				state.ImportCheckpoints[checkpoint.ID] = checkpoint
			}
			if retargetQueuedTurnToLiveBranchLocked(&state, &turn) {
				state.Turns[turn.ID] = turn
			}
			if !turnCanStartWhileUnresolvedExecutionLocked(&state, turn) {
				return tx.Commit()
			}
			if capability.bound() {
				if err := validateQueuedTurnOwnerForClaim(turn, capability); err != nil {
					return err
				}
				turn.MachineID = capability.machineID
				turn.LeaseGeneration = capability.leaseGeneration
			}
			now := time.Now()
			turn.Status = TurnStatusRunning
			if turn.StartNewCodexThread {
				turn.CodexThreadID = ""
			}
			if turn.StartedAt.IsZero() {
				turn.StartedAt = now
			}
			turn.UpdatedAt = now
			state.Turns[turn.ID] = turn
			updateSessionFromTurn(&state, turn, now)
			if err := upsertSQLiteTurnTx(ctx, tx, turn); err != nil {
				return err
			}
			if session := state.Sessions[sessionID]; session.ID != "" {
				if err := upsertSQLiteSessionTx(ctx, tx, session); err != nil {
					return err
				}
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			out = turn
			claimed = true
			return nil
		})
	})
	return out, claimed, handled, err
}

func (s *Store) updateTurnSQLite(ctx context.Context, turnID string, includeOutbox bool, fn func(*State, Turn, time.Time) (Turn, error)) (Turn, bool, error) {
	return s.updateTurnSQLiteWithCapability(ctx, turnID, includeOutbox, storeOwnerCapability{}, fn)
}

func (s *Store) updateTurnSQLiteWithCapability(ctx context.Context, turnID string, includeOutbox bool, capability storeOwnerCapability, fn func(*State, Turn, time.Time) (Turn, error)) (Turn, bool, error) {
	var out Turn
	handled := false
	sessionID := ""
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		current, ok, err := loadSQLiteJSONRow[Turn](ctx, db, `SELECT json FROM turns WHERE id = ?`, turnID)
		if err != nil {
			return err
		}
		handled = true
		if !ok {
			return fmt.Errorf("turn %q not found", turnID)
		}
		sessionID = strings.TrimSpace(current.SessionID)
		return nil
	})
	if err != nil || !handled {
		return out, handled, err
	}
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			current, ok, err := loadSQLiteJSONRow[Turn](ctx, db, `SELECT json FROM turns WHERE id = ?`, turnID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("turn %q not found", turnID)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			state := State{
				SchemaVersion:            SchemaVersion,
				Sessions:                 map[string]SessionContext{},
				Turns:                    map[string]Turn{turnID: current},
				ImportCheckpoints:        map[string]ImportCheckpoint{},
				InboundEvents:            map[string]InboundEvent{},
				OutboxMessages:           map[string]OutboxMessage{},
				TranscriptDeliveries:     map[string]TranscriptDeliveryRecord{},
				HelperDeliveries:         map[string]HelperDeliveryRecord{},
				ArtifactRecords:          map[string]ArtifactRecord{},
				legacyUnresolvedSessions: map[string]bool{},
			}
			// Even a legacy turn with no owner fields must be checked against
			// the current control lease.  Otherwise the JSON backend fences an
			// unscoped callback while SQLite silently accepts it because the
			// targeted-row loader had no reason to fetch the lease.  The lease
			// row is a single small record; loading it here preserves the same
			// safety rule without broadening the hot-path turn scan.
			state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
			if err != nil {
				return err
			}
			if err := validateTurnOwnerCapability(&state, current, capability); err != nil {
				return err
			}
			if current.SessionID != "" {
				if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, current.SessionID); err != nil {
					return err
				} else if ok {
					state.Sessions[current.SessionID] = session
				}
			}
			if err := loadSQLiteSessionTranscriptCheckpointTx(ctx, tx, &state, current.SessionID); err != nil {
				return err
			}
			checkpointID := sessionTranscriptCheckpointID(current.SessionID)
			beforeCheckpoint, beforeCheckpointFound := state.ImportCheckpoints[checkpointID]
			if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, current.SessionID); err != nil {
				return err
			}
			if inboundID := strings.TrimSpace(current.InboundEventID); inboundID != "" {
				if inbound, ok, err := loadSQLiteJSONRow[InboundEvent](ctx, tx, `SELECT json FROM inbound_events WHERE id = ?`, inboundID); err != nil {
					return err
				} else if ok {
					state.InboundEvents[inboundID] = inbound
				}
			}
			if includeOutbox {
				if err := loadSQLiteOutboxMapTx(ctx, tx, `SELECT o.json FROM outbox_messages o WHERE o.turn_id = ? AND `+sqliteOutboxProjectionValidSQL("o"), []any{turnID}, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
					return err
				}
				for outboxID := range state.OutboxMessages {
					if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, outboxID); err != nil {
						return err
					}
				}
			}
			now := time.Now()
			next, err := fn(&state, current, now)
			if err != nil {
				if errors.Is(err, errStoreNoChange) {
					out = current
					return tx.Commit()
				}
				return err
			}
			next.UpdatedAt = now
			state.Turns[turnID] = next
			if err := upsertSQLiteTurnTx(ctx, tx, next); err != nil {
				return err
			}
			if session := state.Sessions[current.SessionID]; session.ID != "" {
				if err := upsertSQLiteSessionTx(ctx, tx, session); err != nil {
					return err
				}
			}
			if inboundID := strings.TrimSpace(current.InboundEventID); inboundID != "" {
				if inbound := state.InboundEvents[inboundID]; inbound.ID != "" {
					if err := upsertSQLiteInboundTx(ctx, tx, inbound); err != nil {
						return err
					}
				}
			}
			for _, msg := range state.OutboxMessages {
				if err := upsertSQLiteOutboxTx(ctx, tx, msg); err != nil {
					return err
				}
			}
			if includeOutbox {
				if err := upsertSQLiteOutboxLinkedRecordsTx(ctx, tx, state); err != nil {
					return err
				}
			}
			if checkpoint, found := state.ImportCheckpoints[checkpointID]; found && (!beforeCheckpointFound || !importCheckpointEqualExceptUpdatedAt(beforeCheckpoint, checkpoint)) {
				if err := upsertSQLiteImportCheckpointTx(ctx, tx, checkpoint); err != nil {
					return err
				}
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			out = next
			return nil
		})
	}
	if sessionID != "" {
		err = s.withSessionLock(ctx, sessionID, run)
	} else {
		err = run()
	}
	return out, handled, err
}

// markTurnCompletedWithTranscriptCheckpointSQLite is the SQLite half of the
// terminal-owner/checkpoint CAS. It deliberately loads only the turn,
// session, legacy ownership probe, and one checkpoint row; unlike the old
// bridge sequence it cannot commit a cursor after a competing anchor has
// become visible.
func (s *Store) markTurnCompletedWithTranscriptCheckpointSQLite(ctx context.Context, turnID string, codexThreadID string, codexTurnID string, progress TranscriptCheckpointProgress) (Turn, bool, error) {
	var out Turn
	handled := false
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return out, false, fmt.Errorf("turn id is required")
	}
	if strings.TrimSpace(progress.ID) == "" {
		return out, false, fmt.Errorf("transcript checkpoint id is required")
	}
	sessionID := ""
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		turn, found, err := loadSQLiteJSONRow[Turn](ctx, db, `SELECT json FROM turns WHERE id = ?`, turnID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("turn %q not found", turnID)
		}
		sessionID = strings.TrimSpace(turn.SessionID)
		if strings.TrimSpace(progress.SessionID) != "" && strings.TrimSpace(progress.SessionID) != sessionID {
			return fmt.Errorf("%w: turn %q belongs to session %q, not %q", ErrSessionStateProvenanceMismatch, turnID, sessionID, progress.SessionID)
		}
		return nil
	})
	if err != nil || !handled {
		return out, handled, err
	}
	if strings.TrimSpace(progress.SessionID) == "" {
		progress.SessionID = sessionID
	}
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			current, found, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, turnID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("turn %q not found", turnID)
			}
			state := State{
				SchemaVersion:            SchemaVersion,
				Sessions:                 map[string]SessionContext{},
				Turns:                    map[string]Turn{turnID: current},
				ImportCheckpoints:        map[string]ImportCheckpoint{},
				legacyUnresolvedSessions: map[string]bool{},
			}
			state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
			if err != nil {
				return err
			}
			if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, sessionID); err != nil {
				return err
			} else if ok {
				state.Sessions[sessionID] = session
			}
			checkpoint, checkpointFound, err := loadSQLiteCheckpointForID(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, strings.TrimSpace(progress.ID))
			if err != nil {
				return err
			}
			if checkpointFound {
				if err := validateImportCheckpointProvenance(checkpoint, sessionID, progress.ID); err != nil {
					return err
				}
				state.ImportCheckpoints[strings.TrimSpace(progress.ID)] = checkpoint
			}
			if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, sessionID); err != nil {
				return err
			}
			if checkpointFound && importCheckpointHasUnresolvedExecution(checkpoint) && checkpoint.UnresolvedExecution != nil && checkpoint.UnresolvedExecution.Generation <= 0 {
				if _, ok := migrateLegacyExecutionAnchorGenerationLocked(&state, progress.ID, sessionID, codexThreadID, turnID, codexTurnID, 0); !ok {
					return ErrUnresolvedExecution
				}
				checkpoint = state.ImportCheckpoints[strings.TrimSpace(progress.ID)]
			}
			now := time.Now()
			completed, completeErr := markTurnCompletedLocked(&state, current, codexThreadID, codexTurnID, now)
			completedChanged := true
			if errors.Is(completeErr, errStoreNoChange) {
				completedChanged = false
				if current.Status != TurnStatusCompleted || !completionIdentityMatches(current, codexThreadID, codexTurnID) {
					out = current
					return tx.Commit()
				}
				completed = current
			} else if completeErr != nil {
				return completeErr
			}
			if completed.Status != TurnStatusCompleted {
				out = completed
				return tx.Commit()
			}
			checkpoint, checkpointFound = state.ImportCheckpoints[strings.TrimSpace(progress.ID)]
			anchorCleared := false
			if checkpointFound && importCheckpointHasUnresolvedExecution(checkpoint) && checkpoint.UnresolvedExecution != nil {
				anchor := checkpoint.UnresolvedExecution
				if strings.TrimSpace(anchor.OuterTurnID) != turnID || strings.TrimSpace(anchor.ThreadID) != strings.TrimSpace(codexThreadID) || strings.TrimSpace(anchor.CodexTurnID) != strings.TrimSpace(codexTurnID) || strings.TrimSpace(codexTurnID) == "" {
					return ErrUnresolvedExecution
				}
				if !completionSourceProofVerified(*anchor, progress) {
					return ErrUnresolvedExecution
				}
				checkpoint.UnresolvedExecution = nil
				anchorCleared = true
				if checkpoint.ExecutionAnchorGeneration < anchor.Generation {
					checkpoint.ExecutionAnchorGeneration = anchor.Generation
				}
			}
			next, checkpointChanged, err := applyTranscriptCheckpointProgress(checkpoint, checkpointFound, progress, now)
			if err != nil {
				return err
			}
			if completedChanged {
				if err := upsertSQLiteTurnTx(ctx, tx, completed); err != nil {
					return err
				}
				if session := state.Sessions[sessionID]; session.ID != "" {
					if err := upsertSQLiteSessionTx(ctx, tx, session); err != nil {
						return err
					}
				}
			}
			if checkpointChanged || anchorCleared {
				if err := upsertSQLiteImportCheckpointTx(ctx, tx, next); err != nil {
					return err
				}
			}
			out = completed
			return tx.Commit()
		})
	}
	err = s.withSessionLock(ctx, sessionID, run)
	return out, handled, err
}

// completeTurnWithFinalSQLite is the SQLite implementation of
// Store.CompleteTurnWithFinal.  It deliberately loads only the current turn,
// session, checkpoint, its outbox rows, and the linked delivery records.  No
// Graph or transcript I/O is performed while the transaction is open.
func (s *Store) completeTurnWithFinalSQLite(ctx context.Context, req CompleteTurnWithFinalRequest) (Turn, bool, error) {
	var out Turn
	handled := false
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			handled = true
			current, found, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, req.TurnID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("turn %q not found", req.TurnID)
			}
			if strings.TrimSpace(current.SessionID) != req.SessionID {
				return fmt.Errorf("%w: turn %q belongs to session %q, not %q", ErrSessionStateProvenanceMismatch, req.TurnID, current.SessionID, req.SessionID)
			}
			state := State{
				SchemaVersion:            SchemaVersion,
				Sessions:                 map[string]SessionContext{},
				Turns:                    map[string]Turn{req.TurnID: current},
				ImportCheckpoints:        map[string]ImportCheckpoint{},
				OutboxMessages:           map[string]OutboxMessage{},
				TranscriptDeliveries:     map[string]TranscriptDeliveryRecord{},
				HelperDeliveries:         map[string]HelperDeliveryRecord{},
				ArtifactRecords:          map[string]ArtifactRecord{},
				ChatSequences:            map[string]ChatSequenceState{},
				legacyUnresolvedSessions: map[string]bool{},
			}
			state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
			if err != nil {
				return err
			}
			if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, req.SessionID); err != nil {
				return err
			} else if ok {
				state.Sessions[req.SessionID] = session
			}
			checkpointID := sessionTranscriptCheckpointID(req.SessionID)
			checkpoint, checkpointFound, err := loadSQLiteCheckpointForID(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, checkpointID)
			if err != nil {
				return err
			}
			if checkpointFound {
				if err := validateImportCheckpointProvenance(checkpoint, req.SessionID, checkpointID); err != nil {
					return err
				}
				state.ImportCheckpoints[checkpointID] = checkpoint
			}
			if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, req.SessionID); err != nil {
				return err
			}
			if err := loadSQLiteOutboxMapTx(ctx, tx, `SELECT o.json FROM outbox_messages o WHERE o.turn_id = ? AND `+sqliteOutboxProjectionValidSQL("o"), []any{req.TurnID}, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
				return err
			}
			// The planned IDs are part of the ownership CAS.  Loading only rows
			// selected by TurnID would let a collision with another Turn reach
			// the upsert and overwrite that row, unlike the JSON backend which
			// rejects the conflict.  Load every intended ID before validation.
			for _, planned := range req.FinalOutbox {
				id := strings.TrimSpace(planned.ID)
				if id == "" {
					continue
				}
				if _, exists := state.OutboxMessages[id]; exists {
					continue
				}
				existing, exists, loadErr := loadSQLiteJSONRow[OutboxMessage](ctx, tx, `SELECT json FROM outbox_messages WHERE id = ?`, id)
				if loadErr != nil {
					return loadErr
				}
				if exists {
					state.OutboxMessages[id] = existing
					if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, id); err != nil {
						return err
					}
				}
			}
			for outboxID := range state.OutboxMessages {
				if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, outboxID); err != nil {
					return err
				}
			}
			for i, msg := range req.FinalOutbox {
				if _, exists := state.OutboxMessages[msg.ID]; exists || msg.Sequence > 0 {
					continue
				}
				sequence, err := allocateSQLiteChatSequenceTx(ctx, tx, &state, msg.TeamsChatID, time.Now())
				if err != nil {
					return err
				}
				msg.Sequence = sequence
				req.FinalOutbox[i] = msg
			}
			beforeCheckpoint, beforeCheckpointFound := state.ImportCheckpoints[checkpointID]
			completed, completeErr := completeTurnWithFinalLocked(&state, req, time.Now())
			if errors.Is(completeErr, errStoreNoChange) {
				out = completed
				return tx.Commit()
			}
			if completeErr != nil {
				return completeErr
			}
			if err := upsertSQLiteTurnTx(ctx, tx, completed); err != nil {
				return err
			}
			if session := state.Sessions[req.SessionID]; session.ID != "" {
				if err := upsertSQLiteSessionTx(ctx, tx, session); err != nil {
					return err
				}
			}
			for _, msg := range state.OutboxMessages {
				if err := upsertSQLiteOutboxTx(ctx, tx, msg); err != nil {
					return err
				}
			}
			if err := upsertSQLiteOutboxLinkedRecordsTx(ctx, tx, state); err != nil {
				return err
			}
			if checkpoint, found := state.ImportCheckpoints[checkpointID]; found && (!beforeCheckpointFound || !importCheckpointEqualExceptUpdatedAt(beforeCheckpoint, checkpoint)) {
				if err := upsertSQLiteImportCheckpointTx(ctx, tx, checkpoint); err != nil {
					return err
				}
			}
			out = completed
			return tx.Commit()
		})
	}
	err := s.withSessionLock(ctx, req.SessionID, run)
	return out, handled, err
}

func (s *Store) clearExecutionAnchorAndConfirmTurnSQLite(ctx context.Context, req ExecutionAnchorClearRequest) (bool, error) {
	return s.clearExecutionAnchorAndConfirmTurnSQLiteWithCapability(ctx, req, storeOwnerCapability{})
}

func (s *Store) clearExecutionAnchorAndConfirmTurnSQLiteWithCapability(ctx context.Context, req ExecutionAnchorClearRequest, capability storeOwnerCapability) (bool, error) {
	handled := false
	if strings.TrimSpace(req.CheckpointID) == "" || strings.TrimSpace(req.SessionID) == "" || strings.TrimSpace(req.OuterTurnID) == "" {
		return false, nil
	}
	err := s.withStateLock(ctx, func() error {
		_, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		handled = true
		return nil
	})
	if err != nil || !handled {
		return handled, err
	}
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			checkpoint, checkpointFound, err := loadSQLiteCheckpointForID(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, req.CheckpointID)
			if err != nil {
				return err
			}
			turn, turnFound, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, req.OuterTurnID)
			if err != nil {
				return err
			}
			if !checkpointFound || !turnFound {
				return tx.Commit()
			}
			state := State{
				SchemaVersion:            SchemaVersion,
				Turns:                    map[string]Turn{turn.ID: turn},
				ImportCheckpoints:        map[string]ImportCheckpoint{checkpoint.ID: checkpoint},
				OutboxMessages:           map[string]OutboxMessage{},
				legacyUnresolvedSessions: map[string]bool{},
			}
			// Owner-scoped anchor cleanup must validate against the lease that is
			// durable at the same transaction boundary.  Omitting it leaves the
			// narrow SQLite state with an empty lease and rejects every live
			// capability, even though the full store still has the active owner.
			state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
			if err != nil {
				return err
			}
			if strings.TrimSpace(turn.SessionID) != strings.TrimSpace(req.SessionID) {
				return tx.Commit()
			}
			if err := validateTurnOwnerCapability(&state, turn, capability); err != nil {
				return err
			}
			if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, req.SessionID); err != nil {
				return err
			}
			if err := loadSQLiteOutboxMapTx(ctx, tx, `SELECT o.json FROM outbox_messages o WHERE o.turn_id = ? AND `+sqliteOutboxProjectionValidSQL("o"), []any{req.OuterTurnID}, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
				return err
			}
			if !clearExecutionAnchorLocked(&state, req) {
				return tx.Commit()
			}
			updatedTurn := state.Turns[turn.ID]
			if updatedTurn.RecoveryReason != turn.RecoveryReason {
				if err := upsertSQLiteTurnTx(ctx, tx, updatedTurn); err != nil {
					return err
				}
			}
			for _, msg := range state.OutboxMessages {
				if err := upsertSQLiteOutboxTx(ctx, tx, msg); err != nil {
					return err
				}
			}
			updatedCheckpoint := state.ImportCheckpoints[checkpoint.ID]
			if err := upsertSQLiteImportCheckpointTx(ctx, tx, updatedCheckpoint); err != nil {
				return err
			}
			return tx.Commit()
		})
	}
	err = s.withSessionLock(ctx, req.SessionID, run)
	return handled, err
}

func (s *Store) queueOutboxSQLite(ctx context.Context, msg OutboxMessage) (OutboxMessage, bool, bool, error) {
	return s.queueOutboxSQLiteWithCapability(ctx, msg, storeOwnerCapability{})
}

func (s *Store) queueOutboxSQLiteWithCapability(ctx context.Context, msg OutboxMessage, capability storeOwnerCapability) (OutboxMessage, bool, bool, error) {
	var out OutboxMessage
	created := false
	handled := false
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if existing, ok, err := loadSQLiteJSONRow[OutboxMessage](ctx, tx, `SELECT json FROM outbox_messages WHERE id = ?`, msg.ID); err != nil {
				return err
			} else if ok {
				out = existing
				handled = true
				return tx.Commit()
			}
			state, err := loadSQLiteColdState(ctx, tx)
			if err != nil {
				return err
			}
			if capability.bound() {
				state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
				if err != nil {
					return err
				}
				if err := validateStoreOwnerCapability(&state, capability); err != nil {
					return err
				}
				msg.MachineID = capability.machineID
				msg.LeaseGeneration = capability.leaseGeneration
			}
			state.Sessions = map[string]SessionContext{}
			state.Turns = map[string]Turn{}
			if turnID := strings.TrimSpace(msg.TurnID); turnID != "" {
				if turn, ok, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, turnID); err != nil {
					return err
				} else if ok {
					state.Turns[turnID] = turn
				}
			}
			if sessionID := strings.TrimSpace(msg.SessionID); sessionID != "" {
				if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, sessionID); err != nil {
					return err
				} else if ok {
					state.Sessions[sessionID] = session
				}
			}
			now := time.Now()
			msg.TeamsChatID = strings.TrimSpace(msg.TeamsChatID)
			if msg.TeamsChatID == "" {
				return fmt.Errorf("Teams chat id is required")
			}
			if msg.Sequence <= 0 {
				sequence, err := allocateSQLiteChatSequenceTx(ctx, tx, &state, msg.TeamsChatID, now)
				if err != nil {
					return err
				}
				msg.Sequence = sequence
			}
			out, created, err = queueOutboxLocked(&state, msg, now)
			if err != nil {
				return err
			}
			handled = true
			if !created {
				return tx.Commit()
			}
			if err := upsertSQLiteOutboxTx(ctx, tx, out); err != nil {
				return err
			}
			if err := upsertSQLiteOutboxLinkedRecordsTx(ctx, tx, state); err != nil {
				return err
			}
			return tx.Commit()
		})
	}
	if msg.SessionID != "" {
		err := s.withSessionLock(ctx, msg.SessionID, run)
		return out, created, handled, err
	}
	err := run()
	return out, created, handled, err
}

func (s *Store) queueTranscriptDeliveryOutboxSQLite(ctx context.Context, req TranscriptDeliveryQueueRequest) (OutboxMessage, bool, bool, bool, error) {
	var out OutboxMessage
	created := false
	alreadyDelivered := false
	handled := false
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			if err := ensureSQLiteParentUnfencedTx(ctx, tx, req.ParentFenceSessionID); err != nil {
				return err
			}
			state := State{
				SchemaVersion:            SchemaVersion,
				Sessions:                 map[string]SessionContext{},
				Turns:                    map[string]Turn{},
				OutboxMessages:           map[string]OutboxMessage{},
				TranscriptDeliveries:     map[string]TranscriptDeliveryRecord{},
				HelperDeliveries:         map[string]HelperDeliveryRecord{},
				ArtifactRecords:          map[string]ArtifactRecord{},
				ImportCheckpoints:        map[string]ImportCheckpoint{},
				ChatSequences:            map[string]ChatSequenceState{},
				legacyUnresolvedSessions: map[string]bool{},
			}
			// Queueing a live transcript is an owner-bound durable mutation. The
			// narrow transaction state must include the lease before the shared
			// reducer validates the message capability; otherwise SQLite would
			// silently accept a stale callback that JSON correctly rejects.
			state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
			if err != nil {
				return err
			}
			msg := req.Message
			delivery := req.Delivery
			if turnID := strings.TrimSpace(msg.TurnID); turnID != "" {
				if turn, ok, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, turnID); err != nil {
					return err
				} else if ok {
					state.Turns[turnID] = turn
				}
			}
			if sessionID := strings.TrimSpace(firstStoreNonEmptyString(msg.SessionID, delivery.SessionID)); sessionID != "" {
				if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, sessionID); err != nil {
					return err
				} else if ok {
					state.Sessions[sessionID] = session
				}
				if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, sessionID); err != nil {
					return err
				}
			}
			deliveryID := strings.TrimSpace(delivery.ID)
			existingDelivery, deliveryFound, err := loadSQLiteJSONRow[TranscriptDeliveryRecord](ctx, tx, `SELECT json FROM transcript_deliveries WHERE id = ?`, deliveryID)
			if err != nil {
				return err
			}
			if deliveryFound {
				state.TranscriptDeliveries[existingDelivery.ID] = existingDelivery
				if outboxID := strings.TrimSpace(existingDelivery.OutboxID); outboxID != "" {
					if existing, ok, err := loadSQLiteJSONRow[OutboxMessage](ctx, tx, `SELECT json FROM outbox_messages WHERE id = ?`, outboxID); err != nil {
						return err
					} else if ok {
						state.OutboxMessages[existing.ID] = existing
					}
					if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, outboxID); err != nil {
						return err
					}
				}
			}
			if msgID := strings.TrimSpace(msg.ID); msgID != "" {
				if existing, ok, err := loadSQLiteJSONRow[OutboxMessage](ctx, tx, `SELECT json FROM outbox_messages WHERE id = ?`, msgID); err != nil {
					return err
				} else if ok {
					state.OutboxMessages[existing.ID] = existing
				}
				if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, msgID); err != nil {
					return err
				}
			}
			checkpointID := strings.TrimSpace(req.Checkpoint.ID)
			if checkpointID == "" {
				checkpointID = sessionTranscriptCheckpointID(firstStoreNonEmptyString(msg.SessionID, delivery.SessionID))
			}
			if checkpointID != "" {
				if checkpoint, ok, err := loadSQLiteCheckpointForID(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, checkpointID); err != nil {
					return err
				} else if ok {
					if err := validateLoadedTranscriptCheckpointRow(checkpoint, checkpointID, firstStoreNonEmptyString(msg.SessionID, delivery.SessionID)); err != nil {
						return err
					}
					state.ImportCheckpoints[checkpoint.ID] = checkpoint
				}
			}
			if err := loadSQLiteSessionTranscriptCheckpointTx(ctx, tx, &state, firstStoreNonEmptyString(msg.SessionID, delivery.SessionID)); err != nil {
				return err
			}

			needCreateOutbox := false
			switch {
			case deliveryFound && transcriptDeliverySuppressesQueue(existingDelivery) &&
				!transcriptDeliveryCanBeRequeuedForExplicitHistory(existingDelivery, state.OutboxMessages[strings.TrimSpace(existingDelivery.OutboxID)], true, msg):
				needCreateOutbox = false
			case deliveryFound:
				outboxID := strings.TrimSpace(existingDelivery.OutboxID)
				if outboxID != "" {
					msg.ID = outboxID
					needCreateOutbox = state.OutboxMessages[outboxID].ID == ""
				} else {
					needCreateOutbox = state.OutboxMessages[strings.TrimSpace(msg.ID)].ID == ""
				}
			default:
				needCreateOutbox = state.OutboxMessages[strings.TrimSpace(msg.ID)].ID == ""
			}
			if needCreateOutbox && msg.Sequence <= 0 && strings.TrimSpace(msg.TeamsChatID) != "" {
				sequence, err := allocateSQLiteChatSequenceTx(ctx, tx, &state, strings.TrimSpace(msg.TeamsChatID), time.Now())
				if err != nil {
					return err
				}
				msg.Sequence = sequence
			}

			beforeDelivery, hadDelivery := state.TranscriptDeliveries[deliveryID]
			beforeCheckpoint, hadCheckpoint := state.ImportCheckpoints[checkpointID]
			beforeOutboxID := strings.TrimSpace(msg.ID)
			beforeOutbox, hadOutbox := state.OutboxMessages[beforeOutboxID]
			beforeHelpers := make(map[string]HelperDeliveryRecord, len(state.HelperDeliveries))
			for id, record := range state.HelperDeliveries {
				beforeHelpers[id] = record
			}
			out, created, alreadyDelivered, err = applyQueueTranscriptDeliveryOutboxLocked(&state, msg, delivery, req.Checkpoint, time.Now())
			if err != nil {
				return err
			}
			handled = true
			if created {
				if err := upsertSQLiteOutboxTx(ctx, tx, out); err != nil {
					return err
				}
			} else if strings.TrimSpace(out.ID) != "" {
				if after, ok := state.OutboxMessages[out.ID]; ok && (!hadOutbox || !reflect.DeepEqual(beforeOutbox, after)) {
					if err := upsertSQLiteOutboxTx(ctx, tx, after); err != nil {
						return err
					}
				}
			}
			if after, ok := state.TranscriptDeliveries[deliveryID]; ok && (!hadDelivery || !reflect.DeepEqual(beforeDelivery, after)) {
				if err := upsertSQLiteTranscriptDeliveryTx(ctx, tx, after); err != nil {
					return err
				}
			}
			if checkpointID != "" {
				if after, ok := state.ImportCheckpoints[checkpointID]; ok && (!hadCheckpoint || !reflect.DeepEqual(beforeCheckpoint, after)) {
					if err := upsertSQLiteImportCheckpointTx(ctx, tx, after); err != nil {
						return err
					}
				}
			}
			for id, record := range state.HelperDeliveries {
				if before, ok := beforeHelpers[id]; !ok || !reflect.DeepEqual(before, record) {
					if err := upsertSQLiteHelperDeliveryTx(ctx, tx, record); err != nil {
						return err
					}
				}
			}
			return tx.Commit()
		})
	}
	sessionID := strings.TrimSpace(firstStoreNonEmptyString(req.Message.SessionID, req.Delivery.SessionID))
	if sessionID != "" {
		err := s.withSessionLock(ctx, sessionID, run)
		return out, created, alreadyDelivered, handled, err
	}
	err := run()
	return out, created, alreadyDelivered, handled, err
}

func (s *Store) updateOutboxSQLite(ctx context.Context, outboxID string, loadCold bool, loadLinked bool, fn func(*State, OutboxMessage, time.Time) (OutboxMessage, error)) (OutboxMessage, bool, error) {
	var out OutboxMessage
	handled := false
	sessionID := ""
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		if err := db.QueryRowContext(ctx, `SELECT session_id FROM outbox_messages WHERE id = ?`, outboxID).Scan(&sessionID); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("outbox message %q not found", outboxID)
		} else if err != nil {
			return err
		}
		sessionID = strings.TrimSpace(sessionID)
		return nil
	})
	if err != nil || !handled {
		return out, handled, err
	}
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			current, ok, err := loadSQLiteJSONRow[OutboxMessage](ctx, db, `SELECT json FROM outbox_messages WHERE id = ?`, outboxID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("outbox message %q not found", outboxID)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			state := newState()
			if loadCold {
				state, err = loadSQLiteColdState(ctx, tx)
				if err != nil {
					return err
				}
			} else {
				state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
				if err != nil {
					return err
				}
			}
			if state.legacyUnresolvedSessions == nil {
				state.legacyUnresolvedSessions = map[string]bool{}
			}
			state.OutboxMessages = map[string]OutboxMessage{outboxID: current}
			state.Sessions = map[string]SessionContext{}
			state.Turns = map[string]Turn{}
			if turnID := strings.TrimSpace(current.TurnID); turnID != "" {
				if turn, ok, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, turnID); err != nil {
					return err
				} else if ok {
					state.Turns[turnID] = turn
				}
			}
			if sessionID := strings.TrimSpace(current.SessionID); sessionID != "" {
				if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, sessionID); err != nil {
					return err
				} else if ok {
					state.Sessions[sessionID] = session
				}
				if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, sessionID); err != nil {
					return err
				}
				checkpointID := sessionTranscriptCheckpointID(sessionID)
				if checkpoint, ok, err := loadSQLiteCheckpointForID(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, checkpointID); err != nil {
					return err
				} else if ok {
					state.ImportCheckpoints[checkpoint.ID] = checkpoint
				}
			}
			if operationID := strings.TrimSpace(current.ForkOperationID); operationID != "" {
				if operation, ok, err := loadSQLiteJSONRow[ForkOperation](ctx, tx, `SELECT json FROM fork_operations WHERE id = ?`, operationID); err != nil {
					return err
				} else if ok {
					state.ForkOperations[operationID] = operation
				}
			}
			if loadCold || loadLinked {
				if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, outboxID); err != nil {
					return err
				}
				if err := loadSQLiteArtifactRecordsByIDTx(ctx, tx, &state, current.ArtifactIDs); err != nil {
					return err
				}
			}
			if parentSessionID := transcriptDeliveryParentFenceSessionID(current); parentSessionID != "" {
				if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM fork_operations WHERE parent_session_id = ?`, []any{parentSessionID}, state.ForkOperations, func(v ForkOperation) string { return v.ID }); err != nil {
					return err
				}
			}
			now := time.Now()
			next, err := fn(&state, current, now)
			if err != nil {
				if errors.Is(err, errStoreNoChange) {
					out = current
					return tx.Commit()
				}
				return err
			}
			next.UpdatedAt = now
			state.OutboxMessages[outboxID] = next
			if err := upsertSQLiteOutboxTx(ctx, tx, next); err != nil {
				return err
			}
			if loadCold || loadLinked {
				if err := upsertSQLiteOutboxLinkedRecordsTx(ctx, tx, state); err != nil {
					return err
				}
			}
			if loadCold {
				if err := saveSQLiteColdStateTx(ctx, tx, state); err != nil {
					return err
				}
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			out = next
			return nil
		})
	}
	if sessionID != "" {
		err = s.withSessionLock(ctx, sessionID, run)
	} else {
		err = run()
	}
	return out, handled, err
}

func (s *Store) markOutboxDeliveredSQLite(ctx context.Context, outboxID string, attemptToken string, teamsMessageID string, sent bool, requireClaim bool, sourceProofPrevalidated bool) (OutboxMessage, bool, error) {
	return s.markOutboxDeliveredSQLiteWithCapability(ctx, outboxID, attemptToken, teamsMessageID, sent, requireClaim, sourceProofPrevalidated, storeOwnerCapability{})
}

func (s *Store) markOutboxDeliveredSQLiteWithCapability(ctx context.Context, outboxID string, attemptToken string, teamsMessageID string, sent bool, requireClaim bool, sourceProofPrevalidated bool, capability storeOwnerCapability) (OutboxMessage, bool, error) {
	var out OutboxMessage
	handled := false
	sessionID := ""
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		if err := db.QueryRowContext(ctx, `SELECT session_id FROM outbox_messages WHERE id = ?`, outboxID).Scan(&sessionID); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("outbox message %q not found", outboxID)
		} else if err != nil {
			return err
		}
		sessionID = strings.TrimSpace(sessionID)
		return nil
	})
	if err != nil || !handled {
		return out, handled, err
	}
	run := func() error {
		return s.withStateLock(ctx, func() error {
			pointer, ok, err := s.currentSQLitePointerUnlocked()
			if err != nil || !ok {
				return err
			}
			db, err := s.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			current, ok, err := loadSQLiteJSONRow[OutboxMessage](ctx, db, `SELECT json FROM outbox_messages WHERE id = ?`, outboxID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("outbox message %q not found", outboxID)
			}
			if err := validateOutboxDeliveryAttempt(current, attemptToken, teamsMessageID, sent, requireClaim); err != nil {
				out = current
				return err
			}
			nextMessageID := firstStoreNonEmptyString(strings.TrimSpace(teamsMessageID), strings.TrimSpace(current.TeamsMessageID))
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			state := newState()
			state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
			if err != nil {
				return err
			}
			if err := validateStoreOwnerCapability(&state, capability); err != nil {
				out = current
				return err
			}
			state.OutboxMessages = map[string]OutboxMessage{outboxID: current}
			state.MessageProvenance = map[string]MessageProvenanceRecord{}
			state.Sessions = map[string]SessionContext{}
			state.Turns = map[string]Turn{}
			if turnID := strings.TrimSpace(current.TurnID); turnID != "" {
				if turn, ok, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, turnID); err != nil {
					return err
				} else if ok {
					state.Turns[turnID] = turn
				}
			}
			if sessionID := strings.TrimSpace(current.SessionID); sessionID != "" {
				if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, sessionID); err != nil {
					return err
				} else if ok {
					state.Sessions[sessionID] = session
				}
				// The canonical session checkpoint (including a materialized
				// pre-anchor compatibility anchor) is part of the same transaction
				// as the final delivery projection.  This closes the race where an
				// execution becomes unresolved after the pre-send check but before
				// Graph's accepted/sent callback.
				if err := loadSQLiteSessionTranscriptCheckpointTx(ctx, tx, &state, sessionID); err != nil {
					return err
				}
				if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, sessionID); err != nil {
					return err
				}
			}
			if nextMessageID != "" {
				id := messageProvenanceID(current.TeamsChatID, nextMessageID)
				if existing, ok, err := loadSQLiteJSONRow[MessageProvenanceRecord](ctx, tx, `SELECT json FROM message_provenance WHERE id = ?`, id); err != nil {
					return err
				} else if ok {
					state.MessageProvenance[id] = existing
				}
			}
			if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, outboxID); err != nil {
				return err
			}
			if err := loadSQLiteArtifactRecordsByIDTx(ctx, tx, &state, current.ArtifactIDs); err != nil {
				return err
			}
			if requireClaim {
				if err := validateOutboxOwnerCapability(&state, current); err != nil {
					out = current
					return err
				}
			}
			now := time.Now()
			msg := current
			bindOutboxToOwnerCapability(&state, &msg, capability)
			if err := markOutboxDeliveryBlockedIfUnresolvedExecution(&state, &msg, teamsMessageID); err != nil {
				out = current
				return err
			}
			if sent {
				msg = applyOutboxSentProjectionLocked(&state, msg, teamsMessageID, now, sourceProofPrevalidated)
			} else {
				if msg.Status != OutboxStatusSent {
					msg.Status = OutboxStatusAccepted
				}
				if teamsMessageID != "" {
					msg.TeamsMessageID = teamsMessageID
				}
				msg.LastSendError = ""
				recordOutboxProvenanceLocked(&state, msg, now)
				if msg.Status == OutboxStatusSent {
					markTranscriptDeliveryForOutboxLocked(&state, msg, TranscriptDeliveryStatusSent, now)
					updateHelperDeliveryForOutboxLocked(&state, msg, HelperDeliveryStatusSent, now)
					updateArtifactRecordsForOutboxLocked(&state, msg, now, "uploaded", "", "")
				} else {
					markTranscriptDeliveryForOutboxLocked(&state, msg, TranscriptDeliveryStatusAccepted, now)
					updateHelperDeliveryForOutboxLocked(&state, msg, HelperDeliveryStatusAccepted, now)
				}
				msg.UpdatedAt = now
			}
			state.OutboxMessages[outboxID] = msg
			if err := upsertSQLiteOutboxTx(ctx, tx, msg); err != nil {
				return err
			}
			for _, record := range state.MessageProvenance {
				if err := upsertSQLiteProvenanceTx(ctx, tx, record); err != nil {
					return err
				}
			}
			if err := upsertSQLiteOutboxLinkedRecordsTx(ctx, tx, state); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			s.invalidateMessageLookupCacheLocked()
			out = msg
			handled = true
			return nil
		})
	}
	if sessionID != "" {
		err = s.withSessionLock(ctx, sessionID, run)
	} else {
		err = run()
	}
	return out, handled, err
}

func (s *Store) applyOutboxReplayFencesSQLite(ctx context.Context, fences []OutboxReplayFence) (int, bool, error) {
	changed := 0
	pointer, handled, err := s.currentSQLitePointerReadOnly()
	if err != nil || !handled {
		return 0, handled, err
	}
	dbPath, err := s.storeSQLitePath(pointer)
	if err != nil {
		return 0, true, err
	}
	current, err := loadSQLiteOutboxReplayFenceMessagesFileReadOnly(ctx, dbPath, fences)
	if err != nil {
		return 0, true, err
	}
	pending := 0
	for _, msg := range current {
		if msg.Status != OutboxStatusSent {
			pending++
		}
	}
	if pending == 0 {
		return 0, true, nil
	}
	err = s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		dbPath, err := s.storeSQLitePath(pointer)
		if err != nil {
			return err
		}
		current, err := loadSQLiteOutboxReplayFenceMessagesFileReadOnly(ctx, dbPath, fences)
		if err != nil {
			return err
		}
		pending := 0
		for _, msg := range current {
			if msg.Status != OutboxStatusSent {
				pending++
			}
		}
		if pending == 0 {
			return nil
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		state := newState()
		state.OutboxMessages = make(map[string]OutboxMessage, pending)
		state.MessageProvenance = map[string]MessageProvenanceRecord{}
		state.InboundEvents = map[string]InboundEvent{}
		state.Turns = map[string]Turn{}
		now := time.Now()
		for _, fence := range fences {
			msg, ok := current[fence.OutboxID]
			if !ok || msg.Status == OutboxStatusSent {
				continue
			}
			provenanceID := messageProvenanceID(msg.TeamsChatID, fence.TeamsMessageID)
			if existing, ok, err := loadSQLiteJSONRow[MessageProvenanceRecord](ctx, tx, `SELECT json FROM message_provenance WHERE id = ?`, provenanceID); err != nil {
				return err
			} else if ok {
				state.MessageProvenance[provenanceID] = existing
				if strings.TrimSpace(existing.Origin) == MessageOriginUserInbound {
					inboundEventID := strings.TrimSpace(existing.InboundID)
					if inboundEventID == "" {
						inboundEventID = inboundID(existing.TeamsChatID, existing.TeamsMessageID)
					}
					if inbound, found, err := loadSQLiteJSONRow[InboundEvent](ctx, tx, `SELECT json FROM inbound_events WHERE id = ?`, inboundEventID); err != nil {
						return err
					} else if found {
						state.InboundEvents[inbound.ID] = inbound
					}
				}
			}
			// The replay fence must see the canonical owner even when the global
			// provenance row is absent or originated from a non-inbound send.  A
			// failed Turn is a durable terminal fence and must not be promoted.
			if turnID := strings.TrimSpace(msg.TurnID); turnID != "" {
				if turn, found, err := loadSQLiteJSONRow[Turn](ctx, tx, `SELECT json FROM turns WHERE id = ?`, turnID); err != nil {
					return err
				} else if found {
					state.Turns[turn.ID] = turn
				}
			}
			if sessionID := strings.TrimSpace(msg.SessionID); sessionID != "" {
				if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, sessionID); err != nil {
					return err
				}
				checkpointID := sessionTranscriptCheckpointID(sessionID)
				if checkpoint, found, err := loadSQLiteCheckpointForID(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ?`, checkpointID); err != nil {
					return err
				} else if found {
					if err := validateImportCheckpointProvenance(checkpoint, sessionID, checkpointID); err != nil {
						return err
					}
					state.ImportCheckpoints[checkpoint.ID] = checkpoint
				}
			}
			if err := validateOutboxReplayCheckpointProvenance(&state, msg); err != nil {
				return err
			}
			if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, msg.ID); err != nil {
				return err
			}
			if err := loadSQLiteArtifactRecordsByIDTx(ctx, tx, &state, msg.ArtifactIDs); err != nil {
				return err
			}
			msg = applyOutboxSentProjectionLocked(&state, msg, fence.TeamsMessageID, now)
			state.OutboxMessages[msg.ID] = msg
			changed++
		}
		if changed == 0 {
			return nil
		}
		for _, msg := range state.OutboxMessages {
			if err := upsertSQLiteOutboxTx(ctx, tx, msg); err != nil {
				return err
			}
		}
		for _, record := range state.MessageProvenance {
			if err := upsertSQLiteProvenanceTx(ctx, tx, record); err != nil {
				return err
			}
		}
		if err := upsertSQLiteOutboxLinkedRecordsTx(ctx, tx, state); err != nil {
			return err
		}
		for _, inbound := range state.InboundEvents {
			if err := upsertSQLiteInboundTx(ctx, tx, inbound); err != nil {
				return err
			}
		}
		for _, turn := range state.Turns {
			if err := upsertSQLiteTurnTx(ctx, tx, turn); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return changed, handled, err
}

func loadSQLiteOutboxReplayFenceMessagesFileReadOnly(ctx context.Context, path string, fences []OutboxReplayFence) (map[string]OutboxMessage, error) {
	const maxAttempts = 3
	var changedErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		dbBefore, err := sqliteReadOnlyFileIdentityForPath(path)
		if err != nil {
			return nil, err
		}
		if !dbBefore.Exists {
			return nil, os.ErrNotExist
		}
		walBefore, err := sqliteReadOnlyFileIdentityForPath(path + "-wal")
		if err != nil {
			return nil, err
		}
		immutable := !walBefore.Exists || walBefore.Size == 0
		if !immutable {
			if err := requireSQLiteReadOnlySHM(path); err != nil {
				return nil, err
			}
		}
		current, err := loadSQLiteOutboxReplayFenceMessagesFileReadOnlyAttempt(ctx, path, immutable, fences)
		if err != nil {
			return nil, err
		}
		if !immutable {
			return current, nil
		}
		dbAfter, err := sqliteReadOnlyFileIdentityForPath(path)
		if err != nil {
			return nil, err
		}
		walAfter, err := sqliteReadOnlyFileIdentityForPath(path + "-wal")
		if err != nil {
			return nil, err
		}
		if dbBefore == dbAfter && walBefore == walAfter {
			return current, nil
		}
		changedErr = fmt.Errorf("database or WAL changed during immutable replay fence attempt %d", attempt+1)
	}
	return nil, fmt.Errorf("read stable sqlite replay fences after %d attempts: %w", maxAttempts, changedErr)
}

func loadSQLiteOutboxReplayFenceMessagesFileReadOnlyAttempt(
	ctx context.Context,
	path string,
	immutable bool,
	fences []OutboxReplayFence,
) (map[string]OutboxMessage, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	if immutable {
		query.Set("immutable", "1")
	}
	db, err := sql.Open("sqlite", sqliteFileURI(path, query))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA query_only = ON`); err != nil {
		return nil, err
	}
	return querySQLiteOutboxReplayFenceMessages(ctx, conn, fences)
}

func querySQLiteOutboxReplayFenceMessages(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, fences []OutboxReplayFence) (map[string]OutboxMessage, error) {
	fencesByID := make(map[string]OutboxReplayFence, len(fences))
	for _, fence := range fences {
		fencesByID[fence.OutboxID] = fence
	}
	current := make(map[string]OutboxMessage, len(fences))
	const replayFenceReadBatch = 400
	for start := 0; start < len(fences); start += replayFenceReadBatch {
		end := min(start+replayFenceReadBatch, len(fences))
		placeholders := make([]string, 0, end-start)
		args := make([]any, 0, end-start)
		for _, fence := range fences[start:end] {
			placeholders = append(placeholders, "?")
			args = append(args, fence.OutboxID)
		}
		rows, err := q.QueryContext(ctx, `SELECT json FROM outbox_messages WHERE json_valid(json) AND id IN (`+strings.Join(placeholders, ",")+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				_ = rows.Close()
				return nil, err
			}
			var msg OutboxMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				_ = rows.Close()
				return nil, err
			}
			fence := fencesByID[msg.ID]
			if err := validateOutboxReplayFence(msg, fence); err != nil {
				_ = rows.Close()
				return nil, err
			}
			current[msg.ID] = msg
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return current, nil
}

func (s *Store) pendingOutboxPageAtSQLite(ctx context.Context, query PendingOutboxQuery) (PendingOutboxPage, bool, error) {
	var out PendingOutboxPage
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		clauses := []string{
			"o.status IN (?, ?, ?)",
			sqliteOutboxProjectionValidSQL("o"),
			"(o.status <> ? OR o.teams_message_id <> '')",
			"NOT (o.status = ? AND o.teams_message_id <> '' AND COALESCE(" + sqliteSafeJSONExtract("o.json", "$.blocked_by_source_rewrite") + ", 0) = 1)",
		}
		args := []any{
			string(OutboxStatusQueued), string(OutboxStatusSending), string(OutboxStatusAccepted),
			string(OutboxStatusAccepted),
			string(OutboxStatusAccepted),
		}
		// Keep the SQLite admission predicate identical to
		// pendingOutboxMatchesQuery.  Once the send lease expires, every
		// Sending row without a durable Teams ID has an unknown external
		// outcome; an attempt token or diagnostic from a newer writer is not
		// sufficient proof that replay is safe.
		lastSendAttemptJSON := sqliteSafeJSONExtract("o.json", "$.last_send_attempt")
		lastSendAttempt := "julianday(" + lastSendAttemptJSON + ")"
		expiryCutoff := query.Now.Add(-outboxSendLease).UTC().Format(time.RFC3339Nano)
		expiredAttempt := "(" + lastSendAttempt + " IS NULL OR " + lastSendAttempt + " <= julianday(?))"
		unknownOutcome := "(o.status = ? AND o.teams_message_id = '' AND " + expiredAttempt + ")"
		if query.AmbiguousOnly {
			clauses = append(clauses, "o.status = ?", "o.teams_message_id = ''")
			args = append(args, string(OutboxStatusSending))
			if query.IncludeAmbiguous {
				clauses = append(clauses, unknownOutcome)
				args = append(args, string(OutboxStatusSending), expiryCutoff)
			} else {
				clauses = append(clauses, "0 = 1")
			}
		} else if !query.IncludeAmbiguous {
			clauses = append(clauses, "NOT "+unknownOutcome)
			args = append(args, string(OutboxStatusSending), expiryCutoff)
		} else {
			// IncludeAmbiguous is the cold recovery view. Include ordinary
			// work and expired unknown outcomes, but keep a fresh explicit
			// ambiguous row out until its lease expires.
			lastSendError := sqliteSafeJSONExtract("o.json", "$.last_send_error")
			clauses = append(clauses, "(o.status <> ? OR o.teams_message_id <> '' OR COALESCE("+lastSendError+", '') NOT LIKE 'ambiguous Graph send;%' OR "+unknownOutcome+")")
			args = append(args, string(OutboxStatusSending), string(OutboxStatusSending), expiryCutoff)
		}
		if !query.IgnoreRetryGate {
			if query.IncludeAmbiguous || query.AmbiguousOnly {
				clauses = append(clauses, "(o.status NOT IN (?, ?, ?) OR o.deliver_after = 0 OR o.deliver_after <= ?)")
				args = append(args, string(OutboxStatusQueued), string(OutboxStatusAccepted), string(OutboxStatusSending), sqliteTime(query.Now))
			} else {
				clauses = append(clauses, "(o.status NOT IN (?, ?) OR o.deliver_after = 0 OR o.deliver_after <= ?)")
				args = append(args, string(OutboxStatusQueued), string(OutboxStatusAccepted), sqliteTime(query.Now))
			}
		}
		if !query.IgnoreRateLimit {
			clauses = append(clauses, "(o.status = ? OR COALESCE(r.blocked_until, 0) = 0 OR COALESCE(r.blocked_until, 0) <= ?)")
			args = append(args, string(OutboxStatusAccepted), sqliteTime(query.Now))
		}
		if query.SessionID = strings.TrimSpace(query.SessionID); query.SessionID != "" {
			clauses = append(clauses, "o.session_id = ?")
			args = append(args, query.SessionID)
		}
		if query.TurnID = strings.TrimSpace(query.TurnID); query.TurnID != "" {
			clauses = append(clauses, "o.turn_id = ?")
			args = append(args, query.TurnID)
		}
		if query.TeamsChatID = strings.TrimSpace(query.TeamsChatID); query.TeamsChatID != "" {
			clauses = append(clauses, "o.teams_chat_id = ?")
			args = append(args, query.TeamsChatID)
		}
		if !query.After.IsZero() {
			clauses = append(clauses, "(o.created_at > ? OR (o.created_at = ? AND o.id > ?))")
			after := sqliteTime(query.After.CreatedAt)
			args = append(args, after, after, strings.TrimSpace(query.After.ID))
		}
		stmt := `SELECT o.json, o.created_at, o.id, COALESCE(o.deliver_after, 0), COALESCE(r.blocked_until, 0)
FROM outbox_messages o
LEFT JOIN chat_rate_limits r ON r.chat_id = o.teams_chat_id
WHERE ` + strings.Join(clauses, " AND ") + `
ORDER BY o.created_at, o.id`
		rawLimit := query.Limit
		if rawLimit > 0 {
			rawLimit++
			stmt += ` LIMIT ?`
			args = append(args, rawLimit)
		}
		rows, err := db.QueryContext(ctx, stmt, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		rawRows := 0
		for rows.Next() {
			var raw []byte
			var createdAtNanos int64
			var rowID string
			var deliverAfterNanos int64
			var blockedUntilNanos int64
			if err := rows.Scan(&raw, &createdAtNanos, &rowID, &deliverAfterNanos, &blockedUntilNanos); err != nil {
				return err
			}
			rawRows++
			if query.Limit > 0 && rawRows > query.Limit {
				out.More = true
				break
			}
			out.NextCursor = PendingOutboxCursor{CreatedAt: time.Unix(0, createdAtNanos), ID: rowID}
			var msg OutboxMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				// A syntactically valid but semantically damaged row must not
				// hide healthy work behind a whole-page decode error.  Keep the
				// cursor on this row and ask the caller for another page so the
				// malformed row is skipped without losing following records; the
				// raw bytes remain available to opaque-row repair diagnostics.
				out.More = true
				continue
			}
			// Older SQLite rows may have a populated compatibility column while
			// their JSON predates NextAttemptAt. Hydrate the projection so the JSON
			// and SQLite pending predicates agree.
			if msg.NextAttemptAt.IsZero() && deliverAfterNanos > 0 {
				msg.NextAttemptAt = time.Unix(0, deliverAfterNanos).UTC()
			}
			state := State{ChatRateLimits: map[string]ChatRateLimitState{}}
			if blockedUntilNanos > 0 {
				state.ChatRateLimits[msg.TeamsChatID] = ChatRateLimitState{ChatID: msg.TeamsChatID, BlockedUntil: time.Unix(0, blockedUntilNanos)}
			}
			if !pendingOutboxMatchesQuery(msg, state, query) {
				continue
			}
			out.Messages = append(out.Messages, msg)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		handled = true
		return nil
	})
	return out, handled, err
}

func (s *Store) sentOutboxMessagesForChatSQLite(ctx context.Context, chatID string) ([]OutboxMessage, bool, error) {
	var out []OutboxMessage
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		rows, err := db.QueryContext(ctx, `SELECT o.json FROM outbox_messages o
WHERE `+sqliteOutboxProjectionValidSQL("o")+`
  AND o.teams_chat_id = ?
  AND status = ?
ORDER BY o.created_at, o.id`, chatID, string(OutboxStatusSent))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				return err
			}
			var msg OutboxMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				return err
			}
			if msg.TeamsChatID == chatID && msg.Status == OutboxStatusSent {
				out = append(out, msg)
			}
		}
		return rows.Err()
	})
	return out, handled, err
}

func (s *Store) pendingSentOutboxSideEffectsSQLite(ctx context.Context, limit int) ([]OutboxMessage, bool, error) {
	var out []OutboxMessage
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		now := time.Now().UTC()
		// A syntactically valid row can still fail typed decoding (for example,
		// a legacy writer may have stored a number in last_send_error).  Do not
		// put a SQL LIMIT in front of that row-local decision: otherwise one bad
		// row at the front of the side-effect lane can hide the healthy rows
		// behind it forever.  The normal case is one query.  The keyset loop only
		// reads another bounded page when a malformed candidate was encountered,
		// so it does not add a scan to the successful-send hot path.
		pageSize := limit
		var cursorCreatedAt int64
		var cursorID string
		hasCursor := false
		for len(out) < limit {
			query := `SELECT o.json, o.created_at, o.id FROM outbox_messages o
WHERE ` + sqliteOutboxProjectionValidSQL("o") + `
  AND o.status = ? AND o.post_send_effects_pending = 1
  AND (o.deliver_after = 0 OR o.deliver_after <= ?)`
			args := []any{string(OutboxStatusSent), sqliteTime(now)}
			if hasCursor {
				query += `
  AND (o.created_at > ? OR (o.created_at = ? AND o.id > ?))`
				args = append(args, cursorCreatedAt, cursorCreatedAt, cursorID)
			}
			query += `
ORDER BY o.created_at, o.id
LIMIT ?`
			args = append(args, pageSize)
			rows, err := db.QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}
			pageRows := 0
			for rows.Next() {
				var raw []byte
				if err := rows.Scan(&raw, &cursorCreatedAt, &cursorID); err != nil {
					_ = rows.Close()
					return err
				}
				hasCursor = true
				pageRows++
				var msg OutboxMessage
				if err := json.Unmarshal(raw, &msg); err != nil {
					// The raw row remains available to the opaque-row repair path.
					// It is not safe to manufacture a Sent message from a partial
					// decode, but it is also not safe to let it abort this lane.
					continue
				}
				if msg.Status == OutboxStatusSent && msg.PostSendEffectsPending && outboxRetryGateDue(msg, now) {
					out = append(out, msg)
				}
				if len(out) >= limit {
					break
				}
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return err
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if len(out) >= limit || pageRows < pageSize {
				break
			}
		}
		return nil
	})
	return out, handled, err
}

func (s *Store) recentOutboxEchoCandidatesSQLite(ctx context.Context, query OutboxEchoCandidateQuery) ([]OutboxMessage, bool, error) {
	var out []OutboxMessage
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		for _, item := range []struct {
			status    OutboxStatus
			ascending bool
		}{
			{status: OutboxStatusSending, ascending: true},
			{status: OutboxStatusAccepted, ascending: false},
			{status: OutboxStatusSent, ascending: false},
		} {
			statusStart := len(out)
			var cursorCreatedAt int64
			var cursorID string
			hasCursor := false
			for page := 0; page < sqliteOutboxEchoMaxPages && len(out)-statusStart < query.LimitPerStatus; page++ {
				querySQL := `SELECT o.json, COALESCE(o.created_at, 0), o.id
FROM outbox_messages o
WHERE ` + sqliteOutboxProjectionValidSQL("o") + `
  AND o.status = ? AND o.teams_chat_id = ?`
				args := []any{string(item.status), query.TeamsChatID}
				if hasCursor {
					if item.ascending {
						querySQL += `
  AND (COALESCE(o.created_at, 0) > ? OR (COALESCE(o.created_at, 0) = ? AND o.id > ?))`
					} else {
						querySQL += `
  AND (COALESCE(o.created_at, 0) < ? OR (COALESCE(o.created_at, 0) = ? AND o.id < ?))`
					}
					args = append(args, cursorCreatedAt, cursorCreatedAt, cursorID)
				}
				if item.ascending {
					querySQL += `
ORDER BY COALESCE(o.created_at, 0) ASC, o.id ASC`
				} else {
					querySQL += `
ORDER BY COALESCE(o.created_at, 0) DESC, o.id DESC`
				}
				querySQL += `
LIMIT ?`
				args = append(args, query.LimitPerStatus)
				rows, err := db.QueryContext(ctx, querySQL, args...)
				if err != nil {
					return err
				}
				pageRows := 0
				for rows.Next() {
					var raw []byte
					if err := rows.Scan(&raw, &cursorCreatedAt, &cursorID); err != nil {
						_ = rows.Close()
						return err
					}
					hasCursor = true
					pageRows++
					var msg OutboxMessage
					if err := json.Unmarshal(raw, &msg); err != nil {
						// A semantically malformed row is local quarantine evidence,
						// not a reason to abort echo recovery for this chat.
						continue
					}
					if strings.TrimSpace(msg.ID) != strings.TrimSpace(cursorID) ||
						strings.TrimSpace(msg.TeamsChatID) != query.TeamsChatID ||
						msg.Status != item.status {
						continue
					}
					out = append(out, msg)
					if len(out)-statusStart >= query.LimitPerStatus {
						break
					}
				}
				if err := rows.Err(); err != nil {
					_ = rows.Close()
					return err
				}
				if err := rows.Close(); err != nil {
					return err
				}
				if len(out)-statusStart >= query.LimitPerStatus || pageRows < query.LimitPerStatus || !hasCursor {
					break
				}
			}
		}
		return nil
	})
	return out, handled, err
}

func (s *Store) earlierUnsentOutboxSQLite(ctx context.Context, msg OutboxMessage) (OutboxMessage, bool, bool, error) {
	var out OutboxMessage
	found := false
	handled := false
	chatID := strings.TrimSpace(msg.TeamsChatID)
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		turnID := strings.TrimSpace(msg.TurnID)
		expiryCutoff := time.Now().UTC().Add(-outboxSendLease).Format(time.RFC3339Nano)
		out, found, err = loadSQLiteJSONRow[OutboxMessage](ctx, db, `SELECT o.json FROM outbox_messages o
WHERE `+sqliteOutboxProjectionValidSQL("o")+`
  AND o.teams_chat_id = ?
  AND o.id <> ?
  AND o.sequence > 0
	AND o.sequence < ?
	AND o.status NOT IN (?, ?)
	AND NOT (o.status = ? AND o.teams_message_id <> '' AND COALESCE(`+sqliteSafeJSONExtract("o.json", "$.blocked_by_source_rewrite")+`, 0) = 1)
	AND NOT (o.status = ? AND COALESCE(`+sqliteSafeJSONExtract("o.json", "$.last_send_error")+`, '') LIKE 'ambiguous Graph send;%' AND (TRIM(COALESCE(o.turn_id, '')) <> ? OR ? = ''))
	AND NOT (o.status = ? AND o.teams_message_id = '' AND (`+sqliteSafeJSONExtract("o.json", "$.last_send_attempt")+` IS NULL OR julianday(`+sqliteSafeJSONExtract("o.json", "$.last_send_attempt")+`) <= julianday(?)) AND (TRIM(COALESCE(o.turn_id, '')) <> ? OR ? = ''))
ORDER BY o.sequence, o.created_at, o.id
LIMIT 1`, chatID, strings.TrimSpace(msg.ID), msg.Sequence, string(OutboxStatusSent), string(OutboxStatusSkipped), string(OutboxStatusAccepted), string(OutboxStatusSending), turnID, turnID, string(OutboxStatusSending), expiryCutoff, turnID, turnID)
		if err != nil {
			return err
		}
		handled = true
		return nil
	})
	return out, found, handled, err
}

func (s *Store) earlierUnsentOutboxesSQLite(ctx context.Context, msg OutboxMessage) ([]OutboxMessage, bool, error) {
	var out []OutboxMessage
	handled := false
	chatID := strings.TrimSpace(msg.TeamsChatID)
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		turnID := strings.TrimSpace(msg.TurnID)
		expiryCutoff := time.Now().UTC().Add(-outboxSendLease).Format(time.RFC3339Nano)
		rows, err := db.QueryContext(ctx, `SELECT o.json FROM outbox_messages o
WHERE `+sqliteOutboxProjectionValidSQL("o")+`
  AND o.teams_chat_id = ?
  AND o.id <> ?
  AND o.sequence > 0
	AND o.sequence < ?
	AND o.status NOT IN (?, ?)
	AND NOT (o.status = ? AND o.teams_message_id <> '' AND COALESCE(`+sqliteSafeJSONExtract("o.json", "$.blocked_by_source_rewrite")+`, 0) = 1)
	AND NOT (o.status = ? AND COALESCE(`+sqliteSafeJSONExtract("o.json", "$.last_send_error")+`, '') LIKE 'ambiguous Graph send;%' AND (TRIM(COALESCE(o.turn_id, '')) <> ? OR ? = ''))
	AND NOT (o.status = ? AND o.teams_message_id = '' AND (`+sqliteSafeJSONExtract("o.json", "$.last_send_attempt")+` IS NULL OR julianday(`+sqliteSafeJSONExtract("o.json", "$.last_send_attempt")+`) <= julianday(?)) AND (TRIM(COALESCE(o.turn_id, '')) <> ? OR ? = ''))
			ORDER BY o.sequence, o.created_at, o.id`, chatID, strings.TrimSpace(msg.ID), msg.Sequence, string(OutboxStatusSent), string(OutboxStatusSkipped), string(OutboxStatusAccepted), string(OutboxStatusSending), turnID, turnID, string(OutboxStatusSending), expiryCutoff, turnID, turnID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				return err
			}
			var candidate OutboxMessage
			if err := json.Unmarshal(raw, &candidate); err != nil {
				return err
			}
			out = append(out, candidate)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		handled = true
		return nil
	})
	return out, handled, err
}

func (s *Store) chatPollSQLite(ctx context.Context, chatID string) (ChatPollState, bool, bool, error) {
	var out ChatPollState
	var found bool
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		out, found, err = loadSQLiteJSONRow[ChatPollState](ctx, db, `SELECT json FROM chat_polls WHERE chat_id = ?`, chatID)
		handled = true
		return err
	})
	return out, found, handled, err
}

// updateChatPollSQLite is the narrow write path for the high-frequency poll
// frontier. It deliberately updates one JSON row in one short transaction;
// using Store.Update here would delete/recreate every operational table from a
// potentially stale full-state snapshot.
func (s *Store) updateChatPollSQLite(ctx context.Context, chatID string, mutate func(*ChatPollState) error) (ChatPollState, bool, bool, error) {
	return s.updateChatPollSQLiteWithCapability(ctx, chatID, nil, mutate)
}

func (s *Store) updateChatPollSQLiteWithCapability(ctx context.Context, chatID string, capability *ChatPollAttemptCapability, mutate func(*ChatPollState) error) (ChatPollState, bool, bool, error) {
	var out ChatPollState
	changed := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if poll, ok, err := loadSQLiteJSONRow[ChatPollState](ctx, tx, `SELECT json FROM chat_polls WHERE chat_id = ?`, chatID); err != nil {
			return err
		} else if ok {
			out = poll
		}
		if capability != nil && capability.LeaseGeneration > 0 {
			matches, err := sqliteStoreActiveOwnerCapabilityMatchesTx(ctx, tx, capability.Owner, capability.LeaseGeneration)
			if err != nil {
				return err
			}
			if !matches {
				handled = true
				return nil
			}
			if chatPollAttemptNeedsActiveLeaseForReclaim(&out, capability) {
				lease, err := loadSQLiteControlLease(ctx, tx)
				if err != nil {
					return err
				}
				if !storeOwnerCapabilityMatchesMaterializedActiveLease(&State{ControlLease: lease}, capability.Owner, capability.LeaseGeneration) {
					handled = true
					return nil
				}
			}
		}
		before := out
		preserveOpaque := chatPollHasOpaqueRecoveryEvidence(out)
		if err := mutate(&out); err != nil {
			if errors.Is(err, errStoreNoChange) {
				handled = true
				return nil
			}
			return err
		}
		out.ChatID = chatID
		if reflect.DeepEqual(before, out) {
			handled = true
			return nil
		}
		if preserveOpaque && chatPollHasOpaqueRecoveryEvidence(out) {
			// The callback may update retry metadata, but it cannot safely
			// replace the raw malformed receipt. Wait for an explicit recovery
			// mutation that clears the marker and writes a canonical gap/frontier.
			handled = true
			return nil
		}
		if err := upsertSQLiteChatPollTx(ctx, tx, out); err != nil {
			return err
		}
		changed = true
		handled = true
		return tx.Commit()
	})
	return out, changed, handled, err
}

func (s *Store) chatSessionActivitySQLite(ctx context.Context, chatID string) (bool, bool, bool, error) {
	matched := false
	active := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		handled = true
		rows, err := db.QueryContext(ctx, `SELECT status FROM sessions WHERE teams_chat_id = ?`, chatID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var rawStatus string
			if err := rows.Scan(&rawStatus); err != nil {
				return err
			}
			matched = true
			if sessionStatusIsActive(SessionStatus(rawStatus)) {
				active = true
			}
		}
		return rows.Err()
	})
	return matched, active, handled, err
}

func (s *Store) recordChatPollSuccessWithContinuationAndScheduleSQLite(ctx context.Context, chatID string, lastModifiedCursor time.Time, seeded bool, windowFull bool, fetched int, continuationPath string, schedule func(ChatPollState) (ChatPollScheduleUpdate, error)) (ChatPollState, bool, error) {
	var out ChatPollState
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state := State{SchemaVersion: SchemaVersion, ChatPolls: map[string]ChatPollState{}}
		if poll, ok, err := loadSQLiteJSONRow[ChatPollState](ctx, tx, `SELECT json FROM chat_polls WHERE chat_id = ?`, chatID); err != nil {
			return err
		} else if ok {
			state.ChatPolls[chatID] = poll
		}
		now := time.Now()
		poll, changed := applyChatPollSuccessLocked(&state, chatID, lastModifiedCursor, seeded, windowFull, fetched, continuationPath, now)
		if schedule != nil {
			update, err := schedule(poll)
			if err != nil {
				return err
			}
			update.ChatID = strings.TrimSpace(update.ChatID)
			switch {
			case update.ChatID == "":
				update.ChatID = chatID
			case update.ChatID != chatID:
				return fmt.Errorf("chat poll schedule chat id %q does not match success chat id %q", update.ChatID, chatID)
			}
			var scheduleChanged bool
			poll, scheduleChanged, err = applyChatPollScheduleUpdateLocked(&state, update, time.Now())
			if err != nil {
				return err
			}
			changed = changed || scheduleChanged
		}
		if changed {
			invalidateChatPollAttempt(&poll)
			state.ChatPolls[chatID] = poll
		}
		out = poll
		handled = true
		if !changed {
			return nil
		}
		if err := upsertSQLiteChatPollTx(ctx, tx, poll); err != nil {
			return err
		}
		return tx.Commit()
	})
	return out, handled, err
}

func (s *Store) recordChatPollErrorWithBlockSQLite(ctx context.Context, chatID string, message string, blockedUntil time.Time) (bool, error) {
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state := State{SchemaVersion: SchemaVersion, ChatPolls: map[string]ChatPollState{}}
		if poll, ok, err := loadSQLiteJSONRow[ChatPollState](ctx, tx, `SELECT json FROM chat_polls WHERE chat_id = ?`, chatID); err != nil {
			return err
		} else if ok {
			state.ChatPolls[chatID] = poll
		}
		poll := applyChatPollErrorWithBlockLocked(&state, chatID, message, blockedUntil, time.Now())
		invalidateChatPollAttempt(&poll)
		if chatPollHasOpaqueRecoveryEvidence(poll) {
			handled = true
			return tx.Commit()
		}
		if err := upsertSQLiteChatPollTx(ctx, tx, poll); err != nil {
			return err
		}
		handled = true
		return tx.Commit()
	})
	return handled, err
}

func (s *Store) markChatPollParkNoticeSentSQLite(ctx context.Context, chatID string, at time.Time) (ChatPollState, bool, error) {
	var out ChatPollState
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if poll, ok, err := loadSQLiteJSONRow[ChatPollState](ctx, tx, `SELECT json FROM chat_polls WHERE chat_id = ?`, chatID); err != nil {
			return err
		} else if ok {
			out = poll
		}
		out.ChatID = chatID
		out.ParkNoticeSentAt = at
		out.UpdatedAt = time.Now()
		out.PollRevision++
		out.ScheduleRevision++
		invalidateChatPollAttempt(&out)
		handled = true
		if chatPollHasOpaqueRecoveryEvidence(out) {
			return tx.Commit()
		}
		if err := upsertSQLiteChatPollTx(ctx, tx, out); err != nil {
			return err
		}
		return tx.Commit()
	})
	return out, handled, err
}

func (s *Store) chatRateLimitSQLite(ctx context.Context, chatID string) (ChatRateLimitState, bool, bool, error) {
	var out ChatRateLimitState
	var found bool
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		out, found, err = loadSQLiteJSONRow[ChatRateLimitState](ctx, db, `SELECT json FROM chat_rate_limits WHERE chat_id = ?`, chatID)
		handled = true
		return err
	})
	return out, found, handled, err
}

func (s *Store) updateChatPollSchedulesSQLite(ctx context.Context, updates []ChatPollScheduleUpdate) (map[string]ChatPollState, bool, error) {
	out := make(map[string]ChatPollState, len(updates))
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state := State{SchemaVersion: SchemaVersion, ChatPolls: map[string]ChatPollState{}}
		for _, update := range updates {
			chatID := strings.TrimSpace(update.ChatID)
			if chatID == "" {
				continue
			}
			if _, ok := state.ChatPolls[chatID]; ok {
				continue
			}
			if poll, ok, err := loadSQLiteJSONRow[ChatPollState](ctx, tx, `SELECT json FROM chat_polls WHERE chat_id = ?`, chatID); err != nil {
				return err
			} else if ok {
				state.ChatPolls[chatID] = poll
			}
		}
		now := time.Now()
		changed := false
		for _, update := range updates {
			poll, updateChanged, err := applyChatPollScheduleUpdateLocked(&state, update, now)
			if err != nil {
				return err
			}
			if updateChanged {
				invalidateChatPollAttempt(&poll)
				state.ChatPolls[poll.ChatID] = poll
			}
			out[poll.ChatID] = poll
			changed = changed || updateChanged
		}
		handled = true
		if !changed {
			return nil
		}
		for _, poll := range out {
			if chatPollHasOpaqueRecoveryEvidence(poll) {
				continue
			}
			if err := upsertSQLiteChatPollTx(ctx, tx, poll); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return out, handled, err
}

func (s *Store) boostChatPollAfterFinalAnswerSQLite(ctx context.Context, req FinalAnswerPollBoostRequest, capability storeOwnerCapability) (ChatPollState, bool, bool, error) {
	var out ChatPollState
	changed := false
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		state := State{
			SchemaVersion:     SchemaVersion,
			Sessions:          map[string]SessionContext{},
			ChatPolls:         map[string]ChatPollState{},
			ImportCheckpoints: map[string]ImportCheckpoint{},
		}
		if capability.bound() {
			state.ControlLease, err = loadSQLiteControlLease(ctx, tx)
			if err != nil {
				return err
			}
			if err := validateStoreOwnerCapability(&state, capability); err != nil {
				return err
			}
		}
		if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, req.SessionID); err != nil {
			return err
		} else if ok {
			state.Sessions[session.ID] = session
		}
		if poll, ok, err := loadSQLiteJSONRow[ChatPollState](ctx, tx, `SELECT json FROM chat_polls WHERE chat_id = ?`, req.TeamsChatID); err != nil {
			return err
		} else if ok {
			state.ChatPolls[poll.ChatID] = poll
			out = poll
		}
		if checkpoint, ok, err := loadSQLiteCheckpointForID(ctx, tx, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE id = ? AND status = ?`, transcriptCheckpointIDForSession(req.SessionID), importCheckpointStatusImporting); err != nil {
			return err
		} else if ok {
			state.ImportCheckpoints[checkpoint.ID] = checkpoint
		}
		if checkpoint := state.ImportCheckpoints[transcriptCheckpointIDForSession(req.SessionID)]; checkpoint.Status == importCheckpointStatusImporting {
			owner, err := loadSQLiteServiceOwnerTx(ctx, tx)
			if err != nil {
				return err
			}
			state.ServiceOwner = owner
		}
		handled = true
		poll, ok := state.ChatPolls[req.TeamsChatID]
		if !ok || !finalAnswerPollBoostGuardAllows(&state, req, poll, req.NextPollAt) {
			return nil
		}
		next, updateChanged, err := applyChatPollScheduleUpdateLocked(&state, finalAnswerPollBoostScheduleUpdate(req), req.NextPollAt)
		if err != nil {
			return err
		}
		if updateChanged && next.Attempt != nil {
			// A final-answer delivery may race the poll that discovered the
			// inbound page. This update changes only scheduling/activity fields;
			// merge it into the active poll capability instead of making the
			// page owner look stale and leaving its receipt stranded.
			next.Attempt.ExpectedPollRevision = next.PollRevision
			next.Attempt.ExpectedScheduleRevision = next.ScheduleRevision
		}
		out = next
		changed = updateChanged
		if !updateChanged {
			return nil
		}
		if err := upsertSQLiteChatPollTx(ctx, tx, next); err != nil {
			return err
		}
		return tx.Commit()
	})
	return out, changed, handled, err
}

func loadSQLiteServiceOwnerTx(ctx context.Context, tx *sql.Tx) (*OwnerMetadata, error) {
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT json FROM runtime_state WHERE key = ?`, sqliteRuntimeKeyServiceOwner).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var owner *OwnerMetadata
	if err := json.Unmarshal(raw, &owner); err != nil {
		return nil, err
	}
	return owner, nil
}

func (s *Store) setChatRateLimitSQLite(ctx context.Context, chatID string, blockedUntil time.Time, reason string, outboxID string) (ChatRateLimitState, bool, error) {
	var out ChatRateLimitState
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if existing, ok, err := loadSQLiteJSONRow[ChatRateLimitState](ctx, tx, `SELECT json FROM chat_rate_limits WHERE chat_id = ?`, chatID); err != nil {
			return err
		} else if ok {
			out = existing
		}
		out.ChatID = chatID
		out.BlockedUntil = blockedUntil
		out.Reason = trimDiagnostic(reason, 240)
		if strings.TrimSpace(outboxID) != "" {
			out.PoisonOutboxID = strings.TrimSpace(outboxID)
		}
		out.UpdatedAt = time.Now()
		if err := upsertSQLiteChatRateLimitTx(ctx, tx, out); err != nil {
			return err
		}
		handled = true
		return tx.Commit()
	})
	return out, handled, err
}

func (s *Store) clearChatRateLimitSQLite(ctx context.Context, chatID string) (bool, error) {
	handled := false
	err := s.withStateLock(ctx, func() error {
		pointer, ok, err := s.currentSQLitePointerUnlocked()
		if err != nil || !ok {
			return err
		}
		db, err := s.sqliteDBUnlocked(pointer)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `DELETE FROM chat_rate_limits WHERE chat_id = ?`, chatID); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT json, COALESCE(deliver_after, 0) FROM outbox_messages WHERE teams_chat_id = ? AND status = ?`, chatID, string(OutboxStatusQueued))
		if err != nil {
			return err
		}
		var queued []OutboxMessage
		for rows.Next() {
			var raw []byte
			var deliverAfterNanos int64
			if err := rows.Scan(&raw, &deliverAfterNanos); err != nil {
				_ = rows.Close()
				return err
			}
			var msg OutboxMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				_ = rows.Close()
				return err
			}
			if msg.NextAttemptAt.IsZero() && deliverAfterNanos > 0 {
				msg.NextAttemptAt = time.Unix(0, deliverAfterNanos).UTC()
			}
			if !msg.NextAttemptAt.IsZero() {
				msg.NextAttemptAt = time.Time{}
				queued = append(queued, msg)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, msg := range queued {
			if err := upsertSQLiteOutboxTx(ctx, tx, msg); err != nil {
				return err
			}
		}
		err = tx.Commit()
		handled = true
		return err
	})
	return handled, err
}

func sqliteTime(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func sqliteBool(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
