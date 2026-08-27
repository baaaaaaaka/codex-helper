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
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	sqlite "modernc.org/sqlite"
)

const (
	storeSQLiteBackend              = "store-sqlite"
	storeSQLiteVersion              = 1
	storeSQLiteFileName             = "store.sqlite"
	storeSQLitePointerSchemaVersion = SchemaVersion + 1
	// Pointer schema 6 was emitted while the JSON state schema was 5. Pointer
	// schemas 6 through 8 remain readable for migration, but the current
	// pointer schema (9) is an upgrade-only boundary because schema-7 helpers
	// cannot preserve the source read-proof and delivery-recovery safety fields.
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
			state, err := s.loadSQLiteStateUnlocked(ctx, pointer)
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
	return s.sqlitePointerFromDataUnlocked(data, info)
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
	data, err := os.ReadFile(s.path)
	if err != nil {
		return storeSQLitePointer{}, false, err
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

func (s *Store) hotPollScheduleSQLite(ctx context.Context, includeParkedSkip bool) (State, map[string]bool, bool, error) {
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
		parkProbeDueAt := sqliteTime(time.Now())
		selected, err := loadSQLiteSelectedStateWithChatPollQuery(ctx, db, hotPollScheduleBaseFields,
			`SELECT json FROM chat_polls
WHERE COALESCE(parked_skip_eligible, 0) = 0
   OR (poll_state = ? AND COALESCE(next_poll_at, 0) <= ?)`,
			chatPollStateParked, parkProbeDueAt,
		)
		if err != nil {
			return err
		}
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns WHERE status IN (?, ?)`, selected.Turns, func(v Turn) string { return v.ID }, string(TurnStatusQueued), string(TurnStatusRunning)); err != nil {
			return err
		}
		if err := loadSQLiteCheckpointMap(ctx, db, `SELECT id, session_id, status, updated_at, json FROM import_checkpoints WHERE status = ?`, selected.ImportCheckpoints, sqliteImportCheckpointImporting); err != nil {
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

func (s *Store) hotPollWorkCandidatesSQLite(ctx context.Context, controlChatID string, idleBefore time.Time) ([]SessionContext, bool, error) {
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
		hasSessions, err := loadSQLiteHasSessions(ctx, db)
		if err != nil {
			return err
		}
		if !hasSessions {
			return nil
		}
		sessions, err := loadSQLiteHotPollWorkCandidates(ctx, db, controlChatID, idleBefore)
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
		hasSessions, err = loadSQLiteHasSessions(ctx, db)
		if err != nil {
			return err
		}
		handled = true
		return nil
	})
	return hasSessions, handled, err
}

func (s *Store) sqliteDBUnlocked(pointer storeSQLitePointer) (*sql.DB, error) {
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
		`SELECT COALESCE(json_extract(value, '$.control_chat'), '{}') FROM state_meta WHERE key = 'state_json'`,
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
		`SELECT COALESCE(json_extract(value, '$.control_chat'), '{}') FROM state_meta WHERE key = 'state_json'`,
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
			return State{}, err
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
		`CREATE TABLE IF NOT EXISTS outbox_messages (id TEXT PRIMARY KEY, session_id TEXT, turn_id TEXT, teams_chat_id TEXT, teams_message_id TEXT, status TEXT, sequence INTEGER, created_at INTEGER, deliver_after INTEGER, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_messages(status, teams_chat_id, created_at, id)`,
		`CREATE INDEX IF NOT EXISTS outbox_session_idx ON outbox_messages(session_id, status, created_at, id)`,
		`CREATE TABLE IF NOT EXISTS message_provenance (id TEXT PRIMARY KEY, teams_chat_id TEXT, teams_message_id TEXT, origin TEXT, session_id TEXT, json BLOB NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS message_provenance_lookup_idx ON message_provenance(teams_chat_id, teams_message_id, origin)`,
		`CREATE TABLE IF NOT EXISTS chat_polls (chat_id TEXT PRIMARY KEY, next_poll_at INTEGER, poll_state TEXT, last_activity_at INTEGER, park_notice_sent_at INTEGER, parked_skip_eligible INTEGER, updated_at INTEGER, json BLOB NOT NULL)`,
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
	if _, err := db.Exec(`ALTER TABLE outbox_messages ADD COLUMN teams_message_id TEXT`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE outbox_messages ADD COLUMN turn_id TEXT`); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
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
	if err := backfillSQLiteChatPollDerivedColumns(db); err != nil {
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
		`CREATE INDEX IF NOT EXISTS chat_polls_parked_skip_idx ON chat_polls(parked_skip_eligible, chat_id)`,
		`CREATE INDEX IF NOT EXISTS chat_polls_auto_park_idx ON chat_polls(last_activity_at, chat_id) WHERE parked_skip_eligible = 0 AND last_activity_at > 0 AND poll_state IN ('cold', 'parked')`,
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
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO sessions(id, teams_chat_id, status, updated_at, json) VALUES (?, ?, ?, ?, ?)`, state.Sessions, func(v SessionContext) []any {
		return []any{v.ID, v.TeamsChatID, string(v.Status), sqliteTime(v.UpdatedAt)}
	}); err != nil {
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
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, state.OutboxMessages, func(v OutboxMessage) []any {
		return []any{v.ID, v.SessionID, v.TurnID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), string(v.Status), v.Sequence, sqliteTime(v.CreatedAt), int64(0)}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO message_provenance(id, teams_chat_id, teams_message_id, origin, session_id, json) VALUES (?, ?, ?, ?, ?, ?)`, state.MessageProvenance, func(v MessageProvenanceRecord) []any {
		return []any{v.ID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), v.Origin, v.SessionID}
	}); err != nil {
		return err
	}
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO chat_polls(chat_id, next_poll_at, poll_state, last_activity_at, park_notice_sent_at, parked_skip_eligible, updated_at, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, state.ChatPolls, func(v ChatPollState) []any {
		return []any{v.ChatID, sqliteTime(v.NextPollAt), v.PollState, sqliteTime(v.LastActivityAt), sqliteTime(v.ParkNoticeSentAt), sqliteBool(chatPollParkedSkipEligible(v)), sqliteTime(v.UpdatedAt)}
	}); err != nil {
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

type opaqueSQLiteCheckpointRow struct {
	ID        string
	IDValid   bool
	SessionID sql.NullString
	Status    sql.NullString
	UpdatedAt sql.NullInt64
	Raw       []byte
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
	rows, err := db.Query(`SELECT chat_id, json FROM chat_polls WHERE park_notice_sent_at IS NULL OR parked_skip_eligible IS NULL OR last_activity_at IS NULL`)
	if err != nil {
		return err
	}
	type chatPollDerivedUpdate struct {
		ChatID             string
		LastActivityAt     int64
		ParkNoticeSentAt   int64
		ParkedSkipEligible int64
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
			_ = rows.Close()
			return err
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
			LastActivityAt:     sqliteTime(poll.LastActivityAt),
			ParkNoticeSentAt:   sqliteTime(poll.ParkNoticeSentAt),
			ParkedSkipEligible: sqliteBool(chatPollParkedSkipEligible(poll)),
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
	stmt, err := tx.Prepare(`UPDATE chat_polls SET last_activity_at = ?, park_notice_sent_at = ?, parked_skip_eligible = ? WHERE chat_id = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, update := range updates {
		if _, err := stmt.Exec(update.LastActivityAt, update.ParkNoticeSentAt, update.ParkedSkipEligible, update.ChatID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func backfillSQLiteInboundDerivedColumns(db *sql.DB) error {
	_, err := db.Exec(`UPDATE inbound_events
SET received_at = COALESCE(CAST(strftime('%s', json_extract(json, '$.received_at')) AS INTEGER) * 1000000000, 0)
WHERE received_at IS NULL`)
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
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM sessions`, state.Sessions, func(v SessionContext) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM inbound_events`, state.InboundEvents, func(v InboundEvent) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns`, state.Turns, func(v Turn) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM outbox_messages`, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM message_provenance`, state.MessageProvenance, func(v MessageProvenanceRecord) string { return v.ID }); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM chat_polls`, state.ChatPolls, func(v ChatPollState) string { return v.ChatID }); err != nil {
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
	return loadSQLiteSelectedStateWithChatPollQuery(ctx, db, wanted, `SELECT json FROM chat_polls`)
}

func loadSQLiteSelectedStateWithChatPollQuery(ctx context.Context, db *sql.DB, wanted map[string]struct{}, chatPollQuery string, chatPollArgs ...any) (State, error) {
	state, err := loadSQLiteColdState(ctx, db)
	if err != nil {
		return State{}, err
	}
	if err := overlaySQLiteRuntimeState(ctx, db, &state); err != nil {
		return State{}, err
	}
	if _, ok := wanted["sessions"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM sessions`, state.Sessions, func(v SessionContext) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["inbound_events"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM inbound_events`, state.InboundEvents, func(v InboundEvent) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["turns"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns`, state.Turns, func(v Turn) string { return v.ID }); err != nil {
			return State{}, err
		}
	}
	if _, ok := wanted["outbox_messages"]; ok {
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM outbox_messages`, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
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
			chatPollQuery = `SELECT json FROM chat_polls`
		}
		if err := loadSQLiteJSONMap(ctx, db, chatPollQuery, state.ChatPolls, func(v ChatPollState) string { return v.ChatID }, chatPollArgs...); err != nil {
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

func loadSQLiteHotPollWorkCandidates(ctx context.Context, db *sql.DB, controlChatID string, idleBefore time.Time) ([]SessionContext, error) {
	controlChatID = strings.TrimSpace(controlChatID)
	args := []any{string(SessionStatusActive), controlChatID, controlChatID, chatPollStateParked, sqliteTime(time.Now())}
	excludeIdle := ""
	if !idleBefore.IsZero() {
		excludeIdle = `  AND NOT (
    COALESCE(p.last_activity_at, 0) > 0
    AND COALESCE(p.last_activity_at, 0) <= ?
    AND COALESCE(s.updated_at, 0) <= ?
    AND COALESCE(p.parked_skip_eligible, 0) = 0
    AND p.poll_state IN (?)
    AND NOT EXISTS (
      SELECT 1 FROM turns t
      WHERE t.session_id = s.id
        AND t.status IN (?, ?)
    )
  )
		`
		idleBeforeUnix := sqliteTime(idleBefore)
		args = append(args, idleBeforeUnix, idleBeforeUnix, chatPollStateCold, string(TurnStatusQueued), string(TurnStatusRunning))
	}
	rows, err := db.QueryContext(ctx, `SELECT s.json
FROM sessions s
LEFT JOIN chat_polls p ON p.chat_id = s.teams_chat_id
WHERE (s.status IS NULL OR s.status = '' OR s.status = ?)
  AND COALESCE(s.teams_chat_id, '') != ''
  AND (? = '' OR s.teams_chat_id != ?)
  AND (p.chat_id IS NULL OR COALESCE(p.parked_skip_eligible, 0) = 0 OR (p.poll_state = ? AND COALESCE(p.next_poll_at, 0) <= ?))
`+excludeIdle+`ORDER BY s.updated_at DESC, s.id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionContext
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var session SessionContext
		if err := json.Unmarshal(raw, &session); err != nil {
			return nil, err
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
	rows, err := db.QueryContext(ctx, `SELECT s.json, p.json
FROM chat_polls p
JOIN sessions s ON s.teams_chat_id = p.chat_id
WHERE p.last_activity_at > 0
  AND p.last_activity_at <= ?
  AND COALESCE(s.updated_at, 0) <= ?
  AND (s.status IS NULL OR s.status = '' OR s.status = ?)
  AND COALESCE(s.teams_chat_id, '') != ''
  AND (? = '' OR s.teams_chat_id != ?)
  AND COALESCE(p.parked_skip_eligible, 0) = 0
  AND p.poll_state IN (?, ?)
  AND NOT EXISTS (
    SELECT 1 FROM turns t
    WHERE t.session_id = s.id
      AND t.status IN (?, ?)
  )
ORDER BY p.last_activity_at ASC, s.updated_at ASC, s.id
LIMIT ?`, sqliteTime(idleBefore), sqliteTime(idleBefore), string(SessionStatusActive), controlChatID, controlChatID, chatPollStateCold, chatPollStateParked, string(TurnStatusQueued), string(TurnStatusRunning), limit)
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
			return nil, err
		}
		if err := json.Unmarshal(pollRaw, &candidate.Poll); err != nil {
			return nil, err
		}
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
			return nil, time.Time{}, 0, 0, false, err
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
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		var zero T
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
	sqliteRuntimeKeyScope           = "scope"
	sqliteRuntimeKeyMachineIdentity = "machine_identity"
	sqliteRuntimeKeyMachines        = "machines"
	sqliteRuntimeKeyControlLease    = "control_lease"
	sqliteRuntimeKeyServiceOwner    = "service_owner"
	sqliteRuntimeKeyLockOwner       = "lock_owner"
	sqliteRuntimeKeyServiceControl  = "service_control"
	sqliteRuntimeKeyUpgrade         = "upgrade"
	sqliteRuntimeKeyAutoUpdate      = "auto_update"
)

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
	return nil
}

func loadSQLiteRuntimeState(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) (State, map[string]bool, error) {
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
		seen[key] = true
		switch key {
		case sqliteRuntimeKeyScope:
			if err := json.Unmarshal(raw, &state.Scope); err != nil {
				return State{}, nil, err
			}
		case sqliteRuntimeKeyMachineIdentity:
			if err := json.Unmarshal(raw, &state.MachineIdentity); err != nil {
				return State{}, nil, err
			}
		case sqliteRuntimeKeyMachines:
			if err := json.Unmarshal(raw, &state.Machines); err != nil {
				return State{}, nil, err
			}
			if state.Machines == nil {
				state.Machines = map[string]MachineRecord{}
			}
		case sqliteRuntimeKeyControlLease:
			if err := json.Unmarshal(raw, &state.ControlLease); err != nil {
				return State{}, nil, err
			}
		case sqliteRuntimeKeyServiceOwner:
			if err := json.Unmarshal(raw, &state.ServiceOwner); err != nil {
				return State{}, nil, err
			}
		case sqliteRuntimeKeyLockOwner:
			if err := json.Unmarshal(raw, &state.LockOwner); err != nil {
				return State{}, nil, err
			}
		case sqliteRuntimeKeyServiceControl:
			if err := json.Unmarshal(raw, &state.ServiceControl); err != nil {
				return State{}, nil, err
			}
		case sqliteRuntimeKeyUpgrade:
			if err := json.Unmarshal(raw, &state.Upgrade); err != nil {
				return State{}, nil, err
			}
		case sqliteRuntimeKeyAutoUpdate:
			if err := json.Unmarshal(raw, &state.AutoUpdate); err != nil {
				return State{}, nil, err
			}
		}
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

func overlaySQLiteRuntimeState(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, state *State) error {
	runtimeState, seen, err := loadSQLiteRuntimeState(ctx, q)
	if err != nil {
		return err
	}
	if !sqliteRuntimeStateUsable(seen) {
		return nil
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
	state.ensure(time.Time{})
	return nil
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
	if !sqliteRuntimeStateUsable(seen) || !seen[sqliteRuntimeKeyUpgrade] {
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
	if seen[sqliteRuntimeKeyServiceControl] && seen[sqliteRuntimeKeyUpgrade] && seen[sqliteRuntimeKeyAutoUpdate] {
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
		seedRuntime := !sqliteRuntimeStateUsable(seen)
		seedOptional := !seen[sqliteRuntimeKeyServiceControl] || !seen[sqliteRuntimeKeyUpgrade] || !seen[sqliteRuntimeKeyAutoUpdate]
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
			checkpoint, _, _, err := decodeSQLiteCheckpointRow(row, row.ID, sessionID, false)
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

func (s *Store) updateImportCheckpointSQLite(ctx context.Context, parentSessionID string, id string, fn func(ImportCheckpoint, bool, time.Time) (ImportCheckpoint, bool, error)) (ImportCheckpoint, bool, bool, error) {
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

func (s *Store) recordTranscriptCheckpointSQLite(ctx context.Context, parentSessionID string, checkpoint ImportCheckpoint, ledger TranscriptLedgerRecord) (bool, error) {
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
SELECT COALESCE(json_extract(value, '$.history_watch'), '{}'),
       json_extract(value, '$.history_watch_ready')
FROM state_meta WHERE key = 'state_json'`).Scan(&raw, &readyRaw); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return errors.New("sqlite teams store is missing state metadata")
				}
				return err
			}
			history = make(map[string]HistoryWatchCheckpoint)
			if len(raw) > 0 && string(raw) != "null" {
				if err := json.Unmarshal(raw, &history); err != nil {
					return err
				}
				if history == nil {
					history = make(map[string]HistoryWatchCheckpoint)
				}
			}
			if readyRaw.Valid && strings.TrimSpace(readyRaw.String) != "" {
				ready, err = time.Parse(time.RFC3339Nano, readyRaw.String)
				if err != nil {
					return fmt.Errorf("invalid history_watch_ready: %w", err)
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
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM sessions`, state.Sessions, func(v SessionContext) string { return v.ID }); err != nil {
			return err
		}
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns`, state.Turns, func(v Turn) string { return v.ID }); err != nil {
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
		if err := loadSQLiteJSONMap(ctx, db, query, out, func(v InboundEvent) string { return v.ID }, args...); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) claimControlLeaseSQLite(ctx context.Context, claim ControlLeaseClaim) (ControlLeaseDecision, bool, error) {
	var out ControlLeaseDecision
	handled, err := s.updateSQLiteRuntimeState(ctx, func(state *State) error {
		decision, err := claimControlLeaseInState(state, claim)
		out = decision
		return err
	})
	return out, handled, err
}

func (s *Store) validateControlLeaseSQLite(ctx context.Context, machineID string, generation int64, now time.Time) (ControlLease, bool, error) {
	var out ControlLease
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
		state, seen, err := loadSQLiteRuntimeState(ctx, db)
		if err != nil {
			return err
		}
		if !sqliteRuntimeStateUsable(seen) {
			state, err = loadSQLiteColdState(ctx, db)
			if err != nil {
				return err
			}
		}
		lease := state.ControlLease
		out = lease
		if lease.HolderMachineID != machineID || lease.Generation != generation || !lease.LeaseUntil.After(now) {
			return ErrControlLeaseNotHeld
		}
		return nil
	})
	return out, handled, err
}

func (s *Store) recordOwnerHeartbeatSQLite(ctx context.Context, owner OwnerMetadata, staleAfter time.Duration, now time.Time) (OwnerMetadata, bool, error) {
	var out OwnerMetadata
	handled, err := s.updateSQLiteRuntimeState(ctx, func(state *State) error {
		next, err := recordOwnerHeartbeatInState(state, owner, staleAfter, now)
		out = next
		return err
	})
	return out, handled, err
}

func (s *Store) readOwnerSQLite(ctx context.Context) (OwnerMetadata, bool, bool, error) {
	var out OwnerMetadata
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
		state, seen, err := loadSQLiteRuntimeState(ctx, db)
		if err != nil {
			return err
		}
		if !sqliteRuntimeStateUsable(seen) {
			state, err = loadSQLiteColdState(ctx, db)
			if err != nil {
				return err
			}
		}
		out, found = state.readOwner()
		return nil
	})
	return out, found, handled, err
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
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_messages(id, session_id, turn_id, teams_chat_id, teams_message_id, status, sequence, created_at, deliver_after, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET session_id = excluded.session_id, turn_id = excluded.turn_id, teams_chat_id = excluded.teams_chat_id, teams_message_id = excluded.teams_message_id, status = excluded.status, sequence = excluded.sequence, created_at = excluded.created_at, deliver_after = excluded.deliver_after, json = excluded.json`,
		v.ID, v.SessionID, v.TurnID, strings.TrimSpace(v.TeamsChatID), strings.TrimSpace(v.TeamsMessageID), string(v.Status), v.Sequence, sqliteTime(v.CreatedAt), int64(0), data)
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
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO chat_polls(chat_id, next_poll_at, poll_state, last_activity_at, park_notice_sent_at, parked_skip_eligible, updated_at, json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(chat_id) DO UPDATE SET next_poll_at = excluded.next_poll_at, poll_state = excluded.poll_state, last_activity_at = excluded.last_activity_at, park_notice_sent_at = excluded.park_notice_sent_at, parked_skip_eligible = excluded.parked_skip_eligible, updated_at = excluded.updated_at, json = excluded.json`,
		v.ChatID, sqliteTime(v.NextPollAt), v.PollState, sqliteTime(v.LastActivityAt), sqliteTime(v.ParkNoticeSentAt), sqliteBool(chatPollParkedSkipEligible(v)), sqliteTime(v.UpdatedAt), data)
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
	if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM transcript_deliveries WHERE outbox_id = ?`, []any{outboxID}, state.TranscriptDeliveries, func(v TranscriptDeliveryRecord) string { return v.ID }); err != nil {
		return err
	}
	if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM helper_deliveries WHERE outbox_id = ?`, []any{outboxID}, state.HelperDeliveries, func(v HelperDeliveryRecord) string { return v.ID }); err != nil {
		return err
	}
	if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM artifact_records WHERE outbox_id = ?`, []any{outboxID}, state.ArtifactRecords, func(v ArtifactRecord) string { return v.ID }); err != nil {
		return err
	}
	return nil
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
		record, ok, err := loadSQLiteJSONRow[ArtifactRecord](ctx, tx, `SELECT json FROM artifact_records WHERE id = ?`, id)
		if err != nil {
			return err
		}
		if ok {
			state.ArtifactRecords[record.ID] = record
		}
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

func (s *Store) recordTranscriptDeliverySQLite(ctx context.Context, parentSessionID string, delivery TranscriptDeliveryRecord, checkpoint ImportCheckpoint) (TranscriptDeliveryRecord, bool, bool, error) {
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
		if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM turns WHERE session_id = ? AND status IN (?, ?)`, []any{req.SessionID, string(TurnStatusQueued), string(TurnStatusRunning)}, state.Turns, func(v Turn) string { return v.ID }); err != nil {
			return err
		}
		if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM inbound_events WHERE session_id = ? AND status IN (?, ?, ?)`, []any{req.SessionID, string(InboundStatusPersisted), string(InboundStatusQueued), string(InboundStatusDeferred)}, state.InboundEvents, func(v InboundEvent) string { return v.ID }); err != nil {
			return err
		}
		if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM outbox_messages WHERE session_id = ? AND status IN (?, ?)`, []any{req.SessionID, string(OutboxStatusQueued), string(OutboxStatusSending)}, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
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
		if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, db, `SELECT json FROM sessions WHERE id = ?`, sessionID); err != nil {
			return State{}, err
		} else if ok {
			state.Sessions[session.ID] = session
		}
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns WHERE session_id = ?`, state.Turns, func(v Turn) string { return v.ID }, sessionID); err != nil {
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
	if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, db, `SELECT json FROM sessions WHERE id = ?`, sessionID); err != nil {
		return State{}, err
	} else if ok {
		state.Sessions[session.ID] = session
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns WHERE session_id = ?`, state.Turns, func(v Turn) string { return v.ID }, sessionID); err != nil {
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
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns WHERE session_id = ?`, state.Turns, func(v Turn) string { return v.ID }, sessionID); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM inbound_events WHERE session_id = ?`, state.InboundEvents, func(v InboundEvent) string { return v.ID }, sessionID); err != nil {
		return State{}, err
	}
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM outbox_messages WHERE session_id = ?`, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }, sessionID); err != nil {
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
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns WHERE session_id = ?`, state.Turns, func(v Turn) string { return v.ID }, sessionID); err != nil {
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
			return false, err
		} else if ok {
			if importCheckpointHasUnresolvedExecution(checkpoint) {
				return true, nil
			}
		}
	}
	rows, err := db.QueryContext(ctx, `SELECT json FROM turns WHERE session_id = ? AND status = ?`, sessionID, string(TurnStatusInterrupted))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return false, err
		}
		var meta struct {
			Status         TurnStatus `json:"status"`
			RecoveryReason string     `json:"recovery_reason"`
		}
		if err := json.Unmarshal(raw, &meta); err != nil {
			return false, err
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
			checkpoint, _, _, err := decodeSQLiteCheckpointRow(row, row.ID, sessionID, true)
			if err != nil {
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
			var meta struct {
				Status         TurnStatus `json:"status"`
				RecoveryReason string     `json:"recovery_reason"`
			}
			if err := json.Unmarshal(raw, &meta); err != nil {
				_ = rows.Close()
				return nil, err
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
	// Two arguments are reserved for the status predicates.
	for start := 0; start < len(sessionIDs); start += sqliteQueryParameterBatchSize - 2 {
		end := start + sqliteQueryParameterBatchSize - 2
		if end > len(sessionIDs) {
			end = len(sessionIDs)
		}
		batch := sessionIDs[start:end]
		placeholders := strings.TrimRight(strings.Repeat("?,", len(batch)), ",")
		args := make([]any, 0, len(batch)+2)
		args = append(args, string(TurnStatusRunning), string(TurnStatusInterrupted))
		for _, sessionID := range batch {
			args = append(args, sessionID)
		}
		rows, err := db.QueryContext(ctx, `SELECT session_id, json FROM turns WHERE status IN (?, ?) AND session_id IN (`+placeholders+`)`, args...)
		if err != nil {
			return LinkedTranscriptExecutionSnapshot{}, err
		}
		for rows.Next() {
			var sessionID string
			var raw []byte
			if err := rows.Scan(&sessionID, &raw); err != nil {
				_ = rows.Close()
				return LinkedTranscriptExecutionSnapshot{}, err
			}
			sessionID = strings.TrimSpace(sessionID)
			if _, wanted := requested[sessionID]; !wanted {
				continue
			}
			var meta struct {
				SessionID      string     `json:"session_id"`
				Status         TurnStatus `json:"status"`
				RecoveryReason string     `json:"recovery_reason"`
			}
			if err := json.Unmarshal(raw, &meta); err != nil {
				_ = rows.Close()
				return LinkedTranscriptExecutionSnapshot{}, err
			}
			if strings.TrimSpace(meta.SessionID) != "" && strings.TrimSpace(meta.SessionID) != sessionID {
				_ = rows.Close()
				return LinkedTranscriptExecutionSnapshot{}, fmt.Errorf("%w: turn row session %q differs from indexed session %q", ErrSessionStateProvenanceMismatch, meta.SessionID, sessionID)
			}
			switch meta.Status {
			case TurnStatusRunning:
				out.Running[sessionID] = true
			case TurnStatusInterrupted:
				if isLegacyUnresolvedTurn(Turn{Status: meta.Status, RecoveryReason: meta.RecoveryReason}) {
					out.Ownership[sessionID] = true
				}
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
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns WHERE session_id = ? AND status IN (?, ?)`, state.Turns, func(v Turn) string { return v.ID }, sessionID, string(TurnStatusQueued), string(TurnStatusRunning)); err != nil {
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
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT 1 FROM inbound_events WHERE teams_chat_id = ? AND teams_message_id = ? LIMIT 1`, chatID, teamsMessageID).Scan(&exists); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return MessageLookup{}, err
		}
	} else if exists == 1 {
		out.HasInbound = true
	}
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
	rows, err = db.QueryContext(ctx, `SELECT teams_chat_id, teams_message_id FROM inbound_events WHERE teams_message_id <> ''`)
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
			cache.Inbound[key] = true
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
			SessionID:  strings.TrimSpace(sessionID),
			Reason:     "malformed canonical transcript checkpoint",
			Provenance: ExecutionAnchorProvenanceLegacy,
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
	// subagent/publish-operation suffix is a different logical row and must not
	// be accepted as a fallback merely because it starts with transcript:.
	return sessionID, sessionID != "" && !strings.Contains(sessionID, ":")
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
				if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM outbox_messages WHERE turn_id = ?`, []any{turnID}, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
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
			if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM outbox_messages WHERE turn_id = ?`, []any{req.TurnID}, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
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
			if strings.TrimSpace(turn.SessionID) != strings.TrimSpace(req.SessionID) {
				return tx.Commit()
			}
			if err := markSQLiteLegacyUnresolvedSessionTx(ctx, tx, &state, req.SessionID); err != nil {
				return err
			}
			if err := loadSQLiteJSONMapTx(ctx, tx, `SELECT json FROM outbox_messages WHERE turn_id = ?`, []any{req.OuterTurnID}, state.OutboxMessages, func(v OutboxMessage) string { return v.ID }); err != nil {
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
			if nextMessageID != "" {
				id := messageProvenanceID(current.TeamsChatID, nextMessageID)
				if existing, ok, err := loadSQLiteJSONRow[MessageProvenanceRecord](ctx, db, `SELECT json FROM message_provenance WHERE id = ?`, id); err != nil {
					return err
				} else if ok && strings.TrimSpace(existing.Origin) == MessageOriginUserInbound {
					out = current
					return nil
				}
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			state := newState()
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
			now := time.Now()
			msg := current
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
		rows, err := q.QueryContext(ctx, `SELECT json FROM outbox_messages WHERE id IN (`+strings.Join(placeholders, ",")+`)`, args...)
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
			"(o.status <> ? OR o.teams_message_id <> '')",
			"NOT (o.status = ? AND o.teams_message_id <> '' AND COALESCE(json_extract(o.json, '$.blocked_by_source_rewrite'), 0) = 1)",
			"NOT (o.status = ? AND COALESCE(json_extract(o.json, '$.last_send_error'), '') LIKE 'ambiguous Graph send;%')",
		}
		args := []any{
			string(OutboxStatusQueued), string(OutboxStatusSending), string(OutboxStatusAccepted),
			string(OutboxStatusAccepted),
			string(OutboxStatusAccepted),
			string(OutboxStatusSending),
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
		stmt := `SELECT o.json, o.created_at, o.id, COALESCE(r.blocked_until, 0)
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
			var blockedUntilNanos int64
			if err := rows.Scan(&raw, &createdAtNanos, &rowID, &blockedUntilNanos); err != nil {
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
				return err
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
		rows, err := db.QueryContext(ctx, `SELECT json FROM outbox_messages
WHERE teams_chat_id = ?
  AND status = ?
ORDER BY created_at, id`, chatID, string(OutboxStatusSent))
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
			status OutboxStatus
			order  string
		}{
			{status: OutboxStatusSending, order: "created_at ASC, id ASC"},
			{status: OutboxStatusAccepted, order: "created_at DESC, id DESC"},
			{status: OutboxStatusSent, order: "created_at DESC, id DESC"},
		} {
			rows, err := db.QueryContext(ctx, `SELECT json FROM outbox_messages
WHERE status = ? AND teams_chat_id = ?
ORDER BY `+item.order+`
LIMIT ?`, string(item.status), query.TeamsChatID, query.LimitPerStatus)
			if err != nil {
				return err
			}
			for rows.Next() {
				var raw []byte
				if err := rows.Scan(&raw); err != nil {
					_ = rows.Close()
					return err
				}
				var msg OutboxMessage
				if err := json.Unmarshal(raw, &msg); err != nil {
					_ = rows.Close()
					return err
				}
				out = append(out, msg)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return err
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
		out, found, err = loadSQLiteJSONRow[OutboxMessage](ctx, db, `SELECT json FROM outbox_messages
WHERE teams_chat_id = ?
  AND id <> ?
  AND sequence > 0
	AND sequence < ?
	AND status NOT IN (?, ?)
	AND NOT (status = ? AND teams_message_id <> '' AND COALESCE(json_extract(json, '$.blocked_by_source_rewrite'), 0) = 1)
	AND NOT (status = ? AND COALESCE(json_extract(json, '$.last_send_error'), '') LIKE 'ambiguous Graph send;%')
ORDER BY sequence, created_at, id
LIMIT 1`, chatID, strings.TrimSpace(msg.ID), msg.Sequence, string(OutboxStatusSent), string(OutboxStatusSkipped), string(OutboxStatusAccepted), string(OutboxStatusSending))
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
		rows, err := db.QueryContext(ctx, `SELECT json FROM outbox_messages
WHERE teams_chat_id = ?
  AND id <> ?
  AND sequence > 0
	AND sequence < ?
	AND status NOT IN (?, ?)
	AND NOT (status = ? AND teams_message_id <> '' AND COALESCE(json_extract(json, '$.blocked_by_source_rewrite'), 0) = 1)
	AND NOT (status = ? AND COALESCE(json_extract(json, '$.last_send_error'), '') LIKE 'ambiguous Graph send;%')
	ORDER BY sequence, created_at, id`, chatID, strings.TrimSpace(msg.ID), msg.Sequence, string(OutboxStatusSent), string(OutboxStatusSkipped), string(OutboxStatusAccepted), string(OutboxStatusSending))
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
		handled = true
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
			out[poll.ChatID] = poll
			changed = changed || updateChanged
		}
		handled = true
		if !changed {
			return nil
		}
		for _, poll := range out {
			if err := upsertSQLiteChatPollTx(ctx, tx, poll); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return out, handled, err
}

func (s *Store) boostChatPollAfterFinalAnswerSQLite(ctx context.Context, req FinalAnswerPollBoostRequest) (ChatPollState, bool, bool, error) {
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
		_, err = db.ExecContext(ctx, `DELETE FROM chat_rate_limits WHERE chat_id = ?`, chatID)
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
