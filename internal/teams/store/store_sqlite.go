package store

import (
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
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const (
	storeSQLiteBackend              = "store-sqlite"
	storeSQLiteVersion              = 1
	storeSQLiteFileName             = "store.sqlite"
	storeSQLitePointerSchemaVersion = SchemaVersion + 1
	sqliteImportCheckpointImporting = "importing"

	DefaultSQLiteStateMigrationMinSize int64 = 1 << 20
)

const (
	sqliteMigrationStageAfterBackup       = "after-backup"
	sqliteMigrationStageAfterTempVerified = "after-temp-verified"
	sqliteMigrationStageAfterDBReplace    = "after-db-replace"
)

// sqliteMigrationTestHook is nil in production; tests use it to inject failures
// at durable migration boundaries.
var sqliteMigrationTestHook func(stage string) error

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
			state, err := s.loadSQLiteStateUnlocked(pointer)
			if err != nil {
				return err
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
		state, err := s.loadUnlocked()
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
		if !stateLogicalEqual(state, got) {
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
	if pointer.SchemaVersion != storeSQLitePointerSchemaVersion {
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
	return pointer, true, nil
}

func (s *Store) clearSQLitePointerCacheUnlocked() {
	s.sqlitePointer = storeSQLitePointer{}
	s.sqlitePointerCached = false
	s.sqlitePointerTrusted = false
	s.sqlitePointerSize = 0
	s.sqlitePointerMod = time.Time{}
	s.sqlitePointerChange = 0
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

func (s *Store) loadSQLiteStateUnlocked(pointer storeSQLitePointer) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	return loadSQLiteState(context.Background(), db)
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

func (s *Store) loadSQLiteSelectedStateFieldsUnlocked(pointer storeSQLitePointer, wanted map[string]struct{}) (State, error) {
	db, err := s.sqliteDBUnlocked(pointer)
	if err != nil {
		return State{}, err
	}
	return loadSQLiteSelectedState(context.Background(), db, wanted)
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
		selected, err := loadSQLiteSelectedStateWithChatPollQuery(ctx, db, hotPollScheduleBaseFields,
			`SELECT json FROM chat_polls WHERE COALESCE(parked_skip_eligible, 0) = 0`,
		)
		if err != nil {
			return err
		}
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM turns WHERE status IN (?, ?)`, selected.Turns, func(v Turn) string { return v.ID }, string(TurnStatusQueued), string(TurnStatusRunning)); err != nil {
			return err
		}
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM import_checkpoints WHERE status = ?`, selected.ImportCheckpoints, func(v ImportCheckpoint) string { return v.ID }, sqliteImportCheckpointImporting); err != nil {
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

func loadSQLiteStateFile(path string) (State, error) {
	db, err := openExistingSQLiteStore(path)
	if err != nil {
		return State{}, err
	}
	defer db.Close()
	return loadSQLiteState(context.Background(), db)
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
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("sqlite store %q does not exist", path)
		}
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("sqlite store %q is a directory", path)
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
		`PRAGMA synchronous = FULL`,
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
	for _, table := range sqliteRequiredTables {
		var name string
		err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
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
	return cold
}

func writeSQLiteState(ctx context.Context, db *sql.DB, state State) error {
	state.ensure(time.Time{})
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range []string{"state_meta", "runtime_state", "sessions", "inbound_events", "turns", "outbox_messages", "message_provenance", "chat_polls", "chat_sequences", "chat_rate_limits", "import_checkpoints", "transcript_ledger", "transcript_deliveries", "helper_deliveries", "artifact_records", "notifications"} {
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
	if err := writeSQLiteMap(ctx, tx, `INSERT INTO import_checkpoints(id, session_id, status, updated_at, json) VALUES (?, ?, ?, ?, ?)`, state.ImportCheckpoints, func(v ImportCheckpoint) []any {
		return []any{v.ID, v.SessionID, v.Status, sqliteTime(v.UpdatedAt)}
	}); err != nil {
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
	return tx.Commit()
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
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM import_checkpoints`, state.ImportCheckpoints, func(v ImportCheckpoint) string { return v.ID }); err != nil {
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
		if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM import_checkpoints`, state.ImportCheckpoints, func(v ImportCheckpoint) string { return v.ID }); err != nil {
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
	args := []any{string(SessionStatusActive), controlChatID, controlChatID}
	excludeIdle := ""
	if !idleBefore.IsZero() {
		excludeIdle = `  AND NOT (
    COALESCE(p.last_activity_at, 0) > 0
    AND COALESCE(p.last_activity_at, 0) <= ?
    AND COALESCE(s.updated_at, 0) <= ?
    AND COALESCE(p.parked_skip_eligible, 0) = 0
    AND p.poll_state IN (?, ?)
    AND NOT EXISTS (
      SELECT 1 FROM turns t
      WHERE t.session_id = s.id
        AND t.status IN (?, ?)
    )
  )
`
		idleBeforeUnix := sqliteTime(idleBefore)
		args = append(args, idleBeforeUnix, idleBeforeUnix, chatPollStateCold, chatPollStateParked, string(TurnStatusQueued), string(TurnStatusRunning))
	}
	rows, err := db.QueryContext(ctx, `SELECT s.json
FROM sessions s
LEFT JOIN chat_polls p ON p.chat_id = s.teams_chat_id
WHERE (s.status IS NULL OR s.status = '' OR s.status = ?)
  AND COALESCE(s.teams_chat_id, '') != ''
  AND (? = '' OR s.teams_chat_id != ?)
  AND (p.chat_id IS NULL OR COALESCE(p.parked_skip_eligible, 0) = 0)
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
	state.ChatSequences = map[string]ChatSequenceState{}
	if includeChatSequences {
		if err := loadSQLiteJSONMap(ctx, q, `SELECT json FROM chat_sequences`, state.ChatSequences, func(v ChatSequenceState) string { return v.ChatID }); err != nil {
			return State{}, err
		}
	}
	return state, nil
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
	return err
}

const (
	sqliteRuntimeKeyScope           = "scope"
	sqliteRuntimeKeyMachineIdentity = "machine_identity"
	sqliteRuntimeKeyMachines        = "machines"
	sqliteRuntimeKeyControlLease    = "control_lease"
	sqliteRuntimeKeyServiceOwner    = "service_owner"
	sqliteRuntimeKeyLockOwner       = "lock_owner"
)

var sqliteRuntimeRequiredKeys = []string{
	sqliteRuntimeKeyScope,
	sqliteRuntimeKeyMachineIdentity,
	sqliteRuntimeKeyMachines,
	sqliteRuntimeKeyControlLease,
	sqliteRuntimeKeyServiceOwner,
	sqliteRuntimeKeyLockOwner,
}

func saveSQLiteRuntimeStateTx(ctx context.Context, tx *sql.Tx, state State) error {
	values := map[string]any{
		sqliteRuntimeKeyScope:           state.Scope,
		sqliteRuntimeKeyMachineIdentity: state.MachineIdentity,
		sqliteRuntimeKeyMachines:        state.Machines,
		sqliteRuntimeKeyControlLease:    state.ControlLease,
		sqliteRuntimeKeyServiceOwner:    state.ServiceOwner,
		sqliteRuntimeKeyLockOwner:       state.LockOwner,
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO runtime_state(key, json) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET json = excluded.json`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, key := range sqliteRuntimeRequiredKeys {
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
	state.ensure(time.Time{})
	return nil
}

func (s *Store) updateSQLiteRuntimeState(ctx context.Context, fn func(*State) error) (bool, error) {
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
		if seedRuntime {
			state, err = loadSQLiteColdState(ctx, tx)
			if err != nil {
				return err
			}
		}
		if err := fn(&state); err != nil {
			if errors.Is(err, errStoreNoChange) && seedRuntime {
				state.ensure(time.Now())
				if saveErr := saveSQLiteRuntimeStateTx(ctx, tx, state); saveErr != nil {
					return saveErr
				}
				return tx.Commit()
			}
			return err
		}
		state.ensure(time.Now())
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
		out, found, err = loadSQLiteJSONRow[ImportCheckpoint](ctx, db, `SELECT json FROM import_checkpoints WHERE id = ?`, id)
		return err
	})
	return out, found, handled, err
}

func (s *Store) updateImportCheckpointSQLite(ctx context.Context, id string, fn func(ImportCheckpoint, bool, time.Time) (ImportCheckpoint, bool, error)) (ImportCheckpoint, bool, bool, error) {
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
		current, found, err := loadSQLiteJSONRow[ImportCheckpoint](ctx, tx, `SELECT json FROM import_checkpoints WHERE id = ?`, id)
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
		if err := upsertSQLiteImportCheckpointTx(ctx, tx, next); err != nil {
			return err
		}
		out = next
		changed = true
		return tx.Commit()
	})
	return out, changed, handled, err
}

func (s *Store) updateSQLiteColdState(ctx context.Context, fn func(*State) error) (bool, error) {
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
		if err := fn(&state); err != nil {
			return err
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
			now := time.Now()
			if turn.Status == "" {
				turn.Status = TurnStatusQueued
			}
			if turn.ModelProfile.IsZero() {
				turn.ModelProfile = session.ModelProfile
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
	if err := loadSQLiteJSONMap(ctx, db, `SELECT json FROM import_checkpoints WHERE session_id = ?`, state.ImportCheckpoints, func(v ImportCheckpoint) string { return v.ID }, sessionID); err != nil {
		return State{}, err
	}
	if checkpointID != "" {
		if checkpoint, ok, err := loadSQLiteJSONRow[ImportCheckpoint](ctx, db, `SELECT json FROM import_checkpoints WHERE id = ?`, checkpointID); err != nil {
			return State{}, err
		} else if ok {
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

func (s *Store) loadSQLiteSessionActiveTurnQueueStateUnlocked(pointer storeSQLitePointer, sessionID string) (State, error) {
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
			var running int
			if err := tx.QueryRowContext(ctx, `SELECT 1 FROM turns WHERE session_id = ? AND status = ? LIMIT 1`, sessionID, string(TurnStatusRunning)).Scan(&running); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			handled = true
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
			state := State{SchemaVersion: SchemaVersion, Sessions: map[string]SessionContext{sessionID: {}}, Turns: map[string]Turn{turn.ID: turn}}
			if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, sessionID); err != nil {
				return err
			} else if ok {
				state.Sessions[sessionID] = session
			}
			now := time.Now()
			turn.Status = TurnStatusRunning
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
			state, err := loadSQLiteColdStateWithoutChatSequences(ctx, tx)
			if err != nil {
				return err
			}
			state.Sessions = map[string]SessionContext{}
			state.Turns = map[string]Turn{turnID: current}
			state.InboundEvents = map[string]InboundEvent{}
			state.OutboxMessages = map[string]OutboxMessage{}
			if current.SessionID != "" {
				if session, ok, err := loadSQLiteJSONRow[SessionContext](ctx, tx, `SELECT json FROM sessions WHERE id = ?`, current.SessionID); err != nil {
					return err
				} else if ok {
					state.Sessions[current.SessionID] = session
				}
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
			}
			now := time.Now()
			next, err := fn(&state, current, now)
			if err != nil {
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
			}
			if loadCold || loadLinked {
				if err := loadSQLiteOutboxLinkedRecordsTx(ctx, tx, &state, outboxID); err != nil {
					return err
				}
			}
			now := time.Now()
			next, err := fn(&state, current, now)
			if err != nil {
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

func (s *Store) markOutboxDeliveredSQLite(ctx context.Context, outboxID string, teamsMessageID string, sent bool) (OutboxMessage, bool, error) {
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
			now := time.Now()
			msg := current
			if sent {
				msg.Status = OutboxStatusSent
				if msg.SentAt.IsZero() {
					msg.SentAt = now
				}
			} else if msg.Status != OutboxStatusSent {
				msg.Status = OutboxStatusAccepted
			}
			if teamsMessageID != "" {
				msg.TeamsMessageID = teamsMessageID
			}
			msg.LastSendError = ""
			if msg.Status == OutboxStatusSent {
				recordOutboxProvenanceLocked(&state, msg, now)
				markTranscriptDeliveryForOutboxLocked(&state, msg, TranscriptDeliveryStatusSent, now)
				updateHelperDeliveryForOutboxLocked(&state, msg, HelperDeliveryStatusSent, now)
				updateArtifactRecordsForOutboxLocked(&state, msg, now, "uploaded", "", "")
			} else {
				recordOutboxProvenanceLocked(&state, msg, now)
				markTranscriptDeliveryForOutboxLocked(&state, msg, TranscriptDeliveryStatusAccepted, now)
				updateHelperDeliveryForOutboxLocked(&state, msg, HelperDeliveryStatusAccepted, now)
			}
			msg.UpdatedAt = now
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
			"(o.status = ? OR COALESCE(r.blocked_until, 0) = 0 OR COALESCE(r.blocked_until, 0) <= ?)",
		}
		args := []any{
			string(OutboxStatusQueued), string(OutboxStatusSending), string(OutboxStatusAccepted),
			string(OutboxStatusAccepted),
			string(OutboxStatusAccepted), sqliteTime(query.Now),
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
ORDER BY sequence, created_at, id
LIMIT 1`, chatID, strings.TrimSpace(msg.ID), msg.Sequence, string(OutboxStatusSent), string(OutboxStatusSkipped))
		if err != nil {
			return err
		}
		handled = true
		return nil
	})
	return out, found, handled, err
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
