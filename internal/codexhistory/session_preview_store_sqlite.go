package codexhistory

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlite "modernc.org/sqlite"
)

// The source session JSONL remains authoritative. This database stores only
// rebuildable preview acceleration: normalized visible messages plus the small
// amount of dedupe state that cannot be recovered from those messages.

const (
	sessionPreviewSQLiteSchemaVersion          = cacheVersion
	sessionPreviewSQLiteApplicationID          = 0x43585050 // "CXPP"
	sessionPreviewSQLiteMaxSessions            = 1024
	sessionPreviewSQLiteMaxBytes         int64 = 128 * 1024 * 1024
	sessionPreviewSQLiteBusyMillis             = 100
	sessionPreviewSQLiteMemoryMaxEntries       = 64
	sessionPreviewSQLiteMemoryMaxBytes   int64 = 16 * 1024 * 1024
)

var (
	errSessionPreviewSQLiteConflict = errors.New("session preview sqlite write conflict")
	errSessionPreviewSQLiteSchema   = errors.New("invalid session preview sqlite schema")

	sessionPreviewAppendFlushBytes int64            = 64 * 1024
	sessionPreviewAppendFlushDelay                  = 30 * time.Second
	sessionPreviewNow              func() time.Time = time.Now
	sessionPreviewSQLiteCommitHook func(string)
)

type sessionPreviewSQLiteStore struct {
	path string
	db   *sql.DB
}

type sessionPreviewSQLiteEntry struct {
	messagesLoaded   bool
	cachedBytes      int64
	committedOffset  int64
	committedOrdinal int64
	pendingMessages  []Message
	pendingDelta     sessionPreviewSeenDelta
	dirtySince       time.Time

	sessionID   int64
	generation  int64
	nextOrdinal int64

	fileKey        fileCacheKey
	filterVersion  string
	offset         int64
	prefixTailHash string
	prefixTailSize int64
	messages       []Message
	text           string
}

type sessionPreviewSeenDelta struct {
	sourceKeys []string
	fallback   []persistentSessionPreviewFallbackMessage
}

type sessionPreviewSQLiteMemoryEntry struct {
	fileKey     fileCacheKey
	messages    []Message
	text        string
	hasMessages bool
	parsed      sessionPreviewSQLiteEntry
	hasParsed   bool
	bytes       int64
	lastAccess  uint64
}

var sessionPreviewSQLiteState struct {
	openMu      sync.Mutex
	mu          sync.Mutex
	path        string
	db          *sql.DB
	dbInfo      os.FileInfo
	memory      map[string]sessionPreviewSQLiteMemoryEntry
	memoryBytes int64
	memoryTick  uint64
}

// Fixed lock stripes bound synchronization state. File I/O never holds the
// global cache-map lock; concurrent readers of the same source serialize.
var sessionPreviewReadLocks [64]sync.Mutex

func readSessionPreviewCacheValueSQLite(filePath string, wantMessages bool) ([]Message, string, error) {
	var stripe uint64
	for _, c := range filePath {
		stripe = stripe*33 + uint64(c)
	}
	lock := &sessionPreviewReadLocks[stripe%uint64(len(sessionPreviewReadLocks))]
	lock.Lock()
	defer lock.Unlock()
	f, err := os.Open(filePath)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, "", err
	}
	completeOffset, complete := sessionPreviewCompleteOffsetFrom(f, info)
	if completeOffset == 0 && info.Size() > 0 {
		return readSessionPreviewUncached(filePath)
	}
	if !complete {
		return nil, "", errors.New("cannot locate complete session boundary")
	}
	store, err := currentSessionPreviewSQLiteStoreForSource(filePath)
	if err != nil {
		return nil, "", err
	}
	if messages, text, ok := loadSessionPreviewSQLiteMemory(store.path, filePath, info, wantMessages); ok {
		return messages, text, nil
	}
	key := newFileCacheKey(filePath, info)
	for attempt := 0; attempt < 3; attempt++ {
		entry, ok := loadSessionPreviewSQLiteAppendBase(store.path, filePath)
		memoryBase := ok
		if !ok {
			entry, ok, err = store.loadHeader(filePath)
			if err != nil {
				return nil, "", err
			}
		}
		valid := ok && entry.filterVersion == sessionPreviewFilterVersion
		exact := valid && matchesFileInfo(filePath, info, entry.fileKey)
		appendable := valid && !exact && completeOffset >= entry.offset && canAppendCacheFileFrom(f, filePath, info, entry.fileKey, entry.offset, entry.prefixTailHash, entry.prefixTailSize)
		if exact || appendable {
			if memoryBase && wantMessages && !entry.messagesLoaded {
				if err = store.promoteMessages(filePath, &entry); errors.Is(err, errSessionPreviewSQLiteConflict) {
					continue
				} else if err != nil {
					return nil, "", err
				}
			}
			if !memoryBase {
				if err = store.loadMessages(&entry, wantMessages); errors.Is(err, errSessionPreviewSQLiteConflict) {
					continue
				} else if err != nil {
					return nil, "", err
				}
				if wantMessages {
					entry.text = FormatPreviewMessages(entry.messages, 0)
				}
			}
			if appendable {
				tail, delta, readErr := store.readAppendCandidates(f, entry, entry.offset, completeOffset-entry.offset)
				if errors.Is(readErr, errSessionPreviewSQLiteConflict) {
					continue
				}
				if readErr != nil {
					invalidateSessionPreviewSQLiteMemory(store.path, filePath)
					return nil, "", readErr
				}
				if entry.messagesLoaded {
					entry.messages = append(entry.messages, tail...)
				}
				entry.text = appendPreviewText(entry.text, FormatPreviewMessages(tail, 0))
				entry.pendingMessages = append(entry.pendingMessages, tail...)
				entry.pendingDelta.sourceKeys = append(entry.pendingDelta.sourceKeys, delta.sourceKeys...)
				entry.pendingDelta.fallback = append(entry.pendingDelta.fallback, delta.fallback...)
				if entry.dirtySince.IsZero() {
					entry.dirtySince = sessionPreviewNow()
				}
				entry.offset = completeOffset
				hash, size, hashOK := sessionPreviewPrefixTailHashFrom(f, completeOffset)
				if !hashOK {
					invalidateSessionPreviewSQLiteMemory(store.path, filePath)
					return nil, "", errors.New("cannot verify session prefix")
				}
				entry.prefixTailHash, entry.prefixTailSize = hash, size
				if err := unchangedJSONLSource(f, filePath, info); err != nil {
					return nil, "", err
				}
				if shouldFlushSessionPreviewEntry(entry) {
					expected := entry
					expected.offset, expected.nextOrdinal = entry.committedOffset, entry.committedOrdinal
					err = store.append(expected, key, completeOffset, hash, size, entry.pendingMessages, entry.pendingDelta)
					if errors.Is(err, errSessionPreviewSQLiteConflict) {
						invalidateSessionPreviewSQLiteMemory(store.path, filePath)
						continue
					}
					if err != nil {
						invalidateSessionPreviewSQLiteMemory(store.path, filePath)
						return nil, "", err
					}
					entry.committedOffset = completeOffset
					entry.committedOrdinal += int64(len(entry.pendingMessages))
					entry.nextOrdinal = entry.committedOrdinal
					entry.pendingMessages = nil
					entry.pendingDelta = sessionPreviewSeenDelta{}
					entry.dirtySince = time.Time{}
				}
			}
		} else {
			seen := newMessageSeenState()
			messages, readErr := readSessionMessagesWindowFrom(f, 0, completeOffset, 0, projectPreviewMessage, seen)
			if readErr != nil {
				return nil, "", readErr
			}
			hash, size, hashOK := sessionPreviewPrefixTailHashFrom(f, completeOffset)
			if !hashOK {
				return nil, "", errors.New("cannot verify session prefix")
			}
			if err := unchangedJSONLSource(f, filePath, info); err != nil {
				return nil, "", err
			}
			nextGeneration := int64(1)
			if ok {
				nextGeneration = entry.generation + 1
			}
			err = store.replace(entry, ok, filePath, key, completeOffset, hash, size, messages, seen)
			if errors.Is(err, errSessionPreviewSQLiteConflict) {
				invalidateSessionPreviewSQLiteMemory(store.path, filePath)
				continue
			}
			if err != nil {
				return nil, "", err
			}
			entry, ok, err = store.loadHeader(filePath)
			if err != nil {
				return nil, "", err
			}
			if !ok {
				// Quota eviction must still expose a valid newline-less tail.
				overlay := newMessageSeenState()
				overlay.parent = seen
				tail, err := readSessionMessagesWindowFrom(f, completeOffset, info.Size()-completeOffset, 0, projectPreviewMessage, overlay)
				if err != nil {
					return nil, "", err
				}
				return sessionPreviewResult(append(messages, tail...), "", wantMessages)
			}
			if entry.generation != nextGeneration || entry.offset != completeOffset || entry.fileKey != key {
				continue
			}
			entry.text = FormatPreviewMessages(messages, 0)
			if wantMessages {
				entry.messages = messages
				entry.messagesLoaded = true
			}
		}
		// The cached key describes the committed complete-line prefix. A final
		// newline-less record is projected through an overlay and never advances
		// the durable cursor or mutates the canonical dedupe state.
		entry.fileKey = key
		if completeOffset < info.Size() {
			provisional, _, readErr := store.readAppendCandidates(f, entry, completeOffset, info.Size()-completeOffset)
			if errors.Is(readErr, errSessionPreviewSQLiteConflict) {
				continue
			}
			if readErr != nil {
				return nil, "", readErr
			}
			// Keep complete state in memory, but never treat the provisional result
			// as an exact hit on the next read.
			invalidateSessionPreviewSQLiteMemory(store.path, filePath)
			entry.fileKey.Size = completeOffset
			storeSessionPreviewSQLiteMemory(store.path, filePath, entry.fileKey, entry.messages, entry.text, entry.messagesLoaded, &entry)
			messages := append(append([]Message(nil), entry.messages...), provisional...)
			return sessionPreviewResult(messages, appendPreviewText(entry.text, FormatPreviewMessages(provisional, 0)), wantMessages)
		}
		return rememberSessionPreviewSQLiteResult(store.path, filePath, key, entry.messages, entry.text, wantMessages, &entry)
	}
	return nil, "", errSessionPreviewSQLiteConflict
}

func shouldFlushSessionPreviewEntry(entry sessionPreviewSQLiteEntry) bool {
	delta := entry.offset - entry.committedOffset
	return delta > 0 && (sessionPreviewAppendFlushBytes <= 0 || delta >= sessionPreviewAppendFlushBytes || sessionPreviewAppendFlushDelay <= 0 || sessionPreviewNow().Sub(entry.dirtySince) >= sessionPreviewAppendFlushDelay)
}

func rememberSessionPreviewSQLiteResult(
	storePath string,
	filePath string,
	key fileCacheKey,
	messages []Message,
	text string,
	wantMessages bool,
	parsed *sessionPreviewSQLiteEntry,
) ([]Message, string, error) {
	resultMessages, resultText, err := sessionPreviewResult(messages, text, wantMessages)
	if err == nil {
		hasMessages := wantMessages
		if parsed != nil {
			hasMessages = parsed.messagesLoaded
		}
		storeSessionPreviewSQLiteMemory(storePath, filePath, key, messages, resultText, hasMessages, parsed)
	}
	return resultMessages, resultText, err
}

func sessionPreviewResult(messages []Message, text string, wantMessages bool) ([]Message, string, error) {
	if text == "" && len(messages) > 0 {
		text = FormatPreviewMessages(messages, 0)
	}
	if wantMessages {
		return messages, text, nil
	}
	return nil, text, nil
}

func sessionPreviewSQLiteMemoryKey(storePath string, filePath string) string {
	return filepath.Clean(storePath) + "\x00" + filepath.Clean(filePath)
}

func loadSessionPreviewSQLiteMemory(storePath string, filePath string, info os.FileInfo, wantMessages bool) ([]Message, string, bool) {
	key := sessionPreviewSQLiteMemoryKey(storePath, filePath)
	sessionPreviewSQLiteState.mu.Lock()
	defer sessionPreviewSQLiteState.mu.Unlock()
	entry, ok := sessionPreviewSQLiteState.memory[key]
	if !ok || (wantMessages && !entry.hasMessages) || !matchesFileInfo(filePath, info, entry.fileKey) {
		return nil, "", false
	}
	sessionPreviewSQLiteState.memoryTick++
	entry.lastAccess = sessionPreviewSQLiteState.memoryTick
	sessionPreviewSQLiteState.memory[key] = entry
	if wantMessages {
		return append([]Message(nil), entry.messages...), entry.text, true
	}
	return nil, entry.text, true
}

func loadSessionPreviewSQLiteAppendBase(storePath string, filePath string) (sessionPreviewSQLiteEntry, bool) {
	key := sessionPreviewSQLiteMemoryKey(storePath, filePath)
	sessionPreviewSQLiteState.mu.Lock()
	defer sessionPreviewSQLiteState.mu.Unlock()
	memory, ok := sessionPreviewSQLiteState.memory[key]
	if !ok || !memory.hasParsed {
		return sessionPreviewSQLiteEntry{}, false
	}
	base := memory.parsed
	base.messagesLoaded = memory.hasMessages
	// Transfer ownership before mutation. Failed parses, conflicts, or an
	// oversized result must not leave a partially mutated seen map cached.
	delete(sessionPreviewSQLiteState.memory, key)
	sessionPreviewSQLiteState.memoryBytes -= memory.bytes
	return base, true
}

func storeSessionPreviewSQLiteMemory(
	storePath string,
	filePath string,
	fileKey fileCacheKey,
	messages []Message,
	text string,
	hasMessages bool,
	parsed *sessionPreviewSQLiteEntry,
) {
	entry := sessionPreviewSQLiteMemoryEntry{
		fileKey:     fileKey,
		text:        text,
		hasMessages: hasMessages,
		bytes:       int64(len(text)) + 512,
	}
	if hasMessages {
		entry.messages = append([]Message(nil), messages...)
		for _, message := range entry.messages {
			entry.bytes += int64(len(message.Role) + len(message.Content) + len(message.sourceID) + 96)
		}
	}
	if parsed != nil {
		entry.parsed = *parsed
		entry.parsed.messages = entry.messages
		entry.parsed.messagesLoaded = hasMessages
		entry.hasParsed = true
		for _, m := range entry.parsed.pendingMessages {
			entry.bytes += int64(len(m.Content) + len(m.sourceID) + 96)
		}
		for _, key := range entry.parsed.pendingDelta.sourceKeys {
			entry.bytes += int64(len(key) + 16)
		}
		for _, m := range entry.parsed.pendingDelta.fallback {
			entry.bytes += int64(len(m.Key) + len(m.SourceKind) + 64)
		}
	}
	if entry.bytes > sessionPreviewSQLiteMemoryMaxBytes {
		return
	}

	key := sessionPreviewSQLiteMemoryKey(storePath, filePath)
	sessionPreviewSQLiteState.mu.Lock()
	defer sessionPreviewSQLiteState.mu.Unlock()
	if sessionPreviewSQLiteState.memory == nil {
		sessionPreviewSQLiteState.memory = make(map[string]sessionPreviewSQLiteMemoryEntry)
	}
	if previous, ok := sessionPreviewSQLiteState.memory[key]; ok {
		sessionPreviewSQLiteState.memoryBytes -= previous.bytes
	}
	sessionPreviewSQLiteState.memoryTick++
	entry.lastAccess = sessionPreviewSQLiteState.memoryTick
	sessionPreviewSQLiteState.memory[key] = entry
	sessionPreviewSQLiteState.memoryBytes += entry.bytes
	pruneSessionPreviewSQLiteMemoryLocked()
}

func invalidateSessionPreviewSQLiteMemory(storePath string, filePath string) {
	key := sessionPreviewSQLiteMemoryKey(storePath, filePath)
	sessionPreviewSQLiteState.mu.Lock()
	defer sessionPreviewSQLiteState.mu.Unlock()
	if entry, ok := sessionPreviewSQLiteState.memory[key]; ok {
		delete(sessionPreviewSQLiteState.memory, key)
		sessionPreviewSQLiteState.memoryBytes -= entry.bytes
	}
}

func pruneSessionPreviewSQLiteMemoryLocked() {
	for len(sessionPreviewSQLiteState.memory) > sessionPreviewSQLiteMemoryMaxEntries ||
		sessionPreviewSQLiteState.memoryBytes > sessionPreviewSQLiteMemoryMaxBytes {
		oldestKey := ""
		var oldestAccess uint64
		for key, entry := range sessionPreviewSQLiteState.memory {
			if oldestKey == "" || entry.lastAccess < oldestAccess {
				oldestKey = key
				oldestAccess = entry.lastAccess
			}
		}
		if oldestKey == "" {
			break
		}
		entry := sessionPreviewSQLiteState.memory[oldestKey]
		delete(sessionPreviewSQLiteState.memory, oldestKey)
		sessionPreviewSQLiteState.memoryBytes -= entry.bytes
	}
}

func currentSessionPreviewSQLiteStore() (*sessionPreviewSQLiteStore, error) {
	path, err := sessionPreviewSQLiteFile()
	if err != nil {
		return nil, err
	}
	store, err := currentSessionPreviewSQLiteStoreAtPath(path)
	if err == nil {
		if root, rootErr := cacheV2RootForSource(""); rootErr == nil {
			cleanupLegacyCaches(root)
		}
	}
	return store, err
}

func currentSessionPreviewSQLiteStoreForSource(sourcePath string) (*sessionPreviewSQLiteStore, error) {
	path, err := sessionPreviewSQLiteFileForSource(sourcePath)
	if err != nil {
		return nil, err
	}
	store, err := currentSessionPreviewSQLiteStoreAtPath(path)
	if err == nil {
		if root, rootErr := cacheV2RootForSource(sourcePath); rootErr == nil {
			cleanupLegacyCaches(root)
		}
	}
	return store, err
}

func currentSessionPreviewSQLiteStoreAtPath(path string) (*sessionPreviewSQLiteStore, error) {
	sessionPreviewSQLiteState.openMu.Lock()
	defer sessionPreviewSQLiteState.openMu.Unlock()
	// A peer can quarantine and rebuild a derived database while this process
	// still holds its old SQLite handle. Reopen on identity change before
	// reusing any parsed progress; mtime/size changes are ordinary DB writes.
	info, statErr := os.Stat(path)
	sessionPreviewSQLiteState.mu.Lock()
	if statErr == nil && sessionPreviewSQLiteState.db != nil && sessionPreviewSQLiteState.path == path && sessionPreviewSQLiteState.dbInfo != nil && os.SameFile(info, sessionPreviewSQLiteState.dbInfo) {
		defer sessionPreviewSQLiteState.mu.Unlock()
		return &sessionPreviewSQLiteStore{path: path, db: sessionPreviewSQLiteState.db}, nil
	}
	previous := sessionPreviewSQLiteState.db
	// Append bases belong to this open database identity. Also discard them
	// when switching roots: a previously closed database may have been rebuilt
	// while another root was selected. Pending tails are replayed from JSONL.
	sessionPreviewSQLiteState.memory = nil
	sessionPreviewSQLiteState.memoryBytes = 0
	sessionPreviewSQLiteState.db = nil
	sessionPreviewSQLiteState.dbInfo = nil
	sessionPreviewSQLiteState.path = ""
	sessionPreviewSQLiteState.mu.Unlock()
	if previous != nil {
		_ = previous.Close()
	}

	db, err := openSessionPreviewSQLite(path)
	if err != nil && isRebuildableSessionPreviewSQLiteError(err) {
		_ = quarantineSessionPreviewSQLite(path)
		db, err = openSessionPreviewSQLite(path)
	}
	if err != nil {
		return nil, err
	}
	info, err = os.Stat(path)
	if err != nil {
		db.Close()
		return nil, err
	}
	sessionPreviewSQLiteState.mu.Lock()
	sessionPreviewSQLiteState.path = path
	sessionPreviewSQLiteState.dbInfo = info
	sessionPreviewSQLiteState.db = db
	sessionPreviewSQLiteState.mu.Unlock()
	return &sessionPreviewSQLiteStore{path: path, db: db}, nil
}

func sessionPreviewSQLiteFile() (string, error) {
	return cacheV2DatabasePath("", cacheV2PreviewFile)
}

func sessionPreviewSQLiteFileForSource(sourcePath string) (string, error) {
	return cacheV2DatabasePath(sourcePath, cacheV2PreviewFile)
}

func openSessionPreviewSQLite(path string) (*sql.DB, error) {
	if err := secureCacheV2Database(path, true); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", sessionPreviewSQLiteFileURI(path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var journalMode string
	if err := db.QueryRow(`PRAGMA journal_mode = DELETE`).Scan(&journalMode); err != nil {
		_ = db.Close()
		return nil, err
	}
	if journalMode != "delete" {
		_ = db.Close()
		return nil, fmt.Errorf("session preview sqlite journal mode = %q, want delete", journalMode)
	}
	for _, statement := range []string{
		`PRAGMA synchronous = NORMAL`,
		fmt.Sprintf(`PRAGMA busy_timeout = %d`, sessionPreviewSQLiteBusyMillis),
		`PRAGMA temp_store = MEMORY`,
		`PRAGMA foreign_keys = ON`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := initializeSessionPreviewSQLiteSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := secureCacheV2Database(path, false); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func initializeSessionPreviewSQLiteSchema(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	var applicationID int
	if err := db.QueryRow(`PRAGMA application_id`).Scan(&applicationID); err != nil {
		return err
	}
	if version != 0 || applicationID != 0 {
		if version != sessionPreviewSQLiteSchemaVersion || applicationID != sessionPreviewSQLiteApplicationID {
			return fmt.Errorf("%w: application_id=%d version=%d", errSessionPreviewSQLiteSchema, applicationID, version)
		}
		return validateSessionPreviewSQLiteSchema(db)
	}
	var existingObjects int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema
		WHERE name NOT LIKE 'sqlite_%'`).Scan(&existingObjects); err != nil {
		return err
	}
	if existingObjects != 0 {
		return fmt.Errorf("%w: uninitialized database contains %d schema objects", errSessionPreviewSQLiteSchema, existingObjects)
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{
		// Cache eviction must not reuse an ID with generation=1 and the same
		// progress tuple while another process still holds that revision.
		`CREATE TABLE preview_session (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				path TEXT NOT NULL UNIQUE,
				filter_version TEXT NOT NULL,
				generation INTEGER NOT NULL,
				parsed_offset INTEGER NOT NULL,
				prefix_tail_hash TEXT NOT NULL,
				prefix_tail_size INTEGER NOT NULL,
				source_size INTEGER NOT NULL,
				source_mtime_ns INTEGER NOT NULL,
				source_mode INTEGER NOT NULL,
				has_file_id INTEGER NOT NULL,
				source_dev TEXT NOT NULL,
				source_ino TEXT NOT NULL,
				has_ctime INTEGER NOT NULL,
				source_ctime_ns INTEGER NOT NULL,
				next_ordinal INTEGER NOT NULL,
				cached_bytes INTEGER NOT NULL DEFAULT 0,
				last_write_ns INTEGER NOT NULL
			) STRICT`,
		`CREATE TABLE preview_message (
				session_id INTEGER NOT NULL REFERENCES preview_session(id) ON DELETE CASCADE,
				generation INTEGER NOT NULL,
				ordinal INTEGER NOT NULL,
				role TEXT NOT NULL,
				content TEXT NOT NULL,
				timestamp_ns INTEGER NOT NULL,
				source_id TEXT NOT NULL,
				PRIMARY KEY (session_id, generation, ordinal)
			) STRICT, WITHOUT ROWID`,
		`CREATE INDEX preview_message_source ON preview_message(session_id,generation,source_id) WHERE source_id<>''`,
		`CREATE TABLE preview_seen_source (
				session_id INTEGER NOT NULL REFERENCES preview_session(id) ON DELETE CASCADE,
				generation INTEGER NOT NULL,
				source_key TEXT NOT NULL,
				PRIMARY KEY (session_id, generation, source_key)
			) STRICT, WITHOUT ROWID`,
		`CREATE TABLE preview_seen_fallback (
				session_id INTEGER NOT NULL REFERENCES preview_session(id) ON DELETE CASCADE,
				generation INTEGER NOT NULL,
				fallback_key TEXT NOT NULL,
				timestamp_ns INTEGER NOT NULL,
				source_kind TEXT NOT NULL,
				PRIMARY KEY (session_id, generation, fallback_key)
			) STRICT, WITHOUT ROWID`,
		fmt.Sprintf(`PRAGMA application_id = %d`, sessionPreviewSQLiteApplicationID),
		fmt.Sprintf(`PRAGMA user_version = %d`, sessionPreviewSQLiteSchemaVersion),
	} {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return validateSessionPreviewSQLiteSchema(db)
}

func validateSessionPreviewSQLiteSchema(db *sql.DB) error {
	queries := []string{
		`SELECT id, path, filter_version, generation, parsed_offset, prefix_tail_hash,
			prefix_tail_size, source_size, source_mtime_ns, source_mode, has_file_id,
			source_dev, source_ino, has_ctime, source_ctime_ns, next_ordinal, cached_bytes,
			last_write_ns FROM preview_session LIMIT 0`,
		`SELECT session_id, generation, ordinal, role, content, timestamp_ns,
			source_id FROM preview_message LIMIT 0`,
		`SELECT session_id, generation, source_key FROM preview_seen_source LIMIT 0`,
		`SELECT session_id, generation, fallback_key, timestamp_ns,
			source_kind FROM preview_seen_fallback LIMIT 0`,
	}
	for _, query := range queries {
		rows, err := db.Query(query)
		if err != nil {
			return fmt.Errorf("%w: %v", errSessionPreviewSQLiteSchema, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("%w: %v", errSessionPreviewSQLiteSchema, err)
		}
	}
	return nil
}

func sessionPreviewSQLiteFileURI(path string) string {
	return cacheV2SQLiteFileURI(path, sessionPreviewSQLiteBusyMillis)
}

func isRebuildableSessionPreviewSQLiteError(err error) bool {
	if errors.Is(err, errSessionPreviewSQLiteSchema) {
		return true
	}
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	switch sqliteErr.Code() & 0xff {
	case 11, 26: // SQLITE_CORRUPT, SQLITE_NOTADB.
		return true
	default:
		return false
	}
}

func quarantineSessionPreviewSQLite(path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	quarantine := fmt.Sprintf("%s.invalid-%d", path, time.Now().UnixNano())
	if previous, _ := filepath.Glob(path + ".invalid-*"); len(previous) > 0 {
		for _, candidate := range previous {
			_ = os.Remove(candidate)
		}
	}
	if err := os.Rename(path, quarantine); err != nil {
		return err
	}
	for _, suffix := range []string{"-journal", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}
	return nil
}

func (store *sessionPreviewSQLiteStore) loadHeader(path string) (sessionPreviewSQLiteEntry, bool, error) {
	var entry sessionPreviewSQLiteEntry
	var hasFileID, hasCtime int
	var dev, ino string
	err := store.db.QueryRow(`SELECT
		id, generation, filter_version, parsed_offset, prefix_tail_hash, prefix_tail_size,
		source_size, source_mtime_ns, source_mode, has_file_id, source_dev, source_ino,
		has_ctime, source_ctime_ns, next_ordinal, cached_bytes
		FROM preview_session WHERE path = ?`, filepath.Clean(path)).Scan(
		&entry.sessionID,
		&entry.generation,
		&entry.filterVersion,
		&entry.offset,
		&entry.prefixTailHash,
		&entry.prefixTailSize,
		&entry.fileKey.Size,
		&entry.fileKey.MtimeUnixNano,
		&entry.fileKey.Mode,
		&hasFileID,
		&dev,
		&ino,
		&hasCtime,
		&entry.fileKey.CtimeUnixNano,
		&entry.nextOrdinal,
		&entry.cachedBytes,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sessionPreviewSQLiteEntry{}, false, nil
	}
	if err != nil {
		return sessionPreviewSQLiteEntry{}, false, err
	}
	entry.fileKey.HasFileID = hasFileID != 0
	entry.fileKey.HasCtime = hasCtime != 0
	decodeCacheDeviceID(&entry.fileKey, dev)
	entry.fileKey.Ino, _ = strconv.ParseUint(ino, 10, 64)

	entry.committedOffset = entry.offset
	entry.committedOrdinal = entry.nextOrdinal
	return entry, true, nil
}

func (store *sessionPreviewSQLiteStore) loadMessages(entry *sessionPreviewSQLiteEntry, wantMessages bool) error {
	if entry == nil {
		return errors.New("nil session preview sqlite entry")
	}
	if !wantMessages {
		rows, err := store.db.Query(`SELECT role, content
			FROM preview_message WHERE session_id = ? AND generation = ? ORDER BY ordinal`, entry.sessionID, entry.generation)
		if err != nil {
			return err
		}
		var builder strings.Builder
		wrote := false
		for rows.Next() {
			var role, content string
			if err := rows.Scan(&role, &content); err != nil {
				_ = rows.Close()
				return err
			}
			label := previewRoleLabel(role)
			content = strings.TrimSpace(content)
			if label == "" || content == "" {
				continue
			}
			if wrote {
				builder.WriteString("\n\n")
			}
			builder.WriteString(label)
			builder.WriteString(":\n")
			builder.WriteString(content)
			wrote = true
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		entry.text = strings.TrimSpace(builder.String())
		return store.checkRevision(*entry)
	}

	entry.messagesLoaded = true
	entry.messages = nil
	rows, err := store.db.Query(`SELECT role, content, timestamp_ns, source_id
		FROM preview_message WHERE session_id = ? AND generation = ? ORDER BY ordinal`, entry.sessionID, entry.generation)
	if err != nil {
		return err
	}
	for rows.Next() {
		var msg Message
		var timestamp int64
		if err := rows.Scan(&msg.Role, &msg.Content, &timestamp, &msg.sourceID); err != nil {
			_ = rows.Close()
			return err
		}
		msg.Timestamp = sessionPreviewTimeFromUnixNano(timestamp)
		entry.messages = append(entry.messages, msg)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return store.checkRevision(*entry)
}

// Rehydrate committed messages and attach the already parsed pending tail.
// A view-mode change must not parse the same uncommitted JSONL records again.
func (store *sessionPreviewSQLiteStore) promoteMessages(path string, entry *sessionPreviewSQLiteEntry) error {
	committed, found, err := store.loadHeader(path)
	if err != nil {
		return err
	}
	if !found || committed.sessionID != entry.sessionID || committed.generation != entry.generation || committed.offset != entry.committedOffset || committed.nextOrdinal != entry.committedOrdinal {
		return errSessionPreviewSQLiteConflict
	}
	if err := store.loadMessages(&committed, true); err != nil {
		return err
	}
	entry.messages = append(committed.messages, entry.pendingMessages...)
	entry.messagesLoaded = true
	return nil
}

// Header-first reads span separate SQL statements. A peer may replace or evict
// rows between them, so never publish a mixed header/payload snapshot.
func (store *sessionPreviewSQLiteStore) checkRevision(entry sessionPreviewSQLiteEntry) error {
	return checkPreviewRevision(store.db, entry)
}
func checkPreviewRevision(db interface{ QueryRow(string, ...any) *sql.Row }, entry sessionPreviewSQLiteEntry) error {
	var current int
	err := db.QueryRow(`SELECT count(*) FROM preview_session WHERE id=? AND generation=? AND parsed_offset=? AND next_ordinal=? AND filter_version=?`, entry.sessionID, entry.generation, entry.committedOffset, entry.committedOrdinal, entry.filterVersion).Scan(&current)
	if err != nil {
		return err
	}
	if current != 1 {
		return errSessionPreviewSQLiteConflict
	}
	return nil
}

func (store *sessionPreviewSQLiteStore) replace(
	expected sessionPreviewSQLiteEntry,
	expectedFound bool,
	path string,
	key fileCacheKey,
	offset int64,
	prefixHash string,
	prefixSize int64,
	messages []Message,
	seen *messageSeenState,
) error {
	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var sessionID, generation int64
	if expectedFound {
		err = tx.QueryRow(`UPDATE preview_session SET
			filter_version = ?, generation = generation + 1, parsed_offset = ?,
			prefix_tail_hash = ?, prefix_tail_size = ?, source_size = ?,
			source_mtime_ns = ?, source_mode = ?, has_file_id = ?, source_dev = ?,
			source_ino = ?, has_ctime = ?, source_ctime_ns = ?, next_ordinal = ?,
			cached_bytes = ?, last_write_ns = ?
			WHERE id = ? AND generation = ? AND parsed_offset = ?
			RETURNING id, generation`,
			sessionPreviewFilterVersion, offset, prefixHash, prefixSize, key.Size,
			key.MtimeUnixNano, key.Mode, boolInt(key.HasFileID),
			cacheDeviceID(key), strconv.FormatUint(key.Ino, 10),
			boolInt(key.HasCtime), key.CtimeUnixNano, len(messages), previewPayloadBytes(messages, seen), sessionPreviewNow().UnixNano(),
			expected.sessionID, expected.generation, expected.offset,
		).Scan(&sessionID, &generation)
	} else {
		err = tx.QueryRow(`INSERT INTO preview_session (
			path, filter_version, generation, parsed_offset, prefix_tail_hash, prefix_tail_size,
			source_size, source_mtime_ns, source_mode, has_file_id, source_dev, source_ino,
			has_ctime, source_ctime_ns, next_ordinal, cached_bytes, last_write_ns
		) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(path) DO NOTHING
		RETURNING id, generation`, filepath.Clean(path), sessionPreviewFilterVersion,
			offset, prefixHash, prefixSize, key.Size, key.MtimeUnixNano, key.Mode,
			boolInt(key.HasFileID), cacheDeviceID(key),
			strconv.FormatUint(key.Ino, 10), boolInt(key.HasCtime), key.CtimeUnixNano,
			len(messages), previewPayloadBytes(messages, seen), sessionPreviewNow().UnixNano(),
		).Scan(&sessionID, &generation)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return errSessionPreviewSQLiteConflict
	}
	if err != nil {
		return err
	}
	if err := insertSessionPreviewMessages(tx, sessionID, generation, 0, messages); err != nil {
		return err
	}
	if err := insertSessionPreviewSeenState(tx, sessionID, generation, messages, seen); err != nil {
		return err
	}
	for _, table := range []string{"preview_message", "preview_seen_source", "preview_seen_fallback"} {
		if _, err := tx.Exec(`DELETE FROM `+table+` WHERE session_id = ? AND generation <> ?`, sessionID, generation); err != nil {
			return err
		}
	}
	if err := pruneSessionPreviewSQLite(tx); err != nil {
		return err
	}
	if err := pruneSessionPreviewBytes(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	notifySessionPreviewSQLiteCommit(store.path)
	return nil
}

func (store *sessionPreviewSQLiteStore) append(
	entry sessionPreviewSQLiteEntry,
	key fileCacheKey,
	offset int64,
	prefixHash string,
	prefixSize int64,
	messages []Message,
	seenDelta sessionPreviewSeenDelta,
) error {
	tx, err := store.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// The offset/ordinal compare-and-swap makes two TUI processes idempotent:
	// only the writer that observed the current tail can advance it.
	result, err := tx.Exec(`UPDATE preview_session SET
		parsed_offset = ?, prefix_tail_hash = ?, prefix_tail_size = ?, source_size = ?,
		source_mtime_ns = ?, source_mode = ?, has_file_id = ?, source_dev = ?, source_ino = ?,
		has_ctime = ?, source_ctime_ns = ?, next_ordinal = next_ordinal + ?, cached_bytes = cached_bytes + ?, last_write_ns = ?
		WHERE id = ? AND generation = ? AND filter_version = ? AND parsed_offset = ? AND next_ordinal = ?`,
		offset, prefixHash, prefixSize, key.Size, key.MtimeUnixNano, key.Mode,
		boolInt(key.HasFileID), cacheDeviceID(key), strconv.FormatUint(key.Ino, 10),
		boolInt(key.HasCtime), key.CtimeUnixNano, len(messages), previewDeltaBytes(messages, seenDelta), sessionPreviewNow().UnixNano(),
		entry.sessionID, entry.generation, sessionPreviewFilterVersion, entry.offset, entry.nextOrdinal,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return errSessionPreviewSQLiteConflict
	}
	if err := insertSessionPreviewMessages(tx, entry.sessionID, entry.generation, entry.nextOrdinal, messages); err != nil {
		return err
	}
	if err := insertSessionPreviewSeenDelta(tx, entry.sessionID, entry.generation, messages, seenDelta); err != nil {
		return err
	}
	if err := pruneSessionPreviewBytes(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	notifySessionPreviewSQLiteCommit(store.path)
	return nil
}

func insertSessionPreviewMessages(tx *sql.Tx, sessionID int64, generation int64, startOrdinal int64, messages []Message) error {
	statement, err := tx.Prepare(`INSERT INTO preview_message (
		session_id, generation, ordinal, role, content, timestamp_ns, source_id
	) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()
	for index, message := range messages {
		if _, err := statement.Exec(
			sessionID,
			generation,
			startOrdinal+int64(index),
			message.Role,
			message.Content,
			sessionPreviewUnixNano(message.Timestamp),
			message.sourceID,
		); err != nil {
			return err
		}
	}
	return nil
}

func insertSessionPreviewSeenState(tx *sql.Tx, sessionID int64, generation int64, messages []Message, seen *messageSeenState) error {
	if seen == nil {
		return nil
	}
	// Retained messages already persist their source IDs. Store only IDs from
	// mirrored records that dedupe intentionally skipped.
	retained := retainedSessionPreviewSourceKeys(messages)
	keys := make([]string, 0, len(seen.sourceKeys))
	for key := range seen.sourceKeys {
		if retained[key] {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := tx.Exec(`INSERT INTO preview_seen_source (session_id, generation, source_key) VALUES (?, ?, ?)`, sessionID, generation, key); err != nil {
			return err
		}
	}
	fallback := make([]persistentSessionPreviewFallbackMessage, 0, len(seen.fallbackTimes))
	for key, timestamp := range seen.fallbackTimes {
		fallback = append(fallback, persistentSessionPreviewFallbackMessage{
			Key:        key,
			Timestamp:  timestamp,
			SourceKind: seen.fallbackSourceKind[key],
		})
	}
	sort.Slice(fallback, func(i, j int) bool { return fallback[i].Key < fallback[j].Key })
	return insertSessionPreviewFallback(tx, sessionID, generation, fallback)
}

func insertSessionPreviewSeenDelta(tx *sql.Tx, sessionID int64, generation int64, messages []Message, delta sessionPreviewSeenDelta) error {
	retained := retainedSessionPreviewSourceKeys(messages)
	for _, key := range delta.sourceKeys {
		if retained[key] {
			continue
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO preview_seen_source (session_id, generation, source_key) VALUES (?, ?, ?)`, sessionID, generation, key); err != nil {
			return err
		}
	}
	return insertSessionPreviewFallback(tx, sessionID, generation, delta.fallback)
}

func retainedSessionPreviewSourceKeys(messages []Message) map[string]bool {
	retained := make(map[string]bool, len(messages))
	for _, message := range messages {
		if key := sourceMessageDedupKey(message.sourceID); key != "" {
			retained[key] = true
		}
	}
	return retained
}

func insertSessionPreviewFallback(tx *sql.Tx, sessionID int64, generation int64, fallback []persistentSessionPreviewFallbackMessage) error {
	for _, message := range fallback {
		if _, err := tx.Exec(`INSERT INTO preview_seen_fallback (
			session_id, generation, fallback_key, timestamp_ns, source_kind
		) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(session_id, generation, fallback_key) DO UPDATE SET
			timestamp_ns = excluded.timestamp_ns,
			source_kind = excluded.source_kind`,
			sessionID, generation, message.Key, sessionPreviewUnixNano(message.Timestamp), message.SourceKind,
		); err != nil {
			return err
		}
	}
	return nil
}

// Logical cache budget, not a claim about SQLite file size or Go heap size.
func previewPayloadBytes(messages []Message, seen *messageSeenState) int64 {
	n := int64(0)
	for _, m := range messages {
		n += int64(len(m.Role) + len(m.Content) + len(m.sourceID) + 96)
	}
	if seen != nil {
		n += seen.bytes
	}
	return n
}
func previewDeltaBytes(messages []Message, delta sessionPreviewSeenDelta) int64 {
	n := previewPayloadBytes(messages, nil)
	for _, key := range delta.sourceKeys {
		n += int64(len(key) + 64)
	}
	for _, m := range delta.fallback {
		n += int64(len(m.Key) + len(m.SourceKind) + 128)
	}
	return n
}
func pruneSessionPreviewBytes(tx *sql.Tx) error {
	var used int64
	if err := tx.QueryRow(`SELECT COALESCE(SUM(cached_bytes),0) FROM preview_session`).Scan(&used); err != nil {
		return err
	}
	if used <= sessionPreviewSQLiteMaxBytes {
		return nil
	}
	// A single oversized session must not evict every smaller usable session.
	if _, err := tx.Exec(`DELETE FROM preview_session WHERE cached_bytes>?`, sessionPreviewSQLiteMaxBytes); err != nil {
		return err
	}
	_, err := tx.Exec(`DELETE FROM preview_session WHERE id IN (SELECT id FROM (SELECT id,SUM(cached_bytes) OVER (ORDER BY last_write_ns DESC,id DESC) AS used FROM preview_session) WHERE used>?)`, sessionPreviewSQLiteMaxBytes)
	return err
}

func pruneSessionPreviewSQLite(tx *sql.Tx) error {
	_, err := tx.Exec(`DELETE FROM preview_session WHERE id IN (
		SELECT id FROM preview_session ORDER BY last_write_ns DESC, id DESC LIMIT -1 OFFSET ?
	)`, sessionPreviewSQLiteMaxSessions)
	return err
}

func sessionPreviewUnixNano(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func sessionPreviewTimeFromUnixNano(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func notifySessionPreviewSQLiteCommit(path string) {
	if sessionPreviewSQLiteCommitHook != nil {
		sessionPreviewSQLiteCommitHook(path)
	}
}

func resetSessionPreviewSQLiteForTest() {
	_ = closeSessionPreviewSQLiteCache()
}
