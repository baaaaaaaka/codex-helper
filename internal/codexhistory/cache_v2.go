package codexhistory

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/gofrs/flock"
)

// cacheVersion is deliberately shared by every codexhistory cache database.
// It is not the cxp release version: bumping it means every cache database is
// rebuilt in a new directory, while ordinary application upgrades reuse the
// current cache unchanged.
const cacheVersion = 2

const (
	cacheV2CatalogFile      = "catalog.sqlite3"
	cacheV2PreviewFile      = "preview.sqlite3"
	cacheV2SharedDirMode    = os.ModeSticky | 0o777
	cacheV2IdentityDirMode  = 0o700
	cacheV2DatabaseFileMode = 0o600
)

func cacheVersionDirName() string {
	return fmt.Sprintf("v%d", cacheVersion)
}

var cacheV2CleanupState = struct {
	sync.Mutex
	roots map[string]bool
}{roots: map[string]bool{}}

var persistentCacheWriterScopeID = defaultPersistentCacheWriterScopeID

// CloseCaches releases all open codexhistory SQLite handles and clears the
// associated in-process acceleration. Long-running callers should use it when
// they are finished with a Codex data root; process exit remains sufficient for
// short-lived commands. Cache contents stay on disk and can be reopened.
func CloseCaches() error {
	var closeErrors []error
	if err := closeSessionPreviewSQLiteCache(); err != nil {
		closeErrors = append(closeErrors, err)
	}
	if err := closeCatalogSQLiteCaches(); err != nil {
		closeErrors = append(closeErrors, err)
	}
	resetSessionFileCache()
	return errors.Join(closeErrors...)
}

func closeSessionPreviewSQLiteCache() error {
	sessionPreviewSQLiteState.mu.Lock()
	var closeErr error
	if sessionPreviewSQLiteState.db != nil {
		closeErr = sessionPreviewSQLiteState.db.Close()
	}
	sessionPreviewSQLiteState.path = ""
	sessionPreviewSQLiteState.db = nil
	sessionPreviewSQLiteState.memory = nil
	sessionPreviewSQLiteState.memoryBytes = 0
	sessionPreviewSQLiteState.memoryTick = 0
	sessionPreviewSQLiteState.mu.Unlock()

	sessionPreviewAppendState.mu.Lock()
	sessionPreviewAppendState.dirtySince = nil
	sessionPreviewAppendState.mu.Unlock()
	return closeErr
}

func closeCatalogSQLiteCaches() error {
	catalogSQLiteState.Lock()
	var closeErrors []error
	for _, store := range catalogSQLiteState.stores {
		if err := store.db.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	catalogSQLiteState.stores = map[string]*catalogSQLiteStore{}
	catalogSQLiteState.Unlock()
	return errors.Join(closeErrors...)
}

func codexRootForSessionFile(filePath string) string {
	path := filepath.Clean(filePath)
	for dir := filepath.Dir(path); dir != "" && dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if filepath.Base(dir) == "sessions" {
			return filepath.Dir(dir)
		}
	}
	return ""
}

func sharedPersistentCacheBase(root string) string {
	root = strings.TrimSpace(root)
	if root == "" {
		return ""
	}
	return filepath.Join(root, ".codex-proxy", "codexhistory")
}

func cacheV2RootForSource(sourcePath string) (string, error) {
	clean := filepath.Clean(strings.TrimSpace(sourcePath))
	if clean != "" && clean != "." {
		if root := codexRootForSessionFile(clean); root != "" {
			return root, nil
		}
		switch filepath.Base(clean) {
		case "history.jsonl", threadNameIndexFile:
			return filepath.Dir(clean), nil
		}
	}
	selection, err := ResolveCodexDirSelection("")
	if err != nil {
		return "", err
	}
	return selection.Dir, nil
}

func cacheV2ScopeDirForSource(sourcePath string) (string, error) {
	root, err := cacheV2RootForSource(sourcePath)
	if err != nil {
		return "", err
	}
	return cacheV2ScopeDir(root)
}

func cacheV2ScopeDir(root string) (string, error) {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "" || root == "." {
		return "", errors.New("empty codex cache root")
	}
	scope, err := persistentCacheWriterScopeID()
	if err != nil {
		return "", err
	}
	scope = strings.TrimSpace(scope)
	if !validCacheV2Scope(scope) {
		return "", fmt.Errorf("invalid codex cache identity %q", scope)
	}
	return filepath.Join(sharedPersistentCacheBase(root), cacheVersionDirName(), scope), nil
}

func cacheV2DatabasePath(sourcePath string, name string) (string, error) {
	if name != cacheV2CatalogFile && name != cacheV2PreviewFile {
		return "", fmt.Errorf("invalid codex cache database name %q", name)
	}
	dir, err := cacheV2ScopeDirForSource(sourcePath)
	if err != nil {
		return "", err
	}
	if err := ensureCacheV2ScopeDir(dir); err != nil {
		return "", err
	}
	return filepath.Join(dir, name), nil
}

func existingCacheV2DatabasePath(sourcePath string, name string) (string, bool, error) {
	if name != cacheV2CatalogFile && name != cacheV2PreviewFile {
		return "", false, fmt.Errorf("invalid codex cache database name %q", name)
	}
	dir, err := cacheV2ScopeDirForSource(sourcePath)
	if err != nil {
		return "", false, err
	}
	path := filepath.Join(dir, name)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return path, false, nil
	}
	if err != nil {
		return path, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return path, false, fmt.Errorf("unsafe codex cache database %q: mode %s", path, info.Mode())
	}
	return path, true, nil
}

func validCacheV2Scope(scope string) bool {
	if scope == "" || scope == "." || scope == ".." {
		return false
	}
	for _, r := range scope {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

func ensureCacheV2ScopeDir(scopeDir string) error {
	scopeDir = filepath.Clean(scopeDir)
	versionDir := filepath.Dir(scopeDir)
	historyDir := filepath.Dir(versionDir)
	proxyDir := filepath.Dir(historyDir)

	for _, dir := range []string{proxyDir, historyDir, versionDir} {
		if err := ensureCacheV2Dir(dir, cacheV2SharedDirMode, true); err != nil {
			return err
		}
	}
	return ensureCacheV2Dir(scopeDir, cacheV2IdentityDirMode, false)
}

func ensureCacheV2Dir(path string, mode os.FileMode, shared bool) error {
	for {
		info, err := os.Lstat(path)
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return fmt.Errorf("unsafe codex cache directory %q: mode %s", path, info.Mode())
			}
			if runtime.GOOS != "windows" {
				want := mode.Perm()
				if shared {
					want |= os.ModeSticky
				}
				if info.Mode().Perm() != mode.Perm() || (shared && info.Mode()&os.ModeSticky == 0) {
					// This is best effort for existing directories. The per-identity
					// child remains private even when the shared parent is writable.
					_ = os.Chmod(path, want)
				}
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.Mkdir(path, mode); err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return err
		}
	}
}

func secureCacheV2Database(path string, create bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && create {
		file, createErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, cacheV2DatabaseFileMode)
		if createErr == nil {
			if closeErr := file.Close(); closeErr != nil {
				return closeErr
			}
			info, err = os.Lstat(path)
		} else if errors.Is(createErr, os.ErrExist) {
			info, err = os.Lstat(path)
		} else {
			return createErr
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("unsafe codex cache database %q: mode %s", path, info.Mode())
	}
	if runtime.GOOS == "windows" || info.Mode().Perm() == cacheV2DatabaseFileMode {
		return nil
	}
	return os.Chmod(path, cacheV2DatabaseFileMode)
}

func cacheV2SQLiteFileURI(path string, busyMillis int) string {
	slash := filepath.ToSlash(path)
	u := &url.URL{Scheme: "file", Path: slash}
	if runtime.GOOS == "windows" {
		if strings.HasPrefix(slash, "//") {
			trimmed := strings.TrimLeft(slash, "/")
			host, rest, ok := strings.Cut(trimmed, "/")
			if ok {
				u.Host = host
				u.Path = "/" + rest
			}
		}
		if len(slash) >= 2 && slash[1] == ':' {
			u.Path = "/" + slash
		}
	}
	query := u.Query()
	for _, pragma := range []string{
		"synchronous(NORMAL)",
		fmt.Sprintf("busy_timeout(%d)", busyMillis),
		"temp_store(MEMORY)",
		"foreign_keys(ON)",
	} {
		query.Add("_pragma", pragma)
	}
	u.RawQuery = query.Encode()
	return u.String()
}

// cleanupLegacyCaches removes only this identity's known legacy artifacts.
// Cache data is disposable, but cleanup must never be required for correctness:
// failures are ignored and retried by a later process.
func cleanupLegacyCaches(root string) {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "" || root == "." {
		return
	}
	cacheV2CleanupState.Lock()
	if cacheV2CleanupState.roots[root] {
		cacheV2CleanupState.Unlock()
		return
	}
	cacheV2CleanupState.roots[root] = true
	cacheV2CleanupState.Unlock()

	base, err := legacyLocalPersistentCacheDir()
	if err != nil {
		return
	}

	writerIDs := legacyWriterIDs(base)
	for _, name := range []string{
		"session_meta_cache.json",
		"history_index_cache.json",
		"session_preview_cache.json",
	} {
		removeLegacyLockedFile(filepath.Join(base, name))
	}

	preview := filepath.Join(base, "session_preview_cache.sqlite3")
	for _, suffix := range []string{"-wal", "-shm", "-journal", ""} {
		_ = os.Remove(preview + suffix)
	}
	if invalid, _ := filepath.Glob(preview + ".invalid-*"); len(invalid) > 0 {
		for _, path := range invalid {
			_ = os.Remove(path)
		}
	}

	shared := sharedPersistentCacheBase(root)
	for _, writerID := range writerIDs {
		for _, kind := range []string{"session-meta", "history-index"} {
			removeLegacyLockedFile(filepath.Join(shared, kind, writerID+".json"))
			_ = os.Remove(filepath.Join(shared, kind))
		}
	}
	for _, pattern := range []string{"shared_writer_id.json", "shared_writer_id.*.json"} {
		matches, _ := filepath.Glob(filepath.Join(base, pattern))
		for _, path := range matches {
			removeLegacyLockedFile(path)
		}
	}
	cleanupPreviousCacheVersions(shared)
}

func cleanupPreviousCacheVersions(base string) {
	scope, err := persistentCacheWriterScopeID()
	if err != nil || !validCacheV2Scope(scope) {
		return
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || name == cacheVersionDirName() || !isCacheVersionDirName(name) {
			continue
		}
		dir := filepath.Join(base, name, scope)
		for _, database := range []string{cacheV2CatalogFile, cacheV2PreviewFile} {
			path := filepath.Join(dir, database)
			for _, suffix := range []string{"-journal", "-wal", "-shm", ""} {
				_ = os.Remove(path + suffix)
			}
			invalid, _ := filepath.Glob(path + ".invalid-*")
			for _, candidate := range invalid {
				_ = os.Remove(candidate)
			}
		}
		_ = os.Remove(dir)
		_ = os.Remove(filepath.Join(base, name))
	}
}

func isCacheVersionDirName(name string) bool {
	if len(name) < 2 || name[0] != 'v' {
		return false
	}
	for _, r := range name[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func legacyLocalPersistentCacheDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return "", err
		}
		base = filepath.Join(home, ".cache")
	}
	return filepath.Join(base, "codex-proxy", "codexhistory"), nil
}

func legacyWriterIDs(base string) []string {
	seen := map[string]bool{}
	var ids []string
	matches, _ := filepath.Glob(filepath.Join(base, "shared_writer_id*.json"))
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		id := normalizePersistentCacheWriterID(data)
		if id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids
}

func normalizePersistentCacheWriterID(data []byte) string {
	var id string
	if err := json.Unmarshal(data, &id); err != nil {
		return ""
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	for _, r := range id {
		if (r < 'a' || r > 'f') && (r < '0' || r > '9') {
			return ""
		}
	}
	return id
}

func removeLegacyLockedFile(path string) {
	if info, err := os.Lstat(path); err != nil || !info.Mode().IsRegular() {
		return
	}
	lock := flock.New(path + ".lock")
	ok, err := lock.TryLock()
	if err != nil || !ok {
		return
	}
	defer lock.Unlock()
	_ = os.Remove(path)
}

func resetCacheV2ForTest() {
	cacheV2CleanupState.Lock()
	cacheV2CleanupState.roots = map[string]bool{}
	cacheV2CleanupState.Unlock()
}
