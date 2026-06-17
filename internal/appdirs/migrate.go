package appdirs

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type migrationHookStage string

const (
	migrationHookFileBeforeCreateTemp migrationHookStage = "file-before-create-temp"
	migrationHookFileAfterCopy        migrationHookStage = "file-after-copy"
	migrationHookFileBeforeRename     migrationHookStage = "file-before-rename"
	migrationHookFileAfterRename      migrationHookStage = "file-after-rename"
	migrationHookDirAfterCopy         migrationHookStage = "dir-after-copy"
	migrationHookDirBeforeRename      migrationHookStage = "dir-before-rename"
	migrationHookDirAfterRename       migrationHookStage = "dir-after-rename"
)

var migrationTestHook func(stage migrationHookStage, path string) error

var (
	migrationCleanupTTL       = 24 * time.Hour
	migrationCleanupNow       = time.Now
	migrationCleanupRemoveAll = os.RemoveAll
)

func ResolveMigratedFile(newPath string, legacyPaths ...string) (string, error) {
	newPath = strings.TrimSpace(newPath)
	if newPath == "" {
		return "", fmt.Errorf("new path is required")
	}
	var newStatErr error
	if ok, err := pathExists(newPath); err != nil {
		newStatErr = err
	} else if ok {
		for _, legacyPath := range cleanExistingLegacyPaths(newPath, legacyPaths) {
			if ok, err := refreshFileFromLegacyIfNeeded(newPath, legacyPath); err != nil && fileNeedsRefresh(newPath, legacyPath) {
				return legacyPath, nil
			} else if !ok {
				return legacyPath, nil
			}
		}
		return newPath, nil
	}
	var firstLegacy string
	for _, legacyPath := range cleanExistingLegacyPaths(newPath, legacyPaths) {
		if firstLegacy == "" {
			firstLegacy = legacyPath
		}
		if err := CopyFileIfMissing(newPath, legacyPath); err == nil {
			return newPath, nil
		}
	}
	if firstLegacy != "" {
		return firstLegacy, nil
	}
	if newStatErr != nil {
		return "", newStatErr
	}
	return newPath, nil
}

func ResolveMigratedDir(newDir string, legacyDirs ...string) (string, error) {
	newDir = strings.TrimSpace(newDir)
	if newDir == "" {
		return "", fmt.Errorf("new dir is required")
	}
	var newStatErr error
	if ok, err := pathExists(newDir); err != nil {
		newStatErr = err
	} else if ok {
		for _, legacyDir := range cleanExistingLegacyPaths(newDir, legacyDirs) {
			_ = CopyDirContentsIfMissing(newDir, legacyDir)
		}
		return newDir, nil
	}
	var firstLegacy string
	for _, legacyDir := range cleanExistingLegacyPaths(newDir, legacyDirs) {
		if firstLegacy == "" {
			firstLegacy = legacyDir
		}
		if err := CopyDirIfMissing(newDir, legacyDir); err == nil {
			return newDir, nil
		}
	}
	if firstLegacy != "" {
		return firstLegacy, nil
	}
	if newStatErr != nil {
		return "", newStatErr
	}
	return newDir, nil
}

func ResolveMigratedDirWithRequired(newDir string, legacyDir string, requiredNames ...string) (string, error) {
	newDir = strings.TrimSpace(newDir)
	legacyDir = strings.TrimSpace(legacyDir)
	if newDir == "" {
		return "", fmt.Errorf("new dir is required")
	}
	if dirHasRequiredFiles(newDir, requiredNames...) {
		if legacyDir != "" && !sameCleanPath(newDir, legacyDir) && dirHasRequiredFiles(legacyDir, requiredNames...) {
			if err := refreshDirContentsFromLegacy(newDir, legacyDir); err != nil && requiredFilesNeedRefresh(newDir, legacyDir, requiredNames...) {
				return legacyDir, nil
			}
		}
		return newDir, nil
	}
	if legacyDir == "" || sameCleanPath(newDir, legacyDir) || !dirHasRequiredFiles(legacyDir, requiredNames...) {
		if ok, err := pathExists(newDir); err != nil {
			return "", err
		} else if ok {
			return newDir, nil
		}
		return newDir, nil
	}
	if ok, err := pathExists(newDir); err == nil && ok {
		if err := CopyDirContentsIfMissing(newDir, legacyDir); err != nil {
			return legacyDir, nil
		}
	} else if err == nil {
		if err := CopyDirIfMissing(newDir, legacyDir); err != nil {
			return legacyDir, nil
		}
	} else {
		return legacyDir, nil
	}
	if dirHasRequiredFiles(newDir, requiredNames...) {
		return newDir, nil
	}
	return legacyDir, nil
}

func ResolveMigratedRelatedFiles(newBase string, legacyBase string, suffixes ...string) (string, error) {
	newBase = strings.TrimSpace(newBase)
	legacyBase = strings.TrimSpace(legacyBase)
	if newBase == "" {
		return "", fmt.Errorf("new base path is required")
	}
	legacySuffixes, err := existingRelatedSuffixes(legacyBase, suffixes...)
	if err != nil {
		return "", err
	}
	if len(legacySuffixes) == 0 {
		return newBase, nil
	}
	if sameCleanPath(newBase, legacyBase) {
		return newBase, nil
	}
	if relatedFamilyComplete(newBase, legacySuffixes) {
		return newBase, nil
	}
	for _, suffix := range legacySuffixes {
		src := legacyBase + suffix
		dst := newBase + suffix
		ok, err := pathExists(dst)
		if err != nil {
			return legacyBase, nil
		}
		if ok {
			continue
		}
		if err := CopyFileIfMissing(dst, src); err != nil {
			return legacyBase, nil
		}
	}
	if !relatedFamilyComplete(newBase, legacySuffixes) {
		return legacyBase, nil
	}
	return newBase, nil
}

func CopyFileIfMissing(dst string, src string) error {
	dst = strings.TrimSpace(dst)
	src = strings.TrimSpace(src)
	if dst == "" || src == "" {
		return fmt.Errorf("source and destination paths are required")
	}
	if sameCleanPath(dst, src) {
		return nil
	}
	cleanupStaleMigrationTemps(dst)
	if ok, err := pathExists(dst); err != nil {
		return err
	} else if ok {
		return nil
	}
	info, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to migrate symlink %s", src)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing to migrate non-regular file %s", src)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if err := runMigrationTestHook(migrationHookFileBeforeCreateTemp, dst); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".migrating-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpPath)
		}
	}()
	srcFile, err := os.Open(src)
	if err != nil {
		_ = tmp.Close()
		return err
	}
	_, copyErr := io.Copy(tmp, srcFile)
	closeSrcErr := srcFile.Close()
	if copyErr != nil {
		_ = tmp.Close()
		return copyErr
	}
	if closeSrcErr != nil {
		_ = tmp.Close()
		return closeSrcErr
	}
	if err := runMigrationTestHook(migrationHookFileAfterCopy, dst); err != nil {
		_ = tmp.Close()
		return err
	}
	mode := info.Mode().Perm()
	if mode == 0 {
		mode = 0o600
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := runMigrationTestHook(migrationHookFileBeforeRename, dst); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		if ok, statErr := pathExists(dst); statErr == nil && ok {
			return nil
		}
		return err
	}
	_ = os.Chtimes(dst, info.ModTime(), info.ModTime())
	syncParentDirBestEffort(dst)
	if err := runMigrationTestHook(migrationHookFileAfterRename, dst); err != nil {
		return err
	}
	removeTmp = false
	return nil
}

func CopyFileReplacing(dst string, src string) error {
	dst = strings.TrimSpace(dst)
	src = strings.TrimSpace(src)
	if dst == "" || src == "" {
		return fmt.Errorf("source and destination paths are required")
	}
	if sameCleanPath(dst, src) {
		return nil
	}
	cleanupStaleMigrationTemps(dst)
	info, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to migrate symlink %s", src)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing to migrate non-regular file %s", src)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if err := runMigrationTestHook(migrationHookFileBeforeCreateTemp, dst); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".migrating-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpPath)
		}
	}()
	srcFile, err := os.Open(src)
	if err != nil {
		_ = tmp.Close()
		return err
	}
	_, copyErr := io.Copy(tmp, srcFile)
	closeSrcErr := srcFile.Close()
	if copyErr != nil {
		_ = tmp.Close()
		return copyErr
	}
	if closeSrcErr != nil {
		_ = tmp.Close()
		return closeSrcErr
	}
	if err := runMigrationTestHook(migrationHookFileAfterCopy, dst); err != nil {
		_ = tmp.Close()
		return err
	}
	mode := info.Mode().Perm()
	if mode == 0 {
		mode = 0o600
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := runMigrationTestHook(migrationHookFileBeforeRename, dst); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		return err
	}
	_ = os.Chtimes(dst, info.ModTime(), info.ModTime())
	syncParentDirBestEffort(dst)
	if err := runMigrationTestHook(migrationHookFileAfterRename, dst); err != nil {
		return err
	}
	removeTmp = false
	return nil
}

func CopyDirIfMissing(dst string, src string) error {
	dst = strings.TrimSpace(dst)
	src = strings.TrimSpace(src)
	if dst == "" || src == "" {
		return fmt.Errorf("source and destination dirs are required")
	}
	if sameCleanPath(dst, src) {
		return nil
	}
	cleanupStaleMigrationTemps(dst)
	if ok, err := pathExists(dst); err != nil {
		return err
	} else if ok {
		return nil
	}
	info, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("source is not a directory: %s", src)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(dst), "."+filepath.Base(dst)+fmt.Sprintf(".migrating-%d-%d", os.Getpid(), time.Now().UnixNano()))
	if err := copyDirContents(tmp, src); err != nil {
		_ = os.RemoveAll(tmp)
		return err
	}
	if err := runMigrationTestHook(migrationHookDirAfterCopy, dst); err != nil {
		_ = os.RemoveAll(tmp)
		return err
	}
	if err := runMigrationTestHook(migrationHookDirBeforeRename, dst); err != nil {
		_ = os.RemoveAll(tmp)
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.RemoveAll(tmp)
		if ok, statErr := pathExists(dst); statErr == nil && ok {
			return nil
		}
		return err
	}
	syncParentDirBestEffort(dst)
	if err := runMigrationTestHook(migrationHookDirAfterRename, dst); err != nil {
		return err
	}
	return nil
}

func CopyDirContentsIfMissing(dst string, src string) error {
	dst = strings.TrimSpace(dst)
	src = strings.TrimSpace(src)
	if dst == "" || src == "" {
		return fmt.Errorf("source and destination dirs are required")
	}
	if sameCleanPath(dst, src) {
		return nil
	}
	info, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("source is not a directory: %s", src)
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return err
	}
	return copyDirContents(dst, src)
}

func cleanExistingLegacyPaths(newPath string, legacyPaths []string) []string {
	var out []string
	seen := map[string]bool{filepath.Clean(newPath): true}
	for _, legacyPath := range legacyPaths {
		legacyPath = strings.TrimSpace(legacyPath)
		if legacyPath == "" {
			continue
		}
		clean := filepath.Clean(legacyPath)
		if seen[clean] {
			continue
		}
		seen[clean] = true
		ok, err := pathExists(clean)
		if err == nil && ok {
			out = append(out, clean)
		}
	}
	return out
}

func existingRelatedSuffixes(base string, suffixes ...string) ([]string, error) {
	base = strings.TrimSpace(base)
	if base == "" {
		return nil, nil
	}
	var out []string
	for _, suffix := range suffixesWithBase(suffixes...) {
		ok, err := pathExists(base + suffix)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, suffix)
		}
	}
	return out, nil
}

func relatedFamilyComplete(base string, suffixes []string) bool {
	if strings.TrimSpace(base) == "" {
		return false
	}
	for _, suffix := range suffixes {
		if _, ok, err := regularFileInfo(base + suffix); err != nil || !ok {
			return false
		}
	}
	return true
}

func dirHasRequiredFiles(dir string, names ...string) bool {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return false
	}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok, err := regularFileInfo(filepath.Join(dir, name)); err != nil || !ok {
			return false
		}
	}
	return true
}

func refreshDirContentsFromLegacy(dst string, src string) error {
	dst = strings.TrimSpace(dst)
	src = strings.TrimSpace(src)
	if dst == "" || src == "" {
		return fmt.Errorf("source and destination dirs are required")
	}
	if sameCleanPath(dst, src) {
		return nil
	}
	info, err := os.Lstat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("source is not a directory: %s", src)
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return err
	}
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		if !info.Mode().IsRegular() || strings.HasSuffix(entry.Name(), ".lock") {
			return nil
		}
		return copyFileIfMissingOrSourceNewer(target, path, info)
	})
}

func copyFileIfMissingOrSourceNewer(dst string, src string, srcInfo os.FileInfo) error {
	dstInfo, err := os.Lstat(dst)
	if errors.Is(err, os.ErrNotExist) {
		return CopyFileIfMissing(dst, src)
	}
	if err != nil {
		return err
	}
	if dstInfo.Mode()&os.ModeSymlink != 0 || !dstInfo.Mode().IsRegular() {
		return fmt.Errorf("destination is not a regular file: %s", dst)
	}
	if os.SameFile(srcInfo, dstInfo) {
		return nil
	}
	if srcInfo.ModTime().After(dstInfo.ModTime()) {
		return CopyFileReplacing(dst, src)
	}
	if srcInfo.ModTime().Equal(dstInfo.ModTime()) {
		sameContent, err := regularFileContentsEqual(src, dst, srcInfo, dstInfo)
		if err != nil {
			return err
		}
		if !sameContent {
			return CopyFileReplacing(dst, src)
		}
	}
	return nil
}

func refreshFileFromLegacyIfNeeded(dst string, src string) (bool, error) {
	srcInfo, srcOK, srcErr := regularFileInfo(src)
	if srcErr != nil {
		return true, srcErr
	}
	if !srcOK {
		return true, nil
	}
	if err := copyFileIfMissingOrSourceNewer(dst, src, srcInfo); err != nil {
		if fileNeedsRefresh(dst, src) {
			return false, err
		}
		return true, nil
	}
	return true, nil
}

func fileNeedsRefresh(dst string, src string) bool {
	srcInfo, srcOK, srcErr := regularFileInfo(src)
	if srcErr != nil || !srcOK {
		return false
	}
	dstInfo, dstOK, dstErr := regularFileInfo(dst)
	if dstErr != nil || !dstOK {
		return true
	}
	if os.SameFile(srcInfo, dstInfo) {
		return false
	}
	if srcInfo.ModTime().After(dstInfo.ModTime()) {
		return true
	}
	if srcInfo.ModTime().Equal(dstInfo.ModTime()) {
		sameContent, err := regularFileContentsEqual(src, dst, srcInfo, dstInfo)
		if err != nil || !sameContent {
			return true
		}
	}
	return false
}

func requiredFilesNeedRefresh(newDir string, legacyDir string, names ...string) bool {
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		legacyInfo, legacyOK, legacyErr := regularFileInfo(filepath.Join(legacyDir, name))
		if legacyErr != nil || !legacyOK {
			continue
		}
		newInfo, newOK, newErr := regularFileInfo(filepath.Join(newDir, name))
		if newErr != nil || !newOK {
			return true
		}
		if os.SameFile(legacyInfo, newInfo) {
			continue
		}
		if legacyInfo.ModTime().After(newInfo.ModTime()) {
			return true
		}
		if legacyInfo.ModTime().Equal(newInfo.ModTime()) {
			sameContent, err := regularFileContentsEqual(filepath.Join(legacyDir, name), filepath.Join(newDir, name), legacyInfo, newInfo)
			if err != nil || !sameContent {
				return true
			}
		}
	}
	return false
}

func regularFileInfo(path string) (os.FileInfo, bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return info, false, nil
	}
	return info, true, nil
}

func regularFileContentsEqual(a string, b string, aInfo os.FileInfo, bInfo os.FileInfo) (bool, error) {
	if aInfo.Size() != bInfo.Size() {
		return false, nil
	}
	aHash, err := fileSHA256(a)
	if err != nil {
		return false, err
	}
	bHash, err := fileSHA256(b)
	if err != nil {
		return false, err
	}
	return aHash == bHash, nil
}

func fileSHA256(path string) ([32]byte, error) {
	var out [32]byte
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return out, err
	}
	copy(out[:], h.Sum(nil))
	return out, nil
}

func runMigrationTestHook(stage migrationHookStage, path string) error {
	if migrationTestHook == nil {
		return nil
	}
	return migrationTestHook(stage, path)
}

func cleanupStaleMigrationTemps(target string) {
	target = strings.TrimSpace(target)
	if target == "" {
		return
	}
	dir := filepath.Dir(target)
	base := filepath.Base(target)
	prefix := "." + base + ".migrating-"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	now := migrationCleanupNow()
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if migrationCleanupTTL > 0 && now.Sub(info.ModTime()) < migrationCleanupTTL {
			continue
		}
		_ = migrationCleanupRemoveAll(path)
	}
}

func syncParentDirBestEffort(path string) {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return
	}
	defer func() { _ = dir.Close() }()
	_ = dir.Sync()
}

func copyDirContents(dst string, src string) error {
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if rel == "." {
			return os.MkdirAll(target, 0o700)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing to migrate symlink %s", path)
		}
		if entry.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refusing to migrate non-regular file %s", path)
		}
		if strings.HasSuffix(entry.Name(), ".lock") {
			return nil
		}
		return CopyFileIfMissing(target, path)
	})
}

func relatedPaths(base string, suffixes ...string) []string {
	withBase := suffixesWithBase(suffixes...)
	paths := make([]string, 0, len(withBase))
	for _, suffix := range withBase {
		paths = append(paths, base+suffix)
	}
	return paths
}

func suffixesWithBase(suffixes ...string) []string {
	out := []string{""}
	for _, suffix := range suffixes {
		if suffix == "" {
			continue
		}
		out = append(out, suffix)
	}
	return out
}

func anyPathExists(paths []string) bool {
	for _, path := range paths {
		ok, err := pathExists(path)
		if err == nil && ok {
			return true
		}
	}
	return false
}

func pathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func sameCleanPath(a string, b string) bool {
	return filepath.Clean(a) == filepath.Clean(b)
}
