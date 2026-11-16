package codexhistory

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

type cacheIdentityInfo struct {
	os.FileInfo
	stat syscall.Stat_t
}

func (s cacheIdentityInfo) Sys() any { return &s.stat }

func TestCacheFileIdentityExt4SurvivesDeviceRenumbering(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	old := cacheStatfs
	t.Cleanup(func() { cacheStatfs = old })
	cacheStatfs = func(_ string, s *unix.Statfs_t) error {
		s.Type = unix.EXT4_SUPER_MAGIC
		s.Fsid.Val = [2]int32{123, 456}
		return nil
	}
	p := t.TempDir()
	info, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	a := cacheIdentityInfo{FileInfo: info, stat: *info.Sys().(*syscall.Stat_t)}
	a.stat.Dev = 2128
	b := a
	b.stat.Dev = 2096
	key := newFileCacheKey(p, a)
	if key.DevKind != "ext4-fsid" {
		t.Fatalf("missing identity namespace: %+v", key)
	}
	if !matchesFileInfo(p, b, key) {
		t.Fatal("device renumbering invalidated unchanged ext4 identity")
	}
	if !canAppendCacheFile(p, b, key, 0, "", 0) {
		t.Fatal("device renumbering rejected the unchanged append base")
	}
	decoded := fileCacheKey{}
	decodeCacheDeviceID(&decoded, cacheDeviceID(key))
	if decoded.Dev != key.Dev || decoded.DevKind != key.DevKind {
		t.Fatalf("identity round trip: %+v", decoded)
	}
	// Legacy rows must not be interpreted as the new kind, even if the numeric ID collides.
	legacy := key
	legacy.DevKind = ""
	if matchesFileInfo(p, b, legacy) {
		t.Fatal("legacy native device accepted as filesystem ID")
	}
	// A different filesystem with the same inode must still invalidate the cache.
	cacheStatfs = func(_ string, s *unix.Statfs_t) error {
		s.Type = unix.EXT4_SUPER_MAGIC
		s.Fsid.Val = [2]int32{123, 457}
		return nil
	}
	if matchesFileInfo(p, b, key) || canAppendCacheFile(p, b, key, 0, "", 0) {
		t.Fatal("different filesystem reused cached data")
	}
}

func TestCacheFileIdentityExt4FallbackIsConservative(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	old := cacheStatfs
	t.Cleanup(func() { cacheStatfs = old })
	for _, name := range []string{"unsupported", "zero", "error"} {
		t.Run(name, func(t *testing.T) {
			cacheStatfs = func(_ string, s *unix.Statfs_t) error {
				switch name {
				case "unsupported":
					s.Type = unix.NFS_SUPER_MAGIC
				case "zero":
					s.Type = unix.EXT4_SUPER_MAGIC
				case "error":
					return errors.New("statfs unavailable")
				}
				return nil
			}
			key := fileCacheKey{HasFileID: true, Dev: 42, Ino: 7}
			populatePlatformFileCacheKey("unused", nil, &key)
			if key.Dev != 42 || key.DevKind != "" {
				t.Fatalf("unsafe fallback: %+v", key)
			}
		})
	}
}

func TestCacheFileIdentityExt4PersistsInCatalogAndPreview(t *testing.T) {
	lockCodexHistoryTestHooks(t)
	setTestUserCacheDir(t)
	old := cacheStatfs
	t.Cleanup(func() { cacheStatfs = old })
	cacheStatfs = func(_ string, s *unix.Statfs_t) error {
		s.Type = unix.EXT4_SUPER_MAGIC
		s.Fsid.Val = [2]int32{321, 654}
		return nil
	}
	root := os.Getenv(EnvCodexDir)
	source := testSessionPath(t, root, "identity")
	writeSessionMetaFile(t, source, "identity", root, "keep this prompt")
	before, err := readSessionFileMetaCached(source)
	if err != nil {
		t.Fatal(err)
	}
	text, err := ReadSessionPreviewText(source, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = CloseCaches(); err != nil {
		t.Fatal(err)
	}
	oldHook := sessionMetaWindowReadHook
	t.Cleanup(func() { sessionMetaWindowReadHook = oldHook })
	sessionMetaWindowReadHook = func(int64, int64) { t.Fatal("unchanged persistent metadata was reparsed") }
	after, err := readSessionFileMetaCached(source)
	if err != nil || after != before {
		t.Fatalf("catalog round trip: %v", err)
	}
	preview, err := currentSessionPreviewSQLiteStoreForSource(source)
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := preview.load(source, false, false)
	if err != nil || !found || entry.fileKey.DevKind != "ext4-fsid" {
		t.Fatalf("preview identity missing: %+v %v", entry.fileKey, err)
	}
	again, err := ReadSessionPreviewText(source, 0, 0)
	if err != nil || again != text {
		t.Fatalf("preview round trip: %v", err)
	}
}
