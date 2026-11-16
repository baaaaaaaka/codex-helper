package codexhistory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCacheDeviceIDEncodingPreservesIdentityKind(t *testing.T) {
	for _, key := range []fileCacheKey{{Dev: 2096}, {DevKind: "ext4-fsid", Dev: 18446744073709551615}} {
		var parsed fileCacheKey
		decodeCacheDeviceID(&parsed, cacheDeviceID(key))
		if parsed.Dev != key.Dev || parsed.DevKind != key.DevKind {
			t.Fatalf("round trip: %+v => %+v", key, parsed)
		}
	}
	for _, value := range []string{"", "ext4-fsid:not-a-number", "-1", ":2096", "unsupported:2096"} {
		var parsed fileCacheKey
		decodeCacheDeviceID(&parsed, value)
		if parsed.DevKind != "invalid" {
			t.Fatalf("invalid identity %q accepted: %+v", value, parsed)
		}
	}
}

func TestCacheFileIdentityRejectsSameSizeMiddleRewrite(t *testing.T) {
	p := filepath.Join(t.TempDir(), "source.jsonl")
	if err := os.WriteFile(p, []byte(strings.Repeat("a", 256*1024)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	key := newFileCacheKey(p, before)
	hash, n, ok := sessionPreviewPrefixTailHash(p, before.Size())
	if !ok {
		t.Fatal("hash unavailable")
	}
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte("b"), before.Size()/2)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("rewrite: %v %v", err, closeErr)
	}
	later := before.ModTime().Add(2 * time.Second)
	if err = os.Chtimes(p, later, later); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	sameHash, _, ok := sessionPreviewPrefixTailHash(p, after.Size())
	if !ok || hash != sameHash {
		t.Fatal("fixture must preserve head/tail hash")
	}
	if matchesFileInfo(p, after, key) || canAppendCacheFile(p, after, key, before.Size(), hash, n) {
		t.Fatal("same-size middle rewrite reused cached contents")
	}
}
