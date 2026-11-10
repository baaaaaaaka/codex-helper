package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A store copied by another machine while its owner is frozen inside a
// write, on a directory both machines share (an NFS export; see
// TAKEOVER-REPORT.md). Skipped unless STORE_MULTIHOST_DIR is set.
//
// STORE_MULTIHOST_ROLE=writer commits STORE_MULTIHOST_COMMITS small
// transactions, recording each acknowledged commit in an ack file, then
// begins one more transaction large enough to spill uncommitted pages into
// the WAL and freezes inside it (it sleeps until killed, store open, write
// lock held, nothing checkpointed).
//
// STORE_MULTIHOST_ROLE=copier, on the other machine, waits for that, copies
// the store with CopySnapshot and checks the copy has every acknowledged
// commit and nothing of the frozen transaction.
func TestMultiHostCopyOfAFrozenWriter(t *testing.T) {
	dir := os.Getenv("STORE_MULTIHOST_DIR")
	if dir == "" {
		t.Skip("STORE_MULTIHOST_DIR not set")
	}
	n, _ := strconv.Atoi(os.Getenv("STORE_MULTIHOST_COMMITS"))
	if n <= 0 {
		n = 2000
	}
	src := filepath.Join(dir, "core.sqlite")
	ack := filepath.Join(dir, "acked")
	frozen := filepath.Join(dir, "frozen")
	switch os.Getenv("STORE_MULTIHOST_ROLE") {
	case "writer":
		mustT(t, os.MkdirAll(dir, 0o755))
		if !networkFS(dir) {
			t.Logf("note: %s is not a network filesystem", dir)
		}
		s, err := OpenWith(src, Options{})
		mustT(t, err)
		_, err = s.Acquire(Ident{Host: "writer", PID: os.Getpid(), BootID: "b"})
		mustT(t, err)
		for i := 1; i <= n; i++ {
			mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta(fmt.Sprintf("engine_user_title:%d", i), strconv.Itoa(i)) }))
			// The commit returned: record it where the copier can see it.
			mustT(t, writeSynced(ack, strconv.Itoa(i)))
		}
		big := strings.Repeat("x", 48<<20) // beyond cache_size: spills into the WAL
		_ = s.Update(func(tx *Tx) error {
			if err := tx.SetMeta("engine_user_title:999999", big); err != nil {
				return err
			}
			mustT(t, writeSynced(frozen, "1"))
			t.Log("frozen inside a write transaction")
			time.Sleep(time.Hour)
			return nil
		})
	case "copier":
		deadline := time.Now().Add(10 * time.Minute)
		for {
			if _, err := os.Stat(frozen); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the writer never froze")
			}
			time.Sleep(200 * time.Millisecond)
		}
		data, err := os.ReadFile(ack)
		mustT(t, err)
		acked, _ := strconv.Atoi(strings.TrimSpace(string(data)))
		if fi, err := os.Stat(src + "-wal"); err == nil {
			t.Logf("source WAL %d bytes (with uncommitted frames of the frozen write)", fi.Size())
		}
		dst := filepath.Join(dir, "core.g2.sqlite")
		start := time.Now()
		mustT(t, CopySnapshot(src, dst))
		t.Logf("copied in %s", time.Since(start).Round(time.Millisecond))
		c, err := OpenWith(dst, Options{ReadOnly: true})
		mustT(t, err)
		defer c.Close()
		missing := 0
		for i := 1; i <= acked; i++ {
			if v, ok, err := c.Meta(fmt.Sprintf("engine_user_title:%d", i)); err != nil || !ok || v != strconv.Itoa(i) {
				missing++
			}
		}
		if missing > 0 {
			t.Fatalf("%d of %d acknowledged commits missing from the copy", missing, acked)
		}
		if _, ok, _ := c.Meta("engine_user_title:999999"); ok {
			t.Fatal("the frozen transaction's data is in the copy")
		}
		t.Logf("all %d acknowledged commits in the copy; the frozen write is not", acked)
	default:
		t.Fatal("STORE_MULTIHOST_ROLE must be writer or copier")
	}
}

func writeSynced(path, s string) error {
	f, err := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(s); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
