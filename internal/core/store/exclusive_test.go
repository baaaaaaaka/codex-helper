package store

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The network-filesystem mode (openDB): one connection, EXCLUSIVE locking,
// the WAL index in heap memory. These tests force it on the local disk.

func openExclusive(t *testing.T, path string) *Store {
	t.Helper()
	s, err := OpenWith(path, Options{Now: newClock().now, PIDAlive: alwaysAlive, exclusive: true})
	mustT(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// No -shm file, the locking mode is exclusive, and reads from several
// goroutines interleave with writes on the one connection.
func TestExclusiveModeHasNoSharedMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.sqlite")
	s := openExclusive(t, path)
	if s.r != s.w {
		t.Fatal("exclusive mode has a separate read pool")
	}
	_, err := s.Acquire(testIdent(1))
	mustT(t, err)
	var mode string
	mustT(t, s.View(func(q Querier) error { return q.QueryRowContext(bg, "PRAGMA locking_mode").Scan(&mode) }))
	if mode != "exclusive" {
		t.Fatalf("locking_mode %q", mode)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errs := make(chan error, 4)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, _, err := s.Meta("k000001"); err != nil {
					errs <- err
					return
				}
				if _, err := s.Conversations(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for i := 1; i <= 200; i++ {
		mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta(fmt.Sprintf("k%06d", i), strconv.Itoa(i)) }))
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + "-shm"); err == nil {
		t.Fatal("a -shm file exists in exclusive mode")
	}
	if fi, err := os.Stat(path + "-wal"); err != nil || fi.Size() == 0 {
		t.Fatalf("the commits should be in the WAL: %v", err)
	}
}

// While the store is open in exclusive mode no other connection can read
// it, and a read-only Store reads a private copy instead: it sees the
// committed data, does not stop the owner from writing, and removes its
// copy on Close.
func TestExclusiveModeReadOnlyReadsACopy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "core.sqlite")
	s := openExclusive(t, path)
	_, err := s.Acquire(testIdent(1))
	mustT(t, err)
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("k", "1") }))

	other, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(0)&mode=ro")
	mustT(t, err)
	defer other.Close()
	var n int
	if err := other.QueryRow("SELECT count(*) FROM meta").Scan(&n); !isBusy(err) {
		t.Fatalf("another connection read the store in exclusive mode: %v", err)
	}

	ro, err := OpenWith(path, Options{ReadOnly: true, exclusive: true})
	mustT(t, err)
	if v, ok, err := ro.Meta("k"); err != nil || !ok || v != "1" {
		t.Fatalf("read-only view: %q %v %v", v, ok, err)
	}
	if o, ok, err := ro.OwnerInfo(); err != nil || !ok || o.PID != 1 {
		t.Fatalf("read-only owner: %+v %v %v", o, ok, err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Update(func(tx *Tx) error { return tx.SetMeta("k", "2") }) }()
	select {
	case err := <-done:
		mustT(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("the owner's write waited for a read-only view")
	}
	if v, _, _ := ro.Meta("k"); v != "1" {
		t.Fatalf("the view is not a snapshot: %q", v)
	}
	view := ro.view
	if view == "" || strings.HasPrefix(view, dir) {
		t.Fatalf("the view is not a private copy: %q", view)
	}
	mustT(t, ro.Close())
	if _, err := os.Stat(view); !os.IsNotExist(err) {
		t.Fatalf("the copy was not removed: %v", err)
	}
	if v, _, _ := s.Meta("k"); v != "2" {
		t.Fatalf("owner after the view: %q", v)
	}
}

// A process that dies with the store open in exclusive mode leaves the
// database and its WAL, and no -shm. Its acknowledged commits survive both
// a copy by a successor (CopySnapshot) and a reopen in place.
func TestExclusiveModeCrashKeepsEveryCommit(t *testing.T) {
	if dir := os.Getenv("STORE_EXCL_CRASH_DIR"); dir != "" {
		// The child: commit, report, die without closing.
		s, err := OpenWith(filepath.Join(dir, "core.sqlite"), Options{exclusive: true})
		mustT(t, err)
		_, err = s.Acquire(testIdent(os.Getpid()))
		mustT(t, err)
		for i := 1; i <= 300; i++ {
			mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta(fmt.Sprintf("k%06d", i), strconv.Itoa(i)) }))
		}
		mustT(t, writeSynced(filepath.Join(dir, "acked"), "300"))
		os.Exit(3)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestExclusiveModeCrashKeepsEveryCommit$")
	cmd.Env = append(os.Environ(), "STORE_EXCL_CRASH_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 3 {
		t.Fatalf("child: %v\n%s", err, out)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "acked")); err != nil || string(b) != "300" {
		t.Fatalf("child did not finish: %v %q", err, b)
	}
	src := filepath.Join(dir, "core.sqlite")
	if _, err := os.Stat(src + "-shm"); err == nil {
		t.Fatal("the dead owner left a -shm file")
	}
	if fi, err := os.Stat(src + "-wal"); err != nil || fi.Size() == 0 {
		t.Fatalf("the test needs the commits in the WAL: %v", err)
	}
	check := func(s *Store, what string) {
		t.Helper()
		for i := 1; i <= 300; i++ {
			if v, ok, err := s.Meta(fmt.Sprintf("k%06d", i)); err != nil || !ok || v != strconv.Itoa(i) {
				t.Fatalf("%s: commit %d lost: %q %v %v", what, i, v, ok, err)
			}
		}
		var res string
		mustT(t, s.View(func(q Querier) error { return q.QueryRowContext(bg, "PRAGMA quick_check").Scan(&res) }))
		if res != "ok" {
			t.Fatalf("%s: quick_check %s", what, res)
		}
	}
	// Another host: copies the raw files and opens the copy.
	dst := filepath.Join(dir, "core.g2.sqlite")
	mustT(t, CopySnapshot(src, dst))
	c := openExclusive(t, dst)
	check(c, "copy")
	// The same host after the crash: opens the file in place.
	again, err := OpenWith(src, Options{PIDAlive: func(int) bool { return false }, exclusive: true})
	mustT(t, err)
	defer again.Close()
	check(again, "in place")
	if _, err := again.Acquire(testIdent(2)); err != nil {
		t.Fatalf("acquire after the crash: %v", err)
	}
	// Reopening an existing WAL database must not map a -shm either: the
	// locking mode is set before the first read.
	if _, err := os.Stat(src + "-shm"); err == nil {
		t.Fatal("reopening the store in place created a -shm file")
	}
}
