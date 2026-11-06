package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// The network-filesystem mode (openDB): one connection, EXCLUSIVE locking,
// the WAL index in heap memory. These tests force it on the local disk.

func openExclusive(t *testing.T, path string) *Store {
	t.Helper()
	s, err := OpenWith(path, Options{Now: newClock().now, PIDAlive: alwaysAlive, Exclusive: true})
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
// it: a read-only Store opened meanwhile gets ErrBusy (a command then
// reads what the helper publishes instead).
func TestExclusiveModeReadOnlyIsRefusedWhileTheOwnerHasIt(t *testing.T) {
	short(t, 200)
	path := filepath.Join(t.TempDir(), "core.sqlite")
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
	if ro, err := OpenWith(path, Options{ReadOnly: true, Exclusive: true}); !errors.Is(err, ErrBusy) {
		if ro != nil {
			ro.Close()
		}
		t.Fatalf("read-only open next to the owner: %v, want ErrBusy", err)
	}
	if v, _, err := s.Meta("k"); err != nil || v != "1" {
		t.Fatalf("owner after the refused open: %q %v", v, err)
	}
}

// With no owner, a read-only Store opens the file itself in exclusive
// mode: it reads the data, writes nothing (query_only), creates no -shm,
// and holds the file until Close. A helper that starts meanwhile waits for
// it instead of failing, and opens the store once the command closes it.
func TestExclusiveModeReadOnlyOpensDirectlyAndTheOwnerWaits(t *testing.T) {
	short(t, 200)
	path := filepath.Join(t.TempDir(), "core.sqlite")
	s := openExclusive(t, path)
	_, err := s.Acquire(testIdent(1))
	mustT(t, err)
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("k", "1") }))
	mustT(t, s.Release())
	mustT(t, s.Close())

	ro, err := OpenWith(path, Options{ReadOnly: true, Exclusive: true})
	mustT(t, err)
	if v, ok, err := ro.Meta("k"); err != nil || !ok || v != "1" {
		t.Fatalf("read-only: %q %v %v", v, ok, err)
	}
	if err := ro.Update(func(tx *Tx) error { return tx.SetMeta("k", "2") }); err == nil {
		t.Fatal("a read-only Store wrote")
	}
	if _, err := ro.r.Exec(`INSERT INTO meta(key,value) VALUES ('x','y')`); err == nil {
		t.Fatal("a read-only connection wrote (query_only is off)")
	}
	if _, err := os.Stat(path + "-shm"); err == nil {
		t.Fatal("a read-only open in exclusive mode created a -shm file")
	}

	type opened struct {
		s   *Store
		err error
		at  time.Time
	}
	done := make(chan opened, 1)
	go func() {
		w, err := OpenWith(path, Options{Now: newClock().now, PIDAlive: alwaysAlive, Exclusive: true})
		done <- opened{w, err, time.Now()}
	}()
	time.Sleep(600 * time.Millisecond) // three busy timeouts: the owner retries
	select {
	case o := <-done:
		t.Fatalf("the owner opened the store while a command held it: %v", o.err)
	default:
	}
	closed := time.Now()
	mustT(t, ro.Close())
	var o opened
	select {
	case o = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the owner did not open the store after the command closed it")
	}
	mustT(t, o.err)
	defer o.s.Close()
	if o.at.Before(closed) {
		t.Fatal("the owner opened before the command closed the store")
	}
	if _, err := o.s.Acquire(testIdent(2)); err != nil {
		t.Fatal(err)
	}
	mustT(t, o.s.Update(func(tx *Tx) error { return tx.SetMeta("k", "3") }))
}

// A command that keeps the file longer than openLockWait makes the owner's
// open fail with ErrBusy (the service starts it again later), never hang.
func TestExclusiveModeOwnerOpenGivesUpOnAHeldFile(t *testing.T) {
	short(t, 100)
	wait := openLockWait
	openLockWait = 500 * time.Millisecond
	t.Cleanup(func() { openLockWait = wait })
	path := filepath.Join(t.TempDir(), "core.sqlite")
	s := openExclusive(t, path)
	mustT(t, s.Close())
	ro, err := OpenWith(path, Options{ReadOnly: true, Exclusive: true})
	mustT(t, err)
	defer ro.Close()
	if w, err := OpenWith(path, Options{Exclusive: true}); !errors.Is(err, ErrBusy) {
		if w != nil {
			w.Close()
		}
		t.Fatalf("open of a held file: %v, want ErrBusy", err)
	}
}

// Changes signals after a commit, coalesced, and not for a rolled-back
// transaction.
func TestChangesSignalsCommits(t *testing.T) {
	for _, excl := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "core.sqlite")
		s, err := OpenWith(path, Options{Now: newClock().now, PIDAlive: alwaysAlive, Exclusive: excl})
		mustT(t, err)
		_, err = s.Acquire(testIdent(1))
		mustT(t, err)
		drain := func() int {
			n := 0
			for {
				select {
				case <-s.Changes():
					n++
				default:
					return n
				}
			}
		}
		drain()
		if err := s.Update(func(tx *Tx) error { return errors.New("no") }); err == nil {
			t.Fatal("rollback reported success")
		}
		if n := drain(); n != 0 {
			t.Fatalf("exclusive=%v: %d signals after a rollback", excl, n)
		}
		for i := 0; i < 3; i++ {
			mustT(t, s.Update(func(tx *Tx) error { return tx.SetMeta("k", strconv.Itoa(i)) }))
		}
		if n := drain(); n != 1 {
			t.Fatalf("exclusive=%v: %d signals after 3 commits, want 1 (coalesced)", excl, n)
		}
		mustT(t, s.Close())
	}
}

// short sets the busy timeout to ms for one test.
func short(t *testing.T, ms int) {
	old := busyTimeoutMS
	busyTimeoutMS = ms
	t.Cleanup(func() { busyTimeoutMS = old })
}

// A process that dies with the store open in exclusive mode leaves the
// database and its WAL, and no -shm. Its acknowledged commits survive both
// a copy by a successor (CopySnapshot) and a reopen in place.
func TestExclusiveModeCrashKeepsEveryCommit(t *testing.T) {
	if dir := os.Getenv("STORE_EXCL_CRASH_DIR"); dir != "" {
		// The child: commit, report, die without closing.
		s, err := OpenWith(filepath.Join(dir, "core.sqlite"), Options{Exclusive: true})
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
	// A command on the same host while no helper runs: reads the files
	// (here a raw copy, so the in-place reopen below still finds the WAL)
	// read-only, recovering the WAL index from the WAL.
	rawDir := t.TempDir()
	if _, err := copyFiles(src, filepath.Join(rawDir, "core.sqlite")); err != nil {
		t.Fatal(err)
	}
	ro, err := OpenWith(filepath.Join(rawDir, "core.sqlite"), Options{ReadOnly: true, Exclusive: true})
	mustT(t, err)
	check(ro, "read-only")
	mustT(t, ro.Close())
	if _, err := os.Stat(filepath.Join(rawDir, "core.sqlite-shm")); err == nil {
		t.Fatal("the read-only open created a -shm file")
	}
	// Another host: copies the raw files and opens the copy.
	dst := filepath.Join(dir, "core.g2.sqlite")
	mustT(t, CopySnapshot(src, dst))
	c := openExclusive(t, dst)
	check(c, "copy")
	// The same host after the crash: opens the file in place.
	again, err := OpenWith(src, Options{PIDAlive: func(int) bool { return false }, Exclusive: true})
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
