package store

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// CopySnapshot copies the store at src into a new store file at dst, for an
// owner that takes over from another host or from a stalled process.
//
// src is never opened with SQLite: its WAL index (-shm) is shared memory of
// the host that used it and means nothing here, so only the database and
// its WAL are copied, as raw bytes, and the -shm is left behind. The copy
// is repeated until two reads of both files agree (nothing wrote to them in
// between). The copy is then opened with SQLite, which rebuilds the WAL
// index from the WAL: frames after the last valid commit frame (a
// transaction the old owner had not finished, or WAL pages it never got to
// the server) fail their checksums and are ignored, exactly as after a
// power loss. The copy is checked, checkpointed into a single file, synced
// and renamed into place, so dst either does not exist or is complete.
//
// A missing src is an error: an owner never starts from an empty store
// when the lease says one exists.
func CopySnapshot(src, dst string) error {
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("store: copy source: %w", err)
	}
	tmp := dst + ".tmp"
	clean := func() {
		for _, s := range []string{"", "-wal", "-shm", "-journal"} {
			_ = os.Remove(tmp + s)
		}
	}
	clean()
	stable := false
	for attempt := 0; attempt < 10 && !stable; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}
		first, err := copyFiles(src, tmp)
		if err != nil {
			clean()
			return err
		}
		again, err := hashFiles(src)
		if err != nil {
			clean()
			return err
		}
		stable = first == again
	}
	if !stable {
		clean()
		return fmt.Errorf("store: %s kept changing while it was copied", src)
	}
	if err := finishCopy(tmp); err != nil {
		clean()
		return fmt.Errorf("store: copy of %s: %w", src, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		clean()
		return err
	}
	clean()
	return syncDir(filepath.Dir(dst))
}

type fileSums [2][sha256.Size + 1]byte // database, WAL (last byte: present)

// copyFiles copies src and src-wal to dst and dst-wal and returns their
// hashes.
func copyFiles(src, dst string) (fileSums, error) {
	var sums fileSums
	for i, suffix := range []string{"", "-wal"} {
		in, err := os.Open(src + suffix)
		if errors.Is(err, os.ErrNotExist) && suffix != "" {
			_ = os.Remove(dst + suffix)
			continue
		}
		if err != nil {
			return sums, err
		}
		out, err := os.OpenFile(dst+suffix, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			in.Close()
			return sums, err
		}
		h := sha256.New()
		_, err = io.Copy(io.MultiWriter(out, h), in)
		in.Close()
		if err == nil {
			err = out.Sync()
		}
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return sums, err
		}
		copy(sums[i][:], h.Sum(nil))
		sums[i][sha256.Size] = 1
	}
	return sums, nil
}

func hashFiles(src string) (fileSums, error) {
	var sums fileSums
	for i, suffix := range []string{"", "-wal"} {
		in, err := os.Open(src + suffix)
		if errors.Is(err, os.ErrNotExist) && suffix != "" {
			continue
		}
		if err != nil {
			return sums, err
		}
		h := sha256.New()
		_, err = io.Copy(h, in)
		in.Close()
		if err != nil {
			return sums, err
		}
		copy(sums[i][:], h.Sum(nil))
		sums[i][sha256.Size] = 1
	}
	return sums, nil
}

// finishCopy opens the copied files with SQLite (WAL recovery), checks
// them and folds the WAL into the database file.
func finishCopy(path string) error {
	db, err := sql.Open("sqlite", dsn(path, false))
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	var res string
	if err := db.QueryRow("PRAGMA quick_check").Scan(&res); err != nil {
		return err
	}
	if res != "ok" {
		return fmt.Errorf("quick_check: %s", res)
	}
	if err := checkVersion(db, true); err != nil {
		return err
	}
	var busy, logFrames, done int
	if err := db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &done); err != nil {
		return err
	}
	if busy != 0 {
		return errors.New("checkpoint did not finish")
	}
	if err := db.Close(); err != nil {
		return err
	}
	if fi, err := os.Stat(path + "-wal"); err == nil && fi.Size() > 0 {
		return errors.New("WAL left after the checkpoint")
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	_ = d.Sync() // not supported everywhere (Windows); the rename stands
	return nil
}
