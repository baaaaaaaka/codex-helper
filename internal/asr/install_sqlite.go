package asr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

const installDDL = `CREATE TABLE install_manifest(id INTEGER PRIMARY KEY CHECK(id=1),asset TEXT NOT NULL,sha256 TEXT NOT NULL,installed TEXT NOT NULL);
 CREATE TABLE install_files(path TEXT PRIMARY KEY,position INTEGER NOT NULL UNIQUE,size INTEGER NOT NULL CHECK(size>=0),sha256 TEXT NOT NULL);
 CREATE TABLE install_imports(source_path TEXT PRIMARY KEY);`

type manifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func openInstallManifest(dir string) (*sql.DB, error) {
	path := filepath.Join(dir, manifestName)
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		if err = migrateInstallManifest(dir); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(path, true)
	if err != nil {
		return nil, err
	}
	var version int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err == nil && version != 1 {
		err = fmt.Errorf("unsupported ASR install database version %d", version)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func verifyInstall(dir string, a Asset) error {
	db, err := openInstallManifest(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	var checksum string
	if err = db.QueryRow(`SELECT sha256 FROM install_manifest WHERE id=1`).Scan(&checksum); err != nil {
		return err
	}
	if !strings.EqualFold(checksum, a.SHA256) {
		return errors.New("install manifest does not match the pinned asset")
	}
	after := -1
	for {
		rows, err := db.Query(`SELECT position,path,size,sha256 FROM install_files WHERE position>? ORDER BY position LIMIT 128`, after)
		if err != nil {
			return err
		}
		var files []manifestFile
		for rows.Next() {
			var f manifestFile
			if err = rows.Scan(&after, &f.Path, &f.Size, &f.SHA256); err != nil {
				break
			}
			files = append(files, f)
		}
		readErr := rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if readErr != nil {
			return readErr
		}
		if len(files) == 0 {
			if after < 0 {
				return errors.New("empty ASR install manifest")
			}
			return nil
		}
		for _, f := range files {
			if !filepath.IsLocal(filepath.FromSlash(f.Path)) {
				return fmt.Errorf("invalid ASR install path %q", f.Path)
			}
			p := filepath.Join(dir, filepath.FromSlash(f.Path))
			info, err := os.Stat(p)
			if err != nil {
				return err
			}
			if info.Size() != f.Size {
				return fmt.Errorf("%s: size %d, manifest %d", f.Path, info.Size(), f.Size)
			}
			if f.Size <= hashOnUseLimit {
				if sum, err := fileSHA256(p); err != nil || sum != f.SHA256 {
					return fmt.Errorf("%s: checksum mismatch", f.Path)
				}
			}
		}
	}
}

// The staging directory is private and unpublished. Its metadata is closed
// before the existing atomic directory publication; no live writer is blocked
// while files are hashed, and a failed install never publishes partial rows.
func writeManifest(stage string, a Asset) error {
	db, err := sqlitestate.Open(filepath.Join(stage, manifestName), false)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(installDDL); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO install_manifest VALUES(1,?,?,?)`, a.Name, a.SHA256, time.Now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO install_files VALUES(?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	position := 0
	err = filepath.WalkDir(stage, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(stage, p)
		if err != nil {
			return err
		}
		if rel == manifestName || rel == manifestName+"-journal" {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		sum := a.SHA256
		if a.Format != FormatFile || rel != a.Name {
			if sum, err = fileSHA256(p); err != nil {
				return err
			}
		}
		_, err = stmt.Exec(filepath.ToSlash(rel), position, info.Size(), sum)
		position++
		return err
	})
	if err != nil {
		return err
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	return tx.Commit()
}
