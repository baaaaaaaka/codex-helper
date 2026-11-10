package asr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

// Legacy manifests are translated into a private database and published once.
// A native file, including a damaged one, is never replaced from legacy data.
func migrateInstallManifest(dir string) error {
	legacy := filepath.Join(dir, ".asr-install.json")
	info, err := os.Lstat(legacy)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("legacy ASR manifest is not regular: %s", legacy)
	}
	f, err := os.Open(legacy)
	if err != nil {
		return err
	}
	defer f.Close()
	tmp, err := os.CreateTemp(dir, ".asr-manifest-*.sqlite")
	if err != nil {
		return err
	}
	path := tmp.Name()
	if err = tmp.Close(); err != nil {
		return err
	}
	defer os.Remove(path)
	db, err := sqlitestate.Open(path, false)
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
	stmt, err := tx.Prepare(`INSERT INTO install_files VALUES(?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return fmt.Errorf("invalid ASR manifest object")
	}
	var asset, sum, installed string
	seen := map[string]bool{}
	position := 0
	for d.More() {
		keyToken, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok || seen[key] {
			return fmt.Errorf("duplicate ASR manifest field %v", keyToken)
		}
		seen[key] = true
		switch key {
		case "asset":
			err = d.Decode(&asset)
		case "sha256":
			err = d.Decode(&sum)
		case "installed":
			err = d.Decode(&installed)
		case "files":
			token, err = d.Token()
			if err == nil && token != json.Delim('[') {
				err = fmt.Errorf("invalid ASR file array")
			}
			if err != nil {
				return err
			}
			for d.More() {
				var file manifestFile
				if err = d.Decode(&file); err != nil {
					return err
				}
				if !filepath.IsLocal(filepath.FromSlash(file.Path)) {
					return fmt.Errorf("invalid ASR manifest path %q", file.Path)
				}
				if _, err = stmt.Exec(file.Path, position, file.Size, file.SHA256); err != nil {
					return err
				}
				position++
			}
			_, err = d.Token()
		default:
			return fmt.Errorf("unknown ASR manifest field %q", key)
		}
		if err != nil {
			return err
		}
	}
	if _, err = d.Token(); err != nil {
		return err
	}
	if _, err = d.Token(); err != io.EOF {
		return fmt.Errorf("trailing ASR manifest data")
	}
	if !seen["asset"] || !seen["sha256"] || !seen["installed"] || !seen["files"] || position == 0 {
		return fmt.Errorf("incomplete ASR install manifest")
	}
	if _, err = tx.Exec(`INSERT INTO install_manifest VALUES(1,?,?,?);`, asset, sum, installed); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO install_imports VALUES(?)`, legacy); err != nil {
		return err
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	if err = os.Link(path, filepath.Join(dir, manifestName)); err != nil {
		// A concurrent migrator may have published its complete database first.
		if os.IsExist(err) {
			return nil
		}
		return err
	}
	f.Close()
	return archiveASRLegacy(legacy)
}
