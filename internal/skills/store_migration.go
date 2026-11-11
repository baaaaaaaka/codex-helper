package skills

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func migrateSkillStore(db *sql.DB, dir string) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version == 1 {
		return sqlitestate.CommitSchema(tx, db)
	}
	if version != 0 {
		return fmt.Errorf("unsupported skills database version %d", version)
	}
	for _, group := range [][]string{skillStoreSchema, skillReviewSchema, skillReportSchema, {`CREATE TABLE skill_imports(source_path TEXT PRIMARY KEY)`}} {
		for _, q := range group {
			if _, err = tx.Exec(q); err != nil {
				return err
			}
		}
	}
	source, err := tx.Prepare(strings.Split(skill_sourcesPut, " ON CONFLICT")[0])
	if err != nil {
		return err
	}
	defer source.Close()
	state, err := tx.Prepare(strings.Split(skill_statesPut, " ON CONFLICT")[0])
	if err != nil {
		return err
	}
	defer state.Close()
	installed, err := tx.Prepare(`INSERT INTO installed_skills(source_id,position,` + installedCols + `) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer installed.Close()
	files, err := tx.Prepare(`INSERT INTO installed_files(source_id,skill_position,position,` + manifestFileCols + `) VALUES(?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer files.Close()
	var imported []string
	for _, name := range []string{ConfigFilename, StateFilename} {
		path := filepath.Join(dir, name)
		err = streamSkillLegacySources(path, func(raw json.RawMessage) error {
			if name == ConfigFilename {
				var v Source
				if err := json.Unmarshal(raw, &v); err != nil {
					return err
				}
				_, err := source.Exec(skill_sourcesArgs(v)...)
				return err
			}
			var v SourceState
			if err := json.Unmarshal(raw, &v); err != nil {
				return err
			}
			if _, err := state.Exec(skill_statesArgs(v)...); err != nil {
				return err
			}
			return insertInstalledRows(installed, files, v)
		})
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		imported = append(imported, path)
	}
	path := filepath.Join(dir, "teams-skill-push-reviews.json")
	if f, e := os.Open(path); e == nil {
		d := json.NewDecoder(f)
		if err = skillLegacyToken(d, '{'); err != nil {
			f.Close()
			return err
		}
		for d.More() {
			key, e := d.Token()
			if e != nil {
				f.Close()
				return e
			}
			chat, ok := key.(string)
			if !ok {
				f.Close()
				return fmt.Errorf("invalid skill review key")
			}
			var v PushReview
			if err = d.Decode(&v); err != nil {
				f.Close()
				return err
			}
			if v.Chat != chat {
				f.Close()
				return fmt.Errorf("skill review key mismatch %q", chat)
			}
			if err = insertSkillReview(tx, v); err != nil {
				f.Close()
				return err
			}
		}
		err = skillLegacyToken(d, '}')
		if err == nil {
			err = skillLegacyEOF(d)
		}
		f.Close()
		if err != nil {
			return err
		}
		imported = append(imported, path)
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	path = filepath.Join(dir, MigrationStatusFilename)
	if f, e := os.Open(path); e == nil {
		d := json.NewDecoder(f)
		var v *MigrationReport
		err = d.Decode(&v)
		if err == nil && v == nil {
			err = fmt.Errorf("skill report must be an object")
		}
		if err == nil {
			err = skillLegacyEOF(d)
		}
		f.Close()
		if err != nil {
			return err
		}
		if err = insertSkillReport(tx, *v); err != nil {
			return err
		}
		imported = append(imported, path)
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	for _, path := range imported {
		if _, err = tx.Exec(`INSERT INTO skill_imports(source_path) VALUES(?)`, path); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	for _, path := range imported {
		if _, err = os.Lstat(path + ".migrated"); err == nil {
			return fmt.Errorf("skills migration committed; archive already exists: %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err = os.Rename(path, path+".migrated"); err != nil {
			return fmt.Errorf("skills migration committed; archive: %w", err)
		}
	}
	return nil
}

func streamSkillLegacySources(path string, visit func(json.RawMessage) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	if err = skillLegacyToken(d, '{'); err != nil {
		return err
	}
	seen := map[string]bool{}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return fmt.Errorf("duplicate or invalid skill field %v", key)
		}
		seen[name] = true
		switch name {
		case "version":
			var version int
			if err = d.Decode(&version); err != nil {
				return err
			}
			if version < 0 || version > 1 {
				return fmt.Errorf("unsupported skill subscriptions version %d", version)
			}
		case "sources":
			token, err := d.Token()
			if err != nil {
				return err
			}
			if token == nil {
				continue
			}
			if token != json.Delim('[') {
				return fmt.Errorf("invalid skill sources")
			}
			for d.More() {
				var raw json.RawMessage
				if err = d.Decode(&raw); err != nil {
					return err
				}
				var id struct {
					ID string `json:"id"`
				}
				if err = json.Unmarshal(raw, &id); err != nil {
					return err
				}
				if id.ID == "" {
					return fmt.Errorf("empty or duplicate skill source %q", id.ID)
				}
				if err = visit(raw); err != nil {
					return err
				}
			}
			if err = skillLegacyToken(d, ']'); err != nil {
				return err
			}
		default:
			var ignored json.RawMessage
			if err = d.Decode(&ignored); err != nil {
				return err
			}
		}
	}
	if err = skillLegacyToken(d, '}'); err != nil {
		return err
	}
	return skillLegacyEOF(d)
}

func skillLegacyToken(d *json.Decoder, want byte) error {
	v, err := d.Token()
	if err != nil {
		return err
	}
	if v != json.Delim(want) {
		return fmt.Errorf("expected skill legacy delimiter %q", want)
	}
	return nil
}
func skillLegacyEOF(d *json.Decoder) error {
	var extra any
	err := d.Decode(&extra)
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing skill legacy input: %v", err)
	}
	return nil
}

func migrateExportManifest(path string) error {
	legacy := filepath.Join(filepath.Dir(path), legacyManifestFilename)
	f, err := os.Open(legacy)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	d := json.NewDecoder(f)
	var v *exportManifest
	err = d.Decode(&v)
	if err == nil && v == nil {
		err = fmt.Errorf("skill manifest must be an object")
	}
	if err == nil && (v.Version < 0 || v.Version > 1) {
		err = fmt.Errorf("unsupported skill manifest version %d", v.Version)
	}
	if err == nil {
		err = skillLegacyEOF(d)
	}
	f.Close()
	if err != nil {
		return err
	}
	if err = publishManifest(path, *v, false); errors.Is(err, os.ErrExist) {
		return nil
	} else if err != nil {
		return err
	}
	if _, err = os.Lstat(legacy + ".migrated"); err == nil {
		return fmt.Errorf("manifest migration committed; archive already exists")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = os.Rename(legacy, legacy+".migrated"); err != nil {
		return fmt.Errorf("manifest migration committed; archive: %w", err)
	}
	return nil
}
