package skills

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// Store owns one module database for subscriptions, reviews and migration
// reports. Export manifests travel with their independently published folder.
type Store struct {
	db   *sql.DB
	path string
}

func NewStore(configDir string) (*Store, error) {
	configDir = filepath.Clean(configDir)
	if configDir == "." || configDir == "" {
		return nil, fmt.Errorf("empty config dir")
	}
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(configDir, "skills.sqlite")
	db, err := sqlitestate.Open(path, false)
	if err != nil {
		return nil, err
	}
	if err = migrateSkillStore(db, configDir); err != nil {
		db.Close()
		return nil, err
	}

	return &Store{db: db, path: path}, nil
}
func (s *Store) Close() error       { return s.db.Close() }
func (s *Store) ConfigPath() string { return s.path }
func (s *Store) StatePath() string  { return s.path }
func (s *Store) transaction(write bool, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: !write})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func skillTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
func parseSkillTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

const skill_sourcesCols = `id,name,url,remote_url,provider,ref,path,target_kind,target_root,auto_sync,added_at,updated_at,last_added_by`
const skill_sourcesPut = `INSERT INTO skill_sources(id,name,url,remote_url,provider,ref,path,target_kind,target_root,auto_sync,added_at,updated_at,last_added_by) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,url=excluded.url,remote_url=excluded.remote_url,provider=excluded.provider,ref=excluded.ref,path=excluded.path,target_kind=excluded.target_kind,target_root=excluded.target_root,auto_sync=excluded.auto_sync,added_at=excluded.added_at,updated_at=excluded.updated_at,last_added_by=excluded.last_added_by WHERE (id,name,url,remote_url,provider,ref,path,target_kind,target_root,auto_sync,added_at,updated_at,last_added_by) IS NOT (excluded.id,excluded.name,excluded.url,excluded.remote_url,excluded.provider,excluded.ref,excluded.path,excluded.target_kind,excluded.target_root,excluded.auto_sync,excluded.added_at,excluded.updated_at,excluded.last_added_by)`

func skill_sourcesArgs(v Source) []any {
	return []any{v.ID, v.Name, v.URL, v.RemoteURL, v.Provider, v.Ref, v.Path, v.TargetKind, v.TargetRoot, v.AutoSync, skillTime(v.AddedAt), skillTime(v.UpdatedAt), v.LastAddedBy}
}
func scanSource(row interface{ Scan(...any) error }) (Source, error) {
	var v Source
	var added_at string
	var updated_at string
	err := row.Scan(&v.ID, &v.Name, &v.URL, &v.RemoteURL, &v.Provider, &v.Ref, &v.Path, &v.TargetKind, &v.TargetRoot, &v.AutoSync, &added_at, &updated_at, &v.LastAddedBy)
	if err != nil {
		return v, err
	}
	v.AddedAt, err = parseSkillTime(added_at)
	if err != nil {
		return v, err
	}
	v.UpdatedAt, err = parseSkillTime(updated_at)
	if err != nil {
		return v, err
	}
	return v, nil
}

const skill_statesCols = `id,status,last_sync_at,last_auto_sync_day,last_commit,last_error`
const skill_statesPut = `INSERT INTO skill_states(id,status,last_sync_at,last_auto_sync_day,last_commit,last_error) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,last_sync_at=excluded.last_sync_at,last_auto_sync_day=excluded.last_auto_sync_day,last_commit=excluded.last_commit,last_error=excluded.last_error WHERE (id,status,last_sync_at,last_auto_sync_day,last_commit,last_error) IS NOT (excluded.id,excluded.status,excluded.last_sync_at,excluded.last_auto_sync_day,excluded.last_commit,excluded.last_error)`

func skill_statesArgs(v SourceState) []any {
	return []any{v.ID, v.Status, skillTime(v.LastSyncAt), v.LastAutoSyncDay, v.LastCommit, v.LastError}
}
func scanSourceState(row interface{ Scan(...any) error }) (SourceState, error) {
	var v SourceState
	var last_sync_at string
	err := row.Scan(&v.ID, &v.Status, &last_sync_at, &v.LastAutoSyncDay, &v.LastCommit, &v.LastError)
	if err != nil {
		return v, err
	}
	v.LastSyncAt, err = parseSkillTime(last_sync_at)
	if err != nil {
		return v, err
	}
	return v, nil
}

const installedCols = `name,export_name,source_path,target_path`
const manifestFileCols = `rel_path,sha256,size,mode`

var skillStoreSchema = []string{
	`CREATE TABLE skill_sources(seq INTEGER PRIMARY KEY,id TEXT NOT NULL UNIQUE CHECK(id<>''),name TEXT NOT NULL,url TEXT NOT NULL,remote_url TEXT NOT NULL,provider TEXT NOT NULL,ref TEXT NOT NULL,path TEXT NOT NULL,target_kind TEXT NOT NULL,target_root TEXT NOT NULL,auto_sync INTEGER NOT NULL,added_at TEXT NOT NULL,updated_at TEXT NOT NULL,last_added_by TEXT NOT NULL)`,
	`CREATE TABLE skill_states(seq INTEGER PRIMARY KEY,id TEXT NOT NULL UNIQUE CHECK(id<>''),status TEXT NOT NULL,last_sync_at TEXT NOT NULL,last_auto_sync_day TEXT NOT NULL,last_commit TEXT NOT NULL,last_error TEXT NOT NULL)`,

	`CREATE TABLE installed_skills(source_id TEXT NOT NULL REFERENCES skill_states(id) ON DELETE CASCADE,position INTEGER NOT NULL,name TEXT NOT NULL,export_name TEXT NOT NULL,source_path TEXT NOT NULL,target_path TEXT NOT NULL,PRIMARY KEY(source_id,position))`,
	`CREATE TABLE installed_files(source_id TEXT NOT NULL,skill_position INTEGER NOT NULL,position INTEGER NOT NULL,rel_path TEXT NOT NULL,sha256 TEXT NOT NULL,size INTEGER NOT NULL,mode INTEGER NOT NULL,PRIMARY KEY(source_id,skill_position,position),FOREIGN KEY(source_id,skill_position) REFERENCES installed_skills(source_id,position) ON DELETE CASCADE)`,
}

func readSkillState(tx *sql.Tx, id string) (SourceState, error) {
	v, err := scanSourceState(tx.QueryRow(`SELECT `+skill_statesCols+` FROM skill_states WHERE id=?`, id))
	if err != nil {
		return v, err
	}
	rows, err := tx.Query(`SELECT `+installedCols+` FROM installed_skills WHERE source_id=? ORDER BY position`, id)
	if err != nil {
		return v, err
	}
	for rows.Next() {
		var skill InstalledSkill
		if err = rows.Scan(&skill.Name, &skill.ExportName, &skill.SourcePath, &skill.TargetPath); err != nil {
			rows.Close()
			return v, err
		}
		v.InstalledSkills = append(v.InstalledSkills, skill)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return v, err
	}
	rows, err = tx.Query(`SELECT skill_position,`+manifestFileCols+` FROM installed_files WHERE source_id=? ORDER BY skill_position,position`, id)
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var i int
		var f FileManifest
		if err = rows.Scan(&i, &f.RelPath, &f.SHA256, &f.Size, &f.Mode); err != nil {
			return v, err
		}
		if i < 0 || i >= len(v.InstalledSkills) {
			return v, fmt.Errorf("invalid skill position %d", i)
		}
		v.InstalledSkills[i].Files = append(v.InstalledSkills[i].Files, f)
	}
	if err = rows.Err(); err != nil {
		return v, err
	}

	return v, nil
}
func (s *Store) SourceState(id string) (SourceState, bool, error) {
	var v SourceState
	err := s.transaction(false, func(tx *sql.Tx) error { var err error; v, err = readSkillState(tx, id); return err })
	if err == sql.ErrNoRows {
		return v, false, nil
	}
	return v, err == nil, err
}
func readSkillConfig(tx *sql.Tx) (Config, error) {
	cfg := Config{Version: configVersion}
	rows, err := tx.Query(`SELECT ` + skill_sourcesCols + ` FROM skill_sources ORDER BY seq`)
	if err != nil {
		return cfg, err
	}
	defer rows.Close()
	for rows.Next() {
		v, err := scanSource(rows)
		if err != nil {
			return cfg, err
		}
		cfg.Sources = append(cfg.Sources, v)
	}
	return cfg, rows.Err()
}
func readSkillStates(tx *sql.Tx) (State, error) {
	st := State{Version: stateVersion}
	rows, err := tx.Query(`SELECT id FROM skill_states ORDER BY seq`)
	if err != nil {
		return st, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return st, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return st, err
	}
	for _, id := range ids {
		v, err := readSkillState(tx, id)
		if err != nil {
			return st, err
		}
		st.Sources = append(st.Sources, v)
	}
	return st, nil
}
func (s *Store) LoadConfig() (Config, error) {
	var cfg Config
	err := s.transaction(false, func(tx *sql.Tx) error { var err error; cfg, err = readSkillConfig(tx); return err })
	return cfg, err
}
func (s *Store) LoadState() (State, error) {
	var st State
	err := s.transaction(false, func(tx *sql.Tx) error { var err error; st, err = readSkillStates(tx); return err })
	return st, err
}
func putSkillState(tx *sql.Tx, v SourceState) error {
	old, err := readSkillState(tx, v.ID)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if _, err = tx.Exec(skill_statesPut, skill_statesArgs(v)...); err != nil {
		return err
	}
	same := len(old.InstalledSkills) == len(v.InstalledSkills)
	if same {
		for i, a := range old.InstalledSkills {
			b := v.InstalledSkills[i]
			if a.Name != b.Name || a.ExportName != b.ExportName || a.SourcePath != b.SourcePath || a.TargetPath != b.TargetPath || !slices.Equal(a.Files, b.Files) {
				same = false
				break
			}
		}
	}
	if same {
		return nil
	}
	if _, err = tx.Exec(`DELETE FROM installed_skills WHERE source_id=?`, v.ID); err != nil {
		return err
	}
	return insertInstalledSkills(tx, v)
}
func insertInstalledSkills(tx *sql.Tx, v SourceState) error {
	skills, err := tx.Prepare(`INSERT INTO installed_skills(source_id,position,` + installedCols + `) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer skills.Close()
	files, err := tx.Prepare(`INSERT INTO installed_files(source_id,skill_position,position,` + manifestFileCols + `) VALUES(?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer files.Close()
	return insertInstalledRows(skills, files, v)
}
func insertInstalledRows(skills, files *sql.Stmt, v SourceState) error {
	for i, sk := range v.InstalledSkills {
		if _, err := skills.Exec(v.ID, i, sk.Name, sk.ExportName, sk.SourcePath, sk.TargetPath); err != nil {
			return err
		}
		for j, f := range sk.Files {
			if _, err := files.Exec(v.ID, i, j, f.RelPath, f.SHA256, f.Size, f.Mode); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) PutSubscription(source Source, state SourceState) error {
	return s.transaction(true, func(tx *sql.Tx) error {
		if _, err := tx.Exec(skill_sourcesPut, skill_sourcesArgs(source)...); err != nil {
			return err
		}
		return putSkillState(tx, state)
	})
}
func (s *Store) SetSyncing(id string) error {
	return s.transaction(true, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO skill_states(id,status,last_sync_at,last_auto_sync_day,last_commit,last_error) VALUES(?,'syncing','','','','') ON CONFLICT(id) DO UPDATE SET status='syncing',last_error='' WHERE status<>'syncing' OR last_error<>''`, id)
		return err
	})
}
func (s *Store) SaveSync(v SourceState) error {
	return s.transaction(true, func(tx *sql.Tx) error {
		if v.LastAutoSyncDay == "" {
			err := tx.QueryRow(`SELECT last_auto_sync_day FROM skill_states WHERE id=?`, v.ID).Scan(&v.LastAutoSyncDay)
			if err != nil && err != sql.ErrNoRows {
				return err
			}
		}
		return putSkillState(tx, v)
	})
}
func (s *Store) SaveFailure(v SourceState) error {
	return s.transaction(true, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO skill_states(id,status,last_sync_at,last_auto_sync_day,last_commit,last_error) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,last_sync_at=excluded.last_sync_at,last_auto_sync_day=CASE WHEN excluded.last_auto_sync_day='' THEN last_auto_sync_day ELSE excluded.last_auto_sync_day END,last_error=excluded.last_error
 WHERE status IS NOT excluded.status OR last_sync_at IS NOT excluded.last_sync_at OR (excluded.last_auto_sync_day<>'' AND last_auto_sync_day IS NOT excluded.last_auto_sync_day) OR last_error IS NOT excluded.last_error`, skill_statesArgs(v)...)
		return err
	})
}
func (s *Store) DeleteSubscription(id string) error {
	return s.transaction(true, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM skill_sources WHERE id=?`, id); err != nil {
			return err
		}
		_, err := tx.Exec(`DELETE FROM skill_states WHERE id=?`, id)
		return err
	})
}
func (s *Store) Retarget(id, root string, installed []InstalledSkill) error {
	return s.transaction(true, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`UPDATE skill_sources SET target_kind=?,target_root=?,updated_at=? WHERE id=?`, TargetAgents, root, skillTime(nowUTC()), id); err != nil {
			return err
		}
		st, err := readSkillState(tx, id)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		st.ID = id
		if len(installed) > 0 {
			st.InstalledSkills = installed
		} else {
			for i := range st.InstalledSkills {
				st.InstalledSkills[i].TargetPath = filepath.Join(root, st.InstalledSkills[i].ExportName)
			}
		}
		if st.Status == "" || st.Status == StatusSyncing {
			st.Status = StatusReady
		}
		return putSkillState(tx, st)
	})
}

func (s *Store) LastAutoSyncDay(id string) (string, error) {
	var day string
	err := s.db.QueryRow(`SELECT last_auto_sync_day FROM skill_states WHERE id=?`, id).Scan(&day)
	if err == sql.ErrNoRows {
		err = nil
	}
	return day, err
}

func (s *Store) Source(id string) (Source, bool, error) {
	v, err := scanSource(s.db.QueryRow(`SELECT `+skill_sourcesCols+` FROM skill_sources WHERE id=?`, id))
	if err == sql.ErrNoRows {
		return v, false, nil
	}
	return v, err == nil, err
}
