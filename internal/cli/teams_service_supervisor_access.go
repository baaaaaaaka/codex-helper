package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

func readNativeSupervisorConfig(path string) (teamsServiceLocalSupervisorConfig, error) {
	native := serviceNativePath(path)
	if _, err := sqlitestate.Inspect(native); err != nil && !os.IsNotExist(err) {
		return teamsServiceLocalSupervisorConfig{}, err
	}
	if err := validateTeamsServiceLocalSupervisorRegularFile(native, "local supervisor config", false); err != nil {
		if !os.IsNotExist(err) {
			return teamsServiceLocalSupervisorConfig{}, err
		}
		legacy := strings.TrimSuffix(native, ".sqlite") + ".json"
		if _, err = os.Lstat(legacy); err != nil {
			return teamsServiceLocalSupervisorConfig{}, err
		}
		store, err := openTeamsSupervisorConfig(native)
		if err != nil {
			return teamsServiceLocalSupervisorConfig{}, err
		}
		defer store.db.Close()
		return store.Config()
	}
	db, err := sqlitestate.Open(native, true)
	if err != nil {
		return teamsServiceLocalSupervisorConfig{}, err
	}
	defer db.Close()
	var version int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return teamsServiceLocalSupervisorConfig{}, err
	}
	if version == 0 {
		db.Close()
		migrated, err := openTeamsSupervisorConfig(native)
		if err != nil {
			return teamsServiceLocalSupervisorConfig{}, err
		}
		defer migrated.db.Close()
		return migrated.Config()
	}
	if version != 1 {
		return teamsServiceLocalSupervisorConfig{}, fmt.Errorf("unsupported supervisor config database version %d", version)
	}
	return (&teamsSupervisorConfigDB{db: db}).Config()
}
func openTeamsServiceStateForRead() (*teamsServiceStateDB, error) {
	path, err := defaultTeamsServiceWatchdogStatePath()
	if err != nil {
		return nil, err
	}
	if _, err := sqlitestate.Inspect(path); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err = validateTeamsServiceLocalSupervisorRegularFile(path, "local supervisor state", false); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		sources, err := serviceWatchdogLegacySources(path)
		if err != nil {
			return nil, err
		}
		status, activation, err := legacyLocalSupervisorRuntimeSources()
		if err != nil {
			return nil, err
		}
		for _, legacy := range append(append(sources, status...), activation...) {
			if _, err = os.Lstat(legacy); err == nil {
				return openTeamsWatchdogState()
			} else if !os.IsNotExist(err) {
				return nil, err
			}
		}
		return nil, nil
	}
	db, err := sqlitestate.Open(path, true)
	if err != nil {
		return nil, err
	}
	var schema int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&schema); err != nil {
		db.Close()
		return nil, err
	}
	if schema == 0 {
		db.Close()
		return openTeamsWatchdogState()
	}
	if err = checkTeamsServiceState(db); err != nil {
		db.Close()
		return nil, err
	}
	host, err := teamsHostname()
	if err != nil {
		db.Close()
		return nil, err
	}
	return &teamsServiceStateDB{db: db, host: host}, nil
}
func deleteNativeSupervisorConfig(path string) error {
	native := serviceNativePath(path)
	if _, err := sqlitestate.Inspect(native); os.IsNotExist(err) {
		if _, err = os.Lstat(strings.TrimSuffix(native, ".sqlite") + ".json"); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	st, err := openTeamsSupervisorConfig(native)
	if err != nil {
		return err
	}
	defer st.db.Close()
	return st.Delete()
}
func deleteNativeSupervisorStatus() error {
	st, err := openTeamsServiceStateForRead()
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}
	st.Close()
	path, err := defaultTeamsServiceWatchdogStatePath()
	if err != nil {
		return err
	}
	db, err := sqlitestate.OpenExisting(path)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = checkTeamsServiceState(db); err != nil {
		return err
	}
	st.db = db
	return st.DeleteSupervisorStatus()
}
