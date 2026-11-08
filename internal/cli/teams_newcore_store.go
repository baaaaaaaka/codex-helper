package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

var migratedMarkerRE = regexp.MustCompile(`^migrated to (.+) at \S+ by cxp teams migrate`)

// errTeamsStoreNotConverted: the store under the state root is from an
// earlier helper version and is converted when the helper next starts.
type errTeamsStoreNotConverted struct{ source string }

func (e errTeamsStoreNotConverted) Error() string {
	return fmt.Sprintf("the Teams store at %s is from an earlier helper version; the helper converts it when it starts (`cxp teams run` or the background service)", e.source)
}

// resolveTeamsCoreStorePath finds the core.sqlite the helper runs on, for
// commands that only read or configure it: a store still to be converted
// is an errTeamsStoreNotConverted.
func resolveTeamsCoreStorePath(stateDir string) (string, error) {
	path, err := planTeamsCoreStore(stateDir)
	if err != nil {
		return "", err
	}
	info, err := lease.Read(path)
	if err != nil {
		return "", err
	}
	if info.CurrentGen != 0 {
		return path, nil
	}
	from, err := teamsCoreStoreMigrationSource(info.DB)
	if err == nil && from != "" {
		return "", errTeamsStoreNotConverted{source: from}
	}
	return path, err
}

// planTeamsCoreStore selects the store anchor, including an unfinished
// first start's lease. Selection does not decide whether migration is done:
// prepareTeamsCoreStore makes that decision after acquiring the lease.
func planTeamsCoreStore(stateDir string) (path string, err error) {
	if p := strings.TrimSpace(os.Getenv(envTeamsCoreStore)); p != "" {
		p, err := filepath.Abs(p)
		return p, err
	}
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }
	var found []string
	add := func(p string) {
		for _, f := range found {
			if f == p {
				return
			}
		}
		found = append(found, p)
	}
	for _, dir := range migrate.Candidates(stateDir) {
		// A lease file means the store is managed there even while the
		// file in use has another name (a takeover's copy).
		if p := filepath.Join(dir, migrate.TargetName); exists(p) || exists(lease.Path(p)) {
			add(p)
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, migrate.MarkerName))
		if err != nil {
			continue
		}
		// Migrated with --out elsewhere: the marker names the target.
		if m := migratedMarkerRE.FindStringSubmatch(strings.TrimSpace(string(raw))); m != nil && exists(m[1]) {
			add(m[1])
		}
	}
	fresh := filepath.Join(stateDir, "teams", migrate.TargetName)
	if exists(fresh) || exists(lease.Path(fresh)) {
		add(fresh)
	}
	switch {
	case len(found) == 1:
		return found[0], nil
	case len(found) > 1:
		return "", fmt.Errorf("several Teams core stores exist (%s); set %s to the one to use", strings.Join(found, ", "), envTeamsCoreStore)
	}
	src, err := migrate.ChooseSource(migrate.Pending(stateDir))
	if err != nil {
		return "", err
	}
	if src != "" {
		return filepath.Join(src, migrate.TargetName), nil
	}
	return fresh, nil
}

// teamsCoreStoreMigrationSource reports the older store to convert when
// the actual database does not exist yet. A lease alone is not a database.
// This is also used by read-only commands; it never creates any files.
func teamsCoreStoreMigrationSource(db string) (string, error) {
	if _, err := os.Stat(db); err == nil {
		return "", nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect Teams core store: %w", err)
	}
	dir := filepath.Dir(db)
	if _, err := os.Stat(filepath.Join(dir, "state.json")); errors.Is(err, os.ErrNotExist) {
		return "", nil // a genuinely new install
	} else if err != nil {
		return "", fmt.Errorf("inspect older Teams store: %w", err)
	}
	return migrate.ChooseSource([]string{dir})
}

// openTeamsCoreStore publishes the lease only after the database is ready.
// In particular a fresh install must create its database before activation,
// so an interrupted open cannot publish a nonexistent predecessor.
func openTeamsCoreStore(ctx context.Context, holder *lease.Holder, sessionsDir string, log io.Writer) (*corestore.Store, error) {
	plan := holder.Plan()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := holder.Verify(); err != nil {
		return nil, teamsLeaseLostError(err, plan.DB)
	}
	if err := prepareTeamsCoreStore(ctx, plan, holder, sessionsDir, log); err != nil {
		return nil, err
	}
	// Copying or converting may take longer than the lease's validity.
	// OpenWith writes schema and WAL state, so fence before opening too.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := holder.Verify(); err != nil {
		return nil, teamsLeaseLostError(err, plan.DB)
	}
	st, err := corestore.OpenWith(plan.DB, corestore.Options{Lease: holder})
	if err != nil {
		return nil, fmt.Errorf("open Teams core store: %w", err)
	}
	if err := holder.Activate(); err != nil {
		_ = st.Close()
		return nil, teamsLeaseLostError(err, plan.DB)
	}
	return st, nil
}

// prepareTeamsCoreStore runs only while holding the lease, after WaitQuiet.
// It prepares the database selected by the lease, not a pre-lease guess:
// copy an existing predecessor, resume an unfinished migration, or leave a
// genuinely new install for OpenWith to create. A published store that is
// missing is an error; it must never be replaced with an empty database.
func prepareTeamsCoreStore(ctx context.Context, plan lease.Plan, owner corestore.Lease, sessionsDir string, log io.Writer) error {
	if !plan.InPlace {
		_, _ = fmt.Fprintf(log, "Teams helper copying the store %s to %s\n", plan.Src, plan.DB)
		if err := corestore.CopySnapshot(plan.Src, plan.DB); err != nil {
			return fmt.Errorf("take over the Teams core store: %w", err)
		}
		return nil
	}
	if plan.FromGen != 0 {
		if _, err := os.Stat(plan.DB); err != nil {
			return fmt.Errorf("previous Teams core store is unavailable; refusing to create an empty store: %w", err)
		}
		return nil
	}
	from, err := teamsCoreStoreMigrationSource(plan.DB)
	if err != nil || from == "" {
		return err
	}
	if plan.DB != filepath.Join(from, migrate.TargetName) {
		return fmt.Errorf("automatic Teams store conversion needs the target name %s; use `cxp teams migrate --source %s` and select its converted store", migrate.TargetName, from)
	}
	return migrateTeamsStoreAtStart(ctx, from, owner, sessionsDir, log)
}

// teamsMigrateRetry is the first wait after a failed conversion at start;
// it doubles up to teamsMigrateRetryMax. The helper waits in-process so a
// failing conversion of a large store is never repeated at the service
// manager's restart rate.
var (
	teamsMigrateRetry    = 30 * time.Second
	teamsMigrateRetryMax = 30 * time.Minute
)

// migrateTeamsStoreAtStart converts the older store at src into
// src/core.sqlite (read-only on the old store, which stays as the backup;
// the result is renamed into place only when complete). The conversion
// leaves a summary in the new store that the engine posts once.
func migrateTeamsStoreAtStart(ctx context.Context, src string, owner corestore.Lease, sessionsDir string, log io.Writer) error {
	wait := teamsMigrateRetry
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if owner != nil {
			if err := owner.Verify(); err != nil {
				return err
			}
		}
		if _, err := os.Stat(filepath.Join(src, migrate.TargetName)); err == nil {
			return nil // converted meanwhile (another start, or `cxp teams migrate`)
		}
		_, _ = fmt.Fprintf(log, "Converting the Teams store at %s (first start of this version; the old store is kept unchanged)\n", src)
		started := time.Now()
		rep, err := migrate.RunContext(ctx, migrate.Options{Source: src, CodexSessions: sessionsDir, CountBacklog: true, Lease: owner})
		if err == nil {
			_, _ = fmt.Fprintf(log, "Teams store converted in %s; report: %s\n", time.Since(started).Round(time.Second), filepath.Join(src, migrate.ReportName))
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, lease.ErrFenced) || errors.Is(err, lease.ErrReleased) {
			return err
		}
		if rep != nil {
			err = fmt.Errorf("%w (report: %s)", err, filepath.Join(src, migrate.ReportName))
		}
		_, _ = fmt.Fprintf(log, "Teams store conversion failed: %v; retrying in %s\n", err, wait)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		if wait *= 2; wait > teamsMigrateRetryMax {
			wait = teamsMigrateRetryMax
		}
	}
}
