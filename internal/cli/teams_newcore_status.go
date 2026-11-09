package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
)

// writeTeamsNewCoreStatus appends the helper's state to `teams service
// status`, read from its store without taking ownership: the owner and its
// heartbeat, the running and pending versions (R-17), conversations, turns,
// unsent messages, Workflow cards and the registry machine id.
func writeTeamsNewCoreStatus(out io.Writer, now time.Time) {
	_, _ = fmt.Fprintln(out, "\nTeams helper")
	if line := teamsStoppedLine(); line != "" {
		_, _ = fmt.Fprintln(out, line)
	}
	writeTeamsCoreStoreStatus(out, now)
}

// writeTeamsCoreStoreStatus prints the store's location and state. While a
// helper holds the store the state is what it published (it never opens
// the file then); otherwise the store is read directly
// (teams_newcore_state.go).
func writeTeamsCoreStoreStatus(out io.Writer, now time.Time) {
	view, err := teamsReadStoreView()
	if err != nil {
		_, _ = fmt.Fprintf(out, "Store: %v\n", err)
		return
	}
	path := view.DB
	if !view.exists() {
		_, _ = fmt.Fprintf(out, "Store: none yet (%s)\n", path)
		return
	}
	_, _ = fmt.Fprintf(out, "Store: %s\n", path)
	if report := filepath.Join(filepath.Dir(path), migrate.ReportName); teamsFileExists(report) {
		_, _ = fmt.Fprintf(out, "Conversion report: %s\n", report)
	}
	if view.Remote == "" && !view.helperHolds() && !teamsFileExists(path) {
		_, _ = fmt.Fprintln(out, view.ownerLine(now))
		return
	}
	// Read everything first: the store (when it is read directly) is
	// closed before the output, which may block, is written.
	s, note, err := teamsReadStoreState(view, now)
	var unpublished errTeamsStateUnpublished
	var remote errTeamsStoreOnOtherHost
	switch {
	case !view.Info.Exists && err != nil:
		_, _ = fmt.Fprintf(out, "Store unreadable: %v\n", err)
		return
	case !view.Info.Exists:
		_, _ = fmt.Fprintln(out, teamsStoreOwnerLine(s, now))
	default:
		_, _ = fmt.Fprintln(out, view.ownerLine(now))
		_, _ = fmt.Fprintln(out, teamsLastPollLine(view, now))
	}
	switch {
	case errors.As(err, &remote):
		// Only the host whose helper used the store opens it.
		_, _ = fmt.Fprintf(out, "Store details: in use by the helper on %s (run `cxp teams status` there); this host stands by\n", remote.host)
		return
	case errors.As(err, &unpublished):
		_, _ = fmt.Fprintf(out, "Store details: %v\n", err)
		return
	case err != nil:
		_, _ = fmt.Fprintf(out, "Store unreadable: %v\n", err)
		return
	}
	if note != "" {
		_, _ = fmt.Fprintf(out, "Store details: %s\n", note)
	}
	for _, l := range teamsStatusDetailLines(s, view, now) {
		_, _ = fmt.Fprintln(out, l)
	}
}

// printTeamsCoreStatus is `cxp teams status` (F-CLI-1): strictly
// read-only, local only (no Graph call), never message bodies or secrets.
// A helper stopped on all machines says so first, with how to start it.
func printTeamsCoreStatus(out io.Writer, now time.Time) error {
	if line := teamsStoppedLine(); line != "" {
		_, _ = fmt.Fprintln(out, line)
	}
	_, _ = fmt.Fprintln(out, "Teams helper")
	_, _ = fmt.Fprintf(out, "Installed version: %s\n", buildVersion())
	cfg, err := chteams.ResolveAuthConfig("")
	switch {
	case err != nil:
		_, _ = fmt.Fprintf(out, "Auth: not ready (%v)\n", err)
	default:
		_, _ = fmt.Fprintf(out, "Auth: %s\n", teamsTokenCacheStatus(cfg.CachePath, now))
		readCfg, readErr := chteams.ResolveReadAuthConfig("")
		if readErr != nil {
			_, _ = fmt.Fprintf(out, "Read auth: not ready (%v)\n", readErr)
		} else if filepath.Clean(readCfg.CachePath) != filepath.Clean(cfg.CachePath) {
			_, _ = fmt.Fprintf(out, "Read auth: %s\n", teamsTokenCacheStatus(readCfg.CachePath, now))
		}
	}
	writeTeamsCoreStoreStatus(out, now)
	return nil
}

// teamsTokenCacheStatus describes a token cache without its contents.
func teamsTokenCacheStatus(path string, now time.Time) string {
	tok, err := chteams.ReadTokenCache(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return "not logged in (run `cxp teams auth`)"
	case err != nil:
		return fmt.Sprintf("token cache unreadable (%v)", err)
	case tok.AccessToken == "" && tok.RefreshToken == "":
		return "not logged in (run `cxp teams auth`)"
	}
	exp := time.Unix(tok.ExpiresAt, 0)
	switch {
	case tok.ExpiresAt > 0 && exp.After(now):
		return fmt.Sprintf("logged in (access token valid for %s)", exp.Sub(now).Round(time.Minute))
	case tok.RefreshToken != "":
		return "logged in (access token expired; refreshed at the next use)"
	}
	return "expired (run `cxp teams auth`)"
}

func teamsFileExists(p string) bool { _, err := os.Stat(p); return err == nil }
