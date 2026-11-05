package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/delegate"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
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

// writeTeamsCoreStoreStatus prints the store's location and state.
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
	if view.Remote != "" {
		// Only the host whose helper uses the store opens it.
		_, _ = fmt.Fprintln(out, view.ownerLine(now))
		_, _ = fmt.Fprintf(out, "Store details: in use by the helper on %s (run `cxp teams status` there); this host stands by\n", view.Remote)
		return
	}
	if !teamsFileExists(path) {
		_, _ = fmt.Fprintln(out, view.ownerLine(now))
		return
	}
	st, err := view.openReadOnly()
	if err != nil {
		_, _ = fmt.Fprintf(out, "Store unreadable: %v\n", err)
		return
	}
	defer st.Close()
	lines, err := teamsNewCoreStatusLines(st, view, now)
	for _, l := range lines {
		_, _ = fmt.Fprintln(out, l)
	}
	if err != nil {
		_, _ = fmt.Fprintf(out, "Store read error: %v\n", err)
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
	_, _ = fmt.Fprintf(out, "Version: %s\n", buildVersion())
	cfg, err := chteams.ResolveAuthConfig("")
	switch {
	case err != nil:
		_, _ = fmt.Fprintf(out, "Auth: not ready (%v)\n", err)
	default:
		_, _ = fmt.Fprintf(out, "Auth: %s\n", teamsTokenCacheStatus(cfg.CachePath, now))
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

func teamsNewCoreStatusLines(st *corestore.Store, view teamsStoreView, now time.Time) ([]string, error) {
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	o, ok, err := st.OwnerInfo()
	if err != nil {
		return lines, err
	}
	switch {
	case view.Info.Exists:
		add("%s", view.ownerLine(now))
	case !ok || o.Generation == 0:
		add("Owner: none (never started)")
	case o.HeartbeatAt == 0:
		add("Owner: none (released by %s pid %d)", o.Host, o.PID)
	default:
		age := now.Sub(time.UnixMilli(o.HeartbeatAt)).Round(time.Second)
		state := "live"
		if age > corestore.OwnerStaleAfter {
			state = "stale"
		}
		add("Owner: %s pid %d, heartbeat %s ago (%s)", o.Host, o.PID, age, state)
	}
	s, err := engine.ReadStoreStatus(st)
	if err != nil {
		return lines, err
	}
	add("Running version: %s", firstNonEmptyString(s.RunningVersion, "unknown"))
	switch {
	case s.UpdateInstalled != "":
		add("Helper update: %s installed, pending the restart that activates it", s.UpdateInstalled)
	case s.UpdateScheduled != "":
		add("Helper update: %s scheduled, waiting for running Codex work", s.UpdateScheduled)
	default:
		add("Helper update: none pending")
	}
	if s.CodexUpgrade {
		add("Codex CLI upgrade: scheduled, waiting for running Codex work")
	}
	convs, err := st.Conversations()
	if err != nil {
		return lines, err
	}
	var work, active, parked int
	for _, c := range convs {
		if c.Kind != corestore.KindWork {
			continue
		}
		work++
		switch c.State {
		case corestore.ConvActive:
			active++
		case corestore.ConvParked:
			parked++
		}
	}
	add("Sessions: %d (%d active, %d parked)", work, active, parked)
	tc, err := st.TurnCounts()
	if err != nil {
		return lines, err
	}
	add("Requests: %d queued, %d running", tc[corestore.TurnQueued], tc[corestore.TurnStarting]+tc[corestore.TurnRunning])
	oc, err := st.OutboxCounts()
	if err != nil {
		return lines, err
	}
	add("Unsent messages: %d queued, %d sending, %d unconfirmed", oc[corestore.OutQueued], oc[corestore.OutSending], oc[corestore.OutUnknown])
	wf, _, err := st.Meta(migrate.MetaWorkflowEnabled)
	if err != nil {
		return lines, err
	}
	if strings.EqualFold(strings.TrimSpace(wf), "true") {
		add("Workflow cards: on")
	} else {
		add("Workflow cards: off (owner @mentions)")
	}
	if id, _, err := st.Meta(delegate.MetaMachineID); err != nil {
		return lines, err
	} else if strings.TrimSpace(id) != "" {
		add("Registry machine: %s", strings.TrimSpace(id))
	}
	return lines, nil
}

func teamsFileExists(p string) bool { _, err := os.Stat(p); return err == nil }
