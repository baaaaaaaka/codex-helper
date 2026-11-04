package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/core/delegate"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// writeTeamsNewCoreStatus appends the new core's state to `teams service
// status`, read from its store without taking ownership: the owner and its
// heartbeat, the running and pending versions (R-17), conversations, turns,
// unsent messages, Workflow cards and the registry machine id. It writes
// nothing when the new core is neither selected nor has a store.
func writeTeamsNewCoreStatus(out io.Writer, now time.Time) {
	selected := teamsEnvSelectsNewCore(map[string]string{envTeamsCore: os.Getenv(envTeamsCore)})
	stateDir, err := appdirs.StateDir()
	if err != nil {
		if selected {
			_, _ = fmt.Fprintf(out, "\nTeams helper (new core): state directory: %v\n", err)
		}
		return
	}
	path, err := resolveTeamsCoreStorePath(stateDir)
	if err != nil {
		if selected {
			_, _ = fmt.Fprintf(out, "\nTeams helper (new core): %v\n", err)
		}
		return
	}
	if _, statErr := os.Stat(path); statErr != nil {
		if selected {
			_, _ = fmt.Fprintf(out, "\nTeams helper (new core): no store yet (%s)\n", path)
		}
		return
	}
	_, _ = fmt.Fprintf(out, "\nTeams helper (new core)\nStore: %s\n", path)
	st, err := corestore.OpenWith(path, corestore.Options{ReadOnly: true})
	if err != nil {
		_, _ = fmt.Fprintf(out, "Store unreadable: %v\n", err)
		return
	}
	defer st.Close()
	lines, err := teamsNewCoreStatusLines(st, now)
	for _, l := range lines {
		_, _ = fmt.Fprintln(out, l)
	}
	if err != nil {
		_, _ = fmt.Fprintf(out, "Store read error: %v\n", err)
	}
}

func teamsNewCoreStatusLines(st *corestore.Store, now time.Time) ([]string, error) {
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	o, ok, err := st.OwnerInfo()
	if err != nil {
		return lines, err
	}
	switch {
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
