package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/delegate"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// What read-only commands see of the helper's store (`cxp teams status`,
// `teams control`, the service bootstrap's control-chat lookup, upgrade
// checks, `teams workflow status`).
//
// While a helper holds the store (its lease is claimed or active, on this
// host or another) a command never opens the store file: on a network
// filesystem the helper holds the file's lock for its whole life, and on
// any filesystem the file may belong to another host. It reads the state
// the helper publishes instead: core-status.json next to the lease, which
// the helper rewrites (tmp + rename) after a commit changes what it holds.
// Nothing runs while the store does not change. The file names the lease
// generation that wrote it, so a command can tell a current file from one
// of an earlier helper, and says so when the helper has not published or
// is not renewing its lease.
//
// While no helper holds the store, the command opens the file itself,
// read-only, and closes it at once (on a network filesystem that takes the
// file's lock for that moment; a helper starting meanwhile waits for it).

const teamsStatusFileName = "core-status.json"

func teamsStatusFilePath(dir string) string { return filepath.Join(dir, teamsStatusFileName) }

// teamsStoreStatus is the store state the commands show.
type teamsStoreStatus struct {
	RunningVersion  string `json:"running_version,omitempty"`
	UpdateInstalled string `json:"update_installed,omitempty"`
	UpdateScheduled string `json:"update_scheduled,omitempty"`
	CodexUpgrade    bool   `json:"codex_upgrade,omitempty"`

	Sessions int `json:"sessions"`
	Active   int `json:"active"`
	Parked   int `json:"parked"`

	Queued  int `json:"queued"`  // requests
	Running int `json:"running"` // requests starting or running

	OutQueued  int `json:"out_queued"`
	OutSending int `json:"out_sending"`
	OutUnknown int `json:"out_unknown"`

	Workflow     bool   `json:"workflow,omitempty"`
	WorkflowFile string `json:"workflow_file,omitempty"` // as configured (relative: next to the store)
	MachineID    string `json:"machine_id,omitempty"`

	Control *teamsControlChat `json:"control,omitempty"`

	// The owner row, read only from the store itself: a store without a
	// lease (never run by a lease-holding helper) names its owner there.
	owner   corestore.Owner
	ownerOK bool
}

// teamsControlChat is the control chat the helper records.
type teamsControlChat struct {
	ChatID string `json:"chat_id"`
	Title  string `json:"title,omitempty"`
	URL    string `json:"url,omitempty"`
}

// teamsPublishedStatus is core-status.json.
type teamsPublishedStatus struct {
	V     int              `json:"v"`
	Gen   int64            `json:"gen"` // the lease generation that wrote it
	Host  string           `json:"host"`
	PID   int              `json:"pid"`
	At    int64            `json:"at"` // unix ms when State last changed
	State teamsStoreStatus `json:"state"`
}

// readTeamsStoreStatus reads the state from an open store.
func readTeamsStoreStatus(st *corestore.Store) (teamsStoreStatus, error) {
	var s teamsStoreStatus
	var err error
	if s.owner, s.ownerOK, err = st.OwnerInfo(); err != nil {
		return s, err
	}
	es, err := engine.ReadStoreStatus(st)
	if err != nil {
		return s, err
	}
	s.RunningVersion, s.UpdateInstalled, s.UpdateScheduled, s.CodexUpgrade = es.RunningVersion, es.UpdateInstalled, es.UpdateScheduled, es.CodexUpgrade
	convs, err := st.Conversations()
	if err != nil {
		return s, err
	}
	for _, c := range convs {
		switch {
		case c.Kind == corestore.KindControl && c.ExtChatID != "" && s.Control == nil:
			s.Control = &teamsControlChat{ChatID: c.ExtChatID, Title: c.Title, URL: c.ExtURL}
		case c.Kind == corestore.KindWork:
			s.Sessions++
			switch c.State {
			case corestore.ConvActive:
				s.Active++
			case corestore.ConvParked:
				s.Parked++
			}
		}
	}
	tc, err := st.TurnCounts()
	if err != nil {
		return s, err
	}
	s.Queued, s.Running = tc[corestore.TurnQueued], tc[corestore.TurnStarting]+tc[corestore.TurnRunning]
	oc, err := st.OutboxCounts()
	if err != nil {
		return s, err
	}
	s.OutQueued, s.OutSending, s.OutUnknown = oc[corestore.OutQueued], oc[corestore.OutSending], oc[corestore.OutUnknown]
	wf, _, err := st.Meta(migrate.MetaWorkflowEnabled)
	if err != nil {
		return s, err
	}
	s.Workflow = strings.EqualFold(strings.TrimSpace(wf), "true")
	file, _, err := st.Meta(migrate.MetaWorkflowSecretFile)
	if err != nil {
		return s, err
	}
	s.WorkflowFile = strings.TrimSpace(file)
	id, _, err := st.Meta(delegate.MetaMachineID)
	if err != nil {
		return s, err
	}
	s.MachineID = strings.TrimSpace(id)
	return s, nil
}

// workflowFile resolves the configured webhook file ("" when none) for the
// store in dir.
func (s teamsStoreStatus) workflowFile(dir string) string {
	file := s.WorkflowFile
	if file != "" && !filepath.IsAbs(file) {
		file = filepath.Join(dir, filepath.Base(file))
	}
	return file
}

func readTeamsPublishedStatus(dir string) (teamsPublishedStatus, bool) {
	// Opening the directory first revalidates it on NFS (close-to-open),
	// so a file the helper renamed into place on another host is seen.
	if d, err := os.Open(dir); err == nil {
		_ = d.Close()
	}
	var p teamsPublishedStatus
	raw, err := os.ReadFile(teamsStatusFilePath(dir))
	if err != nil || json.Unmarshal(raw, &p) != nil || p.V != 1 || p.Gen == 0 {
		return teamsPublishedStatus{}, false
	}
	return p, true
}

// teamsStatusPublisher is the helper's side: it keeps core-status.json
// equal to the store's state. It reads the store after commits
// (Store.Changes), no more often than every teamsStatusPublishGap, and
// rewrites the file only when the state changed. An idle store costs
// nothing: no commit, no read.
type teamsStatusPublisher struct {
	st   *corestore.Store
	dir  string
	gen  int64
	host string
	log  io.Writer

	last   []byte // the state last written
	failed string // the last error logged
}

// teamsStatusPublishGap folds the commits of a burst into one read.
const teamsStatusPublishGap = 250 * time.Millisecond

func newTeamsStatusPublisher(st *corestore.Store, dir string, gen int64, host string, log io.Writer) *teamsStatusPublisher {
	return &teamsStatusPublisher{st: st, dir: dir, gen: gen, host: host, log: log}
}

// publish reads the state and rewrites the file if it changed.
func (p *teamsStatusPublisher) publish(now time.Time) {
	s, err := readTeamsStoreStatus(p.st)
	if err == nil {
		body, _ := json.Marshal(s)
		if bytes.Equal(body, p.last) {
			return
		}
		err = writeTeamsJSONFile(teamsStatusFilePath(p.dir), teamsPublishedStatus{V: 1, Gen: p.gen, Host: p.host, PID: os.Getpid(), At: now.UnixMilli(), State: s})
		if err == nil {
			p.last, p.failed = body, ""
			return
		}
	}
	if msg := err.Error(); msg != p.failed && p.log != nil {
		p.failed = msg
		_, _ = fmt.Fprintf(p.log, "Teams helper: cannot publish its state for `cxp teams status`: %v\n", err)
	}
}

// run publishes at once, then after each commit, until ctx ends.
func (p *teamsStatusPublisher) run(ctx context.Context) {
	p.publish(time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.st.Changes():
		}
		p.publish(time.Now())
		if sleepContext(ctx, teamsStatusPublishGap) != nil {
			return
		}
	}
}

// helperHolds reports whether a helper holds the store per the lease: its
// head generation is claimed or active and its process is not known to
// have exited (a crash on this host).
func (v teamsStoreView) helperHolds() bool {
	in := v.Info
	switch {
	case !in.Exists || in.HeadGen == 0:
		return false
	case !in.HeadOK:
		return true // a record this version cannot read: assume it holds
	case in.Head.State != lease.Active && in.Head.State != lease.Claimed:
		return false
	}
	host, _ := teamsHostname()
	return !in.HeadGone(host, teamsBootID())
}

// errTeamsStateUnpublished: a helper holds the store, but the state it
// published is missing or not its own.
type errTeamsStateUnpublished struct{ reason string }

func (e errTeamsStateUnpublished) Error() string { return e.reason }

// teamsReadStoreState returns the store's state for a read-only command.
// note is set when the state may be out of date (and says why).
func teamsReadStoreState(v teamsStoreView, now time.Time) (s teamsStoreStatus, note string, err error) {
	if !v.helperHolds() {
		if v.Remote != "" {
			return teamsRemoteStoreState(v)
		}
		s, err := teamsReadStoreDirect(v)
		if !errors.Is(err, corestore.ErrBusy) {
			return s, "", err
		}
		// A helper took the store after the lease was read (or another
		// command holds it for a moment).
		again, rerr := teamsStoreViewAt(v.Anchor)
		if rerr != nil || !again.helperHolds() {
			return s, "", fmt.Errorf("the Teams store is in use by another process; retry in a few seconds (%v)", err)
		}
		v = again
	}
	return teamsPublishedState(v, now)
}

// teamsReadStoreDirect reads the store file itself (no helper holds it),
// holding it only while it reads.
func teamsReadStoreDirect(v teamsStoreView) (teamsStoreStatus, error) {
	if !teamsFileExists(v.DB) {
		return teamsStoreStatus{}, nil
	}
	st, err := corestore.OpenWith(v.DB, corestore.Options{ReadOnly: true})
	if err != nil {
		return teamsStoreStatus{}, err
	}
	s, err := readTeamsStoreStatus(st)
	if cerr := st.Close(); err == nil && cerr != nil {
		err = cerr
	}
	return s, err
}

// teamsPublishedState is the state the helper that holds the store
// published.
func teamsPublishedState(v teamsStoreView, now time.Time) (teamsStoreStatus, string, error) {
	in := v.Info
	p, ok := readTeamsPublishedStatus(filepath.Dir(v.Anchor))
	current := ok && p.Gen == in.HeadGen
	if !in.HeadOK {
		if current {
			return p.State, "", nil
		}
		return teamsStoreStatus{}, "", errTeamsStateUnpublished{"the helper that holds the Teams state has not published it in a form this version reads"}
	}
	who := fmt.Sprintf("the helper on %s (pid %d)", firstNonEmptyString(in.Head.Host, "an unknown host"), in.Head.PID)
	age := in.Age(now).Round(time.Second)
	if age < 0 {
		age = 0
	}
	live := in.Live(now, lease.ProductionStale)
	switch {
	case in.Head.State == lease.Claimed:
		return teamsStoreStatus{}, "", errTeamsStateUnpublished{who + " is taking over the Teams state and has not published it yet; retry in a few seconds"}
	case !current && !live:
		return teamsStoreStatus{}, "", errTeamsStateUnpublished{fmt.Sprintf("%s is not answering: it last renewed its lease %s ago and published no state", who, age)}
	case !current:
		return teamsStoreStatus{}, "", errTeamsStateUnpublished{who + " has not published the Teams state yet (it does within seconds of starting); if this persists, the helper is not answering"}
	case !live:
		return p.State, fmt.Sprintf("%s is not answering (it last renewed its lease %s ago); this is the state it published at %s",
			who, age, time.UnixMilli(p.At).Local().Format(time.RFC3339)), nil
	}
	return p.State, "", nil
}

// teamsRemoteStoreState: no helper holds the store, and its file belongs
// to another host, so only that host opens it. What the last helper
// published is shown instead, when there is any.
func teamsRemoteStoreState(v teamsStoreView) (teamsStoreStatus, string, error) {
	p, ok := readTeamsPublishedStatus(filepath.Dir(v.Anchor))
	if !ok {
		return teamsStoreStatus{}, "", errTeamsStoreOnOtherHost{host: v.Remote}
	}
	return p.State, fmt.Sprintf("the helper on %s (pid %d), which is not running now, published this state at %s",
		firstNonEmptyString(p.Host, v.Remote), p.PID, time.UnixMilli(p.At).Local().Format(time.RFC3339)), nil
}

// teamsStoredControlChat is the control chat the helper recorded, for
// `cxp teams control`, the service bootstrap and `teams doctor`.
func teamsStoredControlChat() (teamsControlChat, bool, error) {
	view, err := teamsReadStoreView()
	if err != nil {
		return teamsControlChat{}, false, err
	}
	if !view.exists() {
		return teamsControlChat{}, false, nil
	}
	s, _, err := teamsReadStoreState(view, time.Now())
	if err != nil || s.Control == nil {
		return teamsControlChat{}, false, err
	}
	return *s.Control, true, nil
}

// teamsStoreOwnerLine is the owner line of a store without a lease, from
// its owner row.
func teamsStoreOwnerLine(s teamsStoreStatus, now time.Time) string {
	o := s.owner
	switch {
	case !s.ownerOK || o.Generation == 0:
		return "Owner: none (never started)"
	case o.HeartbeatAt == 0:
		return fmt.Sprintf("Owner: none (released by %s pid %d)", o.Host, o.PID)
	}
	age := now.Sub(time.UnixMilli(o.HeartbeatAt)).Round(time.Second)
	state := "live"
	if age > corestore.OwnerStaleAfter {
		state = "stale"
	}
	return fmt.Sprintf("Owner: %s pid %d, heartbeat %s ago (%s)", o.Host, o.PID, age, state)
}

// teamsStatusDetailLines are the status lines after the owner and the
// last poll.
func teamsStatusDetailLines(s teamsStoreStatus, view teamsStoreView, now time.Time) []string {
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	add("Running version: %s", firstNonEmptyString(s.RunningVersion, "unknown"))
	if line := teamsPendingVersionLine(view, s.RunningVersion, now); line != "" {
		add("%s", line)
	}
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
	add("Sessions: %d (%d active, %d parked)", s.Sessions, s.Active, s.Parked)
	add("Requests: %d queued, %d running", s.Queued, s.Running)
	add("Unsent messages: %d queued, %d sending, %d unconfirmed", s.OutQueued, s.OutSending, s.OutUnknown)
	if s.Workflow {
		add("Workflow cards: on")
	} else {
		add("Workflow cards: off (owner @mentions)")
	}
	if s.MachineID != "" {
		add("Registry machine: %s", s.MachineID)
	}
	return lines
}
