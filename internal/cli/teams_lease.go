package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Takeover on a shared home (internal/core/lease, TAKEOVER-REPORT.md).
//
// Hosts that share the state directory agree on one owner through the
// lease files next to the store. A host opens the store with SQLite only
// while it holds the lease, or, for a read-only command, when the store
// file in use belongs to its own host: SQLite's WAL index is shared memory
// that does not work across hosts. A helper that is refused stands by
// (service) or exits (foreground); a standby takes over once the owner's
// lease has not been renewed for the stale bound, copying the store into a
// file of its own when it comes from another host.

// teamsLeaseTiming is the production lease timing, or the seam's: the
// Takeover knob sets the stale bound (every other interval scales with it)
// and RefusalRecheck the standby's poll.
func teamsLeaseTiming(seam *teamsTestSeam) lease.Timing {
	t := lease.Production()
	if seam == nil {
		return t
	}
	if d := seam.overrides["Takeover"]; d > 0 {
		t = lease.Scaled(d)
	}
	if d := seam.overrides["RefusalRecheck"]; d > 0 && d < t.Stale {
		t.Poll = d
	}
	return t
}

// teamsAcquireLease waits for (service child) or tries to take (foreground)
// the store's lease. A service child reports each reason to wait once; a
// foreground start says so when it waits for an owner's lease to run out.
func teamsAcquireLease(ctx context.Context, anchor, host string, timing lease.Timing, wait bool, errOut io.Writer) (*lease.Holder, error) {
	var last string
	cfg := lease.Config{
		Anchor: anchor,
		Self:   lease.Local(host, teamsBootID()),
		Timing: timing,
		Wait:   wait,
		Report: func(s lease.Standby) {
			var reason string
			switch s.Reason {
			case lease.ReasonDisabled:
				reason = "the kill-switch file " + lease.DisabledFile + " exists"
			case lease.ReasonLive, lease.ReasonWatching:
				// Watching: not yet seen renewing. Either way the owner is
				// left alone until its lease stays unrenewed for Stale.
				reason = fmt.Sprintf("another helper owns the Teams state (%s pid %d); this host takes over if it stops renewing its lease for %s",
					firstNonEmptyString(s.Owner.Host, "unknown host"), s.Owner.PID, timing.Stale)
			case lease.ReasonClaimLost:
				// Another helper won the claim this host tried; its record
				// may not be written yet. Without this line a service child
				// that lost a simultaneous start says nothing until its
				// next check, a full Poll later.
				reason = fmt.Sprintf("another helper claimed the Teams state first; this host takes over if it stops renewing its lease for %s", timing.Stale)
			case lease.ReasonBatch:
				reason = fmt.Sprintf("this is a batch-job host; it leaves the takeover to an interactive host for %s", timing.BatchDefer)
			case lease.ReasonError:
				reason = fmt.Sprintf("the lease file cannot be read or claimed (%v)", s.Err)
			default:
				return
			}
			if reason == last {
				return
			}
			last = reason
			switch {
			case wait:
				_, _ = fmt.Fprintf(errOut, "Teams helper standing by: %s; checking again every %s\n", reason, timing.Poll)
			case s.Reason == lease.ReasonWatching:
				// A foreground start waits only for an owner that is not
				// seen renewing (it exits on one that is): say why it is
				// silent.
				_, _ = fmt.Fprintf(errOut, "Teams helper waiting: %s\n", reason)
			}
		},
	}
	return lease.Acquire(ctx, cfg)
}

// errTeamsStoreOnOtherHost: the store in use belongs to the helper of
// another host; only that host opens it.
type errTeamsStoreOnOtherHost struct{ host string }

func (e errTeamsStoreOnOtherHost) Error() string {
	return fmt.Sprintf("the Teams state is in use by the helper on %s; run this command there", e.host)
}

// teamsStoreView is what a read-only command may see of the store.
type teamsStoreView struct {
	Anchor string // the store's anchor (core.sqlite)
	DB     string // the store file in use
	Info   lease.Info
	// Remote is the host whose helper uses DB when it is not this host:
	// the command must not open it.
	Remote string
}

// teamsReadStoreView resolves the store a read-only command looks at.
func teamsReadStoreView() (teamsStoreView, error) {
	anchor, _, err := teamsNewCoreCLIStorePath()
	if err != nil {
		return teamsStoreView{}, err
	}
	return teamsStoreViewAt(anchor)
}

func teamsStoreViewAt(anchor string) (teamsStoreView, error) {
	v := teamsStoreView{Anchor: anchor, DB: anchor}
	info, err := lease.Read(anchor)
	if err != nil {
		return v, fmt.Errorf("Teams lease: %w", err)
	}
	v.Info = info
	if !info.Exists {
		return v, nil
	}
	v.DB = info.DB
	host, _ := teamsHostname()
	if !info.Readable(host) {
		v.Remote = info.DBHost()
	}
	return v, nil
}

// exists reports whether there is a store to read.
func (v teamsStoreView) exists() bool { return v.Info.Exists || teamsFileExists(v.DB) }

// openReadOnly opens the store file in use, read-only, when this host may.
func (v teamsStoreView) openReadOnly() (*corestore.Store, error) {
	if v.Remote != "" {
		return nil, errTeamsStoreOnOtherHost{host: v.Remote}
	}
	return corestore.OpenWith(v.DB, corestore.Options{ReadOnly: true})
}

// ownerLine is the status line for the lease's owner. The heartbeat age
// is from the owner's clock (display only).
func (v teamsStoreView) ownerLine(now time.Time) string {
	in := v.Info
	switch {
	case in.HeadGen == 0:
		return "Owner: none (never started)"
	case !in.HeadOK:
		return fmt.Sprintf("Owner: unknown (lease generation %d is not readable by this version)", in.HeadGen)
	case in.Head.State == lease.Released:
		return fmt.Sprintf("Owner: none (released by %s pid %d)", in.Head.Host, in.Head.PID)
	}
	age := in.Age(now).Round(time.Second)
	if age < 0 {
		age = 0
	}
	state := "live"
	if !in.Live(now, lease.ProductionStale) {
		state = "stale"
	}
	line := fmt.Sprintf("Owner: %s pid %d, heartbeat %s ago (%s)", in.Head.Host, in.Head.PID, age, state)
	if in.Head.State == lease.Claimed {
		line += ", taking over"
	}
	return line
}

// teamsWithLeasedStore runs fn on the store as its owner, for a command
// that changes the configuration: it takes the lease only when that is
// possible at once and the store can be used in place (no helper owns it,
// the last one ran on this host), and returns errTeamsHelperRunning
// otherwise, so the change goes through the running (or next) helper.
func teamsWithLeasedStore(ctx context.Context, anchor string, fn func(*corestore.Store) error) error {
	host, err := teamsHostname()
	if err != nil {
		return err
	}
	h, err := lease.Acquire(ctx, lease.Config{
		Anchor: anchor, Self: lease.Local(host, teamsBootID()), Timing: lease.Production(), Immediate: true,
	})
	switch {
	case errors.Is(err, lease.ErrOwnerLive):
		return errTeamsHelperRunning
	case errors.Is(err, lease.ErrDisabled):
		return fmt.Errorf("the Teams helper is disabled (%s exists next to %s)", lease.DisabledFile, anchor)
	case err != nil:
		return fmt.Errorf("Teams lease: %w", err)
	}
	defer h.Release()
	if !h.Plan().InPlace {
		return errTeamsHelperRunning
	}
	if err := h.Activate(); err != nil {
		return fmt.Errorf("Teams lease: %w", err)
	}
	st, err := corestore.OpenWith(h.Plan().DB, corestore.Options{Lease: h})
	if err != nil {
		return fmt.Errorf("open Teams core store: %w", err)
	}
	defer st.Close()
	if _, err := st.Acquire(corestore.Ident{Host: host, PID: os.Getpid(), BootID: teamsBootID()}); err != nil {
		if errors.Is(err, corestore.ErrOwnerLive) || errors.Is(err, corestore.ErrLocked) {
			return errTeamsHelperRunning
		}
		return fmt.Errorf("acquire Teams core store: %w", err)
	}
	defer st.Release()
	return fn(st)
}

// teamsLeaseLostError explains why a running helper stopped on losing its
// lease.
func teamsLeaseLostError(err error, anchor string) error {
	switch {
	case errors.Is(err, lease.ErrFenced):
		return fmt.Errorf("Teams helper fenced: another helper took over the Teams state (%v); stopped", err)
	}
	return err
}

// teamsHostEqual compares host names.
func teamsHostEqual(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}
