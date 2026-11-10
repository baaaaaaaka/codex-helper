package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
)

// Commands that act through the running helper (gap batch G: `teams
// control --recreate`, `teams chat recreate`, `teams chat publish-history`,
// `teams send-file`). The command never opens the store a helper holds: it
// submits a row to the independent exchange database; the helper
// picks it up on its 5 s watcher tick (the one that applies Workflow
// changes), hands it to engine.CLIRequest and publishes result rows in the exchange database,
// which the command prints. A request left while no helper runs is run by
// the next one, on this host or (shared home) on another.

const teamsCLIRequestDirName = "cli-requests"

// teamsCLIRequestWait bounds how long a command waits for its result.
var teamsCLIRequestWait = 3 * time.Minute

// teamsCLIResultKeep is how long the helper keeps a result nobody read.
const teamsCLIResultKeep = time.Hour

func teamsCLIRequestDir(anchor string) string {
	return filepath.Join(filepath.Dir(anchor), teamsCLIRequestDirName)
}

func teamsCLIStageDir(dir, id string) string { return filepath.Join(dir, id+".d") }

func newTeamsCLIRequestID(now time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%d-%s", now.UnixMilli(), hex.EncodeToString(b[:]))
}

// teamsCLISubmission is a request a command is about to leave.
type teamsCLISubmission struct {
	req engine.CLIRequest
	// stage copies the command's inputs (send-file) into the request's
	// own directory before the request is visible to the helper.
	// It returns the staged file (CLIRequest.File).
	stage func(dir string) (string, error)
}

// submitTeamsCLIRequest leaves the request for the helper and prints its
// result (or that it runs later). The returned error is the request's.
func submitTeamsCLIRequest(out io.Writer, sub teamsCLISubmission) error {
	view, err := teamsReadStoreView()
	if err != nil {
		return err
	}
	if !view.exists() {
		return fmt.Errorf("the Teams helper has not run on this machine yet; start it first (`cxp teams run`)")
	}
	dir := teamsCLIRequestDir(view.Anchor)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	now := time.Now()
	sub.req.ID = newTeamsCLIRequestID(now)
	if sub.stage != nil {
		stage := teamsCLIStageDir(dir, sub.req.ID)
		if err := os.MkdirAll(stage, 0o700); err != nil {
			return err
		}
		file, err := sub.stage(stage)
		if err != nil {
			_ = os.RemoveAll(stage)
			return err
		}
		sub.req.File = file
	}
	mail, err := openTeamsExchange(view.Anchor, false)
	if err != nil {
		_ = os.RemoveAll(teamsCLIStageDir(dir, sub.req.ID))
		return err
	}
	defer mail.Close()
	if err = mail.Submit(sub.req); err != nil {
		_ = os.RemoveAll(teamsCLIStageDir(dir, sub.req.ID))
		return err
	}

	if !view.Info.Exists || !view.Info.Live(now, lease.ProductionStale) {
		_, _ = fmt.Fprintln(out, "The Teams helper is not running. The request is kept and runs when the helper starts (`cxp teams start`, or `cxp teams run --once` to run it once now).")
		return nil
	}
	_, _ = fmt.Fprintln(out, "Waiting for the running Teams helper...")
	deadline := now.Add(teamsCLIRequestWait)
	for time.Now().Before(deadline) {
		res, ok, err := mail.Result(sub.req.ID)
		if err != nil {
			return fmt.Errorf("read the helper's result: %w", err)
		}
		if ok {
			if err = mail.Ack(sub.req.ID); err != nil {
				return err
			}
			_ = os.RemoveAll(teamsCLIStageDir(dir, sub.req.ID))
			if res.Error != "" {
				return errors.New(res.Error)
			}
			for _, l := range res.Lines {
				_, _ = fmt.Fprintln(out, l)
			}
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	_, _ = fmt.Fprintln(out, "The running Teams helper has not finished the request yet; it continues in the background.")
	return nil
}

// teamsCLIRequests is the helper's side: it runs the requests it finds,
// one goroutine per request (the engine runs them one at a time).
type teamsCLIRequests struct {
	mail *teamsExchangeStore
	dir  string
	e    *engine.Engine
	ctx  context.Context
	log  io.Writer

	mu       sync.Mutex
	inflight map[string]bool
	logged   map[string]string
	wg       sync.WaitGroup
}

func newTeamsCLIRequests(ctx context.Context, anchor string, e *engine.Engine, log io.Writer, mail *teamsExchangeStore) *teamsCLIRequests {
	return &teamsCLIRequests{mail: mail, dir: teamsCLIRequestDir(anchor), e: e, ctx: ctx, log: log,
		inflight: map[string]bool{}, logged: map[string]string{}}
}

// tick starts the requests found and drops results nobody read. It runs
// on the helper's watcher tick (no timer of its own).
func (q *teamsCLIRequests) tick(now time.Time) {
	for {
		expired, err := q.mail.Expire(now.Add(-teamsCLIResultKeep))
		if err != nil {
			fmt.Fprintf(q.log, "Teams CLI exchange cleanup: %v\n", err)
			return
		}
		for _, id := range expired {
			_ = os.RemoveAll(teamsCLIStageDir(q.dir, id))
		}
		if len(expired) < 128 {
			break
		}
	}
	// Page through keys, closing each cursor before starting work. A running
	// first page cannot starve requests on later pages.
	after := ""
	for {
		ids, err := q.mail.IDs(after)
		if err != nil {
			fmt.Fprintf(q.log, "Teams CLI exchange read: %v\n", err)
			return
		}
		for _, id := range ids {
			q.start(id)
			after = id
		}
		if len(ids) < 128 {
			return
		}
	}
}

func (q *teamsCLIRequests) start(id string) {
	q.mu.Lock()
	if q.inflight[id] || q.ctx.Err() != nil {
		q.mu.Unlock()
		return
	}
	q.inflight[id] = true
	q.wg.Add(1)
	q.mu.Unlock()
	go func() {
		defer q.wg.Done()
		defer func() {
			q.mu.Lock()
			delete(q.inflight, id)
			q.mu.Unlock()
		}()
		r, ok, err := q.mail.Request(id)
		if err != nil || !ok {
			return
		}

		res := engine.CLIResult{}
		if r.ID != id {
			res.Error = "the request file is not readable by this helper version"
		} else {
			res, err = q.e.CLIRequest(q.ctx, r)
			if err != nil {
				q.mu.Lock()
				first := q.logged[id] != err.Error()
				q.logged[id] = err.Error()
				q.mu.Unlock()
				if first && !errors.Is(err, engine.ErrCLIRetry) {
					_, _ = fmt.Fprintf(q.log, "Teams command-line request %s: %v (retrying)\n", id, err)
				}
				return // handed in again at the next tick
			}
		}
		if err := q.mail.Complete(id, res, time.Now()); err != nil {
			_, _ = fmt.Fprintf(q.log, "Teams command-line request %s: write result: %v\n", id, err)
			return
		}
		q.mu.Lock()
		delete(q.logged, id)
		q.mu.Unlock()
	}()
}

// pending reports whether a request waits or runs (`run --once` finishes
// them before it exits).
func (q *teamsCLIRequests) pending() bool {
	q.mu.Lock()
	n := len(q.inflight)
	q.mu.Unlock()
	if n > 0 {
		return true
	}
	pending, err := q.mail.Pending()
	return pending || err != nil // never declare a damaged/unreadable mailbox drained

}

// wait waits (bounded) for the running requests after the engine stopped:
// they end with its context.
func (q *teamsCLIRequests) wait(d time.Duration) {
	done := make(chan struct{})
	go func() { q.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
	}
}
