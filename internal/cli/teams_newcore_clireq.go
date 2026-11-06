package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
)

// Commands that act through the running helper (gap batch G: `teams
// control --recreate`, `teams chat recreate`, `teams chat publish-history`,
// `teams send-file`). The command never opens the store a helper holds: it
// leaves a request file in cli-requests/ next to the store; the helper
// picks it up on its 5 s watcher tick (the one that applies Workflow
// changes), hands it to engine.CLIRequest and writes <id>.result.json,
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

func teamsCLIResultPath(dir, id string) string { return filepath.Join(dir, id+".result.json") }
func teamsCLIStageDir(dir, id string) string   { return filepath.Join(dir, id+".d") }

func newTeamsCLIRequestID(now time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%d-%s", now.UnixMilli(), hex.EncodeToString(b[:]))
}

func writeTeamsJSONFile(path string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.%d.tmp", path, os.Getpid())
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
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
	reqPath := filepath.Join(dir, sub.req.ID+".json")
	if err := writeTeamsJSONFile(reqPath, sub.req); err != nil {
		_ = os.RemoveAll(teamsCLIStageDir(dir, sub.req.ID))
		return err
	}
	if !view.Info.Exists || !view.Info.Live(now, lease.ProductionStale) {
		_, _ = fmt.Fprintln(out, "The Teams helper is not running. The request is kept and runs when the helper starts (`cxp teams start`, or `cxp teams run --once` to run it once now).")
		return nil
	}
	_, _ = fmt.Fprintln(out, "Waiting for the running Teams helper...")
	deadline := now.Add(teamsCLIRequestWait)
	resPath := teamsCLIResultPath(dir, sub.req.ID)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(resPath)
		if err == nil {
			var res engine.CLIResult
			if err := json.Unmarshal(raw, &res); err != nil {
				return fmt.Errorf("read the helper's result: %w", err)
			}
			_ = os.Remove(resPath)
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
	dir string
	e   *engine.Engine
	ctx context.Context
	log io.Writer

	mu       sync.Mutex
	inflight map[string]bool
	logged   map[string]string
	wg       sync.WaitGroup
}

func newTeamsCLIRequests(ctx context.Context, anchor string, e *engine.Engine, log io.Writer) *teamsCLIRequests {
	return &teamsCLIRequests{dir: teamsCLIRequestDir(anchor), e: e, ctx: ctx, log: log,
		inflight: map[string]bool{}, logged: map[string]string{}}
}

// tick starts the requests found and drops results nobody read. It runs
// on the helper's watcher tick (no timer of its own).
func (q *teamsCLIRequests) tick(now time.Time) {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return // no request was ever left
	}
	for _, ent := range entries {
		name := ent.Name()
		switch {
		case strings.HasSuffix(name, ".result.json"):
			if info, err := ent.Info(); err == nil && now.Sub(info.ModTime()) > teamsCLIResultKeep {
				id := strings.TrimSuffix(name, ".result.json")
				_ = os.Remove(filepath.Join(q.dir, name))
				_ = os.RemoveAll(teamsCLIStageDir(q.dir, id))
			}
		case strings.HasSuffix(name, ".json") && !ent.IsDir():
			q.start(strings.TrimSuffix(name, ".json"))
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
		reqPath := filepath.Join(q.dir, id+".json")
		raw, err := os.ReadFile(reqPath)
		if err != nil {
			return
		}
		var r engine.CLIRequest
		res := engine.CLIResult{}
		if err := json.Unmarshal(raw, &r); err != nil || r.ID != id {
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
		if err := writeTeamsJSONFile(teamsCLIResultPath(q.dir, id), res); err != nil {
			_, _ = fmt.Fprintf(q.log, "Teams command-line request %s: write result: %v\n", id, err)
			return
		}
		_ = os.Remove(reqPath)
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
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return false
	}
	for _, ent := range entries {
		if name := ent.Name(); strings.HasSuffix(name, ".json") && !strings.HasSuffix(name, ".result.json") && !ent.IsDir() {
			return true
		}
	}
	return false
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
