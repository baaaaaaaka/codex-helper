package harness

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
)

// BrainURL starts (once) the HTTP endpoint the fake app-server binary uses
// to ask this FakeCodex what to do, and returns its base URL. It listens on
// loopback only.
func (f *FakeCodex) BrainURL() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.server == nil {
		f.server = httptest.NewServer(f.Handler())
	}
	return f.server.URL
}

// Close stops the brain endpoint (if started).
func (f *FakeCodex) Close() {
	f.mu.Lock()
	srv := f.server
	f.mu.Unlock()
	if srv != nil {
		srv.CloseClientConnections()
		srv.Close()
	}
}

// KillAppServers SIGKILLs every fake app-server process that ran a turn and
// may still be alive (e.g. a SurviveHelper turn after its helper died), so a
// scenario leaves no processes behind.
func (f *FakeCodex) KillAppServers() {
	seen := map[int]bool{}
	for _, c := range f.Calls() {
		if c.Pid > 0 && c.Pid != os.Getpid() && !seen[c.Pid] {
			seen[c.Pid] = true
			_ = syscall.Kill(c.Pid, syscall.SIGKILL)
		}
	}
}

// reapDead ends running calls whose app-server process is gone (SIGKILLed
// with its helper, or killed by the helper's runner) as "killed".
func (f *FakeCodex) reapDead() {
	f.mu.Lock()
	var dead []int
	for _, c := range f.calls {
		if c.Outcome == "running" && c.Pid > 0 && c.Pid != os.Getpid() && !processAlive(c.Pid) {
			dead = append(dead, c.Seq)
		}
	}
	f.mu.Unlock()
	for _, seq := range dead {
		_, _ = f.finishSeq(seq, "killed", "", errors.New("app-server process gone"))
	}
}

// processAlive reports whether pid runs and is not a zombie (in a container
// without an init, orphaned app-servers are never reaped).
func processAlive(pid int) bool {
	if data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat"); err == nil {
		if i := strings.LastIndexByte(string(data), ')'); i >= 0 && i+2 < len(data) {
			return data[i+2] != 'Z' && data[i+2] != 'X'
		}
		return true
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// Handler serves the brain protocol (see package fakecodex):
//
//	POST /begin        BeginRequest -> Plan
//	POST /await?seq=N  -> AwaitResult (blocks; cancel by closing the request)
//	POST /end?seq=N    EndRequest
func (f *FakeCodex) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /begin", func(w http.ResponseWriter, r *http.Request) {
		var req fakecodex.BeginRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		seq, b := f.begin(CodexCall{
			Instance: req.Instance, Kind: req.Kind, Pid: req.Pid,
			ThreadID: req.ThreadID, TurnID: req.TurnID, ClientID: req.ClientID, Cwd: req.Cwd,
			Prompt: req.Prompt, Images: req.Images,
		})
		plan := fakecodex.Plan{Seq: seq, Commentary: b.Commentary, Crash: b.Crash, HoldCompletion: b.HoldCompletion, SurviveHelper: b.SurviveHelper}
		if b.FailKind == FailActiveWriter {
			err := behaviorErr(b)
			plan.RejectStart = err.Error()
			_, _ = f.finishSeq(seq, "rejected", "", err)
		}
		writeJSON(w, plan)
	})
	mux.HandleFunc("POST /await", func(w http.ResponseWriter, r *http.Request) {
		seq, err := strconv.Atoi(r.URL.Query().Get("seq"))
		if err != nil {
			http.Error(w, "bad seq", http.StatusBadRequest)
			return
		}
		// Drain the body so the server notices when the client goes away
		// (that cancels r.Context()).
		_, _ = io.Copy(io.Discard, r.Body)
		f.mu.Lock()
		b, ok := f.behaviors[seq]
		f.mu.Unlock()
		if !ok {
			http.Error(w, "unknown seq", http.StatusNotFound)
			return
		}
		call, callErr := f.awaitMode(r.Context(), seq, b, false)
		res := fakecodex.AwaitResult{Outcome: call.Outcome, Answer: call.Answer}
		if callErr != nil {
			res.Error = callErr.Error()
		}
		if call.Outcome == "failed" && b.FailKind == FailCapacity {
			res.Capacity = true
		}
		writeJSON(w, res)
	})
	mux.HandleFunc("POST /end", func(w http.ResponseWriter, r *http.Request) {
		seq, err := strconv.Atoi(r.URL.Query().Get("seq"))
		if err != nil {
			http.Error(w, "bad seq", http.StatusBadRequest)
			return
		}
		var req fakecodex.EndRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var endErr error
		if req.Error != "" {
			endErr = errors.New(req.Error)
		}
		_, _ = f.finishSeq(seq, req.Outcome, "", endErr)
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
