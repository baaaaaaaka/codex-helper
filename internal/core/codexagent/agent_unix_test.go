//go:build !windows

package codexagent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/internal/leakcheck"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
)

const envFake = "CODEXAGENT_TEST_FAKE_CODEX"

// TestMain doubles as the fake Codex binary.
func TestMain(m *testing.M) {
	if os.Getenv(envFake) == "1" {
		home := os.Getenv("CODEX_HOME")
		err := fakecodex.Serve(fakecodex.Config{
			In: os.Stdin, Out: os.Stdout, Log: io.Discard,
			Store:       fakecodex.Store{Dir: filepath.Join(home, "e2e-fakecodex")},
			SessionsDir: filepath.Join(home, "sessions"),
			Brain:       os.Getenv(fakecodex.EnvBrainURL), Kind: "work",
		})
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	leakcheck.Main(m)
}

// Review M4: when turn/start fails with an error that is neither a
// rejection nor an exit, the turn may be running; the agent kills the
// app-server's process group before reporting the outcome unknown, so the
// turn cannot go on beside the next one.
func TestUnknownStartErrorKillsTheServer(t *testing.T) {
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/begin":
			_ = json.NewEncoder(w).Encode(fakecodex.Plan{Seq: 1, MalformedStart: true})
		case "/await":
			<-r.Context().Done() // the turn runs until its server dies
		default:
			_, _ = w.Write([]byte("{}"))
		}
	}))
	defer func() { brain.CloseClientConnections(); brain.Close() }()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	pidFile := filepath.Join(home, "pids.json")
	a := New(Config{
		Server:  codex.Config{Bin: exe, Env: []string{envFake + "=1", "CODEX_HOME=" + home, fakecodex.EnvBrainURL + "=" + brain.URL}, Dir: home},
		PIDFile: pidFile, Poll: 100 * time.Millisecond,
	})
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var pid int
	_, err = a.Run(ctx, engine.TurnRequest{ConvID: 1, Cwd: home, TurnKey: "k-1", Text: "hello"}, engine.TurnHooks{
		ThreadStarted: func(string) error {
			data, err := os.ReadFile(pidFile)
			if err != nil {
				return err
			}
			var recs []struct {
				PID int `json:"pid"`
			}
			if err := json.Unmarshal(data, &recs); err != nil || len(recs) != 1 {
				return errors.New("pid file: " + string(data))
			}
			pid = recs[0].PID
			return nil
		},
	})
	if !errors.Is(err, engine.ErrUnknownOutcome) {
		t.Fatalf("Run: %v, want ErrUnknownOutcome", err)
	}
	if pid == 0 {
		t.Fatal("no app-server pid recorded")
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("app-server %d still alive after an unknown outcome (kill 0: %v)", pid, err)
	}
}
