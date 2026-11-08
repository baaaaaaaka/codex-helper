//go:build !windows

package codexagent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

func TestTitleSingleSlotAndShutdown(t *testing.T) {
	var titles atomic.Int32
	started := make(chan struct{}, 2)
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch r.URL.Path {
		case "/begin":
			_ = json.NewEncoder(w).Encode(fakecodex.Plan{Seq: 1})
		case "/await":
			_ = json.NewEncoder(w).Encode(fakecodex.AwaitResult{Outcome: "answered", Answer: "Normal answer"})
		case "/title":
			titles.Add(1)
			started <- struct{}{}
			<-r.Context().Done()
		default:
			_, _ = w.Write([]byte("{}"))
		}
	}))
	defer brain.Close()
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	a := New(Config{Server: codex.Config{Bin: exe, Dir: home, Env: []string{envFake + "=1", "CODEX_HOME=" + home, fakecodex.EnvBrainURL + "=" + brain.URL}}, PIDFile: filepath.Join(home, "pids.json")})
	defer a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	hooks := engine.TurnHooks{Title: func(context.Context, func(context.Context) (string, error)) error {
		t.Error("cancelled title was applied")
		return nil
	}}
	run := func(conv int64) {
		t.Helper()
		out, err := a.Run(ctx, engine.TurnRequest{ConvID: conv, Cwd: home, Model: "test-model", Effort: "low", Text: "Main task", TurnKey: "task", GenerateTitle: true, TitleText: "Title topic"}, hooks)
		if err != nil || out.Final != "Normal answer" {
			t.Fatalf("main answer: %+v %v", out, err)
		}
	}
	var firstThread string
	hooks.ThreadStarted = func(id string) error {
		if firstThread == "" {
			firstThread = id
		}
		return nil
	}
	run(1)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	run(2)
	if titles.Load() != 1 {
		t.Fatal("concurrent cosmetic request was queued or started")
	}
	a.CancelTitle(1)
	deadline := time.Now().Add(5 * time.Second)
	for {
		a.mu.Lock()
		busy := a.titleCancel != nil
		a.mu.Unlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancel did not free title slot")
		}
		time.Sleep(time.Millisecond)
	}
	out, err := a.Run(ctx, engine.TurnRequest{ConvID: 1, ThreadID: firstThread, Cwd: home, Model: "test-model", Effort: "low", Text: "Continue existing chat", TurnKey: "next", GenerateTitle: true, TitleText: "Must not regenerate"}, hooks)
	if err != nil || out.Final != "Normal answer" || titles.Load() != 1 {
		t.Fatalf("existing thread generated a title: %+v %v (%d requests)", out, err, titles.Load())
	}
	run(3)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	closeDone := make(chan struct{})
	go func() { a.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Close retained hidden request")
	}
	if titles.Load() != 2 {
		t.Fatalf("%d title requests", titles.Load())
	}
}
