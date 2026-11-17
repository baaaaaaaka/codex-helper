package codex

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
)

func TestRealCodexGoalReadWithoutResume(t *testing.T) {
	bin := os.Getenv("E2E_REAL_CODEX")
	if bin == "" {
		t.Skip("offline native contract: E2E_REAL_CODEX is not set")
	}
	home := t.TempDir()
	responses := fakecodex.NewResponses()
	defer responses.Close()
	if err := fakecodex.WriteCodexHome(home, responses.URL()); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(home, "config.toml"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Goal is an experimental native capability. No remote inference is involved.
	_, err = f.WriteString("\ngoals = true\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := Config{Bin: bin, Env: fakecodex.CodexEnv(home), Dir: home}
	writer, err := Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Kill()
	th, err := writer.ThreadStart(ctx, ThreadStartParams{Cwd: home})
	if err != nil {
		t.Fatal(err)
	}
	g, err := writer.GoalSet(ctx, th.ID, "native goal contract", GoalPaused)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != GoalPaused {
		t.Fatal(g)
	}
	observer, err := Start(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Kill()
	got, err := observer.GoalGet(ctx, th.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Objective != g.Objective || got.Status != GoalPaused {
		t.Fatalf("goal=%+v", got)
	}
	if !observer.Info(th.ID).GoalKnown {
		t.Fatal("query did not establish authoritative observation")
	}
	if len(responses.Requests()) != 0 {
		t.Fatal("query or pause triggered inference")
	}
	// Read access does not acquire the execution writer. Release the original
	// writer before testing an offline mutation on the second app-server.
	writer.Kill()
	// Recreate the original failure condition: an active goal on a thread this
	// observer has never loaded. Reading and pausing it must remain execution-free.
	if _, err := observer.GoalSet(ctx, th.ID, "", GoalActive); err != nil {
		t.Fatal(err)
	}
	got, err = observer.GoalGet(ctx, th.ID)
	if err != nil || got == nil || got.Status != GoalActive {
		t.Fatalf("active read: %+v %v", got, err)
	}
	if _, err := observer.GoalSet(ctx, th.ID, "", GoalPaused); err != nil {
		t.Fatal(err)
	}
	var loaded struct {
		Data []string `json:"data"`
	}
	if err := observer.Call(ctx, "thread/loaded/list", map[string]any{}, &loaded); err != nil {
		t.Fatal(err)
	}
	if len(loaded.Data) != 0 {
		t.Fatalf("goal observation loaded execution: %v", loaded.Data)
	}
	if len(responses.Requests()) != 0 {
		t.Fatal("offline goal control triggered inference")
	}
	if err := observer.GoalClear(ctx, th.ID); err != nil {
		t.Fatal(err)
	}
	got, err = observer.GoalGet(ctx, th.ID)
	if err != nil || got != nil {
		t.Fatalf("clear=%+v %v", got, err)
	}
}
