package harness

import (
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// Rollout plays a local Codex TUI user: it writes a rollout JSONL file under
// the scenario's CODEX_HOME/sessions. See package rollout for the format.
type Rollout struct {
	*rollout.Writer
}

// NewRollout creates a new TUI session rollout for cwd, starting now.
func NewRollout(t testing.TB, env *Env, cwd string) *Rollout {
	t.Helper()
	return NewRolloutAt(t, env, cwd, time.Time{})
}

// NewRolloutAt creates a TUI session rollout whose records start at start
// (zero = now). Use SetModTime afterwards to age the file itself.
func NewRolloutAt(t testing.TB, env *Env, cwd string, start time.Time) *Rollout {
	t.Helper()
	w, err := rollout.New(env.SessionsDir, cwd, rollout.Options{Start: start})
	if err != nil {
		t.Fatalf("create rollout: %v", err)
	}
	return &Rollout{Writer: w}
}

// AppendTurn appends one complete TUI turn (see rollout.Writer.AppendTurn).
func (r *Rollout) AppendTurn(t testing.TB, prompt, commentary, answer string) {
	t.Helper()
	if _, err := r.Writer.AppendTurn(rollout.Turn{Prompt: prompt, Commentary: commentary, Answer: answer}); err != nil {
		t.Fatalf("append rollout turn: %v", err)
	}
}
