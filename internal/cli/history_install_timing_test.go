package cli

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/tui"
	"github.com/spf13/cobra"
)

func TestRunHistoryTuiDoesNotRequireCodexBeforeSelection(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.Update(func(c *config.Config) error {
		enabled := false
		c.ProxyEnabled = &enabled
		return nil
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	prevSelect := selectSession
	defer func() { selectSession = prevSelect }()
	selectSession = func(_ context.Context, _ tui.Options) (*tui.Selection, error) {
		return nil, nil
	}

	t.Setenv("PATH", t.TempDir())

	cmd := &cobra.Command{}
	cmd.SetContext(context.Background())
	if err := runHistoryTui(cmd, &rootOptions{configPath: cfgPath}, "", "", "", 0); err != nil {
		t.Fatalf("runHistoryTui error: %v", err)
	}
}
