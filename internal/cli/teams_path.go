package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/userpath"
)

func newTeamsPathCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "path",
		Short: "Inspect or configure the PATH exposed to Teams-launched Codex",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(
		newTeamsPathStatusCmd(root),
		newTeamsPathModeCmd(root),
		newTeamsPathExplicitCmd(root),
		newTeamsPathCaptureCmd(root),
		newTeamsPathShellCmd(root),
	)
	return cmd
}

func newTeamsPathStatusCmd(root *rootOptions) *cobra.Command {
	var showPath bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Resolve and report the effective Teams Codex PATH",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			cfg, err := store.Load()
			if err != nil {
				return err
			}
			cwd, err := os.Getwd()
			if err != nil {
				return err
			}
			configPath := ""
			if root != nil {
				configPath = root.configPath
			}
			paths, err := resolveEffectiveLaunchPaths(configPath, "", cwd)
			if err != nil {
				return err
			}
			result, err := resolveTeamsCodexUserPath(cmd.Context(), cfg, paths, teamsCodexChildEnv(), cwd)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(out, "Mode: %s\n", result.Mode)
			_, _ = fmt.Fprintf(out, "Source: %s\n", result.Source)
			if result.Target.Username != "" || result.Target.UID != 0 {
				_, _ = fmt.Fprintf(out, "Target: %s uid=%d gid=%d\n", result.Target.Username, result.Target.UID, result.Target.GID)
			}
			if result.Target.SID != "" {
				_, _ = fmt.Fprintf(out, "Target SID: %s\n", result.Target.SID)
			}
			if result.AccountShell != "" {
				_, _ = fmt.Fprintf(out, "Shell: %s (%s)\n", result.AccountShell, result.Adapter)
			}
			if result.BaselineSource != "" {
				_, _ = fmt.Fprintf(out, "Baseline: %s\n", result.BaselineSource)
			}
			_, _ = fmt.Fprintf(out, "PATH entries: %d\n", result.EntryCount)
			_, _ = fmt.Fprintf(out, "PATH fingerprint: %s\n", result.Fingerprint)
			if showPath {
				_, _ = fmt.Fprintf(out, "PATH: %s\n", result.Path)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&showPath, "show-path", false, "Print the complete resolved PATH")
	return cmd
}

func newTeamsPathModeCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "mode <account-default|captured-terminal|explicit|service>",
		Short: "Choose how Teams resolves the Codex PATH",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mode := strings.ToLower(strings.TrimSpace(args[0]))
			switch userpath.Mode(mode) {
			case userpath.ModeAccountDefault, userpath.ModeCapturedTerminal, userpath.ModeExplicit, userpath.ModeService:
			default:
				return fmt.Errorf("unknown Teams Codex PATH mode %q", args[0])
			}
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			if err := store.Update(func(cfg *config.Config) error {
				if (mode == string(userpath.ModeExplicit) || mode == string(userpath.ModeCapturedTerminal)) && cfg.TeamsCodexPath.ExplicitPath == "" {
					return fmt.Errorf("mode %s requires a saved PATH; use `cxp teams path explicit` or `capture-terminal`", mode)
				}
				cfg.TeamsCodexPath.Mode = mode
				return nil
			}); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams Codex PATH mode: %s\n", mode)
			return nil
		},
	}
}

func newTeamsPathExplicitCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "explicit <PATH>",
		Short: "Save an explicit PATH and select explicit mode",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if args[0] == "" {
				return fmt.Errorf("PATH must not be empty")
			}
			return updateTeamsPathConfig(root, cmd, string(userpath.ModeExplicit), args[0], "Saved explicit Teams Codex PATH")
		},
	}
}

func newTeamsPathCaptureCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "capture-terminal",
		Short: "Capture this terminal's PATH and select captured-terminal mode",
		Long:  "Capture the current terminal PATH, including deliberate venv, direnv, Nix, or other terminal-specific activation, and use it for future Teams Codex cold starts.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pathValue := os.Getenv("PATH")
			if pathValue == "" {
				return fmt.Errorf("current terminal PATH is empty")
			}
			return updateTeamsPathConfig(root, cmd, string(userpath.ModeCapturedTerminal), pathValue, "Captured terminal PATH for Teams Codex")
		},
	}
}

func newTeamsPathShellCmd(root *rootOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "shell <absolute-shell-path|default>",
		Short: "Override or restore the account shell used by account-default mode",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			shell := strings.TrimSpace(args[0])
			clearOverride := strings.EqualFold(shell, "default") || strings.EqualFold(shell, "auto")
			if !clearOverride && (shell == "" || !filepath.IsAbs(shell)) {
				return fmt.Errorf("shell override must be an absolute path")
			}
			if clearOverride {
				shell = ""
			}
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}
			if err := store.Update(func(cfg *config.Config) error {
				cfg.TeamsCodexPath.Mode = string(userpath.ModeAccountDefault)
				cfg.TeamsCodexPath.ShellOverride = shell
				return nil
			}); err != nil {
				return err
			}
			if clearOverride {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams Codex account shell: account database default")
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams Codex account shell: %s\n", shell)
			}
			return nil
		},
	}
}

func updateTeamsPathConfig(root *rootOptions, cmd *cobra.Command, mode, pathValue, message string) error {
	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	if err := store.Update(func(cfg *config.Config) error {
		cfg.TeamsCodexPath.Mode = mode
		cfg.TeamsCodexPath.ExplicitPath = pathValue
		return nil
	}); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), message)
	return nil
}
