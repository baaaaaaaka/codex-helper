package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/tui"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

var (
	selectSession        = tui.SelectSession
	runCodexSessionFunc  = runCodexSession
	runCodexNewSessionFn = runCodexNewSession
)

const defaultRefreshInterval = 5 * time.Second

func newHistoryCmd(root *rootOptions) *cobra.Command {
	var codexDir string
	var codexPath string
	var profileRef string

	cmd := &cobra.Command{
		Use:   "history",
		Short: "Inspect Codex history",
	}
	cmd.PersistentFlags().StringVar(&codexDir, "codex-dir", "", "Override Codex data dir (default: ~/.codex)")
	cmd.PersistentFlags().StringVar(&codexPath, "codex-path", "", "Override Codex CLI path (default: search PATH)")
	cmd.PersistentFlags().StringVar(&profileRef, "profile", "", "Proxy profile id or name")

	cmd.AddCommand(
		newHistoryTuiCmd(root, &codexDir, &codexPath, &profileRef),
		newHistoryListCmd(&codexDir),
		newHistoryShowCmd(&codexDir),
		newHistoryOpenCmd(root, &codexDir, &codexPath, &profileRef),
	)
	return cmd
}

func newHistoryTuiCmd(root *rootOptions, codexDir *string, codexPath *string, profileRef *string) *cobra.Command {
	var refreshInterval time.Duration
	cmd := &cobra.Command{
		Use:   "tui",
		Short: "Browse history in a terminal UI",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runHistoryTui(cmd, root, *profileRef, *codexDir, *codexPath, refreshInterval)
		},
	}
	cmd.Flags().DurationVar(&refreshInterval, "refresh-interval", defaultRefreshInterval, "Auto-refresh interval (0 to disable)")
	return cmd
}

func newHistoryListCmd(codexDir *string) *cobra.Command {
	var pretty bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List discovered projects and sessions as JSON",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			projects, err := codexhistory.DiscoverProjects(*codexDir)
			if err != nil && len(projects) == 0 {
				return err
			}
			payload := map[string]any{"projects": projects}
			out, err := json.MarshalIndent(payload, "", "  ")
			if err != nil {
				return err
			}
			if !pretty {
				out, err = json.Marshal(payload)
				if err != nil {
					return err
				}
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(out))
			return nil
		},
	}
	cmd.Flags().BoolVar(&pretty, "pretty", false, "Pretty-print JSON")
	return cmd
}

func newHistoryShowCmd(codexDir *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show <session-id>",
		Short: "Print full history for a session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sessionID := args[0]
			session, err := codexhistory.FindSessionByID(*codexDir, sessionID)
			if err != nil {
				return err
			}
			if session == nil {
				return fmt.Errorf("session %q not found", sessionID)
			}
			txt := codexhistory.FormatSession(*session)
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), txt)
			return nil
		},
	}
	return cmd
}

func newHistoryOpenCmd(root *rootOptions, codexDir *string, codexPath *string, profileRef *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "open <session-id>",
		Short: "Open a session in Codex",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := config.NewStore(root.configPath)
			if err != nil {
				return err
			}

			useProxy, cfg, err := ensureProxyPreference(cmd.Context(), store, *profileRef, cmd.ErrOrStderr())
			if err != nil {
				return err
			}

			var profile *config.Profile
			if useProxy {
				p, cfgWithProfile, err := ensureProfile(cmd.Context(), store, *profileRef, true, cmd.OutOrStdout())
				if err != nil {
					return err
				}
				cfg = cfgWithProfile
				profile = &p
			}
			useYolo := resolveYoloEnabled(cfg)

			sessionID := args[0]
			session, project, err := codexhistory.FindSessionWithProject(*codexDir, sessionID)
			if err != nil {
				return err
			}
			if session == nil {
				return fmt.Errorf("session %q not found", sessionID)
			}
			proj := codexhistory.Project{}
			if project != nil {
				proj = *project
			}
			return runCodexSession(
				cmd.Context(),
				root,
				store,
				profile,
				cfg.Instances,
				*session,
				proj,
				*codexPath,
				*codexDir,
				useProxy,
				useYolo,
				cmd.ErrOrStderr(),
			)
		},
	}
	return cmd
}

func runHistoryTui(cmd *cobra.Command, root *rootOptions, profileRef string, codexDir string, codexPath string, refreshInterval time.Duration) error {
	ctx := cmd.Context()
	store, err := config.NewStore(root.configPath)
	if err != nil {
		return err
	}

	for {
		useProxy, cfg, err := ensureProxyPreference(ctx, store, profileRef, cmd.ErrOrStderr())
		if err != nil {
			return err
		}
		useYolo := resolveYoloEnabled(cfg)

		var profile *config.Profile
		if useProxy {
			p, cfgWithProfile, err := ensureProfile(ctx, store, profileRef, true, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			cfg = cfgWithProfile
			profile = &p
		}

		resolvedCodexPath, err := ensureCodexInstalled(ctx, codexPath, cmd.ErrOrStderr())
		if err != nil {
			return err
		}
		codexPath = resolvedCodexPath

		defaultCwd, _ := os.Getwd()
		selection, err := selectSession(ctx, tui.Options{
			LoadProjects: func(ctx context.Context) ([]codexhistory.Project, error) {
				return codexhistory.DiscoverProjects(codexDir)
			},
			Version:         version,
			ProxyEnabled:    useProxy,
			ProxyConfigured: len(cfg.Profiles) > 0,
			YoloEnabled:     useYolo,
			RefreshInterval: refreshInterval,
			DefaultCwd:      defaultCwd,
			PersistYolo: func(enabled bool) error {
				return persistYoloEnabled(store, enabled)
			},
			CheckUpdate: func(ctx context.Context) update.Status {
				return update.CheckForUpdate(ctx, update.CheckOptions{
					InstalledVersion: version,
					Timeout:          8 * time.Second,
				})
			},
		})
		if err != nil {
			var upd tui.UpdateRequested
			if errors.As(err, &upd) {
				return handleUpdateAndRestart(ctx, cmd)
			}
			var toggle tui.ProxyToggleRequested
			if errors.As(err, &toggle) {
				if err := persistProxyPreference(store, toggle.Enable); err != nil {
					return err
				}
				if toggle.Enable && toggle.RequireConfig {
					if _, err := initProfileInteractive(ctx, store); err != nil {
						return err
					}
				}
				continue
			}
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if selection == nil {
			return nil
		}
		if selection.Cwd != "" {
			return runCodexNewSessionFn(
				ctx,
				root,
				store,
				profile,
				cfg.Instances,
				selection.Cwd,
				codexPath,
				codexDir,
				selection.UseProxy,
				selection.UseYolo,
				cmd.ErrOrStderr(),
			)
		}
		return runCodexSessionFunc(
			ctx,
			root,
			store,
			profile,
			cfg.Instances,
			selection.Session,
			selection.Project,
			codexPath,
			codexDir,
			selection.UseProxy,
			selection.UseYolo,
			cmd.ErrOrStderr(),
		)
	}
}
