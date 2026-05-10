package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func newTeamsWorkflowCmd(root *rootOptions, registryPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflow",
		Short: "Configure optional Teams Workflow notification cards",
		Long:  "Configure optional Teams Workflow notification cards. The webhook URL is treated as a local secret: store it in a private file and pass the file path, not the raw URL.",
	}
	cmd.AddCommand(
		newTeamsWorkflowEnableCmd(root, registryPath),
		newTeamsWorkflowDisableCmd(root, registryPath),
		newTeamsWorkflowStatusCmd(),
		newTeamsWorkflowTestCmd(root, registryPath),
	)
	return cmd
}

func newTeamsWorkflowEnableCmd(root *rootOptions, registryPath *string) *cobra.Command {
	var webhookURLFile string
	cmd := &cobra.Command{
		Use:   "enable --webhook-url-file <path>",
		Short: "Enable Workflow cards for important Teams helper events",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := normalizeTeamsWorkflowURLFile(webhookURLFile)
			if err != nil {
				return err
			}
			bridge, closeBridge, err := newTeamsWorkflowBridge(cmd.Context(), root, registryPath, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer closeBridge()
			chat, err := bridge.EnsureControlChat(cmd.Context())
			if err != nil {
				return err
			}
			cfg, err := bridge.ConfigureWorkflowNotifications(cmd.Context(), path, true)
			if err != nil {
				return err
			}
			if err := bridge.Save(); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams workflow notifications enabled.")
			printTeamsControlChatDetails(cmd.OutOrStdout(), "Control chat", chat.ID, chat.Topic, chat.WebURL)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Webhook URL file: configured (%s)\n", workflowSecretFileStatus(path))
			if cfg.ControlChatID != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Bound control chat ID: %s\n", cfg.ControlChatID)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&webhookURLFile, "webhook-url-file", "", "Absolute path to a private file containing the Teams Workflow webhook URL")
	_ = cmd.MarkFlagRequired("webhook-url-file")
	return cmd
}

func newTeamsWorkflowDisableCmd(root *rootOptions, registryPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Disable Workflow notification cards",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			bridge, closeBridge, err := newTeamsWorkflowBridge(cmd.Context(), root, registryPath, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer closeBridge()
			if _, err := bridge.ConfigureWorkflowNotifications(cmd.Context(), "", false); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams workflow notifications disabled.")
			return nil
		},
	}
}

func newTeamsWorkflowStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show local Workflow notification configuration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return printTeamsWorkflowStatus(cmd)
		},
	}
}

func newTeamsWorkflowTestCmd(root *rootOptions, registryPath *string) *cobra.Command {
	var webhookURLFile string
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Send a test Workflow notification card to the configured control chat",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			bridge, closeBridge, err := newTeamsWorkflowBridge(cmd.Context(), root, registryPath, cmd.OutOrStdout(), cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer closeBridge()
			chat, err := bridge.EnsureControlChat(cmd.Context())
			if err != nil {
				return err
			}
			if strings.TrimSpace(webhookURLFile) != "" {
				path, err := normalizeTeamsWorkflowURLFile(webhookURLFile)
				if err != nil {
					return err
				}
				if _, err := bridge.ConfigureWorkflowNotifications(cmd.Context(), path, true); err != nil {
					return err
				}
				if err := bridge.Save(); err != nil {
					return err
				}
			}
			if err := bridge.SendWorkflowNotificationTest(cmd.Context()); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams workflow test card accepted.")
			printTeamsControlChatDetails(cmd.OutOrStdout(), "Control chat", chat.ID, chat.Topic, chat.WebURL)
			return nil
		},
	}
	cmd.Flags().StringVar(&webhookURLFile, "webhook-url-file", "", "Optional absolute path to configure before sending the test card")
	return cmd
}

func newTeamsWorkflowBridge(ctx context.Context, root *rootOptions, registryPath *string, out io.Writer, errOut io.Writer) (*teams.Bridge, func(), error) {
	httpClient, err := newTeamsGraphHTTPClientLease(ctx, root, errOut)
	if err != nil {
		return nil, func() {}, err
	}
	closeBridge := func() { _ = httpClient.Close(context.Background()) }
	auth, err := newTeamsAuthManagerWithHTTPClient(httpClient.Client)
	if err != nil {
		closeBridge()
		return nil, func() {}, err
	}
	bridge, err := teams.NewBridgeWithHTTPClient(ctx, auth, *registryPath, out, httpClient.Client)
	if err != nil {
		closeBridge()
		return nil, func() {}, err
	}
	httpClient.RetireSuspects(ctx, errOut)
	return bridge, closeBridge, nil
}

func normalizeTeamsWorkflowURLFile(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("workflow webhook URL file is required")
	}
	if strings.HasPrefix(path, "~/") || path == "~" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if path == "~" {
			path = home
		} else {
			path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if err := teams.ValidateWorkflowWebhookURLFile(abs); err != nil {
		return "", err
	}
	return abs, nil
}

func printTeamsWorkflowStatus(cmd *cobra.Command) error {
	paths, err := existingTeamsStorePaths()
	if err != nil {
		return err
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams workflow notifications: disabled")
		return nil
	}
	printed := false
	for _, path := range paths {
		st, err := teamsstore.Open(path)
		if err != nil {
			return err
		}
		state, err := st.Load(cmd.Context())
		if err != nil {
			return err
		}
		cfg := state.Workflow
		if !cfg.Enabled && cfg.ControlWebhookURLFile == "" && cfg.ControlChatID == "" && state.Scope.ID != "" {
			sidecarCfg, ok, err := teams.LoadWorkflowNotificationConfigFileForScope(state.Scope.ID)
			if err != nil {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams workflow notifications: invalid sidecar config (%v)\n", err)
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "State file: %s\n", path)
				printed = true
				continue
			}
			if ok {
				cfg = sidecarCfg
			}
		}
		if !cfg.Enabled && cfg.ControlWebhookURLFile == "" && cfg.ControlChatID == "" {
			continue
		}
		if printed {
			_, _ = fmt.Fprintln(cmd.OutOrStdout())
		}
		printed = true
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams workflow notifications: %s\n", workflowEnabledLabel(cfg.Enabled))
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "State file: %s\n", path)
		if cfg.ControlChatID != "" {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Bound control chat ID: %s\n", cfg.ControlChatID)
		}
		if cfg.ControlWebhookURLFile != "" {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Webhook URL file: configured (%s)\n", workflowSecretFileStatus(cfg.ControlWebhookURLFile))
		}
		if !cfg.UpdatedAt.IsZero() {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Updated: %s\n", cfg.UpdatedAt.Format("2006-01-02 15:04:05 MST"))
		}
	}
	if !printed {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams workflow notifications: disabled")
	}
	return nil
}

func workflowEnabledLabel(enabled bool) string {
	if enabled {
		return "enabled"
	}
	return "disabled"
}

func workflowSecretFileStatus(path string) string {
	if err := teams.ValidateWorkflowWebhookURLFile(path); err != nil {
		return "invalid: " + err.Error()
	}
	return "ok"
}
