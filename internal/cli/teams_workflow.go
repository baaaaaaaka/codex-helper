package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
)

func newTeamsWorkflowCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflow",
		Short: "Configure optional Teams Workflow notification cards",
		Long:  "Configure optional Teams Workflow notification cards. The webhook URL is treated as a local secret: store it in a private file and pass the file path, not the raw URL.",
	}
	cmd.AddCommand(
		newTeamsWorkflowEnableCmd(),
		newTeamsWorkflowDisableCmd(),
		newTeamsWorkflowStatusCmd(),
		newTeamsWorkflowTestCmd(root),
	)
	return cmd
}

func newTeamsWorkflowEnableCmd() *cobra.Command {
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
			corePath, _, err := teamsNewCoreCLIStorePath()
			if err != nil {
				return err
			}
			return runTeamsNewCoreWorkflowEnable(cmd.OutOrStdout(), corePath, path)
		},
	}
	cmd.Flags().StringVar(&webhookURLFile, "webhook-url-file", "", "Absolute path to a private file containing the Teams Workflow webhook URL")
	_ = cmd.MarkFlagRequired("webhook-url-file")
	return cmd
}

func newTeamsWorkflowDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Disable Workflow notification cards",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			corePath, _, err := teamsNewCoreCLIStorePath()
			if err != nil {
				return err
			}
			return runTeamsNewCoreWorkflowDisable(cmd.OutOrStdout(), corePath)
		},
	}
}

func newTeamsWorkflowStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show local Workflow notification configuration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			corePath, _, err := teamsNewCoreCLIStorePath()
			if err != nil {
				return err
			}
			return printTeamsNewCoreWorkflowStatus(cmd.OutOrStdout(), corePath)
		},
	}
}

func newTeamsWorkflowTestCmd(root *rootOptions) *cobra.Command {
	var webhookURLFile string
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Send a test Workflow notification card to the configured control chat",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			corePath, _, err := teamsNewCoreCLIStorePath()
			if err != nil {
				return err
			}
			file := ""
			if strings.TrimSpace(webhookURLFile) != "" {
				if file, err = normalizeTeamsWorkflowURLFile(webhookURLFile); err != nil {
					return err
				}
			}
			return runTeamsNewCoreWorkflowTest(cmd, root, corePath, file)
		},
	}
	cmd.Flags().StringVar(&webhookURLFile, "webhook-url-file", "", "Optional absolute path to configure before sending the test card")
	return cmd
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
	if _, err := chteams.ReadWebhookURLFile(abs); err != nil {
		return "", err
	}
	return abs, nil
}

func workflowEnabledLabel(enabled bool) string {
	if enabled {
		return "enabled"
	}
	return "disabled"
}

func workflowSecretFileStatus(path string) string {
	if _, err := chteams.ReadWebhookURLFile(path); err != nil {
		return "invalid: " + err.Error()
	}
	return "ok"
}
