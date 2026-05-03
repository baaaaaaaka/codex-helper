package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
)

func newTeamsProbeChatCmd(root *rootOptions) *cobra.Command {
	var chatInput string
	var top int
	var sendTest bool
	var webhookURLFile string
	var webhookURLEnv string
	var testText string
	cmd := &cobra.Command{
		Use:   "probe-chat --chat <teams-chat-id-or-link>",
		Short: "Probe an external Teams chat without binding helper state",
		Long:  "Probe an external Teams chat without binding helper state. This is intended for safe experiments such as a self-meeting chat or Workflow webhook fallback. By default it is read-only; it sends a Teams message only when --send-test is set.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			chatID, err := teams.ExtractChatID(chatInput)
			if err != nil {
				return err
			}
			if top <= 0 || top > 20 {
				top = 3
			}
			httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer func() { _ = httpClient.Close(context.Background()) }()
			if err := probeTeamsChatReadOnly(cmd.Context(), cmd.OutOrStdout(), httpClient.Client, chatID, top); err != nil {
				return err
			}
			if !sendTest {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Webhook send: skipped (use --send-test with --webhook-url-file or --webhook-url-env)")
				return nil
			}
			webhookURL, err := readTeamsWebhookURL(webhookURLFile, webhookURLEnv)
			if err != nil {
				return err
			}
			text := strings.TrimSpace(testText)
			if text == "" {
				text = "This is a codex-helper side-channel test. It does not change the existing control or work chat binding."
			}
			result, err := teams.PostWorkflowWebhook(cmd.Context(), httpClient.Client, webhookURL, teams.WorkflowWebhookMessage{
				Title: "Codex helper Teams experiment",
				Text:  text,
			})
			if err != nil {
				if result.StatusCode != 0 {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Webhook send: failed HTTP %d\n", result.StatusCode)
				}
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Webhook send: ok HTTP %d\n", result.StatusCode)
			return nil
		},
	}
	cmd.Flags().StringVar(&chatInput, "chat", "", "Teams chat id or copied Teams chat/meeting link to probe")
	cmd.Flags().IntVar(&top, "top", 3, "Number of recent messages to read for the probe, max 20")
	cmd.Flags().BoolVar(&sendTest, "send-test", false, "Send one small test Adaptive Card through the configured Workflow webhook")
	cmd.Flags().StringVar(&webhookURLFile, "webhook-url-file", "", "Path to a 0600 file containing the Teams Workflow webhook URL")
	cmd.Flags().StringVar(&webhookURLEnv, "webhook-url-env", "", "Environment variable containing the Teams Workflow webhook URL")
	cmd.Flags().StringVar(&testText, "test-text", "", "Text for the optional webhook test card")
	_ = cmd.MarkFlagRequired("chat")
	return cmd
}

func probeTeamsChatReadOnly(ctx context.Context, out io.Writer, client *http.Client, chatID string, top int) error {
	graph, err := teams.NewReadGraphClientWithHTTPClient(out, client)
	if err != nil {
		return err
	}
	chat, err := graph.GetChat(ctx, chatID)
	if err != nil {
		return err
	}
	members, err := graph.ListChatMembers(ctx, chatID)
	if err != nil {
		return err
	}
	messages, err := graph.ListMessages(ctx, chatID, top)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "Teams chat probe")
	_, _ = fmt.Fprintf(out, "Chat ID: %s\n", chatID)
	_, _ = fmt.Fprintf(out, "Type: %s\n", firstNonEmptyCLI(chat.ChatType, "unknown"))
	_, _ = fmt.Fprintf(out, "Title: %s\n", firstNonEmptyCLI(chat.Topic, "(none)"))
	_, _ = fmt.Fprintf(out, "URL: %s\n", firstNonEmptyCLI(chat.WebURL, "(none)"))
	_, _ = fmt.Fprintf(out, "Members: %d\n", len(members))
	for i, member := range members {
		if i >= 5 {
			_, _ = fmt.Fprintf(out, "  ... %d more\n", len(members)-i)
			break
		}
		_, _ = fmt.Fprintf(out, "  - %s <%s> roles=%s\n", firstNonEmptyCLI(member.DisplayName, member.UserID, "(unknown)"), member.Email, strings.Join(member.Roles, ","))
	}
	_, _ = fmt.Fprintf(out, "Recent messages readable: %d\n", len(messages))
	if chat.ChatType == "group" && len(members) < 2 {
		_, _ = fmt.Fprintln(out, "Warning: this is a single-member group chat. Teams Android/mobile notifications are not expected to be reliable for this shape.")
	}
	if strings.Contains(chatID, "19:meeting_") {
		_, _ = fmt.Fprintln(out, "Meeting chat: yes")
	}
	return nil
}

func readTeamsWebhookURL(path string, envName string) (string, error) {
	if strings.TrimSpace(path) != "" && strings.TrimSpace(envName) != "" {
		return "", fmt.Errorf("use either --webhook-url-file or --webhook-url-env, not both")
	}
	if strings.TrimSpace(path) != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(raw)), nil
	}
	if strings.TrimSpace(envName) != "" {
		return strings.TrimSpace(os.Getenv(envName)), nil
	}
	return "", fmt.Errorf("--send-test requires --webhook-url-file or --webhook-url-env")
}
