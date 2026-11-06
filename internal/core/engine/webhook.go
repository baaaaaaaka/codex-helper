package engine

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Workflow webhook from the control chat (gap batch B, F-CTL-14): v0.1.20's
// `helper webhook setup|<url>|test|status|off` (workflowWebhookFromControl
// B:5529) with its texts. The setting itself belongs to the Notifier (the
// cxp entry point keeps it in the store and reads it at every card, so a
// change applies without a restart).

// WebhookSettings is a Notifier whose Workflow webhook the control chat
// manages.
type WebhookSettings interface {
	// WebhookEnabled reports whether Workflow cards are on.
	WebhookEnabled() (bool, error)
	// SetWebhook saves url as a private local file and turns cards on.
	SetWebhook(url string) error
	// DisableWebhook turns cards off.
	DisableWebhook() error
}

// metaWebhookTest is the inbound id of the last `helper webhook test` whose
// card post was started: a command re-run after a restart never posts a
// second card (v0.1.20 controlCommandAlreadyHandled).
const metaWebhookTest = "engine_webhook_test"

const (
	textWebhookSaveUsage = "⚠️ Workflow webhook URL was not saved.\n\nSend an absolute `https://...` Teams Workflow webhook URL, for example:\n`helper webhook https://...`"
	webhookGuideURL      = "https://support.microsoft.com/en-us/office/create-incoming-webhooks-with-workflows-for-microsoft-teams-8ae491c7-0394-4861-ba59-055e33f75498"
)

func webhookHelpText() string {
	return strings.Join([]string{
		"## 🔔 Workflow webhook",
		"",
		"Use this to send Teams Workflow notification cards for important helper events.",
		"",
		"Commands:",
		"- `helper webhook setup` - show the setup steps",
		"- `helper webhook <https-url>` - save and enable the webhook",
		"- `helper webhook test` - send one test card",
		"- `helper webhook status` - show whether it is enabled",
		"- `helper webhook off` - disable cards",
		"",
		"The webhook URL is stored locally as a private secret file and is not echoed back.",
	}, "\n")
}

// webhookSetupText is v0.1.20's workflowWebhookControlSetupText; Teams
// does not give the helper a hyperlink's target, so the line about
// shortened hyperlinks asks for the plain URL instead.
func webhookSetupText(title, chatID string) string {
	hint := firstNonEmpty(strings.TrimSpace(title), "this Control chat")
	if chatID != "" {
		hint += "\nChat ID: `" + chatID + "`"
	}
	return strings.Join([]string{
		"## 🔔 Workflow webhook setup",
		"",
		"Use this to send notification cards when Codex finishes, restarts, or needs your attention.",
		"",
		"Step 1: Open Workflows here",
		"In this Control chat, click the `+` button at the lower right of the message box, then choose `Workflow`.",
		"",
		"Step 2: Pick exactly this Workflow template",
		"Search `webhook`, then choose exactly: **Send webhook alerts to a chat**",
		"",
		"Step 3: Confirm the target chat",
		"Keep the target set to this Control chat:",
		hint,
		"",
		"Step 4: Create the Workflow and copy the URL",
		"Click `Next` / `Add workflow`. Teams will show a webhook URL; copy it.",
		"",
		"Step 5: Send this here",
		"`helper webhook <paste-url>`",
		"If Teams turns the pasted URL into a shortened hyperlink, paste the full URL as plain text instead.",
		"",
		"Need the Microsoft walkthrough? Open [Teams webhook setup guide](" + webhookGuideURL + ").",
		"",
		"Note: Microsoft does not provide a reliable one-click link that pre-fills every workflow field and returns the generated webhook URL.",
	}, "\n")
}

// safeWebhookURL is v0.1.20's safeWorkflowWebhookURL.
func safeWebhookURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return u.Scheme == "https" && u.Host != "" && u.User == nil && u.Fragment == "" && u.IsAbs()
}

// cmdWebhook is `helper webhook …` in the control chat.
func (e *Engine) cmdWebhook(ctx context.Context, j cmdJob, cs *convState, arg string) {
	ws, _ := e.cfg.Notifier.(WebhookSettings)
	if ws == nil {
		e.reply(j, "⚠️ Workflow webhooks are not available in this Teams service.", nil)
		return
	}
	arg = strings.TrimSpace(arg)
	name, rest := splitBody(arg)
	switch name {
	case "", "help", "?":
		e.reply(j, webhookHelpText(), nil)
		return
	case "setup", "guide", "start":
		var title, chatID string
		if cs != nil {
			e.mu.Lock()
			title, chatID = cs.title, cs.chat
			e.mu.Unlock()
		}
		e.reply(j, webhookSetupText(title, chatID), nil)
		return
	case "status", "st":
		on, err := ws.WebhookEnabled()
		switch {
		case err != nil:
			e.reply(j, "⚠️ Workflow webhook status failed.\n\n"+userError(err.Error()), nil)
		case !on:
			e.reply(j, "🔕 Workflow webhook disabled.\n\nSend `helper webhook <https-url>` to enable Workflow notification cards.", nil)
		default:
			e.reply(j, "🔔 Workflow webhook enabled.\n\nWebhook URL: configured as a local private secret file.", nil)
		}
		return
	case "off", "disable", "disabled":
		if err := ws.DisableWebhook(); err != nil {
			e.reply(j, "⚠️ Workflow webhook was not disabled.\n\n"+userError(err.Error()), nil)
			return
		}
		e.reply(j, "🔕 Workflow webhook disabled.\n\nThe helper will stop sending Workflow notification cards.", nil)
		return
	case "test":
		e.webhookTest(ctx, j, cs, ws)
		return
	case "set", "url", "enable":
		arg = rest
	}
	if arg == "" {
		e.reply(j, webhookHelpText(), nil)
		return
	}
	u := strings.Trim(strings.TrimSpace(arg), "`<>\"'")
	if !safeWebhookURL(u) {
		e.reply(j, textWebhookSaveUsage, nil)
		return
	}
	// Idempotent: a command re-run after a restart saves the same URL and
	// answers once (the reply commits with the command).
	if err := ws.SetWebhook(u); err != nil {
		e.reply(j, "⚠️ Workflow webhook was not saved.\n\n"+userError(err.Error()), nil)
		return
	}
	e.reply(j, strings.Join([]string{
		"✅ Workflow webhook enabled.",
		"",
		"Notification cards will be sent for important helper events.",
		"Webhook URL: saved locally as a private secret file; I will not echo it back.",
		"",
		"Send `helper webhook test` to send a test card.",
	}, "\n"), nil)
}

// webhookTest posts one test card (v0.1.20 SendWorkflowNotificationTest)
// at most once per command: the post is recorded before it starts.
func (e *Engine) webhookTest(ctx context.Context, j cmdJob, cs *convState, ws WebhookSettings) {
	on, err := ws.WebhookEnabled()
	if err == nil && !on {
		err = fmt.Errorf("Teams workflow notifications are disabled")
	}
	if err != nil {
		e.reply(j, "⚠️ Workflow webhook test failed.\n\n"+userError(err.Error()), nil)
		return
	}
	mark := strconv.FormatInt(j.inbound, 10)
	again := false
	if err := e.update(func(tx *store.Tx) error {
		v, _, err := tx.Meta(metaWebhookTest)
		if err != nil {
			return err
		}
		if again = v == mark; again {
			return nil
		}
		return tx.SetMeta(metaWebhookTest, mark)
	}); err != nil {
		e.reply(j, "⚠️ Workflow webhook test failed.\n\n"+userError(err.Error()), nil)
		return
	}
	if again {
		e.reply(j, "⚠️ Workflow webhook test: the helper stopped while the test card was being sent, so it may have arrived already. It was not sent again; send `helper webhook test` to try once more.", nil)
		return
	}
	link := ""
	if cs != nil {
		e.mu.Lock()
		link = cs.url
		e.mu.Unlock()
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if ev, ok := e.cfg.Notifier.(EventNotifier); ok {
		err = ev.NotifyEvent(cctx, "✅ Codex helper notification test", "Workflow card delivery is configured for this control chat.", link)
	} else {
		err = e.cfg.Notifier.NotifyFinal(cctx, "", "notification test", "Workflow card delivery is configured for this control chat.", link)
	}
	if err != nil {
		e.reply(j, "⚠️ Workflow webhook test failed.\n\n"+userError(err.Error()), nil)
		return
	}
	e.reply(j, "✅ Workflow webhook test card queued.", nil)
}
