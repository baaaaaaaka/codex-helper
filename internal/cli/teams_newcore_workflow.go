package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/channel"
	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// teamsNewCoreCLIStorePath returns the Teams helper's store for the
// commands a user runs (status, workflow). use is false when the store
// cannot be located (err says why).
func teamsNewCoreCLIStorePath() (path string, use bool, err error) {
	stateDir, err := appdirs.StateDir()
	if err != nil {
		return "", true, fmt.Errorf("state directory: %w", err)
	}
	path, err = resolveTeamsCoreStorePath(stateDir)
	return path, true, err
}

// errTeamsHelperRunning means a live helper owns the store, so only it may
// write there.
var errTeamsHelperRunning = errors.New("the Teams helper is running")

// updateTeamsNewCoreConfig writes configuration to the new core's store.
// Only the owner writes the store, so the command takes ownership for the
// write; errTeamsHelperRunning when a live helper holds it.
func updateTeamsNewCoreConfig(path string, fn func(*corestore.Tx) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return teamsWithLeasedStore(context.Background(), path, func(st *corestore.Store) error {
		return st.Update(fn)
	})
}

type teamsWorkflowRequest struct {
	Enabled        bool   `json:"enabled"`
	WebhookURLFile string `json:"webhook_url_file,omitempty"`
}

func setTeamsWorkflowMeta(tx *corestore.Tx, r teamsWorkflowRequest) error {
	if r.Enabled {
		if err := tx.SetMeta(migrate.MetaWorkflowSecretFile, r.WebhookURLFile); err != nil {
			return err
		}
	}
	return tx.SetMeta(migrate.MetaWorkflowEnabled, fmt.Sprint(r.Enabled))
}

// teamsWorkflowApplyWait bounds how long a command waits for the running
// helper to apply its request.
var teamsWorkflowApplyWait = 15 * time.Second

// setTeamsNewCoreWorkflow turns Workflow cards on with the webhook URL file
// (an absolute, validated path) or off. The file is referenced, not copied,
// as the old core did, so replacing its contents rotates the URL. With the
// helper stopped the store is written directly; with it running the
// request is handed to it. live reports the latter; applied whether the
// change is in effect when it returns.
func setTeamsNewCoreWorkflow(path string, enabled bool, webhookURLFile string) (live, applied bool, err error) {
	r := teamsWorkflowRequest{Enabled: enabled, WebhookURLFile: webhookURLFile}
	mail, err := openTeamsExchange(path, false)
	if err != nil {
		return false, false, err
	}
	defer mail.Close()
	_, prior, hadPrior, err := mail.Workflow()
	if err != nil {
		return false, false, err
	}
	err = updateTeamsNewCoreConfig(path, func(tx *corestore.Tx) error {
		if err := setTeamsWorkflowMeta(tx, r); err != nil {
			return err
		}
		if hadPrior {
			return mail.ClearWorkflow(prior)
		}
		return nil
	})
	if !errors.Is(err, errTeamsHelperRunning) {
		return false, err == nil, err
	}
	rev := newTeamsCLIRequestID(time.Now())
	if err := mail.SetWorkflow(rev, r); err != nil {
		return true, false, err
	}
	deadline := time.Now().Add(teamsWorkflowApplyWait)
	for time.Now().Before(deadline) {
		_, _, pending, err := mail.Workflow()
		if err != nil {
			return true, false, err
		}
		if !pending {
			return true, true, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return true, false, nil
}

// applyTeamsWorkflowRequest applies a pending request as the store's owner
// and removes it. An invalid request (say, the webhook file became
// unreadable) is dropped with an error.
func applyTeamsWorkflowRequest(st *corestore.Store, mail *teamsExchangeStore) (bool, error) {
	r, rev, ok, err := mail.Workflow()
	if err != nil || !ok {
		return false, err
	}
	if r.Enabled {
		_, err = normalizeTeamsWorkflowURLFile(r.WebhookURLFile)
	}
	if err != nil {
		if dropErr := mail.ClearWorkflow(rev); dropErr != nil {
			return false, dropErr
		}
		return false, fmt.Errorf("Workflow request dropped: %w", err)
	}
	if err = st.Update(func(tx *corestore.Tx) error { return setTeamsWorkflowMeta(tx, r) }); err != nil {
		return false, err
	}
	// Conditional acknowledgement cannot erase a newer request submitted while
	// the business transaction was applying this one.
	return true, mail.ClearWorkflow(rev)
}

// teamsNewCoreNotifier is the engine's Notifier. Like the old core it reads
// the Workflow setting and the webhook file at every card, so a change
// (or a rotated URL) applies without a restart.
type teamsNewCoreNotifier struct {
	applyMu sync.Mutex
	mail    *teamsExchangeStore
	st      *corestore.Store
	path    string
	client  *http.Client
	log     io.Writer
	mu      sync.Mutex
	last    string
}

func (n *teamsNewCoreNotifier) apply() {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()
	if ok, err := applyTeamsWorkflowRequest(n.st, n.mail); err != nil {
		_, _ = fmt.Fprintf(n.log, "Teams Workflow: %v\n", err)
	} else if ok {
		_, _ = fmt.Fprintln(n.log, "Teams Workflow setting changed")
	}
}

func (n *teamsNewCoreNotifier) url() string {
	n.apply()
	u, err := teamsNewCoreWorkflowURL(n.st, filepath.Dir(n.path))
	n.mu.Lock()
	defer n.mu.Unlock()
	if err != nil {
		if msg := err.Error(); msg != n.last {
			n.last = msg
			_, _ = fmt.Fprintf(n.log, "Teams Workflow cards are off: %v\n", err)
		}
		return ""
	}
	n.last = ""
	return u
}

func (n *teamsNewCoreNotifier) Available() bool { return n.url() != "" }

func (n *teamsNewCoreNotifier) Notify(ctx context.Context, notice channel.OwnerNotice) error {
	return chteams.NewWorkflowNotifier(n.url(), n.client).Notify(ctx, notice)
}

// The control chat's `helper webhook …` (engine.WebhookSettings, gap batch
// B, F-CTL-14). The URL is saved as v0.1.20 saved it: a private file named
// workflow-webhook-url next to the store (the name the migration copies
// the old secret to); a file the user configured elsewhere is not touched.
const teamsWorkflowChatSecretName = "workflow-webhook-url"

func (n *teamsNewCoreNotifier) WebhookEnabled() (bool, error) {
	n.apply()
	on, _, err := n.st.Meta(migrate.MetaWorkflowEnabled)
	return strings.EqualFold(strings.TrimSpace(on), "true"), err
}

func (n *teamsNewCoreNotifier) SetWebhook(url string) error {
	url = strings.TrimSpace(url)
	if !chteams.SafeWebhookURL(url) {
		return fmt.Errorf("workflow webhook URL must be an absolute https URL")
	}
	n.apply() // an older command's request must not undo this change later
	file := filepath.Join(filepath.Dir(n.path), teamsWorkflowChatSecretName)
	if info, err := os.Lstat(file); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("workflow webhook URL file must not be a symlink")
	}
	tmp := file + ".tmp"
	if err := os.WriteFile(tmp, []byte(url+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, file); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if _, err := chteams.ReadWebhookURLFile(file); err != nil {
		return err
	}
	return n.st.Update(func(tx *corestore.Tx) error {
		return setTeamsWorkflowMeta(tx, teamsWorkflowRequest{Enabled: true, WebhookURLFile: file})
	})
}

func (n *teamsNewCoreNotifier) DisableWebhook() error {
	n.apply()
	return n.st.Update(func(tx *corestore.Tx) error { return setTeamsWorkflowMeta(tx, teamsWorkflowRequest{}) })
}

// teamsWorkflowWatchEvery is how often the running helper looks for a
// workflow enable/disable request (well inside teamsWorkflowApplyWait).
const teamsWorkflowWatchEvery = 5 * time.Second

// watch applies a command's request promptly, so the command can report
// it in effect. also runs on the same tick (the helper's other sidecar
// files: poll stamp, command-line requests), so they need no timer.
func (n *teamsNewCoreNotifier) watch(ctx context.Context, also ...func(time.Time)) {
	t := time.NewTicker(teamsWorkflowWatchEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			n.apply()
			for _, f := range also {
				f(now)
			}
		}
	}
}

func reportTeamsWorkflowChange(out io.Writer, live, applied bool) {
	switch {
	case live && applied:
		_, _ = fmt.Fprintln(out, "The running Teams helper applied the change.")
	case live:
		_, _ = fmt.Fprintln(out, "The running Teams helper has not picked the change up yet; it applies it within a few seconds or at its next start.")
	}
}

func runTeamsNewCoreWorkflowEnable(out io.Writer, path, webhookURLFile string) error {
	live, applied, err := setTeamsNewCoreWorkflow(path, true, webhookURLFile)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "Teams workflow notifications enabled.")
	_, _ = fmt.Fprintf(out, "Webhook URL file: configured (%s)\n", workflowSecretFileStatus(webhookURLFile))
	reportTeamsWorkflowChange(out, live, applied)
	return nil
}

func runTeamsNewCoreWorkflowDisable(out io.Writer, path string) error {
	live, applied, err := setTeamsNewCoreWorkflow(path, false, "")
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "Teams workflow notifications disabled.")
	reportTeamsWorkflowChange(out, live, applied)
	return nil
}

// teamsNewCoreWorkflowConfig reads the Workflow setting of the store
// ("" file when none is configured): what the running helper published,
// or the store itself while no helper holds it.
func teamsNewCoreWorkflowConfig(path string) (enabled bool, file string, err error) {
	view, err := teamsStoreViewAt(path)
	if err != nil {
		return false, "", err
	}
	if !view.helperHolds() && !teamsFileExists(view.DB) {
		return false, "", nil
	}
	s, _, err := teamsReadStoreState(view, time.Now())
	if err != nil {
		return false, "", fmt.Errorf("read the Teams helper's settings: %w", err)
	}
	return s.Workflow, s.workflowFile(filepath.Dir(path)), nil
}

func printTeamsNewCoreWorkflowStatus(out io.Writer, path string) error {
	enabled, file, err := teamsNewCoreWorkflowConfig(path)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "Teams workflow notifications: %s\n", workflowEnabledLabel(enabled))
	_, _ = fmt.Fprintf(out, "Store: %s\n", path)
	if enabled && file != "" {
		_, _ = fmt.Fprintf(out, "Webhook URL file: configured (%s)\n", workflowSecretFileStatus(file))
	}
	mail, readErr := openTeamsExchange(path, true)
	if readErr != nil && !os.IsNotExist(readErr) {
		return readErr
	}
	if readErr == nil {
		defer mail.Close()
		r, _, pending, readErr := mail.Workflow()
		if readErr != nil {
			return readErr
		}
		if pending {
			fmt.Fprintf(out, "Pending change: %s (waiting for the running Teams helper)\n", workflowEnabledLabel(r.Enabled))
		}
	}
	return nil
}

// runTeamsNewCoreWorkflowTest posts one test card through the webhook the
// helper uses, with the helper's Graph HTTP client (proxy included).
func runTeamsNewCoreWorkflowTest(cmd *cobra.Command, root *rootOptions, path, webhookURLFile string) error {
	enabled, file, err := teamsNewCoreWorkflowConfig(path)
	if webhookURLFile != "" {
		// Configure first, as the old command did; the card goes to the
		// file given even before the running helper applies it.
		if _, _, err := setTeamsNewCoreWorkflow(path, true, webhookURLFile); err != nil {
			return err
		}
		enabled, file, err = true, webhookURLFile, nil
	}
	if err != nil {
		return err
	}
	if !enabled || file == "" {
		return fmt.Errorf("Teams workflow notifications are disabled")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("Workflow webhook URL file: %w", err)
	}
	lease, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	defer func() { _ = lease.Close(context.Background()) }()
	n := chteams.NewWorkflowNotifier(strings.TrimSpace(string(raw)), lease.Client)
	if err := n.Notify(cmd.Context(), channel.OwnerNotice{Heading: "✅ Codex helper notification test", Hint: "Workflow card delivery is configured."}); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams workflow test card accepted.")
	return nil
}
