package cli

import (
	"context"
	"encoding/json"
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
	st, err := corestore.Open(path)
	if err != nil {
		return fmt.Errorf("open Teams core store: %w", err)
	}
	defer st.Close()
	host, _ := os.Hostname()
	if _, err := st.Acquire(corestore.Ident{Host: host, PID: os.Getpid(), BootID: teamsBootID()}); err != nil {
		switch {
		case errors.Is(err, corestore.ErrOwnerLive):
			return errTeamsHelperRunning
		case errors.Is(err, corestore.ErrDisabled):
			return fmt.Errorf("the Teams helper is disabled (%s exists next to %s)", corestore.DisabledFile, path)
		}
		return fmt.Errorf("acquire Teams core store: %w", err)
	}
	defer st.Release()
	return st.Update(fn)
}

// teamsWorkflowRequestName is the sidecar a command leaves next to the
// store for the running helper, which applies it under its owner fence
// (the old core applied a Workflow change without a restart).
const teamsWorkflowRequestName = "workflow-request.json"

type teamsWorkflowRequest struct {
	Enabled        bool   `json:"enabled"`
	WebhookURLFile string `json:"webhook_url_file,omitempty"`
}

func teamsWorkflowRequestPath(storePath string) string {
	return filepath.Join(filepath.Dir(storePath), teamsWorkflowRequestName)
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
	reqPath := teamsWorkflowRequestPath(path)
	err = updateTeamsNewCoreConfig(path, func(tx *corestore.Tx) error {
		if err := setTeamsWorkflowMeta(tx, r); err != nil {
			return err
		}
		// An older request must not undo this one later.
		if err := os.Remove(reqPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	})
	if !errors.Is(err, errTeamsHelperRunning) {
		return false, err == nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return true, false, err
	}
	tmp := reqPath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return true, false, err
	}
	if err := os.Rename(tmp, reqPath); err != nil {
		_ = os.Remove(tmp)
		return true, false, err
	}
	deadline := time.Now().Add(teamsWorkflowApplyWait)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(reqPath); errors.Is(err, os.ErrNotExist) {
			return true, true, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return true, false, nil
}

// applyTeamsWorkflowRequest applies a pending request as the store's owner
// and removes it. An invalid request (say, the webhook file became
// unreadable) is dropped with an error.
func applyTeamsWorkflowRequest(st *corestore.Store, storePath string) (bool, error) {
	reqPath := teamsWorkflowRequestPath(storePath)
	raw, err := os.ReadFile(reqPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var r teamsWorkflowRequest
	if err := json.Unmarshal(raw, &r); err == nil && r.Enabled {
		_, err = normalizeTeamsWorkflowURLFile(r.WebhookURLFile)
	}
	if err != nil {
		_ = os.Remove(reqPath)
		return false, fmt.Errorf("Workflow request dropped: %w", err)
	}
	if err := st.Update(func(tx *corestore.Tx) error { return setTeamsWorkflowMeta(tx, r) }); err != nil {
		return false, err
	}
	return true, os.Remove(reqPath)
}

// teamsNewCoreNotifier is the engine's Notifier. Like the old core it reads
// the Workflow setting and the webhook file at every card, so a change
// (or a rotated URL) applies without a restart.
type teamsNewCoreNotifier struct {
	st     *corestore.Store
	path   string
	client *http.Client
	log    io.Writer
	mu     sync.Mutex
	last   string
}

func (n *teamsNewCoreNotifier) apply() {
	if ok, err := applyTeamsWorkflowRequest(n.st, n.path); err != nil {
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

func (n *teamsNewCoreNotifier) NotifyFinal(ctx context.Context, conv, title, text, url string) error {
	return chteams.NewWorkflowNotifier(n.url(), n.client).NotifyFinal(ctx, conv, title, text, url)
}

// watch applies a command's request promptly, so the command can report
// it in effect.
func (n *teamsNewCoreNotifier) watch(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := os.Stat(teamsWorkflowRequestPath(n.path)); err == nil {
				n.apply()
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

// teamsNewCoreWorkflowConfig reads the Workflow setting from the store
// ("" file when none is configured).
func teamsNewCoreWorkflowConfig(path string) (enabled bool, file string, err error) {
	if _, statErr := os.Stat(path); statErr != nil {
		return false, "", nil
	}
	st, err := corestore.OpenWith(path, corestore.Options{ReadOnly: true})
	if err != nil {
		return false, "", fmt.Errorf("open Teams core store: %w", err)
	}
	defer st.Close()
	on, _, err := st.Meta(migrate.MetaWorkflowEnabled)
	if err != nil {
		return false, "", err
	}
	file, _, err = st.Meta(migrate.MetaWorkflowSecretFile)
	if err != nil {
		return false, "", err
	}
	file = strings.TrimSpace(file)
	if file != "" && !filepath.IsAbs(file) {
		file = filepath.Join(filepath.Dir(path), filepath.Base(file))
	}
	return strings.EqualFold(strings.TrimSpace(on), "true"), file, nil
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
	if raw, err := os.ReadFile(teamsWorkflowRequestPath(path)); err == nil {
		var r teamsWorkflowRequest
		if json.Unmarshal(raw, &r) == nil {
			_, _ = fmt.Fprintf(out, "Pending change: %s (waiting for the running Teams helper)\n", workflowEnabledLabel(r.Enabled))
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
	if err := n.NotifyFinal(cmd.Context(), "", "notification test",
		"This is a test card from `cxp teams workflow test`. A card like this arrives when Codex finishes a Teams request.", ""); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams workflow test card accepted.")
	return nil
}
