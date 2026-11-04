package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// teamsNewCoreCLIStorePath returns the new core's store for the teams
// commands a user runs (status, workflow). It is used when the new core is
// selected (CODEX_HELPER_TEAMS_CORE=new) or its store exists; use is false
// otherwise and the old core's commands run. err is only set when used.
func teamsNewCoreCLIStorePath() (path string, use bool, err error) {
	selected := teamsEnvSelectsNewCore(map[string]string{envTeamsCore: os.Getenv(envTeamsCore)})
	stateDir, err := appdirs.StateDir()
	if err != nil {
		return "", selected, fmt.Errorf("state directory: %w", err)
	}
	path, err = resolveTeamsCoreStorePath(stateDir)
	if err != nil {
		return "", selected, err
	}
	if _, statErr := os.Stat(path); statErr == nil {
		return path, true, nil
	}
	return path, selected, nil
}

// updateTeamsNewCoreConfig writes configuration to the new core's store.
// Only the owner writes the store, so the command takes ownership for the
// write; the running helper reads its configuration when it starts, so it
// must be stopped first.
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
			return fmt.Errorf("the Teams helper is running and reads this setting only when it starts: stop it with `cxp teams service stop`, run this command again, then start it with `cxp teams service start`")
		case errors.Is(err, corestore.ErrDisabled):
			return fmt.Errorf("the Teams helper is disabled (%s exists next to %s)", corestore.DisabledFile, path)
		}
		return fmt.Errorf("acquire Teams core store: %w", err)
	}
	defer st.Release()
	return st.Update(fn)
}

// setTeamsNewCoreWorkflow turns Workflow cards on with the webhook URL file
// (an absolute, validated path) or off. The file is referenced, not copied,
// as the old core did, so replacing its contents rotates the URL.
func setTeamsNewCoreWorkflow(path string, enabled bool, webhookURLFile string) error {
	return updateTeamsNewCoreConfig(path, func(tx *corestore.Tx) error {
		if enabled {
			if err := tx.SetMeta(migrate.MetaWorkflowSecretFile, webhookURLFile); err != nil {
				return err
			}
		}
		return tx.SetMeta(migrate.MetaWorkflowEnabled, fmt.Sprint(enabled))
	})
}

func runTeamsNewCoreWorkflowEnable(out io.Writer, path, webhookURLFile string) error {
	if err := setTeamsNewCoreWorkflow(path, true, webhookURLFile); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "Teams workflow notifications enabled.")
	_, _ = fmt.Fprintf(out, "Webhook URL file: configured (%s)\n", workflowSecretFileStatus(webhookURLFile))
	_, _ = fmt.Fprintln(out, "Cards are sent from the next start of the Teams helper.")
	return nil
}

func runTeamsNewCoreWorkflowDisable(out io.Writer, path string) error {
	if err := setTeamsNewCoreWorkflow(path, false, ""); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(out, "Teams workflow notifications disabled.")
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
	return nil
}

// runTeamsNewCoreWorkflowTest posts one test card through the webhook the
// helper uses, with the helper's Graph HTTP client (proxy included).
func runTeamsNewCoreWorkflowTest(cmd *cobra.Command, root *rootOptions, path, webhookURLFile string) error {
	if webhookURLFile != "" {
		if err := setTeamsNewCoreWorkflow(path, true, webhookURLFile); err != nil {
			return err
		}
	}
	enabled, file, err := teamsNewCoreWorkflowConfig(path)
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
