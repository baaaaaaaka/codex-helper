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
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

const (
	defaultTeamsOwnerStaleAfter       = 5 * time.Minute
	defaultTeamsChatRecreateDrainTime = 2 * time.Minute
)

func newTeamsCmd(root *rootOptions) *cobra.Command {
	var registryPath string

	cmd := &cobra.Command{
		Use:   "teams",
		Short: "Run the Microsoft Teams bridge",
		Long:  "Run the Microsoft Teams bridge for Codex helper control and work chats. By default, local status and doctor commands are read-only; commands that create chats or send files say so in their help.",
	}
	cmd.PersistentFlags().StringVar(&registryPath, "registry", "", "Override Teams bridge registry path")
	cmd.AddCommand(
		newTeamsSetupCmd(),
		newTeamsAuthCmd(root),
		newTeamsControlCmd(root, &registryPath),
		newTeamsChatCmd(root, &registryPath),
		newTeamsWorkflowCmd(root, &registryPath),
		newTeamsProbeChatCmd(root),
		newTeamsRunCmd(root, &registryPath),
		newTeamsSendFileCmd(root, &registryPath),
		newTeamsStatusCmd(&registryPath),
		newTeamsDoctorCmd(root, &registryPath),
		newTeamsServiceCmd(root, &registryPath),
		newTeamsPauseCmd(),
		newTeamsResumeCmd(),
		newTeamsDrainCmd(),
		newTeamsRecoverCmd(),
	)
	return cmd
}

func newTeamsSetupCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "setup",
		Short: "Show the safe setup checklist for Teams mode",
		Long:  "Show a local-only checklist for setting up Teams mode. This command does not authenticate, create chats, upload files, or send Teams messages.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintln(out, "Teams setup checklist")
			_, _ = fmt.Fprintln(out, "1. Configure your tenant and Teams Graph client ID with `codex-proxy teams auth config --tenant-id <tenant-id> --client-id <client-id>`.")
			_, _ = fmt.Fprintln(out, "2. Run `codex-proxy teams auth full` in a foreground terminal and finish Microsoft device login for Teams read, send, meeting chat, and file upload.")
			_, _ = fmt.Fprintln(out, "3. Optional: run `codex-proxy teams auth read` later if you want a separate read-only token for low-latency polling experiments.")
			_, _ = fmt.Fprintln(out, "4. Run `codex-proxy teams doctor --live` to verify Graph identity and existing chat read access for your account.")
			_, _ = fmt.Fprintln(out, "5. Run `codex-proxy teams control` to create or show this machine's meeting-based control chat. This command may create a Teams chat and send an @mention plus a ready message.")
			_, _ = fmt.Fprintln(out, "6. Run `codex-proxy teams service doctor` to check the no-root service backend for this platform.")
			_, _ = fmt.Fprintln(out, "7. Start the foreground bridge with `codex-proxy teams run`, or install a user service with `codex-proxy teams service install` followed by `codex-proxy teams service enable` and `codex-proxy teams service start`.")
			_, _ = fmt.Fprintln(out, "8. Foreground `teams run` stops when its terminal exits. Use the service path for terminal close, SSH disconnect, WSL, sleep/wake, and helper upgrade recovery.")
			_, _ = fmt.Fprintln(out, "9. File uploads use the full token by default. Advanced split-token users can run `codex-proxy teams auth file-write` instead.")
			_, _ = fmt.Fprintln(out, "Local checks: `codex-proxy teams status`, `codex-proxy teams control --print`, `codex-proxy teams doctor`, `codex-proxy teams service doctor`.")
			return nil
		},
	}
}

func newTeamsAuthCmd(root *rootOptions) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Authenticate to Microsoft Graph for Teams chat access",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer func() { _ = httpClient.Close(context.Background()) }()
			cfg, err := teams.DefaultAuthConfig()
			if err != nil {
				return err
			}
			auth := teams.NewAuthManagerWithHTTPClient(cfg, httpClient.Client)
			ctx := cmd.Context()
			if _, err := auth.AccessToken(ctx, cmd.OutOrStdout(), force); err != nil {
				return err
			}
			graph := teams.NewGraphClientWithHTTPClient(auth, cmd.OutOrStdout(), httpClient.Client)
			me, err := graph.Me(ctx)
			if err != nil {
				return err
			}
			httpClient.RetireSuspects(ctx, cmd.ErrOrStderr())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Authenticated as %s <%s>\n", me.DisplayName, me.UserPrincipalName)
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force a fresh device-code login")
	cmd.AddCommand(
		newTeamsAuthConfigCmd(),
		newTeamsAuthFullCmd(root),
		newTeamsAuthFullStatusCmd(),
		newTeamsAuthFullLogoutCmd(),
		newTeamsAuthReadCmd(root),
		newTeamsAuthReadStatusCmd(),
		newTeamsAuthReadLogoutCmd(),
		newTeamsAuthStatusCmd(),
		newTeamsLogoutCmd(),
		newTeamsAuthFileWriteCmd(root),
		newTeamsAuthFileWriteStatusCmd(),
		newTeamsAuthFileWriteLogoutCmd(),
	)
	return cmd
}

func newTeamsAuthConfigCmd() *cobra.Command {
	var tenantID string
	var readClientID string
	var chatClientID string
	var fileWriteClientID string
	var fullClientID string
	var readScopes string
	var chatScopes string
	var fileWriteScopes string
	var fullScopes string
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Configure local Teams Graph tenant and client IDs",
		Long:  "Configure local Teams Graph tenant and client IDs. This writes a local user config file; client IDs are not stored in the source tree.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := teams.DefaultTeamsAuthConfigPath()
			if err != nil {
				return err
			}
			cfg, err := teams.LoadTeamsAuthConfigFile(path)
			if err != nil {
				return err
			}
			changed := false
			if cmd.Flags().Changed("tenant-id") {
				cfg.TenantID = strings.TrimSpace(tenantID)
				changed = true
			}
			if cmd.Flags().Changed("read-client-id") {
				cfg.Read.ClientID = strings.TrimSpace(readClientID)
				changed = true
			}
			if cmd.Flags().Changed("chat-client-id") || cmd.Flags().Changed("client-id") {
				cfg.ChatWrite.ClientID = strings.TrimSpace(chatClientID)
				changed = true
			}
			if cmd.Flags().Changed("file-write-client-id") {
				cfg.FileWrite.ClientID = strings.TrimSpace(fileWriteClientID)
				changed = true
			}
			if cmd.Flags().Changed("full-client-id") {
				cfg.Full.ClientID = strings.TrimSpace(fullClientID)
				changed = true
			}
			if cmd.Flags().Changed("read-scopes") {
				cfg.Read.Scopes = strings.TrimSpace(readScopes)
				changed = true
			}
			if cmd.Flags().Changed("chat-scopes") {
				cfg.ChatWrite.Scopes = strings.TrimSpace(chatScopes)
				changed = true
			}
			if cmd.Flags().Changed("file-write-scopes") {
				cfg.FileWrite.Scopes = strings.TrimSpace(fileWriteScopes)
				changed = true
			}
			if cmd.Flags().Changed("full-scopes") {
				cfg.Full.Scopes = strings.TrimSpace(fullScopes)
				changed = true
			}
			if changed {
				if err := teams.SaveTeamsAuthConfigFile(path, cfg); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Saved Teams auth config: %s\n", path)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams auth config: %s\n", path)
			}
			printTeamsAuthConfigSummary(cmd.OutOrStdout(), cfg)
			return nil
		},
	}
	cmd.Flags().StringVar(&tenantID, "tenant-id", "", "Microsoft Entra tenant id")
	cmd.Flags().StringVar(&readClientID, "read-client-id", "", "Public client id for Teams read Graph scopes")
	cmd.Flags().StringVar(&chatClientID, "chat-client-id", "", "Public client id for Teams chat creation/send scopes")
	cmd.Flags().StringVar(&chatClientID, "client-id", "", "Alias for --chat-client-id")
	cmd.Flags().StringVar(&fileWriteClientID, "file-write-client-id", "", "Public client id for Teams file upload scopes; defaults to chat client id when omitted")
	cmd.Flags().StringVar(&fullClientID, "full-client-id", "", "Public client id for one-shot Teams full scopes; defaults to chat client id when omitted")
	cmd.Flags().StringVar(&readScopes, "read-scopes", "", "Override Teams read scopes")
	cmd.Flags().StringVar(&chatScopes, "chat-scopes", "", "Override Teams chat write scopes")
	cmd.Flags().StringVar(&fileWriteScopes, "file-write-scopes", "", "Override Teams file write scopes")
	cmd.Flags().StringVar(&fullScopes, "full-scopes", "", "Override Teams one-shot full scopes")
	return cmd
}

func printTeamsAuthConfigSummary(out io.Writer, cfg teams.TeamsAuthConfigFile) {
	_, _ = fmt.Fprintf(out, "Tenant ID: %s\n", configuredStatus(cfg.TenantID))
	_, _ = fmt.Fprintf(out, "Read client ID: %s\n", configuredStatus(cfg.Read.ClientID))
	_, _ = fmt.Fprintf(out, "Chat write client ID: %s\n", configuredStatus(cfg.ChatWrite.ClientID))
	fileStatus := configuredStatus(cfg.FileWrite.ClientID)
	if strings.TrimSpace(cfg.FileWrite.ClientID) == "" && strings.TrimSpace(cfg.ChatWrite.ClientID) != "" {
		fileStatus = "using chat write client"
	}
	_, _ = fmt.Fprintf(out, "File write client ID: %s\n", fileStatus)
	fullStatus := configuredStatus(cfg.Full.ClientID)
	if strings.TrimSpace(cfg.Full.ClientID) == "" && strings.TrimSpace(cfg.ChatWrite.ClientID) != "" {
		fullStatus = "using chat write client"
	}
	_, _ = fmt.Fprintf(out, "Full client ID: %s\n", fullStatus)
}

func configuredStatus(value string) string {
	if strings.TrimSpace(value) == "" {
		return "missing"
	}
	return "configured"
}

func newTeamsAuthFullCmd(root *rootOptions) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "full",
		Short: "Authenticate once for Teams read, send, meeting chats, and file uploads",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer func() { _ = httpClient.Close(context.Background()) }()
			auth, err := newTeamsFullAuthManagerWithHTTPClient(httpClient.Client)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			if _, err := auth.AccessToken(ctx, cmd.OutOrStdout(), force); err != nil {
				return err
			}
			graph := teams.NewGraphClientWithHTTPClient(auth, cmd.OutOrStdout(), httpClient.Client)
			me, err := graph.Me(ctx)
			if err != nil {
				return err
			}
			httpClient.RetireSuspects(ctx, cmd.ErrOrStderr())
			cfg, _ := teams.DefaultFullAuthConfig()
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Authenticated Teams full access as %s <%s>\n", me.DisplayName, me.UserPrincipalName)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Full token cache: %s\n", cfg.CachePath)
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force a fresh device-code login")
	return cmd
}

func newTeamsAuthFullStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "full-status",
		Short: "Show local Teams full auth cache status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := teams.DefaultFullAuthConfig()
			if err != nil {
				return err
			}
			status, err := readTeamsTokenStatus(cfg.CachePath)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams full auth cache: %s\n", status)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Full token cache: %s\n", cfg.CachePath)
			if status == "missing" {
				effectiveCfg, err := teams.DefaultEffectiveFileWriteAuthConfig()
				if err == nil && filepath.Clean(effectiveCfg.CachePath) != filepath.Clean(cfg.CachePath) {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Runtime full fallback cache: %s\n", effectiveCfg.CachePath)
				}
			}
			return nil
		},
	}
}

func newTeamsAuthFullLogoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "full-logout",
		Short: "Remove the local Teams full auth cache",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := teams.DefaultFullAuthConfig()
			if err != nil {
				return err
			}
			if err := teams.RemoveTokenCache(cfg.CachePath); errors.Is(err, os.ErrNotExist) {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams full auth cache already absent: %s\n", cfg.CachePath)
				return nil
			} else if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Removed Teams full auth cache: %s\n", cfg.CachePath)
			return nil
		},
	}
}

func newTeamsAuthReadCmd(root *rootOptions) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "read",
		Short: "Authenticate to Microsoft Graph for Teams message polling",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer func() { _ = httpClient.Close(context.Background()) }()
			cfg, err := teams.DefaultReadAuthConfig()
			if err != nil {
				return err
			}
			auth := teams.NewAuthManagerWithHTTPClient(cfg, httpClient.Client)
			ctx := cmd.Context()
			if _, err := auth.AccessToken(ctx, cmd.OutOrStdout(), force); err != nil {
				return err
			}
			graph := teams.NewGraphClientWithHTTPClient(auth, cmd.OutOrStdout(), httpClient.Client)
			me, err := graph.Me(ctx)
			if err != nil {
				return err
			}
			httpClient.RetireSuspects(ctx, cmd.ErrOrStderr())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Authenticated Teams read access as %s <%s>\n", me.DisplayName, me.UserPrincipalName)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Read token cache: %s\n", cfg.CachePath)
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force a fresh device-code login")
	return cmd
}

func newTeamsAuthReadStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "read-status",
		Short: "Show local Teams read auth cache status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := teams.DefaultReadAuthConfig()
			if err != nil {
				return err
			}
			status, err := readTeamsTokenStatus(cfg.CachePath)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams read auth cache: %s\n", status)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Read token cache: %s\n", cfg.CachePath)
			return nil
		},
	}
}

func newTeamsAuthReadLogoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "read-logout",
		Short: "Remove the local Teams read auth cache",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := teams.DefaultReadAuthConfig()
			if err != nil {
				return err
			}
			if err := teams.RemoveTokenCache(cfg.CachePath); errors.Is(err, os.ErrNotExist) {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams read auth cache already absent: %s\n", cfg.CachePath)
				return nil
			} else if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Removed Teams read auth cache: %s\n", cfg.CachePath)
			return nil
		},
	}
}

func newTeamsControlCmd(root *rootOptions, registryPath *string) *cobra.Command {
	var noCreate bool
	var recreate bool
	var yes bool
	var recreateDrainTimeout time.Duration
	cmd := &cobra.Command{
		Use:   "control",
		Short: "Show or create the Teams control chat",
		Long:  "Show, create, or recreate this machine's meeting-based Teams control chat. Without --no-create, this may call Microsoft Graph to create the chat, update its title, and send an @mention plus a ready message.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if noCreate && recreate {
				return fmt.Errorf("use only one of --no-create/--print or --recreate")
			}
			if noCreate {
				return printTeamsControlChatLocal(cmd, *registryPath)
			}
			if recreate && !yes {
				return fmt.Errorf("recreating the control chat creates a new Teams chat and sends messages; rerun with --yes")
			}
			if recreate {
				if err := drainTeamsBridgeForChatRecreate(cmd.Context(), cmd.OutOrStdout(), recreateDrainTimeout); err != nil {
					return err
				}
			}
			httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer func() { _ = httpClient.Close(context.Background()) }()
			auth, err := newTeamsAuthManagerWithHTTPClient(httpClient.Client)
			if err != nil {
				return err
			}
			bridge, err := teams.NewBridgeWithHTTPClient(cmd.Context(), auth, *registryPath, cmd.OutOrStdout(), httpClient.Client)
			if err != nil {
				return err
			}
			httpClient.RetireSuspects(cmd.Context(), cmd.ErrOrStderr())
			var chat teams.Chat
			var old teams.Chat
			if recreate {
				recreated, err := bridge.RecreateControlChat(cmd.Context())
				if err != nil {
					return err
				}
				chat = recreated.NewChat
				old = recreated.OldChat
			} else {
				chat, err = bridge.EnsureControlChat(cmd.Context())
				if err != nil {
					return err
				}
			}
			if err := bridge.Save(); err != nil {
				return err
			}
			if recreate {
				printTeamsControlChatDetails(cmd.OutOrStdout(), "Teams control chat recreated", chat.ID, chat.Topic, chat.WebURL)
				if old.ID != "" {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Previous Chat ID: %s\n", old.ID)
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Restart or reload the Teams helper so the running listener uses the new control chat.")
				return nil
			}
			printTeamsControlChatDetails(cmd.OutOrStdout(), "Teams control chat ready", chat.ID, chat.Topic, chat.WebURL)
			return nil
		},
	}
	cmd.Flags().BoolVar(&noCreate, "no-create", false, "Only print the locally known control chat; do not call Graph or create/update/send")
	cmd.Flags().BoolVar(&noCreate, "print", false, "Alias for --no-create")
	cmd.Flags().BoolVar(&recreate, "recreate", false, "Create a fresh meeting-based control chat and rebind local Teams helper state; old Teams chats are not deleted")
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm that --recreate may create a Teams chat and send an @mention plus a ready message")
	cmd.Flags().DurationVar(&recreateDrainTimeout, "drain-timeout", defaultTeamsChatRecreateDrainTime, "How long to wait for the running Teams listener to drain before recreating")
	return cmd
}

func newTeamsChatCmd(root *rootOptions, registryPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chat",
		Short: "Developer maintenance for Teams work chats",
		Long:  "Developer maintenance for Teams work chats. These commands may create Teams chats and send messages; they never delete old Teams chats.",
	}
	cmd.AddCommand(newTeamsChatRecreateCmd(root, registryPath))
	return cmd
}

func newTeamsChatRecreateCmd(root *rootOptions, registryPath *string) *cobra.Command {
	var yes bool
	var recreateDrainTimeout time.Duration
	cmd := &cobra.Command{
		Use:   "recreate <session-id|codex-thread-id|teams-chat-id>",
		Short: "Create a fresh Teams work chat for an existing session",
		Long:  "Create a fresh meeting-based Teams work chat for an existing helper session and rebind local state. The old Teams chat is left untouched.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !yes {
				return fmt.Errorf("recreating a work chat creates a new Teams chat and sends messages; rerun with --yes")
			}
			if err := drainTeamsBridgeForChatRecreate(cmd.Context(), cmd.OutOrStdout(), recreateDrainTimeout); err != nil {
				return err
			}
			httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer func() { _ = httpClient.Close(context.Background()) }()
			auth, err := newTeamsAuthManagerWithHTTPClient(httpClient.Client)
			if err != nil {
				return err
			}
			bridge, err := teams.NewBridgeWithHTTPClient(cmd.Context(), auth, *registryPath, cmd.OutOrStdout(), httpClient.Client)
			if err != nil {
				return err
			}
			httpClient.RetireSuspects(cmd.Context(), cmd.ErrOrStderr())
			recreated, err := bridge.RecreateSessionChat(cmd.Context(), args[0], teams.RecreateSessionChatOptions{})
			if err != nil {
				return err
			}
			if err := bridge.Save(); err != nil {
				return err
			}
			printTeamsControlChatDetails(cmd.OutOrStdout(), "Teams work chat recreated", recreated.NewChat.ID, recreated.NewChat.Topic, recreated.NewChat.WebURL)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Session: %s\n", recreated.SessionID)
			if recreated.OldChat.ID != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Previous Chat ID: %s\n", recreated.OldChat.ID)
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Restart or reload the Teams helper so the running listener uses the new work chat.")
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm that this may create a Teams chat and send an @mention plus a ready message")
	cmd.Flags().DurationVar(&recreateDrainTimeout, "drain-timeout", defaultTeamsChatRecreateDrainTime, "How long to wait for the running Teams listener to drain before recreating")
	return cmd
}

func drainTeamsBridgeForChatRecreate(ctx context.Context, out io.Writer, timeout time.Duration) error {
	paths, err := existingTeamsStorePaths()
	if err != nil {
		return err
	}
	type recreateStore struct {
		Path string
		St   *teamsstore.Store
	}
	var stores []recreateStore
	for _, path := range paths {
		st, err := teamsstore.Open(path)
		if err != nil {
			return err
		}
		state, err := st.Load(ctx)
		if err != nil {
			return err
		}
		owner, hasOwner := stateOwner(state)
		if !hasOwner {
			continue
		}
		if teamsstore.IsStale(owner, defaultTeamsOwnerStaleAfter, time.Now()) {
			return fmt.Errorf("Teams bridge owner appears stale in %s; run `codex-proxy teams recover` before recreating chats", path)
		}
		if _, err := st.SetDraining(ctx, "chat recreate"); err != nil {
			return err
		}
		stores = append(stores, recreateStore{Path: path, St: st})
	}
	if len(stores) == 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = defaultTeamsChatRecreateDrainTime
	}
	if out != nil {
		_, _ = fmt.Fprintln(out, "Waiting for active Teams listener to drain before recreating chat...")
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(teamsUpgradePollInterval)
	defer tick.Stop()
	for {
		drained := true
		for _, item := range stores {
			itemDrained, err := teamsUpgradeStateDrained(ctx, item.St)
			if err != nil {
				return err
			}
			if !itemDrained {
				drained = false
				break
			}
		}
		if drained {
			for _, item := range stores {
				if _, err := item.St.ClearDrain(ctx); err != nil {
					return err
				}
			}
			if out != nil {
				_, _ = fmt.Fprintln(out, "Teams listener drained.")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			for _, item := range stores {
				_, _ = item.St.ClearDrain(context.Background())
			}
			return fmt.Errorf("timed out waiting for Teams listener to drain before recreating chat; run `codex-proxy teams status` or `codex-proxy teams recover --force` if the owner is gone")
		case <-tick.C:
		}
	}
}

func newTeamsRunCmd(root *rootOptions, registryPath *string) *cobra.Command {
	var interval time.Duration
	var once bool
	var top int
	var executorName string
	var runnerName string
	var codexPath string
	var workDir string
	var codexArgs []string
	var controlFallbackModel string
	var timeout time.Duration
	var ownerStaleAfter time.Duration
	var maxWorkChatPolls int
	var upgradeCodex bool
	var autoUpdate bool
	var autoUpdateRepo string
	var autoUpdatePrerelease bool
	var autoService bool
	cmd := &cobra.Command{
		Use:     "run",
		Aliases: []string{"listen"},
		Short:   "Poll Teams chats and route session messages to Codex",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if upgradeCodex {
				if err := runTeamsUpgradeCodexOnce(cmd, root, codexPath); err != nil {
					return err
				}
			}
			runOnce := func() error {
				if autoService && !once {
					if err := ensureTeamsServiceForRun(cmd.Context(), registryPath); err != nil {
						_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Teams service auto-ensure warning: %v\n", err)
					}
				}
				httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
				if err != nil {
					return err
				}
				defer func() { _ = httpClient.Close(context.Background()) }()
				auth, err := newTeamsAuthManagerWithHTTPClient(httpClient.Client)
				if err != nil {
					return err
				}
				bridge, err := teams.NewBridgeWithHTTPClient(cmd.Context(), auth, *registryPath, cmd.OutOrStdout(), httpClient.Client)
				if err != nil {
					return err
				}
				httpClient.RetireSuspects(cmd.Context(), cmd.ErrOrStderr())
				executor, err := newTeamsExecutor(root, executorName, runnerName, codexPath, workDir, codexArgs, timeout, cmd.ErrOrStderr())
				if err != nil {
					return err
				}
				controlFallbackExecutor, err := newTeamsControlFallbackExecutor(root, runnerName, codexPath, workDir, codexArgs, controlFallbackModel, timeout, cmd.ErrOrStderr())
				if err != nil {
					return err
				}
				var helperAutoUpdater teams.HelperAutoUpdater
				if autoUpdate {
					helperAutoUpdater = newTeamsReleaseAutoUpdater(autoUpdateRepo, autoUpdatePrerelease)
				}
				return bridge.Listen(cmd.Context(), teams.BridgeOptions{
					RegistryPath:               *registryPath,
					HelperVersion:              buildVersion(),
					Interval:                   interval,
					Once:                       once,
					Top:                        top,
					OwnerStaleAfter:            ownerStaleAfter,
					MaxWorkChatPollsPerCycle:   maxWorkChatPolls,
					Executor:                   executor,
					ControlFallbackExecutor:    controlFallbackExecutor,
					ControlFallbackModel:       controlFallbackModel,
					ControlFallbackHelpContext: teamsControlFallbackHelpContext(),
					HelperRestarter:            restartTeamsHelperFromTeams,
					HelperPendingRestarter:     restartTeamsHelperFromTeamsAfterPendingReplacement,
					HelperReloader:             reloadTeamsHelperFromTeams,
					HelperAutoUpdater:          helperAutoUpdater,
					HelperAutoUpdatePrerelease: autoUpdatePrerelease,
					CodexUpgrader: func(ctx context.Context) (teams.CodexUpgradeResult, error) {
						return runTeamsCodexUpgradeFromBridge(ctx, root, cmd.ErrOrStderr(), codexPath)
					},
				})
			}
			if teamsRunShouldRetryInProcess(once) {
				return runTeamsServiceRetryLoop(cmd.Context(), cmd.ErrOrStderr(), runOnce)
			}
			return runOnce()
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "Polling interval")
	cmd.Flags().BoolVar(&once, "once", false, "Poll once and exit")
	cmd.Flags().IntVar(&top, "top", 20, "Messages to inspect per chat per poll")
	cmd.Flags().StringVar(&executorName, "executor", "codex", "Executor for session messages: codex or echo")
	cmd.Flags().StringVar(&runnerName, "runner", "exec", "Codex runner for --executor codex: exec or appserver")
	cmd.Flags().StringVar(&codexPath, "codex-path", "", "Override Codex CLI path")
	cmd.Flags().StringVar(&workDir, "workdir", "", "Working directory for Codex sessions")
	cmd.Flags().StringArrayVar(&codexArgs, "codex-arg", nil, "Extra argument to pass to codex exec (repeatable)")
	cmd.Flags().StringVar(&controlFallbackModel, "control-fallback-model", teams.DefaultControlFallbackModel, "Optional Codex model override for unrecognized control-chat requests; empty uses Codex default")
	cmd.Flags().DurationVar(&timeout, "codex-timeout", 0, "Timeout for each Codex turn; 0 disables the helper-enforced turn timeout")
	cmd.Flags().DurationVar(&ownerStaleAfter, "owner-stale-after", defaultTeamsOwnerStaleAfter, "How long a Teams helper owner can miss heartbeats before recovery or another helper may take over")
	cmd.Flags().IntVar(&maxWorkChatPolls, "max-work-chat-polls", teams.DefaultMaxWorkChatPollsPerCycle, "Maximum work chats to read per poll cycle")
	cmd.Flags().BoolVar(&upgradeCodex, "upgrade-codex", false, "Upgrade Codex CLI once before starting Teams polling")
	cmd.Flags().BoolVar(&autoUpdate, "auto-update", true, "Check codex-helper GitHub releases periodically and apply eligible p0/p1 helper updates")
	cmd.Flags().StringVar(&autoUpdateRepo, "auto-update-repo", "", "Override GitHub repo for Teams helper auto-update checks")
	cmd.Flags().BoolVar(&autoUpdatePrerelease, "auto-update-prerelease", false, "Allow Teams helper auto-update checks to select eligible GitHub prereleases")
	cmd.Flags().BoolVar(&autoService, "auto-service", true, "Automatically repair and start the per-user background service when supported")
	return cmd
}

var (
	teamsRunServiceRetryDelay = 30 * time.Second
	teamsRunServiceSleep      = sleepContext
)

func teamsRunShouldRetryInProcess(once bool) bool {
	return !once && strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SERVICE")) != ""
}

func runTeamsServiceRetryLoop(ctx context.Context, errOut io.Writer, runOnce func() error) error {
	for {
		err := runOnce()
		if err == nil || !isRecoverableTeamsRunError(err) {
			return err
		}
		delay := teamsRunServiceRetryDelay
		if delay <= 0 {
			delay = 30 * time.Second
		}
		if errOut != nil {
			_, _ = fmt.Fprintf(errOut, "Teams service recoverable error: %v; retrying in %s\n", err, delay)
		}
		if sleepErr := teamsRunServiceSleep(ctx, delay); sleepErr != nil {
			return sleepErr
		}
	}
}

func isRecoverableTeamsRunError(err error) bool {
	if err == nil {
		return false
	}
	var persistent *teams.PersistentPollFailureError
	if errors.As(err, &persistent) {
		return true
	}
	return teams.IsRecoverablePollFailure(err)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func restartTeamsHelperFromTeams(context.Context) error {
	if err := rejectTeamsHelperSelfManagementFromChild("restart the Teams helper", "helper restart now"); err != nil {
		return err
	}
	if err := helperRestartBeaconBlockerError(); err != nil {
		return err
	}
	if teamsServiceGOOS() == "windows" && strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SERVICE")) != "" {
		if err := scheduleDelayedTeamsServiceStart(context.Background(), ""); err != nil {
			return err
		}
		exitFunc(0)
		return nil
	}
	return restartSelf()
}

func restartTeamsHelperFromTeamsAfterPendingReplacement(ctx context.Context, pendingReplacePath string, installPath string) error {
	if strings.TrimSpace(pendingReplacePath) == "" {
		return restartTeamsHelperFromTeams(ctx)
	}
	if err := rejectTeamsHelperSelfManagementFromChild("restart the Teams helper", "helper restart now"); err != nil {
		return err
	}
	if err := helperRestartBeaconBlockerError(); err != nil {
		return err
	}
	if teamsServiceGOOS() == "windows" {
		if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SERVICE")) != "" {
			if err := scheduleTeamsPendingHelperActivationForReplacement(context.Background(), pendingReplacePath, installPath); err != nil {
				return err
			}
		} else {
			if err := scheduleTeamsPendingHelperProcessRestart(context.Background(), pendingReplacePath, installPath, os.Args[1:]); err != nil {
				return err
			}
		}
		exitFunc(0)
		return nil
	}
	return restartTeamsHelperFromTeams(ctx)
}

func newTeamsStatusCmd(registryPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show local Teams bridge state",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return printTeamsLocalStatus(cmd, *registryPath)
		},
	}
}

func newTeamsSendFileCmd(root *rootOptions, registryPath *string) *cobra.Command {
	var sessionID string
	var chatID string
	var allowLocalPath bool
	var message string
	var uploadFolder string
	var outboundRoot string
	var yes bool
	var verbose bool
	cmd := &cobra.Command{
		Use:   "send-file <path>",
		Short: "Upload a local file to OneDrive and send it as a Teams attachment",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(chatID) != "" && !yes {
				return fmt.Errorf("refusing explicit --chat-id without --yes; prefer --session to avoid sending a file to the wrong Teams chat")
			}
			targetChatID, err := resolveTeamsSendFileChatID(*registryPath, sessionID, chatID)
			if err != nil {
				return err
			}
			if strings.TrimSpace(uploadFolder) == "" {
				uploadFolder = teams.DefaultOutboundUploadFolder()
			}
			file, err := teams.PrepareOutboundAttachment(args[0], teams.OutboundAttachmentOptions{
				Root:         outboundRoot,
				AllowAnyPath: allowLocalPath,
			})
			if err != nil {
				if !allowLocalPath {
					return fmt.Errorf("%w (use --allow-local-path for an explicit local upload)", err)
				}
				return err
			}
			httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer func() { _ = httpClient.Close(context.Background()) }()
			graph, err := teams.NewFileWriteGraphClientWithHTTPClient(cmd.OutOrStdout(), httpClient.Client)
			if err != nil {
				return err
			}
			result, err := teams.SendOutboundAttachment(cmd.Context(), graph, targetChatID, file, teams.OutboundAttachmentOptions{
				UploadFolder: uploadFolder,
				Message:      message,
			})
			if err != nil {
				return err
			}
			httpClient.RetireSuspects(cmd.Context(), cmd.ErrOrStderr())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Sent Teams file attachment: %s\n", result.Item.Name)
			if strings.TrimSpace(sessionID) != "" {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Target session: %s\n", strings.TrimSpace(sessionID))
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Target chat: %s\n", targetChatID)
			}
			if verbose {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Uploaded item id: %s\n", result.Item.ID)
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams message id: %s\n", result.Message.ID)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&sessionID, "session", "", "Send to a known Teams session id")
	cmd.Flags().StringVar(&chatID, "chat-id", "", "Send to an explicit Teams chat id")
	cmd.Flags().BoolVar(&allowLocalPath, "allow-local-path", false, "Allow uploading the explicit local path instead of the Teams outbound root")
	cmd.Flags().StringVar(&message, "message", "", "Message text to send with the attachment")
	cmd.Flags().StringVar(&uploadFolder, "upload-folder", teams.DefaultOutboundUploadFolder(), "OneDrive folder to upload into")
	cmd.Flags().StringVar(&outboundRoot, "outbound-root", "", "Root directory for relative upload paths")
	cmd.Flags().BoolVar(&yes, "yes", false, "Allow an explicit --chat-id target")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Print Graph item and Teams message IDs after upload")
	return cmd
}

func newTeamsDoctorCmd(root *rootOptions, registryPath *string) *cobra.Command {
	var live bool
	var appServerProbe bool
	var codexPath string
	var workDir string
	var probeTimeout time.Duration
	var appServerProbeRuns int
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check local Teams CLI configuration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out := cmd.OutOrStdout()
			_, _ = fmt.Fprintln(out, "Teams doctor")
			if live {
				if err := runTeamsDoctorLiveCheck(cmd, root, *registryPath); err != nil {
					return err
				}
			} else {
				_, _ = fmt.Fprintln(out, "Graph: not checked (doctor is local-only; use --live)")
			}
			if appServerProbe {
				if err := runTeamsAppServerProbe(cmd, teamsAppServerProbeOptions{
					CodexPath: codexPath,
					WorkDir:   workDir,
					Timeout:   probeTimeout,
					Runs:      appServerProbeRuns,
				}); err != nil {
					return err
				}
			} else {
				_, _ = fmt.Fprintln(out, "Codex app-server: not checked (use --appserver-probe)")
			}
			if err := printTeamsAuthDoctorSummary(out); err != nil {
				return err
			}
			if err := printTeamsLocalStatus(cmd, *registryPath); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(out, "Next steps: run `codex-proxy teams setup` for the safe setup checklist.")
			return nil
		},
	}
	cmd.Flags().BoolVar(&live, "live", false, "Check Microsoft Graph auth and Teams read access")
	cmd.Flags().BoolVar(&appServerProbe, "appserver-probe", false, "Probe codex app-server compatibility without starting a model turn")
	cmd.Flags().StringVar(&codexPath, "codex-path", "", "Override Codex CLI path for --appserver-probe")
	cmd.Flags().StringVar(&workDir, "workdir", "", "Working directory for --appserver-probe")
	cmd.Flags().DurationVar(&probeTimeout, "probe-timeout", 10*time.Second, "Timeout for --appserver-probe")
	cmd.Flags().IntVar(&appServerProbeRuns, "appserver-probe-runs", 1, "Number of cold app-server probes to run")
	return cmd
}

func newTeamsPauseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pause [reason]",
		Short: "Pause Teams processing",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			stores, err := openTeamsStoresForControl()
			if err != nil {
				return err
			}
			var control teamsstore.ServiceControl
			for _, item := range stores {
				control, err = item.Store.SetPaused(cmd.Context(), true, strings.Join(args, " "))
				if err != nil {
					return err
				}
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams processing paused%s (%d state file(s))\n", formatControlReason(control), len(stores))
			return nil
		},
	}
}

func newTeamsResumeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resume",
		Short: "Resume Teams processing",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			stores, err := openTeamsStoresForControl()
			if err != nil {
				return err
			}
			var control teamsstore.ServiceControl
			for _, item := range stores {
				control, err = item.Store.SetPaused(cmd.Context(), false, "")
				if err != nil {
					return err
				}
				control, err = item.Store.ClearDrain(cmd.Context())
				if err != nil {
					return err
				}
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams processing resumed%s (%d state file(s))\n", formatControlReason(control), len(stores))
			return nil
		},
	}
}

func newTeamsDrainCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "drain [reason]",
		Short: "Drain queued Teams work",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			stores, err := openTeamsStoresForControl()
			if err != nil {
				return err
			}
			var control teamsstore.ServiceControl
			for _, item := range stores {
				control, err = item.Store.SetDraining(cmd.Context(), strings.Join(args, " "))
				if err != nil {
					return err
				}
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams processing draining%s (%d state file(s))\n", formatControlReason(control), len(stores))
			return nil
		},
	}
}

func newTeamsRecoverCmd() *cobra.Command {
	var force bool
	var staleAfter time.Duration
	cmd := &cobra.Command{
		Use:   "recover",
		Short: "Mark ambiguous local Teams turns as interrupted",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			summary, err := recoverTeamsStores(cmd.Context(), force, staleAfter)
			if err != nil {
				return err
			}
			printTeamsRecoverSummary(cmd.OutOrStdout(), summary)
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Recover even when the Teams bridge owner still appears active")
	cmd.Flags().DurationVar(&staleAfter, "stale-after", 2*time.Minute, "Owner heartbeat age after which recover may proceed without --force")
	return cmd
}

type teamsRecoverSummary struct {
	StateUnavailable  string
	ClearedOwners     []string
	RecoveredTurns    []string
	SupersededOutbox  []string
	PreservedOutbox   []string
	RemainingBlockers []string
}

func recoverTeamsStores(ctx context.Context, force bool, staleAfter time.Duration) (teamsRecoverSummary, error) {
	paths, err := existingTeamsStorePaths()
	if err != nil {
		return teamsRecoverSummary{}, err
	}
	if len(paths) == 0 {
		path, err := teamsStorePath()
		if err != nil {
			return teamsRecoverSummary{}, err
		}
		return teamsRecoverSummary{StateUnavailable: path}, nil
	}
	var summary teamsRecoverSummary
	for _, path := range paths {
		st, err := teamsstore.Open(path)
		if err != nil {
			return teamsRecoverSummary{}, err
		}
		owner, ok, err := st.ReadOwner(ctx)
		if err != nil {
			return teamsRecoverSummary{}, err
		}
		if ok && !force && !teamsstore.IsStale(owner, staleAfter, time.Now()) {
			return teamsRecoverSummary{}, fmt.Errorf("Teams bridge owner is active in %s: pid=%d host=%s active_session=%s active_turn=%s; run `teams drain` first or use `teams recover --force` if the process is gone", path, owner.PID, owner.Hostname, owner.ActiveSessionID, owner.ActiveTurnID)
		}
		if ok && (force || teamsstore.IsStale(owner, staleAfter, time.Now())) {
			if err := st.ClearOwner(ctx); err != nil {
				return teamsRecoverSummary{}, err
			}
			summary.ClearedOwners = append(summary.ClearedOwners, fmt.Sprintf("%s pid=%d host=%s active_session=%s active_turn=%s", path, owner.PID, owner.Hostname, owner.ActiveSessionID, owner.ActiveTurnID))
		}
		report, err := st.Recover(ctx)
		if err != nil {
			return teamsRecoverSummary{}, err
		}
		for _, id := range report.InterruptedTurnIDs {
			summary.RecoveredTurns = append(summary.RecoveredTurns, path+" "+id)
		}
		for _, id := range report.SupersededOutboxIDs {
			summary.SupersededOutbox = append(summary.SupersededOutbox, path+" "+id)
		}
		for _, id := range report.PreservedOutboxBlockerIDs {
			summary.PreservedOutbox = append(summary.PreservedOutbox, path+" "+id)
		}
		state, err := st.Load(ctx)
		if err != nil {
			return teamsRecoverSummary{}, err
		}
		if blockers := teamsUpgradeBlockers(state); len(blockers) > 0 {
			summary.RemainingBlockers = append(summary.RemainingBlockers, path+" "+teamsUpgradeBlockerSummary(blockers))
		}
	}
	sort.Strings(summary.ClearedOwners)
	sort.Strings(summary.RecoveredTurns)
	sort.Strings(summary.SupersededOutbox)
	sort.Strings(summary.PreservedOutbox)
	sort.Strings(summary.RemainingBlockers)
	return summary, nil
}

func printTeamsRecoverSummary(out io.Writer, summary teamsRecoverSummary) {
	if strings.TrimSpace(summary.StateUnavailable) != "" {
		_, _ = fmt.Fprintf(out, "Teams state unavailable: %s\n", summary.StateUnavailable)
		return
	}
	_, _ = fmt.Fprintf(out, "Cleared stale owners: %d\n", len(summary.ClearedOwners))
	for _, id := range summary.ClearedOwners {
		_, _ = fmt.Fprintf(out, "- %s\n", id)
	}
	_, _ = fmt.Fprintf(out, "Recovered interrupted turns: %d\n", len(summary.RecoveredTurns))
	for _, id := range summary.RecoveredTurns {
		_, _ = fmt.Fprintf(out, "- %s\n", id)
	}
	_, _ = fmt.Fprintf(out, "Superseded transient outbox: %d\n", len(summary.SupersededOutbox))
	for _, id := range summary.SupersededOutbox {
		_, _ = fmt.Fprintf(out, "- %s\n", id)
	}
	if len(summary.PreservedOutbox) > 0 {
		_, _ = fmt.Fprintf(out, "Preserved protected outbox: %d\n", len(summary.PreservedOutbox))
		for _, id := range summary.PreservedOutbox {
			_, _ = fmt.Fprintf(out, "- %s\n", id)
		}
	}
	if len(summary.RemainingBlockers) > 0 {
		_, _ = fmt.Fprintf(out, "Remaining upgrade blockers: %d\n", len(summary.RemainingBlockers))
		for _, id := range summary.RemainingBlockers {
			_, _ = fmt.Fprintf(out, "- %s\n", id)
		}
	}
}

func newTeamsAuthStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show local Teams auth cache status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := teams.DefaultAuthConfig()
			if err != nil {
				return err
			}
			status, err := readTeamsTokenStatus(cfg.CachePath)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams auth cache: %s\n", status)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Token cache: %s\n", cfg.CachePath)
			return nil
		},
	}
}

func newTeamsLogoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Remove the local Teams auth cache",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := teams.DefaultAuthConfig()
			if err != nil {
				return err
			}
			if err := teams.RemoveTokenCache(cfg.CachePath); errors.Is(err, os.ErrNotExist) {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams auth cache already absent: %s\n", cfg.CachePath)
				return nil
			} else if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Removed Teams auth cache: %s\n", cfg.CachePath)
			return nil
		},
	}
}

func newTeamsAuthFileWriteCmd(root *rootOptions) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "file-write",
		Short: "Authenticate to Microsoft Graph for Teams file uploads",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			defer func() { _ = httpClient.Close(context.Background()) }()
			cfg, err := teams.DefaultFileWriteAuthConfig()
			if err != nil {
				return err
			}
			auth := teams.NewAuthManagerWithHTTPClient(cfg, httpClient.Client)
			ctx := cmd.Context()
			if _, err := auth.AccessToken(ctx, cmd.OutOrStdout(), force); err != nil {
				return err
			}
			graph := teams.NewGraphClientWithHTTPClient(auth, cmd.OutOrStdout(), httpClient.Client)
			me, err := graph.Me(ctx)
			if err != nil {
				return err
			}
			httpClient.RetireSuspects(ctx, cmd.ErrOrStderr())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Authenticated Teams file upload as %s <%s>\n", me.DisplayName, me.UserPrincipalName)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "File-write token cache: %s\n", cfg.CachePath)
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force a fresh device-code login")
	return cmd
}

func newTeamsAuthFileWriteStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "file-write-status",
		Short: "Show local Teams file-write auth cache status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := teams.DefaultFileWriteAuthConfig()
			if err != nil {
				return err
			}
			status, err := readTeamsTokenStatus(cfg.CachePath)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams file-write auth cache: %s\n", status)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "File-write token cache: %s\n", cfg.CachePath)
			return nil
		},
	}
}

func newTeamsAuthFileWriteLogoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "file-write-logout",
		Short: "Remove the local Teams file-write auth cache",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := teams.DefaultFileWriteAuthConfig()
			if err != nil {
				return err
			}
			if err := teams.RemoveTokenCache(cfg.CachePath); errors.Is(err, os.ErrNotExist) {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams file-write auth cache already absent: %s\n", cfg.CachePath)
				return nil
			} else if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Removed Teams file-write auth cache: %s\n", cfg.CachePath)
			return nil
		},
	}
}

func newTeamsExecutor(root *rootOptions, name string, runnerName string, codexPath string, workDir string, codexArgs []string, timeout time.Duration, log io.Writer) (teams.Executor, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "codex":
		args := codexArgsWithDefaultReasoningEffort(codexArgs, teams.DefaultSessionReasoningEffort)
		return newManagedTeamsCodexExecutor(root, runnerName, codexPath, workDir, args, timeout, log)
	case "echo":
		return teams.EchoExecutor{}, nil
	default:
		return nil, fmt.Errorf("unknown Teams executor %q (expected codex or echo)", name)
	}
}

func newTeamsControlFallbackExecutor(root *rootOptions, runnerName string, codexPath string, workDir string, codexArgs []string, model string, timeout time.Duration, log io.Writer) (teams.Executor, error) {
	model = strings.TrimSpace(model)
	args := codexArgsWithModel(codexArgs, model)
	args = codexArgsWithReasoningEffort(args, teams.DefaultControlFallbackReasoningEffort)
	return newManagedTeamsCodexExecutor(root, runnerName, codexPath, workDir, args, timeout, log)
}

func codexArgsWithDefaultReasoningEffort(args []string, effort string) []string {
	if codexArgsHasConfigOverride(args, teams.CodexReasoningEffortConfigKey) {
		return append([]string{}, args...)
	}
	return codexArgsWithReasoningEffort(args, effort)
}

func codexArgsWithReasoningEffort(args []string, effort string) []string {
	return codexArgsWithConfigOverride(args, teams.CodexReasoningEffortConfigKey, strings.TrimSpace(effort))
}

func codexArgsWithConfigOverride(args []string, key string, value string) []string {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	out := codexArgsWithoutConfigOverride(args, key)
	if key != "" && value != "" {
		out = append(out, "-c", teams.CodexReasoningEffortConfigArg(value))
	}
	return out
}

func codexArgsHasConfigOverride(args []string, key string) bool {
	key = strings.TrimSpace(key)
	for i := 0; i < len(args); i++ {
		trimmed := strings.TrimSpace(args[i])
		switch {
		case trimmed == "-c" || trimmed == "--config":
			if i+1 < len(args) && codexConfigOverrideMatchesKey(args[i+1], key) {
				return true
			}
			i++
		case strings.HasPrefix(trimmed, "-c="):
			if codexConfigOverrideMatchesKey(strings.TrimPrefix(trimmed, "-c="), key) {
				return true
			}
		case strings.HasPrefix(trimmed, "--config="):
			if codexConfigOverrideMatchesKey(strings.TrimPrefix(trimmed, "--config="), key) {
				return true
			}
		}
	}
	return false
}

func codexArgsWithoutConfigOverride(args []string, key string) []string {
	key = strings.TrimSpace(key)
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		trimmed := strings.TrimSpace(arg)
		switch {
		case trimmed == "-c" || trimmed == "--config":
			if i+1 < len(args) && codexConfigOverrideMatchesKey(args[i+1], key) {
				i++
				continue
			}
			out = append(out, arg)
			if i+1 < len(args) {
				i++
				out = append(out, args[i])
			}
		case strings.HasPrefix(trimmed, "-c="):
			if codexConfigOverrideMatchesKey(strings.TrimPrefix(trimmed, "-c="), key) {
				continue
			}
			out = append(out, arg)
		case strings.HasPrefix(trimmed, "--config="):
			if codexConfigOverrideMatchesKey(strings.TrimPrefix(trimmed, "--config="), key) {
				continue
			}
			out = append(out, arg)
		default:
			out = append(out, arg)
		}
	}
	return out
}

func codexConfigOverrideMatchesKey(raw string, key string) bool {
	rawKey, _, ok := strings.Cut(strings.TrimSpace(raw), "=")
	return ok && strings.TrimSpace(rawKey) == strings.TrimSpace(key)
}

func codexArgsWithModel(args []string, model string) []string {
	out := make([]string, 0, len(args)+2)
	skipNext := false
	for _, arg := range args {
		if skipNext {
			skipNext = false
			continue
		}
		trimmed := strings.TrimSpace(arg)
		switch {
		case trimmed == "--model" || trimmed == "-m":
			skipNext = true
			continue
		case strings.HasPrefix(trimmed, "--model=") || strings.HasPrefix(trimmed, "-m="):
			continue
		default:
			out = append(out, arg)
		}
	}
	model = strings.TrimSpace(model)
	if model != "" {
		out = append(out, "--model", model)
	}
	return out
}

var runTeamsDoctorLiveCheck = defaultRunTeamsDoctorLiveCheck

type teamsAppServerProbeOptions struct {
	CodexPath string
	WorkDir   string
	Timeout   time.Duration
	Runs      int
}

var runTeamsAppServerProbe = defaultRunTeamsAppServerProbe

func defaultRunTeamsDoctorLiveCheck(cmd *cobra.Command, root *rootOptions, registryPath string) error {
	out := cmd.OutOrStdout()
	httpClient, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	defer func() { _ = httpClient.Close(context.Background()) }()
	readAuth, err := newTeamsReadAuthManagerWithHTTPClient(httpClient.Client)
	if err != nil {
		return err
	}
	readGraph := teams.NewGraphClientWithHTTPClient(readAuth, out, httpClient.Client)
	me, err := readGraph.Me(cmd.Context())
	if err != nil {
		_, _ = fmt.Fprintf(out, "Graph read auth: failed (%v)\n", err)
		return err
	}
	httpClient.RetireSuspects(cmd.Context(), cmd.ErrOrStderr())
	_, _ = fmt.Fprintf(out, "Graph read auth: ok as %s <%s>\n", me.DisplayName, me.UserPrincipalName)
	writeAuth, err := newTeamsAuthManagerWithHTTPClient(httpClient.Client)
	if err != nil {
		return err
	}
	writeGraph := teams.NewGraphClientWithHTTPClient(writeAuth, out, httpClient.Client)
	if _, err := writeGraph.Me(cmd.Context()); err != nil {
		_, _ = fmt.Fprintf(out, "Graph write auth: failed (%v)\n", err)
		return err
	}
	_, _ = fmt.Fprintln(out, "Graph write auth: ok")
	reg, err := teams.LoadRegistry(registryPath)
	if err != nil {
		return err
	}
	if reg.ControlChatID == "" {
		_, _ = fmt.Fprintln(out, "Graph chat read: skipped (control chat unavailable)")
		return nil
	}
	if _, err := readGraph.ListMessages(cmd.Context(), reg.ControlChatID, 20); err != nil {
		_, _ = fmt.Fprintf(out, "Graph chat read: failed (%v)\n", err)
		return err
	}
	_, _ = fmt.Fprintln(out, "Graph chat read: ok")
	return nil
}

func printTeamsAuthDoctorSummary(out io.Writer) error {
	fullCfg, err := teams.DefaultFullAuthConfig()
	if err != nil {
		printTeamsAuthConfigNotConfigured(out, "Teams full auth cache", err)
	} else {
		fullStatus, err := readTeamsTokenStatus(fullCfg.CachePath)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "Teams full auth cache: %s (%s)\n", fullStatus, fullCfg.CachePath)
	}
	readCfg, err := teams.DefaultEffectiveReadAuthConfig()
	if err != nil {
		printTeamsAuthConfigNotConfigured(out, "Teams read auth cache", err)
	} else {
		readStatus, err := readTeamsTokenStatus(readCfg.CachePath)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "Teams read auth cache: %s (%s)\n", readStatus, readCfg.CachePath)
		if readStatus == "missing" {
			_, _ = fmt.Fprintf(out, "Read auth next step: run `%s` in a foreground terminal.\n", teamsAuthCommandForCache(readCfg.CachePath, "codex-proxy teams auth read"))
		}
	}
	chatCfg, err := teams.DefaultEffectiveAuthConfig()
	if err != nil {
		printTeamsAuthConfigNotConfigured(out, "Teams auth cache", err)
	} else {
		chatStatus, err := readTeamsTokenStatus(chatCfg.CachePath)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "Teams auth cache: %s (%s)\n", chatStatus, chatCfg.CachePath)
		if chatStatus == "missing" {
			_, _ = fmt.Fprintf(out, "Auth next step: run `%s` in a foreground terminal.\n", teamsAuthCommandForCache(chatCfg.CachePath, "codex-proxy teams auth"))
		}
	}
	fileCfg, err := teams.DefaultEffectiveFileWriteAuthConfig()
	if err != nil {
		printTeamsAuthConfigNotConfigured(out, "Teams file-write auth cache", err)
	} else {
		fileStatus, err := readTeamsTokenStatus(fileCfg.CachePath)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "Teams file-write auth cache: %s (%s)\n", fileStatus, fileCfg.CachePath)
		if fileStatus == "missing" {
			_, _ = fmt.Fprintf(out, "File upload next step: run `%s` before using `helper file <relative-path>` or `codex-proxy teams send-file`.\n", teamsAuthCommandForCache(fileCfg.CachePath, "codex-proxy teams auth file-write"))
		}
	}
	return nil
}

func printTeamsAuthConfigNotConfigured(out io.Writer, label string, err error) {
	_, _ = fmt.Fprintf(out, "%s: not configured (%v)\n", label, err)
}

func defaultRunTeamsAppServerProbe(cmd *cobra.Command, opts teamsAppServerProbeOptions) error {
	out := cmd.OutOrStdout()
	command := strings.TrimSpace(opts.CodexPath)
	if command == "" {
		command = "codex"
	}
	ctx := cmd.Context()
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	started := time.Now()
	probe, err := codexrunner.ProbeAppServerCompatibility(ctx, codexrunner.AppServerProbeOptions{
		Starter:    codexrunner.AppServerProcessStarter{},
		Command:    command,
		WorkingDir: opts.WorkDir,
		Timeout:    opts.Timeout,
		Runs:       opts.Runs,
		Limit:      1,
	})
	elapsed := time.Since(started).Round(time.Millisecond)
	if err != nil {
		_, _ = fmt.Fprintf(out, "Codex app-server: failed after %s (%v)\n", elapsed, err)
		return err
	}
	if len(probe.Runs) == 1 {
		_, _ = fmt.Fprintf(out, "Codex app-server: ok (%d thread(s) listed, %s)\n", probe.Runs[0].ThreadCount, probe.Runs[0].Duration.Round(time.Millisecond))
		return nil
	}
	_, _ = fmt.Fprintf(
		out,
		"Codex app-server: ok (%d cold probe(s), min %s, max %s, total %s)\n",
		len(probe.Runs),
		probe.Min.Round(time.Millisecond),
		probe.Max.Round(time.Millisecond),
		probe.Total.Round(time.Millisecond),
	)
	return nil
}

func newTeamsAuthManager() (*teams.AuthManager, error) {
	return newTeamsAuthManagerWithHTTPClient(nil)
}

func newTeamsAuthManagerWithHTTPClient(client *http.Client) (*teams.AuthManager, error) {
	cfg, err := teams.DefaultEffectiveAuthConfig()
	if err != nil {
		return nil, err
	}
	return teams.NewAuthManagerWithHTTPClient(cfg, client), nil
}

func newTeamsFullAuthManagerWithHTTPClient(client *http.Client) (*teams.AuthManager, error) {
	cfg, err := teams.DefaultFullAuthConfig()
	if err != nil {
		return nil, err
	}
	return teams.NewAuthManagerWithHTTPClient(cfg, client), nil
}

func newTeamsReadAuthManager() (*teams.AuthManager, error) {
	return newTeamsReadAuthManagerWithHTTPClient(nil)
}

func newTeamsReadAuthManagerWithHTTPClient(client *http.Client) (*teams.AuthManager, error) {
	cfg, err := teams.DefaultEffectiveReadAuthConfig()
	if err != nil {
		return nil, err
	}
	return teams.NewAuthManagerWithHTTPClient(cfg, client), nil
}

func newTeamsFileWriteAuthManager() (*teams.AuthManager, error) {
	return newTeamsFileWriteAuthManagerWithHTTPClient(nil)
}

func newTeamsFileWriteAuthManagerWithHTTPClient(client *http.Client) (*teams.AuthManager, error) {
	cfg, err := teams.DefaultEffectiveFileWriteAuthConfig()
	if err != nil {
		return nil, err
	}
	return teams.NewAuthManagerWithHTTPClient(cfg, client), nil
}

func printTeamsLocalStatus(cmd *cobra.Command, registryPath string) error {
	out := cmd.OutOrStdout()
	resolvedRegistryPath, err := teamsRegistryPath(registryPath)
	if err != nil {
		return err
	}
	reg, err := loadTeamsStatusRegistry(registryPath)
	if err != nil {
		return err
	}
	statusSessions := make(map[string]string)
	for _, session := range reg.Sessions {
		addStatusSession(statusSessions, session.ID, session.ChatID, session.Status)
	}
	defaultStatePath, err := teamsStorePath()
	if err != nil {
		return err
	}
	statePaths, err := existingTeamsStorePaths()
	if err != nil {
		return err
	}
	controlChatID := strings.TrimSpace(reg.ControlChatID)
	controlChatTopic := strings.TrimSpace(reg.ControlChatTopic)
	controlChatURL := strings.TrimSpace(reg.ControlChatURL)
	controlChatSource := ""
	if controlChatID != "" {
		controlChatSource = resolvedRegistryPath
	}
	stateSessions := 0
	turns := 0
	queuedOutbox := 0
	runningTurns := 0
	queuedTurns := 0
	combinedPolls := map[string]teamsstore.ChatPollState{}
	var owners []teamsstore.OwnerMetadata
	var serviceControls []teamsstore.ServiceControl
	var controlLeases []string
	for _, statePath := range statePaths {
		st, err := teamsstore.Open(statePath)
		if err != nil {
			return err
		}
		state, err := st.Load(cmd.Context())
		if err != nil {
			return err
		}
		serviceControls = append(serviceControls, state.ServiceControl)
		for _, session := range state.Sessions {
			if session.RunnerKind == "control_fallback" && session.TeamsChatID == "" {
				continue
			}
			stateSessions++
			addStatusSession(statusSessions, session.ID, session.TeamsChatID, string(session.Status))
		}
		for _, msg := range state.OutboxMessages {
			if msg.Status == teamsstore.OutboxStatusQueued {
				queuedOutbox++
			}
		}
		for _, turn := range state.Turns {
			turns++
			switch turn.Status {
			case teamsstore.TurnStatusQueued:
				queuedTurns++
			case teamsstore.TurnStatusRunning:
				runningTurns++
			}
		}
		for chatID, poll := range state.ChatPolls {
			combinedPolls[statePath+" "+chatID] = poll
		}
		if owner, ok := stateOwner(state); ok {
			owners = append(owners, owner)
		}
		if state.ControlLease.HolderMachineID != "" {
			controlLeases = append(controlLeases, fmt.Sprintf("Control lease: holder=%s kind=%s generation=%d until=%s", state.ControlLease.HolderMachineID, state.ControlLease.HolderKind, state.ControlLease.Generation, state.ControlLease.LeaseUntil.Format(time.RFC3339)))
		}
		if state.ControlChat.TeamsChatID != "" && (controlChatID == "" || controlChatSource == resolvedRegistryPath) {
			controlChatID = state.ControlChat.TeamsChatID
			controlChatTopic = state.ControlChat.TeamsChatTopic
			controlChatURL = state.ControlChat.TeamsChatURL
			controlChatSource = statePath
		}
	}
	_, _ = fmt.Fprintln(out, "Teams status")
	_, _ = fmt.Fprintf(out, "Registry: %s\n", resolvedRegistryPath)
	if controlChatID == "" {
		_, _ = fmt.Fprintln(out, "Control chat: unavailable")
	} else if len(owners) == 0 {
		_, _ = fmt.Fprintln(out, "Control chat: configured, listener stopped")
	} else {
		_, _ = fmt.Fprintln(out, "Control chat: configured")
	}
	if controlChatID != "" {
		if controlChatTopic != "" {
			_, _ = fmt.Fprintf(out, "Control chat title: %s\n", controlChatTopic)
		}
		if controlChatURL != "" {
			_, _ = fmt.Fprintf(out, "Control chat URL: %s\n", controlChatURL)
		}
		if controlChatSource != "" && controlChatSource != resolvedRegistryPath {
			_, _ = fmt.Fprintf(out, "Control chat source: %s\n", controlChatSource)
		}
	}
	_, _ = fmt.Fprintf(out, "Sessions: %d total, %d active\n", len(statusSessions), activeStatusSessionCount(statusSessions))
	_, _ = fmt.Fprintf(out, "State: %s\n", defaultStatePath)
	pathStatus := resolveTeamsHelperExecutableStatus()
	if pathStatus.Raw != "" {
		_, _ = fmt.Fprintf(out, "Helper raw executable: %s\n", pathStatus.Raw)
	}
	if pathStatus.Stable != "" {
		_, _ = fmt.Fprintf(out, "Helper stable executable: %s\n", pathStatus.Stable)
	}
	if pathStatus.Source != "" || pathStatus.Reason != "" {
		_, _ = fmt.Fprintf(out, "Helper path resolution: source=%s reason=%s\n", firstNonEmptyCLI(pathStatus.Source, "unknown"), firstNonEmptyCLI(pathStatus.Reason, "unknown"))
	}
	if pathStatus.Warning != "" {
		_, _ = fmt.Fprintf(out, "Helper path warning: %s\n", pathStatus.Warning)
	}
	_, _ = fmt.Fprintf(out, "OS service: %s\n", formatTeamsOSServiceStatus(cmd.Context()))
	_, _ = fmt.Fprintf(out, "Helper version: %s\n", formatTeamsHelperVersionStatus(cmd.Context(), owners))
	if len(statePaths) == 0 {
		_, _ = fmt.Fprintln(out, "Bridge: not running")
		_, _ = fmt.Fprintln(out, "State summary: unavailable")
		return nil
	}
	if len(statePaths) > 1 || statePaths[0] != defaultStatePath {
		_, _ = fmt.Fprintf(out, "State files: %d\n", len(statePaths))
	}
	for _, lease := range controlLeases {
		_, _ = fmt.Fprintln(out, lease)
	}
	if serviceControl, ok := summarizeServiceControls(serviceControls); ok {
		_, _ = fmt.Fprintf(out, "Service control: %s\n", formatServiceControl(serviceControl))
	}
	if len(owners) == 0 {
		_, _ = fmt.Fprintln(out, "Bridge: not running")
		_, _ = fmt.Fprintln(out, "Teams listener: stopped - Teams messages are not being read.")
		_, _ = fmt.Fprintln(out, "To make Teams messages work, run `codex-proxy teams run` or start the Teams service.")
		_, _ = fmt.Fprintln(out, "This must run on the machine named in the control chat; messages sent from your phone cannot start a stopped local listener.")
	} else {
		_, _ = fmt.Fprintln(out, "Bridge: running")
		_, _ = fmt.Fprintln(out, "Teams listener: running")
	}
	_, _ = fmt.Fprintf(out, "State summary: %d sessions, %d turns (%d queued, %d running), %d queued outbox\n", stateSessions, turns, queuedTurns, runningTurns, queuedOutbox)
	_, _ = fmt.Fprintf(out, "Poll summary: %s\n", formatPollSummary(combinedPolls))
	if len(owners) == 0 {
		_, _ = fmt.Fprintln(out, "Owner: none")
	} else {
		for _, owner := range owners {
			_, _ = fmt.Fprintf(out, "Owner: pid=%d host=%s machine=%s generation=%d version=%s last_heartbeat=%s active_session=%s active_turn=%s\n", owner.PID, owner.Hostname, owner.MachineID, owner.LeaseGeneration, firstNonEmptyCLI(owner.HelperVersion, "unknown"), owner.LastHeartbeat.Format(time.RFC3339), owner.ActiveSessionID, owner.ActiveTurnID)
		}
	}
	return nil
}

type teamsHelperExecutableStatus struct {
	Raw               string
	Stable            string
	Source            string
	Reason            string
	Warning           string
	ActivationPending bool
}

func resolveTeamsHelperExecutableStatus() teamsHelperExecutableStatus {
	raw, err := teamsServiceExecutable()
	if err != nil {
		return teamsHelperExecutableStatus{Warning: "raw executable unavailable: " + err.Error()}
	}
	out := teamsHelperExecutableStatus{Raw: raw}
	resolved, resolveErr := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS()})
	if resolveErr != nil {
		class := helperpath.ClassifyPath(raw)
		out.Stable = class.Clean
		out.Reason = class.Reason
		out.Warning = resolveErr.Error()
		out.ActivationPending = true
		return out
	}
	out.Stable = resolved.Path
	out.Source = string(resolved.Source)
	out.Reason = resolved.Reason
	class := helperpath.ClassifyPath(raw)
	if class.Transient || (strings.TrimSpace(resolved.Path) != "" && resolved.Path != strings.TrimSpace(class.Clean)) {
		out.ActivationPending = true
	}
	return out
}

func formatTeamsHelperVersionStatus(ctx context.Context, owners []teamsstore.OwnerMetadata) string {
	ownerVersion := "none"
	if len(owners) > 0 {
		var versions []string
		for _, owner := range owners {
			if v := strings.TrimSpace(owner.HelperVersion); v != "" {
				versions = append(versions, v)
			}
		}
		if len(versions) > 0 {
			ownerVersion = strings.Join(versions, ", ")
		} else {
			ownerVersion = "unknown"
		}
	}
	entryVersion := "unknown"
	entryVersionRaw := ""
	entryVersionComparable := false
	pathStatus := resolveTeamsHelperExecutableStatus()
	installPath := pathStatus.Stable
	if installPath != "" {
		if probed, err := update.ProbeBinaryVersion(ctx, installPath, 5*time.Second); err == nil {
			entryVersionRaw = strings.TrimPrefix(strings.TrimSpace(probed.Version), "v")
			entryVersion = "v" + entryVersionRaw
			if entryVersionRaw != "" {
				_, entryVersionComparable = update.CompareVersions(entryVersionRaw, entryVersionRaw)
			}
		} else {
			entryVersion = "error: " + err.Error()
		}
	}
	pendingText := "none"
	if installPath != "" {
		if pending, err := update.FindPendingReplacementsForPlatform(installPath, teamsServiceGOOS(), runtime.GOARCH); err == nil && len(pending) > 0 {
			var parts []string
			for i, candidate := range pending {
				if i >= 3 {
					parts = append(parts, fmt.Sprintf("+%d more", len(pending)-i))
					break
				}
				parts = append(parts, formatTeamsPendingReplacementStatus(candidate, entryVersionRaw, entryVersionComparable))
			}
			pendingText = strings.Join(parts, ", ")
		}
	}
	activationText := "none"
	if pathStatus.ActivationPending {
		activationText = "pending"
		if pathStatus.Warning != "" {
			activationText += ": " + pathStatus.Warning
		} else if pathStatus.Reason != "" {
			activationText += ": " + pathStatus.Reason
		}
	}
	return fmt.Sprintf("owner=%s, entry=%s, pending=%s, activation=%s", ownerVersion, entryVersion, pendingText, activationText)
}

func formatTeamsPendingReplacementStatus(candidate update.PendingReplacement, entryVersion string, entryVersionComparable bool) string {
	version := "v" + strings.TrimPrefix(strings.TrimSpace(candidate.Version), "v")
	if status, ok := readTeamsPendingActivationStatus(candidate.Path); ok && strings.EqualFold(status, "failed") {
		return "failed:" + version
	}
	if entryVersion == "" {
		return "unknown:" + version
	}
	if cmp, ok := update.CompareVersions(candidate.Version, entryVersion); ok {
		if cmp > 0 {
			return "newer:" + version
		}
		return "stale:" + version
	}
	if entryVersionComparable {
		return "unknown:" + version
	}
	return "unknown:" + version
}

type teamsPendingActivationStatus struct {
	Status string `json:"status,omitempty"`
}

func readTeamsPendingActivationStatus(pendingPath string) (string, bool) {
	pendingPath = strings.TrimSpace(pendingPath)
	if pendingPath == "" {
		return "", false
	}
	data, err := os.ReadFile(pendingPath + ".activation.json")
	if err != nil {
		return "", false
	}
	if len(data) >= 3 && data[0] == 0xef && data[1] == 0xbb && data[2] == 0xbf {
		data = data[3:]
	}
	var status teamsPendingActivationStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return "", false
	}
	statusText := strings.TrimSpace(status.Status)
	return statusText, statusText != ""
}

func formatTeamsOSServiceStatus(ctx context.Context) string {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return "unsupported (" + err.Error() + ")"
	}
	name := backend.Name()
	installed, err := backend.Installed()
	if err != nil {
		return fmt.Sprintf("unknown (%s: %s; %v)", backend.ID(), name, err)
	}
	if !installed {
		return fmt.Sprintf("missing (%s: %s)", backend.ID(), name)
	}
	active, err := backend.Active(ctx)
	if err != nil {
		return fmt.Sprintf("installed, running unknown (%s: %s; %v)", backend.ID(), name, err)
	}
	if active {
		return fmt.Sprintf("installed, running (%s: %s)", backend.ID(), name)
	}
	return fmt.Sprintf("installed, not running (%s: %s)", backend.ID(), name)
}

func printTeamsControlChatLocal(cmd *cobra.Command, registryPath string) error {
	reg, err := teams.LoadRegistry(registryPath)
	if err != nil {
		return err
	}
	if reg.ControlChatID != "" {
		printTeamsControlChatDetails(cmd.OutOrStdout(), "Teams control chat", reg.ControlChatID, reg.ControlChatTopic, reg.ControlChatURL)
		printTeamsControlChatExamples(cmd.OutOrStdout())
		return nil
	}
	statePaths, err := existingTeamsStorePaths()
	if err != nil {
		return err
	}
	for _, statePath := range statePaths {
		st, err := teamsstore.Open(statePath)
		if err != nil {
			return err
		}
		state, err := st.Load(cmd.Context())
		if err != nil {
			return err
		}
		if state.ControlChat.TeamsChatID != "" {
			printTeamsControlChatDetails(cmd.OutOrStdout(), "Teams control chat", state.ControlChat.TeamsChatID, state.ControlChat.TeamsChatTopic, state.ControlChat.TeamsChatURL)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Source: %s\n", statePath)
			printTeamsControlChatExamples(cmd.OutOrStdout())
			return nil
		}
	}
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Teams control chat: unavailable")
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Run `codex-proxy teams control` to create the meeting-based control chat.")
	printTeamsControlChatExamples(cmd.OutOrStdout())
	return nil
}

func printTeamsControlChatDetails(out io.Writer, label string, chatID string, topic string, webURL string) {
	_, _ = fmt.Fprintf(out, "%s: %s\n", label, firstNonEmptyCLI(webURL, chatID, "unavailable"))
	if topic != "" {
		_, _ = fmt.Fprintf(out, "Title: %s\n", topic)
	}
	if chatID != "" {
		_, _ = fmt.Fprintf(out, "Chat ID: %s\n", chatID)
	}
}

func printTeamsControlChatExamples(out io.Writer) {
	_, _ = fmt.Fprintln(out, "Open this Teams chat, type `help`, and send it.")
	_, _ = fmt.Fprintln(out, "After that, try `projects` to choose a folder, `new <directory>` to start repo work, or `status` to check the helper.")
	_, _ = fmt.Fprintln(out, "Keep `codex-proxy teams run` or the Teams service running on this machine; Teams messages are not read after the local listener stops.")
}

func firstNonEmptyCLI(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func resolveTeamsSendFileChatID(registryPath string, sessionID string, explicitChatID string) (string, error) {
	explicitChatID = strings.TrimSpace(explicitChatID)
	sessionID = strings.TrimSpace(sessionID)
	if explicitChatID != "" && sessionID != "" {
		return "", fmt.Errorf("use only one of --chat-id or --session")
	}
	if explicitChatID != "" {
		return explicitChatID, nil
	}
	reg, err := teams.LoadRegistry(registryPath)
	if err != nil {
		return "", err
	}
	if sessionID != "" {
		session := reg.SessionByID(sessionID)
		if session == nil {
			return "", fmt.Errorf("Teams session not found: %s", sessionID)
		}
		return session.ChatID, nil
	}
	return "", fmt.Errorf("choose a target with --session or --chat-id")
}

func formatPollSummary(polls map[string]teamsstore.ChatPollState) string {
	if len(polls) == 0 {
		return "unavailable"
	}
	errorCount := 0
	historicalWindowCount := 0
	activeWindowCount := 0
	activeContinuationCount := 0
	activeReadBackoffCount := 0
	var lastSuccess time.Time
	now := time.Now()
	for _, poll := range polls {
		if poll.LastError != "" {
			errorCount++
		}
		if !poll.LastWindowFullAt.IsZero() {
			historicalWindowCount++
		}
		if poll.LastWindowFullMessage != "" {
			activeWindowCount++
		}
		if poll.ContinuationPath != "" {
			activeContinuationCount++
		}
		if poll.BlockedUntil.After(now) {
			activeReadBackoffCount++
		}
		if poll.LastSuccessfulPollAt.After(lastSuccess) {
			lastSuccess = poll.LastSuccessfulPollAt
		}
	}
	parts := []string{fmt.Sprintf("%d chats", len(polls))}
	if !lastSuccess.IsZero() {
		parts = append(parts, "last_success "+lastSuccess.Format(time.RFC3339))
	}
	parts = append(parts,
		fmt.Sprintf("%d errors", errorCount),
		fmt.Sprintf("%d active window warnings", activeWindowCount),
		fmt.Sprintf("%d historical window warnings", historicalWindowCount),
		fmt.Sprintf("%d active continuations", activeContinuationCount),
		fmt.Sprintf("%d active read backoffs", activeReadBackoffCount),
	)
	return strings.Join(parts, ", ")
}

func openTeamsStore() (*teamsstore.Store, error) {
	path, err := teamsStorePath()
	if err != nil {
		return nil, err
	}
	return teamsstore.Open(path)
}

type teamsStoreHandle struct {
	Path  string
	Store *teamsstore.Store
}

func openTeamsStoresForControl() ([]teamsStoreHandle, error) {
	paths, err := existingTeamsStorePaths()
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		path, err := teamsStorePath()
		if err != nil {
			return nil, err
		}
		paths = []string{path}
	}
	out := make([]teamsStoreHandle, 0, len(paths))
	for _, path := range paths {
		st, err := teamsstore.Open(path)
		if err != nil {
			return nil, err
		}
		out = append(out, teamsStoreHandle{Path: path, Store: st})
	}
	return out, nil
}

func formatServiceControl(control teamsstore.ServiceControl) string {
	status := "running"
	switch {
	case control.Paused && control.Draining:
		status = "paused, draining"
	case control.Paused:
		status = "paused"
	case control.Draining:
		status = "draining"
	}
	if control.Reason != "" {
		status += " (" + control.Reason + ")"
	}
	if !control.UpdatedAt.IsZero() {
		status += ", updated " + control.UpdatedAt.Format(time.RFC3339)
	}
	return status
}

func summarizeServiceControls(controls []teamsstore.ServiceControl) (teamsstore.ServiceControl, bool) {
	if len(controls) == 0 {
		return teamsstore.ServiceControl{}, false
	}
	best := controls[0]
	bestRank := serviceControlRank(best)
	for _, control := range controls[1:] {
		rank := serviceControlRank(control)
		if rank > bestRank || (rank == bestRank && control.UpdatedAt.After(best.UpdatedAt)) {
			best = control
			bestRank = rank
		}
	}
	return best, true
}

func serviceControlRank(control teamsstore.ServiceControl) int {
	switch {
	case control.Paused && control.Draining:
		return 3
	case control.Draining:
		return 2
	case control.Paused:
		return 1
	default:
		return 0
	}
}

func formatControlReason(control teamsstore.ServiceControl) string {
	if control.Reason == "" {
		return ""
	}
	return ": " + control.Reason
}

func teamsRegistryPath(registryPath string) (string, error) {
	if strings.TrimSpace(registryPath) != "" {
		return registryPath, nil
	}
	return teams.DefaultRegistryPath()
}

func loadTeamsStatusRegistry(registryPath string) (teams.Registry, error) {
	if strings.TrimSpace(registryPath) != "" {
		return teams.LoadRegistry(registryPath)
	}
	paths, err := existingTeamsRegistryPaths()
	if err != nil {
		return teams.Registry{}, err
	}
	if len(paths) == 0 {
		return teams.Registry{Version: 1}, nil
	}
	var merged teams.Registry
	merged.Version = 1
	for _, path := range paths {
		reg, err := teams.LoadRegistry(path)
		if err != nil {
			return teams.Registry{}, err
		}
		mergeTeamsRegistryProjection(&merged, reg)
	}
	return merged, nil
}

func mergeTeamsRegistryProjection(dst *teams.Registry, src teams.Registry) {
	if dst == nil {
		return
	}
	if dst.Version == 0 {
		dst.Version = 1
	}
	if dst.UserID == "" {
		dst.UserID = src.UserID
	}
	if dst.UserPrincipal == "" {
		dst.UserPrincipal = src.UserPrincipal
	}
	if src.ControlChatID != "" {
		dst.ControlChatID = src.ControlChatID
		dst.ControlChatURL = src.ControlChatURL
		dst.ControlChatTopic = src.ControlChatTopic
	}
	for _, session := range src.Sessions {
		if session.ID == "__control_fallback__" {
			continue
		}
		if session.ID != "" && dst.SessionByID(session.ID) != nil {
			continue
		}
		if session.ChatID != "" && dst.SessionByChatID(session.ChatID) != nil {
			continue
		}
		dst.Sessions = append(dst.Sessions, session)
	}
}

func addStatusSession(sessions map[string]string, sessionID string, chatID string, status string) {
	key := strings.TrimSpace(sessionID)
	if key == "" {
		key = strings.TrimSpace(chatID)
	}
	if key == "" || key == "__control_fallback__" {
		return
	}
	status = strings.TrimSpace(status)
	if status == "" {
		status = "active"
	}
	if existing, ok := sessions[key]; ok && existing == "active" {
		return
	}
	sessions[key] = status
}

func activeStatusSessionCount(sessions map[string]string) int {
	active := 0
	for _, status := range sessions {
		if status == "active" {
			active++
		}
	}
	return active
}

func teamsRegistryPaths(registryPath string) ([]string, error) {
	if strings.TrimSpace(registryPath) != "" {
		return []string{registryPath}, nil
	}
	legacy, err := teams.DefaultRegistryPath()
	if err != nil {
		return nil, err
	}
	paths := []string{legacy}
	base := filepath.Dir(legacy)
	matches, err := filepath.Glob(filepath.Join(base, "teams", "scopes", "*", "registry.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	seen := map[string]struct{}{legacy: {}}
	for _, path := range matches {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	return paths, nil
}

func existingTeamsRegistryPaths() ([]string, error) {
	paths, err := teamsRegistryPaths("")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		out = append(out, path)
	}
	return out, nil
}

func teamsStorePath() (string, error) {
	return teamsstore.DefaultPath()
}

func teamsStorePaths() ([]string, error) {
	legacy, err := teamsstore.DefaultPath()
	if err != nil {
		return nil, err
	}
	paths := []string{legacy}
	base := filepath.Dir(legacy)
	matches, err := filepath.Glob(filepath.Join(base, "scopes", "*", "state.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	seen := map[string]struct{}{legacy: {}}
	for _, path := range matches {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	return paths, nil
}

func existingTeamsStorePaths() ([]string, error) {
	paths, err := teamsStorePaths()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}
		out = append(out, path)
	}
	return out, nil
}

func readTeamsTokenStatus(path string) (string, error) {
	return teams.TokenCacheStatus(path)
}

func teamsAuthCommandForCache(path string, fallback string) string {
	if filepath.Base(strings.TrimSpace(path)) == "teams-full-token.json" {
		return "codex-proxy teams auth full"
	}
	return strings.TrimSpace(fallback)
}
