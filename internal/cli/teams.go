package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/asr"
	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
)

const (
	envTeamsASRMirrors     = "CODEX_HELPER_TEAMS_ASR_MIRRORS"
	envTeamsASRCommand     = "CODEX_HELPER_TEAMS_ASR_COMMAND"
	envTeamsASRArgsJSON    = "CODEX_HELPER_TEAMS_ASR_ARGS_JSON"
	envTeamsASRLlamaBinary = "CODEX_HELPER_TEAMS_ASR_LLAMA_BINARY"
	envTeamsASRLlamaModel  = "CODEX_HELPER_TEAMS_ASR_LLAMA_MODEL"
	envTeamsASRLlamaMMProj = "CODEX_HELPER_TEAMS_ASR_LLAMA_MMPROJ"
	envTeamsASRLlamaDevice = "CODEX_HELPER_TEAMS_ASR_LLAMA_DEVICE"
	envTeamsASRFFmpeg      = "CODEX_HELPER_TEAMS_ASR_FFMPEG"
)

func newTeamsCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "teams",
		Short: "Run the Microsoft Teams bridge",
		Long:  "Run the Microsoft Teams bridge for Codex helper control and work chats. Status, doctor and control commands are local and read-only unless their help says otherwise.",
	}
	// Service units written by earlier releases start `teams run --registry
	// <path>`; the helper's state lives in its store now. Accepting the flag
	// lets such a unit start the new version, which then rewrites the unit.
	var registryPath string
	cmd.PersistentFlags().StringVar(&registryPath, "registry", "", "Ignored (kept for service units written by earlier releases)")
	_ = cmd.PersistentFlags().MarkHidden("registry")
	cmd.AddCommand(
		newTeamsSetupCmd(),
		newTeamsAuthCmd(root),
		newTeamsControlCmd(),
		newTeamsChatCmd(),
		newTeamsSendFileCmd(),
		newTeamsWorkflowCmd(root),
		newTeamsRunCmd(root),
		newTeamsStopCmd(),
		newTeamsStartCmd(),
		newTeamsPauseCmd(),
		newTeamsResumeCmd(),
		newTeamsStatusCmd(),
		newTeamsDoctorCmd(root),
		newTeamsPathCmd(root),
		newTeamsServiceCmd(root),
		newTeamsMigrateCmd(),
	)
	rejectUnknownSubcommands(cmd)
	return cmd
}

// rejectUnknownSubcommands makes every command group under cmd fail on an
// unknown subcommand. Cobra checks that on the root command only; a group
// otherwise prints its help and exits 0, so a removed command such as
// `cxp teams recover` would look like it succeeded.
func rejectUnknownSubcommands(cmd *cobra.Command) {
	if cmd.HasSubCommands() && cmd.Run == nil && cmd.RunE == nil {
		cmd.Args = cobra.NoArgs
		cmd.RunE = func(c *cobra.Command, _ []string) error { return c.Help() }
	}
	for _, sub := range cmd.Commands() {
		rejectUnknownSubcommands(sub)
	}
}

func newTeamsSetupCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "setup",
		Short: "Show the safe setup checklist for Teams mode",
		Long:  "Show a local-only checklist for setting up Teams mode. This command does not authenticate, create chats, upload files, or send Teams messages.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			out := cmd.OutOrStdout()
			for _, line := range []string{
				"Teams setup checklist",
				"1. Configure your tenant and Teams Graph client IDs with `cxp teams auth config --tenant-id <tenant-id> --read-client-id <read-client-id> --client-id <write-client-id>`.",
				"2. Run `cxp teams auth read` and `cxp teams auth full` in a foreground terminal when the read and write client IDs differ. If they match, run `cxp teams auth full` once.",
				"3. Run `cxp teams doctor --live` to verify Graph access for your account.",
				"4. Run `cxp teams service doctor` to check the background service backend for this platform.",
				"5. Start the helper with `cxp teams run`; where supported it installs and starts the background service, which keeps the helper running after the terminal closes. The helper creates this machine's control chat when it first starts.",
				"6. A helper store from v0.1.20 or v0.1.22-rc.59 is converted at the first start; the old store is kept unchanged as a backup.",
				"Local checks: `cxp teams status`, `cxp teams control`, `cxp teams doctor`, `cxp teams service status`.",
			} {
				_, _ = fmt.Fprintln(out, line)
			}
			return nil
		},
	}
}

// Auth uses a full delegated token for Graph writes and an optional read token.

func newTeamsAuthCmd(root *rootOptions) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Sign in to Microsoft Graph for Teams chat access",
		Long:  "Sign in to Microsoft Graph with a device code in a foreground terminal. The full token handles writes. An optional read token handles Graph reads.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTeamsAuthLogin(cmd, root, force)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force a fresh device-code login")
	login := &cobra.Command{
		Use:     "login",
		Aliases: []string{"full"},
		Short:   "Sign in (the same as `cxp teams auth`)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTeamsAuthLogin(cmd, root, force)
		},
	}
	login.Flags().BoolVar(&force, "force", false, "Force a fresh device-code login")
	cmd.AddCommand(login, newTeamsAuthReadCmd(root), newTeamsAuthConfigCmd(), newTeamsAuthStatusCmd(), newTeamsAuthReadStatusCmd(), newTeamsLogoutCmd(), newTeamsAuthReadLogoutCmd())
	return cmd
}

func runTeamsAuthLogin(cmd *cobra.Command, root *rootOptions, force bool) error {
	cfg, err := chteams.ResolveAuthConfig("")
	if err != nil {
		return err
	}
	return runTeamsAuthLoginWithConfig(cmd, root, force, cfg, "Authenticated as")
}

func runTeamsAuthReadLogin(cmd *cobra.Command, root *rootOptions, force bool) error {
	cfg, err := chteams.ResolveReadAuthConfig("")
	if err != nil {
		return err
	}
	return runTeamsAuthLoginWithConfig(cmd, root, force, cfg, "Authenticated read access as")
}

func runTeamsAuthLoginWithConfig(cmd *cobra.Command, root *rootOptions, force bool, cfg chteams.AuthConfig, label string) error {
	lease, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	defer func() { _ = lease.Close(context.Background()) }()
	ctx := cmd.Context()
	tok, readErr := chteams.ReadTokenCache(cfg.Cache)
	if force || readErr != nil || tok.RefreshToken == "" {
		if err := chteams.DeviceLogin(ctx, cfg, lease.Client, cmd.OutOrStdout()); err != nil {
			return err
		}
	}
	ch, err := chteams.New(chteams.Config{HTTP: lease.Client, Tokens: chteams.NewDelegatedAuth(cfg, lease.Client), TenantID: cfg.TenantID})
	if err != nil {
		return err
	}
	defer ch.Close()
	_, name, err := ch.Owner(ctx)
	if err != nil {
		return err
	}
	lease.RetireSuspects(ctx, cmd.ErrOrStderr())
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s %s\n", label, name)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Token cache: %s\n", cfg.Cache)
	return nil
}

func newTeamsAuthReadCmd(root *rootOptions) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "read",
		Short: "Sign in to Microsoft Graph for Teams reads",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTeamsAuthReadLogin(cmd, root, force)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force a fresh device-code login")
	return cmd
}

func newTeamsAuthConfigCmd() *cobra.Command {
	var tenantID, clientID, readClientID, scopes, readScopes string
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Configure local Teams Graph tenant and client IDs",
		Long:  "Configure local Teams Graph tenant and client IDs. This writes a local user config file; client IDs are not stored in the source tree.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := chteams.AuthConfigPath()
			if err != nil {
				return err
			}
			set := map[string]string{}
			if cmd.Flags().Changed("tenant-id") {
				set["tenant_id"] = tenantID
			}
			for _, name := range []string{"client-id", "full-client-id", "chat-client-id"} {
				if cmd.Flags().Changed(name) {
					set["client_id"] = clientID
				}
			}
			if cmd.Flags().Changed("read-client-id") {
				set["read_client_id"] = readClientID
			}
			if cmd.Flags().Changed("scopes") {
				set["scopes"] = scopes
			}
			if cmd.Flags().Changed("read-scopes") {
				set["read_scopes"] = readScopes
			}
			if len(set) > 0 {
				if err := writeTeamsAuthConfig(path, set); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Saved Teams auth config: %s\n", path)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams auth config: %s\n", path)
			}
			cfg, err := chteams.ResolveAuthConfig("")
			if err != nil {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Not ready: %v\n", err)
				return nil
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Tenant ID: configured")
			readCfg, readErr := chteams.ResolveReadAuthConfig("")
			if readErr == nil {
				if readCfg.ClientID == cfg.ClientID {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Read client ID: using full client")
				} else {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Read client ID: configured")
				}
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Client ID: configured")
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Scopes: %s\n", cfg.Scopes)
			return nil
		},
	}
	cmd.Flags().StringVar(&tenantID, "tenant-id", "", "Microsoft Entra tenant id")
	cmd.Flags().StringVar(&clientID, "client-id", "", "Public client id of the Teams Graph app")
	cmd.Flags().StringVar(&clientID, "full-client-id", "", "Alias for --client-id")
	cmd.Flags().StringVar(&clientID, "chat-client-id", "", "Alias for --client-id")
	cmd.Flags().StringVar(&readClientID, "read-client-id", "", "Public client id for Teams Graph reads; defaults to --client-id")
	cmd.Flags().StringVar(&scopes, "scopes", "", "Override the delegated Graph scopes")
	cmd.Flags().StringVar(&readScopes, "read-scopes", "", "Override the delegated Graph read scopes")
	_ = cmd.Flags().MarkHidden("full-client-id")
	_ = cmd.Flags().MarkHidden("chat-client-id")
	return cmd
}

// writeTeamsAuthConfig edits the native defaults row without replacing profiles.
func writeTeamsAuthConfig(path string, set map[string]string) error {
	return chteams.UpdateAuthConfig(path, set)
}

func newTeamsAuthStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "status",
		Aliases: []string{"full-status"},
		Short:   "Show the local Teams sign-in state (never the token)",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := chteams.ResolveAuthConfig("")
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams auth: %s\n", teamsTokenCacheStatus(cfg.Cache, time.Now()))
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Token cache: %s\n", cfg.Cache)
			return nil
		},
	}
}

func newTeamsAuthReadStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "read-status",
		Short: "Show the local Teams read sign-in state (never the token)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := chteams.ResolveReadAuthConfig("")
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams read auth: %s\n", teamsTokenCacheStatus(cfg.Cache, time.Now()))
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Read token cache: %s\n", cfg.Cache)
			return nil
		},
	}
}

func newTeamsLogoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "logout",
		Aliases: []string{"full-logout"},
		Short:   "Remove the local Teams token cache",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := chteams.ResolveAuthConfig("")
			if err != nil {
				return err
			}
			// The cache path can be overridden: never remove a file that
			// is not a token cache.
			if tok, err := chteams.ReadTokenCache(cfg.Cache); err == nil && tok.AccessToken == "" && tok.RefreshToken == "" {
				return fmt.Errorf("%s does not look like a Teams token cache; not removed", cfg.Cache)
			} else if err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%v; not removed (it does not look like a Teams token cache)", err)
			}
			if err := chteams.RemoveTokenCache(cmd.Context(), cfg.Cache); errors.Is(err, os.ErrNotExist) {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams token cache already absent: %s\n", cfg.Cache)
				return nil
			} else if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Removed Teams token cache: %s\n", cfg.Cache)
			return nil
		},
	}
}

func newTeamsAuthReadLogoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "read-logout",
		Short: "Remove the local Teams read token cache",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			readCfg, err := chteams.ResolveReadAuthConfig("")
			if err != nil {
				return err
			}
			fullCfg, err := chteams.ResolveAuthConfig("")
			if err != nil {
				return err
			}
			if readCfg.Cache == fullCfg.Cache {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams read auth uses the full token cache: %s\n", readCfg.Cache)
				return nil
			}
			if tok, err := chteams.ReadTokenCache(readCfg.Cache); err == nil && tok.AccessToken == "" && tok.RefreshToken == "" {
				return fmt.Errorf("%s does not look like a Teams token cache; not removed", readCfg.Cache)
			} else if err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%v; not removed (it does not look like a Teams token cache)", err)
			}
			if err := chteams.RemoveTokenCache(cmd.Context(), readCfg.Cache); errors.Is(err, os.ErrNotExist) {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Teams read token cache already absent: %s\n", readCfg.Cache)
				return nil
			} else if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Removed Teams read token cache: %s\n", readCfg.Cache)
			return nil
		},
	}
}

// newTeamsControlCmd prints this machine's control chat from the helper's
// store. The helper creates the chat itself when it starts.
func newTeamsControlCmd() *cobra.Command {
	var printOnly, recreate, yes bool
	cmd := &cobra.Command{
		Use:   "control",
		Short: "Show or recreate this machine's Teams control chat",
		Long:  "Show this machine's Teams control chat, read from the helper's local store. The helper creates the control chat when it first starts. With --recreate --yes the running helper creates a fresh control chat, switches to it and posts a link to it in the old one (which is not deleted).",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if printOnly && recreate {
				return fmt.Errorf("use only one of --no-create/--print or --recreate")
			}
			if recreate {
				if !yes {
					return fmt.Errorf("recreating the control chat creates a new Teams chat and sends messages; rerun with --yes")
				}
				return runTeamsControlRecreate(cmd.OutOrStdout())
			}
			return printTeamsControlChat(cmd.OutOrStdout())
		},
	}
	cmd.Flags().BoolVar(&recreate, "recreate", false, "Create a fresh meeting-based control chat and switch the helper to it; old Teams chats are not deleted")
	cmd.Flags().BoolVar(&yes, "yes", false, "Confirm that --recreate may create a Teams chat and send an @mention plus a ready message")
	teamsDrainTimeoutFlag(cmd)
	// Accepted for scripts written for earlier releases (printing is the
	// only mode now).
	cmd.Flags().BoolVar(&printOnly, "print", false, "Print the control chat (the default)")
	cmd.Flags().BoolVar(&printOnly, "no-create", false, "Print the control chat (the default)")
	_ = cmd.Flags().MarkHidden("print")
	_ = cmd.Flags().MarkHidden("no-create")
	return cmd
}

func printTeamsControlChat(out io.Writer) error {
	view, err := teamsReadStoreView()
	if err != nil {
		return err
	}
	if !view.exists() {
		_, _ = fmt.Fprintln(out, "Teams control chat: none yet (the helper creates it when it first starts: `cxp teams run`)")
		return nil
	}
	c, ok, err := teamsStoredControlChat()
	if err != nil {
		return err
	}
	if !ok {
		_, _ = fmt.Fprintln(out, "Teams control chat: none yet (the helper creates it when it starts)")
		return nil
	}
	_, _ = fmt.Fprintln(out, "Teams control chat")
	if c.Title != "" {
		_, _ = fmt.Fprintf(out, "Title: %s\n", c.Title)
	}
	if c.URL != "" {
		_, _ = fmt.Fprintf(out, "Link: %s\n", c.URL)
	}
	_, _ = fmt.Fprintln(out, "Send `help` in the control chat for its commands.")
	return nil
}

func newTeamsRunCmd(root *rootOptions) *cobra.Command {
	var codexPath string
	var codexArgs []string
	var asrCommand string
	var asrArgs []string
	var ownerStaleAfter time.Duration
	var upgradeCodex bool
	var autoUpdate bool
	var autoUpdateRepo string
	var autoUpdatePrerelease bool
	var autoService bool
	var machineRegistry bool
	var managedServiceChild bool
	var interval time.Duration
	var once bool
	cmd := &cobra.Command{
		Use:     "run",
		Aliases: []string{"listen"},
		Short:   "Poll Teams chats and route session messages to Codex",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("interval") && interval <= 0 {
				return fmt.Errorf("--interval must be positive")
			}
			if !cmd.Flags().Changed("interval") {
				interval = 0 // the helper's own read tiers
			}
			if once {
				// F-CLI-10: one cycle in the foreground, as in v0.1.20: no
				// background service, machine registry or self-update.
				if managedServiceChild {
					return fmt.Errorf("--once cannot be used by the Teams service")
				}
				autoService, machineRegistry, autoUpdate = false, false, false
			}
			if autoService && !managedServiceChild {
				// F-SVC-5: a foreground run repairs and starts the background
				// service where one is supported; the service then runs the
				// helper, so this terminal is not needed.
				if err := ensureTeamsServiceForRun(cmd.Context(), teamsServiceSpecEnvironmentOverrides(teamsASRServiceEnvironmentOverrides(asrCommand, asrArgs))); err != nil {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Teams service auto-ensure warning: %v\n", err)
				} else if active, err := teamsServiceActiveForRun(cmd.Context()); err == nil && active {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "The Teams helper runs as the background service; check it with `cxp teams service status`.")
					return nil
				}
			}
			if upgradeCodex {
				if err := runTeamsUpgradeCodexOnce(cmd, root, codexPath); err != nil {
					return err
				}
			}
			return runTeamsNewCore(cmd, root, teamsNewCoreOptions{
				codexPath:            codexPath,
				asrCommand:           firstNonEmptyString(asrCommand, os.Getenv(envTeamsASRCommand)),
				asrArgs:              asrArgs,
				registry:             machineRegistry,
				autoUpdate:           autoUpdate,
				autoUpdateRepo:       autoUpdateRepo,
				autoUpdatePrerelease: autoUpdatePrerelease,
				codexArgs:            codexArgs,
				managedServiceChild:  managedServiceChild,
				interval:             interval,
				once:                 once,
			})
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "Polling interval (the control chat and active chats; idle chats are read less often)")
	cmd.Flags().BoolVar(&once, "once", false, "Poll once, finish the work that brings in, and exit")
	cmd.Flags().StringVar(&codexPath, "codex-path", "", "Override Codex CLI path")
	cmd.Flags().StringArrayVar(&codexArgs, "codex-arg", nil, "Extra Codex argument for the app-server (repeatable)")
	cmd.Flags().StringVar(&asrCommand, "asr-command", "", "Optional local Teams audio/video transcription command; stdout may be plain text or ASRTranscript JSON")
	cmd.Flags().StringArrayVar(&asrArgs, "asr-arg", nil, "Argument for --asr-command (repeatable); placeholders: {input}, {language}, {speed}, {threads}, {source_index}, {prompt_path}, {content_type}")
	_ = cmd.Flags().MarkHidden("asr-command")
	_ = cmd.Flags().MarkHidden("asr-arg")
	// Service units written by earlier releases pass --owner-stale-after; the
	// store's owner lease has a fixed staleness bound now.
	cmd.Flags().DurationVar(&ownerStaleAfter, "owner-stale-after", 0, "Ignored (kept for service units written by earlier releases)")
	_ = cmd.Flags().MarkHidden("owner-stale-after")
	cmd.Flags().BoolVar(&upgradeCodex, "upgrade-codex", false, "Upgrade Codex CLI once before starting Teams polling")
	cmd.Flags().BoolVar(&autoUpdate, "auto-update", true, "Check codex-helper GitHub releases periodically and apply eligible helper updates")
	cmd.Flags().StringVar(&autoUpdateRepo, "auto-update-repo", "", "Override GitHub repo for Teams helper auto-update checks")
	cmd.Flags().BoolVar(&autoUpdatePrerelease, "auto-update-prerelease", false, "Allow Teams helper auto-update checks to select eligible GitHub prereleases")
	cmd.Flags().BoolVar(&autoService, "auto-service", true, "Automatically repair and start the per-user background service when supported")
	cmd.Flags().BoolVar(&machineRegistry, "machine-registry", true, "Enable cross-machine Teams registry heartbeat and delegation worker")
	cmd.Flags().BoolVar(&managedServiceChild, "managed-service-child", false, "Internal marker for a Teams service-managed listener")
	_ = cmd.Flags().MarkHidden("managed-service-child")
	return cmd
}

// teamsServiceActiveForRun reports whether the background service is
// running the helper (tests replace it).
var teamsServiceActiveForRun = teamsServiceActive

func teamsASRArgsFromFlagsOrEnv(flagArgs []string, enabled bool) ([]string, error) {
	if len(flagArgs) > 0 {
		return append([]string(nil), flagArgs...), nil
	}
	if !enabled {
		return nil, nil
	}
	raw := strings.TrimSpace(os.Getenv(envTeamsASRArgsJSON))
	if raw == "" {
		return nil, nil
	}
	var args []string
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil, fmt.Errorf("invalid %s JSON array: %w", envTeamsASRArgsJSON, err)
	}
	return args, nil
}

// teamsASRServiceEnvironmentOverrides carries the ASR settings of a
// foreground `teams run` into the service it installs.
func teamsASRServiceEnvironmentOverrides(command string, args []string) map[string]string {
	env := make(map[string]string)
	if command = strings.TrimSpace(command); command != "" {
		env[envTeamsASRCommand] = command
	}
	if len(args) > 0 {
		if data, err := json.Marshal(args); err == nil {
			env[envTeamsASRArgsJSON] = string(data)
		}
	}
	for _, key := range teamsASRManagedEnvironmentKeys() {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			env[key] = value
		}
	}
	if len(env) == 0 {
		return nil
	}
	return env
}

// teamsASRManagedConfig is the managed llama.cpp ASR configured by the
// environment (offline overrides; the device defaults to the CPU).
func teamsASRManagedConfig() asr.ManagedConfig {
	return asr.ManagedConfig{
		MirrorBaseURLs: strings.FieldsFunc(os.Getenv(envTeamsASRMirrors), func(r rune) bool { return r == ',' || r == ' ' || r == '\n' }),
		Device:         firstNonEmptyString(strings.TrimSpace(os.Getenv(envTeamsASRLlamaDevice)), "cpu"),
		ServerPath:     strings.TrimSpace(os.Getenv(envTeamsASRLlamaBinary)),
		ModelPath:      strings.TrimSpace(os.Getenv(envTeamsASRLlamaModel)),
		MMProjPath:     strings.TrimSpace(os.Getenv(envTeamsASRLlamaMMProj)),
		FFmpegPath:     strings.TrimSpace(os.Getenv(envTeamsASRFFmpeg)),
	}
}

func teamsASRManagedEnvironmentKeys() []string {
	return []string{envTeamsASRLlamaBinary, envTeamsASRLlamaModel, envTeamsASRLlamaMMProj, envTeamsASRLlamaDevice, envTeamsASRFFmpeg, envTeamsASRMirrors}
}

func newTeamsStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show local Teams bridge state",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return printTeamsCoreStatus(cmd.OutOrStdout(), time.Now())
		},
	}
}

func newTeamsDoctorCmd(root *rootOptions) *cobra.Command {
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
				if err := runTeamsDoctorLiveCheck(cmd, root); err != nil {
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
			if err := printTeamsCoreStatus(out, time.Now()); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(out, "Next steps: run `cxp teams setup` for the safe setup checklist.")
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

var runTeamsDoctorLiveCheck = defaultRunTeamsDoctorLiveCheck

type teamsAppServerProbeOptions struct {
	CodexPath string
	WorkDir   string
	Timeout   time.Duration
	Runs      int
}

var runTeamsAppServerProbe = defaultRunTeamsAppServerProbe

// defaultRunTeamsDoctorLiveCheck signs in with the helper's token and
// reads the signed-in account and the control chat (no message is sent).
func defaultRunTeamsDoctorLiveCheck(cmd *cobra.Command, root *rootOptions) error {
	out := cmd.OutOrStdout()
	cfg, err := chteams.ResolveAuthConfig("")
	if err != nil {
		_, _ = fmt.Fprintf(out, "Graph auth: not configured (%v)\n", err)
		return err
	}
	readCfg, err := chteams.ResolveReadAuthConfig("")
	if err != nil {
		return fmt.Errorf("Teams read auth: %w", err)
	}
	lease, err := newTeamsGraphHTTPClientLease(cmd.Context(), root, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	defer func() { _ = lease.Close(context.Background()) }()
	ch, err := chteams.New(teamsNewCoreChannelConfig(lease.Client, cfg, readCfg))
	if err != nil {
		return err
	}
	_, name, err := ch.Owner(cmd.Context())
	if err != nil {
		_, _ = fmt.Fprintf(out, "Graph auth: failed (%v)\n", err)
		return err
	}
	lease.RetireSuspects(cmd.Context(), cmd.ErrOrStderr())
	_, _ = fmt.Fprintf(out, "Graph auth: ok as %s\n", name)
	c, ok, cerr := teamsStoredControlChat()
	chat := c.ChatID
	if cerr != nil {
		_, _ = fmt.Fprintf(out, "Graph chat read: skipped (%v)\n", cerr)
		return nil
	}
	if !ok || chat == "" {
		_, _ = fmt.Fprintln(out, "Graph chat read: skipped (no control chat yet)")
		return nil
	}
	if _, err := ch.ReadBack(cmd.Context(), chat, time.Now().Add(-time.Minute)); err != nil {
		_, _ = fmt.Fprintf(out, "Graph chat read: failed (%v)\n", err)
		return err
	}
	_, _ = fmt.Fprintln(out, "Graph chat read: ok")
	return nil
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
	_, _ = fmt.Fprintf(out, "Codex app-server: ok (%d cold probe(s), min %s, max %s, total %s)\n",
		len(probe.Runs), probe.Min.Round(time.Millisecond), probe.Max.Round(time.Millisecond), probe.Total.Round(time.Millisecond))
	return nil
}

func firstNonEmptyCLI(values ...string) string {
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
