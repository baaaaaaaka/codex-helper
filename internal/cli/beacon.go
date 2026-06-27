package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/migration"
	"github.com/spf13/cobra"
)

var beaconExecutable = helperpath.RawExecutable

func newBeaconCmd(root *rootOptions) *cobra.Command {
	var storePath string

	cmd := &cobra.Command{
		Use:   "beacon",
		Short: "Manage beacon execution profiles and worker leases",
	}
	cmd.PersistentFlags().StringVar(&storePath, "store", "", "Override beacon state path")
	cmd.AddCommand(
		newBeaconProfileCmd(root, &storePath),
		newBeaconStatusCmd(&storePath),
		newBeaconReleaseCmd(&storePath),
		newBeaconSwitchProfileCmd(root, &storePath),
		newBeaconAllocationCmd(&storePath),
		newBeaconMachineCmd(&storePath),
		newBeaconWorkerCmd(&storePath),
		newBeaconProviderCmd(),
	)
	return cmd
}

func newBeaconProfileCmd(root *rootOptions, storePath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "profile", Short: "Manage beacon profiles"}
	cmd.AddCommand(
		newBeaconProfileListCmd(root, storePath),
		newBeaconProfileCreateCmd(root, storePath),
		newBeaconProfileUpdateCmd(root, storePath),
		newBeaconProfileHistoryCmd(root, storePath),
		newBeaconProfileRollbackCmd(root, storePath),
		newBeaconProfileGCCmd(storePath),
		newBeaconProfileStatusCmd(root, storePath),
		newBeaconProfileDoctorCmd(root, storePath),
		newBeaconProfileConfirmCmd(root, storePath),
		newBeaconProfileDeleteCmd(storePath),
	)
	return cmd
}

func newBeaconProfileListCmd(root *rootOptions, storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List beacon profiles",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			st, err := loadBeaconState(*storePath)
			if err != nil {
				return err
			}
			proxyExists, err := beaconProxyResolver(root)
			if err != nil {
				return err
			}
			profiles := beacon.ListProfiles(st)
			if len(profiles) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No beacon profiles.")
				return nil
			}
			for _, p := range profiles {
				state := "ready"
				if p.Archived {
					state = "archived"
				} else if reasons := p.DraftReasons(proxyExists); len(reasons) > 0 {
					state = "draft: " + strings.Join(reasons, "; ")
				}
				revision := p.Revision
				if revision <= 0 {
					revision = 1
				}
				adapter := cliBeaconAdapterLabel(p)
				if adapter != "" {
					adapter = "\tadapter=" + adapter
				}
				shared := ""
				if strings.TrimSpace(p.SharedPath) != "" {
					shared = "\tshared_path=" + p.SharedPath
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\trev=%d\t%s%s%s\n", p.Name, p.Provider, revision, state, adapter, shared)
			}
			return nil
		},
	}
}

func newBeaconProfileCreateCmd(root *rootOptions, storePath *string) *cobra.Command {
	var provider string
	var proxyMode string
	var proxyProfile string
	var isolation string
	var sharedPath string
	var nodes int
	var gpuCount int
	var partition string
	var image string
	var duration int
	var queue string
	var lsfSitePolicy bool
	var lsfAdvanced bool
	var queryCommand string
	var submitCommand string
	var cancelCommand string
	var renewCommand string
	var adapterShell string

	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a beacon profile draft",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			proxyExists, err := beaconProxyResolver(root)
			if err != nil {
				return err
			}
			var created beacon.Profile
			err = store.Update(func(st *beacon.State) error {
				var err error
				if strings.TrimSpace(sharedPath) == "" && beacon.ManagedProviderRequiresSharedPath(beacon.Provider(provider)) && strings.TrimSpace(*storePath) != "" {
					sharedPath = filepath.Dir(*storePath)
				}
				created, err = beacon.CreateProfile(st, beacon.CreateProfileInput{
					Name:                         args[0],
					Provider:                     beacon.Provider(provider),
					ProxyMode:                    beacon.ProxyMode(proxyMode),
					ProxyProfile:                 proxyProfile,
					IsolationDefault:             beacon.Isolation(isolation),
					SharedPath:                   sharedPath,
					Slurm:                        beacon.SlurmProfile{Nodes: nodes, GPUCount: gpuCount, Partition: partition, Image: image, Duration: duration},
					LSF:                          beacon.LSFProfile{QueueName: queue, SitePolicyDerivesResources: lsfSitePolicy, AdvancedApproved: lsfAdvanced},
					Adapter:                      providerCommandConfigForProfileInput(beacon.Provider(provider), queryCommand, submitCommand, cancelCommand, renewCommand, adapterShell),
					ExistingProxyProfileResolver: proxyExists,
				})
				return err
			})
			if err != nil {
				return err
			}
			reasons := created.DraftReasons(proxyExists)
			if len(reasons) == 0 {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Created ready beacon profile %q.\n", created.Name)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Created draft beacon profile %q: %s\n", created.Name, strings.Join(reasons, "; "))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&provider, "provider", "slurm", "Provider: slurm, lsf, or local")
	cmd.Flags().StringVar(&proxyMode, "proxy", "none", "Proxy mode: none or ssh_profile")
	cmd.Flags().StringVar(&proxyProfile, "proxy-profile", "", "Existing SSH proxy profile to use when --proxy=ssh_profile")
	cmd.Flags().StringVar(&isolation, "isolation", string(beacon.IsolationShared), "Default isolation: shared or exclusive")
	cmd.Flags().StringVar(&sharedPath, "shared-path", "", "Directory visible to both the control machine and allocated workers")
	cmd.Flags().IntVar(&nodes, "nodes", 0, "Slurm node count")
	cmd.Flags().IntVar(&gpuCount, "gpu", 0, "Slurm GPU count")
	cmd.Flags().StringVar(&partition, "partition", "", "Slurm partition")
	cmd.Flags().StringVar(&image, "image", "", "Slurm container image")
	cmd.Flags().IntVar(&duration, "duration", 0, "Slurm duration")
	cmd.Flags().StringVar(&queue, "queue", "", "LSF queue name")
	cmd.Flags().BoolVar(&lsfSitePolicy, "lsf-site-policy", false, "Allow site policy to derive LSF resources")
	cmd.Flags().BoolVar(&lsfAdvanced, "lsf-advanced-approved", false, "Mark advanced LSF resources locally approved")
	cmd.Flags().StringVar(&queryCommand, "query-command", "", "Provider query adapter command stored on this profile")
	cmd.Flags().StringVar(&submitCommand, "submit-command", "", "Provider submit adapter command stored on this profile")
	cmd.Flags().StringVar(&cancelCommand, "cancel-command", "", "Provider cancel adapter command stored on this profile")
	cmd.Flags().StringVar(&renewCommand, "renew-command", "", "Provider renew adapter command stored on this profile")
	cmd.Flags().StringVar(&adapterShell, "adapter-shell", "", "Provider adapter shell mode: direct, login, interactive-login, user, or shell-command")
	return cmd
}

func newBeaconProfileUpdateCmd(root *rootOptions, storePath *string) *cobra.Command {
	var provider string
	var proxyMode string
	var proxyProfile string
	var isolation string
	var sharedPath string
	var nodes int
	var gpuCount int
	var partition string
	var image string
	var duration int
	var queue string
	var lsfSitePolicy bool
	var lsfAdvanced bool
	var queryCommand string
	var submitCommand string
	var cancelCommand string
	var renewCommand string
	var adapterShell string

	cmd := &cobra.Command{
		Use:   "update <name>",
		Short: "Update a beacon profile by creating a new revision",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			proxyExists, err := beaconProxyResolver(root)
			if err != nil {
				return err
			}
			var updated beacon.Profile
			err = store.Update(func(st *beacon.State) error {
				var err error
				if strings.TrimSpace(sharedPath) == "" && beacon.ManagedProviderRequiresSharedPath(beacon.Provider(provider)) && strings.TrimSpace(*storePath) != "" {
					sharedPath = filepath.Dir(*storePath)
				}
				updated, err = beacon.UpdateProfileConfig(st, beacon.UpdateProfileInput{
					Name:                         args[0],
					Provider:                     beacon.Provider(provider),
					ProxyMode:                    beacon.ProxyMode(proxyMode),
					ProxyProfile:                 proxyProfile,
					IsolationDefault:             beacon.Isolation(isolation),
					SharedPath:                   sharedPath,
					Slurm:                        beacon.SlurmProfile{Nodes: nodes, GPUCount: gpuCount, Partition: partition, Image: image, Duration: duration},
					LSF:                          beacon.LSFProfile{QueueName: queue, SitePolicyDerivesResources: lsfSitePolicy, AdvancedApproved: lsfAdvanced},
					Adapter:                      providerCommandConfigForProfileInput(beacon.Provider(provider), queryCommand, submitCommand, cancelCommand, renewCommand, adapterShell),
					ExistingProxyProfileResolver: proxyExists,
				})
				return err
			})
			if err != nil {
				return err
			}
			reasons := updated.DraftReasons(proxyExists)
			if len(reasons) == 0 {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Updated ready beacon profile %q revision %d.\n", updated.Name, updated.Revision)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Updated draft beacon profile %q revision %d: %s\n", updated.Name, updated.Revision, strings.Join(reasons, "; "))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&provider, "provider", "", "Provider: slurm, lsf, or local")
	cmd.Flags().StringVar(&proxyMode, "proxy", "", "Proxy mode: none or ssh_profile")
	cmd.Flags().StringVar(&proxyProfile, "proxy-profile", "", "Existing SSH proxy profile to use when --proxy=ssh_profile")
	cmd.Flags().StringVar(&isolation, "isolation", "", "Default isolation: shared or exclusive")
	cmd.Flags().StringVar(&sharedPath, "shared-path", "", "Directory visible to both the control machine and allocated workers")
	cmd.Flags().IntVar(&nodes, "nodes", 0, "Slurm node count")
	cmd.Flags().IntVar(&gpuCount, "gpu", 0, "Slurm GPU count")
	cmd.Flags().StringVar(&partition, "partition", "", "Slurm partition")
	cmd.Flags().StringVar(&image, "image", "", "Slurm container image")
	cmd.Flags().IntVar(&duration, "duration", 0, "Slurm duration")
	cmd.Flags().StringVar(&queue, "queue", "", "LSF queue name")
	cmd.Flags().BoolVar(&lsfSitePolicy, "lsf-site-policy", false, "Allow site policy to derive LSF resources")
	cmd.Flags().BoolVar(&lsfAdvanced, "lsf-advanced-approved", false, "Mark advanced LSF resources locally approved")
	cmd.Flags().StringVar(&queryCommand, "query-command", "", "Provider query adapter command stored on this profile")
	cmd.Flags().StringVar(&submitCommand, "submit-command", "", "Provider submit adapter command stored on this profile")
	cmd.Flags().StringVar(&cancelCommand, "cancel-command", "", "Provider cancel adapter command stored on this profile")
	cmd.Flags().StringVar(&renewCommand, "renew-command", "", "Provider renew adapter command stored on this profile")
	cmd.Flags().StringVar(&adapterShell, "adapter-shell", "", "Provider adapter shell mode: direct, login, interactive-login, user, or shell-command")
	return cmd
}

func providerCommandConfigForProfileInput(provider beacon.Provider, query, submit, cancel, renew, shellMode string) beacon.ProviderCommandConfig {
	if provider == "" {
		config := beacon.ProviderCommandConfig{
			SlurmQueryCommand:  strings.TrimSpace(query),
			SlurmSubmitCommand: strings.TrimSpace(submit),
			SlurmCancelCommand: strings.TrimSpace(cancel),
			SlurmRenewCommand:  strings.TrimSpace(renew),
			LSFQueryCommand:    strings.TrimSpace(query),
			LSFSubmitCommand:   strings.TrimSpace(submit),
			LSFCancelCommand:   strings.TrimSpace(cancel),
			LSFRenewCommand:    strings.TrimSpace(renew),
			ShellMode:          strings.TrimSpace(shellMode),
		}
		return config
	}
	config := beacon.ProviderCommandConfigForProvider(provider, query, submit, cancel, renew)
	config.ShellMode = strings.TrimSpace(shellMode)
	return config
}

func newBeaconProfileHistoryCmd(root *rootOptions, storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "history <name>",
		Short: "List revisions for a beacon profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			st, err := loadBeaconState(*storePath)
			if err != nil {
				return err
			}
			proxyExists, err := beaconProxyResolver(root)
			if err != nil {
				return err
			}
			revisions := beacon.ListProfileRevisions(st, args[0])
			if len(revisions) == 0 {
				return fmt.Errorf("beacon profile %q not found", args[0])
			}
			currentRevision := 0
			if current, ok := st.Profiles[strings.TrimSpace(args[0])]; ok {
				currentRevision = cliBeaconProfileRevision(current.Revision)
			}
			for _, p := range revisions {
				state := "draft"
				if p.Archived {
					state = "archived"
				} else if p.Ready(proxyExists) {
					state = "ready"
				}
				kind := "history"
				if cliBeaconProfileRevision(p.Revision) == currentRevision {
					kind = "current"
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\trev=%d\t%s\tprovider=%s\tisolation=%s\tadapter=%s\t%s\n", p.Name, cliBeaconProfileRevision(p.Revision), kind, p.Provider, p.IsolationDefault, cliBeaconAdapterLabel(p), state)
			}
			return nil
		},
	}
}

func newBeaconProfileRollbackCmd(root *rootOptions, storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "rollback <name> <revision>",
		Short: "Publish an old beacon profile revision as a new revision",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			revision, err := strconv.Atoi(args[1])
			if err != nil || revision <= 0 {
				return fmt.Errorf("revision must be a positive integer")
			}
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			proxyExists, err := beaconProxyResolver(root)
			if err != nil {
				return err
			}
			var rolledBack beacon.Profile
			if err := store.Update(func(st *beacon.State) error {
				var err error
				rolledBack, err = beacon.RollbackProfileRevision(st, args[0], revision, time.Time{})
				return err
			}); err != nil {
				return err
			}
			reasons := rolledBack.DraftReasons(proxyExists)
			if len(reasons) == 0 {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Rolled back ready beacon profile %q to revision %d.\n", rolledBack.Name, rolledBack.Revision)
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Rolled back draft beacon profile %q to revision %d: %s\n", rolledBack.Name, rolledBack.Revision, strings.Join(reasons, "; "))
			}
			return nil
		},
	}
}

func newBeaconProfileGCCmd(storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:     "gc <name>",
		Aliases: []string{"prune-history"},
		Short:   "Prune unreferenced historical beacon profile revisions",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			removed := 0
			if err := store.Update(func(st *beacon.State) error {
				var err error
				removed, err = beacon.PruneProfileHistory(st, args[0])
				return err
			}); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Pruned %d unreferenced revisions for beacon profile %q.\n", removed, args[0])
			return nil
		},
	}
}

func newBeaconProfileStatusCmd(root *rootOptions, storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status <name>",
		Short: "Show beacon profile status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			st, err := loadBeaconState(*storePath)
			if err != nil {
				return err
			}
			p, ok := st.Profiles[strings.TrimSpace(args[0])]
			if !ok {
				return fmt.Errorf("beacon profile %q not found", args[0])
			}
			proxyExists, err := beaconProxyResolver(root)
			if err != nil {
				return err
			}
			reasons := p.DraftReasons(proxyExists)
			status := "ready"
			if len(reasons) > 0 {
				status = "draft: " + strings.Join(reasons, "; ")
			}
			revision := p.Revision
			if revision <= 0 {
				revision = 1
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "profile=%s revision=%d provider=%s proxy=%s isolation=%s shared_path=%s adapter=%s status=%s\n", p.Name, revision, p.Provider, p.ProxyMode, p.IsolationDefault, firstNonEmptyString(p.SharedPath, "<none>"), cliBeaconAdapterLabel(p), status)
			return nil
		},
	}
}

func cliBeaconAdapterLabel(p beacon.Profile) string {
	ops := beacon.ConfiguredProviderCommandOperations(p.Adapter, p.Provider)
	shell, defaulted := beacon.ProviderCommandShellModeForProfileWithBase(p, beacon.ProviderCommandConfigFromEnv(os.Getenv))
	shell = cliBeaconShellLabel(shell, defaulted)
	if len(ops) == 0 {
		if shell != "" {
			return "env,shell=" + shell
		}
		return "env"
	}
	label := "profile:" + strings.Join(ops, ",")
	if shell != "" {
		label += ",shell=" + shell
	}
	return label
}

func cliBeaconShellLabel(shell string, defaulted bool) string {
	shell = strings.TrimSpace(shell)
	if shell == "" || shell == beacon.ProviderCommandShellDirect && defaulted {
		return ""
	}
	if defaulted {
		return shell + "(default)"
	}
	return shell
}

func cliBeaconProfileRevision(revision int) int {
	if revision <= 0 {
		return 1
	}
	return revision
}

func newBeaconProfileDoctorCmd(root *rootOptions, storePath *string) *cobra.Command {
	var smoke bool
	cmd := &cobra.Command{
		Use:   "doctor <name>",
		Short: "Validate beacon profile scheduler adapter readiness",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			proxyExists, err := beaconProxyResolver(root)
			if err != nil {
				return err
			}
			var p beacon.Profile
			var report beacon.ProfileDoctorReport
			if err := store.Update(func(st *beacon.State) error {
				var err error
				p, report, err = beacon.DoctorProfileWithInput(st, args[0], beacon.DoctorProfileInput{
					ProxyExists:         proxyExists,
					EnvProviderCommands: beacon.ProviderCommandConfigFromEnv(nil),
				})
				return err
			}); err != nil {
				return err
			}
			if smoke {
				smokeOps := beacon.RunProfileDoctorSmoke(cmd.Context(), p, beacon.ProfileDoctorSmokeInput{
					Adapter: beacon.NewCommandProviderAdapterFromEnv(nil),
				})
				if err := store.Update(func(st *beacon.State) error {
					var err error
					p, err = beacon.ApplyProfileDoctorSmokeReport(st, p.Name, p.Revision, smokeOps, time.Now())
					if err == nil {
						report = p.DoctorReport
					}
					return err
				}); err != nil {
					return err
				}
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), formatBeaconProfileDoctorResultCLI(p, report, proxyExists))
			return nil
		},
	}
	cmd.Flags().BoolVar(&smoke, "smoke", false, "Run a submit/query/cancel provider smoke test after static doctor checks")
	return cmd
}

func newBeaconProfileConfirmCmd(root *rootOptions, storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "confirm <name>",
		Short: "Confirm a beacon profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			proxyExists, err := beaconProxyResolver(root)
			if err != nil {
				return err
			}
			var p beacon.Profile
			if err := store.Update(func(st *beacon.State) error {
				var err error
				p, err = beacon.ConfirmProfile(st, args[0], time.Time{}, proxyExists)
				return err
			}); err != nil {
				return err
			}
			reasons := p.DraftReasons(proxyExists)
			if len(reasons) > 0 {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Confirmed beacon profile %q, still draft: %s\n", p.Name, strings.Join(reasons, "; "))
			} else {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Confirmed ready beacon profile %q.\n", p.Name)
			}
			return nil
		},
	}
}

func newBeaconProfileDeleteCmd(storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Archive a beacon profile without breaking pinned references",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			if err := store.Update(func(st *beacon.State) error { return beacon.DeleteProfile(st, args[0]) }); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Archived beacon profile %q. Existing pinned turns can drain, but new turns cannot select it.\n", args[0])
			return nil
		},
	}
}

func newBeaconStatusCmd(storePath *string) *cobra.Command {
	var session string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show beacon status for a conversation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			st, err := loadBeaconState(*storePath)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), beacon.ConversationStatusNotice(st, session).Render())
			return nil
		},
	}
	cmd.Flags().StringVar(&session, "session", "", "Conversation/session id")
	return cmd
}

func newBeaconReleaseCmd(storePath *string) *cobra.Command {
	var force bool
	var confirm string
	cmd := &cobra.Command{
		Use:   "release <profile|allocation|provider-job|machine>",
		Short: "Release beacon resources without requiring internal object knowledge",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			out, err := releaseBeaconResourceFromCLI(cmd.Context(), *storePath, args[0], force, confirm)
			if out != "" {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), out)
			}
			return err
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Cancel even if a worker may have started work")
	cmd.Flags().StringVar(&confirm, "confirm", "", "Confirmation token from the release preview")
	return cmd
}

func newBeaconSwitchProfileCmd(root *rootOptions, storePath *string) *cobra.Command {
	var session string
	var fork bool
	var queuedOrRunning bool
	var afterCurrentTurn bool
	var signature string
	var signatureCompatible bool
	cmd := &cobra.Command{
		Use:   "switch-profile <name> --session <session-id>",
		Short: "Switch a conversation to another beacon profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(session) == "" {
				return fmt.Errorf("--session is required for switch-profile")
			}
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			proxyExists, err := beaconProxyResolver(root)
			if err != nil {
				return err
			}
			var res beacon.SwitchResult
			if err := store.Update(func(st *beacon.State) error {
				var err error
				res, err = beacon.SwitchProfile(st, beacon.SwitchInput{
					ConversationID:      session,
					ProfileName:         args[0],
					Signature:           signature,
					Fork:                fork,
					HasQueuedOrRunning:  queuedOrRunning || afterCurrentTurn,
					SignatureCompatible: signatureCompatible,
				}, proxyExists)
				return err
			}); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), res.Message)
			return nil
		},
	}
	cmd.Flags().StringVar(&session, "session", "", "Conversation/session id")
	cmd.Flags().BoolVar(&fork, "fork", false, "Fork when execution signatures are incompatible")
	cmd.Flags().BoolVar(&queuedOrRunning, "queued-or-running", false, "Schedule switch as pending because work is queued or running")
	cmd.Flags().BoolVar(&afterCurrentTurn, "after-current-turn", false, "Defer switch until the current Codex turn finishes")
	cmd.Flags().StringVar(&signature, "signature", "", "Execution signature snapshot")
	cmd.Flags().BoolVar(&signatureCompatible, "signature-compatible", true, "Whether the target signature is compatible")
	return cmd
}

func newBeaconMachineCmd(storePath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "machine", Short: "Manage beacon machines and leases"}
	cmd.AddCommand(newBeaconMachineListCmd(storePath), newBeaconMachineStatusCmd(storePath), newBeaconMachineReleaseCmd(storePath), newBeaconMachineKillCmd(storePath))
	return cmd
}

func newBeaconWorkerCmd(storePath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "worker", Short: "Run a beacon worker inside an allocated machine"}
	cmd.AddCommand(newBeaconWorkerRunOnceCmd(storePath), newBeaconWorkerServeCmd(storePath))
	return cmd
}

func newBeaconProviderCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "provider",
		Short: "Print beacon scheduler provider adapter templates",
		Long: strings.TrimSpace(`Print scheduler adapter templates for managed beacon allocations.

Custom Slurm/LSF submit adapters must preserve the --shared-store argument and pass
it to the worker with "cxp beacon --store <path> worker ..."; otherwise the worker
can look in its default store and fail with "allocation request ... not found".
Keep exactly one exec in the submitted worker command. If a site wrapper already
prepends exec, remove the extra exec from the adapter.`),
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "template <slurm|lsf>",
		Short: "Print a scheduler adapter template",
		Long: strings.TrimSpace(`Print an editable scheduler adapter template.

The generated template parses --shared-store, uses one exec in the submitted
worker command, and honors CXP_BEACON_CXP_BIN plus CXP_BEACON_CODEX_BIN so a
scheduler/container environment can point at the correct cxp and Codex/wrapper
paths.`),
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch strings.ToLower(strings.TrimSpace(args[0])) {
			case "slurm":
				_, _ = cmd.OutOrStdout().Write([]byte(beaconSlurmAdapterTemplate))
			case "lsf":
				_, _ = cmd.OutOrStdout().Write([]byte(beaconLSFAdapterTemplate))
			default:
				return fmt.Errorf("unknown beacon provider %q; expected slurm or lsf", args[0])
			}
			return nil
		},
	})
	return cmd
}

const beaconSlurmAdapterTemplate = `#!/bin/sh
set -eu
operation=
request_id=
name=
partition=
image=
nodes=1
gpu=0
duration=
provider_job_id=
shared_store=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --operation) operation="$2"; shift 2 ;;
    --request-id) request_id="$2"; shift 2 ;;
    --name) name="$2"; shift 2 ;;
    --provider-job-id) provider_job_id="$2"; shift 2 ;;
    --shared-store) shared_store="$2"; shift 2 ;;
    --partition) partition="$2"; shift 2 ;;
    --image) image="$2"; shift 2 ;;
    --nodes) nodes="$2"; shift 2 ;;
    --gpu) gpu="$2"; shift 2 ;;
    --duration) duration="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [ -z "$operation" ] || [ -z "$request_id" ] || [ -z "$name" ]; then
  echo "query_error=true reason=missing_required_beacon_adapter_args"
  exit 0
fi
find_slurm_line() {
  if [ -n "$provider_job_id" ]; then
    squeue --noheader --jobs "$provider_job_id" --format '%A|%t|%R' 2>/dev/null | head -n 1 || true
  else
    squeue --noheader --name "$name" --format '%A|%t|%R' 2>/dev/null | head -n 1 || true
  fi
}
slurm_job_id() {
  line=$(find_slurm_line)
  if [ -n "$line" ]; then
    printf '%s' "$line" | awk -F'|' '{print $1}'
  fi
}
query_slurm() {
  line=$(find_slurm_line)
  if [ -z "$line" ]; then
    echo "durable_negative=true"
    return 0
  fi
  job_id=$(printf '%s' "$line" | awk -F'|' '{print $1}')
  raw_state=$(printf '%s' "$line" | awk -F'|' '{print $2}')
  reason=$(printf '%s' "$line" | awk -F'|' '{print $3}')
  echo "provider_job_id=$job_id raw_state=$raw_state reason=$reason"
}
case "$operation" in
  query)
    query_slurm
    ;;
  submit)
    cxp_bin=${CXP_BEACON_CXP_BIN:-cxp}
    codex_bin=${CXP_BEACON_CODEX_BIN:-}
    worker_idle=${CXP_BEACON_WORKER_IDLE_TIMEOUT:-30m}
    codex_arg=
    if [ -n "$codex_bin" ]; then
      codex_arg=" --codex-path \"$codex_bin\""
    fi
    # Preserve --shared-store so the worker uses the same beacon state as Teams.
    # Keep exactly one exec in this submitted command.
    if [ -n "$shared_store" ]; then
      wrap="exec \"$cxp_bin\" beacon --store \"$shared_store\" worker serve --allocation \"$request_id\" --idle-timeout \"$worker_idle\"$codex_arg"
    else
      wrap="exec \"$cxp_bin\" beacon worker serve --allocation \"$request_id\" --idle-timeout \"$worker_idle\"$codex_arg"
    fi
    args="--parsable --job-name=$name --nodes=$nodes"
    [ -n "$partition" ] && args="$args --partition=$partition"
    [ -n "$duration" ] && args="$args --time=$duration"
    [ "$gpu" != "0" ] && args="$args --gres=gpu:$gpu"
    job_id=$(sbatch $args --wrap "$wrap" | awk -F';' '{print $1}')
    echo "provider_job_id=$job_id raw_state=PD reason=submitted"
    ;;
  cancel)
    job_id=$(slurm_job_id)
    if [ -z "$job_id" ]; then
      echo "durable_negative=true reason=no_matching_job_to_cancel"
      exit 0
    fi
    scancel "$job_id"
    echo "provider_job_id=$job_id raw_state=CA reason=cancel_requested"
    ;;
  renew)
    job_id=$(slurm_job_id)
    if [ -z "$job_id" ]; then
      echo "durable_negative=true reason=no_matching_job_to_renew"
      exit 0
    fi
    # Site policy decides how to extend walltime, for example:
    # scontrol update JobId="$job_id" TimeLimit="$duration"
    echo "provider_job_id=$job_id reason=renew_requires_site_policy"
    exit 1
    ;;
  *)
    echo "query_error=true reason=unknown_operation_$operation"
    ;;
esac
`

const beaconLSFAdapterTemplate = `#!/bin/sh
set -eu
operation=
request_id=
name=
queue=
provider_job_id=
shared_store=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --operation) operation="$2"; shift 2 ;;
    --request-id) request_id="$2"; shift 2 ;;
    --name) name="$2"; shift 2 ;;
    --provider-job-id) provider_job_id="$2"; shift 2 ;;
    --shared-store) shared_store="$2"; shift 2 ;;
    --queue) queue="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [ -z "$operation" ] || [ -z "$request_id" ] || [ -z "$name" ]; then
  echo "query_error=true reason=missing_required_beacon_adapter_args"
  exit 0
fi
find_lsf_line() {
  if [ -n "$provider_job_id" ]; then
    bjobs -noheader "$provider_job_id" -o 'jobid stat pend_reason' 2>/dev/null | head -n 1 || true
  else
    bjobs -noheader -J "$name" -o 'jobid stat pend_reason' 2>/dev/null | head -n 1 || true
  fi
}
lsf_job_id() {
  line=$(find_lsf_line)
  if [ -n "$line" ]; then
    printf '%s' "$line" | awk '{print $1}'
  fi
}
query_lsf() {
  line=$(find_lsf_line)
  if [ -z "$line" ]; then
    echo "durable_negative=true"
    return 0
  fi
  job_id=$(printf '%s' "$line" | awk '{print $1}')
  raw_state=$(printf '%s' "$line" | awk '{print $2}')
  reason=$(printf '%s' "$line" | cut -d' ' -f3-)
  echo "provider_job_id=$job_id raw_state=$raw_state reason=$reason"
}
case "$operation" in
  query)
    query_lsf
    ;;
  submit)
    cxp_bin=${CXP_BEACON_CXP_BIN:-cxp}
    codex_bin=${CXP_BEACON_CODEX_BIN:-}
    worker_idle=${CXP_BEACON_WORKER_IDLE_TIMEOUT:-30m}
    codex_arg=
    if [ -n "$codex_bin" ]; then
      codex_arg=" --codex-path \"$codex_bin\""
    fi
    # Preserve --shared-store so the worker uses the same beacon state as Teams.
    # Keep exactly one exec in this submitted command.
    if [ -n "$shared_store" ]; then
      command="exec \"$cxp_bin\" beacon --store \"$shared_store\" worker serve --allocation \"$request_id\" --idle-timeout \"$worker_idle\"$codex_arg"
    else
      command="exec \"$cxp_bin\" beacon worker serve --allocation \"$request_id\" --idle-timeout \"$worker_idle\"$codex_arg"
    fi
    if [ -n "$queue" ]; then
      out=$(printf '%s\n' "$command" | bsub -J "$name" -q "$queue")
    else
      out=$(printf '%s\n' "$command" | bsub -J "$name")
    fi
    job_id=$(printf '%s\n' "$out" | sed -n 's/.*<\([0-9][0-9]*\)>.*/\1/p' | head -n 1)
    echo "provider_job_id=$job_id raw_state=PEND reason=submitted"
    ;;
  cancel)
    job_id=$(lsf_job_id)
    if [ -z "$job_id" ]; then
      echo "durable_negative=true reason=no_matching_job_to_cancel"
      exit 0
    fi
    bkill "$job_id"
    echo "provider_job_id=$job_id raw_state=EXIT reason=cancel_requested"
    ;;
  renew)
    job_id=$(lsf_job_id)
    if [ -z "$job_id" ]; then
      echo "durable_negative=true reason=no_matching_job_to_renew"
      exit 0
    fi
    # Site policy decides how to extend walltime or queues.
    echo "provider_job_id=$job_id reason=renew_requires_site_policy"
    exit 1
    ;;
  *)
    echo "query_error=true reason=unknown_operation_$operation"
    ;;
esac
`

func newBeaconWorkerRunOnceCmd(storePath *string) *cobra.Command {
	var machineID string
	var allocationID string
	var leaseID string
	var providerJobID string
	var host string
	var workerID string
	var codexPath string
	var waitDuration time.Duration
	var legacySandbox bool
	cmd := &cobra.Command{
		Use:   "run-once (--machine <machine-id> | --allocation <request-id>)",
		Short: "Claim one queued beacon job, run Codex, and publish a terminal result",
		Long: strings.TrimSpace(`Claim one queued beacon job, run Codex, and publish a terminal result.

When this runs inside Slurm/LSF, make sure the worker command uses the same beacon
state as the Teams owner, normally by launching through "cxp beacon --store
<shared-store> worker ...". If the scheduler/container PATH cannot find Codex,
pass --codex-path to the real Codex executable or to a small wrapper. The
worker starts the standard approval runtime inside the allocation, so approved
commands inherit only the devices and mounts already granted by the scheduler
or container. Teams service --codex-arg settings do not automatically apply to
remote beacon workers.`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(workerID) == "" {
				workerID = defaultBeaconWorkerID()
			}
			if strings.TrimSpace(host) == "" {
				host = defaultBeaconWorkerHost()
			}
			if strings.TrimSpace(providerJobID) == "" {
				providerJobID = defaultBeaconProviderJobID()
			}
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			if strings.TrimSpace(allocationID) != "" {
				var machine beacon.Machine
				if err := store.Update(func(st *beacon.State) error {
					var err error
					machine, err = beacon.RegisterWorkerMachineForAllocation(st, allocationID, beacon.WorkerRegistrationInput{
						MachineID:       machineID,
						LeaseID:         leaseID,
						ProviderJobID:   providerJobID,
						WorkerID:        workerID,
						Host:            host,
						State:           beacon.LeaseAccepting,
						Doctor:          runBeaconWorkerDoctor(codexPath, *storePath),
						Bootstrap:       beaconWorkerBootstrapDiagnostics(codexPath, *storePath),
						MembershipProof: defaultBeaconMembershipProof(providerJobID),
					}, time.Now())
					return err
				}); err != nil {
					return err
				}
				machineID = machine.ID
				leaseID = machine.LeaseID
				providerJobID = machine.ProviderJobID
				defer func() { _ = markBeaconWorkerMachineDrained(store, machineID) }()
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "registered machine=%s lease=%s provider_job=%s\n", machineID, leaseID, providerJobID)
				if machine.State != string(beacon.LeaseAccepting) {
					return fmt.Errorf("worker machine %s is not accepting after doctor: state=%s blockers=%s", machine.ID, machine.State, strings.Join(machine.DoctorBlockers, "; "))
				}
			}
			if strings.TrimSpace(machineID) == "" {
				return fmt.Errorf("--machine is required unless --allocation is provided")
			}
			var job beacon.JobAttempt
			var ok bool
			if err := waitAndClaimBeaconWorkerJob(cmd.Context(), store, machineID, workerID, waitDuration, &job, &ok); err != nil {
				return err
			}
			if !ok {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No queued beacon jobs.")
				return nil
			}
			return runClaimedBeaconWorkerJob(cmd.Context(), cmd.OutOrStdout(), store, machineID, workerID, codexPath, job)
		},
	}
	cmd.Flags().StringVar(&machineID, "machine", "", "Beacon machine id to claim jobs for")
	cmd.Flags().StringVar(&allocationID, "allocation", "", "Managed allocation request id to register this worker for")
	cmd.Flags().StringVar(&leaseID, "lease", "", "Beacon lease id to register with --allocation")
	cmd.Flags().StringVar(&providerJobID, "provider-job", "", "Scheduler provider job id for this worker (default: SLURM_JOB_ID or LSB_JOBID)")
	cmd.Flags().StringVar(&host, "host", "", "Worker hostname to register with --allocation")
	cmd.Flags().StringVar(&workerID, "worker", "", "Worker id to stamp terminal output with")
	cmd.Flags().StringVar(&codexPath, "codex-path", "", "Codex executable or wrapper path (default: codex)")
	cmd.Flags().DurationVar(&waitDuration, "wait", 0, "Wait for a queued job before exiting, for example 30m")
	cmd.Flags().BoolVar(&legacySandbox, migration.LegacyBeaconSandboxFlagName, false, "")
	_ = cmd.Flags().MarkHidden(migration.LegacyBeaconSandboxFlagName)
	return cmd
}

func newBeaconWorkerServeCmd(storePath *string) *cobra.Command {
	var allocationID string
	var machineID string
	var leaseID string
	var providerJobID string
	var host string
	var workerID string
	var codexPath string
	var idleTimeout time.Duration
	var maxJobs int
	var legacySandbox bool
	cmd := &cobra.Command{
		Use:   "serve --allocation <request-id>",
		Short: "Register a beacon worker and serve queued jobs until idle or stopped",
		Long: strings.TrimSpace(`Register a beacon worker and serve queued jobs until idle or stopped.

When this runs inside Slurm/LSF, make sure the worker command uses the same beacon
state as the Teams owner, normally by launching through "cxp beacon --store
<shared-store> worker ...". If the scheduler/container PATH cannot find Codex,
pass --codex-path to the real Codex executable or to a small wrapper. The
worker starts the standard approval runtime inside the allocation, so approved
commands inherit only the devices and mounts already granted by the scheduler
or container. Teams service --codex-arg settings do not automatically apply to
remote beacon workers.`),
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(allocationID) == "" {
				return fmt.Errorf("--allocation is required")
			}
			if strings.TrimSpace(workerID) == "" {
				workerID = defaultBeaconWorkerID()
			}
			if strings.TrimSpace(host) == "" {
				host = defaultBeaconWorkerHost()
			}
			if strings.TrimSpace(providerJobID) == "" {
				providerJobID = defaultBeaconProviderJobID()
			}
			if idleTimeout <= 0 {
				idleTimeout = 30 * time.Minute
			}
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			var machine beacon.Machine
			if err := store.Update(func(st *beacon.State) error {
				var err error
				machine, err = beacon.RegisterWorkerMachineForAllocation(st, allocationID, beacon.WorkerRegistrationInput{
					MachineID:       machineID,
					LeaseID:         leaseID,
					ProviderJobID:   providerJobID,
					WorkerID:        workerID,
					Host:            host,
					State:           beacon.LeaseAccepting,
					Doctor:          runBeaconWorkerDoctor(codexPath, *storePath),
					Bootstrap:       beaconWorkerBootstrapDiagnostics(codexPath, *storePath),
					MembershipProof: defaultBeaconMembershipProof(providerJobID),
				}, time.Now())
				return err
			}); err != nil {
				return err
			}
			machineID = machine.ID
			defer func() { _ = markBeaconWorkerMachineDrained(store, machineID) }()
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "registered machine=%s lease=%s provider_job=%s\n", machine.ID, machine.LeaseID, machine.ProviderJobID)
			if machine.State != string(beacon.LeaseAccepting) {
				return fmt.Errorf("worker machine %s is not accepting after doctor: state=%s blockers=%s", machine.ID, machine.State, strings.Join(machine.DoctorBlockers, "; "))
			}
			served := 0
			for {
				var job beacon.JobAttempt
				var ok bool
				if err := waitAndClaimBeaconWorkerJob(cmd.Context(), store, machineID, workerID, idleTimeout, &job, &ok); err != nil {
					return err
				}
				if !ok {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "No queued beacon jobs before idle timeout after serving %d job(s).\n", served)
					return nil
				}
				if err := runClaimedBeaconWorkerJob(cmd.Context(), cmd.OutOrStdout(), store, machineID, workerID, codexPath, job); err != nil {
					return err
				}
				served++
				if maxJobs > 0 && served >= maxJobs {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Served max jobs: %d\n", served)
					return nil
				}
			}
		},
	}
	cmd.Flags().StringVar(&allocationID, "allocation", "", "Managed allocation request id to register this worker for")
	cmd.Flags().StringVar(&machineID, "machine", "", "Beacon machine id to register")
	cmd.Flags().StringVar(&leaseID, "lease", "", "Beacon lease id to register")
	cmd.Flags().StringVar(&providerJobID, "provider-job", "", "Scheduler provider job id for this worker (default: SLURM_JOB_ID or LSB_JOBID)")
	cmd.Flags().StringVar(&host, "host", "", "Worker hostname")
	cmd.Flags().StringVar(&workerID, "worker", "", "Worker id to stamp terminal output with")
	cmd.Flags().StringVar(&codexPath, "codex-path", "", "Codex executable or wrapper path (default: codex)")
	cmd.Flags().DurationVar(&idleTimeout, "idle-timeout", 30*time.Minute, "Exit after this long without a queued job")
	cmd.Flags().IntVar(&maxJobs, "max-jobs", 0, "Maximum jobs to serve before exiting (0 means unlimited until idle)")
	cmd.Flags().BoolVar(&legacySandbox, migration.LegacyBeaconSandboxFlagName, false, "")
	_ = cmd.Flags().MarkHidden(migration.LegacyBeaconSandboxFlagName)
	return cmd
}

func markBeaconWorkerMachineDrained(store *beacon.Store, machineID string) error {
	if store == nil {
		return nil
	}
	machineID = strings.TrimSpace(machineID)
	if machineID == "" {
		return nil
	}
	return store.Update(func(st *beacon.State) error {
		machine, ok := st.Machines[machineID]
		if !ok {
			return nil
		}
		switch strings.ToLower(strings.TrimSpace(machine.State)) {
		case string(beacon.LeaseNeedsAttention), string(beacon.LeaseLost), string(beacon.LeaseExpired), string(beacon.LeaseIncompatible), string(beacon.LeaseAmbiguous), "kill_quarantine":
			return nil
		}
		machine.State = string(beacon.LeaseDrained)
		st.Machines[machineID] = machine
		return nil
	})
}

func waitAndClaimBeaconWorkerJob(ctx context.Context, store *beacon.Store, machineID string, workerID string, waitDuration time.Duration, job *beacon.JobAttempt, ok *bool) error {
	deadline := time.Time{}
	if waitDuration > 0 {
		deadline = time.Now().Add(waitDuration)
	}
	for {
		if err := store.Update(func(st *beacon.State) error {
			if _, err := beacon.RecordWorkerHeartbeat(st, machineID, workerID, time.Now()); err != nil {
				return err
			}
			var err error
			*job, *ok, err = beacon.ClaimNextJobForMachine(st, machineID, workerID, time.Now())
			return err
		}); err != nil {
			return err
		}
		if *ok || waitDuration <= 0 {
			return nil
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return fmt.Errorf("timed out waiting for queued beacon job on machine %q", machineID)
		}
		wait := time.Second
		if remaining := time.Until(deadline); !deadline.IsZero() && remaining < wait {
			wait = remaining
		}
		if wait <= 0 {
			wait = 10 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func runClaimedBeaconWorkerJob(ctx context.Context, out anyWriter, store *beacon.Store, machineID string, workerID string, codexPath string, job beacon.JobAttempt) error {
	streamWriter, streamErr := beacon.NewJobStreamWriter(store.Path(), job)
	if streamErr != nil {
		_, _ = fmt.Fprintf(out, "job=%s stream=disabled reason=%s\n", job.ID, streamErr)
	}
	if err := store.Update(func(st *beacon.State) error {
		_, err := beacon.MarkJobStarted(st, job.ID, time.Now())
		return err
	}); err != nil {
		if streamWriter != nil {
			_ = streamWriter.Close()
		}
		return err
	}
	stopHeartbeat := startBeaconWorkerHeartbeat(ctx, store, machineID, workerID, time.Minute)
	var handler codexrunner.EventHandler
	if streamWriter != nil {
		handler = func(event codexrunner.StreamEvent) {
			_ = streamWriter.Append(event)
		}
	}
	payload, runErr := runBeaconWorkerJob(ctx, job, codexPath, handler)
	stopHeartbeat()
	if streamWriter != nil {
		_ = streamWriter.Close()
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal worker terminal payload: %w", err)
	}
	var decision beacon.WorkerTerminalDecision
	if err := store.Update(func(st *beacon.State) error {
		var err error
		decision, err = beacon.AcceptWorkerTerminal(st, beacon.WorkerTerminalEnvelope{
			JobID:      job.ID,
			RequestID:  job.RequestID,
			TurnID:     job.TurnID,
			WorkerID:   workerID,
			LeaseID:    job.LeaseID,
			ClaimEpoch: job.ClaimEpoch,
			ProviderIdentity: beacon.ProviderIdentity{
				ProviderJobID: job.ProviderIdentity.ProviderJobID,
			},
			Payload: payloadBytes,
		}, time.Now())
		return err
	}); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "job=%s terminal=%s outbox_queued=%t\n", job.ID, decision.Integrity, decision.OutboxQueued)
	return runErr
}

type anyWriter interface {
	Write([]byte) (int, error)
}

func startBeaconWorkerHeartbeat(ctx context.Context, store *beacon.Store, machineID string, workerID string, interval time.Duration) func() {
	if interval <= 0 {
		interval = time.Minute
	}
	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			_ = store.Update(func(st *beacon.State) error {
				_, err := beacon.RecordWorkerHeartbeat(st, machineID, workerID, time.Now())
				return err
			})
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func runBeaconWorkerJob(ctx context.Context, job beacon.JobAttempt, codexPath string, handler codexrunner.EventHandler) (beacon.JobTerminalPayload, error) {
	command := strings.TrimSpace(codexPath)
	store, paths, storeErr := newRootStore(nil, "")
	if storeErr != nil {
		return beacon.JobTerminalPayload{}, storeErr
	}
	paths, storeErr = resolveEffectiveLaunchPaths(store.Path(), paths.CodexDir, job.Payload.WorkingDir)
	if storeErr != nil {
		return beacon.JobTerminalPayload{}, storeErr
	}
	command, storeErr = ensureCodexBrokerRuntime(ctx, command, nil, codexInstallOptions{}, codexPathAllowsAutomaticUpgrade(codexPath))
	if storeErr != nil {
		return beacon.JobTerminalPayload{Error: storeErr.Error()}, storeErr
	}
	if storeErr = prepareRuntimeMigration(store, paths, command, nil); storeErr != nil {
		return beacon.JobTerminalPayload{Error: storeErr.Error()}, storeErr
	}
	configureIdentity := func(process *exec.Cmd) error {
		updated, applyErr := applyExecIdentity(process, process.Env, paths.ExecIdentity)
		if applyErr != nil {
			return applyErr
		}
		process.Env = updated
		return nil
	}
	runner := &codexrunner.AppServerRunner{
		Starter: configureAppServerStarter{base: codexrunner.PolicyAppServerStarter{
			ReadyHook: runtimeMigrationReadyHook(store, paths, command, nil),
		}, configure: configureIdentity},
		Command:            command,
		AppServerArgs:      []string{"--analytics-default-enabled"},
		ExtraEnv:           codexHomeEnv(paths.CodexDir),
		WorkingDir:         job.Payload.WorkingDir,
		BackfillThreadName: true,
	}
	defer func() { _ = runner.Close() }()
	result, err := runner.StartTurn(ctx, codexrunner.StartTurnInput{
		ThreadID: strings.TrimSpace(job.Payload.CodexThreadID),
		TurnInput: codexrunner.TurnInput{
			Prompt:       job.Payload.Prompt,
			ImagePaths:   append([]string(nil), job.Payload.ImagePaths...),
			WorkingDir:   job.Payload.WorkingDir,
			EventHandler: handler,
		},
	})
	payload := beacon.JobTerminalPayload{
		Text:             strings.TrimSpace(result.FinalAgentMessage),
		CodexThreadID:    result.ThreadID,
		CodexThreadTitle: strings.TrimSpace(result.ThreadName),
		CodexTurnID:      result.TurnID,
	}
	if err != nil {
		payload.Error = err.Error()
		return payload, err
	}
	if payload.Text == "" {
		payload.Text = "(Codex finished without a final message.)"
	}
	return payload, nil
}

func defaultBeaconWorkerID() string {
	host, _ := os.Hostname()
	host = strings.TrimSpace(host)
	if host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}

func defaultBeaconWorkerHost() string {
	host, _ := os.Hostname()
	host = strings.TrimSpace(host)
	if host == "" {
		return "unknown-host"
	}
	return host
}

func defaultBeaconProviderJobID() string {
	for _, name := range []string{"SLURM_JOB_ID", "LSB_JOBID"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func defaultBeaconMembershipProof(providerJobID string) string {
	providerJobID = strings.TrimSpace(providerJobID)
	if providerJobID == "" {
		return ""
	}
	if strings.TrimSpace(os.Getenv("SLURM_JOB_ID")) == providerJobID {
		return "slurm:" + providerJobID
	}
	if strings.TrimSpace(os.Getenv("LSB_JOBID")) == providerJobID {
		return "lsf:" + providerJobID
	}
	return providerJobID
}

func beaconWorkerBootstrapDiagnostics(codexPath string, storePath string) beacon.BootstrapDiagnostics {
	sharedStore := strings.TrimSpace(storePath)
	if sharedStore == "" {
		if path, err := beacon.DefaultStorePath(); err == nil {
			sharedStore = path
		}
	}
	cxpPath := ""
	if exe, err := beaconExecutable(); err == nil {
		if resolved, resolveErr := helperpath.StableRunnablePathFromSources(exe, restartArgv0(), helperpath.Options{}); resolveErr == nil {
			cxpPath = resolved.Path
		}
	}
	codex := strings.TrimSpace(codexPath)
	if codex == "" {
		if path, err := exec.LookPath("codex"); err == nil {
			codex = path
		} else {
			codex = "codex"
		}
	}
	return beacon.BootstrapDiagnostics{
		NodeList:        firstNonEmptyString(os.Getenv("SLURM_JOB_NODELIST"), os.Getenv("LSB_HOSTS"), os.Getenv("HOSTNAME")),
		StdoutPath:      firstNonEmptyString(os.Getenv("CODEX_HELPER_BEACON_BOOTSTRAP_STDOUT"), os.Getenv("CXP_BEACON_BOOTSTRAP_STDOUT")),
		StderrPath:      firstNonEmptyString(os.Getenv("CODEX_HELPER_BEACON_BOOTSTRAP_STDERR"), os.Getenv("CXP_BEACON_BOOTSTRAP_STDERR")),
		SharedStorePath: sharedStore,
		CodexPath:       codex,
		CXPPath:         cxpPath,
		ProtocolVersion: "1",
	}
}

func runBeaconWorkerDoctor(codexPath string, storePath string) beacon.WorkerDoctor {
	doctor := beacon.WorkerDoctor{
		SharedRootMounted: true,
		AtomicCreateOK:    true,
		FreeBytesOK:       true,
		FreeInodesOK:      true,
		HomeOK:            true,
		TmpWritable:       true,
		ProxyOK:           true,
		AuthPathOK:        true,
		ImageDigestMatch:  true,
		ProtocolOK:        true,
		MembershipProofOK: true,
		// Site-specific checks can be tightened by wrapper scripts before starting the worker.
		ContainerRuntimeOK: true,
		ModulesOK:          true,
		BindMountsOK:       true,
		ProxyEnvInsideOK:   true,
	}
	command := strings.TrimSpace(codexPath)
	if command == "" {
		command = "codex"
	}
	if strings.ContainsRune(command, rune(os.PathSeparator)) {
		if st, err := os.Stat(command); err == nil && !st.IsDir() {
			doctor.CodexAvailable = true
		}
	} else if _, err := exec.LookPath(command); err == nil {
		doctor.CodexAvailable = true
	}
	if exe, err := beaconExecutable(); err == nil {
		if resolved, resolveErr := helperpath.StableRunnablePathFromSources(exe, restartArgv0(), helperpath.Options{}); resolveErr == nil {
			exe = resolved.Path
		} else {
			exe = ""
		}
		if exe != "" {
			if st, statErr := os.Stat(exe); statErr == nil && !st.IsDir() {
				doctor.CXPAvailable = true
			}
		}
	}
	if _, err := os.UserHomeDir(); err != nil {
		doctor.HomeOK = false
	}
	if err := probeBeaconWorkerSharedStore(storePath); err != nil {
		doctor.SharedRootMounted = false
		doctor.AtomicCreateOK = false
	}
	tmp, err := os.CreateTemp("", "cxp-beacon-doctor-*")
	if err != nil {
		doctor.TmpWritable = false
		return doctor
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write([]byte("ok")); err != nil {
		doctor.TmpWritable = false
	}
	if err := tmp.Close(); err != nil {
		doctor.TmpWritable = false
	}
	_ = os.Remove(tmpName)
	return doctor
}

func probeBeaconWorkerSharedStore(storePath string) error {
	storePath = strings.TrimSpace(storePath)
	if storePath == "" {
		var err error
		storePath, err = beacon.DefaultStorePath()
		if err != nil {
			return err
		}
	}
	dir := filepath.Dir(storePath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".cxp-beacon-worker-shared-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write([]byte("ok")); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	renamed := tmpName + ".renamed"
	if err := os.Rename(tmpName, renamed); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Remove(renamed)
}

func newBeaconAllocationCmd(storePath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "allocation", Short: "Manage beacon allocation requests"}
	cmd.AddCommand(newBeaconAllocationListCmd(storePath), newBeaconAllocationStatusCmd(storePath), newBeaconAllocationCancelCmd(storePath), newBeaconAllocationReconcileCmd(storePath), newBeaconAllocationReconcileAllCmd(storePath))
	return cmd
}

func newBeaconAllocationListCmd(storePath *string) *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List beacon allocation requests",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			st, err := loadBeaconState(*storePath)
			if err != nil {
				return err
			}
			lines := beacon.ActiveAllocationSummaryLines(st)
			if all {
				lines = beacon.AllocationSummaryLines(st)
				if len(lines) == 0 {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No beacon allocations.")
					return nil
				}
			} else if len(lines) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No active beacon allocations.")
				return nil
			}
			for _, line := range lines {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), line)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "Include terminal allocations that were already reported")
	return cmd
}

func newBeaconAllocationStatusCmd(storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status <allocation-or-provider-job>",
		Short: "Show beacon allocation request status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			st, err := loadBeaconState(*storePath)
			if err != nil {
				return err
			}
			req, ok := findBeaconAllocation(st, args[0])
			if !ok {
				return fmt.Errorf("beacon allocation %q not found", args[0])
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), beacon.AllocationStatusNotice(req).Render())
			return nil
		},
	}
}

func newBeaconAllocationCancelCmd(storePath *string) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "cancel <allocation-or-provider-job>",
		Short: "Cancel a beacon allocation through the configured provider adapter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			res, err := beacon.CancelAllocationOutsideLock(cmd.Context(), store, args[0], beacon.NewCommandProviderAdapterFromEnv(nil), "canceled by operator", force, time.Now())
			if res.Request.ID != "" {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), formatAllocationCancelResult(res))
			}
			return err
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Cancel even if a worker may have started work")
	return cmd
}

func newBeaconAllocationReconcileCmd(storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "reconcile <allocation>",
		Short: "Query or submit a beacon allocation through the configured provider adapter",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			var req beacon.AllocationRequest
			var action beacon.AllocationSubmitAction
			var reconcileErr error
			st, err := store.Load()
			if err != nil {
				return err
			}
			found, ok := findBeaconAllocation(st, args[0])
			if !ok {
				return fmt.Errorf("beacon allocation %q not found", args[0])
			}
			req, action, reconcileErr = beacon.ReconcileAllocationSubmitOutsideLock(cmd.Context(), store, found.ID, beacon.NewCommandProviderAdapterFromEnv(nil), time.Now())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Beacon allocation reconcile: %s.\n\n", action)
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), beacon.AllocationStatusNotice(req).Render())
			return reconcileErr
		},
	}
}

func newBeaconAllocationReconcileAllCmd(storePath *string) *cobra.Command {
	var staleAfter time.Duration
	var staleJobAfter time.Duration
	cmd := &cobra.Command{
		Use:   "reconcile-all",
		Short: "Reconcile every beacon allocation and drain stale worker machines",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			var lines []string
			var errorsOut []string
			st, err := store.Load()
			if err != nil {
				return err
			}
			allocations := sortedBeaconAllocations(st)
			adapter := beacon.NewCommandProviderAdapterFromEnv(nil)
			now := time.Now()
			for _, current := range allocations {
				req, action, reconcileErr := beacon.ReconcileAllocationSubmitOutsideLock(cmd.Context(), store, current.ID, adapter, now)
				if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" && strings.TrimSpace(req.RawProviderState) != "" {
					_ = store.Update(func(st *beacon.State) error {
						projection := beacon.ProjectRawProviderState(req.Provider, req.RawProviderState, req.ProviderReason, beaconAllocationHasStartedJob(*st, req.ID), beaconAllocationHasEverRun(req))
						var err error
						req, err = beacon.UpdateAllocationProjection(st, req.ID, projection, now)
						return err
					})
				}
				line := fmt.Sprintf("- allocation %s: action=%s, state=%s", req.ID, action, req.State)
				var facts []string
				if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" {
					facts = append(facts, "provider_job="+req.ProviderIdentity.ProviderJobID)
				}
				if strings.TrimSpace(req.RawProviderState) != "" {
					facts = append(facts, "provider_state="+req.RawProviderState)
				}
				if strings.TrimSpace(req.ProviderReason) != "" {
					facts = append(facts, "reason="+req.ProviderReason)
				}
				if len(facts) > 0 {
					line += " - " + strings.Join(facts, ", ")
				}
				lines = append(lines, line)
				if reconcileErr != nil {
					errorsOut = append(errorsOut, fmt.Sprintf("%s: %v", current.ID, reconcileErr))
				}
			}
			if err := store.Update(func(st *beacon.State) error {
				for _, machine := range beacon.DrainStaleWorkerMachines(st, staleAfter, now) {
					lines = append(lines, fmt.Sprintf("machine=%s action=drain_stale last_heartbeat=%s", machine.ID, machine.LastHeartbeat.Format(time.RFC3339)))
				}
				for _, job := range beacon.RecoverStaleJobAttempts(st, staleJobAfter, now) {
					lines = append(lines, fmt.Sprintf("job=%s action=recover_stale phase=%s", job.ID, job.Phase))
				}
				return nil
			}); err != nil {
				return err
			}
			if len(lines) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No beacon allocations.")
			} else {
				for _, line := range lines {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), line)
				}
			}
			if len(errorsOut) > 0 {
				return fmt.Errorf("reconcile errors: %s", strings.Join(errorsOut, "; "))
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&staleAfter, "stale-after", 2*time.Minute, "Drain accepting worker machines whose heartbeat is older than this")
	cmd.Flags().DurationVar(&staleJobAfter, "stale-job-after", 10*time.Minute, "Recover claimed jobs or mark started jobs ambiguous after this much silence")
	return cmd
}

func newBeaconMachineListCmd(storePath *string) *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List beacon machines",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			st, err := loadBeaconState(*storePath)
			if err != nil {
				return err
			}
			lines := beacon.ActiveMachineSummaryLines(st)
			if all {
				lines = beacon.MachineSummaryLines(st)
				if len(lines) == 0 {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No beacon machines.")
					return nil
				}
			} else if len(lines) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No active beacon machines.")
				return nil
			}
			for _, line := range lines {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), line)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "Include terminal machines that were already reported")
	return cmd
}

func newBeaconMachineStatusCmd(storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "status <machine-or-lease>",
		Short: "Show beacon machine status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			st, err := loadBeaconState(*storePath)
			if err != nil {
				return err
			}
			m, ok := findBeaconMachine(st, args[0])
			if !ok {
				return fmt.Errorf("beacon machine or lease %q not found", args[0])
			}
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), beacon.MachineStatusNotice(m).Render())
			return nil
		},
	}
}

func newBeaconMachineReleaseCmd(storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "release <machine-or-lease>",
		Short: "Drain or release a beacon machine",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			var res beacon.ReleaseResult
			if err := store.Update(func(st *beacon.State) error {
				key, m, ok := findBeaconMachineEntry(*st, args[0])
				if !ok {
					return fmt.Errorf("beacon machine or lease %q not found", args[0])
				}
				var err error
				res, err = beacon.DecideRelease(m, beacon.ReleaseInput{})
				if err != nil {
					return err
				}
				applyBeaconMachineRelease(st, key, m, res.Action)
				return nil
			}); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "action=%s machine=%s lease=%s chats=%s jobs=%s\n", res.Action, res.Preview.MachineID, res.Preview.LeaseID, strings.Join(res.Preview.Chats, ","), strings.Join(res.Preview.Jobs, ","))
			return nil
		},
	}
}

func newBeaconMachineKillCmd(storePath *string) *cobra.Command {
	var confirm string
	cmd := &cobra.Command{
		Use:   "kill <machine-or-lease-or-job>",
		Short: "Hard kill a beacon machine after confirmation",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			var res beacon.ReleaseResult
			if err := store.Update(func(st *beacon.State) error {
				key, m, ok := findBeaconMachineEntry(*st, args[0])
				if !ok {
					return fmt.Errorf("beacon machine or lease %q not found", args[0])
				}
				preview := beacon.PreviewRelease(m)
				var err error
				res, err = beacon.DecideRelease(m, beacon.ReleaseInput{HardKill: true, ExactID: args[0], JobID: args[0], ConfirmToken: preview.Confirmation, ProvidedToken: confirm})
				if err != nil {
					return err
				}
				applyBeaconMachineRelease(st, key, m, res.Action)
				return nil
			}); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "action=%s machine=%s lease=%s chats=%s jobs=%s confirm=%s\n", res.Action, res.Preview.MachineID, res.Preview.LeaseID, strings.Join(res.Preview.Chats, ","), strings.Join(res.Preview.Jobs, ","), res.Preview.Confirmation)
			return nil
		},
	}
	cmd.Flags().StringVar(&confirm, "confirm", "", "Confirmation token from beacon machine status")
	return cmd
}

func releaseBeaconResourceFromCLI(ctx context.Context, storePath string, ref string, force bool, confirm string) (string, error) {
	store, err := beacon.NewStore(storePath)
	if err != nil {
		return "", err
	}
	st, err := store.Load()
	if err != nil {
		return "", err
	}
	if _, ok := st.Profiles[ref]; ok {
		allocations := beaconAllocationsForProfileCLI(st, ref)
		if len(allocations) == 0 {
			return fmt.Sprintf("Beacon release: no active allocations are using profile %q.", ref), nil
		}
		preview := beacon.PreviewAllocationRelease(st, "profile", ref, allocations, force)
		if cliReleaseConfirmationRequired(preview, confirm) {
			return formatAllocationReleasePreviewCLI(preview), nil
		}
		var lines []string
		lines = append(lines, formatAllocationReleasePreviewCLI(preview))
		for _, req := range allocations {
			res, cancelErr := beacon.CancelAllocationOutsideLock(ctx, store, req.ID, beacon.NewCommandProviderAdapterFromEnv(nil), "released profile "+ref+" by operator", force, time.Now())
			lines = append(lines, formatAllocationCancelResult(res))
			if cancelErr != nil {
				lines = append(lines, "Error: "+cancelErr.Error())
			}
		}
		return strings.Join(lines, "\n"), nil
	}
	if req, ok := beacon.FindAllocationByRef(st, ref); ok {
		preview := beacon.PreviewAllocationRelease(st, "allocation", ref, []beacon.AllocationRequest{req}, force)
		if cliReleaseConfirmationRequired(preview, confirm) {
			return formatAllocationReleasePreviewCLI(preview), nil
		}
		res, err := beacon.CancelAllocationOutsideLock(ctx, store, req.ID, beacon.NewCommandProviderAdapterFromEnv(nil), "released by operator", force, time.Now())
		return formatAllocationReleasePreviewCLI(preview) + "\n" + formatAllocationCancelResult(res), err
	}
	var res beacon.ReleaseResult
	if err := store.Update(func(st *beacon.State) error {
		key, m, ok := findBeaconMachineEntry(*st, ref)
		if !ok {
			return fmt.Errorf("beacon resource %q not found", ref)
		}
		var err error
		res, err = beacon.DecideRelease(m, beacon.ReleaseInput{})
		if err != nil {
			return err
		}
		applyBeaconMachineRelease(st, key, m, res.Action)
		return nil
	}); err != nil {
		return "", err
	}
	return fmt.Sprintf("Beacon release: action=%s machine=%s lease=%s chats=%s jobs=%s", res.Action, res.Preview.MachineID, res.Preview.LeaseID, strings.Join(res.Preview.Chats, ","), strings.Join(res.Preview.Jobs, ",")), nil
}

func formatAllocationCancelResult(res beacon.AllocationCancelResult) string {
	req := res.Request
	var facts []string
	if strings.TrimSpace(req.ID) != "" {
		facts = append(facts, "allocation="+req.ID)
	}
	if strings.TrimSpace(req.Profile) != "" {
		facts = append(facts, "profile="+req.Profile)
	}
	if strings.TrimSpace(req.ProviderIdentity.ProviderJobID) != "" {
		facts = append(facts, "provider_job="+req.ProviderIdentity.ProviderJobID)
	}
	if strings.TrimSpace(req.RawProviderState) != "" {
		facts = append(facts, "provider_state="+req.RawProviderState)
	}
	if strings.TrimSpace(req.ProviderReason) != "" {
		facts = append(facts, "reason="+req.ProviderReason)
	}
	return "Beacon allocation release: action=" + string(res.Action) + " state=" + string(req.State) + " - " + strings.Join(facts, ", ")
}

func formatAllocationReleasePreviewCLI(preview beacon.AllocationReleasePreview) string {
	var lines []string
	lines = append(lines, fmt.Sprintf("Beacon release preview: scope=%s ref=%s allocations=%d force=%t affected_chats=%s queued_turns=%s running_turns=%s", preview.Scope, preview.Ref, len(preview.Allocations), preview.Force, strings.Join(preview.AffectedChats, ","), strings.Join(preview.QueuedTurns, ","), strings.Join(preview.RunningTurns, ",")))
	for _, item := range preview.Allocations {
		lines = append(lines, fmt.Sprintf("preview: allocation=%s action=%s profile=%s state=%s provider_job=%s machines=%s chats=%s", item.AllocationID, item.Action, item.Profile, item.State, item.ProviderJob, strings.Join(item.Machines, ","), strings.Join(item.Chats, ",")))
	}
	if preview.RequiresConfirmation {
		lines = append(lines, "confirm: "+preview.Confirmation)
	}
	return strings.Join(lines, "\n")
}

func cliReleaseConfirmationRequired(preview beacon.AllocationReleasePreview, provided string) bool {
	if !preview.RequiresConfirmation {
		return false
	}
	return strings.TrimSpace(provided) == "" || strings.TrimSpace(provided) != strings.TrimSpace(preview.Confirmation)
}

func formatBeaconProfileDoctorResultCLI(p beacon.Profile, report beacon.ProfileDoctorReport, proxyExists func(string) bool) string {
	status := "failed"
	if report.Passed {
		status = "passed"
	}
	lines := []string{
		fmt.Sprintf("Beacon profile doctor: profile=%s revision=%d status=%s ready=%t provider=%s shared_path=%s", p.Name, maxProfileRevisionForCLI(p.Revision), status, p.Ready(proxyExists), p.Provider, firstNonEmptyString(p.SharedPath, "<none>")),
	}
	for _, op := range report.Operations {
		var facts []string
		facts = append(facts, "operation="+op.Operation)
		facts = append(facts, "status="+firstNonEmptyString(op.Status, "unknown"))
		if strings.TrimSpace(op.Source) != "" {
			facts = append(facts, "source="+strings.ReplaceAll(op.Source, " ", "_"))
		}
		if strings.TrimSpace(op.ProfileFlag) != "" {
			facts = append(facts, "profile_flag="+op.ProfileFlag)
		}
		if strings.TrimSpace(op.EnvName) != "" {
			facts = append(facts, "env="+op.EnvName)
		}
		if strings.TrimSpace(op.Error) != "" {
			facts = append(facts, "error="+op.Error)
		}
		lines = append(lines, "adapter: "+strings.Join(facts, " "))
	}
	for _, op := range report.Smoke {
		var facts []string
		facts = append(facts, "operation="+op.Operation)
		facts = append(facts, "status="+firstNonEmptyString(op.Status, "unknown"))
		if strings.TrimSpace(op.ProviderJobID) != "" {
			facts = append(facts, "provider_job="+op.ProviderJobID)
		}
		if strings.TrimSpace(op.RawState) != "" {
			facts = append(facts, "provider_state="+op.RawState)
		}
		if strings.TrimSpace(op.Reason) != "" {
			facts = append(facts, "reason="+strings.ReplaceAll(op.Reason, " ", "_"))
		}
		if strings.TrimSpace(op.Error) != "" {
			facts = append(facts, "error="+op.Error)
		}
		lines = append(lines, "smoke: "+strings.Join(facts, " "))
	}
	for _, issue := range report.Issues {
		lines = append(lines, "issue: "+issue)
	}
	return strings.Join(lines, "\n")
}

func maxProfileRevisionForCLI(revision int) int {
	if revision <= 0 {
		return 1
	}
	return revision
}

func beaconAllocationsForProfileCLI(st beacon.State, profile string) []beacon.AllocationRequest {
	profile = strings.TrimSpace(profile)
	var out []beacon.AllocationRequest
	for _, req := range st.Allocations {
		if strings.TrimSpace(req.Profile) != profile && strings.TrimSpace(req.Target.Profile) != profile {
			continue
		}
		switch req.State {
		case beacon.AllocationCanceled, beacon.AllocationExpired, beacon.AllocationFailed:
			continue
		}
		out = append(out, req)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func loadBeaconState(path string) (beacon.State, error) {
	store, err := beacon.NewStore(path)
	if err != nil {
		return beacon.State{}, err
	}
	return store.Load()
}

func beaconProxyResolver(root *rootOptions) (func(string) bool, error) {
	store, err := config.NewStore(root.configPath)
	if err != nil {
		return nil, err
	}
	cfg, err := store.Load()
	if err != nil {
		return nil, err
	}
	return func(name string) bool {
		name = strings.TrimSpace(name)
		for _, p := range cfg.Profiles {
			if p.ID == name || p.Name == name {
				return true
			}
		}
		return false
	}, nil
}

func findBeaconMachine(st beacon.State, ref string) (beacon.Machine, bool) {
	_, m, ok := findBeaconMachineEntry(st, ref)
	return m, ok
}

func findBeaconMachineEntry(st beacon.State, ref string) (string, beacon.Machine, bool) {
	ref = strings.TrimSpace(ref)
	for key, m := range st.Machines {
		if m.ID == ref || m.LeaseID == ref || m.ProviderJobID == ref {
			return key, m, true
		}
		for _, job := range m.Jobs {
			if job == ref {
				return key, m, true
			}
		}
	}
	return "", beacon.Machine{}, false
}

func sortedBeaconAllocations(st beacon.State) []beacon.AllocationRequest {
	out := make([]beacon.AllocationRequest, 0, len(st.Allocations))
	for _, req := range st.Allocations {
		out = append(out, req)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func findBeaconAllocation(st beacon.State, ref string) (beacon.AllocationRequest, bool) {
	ref = strings.TrimSpace(ref)
	for _, req := range st.Allocations {
		if req.ID == ref || req.DeterministicName == ref || req.ProviderIdentity.ProviderJobID == ref {
			return req, true
		}
	}
	return beacon.AllocationRequest{}, false
}

func beaconAllocationHasStartedJob(st beacon.State, allocationID string) bool {
	return beacon.AllocationHasStartedJob(st, allocationID)
}

func beaconAllocationHasEverRun(req beacon.AllocationRequest) bool {
	switch req.State {
	case beacon.AllocationRunning, beacon.AllocationExpired, beacon.AllocationFailed:
		return true
	default:
		return false
	}
}

func applyBeaconMachineRelease(st *beacon.State, key string, m beacon.Machine, action string) {
	switch action {
	case "drain":
		m.State = "draining"
		st.Machines[key] = m
	case "release":
		delete(st.Machines, key)
	case "kill_quarantine":
		beacon.TombstoneJobsForMachine(st, m, "machine hard-killed by operator", time.Now())
		m.Jobs = nil
		m.State = "kill_quarantine"
		st.Machines[key] = m
	}
}
