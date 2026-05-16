package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/spf13/cobra"
)

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
		newBeaconSwitchProfileCmd(root, &storePath),
		newBeaconMachineCmd(&storePath),
	)
	return cmd
}

func newBeaconProfileCmd(root *rootOptions, storePath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "profile", Short: "Manage beacon profiles"}
	cmd.AddCommand(
		newBeaconProfileListCmd(root, storePath),
		newBeaconProfileCreateCmd(root, storePath),
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
				if reasons := p.DraftReasons(proxyExists); len(reasons) > 0 {
					state = "draft: " + strings.Join(reasons, "; ")
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", p.Name, p.Provider, state)
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
	var nodes int
	var gpuCount int
	var partition string
	var image string
	var duration int
	var queue string
	var lsfSitePolicy bool
	var lsfAdvanced bool

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
				created, err = beacon.CreateProfile(st, beacon.CreateProfileInput{
					Name:                         args[0],
					Provider:                     beacon.Provider(provider),
					ProxyMode:                    beacon.ProxyMode(proxyMode),
					ProxyProfile:                 proxyProfile,
					IsolationDefault:             beacon.Isolation(isolation),
					Slurm:                        beacon.SlurmProfile{Nodes: nodes, GPUCount: gpuCount, Partition: partition, Image: image, Duration: duration},
					LSF:                          beacon.LSFProfile{QueueName: queue, SitePolicyDerivesResources: lsfSitePolicy, AdvancedApproved: lsfAdvanced},
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
	cmd.Flags().IntVar(&nodes, "nodes", 0, "Slurm node count")
	cmd.Flags().IntVar(&gpuCount, "gpu", 0, "Slurm GPU count")
	cmd.Flags().StringVar(&partition, "partition", "", "Slurm partition")
	cmd.Flags().StringVar(&image, "image", "", "Slurm container image")
	cmd.Flags().IntVar(&duration, "duration", 0, "Slurm duration")
	cmd.Flags().StringVar(&queue, "queue", "", "LSF queue name")
	cmd.Flags().BoolVar(&lsfSitePolicy, "lsf-site-policy", false, "Allow site policy to derive LSF resources")
	cmd.Flags().BoolVar(&lsfAdvanced, "lsf-advanced-approved", false, "Mark advanced LSF resources locally approved")
	return cmd
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
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "profile=%s provider=%s proxy=%s isolation=%s status=%s\n", p.Name, p.Provider, p.ProxyMode, p.IsolationDefault, status)
			return nil
		},
	}
}

func newBeaconProfileDoctorCmd(root *rootOptions, storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor <name>",
		Short: "Mark a beacon profile doctor check successful",
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
				p, err = beacon.DoctorProfile(st, args[0], time.Time{}, proxyExists)
				return err
			}); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Doctor passed for beacon profile %q.\n", p.Name)
			return nil
		},
	}
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
		Short: "Delete a beacon profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := beacon.NewStore(*storePath)
			if err != nil {
				return err
			}
			if err := store.Update(func(st *beacon.State) error { return beacon.DeleteProfile(st, args[0]) }); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Deleted beacon profile %q.\n", args[0])
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
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), beacon.RenderStatus(st, session))
			return nil
		},
	}
	cmd.Flags().StringVar(&session, "session", "", "Conversation/session id")
	return cmd
}

func newBeaconSwitchProfileCmd(root *rootOptions, storePath *string) *cobra.Command {
	var session string
	var fork bool
	var queuedOrRunning bool
	var signature string
	var signatureCompatible bool
	cmd := &cobra.Command{
		Use:   "switch-profile <name>",
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
					HasQueuedOrRunning:  queuedOrRunning,
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
	cmd.Flags().StringVar(&signature, "signature", "", "Execution signature snapshot")
	cmd.Flags().BoolVar(&signatureCompatible, "signature-compatible", true, "Whether the target signature is compatible")
	return cmd
}

func newBeaconMachineCmd(storePath *string) *cobra.Command {
	cmd := &cobra.Command{Use: "machine", Short: "Manage beacon machines and leases"}
	cmd.AddCommand(newBeaconMachineListCmd(storePath), newBeaconMachineStatusCmd(storePath), newBeaconMachineReleaseCmd(storePath), newBeaconMachineKillCmd(storePath))
	return cmd
}

func newBeaconMachineListCmd(storePath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List beacon machines",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			st, err := loadBeaconState(*storePath)
			if err != nil {
				return err
			}
			if len(st.Machines) == 0 {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No beacon machines.")
				return nil
			}
			var machines []beacon.Machine
			for _, m := range st.Machines {
				machines = append(machines, m)
			}
			sort.Slice(machines, func(i, j int) bool { return machines[i].ID < machines[j].ID })
			for _, m := range machines {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s\tlease=%s\tprovider_job=%s\tstate=%s\tjobs=%s\n", m.ID, m.LeaseID, m.ProviderJobID, m.State, strings.Join(m.Jobs, ","))
			}
			return nil
		},
	}
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
			p := beacon.PreviewRelease(m)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "machine=%s lease=%s provider_job=%s chats=%s jobs=%s confirm=%s\n", p.MachineID, p.LeaseID, p.ProviderJobID, strings.Join(p.Chats, ","), strings.Join(p.Jobs, ","), p.Confirmation)
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
		if m.ID == ref || m.LeaseID == ref {
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

func applyBeaconMachineRelease(st *beacon.State, key string, m beacon.Machine, action string) {
	switch action {
	case "drain":
		m.State = "draining"
		st.Machines[key] = m
	case "release":
		delete(st.Machines, key)
	case "kill_quarantine":
		m.State = "kill_quarantine"
		st.Machines[key] = m
	}
}
