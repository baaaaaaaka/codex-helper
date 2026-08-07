package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func runUpgradeCodexAppFromRoot(cmd *cobra.Command, root *rootOptions) error {
	profileRef, err := rootProfileArg(cmd)
	if err != nil {
		return err
	}
	if err := validateCodexWindowsManagedAppUpdatePlatform(); err != nil {
		return err
	}
	ctx, stop := withSignalContext(cmd.Context())
	defer stop()

	store, _, err := newRootStore(root, "")
	if err != nil {
		return err
	}
	cfg, err := store.Load()
	if err != nil {
		return err
	}

	installOpts := codexDesktopAppOptions{Log: cmd.ErrOrStderr()}
	if profileRef != "" || upgradeUsesProxy(cfg) {
		profile, err := selectProfile(cfg, profileRef)
		if err != nil {
			return err
		}
		proxyURL, err := codexAppEnsureProxyURLFn(ctx, store, profile, cfg.Instances, cmd.ErrOrStderr())
		if err != nil {
			return err
		}
		installOpts.ProxyURL = proxyURL
	}

	managedRoot, err := codexAppWindowsManagedRootFn(ctx)
	if err != nil {
		return fmt.Errorf("resolve managed Codex desktop app root: %w", err)
	}
	managedRoot = strings.TrimSpace(managedRoot)
	if managedRoot == "" {
		return fmt.Errorf("resolve managed Codex desktop app root: empty path")
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex desktop app update target (CXP-managed): %s\n", managedRoot)

	state, changed, err := codexAppUpgradeManagedInstallFn(ctx, managedRoot, installOpts)
	if err != nil {
		return err
	}
	if changed {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex desktop app upgraded: %s\n", state.PackageVersion)
	} else {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex desktop app is already up to date: %s\n", state.PackageVersion)
	}
	return nil
}

func validateCodexWindowsManagedAppUpdatePlatform() error {
	switch codexAppGOOS() {
	case "windows":
		arch := strings.TrimSpace(codexAppGOARCH())
		if !strings.EqualFold(arch, codexWindowsManagedArch) && !strings.EqualFold(arch, "amd64") {
			return fmt.Errorf("--upgrade-codex-app supports x64 Windows only; current architecture: %s", arch)
		}
		return nil
	case "linux":
		if codexAppIsWSL() {
			return nil
		}
	}
	return fmt.Errorf("--upgrade-codex-app is only supported on native Windows or WSL; current platform: %s", codexAppGOOS())
}
