package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/migration"
)

// Codex CLI arguments and upgrades of the Teams helper.

func translateTeamsCodexArgsToAppServer(args []string) ([]string, error) {
	args = expandCodexOptionEquals(migration.RemoveLegacyCodexExecutionOverrides(args))
	out := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		switch arg {
		case "":
			continue
		case "--skip-git-repo-check":
			// app-server does not perform the exec subcommand's repository gate.
			continue
		case "--model", "-m":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a model", arg)
			}
			index++
			out = append(out, "-c", `model="`+tomlEscapeString(args[index])+`"`)
		case "--search":
			out = append(out, "-c", `web_search="live"`)
		case "--oss":
			out = append(out, "-c", `model_provider="ollama"`)
		case "--local-provider":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("--local-provider requires a value")
			}
			index++
			out = append(out, "-c", `model_provider="`+tomlEscapeString(args[index])+`"`)
		case "--dangerously-bypass-hook-trust":
			out = append(out, "-c", "bypass_hook_trust=true")
		case "-c", "--config", "--enable", "--disable":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			out = append(out, arg, args[index+1])
			index++
		case "--strict-config":
			out = append(out, arg)
		case "--sandbox", "-s", "--ask-for-approval", "-a":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			// The unified runtime owns these settings and appends its policy last.
			index++
		case "--ephemeral", "--json":
			// Ephemeral and turn inputs are carried over the app-server protocol.
		case "-C", "--cd", "--add-dir", "-i", "--image", "--output-schema", "-o", "--output-last-message", "--color":
			if index+1 >= len(args) {
				return nil, fmt.Errorf("%s requires a value", arg)
			}
			index++
		default:
			return nil, fmt.Errorf("Teams Codex argument %q cannot be translated to app-server", arg)
		}
	}
	return out, nil
}

var resolveTeamsCodexUpgradeTargetForRun = resolveTeamsCodexUpgradeTarget

func runTeamsUpgradeCodexOnce(cmd interface {
	Context() context.Context
	ErrOrStderr() io.Writer
	OutOrStdout() io.Writer
}, root *rootOptions, codexPath string) error {
	if strings.TrimSpace(codexPath) != "" {
		return fmt.Errorf("--upgrade-codex cannot be used with --codex-path")
	}
	if err := ensureTeamsIdleBeforeCodexUpgrade(cmd.Context()); err != nil {
		return err
	}
	path, err := upgradeTeamsCodex(cmd.Context(), root, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex upgraded before Teams listen: %s\n", path)
	return nil
}

// upgradeTeamsCodex upgrades the discovered Codex CLI from its install
// source (through the proxy profile when the helper uses one) and returns
// the installed path.
func upgradeTeamsCodex(ctx context.Context, root *rootOptions, out io.Writer) (string, error) {
	store, paths, err := newRootStore(root, "")
	if err != nil {
		return "", err
	}
	defer store.Close()
	cfg, err := store.Load()
	if err != nil {
		return "", err
	}
	upgradeTarget, err := resolveTeamsCodexUpgradeTargetForRun(ctx, cfg, paths)
	if err != nil {
		return "", err
	}
	installOpts, err := teamsManagedCodexInstallOptions(ctx, store, cfg, upgradeTarget, out)
	if err != nil {
		return "", err
	}
	return upgradeOrInstallManagedCodex(ctx, out, upgradeTarget, installOpts)
}

// teamsManagedCodexInstallOptions are the install options for the
// CXP-managed Codex of target: its environment and identity, and the cxp
// proxy profile for the installer when the helper uses one. The Teams
// launch (first install) and the Teams upgrade share them.
func teamsManagedCodexInstallOptions(ctx context.Context, store *config.Store, cfg config.Config, target managedCodexUpgradeTarget, out io.Writer) (codexInstallOptions, error) {
	installOpts := managedCodexUpgradeInstallOptions(target)
	if upgradeUsesProxy(cfg) {
		profile, cfgWithProfile, err := ensureProfileRunFn(ctx, store, "", true, out)
		if err != nil {
			return codexInstallOptions{}, err
		}
		installOpts.withInstallerEnv = func(ctx context.Context, runInstall func([]string) error) error {
			return withProfileInstallEnv(ctx, store, profile, cfgWithProfile.Instances, func(profileEnv []string) error {
				return runInstall(managedCodexUpgradeProxyEnvironment(target.environment, profileEnv))
			})
		}
	}
	return installOpts, nil
}

func resolveTeamsCodexUpgradeTarget(ctx context.Context, cfg config.Config, paths effectivePaths) (managedCodexUpgradeTarget, error) {
	workDir, err := os.Getwd()
	if err != nil {
		return managedCodexUpgradeTarget{}, fmt.Errorf("resolve Teams Codex upgrade working directory: %w", err)
	}
	pathResult, err := resolveTeamsCodexUserPath(ctx, cfg, paths, teamsCodexChildEnv(), workDir)
	if err != nil {
		return managedCodexUpgradeTarget{}, err
	}
	environment := codexRuntimeEnvironment(os.Environ(), []string{"PATH=" + pathResult.Path}, paths.ExecIdentity)
	return resolveManagedCodexUpgradeTarget(ctx, environment, paths.ExecIdentity), nil
}
