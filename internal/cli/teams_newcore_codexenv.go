package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/env"
	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

// teamsNewCoreCodexLaunch is how the new core starts `codex app-server`
// (SPEC R-24: Codex runs in the user's environment). It ports what the old
// core's teamsPolicyAppServerStarter added to every app-server:
//
//   - the local Responses policy gateway: Codex's shell tool calls ask for
//     escalated sandbox permissions ("run with access to the execution
//     target's assigned hardware and mounts"), which the helper approves,
//     so commands reach GPUs, mounts and the network as in the user's
//     terminal; Codex is started with approval_policy="on-request" and
//     approvals_reviewer="user" and its OpenAI base URL on the gateway;
//   - the cxp proxy (HTTP(S)_PROXY) when cxp proxy profiles are on, for
//     Codex and for the gateway's upstream connections;
//   - the user's login-shell PATH (and the native Codex binary behind an
//     npm/bun wrapper, as the old core resolved it);
//   - the child markers that stop a Codex turn from managing the helper
//     service (CODEX_HELPER_TEAMS_CHILD and the helper CLI path);
//   - the execution identity when the helper runs as root for a user.
//
// One gateway serves every app-server of the process.
type teamsNewCoreCodexLaunch struct {
	Bin       string
	Identity  *execIdentity
	Args      []string
	Configure func(*exec.Cmd) error
	// Done reports the gateway failing (nil channel: no gateway).
	Done  <-chan error
	Close func()
}

func newTeamsNewCoreCodexLaunch(ctx context.Context, root *rootOptions, codexBin string, log io.Writer) (teamsNewCoreCodexLaunch, error) {
	store, paths, err := newRootStore(root, "")
	if err != nil {
		return teamsNewCoreCodexLaunch{}, err
	}
	defer store.Close()
	cfg, err := store.Load()
	if err != nil {
		return teamsNewCoreCodexLaunch{}, err
	}
	environ := setEnvValue(os.Environ(), "CODEX_HOME", paths.CodexDir)
	proxyURL := ""
	if upgradeUsesProxy(cfg) {
		profile, err := selectProfile(cfg, "")
		if err != nil {
			return teamsNewCoreCodexLaunch{}, err
		}
		if proxyURL, err = codexAppEnsureProxyURLFn(ctx, store, profile, cfg.Instances, log); err != nil {
			return teamsNewCoreCodexLaunch{}, fmt.Errorf("cxp proxy for Codex: %w", err)
		}
		environ = env.WithProxy(environ, proxyURL)
	}
	for _, kv := range teamsCodexChildEnv() {
		if k, v, ok := strings.Cut(kv, "="); ok {
			environ = setEnvValue(environ, k, v)
		}
	}
	pathResult, pathErr := resolveTeamsCodexUserPath(ctx, cfg, paths, environ, paths.Home)
	if pathErr != nil {
		_, _ = fmt.Fprintf(log, "Codex runs with the service PATH: the login-shell PATH could not be resolved: %v\n", pathErr)
	} else {
		environ = setEnvValue(environ, "PATH", pathResult.Path)
	}
	if strings.TrimSpace(codexBin) == "" {
		if codexBin, err = resolveTeamsNewCoreManagedCodex(ctx, store, cfg, codexRuntimeEnvironment(environ, nil, paths.ExecIdentity), paths.ExecIdentity, log); err != nil {
			return teamsNewCoreCodexLaunch{}, fmt.Errorf("prepare CXP-managed Codex for Teams: %w", err)
		}
	}
	bin := codexBin
	if pathErr == nil {
		rt := applyTeamsUserPathRuntime(resolvedCodexRuntime{Command: codexBin, WrapperCommand: codexBin,
			Environment: environ}, pathResult, log)
		bin, environ = rt.Command, rt.Environment
	}
	gw, err := responsespolicy.StartServer(responsespolicy.ServerOptions{ProxyURL: proxyURL})
	if err != nil {
		return teamsNewCoreCodexLaunch{}, err
	}
	identity := paths.ExecIdentity
	return teamsNewCoreCodexLaunch{
		Bin:      bin,
		Identity: identity,
		Args:     gw.CodexConfigArgs(),
		Configure: func(cmd *exec.Cmd) error {
			e, err := applyExecIdentity(cmd, append([]string(nil), environ...), identity)
			if err != nil {
				return err
			}
			cmd.Env = e
			return nil
		},
		Done: gw.Done(),
		Close: func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = gw.Close(sctx)
		},
	}, nil
}

// resolveTeamsNewCoreManagedCodex is the Codex the Teams app-servers run
// without --codex-path (SPEC R-24): the CXP-managed install, found in the
// app-server environment (its HOME, CODEX_NPM_PREFIX, identity), else
// installed there — as the old core did, and the same resolution the Teams
// Codex upgrade uses, so an upgrade replaces the binary that runs. A
// `codex` earlier on PATH is not used: it is not what `codex update now`
// or a too-old rejection upgrades.
var resolveTeamsNewCoreManagedCodex = func(ctx context.Context, store *config.Store, cfg config.Config, environment []string, identity *execIdentity, log io.Writer) (string, error) {
	target := resolveManagedCodexUpgradeTarget(ctx, environment, identity)
	if target.path != "" {
		return target.path, nil
	}
	opts, err := teamsManagedCodexInstallOptions(ctx, store, cfg, target, log)
	if err != nil {
		return "", err
	}
	return upgradeOrInstallManagedCodex(ctx, log, target, opts)
}

// teamsNewCoreCodexArgs translates `teams run --codex-arg` for the new
// core's app-servers with the old core's translation (--model, --search,
// -c/--config, --enable/--disable, --strict-config,
// --dangerously-bypass-hook-trust). Arguments that do not apply are
// skipped with a warning rather than failing the service:
//   - --oss and --local-provider select a local or third-party model
//     provider, which the helper no longer supports (Codex's own API only);
//   - -C/--cd, --add-dir, -i/--image, --output-schema, -o, --ephemeral,
//     --json and --color are per-run options of `codex exec`; each work
//     chat has its own directory and attachments in the new core;
//   - --sandbox and --ask-for-approval are set by the helper's policy.
//
// An argument the translation does not know fails, as in the old core.
func teamsNewCoreCodexArgs(args []string, log io.Writer) ([]string, error) {
	args = expandCodexOptionEquals(args)
	kept := make([]string, 0, len(args))
	skip := func(arg string, withValue bool, why string, index *int) {
		if withValue {
			*index++
		}
		_, _ = fmt.Fprintf(log, "--codex-arg %s is ignored by the new Teams core: %s\n", arg, why)
	}
	for i := 0; i < len(args); i++ {
		arg := strings.TrimSpace(args[i])
		switch arg {
		case "--oss":
			skip(arg, false, "local and third-party model providers are not supported", &i)
		case "--local-provider":
			skip(arg, i+1 < len(args), "local and third-party model providers are not supported", &i)
		case "--ephemeral", "--json":
			skip(arg, false, "it is a per-run option of codex exec", &i)
		case "-C", "--cd", "--add-dir", "-i", "--image", "--output-schema", "-o", "--output-last-message", "--color":
			skip(arg, i+1 < len(args), "it is a per-run option of codex exec", &i)
		case "--sandbox", "-s", "--ask-for-approval", "-a":
			skip(arg, i+1 < len(args), "the helper sets the sandbox and approval policy", &i)
		case "-c", "--config", "--enable", "--disable", "--model", "-m":
			kept = append(kept, args[i])
			if i+1 < len(args) {
				i++
				kept = append(kept, args[i])
			}
		default:
			kept = append(kept, args[i])
		}
	}
	return translateTeamsCodexArgsToAppServer(kept)
}
