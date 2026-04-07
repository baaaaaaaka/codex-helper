package cli

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

const envUserHomeHint = codexhistory.EnvUserHomeHint

type effectivePaths struct {
	Home             string
	HomeSource       string
	CodexDir         string
	CodexDirSource   string
	ConfigPath       string
	ConfigPathSource string
	ExecIdentity     *execIdentity
	ExecSource       string
	AliasRootHome    bool
	AliasProof       string
}

type effectivePathMode struct {
	requireExecIdentity bool
}

var (
	effectivePathsGetenv           = os.Getenv
	effectivePathsUserHomeDir      = os.UserHomeDir
	effectivePathsLookupUserByID   = user.LookupId
	effectivePathsLookupUserByName = user.Lookup
	effectivePathsRunningAsRoot    = cliRunningAsRoot
	effectivePathsStat             = os.Stat
	effectivePathsSameFile         = os.SameFile
	effectivePathsEvalSymlinks     = filepath.EvalSymlinks
	effectivePathsExecIdentityHome = execIdentityForHome
	effectivePathsExecIdentityUser = execIdentityForUser
)

func resolveEffectivePaths(configPathOverride string, codexDirOverride string, workingDir string) (effectivePaths, error) {
	return resolveEffectivePathsWithMode(configPathOverride, codexDirOverride, workingDir, effectivePathMode{})
}

func resolveEffectiveLaunchPaths(configPathOverride string, codexDirOverride string, workingDir string) (effectivePaths, error) {
	return resolveEffectivePathsWithMode(configPathOverride, codexDirOverride, workingDir, effectivePathMode{
		requireExecIdentity: true,
	})
}

func resolveEffectivePathsWithMode(
	configPathOverride string,
	codexDirOverride string,
	workingDir string,
	mode effectivePathMode,
) (effectivePaths, error) {
	paths := effectivePaths{}

	currentHome, err := effectivePathsUserHomeDir()
	if err != nil {
		return paths, err
	}
	currentHome = filepath.Clean(currentHome)
	paths.Home = currentHome
	paths.HomeSource = "current-home"

	if hint, err := trustedUserHomeHint(mode); err != nil {
		return paths, err
	} else if hint != nil {
		candidateHome := hint.Home
		source := hint.Source
		switch same, proof, _ := proveSameHome(currentHome, candidateHome); {
		case same:
			paths.AliasRootHome = effectivePathsRunningAsRoot()
			paths.AliasProof = proof
			paths.HomeSource = "alias:" + source
			if effectivePathsRunningAsRoot() && hint.Identity != nil && hint.Identity.UID != 0 {
				paths.ExecIdentity = hint.Identity
				paths.ExecSource = source
			}
		default:
			paths.Home = candidateHome
			paths.HomeSource = source
			if effectivePathsRunningAsRoot() && hint.Identity != nil && hint.Identity.UID != 0 {
				paths.ExecIdentity = hint.Identity
				paths.ExecSource = source
			}
		}
		if mode.requireExecIdentity && effectivePathsRunningAsRoot() && hint.Identity == nil {
			return paths, execIdentityRequiredError(candidateHome)
		}
	}

	codexDir, source, err := resolveCodexDirOverride(codexDirOverride, workingDir)
	if err != nil {
		return paths, err
	}
	if strings.TrimSpace(codexDir) == "" {
		codexDir = filepath.Join(paths.Home, ".codex")
		source = paths.HomeSource
	}
	paths.CodexDir = codexDir
	paths.CodexDirSource = source

	if v := strings.TrimSpace(configPathOverride); v != "" {
		paths.ConfigPath = v
		paths.ConfigPathSource = "override:config"
		return paths, nil
	}

	if samePath(paths.Home, currentHome) || paths.ExecIdentity != nil {
		configPath, err := config.DefaultPath()
		if err != nil {
			return paths, err
		}
		paths.ConfigPath = configPath
		paths.ConfigPathSource = "default"
		return paths, nil
	}

	configPath, err := config.DefaultPathForHome(paths.Home)
	if err != nil {
		return paths, err
	}
	paths.ConfigPath = configPath
	paths.ConfigPathSource = paths.HomeSource
	return paths, nil
}

func resolveCodexDirOverride(override string, workingDir string) (string, string, error) {
	if v := strings.TrimSpace(override); v != "" {
		path, err := resolveCodexHomePath(v, workingDir)
		return path, "override:codex-dir", err
	}
	if v := strings.TrimSpace(effectivePathsGetenv(codexhistory.EnvCodexDir)); v != "" {
		path, err := resolveCodexHomePath(v, workingDir)
		return path, "env:" + codexhistory.EnvCodexDir, err
	}
	if v := strings.TrimSpace(effectivePathsGetenv(envCodexHome)); v != "" {
		path, err := resolveCodexHomePath(v, workingDir)
		return path, "env:" + envCodexHome, err
	}
	return "", "", nil
}

type userHomeHint struct {
	Home     string
	Source   string
	Identity *execIdentity
}

func trustedUserHomeHint(mode effectivePathMode) (*userHomeHint, error) {
	if v := strings.TrimSpace(effectivePathsGetenv(envUserHomeHint)); v != "" {
		home := filepath.Clean(os.ExpandEnv(v))
		if !filepath.IsAbs(home) {
			return nil, nil
		}
		identity, err := effectivePathsExecIdentityHome(home)
		if err != nil {
			if mode.requireExecIdentity {
				return nil, err
			}
			identity = nil
		}
		return &userHomeHint{
			Home:     home,
			Source:   "env:" + envUserHomeHint,
			Identity: identity,
		}, nil
	}
	if !effectivePathsRunningAsRoot() {
		return nil, nil
	}

	sudoUID := strings.TrimSpace(effectivePathsGetenv("SUDO_UID"))
	sudoUser := strings.TrimSpace(effectivePathsGetenv("SUDO_USER"))
	if sudoUID == "" || sudoUID == "0" || sudoUser == "" || strings.EqualFold(sudoUser, "root") {
		return nil, nil
	}

	byID, err := effectivePathsLookupUserByID(sudoUID)
	if err != nil || byID == nil || strings.TrimSpace(byID.HomeDir) == "" {
		return nil, nil
	}
	byName, err := effectivePathsLookupUserByName(sudoUser)
	if err != nil || byName == nil || strings.TrimSpace(byName.HomeDir) == "" {
		return nil, nil
	}

	if strings.TrimSpace(byID.Uid) == "" || strings.TrimSpace(byName.Uid) == "" || byID.Uid != byName.Uid {
		return nil, nil
	}

	homeByID := filepath.Clean(byID.HomeDir)
	homeByName := filepath.Clean(byName.HomeDir)
	if homeByID != homeByName {
		return nil, nil
	}
	identity, err := effectivePathsExecIdentityUser(byName, homeByID)
	if err != nil {
		if mode.requireExecIdentity {
			return nil, err
		}
		identity = nil
	}
	if identity == nil {
		identity, err = effectivePathsExecIdentityHome(homeByID)
		if err != nil {
			if mode.requireExecIdentity {
				return nil, err
			}
			identity = nil
		}
		if identity == nil && mode.requireExecIdentity {
			return &userHomeHint{
				Home:   homeByID,
				Source: "env:SUDO_USER+SUDO_UID",
			}, nil
		}
	}
	return &userHomeHint{
		Home:     homeByID,
		Source:   "env:SUDO_USER+SUDO_UID",
		Identity: identity,
	}, nil
}

func proveSameHome(currentHome string, candidateHome string) (same bool, proof string, known bool) {
	currentHome = filepath.Clean(strings.TrimSpace(currentHome))
	candidateHome = filepath.Clean(strings.TrimSpace(candidateHome))
	if currentHome == "" || candidateHome == "" {
		return false, "", false
	}
	if samePath(currentHome, candidateHome) {
		return true, "path", true
	}

	checks := []struct {
		proof string
		left  string
		right string
	}{
		{
			proof: "config",
			left:  filepath.Join(currentHome, ".config", "codex-proxy", "config.json"),
			right: filepath.Join(candidateHome, ".config", "codex-proxy", "config.json"),
		},
		{
			proof: "config-dir",
			left:  filepath.Join(currentHome, ".config"),
			right: filepath.Join(candidateHome, ".config"),
		},
		{
			proof: "codex",
			left:  filepath.Join(currentHome, ".codex"),
			right: filepath.Join(candidateHome, ".codex"),
		},
		{
			proof: "home",
			left:  currentHome,
			right: candidateHome,
		},
	}

	known = false
	for _, check := range checks {
		match, ok := sameExistingFile(check.left, check.right)
		if !ok {
			continue
		}
		known = true
		if match {
			return true, check.proof, true
		}
	}
	return false, "", known
}

func sameExistingFile(left string, right string) (bool, bool) {
	left = comparablePath(left)
	right = comparablePath(right)
	if left == "" || right == "" {
		return false, false
	}

	leftInfo, err := effectivePathsStat(left)
	if err != nil {
		return false, false
	}
	rightInfo, err := effectivePathsStat(right)
	if err != nil {
		return false, false
	}
	return effectivePathsSameFile(leftInfo, rightInfo), true
}

func comparablePath(path string) string {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" {
		return ""
	}
	resolved, err := effectivePathsEvalSymlinks(path)
	if err == nil && strings.TrimSpace(resolved) != "" {
		return filepath.Clean(resolved)
	}
	return path
}

func samePath(left string, right string) bool {
	return filepath.Clean(strings.TrimSpace(left)) == filepath.Clean(strings.TrimSpace(right))
}

func newRootStore(root *rootOptions, codexDirOverride string) (*config.Store, effectivePaths, error) {
	configPathOverride := ""
	if root != nil {
		configPathOverride = root.configPath
	}
	paths, err := resolveEffectivePaths(configPathOverride, codexDirOverride, "")
	if err != nil {
		return nil, effectivePaths{}, err
	}
	store, err := config.NewStore(paths.ConfigPath)
	if err != nil {
		return nil, effectivePaths{}, fmt.Errorf("open config store: %w", err)
	}
	return store, paths, nil
}
