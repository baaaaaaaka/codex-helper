package codexhistory

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

const envCodexHome = "CODEX_HOME"
const EnvUserHomeHint = "CXP_USER_HOME"

type CodexDirSelection struct {
	Dir    string
	Home   string
	Source string
}

var (
	resolveCodexDirGetenv           = os.Getenv
	resolveCodexDirUserHomeDir      = os.UserHomeDir
	resolveCodexDirLookupUserByID   = user.LookupId
	resolveCodexDirLookupUserByName = user.Lookup
	resolveCodexDirRunningAsRoot    = runningAsRoot
	resolveCodexDirStat             = os.Stat
	resolveCodexDirSameFile         = os.SameFile
	resolveCodexDirEvalSymlinks     = filepath.EvalSymlinks
)

func ResolveCodexDirSelection(override string) (CodexDirSelection, error) {
	if v := strings.TrimSpace(override); v != "" {
		return codexDirSelectionForPath(v, "override"), nil
	}
	if v := strings.TrimSpace(resolveCodexDirGetenv(EnvCodexDir)); v != "" {
		return codexDirSelectionForPath(v, "env:"+EnvCodexDir), nil
	}
	if v := strings.TrimSpace(resolveCodexDirGetenv(envCodexHome)); v != "" {
		return codexDirSelectionForPath(v, "env:"+envCodexHome), nil
	}

	home, err := resolveCodexDirUserHomeDir()
	if err != nil {
		return CodexDirSelection{}, err
	}
	home = filepath.Clean(home)

	if candidateHome, source, ok := trustedUserHomeHint(); ok {
		if same, _, _ := proveSameHome(home, candidateHome); same {
			return CodexDirSelection{
				Dir:    filepath.Join(home, ".codex"),
				Home:   home,
				Source: "alias:" + source,
			}, nil
		}
		return CodexDirSelection{
			Dir:    filepath.Join(candidateHome, ".codex"),
			Home:   candidateHome,
			Source: source,
		}, nil
	}
	return CodexDirSelection{
		Dir:    filepath.Join(home, ".codex"),
		Home:   home,
		Source: "default",
	}, nil
}

func codexDirSelectionForPath(raw string, source string) CodexDirSelection {
	dir := filepath.Clean(os.ExpandEnv(strings.TrimSpace(raw)))
	selection := CodexDirSelection{
		Dir:    dir,
		Source: source,
	}
	if filepath.Base(dir) == ".codex" {
		selection.Home = filepath.Dir(dir)
	}
	return selection
}

func trustedUserHomeHint() (string, string, bool) {
	if v := strings.TrimSpace(resolveCodexDirGetenv(EnvUserHomeHint)); v != "" {
		home := filepath.Clean(os.ExpandEnv(v))
		if !filepath.IsAbs(home) {
			return "", "", false
		}
		return home, "env:" + EnvUserHomeHint, true
	}

	if !resolveCodexDirRunningAsRoot() {
		return "", "", false
	}

	sudoUID := strings.TrimSpace(resolveCodexDirGetenv("SUDO_UID"))
	sudoUser := strings.TrimSpace(resolveCodexDirGetenv("SUDO_USER"))
	if sudoUID == "" || sudoUID == "0" || sudoUser == "" || strings.EqualFold(sudoUser, "root") {
		return "", "", false
	}

	byID, err := resolveCodexDirLookupUserByID(sudoUID)
	if err != nil || byID == nil || strings.TrimSpace(byID.HomeDir) == "" {
		return "", "", false
	}
	byName, err := resolveCodexDirLookupUserByName(sudoUser)
	if err != nil || byName == nil || strings.TrimSpace(byName.HomeDir) == "" {
		return "", "", false
	}
	if strings.TrimSpace(byID.Uid) == "" || strings.TrimSpace(byName.Uid) == "" || byID.Uid != byName.Uid {
		return "", "", false
	}

	homeByID := filepath.Clean(byID.HomeDir)
	homeByName := filepath.Clean(byName.HomeDir)
	if homeByID != homeByName {
		return "", "", false
	}
	return homeByID, "env:SUDO_USER+SUDO_UID", true
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

	leftInfo, err := resolveCodexDirStat(left)
	if err != nil {
		return false, false
	}
	rightInfo, err := resolveCodexDirStat(right)
	if err != nil {
		return false, false
	}
	return resolveCodexDirSameFile(leftInfo, rightInfo), true
}

func comparablePath(path string) string {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" {
		return ""
	}
	resolved, err := resolveCodexDirEvalSymlinks(path)
	if err == nil && strings.TrimSpace(resolved) != "" {
		return filepath.Clean(resolved)
	}
	return path
}

func samePath(left string, right string) bool {
	return filepath.Clean(strings.TrimSpace(left)) == filepath.Clean(strings.TrimSpace(right))
}
