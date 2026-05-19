package skills

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type GitRunner interface {
	Run(ctx context.Context, dir string, env []string, args ...string) ([]byte, error)
}

type ExecGitRunner struct {
	GitPath string
	Timeout time.Duration
}

type GitError struct {
	Args   []string
	Output string
	Err    error
}

func (e *GitError) Error() string {
	msg := strings.TrimSpace(e.Output)
	if msg == "" {
		msg = e.Err.Error()
	}
	return "git " + strings.Join(scrubGitArgs(e.Args), " ") + ": " + scrubGitOutput(msg)
}

func (e *GitError) Unwrap() error { return e.Err }

func (r ExecGitRunner) Run(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.Timeout)
		defer cancel()
	}
	git := strings.TrimSpace(r.GitPath)
	if git == "" {
		git = "git"
	}
	cmd := exec.CommandContext(ctx, git, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = safeGitEnv(env)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		output := stderr.String()
		if len(out) > 0 {
			output += "\n" + string(out)
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			output = "command timed out"
		}
		return out, &GitError{Args: args, Output: output, Err: err}
	}
	return out, nil
}

func safeGitEnv(extra []string) []string {
	base := os.Environ()
	env := make([]string, 0, len(base)+len(extra)+6)
	blocked := map[string]bool{
		"GIT_DIR":              true,
		"GIT_WORK_TREE":        true,
		"GIT_INDEX_FILE":       true,
		"GIT_OBJECT_DIRECTORY": true,
	}
	for _, kv := range base {
		name := kv
		if idx := strings.IndexByte(kv, '='); idx >= 0 {
			name = kv[:idx]
		}
		if blocked[name] {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"GIT_TERMINAL_PROMPT=0",
		"GCM_INTERACTIVE=never",
		"GIT_CONFIG_NOSYSTEM=1",
	)
	env = append(env, extra...)
	return env
}

func scrubGitArgs(args []string) []string {
	out := make([]string, len(args))
	for i, arg := range args {
		out[i] = scrubURLSecrets(arg)
	}
	return out
}

func scrubGitOutput(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "://") {
			line = scrubURLSecrets(line)
		}
		lines[i] = line
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func scrubURLSecrets(s string) string {
	for _, scheme := range []string{"https://", "http://"} {
		offset := 0
		for offset < len(s) {
			idx := strings.Index(strings.ToLower(s[offset:]), scheme)
			if idx < 0 {
				break
			}
			idx += offset
			start := idx + len(scheme)
			at := strings.IndexByte(s[start:], '@')
			if at < 0 {
				offset = start
				break
			}
			secretEnd := start + at
			s = s[:start] + "<redacted>@" + s[secretEnd+1:]
			offset = start + len("<redacted>@")
		}
	}
	return s
}

func ensureBareMirror(ctx context.Context, git GitRunner, cacheDir string, source Source) (string, error) {
	mirror := filepath.Join(cacheDir, "mirrors", source.ID+".git")
	if err := os.MkdirAll(filepath.Dir(mirror), 0o700); err != nil {
		return "", fmt.Errorf("create skill mirror cache: %w", err)
	}
	if _, err := os.Stat(filepath.Join(mirror, "HEAD")); err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat git mirror: %w", err)
		}
		if _, err := git.Run(ctx, "", nil, "init", "--bare", mirror); err != nil {
			return "", authHintError(source, err)
		}
	}
	for _, args := range [][]string{
		{"config", "core.hooksPath", "NUL"},
		{"config", "advice.detachedHead", "false"},
	} {
		_, _ = git.Run(ctx, mirror, nil, args...)
	}
	return mirror, nil
}

func fetchSource(ctx context.Context, git GitRunner, mirror string, source Source) (string, error) {
	ref := strings.TrimSpace(source.Ref)
	if ref == "" {
		detected, err := detectDefaultRef(ctx, git, source.RemoteURL)
		if err == nil && detected != "" {
			ref = detected
		} else {
			ref = "HEAD"
		}
	}
	_, err := git.Run(ctx, mirror, nil, "fetch", "--force", "--prune", "--depth=1", source.RemoteURL, ref)
	if err != nil {
		return "", authHintError(source, err)
	}
	out, err := git.Run(ctx, mirror, nil, "rev-parse", "FETCH_HEAD^{commit}")
	if err != nil {
		return "", err
	}
	commit := strings.TrimSpace(string(out))
	if commit == "" {
		return "", fmt.Errorf("git fetch produced empty commit")
	}
	return commit, nil
}

func detectDefaultRef(ctx context.Context, git GitRunner, remote string) (string, error) {
	out, err := git.Run(ctx, "", nil, "ls-remote", "--symref", remote, "HEAD")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == "ref:" && fields[2] == "HEAD" {
			ref := strings.TrimPrefix(fields[1], "refs/heads/")
			if ref != fields[1] {
				return ref, nil
			}
		}
	}
	return "", nil
}

func knownRemoteRefs(ctx context.Context, git GitRunner, remote string) []string {
	out, err := git.Run(ctx, "", nil, "ls-remote", "--heads", remote)
	if err != nil {
		return nil
	}
	var refs []string
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		ref := strings.TrimPrefix(fields[1], "refs/heads/")
		if ref != fields[1] && ref != "" {
			refs = append(refs, ref)
		}
	}
	return refs
}

func authHintError(source Source, err error) error {
	text := strings.ToLower(err.Error())
	if strings.Contains(text, "permission denied") ||
		strings.Contains(text, "publickey") ||
		strings.Contains(text, "authentication failed") ||
		strings.Contains(text, "could not read username") ||
		strings.Contains(text, "repository not found") ||
		strings.Contains(text, "403") ||
		strings.Contains(text, "401") {
		return fmt.Errorf("%w\n\n%s", err, authHint(source))
	}
	return err
}

func authHint(source Source) string {
	host := "the git host"
	if u, err := parseRemoteHost(source.RemoteURL); err == nil && u != "" {
		host = u
	}
	sshCheck, sshUserMissing := sshAuthCheckCommand(source.RemoteURL, "git")
	if sshCheck == "" {
		sshCheck = "ssh -T git@" + host
	}
	missingUserNote := ""
	if sshUserMissing {
		missingUserNote = " The subscribed SSH URL has no user, so SSH will use your local OS username; Git SSH remotes usually need `git@` or an SSH config with `User git`."
	}
	switch source.Provider {
	case "github":
		return "Authentication hint: run `gh auth login`, then `gh auth setup-git`; for SSH remotes also check `" + sshCheck + "`." + missingUserNote
	case "gitlab":
		return "Authentication hint: run `glab auth login` if you use glab, or verify your SSH key with `" + sshCheck + "`." + missingUserNote
	default:
		return "Authentication hint: verify that your git credentials can read " + source.RemoteURL + ". For SSH remotes, test with `" + sshCheck + "`." + missingUserNote
	}
}

func sshAuthCheckCommand(remote, fallbackUser string) (string, bool) {
	info, ok := parseSSHRemote(remote)
	if !ok || info.host == "" {
		return "", false
	}
	user := info.user
	userMissing := false
	if user == "" && fallbackUser != "" {
		user = fallbackUser
		userMissing = true
	}
	target := info.host
	if user != "" {
		target = user + "@" + info.host
	}
	parts := []string{"ssh", "-T"}
	if info.port != "" {
		parts = append(parts, "-p", info.port)
	}
	parts = append(parts, target)
	return strings.Join(parts, " "), userMissing
}

type sshRemoteInfo struct {
	user string
	host string
	port string
}

func parseSSHRemote(remote string) (sshRemoteInfo, bool) {
	if strings.Contains(remote, "://") {
		u, err := url.Parse(remote)
		if err != nil || strings.ToLower(u.Scheme) != "ssh" {
			return sshRemoteInfo{}, false
		}
		user := ""
		if u.User != nil {
			user = u.User.Username()
		}
		return sshRemoteInfo{user: user, host: u.Hostname(), port: u.Port()}, true
	}
	at := strings.Index(remote, "@")
	colon := strings.Index(remote, ":")
	if at <= 0 || colon <= at+1 {
		return sshRemoteInfo{}, false
	}
	return sshRemoteInfo{user: remote[:at], host: remote[at+1 : colon]}, true
}

func parseRemoteHost(remote string) (string, error) {
	if strings.Contains(remote, "://") {
		u, err := urlParse(remote)
		if err != nil {
			return "", err
		}
		return u, nil
	}
	at := strings.Index(remote, "@")
	colon := strings.Index(remote, ":")
	if at >= 0 && colon > at {
		return remote[at+1 : colon], nil
	}
	return "", fmt.Errorf("unknown remote host")
}

func urlParse(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	return u.Hostname(), nil
}
