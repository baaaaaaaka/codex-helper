//go:build !windows

package userpath

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	accountCurrentUser = user.Current
	accountReadFile    = os.ReadFile
	accountStat        = os.Stat
	accountCommand     = exec.CommandContext
)

type shellAdapter struct {
	name          string
	args          func(string) []string
	scriptOnStdin bool
}

func resolveAccountDefault(ctx context.Context, req Request) (Result, error) {
	if req.Timeout <= 0 {
		req.Timeout = 15 * time.Second
	}
	target, err := completeTargetIdentity(req.Target)
	if err != nil {
		return Result{}, err
	}
	req.Target = target
	shell := strings.TrimSpace(req.Policy.ShellOverride)
	if shell == "" {
		shell, err = lookupAccountShell(ctx, target)
		if err != nil {
			return Result{}, err
		}
	}
	if !filepath.IsAbs(shell) {
		return Result{}, fmt.Errorf("account shell %q is not an absolute path", shell)
	}
	info, err := accountStat(shell)
	if err != nil {
		return Result{}, fmt.Errorf("account shell %q is not runnable: %w", shell, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return Result{}, fmt.Errorf("account shell %q is not an executable file", shell)
	}
	adapter, err := adapterForShell(shell)
	if err != nil {
		return Result{}, err
	}
	help := strings.TrimSpace(req.HelperExecutable)
	if help == "" || !filepath.IsAbs(help) {
		return Result{}, fmt.Errorf("user PATH probe requires an absolute helper executable")
	}
	req.HelperExecutable = help

	baseline, baselineSource := accountProbeEnvironment(req.ServiceEnvironment, target, shell)
	payload, stdout, stderr, err := runShellProbe(ctx, req, target, shell, adapter, baseline)
	if err != nil {
		return Result{}, fmt.Errorf("resolve account-default PATH via %s (%s): %w%s", shell, adapter.name, err, formatProbeOutput(stdout, stderr))
	}
	payload.Path = sanitizeResolvedPath(payload.Path, req.ServiceEnvironment)
	if payload.Path == "" {
		return Result{}, fmt.Errorf("account shell %s returned an empty PATH", shell)
	}
	return Result{
		Path:           payload.Path,
		Source:         "account-shell",
		Mode:           ModeAccountDefault,
		Target:         target,
		AccountShell:   shell,
		Adapter:        adapter.name,
		BaselineSource: baselineSource,
	}, nil
}

func completeTargetIdentity(target TargetIdentity) (TargetIdentity, error) {
	if strings.TrimSpace(target.Username) != "" && strings.TrimSpace(target.Home) != "" {
		return target, nil
	}
	current, err := accountCurrentUser()
	if err != nil {
		return target, fmt.Errorf("resolve current account: %w", err)
	}
	uid, err := strconv.ParseUint(strings.TrimSpace(current.Uid), 10, 32)
	if err != nil {
		return target, fmt.Errorf("parse current account uid %q: %w", current.Uid, err)
	}
	gid, err := strconv.ParseUint(strings.TrimSpace(current.Gid), 10, 32)
	if err != nil {
		return target, fmt.Errorf("parse current account gid %q: %w", current.Gid, err)
	}
	if target.Username == "" {
		target.Username = strings.TrimSpace(current.Username)
	}
	if target.Home == "" {
		target.Home = filepath.Clean(current.HomeDir)
	}
	if target.UID == 0 && uint32(os.Geteuid()) != 0 {
		target.UID = uint32(uid)
	}
	if target.GID == 0 && uint32(os.Getegid()) != 0 {
		target.GID = uint32(gid)
	}
	return target, nil
}

func lookupAccountShell(ctx context.Context, target TargetIdentity) (string, error) {
	if runtime.GOOS == "darwin" {
		if shell := lookupDarwinAccountShell(ctx, target.Username); shell != "" {
			return shell, nil
		}
	} else {
		if shell := lookupGetentAccountShell(ctx, target); shell != "" {
			return shell, nil
		}
	}
	data, err := accountReadFile("/etc/passwd")
	if err == nil {
		if shell := shellFromPasswd(string(data), target); shell != "" {
			return shell, nil
		}
	}
	return "", fmt.Errorf("cannot resolve login shell for user %q uid %d; configure teamsCodexPath.shellOverride", target.Username, target.UID)
}

func lookupGetentAccountShell(ctx context.Context, target TargetIdentity) string {
	getent := firstExistingPath("/usr/bin/getent", "/bin/getent")
	if getent == "" {
		return ""
	}
	key := strings.TrimSpace(target.Username)
	if key == "" {
		key = strconv.FormatUint(uint64(target.UID), 10)
	}
	commandCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := accountCommand(commandCtx, getent, "passwd", key).Output()
	if err != nil {
		return ""
	}
	return shellFromPasswd(string(out), target)
}

func lookupDarwinAccountShell(ctx context.Context, username string) string {
	username = strings.TrimSpace(username)
	if username == "" {
		return ""
	}
	dscl := firstExistingPath("/usr/bin/dscl", "/bin/dscl")
	if dscl == "" {
		return ""
	}
	commandCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// /Search includes local, mobile, and directory-backed accounts. Using the
	// local "." node would incorrectly reject valid enterprise login users.
	out, err := accountCommand(commandCtx, dscl, "/Search", "-read", "/Users/"+username, "UserShell").Output()
	if err != nil {
		return ""
	}
	return shellFromDSCLOutput(string(out))
}

func shellFromDSCLOutput(output string) string {
	line := strings.TrimSpace(output)
	_, value, ok := strings.Cut(line, ":")
	if !ok {
		return ""
	}
	value = strings.TrimSpace(value)
	if filepath.IsAbs(value) {
		return value
	}
	return ""
}

func shellFromPasswd(data string, target TargetIdentity) string {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Split(line, ":")
		if len(fields) < 7 {
			continue
		}
		uid, err := strconv.ParseUint(strings.TrimSpace(fields[2]), 10, 32)
		if err != nil || uint32(uid) != target.UID {
			continue
		}
		if target.Username != "" && fields[0] != target.Username {
			continue
		}
		if target.Home != "" && !sameAccountHome(fields[5], target.Home) {
			continue
		}
		shell := strings.TrimSpace(fields[6])
		if filepath.IsAbs(shell) {
			return shell
		}
	}
	return ""
}

func sameAccountHome(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" || !filepath.IsAbs(left) || !filepath.IsAbs(right) {
		return false
	}
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if left == right {
		return true
	}
	leftInfo, leftErr := accountStat(left)
	rightInfo, rightErr := accountStat(right)
	return leftErr == nil && rightErr == nil && os.SameFile(leftInfo, rightInfo)
}

func firstExistingPath(paths ...string) string {
	for _, path := range paths {
		if _, err := accountStat(path); err == nil {
			return path
		}
	}
	return ""
}

func adapterForShell(shell string) (shellAdapter, error) {
	base := strings.ToLower(filepath.Base(shell))
	switch base {
	case "bash", "zsh", "ksh", "ksh93", "mksh":
		return shellAdapter{name: base, args: func(script string) []string { return []string{"-lic", script} }}, nil
	case "sh", "dash", "ash":
		return shellAdapter{name: base, args: func(script string) []string { return []string{"-lc", script} }}, nil
	case "fish":
		return shellAdapter{name: base, args: func(script string) []string { return []string{"--login", "--interactive", "--command", script} }}, nil
	case "tcsh", "csh":
		return shellAdapter{name: base, args: func(string) []string { return []string{"-l"} }, scriptOnStdin: true}, nil
	case "pwsh", "powershell":
		return shellAdapter{name: base, args: func(script string) []string { return []string{"-Login", "-NoLogo", "-Command", script} }}, nil
	case "nu", "nushell":
		return shellAdapter{name: "nu", args: func(script string) []string { return []string{"--login", "--commands", script} }}, nil
	default:
		return shellAdapter{}, fmt.Errorf("unsupported account shell %q; configure teamsCodexPath.shellOverride or use explicit/service mode", shell)
	}
}

func probeScript(adapter shellAdapter, helper string, nonce string, socketPath string) string {
	if adapter.name == "pwsh" || adapter.name == "powershell" {
		return "& " + quotePowerShell(helper) + " __user-path-probe --socket " + quotePowerShell(socketPath) + " --nonce " + quotePowerShell(nonce)
	}
	if adapter.name == "nu" {
		return "run-external " + quoteNu(helper) + " \"__user-path-probe\" \"--socket\" " + quoteNu(socketPath) + " \"--nonce\" " + quoteNu(nonce)
	}
	return quotePOSIX(helper) + " __user-path-probe --socket " + quotePOSIX(socketPath) + " --nonce " + quotePOSIX(nonce)
}

func quotePOSIX(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func quotePowerShell(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func quoteNu(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return `"` + replacer.Replace(value) + `"`
}

func runShellProbe(ctx context.Context, req Request, target TargetIdentity, shell string, adapter shellAdapter, environment []string) (payload probePayload, stdoutText string, stderrText string, resultErr error) {
	listener, socketPath, cleanupSocket, err := newProbeSocket(target)
	if err != nil {
		return probePayload{}, "", "", fmt.Errorf("create protocol socket: %w", err)
	}
	defer cleanupSocket()

	nonceBytes := make([]byte, probeNonceBytes)
	if _, err := rand.Read(nonceBytes); err != nil {
		return probePayload{}, "", "", fmt.Errorf("create protocol nonce: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)
	probeCtx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()
	script := probeScript(adapter, req.HelperExecutable, nonce, socketPath)
	cmd := accountCommand(probeCtx, shell, adapter.args(script)...)
	if adapter.scriptOnStdin {
		cmd.Stdin = strings.NewReader(script + "\nexit\n")
	}
	cmd.Env = append([]string(nil), environment...)
	cmd.Dir = target.Home
	output, err := newProbeOutputCapture(32 * 1024)
	if err != nil {
		return probePayload{}, "", "", fmt.Errorf("create diagnostic pipes: %w", err)
	}
	defer func() {
		output.finish()
		stdoutText = output.stdout.String()
		stderrText = output.stderr.String()
	}()
	cmd.Stdout = output.stdoutWriter
	cmd.Stderr = output.stderrWriter
	if req.ConfigureCommand != nil {
		if err := req.ConfigureCommand(cmd); err != nil {
			return probePayload{}, "", "", fmt.Errorf("configure target identity: %w", err)
		}
	}
	configureProbeProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		return probePayload{}, "", "", fmt.Errorf("start account shell: %w", err)
	}
	output.started()
	stopKill := killProbeGroupOnContext(probeCtx, cmd)
	defer stopKill()

	type readResult struct {
		payload probePayload
		err     error
	}
	readDone := make(chan readResult, 1)
	go func() {
		connection, acceptErr := listener.AcceptUnix()
		if acceptErr != nil {
			readDone <- readResult{err: fmt.Errorf("accept user PATH probe: %w", acceptErr)}
			return
		}
		defer connection.Close()
		payload, readErr := readProbePayload(connection, nonce)
		readDone <- readResult{payload: payload, err: readErr}
	}()
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

	var result readResult
	select {
	case result = <-readDone:
	case <-probeCtx.Done():
		killProbeProcessGroup(cmd)
		<-waitDone
		return probePayload{}, "", "", probeCtx.Err()
	}
	if result.err != nil {
		killProbeProcessGroup(cmd)
		waitErr := <-waitDone
		return probePayload{}, "", "", errors.Join(result.err, waitErr)
	}
	select {
	case waitErr := <-waitDone:
		if waitErr != nil {
			return probePayload{}, "", "", fmt.Errorf("account shell exited after probe: %w", waitErr)
		}
	case <-probeCtx.Done():
		killProbeProcessGroup(cmd)
		<-waitDone
		return probePayload{}, "", "", probeCtx.Err()
	}
	// Do not signal the process group after Wait has reaped its leader. At that
	// point the numeric PGID can be reused, and successful shell startup may also
	// have intentionally launched unrelated background processes.
	if result.payload.UID != target.UID || result.payload.EUID != target.UID || result.payload.GID != target.GID {
		return probePayload{}, "", "", fmt.Errorf("probe identity uid/euid/gid=%d/%d/%d, expected %d/%d/%d", result.payload.UID, result.payload.EUID, result.payload.GID, target.UID, target.UID, target.GID)
	}
	if target.GroupsKnown && !sameSupplementaryGroups(result.payload.Groups, target.Groups, target.GID) {
		return probePayload{}, "", "", fmt.Errorf("probe supplementary groups=%v, expected %v", normalizedSupplementaryGroups(result.payload.Groups, target.GID), normalizedSupplementaryGroups(target.Groups, target.GID))
	}
	if !sameAccountHome(result.payload.Home, target.Home) {
		return probePayload{}, "", "", fmt.Errorf("probe HOME %q, expected %q", result.payload.Home, target.Home)
	}
	return result.payload, "", "", nil
}

func newProbeSocket(target TargetIdentity) (*net.UnixListener, string, func(), error) {
	// Use the shared Unix temporary root rather than TMPDIR. A root-started
	// service may have a private TMPDIR that remains untraversable after the
	// leaf directory is handed to the target account.
	directory, err := os.MkdirTemp("/tmp", "cxp-user-path-")
	if err != nil {
		return nil, "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	if err := os.Chmod(directory, 0o700); err != nil {
		cleanup()
		return nil, "", nil, err
	}
	if os.Geteuid() == 0 {
		if err := os.Chown(directory, int(target.UID), int(target.GID)); err != nil {
			cleanup()
			return nil, "", nil, fmt.Errorf("assign probe directory to uid/gid %d/%d: %w", target.UID, target.GID, err)
		}
	} else if uint32(os.Geteuid()) != target.UID {
		cleanup()
		return nil, "", nil, fmt.Errorf("cannot create probe socket for uid %d while running as uid %d", target.UID, os.Geteuid())
	}
	socketPath := filepath.Join(directory, "probe.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		cleanup()
		return nil, "", nil, err
	}
	cleanupAll := func() {
		_ = listener.Close()
		cleanup()
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		cleanupAll()
		return nil, "", nil, err
	}
	if os.Geteuid() == 0 {
		if err := os.Chown(socketPath, int(target.UID), int(target.GID)); err != nil {
			cleanupAll()
			return nil, "", nil, fmt.Errorf("assign probe socket to uid/gid %d/%d: %w", target.UID, target.GID, err)
		}
	}
	return listener, socketPath, cleanupAll, nil
}

// probeOutputCapture deliberately gives os/exec real files instead of Go
// writers. When a shell startup file launches a background process that keeps
// stdout or stderr open, Cmd.Wait must still be allowed to return as soon as
// the account shell exits. The bounded readers are closed after a short drain
// window so inherited descriptors cannot turn a successful PATH probe into a
// timeout.
type probeOutputCapture struct {
	stdoutReader *os.File
	stdoutWriter *os.File
	stderrReader *os.File
	stderrWriter *os.File
	stdout       *limitedBuffer
	stderr       *limitedBuffer
	done         chan struct{}
	startOnce    sync.Once
	finishOnce   sync.Once
}

func newProbeOutputCapture(limit int) (*probeOutputCapture, error) {
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		return nil, err
	}
	return &probeOutputCapture{
		stdoutReader: stdoutReader,
		stdoutWriter: stdoutWriter,
		stderrReader: stderrReader,
		stderrWriter: stderrWriter,
		stdout:       &limitedBuffer{limit: limit},
		stderr:       &limitedBuffer{limit: limit},
		done:         make(chan struct{}),
	}, nil
}

func (capture *probeOutputCapture) started() {
	capture.startOnce.Do(func() {
		var readers sync.WaitGroup
		readers.Add(2)
		go func() {
			defer readers.Done()
			_, _ = io.Copy(capture.stdout, capture.stdoutReader)
		}()
		go func() {
			defer readers.Done()
			_, _ = io.Copy(capture.stderr, capture.stderrReader)
		}()
		go func() {
			readers.Wait()
			close(capture.done)
		}()
		_ = capture.stdoutWriter.Close()
		_ = capture.stderrWriter.Close()
	})
}

func (capture *probeOutputCapture) finish() {
	capture.finishOnce.Do(func() {
		capture.started()
		select {
		case <-capture.done:
		case <-time.After(25 * time.Millisecond):
			_ = capture.stdoutReader.Close()
			_ = capture.stderrReader.Close()
			<-capture.done
		}
		_ = capture.stdoutReader.Close()
		_ = capture.stderrReader.Close()
	})
}

func sameSupplementaryGroups(actual []uint32, expected []uint32, primary uint32) bool {
	left := normalizedSupplementaryGroups(actual, primary)
	right := normalizedSupplementaryGroups(expected, primary)
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func normalizedSupplementaryGroups(groups []uint32, primary uint32) []uint32 {
	seen := make(map[uint32]struct{}, len(groups))
	out := make([]uint32, 0, len(groups))
	for _, group := range groups {
		if group == primary {
			continue
		}
		if _, ok := seen[group]; ok {
			continue
		}
		seen[group] = struct{}{}
		out = append(out, group)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

type limitedBuffer struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	original := len(p)
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = b.buf.Write(p)
	}
	return original, nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func formatProbeOutput(stdout, stderr string) string {
	stdout = strings.TrimSpace(stdout)
	stderr = strings.TrimSpace(stderr)
	if stdout == "" && stderr == "" {
		return ""
	}
	var parts []string
	if stdout != "" {
		parts = append(parts, "stdout="+strconv.Quote(stdout))
	}
	if stderr != "" {
		parts = append(parts, "stderr="+strconv.Quote(stderr))
	}
	return "; " + strings.Join(parts, " ")
}

var _ io.Writer = (*limitedBuffer)(nil)
