package ssh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TunnelConfig struct {
	Host      string
	Port      int
	User      string
	SocksPort int

	// ExtraArgs are appended before the destination argument.
	ExtraArgs []string

	// ConfigTarget treats Host as an OpenSSH config alias and avoids overriding
	// the config file's User or Port with command-line destination flags.
	ConfigTarget bool

	// BatchMode enables non-interactive SSH behavior (recommended for tunnels).
	BatchMode bool

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// EffectiveEndpoint is the host and port OpenSSH selected after applying a
// config file and command-line options. It is used for bounded wake/network
// admission checks; an interface becoming UP is not sufficient evidence that
// this endpoint is reachable.
type EffectiveEndpoint struct {
	Host         string
	Port         int
	ProxyJump    string
	ProxyCommand string
}

// ResolveEffectiveEndpoint asks OpenSSH to resolve its own configuration. It
// intentionally does not make a network connection. Callers decide whether
// to probe the final endpoint or the first hop of a proxy route.
func ResolveEffectiveEndpoint(ctx context.Context, host string, args []string) (EffectiveEndpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return EffectiveEndpoint{}, errors.New("ssh config target is required")
	}
	cmdArgs := []string{"-G"}
	cmdArgs = append(cmdArgs, args...)
	cmdArgs = append(cmdArgs, host)
	cmd := exec.CommandContext(ctx, "ssh", cmdArgs...)
	out, err := cmd.Output()
	if err != nil {
		if ctx.Err() != nil {
			return EffectiveEndpoint{}, ctx.Err()
		}
		return EffectiveEndpoint{}, fmt.Errorf("ssh -G %s: %w", host, err)
	}
	endpoint := EffectiveEndpoint{Host: host, Port: 22}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		switch strings.ToLower(fields[0]) {
		case "hostname":
			if strings.TrimSpace(fields[1]) != "" {
				endpoint.Host = strings.Trim(strings.TrimSpace(fields[1]), "[]")
			}
		case "port":
			port, parseErr := strconv.Atoi(fields[1])
			if parseErr == nil && port > 0 && port <= 65535 {
				endpoint.Port = port
			}
		case "proxyjump":
			endpoint.ProxyJump = strings.TrimSpace(strings.Join(fields[1:], " "))
		case "proxycommand":
			endpoint.ProxyCommand = strings.TrimSpace(strings.Join(fields[1:], " "))
		}
	}
	if endpoint.Host == "" || endpoint.Port <= 0 {
		return EffectiveEndpoint{}, fmt.Errorf("ssh -G %s returned no usable host/port", host)
	}
	return endpoint, nil
}

// FirstProxyHop returns the first host and port in a ProxyJump value. It is a
// local-only admission probe; the complete route is still validated by the
// real SSH tunnel before the backend is marked ready.
func FirstProxyHop(value string) (string, int, bool) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "none") {
		return "", 0, false
	}
	hop := strings.TrimSpace(strings.Split(value, ",")[0])
	if at := strings.LastIndexByte(hop, '@'); at >= 0 {
		hop = hop[at+1:]
	}
	port := 22
	if host, rawPort, err := net.SplitHostPort(hop); err == nil {
		hop = strings.Trim(host, "[]")
		if parsed, parseErr := strconv.Atoi(rawPort); parseErr == nil && parsed > 0 && parsed <= 65535 {
			port = parsed
		}
	} else if strings.Count(hop, ":") == 1 {
		parts := strings.SplitN(hop, ":", 2)
		if parsed, parseErr := strconv.Atoi(parts[1]); parseErr == nil && parsed > 0 && parsed <= 65535 {
			hop = parts[0]
			port = parsed
		}
	}
	hop = strings.TrimSpace(strings.Trim(hop, "[]"))
	return hop, port, hop != ""
}

// DefaultHostKeyArgs accepts first-seen SSH host keys without prompting while
// still rejecting changed host keys.
func DefaultHostKeyArgs() []string {
	return []string{"-o", "StrictHostKeyChecking=accept-new"}
}

func legacyStrictHostKeyArgs() []string {
	return []string{"-o", "StrictHostKeyChecking=yes"}
}

var (
	sshAcceptNewSupportOnce     sync.Once
	sshAcceptNewSupportValue    bool
	sshAcceptNewSupportDetector = detectSSHAcceptNewSupport
	sshKeygenFindHost           = defaultSSHKeygenFindHost
	runSSHKeyscan               = defaultRunSSHKeyscan
)

func HostKeyArgsForTarget(host string, port int, extraArgs []string) ([]string, error) {
	if err := validateHostKeyTarget(host, port); err != nil {
		return nil, err
	}
	return hostKeyArgsForTarget(host, port, extraArgs, false)
}

func hostKeyArgsForTarget(host string, port int, extraArgs []string, configTarget bool) ([]string, error) {
	if extraArgsSetStrictHostKeyChecking(extraArgs) {
		return nil, nil
	}
	if sshAcceptNewSupported() {
		return DefaultHostKeyArgs(), nil
	}
	if configTarget {
		return nil, nil
	}
	if err := ensureLegacyKnownHost(host, port, extraArgs); err != nil {
		return nil, err
	}
	return legacyStrictHostKeyArgs(), nil
}

func validateHostKeyTarget(host string, port int) error {
	if strings.TrimSpace(host) == "" {
		return errors.New("host is required")
	}
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid ssh port %d", port)
	}
	return nil
}

func sshAcceptNewSupported() bool {
	sshAcceptNewSupportOnce.Do(func() {
		sshAcceptNewSupportValue = sshAcceptNewSupportDetector()
	})
	return sshAcceptNewSupportValue
}

func detectSSHAcceptNewSupport() bool {
	if _, err := exec.LookPath("ssh"); err != nil {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh", "-G", "-o", "StrictHostKeyChecking=accept-new", "example.com")
	out, err := cmd.CombinedOutput()
	if err == nil {
		return true
	}
	text := strings.ToLower(string(out))
	return !(strings.Contains(text, "accept-new") ||
		strings.Contains(text, "bad configuration option") ||
		strings.Contains(text, "unsupported option") ||
		strings.Contains(text, "unknown option") ||
		strings.Contains(text, "invalid option") ||
		strings.Contains(text, "illegal option"))
}

func ensureLegacyKnownHost(host string, port int, extraArgs []string) error {
	knownHosts, err := userKnownHostsFile(extraArgs)
	if err != nil {
		return err
	}
	lookupHost := knownHostsLookupHost(host, port)
	if knownHostExists(knownHosts, lookupHost) {
		return nil
	}
	keys, err := scanHostKeys(host, port)
	if err != nil {
		return err
	}
	return appendKnownHosts(knownHosts, keys)
}

func knownHostExists(knownHosts, host string) bool {
	if knownHosts == "" {
		return false
	}
	if _, err := os.Stat(knownHosts); err != nil {
		return false
	}
	return sshKeygenFindHost(knownHosts, host)
}

func defaultSSHKeygenFindHost(knownHosts, host string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh-keygen", "-F", host, "-f", knownHosts)
	return cmd.Run() == nil
}

func scanHostKeys(host string, port int) (string, error) {
	args := []string{"-T", "5", "-H"}
	if port != 22 {
		args = append(args, "-p", strconv.Itoa(port))
	}
	args = append(args, host)
	out, err := runSSHKeyscan(args)
	if err != nil {
		args = []string{"-T", "5"}
		if port != 22 {
			args = append(args, "-p", strconv.Itoa(port))
		}
		args = append(args, host)
		out, err = runSSHKeyscan(args)
	}
	if err != nil {
		return "", err
	}
	text := strings.TrimSpace(string(out))
	if text == "" {
		return "", fmt.Errorf("ssh-keyscan returned no host keys for %s", knownHostsLookupHost(host, port))
	}
	return text + "\n", nil
}

func defaultRunSSHKeyscan(args []string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh-keyscan", args...)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return nil, fmt.Errorf("ssh-keyscan timed out")
	}
	if err != nil {
		return nil, fmt.Errorf("ssh-keyscan failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func appendKnownHosts(path, keys string) error {
	if path == "" {
		return errors.New("known_hosts path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create known_hosts directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open known_hosts: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(keys); err != nil {
		return fmt.Errorf("append known_hosts: %w", err)
	}
	return nil
}

func userKnownHostsFile(extraArgs []string) (string, error) {
	if path := userKnownHostsFileFromArgs(extraArgs); path != "" {
		if isDiscardKnownHostsFile(path) {
			return "", fmt.Errorf("OpenSSH client does not support StrictHostKeyChecking=accept-new and UserKnownHostsFile=%s cannot store the first host key", path)
		}
		return expandUserKnownHostsFile(path)
	}
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("OpenSSH client does not support StrictHostKeyChecking=accept-new and the user home directory is unavailable")
	}
	return filepath.Join(home, ".ssh", "known_hosts"), nil
}

func isDiscardKnownHostsFile(path string) bool {
	return strings.EqualFold(path, "none") ||
		strings.EqualFold(path, os.DevNull) ||
		path == "/dev/null"
}

func expandUserKnownHostsFile(path string) (string, error) {
	if path != "~" && !strings.HasPrefix(path, "~/") && !strings.HasPrefix(path, `~\`) {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("OpenSSH client does not support StrictHostKeyChecking=accept-new and UserKnownHostsFile=%s needs a user home directory", path)
	}
	if path == "~" {
		return home, nil
	}
	return filepath.Join(home, filepath.FromSlash(strings.TrimLeft(path[1:], `/\`))), nil
}

func userKnownHostsFileFromArgs(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-o" && i+1 < len(args) {
			if v, ok := sshOptionValue(args[i+1], "UserKnownHostsFile"); ok {
				return firstSSHPathValue(v)
			}
			continue
		}
		if strings.HasPrefix(arg, "-o") {
			if v, ok := sshOptionValue(strings.TrimPrefix(arg, "-o"), "UserKnownHostsFile"); ok {
				return firstSSHPathValue(v)
			}
		}
	}
	return ""
}

func extraArgsSetStrictHostKeyChecking(args []string) bool {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-o" && i+1 < len(args) {
			if _, ok := sshOptionValue(args[i+1], "StrictHostKeyChecking"); ok {
				return true
			}
			continue
		}
		if strings.HasPrefix(arg, "-o") {
			if _, ok := sshOptionValue(strings.TrimPrefix(arg, "-o"), "StrictHostKeyChecking"); ok {
				return true
			}
		}
	}
	return false
}

func sshOptionValue(arg, name string) (string, bool) {
	key, value, ok := strings.Cut(strings.TrimSpace(arg), "=")
	if !ok {
		return "", false
	}
	if !strings.EqualFold(strings.TrimSpace(key), name) {
		return "", false
	}
	return strings.TrimSpace(value), true
}

func firstSSHPathValue(value string) string {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return ""
	}
	return strings.Trim(fields[0], `"`)
}

func knownHostsLookupHost(host string, port int) string {
	host = strings.Trim(host, "[]")
	if port == 22 {
		return host
	}
	return "[" + host + "]:" + strconv.Itoa(port)
}

func (c TunnelConfig) destination() string {
	if c.ConfigTarget || c.User == "" {
		return c.Host
	}
	return c.User + "@" + c.Host
}

func ArgsUseConfigFile(args []string) bool {
	for i, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == "-F" && i+1 < len(args) && strings.TrimSpace(args[i+1]) != "" {
			return true
		}
		if strings.HasPrefix(arg, "-F") && len(arg) > 2 {
			return true
		}
	}
	return false
}

// ArgsUseProxyRoute reports whether ExtraArgs make the SSH destination
// reachable through another SSH route rather than by dialing the destination
// directly. A local host-network probe must not dial the final destination in
// that case: the destination may only be reachable from the jump host.
func ArgsUseProxyRoute(args []string) bool {
	for i, arg := range args {
		arg = strings.TrimSpace(arg)
		switch {
		case arg == "-J" || arg == "-W":
			return true
		case strings.HasPrefix(arg, "-J") || strings.HasPrefix(arg, "-W"):
			return len(arg) > 2
		case arg == "-o" && i+1 < len(args):
			if sshProxyOption(args[i+1]) {
				return true
			}
		case strings.HasPrefix(arg, "-o"):
			if sshProxyOption(strings.TrimPrefix(arg, "-o")) {
				return true
			}
		}
	}
	return false
}

func sshProxyOption(arg string) bool {
	for _, name := range []string{"ProxyJump", "ProxyCommand"} {
		if value, ok := sshOptionValue(strings.TrimSpace(arg), name); ok {
			return value != "" && !strings.EqualFold(value, "none")
		}
	}
	return false
}

func BuildArgs(c TunnelConfig) ([]string, error) {
	return buildArgs(c, DefaultHostKeyArgs())
}

func validateTunnelConfig(c TunnelConfig) error {
	if strings.TrimSpace(c.Host) == "" {
		return errors.New("host is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid ssh port %d", c.Port)
	}
	if c.SocksPort <= 0 || c.SocksPort > 65535 {
		return fmt.Errorf("invalid socks port %d", c.SocksPort)
	}
	return nil
}

func buildArgs(c TunnelConfig, hostKeyArgs []string) ([]string, error) {
	if err := validateTunnelConfig(c); err != nil {
		return nil, err
	}

	args := []string{
		"-N",
		"-o", "ExitOnForwardFailure=yes",
		"-o", "ConnectTimeout=15",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=3",
		"-o", "TCPKeepAlive=yes",
	}
	args = append(args, hostKeyArgs...)
	if !c.ConfigTarget {
		args = append(args, "-p", strconv.Itoa(c.Port))
	}
	args = append(args, "-D", "127.0.0.1:"+strconv.Itoa(c.SocksPort))

	if c.BatchMode {
		args = append(args, "-o", "BatchMode=yes")
	}

	args = append(args, c.ExtraArgs...)
	args = append(args, c.destination())
	return args, nil
}

type Tunnel struct {
	cfg TunnelConfig

	mu            sync.Mutex
	cmd           *exec.Cmd
	processHandle tunnelProcessHandle
	waitErr       error
	done          chan struct{}
}

func NewTunnel(cfg TunnelConfig) (*Tunnel, error) {
	if err := validateTunnelConfig(cfg); err != nil {
		return nil, err
	}
	hostKeyArgs, err := hostKeyArgsForTarget(cfg.Host, cfg.Port, cfg.ExtraArgs, cfg.ConfigTarget)
	if err != nil {
		return nil, err
	}
	args, err := buildArgs(cfg, hostKeyArgs)
	if err != nil {
		return nil, err
	}

	t := &Tunnel{
		cfg:  cfg,
		cmd:  exec.Command("ssh", args...),
		done: make(chan struct{}),
	}
	configureTunnelCommand(t.cmd)
	t.cmd.Stdin = cfg.Stdin
	t.cmd.Stdout = cfg.Stdout
	t.cmd.Stderr = cfg.Stderr
	return t, nil
}

func (t *Tunnel) PID() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cmd == nil || t.cmd.Process == nil {
		return 0
	}
	return t.cmd.Process.Pid
}

func (t *Tunnel) Start() error {
	t.mu.Lock()
	cmd := t.cmd
	t.mu.Unlock()

	if cmd == nil {
		return errors.New("tunnel not initialized")
	}

	if err := cmd.Start(); err != nil {
		return err
	}
	processHandle := attachTunnelProcess(cmd)
	t.mu.Lock()
	t.processHandle = processHandle
	t.mu.Unlock()

	go func() {
		err := cmd.Wait()
		t.mu.Lock()
		t.waitErr = err
		handle := t.processHandle
		t.processHandle = tunnelProcessHandle{}
		t.mu.Unlock()
		closeTunnelProcess(handle)
		close(t.done)
	}()

	return nil
}

func (t *Tunnel) Done() <-chan struct{} { return t.done }

func (t *Tunnel) Wait() error {
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.waitErr
}

func (t *Tunnel) Stop(grace time.Duration) error {
	t.mu.Lock()
	cmd := t.cmd
	processHandle := t.processHandle
	t.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}
	select {
	case <-t.done:
		return t.Wait()
	default:
	}

	_ = terminateTunnelProcess(cmd, processHandle, t.done, grace)
	<-t.done
	return t.Wait()
}
