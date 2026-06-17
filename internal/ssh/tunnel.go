package ssh

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	mu      sync.Mutex
	cmd     *exec.Cmd
	waitErr error
	done    chan struct{}
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

	go func() {
		err := cmd.Wait()
		t.mu.Lock()
		t.waitErr = err
		t.mu.Unlock()
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
	t.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}

	_ = cmd.Process.Signal(os.Interrupt)

	select {
	case <-t.done:
		return t.Wait()
	case <-time.After(grace):
		_ = cmd.Process.Kill()
		<-t.done
		return t.Wait()
	}
}
