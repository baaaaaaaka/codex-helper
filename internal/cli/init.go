package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	internalssh "github.com/baaaaaaaka/codex-helper/internal/ssh"
)

func newInitCmd(root *rootOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create an SSH profile",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, _, err := newRootStore(root, "")
			if err != nil {
				return err
			}

			prof, err := initProfileInteractiveFunc(cmd.Context(), store)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Saved profile %q (%s)\n", prof.Name, prof.ID)
			return nil
		},
	}
	return cmd
}

type sshOps interface {
	probe(ctx context.Context, prof config.Profile, interactive bool, stdin io.Reader) error
	generateKeypair(ctx context.Context, store *config.Store, prof config.Profile) (string, error)
	installPublicKey(ctx context.Context, prof config.Profile, pubKeyPath string) error
}

type defaultSSHOps struct{}

type sshProbeFailureKind int

const (
	sshProbeFailureOther sshProbeFailureKind = iota
	sshProbeFailureAuth
	sshProbeFailureHostKey
)

type sshProbeError struct {
	kind   sshProbeFailureKind
	err    error
	output string
}

type sshTunnel interface {
	Start() error
	Stop(grace time.Duration) error
	Done() <-chan struct{}
	Wait() error
}

var newSSHTunnel = func(cfg internalssh.TunnelConfig) (sshTunnel, error) {
	return internalssh.NewTunnel(cfg)
}

var sshConfigPathForInit = defaultSSHConfigPathForInit
var sshConfigCurrentUserName = defaultSSHConfigCurrentUserName
var resolveSSHConfigProfileForInit = defaultResolveSSHConfigProfileForInit

var interactiveSSHProfileSetupAllowed = func() bool {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SERVICE")) != "" {
		return false
	}
	return isTerminalFile(os.Stdin)
}

func (e *sshProbeError) Error() string {
	if e.output == "" {
		return e.err.Error()
	}
	return fmt.Sprintf("%v: %s", e.err, e.output)
}

func (e *sshProbeError) Unwrap() error { return e.err }

func (defaultSSHOps) probe(ctx context.Context, prof config.Profile, interactive bool, stdin io.Reader) error {
	return sshProbe(ctx, prof, interactive, stdin)
}

func (defaultSSHOps) generateKeypair(ctx context.Context, store *config.Store, prof config.Profile) (string, error) {
	return generateKeypair(ctx, store, prof)
}

func (defaultSSHOps) installPublicKey(ctx context.Context, prof config.Profile, pubKeyPath string) error {
	return installPublicKey(ctx, prof, pubKeyPath)
}

func initProfileInteractive(ctx context.Context, store *config.Store) (config.Profile, error) {
	if !interactiveSSHProfileSetupAllowed() {
		return config.Profile{}, fmt.Errorf("interactive SSH profile setup requires a terminal; run `codex-proxy init` from an interactive shell before enabling proxy mode")
	}
	return initProfileInteractiveWithDeps(ctx, store, bufio.NewReader(os.Stdin), defaultSSHOps{}, os.Stderr)
}

func initProfileInteractiveWithDeps(
	ctx context.Context,
	store *config.Store,
	reader *bufio.Reader,
	ops sshOps,
	out io.Writer,
) (config.Profile, error) {
	if out != nil {
		_, _ = fmt.Fprintln(out, "Proxy mode uses an SSH tunnel to reach Codex through your network.")
		_, _ = fmt.Fprintln(out, "You can use an existing SSH config host or enter SSH host, port, and username manually.")
	}

	if prof, ok, err := initProfileFromSSHConfig(ctx, store, reader, ops, out); err != nil {
		return config.Profile{}, err
	} else if ok {
		return prof, nil
	}

	host, err := promptRequiredForInit(reader, "SSH host (required)")
	if err != nil {
		return config.Profile{}, err
	}
	port, err := promptIntForInit(reader, "SSH port", 22)
	if err != nil {
		return config.Profile{}, err
	}
	user, err := promptRequiredForInit(reader, "SSH user (required)")
	if err != nil {
		return config.Profile{}, err
	}

	id, err := ids.New()
	if err != nil {
		return config.Profile{}, err
	}

	name := user + "@" + host
	prof := config.Profile{
		ID:        id,
		Name:      name,
		Host:      host,
		Port:      port,
		User:      user,
		CreatedAt: time.Now(),
	}

	if err := initialSSHProbe(ctx, ops, prof); err != nil {
		if !shouldInstallManagedKey(err) {
			return config.Profile{}, err
		}
		if out != nil {
			_, _ = fmt.Fprintln(out, "Direct SSH access failed; creating a dedicated codex-proxy key and installing it.")
		}
		keyPath, err := ops.generateKeypair(ctx, store, prof)
		if err != nil {
			return config.Profile{}, err
		}
		if err := ops.installPublicKey(ctx, prof, keyPath+".pub"); err != nil {
			return config.Profile{}, err
		}
		prof.SSHArgs = []string{"-i", keyPath}

		if err := ops.probe(ctx, prof, false, nil); err != nil {
			return config.Profile{}, fmt.Errorf("key-based ssh probe failed: %w", err)
		}
	}

	if err := store.Update(func(cfg *config.Config) error {
		cfg.UpsertProfile(prof)
		return nil
	}); err != nil {
		return config.Profile{}, err
	}

	return prof, nil
}

type sshConfigProfile struct {
	Alias      string
	User       string
	Port       int
	ConfigPath string
}

type sshConfigProfileOptions struct {
	User    string
	Port    int
	UserSet bool
	PortSet bool
}

type sshConfigHostBlock struct {
	Patterns []string
	Options  sshConfigProfileOptions
}

func initProfileFromSSHConfig(
	ctx context.Context,
	store *config.Store,
	reader *bufio.Reader,
	ops sshOps,
	out io.Writer,
) (config.Profile, bool, error) {
	configPath, err := sshConfigPathForInit()
	if err != nil || strings.TrimSpace(configPath) == "" {
		return config.Profile{}, false, nil
	}
	profiles, err := readSSHConfigProfiles(configPath)
	if err != nil {
		if out != nil {
			_, _ = fmt.Fprintf(out, "Could not read SSH config %s: %v\n", configPath, err)
		}
		return config.Profile{}, false, nil
	}
	if len(profiles) == 0 {
		return config.Profile{}, false, nil
	}
	if out != nil {
		_, _ = fmt.Fprintf(out, "Found %d SSH config host entries in %s.\n", len(profiles), configPath)
	}
	useConfig, err := promptYesNoForInit(reader, "Use an existing SSH config host?", true)
	if err != nil {
		return config.Profile{}, false, err
	}
	if !useConfig {
		return config.Profile{}, false, nil
	}
	for {
		selected, manual, err := promptSSHConfigProfile(reader, profiles, out)
		if err != nil {
			return config.Profile{}, false, err
		}
		if manual {
			return config.Profile{}, false, nil
		}
		if resolved, err := resolveSSHConfigProfileForInit(selected); err == nil {
			selected = resolved
		}
		prof, err := profileFromSSHConfigProfile(selected)
		if err != nil {
			return config.Profile{}, false, err
		}
		if err := ops.probe(ctx, prof, false, nil); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return config.Profile{}, false, err
			}
			if out != nil {
				_, _ = fmt.Fprintf(out, "SSH config host %q is not reachable: %v\n", selected.Alias, err)
				_, _ = fmt.Fprintln(out, "Choose another SSH config host, or choose 0 to enter SSH details manually.")
			}
			continue
		}
		if err := store.Update(func(cfg *config.Config) error {
			cfg.UpsertProfile(prof)
			return nil
		}); err != nil {
			return config.Profile{}, false, err
		}
		return prof, true, nil
	}
}

func defaultSSHConfigPathForInit() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", err
	}
	return filepath.Join(home, ".ssh", "config"), nil
}

func readSSHConfigProfiles(path string) ([]sshConfigProfile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseSSHConfigProfiles(string(raw), path), nil
}

func defaultResolveSSHConfigProfileForInit(prof sshConfigProfile) (sshConfigProfile, error) {
	if strings.TrimSpace(prof.ConfigPath) == "" || strings.TrimSpace(prof.Alias) == "" {
		return prof, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ssh", "-G", "-F", prof.ConfigPath, prof.Alias).Output()
	if ctx.Err() != nil {
		return prof, ctx.Err()
	}
	if err != nil {
		return prof, err
	}
	resolved := prof
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}
		switch strings.ToLower(fields[0]) {
		case "user":
			if fields[1] != "" {
				resolved.User = fields[1]
			}
		case "port":
			port, err := strconv.Atoi(fields[1])
			if err == nil && port > 0 && port <= 65535 {
				resolved.Port = port
			}
		}
	}
	return resolved, nil
}

func parseSSHConfigProfiles(text string, path string) []sshConfigProfile {
	var blocks []sshConfigHostBlock
	global := sshConfigProfileOptions{}
	var current *sshConfigHostBlock
	beforeFirstSection := true

	commit := func() {
		if current == nil {
			return
		}
		blocks = append(blocks, *current)
		current = nil
	}

	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := strings.TrimSpace(stripSSHConfigComment(scanner.Text()))
		if line == "" {
			continue
		}
		fields := sshConfigLineFields(line)
		if len(fields) == 0 {
			continue
		}
		key := strings.ToLower(fields[0])
		values := fields[1:]
		switch key {
		case "host":
			commit()
			current = &sshConfigHostBlock{Patterns: append([]string{}, values...)}
			beforeFirstSection = false
		case "match":
			commit()
			beforeFirstSection = false
		case "user":
			if len(values) == 0 {
				continue
			}
			if current != nil && !current.Options.UserSet {
				current.Options.User = strings.Trim(values[0], `"`)
				current.Options.UserSet = current.Options.User != ""
			} else if current == nil && beforeFirstSection && !global.UserSet {
				global.User = strings.Trim(values[0], `"`)
				global.UserSet = global.User != ""
			}
		case "port":
			if len(values) == 0 {
				continue
			}
			port, err := strconv.Atoi(strings.Trim(values[0], `"`))
			if err != nil || port <= 0 || port > 65535 {
				continue
			}
			if current != nil && !current.Options.PortSet {
				current.Options.Port = port
				current.Options.PortSet = true
			} else if current == nil && beforeFirstSection && !global.PortSet {
				global.Port = port
				global.PortSet = true
			}
		}
	}
	commit()
	var profiles []sshConfigProfile
	for _, alias := range sshConfigConcreteHostAliases(blocks) {
		options := effectiveSSHConfigOptions(alias, global, blocks)
		user := strings.TrimSpace(options.User)
		if user == "" {
			user = sshConfigCurrentUserName()
		}
		port := options.Port
		if port <= 0 {
			port = 22
		}
		profiles = append(profiles, sshConfigProfile{
			Alias:      alias,
			User:       user,
			Port:       port,
			ConfigPath: path,
		})
	}
	return dedupeSSHConfigProfiles(profiles)
}

func sshConfigConcreteHostAliases(blocks []sshConfigHostBlock) []string {
	var aliases []string
	for _, block := range blocks {
		for _, pattern := range block.Patterns {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" || sshConfigHostPatternIgnored(pattern) {
				continue
			}
			if !sshConfigHostBlockMatches(pattern, block.Patterns) {
				continue
			}
			aliases = append(aliases, pattern)
		}
	}
	return aliases
}

func sshConfigLineFields(line string) []string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}
	key, value, ok := strings.Cut(fields[0], "=")
	if !ok {
		return fields
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" {
		return fields
	}
	out := []string{key}
	if value != "" {
		out = append(out, value)
	}
	return append(out, fields[1:]...)
}

func effectiveSSHConfigOptions(alias string, global sshConfigProfileOptions, blocks []sshConfigHostBlock) sshConfigProfileOptions {
	out := sshConfigProfileOptions{}
	if global.UserSet {
		out.User = global.User
		out.UserSet = true
	}
	if global.PortSet {
		out.Port = global.Port
		out.PortSet = true
	}
	for _, block := range blocks {
		if !sshConfigHostBlockMatches(alias, block.Patterns) {
			continue
		}
		if !out.UserSet && block.Options.UserSet {
			out.User = block.Options.User
			out.UserSet = true
		}
		if !out.PortSet && block.Options.PortSet {
			out.Port = block.Options.Port
			out.PortSet = true
		}
	}
	return out
}

func sshConfigHostBlockMatches(alias string, patterns []string) bool {
	matched := false
	for _, pattern := range patterns {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		negated := strings.HasPrefix(pattern, "!")
		if negated {
			pattern = strings.TrimPrefix(pattern, "!")
		}
		if sshConfigHostPatternMatches(pattern, alias) {
			if negated {
				return false
			}
			matched = true
		}
	}
	return matched
}

func sshConfigHostPatternMatches(pattern string, alias string) bool {
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	alias = strings.ToLower(strings.TrimSpace(alias))
	if pattern == alias {
		return true
	}
	if strings.ContainsAny(pattern, "*?") {
		ok, err := path.Match(pattern, alias)
		return err == nil && ok
	}
	return false
}

func stripSSHConfigComment(line string) string {
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "#") {
		return ""
	}
	if idx := strings.Index(line, " #"); idx >= 0 {
		return line[:idx]
	}
	return line
}

func sshConfigHostPatternIgnored(pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	return strings.HasPrefix(pattern, "!") ||
		strings.ContainsAny(pattern, "*?") ||
		strings.EqualFold(pattern, "none")
}

func dedupeSSHConfigProfiles(in []sshConfigProfile) []sshConfigProfile {
	seen := map[string]bool{}
	out := make([]sshConfigProfile, 0, len(in))
	for _, prof := range in {
		key := strings.ToLower(strings.TrimSpace(prof.Alias))
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, prof)
	}
	return out
}

func defaultSSHConfigCurrentUserName() string {
	for _, key := range []string{"USER", "USERNAME"} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return "user"
}

func promptSSHConfigProfile(reader *bufio.Reader, profiles []sshConfigProfile, out io.Writer) (sshConfigProfile, bool, error) {
	for {
		if out != nil {
			_, _ = fmt.Fprintln(out, "SSH config hosts:")
			_, _ = fmt.Fprintln(out, "  0) Enter SSH details manually")
			for i, prof := range profiles {
				_, _ = fmt.Fprintf(out, "  %d) %s (%s@%s:%d)\n", i+1, prof.Alias, prof.User, prof.Alias, prof.Port)
			}
		}
		choice, err := promptForInit(reader, "Choose SSH config host", "1")
		if err != nil {
			return sshConfigProfile{}, false, err
		}
		choice = strings.TrimSpace(choice)
		switch strings.ToLower(choice) {
		case "m", "manual":
			return sshConfigProfile{}, true, nil
		}
		n, err := strconv.Atoi(choice)
		if err != nil || n < 0 || n > len(profiles) {
			if out != nil {
				_, _ = fmt.Fprintf(out, "Enter a number from 0 to %d, or manual.\n", len(profiles))
			}
			continue
		}
		if n == 0 {
			return sshConfigProfile{}, true, nil
		}
		return profiles[n-1], false, nil
	}
}

func profileFromSSHConfigProfile(selected sshConfigProfile) (config.Profile, error) {
	id, err := ids.New()
	if err != nil {
		return config.Profile{}, err
	}
	alias := strings.TrimSpace(selected.Alias)
	user := strings.TrimSpace(selected.User)
	if user == "" {
		user = sshConfigCurrentUserName()
	}
	port := selected.Port
	if port <= 0 {
		port = 22
	}
	args := []string{}
	if path := strings.TrimSpace(selected.ConfigPath); path != "" {
		args = append(args, "-F", path)
	}
	return config.Profile{
		ID:        id,
		Name:      alias,
		Host:      alias,
		Port:      port,
		User:      user,
		SSHArgs:   args,
		CreatedAt: time.Now(),
	}, nil
}

func initialSSHProbe(
	ctx context.Context,
	ops sshOps,
	prof config.Profile,
) error {
	return ops.probe(ctx, prof, false, nil)
}

func prompt(r *bufio.Reader, label, def string) string {
	for {
		if def != "" {
			_, _ = fmt.Fprintf(os.Stderr, "%s [%s]: ", label, def)
		} else {
			_, _ = fmt.Fprintf(os.Stderr, "%s: ", label)
		}
		s, _ := r.ReadString('\n')
		s = strings.TrimSpace(s)
		if s == "" {
			return def
		}
		return s
	}
}

func promptRequired(r *bufio.Reader, label string) string {
	for {
		v := prompt(r, label, "")
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
}

func promptInt(r *bufio.Reader, label string, def int) int {
	for {
		v := prompt(r, label, strconv.Itoa(def))
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err == nil && n > 0 && n <= 65535 {
			return n
		}
	}
}

func promptYesNo(r *bufio.Reader, label string, def bool) bool {
	defStr := "n"
	if def {
		defStr = "y"
	}

	for {
		s := prompt(r, label+" (y/n)", defStr)
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "y", "yes":
			return true
		case "n", "no":
			return false
		default:
		}
	}
}

func promptForInit(r *bufio.Reader, label, def string) (string, error) {
	if def != "" {
		_, _ = fmt.Fprintf(os.Stderr, "%s [%s]: ", label, def)
	} else {
		_, _ = fmt.Fprintf(os.Stderr, "%s: ", label)
	}
	s, err := r.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read %s: %w", label, err)
	}
	s = strings.TrimSpace(s)
	if s == "" {
		if errors.Is(err, io.EOF) {
			return "", fmt.Errorf("input ended while reading %s", label)
		}
		return def, nil
	}
	return s, nil
}

func promptRequiredForInit(r *bufio.Reader, label string) (string, error) {
	for {
		v, err := promptForInit(r, label, "")
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(v) != "" {
			return v, nil
		}
	}
}

func promptIntForInit(r *bufio.Reader, label string, def int) (int, error) {
	for {
		v, err := promptForInit(r, label, strconv.Itoa(def))
		if err != nil {
			return 0, err
		}
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err == nil && n > 0 && n <= 65535 {
			return n, nil
		}
	}
}

func promptYesNoForInit(r *bufio.Reader, label string, def bool) (bool, error) {
	defStr := "n"
	if def {
		defStr = "y"
	}

	for {
		s, err := promptForInit(r, label+" (y/n)", defStr)
		if err != nil {
			return false, err
		}
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "y", "yes":
			return true, nil
		case "n", "no":
			return false, nil
		default:
		}
	}
}

func sshProbe(ctx context.Context, prof config.Profile, interactive bool, stdin io.Reader) error {
	_ = stdin
	if interactive {
		return errors.New("interactive SSH host-key checks are no longer used; retry the non-interactive probe")
	}

	probePort, err := pickFreeTCPPort()
	if err != nil {
		return err
	}

	var out bytes.Buffer
	tun, err := newSSHTunnel(internalssh.TunnelConfig{
		Host:         prof.Host,
		Port:         prof.Port,
		User:         prof.User,
		SocksPort:    probePort,
		ExtraArgs:    prof.SSHArgs,
		ConfigTarget: internalssh.ArgsUseConfigFile(prof.SSHArgs),
		BatchMode:    true,
		Stdout:       &out,
		Stderr:       &out,
	})
	if err != nil {
		return newSSHProbeError(err, out.String())
	}
	if err := tun.Start(); err != nil {
		return newSSHProbeError(err, out.String())
	}
	needStop := true
	defer func() {
		if needStop {
			_ = tun.Stop(500 * time.Millisecond)
		}
	}()

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(probePort))
	if err := waitForSSHProbeReady(ctx, addr, 10*time.Second, tun); err != nil {
		needStop = false
		_ = tun.Stop(500 * time.Millisecond)
		return newSSHProbeError(err, out.String())
	}
	return nil
}

func pickFreeTCPPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(portStr)
}

func waitForSSHProbeReady(ctx context.Context, addr string, timeout time.Duration, tun sshTunnel) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tun.Done():
			return fmt.Errorf("ssh tunnel exited before SOCKS ready: %w", tun.Wait())
		default:
		}

		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tun.Done():
			return fmt.Errorf("ssh tunnel exited before SOCKS ready: %w", tun.Wait())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for ssh SOCKS listener on %s: %w", addr, lastErr)
	}
	return fmt.Errorf("timeout waiting for ssh SOCKS listener on %s", addr)
}

func newSSHProbeError(err error, output string) error {
	if err == nil {
		return nil
	}
	output = strings.TrimSpace(output)
	return &sshProbeError{
		kind:   classifySSHProbeFailure(output),
		err:    err,
		output: output,
	}
}

func classifySSHProbeFailure(output string) sshProbeFailureKind {
	output = strings.ToLower(strings.TrimSpace(output))
	if output == "" {
		return sshProbeFailureOther
	}

	hostKeyHints := []string{
		"host key verification failed",
		"remote host identification has changed",
		"no host key is known for",
	}
	for _, hint := range hostKeyHints {
		if strings.Contains(output, hint) {
			return sshProbeFailureHostKey
		}
	}

	authHints := []string{
		"permission denied",
		"authentication failed",
		"no more authentication methods available",
		"no supported authentication methods available",
		"too many authentication failures",
		"sign_and_send_pubkey",
	}
	for _, hint := range authHints {
		if strings.Contains(output, hint) {
			return sshProbeFailureAuth
		}
	}
	return sshProbeFailureOther
}

func shouldInstallManagedKey(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var probeErr *sshProbeError
	return errors.As(err, &probeErr) && probeErr.kind == sshProbeFailureAuth
}

func generateKeypair(ctx context.Context, store *config.Store, prof config.Profile) (string, error) {
	dir := filepath.Dir(store.Path())
	keyDir := filepath.Join(dir, "keys")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return "", err
	}

	keyPath, err := nextAvailableKeyPath(filepath.Join(keyDir, "id_ed25519_"+prof.ID))
	if err != nil {
		return "", err
	}

	args := []string{
		"-t", "ed25519",
		"-f", keyPath,
		"-N", "",
		"-C", "codex-proxy " + prof.Name,
	}
	c := exec.CommandContext(ctx, "ssh-keygen", args...)
	c.Stdin = os.Stdin
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return "", err
	}
	return keyPath, nil
}

func nextAvailableKeyPath(base string) (string, error) {
	path := base
	for i := 0; ; i++ {
		_, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return path, nil
			}
			return "", err
		}
		path = fmt.Sprintf("%s_%d", base, i+1)
	}
}

func installPublicKey(ctx context.Context, prof config.Profile, pubKeyPath string) error {
	pub, err := os.ReadFile(pubKeyPath)
	if err != nil {
		return err
	}
	if !bytes.HasSuffix(pub, []byte("\n")) {
		pub = append(pub, '\n')
	}

	args := []string{
		"-p", strconv.Itoa(prof.Port),
	}
	hostKeyArgs, err := internalssh.HostKeyArgsForTarget(prof.Host, prof.Port, prof.SSHArgs)
	if err != nil {
		return err
	}
	args = append(args, hostKeyArgs...)
	args = append(args, prof.SSHArgs...)
	args = append(args,
		prof.User+"@"+prof.Host,
		"umask 077; mkdir -p ~/.ssh; cat >> ~/.ssh/authorized_keys",
	)
	c := exec.CommandContext(ctx, "ssh", args...)
	c.Stdin = bytes.NewReader(pub)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}
