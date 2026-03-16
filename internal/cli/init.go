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
			store, err := config.NewStore(root.configPath)
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
	probe(ctx context.Context, prof config.Profile, interactive bool) error
	generateKeypair(ctx context.Context, store *config.Store, prof config.Profile) (string, error)
	installPublicKey(ctx context.Context, prof config.Profile, pubKeyPath string) error
}

type defaultSSHOps struct{}

type sshProbeFailureKind int

const (
	sshProbeFailureOther sshProbeFailureKind = iota
	sshProbeFailureAuth
)

type sshProbeError struct {
	kind   sshProbeFailureKind
	err    error
	output string
}

func (e *sshProbeError) Error() string {
	if e.output == "" {
		return e.err.Error()
	}
	return fmt.Sprintf("%v: %s", e.err, e.output)
}

func (e *sshProbeError) Unwrap() error { return e.err }

func (defaultSSHOps) probe(ctx context.Context, prof config.Profile, interactive bool) error {
	return sshProbe(ctx, prof, interactive)
}

func (defaultSSHOps) generateKeypair(ctx context.Context, store *config.Store, prof config.Profile) (string, error) {
	return generateKeypair(ctx, store, prof)
}

func (defaultSSHOps) installPublicKey(ctx context.Context, prof config.Profile, pubKeyPath string) error {
	return installPublicKey(ctx, prof, pubKeyPath)
}

func initProfileInteractive(ctx context.Context, store *config.Store) (config.Profile, error) {
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
		_, _ = fmt.Fprintln(out, "Enter your SSH host, port, and username to establish that tunnel.")
	}

	host := promptRequired(reader, "SSH host (required)")
	port := promptInt(reader, "SSH port", 22)
	user := promptRequired(reader, "SSH user (required)")

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

	if err := ops.probe(ctx, prof, false); err != nil {
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

		if err := ops.probe(ctx, prof, false); err != nil {
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

func sshProbe(ctx context.Context, prof config.Profile, interactive bool) error {
	dest := prof.User + "@" + prof.Host
	if interactive {
		args := []string{
			"-p", strconv.Itoa(prof.Port),
		}
		args = append(args, prof.SSHArgs...)
		args = append(args, dest)

		c := exec.CommandContext(ctx, "ssh", args...)
		c.Stdin = os.Stdin
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		return c.Run()
	}

	probePort, err := pickFreeTCPPort()
	if err != nil {
		return err
	}

	var out bytes.Buffer
	tun, err := internalssh.NewTunnel(internalssh.TunnelConfig{
		Host:      prof.Host,
		Port:      prof.Port,
		User:      prof.User,
		SocksPort: probePort,
		ExtraArgs: prof.SSHArgs,
		BatchMode: true,
		Stdout:    &out,
		Stderr:    &out,
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

func waitForSSHProbeReady(ctx context.Context, addr string, timeout time.Duration, tun *internalssh.Tunnel) error {
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
		prof.User + "@" + prof.Host,
		"umask 077; mkdir -p ~/.ssh; cat >> ~/.ssh/authorized_keys",
	}
	c := exec.CommandContext(ctx, "ssh", args...)
	c.Stdin = bytes.NewReader(pub)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}
