package stack

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/localproxy"
	"github.com/baaaaaaaka/codex-helper/internal/ssh"
)

type Options struct {
	SocksPort      int
	HTTPListenAddr string

	SocksReadyTimeout time.Duration

	MaxRestarts     int
	RestartBackoff  time.Duration
	TunnelStopGrace time.Duration
}

type Stack struct {
	InstanceID string
	Profile    config.Profile

	SocksPort int
	HTTPAddr  string
	HTTPPort  int

	proxy  *localproxy.HTTPProxy
	tunnel *ssh.Tunnel

	fatalCh chan error
	stopCh  chan struct{}
}

// proxySetup holds the local HTTP proxy components created by setupHTTPProxy.
type proxySetup struct {
	proxy    *localproxy.HTTPProxy
	httpAddr string
	httpPort int
}

// setupHTTPProxy creates a SOCKS5 dialer pointing at socksAddr, builds an
// HTTP proxy on top of it, and starts listening on httpListenAddr.
func setupHTTPProxy(socksAddr, httpListenAddr, instanceID string) (*proxySetup, error) {
	dialer, err := localproxy.NewSOCKS5Dialer(socksAddr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	hp := localproxy.NewHTTPProxy(dialer, localproxy.Options{InstanceID: instanceID})
	httpAddr, err := hp.Start(httpListenAddr)
	if err != nil {
		return nil, err
	}
	_, portStr, err := net.SplitHostPort(httpAddr)
	if err != nil {
		_ = hp.Close(context.Background())
		return nil, err
	}
	httpPort, err := parsePort(portStr)
	if err != nil {
		_ = hp.Close(context.Background())
		return nil, err
	}
	return &proxySetup{proxy: hp, httpAddr: httpAddr, httpPort: httpPort}, nil
}

// reservePort allocates a TCP port on the loopback interface and returns the
// port number together with the held listener. The caller must close the
// listener to release the port (typically right before handing it to SSH).
func reservePort() (int, net.Listener, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, nil, err
	}
	return ln.Addr().(*net.TCPAddr).Port, ln, nil
}

func Start(profile config.Profile, instanceID string, opts Options) (*Stack, error) {
	if profile.Host == "" {
		return nil, errors.New("profile host is required")
	}
	if profile.Port <= 0 {
		return nil, errors.New("profile port is required")
	}
	if profile.User == "" {
		return nil, errors.New("profile user is required")
	}
	if instanceID == "" {
		return nil, errors.New("instance id is required")
	}

	if opts.HTTPListenAddr == "" {
		opts.HTTPListenAddr = "127.0.0.1:0"
	}
	if opts.MaxRestarts <= 0 {
		opts.MaxRestarts = 3
	}
	if opts.RestartBackoff <= 0 {
		opts.RestartBackoff = 1 * time.Second
	}
	if opts.TunnelStopGrace <= 0 {
		opts.TunnelStopGrace = 2 * time.Second
	}
	if opts.SocksReadyTimeout <= 0 {
		opts.SocksReadyTimeout = 30 * time.Second
	}

	// Reserve SOCKS port: hold the listener open so that the HTTP proxy
	// (which also binds :0) cannot accidentally grab the same port.
	socksPort := opts.SocksPort
	var socksReserve net.Listener
	if socksPort == 0 {
		port, ln, err := reservePort()
		if err != nil {
			return nil, fmt.Errorf("reserve socks port: %w", err)
		}
		socksPort = port
		socksReserve = ln
	}

	socksAddr := fmt.Sprintf("127.0.0.1:%d", socksPort)
	ps, err := setupHTTPProxy(socksAddr, opts.HTTPListenAddr, instanceID)
	if err != nil {
		if socksReserve != nil {
			socksReserve.Close()
		}
		return nil, err
	}

	// Only retry with new ports when the SOCKS port was auto-selected.
	// When an explicit port was requested, honour the caller's choice:
	// return exactly that port or an error.
	canRetry := opts.SocksPort == 0
	const maxPortRetries = 3

	// Release the reserved SOCKS port and immediately start the SSH tunnel.
	// If the tunnel fails to bind, retry with a freshly reserved port.
	var tun *ssh.Tunnel
	for attempt := 0; ; attempt++ {
		if socksReserve != nil {
			socksReserve.Close()
			socksReserve = nil
		}
		t, terr := newTunnel(profile, socksPort)
		if terr != nil {
			_ = ps.proxy.Close(context.Background())
			return nil, terr
		}
		if terr := t.Start(); terr != nil {
			_ = ps.proxy.Close(context.Background())
			return nil, terr
		}
		if terr := waitForTCPTunnel(socksAddr, opts.SocksReadyTimeout, t); terr != nil {
			_ = t.Stop(opts.TunnelStopGrace)
			if canRetry && attempt < maxPortRetries {
				// Reserve a new port (held open until the next iteration
				// releases it), then rebuild the HTTP proxy for the new
				// SOCKS address.
				port, ln, reserveErr := reservePort()
				if reserveErr == nil {
					socksPort = port
					socksReserve = ln
					socksAddr = fmt.Sprintf("127.0.0.1:%d", socksPort)
					_ = ps.proxy.Close(context.Background())
					newPs, psErr := setupHTTPProxy(socksAddr, opts.HTTPListenAddr, instanceID)
					if psErr != nil {
						socksReserve.Close()
						return nil, psErr
					}
					ps = newPs
					continue
				}
			}
			_ = ps.proxy.Close(context.Background())
			return nil, terr
		}
		tun = t
		break
	}

	s := &Stack{
		InstanceID: instanceID,
		Profile:    profile,
		SocksPort:  socksPort,
		HTTPAddr:   ps.httpAddr,
		HTTPPort:   ps.httpPort,
		proxy:      ps.proxy,
		tunnel:     tun,
		fatalCh:    make(chan error, 1),
		stopCh:     make(chan struct{}),
	}

	go s.monitor(opts)
	return s, nil
}

func (s *Stack) HTTPProxyURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", s.HTTPPort)
}

func (s *Stack) Fatal() <-chan error { return s.fatalCh }

func (s *Stack) Close(ctx context.Context) error {
	select {
	case <-s.stopCh:
		// already closed
	default:
		close(s.stopCh)
	}

	var firstErr error
	if s.tunnel != nil {
		if err := s.tunnel.Stop(2 * time.Second); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.proxy != nil {
		if err := s.proxy.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Stack) monitor(opts Options) {
	restarts := 0
	for {
		err := s.tunnel.Wait()

		select {
		case <-s.stopCh:
			return
		default:
		}

		restarts++
		if restarts > opts.MaxRestarts {
			s.fatalCh <- fmt.Errorf("ssh tunnel exited too many times: %w", err)
			return
		}

		time.Sleep(opts.RestartBackoff)

		// Reconnect using the same SOCKS port. If the port is now occupied
		// by another process the tunnel will fail and we report fatal rather
		// than switching ports, because changing the SOCKS address would
		// require rebuilding the dialer and HTTP proxy. Port conflicts during
		// reconnect are rare; when they occur the caller should recreate the
		// entire stack.
		tun, terr := newTunnel(s.Profile, s.SocksPort)
		if terr != nil {
			s.fatalCh <- terr
			return
		}
		if terr := tun.Start(); terr != nil {
			s.fatalCh <- terr
			return
		}
		if terr := waitForTCPTunnel(fmt.Sprintf("127.0.0.1:%d", s.SocksPort), opts.SocksReadyTimeout, tun); terr != nil {
			_ = tun.Stop(opts.TunnelStopGrace)
			s.fatalCh <- terr
			return
		}

		s.tunnel = tun
		restarts = 0
	}
}

func newTunnel(profile config.Profile, socksPort int) (*ssh.Tunnel, error) {
	return ssh.NewTunnel(ssh.TunnelConfig{
		Host:      profile.Host,
		Port:      profile.Port,
		User:      profile.User,
		SocksPort: socksPort,
		ExtraArgs: profile.SSHArgs,
		BatchMode: true,
		Stdout:    os.Stderr,
		Stderr:    os.Stderr,
	})
}

func waitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for %s: %w", addr, lastErr)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

func waitForTCPTunnel(addr string, timeout time.Duration, tun *ssh.Tunnel) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if tun != nil {
			select {
			case <-tun.Done():
				return fmt.Errorf("ssh tunnel exited before SOCKS ready: %w", tun.Wait())
			default:
			}
		}

		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for %s: %w", addr, lastErr)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

func pickFreePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return 0, err
	}
	return parsePort(portStr)
}

func parsePort(s string) (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:"+s)
	if err != nil {
		return 0, err
	}
	return addr.Port, nil
}
