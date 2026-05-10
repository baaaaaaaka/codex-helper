package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
)

type teamsGraphHTTPClientLease struct {
	Client                 *http.Client
	ProxyURL               string
	Mode                   string
	close                  func(context.Context) error
	store                  *config.Store
	suspectReusableProxies []config.Instance
}

var teamsGraphProxyCONNECTTarget = "graph.microsoft.com:443"
var teamsGraphProxyRetireGrace = 2 * time.Second

func (l teamsGraphHTTPClientLease) Close(ctx context.Context) error {
	if l.close == nil {
		return nil
	}
	return l.close(ctx)
}

func (l teamsGraphHTTPClientLease) RetireSuspects(ctx context.Context, out io.Writer) {
	if l.store == nil || len(l.suspectReusableProxies) == 0 {
		return
	}
	hc := manager.HealthClient{Timeout: 3 * time.Second}
	seen := make(map[string]struct{}, len(l.suspectReusableProxies))
	for _, inst := range l.suspectReusableProxies {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return
			}
		}
		if inst.ID == "" {
			continue
		}
		if _, ok := seen[inst.ID]; ok {
			continue
		}
		seen[inst.ID] = struct{}{}
		if err := retireTeamsGraphProxySuspect(ctx, l.store, inst, hc, out); err != nil && out != nil {
			_, _ = fmt.Fprintf(out, "Teams Graph proxy: failed to retire stale reusable proxy %s: %v\n", inst.ID, err)
		}
	}
}

func newTeamsGraphHTTPClientLease(ctx context.Context, root *rootOptions, out io.Writer) (teamsGraphHTTPClientLease, error) {
	store, _, err := newRootStore(root, "")
	if err != nil {
		return teamsGraphHTTPClientLease{}, err
	}
	cfg, err := store.Load()
	if err != nil {
		return teamsGraphHTTPClientLease{}, err
	}
	if !teamsGraphProxyEnabled(cfg) {
		return teamsGraphHTTPClientLease{Client: newTeamsDirectHTTPClient(), Mode: "direct"}, nil
	}
	profile, err := selectProfile(cfg, "")
	if err != nil {
		return teamsGraphHTTPClientLease{}, fmt.Errorf("Teams Graph proxy is enabled but no unambiguous proxy profile is configured: %w", err)
	}
	hc := manager.HealthClient{Timeout: 3 * time.Second}
	var suspects []config.Instance
	if lease, suspect, ok, err := reusableTeamsGraphProxyLease(cfg, profile.ID, hc); ok || err != nil {
		return lease, err
	} else {
		suspects = appendTeamsGraphProxySuspect(suspects, suspect)
	}
	if fresh, err := store.Load(); err == nil {
		if lease, suspect, ok, err := reusableTeamsGraphProxyLease(fresh, profile.ID, hc); ok || err != nil {
			return lease, err
		} else {
			suspects = appendTeamsGraphProxySuspect(suspects, suspect)
		}
	}
	instanceID, err := ids.New()
	if err != nil {
		return teamsGraphHTTPClientLease{}, err
	}
	st, err := stackStart(profile, instanceID, stack.Options{})
	if err != nil {
		// A reusable proxy can become healthy while we are trying to start a
		// fallback stack, especially on loaded machines. Re-check once before
		// surfacing the fallback start error.
		if fresh, loadErr := store.Load(); loadErr == nil {
			if lease, suspect, ok, leaseErr := reusableTeamsGraphProxyLease(fresh, profile.ID, hc); ok || leaseErr != nil {
				return lease, leaseErr
			} else {
				suspects = appendTeamsGraphProxySuspect(suspects, suspect)
			}
		}
		return teamsGraphHTTPClientLease{}, fmt.Errorf("start Teams Graph proxy stack: %w", err)
	}
	proxyURL := st.HTTPProxyURL()
	client, err := newTeamsProxyHTTPClient(proxyURL)
	if err != nil {
		_ = st.Close(context.Background())
		return teamsGraphHTTPClientLease{}, err
	}
	if out != nil {
		_, _ = fmt.Fprintf(out, "Teams Graph proxy: using %s\n", proxyURL)
	}
	return teamsGraphHTTPClientLease{Client: client, ProxyURL: proxyURL, Mode: "proxy-stack", close: st.Close, store: store, suspectReusableProxies: suspects}, nil
}

func reusableTeamsGraphProxyLease(cfg config.Config, profileID string, hc manager.HealthClient) (teamsGraphHTTPClientLease, *config.Instance, bool, error) {
	inst := manager.FindReusableInstance(cfg.Instances, profileID, hc)
	if inst == nil {
		return teamsGraphHTTPClientLease{}, nil, false, nil
	}
	if err := hc.CheckHTTPProxyCONNECT(inst.HTTPPort, teamsGraphProxyCONNECTTarget); err != nil {
		suspect := *inst
		return teamsGraphHTTPClientLease{}, &suspect, false, nil
	}
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort)
	client, err := newTeamsProxyHTTPClient(proxyURL)
	if err != nil {
		return teamsGraphHTTPClientLease{}, nil, false, err
	}
	return teamsGraphHTTPClientLease{Client: client, ProxyURL: proxyURL, Mode: "proxy-reuse"}, nil, true, nil
}

func teamsGraphProxyEnabled(cfg config.Config) bool {
	if cfg.ProxyEnabled != nil {
		return *cfg.ProxyEnabled
	}
	return len(cfg.Profiles) > 0
}

func appendTeamsGraphProxySuspect(suspects []config.Instance, suspect *config.Instance) []config.Instance {
	if suspect == nil || suspect.ID == "" {
		return suspects
	}
	for _, existing := range suspects {
		if existing.ID == suspect.ID {
			return suspects
		}
	}
	return append(suspects, *suspect)
}

func retireTeamsGraphProxySuspect(ctx context.Context, store *config.Store, inst config.Instance, hc manager.HealthClient, out io.Writer) error {
	if store == nil || inst.ID == "" {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	remove := func() error {
		return proxyRemoveInstance(store, inst.ID)
	}
	if inst.DaemonPID <= 0 || !proxyProcessAlive(inst.DaemonPID) {
		if err := remove(); err != nil {
			return err
		}
		if out != nil {
			_, _ = fmt.Fprintf(out, "Teams Graph proxy: unregistered stale reusable proxy %s\n", inst.ID)
		}
		return nil
	}
	looksLike, err := proxyLooksLikeProxyDaemon(inst.DaemonPID)
	if err != nil || !looksLike {
		if removeErr := remove(); removeErr != nil {
			return removeErr
		}
		if out != nil {
			if err != nil {
				_, _ = fmt.Fprintf(out, "Teams Graph proxy: unregistered stale reusable proxy %s (pid %d could not be verified: %v)\n", inst.ID, inst.DaemonPID, err)
			} else {
				_, _ = fmt.Fprintf(out, "Teams Graph proxy: unregistered stale reusable proxy %s (pid %d is not a proxy daemon)\n", inst.ID, inst.DaemonPID)
			}
		}
		return nil
	}
	if err := proxyCheckHTTPProxy(hc, inst.HTTPPort, inst.ID); err != nil {
		if removeErr := remove(); removeErr != nil {
			return removeErr
		}
		if out != nil {
			_, _ = fmt.Fprintf(out, "Teams Graph proxy: unregistered stale reusable proxy %s (health check changed: %v)\n", inst.ID, err)
		}
		return nil
	}
	process, err := proxyFindProcess(inst.DaemonPID)
	if err != nil {
		return err
	}
	if err := proxyTerminate(process, teamsGraphProxyRetireGrace); err != nil {
		return err
	}
	if err := remove(); err != nil {
		return err
	}
	if out != nil {
		_, _ = fmt.Fprintf(out, "Teams Graph proxy: retired stale reusable proxy %s (pid %d)\n", inst.ID, inst.DaemonPID)
	}
	return nil
}

func newTeamsDirectHTTPClient() *http.Client {
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: newTeamsHTTPTransport(nil),
	}
}

func newTeamsProxyHTTPClient(proxyURL string) (*http.Client, error) {
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: newTeamsHTTPTransport(http.ProxyURL(parsed)),
	}, nil
}

func newTeamsHTTPTransport(proxy func(*http.Request) (*url.URL, error)) *http.Transport {
	return &http.Transport{
		Proxy: proxy,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}
