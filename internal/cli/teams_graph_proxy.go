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
	Client   *http.Client
	ProxyURL string
	Mode     string
	close    func(context.Context) error
}

func (l teamsGraphHTTPClientLease) Close(ctx context.Context) error {
	if l.close == nil {
		return nil
	}
	return l.close(ctx)
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
	if lease, ok, err := reusableTeamsGraphProxyLease(cfg, profile.ID, hc); ok || err != nil {
		return lease, err
	}
	if fresh, err := store.Load(); err == nil {
		if lease, ok, err := reusableTeamsGraphProxyLease(fresh, profile.ID, hc); ok || err != nil {
			return lease, err
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
			if lease, ok, leaseErr := reusableTeamsGraphProxyLease(fresh, profile.ID, hc); ok || leaseErr != nil {
				return lease, leaseErr
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
	return teamsGraphHTTPClientLease{Client: client, ProxyURL: proxyURL, Mode: "proxy-stack", close: st.Close}, nil
}

func reusableTeamsGraphProxyLease(cfg config.Config, profileID string, hc manager.HealthClient) (teamsGraphHTTPClientLease, bool, error) {
	inst := manager.FindReusableInstance(cfg.Instances, profileID, hc)
	if inst == nil {
		return teamsGraphHTTPClientLease{}, false, nil
	}
	proxyURL := fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort)
	client, err := newTeamsProxyHTTPClient(proxyURL)
	if err != nil {
		return teamsGraphHTTPClientLease{}, false, err
	}
	return teamsGraphHTTPClientLease{Client: client, ProxyURL: proxyURL, Mode: "proxy-reuse"}, true, nil
}

func teamsGraphProxyEnabled(cfg config.Config) bool {
	if cfg.ProxyEnabled != nil {
		return *cfg.ProxyEnabled
	}
	return len(cfg.Profiles) > 0
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
