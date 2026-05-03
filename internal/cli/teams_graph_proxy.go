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
	hc := manager.HealthClient{Timeout: time.Second}
	if inst := manager.FindReusableInstance(cfg.Instances, profile.ID, hc); inst != nil {
		proxyURL := fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort)
		client, err := newTeamsProxyHTTPClient(proxyURL)
		if err != nil {
			return teamsGraphHTTPClientLease{}, err
		}
		return teamsGraphHTTPClientLease{Client: client, ProxyURL: proxyURL, Mode: "proxy-reuse"}, nil
	}
	if fresh, err := store.Load(); err == nil {
		if inst := manager.FindReusableInstance(fresh.Instances, profile.ID, hc); inst != nil {
			proxyURL := fmt.Sprintf("http://127.0.0.1:%d", inst.HTTPPort)
			client, err := newTeamsProxyHTTPClient(proxyURL)
			if err != nil {
				return teamsGraphHTTPClientLease{}, err
			}
			return teamsGraphHTTPClientLease{Client: client, ProxyURL: proxyURL, Mode: "proxy-reuse"}, nil
		}
	}
	instanceID, err := ids.New()
	if err != nil {
		return teamsGraphHTTPClientLease{}, err
	}
	st, err := stackStart(profile, instanceID, stack.Options{})
	if err != nil {
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

func teamsGraphProxyEnabled(cfg config.Config) bool {
	if cfg.ProxyEnabled != nil {
		return *cfg.ProxyEnabled
	}
	return len(cfg.Profiles) > 0
}

func newTeamsDirectHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}
}

func newTeamsProxyHTTPClient(proxyURL string) (*http.Client, error) {
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(parsed),
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}, nil
}
