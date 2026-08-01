package appgateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Health is intentionally more permissive than the legacy reusable-instance
// check: a live Frontend may return 503 while its backend waits for DNS/VPN,
// and that is precisely the state in which starting a second Gateway would be
// harmful.
type Health struct {
	OK         bool   `json:"ok"`
	InstanceID string `json:"instanceId"`
	Error      string `json:"error,omitempty"`
	Recovery   string `json:"recovery,omitempty"`
}

func Probe(ctx context.Context, port int, timeout time.Duration) (Health, error) {
	if port < 1 || port > 65535 {
		return Health{}, fmt.Errorf("invalid app gateway port %d", port)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/_codex_proxy/health", port), nil)
	if err != nil {
		return Health{}, err
	}
	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	client.Transport = &http.Transport{Proxy: nil}
	defer client.Transport.(*http.Transport).CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return Health{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		return Health{}, fmt.Errorf("unexpected app gateway health status %s", resp.Status)
	}
	var health Health
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return Health{}, err
	}
	if strings.TrimSpace(health.InstanceID) == "" {
		return Health{}, fmt.Errorf("app gateway health response has no instance id (status %s)", resp.Status)
	}
	return health, nil
}
