package manager

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

type HealthClient struct {
	Timeout time.Duration
}

type healthResponse struct {
	OK         bool   `json:"ok"`
	InstanceID string `json:"instanceId"`
}

func (c HealthClient) CheckHTTPProxy(port int, expectedInstanceID string) error {
	return c.CheckHTTPProxyContext(context.Background(), port, expectedInstanceID)
}

func (c HealthClient) CheckHTTPProxyContext(ctx context.Context, port int, expectedInstanceID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid http port %d", port)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 1 * time.Second
	}

	tr := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout: timeout,
		}).DialContext,
		DisableKeepAlives: true,
	}
	defer tr.CloseIdleConnections()

	client := &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/_codex_proxy/health", port), nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}

	var hr healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&hr); err != nil {
		return err
	}
	if !hr.OK {
		return errors.New("health check not ok")
	}
	if expectedInstanceID != "" && hr.InstanceID != expectedInstanceID {
		return fmt.Errorf("unexpected instance id %q", hr.InstanceID)
	}
	return nil
}

func (c HealthClient) CheckHTTPProxyCONNECT(port int, target string) error {
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid http port %d", port)
	}
	target, err := cleanConnectTarget(target)
	if err != nil {
		return err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 1 * time.Second
	}

	conn, err := (&net.Dialer{Timeout: timeout}).Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Connection: close\r\nConnection: close\r\n\r\n", target, target); err != nil {
		return err
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("CONNECT %s returned %s", target, resp.Status)
	}
	return nil
}

func cleanConnectTarget(target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", errors.New("CONNECT target is required")
	}
	if strings.ContainsAny(target, "\r\n") {
		return "", errors.New("CONNECT target contains a newline")
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return "", fmt.Errorf("invalid CONNECT target %q: %w", target, err)
	}
	if strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return "", fmt.Errorf("invalid CONNECT target %q", target)
	}
	return net.JoinHostPort(host, port), nil
}
