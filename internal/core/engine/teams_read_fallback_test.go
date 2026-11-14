package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/channel/teams"
)

// Exercise the real adapter's HTTP routing and the real engine's admission
// and account gate together. Fake owns only the surrounding chat lifecycle
// and message fixture; no helper process or external service is started.
type teamsFallbackChannel struct {
	*channeltest.Fake
	adapter *teams.Adapter
}

func (c *teamsFallbackChannel) Receive(ctx context.Context, stream, cursor string) (channel.Window, error) {
	if _, err := c.adapter.Receive(ctx, stream, ""); err != nil {
		return channel.Window{}, err
	}
	return c.Fake.Receive(ctx, stream, cursor)
}

type teamsFallbackTokens string

func (t teamsFallbackTokens) Token(context.Context) (string, error)           { return string(t), nil }
func (t teamsFallbackTokens) Refresh(context.Context, string) (string, error) { return string(t), nil }

type teamsFallbackTransport func(*http.Request) (*http.Response, error)

func (f teamsFallbackTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func teamsFallbackToken(app string) teamsFallbackTokens {
	b, _ := json.Marshal(map[string]string{"tid": "tenant", "oid": "owner", "appid": app})
	return teamsFallbackTokens("e30." + base64.RawURLEncoding.EncodeToString(b) + ".sig")
}

func TestTeamsReadFallbackEngineAdmissionAndAccountGate(t *testing.T) {
	for _, both := range []bool{false, true} {
		name := "primary-only"
		if both {
			name = "both"
		}
		t.Run(name, func(t *testing.T) {
			var armed atomic.Bool
			var attempts, primary, full atomic.Int32
			readToken, fullToken := teamsFallbackToken("read"), teamsFallbackToken("full")
			adapter, err := teams.New(teams.Config{ReadTokens: readToken, Tokens: fullToken, ReadFallback: true, HTTP: &http.Client{Transport: teamsFallbackTransport(func(req *http.Request) (*http.Response, error) {
				status, body := 200, `{"value":[]}`
				if req.URL.Path == "/v1.0/me" {
					body = `{"id":"owner"}`
				} else if armed.Load() {
					attempts.Add(1)
					isRead := req.Header.Get("Authorization") == "Bearer "+string(readToken)
					if isRead {
						primary.Add(1)
					} else {
						full.Add(1)
					}
					if isRead || both {
						status = 429
					}
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"62"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}})
			if err != nil {
				t.Fatal(err)
			}
			r := newRig(t, func(cfg *Config) {
				cfg.Channel = &teamsFallbackChannel{Fake: cfg.Channel.(*channeltest.Fake), adapter: adapter}
			})
			work := r.newWork(t.TempDir())
			armed.Store(true)
			r.ch.PostUser(work, "fallback prompt")
			if both {
				r.wait("both credentials cause account cooldown", func() bool { return r.e.gate(r.e.gRead).After(time.Now()) })
				before := attempts.Load()
				time.Sleep(200 * time.Millisecond)
				if after := attempts.Load(); after != before {
					t.Fatalf("HTTP requests during cooldown: %d -> %d", before, after)
				}
				if r.ag.calls("fallback prompt") != 0 {
					t.Fatal("admitted a message after failed receive")
				}
			} else {
				r.wait("prompt admitted through alternate", func() bool { return r.ag.calls("fallback prompt") == 1 })
				if r.e.gate(r.e.gRead).After(time.Now()) {
					t.Fatal("successful alternate closed account gate")
				}
			}
			if primary.Load() == 0 || full.Load() == 0 {
				t.Fatalf("routing not exercised: primary=%d full=%d", primary.Load(), full.Load())
			}
		})
	}
}
