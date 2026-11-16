package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
)

type teamsReadTestTokens struct{}

func (teamsReadTestTokens) Token(context.Context) (string, error)           { return "read", nil }
func (teamsReadTestTokens) Refresh(context.Context, string) (string, error) { return "read", nil }
func TestTeamsReadCLICommands(t *testing.T) {
	lockCLITestHooks(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer read" {
			t.Error("wrong token")
		}
		if r.Method == "POST" {
			fmt.Fprint(w, `{"value":[{"hitsContainers":[{"hits":[{"summary":"match","resource":{"id":"1","chatId":"chat"}}],"moreResultsAvailable":false}]}]}`)
			return
		}
		fmt.Fprint(w, `{"id":"1","createdDateTime":"2026-11-01T00:00:01Z","lastModifiedDateTime":"2026-11-01T00:00:01Z","body":{"contentType":"html","content":"<a href='https://example.com'>说明</a>"}}`)
	}))
	defer srv.Close()
	previous := openTeamsReader
	t.Cleanup(func() { openTeamsReader = previous })
	openTeamsReader = func(context.Context, *rootOptions) (*teamsReadSession, error) {
		r, err := chteams.NewReader(chteams.Config{GraphURL: srv.URL + "/v1.0", Tokens: teamsReadTestTokens{}, TenantID: "tenant"})
		return &teamsReadSession{Reader: r}, err
	}
	for _, args := range [][]string{{"teams", "read", "https://teams.microsoft.com/l/message/chat/1", "--json"}, {"teams", "search", "word", "--json"}, {"teams", "read", "https://teams.microsoft.com/l/message/chat/1"}} {
		root := newRootCmd()
		root.SetArgs(args)
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetErr(io.Discard)
		if e := root.Execute(); e != nil {
			t.Fatal(e)
		}
		if args[len(args)-1] == "--json" {
			var v map[string]any
			if e := json.Unmarshal(out.Bytes(), &v); e != nil || v["version"] != float64(1) {
				t.Fatalf("invalid JSON %s %v", out.String(), e)
			}
		} else if !strings.Contains(out.String(), "说明: https://example.com") {
			t.Fatal("text output lost hyperlinks")
		}
	}
	for _, args := range [][]string{{"teams", "read"}, {"teams", "read", "https://teams.microsoft.com/l/chat/chat/0", "--before", "2"}, {"teams", "read", "https://teams.microsoft.com/l/message/chat/1", "--before", "0"}, {"teams", "read", "https://teams.microsoft.com/l/message/chat/1", "--limit", "5"}, {"teams", "search"}, {"teams", "fetch", "invalid", "--output", "unused"}, {"teams", "read", "--cursor", "invalid", "--latest", "1"}} {
		before := calls.Load()
		root := newRootCmd()
		root.SetArgs(args)
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		if e := root.Execute(); e == nil {
			t.Errorf("accepted %v", args)
		}
		if calls.Load() != before {
			t.Fatal("invalid flags performed IO")
		}
	}
}

// Exercise the production opener without starting SSH or touching user state.
func teamsReadLeaseFixture(t *testing.T) *config.Store {
	t.Helper()
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	for _, name := range []string{"CODEX_HELPER_TEAMS_FULL_CLIENT_ID", "CODEX_HELPER_TEAMS_FULL_TENANT_ID", "CODEX_HELPER_TEAMS_READ_TENANT_ID", "CODEX_HELPER_TEAMS_FULL_SCOPES", "CODEX_HELPER_TEAMS_READ_SCOPES"} {
		t.Setenv(name, "")
	}
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_CONFIG", filepath.Join(tmp, "auth.sqlite"))
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", filepath.Join(tmp, "full.sqlite"))
	t.Setenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", filepath.Join(tmp, "read.sqlite"))
	return newTeamsGraphProxyTestStore(t, config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(true), Profiles: []config.Profile{{ID: "p", Name: "profile", Host: "example.com", Port: 22, User: "alice", CreatedAt: time.Now()}}})
}

func TestTeamsReadPrivateLeaseLifetime(t *testing.T) {
	lockCLITestHooks(t)
	store := teamsReadLeaseFixture(t)
	before, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	previous := teamsGraphStackStart
	t.Cleanup(func() { teamsGraphStackStart = previous })
	var starts, closes int
	teamsGraphStackStart = func(ctx context.Context, p config.Profile, id string, _ stack.Options) (*stack.Stack, error) {
		if ctx.Err() != nil || p.ID != "p" || id == "" {
			t.Fatal("lost startup context/profile/identity")
		}
		starts++
		return stack.NewStackForTestWithCloseHook(10000+starts, 20000+starts, func() { closes++ }), nil
	}
	one, err := openTeamsReader(context.Background(), &rootOptions{configPath: store.Path()})
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	two, err := openTeamsReader(context.Background(), &rootOptions{configPath: store.Path()})
	if err != nil {
		t.Fatal(err)
	}
	defer two.Close()
	if starts != 2 || closes != 0 || one.lease.Mode != "proxy-stack" || one.lease.Client.Timeout != 0 {
		t.Fatalf("bad lease lifetime: starts=%d closes=%d timeout=%s", starts, closes, one.lease.Client.Timeout)
	}
	if err := one.Close(); err != nil {
		t.Fatal(err)
	}
	if closes != 1 {
		t.Fatalf("closing one session stopped %d stacks", closes)
	}
	if err := two.Close(); err != nil {
		t.Fatal(err)
	}
	if closes != 2 {
		t.Fatalf("temporary stack leaked: %d closes", closes)
	}
	after, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("read networking changed proxy configuration or instance records")
	}
}

func TestTeamsReadStartupFailureAndCancellation(t *testing.T) {
	lockCLITestHooks(t)
	store := teamsReadLeaseFixture(t)
	previous := teamsGraphStackStart
	t.Cleanup(func() { teamsGraphStackStart = previous })
	sentinel := errors.New("SSH unavailable")
	var starts int
	teamsGraphStackStart = func(context.Context, config.Profile, string, stack.Options) (*stack.Stack, error) {
		starts++
		return nil, sentinel
	}
	if _, err := openTeamsReader(context.Background(), &rootOptions{configPath: store.Path()}); !errors.Is(err, sentinel) {
		t.Fatalf("lost startup cause: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := openTeamsReader(ctx, &rootOptions{configPath: store.Path()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if starts != 1 {
		t.Fatal("cancelled request started a stack")
	}
}

func TestTeamsReadFailureClosesPrivateLeaseAndPreservesJSON(t *testing.T) {
	lockCLITestHooks(t)
	store := teamsReadLeaseFixture(t)
	previous := teamsGraphStackStart
	t.Cleanup(func() { teamsGraphStackStart = previous })
	var closed bool
	teamsGraphStackStart = func(context.Context, config.Profile, string, stack.Options) (*stack.Stack, error) {
		return stack.NewStackForTestWithCloseHook(12345, 23456, func() { closed = true }), nil
	}
	cmd := newRootCmd()
	cmd.SetArgs([]string{"--config", store.Path(), "teams", "read", "https://teams.microsoft.com/l/message/chat/1", "--json"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	// No token exists in this fixture. Failure must not leak the private stack.
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected missing read credential")
	}
	if !closed {
		t.Fatal("read failure leaked private stack")
	}
	var result map[string]any
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || result["version"] != float64(1) {
		t.Fatalf("polluted JSON: %q (%v)", out.String(), err)
	}
}
