package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/config"
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
	openTeamsReader = func(context.Context, *rootOptions) (*chteams.Reader, error) {
		return chteams.NewReader(chteams.Config{GraphURL: srv.URL + "/v1.0", Tokens: teamsReadTestTokens{}, TenantID: "tenant"})
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
func TestTeamsReadNetworkingHasNoLifecycle(t *testing.T) {
	lockCLITestHooks(t)
	// A configured proxy with no registered daemon is a read error; the
	// lifecycle lease would try to create a stack here.
	store := newTeamsGraphProxyTestStore(t, config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(true), Profiles: []config.Profile{{ID: "p", Name: "profile", Host: "example.com", Port: 22, User: "alice", CreatedAt: time.Now()}}})
	if _, e := newTeamsReadHTTPClient(context.Background(), &rootOptions{configPath: store.Path()}); e == nil || !strings.Contains(e.Error(), "no reusable proxy") {
		t.Fatalf("unexpected passive networking result %v", e)
	}
}
