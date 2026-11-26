package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/codexagent"
	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

type teamsRecoveryTestLog struct{ t *testing.T }

func (l teamsRecoveryTestLog) Write(p []byte) (int, error) {
	l.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// The real engine runs on a durable outbox under one lease while its
// production adapter and auth source use a recoverable HTTP route.
func TestTeamsGraphRecoveryResumesPollAndDurableOutbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dir := t.TempDir()
	anchor := filepath.Join(dir, "core.sqlite")
	host := "teams-graph-recovery-test"
	boot := teamsBootID()
	holder, err := lease.Acquire(ctx, lease.Config{Anchor: anchor, Self: lease.Local(host, boot)})
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()
	st, err := openTeamsCoreStore(ctx, holder, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Acquire(corestore.Ident{Host: host, BootID: boot, PID: os.Getpid()}); err != nil {
		t.Fatal(err)
	}
	defer st.Release()

	graph := fakegraph.New(fakegraph.Options{})
	defer graph.Close()
	const chat = "19:recovery@thread.v2"
	const body = "durable output through the recovered route"
	graph.SeedChat(chat, "Control")
	if err := st.Update(func(tx *corestore.Tx) error {
		id, err := tx.CreateConversation(corestore.NewConversation{Label: "control", Kind: corestore.KindControl, Channel: "teams", ExtChatID: chat})
		if err != nil {
			return err
		}
		_, err = tx.Enqueue(id, []corestore.OutMsg{{DeliveryKey: "graph-recovery-queued", Kind: corestore.OutNotice, Body: fmt.Sprintf(`{"body":{"contentType":"html","content":"%s"}}`, body)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	login := chteams.AuthConfig{TenantID: fakegraph.TenantID, ClientID: "fixture", Scopes: chteams.DefaultScopes,
		Cache: chteams.TokenLocation{Database: filepath.Join(dir, "tokens.sqlite"), Slot: "full"}}
	_, refresh := graph.IssueTokens("before-outage")
	if err := chteams.WriteTokenCache(login.Cache, chteams.TokenCache{RefreshToken: refresh}); err != nil {
		t.Fatal(err)
	}
	address, err := url.Parse(graph.URL())
	if err != nil {
		t.Fatal(err)
	}
	var failures, refreshes atomic.Int32
	newRoute := func(broken bool) teamsGraphHTTPClientLease {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		client := &http.Client{Transport: teamsGraphRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			isOAuth := req.URL.Hostname() == "login.microsoftonline.com"
			if broken && isOAuth {
				failures.Add(1)
				return nil, teamsGraphConnectError{status: http.StatusBadGateway}
			}
			if isOAuth {
				refreshes.Add(1)
			}
			out := req.Clone(req.Context())
			out.Header.Set(fakegraph.HeaderOriginalHost, req.URL.Hostname())
			out.URL.Scheme, out.URL.Host = address.Scheme, address.Host
			return transport.RoundTrip(out)
		})}
		return teamsGraphHTTPClientLease{Client: client, close: func(context.Context) error {
			transport.CloseIdleConnections()
			return nil
		}}
	}
	allowReplacement := make(chan struct{})
	recovery := newTeamsGraphRecovery(ctx, newRoute(true), func(ctx context.Context) (teamsGraphHTTPClientLease, error) {
		select {
		case <-ctx.Done():
			return teamsGraphHTTPClientLease{}, ctx.Err()
		case <-allowReplacement:
			return newRoute(false), nil
		}
	}, func(ctx context.Context, candidate *http.Client, _ teamsGraphRequestKind) error {
		return teamsGraphRecoveryProbe(ctx, teamsFencedHTTPClient(candidate, st.Fence), login)
	}, time.Now, 100*time.Millisecond, teamsRecoveryTestLog{t})
	defer recovery.close()

	ch, err := chteams.New(chteams.Config{
		HTTP:   teamsFencedHTTPClient(recovery.client(teamsGraphRead, 2*time.Second), st.Fence),
		Tokens: chteams.NewDelegatedAuth(login, teamsFencedHTTPClient(recovery.client(teamsGraphFullAuth, 2*time.Second), st.Fence)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	e, err := engine.New(engine.Config{
		Store: st, Channel: ch, Agent: codexagent.New(codexagent.Config{PIDFile: filepath.Join(dir, "agents.sqlite")}),
		Timing: engine.FastTiming(100 * time.Millisecond), ActivityScan: true, Host: host, Log: teamsRecoveryTestLog{t},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("engine stopped with %v", err)
		}
	}()

	await := func(check func() bool, reason string) {
		t.Helper()
		for !check() {
			if err := sleepContext(ctx, 20*time.Millisecond); err != nil {
				counts, _ := st.OutboxCounts()
				t.Fatalf("%s: %v, OAuth failures=%d refreshes=%d poll=%s outbox=%v fixture violations=%v", reason, err, failures.Load(), refreshes.Load(), e.Snapshot().LastRead, counts, graph.Violations())
			}
		}
	}
	await(func() bool { return failures.Load() >= 2 && !recovery.degradedSince(time.Time{}).IsZero() }, "no sustained failure")
	counts, err := st.OutboxCounts()
	if err != nil || counts[corestore.OutQueued]+counts[corestore.OutSending] == 0 || !e.Snapshot().LastRead.IsZero() {
		t.Fatalf("bridge unexpectedly polled or lost its backlog during the outage: poll=%v counts=%v err=%v", e.Snapshot().LastRead, counts, err)
	}

	close(allowReplacement)
	rows := func() []corestore.Outbox {
		found, _ := st.OutboxByKeyPrefix("graph-recovery-queued")
		return found
	}
	await(func() bool {
		got := rows()
		return len(got) == 1 && got[0].State == corestore.OutSent && !e.Snapshot().LastRead.IsZero()
	}, "running bridge did not finish the durable send and poll")
	info, err := lease.Read(anchor)
	if err != nil || info.Head.Gen != holder.Gen() || info.Head.PID != os.Getpid() {
		t.Fatalf("owner changed while recovering: %+v %v", info.Head, err)
	}
	messages := 0
	for _, msg := range graph.Messages(chat) {
		if strings.Contains(msg.Content, body) {
			messages++
		}
	}
	if messages != 1 || refreshes.Load() != 1 {
		t.Fatalf("outbound copies=%d OAuth refreshes=%d, want one each", messages, refreshes.Load())
	}
}
