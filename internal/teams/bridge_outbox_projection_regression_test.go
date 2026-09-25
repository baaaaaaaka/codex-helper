package teams

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type bridgeTakeoverGraphAuth struct {
	token    string
	onAccess func()
	once     sync.Once
}

func TestGraphWriteAdmissionRevalidatesOwnerAfterGateWait(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	now := time.Now().UTC().Truncate(time.Microsecond)
	lease := teamstore.ControlLease{
		ScopeID: bridge.scope.ID, HolderMachineID: bridge.machine.ID,
		HolderKind: bridge.machine.Kind, Priority: bridge.machine.Priority,
		Generation: 1, Status: teamstore.ControlLeaseStatusActive,
		LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Scope = bridge.scope
		state.Machines[bridge.machine.ID] = bridge.machine
		state.ControlLease = lease
		return nil
	}); err != nil {
		t.Fatalf("seed write-boundary lease: %v", err)
	}
	bridge.setControlLease(lease)
	owner := teamstore.OwnerMetadata{ScopeID: bridge.scope.ID, MachineID: bridge.machine.ID, LeaseGeneration: lease.Generation}
	ownerCtx := withTeamsOwnerCapability(ctx, owner)
	requestGate, err := bridge.acquireGraphWriteRequest(ctx)
	if err != nil {
		t.Fatalf("hold write-boundary gate: %v", err)
	}
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { requestGate <- struct{}{} }) }
	t.Cleanup(releaseGate)

	writeWaiting := make(chan struct{})
	productionAdmission := bridge.graphWriteRequestAdmission("chat-owner-fenced", true)
	requestCtx := withGraphBeforeWriteRequest(ownerCtx, func(requestCtx context.Context, method string) (graphWriteRequestRelease, error) {
		close(writeWaiting)
		return productionAdmission(requestCtx, method)
	})
	var requests atomic.Int32
	graph := &GraphClient{
		auth: &fakeGraphAuth{token: "access"},
		client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return jsonResponse(http.StatusCreated, `{"id":"unexpected-post"}`), nil
		})},
		baseURL: "https://graph.example.test", maxRetries: 0,
		sleep: sleepContext, jitter: func(d time.Duration) time.Duration { return d },
	}
	requestDone := make(chan error, 1)
	go func() {
		requestDone <- graph.doWithOptions(requestCtx, http.MethodPost, "/chats/chat-owner-fenced/messages", map[string]any{"body": "must stay fenced"}, nil, graphRequestOptions{
			returnRateLimitWithoutRetry: true,
			noReplayAfterFirstRequest:   true,
		})
	}()
	select {
	case <-writeWaiting:
	case <-time.After(5 * time.Second):
		t.Fatal("Graph write did not reach the blocked final admission boundary")
	}
	takeoverMachine := bridge.machine
	takeoverMachine.ID += "-takeover"
	if err := store.Update(ctx, func(state *teamstore.State) error {
		next := state.ControlLease
		next.HolderMachineID = takeoverMachine.ID
		next.Generation = lease.Generation + 1
		next.LeaseUntil = time.Now().UTC().Add(time.Hour)
		next.LastHeartbeat = time.Now().UTC()
		next.UpdatedAt = time.Now().UTC()
		state.Machines[takeoverMachine.ID] = takeoverMachine
		state.ControlLease = next
		return nil
	}); err != nil {
		releaseGate()
		t.Fatalf("take over control lease while write waits: %v", err)
	}
	releaseGate()
	select {
	case err := <-requestDone:
		if !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
			t.Fatalf("write after lease takeover = %v, want fenced lease-loss error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write did not leave final admission after gate release")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("Graph requests after owner takeover = %d, want zero", got)
	}
}

func (a *bridgeTakeoverGraphAuth) AccessToken(context.Context, io.Writer, bool) (string, error) {
	if a != nil {
		a.once.Do(a.onAccess)
		return a.token, nil
	}
	return "", nil
}

func (a *bridgeTakeoverGraphAuth) RefreshAccessToken(context.Context) (string, error) {
	if a == nil {
		return "", nil
	}
	return a.token, nil
}

// A large inherited outbox is deliberately left on the durable deferred
// marker by startup.  The listener-owned one-shot maintenance task must then
// resume the audit without making foreground startup wait for it, and must
// publish the native capability only through the store's existing audit and
// generation fence.
func TestBridgeDeferredOutboxProjectionAuditRunsAndPublishesTrustedMarkers(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	owner := teamstore.OwnerMetadata{
		PID: 91001, Hostname: "deferred-audit-host", ExecutablePath: "/opt/cxp",
		InstanceID: "deferred-audit-instance", ScopeID: "deferred-audit-scope",
		MachineID: "deferred-audit-machine", LeaseGeneration: 1,
		StartedAt: now.Add(-time.Minute), LastHeartbeat: now,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Scope = teamstore.ScopeIdentity{ID: owner.ScopeID, AccountID: "deferred-audit-account"}
		state.ControlLease = teamstore.ControlLease{
			ScopeID: owner.ScopeID, HolderMachineID: owner.MachineID,
			Generation: owner.LeaseGeneration, Status: teamstore.ControlLeaseStatusActive,
			LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
		}
		serviceOwner := owner
		lockOwner := owner
		state.ServiceOwner = &serviceOwner
		state.LockOwner = &lockOwner
		state.OutboxMessages = make(map[string]teamstore.OutboxMessage, 5000)
		for i := 0; i < 5000; i++ {
			id := "outbox:deferred-audit:" + time.Unix(int64(i), 0).UTC().Format("150405.000000000")
			state.OutboxMessages[id] = teamstore.OutboxMessage{
				ID: id, TeamsChatID: "chat:deferred-audit", Kind: "helper",
				Body: "deferred audit payload", Status: teamstore.OutboxStatusQueued,
				Sequence: int64(i + 1), CreatedAt: now.Add(time.Duration(i) * time.Nanosecond), UpdatedAt: now,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed deferred-audit outbox: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate deferred-audit outbox: %v", err)
	}

	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(store.Path()), teamstore.SQLiteFileName))
	if err != nil {
		t.Fatalf("open deferred-audit SQLite store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `UPDATE state_meta SET value = 'deferred-v1' WHERE key IN ('outbox_projection_trust', 'outbox_session_projection_trust', 'outbox_turn_projection_trust')`); err != nil {
		t.Fatalf("defer outbox projection markers: %v", err)
	}

	bridge := &Bridge{store: store, out: &bytes.Buffer{}}
	ownerCtx := withTeamsOwnerCapability(ctx, owner)
	done := bridge.startDeferredOutboxProjectionAudit(ownerCtx)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deferred outbox projection audit did not finish")
	}

	rows, err := db.QueryContext(ctx, `SELECT key, value FROM state_meta WHERE key IN ('outbox_projection_trust', 'outbox_session_projection_trust', 'outbox_turn_projection_trust') ORDER BY key`)
	if err != nil {
		t.Fatalf("read deferred-audit markers: %v", err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatalf("scan deferred-audit marker: %v", err)
		}
		seen++
		if value != "trusted-v1" {
			t.Fatalf("deferred-audit marker %s = %q, want trusted-v1", key, value)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate deferred-audit markers: %v", err)
	}
	if seen != 3 {
		t.Fatalf("deferred-audit marker count = %d, want 3", seen)
	}
}

func TestBridgeDirectOutboxSenderDefersUnboundDestinationWithoutGraphPOST(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	outbox := teamstore.OutboxMessage{
		ID: "outbox:direct-unbound-destination", Kind: "helper-final", Body: "must not post",
		Status: teamstore.OutboxStatusQueued,
	}
	if err := bridge.sendQueuedOutboxWithOptions(context.Background(), outbox, outboxSendOptions{}); !isOutboxDeliveryDeferred(err) {
		t.Fatalf("unbound direct sender error = %v, want durable delivery deferral", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("unbound direct sender issued Graph POST(s): %#v", *sent)
	}
}

func TestBridgeOwnerFenceStopsPostAfterDurableTakeover(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	now := time.Now().UTC().Truncate(time.Microsecond)
	const outboxID = "outbox:owner-fence-before-post"
	lease := teamstore.ControlLease{
		ScopeID:         bridge.scope.ID,
		HolderMachineID: bridge.machine.ID,
		HolderKind:      bridge.machine.Kind,
		Priority:        bridge.machine.Priority,
		Generation:      1,
		Status:          teamstore.ControlLeaseStatusActive,
		LeaseUntil:      now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Scope = bridge.scope
		state.Machines[bridge.machine.ID] = bridge.machine
		state.ControlLease = lease
		state.OutboxMessages[outboxID] = teamstore.OutboxMessage{
			ID: outboxID, TeamsChatID: "chat-owner-fence", Kind: "helper-final",
			Body: "must not cross Graph after takeover", Status: teamstore.OutboxStatusQueued,
			Sequence: 1, CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed owner-fence outbox: %v", err)
	}
	bridge.setControlLease(lease)
	ownerCtx := withTeamsOwnerCapability(ctx, teamstore.OwnerMetadata{
		ScopeID: bridge.scope.ID, MachineID: bridge.machine.ID, LeaseGeneration: lease.Generation,
	})
	takeoverMachine := bridge.machine
	takeoverMachine.ID += "-takeover"
	takeoverErr := error(nil)
	graph.auth = &bridgeTakeoverGraphAuth{
		token: "access",
		onAccess: func() {
			takeoverErr = store.Update(ctx, func(state *teamstore.State) error {
				next := state.ControlLease
				next.HolderMachineID = takeoverMachine.ID
				next.Generation = lease.Generation + 1
				next.LeaseUntil = now.Add(time.Hour)
				next.LastHeartbeat = time.Now().UTC()
				next.UpdatedAt = time.Now().UTC()
				state.Machines[takeoverMachine.ID] = takeoverMachine
				state.ControlLease = next
				return nil
			})
		},
	}
	outbox, err := store.OutboxMessageByID(ctx, outboxID)
	if err != nil {
		t.Fatalf("load owner-fence outbox: %v", err)
	}
	err = bridge.sendQueuedOutboxWithOptions(ownerCtx, outbox, outboxSendOptions{})
	if takeoverErr != nil {
		t.Fatalf("durable takeover in auth callback: %v", takeoverErr)
	}
	if err == nil || !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
		t.Fatalf("owner-fenced send error = %v, want control-lease loss before POST", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("owner-fenced send issued Graph POST(s): %#v", *sent)
	}
	got, err := store.OutboxMessageByID(ctx, outboxID)
	if err != nil {
		t.Fatalf("reload owner-fence outbox: %v", err)
	}
	if got.TeamsMessageID != "" || teamstore.OutboxSendIsAmbiguous(got) {
		t.Fatalf("owner-fenced pre-POST failure left unsafe outbox state: %#v", got)
	}
}
