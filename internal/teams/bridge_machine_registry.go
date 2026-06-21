package teams

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/teams/machineregistry"
)

const (
	machineRegistryDrainPublishTimeout = 10 * time.Second
	machineRegistryMaxBackoff          = 30 * time.Minute
)

var machineRegistryWorkerSeq atomic.Uint64

type bridgeMachineRegistryPublisher struct {
	store               machineregistry.Store
	cache               machineregistry.Cache
	tenantID            string
	userID              string
	machineID           string
	label               string
	hostname            string
	profile             string
	version             string
	interval            time.Duration
	ttl                 time.Duration
	now                 func() time.Time
	executor            Executor
	inboxPollInterval   time.Duration
	claimRecheckDelay   time.Duration
	workerInstance      string
	delegationStatePath string
	workerMu            sync.Mutex
	activeDelegations   map[string]bool
	activeRemoteThreads map[string]string
	activeCancels       map[string]context.CancelFunc
	lastInboxHeadID     string
}

func (b *Bridge) startMachineRegistryHeartbeat(ctx context.Context, opts BridgeOptions) <-chan struct{} {
	if !opts.MachineRegistryEnabled || opts.Once {
		return nil
	}
	publisher, err := b.newBridgeMachineRegistryPublisher(opts)
	if err != nil {
		b.logMachineRegistryHeartbeatError(err)
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		publisher.run(ctx, b.logMachineRegistryHeartbeatError)
	}()
	return done
}

func (b *Bridge) newBridgeMachineRegistryPublisher(opts BridgeOptions) (*bridgeMachineRegistryPublisher, error) {
	graph := opts.MachineRegistryGraph
	if graph == nil {
		if b == nil || b.graph == nil {
			return nil, fmt.Errorf("machine registry graph is unavailable")
		}
		graph = NewMachineRegistryGraphAdapter(b.graph)
	}
	cachePath := strings.TrimSpace(opts.MachineRegistryCachePath)
	if cachePath == "" {
		var err error
		cachePath, err = machineregistry.DefaultCachePath()
		if err != nil {
			return nil, err
		}
	}
	delegationStatePath := strings.TrimSpace(opts.MachineDelegationStatePath)
	if delegationStatePath == "" {
		var err error
		delegationStatePath, err = appdirs.StatePath("teams", "delegation-worker-state.sqlite")
		if err != nil {
			return nil, err
		}
	}
	interval := opts.MachineRegistryInterval
	if interval <= 0 {
		interval = machineregistry.DefaultHeartbeatInterval
	}
	ttl := opts.MachineRegistryTTL
	if ttl <= 0 {
		ttl = machineregistry.DefaultOnlineTTL
	}
	if ttl <= interval {
		ttl = interval * 3
	}
	now := opts.MachineRegistryNow
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	machine := b.machine
	label := strings.TrimSpace(machine.Label)
	if label == "" {
		label = strings.TrimSpace(machine.Hostname)
	}
	if label == "" {
		label = strings.TrimSpace(machine.ID)
	}
	profile := strings.TrimSpace(machine.Profile)
	if profile == "" {
		profile = strings.TrimSpace(b.scope.Profile)
	}
	userID := strings.TrimSpace(b.user.ID)
	if userID == "" {
		userID = strings.TrimSpace(b.user.UserPrincipalName)
	}
	return &bridgeMachineRegistryPublisher{
		store: machineregistry.Store{
			Graph:     graph,
			CachePath: cachePath,
			Now:       now,
		},
		tenantID:            strings.TrimSpace(b.graphTenantID()),
		userID:              userID,
		machineID:           strings.TrimSpace(machine.ID),
		label:               label,
		hostname:            strings.TrimSpace(machine.Hostname),
		profile:             profile,
		version:             strings.TrimSpace(opts.HelperVersion),
		interval:            interval,
		ttl:                 ttl,
		now:                 now,
		executor:            opts.Executor,
		inboxPollInterval:   firstPositiveDuration(opts.Interval, 5*time.Second),
		claimRecheckDelay:   opts.MachineDelegationClaimRecheckDelay,
		workerInstance:      fmt.Sprintf("worker_%s_%d", strings.TrimSpace(machine.ID), machineRegistryWorkerSeq.Add(1)),
		delegationStatePath: delegationStatePath,
		activeDelegations:   map[string]bool{},
		activeRemoteThreads: map[string]string{},
		activeCancels:       map[string]context.CancelFunc{},
	}, nil
}

func (b *Bridge) graphTenantID() string {
	if b == nil || b.graph == nil {
		return ""
	}
	return b.graph.tenantID()
}

func (p *bridgeMachineRegistryPublisher) run(ctx context.Context, logError func(error)) {
	backoff := time.Duration(0)
	nextDelay := p.publishDelay(ctx, false, &backoff, logError)
	heartbeatTimer := time.NewTimer(nextDelay)
	defer heartbeatTimer.Stop()
	var inboxTimer *time.Timer
	if p.executor != nil {
		inboxTimer = time.NewTimer(p.inboxPollInterval)
		defer inboxTimer.Stop()
	}
	for {
		var inboxC <-chan time.Time
		if inboxTimer != nil {
			inboxC = inboxTimer.C
		}
		select {
		case <-ctx.Done():
			drainCtx, cancel := context.WithTimeout(context.Background(), machineRegistryDrainPublishTimeout)
			_ = p.publish(drainCtx, true)
			cancel()
			return
		case <-heartbeatTimer.C:
			nextDelay = p.publishDelay(ctx, false, &backoff, logError)
			heartbeatTimer.Reset(nextDelay)
		case <-inboxC:
			nextInboxDelay := p.pollDelegationInboxDelay(ctx, logError)
			inboxTimer.Reset(nextInboxDelay)
		}
	}
}

func (p *bridgeMachineRegistryPublisher) publishDelay(ctx context.Context, draining bool, backoff *time.Duration, logError func(error)) time.Duration {
	if err := p.publish(ctx, draining); err != nil {
		if logError != nil {
			logError(err)
		}
		*backoff = machineRegistryNextBackoff(*backoff, p.interval)
		return *backoff
	}
	*backoff = 0
	return p.interval
}

func (p *bridgeMachineRegistryPublisher) publish(ctx context.Context, draining bool) error {
	if p == nil {
		return fmt.Errorf("machine registry publisher is nil")
	}
	if strings.TrimSpace(p.userID) == "" {
		return fmt.Errorf("machine registry user id is unavailable")
	}
	if strings.TrimSpace(p.machineID) == "" {
		return fmt.Errorf("machine registry machine id is unavailable")
	}
	cache := p.cache
	if strings.TrimSpace(cache.RegistryChatID) == "" || strings.TrimSpace(cache.MeetingID) == "" {
		ensured, err := p.store.Ensure(ctx, p.tenantID, p.userID)
		if err != nil {
			return err
		}
		cache = ensured.Cache
	}
	if strings.TrimSpace(cache.InboxChatID) == "" || strings.TrimSpace(cache.InboxMeetingID) == "" || strings.TrimSpace(cache.InboxMachineID) != strings.TrimSpace(p.machineID) {
		next, _, _, _, err := p.store.EnsureInbox(ctx, cache, p.machineID)
		if err != nil {
			return err
		}
		cache = next
	}
	if !cache.NextRefreshAt.IsZero() && !p.now().Before(cache.NextRefreshAt) {
		refreshed, _, err := p.store.RefreshMeeting(ctx, cache)
		if err != nil {
			return err
		}
		cache = refreshed
	}
	card := p.card(cache, draining)
	next, _, _, err := p.store.Publish(ctx, cache, card)
	if err != nil {
		return err
	}
	p.cache = next
	return nil
}

func (p *bridgeMachineRegistryPublisher) card(cache machineregistry.Cache, draining bool) machineregistry.MachineCard {
	now := p.now().UTC()
	ttl := p.ttl
	aliases := machineRegistryCompactStrings([]string{p.hostname, p.profile})
	capabilities := []string{"cxp", "codex", "teams-helper", "teams-registry"}
	return machineregistry.MachineCard{
		Kind:                     machineregistry.CardKind,
		RegistryKey:              cache.RegistryKey,
		MachineID:                p.machineID,
		InstanceID:               p.workerInstanceID(),
		MachineLabel:             firstNonEmptyString(p.label, p.machineID),
		HostLabel:                strings.TrimSpace(p.hostname),
		Aliases:                  aliases,
		HelperProfile:            p.profile,
		CXPVersion:               p.version,
		Platform:                 machineregistry.MachinePlatform{OS: runtime.GOOS, Arch: runtime.GOARCH},
		Capabilities:             capabilities,
		CapabilityFingerprint:    machineregistry.CapabilityFingerprint(capabilities),
		Skills:                   []string{"cxp"},
		ProtocolVersions:         []string{"cxp-delegation-v1"},
		InboxRef:                 strings.TrimSpace(cache.InboxExternalID),
		InboxGeneration:          strings.TrimSpace(cache.InboxGeneration),
		Accepting:                !draining,
		Draining:                 draining,
		Revision:                 int(now.Unix()),
		Sequence:                 int(now.Unix()),
		HeartbeatIntervalSeconds: int(p.interval.Seconds()),
		TTLSeconds:               int(ttl.Seconds()),
		PublishedAt:              now.Format(time.RFC3339Nano),
		ExpiresAt:                now.Add(ttl).Format(time.RFC3339Nano),
	}
}

func machineRegistryNextBackoff(current time.Duration, interval time.Duration) time.Duration {
	if interval <= 0 {
		interval = machineregistry.DefaultHeartbeatInterval
	}
	if current <= 0 {
		return interval
	}
	next := current * 2
	if next > machineRegistryMaxBackoff {
		return machineRegistryMaxBackoff
	}
	return next
}

func firstPositiveDuration(value time.Duration, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func machineRegistryCompactStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (b *Bridge) logMachineRegistryHeartbeatError(err error) {
	if err == nil || b == nil || b.out == nil {
		return
	}
	_, _ = fmt.Fprintf(b.out, "Teams machine registry heartbeat warning: %v\n", err)
}
