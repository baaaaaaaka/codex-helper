package stack

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/hoststate"
	"github.com/baaaaaaaka/codex-helper/internal/localproxy"
	"github.com/baaaaaaaka/codex-helper/internal/ssh"
)

type Options struct {
	// BrokerID and BrokerEpoch are persisted daemon identity values. They are
	// exposed by the health endpoint so a replacement process can be
	// distinguished from a stale listener using the same instance record.
	BrokerID       string
	BrokerEpoch    string
	SocksPort      int
	HTTPListenAddr string

	SocksReadyTimeout time.Duration

	MaxRestarts     int
	RestartBackoff  time.Duration
	TunnelStopGrace time.Duration
	RestartWindow   time.Duration
	// MaxRequestFailureRecoveries bounds recovery attempts triggered by
	// upstream request failures separately from tunnel-exit restarts.
	MaxRequestFailureRecoveries int
	RequestFailureWindow        time.Duration
	// RequestFailureConfirmations requires independent request/probe evidence
	// before a shared backend is replaced. The request failure itself counts
	// as the first observation and a failed active-backend probe counts as the
	// second, so the default does not add an extra network round trip.
	RequestFailureConfirmations int
	// RequestFailureAdmissionWindow bounds how long a burst may accumulate
	// confirmations. A later failure starts a fresh flight instead of joining
	// an old outage and causing a delayed recovery storm.
	RequestFailureAdmissionWindow time.Duration
	// ProbeInterval controls the low-cost local SOCKS readiness probe. A long
	// gap between probes is treated as a sleep/resume event and triggers the
	// stronger remote-path probe before recovery is attempted.
	ProbeInterval time.Duration
	// RouteProbe, when set, verifies a concrete target through the active SOCKS
	// backend. Production stacks install the real ProbeSOCKS5Target function;
	// tests that construct a monitor with an in-memory tunnel can leave it nil
	// and exercise local SOCKS readiness without making external connections.
	RouteProbe func(context.Context, string, string, int, time.Duration) error
	// RouteTargetHost/Port identify the actual destination that proves the
	// tunnel's capability. They are explicit because an SSH config alias or a
	// ProxyJump endpoint is not necessarily the route target itself.
	RouteTargetHost string
	RouteTargetPort int
	// RecoveryBudget and PersistRecoveryBudget make the circuit breaker survive
	// daemon and native-supervisor replacement. The callback must be fenced by
	// the owning broker epoch/token by the caller.
	RecoveryBudget        config.ProxyRecoveryBudget
	PersistRecoveryBudget func(config.ProxyRecoveryBudget) error
	// Now is an optional clock used by the monitor when deciding whether a
	// probe interval contains a sleep/resume-sized gap. Production uses
	// time.Now; CI fault labs can advance a deterministic clock without
	// suspending the runner or changing its network interfaces.
	Now func() time.Time
	// HostObserver supplies user-mode power/network lifecycle hints. Events are
	// advisory; host/backend probes remain the source of truth.
	HostObserver hoststate.Observer
	// HostProbe is the host-network admission gate used after wake/network
	// events. It must be cheap and bounded; nil uses the built-in probe.
	HostProbe        func(context.Context) error
	HostProbeTimeout time.Duration
}

type Stack struct {
	InstanceID string
	Profile    config.Profile

	SocksPort int
	HTTPAddr  string
	HTTPPort  int

	proxy  *localproxy.HTTPProxy
	router *localproxy.GenerationRouter
	tunnel tunnelProcess

	hostObserver hoststate.Observer
	hostEvents   <-chan hoststate.Event
	hostProbe    func(context.Context) error

	hostMu          sync.RWMutex
	powerState      hoststate.PowerState
	networkState    hoststate.NetworkState
	hostReady       bool
	lastHostEventAt time.Time
	hostProbeAt     time.Time
	hostProbeCount  uint64
	recoveryGen     uint64

	recoveryMu     sync.Mutex
	recoveryCancel context.CancelFunc
	candidate      tunnelProcess

	tunnelMu sync.Mutex
	socksMu  sync.RWMutex
	// setProxySocksAddr is owned by the HTTP proxy setup and lets health
	// checks follow the active backend when recovery moves the SOCKS listener
	// to a temporary candidate port.
	setProxySocksAddr   func(string)
	setRecoveryState    func(string, error)
	recordRouteEvidence func(string, error)
	setHostState        func(hoststate.PowerState, hoststate.NetworkState, bool, time.Time, time.Time, uint64, uint64, bool)

	fatalCh   chan error
	stopCh    chan struct{}
	probeCh   chan struct{}
	failureCh chan error

	closeMu   sync.Mutex
	closing   bool
	closeDone chan struct{}
	closeErr  error

	closeHook func()

	budgetMu              sync.Mutex
	recoveryBudget        config.ProxyRecoveryBudget
	persistRecoveryBudget func(config.ProxyRecoveryBudget) error
}

type tunnelProcess interface {
	Start() error
	Stop(time.Duration) error
	Done() <-chan struct{}
	Wait() error
}

// proxySetup holds the local HTTP proxy components created by setupHTTPProxy.
type proxySetup struct {
	proxy               *localproxy.HTTPProxy
	router              *localproxy.GenerationRouter
	httpAddr            string
	httpPort            int
	setSocksAddr        func(string)
	probeCh             chan struct{}
	failureCh           chan error
	setRecoveryState    func(string, error)
	recordRouteEvidence func(string, error)
	setHostState        func(hoststate.PowerState, hoststate.NetworkState, bool, time.Time, time.Time, uint64, uint64, bool)
}

type proxyHealthState struct {
	mu              sync.RWMutex
	recovery        string
	lastProbeAt     time.Time
	lastProbeError  string
	probeCount      uint64
	recoveryCount   uint64
	backendFailures uint64
	routeEvidence   map[string]localproxy.RouteEvidence
	powerState      hoststate.PowerState
	networkState    hoststate.NetworkState
	hostReady       bool
	lastHostEventAt time.Time
	hostProbeAt     time.Time
	hostProbeCount  uint64
	recoveryGen     uint64
	candidateActive bool
	lastRecovery    string
}

type requestFailureKind string

const (
	requestFailureTransient requestFailureKind = "transient"
	requestFailureTarget    requestFailureKind = "target"
	requestFailureIgnored   requestFailureKind = "ignored"
)

type requestFailureGate struct {
	count int
	since time.Time
}

// ErrRecoveryBudgetBlocked is returned before opening any listener or SSH
// process when a previous broker instance has durably exhausted its budget.
var ErrRecoveryBudgetBlocked = errors.New("proxy recovery budget is blocked")

func (g *requestFailureGate) observe(now time.Time, window time.Duration) int {
	if window <= 0 {
		window = time.Minute
	}
	if g.since.IsZero() || now.Before(g.since) || now.Sub(g.since) > window {
		g.count = 0
		g.since = now
	}
	g.count++
	return g.count
}

func (g *requestFailureGate) reset() {
	g.count = 0
	g.since = time.Time{}
}

func classifyRequestFailure(err error) requestFailureKind {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return requestFailureIgnored
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{"401", "403", "unauthorized", "forbidden", "authentication failed", "permission denied"} {
		if strings.Contains(message, marker) {
			return requestFailureTarget
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) || strings.Contains(message, "connection reset") || strings.Contains(message, "broken pipe") || strings.Contains(message, "network is unreachable") || strings.Contains(message, "no route") || strings.Contains(message, "eof") {
		return requestFailureTransient
	}
	// Backend dialers can return plain errors, so unknown failures remain
	// recoverable. The active-backend probe is the final admission check.
	return requestFailureTransient
}

func optionsNow(opts Options) time.Time {
	if opts.Now != nil {
		return opts.Now()
	}
	return time.Now()
}

const routeEvidenceTTL = 30 * time.Second

func (h *proxyHealthState) set(state string, err error) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.recovery = state
	if state == "probing" || state == "ready" || err != nil {
		h.lastProbeAt = time.Now()
	}
	if state == "probing" {
		h.probeCount++
	}
	if state == "building-candidate" {
		h.recoveryCount++
	}
	if err != nil {
		h.lastProbeError = err.Error()
		h.lastRecovery = err.Error()
	} else if state == "ready" {
		h.lastProbeError = ""
	}
	h.mu.Unlock()
}

func (h *proxyHealthState) setHostState(power hoststate.PowerState, network hoststate.NetworkState, ready bool, eventAt, probeAt time.Time, probeCount, generation uint64, candidate bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.powerState = power
	h.networkState = network
	h.hostReady = ready
	if !eventAt.IsZero() {
		h.lastHostEventAt = eventAt
	}
	if !probeAt.IsZero() {
		h.hostProbeAt = probeAt
	}
	h.hostProbeCount = probeCount
	h.recoveryGen = generation
	h.candidateActive = candidate
	h.mu.Unlock()
}

func (h *proxyHealthState) recordBackendFailure() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.backendFailures++
	h.mu.Unlock()
}

func (h *proxyHealthState) recordRouteEvidence(target string, err error) {
	if h == nil || target == "" {
		return
	}
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.routeEvidence == nil {
		h.routeEvidence = make(map[string]localproxy.RouteEvidence)
	}
	evidence := h.routeEvidence[target]
	evidence.Target = target
	if err == nil {
		evidence.Ready = true
		evidence.LastSuccessAt = now
		evidence.LastError = ""
		evidence.ExpiresAt = now.Add(routeEvidenceTTL)
	} else {
		evidence.Ready = false
		evidence.LastFailureAt = now
		evidence.LastError = err.Error()
		evidence.ExpiresAt = time.Time{}
	}
	h.routeEvidence[target] = evidence
}

func (h *proxyHealthState) snapshot() localproxy.HealthStatus {
	if h == nil {
		return localproxy.HealthStatus{}
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	evidence := make(map[string]localproxy.RouteEvidence, len(h.routeEvidence))
	capabilityReady := false
	now := time.Now()
	for target, item := range h.routeEvidence {
		evidence[target] = item
		if item.Ready && item.ExpiresAt.After(now) {
			capabilityReady = true
		}
	}
	return localproxy.HealthStatus{
		RouteEvidence:        evidence,
		CapabilityRouteReady: capabilityReady,
		Recovery:             h.recovery,
		LastProbeAt:          h.lastProbeAt,
		LastProbeError:       h.lastProbeError,
		ProbeCount:           h.probeCount,
		RecoveryCount:        h.recoveryCount,
		BackendFailures:      h.backendFailures,
		PowerState:           string(h.powerState),
		HostNetworkState:     string(h.networkState),
		HostReady:            h.hostReady,
		LastHostEventAt:      h.lastHostEventAt,
		LastHostProbeAt:      h.hostProbeAt,
		HostProbeCount:       h.hostProbeCount,
		RecoveryGeneration:   h.recoveryGen,
		CandidateActive:      h.candidateActive,
		LastRecoveryCause:    h.lastRecovery,
	}
}

// setupHTTPProxy creates a SOCKS5 dialer pointing at socksAddr, builds an
// HTTP proxy on top of it, and starts listening on httpListenAddr.
func setupHTTPProxy(socksAddr, httpListenAddr, instanceID string) (*proxySetup, error) {
	return setupHTTPProxyWithIdentity(socksAddr, httpListenAddr, instanceID, instanceID, "")
}

func setupHTTPProxyWithIdentity(socksAddr, httpListenAddr, instanceID, brokerID, brokerEpoch string) (*proxySetup, error) {
	dialer, err := localproxy.NewSOCKS5Dialer(socksAddr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	router, err := localproxy.NewGenerationRouter(dialer)
	if err != nil {
		return nil, err
	}
	var addrMu sync.RWMutex
	currentSocksAddr := socksAddr
	getSocksAddr := func() string {
		addrMu.RLock()
		defer addrMu.RUnlock()
		return currentSocksAddr
	}
	setSocksAddr := func(addr string) {
		addrMu.Lock()
		currentSocksAddr = addr
		addrMu.Unlock()
	}
	probeCh := make(chan struct{}, 1)
	failureCh := make(chan error, 1)
	if brokerID == "" {
		brokerID = instanceID
	}
	if brokerEpoch == "" {
		brokerEpoch = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	healthState := &proxyHealthState{
		recovery:      "starting",
		routeEvidence: make(map[string]localproxy.RouteEvidence),
	}
	hp := localproxy.NewHTTPProxy(router, localproxy.Options{
		InstanceID: instanceID,
		HealthProbe: func(ctx context.Context) error {
			return localproxy.ProbeSOCKS5(ctx, getSocksAddr(), 750*time.Millisecond)
		},
		OnBackendFailure: func(failureErr error) {
			healthState.recordBackendFailure()
			// Request failures have their own coalesced channel. Do not also
			// enqueue probeCh: if monitor selects the resume channel first it
			// could drain the generic signal and bypass the request-failure
			// budget entirely.
			select {
			case failureCh <- failureErr:
			default:
			}
		},
		OnTargetFailure: func(target string, failureErr error) {
			// A target-specific request failure is useful evidence, but the
			// monitor still probes the shared backend before deciding whether
			// recovery is warranted. One destination must not take down a
			// healthy tunnel used by other destinations.
			healthState.recordRouteEvidence(target, failureErr)
		},
		HealthDetails: func(context.Context) localproxy.HealthStatus {
			details := healthState.snapshot()
			details.BrokerID = brokerID
			details.BrokerEpoch = brokerEpoch
			details.ActiveGeneration = router.CurrentGeneration()
			return details
		},
	})
	httpAddr, err := hp.Start(httpListenAddr)
	if err != nil {
		_ = router.Close(context.Background())
		return nil, err
	}
	_, portStr, err := net.SplitHostPort(httpAddr)
	if err != nil {
		_ = hp.Close(context.Background())
		_ = router.Close(context.Background())
		return nil, err
	}
	httpPort, err := parsePort(portStr)
	if err != nil {
		_ = hp.Close(context.Background())
		_ = router.Close(context.Background())
		return nil, err
	}
	return &proxySetup{
		proxy:               hp,
		router:              router,
		httpAddr:            httpAddr,
		httpPort:            httpPort,
		setSocksAddr:        setSocksAddr,
		probeCh:             probeCh,
		failureCh:           failureCh,
		setRecoveryState:    healthState.set,
		recordRouteEvidence: healthState.recordRouteEvidence,
		setHostState:        healthState.setHostState,
	}, nil
}

func closeProxySetup(ps *proxySetup) {
	if ps == nil {
		return
	}
	if ps.proxy != nil {
		_ = ps.proxy.Close(context.Background())
	}
	if ps.router != nil {
		_ = ps.router.Close(context.Background())
	}
}

// reservePort allocates a TCP port on the loopback interface and returns the
// port number together with the held listener. The caller must close the
// listener to release the port (typically right before handing it to SSH).
func reservePort() (int, net.Listener, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, nil, err
	}
	return ln.Addr().(*net.TCPAddr).Port, ln, nil
}

func Start(profile config.Profile, instanceID string, opts Options) (*Stack, error) {
	if profile.Host == "" {
		return nil, errors.New("profile host is required")
	}
	if profile.Port <= 0 {
		return nil, errors.New("profile port is required")
	}
	if profile.User == "" {
		return nil, errors.New("profile user is required")
	}
	if instanceID == "" {
		return nil, errors.New("instance id is required")
	}

	if opts.HTTPListenAddr == "" {
		opts.HTTPListenAddr = "127.0.0.1:0"
	}
	if opts.MaxRestarts <= 0 {
		opts.MaxRestarts = 3
	}
	if opts.RestartBackoff <= 0 {
		opts.RestartBackoff = 1 * time.Second
	}
	if opts.TunnelStopGrace <= 0 {
		opts.TunnelStopGrace = 2 * time.Second
	}
	if opts.RestartWindow <= 0 {
		opts.RestartWindow = time.Minute
	}
	if opts.MaxRequestFailureRecoveries <= 0 {
		opts.MaxRequestFailureRecoveries = opts.MaxRestarts
	}
	if opts.RequestFailureWindow <= 0 {
		opts.RequestFailureWindow = opts.RestartWindow
	}
	if opts.RequestFailureConfirmations <= 0 {
		opts.RequestFailureConfirmations = 2
	}
	if opts.RequestFailureAdmissionWindow <= 0 {
		opts.RequestFailureAdmissionWindow = opts.RequestFailureWindow
	}
	if opts.SocksReadyTimeout <= 0 {
		opts.SocksReadyTimeout = 30 * time.Second
	}
	if opts.ProbeInterval <= 0 {
		opts.ProbeInterval = 5 * time.Second
	}
	if opts.HostProbeTimeout <= 0 {
		opts.HostProbeTimeout = minProbeTimeout(opts.SocksReadyTimeout)
	}
	if opts.RouteProbe == nil {
		opts.RouteProbe = localproxy.ProbeSOCKS5Target
	}
	if opts.RouteTargetHost == "" {
		opts.RouteTargetHost = profile.Host
	}
	if opts.RouteTargetPort == 0 {
		opts.RouteTargetPort = profile.Port
	}
	if opts.HostObserver == nil {
		opts.HostObserver = hoststate.NewDefaultObserver(hoststate.Options{Interval: opts.ProbeInterval})
	}
	if opts.HostProbe == nil {
		opts.HostProbe = func(ctx context.Context) error {
			return probeHostNetwork(ctx, profile)
		}
	}
	if opts.RecoveryBudget.Blocked {
		return nil, fmt.Errorf("%w: %s", ErrRecoveryBudgetBlocked, opts.RecoveryBudget.LastReason)
	}

	// Reserve the requested SOCKS port: hold the listener open so that the HTTP
	// proxy (which also binds :0 by default) cannot accidentally grab the same
	// port before SSH starts. For explicit ports this also fails fast if the
	// caller asked for a port that is already unavailable.
	socksPort := opts.SocksPort
	var socksReserve net.Listener
	if socksPort == 0 {
		port, ln, err := reservePort()
		if err != nil {
			return nil, fmt.Errorf("reserve socks port: %w", err)
		}
		socksPort = port
		socksReserve = ln
	} else {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", socksPort))
		if err != nil {
			return nil, fmt.Errorf("reserve requested socks port %d: %w", socksPort, err)
		}
		socksReserve = ln
	}

	socksAddr := fmt.Sprintf("127.0.0.1:%d", socksPort)
	ps, err := setupHTTPProxyWithIdentity(socksAddr, opts.HTTPListenAddr, instanceID, opts.BrokerID, opts.BrokerEpoch)
	if err != nil {
		if socksReserve != nil {
			socksReserve.Close()
		}
		return nil, err
	}

	// Only retry with new ports when the SOCKS port was auto-selected.
	// When an explicit port was requested, honour the caller's choice:
	// return exactly that port or an error.
	canRetry := opts.SocksPort == 0
	const maxPortRetries = 3

	// Release the reserved SOCKS port and immediately start the SSH tunnel.
	// If the tunnel fails to bind, retry with a freshly reserved port.
	var tun tunnelProcess
	for attempt := 0; ; attempt++ {
		if socksReserve != nil {
			socksReserve.Close()
			socksReserve = nil
		}
		t, terr := newStackTunnel(profile, socksPort)
		if terr != nil {
			closeProxySetup(ps)
			return nil, terr
		}
		if terr := t.Start(); terr != nil {
			closeProxySetup(ps)
			return nil, terr
		}
		if terr := waitForTCPTunnel(socksAddr, opts.SocksReadyTimeout, t); terr != nil {
			_ = t.Stop(opts.TunnelStopGrace)
			if canRetry && attempt < maxPortRetries {
				// Reserve a new port (held open until the next iteration
				// releases it), then rebuild the HTTP proxy for the new
				// SOCKS address.
				port, ln, reserveErr := reservePort()
				if reserveErr == nil {
					socksPort = port
					socksReserve = ln
					socksAddr = fmt.Sprintf("127.0.0.1:%d", socksPort)
					closeProxySetup(ps)
					newPs, psErr := setupHTTPProxyWithIdentity(socksAddr, opts.HTTPListenAddr, instanceID, opts.BrokerID, opts.BrokerEpoch)
					if psErr != nil {
						socksReserve.Close()
						return nil, psErr
					}
					ps = newPs
					continue
				}
			}
			closeProxySetup(ps)
			return nil, terr
		}
		if terr := waitForSOCKS5(socksAddr, opts.SocksReadyTimeout, t); terr != nil {
			_ = t.Stop(opts.TunnelStopGrace)
			closeProxySetup(ps)
			return nil, terr
		}
		tun = t
		break
	}

	s := &Stack{
		InstanceID:            instanceID,
		Profile:               profile,
		SocksPort:             socksPort,
		HTTPAddr:              ps.httpAddr,
		HTTPPort:              ps.httpPort,
		proxy:                 ps.proxy,
		tunnel:                tun,
		fatalCh:               make(chan error, 1),
		stopCh:                make(chan struct{}),
		probeCh:               ps.probeCh,
		failureCh:             ps.failureCh,
		router:                ps.router,
		setProxySocksAddr:     ps.setSocksAddr,
		setRecoveryState:      ps.setRecoveryState,
		recordRouteEvidence:   ps.recordRouteEvidence,
		setHostState:          ps.setHostState,
		hostObserver:          opts.HostObserver,
		hostProbe:             opts.HostProbe,
		powerState:            hoststate.PowerAwake,
		networkState:          hoststate.NetworkReady,
		hostReady:             true,
		recoveryBudget:        opts.RecoveryBudget,
		persistRecoveryBudget: opts.PersistRecoveryBudget,
	}
	s.hostEvents = opts.HostObserver.Events()
	if err := opts.HostObserver.Start(); err != nil {
		_ = tun.Stop(opts.TunnelStopGrace)
		closeProxySetup(ps)
		return nil, fmt.Errorf("start host state observer: %w", err)
	}
	s.publishHostState(time.Time{}, time.Time{})
	s.setRecoveryStateNow("ready", nil)

	go s.monitor(opts)
	return s, nil
}

func (s *Stack) HTTPProxyURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", s.HTTPPort)
}

// CurrentSocksPort returns the SOCKS port used by the active backend. The
// public HTTP proxy port remains stable while this value may change during a
// candidate generation switch.
func (s *Stack) CurrentSocksPort() int {
	if s == nil {
		return 0
	}
	s.socksMu.RLock()
	defer s.socksMu.RUnlock()
	return s.SocksPort
}

func (s *Stack) Fatal() <-chan error { return s.fatalCh }

func (s *Stack) setRecoveryStateNow(state string, err error) {
	if s != nil && s.setRecoveryState != nil {
		s.setRecoveryState(state, err)
	}
}

var errHostSuspended = errors.New("host is suspended")

func (s *Stack) publishHostState(eventAt, probeAt time.Time) {
	if s == nil {
		return
	}
	s.hostMu.RLock()
	power := s.powerState
	network := s.networkState
	ready := s.hostReady
	probeCount := s.hostProbeCount
	generation := s.recoveryGen
	s.hostMu.RUnlock()
	candidate := s.currentCandidate() != nil
	if s.setHostState != nil {
		s.setHostState(power, network, ready, eventAt, probeAt, probeCount, generation, candidate)
	}
}

func (s *Stack) hostGateRequired() bool {
	return s != nil && s.hostObserver != nil && s.hostProbe != nil
}

func (s *Stack) hostSuspended() bool {
	if s == nil {
		return false
	}
	s.hostMu.RLock()
	defer s.hostMu.RUnlock()
	return s.powerState == hoststate.PowerSuspended
}

func (s *Stack) hostReadyNow() bool {
	if s == nil || !s.hostGateRequired() {
		return true
	}
	s.hostMu.RLock()
	defer s.hostMu.RUnlock()
	return s.hostReady && s.powerState != hoststate.PowerSuspended
}

func (s *Stack) currentRecoveryGeneration() uint64 {
	if s == nil {
		return 0
	}
	s.hostMu.RLock()
	defer s.hostMu.RUnlock()
	return s.recoveryGen
}

func (s *Stack) recoveryGenerationCurrent(generation uint64) bool {
	if s == nil || s.stopped() {
		return false
	}
	s.drainHostEvents()
	if s.hostGateRequired() && !s.hostReadyNow() {
		return false
	}
	return s.currentRecoveryGeneration() == generation
}

func (s *Stack) handleHostEvent(event hoststate.Event) bool {
	if s == nil {
		return false
	}
	if event.At.IsZero() {
		event.At = time.Now()
	}
	s.hostMu.Lock()
	s.lastHostEventAt = event.At
	s.recoveryGen++
	switch event.Kind {
	case hoststate.EventPowerWillSleep:
		s.powerState = hoststate.PowerSuspended
		s.networkState = hoststate.NetworkUnknown
		s.hostReady = false
	case hoststate.EventPowerDidWake:
		s.powerState = hoststate.PowerWaking
		s.networkState = hoststate.NetworkUnknown
		s.hostReady = false
	case hoststate.EventNetworkChanged, hoststate.EventObserverError:
		if s.powerState != hoststate.PowerSuspended {
			s.powerState = hoststate.PowerAwake
		}
		s.networkState = hoststate.NetworkUnknown
		s.hostReady = false
	default:
		s.networkState = hoststate.NetworkUnknown
		s.hostReady = false
	}
	s.hostMu.Unlock()
	s.publishHostState(event.At, time.Time{})

	switch event.Kind {
	case hoststate.EventPowerWillSleep:
		s.setRecoveryStateNow("suspended", nil)
		return false
	case hoststate.EventPowerDidWake, hoststate.EventNetworkChanged, hoststate.EventObserverError:
		s.setRecoveryStateNow("waiting-for-network", nil)
		return true
	default:
		return false
	}
}

func (s *Stack) recordHostProbe(probeAt time.Time, err error) {
	s.hostMu.Lock()
	s.hostProbeAt = probeAt
	s.hostProbeCount++
	if err == nil {
		s.powerState = hoststate.PowerAwake
		s.networkState = hoststate.NetworkReady
		s.hostReady = true
	} else if s.powerState != hoststate.PowerSuspended {
		s.networkState = hoststate.NetworkDown
		s.hostReady = false
	}
	s.hostMu.Unlock()
	s.publishHostState(time.Time{}, probeAt)
}

func (s *Stack) waitForHostReady(opts Options) error {
	if s == nil || !s.hostGateRequired() {
		return nil
	}
	if opts.HostProbeTimeout <= 0 {
		opts.HostProbeTimeout = minProbeTimeout(opts.SocksReadyTimeout)
	}
	delays := []time.Duration{
		250 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		15 * time.Second,
		30 * time.Second,
	}
	for attempt := 0; ; attempt++ {
		if s.stopped() {
			return context.Canceled
		}
		if s.hostSuspended() {
			return errHostSuspended
		}
		probeAt := optionsNow(opts)
		ctx, cancel := context.WithTimeout(context.Background(), minProbeTimeout(opts.HostProbeTimeout))
		err := s.hostProbe(ctx)
		cancel()
		s.recordHostProbe(probeAt, err)
		if err == nil {
			s.setRecoveryStateNow("revalidating-after-wake", nil)
			return nil
		}
		s.setRecoveryStateNow("waiting-for-network", err)
		delay := delays[len(delays)-1]
		if attempt < len(delays) {
			delay = delays[attempt]
		}
		timer := time.NewTimer(delay)
		select {
		case <-s.stopCh:
			if !timer.Stop() {
				<-timer.C
			}
			return context.Canceled
		case event, ok := <-s.hostEvents:
			if !timer.Stop() {
				<-timer.C
			}
			if !ok {
				s.hostEvents = nil
				continue
			}
			s.handleHostEvent(event)
			s.drainHostEvents()
			if s.hostSuspended() {
				return errHostSuspended
			}
		case <-timer.C:
		}
	}
}

func probeHostNetwork(ctx context.Context, profile config.Profile) error {
	interfaces, err := net.Interfaces()
	if err != nil {
		return fmt.Errorf("list network interfaces: %w", err)
	}
	hasUsableInterface := false
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagLoopback == 0 {
			hasUsableInterface = true
			break
		}
	}
	if !hasUsableInterface {
		return errors.New("no usable network interface")
	}
	host := profile.Host
	port := profile.Port
	// An SSH config alias is not necessarily DNS-resolvable locally. When an
	// explicit route target exists it is a useful network admission endpoint;
	// otherwise the interface gate is the only safe local fact and the actual
	// ssh candidate remains the authoritative endpoint probe.
	if ssh.ArgsUseConfigFile(profile.SSHArgs) {
		if profile.RouteTargetHost == "" || profile.RouteTargetPort <= 0 {
			return nil
		}
		host = profile.RouteTargetHost
		port = profile.RouteTargetPort
	}
	address := net.JoinHostPort(host, strconv.Itoa(port))
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("ssh endpoint %s unavailable: %w", address, err)
	}
	_ = conn.Close()
	return nil
}

// NotifyNetworkResume asks the stack to re-establish the tunnel. Calls are
// coalesced so a burst of resume/network notifications schedules one recovery
// attempt rather than one process restart per notification.
func (s *Stack) NotifyNetworkResume() {
	if s == nil || s.probeCh == nil || s.stopped() {
		return
	}
	select {
	case s.probeCh <- struct{}{}:
	default:
	}
}

func (s *Stack) Close(ctx context.Context) error {
	s.setRecoveryStateNow("stopping", nil)
	s.closeMu.Lock()
	if s.closeDone == nil {
		s.closeDone = make(chan struct{})
	}
	if s.closing {
		done := s.closeDone
		s.closeMu.Unlock()
		<-done
		s.closeMu.Lock()
		err := s.closeErr
		s.closeMu.Unlock()
		return err
	}
	s.closing = true
	done := s.closeDone
	s.closeMu.Unlock()

	defer func() {
		s.closeMu.Lock()
		close(done)
		s.closeMu.Unlock()
	}()
	var firstErr error

	select {
	case <-s.stopCh:
		// already closed
	default:
		close(s.stopCh)
	}
	s.cancelRecovery()
	if s.hostObserver != nil {
		if err := s.hostObserver.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	defer func() {
		if s.closeHook != nil {
			s.closeHook()
		}
	}()

	if candidate := s.currentCandidate(); candidate != nil && candidate != s.currentTunnel() {
		if err := candidate.Stop(2 * time.Second); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if tun := s.currentTunnel(); tun != nil {
		if err := tun.Stop(2 * time.Second); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.proxy != nil {
		if err := s.proxy.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.router != nil {
		if err := s.router.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.closeMu.Lock()
	s.closeErr = firstErr
	s.closeMu.Unlock()
	return firstErr
}

func (s *Stack) monitor(opts Options) {
	if opts.ProbeInterval <= 0 {
		opts.ProbeInterval = 5 * time.Second
	}
	if opts.RestartWindow <= 0 {
		opts.RestartWindow = time.Minute
	}
	if opts.RequestFailureWindow <= 0 {
		opts.RequestFailureWindow = opts.RestartWindow
	}
	if opts.RequestFailureAdmissionWindow <= 0 {
		opts.RequestFailureAdmissionWindow = opts.RequestFailureWindow
	}
	if opts.RequestFailureConfirmations <= 0 {
		opts.RequestFailureConfirmations = 2
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	restarts := []time.Time{}
	requestFailures := []time.Time{}
	requestGate := requestFailureGate{}
	probeTicker := time.NewTicker(opts.ProbeInterval)
	defer probeTicker.Stop()
	lastProbe := now()
	for {
		tun := s.currentTunnel()
		if tun == nil {
			if !s.stopped() {
				s.fatalCh <- errors.New("ssh tunnel missing")
			}
			return
		}
		select {
		case <-s.stopCh:
			return
		case event, ok := <-s.hostEvents:
			if !ok {
				s.hostEvents = nil
				continue
			}
			if !s.handleHostEvent(event) {
				continue
			}
			s.drainHostEvents()
			if s.hostSuspended() {
				continue
			}
			if err := s.waitForHostReady(opts); err != nil {
				if errors.Is(err, context.Canceled) && s.stopped() {
					return
				}
				continue
			}
			if err := s.probeTunnel(opts, true); err != nil {
				if !s.recoverTunnelAt(opts, &restarts, fmt.Errorf("host event probe failed: %w", err), now()) {
					return
				}
			}
			lastProbe = now()
		case <-tun.Done():
			s.drainHostEvents()
			if s.hostGateRequired() && !s.hostReadyNow() {
				if err := s.waitForHostReady(opts); err != nil {
					if errors.Is(err, context.Canceled) && s.stopped() {
						return
					}
					continue
				}
			}
			if err := tun.Wait(); err != nil {
				if !s.recoverTunnelAt(opts, &restarts, err, now()) {
					return
				}
			} else if !s.recoverTunnel(opts, &restarts, errors.New("ssh tunnel exited")) {
				return
			}
			s.drainProbeSignals()
			lastProbe = now()
		case <-s.probeCh:
			if s.stopped() {
				return
			}
			if s.hostGateRequired() {
				s.handleHostEvent(hoststate.Event{Kind: hoststate.EventNetworkChanged, Source: "legacy-resume-signal"})
				if err := s.waitForHostReady(opts); err != nil {
					if errors.Is(err, context.Canceled) && s.stopped() {
						return
					}
					continue
				}
			}
			if err := s.probeTunnel(opts, true); err != nil {
				if !s.recoverTunnelAt(opts, &restarts, fmt.Errorf("network resume probe failed: %w", err), now()) {
					return
				}
			}
			s.drainProbeSignals()
			lastProbe = now()
		case failureErr := <-s.failureCh:
			if s.stopped() {
				return
			}
			if classifyRequestFailure(failureErr) != requestFailureTransient {
				// Authentication/authorization and canceled requests are not
				// evidence that the shared tunnel is broken.
				requestGate.reset()
				s.drainFailureSignals()
				continue
			}
			if s.hostGateRequired() {
				if err := s.waitForHostReady(opts); err != nil {
					if errors.Is(err, context.Canceled) && s.stopped() {
						return
					}
					s.drainFailureSignals()
					continue
				}
			}
			requestGate.observe(now(), opts.RequestFailureAdmissionWindow)
			if err := s.probeTunnel(opts, true); err != nil {
				confirmations := requestGate.observe(now(), opts.RequestFailureAdmissionWindow)
				if confirmations < opts.RequestFailureConfirmations {
					s.setRecoveryStateNow("degraded", fmt.Errorf("request failure confirmation pending: %w", err))
					s.drainFailureSignals()
					continue
				}
				if !s.allowRequestFailureRecoveryAt(opts, &requestFailures, failureErr, err, now()) {
					return
				}
				if !s.recoverTunnelAt(opts, &restarts, fmt.Errorf("request failure probe failed: %w", err), now()) {
					return
				}
				requestGate.reset()
			} else {
				requestGate.reset()
				requestFailures = requestFailures[:0]
			}
			s.drainProbeSignals()
			s.drainFailureSignals()
			lastProbe = now()
		case <-probeTicker.C:
			current := now()
			resumed := current.Sub(lastProbe) > 2*opts.ProbeInterval
			lastProbe = current
			if resumed && s.hostGateRequired() {
				s.handleHostEvent(hoststate.Event{Kind: hoststate.EventPowerDidWake, At: current, Source: "probe-gap"})
			}
			if s.hostGateRequired() && !s.hostReadyNow() {
				if err := s.waitForHostReady(opts); err != nil {
					if errors.Is(err, context.Canceled) && s.stopped() {
						return
					}
					continue
				}
			}
			if err := s.probeTunnel(opts, resumed); err != nil {
				if !s.recoverTunnelAt(opts, &restarts, fmt.Errorf("periodic proxy probe failed: %w", err), now()) {
					return
				}
				lastProbe = now()
				s.drainProbeSignals()
			}
		}
	}
}

func (s *Stack) probeTunnel(opts Options, remotePath bool) error {
	s.setRecoveryStateNow("probing", nil)
	tun := s.currentTunnel()
	if tun == nil {
		return s.probeResult(errors.New("ssh tunnel missing"))
	}
	select {
	case <-tun.Done():
		return s.probeResult(fmt.Errorf("ssh tunnel exited: %w", tun.Wait()))
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), minProbeTimeout(opts.SocksReadyTimeout))
	defer cancel()
	socksAddr := fmt.Sprintf("127.0.0.1:%d", s.CurrentSocksPort())
	if remotePath && opts.RouteProbe != nil {
		targetHost, targetPort := routeTarget(s.Profile, opts)
		target := net.JoinHostPort(targetHost, strconv.Itoa(targetPort))
		err := opts.RouteProbe(ctx, socksAddr, targetHost, targetPort, minProbeTimeout(opts.SocksReadyTimeout))
		if s.recordRouteEvidence != nil {
			s.recordRouteEvidence(target, err)
		}
		return s.probeResult(err)
	}
	return s.probeResult(localproxy.ProbeSOCKS5(ctx, socksAddr, minProbeTimeout(opts.SocksReadyTimeout)))
}

func (s *Stack) probeResult(err error) error {
	if err != nil {
		s.setRecoveryStateNow("degraded", err)
	} else {
		s.setRecoveryStateNow("ready", nil)
	}
	return err
}

func minProbeTimeout(timeout time.Duration) time.Duration {
	const maxProbeTimeout = 750 * time.Millisecond
	if timeout <= 0 || timeout > maxProbeTimeout {
		return maxProbeTimeout
	}
	return timeout
}

func (s *Stack) drainProbeSignals() {
	for {
		select {
		case <-s.probeCh:
		default:
			return
		}
	}
}

func (s *Stack) drainHostEvents() {
	if s == nil || s.hostEvents == nil {
		return
	}
	for {
		select {
		case event, ok := <-s.hostEvents:
			if !ok {
				s.hostEvents = nil
				return
			}
			s.handleHostEvent(event)
		default:
			return
		}
	}
}

func (s *Stack) drainFailureSignals() {
	if s == nil || s.failureCh == nil {
		return
	}
	for {
		select {
		case <-s.failureCh:
		default:
			return
		}
	}
}

func (s *Stack) allowRequestFailureRecovery(opts Options, attempts *[]time.Time, cause, probeErr error) bool {
	return s.allowRequestFailureRecoveryAt(opts, attempts, cause, probeErr, optionsNow(opts))
}

func (s *Stack) allowRequestFailureRecoveryAt(opts Options, attempts *[]time.Time, cause, probeErr error, now time.Time) bool {
	window := opts.RequestFailureWindow
	if window <= 0 {
		window = opts.RestartWindow
	}
	if window <= 0 {
		window = time.Minute
	}
	cutoff := now.Add(-window)
	kept := (*attempts)[:0]
	for _, at := range *attempts {
		if at.After(cutoff) {
			kept = append(kept, at)
		}
	}
	*attempts = append(kept, now)
	max := opts.MaxRequestFailureRecoveries
	if max <= 0 {
		max = opts.MaxRestarts
	}
	if max <= 0 {
		max = 3
	}
	if !s.admitPersistentRecoveryBudget("request", now, max, window, cause, probeErr) {
		return false
	}
	if len(*attempts) <= max {
		return true
	}
	err := fmt.Errorf("request failure recovery budget exceeded: cause=%v probe=%v", cause, probeErr)
	s.setRecoveryStateNow("blocked", err)
	s.reportFatal(err)
	return false
}

func (s *Stack) recoverTunnel(opts Options, restarts *[]time.Time, cause error) bool {
	return s.recoverTunnelAt(opts, restarts, cause, optionsNow(opts))
}

func (s *Stack) recoverTunnelAt(opts Options, restarts *[]time.Time, cause error, now time.Time) bool {
	if s.stopped() {
		return false
	}
	if s.hostGateRequired() && !s.hostReadyNow() {
		if err := s.waitForHostReady(opts); err != nil {
			return !s.stopped()
		}
	}
	attemptCtx, cancel := context.WithCancel(context.Background())
	s.setRecoveryCancel(cancel)
	defer s.clearRecoveryCancel(cancel)
	if opts.RestartWindow <= 0 {
		opts.RestartWindow = time.Minute
	}
	cutoff := now.Add(-opts.RestartWindow)
	kept := (*restarts)[:0]
	for _, at := range *restarts {
		if at.After(cutoff) {
			kept = append(kept, at)
		}
	}
	*restarts = kept

	if s.waitStopped(opts.RestartBackoff) {
		return false
	}
	s.drainHostEvents()
	if s.hostGateRequired() && !s.hostReadyNow() {
		return !s.stopped()
	}
	recoveryGeneration := s.currentRecoveryGeneration()
	oldTun := s.currentTunnel()
	oldPort := s.CurrentSocksPort()
	s.setRecoveryStateNow("building-candidate", cause)

	// Build the candidate on a separate port while the current generation is
	// still serving. A failed candidate must not destroy a backend that may
	// still be usable, and a successful candidate must be ready before the
	// router is switched.
	candidatePort, reserve, terr := reservePort()
	if terr != nil {
		return s.recoveryCandidateFailed(opts, oldTun, fmt.Errorf("reserve candidate SOCKS port: %w", terr))
	}
	if candidatePort == oldPort {
		reserve.Close()
		candidatePort, reserve, terr = reservePort()
		if terr != nil {
			return s.recoveryCandidateFailed(opts, oldTun, fmt.Errorf("reserve distinct candidate SOCKS port: %w", terr))
		}
	}
	reserve.Close()

	tun, terr := newStackTunnel(s.Profile, candidatePort)
	if terr != nil {
		return s.recoveryCandidateFailed(opts, oldTun, fmt.Errorf("create candidate tunnel: %w", terr))
	}
	s.setCandidate(tun)
	candidateInstalled := false
	defer func() {
		s.clearCandidate(tun)
		if !candidateInstalled {
			_ = tun.Stop(opts.TunnelStopGrace)
		}
	}()
	if !s.recoveryGenerationCurrent(recoveryGeneration) {
		return s.recoveryCandidateFailed(opts, oldTun, errors.New("host state changed before candidate start"))
	}
	// Admission happens immediately before starting a candidate process. Host
	// wait, sleep, interface changes, and candidate construction do not consume
	// the durable restart budget.
	if opts.MaxRestarts >= 0 && !s.admitPersistentRecoveryBudget("restart", now, opts.MaxRestarts, opts.RestartWindow, cause, nil) {
		_ = tun.Stop(opts.TunnelStopGrace)
		return false
	}
	*restarts = append(*restarts, now)
	if opts.MaxRestarts >= 0 && len(*restarts) > opts.MaxRestarts {
		s.setRecoveryStateNow("blocked", cause)
		s.reportFatal(fmt.Errorf("ssh tunnel recovery budget exceeded: %w", cause))
		_ = tun.Stop(opts.TunnelStopGrace)
		return false
	}
	if terr := tun.Start(); terr != nil {
		_ = tun.Stop(opts.TunnelStopGrace)
		return s.recoveryCandidateFailed(opts, oldTun, fmt.Errorf("start candidate tunnel: %w", terr))
	}
	if s.stopped() {
		_ = tun.Stop(opts.TunnelStopGrace)
		return false
	}
	candidateAddr := fmt.Sprintf("127.0.0.1:%d", candidatePort)
	if terr := waitForTCPTunnelContext(attemptCtx, candidateAddr, opts.SocksReadyTimeout, tun); terr != nil {
		_ = tun.Stop(opts.TunnelStopGrace)
		return s.recoveryCandidateFailed(opts, oldTun, fmt.Errorf("candidate TCP readiness: %w", terr))
	}
	if terr := waitForSOCKS5Context(attemptCtx, candidateAddr, opts.SocksReadyTimeout, tun); terr != nil {
		_ = tun.Stop(opts.TunnelStopGrace)
		return s.recoveryCandidateFailed(opts, oldTun, fmt.Errorf("candidate SOCKS5 readiness: %w", terr))
	}
	if !s.recoveryGenerationCurrent(recoveryGeneration) {
		return s.recoveryCandidateFailed(opts, oldTun, errors.New("host state changed during candidate readiness"))
	}
	if opts.RouteProbe != nil {
		probeTimeout := minProbeTimeout(opts.SocksReadyTimeout)
		probeCtx, cancel := context.WithTimeout(attemptCtx, probeTimeout)
		targetHost, targetPort := routeTarget(s.Profile, opts)
		target := net.JoinHostPort(targetHost, strconv.Itoa(targetPort))
		probeErr := opts.RouteProbe(probeCtx, candidateAddr, targetHost, targetPort, probeTimeout)
		cancel()
		if s.recordRouteEvidence != nil {
			s.recordRouteEvidence(target, probeErr)
		}
		if probeErr != nil {
			_ = tun.Stop(opts.TunnelStopGrace)
			return s.recoveryCandidateFailed(opts, oldTun, fmt.Errorf("candidate route capability probe: %w", probeErr))
		}
	}
	if !s.recoveryGenerationCurrent(recoveryGeneration) {
		return s.recoveryCandidateFailed(opts, oldTun, errors.New("host state changed before generation switch"))
	}

	var oldGeneration uint64
	if s.router != nil {
		dialer, derr := localproxy.NewSOCKS5Dialer(candidateAddr, 10*time.Second)
		if derr != nil {
			_ = tun.Stop(opts.TunnelStopGrace)
			return s.recoveryCandidateFailed(opts, oldTun, fmt.Errorf("create candidate dialer: %w", derr))
		}
		var swapErr error
		s.setRecoveryStateNow("switching", nil)
		oldGeneration, swapErr = s.router.Swap(dialer)
		if swapErr != nil {
			_ = tun.Stop(opts.TunnelStopGrace)
			return s.recoveryCandidateFailed(opts, oldTun, fmt.Errorf("switch proxy generation: %w", swapErr))
		}
	}
	if s.setProxySocksAddr != nil {
		s.setProxySocksAddr(candidateAddr)
	}
	s.socksMu.Lock()
	s.SocksPort = candidatePort
	s.socksMu.Unlock()
	s.setTunnel(tun)
	candidateInstalled = true

	// The swap is complete before the old tunnel is stopped. Existing leased
	// connections get a bounded opportunity to drain on the old generation;
	// new requests use the candidate immediately.
	if oldGeneration != 0 && s.router != nil {
		ctx, cancel := context.WithTimeout(attemptCtx, opts.TunnelStopGrace)
		_ = s.router.Drain(ctx, oldGeneration)
		cancel()
	}
	if oldTun != nil && oldTun != tun {
		_ = oldTun.Stop(opts.TunnelStopGrace)
	}
	s.setRecoveryStateNow("ready", nil)
	return true
}

func routeTarget(profile config.Profile, opts Options) (string, int) {
	host := strings.TrimSpace(opts.RouteTargetHost)
	if host == "" {
		host = strings.TrimSpace(profile.RouteTargetHost)
	}
	if host == "" {
		host = profile.Host
	}
	port := opts.RouteTargetPort
	if port <= 0 {
		port = profile.RouteTargetPort
	}
	if port <= 0 {
		port = profile.Port
	}
	return host, port
}

// admitPersistentRecoveryBudget is the cross-process circuit breaker. It is
// intentionally independent from the monitor's local slice: a child daemon
// restart must not reset the number of recovery attempts that a supervisor
// sees. A persistence failure is fail-closed because otherwise the next
// process could restart indefinitely with no durable accounting.
func (s *Stack) admitPersistentRecoveryBudget(kind string, now time.Time, max int, window time.Duration, cause, probeErr error) bool {
	if s == nil || max < 0 {
		return true
	}
	if window <= 0 {
		window = time.Minute
	}

	s.budgetMu.Lock()
	budget := s.recoveryBudget
	var start *time.Time
	var attempts *int
	switch kind {
	case "request":
		start = &budget.RequestWindowStartedAt
		attempts = &budget.RequestAttempts
	default:
		start = &budget.RestartWindowStartedAt
		attempts = &budget.RestartAttempts
	}
	if start.IsZero() || now.Before(*start) || now.Sub(*start) > window {
		*start = now
		*attempts = 0
	}
	(*attempts)++
	if *attempts > max {
		budget.Blocked = true
		budget.BlockedAt = now
		reason := fmt.Sprintf("%s recovery budget exceeded: cause=%v probe=%v", kind, cause, probeErr)
		if kind == "request" {
			reason = fmt.Sprintf("request failure recovery budget exceeded: cause=%v probe=%v", cause, probeErr)
		}
		budget.LastReason = reason
	}
	s.recoveryBudget = budget
	persist := s.persistRecoveryBudget
	s.budgetMu.Unlock()

	if persist != nil {
		if err := persist(budget); err != nil {
			budget.Blocked = true
			budget.BlockedAt = now
			budget.LastReason = fmt.Sprintf("persist recovery budget: %v", err)
			s.budgetMu.Lock()
			s.recoveryBudget = budget
			s.budgetMu.Unlock()
		}
	}
	if !budget.Blocked {
		return true
	}
	err := fmt.Errorf("%w: %s", ErrRecoveryBudgetBlocked, budget.LastReason)
	s.setRecoveryStateNow("blocked", err)
	s.reportFatal(err)
	return false
}

func (s *Stack) recoveryCandidateFailed(_ Options, _ tunnelProcess, err error) bool {
	s.setRecoveryStateNow("degraded", err)
	if s.stopped() {
		return false
	}
	// Keep monitoring after a candidate failure even when the previous backend
	// has already exited. A network can come back between two candidate
	// attempts; making the first failed replacement fatal would turn a
	// recoverable outage into a permanent proxy failure. recoverTunnel applies
	// the bounded restart budget before each attempt, so this remains finite
	// and does not create an unbounded restart storm.
	return true
}

func (s *Stack) currentTunnel() tunnelProcess {
	s.tunnelMu.Lock()
	defer s.tunnelMu.Unlock()
	return s.tunnel
}

func (s *Stack) setTunnel(tun tunnelProcess) {
	s.tunnelMu.Lock()
	s.tunnel = tun
	s.tunnelMu.Unlock()
}

func (s *Stack) setCandidate(tun tunnelProcess) {
	if s == nil {
		return
	}
	s.recoveryMu.Lock()
	s.candidate = tun
	s.recoveryMu.Unlock()
	s.publishHostState(time.Time{}, time.Time{})
}

func (s *Stack) clearCandidate(tun tunnelProcess) {
	if s == nil {
		return
	}
	s.recoveryMu.Lock()
	if s.candidate == tun {
		s.candidate = nil
	}
	s.recoveryMu.Unlock()
	s.publishHostState(time.Time{}, time.Time{})
}

func (s *Stack) currentCandidate() tunnelProcess {
	if s == nil {
		return nil
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	return s.candidate
}

func (s *Stack) setRecoveryCancel(cancel context.CancelFunc) {
	if s == nil {
		return
	}
	s.recoveryMu.Lock()
	s.recoveryCancel = cancel
	s.recoveryMu.Unlock()
}

func (s *Stack) clearRecoveryCancel(cancel context.CancelFunc) {
	if s == nil {
		return
	}
	s.recoveryMu.Lock()
	if s.recoveryCancel != nil {
		// A CancelFunc is not comparable. Clearing the current operation is safe
		// because the monitor owns recovery serialization; Close can still call
		// the function while this operation is unwinding.
		s.recoveryCancel = nil
	}
	s.recoveryMu.Unlock()
}

func (s *Stack) cancelRecovery() {
	if s == nil {
		return
	}
	s.recoveryMu.Lock()
	cancel := s.recoveryCancel
	s.recoveryCancel = nil
	s.recoveryMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Stack) stopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

func (s *Stack) waitStopped(delay time.Duration) bool {
	if delay <= 0 {
		return s.stopped()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-s.stopCh:
		return true
	case <-timer.C:
		return false
	}
}

func (s *Stack) reportFatal(err error) {
	if err == nil || s.stopped() {
		return
	}
	select {
	case s.fatalCh <- err:
	case <-s.stopCh:
	}
}

func newTunnel(profile config.Profile, socksPort int) (*ssh.Tunnel, error) {
	return ssh.NewTunnel(ssh.TunnelConfig{
		Host:         profile.Host,
		Port:         profile.Port,
		User:         profile.User,
		SocksPort:    socksPort,
		ExtraArgs:    profile.SSHArgs,
		ConfigTarget: ssh.ArgsUseConfigFile(profile.SSHArgs),
		BatchMode:    true,
		Stdout:       os.Stderr,
		Stderr:       os.Stderr,
	})
}

var newStackTunnel = func(profile config.Profile, socksPort int) (tunnelProcess, error) {
	return newTunnel(profile, socksPort)
}

func waitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for %s: %w", addr, lastErr)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

func waitForTCPTunnel(addr string, timeout time.Duration, tun tunnelProcess) error {
	return waitForTCPTunnelContext(context.Background(), addr, timeout, tun)
}

func waitForTCPTunnelContext(ctx context.Context, addr string, timeout time.Duration, tun tunnelProcess) error {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if tun != nil {
			select {
			case <-tun.Done():
				return fmt.Errorf("ssh tunnel exited before SOCKS ready: %w", tun.Wait())
			default:
			}
		}

		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		lastErr = err
		delay := 100 * time.Millisecond
		if remaining := time.Until(deadline); remaining < delay {
			delay = remaining
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for %s: %w", addr, lastErr)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

func waitForSOCKS5(addr string, timeout time.Duration, tun tunnelProcess) error {
	return waitForSOCKS5Context(context.Background(), addr, timeout, tun)
}

func waitForSOCKS5Context(ctx context.Context, addr string, timeout time.Duration, tun tunnelProcess) error {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if tun != nil {
			select {
			case <-tun.Done():
				return fmt.Errorf("ssh tunnel exited before SOCKS5 handshake: %w", tun.Wait())
			default:
			}
		}
		remaining := time.Until(deadline)
		if remaining > 200*time.Millisecond {
			remaining = 200 * time.Millisecond
		}
		probeCtx, cancel := context.WithTimeout(ctx, remaining)
		if err := localproxy.ProbeSOCKS5(probeCtx, addr, remaining); err == nil {
			cancel()
			return nil
		} else {
			cancel()
			lastErr = err
		}
		delay := 50 * time.Millisecond
		if left := time.Until(deadline); left < delay {
			delay = left
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	if lastErr != nil {
		return fmt.Errorf("timeout waiting for SOCKS5 handshake on %s: %w", addr, lastErr)
	}
	return fmt.Errorf("timeout waiting for SOCKS5 handshake on %s", addr)
}

func pickFreePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return 0, err
	}
	return parsePort(portStr)
}

func parsePort(s string) (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:"+s)
	if err != nil {
		return 0, err
	}
	return addr.Port, nil
}
