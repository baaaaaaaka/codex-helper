package cli

// The single test seam of the Teams helper (docs/teams-e2e-test-seam.md).
//
// The black-box E2E suite (internal/e2e) runs the release `cxp` binary
// against a fake Microsoft Graph, a fake Codex app-server and the local
// file system. Everything except the network endpoints of Microsoft and
// the wall clock is reachable the way a user reaches it (flags, config,
// files, CLI). This file is the only place where cxp behaves differently
// for a test, and only when CXP_TEST_ONLY_SEAM is set:
//
//   - endpoint: every HTTP request cxp's Teams HTTP client makes (Graph,
//     login.microsoftonline.com, SharePoint upload URLs, the Workflow
//     webhook) is sent to this loopback URL instead. The request is not
//     otherwise changed: method, path, query, headers and body are the
//     ones production would send; the original scheme and host travel in
//     X-Cxp-Test-Original-Host / X-Cxp-Test-Original-Scheme.
//   - instance: an opaque tag sent as X-Cxp-Test-Instance on every such
//     request, so the fake can tell helper instances apart.
//   - timing: "production" (default) or "fast:<base>", which shortens the
//     poll and mirror cadences to multiples of base (engine.FastTiming),
//     plus "timing.<Name>=<duration>" for the user-visible waits listed in
//     teamsTestSeamTimingKnobs.
//   - hostname: the host identity used for the store owner, per-host files
//     and the default machine label, so two helpers in one test container
//     act as two hosts sharing one home.
//
// The seam must never change what cxp selects, sends, renders or decides:
// only where the requests go, how long the waits are, and which host name
// it believes it runs on. It is rejected (cxp refuses to run) unless the
// endpoint is a loopback http URL, so it cannot redirect a real helper to
// another server, and unknown keys are rejected rather than ignored.

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

const (
	// envTeamsTestSeam is the only switch of the seam. Its name says what
	// it is; there is no other way to enable it.
	envTeamsTestSeam = "CXP_TEST_ONLY_SEAM"

	teamsTestSeamHeaderHost     = "X-Cxp-Test-Original-Host"
	teamsTestSeamHeaderScheme   = "X-Cxp-Test-Original-Scheme"
	teamsTestSeamHeaderInstance = "X-Cxp-Test-Instance"
)

// teamsTestSeam is the parsed CXP_TEST_ONLY_SEAM.
type teamsTestSeam struct {
	endpoint *url.URL
	instance string
	hostname string
	// fast is the base of the fast timing profile (0 = production timing).
	fast      time.Duration
	overrides map[string]time.Duration
}

// teamsTestSeamTimingKnobs are the waits a scenario may shorten beyond the
// fast profile. Each is a user-visible duration of the helper; the names
// are the seam's contract (a rewrite maps them to its own settings).
var teamsTestSeamTimingKnobs = map[string]func(*engine.Timing, time.Duration){
	// SilenceHint, SilenceStuck: the "still working" hint and the "may be
	// stuck" notice of a silent turn (7 min, 15 min). A silent turn is never
	// stopped (F-TRN-18), so there is no stall knob.
	"SilenceHint":  func(t *engine.Timing, d time.Duration) { t.SilenceHint = d },
	"SilenceStuck": func(t *engine.Timing, d time.Duration) { t.SilenceStuck = d },
	// DuplicateFor: the window in which a repeated prompt is ignored (3 min).
	"DuplicateFor": func(t *engine.Timing, d time.Duration) { t.DuplicateFor = d },
	// Heartbeat: the store owner's heartbeat (30 s), which bounds how soon
	// a kill-switch file stops a running helper.
	"Heartbeat": func(t *engine.Timing, d time.Duration) { t.Heartbeat = d },
	// CapacityRetry, CapacityBackoff: how long a turn refused for model
	// capacity is retried (5 min) and the first retry delay (10 s, doubling).
	"CapacityRetry":   func(t *engine.Timing, d time.Duration) { t.CapacityRetry = d },
	"CapacityBackoff": func(t *engine.Timing, d time.Duration) { t.CapacityBackoff = d },
	// ParkAfter: how long a work chat stays idle before it is parked with
	// its freeze notice (48 h).
	"ParkAfter": func(t *engine.Timing, d time.Duration) { t.ParkAfter = d },
	// RefusalRecheck (not an engine setting): how often a standby reads
	// the lease (20 s) and a service-run helper refused the store's write
	// lock re-checks it (45 s).
	"RefusalRecheck": nil,
	// Takeover (not an engine setting): how long a standby waits for the
	// owner to renew its lease before it takes over (90 s); the lease's
	// other intervals scale with it (internal/core/lease.Scaled).
	"Takeover": nil,
}

var errTeamsTestSeamInvalid = errors.New(envTeamsTestSeam + " is set but invalid; it is a test-only switch: unset it")

// loadTeamsTestSeam parses CXP_TEST_ONLY_SEAM. It returns nil when the
// variable is unset and an error when it is set but not acceptable.
func loadTeamsTestSeam() (*teamsTestSeam, error) {
	raw, ok := os.LookupEnv(envTeamsTestSeam)
	if !ok {
		return nil, nil
	}
	return parseTeamsTestSeam(raw)
}

func parseTeamsTestSeam(raw string) (*teamsTestSeam, error) {
	bad := func(format string, args ...any) (*teamsTestSeam, error) {
		return nil, fmt.Errorf("%w: %s", errTeamsTestSeamInvalid, fmt.Sprintf(format, args...))
	}
	s := &teamsTestSeam{overrides: map[string]time.Duration{}}
	for _, item := range strings.Split(raw, ";") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key, val, found := strings.Cut(item, "=")
		key, val = strings.TrimSpace(key), strings.TrimSpace(val)
		if !found || val == "" {
			return bad("%q is not key=value", item)
		}
		switch {
		case key == "endpoint":
			u, err := url.Parse(val)
			if err != nil {
				return bad("endpoint: %v", err)
			}
			if u.Scheme != "http" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || !teamsTestSeamLoopback(u.Hostname()) || u.Port() == "" {
				return bad("endpoint %q is not http://<loopback address>:<port>", val)
			}
			s.endpoint = &url.URL{Scheme: u.Scheme, Host: u.Host}
		case key == "instance":
			s.instance = val
		case key == "hostname":
			s.hostname = val
		case key == "timing":
			switch {
			case val == "production":
				s.fast = 0
			case strings.HasPrefix(val, "fast:"):
				d, err := time.ParseDuration(strings.TrimPrefix(val, "fast:"))
				if err != nil || d <= 0 {
					return bad("timing %q: want fast:<positive duration>", val)
				}
				s.fast = d
			default:
				return bad("timing %q: want production or fast:<duration>", val)
			}
		case strings.HasPrefix(key, "timing."):
			name := strings.TrimPrefix(key, "timing.")
			if _, known := teamsTestSeamTimingKnobs[name]; !known {
				return bad("unknown timing knob %q (known: %s)", name, strings.Join(teamsTestSeamKnobNames(), ", "))
			}
			d, err := time.ParseDuration(val)
			if err != nil || d <= 0 {
				return bad("%s: want a positive duration, got %q", key, val)
			}
			s.overrides[name] = d
		default:
			return bad("unknown key %q", key)
		}
	}
	if s.endpoint == nil {
		return bad("no endpoint (the seam only works together with a fake loopback endpoint)")
	}
	return s, nil
}

func teamsTestSeamKnobNames() []string {
	names := make([]string, 0, len(teamsTestSeamTimingKnobs))
	for n := range teamsTestSeamTimingKnobs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func teamsTestSeamLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// timing returns the engine timing and the account read rate (0 = the
// engine's default) of the seam's profile.
func (s *teamsTestSeam) timing() (engine.Timing, float64) {
	t, rate := engine.ProductionTiming(), 0.0
	if s.fast > 0 {
		t = engine.FastTiming(s.fast)
		rate = 4 * float64(time.Second) / float64(s.fast)
	}
	for name, d := range s.overrides {
		if set := teamsTestSeamTimingKnobs[name]; set != nil {
			set(&t, d)
		}
	}
	return t, rate
}

// refusalRecheck is the RefusalRecheck knob (0 = unchanged).
func (s *teamsTestSeam) refusalRecheck() time.Duration { return s.overrides["RefusalRecheck"] }

// httpClient is the Teams HTTP client of the seam: the production client
// settings with every request sent to the endpoint.
func (s *teamsTestSeam) httpClient() *http.Client {
	c := newTeamsDirectHTTPClient()
	c.Transport = &teamsTestSeamTransport{seam: s, inner: c.Transport}
	return c
}

// describe is the one log line a helper prints when the seam is active.
func (s *teamsTestSeam) describe() string {
	return fmt.Sprintf("%s is set: Microsoft endpoints are served by %s (test only)", envTeamsTestSeam, s.endpoint)
}

type teamsTestSeamTransport struct {
	seam  *teamsTestSeam
	inner http.RoundTripper
}

func (t *teamsTestSeamTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	out := req.Clone(req.Context())
	u := *req.URL
	u.Scheme, u.Host = t.seam.endpoint.Scheme, t.seam.endpoint.Host
	out.URL = &u
	out.Host = t.seam.endpoint.Host
	out.Header.Set(teamsTestSeamHeaderHost, strings.ToLower(req.URL.Hostname()))
	out.Header.Set(teamsTestSeamHeaderScheme, req.URL.Scheme)
	if t.seam.instance != "" {
		out.Header.Set(teamsTestSeamHeaderInstance, t.seam.instance)
	}
	return t.inner.RoundTrip(out)
}

// teamsHostname is the host identity of this helper: os.Hostname(), or the
// seam's hostname.
func teamsHostname() (string, error) {
	seam, err := loadTeamsTestSeam()
	if err != nil {
		return "", err
	}
	if seam != nil && seam.hostname != "" {
		return seam.hostname, nil
	}
	return os.Hostname()
}
