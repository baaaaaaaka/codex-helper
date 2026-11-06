package harness

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// EnvSeam is cxp's single test seam (docs/teams-e2e-test-seam.md): one
// environment variable that sends cxp's Microsoft traffic to the fake
// Graph, tags it with the instance, compresses the timing and may give the
// helper another host name. The suite reaches cxp through nothing else
// that a user could not also use.
const EnvSeam = "CXP_TEST_ONLY_SEAM"

// Seam is what a scenario may set through the seam besides the endpoint
// and the instance tag, which the harness always sets.
type Seam struct {
	// ProductionTiming runs cxp with its production timing (cost
	// scenarios); otherwise the fast profile with the scenario interval.
	ProductionTiming bool
	// Knobs shorten named user-visible waits, e.g. "SilenceHint": "3s".
	// The names are the seam's (see cxp's teamsTestSeamTimingKnobs).
	Knobs map[string]string
	// Hostname makes the helper believe it runs on this host (two helpers
	// sharing one home act as two hosts).
	Hostname string
	// CPUProfile is a directory for the helper's one-minute CPU profiles
	// (diagnostic runs only).
	CPUProfile string
}

// With returns s with the fields of o that are set.
func (s Seam) With(o Seam) Seam {
	out := Seam{ProductionTiming: s.ProductionTiming || o.ProductionTiming, Hostname: s.Hostname, CPUProfile: s.CPUProfile, Knobs: map[string]string{}}
	for k, v := range s.Knobs {
		out.Knobs[k] = v
	}
	for k, v := range o.Knobs {
		out.Knobs[k] = v
	}
	if o.Hostname != "" {
		out.Hostname = o.Hostname
	}
	if o.CPUProfile != "" {
		out.CPUProfile = o.CPUProfile
	}
	return out
}

// Value renders CXP_TEST_ONLY_SEAM for one instance.
func (s Seam) Value(endpoint, instance string, interval time.Duration) string {
	parts := []string{"endpoint=" + endpoint, "instance=" + instance}
	if s.ProductionTiming {
		parts = append(parts, "timing=production")
	} else {
		parts = append(parts, "timing=fast:"+interval.String())
	}
	names := make([]string, 0, len(s.Knobs))
	for k := range s.Knobs {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		parts = append(parts, fmt.Sprintf("timing.%s=%s", k, s.Knobs[k]))
	}
	if s.Hostname != "" {
		parts = append(parts, "hostname="+s.Hostname)
	}
	if s.CPUProfile != "" {
		parts = append(parts, "cpuprofile="+s.CPUProfile)
	}
	return strings.Join(parts, ";")
}
