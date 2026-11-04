package migrate

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// Entity counts one source entity.
type Entity struct {
	Name     string
	Source   int
	Imported int
	Outcomes map[string]int // e.g. "unknown", "held", "dropped: terminal skipped"
	Failed   []string       // ids of rows that failed to parse
}

// Check is one validation result.
type Check struct {
	Name   string
	OK     bool
	Detail string
}

// Report is the result of one conversion.
type Report struct {
	SourceDir string
	Version   Version
	OutPath   string
	DryRun    bool
	Started   time.Time
	Wall      time.Duration
	entities  []*Entity
	Buckets   map[string]int // transcript source classification
	Checks    []Check
	Notes     []string
}

func newReport() *Report { return &Report{Buckets: map[string]int{}} }

// E returns (creating) the entity counter for name.
func (r *Report) E(name string) *Entity {
	for _, e := range r.entities {
		if e.Name == name {
			return e
		}
	}
	e := &Entity{Name: name, Outcomes: map[string]int{}}
	r.entities = append(r.entities, e)
	return e
}

// Entities returns the entity counters in creation order.
func (r *Report) Entities() []*Entity { return r.entities }

func (r *Report) note(format string, a ...any) { r.Notes = append(r.Notes, fmt.Sprintf(format, a...)) }

// OK reports whether every validation check passed.
func (r *Report) OK() bool {
	for _, c := range r.Checks {
		if !c.OK {
			return false
		}
	}
	return len(r.Checks) > 0
}

// WriteText writes the human-readable report.
func (r *Report) WriteText(w io.Writer) {
	fmt.Fprintf(w, "cxp teams migrate report\n")
	fmt.Fprintf(w, "source:   %s (%s)\n", r.SourceDir, r.Version)
	fmt.Fprintf(w, "target:   %s\n", r.OutPath)
	fmt.Fprintf(w, "dry run:  %v\n", r.DryRun)
	fmt.Fprintf(w, "started:  %s\n", r.Started.Format(time.RFC3339))
	fmt.Fprintf(w, "wall:     %s\n\n", r.Wall.Round(time.Millisecond))
	fmt.Fprintf(w, "%-24s %9s %9s  %s\n", "entity", "source", "imported", "outcomes")
	for _, e := range r.entities {
		fmt.Fprintf(w, "%-24s %9d %9d  %s\n", e.Name, e.Source, e.Imported, outcomes(e.Outcomes))
		if len(e.Failed) > 0 {
			ids := e.Failed
			more := ""
			if len(ids) > 20 {
				more = fmt.Sprintf(" (+%d more)", len(ids)-20)
				ids = ids[:20]
			}
			fmt.Fprintf(w, "%-24s failed to parse: %s%s\n", "", strings.Join(ids, ", "), more)
		}
	}
	fmt.Fprintf(w, "\ntranscript sources:\n")
	for _, k := range sortedKeys(r.Buckets) {
		fmt.Fprintf(w, "  %-58s %7d\n", k, r.Buckets[k])
	}
	fmt.Fprintf(w, "\nchecks:\n")
	for _, c := range r.Checks {
		st := "ok  "
		if !c.OK {
			st = "FAIL"
		}
		fmt.Fprintf(w, "  [%s] %s", st, c.Name)
		if c.Detail != "" {
			fmt.Fprintf(w, ": %s", c.Detail)
		}
		fmt.Fprintln(w)
	}
	if len(r.Notes) > 0 {
		fmt.Fprintf(w, "\nnotes:\n")
		for _, n := range r.Notes {
			fmt.Fprintf(w, "  - %s\n", n)
		}
	}
}

func outcomes(m map[string]int) string {
	var parts []string
	for _, k := range sortedKeys(m) {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, " ")
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
