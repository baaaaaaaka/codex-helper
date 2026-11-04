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
	Shared    []SharedThread
	Resume    []ResumeRow
	Backlog   Backlog
	Scan      struct {
		Files, Errors int
		Bytes         int64
		Wall          time.Duration
	}
}

// SharedThread is one conversation that lost a shared Codex thread to the
// conversation with the most recent activity.
type SharedThread struct {
	Thread                  string
	Winner, Loser           string // conversation labels
	WinnerLast, LoserLast   int64  // newest activity (unix ms, 0: none)
	LoserBefore, LoserAfter string // conversation state
}

// ResumeRow explains the resume position of one tracked rollout whose old
// positions disagree or lack proof.
type ResumeRow struct {
	Path, Conv, Case string
	Size             int64
	Checkpoint       int64 // furthest import checkpoint offset (0: none)
	Cutoff           int64 // execution anchor cutoff (0: none)
	Watcher          int64 // history_watch offset (0: none)
	Proof            int64 // end of the last record proven delivered (0: none)
	Resume           int64
	GapPrompts       int // prompts/finals in [Proof, max old position) that are mirrored
	GapFinals        int
	BacklogPrompts   int // prompts/finals from Resume to EOF (-1: not counted)
	BacklogFinals    int
	SkipOld          int // unproven records older than 120 days: not posted, resume moved past them
	SkipClosed       int // records of a closed conversation: no catch-up
}

// Backlog totals the first-start catch-up of all tracked rollouts.
type Backlog struct {
	Counted                              bool
	Sources                              int
	Bytes                                int64 // Resume..EOF
	Prompts, Finals                      int
	Gap                                  int // records behind an old furthest position without proof: mirrored
	NoProofSources                       int
	NoProofPrompts, NoProofFinals        int
	SkippedOld                           int // records not posted: older than the 120-day window
	SkippedClosed                        int // records not posted: conversation closed
	ClosedSources                        int
	NoPositionSources, NoPositionRecords int // linked rollouts the old core never positioned: kept at EOF
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
	r.writeResume(w)
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

func (r *Report) writeResume(w io.Writer) {
	if r.Scan.Files > 0 {
		fmt.Fprintf(w, "\nproof scan (read-only): %d files, %.2f GB read, %d errors, %s\n",
			r.Scan.Files, float64(r.Scan.Bytes)/1e9, r.Scan.Errors, r.Scan.Wall.Round(time.Millisecond))
	}
	if b := r.Backlog; b.Counted {
		fmt.Fprintf(w, "first-start catch-up (120-day window, uncapped): %d sources, %.2f GB, %d prompts + %d finals\n",
			b.Sources, float64(b.Bytes)/1e9, b.Prompts, b.Finals)
		fmt.Fprintf(w, "  of which behind an old position without proof (the gap): %d\n", b.Gap)
		fmt.Fprintf(w, "  of which in no-proof sources: %d sources, %d prompts + %d finals\n",
			b.NoProofSources, b.NoProofPrompts, b.NoProofFinals)
		fmt.Fprintf(w, "not posted (rollouts stay on disk): %d records older than 120 days; %d records of %d closed conversations' sources\n",
			b.SkippedOld, b.SkippedClosed, b.ClosedSources)
		fmt.Fprintf(w, "linked rollouts with no old position and no proof, kept at EOF: %d (%d prompts/finals not caught up)\n", b.NoPositionSources, b.NoPositionRecords)
	}
	if len(r.Shared) > 0 {
		fmt.Fprintf(w, "\nshared threads (winner keeps the rollout):\n")
		for _, s := range r.Shared {
			fmt.Fprintf(w, "  %s  winner %s (last %s)  loser %s (last %s, %s -> %s)\n", s.Thread,
				s.Winner, msTime(s.WinnerLast), s.Loser, msTime(s.LoserLast), s.LoserBefore, s.LoserAfter)
		}
	}
	if len(r.Resume) > 0 {
		fmt.Fprintf(w, "\nresume positions (C checkpoint, A anchor cutoff, W watcher, P proof, R resume; gap = prompts/finals in [R, max(C,A,W)) mirrored; skip-old = unproven records older than 120 days passed over; closed rows stay at C/A/W):\n")
		for _, x := range r.Resume {
			bl := "-"
			if x.BacklogPrompts >= 0 {
				bl = fmt.Sprintf("%d/%d", x.BacklogPrompts, x.BacklogFinals)
			}
			fmt.Fprintf(w, "  %-15s %-6s C=%d A=%d W=%d P=%d R=%d size=%d gap=%d/%d backlog=%s skip-old=%d skip-closed=%d %s\n",
				x.Case, x.Conv, x.Checkpoint, x.Cutoff, x.Watcher, x.Proof, x.Resume, x.Size,
				x.GapPrompts, x.GapFinals, bl, x.SkipOld, x.SkipClosed, x.Path)
		}
	}
}

func msTime(ms int64) string {
	if ms == 0 {
		return "none"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}
