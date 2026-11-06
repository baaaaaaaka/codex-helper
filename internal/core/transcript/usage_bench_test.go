//go:build unix

// The benchmark measures CPU time with getrusage(2), which exists only on
// Unix; both tests here are skipped unless their corpus is named.

package transcript

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestUsageBenchCorpus measures `helper stats` calls on a sessions
// directory (F-STA-1 performance): the first (cold) call, a repeated call
// with nothing changed, a control-chat call over many chats, and a call
// after an append. It is skipped unless CXP_STATS_BENCH_DIR names the
// directory (read only; the append case works on a copy under TempDir).
func TestUsageBenchCorpus(t *testing.T) {
	dir := os.Getenv("CXP_STATS_BENCH_DIR")
	if dir == "" {
		t.Skip("CXP_STATS_BENCH_DIR not set")
	}
	type rollout struct {
		path, thread string
		size         int64
		mod          time.Time
	}
	var all []rollout
	var total int64
	days, err := DayDirs(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range days {
		ents, _ := os.ReadDir(d.Path)
		for _, e := range ents {
			n := e.Name()
			if !strings.HasPrefix(n, "rollout-") || !strings.HasSuffix(n, ".jsonl") || threadOfName(n) == "" {
				continue
			}
			fi, err := e.Info()
			if err != nil {
				continue
			}
			all = append(all, rollout{filepath.Join(d.Path, n), threadOfName(n), fi.Size(), fi.ModTime()})
			total += fi.Size()
		}
	}
	if len(all) == 0 {
		t.Fatal("no rollouts")
	}
	t.Logf("corpus: %d rollouts, %.1f GiB, %d day dirs", len(all), float64(total)/(1<<30), len(days))
	sort.Slice(all, func(i, j int) bool { return all[i].size < all[j].size })
	median, p90, largest := all[len(all)/2], all[len(all)*9/10], all[len(all)-1]
	byMod := append([]rollout(nil), all...)
	sort.Slice(byMod, func(i, j int) bool { return byMod[i].mod.After(byMod[j].mod) })
	var recent [][]string
	for _, r := range byMod[:min(50, len(byMod))] {
		recent = append(recent, []string{r.thread})
	}

	cpu := func() time.Duration {
		var ru syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
		return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	}
	read := func(c *UsageCache) (n int, b int64) {
		for _, f := range c.c.Files {
			if f.St != nil {
				n++
				b += f.Off
			}
		}
		return
	}
	measure := func(name string, c *UsageCache, chats [][]string) {
		t.Helper()
		n0, b0 := read(c)
		c0, w0 := cpu(), time.Now()
		out, err := c.Usage(context.Background(), chats)
		wall, used := time.Since(w0), cpu()-c0
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		n1, b1 := read(c)
		var subs int
		var tok int64
		for _, u := range out {
			subs += len(u.Subagents)
			for _, m := range u.Main {
				tok += m.Stats.Overall().Total
			}
		}
		t.Logf("%-34s wall %8.1f ms  cpu %8.1f ms  rollouts read %4d (%7.1f MiB)  subagents %3d  main tokens %d",
			name, float64(wall.Microseconds())/1000, float64(used.Microseconds())/1000, n1-n0, float64(b1-b0)/(1<<20), subs, tok)
	}
	one := func(r rollout) [][]string { return [][]string{{r.thread}} }

	t.Logf("median rollout %.1f MiB, p90 %.1f MiB, largest %.1f MiB", float64(median.size)/(1<<20), float64(p90.size)/(1<<20), float64(largest.size)/(1<<20))
	c := NewUsageCache(dir)
	measure("cold index + median chat", c, one(median))
	measure("median chat again (no change)", c, one(median))
	measure("p90 chat cold", c, one(p90))
	measure("p90 chat again", c, one(p90))
	measure("largest chat cold", c, one(largest))
	var largestTotal int64
	if f := c.c.Files[largest.path]; f != nil && f.St != nil {
		st, _ := f.St.stats()
		largestTotal = st.Overall().Total
	}
	measure("largest chat again", c, one(largest))
	measure("control: 50 recent chats cold", c, recent)
	measure("control: 50 recent chats again", c, recent)
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	n, b := read(c)
	t.Logf("cache: %d rollouts parsed (%.1f GiB), %d entries; heap in use %.1f MiB", n, float64(b)/(1<<30), len(c.c.Files), float64(ms.HeapInuse)/(1<<20))

	// v0.1.20 read the whole linked rollout on every call.
	c0, w0 := cpu(), time.Now()
	fh, err := os.Open(largest.path)
	if err != nil {
		t.Fatal(err)
	}
	one20, _ := ParseTokenStats(fh)
	fh.Close()
	t.Logf("%-34s wall %8.1f ms  cpu %8.1f ms  total %d (cache: %d)", "v0.1.20 one-shot parse of largest", float64(time.Since(w0).Microseconds())/1000, float64((cpu()-c0).Microseconds())/1000,
		one20.Overall().Total, largestTotal)

	// An append to a live rollout: only the new bytes are read.
	tmp := t.TempDir()
	rel, _ := filepath.Rel(dir, p90.path)
	dst := filepath.Join(tmp, rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(p90.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatal(err)
	}
	ci := NewUsageCache(tmp)
	measure("copy of p90: cold", ci, one(p90))
	var app []string
	native := TokenUsage{Input: 1 << 30, Total: 1 << 30}
	for len(strings.Join(app, "\n")) < 1<<20 {
		last := TokenUsage{Input: 1000, Cached: 600, Output: 50, Reasoning: 5, Total: 1050}
		native = add(native, last)
		app = append(app, lineTool(len(app), 4000), lineCount(len(app)+1, native, last))
	}
	writeUsageLines(t, dst, app, true)
	measure("copy of p90: after 1 MiB append", ci, one(p90))
	measure("copy of p90: again", ci, one(p90))
}

// TestUsageBenchParse parses CXP_STATS_BENCH_FILE once (profiling aid).
func TestUsageBenchParse(t *testing.T) {
	p := os.Getenv("CXP_STATS_BENCH_FILE")
	if p == "" {
		t.Skip("CXP_STATS_BENCH_FILE not set")
	}
	fh, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	w0 := time.Now()
	st, err := ParseTokenStats(fh)
	t.Logf("parse: %v, total %d, events %d, resets %d, err %v", time.Since(w0), st.Info.Total.Total, st.UsageEvents, st.Resets, err)
}
