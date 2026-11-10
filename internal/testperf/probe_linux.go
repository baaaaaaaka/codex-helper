// Package testperf measures opt-in synthetic Linux persistence tests. Product
// packages must not import it; it owns no storage schema or application state.
package testperf

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

// RunFresh separates the measured process from fixture construction, so its
// high-water RSS does not include buffers used only to create the legacy input.
func RunFresh(t *testing.T, path string) {
	t.Helper()
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^"+t.Name()+"$", "-test.v", "-test.count=1", "-test.timeout=10m")
	for _, item := range os.Environ() {
		if !strings.HasPrefix(item, "CXP_SQLITE_PERF_INPUT=") {
			cmd.Env = append(cmd.Env, item)
		}
	}
	cmd.Env = append(cmd.Env, "CXP_SQLITE_PERF_INPUT="+path)
	output, err := cmd.CombinedOutput()
	t.Logf("fresh measurement process:\n%s", output)
	if err != nil {
		t.Fatal(err)
	}
}

// Measure reports phase CPU, cumulative allocations, process high-water RSS,
// proc I/O and sampled directory peak. RSS includes fixture construction;
// sampling is a lower bound for short-lived disk peaks, not a capacity ceiling.
func Measure(t testing.TB, dir, name string, rows int, work func()) {
	t.Helper()
	var peak atomic.Int64
	sample := func() {
		var total int64
		_ = filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
			if err == nil && !entry.IsDir() {
				if info, err := entry.Info(); err == nil {
					total += info.Size()
				}
			}
			return nil
		})
		for old := peak.Load(); total > old; old = peak.Load() {
			if peak.CompareAndSwap(old, total) {
				break
			}
		}
	}
	done, stopped := make(chan struct{}), make(chan struct{})
	sample()
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sample()
			case <-done:
				return
			}
		}
	}()
	defer func() { close(done); <-stopped }()
	runtime.GC()
	var before, after runtime.MemStats
	var cpu0, cpu1 syscall.Rusage
	runtime.ReadMemStats(&before)
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &cpu0)
	io0, _ := os.ReadFile("/proc/self/io")
	fmt.Printf("CXP_PERF_BEGIN %s rows=%d\n", name, rows)
	start := time.Now()
	work()
	wall := time.Since(start)
	fmt.Printf("CXP_PERF_END %s\n", name)
	runtime.ReadMemStats(&after)
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &cpu1)
	io1, _ := os.ReadFile("/proc/self/io")
	status, _ := os.ReadFile("/proc/self/status")
	sample()
	t.Logf("phase=%s rows=%d wall=%s cpu_user_us=%d cpu_system_us=%d allocated_bytes=%d allocs=%d maxrss_kib=%d sampled_peak_directory_bytes=%d io_before=[%s] io_after=[%s]", name, rows, wall, cpu1.Utime.Nano()/1000-cpu0.Utime.Nano()/1000, cpu1.Stime.Nano()/1000-cpu0.Stime.Nano()/1000, after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, cpu1.Maxrss, peak.Load(), strings.Join(strings.Fields(string(io0)), " "), strings.Join(strings.Fields(string(io1)), " "))
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "VmRSS:") || strings.HasPrefix(line, "VmHWM:") {
			t.Log(line)
		}
	}
}
