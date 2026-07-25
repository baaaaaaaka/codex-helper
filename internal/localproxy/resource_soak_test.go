package localproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestHTTPProxyRepeatedStartCloseResourceBudget(t *testing.T) {
	const (
		batches          = 8
		cyclesPerBatch   = 32
		maxHeapObjGrowth = 192
	)

	// A short repeated workload plus a forced GC boundary is a higher-signal
	// leak detector than an RSS assertion: RSS legitimately retains allocator
	// pages, while live HeapObjects should settle when proxy-owned references
	// are released. Eight samples also distinguish one noisy GC from a linear
	// leak without making CI a long-running soak job.
	settleProxyResourceRuntime()
	baseline := readProxyResourceSample()
	samples := make([]proxyResourceSnapshot, 0, batches)

	for batch := 0; batch < batches; batch++ {
		for round := 0; round < cyclesPerBatch; round++ {
			exerciseProxyResourceRound(t, batch*cyclesPerBatch+round)
		}
		settleProxyResourceRuntime()
		samples = append(samples, readProxyResourceSample())
	}

	if got := waitForProxyResourceGoroutines(baseline.Goroutines, 2*time.Second); got > baseline.Goroutines+4 {
		t.Fatalf("goroutines after short resource budget = %d, baseline=%d", got, baseline.Goroutines)
	}
	if baseline.FDs >= 0 {
		if got := openProxyTestFDCount(); got > baseline.FDs+2 {
			t.Fatalf("file descriptors after short resource budget = %d, baseline=%d", got, baseline.FDs)
		}
	}

	// Ignore the first half's allocator and net/http warmup. A real leak keeps
	// increasing after the workload is steady; the last half still contains
	// 128 cycles, enough to catch a sustained multi-object-per-cycle leak.
	steadyStart := samples[batches/2-1].HeapObjects
	last := samples[len(samples)-1].HeapObjects
	t.Logf("short resource heap samples: baseline=%+v samples=%v", baseline, samples)
	if last > steadyStart+maxHeapObjGrowth {
		t.Fatalf("live heap objects grew from steady sample %d to %d across final %d proxy cycles (allowed growth=%d)", steadyStart, last, batches/2*cyclesPerBatch, maxHeapObjGrowth)
	}
	t.Logf("short resource budget settled: baseline=%+v final=%+v samples=%v", baseline, readProxyResourceSample(), samples)
}

type proxyResourceSnapshot struct {
	Goroutines  int
	FDs         int
	HeapAlloc   uint64
	HeapInuse   uint64
	HeapObjects uint64
}

func readProxyResourceSample() proxyResourceSnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return proxyResourceSnapshot{
		Goroutines:  runtime.NumGoroutine(),
		FDs:         openProxyTestFDCount(),
		HeapAlloc:   stats.HeapAlloc,
		HeapInuse:   stats.HeapInuse,
		HeapObjects: stats.HeapObjects,
	}
}

func settleProxyResourceRuntime() {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	runtime.GC()
}

func waitForProxyResourceGoroutines(baseline int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for runtime.NumGoroutine() > baseline+4 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return runtime.NumGoroutine()
}

func exerciseProxyResourceRound(t *testing.T, round int) {
	t.Helper()
	// Hosted macOS Intel runners can briefly pause between listener creation
	// and the first proxied request; keep the resource assertions strict while
	// avoiding a false failure from a one-second wall-clock budget.
	const proxyRoundTimeout = 3 * time.Second
	p := NewHTTPProxy(dialerFunc(func(string, string) (net.Conn, error) {
		return nil, io.EOF
	}), Options{InstanceID: fmt.Sprintf("resource-short-%d", round)})
	addr, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("round %d Start: %v", round, err)
	}

	healthTransport := &http.Transport{DisableKeepAlives: true}
	healthClient := &http.Client{Transport: healthTransport, Timeout: proxyRoundTimeout}
	resp, err := healthClient.Get("http://" + addr + "/_codex_proxy/health")
	if err != nil {
		_ = p.Close(context.Background())
		t.Fatalf("round %d health: %v", round, err)
	}
	_ = resp.Body.Close()
	healthTransport.CloseIdleConnections()

	proxyURL, err := neturlForTest(addr)
	if err != nil {
		_ = p.Close(context.Background())
		t.Fatalf("round %d proxy URL: %v", round, err)
	}
	requestTransport := &http.Transport{Proxy: http.ProxyURL(proxyURL), DisableKeepAlives: true}
	requestClient := &http.Client{Transport: requestTransport, Timeout: proxyRoundTimeout}
	request, err := requestClient.Get("http://resource-test.invalid/round")
	if err != nil {
		_ = p.Close(context.Background())
		t.Fatalf("round %d failed proxy request: %v", round, err)
	}
	if request.StatusCode != http.StatusBadGateway {
		_ = request.Body.Close()
		_ = p.Close(context.Background())
		t.Fatalf("round %d proxy request status = %d, want 502", round, request.StatusCode)
	}
	_ = request.Body.Close()
	requestTransport.CloseIdleConnections()

	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("round %d Close: %v", round, err)
	}
	// Rebind immediately while the address is not retained by the test. This
	// checks listener release without making the heap slope measure the test's
	// own historical port strings.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("round %d port %s remained owned after proxy close: %v", round, addr, err)
	}
	_ = ln.Close()
}

func neturlForTest(addr string) (*url.URL, error) {
	return url.Parse("http://" + addr)
}

func openProxyTestFDCount() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}
