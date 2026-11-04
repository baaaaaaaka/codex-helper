// Package leakcheck is a small goroutine leak detector for the core
// packages' tests, in the spirit of go.uber.org/goleak (which is not in the
// offline module cache this repository is tested with).
//
// Use it from TestMain:
//
//	func TestMain(m *testing.M) { leakcheck.Main(m) }
//
// or per test with defer leakcheck.Check(t)().
package leakcheck

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// ignored are stack substrings of goroutines that belong to the runtime, the
// testing package or long-lived library pools rather than to the code
// under test.
var ignored = []string{
	"testing.(*T).Run",
	"testing.(*M).",
	"testing.tRunner",
	"testing.runTests",
	"testing.(*F).",
	"runtime.goexit",
	"os/signal.signal_recv",
	"os/signal.loop",
	"database/sql.(*DB).connectionOpener",
	"database/sql.(*DB).connectionCleaner",
	"internal/poll.runtime_pollWait",
	"runtime.ensureSigM",
	"created by runtime.gc",
	"runtime/trace.Start",
	"modernc.org/sqlite",
	"net/http.(*persistConn)",
	"net/http.(*Transport).dialConnFor",
}

// interesting returns the goroutines (stack text) other than the current
// one that are not ignored.
func interesting() []string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var out []string
	for i, g := range strings.Split(string(buf), "\n\n") {
		if i == 0 || strings.TrimSpace(g) == "" { // the calling goroutine
			continue
		}
		skip := false
		for _, s := range ignored {
			// A goroutine whose only frames are the ignored entry points.
			if strings.Contains(g, s) && !strings.Contains(g, "codex-helper/internal/core/") {
				skip = true
				break
			}
		}
		if !skip {
			out = append(out, g)
		}
	}
	sort.Strings(out)
	return out
}

// Find waits up to timeout for leaked goroutines to exit and returns the
// remaining ones.
func Find(timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		left := interesting()
		if len(left) == 0 || time.Now().After(deadline) {
			return left
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Main runs the tests and fails the binary when goroutines leak.
func Main(m *testing.M) {
	code := m.Run()
	if code == 0 {
		if left := Find(5 * time.Second); len(left) > 0 {
			fmt.Fprintf(os.Stderr, "leakcheck: %d leaked goroutine(s):\n\n%s\n", len(left), strings.Join(left, "\n\n"))
			code = 1
		}
	}
	os.Exit(code)
}

// Check returns a function that fails t when goroutines started during the
// test are still running (call it deferred).
func Check(t testing.TB) func() {
	before := map[string]bool{}
	for _, g := range interesting() {
		before[firstLine(g)] = true
	}
	return func() {
		t.Helper()
		var left []string
		for _, g := range Find(5 * time.Second) {
			if !before[firstLine(g)] {
				left = append(left, g)
			}
		}
		if len(left) > 0 {
			t.Errorf("leakcheck: %d leaked goroutine(s):\n\n%s", len(left), strings.Join(left, "\n\n"))
		}
	}
}

func firstLine(g string) string {
	if i := strings.IndexByte(g, '\n'); i >= 0 {
		return g[:i]
	}
	return g
}
