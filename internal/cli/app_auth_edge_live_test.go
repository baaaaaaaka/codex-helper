package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestWindowsProxyReachabilityScriptExecutesAgainstLiveProxy runs the exact
// PowerShell probe used by WSL app-auth against a local HTTP proxy. It covers
// both sides of the Windows boundary: Invoke-RestMethod must read the health
// response, and TcpClient must be able to send a raw CONNECT to the same
// listener. The native Edge test below then verifies effective browser
// routing separately.
func TestWindowsProxyReachabilityScriptExecutesAgainstLiveProxy(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("native Windows PowerShell probe runs on Windows")
	}

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/_codex_proxy/health" {
			http.Error(w, "unexpected probe", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "instanceId": "live-script-test"})
	}))
	defer proxy.Close()

	if err := codexAppAuthWindowsProxyReachable(context.Background(), proxy.URL); err != nil {
		t.Fatalf("Windows proxy reachability script failed against live proxy: %v", err)
	}
}

// TestWindowsEdgeActuallyUsesProxyForHTTP is an opt-in native Windows test.
// It launches the installed Edge binary, gives it a deliberately non-resolving
// URL, and succeeds only when the local HTTP proxy receives that request. The
// test therefore verifies the browser's effective proxy behavior, not merely
// that the generated PowerShell contains --proxy-server.
func TestWindowsEdgeActuallyUsesProxyForHTTP(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("native Edge proxy test runs on Windows")
	}
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_EDGE_PROXY_TEST")) == "" {
		t.Skip("set CODEX_HELPER_LIVE_EDGE_PROXY_TEST=1 to launch the installed Edge binary")
	}

	edge := findNativeEdgeForTest()
	if edge == "" {
		t.Fatal("CODEX_HELPER_LIVE_EDGE_PROXY_TEST is enabled but msedge.exe was not found")
	}

	const marker = "CODEX_EDGE_PROXY_MARKER_7f2c"
	seen := make(chan string, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case seen <- fmt.Sprintf("%s %s", r.Method, r.URL.String()):
		default:
		}
		if r.Method != http.MethodGet || r.URL.Host != "edge-proxy-check.invalid" {
			http.Error(w, "unexpected browser request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprintf(w, "<html><body>%s</body></html>", marker)
	}))
	defer proxy.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	profile := t.TempDir()
	cmd := exec.CommandContext(ctx, edge,
		"--headless=new",
		"--disable-gpu",
		"--no-first-run",
		"--no-default-browser-check",
		"--disable-extensions",
		"--user-data-dir="+profile,
		"--proxy-server="+proxy.URL,
		"--proxy-bypass-list=<-loopback>",
		"--dump-dom",
		"http://edge-proxy-check.invalid/marker",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Edge proxy launch failed: %v\noutput:\n%s", err, out)
	}
	if !strings.Contains(string(out), marker) {
		t.Fatalf("Edge output does not contain proxy marker %q:\n%s", marker, out)
	}
	select {
	case request := <-seen:
		if !strings.HasPrefix(request, "GET http://edge-proxy-check.invalid/marker") {
			t.Fatalf("unexpected proxy request: %q", request)
		}
	case <-time.After(time.Second):
		t.Fatal("Edge exited without sending a request through the proxy")
	}
}

// TestWindowsWSL2CanReachLiveProxy checks one explicit WSL2 boundary mode on a
// Windows self-hosted runner. "nat" binds 0.0.0.0 and uses the WSL gateway;
// "mirrored" binds the production loopback address 127.0.0.1 and uses the
// mirrored localhost path. Keeping the modes explicit avoids claiming that a
// wildcard listener proves the production loopback configuration.
func TestWindowsWSL2CanReachLiveProxy(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("native WSL2 proxy test runs on Windows")
	}
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_WSL_PROXY_TEST")) == "" {
		t.Skip("set CODEX_HELPER_LIVE_WSL_PROXY_TEST=1 to run the WSL2 proxy test")
	}
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_WSL_MODE")))
	if mode == "" {
		mode = "nat"
	}
	if mode != "nat" && mode != "mirrored" {
		t.Fatalf("unsupported WSL2 test mode %q; want nat or mirrored", mode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := exec.LookPath("wsl.exe"); err != nil {
		t.Fatalf("WSL2 test enabled but wsl.exe was not found: %v", err)
	}
	gateway := ""
	if mode == "nat" {
		gatewayCmd := exec.CommandContext(ctx, "wsl.exe", "-e", "sh", "-lc", "ip route | awk '/default/ {print $3; exit}'")
		gatewayOutput, err := gatewayCmd.Output()
		if err != nil {
			t.Fatalf("discover WSL2 default gateway: %v", err)
		}
		gateway = strings.TrimSpace(string(gatewayOutput))
		if gateway == "" {
			t.Fatal("WSL2 default gateway is empty")
		}
	}
	curlCheck := exec.CommandContext(ctx, "wsl.exe", "-e", "sh", "-lc", "command -v curl")
	if output, err := curlCheck.Output(); err != nil || strings.TrimSpace(string(output)) == "" {
		t.Fatalf("WSL2 curl is required for the live proxy test: %v", err)
	}

	const marker = "CODEX_WSL2_PROXY_MARKER_4e7a"
	seen := make(chan struct{}, 1)
	proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Host != "wsl-proxy-check.invalid" {
			http.Error(w, "unexpected WSL2 request", http.StatusBadRequest)
			return
		}
		select {
		case seen <- struct{}{}:
		default:
		}
		_, _ = fmt.Fprint(w, marker)
	}))
	bindAddr := "127.0.0.1:0"
	proxyHost := "127.0.0.1"
	if mode == "nat" {
		bindAddr = "0.0.0.0:0"
		proxyHost = gateway
	}
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		t.Fatalf("listen Windows-side proxy: %v", err)
	}
	proxy.Listener = listener
	proxy.Start()
	defer proxy.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	proxyURL := fmt.Sprintf("http://%s:%d", proxyHost, port)
	curl := "curl --fail --silent --show-error --noproxy \"\" --proxy " + shellQuoteForWSL(proxyURL) + " http://wsl-proxy-check.invalid/marker"
	requestCmd := exec.CommandContext(ctx, "wsl.exe", "-e", "sh", "-lc", curl)
	output, err := requestCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("WSL2 curl through Windows proxy failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), marker) {
		t.Fatalf("WSL2 curl response did not contain marker %q: %s", marker, output)
	}
	select {
	case <-seen:
	case <-time.After(time.Second):
		t.Fatal("Windows-side proxy did not observe the WSL2 request")
	}
}

// TestWindowsWSL2LauncherExitKeepsDetachedProcess verifies the WSL boundary
// relevant to cxp app: the wsl.exe invocation that starts a Linux detached
// process may exit, while a later invocation can still observe that process.
// The Linux lifecycle test covers CXP's production setsid configuration; this
// test covers the Windows-to-WSL launcher lifetime separately.
func TestWindowsWSL2LauncherExitKeepsDetachedProcess(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("native WSL2 launcher test runs on Windows")
	}
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_LIVE_WSL_PROXY_TEST")) == "" {
		t.Skip("set CODEX_HELPER_LIVE_WSL_PROXY_TEST=1 to run the WSL2 launcher test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := exec.LookPath("wsl.exe"); err != nil {
		t.Fatalf("WSL2 launcher test enabled but wsl.exe was not found: %v", err)
	}
	statePath := fmt.Sprintf("/tmp/codex-helper-wsl-detached-%d", time.Now().UnixNano())
	cleanup := func() {
		cleanupScript := "state=" + shellQuoteForWSL(statePath) + "; if test -s \"$state\"; then pid=$(cat \"$state\"); kill \"$pid\" 2>/dev/null || true; fi; rm -f \"$state\""
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupCmd := exec.CommandContext(cleanupCtx, "wsl.exe", "-e", "sh", "-lc", cleanupScript)
		_, _ = cleanupCmd.CombinedOutput()
	}
	t.Cleanup(cleanup)

	launchScript := "state=" + shellQuoteForWSL(statePath) + "; setsid sh -c 'echo $$ > \"$1\"; exec sleep 30' sh \"$state\" >/dev/null 2>&1 &"
	launchCmd := exec.CommandContext(ctx, "wsl.exe", "-e", "sh", "-lc", launchScript)
	if output, err := launchCmd.CombinedOutput(); err != nil {
		t.Fatalf("launch detached WSL process: %v\n%s", err, output)
	}
	probeScript := "state=" + shellQuoteForWSL(statePath) + "; i=0; while test ! -s \"$state\" && test $i -lt 50; do i=$((i+1)); sleep 0.1; done; test -s \"$state\"; pid=$(cat \"$state\"); kill -0 \"$pid\""
	probeCmd := exec.CommandContext(ctx, "wsl.exe", "-e", "sh", "-lc", probeScript)
	if output, err := probeCmd.CombinedOutput(); err != nil {
		t.Fatalf("detached WSL process did not survive launcher exit: %v\n%s", err, output)
	}
}

func shellQuoteForWSL(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func findNativeEdgeForTest() string {
	candidates := []string{}
	for _, name := range []string{"LOCALAPPDATA", "ProgramFiles", "ProgramFiles(x86)"} {
		if root := strings.TrimSpace(os.Getenv(name)); root != "" {
			candidates = append(candidates, filepath.Join(root, "Microsoft", "Edge", "Application", "msedge.exe"))
		}
	}
	if path, err := exec.LookPath("msedge.exe"); err == nil {
		candidates = append(candidates, path)
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return ""
}
