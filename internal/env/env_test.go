package env

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

func TestWithProxy_SetsProxyAndMergesNoProxy(t *testing.T) {
	base := []string{
		"PATH=/bin",
		"NO_PROXY=example.com,localhost",
	}

	out := WithProxy(base, "http://127.0.0.1:8080")
	m := toMap(out)

	if got := m["HTTP_PROXY"]; got != "http://127.0.0.1:8080" {
		t.Fatalf("HTTP_PROXY=%q", got)
	}
	if got := m["http_proxy"]; got != "http://127.0.0.1:8080" {
		t.Fatalf("http_proxy=%q", got)
	}
	if got := m["HTTPS_PROXY"]; got != "http://127.0.0.1:8080" {
		t.Fatalf("HTTPS_PROXY=%q", got)
	}
	if got := m["https_proxy"]; got != "http://127.0.0.1:8080" {
		t.Fatalf("https_proxy=%q", got)
	}

	noProxy := firstNonEmpty(m["NO_PROXY"], m["no_proxy"])
	for _, want := range []string{"example.com", "localhost", "127.0.0.1", "::1"} {
		if !containsCSV(noProxy, want) {
			t.Fatalf("NO_PROXY=%q missing %q", noProxy, want)
		}
	}
}

func TestWithProxy_PreservesExistingLowercaseNoProxy(t *testing.T) {
	base := []string{
		"no_proxy=foo.local",
	}

	out := WithProxy(base, "http://127.0.0.1:8080")
	m := toMap(out)

	noProxy := firstNonEmpty(m["NO_PROXY"], m["no_proxy"])
	if !containsCSV(noProxy, "foo.local") {
		t.Fatalf("NO_PROXY=%q missing foo.local", noProxy)
	}
}

func TestWithProxyReplacesInheritedProxyVariantsDeterministically(t *testing.T) {
	const (
		stale = "socks5://127.0.0.1:11080"
		proxy = "http://127.0.0.1:8080"
	)
	base := []string{
		"PATH=/bin",
		"KEEP=first",
		"HTTP_PROXY=" + stale,
		"Http_Proxy=" + stale,
		"https_proxy=" + stale,
		"ALL_PROXY=" + stale,
		"all_proxy=" + stale,
		"All_Proxy=" + stale,
		"NO_PROXY=corp.example,localhost",
		"KEEP=second",
	}

	var want []string
	for i := 0; i < 20; i++ {
		got := WithProxy(base, proxy)
		if i == 0 {
			want = got
		} else if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
			t.Fatalf("WithProxy output must be deterministic:\nfirst: %#v\n got: %#v", want, got)
		}
	}

	for _, kv := range want {
		key, value, _ := strings.Cut(kv, "=")
		if isStandardProxyKeyForTest(key) && value == stale {
			t.Fatalf("stale proxy value survived in %q", kv)
		}
	}

	if got := nonProxyEnvironmentForTest(want); strings.Join(got, "\x00") != "PATH=/bin\x00KEEP=first\x00KEEP=second" {
		t.Fatalf("unmanaged environment was changed or reordered: %#v", got)
	}

	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"} {
		if got := valueForExactKeyForTest(want, key); got != proxy {
			t.Fatalf("%s = %q, want %q", key, got, proxy)
		}
	}
	noProxy := valueForExactKeyForTest(want, "NO_PROXY")
	for _, required := range []string{"corp.example", "localhost", "127.0.0.1", "::1"} {
		if !containsCSV(noProxy, required) {
			t.Fatalf("NO_PROXY = %q, missing %q", noProxy, required)
		}
	}

	if runtime.GOOS == "windows" {
		for _, key := range []string{"http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
			if got := valueForExactKeyForTest(want, key); got != "" {
				t.Fatalf("Windows output must use canonical uppercase keys; %s = %q", key, got)
			}
		}
		return
	}
	for _, key := range []string{"http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
		if got := valueForExactKeyForTest(want, key); got == "" {
			t.Fatalf("Unix output missing lowercase compatibility key %s", key)
		}
	}
}

func TestWithProxyChildReceivesOnlyManagedProxyValues(t *testing.T) {
	const (
		stale = "socks5://127.0.0.1:11080"
		proxy = "http://127.0.0.1:8080"
	)
	cmd := exec.Command(os.Args[0], "-test.run=^TestWithProxyEnvironmentChild$")
	cmd.Env = append(WithProxy([]string{
		"PATH=" + os.Getenv("PATH"),
		"ALL_PROXY=" + stale,
		"all_proxy=" + stale,
		"All_Proxy=" + stale,
	}, proxy), "CXP_ENV_PROXY_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run proxy environment child: %v\n%s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		key, value, _ := strings.Cut(line, "=")
		if isStandardProxyKeyForTest(key) && value == stale {
			t.Fatalf("child retained stale proxy value in %q\n%s", line, out)
		}
	}
	if !strings.Contains(string(out), "ALL_PROXY="+proxy) {
		t.Fatalf("child did not receive ALL_PROXY=%q\n%s", proxy, out)
	}
}

func TestWithProxyEnvironmentChild(t *testing.T) {
	if os.Getenv("CXP_ENV_PROXY_CHILD") != "1" {
		return
	}
	_, _ = os.Stdout.WriteString(strings.Join(os.Environ(), "\n"))
}

func isStandardProxyKeyForTest(key string) bool {
	for _, candidate := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"} {
		if strings.EqualFold(key, candidate) {
			return true
		}
	}
	return false
}

func nonProxyEnvironmentForTest(env []string) []string {
	var out []string
	for _, kv := range env {
		key, _, _ := strings.Cut(kv, "=")
		if !isStandardProxyKeyForTest(key) {
			out = append(out, kv)
		}
	}
	return out
}

func valueForExactKeyForTest(env []string, want string) string {
	for _, kv := range env {
		key, value, _ := strings.Cut(kv, "=")
		if key == want {
			return value
		}
	}
	return ""
}

func containsCSV(csv, needle string) bool {
	for _, part := range strings.Split(csv, ",") {
		if strings.EqualFold(strings.TrimSpace(part), needle) {
			return true
		}
	}
	return false
}

func TestEnvHelpers(t *testing.T) {
	t.Run("WithProxy handles empty env and proxy", func(t *testing.T) {
		out := WithProxy(nil, "")
		m := toMap(out)
		if _, ok := m["HTTP_PROXY"]; !ok {
			t.Fatalf("expected HTTP_PROXY to be set")
		}
		noProxy := firstNonEmpty(m["NO_PROXY"], m["no_proxy"])
		for _, want := range []string{"localhost", "127.0.0.1", "::1"} {
			if !containsCSV(noProxy, want) {
				t.Fatalf("NO_PROXY=%q missing %q", noProxy, want)
			}
		}
	})

	t.Run("mergeNoProxy dedupes and trims", func(t *testing.T) {
		out := mergeNoProxy(" example.com,EXAMPLE.com, ,localhost ", []string{"LOCALHOST", "127.0.0.1"})
		if !containsCSV(out, "example.com") || !containsCSV(out, "localhost") || !containsCSV(out, "127.0.0.1") {
			t.Fatalf("unexpected merge output: %q", out)
		}
		parts := strings.Split(out, ",")
		seen := map[string]bool{}
		for _, part := range parts {
			key := strings.ToLower(strings.TrimSpace(part))
			if key == "" {
				continue
			}
			if seen[key] {
				t.Fatalf("expected dedupe, got %q", out)
			}
			seen[key] = true
		}
	})

	t.Run("toMap and fromMap handle malformed entries", func(t *testing.T) {
		m := toMap([]string{"INVALID", "KEY=value"})
		if m["KEY"] != "value" {
			t.Fatalf("expected KEY=value, got %#v", m)
		}
		out := fromMap(map[string]string{"A": "1"})
		if len(out) != 1 || out[0] != "A=1" {
			t.Fatalf("unexpected fromMap output: %#v", out)
		}
	})
}
