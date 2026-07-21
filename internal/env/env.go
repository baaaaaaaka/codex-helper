package env

import (
	"runtime"
	"strings"
)

var loopbackNoProxy = []string{"localhost", "127.0.0.1", "::1"}

func WithProxy(base []string, proxyURL string) []string {
	noProxy := mergeNoProxy(inheritedNoProxy(base), loopbackNoProxy)

	// Preserve unrelated entries and their order. Rebuilding the complete
	// environment through a map makes the final Windows environment depend on
	// random map iteration when an inherited key uses unusual casing.
	out := make([]string, 0, len(base)+8)
	for _, kv := range base {
		key, _, ok := strings.Cut(kv, "=")
		if ok && isManagedProxyKey(key) {
			continue
		}
		out = append(out, kv)
	}

	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"} {
		out = append(out, key+"="+proxyURL)
		if runtime.GOOS != "windows" {
			out = append(out, strings.ToLower(key)+"="+proxyURL)
		}
	}
	out = append(out, "NO_PROXY="+noProxy)
	if runtime.GOOS != "windows" {
		out = append(out, "no_proxy="+noProxy)
	}
	return out
}

func inheritedNoProxy(base []string) string {
	if runtime.GOOS == "windows" {
		var value string
		for _, kv := range base {
			key, candidate, ok := strings.Cut(kv, "=")
			if ok && strings.EqualFold(key, "NO_PROXY") {
				value = candidate
			}
		}
		return value
	}

	// Preserve the historical Unix precedence: uppercase wins when non-empty,
	// followed by lowercase. Other spellings are deliberately not promoted into
	// a new NO_PROXY behavior, although they are removed as managed aliases.
	var uppercase, lowercase string
	for _, kv := range base {
		key, value, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		switch key {
		case "NO_PROXY":
			uppercase = value
		case "no_proxy":
			lowercase = value
		}
	}
	return firstNonEmpty(uppercase, lowercase)
}

func isManagedProxyKey(key string) bool {
	for _, candidate := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"} {
		if strings.EqualFold(key, candidate) {
			return true
		}
	}
	return false
}

func mergeNoProxy(existing string, required []string) string {
	set := map[string]bool{}
	var out []string

	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		key := strings.ToLower(v)
		if set[key] {
			return
		}
		set[key] = true
		out = append(out, v)
	}

	for _, part := range strings.Split(existing, ",") {
		add(part)
	}
	for _, req := range required {
		add(req)
	}

	return strings.Join(out, ",")
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

func toMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, kv := range env {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		out[k] = v
	}
	return out
}

func fromMap(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return out
}
