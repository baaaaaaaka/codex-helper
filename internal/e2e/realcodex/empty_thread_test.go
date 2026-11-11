//go:build !windows

package realcodex

import (
	"strings"
	"testing"
)

// Exercise the zero-input boundary that the existing shared-client tests skip.
func TestRealCodexSharedEmptyThreadHandoff(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		extra        map[string]any
	}{
		{"read-metadata", "thread/read", map[string]any{"includeTurns": false}},
		{"resume-without-turns", "thread/resume", map[string]any{"excludeTurns": true}},
		{"resume-with-turns", "thread/resume", map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newProbe(t)
			connect := p.sharedServer()
			a := connect()
			thread := sharedThread(a, p.dir)
			b := connect()
			params := map[string]any{"threadId": thread}
			for k, v := range tc.extra {
				params[k] = v
			}
			_, rpcErr := b.request(tc.method, params)
			// This is a native behavior probe, not a requirement that CXP may
			// depend on. Releases may support live resume or require a rollout.
			if rpcErr != nil && (tc.method == "thread/read" || !strings.Contains(rpcErr.Message, "no rollout found")) {
				t.Fatalf("zero-input handoff: %v", rpcErr)
			}
			p.evidence(tc.name, map[string]any{"error": rpcErr})
		})
	}
}

func TestRealCodexSharedExternalThreadStartNotification(t *testing.T) {
	p := newProbe(t)
	connect := p.sharedServer()
	observer, creator := connect(), connect()
	thread := sharedThread(creator, p.dir)
	observer.wait("thread/started", thread)
	p.evidence("external-thread-created", map[string]any{"thread": thread})
}
