package codexrunner

import (
	"context"
	"testing"
)

func TestProbeAppServerCompatibilityClosesTransportAfterFailedProbe(t *testing.T) {
	var transport *fakeAppServerTransport
	starter := AppServerTransportStarterFunc(func(_ context.Context, _ AppServerStartRequest) (AppServerLineTransport, error) {
		transport = newFakeAppServerTransport(
			`{"id":1,"result":{"userAgent":"codex-helper-test/0"}}`,
			`{"id":2,"result":{"data":[],"nextCursor":null,"backwardsCursor":null}}`,
			`{"id":3,"error":{"code":"probe_failed","message":"list failed"}}`,
		)
		return transport, nil
	})

	got, err := ProbeAppServerCompatibility(context.Background(), AppServerProbeOptions{
		Starter: starter,
		Command: "/managed/codex",
		Runs:    1,
		Limit:   1,
	})
	if err == nil {
		t.Fatalf("ProbeAppServerCompatibility result = %#v, want error", got)
	}
	if transport == nil {
		t.Fatal("probe did not start a transport")
	}
	if !transport.closed {
		t.Fatal("failed probe did not close transport")
	}
	if len(got.Runs) != 0 {
		t.Fatalf("probe runs = %#v, want no successful runs after failure", got.Runs)
	}
}
