package cli

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

func TestTeamsTestSeamIsOffUnlessSet(t *testing.T) {
	t.Setenv(envTeamsTestSeam, "")
	if _, err := loadTeamsTestSeam(); err == nil {
		t.Fatal("an empty CXP_TEST_ONLY_SEAM must be rejected (no endpoint), not ignored")
	}
	os.Unsetenv(envTeamsTestSeam)
	if s, err := loadTeamsTestSeam(); s != nil || err != nil {
		t.Fatalf("unset: seam %v, err %v", s, err)
	}
}

func TestTeamsTestSeamRejectsAnythingButALoopbackEndpoint(t *testing.T) {
	for _, raw := range []string{
		"",
		"instance=x",
		"endpoint=https://127.0.0.1:1234",
		"endpoint=http://graph.microsoft.com:80",
		"endpoint=http://10.0.0.1:8080",
		"endpoint=http://127.0.0.1",
		"endpoint=http://127.0.0.1:1234/v1.0",
		"endpoint=http://u:p@127.0.0.1:1234",
		"endpoint=http://127.0.0.1:1234;bogus=1",
		"endpoint=http://127.0.0.1:1234;timing=slow",
		"endpoint=http://127.0.0.1:1234;timing.Tick=1s",
		"endpoint=http://127.0.0.1:1234;timing.SilenceHint=-1s",
		"endpoint=http://127.0.0.1:1234;hostname",
		"endpoint=http://127.0.0.1:1234;cpuprofile=relative/dir",
	} {
		if _, err := parseTeamsTestSeam(raw); !errors.Is(err, errTeamsTestSeamInvalid) {
			t.Errorf("%q: err = %v, want errTeamsTestSeamInvalid", raw, err)
		}
	}
}

func TestTeamsTestSeamTimingProfiles(t *testing.T) {
	s, err := parseTeamsTestSeam("endpoint=http://127.0.0.1:1234")
	if err != nil {
		t.Fatal(err)
	}
	if got, rate := s.timing(); got != engine.ProductionTiming() || rate != 0 {
		t.Fatalf("default timing = %+v rate %v, want production", got, rate)
	}
	s, err = parseTeamsTestSeam("endpoint=http://[::1]:1234; timing=fast:250ms; timing.SilenceHint=3s; timing.RefusalRecheck=500ms")
	if err != nil {
		t.Fatal(err)
	}
	want := engine.FastTiming(250 * time.Millisecond)
	want.SilenceHint = 3 * time.Second
	if got, rate := s.timing(); got != want || rate != 16 {
		t.Fatalf("fast timing = %+v rate %v, want %+v rate 16", got, rate, want)
	}
	if s.refusalRecheck() != 500*time.Millisecond {
		t.Fatalf("refusal recheck = %v", s.refusalRecheck())
	}
}

func TestTeamsTestSeamCPUProfileWritesAProfileUntilStopped(t *testing.T) {
	dir := t.TempDir()
	s, err := parseTeamsTestSeam("endpoint=http://127.0.0.1:1234;cpuprofile=" + dir)
	if err != nil {
		t.Fatal(err)
	}
	s.startCPUProfile(io.Discard)()
	files, _ := filepath.Glob(filepath.Join(dir, "cpu-*.pprof"))
	if len(files) != 1 {
		t.Fatalf("profiles = %v, want one", files)
	}
	if fi, err := os.Stat(files[0]); err != nil || fi.Size() == 0 {
		t.Fatalf("profile %s: %v (a stopped profile is complete, not empty)", files[0], err)
	}
	var off *teamsTestSeam
	off.startCPUProfile(io.Discard)() // no seam: no profile, no error
}

func TestTeamsTestSeamSendsEveryRequestToTheEndpointUnchanged(t *testing.T) {
	type seen struct{ method, uri, host, scheme, instance, auth, body string }
	got := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- seen{r.Method, r.RequestURI, r.Header.Get(teamsTestSeamHeaderHost), r.Header.Get(teamsTestSeamHeaderScheme),
			r.Header.Get(teamsTestSeamHeaderInstance), r.Header.Get("Authorization"), string(b)}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	s, err := parseTeamsTestSeam("endpoint=" + srv.URL + ";instance=cxp-7")
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, "https://graph.microsoft.com/v1.0/chats/19:a@thread.v2/messages?$top=5", strings.NewReader(`{"x":1}`))
	req.Header.Set("Authorization", "Bearer t")
	resp, err := s.httpClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	g := <-got
	want := seen{"POST", "/v1.0/chats/19:a@thread.v2/messages?$top=5", "graph.microsoft.com", "https", "cxp-7", "Bearer t", `{"x":1}`}
	if g != want {
		t.Fatalf("endpoint saw %+v, want %+v", g, want)
	}
}

func TestTeamsHostnameFollowsTheSeam(t *testing.T) {
	t.Setenv(envTeamsTestSeam, "endpoint=http://127.0.0.1:9;hostname=host-b")
	if h, err := teamsHostname(); err != nil || h != "host-b" {
		t.Fatalf("teamsHostname = %q, %v", h, err)
	}
	t.Setenv(envTeamsTestSeam, "endpoint=http://example.com:9;hostname=host-b")
	if _, err := teamsHostname(); err == nil {
		t.Fatal("an invalid seam must fail, not fall back")
	}
}
