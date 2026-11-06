package codex

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// Error notifications Codex retries by itself reach TurnHooks.Retrying with
// v0.1.20's composition (message: details, reason from codexErrorInfo) and
// sanitization (proxy token redacted, one line).
func TestRunTurnReportsRetries(t *testing.T) {
	s, _, cfg := startFake(t)
	ctx := tctx(t)
	th, err := s.ThreadStart(ctx, ThreadStartParams{Cwd: cfg.Dir})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var got []string
	res, err := s.RunTurn(ctx, TurnStartParams{ThreadID: th.ID, ClientID: "k", Input: []Input{{Text: "retry me"}}}, time.Minute, TurnHooks{
		Retrying: func(message, code string) {
			mu.Lock()
			got = append(got, message+" | "+code)
			mu.Unlock()
		},
	})
	if err != nil || res.Final != "ANSWER retry me" {
		t.Fatalf("%v %+v", err, res)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"Reconnecting... 1/5: stream disconnected: via http://h/_cxp/<redacted>/v1 | responseStreamDisconnected",
		"Codex stream disconnected; reconnecting | serverOverloaded",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("retries:\n%q\nwant\n%q", got, want)
	}
}

func TestParseRetry(t *testing.T) {
	for _, c := range []struct {
		raw               string
		ok                bool
		turn, msg, reason string
	}{
		{`{"willRetry":false,"error":{"message":"boom"}}`, false, "", "", ""},
		{`{"turnId":"t1","willRetry":true,"error":{"message":"m","code":"c","additionalDetails":"m"}}`, true, "t1", "m", "c"},
		{`{"willRetry":true,"message":"legacy","code":"x","error":{"message":"","codexErrorInfo":"other"}}`, true, "", "legacy", "x"},
		{`{"willRetry":true,"error":{"message":"<html><body>bad gateway</body></html>"}}`, true, "", "HTML response omitted", ""},
		{`{"willRetry":true,"error":{"message":"` + strings.Repeat("a", 2100) + `"}}`, true, "", strings.Repeat("a", 2048) + "...", ""},
	} {
		r, ok := parseRetry(json.RawMessage(c.raw))
		if ok != c.ok || r.turnID != c.turn || r.message != c.msg || r.code != c.reason {
			t.Errorf("parseRetry(%.80s) = %+v %v", c.raw, r, ok)
		}
	}
}
