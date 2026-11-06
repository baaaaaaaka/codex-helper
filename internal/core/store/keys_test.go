package store

import "testing"

func TestKeyFormats(t *testing.T) {
	for got, want := range map[string]string{
		ChatStreamKey("teams", "19:a@thread.v2"): "teams:chat:19:a@thread.v2",
		AccountReadGate("teams"):                 "teams:account-read",
		AccountWriteGate("teams"):                "teams:account-write",
		ChatGate("teams", "19:a@thread.v2"):      "teams:chat-gate:19:a@thread.v2",
	} {
		if got != want {
			t.Errorf("key %q, want %q", got, want)
		}
	}
}
