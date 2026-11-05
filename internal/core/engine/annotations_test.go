package engine

import (
	"strings"
	"testing"
	"time"
)

func TestFreezeText(t *testing.T) {
	got := freezeText("s004", "https://teams/x", 48*time.Hour)
	for _, want := range []string{"🧊 This chat is paused", "read and run once it wakes", "Paused after 48h idle.",
		"[Control chat](https://teams/x)", "Send: `r s004`"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
	if !strings.Contains(freezeText("s1", "", 90*time.Second), "Paused after 1m30s idle.") {
		t.Error("idle text")
	}
}
