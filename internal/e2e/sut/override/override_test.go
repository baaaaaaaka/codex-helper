package override

import (
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

func TestApplyTiming(t *testing.T) {
	tm := engine.FastTiming(250 * time.Millisecond)
	if err := ApplyTiming(&tm, "SilenceHint=2s, StallLimit=8s,BreakerCount=5"); err != nil {
		t.Fatal(err)
	}
	if tm.SilenceHint != 2*time.Second || tm.StallLimit != 8*time.Second || tm.BreakerCount != 5 {
		t.Fatalf("not applied: %v %v %v", tm.SilenceHint, tm.StallLimit, tm.BreakerCount)
	}
	for _, bad := range []string{"Nope=1s", "SilenceHint", "SilenceHint=abc", "BreakerCount=x"} {
		if err := ApplyTiming(&tm, bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
	if err := ApplyTiming(&tm, ""); err != nil {
		t.Fatal(err)
	}
}
