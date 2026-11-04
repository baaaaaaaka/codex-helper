package teams

import (
	"context"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestScanActivityStopsAtWatermark(t *testing.T) {
	fg, a := newFake(t, fakegraph.Options{})
	ctx := context.Background()
	x := fg.CreateMeetingChatAsUser("x").ID
	y := fg.CreateMeetingChatAsUser("y").ID
	_ = fg.CreateMeetingChatAsUser("empty")
	if _, err := fg.PostAsUser(x, "first"); err != nil {
		t.Fatal(err)
	}
	if _, err := fg.PostAsUser(y, "second"); err != nil {
		t.Fatal(err)
	}
	all, err := a.ScanActivity(ctx, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].Chat != y || all[1].Chat != x || all[0].LastID == "" || all[0].Last.IsZero() {
		t.Fatalf("scan = %+v", all)
	}
	if all[0].ByMe != true {
		t.Fatalf("owner post not ByMe: %+v", all[0])
	}
	got, err := a.ScanActivity(ctx, all[0].Last)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("scan after watermark = %+v", got)
	}
	if _, err := fg.PostAsUser(x, "third"); err != nil {
		t.Fatal(err)
	}
	got, err = a.ScanActivity(ctx, all[0].Last)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Chat != x {
		t.Fatalf("scan after new message = %+v", got)
	}
	noViolations(t, fg)
}
