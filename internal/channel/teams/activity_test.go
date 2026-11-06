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

func TestDecodeChatsStopsAndFindsTheNextLink(t *testing.T) {
	chat := func(id string) string {
		return `{"id":"` + id + `","lastMessagePreview":{"id":"m` + id + `","createdDateTime":"2026-01-01T00:00:00Z","from":{"user":{"id":"u"}}}}`
	}
	collect := func(body string, stopAt string) ([]string, string, error) {
		var ids []string
		next, err := decodeChats([]byte(body), func(c activityChat) bool {
			ids = append(ids, c.ID)
			return c.ID != stopAt
		})
		return ids, next, err
	}
	for _, body := range []string{
		`{"@odata.context":"x","value":[` + chat("a") + `,` + chat("b") + `],"@odata.nextLink":"/next"}`,
		`{"@odata.nextLink":"/next","@odata.count":2,"value":[` + chat("a") + `,` + chat("b") + `]}`,
	} {
		ids, next, err := collect(body, "")
		if err != nil || next != "/next" || len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
			t.Fatalf("%s: ids %v next %q err %v", body, ids, next, err)
		}
	}
	// Stopping leaves the rest of the page undecoded.
	ids, next, err := collect(`{"value":[`+chat("a")+`,`+chat("b")+`,{"id":`, "a")
	if err != nil || next != "" || len(ids) != 1 {
		t.Fatalf("stop: ids %v next %q err %v", ids, next, err)
	}
	if ids, _, err := collect(`{"value":null}`, ""); err != nil || len(ids) != 0 {
		t.Fatalf("null value: ids %v err %v", ids, err)
	}
	for _, bad := range []string{`[]`, `{"value":{}}`, `{"value":[{"id":`} {
		if _, _, err := collect(bad, ""); err == nil {
			t.Fatalf("%s decoded", bad)
		}
	}
}

func TestScanActivityPagesPastAFullPage(t *testing.T) {
	fg, a := newFake(t, fakegraph.Options{})
	for i := 0; i < activityPageSize+3; i++ {
		c := fg.CreateMeetingChatAsUser("c").ID
		if _, err := fg.PostAsUser(c, "hi"); err != nil {
			t.Fatal(err)
		}
	}
	all, err := a.ScanActivity(context.Background(), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != activityPageSize+3 {
		t.Fatalf("scan found %d chats", len(all))
	}
	noViolations(t, fg)
}
