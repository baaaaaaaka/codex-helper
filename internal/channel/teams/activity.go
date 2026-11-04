package teams

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// activityPageSize is the $top of an activity scan page (Graph's maximum
// for /me/chats).
const activityPageSize = 50

// ScanActivity implements channel.ActivityScanner with
// GET /me/chats?$expand=lastMessagePreview&$orderby=lastMessagePreview/createdDateTime desc.
// Paging stops at the first chat whose newest message is not newer than
// since, so a quiet account costs one request per scan.
func (a *Adapter) ScanActivity(ctx context.Context, since time.Time) ([]channel.ChatActivity, error) {
	me, err := a.me(ctx)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Set("$expand", "lastMessagePreview")
	q.Set("$orderby", "lastMessagePreview/createdDateTime desc")
	q.Set("$top", strconv.Itoa(activityPageSize))
	next := "/me/chats?" + q.Encode()
	seen := map[string]bool{}
	var out []channel.ChatActivity
	for pages := 0; next != ""; pages++ {
		if pages >= a.cfg.MaxPages {
			break
		}
		if seen[next] {
			return nil, &channel.Rejected{Err: fmt.Errorf("teams: /me/chats nextLink loop")}
		}
		seen[next] = true
		var page struct {
			Value []struct {
				ID      string `json:"id"`
				Preview *struct {
					ID      string `json:"id"`
					Created string `json:"createdDateTime"`
					From    *struct {
						User *graphIdentity `json:"user"`
					} `json:"from"`
				} `json:"lastMessagePreview"`
			} `json:"value"`
			NextLink string `json:"@odata.nextLink"`
		}
		if err := a.g.getJSON(ctx, next, pageBodyLimit, &page); err != nil {
			return nil, err
		}
		older := false
		for _, c := range page.Value {
			if c.ID == "" || c.Preview == nil || c.Preview.ID == "" {
				older = true // chats without messages sort last
				continue
			}
			t, _ := time.Parse(time.RFC3339Nano, c.Preview.Created)
			if !since.IsZero() && !t.After(since) {
				older = true
				break
			}
			out = append(out, channel.ChatActivity{
				Chat: c.ID, LastID: c.Preview.ID, Last: t,
				ByMe: c.Preview.From != nil && c.Preview.From.User != nil && c.Preview.From.User.ID == me.ID,
			})
		}
		if older {
			break
		}
		next = page.NextLink
	}
	return out, nil
}

var _ channel.ActivityScanner = (*Adapter)(nil)
