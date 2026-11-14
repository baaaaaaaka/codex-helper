package teams

import (
	"bytes"
	"context"
	"encoding/json"
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
		older := false
		var link string
		err := a.g.monitorDecoded(ctx, next, pageBodyLimit, func(body []byte) (err error) {
			link, err = decodeChats(body, func(c activityChat) bool {
				if c.ID == "" || c.Preview == nil || c.Preview.ID == "" {
					older = true // chats without messages sort last
					return true
				}
				t, _ := time.Parse(time.RFC3339Nano, c.Preview.Created)
				if !since.IsZero() && !t.After(since) {
					older = true
					return false
				}
				out = append(out, channel.ChatActivity{
					Chat: c.ID, LastID: c.Preview.ID, Last: t,
					ByMe: c.Preview.From != nil && c.Preview.From.User != nil && c.Preview.From.User.ID == me.ID,
				})
				return true
			})
			return err
		})
		if err != nil {
			return nil, err
		}
		if older {
			break
		}
		next = link
	}
	return out, nil
}

// activityChat is what the scan reads of a chat of /me/chats.
type activityChat struct {
	ID      string `json:"id"`
	Preview *struct {
		ID      string `json:"id"`
		Created string `json:"createdDateTime"`
		From    *struct {
			User *graphIdentity `json:"user"`
		} `json:"from"`
	} `json:"lastMessagePreview"`
}

// decodeChats decodes a /me/chats page chat by chat, in page order, until
// each returns false, and returns the page's @odata.nextLink. The chats
// after the one each stopped at are not decoded: a quiet account's scan
// stops at the first chat of the page, and decoding the other previews
// (full message bodies) was most of an idle scan's cost.
func decodeChats(body []byte, each func(activityChat) bool) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	if tok, err := dec.Token(); err != nil {
		return "", err
	} else if tok != json.Delim('{') {
		return "", fmt.Errorf("page is not an object")
	}
	var next string
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return "", err
		}
		switch key {
		case "value":
			tok, err := dec.Token()
			if err != nil {
				return "", err
			}
			if tok == nil {
				continue // null: no chats
			}
			if tok != json.Delim('[') {
				return "", fmt.Errorf("value is not an array")
			}
			for dec.More() {
				var c activityChat
				if err := dec.Decode(&c); err != nil {
					return "", err
				}
				if !each(c) {
					return "", nil
				}
			}
			if _, err := dec.Token(); err != nil {
				return "", err
			}
		case "@odata.nextLink":
			if err := dec.Decode(&next); err != nil {
				return "", err
			}
		default:
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return "", err
			}
		}
	}
	return next, nil
}

var _ channel.ActivityScanner = (*Adapter)(nil)
