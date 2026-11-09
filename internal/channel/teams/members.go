package teams

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// OtherParticipants reads current members, not chatType (a one-person
// meeting chat is also "group") or the authors of historical messages.
func (a *Adapter) OtherParticipants(ctx context.Context, chat string) (bool, error) {
	me, err := a.me(ctx)
	if err != nil {
		return false, err
	}
	next := "/chats/" + url.PathEscape(chat) + "/members"
	seen := map[string]bool{}
	ownerSeen := false
	for pageNumber := 0; pageNumber < 10; pageNumber++ {
		if seen[next] {
			return false, fmt.Errorf("teams: membership pagination loop")
		}
		seen[next] = true
		var page struct {
			Value []struct {
				UserID string `json:"userId"`
			} `json:"value"`
			Next string `json:"@odata.nextLink"`
		}
		if err := a.g.getJSON(ctx, next, pageBodyLimit, &page); err != nil {
			return false, err
		}
		for _, member := range page.Value {
			if member.UserID == "" {
				return false, fmt.Errorf("teams: member response has no user identity")
			}
			if !strings.EqualFold(member.UserID, me.ID) {
				return true, nil
			}
			ownerSeen = true
		}
		if page.Next == "" {
			if !ownerSeen {
				return false, fmt.Errorf("teams: empty chat membership response")
			}
			return false, nil
		}
		next = page.Next
	}
	return false, fmt.Errorf("teams: chat membership exceeds page limit")
}
