package teams

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func TestMembersUseCurrentParticipantsNotGroupChatType(t *testing.T) {
	fg, a := newFake(t, fakegraph.Options{})
	chat := fg.CreateMeetingChatAsUser("owner-only group chat").ID
	if other, err := a.OtherParticipants(context.Background(), chat); err != nil || other {
		t.Fatalf("one-person group: %v %v", other, err)
	}
	if _, err := fg.PostAsCoworker(chat, "hello"); err != nil {
		t.Fatal(err)
	}
	if other, err := a.OtherParticipants(context.Background(), chat); err != nil || !other {
		t.Fatalf("real second member: %v %v", other, err)
	}
	noViolations(t, fg)
}

func TestMembersRejectUnknownIdentityAndFollowPages(t *testing.T) {
	for _, mode := range []string{"empty", "unknown", "pages", "loop"} {
		t.Run(mode, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/v1.0/me" {
					w.Write([]byte(`{"id":"owner"}`))
					return
				}
				switch mode {
				case "empty":
					w.Write([]byte(`{"value":[]}`))
				case "unknown":
					w.Write([]byte(`{"value":[{"id":"opaque-member"}]}`))
				case "pages":
					if r.URL.Path == "/v1.0/page2" {
						w.Write([]byte(`{"value":[{"userId":"human"}]}`))
					} else {
						w.Write([]byte(`{"value":[{"userId":"owner"}],"@odata.nextLink":"/page2"}`))
					}
				case "loop":
					w.Write([]byte(`{"value":[{"userId":"owner"}],"@odata.nextLink":"/chats/chat/members"}`))
				}
			}))
			defer s.Close()
			a, err := New(Config{GraphURL: s.URL + "/v1.0", Tokens: &staticTokens{tok: "fixture"}})
			if err != nil {
				t.Fatal(err)
			}
			other, err := a.OtherParticipants(context.Background(), "chat")
			if mode == "pages" {
				if err != nil || !other {
					t.Fatalf("missed participant on next page: %v %v", other, err)
				}
			} else if err == nil {
				t.Fatal("invalid response became a membership decision")
			}
		})
	}
}
