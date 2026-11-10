package store

import (
	"fmt"
	"strings"
	"testing"
)

func TestNotificationEventQueriesUsePointIndexes(t *testing.T) {
	s, _ := openTest(t)
	for _, scope := range []NotificationScope{{ID: "turn:42"}, {ID: "mirror:thread:record:"}, {Conv: 1, Questions: true}, {Conv: 1}} {
		q, args := notificationPageQuery(scope)
		args = append(append([]any{0}, args...), false, 128)
		rows, err := s.r.Query("EXPLAIN QUERY PLAN "+q, args...)
		mustT(t, err)
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			mustT(t, rows.Scan(&id, &parent, &unused, &detail))
			details = append(details, detail)
			if strings.HasPrefix(detail, "SCAN n") || strings.HasPrefix(detail, "SCAN o") {
				t.Errorf("event scan %+v: %s", scope, detail)
			}
		}
		mustT(t, rows.Err())
		rows.Close()
		if !strings.Contains(strings.Join(details, ";"), "SEARCH n") || !strings.Contains(strings.Join(details, ";"), "SEARCH o") {
			t.Errorf("missing indexed event evidence: %v", details)
		}
	}
}

func TestNotificationDeliveryScopes(t *testing.T) {
	for _, tc := range []struct{ key, want string }{{"turn:42:final:0", "turn:42"}, {"mirror:thread:record:part:0", "mirror:thread:record:"}, {"ready:work:text", "ready:work:"}} {
		found := false
		for _, s := range DeliveryNotificationScopes(1, tc.key) {
			found = found || s.ID == tc.want
		}
		if !found {
			t.Errorf("%s missing scope %s", tc.key, tc.want)
		}
	}
	// Same prefix, different ID: a turn event must not wake turn 420.
	scopes := DeliveryNotificationScopes(1, "turn:42:final:0")
	for _, scope := range scopes {
		if scope.ID == "turn:420" {
			t.Fatal(scopes)
		}
	}
}

func TestNotificationAddedHintsAreBoundedAndNotPersisted(t *testing.T) {
	s, _ := openTest(t)
	mustT(t, s.Update(func(tx *Tx) error {
		for i := 0; i < 300; i++ {
			if err := tx.AddNotification(PendingNotification{Key: fmt.Sprint(i)}); err != nil {
				return err
			}
		}
		if len(tx.NotificationChanges()) != 1 || tx.NotificationChanges()[0] != (NotificationScope{}) {
			t.Fatal("unbounded transaction hints")
		}
		return nil
	}))
	mustT(t, s.Read(func(tx *Tx) error {
		if len(tx.NotificationChanges()) != 0 {
			t.Fatal("hints became durable state")
		}
		return nil
	}))
}
