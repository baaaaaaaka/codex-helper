package store

import (
	"fmt"
	"strings"
)

// PendingNotification is one durable owner notice. Outbox remains the only
// authority for delivery confirmation; Attempted prevents a second card POST.
type PendingNotification struct {
	Turn      int64  `json:"turn"`
	Conv      int64  `json:"conv"`
	Key       string `json:"key,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Text      string `json:"text,omitempty"`
	Request   string `json:"request,omitempty"`
	Attempted bool   `json:"attempted,omitempty"`
}

func (n PendingNotification) ID() string {
	if n.Turn != 0 {
		return fmt.Sprintf("turn:%d", n.Turn)
	}
	return n.Key
}

const notificationCols = `turn_id,conv_id,key,kind,text,request,attempted`

// NotificationScope is an event's affected notices. The zero value is a full
// recovery pass; normal sends use a notification ID or a question conversation.
type NotificationScope struct {
	ID        string
	Conv      int64
	Questions bool
}

func scanNotification(sc scanner) (PendingNotification, error) {
	var n PendingNotification
	err := sc.Scan(&n.Turn, &n.Conv, &n.Key, &n.Kind, &n.Text, &n.Request, &n.Attempted)
	return n, err
}

func (t *Tx) AddNotification(n PendingNotification) error {
	if n.ID() == "" {
		return fmt.Errorf("store: notification without key")
	}
	rows, err := affected(t.exec(`INSERT INTO pending_notifications(id,turn_id,conv_id,key,kind,text,request,attempted)
VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`, n.ID(), n.Turn, n.Conv, n.Key, n.Kind, n.Text, n.Request, n.Attempted))
	if err == nil && rows != 0 {
		t.hintNotifications(NotificationScope{ID: n.ID()})
	}
	return err
}

// NotificationChanges are bounded hints, not another delivery authority. The
// engine uses them only after a successful commit, including batched additions.
func (t *Tx) NotificationChanges() []NotificationScope { return t.addedNotifications }

// NotificationDeliveryChanged covers read-back in an inbound transaction, as
// well as sender outcomes. The caller already has the delivery key; no body or
// additional SQL query is needed to construct these post-commit hints.
func (t *Tx) NotificationDeliveryChanged(conv int64, key string) {
	t.hintNotifications(DeliveryNotificationScopes(conv, key)...)
}

func (t *Tx) hintNotifications(scopes ...NotificationScope) {
	for _, scope := range scopes {
		if len(t.addedNotifications) == 1 && t.addedNotifications[0] == (NotificationScope{}) {
			return
		}
		if len(t.addedNotifications) >= 128 {
			t.addedNotifications = []NotificationScope{{}}
			return
		}
		t.addedNotifications = append(t.addedNotifications, scope)
	}
}

func (t *Tx) Notification(id string) (PendingNotification, bool, error) {
	return one(t.row(`SELECT `+notificationCols+` FROM pending_notifications WHERE id=?`, id), scanNotification)
}

func (t *Tx) AttemptNotification(id string) (bool, error) {
	n, err := affected(t.exec(`UPDATE pending_notifications SET attempted=1 WHERE id=? AND attempted=0`, id))
	return n != 0, err
}

func (t *Tx) DeleteNotification(id string) error {
	_, err := t.exec(`DELETE FROM pending_notifications WHERE id=?`, id)
	return err
}

func (t *Tx) HasNotifications() (bool, error) {
	var hit bool
	err := t.row(`SELECT EXISTS(SELECT 1 FROM pending_notifications)`).Scan(&hit)
	return hit, err
}

// NotificationPage uses the insertion sequence, never OFFSET. The correlated
// range checks read only delivery evidence, not outbox bodies. Unknown notices
// are excluded before loading their text and cannot occupy a ready batch.
// A pass still visits pending metadata; its cost is reported separately from
// historical outbox size. It stores no duplicate ready/confirmed state.
func (t *Tx) NotificationPage(after int64, limit int, dropPassive bool) ([]PendingNotification, int64, error) {
	return t.NotificationPageFor(NotificationScope{}, after, limit, dropPassive)
}

func notificationPageQuery(scope NotificationScope) (string, []any) {
	where := ""
	var args []any
	if scope.ID != "" {
		where = " AND id=?"
		args = append(args, scope.ID)
	} else if scope.Conv != 0 {
		where = " AND conv_id=?"
		args = append(args, scope.Conv)
	}
	if scope.Questions {
		where += " AND kind='question'"
	}
	return `SELECT seq,` + notificationCols + ` FROM pending_notifications n
WHERE seq>?` + where + ` AND (kind='question' OR (? AND kind IN ('ready','service','attention') AND attempted=0) OR COALESCE((
 SELECT MIN(o.state) FROM outbox o WHERE o.delivery_key>=
 CASE WHEN n.turn_id<>0 THEN 'turn:'||n.turn_id||':' ELSE n.key END
 AND o.delivery_key<CASE WHEN n.turn_id<>0 THEN 'turn:'||n.turn_id||':' ELSE n.key END||char(1114111)
 AND o.state IN ('unknown','dropped') AND (n.turn_id=0 OR o.kind='final')),'')<>'unknown') ORDER BY seq LIMIT ?`, args
}

func (t *Tx) NotificationPageFor(scope NotificationScope, after int64, limit int, dropPassive bool) ([]PendingNotification, int64, error) {
	query, scoped := notificationPageQuery(scope)
	args := append([]any{after}, scoped...)
	args = append(args, dropPassive, limit)
	rows, err := t.rows(query, args...)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	var out []PendingNotification
	bytes := 0
	for rows.Next() {
		var n PendingNotification
		if err = rows.Scan(&after, &n.Turn, &n.Conv, &n.Key, &n.Kind, &n.Text, &n.Request, &n.Attempted); err != nil {
			return nil, after, err
		}
		out = append(out, n)
		bytes += len(n.Text) + len(n.Request) + len(n.Key)
		if bytes >= 1<<20 {
			break
		} // one oversized object may exceed the page budget
	}
	return out, after, rows.Err()
}

// DeliveryNotificationScopes uses the existing delivery-key hierarchy. Keyed
// notices are prefix IDs; native finals use turn:<id>. No readiness is stored.
func DeliveryNotificationScopes(conv int64, key string) []NotificationScope {
	var scopes []NotificationScope
	for i := 0; i < len(key); i++ {
		if key[i] == ':' {
			scopes = append(scopes, NotificationScope{ID: key[:i+1]})
			if strings.HasPrefix(key, "turn:") && i > len("turn:") {
				scopes = append(scopes, NotificationScope{ID: key[:i]})
				break
			}
		}
	}
	if key != "" {
		scopes = append(scopes, NotificationScope{ID: key})
	}
	if conv != 0 {
		scopes = append(scopes, NotificationScope{Conv: conv, Questions: true})
	}
	return scopes
}

func (t *Tx) ActiveUnder(prefix string) (bool, error) {
	var hit bool
	err := t.row(`SELECT EXISTS(SELECT 1 FROM outbox WHERE delivery_key>=? AND delivery_key<? AND state IN ('queued','sending'))`, prefix, prefix+"\U0010FFFF").Scan(&hit)
	return hit, err
}
