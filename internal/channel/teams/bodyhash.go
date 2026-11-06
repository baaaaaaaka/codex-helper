package teams

import (
	"encoding/json"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// BodyHash returns the own-message hash (channel.TextHash of the visible
// text) of a stored part body, the same value Render put in Part.Hash. The
// core uses it to re-hash outbox rows imported from the legacy helper
// (store.Tx.RehashLegacy). A body that is not a chatMessage hashes its raw
// text.
func (a *Adapter) BodyHash(body string) string {
	var m struct {
		Body struct {
			Content string `json:"content"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(body), &m); err != nil || m.Body.Content == "" {
		return channel.TextHash(plainText(body))
	}
	return channel.TextHash(plainText(m.Body.Content))
}
