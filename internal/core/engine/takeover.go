package engine

import (
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// metaOwnerHost is the host of the last helper that served the store.
const metaOwnerHost = "owner_host"

// announceTakeover tells the owner, once, that the helper now runs on
// another host of a shared home: the notice is enqueued in the same
// transaction that records the new host, so a crash cannot lose or repeat
// it. The first run records the host silently, and a restart on the same
// host writes nothing.
func (e *Engine) announceTakeover() error {
	host := strings.TrimSpace(e.cfg.Host)
	if host == "" {
		return nil
	}
	return e.update(func(tx *store.Tx) error {
		prev, _, err := tx.Meta(metaOwnerHost)
		if err != nil {
			return err
		}
		prev = strings.TrimSpace(prev)
		if strings.EqualFold(prev, host) {
			return nil
		}
		if prev != "" {
			ended := "stopped"
			if e.cfg.TakeoverWhy == "stale" {
				ended = "stopped answering"
			}
			text := fmt.Sprintf("🔁 The helper now runs on %s: it took over from %s, whose helper %s.", host, prev, ended)
			base := fmt.Sprintf("takeover:%d:%s", e.st.Generation(), host)
			if err := e.enqueueNotice(tx, e.controlID, base, text); err != nil {
				return err
			}
		}
		return tx.SetMeta(metaOwnerHost, host)
	})
}
