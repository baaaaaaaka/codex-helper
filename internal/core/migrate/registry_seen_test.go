package migrate

import (
	"os"
	"path/filepath"
	"testing"
)

// The old helper's registry seen ids are imported as done inbound rows also
// for a chat that has inbound rows (the control chat): the old helper
// recorded the control commands it had handled only there, so a seen
// command in the receiver's re-read overlap would run again. An id that is
// already an inbound row keeps that row.
func TestRegistrySeenIDsImportedForChatsWithInboundRows(t *testing.T) {
	dir := fixture(t, "rc59", "json")
	reg := `{"control_chat_id":"19:control@thread.v2","chats":{"19:control@thread.v2":{"seen_message_ids":["1788256829000","1788256815000"]}}}`
	if err := os.WriteFile(filepath.Join(dir, "registry.json"), []byte(reg), 0o600); err != nil {
		t.Fatal(err)
	}
	rep, db := dryRun(t, dir, nil)
	eq(t, "seen command", qs(t, db, `SELECT state FROM inbound WHERE ext_msg_id = '1788256829000'`), "done")
	eq(t, "seen command's chat", qs(t, db, `SELECT c.ext_chat_id FROM inbound i JOIN conversations c ON c.id = i.conv_id WHERE i.ext_msg_id = '1788256829000'`), "19:control@thread.v2")
	eq(t, "existing row kept", qi(t, db, `SELECT COUNT(*) FROM inbound WHERE ext_msg_id = '1788256815000'`), int64(1))
	e := rep.E("registry seen ids")
	eq(t, "imported", e.Outcomes["imported (done)"], 1)
	eq(t, "already a row", e.Outcomes["already an inbound row"], 1)
}
