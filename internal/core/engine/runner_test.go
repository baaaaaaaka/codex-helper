package engine

import (
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// B5: the runner's sweep looks only at the conversations that have a
// queued turn (one read query), not at every conversation with a write
// transaction each; a conversation a wake marked is still checked.
func TestRunnerSweepChecksOnlyConversationsWithAQueuedTurn(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "core.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	e := &Engine{st: st, t: ProductionTiming(), now: time.Now, log: io.Discard, convs: map[int64]*convState{}, runnerDirty: map[int64]bool{}}
	var ids []int64
	if err := st.Update(func(tx *store.Tx) error {
		for i := 0; i < 20; i++ {
			chat := fmt.Sprintf("c%d", i)
			id, err := tx.CreateConversation(store.NewConversation{Label: fmt.Sprintf("s%03d", i), Kind: store.KindWork, Channel: "teams", ExtChatID: chat})
			if err != nil {
				return err
			}
			ids = append(ids, id)
			e.convs[id] = &convState{kind: store.KindWork, state: store.ConvActive, chat: chat}
		}
		_, err := tx.AdmitWindow("c7", ids[7], []store.InboundMsg{{ExtMsgID: "m1", ExtTime: 1, Sender: store.SenderOwner, Kind: store.InPrompt, Body: "hi"}}, "cur")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	e.runnerDirty[ids[3]] = true
	got := e.takeDirty(true)
	slices.Sort(got)
	if want := []int64{ids[3], ids[7]}; !slices.Equal(got, want) {
		t.Fatalf("sweep checks %v, want %v (the queued turn's and the woken conversation)", got, want)
	}
	if got := e.takeDirty(false); len(got) != 0 {
		t.Fatalf("wake without marks checks %v", got)
	}
}
