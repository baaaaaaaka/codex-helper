package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func TestPersistRetriesStateWithoutRepeatingSend(t *testing.T) {
	r := newRig(t)
	r.control()
	writes := 0
	err := r.e.persist(context.Background(), "accepted send", func(tx *store.Tx) error {
		writes++
		if writes < 3 {
			return errors.New("temporary disk failure")
		}
		return tx.SetMeta("engine_control_user_title", "yes")
	})
	if err != nil || writes != 3 {
		t.Fatalf("persist: attempts=%d error=%v", writes, err)
	}
	v, ok, err := r.st.Meta("engine_control_user_title")
	if err != nil || !ok || v != "yes" {
		t.Fatal("accepted outcome lost")
	}
}
