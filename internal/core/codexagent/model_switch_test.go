package codexagent

import (
	"context"
	"testing"
)

func TestModelSwitchInterruptCannotTargetLaterExecution(t *testing.T) {
	rt := &runningTurn{clientID: "later-execution"}
	a := &Agent{running: map[int64]*runningTurn{1: rt}}
	if err := a.InterruptExecution(context.Background(), 1, "old-execution"); err != nil {
		t.Fatal(err)
	}
	if rt.interrupt {
		t.Fatal("stale interrupt stopped a later execution")
	}
	if err := a.InterruptExecution(context.Background(), 1, "later-execution"); err != nil {
		t.Fatal(err)
	}
	if !rt.interrupt {
		t.Fatal("matching startup interrupt was lost")
	}
}
