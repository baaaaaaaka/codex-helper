package tui

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/gdamore/tcell/v2"
)

type bootOrderScreen struct {
	tcell.Screen
	shown, polled, finished atomic.Bool
}

func (s *bootOrderScreen) Init() error {
	if err := s.Screen.Init(); err != nil {
		return err
	}
	s.Screen.(tcell.SimulationScreen).SetSize(80, 24)
	return nil
}
func (s *bootOrderScreen) Show()                  { s.Screen.Show(); s.shown.Store(true) }
func (s *bootOrderScreen) PollEvent() tcell.Event { s.polled.Store(true); return s.Screen.PollEvent() }
func (s *bootOrderScreen) Fini()                  { s.finished.Store(true); s.Screen.Fini() }

func TestSelectSessionReadinessFollowsDrawAndPrecedesInput(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "failed handoff"}[fail], func(t *testing.T) {
			screen := &bootOrderScreen{Screen: tcell.NewSimulationScreen("UTF-8")}
			prev := newScreen
			newScreen = func() (tcell.Screen, error) { return screen, nil }
			t.Cleanup(func() { newScreen = prev })
			var calls int
			sentinel := errors.New("parent disappeared")
			_, err := SelectSession(context.Background(), Options{
				LoadProjects: func(context.Context) ([]codexhistory.Project, error) { return nil, nil },
				OnReady: func() error {
					calls++
					if !screen.shown.Load() || screen.polled.Load() {
						t.Error("input preceded first frame / handoff")
					}
					if fail {
						return sentinel
					}
					screen.PostEvent(tcell.NewEventKey(tcell.KeyRune, 'q', tcell.ModNone))
					return nil
				},
			})
			if calls != 1 || !screen.finished.Load() {
				t.Fatalf("ready calls=%d; terminal finalized=%v", calls, screen.finished.Load())
			}
			if fail {
				if !errors.Is(err, sentinel) || screen.polled.Load() {
					t.Fatalf("failed handoff accepted input: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
