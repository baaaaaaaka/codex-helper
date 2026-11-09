package tui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker")
		var zero T
		return zero
	}
}
func waitWorker[R any, J any](t *testing.T, w *latestWorker[J, R]) R {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		if r, ok := w.take(); ok {
			return r
		}
		select {
		case <-deadline:
			t.Fatal("worker did not publish result")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestLatestWorkerCoalescesAndRejectsUncancellableOldRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan int, 4)
	release := make(chan struct{})
	w := newLatestWorker(ctx, func(context.Context) {}, func(ctx context.Context, i int) int {
		started <- i
		if i == 1 {
			<-release
		} // Simulate the existing reader's lack of cancellation.
		return i
	})
	w.submit(1)
	if got := receive(t, started); got != 1 {
		t.Fatal(got)
	}
	for i := 2; i <= 100; i++ {
		w.submit(i)
	}
	if len(w.jobs) != 1 {
		t.Fatalf("pending=%d", len(w.jobs))
	}
	close(release)
	if got := receive(t, started); got != 100 {
		t.Fatalf("ran obsolete pending job %d", got)
	}
	if got := waitWorker(t, w); got != 100 {
		t.Fatalf("published stale result %d", got)
	}
	cancel()
	receive(t, w.exited)
}

func TestLatestWorkerCancelsActiveAndGenerationABA(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan int, 4)
	stopped := make(chan struct{}, 4)
	w := newLatestWorker(ctx, func(context.Context) {}, func(ctx context.Context, j int) int {
		started <- j
		if j == 1 {
			<-ctx.Done()
			stopped <- struct{}{}
		}
		return j
	})
	w.submit(1)
	receive(t, started)
	w.submit(2)
	receive(t, stopped)
	if got := waitWorker(t, w); got != 2 {
		t.Fatal(got)
	}
	old := w.sequence
	if got := receive(t, started); got != 2 {
		t.Fatal(got)
	}
	w.submit(1)
	if got := receive(t, started); got != 1 {
		t.Fatal(got)
	}
	w.results <- workerResult[int]{id: old, value: 1}
	if _, ok := w.take(); ok {
		t.Fatal("old generation accepted after A -> B -> A")
	}
	w.cancel()
	receive(t, stopped)
	if _, ok := w.take(); ok {
		t.Fatal("cancelled result remained applicable")
	}
	cancel()
	receive(t, w.exited)
}

func TestLatestWorkerCloseDuringReadAndFullWakeQueue(t *testing.T) {
	for _, blockRead := range []bool{false, true} {
		t.Run(fmt.Sprint(blockRead), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			screen := newTestScreen(t, 80, 24)
			// A full screen queue must not retain the worker after cancellation.
			for screen.PostEvent(tcell.NewEventKey(tcell.KeyDown, 0, 0)) == nil {
			}
			started := make(chan struct{})
			release := make(chan struct{})
			woke := make(chan struct{}, 1)
			w := newLatestWorker(ctx, func(ctx context.Context) {
				woke <- struct{}{}
				postUIEventWithRetry(ctx, nil, screen, &uiEvent{kind: "work"})
			}, func(context.Context, int) int {
				close(started)
				if blockRead {
					<-release
				}
				return 1
			})
			w.submit(1)
			receive(t, started)
			if !blockRead {
				receive(t, woke)
			}
			cancel()
			close(release)
			receive(t, w.exited)
			if _, ok := w.take(); ok {
				t.Fatal("result after shutdown")
			}
		})
	}
}

func TestPrepareLayoutPreservesUnicodeWrappingAndSearch(t *testing.T) {
	texts := []string{"", "a\n\nb\n", "中文🙂e\u0301\tZ", strings.Repeat("🙂中文 abc e\u0301\n", 4000), strings.Repeat("巨", 10000)}
	for _, width := range []int{0, 1, 2, 17, 80} {
		for _, text := range texts {
			lines := []string{"Session:", text, "footer"}
			want := buildWrappedLines(lines, width)
			got := prepareLayout(context.Background(), layoutJob{key: "k", lines: lines, width: width, search: "文"})
			if !reflect.DeepEqual(got.entry.lines, want) {
				t.Fatalf("wrap differs at width=%d bytes=%d", width, len(text))
			}
			wantMatches := previewFindMatches(want, "文")
			if len(got.matches) != len(wantMatches) || len(got.matches) > 0 && !reflect.DeepEqual(got.matches, wantMatches) {
				t.Fatal("search differs")
			}
			reused := prepareLayout(context.Background(), layoutJob{key: "k", wrapped: want, width: width, search: "SESSION"})
			if len(want) > 0 && &reused.entry.lines[0] != &want[0] {
				t.Fatal("search copied wrapped lines")
			}
		}
	}
}
func TestLayoutCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := buildWrappedLinesContext(ctx, []string{strings.Repeat("x", 1000000)}, 20); got != nil {
		t.Fatal("cancelled wrap produced lines")
	}
	if got := wrapTextContext(ctx, strings.Repeat("x", 1000000), 20); got != nil {
		t.Fatal("cancelled long line produced lines")
	}
}

func asyncState(t *testing.T, projects []codexhistory.Project) (*uiState, tcell.Screen) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	screen := newTestScreen(t, 160, 50)
	state := newTestState(projects)
	state.workers = newUIWorkers(ctx, nil, screen, Options{LoadProjects: func(context.Context) ([]codexhistory.Project, error) { return nil, nil }})
	initialLayout, initialRead, initialRefresh := state.workers.layout, state.workers.read, state.workers.refresh
	t.Cleanup(func() {
		cancel()
		if state.workers.layout != initialLayout {
			receive(t, initialLayout.exited)
		}
		if state.workers.read != initialRead {
			receive(t, initialRead.exited)
		}
		if state.workers.refresh != initialRefresh {
			receive(t, initialRefresh.exited)
		}
		receive(t, state.workers.layout.exited)
		receive(t, state.workers.refresh.exited)
		receive(t, state.workers.read.exited)
	})
	return state, screen
}
func settleLayout(t *testing.T, s *uiState, p codexhistory.Project, session *codexhistory.Session, width int) []string {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		lines := s.asyncPreviewLines(p, session, nil, false, Options{}, width)
		if !s.layoutPending && s.previewSearchKey == s.layoutRequest {
			return lines
		}
		select {
		case <-deadline:
			t.Fatal("layout did not settle")
		case <-time.After(time.Millisecond):
		}
	}
}
func TestPreviewLayoutInputMatchesExistingBodyAndFooters(t *testing.T) {
	p := codexhistory.Project{Path: "/project"}
	session := &codexhistory.Session{SessionID: "a"}
	for _, text := range []string{"", "body", "body\n", strings.Repeat("large\n", 10000)} {
		for _, status := range []string{"normal", "refresh", "error"} {
			s := newTestState([]codexhistory.Project{p})
			key := previewCacheKey(session, nil)
			s.previewCache[key] = previewCacheEntry{text: text}
			if status == "refresh" {
				s.previewLoading[key] = previewCacheMeta{}
			}
			if status == "error" {
				s.previewError[key] = previewErrorEntry{message: "failure"}
			}
			want := buildWrappedLines(buildPreviewLines(p, session, nil, false, s, previewTextForItem(s, session, nil), Options{}), 40)
			got := buildWrappedLines(s.previewLayoutInput(p, session, nil, false, Options{}), 40)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("body/footer mismatch status=%s textbytes=%d", status, len(text))
			}
		}
	}
}
func TestAsyncLayoutSwitchResizeRevisionSearchAndScroll(t *testing.T) {
	p := codexhistory.Project{Path: "/project"}
	a, b := &codexhistory.Session{SessionID: "A"}, &codexhistory.Session{SessionID: "B"}
	s, _ := asyncState(t, []codexhistory.Project{p})
	s.rememberPreview(previewCacheKey(a, nil), previewCacheEntry{text: strings.Repeat("alpha 中文\n", 200), revision: "a1"})
	s.rememberPreview(previewCacheKey(b, nil), previewCacheEntry{text: "only-B", revision: "b1"})
	first := settleLayout(t, s, p, a, 40)
	s.previewState.scroll = 100
	placeholder := s.asyncPreviewLines(p, b, nil, false, Options{}, 40)
	if !s.layoutPending || strings.Contains(strings.Join(placeholder, "\n"), "alpha") {
		t.Fatal("new selection displayed old body")
	}
	if s.previewState.scroll != 100 {
		t.Fatal("placeholder reset scroll")
	}
	// A -> B -> A while B is pending must neither apply B nor strand A.
	again := settleLayout(t, s, p, a, 40)
	if !reflect.DeepEqual(first, again) {
		t.Fatal("A -> B -> A changed body")
	}
	readyB := settleLayout(t, s, p, b, 40)
	if !strings.Contains(strings.Join(readyB, "\n"), "only-B") {
		t.Fatal("missing B")
	}
	s.rememberPreview(previewCacheKey(a, nil), previewCacheEntry{text: "revised needle\n" + strings.Repeat("尾\n", 100), revision: "a2"})
	s.previewSearch = "needle"
	resized := settleLayout(t, s, p, a, 1)
	want := buildWrappedLines(buildPreviewLines(p, a, nil, false, s, previewTextForItem(s, a, nil), Options{}), 1)
	if !reflect.DeepEqual(resized, want) {
		t.Fatal("revision/resize mismatch")
	}
	s.previewSearch = "尾"
	settled := settleLayout(t, s, p, a, 40)
	if len(s.previewMatches) != 100 {
		t.Fatalf("matches=%d", len(s.previewMatches))
	}
	s.previewSearch = "revised"
	next := settleLayout(t, s, p, a, 40)
	if &settled[0] != &next[0] {
		t.Fatal("search rewrapped whole body")
	}
	if len(s.previewMatches) != 1 {
		t.Fatal("obsolete search result applied")
	}
	s.previewSearch = ""
	settleLayout(t, s, p, a, 40)
	if len(s.previewMatches) != 0 {
		t.Fatal("clear search kept matches")
	}
}

func TestPreviewTextCacheBudgetAndLoadingCancellation(t *testing.T) {
	s, _ := asyncState(t, nil)
	for i := 0; i < 20; i++ {
		s.rememberPreview(fmt.Sprint(i), previewCacheEntry{text: strings.Repeat("x", 100)})
	}
	if len(s.previewCache) != previewTextCacheMaxEntries || s.previewCacheBytes != 600 {
		t.Fatal("entry budget/accounting")
	}
	s.rememberPreview("huge", previewCacheEntry{text: strings.Repeat("x", previewTextCacheMaxBytes+1)})
	if len(s.previewCache) != 1 {
		t.Fatal("oversized current body retained siblings")
	}
	s.rememberPreview("small", previewCacheEntry{text: "x"})
	if len(s.previewCache) != 1 || s.previewCacheBytes != 1 {
		t.Fatal("oversized old body was retained")
	}
	s.previewLoading["old"] = previewCacheMeta{}
	s.selectPreviewRead("new")
	if len(s.previewLoading) != 0 {
		t.Fatal("cancelled load marker retained")
	}
}

func TestListViewsInvalidateAndShareNavigationSnapshot(t *testing.T) {
	p := codexhistory.Project{Key: "p", Path: "/p", Sessions: []codexhistory.Session{{SessionID: "A", FirstPrompt: "alpha", Subagents: []codexhistory.SubagentSession{{AgentID: "child", ParentSessionID: "A"}}}}}
	s := newTestState(nil)
	if len(s.projectItems(Options{})) != 0 {
		t.Fatal("expected initial empty lists")
	}
	s.projects = []codexhistory.Project{p}
	s.projectRevision++
	projects := s.projectItems(Options{})
	if len(projects) != 1 {
		t.Fatal("initial loading cache not invalidated")
	}
	if &projects[0] != &s.projectItems(Options{})[0] {
		t.Fatal("project snapshot not reused")
	}
	sessions := s.sessionItems(projects[0].project)
	if &sessions[0] != &s.sessionItems(projects[0].project)[0] {
		t.Fatal("session snapshot not reused")
	}
	screen := newTestScreen(t, 160, 50)
	s.focus = "sessions"
	s.sessionState.selected = 1
	if _, err := handleKey(context.Background(), screen, s, Options{}, tcell.NewEventKey(tcell.KeyCtrlO, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if len(s.sessionItems(p)) != 3 {
		t.Fatal("expansion not invalidated")
	}
	s.sessionFilter = "missing"
	if len(s.sessionItems(p)) != 1 {
		t.Fatal("session filter not invalidated")
	}
	s.sessionFilter = ""
	s.projectFilter = "missing"
	if len(s.projectItems(Options{})) != 0 {
		t.Fatal("project filter not invalidated")
	}
	if len(s.projectItems(Options{DefaultCwd: "/current"})) != 1 {
		t.Fatal("cwd pin not invalidated")
	}
	s.projectFilter = ""
	s.projectRevision++
	s.projects = []codexhistory.Project{{Path: "/new"}}
	if got := s.projectItems(Options{}); len(got) != 1 || got[0].project.Path != "/new" {
		t.Fatal("refresh not invalidated")
	}
}

func TestAsyncRefreshCoalescesPreservesApplicationSelectionAndErrors(t *testing.T) {
	p := codexhistory.Project{Path: "/p", Sessions: []codexhistory.Session{{SessionID: "A"}, {SessionID: "B"}}}
	s, _ := asyncState(t, []codexhistory.Project{p})
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	s.workers.refresh = newLatestWorker(s.workers.read.ctx, func(context.Context) {}, func(ctx context.Context, j refreshJob) refreshResult {
		calls.Add(1)
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return refreshResult{projectLoadEvent: projectLoadEvent{projects: []codexhistory.Project{p}}, preserve: j.preserve}
	})
	s.currentSelection = selectionIdentity{project: projectIdentity(p), session: sessionItemSelectionIdentity(sessionItem{kind: sessionItemMain, session: p.Sessions[0]})}
	s.workers.requestRefresh(true)
	receive(t, started)
	for i := 0; i < 50; i++ {
		s.workers.requestRefresh(true)
	}
	if calls.Load() != 1 || len(s.workers.refresh.jobs) != 0 {
		t.Fatal("automatic refresh accumulated jobs")
	}
	// User has moved since the refresh began.
	s.currentSelection.session = sessionItemSelectionIdentity(sessionItem{kind: sessionItemMain, session: p.Sessions[1]})
	close(release)
	r := waitWorker(t, s.workers.refresh)
	// Reinsert a completed, matching worker result to exercise UI application.
	s.workers.refresh.results <- workerResult[refreshResult]{id: s.workers.refresh.sequence, value: r}
	s.applyWorkerResults()
	_, _, item, _ := resolveSelectionForDraw(s, s.projectItems(Options{}))
	if item.session.SessionID != "B" {
		t.Fatal("refresh restored request-time selection")
	}
	old := s.projects
	s.workers.refresh.results <- workerResult[refreshResult]{id: s.workers.refresh.sequence, value: refreshResult{projectLoadEvent: projectLoadEvent{err: errors.New("failure")}}}
	s.applyWorkerResults()
	if s.loadError == nil || !reflect.DeepEqual(s.projects, old) {
		t.Fatal("failed refresh discarded last snapshot")
	}
	s.workers.refresh.results <- workerResult[refreshResult]{id: s.workers.refresh.sequence, value: refreshResult{projectLoadEvent: projectLoadEvent{projects: []codexhistory.Project{p}}, preserve: false}}
	s.applyWorkerResults()
	if s.sessionState.selected != 0 || s.currentSelection != (selectionIdentity{}) {
		t.Fatal("manual refresh did not reset selection")
	}
}

func TestSelectSessionNavigationAndQuitDuringBlockedRefresh(t *testing.T) {
	for _, quit := range []bool{false, true} {
		t.Run(fmt.Sprint(quit), func(t *testing.T) {
			screen, initDone, events := newObservedSelectSessionTestScreen(t)
			p := codexhistory.Project{Path: "/isolated-project", Sessions: []codexhistory.Session{{SessionID: "A", FirstPrompt: "alpha", ModifiedAt: time.Unix(2, 0)}, {SessionID: "B", FirstPrompt: "beta", ModifiedAt: time.Unix(1, 0)}}}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			started, stopped := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			type outcome struct {
				selection *Selection
				err       error
			}
			result := make(chan outcome, 1)
			go func() {
				selection, err := SelectSession(ctx, Options{LoadProjects: func(ctx context.Context) ([]codexhistory.Project, error) {
					if calls.Add(1) == 1 {
						return []codexhistory.Project{p}, nil
					}
					close(started)
					<-ctx.Done()
					close(stopped)
					return nil, ctx.Err()
				}})
				result <- outcome{selection, err}
			}()
			receive(t, initDone)
			if err := waitForSelectSessionUIEvent(ctx, events, "load"); err != nil {
				t.Fatal(err)
			}
			for _, ev := range []tcell.Event{tcell.NewEventKey(tcell.KeyTab, 0, 0), tcell.NewEventKey(tcell.KeyDown, 0, 0), &uiEvent{kind: "refresh"}} {
				if err := screen.PostEvent(ev); err != nil {
					t.Fatal(err)
				}
			}
			receive(t, started)
			if quit {
				if err := screen.PostEvent(tcell.NewEventKey(tcell.KeyEscape, 0, 0)); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := screen.PostEvent(tcell.NewEventKey(tcell.KeyDown, 0, 0)); err != nil {
					t.Fatal(err)
				}
				if err := screen.PostEvent(tcell.NewEventKey(tcell.KeyEnter, 0, 0)); err != nil {
					t.Fatal(err)
				}
			}
			got := receive(t, result)
			if got.err != nil {
				t.Fatal(got.err)
			}
			if !quit && (got.selection == nil || got.selection.Session.SessionID != "B") {
				t.Fatalf("navigation stalled or wrong selection: %#v", got.selection)
			}
			if quit && got.selection != nil {
				t.Fatal("quit selected a session")
			}
			receive(t, stopped)
		})
	}
}

func TestAsyncLayoutLoadingDoesNotClampSavedScroll(t *testing.T) {
	p := codexhistory.Project{Path: "/p", Sessions: []codexhistory.Session{{SessionID: "A"}}}
	s, screen := asyncState(t, []codexhistory.Project{p})
	s.sessionState.selected = 1
	s.focus = "preview"
	s.lastListFocus = "sessions"
	s.previewState.scroll = 200
	started := make(chan struct{})
	s.workers.layout = newLatestWorker(s.workers.read.ctx, func(context.Context) {}, func(ctx context.Context, j layoutJob) layoutResult {
		close(started)
		<-ctx.Done()
		return layoutResult{}
	})
	if err := draw(screen, s, Options{}, make(chan previewEvent, 1)); err != nil {
		t.Fatal(err)
	}
	receive(t, started)
	if s.previewState.scroll != 200 {
		t.Fatal("draw clamped saved scroll to placeholder height")
	}
	if _, err := handleKey(context.Background(), screen, s, Options{}, tcell.NewEventKey(tcell.KeyDown, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if s.previewState.scroll != 200 {
		t.Fatal("loading navigation clamped saved scroll")
	}
}

func BenchmarkListViewsNavigation(b *testing.B) {
	projects := make([]codexhistory.Project, 879)
	for i := range projects {
		projects[i] = codexhistory.Project{Path: fmt.Sprintf("/project/%d", i)}
		projects[i].Sessions = []codexhistory.Session{{SessionID: fmt.Sprint(i), Subagents: make([]codexhistory.SubagentSession, 8)}}
	}
	s := &uiState{projects: projects, expandedSessions: map[string]bool{}}
	s.projectItems(Options{})
	project := s.projects[0]
	s.sessionItems(project)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.projectItems(Options{})
		s.sessionItems(project)
	}
}

func TestRuntimePreviewReaderRejectsABAAndRestartsCancelledPending(t *testing.T) {
	dir := t.TempDir()
	a, b := &codexhistory.Session{SessionID: "A", FilePath: filepath.Join(dir, "a.jsonl")}, &codexhistory.Session{SessionID: "B", FilePath: filepath.Join(dir, "b.jsonl")}
	for _, session := range []*codexhistory.Session{a, b} {
		if err := os.WriteFile(session.FilePath, []byte("{}\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s, screen := asyncState(t, nil)
	started := make(chan previewReadJob, 2)
	release := make(chan struct{})
	calls := 0
	s.workers.read = newLatestWorker(s.workers.read.ctx, func(context.Context) {}, func(ctx context.Context, j previewReadJob) previewEvent {
		calls++
		started <- j
		text := "fresh"
		if calls == 1 {
			<-release
			text = "obsolete-A"
		}
		return previewEvent{cacheKey: j.key, meta: j.meta, text: text}
	})
	ch := make(chan previewEvent)
	ensurePreview(screen, s, Options{}, a, nil, ch)
	receive(t, started)
	ensurePreview(screen, s, Options{}, b, nil, ch)
	ensurePreview(screen, s, Options{}, a, nil, ch)
	close(release)
	job := receive(t, started)
	if job.key != previewCacheKey(a, nil) {
		t.Fatal("cancelled B was read")
	}
	result := waitWorker(t, s.workers.read)
	s.workers.read.results <- workerResult[previewEvent]{id: s.workers.read.sequence, value: result}
	s.applyWorkerResults()
	if got := s.previewCache[previewCacheKey(a, nil)].text; got != "fresh" {
		t.Fatalf("ABA accepted obsolete body: %q", got)
	}
	if len(s.previewLoading) != 0 {
		t.Fatal("cancelled job left loading marker")
	}
	// A selection without a source file also cancels work from the old selection.
	ensurePreview(screen, s, Options{}, &codexhistory.Session{SessionID: "empty"}, nil, ch)
	if s.workers.readKey != "" {
		t.Fatal("empty selection retained previous read")
	}
}

func BenchmarkTUIKeyAndFrame(b *testing.B) {
	projects := make([]codexhistory.Project, 879)
	for i := range projects {
		projects[i] = codexhistory.Project{Path: fmt.Sprintf("/project/%d", i), Sessions: []codexhistory.Session{{SessionID: fmt.Sprint(i), FirstPrompt: "benchmark conversation", Subagents: make([]codexhistory.SubagentSession, 8)}}}
	}
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		b.Fatal(err)
	}
	screen.SetSize(160, 50)
	b.Cleanup(screen.Fini)
	state := newTestState(projects)
	state.focus = "sessions"
	ch := make(chan previewEvent, 1)
	if err := draw(screen, state, Options{}, ch); err != nil {
		b.Fatal(err)
	}
	up, down := tcell.NewEventKey(tcell.KeyUp, 0, 0), tcell.NewEventKey(tcell.KeyDown, 0, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := up
		if i%2 == 0 {
			key = down
		}
		if _, err := handleKey(context.Background(), screen, state, Options{}, key); err != nil {
			b.Fatal(err)
		}
		if err := draw(screen, state, Options{}, ch); err != nil {
			b.Fatal(err)
		}
	}
}

// A cancellation wakeup can be refused by tcell while an input event is already
// queued. After processing that input, the event loop must observe ctx itself.
type cancelledWakeScreen struct {
	tcell.Screen
	ctx         context.Context
	initialized chan struct{}
	first       bool
}

func (s *cancelledWakeScreen) Init() error {
	if err := s.Screen.Init(); err != nil {
		return err
	}
	s.Screen.SetSize(80, 24)
	close(s.initialized)
	return nil
}
func (s *cancelledWakeScreen) PostEvent(tcell.Event) error { return tcell.ErrEventQFull }
func (s *cancelledWakeScreen) PollEvent() tcell.Event {
	if !s.first {
		s.first = true
		<-s.ctx.Done()
		return tcell.NewEventKey(tcell.KeyF1, 0, 0)
	}
	return s.Screen.PollEvent()
}
func TestSelectSessionCancellationWhenWakeQueueIsFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	screen := &cancelledWakeScreen{Screen: tcell.NewSimulationScreen("UTF-8"), ctx: ctx, initialized: make(chan struct{})}
	old := newScreen
	newScreen = func() (tcell.Screen, error) { return screen, nil }
	defer func() { newScreen = old }()
	done := make(chan error, 1)
	go func() {
		_, err := SelectSession(ctx, Options{LoadProjects: func(ctx context.Context) ([]codexhistory.Project, error) { <-ctx.Done(); return nil, ctx.Err() }})
		done <- err
	}()
	receive(t, screen.initialized)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel error=%v", err)
		}
	case <-time.After(3 * time.Second):
		// Unstick the test even when checking a regression that loses the wakeup.
		_ = screen.Screen.PostEvent(tcell.NewEventKey(tcell.KeyEscape, 0, 0))
		receive(t, done)
		t.Fatal("selector lost cancellation after a full wake queue")
	}
}
