package tui

import (
	"context"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

// latestWorker has one running job, one pending job and one completed result.
// submit/take/cancel are owned by the UI goroutine. Jobs never access uiState.
// A non-cancellable reader may finish its current read after close, but cannot
// publish it or start further reads. Layout jobs check cancellation while working.
type workerJob[J any] struct {
	id    uint64
	ctx   context.Context
	value J
}
type workerResult[R any] struct {
	id    uint64
	value R
}
type latestWorker[J, R any] struct {
	ctx       context.Context
	jobs      chan workerJob[J]
	results   chan workerResult[R]
	sequence  uint64
	cancelJob context.CancelFunc
	exited    chan struct{}
}

func newLatestWorker[J, R any](ctx context.Context, wake func(context.Context), run func(context.Context, J) R) *latestWorker[J, R] {
	w := &latestWorker[J, R]{ctx: ctx, jobs: make(chan workerJob[J], 1), results: make(chan workerResult[R], 1), exited: make(chan struct{})}
	go func() {
		defer close(w.exited)
		for {
			select {
			case <-ctx.Done():
				return
			case j := <-w.jobs:
				if j.ctx.Err() != nil {
					continue
				}
				result := run(j.ctx, j.value)
				if j.ctx.Err() != nil {
					continue
				}
				// Only this goroutine publishes; the UI may concurrently consume.
				select {
				case <-w.results:
				default:
				}
				select {
				case <-j.ctx.Done():
					continue
				case w.results <- workerResult[R]{id: j.id, value: result}:
				}
				wake(j.ctx)
			}
		}
	}()
	return w
}

func (w *latestWorker[J, R]) cancel() {
	if w.cancelJob != nil {
		w.cancelJob()
		w.cancelJob = nil
	}
	w.sequence++
	select {
	case <-w.jobs:
	default:
	}
	select {
	case <-w.results:
	default:
	}
}
func (w *latestWorker[J, R]) submit(value J) {
	w.cancel()
	ctx, cancel := context.WithCancel(w.ctx)
	w.cancelJob = cancel
	w.jobs <- workerJob[J]{id: w.sequence, ctx: ctx, value: value}
}
func (w *latestWorker[J, R]) take() (R, bool) {
	select {
	case r := <-w.results:
		if r.id == w.sequence && w.ctx.Err() == nil {
			return r.value, true
		}
	default:
	}
	var zero R
	return zero, false
}

type previewReadJob struct {
	key  string
	meta previewCacheMeta
}
type layoutJob struct {
	key, search    string
	lines, wrapped []string
	width          int
}
type layoutResult struct {
	entry   previewLinesCacheEntry
	search  string
	matches []int
}
type refreshJob struct{ preserve bool }
type refreshResult struct {
	projectLoadEvent
	preserve bool
}
type uiWorkers struct {
	screen      tcell.Screen
	read        *latestWorker[previewReadJob, previewEvent]
	layout      *latestWorker[layoutJob, layoutResult]
	refresh     *latestWorker[refreshJob, refreshResult]
	refreshBusy bool
	readKey     string
}

func newUIWorkers(ctx context.Context, done <-chan struct{}, screen tcell.Screen, opts Options) *uiWorkers {
	wake := func(ctx context.Context) {
		postUIEventWithRetry(ctx, done, screen, &uiEvent{when: time.Now(), kind: "work"})
	}
	return &uiWorkers{
		screen: screen,
		read: newLatestWorker(ctx, wake, func(ctx context.Context, j previewReadJob) previewEvent {
			text, err := codexhistory.ReadSessionPreviewText(j.meta.path, j.meta.maxMessages, 0)
			return previewEvent{cacheKey: j.key, meta: j.meta, text: text, err: err}
		}),
		layout: newLatestWorker(ctx, wake, prepareLayout),
		refresh: newLatestWorker(ctx, wake, func(ctx context.Context, j refreshJob) refreshResult {
			projects, err := opts.LoadProjects(ctx)
			return refreshResult{projectLoadEvent: projectLoadEvent{projects: projects, err: err}, preserve: j.preserve}
		}),
	}
}

func prepareLayout(ctx context.Context, j layoutJob) layoutResult {
	lines := j.wrapped
	if lines == nil {
		lines = buildWrappedLinesContext(ctx, j.lines, j.width)
	}
	result := layoutResult{entry: previewLinesCacheEntry{key: j.key, lines: lines}, search: j.search}
	if ctx.Err() != nil {
		return result
	}
	result.entry.cost = previewLinesCost(lines)
	n := strings.ToLower(strings.TrimSpace(j.search))
	if n != "" {
		for i, line := range lines {
			if ctx.Err() != nil {
				return result
			}
			if strings.Contains(strings.ToLower(line), n) {
				result.matches = append(result.matches, i)
			}
		}
	}
	return result
}

func (w *uiWorkers) requestRefresh(preserve bool) {
	// Timer ticks are coalesced. An explicit refresh supersedes an automatic one.
	if w.refreshBusy && preserve {
		return
	}
	w.refreshBusy = true
	w.refresh.submit(refreshJob{preserve: preserve})
}

func (s *uiState) applyWorkerResults() {
	w := s.workers
	if ev, ok := w.refresh.take(); ok {
		w.refreshBusy = false
		if ev.err != nil {
			s.loadError = ev.err
		} else {
			s.loadError = nil
			s.projects = ev.projects
			s.projectRevision++
			if ev.preserve {
				// Capture at application, not request: navigation continues during loading.
				s.pendingSelection = s.currentSelection
			} else {
				s.projectState = listState{}
				s.sessionState = listState{}
				s.previewState = previewState{}
				s.currentSelection = selectionIdentity{}
				s.pendingSelection = selectionIdentity{}
			}
		}
	}
	if ev, ok := w.read.take(); ok {
		applyPreviewEvent(s, ev)
	}
}

func (s *uiState) selectPreviewRead(key string) {
	if s.workers.readKey == key {
		return
	}
	s.workers.read.cancel()
	s.workers.readKey = key
	// Cancelled pending jobs must not leave permanent "Loading" markers.
	clear(s.previewLoading)
	clear(s.previewError)
}

// Keep the original text as a shared immutable string. In particular, adding a
// refresh/error footer must not copy a multi-megabyte preview on the UI thread.
func (s *uiState) previewLayoutInput(project codexhistory.Project, session *codexhistory.Session, subagent *codexhistory.SubagentSession, isNew bool, opts Options) []string {
	key := previewCacheKey(session, subagent)
	text, footer := "", ""
	entry, exists := s.previewCache[key]
	if exists {
		text = entry.text
	}
	if errEntry, ok := s.previewError[key]; ok && errEntry.message != "" {
		if text != "" {
			footer = "Preview refresh failed: " + errEntry.message
		} else {
			text = "Preview failed: " + errEntry.message
		}
	} else if _, loading := s.previewLoading[key]; loading {
		if text != "" {
			footer = "Refreshing preview..."
		} else {
			text = "Loading preview..."
		}
	}
	lines := buildPreviewLines(project, session, subagent, isNew, s, text, opts)
	// buildPreviewLines suppresses the body for errors, initial loading and New.
	if footer != "" && s.loadError == nil && !shouldShowLoadingRows(s) && session != nil && !isNew {
		lines = append(lines, "", footer)
	}
	return lines
}

func (s *uiState) asyncPreviewLines(project codexhistory.Project, session *codexhistory.Session, subagent *codexhistory.SubagentSession, isNew bool, opts Options, width int) []string {
	key := previewLinesCacheKey(s, project, session, subagent, isNew, opts, width)
	request := key + "|search:" + s.previewSearch
	if request != s.layoutRequest {
		s.workers.layout.cancel()
		s.layoutRequest = request
		s.layoutSubmitted = ""
		s.previewMatches = nil
		s.previewMatchIdx = 0
	}
	if result, ok := s.workers.layout.take(); ok && result.entry.key == key && result.search == s.previewSearch {
		s.previewLines = result.entry
		rememberPreviewLines(s, result.entry)
		s.previewMatches = result.matches
		s.previewSearchKey = request
		if len(result.matches) > 0 {
			s.previewState.scroll = previewScrollToMatch(result.matches[0], max(0, computeLayout(s.workers.screen, max(1, s.statusHeight)).preview.h-2))
		}
	}
	var entry previewLinesCacheEntry
	if s.previewLines.key == key {
		entry = s.previewLines
	} else {
		entry = s.previewLinesCache[key]
	}
	if entry.key != "" {
		s.previewLines = entry
		if _, ok := s.previewLinesCache[key]; ok {
			touchPreviewLinesCache(s, key)
		}
		s.layoutPending = false
		if s.previewSearch == "" {
			s.previewSearchKey = request
			return entry.lines
		}
		if s.previewSearchKey == request {
			return entry.lines
		}
	}
	// sequence is advanced only on a changed request. A separate marker prevents
	// every paint from re-submitting and starving the same job.
	if s.layoutSubmitted != request {
		s.layoutSubmitted = request
		s.workers.layout.submit(layoutJob{key: key, search: s.previewSearch, lines: s.previewLayoutInput(project, session, subagent, isNew, opts), wrapped: entry.lines, width: width})
	}
	if entry.key != "" {
		return entry.lines
	}
	s.layoutPending = true
	// Never borrow a different session's body for this placeholder.
	return []string{"Preparing preview..."}
}

const previewTextCacheMaxEntries = 6
const previewTextCacheMaxBytes = 24 * 1024 * 1024

func (s *uiState) rememberPreview(key string, entry previewCacheEntry) {
	if s.previewCache == nil {
		s.previewCache = make(map[string]previewCacheEntry)
	}
	s.forgetPreview(key)
	s.previewCache[key] = entry
	s.previewCacheOrder = append(s.previewCacheOrder, key)
	s.previewCacheBytes += len(entry.text)
	// One selected oversized preview is allowed, but never accumulates siblings.
	// Complete-history semantics are preserved; its peak is measured separately.
	for len(s.previewCacheOrder) > 1 && (len(s.previewCacheOrder) > previewTextCacheMaxEntries || s.previewCacheBytes > previewTextCacheMaxBytes) {
		s.forgetPreview(s.previewCacheOrder[0])
	}
}
func (s *uiState) forgetPreview(key string) {
	entry := s.previewCache[key]
	delete(s.previewCache, key)
	for i, k := range s.previewCacheOrder {
		if k == key {
			s.previewCacheBytes -= len(entry.text)
			s.previewCacheOrder = append(s.previewCacheOrder[:i], s.previewCacheOrder[i+1:]...)
			break
		}
	}
}

func buildWrappedLinesContext(ctx context.Context, lines []string, width int) []string {
	if width <= 0 {
		return nil
	}
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if ctx.Err() != nil {
			return nil
		}
		out = append(out, wrapTextContext(ctx, line, width)...)
	}
	return out
}
