package teams

// These tests deliberately exercise the production listener loop rather than
// calling one phase directly.  A phase-level test can prove that a helper
// returns an error, but it cannot prove that the next cycle still reaches a
// healthy chat or that durable work survives a listener restart.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	_ "modernc.org/sqlite"
)

type listenerRecoveryGraphState struct {
	mu                    sync.Mutex
	delays                map[string]time.Duration
	delayOnce             map[string]time.Duration
	messages              map[string][]ChatMessage
	sent                  []listenerRecoverySentMessage
	gets                  map[string]int
	activeGets            int
	maxActiveGets         int
	errors                []error
	requests              []string
	postWait              time.Duration
	slowGetChatIDs        map[string]bool
	slowGetBarrierTarget  int
	slowGetBarrierRelease chan struct{}
	slowGetBarrierOnce    sync.Once
	slowGetBarrierActive  int
	topicUpdateEntered    chan struct{}
	topicUpdateRelease    chan struct{}
	topicUpdateOnce       sync.Once
	// pageSize turns this fake into a stateful Graph window. Zero preserves the
	// older all-items behavior used by tests that only need a static response.
	pageSize int
	// getFailures injects bounded provider failures before a page is returned.
	// Tests use this to model a bad chat's Graph head without making the fake
	// server itself unavailable to healthy chats.
	getFailures map[string]int
	// blockContinuation pauses a selected skip-token request after the first
	// page has been durably committed.  It lets restart tests stop the first
	// listener with a real persisted frontier instead of seeding one directly.
	blockContinuationChatID string
	continuationEntered     chan struct{}
	continuationRelease     chan struct{}
	continuationOnce        sync.Once
}

type listenerRecoverySentMessage struct {
	ChatID string
	Body   string
}

// listenerRecoveryCancelAfterReadBody models the narrow race where Graph has
// returned a successful response just as the short maintenance phase expires.
// The response is already complete, so the local Accepted/Sent projection must
// use the listener-lifetime context rather than the canceled phase context.
type listenerRecoveryCancelAfterReadBody struct {
	data   []byte
	cancel context.CancelFunc
	done   bool
}

func (b *listenerRecoveryCancelAfterReadBody) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	if !b.done {
		b.done = true
		b.cancel()
	}
	return n, nil
}

func (b *listenerRecoveryCancelAfterReadBody) Close() error { return nil }

func newListenerRecoveryGraph(t *testing.T, delays map[string]time.Duration, messages map[string][]ChatMessage, postWait time.Duration) (*GraphClient, *listenerRecoveryGraphState) {
	t.Helper()
	state := &listenerRecoveryGraphState{
		delays:   delays,
		messages: messages,
		gets:     make(map[string]int),
		postWait: postWait,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		state.mu.Lock()
		state.requests = append(state.requests, r.Method+" "+r.URL.RequestURI())
		state.mu.Unlock()
		if got := r.Header.Get("Authorization"); got != "Bearer access" {
			state.mu.Lock()
			state.errors = append(state.errors, fmt.Errorf("unexpected Graph authorization %q", got))
			state.mu.Unlock()
			http.Error(w, "missing fake Graph authorization", http.StatusUnauthorized)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) == 4 && parts[0] == "chats" && parts[2] == "messages" && r.Method == http.MethodGet {
			chatID, messageID := parts[1], parts[3]
			state.mu.Lock()
			var found ChatMessage
			for _, item := range state.messages[chatID] {
				if item.ID == messageID {
					found = item
					break
				}
			}
			state.mu.Unlock()
			if found.ID == "" {
				http.Error(w, "message not found", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(found)
			return
		}
		if len(parts) == 2 && parts[0] == "chats" && r.Method == http.MethodPatch {
			state.mu.Lock()
			entered := state.topicUpdateEntered
			release := state.topicUpdateRelease
			state.mu.Unlock()
			if entered != nil && release != nil {
				state.topicUpdateOnce.Do(func() { close(entered) })
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if len(parts) == 3 && parts[0] == "chats" && parts[2] == "messages" {
			chatID := parts[1]
			switch r.Method {
			case http.MethodGet:
				if !listenerRecoveryMessagesQueryAllowed(r.URL.Query()) {
					state.mu.Lock()
					state.errors = append(state.errors, fmt.Errorf("unexpected fake Graph message query %q", r.URL.RawQuery))
					state.mu.Unlock()
					http.Error(w, "unexpected fake Graph message query", http.StatusBadRequest)
					return
				}
				state.mu.Lock()
				state.gets[chatID]++
				requestNumber := state.gets[chatID]
				delay := state.delays[chatID]
				if requestNumber == 1 {
					if firstDelay := state.delayOnce[chatID]; firstDelay > 0 {
						delay = firstDelay
					}
				}
				items := append([]ChatMessage(nil), state.messages[chatID]...)
				state.activeGets++
				if state.activeGets > state.maxActiveGets {
					state.maxActiveGets = state.activeGets
				}
				barrierSlow := state.slowGetBarrierRelease != nil && state.slowGetChatIDs[chatID]
				if barrierSlow {
					state.slowGetBarrierActive++
					if state.slowGetBarrierTarget > 0 && state.slowGetBarrierActive >= state.slowGetBarrierTarget {
						state.slowGetBarrierOnce.Do(func() { close(state.slowGetBarrierRelease) })
					}
				}
				barrierRelease := state.slowGetBarrierRelease
				state.mu.Unlock()
				defer func() {
					state.mu.Lock()
					state.activeGets--
					if barrierSlow {
						state.slowGetBarrierActive--
					}
					state.mu.Unlock()
				}()
				if barrierSlow {
					select {
					case <-barrierRelease:
					case <-r.Context().Done():
						return
					}
				}
				if delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case <-r.Context().Done():
						if !timer.Stop() {
							<-timer.C
						}
						return
					}
				}
				pageSize := 0
				fail := false
				state.mu.Lock()
				pageSize = state.pageSize
				if state.getFailures[chatID] > 0 {
					state.getFailures[chatID]--
					fail = true
				}
				state.mu.Unlock()
				if fail {
					http.Error(w, "temporary fake Graph head failure", http.StatusServiceUnavailable)
					return
				}
				state.mu.Lock()
				blockContinuation := state.blockContinuationChatID == chatID && r.URL.Query().Get("$skiptoken") != ""
				continuationEntered := state.continuationEntered
				continuationRelease := state.continuationRelease
				state.mu.Unlock()
				if blockContinuation {
					if continuationEntered != nil {
						state.continuationOnce.Do(func() { close(continuationEntered) })
					}
					select {
					case <-continuationRelease:
					case <-r.Context().Done():
						return
					}
				}
				page, nextLink, pageErr := listenerRecoveryGraphPage(r.URL.Query(), items, pageSize)
				if pageErr != nil {
					state.mu.Lock()
					state.errors = append(state.errors, pageErr)
					state.mu.Unlock()
					http.Error(w, pageErr.Error(), http.StatusBadRequest)
					return
				}
				// Keep an empty Graph collection as [] rather than JSON null. The
				// latter is a malformed message page and is intentionally rejected
				// by the production decoder.
				if page == nil {
					page = []ChatMessage{}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"value": page, "@odata.nextLink": nextLink})
				return
			case http.MethodPost:
				if state.postWait > 0 {
					timer := time.NewTimer(state.postWait)
					select {
					case <-timer.C:
					case <-r.Context().Done():
						if !timer.Stop() {
							<-timer.C
						}
						return
					}
				}
				var payload struct {
					Body struct {
						Content string `json:"content"`
					} `json:"body"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					state.mu.Lock()
					state.errors = append(state.errors, err)
					state.mu.Unlock()
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				if strings.TrimSpace(payload.Body.Content) == "" {
					state.mu.Lock()
					state.errors = append(state.errors, errors.New("fake Graph POST has empty body content"))
					state.mu.Unlock()
					http.Error(w, "empty fake Graph body", http.StatusBadRequest)
					return
				}
				state.mu.Lock()
				messageID := fmt.Sprintf("listener-sent-%d", len(state.sent)+1)
				state.sent = append(state.sent, listenerRecoverySentMessage{ChatID: chatID, Body: payload.Body.Content})
				state.mu.Unlock()
				_ = json.NewEncoder(w).Encode(map[string]any{"id": messageID, "messageType": "message"})
				return
			}
		}
		http.Error(w, "unexpected fake Graph request", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	return &GraphClient{
		auth:    &fakeGraphAuth{token: "access"},
		client:  server.Client(),
		baseURL: server.URL,
		// Zero means "use the production default" in GraphClient; use an
		// explicit small retry budget so fault counts in these tests are not
		// accidentally changed by that fallback.
		maxRetries: 1,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, state
}

func (s *listenerRecoveryGraphState) sentSnapshot() []listenerRecoverySentMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]listenerRecoverySentMessage(nil), s.sent...)
}

func (s *listenerRecoveryGraphState) getCount(chatID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[chatID]
}

func (s *listenerRecoveryGraphState) maxGetConcurrency() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxActiveGets
}

func (s *listenerRecoveryGraphState) errorsSnapshot() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errors...)
}

func (s *listenerRecoveryGraphState) requestsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

func (s *listenerRecoveryGraphState) setGetFailures(chatID string, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getFailures == nil {
		s.getFailures = make(map[string]int)
	}
	s.getFailures[chatID] = count
}

// listenerRecoveryMessagesQueryAllowed is deliberately independent of the
// production Graph query builder.  It makes the shared vertical fake reject a
// silently broadened endpoint or malformed cursor while still allowing the
// small set of Microsoft Graph message-list query forms used by the listener.
func listenerRecoveryMessagesQueryAllowed(values url.Values) bool {
	for key, items := range values {
		switch key {
		case "$top", "$orderby", "$filter", "$skiptoken":
		default:
			return false
		}
		if len(items) != 1 {
			return false
		}
	}
	if rawTop := values.Get("$top"); rawTop != "" {
		top, err := strconv.Atoi(rawTop)
		if err != nil || top <= 0 || top > 50 {
			return false
		}
	}
	if rawOrder := values.Get("$orderby"); rawOrder != "" && rawOrder != "lastModifiedDateTime desc" {
		return false
	}
	if rawFilter := values.Get("$filter"); rawFilter != "" {
		if !listenerRecoveryLastModifiedFilterAllowed(rawFilter) || values.Get("$orderby") == "" {
			return false
		}
	}
	if rawCursor := values.Get("$skiptoken"); rawCursor != "" && (len(rawCursor) > 2048 || strings.ContainsAny(rawCursor, "\r\n")) {
		return false
	}
	return true
}

func listenerRecoveryLastModifiedFilterAllowed(value string) bool {
	parts := strings.Split(strings.TrimSpace(value), " and ")
	if len(parts) > 2 {
		return false
	}
	parse := func(raw string, lower bool) (time.Time, bool) {
		fields := strings.Fields(strings.TrimSpace(raw))
		if len(fields) != 3 || fields[0] != "lastModifiedDateTime" {
			return time.Time{}, false
		}
		if lower && fields[1] != "gt" && fields[1] != "ge" {
			return time.Time{}, false
		}
		if !lower && fields[1] != "lt" && fields[1] != "le" {
			return time.Time{}, false
		}
		stamp, err := time.Parse(time.RFC3339Nano, fields[2])
		return stamp, err == nil && !stamp.IsZero()
	}
	lower, ok := parse(parts[0], true)
	if !ok || len(parts) == 1 {
		return ok
	}
	upper, ok := parse(parts[1], false)
	return ok && !upper.Before(lower)
}

// listenerRecoveryGraphPage models the part of Graph that a static fixture
// cannot catch: every request is a new view over mutable message state, and a
// page is followed only through its opaque continuation token. It deliberately
// applies the production query's filter/order/top contract before slicing.
func listenerRecoveryGraphPage(values url.Values, messages []ChatMessage, pageSize int) ([]ChatMessage, string, error) {
	if pageSize <= 0 {
		return messages, "", nil
	}
	if rawTop := values.Get("$top"); rawTop != "" {
		top, err := strconv.Atoi(rawTop)
		if err != nil || top <= 0 {
			return nil, "", fmt.Errorf("stateful fake Graph received invalid $top %q", rawTop)
		}
	}
	filtered := append([]ChatMessage(nil), messages...)
	if rawFilter := values.Get("$filter"); rawFilter != "" {
		parts := strings.Split(strings.TrimSpace(rawFilter), " and ")
		if len(parts) == 0 || len(parts) > 2 {
			return nil, "", fmt.Errorf("stateful fake Graph cannot parse filter %q", rawFilter)
		}
		parseBound := func(raw string, lower bool) (time.Time, bool, bool) {
			fields := strings.Fields(strings.TrimSpace(raw))
			if len(fields) != 3 || fields[0] != "lastModifiedDateTime" {
				return time.Time{}, false, false
			}
			if lower && fields[1] != "gt" && fields[1] != "ge" {
				return time.Time{}, false, false
			}
			if !lower && fields[1] != "lt" && fields[1] != "le" {
				return time.Time{}, false, false
			}
			stamp, err := time.Parse(time.RFC3339Nano, fields[2])
			return stamp, err == nil && !stamp.IsZero(), fields[1] == "ge" || fields[1] == "le"
		}
		lower, ok, lowerInclusive := parseBound(parts[0], true)
		if !ok {
			return nil, "", fmt.Errorf("stateful fake Graph cannot parse lower filter bound %q", rawFilter)
		}
		upper := time.Time{}
		upperInclusive := false
		if len(parts) == 2 {
			var upperOK bool
			upper, upperOK, upperInclusive = parseBound(parts[1], false)
			if !upperOK {
				return nil, "", fmt.Errorf("stateful fake Graph cannot parse upper filter bound %q", rawFilter)
			}
		}
		filtered = filtered[:0]
		for _, message := range messages {
			stamp, err := time.Parse(time.RFC3339Nano, message.LastModifiedDateTime)
			if err != nil {
				return nil, "", fmt.Errorf("stateful fake Graph message %q has invalid modified time: %w", message.ID, err)
			}
			lowerExcluded := stamp.Before(lower) || (!lowerInclusive && stamp.Equal(lower))
			if lowerExcluded || (!upper.IsZero() && (stamp.After(upper) || (!upperInclusive && stamp.Equal(upper)))) {
				continue
			}
			filtered = append(filtered, message)
		}
	}
	if values.Get("$orderby") == "lastModifiedDateTime desc" {
		sort.SliceStable(filtered, func(i, j int) bool {
			left, _ := time.Parse(time.RFC3339Nano, filtered[i].LastModifiedDateTime)
			right, _ := time.Parse(time.RFC3339Nano, filtered[j].LastModifiedDateTime)
			return left.After(right)
		})
	}
	pageNumber := 0
	if rawCursor := values.Get("$skiptoken"); rawCursor != "" {
		const prefix = "listener-page-"
		if !strings.HasPrefix(rawCursor, prefix) {
			return nil, "", fmt.Errorf("stateful fake Graph received unknown cursor %q", rawCursor)
		}
		parsed, err := strconv.Atoi(strings.TrimPrefix(rawCursor, prefix))
		if err != nil || parsed < 0 {
			return nil, "", fmt.Errorf("stateful fake Graph received invalid cursor %q", rawCursor)
		}
		pageNumber = parsed
	}
	start := pageNumber * pageSize
	if start >= len(filtered) {
		return []ChatMessage{}, "", nil
	}
	end := start + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	nextLink := ""
	if end < len(filtered) {
		next := url.Values{}
		if top := values.Get("$top"); top != "" {
			next.Set("$top", top)
		}
		if filter := values.Get("$filter"); filter != "" {
			next.Set("$filter", filter)
		}
		if order := values.Get("$orderby"); order != "" {
			next.Set("$orderby", order)
		}
		next.Set("$skiptoken", fmt.Sprintf("listener-page-%d", pageNumber+1))
		nextLink = "/chats/" + url.PathEscape(filtered[0].ChatID) + "/messages?" + next.Encode()
	}
	return filtered[start:end], nextLink, nil
}

func TestListenerRecoveryGraphPageHonorsInclusiveTimeBounds(t *testing.T) {
	stamp := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	messages := []ChatMessage{
		bridgePollMessage("equal-a", stamp.Format(time.RFC3339Nano), "equal a"),
		bridgePollMessage("equal-b", stamp.Format(time.RFC3339Nano), "equal b"),
		bridgePollMessage("newer", stamp.Add(time.Second).Format(time.RFC3339Nano), "newer"),
	}
	values := url.Values{}
	values.Set("$top", "10")
	values.Set("$orderby", "lastModifiedDateTime desc")
	values.Set("$filter", "lastModifiedDateTime ge "+stamp.Format(time.RFC3339Nano))
	page, next, err := listenerRecoveryGraphPage(values, messages, 10)
	if err != nil {
		t.Fatalf("inclusive fake Graph page: %v", err)
	}
	if next != "" || len(page) != len(messages) {
		t.Fatalf("inclusive fake Graph page = %v next=%q, want all %d records", page, next, len(messages))
	}
	values.Set("$filter", "lastModifiedDateTime gt "+stamp.Format(time.RFC3339Nano))
	page, next, err = listenerRecoveryGraphPage(values, messages, 10)
	if err != nil {
		t.Fatalf("strict fake Graph page: %v", err)
	}
	if next != "" || len(page) != 1 || page[0].ID != "newer" {
		t.Fatalf("strict fake Graph page = %v next=%q, want newer only", page, next)
	}
}

type listenerRecoveryExecutor struct {
	mu       sync.Mutex
	calls    []string
	called   chan string
	result   ExecutionResult
	callOnce sync.Once
}

type listenerRecoveryPerPromptExecutor struct {
	mu     sync.Mutex
	calls  []string
	called chan string
	once   sync.Once
}

func (e *listenerRecoveryPerPromptExecutor) Run(_ context.Context, _ *Session, prompt string) (ExecutionResult, error) {
	e.mu.Lock()
	e.calls = append(e.calls, prompt)
	marker := "LISTENER_RECOVERY_MALFORMED_POLL_HEALTHY_FINAL"
	if strings.Contains(prompt, "MALFORMED_POLL_PROMPT") {
		marker = "LISTENER_RECOVERY_MALFORMED_POLL_FINAL"
	}
	e.mu.Unlock()
	e.once.Do(func() { close(e.called) })
	return ExecutionResult{
		Text:          marker,
		CodexThreadID: "thread-" + marker,
		CodexTurnID:   "turn-" + marker,
	}, nil
}

func (e *listenerRecoveryPerPromptExecutor) callsSnapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...)
}

type listenerRecoveryRunner struct {
	mu         sync.Mutex
	calls      []string
	called     chan string
	sawHandler bool
	callOnce   sync.Once
	threadID   string
	turnID     string
	final      string
}

func (r *listenerRecoveryRunner) StartThread(ctx context.Context, input codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	return r.run(ctx, input, "")
}

func (r *listenerRecoveryRunner) ResumeThread(ctx context.Context, threadID string, input codexrunner.TurnInput) (codexrunner.TurnResult, error) {
	return r.run(ctx, input, threadID)
}

func (r *listenerRecoveryRunner) StartTurn(ctx context.Context, input codexrunner.StartTurnInput) (codexrunner.TurnResult, error) {
	return r.run(ctx, input.TurnInput, input.ThreadID)
}

func (r *listenerRecoveryRunner) run(ctx context.Context, input codexrunner.TurnInput, existingThreadID string) (codexrunner.TurnResult, error) {
	if err := ctx.Err(); err != nil {
		return codexrunner.TurnResult{}, err
	}
	r.mu.Lock()
	r.calls = append(r.calls, input.Prompt)
	if input.EventHandler != nil {
		r.sawHandler = true
	}
	threadID := r.threadID
	turnID := r.turnID
	final := r.final
	handler := input.EventHandler
	r.mu.Unlock()
	if threadID == "" {
		threadID = existingThreadID
	}
	if threadID == "" {
		threadID = "listener-runner-thread"
	}
	if turnID == "" {
		turnID = "listener-runner-turn"
	}
	if final == "" {
		final = "LISTENER_RECOVERY_CONFIGURED_RUNNER_FINAL"
	}
	r.callOnce.Do(func() { close(r.called) })
	if handler != nil {
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, ThreadID: threadID, TurnID: turnID, Phase: "commentary", Text: "LISTENER_RECOVERY_CONFIGURED_RUNNER_PROGRESS"})
	}
	return codexrunner.TurnResult{
		ThreadID:                  threadID,
		TurnID:                    turnID,
		Status:                    codexrunner.TurnStatusCompleted,
		FinalAgentMessage:         final,
		FinalAgentMessageComplete: true,
	}, nil
}

func (r *listenerRecoveryRunner) InterruptTurn(context.Context, codexrunner.TurnRef) error {
	return nil
}

func (r *listenerRecoveryRunner) ReadThread(context.Context, string) (codexrunner.Thread, error) {
	return codexrunner.Thread{}, nil
}

func (r *listenerRecoveryRunner) ListThreads(context.Context, codexrunner.ListThreadsOptions) ([]codexrunner.Thread, error) {
	return nil, nil
}

func (r *listenerRecoveryRunner) callsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *listenerRecoveryRunner) sawStreamingHandler() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sawHandler
}

type listenerRecoveryBlockingExecutor struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type listenerRecoveryCooperativeCancellationExecutor struct {
	started chan struct{}
	once    sync.Once
}

// listenerRecoveryTaskPromptRaceExecutor is a deliberately small Codex
// runner stand-in.  It writes the same source ordering as the real app-server
// transcript: task_started is durable before the user prompt and final are
// appended.  The listener must observe the intermediate snapshot without
// turning a normal Teams turn into a permanent history quarantine.
type listenerRecoveryTaskPromptRaceExecutor struct {
	path    string
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

// The full internal/teams package deliberately exercises many heavyweight
// history, Graph, and persistence fixtures before these vertical tests run.
// Keep a generous deterministic watchdog for listener admission/progress so a
// busy CI runner reports a real liveness failure instead of a scheduling flake.
// The outer `go test -timeout`/manifest budget remains the hard process-level
// watchdog for a genuinely wedged listener.
const listenerRecoveryProgressTimeout = 10 * time.Second

func (e *listenerRecoveryBlockingExecutor) Run(_ context.Context, _ *Session, _ string) (ExecutionResult, error) {
	e.once.Do(func() { close(e.started) })
	<-e.release
	return ExecutionResult{}, errors.New("listener recovery test executor failed after shutdown")
}

func (e *listenerRecoveryCooperativeCancellationExecutor) Run(ctx context.Context, _ *Session, _ string) (ExecutionResult, error) {
	e.once.Do(func() { close(e.started) })
	<-ctx.Done()
	return ExecutionResult{}, ctx.Err()
}

func (e *listenerRecoveryTaskPromptRaceExecutor) Run(ctx context.Context, session *Session, _ string) (ExecutionResult, error) {
	return e.RunWithEventHandler(ctx, session, "", nil)
}

// RunWithEventHandler deliberately follows the real streaming executor
// contract.  The task_started record is durable before the external prompt,
// while a commentary event is emitted through the live event handler and the
// final event is emitted only after the prompt/final records are appended.
// This keeps the transcript-race fixture on the production CLI-streaming
// branch instead of silently falling back to Executor.Run in vertical tests.
func (e *listenerRecoveryTaskPromptRaceExecutor) RunWithEventHandler(ctx context.Context, session *Session, prompt string, handler codexrunner.EventHandler) (ExecutionResult, error) {
	if session == nil {
		return ExecutionResult{}, errors.New("task/prompt race executor received nil session")
	}
	if err := appendListenerRecoveryTranscript(e.path, listenerRecoveryTranscriptTaskStartedLine("listener-race-codex-turn")); err != nil {
		return ExecutionResult{}, err
	}
	e.once.Do(func() { close(e.started) })
	select {
	case <-e.release:
	case <-ctx.Done():
		return ExecutionResult{}, ctx.Err()
	}
	if err := appendListenerRecoveryTranscript(e.path,
		listenerRecoveryTranscriptPromptWithTurnLine("listener-race-prompt", "listener-race-codex-turn", prompt),
		listenerRecoveryTranscriptFinalWithTurnLine("listener-race-final", "listener-race-codex-turn", "LISTENER_RECOVERY_VERTICAL_TASK_PROMPT_RACE_FINAL"),
	); err != nil {
		return ExecutionResult{}, err
	}
	if handler != nil {
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Phase: "commentary", Text: "LISTENER_RECOVERY_STREAMING_PROGRESS"})
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventAgentMessage, Phase: "final_answer", Text: "LISTENER_RECOVERY_VERTICAL_TASK_PROMPT_RACE_FINAL"})
		handler(codexrunner.StreamEvent{Kind: codexrunner.StreamEventTurnCompleted})
	}
	return ExecutionResult{
		Text:          "LISTENER_RECOVERY_VERTICAL_TASK_PROMPT_RACE_FINAL",
		CodexThreadID: session.CodexThreadID,
		CodexTurnID:   "listener-race-codex-turn",
	}, nil
}

func (e *listenerRecoveryExecutor) Run(_ context.Context, _ *Session, prompt string) (ExecutionResult, error) {
	e.mu.Lock()
	e.calls = append(e.calls, prompt)
	result := e.result
	e.mu.Unlock()
	e.callOnce.Do(func() { close(e.called) })
	return result, nil
}

func (e *listenerRecoveryExecutor) callsSnapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...)
}

func appendListenerRecoveryTranscript(path string, records ...string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, record := range records {
		if _, err := f.WriteString(record); err != nil {
			return err
		}
	}
	return f.Sync()
}

func listenerRecoverySeedDuePoll(t *testing.T, store *teamstore.Store, chatID string, now time.Time) {
	t.Helper()
	if _, err := store.RecordChatPollSuccess(context.Background(), chatID, now, true, false, 0); err != nil {
		t.Fatalf("seed %s poll cursor: %v", chatID, err)
	}
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         chatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     now.Add(-time.Second),
		LastActivityAt: now,
	}); err != nil {
		t.Fatalf("seed %s poll schedule: %v", chatID, err)
	}
}

func waitListenerRecovery(t *testing.T, waitFor func() bool, timeout time.Duration, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if waitFor() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

type listenerRecoveryHandle struct {
	cancel context.CancelFunc
	done   <-chan error
	once   sync.Once
	err    error
}

func startListenerRecovery(t *testing.T, bridge *Bridge, options BridgeOptions) *listenerRecoveryHandle {
	t.Helper()
	if bridge == nil {
		t.Fatal("listener recovery harness requires a Bridge")
	}
	if options.Once {
		t.Fatal("listener recovery harness must exercise Listen with Once=false")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- bridge.Listen(ctx, options) }()
	handle := &listenerRecoveryHandle{cancel: cancel, done: done}
	t.Cleanup(func() { handle.stop(t) })
	return handle
}

func (h *listenerRecoveryHandle) stop(t *testing.T) {
	if h == nil {
		return
	}
	h.once.Do(func() {
		// Once=false listeners are expected to remain alive until the test (or
		// its context) explicitly stops them.  A completed nil result is not a
		// successful cleanup: it would make a false-positive test look healthy
		// while all later cycles had already been lost.
		select {
		case err := <-h.done:
			h.err = err
			if err == nil {
				t.Errorf("listener recovery test listener exited before explicit cancellation")
			}
			return
		default:
		}
		h.cancel()
		select {
		case h.err = <-h.done:
		case <-time.After(5 * time.Second):
			t.Errorf("listener recovery test listener did not stop within 5s")
		}
	})
	if h.err != nil && !errors.Is(h.err, context.Canceled) {
		t.Errorf("Listen returned unexpected error: %v", h.err)
	}
}

func listenerRecoveryBaseOptions(store *teamstore.Store, registryPath string, executor Executor) BridgeOptions {
	return BridgeOptions{
		Store:        store,
		RegistryPath: registryPath,
		Interval:     time.Millisecond,
		// The listener's production worker budget is a fraction of its phase
		// budget.  Keep this test budget bounded but large enough for SQLite
		// transactions under -race; a 100ms phase would leave only 50ms for the
		// linked scan and turn a healthy persistence path into a deterministic
		// timeout rather than exercising recovery.
		PhaseBudget:              500 * time.Millisecond,
		PollWorkerBudget:         100 * time.Millisecond,
		TranscriptSyncInterval:   time.Millisecond,
		OwnerStaleAfter:          time.Second,
		Executor:                 executor,
		HelperVersion:            "listener-recovery-test",
		MaxWorkChatPollsPerCycle: 8,
		Once:                     false,
	}
}

// TestTeamsListenFalseUntrustedSQLiteLeaseHoldsAndRecovers exercises the
// startup failure that previously made a managed child exit whenever the
// persisted control-lease row was malformed or belonged to a newer helper.
// The listener must remain alive without claiming over that row, then become
// active after an explicit, safe repair.  This is intentionally a vertical
// Listen(Once=false) test: a classifier-only test cannot catch a supervisor
// restart loop or prove that the same process can recover after the repair.
func TestTeamsListenFalseUntrustedSQLiteLeaseHoldsAndRecovers(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	if err := store.Update(ctx, func(*teamstore.State) error { return nil }); err != nil {
		t.Fatalf("materialize lease-hold legacy state: %v", err)
	}
	migration, err := store.MigrateLargeStateToSQLite(ctx, 0)
	if err != nil {
		t.Fatalf("migrate lease-hold fixture to SQLite: %v", err)
	}
	path := store.Path()
	sqlitePath := migration.Path
	if strings.TrimSpace(sqlitePath) == "" {
		sqlitePath = filepath.Join(filepath.Dir(path), teamstore.SQLiteFileName)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close lease-hold fixture before corruption: %v", err)
	}
	db, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("open lease-hold SQLite fixture: %v", err)
	}
	unknownLease := []byte(`{"status":"future-active","holder_machine_id":"machine-from-newer-helper","generation":19}`)
	if _, err := db.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, unknownLease, "control_lease"); err != nil {
		_ = db.Close()
		t.Fatalf("seed untrusted control lease: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close corrupted lease-hold fixture: %v", err)
	}
	reopened, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("reopen lease-hold fixture: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
	bridge := newBridgeTestBridge(graph, reopened, &recordingExecutor{})
	listener := startListenerRecovery(t, bridge, listenerRecoveryBaseOptions(reopened, filepath.Join(t.TempDir(), "registry.json"), bridge.executor))
	// A broken lease must not make Listen return to the service supervisor.  A
	// short observation is enough to distinguish the old immediate-exit path;
	// the production hold timer deliberately has a one-second floor.
	select {
	case err := <-listener.done:
		listener.err = err
		t.Fatalf("listener exited while control lease was untrusted: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if got := graphState.requestsSnapshot(); len(got) != 0 {
		t.Fatalf("listener contacted Graph before lease repair: %#v", got)
	}

	validLease, err := json.Marshal(teamstore.ControlLease{ScopeID: bridge.scope.ID})
	if err != nil {
		t.Fatalf("marshal repaired control lease: %v", err)
	}
	db, err = sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("reopen lease-hold SQLite fixture for repair: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE runtime_state SET json = ? WHERE key = ?`, validLease, "control_lease"); err != nil {
		_ = db.Close()
		t.Fatalf("repair control lease: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close repaired lease-hold fixture: %v", err)
	}

	if !waitListenerRecoveryResult(func() bool {
		owner, ok, readErr := reopened.ReadOwner(ctx)
		return readErr == nil && ok && owner.MachineID == bridge.machine.ID && bridge.currentLease().Generation > 0
	}, 4*time.Second) {
		state, loadErr := reopened.Load(ctx)
		select {
		case err := <-listener.done:
			listener.err = err
			t.Fatalf("listener exited before repaired lease recovery: %v; load=%v state=%#v lease=%#v requests=%#v", err, loadErr, state, bridge.currentLease(), graphState.requestsSnapshot())
		default:
			t.Fatalf("listener did not resume after explicit control-lease repair; load=%v state=%#v lease=%#v requests=%#v", loadErr, state, bridge.currentLease(), graphState.requestsSnapshot())
		}
	}
	if !waitListenerRecoveryResult(func() bool {
		return len(graphState.requestsSnapshot()) > 0
	}, 2*time.Second) {
		state, loadErr := reopened.Load(ctx)
		select {
		case err := <-listener.done:
			listener.err = err
			t.Fatalf("listener exited after repaired lease claim: %v; load=%v state=%#v lease=%#v", err, loadErr, state, bridge.currentLease())
		default:
			t.Fatalf("listener claimed repaired lease but did not resume Graph loop; load=%v state=%#v lease=%#v", loadErr, state, bridge.currentLease())
		}
	}
	listener.stop(t)
}

// TestTeamsListenFalseGraphWorkerSaturationPreservesHealthyPoll covers the
// production shape where four Graph requests are slow and a fifth chat is
// healthy.  A slow first worker wave must not consume the entire poll phase;
// otherwise the healthy chat never reaches Graph and its user message cannot
// enter the normal Codex path.
func TestTeamsListenFalseGraphWorkerSaturationPreservesHealthyPoll(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	healthy := bridgeTestMessageWithText("healthy-message", "LISTENER_RECOVERY_HEALTHY_PROMPT")
	healthy.ChatID = "chat-5"
	healthy.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	healthy.LastModifiedDateTime = healthy.CreatedDateTime
	delays := map[string]time.Duration{
		"chat-1": 250 * time.Millisecond,
		"chat-2": 250 * time.Millisecond,
		"chat-3": 250 * time.Millisecond,
		"chat-4": 250 * time.Millisecond,
	}
	graph, graphState := newListenerRecoveryGraph(t, delays, map[string][]ChatMessage{
		"chat-5": {healthy},
	}, 0)
	store := newBridgeTestStore(t)
	executor := &listenerRecoveryExecutor{
		called: make(chan string),
		result: ExecutionResult{
			Text:          "LISTENER_RECOVERY_HEALTHY_FINAL",
			CodexThreadID: "thread-listener-recovery",
			CodexTurnID:   "turn-listener-recovery",
		},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	for i := 2; i <= 5; i++ {
		appendBridgeTestSession(t, bridge, store, fmt.Sprintf("s00%d", i), fmt.Sprintf("chat-%d", i))
	}
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	for i := 1; i <= 5; i++ {
		listenerRecoverySeedDuePoll(t, store, fmt.Sprintf("chat-%d", i), now)
	}

	registryPath := filepath.Join(t.TempDir(), "registry.json")
	options := listenerRecoveryBaseOptions(store, registryPath, executor)
	// Race instrumentation makes the initial JSON/store admission materially
	// slower.  Keep the worker wave bounded, but do not turn this liveness test
	// into a phase-startup timing test.
	options.PhaseBudget = 5 * time.Second
	options.PollWorkerBudget = time.Second
	listener := startListenerRecovery(t, bridge, options)
	select {
	case <-executor.called:
	case <-time.After(listenerRecoveryProgressTimeout):
		listener.stop(t)
		state, _ := store.Load(context.Background())
		t.Fatalf("healthy chat never dispatched to executor; Graph reads: chat-1=%d chat-2=%d chat-3=%d chat-4=%d chat-5=%d; state=%#v", graphState.getCount("chat-1"), graphState.getCount("chat-2"), graphState.getCount("chat-3"), graphState.getCount("chat-4"), graphState.getCount("chat-5"), state)
	}
	if got := executor.callsSnapshot(); len(got) != 1 || !strings.Contains(got[0], "LISTENER_RECOVERY_HEALTHY_PROMPT") {
		t.Fatalf("executor calls = %#v, want one healthy prompt", got)
	}
	deadline := time.Now().Add(time.Second)
	healthyFinalSent := false
	for time.Now().Before(deadline) {
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_HEALTHY_FINAL") {
				healthyFinalSent = true
				break
			}
		}
		if healthyFinalSent {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !healthyFinalSent {
		state, _ := store.Load(context.Background())
		t.Fatalf("healthy final outbox was not sent; sent=%#v state=%#v lease=%#v", graphState.sentSnapshot(), state.OutboxMessages, bridge.currentLease())
	}
	listener.stop(t)
	if errs := graphState.errorsSnapshot(); len(errs) > 0 {
		t.Fatalf("fake Graph decode errors: %v", errs)
	}
	for i := 1; i <= 5; i++ {
		if got := graphState.getCount(fmt.Sprintf("chat-%d", i)); got == 0 {
			t.Fatalf("healthy/slow chat-%d received no Graph poll; worker saturation test did not exercise the intended request", i)
		}
	}
	if got := graphState.maxGetConcurrency(); got < 4 {
		t.Fatalf("maximum concurrent Graph GETs = %d, want at least four slow workers active", got)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state after listener: %v", err)
	}
	if got := countListenerRecoveryInbound(state, "s005", healthy.ID); got != 1 {
		t.Fatalf("healthy inbound count = %d, want one; state=%#v", got, state.InboundEvents)
	}
}

// TestTeamsListenFalseGraphHeadFailureDoesNotStarveHealthyTail proves the
// simpler failure shape behind the saturation test: one chat's Graph head can
// return repeated retryable errors while a later chat has a real user message.
// The bad chat must receive only a local backoff; it must not abort the whole
// poll phase or prevent the healthy chat from entering Codex and delivering an
// answer.
func TestTeamsListenFalseGraphHeadFailureDoesNotStarveHealthyTail(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	healthy := bridgeTestMessageWithText("head-failure-healthy-message", "LISTENER_RECOVERY_HEAD_FAILURE_HEALTHY_PROMPT")
	healthy.ChatID = "chat-2"
	healthy.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	healthy.LastModifiedDateTime = healthy.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1": nil,
		"chat-2": {healthy},
	}, 0)
	// Keep the bad chat in its local backoff lane long enough to inspect its
	// durable error state after the healthy tail has completed.  The failure
	// count is intentionally larger than the number of requests a phase can
	// issue; this test must not accidentally pass after the bad chat recovers
	// and clears the evidence we want to assert.
	graphState.setGetFailures("chat-1", 100)
	store := newBridgeTestStore(t)
	executor := &listenerRecoveryExecutor{
		called: make(chan string),
		result: ExecutionResult{
			Text:          "LISTENER_RECOVERY_HEAD_FAILURE_HEALTHY_FINAL",
			CodexThreadID: "thread-head-failure-healthy",
			CodexTurnID:   "turn-head-failure-healthy",
		},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	appendBridgeTestSession(t, bridge, store, "s001", "chat-1")
	appendBridgeTestSession(t, bridge, store, "s002", "chat-2")
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)
	listenerRecoverySeedDuePoll(t, store, "chat-2", now)
	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	options.PhaseBudget = 5 * time.Second
	options.PollWorkerBudget = time.Second
	listener := startListenerRecovery(t, bridge, options)
	select {
	case <-executor.called:
	case <-time.After(listenerRecoveryProgressTimeout):
		listener.stop(t)
		t.Fatalf("healthy tail did not reach Codex while head failed: calls=%#v gets=(%d,%d) phase=%#v", executor.callsSnapshot(), graphState.getCount("chat-1"), graphState.getCount("chat-2"), bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	waitListenerRecovery(t, func() bool {
		return countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_HEAD_FAILURE_HEALTHY_FINAL") == 1
	}, listenerRecoveryProgressTimeout, "healthy tail final after Graph head failures")
	badDeadline := time.Now().Add(listenerRecoveryProgressTimeout)
	var badPoll teamstore.ChatPollState
	for time.Now().Before(badDeadline) {
		state, err := store.Load(context.Background())
		if err == nil {
			badPoll = state.ChatPolls["chat-1"]
			if badPoll.LastError != "" && badPoll.FailureCount > 0 && badPoll.NextPollAt.After(time.Now()) {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if badPoll.LastError == "" || badPoll.FailureCount == 0 || !badPoll.NextPollAt.After(time.Now()) {
		listener.stop(t)
		t.Fatalf("bad chat durable error was not recorded: poll=%#v gets=(%d,%d) requests=%v phase=%#v", badPoll, graphState.getCount("chat-1"), graphState.getCount("chat-2"), graphState.requestsSnapshot(), bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	if graphState.getCount("chat-1") == 0 || graphState.getCount("chat-2") == 0 {
		t.Fatalf("Graph head failure test did not exercise both chats: bad=%d healthy=%d", graphState.getCount("chat-1"), graphState.getCount("chat-2"))
	}
	if got := countListenerRecoveryInbound(mustListenerRecoveryState(t, store), "s002", healthy.ID); got != 1 {
		t.Fatalf("healthy inbound count = %d, want one", got)
	}
	if got := countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_HEAD_FAILURE_HEALTHY_FINAL"); got != 1 {
		listener.stop(t)
		t.Fatalf("healthy final POST count = %d, want one", got)
	}
	badState := mustListenerRecoveryState(t, store).ChatPolls["chat-1"]
	if badState.LastError == "" || badState.FailureCount == 0 {
		listener.stop(t)
		t.Fatalf("bad chat error was not isolated durably: %#v", badState)
	}
	// Once the provider recovers, the same chat must be able to clear only its
	// local gate.  Make it due explicitly so this assertion does not wait for a
	// production backoff interval.
	graphState.setGetFailures("chat-1", 0)
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID: "chat-1", NextPollAt: time.Now().UTC().Add(-time.Second),
		ClearBlockedUntil: true, ResetFailures: true,
	}); err != nil {
		listener.stop(t)
		t.Fatalf("make isolated bad chat due for recovery: %v", err)
	}
	waitListenerRecovery(t, func() bool {
		state, err := store.Load(context.Background())
		if err != nil {
			return false
		}
		poll := state.ChatPolls["chat-1"]
		return poll.LastError == "" && poll.FailureCount == 0 && !poll.LastSuccessfulPollAt.IsZero()
	}, listenerRecoveryProgressTimeout, "isolated bad chat recovery")
	if got := countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_HEAD_FAILURE_HEALTHY_FINAL"); got != 1 {
		listener.stop(t)
		t.Fatalf("healthy final changed while bad chat recovered: %d", got)
	}
	listener.stop(t)
}

// TestTeamsListenFalseGraphContinuationRecoversAfterTransientOutage exercises
// the durable inbound frontier through the real listener.  The chat starts
// with an opaque continuation, the first dereference gets a retryable 429,
// and the next listener cycle drains the old page before ordinary head polling
// resumes.  The final assertion is deliberately user-visible: the recovered
// prompt reaches the executor and its answer reaches the isolated fake Graph.
func TestTeamsListenFalseGraphContinuationRecoversAfterTransientOutage(t *testing.T) {
	chatID := "chat-1"
	oldPath := "/chats/" + chatID + "/messages?$skiptoken=old"
	secondPath := "/chats/" + chatID + "/messages?$skiptoken=old-2"
	message := bridgeTestMessageWithText("continuation-prompt", "LISTENER_RECOVERY_CONTINUATION_PROMPT")
	message.ChatID = chatID
	message.CreatedDateTime = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime

	var mu sync.Mutex
	continuationRequests := 0
	var requests []string
	var posts []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		mu.Unlock()
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages") {
			cursor := r.URL.Query().Get("$skiptoken")
			if cursor == "old" {
				mu.Lock()
				continuationRequests++
				attempt := continuationRequests
				mu.Unlock()
				if attempt == 1 {
					w.Header().Set("Retry-After", "1")
					http.Error(w, `{"error":{"code":"TooManyRequests","message":"temporary continuation outage"}}`, http.StatusTooManyRequests)
					return
				}
				// The first successful continuation page deliberately has no
				// visible message but advertises another page.  This makes the
				// vertical listener test prove that a recovered opaque frontier is
				// followed to its terminal page rather than merely retrying the
				// first request once.
				_ = json.NewEncoder(w).Encode(map[string]any{
					"value":           []ChatMessage{},
					"@odata.nextLink": secondPath,
				})
				return
			}
			if cursor == "old-2" {
				mu.Lock()
				continuationRequests++
				mu.Unlock()
				_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{message}})
				return
			}
			// Once the old frontier has drained, ordinary head polls are allowed
			// to return empty.  The test must not accidentally pass by replaying
			// the continuation forever.
			_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{}})
			return
		}
		if r.Method == http.MethodPost && r.URL.Path == "/chats/"+chatID+"/messages" {
			var payload struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			posts = append(posts, payload.Body.Content)
			postID := fmt.Sprintf("continuation-final-%d", len(posts))
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"id": postID, "messageType": "message"})
			return
		}
		http.Error(w, "unexpected fake Graph request", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	executor := &listenerRecoveryExecutor{
		called: make(chan string),
		result: ExecutionResult{
			Text:          "LISTENER_RECOVERY_CONTINUATION_FINAL",
			CodexThreadID: "thread-listener-continuation",
			CodexTurnID:   "turn-listener-continuation",
		},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	now := time.Now().UTC().Add(-time.Minute)
	if _, err := store.RecordChatPollSuccess(context.Background(), bridge.reg.ControlChatID, now, true, false, 0); err != nil {
		t.Fatalf("seed control poll: %v", err)
	}
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:         bridge.reg.ControlChatID,
		PollState:      inboundPollStateWarm,
		NextPollAt:     now,
		LastActivityAt: now,
	}); err != nil {
		t.Fatalf("seed control poll schedule: %v", err)
	}
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		if state.ChatPolls == nil {
			state.ChatPolls = make(map[string]teamstore.ChatPollState)
		}
		state.ChatPolls[chatID] = teamstore.ChatPollState{
			ChatID:               chatID,
			Seeded:               true,
			PollState:            inboundPollStateWarm,
			NextPollAt:           now,
			LastActivityAt:       now,
			LastModifiedCursor:   now.Add(-time.Minute),
			ContinuationPath:     oldPath,
			FrontierEpoch:        1,
			LastSuccessfulPollAt: now,
			UpdatedAt:            now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed continuation frontier: %v", err)
	}

	registryPath := filepath.Join(t.TempDir(), "registry.json")
	options := listenerRecoveryBaseOptions(store, registryPath, executor)
	options.PhaseBudget = 5 * time.Second
	options.PollWorkerBudget = 3 * time.Second
	listener := startListenerRecovery(t, bridge, options)
	if !waitListenerRecoveryResult(func() bool {
		state, err := store.Load(context.Background())
		if err != nil || countListenerRecoveryInbound(state, "s001", message.ID) != 1 {
			return false
		}
		return len(executor.callsSnapshot()) == 1
	}, 30*time.Second) {
		state, _ := store.Load(context.Background())
		mu.Lock()
		requestSnapshot := append([]string(nil), requests...)
		mu.Unlock()
		poll := state.ChatPolls[chatID]
		pending := "<nil>"
		if poll.PendingPage != nil {
			pending = fmt.Sprintf("%#v", *poll.PendingPage)
		}
		attempt := "<nil>"
		if poll.Attempt != nil {
			attempt = fmt.Sprintf("%#v", *poll.Attempt)
		}
		t.Fatalf("continuation prompt after transient outage: calls=%#v requests=%#v poll=%#v pending=%s attempt=%s phase=%#v", executor.callsSnapshot(), requestSnapshot, poll, pending, attempt, bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	waitListenerRecovery(t, func() bool {
		for _, body := range graphContinuationPosts(&mu, &posts) {
			if strings.Contains(PlainTextFromTeamsHTML(body), "LISTENER_RECOVERY_CONTINUATION_FINAL") {
				return true
			}
		}
		return false
	}, listenerRecoveryProgressTimeout, "continuation final delivery")
	mu.Lock()
	gotRequests := continuationRequests
	gotPosts := append([]string(nil), posts...)
	mu.Unlock()
	if gotRequests < 3 {
		t.Fatalf("continuation requests = %d, want transient failure followed by two-page recovery", gotRequests)
	}
	mu.Lock()
	requestText := strings.Join(append([]string(nil), requests...), "\n")
	mu.Unlock()
	if !strings.Contains(requestText, "skiptoken=old-2") {
		t.Fatalf("continuation recovery never requested the advertised second page; requests=%s", requestText)
	}
	if len(gotPosts) != 2 {
		t.Fatalf("Graph POST count = %d, want one ACK and one recovered final; posts=%#v", len(gotPosts), gotPosts)
	}
	finalPosts := 0
	for _, body := range gotPosts {
		if strings.Contains(PlainTextFromTeamsHTML(body), "LISTENER_RECOVERY_CONTINUATION_FINAL") {
			finalPosts++
		}
	}
	if finalPosts != 1 {
		t.Fatalf("recovered final POST count = %d, want one; posts=%#v", finalPosts, gotPosts)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state after continuation recovery: %v", err)
	}
	poll := state.ChatPolls[chatID]
	if poll.ContinuationPath != "" || poll.Gap != nil || poll.PendingPage != nil {
		t.Fatalf("continuation frontier not drained: %#v", poll)
	}
	listener.stop(t)
}

func graphContinuationPosts(mu *sync.Mutex, posts *[]string) []string {
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), (*posts)...)
}

// TestTeamsListenFalseGraphStatefulHeadContinuationDrainsTerminalPage proves
// the normal head path as well as the persisted nextLink path. The fake applies
// the real filter/order/top query and returns a different page for each opaque
// cursor; a fixture that always returns the same full slice would not catch a
// dropped cursor, an incorrect filter, or a false terminal decision.
func TestTeamsListenFalseGraphStatefulHeadContinuationDrainsTerminalPage(t *testing.T) {
	chatID := "chat-stateful-pages"
	now := time.Now().UTC().Add(-time.Minute)
	newMessage := func(id, text string, stamp time.Time) ChatMessage {
		message := bridgeTestMessageWithText(id, text)
		message.ChatID = chatID
		message.CreatedDateTime = stamp.Format(time.RFC3339Nano)
		message.LastModifiedDateTime = message.CreatedDateTime
		return message
	}
	newest := newMessage("stateful-newest", "LISTENER_RECOVERY_STATEFUL_NEWEST", now.Add(2*time.Second))
	older := newMessage("stateful-older", "LISTENER_RECOVERY_STATEFUL_OLDER", now.Add(time.Second))
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		chatID: {older, newest},
	}, 0)
	graphState.mu.Lock()
	graphState.pageSize = 1
	graphState.mu.Unlock()

	store := newBridgeTestStore(t)
	executor := &listenerRecoveryExecutor{
		called: make(chan string),
		result: ExecutionResult{
			Text:          "LISTENER_RECOVERY_STATEFUL_FINAL",
			CodexThreadID: "thread-listener-stateful",
			CodexTurnID:   "turn-listener-stateful",
		},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	appendBridgeTestSession(t, bridge, store, "s-stateful", chatID)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, chatID, now)
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		poll := state.ChatPolls[chatID]
		poll.LastModifiedCursor = now
		poll.Seeded = true
		state.ChatPolls[chatID] = poll
		return nil
	}); err != nil {
		t.Fatalf("seed stateful modified cursor: %v", err)
	}

	registryPath := filepath.Join(t.TempDir(), "registry.json")
	options := listenerRecoveryBaseOptions(store, registryPath, executor)
	options.PhaseBudget = 5 * time.Second
	options.PollWorkerBudget = time.Second
	listener := startListenerRecovery(t, bridge, options)
	deadline := time.Now().Add(listenerRecoveryProgressTimeout)
	for time.Now().Before(deadline) {
		calls := executor.callsSnapshot()
		if len(calls) == 2 && strings.Contains(strings.Join(calls, "\n"), "LISTENER_RECOVERY_STATEFUL_NEWEST") &&
			strings.Contains(strings.Join(calls, "\n"), "LISTENER_RECOVERY_STATEFUL_OLDER") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if calls := executor.callsSnapshot(); len(calls) != 2 || !strings.Contains(strings.Join(calls, "\n"), "LISTENER_RECOVERY_STATEFUL_NEWEST") || !strings.Contains(strings.Join(calls, "\n"), "LISTENER_RECOVERY_STATEFUL_OLDER") {
		state, _ := store.Load(context.Background())
		t.Fatalf("stateful page prompts = %#v; requests=%v errors=%v polls=%#v", calls, graphState.requestsSnapshot(), graphState.errorsSnapshot(), state.ChatPolls[chatID])
	}
	waitListenerRecovery(t, func() bool {
		finals := 0
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_STATEFUL_FINAL") {
				finals++
			}
		}
		return finals == 2
	}, listenerRecoveryProgressTimeout, "stateful head and continuation final delivery")
	waitListenerRecovery(t, func() bool {
		state, err := store.Load(context.Background())
		if err != nil {
			return false
		}
		poll := state.ChatPolls[chatID]
		return poll.PendingPage == nil && poll.Attempt == nil &&
			strings.TrimSpace(poll.ContinuationPath) == ""
	}, listenerRecoveryProgressTimeout, "stateful head and continuation durable poll completion")

	requests := graphState.requestsSnapshot()
	headQuerySeen := false
	continuationSeen := false
	headFilter := ""
	headTop := ""
	for _, request := range requests {
		if !strings.HasPrefix(request, "GET ") {
			continue
		}
		parsed, parseErr := url.Parse(strings.TrimPrefix(request, "GET "))
		if parseErr != nil || parsed.Path != "/chats/"+chatID+"/messages" {
			continue
		}
		query := parsed.Query()
		switch query.Get("$skiptoken") {
		case "":
			if query.Get("$orderby") == "lastModifiedDateTime desc" && query.Get("$filter") != "" && query.Get("$top") != "" {
				headQuerySeen = true
				headFilter = query.Get("$filter")
				headTop = query.Get("$top")
			}
		case "listener-page-1":
			continuationSeen = query.Get("$filter") == headFilter && query.Get("$top") == headTop && query.Get("$orderby") == "lastModifiedDateTime desc"
		}
	}
	if !headQuerySeen {
		t.Fatalf("stateful head request did not carry production ordering/filter: %v", requests)
	}
	if !continuationSeen {
		t.Fatalf("stateful fake nextLink was not followed: %v", requests)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state after stateful page recovery: %v", err)
	}
	poll := state.ChatPolls[chatID]
	if poll.ContinuationPath != "" || poll.PendingPage != nil || poll.Gap != nil {
		t.Fatalf("stateful Graph frontier not terminal after recovery: %#v", poll)
	}
	if errs := graphState.errorsSnapshot(); len(errs) > 0 {
		t.Fatalf("stateful fake Graph errors: %v", errs)
	}
	listener.stop(t)
}

// TestTeamsListenFalseUsesConfiguredRunnerStreaming exercises the production
// BridgeOptions.Runner adapter. A green test that only injects Executor would
// miss failures in RunnerExecutor, event forwarding, or configured app-server
// ownership while the real listener is running.
func TestTeamsListenFalseUsesConfiguredRunnerStreaming(t *testing.T) {
	chatID := "chat-configured-runner"
	now := time.Now().UTC().Add(-time.Minute)
	message := bridgeTestMessageWithText("configured-runner-message", "LISTENER_RECOVERY_CONFIGURED_RUNNER_PROMPT")
	message.ChatID = chatID
	message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{chatID: {message}}, 0)
	store := newBridgeTestStore(t)
	wrongExecutor := &listenerRecoveryExecutor{called: make(chan string), result: ExecutionResult{Text: "WRONG_EXECUTOR"}}
	bridge := newBridgeTestBridge(graph, store, wrongExecutor)
	appendBridgeTestSession(t, bridge, store, "s-configured-runner", chatID)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, chatID, now)
	runner := &listenerRecoveryRunner{
		called:   make(chan string),
		threadID: "configured-runner-thread",
		turnID:   "configured-runner-turn",
		final:    "LISTENER_RECOVERY_CONFIGURED_RUNNER_FINAL",
	}

	registryPath := filepath.Join(t.TempDir(), "registry.json")
	options := listenerRecoveryBaseOptions(store, registryPath, wrongExecutor)
	options.PhaseBudget = 2 * time.Second
	options.Runner = runner
	listener := startListenerRecovery(t, bridge, options)
	select {
	case <-runner.called:
	case <-time.After(listenerRecoveryProgressTimeout):
		listener.stop(t)
		t.Fatalf("configured runner was not dispatched; requests=%v errors=%v", graphState.requestsSnapshot(), graphState.errorsSnapshot())
	}
	waitListenerRecovery(t, func() bool {
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_CONFIGURED_RUNNER_FINAL") {
				return true
			}
		}
		return false
	}, listenerRecoveryProgressTimeout, "configured runner final delivery")
	if got := runner.callsSnapshot(); len(got) != 1 || !strings.Contains(got[0], "LISTENER_RECOVERY_CONFIGURED_RUNNER_PROMPT") {
		t.Fatalf("configured runner calls = %#v, want one prompt", got)
	}
	if !runner.sawStreamingHandler() {
		t.Fatal("configured runner did not receive the production streaming event handler")
	}
	if got := wrongExecutor.callsSnapshot(); len(got) != 0 {
		t.Fatalf("fallback Executor was used despite configured Runner: %#v", got)
	}
	if errs := graphState.errorsSnapshot(); len(errs) > 0 {
		t.Fatalf("configured runner fake Graph errors: %v", errs)
	}
	listener.stop(t)
}

// TestTeamsOutboxUnauthorizedResponseRetainsQueuedWorkForRefreshedAttempt
// covers the authentication boundary that is easy to confuse with a terminal
// Graph rejection.  A POST that receives 401 is not replayed inside the same
// HTTP call, but the durable user work must remain queued after a non-
// interactive token refresh so the next listener pass can deliver it.
func TestTeamsOutboxUnauthorizedResponseRetainsQueuedWorkForRefreshedAttempt(t *testing.T) {
	auth := &fakeGraphAuth{token: "old-access", refreshedToken: "new-access"}
	var mu sync.Mutex
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			http.Error(w, "unexpected fake Graph request", http.StatusNotFound)
			return
		}
		mu.Lock()
		attempts++
		attempt := attempts
		mu.Unlock()
		switch r.Header.Get("Authorization") {
		case "Bearer old-access":
			if attempt != 1 {
				t.Fatalf("old token used after refresh on attempt %d", attempt)
			}
			http.Error(w, `{"error":{"code":"InvalidAuthenticationToken","message":"expired"}}`, http.StatusUnauthorized)
		case "Bearer new-access":
			if attempt != 2 {
				t.Fatalf("refreshed token used on unexpected attempt %d", attempt)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "teams-auth-recovered", "messageType": "message"})
		default:
			t.Fatalf("unexpected authorization header %q", r.Header.Get("Authorization"))
		}
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       auth,
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 1,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	queued, created, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "outbox:unauthorized-retry",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "retain this user-visible message",
	})
	if err != nil || !created {
		t.Fatalf("queue unauthorized outbox: created=%v err=%v", created, err)
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	firstErr := bridge.sendQueuedOutboxWithOptions(context.Background(), queued, outboxSendOptions{})
	if firstErr == nil || !strings.Contains(firstErr.Error(), "401") {
		t.Fatalf("first unauthorized send error = %v, want Graph 401", firstErr)
	}
	mu.Lock()
	gotAttempts := attempts
	mu.Unlock()
	if gotAttempts != 1 {
		t.Fatalf("first unauthorized send attempts = %d, want one POST and no in-call replay", gotAttempts)
	}
	if auth.refreshCalls != 1 {
		t.Fatalf("token refresh calls = %d, want one non-interactive refresh", auth.refreshCalls)
	}
	retained, err := store.OutboxMessageByID(context.Background(), queued.ID)
	if err != nil {
		t.Fatalf("load retained unauthorized outbox: %v", err)
	}
	if retained.Status != teamstore.OutboxStatusQueued || retained.TeamsMessageID != "" {
		t.Fatalf("unauthorized outbox was discarded or assigned an ID: %#v", retained)
	}

	if err := bridge.sendQueuedOutboxWithOptions(context.Background(), retained, outboxSendOptions{}); err != nil {
		t.Fatalf("refreshed unauthorized outbox send: %v", err)
	}
	final, err := store.OutboxMessageByID(context.Background(), queued.ID)
	if err != nil {
		t.Fatalf("load delivered unauthorized outbox: %v", err)
	}
	if final.Status != teamstore.OutboxStatusSent || final.TeamsMessageID != "teams-auth-recovered" {
		t.Fatalf("refreshed unauthorized outbox = %#v, want sent with Graph identity", final)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("total unauthorized recovery POSTs = %d, want one rejected POST and one later refreshed POST", attempts)
	}
}

// TestTeamsListenFalseLinkedTranscriptSlowHeadDoesNotStarveHealthyTail uses
// the real listener and durable linked-transcript phase.  The first session's
// source worker deliberately waits for the second session.  A serial phase
// deadlocks at the head of the registry; the bounded worker pool must still
// let the healthy tail advance its durable checkpoint in this cycle.
func TestTeamsListenFalseLinkedTranscriptSlowHeadDoesNotStarveHealthyTail(t *testing.T) {
	store := newBridgeTestStore(t)
	graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.transcriptSyncWorkerCount = 2

	root := t.TempDir()
	bridge.scope.CodexHome = root
	paths := map[string]string{
		"s001": filepath.Join(root, "s001.jsonl"),
		"s002": filepath.Join(root, "s002.jsonl"),
	}
	for sessionID, path := range paths {
		initial := listenerRecoveryTranscriptLine("initial-"+sessionID, "baseline-"+sessionID)
		if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
			t.Fatalf("write %s transcript: %v", sessionID, err)
		}
		session := bridge.reg.SessionByID(sessionID)
		if session == nil {
			if sessionID == "s002" {
				session = appendBridgeTestSession(t, bridge, store, sessionID, "chat-2")
			} else {
				t.Fatalf("missing registry session %s", sessionID)
			}
		}
		session.CodexThreadID = "thread-" + sessionID
		session.Cwd = root
		if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
			t.Fatalf("persist linked session %s: %v", sessionID, err)
		}
		if _, _, err := store.UpdateSessionContext(context.Background(), sessionID, func(current teamstore.SessionContext, found bool, now time.Time) (teamstore.SessionContext, bool, error) {
			if !found {
				return current, false, fmt.Errorf("linked session %s was not persisted", sessionID)
			}
			current.CodexThreadID = session.CodexThreadID
			current.Cwd = root
			current.UpdatedAt = now
			return current, true, nil
		}); err != nil {
			t.Fatalf("persist linked session context %s: %v", sessionID, err)
		}
		listenerRecoverySeedLinkedCheckpoint(t, store, session, path, true)
		tail := listenerRecoveryTranscriptLine("tail-"+sessionID, "tail-status-"+sessionID)
		if sessionID == "s002" {
			tail += listenerRecoveryTranscriptFinalLine("tail-final-"+sessionID, "LISTENER_RECOVERY_LINKED_TAIL_FINAL")
		}
		if err := appendListenerRecoveryTranscript(path, tail); err != nil {
			t.Fatalf("append %s transcript: %v", sessionID, err)
		}
	}

	headEntered := make(chan struct{})
	var headOnce sync.Once
	tailEntered := make(chan struct{})
	var tailOnce sync.Once
	var hookMu sync.Mutex
	var hookCalls []string
	bridge.linkedTranscriptSessionHook = func(ctx context.Context, session Session) error {
		hookMu.Lock()
		hookCalls = append(hookCalls, session.ID)
		hookMu.Unlock()
		if session.ID == "s001" {
			headOnce.Do(func() { close(headEntered) })
			select {
			case <-tailEntered:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if session.ID == "s002" {
			tailOnce.Do(func() { close(tailEntered) })
		}
		return nil
	}
	bridge.lastTranscriptSync = time.Time{}
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, time.Now().UTC().Add(-time.Minute))
	listenerRecoverySeedDuePoll(t, store, "chat-1", time.Now().UTC().Add(-time.Minute))
	listenerRecoverySeedDuePoll(t, store, "chat-2", time.Now().UTC().Add(-time.Minute))

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor)
	options.PhaseBudget = 2 * time.Second
	listener := startListenerRecovery(t, bridge, options)
	waitListenerRecovery(t, func() bool {
		select {
		case <-headEntered:
			return true
		default:
			return false
		}
	}, listenerRecoveryProgressTimeout, "linked transcript slow head to enter")
	healthyInfo, err := os.Stat(paths["s002"])
	if err != nil {
		listener.stop(t)
		t.Fatalf("stat healthy linked transcript: %v", err)
	}
	linkedProgress := func() bool {
		state, err := store.Load(context.Background())
		if err != nil {
			return false
		}
		return state.ImportCheckpoints[transcriptCheckpointID("s002")].LastOffset == healthyInfo.Size()
	}
	deadline := time.Now().Add(listenerRecoveryProgressTimeout)
	for !linkedProgress() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !linkedProgress() {
		state, _ := store.Load(context.Background())
		hookMu.Lock()
		calls := append([]string(nil), hookCalls...)
		hookMu.Unlock()
		listener.stop(t)
		t.Fatalf("linked transcript healthy tail did not progress; hooks=%v sessions=%#v checkpoints=%#v phase=%#v", calls, bridge.reg.Sessions, state.ImportCheckpoints, bridge.mainLoopPhaseStatsSnapshot("linked-transcript"))
	}
	waitListenerRecovery(t, func() bool {
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_LINKED_TAIL_FINAL") {
				return true
			}
		}
		return false
	}, listenerRecoveryProgressTimeout, "linked transcript healthy tail final delivery")
	listener.stop(t)
}

// TestTeamsListenFalseLinkedTranscriptFullPoolDoesNotStarveHealthyTail covers
// the failure mode where every worker starts a cooperative slow session before
// a healthy session later in the registry.  A phase-wide deadline alone is not
// enough: without a per-job slice, the healthy session only sees an already
// cancelled context and is skipped again on every cycle.
func TestTeamsListenFalseLinkedTranscriptFullPoolDoesNotStarveHealthyTail(t *testing.T) {
	store := newBridgeTestStore(t)
	graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.transcriptSyncWorkerCount = 4

	root := t.TempDir()
	bridge.scope.CodexHome = root
	const sessionCount = 5
	paths := make([]string, 0, sessionCount)
	for index := 1; index <= sessionCount; index++ {
		sessionID := fmt.Sprintf("s%03d", index)
		chatID := fmt.Sprintf("chat-%d", index)
		session := bridge.reg.SessionByID(sessionID)
		if session == nil {
			session = appendBridgeTestSession(t, bridge, store, sessionID, chatID)
		}
		session.CodexThreadID = "thread-" + sessionID
		session.Cwd = root
		if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
			t.Fatalf("persist linked session %s: %v", sessionID, err)
		}
		if _, _, err := store.UpdateSessionContext(context.Background(), sessionID, func(current teamstore.SessionContext, found bool, now time.Time) (teamstore.SessionContext, bool, error) {
			if !found {
				return current, false, fmt.Errorf("linked session %s was not persisted", sessionID)
			}
			current.CodexThreadID = session.CodexThreadID
			current.Cwd = root
			current.UpdatedAt = now
			return current, true, nil
		}); err != nil {
			t.Fatalf("persist linked session context %s: %v", sessionID, err)
		}
		path := filepath.Join(root, sessionID+".jsonl")
		initial := listenerRecoveryTranscriptLine("initial-"+sessionID, "baseline-"+sessionID)
		final := listenerRecoveryTranscriptFinalLine("final-"+sessionID, fmt.Sprintf("LISTENER_RECOVERY_FULL_POOL_FINAL_%s", sessionID))
		if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
			t.Fatalf("write %s transcript: %v", sessionID, err)
		}
		listenerRecoverySeedLinkedCheckpoint(t, store, session, path, true)
		if err := appendListenerRecoveryTranscript(path, final); err != nil {
			t.Fatalf("append %s transcript: %v", sessionID, err)
		}
		paths = append(paths, path)
	}

	var slowCallsMu sync.Mutex
	slowCalls := make(map[string]int)
	healthyEntered := make(chan struct{})
	var healthyOnce sync.Once
	bridge.linkedTranscriptSessionHook = func(ctx context.Context, session Session) error {
		if session.ID == "s005" {
			healthyOnce.Do(func() { close(healthyEntered) })
			return nil
		}
		slowCallsMu.Lock()
		slowCalls[session.ID]++
		slowCallsMu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}
	bridge.lastTranscriptSync = time.Time{}
	now := time.Now().UTC().Add(-time.Minute)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	for index := 1; index <= sessionCount; index++ {
		listenerRecoverySeedDuePoll(t, store, fmt.Sprintf("chat-%d", index), now)
	}

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor)
	options.PhaseBudget = 2 * time.Second
	listener := startListenerRecovery(t, bridge, options)
	waitListenerRecovery(t, func() bool {
		select {
		case <-healthyEntered:
			return true
		default:
			return false
		}
	}, listenerRecoveryProgressTimeout, "healthy tail to enter after full slow worker pool")

	healthyPath := paths[len(paths)-1]
	healthyInfo, err := os.Stat(healthyPath)
	if err != nil {
		listener.stop(t)
		t.Fatalf("stat healthy transcript: %v", err)
	}
	waitListenerRecovery(t, func() bool {
		state, loadErr := store.Load(context.Background())
		if loadErr != nil {
			return false
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID("s005")]
		if checkpoint.LastOffset != healthyInfo.Size() {
			return false
		}
		return countListenerRecoveryTranscriptFinals(graphState.sentSnapshot(), "LISTENER_RECOVERY_FULL_POOL_FINAL_s005") == 1
	}, listenerRecoveryProgressTimeout, "healthy tail after full slow worker pool")
	listener.stop(t)

	slowCallsMu.Lock()
	defer slowCallsMu.Unlock()
	for _, sessionID := range []string{"s001", "s002", "s003", "s004"} {
		if slowCalls[sessionID] == 0 {
			t.Fatalf("slow session %s was never scheduled", sessionID)
		}
	}
}

// TestTeamsListenFalseLinkedTranscriptSessionErrorDoesNotStarveHealthyTail
// covers the error variant of the same isolation boundary.  A source failure
// in one linked transcript is chat/session-local; it must be reported and
// retried without canceling the shared worker context before a healthy later
// session gets a chance to commit its checkpoint and deliver its final.
func TestTeamsListenFalseLinkedTranscriptSessionErrorDoesNotStarveHealthyTail(t *testing.T) {
	store := newBridgeTestStore(t)
	graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	// One worker makes the test prove that the local error does not cancel the
	// queue before the healthy tail is visited; a larger pool could hide that
	// regression through scheduling.
	bridge.transcriptSyncWorkerCount = 1

	root := t.TempDir()
	bridge.scope.CodexHome = root
	paths := map[string]string{
		"s001": filepath.Join(root, "s001.jsonl"),
		"s002": filepath.Join(root, "s002.jsonl"),
	}
	for sessionID, path := range paths {
		initial := listenerRecoveryTranscriptLine("initial-"+sessionID, "baseline-"+sessionID)
		if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
			t.Fatalf("write %s transcript: %v", sessionID, err)
		}
		session := bridge.reg.SessionByID(sessionID)
		if session == nil {
			if sessionID != "s002" {
				t.Fatalf("missing registry session %s", sessionID)
			}
			session = appendBridgeTestSession(t, bridge, store, sessionID, "chat-2")
		}
		session.CodexThreadID = "thread-" + sessionID
		session.Cwd = root
		if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
			t.Fatalf("persist linked session %s: %v", sessionID, err)
		}
		if _, _, err := store.UpdateSessionContext(context.Background(), sessionID, func(current teamstore.SessionContext, found bool, now time.Time) (teamstore.SessionContext, bool, error) {
			if !found {
				return current, false, fmt.Errorf("linked session %s was not persisted", sessionID)
			}
			current.CodexThreadID = session.CodexThreadID
			current.Cwd = root
			current.UpdatedAt = now
			return current, true, nil
		}); err != nil {
			t.Fatalf("persist linked session context %s: %v", sessionID, err)
		}
		listenerRecoverySeedLinkedCheckpoint(t, store, session, path, true)
		tail := listenerRecoveryTranscriptLine("tail-"+sessionID, "tail-status-"+sessionID)
		if sessionID == "s002" {
			tail += listenerRecoveryTranscriptFinalLine("tail-final-"+sessionID, "LISTENER_RECOVERY_LINKED_ERROR_TAIL_FINAL")
		}
		if err := appendListenerRecoveryTranscript(path, tail); err != nil {
			t.Fatalf("append %s transcript: %v", sessionID, err)
		}
	}

	var headCalls, tailCalls int
	var callsMu sync.Mutex
	bridge.linkedTranscriptSessionHook = func(_ context.Context, session Session) error {
		callsMu.Lock()
		switch session.ID {
		case "s001":
			headCalls++
		case "s002":
			tailCalls++
		}
		callsMu.Unlock()
		if session.ID == "s001" {
			return errors.New("synthetic session-local transcript read failure")
		}
		return nil
	}
	bridge.lastTranscriptSync = time.Time{}
	now := time.Now().UTC().Add(-time.Minute)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)
	listenerRecoverySeedDuePoll(t, store, "chat-2", now)

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor)
	options.PhaseBudget = 2 * time.Second
	listener := startListenerRecovery(t, bridge, options)
	waitListenerRecovery(t, func() bool {
		state, err := store.Load(context.Background())
		if err != nil {
			return false
		}
		info, err := os.Stat(paths["s002"])
		if err != nil {
			return false
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID("s002")]
		if checkpoint.LastOffset != info.Size() {
			return false
		}
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_LINKED_ERROR_TAIL_FINAL") {
				return true
			}
		}
		return false
	}, listenerRecoveryProgressTimeout, "healthy tail after session-local transcript error")
	callsMu.Lock()
	gotHeadCalls, gotTailCalls := headCalls, tailCalls
	callsMu.Unlock()
	if gotHeadCalls == 0 || gotTailCalls == 0 {
		t.Fatalf("linked transcript hooks = head:%d tail:%d, want both sessions exercised", gotHeadCalls, gotTailCalls)
	}
	if got := countListenerRecoveryTranscriptFinals(graphState.sentSnapshot(), "LISTENER_RECOVERY_LINKED_ERROR_TAIL_FINAL"); got != 1 {
		t.Fatalf("healthy tail final POST count = %d, want one; sent=%#v", got, graphState.sentSnapshot())
	}
	listener.stop(t)
}

// TestTeamsListenFalseHistoryWatchSlowHeadDoesNotStarveHealthyTail is the
// equivalent production-listener regression for the cold HistoryWatch path.
// A slow first path must not prevent a later path from committing its own
// checkpoint, even though both paths share one history-watch phase.
func TestTeamsListenFalseHistoryWatchSlowHeadDoesNotStarveHealthyTail(t *testing.T) {
	store := newBridgeTestStore(t)
	graph, _ := newListenerRecoveryGraph(t, nil, nil, 0)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.transcriptSyncWorkerCount = 2
	root := filepath.Join(t.TempDir(), "codex")
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0o700); err != nil {
		t.Fatalf("mkdir history root: %v", err)
	}
	bridge.scope.CodexHome = root
	paths := []string{
		filepath.Join(root, "sessions", "a-history.jsonl"),
		filepath.Join(root, "sessions", "b-history.jsonl"),
	}
	initialOffsets := make(map[string]int64, len(paths))
	for index, path := range paths {
		initial := listenerRecoveryTranscriptLine(fmt.Sprintf("history-initial-%d", index), fmt.Sprintf("history-baseline-%d", index))
		if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
			t.Fatalf("write history transcript %s: %v", path, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat history transcript %s: %v", path, err)
		}
		initialOffsets[path] = info.Size()
		checkpoint := listenerRecoveryHistoryCheckpoint(path, fmt.Sprintf("history-session-%d", index), fmt.Sprintf("history-thread-%d", index), info)
		if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
			history[checkpoint.ID] = checkpoint
			*ready = time.Now().UTC().Add(-time.Minute)
			return nil
		}); err != nil {
			t.Fatalf("seed history checkpoint %s: %v", path, err)
		}
		if err := appendListenerRecoveryTranscript(path, listenerRecoveryTranscriptLine(fmt.Sprintf("history-tail-%d", index), fmt.Sprintf("history-tail-status-%d", index))); err != nil {
			t.Fatalf("append history transcript %s: %v", path, err)
		}
	}

	headEntered := make(chan struct{})
	var headOnce sync.Once
	tailEntered := make(chan struct{})
	var tailOnce sync.Once
	bridge.historyWatchPathHook = func(ctx context.Context, path string) error {
		if filepath.Clean(path) == filepath.Clean(paths[0]) {
			headOnce.Do(func() { close(headEntered) })
			select {
			case <-tailEntered:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		tailOnce.Do(func() { close(tailEntered) })
		return nil
	}
	// Keep the first listener cycle out of the five-minute reconciliation path;
	// this test is about changed-path fairness, not project discovery.
	bridge.lastHistoryWatchReconcile = time.Now().UTC()
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, time.Now().UTC().Add(-time.Minute))

	listener := startListenerRecovery(t, bridge, listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor))
	waitListenerRecovery(t, func() bool {
		select {
		case <-headEntered:
			return true
		default:
			return false
		}
	}, listenerRecoveryProgressTimeout, "history-watch slow head to enter")
	waitListenerRecovery(t, func() bool {
		state, err := store.Load(context.Background())
		if err != nil {
			return false
		}
		for _, path := range paths {
			checkpoint := state.HistoryWatch[historyWatchCheckpointID(path)]
			if checkpoint.Offset <= initialOffsets[path] {
				return false
			}
		}
		return true
	}, listenerRecoveryProgressTimeout, "history-watch healthy tail and slow head")
	listener.stop(t)
}

// TestTeamsListenFalseHistoryWatchFullPoolDoesNotStarveHealthyTail is the
// full-pool counterpart of the two-path history-watch regression.  It catches
// the case where every worker is occupied by a cooperative slow path and a
// later healthy path would otherwise receive only an already-cancelled phase
// context on every cycle.
func TestTeamsListenFalseHistoryWatchFullPoolDoesNotStarveHealthyTail(t *testing.T) {
	store := newBridgeTestStore(t)
	graph, _ := newListenerRecoveryGraph(t, nil, nil, 0)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.transcriptSyncWorkerCount = 4
	root := filepath.Join(t.TempDir(), "codex")
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0o700); err != nil {
		t.Fatalf("mkdir history root: %v", err)
	}
	bridge.scope.CodexHome = root
	const pathCount = 5
	paths := make([]string, 0, pathCount)
	initialOffsets := make(map[string]int64, pathCount)
	for index := 0; index < pathCount; index++ {
		path := filepath.Join(root, "sessions", fmt.Sprintf("%c-history.jsonl", 'a'+index))
		initial := listenerRecoveryTranscriptLine(fmt.Sprintf("full-pool-history-initial-%d", index), fmt.Sprintf("full-pool-history-baseline-%d", index))
		if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
			t.Fatalf("write history transcript %s: %v", path, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat history transcript %s: %v", path, err)
		}
		initialOffsets[path] = info.Size()
		checkpoint := listenerRecoveryHistoryCheckpoint(path, fmt.Sprintf("full-pool-history-session-%d", index), fmt.Sprintf("full-pool-history-thread-%d", index), info)
		if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
			history[checkpoint.ID] = checkpoint
			*ready = time.Now().UTC().Add(-time.Minute)
			return nil
		}); err != nil {
			t.Fatalf("seed history checkpoint %s: %v", path, err)
		}
		if err := appendListenerRecoveryTranscript(path, listenerRecoveryTranscriptLine(fmt.Sprintf("full-pool-history-tail-%d", index), fmt.Sprintf("full-pool-history-tail-status-%d", index))); err != nil {
			t.Fatalf("append history transcript %s: %v", path, err)
		}
		paths = append(paths, path)
	}

	pathIndex := make(map[string]int, len(paths))
	for index, path := range paths {
		pathIndex[filepath.Clean(path)] = index
	}
	healthyEntered := make(chan struct{})
	var healthyOnce sync.Once
	bridge.historyWatchPathHook = func(ctx context.Context, path string) error {
		index := pathIndex[filepath.Clean(path)]
		if index == pathCount-1 {
			healthyOnce.Do(func() { close(healthyEntered) })
			return nil
		}
		<-ctx.Done()
		return ctx.Err()
	}
	bridge.lastHistoryWatchReconcile = time.Now().UTC()
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, time.Now().UTC().Add(-time.Minute))

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor)
	options.PhaseBudget = 2 * time.Second
	listener := startListenerRecovery(t, bridge, options)
	waitListenerRecovery(t, func() bool {
		select {
		case <-healthyEntered:
			return true
		default:
			return false
		}
	}, listenerRecoveryProgressTimeout, "history-watch healthy tail to enter after full slow worker pool")

	healthyPath := paths[len(paths)-1]
	healthyInfo, err := os.Stat(healthyPath)
	if err != nil {
		listener.stop(t)
		t.Fatalf("stat healthy history transcript: %v", err)
	}
	waitListenerRecovery(t, func() bool {
		state, loadErr := store.Load(context.Background())
		if loadErr != nil {
			return false
		}
		return state.HistoryWatch[historyWatchCheckpointID(healthyPath)].Offset == healthyInfo.Size()
	}, listenerRecoveryProgressTimeout, "history-watch healthy tail after full slow worker pool")
	listener.stop(t)
}

// TestTeamsListenFalseOwnerLossCancelsHistoryWatchBeforeStaleCommit proves
// that the heartbeat failure is part of the listener's phase context.  A
// history scan that is still in progress when another owner takes over must
// stop before it can publish or advance the old owner's checkpoint.
func TestTeamsListenFalseOwnerLossCancelsHistoryWatchBeforeStaleCommit(t *testing.T) {
	store := newBridgeTestStore(t)
	graph, _ := newListenerRecoveryGraph(t, nil, nil, 0)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.ownerHeartbeatInterval = 5 * time.Millisecond
	root := filepath.Join(t.TempDir(), "codex")
	path := filepath.Join(root, "sessions", "owner-loss.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir history root: %v", err)
	}
	bridge.scope.CodexHome = root
	initial := listenerRecoveryTranscriptLine("owner-loss-initial", "owner-loss-baseline")
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write owner-loss transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat owner-loss transcript: %v", err)
	}
	checkpoint := listenerRecoveryHistoryCheckpoint(path, "owner-loss-session", "owner-loss-thread", info)
	if err := store.UpdateHistoryWatch(context.Background(), func(history map[string]teamstore.HistoryWatchCheckpoint, ready *time.Time) error {
		history[checkpoint.ID] = checkpoint
		*ready = time.Now().UTC().Add(-time.Minute)
		return nil
	}); err != nil {
		t.Fatalf("seed owner-loss checkpoint: %v", err)
	}
	if err := appendListenerRecoveryTranscript(path, listenerRecoveryTranscriptLine("owner-loss-tail", "owner-loss-tail-status")); err != nil {
		t.Fatalf("append owner-loss transcript: %v", err)
	}
	bridge.lastHistoryWatchReconcile = time.Now().UTC()
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, time.Now().UTC().Add(-time.Minute))

	entered := make(chan struct{})
	hookExited := make(chan struct{})
	var enteredOnce sync.Once
	bridge.historyWatchPathHook = func(ctx context.Context, candidate string) error {
		if filepath.Clean(candidate) != filepath.Clean(path) {
			return nil
		}
		enteredOnce.Do(func() { close(entered) })
		<-ctx.Done()
		close(hookExited)
		return ctx.Err()
	}
	listenCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listenDone := make(chan error, 1)
	go func() {
		options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor)
		// Leave enough room that the hook can only exit because the owner
		// heartbeat cancels the phase, not because the ordinary phase budget
		// expires first.
		options.PhaseBudget = 5 * time.Second
		options.PollWorkerBudget = time.Second
		listenDone <- bridge.Listen(listenCtx, options)
	}()
	select {
	case <-entered:
	case err := <-listenDone:
		t.Fatalf("listener exited before owner-loss hook: %v", err)
	case <-time.After(listenerRecoveryProgressTimeout):
		t.Fatal("owner-loss history-watch hook was not entered")
	}
	oldGeneration := bridge.currentLeaseGeneration()
	if oldGeneration <= 0 {
		t.Fatal("listener did not acquire a control-lease generation")
	}
	ownerLossAt := time.Now()
	if released, err := store.ReleaseControlLeaseIfHolder(context.Background(), bridge.machine.ID, oldGeneration); err != nil || !released {
		t.Fatalf("release old owner: released=%v err=%v", released, err)
	}
	replacement := teamstore.MachineRecord{ID: "owner-loss-replacement", ScopeID: bridge.scope.ID, Kind: teamstore.MachineKindPrimary}
	decision, err := store.ClaimControlLease(context.Background(), teamstore.ControlLeaseClaim{
		Scope: bridge.scope, Machine: replacement, Duration: time.Minute, Now: time.Now().UTC(),
	})
	if err != nil || decision.Mode != teamstore.LeaseModeActive {
		t.Fatalf("claim replacement owner: mode=%v err=%v", decision.Mode, err)
	}
	// Losing the lease does not require the process to exit: the production
	// listener is allowed to remain alive in standby and wait for a later
	// takeover.  The invariant here is that the owner-bound scan is canceled
	// and cannot commit; cancel the test listener explicitly after the
	// replacement is durable so this test does not conflate standby with a
	// failure to stop.
	waitListenerRecovery(t, func() bool {
		select {
		case <-hookExited:
			return true
		default:
			return false
		}
	}, 2*time.Second, "owner-loss history-watch cancellation")
	if elapsed := time.Since(ownerLossAt); elapsed >= 2*time.Second {
		t.Fatalf("history-watch cancellation took %s; phase timeout may have masked owner loss", elapsed)
	}
	if stats := bridge.mainLoopPhaseStatsSnapshot("history-watch"); stats.DeadlineExceeded != 0 {
		t.Fatalf("history-watch phase reached its ordinary deadline during owner loss: %#v", stats)
	}
	cancel()
	select {
	case err := <-listenDone:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
			t.Fatalf("listener owner-loss shutdown error = %v", err)
		}
	case <-time.After(listenerRecoveryProgressTimeout):
		t.Fatal("listener did not stop after explicit owner-loss test cancellation")
	}
	state, err := store.HistoryWatchState(context.Background())
	if err != nil {
		t.Fatalf("load owner-loss history-watch state: %v", err)
	}
	if got := state.HistoryWatch[checkpoint.ID]; got.Offset != checkpoint.Offset {
		t.Fatalf("stale owner advanced history-watch offset from %d to %d", checkpoint.Offset, got.Offset)
	}
}

// TestTeamsListenFalseOwnerLossFencesCooperativeTurn proves that a live
// Codex turn is canceled by loss of the durable owner lease even while the
// listener root context remains active.  The old worker must leave the turn
// as a takeover candidate and must not run its generic failure/follow-up path
// after a replacement generation has claimed the lease.
func TestTeamsListenFalseOwnerLossFencesCooperativeTurn(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	message := bridgeTestMessageWithText("owner-loss-turn-message", "LISTENER_RECOVERY_OWNER_LOSS_PROMPT")
	message.ChatID = "chat-1"
	message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1": {message},
	}, 0)
	store := newBridgeTestStore(t)
	executor := &listenerRecoveryCooperativeCancellationExecutor{started: make(chan struct{})}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.ownerHeartbeatInterval = 5 * time.Millisecond
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)

	listenCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listenDone := make(chan error, 1)
	go func() {
		options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
		// The executor is intentionally cooperative; this larger phase budget
		// makes the owner-loss assertion distinguish heartbeat cancellation from
		// the normal phase deadline.
		options.PhaseBudget = 5 * time.Second
		options.PollWorkerBudget = time.Second
		listenDone <- bridge.Listen(listenCtx, options)
	}()
	select {
	case <-executor.started:
	case err := <-listenDone:
		t.Fatalf("listener exited before owner-loss turn: %v", err)
	case <-time.After(listenerRecoveryProgressTimeout):
		t.Fatalf("owner-loss turn was not dispatched; Graph reads=%d", graphState.getCount("chat-1"))
	}

	oldMachineID := bridge.machine.ID
	oldGeneration := bridge.currentLeaseGeneration()
	if oldMachineID == "" || oldGeneration <= 0 {
		t.Fatalf("listener owner capability = machine=%q generation=%d", oldMachineID, oldGeneration)
	}
	ownerLossAt := time.Now()
	if released, err := store.ReleaseControlLeaseIfHolder(context.Background(), oldMachineID, oldGeneration); err != nil || !released {
		t.Fatalf("release old owner lease: released=%v err=%v", released, err)
	}
	replacementMachine := teamstore.MachineRecord{ID: "owner-loss-turn-replacement", ScopeID: bridge.scope.ID, Kind: teamstore.MachineKindPrimary}
	replacementOwner, err := teamstore.CurrentOwner("owner-loss-replacement", "", "", time.Now().UTC())
	if err != nil {
		t.Fatalf("current replacement owner: %v", err)
	}
	decision, err := store.ClaimControlLease(context.Background(), teamstore.ControlLeaseClaim{
		Scope: bridge.scope, Machine: replacementMachine, Owner: replacementOwner, Duration: time.Minute, Now: time.Now().UTC(),
	})
	if err != nil || decision.Mode != teamstore.LeaseModeActive {
		t.Fatalf("claim replacement owner: mode=%v err=%v", decision.Mode, err)
	}

	// A replaced owner may leave this process in the normal standby loop.  Do
	// not make process exit part of the ownership-fencing assertion; wait for
	// the cooperative worker to observe the owner cancellation, then close the
	// test listener explicitly.
	waitListenerRecovery(t, func() bool {
		return bridge.activeAsyncTurnCount() == 0
	}, 2*time.Second, "owner-loss cooperative turn cancellation")
	if elapsed := time.Since(ownerLossAt); elapsed >= 2*time.Second {
		t.Fatalf("cooperative turn cancellation took %s; phase timeout may have masked owner loss", elapsed)
	}
	cancel()
	select {
	case err := <-listenDone:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
			t.Fatalf("listener owner-loss shutdown error = %v", err)
		}
	case <-time.After(listenerRecoveryProgressTimeout):
		t.Fatal("listener did not stop after explicit owner-loss test cancellation")
	}
	waitListenerRecovery(t, func() bool { return bridge.activeAsyncTurnCount() == 0 }, time.Second, "owner-loss turn cancellation")

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load owner-loss turn state: %v", err)
	}
	var runningTurns int
	for _, turn := range state.Turns {
		if turn.SessionID != "s001" {
			continue
		}
		if turn.Status != teamstore.TurnStatusRunning {
			t.Fatalf("owner-loss worker changed turn after takeover: %#v", turn)
		}
		runningTurns++
	}
	if runningTurns != 1 {
		t.Fatalf("running turns after owner loss = %d, want one takeover candidate", runningTurns)
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.TurnID != "" && (outbox.Kind == "error" || outbox.Kind == "interrupted" || strings.HasSuffix(outbox.ID, ":queued-turn-error")) {
			t.Fatalf("owner-loss worker ran failure/follow-up side effect: %#v", outbox)
		}
	}
}

// TestTeamsListenFalseStartupHeartbeatProtectsSlowInitialization proves that
// a process which already owns the durable lease keeps renewing it while
// startup reconciliation is slow.  A second process must remain standby
// rather than taking over merely because the first listener has not reached
// its main loop yet.
func TestTeamsListenFalseStartupHeartbeatProtectsSlowInitialization(t *testing.T) {
	graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	// Keep the lease deliberately short, but leave enough scheduling and file
	// I/O margin for the full package test suite.  A 50ms lease made this test
	// measure unrelated CI contention instead of startup-heartbeat behavior.
	bridge.leaseDuration = 500 * time.Millisecond
	bridge.ownerHeartbeatInterval = 20 * time.Millisecond

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseStartup := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseStartup()
	graphState.mu.Lock()
	graphState.topicUpdateEntered = entered
	graphState.topicUpdateRelease = release
	graphState.mu.Unlock()

	listenCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listenDone := make(chan error, 1)
	go func() {
		options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor)
		options.OwnerStaleAfter = 250 * time.Millisecond
		listenDone <- bridge.Listen(listenCtx, options)
	}()
	select {
	case <-entered:
	case err := <-listenDone:
		t.Fatalf("listener exited before slow startup hook: %v", err)
	case <-time.After(listenerRecoveryProgressTimeout):
		t.Fatal("listener did not reach the slow startup hook")
	}

	initialOwner, ok, err := store.ReadOwner(context.Background())
	if err != nil || !ok {
		t.Fatalf("read owner during slow startup: ok=%v err=%v owner=%#v", ok, err, initialOwner)
	}
	var renewedOwner teamstore.OwnerMetadata
	var renewed bool
	waitListenerRecovery(t, func() bool {
		var readErr error
		renewedOwner, renewed, readErr = store.ReadOwner(context.Background())
		return readErr == nil && renewed && renewedOwner.LastHeartbeat.After(initialOwner.LastHeartbeat)
	}, listenerRecoveryProgressTimeout, "startup heartbeat renewal")
	replacementMachine := teamstore.MachineRecord{ID: "startup-heartbeat-replacement", ScopeID: bridge.scope.ID, Kind: teamstore.MachineKindPrimary}
	replacementOwner, err := teamstore.CurrentOwner("startup-heartbeat-replacement", "", "", time.Now().UTC())
	if err != nil {
		t.Fatalf("current replacement owner: %v", err)
	}
	decision, err := store.ClaimControlLease(context.Background(), teamstore.ControlLeaseClaim{
		Scope: bridge.scope, Machine: replacementMachine, Owner: replacementOwner, Duration: time.Minute, Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("claim replacement during startup: %v", err)
	}
	if decision.Mode != teamstore.LeaseModeStandby {
		releaseStartup()
		cancel()
		t.Fatalf("replacement took over during protected startup: mode=%v lease=%#v", decision.Mode, decision.Lease)
	}

	releaseStartup()
	cancel()
	select {
	case err := <-listenDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("listener shutdown after startup heartbeat test: %v", err)
		}
	case <-time.After(listenerRecoveryProgressTimeout):
		t.Fatal("listener did not stop after startup heartbeat test")
	}
}

// TestTeamsListenFalseTaskStartedPromptRaceRecoversAfterNextCycle reproduces
// the s528/s512 writer ordering that caused the original false orphan gate:
// Codex durably appends task_started, the listener observes that complete
// record, and only then appends the external user prompt and final.  The first
// snapshot must remain a silent pending boundary; the second snapshot must
// release it and produce a real Teams-visible final through the listener.
func TestTeamsListenFalseTaskStartedPromptRaceRecoversAfterNextCycle(t *testing.T) {
	store := newBridgeTestStore(t)
	graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	root := t.TempDir()
	bridge.scope.CodexHome = root
	path := filepath.Join(root, "s001.jsonl")
	initial := listenerRecoveryTranscriptLine("initial-s001", "baseline-s001")
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("missing s001 registry session")
	}
	session.CodexThreadID = "thread-s001"
	session.Cwd = root
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("persist linked session: %v", err)
	}
	if _, _, err := store.UpdateSessionContext(context.Background(), session.ID, func(current teamstore.SessionContext, found bool, now time.Time) (teamstore.SessionContext, bool, error) {
		if !found {
			return current, false, fmt.Errorf("linked session was not persisted")
		}
		current.CodexThreadID = session.CodexThreadID
		current.Cwd = root
		current.UpdatedAt = now
		return current, true, nil
	}); err != nil {
		t.Fatalf("persist linked session context: %v", err)
	}
	listenerRecoverySeedLinkedCheckpoint(t, store, session, path, true)
	outerFinal := listenerRecoveryTranscriptFinalLine("outer-final", "LISTENER_RECOVERY_TASK_PROMPT_RACE_OUTER_FINAL")
	if err := appendListenerRecoveryTranscript(path, outerFinal+listenerRecoveryTranscriptTaskStartedLine("new-root")); err != nil {
		t.Fatalf("append task_started: %v", err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor)
	options.PhaseBudget = 2 * time.Second
	listener := startListenerRecovery(t, bridge, options)
	pendingBoundary := func() bool {
		state, err := store.Load(context.Background())
		if err != nil {
			return false
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
		return checkpoint.PendingHistoryRange != nil &&
			strings.EqualFold(checkpoint.PendingHistoryRange.Kind, "pending_root_task_started") &&
			checkpoint.UnresolvedExecution == nil
	}
	deadline := time.Now().Add(listenerRecoveryProgressTimeout)
	for !pendingBoundary() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !pendingBoundary() {
		state, _ := store.Load(context.Background())
		phase := bridge.mainLoopPhaseStatsSnapshot("linked-transcript")
		listener.stop(t)
		t.Fatalf("task_started-only snapshot did not persist a pending boundary; state=%#v phase=%#v", state.ImportCheckpoints[transcriptCheckpointID(session.ID)], phase)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		listener.stop(t)
		t.Fatalf("load pending state: %v", err)
	}
	checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.TranscriptQuarantine != nil || checkpoint.ContextGap != nil {
		t.Fatalf("task_started-only snapshot created a quarantine: %#v", checkpoint)
	}
	// Root-release is allowed only when the marker's Codex turn is tied to a
	// durable Teams turn.  This is the production proof that distinguishes a
	// normal Teams request from an internal continuation; the fixture models the
	// turn record that the inbound path writes before Codex starts streaming.
	if err := store.Update(context.Background(), func(state *teamstore.State) error {
		state.InboundEvents["inbound-task-prompt-race"] = teamstore.InboundEvent{
			ID:          "inbound-task-prompt-race",
			SessionID:   session.ID,
			TeamsChatID: session.ChatID,
			Source:      "teams",
			Status:      teamstore.InboundStatusPersisted,
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}
		state.Turns["turn-task-prompt-race"] = teamstore.Turn{
			ID:             "turn-task-prompt-race",
			SessionID:      session.ID,
			InboundEventID: "inbound-task-prompt-race",
			Status:         teamstore.TurnStatusCompleted,
			CodexThreadID:  session.CodexThreadID,
			CodexTurnID:    "new-root",
			CreatedAt:      time.Now().UTC(),
			UpdatedAt:      time.Now().UTC(),
		}
		return nil
	}); err != nil {
		listener.stop(t)
		t.Fatalf("seed durable Teams turn proof: %v", err)
	}

	prompt := listenerRecoveryTranscriptPromptWithTurnLine("new-prompt", "new-root", "new Teams prompt")
	final := listenerRecoveryTranscriptFinalWithTurnLine("new-final", "new-root", "LISTENER_RECOVERY_TASK_PROMPT_RACE_FINAL")
	if err := appendListenerRecoveryTranscript(path, prompt+final); err != nil {
		listener.stop(t)
		t.Fatalf("append prompt/final: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		listener.stop(t)
		t.Fatalf("stat completed transcript: %v", err)
	}
	recovered := func() bool {
		state, err := store.Load(context.Background())
		if err != nil {
			return false
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
		if checkpoint.UnresolvedExecution != nil || checkpoint.PendingHistoryRange != nil || checkpoint.LastOffset != info.Size() {
			return false
		}
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_TASK_PROMPT_RACE_FINAL") {
				return true
			}
		}
		return false
	}
	// Releasing the semantic frontier is a separate durable CAS from consuming
	// the newly visible prompt/final.  The listener therefore needs one cycle
	// to release the boundary and a subsequent cycle to deliver the records.
	deadline = time.Now().Add(listenerRecoveryProgressTimeout)
	for !recovered() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !recovered() {
		state, _ := store.Load(context.Background())
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
		probe, probeErr := bridge.readLinkedTranscriptDelta(path, checkpoint, session.CodexThreadID, session.CodexThreadID)
		teamsProof := linkedTranscriptRootReleaseTeamsProof(context.Background(), store, *session, probe.RootReleaseWitness)
		sourceProof := transcriptRootReleaseWitnessMatchesSource(path, checkpoint, probe.RootReleaseWitness)
		listener.stop(t)
		t.Fatalf("task_started/prompt race did not recover; checkpoint=%#v pending=%+v probe=%#v probeErr=%v teamsProof=%t sourceProof=%t turns=%#v sent=%#v phase=%#v", checkpoint, checkpoint.PendingHistoryRange, probe, probeErr, teamsProof, sourceProof, state.Turns, graphState.sentSnapshot(), bridge.mainLoopPhaseStatsSnapshot("linked-transcript"))
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load recovered state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
	if checkpoint.UnresolvedExecution != nil || checkpoint.PendingHistoryRange != nil || checkpoint.LastOffset != info.Size() {
		t.Fatalf("race recovery checkpoint = %#v, want clean EOF", checkpoint)
	}
	if strings.Contains(sentPlainJoinedListenerRecovery(graphState.sentSnapshot()), "publish-history") {
		t.Fatalf("race recovery emitted a manual history gate: %#v", graphState.sentSnapshot())
	}
	listener.stop(t)
}

// TestTeamsListenFalseTaskStartedPromptRaceFromPolledTeamsTurn closes the
// remaining vertical-test gap: the race is triggered by an actual Teams poll
// and queue admission, not by directly inserting a synthetic durable turn.
// The fake executor only controls the transcript write timing; claim, owner,
// completion, checkpoint and Graph delivery all remain production paths.
func TestTeamsListenFalseTaskStartedPromptRaceFromPolledTeamsTurn(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	const verticalPrompt = "LISTENER_RECOVERY_VERTICAL_USER_PROMPT"
	message := bridgeTestMessageWithText("vertical-race-message", verticalPrompt)
	message.ChatID = "chat-1"
	message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1": {message},
	}, 0)
	store := newBridgeTestStore(t)
	root := t.TempDir()
	path := filepath.Join(root, "s001.jsonl")
	initial := listenerRecoveryTranscriptLine("vertical-race-initial", "baseline")
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, nil)
	bridge.scope.CodexHome = root
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("missing s001 registry session")
	}
	session.CodexThreadID = "thread-vertical-race"
	session.Cwd = root
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("persist vertical race session: %v", err)
	}
	if _, _, err := store.UpdateSessionContext(context.Background(), session.ID, func(current teamstore.SessionContext, found bool, updatedAt time.Time) (teamstore.SessionContext, bool, error) {
		if !found {
			return current, false, errors.New("vertical race session was not persisted")
		}
		current.CodexThreadID = session.CodexThreadID
		current.Cwd = root
		current.UpdatedAt = updatedAt
		return current, true, nil
	}); err != nil {
		t.Fatalf("persist vertical race session context: %v", err)
	}
	listenerRecoverySeedLinkedCheckpoint(t, store, session, path, true)
	executor := &listenerRecoveryTaskPromptRaceExecutor{
		path:    path,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	bridge.executor = executor
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, session.ChatID, now)

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	options.PhaseBudget = 5 * time.Second
	options.PollWorkerBudget = time.Second
	listener := startListenerRecovery(t, bridge, options)
	finalID := "outbox:turn:inbound:chat-1:vertical-race-message:final"
	select {
	case <-executor.started:
	case <-time.After(3 * listenerRecoveryProgressTimeout):
		listener.stop(t)
		t.Fatalf("vertical race executor was not reached; Graph reads=%d state=%#v", graphState.getCount(session.ChatID), mustListenerRecoveryState(t, store))
	}
	// A live Teams turn is intentionally excluded from the independent linked
	// transcript worker while its owner is running. Observe completed linked
	// transcript cycles at this boundary rather than sleeping for a guessed
	// duration: the scanner must not manufacture a history gate or a second
	// answer merely because task_started is already visible.
	waitListenerRecovery(t, func() bool {
		return bridge.mainLoopPhaseStatsSnapshot("linked-transcript").Runs >= 2
	}, listenerRecoveryProgressTimeout, "linked transcript cycles while task_started is visible")
	if plain := sentPlainJoinedListenerRecovery(graphState.sentSnapshot()); strings.Contains(plain, "publish-history") || strings.Contains(plain, "previous Codex execution is still unconfirmed") {
		listener.stop(t)
		t.Fatalf("active vertical turn emitted a premature history/recovery gate: %s", plain)
	}
	close(executor.release)
	completed := func() bool {
		state, err := store.Load(context.Background())
		if err != nil {
			return false
		}
		turnCompleted := false
		for _, turn := range state.Turns {
			if turn.SessionID == session.ID && turn.Status == teamstore.TurnStatusCompleted {
				turnCompleted = true
				break
			}
		}
		if !turnCompleted {
			return false
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
		if checkpoint.PendingHistoryRange != nil || checkpoint.UnresolvedExecution != nil {
			return false
		}
		finalOutbox, found := state.OutboxMessages[finalID]
		if !found || finalOutbox.Status != teamstore.OutboxStatusSent || !finalOutbox.NextAttemptAt.IsZero() {
			return false
		}
		return strings.Contains(sentPlainJoinedListenerRecovery(graphState.sentSnapshot()), "LISTENER_RECOVERY_VERTICAL_TASK_PROMPT_RACE_FINAL")
	}
	if !waitListenerRecoveryResult(completed, listenerRecoveryProgressTimeout) {
		state, _ := store.Load(context.Background())
		listener.stop(t)
		finalID := "outbox:turn:inbound:chat-1:vertical-race-message:final"
		pending := make([]string, 0)
		allOutbox := make([]string, 0, len(state.OutboxMessages))
		for id, msg := range state.OutboxMessages {
			diagnostic := fmt.Sprintf("%s status=%s next=%s err=%q kind=%s seq=%d turn=%s source=%s:%d/%t proof=%s:%d/%t blocked=(anchor:%t terminal:%t rewrite:%t)", id, msg.Status, msg.NextAttemptAt.Format(time.RFC3339Nano), msg.LastSendError, msg.Kind, msg.Sequence, msg.TurnID, msg.TranscriptSourcePath, msg.TranscriptSourceOffset, msg.TranscriptSourceOffsetKnown, msg.TranscriptSourceProofFingerprint, msg.TranscriptSourceProofOffset, msg.TranscriptSourceProofOffsetKnown, msg.BlockedByUnresolvedExecution, msg.BlockedByTerminalFailure, msg.BlockedBySourceRewrite)
			allOutbox = append(allOutbox, diagnostic)
			if msg.Status == teamstore.OutboxStatusQueued || msg.Status == teamstore.OutboxStatusSending || msg.Status == teamstore.OutboxStatusAccepted {
				pending = append(pending, diagnostic)
			}
		}
		sort.Strings(allOutbox)
		sort.Strings(pending)
		t.Fatalf("vertical task/prompt race did not complete; turn=%#v final=%#v checkpoint=%#v pending=%v all-outbox=%v rate-limits=%#v sent=%v graph-errors=%v phases poll=%#v linked=%#v outbox=%#v listener-err=%v", state.Turns["turn:inbound:chat-1:vertical-race-message"], state.OutboxMessages[finalID], state.ImportCheckpoints[transcriptCheckpointID(session.ID)], pending, allOutbox, state.ChatRateLimits, graphState.sentSnapshot(), graphState.errorsSnapshot(), bridge.mainLoopPhaseStatsSnapshot("poll"), bridge.mainLoopPhaseStatsSnapshot("linked-transcript"), bridge.mainLoopPhaseStatsSnapshot("outbox"), listener.err)
	}
	if got := countListenerRecoveryTranscriptFinals(graphState.sentSnapshot(), "LISTENER_RECOVERY_VERTICAL_TASK_PROMPT_RACE_FINAL"); got != 1 {
		t.Fatalf("vertical race final POST count = %d, want exactly one; sent=%#v", got, graphState.sentSnapshot())
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load vertical race state after completion: %v", err)
	}
	finalOutbox, finalFound := state.OutboxMessages[finalID]
	if !finalFound || finalOutbox.Status != teamstore.OutboxStatusSent || !finalOutbox.NextAttemptAt.IsZero() {
		t.Fatalf("vertical race final durable delivery = %#v found=%t, want Sent with no retry gate", finalOutbox, finalFound)
	}
	linkedStats := bridge.mainLoopPhaseStatsSnapshot("linked-transcript")
	if linkedStats.Errors != 0 || linkedStats.DeadlineExceeded != 0 {
		t.Fatalf("vertical race linked-transcript phase had avoidable failures: %#v", linkedStats)
	}
	for id, outbox := range state.OutboxMessages {
		if outbox.SessionID != session.ID || !strings.HasPrefix(id, "outbox:transcript-delivery:") {
			continue
		}
		if strings.Contains(outbox.Body, verticalPrompt) {
			t.Fatalf("wrapped Teams user prompt created a transcript-delivery outbox row: %#v", outbox)
		}
	}
	if plain := sentPlainJoinedListenerRecovery(graphState.sentSnapshot()); strings.Contains(plain, "publish-history") || strings.Contains(plain, "previous Codex execution is still unconfirmed") {
		t.Fatalf("vertical race emitted a manual/recovery gate: %s", plain)
	} else if !strings.Contains(plain, "LISTENER_RECOVERY_STREAMING_PROGRESS") {
		t.Fatalf("vertical race did not exercise the streaming progress path: %s", plain)
	}
	listener.stop(t)
}

// TestTeamsListenFalsePollPhaseTimeoutDoesNotPoisonNextCycle exercises the
// cancellation boundary that is easy to miss in a direct poll test.  The
// first Graph request is canceled by the short phase budget after its poll
// attempt has been claimed.  The next cycle must be able to acquire the same
// chat and deliver the message; a canceled cleanup context would leave the
// durable Attempt until its normal two-minute TTL and make this chat appear
// frozen.
func TestTeamsListenFalsePollPhaseTimeoutDoesNotPoisonNextCycle(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	message := bridgeTestMessageWithText("phase-timeout-message", "LISTENER_RECOVERY_PHASE_TIMEOUT_PROMPT")
	message.ChatID = "chat-1"
	message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1": {message},
	}, 0)
	graphState.delayOnce = map[string]time.Duration{"chat-1": 8 * time.Second}
	store := newBridgeTestStore(t)
	executor := &listenerRecoveryExecutor{
		called: make(chan string),
		result: ExecutionResult{
			Text:          "LISTENER_RECOVERY_PHASE_TIMEOUT_FINAL",
			CodexThreadID: "thread-phase-timeout",
			CodexTurnID:   "turn-phase-timeout",
		},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	// Force the mutation grace to expire during the first canceled Graph read;
	// the production refresh-after-I/O path must still leave enough time to
	// abandon the attempt before the next listener cycle.  Keep the refreshed
	// window comfortably above normal test scheduling noise: this test is
	// checking the refresh boundary, not imposing an unrealistically tiny
	// budget on the successful second-cycle handler/commit.
	bridge.pollAttemptDurableGrace = 500 * time.Millisecond
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	// Leave enough time for the real SQLite admission path under -race, while
	// keeping the phase deadline ahead of the deliberately slower Graph read.
	// The worker budget must exceed the phase budget; otherwise the worker would
	// cancel itself and this test would not exercise phase-owned cleanup.
	options.PhaseBudget = 3 * time.Second
	options.PollWorkerBudget = 5 * time.Second
	listener := startListenerRecovery(t, bridge, options)
	waitListenerRecovery(t, func() bool {
		return graphState.getCount("chat-1") >= 1 && bridge.mainLoopPhaseStatsSnapshot("poll").DeadlineExceeded > 0
	}, listenerRecoveryProgressTimeout, "phase deadline after claiming chat poll")

	if !waitListenerRecoveryResult(func() bool {
		return len(executor.callsSnapshot()) == 1
	}, 20*time.Second) {
		state, _ := store.Load(context.Background())
		listener.stop(t)
		t.Fatalf("next-cycle poll after phase timeout did not dispatch; gets=%d calls=%#v state=%#v phase=%#v", graphState.getCount("chat-1"), executor.callsSnapshot(), state, bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	waitListenerRecovery(t, func() bool {
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_PHASE_TIMEOUT_FINAL") {
				return true
			}
		}
		return false
	}, 20*time.Second, "phase-timeout final delivery")
	messageModifiedAt, err := time.Parse(time.RFC3339Nano, message.LastModifiedDateTime)
	if err != nil {
		t.Fatalf("parse phase-timeout message timestamp: %v", err)
	}
	// The final outbox POST is intentionally asynchronous with the poll
	// attempt's terminal CAS.  Wait for the successful retry to publish its
	// durable cursor before canceling the listener; otherwise a slow runner can
	// cancel the commit and leave a perfectly valid follow-up Attempt in the
	// state, making this test race with its own cleanup.
	waitListenerRecovery(t, func() bool {
		state, loadErr := store.Load(context.Background())
		if loadErr != nil {
			return false
		}
		poll := state.ChatPolls["chat-1"]
		return !poll.LastModifiedCursor.Before(messageModifiedAt)
	}, 20*time.Second, "phase-timeout durable cursor")

	// The listener continues polling after the final outbox side effect. Stop
	// it after the successful retry's cursor is durable. A later, perfectly
	// valid poll may still have an Attempt in flight; that is not the stale
	// capability this regression is guarding against.
	listener.stop(t)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load phase-timeout state: %v", err)
	}
	poll := state.ChatPolls["chat-1"]
	if poll.LastModifiedCursor.Before(messageModifiedAt) {
		t.Fatalf("phase-timeout cursor remained behind recovered message: %#v", poll)
	}
	if got := countListenerRecoveryInbound(state, "s001", message.ID); got != 1 {
		t.Fatalf("phase-timeout inbound count = %d, want one; state=%#v", got, state.InboundEvents)
	}
}

// TestTeamsListenFalseSlowInboundMutationDoesNotConsumeDurableCleanupGrace
// protects the boundary between the Graph page budget and the post-claim
// mutation budget.  An ACK POST may legitimately take longer than the small
// cleanup grace used after a page is claimed.  The handler must use the phase
// context for that operation, then the poll attempt must receive a fresh
// durable context before its terminal CAS.  Reusing the cleanup context for
// both operations leaves the attempt stuck after an otherwise successful
// inbound message.
func TestTeamsListenFalseSlowInboundMutationDoesNotConsumeDurableCleanupGrace(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	message := bridgeTestMessageWithText("slow-inbound-mutation-message", "LISTENER_RECOVERY_SLOW_INBOUND_PROMPT")
	message.ChatID = "chat-1"
	message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1": {message},
	}, 100*time.Millisecond)
	store := newBridgeTestStore(t)
	executor := &listenerRecoveryExecutor{
		called: make(chan string),
		result: ExecutionResult{
			Text:          "LISTENER_RECOVERY_SLOW_INBOUND_FINAL",
			CodexThreadID: "thread-slow-inbound",
			CodexTurnID:   "turn-slow-inbound",
		},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	// Deliberately make the post-claim cleanup grace shorter than the fake
	// provider's ACK latency.  This is still a bounded, cooperative provider
	// operation; the test should fail only if the handler consumes the cleanup
	// context instead of getting the phase context.
	bridge.pollAttemptDurableGrace = 50 * time.Millisecond
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	options.PhaseBudget = 5 * time.Second
	options.PollWorkerBudget = time.Second
	listener := startListenerRecovery(t, bridge, options)
	if !waitListenerRecoveryResult(func() bool {
		return len(executor.callsSnapshot()) == 1
	}, 8*time.Second) {
		state, _ := store.Load(context.Background())
		listener.stop(t)
		t.Fatalf("slow inbound mutation did not dispatch: gets=%d calls=%#v state=%#v phase=%#v", graphState.getCount("chat-1"), executor.callsSnapshot(), state, bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	waitListenerRecovery(t, func() bool {
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_SLOW_INBOUND_FINAL") {
				return true
			}
		}
		return false
	}, 8*time.Second, "slow inbound final delivery")

	state, err := store.Load(context.Background())
	if err != nil {
		listener.stop(t)
		t.Fatalf("load slow inbound state: %v", err)
	}
	if poll := state.ChatPolls["chat-1"]; poll.Attempt != nil {
		listener.stop(t)
		t.Fatalf("slow inbound poll attempt remained after handler/cleanup: %#v", poll)
	}
	if got := countListenerRecoveryInbound(state, "s001", message.ID); got != 1 {
		listener.stop(t)
		t.Fatalf("slow inbound count = %d, want one; state=%#v", got, state.InboundEvents)
	}
	if got := countListenerRecoveryTranscriptFinals(graphState.sentSnapshot(), "LISTENER_RECOVERY_SLOW_INBOUND_FINAL"); got != 1 {
		listener.stop(t)
		t.Fatalf("slow inbound final POST count = %d, want one; sent=%#v", got, graphState.sentSnapshot())
	}
	listener.stop(t)
}

func mustListenerRecoveryState(t *testing.T, store *teamstore.Store) teamstore.State {
	t.Helper()
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load listener recovery state: %v", err)
	}
	return state
}

func waitListenerRecoveryResult(waitFor func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if waitFor() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return waitFor()
}

// TestTeamsListenFalseLargeTranscriptRecordDoesNotBlockLaterFinal exercises
// the two local-source shapes that previously livelocked s514 and s519.  The
// test deliberately uses the complete listener and waits for the durable
// checkpoint plus the fake-Graph POST, rather than accepting a scanner result
// as evidence that the user-visible backlog recovered.
func TestTeamsListenFalseLargeTranscriptRecordDoesNotBlockLaterFinal(t *testing.T) {
	cases := []struct {
		name       string
		transcript string
	}{
		{
			name: "oversized-record",
			transcript: `{"type":"event_msg","payload":{"type":"agent_message","phase":"commentary","message":"` +
				strings.Repeat("x", historyTieredMaxRecordBytes+4096) + `"}}` + "\n",
		},
		{
			name: "large-invisible-record",
			transcript: `{"type":"item.completed","item":{"type":"message","role":"assistant","metadata":"` +
				strings.Repeat("x", historyTieredMaxRecordBytes-512*1024) + `"}}` + "\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newBridgeTestStore(t)
			graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			root := t.TempDir()
			bridge.scope.CodexHome = root
			path := filepath.Join(root, "s001.jsonl")
			initial := listenerRecoveryTranscriptLine("initial-large-"+tc.name, "baseline-large-"+tc.name)
			if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
				t.Fatalf("write initial transcript: %v", err)
			}
			session := bridge.reg.SessionByID("s001")
			if session == nil {
				t.Fatal("missing registry session")
			}
			session.CodexThreadID = "thread-large-" + tc.name
			session.Cwd = root
			if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
				t.Fatalf("persist linked session: %v", err)
			}
			if _, _, err := store.UpdateSessionContext(context.Background(), session.ID, func(current teamstore.SessionContext, found bool, now time.Time) (teamstore.SessionContext, bool, error) {
				if !found {
					return current, false, fmt.Errorf("linked session was not persisted")
				}
				current.CodexThreadID = session.CodexThreadID
				current.Cwd = root
				current.UpdatedAt = now
				return current, true, nil
			}); err != nil {
				t.Fatalf("persist linked session context: %v", err)
			}
			listenerRecoverySeedLinkedCheckpoint(t, store, session, path, true)
			if err := appendListenerRecoveryTranscript(path, tc.transcript); err != nil {
				t.Fatalf("write large transcript: %v", err)
			}
			largeInfo, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat large transcript: %v", err)
			}
			now := time.Now().UTC().Add(-time.Minute)
			listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
			listenerRecoverySeedDuePoll(t, store, session.ChatID, now)

			options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor)
			options.PhaseBudget = 2 * time.Second
			listener := startListenerRecovery(t, bridge, options)

			largeDispositionReady := waitListenerRecoveryResult(func() bool {
				state, loadErr := store.Load(context.Background())
				if loadErr != nil {
					return false
				}
				checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
				if checkpoint.LastOffset != largeInfo.Size() || checkpoint.Status == importCheckpointStatusBlocked || checkpoint.OversizedRecordBlocked {
					return false
				}
				if checkpoint.LastRecordID == "" || checkpoint.LastRecordID == "initial-"+session.ID {
					return false
				}
				if tc.name == "oversized-record" {
					if checkpoint.ContextGap == nil || checkpoint.TranscriptQuarantine == nil ||
						!strings.EqualFold(checkpoint.TranscriptQuarantine.Kind, "oversized_record") ||
						checkpoint.ContextGap.ExclusiveEndOffset != largeInfo.Size() {
						return false
					}
				}
				// An opaque record carries its durable disposition in the checkpoint
				// (and, for the oversized shape, the bounded quarantine).  It is not a
				// user-visible delivery and therefore must not manufacture a delivery
				// ledger row merely to make this test pass.
				return true
			}, 5*time.Second)
			if !largeDispositionReady {
				state, loadErr := store.Load(context.Background())
				listener.stop(t)
				t.Fatalf("timed out waiting for large transcript disposition: load=%v state=%#v phases linked=%#v outbox=%#v poll=%#v sent=%#v", loadErr, state, bridge.mainLoopPhaseStatsSnapshot("linked-transcript"), bridge.mainLoopPhaseStatsSnapshot("outbox"), bridge.mainLoopPhaseStatsSnapshot("poll"), graphState.sentSnapshot())
			}
			final := listenerRecoveryTranscriptFinalLine("final-large-"+tc.name, "LISTENER_RECOVERY_LARGE_RECORD_FINAL_"+tc.name)
			if err := appendListenerRecoveryTranscript(path, final); err != nil {
				listener.stop(t)
				t.Fatalf("append later final: %v", err)
			}
			fullInfo, err := os.Stat(path)
			if err != nil {
				listener.stop(t)
				t.Fatalf("stat full transcript: %v", err)
			}
			waitListenerRecovery(t, func() bool {
				state, loadErr := store.Load(context.Background())
				if loadErr != nil {
					return false
				}
				checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
				if checkpoint.LastOffset != fullInfo.Size() || checkpoint.Status == importCheckpointStatusBlocked || checkpoint.OversizedRecordBlocked {
					return false
				}
				for _, sent := range graphState.sentSnapshot() {
					if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_LARGE_RECORD_FINAL_"+tc.name) {
						return true
					}
				}
				return false
			}, 5*time.Second, "large transcript durable disposition")
			plain := sentPlainJoinedListenerRecovery(graphState.sentSnapshot())
			if strings.Contains(plain, "helper publish-history") || strings.Contains(plain, "previous Codex execution is still unconfirmed") {
				t.Fatalf("large transcript emitted a manual/recovery gate: %s", plain)
			}
			listener.stop(t)
		})
	}
}

// TestTeamsListenFalseSQLiteTranscriptBacklogProgresses exercises the same
// live linked-transcript queue path with the production SQLite backend. The
// owner capability is deliberately populated by Listen itself; a test that
// only calls the store API cannot catch a missing lease projection in the
// narrow SQLite transaction.
func TestTeamsListenFalseSQLiteTranscriptBacklogProgresses(t *testing.T) {
	store := newBridgeTestStore(t)
	if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("migrate store to SQLite: %v", err)
	}
	graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	root := t.TempDir()
	bridge.scope.CodexHome = root
	path := filepath.Join(root, "s001.jsonl")
	initial := listenerRecoveryTranscriptLine("sqlite-initial", "sqlite-baseline")
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial transcript: %v", err)
	}
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("missing registry session")
	}
	session.CodexThreadID = "thread-sqlite-listener"
	session.Cwd = root
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("persist linked session: %v", err)
	}
	if _, _, err := store.UpdateSessionContext(context.Background(), session.ID, func(current teamstore.SessionContext, found bool, now time.Time) (teamstore.SessionContext, bool, error) {
		if !found {
			return current, false, fmt.Errorf("linked session was not persisted")
		}
		current.CodexThreadID = session.CodexThreadID
		current.Cwd = root
		current.UpdatedAt = now
		return current, true, nil
	}); err != nil {
		t.Fatalf("persist session context: %v", err)
	}
	listenerRecoverySeedLinkedCheckpoint(t, store, session, path, true)
	final := listenerRecoveryTranscriptFinalLine("sqlite-final", "LISTENER_RECOVERY_SQLITE_FINAL")
	if err := appendListenerRecoveryTranscript(path, final); err != nil {
		t.Fatalf("append transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat transcript: %v", err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)

	listener := startListenerRecovery(t, bridge, listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor))
	progressed := waitListenerRecoveryResult(func() bool {
		state, loadErr := store.Load(context.Background())
		if loadErr != nil {
			return false
		}
		checkpoint := state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
		if checkpoint.LastOffset != info.Size() || checkpoint.Status == importCheckpointStatusBlocked {
			return false
		}
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_SQLITE_FINAL") {
				return true
			}
		}
		return false
	}, listenerRecoveryProgressTimeout)
	if !progressed {
		state, loadErr := store.Load(context.Background())
		checkpoint := teamstore.ImportCheckpoint{}
		if loadErr == nil {
			checkpoint = state.ImportCheckpoints[transcriptCheckpointID(session.ID)]
		}
		t.Fatalf("timed out waiting for SQLite linked transcript final: load=%v checkpoint=%#v sent=%#v graph-errors=%v phase-linked=%#v phase-outbox=%#v phase-poll=%#v", loadErr, checkpoint, graphState.sentSnapshot(), graphState.errorsSnapshot(), bridge.mainLoopPhaseStatsSnapshot("linked-transcript"), bridge.mainLoopPhaseStatsSnapshot("outbox"), bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	listener.stop(t)
	if got := countListenerRecoveryTranscriptFinals(graphState.sentSnapshot(), "LISTENER_RECOVERY_SQLITE_FINAL"); got != 1 {
		t.Fatalf("SQLite final POST count = %d, want one; sent=%#v", got, graphState.sentSnapshot())
	}
}

// TestTeamsListenFalseMalformedPollDoesNotBlockHealthyChat exercises the
// complete listener after reopening a legacy JSON store whose one chat-poll
// projection has a type-corrupt row. The bad row must remain local to that
// chat: the healthy chat still reaches the real poll/queue/execution/outbox
// path, and the listener itself keeps running with Once=false.
func TestTeamsListenFalseMalformedPollDoesNotBlockHealthyChat(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	healthy := bridgeTestMessageWithText("malformed-poll-healthy-message", "LISTENER_RECOVERY_MALFORMED_POLL_HEALTHY_PROMPT")
	healthy.ChatID = "chat-1"
	healthy.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	healthy.LastModifiedDateTime = healthy.CreatedDateTime
	malformedMessage := bridgeTestMessageWithText("malformed-poll-message", "LISTENER_RECOVERY_MALFORMED_POLL_PROMPT")
	malformedMessage.ChatID = "chat-malformed-poll"
	malformedMessage.CreatedDateTime = now.Add(2 * time.Second).Format(time.RFC3339Nano)
	malformedMessage.LastModifiedDateTime = malformedMessage.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1":              {healthy},
		"chat-malformed-poll": {malformedMessage},
	}, 0)
	store := newBridgeTestStore(t)
	seedBridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := seedBridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("missing healthy session")
	}
	if err := seedBridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("persist healthy session: %v", err)
	}
	malformedSession := appendBridgeTestSession(t, seedBridge, store, "s-malformed-poll", "chat-malformed-poll")
	malformedSession.CodexThreadID = "thread-malformed-poll"
	if err := seedBridge.ensureDurableSession(context.Background(), malformedSession); err != nil {
		t.Fatalf("persist malformed-poll session: %v", err)
	}
	if _, _, err := store.UpdateSessionContext(context.Background(), malformedSession.ID, func(current teamstore.SessionContext, found bool, updatedAt time.Time) (teamstore.SessionContext, bool, error) {
		if !found {
			return current, false, errors.New("malformed-poll session was not persisted")
		}
		current.CodexThreadID = malformedSession.CodexThreadID
		current.UpdatedAt = updatedAt
		return current, true, nil
	}); err != nil {
		t.Fatalf("persist malformed-poll session context: %v", err)
	}
	listenerRecoverySeedDuePoll(t, store, seedBridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, session.ChatID, now)
	listenerRecoverySeedDuePoll(t, store, malformedSession.ChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-malformed-poll", now)

	path := store.Path()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy store before corruption: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatalf("decode legacy store before corruption: %v", err)
	}
	var polls map[string]json.RawMessage
	if err := json.Unmarshal(root["chat_polls"], &polls); err != nil {
		t.Fatalf("decode legacy chat polls before corruption: %v", err)
	}
	malformedRaw := json.RawMessage(`{"chat_id":"chat-malformed-poll","state":123,"opaque_marker":"listener-local"}`)
	polls["chat-malformed-poll"] = malformedRaw
	root["chat_polls"], err = json.Marshal(polls)
	if err != nil {
		t.Fatalf("encode malformed legacy chat polls: %v", err)
	}
	corrupted, err := json.Marshal(root)
	if err != nil {
		t.Fatalf("encode malformed legacy store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store before malformed poll reopen: %v", err)
	}
	if err := os.WriteFile(path, corrupted, 0o600); err != nil {
		t.Fatalf("write malformed legacy store: %v", err)
	}
	reopened, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("reopen malformed legacy store: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	executor := &listenerRecoveryPerPromptExecutor{called: make(chan string)}
	bridge := newBridgeTestBridge(graph, reopened, executor)
	options := listenerRecoveryBaseOptions(reopened, filepath.Join(t.TempDir(), "registry.json"), executor)
	options.PhaseBudget = 5 * time.Second
	options.PollWorkerBudget = 500 * time.Millisecond
	listener := startListenerRecovery(t, bridge, options)
	select {
	case <-executor.called:
	case <-time.After(3 * listenerRecoveryProgressTimeout):
		listener.stop(t)
		state, _ := reopened.Load(context.Background())
		t.Fatalf("healthy chat was not dispatched after malformed poll reopen; gets=%d errors=%v state=%#v", graphState.getCount(session.ChatID), graphState.errorsSnapshot(), state)
	}
	if !waitListenerRecoveryResult(func() bool {
		calls := executor.callsSnapshot()
		return len(calls) == 2 &&
			strings.Contains(strings.Join(calls, "\n"), "LISTENER_RECOVERY_MALFORMED_POLL_HEALTHY_PROMPT") &&
			strings.Contains(strings.Join(calls, "\n"), "LISTENER_RECOVERY_MALFORMED_POLL_PROMPT")
	}, 3*time.Second) {
		state, _ := reopened.Load(context.Background())
		listener.stop(t)
		t.Fatalf("both healthy and malformed-poll chats did not reach execution; calls=%#v polls=%#v sessions=%#v phase=%#v", executor.callsSnapshot(), state.ChatPolls, state.Sessions, bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	waitListenerRecovery(t, func() bool {
		healthyFinals, malformedFinals := 0, 0
		for _, sent := range graphState.sentSnapshot() {
			plain := PlainTextFromTeamsHTML(sent.Body)
			healthyFinals += strings.Count(plain, "LISTENER_RECOVERY_MALFORMED_POLL_HEALTHY_FINAL")
			malformedFinals += strings.Count(plain, "LISTENER_RECOVERY_MALFORMED_POLL_FINAL")
		}
		return healthyFinals == 1 && malformedFinals == 1
	}, 3*time.Second, "healthy and malformed-poll finals after reopen")
	waitListenerRecovery(t, func() bool {
		state, err := reopened.Load(context.Background())
		if err != nil {
			return false
		}
		poll := state.ChatPolls[malformedSession.ChatID]
		return !poll.RecoveryRequired && poll.RecoverySourceHash == "" && poll.PendingPage == nil && poll.Attempt == nil
	}, 3*time.Second, "malformed-poll recovery marker retirement")
	state, err := reopened.Load(context.Background())
	if err != nil {
		listener.stop(t)
		t.Fatalf("load state after malformed-poll recovery: %v", err)
	}
	if poll := state.ChatPolls[malformedSession.ChatID]; poll.RecoveryRequired || poll.RecoverySourceHash != "" || poll.PendingPage != nil || poll.Attempt != nil {
		listener.stop(t)
		raw, _ := os.ReadFile(reopened.Path())
		t.Fatalf("malformed-poll recovery marker was not retired after successful poll: %#v raw=%s", poll, raw)
	}
	if errs := graphState.errorsSnapshot(); len(errs) > 0 {
		listener.stop(t)
		t.Fatalf("malformed poll poisoned fake Graph listener path: %v", errs)
	}
	listener.stop(t)
}

// TestTeamsListenFalseMalformedActiveSQLitePollDoesNotBaseline exercises the
// materialized SQLite admission path.  A malformed operational row belonging
// to an active session must be admitted as a recovery placeholder; otherwise
// the SQL query drops it and the bridge treats the first visible Graph page as
// historical baseline, silently consuming the user's message.
func TestTeamsListenFalseMalformedActiveSQLitePollDoesNotBaseline(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Minute)
	message := bridgeTestMessageWithText("sqlite-malformed-poll-message", "LISTENER_RECOVERY_SQLITE_MALFORMED_POLL_PROMPT")
	message.ChatID = "chat-sqlite-malformed-poll"
	message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		message.ChatID: {message},
	}, 0)
	store := newBridgeTestStore(t)
	seedBridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := appendBridgeTestSession(t, seedBridge, store, "s-sqlite-malformed-poll", message.ChatID)
	session.CodexThreadID = "thread-sqlite-malformed-poll"
	if err := seedBridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("persist SQLite malformed-poll session: %v", err)
	}
	if _, _, err := store.UpdateSessionContext(ctx, session.ID, func(current teamstore.SessionContext, found bool, updatedAt time.Time) (teamstore.SessionContext, bool, error) {
		if !found {
			return current, false, errors.New("SQLite malformed-poll session was not persisted")
		}
		current.CodexThreadID = session.CodexThreadID
		current.UpdatedAt = updatedAt
		return current, true, nil
	}); err != nil {
		t.Fatalf("persist SQLite malformed-poll session context: %v", err)
	}
	listenerRecoverySeedDuePoll(t, store, seedBridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, message.ChatID, now)
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate malformed-poll fixture to SQLite: %v", err)
	}
	path := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close SQLite fixture before corruption: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(filepath.Dir(path), teamstore.SQLiteFileName))
	if err != nil {
		t.Fatalf("open SQLite fixture for corruption: %v", err)
	}
	_, err = db.ExecContext(ctx, `UPDATE chat_polls SET json = ? WHERE chat_id = ?`, []byte(`{"chat_id":"chat-sqlite-malformed-poll","state":123}`), message.ChatID)
	closeErr := db.Close()
	if err != nil {
		t.Fatalf("corrupt SQLite chat poll: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("close SQLite fixture after corruption: %v", closeErr)
	}
	reopened, err := teamstore.Open(path)
	if err != nil {
		t.Fatalf("reopen SQLite malformed-poll fixture: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	executor := &listenerRecoveryPerPromptExecutor{called: make(chan string)}
	bridge := newBridgeTestBridge(graph, reopened, executor)
	options := listenerRecoveryBaseOptions(reopened, filepath.Join(t.TempDir(), "registry.json"), executor)
	options.PhaseBudget = 2 * time.Second
	options.PollWorkerBudget = 500 * time.Millisecond
	listener := startListenerRecovery(t, bridge, options)
	if !waitListenerRecoveryResult(func() bool {
		calls := executor.callsSnapshot()
		return len(calls) == 1 && strings.Contains(calls[0], "LISTENER_RECOVERY_SQLITE_MALFORMED_POLL_PROMPT")
	}, 3*time.Second) {
		state, _ := reopened.Load(ctx)
		listener.stop(t)
		t.Fatalf("SQLite malformed-poll chat did not reach execution; calls=%#v polls=%#v phase=%#v", executor.callsSnapshot(), state.ChatPolls, bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	waitListenerRecovery(t, func() bool {
		for _, sent := range graphState.sentSnapshot() {
			if strings.Contains(PlainTextFromTeamsHTML(sent.Body), "LISTENER_RECOVERY_MALFORMED_POLL_FINAL") {
				return true
			}
		}
		return false
	}, 3*time.Second, "SQLite malformed-poll final")
	waitListenerRecovery(t, func() bool {
		state, err := reopened.Load(ctx)
		if err != nil {
			return false
		}
		poll := state.ChatPolls[message.ChatID]
		return !poll.RecoveryRequired && poll.RecoverySourceHash == "" && poll.PendingPage == nil && poll.Attempt == nil
	}, 3*time.Second, "SQLite malformed-poll recovery marker retirement")
	state, err := reopened.Load(ctx)
	if err != nil {
		listener.stop(t)
		t.Fatalf("load SQLite state after malformed-poll recovery: %v", err)
	}
	if poll := state.ChatPolls[message.ChatID]; poll.RecoveryRequired || poll.RecoverySourceHash != "" || poll.PendingPage != nil || poll.Attempt != nil {
		listener.stop(t)
		t.Fatalf("SQLite malformed-poll recovery marker was not retired: %#v", poll)
	}
	if errs := graphState.errorsSnapshot(); len(errs) > 0 {
		listener.stop(t)
		t.Fatalf("SQLite malformed-poll poisoned fake Graph listener path: %v", errs)
	}
	listener.stop(t)
}

// TestPollMissingFrontierEstablishedSessionDoesNotBaseline covers the other
// half of the row-local recovery boundary: a partial migration or repair may
// remove a valid chat_polls row without corrupting the session row. An
// established session must not treat the next Graph head as historical
// baseline, because that would silently consume a new user message. The
// durable recovery placeholder is retired by the successful poll and the
// one-time session hint is recorded for a later missing-row recovery.
func TestPollMissingFrontierEstablishedSessionDoesNotBaseline(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now().UTC().Add(-time.Minute)
			chatID := "chat-missing-frontier-" + name
			message := bridgeTestMessageWithText("missing-frontier-message-"+name, "LISTENER_RECOVERY_MISSING_FRONTIER_PROMPT_"+name)
			message.ChatID = chatID
			message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
			message.LastModifiedDateTime = message.CreatedDateTime
			graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{chatID: {message}}, 0)
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			session := appendBridgeTestSession(t, bridge, store, "s-missing-frontier-"+name, chatID)
			session.CodexThreadID = "thread-missing-frontier-" + name
			if _, _, err := store.UpdateSessionContext(ctx, session.ID, func(current teamstore.SessionContext, found bool, updatedAt time.Time) (teamstore.SessionContext, bool, error) {
				if !found {
					return current, false, errors.New("missing-frontier session was not persisted")
				}
				current.CodexThreadID = session.CodexThreadID
				current.UpdatedAt = updatedAt
				return current, true, nil
			}); err != nil {
				t.Fatalf("persist established session: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate missing-frontier fixture: %v", err)
				}
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				delete(state.ChatPolls, chatID)
				return nil
			}); err != nil {
				t.Fatalf("remove chat poll frontier: %v", err)
			}

			var handled []string
			if _, err := bridge.pollChat(ctx, chatID, 20, func(_ context.Context, _ ChatMessage, text string) error {
				handled = append(handled, text)
				return nil
			}); err != nil {
				t.Fatalf("poll established session with missing frontier: %v", err)
			}
			if len(handled) != 1 || handled[0] != "LISTENER_RECOVERY_MISSING_FRONTIER_PROMPT_"+name {
				t.Fatalf("handled messages = %#v, want the live Graph prompt", handled)
			}
			poll, found, err := store.ChatPoll(ctx, chatID)
			if err != nil || !found {
				t.Fatalf("recovered ChatPoll = %#v found=%v err=%v", poll, found, err)
			}
			if !poll.Seeded || poll.RecoveryRequired || poll.PendingPage != nil || poll.Attempt != nil {
				t.Fatalf("recovered ChatPoll retained transient recovery state: %#v", poll)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load state after missing-frontier recovery: %v", err)
			}
			if !state.Sessions[session.ID].PollFrontierInitialized {
				t.Fatalf("session poll frontier hint was not persisted: %#v", state.Sessions[session.ID])
			}
			if got := graphState.getCount(chatID); got != 1 {
				t.Fatalf("Graph reads = %d, want one bounded recovery read", got)
			}
		})
	}
}

// TestTeamsListenFalseCurrentStateReplayMatrix starts with durable state that a
// restarted helper could genuinely inherit: one ordinary backlog, one
// task_started/prompt race whose prompt arrives after startup, and one legacy
// checkpoint without source proof.  The listener must drain the safe rows,
// release the race after its durable Teams proof is visible, and isolate the
// legacy row without turning it into a chat-wide blocker or a manual-recovery
// notice.  No state is inserted after Listen starts; only the transcript writer
// appends the delayed prompt/final.
func TestTeamsListenFalseCurrentStateReplayMatrix(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate store to SQLite: %v", err)
				}
			}
			graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			root := newBridgeTestCodexRoot(t)
			bridge.scope.CodexHome = root

			type fixture struct {
				session *Session
				path    string
				final   string
			}
			addFixture := func(t *testing.T, sessionID string, chatID string, initial string, final string) fixture {
				t.Helper()
				var session *Session
				if sessionID == "s001" {
					session = bridge.reg.SessionByID(sessionID)
					if session == nil {
						t.Fatalf("missing initial session %s", sessionID)
					}
				} else {
					session = appendBridgeTestSession(t, bridge, store, sessionID, chatID)
				}
				session.CodexThreadID = "thread-current-" + sessionID
				session.Cwd = root
				if err := bridge.ensureDurableSession(ctx, session); err != nil {
					t.Fatalf("persist %s session: %v", sessionID, err)
				}
				if _, _, err := store.UpdateSessionContext(ctx, sessionID, func(current teamstore.SessionContext, found bool, now time.Time) (teamstore.SessionContext, bool, error) {
					if !found {
						return current, false, fmt.Errorf("session %s was not persisted", sessionID)
					}
					current.CodexThreadID = session.CodexThreadID
					current.Cwd = session.Cwd
					current.UpdatedAt = now
					return current, true, nil
				}); err != nil {
					t.Fatalf("persist %s session context: %v", sessionID, err)
				}
				path := filepath.Join(root, sessionID+".jsonl")
				if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
					t.Fatalf("write %s transcript: %v", sessionID, err)
				}
				listenerRecoverySeedLinkedCheckpoint(t, store, session, path, true)
				return fixture{session: session, path: path, final: final}
			}

			healthyBaseline := listenerRecoveryTranscriptLine("current-healthy-initial", "current healthy baseline")
			healthyFinal := "LISTENER_RECOVERY_CURRENT_HEALTHY_FINAL"
			healthy := addFixture(t, "s001", "chat-1", healthyBaseline+listenerRecoveryTranscriptFinalLine("current-healthy-final", healthyFinal), healthyFinal)
			healthyCheckpoint, found, err := store.ImportCheckpoint(ctx, transcriptCheckpointID(healthy.session.ID))
			if err != nil || !found {
				t.Fatalf("load inherited healthy checkpoint: found=%t err=%v", found, err)
			}
			healthyInfo, err := os.Stat(healthy.path)
			if err != nil {
				t.Fatalf("stat inherited healthy transcript: %v", err)
			}
			healthyCheckpoint.LastRecordID = "current-healthy-initial"
			healthyCheckpoint.LastSourceLine = 1
			healthyCheckpoint.LastOffset = int64(len([]byte(healthyBaseline)))
			healthyCheckpoint.SourceSize = healthyCheckpoint.LastOffset
			healthyCheckpoint.SourceModTime = healthyInfo.ModTime()
			healthyCheckpoint.SourceFingerprint = transcriptCheckpointSourceFingerprint(healthy.path, healthyCheckpoint.LastOffset)
			healthyCheckpoint.LastFinalID = ""
			healthyCheckpoint.LastFinalLine = 0
			healthyCheckpoint.LastFinalStartOffset = 0
			healthyCheckpoint.LastFinalStartOffsetKnown = false
			healthyCheckpoint.LastFinalThreadID = ""
			healthyCheckpoint.LastFinalTurnID = ""
			healthyCheckpoint.LastFinalTextHash = ""
			healthyCheckpoint.TerminalBoundarySeen = false
			healthyCheckpoint.TerminalBoundaryLine = 0
			healthyCheckpoint.TerminalBoundary = nil
			if _, _, err := store.UpdateImportCheckpoint(ctx, healthyCheckpoint.ID, func(teamstore.ImportCheckpoint, bool, time.Time) (teamstore.ImportCheckpoint, bool, error) {
				return healthyCheckpoint, true, nil
			}); err != nil {
				t.Fatalf("seed inherited healthy baseline checkpoint: %v", err)
			}

			raceBaseline := listenerRecoveryTranscriptLine("current-race-initial", "current race baseline") + listenerRecoveryTranscriptFinalLine("current-race-previous-final", "current race previous final")
			raceInitial := raceBaseline + listenerRecoveryTranscriptTaskStartedLine("current-race-turn")
			raceFinal := "LISTENER_RECOVERY_CURRENT_RACE_FINAL"
			race := addFixture(t, "s002", "chat-2", raceInitial, raceFinal)
			// The inherited checkpoint must end at the durable baseline, not at
			// the task_started marker.  This is the restart shape in which the
			// listener sees the marker as the first unprocessed record and has to
			// retain it until the already-owned Teams turn proves the root.
			baselineSize := int64(len([]byte(raceBaseline)))
			checkpoint, found, err := store.ImportCheckpoint(ctx, transcriptCheckpointID(race.session.ID))
			if err != nil || !found {
				t.Fatalf("load inherited race checkpoint: found=%t err=%v", found, err)
			}
			info, err := os.Stat(race.path)
			if err != nil {
				t.Fatalf("stat inherited race transcript: %v", err)
			}
			checkpoint.LastRecordID = "current-race-previous-final"
			checkpoint.LastSourceLine = 2
			checkpoint.LastOffset = baselineSize
			checkpoint.SourceSize = baselineSize
			checkpoint.SourceModTime = info.ModTime()
			checkpoint.SourceFingerprint = transcriptCheckpointSourceFingerprint(race.path, baselineSize)
			checkpoint.LastFinalID = "current-race-previous-final"
			checkpoint.LastFinalLine = 2
			checkpoint.LastFinalStartOffset = int64(len([]byte(listenerRecoveryTranscriptLine("current-race-initial", "current race baseline"))))
			checkpoint.LastFinalStartOffsetKnown = true
			checkpoint.LastFinalThreadID = ""
			checkpoint.LastFinalTurnID = ""
			checkpoint.LastFinalTextHash = ""
			checkpoint.TerminalBoundarySeen = true
			checkpoint.TerminalBoundaryLine = 2
			checkpoint.TerminalBoundary = nil
			if _, _, err := store.UpdateImportCheckpoint(ctx, checkpoint.ID, func(teamstore.ImportCheckpoint, bool, time.Time) (teamstore.ImportCheckpoint, bool, error) {
				return checkpoint, true, nil
			}); err != nil {
				t.Fatalf("seed inherited race baseline checkpoint: %v", err)
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				inboundID := "inbound:current-race-proof"
				state.InboundEvents[inboundID] = teamstore.InboundEvent{
					ID:             inboundID,
					SessionID:      race.session.ID,
					TeamsChatID:    race.session.ChatID,
					TeamsMessageID: "teams-current-race-proof",
					Source:         "teams",
					Status:         teamstore.InboundStatusPersisted,
					TurnID:         "turn:current-race-proof",
					CreatedAt:      time.Now().UTC(),
					UpdatedAt:      time.Now().UTC(),
				}
				state.Turns["turn:current-race-proof"] = teamstore.Turn{
					ID:             "turn:current-race-proof",
					SessionID:      race.session.ID,
					InboundEventID: inboundID,
					Status:         teamstore.TurnStatusCompleted,
					CodexThreadID:  race.session.CodexThreadID,
					CodexTurnID:    "current-race-turn",
					CreatedAt:      time.Now().UTC(),
					UpdatedAt:      time.Now().UTC(),
				}
				return nil
			}); err != nil {
				t.Fatalf("seed current race Teams proof: %v", err)
			}

			legacyInitial := listenerRecoveryTranscriptLine("current-legacy-initial", "current legacy baseline")
			legacyFinal := "LISTENER_RECOVERY_CURRENT_LEGACY_FINAL_MUST_WAIT"
			legacy := addFixture(t, "s003", "chat-3", legacyInitial, legacyFinal)
			legacyCheckpoint, found, err := store.ImportCheckpoint(ctx, transcriptCheckpointID(legacy.session.ID))
			if err != nil || !found {
				t.Fatalf("load legacy checkpoint: found=%t err=%v", found, err)
			}
			legacyCheckpoint.SourceFingerprint = ""
			legacyCheckpoint.SourceGeneration = ""
			legacyCheckpoint.LegacySourceUnverified = false
			if _, _, err := store.UpdateImportCheckpoint(ctx, legacyCheckpoint.ID, func(teamstore.ImportCheckpoint, bool, time.Time) (teamstore.ImportCheckpoint, bool, error) {
				return legacyCheckpoint, true, nil
			}); err != nil {
				t.Fatalf("seed legacy checkpoint: %v", err)
			}
			if err := appendListenerRecoveryTranscript(legacy.path, listenerRecoveryTranscriptFinalLine("current-legacy-final", legacyFinal)); err != nil {
				t.Fatalf("append legacy final: %v", err)
			}
			var linkedHookMu sync.Mutex
			var linkedHookCalls []string
			bridge.linkedTranscriptSessionHook = func(_ context.Context, session Session) error {
				linkedHookMu.Lock()
				linkedHookCalls = append(linkedHookCalls, session.ID)
				linkedHookMu.Unlock()
				return nil
			}
			now := time.Now().UTC().Add(-time.Minute)
			listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
			bridge.lastTranscriptSync = time.Time{}
			bridge.lastHistoryWatchSync = time.Now()
			bridge.lastHistoryWatchReconcile = time.Now()
			listenerOptions := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor)
			// The matrix exercises restart/current-state semantics, not the phase
			// timeout itself.  Give the race-instrumented store enough room to finish
			// its bounded linked-transcript pass; the progress watchdog below remains
			// the liveness oracle.
			listenerOptions.PhaseBudget = time.Second
			listener := startListenerRecovery(t, bridge, listenerOptions)

			pendingBoundaryReady := waitListenerRecoveryResult(func() bool {
				state, loadErr := store.Load(ctx)
				if loadErr != nil {
					return false
				}
				checkpoint := state.ImportCheckpoints[transcriptCheckpointID(race.session.ID)]
				return checkpoint.PendingHistoryRange != nil && strings.EqualFold(checkpoint.PendingHistoryRange.Kind, "pending_root_task_started")
			}, listenerRecoveryProgressTimeout)
			if !pendingBoundaryReady {
				state, loadErr := store.Load(ctx)
				if loadErr != nil {
					listener.stop(t)
					t.Fatalf("load inherited pending-boundary diagnostic: %v", loadErr)
				}
				linkedHookMu.Lock()
				calls := append([]string(nil), linkedHookCalls...)
				linkedHookMu.Unlock()
				ttRegistry := bridge.registrySnapshot()
				t.Logf("inherited pending-boundary diagnostic: checkpoint=%#v turns=%#v history=%#v registry=%#v linked-hook-calls=%v phases: outbox=%#v poll=%#v linked=%#v history-watch=%#v", state.ImportCheckpoints[transcriptCheckpointID(race.session.ID)], state.Turns, state.HistoryWatch, ttRegistry.Sessions, calls, bridge.mainLoopPhaseStatsSnapshot("outbox"), bridge.mainLoopPhaseStatsSnapshot("poll"), bridge.mainLoopPhaseStatsSnapshot("linked-transcript"), bridge.mainLoopPhaseStatsSnapshot("history-watch"))
				listener.stop(t)
				t.Fatalf("timed out waiting for current inherited pending task boundary")
			}

			if err := appendListenerRecoveryTranscript(race.path,
				listenerRecoveryTranscriptPromptWithTurnLine("current-race-prompt", "current-race-turn", "current Teams prompt"),
				listenerRecoveryTranscriptFinalWithTurnLine("current-race-final", "current-race-turn", raceFinal),
			); err != nil {
				listener.stop(t)
				t.Fatalf("append current race prompt/final: %v", err)
			}

			safeBacklogReady := waitListenerRecoveryResult(func() bool {
				state, loadErr := store.Load(ctx)
				if loadErr != nil {
					return false
				}
				for _, item := range []fixture{healthy, race} {
					checkpoint := state.ImportCheckpoints[transcriptCheckpointID(item.session.ID)]
					if checkpoint.LastOffset <= 0 || checkpoint.PendingHistoryRange != nil || checkpoint.UnresolvedExecution != nil || checkpoint.Status == importCheckpointStatusBlocked {
						return false
					}
				}
				plain := sentPlainJoinedListenerRecovery(graphState.sentSnapshot())
				return strings.Contains(plain, healthyFinal) && strings.Contains(plain, raceFinal)
			}, listenerRecoveryProgressTimeout)
			if !safeBacklogReady {
				state, loadErr := store.Load(ctx)
				if loadErr != nil {
					listener.stop(t)
					t.Fatalf("load safe-backlog diagnostic: %v", loadErr)
				}
				raceCheckpoint, found, checkpointErr := store.ImportCheckpoint(ctx, transcriptCheckpointID(race.session.ID))
				if checkpointErr != nil || !found {
					listener.stop(t)
					t.Fatalf("load race checkpoint after prompt diagnostic: found=%t err=%v", found, checkpointErr)
				}
				directTranscript, directErr := bridge.readLinkedTranscriptDelta(race.path, raceCheckpoint, race.session.CodexThreadID, race.session.CodexThreadID)
				teamsProof := linkedTranscriptRootReleaseTeamsProof(ctx, store, *race.session, directTranscript.RootReleaseWitness)
				witnessSourceMatch := transcriptRootReleaseWitnessMatchesSource(race.path, raceCheckpoint, directTranscript.RootReleaseWitness)
				linkedHookMu.Lock()
				calls := append([]string(nil), linkedHookCalls...)
				linkedHookMu.Unlock()
				t.Logf("safe-backlog diagnostic: checkpoint=%#v direct=(pending:%t unresolved:%t records:%d consumed:%#v witness:%#v err:%v teams-proof:%t source-match:%t) registry=%#v linked-hook-calls=%v sent=%#v phases: linked=%#v outbox=%#v", state.ImportCheckpoints[transcriptCheckpointID(race.session.ID)], directTranscript.PendingContinuation, directTranscript.UnresolvedContinuation, len(directTranscript.Records), directTranscript.Consumed, directTranscript.RootReleaseWitness, directErr, teamsProof, witnessSourceMatch, bridge.registrySnapshot().Sessions, calls, graphState.sentSnapshot(), bridge.mainLoopPhaseStatsSnapshot("linked-transcript"), bridge.mainLoopPhaseStatsSnapshot("outbox"))
				listener.stop(t)
				t.Fatalf("timed out waiting for safe current-state backlog finals")
			}

			plain := sentPlainJoinedListenerRecovery(graphState.sentSnapshot())
			if strings.Contains(plain, legacyFinal) || strings.Contains(plain, "publish-history") || strings.Contains(plain, "previous Codex execution is still unconfirmed") {
				listener.stop(t)
				t.Fatalf("current-state replay leaked legacy/manual recovery output: %s", plain)
			}
			state, err := store.Load(ctx)
			if err != nil {
				listener.stop(t)
				t.Fatalf("load final current-state replay: %v", err)
			}
			legacyAfter := state.ImportCheckpoints[transcriptCheckpointID(legacy.session.ID)]
			if !legacyAfter.LegacySourceUnverified || legacyAfter.Status == importCheckpointStatusBlocked {
				listener.stop(t)
				t.Fatalf("legacy checkpoint after replay = %#v, want silent history-only boundary", legacyAfter)
			}
			listener.stop(t)
			if countListenerRecoveryTranscriptFinals(graphState.sentSnapshot(), healthyFinal) != 1 || countListenerRecoveryTranscriptFinals(graphState.sentSnapshot(), raceFinal) != 1 {
				t.Fatalf("safe current-state final counts = healthy:%d race:%d, want one each; sent=%#v", countListenerRecoveryTranscriptFinals(graphState.sentSnapshot(), healthyFinal), countListenerRecoveryTranscriptFinals(graphState.sentSnapshot(), raceFinal), graphState.sentSnapshot())
			}
		})
	}
}

func listenerRecoveryTranscriptLine(id string, text string) string {
	return fmt.Sprintf(`{"type":"event_msg","payload":{"id":%q,"type":"agent_message","phase":"commentary","message":%q}}`+"\n", id, text)
}

func listenerRecoveryTranscriptFinalLine(id string, text string) string {
	return fmt.Sprintf(`{"type":"event_msg","payload":{"id":%q,"type":"agent_message","phase":"final_answer","message":%q}}`+"\n", id, text)
}

func listenerRecoveryTranscriptFinalWithTurnLine(id string, turnID string, text string) string {
	return fmt.Sprintf(`{"type":"event_msg","payload":{"id":%q,"type":"agent_message","turn_id":%q,"phase":"final_answer","message":%q}}`+"\n", id, turnID, text)
}

func listenerRecoveryTranscriptTaskStartedLine(turnID string) string {
	return fmt.Sprintf(`{"type":"event_msg","payload":{"type":"task_started","turn_id":%q,"started_at":1786181089,"model_context_window":128000,"collaboration_mode_kind":"default"}}`+"\n", turnID)
}

func listenerRecoveryTranscriptPromptLine(id string, text string) string {
	return fmt.Sprintf(`{"type":"response_item","payload":{"id":%q,"type":"message","role":"user","content":[{"type":"input_text","text":%q}]}}`+"\n", id, text)
}

func listenerRecoveryTranscriptPromptWithTurnLine(id string, turnID string, text string) string {
	return fmt.Sprintf(`{"type":"response_item","payload":{"id":%q,"type":"message","role":"user","turn_id":%q,"content":[{"type":"input_text","text":%q}]}}`+"\n", id, turnID, text)
}

func sentPlainJoinedListenerRecovery(items []listenerRecoverySentMessage) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, PlainTextFromTeamsHTML(item.Body))
	}
	return strings.Join(parts, "\n")
}

func countListenerRecoveryTranscriptFinals(items []listenerRecoverySentMessage, marker string) int {
	count := 0
	for _, item := range items {
		if strings.Contains(PlainTextFromTeamsHTML(item.Body), marker) {
			count++
		}
	}
	return count
}

func countListenerRecoverySentBodies(items []listenerRecoverySentMessage, marker string) int {
	count := 0
	for _, item := range items {
		if strings.Contains(PlainTextFromTeamsHTML(item.Body), marker) {
			count++
		}
	}
	return count
}

func listenerRecoverySeedLinkedCheckpoint(t *testing.T, store *teamstore.Store, session *Session, path string, released bool) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat linked checkpoint source %s: %v", path, err)
	}
	offset := info.Size()
	checkpoint := teamstore.ImportCheckpoint{
		ID:                  transcriptCheckpointID(session.ID),
		SessionID:           session.ID,
		SourcePath:          path,
		SourceGeneration:    historyTieredSourceIdentity(path, info),
		SourceFingerprint:   transcriptCheckpointSourceFingerprint(path, offset),
		LastRecordID:        "initial-" + session.ID,
		LastSourceLine:      1,
		LastOffset:          offset,
		LastOffsetKnown:     true,
		SourceSize:          offset,
		SourceModTime:       info.ModTime(),
		Status:              importCheckpointStatusComplete,
		HistoryRootReleased: released,
	}
	if checkpoint.SourceGeneration == "" || checkpoint.SourceFingerprint == "" {
		t.Fatalf("linked checkpoint source proof is empty: %#v", checkpoint)
	}
	if _, _, err := store.UpdateImportCheckpoint(context.Background(), checkpoint.ID, func(teamstore.ImportCheckpoint, bool, time.Time) (teamstore.ImportCheckpoint, bool, error) {
		return checkpoint, true, nil
	}); err != nil {
		t.Fatalf("seed linked checkpoint %s: %v", session.ID, err)
	}
}

func listenerRecoveryHistoryCheckpoint(path string, sessionID string, threadID string, info os.FileInfo) teamstore.HistoryWatchCheckpoint {
	offset := info.Size()
	return teamstore.HistoryWatchCheckpoint{
		ID:                historyWatchCheckpointID(path),
		Path:              path,
		Size:              offset,
		ModTime:           info.ModTime(),
		SourceGeneration:  historyTieredSourceIdentity(path, info),
		SourceFingerprint: transcriptCheckpointSourceFingerprint(path, offset),
		Offset:            offset,
		Line:              1,
		SessionID:         sessionID,
		ThreadID:          threadID,
	}
}

// TestTeamsListenFalseBacklogProgressSurvivesStoreReopen proves that the
// listener's durable outbox frontier, rather than an in-memory cycle counter,
// determines where recovery resumes.  The first owner is stopped mid-backlog;
// a new owner must send only the remaining rows and eventually send all rows.
func TestTeamsListenFalseBacklogProgressSurvivesStoreReopen(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			runListenerRecoveryBacklogProgressSurvivesReopen(t, useSQLite)
		})
	}
}

func runListenerRecoveryBacklogProgressSurvivesReopen(t *testing.T, useSQLite bool) {
	t.Helper()
	storePath := filepath.Join(t.TempDir(), "state.json")
	store, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	now := time.Now().UTC()
	// Keep POSTs immediate in this restart/frontier test.  A deliberately slow
	// POST belongs in the ambiguous-result test; combining it with the race
	// detector would turn a successful response into a real unknown-side-effect
	// boundary and make this test assert the wrong recovery contract.
	graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
	registryPath := filepath.Join(t.TempDir(), "registry.json")
	seedBridge := newBridgeTestBridge(graph, store, &listenerRecoveryExecutor{called: make(chan string)})
	for i := 0; i < 6; i++ {
		if _, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
			ID:          fmt.Sprintf("outbox:listener-reopen:%02d", i),
			TeamsChatID: "chat-1",
			Kind:        "helper",
			Body:        fmt.Sprintf("LISTENER_REOPEN_MESSAGE_%02d", i),
		}); err != nil {
			t.Fatalf("queue outbox %d: %v", i, err)
		}
	}
	if useSQLite {
		if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
			t.Fatalf("migrate listener reopen store to SQLite: %v", err)
		}
	}
	listenerRecoverySeedDuePoll(t, store, seedBridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)

	firstOptions := listenerRecoveryBaseOptions(store, registryPath, seedBridge.executor)
	// Keep the first owner in its first cycle after the bounded outbox batch.
	// The pre-send hook deterministically pauses the second send, so the
	// restart boundary contains a queued row rather than an unknown side effect.
	firstOptions.Interval = time.Hour
	firstOptions.PhaseBudget = 3 * time.Second
	secondSendStarted := make(chan struct{})
	var secondSendOnce sync.Once
	seedBridge.outboxSendHook = func(ctx context.Context, msg teamstore.OutboxMessage) error {
		if msg.ID != "outbox:listener-reopen:01" {
			return nil
		}
		secondSendOnce.Do(func() { close(secondSendStarted) })
		<-ctx.Done()
		return ctx.Err()
	}
	firstListener := startListenerRecovery(t, seedBridge, firstOptions)
	select {
	case <-secondSendStarted:
	case <-time.After(listenerRecoveryProgressTimeout):
		firstListener.stop(t)
		t.Fatalf("timed out waiting for deterministic pre-send restart boundary")
	}
	firstState, firstLoadErr := store.Load(context.Background())
	if firstLoadErr != nil || countListenerRecoverySentOutbox(firstState) < 1 {
		firstListener.stop(t)
		t.Fatalf("timed out waiting for first durable outbox rows: load=%v sent=%d graph=%#v phase-outbox=%#v phase-poll=%#v phase-linked=%#v listenerErr=%v state=%#v", firstLoadErr, countListenerRecoverySentOutbox(firstState), graphState.sentSnapshot(), seedBridge.mainLoopPhaseStatsSnapshot("outbox"), seedBridge.mainLoopPhaseStatsSnapshot("poll"), seedBridge.mainLoopPhaseStatsSnapshot("linked-transcript"), firstListener.err, firstState)
	}
	firstListener.stop(t)
	firstState, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state after first listener: %v", err)
	}
	firstSent := countListenerRecoverySentOutbox(firstState)
	if firstSent != 1 {
		t.Fatalf("first listener sent %d rows, want a partial durable backlog", firstSent)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	recoveredStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	recoveredExecutor := &listenerRecoveryExecutor{called: make(chan string)}
	recoveredBridge := newBridgeTestBridge(graph, recoveredStore, recoveredExecutor)
	t.Cleanup(func() { _ = recoveredStore.Close() })
	recoveredOptions := listenerRecoveryBaseOptions(recoveredStore, registryPath, recoveredExecutor)
	// The recovery assertion is about durable outbox progress, not the short
	// maintenance deadline used by most phase-isolation tests.  Under -race,
	// SQLite owner-CAS and the first outbox page can legitimately exceed the
	// 500ms fixture budget; using a still-bounded five-second phase budget keeps
	// this test aligned with the production budget without turning a wedged
	// listener into an unbounded wait.
	recoveredOptions.PhaseBudget = 5 * time.Second
	secondListener := startListenerRecovery(t, recoveredBridge, recoveredOptions)
	recoveredDeadline := time.Now().Add(listenerRecoveryProgressTimeout)
	recovered := false
	for time.Now().Before(recoveredDeadline) {
		state, loadErr := recoveredStore.Load(context.Background())
		if loadErr == nil && countListenerRecoverySentOutbox(state) == 6 {
			recovered = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !recovered {
		state, loadErr := recoveredStore.Load(context.Background())
		t.Fatalf("timed out waiting for reopened outbox backlog: load=%v sent=%d graph=%#v phase-outbox=%#v phase-poll=%#v listenerErr=%v state=%#v", loadErr, countListenerRecoverySentOutbox(state), graphState.sentSnapshot(), recoveredBridge.mainLoopPhaseStatsSnapshot("outbox"), recoveredBridge.mainLoopPhaseStatsSnapshot("poll"), secondListener.err, state)
	}
	secondListener.stop(t)
	finalState, err := recoveredStore.Load(context.Background())
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	if got := countListenerRecoverySentOutbox(finalState); got != 6 {
		t.Fatalf("final sent outbox count = %d, want 6", got)
	}
	sent := graphState.sentSnapshot()
	counts := make(map[string]int)
	for _, item := range sent {
		for i := 0; i < 6; i++ {
			body := fmt.Sprintf("LISTENER_REOPEN_MESSAGE_%02d", i)
			if strings.Contains(PlainTextFromTeamsHTML(item.Body), body) {
				counts[body]++
			}
		}
	}
	for i := 0; i < 6; i++ {
		body := fmt.Sprintf("LISTENER_REOPEN_MESSAGE_%02d", i)
		if counts[body] != 1 {
			t.Fatalf("outbox body %q sent %d times, want exactly once; sent=%#v", body, counts[body], sent)
		}
	}
}

// TestTeamsListenFalsePolledTurnOutboxSurvivesReopen covers the production
// path missing from queue-only restart tests: Graph poll admits a user
// message, Codex produces a final, the first listener is interrupted before
// the final POST, and a new owner/store instance resumes that durable outbox.
// The inbound poll cursor must not be replayed as a second Codex turn, while
// the generated final must still be delivered exactly once.
func TestTeamsListenFalsePolledTurnOutboxSurvivesReopen(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			runListenerRecoveryPolledTurnOutboxSurvivesReopen(t, useSQLite)
		})
	}
}

func runListenerRecoveryPolledTurnOutboxSurvivesReopen(t *testing.T, useSQLite bool) {
	t.Helper()
	ctx := context.Background()
	storePath := filepath.Join(t.TempDir(), "state.json")
	store, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	message := bridgeTestMessageWithText("polled-reopen-inbound", "LISTENER_RECOVERY_POLLED_REOPEN_PROMPT")
	message.ChatID = "chat-1"
	message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1": {message},
	}, 0)
	executor := &listenerRecoveryExecutor{
		called: make(chan string),
		result: ExecutionResult{
			Text:          "LISTENER_RECOVERY_POLLED_REOPEN_FINAL",
			CodexThreadID: "thread-polled-reopen",
			CodexTurnID:   "turn-polled-reopen",
		},
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)
	if useSQLite {
		if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
			t.Fatalf("migrate first store to SQLite: %v", err)
		}
	}

	finalSendStarted := make(chan struct{})
	var finalSendOnce sync.Once
	bridge.outboxSendHook = func(hookCtx context.Context, outbox teamstore.OutboxMessage) error {
		if !strings.Contains(outbox.Body, "LISTENER_RECOVERY_POLLED_REOPEN_FINAL") {
			return nil
		}
		finalSendOnce.Do(func() { close(finalSendStarted) })
		<-hookCtx.Done()
		return hookCtx.Err()
	}
	firstOptions := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	firstOptions.Interval = time.Hour
	firstOptions.PhaseBudget = 3 * time.Second
	first := startListenerRecovery(t, bridge, firstOptions)
	select {
	case <-executor.called:
	case <-time.After(listenerRecoveryProgressTimeout):
		first.stop(t)
		t.Fatalf("polled turn did not reach executor; gets=%d errors=%v", graphState.getCount("chat-1"), graphState.errorsSnapshot())
	}
	select {
	case <-finalSendStarted:
	case <-time.After(listenerRecoveryProgressTimeout):
		first.stop(t)
		state, _ := store.Load(ctx)
		t.Fatalf("generated final did not reach pre-send restart boundary: state=%#v calls=%#v phases outbox=%#v poll=%#v", state, executor.callsSnapshot(), bridge.mainLoopPhaseStatsSnapshot("outbox"), bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	state, err := store.Load(ctx)
	if err != nil {
		first.stop(t)
		t.Fatalf("load first state: %v", err)
	}
	generatedID := ""
	for id, outbox := range state.OutboxMessages {
		if strings.Contains(outbox.Body, "LISTENER_RECOVERY_POLLED_REOPEN_FINAL") {
			generatedID = id
			if outbox.Status == teamstore.OutboxStatusSent {
				first.stop(t)
				t.Fatalf("generated final was sent before restart boundary: %#v", outbox)
			}
		}
	}
	if generatedID == "" {
		first.stop(t)
		t.Fatalf("polled turn did not create a durable final outbox: %#v", state.OutboxMessages)
	}
	first.stop(t)
	if err := store.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	recoveredStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	recoveredExecutor := &listenerRecoveryExecutor{called: make(chan string)}
	recoveredBridge := newBridgeTestBridge(graph, recoveredStore, recoveredExecutor)
	t.Cleanup(func() { _ = recoveredStore.Close() })
	recovered := startListenerRecovery(t, recoveredBridge, listenerRecoveryBaseOptions(recoveredStore, filepath.Join(t.TempDir(), "registry.json"), recoveredExecutor))
	waitListenerRecovery(t, func() bool {
		state, err := recoveredStore.Load(ctx)
		if err != nil {
			return false
		}
		outbox, ok := state.OutboxMessages[generatedID]
		return ok && outbox.Status == teamstore.OutboxStatusSent && countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_POLLED_REOPEN_FINAL") == 1
	}, listenerRecoveryProgressTimeout, "polled generated outbox after reopen")
	recovered.stop(t)

	if got := len(executor.callsSnapshot()); got != 1 {
		t.Fatalf("first listener Codex calls = %d, want one", got)
	}
	if got := len(recoveredExecutor.callsSnapshot()); got != 0 {
		t.Fatalf("reopened listener replayed inbound as %d Codex turn(s), want none", got)
	}
	if got := countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_POLLED_REOPEN_FINAL"); got != 1 {
		t.Fatalf("reopened generated final POST count = %d, want exactly one; sent=%#v", got, graphState.sentSnapshot())
	}
}

// TestTeamsListenFalsePollFrontierSurvivesStoreReopenAndOwnerTakeover proves
// that production-generated inbound/outbox state is durable across a store
// reopen and owner takeover. The second generation must not execute or post a
// completed inbound message again.
func TestTeamsListenFalsePollFrontierSurvivesStoreReopenAndOwnerTakeover(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			runListenerRecoveryPollFrontierSurvivesReopen(t, useSQLite)
		})
	}
}

// runListenerRecoveryPollFrontierSurvivesReopen makes the first generation
// create its durable Graph continuation through the production poll path. A
// test that only seeds frontier fields directly cannot catch a regression in
// staging, attempt ownership, or the first generation's durable commit.
func runListenerRecoveryPollFrontierSurvivesReopen(t *testing.T, useSQLite bool) {
	t.Helper()
	ctx := context.Background()
	storePath := filepath.Join(t.TempDir(), "state.json")
	chatID := "chat-reopen-frontier"
	now := time.Now().UTC().Add(-time.Minute)
	firstMessage := bridgeTestMessageWithText("reopen-frontier-message-1", "LISTENER_RECOVERY_REOPEN_FRONTIER_PROMPT_1")
	secondMessage := bridgeTestMessageWithText("reopen-frontier-message-2", "LISTENER_RECOVERY_REOPEN_FRONTIER_PROMPT_2")
	messages := []*ChatMessage{&firstMessage, &secondMessage}
	for i, message := range messages {
		message.ChatID = chatID
		message.CreatedDateTime = now.Add(time.Duration(i+1) * time.Second).Format(time.RFC3339Nano)
		message.LastModifiedDateTime = message.CreatedDateTime
	}
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{chatID: {firstMessage, secondMessage}}, 0)
	graphState.mu.Lock()
	graphState.pageSize = 1
	graphState.mu.Unlock()
	firstStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open first frontier store: %v", err)
	}
	firstExecutor := &listenerRecoveryExecutor{called: make(chan string), result: ExecutionResult{
		Text: "LISTENER_RECOVERY_REOPEN_FRONTIER_FIRST_FINAL", CodexThreadID: "thread-reopen-frontier", CodexTurnID: "turn-reopen-frontier-first",
	}}
	firstBridge := newBridgeTestBridge(graph, firstStore, firstExecutor)
	appendBridgeTestSession(t, firstBridge, firstStore, "s-reopen-frontier", chatID)
	listenerRecoverySeedDuePoll(t, firstStore, firstBridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, firstStore, chatID, now)
	if useSQLite {
		if _, err := firstStore.MigrateLargeStateToSQLite(ctx, 0); err != nil {
			t.Fatalf("migrate first frontier store to SQLite: %v", err)
		}
	}
	firstOptions := listenerRecoveryBaseOptions(firstStore, filepath.Join(t.TempDir(), "registry-first.json"), firstExecutor)
	firstOptions.Interval = time.Hour
	firstOptions.PhaseBudget = 5 * time.Second
	first := startListenerRecovery(t, firstBridge, firstOptions)
	select {
	case <-firstExecutor.called:
	case <-time.After(listenerRecoveryProgressTimeout):
		first.stop(t)
		t.Fatalf("first listener did not process production poll page; Graph reads=%d errors=%v", graphState.getCount(chatID), graphState.errorsSnapshot())
	}
	waitListenerRecovery(t, func() bool {
		state, err := firstStore.Load(ctx)
		if err != nil {
			return false
		}
		firstTurn := state.Turns["turn:inbound:"+chatID+":reopen-frontier-message-1"]
		secondTurn := state.Turns["turn:inbound:"+chatID+":reopen-frontier-message-2"]
		poll := state.ChatPolls[chatID]
		return firstTurn.Status == teamstore.TurnStatusCompleted && secondTurn.Status == teamstore.TurnStatusCompleted && poll.PendingPage == nil && strings.TrimSpace(poll.ContinuationPath) == "" && countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_REOPEN_FRONTIER_FIRST_FINAL") == 2
	}, listenerRecoveryProgressTimeout, "first generation drained Graph continuation")
	if got := graphState.getCount(chatID); got < 2 {
		first.stop(t)
		t.Fatalf("first generation did not follow the stateful Graph continuation: Graph GETs=%d requests=%v", got, graphState.requestsSnapshot())
	}
	firstState, err := firstStore.Load(ctx)
	if err != nil {
		first.stop(t)
		t.Fatalf("load first frontier state: %v", err)
	}
	oldGeneration := firstState.ControlLease.Generation
	if oldGeneration <= 0 {
		first.stop(t)
		t.Fatalf("first listener did not acquire a durable lease: %#v", firstState.ControlLease)
	}
	first.stop(t)
	if err := firstStore.Close(); err != nil {
		t.Fatalf("close first frontier store: %v", err)
	}

	recoveredStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open recovered frontier store: %v", err)
	}
	t.Cleanup(func() { _ = recoveredStore.Close() })
	recoveredExecutor := &listenerRecoveryExecutor{called: make(chan string), result: ExecutionResult{
		Text: "LISTENER_RECOVERY_REOPEN_FRONTIER_FINAL", CodexThreadID: "thread-reopen-frontier", CodexTurnID: "turn-reopen-frontier-second",
	}}
	recoveredBridge := newBridgeTestBridge(graph, recoveredStore, recoveredExecutor)
	recoveredBridge.machine.ID = "machine:reopen-frontier-new"
	recoveredBridge.machine.ScopeID = recoveredBridge.scope.ID
	recoveredBridge.machine.Kind = teamstore.MachineKindPrimary
	options := listenerRecoveryBaseOptions(recoveredStore, filepath.Join(t.TempDir(), "registry-recovered.json"), recoveredExecutor)
	options.PhaseBudget = 5 * time.Second
	listener := startListenerRecovery(t, recoveredBridge, options)
	waitListenerRecovery(t, func() bool {
		state, err := recoveredStore.Load(ctx)
		return err == nil && state.ControlLease.Generation > oldGeneration
	}, listenerRecoveryProgressTimeout, "reopened listener owner takeover")
	select {
	case call := <-recoveredExecutor.called:
		listener.stop(t)
		t.Fatalf("reopened listener re-executed a completed inbound message: %q; calls=%v", call, recoveredExecutor.callsSnapshot())
	case <-time.After(250 * time.Millisecond):
	}
	if errs := graphState.errorsSnapshot(); len(errs) > 0 {
		listener.stop(t)
		t.Fatalf("reopened listener fake Graph errors: %v", errs)
	}
	if got := countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_REOPEN_FRONTIER_FIRST_FINAL"); got != 2 {
		listener.stop(t)
		t.Fatalf("reopened listener changed durable final count: %d, want 2", got)
	}
	listener.stop(t)
}

// TestTeamsListenFalsePollContinuationSurvivesReopenBeforeDrain exercises the
// restart boundary at the point that matters for a long Graph backlog: the
// first page has been committed, the next-link request is in flight, and the
// old owner disappears.  The replacement owner must follow the durable
// continuation rather than replaying the first page or falling back to the
// current head.
func TestTeamsListenFalsePollContinuationSurvivesReopenBeforeDrain(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			runListenerRecoveryPollContinuationSurvivesReopenBeforeDrain(t, useSQLite)
		})
	}
}

func runListenerRecoveryPollContinuationSurvivesReopenBeforeDrain(t *testing.T, useSQLite bool) {
	t.Helper()
	ctx := context.Background()
	storePath := filepath.Join(t.TempDir(), "state.json")
	chatID := "chat-reopen-before-drain"
	now := time.Now().UTC().Add(-time.Minute)
	firstMessage := bridgeTestMessageWithText("reopen-before-drain-message-1", "LISTENER_RECOVERY_REOPEN_BEFORE_DRAIN_PROMPT_1")
	secondMessage := bridgeTestMessageWithText("reopen-before-drain-message-2", "LISTENER_RECOVERY_REOPEN_BEFORE_DRAIN_PROMPT_2")
	for i, message := range []*ChatMessage{&firstMessage, &secondMessage} {
		message.ChatID = chatID
		message.CreatedDateTime = now.Add(time.Duration(i+1) * time.Second).Format(time.RFC3339Nano)
		message.LastModifiedDateTime = message.CreatedDateTime
	}
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{chatID: {firstMessage, secondMessage}}, 0)
	graphState.mu.Lock()
	graphState.pageSize = 1
	graphState.blockContinuationChatID = chatID
	graphState.continuationEntered = make(chan struct{})
	graphState.continuationRelease = make(chan struct{})
	graphState.mu.Unlock()

	firstStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open first continuation store: %v", err)
	}
	firstExecutor := &listenerRecoveryExecutor{called: make(chan string), result: ExecutionResult{
		Text: "LISTENER_RECOVERY_REOPEN_BEFORE_DRAIN_FINAL_1", CodexThreadID: "thread-reopen-before-drain", CodexTurnID: "turn-reopen-before-drain-1",
	}}
	firstBridge := newBridgeTestBridge(graph, firstStore, firstExecutor)
	appendBridgeTestSession(t, firstBridge, firstStore, "s-reopen-before-drain", chatID)
	listenerRecoverySeedDuePoll(t, firstStore, firstBridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, firstStore, chatID, now)
	if useSQLite {
		if _, err := firstStore.MigrateLargeStateToSQLite(ctx, 0); err != nil {
			t.Fatalf("migrate first continuation store: %v", err)
		}
	}
	firstOptions := listenerRecoveryBaseOptions(firstStore, filepath.Join(t.TempDir(), "registry-first.json"), firstExecutor)
	firstOptions.Interval = time.Hour
	firstOptions.PhaseBudget = 5 * time.Second
	first := startListenerRecovery(t, firstBridge, firstOptions)
	select {
	case <-firstExecutor.called:
	case <-time.After(listenerRecoveryProgressTimeout):
		first.stop(t)
		t.Fatalf("first page did not reach executor; Graph reads=%d requests=%v", graphState.getCount(chatID), graphState.requestsSnapshot())
	}
	select {
	case <-graphState.continuationEntered:
	case <-time.After(listenerRecoveryProgressTimeout):
		first.stop(t)
		t.Fatalf("first listener never opened the durable continuation; Graph reads=%d requests=%v", graphState.getCount(chatID), graphState.requestsSnapshot())
	}

	deadline := time.Now().Add(listenerRecoveryProgressTimeout)
	var lastState teamstore.State
	for time.Now().Before(deadline) {
		state, loadErr := firstStore.Load(ctx)
		if loadErr == nil {
			lastState = state
			// Graph returns newest-first. The first page therefore contains
			// message-2; message-1 is the older record reached through the
			// continuation that is about to be interrupted.
			firstTurn := state.Turns["turn:inbound:"+chatID+":reopen-before-drain-message-2"]
			poll := state.ChatPolls[chatID]
			if firstTurn.Status == teamstore.TurnStatusCompleted && poll.PendingPage == nil && strings.TrimSpace(poll.ContinuationPath) != "" {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	firstTurn := lastState.Turns["turn:inbound:"+chatID+":reopen-before-drain-message-2"]
	if firstTurn.Status != teamstore.TurnStatusCompleted || lastState.ChatPolls[chatID].PendingPage != nil || strings.TrimSpace(lastState.ChatPolls[chatID].ContinuationPath) == "" {
		first.stop(t)
		t.Fatalf("timed out waiting for durable continuation before restart: turn=%#v poll=%#v turns=%#v requests=%v phase=%#v", firstTurn, lastState.ChatPolls[chatID], lastState.Turns, graphState.requestsSnapshot(), firstBridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	if got := countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_REOPEN_BEFORE_DRAIN_FINAL_1"); got != 1 {
		first.stop(t)
		t.Fatalf("first page final count before restart=%d, want one", got)
	}

	first.stop(t)
	graphState.mu.Lock()
	graphState.blockContinuationChatID = ""
	release := graphState.continuationRelease
	graphState.mu.Unlock()
	close(release)
	// A real process crash cannot run the graceful attempt-abandon callback.
	// Make the in-flight capability stale at the restart boundary so the
	// replacement listener exercises the same bounded takeover path without
	// sleeping for the production two-minute attempt TTL.
	if _, _, err := firstStore.UpdateChatPoll(ctx, chatID, func(poll *teamstore.ChatPollState) error {
		if poll.Attempt != nil {
			poll.Attempt.ExpiresAt = time.Now().UTC().Add(-time.Second)
		}
		return nil
	}); err != nil {
		t.Fatalf("expire interrupted continuation attempt for restart: %v", err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatalf("close first continuation store: %v", err)
	}

	recoveredStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open recovered continuation store: %v", err)
	}
	t.Cleanup(func() { _ = recoveredStore.Close() })
	recoveredExecutor := &listenerRecoveryExecutor{called: make(chan string), result: ExecutionResult{
		Text: "LISTENER_RECOVERY_REOPEN_BEFORE_DRAIN_FINAL_2", CodexThreadID: "thread-reopen-before-drain", CodexTurnID: "turn-reopen-before-drain-2",
	}}
	recoveredBridge := newBridgeTestBridge(graph, recoveredStore, recoveredExecutor)
	recoveredBridge.machine.ID = "machine:reopen-before-drain-new"
	recoveredBridge.machine.ScopeID = recoveredBridge.scope.ID
	recoveredBridge.machine.Kind = teamstore.MachineKindPrimary
	options := listenerRecoveryBaseOptions(recoveredStore, filepath.Join(t.TempDir(), "registry-recovered.json"), recoveredExecutor)
	options.PhaseBudget = 5 * time.Second
	listener := startListenerRecovery(t, recoveredBridge, options)
	defer listener.stop(t)
	select {
	case <-recoveredExecutor.called:
	case <-time.After(listenerRecoveryProgressTimeout):
		state, _ := recoveredStore.Load(ctx)
		t.Fatalf("replacement owner did not execute the persisted continuation page; state=%#v requests=%v", state, graphState.requestsSnapshot())
	}
	waitListenerRecovery(t, func() bool {
		return countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_REOPEN_BEFORE_DRAIN_FINAL_2") == 1
	}, listenerRecoveryProgressTimeout, "replacement continuation final")

	firstCalls := firstExecutor.callsSnapshot()
	if len(firstCalls) != 1 || !strings.Contains(firstCalls[0], "REOPEN_BEFORE_DRAIN_PROMPT_2") {
		t.Fatalf("first owner calls=%v, want only newest first-page prompt", firstCalls)
	}
	recoveredCalls := recoveredExecutor.callsSnapshot()
	if len(recoveredCalls) != 1 || !strings.Contains(recoveredCalls[0], "REOPEN_BEFORE_DRAIN_PROMPT_1") {
		t.Fatalf("replacement owner calls=%v, want only older continuation-page prompt", recoveredCalls)
	}
	if got := countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_REOPEN_BEFORE_DRAIN_FINAL_1"); got != 1 {
		t.Fatalf("first page final was replayed after restart: count=%d", got)
	}
}

// TestTeamsListenFalseSQLiteOperationalFloodPreservesHealthyOrdinaryChat
// exercises the production listener admission path with more operational
// continuation rows than the durable hot-poll quantum.  The healthy chat is
// deliberately inserted after that prefix and has no continuation.  It must
// still reach Graph and Codex in the same listener lifetime; testing only the
// store candidate list would miss a regression in the real Listen pipeline.
func TestTeamsListenFalseSQLiteOperationalFloodPreservesHealthyOrdinaryChat(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Minute)
	healthyChatID := "chat-operational-flood-healthy"
	healthy := bridgeTestMessageWithText("operational-flood-healthy-message", "LISTENER_RECOVERY_OPERATIONAL_FLOOD_HEALTHY_PROMPT")
	healthy.ChatID = healthyChatID
	healthy.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	healthy.LastModifiedDateTime = healthy.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		healthyChatID: {healthy},
	}, 0)
	storePath := filepath.Join(t.TempDir(), "state.json")
	store, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open operational flood store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	executor := &listenerRecoveryExecutor{called: make(chan string), result: ExecutionResult{
		Text: "LISTENER_RECOVERY_OPERATIONAL_FLOOD_HEALTHY_FINAL", CodexThreadID: "thread-operational-flood", CodexTurnID: "turn-operational-flood",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)
	// Remove the generic chat-1 fixture from newBridgeTestBridge.  It is not
	// part of this scenario; leaving it in the registry would create another
	// ordinary candidate and make the fairness assertion select that fixture
	// instead of the deliberately tail-positioned healthy chat.
	bridge.reg.Sessions = nil
	const operationalCount = 65
	for i := 0; i < operationalCount; i++ {
		chatID := fmt.Sprintf("chat-operational-flood-%03d", i)
		appendBridgeTestSession(t, bridge, store, fmt.Sprintf("session-operational-flood-%03d", i), chatID)
	}
	appendBridgeTestSession(t, bridge, store, "session-operational-flood-healthy", healthyChatID)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		for i := 0; i < operationalCount; i++ {
			chatID := fmt.Sprintf("chat-operational-flood-%03d", i)
			state.ChatPolls[chatID] = teamstore.ChatPollState{
				ChatID: chatID, Seeded: true, PollState: inboundPollStateWarm,
				NextPollAt: now.Add(-time.Second), LastActivityAt: now,
				ContinuationPath: "/chats/" + chatID + "/messages?$skiptoken=operational-flood",
				UpdatedAt:        now,
			}
		}
		state.ChatPolls[healthyChatID] = teamstore.ChatPollState{
			ChatID: healthyChatID, Seeded: true, PollState: inboundPollStateWarm,
			NextPollAt: now.Add(-time.Second), LastActivityAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed operational flood polls: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate operational flood store to SQLite: %v", err)
	}

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	// The admission query intentionally evaluates every operational row.  The
	// race detector makes SQLite JSON1 and the surrounding state-lock path much
	// slower than production; give the query a generous phase budget so this
	// test measures lane fairness rather than a race-instrumentation deadline.
	options.PhaseBudget = 30 * time.Second
	options.PollWorkerBudget = 5 * time.Second
	// The race detector can hold the test store's state mutex across the
	// intentionally wide admission query for longer than the normal 30-second
	// lease.  This test is about ordinary-lane admission, not lease-expiry
	// recovery; keep the owner alive long enough that instrumentation cannot
	// turn the query cost into a synthetic takeover loop.
	options.OwnerStaleAfter = 2 * time.Minute
	bridge.leaseDuration = 5 * time.Minute
	bridge.ownerHeartbeatInterval = 5 * time.Second
	listener := startListenerRecovery(t, bridge, options)
	progressDeadline := 30 * time.Second
	select {
	case <-executor.called:
		calls := executor.callsSnapshot()
		if len(calls) == 0 || !strings.Contains(calls[0], "LISTENER_RECOVERY_OPERATIONAL_FLOOD_HEALTHY_PROMPT") {
			listener.stop(t)
			t.Fatalf("operational flood dispatched unexpected prompt; calls=%v", calls)
		}
	case <-time.After(progressDeadline):
		listener.stop(t)
		t.Fatalf("healthy ordinary chat never reached Codex behind %d operational rows; Graph reads=%d requests=%v poll=%#v", operationalCount, graphState.getCount(healthyChatID), graphState.requestsSnapshot(), bridge.mainLoopPhaseStatsSnapshot("poll"))
	}
	waitListenerRecovery(t, func() bool {
		return countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_OPERATIONAL_FLOOD_HEALTHY_FINAL") == 1
	}, progressDeadline, "healthy ordinary chat final after operational flood")
	listener.stop(t)
	if got := countListenerRecoverySentBodies(graphState.sentSnapshot(), "LISTENER_RECOVERY_OPERATIONAL_FLOOD_HEALTHY_FINAL"); got != 1 {
		t.Fatalf("healthy ordinary final POST count=%d, want one", got)
	}
	if got := graphState.getCount(healthyChatID); got < 1 {
		t.Fatalf("healthy ordinary chat Graph GET count=%d, want at least one", got)
	}
	if errs := graphState.errorsSnapshot(); len(errs) > 0 {
		t.Fatalf("operational flood fake Graph errors: %v", errs)
	}
}

// TestTeamsListenFalseShutdownDoesNotRunAsyncTurnFollowupAfterGrace proves
// that a worker which ignores cancellation cannot mutate the store after the
// listener has released its owner lease.  This is the restart boundary that
// previously allowed a late error callback to create a misleading failure
// notice (or race a replacement owner).
func TestTeamsListenFalseShutdownDoesNotRunAsyncTurnFollowupAfterGrace(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	message := bridgeTestMessageWithText("shutdown-message", "LISTENER_RECOVERY_SHUTDOWN_PROMPT")
	message.ChatID = "chat-1"
	message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1": {message},
	}, 0)
	store := newBridgeTestStore(t)
	executor := &listenerRecoveryBlockingExecutor{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	options.PhaseBudget = 5 * time.Second
	options.PollWorkerBudget = time.Second
	options.AsyncTurnShutdownGrace = 25 * time.Millisecond
	listener := startListenerRecovery(t, bridge, options)
	select {
	case <-executor.started:
	case <-time.After(3 * listenerRecoveryProgressTimeout):
		close(executor.release)
		listener.stop(t)
		t.Fatalf("listener never dispatched the blocking executor; Graph reads=%d", graphState.getCount("chat-1"))
	}

	listener.stop(t)
	close(executor.release)
	waitListenerRecovery(t, func() bool { return bridge.activeAsyncTurnCount() == 0 }, time.Second, "blocking worker exit")

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state after shutdown: %v", err)
	}
	var runningTurns int
	for _, turn := range state.Turns {
		if turn.SessionID != "s001" {
			continue
		}
		if turn.Status != teamstore.TurnStatusRunning {
			t.Fatalf("late worker changed turn after listener shutdown: %#v", turn)
		}
		runningTurns++
	}
	if runningTurns != 1 {
		t.Fatalf("running turns after shutdown = %d, want one durable takeover candidate", runningTurns)
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.TurnID != "" && (outbox.Kind == "error" || outbox.Kind == "interrupted" || strings.HasSuffix(outbox.ID, ":queued-turn-error")) {
			t.Fatalf("late worker ran queued-turn failure/recovery side effect after listener shutdown: %#v", outbox)
		}
	}
}

// TestTeamsListenFalseShutdownFencesCooperativeExecutorError covers the
// opposite timing from the non-cooperative shutdown test: the executor exits
// immediately when the listener context is canceled.  The worker can wake up
// before Listen's deferred shutdown wait runs, so the context cancellation
// itself must fence the failure/recovery path.
func TestTeamsListenFalseShutdownFencesCooperativeExecutorError(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	message := bridgeTestMessageWithText("shutdown-cooperative-message", "LISTENER_RECOVERY_COOPERATIVE_SHUTDOWN_PROMPT")
	message.ChatID = "chat-1"
	message.CreatedDateTime = now.Add(time.Second).Format(time.RFC3339Nano)
	message.LastModifiedDateTime = message.CreatedDateTime
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1": {message},
	}, 0)
	store := newBridgeTestStore(t)
	executor := &listenerRecoveryCooperativeCancellationExecutor{started: make(chan struct{})}
	bridge := newBridgeTestBridge(graph, store, executor)
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)

	options := listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), executor)
	options.PhaseBudget = 5 * time.Second
	options.PollWorkerBudget = time.Second
	options.AsyncTurnShutdownGrace = 25 * time.Millisecond
	listener := startListenerRecovery(t, bridge, options)
	select {
	case <-executor.started:
	case <-time.After(3 * listenerRecoveryProgressTimeout):
		listener.stop(t)
		t.Fatalf("listener never dispatched the cooperative executor; Graph reads=%d", graphState.getCount("chat-1"))
	}

	listener.stop(t)
	waitListenerRecovery(t, func() bool { return bridge.activeAsyncTurnCount() == 0 }, time.Second, "cooperative worker exit")
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load state after cooperative shutdown: %v", err)
	}
	var runningTurns int
	for _, turn := range state.Turns {
		if turn.SessionID != "s001" {
			continue
		}
		if turn.Status != teamstore.TurnStatusRunning {
			t.Fatalf("cooperative late worker changed turn after listener shutdown: %#v", turn)
		}
		runningTurns++
	}
	if runningTurns != 1 {
		t.Fatalf("running turns after cooperative shutdown = %d, want one durable takeover candidate", runningTurns)
	}
	for _, outbox := range state.OutboxMessages {
		if outbox.TurnID != "" && (outbox.Kind == "error" || outbox.Kind == "interrupted") {
			t.Fatalf("cooperative late worker ran failure/recovery side effect: %#v", outbox)
		}
	}
}

// TestTeamsAmbiguousPostDoesNotHeuristicallySettleDifferentMessage protects
// the no-duplicate boundary after a POST response is lost.  Matching author,
// time, and rendered text is not enough: a separate user-authored message can
// look identical, so only the exact helper provenance suffix may settle the
// unknown Graph outcome.
func TestTeamsAmbiguousPostDoesNotHeuristicallySettleDifferentMessage(t *testing.T) {
	ctx := context.Background()
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{}, 0)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	outbox, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:listener-ambiguous-marker",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "LISTENER_AMBIGUOUS_SAME_TEXT",
	})
	if err != nil {
		t.Fatalf("queue ambiguous outbox: %v", err)
	}
	outbox, err = store.MarkOutboxSendAttempt(ctx, outbox.ID)
	if err != nil {
		t.Fatalf("claim ambiguous outbox: %v", err)
	}
	outbox, err = store.MarkOutboxAmbiguousSendErrorForAttempt(ctx, outbox.ID, outbox.SendAttemptToken, "response lost")
	if err != nil {
		t.Fatalf("mark ambiguous outbox: %v", err)
	}
	rendered, _, _ := bridge.renderOutboxHTMLForSend(ctx, outbox)
	distractor := bridgeTestMessageWithText("different-message", rendered)
	distractor.ChatID = "chat-1"
	distractor.CreatedDateTime = time.Now().UTC().Format(time.RFC3339Nano)
	distractor.LastModifiedDateTime = distractor.CreatedDateTime
	graphState.mu.Lock()
	graphState.messages["chat-1"] = []ChatMessage{distractor}
	graphState.mu.Unlock()

	probeBudget := 1
	pageBudget := 1
	err = bridge.sendQueuedOutboxWithOptions(ctx, outbox, outboxSendOptions{
		RecoveryProbeBudget: &probeBudget,
		RecoveryPageBudget:  &pageBudget,
	})
	if err == nil {
		t.Fatal("ambiguous outbox recovery returned nil; want a deferred no-match result")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state after ambiguous recovery: %v", err)
	}
	recovered := state.OutboxMessages[outbox.ID]
	if recovered.Status != teamstore.OutboxStatusSending || strings.TrimSpace(recovered.TeamsMessageID) != "" {
		t.Fatalf("markerless distractor settled ambiguous outbox: %#v", recovered)
	}
	if got := len(graphState.sentSnapshot()); got != 0 {
		t.Fatalf("ambiguous recovery issued %d new Graph POST(s), want none", got)
	}
}

// TestTeamsOutboxAcceptedResponseFinishesAfterPhaseDeadline protects the
// post-Graph boundary.  A successful Graph response can race the listener's
// short outbox phase deadline; the response must not leave a plain helper row
// stuck in Accepted merely because its final local CAS inherited the expired
// phase context.
func TestTeamsOutboxAcceptedResponseFinishesAfterPhaseDeadline(t *testing.T) {
	ctx, cancelPhase := context.WithCancel(context.Background())
	defer cancelPhase()
	executionCtx := context.Background()
	phaseCtx := withTeamsPhaseExecutionContext(ctx, executionCtx)
	graph := &GraphClient{
		auth: &fakeGraphAuth{token: "access"},
		client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
				return nil, fmt.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			if len(bytes.TrimSpace(body)) == 0 {
				return nil, errors.New("empty Graph request body")
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: &listenerRecoveryCancelAfterReadBody{
					data:   []byte(`{"id":"phase-race-sent","messageType":"message"}`),
					cancel: cancelPhase,
				},
			}, nil
		})},
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	row, _, err := store.QueueOutbox(phaseCtx, teamstore.OutboxMessage{
		ID:          "outbox:phase-deadline-after-graph",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "phase deadline must not strand accepted response",
	})
	if err != nil {
		t.Fatalf("queue phase-race outbox: %v", err)
	}
	if err := bridge.sendQueuedOutboxWithOptions(phaseCtx, row, outboxSendOptions{IgnoreEarlierOutbox: true}); err != nil {
		t.Fatalf("send phase-race outbox: %v", err)
	}
	if phaseCtx.Err() == nil {
		t.Fatal("phase context was not canceled at Graph response boundary")
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load phase-race state: %v", err)
	}
	got := state.OutboxMessages[row.ID]
	if got.Status != teamstore.OutboxStatusSent || got.TeamsMessageID != "phase-race-sent" || got.SendAttemptToken != "" {
		t.Fatalf("phase-race outbox = %#v, want durable Sent projection", got)
	}
}

// TestTeamsListenFalseRecoversExpiredAmbiguousOutboxWithoutPost exercises the
// actual listener restart path.  An earlier process has already sent a POST
// but lost the response, so the durable row is Sending/ambiguous.  A new
// listener must reconcile the exact provenance marker from Graph history and
// settle the row without issuing a second POST.
func TestTeamsListenFalseRecoversExpiredAmbiguousOutboxWithoutPost(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	const outboxID = "outbox:listener-ambiguous-restart"
	now := time.Now().UTC()
	legacyAttemptAt := now.Add(-10 * time.Minute)
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.OutboxMessages[outboxID] = teamstore.OutboxMessage{
			ID: outboxID, TeamsChatID: "chat-1", Kind: "helper",
			Body:             "LISTENER_AMBIGUOUS_RESTART_RECOVERY",
			Status:           teamstore.OutboxStatusSending,
			SendAttemptToken: "old-process-attempt",
			LastSendAttempt:  legacyAttemptAt,
			LastSendError:    "ambiguous Graph send; listener restart lost response",
			CreatedAt:        legacyAttemptAt,
			UpdatedAt:        legacyAttemptAt,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed ambiguous outbox: %v", err)
	}

	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	rendered, _, _ := bridge.renderOutboxHTMLForSend(ctx, teamstore.OutboxMessage{
		ID: outboxID, TeamsChatID: "chat-1", Kind: "helper", Body: "LISTENER_AMBIGUOUS_RESTART_RECOVERY",
	})
	remote := bridgeTestMessageWithText("teams-ambiguous-restart", rendered+helperOutboxProvenanceMarker(outboxID))
	remote.ChatID = "chat-1"
	remote.CreatedDateTime = legacyAttemptAt.Add(time.Second).Format(time.RFC3339Nano)
	remote.LastModifiedDateTime = remote.CreatedDateTime
	// Construct the live fake Graph with the exact remote marker after the
	// rendering helper has produced the canonical body.
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{
		"chat-1": {remote},
	}, 0)
	bridge.graph = graph
	if got := helperOutboxProvenanceMarkerID(remote.Body.Content); got != outboxID {
		t.Fatalf("recovery fixture marker = %q, want %q; body=%q", got, outboxID, remote.Body.Content)
	}
	incomingContent := stripHelperOutboxProvenanceMarker(remote.Body.Content)
	incomingKey := comparableTeamsPlainText(PlainTextFromTeamsHTML(incomingContent))
	if !outboxRenderedPlainTextMatches(teamstore.OutboxMessage{
		ID: outboxID, TeamsChatID: "chat-1", Kind: "helper", Body: "LISTENER_AMBIGUOUS_RESTART_RECOVERY",
	}, bridge.user, incomingKey) {
		t.Fatalf("recovery fixture body mismatch: rendered=%q stripped=%q key=%q", rendered, incomingContent, incomingKey)
	}
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now.Add(-time.Minute))
	listenerRecoverySeedDuePoll(t, store, "chat-1", now.Add(-time.Minute))

	listener := startListenerRecovery(t, bridge, listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor))
	settled := false
	deadline := time.Now().Add(listenerRecoveryProgressTimeout)
	for time.Now().Before(deadline) {
		state, err := store.Load(ctx)
		if err == nil {
			row := state.OutboxMessages[outboxID]
			if row.Status == teamstore.OutboxStatusSent && row.TeamsMessageID == remote.ID {
				settled = true
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if !settled {
		state, err := store.Load(ctx)
		t.Fatalf("expired ambiguous outbox did not settle: load=%v row=%#v gets=%d posts=%#v phase-outbox=%#v phase-poll=%#v listener-err=%v state=%#v", err, state.OutboxMessages[outboxID], graphState.getCount("chat-1"), graphState.sentSnapshot(), bridge.mainLoopPhaseStatsSnapshot("outbox"), bridge.mainLoopPhaseStatsSnapshot("poll"), listener.err, state)
	}
	listener.stop(t)
	if got := len(graphState.sentSnapshot()); got != 0 {
		t.Fatalf("ambiguous restart recovery issued %d Graph POST(s), want none", got)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load settled ambiguous outbox: %v", err)
	}
	row := state.OutboxMessages[outboxID]
	if row.Status != teamstore.OutboxStatusSent || row.TeamsMessageID != remote.ID || row.SendAttemptToken != "" {
		t.Fatalf("settled ambiguous outbox = %#v, want Sent with exact remote identity and cleared attempt", row)
	}
}

// TestTeamsListenFalseGraphAcceptedDisconnectReconcilesAfterReopen exercises
// the real listener at the most dangerous delivery boundary. Graph commits
// the POST (the fake provider records the message) and then drops the HTTP
// connection before returning its ID. The first owner must retain an
// ambiguous Sending row; after a simulated elapsed lease and store reopen, a
// replacement listener must find the exact marker and settle it without a
// second POST.
func TestTeamsListenFalseGraphAcceptedDisconnectReconcilesAfterReopen(t *testing.T) {
	ctx := context.Background()
	storePath := filepath.Join(t.TempDir(), "state.json")
	store, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	var mu sync.Mutex
	var remote []ChatMessage
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) == 3 && parts[0] == "chats" && parts[2] == "messages" && r.Method == http.MethodGet {
			mu.Lock()
			messages := append([]ChatMessage(nil), remote...)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"value": messages})
			return
		}
		if len(parts) != 3 || parts[0] != "chats" || parts[2] != "messages" || r.Method != http.MethodPost {
			http.Error(w, "unexpected fake Graph request", http.StatusNotFound)
			return
		}
		var payload struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		created := time.Now().UTC().Format(time.RFC3339Nano)
		message := ChatMessage{
			ID:                   "graph-accepted-before-disconnect",
			ChatID:               parts[1],
			CreatedDateTime:      created,
			LastModifiedDateTime: created,
			MessageType:          "message",
		}
		message.From.User = &struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		}{ID: "user-1", DisplayName: "User"}
		message.Body.ContentType = "html"
		message.Body.Content = payload.Body.Content
		mu.Lock()
		posts++
		first := posts == 1
		remote = append(remote, message)
		mu.Unlock()
		if first {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "fake Graph server does not support connection drop", http.StatusInternalServerError)
				return
			}
			connection, _, err := hijacker.Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		_ = json.NewEncoder(w).Encode(message)
	}))
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:graph-accepted-disconnect",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "LISTENER_RECOVERY_GRAPH_ACCEPTED_DISCONNECT",
	})
	if err != nil {
		t.Fatalf("queue outbox: %v", err)
	}
	listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now)
	listenerRecoverySeedDuePoll(t, store, "chat-1", now)
	first := startListenerRecovery(t, bridge, listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor))
	ambiguous := false
	deadline := time.Now().Add(listenerRecoveryProgressTimeout)
	for time.Now().Before(deadline) {
		state, loadErr := store.Load(ctx)
		if loadErr == nil {
			row := state.OutboxMessages[queued.ID]
			if row.Status == teamstore.OutboxStatusSending && teamstore.OutboxSendIsAmbiguous(row) {
				ambiguous = true
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if !ambiguous {
		first.stop(t)
		state, _ := store.Load(ctx)
		mu.Lock()
		postCount := posts
		mu.Unlock()
		t.Fatalf("Graph accepted disconnect did not produce ambiguous Sending row: posts=%d row=%#v phase=%#v", postCount, state.OutboxMessages[queued.ID], bridge.mainLoopPhaseStatsSnapshot("outbox"))
	}
	first.stop(t)
	// Advance only the durable lease timestamp. This models the service being
	// down past the normal ambiguity lease without making the test wait two
	// minutes; it does not manufacture a Graph identity or alter the payload.
	if err := store.Update(ctx, func(state *teamstore.State) error {
		row := state.OutboxMessages[queued.ID]
		row.LastSendAttempt = now.Add(-10 * time.Minute)
		row.NextAttemptAt = time.Time{}
		state.OutboxMessages[queued.ID] = row
		return nil
	}); err != nil {
		t.Fatalf("advance ambiguous lease timestamp: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}
	recoveredStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	recoveredExecutor := &recordingExecutor{}
	recoveredBridge := newBridgeTestBridge(graph, recoveredStore, recoveredExecutor)
	t.Cleanup(func() { _ = recoveredStore.Close() })
	recovered := startListenerRecovery(t, recoveredBridge, listenerRecoveryBaseOptions(recoveredStore, filepath.Join(t.TempDir(), "registry.json"), recoveredExecutor))
	settled := false
	deadline = time.Now().Add(listenerRecoveryProgressTimeout)
	for time.Now().Before(deadline) {
		state, loadErr := recoveredStore.Load(ctx)
		if loadErr == nil {
			row := state.OutboxMessages[queued.ID]
			if row.Status == teamstore.OutboxStatusSent && row.TeamsMessageID == "graph-accepted-before-disconnect" {
				settled = true
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if !settled {
		state, _ := recoveredStore.Load(ctx)
		recovered.stop(t)
		mu.Lock()
		postCount := posts
		mu.Unlock()
		t.Fatalf("reopened listener did not reconcile exact Graph message: posts=%d row=%#v polls=%v phase=%#v", postCount, state.OutboxMessages[queued.ID], state.ChatPolls, recoveredBridge.mainLoopPhaseStatsSnapshot("outbox"))
	}
	recovered.stop(t)
	mu.Lock()
	gotPosts := posts
	gotRemote := append([]ChatMessage(nil), remote...)
	mu.Unlock()
	if gotPosts != 1 || len(gotRemote) != 1 {
		t.Fatalf("Graph accepted disconnect recovery posts=%d remote=%d, want exactly one of each", gotPosts, len(gotRemote))
	}
}

// TestTeamsListenFalseMarkerlessAmbiguousOutboxStaysHeldWithoutPost covers a
// legacy pre-attempt-token Sending row whose Graph result cannot be proven.
// The row must remain durable and held: treating an empty Graph window as a
// rejection would permit a duplicate POST after restart.  The same listener
// must nevertheless continue delivering an unrelated healthy outbox row, so
// the safety fence is local to the unknown side effect rather than a global
// outbox deadlock.
func TestTeamsListenFalseMarkerlessAmbiguousOutboxStaysHeldWithoutPost(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate markerless ambiguous store: %v", err)
				}
			}
			now := time.Now().UTC()
			const legacyID = "outbox:listener-markerless-ambiguous"
			const healthyID = "outbox:listener-healthy-after-ambiguous"
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.OutboxMessages[legacyID] = teamstore.OutboxMessage{
					ID: legacyID, SessionID: "s001", TurnID: "turn:legacy-markerless",
					TeamsChatID: "chat-1", Kind: "helper",
					Body:            "LEGACY_MARKERLESS_MUST_STAY_HELD",
					Status:          teamstore.OutboxStatusSending,
					LastSendAttempt: now.Add(-10 * time.Minute),
					CreatedAt:       now.Add(-10 * time.Minute), UpdatedAt: now.Add(-10 * time.Minute),
				}
				state.OutboxMessages[healthyID] = teamstore.OutboxMessage{
					ID: healthyID, TeamsChatID: "chat-healthy-after-ambiguous",
					Kind: "helper", Body: "HEALTHY_OUTBOX_AFTER_AMBIGUOUS",
					Status:    teamstore.OutboxStatusQueued,
					CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed markerless ambiguous rows: %v", err)
			}

			graph, graphState := newListenerRecoveryGraph(t, nil, nil, 0)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			listenerRecoverySeedDuePoll(t, store, bridge.reg.ControlChatID, now.Add(-time.Minute))
			listenerRecoverySeedDuePoll(t, store, "chat-1", now.Add(-time.Minute))
			listener := startListenerRecovery(t, bridge, listenerRecoveryBaseOptions(store, filepath.Join(t.TempDir(), "registry.json"), bridge.executor))

			progressed := false
			deadline := time.Now().Add(listenerRecoveryProgressTimeout)
			for time.Now().Before(deadline) {
				state, err := store.Load(ctx)
				if err == nil {
					legacy := state.OutboxMessages[legacyID]
					healthy := state.OutboxMessages[healthyID]
					if healthy.Status == teamstore.OutboxStatusSent &&
						legacy.Status == teamstore.OutboxStatusSending &&
						strings.TrimSpace(legacy.TeamsMessageID) == "" &&
						strings.TrimSpace(legacy.SendAttemptToken) != "" &&
						teamstore.OutboxSendIsAmbiguous(legacy) {
						progressed = true
						break
					}
				}
				time.Sleep(time.Millisecond)
			}
			if !progressed {
				state, err := store.Load(ctx)
				t.Fatalf("markerless ambiguous isolation did not progress: load=%v legacy=%#v healthy=%#v sent=%#v phases: outbox=%#v poll=%#v listener-err=%v", err, state.OutboxMessages[legacyID], state.OutboxMessages[healthyID], graphState.sentSnapshot(), bridge.mainLoopPhaseStatsSnapshot("outbox"), bridge.mainLoopPhaseStatsSnapshot("poll"), listener.err)
			}
			listener.stop(t)

			sent := graphState.sentSnapshot()
			if len(sent) != 1 || !strings.Contains(PlainTextFromTeamsHTML(sent[0].Body), "HEALTHY_OUTBOX_AFTER_AMBIGUOUS") {
				t.Fatalf("markerless ambiguous isolation Graph POSTs = %#v, want only healthy outbox", sent)
			}
			if graphErrors := graphState.errorsSnapshot(); len(graphErrors) != 0 {
				t.Fatalf("markerless ambiguous isolation fake Graph errors = %v; requests=%v", graphErrors, graphState.requestsSnapshot())
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load markerless ambiguous final state: %v", err)
			}
			legacy := state.OutboxMessages[legacyID]
			if legacy.Status != teamstore.OutboxStatusSending || strings.TrimSpace(legacy.TeamsMessageID) != "" || !teamstore.OutboxSendIsAmbiguous(legacy) {
				t.Fatalf("markerless ambiguous final row = %#v, want held Sending without Graph identity", legacy)
			}
		})
	}
}

// TestTeamsLegacyAttachmentUnknownStateReconcilesBeforePost verifies the
// migration-only attachment shape that has a DriveItem but no durable record
// of whether the later Teams POST started.  A restarted listener must probe
// the exact helper marker first; an empty Graph window is not permission to
// POST again.  Once the marker is visible, the same durable row can settle as
// Sent without creating a second Teams message.
func TestTeamsLegacyAttachmentUnknownStateReconcilesBeforePost(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate attachment recovery store: %v", err)
				}
			}
			graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{"chat-1": nil}, 0)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			const outboxID = "outbox:legacy-attachment-unknown-post-state"
			queued, created, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
				ID:              outboxID,
				TeamsChatID:     "chat-1",
				Kind:            "attachment",
				Body:            "legacy attachment must reconcile",
				AttachmentPath:  filepath.Join(t.TempDir(), "legacy.bin"),
				AttachmentName:  "legacy.bin",
				DriveItemID:     "drive-legacy-attachment",
				DriveItemName:   "legacy.bin",
				DriveItemETag:   `"{E54AD2C5-ADAA-4F2B-A866-A119814FD3AA},1"`,
				DriveItemWebDav: "https://contoso.sharepoint.com/legacy.bin",
			})
			if err != nil || !created || queued.AttachmentMessagePostState != "unknown" {
				t.Fatalf("legacy attachment admission = %#v created=%v err=%v, want unknown", queued, created, err)
			}

			flushErr := bridge.flushPendingOutboxForChat(ctx, "chat-1")
			if flushErr == nil || !isOutboxDeliveryDeferred(flushErr) {
				t.Fatalf("legacy attachment empty-window flush error = %v, want deferred recovery", flushErr)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load legacy attachment after empty-window probe: %v", err)
			}
			held := state.OutboxMessages[outboxID]
			if held.Status != teamstore.OutboxStatusSending || !teamstore.OutboxSendIsAmbiguous(held) || held.AttachmentMessagePostState != "started" || held.TeamsMessageID != "" {
				t.Fatalf("legacy attachment after empty-window probe = %#v, want ambiguous started without ID", held)
			}
			if got := len(graphState.sentSnapshot()); got != 0 {
				t.Fatalf("empty-window legacy recovery issued %d Graph POST(s), want none", got)
			}

			rendered, _, _ := bridge.renderOutboxHTMLForSend(ctx, held)
			remote := bridgeTestMessageWithText("teams-legacy-attachment-recovered", rendered+helperOutboxProvenanceMarker(outboxID))
			remote.ChatID = "chat-1"
			remote.CreatedDateTime = held.LastSendAttempt.Add(time.Second).Format(time.RFC3339Nano)
			remote.LastModifiedDateTime = remote.CreatedDateTime
			graphState.mu.Lock()
			graphState.messages["chat-1"] = []ChatMessage{remote}
			graphState.mu.Unlock()

			recovered, err := bridge.recoverAcceptedOutboxFromGraph(ctx, held, outboxSendOptions{})
			if err != nil || !recovered {
				t.Fatalf("legacy attachment exact-marker recovery = recovered:%t err:%v", recovered, err)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("load legacy attachment after exact-marker recovery: %v", err)
			}
			settled := state.OutboxMessages[outboxID]
			if settled.Status != teamstore.OutboxStatusSent || settled.TeamsMessageID != remote.ID || settled.AttachmentMessagePostState != "started" {
				t.Fatalf("legacy attachment after exact-marker recovery = %#v, want Sent with marker ID", settled)
			}
			if got := len(graphState.sentSnapshot()); got != 0 {
				t.Fatalf("exact-marker legacy recovery issued %d Graph POST(s), want none", got)
			}
		})
	}
}

// TestTeamsOutboxDoesNotMarkSentBeforeOutboundLedgerIsDurable covers the
// Graph-accepted/local-persistence boundary.  A failed ledger write must leave
// the durable row Accepted, and the next pass must finish that row without a
// second Graph POST.
func TestTeamsOutboxDoesNotMarkSentBeforeOutboundLedgerIsDurable(t *testing.T) {
	ctx := context.Background()
	graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{}, 0)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	badParent := filepath.Join(t.TempDir(), "ledger-parent-file")
	if err := os.WriteFile(badParent, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write invalid ledger parent: %v", err)
	}
	bridge.registryPath = filepath.Join(badParent, "registry.json")

	outbox, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:listener-ledger-boundary",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "LISTENER_LEDGER_BOUNDARY",
	})
	if err != nil {
		t.Fatalf("queue outbox: %v", err)
	}
	if err := bridge.sendQueuedOutboxWithOptions(ctx, outbox, outboxSendOptions{}); err == nil {
		t.Fatal("send returned nil after forced global ledger failure")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state after ledger failure: %v", err)
	}
	accepted := state.OutboxMessages[outbox.ID]
	if accepted.Status != teamstore.OutboxStatusAccepted || strings.TrimSpace(accepted.TeamsMessageID) == "" {
		t.Fatalf("outbox after ledger failure = %#v, want Accepted with Graph identity", accepted)
	}
	if got := len(graphState.sentSnapshot()); got != 1 {
		t.Fatalf("Graph POST count after first attempt = %d, want one", got)
	}

	bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")
	if err := bridge.sendQueuedOutboxWithOptions(ctx, accepted, outboxSendOptions{}); err != nil {
		t.Fatalf("retry accepted outbox after ledger recovery: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load state after ledger recovery: %v", err)
	}
	sent := state.OutboxMessages[outbox.ID]
	if sent.Status != teamstore.OutboxStatusSent {
		t.Fatalf("outbox after ledger recovery = %#v, want Sent", sent)
	}
	if got := len(graphState.sentSnapshot()); got != 1 {
		t.Fatalf("Graph POST count after ledger recovery = %d, want no duplicate POST", got)
	}
}

// TestTeamsMainLoopOutboxLedgerFailureDoesNotStarveHealthyTail ensures that
// a large prefix of Graph-accepted rows whose local outbound-ledger write is
// temporarily failing is both cooled down durably and isolated from a
// healthy row behind the scan budget.  The accepted rows must never be POSTed
// again; the healthy row must become visible on the next main-loop pass.
func TestTeamsMainLoopOutboxLedgerFailureDoesNotStarveHealthyTail(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			graph, graphState := newListenerRecoveryGraph(t, nil, map[string][]ChatMessage{}, 0)
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate store to SQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			badParent := filepath.Join(t.TempDir(), "ledger-parent-file")
			if err := os.WriteFile(badParent, []byte("not a directory"), 0o600); err != nil {
				t.Fatalf("write invalid ledger parent: %v", err)
			}
			bridge.registryPath = filepath.Join(badParent, "registry.json")
			now := time.Now().UTC().Add(-time.Minute)
			const acceptedPrefix = 65
			for index := 0; index < acceptedPrefix; index++ {
				if _, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
					ID:             fmt.Sprintf("outbox:ledger-prefix:%03d", index),
					TeamsChatID:    "chat-ledger-poison",
					Kind:           "helper",
					Body:           fmt.Sprintf("LEDGER_POISON_%03d", index),
					Status:         teamstore.OutboxStatusAccepted,
					TeamsMessageID: fmt.Sprintf("teams-ledger-poison-%03d", index),
					CreatedAt:      now.Add(time.Duration(index) * time.Millisecond),
				}); err != nil {
					t.Fatalf("queue accepted prefix %d: %v", index, err)
				}
			}
			healthy, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
				ID:          "outbox:ledger-healthy-tail",
				TeamsChatID: "chat-ledger-healthy",
				Kind:        "helper",
				Body:        "LEDGER_HEALTHY_TAIL",
				CreatedAt:   now.Add(acceptedPrefix * time.Millisecond),
			})
			if err != nil {
				t.Fatalf("queue healthy tail: %v", err)
			}

			if err := bridge.flushPendingOutboxMainLoop(ctx); err == nil {
				t.Fatal("first outbox flush unexpectedly succeeded with a broken ledger")
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load state after broken-ledger flush: %v", err)
			}
			firstPoison := state.OutboxMessages["outbox:ledger-prefix:000"]
			if firstPoison.Status != teamstore.OutboxStatusAccepted || firstPoison.NextAttemptAt.IsZero() {
				t.Fatalf("accepted prefix after first flush = %#v, want gated Accepted row", firstPoison)
			}
			if state.OutboxMessages[healthy.ID].Status != teamstore.OutboxStatusQueued {
				t.Fatalf("healthy tail after first flush = %#v, want queued behind bounded poison scan", state.OutboxMessages[healthy.ID])
			}
			if got := len(graphState.sentSnapshot()); got != 0 {
				t.Fatalf("broken-ledger flush issued %d Graph POST(s), want none", got)
			}

			bridge.registryPath = filepath.Join(t.TempDir(), "registry.json")
			// The 65th poison row was outside the first bounded page and may
			// still fail before the healthy row. Either outcome must leave the
			// healthy row delivered.
			_ = bridge.flushPendingOutboxMainLoop(ctx)
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("load state after recovered-ledger flush: %v", err)
			}
			if got := state.OutboxMessages[healthy.ID].Status; got != teamstore.OutboxStatusSent {
				t.Fatalf("healthy tail after recovered-ledger flush = %s, want Sent", got)
			}
			if got := countListenerRecoverySentBodies(graphState.sentSnapshot(), "LEDGER_HEALTHY_TAIL"); got != 1 {
				t.Fatalf("healthy tail Graph POST count = %d, want one; sent=%#v", got, graphState.sentSnapshot())
			}
			if got := countListenerRecoverySentBodies(graphState.sentSnapshot(), "LEDGER_POISON_"); got != 0 {
				t.Fatalf("poison prefix Graph POST count = %d, want none; sent=%#v", got, graphState.sentSnapshot())
			}
		})
	}
}

// TestTeamsOutboxOwnerCapabilitySurvivesTakeoverAfterPost exercises the
// narrowest dangerous crash boundary: Graph accepts a POST while owner A is
// still in flight, owner B takes the durable lease, and A receives the
// response only afterwards. A's final CAS must be rejected; B may reconcile
// the exact marker without issuing a second POST.
func TestTeamsOutboxOwnerCapabilitySurvivesTakeoverAfterPost(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			var mu sync.Mutex
			var remote []ChatMessage
			var postStarted = make(chan struct{})
			var releasePost = make(chan struct{})
			var postOnce sync.Once
			var posts int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages" {
					mu.Lock()
					items := append([]ChatMessage(nil), remote...)
					mu.Unlock()
					_ = json.NewEncoder(w).Encode(map[string]any{"value": items})
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
					http.Error(w, "unexpected fake Graph request", http.StatusNotFound)
					return
				}
				var payload struct {
					Body struct {
						Content string `json:"content"`
					} `json:"body"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				postOnce.Do(func() { close(postStarted) })
				mu.Lock()
				posts++
				message := ChatMessage{
					ID:                   "owner-takeover-remote",
					MessageType:          "message",
					CreatedDateTime:      time.Now().UTC().Format(time.RFC3339Nano),
					LastModifiedDateTime: time.Now().UTC().Format(time.RFC3339Nano),
				}
				message.ChatID = "chat-1"
				message.Body.ContentType = "html"
				message.Body.Content = payload.Body.Content
				message.From.User = &struct {
					ID          string `json:"id"`
					DisplayName string `json:"displayName"`
				}{ID: "user-1", DisplayName: "User"}
				remote = append(remote, message)
				mu.Unlock()
				select {
				case <-releasePost:
				case <-r.Context().Done():
					return
				}
				_ = json.NewEncoder(w).Encode(message)
			}))
			t.Cleanup(server.Close)
			graph := &GraphClient{
				auth:       &fakeGraphAuth{token: "access"},
				client:     server.Client(),
				baseURL:    server.URL,
				maxRetries: 0,
				sleep:      sleepContext,
				jitter:     func(d time.Duration) time.Duration { return d },
			}
			store := newBridgeTestStore(t)
			bridgeA := newBridgeTestBridge(graph, store, &recordingExecutor{})
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate to SQLite: %v", err)
				}
			}
			now := time.Now().UTC()
			ownerA, err := teamstore.CurrentOwner("owner-takeover-test", "", "", now)
			if err != nil {
				t.Fatalf("current owner A: %v", err)
			}
			leaseA, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
				Scope:    bridgeA.scope,
				Machine:  bridgeA.machine,
				Owner:    ownerA,
				Duration: time.Minute,
				Now:      now,
			})
			if err != nil || leaseA.Mode != teamstore.LeaseModeActive {
				t.Fatalf("claim owner A: mode=%v err=%v", leaseA.Mode, err)
			}
			bridgeA.setControlLease(leaseA.Lease)
			queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
				ID:          "outbox:owner-takeover",
				TeamsChatID: "chat-1",
				Kind:        "helper",
				Body:        "OWNER_TAKEOVER_POST",
			})
			if err != nil {
				t.Fatalf("queue outbox: %v", err)
			}
			oldDone := make(chan error, 1)
			go func() {
				oldDone <- bridgeA.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{IgnoreEarlierOutbox: true})
			}()
			select {
			case <-postStarted:
			case <-time.After(listenerRecoveryProgressTimeout):
				close(releasePost)
				<-oldDone
				t.Fatal("owner A did not reach Graph POST")
			}

			machineB := bridgeA.machine
			machineB.ID += "-takeover"
			machineB.Status = teamstore.MachineStatusStandby
			ownerB, err := teamstore.CurrentOwner("owner-takeover-test", "", "", now.Add(2*time.Minute))
			if err != nil {
				t.Fatalf("current owner B: %v", err)
			}
			leaseB, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
				Scope:    bridgeA.scope,
				Machine:  machineB,
				Owner:    ownerB,
				Duration: time.Minute,
				Now:      now.Add(2 * time.Minute),
			})
			if err != nil || leaseB.Mode != teamstore.LeaseModeActive {
				t.Fatalf("claim owner B: mode=%v err=%v", leaseB.Mode, err)
			}
			close(releasePost)
			if err := <-oldDone; !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
				t.Fatalf("owner A completion error = %v, want owner fence", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load state after owner A callback: %v", err)
			}
			held := state.OutboxMessages[queued.ID]
			if held.Status != teamstore.OutboxStatusSending || strings.TrimSpace(held.TeamsMessageID) != "" {
				t.Fatalf("owner A callback changed outbox after takeover: %#v", held)
			}
			oldAttemptToken := held.SendAttemptToken

			bridgeB := newBridgeTestBridge(graph, store, &recordingExecutor{})
			bridgeB.machine = machineB
			bridgeB.setControlLease(leaseB.Lease)
			bound, err := store.BindOutboxRecoveryAttemptForOwner(ctx, held.ID, machineB.ID, leaseB.Lease.Generation)
			if err != nil {
				t.Fatalf("bind owner B recovery attempt: %v", err)
			}
			if bound.SendAttemptToken == "" || bound.SendAttemptToken == oldAttemptToken {
				t.Fatalf("owner B recovery did not rotate attempt token: old=%q new=%q row=%#v", oldAttemptToken, bound.SendAttemptToken, bound)
			}
			if !bound.LastSendAttempt.Equal(held.LastSendAttempt) {
				t.Fatalf("owner B recovery rewrote original Graph attempt time: before=%s after=%s", held.LastSendAttempt, bound.LastSendAttempt)
			}
			if bound.ScopeID != bridgeA.scope.ID {
				t.Fatalf("owner B recovery did not bind outbox scope: got=%q want=%q", bound.ScopeID, bridgeA.scope.ID)
			}
			if _, err := store.MarkOutboxSendErrorForAttempt(ctx, held.ID, oldAttemptToken, "stale owner error"); !errors.Is(err, teamstore.ErrOutboxSendNotClaimed) {
				t.Fatalf("stale owner error = %v, want ErrOutboxSendNotClaimed", err)
			}
			if _, err := store.MarkOutboxAcceptedForAttempt(ctx, held.ID, oldAttemptToken, "stale-message"); !errors.Is(err, teamstore.ErrOutboxSendNotClaimed) {
				t.Fatalf("stale owner accepted callback = %v, want ErrOutboxSendNotClaimed", err)
			}
			if _, err := store.MarkOutboxSentForAttempt(ctx, held.ID, oldAttemptToken, "stale-message"); !errors.Is(err, teamstore.ErrOutboxSendNotClaimed) {
				t.Fatalf("stale owner sent callback = %v, want ErrOutboxSendNotClaimed", err)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("load state after stale owner callbacks: %v", err)
			}
			staleProtected := state.OutboxMessages[held.ID]
			if staleProtected.Status != teamstore.OutboxStatusSending || staleProtected.MachineID != machineB.ID || staleProtected.LeaseGeneration != leaseB.Lease.Generation || staleProtected.SendAttemptToken != bound.SendAttemptToken || strings.TrimSpace(staleProtected.TeamsMessageID) != "" {
				t.Fatalf("stale owner callback changed rebound outbox: %#v", staleProtected)
			}
			probeBudget := 1
			pageBudget := 2
			if err := bridgeB.sendQueuedOutboxWithOptions(ctx, bound, outboxSendOptions{
				IgnoreEarlierOutbox: true,
				RecoveryProbeBudget: &probeBudget,
				RecoveryPageBudget:  &pageBudget,
			}); err != nil {
				t.Fatalf("owner B exact recovery: %v", err)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("load state after owner B recovery: %v", err)
			}
			settled := state.OutboxMessages[queued.ID]
			if settled.Status != teamstore.OutboxStatusSent || strings.TrimSpace(settled.TeamsMessageID) != "owner-takeover-remote" {
				t.Fatalf("owner B did not settle exact remote message: %#v", settled)
			}
			mu.Lock()
			gotPosts := posts
			mu.Unlock()
			if gotPosts != 1 {
				t.Fatalf("Graph POST count = %d, want exactly one", gotPosts)
			}
		})
	}
}

// TestTeamsTranscriptCheckpointOwnerCapabilitySurvivesTakeover protects the
// other half of the delivery boundary: a slow transcript reader must not
// advance the physical checkpoint or append a ledger row after a replacement
// listener has acquired the control lease. Run it against both state backends
// because a JSON full-state CAS and a SQLite split transaction must enforce
// the same owner capability.
func TestTeamsTranscriptCheckpointOwnerCapabilitySurvivesTakeover(t *testing.T) {
	for _, sqliteMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%v", sqliteMode), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			now := time.Now().UTC()
			scope := teamstore.ScopeIdentity{ID: "scope:checkpoint-owner"}
			machineA := teamstore.MachineRecord{ID: "machine:checkpoint-owner-a", Kind: teamstore.MachineKindPrimary}
			machineB := teamstore.MachineRecord{ID: "machine:checkpoint-owner-b", Kind: teamstore.MachineKindEphemeral}
			ownerA, err := teamstore.CurrentOwner("checkpoint-owner-test", "", "", now)
			if err != nil {
				t.Fatalf("owner A: %v", err)
			}
			leaseA, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
				Scope: scope, Machine: machineA, Owner: ownerA, Duration: time.Minute, Now: now,
			})
			if err != nil || leaseA.Mode != teamstore.LeaseModeActive {
				t.Fatalf("claim owner A: mode=%v err=%v", leaseA.Mode, err)
			}

			checkpoint := teamstore.ImportCheckpoint{
				ID: "transcript:checkpoint-owner", SessionID: "session:checkpoint-owner",
				SourcePath: "/fixture/checkpoint-owner.jsonl", LastRecordID: "record-before",
				LastSourceLine: 1, LastOffset: 64, LastOffsetKnown: true, SourceSize: 64,
				Status: "complete",
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.Sessions[checkpoint.SessionID] = teamstore.SessionContext{ID: checkpoint.SessionID, Status: teamstore.SessionStatusActive}
				return nil
			}); err != nil {
				t.Fatalf("seed checkpoint owner session: %v", err)
			}
			ledger := teamstore.TranscriptLedgerRecord{
				ID: "ledger:checkpoint-owner:before", SessionID: checkpoint.SessionID,
				SourcePath: checkpoint.SourcePath, SourceLine: 1, SourceRecordID: checkpoint.LastRecordID,
			}
			if err := store.RecordTranscriptCheckpointForOwner(ctx, checkpoint, ledger, machineA.ID, leaseA.Lease.Generation); err != nil {
				t.Fatalf("record initial checkpoint: %v", err)
			}
			if sqliteMode {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate store to sqlite: %v", err)
				}
			}

			ownerB, err := teamstore.CurrentOwner("checkpoint-owner-test", "", "", now.Add(2*time.Minute))
			if err != nil {
				t.Fatalf("owner B: %v", err)
			}
			leaseB, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
				Scope: scope, Machine: machineB, Owner: ownerB, Duration: time.Minute, Now: now.Add(2 * time.Minute),
			})
			if err != nil || leaseB.Mode != teamstore.LeaseModeActive {
				t.Fatalf("claim owner B: mode=%v err=%v", leaseB.Mode, err)
			}

			stale := checkpoint
			stale.LastRecordID = "record-stale-owner"
			stale.LastSourceLine = 2
			stale.LastOffset = 128
			stale.SourceSize = 128
			staleLedger := ledger
			staleLedger.ID = "ledger:checkpoint-owner:stale"
			staleLedger.SourceRecordID = stale.LastRecordID
			if err := store.RecordTranscriptCheckpointForOwner(ctx, stale, staleLedger, machineA.ID, leaseA.Lease.Generation); !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
				t.Fatalf("stale checkpoint write error = %v, want owner fence", err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load after stale checkpoint: %v", err)
			}
			got := state.ImportCheckpoints[checkpoint.ID]
			if got.LastRecordID != checkpoint.LastRecordID || got.LastOffset != checkpoint.LastOffset {
				t.Fatalf("stale owner changed checkpoint: %#v", got)
			}
			if _, ok := state.TranscriptLedger[staleLedger.ID]; ok {
				t.Fatalf("stale owner appended ledger row: %#v", state.TranscriptLedger[staleLedger.ID])
			}

			staleOutbox := teamstore.OutboxMessage{
				ID: "outbox:checkpoint-owner:stale", SessionID: checkpoint.SessionID,
				TeamsChatID: "chat:checkpoint-owner", Kind: "status-progress", Body: "stale queue",
				MachineID: machineA.ID, LeaseGeneration: leaseA.Lease.Generation,
			}
			_, _, _, err = store.QueueTranscriptDeliveryOutbox(ctx, teamstore.TranscriptDeliveryQueueRequest{
				Message: staleOutbox,
				Delivery: teamstore.TranscriptDeliveryRecord{
					ID: "delivery:checkpoint-owner:stale", SessionID: checkpoint.SessionID,
					SourcePath: checkpoint.SourcePath, SourceLine: 2, SourceRecordID: "record-stale-queue",
				},
				Checkpoint: teamstore.ImportCheckpoint{ID: checkpoint.ID, SessionID: checkpoint.SessionID},
			})
			if !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
				t.Fatalf("stale owner queue error = %v, want owner fence", err)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("load after stale queue: %v", err)
			}
			if _, ok := state.OutboxMessages[staleOutbox.ID]; ok {
				t.Fatalf("stale owner queued transcript outbox: %#v", state.OutboxMessages[staleOutbox.ID])
			}
			if _, ok := state.TranscriptDeliveries["delivery:checkpoint-owner:stale"]; ok {
				t.Fatalf("stale owner appended transcript delivery: %#v", state.TranscriptDeliveries["delivery:checkpoint-owner:stale"])
			}
			staleDelivery := teamstore.TranscriptDeliveryRecord{
				ID: "delivery:checkpoint-owner:stale-audit", SessionID: checkpoint.SessionID,
				SourcePath: checkpoint.SourcePath, SourceLine: 2, SourceRecordID: "record-stale-audit",
			}
			_, _, err = store.RecordTranscriptDeliveryForOwner(ctx, staleDelivery, teamstore.ImportCheckpoint{ID: checkpoint.ID, SessionID: checkpoint.SessionID}, machineA.ID, leaseA.Lease.Generation)
			if !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
				t.Fatalf("stale owner delivery error = %v, want owner fence", err)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("load after stale delivery: %v", err)
			}
			if _, ok := state.TranscriptDeliveries[staleDelivery.ID]; ok {
				t.Fatalf("stale owner appended audit delivery: %#v", state.TranscriptDeliveries[staleDelivery.ID])
			}

			replacement := checkpoint
			replacement.LastRecordID = "record-after"
			replacement.LastSourceLine = 3
			replacement.LastOffset = 192
			replacement.SourceSize = 192
			replacementLedger := ledger
			replacementLedger.ID = "ledger:checkpoint-owner:after"
			replacementLedger.SourceRecordID = replacement.LastRecordID
			if err := store.RecordTranscriptCheckpointForOwner(ctx, replacement, replacementLedger, machineB.ID, leaseB.Lease.Generation); err != nil {
				t.Fatalf("replacement checkpoint write: %v", err)
			}
			state, err = store.Load(ctx)
			if err != nil {
				t.Fatalf("load after replacement checkpoint: %v", err)
			}
			got = state.ImportCheckpoints[checkpoint.ID]
			if got.LastRecordID != replacement.LastRecordID || got.LastOffset != replacement.LastOffset {
				t.Fatalf("replacement owner checkpoint = %#v", got)
			}
			if _, ok := state.TranscriptLedger[replacementLedger.ID]; !ok {
				t.Fatalf("replacement owner ledger missing: %#v", state.TranscriptLedger)
			}
		})
	}
}

// A definitive Graph rejection cannot be retried successfully by the normal
// sender.  Leaving that row queued is worse than the rejected message itself:
// same-chat FIFO then prevents every later answer from being sent forever.
// Retryable 429/5xx paths retain their existing retry/ambiguous semantics; this
// test is specifically for a response that proves this row will not be
// accepted as submitted.
func TestTeamsSameChatDefinitiveSendFailureDoesNotStarveLaterOutbox(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			var mu sync.Mutex
			var posts []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
					http.Error(w, "unexpected fake Graph request", http.StatusNotFound)
					return
				}
				var payload struct {
					Body struct {
						Content string `json:"content"`
					} `json:"body"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				mu.Lock()
				posts = append(posts, payload.Body.Content)
				mu.Unlock()
				if strings.Contains(payload.Body.Content, "LISTENER_TERMINAL_FAILURE") {
					http.Error(w, `{"error":{"code":"BadRequest","message":"payload is permanently invalid"}}`, http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"id": "later-sent", "messageType": "message"})
			}))
			t.Cleanup(server.Close)
			graph := &GraphClient{
				auth:       &fakeGraphAuth{token: "access"},
				client:     server.Client(),
				baseURL:    server.URL,
				maxRetries: 0,
				sleep:      sleepContext,
				jitter:     func(d time.Duration) time.Duration { return d },
			}
			store := newBridgeTestStore(t)
			_, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
				ID:          "outbox:listener-terminal-failure",
				TeamsChatID: "chat-1",
				Kind:        "helper",
				Body:        "LISTENER_TERMINAL_FAILURE",
			})
			if err != nil {
				t.Fatalf("queue first outbox: %v", err)
			}
			_, _, err = store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
				ID:          "outbox:listener-later",
				TeamsChatID: "chat-1",
				Kind:        "helper",
				Body:        "LISTENER_LATER_DELIVERY",
			})
			if err != nil {
				t.Fatalf("queue later outbox: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
					t.Fatalf("migrate to SQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			err = bridge.flushPendingOutboxForChat(context.Background(), "chat-1")
			if err == nil {
				t.Fatal("flush after definitive failure returned nil; want diagnostic error")
			}
			state, err := store.Load(context.Background())
			if err != nil {
				t.Fatalf("load state after definitive failure: %v", err)
			}
			failed := state.OutboxMessages["outbox:listener-terminal-failure"]
			if failed.Status != teamstore.OutboxStatusSkipped {
				t.Fatalf("definitive failure status = %q, want skipped terminal disposition: %#v", failed.Status, failed)
			}
			if got := state.OutboxMessages["outbox:listener-later"].Status; got != teamstore.OutboxStatusSent {
				t.Fatalf("later same-chat outbox status = %q, want sent; state=%#v", got, state.OutboxMessages)
			}
			mu.Lock()
			gotPosts := append([]string(nil), posts...)
			mu.Unlock()
			if len(gotPosts) != 2 || !strings.Contains(gotPosts[0], "LISTENER_TERMINAL_FAILURE") || !strings.Contains(gotPosts[1], "LISTENER_LATER_DELIVERY") {
				t.Fatalf("Graph posts = %#v, want rejected first then delivered later row", gotPosts)
			}
		})
	}
}

// TestTeamsLegacyZeroTimestampSendingRecoveryReachesExactMarker covers a
// pre-attempt-token row whose durable LastSendAttempt was never written. The
// row is still an unknown Graph outcome: recovery may only settle it after
// finding the exact helper marker, and must never issue a replacement POST.
// Both store backends must expose the adopted row to the cold recovery lane;
// otherwise owner adoption would make this legacy row permanently invisible.
func TestTeamsLegacyZeroTimestampSendingRecoveryReachesExactMarker(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate legacy zero-time store: %v", err)
				}
			}
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			now := time.Now().UTC()
			const outboxID = "outbox:legacy-zero-time-recovery"
			legacy := teamstore.OutboxMessage{
				ID: outboxID, SessionID: "s001", TurnID: "turn:legacy-zero-time",
				TeamsChatID: "chat-1", Kind: "helper",
				Body:      "LISTENER_LEGACY_ZERO_TIME_RECOVERY",
				Status:    teamstore.OutboxStatusSending,
				CreatedAt: now.Add(-10 * time.Minute),
				UpdatedAt: now.Add(-10 * time.Minute),
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.OutboxMessages[outboxID] = legacy
				return nil
			}); err != nil {
				t.Fatalf("seed legacy zero-time row: %v", err)
			}
			lease, err := store.ClaimControlLease(ctx, teamstore.ControlLeaseClaim{
				Scope: bridge.scope, Machine: bridge.machine, Duration: time.Hour, Now: now,
			})
			if err != nil || lease.Mode != teamstore.LeaseModeActive {
				t.Fatalf("claim recovery owner: mode=%v err=%v", lease.Mode, err)
			}
			bridge.setControlLease(lease.Lease)
			bound, err := store.BindOutboxRecoveryAttemptForOwner(ctx, outboxID, bridge.machine.ID, lease.Lease.Generation)
			if err != nil {
				t.Fatalf("bind legacy zero-time row: %v", err)
			}
			if !bound.LastSendAttempt.IsZero() {
				t.Fatalf("legacy recovery rewrote zero attempt time: %#v", bound)
			}
			if !teamstore.OutboxSendIsAmbiguous(bound) || bound.SendAttemptToken == "" {
				t.Fatalf("legacy recovery row is not explicitly ambiguous: %#v", bound)
			}
			pending, err := store.PendingOutboxPageAt(ctx, teamstore.PendingOutboxQuery{
				Now: now.Add(time.Minute), IncludeAmbiguous: true, AmbiguousOnly: true, Limit: 8,
			})
			if err != nil {
				t.Fatalf("query legacy recovery lane: %v", err)
			}
			if len(pending.Messages) != 1 || pending.Messages[0].ID != outboxID {
				t.Fatalf("legacy zero-time recovery lane = %#v, want %q", pending.Messages, outboxID)
			}

			accepted := ownershipStressAcceptedOutboxMessage("legacy-zero-time-remote", legacy.Body)
			accepted.ChatID = legacy.TeamsChatID
			accepted.Body.Content = renderOutboxHTML(bound) + helperOutboxProvenanceMarker(outboxID)
			accepted.CreatedDateTime = now.Add(-9 * time.Minute).Format(time.RFC3339Nano)
			accepted.LastModifiedDateTime = accepted.CreatedDateTime
			var posts int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts++
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "duplicate-legacy-zero-time", "messageType": "message"})
					return
				}
				if r.Method != http.MethodGet || r.URL.Path != "/chats/chat-1/messages" {
					http.Error(w, "unexpected Graph request", http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"value": []ChatMessage{accepted}})
			}))
			t.Cleanup(server.Close)
			bridge.graph = &GraphClient{
				auth: &fakeGraphAuth{token: "access"}, client: server.Client(), baseURL: server.URL,
				maxRetries: 0, sleep: sleepContext, jitter: func(d time.Duration) time.Duration { return d },
			}
			pageBudget := 2
			handled, err := bridge.recoverAcceptedOutboxFromGraph(ctx, bound, outboxSendOptions{RecoveryPageBudget: &pageBudget})
			if !handled || err != nil {
				t.Fatalf("legacy zero-time exact recovery = handled %v err %v", handled, err)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load recovered legacy zero-time row: %v", err)
			}
			recovered := state.OutboxMessages[outboxID]
			if recovered.Status != teamstore.OutboxStatusSent || recovered.TeamsMessageID != accepted.ID || posts != 0 {
				t.Fatalf("legacy zero-time recovery = %#v posts=%d, want Sent exact marker without POST", recovered, posts)
			}
		})
	}
}

// TestImportCheckpointComparisonIncludesAllProgressFields prevents newly
// added partial/opaque cursor fields from being silently treated as an
// UpdatedAt-only change. A missed comparison causes a scanner's durable
// progress update to be dropped and makes it reread the same range forever.
func TestImportCheckpointComparisonIncludesAllProgressFields(t *testing.T) {
	base := teamstore.ImportCheckpoint{ID: "checkpoint:compare", SessionID: "s001", UpdatedAt: time.Unix(10, 0).UTC()}
	updatedOnly := base
	updatedOnly.UpdatedAt = time.Unix(20, 0).UTC()
	if !importCheckpointSameExceptUpdatedAt(base, updatedOnly) {
		t.Fatal("UpdatedAt-only checkpoint change was treated as a semantic change")
	}
	cases := []struct {
		name   string
		change func(*teamstore.ImportCheckpoint)
	}{
		{"source_change_time", func(c *teamstore.ImportCheckpoint) { c.SourceChangeTime = 1 }},
		{"partial_line_start", func(c *teamstore.ImportCheckpoint) { c.PartialLineStartOffset = 1 }},
		{"partial_read_offset", func(c *teamstore.ImportCheckpoint) { c.PartialReadOffset = 2 }},
		{"partial_observed_size", func(c *teamstore.ImportCheckpoint) { c.PartialObservedSize = 3 }},
		{"partial_line", func(c *teamstore.ImportCheckpoint) { c.PartialLine = 4 }},
		{"partial_started_at", func(c *teamstore.ImportCheckpoint) { c.PartialStartedAt = time.Unix(30, 0).UTC() }},
		{"partial_source_identity", func(c *teamstore.ImportCheckpoint) { c.PartialSourceIdentity = "source:partial" }},
		{"partial_source_change_time", func(c *teamstore.ImportCheckpoint) { c.PartialSourceChangeTime = 5 }},
		{"partial_replay_offset", func(c *teamstore.ImportCheckpoint) { c.PartialReplayOffset = 6 }},
		{"partial_replay_line", func(c *teamstore.ImportCheckpoint) { c.PartialReplayLine = 7 }},
		{"partial_last_progress_at", func(c *teamstore.ImportCheckpoint) { c.PartialLastProgressAt = time.Unix(40, 0).UTC() }},
		{"partial_prefix_released", func(c *teamstore.ImportCheckpoint) { c.PartialPrefixReleased = true }},
		{"pending_opaque_start", func(c *teamstore.ImportCheckpoint) { c.PendingOpaqueRecordStartOffset = 8 }},
		{"pending_opaque_end", func(c *teamstore.ImportCheckpoint) { c.PendingOpaqueRecordEndOffset = 9 }},
		{"pending_opaque_line", func(c *teamstore.ImportCheckpoint) { c.PendingOpaqueRecordLine = 10 }},
		{"pending_opaque_id", func(c *teamstore.ImportCheckpoint) { c.PendingOpaqueRecordID = "opaque:record" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := base
			tc.change(&changed)
			if importCheckpointSameExceptUpdatedAt(base, changed) {
				t.Fatalf("checkpoint field %s was ignored", tc.name)
			}
		})
	}
}

// TestTeamsSentSideEffectMarkerFailureGetsDurableRetryGate models a transient
// failure of the final local CAS after all post-send effects have completed.
// The Graph identity is already Sent, so the marker must remain pending but
// become temporarily invisible to the replay sweep; the next due pass must
// clear it without another Graph POST.
func TestTeamsSentSideEffectMarkerFailureGetsDurableRetryGate(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	now := time.Now().UTC()
	const outboxID = "outbox:post-send-marker-cas-retry"
	row := teamstore.OutboxMessage{
		ID: outboxID, TeamsChatID: "chat-1", Kind: "helper",
		Body: "post-send effects", Status: teamstore.OutboxStatusSent,
		TeamsMessageID: "teams-post-send-effects", PostSendEffectsPending: true,
		CreatedAt: now.Add(-time.Minute),
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.OutboxMessages[outboxID] = row
		return nil
	}); err != nil {
		t.Fatalf("seed sent row: %v", err)
	}
	failOnce := true
	bridge.outboxSideEffectsCompleteHook = func(context.Context, teamstore.OutboxMessage) error {
		if failOnce {
			failOnce = false
			return errors.New("injected final marker CAS failure")
		}
		return nil
	}
	if err := bridge.completeSentOutboxSideEffects(ctx, row, ChatMessage{ID: row.TeamsMessageID}, false); err == nil {
		t.Fatal("first post-send completion unexpectedly succeeded")
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load after marker CAS failure: %v", err)
	}
	deferred := state.OutboxMessages[outboxID]
	if !deferred.PostSendEffectsPending || !deferred.NextAttemptAt.After(now) {
		t.Fatalf("marker CAS failure did not create a durable retry gate: %#v", deferred)
	}
	pending, err := store.PendingSentOutboxSideEffects(ctx, 8)
	if err != nil {
		t.Fatalf("pending side effects after marker CAS failure: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("gated marker remained hot: %#v", pending)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		current := state.OutboxMessages[outboxID]
		current.NextAttemptAt = now.Add(-time.Second)
		state.OutboxMessages[outboxID] = current
		return nil
	}); err != nil {
		t.Fatalf("make side-effect retry due: %v", err)
	}
	if err := bridge.reconcilePendingSentOutboxSideEffects(ctx); err != nil {
		t.Fatalf("reconcile post-send marker after retry gate: %v", err)
	}
	state, err = store.Load(ctx)
	if err != nil {
		t.Fatalf("load after marker retry: %v", err)
	}
	completed := state.OutboxMessages[outboxID]
	if completed.Status != teamstore.OutboxStatusSent || completed.PostSendEffectsPending || completed.TeamsMessageID != row.TeamsMessageID {
		t.Fatalf("post-send marker was not cleared on retry: %#v", completed)
	}
}

func countListenerRecoveryInbound(state teamstore.State, sessionID string, messageID string) int {
	count := 0
	for _, inbound := range state.InboundEvents {
		if inbound.SessionID == sessionID && inbound.TeamsMessageID == messageID {
			count++
		}
	}
	return count
}

func countListenerRecoverySentOutbox(state teamstore.State) int {
	count := 0
	for _, outbox := range state.OutboxMessages {
		if strings.HasPrefix(outbox.ID, "outbox:listener-reopen:") && outbox.Status == teamstore.OutboxStatusSent {
			count++
		}
	}
	return count
}
