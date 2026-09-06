package teams

import (
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
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	dockerRealDataExperimentEnv   = "CXP_TEAMS_DOCKER_REAL_DATA_EXPERIMENT"
	dockerRealDataDurationEnv     = "CXP_TEAMS_DOCKER_REAL_DATA_DURATION"
	dockerRealDataPageSize        = 20
	dockerRealDataMessageIDPrefix = "docker-real-data:"
	dockerRealDataDefaultDuration = 90 * time.Second
	dockerRealDataMinimumDuration = 15 * time.Second
	dockerRealDataDefaultTop      = ownerPollMessageTop
	dockerRealDataMinimumReplay   = 100
)

// dockerRealDataExecutor is deliberately not a Codex smoke stub. It is the
// narrow no-network execution boundary needed to let the copied production
// Teams queue complete without launching Codex or requiring a Codex API key.
// Every input still traverses the real durable inbound -> turn -> outbox path.
type dockerRealDataExecutor struct {
	runs      atomic.Int64
	active    atomic.Int64
	stopping  atomic.Bool
	holdFirst time.Duration
	firstMu   sync.Mutex
	first     time.Time
}

func (e *dockerRealDataExecutor) Run(ctx context.Context, session *Session, _ string) (ExecutionResult, error) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ExecutionResult{}, ctx.Err()
		default:
		}
	}
	n := e.runs.Add(1)
	e.active.Add(1)
	defer e.active.Add(-1)
	e.firstMu.Lock()
	if e.first.IsZero() {
		e.first = time.Now()
	}
	e.firstMu.Unlock()
	if n == 1 && e.holdFirst > 0 && !e.stopping.Load() {
		timer := time.NewTimer(e.holdFirst)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ExecutionResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	threadID := "docker-real-data-thread"
	if session != nil && strings.TrimSpace(session.CodexThreadID) != "" {
		threadID = session.CodexThreadID
	}
	return ExecutionResult{
		Text:                     "docker real-data execution result",
		CodexThreadID:            threadID,
		CodexTurnID:              fmt.Sprintf("docker-real-data-turn-%d", n),
		canonicalTranscriptFinal: true,
	}, nil
}

func (e *dockerRealDataExecutor) requestGracefulStop() {
	if e == nil {
		return
	}
	e.stopping.Store(true)
}

func (e *dockerRealDataExecutor) activeRuns() int64 {
	if e == nil {
		return 0
	}
	return e.active.Load()
}

func (e *dockerRealDataExecutor) resetGracefulStop() {
	if e == nil {
		return
	}
	// The experiment reopens the disposable store with a replacement listener;
	// the executor is shared only so its run accounting remains continuous.
	e.stopping.Store(false)
}

func (e *dockerRealDataExecutor) firstRunAt() time.Time {
	e.firstMu.Lock()
	defer e.firstMu.Unlock()
	return e.first
}

// stopDockerRealDataAsyncAdmission closes only the in-process admission gate.
// Already-admitted workers retain the listener lifecycle context long enough to
// commit their durable completion; the coordinator cancels Listen only after
// the idle signal. This keeps the experiment's stop boundary from manufacturing
// an Interrupted row that a real graceful restart would not need to create.
func stopDockerRealDataAsyncAdmission(bridge *Bridge) <-chan struct{} {
	if bridge == nil {
		idle := make(chan struct{})
		close(idle)
		return idle
	}
	bridge.asyncTurnStateMu.Lock()
	bridge.asyncTurnStopping = true
	idle := bridge.asyncTurnIdle
	if idle == nil {
		idle = make(chan struct{})
		close(idle)
	}
	bridge.asyncTurnStateMu.Unlock()
	return idle
}

// dockerRealDataReplayCorpus is built from the copied production inbound
// rows. The message IDs and timestamps are remapped only because the original
// rows are already durable in the copied store and would therefore be correctly
// deduplicated. Bodies, authors, chat/session distribution, and queued-status
// selection remain from the real snapshot; attachment rows are excluded from
// this ordinary-message throughput lane because they require separate hosted
// content/file materialization.
func dockerRealDataReplayCorpus(state teamstore.State, controlChatID string) (map[string][]ChatMessage, int) {
	activeChats := make(map[string]struct{}, len(state.Sessions))
	for _, session := range state.Sessions {
		chatID := strings.TrimSpace(session.TeamsChatID)
		if chatID == "" || chatID == strings.TrimSpace(controlChatID) || !isActiveSessionStatus(string(session.Status)) {
			continue
		}
		activeChats[chatID] = struct{}{}
	}
	type replaySource struct {
		chatID string
		id     string
		msg    ChatMessage
		at     time.Time
	}
	byChat := make(map[string][]replaySource)
	for _, inbound := range state.InboundEvents {
		if !strings.EqualFold(strings.TrimSpace(inbound.Source), "teams") || inbound.Status != teamstore.InboundStatusQueued {
			continue
		}
		chatID := strings.TrimSpace(inbound.TeamsChatID)
		if _, ok := activeChats[chatID]; !ok {
			continue
		}
		msg, ok := chatMessageFromInboundContext(inbound)
		if !ok || strings.TrimSpace(inbound.ID) == "" {
			continue
		}
		// Do not replay dashboard commands: those would intentionally mutate the
		// disposable fixture instead of measuring ordinary message admission.
		if parsed := ParseDashboardCommand(ChatScopeWork, strings.TrimSpace(inbound.Text)); parsed.HelperCommand {
			continue
		}
		if strings.TrimSpace(inbound.Text) == "" && strings.TrimSpace(msg.Body.Content) == "" {
			continue
		}
		if msg.Body.Content == "" {
			msg.Body.ContentType = "html"
			msg.Body.Content = inbound.Text
		}
		// Hosted-content and message-reference rows need additional Graph
		// endpoints and file materialization. Keep this experiment focused on the
		// ordinary message admission path while retaining the real bodies,
		// authors, chat distribution, and durable queued-row selection.
		if len(msg.Attachments) > 0 || len(HostedContentIDsFromHTML(msg.Body.Content)) > 0 {
			continue
		}
		// The source row often stores the normalized prompt without the native
		// Teams mention. Add only the routing marker required to pass the same
		// multi-member work-chat gate; the prompt text itself is unchanged.
		if !teamsMessageHasCodexMention(msg, inbound.Text) {
			msg.Body.Content = "@codex " + msg.Body.Content
		}
		authorID := strings.TrimSpace(inbound.AuthorUserID)
		if authorID == "" {
			authorID = "docker-real-data-replay-user"
		}
		msg.From.User = &struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		}{ID: authorID, DisplayName: firstNonEmptyString(inbound.AuthorName, "Real Teams replay user")}
		msg.MessageType = "message"
		msg.ChatID = chatID
		at := inbound.ReceivedAt
		if at.IsZero() {
			at = inbound.CreatedAt
		}
		byChat[chatID] = append(byChat[chatID], replaySource{chatID: chatID, id: inbound.ID, msg: msg, at: at})
	}

	base := time.Now().UTC().Add(2 * time.Second)
	corpus := make(map[string][]ChatMessage, len(byChat))
	total := 0
	for chatID, sources := range byChat {
		sort.SliceStable(sources, func(i, j int) bool {
			if !sources[i].at.Equal(sources[j].at) {
				return sources[i].at.Before(sources[j].at)
			}
			return sources[i].id < sources[j].id
		})
		messages := make([]ChatMessage, 0, len(sources))
		for index, source := range sources {
			// Keep the real message order while placing the replay after the
			// copied poll cursor, so the production cursor reducer sees it as a
			// fresh Graph backlog rather than as a duplicate historical page.
			at := base.Add(time.Duration(total+index) * time.Millisecond)
			source.msg.ID = fmt.Sprintf("%sreal:%s", dockerRealDataMessageIDPrefix, shortStableID(source.id))
			source.msg.CreatedDateTime = at.Format(time.RFC3339Nano)
			source.msg.LastModifiedDateTime = at.Format(time.RFC3339Nano)
			messages = append(messages, source.msg)
		}
		corpus[chatID] = messages
		total += len(messages)
	}
	return corpus, total
}

type dockerRealDataTraceWriter struct {
	mu            sync.Mutex
	listeningAt   time.Time
	lines         []string
	events        []string
	leaseClaims   []string
	ownerFailures []string
}

func (w *dockerRealDataTraceWriter) Write(p []byte) (int, error) {
	if w == nil {
		return len(p), nil
	}
	w.mu.Lock()
	if w.listeningAt.IsZero() && strings.Contains(string(p), "Listening.") {
		w.listeningAt = time.Now()
	}
	message := strings.TrimSpace(string(p))
	if message != "" && len(w.lines) < 256 {
		w.lines = append(w.lines, message)
	}
	if strings.Contains(strings.ToLower(message), "standby") || strings.Contains(strings.ToLower(message), "acquired control lease") || strings.Contains(strings.ToLower(message), "heartbeat") {
		if len(w.events) < 64 {
			w.events = append(w.events, message)
		}
	}
	w.mu.Unlock()
	return len(p), nil
}

func (w *dockerRealDataTraceWriter) linesSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.lines...)
}

func (w *dockerRealDataTraceWriter) listeningTime() time.Time {
	if w == nil {
		return time.Time{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.listeningAt
}

func (w *dockerRealDataTraceWriter) eventsSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.events...)
}

func (w *dockerRealDataTraceWriter) recordLeaseClaim(decision teamstore.ControlLeaseDecision, err error) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.leaseClaims) >= 32 {
		return
	}
	w.leaseClaims = append(w.leaseClaims, fmt.Sprintf("mode=%s generation=%d holder=%s reason=%q err=%v", decision.Mode, decision.Lease.Generation, decision.Lease.HolderMachineID, decision.Reason, err))
}

func (w *dockerRealDataTraceWriter) leaseClaimsSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.leaseClaims...)
}

func (w *dockerRealDataTraceWriter) recordOwnerFailure(err error) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.ownerFailures) < 16 {
		w.ownerFailures = append(w.ownerFailures, fmt.Sprint(err))
	}
}

func (w *dockerRealDataTraceWriter) ownerFailuresSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.ownerFailures...)
}

// dockerRealDataOwnerTrace observes only the in-memory lease generation. It
// does not call Store.Load or open a second SQLite connection, so measuring
// ownership cannot itself recreate the full-state read/lock pressure under
// test. A generation change is a real listener takeover/reclaim boundary.
type dockerRealDataOwnerTrace struct {
	bridge *Bridge
	stop   chan struct{}
	done   chan struct{}

	mu          sync.Mutex
	generations []int64
	seen        map[int64]struct{}
	changes     int
	last        int64
	stopOnce    sync.Once
}

func startDockerRealDataOwnerTrace(bridge *Bridge) *dockerRealDataOwnerTrace {
	trace := &dockerRealDataOwnerTrace{
		bridge: bridge,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		seen:   make(map[int64]struct{}),
	}
	go func() {
		defer close(trace.done)
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		trace.observe()
		for {
			select {
			case <-trace.stop:
				return
			case <-ticker.C:
				trace.observe()
			}
		}
	}()
	return trace
}

func (t *dockerRealDataOwnerTrace) observe() {
	if t == nil || t.bridge == nil {
		return
	}
	generation := t.bridge.currentLeaseGeneration()
	if generation <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.last > 0 && t.last != generation {
		t.changes++
	}
	t.last = generation
	if _, ok := t.seen[generation]; !ok {
		t.seen[generation] = struct{}{}
		t.generations = append(t.generations, generation)
	}
}

func (t *dockerRealDataOwnerTrace) stopTrace() {
	if t == nil {
		return
	}
	t.stopOnce.Do(func() { close(t.stop) })
	<-t.done
	// Capture a final generation in case the listener stopped between ticker
	// ticks, which is exactly when a short-lived takeover is easiest to miss.
	t.observe()
}

func (t *dockerRealDataOwnerTrace) snapshot() (generations []int64, changes int) {
	if t == nil {
		return nil, 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]int64(nil), t.generations...), t.changes
}

type dockerRealDataMaintenanceTrace struct {
	store            *teamstore.Store
	historyMandatory map[string]bool
	linkedMandatory  map[string]bool

	mu                          sync.Mutex
	backlogSamples              int
	ordinaryHistoryWhileBacklog int
	ordinaryLinkedWhileBacklog  int
	ordinaryHistoryPaths        []string
	ordinaryLinkedSessions      []string
}

func (t *dockerRealDataMaintenanceTrace) observeBacklog(ctx context.Context) (teamstore.TeamsOperationalBacklog, error) {
	if t == nil || t.store == nil {
		return teamstore.TeamsOperationalBacklog{}, nil
	}
	backlog, err := t.store.TeamsOperationalBacklog(ctx)
	if err != nil {
		return teamstore.TeamsOperationalBacklog{}, err
	}
	if backlog.Active() {
		t.mu.Lock()
		t.backlogSamples++
		t.mu.Unlock()
	}
	return backlog, nil
}

func (t *dockerRealDataMaintenanceTrace) observeHistory(ctx context.Context, path string) error {
	backlog, err := t.observeBacklog(ctx)
	if err != nil || !backlog.Active() || t == nil || t.store == nil {
		return err
	}
	if !t.historyMandatory[historyWatchCheckpointID(path)] {
		t.mu.Lock()
		t.ordinaryHistoryWhileBacklog++
		if len(t.ordinaryHistoryPaths) < 16 {
			t.ordinaryHistoryPaths = append(t.ordinaryHistoryPaths, path)
		}
		t.mu.Unlock()
	}
	return nil
}

func (t *dockerRealDataMaintenanceTrace) observeLinked(ctx context.Context, session Session) error {
	backlog, err := t.observeBacklog(ctx)
	if err != nil || !backlog.Active() || t == nil || t.store == nil {
		return err
	}
	mandatory := t.linkedMandatory[transcriptCheckpointID(session.ID)]
	if !mandatory {
		checkpoint, found, checkpointErr := t.store.ImportCheckpoint(ctx, transcriptCheckpointID(session.ID))
		if checkpointErr != nil {
			return checkpointErr
		}
		mandatory = found && linkedTranscriptCheckpointNeedsMandatoryMaintenance(checkpoint)
	}
	if !mandatory {
		t.mu.Lock()
		t.ordinaryLinkedWhileBacklog++
		if len(t.ordinaryLinkedSessions) < 16 {
			t.ordinaryLinkedSessions = append(t.ordinaryLinkedSessions, session.ID)
		}
		t.mu.Unlock()
	}
	return nil
}

func (t *dockerRealDataMaintenanceTrace) snapshot() (backlogSamples, ordinaryHistory, ordinaryLinked int) {
	if t == nil {
		return 0, 0, 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.backlogSamples, t.ordinaryHistoryWhileBacklog, t.ordinaryLinkedWhileBacklog
}

func (t *dockerRealDataMaintenanceTrace) ordinaryWorkSnapshot() (historyPaths, linkedSessions []string) {
	if t == nil {
		return nil, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.ordinaryHistoryPaths...), append([]string(nil), t.ordinaryLinkedSessions...)
}

// dockerRealDataGraphServer replays pages at the Graph boundary only. It
// intentionally accepts the real chat IDs/frontier paths from the copied
// SQLite state. Message bodies/authors/order come from the copied durable
// queued rows; only IDs/timestamps are remapped to make them fresh Graph
// input, and POST results are local. The Docker runner places the whole
// process in --network=none, so no request can reach Microsoft or any other
// external service.
type dockerRealDataGraphServer struct {
	token         string
	user          User
	controlChatID string
	base          time.Time
	replay        map[string][]ChatMessage

	mu             sync.Mutex
	headPage       map[string]int
	listPaths      []string
	unknownPath    []string
	postAttempts   map[string]int
	unknownPostKey string
	faultChatID    string
	faultSequence  []int

	listGETs           atomic.Int64
	itemGETs           atomic.Int64
	posts              atomic.Int64
	status429          atomic.Int64
	status503          atomic.Int64
	unknownPosts       atomic.Int64
	unknownPostRepeats atomic.Int64
}

func newDockerRealDataGraphServer(token string, user User, replay map[string][]ChatMessage) *dockerRealDataGraphServer {
	return &dockerRealDataGraphServer{
		token:        token,
		user:         user,
		base:         time.Now().UTC().Add(2 * time.Second),
		replay:       replay,
		headPage:     make(map[string]int),
		postAttempts: make(map[string]int),
	}
}

func (g *dockerRealDataGraphServer) setFaultChat(chatID string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.faultChatID = strings.TrimSpace(chatID)
	g.faultSequence = []int{http.StatusTooManyRequests, http.StatusServiceUnavailable}
	g.mu.Unlock()
}

func (g *dockerRealDataGraphServer) consumeListFault(chatID string) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if strings.TrimSpace(chatID) == "" || strings.TrimSpace(chatID) == strings.TrimSpace(g.controlChatID) || len(g.faultSequence) == 0 {
		return 0
	}
	status := g.faultSequence[0]
	g.faultSequence = g.faultSequence[1:]
	return status
}

func (g *dockerRealDataGraphServer) recordUnknown(path string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.unknownPath) < 32 {
		g.unknownPath = append(g.unknownPath, path)
	}
}

func (g *dockerRealDataGraphServer) unknownPaths() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.unknownPath...)
}

func (g *dockerRealDataGraphServer) listRequests() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.listPaths...)
}

func (g *dockerRealDataGraphServer) pageForRequest(chatID string, req *http.Request) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	page := g.headPage[chatID]
	if raw := strings.TrimSpace(req.URL.Query().Get("cxp_replay_page")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			page = parsed
			if g.headPage[chatID] <= parsed {
				g.headPage[chatID] = parsed + 1
			}
		}
	} else {
		g.headPage[chatID] = page + 1
	}
	return page
}

func dockerRealDataMessage(chatID string, page int, index int, at time.Time) ChatMessage {
	messageTime := at.Add(time.Duration(page*dockerRealDataPageSize+index) * time.Millisecond)
	message := ChatMessage{
		ID:                   fmt.Sprintf("%s%s:%06d", dockerRealDataMessageIDPrefix, shortStableID(chatID), page*dockerRealDataPageSize+index),
		ChatID:               chatID,
		CreatedDateTime:      messageTime.Format(time.RFC3339Nano),
		LastModifiedDateTime: messageTime.Format(time.RFC3339Nano),
		MessageType:          "message",
	}
	message.From.User = &struct {
		ID          string `json:"id"`
		DisplayName string `json:"displayName"`
	}{ID: "docker-real-data-external-user", DisplayName: "Docker real-data replay"}
	message.Body.ContentType = "html"
	message.Body.Content = fmt.Sprintf("<p>@codex docker real-data replay page %d message %d</p>", page+1, index+1)
	return message
}

func writeDockerRealDataJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func dockerRealDataChatItemPath(path string) (chatID string, messageID string, ok bool) {
	const prefix = "/chats/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(path, prefix)
	parts := strings.Split(rest, "/")
	if len(parts) < 3 || parts[1] != "messages" || parts[2] == "" {
		return "", "", false
	}
	chatID, err := url.PathUnescape(parts[0])
	if err != nil || strings.TrimSpace(chatID) == "" {
		return "", "", false
	}
	messageID, err = url.PathUnescape(parts[2])
	if err != nil || strings.TrimSpace(messageID) == "" {
		return "", "", false
	}
	return chatID, messageID, true
}

func (g *dockerRealDataGraphServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Header.Get("Authorization") != "Bearer "+g.token {
		http.Error(w, "deterministic Docker Graph token required", http.StatusUnauthorized)
		return
	}
	path := req.URL.Path
	if chatID, list := dockerFixtureChatIDFromMessagesPath(path); list && req.Method == http.MethodGet {
		g.listGETs.Add(1)
		g.mu.Lock()
		if len(g.listPaths) < 64 {
			g.listPaths = append(g.listPaths, req.URL.RequestURI())
		}
		g.mu.Unlock()
		if chatID == "" {
			writeDockerRealDataJSON(w, struct {
				Value []ChatMessage `json:"value"`
			}{Value: []ChatMessage{}})
			return
		}
		if chatID == strings.TrimSpace(g.controlChatID) {
			writeDockerRealDataJSON(w, struct {
				Value []ChatMessage `json:"value"`
			}{Value: []ChatMessage{}})
			return
		}
		if status := g.consumeListFault(chatID); status != 0 {
			w.Header().Set("Retry-After", "1")
			if status == http.StatusTooManyRequests {
				g.status429.Add(1)
			} else if status == http.StatusServiceUnavailable {
				g.status503.Add(1)
			}
			w.WriteHeader(status)
			return
		}
		page := g.pageForRequest(chatID, req)
		corpus := g.replay[chatID]
		start := page * dockerRealDataPageSize
		if start >= len(corpus) {
			writeDockerRealDataJSON(w, struct {
				Value []ChatMessage `json:"value"`
			}{Value: []ChatMessage{}})
			return
		}
		end := start + dockerRealDataPageSize
		if end > len(corpus) {
			end = len(corpus)
		}
		messages := corpus[start:end]
		response := struct {
			Value    []ChatMessage `json:"value"`
			NextLink string        `json:"@odata.nextLink,omitempty"`
		}{Value: messages}
		if end < len(corpus) {
			response.NextLink = "/chats/" + url.PathEscape(chatID) + "/messages?cxp_replay_page=" + strconv.Itoa(page+1)
		}
		writeDockerRealDataJSON(w, response)
		return
	}
	if chatID, messageID, item := dockerRealDataChatItemPath(path); item && req.Method == http.MethodGet {
		g.itemGETs.Add(1)
		message := dockerRealDataMessage(chatID, 0, 0, g.base)
		message.ID = messageID
		writeDockerRealDataJSON(w, message)
		return
	}
	if req.Method == http.MethodPost && strings.HasPrefix(path, "/chats/") && strings.HasSuffix(path, "/markChatUnreadForUser") {
		g.posts.Add(1)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if req.Method == http.MethodPost && strings.HasPrefix(path, "/chats/") && (strings.HasSuffix(path, "/messages") || strings.HasSuffix(path, "/messages/replyWithQuote")) {
		g.posts.Add(1)
		payload, _ := io.ReadAll(req.Body)
		postKey := shortStableID(string(payload))
		g.mu.Lock()
		g.postAttempts[postKey]++
		attempt := g.postAttempts[postKey]
		if g.unknownPostKey == "" {
			g.unknownPostKey = postKey
		}
		unknown := postKey == g.unknownPostKey && attempt == 1
		repeatUnknown := postKey == g.unknownPostKey && attempt > 1
		g.mu.Unlock()
		if unknown {
			g.unknownPosts.Add(1)
			if hijacker, ok := w.(http.Hijacker); ok {
				conn, _, hijackErr := hijacker.Hijack()
				if hijackErr == nil {
					_ = conn.Close()
					return
				}
			}
			// A server that cannot hijack still exposes a transport-like failure
			// to the client. The production POST path must not replay it.
			http.Error(w, "connection closed before response", http.StatusServiceUnavailable)
			return
		}
		if repeatUnknown {
			g.unknownPostRepeats.Add(1)
		}
		chatID := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSuffix(strings.TrimPrefix(path, "/chats/"), "/messages/replyWithQuote"), "/messages"), "/")
		chatID, _ = url.PathUnescape(chatID)
		message := dockerRealDataMessage(chatID, 0, int(g.posts.Load()), g.base)
		message.ID = fmt.Sprintf("docker-real-data-outbound:%06d", g.posts.Load())
		writeDockerRealDataJSON(w, message)
		return
	}
	if req.Method == http.MethodGet && path == "/me" {
		writeDockerRealDataJSON(w, g.user)
		return
	}
	if req.Method == http.MethodGet && strings.HasPrefix(path, "/chats/") && strings.HasSuffix(path, "/members") {
		writeDockerRealDataJSON(w, struct {
			Value []ChatMember `json:"value"`
		}{Value: []ChatMember{
			{ID: "docker-real-data-member-1", UserID: g.user.ID, DisplayName: "Docker real-data user"},
			{ID: "docker-real-data-member-2", UserID: "docker-real-data-external-user", DisplayName: "Docker real-data replay"},
		}})
		return
	}
	if req.Method == http.MethodPatch {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if req.Method == http.MethodPost && path == "/me/onlineMeetings" {
		writeDockerRealDataJSON(w, OnlineMeeting{ID: "docker-real-data-meeting", Subject: "Docker real-data experiment"})
		return
	}
	if req.Method == http.MethodGet {
		// Collection endpoints used only by optional recovery/metadata paths are
		// kept deterministic and empty. Unknown paths are reported so the test
		// output exposes accidental Graph work instead of hiding it.
		g.recordUnknown(req.Method + " " + req.URL.RequestURI())
		writeDockerRealDataJSON(w, struct {
			Value []any `json:"value"`
		}{Value: []any{}})
		return
	}
	g.recordUnknown(req.Method + " " + req.URL.RequestURI())
	writeDockerRealDataJSON(w, map[string]any{"id": "docker-real-data-generic"})
}

type dockerRealDataCounts struct {
	inbound     int64
	completed   int64
	failed      int64
	queued      int64
	running     int64
	interrupted int64
}

type dockerRealDataMeasuredWindow struct {
	completed       bool
	before          dockerRealDataCounts
	after           dockerRealDataCounts
	executorBefore  int64
	executorAfter   int64
	countsReadError error
}

func dockerRealDataCountsFromSQLite(ctx context.Context, path string) (dockerRealDataCounts, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return dockerRealDataCounts{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var out dockerRealDataCounts
	prefix := dockerRealDataMessageIDPrefix + "%"
	inboundRows, err := db.QueryContext(ctx, `SELECT id FROM inbound_events WHERE teams_message_id LIKE ?`, prefix)
	if err != nil {
		return out, err
	}
	syntheticInbound := make(map[string]struct{})
	for inboundRows.Next() {
		var id string
		if err := inboundRows.Scan(&id); err != nil {
			_ = inboundRows.Close()
			return out, err
		}
		syntheticInbound[id] = struct{}{}
	}
	if err := inboundRows.Err(); err != nil {
		_ = inboundRows.Close()
		return out, err
	}
	if err := inboundRows.Close(); err != nil {
		return out, err
	}
	out.inbound = int64(len(syntheticInbound))

	// Keep the exact durable completion measurement read-only without a SQL
	// join on two JSON projections.  The indexed inbound lookup above gives us
	// the synthetic event IDs; one bounded turns scan then classifies statuses
	// in Go.  This avoids a query plan that can repeatedly decode every large
	// turn JSON row for every synthetic inbound row and contend with the
	// listener's writes.
	turnRows, err := db.QueryContext(ctx, `SELECT status, json_extract(json, '$.inbound_event_id') FROM turns`)
	if err != nil {
		return out, err
	}
	for turnRows.Next() {
		var status string
		var inboundID sql.NullString
		if err := turnRows.Scan(&status, &inboundID); err != nil {
			_ = turnRows.Close()
			return out, err
		}
		if !inboundID.Valid {
			continue
		}
		if _, ok := syntheticInbound[inboundID.String]; !ok {
			continue
		}
		switch teamstore.TurnStatus(status) {
		case teamstore.TurnStatusCompleted:
			out.completed++
		case teamstore.TurnStatusFailed:
			out.failed++
		case teamstore.TurnStatusQueued:
			out.queued++
		case teamstore.TurnStatusRunning:
			out.running++
		case teamstore.TurnStatusInterrupted:
			out.interrupted++
		}
	}
	if err := turnRows.Err(); err != nil {
		_ = turnRows.Close()
		return out, err
	}
	return out, turnRows.Close()
}

func dockerRealDataInterruptedTurnDetails(ctx context.Context, path string) ([]string, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	rows, err := db.QueryContext(ctx, `
		SELECT t.id, t.json
		FROM turns t
		JOIN inbound_events i ON json_extract(t.json, '$.inbound_event_id') = i.id
		WHERE i.teams_message_id LIKE ? AND t.status = ?
		ORDER BY t.id`, dockerRealDataMessageIDPrefix+"%", teamstore.TurnStatusInterrupted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		details = append(details, id+":"+string(raw))
	}
	return details, rows.Err()
}

func dockerRealDataHistoryOffsetSum(state teamstore.State) int64 {
	var total int64
	for _, checkpoint := range state.HistoryWatch {
		if checkpoint.Offset > 0 {
			total += checkpoint.Offset
		}
	}
	return total
}

func dockerRealDataDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(dockerRealDataDurationEnv))
	if raw == "" {
		return dockerRealDataDefaultDuration
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < dockerRealDataMinimumDuration {
		t.Fatalf("invalid %s=%q; want a duration of at least %s", dockerRealDataDurationEnv, raw, dockerRealDataMinimumDuration)
	}
	return duration
}

func prepareDockerRealDataPollSchedules(t *testing.T, store *teamstore.Store, state teamstore.State, replay map[string][]ChatMessage, controlChatID string, now time.Time) (int, int) {
	t.Helper()
	if now.IsZero() {
		now = time.Now()
	}
	holdUntil := now.Add(24 * time.Hour)
	replayChats := 0
	heldChats := 0
	seenChats := make(map[string]struct{}, len(state.Sessions))
	updates := make([]teamstore.ChatPollScheduleUpdate, 0, len(state.Sessions))
	for _, session := range state.Sessions {
		chatID := strings.TrimSpace(session.TeamsChatID)
		if chatID == "" || chatID == strings.TrimSpace(controlChatID) || !isActiveSessionStatus(string(session.Status)) {
			continue
		}
		if _, seen := seenChats[chatID]; seen {
			continue
		}
		seenChats[chatID] = struct{}{}
		poll := state.ChatPolls[chatID]
		pollState := strings.TrimSpace(poll.PollState)
		if pollState == "" {
			pollState = inboundPollStateWarm
		}
		nextPollAt := now.Add(-time.Second)
		if _, ok := replay[chatID]; !ok {
			nextPollAt = holdUntil
			heldChats++
		} else {
			replayChats++
		}
		updates = append(updates, teamstore.ChatPollScheduleUpdate{
			ChatID:            chatID,
			PollState:         pollState,
			NextPollAt:        nextPollAt,
			LastActivityAt:    poll.LastActivityAt,
			ClearBlockedUntil: true,
			ResetFailures:     true,
		})
	}
	if _, err := store.UpdateChatPollSchedules(context.Background(), updates); err != nil {
		t.Fatalf("prepare disposable replay schedules: %v", err)
	}
	return replayChats, heldChats
}

// TestDockerRealDataTeamsProgressThroughput is an opt-in experiment, not a
// smoke test. It runs the actual listener loop against a point-in-time copy of
// the current Teams SQLite state, the current registry projection, and the
// real Codex session files mounted read-only. Only the external token/Graph
// boundary and Codex executor are replaced. The test is intentionally skipped
// unless the Docker runner supplies CXP_TEAMS_DOCKER_REAL_DATA_EXPERIMENT=1.
func TestDockerRealDataTeamsProgressThroughput(t *testing.T) {
	if os.Getenv(dockerRealDataExperimentEnv) != "1" {
		t.Skipf("set %s=1 to run the real-data Docker experiment", dockerRealDataExperimentEnv)
	}
	fixtureRoot := dockerTeamsFixtureRoot(t)
	store, statePath := prepareDockerFixtureStore(t, fixtureRoot)
	ctx, cancel := context.WithTimeout(context.Background(), dockerRealDataDuration(t)+10*time.Minute)
	defer cancel()

	loadStarted := time.Now()
	beforeState, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load copied real Teams SQLite state: %v", err)
	}
	loadElapsed := time.Since(loadStarted)
	if beforeState.Scope.ID == "" || beforeState.ControlChat.TeamsChatID == "" {
		t.Fatalf("copied real state is missing scope/control binding: scope=%q control=%q", beforeState.Scope.ID, beforeState.ControlChat.TeamsChatID)
	}
	_, _, _ = dockerFixtureHistoryLag(t, fixtureRoot, beforeState)
	beforeHistoryOffsets := dockerRealDataHistoryOffsetSum(beforeState)
	beforeCounts, err := dockerRealDataCountsFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
	if err != nil {
		t.Fatalf("read copied real-data baseline counters: %v", err)
	}
	if beforeCounts.inbound != 0 {
		t.Fatalf("copied runtime already contains synthetic inbound rows before experiment: %d", beforeCounts.inbound)
	}

	// The source snapshot may contain the live helper's lease. Retire it only in
	// the disposable copied runtime; the real helper and its database are never
	// opened for writing by this test.
	machine := teamstore.MachineRecord{
		ID:      "docker-real-data-machine",
		ScopeID: beforeState.Scope.ID,
		Kind:    teamstore.MachineKindEphemeral,
	}
	if priorLease := beforeState.ControlLease; priorLease.Generation > 0 {
		if _, err := store.ReleaseControlLeaseIfHolder(ctx, priorLease.HolderMachineID, priorLease.Generation); err != nil {
			t.Fatalf("release copied source lease: %v", err)
		}
	}

	registry, err := LoadRegistry(filepath.Join(fixtureRoot, "teams", "registry.json"))
	if err != nil {
		t.Fatalf("load copied real registry projection: %v", err)
	}
	registryPath := filepath.Join(filepath.Dir(statePath), "registry.json")
	user := User{
		ID:                beforeState.Scope.AccountID,
		UserPrincipalName: beforeState.Scope.UserPrincipal,
		DisplayName:       beforeState.MachineIdentity.Label,
	}
	if strings.TrimSpace(registry.ControlChatID) == "" {
		registry.ControlChatID = beforeState.ControlChat.TeamsChatID
	}
	registry.UserID = user.ID
	registry.UserPrincipal = user.UserPrincipalName
	replayCorpus, replayCount := dockerRealDataReplayCorpus(beforeState, beforeState.ControlChat.TeamsChatID)
	if replayCount < dockerRealDataMinimumReplay {
		t.Fatalf("copied real-data snapshot produced only %d ordinary queued Teams messages for replay; refusing a smoke-sized workload", replayCount)
	}
	t.Logf("real-data replay corpus: queued Teams payloads=%d chats=%d; bodies/authors/order copied from durable rows, only IDs/timestamps remapped", replayCount, len(replayCorpus))
	replayChats, heldChats := prepareDockerRealDataPollSchedules(t, store, beforeState, replayCorpus, beforeState.ControlChat.TeamsChatID, time.Now())
	t.Logf("real-data disposable schedule: replay chats due=%d unrelated active chats held=%d; source schedule/database is untouched", replayChats, heldChats)

	const graphToken = "docker-real-data-deterministic-token"
	graphServerState := newDockerRealDataGraphServer(graphToken, user, replayCorpus)
	graphServerState.controlChatID = beforeState.ControlChat.TeamsChatID
	faultChats := make([]string, 0, len(replayCorpus))
	for chatID := range replayCorpus {
		faultChats = append(faultChats, chatID)
	}
	sort.Strings(faultChats)
	if len(faultChats) == 0 {
		t.Fatal("real-data replay corpus has no chat for Graph fault injection")
	}
	graphServerState.setFaultChat(faultChats[0])
	graphServer := httptest.NewServer(graphServerState)
	defer graphServer.Close()
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: graphToken},
		client:     graphServer.Client(),
		baseURL:    graphServer.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(delay time.Duration) time.Duration { return delay },
	}
	executor := &dockerRealDataExecutor{holdFirst: 3 * time.Second}
	traceWriter := &dockerRealDataTraceWriter{}
	historyMandatory := make(map[string]bool)
	for _, checkpoint := range beforeState.HistoryWatch {
		if historyWatchCheckpointNeedsMandatoryMaintenance(checkpoint) {
			historyMandatory[historyWatchCheckpointID(checkpoint.Path)] = true
		}
	}
	linkedMandatory := make(map[string]bool)
	for id, checkpoint := range beforeState.ImportCheckpoints {
		if linkedTranscriptCheckpointNeedsMandatoryMaintenance(checkpoint) {
			linkedMandatory[id] = true
		}
	}
	maintenanceTrace := &dockerRealDataMaintenanceTrace{store: store, historyMandatory: historyMandatory, linkedMandatory: linkedMandatory}
	duration := dockerRealDataDuration(t)
	runStarted := time.Now()
	var bridges []*Bridge
	var ownerGenerations []int64
	var ownerChangesByRun []int
	ownerChanges := 0
	measuredWindows := 0
	var measuredInbound int64
	var measuredCompleted int64
	var measuredExecutorRuns int64
	var lastListenerErr error
	runListener := func(activeStore *teamstore.Store) error {
		bridge := &Bridge{
			graph:                    graph,
			readGraph:                graph,
			registryPath:             registryPath,
			reg:                      registry,
			user:                     user,
			scope:                    beforeState.Scope,
			machine:                  machine,
			leaseDuration:            2 * time.Minute,
			out:                      traceWriter,
			executor:                 executor,
			controlFallbackExecutor:  executor,
			store:                    activeStore,
			markAnswerChatsUnread:    true,
			groupChatGuardEnabled:    true,
			maxWorkChatPollsPerCycle: DefaultMaxWorkChatPollsPerCycle,
			pollWorkerBudget:         mainLoopPollWorkerBudget,
		}
		bridge.controlLeaseClaimHook = traceWriter.recordLeaseClaim
		bridge.ownerFailureHook = traceWriter.recordOwnerFailure
		bridge.historyWatchPathHook = maintenanceTrace.observeHistory
		bridge.linkedTranscriptSessionHook = maintenanceTrace.observeLinked
		cycleDone := make(chan struct{}, 1)
		var measuredWindowCompleted atomic.Bool
		var measuredWindow dockerRealDataMeasuredWindow
		bridge.mainLoopCycleDoneHook = func() {
			select {
			case cycleDone <- struct{}{}:
			default:
			}
		}
		ownerTrace := startDockerRealDataOwnerTrace(bridge)
		listenCtx, listenCancel := context.WithCancel(ctx)
		stopCoordinatorDone := make(chan struct{})
		go func() {
			defer close(stopCoordinatorDone)
			// Startup reconciliation can legitimately take tens of seconds on the
			// copied production registry/history projection.  Start the measured
			// listener window only after the first complete main-loop cycle, so the
			// stop boundary cannot be scheduled in the middle of startup work.
			startupDeadline := time.NewTimer(5 * time.Minute)
			select {
			case <-cycleDone:
				if !startupDeadline.Stop() {
					select {
					case <-startupDeadline.C:
					default:
					}
				}
			case <-startupDeadline.C:
				t.Log("real-data listener did not complete its first main-loop cycle before the startup deadline")
				return
			case <-ctx.Done():
				return
			}
			measuredWindow.before, measuredWindow.countsReadError = dockerRealDataCountsFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
			measuredWindow.executorBefore = executor.runs.Load()
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-timer.C:
				measuredWindowCompleted.Store(true)
				measuredWindow.executorAfter = executor.runs.Load()
				measuredWindow.after, measuredWindow.countsReadError = dockerRealDataCountsFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
				measuredWindow.completed = measuredWindow.countsReadError == nil
			case <-ctx.Done():
				return
			}
			// Ask the disposable executor to finish any already-admitted call and
			// make later calls complete immediately. This gives the real listener a
			// clean durable completion boundary before its owner context is closed;
			// a timeout-driven cancel otherwise turns the last legitimate turn into
			// an artificial Interrupted row.
			executor.requestGracefulStop()
			deadline := time.Now().Add(30 * time.Second)
			for executor.activeRuns() > 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			// Do not wait for another full main-loop cycle here.  The executor
			// and async admission boundaries are the durable completion boundary;
			// allowing a fresh poll wave after the measured window can manufacture
			// a phase deadline on a large real-data schedule and obscure the
			// workload result.  Cancellation below still lets Listen finish its
			// normal owner/lease cleanup path.
			asyncTurnIdle := stopDockerRealDataAsyncAdmission(bridge)
			asyncDeadline := time.Now().Add(30 * time.Second)
			select {
			case <-asyncTurnIdle:
			case <-time.After(time.Until(asyncDeadline)):
				t.Log("real-data listener had an async turn still active at graceful-stop deadline")
			}
			// Stop observing before canceling the listener.  Cancellation causes the
			// owner cleanup path to release its lease; if observation continues until
			// Listen returns, the test can mistake that intentional teardown (or a
			// brief re-claim while cancellation propagates) for an owner loss during
			// the measured workload.
			ownerTrace.stopTrace()
			listenCancel()
		}()
		listenErr := bridge.Listen(listenCtx, BridgeOptions{
			Store:                      activeStore,
			RegistryPath:               registryPath,
			Interval:                   5 * time.Second,
			Once:                       false,
			Top:                        dockerRealDataDefaultTop,
			MaxWorkChatPollsPerCycle:   DefaultMaxWorkChatPollsPerCycle,
			PhaseBudget:                mainLoopPhaseBudget,
			PollWorkerBudget:           mainLoopPollWorkerBudget,
			OwnerStaleAfter:            2 * time.Minute,
			AsyncTurnShutdownGrace:     10 * time.Second,
			HelperVersion:              "docker-real-data-experiment",
			Executor:                   executor,
			ControlFallbackExecutor:    executor,
			ControlFallbackHelpContext: "docker-real-data-experiment",
		})
		<-stopCoordinatorDone
		listenCancel()
		ownerTrace.stopTrace()
		generations, changes := ownerTrace.snapshot()
		ownerGenerations = append(ownerGenerations, generations...)
		ownerChangesByRun = append(ownerChangesByRun, changes)
		ownerChanges += changes
		if measuredWindowCompleted.Load() {
			measuredWindows++
			if measuredWindow.countsReadError != nil {
				return fmt.Errorf("read measured Docker window counters: %w", measuredWindow.countsReadError)
			}
			measuredInbound += measuredWindow.after.inbound - measuredWindow.before.inbound
			measuredCompleted += measuredWindow.after.completed - measuredWindow.before.completed
			measuredExecutorRuns += measuredWindow.executorAfter - measuredWindow.executorBefore
		}
		t.Logf("real-data listener generation run=%d generations=%v changes=%d", len(ownerChangesByRun), generations, changes)
		t.Logf("real-data listener lifecycle run=%d events=%v", len(ownerChangesByRun), traceWriter.eventsSnapshot())
		t.Logf("real-data listener lease claims run=%d claims=%v", len(ownerChangesByRun), traceWriter.leaseClaimsSnapshot())
		t.Logf("real-data listener owner failures run=%d failures=%v", len(ownerChangesByRun), traceWriter.ownerFailuresSnapshot())
		if changes != 0 {
			t.Logf("real-data listener output around owner change run=%d lines=%v", len(ownerChangesByRun), traceWriter.linesSnapshot())
		}
		bridge.asyncTurnWG.Wait()
		bridges = append(bridges, bridge)
		if listenErr != nil && !errors.Is(listenErr, context.DeadlineExceeded) && !errors.Is(listenErr, context.Canceled) {
			lastListenerErr = listenErr
			return fmt.Errorf("real-data listener experiment failed: %w", listenErr)
		}
		return nil
	}

	if err := runListener(store); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close copied real-data store before reopen: %v", err)
	}
	reopenedStore, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("reopen copied real-data store during replay: %v", err)
	}
	t.Cleanup(func() {
		if err := reopenedStore.Close(); err != nil {
			t.Errorf("close reopened copied real-data store: %v", err)
		}
	})
	store = reopenedStore
	maintenanceTrace.store = store
	executor.resetGracefulStop()
	if err := runListener(store); err != nil {
		t.Fatal(err)
	}
	runEnded := time.Now()
	runElapsed := runEnded.Sub(runStarted)

	postCtx, postCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer postCancel()
	afterCounts, err := dockerRealDataCountsFromSQLite(postCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
	if err != nil {
		t.Fatalf("read copied real-data result counters: %v", err)
	}
	afterState, err := store.Load(postCtx)
	if err != nil {
		t.Fatalf("load copied real-data state after listener experiment: %v", err)
	}
	afterHistoryOffsets := dockerRealDataHistoryOffsetSum(afterState)
	runs := executor.runs.Load()
	firstRun := executor.firstRunAt()
	listeningAt := traceWriter.listeningTime()
	startupReadyElapsed := time.Duration(0)
	if !listeningAt.IsZero() && listeningAt.After(runStarted) {
		startupReadyElapsed = listeningAt.Sub(runStarted)
	}
	firstExecutionElapsed := time.Duration(0)
	if !firstRun.IsZero() && firstRun.After(runStarted) {
		firstExecutionElapsed = firstRun.Sub(runStarted)
	}
	// Exclude startup, SQLite close/reopen, and the second startup from the
	// throughput denominator. Each completed listener window is explicitly
	// bounded by the coordinator timer above; the full wall clock remains useful
	// as a separate diagnostic for recovery/startup cost.
	steadyElapsed := duration * time.Duration(measuredWindows)
	if steadyElapsed <= 0 {
		steadyElapsed = runElapsed
	}
	unknown := graphServerState.unknownPaths()
	backlogSamples, ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog := maintenanceTrace.snapshot()
	ordinaryHistoryPaths, ordinaryLinkedSessions := maintenanceTrace.ordinaryWorkSnapshot()
	phaseStats := func(name string) mainLoopPhaseStats {
		var total mainLoopPhaseStats
		for _, bridge := range bridges {
			current := bridge.mainLoopPhaseStatsSnapshot(name)
			total.Runs += current.Runs
			total.DeadlineExceeded += current.DeadlineExceeded
			total.Errors += current.Errors
			if current.LastDuration > total.LastDuration {
				total.LastDuration = current.LastDuration
			}
			if current.LastError != "" {
				total.LastError = current.LastError
			}
		}
		return total
	}
	pollStats := phaseStats("poll")
	historyWatchStats := phaseStats("history-watch")
	linkedTranscriptStats := phaseStats("linked-transcript")
	t.Logf("real-data Teams experiment: startup_load=%s listener_wall=%s measured_windows=%d measured_window=%s startup_ready_after=%s first_execution_after=%s baseline_inbound=%d replayed_inbound=%d completed=%d failed=%d queued=%d running=%d interrupted=%d executor_runs=%d measured_inbound=%d measured_completed=%d measured_executor_runs=%d wall_inbound_per_sec=%.3f measured_inbound_per_sec=%.3f measured_completed_per_sec=%.3f measured_executor_per_sec=%.3f graph_list_gets=%d graph_item_gets=%d graph_posts_blocked_locally=%d graph_429=%d graph_503=%d unknown_posts=%d repeated_unknown_posts=%d history_offset_delta=%d poll_runs=%d poll_deadlines=%d poll_errors=%d poll_last_duration=%s poll_last_error=%q history_watch_runs=%d history_watch_deadlines=%d history_watch_errors=%d history_watch_last_duration=%s history_watch_last_error=%q linked_transcript_runs=%d linked_transcript_deadlines=%d linked_transcript_errors=%d linked_transcript_last_duration=%s linked_transcript_last_error=%q owner_generations=%v owner_changes=%d owner_changes_by_run=%v backlog_samples=%d ordinary_history_while_backlog=%d ordinary_linked_while_backlog=%d ordinary_history_paths=%v ordinary_linked_sessions=%v baseline_history_offset_sum=%d final_history_offset_sum=%d graph_list_requests=%v unknown_graph_paths=%v listener_error=%v", loadElapsed, runElapsed, measuredWindows, steadyElapsed, startupReadyElapsed, firstExecutionElapsed, beforeCounts.inbound, afterCounts.inbound, afterCounts.completed, afterCounts.failed, afterCounts.queued, afterCounts.running, afterCounts.interrupted, runs, measuredInbound, measuredCompleted, measuredExecutorRuns, float64(afterCounts.inbound)/runElapsed.Seconds(), float64(measuredInbound)/steadyElapsed.Seconds(), float64(measuredCompleted)/steadyElapsed.Seconds(), float64(measuredExecutorRuns)/steadyElapsed.Seconds(), graphServerState.listGETs.Load(), graphServerState.itemGETs.Load(), graphServerState.posts.Load(), graphServerState.status429.Load(), graphServerState.status503.Load(), graphServerState.unknownPosts.Load(), graphServerState.unknownPostRepeats.Load(), afterHistoryOffsets-beforeHistoryOffsets, pollStats.Runs, pollStats.DeadlineExceeded, pollStats.Errors, pollStats.LastDuration, pollStats.LastError, historyWatchStats.Runs, historyWatchStats.DeadlineExceeded, historyWatchStats.Errors, historyWatchStats.LastDuration, historyWatchStats.LastError, linkedTranscriptStats.Runs, linkedTranscriptStats.DeadlineExceeded, linkedTranscriptStats.Errors, linkedTranscriptStats.LastDuration, linkedTranscriptStats.LastError, ownerGenerations, ownerChanges, ownerChangesByRun, backlogSamples, ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog, ordinaryHistoryPaths, ordinaryLinkedSessions, beforeHistoryOffsets, afterHistoryOffsets, graphServerState.listRequests(), unknown, lastListenerErr)

	if afterCounts.inbound <= beforeCounts.inbound || afterCounts.completed <= 0 || runs <= 0 {
		t.Fatalf("real copied Teams workload did not make durable execution progress: before=%#v after=%#v executor_runs=%d listener_err=%v", beforeCounts, afterCounts, runs, lastListenerErr)
	}
	if afterCounts.inbound > int64(replayCount) {
		t.Fatalf("synthetic inbound count=%d exceeded replay corpus=%d", afterCounts.inbound, replayCount)
	}
	accounted := afterCounts.completed + afterCounts.failed + afterCounts.queued + afterCounts.running + afterCounts.interrupted
	if accounted != afterCounts.inbound {
		t.Fatalf("synthetic durable turn accounting is not closed: inbound=%d completed=%d failed=%d queued=%d running=%d interrupted=%d", afterCounts.inbound, afterCounts.completed, afterCounts.failed, afterCounts.queued, afterCounts.running, afterCounts.interrupted)
	}
	if afterCounts.failed != 0 || afterCounts.running != 0 || afterCounts.interrupted != 0 {
		interruptedDetails, detailErr := dockerRealDataInterruptedTurnDetails(postCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
		t.Logf("synthetic interrupted turn details: rows=%v err=%v", interruptedDetails, detailErr)
		t.Fatalf("synthetic executions did not close successfully: after=%#v executor_runs=%d", afterCounts, runs)
	}
	if runs != afterCounts.completed {
		t.Fatalf("executor invocation count=%d does not equal durable completed turns=%d; possible owner/fence loss or duplicate admission", runs, afterCounts.completed)
	}
	for run, changes := range ownerChangesByRun {
		if changes != 0 {
			t.Fatalf("listener owner generation changed within isolated run %d: changes=%d all_generations=%v", run+1, changes, ownerGenerations)
		}
	}
	if len(unknown) != 0 {
		t.Fatalf("fake Graph observed unexpected routes in the production listener: %v", unknown)
	}
	if graphServerState.status429.Load() == 0 || graphServerState.status503.Load() == 0 {
		t.Fatalf("real-data replay did not exercise both retryable Graph failures: 429=%d 503=%d", graphServerState.status429.Load(), graphServerState.status503.Load())
	}
	if graphServerState.unknownPosts.Load() != 1 || graphServerState.unknownPostRepeats.Load() != 0 {
		t.Fatalf("unknown Graph POST result was not handled as a single non-replayed attempt: unknown=%d repeats=%d", graphServerState.unknownPosts.Load(), graphServerState.unknownPostRepeats.Load())
	}
	if backlogSamples == 0 {
		t.Fatalf("real-data replay never observed an operational Teams backlog; optional-maintenance suppression was not exercised")
	}
	if ordinaryHistoryWhileBacklog != 0 || ordinaryLinkedWhileBacklog != 0 {
		t.Fatalf("ordinary optional maintenance ran while Teams backlog was active: history=%d linked=%d", ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog)
	}
	if pollStats.DeadlineExceeded != 0 || pollStats.Errors != 0 {
		t.Fatalf("poll phase degraded in isolated real-data run: %#v", pollStats)
	}
	if historyWatchStats.DeadlineExceeded != 0 || historyWatchStats.Errors != 0 {
		t.Fatalf("history-watch phase degraded in isolated real-data run: %#v", historyWatchStats)
	}
	if linkedTranscriptStats.DeadlineExceeded != 0 || linkedTranscriptStats.Errors != 0 {
		t.Fatalf("linked-transcript phase degraded in isolated real-data run: %#v", linkedTranscriptStats)
	}
	if afterCounts.queued > 0 {
		// A real replay run is intentionally long enough to leave a durable Teams
		// backlog. Optional history work must not consume the same phase budget in
		// that state; a short no-op phase is acceptable, but a full scan/deadline
		// is not.
		const optionalMaintenanceBudget = time.Second
		if historyWatchStats.LastDuration > optionalMaintenanceBudget || linkedTranscriptStats.LastDuration > optionalMaintenanceBudget {
			t.Fatalf("optional maintenance remained expensive while durable Teams backlog existed: queued=%d history=%#v linked=%#v", afterCounts.queued, historyWatchStats, linkedTranscriptStats)
		}
		if afterHistoryOffsets != beforeHistoryOffsets {
			t.Fatalf("history cursor advanced while durable Teams backlog remained: before=%d after=%d queued=%d", beforeHistoryOffsets, afterHistoryOffsets, afterCounts.queued)
		}
	}
}
