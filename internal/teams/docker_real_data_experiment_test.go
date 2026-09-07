package teams

import (
	"context"
	"crypto/sha256"
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
	dockerRealDataModeEnv         = "CXP_TEAMS_DOCKER_REAL_DATA_MODE"
	dockerRealDataDurationEnv     = "CXP_TEAMS_DOCKER_REAL_DATA_DURATION"
	dockerRealDataResumeEnv       = "CXP_TEAMS_DOCKER_REAL_DATA_RESUME"
	dockerRealDataPageSize        = 20
	dockerRealDataMessageIDPrefix = "docker-real-data:"
	dockerRealDataDefaultDuration = 5 * time.Minute
	dockerRealDataMinimumDuration = time.Minute
	dockerRealDataDefaultTop      = ownerPollMessageTop
	dockerRealDataMinimumReplay   = 100
	dockerRealDataModeThroughput  = "throughput"
	dockerRealDataModeComplete    = "complete"
	dockerRealDataExecutionPrefix = "docker real-data execution result #"
)

func dockerRealDataProductionPhaseNames() []string {
	// Keep this list in lockstep with Bridge.Listen's runPhase calls.  A Docker
	// acceptance run must not silently ignore an error in an auxiliary phase
	// merely because the poll/history/linked trio happened to make progress.
	return []string{
		"outbox",
		"workflow",
		"poll",
		"fork-reconcile",
		"idle-auto-park",
		"queued-turns",
		"optional-maintenance-gate",
		"linked-transcript",
		"history-watch",
		"helper-auto-update",
		"helper-upgrade-notice",
		"codex-upgrade",
		"beacon-reconcile",
		"beacon-lease-maintenance",
		"deferred-inbound",
		"interrupted-notices",
	}
}

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
		Text:                     fmt.Sprintf("%s%d", dockerRealDataExecutionPrefix, n),
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

type dockerRealDataReplayReport struct {
	QueuedTeams             int
	ActiveQueued            int
	Accepted                int
	ExcludedControlChat     int
	ExcludedNoActiveChat    int
	ExcludedMalformed       int
	ExcludedDashboard       int
	ExcludedEmptyTerminal   int
	ExcludedEmptyActionable int
	ExcludedAttachments     int
	ExcludedHostedContent   int
	ExcludedUnsupported     int
}

// dockerRealDataInheritedOperationalRows identifies durable work that was
// already executable before the replay corpus is injected. It remains in the
// disposable fixture and is reported separately; the synthetic counters and
// correlation checks never use it as replay progress. Historical queued
// inbound rows linked to completed/failed/interrupted turns are provenance and
// are intentionally allowed to remain in the source snapshot.
func dockerRealDataInheritedOperationalRows(state teamstore.State) (inboundRows, turnRows, outboxRows int) {
	isSynthetic := func(messageID string) bool {
		return strings.HasPrefix(strings.TrimSpace(messageID), dockerRealDataMessageIDPrefix)
	}
	isSyntheticOutbox := func(outbox teamstore.OutboxMessage) bool {
		return strings.HasPrefix(strings.TrimSpace(outbox.TurnID), "docker-real-data-turn-") ||
			strings.HasPrefix(strings.TrimSpace(outbox.Body), dockerRealDataExecutionPrefix)
	}
	for _, inbound := range state.InboundEvents {
		if !strings.EqualFold(strings.TrimSpace(inbound.Source), "teams") || isSynthetic(inbound.TeamsMessageID) {
			continue
		}
		switch inbound.Status {
		case teamstore.InboundStatusPersisted, teamstore.InboundStatusDeferred, teamstore.InboundStatusQueued:
		default:
			continue
		}
		turn, found := state.Turns[strings.TrimSpace(inbound.TurnID)]
		if !found || !dockerRealDataTerminalTurn(turn.Status) {
			inboundRows++
		}
	}
	for _, turn := range state.Turns {
		if turn.Status != teamstore.TurnStatusQueued && turn.Status != teamstore.TurnStatusRunning {
			continue
		}
		inbound, found := state.InboundEvents[strings.TrimSpace(turn.InboundEventID)]
		if !found || !isSynthetic(inbound.TeamsMessageID) {
			turnRows++
		}
	}
	for _, outbox := range state.OutboxMessages {
		if isSyntheticOutbox(outbox) {
			continue
		}
		switch outbox.Status {
		case teamstore.OutboxStatusQueued, teamstore.OutboxStatusSending, teamstore.OutboxStatusAccepted:
			outboxRows++
		case teamstore.OutboxStatusSent, teamstore.OutboxStatusSkipped:
			if outbox.PostSendEffectsPending {
				outboxRows++
			}
		}
	}
	return inboundRows, turnRows, outboxRows
}

func dockerRealDataReplayCorpusPath(statePath string) string {
	return filepath.Join(filepath.Dir(statePath), "docker-real-data-replay-corpus.json")
}

func writeDockerRealDataReplayCorpus(t *testing.T, statePath string, corpus map[string][]ChatMessage) {
	t.Helper()
	data, err := json.Marshal(corpus)
	if err != nil {
		t.Fatalf("encode Docker real-data replay corpus: %v", err)
	}
	path := dockerRealDataReplayCorpusPath(statePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create Docker real-data replay corpus directory: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write Docker real-data replay corpus %q: %v", path, err)
	}
}

func readDockerRealDataReplayCorpus(t *testing.T, statePath string) (map[string][]ChatMessage, int) {
	t.Helper()
	path := dockerRealDataReplayCorpusPath(statePath)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted Docker real-data replay corpus %q: %v", path, err)
	}
	var corpus map[string][]ChatMessage
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatalf("decode persisted Docker real-data replay corpus %q: %v", path, err)
	}
	total := 0
	for chatID, messages := range corpus {
		if strings.TrimSpace(chatID) == "" {
			t.Fatalf("persisted Docker real-data replay corpus contains an empty chat ID")
		}
		for _, message := range messages {
			if !strings.HasPrefix(strings.TrimSpace(message.ID), dockerRealDataMessageIDPrefix) {
				t.Fatalf("persisted Docker real-data replay corpus contains non-synthetic message %q", message.ID)
			}
			total++
		}
	}
	return corpus, total
}

func dockerRealDataTerminalTurn(status teamstore.TurnStatus) bool {
	return status == teamstore.TurnStatusCompleted || status == teamstore.TurnStatusFailed || status == teamstore.TurnStatusInterrupted
}

func dockerRealDataAmbiguousExecutionOutboxes(state teamstore.State) []teamstore.OutboxMessage {
	const firstExecutionBody = dockerRealDataExecutionPrefix + "1"
	rows := make([]teamstore.OutboxMessage, 0)
	for _, outbox := range state.OutboxMessages {
		if strings.TrimSpace(outbox.Body) != firstExecutionBody || !teamstore.OutboxSendIsAmbiguous(outbox) {
			continue
		}
		rows = append(rows, outbox)
	}
	return rows
}

func dockerRealDataTurnStatusSQL(alias string) string {
	return `(CASE WHEN json_valid(` + alias + `.json) AND json_type(` + alias + `.json, '$.status') = 'text' THEN trim(COALESCE(json_extract(` + alias + `.json, '$.status'), '')) ELSE trim(COALESCE(` + alias + `.status, '')) END)`
}

func dockerRealDataTurnInboundIDSQL(alias string) string {
	return `(CASE WHEN json_valid(` + alias + `.json) THEN json_extract(` + alias + `.json, '$.inbound_event_id') ELSE NULL END)`
}

// dockerRealDataReplayCorpus is built from the copied production inbound
// rows. The message IDs and timestamps are remapped only because the original
// rows are already durable in the copied store and would therefore be correctly
// deduplicated. Bodies, authors, chat/session distribution, and queued-status
// selection remain from the real snapshot. Every exclusion is counted in the
// report; an acceptance run fails for an active row that this ordinary-message
// lane cannot safely model instead of silently turning a partial corpus into a
// false-green result.
func dockerRealDataReplayCorpus(state teamstore.State, controlChatID string) (map[string][]ChatMessage, int, dockerRealDataReplayReport) {
	var report dockerRealDataReplayReport
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
		if strings.HasPrefix(strings.TrimSpace(inbound.TeamsMessageID), dockerRealDataMessageIDPrefix) {
			continue
		}
		report.QueuedTeams++
		chatID := strings.TrimSpace(inbound.TeamsChatID)
		if chatID == strings.TrimSpace(controlChatID) {
			report.ExcludedControlChat++
			continue
		}
		if _, ok := activeChats[chatID]; !ok {
			report.ExcludedNoActiveChat++
			continue
		}
		report.ActiveQueued++
		// Decode-free empty rows are a common legacy shape. Classify them before
		// calling chatMessageFromInboundContext (which intentionally returns
		// false for an empty payload), otherwise terminal provenance rows would be
		// misreported as malformed and the fixture audit would lose its useful
		// distinction between stale and actionable data.
		if strings.TrimSpace(inbound.Text) == "" && strings.TrimSpace(inbound.TeamsBodyHTML) == "" && len(inbound.TeamsAttachments) == 0 {
			if turn, found := state.Turns[strings.TrimSpace(inbound.TurnID)]; found && dockerRealDataTerminalTurn(turn.Status) {
				report.ExcludedEmptyTerminal++
			} else {
				report.ExcludedEmptyActionable++
			}
			continue
		}
		msg, ok := chatMessageFromInboundContext(inbound)
		if !ok || strings.TrimSpace(inbound.ID) == "" {
			report.ExcludedMalformed++
			continue
		}
		// Do not replay dashboard commands: those would intentionally mutate the
		// disposable fixture instead of measuring ordinary message admission.
		if parsed := ParseDashboardCommand(ChatScopeWork, strings.TrimSpace(inbound.Text)); parsed.HelperCommand {
			report.ExcludedDashboard++
			continue
		}
		hostedContent := HostedContentIDsFromHTML(msg.Body.Content)
		if msg.Body.Content == "" {
			msg.Body.ContentType = "html"
			msg.Body.Content = inbound.Text
		}
		// Hosted-content and message-reference rows need additional Graph
		// endpoints and file materialization. Keep this experiment focused on the
		// ordinary message admission path while retaining the real bodies,
		// authors, chat distribution, and durable queued-row selection. These are
		// explicit unsupported rows, never silently discarded input.
		if len(msg.Attachments) > 0 {
			report.ExcludedAttachments++
			report.ExcludedUnsupported++
			continue
		}
		if len(hostedContent) > 0 {
			report.ExcludedHostedContent++
			report.ExcludedUnsupported++
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
		report.Accepted++
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
	return corpus, total, report
}

func TestDockerRealDataReplayCorpusAuditsEveryQueuedRow(t *testing.T) {
	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	state := teamstore.State{
		Sessions: map[string]teamstore.SessionContext{
			"active":  {ID: "active", Status: teamstore.SessionStatusActive, TeamsChatID: "active-chat"},
			"closed":  {ID: "closed", Status: teamstore.SessionStatusClosed, TeamsChatID: "closed-chat"},
			"control": {ID: "control", Status: teamstore.SessionStatusActive, TeamsChatID: "control-chat"},
		},
		Turns: map[string]teamstore.Turn{
			"terminal":   {ID: "terminal", Status: teamstore.TurnStatusCompleted},
			"actionable": {ID: "actionable", Status: teamstore.TurnStatusQueued},
		},
		InboundEvents: map[string]teamstore.InboundEvent{},
	}
	add := func(id, chatID, text, body string) {
		state.InboundEvents[id] = teamstore.InboundEvent{
			ID: id, SessionID: "active", TeamsChatID: chatID, TeamsMessageID: "source-" + id,
			Text: text, TeamsBodyType: "html", TeamsBodyHTML: body, Source: "teams",
			Status: teamstore.InboundStatusQueued, TurnID: "terminal", ReceivedAt: now,
		}
	}
	add("accepted", "active-chat", "hello", "<p>hello</p>")
	add("control", "control-chat", "control", "<p>control</p>")
	add("closed", "closed-chat", "closed", "<p>closed</p>")
	add("unknown-chat", "missing-chat", "unknown", "<p>unknown</p>")
	add("malformed", "active-chat", "malformed", "<p>malformed</p>")
	state.InboundEvents["malformed"] = func() teamstore.InboundEvent {
		event := state.InboundEvents["malformed"]
		event.ID = ""
		return event
	}()
	add("dashboard", "active-chat", "helper status", "<p>helper status</p>")
	state.InboundEvents["empty-terminal"] = teamstore.InboundEvent{
		ID: "empty-terminal", SessionID: "active", TeamsChatID: "active-chat", TeamsMessageID: "source-empty-terminal",
		TurnID: "terminal", Source: "teams", Status: teamstore.InboundStatusQueued, ReceivedAt: now,
	}
	state.InboundEvents["empty-actionable"] = teamstore.InboundEvent{
		ID: "empty-actionable", SessionID: "active", TeamsChatID: "active-chat", TeamsMessageID: "source-empty-actionable",
		TurnID: "actionable", Source: "teams", Status: teamstore.InboundStatusQueued, ReceivedAt: now,
	}
	add("attachment", "active-chat", "file", "<p>file</p>")
	state.InboundEvents["attachment"] = func() teamstore.InboundEvent {
		event := state.InboundEvents["attachment"]
		event.TeamsAttachments = []teamstore.InboundAttachmentContext{{ID: "file", ContentType: "text/plain"}}
		return event
	}()
	add("hosted", "active-chat", "image", `<p><img src="https://graph.test/hostedContents/cid-1/$value"></p>`)

	corpus, count, report := dockerRealDataReplayCorpus(state, "control-chat")
	if count != 1 || report.Accepted != 1 || len(corpus["active-chat"]) != 1 {
		t.Fatalf("replay acceptance = count=%d corpus=%d report=%+v, want one ordinary message", count, len(corpus["active-chat"]), report)
	}
	if report.QueuedTeams != 10 || report.ActiveQueued != 7 || report.ExcludedControlChat != 1 || report.ExcludedNoActiveChat != 2 ||
		report.ExcludedMalformed != 1 || report.ExcludedDashboard != 1 || report.ExcludedEmptyTerminal != 1 ||
		report.ExcludedEmptyActionable != 1 || report.ExcludedAttachments != 1 || report.ExcludedHostedContent != 1 || report.ExcludedUnsupported != 2 {
		t.Fatalf("replay source audit = %+v, want every queued row classified", report)
	}
	if inboundRows, turnRows, outboxRows := dockerRealDataInheritedOperationalRows(state); inboundRows != 1 || turnRows != 1 || outboxRows != 0 {
		t.Fatalf("inherited operational audit = inbound=%d turns=%d outbox=%d, want terminal provenance excluded and queued turn detected", inboundRows, turnRows, outboxRows)
	}
}

func TestDockerRealDataInheritedOutboxRowsAreRejectedFromSyntheticAccounting(t *testing.T) {
	state := teamstore.State{OutboxMessages: map[string]teamstore.OutboxMessage{
		"queued":          {ID: "queued", Status: teamstore.OutboxStatusQueued},
		"sending":         {ID: "sending", Status: teamstore.OutboxStatusSending},
		"accepted":        {ID: "accepted", Status: teamstore.OutboxStatusAccepted},
		"sent-effects":    {ID: "sent-effects", Status: teamstore.OutboxStatusSent, PostSendEffectsPending: true},
		"skipped-effects": {ID: "skipped-effects", Status: teamstore.OutboxStatusSkipped, PostSendEffectsPending: true},
		"sent-clean":      {ID: "sent-clean", Status: teamstore.OutboxStatusSent},
		"skipped-clean":   {ID: "skipped-clean", Status: teamstore.OutboxStatusSkipped},
	}}
	if inboundRows, turnRows, outboxRows := dockerRealDataInheritedOperationalRows(state); inboundRows != 0 || turnRows != 0 || outboxRows != 5 {
		t.Fatalf("inherited outbox audit = inbound=%d turns=%d outbox=%d, want five operational rows", inboundRows, turnRows, outboxRows)
	}
}

type dockerRealDataTraceWriter struct {
	mu            sync.Mutex
	listeningAt   time.Time
	lines         []string
	events        []string
	leaseClaims   []string
	ownerFailures []string
	pollChats     []string
}

func (w *dockerRealDataTraceWriter) recordPollChat(chatID string, elapsed time.Duration, err error) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pollChats) >= 128 {
		return
	}
	diagnostic := ""
	if err != nil {
		diagnostic = trimPollDiagnostic(err.Error())
	}
	w.pollChats = append(w.pollChats, fmt.Sprintf("%s=%s err=%q", strings.TrimSpace(chatID), elapsed, diagnostic))
}

func (w *dockerRealDataTraceWriter) pollChatsSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.pollChats...)
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
	linkedUnindexed  map[string]bool
	syntheticBacklog func(context.Context) (bool, error)

	mu                          sync.Mutex
	backlogSamples              int
	ordinaryHistoryWhileBacklog int
	ordinaryLinkedWhileBacklog  int
	ordinaryHistoryPaths        []string
	ordinaryLinkedSessions      []string
	ordinaryUnindexedLinked     int
	ordinaryUnindexedLinkedIDs  map[string]int
	syntheticBacklogSamples     int
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
		if t.syntheticBacklog != nil {
			synthetic, syntheticErr := t.syntheticBacklog(ctx)
			if syntheticErr != nil {
				return backlog, syntheticErr
			}
			if synthetic {
				t.mu.Lock()
				t.syntheticBacklogSamples++
				t.mu.Unlock()
			}
		}
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
		if t.linkedUnindexed[session.ID] {
			t.ordinaryUnindexedLinked++
			if t.ordinaryUnindexedLinkedIDs == nil {
				t.ordinaryUnindexedLinkedIDs = make(map[string]int)
			}
			t.ordinaryUnindexedLinkedIDs[session.ID]++
		}
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

func (t *dockerRealDataMaintenanceTrace) ordinaryUnindexedLinkedSnapshot() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ordinaryUnindexedLinked
}

func (t *dockerRealDataMaintenanceTrace) ordinaryUnindexedLinkedIDsSnapshot() map[string]int {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]int, len(t.ordinaryUnindexedLinkedIDs))
	for id, count := range t.ordinaryUnindexedLinkedIDs {
		out[id] = count
	}
	return out
}

func (t *dockerRealDataMaintenanceTrace) syntheticBacklogObserved() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.syntheticBacklogSamples > 0
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
	knownChats    map[string]struct{}

	mu                   sync.Mutex
	listPaths            []string
	unknownPath          []string
	postAttempts         map[string]int
	acceptedPostKeys     map[string]int
	pollServedMessageIDs map[string]int
	providerTokens       map[string]dockerRealDataProviderContinuation
	expiredTokens        map[string]struct{}
	unknownPostKey       string
	unknownPostChat      string
	unknownPostMarker    string
	faultChatID          string
	faultSequence        []int

	listGETs             atomic.Int64
	itemGETs             atomic.Int64
	posts                atomic.Int64
	status429            atomic.Int64
	status503            atomic.Int64
	unknownPosts         atomic.Int64
	unknownPostRepeats   atomic.Int64
	opaqueContinuations  atomic.Int64
	expiredContinuations atomic.Int64
	unknownContinuations atomic.Int64
	unsupportedFilters   atomic.Int64
	invalidListQueries   atomic.Int64
	pageCounts           map[string]int
}

type dockerRealDataProviderContinuation struct {
	chatID string
	offset int
}

type dockerRealDataExpiredContinuationError struct {
	token string
}

func (e dockerRealDataExpiredContinuationError) Error() string {
	return fmt.Sprintf("provider continuation expired: %s", e.token)
}

func newDockerRealDataGraphServer(token string, user User, replay map[string][]ChatMessage) *dockerRealDataGraphServer {
	knownChats := make(map[string]struct{}, len(replay))
	for chatID := range replay {
		knownChats[strings.TrimSpace(chatID)] = struct{}{}
	}
	return &dockerRealDataGraphServer{
		token:                token,
		user:                 user,
		base:                 time.Now().UTC().Add(2 * time.Second),
		replay:               replay,
		knownChats:           knownChats,
		postAttempts:         make(map[string]int),
		acceptedPostKeys:     make(map[string]int),
		pollServedMessageIDs: make(map[string]int),
		providerTokens:       make(map[string]dockerRealDataProviderContinuation),
		expiredTokens:        make(map[string]struct{}),
		pageCounts:           make(map[string]int),
	}
}

func (g *dockerRealDataGraphServer) setValidProviderContinuation(token string, chatID string, offset int) {
	if g == nil || strings.TrimSpace(token) == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.providerTokens == nil {
		g.providerTokens = make(map[string]dockerRealDataProviderContinuation)
	}
	g.providerTokens[strings.TrimSpace(token)] = dockerRealDataProviderContinuation{chatID: strings.TrimSpace(chatID), offset: offset}
	delete(g.expiredTokens, strings.TrimSpace(token))
}

func (g *dockerRealDataGraphServer) setExpiredProviderContinuation(token string) {
	if g == nil || strings.TrimSpace(token) == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.expiredTokens == nil {
		g.expiredTokens = make(map[string]struct{})
	}
	g.expiredTokens[strings.TrimSpace(token)] = struct{}{}
	delete(g.providerTokens, strings.TrimSpace(token))
}

func (g *dockerRealDataGraphServer) expiredContinuationCount() int64 {
	if g == nil {
		return 0
	}
	return g.expiredContinuations.Load()
}

func dockerRealDataProviderToken(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	parsed, err := url.Parse(path)
	if err != nil {
		return ""
	}
	token := strings.TrimSpace(parsed.Query().Get("$skiptoken"))
	if token == "" || strings.HasPrefix(token, "docker-real-data-page:") {
		return ""
	}
	return token
}

func dockerRealDataExpiredProviderTokens(server *dockerRealDataGraphServer, state teamstore.State) int {
	if server == nil {
		return 0
	}
	paths := make([]string, 0, len(state.ChatPolls)*8)
	for _, poll := range state.ChatPolls {
		paths = append(paths, poll.ContinuationPath, poll.DeferredContinuationPath)
		if poll.PendingPage != nil {
			paths = append(paths, poll.PendingPage.RequestPath, poll.PendingPage.NextPath)
		}
		if poll.Gap != nil {
			paths = append(paths, poll.Gap.FrontierPath, poll.Gap.RecoveryPath, poll.Gap.HeadProbeContinuationPath)
		}
	}
	seen := make(map[string]struct{})
	for _, path := range paths {
		if token := dockerRealDataProviderToken(path); token != "" {
			if _, ok := seen[token]; ok {
				continue
			}
			seen[token] = struct{}{}
			server.setExpiredProviderContinuation(token)
		}
	}
	return len(seen)
}

func (g *dockerRealDataGraphServer) skipOffset(chatID string, values url.Values, filteredLength int) (int, error) {
	raw := strings.TrimSpace(values.Get("$skiptoken"))
	if raw == "" {
		return 0, nil
	}
	const prefix = "docker-real-data-page:"
	if strings.HasPrefix(raw, prefix) {
		return dockerRealDataSkipOffset(chatID, values, filteredLength)
	}
	g.mu.Lock()
	provider, valid := g.providerTokens[raw]
	_, expired := g.expiredTokens[raw]
	g.mu.Unlock()
	if expired {
		g.expiredContinuations.Add(1)
		return 0, dockerRealDataExpiredContinuationError{token: raw}
	}
	if !valid {
		g.unknownContinuations.Add(1)
		return 0, fmt.Errorf("unknown provider opaque skiptoken %q", raw)
	}
	if provider.chatID != strings.TrimSpace(chatID) {
		return 0, fmt.Errorf("provider skiptoken chat binding mismatch %q", raw)
	}
	if provider.offset < 0 || provider.offset > filteredLength {
		return 0, fmt.Errorf("provider skiptoken offset out of range %q", raw)
	}
	return provider.offset, nil
}

func (g *dockerRealDataGraphServer) setKnownChats(chatIDs ...string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.knownChats == nil {
		g.knownChats = make(map[string]struct{})
	}
	for _, chatID := range chatIDs {
		if chatID = strings.TrimSpace(chatID); chatID != "" {
			g.knownChats[chatID] = struct{}{}
		}
	}
}

func (g *dockerRealDataGraphServer) setFaultChat(chatID string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.faultChatID = strings.TrimSpace(chatID)
	// The GET client retries 5xx responses in-process. Keep the 503 fault active
	// for the complete client retry budget so the listener must durably record a
	// failed poll and recover it in a later cycle; a single 503 would otherwise
	// be hidden by the Graph transport retry loop.
	g.faultSequence = []int{http.StatusTooManyRequests}
	for i := 0; i <= defaultGraphRetries; i++ {
		g.faultSequence = append(g.faultSequence, http.StatusServiceUnavailable)
	}
	g.mu.Unlock()
}

func (g *dockerRealDataGraphServer) consumeListFault(chatID string) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	chatID = strings.TrimSpace(chatID)
	if chatID == "" || chatID == strings.TrimSpace(g.controlChatID) || len(g.faultSequence) == 0 {
		return 0
	}
	// Bind a fault configured without a chat to the first real replay chat that
	// the production scheduler actually selects. Choosing from the full copied
	// corpus is not sufficient: a long-lived real schedule may not reach the
	// lexicographically first chat during the measured window, which would make
	// the retry path silently untested.
	if strings.TrimSpace(g.faultChatID) == "" {
		// Empty replay chats are scheduled in the copied production state too,
		// but they cannot exercise a Graph retry or pagination path. Bind the
		// injected fault only after a selected chat has an actual replay page.
		if len(g.replay[chatID]) == 0 {
			return 0
		}
		g.faultChatID = chatID
	}
	if chatID != strings.TrimSpace(g.faultChatID) {
		return 0
	}
	status := g.faultSequence[0]
	g.faultSequence = g.faultSequence[1:]
	return status
}

func (g *dockerRealDataGraphServer) setUnknownPostMarker(marker string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.unknownPostMarker = strings.TrimSpace(marker)
	g.mu.Unlock()
}

func dockerRealDataPostPayloadMatchesMarker(raw []byte, marker string) bool {
	marker = strings.TrimSpace(marker)
	if marker == "" {
		return true
	}
	var payload struct {
		Body *struct {
			Content string `json:"content"`
		} `json:"body"`
		ReplyMessage *struct {
			Body *struct {
				Content string `json:"content"`
			} `json:"body"`
		} `json:"replyMessage"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}
	if payload.Body != nil && payload.Body.Content == marker {
		return true
	}
	return payload.ReplyMessage != nil && payload.ReplyMessage.Body != nil && payload.ReplyMessage.Body.Content == marker
}

func dockerRealDataRequestDiagnostic(req *http.Request) string {
	if req == nil || req.URL == nil {
		return "<nil request>"
	}
	values := req.URL.Query()
	if values.Get("$skiptoken") != "" {
		// Provider continuation values are opaque and may contain sensitive
		// tenant/session data. They are useful to the fake server, but must never
		// be emitted in test logs or failure messages.
		values.Set("$skiptoken", "<redacted>")
	}
	uri := req.URL.Path
	if encoded := values.Encode(); encoded != "" {
		uri += "?" + encoded
	}
	return req.Method + " " + uri
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

// dockerRealDataPollListRequest identifies the list shape emitted by the
// durable Teams poller. Exact-top maintenance lookups (park notices), outbox
// reconciliation, and delegation inbox probes deliberately do not carry the
// poll's descending timestamp filter; counting those pages as poll delivery
// would make the real-data durability oracle report false missing messages.
// A filtered request is also preserved on every fake continuation link, so
// all pages of a real poll remain observable.
func dockerRealDataPollListRequest(values url.Values) bool {
	return strings.TrimSpace(values.Get("$filter")) != "" &&
		values.Get("$orderby") == "lastModifiedDateTime desc"
}

func (g *dockerRealDataGraphServer) recordPollServedMessages(messages []ChatMessage) {
	if g == nil || len(messages) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pollServedMessageIDs == nil {
		g.pollServedMessageIDs = make(map[string]int)
	}
	for _, message := range messages {
		if id := strings.TrimSpace(message.ID); id != "" {
			g.pollServedMessageIDs[id]++
		}
	}
}

func (g *dockerRealDataGraphServer) servedMessages() map[string]int {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]int, len(g.pollServedMessageIDs))
	for id, count := range g.pollServedMessageIDs {
		out[id] = count
	}
	return out
}

func (g *dockerRealDataGraphServer) recordListPage(chatID string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.pageCounts[chatID]++
	g.mu.Unlock()
}

func (g *dockerRealDataGraphServer) listPageCount(chatID string) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.pageCounts[chatID]
}

func (g *dockerRealDataGraphServer) faultChatSnapshot() string {
	if g == nil {
		return ""
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.faultChatID
}

func (g *dockerRealDataGraphServer) unknownPostChatSnapshot() string {
	if g == nil {
		return ""
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.unknownPostChat
}

func (g *dockerRealDataGraphServer) unknownPostAcceptedAttempts() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.unknownPostKey == "" {
		return 0
	}
	return g.acceptedPostKeys[g.unknownPostKey]
}

func (g *dockerRealDataGraphServer) opaqueContinuationCount() int64 {
	if g == nil {
		return 0
	}
	return g.opaqueContinuations.Load()
}

func (g *dockerRealDataGraphServer) knownChat(chatID string) bool {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if chatID == strings.TrimSpace(g.controlChatID) {
		return true
	}
	_, known := g.knownChats[chatID]
	return known
}

func dockerRealDataListFilter(values url.Values) (func(time.Time) bool, error) {
	raw := strings.TrimSpace(values.Get("$filter"))
	if raw == "" {
		return func(time.Time) bool { return true }, nil
	}
	if values.Get("$orderby") != "lastModifiedDateTime desc" {
		return nil, fmt.Errorf("filtered message request must use descending lastModifiedDateTime order")
	}
	parts := strings.Split(raw, " and ")
	if len(parts) > 2 || len(parts) == 0 {
		return nil, fmt.Errorf("unsupported message filter %q", raw)
	}
	var lower, upper time.Time
	for _, part := range parts {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) != 3 || fields[0] != "lastModifiedDateTime" || (fields[1] != "gt" && fields[1] != "lt") {
			return nil, fmt.Errorf("unsupported message filter %q", raw)
		}
		stamp, err := time.Parse(time.RFC3339Nano, fields[2])
		if err != nil || stamp.IsZero() {
			return nil, fmt.Errorf("invalid message filter bound %q", fields[2])
		}
		if fields[1] == "gt" {
			if !lower.IsZero() {
				return nil, fmt.Errorf("duplicate lower message filter bound")
			}
			// Model a Graph tenant that normalizes filter literals to its
			// millisecond provider precision before evaluating them. The
			// production formatter must widen its bounds accordingly.
			lower = stamp.Truncate(time.Millisecond)
		} else {
			if !upper.IsZero() {
				return nil, fmt.Errorf("duplicate upper message filter bound")
			}
			upper = stamp.Truncate(time.Millisecond)
		}
	}
	if !lower.IsZero() && !upper.IsZero() && upper.Before(lower) {
		return func(time.Time) bool { return false }, nil
	}
	return func(stamp time.Time) bool {
		return (lower.IsZero() || stamp.After(lower)) && (upper.IsZero() || stamp.Before(upper))
	}, nil
}

func dockerRealDataSkipOffset(chatID string, values url.Values, filteredLength int) (int, error) {
	raw := strings.TrimSpace(values.Get("$skiptoken"))
	if raw == "" {
		return 0, nil
	}
	const prefix = "docker-real-data-page:"
	if !strings.HasPrefix(raw, prefix) {
		return 0, fmt.Errorf("unknown provider opaque skiptoken %q", raw)
	}
	parts := strings.Split(strings.TrimPrefix(raw, prefix), ":")
	if len(parts) != 2 || parts[0] != shortStableID(chatID) {
		return 0, fmt.Errorf("skiptoken chat binding mismatch %q", raw)
	}
	offset, err := strconv.Atoi(parts[1])
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid opaque skiptoken %q", raw)
	}
	if offset > filteredLength {
		return 0, fmt.Errorf("opaque skiptoken offset out of range %q", raw)
	}
	return offset, nil
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

func dockerRealDataPostChatPath(path string) (chatID string, replyWithQuote bool, ok bool) {
	const prefix = "/chats/"
	if !strings.HasPrefix(path, prefix) {
		return "", false, false
	}
	rest := strings.TrimPrefix(path, prefix)
	suffix := "/messages"
	if strings.HasSuffix(rest, "/messages/replyWithQuote") {
		suffix = "/messages/replyWithQuote"
		replyWithQuote = true
	}
	if !strings.HasSuffix(rest, suffix) {
		return "", false, false
	}
	rawChatID := strings.TrimSuffix(rest, suffix)
	if strings.TrimSpace(rawChatID) == "" {
		return "", false, false
	}
	chatID, err := url.PathUnescape(rawChatID)
	if err != nil || strings.TrimSpace(chatID) == "" {
		return "", false, false
	}
	return chatID, replyWithQuote, true
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
			g.listPaths = append(g.listPaths, dockerRealDataRequestDiagnostic(req))
		}
		g.mu.Unlock()
		if chatID == "" {
			writeDockerRealDataJSON(w, struct {
				Value []ChatMessage `json:"value"`
			}{Value: []ChatMessage{}})
			return
		}
		if !g.knownChat(chatID) {
			g.recordUnknown(dockerRealDataRequestDiagnostic(req))
			http.Error(w, "unknown replay chat", http.StatusNotFound)
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
		values := req.URL.Query()
		if rawToken := strings.TrimSpace(values.Get("$skiptoken")); rawToken != "" && !strings.HasPrefix(rawToken, "docker-real-data-page:") {
			g.opaqueContinuations.Add(1)
		}
		filter, filterErr := dockerRealDataListFilter(values)
		if filterErr != nil {
			if strings.Contains(values.Get("$filter"), " ge ") || strings.Contains(values.Get("$filter"), " le ") {
				g.unsupportedFilters.Add(1)
			} else {
				g.invalidListQueries.Add(1)
			}
			http.Error(w, filterErr.Error(), http.StatusBadRequest)
			return
		}
		if rawTop := strings.TrimSpace(values.Get("$top")); rawTop != "" {
			top, topErr := strconv.Atoi(rawTop)
			if topErr != nil || top <= 0 || top > ownerPollMessageTop {
				g.invalidListQueries.Add(1)
				http.Error(w, "invalid $top", http.StatusBadRequest)
				return
			}
		}
		if order := strings.TrimSpace(values.Get("$orderby")); order != "" && order != "lastModifiedDateTime desc" {
			g.invalidListQueries.Add(1)
			http.Error(w, "invalid $orderby", http.StatusBadRequest)
			return
		}
		corpus := append([]ChatMessage(nil), g.replay[chatID]...)
		filtered := corpus[:0]
		for _, message := range corpus {
			stamp, stampErr := time.Parse(time.RFC3339Nano, message.LastModifiedDateTime)
			if stampErr != nil {
				g.invalidListQueries.Add(1)
				http.Error(w, "replay message has invalid lastModifiedDateTime", http.StatusInternalServerError)
				return
			}
			if filter(stamp) {
				filtered = append(filtered, message)
			}
		}
		offset, offsetErr := g.skipOffset(chatID, values, len(filtered))
		if offsetErr != nil {
			var expiredErr dockerRealDataExpiredContinuationError
			if errors.As(offsetErr, &expiredErr) {
				http.Error(w, offsetErr.Error(), http.StatusGone)
				return
			}
			g.invalidListQueries.Add(1)
			http.Error(w, offsetErr.Error(), http.StatusBadRequest)
			return
		}
		if values.Get("$orderby") == "lastModifiedDateTime desc" {
			sort.SliceStable(filtered, func(i, j int) bool {
				left, _ := time.Parse(time.RFC3339Nano, filtered[i].LastModifiedDateTime)
				right, _ := time.Parse(time.RFC3339Nano, filtered[j].LastModifiedDateTime)
				return left.After(right)
			})
		}
		if offset >= len(filtered) {
			g.recordListPage(chatID)
			writeDockerRealDataJSON(w, struct {
				Value []ChatMessage `json:"value"`
			}{Value: []ChatMessage{}})
			return
		}
		pageSize := dockerRealDataPageSize
		if rawTop := strings.TrimSpace(values.Get("$top")); rawTop != "" {
			if top, topErr := strconv.Atoi(rawTop); topErr == nil && top < pageSize {
				pageSize = top
			}
		}
		end := offset + pageSize
		if end > len(filtered) {
			end = len(filtered)
		}
		messages := filtered[offset:end]
		if dockerRealDataPollListRequest(values) {
			g.recordPollServedMessages(messages)
		}
		g.recordListPage(chatID)
		response := struct {
			Value    []ChatMessage `json:"value"`
			NextLink string        `json:"@odata.nextLink,omitempty"`
		}{Value: messages}
		if end < len(filtered) {
			next := url.Values{}
			if top := values.Get("$top"); top != "" {
				next.Set("$top", top)
			}
			if order := values.Get("$orderby"); order != "" {
				next.Set("$orderby", order)
			}
			if rawFilter := values.Get("$filter"); rawFilter != "" {
				next.Set("$filter", rawFilter)
			}
			next.Set("$skiptoken", "docker-real-data-page:"+shortStableID(chatID)+":"+strconv.Itoa(end))
			response.NextLink = "/chats/" + url.PathEscape(chatID) + "/messages?" + next.Encode()
		}
		writeDockerRealDataJSON(w, response)
		return
	}
	if chatID, messageID, item := dockerRealDataChatItemPath(path); item && req.Method == http.MethodGet {
		if !g.knownChat(chatID) {
			g.recordUnknown(dockerRealDataRequestDiagnostic(req))
			http.Error(w, "unknown replay chat", http.StatusNotFound)
			return
		}
		var message ChatMessage
		found := false
		for _, candidate := range g.replay[chatID] {
			if strings.TrimSpace(candidate.ID) == strings.TrimSpace(messageID) {
				message = candidate
				found = true
				break
			}
		}
		if !found {
			g.recordUnknown(dockerRealDataRequestDiagnostic(req))
			http.Error(w, "unknown replay message", http.StatusNotFound)
			return
		}
		g.itemGETs.Add(1)
		writeDockerRealDataJSON(w, message)
		return
	}
	if req.Method == http.MethodPost && strings.HasPrefix(path, "/chats/") && strings.HasSuffix(path, "/markChatUnreadForUser") {
		chatID := strings.TrimSuffix(strings.TrimPrefix(path, "/chats/"), "/markChatUnreadForUser")
		chatID, _ = url.PathUnescape(chatID)
		if !g.knownChat(chatID) {
			g.recordUnknown(dockerRealDataRequestDiagnostic(req))
			http.Error(w, "unknown replay chat", http.StatusNotFound)
			return
		}
		var payload map[string]any
		body, readErr := io.ReadAll(req.Body)
		if readErr != nil || json.Unmarshal(body, &payload) != nil || payload["user"] == nil {
			g.recordUnknown(req.Method + " " + req.URL.RequestURI())
			http.Error(w, "invalid mark-unread payload", http.StatusBadRequest)
			return
		}
		g.posts.Add(1)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if req.Method == http.MethodPost {
		chatID, replyWithQuote, messagePost := dockerRealDataPostChatPath(path)
		if !messagePost {
			// Let the route-specific handlers below classify other POSTs.
			chatID = ""
		}
		if messagePost {
			rawPayload, readErr := io.ReadAll(req.Body)
			// Identical rendered bodies in two chats are independent external
			// operations. Include the destination in the fake's diagnostic key so a
			// repeated unknown result is never misattributed across chats.
			postKey := shortStableID(chatID + "\x00" + string(rawPayload))
			if !g.knownChat(chatID) {
				g.recordUnknown(dockerRealDataRequestDiagnostic(req))
				http.Error(w, "unknown replay chat", http.StatusNotFound)
				return
			}
			var payload map[string]any
			validPayload := readErr == nil && json.Unmarshal(rawPayload, &payload) == nil
			if validPayload && replyWithQuote {
				reply, replyOK := payload["replyMessage"].(map[string]any)
				validPayload = replyOK && reply["body"] != nil
			} else if validPayload {
				validPayload = payload["body"] != nil
			}
			if !validPayload {
				g.recordUnknown(dockerRealDataRequestDiagnostic(req))
				http.Error(w, "invalid message payload", http.StatusBadRequest)
				return
			}
			g.posts.Add(1)
			g.mu.Lock()
			g.postAttempts[postKey]++
			attempt := g.postAttempts[postKey]
			// Model the remote side durably accepting the operation before the
			// response is lost. The production outbox must therefore treat the
			// first transport failure as ambiguous and never issue a second POST.
			g.acceptedPostKeys[postKey]++
			marker := strings.TrimSpace(g.unknownPostMarker)
			markerMatches := dockerRealDataPostPayloadMatchesMarker(rawPayload, marker)
			if g.unknownPostKey == "" && markerMatches {
				g.unknownPostKey = postKey
			}
			unknown := postKey == g.unknownPostKey && attempt == 1
			repeatUnknown := postKey == g.unknownPostKey && attempt > 1
			if unknown {
				g.unknownPostChat = chatID
			}
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
			message := dockerRealDataMessage(chatID, 0, int(g.posts.Load()), g.base)
			message.ID = fmt.Sprintf("docker-real-data-outbound:%06d", g.posts.Load())
			writeDockerRealDataJSON(w, message)
			return
		}
	}
	if req.Method == http.MethodGet && path == "/me" {
		writeDockerRealDataJSON(w, g.user)
		return
	}
	if req.Method == http.MethodGet && strings.HasPrefix(path, "/chats/") && strings.HasSuffix(path, "/members") {
		chatID := strings.TrimSuffix(strings.TrimPrefix(path, "/chats/"), "/members")
		chatID, _ = url.PathUnescape(chatID)
		if !g.knownChat(chatID) {
			g.recordUnknown(dockerRealDataRequestDiagnostic(req))
			http.Error(w, "unknown replay chat", http.StatusNotFound)
			return
		}
		writeDockerRealDataJSON(w, struct {
			Value []ChatMember `json:"value"`
		}{Value: []ChatMember{
			{ID: "docker-real-data-member-1", UserID: g.user.ID, DisplayName: "Docker real-data user"},
			{ID: "docker-real-data-member-2", UserID: "docker-real-data-external-user", DisplayName: "Docker real-data replay"},
		}})
		return
	}
	if req.Method == http.MethodPatch && strings.HasPrefix(path, "/chats/") {
		chatID := strings.TrimPrefix(path, "/chats/")
		if slash := strings.IndexByte(chatID, '/'); slash >= 0 {
			chatID = chatID[:slash]
		}
		chatID, _ = url.PathUnescape(chatID)
		if !g.knownChat(chatID) {
			g.recordUnknown(dockerRealDataRequestDiagnostic(req))
			http.Error(w, "unknown replay chat", http.StatusNotFound)
			return
		}
		body, readErr := io.ReadAll(req.Body)
		var payload map[string]any
		if readErr != nil || json.Unmarshal(body, &payload) != nil {
			g.recordUnknown(dockerRealDataRequestDiagnostic(req))
			http.Error(w, "invalid patch payload", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if req.Method == http.MethodPost && path == "/me/onlineMeetings" {
		writeDockerRealDataJSON(w, OnlineMeeting{ID: "docker-real-data-meeting", Subject: "Docker real-data experiment"})
		return
	}
	if req.Method == http.MethodGet {
		g.recordUnknown(dockerRealDataRequestDiagnostic(req))
		http.Error(w, "unexpected Graph route", http.StatusNotFound)
		return
	}
	g.recordUnknown(dockerRealDataRequestDiagnostic(req))
	http.Error(w, "unexpected Graph route", http.StatusNotFound)
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
	completed                    bool
	before                       dockerRealDataCounts
	after                        dockerRealDataCounts
	executorBefore               int64
	executorAfter                int64
	beforeCountsReadError        error
	afterCountsReadError         error
	stopRequestedAt              time.Time
	phaseErrorsAtMeasureStart    map[string]uint64
	phaseErrorsAtStop            map[string]uint64
	phaseDeadlinesAtMeasureStart map[string]uint64
	phaseDeadlinesAtStop         map[string]uint64
	startupPhaseErrors           map[string]uint64
	startupPhaseDeadlines        map[string]uint64
}

func dockerRealDataSyntheticInboundIDs(ctx context.Context, path string) (map[string]struct{}, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	rows, err := db.QueryContext(ctx, `SELECT teams_message_id FROM inbound_events WHERE teams_message_id LIKE ?`, dockerRealDataMessageIDPrefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id = strings.TrimSpace(id); id != "" {
			ids[id] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, rows.Close()
}

// dockerRealDataProtectedPollMessageIDs returns replay records that were
// already returned by Graph and are still covered by a durable poll receipt.
// A throughput run may stop immediately after staging a page (or after moving
// it into bounded gap evidence), before the first action in that page creates
// an inbound row. Treating those records as dropped would turn a graceful
// process boundary into a false failure; a complete run still rejects the
// non-empty receipt through dockerRealDataSyntheticResiduals.
func dockerRealDataProtectedPollMessageIDs(state teamstore.State) map[string]string {
	protected := make(map[string]string)
	addPage := func(chatID, reason string, page *teamstore.ChatPollPendingPage) {
		if page == nil {
			return
		}
		for _, id := range page.RecordIDs {
			if id = strings.TrimSpace(id); id != "" {
				protected[id] = reason
			}
		}
		// Old receipts may not have RecordIDs. Decode only the stable message ID
		// from their bounded raw records; malformed raw data is not evidence that
		// a served message was safely retained.
		if len(page.RecordIDs) == 0 {
			for _, raw := range page.Records {
				var message struct {
					ID string `json:"id"`
				}
				if json.Unmarshal(raw, &message) == nil {
					if id := strings.TrimSpace(message.ID); id != "" {
						protected[id] = reason
					}
				}
			}
		}
		_ = chatID
	}
	for chatID, poll := range state.ChatPolls {
		addPage(chatID, "pending_page", poll.PendingPage)
		if poll.Gap == nil {
			continue
		}
		addPage(chatID, "quarantined_gap_page", poll.Gap.QuarantinedPage)
		for _, id := range poll.QuarantinedRecordIDs {
			if id = strings.TrimSpace(id); id != "" {
				protected[id] = "quarantined_record"
			}
		}
	}
	return protected
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
	turnRows, err := db.QueryContext(ctx, `SELECT `+dockerRealDataTurnStatusSQL("t")+`, `+dockerRealDataTurnInboundIDSQL("t")+` FROM turns t`)
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

type dockerRealDataResiduals struct {
	InboundOperational int
	TurnUnfinished     int
	OutboxPending      int
	PollRecovery       int
}

func dockerRealDataPollHasRecovery(poll teamstore.ChatPollState) bool {
	return poll.RecoveryRequired || poll.Attempt != nil ||
		strings.TrimSpace(poll.ContinuationPath) != "" ||
		strings.TrimSpace(poll.DeferredContinuationPath) != "" ||
		poll.PendingPage != nil ||
		(poll.Gap != nil && strings.TrimSpace(poll.Gap.HeadProbeContinuationPath) != "") ||
		(poll.Gap != nil && !(poll.Gap.HeadProbePending && strings.TrimSpace(poll.Gap.RecoveryPath) == ""))
}

// dockerRealDataSyntheticResiduals is the complete-mode drain gate. It is
// deliberately scoped to the replay corpus: inherited terminal provenance in
// the copied production store is valid history and must not make a successful
// synthetic drain impossible. Unknown/non-terminal states are counted rather
// than silently ignored.
func dockerRealDataSyntheticResiduals(state teamstore.State, corpus map[string][]ChatMessage) dockerRealDataResiduals {
	var residual dockerRealDataResiduals
	for _, inbound := range state.InboundEvents {
		if !strings.HasPrefix(strings.TrimSpace(inbound.TeamsMessageID), dockerRealDataMessageIDPrefix) {
			continue
		}
		turn, found := state.Turns[strings.TrimSpace(inbound.TurnID)]
		if inbound.Status != teamstore.InboundStatusIgnored && (!found || !dockerRealDataTerminalTurn(turn.Status)) {
			residual.InboundOperational++
		}
	}
	for _, turn := range state.Turns {
		inbound, found := state.InboundEvents[strings.TrimSpace(turn.InboundEventID)]
		if !found || !strings.HasPrefix(strings.TrimSpace(inbound.TeamsMessageID), dockerRealDataMessageIDPrefix) {
			continue
		}
		if !dockerRealDataTerminalTurn(turn.Status) {
			residual.TurnUnfinished++
		}
	}
	for _, outbox := range state.OutboxMessages {
		if !strings.HasPrefix(strings.TrimSpace(outbox.TurnID), "docker-real-data-turn-") &&
			!strings.HasPrefix(strings.TrimSpace(outbox.Body), dockerRealDataExecutionPrefix) {
			continue
		}
		switch outbox.Status {
		case teamstore.OutboxStatusQueued, teamstore.OutboxStatusSending, teamstore.OutboxStatusAccepted:
			residual.OutboxPending++
		case teamstore.OutboxStatusSent, teamstore.OutboxStatusSkipped:
			if outbox.PostSendEffectsPending {
				residual.OutboxPending++
			}
		default:
			// Unknown outbox states are held by the store and are not a
			// successful drain, even if a future decoder adds more statuses.
			if strings.TrimSpace(string(outbox.Status)) != "" {
				residual.OutboxPending++
			}
		}
	}
	for chatID := range corpus {
		if poll, found := state.ChatPolls[strings.TrimSpace(chatID)]; found && dockerRealDataPollHasRecovery(poll) {
			residual.PollRecovery++
		}
	}
	return residual
}

// dockerRealDataDurableCorrelation verifies the stronger invariant that every
// synthetic inbound event has exactly one durable turn. Complete mode also
// requires that turn to reach completion; throughput mode intentionally allows
// the measured window to stop with a queued turn, while still rejecting
// duplicate admission or an inbound row that never acquired a turn.
func dockerRealDataDurableCorrelation(ctx context.Context, path string, requireCompleted bool) (int64, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return 0, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	turnStatus := dockerRealDataTurnStatusSQL("t")
	turnInboundID := dockerRealDataTurnInboundIDSQL("t")
	rows, err := db.QueryContext(ctx, `
		SELECT i.teams_message_id, t.id, `+turnStatus+`
		FROM inbound_events i
		LEFT JOIN turns t ON `+turnInboundID+` = i.id
		WHERE i.teams_message_id LIKE ?
		ORDER BY i.teams_message_id, t.id`, dockerRealDataMessageIDPrefix+"%")
	if err != nil {
		return 0, err
	}
	type correlation struct {
		turns     int
		completed int
	}
	byMessage := make(map[string]*correlation)
	for rows.Next() {
		var messageID string
		var turnID sql.NullString
		var status sql.NullString
		if err := rows.Scan(&messageID, &turnID, &status); err != nil {
			_ = rows.Close()
			return 0, err
		}
		entry := byMessage[messageID]
		if entry == nil {
			entry = &correlation{}
			byMessage[messageID] = entry
		}
		if !turnID.Valid || strings.TrimSpace(turnID.String) == "" {
			continue
		}
		entry.turns++
		if teamstore.TurnStatus(status.String) == teamstore.TurnStatusCompleted {
			entry.completed++
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	var unresolved int64
	for _, entry := range byMessage {
		if entry.turns != 1 || (requireCompleted && entry.completed != 1) {
			unresolved++
		}
	}
	return unresolved, nil
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
	turnStatus := dockerRealDataTurnStatusSQL("t")
	turnInboundID := dockerRealDataTurnInboundIDSQL("t")
	rows, err := db.QueryContext(ctx, `
		SELECT t.id, `+turnInboundID+`, t.json
		FROM turns t
		JOIN inbound_events i ON `+turnInboundID+` = i.id
		WHERE i.teams_message_id LIKE ? AND `+turnStatus+` = ?
		ORDER BY t.id`, dockerRealDataMessageIDPrefix+"%", teamstore.TurnStatusInterrupted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id string
		var inboundID sql.NullString
		var raw []byte
		if err := rows.Scan(&id, &inboundID, &raw); err != nil {
			return nil, err
		}
		digest := sha256.Sum256(raw)
		details = append(details, fmt.Sprintf("id=%s inbound_event_id=%s bytes=%d sha256=%x", id, inboundID.String, len(raw), digest))
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

func dockerRealDataMode(t *testing.T) string {
	t.Helper()
	mode := strings.ToLower(strings.TrimSpace(os.Getenv(dockerRealDataModeEnv)))
	if mode == "" {
		mode = dockerRealDataModeThroughput
	}
	if mode != dockerRealDataModeThroughput && mode != dockerRealDataModeComplete {
		t.Fatalf("invalid %s=%q; want %q or %q", dockerRealDataModeEnv, mode, dockerRealDataModeThroughput, dockerRealDataModeComplete)
	}
	return mode
}

func dockerRealDataResume(t *testing.T) bool {
	t.Helper()
	switch strings.TrimSpace(os.Getenv(dockerRealDataResumeEnv)) {
	case "", "0":
		return false
	case "1":
		return true
	default:
		t.Fatalf("invalid %s=%q; want 0 or 1", dockerRealDataResumeEnv, os.Getenv(dockerRealDataResumeEnv))
		return false
	}
}

func prepareDockerRealDataPollSchedules(t *testing.T, store *teamstore.Store, state teamstore.State, replay map[string][]ChatMessage, controlChatID string, now time.Time) (int, int) {
	t.Helper()
	if now.IsZero() {
		now = time.Now()
	}
	replayChats := 0
	emptyChats := 0
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
			emptyChats++
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
	return replayChats, emptyChats
}

func TestDockerRealDataPollListRequestExcludesMaintenanceLookups(t *testing.T) {
	filteredPoll := url.Values{}
	filteredPoll.Set("$top", "20")
	filteredPoll.Set("$orderby", "lastModifiedDateTime desc")
	filteredPoll.Set("$filter", "lastModifiedDateTime gt 2026-09-07T12:00:00Z")
	if !dockerRealDataPollListRequest(filteredPoll) {
		t.Fatal("filtered descending message request was not classified as a poll")
	}

	for name, values := range map[string]url.Values{
		"park notice exact-top lookup": {
			"$top": []string{"20"},
		},
		"outbox recovery head": {
			"$top": []string{"20"},
		},
		"unseeded baseline without filter": {
			"$top": []string{"20"},
		},
		"filtered request without descending order": {
			"$top":    []string{"20"},
			"$filter": []string{"lastModifiedDateTime gt 2026-09-07T12:00:00Z"},
		},
	} {
		if dockerRealDataPollListRequest(values) {
			t.Errorf("%s was classified as a poll: %v", name, values)
		}
	}
}

func TestDockerRealDataGraphServerEnforcesFilterAndOpaquePagination(t *testing.T) {
	chatID := "docker-contract-chat"
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	messages := make([]ChatMessage, 46)
	for i := range messages {
		stamp := base.Add(time.Duration(i) * time.Second)
		messages[i] = dockerRealDataMessage(chatID, i/dockerRealDataPageSize, i%dockerRealDataPageSize, stamp)
		messages[i].LastModifiedDateTime = stamp.Format(time.RFC3339Nano)
	}
	serverState := newDockerRealDataGraphServer("docker-contract-token", User{ID: "docker-user"}, map[string][]ChatMessage{chatID: messages})
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "docker-contract-token"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(delay time.Duration) time.Duration { return delay },
	}
	path := chatMessagesGapPath(chatID, dockerRealDataPageSize, base.Add(-time.Second), base.Add(45*time.Second))
	window, err := graph.ListMessagesWindowFromPathWithoutRateLimitRetry(context.Background(), path)
	if err != nil {
		t.Fatalf("strict Docker Graph first page: %v", err)
	}
	got := append([]ChatMessage(nil), window.Messages...)
	for window.Truncated {
		if strings.Contains(window.NextPath, "cxp_replay_page") {
			t.Fatalf("Docker Graph emitted test-only page cursor: %q", window.NextPath)
		}
		window, err = graph.ListMessagesWindowFromPathWithoutRateLimitRetry(context.Background(), window.NextPath)
		if err != nil {
			t.Fatalf("strict Docker Graph continuation: %v", err)
		}
		got = append(got, window.Messages...)
	}
	if len(got) != len(messages) {
		t.Fatalf("strict Docker Graph pagination returned %d/%d messages", len(got), len(messages))
	}
	seen := make(map[string]struct{}, len(got))
	for i, message := range got {
		if _, duplicate := seen[message.ID]; duplicate {
			t.Fatalf("strict Docker Graph pagination duplicated message %q", message.ID)
		}
		seen[message.ID] = struct{}{}
		if i > 0 && !strings.HasSuffix(got[i-1].LastModifiedDateTime, "Z") {
			t.Fatalf("message timestamp %q is not normalized", got[i-1].LastModifiedDateTime)
		}
		if i > 0 {
			previous, _ := time.Parse(time.RFC3339Nano, got[i-1].LastModifiedDateTime)
			current, _ := time.Parse(time.RFC3339Nano, message.LastModifiedDateTime)
			if current.After(previous) {
				t.Fatalf("strict Docker Graph order moved newer at index %d: %s then %s", i, got[i-1].LastModifiedDateTime, message.LastModifiedDateTime)
			}
		}
	}
	if serverState.listPageCount(chatID) < 3 {
		t.Fatalf("strict Docker Graph pagination used %d pages, want at least 3", serverState.listPageCount(chatID))
	}
	validItem := httptest.NewRequest(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/messages/"+url.PathEscape(messages[0].ID), nil)
	validItem.Header.Set("Authorization", "Bearer docker-contract-token")
	validItemRecorder := httptest.NewRecorder()
	serverState.ServeHTTP(validItemRecorder, validItem)
	if validItemRecorder.Code != http.StatusOK {
		t.Fatalf("Docker Graph known message item response = %d, want 200", validItemRecorder.Code)
	}
	unknownItem := httptest.NewRequest(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/messages/not-in-replay", nil)
	unknownItem.Header.Set("Authorization", "Bearer docker-contract-token")
	unknownItemRecorder := httptest.NewRecorder()
	serverState.ServeHTTP(unknownItemRecorder, unknownItem)
	if unknownItemRecorder.Code != http.StatusNotFound {
		t.Fatalf("Docker Graph unknown message item response = %d, want 404", unknownItemRecorder.Code)
	}
	bad := httptest.NewRequest(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/messages?$top=20&$orderby=lastModifiedDateTime%20desc&$filter=lastModifiedDateTime%20ge%20"+base.Format(time.RFC3339Nano), nil)
	bad.Header.Set("Authorization", "Bearer docker-contract-token")
	badRecorder := httptest.NewRecorder()
	serverState.ServeHTTP(badRecorder, bad)
	if badRecorder.Code != http.StatusBadRequest || serverState.unsupportedFilters.Load() != 1 {
		t.Fatalf("Docker Graph unsupported filter response = code %d unsupported=%d, want 400/1", badRecorder.Code, serverState.unsupportedFilters.Load())
	}
	// A real copied poll row can carry a provider-issued opaque continuation
	// token. The fake accepts it only when the test explicitly registers the
	// provider's server-side meaning; arbitrary opaque text must not become a
	// successful oldest-page fallback.
	values := url.Values{}
	values.Set("$top", "20")
	providerToken := "Source=MessagingFrontEnd##ContinuationToken=provider-issued-opaque-token"
	values.Set("$skiptoken", providerToken)
	serverState.setValidProviderContinuation(providerToken, chatID, len(messages)-dockerRealDataPageSize)
	opaqueRequest := httptest.NewRequest(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/messages?"+values.Encode(), nil)
	opaqueRequest.Header.Set("Authorization", "Bearer docker-contract-token")
	opaqueRecorder := httptest.NewRecorder()
	serverState.ServeHTTP(opaqueRecorder, opaqueRequest)
	if opaqueRecorder.Code != http.StatusOK {
		t.Fatalf("Docker Graph provider opaque continuation response = %d, want 200", opaqueRecorder.Code)
	}
	if serverState.opaqueContinuationCount() != 1 {
		t.Fatalf("Docker Graph did not exercise exactly one provider opaque continuation: count=%d", serverState.opaqueContinuationCount())
	}
	unknownValues := url.Values{}
	unknownValues.Set("$top", "20")
	unknownValues.Set("$skiptoken", "provider-token-not-registered")
	unknownRequest := httptest.NewRequest(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/messages?"+unknownValues.Encode(), nil)
	unknownRequest.Header.Set("Authorization", "Bearer docker-contract-token")
	unknownRecorder := httptest.NewRecorder()
	serverState.ServeHTTP(unknownRecorder, unknownRequest)
	if unknownRecorder.Code != http.StatusBadRequest || serverState.unknownContinuations.Load() != 1 {
		t.Fatalf("Docker Graph unknown provider opaque continuation response = code %d unknown=%d, want 400/1", unknownRecorder.Code, serverState.unknownContinuations.Load())
	}
	expiredToken := "Source=MessagingFrontEnd##ContinuationToken=expired-provider-token"
	serverState.setExpiredProviderContinuation(expiredToken)
	expiredValues := url.Values{}
	expiredValues.Set("$top", "20")
	expiredValues.Set("$skiptoken", expiredToken)
	expiredRequest := httptest.NewRequest(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/messages?"+expiredValues.Encode(), nil)
	expiredRequest.Header.Set("Authorization", "Bearer docker-contract-token")
	expiredRecorder := httptest.NewRecorder()
	serverState.ServeHTTP(expiredRecorder, expiredRequest)
	if expiredRecorder.Code != http.StatusGone || serverState.expiredContinuationCount() != 1 {
		t.Fatalf("Docker Graph expired provider opaque continuation response = code %d expired=%d, want 410/1", expiredRecorder.Code, serverState.expiredContinuationCount())
	}
	for _, rawToken := range []string{
		"docker-real-data-page:other-chat:20",
		"docker-real-data-page:" + shortStableID(chatID) + ":not-an-offset",
	} {
		invalidValues := url.Values{}
		invalidValues.Set("$top", "20")
		invalidValues.Set("$skiptoken", rawToken)
		invalidRequest := httptest.NewRequest(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/messages?"+invalidValues.Encode(), nil)
		invalidRequest.Header.Set("Authorization", "Bearer docker-contract-token")
		invalidRecorder := httptest.NewRecorder()
		serverState.ServeHTTP(invalidRecorder, invalidRequest)
		if invalidRecorder.Code != http.StatusBadRequest {
			t.Fatalf("Docker Graph invalid opaque token %q response = %d, want 400", rawToken, invalidRecorder.Code)
		}
	}
}

func TestDockerRealDataGraphServerPaginatesEqualTimestampBucket(t *testing.T) {
	chatID := "docker-equal-timestamp-chat"
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	const messageCount = dockerRealDataPageSize*2 + 7
	messages := make([]ChatMessage, messageCount)
	for i := range messages {
		messages[i] = dockerRealDataMessage(chatID, 0, i, base)
		// Graph may return a large same-millisecond bucket. The provider's
		// nextLink, rather than an invented ID order, is the only safe way to
		// enumerate beyond one page in that case.
		messages[i].CreatedDateTime = base.Format(time.RFC3339Nano)
		messages[i].LastModifiedDateTime = base.Format(time.RFC3339Nano)
	}
	serverState := newDockerRealDataGraphServer("docker-equal-timestamp-token", User{ID: "docker-equal-timestamp-user"}, map[string][]ChatMessage{
		chatID: messages,
	})
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)
	graph := &GraphClient{
		auth:       &fakeGraphAuth{token: "docker-equal-timestamp-token"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(delay time.Duration) time.Duration { return delay },
	}

	path := chatMessagesGapPath(chatID, dockerRealDataPageSize, base.Add(-time.Second), base)
	window, err := graph.ListMessagesWindowFromPathWithoutRateLimitRetry(context.Background(), path)
	if err != nil {
		t.Fatalf("equal-timestamp first page: %v", err)
	}
	got := append([]ChatMessage(nil), window.Messages...)
	for window.Truncated {
		window, err = graph.ListMessagesWindowFromPathWithoutRateLimitRetry(context.Background(), window.NextPath)
		if err != nil {
			t.Fatalf("equal-timestamp continuation: %v", err)
		}
		got = append(got, window.Messages...)
	}
	if len(got) != messageCount {
		t.Fatalf("equal-timestamp pagination returned %d/%d messages", len(got), messageCount)
	}
	seen := make(map[string]struct{}, len(got))
	for _, message := range got {
		if _, duplicate := seen[message.ID]; duplicate {
			t.Fatalf("equal-timestamp pagination duplicated message %q", message.ID)
		}
		seen[message.ID] = struct{}{}
		if message.LastModifiedDateTime != base.Format(time.RFC3339Nano) {
			t.Fatalf("equal-timestamp message changed timestamp: %q", message.LastModifiedDateTime)
		}
	}
	if serverState.listPageCount(chatID) != 3 {
		t.Fatalf("equal-timestamp pagination used %d pages, want 3", serverState.listPageCount(chatID))
	}
}

func TestDockerRealDataGraphServerParsesMessagePostRoutes(t *testing.T) {
	chatID := "19:docker-post-chat@thread.v2"
	for _, test := range []struct {
		path  string
		quote bool
	}{
		{path: "/chats/" + url.PathEscape(chatID) + "/messages"},
		{path: "/chats/" + url.PathEscape(chatID) + "/messages/replyWithQuote", quote: true},
	} {
		gotChat, gotQuote, ok := dockerRealDataPostChatPath(test.path)
		if !ok || gotChat != chatID || gotQuote != test.quote {
			t.Fatalf("dockerRealDataPostChatPath(%q) = chat=%q quote=%v ok=%v, want %q/%v/true", test.path, gotChat, gotQuote, ok, chatID, test.quote)
		}
	}

	serverState := newDockerRealDataGraphServer("docker-post-token", User{ID: "docker-post-user"}, map[string][]ChatMessage{
		chatID: nil,
	})
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)
	client := server.Client()
	post := func(path string, payload string) (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer docker-post-token")
		return client.Do(req)
	}
	// The first valid ordinary message POST is deliberately made ambiguous by
	// this fake. A transport error (rather than 400 invalid-payload) proves the
	// route and body were accepted before the injected unknown result.
	if response, err := post("/chats/"+url.PathEscape(chatID)+"/messages", `{"body":{"content":"ordinary"}}`); err == nil {
		_ = response.Body.Close()
		t.Fatal("ordinary message POST unexpectedly returned a response for the injected unknown result")
	}
	response, err := post("/chats/"+url.PathEscape(chatID)+"/messages/replyWithQuote", `{"messageIds":["source"],"replyMessage":{"body":{"content":"quoted"}}}`)
	if err != nil {
		t.Fatalf("quoted message POST: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("quoted message POST status = %d, want 200", response.StatusCode)
	}
	if got := serverState.unknownPosts.Load(); got != 1 {
		t.Fatalf("valid message POST unknown count = %d, want 1", got)
	}
	if got := serverState.posts.Load(); got != 2 {
		t.Fatalf("valid message POST count = %d, want 2", got)
	}
}

func TestDockerRealDataGraphServerUnknownPostMarkerSkipsUnrelatedOutbox(t *testing.T) {
	chatID := "docker-marker-chat"
	serverState := newDockerRealDataGraphServer("docker-marker-token", User{ID: "docker-marker-user"}, map[string][]ChatMessage{chatID: nil})
	serverState.setUnknownPostMarker("target-final")
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)
	post := func(content string) error {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/chats/"+url.PathEscape(chatID)+"/messages", strings.NewReader(`{"body":{"content":"`+content+`"}}`))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer docker-marker-token")
		response, err := server.Client().Do(req)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return err
	}
	if err := post("unrelated"); err != nil {
		t.Fatalf("unrelated POST: %v", err)
	}
	if got := serverState.unknownPosts.Load(); got != 0 {
		t.Fatalf("unrelated POST consumed unknown-result fault: %d", got)
	}
	if err := post("target-final"); err == nil {
		t.Fatal("target POST unexpectedly returned a response after unknown-result injection")
	}
	if got := serverState.unknownPosts.Load(); got != 1 {
		t.Fatalf("target POST unknown-result count = %d, want 1", got)
	}
}

func TestDockerRealDataPostMarkerRequiresExactRenderedBody(t *testing.T) {
	marker := "docker real-data execution result #1"
	for name, payload := range map[string]string{
		"ordinary":         `{"body":{"content":"docker real-data execution result #1"}}`,
		"quoted":           `{"messageIds":["source"],"replyMessage":{"body":{"content":"docker real-data execution result #1"}}}`,
		"suffix":           `{"body":{"content":"docker real-data execution result #10"}}`,
		"nested-unrelated": `{"body":{"content":"unrelated","metadata":"docker real-data execution result #1"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			want := name == "ordinary" || name == "quoted"
			if got := dockerRealDataPostPayloadMatchesMarker([]byte(payload), marker); got != want {
				t.Fatalf("marker match = %v, want %v for payload %s", got, want, payload)
			}
		})
	}
}

func TestDockerRealDataRequestDiagnosticRedactsOpaqueContinuation(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/chats/real-chat/messages?$top=20&$skiptoken=provider-secret-token", nil)
	diagnostic := dockerRealDataRequestDiagnostic(req)
	if strings.Contains(diagnostic, "provider-secret-token") {
		t.Fatalf("request diagnostic leaked opaque continuation: %q", diagnostic)
	}
	if !strings.Contains(diagnostic, "%24skiptoken=%3Credacted%3E") {
		t.Fatalf("request diagnostic did not retain a redaction marker: %q", diagnostic)
	}
}

func TestDockerRealDataReplayCorpusPersistsAcrossProcessBoundary(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state", "state.json")
	message := ChatMessage{
		ID:                   dockerRealDataMessageIDPrefix + "message-1",
		ChatID:               "persisted-chat",
		MessageType:          "message",
		CreatedDateTime:      "2026-09-07T12:00:00Z",
		LastModifiedDateTime: "2026-09-07T12:00:00Z",
	}
	message.Body.ContentType = "html"
	message.Body.Content = "@codex persisted"
	corpus := map[string][]ChatMessage{
		"persisted-chat": {message},
	}
	writeDockerRealDataReplayCorpus(t, statePath, corpus)
	got, count := readDockerRealDataReplayCorpus(t, statePath)
	if count != 1 || len(got["persisted-chat"]) != 1 || got["persisted-chat"][0].Body.Content != "@codex persisted" {
		t.Fatalf("persisted Docker replay corpus = %#v count=%d, want one unchanged message", got, count)
	}
}

// TestDockerRealDataTeamsProgressThroughput is an opt-in experiment, not a
// smoke test. It runs the actual listener loop against a point-in-time copy of
// the current Teams SQLite state, registry projection, shared ledgers, and a
// copied Codex session tree. Only the external token/Graph boundary and Codex
// executor are replaced. The test is intentionally skipped
// unless the Docker runner supplies CXP_TEAMS_DOCKER_REAL_DATA_EXPERIMENT=1.
func TestDockerRealDataTeamsProgressThroughput(t *testing.T) {
	if os.Getenv(dockerRealDataExperimentEnv) != "1" {
		t.Skipf("set %s=1 to run the real-data Docker experiment", dockerRealDataExperimentEnv)
	}
	mode := dockerRealDataMode(t)
	resume := dockerRealDataResume(t)
	fixtureRoot := dockerTeamsFixtureRoot(t)
	store, statePath := prepareDockerFixtureStore(t, fixtureRoot)
	dockerFixtureRemapCodexPaths(t, store)
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
	if !resume && beforeCounts.inbound != 0 {
		t.Fatalf("copied runtime already contains synthetic inbound rows before experiment: %d", beforeCounts.inbound)
	}
	inheritedInbound, inheritedTurns, inheritedOutbox := dockerRealDataInheritedOperationalRows(beforeState)
	t.Logf("real-data source audit: inherited operational Teams inbound=%d turns=%d outbox=%d; retained in disposable fixture and excluded from synthetic counters; terminal queued provenance is excluded from inbound count", inheritedInbound, inheritedTurns, inheritedOutbox)
	if resume {
		ambiguous := dockerRealDataAmbiguousExecutionOutboxes(beforeState)
		if len(ambiguous) != 1 {
			t.Fatalf("real-data resume expected exactly one durable ambiguous first execution outbox from the previous process, got=%d rows=%#v", len(ambiguous), ambiguous)
		}
		t.Logf("real-data resume audit: retained one ambiguous first execution outbox=%q; the second process must not POST it again", ambiguous[0].ID)
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
	dockerFixtureSanitizeRegistryWorkspacePaths(&registry)
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
	var replayCorpus map[string][]ChatMessage
	var replayCount int
	var replayReport dockerRealDataReplayReport
	if resume {
		replayCorpus, replayCount = readDockerRealDataReplayCorpus(t, statePath)
		t.Logf("real-data replay resume: loaded persisted corpus messages=%d chats=%d from disposable runtime", replayCount, len(replayCorpus))
	} else {
		replayCorpus, replayCount, replayReport = dockerRealDataReplayCorpus(beforeState, beforeState.ControlChat.TeamsChatID)
		t.Logf("real-data replay source audit: %+v", replayReport)
		if replayReport.ExcludedMalformed > 0 || replayReport.ExcludedEmptyActionable > 0 || replayReport.ExcludedUnsupported > 0 {
			t.Fatalf("copied real-data snapshot contains active queued rows outside the ordinary replay contract: %+v; refusing a partial/false-green experiment", replayReport)
		}
		if replayReport.Accepted != replayCount || replayReport.Accepted+replayReport.ExcludedMalformed+replayReport.ExcludedDashboard+replayReport.ExcludedEmptyTerminal+replayReport.ExcludedEmptyActionable+replayReport.ExcludedAttachments+replayReport.ExcludedHostedContent+replayReport.ExcludedControlChat+replayReport.ExcludedNoActiveChat != replayReport.QueuedTeams {
			t.Fatalf("real-data replay source audit does not account for every queued Teams row: %+v", replayReport)
		}
		writeDockerRealDataReplayCorpus(t, statePath, replayCorpus)
	}
	if replayCount < dockerRealDataMinimumReplay {
		t.Fatalf("copied real-data snapshot produced only %d ordinary queued Teams messages for replay; refusing a smoke-sized workload", replayCount)
	}
	t.Logf("real-data replay corpus: queued Teams payloads=%d chats=%d; bodies/authors/order copied from durable rows, only IDs/timestamps remapped", replayCount, len(replayCorpus))
	replayChats, emptyChats := prepareDockerRealDataPollSchedules(t, store, beforeState, replayCorpus, beforeState.ControlChat.TeamsChatID, time.Now())
	t.Logf("real-data disposable schedule: replay chats due=%d active chats with empty fake corpus also due=%d; no active chat is held for 24h; source schedule/database is untouched", replayChats, emptyChats)

	const graphToken = "docker-real-data-deterministic-token"
	graphServerState := newDockerRealDataGraphServer(graphToken, user, replayCorpus)
	graphServerState.controlChatID = beforeState.ControlChat.TeamsChatID
	// Arm the unknown-result fault only for the synthetic final body emitted by
	// the disposable executor. This prevents an old/control outbox row copied
	// from the source snapshot from consuming the fault and making the experiment
	// pass without exercising the target replay turn.
	// Match only the first synthetic executor result by exact rendered body.
	// A substring match could let an inherited/terminal outbox row consume the
	// fault before the replay lane reaches its own unknown POST boundary.
	unknownFaultEnabled := !resume && mode != dockerRealDataModeComplete
	if !unknownFaultEnabled {
		// A process restart must not replay the first process's unknown POST.  A
		// non-empty impossible marker is used because an empty marker means
		// "match every POST" in the deterministic Graph server. Complete mode
		// uses a separate drain gate, so it does not intentionally leave an
		// ambiguous final outbox behind.
		graphServerState.setUnknownPostMarker("__docker_real_data_unknown_post_disabled__")
	} else {
		graphServerState.setUnknownPostMarker(dockerRealDataExecutionPrefix + "1")
	}
	knownChats := []string{beforeState.ControlChat.TeamsChatID}
	for _, session := range beforeState.Sessions {
		if !isActiveSessionStatus(string(session.Status)) {
			continue
		}
		if chatID := strings.TrimSpace(session.TeamsChatID); chatID != "" {
			knownChats = append(knownChats, chatID)
		}
	}
	// Active chats whose copied queue contains only attachments, dashboard
	// commands, or no ordinary replayable row are still valid production chats.
	// Keep them in the fake Graph's namespace and return an empty page instead
	// of manufacturing a 404 that would look like a listener bug.
	graphServerState.setKnownChats(knownChats...)
	expiredProviderTokens := dockerRealDataExpiredProviderTokens(graphServerState, beforeState)
	t.Logf("real-data persisted provider continuations marked expired in isolated Graph: %d; arbitrary opaque tokens remain invalid", expiredProviderTokens)
	// Bind the retryable fault to the first replay chat that the production
	// scheduler actually selects. Picking an arbitrary corpus entry can leave
	// the fault unobserved when the real schedule does not reach that chat in
	// the measured window.
	graphServerState.setFaultChat("")
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
	linkedUnindexed := make(map[string]bool)
	for _, session := range beforeState.Sessions {
		if strings.TrimSpace(session.ID) == "" || strings.TrimSpace(session.CodexThreadID) == "" ||
			strings.TrimSpace(session.TeamsChatID) == "" || strings.TrimSpace(session.TeamsChatID) == strings.TrimSpace(beforeState.ControlChat.TeamsChatID) ||
			!isActiveSessionStatus(string(session.Status)) {
			continue
		}
		if _, found := beforeState.ImportCheckpoints[transcriptCheckpointID(session.ID)]; !found {
			linkedUnindexed[session.ID] = true
		}
	}
	maintenanceTrace := &dockerRealDataMaintenanceTrace{
		store:            store,
		historyMandatory: historyMandatory,
		linkedMandatory:  linkedMandatory,
		linkedUnindexed:  linkedUnindexed,
		syntheticBacklog: func(checkCtx context.Context) (bool, error) {
			counts, countErr := dockerRealDataCountsFromSQLite(checkCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
			if countErr != nil {
				return false, countErr
			}
			return counts.queued > 0 || counts.running > 0, nil
		},
	}
	t.Logf("real-data linked discovery candidates: unindexed_active_sessions=%d", len(linkedUnindexed))
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
	var measuredWindowInbound []int64
	var measuredWindowCompletedDeltas []int64
	var lastListenerErr error
	phaseErrorsAtMeasureStart := make(map[string]uint64)
	phaseErrorsAtStop := make(map[string]uint64)
	phaseDeadlinesAtMeasureStart := make(map[string]uint64)
	phaseDeadlinesAtStop := make(map[string]uint64)
	startupPhaseErrors := make(map[string]uint64)
	startupPhaseDeadlines := make(map[string]uint64)
	var gracefulStopAt []time.Time
	phaseNames := dockerRealDataProductionPhaseNames()
	runListener := func(activeStore *teamstore.Store) error {
		runRegistry, registryErr := LoadRegistry(registryPath)
		if registryErr != nil {
			return fmt.Errorf("reload copied registry projection for listener run: %w", registryErr)
		}
		dockerFixtureSanitizeRegistryWorkspacePaths(&runRegistry)
		if strings.TrimSpace(runRegistry.ControlChatID) == "" {
			runRegistry.ControlChatID = beforeState.ControlChat.TeamsChatID
		}
		runRegistry.UserID = user.ID
		runRegistry.UserPrincipal = user.UserPrincipalName
		bridge := &Bridge{
			graph:                    graph,
			readGraph:                graph,
			registryPath:             registryPath,
			reg:                      runRegistry,
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
		bridge.pollChatTraceHook = traceWriter.recordPollChat
		bridge.historyWatchPathHook = maintenanceTrace.observeHistory
		bridge.linkedTranscriptSessionHook = maintenanceTrace.observeLinked
		cycleDone := make(chan struct{}, 1)
		var measuredWindowCompleted atomic.Bool
		var startupTimedOut atomic.Bool
		var measuredWindow dockerRealDataMeasuredWindow
		bridge.mainLoopCycleDoneHook = func() {
			select {
			case cycleDone <- struct{}{}:
			default:
			}
		}
		ownerTrace := startDockerRealDataOwnerTrace(bridge)
		listenCtx, listenCancel := context.WithCancel(ctx)
		// Keep listener startup observable.  If initialization returns before the
		// first cycle, waiting only on cycleDone would hide the real error behind
		// the five-minute startup watchdog and make a Docker failure look like a
		// hang.
		listenDone := make(chan struct{})
		var listenErr error
		var startupFailure error
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
				// Record the first complete cycle separately from the measured
				// steady window. Otherwise an error in startup/first-cycle
				// recovery could be hidden by taking the measured baseline only
				// after that cycle completed.
				measuredWindow.startupPhaseErrors = make(map[string]uint64, len(phaseNames))
				measuredWindow.startupPhaseDeadlines = make(map[string]uint64, len(phaseNames))
				for _, phaseName := range phaseNames {
					stats := bridge.mainLoopPhaseStatsSnapshot(phaseName)
					measuredWindow.startupPhaseErrors[phaseName] = stats.Errors
					measuredWindow.startupPhaseDeadlines[phaseName] = stats.DeadlineExceeded
				}
			case <-listenDone:
				startupTimedOut.Store(true)
				if listenErr != nil {
					startupFailure = listenErr
				} else {
					startupFailure = errors.New("listener returned before its first main-loop cycle")
				}
				t.Logf("real-data listener returned before its first main-loop cycle: err=%v phases=%#v lease_claims=%v owner_failures=%v lines=%v", listenErr, bridge.mainLoopPhaseStatsSnapshot("poll"), traceWriter.leaseClaimsSnapshot(), traceWriter.ownerFailuresSnapshot(), traceWriter.linesSnapshot())
				return
			case <-startupDeadline.C:
				startupTimedOut.Store(true)
				startupFailure = errors.New("first main-loop cycle did not complete before the startup deadline")
				t.Logf("real-data listener did not complete its first main-loop cycle before the startup deadline: phases=%#v lease_claims=%v owner_failures=%v lines=%v", bridge.mainLoopPhaseStatsSnapshot("poll"), traceWriter.leaseClaimsSnapshot(), traceWriter.ownerFailuresSnapshot(), traceWriter.linesSnapshot())
				listenCancel()
				return
			case <-ctx.Done():
				return
			}
			measuredWindow.before, measuredWindow.beforeCountsReadError = dockerRealDataCountsFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
			if measuredWindow.beforeCountsReadError != nil {
				measuredWindow.stopRequestedAt = time.Now()
				measuredWindowCompleted.Store(true)
				listenCancel()
				return
			}
			measuredWindow.executorBefore = executor.runs.Load()
			measuredWindow.phaseErrorsAtMeasureStart = make(map[string]uint64, len(phaseNames))
			measuredWindow.phaseDeadlinesAtMeasureStart = make(map[string]uint64, len(phaseNames))
			for _, phaseName := range phaseNames {
				stats := bridge.mainLoopPhaseStatsSnapshot(phaseName)
				measuredWindow.phaseErrorsAtMeasureStart[phaseName] = stats.Errors
				measuredWindow.phaseDeadlinesAtMeasureStart[phaseName] = stats.DeadlineExceeded
			}
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-timer.C:
				measuredWindowCompleted.Store(true)
				measuredWindow.executorAfter = executor.runs.Load()
				measuredWindow.after, measuredWindow.afterCountsReadError = dockerRealDataCountsFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
				measuredWindow.completed = measuredWindow.afterCountsReadError == nil
				measuredWindow.stopRequestedAt = time.Now()
				measuredWindow.phaseErrorsAtStop = make(map[string]uint64, len(phaseNames))
				measuredWindow.phaseDeadlinesAtStop = make(map[string]uint64, len(phaseNames))
				for _, phaseName := range phaseNames {
					stats := bridge.mainLoopPhaseStatsSnapshot(phaseName)
					measuredWindow.phaseErrorsAtStop[phaseName] = stats.Errors
					measuredWindow.phaseDeadlinesAtStop[phaseName] = stats.DeadlineExceeded
				}
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
			// A poll handler can persist an inbound event and queue its turn in
			// separate durable operations. Do not cancel the listener in that small
			// interval: otherwise the experiment would manufacture an orphaned
			// inbound row at its own stop boundary and report a false liveness
			// failure. The phase itself remains bounded by the production cleanup
			// grace; a genuinely stuck phase is still canceled after this watchdog.
			pollDeadline := time.Now().Add(30 * time.Second)
			for {
				if bridge.mainLoopPhaseStatsSnapshot("poll").Active == 0 {
					break
				}
				if !time.Now().Before(pollDeadline) {
					t.Log("real-data listener had a poll phase still active at graceful-stop deadline")
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			// Stop observing before canceling the listener.  Cancellation causes the
			// owner cleanup path to release its lease; if observation continues until
			// Listen returns, the test can mistake that intentional teardown (or a
			// brief re-claim while cancellation propagates) for an owner loss during
			// the measured workload.
			ownerTrace.stopTrace()
			listenCancel()
		}()
		go func() {
			defer close(listenDone)
			listenErr = bridge.Listen(listenCtx, BridgeOptions{
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
		}()
		<-stopCoordinatorDone
		listenCancel()
		<-listenDone
		ownerTrace.stopTrace()
		if startupTimedOut.Load() {
			if startupFailure != nil {
				return fmt.Errorf("real-data listener startup did not complete a measured cycle: %w", startupFailure)
			}
			return fmt.Errorf("real-data listener startup did not complete a measured cycle before the startup deadline")
		}
		generations, changes := ownerTrace.snapshot()
		ownerGenerations = append(ownerGenerations, generations...)
		ownerChangesByRun = append(ownerChangesByRun, changes)
		ownerChanges += changes
		if measuredWindowCompleted.Load() {
			measuredWindows++
			if measuredWindow.beforeCountsReadError != nil {
				return fmt.Errorf("read measured Docker window baseline counters: %w", measuredWindow.beforeCountsReadError)
			}
			if measuredWindow.afterCountsReadError != nil {
				return fmt.Errorf("read measured Docker window final counters: %w", measuredWindow.afterCountsReadError)
			}
			measuredInbound += measuredWindow.after.inbound - measuredWindow.before.inbound
			measuredCompleted += measuredWindow.after.completed - measuredWindow.before.completed
			measuredExecutorRuns += measuredWindow.executorAfter - measuredWindow.executorBefore
			measuredWindowInbound = append(measuredWindowInbound, measuredWindow.after.inbound-measuredWindow.before.inbound)
			measuredWindowCompletedDeltas = append(measuredWindowCompletedDeltas, measuredWindow.after.completed-measuredWindow.before.completed)
			for phaseName, count := range measuredWindow.phaseErrorsAtMeasureStart {
				phaseErrorsAtMeasureStart[phaseName] += count
			}
			for phaseName, count := range measuredWindow.phaseErrorsAtStop {
				phaseErrorsAtStop[phaseName] += count
			}
			for phaseName, count := range measuredWindow.phaseDeadlinesAtMeasureStart {
				phaseDeadlinesAtMeasureStart[phaseName] += count
			}
			for phaseName, count := range measuredWindow.phaseDeadlinesAtStop {
				phaseDeadlinesAtStop[phaseName] += count
			}
			for phaseName, count := range measuredWindow.startupPhaseErrors {
				startupPhaseErrors[phaseName] += count
			}
			for phaseName, count := range measuredWindow.startupPhaseDeadlines {
				startupPhaseDeadlines[phaseName] += count
			}
			if !measuredWindow.stopRequestedAt.IsZero() {
				gracefulStopAt = append(gracefulStopAt, measuredWindow.stopRequestedAt)
			}
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
	if measuredWindows != 1 {
		t.Fatalf("real-data listener did not complete its measured window: got=%d want=1", measuredWindows)
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
	residuals := dockerRealDataSyntheticResiduals(afterState, replayCorpus)
	if mode == dockerRealDataModeComplete && residuals != (dockerRealDataResiduals{}) {
		t.Fatalf("complete real-data drain left synthetic durable work: %+v", residuals)
	}
	t.Logf("real-data synthetic residual audit: %+v (complete_mode=%t)", residuals, mode == dockerRealDataModeComplete)
	afterHistoryOffsets := dockerRealDataHistoryOffsetSum(afterState)
	inboundDelta := afterCounts.inbound - beforeCounts.inbound
	completedDelta := afterCounts.completed - beforeCounts.completed
	resumeWorkExpected := beforeCounts.inbound < int64(replayCount) || beforeCounts.queued > 0 || beforeCounts.running > 0
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
	// Exclude startup and teardown from the throughput denominator. The measured
	// listener window is explicitly bounded by the coordinator timer above; the
	// full wall clock remains useful as a separate diagnostic for startup cost.
	steadyElapsed := duration * time.Duration(measuredWindows)
	if steadyElapsed <= 0 {
		steadyElapsed = runElapsed
	}
	unknown := graphServerState.unknownPaths()
	servedMessages := graphServerState.servedMessages()
	persistedSyntheticIDs, err := dockerRealDataSyntheticInboundIDs(postCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
	if err != nil {
		t.Fatalf("read Graph-served synthetic inbound IDs: %v", err)
	}
	protectedPollMessageIDs := dockerRealDataProtectedPollMessageIDs(afterState)
	if len(servedMessages) == 0 && (!resume || resumeWorkExpected) {
		t.Fatalf("real-data fake Graph served no replay messages; throughput would be an unmeasured scheduler result")
	}
	missingDurableInbound := make([]string, 0)
	protectedServedMessages := 0
	for chatID, messages := range replayCorpus {
		for _, message := range messages {
			messageID := strings.TrimSpace(message.ID)
			if _, served := servedMessages[messageID]; !served {
				continue
			}
			if _, found := persistedSyntheticIDs[messageID]; found {
				continue
			}
			if _, protected := protectedPollMessageIDs[messageID]; protected {
				protectedServedMessages++
				continue
			}
			// Keep this diagnostic body-free: the corpus is copied from real Teams
			// conversations. A served-but-not-durable row can be either a genuinely
			// ignored helper echo or a page that was still protected by a durable
			// pending receipt at the graceful-stop boundary; the classification below
			// makes that distinction reviewable without leaking prompt text.
			authorID := chatMessageAuthorUserID(message)
			self := messageAuthoredByCurrentUser(message, user)
			plainText := PlainTextFromTeamsHTML(message.Body.Content)
			renderedOutput := self && (looksLikeRenderedOutboxPlainText(plainText) ||
				looksLikeRenderedHelperGeneratedOutputPlainText(plainText) ||
				looksLikeRenderedHelperOutputMessage(message, plainText))
			missingDurableInbound = append(missingDurableInbound, fmt.Sprintf("id=%s chat=%s author=%s self=%t type=%s body_bytes=%d mention=%t rendered_output=%t", messageID, chatID, authorID, self, message.MessageType, len(message.Body.Content), teamsMessageHasCodexMention(message, plainText), renderedOutput))
		}
	}
	t.Logf("real-data served-message durability: graph_unique=%d persisted_inbound=%d protected_by_poll_receipt=%d", len(servedMessages), len(persistedSyntheticIDs), protectedServedMessages)
	if len(missingDurableInbound) > 0 {
		if len(missingDurableInbound) > 16 {
			missingDurableInbound = missingDurableInbound[:16]
		}
		t.Fatalf("fake Graph served replay messages without durable inbound events (first %d): %v", len(missingDurableInbound), missingDurableInbound)
	}
	duplicateServedMessages := 0
	for _, count := range servedMessages {
		if count > 1 {
			duplicateServedMessages += count - 1
		}
	}
	backlogSamples, ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog := maintenanceTrace.snapshot()
	ordinaryHistoryPaths, ordinaryLinkedSessions := maintenanceTrace.ordinaryWorkSnapshot()
	ordinaryUnindexedLinked := maintenanceTrace.ordinaryUnindexedLinkedSnapshot()
	ordinaryUnindexedLinkedIDs := maintenanceTrace.ordinaryUnindexedLinkedIDsSnapshot()
	phaseStats := func(name string) mainLoopPhaseStats {
		var total mainLoopPhaseStats
		for _, bridge := range bridges {
			current := bridge.mainLoopPhaseStatsSnapshot(name)
			total.Runs += current.Runs
			total.DeadlineExceeded += current.DeadlineExceeded
			total.Errors += current.Errors
			if current.LastStartedAt.After(total.LastStartedAt) {
				total.LastStartedAt = current.LastStartedAt
			}
			if current.LastFinishedAt.After(total.LastFinishedAt) {
				total.LastFinishedAt = current.LastFinishedAt
			}
			if current.LastDuration > total.LastDuration {
				total.LastDuration = current.LastDuration
			}
			if current.LastError != "" {
				total.LastError = current.LastError
			}
		}
		return total
	}
	phaseStatsByName := make(map[string]mainLoopPhaseStats, len(phaseNames))
	for _, phaseName := range phaseNames {
		phaseStatsByName[phaseName] = phaseStats(phaseName)
	}
	pollStats := phaseStatsByName["poll"]
	historyWatchStats := phaseStatsByName["history-watch"]
	linkedTranscriptStats := phaseStatsByName["linked-transcript"]
	t.Logf("real-data Teams experiment: resume=%t startup_load=%s listener_wall=%s measured_windows=%d measured_window=%s startup_ready_after=%s first_execution_after=%s baseline_inbound=%d final_inbound=%d inbound_delta=%d final_completed=%d completed_delta=%d failed=%d queued=%d running=%d interrupted=%d executor_runs=%d measured_inbound=%d measured_completed=%d measured_executor_runs=%d wall_inbound_per_sec=%.3f measured_inbound_per_sec=%.3f measured_completed_per_sec=%.3f measured_executor_per_sec=%.3f graph_list_gets=%d graph_item_gets=%d graph_posts_blocked_locally=%d graph_429=%d graph_503=%d unknown_posts=%d repeated_unknown_posts=%d graph_served_unique=%d graph_served_duplicates=%d history_offset_delta=%d poll_runs=%d poll_deadlines=%d poll_errors=%d poll_last_duration=%s poll_last_error=%q history_watch_runs=%d history_watch_deadlines=%d history_watch_errors=%d history_watch_last_duration=%s history_watch_last_error=%q linked_transcript_runs=%d linked_transcript_deadlines=%d linked_transcript_errors=%d linked_transcript_last_duration=%s linked_transcript_last_error=%q phase_stats=%v startup_phase_errors=%v startup_phase_deadlines=%v owner_generations=%v owner_changes=%d owner_changes_by_run=%v backlog_samples=%d ordinary_history_while_backlog=%d ordinary_linked_while_backlog=%d ordinary_unindexed_linked=%d unindexed_linked_candidates=%d ordinary_history_paths=%v ordinary_linked_sessions=%v ordinary_unindexed_linked_ids=%v baseline_history_offset_sum=%d final_history_offset_sum=%d graph_list_requests=%v unknown_graph_paths=%v listener_error=%v", resume, loadElapsed, runElapsed, measuredWindows, steadyElapsed, startupReadyElapsed, firstExecutionElapsed, beforeCounts.inbound, afterCounts.inbound, inboundDelta, afterCounts.completed, completedDelta, afterCounts.failed, afterCounts.queued, afterCounts.running, afterCounts.interrupted, runs, measuredInbound, measuredCompleted, measuredExecutorRuns, float64(inboundDelta)/runElapsed.Seconds(), float64(measuredInbound)/steadyElapsed.Seconds(), float64(measuredCompleted)/steadyElapsed.Seconds(), float64(measuredExecutorRuns)/steadyElapsed.Seconds(), graphServerState.listGETs.Load(), graphServerState.itemGETs.Load(), graphServerState.posts.Load(), graphServerState.status429.Load(), graphServerState.status503.Load(), graphServerState.unknownPosts.Load(), graphServerState.unknownPostRepeats.Load(), len(servedMessages), duplicateServedMessages, afterHistoryOffsets-beforeHistoryOffsets, pollStats.Runs, pollStats.DeadlineExceeded, pollStats.Errors, pollStats.LastDuration, pollStats.LastError, historyWatchStats.Runs, historyWatchStats.DeadlineExceeded, historyWatchStats.Errors, historyWatchStats.LastDuration, historyWatchStats.LastError, linkedTranscriptStats.Runs, linkedTranscriptStats.DeadlineExceeded, linkedTranscriptStats.Errors, linkedTranscriptStats.LastDuration, linkedTranscriptStats.LastError, phaseStatsByName, startupPhaseErrors, startupPhaseDeadlines, ownerGenerations, ownerChanges, ownerChangesByRun, backlogSamples, ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog, ordinaryUnindexedLinked, len(linkedUnindexed), ordinaryHistoryPaths, ordinaryLinkedSessions, ordinaryUnindexedLinkedIDs, beforeHistoryOffsets, afterHistoryOffsets, graphServerState.listRequests(), unknown, lastListenerErr)
	t.Logf("real-data per-chat poll timings: %v", traceWriter.pollChatsSnapshot())

	minimumCompleted := int64(replayCount)
	if minimumCompleted > dockerRealDataMinimumReplay {
		minimumCompleted = dockerRealDataMinimumReplay
	}
	if (!resume && completedDelta <= 0) || (resume && resumeWorkExpected && completedDelta <= 0) || (!resume && inboundDelta <= 0) || (!resume && runs <= 0) {
		t.Fatalf("real copied Teams workload did not make durable execution progress: before=%#v after=%#v executor_runs=%d listener_err=%v", beforeCounts, afterCounts, runs, lastListenerErr)
	}
	if len(measuredWindowInbound) != measuredWindows || len(measuredWindowCompletedDeltas) != measuredWindows {
		t.Fatalf("real-data measured window accounting is incomplete: windows=%d inbound_deltas=%v completed_deltas=%v", measuredWindows, measuredWindowInbound, measuredWindowCompletedDeltas)
	}
	for index := range measuredWindowInbound {
		if ((!resume || resumeWorkExpected) && measuredWindowCompletedDeltas[index] <= 0) || (!resume && measuredWindowInbound[index] <= 0) {
			t.Fatalf("real-data measured window %d made no durable progress: inbound_delta=%d completed_delta=%d", index+1, measuredWindowInbound[index], measuredWindowCompletedDeltas[index])
		}
	}
	if resume {
		if resumeWorkExpected {
			minimumCompleted = 1
		} else {
			minimumCompleted = 0
		}
	}
	if mode == dockerRealDataModeThroughput && completedDelta < minimumCompleted {
		t.Fatalf("real-data process %s completed only %d synthetic turns; want at least %d new completions to make the measured rate meaningful (corpus=%d duration=%s windows=%d)", map[bool]string{true: "resume", false: "initial"}[resume], completedDelta, minimumCompleted, replayCount, duration, measuredWindows)
	}
	if afterCounts.inbound > int64(replayCount) {
		t.Fatalf("synthetic inbound count=%d exceeded replay corpus=%d", afterCounts.inbound, replayCount)
	}
	if mode == dockerRealDataModeComplete {
		if len(persistedSyntheticIDs) != replayCount || afterCounts.inbound != int64(replayCount) || afterCounts.completed != int64(replayCount) {
			t.Fatalf("complete real-data acceptance did not close the entire replay corpus: corpus=%d persisted=%d inbound=%d completed=%d", replayCount, len(persistedSyntheticIDs), afterCounts.inbound, afterCounts.completed)
		}
		for _, messages := range replayCorpus {
			for _, message := range messages {
				if _, served := servedMessages[message.ID]; !served {
					t.Fatalf("complete real-data acceptance never served replay message %q from fake Graph", message.ID)
				}
			}
		}
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
	if runs != completedDelta {
		t.Fatalf("executor invocation count=%d does not equal this process's durable completion delta=%d (before=%d after=%d); possible owner/fence loss or duplicate admission", runs, completedDelta, beforeCounts.completed, afterCounts.completed)
	}
	unresolvedCorrelations, err := dockerRealDataDurableCorrelation(postCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName), mode == dockerRealDataModeComplete)
	if err != nil {
		t.Fatalf("read synthetic inbound/turn correlation: %v", err)
	}
	if unresolvedCorrelations != 0 {
		t.Fatalf("synthetic durable inbound/turn correlation has %d unresolved or duplicate message(s)", unresolvedCorrelations)
	}
	for run, changes := range ownerChangesByRun {
		if changes != 0 {
			t.Fatalf("listener owner generation changed within isolated run %d: changes=%d all_generations=%v", run+1, changes, ownerGenerations)
		}
	}
	if len(unknown) != 0 {
		t.Fatalf("fake Graph observed unexpected routes in the production listener: %v", unknown)
	}
	if graphServerState.unsupportedFilters.Load() != 0 || graphServerState.invalidListQueries.Load() != 0 {
		t.Fatalf("strict fake Graph observed invalid production list queries: unsupported_filters=%d invalid_queries=%d requests=%v", graphServerState.unsupportedFilters.Load(), graphServerState.invalidListQueries.Load(), graphServerState.listRequests())
	}
	requireGraphWork := !resume || resumeWorkExpected
	observedPaginatedChats := 0
	largeReplayChats := 0
	unobservedLargeReplayChats := 0
	for chatID, corpus := range replayCorpus {
		if len(corpus) <= dockerRealDataPageSize {
			continue
		}
		largeReplayChats++
		pages := graphServerState.listPageCount(chatID)
		if pages == 0 {
			// The copied production schedule is intentionally not rewritten into a
			// synthetic one-chat workload. A 90-second real listener window cannot
			// promise that every one of hundreds of due chats gets a turn, so an
			// unselected large chat is diagnostic rather than a pagination failure.
			unobservedLargeReplayChats++
			continue
		}
		// A production poll is deliberately bounded: the first head page may
		// persist a continuation and leave its remaining pages for a later
		// durable action. Therefore one observed page is valid even when this
		// chat has more than one Graph page. The aggregate assertion below still
		// requires at least one selected real chat to cross a page boundary.
		if pages >= 2 {
			observedPaginatedChats++
		}
	}
	t.Logf("real-data pagination coverage: observed_paginated_chats=%d large_replay_chats=%d unobserved_large_replay_chats=%d", observedPaginatedChats, largeReplayChats, unobservedLargeReplayChats)
	if requireGraphWork && observedPaginatedChats == 0 {
		t.Fatalf("real-data listener observed no multi-page chat; large_replay_chats=%d unobserved_large_replay_chats=%d", largeReplayChats, unobservedLargeReplayChats)
	}
	if requireGraphWork && (graphServerState.status429.Load() == 0 || graphServerState.status503.Load() == 0) {
		t.Fatalf("real-data replay did not exercise both retryable Graph failures: 429=%d 503=%d", graphServerState.status429.Load(), graphServerState.status503.Load())
	}
	if requireGraphWork && graphServerState.status503.Load() < int64(defaultGraphRetries+1) {
		t.Fatalf("real-data replay did not exhaust the in-process 503 retry budget: 503=%d retry_budget=%d", graphServerState.status503.Load(), defaultGraphRetries)
	}
	if requireGraphWork && expiredProviderTokens > 0 && graphServerState.expiredContinuationCount() == 0 {
		t.Fatalf("real-data replay copied %d opaque provider continuations but never exercised an expired continuation response", expiredProviderTokens)
	}
	faultChatID := graphServerState.faultChatSnapshot()
	if requireGraphWork && (faultChatID == "" || graphServerState.listPageCount(faultChatID) == 0) {
		t.Fatalf("retryable Graph fault never recovered to a successful page: fault_chat=%q successful_pages=%d", faultChatID, graphServerState.listPageCount(faultChatID))
	}
	if requireGraphWork {
		if faultPoll, found := afterState.ChatPolls[faultChatID]; !found {
			t.Fatalf("retryable Graph fault chat has no durable poll state after recovery: chat=%q", faultChatID)
		} else if strings.Contains(faultPoll.LastError, "429") || strings.Contains(faultPoll.LastError, "503") {
			t.Fatalf("retryable Graph fault remained the terminal durable poll error after a later successful page: chat=%q poll=%#v", faultChatID, faultPoll)
		}
	}
	ambiguousUnknownRows := dockerRealDataAmbiguousExecutionOutboxes(afterState)
	if resume {
		if graphServerState.unknownPosts.Load() != 0 || graphServerState.unknownPostRepeats.Load() != 0 {
			t.Fatalf("resumed process replayed an unknown Graph POST result: unknown=%d repeats=%d", graphServerState.unknownPosts.Load(), graphServerState.unknownPostRepeats.Load())
		}
		if len(ambiguousUnknownRows) != 1 {
			t.Fatalf("resumed process did not preserve exactly one durable ambiguous outbox without retrying it: rows=%#v", ambiguousUnknownRows)
		}
	} else if unknownFaultEnabled {
		if graphServerState.unknownPosts.Load() != 1 || graphServerState.unknownPostRepeats.Load() != 0 || graphServerState.unknownPostAcceptedAttempts() != 1 {
			t.Fatalf("unknown Graph POST result was not handled as one remotely accepted, non-replayed attempt: unknown=%d repeats=%d remote_accepts=%d", graphServerState.unknownPosts.Load(), graphServerState.unknownPostRepeats.Load(), graphServerState.unknownPostAcceptedAttempts())
		}
		unknownPostChat := graphServerState.unknownPostChatSnapshot()
		if unknownPostChat == "" || len(ambiguousUnknownRows) != 1 || strings.TrimSpace(ambiguousUnknownRows[0].TeamsChatID) != unknownPostChat {
			t.Fatalf("unknown Graph POST was not represented by one durable ambiguous outbox row: chat=%q rows=%#v", unknownPostChat, ambiguousUnknownRows)
		}
	} else if graphServerState.unknownPosts.Load() != 0 || len(ambiguousUnknownRows) != 0 {
		t.Fatalf("complete real-data mode unexpectedly left an ambiguous Graph POST: unknown=%d rows=%#v", graphServerState.unknownPosts.Load(), ambiguousUnknownRows)
	}
	replayBacklogObserved := maintenanceTrace.syntheticBacklogObserved()
	if !replayBacklogObserved {
		t.Fatalf("real-data replay never observed its synthetic Teams backlog while optional maintenance was running; backlog_samples=%d ordinary_history=%d ordinary_linked=%d", backlogSamples, ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog)
	}
	// Fairness is intentionally allowed during a long-lived backlog, but it
	// must be measured by durable history progress. A callback can return
	// without advancing its checkpoint, so callback counts alone must not make
	// this experiment pass.
	if replayBacklogObserved && afterHistoryOffsets <= beforeHistoryOffsets {
		t.Fatalf("optional history maintenance made no durable progress while Teams backlog remained: history_callbacks=%d linked_callbacks=%d offsets_before=%d offsets_after=%d", ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog, beforeHistoryOffsets, afterHistoryOffsets)
	}
	if replayBacklogObserved && len(linkedUnindexed) == 0 {
		t.Logf("real-data replay has no unindexed active linked session candidate; fairness is not observable in this snapshot: ordinary_sessions=%v", ordinaryLinkedSessions)
	}
	if replayBacklogObserved && len(linkedUnindexed) > 0 {
		missingUnindexed := make([]string, 0)
		for sessionID := range linkedUnindexed {
			if ordinaryUnindexedLinkedIDs[sessionID] == 0 {
				missingUnindexed = append(missingUnindexed, sessionID)
			}
		}
		sort.Strings(missingUnindexed)
		if len(missingUnindexed) > 0 {
			t.Fatalf("backlog fairness did not reach every unindexed active linked session: missing=%v candidates=%d ordinary_unindexed_linked=%d observed=%v", missingUnindexed, len(linkedUnindexed), ordinaryUnindexedLinked, ordinaryUnindexedLinkedIDs)
		}
	}
	latestGracefulStop := time.Time{}
	for _, stoppedAt := range gracefulStopAt {
		if stoppedAt.After(latestGracefulStop) {
			latestGracefulStop = stoppedAt
		}
	}
	for phaseName, stats := range phaseStatsByName {
		if startupPhaseErrors[phaseName] != 0 {
			t.Fatalf("%s phase produced %d error(s) during startup/first cycle before the measured window: stats=%#v", phaseName, startupPhaseErrors[phaseName], stats)
		}
		if startupPhaseDeadlines[phaseName] != 0 {
			t.Fatalf("%s phase exceeded its budget %d time(s) during startup/first cycle: stats=%#v", phaseName, startupPhaseDeadlines[phaseName], stats)
		}
		beforeStop := phaseErrorsAtStop[phaseName]
		atMeasureStart := phaseErrorsAtMeasureStart[phaseName]
		if beforeStop > atMeasureStart {
			t.Fatalf("%s phase produced an error during the measured listener window: start_errors=%d stop_errors=%d stats=%#v", phaseName, atMeasureStart, beforeStop, stats)
		}
		if stats.Errors < beforeStop {
			t.Fatalf("%s phase error counter regressed: stop_errors=%d final_errors=%d stats=%#v", phaseName, beforeStop, stats.Errors, stats)
		}
		postStop := stats.Errors - beforeStop
		if postStop == 0 {
			continue
		}
		teardownCancellation := !latestGracefulStop.IsZero() && stats.LastFinishedAt.After(latestGracefulStop) &&
			(strings.Contains(stats.LastError, "context canceled") || strings.Contains(stats.LastError, "context deadline exceeded"))
		if !teardownCancellation {
			t.Fatalf("%s phase produced a non-teardown error after the measured window: stop_errors=%d final_errors=%d stats=%#v graceful_stop=%s", phaseName, beforeStop, stats.Errors, stats, latestGracefulStop.Format(time.RFC3339Nano))
		}
		t.Logf("%s phase cancellation observed only during intentional Docker teardown: post_stop_errors=%d last_error=%q", phaseName, postStop, stats.LastError)
	}
	for phaseName, deadlines := range phaseDeadlinesAtStop {
		startDeadlines := phaseDeadlinesAtMeasureStart[phaseName]
		if deadlines < startDeadlines {
			t.Fatalf("%s phase deadline counter regressed: start_deadlines=%d stop_deadlines=%d", phaseName, startDeadlines, deadlines)
		}
		if deadlines > startDeadlines {
			t.Fatalf("%s phase exceeded its configured budget during measured real-data work: start_deadlines=%d stop_deadlines=%d stats=%#v", phaseName, startDeadlines, deadlines, phaseStatsByName[phaseName])
		}
	}
	if replayBacklogObserved {
		// A real replay run is intentionally long enough to leave a durable Teams
		// backlog. Optional history work must not consume the same phase budget in
		// that state; a short no-op phase is acceptable, but a full scan/deadline
		// is not.
		const optionalMaintenanceBudget = time.Second
		if historyWatchStats.LastDuration > optionalMaintenanceBudget || linkedTranscriptStats.LastDuration > optionalMaintenanceBudget {
			t.Fatalf("optional maintenance remained expensive while the synthetic Teams backlog was observed: queued_after=%d history=%#v linked=%#v", afterCounts.queued, historyWatchStats, linkedTranscriptStats)
		}
		if afterHistoryOffsets < beforeHistoryOffsets {
			t.Fatalf("history cursor regressed while the synthetic Teams backlog was observed: before=%d after=%d queued_after=%d", beforeHistoryOffsets, afterHistoryOffsets, afterCounts.queued)
		}
	}
}
