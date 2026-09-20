package teams

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	dockerRealDataExperimentEnv        = "CXP_TEAMS_DOCKER_REAL_DATA_EXPERIMENT"
	dockerRealDataModeEnv              = "CXP_TEAMS_DOCKER_REAL_DATA_MODE"
	dockerRealDataChatCoverageEnv      = "CXP_TEAMS_DOCKER_REAL_DATA_CHAT_COVERAGE"
	dockerRealDataRequireAllLaggingEnv = "CXP_TEAMS_DOCKER_REAL_DATA_REQUIRE_ALL_LAGGING"
	dockerRealDataDurationEnv          = "CXP_TEAMS_DOCKER_REAL_DATA_DURATION"
	dockerRealDataResumeEnv            = "CXP_TEAMS_DOCKER_REAL_DATA_RESUME"
	dockerRealDataProcessRestartEnv    = "CXP_TEAMS_DOCKER_PROCESS_RESTART"
	dockerRealData429ExperimentEnv     = "CXP_TEAMS_DOCKER_REAL_DATA_429"
	dockerRealData429ScopeEnv          = "CXP_TEAMS_DOCKER_REAL_DATA_429_SCOPE"
	dockerRealData429PollOnlyEnv       = "CXP_TEAMS_DOCKER_REAL_DATA_429_POLL_ONLY"
	dockerRealDataPollIntervalEnv      = "CXP_TEAMS_DOCKER_REAL_DATA_POLL_INTERVAL"
	dockerRealDataStartupDeadlineEnv   = "CXP_TEAMS_DOCKER_STARTUP_DEADLINE"
	dockerRealDataPageSize             = 20
	dockerRealDataMessageIDPrefix      = "docker-real-data:"
	dockerRealDataDefaultDuration      = 5 * time.Minute
	dockerRealDataMinimumDuration      = time.Minute
	dockerRealData429DefaultDuration   = 12 * time.Second
	dockerRealData429MinimumDuration   = 2 * time.Second
	dockerRealData429DefaultInterval   = 100 * time.Millisecond
	dockerRealData429MinimumInterval   = 10 * time.Millisecond
	dockerRealData429Failures          = 4
	dockerRealData429Chats             = 1
	dockerRealData429HealthyChats      = 1
	dockerRealData429ScopeChat         = "chat"
	dockerRealData429ScopeAccount      = "account"
	dockerRealData429ScopeGlobal       = "global"
	dockerRealDataDefaultTop           = ownerPollMessageTop
	dockerRealDataMinimumReplay        = 100
	dockerRealDataModeThroughput       = "throughput"
	dockerRealDataModeComplete         = "complete"
	dockerRealDataExecutionPrefix      = "docker real-data execution result #"
	dockerRealDataAnyExecutionMarker   = "__docker_real_data_any_execution_result__"
	dockerRealDataGraphOpMessageList   = "message-list-get"
	dockerRealDataGraphOpMessageItem   = "message-item-get"
	dockerRealDataGraphOpMembers       = "members-get"
	dockerRealDataGraphOpMe            = "me-get"
	dockerRealDataGraphOpMessagePost   = "message-post"
	dockerRealDataGraphOpMarkUnread    = "mark-unread-post"
	dockerRealDataGraphOpMeetingPost   = "meeting-post"
	dockerRealDataGraphOpPatch         = "chat-patch"
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
	now       func() time.Time
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
		if e.now != nil {
			e.first = e.now()
		} else {
			e.first = time.Now()
		}
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

// dockerRealDataLaggingChatManifest is the inventory boundary for the
// exhaustive acceptance run.  A queued Teams inbound row is already a
// durable promise that this chat needs work; whether it has an active CXP
// session is a separate recovery condition and must not silently shrink the
// proof set.
type dockerRealDataLaggingChatManifest struct {
	AllChatIDs              []string
	ActiveChatIDs           []string
	OrphanChatIDs           []string
	ActionableOrphanChatIDs []string
	TerminalQueuedChatIDs   []string
}

func dockerRealDataQueuedWorkChatManifest(state teamstore.State, controlChatID string) dockerRealDataLaggingChatManifest {
	controlChatID = strings.TrimSpace(controlChatID)
	activeChats := make(map[string]struct{}, len(state.Sessions))
	for _, session := range state.Sessions {
		chatID := strings.TrimSpace(session.TeamsChatID)
		if chatID == "" || chatID == controlChatID || !isActiveSessionStatus(string(session.Status)) {
			continue
		}
		activeChats[chatID] = struct{}{}
	}
	all := make(map[string]struct{})
	terminalQueued := make(map[string]struct{})
	actionableQueued := make(map[string]struct{})
	for _, inbound := range state.InboundEvents {
		if !strings.EqualFold(strings.TrimSpace(inbound.Source), "teams") || inbound.Status != teamstore.InboundStatusQueued {
			continue
		}
		chatID := strings.TrimSpace(inbound.TeamsChatID)
		if chatID == "" || chatID == controlChatID {
			continue
		}
		// A production snapshot can retain queued inbound provenance after its
		// turn has completed.  That terminal row must be reported separately,
		// but it must not erase the chat from the exhaustive proof set: the same
		// chat may also have ordinary queued rows that the replay corpus must
		// cover.
		all[chatID] = struct{}{}
		if turn, found := state.Turns[strings.TrimSpace(inbound.TurnID)]; found && dockerRealDataTerminalTurn(turn.Status) {
			// This is stale queued provenance, not executable backlog: its
			// durable turn is already terminal. Keep it visible in the audit,
			// but do not count it as a new executable turn.
			terminalQueued[chatID] = struct{}{}
		} else {
			actionableQueued[chatID] = struct{}{}
		}
	}
	manifest := dockerRealDataLaggingChatManifest{
		AllChatIDs:              make([]string, 0, len(all)),
		ActiveChatIDs:           make([]string, 0, len(all)),
		OrphanChatIDs:           make([]string, 0),
		ActionableOrphanChatIDs: make([]string, 0),
		TerminalQueuedChatIDs:   make([]string, 0, len(terminalQueued)),
	}
	for chatID := range all {
		manifest.AllChatIDs = append(manifest.AllChatIDs, chatID)
		if _, ok := activeChats[chatID]; ok {
			manifest.ActiveChatIDs = append(manifest.ActiveChatIDs, chatID)
		} else {
			manifest.OrphanChatIDs = append(manifest.OrphanChatIDs, chatID)
			if _, ok := actionableQueued[chatID]; ok {
				manifest.ActionableOrphanChatIDs = append(manifest.ActionableOrphanChatIDs, chatID)
			}
		}
	}
	for chatID := range terminalQueued {
		manifest.TerminalQueuedChatIDs = append(manifest.TerminalQueuedChatIDs, chatID)
	}
	sort.Strings(manifest.AllChatIDs)
	sort.Strings(manifest.ActiveChatIDs)
	sort.Strings(manifest.OrphanChatIDs)
	sort.Strings(manifest.ActionableOrphanChatIDs)
	sort.Strings(manifest.TerminalQueuedChatIDs)
	return manifest
}

func TestDockerRealDataQueuedWorkChatManifest(t *testing.T) {
	state := teamstore.State{
		Sessions: map[string]teamstore.SessionContext{
			"active":          {ID: "active", Status: teamstore.SessionStatusActive, TeamsChatID: "active-chat"},
			"active-terminal": {ID: "active-terminal", Status: teamstore.SessionStatusActive, TeamsChatID: "active-terminal-chat"},
			"closed":          {ID: "closed", Status: teamstore.SessionStatusClosed, TeamsChatID: "closed-chat"},
		},
		InboundEvents: map[string]teamstore.InboundEvent{
			"active-inbound":   {ID: "active-inbound", Source: "teams", Status: teamstore.InboundStatusQueued, TeamsChatID: "active-chat"},
			"active-terminal":  {ID: "active-terminal", Source: "teams", Status: teamstore.InboundStatusQueued, TeamsChatID: "active-terminal-chat", TurnID: "terminal-turn"},
			"orphan-inbound":   {ID: "orphan-inbound", Source: "teams", Status: teamstore.InboundStatusQueued, TeamsChatID: "orphan-chat"},
			"closed-inbound":   {ID: "closed-inbound", Source: "teams", Status: teamstore.InboundStatusQueued, TeamsChatID: "closed-chat"},
			"control-inbound":  {ID: "control-inbound", Source: "teams", Status: teamstore.InboundStatusQueued, TeamsChatID: "control-chat"},
			"terminal-inbound": {ID: "terminal-inbound", Source: "teams", Status: teamstore.InboundStatusQueued, TeamsChatID: "terminal-chat", TurnID: "terminal-turn"},
			"other-source":     {ID: "other-source", Source: "codex", Status: teamstore.InboundStatusQueued, TeamsChatID: "other-chat"},
		},
		Turns: map[string]teamstore.Turn{
			"terminal-turn": {ID: "terminal-turn", Status: teamstore.TurnStatusCompleted},
		},
	}
	got := dockerRealDataQueuedWorkChatManifest(state, "control-chat")
	wantAll := []string{"active-chat", "active-terminal-chat", "closed-chat", "orphan-chat", "terminal-chat"}
	wantActive := []string{"active-chat", "active-terminal-chat"}
	wantOrphans := []string{"closed-chat", "orphan-chat", "terminal-chat"}
	wantTerminal := []string{"active-terminal-chat", "terminal-chat"}
	if !reflect.DeepEqual(got.AllChatIDs, wantAll) || !reflect.DeepEqual(got.ActiveChatIDs, wantActive) || !reflect.DeepEqual(got.OrphanChatIDs, wantOrphans) || !reflect.DeepEqual(got.ActionableOrphanChatIDs, wantOrphans[:2]) || !reflect.DeepEqual(got.TerminalQueuedChatIDs, wantTerminal) {
		t.Fatalf("queued work chat manifest = %#v, want all=%v active=%v orphans=%v actionable_orphans=%v terminal=%v", got, wantAll, wantActive, wantOrphans, wantOrphans[:2], wantTerminal)
	}
}

// dockerRealDataChatOnlyTerminalEmptyQueuedRows identifies a queued chat that
// has no replayable message at all because every queued row is an empty
// terminal provenance row.  Such a row must not be replayed or treated as a
// missing ordinary-message proof, but it remains visible in the source audit.
func dockerRealDataChatOnlyTerminalEmptyQueuedRows(state teamstore.State, chatID string) bool {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return false
	}
	found := false
	for _, inbound := range state.InboundEvents {
		if !strings.EqualFold(strings.TrimSpace(inbound.Source), "teams") || inbound.Status != teamstore.InboundStatusQueued || strings.TrimSpace(inbound.TeamsChatID) != chatID {
			continue
		}
		found = true
		if strings.TrimSpace(inbound.Text) != "" || strings.TrimSpace(inbound.TeamsBodyHTML) != "" || len(inbound.TeamsAttachments) != 0 {
			return false
		}
		turn, ok := state.Turns[strings.TrimSpace(inbound.TurnID)]
		if !ok || !dockerRealDataTerminalTurn(turn.Status) {
			return false
		}
	}
	return found
}

func TestDockerRealDataChatOnlyTerminalEmptyQueuedRows(t *testing.T) {
	state := teamstore.State{
		InboundEvents: map[string]teamstore.InboundEvent{
			"terminal-empty":   {ID: "terminal-empty", Source: "teams", Status: teamstore.InboundStatusQueued, TeamsChatID: "terminal-empty-chat", TurnID: "terminal-empty-turn"},
			"terminal-message": {ID: "terminal-message", Source: "teams", Status: teamstore.InboundStatusQueued, TeamsChatID: "terminal-message-chat", TurnID: "terminal-message-turn", Text: "message"},
			"actionable-empty": {ID: "actionable-empty", Source: "teams", Status: teamstore.InboundStatusQueued, TeamsChatID: "actionable-empty-chat", TurnID: "actionable-empty-turn"},
		},
		Turns: map[string]teamstore.Turn{
			"terminal-empty-turn":   {ID: "terminal-empty-turn", Status: teamstore.TurnStatusCompleted},
			"terminal-message-turn": {ID: "terminal-message-turn", Status: teamstore.TurnStatusCompleted},
			"actionable-empty-turn": {ID: "actionable-empty-turn", Status: teamstore.TurnStatusQueued},
		},
	}
	if !dockerRealDataChatOnlyTerminalEmptyQueuedRows(state, "terminal-empty-chat") {
		t.Fatal("terminal empty provenance chat was not recognized")
	}
	if dockerRealDataChatOnlyTerminalEmptyQueuedRows(state, "terminal-message-chat") || dockerRealDataChatOnlyTerminalEmptyQueuedRows(state, "actionable-empty-chat") || dockerRealDataChatOnlyTerminalEmptyQueuedRows(state, "missing-chat") {
		t.Fatal("non-empty, actionable, or missing chat was misclassified as terminal empty provenance")
	}
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

// dockerRealDataInheritedOperationalRowsWithoutOutbox keeps the data audit
// useful without materializing the copied production outbox. The source
// fixture can contain more than a gigabyte of outbox JSON; the throughput
// experiment only needs the inherited operational-row count, not every body.
func dockerRealDataInheritedOperationalRowsWithoutOutbox(state teamstore.State) (inboundRows, turnRows int) {
	isSynthetic := func(messageID string) bool {
		return strings.HasPrefix(strings.TrimSpace(messageID), dockerRealDataMessageIDPrefix)
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
	return inboundRows, turnRows
}

// dockerRealDataInheritedOperationalOutboxRowsCount observes only indexed
// outbox columns plus the small synthetic markers. It deliberately does not
// decode a historical outbox body; the copied rows remain untouched and the
// production listener still sees the complete table.
func dockerRealDataInheritedOperationalOutboxRowsCount(ctx context.Context, path string) (int, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return 0, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var count int
	err = db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM outbox_messages
		WHERE status IN (?, ?, ?)
		  AND COALESCE(trim(turn_id), '') NOT LIKE 'docker-real-data-turn-%'
		  AND COALESCE(CAST(json AS TEXT), '') NOT LIKE '%' || ? || '%'`,
		string(teamstore.OutboxStatusQueued), string(teamstore.OutboxStatusSending), string(teamstore.OutboxStatusAccepted), dockerRealDataExecutionPrefix).Scan(&count)
	return count, err
}

// dockerRealDataOutboxMessageByIDReadOnly is the only outbox materialization
// needed by a resumed run: the prior process writes one durable ambiguous-POST
// witness ID. A point lookup keeps the resume audit independent of the size of
// the inherited outbox table.
func dockerRealDataOutboxMessageByIDReadOnly(ctx context.Context, path string, outboxID string) (teamstore.OutboxMessage, bool, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return teamstore.OutboxMessage{}, false, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var rowID string
	var raw []byte
	err = db.QueryRowContext(ctx, `SELECT id, json FROM outbox_messages WHERE id = ?`, strings.TrimSpace(outboxID)).Scan(&rowID, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return teamstore.OutboxMessage{}, false, nil
	}
	if err != nil {
		return teamstore.OutboxMessage{}, false, err
	}
	var message teamstore.OutboxMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return teamstore.OutboxMessage{}, false, fmt.Errorf("decode outbox witness %q: %w", rowID, err)
	}
	if strings.TrimSpace(message.ID) == "" || strings.TrimSpace(message.ID) != strings.TrimSpace(rowID) {
		return teamstore.OutboxMessage{}, false, fmt.Errorf("outbox witness identity mismatch: sql=%q json=%q", rowID, message.ID)
	}
	return message, true, nil
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
	rows := make([]teamstore.OutboxMessage, 0)
	for _, outbox := range state.OutboxMessages {
		if !dockerRealDataExecutionResultBody(outbox.Body) || !teamstore.OutboxSendIsAmbiguous(outbox) {
			continue
		}
		rows = append(rows, outbox)
	}
	return rows
}

func dockerRealDataAmbiguousOutboxesForChat(state teamstore.State, chatID string) []teamstore.OutboxMessage {
	chatID = strings.TrimSpace(chatID)
	rows := make([]teamstore.OutboxMessage, 0)
	for _, outbox := range state.OutboxMessages {
		if strings.TrimSpace(outbox.TeamsChatID) != chatID || !teamstore.OutboxSendIsAmbiguous(outbox) {
			continue
		}
		rows = append(rows, outbox)
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.TrimSpace(rows[i].ID) < strings.TrimSpace(rows[j].ID)
	})
	return rows
}

type dockerRealDataUnknownPostWitness struct {
	OutboxID    string `json:"outbox_id"`
	ChatID      string `json:"chat_id"`
	BodyHash    string `json:"body_hash"`
	PayloadHash string `json:"payload_hash"`
}

func dockerRealDataUnknownPostWitnessPath(statePath string) string {
	return filepath.Join(filepath.Dir(statePath), "docker-real-data-unknown-post-witness.json")
}

func dockerRealDataOutboxBodyHash(body string) string {
	digest := sha256.Sum256([]byte(body))
	return fmt.Sprintf("%x", digest[:])
}

func dockerRealDataPostPayloadHash(payload []byte) string {
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("%x", digest[:])
}

func dockerRealDataPostPayloadOutboxID(raw []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	contentFrom := func(value any) string {
		body, ok := value.(map[string]any)
		if !ok {
			return ""
		}
		content, _ := body["content"].(string)
		return content
	}
	if id := helperOutboxProvenanceMarkerID(contentFrom(payload["body"])); id != "" {
		return id
	}
	replyMessage, _ := payload["replyMessage"].(map[string]any)
	return helperOutboxProvenanceMarkerID(contentFrom(replyMessage["body"]))
}

func dockerRealDataUnknownPostDispositionSafe(outbox teamstore.OutboxMessage) bool {
	switch outbox.Status {
	case teamstore.OutboxStatusSending:
		return strings.TrimSpace(outbox.TeamsMessageID) == "" && teamstore.OutboxSendIsAmbiguous(outbox)
	case teamstore.OutboxStatusAccepted, teamstore.OutboxStatusSent:
		return strings.TrimSpace(outbox.TeamsMessageID) != ""
	case teamstore.OutboxStatusSkipped:
		// Only low-value control/progress output may be retired after an unknown
		// POST to release a later user-visible row. A final/turn_completed row
		// must never satisfy this oracle: skipping it would hide a lost answer
		// behind a superficially "safe" non-queued status.
		return outboxDeliverySupersedable(outbox) && !isCompletionNotificationOutbox(outbox)
	default:
		return false
	}
}

func writeDockerRealDataUnknownPostWitness(t *testing.T, statePath string, outbox teamstore.OutboxMessage, graph *dockerRealDataGraphServer) {
	t.Helper()
	witness := dockerRealDataUnknownPostWitness{
		OutboxID:    strings.TrimSpace(outbox.ID),
		ChatID:      strings.TrimSpace(outbox.TeamsChatID),
		BodyHash:    dockerRealDataOutboxBodyHash(outbox.Body),
		PayloadHash: graph.unknownPostPayloadHashSnapshot(),
	}
	if witness.PayloadHash == "" {
		t.Fatalf("write Docker real-data unknown POST witness: fake Graph has no accepted payload hash")
	}
	data, err := json.Marshal(witness)
	if err != nil {
		t.Fatalf("encode Docker real-data unknown POST witness: %v", err)
	}
	if err := os.WriteFile(dockerRealDataUnknownPostWitnessPath(statePath), data, 0o600); err != nil {
		t.Fatalf("write Docker real-data unknown POST witness: %v", err)
	}
}

func readDockerRealDataUnknownPostWitness(t *testing.T, statePath string) dockerRealDataUnknownPostWitness {
	t.Helper()
	path := dockerRealDataUnknownPostWitnessPath(statePath)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Docker real-data unknown POST witness %q: %v", path, err)
	}
	var witness dockerRealDataUnknownPostWitness
	if err := json.Unmarshal(data, &witness); err != nil {
		t.Fatalf("decode Docker real-data unknown POST witness %q: %v", path, err)
	}
	if witness.OutboxID == "" || witness.ChatID == "" || witness.BodyHash == "" || witness.PayloadHash == "" {
		t.Fatalf("Docker real-data unknown POST witness is incomplete: %#v", witness)
	}
	return witness
}

func dockerRealDataExecutionResultBody(text string) bool {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, dockerRealDataExecutionPrefix) {
		return false
	}
	suffix := text[len(dockerRealDataExecutionPrefix):]
	digits := 0
	for digits < len(suffix) && suffix[digits] >= '0' && suffix[digits] <= '9' {
		digits++
	}
	return digits > 0 && strings.TrimSpace(suffix[digits:]) == ""
}

func dockerRealDataExecutionOutboxID(outboxID string) bool {
	outboxID = strings.TrimSpace(outboxID)
	if outboxID == "" {
		return false
	}
	// Terminal execution rows are deterministic even when their rendered body
	// is changed by a Teams markdown/mention wrapper.  Keep this fallback
	// limited to the final chunk suffix; ACK, marker, status, and inherited
	// control rows must never consume the unknown-POST witness.
	return strings.HasSuffix(outboxID, ":final") || strings.Contains(outboxID, ":final-")
}

func lastLineAfterTeamsLabel(plain string) string {
	index := strings.LastIndex(plain, dockerRealDataExecutionPrefix)
	if index < 0 {
		return ""
	}
	return strings.TrimSpace(plain[index:])
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

	// The copied store can spend seconds (or, on a large historical fixture,
	// minutes) preparing its first durable cycle.  A two-second lead is not a
	// stable freshness boundary: by the time the listener builds its first
	// Graph filter, the synthetic page may already be behind the copied cursor
	// or the current-time lower bound and the experiment would measure an empty
	// page instead of the real admission path.  Keep the replay timestamps
	// comfortably ahead of startup while retaining their source order.
	base := time.Now().UTC().Add(10 * time.Minute)
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

// dockerRealDataOneMessagePerChat is an opt-in coverage corpus for a copied
// production snapshot. The normal throughput corpus intentionally retains all
// eligible queued messages; this reduced corpus is only for proving that every
// lagging active work chat reaches the real durable Graph -> inbound -> turn
// path within a bounded Docker run. It preserves the first source message,
// including its body, author, chat, and replay identity, and never invents a
// chat that was absent from the real snapshot.
func dockerRealDataOneMessagePerChat(corpus map[string][]ChatMessage) map[string][]ChatMessage {
	coverage := make(map[string][]ChatMessage, len(corpus))
	for chatID, messages := range corpus {
		if len(messages) == 0 {
			continue
		}
		coverage[chatID] = []ChatMessage{messages[0]}
	}
	return coverage
}

func TestDockerRealDataOneMessagePerChat(t *testing.T) {
	corpus := map[string][]ChatMessage{
		"chat-a":     {{ID: "a-1"}, {ID: "a-2"}},
		"chat-b":     {{ID: "b-1"}},
		"chat-empty": nil,
	}
	want := map[string][]ChatMessage{
		"chat-a": {{ID: "a-1"}},
		"chat-b": {{ID: "b-1"}},
	}
	got := dockerRealDataOneMessagePerChat(corpus)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("one-message-per-chat coverage = %#v, want %#v", got, want)
	}
}

func TestDockerRealDataNewPollRecoveryChatsIgnoresInheritedRecovery(t *testing.T) {
	corpus := map[string][]ChatMessage{
		"inherited": {{ID: "inherited-message"}},
		"new":       {{ID: "new-message"}},
		"clean":     {{ID: "clean-message"}},
	}
	before := teamstore.State{
		ChatPolls: map[string]teamstore.ChatPollState{
			"inherited": {ChatID: "inherited", Gap: &teamstore.ChatPollGap{HeadProbePending: true}},
			"new":       {ChatID: "new"},
			"clean":     {ChatID: "clean"},
		},
	}
	after := teamstore.State{
		ChatPolls: map[string]teamstore.ChatPollState{
			"inherited": {ChatID: "inherited", Gap: &teamstore.ChatPollGap{HeadProbePending: true}},
			"new":       {ChatID: "new", RecoveryRequired: true},
			"clean":     {ChatID: "clean"},
		},
	}
	if got, want := dockerRealDataNewPollRecoveryChats(before, after, corpus), []string{"new"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("new recovery chats = %v, want %v", got, want)
	}
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

type dockerRealDataTimingAggregate struct {
	Count  int
	Errors int
	Total  time.Duration
	Max    time.Duration
}

type dockerRealDataTraceWriter struct {
	mu             sync.Mutex
	listeningAt    time.Time
	lines          []string
	events         []string
	leaseClaims    []string
	ownerFailures  []string
	pollChats      []string
	pollSelections []string
	queuedTurns    []string
	pollMessages   []string
	outboxStages   []string
	storeTimings   []string
	timings        map[string]dockerRealDataTimingAggregate
}

// recordTiming retains every observation for the real-data diagnosis. A phase
// can run jobs concurrently, so Total is the sum of observations while Max is
// the contribution of the slowest member to the phase wall time. The report
// emits both instead of incorrectly treating parallel work as serial.
func (w *dockerRealDataTraceWriter) recordTiming(name string, elapsed time.Duration, err error) {
	if w == nil || strings.TrimSpace(name) == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.timings == nil {
		w.timings = make(map[string]dockerRealDataTimingAggregate)
	}
	name = strings.TrimSpace(name)
	aggregate := w.timings[name]
	aggregate.Count++
	aggregate.Total += elapsed
	if elapsed > aggregate.Max {
		aggregate.Max = elapsed
	}
	if err != nil {
		aggregate.Errors++
	}
	w.timings[name] = aggregate
}

func (w *dockerRealDataTraceWriter) timingAggregatesSummary() string {
	return w.timingAggregatesSummaryFor()
}

func (w *dockerRealDataTraceWriter) timingAggregatesSummaryFor(prefixes ...string) string {
	if w == nil {
		return "count=0"
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.timings) == 0 {
		return "count=0"
	}
	keys := make([]string, 0, len(w.timings))
	for key := range w.timings {
		if len(prefixes) > 0 {
			matched := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(key, prefix) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		aggregate := w.timings[key]
		average := time.Duration(0)
		if aggregate.Count > 0 {
			average = aggregate.Total / time.Duration(aggregate.Count)
		}
		parts = append(parts, fmt.Sprintf("%s{n=%d,total=%s,avg=%s,max=%s,errors=%d}", key, aggregate.Count, aggregate.Total, average, aggregate.Max, aggregate.Errors))
	}
	return strings.Join(parts, " ")
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

func (w *dockerRealDataTraceWriter) recordPollSelection(stage, faultChatID string, decisions []inboundPollDecision) {
	if w == nil || strings.TrimSpace(faultChatID) == "" {
		return
	}
	present := false
	targetIndex := -1
	var target inboundPollDecision
	for index, decision := range decisions {
		if strings.TrimSpace(decision.ChatID) == strings.TrimSpace(faultChatID) {
			targetIndex = index
			target = decision
			present = true
			break
		}
	}
	targetSummary := "absent"
	if targetIndex >= 0 {
		targetSummary = fmt.Sprintf("index=%d state=%s due=%t retry=%t operational=%t next=%s last_error=%s last_success=%s last_activity=%s", targetIndex, target.State, target.Due, target.RetryFailure, target.OperationalFrontier, target.NextPollAt.UTC().Format(time.RFC3339Nano), target.LastErrorAt.UTC().Format(time.RFC3339Nano), target.LastSuccessfulPollAt.UTC().Format(time.RFC3339Nano), target.LastActivityAt.UTC().Format(time.RFC3339Nano))
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pollSelections) >= 128 {
		return
	}
	w.pollSelections = append(w.pollSelections, fmt.Sprintf("stage=%s fault_chat=%s target_present=%t decisions=%d target=%s", strings.TrimSpace(stage), strings.TrimSpace(faultChatID), present, len(decisions), targetSummary))
}

func (w *dockerRealDataTraceWriter) pollSelectionsSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.pollSelections...)
}

func (w *dockerRealDataTraceWriter) recordQueuedTurn(stage, sessionID, turnID string, started bool, err error) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queuedTurns) >= 512 {
		return
	}
	diagnostic := ""
	if err != nil {
		diagnostic = trimPollDiagnostic(err.Error())
	}
	w.queuedTurns = append(w.queuedTurns, fmt.Sprintf("at=%s stage=%s session=%s turn=%s started=%t err=%q", time.Now().UTC().Format(time.RFC3339Nano), stage, strings.TrimSpace(sessionID), strings.TrimSpace(turnID), started, diagnostic))
}

func (w *dockerRealDataTraceWriter) queuedTurnsSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.queuedTurns...)
}

func (w *dockerRealDataTraceWriter) recordPollMessage(chatID, messageID, disposition string, err error) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pollMessages) >= 2048 {
		return
	}
	diagnostic := ""
	if err != nil {
		diagnostic = trimPollDiagnostic(err.Error())
	}
	w.pollMessages = append(w.pollMessages, fmt.Sprintf("at=%s chat=%s message=%s disposition=%s err=%q", time.Now().UTC().Format(time.RFC3339Nano), strings.TrimSpace(chatID), strings.TrimSpace(messageID), strings.TrimSpace(disposition), diagnostic))
}

func (w *dockerRealDataTraceWriter) pollMessagesSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.pollMessages...)
}

func (w *dockerRealDataTraceWriter) recordOutboxSendStage(outboxID, stage string, duration time.Duration, err error) {
	if w == nil {
		return
	}
	w.recordTiming("outbox.send."+strings.TrimSpace(stage), duration, err)
	if duration < 10*time.Millisecond && err == nil {
		return
	}
	diagnostic := ""
	if err != nil {
		diagnostic = trimPollDiagnostic(err.Error())
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.outboxStages) >= 1024 {
		return
	}
	w.outboxStages = append(w.outboxStages, fmt.Sprintf("outbox=%s stage=%s duration=%s err=%q", strings.TrimSpace(outboxID), strings.TrimSpace(stage), duration, diagnostic))
}

func (w *dockerRealDataTraceWriter) outboxSendStagesSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.outboxStages...)
}

func (w *dockerRealDataTraceWriter) recordStoreTiming(event teamstore.StoreTimingEvent) {
	if w == nil {
		return
	}
	// Store emits a timing event for every lock/file boundary. Recording every
	// sub-millisecond event would make the diagnostic observer itself contend
	// with the Store mutex and distort the workload. The report is intended to
	// explain material wall-time, so retain >=1ms events and every error; the
	// existing sample remains stricter at 10ms.
	if event.Duration >= time.Millisecond || event.Err != nil {
		w.recordTiming("store."+strings.TrimSpace(event.Operation)+"."+strings.TrimSpace(event.Stage), event.Duration, event.Err)
	}
	if event.Duration < 10*time.Millisecond && event.Err == nil {
		return
	}
	diagnostic := ""
	if event.Err != nil {
		diagnostic = trimPollDiagnostic(event.Err.Error())
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.storeTimings) >= 1024 {
		return
	}
	w.storeTimings = append(w.storeTimings, fmt.Sprintf("operation=%s stage=%s duration=%s err=%q", strings.TrimSpace(event.Operation), strings.TrimSpace(event.Stage), event.Duration, diagnostic))
}

func (w *dockerRealDataTraceWriter) storeTimingsSnapshot() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.storeTimings...)
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

// linesContaining returns only the small, structured diagnostics needed to
// explain a slow phase. Keep this separate from linesSnapshot: the latter is
// intentionally capped and is useful for startup failures, while a realistic
// replay can fill that cap with unrelated listener output before the final
// throughput assertion runs.
func (w *dockerRealDataTraceWriter) linesContaining(needle string) []string {
	if w == nil || strings.TrimSpace(needle) == "" {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, line := range w.lines {
		if strings.Contains(line, needle) {
			out = append(out, line)
		}
	}
	return out
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

func (w *dockerRealDataTraceWriter) recordLeaseClaimWitness(store *teamstore.Store) {
	if w == nil || store == nil {
		return
	}
	owner, found, err := store.ReadOwner(context.Background())
	diagnostic := ""
	if err != nil {
		diagnostic = trimPollDiagnostic(err.Error())
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.leaseClaims) >= 32 {
		return
	}
	w.leaseClaims = append(w.leaseClaims, fmt.Sprintf("witness found=%t machine=%s generation=%d pid=%d instance=%s err=%q", found, strings.TrimSpace(owner.MachineID), owner.LeaseGeneration, owner.PID, strings.TrimSpace(owner.InstanceID), diagnostic))
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
	ordinaryLinkedSuppressed    int
	ordinaryLinkedUnsuppressed  int
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
		if linkedTranscriptNonEssentialSideEffectsSuppressed(ctx) {
			t.ordinaryLinkedSuppressed++
		} else {
			t.ordinaryLinkedUnsuppressed++
		}
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

func (t *dockerRealDataMaintenanceTrace) ordinaryLinkedPolicySnapshot() (suppressed, unsuppressed int) {
	if t == nil {
		return 0, 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ordinaryLinkedSuppressed, t.ordinaryLinkedUnsuppressed
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
	now           func() time.Time
	replay        map[string][]ChatMessage
	knownChats    map[string]struct{}

	mu                        sync.Mutex
	listPaths                 []string
	unknownPath               []string
	postAttempts              map[string]int
	acceptedPostKeys          map[string]int
	messagePostOutboxAttempts map[string]int
	pollServedMessageIDs      map[string]int
	pollServedMessageRequests map[string][]string
	providerTokens            map[string]dockerRealDataProviderContinuation
	expiredTokens             map[string]struct{}
	unknownPostKey            string
	unknownPostChat           string
	unknownPostOutboxID       string
	unknownPostPayloadHash    string
	unknownPostMarker         string
	// durableUnknownPostWitness is loaded from the previous disposable
	// process.  It is a test-side remote-operation fence: if a resumed listener
	// tries to POST the exact already-ambiguous outbox again, the fake Graph
	// rejects it and records the replay instead of accepting a new operation.
	durableUnknownPostWitness bool
	faultChatID               string
	faultSequence             []int
	faultResponses            []int
	faultRequestPaths         []string
	rateLimitListFailures     map[string]int
	rateLimitBindRemaining    int
	rateLimitFailureBudget    int
	rateLimitGlobalRemaining  int
	rateLimitGlobalConfigured bool
	rateLimitGlobalScope      string
	rateLimitOperations       map[string]dockerRealDataRateLimitBudget
	graphRequests             []dockerRealDataGraphRequest
	graphRequestCounts        map[string]int
	graphRequest429s          map[string]int
	graphRequestAccepts       map[string]int
	invalidListDiagnostics    []string

	listGETs              atomic.Int64
	itemGETs              atomic.Int64
	itemGETNotFound       atomic.Int64
	posts                 atomic.Int64
	messagePostAttempts   atomic.Int64
	messagePostResponses  atomic.Int64
	messagePostAccepts    atomic.Int64
	markUnreadPosts       atomic.Int64
	status429             atomic.Int64
	status503             atomic.Int64
	unknownPosts          atomic.Int64
	unknownPostRepeats    atomic.Int64
	unknownPostMismatches atomic.Int64
	opaqueContinuations   atomic.Int64
	expiredContinuations  atomic.Int64
	unknownContinuations  atomic.Int64
	unsupportedFilters    atomic.Int64
	invalidListQueries    atomic.Int64
	pageCounts            map[string]int
	nextLinkCounts        map[string]int
}

type dockerRealDataProviderContinuation struct {
	chatID string
	offset int
	query  url.Values
	poll   bool
}

// dockerRealDataGraphRequest is a bounded, exact remote-side witness for the
// fake Graph boundary. The real-data experiment must be able to prove which
// operation consumed a fault and whether that operation reached the fake
// remote side; aggregate status429 counters alone cannot distinguish a poll
// list read from an outbox POST or a maintenance PATCH.
type dockerRealDataGraphRequest struct {
	Method         string
	Path           string
	Operation      string
	BodyHash       string
	StatusCode     int
	RateLimitScope string
	RemoteAccepted bool
	StartedAt      time.Time
	CompletedAt    time.Time
}

type dockerRealDataRateLimitBudget struct {
	Scope     string
	Remaining int
}

// dockerRealDataResponseWriter captures the final HTTP status without
// weakening the unknown-POST test's connection-hijack behavior.
type dockerRealDataResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *dockerRealDataResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *dockerRealDataResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *dockerRealDataResponseWriter) finalStatus() int {
	if w == nil || !w.wroteHeader {
		return http.StatusOK
	}
	return w.status
}

func (w *dockerRealDataResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("fake Graph response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *dockerRealDataResponseWriter) Flush() {
	flusher, ok := w.ResponseWriter.(http.Flusher)
	if ok {
		flusher.Flush()
	}
}

type dockerRealDataExpiredContinuationError struct {
	token string
}

func (e dockerRealDataExpiredContinuationError) Error() string {
	return fmt.Sprintf("provider continuation expired: %s", e.token)
}

func newDockerRealDataGraphServer(token string, user User, replay map[string][]ChatMessage) *dockerRealDataGraphServer {
	return newDockerRealDataGraphServerAt(token, user, replay, time.Now)
}

// newDockerRealDataGraphServerAt keeps the fake Graph's observable clock
// injectable for bounded acceptance scenarios. Production-style real-data
// experiments retain the wall-clock constructor above; deterministic tests
// can advance a private clock without sleeping or touching the bridge/store
// clock.
func newDockerRealDataGraphServerAt(token string, user User, replay map[string][]ChatMessage, now func() time.Time) *dockerRealDataGraphServer {
	if now == nil {
		now = time.Now
	}
	knownChats := make(map[string]struct{}, len(replay))
	for chatID := range replay {
		knownChats[strings.TrimSpace(chatID)] = struct{}{}
	}
	return &dockerRealDataGraphServer{
		token:                     token,
		user:                      user,
		base:                      now().UTC().Add(2 * time.Second),
		now:                       now,
		replay:                    replay,
		knownChats:                knownChats,
		postAttempts:              make(map[string]int),
		acceptedPostKeys:          make(map[string]int),
		messagePostOutboxAttempts: make(map[string]int),
		pollServedMessageIDs:      make(map[string]int),
		pollServedMessageRequests: make(map[string][]string),
		providerTokens:            make(map[string]dockerRealDataProviderContinuation),
		expiredTokens:             make(map[string]struct{}),
		rateLimitListFailures:     make(map[string]int),
		rateLimitOperations:       make(map[string]dockerRealDataRateLimitBudget),
		graphRequestCounts:        make(map[string]int),
		graphRequest429s:          make(map[string]int),
		graphRequestAccepts:       make(map[string]int),
		pageCounts:                make(map[string]int),
		nextLinkCounts:            make(map[string]int),
	}
}

func (g *dockerRealDataGraphServer) nowTime() time.Time {
	if g != nil && g.now != nil {
		return g.now()
	}
	return time.Now()
}

func (g *dockerRealDataGraphServer) setValidProviderContinuation(token string, chatID string, offset int) {
	g.setValidProviderContinuationWithQuery(token, chatID, offset, nil)
}

func (g *dockerRealDataGraphServer) setValidProviderContinuationWithQuery(token string, chatID string, offset int, query url.Values) {
	if g == nil || strings.TrimSpace(token) == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.providerTokens == nil {
		g.providerTokens = make(map[string]dockerRealDataProviderContinuation)
	}
	cleanQuery := make(url.Values, len(query))
	for key, values := range query {
		if key == "$skiptoken" {
			continue
		}
		cleanQuery[key] = append([]string(nil), values...)
	}
	g.providerTokens[strings.TrimSpace(token)] = dockerRealDataProviderContinuation{
		chatID: strings.TrimSpace(chatID),
		offset: offset,
		query:  cleanQuery,
		poll:   dockerRealDataPollListRequest(cleanQuery),
	}
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

func dockerRealDataProviderTokenVariants(path string) []string {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	variants := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)
	add := func(token string) {
		token = strings.TrimSpace(token)
		if token == "" || strings.HasPrefix(token, "docker-real-data-page:") {
			return
		}
		if _, ok := seen[token]; ok {
			return
		}
		seen[token] = struct{}{}
		variants = append(variants, token)
	}
	// Graph's opaque continuation is normally URL-escaped, but copied durable
	// checkpoints from older helpers can contain a literal '#'.  url.Parse then
	// treats the remainder as a fragment and silently truncates Query().Get;
	// recover the raw query value first so the isolated fixture can mark the
	// exact inherited token expired instead of turning it into a false unknown-
	// token provider error.
	if marker := "$skiptoken="; strings.Index(path, marker) >= 0 {
		start := strings.Index(path, marker) + len(marker)
		raw := path[start:]
		if end := strings.IndexByte(raw, '&'); end >= 0 {
			raw = raw[:end]
		}
		raw = strings.TrimSpace(raw)
		add(raw)
		decoded := raw
		// A nextLink may be persisted once URL-escaped and then escaped again
		// when an older writer embeds it in another query value. Keep each
		// bounded spelling; the fake Graph still requires an explicit
		// registration and never turns arbitrary values into valid continuations.
		for i := 0; i < 3; i++ {
			next, err := url.QueryUnescape(decoded)
			if err != nil || next == decoded {
				break
			}
			decoded = next
			add(decoded)
		}
	}
	parsed, err := url.Parse(path)
	if err != nil {
		return variants
	}
	add(parsed.Query().Get("$skiptoken"))
	return variants
}

func dockerRealDataProviderToken(path string) string {
	variants := dockerRealDataProviderTokenVariants(path)
	if len(variants) == 0 {
		return ""
	}
	// Prefer the decoded query value for callers that need the server-side
	// token. The raw spelling is retained in the variant set for exact fixture
	// re-binding because a copied provider continuation can be encoded once or
	// twice depending on which historical writer produced it.
	for _, token := range variants {
		if !strings.Contains(token, "%") {
			return token
		}
	}
	return variants[0]
}

func dockerRealDataProviderTokenAliases(token string) []string {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}
	aliases := []string{token}
	// Some historical checkpoints store the provider opaque value as a
	// base64-wrapped query value. Keep the encoded value as the primary key and
	// add only the recognizable provider form as an alias; arbitrary unknown
	// base64 strings must still remain unknown/400 in the strict fake Graph.
	if decoded, err := base64.StdEncoding.DecodeString(token); err == nil {
		decodedToken := strings.TrimSpace(string(decoded))
		if strings.HasPrefix(decodedToken, "Source=MessagingFrontEnd##") {
			aliases = append(aliases, decodedToken)
		}
	}
	return aliases
}

func dockerRealDataAllowsGracefulAdmissionBoundary(mode string) bool {
	// A bounded throughput window may stop immediately after durable inbound
	// admission, regardless of whether a second process is requested.  The
	// boundary is safe only for turns created during this run; callers still
	// reject every non-terminal row that existed at the run boundary.
	return mode == dockerRealDataModeThroughput
}

func dockerRealDataExpectedPhaseErrorBudget(phaseName string, unknownFaultEnabled bool, unknownPosts, unknownPostRepeats, unknownPostMismatches int64, resumeAmbiguousWitness bool) uint64 {
	if resumeAmbiguousWitness && phaseName == "outbox" && unknownPosts == 0 && unknownPostRepeats == 0 && unknownPostMismatches == 0 {
		// A resumed disposable process deliberately retains the previous
		// process's ambiguous outbox row.  Its marker-only recovery is allowed to
		// report one durable deferral, but it must never issue another POST.  This
		// budget is scoped to that one known witness and to the outbox phase;
		// all other phase errors remain failures below.
		return 1
	}
	if !unknownFaultEnabled || phaseName != "outbox" || unknownPosts != 1 || unknownPostRepeats != 0 || unknownPostMismatches != 0 {
		return 0
	}
	// The real-data harness intentionally turns exactly one synthetic POST into
	// an EOF/unknown outcome. That error is expected only when the durable row is
	// fenced; every additional outbox error remains a test failure.
	return 1
}

func dockerRealDataIntentionalTeardownPhaseCancellation(stats mainLoopPhaseStats, gracefulStop time.Time) bool {
	return !gracefulStop.IsZero() && stats.LastFinishedAt.After(gracefulStop) &&
		(strings.Contains(stats.LastError, "context canceled") || strings.Contains(stats.LastError, "context deadline exceeded"))
}

func dockerRealDataExpiredProviderTokens(server *dockerRealDataGraphServer, state teamstore.State) int {
	if server == nil {
		return 0
	}
	paths := make([]string, 0, len(state.ChatPolls)*8)
	for _, poll := range state.ChatPolls {
		paths = append(paths, poll.ContinuationPath, poll.DeferredContinuationPath, poll.ContinuationLastPath)
		paths = append(paths, poll.ContinuationPathHistory...)
		if poll.PendingPage != nil {
			paths = append(paths, poll.PendingPage.RequestPath, poll.PendingPage.NextPath)
		}
		if poll.Gap != nil {
			paths = append(paths, poll.Gap.FrontierPath, poll.Gap.RecoveryPath, poll.Gap.HeadProbeContinuationPath, poll.Gap.Evidence)
			if poll.Gap.QuarantinedPage != nil {
				paths = append(paths, poll.Gap.QuarantinedPage.RequestPath, poll.Gap.QuarantinedPage.NextPath)
			}
		}
	}
	// Ambiguous outbox rows have their own durable Graph recovery cursor.  It is
	// not part of ChatPollState, but the listener can legitimately issue this
	// path before any chat-poll continuation.  Register copied provider tokens
	// from both durable lanes so the isolated Graph models an expired inherited
	// continuation instead of manufacturing an unknown-token 400.
	for _, outbox := range state.OutboxMessages {
		paths = append(paths, outbox.GraphRecoveryNextPath)
	}
	seen := make(map[string]struct{})
	for _, path := range paths {
		variants := dockerRealDataProviderTokenVariants(path)
		if len(variants) > 0 {
			// Count one inherited continuation per path-level token, while
			// registering all equivalent URL/base64 spellings in the isolated
			// server. This makes old encoded checkpoints expire without weakening
			// the unknown-token 400 contract.
			canonical := variants[0]
			for _, variant := range variants {
				if !strings.Contains(variant, "%") {
					canonical = variant
					break
				}
			}
			if _, ok := seen[canonical]; !ok {
				seen[canonical] = struct{}{}
			}
			for _, variant := range variants {
				for _, alias := range dockerRealDataProviderTokenAliases(variant) {
					server.setExpiredProviderContinuation(alias)
				}
			}
		}
	}
	return len(seen)
}

// dockerRealDataExpiredProviderTokensFromSQLite covers the outbox recovery
// lane, which is intentionally omitted from PollStateSnapshot so production
// startup/admission does not decode the unbounded outbox JSON. A real-data
// Docker fixture must nevertheless model those copied opaque cursors as
// expired provider state; otherwise the fake Graph turns an expected
// recoverable 410 into an artificial unknown-token 400 and the experiment
// measures a fixture omission instead of the listener's recovery behavior.
// This helper reads only the scalar recovery path and never materializes the
// outbox rows or writes the database.
func dockerRealDataExpiredProviderTokensFromSQLite(ctx context.Context, server *dockerRealDataGraphServer, dbPath string) (int, error) {
	if server == nil || strings.TrimSpace(dbPath) == "" {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(dbPath, query))
	if err != nil {
		return 0, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT json_extract(json, '$.graph_recovery_next_path')
FROM outbox_messages
WHERE json_valid(json)
  AND trim(COALESCE(json_extract(json, '$.graph_recovery_next_path'), '')) <> ''`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	seen := make(map[string]struct{})
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return 0, err
		}
		variants := dockerRealDataProviderTokenVariants(path)
		if len(variants) == 0 {
			continue
		}
		canonical := variants[0]
		for _, variant := range variants {
			if !strings.Contains(variant, "%") {
				canonical = variant
				break
			}
		}
		if canonical != "" {
			seen[canonical] = struct{}{}
		}
		for _, variant := range variants {
			for _, alias := range dockerRealDataProviderTokenAliases(variant) {
				server.setExpiredProviderContinuation(alias)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return len(seen), nil
}

// dockerRealDataExpiredProviderContinuationChats returns copied, active work
// chats whose *next production poll request* contains a provider-issued opaque
// continuation. A poll row can contain opaque paths that are only historical
// evidence (notably Gap.FrontierPath), while pollPageRequestForState selects a
// different safe recovery/head action. The real-data experiment must not move
// an ineligible row to the front and then claim that its recovery was
// exercised; only chats that durable admission can actually select and issue a
// Graph request for belong in this witness set.
func dockerRealDataExpiredProviderContinuationChats(state teamstore.State, controlChatID string) []string {
	controlChatID = strings.TrimSpace(controlChatID)
	eligible := make(map[string]struct{}, len(state.Sessions))
	for _, session := range state.Sessions {
		chatID := strings.TrimSpace(session.TeamsChatID)
		if chatID == "" || chatID == controlChatID || !isActiveSessionStatus(string(session.Status)) {
			continue
		}
		eligible[chatID] = struct{}{}
	}
	queueStateByChat := pollChatTurnQueueStates(state)
	chats := make([]string, 0)
	seenChats := make(map[string]struct{})
	for chatID, poll := range state.ChatPolls {
		chatID = strings.TrimSpace(chatID)
		if chatID == "" {
			continue
		}
		if _, ok := eligible[chatID]; !ok {
			continue
		}
		// A pending page is a local durable receipt. Its request path can contain
		// an opaque token, but production must replay the receipt without Graph.
		// Likewise, an active attempt is owned by another logical poll and a
		// queued/running turn fences the ordinary continuation lane. Do not use
		// either as a short-experiment witness.
		if poll.PendingPage != nil || poll.Attempt != nil {
			continue
		}
		if queue := queueStateByChat[chatID]; queue.Running || queue.Queued > 0 {
			continue
		}
		_, requestPath, _ := pollPageRequestForState(chatID, ownerPollMessageTop, inboundPollRoleWork, poll)
		if dockerRealDataProviderToken(requestPath) == "" {
			continue
		}
		if _, seen := seenChats[chatID]; seen {
			continue
		}
		seenChats[chatID] = struct{}{}
		chats = append(chats, chatID)
	}
	sort.Strings(chats)
	return chats
}

func TestDockerRealDataExpiredProviderContinuationChatsOnlyReturnsExecutableWorkChats(t *testing.T) {
	now := time.Now()
	state := teamstore.State{
		Sessions: map[string]teamstore.SessionContext{
			"control": {ID: "control", Status: teamstore.SessionStatusActive, TeamsChatID: "control-chat"},
			"active":  {ID: "active", Status: teamstore.SessionStatusActive, TeamsChatID: "active-chat"},
			"closed":  {ID: "closed", Status: teamstore.SessionStatusClosed, TeamsChatID: "closed-chat"},
			"empty":   {ID: "empty", Status: teamstore.SessionStatusActive, TeamsChatID: "empty-chat"},
		},
		ChatPolls: map[string]teamstore.ChatPollState{
			"control-chat": {ChatID: "control-chat", ContinuationPath: "/chats/control-chat/messages?$skiptoken=control-token"},
			"active-chat":  {ChatID: "active-chat", ContinuationPath: "/chats/active-chat/messages?$skiptoken=active-token"},
			"closed-chat":  {ChatID: "closed-chat", ContinuationPath: "/chats/closed-chat/messages?$skiptoken=closed-token"},
			"empty-chat":   {ChatID: "empty-chat", ContinuationPath: "/chats/empty-chat/messages?$skiptoken=empty-token"},
			"orphan-chat":  {ChatID: "orphan-chat", ContinuationPath: "/chats/orphan-chat/messages?$skiptoken=orphan-token"},
			// FrontierPath is retained evidence for a gap, not necessarily the
			// next request. With no recovery/head continuation, production builds
			// a bounded timestamp query instead of dereferencing this token.
			"evidence-only-chat": {
				ChatID: "evidence-only-chat",
				Gap: &teamstore.ChatPollGap{
					FrontierPath:     "/chats/evidence-only-chat/messages?$skiptoken=evidence-token",
					HeadProbePending: true,
				},
			},
			"gap-recovery-chat": {
				ChatID: "gap-recovery-chat",
				Gap: &teamstore.ChatPollGap{
					FrontierPath: "/chats/gap-recovery-chat/messages?$skiptoken=old-evidence-token",
					RecoveryPath: "/chats/gap-recovery-chat/messages?$skiptoken=recovery-token",
				},
			},
			"head-continuation-chat": {
				ChatID: "head-continuation-chat",
				Gap: &teamstore.ChatPollGap{
					FrontierPath:              "/chats/head-continuation-chat/messages?$skiptoken=old-evidence-token",
					HeadProbeContinuationPath: "/chats/head-continuation-chat/messages?$skiptoken=head-token",
				},
			},
			"pending-receipt-chat": {
				ChatID:           "pending-receipt-chat",
				ContinuationPath: "/chats/pending-receipt-chat/messages?$skiptoken=continuation-token",
				PendingPage: &teamstore.ChatPollPendingPage{
					RequestPath: "/chats/pending-receipt-chat/messages?$skiptoken=receipt-token",
				},
			},
			"attempt-chat": {
				ChatID:           "attempt-chat",
				ContinuationPath: "/chats/attempt-chat/messages?$skiptoken=attempt-token",
				Attempt:          &teamstore.ChatPollAttempt{StartedAt: now, ExpiresAt: now.Add(time.Minute)},
			},
			"queued-chat": {
				ChatID:           "queued-chat",
				ContinuationPath: "/chats/queued-chat/messages?$skiptoken=queued-token",
			},
			"running-chat": {
				ChatID:           "running-chat",
				ContinuationPath: "/chats/running-chat/messages?$skiptoken=running-token",
			},
		},
	}
	state.Sessions["evidence-only"] = teamstore.SessionContext{ID: "evidence-only", Status: teamstore.SessionStatusActive, TeamsChatID: "evidence-only-chat"}
	state.Sessions["gap-recovery"] = teamstore.SessionContext{ID: "gap-recovery", Status: teamstore.SessionStatusActive, TeamsChatID: "gap-recovery-chat"}
	state.Sessions["head-continuation"] = teamstore.SessionContext{ID: "head-continuation", Status: teamstore.SessionStatusActive, TeamsChatID: "head-continuation-chat"}
	state.Sessions["pending-receipt"] = teamstore.SessionContext{ID: "pending-receipt", Status: teamstore.SessionStatusActive, TeamsChatID: "pending-receipt-chat"}
	state.Sessions["attempt"] = teamstore.SessionContext{ID: "attempt", Status: teamstore.SessionStatusActive, TeamsChatID: "attempt-chat"}
	state.Sessions["queued"] = teamstore.SessionContext{ID: "queued", Status: teamstore.SessionStatusActive, TeamsChatID: "queued-chat"}
	state.Sessions["running"] = teamstore.SessionContext{ID: "running", Status: teamstore.SessionStatusActive, TeamsChatID: "running-chat"}
	state.Turns = map[string]teamstore.Turn{
		"queued-turn":  {ID: "queued-turn", SessionID: "queued", Status: teamstore.TurnStatusQueued},
		"running-turn": {ID: "running-turn", SessionID: "running", Status: teamstore.TurnStatusRunning},
	}
	got := dockerRealDataExpiredProviderContinuationChats(state, "control-chat")
	want := []string{"active-chat", "empty-chat", "gap-recovery-chat", "head-continuation-chat"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("executable expired-continuation chats = %v, want %v", got, want)
	}
}

func TestDockerRealDataProviderTokenPreservesLegacyFragmentCharacters(t *testing.T) {
	path := "/chats/chat/messages?$skiptoken=Source=MessagingFrontEnd##Type=SyncState##ContinuationToken=legacy-token&$top=20"
	if got, want := dockerRealDataProviderToken(path), "Source=MessagingFrontEnd##Type=SyncState##ContinuationToken=legacy-token"; got != want {
		t.Fatalf("dockerRealDataProviderToken(%q)=%q, want %q", path, got, want)
	}
	encoded := "/chats/chat/messages?%24skiptoken=" + url.QueryEscape("Source=MessagingFrontEnd##Type=SyncState##ContinuationToken=encoded")
	if got, want := dockerRealDataProviderToken(encoded), "Source=MessagingFrontEnd##Type=SyncState##ContinuationToken=encoded"; got != want {
		t.Fatalf("dockerRealDataProviderToken(encoded %q)=%q, want %q", encoded, got, want)
	}
}

func TestDockerRealDataProviderTokenAliasesEncodedHistoricalValue(t *testing.T) {
	provider := "Source=MessagingFrontEnd##Type=SyncState##ContinuationToken=encoded-historical-token"
	encoded := base64.StdEncoding.EncodeToString([]byte(provider))
	path := "/chats/chat/messages?$filter=lastModifiedDateTime+gt+2026-01-01T00%3A00%3A00Z&$skiptoken=" + url.QueryEscape(encoded)
	variants := dockerRealDataProviderTokenVariants(path)
	if !reflect.DeepEqual(variants, []string{encoded}) {
		t.Fatalf("encoded historical provider token variants = %v, want decoded token form %q", variants, encoded)
	}
	aliases := dockerRealDataProviderTokenAliases(encoded)
	if !reflect.DeepEqual(aliases, []string{encoded, provider}) {
		t.Fatalf("encoded historical provider token aliases = %v, want encoded and provider forms", aliases)
	}
	server := newDockerRealDataGraphServer("docker-token", User{}, map[string][]ChatMessage{})
	state := teamstore.State{ChatPolls: map[string]teamstore.ChatPollState{
		"chat": {ChatID: "chat", Gap: &teamstore.ChatPollGap{FrontierPath: path}},
	}}
	if got := dockerRealDataExpiredProviderTokens(server, state); got != 1 {
		t.Fatalf("expired encoded historical provider token count = %d, want 1", got)
	}
	values := url.Values{}
	values.Set("$skiptoken", encoded)
	var expiredErr dockerRealDataExpiredContinuationError
	if _, err := server.skipOffset("chat", values, 20); !errors.As(err, &expiredErr) {
		t.Fatalf("encoded historical provider continuation error = %v, want expired continuation", err)
	}
}

func TestDockerRealDataAllowsGracefulAdmissionBoundary(t *testing.T) {
	if !dockerRealDataAllowsGracefulAdmissionBoundary(dockerRealDataModeThroughput) {
		t.Fatal("throughput windows must allow only their newly-created graceful boundary")
	}
	if dockerRealDataAllowsGracefulAdmissionBoundary(dockerRealDataModeComplete) {
		t.Fatal("complete mode must not allow a non-terminal graceful boundary")
	}
}

func TestDockerRealDataExpectedPhaseErrorBudget(t *testing.T) {
	if got := dockerRealDataExpectedPhaseErrorBudget("outbox", true, 1, 0, 0, false); got != 1 {
		t.Fatalf("unknown POST budget = %d, want 1", got)
	}
	if got := dockerRealDataExpectedPhaseErrorBudget("outbox", false, 0, 0, 0, true); got != 1 {
		t.Fatalf("resumed ambiguous witness budget = %d, want 1", got)
	}
	for _, test := range []struct {
		name       string
		phase      string
		enabled    bool
		posts      int64
		repeats    int64
		mismatches int64
		resume     bool
	}{
		{name: "disabled", phase: "outbox", enabled: false, posts: 1},
		{name: "wrong phase", phase: "poll", enabled: true, posts: 1},
		{name: "repeat", phase: "outbox", enabled: true, posts: 1, repeats: 1},
		{name: "mismatch", phase: "outbox", enabled: true, posts: 1, mismatches: 1},
		{name: "multiple posts", phase: "outbox", enabled: true, posts: 2},
		{name: "resume witness wrong phase", phase: "poll", resume: true},
		{name: "resume witness with replay", phase: "outbox", resume: true, posts: 1},
		{name: "resume witness repeated", phase: "outbox", resume: true, repeats: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := dockerRealDataExpectedPhaseErrorBudget(test.phase, test.enabled, test.posts, test.repeats, test.mismatches, test.resume); got != 0 {
				t.Fatalf("unexpected phase error budget = %d, want 0", got)
			}
		})
	}
}

func TestDockerRealDataIntentionalTeardownPhaseCancellation(t *testing.T) {
	stop := time.Unix(100, 0)
	if !dockerRealDataIntentionalTeardownPhaseCancellation(mainLoopPhaseStats{
		LastFinishedAt: stop.Add(time.Second),
		LastError:      "context canceled",
	}, stop) {
		t.Fatal("teardown context cancellation was not recognized")
	}
	if dockerRealDataIntentionalTeardownPhaseCancellation(mainLoopPhaseStats{
		LastFinishedAt: stop.Add(time.Second),
		LastError:      "SQLite busy",
	}, stop) {
		t.Fatal("non-cancellation teardown error was misclassified")
	}
	if dockerRealDataIntentionalTeardownPhaseCancellation(mainLoopPhaseStats{
		LastFinishedAt: stop.Add(-time.Second),
		LastError:      "context canceled",
	}, stop) {
		t.Fatal("workload cancellation before graceful stop was misclassified")
	}
}

func TestDockerRealDataExpiredProviderTokensIncludesOutboxRecoveryPath(t *testing.T) {
	provider := "Source=MessagingFrontEnd##Type=SyncState##ContinuationToken=outbox-historical-token"
	encoded := base64.StdEncoding.EncodeToString([]byte(provider))
	path := "/chats/chat/messages?$top=20&$skiptoken=" + url.QueryEscape(encoded)
	server := newDockerRealDataGraphServer("docker-token", User{}, map[string][]ChatMessage{})
	state := teamstore.State{OutboxMessages: map[string]teamstore.OutboxMessage{
		"outbox:recovery": {ID: "outbox:recovery", GraphRecoveryNextPath: path},
	}}
	if got := dockerRealDataExpiredProviderTokens(server, state); got != 1 {
		t.Fatalf("expired outbox provider token count = %d, want 1", got)
	}
	values := url.Values{}
	values.Set("$skiptoken", encoded)
	var expiredErr dockerRealDataExpiredContinuationError
	if _, err := server.skipOffset("chat", values, 20); !errors.As(err, &expiredErr) {
		t.Fatalf("outbox historical provider token error = %v, want expired continuation", err)
	}
}

func TestDockerRealDataExpiredProviderTokensFromSQLiteReadsScalarRecoveryPath(t *testing.T) {
	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state", "state.json")
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("open outbox recovery fixture store: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.OutboxMessages["outbox:scalar-recovery"] = teamstore.OutboxMessage{
			ID:                    "outbox:scalar-recovery",
			TeamsChatID:           "scalar-recovery-chat",
			GraphRecoveryNextPath: "/chats/scalar-recovery-chat/messages?$top=20&$skiptoken=" + url.QueryEscape("Source=MessagingFrontEnd##ContinuationToken=scalar-recovery"),
			Status:                teamstore.OutboxStatusSending,
		}
		return nil
	}); err != nil {
		_ = store.Close()
		t.Fatalf("seed scalar outbox recovery path: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		_ = store.Close()
		t.Fatalf("migrate scalar outbox recovery fixture: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close scalar outbox recovery fixture: %v", err)
	}

	server := newDockerRealDataGraphServer("docker-token", User{}, nil)
	count, err := dockerRealDataExpiredProviderTokensFromSQLite(ctx, server, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
	if err != nil {
		t.Fatalf("read scalar outbox recovery path: %v", err)
	}
	if count != 1 {
		t.Fatalf("scalar outbox recovery token count = %d, want 1", count)
	}
	values := url.Values{}
	values.Set("$skiptoken", "Source=MessagingFrontEnd##ContinuationToken=scalar-recovery")
	var expiredErr dockerRealDataExpiredContinuationError
	if _, err := server.skipOffset("scalar-recovery-chat", values, 20); !errors.As(err, &expiredErr) {
		t.Fatalf("scalar outbox recovery token = %v, want expired continuation", err)
	}
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

func (g *dockerRealDataGraphServer) effectiveListQuery(values url.Values) (url.Values, bool) {
	if g == nil {
		return values, dockerRealDataPollListRequest(values)
	}
	effective := make(url.Values, len(values))
	for key, rawValues := range values {
		effective[key] = append([]string(nil), rawValues...)
	}
	pollRequest := dockerRealDataPollListRequest(values)
	rawToken := strings.TrimSpace(values.Get("$skiptoken"))
	if rawToken == "" || strings.HasPrefix(rawToken, "docker-real-data-page:") {
		return effective, pollRequest
	}
	g.mu.Lock()
	provider, found := g.providerTokens[rawToken]
	g.mu.Unlock()
	if !found {
		return effective, pollRequest
	}
	// Real Graph nextLink values are opaque: a continuation may contain only
	// $skiptoken and $top, with no copy of the original filter/order. The fake
	// stores the provider-side query semantics out of band and applies them to
	// the request while retaining the actual opaque URL shape for diagnostics.
	for key, rawValues := range provider.query {
		effective[key] = append([]string(nil), rawValues...)
	}
	if top := values.Get("$top"); top != "" {
		effective.Set("$top", top)
	}
	effective.Set("$skiptoken", rawToken)
	return effective, pollRequest || provider.poll
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
	g.faultResponses = nil
	g.faultRequestPaths = nil
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

func (g *dockerRealDataGraphServer) consumeListFault(chatID string, pollRequest bool, requestPath string) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	chatID = strings.TrimSpace(chatID)
	if !pollRequest || chatID == "" || chatID == strings.TrimSpace(g.controlChatID) || len(g.faultSequence) == 0 {
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
	g.faultResponses = append(g.faultResponses, status)
	if len(g.faultRequestPaths) < 16 {
		g.faultRequestPaths = append(g.faultRequestPaths, requestPath)
	}
	return status
}

func (g *dockerRealDataGraphServer) faultStateSnapshot() (string, []int, int) {
	if g == nil {
		return "", nil, 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.faultChatID, append([]int(nil), g.faultResponses...), len(g.faultSequence)
}

func (g *dockerRealDataGraphServer) faultRequestPathsSnapshot() []string {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.faultRequestPaths...)
}

// setPersistentList429 arms a deterministic, chat-local provider throttle for
// the real-data Docker experiment. An empty chat list binds the fault to the
// first selected replay chats at the Graph poll boundary, which avoids a
// false-green run where the injected chat never reaches the production
// scheduler. Each bound chat fails a finite number of requests and then
// recovers automatically; no test-side schedule or block clearing is allowed.
func (g *dockerRealDataGraphServer) setPersistentList429(chatIDs []string, chats, failures int) {
	if g == nil || chats <= 0 || failures <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rateLimitListFailures = make(map[string]int)
	g.rateLimitBindRemaining = 0
	g.rateLimitFailureBudget = failures
	g.rateLimitGlobalRemaining = 0
	g.rateLimitGlobalConfigured = false
	g.rateLimitGlobalScope = ""
	for _, chatID := range chatIDs {
		chatID = strings.TrimSpace(chatID)
		if chatID == "" || chatID == strings.TrimSpace(g.controlChatID) {
			continue
		}
		g.rateLimitListFailures[chatID] = failures
	}
	if len(g.rateLimitListFailures) == 0 {
		g.rateLimitBindRemaining = chats
	}
}

// setPersistentGlobalList429 arms an account-level message-read throttle. The
// counter is global to the fake Graph server rather than keyed by chat, so a
// request for any known chat consumes the same provider budget. The caller
// supplies the exact number of requests that should be throttled; keeping the
// window finite makes the Docker experiment fast while still exercising a
// tenant-wide outage followed by automatic recovery.
func (g *dockerRealDataGraphServer) setPersistentGlobalList429(requests int) {
	g.setPersistentGlobalList429WithScope(requests, dockerRealData429ScopeAccount)
}

func (g *dockerRealDataGraphServer) setPersistentGlobalList429WithScope(requests int, scope string) {
	if g == nil || requests <= 0 {
		return
	}
	scope = strings.ToLower(strings.TrimSpace(scope))
	if scope != dockerRealData429ScopeAccount && scope != dockerRealData429ScopeGlobal {
		scope = dockerRealData429ScopeAccount
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rateLimitListFailures = make(map[string]int)
	g.rateLimitBindRemaining = 0
	g.rateLimitFailureBudget = 0
	g.rateLimitGlobalRemaining = requests
	g.rateLimitGlobalConfigured = true
	g.rateLimitGlobalScope = scope
}

func (g *dockerRealDataGraphServer) consumePersistentGlobalList429() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.rateLimitGlobalRemaining <= 0 {
		return false
	}
	g.rateLimitGlobalRemaining--
	return true
}

func (g *dockerRealDataGraphServer) persistentGlobalList429Configured() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rateLimitGlobalConfigured
}

// setPersistentOperation429 arms a finite fault for one exact Graph
// operation. It intentionally does not reuse the legacy list-only counters:
// a Docker acceptance run must be able to distinguish a tenant-wide list
// throttle from a write or item-read throttle. The scope is recorded in the
// response and request witness, while the operation key is matched before any
// fake remote side effect is performed.
func (g *dockerRealDataGraphServer) setPersistentOperation429(operation string, scope string, requests int) {
	if g == nil || strings.TrimSpace(operation) == "" || requests <= 0 {
		return
	}
	scope = strings.ToLower(strings.TrimSpace(scope))
	if scope != dockerRealData429ScopeChat && scope != dockerRealData429ScopeAccount && scope != dockerRealData429ScopeGlobal {
		scope = dockerRealData429ScopeAccount
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.rateLimitOperations == nil {
		g.rateLimitOperations = make(map[string]dockerRealDataRateLimitBudget)
	}
	g.rateLimitOperations[strings.TrimSpace(operation)] = dockerRealDataRateLimitBudget{Scope: scope, Remaining: requests}
}

func (g *dockerRealDataGraphServer) consumePersistentOperation429(operation string) (string, bool) {
	if g == nil {
		return "", false
	}
	operation = strings.TrimSpace(operation)
	if operation == "" {
		return "", false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	budget, ok := g.rateLimitOperations[operation]
	if !ok || budget.Remaining <= 0 {
		return "", false
	}
	budget.Remaining--
	g.rateLimitOperations[operation] = budget
	return budget.Scope, true
}

func (g *dockerRealDataGraphServer) graphRequestsSnapshot() []dockerRealDataGraphRequest {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]dockerRealDataGraphRequest(nil), g.graphRequests...)
}

func (g *dockerRealDataGraphServer) graphOperationCounts(operation string) (attempts, throttled, accepted int) {
	if g == nil {
		return 0, 0, 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	operation = strings.TrimSpace(operation)
	return g.graphRequestCounts[operation], g.graphRequest429s[operation], g.graphRequestAccepts[operation]
}

func dockerRealDataGraphOperation(req *http.Request) string {
	if req == nil || req.URL == nil {
		return "unknown"
	}
	path := req.URL.Path
	if req.Method == http.MethodGet {
		if _, list := dockerFixtureChatIDFromMessagesPath(path); list {
			return dockerRealDataGraphOpMessageList
		}
		if _, _, ok := dockerRealDataChatItemPath(path); ok {
			return dockerRealDataGraphOpMessageItem
		}
		if path == "/me" {
			return dockerRealDataGraphOpMe
		}
		if strings.HasPrefix(path, "/chats/") && strings.HasSuffix(path, "/members") {
			return dockerRealDataGraphOpMembers
		}
	}
	if req.Method == http.MethodPost {
		if strings.HasPrefix(path, "/chats/") && strings.HasSuffix(path, "/markChatUnreadForUser") {
			return dockerRealDataGraphOpMarkUnread
		}
		if _, _, ok := dockerRealDataPostChatPath(path); ok {
			return dockerRealDataGraphOpMessagePost
		}
		if path == "/me/onlineMeetings" {
			return dockerRealDataGraphOpMeetingPost
		}
	}
	if req.Method == http.MethodPatch && strings.HasPrefix(path, "/chats/") {
		return dockerRealDataGraphOpPatch
	}
	return "unknown"
}

func (g *dockerRealDataGraphServer) recordGraphRequest(req *http.Request, operation string, bodyHash string, status int, scope string, accepted bool, startedAt time.Time) {
	if g == nil {
		return
	}
	if req == nil || req.URL == nil {
		return
	}
	record := dockerRealDataGraphRequest{
		Method:         req.Method,
		Path:           req.URL.RequestURI(),
		Operation:      strings.TrimSpace(operation),
		BodyHash:       strings.TrimSpace(bodyHash),
		StatusCode:     status,
		RateLimitScope: strings.TrimSpace(scope),
		RemoteAccepted: accepted,
		StartedAt:      startedAt,
		CompletedAt:    g.nowTime(),
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.graphRequestCounts == nil {
		g.graphRequestCounts = make(map[string]int)
	}
	if g.graphRequest429s == nil {
		g.graphRequest429s = make(map[string]int)
	}
	if g.graphRequestAccepts == nil {
		g.graphRequestAccepts = make(map[string]int)
	}
	g.graphRequestCounts[record.Operation]++
	if status == http.StatusTooManyRequests {
		g.graphRequest429s[record.Operation]++
	}
	if accepted {
		g.graphRequestAccepts[record.Operation]++
	}
	// Keep enough detail for a real-data diagnostic while bounding memory on a
	// long fixture run. Aggregate counters remain exact after this sample fills.
	if len(g.graphRequests) < 4096 {
		g.graphRequests = append(g.graphRequests, record)
	}
}

func (g *dockerRealDataGraphServer) consumePersistentList429(chatID string, pollRequest bool) bool {
	if g == nil || !pollRequest {
		return false
	}
	chatID = strings.TrimSpace(chatID)
	if chatID == "" || chatID == strings.TrimSpace(g.controlChatID) {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if remaining, ok := g.rateLimitListFailures[chatID]; ok {
		if remaining <= 0 {
			return false
		}
		g.rateLimitListFailures[chatID] = remaining - 1
		return true
	}
	if g.rateLimitBindRemaining <= 0 || len(g.replay[chatID]) == 0 || g.rateLimitFailureBudget <= 0 {
		return false
	}
	g.rateLimitBindRemaining--
	g.rateLimitListFailures[chatID] = g.rateLimitFailureBudget - 1
	return true
}

func (g *dockerRealDataGraphServer) persistentRateLimitChats() []string {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	chats := make([]string, 0, len(g.rateLimitListFailures))
	for chatID := range g.rateLimitListFailures {
		chats = append(chats, chatID)
	}
	sort.Strings(chats)
	return chats
}

func (g *dockerRealDataGraphServer) setUnknownPostMarker(marker string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.unknownPostMarker = strings.TrimSpace(marker)
	g.mu.Unlock()
}

func (g *dockerRealDataGraphServer) setDurableUnknownPostWitness(witness dockerRealDataUnknownPostWitness) {
	if g == nil {
		return
	}
	witness.OutboxID = strings.TrimSpace(witness.OutboxID)
	witness.ChatID = strings.TrimSpace(witness.ChatID)
	witness.BodyHash = strings.TrimSpace(witness.BodyHash)
	witness.PayloadHash = strings.TrimSpace(witness.PayloadHash)
	if witness.OutboxID == "" || witness.ChatID == "" || witness.BodyHash == "" || witness.PayloadHash == "" {
		return
	}
	g.mu.Lock()
	g.unknownPostOutboxID = witness.OutboxID
	g.unknownPostChat = witness.ChatID
	g.unknownPostPayloadHash = witness.PayloadHash
	g.durableUnknownPostWitness = true
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
	if payload.ReplyMessage != nil && payload.ReplyMessage.Body != nil && payload.ReplyMessage.Body.Content == marker {
		return true
	}
	// Production final outbox rows are sent as Teams-rendered HTML and include
	// the assistant label before the rendered result. Match the exact final
	// text at a line boundary after HTML decoding; a substring match would let
	// an unrelated metadata field or a result such as "#10" consume the fault.
	isRenderedMarker := func(content string) bool {
		plain := strings.TrimSpace(PlainTextFromTeamsHTML(content))
		if marker == dockerRealDataAnyExecutionMarker {
			// The first executor invocation is not guaranteed to be the first
			// final POST: startup/control work and per-chat FIFO predecessors can
			// legitimately win that race. Match one complete deterministic
			// executor-result line instead of assuming result #1 is observable in
			// this finite real-data window.
			return dockerRealDataExecutionResultBody(lastLineAfterTeamsLabel(plain))
		}
		if plain == marker {
			return true
		}
		if !strings.HasSuffix(plain, marker) {
			return false
		}
		prefix := plain[:len(plain)-len(marker)]
		last, _ := utf8.DecodeLastRuneInString(prefix)
		return unicode.IsSpace(last)
	}
	if marker == dockerRealDataAnyExecutionMarker && dockerRealDataExecutionOutboxID(dockerRealDataPostPayloadOutboxID(raw)) {
		return true
	}
	if payload.Body != nil && isRenderedMarker(payload.Body.Content) {
		return true
	}
	return payload.ReplyMessage != nil && payload.ReplyMessage.Body != nil && isRenderedMarker(payload.ReplyMessage.Body.Content)
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

func (g *dockerRealDataGraphServer) recordInvalidList(req *http.Request, reason string) {
	if g == nil {
		return
	}
	diagnostic := dockerRealDataRequestDiagnostic(req) + ": " + strings.TrimSpace(reason)
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.invalidListDiagnostics) < 32 {
		g.invalidListDiagnostics = append(g.invalidListDiagnostics, diagnostic)
	}
}

func (g *dockerRealDataGraphServer) invalidListDiagnosticsSnapshot() []string {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.invalidListDiagnostics...)
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

// dockerRealDataRequestSummary keeps Docker acceptance output useful when a
// copied production fixture produces many reads. The full bounded request
// sample remains available through listRequests for focused unit assertions;
// the long-running experiment should report counts and a small redacted sample
// instead of spending most of its log budget printing opaque continuations.
func dockerRealDataRequestSummary(requests []string) string {
	if len(requests) == 0 {
		return "count=0"
	}
	routeCounts := make(map[string]int)
	for _, request := range requests {
		route := strings.TrimSpace(request)
		if fields := strings.Fields(route); len(fields) >= 2 {
			route = fields[0] + " " + strings.SplitN(fields[1], "?", 2)[0]
		}
		routeCounts[route]++
	}
	routes := make([]string, 0, len(routeCounts))
	for route, count := range routeCounts {
		routes = append(routes, fmt.Sprintf("%s=%d", route, count))
	}
	sort.Strings(routes)
	sample := append([]string(nil), requests...)
	if len(sample) > 6 {
		sample = append(append([]string(nil), sample[:3]...), sample[len(sample)-3:]...)
	}
	return fmt.Sprintf("count=%d routes=%v sample=%v", len(requests), routes, sample)
}

// dockerRealDataTraceSummary bounds high-cardinality diagnostics without
// dropping the first and last observations that explain a phase transition.
// Failure assertions still carry the precise counters and durable state; this
// helper only prevents a realistic fixture from hiding those counters behind
// thousands of repetitive trace entries.
func dockerRealDataTraceSummary(values []string) string {
	if len(values) == 0 {
		return "count=0"
	}
	sample := append([]string(nil), values...)
	if len(sample) > 8 {
		sample = append(append([]string(nil), sample[:4]...), sample[len(sample)-4:]...)
	}
	return fmt.Sprintf("count=%d sample=%v", len(values), sample)
}

// dockerRealDataPollListRequest identifies the list shape emitted by the
// durable Teams poller. Exact-top maintenance lookups (park notices), outbox
// reconciliation, and delegation inbox probes deliberately do not carry the
// poll's descending timestamp filter; counting those pages as poll delivery
// would make the real-data durability oracle report false missing messages.
// The fake keeps opaque continuation semantics out of band, so a provider
// nextLink that omits the original filter/order is still attributed to the
// poll that created it without classifying maintenance reads as delivery.
func dockerRealDataPollListRequest(values url.Values) bool {
	return strings.TrimSpace(values.Get("$filter")) != "" &&
		values.Get("$orderby") == "lastModifiedDateTime desc"
}

func (g *dockerRealDataGraphServer) recordPollServedMessages(messages []ChatMessage, request string) {
	if g == nil || len(messages) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pollServedMessageIDs == nil {
		g.pollServedMessageIDs = make(map[string]int)
	}
	if g.pollServedMessageRequests == nil {
		g.pollServedMessageRequests = make(map[string][]string)
	}
	for _, message := range messages {
		if id := strings.TrimSpace(message.ID); id != "" {
			g.pollServedMessageIDs[id]++
			if strings.TrimSpace(request) != "" && len(g.pollServedMessageRequests[id]) < 8 {
				g.pollServedMessageRequests[id] = append(g.pollServedMessageRequests[id], request)
			}
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

func (g *dockerRealDataGraphServer) servedMessageRequests() map[string][]string {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string][]string, len(g.pollServedMessageRequests))
	for id, requests := range g.pollServedMessageRequests {
		out[id] = append([]string(nil), requests...)
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

func (g *dockerRealDataGraphServer) recordNextLink(chatID string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.nextLinkCounts[chatID]++
	g.mu.Unlock()
}

func (g *dockerRealDataGraphServer) nextLinkCount(chatID string) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.nextLinkCounts[chatID]
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

func (g *dockerRealDataGraphServer) unknownPostOutboxIDSnapshot() string {
	if g == nil {
		return ""
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.unknownPostOutboxID
}

func (g *dockerRealDataGraphServer) unknownPostPayloadHashSnapshot() string {
	if g == nil {
		return ""
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.unknownPostPayloadHash
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

// duplicateMessagePostOutboxIDs returns durable outbox identities that crossed
// the fake Graph POST boundary more than once. Re-reading a poll page is safe
// when inbound admission is idempotent, but replaying the same durable outbox
// operation would create a duplicate Teams message. Keep this assertion
// separate from the inbound/turn correlation audit so a test cannot pass just
// because the duplicate POST happened before the second inbound was persisted.
func (g *dockerRealDataGraphServer) duplicateMessagePostOutboxIDs() []string {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	duplicates := make([]string, 0)
	for outboxID, count := range g.messagePostOutboxAttempts {
		if strings.TrimSpace(outboxID) != "" && count > 1 {
			duplicates = append(duplicates, outboxID)
		}
	}
	sort.Strings(duplicates)
	return duplicates
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
	startedAt := g.nowTime()
	operation := dockerRealDataGraphOperation(req)
	bodyHash := ""
	if req != nil && req.Body != nil {
		rawBody, readErr := io.ReadAll(req.Body)
		_ = req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(rawBody))
		if readErr == nil {
			bodyHash = dockerRealDataPostPayloadHash(rawBody)
		} else {
			bodyHash = "read-error"
		}
	}
	responseWriter := &dockerRealDataResponseWriter{ResponseWriter: w}
	remoteAccepted := false
	rateLimitScope := ""
	defer func() {
		g.recordGraphRequest(req, operation, bodyHash, responseWriter.finalStatus(), rateLimitScope, remoteAccepted, startedAt)
	}()
	w = responseWriter
	consumeOperation429 := func() bool {
		scope, limited := g.consumePersistentOperation429(operation)
		if !limited {
			return false
		}
		rateLimitScope = scope
		g.status429.Add(1)
		w.Header().Set("Retry-After", "1")
		w.Header().Set("X-CXP-RateLimit-Scope", scope)
		w.WriteHeader(http.StatusTooManyRequests)
		return true
	}
	// The bounded acceptance harness deliberately passes an empty token. Keep
	// the normal deterministic-token contract for the real-data experiment, but
	// accept only GraphClient's exact empty-token header in the tokenless mode;
	// a non-empty credential can never silently cross this fake boundary.
	expectedAuthorization := "Bearer " + g.token
	actualAuthorization := req.Header.Get("Authorization")
	if strings.TrimSpace(g.token) == "" {
		// net/http trims trailing header whitespace, so GraphClient's
		// "Bearer " serialization arrives as exactly "Bearer".
		if strings.TrimSpace(actualAuthorization) != "Bearer" {
			http.Error(w, "deterministic Docker Graph authorization required", http.StatusUnauthorized)
			return
		}
	} else if actualAuthorization != expectedAuthorization {
		http.Error(w, "deterministic Docker Graph authorization required", http.StatusUnauthorized)
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
		if consumeOperation429() {
			return
		}
		// Account-level throttling is applied before the control/work distinction:
		// every message-list read shares the same provider budget. It is still
		// limited to this deterministic fake Graph boundary and never touches the
		// live Teams account.
		if g.consumePersistentGlobalList429() {
			g.status429.Add(1)
			w.Header().Set("Retry-After", "1")
			g.mu.Lock()
			scope := g.rateLimitGlobalScope
			g.mu.Unlock()
			if scope == "" {
				scope = dockerRealData429ScopeAccount
			}
			rateLimitScope = scope
			w.Header().Set("X-CXP-RateLimit-Scope", scope)
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if chatID == strings.TrimSpace(g.controlChatID) {
			writeDockerRealDataJSON(w, struct {
				Value []ChatMessage `json:"value"`
			}{Value: []ChatMessage{}})
			return
		}
		values := req.URL.Query()
		effectiveValues, pollRequest := g.effectiveListQuery(values)
		if status := g.consumeListFault(chatID, pollRequest, dockerRealDataRequestDiagnostic(req)); status != 0 {
			w.Header().Set("Retry-After", "1")
			if status == http.StatusTooManyRequests {
				g.status429.Add(1)
			} else if status == http.StatusServiceUnavailable {
				g.status503.Add(1)
			}
			w.WriteHeader(status)
			return
		}
		if g.consumePersistentList429(chatID, pollRequest) {
			g.status429.Add(1)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if rawToken := strings.TrimSpace(values.Get("$skiptoken")); rawToken != "" && !strings.HasPrefix(rawToken, "docker-real-data-page:") {
			g.opaqueContinuations.Add(1)
		}
		filter, filterErr := dockerRealDataListFilter(effectiveValues)
		if filterErr != nil {
			if strings.Contains(values.Get("$filter"), " ge ") || strings.Contains(values.Get("$filter"), " le ") {
				g.unsupportedFilters.Add(1)
			} else {
				g.invalidListQueries.Add(1)
				g.recordInvalidList(req, filterErr.Error())
			}
			http.Error(w, filterErr.Error(), http.StatusBadRequest)
			return
		}
		if rawTop := strings.TrimSpace(effectiveValues.Get("$top")); rawTop != "" {
			top, topErr := strconv.Atoi(rawTop)
			if topErr != nil || top <= 0 || top > ownerPollMessageTop {
				g.invalidListQueries.Add(1)
				g.recordInvalidList(req, fmt.Sprintf("invalid $top=%q", rawTop))
				http.Error(w, "invalid $top", http.StatusBadRequest)
				return
			}
		}
		if order := strings.TrimSpace(effectiveValues.Get("$orderby")); order != "" && order != "lastModifiedDateTime desc" {
			g.invalidListQueries.Add(1)
			g.recordInvalidList(req, fmt.Sprintf("invalid $orderby=%q", order))
			http.Error(w, "invalid $orderby", http.StatusBadRequest)
			return
		}
		corpus := append([]ChatMessage(nil), g.replay[chatID]...)
		filtered := corpus[:0]
		for _, message := range corpus {
			stamp, stampErr := time.Parse(time.RFC3339Nano, message.LastModifiedDateTime)
			if stampErr != nil {
				g.invalidListQueries.Add(1)
				g.recordInvalidList(req, fmt.Sprintf("invalid replay lastModifiedDateTime: %v", stampErr))
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
			g.recordInvalidList(req, offsetErr.Error())
			http.Error(w, offsetErr.Error(), http.StatusBadRequest)
			return
		}
		if effectiveValues.Get("$orderby") == "lastModifiedDateTime desc" {
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
		if pollRequest {
			g.recordPollServedMessages(messages, dockerRealDataRequestDiagnostic(req))
		}
		g.recordListPage(chatID)
		response := struct {
			Value    []ChatMessage `json:"value"`
			NextLink string        `json:"@odata.nextLink,omitempty"`
		}{Value: messages}
		if end < len(filtered) {
			if pollRequest {
				// A short real-data window may end after the first bounded page.
				// Record the provider nextLink separately so the acceptance check can
				// require its durable continuation even when the next worker quantum
				// has not yet issued the follow-up GET.
				g.recordNextLink(chatID)
			}
			next := url.Values{}
			if top := effectiveValues.Get("$top"); top != "" {
				next.Set("$top", top)
			}
			if pollRequest {
				// Keep the continuation URL opaque, as Graph does. Its server-side
				// query semantics are retained in providerTokens rather than echoed
				// into the request; this also makes the poll classifier exercise the
				// same provenance path used by a copied durable continuation.
				token := fmt.Sprintf("Source=MessagingFrontEnd##DockerContinuation=%s:%d", shortStableID(chatID), end)
				g.setValidProviderContinuationWithQuery(token, chatID, end, effectiveValues)
				next.Set("$skiptoken", token)
			} else {
				if order := effectiveValues.Get("$orderby"); order != "" {
					next.Set("$orderby", order)
				}
				if rawFilter := effectiveValues.Get("$filter"); rawFilter != "" {
					next.Set("$filter", rawFilter)
				}
				next.Set("$skiptoken", "docker-real-data-page:"+shortStableID(chatID)+":"+strconv.Itoa(end))
			}
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
		if consumeOperation429() {
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
			// A targeted lookup for a durable recovery receipt may refer to a
			// message that is no longer present in the copied Graph corpus (for
			// example, a deleted/expired control message). This is a recognized
			// Graph route with an ordinary 404, not an unmodeled request. Keep it
			// visible as a diagnostic without failing the real-data experiment's
			// route-completeness assertion.
			g.itemGETNotFound.Add(1)
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
		if consumeOperation429() {
			return
		}
		g.posts.Add(1)
		g.markUnreadPosts.Add(1)
		remoteAccepted = true
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
			if consumeOperation429() {
				return
			}
			if outboxID := dockerRealDataPostPayloadOutboxID(rawPayload); outboxID != "" {
				g.mu.Lock()
				if g.messagePostOutboxAttempts == nil {
					g.messagePostOutboxAttempts = make(map[string]int)
				}
				g.messagePostOutboxAttempts[outboxID]++
				g.mu.Unlock()
			}
			// On a resumed Docker process the prior fake Graph may already have
			// accepted this exact outbox operation before its response was lost.
			// Keep that witness across the process boundary: a buggy sender that
			// POSTs it again must be observable and must not be rewarded with a
			// second synthetic remote message.
			payloadOutboxID := dockerRealDataPostPayloadOutboxID(rawPayload)
			payloadHash := dockerRealDataPostPayloadHash(rawPayload)
			g.mu.Lock()
			witnessOperation := g.durableUnknownPostWitness &&
				payloadOutboxID != "" && payloadOutboxID == g.unknownPostOutboxID &&
				chatID == g.unknownPostChat
			witnessReplay := witnessOperation && payloadHash == g.unknownPostPayloadHash
			witnessPayloadMismatch := witnessOperation && !witnessReplay
			g.mu.Unlock()
			if witnessReplay || witnessPayloadMismatch {
				g.posts.Add(1)
				g.messagePostAttempts.Add(1)
				if witnessPayloadMismatch {
					g.unknownPostMismatches.Add(1)
					http.Error(w, "durable unknown POST witness payload mismatch", http.StatusConflict)
				} else {
					g.unknownPostRepeats.Add(1)
					http.Error(w, "durable unknown POST witness replay rejected", http.StatusConflict)
				}
				return
			}
			g.posts.Add(1)
			g.messagePostAttempts.Add(1)
			g.mu.Lock()
			g.postAttempts[postKey]++
			attempt := g.postAttempts[postKey]
			// Model the remote side durably accepting the operation before the
			// response is lost. The production outbox must therefore treat the
			// first transport failure as ambiguous and never issue a second POST.
			g.acceptedPostKeys[postKey]++
			g.messagePostAccepts.Add(1)
			marker := strings.TrimSpace(g.unknownPostMarker)
			markerMatches := dockerRealDataPostPayloadMatchesMarker(rawPayload, marker)
			if g.unknownPostKey == "" && markerMatches {
				g.unknownPostKey = postKey
			}
			unknown := postKey == g.unknownPostKey && attempt == 1
			repeatUnknown := postKey == g.unknownPostKey && attempt > 1
			if unknown {
				g.unknownPostChat = chatID
				g.unknownPostOutboxID = dockerRealDataPostPayloadOutboxID(rawPayload)
				g.unknownPostPayloadHash = payloadHash
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
			g.messagePostResponses.Add(1)
			remoteAccepted = true
			message := dockerRealDataMessage(chatID, 0, int(g.posts.Load()), g.base)
			message.ID = fmt.Sprintf("docker-real-data-outbound:%06d", g.posts.Load())
			writeDockerRealDataJSON(w, message)
			return
		}
	}
	if req.Method == http.MethodGet && path == "/me" {
		if consumeOperation429() {
			return
		}
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
		if consumeOperation429() {
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
		if consumeOperation429() {
			return
		}
		remoteAccepted = true
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if req.Method == http.MethodPost && path == "/me/onlineMeetings" {
		if consumeOperation429() {
			return
		}
		remoteAccepted = true
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

type dockerRealDataSyntheticTurnObservation struct {
	ID              string
	InboundID       string
	SessionID       string
	Status          teamstore.TurnStatus
	MachineID       string
	LeaseGeneration int64
	RecoveryReason  string
	CodexThreadID   string
	CodexTurnID     string
	QueuedAt        time.Time
	StartedAt       time.Time
	UpdatedAt       time.Time
}

func dockerRealDataTurnIsNonTerminal(status teamstore.TurnStatus) bool {
	switch status {
	case teamstore.TurnStatusQueued, teamstore.TurnStatusRunning, teamstore.TurnStatusInterrupted:
		return true
	default:
		return false
	}
}

func dockerRealDataTurnNeedsBoundaryRepair(status teamstore.TurnStatus) bool {
	switch status {
	case teamstore.TurnStatusQueued, teamstore.TurnStatusRunning:
		return true
	default:
		// Interrupted is already an explicit durable attention state. It must not
		// be silently retried or converted to a successful completion merely to
		// make the timed Docker boundary look clean.
		return false
	}
}

func dockerRealDataSyntheticTurnSnapshot(ctx context.Context, path string) (map[string]dockerRealDataSyntheticTurnObservation, error) {
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
		SELECT t.id, `+turnInboundID+`, `+turnStatus+`, t.json
		FROM turns t
		JOIN inbound_events i ON `+turnInboundID+` = i.id
		WHERE i.teams_message_id LIKE ?`, dockerRealDataMessageIDPrefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	observations := make(map[string]dockerRealDataSyntheticTurnObservation)
	for rows.Next() {
		var rowID string
		var inboundID sql.NullString
		var status string
		var raw []byte
		if err := rows.Scan(&rowID, &inboundID, &status, &raw); err != nil {
			return nil, err
		}
		var turn teamstore.Turn
		if err := json.Unmarshal(raw, &turn); err != nil {
			return nil, fmt.Errorf("decode synthetic turn snapshot %q: %w", rowID, err)
		}
		if strings.TrimSpace(turn.ID) == "" || strings.TrimSpace(turn.ID) != strings.TrimSpace(rowID) {
			return nil, fmt.Errorf("synthetic turn snapshot identity mismatch: sql=%q json=%q", rowID, turn.ID)
		}
		observations[rowID] = dockerRealDataSyntheticTurnObservation{
			ID:              rowID,
			InboundID:       strings.TrimSpace(inboundID.String),
			SessionID:       strings.TrimSpace(turn.SessionID),
			Status:          teamstore.TurnStatus(strings.TrimSpace(status)),
			MachineID:       strings.TrimSpace(turn.MachineID),
			LeaseGeneration: turn.LeaseGeneration,
			RecoveryReason:  strings.TrimSpace(turn.RecoveryReason),
			CodexThreadID:   strings.TrimSpace(turn.CodexThreadID),
			CodexTurnID:     strings.TrimSpace(turn.CodexTurnID),
			QueuedAt:        turn.QueuedAt,
			StartedAt:       turn.StartedAt,
			UpdatedAt:       turn.UpdatedAt,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return observations, rows.Close()
}

type dockerRealDataMeasuredWindow struct {
	completed                    bool
	startedAt                    time.Time
	endedAt                      time.Time
	before                       dockerRealDataCounts
	after                        dockerRealDataCounts
	beforeTurns                  map[string]dockerRealDataSyntheticTurnObservation
	afterTurns                   map[string]dockerRealDataSyntheticTurnObservation
	completedInWindow            int64
	beforeTurnsReadError         error
	afterTurnsReadError          error
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

// dockerRealDataCompletedTransitionsBetween counts only synthetic turns whose
// durable terminal transition was timestamped inside the measured window. A
// counter snapshot taken just after the timer can otherwise include a turn
// that completed during graceful teardown, which inflates the reported steady
// msg/s. Existing completed rows are excluded even if their JSON was rewritten
// during the window.
func dockerRealDataCompletedTransitionsBetween(before, after map[string]dockerRealDataSyntheticTurnObservation, startedAt, endedAt time.Time) int64 {
	if startedAt.IsZero() || endedAt.IsZero() || !endedAt.After(startedAt) {
		return 0
	}
	var completed int64
	for id, observation := range after {
		if observation.Status != teamstore.TurnStatusCompleted || observation.UpdatedAt.IsZero() ||
			observation.UpdatedAt.Before(startedAt) || observation.UpdatedAt.After(endedAt) {
			continue
		}
		if previous, found := before[id]; found && previous.Status == teamstore.TurnStatusCompleted {
			continue
		}
		completed++
	}
	return completed
}

// dockerRealDataCompletionForGate keeps the throughput result inside the
// declared measurement window.  Graceful teardown may complete already-admitted
// work afterwards, but that drain is a separate diagnostic and must not inflate
// a rate or make a zero-throughput window pass.
func dockerRealDataCompletionForGate(mode string, measured, overall int64) int64 {
	if mode == dockerRealDataModeThroughput {
		return measured
	}
	return overall
}

func dockerRealDataMeasuredWindowDuration(window dockerRealDataMeasuredWindow, fallback time.Duration) time.Duration {
	if !window.startedAt.IsZero() && !window.endedAt.IsZero() && window.endedAt.After(window.startedAt) {
		return window.endedAt.Sub(window.startedAt)
	}
	return fallback
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

// dockerRealDataPollHasRecoveryBoundary identifies a durable recovery domain,
// including a dormant gap whose next action is only a periodic head probe. A
// copied production chat can legitimately move between dormant and actionable
// states during the experiment; that transition is inherited history, not a
// newly created blocker. A completely clean chat acquiring any gap/receipt is
// still a synthetic regression.
func dockerRealDataPollHasRecoveryBoundary(poll teamstore.ChatPollState) bool {
	return poll.RecoveryRequired || poll.Attempt != nil ||
		strings.TrimSpace(poll.ContinuationPath) != "" ||
		strings.TrimSpace(poll.DeferredContinuationPath) != "" ||
		poll.PendingPage != nil || poll.Gap != nil
}

func dockerRealDataPollRecoveryChats(state teamstore.State, corpus map[string][]ChatMessage) []string {
	chatIDs := make([]string, 0)
	for chatID := range corpus {
		chatID = strings.TrimSpace(chatID)
		if chatID == "" {
			continue
		}
		poll, found := state.ChatPolls[chatID]
		if found && dockerRealDataPollHasRecovery(poll) {
			chatIDs = append(chatIDs, chatID)
		}
	}
	sort.Strings(chatIDs)
	return chatIDs
}

// dockerRealDataNewPollRecoveryChats returns only recovery states introduced
// by the disposable experiment. A copied production database may already have
// a bounded continuation/head-probe recovery lane; that inherited state is
// evidence about the source environment, but it is not synthetic work left by
// the representative-message drain. New recovery on a previously clean chat
// remains a hard complete-mode failure.
func dockerRealDataNewPollRecoveryChats(before, after teamstore.State, corpus map[string][]ChatMessage) []string {
	newRecovery := make([]string, 0)
	for chatID := range corpus {
		chatID = strings.TrimSpace(chatID)
		if chatID == "" {
			continue
		}
		afterPoll, foundAfterPoll := after.ChatPolls[chatID]
		if !foundAfterPoll || !dockerRealDataPollHasRecoveryBoundary(afterPoll) {
			continue
		}
		beforePoll, hadBeforePoll := before.ChatPolls[chatID]
		if !hadBeforePoll || !dockerRealDataPollHasRecoveryBoundary(beforePoll) {
			newRecovery = append(newRecovery, chatID)
		}
	}
	sort.Strings(newRecovery)
	return newRecovery
}

// dockerRealDataPostStateFromSQLite reconstructs only the synthetic rows that
// the post-run assertions need. Calling Store.Load here would decode every
// historical inbound/turn/outbox body again after the listener has stopped;
// on a real fixture that can dominate the experiment and look like a hung
// 429 retry. The query is read-only and keeps the same typed JSON validation
// as the assertions, while unrelated historical rows remain untouched.
//
// extraOutboxIDs is an explicit observation set for durable witnesses created
// by the experiment. A low-value ACK/helper row can be the first POST whose
// response is lost, but it has neither a synthetic turn ID nor the synthetic
// execution-result body marker. Omitting that row from this bounded
// post-reader would make the harness report a false data-loss failure even
// though SQLite still contains the durable row.
func dockerRealDataPostStateFromSQLite(ctx context.Context, path string, corpus map[string][]ChatMessage, extraOutboxIDs ...string) (teamstore.State, error) {
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return teamstore.State{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	state := teamstore.State{
		InboundEvents:  make(map[string]teamstore.InboundEvent),
		Turns:          make(map[string]teamstore.Turn),
		OutboxMessages: make(map[string]teamstore.OutboxMessage),
		ChatPolls:      make(map[string]teamstore.ChatPollState),
	}
	turnIDs := make(map[string]struct{})
	inboundRows, err := db.QueryContext(ctx, `SELECT id, json FROM inbound_events WHERE teams_message_id LIKE ?`, dockerRealDataMessageIDPrefix+"%")
	if err != nil {
		return teamstore.State{}, err
	}
	for inboundRows.Next() {
		var rowID string
		var raw []byte
		if err := inboundRows.Scan(&rowID, &raw); err != nil {
			_ = inboundRows.Close()
			return teamstore.State{}, err
		}
		var inbound teamstore.InboundEvent
		if err := json.Unmarshal(raw, &inbound); err != nil {
			_ = inboundRows.Close()
			return teamstore.State{}, fmt.Errorf("decode synthetic inbound %q: %w", rowID, err)
		}
		if strings.TrimSpace(inbound.ID) == "" || strings.TrimSpace(inbound.ID) != strings.TrimSpace(rowID) {
			_ = inboundRows.Close()
			return teamstore.State{}, fmt.Errorf("synthetic inbound identity mismatch: sql=%q json=%q", rowID, inbound.ID)
		}
		state.InboundEvents[inbound.ID] = inbound
		if turnID := strings.TrimSpace(inbound.TurnID); turnID != "" {
			turnIDs[turnID] = struct{}{}
		}
	}
	if err := inboundRows.Err(); err != nil {
		_ = inboundRows.Close()
		return teamstore.State{}, err
	}
	if err := inboundRows.Close(); err != nil {
		return teamstore.State{}, err
	}

	turnIDList := make([]string, 0, len(turnIDs))
	for turnID := range turnIDs {
		turnIDList = append(turnIDList, turnID)
	}
	sort.Strings(turnIDList)
	for start := 0; start < len(turnIDList); start += 500 {
		end := start + 500
		if end > len(turnIDList) {
			end = len(turnIDList)
		}
		placeholders := make([]string, end-start)
		args := make([]any, end-start)
		for index, turnID := range turnIDList[start:end] {
			placeholders[index] = "?"
			args[index] = turnID
		}
		rows, err := db.QueryContext(ctx, `SELECT id, json FROM turns WHERE id IN (`+strings.Join(placeholders, ",")+")", args...)
		if err != nil {
			return teamstore.State{}, err
		}
		for rows.Next() {
			var rowID string
			var raw []byte
			if err := rows.Scan(&rowID, &raw); err != nil {
				_ = rows.Close()
				return teamstore.State{}, err
			}
			var turn teamstore.Turn
			if err := json.Unmarshal(raw, &turn); err != nil {
				_ = rows.Close()
				return teamstore.State{}, fmt.Errorf("decode synthetic turn %q: %w", rowID, err)
			}
			if strings.TrimSpace(turn.ID) == "" || strings.TrimSpace(turn.ID) != strings.TrimSpace(rowID) {
				_ = rows.Close()
				return teamstore.State{}, fmt.Errorf("synthetic turn identity mismatch: sql=%q json=%q", rowID, turn.ID)
			}
			state.Turns[turn.ID] = turn
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return teamstore.State{}, err
		}
		if err := rows.Close(); err != nil {
			return teamstore.State{}, err
		}
	}

	outboxQuery := `SELECT id, json FROM outbox_messages WHERE turn_id LIKE ? OR json LIKE ?`
	outboxArgs := []any{"docker-real-data-turn-%", "%" + dockerRealDataExecutionPrefix + "%"}
	witnessIDs := make([]string, 0, len(extraOutboxIDs))
	seenWitnessIDs := make(map[string]struct{}, len(extraOutboxIDs))
	for _, id := range extraOutboxIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, seen := seenWitnessIDs[id]; seen {
			continue
		}
		seenWitnessIDs[id] = struct{}{}
		witnessIDs = append(witnessIDs, id)
	}
	if len(witnessIDs) > 0 {
		placeholders := make([]string, len(witnessIDs))
		for index, id := range witnessIDs {
			placeholders[index] = "?"
			outboxArgs = append(outboxArgs, id)
		}
		outboxQuery += ` OR id IN (` + strings.Join(placeholders, ",") + `)`
	}
	outboxRows, err := db.QueryContext(ctx, outboxQuery, outboxArgs...)
	if err != nil {
		return teamstore.State{}, err
	}
	for outboxRows.Next() {
		var rowID string
		var raw []byte
		if err := outboxRows.Scan(&rowID, &raw); err != nil {
			_ = outboxRows.Close()
			return teamstore.State{}, err
		}
		var outbox teamstore.OutboxMessage
		if err := json.Unmarshal(raw, &outbox); err != nil {
			_ = outboxRows.Close()
			return teamstore.State{}, fmt.Errorf("decode synthetic outbox %q: %w", rowID, err)
		}
		if strings.TrimSpace(outbox.ID) == "" || strings.TrimSpace(outbox.ID) != strings.TrimSpace(rowID) {
			_ = outboxRows.Close()
			return teamstore.State{}, fmt.Errorf("synthetic outbox identity mismatch: sql=%q json=%q", rowID, outbox.ID)
		}
		include := strings.HasPrefix(strings.TrimSpace(outbox.TurnID), "docker-real-data-turn-") || strings.HasPrefix(strings.TrimSpace(outbox.Body), dockerRealDataExecutionPrefix)
		if _, witness := seenWitnessIDs[strings.TrimSpace(outbox.ID)]; witness {
			include = true
		}
		if include {
			state.OutboxMessages[outbox.ID] = outbox
		}
	}
	if err := outboxRows.Err(); err != nil {
		_ = outboxRows.Close()
		return teamstore.State{}, err
	}
	if err := outboxRows.Close(); err != nil {
		return teamstore.State{}, err
	}

	for chatID := range corpus {
		pollRows, err := db.QueryContext(ctx, `SELECT chat_id, json FROM chat_polls WHERE chat_id = ?`, strings.TrimSpace(chatID))
		if err != nil {
			return teamstore.State{}, err
		}
		for pollRows.Next() {
			var rowChatID string
			var raw []byte
			if err := pollRows.Scan(&rowChatID, &raw); err != nil {
				_ = pollRows.Close()
				return teamstore.State{}, err
			}
			var poll teamstore.ChatPollState
			if err := json.Unmarshal(raw, &poll); err != nil {
				_ = pollRows.Close()
				return teamstore.State{}, fmt.Errorf("decode replay chat poll %q: %w", rowChatID, err)
			}
			if strings.TrimSpace(poll.ChatID) == "" {
				poll.ChatID = rowChatID
			}
			if strings.TrimSpace(poll.ChatID) != strings.TrimSpace(rowChatID) {
				_ = pollRows.Close()
				return teamstore.State{}, fmt.Errorf("replay chat poll identity mismatch: sql=%q json=%q", rowChatID, poll.ChatID)
			}
			state.ChatPolls[poll.ChatID] = poll
		}
		if err := pollRows.Err(); err != nil {
			_ = pollRows.Close()
			return teamstore.State{}, err
		}
		if err := pollRows.Close(); err != nil {
			return teamstore.State{}, err
		}
	}
	return state, nil
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
	residual.PollRecovery = len(dockerRealDataPollRecoveryChats(state, corpus))
	return residual
}

type dockerRealDataCorrelationAudit struct {
	Unresolved          int64
	MissingTurn         int64
	DuplicateTurn       int64
	MissingMessageIDs   []string
	DuplicateMessageIDs []string
}

// dockerRealDataChatDrainAudit closes the gap between a global message count
// and an exhaustive chat proof.  A complete run must account for every
// replayed message under the same TeamsChatID it came from; otherwise one
// healthy chat could compensate for an omitted or misrouted chat elsewhere.
type dockerRealDataChatDrainAudit struct {
	ExpectedChats     int
	ExpectedMessages  int
	ServedMessages    int
	InboundMessages   int
	CompletedMessages int
	Mismatches        []string
}

func auditDockerRealDataChatDrain(state teamstore.State, corpus map[string][]ChatMessage, served map[string]int) dockerRealDataChatDrainAudit {
	audit := dockerRealDataChatDrainAudit{ExpectedChats: len(corpus)}
	expectedChatByMessage := make(map[string]string)
	expectedByChat := make(map[string]int, len(corpus))
	for chatID, messages := range corpus {
		chatID = strings.TrimSpace(chatID)
		for _, message := range messages {
			messageID := strings.TrimSpace(message.ID)
			if messageID == "" {
				continue
			}
			audit.ExpectedMessages++
			expectedByChat[chatID]++
			if prior, exists := expectedChatByMessage[messageID]; exists && prior != chatID {
				audit.Mismatches = append(audit.Mismatches, fmt.Sprintf("message %q appears in chats %q and %q", messageID, prior, chatID))
				continue
			}
			expectedChatByMessage[messageID] = chatID
		}
	}
	servedByChat := make(map[string]int, len(corpus))
	inboundByMessage := make(map[string]int)
	completedByMessage := make(map[string]int)
	inboundByChat := make(map[string]int, len(corpus))
	completedByChat := make(map[string]int, len(corpus))
	for messageID, chatID := range expectedChatByMessage {
		if served[messageID] > 0 {
			audit.ServedMessages++
			servedByChat[chatID]++
		}
	}
	for _, inbound := range state.InboundEvents {
		messageID := strings.TrimSpace(inbound.TeamsMessageID)
		chatID, expected := expectedChatByMessage[messageID]
		if !strings.HasPrefix(messageID, dockerRealDataMessageIDPrefix) || !expected {
			continue
		}
		inboundByMessage[messageID]++
		audit.InboundMessages++
		inboundByChat[chatID]++
		if strings.TrimSpace(inbound.TeamsChatID) != chatID {
			audit.Mismatches = append(audit.Mismatches, fmt.Sprintf("message %q durable chat=%q, expected chat=%q", messageID, strings.TrimSpace(inbound.TeamsChatID), chatID))
		}
		turn, found := state.Turns[strings.TrimSpace(inbound.TurnID)]
		if found && turn.Status == teamstore.TurnStatusCompleted {
			completedByMessage[messageID]++
			completedByChat[chatID]++
			audit.CompletedMessages++
		}
	}
	for messageID, chatID := range expectedChatByMessage {
		if served[messageID] <= 0 || inboundByMessage[messageID] != 1 || completedByMessage[messageID] != 1 {
			audit.Mismatches = append(audit.Mismatches, fmt.Sprintf("message %q chat=%q served=%d inbound=%d completed=%d", messageID, chatID, served[messageID], inboundByMessage[messageID], completedByMessage[messageID]))
		}
	}
	for chatID, expected := range expectedByChat {
		if servedByChat[chatID] != expected || inboundByChat[chatID] != expected || completedByChat[chatID] != expected {
			audit.Mismatches = append(audit.Mismatches, fmt.Sprintf("chat %q expected=%d served=%d inbound=%d completed=%d", chatID, expected, servedByChat[chatID], inboundByChat[chatID], completedByChat[chatID]))
		}
	}
	if len(audit.Mismatches) > 32 {
		audit.Mismatches = audit.Mismatches[:32]
	}
	return audit
}

func TestDockerRealDataChatDrainAudit(t *testing.T) {
	corpus := map[string][]ChatMessage{
		"chat-a": {{ID: "docker-real-data:a-1"}, {ID: "docker-real-data:a-2"}},
		"chat-b": {{ID: "docker-real-data:b-1"}},
	}
	state := teamstore.State{
		InboundEvents: map[string]teamstore.InboundEvent{
			"in-a-1": {ID: "in-a-1", TeamsChatID: "chat-a", TeamsMessageID: "docker-real-data:a-1", TurnID: "turn-a-1"},
			"in-a-2": {ID: "in-a-2", TeamsChatID: "chat-a", TeamsMessageID: "docker-real-data:a-2", TurnID: "turn-a-2"},
			"in-b-1": {ID: "in-b-1", TeamsChatID: "chat-b", TeamsMessageID: "docker-real-data:b-1", TurnID: "turn-b-1"},
		},
		Turns: map[string]teamstore.Turn{
			"turn-a-1": {ID: "turn-a-1", Status: teamstore.TurnStatusCompleted},
			"turn-a-2": {ID: "turn-a-2", Status: teamstore.TurnStatusCompleted},
			"turn-b-1": {ID: "turn-b-1", Status: teamstore.TurnStatusCompleted},
		},
	}
	audit := auditDockerRealDataChatDrain(state, corpus, map[string]int{
		"docker-real-data:a-1": 1,
		"docker-real-data:a-2": 2,
		"docker-real-data:b-1": 1,
	})
	if audit.ExpectedChats != 2 || audit.ExpectedMessages != 3 || audit.ServedMessages != 3 || audit.InboundMessages != 3 || audit.CompletedMessages != 3 || len(audit.Mismatches) != 0 {
		t.Fatalf("chat drain audit = %#v, want all messages/chats closed without mismatch", audit)
	}
}

// dockerRealDataDurableCorrelationAudit verifies the stronger invariant that
// every synthetic inbound event has exactly one durable turn. Complete mode
// also requires that turn to reach completion; throughput mode intentionally
// allows the measured window to stop with a queued turn, while still rejecting
// duplicate admission or an inbound row that never acquired a turn.
func dockerRealDataDurableCorrelationAudit(ctx context.Context, path string, requireCompleted bool) (dockerRealDataCorrelationAudit, error) {
	var audit dockerRealDataCorrelationAudit
	query := url.Values{}
	query.Set("mode", "ro")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return audit, err
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
		return audit, err
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
			return audit, err
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
		return audit, err
	}
	if err := rows.Close(); err != nil {
		return audit, err
	}
	for messageID, entry := range byMessage {
		switch {
		case entry.turns == 0:
			audit.MissingTurn++
			audit.MissingMessageIDs = append(audit.MissingMessageIDs, messageID)
		case entry.turns > 1:
			audit.DuplicateTurn++
			audit.DuplicateMessageIDs = append(audit.DuplicateMessageIDs, messageID)
		}
		if entry.turns != 1 || (requireCompleted && entry.completed != 1) {
			audit.Unresolved++
		}
	}
	sort.Strings(audit.MissingMessageIDs)
	sort.Strings(audit.DuplicateMessageIDs)
	return audit, nil
}

func dockerRealDataDurableCorrelation(ctx context.Context, path string, requireCompleted bool) (int64, error) {
	audit, err := dockerRealDataDurableCorrelationAudit(ctx, path, requireCompleted)
	return audit.Unresolved, err
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
		var turn teamstore.Turn
		decodeErr := json.Unmarshal(raw, &turn)
		digest := sha256.Sum256(raw)
		if decodeErr != nil {
			details = append(details, fmt.Sprintf("id=%s inbound_event_id=%s bytes=%d sha256=%x decode_error=%q", id, inboundID.String, len(raw), digest, trimPollDiagnostic(decodeErr.Error())))
			continue
		}
		// Keep the interrupted-turn diagnostic body-free. These fields identify
		// whether the row was interrupted by a pre-dispatch preparation failure,
		// an ownership fence, or intentional shutdown without copying prompt text.
		details = append(details, fmt.Sprintf("id=%s inbound_event_id=%s session=%s status=%s start_new=%t recovery_reason=%q codex_thread=%s codex_turn=%s bytes=%d sha256=%x", id, inboundID.String, turn.SessionID, turn.Status, turn.StartNewCodexThread, trimPollDiagnostic(turn.RecoveryReason), turn.CodexThreadID, turn.CodexTurnID, len(raw), digest))
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

func dockerRealDataHistoryCheckpointEqual(a, b teamstore.HistoryWatchCheckpoint) bool {
	// UpdatedAt is an audit timestamp and is deliberately excluded from the
	// store's history-watch CAS equality. Match that semantic comparison here:
	// a durable partial-read hint, recovery boundary, or source proof change is
	// progress even when the physical newline cursor cannot advance safely.
	a.UpdatedAt = time.Time{}
	b.UpdatedAt = time.Time{}
	return reflect.DeepEqual(a, b)
}

func dockerRealDataOrdinaryHistoryDurableProgress(before, after teamstore.State, mandatory map[string]bool) (changed, added int, details []string) {
	seen := make(map[string]struct{})
	for id, beforeCheckpoint := range before.HistoryWatch {
		if mandatory[id] {
			continue
		}
		seen[id] = struct{}{}
		afterCheckpoint, found := after.HistoryWatch[id]
		if !found {
			changed++
			details = append(details, fmt.Sprintf("removed=%s", id))
			continue
		}
		if !dockerRealDataHistoryCheckpointEqual(beforeCheckpoint, afterCheckpoint) {
			changed++
			details = append(details, fmt.Sprintf("changed=%s offset=%d->%d size=%d->%d partial=%d/%d->%d/%d", id, beforeCheckpoint.Offset, afterCheckpoint.Offset, beforeCheckpoint.Size, afterCheckpoint.Size, beforeCheckpoint.PartialReadOffset, beforeCheckpoint.PartialObservedSize, afterCheckpoint.PartialReadOffset, afterCheckpoint.PartialObservedSize))
		}
	}
	for id, afterCheckpoint := range after.HistoryWatch {
		if mandatory[id] {
			continue
		}
		if _, wasPresent := seen[id]; wasPresent {
			continue
		}
		if _, wasPresent := before.HistoryWatch[id]; wasPresent {
			continue
		}
		added++
		details = append(details, fmt.Sprintf("added=%s offset=%d size=%d", id, afterCheckpoint.Offset, afterCheckpoint.Size))
	}
	sort.Strings(details)
	return changed, added, details
}

func dockerRealDataBacklogFairCursorChanges(before, after teamstore.ServiceControl) []string {
	changes := make([]string, 0, 4)
	values := []struct {
		lane   string
		before string
		after  string
	}{
		{teamstore.BacklogFairLaneHistoryRecovery, before.BacklogHistoryRecoveryFairCursor, after.BacklogHistoryRecoveryFairCursor},
		{teamstore.BacklogFairLaneHistoryDiscovery, before.BacklogHistoryDiscoveryFairCursor, after.BacklogHistoryDiscoveryFairCursor},
		{teamstore.BacklogFairLaneLinkedDiscovery, before.BacklogLinkedDiscoveryFairCursor, after.BacklogLinkedDiscoveryFairCursor},
		{teamstore.BacklogFairLaneLinked, before.BacklogLinkedFairCursor, after.BacklogLinkedFairCursor},
	}
	for _, value := range values {
		if strings.TrimSpace(value.before) != strings.TrimSpace(value.after) {
			changes = append(changes, fmt.Sprintf("%s=%q->%q", value.lane, value.before, value.after))
		}
	}
	return changes
}

func dockerRealDataDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(dockerRealDataDurationEnv))
	minimum := dockerRealDataMinimumDuration
	defaultDuration := dockerRealDataDefaultDuration
	if os.Getenv(dockerRealData429ExperimentEnv) == "1" {
		minimum = dockerRealData429MinimumDuration
		defaultDuration = dockerRealData429DefaultDuration
	}
	if raw == "" {
		return defaultDuration
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < minimum {
		t.Fatalf("invalid %s=%q; want a duration of at least %s", dockerRealDataDurationEnv, raw, minimum)
	}
	return duration
}

func dockerRealDataStartupDeadline(t *testing.T) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(dockerRealDataStartupDeadlineEnv))
	if raw == "" {
		return 5 * time.Minute
	}
	deadline, err := time.ParseDuration(raw)
	if err != nil || deadline < 30*time.Second {
		t.Fatalf("invalid %s=%q; want a duration of at least 30s", dockerRealDataStartupDeadlineEnv, raw)
	}
	return deadline
}

func dockerRealDataPollInterval(t *testing.T, rateLimitExperiment bool) time.Duration {
	t.Helper()
	if !rateLimitExperiment {
		return 5 * time.Second
	}
	raw := strings.TrimSpace(os.Getenv(dockerRealDataPollIntervalEnv))
	if raw == "" {
		return dockerRealData429DefaultInterval
	}
	interval, err := time.ParseDuration(raw)
	if err != nil || interval < dockerRealData429MinimumInterval {
		t.Fatalf("invalid %s=%q; want an interval of at least %s", dockerRealDataPollIntervalEnv, raw, dockerRealData429MinimumInterval)
	}
	return interval
}

func dockerRealData429Scope(t *testing.T) string {
	t.Helper()
	scope := strings.ToLower(strings.TrimSpace(os.Getenv(dockerRealData429ScopeEnv)))
	if scope == "" {
		return dockerRealData429ScopeChat
	}
	if scope != dockerRealData429ScopeChat && scope != dockerRealData429ScopeAccount && scope != dockerRealData429ScopeGlobal {
		t.Fatalf("invalid %s=%q; want %q, %q, or %q", dockerRealData429ScopeEnv, scope, dockerRealData429ScopeChat, dockerRealData429ScopeAccount, dockerRealData429ScopeGlobal)
	}
	return scope
}

func dockerRealData429ScopeIsAccountWide(scope string) bool {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case dockerRealData429ScopeAccount, dockerRealData429ScopeGlobal:
		return true
	default:
		return false
	}
}

func dockerRealData429PollOnly(t *testing.T) bool {
	t.Helper()
	switch strings.TrimSpace(os.Getenv(dockerRealData429PollOnlyEnv)) {
	case "", "0":
		return false
	case "1":
		return true
	default:
		t.Fatalf("invalid %s=%q; want 0 or 1", dockerRealData429PollOnlyEnv, os.Getenv(dockerRealData429PollOnlyEnv))
		return false
	}
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

func dockerRealDataChatCoverage(t *testing.T) bool {
	t.Helper()
	switch strings.TrimSpace(os.Getenv(dockerRealDataChatCoverageEnv)) {
	case "", "0":
		return false
	case "1":
		return true
	default:
		t.Fatalf("invalid %s=%q; want 0 or 1", dockerRealDataChatCoverageEnv, os.Getenv(dockerRealDataChatCoverageEnv))
		return false
	}
}

func dockerRealDataRequireAllLagging(t *testing.T) bool {
	t.Helper()
	switch strings.TrimSpace(os.Getenv(dockerRealDataRequireAllLaggingEnv)) {
	case "", "0":
		return false
	case "1":
		return true
	default:
		t.Fatalf("invalid %s=%q; want 0 or 1", dockerRealDataRequireAllLaggingEnv, os.Getenv(dockerRealDataRequireAllLaggingEnv))
		return false
	}
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
		lastActivityAt := poll.LastActivityAt
		replayMessages, isReplay := replay[chatID]
		if !isReplay || len(replayMessages) == 0 {
			emptyChats++
		} else {
			replayChats++
			// The replay corpus is copied from durable queued work rather than
			// from a live Graph timestamp. Wake only those chats in the disposable
			// schedule; leaving their historical activity untouched makes the
			// production idle-admission query correctly exclude every old chat
			// before the experiment can observe its replay page.
			lastActivityAt = now
		}
		updates = append(updates, teamstore.ChatPollScheduleUpdate{
			ChatID:         chatID,
			PollState:      pollState,
			NextPollAt:     nextPollAt,
			LastActivityAt: lastActivityAt,
		})
		if isReplay && len(replayMessages) > 0 {
			updates[len(updates)-1].ClearBlockedUntil = true
			updates[len(updates)-1].ResetFailures = true
		}
	}
	if _, err := store.UpdateChatPollSchedules(context.Background(), updates); err != nil {
		t.Fatalf("prepare disposable replay schedules: %v", err)
	}
	return replayChats, emptyChats
}

func TestPrepareDockerRealDataPollSchedulesWakesReplayChats(t *testing.T) {
	store := newBridgeTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load disposable Teams state: %v", err)
	}
	state.Sessions["docker-replay-session"] = teamstore.SessionContext{
		ID:          "docker-replay-session",
		Status:      teamstore.SessionStatusActive,
		TeamsChatID: "docker-replay-chat",
		UpdatedAt:   now.Add(-49 * time.Hour),
	}
	state.Sessions["docker-empty-session"] = teamstore.SessionContext{
		ID:          "docker-empty-session",
		Status:      teamstore.SessionStatusActive,
		TeamsChatID: "docker-empty-chat",
		UpdatedAt:   now.Add(-49 * time.Hour),
	}
	state.ChatPolls["docker-replay-chat"] = teamstore.ChatPollState{
		ChatID:         "docker-replay-chat",
		Seeded:         true,
		PollState:      inboundPollStateCold,
		LastActivityAt: now.Add(-49 * time.Hour),
		BlockedUntil:   now.Add(2 * time.Hour),
		FailureCount:   7,
	}
	state.ChatPolls["docker-empty-chat"] = teamstore.ChatPollState{
		ChatID:         "docker-empty-chat",
		Seeded:         true,
		PollState:      inboundPollStateCold,
		LastActivityAt: now.Add(-49 * time.Hour),
		BlockedUntil:   now.Add(2 * time.Hour),
		FailureCount:   11,
	}
	if err := store.Update(context.Background(), func(current *teamstore.State) error {
		*current = state
		return nil
	}); err != nil {
		t.Fatalf("persist disposable Docker schedule fixture: %v", err)
	}
	replayChats, emptyChats := prepareDockerRealDataPollSchedules(t, store, state, map[string][]ChatMessage{
		"docker-replay-chat": {{ID: "docker-replay-message"}},
	}, "control-chat", now)
	if replayChats != 1 || emptyChats != 1 {
		t.Fatalf("prepared Docker replay schedule counts = replay=%d empty=%d, want 1/1", replayChats, emptyChats)
	}
	replayPoll, found, err := store.ChatPoll(context.Background(), "docker-replay-chat")
	if err != nil || !found {
		t.Fatalf("read replay chat poll found=%v err=%v", found, err)
	}
	if !replayPoll.LastActivityAt.Equal(now) || !replayPoll.NextPollAt.Before(time.Now()) || !replayPoll.BlockedUntil.IsZero() || replayPoll.FailureCount != 0 {
		t.Fatalf("replay chat was not durably woken: %#v", replayPoll)
	}
	emptyPoll, found, err := store.ChatPoll(context.Background(), "docker-empty-chat")
	if err != nil || !found {
		t.Fatalf("read empty chat poll found=%v err=%v", found, err)
	}
	if !emptyPoll.LastActivityAt.Equal(now.Add(-49*time.Hour)) || !emptyPoll.BlockedUntil.Equal(now.Add(2*time.Hour)) || emptyPoll.FailureCount != 11 {
		t.Fatalf("empty chat activity was changed unexpectedly: %#v", emptyPoll)
	}
}

// prioritizeDockerRealDataReplaySchedules changes only the disposable
// experiment's due times. A real-data 429 run has a deliberately short
// measured window, so letting hundreds of empty active chats compete with the
// copied backlog could finish the window without exercising a single replay
// page. Durable frontier fields, leases, and failure state are left intact;
// only the scheduler's ordinary due-time ordering is made representative of
// the workload being measured.
func prioritizeDockerRealDataReplaySchedules(t *testing.T, store *teamstore.Store, state teamstore.State, replay map[string][]ChatMessage, controlChatID string, now time.Time) {
	t.Helper()
	if now.IsZero() {
		now = time.Now()
	}
	controlChatID = strings.TrimSpace(controlChatID)
	seenChats := make(map[string]struct{}, len(state.Sessions))
	updates := make([]teamstore.ChatPollScheduleUpdate, 0, len(state.Sessions))
	for _, session := range state.Sessions {
		chatID := strings.TrimSpace(session.TeamsChatID)
		if chatID == "" || chatID == controlChatID || !isActiveSessionStatus(string(session.Status)) {
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
		nextPollAt := now.Add(24 * time.Hour)
		lastActivityAt := poll.LastActivityAt
		if len(replay[chatID]) > 0 {
			nextPollAt = now.Add(-time.Second)
			// The copied queue is intentionally replayed as fresh Graph input.
			// Marking its chat active in the disposable schedule is the narrow
			// equivalent of the user having sent that backlog now; otherwise the
			// production idle-admission rule correctly parks old chats before the
			// experiment can measure message delivery.
			lastActivityAt = now
		}
		updates = append(updates, teamstore.ChatPollScheduleUpdate{
			ChatID:         chatID,
			PollState:      pollState,
			NextPollAt:     nextPollAt,
			LastActivityAt: lastActivityAt,
		})
	}
	if _, err := store.UpdateChatPollSchedules(context.Background(), updates); err != nil {
		t.Fatalf("prioritize disposable replay schedules: %v", err)
	}
}

// prioritizeDockerRealDataPaginationWitness puts one actual multi-page replay
// chat ahead of the other due chats in the disposable schedule.  The regular
// listener still owns selection, worker count, and continuation persistence;
// this only makes the pagination assertion deterministic in a short measured
// window instead of depending on which of hundreds of equally-due chats wins
// the scheduler tie-break.
func prioritizeDockerRealDataPaginationWitness(t *testing.T, store *teamstore.Store, state teamstore.State, replay map[string][]ChatMessage, controlChatID string, now time.Time) string {
	t.Helper()
	if now.IsZero() {
		now = time.Now()
	}
	controlChatID = strings.TrimSpace(controlChatID)
	seenChats := make(map[string]struct{}, len(state.Sessions))
	queueStateByChat := pollChatTurnQueueStates(state)
	witness := ""
	for _, session := range state.Sessions {
		chatID := strings.TrimSpace(session.TeamsChatID)
		if chatID == "" || chatID == controlChatID || !isActiveSessionStatus(string(session.Status)) {
			continue
		}
		if _, seen := seenChats[chatID]; seen {
			continue
		}
		seenChats[chatID] = struct{}{}
		if len(replay[chatID]) <= dockerRealDataPageSize {
			continue
		}
		poll := state.ChatPolls[chatID]
		// Preserve the copied frontier/recovery evidence, but choose a clean
		// head-poll witness. A chat already carrying a continuation, gap,
		// pending receipt, attempt, or queued turn may legitimately spend the
		// short window on recovery rather than exercising the replay corpus's
		// own multi-page head path.
		if poll.ContinuationPath != "" || poll.DeferredContinuationPath != "" || poll.PendingPage != nil || poll.Attempt != nil || poll.Gap != nil {
			continue
		}
		if queue := queueStateByChat[chatID]; queue.Running || queue.Queued > 0 {
			continue
		}
		if witness == "" || chatID < witness {
			witness = chatID
		}
	}
	if witness == "" {
		return ""
	}
	poll := state.ChatPolls[witness]
	pollState := strings.TrimSpace(poll.PollState)
	if pollState == "" {
		pollState = inboundPollStateWarm
	}
	if _, err := store.UpdateChatPollSchedule(context.Background(), teamstore.ChatPollScheduleUpdate{
		ChatID:            witness,
		PollState:         pollState,
		NextPollAt:        now.Add(-4 * time.Minute),
		LastActivityAt:    now,
		ClearBlockedUntil: true,
		ResetFailures:     true,
	}); err != nil {
		t.Fatalf("prioritize disposable pagination witness %q: %v", witness, err)
	}
	// The production scalar admission order is intentionally stable by
	// updated_at before next_poll_at.  Updating the witness after the bulk
	// replay wake-up would therefore move it behind every replay row even
	// though its next_poll_at is earlier.  Refresh the remaining replay rows
	// after the witness so the witness is the oldest due row, while retaining
	// the same due-time/activity policy and all copied frontier/error fields.
	otherReplayUpdates := make([]teamstore.ChatPollScheduleUpdate, 0, len(replay))
	for chatID, messages := range replay {
		chatID = strings.TrimSpace(chatID)
		if chatID == "" || chatID == witness || len(messages) == 0 || chatID == controlChatID {
			continue
		}
		otherPoll := state.ChatPolls[chatID]
		otherState := strings.TrimSpace(otherPoll.PollState)
		if otherState == "" {
			otherState = inboundPollStateWarm
		}
		otherReplayUpdates = append(otherReplayUpdates, teamstore.ChatPollScheduleUpdate{
			ChatID:    chatID,
			PollState: otherState,
			// The bulk replay wake-up already used now-1s.  Use a still-due
			// timestamp one second earlier so this second pass is a real durable
			// update and therefore advances updated_at behind the witness.
			NextPollAt:     now.Add(-2 * time.Second),
			LastActivityAt: now,
		})
	}
	if len(otherReplayUpdates) > 0 {
		if _, err := store.UpdateChatPollSchedules(context.Background(), otherReplayUpdates); err != nil {
			t.Fatalf("refresh replay schedules behind pagination witness %q: %v", witness, err)
		}
	}
	return witness
}

func TestPrioritizeDockerRealDataPaginationWitnessWakesOneLargeReplayChat(t *testing.T) {
	store := newBridgeTestStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load disposable Teams state: %v", err)
	}
	state.Sessions["large-session"] = teamstore.SessionContext{
		ID:          "large-session",
		Status:      teamstore.SessionStatusActive,
		TeamsChatID: "large-replay-chat",
	}
	state.Sessions["small-session"] = teamstore.SessionContext{
		ID:          "small-session",
		Status:      teamstore.SessionStatusActive,
		TeamsChatID: "small-replay-chat",
	}
	state.ChatPolls["large-replay-chat"] = teamstore.ChatPollState{ChatID: "large-replay-chat", Seeded: true, PollState: inboundPollStateCold}
	state.ChatPolls["small-replay-chat"] = teamstore.ChatPollState{ChatID: "small-replay-chat", Seeded: true, PollState: inboundPollStateCold}
	if err := store.Update(context.Background(), func(current *teamstore.State) error {
		*current = state
		return nil
	}); err != nil {
		t.Fatalf("persist pagination schedule fixture: %v", err)
	}
	witness := prioritizeDockerRealDataPaginationWitness(t, store, state, map[string][]ChatMessage{
		"large-replay-chat": make([]ChatMessage, dockerRealDataPageSize+1),
		"small-replay-chat": make([]ChatMessage, dockerRealDataPageSize),
	}, "control-chat", now)
	if witness != "large-replay-chat" {
		t.Fatalf("pagination witness=%q, want large-replay-chat", witness)
	}
	poll, found, err := store.ChatPoll(context.Background(), witness)
	if err != nil || !found {
		t.Fatalf("read pagination witness poll found=%v err=%v", found, err)
	}
	if !poll.NextPollAt.Before(now) || !poll.LastActivityAt.Equal(now) || !poll.BlockedUntil.IsZero() || poll.FailureCount != 0 {
		t.Fatalf("pagination witness was not durably prioritized: %#v", poll)
	}
	other, found, err := store.ChatPoll(context.Background(), "small-replay-chat")
	if err != nil || !found {
		t.Fatalf("read non-witness poll found=%v err=%v", found, err)
	}
	if !other.NextPollAt.Before(now) || !other.LastActivityAt.Equal(now) || !other.UpdatedAt.After(poll.UpdatedAt) {
		t.Fatalf("non-witness replay schedule was not refreshed behind witness: witness=%#v other=%#v", poll, other)
	}
}

// dockerRealData429PollSessions selects a small, real-data subset for the
// high-intensity 429 experiment. The selection is deliberately limited to
// seeded chats with no existing continuation/receipt/gap/attempt, so the test
// invokes the same production poll frontier and handler with an ordinary head
// page instead of accidentally measuring unrelated recovery work from the
// copied snapshot. The rest of the copied SQLite state is retained untouched.
func dockerRealData429PollSessions(reg Registry, state teamstore.State, replay map[string][]ChatMessage, controlChatID string, rateLimited int, healthy int) ([]Session, error) {
	if rateLimited <= 0 || healthy < 0 {
		return nil, fmt.Errorf("invalid Docker 429 session selection: rate_limited=%d healthy=%d", rateLimited, healthy)
	}
	controlChatID = strings.TrimSpace(controlChatID)
	sessions := append([]Session(nil), reg.Sessions...)
	sort.SliceStable(sessions, func(i, j int) bool {
		return strings.TrimSpace(sessions[i].ChatID) < strings.TrimSpace(sessions[j].ChatID)
	})
	want := rateLimited + healthy
	selected := make([]Session, 0, want)
	seen := make(map[string]struct{}, want)
	for _, session := range sessions {
		chatID := strings.TrimSpace(session.ChatID)
		if chatID == "" || chatID == controlChatID || !isActiveSessionStatus(session.Status) {
			continue
		}
		if _, ok := seen[chatID]; ok || len(replay[chatID]) == 0 {
			continue
		}
		poll, ok := state.ChatPolls[chatID]
		if !ok || !poll.Seeded || poll.LastModifiedCursor.IsZero() || poll.Attempt != nil || poll.PendingPage != nil || poll.Gap != nil ||
			strings.TrimSpace(poll.ContinuationPath) != "" || strings.TrimSpace(poll.DeferredContinuationPath) != "" {
			continue
		}
		seen[chatID] = struct{}{}
		selected = append(selected, session)
		if len(selected) == want {
			break
		}
	}
	if len(selected) != want {
		return nil, fmt.Errorf("copied real-data snapshot has only %d clean seeded replay chats; want %d (rate_limited=%d healthy=%d)", len(selected), want, rateLimited, healthy)
	}
	return selected, nil
}

func TestDockerRealData429PollSessionsSelectsOnlyCleanSeededReplayChats(t *testing.T) {
	cursor := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	reg := Registry{Sessions: []Session{
		{ID: "s-control", ChatID: "control-chat", Status: "active"},
		{ID: "s-rate", ChatID: "z-rate-chat", Status: "active"},
		{ID: "s-healthy", ChatID: "a-healthy-chat", Status: "active"},
		{ID: "s-duplicate", ChatID: "z-rate-chat", Status: "active"},
		{ID: "s-attempt", ChatID: "b-attempt-chat", Status: "active"},
		{ID: "s-pending", ChatID: "c-pending-chat", Status: "active"},
		{ID: "s-gap", ChatID: "d-gap-chat", Status: "active"},
		{ID: "s-continuation", ChatID: "e-continuation-chat", Status: "active"},
		{ID: "s-deferred", ChatID: "f-deferred-chat", Status: "active"},
		{ID: "s-unseeded", ChatID: "g-unseeded-chat", Status: "active"},
		{ID: "s-no-cursor", ChatID: "h-no-cursor-chat", Status: "active"},
		{ID: "s-closed", ChatID: "i-closed-chat", Status: "closed"},
	}}
	replay := make(map[string][]ChatMessage)
	for _, session := range reg.Sessions {
		replay[session.ChatID] = []ChatMessage{{}}
	}
	state := teamstore.State{ChatPolls: map[string]teamstore.ChatPollState{
		"z-rate-chat": {
			ChatID: "z-rate-chat", Seeded: true, LastModifiedCursor: cursor,
		},
		"a-healthy-chat": {
			ChatID: "a-healthy-chat", Seeded: true, LastModifiedCursor: cursor,
		},
		"b-attempt-chat": {
			ChatID: "b-attempt-chat", Seeded: true, LastModifiedCursor: cursor,
			Attempt: &teamstore.ChatPollAttempt{ID: "attempt"},
		},
		"c-pending-chat": {
			ChatID: "c-pending-chat", Seeded: true, LastModifiedCursor: cursor,
			PendingPage: &teamstore.ChatPollPendingPage{ReceiptID: "receipt"},
		},
		"d-gap-chat": {
			ChatID: "d-gap-chat", Seeded: true, LastModifiedCursor: cursor,
			Gap: &teamstore.ChatPollGap{Kind: "gap-recovery"},
		},
		"e-continuation-chat": {
			ChatID: "e-continuation-chat", Seeded: true, LastModifiedCursor: cursor,
			ContinuationPath: "/chats/e-continuation-chat/messages?$skiptoken=next",
		},
		"f-deferred-chat": {
			ChatID: "f-deferred-chat", Seeded: true, LastModifiedCursor: cursor,
			DeferredContinuationPath: "/chats/f-deferred-chat/messages?$skiptoken=next",
		},
		"g-unseeded-chat": {
			ChatID: "g-unseeded-chat", LastModifiedCursor: cursor,
		},
		"h-no-cursor-chat": {
			ChatID: "h-no-cursor-chat", Seeded: true,
		},
		"i-closed-chat": {
			ChatID: "i-closed-chat", Seeded: true, LastModifiedCursor: cursor,
		},
	}}

	selected, err := dockerRealData429PollSessions(reg, state, replay, "control-chat", 1, 1)
	if err != nil {
		t.Fatalf("select clean replay chats: %v", err)
	}
	if got := []string{selected[0].ChatID, selected[1].ChatID}; !reflect.DeepEqual(got, []string{"a-healthy-chat", "z-rate-chat"}) {
		t.Fatalf("selected chats = %v, want deterministic clean subset [a-healthy-chat z-rate-chat]", got)
	}
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
	served := serverState.servedMessages()
	if len(served) != len(messages) {
		t.Fatalf("strict Docker Graph poll served %d/%d unique messages", len(served), len(messages))
	}
	for _, message := range messages {
		if got := served[message.ID]; got != 1 {
			t.Fatalf("strict Docker Graph poll served message %q %d times, want exactly once", message.ID, got)
		}
	}
	if serverState.opaqueContinuationCount() < 2 {
		t.Fatalf("strict Docker Graph poll did not exercise opaque continuations: count=%d", serverState.opaqueContinuationCount())
	}
	var opaquePollRequest bool
	for _, request := range serverState.listRequests() {
		if strings.Contains(request, "%24skiptoken=%3Credacted%3E") &&
			!strings.Contains(request, "%24filter=") && !strings.Contains(request, "%24orderby=") {
			opaquePollRequest = true
			break
		}
	}
	if !opaquePollRequest {
		t.Fatalf("strict Docker Graph poll did not issue an opaque continuation without filter/order: requests=%v", serverState.listRequests())
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
	opaqueBeforeExplicitProviderRequest := serverState.opaqueContinuationCount()
	opaqueRequest := httptest.NewRequest(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/messages?"+values.Encode(), nil)
	opaqueRequest.Header.Set("Authorization", "Bearer docker-contract-token")
	opaqueRecorder := httptest.NewRecorder()
	serverState.ServeHTTP(opaqueRecorder, opaqueRequest)
	if opaqueRecorder.Code != http.StatusOK {
		t.Fatalf("Docker Graph provider opaque continuation response = %d, want 200", opaqueRecorder.Code)
	}
	if serverState.opaqueContinuationCount() != opaqueBeforeExplicitProviderRequest+1 {
		t.Fatalf("Docker Graph did not exercise exactly one explicit provider opaque continuation: before=%d after=%d", opaqueBeforeExplicitProviderRequest, serverState.opaqueContinuationCount())
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

func TestDockerRealDataGraphServerRetryFaultIgnoresNonPollReads(t *testing.T) {
	chatID := "docker-retry-fault-chat"
	message := dockerRealDataMessage(chatID, 0, 0, time.Now().UTC())
	serverState := newDockerRealDataGraphServer("docker-retry-fault-token", User{ID: "docker-retry-fault-user"}, map[string][]ChatMessage{
		chatID: {message},
	})
	serverState.setFaultChat("")
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)

	requestStatus := func(path string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if err != nil {
			t.Fatalf("retry fault request %q: %v", path, err)
		}
		req.Header.Set("Authorization", "Bearer docker-retry-fault-token")
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatalf("retry fault request %q: %v", path, err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	// Outbox recovery and other message reads must not consume the poll fault:
	// otherwise a real-data run can report a 429 without ever exercising the
	// durable poll retry path it was intended to test.
	nonPollPath := "/chats/" + url.PathEscape(chatID) + "/messages?$top=20"
	if got := requestStatus(nonPollPath); got != http.StatusOK {
		t.Fatalf("non-poll read status = %d, want 200", got)
	}
	if serverState.status429.Load() != 0 || serverState.status503.Load() != 0 {
		t.Fatalf("non-poll read consumed retry fault: 429=%d 503=%d", serverState.status429.Load(), serverState.status503.Load())
	}

	values := url.Values{}
	values.Set("$filter", "lastModifiedDateTime gt 2026-09-07T12:00:00Z")
	values.Set("$orderby", "lastModifiedDateTime desc")
	values.Set("$top", "20")
	pollPath := "/chats/" + url.PathEscape(chatID) + "/messages?" + values.Encode()
	for index, want := range append([]int{http.StatusTooManyRequests}, make([]int, defaultGraphRetries+1)...) {
		if index > 0 {
			want = http.StatusServiceUnavailable
		}
		if got := requestStatus(pollPath); got != want {
			t.Fatalf("poll retry fault request %d status = %d, want %d", index+1, got, want)
		}
	}
	if got := requestStatus(pollPath); got != http.StatusOK {
		t.Fatalf("poll request after retry fault status = %d, want 200", got)
	}
	if serverState.status429.Load() != 1 || serverState.status503.Load() != int64(defaultGraphRetries+1) {
		t.Fatalf("retry fault counts = 429:%d 503:%d, want 1/%d", serverState.status429.Load(), serverState.status503.Load(), defaultGraphRetries+1)
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

func TestDockerRealDataGraphServerAccount429SharesBudgetAcrossChats(t *testing.T) {
	replay := map[string][]ChatMessage{
		"account-429-chat-a": {dockerRealDataMessage("account-429-chat-a", 0, 0, time.Now().UTC())},
		"account-429-chat-b": {dockerRealDataMessage("account-429-chat-b", 0, 0, time.Now().UTC())},
	}
	serverState := newDockerRealDataGraphServer("account-429-token", User{ID: "account-429-user"}, replay)
	serverState.setPersistentGlobalList429(4)
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)

	request := func(chatID string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, server.URL+"/chats/"+url.PathEscape(chatID)+"/messages?$top=20", nil)
		if err != nil {
			t.Fatalf("create account 429 request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer account-429-token")
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatalf("account 429 request for %s: %v", chatID, err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	for index, chatID := range []string{"account-429-chat-a", "account-429-chat-b", "account-429-chat-a", "account-429-chat-b"} {
		if got := request(chatID); got != http.StatusTooManyRequests {
			t.Fatalf("account 429 request %d for %s status = %d, want 429", index+1, chatID, got)
		}
	}
	if got := request("account-429-chat-a"); got != http.StatusOK {
		t.Fatalf("first request after account 429 budget status = %d, want 200", got)
	}
	if got := serverState.status429.Load(); got != 4 {
		t.Fatalf("account 429 responses = %d, want shared budget of 4 across both chats", got)
	}
	if !serverState.persistentGlobalList429Configured() {
		t.Fatal("account 429 server did not retain global-scope configuration")
	}
}

func TestDockerRealDataGraphServerGlobal429PreservesExplicitScope(t *testing.T) {
	chatID := "global-429-chat"
	serverState := newDockerRealDataGraphServer("global-429-token", User{ID: "global-429-user"}, map[string][]ChatMessage{
		chatID: {dockerRealDataMessage(chatID, 0, 0, time.Now().UTC())},
	})
	serverState.setPersistentGlobalList429WithScope(1, dockerRealData429ScopeGlobal)
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)

	request := func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodGet, server.URL+"/chats/"+url.PathEscape(chatID)+"/messages?$top=20", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer global-429-token")
		return server.Client().Do(req)
	}
	first, err := request()
	if err != nil {
		t.Fatalf("global 429 request: %v", err)
	}
	if first.StatusCode != http.StatusTooManyRequests || first.Header.Get("X-CXP-RateLimit-Scope") != dockerRealData429ScopeGlobal {
		_ = first.Body.Close()
		t.Fatalf("global 429 response = status=%d scope=%q, want 429/global", first.StatusCode, first.Header.Get("X-CXP-RateLimit-Scope"))
	}
	_ = first.Body.Close()
	second, err := request()
	if err != nil {
		t.Fatalf("global recovery request: %v", err)
	}
	defer second.Body.Close()
	if second.StatusCode != http.StatusOK {
		t.Fatalf("global recovery response status = %d, want 200", second.StatusCode)
	}
}

func TestDockerRealDataGraphServerOperation429WitnessCoversReadsAndWrites(t *testing.T) {
	chatID := "operation-429-chat"
	message := dockerRealDataMessage(chatID, 0, 0, time.Now().UTC())
	serverState := newDockerRealDataGraphServer("operation-429-token", User{ID: "operation-429-user"}, map[string][]ChatMessage{
		chatID: {message},
	})
	serverState.setUnknownPostMarker("__operation_429_unknown_marker__")
	for _, operation := range []string{
		dockerRealDataGraphOpMessageItem,
		dockerRealDataGraphOpMembers,
		dockerRealDataGraphOpMe,
		dockerRealDataGraphOpMessagePost,
		dockerRealDataGraphOpMarkUnread,
		dockerRealDataGraphOpMeetingPost,
		dockerRealDataGraphOpPatch,
	} {
		serverState.setPersistentOperation429(operation, dockerRealData429ScopeGlobal, 1)
	}
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)

	request := func(method, path, body string) int {
		t.Helper()
		var bodyReader io.Reader
		if body != "" {
			bodyReader = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, server.URL+path, bodyReader)
		if err != nil {
			t.Fatalf("create operation 429 request %s %s: %v", method, path, err)
		}
		req.Header.Set("Authorization", "Bearer operation-429-token")
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatalf("operation 429 request %s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}

	itemPath := "/chats/" + url.PathEscape(chatID) + "/messages/" + url.PathEscape(message.ID)
	if got := request(http.MethodGet, itemPath, ""); got != http.StatusTooManyRequests {
		t.Fatalf("item GET first status=%d, want 429", got)
	}
	if got := request(http.MethodGet, itemPath, ""); got != http.StatusOK {
		t.Fatalf("item GET recovery status=%d, want 200", got)
	}
	if got := request(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/members", ""); got != http.StatusTooManyRequests {
		t.Fatalf("members GET first status=%d, want 429", got)
	}
	if got := request(http.MethodGet, "/chats/"+url.PathEscape(chatID)+"/members", ""); got != http.StatusOK {
		t.Fatalf("members GET recovery status=%d, want 200", got)
	}
	if got := request(http.MethodGet, "/me", ""); got != http.StatusTooManyRequests {
		t.Fatalf("/me first status=%d, want 429", got)
	}
	if got := request(http.MethodGet, "/me", ""); got != http.StatusOK {
		t.Fatalf("/me recovery status=%d, want 200", got)
	}
	messageBody := `{"body":{"content":"operation fault post"}}`
	messagePath := "/chats/" + url.PathEscape(chatID) + "/messages"
	if got := request(http.MethodPost, messagePath, messageBody); got != http.StatusTooManyRequests {
		t.Fatalf("message POST first status=%d, want 429", got)
	}
	if got := request(http.MethodPost, messagePath, messageBody); got != http.StatusOK {
		t.Fatalf("message POST recovery status=%d, want 200", got)
	}
	markUnreadPath := "/chats/" + url.PathEscape(chatID) + "/markChatUnreadForUser"
	if got := request(http.MethodPost, markUnreadPath, `{"user":{"id":"operation-429-user"}}`); got != http.StatusTooManyRequests {
		t.Fatalf("mark-unread POST first status=%d, want 429", got)
	}
	if got := request(http.MethodPost, markUnreadPath, `{"user":{"id":"operation-429-user"}}`); got != http.StatusNoContent {
		t.Fatalf("mark-unread POST recovery status=%d, want 204", got)
	}
	if got := request(http.MethodPost, "/me/onlineMeetings", `{"subject":"operation fault meeting"}`); got != http.StatusTooManyRequests {
		t.Fatalf("meeting POST first status=%d, want 429", got)
	}
	if got := request(http.MethodPost, "/me/onlineMeetings", `{"subject":"operation fault meeting"}`); got != http.StatusOK {
		t.Fatalf("meeting POST recovery status=%d, want 200", got)
	}
	patchPath := "/chats/" + url.PathEscape(chatID) + "/messages/" + url.PathEscape(message.ID)
	patchBody := `{"body":{"content":"operation fault patch"}}`
	if got := request(http.MethodPatch, patchPath, patchBody); got != http.StatusTooManyRequests {
		t.Fatalf("PATCH first status=%d, want 429", got)
	}
	if got := request(http.MethodPatch, patchPath, patchBody); got != http.StatusNoContent {
		t.Fatalf("PATCH recovery status=%d, want 204", got)
	}

	for _, operation := range []string{
		dockerRealDataGraphOpMessageItem,
		dockerRealDataGraphOpMembers,
		dockerRealDataGraphOpMe,
		dockerRealDataGraphOpMessagePost,
		dockerRealDataGraphOpMarkUnread,
		dockerRealDataGraphOpMeetingPost,
		dockerRealDataGraphOpPatch,
	} {
		attempts, throttled, accepted := serverState.graphOperationCounts(operation)
		if attempts != 2 || throttled != 1 {
			t.Fatalf("operation %s counts=(attempts=%d,429=%d,accepted=%d), want 2/1", operation, attempts, throttled, accepted)
		}
		if operation == dockerRealDataGraphOpMessagePost || operation == dockerRealDataGraphOpMarkUnread || operation == dockerRealDataGraphOpMeetingPost || operation == dockerRealDataGraphOpPatch {
			if accepted != 1 {
				t.Fatalf("operation %s accepted=%d, want one remote accept after one 429", operation, accepted)
			}
		} else if accepted != 0 {
			t.Fatalf("read operation %s accepted=%d, want zero remote write accepts", operation, accepted)
		}
	}

	records := serverState.graphRequestsSnapshot()
	if len(records) != 14 {
		t.Fatalf("graph request witness count=%d, want 14", len(records))
	}
	seen429 := make(map[string]bool)
	seenAccepted := make(map[string]bool)
	for _, record := range records {
		if record.RateLimitScope != "" && record.RateLimitScope != dockerRealData429ScopeGlobal {
			t.Fatalf("request witness scope=%q, want global: %#v", record.RateLimitScope, record)
		}
		if record.StatusCode == http.StatusTooManyRequests {
			seen429[record.Operation] = true
			if record.RemoteAccepted {
				t.Fatalf("429 request was marked remotely accepted: %#v", record)
			}
		}
		if record.RemoteAccepted {
			seenAccepted[record.Operation] = true
			if record.BodyHash == "" {
				t.Fatalf("accepted write lacks exact body hash: %#v", record)
			}
		}
	}
	for _, operation := range []string{
		dockerRealDataGraphOpMessageItem,
		dockerRealDataGraphOpMembers,
		dockerRealDataGraphOpMe,
		dockerRealDataGraphOpMessagePost,
		dockerRealDataGraphOpMarkUnread,
		dockerRealDataGraphOpMeetingPost,
		dockerRealDataGraphOpPatch,
	} {
		if !seen429[operation] || (operation == dockerRealDataGraphOpMessagePost && !seenAccepted[operation]) {
			t.Fatalf("operation %s lacks a complete 429/accept witness: records=%#v", operation, records)
		}
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
	if got := serverState.messagePostAttempts.Load(); got != 2 {
		t.Fatalf("valid message POST attempts = %d, want 2", got)
	}
	if got := serverState.messagePostAccepts.Load(); got != 2 {
		t.Fatalf("valid message POST remote accepts = %d, want 2", got)
	}
	if got := serverState.messagePostResponses.Load(); got != 1 {
		t.Fatalf("successful message POST responses = %d, want 1 after one unknown response", got)
	}
	if got := serverState.markUnreadPosts.Load(); got != 0 {
		t.Fatalf("mark-unread POST count = %d, want 0", got)
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

func TestDockerRealDataGraphServerRejectsDurableUnknownPostReplay(t *testing.T) {
	chatID := "docker-durable-witness-chat"
	outboxID := "docker-durable-witness-outbox"
	serverState := newDockerRealDataGraphServer("docker-durable-witness-token", User{ID: "docker-durable-witness-user"}, map[string][]ChatMessage{chatID: nil})
	payload := `{"body":{"content":"replayed result ` + helperOutboxProvenanceMarker(outboxID) + `"}}`
	serverState.setDurableUnknownPostWitness(dockerRealDataUnknownPostWitness{
		OutboxID:    outboxID,
		ChatID:      chatID,
		BodyHash:    "already-recorded-body",
		PayloadHash: dockerRealDataPostPayloadHash([]byte(payload)),
	})
	server := httptest.NewServer(serverState)
	t.Cleanup(server.Close)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/chats/"+url.PathEscape(chatID)+"/messages", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("create replay request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer docker-durable-witness-token")
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("durable unknown POST replay request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("durable unknown POST replay status = %d, want %d", response.StatusCode, http.StatusConflict)
	}
	if got := serverState.unknownPostRepeats.Load(); got != 1 {
		t.Fatalf("durable unknown POST replay count = %d, want 1", got)
	}
	if got := serverState.messagePostAttempts.Load(); got != 1 {
		t.Fatalf("durable unknown POST replay attempts = %d, want 1", got)
	}
	if got := serverState.messagePostAccepts.Load(); got != 0 {
		t.Fatalf("durable unknown POST replay remote accepts = %d, want 0", got)
	}
	// Matching the outbox ID and destination is not sufficient: a changed
	// payload must not be mistaken for the previously accepted operation.
	mismatchPayload := `{"body":{"content":"changed result ` + helperOutboxProvenanceMarker(outboxID) + `"}}`
	mismatchReq, err := http.NewRequest(http.MethodPost, server.URL+"/chats/"+url.PathEscape(chatID)+"/messages", strings.NewReader(mismatchPayload))
	if err != nil {
		t.Fatalf("create mismatched replay request: %v", err)
	}
	mismatchReq.Header.Set("Authorization", "Bearer docker-durable-witness-token")
	mismatchResponse, err := server.Client().Do(mismatchReq)
	if err != nil {
		t.Fatalf("mismatched durable unknown POST replay request: %v", err)
	}
	defer mismatchResponse.Body.Close()
	if mismatchResponse.StatusCode != http.StatusConflict {
		t.Fatalf("mismatched durable unknown POST replay status = %d, want %d", mismatchResponse.StatusCode, http.StatusConflict)
	}
	if got := serverState.unknownPostMismatches.Load(); got != 1 {
		t.Fatalf("mismatched durable unknown POST count = %d, want 1", got)
	}
	if got := serverState.messagePostAccepts.Load(); got != 0 {
		t.Fatalf("mismatched durable unknown POST remote accepts = %d, want 0", got)
	}
}

func TestDockerRealDataPostMarkerRequiresExactRenderedBody(t *testing.T) {
	marker := "docker real-data execution result #1"
	for name, payload := range map[string]string{
		"ordinary":         `{"body":{"content":"docker real-data execution result #1"}}`,
		"quoted":           `{"messageIds":["source"],"replyMessage":{"body":{"content":"docker real-data execution result #1"}}}`,
		"rendered":         `{"body":{"content":"<p><strong>Codex:</strong></p><p>docker real-data execution result #1</p>"}}`,
		"rendered-quoted":  `{"replyMessage":{"body":{"content":"<p><strong>Codex:</strong></p><p>docker real-data execution result #1</p>"}}}`,
		"suffix":           `{"body":{"content":"docker real-data execution result #10"}}`,
		"nested-unrelated": `{"body":{"content":"unrelated","metadata":"docker real-data execution result #1"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			want := name == "ordinary" || name == "quoted" || name == "rendered" || name == "rendered-quoted"
			if got := dockerRealDataPostPayloadMatchesMarker([]byte(payload), marker); got != want {
				t.Fatalf("marker match = %v, want %v for payload %s", got, want, payload)
			}
		})
	}
	finalOutboxID := "outbox:docker-real-data-turn-1:final"
	ackOutboxID := "outbox:docker-real-data-turn-1:ack"
	for name, payload := range map[string]string{
		"any-rendered":         `{"body":{"content":"<p><strong>🤖 ✅ Codex answer:</strong></p><p>docker real-data execution result #10</p>"}}`,
		"any-unrelated":        `{"body":{"content":"<p>unrelated docker real-data execution result #10 metadata</p>"}}`,
		"any-no-number":        `{"body":{"content":"<p>docker real-data execution result #</p>"}}`,
		"any-final-provenance": `{"body":{"content":"<p>wrapped final body</p>` + helperOutboxProvenanceMarker(finalOutboxID) + `"}}`,
		"any-ack-provenance":   `{"body":{"content":"<p>wrapped ack body</p>` + helperOutboxProvenanceMarker(ackOutboxID) + `"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			want := name == "any-rendered" || name == "any-final-provenance"
			if got := dockerRealDataPostPayloadMatchesMarker([]byte(payload), dockerRealDataAnyExecutionMarker); got != want {
				t.Fatalf("any execution marker match = %v, want %v for payload %s", got, want, payload)
			}
		})
	}
}

func TestDockerRealDataDuplicateMessagePostAudit(t *testing.T) {
	server := newDockerRealDataGraphServer("token", User{}, nil)
	server.mu.Lock()
	server.messagePostOutboxAttempts["outbox:duplicate"] = 2
	server.messagePostOutboxAttempts["outbox:single"] = 1
	server.messagePostOutboxAttempts[""] = 9
	server.mu.Unlock()
	if got, want := server.duplicateMessagePostOutboxIDs(), []string{"outbox:duplicate"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("duplicate message POST outbox IDs = %v, want %v", got, want)
	}
}

func TestDockerRealDataUnknownPostDispositionRejectsSkippedCompletion(t *testing.T) {
	base := teamstore.OutboxMessage{Status: teamstore.OutboxStatusSkipped, TeamsChatID: "chat-disposition"}
	for name, msg := range map[string]teamstore.OutboxMessage{
		"ack": {
			Status:      base.Status,
			TeamsChatID: base.TeamsChatID,
			Kind:        "ack",
		},
		"helper": {
			Status:      base.Status,
			TeamsChatID: base.TeamsChatID,
			Kind:        "helper-status",
		},
		"final": {
			Status:           base.Status,
			TeamsChatID:      base.TeamsChatID,
			Kind:             "final",
			NotificationKind: "turn_completed",
		},
		"needs-attention": {
			Status:           base.Status,
			TeamsChatID:      base.TeamsChatID,
			Kind:             "final",
			NotificationKind: "needs_attention",
		},
	} {
		t.Run(name, func(t *testing.T) {
			want := name == "ack" || name == "helper"
			if got := dockerRealDataUnknownPostDispositionSafe(msg); got != want {
				t.Fatalf("unknown POST disposition safe = %v, want %v for %#v", got, want, msg)
			}
		})
	}
}

func TestDockerRealDataThroughputGateExcludesGracefulDrain(t *testing.T) {
	if got := dockerRealDataCompletionForGate(dockerRealDataModeThroughput, 0, 7); got != 0 {
		t.Fatalf("throughput gate completion = %d, want measured-window value 0", got)
	}
	if got := dockerRealDataCompletionForGate(dockerRealDataModeThroughput, 3, 7); got != 3 {
		t.Fatalf("throughput gate completion = %d, want measured-window value 3", got)
	}
	if got := dockerRealDataCompletionForGate(dockerRealDataModeComplete, 0, 7); got != 7 {
		t.Fatalf("complete gate completion = %d, want post-drain value 7", got)
	}
	started := time.Unix(100, 0)
	ended := started.Add(1250 * time.Millisecond)
	window := dockerRealDataMeasuredWindow{startedAt: started, endedAt: ended}
	if got := dockerRealDataMeasuredWindowDuration(window, 9*time.Second); got != 1250*time.Millisecond {
		t.Fatalf("measured window duration = %s, want 1.25s", got)
	}
	if got := dockerRealDataMeasuredWindowDuration(dockerRealDataMeasuredWindow{}, 9*time.Second); got != 9*time.Second {
		t.Fatalf("invalid measured window duration = %s, want fallback 9s", got)
	}
}

func TestDockerRealDataMeasuredCompletionUsesDurableTransitionTime(t *testing.T) {
	started := time.Unix(100, 0).UTC()
	ended := started.Add(time.Second)
	before := map[string]dockerRealDataSyntheticTurnObservation{
		"already-complete": {ID: "already-complete", Status: teamstore.TurnStatusCompleted, UpdatedAt: started.Add(-time.Millisecond)},
		"running":          {ID: "running", Status: teamstore.TurnStatusRunning, UpdatedAt: started.Add(-time.Millisecond)},
	}
	after := map[string]dockerRealDataSyntheticTurnObservation{
		"already-complete": {ID: "already-complete", Status: teamstore.TurnStatusCompleted, UpdatedAt: started.Add(500 * time.Millisecond)},
		"running":          {ID: "running", Status: teamstore.TurnStatusCompleted, UpdatedAt: started.Add(250 * time.Millisecond)},
		"after-window":     {ID: "after-window", Status: teamstore.TurnStatusCompleted, UpdatedAt: ended.Add(time.Nanosecond)},
		"before-window":    {ID: "before-window", Status: teamstore.TurnStatusCompleted, UpdatedAt: started.Add(-time.Nanosecond)},
	}
	if got := dockerRealDataCompletedTransitionsBetween(before, after, started, ended); got != 1 {
		t.Fatalf("measured durable completion transitions = %d, want 1", got)
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

func TestDockerRealDataPostStateIncludesExplicitOutboxWitness(t *testing.T) {
	ctx := context.Background()
	statePath := filepath.Join(t.TempDir(), "state", "state.json")
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("open witness store: %v", err)
	}
	defer store.Close()
	createdAt := time.Now().UTC()
	witness := teamstore.OutboxMessage{
		ID:              "outbox:docker-real-data-helper-witness",
		TeamsChatID:     "docker-real-data-witness-chat",
		Kind:            "helper-008",
		Body:            "low-value helper acknowledgement",
		Status:          teamstore.OutboxStatusSending,
		CreatedAt:       createdAt,
		UpdatedAt:       createdAt,
		LastSendAttempt: createdAt,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.OutboxMessages[witness.ID] = witness
		return nil
	}); err != nil {
		t.Fatalf("seed witness store: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate witness store to SQLite: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close witness store before read: %v", err)
	}

	got, err := dockerRealDataPostStateFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName), map[string][]ChatMessage{}, witness.ID)
	if err != nil {
		t.Fatalf("read explicit outbox witness: %v", err)
	}
	row, found := got.OutboxMessages[witness.ID]
	if !found {
		t.Fatalf("post-state reader omitted explicit helper witness %q", witness.ID)
	}
	if row.TeamsChatID != witness.TeamsChatID || row.Body != witness.Body || row.Status != witness.Status {
		t.Fatalf("post-state helper witness = %#v, want chat/body/status from %#v", row, witness)
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
	chatCoverage := dockerRealDataChatCoverage(t)
	requireAllLagging := dockerRealDataRequireAllLagging(t)
	resume := dockerRealDataResume(t)
	if chatCoverage && mode != dockerRealDataModeComplete {
		t.Fatalf("%s=1 requires %s=%q so every selected chat must close its durable representative message", dockerRealDataChatCoverageEnv, dockerRealDataModeEnv, dockerRealDataModeComplete)
	}
	if chatCoverage && resume {
		t.Fatalf("%s=1 cannot be combined with %s=1; chat coverage must start from a fresh disposable runtime", dockerRealDataChatCoverageEnv, dockerRealDataResumeEnv)
	}
	rateLimitExperiment := os.Getenv(dockerRealData429ExperimentEnv) == "1"
	if chatCoverage && rateLimitExperiment {
		t.Fatalf("%s=1 cannot be combined with the 429 experiment; run the all-chat completion and rate-limit liveness gates separately", dockerRealDataChatCoverageEnv)
	}
	if requireAllLagging && mode != dockerRealDataModeComplete {
		t.Fatalf("%s=1 requires %s=%q so every lagging message must reach a terminal durable state", dockerRealDataRequireAllLaggingEnv, dockerRealDataModeEnv, dockerRealDataModeComplete)
	}
	rateLimitScope := dockerRealData429Scope(t)
	rateLimitAccountWide := dockerRealData429ScopeIsAccountWide(rateLimitScope)
	rateLimitPollOnly := rateLimitExperiment && dockerRealData429PollOnly(t)
	duration := dockerRealDataDuration(t)
	startupDeadlineDuration := dockerRealDataStartupDeadline(t)
	pollInterval := dockerRealDataPollInterval(t, rateLimitExperiment)
	fixtureRoot := dockerTeamsFixtureRoot(t)
	store, statePath := prepareDockerFixtureStore(t, fixtureRoot)
	dockerFixtureRemapCodexPaths(t, store)
	dockerFixtureRebindSourceProofs(t, store)
	dockerFixtureVerifyOutboxSourceProofs(t, store)
	ctx, cancel := context.WithTimeout(context.Background(), duration+10*time.Minute)
	defer cancel()

	loadStarted := time.Now()
	// This experiment needs the real queued inbound/turn/poll/checkpoint rows,
	// but it must not materialize the inherited outbox just to establish a
	// baseline. The current fixture contains about 1.3GB of outbox JSON; the
	// production listener keeps that table intact, while this test uses a
	// scalar/SQL count and reads a single resume witness by ID.
	beforeState, err := store.PollStateSnapshot(ctx)
	if err != nil {
		t.Fatalf("load copied real Teams SQLite poll state: %v", err)
	}
	historyState, err := store.HistoryWatchState(ctx)
	if err != nil {
		t.Fatalf("load copied real Teams SQLite history state: %v", err)
	}
	beforeState.HistoryWatch = historyState.HistoryWatch
	beforeState.HistoryWatchReady = historyState.HistoryWatchReady
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
	beforeCorrelationAudit, err := dockerRealDataDurableCorrelationAudit(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName), false)
	if err != nil {
		t.Fatalf("read copied real-data baseline inbound/turn correlation: %v", err)
	}
	if !resume && beforeCounts.inbound != 0 {
		t.Fatalf("copied runtime already contains synthetic inbound rows before experiment: %d", beforeCounts.inbound)
	}
	inheritedInbound, inheritedTurns := dockerRealDataInheritedOperationalRowsWithoutOutbox(beforeState)
	inheritedOutbox, err := dockerRealDataInheritedOperationalOutboxRowsCount(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
	if err != nil {
		t.Fatalf("count copied inherited operational outbox rows: %v", err)
	}
	t.Logf("real-data source audit: inherited operational Teams inbound=%d turns=%d outbox=%d; retained in disposable fixture and excluded from synthetic counters; terminal queued provenance is excluded from inbound count", inheritedInbound, inheritedTurns, inheritedOutbox)
	var resumeWitnessOutboxID string
	var resumeUnknownPostWitness dockerRealDataUnknownPostWitness
	if resume {
		witness := readDockerRealDataUnknownPostWitness(t, statePath)
		resumeUnknownPostWitness = witness
		resumeWitnessOutboxID = witness.OutboxID
		ambiguous, found, witnessErr := dockerRealDataOutboxMessageByIDReadOnly(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName), witness.OutboxID)
		if witnessErr != nil {
			t.Fatalf("read real-data resume outbox witness: %v", witnessErr)
		}
		if !found {
			t.Fatalf("real-data resume lost the durable outbox selected by the previous unknown POST: witness=%#v", witness)
		}
		if strings.TrimSpace(ambiguous.TeamsChatID) != witness.ChatID || dockerRealDataOutboxBodyHash(ambiguous.Body) != witness.BodyHash || !dockerRealDataUnknownPostDispositionSafe(ambiguous) {
			t.Fatalf("real-data resume changed the durable unknown-POST witness: witness=%#v row=%#v", witness, ambiguous)
		}
		t.Logf("real-data resume audit: retained ambiguous outbox=%q for chat=%q; the second process must not POST it again", ambiguous.ID, ambiguous.TeamsChatID)
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
	laggingManifest := dockerRealDataQueuedWorkChatManifest(beforeState, beforeState.ControlChat.TeamsChatID)
	t.Logf("real-data lagging work-chat manifest: queued_work_chats=%d active_session_chats=%d orphan_chats=%d orphan_ids=%v actionable_orphan_chats=%d actionable_orphan_ids=%v terminal_queued_provenance_chats=%d terminal_ids=%v", len(laggingManifest.AllChatIDs), len(laggingManifest.ActiveChatIDs), len(laggingManifest.OrphanChatIDs), laggingManifest.OrphanChatIDs, len(laggingManifest.ActionableOrphanChatIDs), laggingManifest.ActionableOrphanChatIDs, len(laggingManifest.TerminalQueuedChatIDs), laggingManifest.TerminalQueuedChatIDs)
	if requireAllLagging && len(laggingManifest.ActionableOrphanChatIDs) != 0 {
		t.Fatalf("exhaustive all-lagging proof found actionable queued work chats without an active durable session: orphan_chats=%v; refusing to call the active-session subset complete", laggingManifest.ActionableOrphanChatIDs)
	}
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
		if requireAllLagging {
			corpusChats := make(map[string]struct{}, len(replayCorpus))
			for chatID := range replayCorpus {
				corpusChats[strings.TrimSpace(chatID)] = struct{}{}
			}
			missingActionableChats := make([]string, 0)
			terminalEmptyOnlyChats := make([]string, 0)
			for _, chatID := range laggingManifest.ActiveChatIDs {
				if _, found := corpusChats[strings.TrimSpace(chatID)]; !found {
					if dockerRealDataChatOnlyTerminalEmptyQueuedRows(beforeState, chatID) {
						terminalEmptyOnlyChats = append(terminalEmptyOnlyChats, chatID)
					} else {
						missingActionableChats = append(missingActionableChats, chatID)
					}
				}
			}
			if len(terminalEmptyOnlyChats) != 0 {
				t.Logf("real-data active chats with only empty terminal provenance excluded from ordinary replay: chats=%v", terminalEmptyOnlyChats)
			}
			if len(missingActionableChats) != 0 {
				t.Fatalf("exhaustive all-lagging proof found active queued chats with no ordinary replay corpus: chats=%v report=%+v", missingActionableChats, replayReport)
			}
		}
		if chatCoverage {
			sourceChats, sourceMessages := len(replayCorpus), replayCount
			replayCorpus = dockerRealDataOneMessagePerChat(replayCorpus)
			replayCount = 0
			for _, messages := range replayCorpus {
				replayCount += len(messages)
			}
			t.Logf("real-data all-chat coverage corpus: source chats=%d source queued messages=%d selected chats=%d representative messages=%d", sourceChats, sourceMessages, len(replayCorpus), replayCount)
		}
		writeDockerRealDataReplayCorpus(t, statePath, replayCorpus)
	}
	if replayCount < dockerRealDataMinimumReplay {
		t.Fatalf("copied real-data snapshot produced only %d ordinary queued Teams messages for replay; refusing a smoke-sized workload", replayCount)
	}
	t.Logf("real-data replay corpus: queued Teams payloads=%d chats=%d; bodies/authors/order copied from durable rows, only IDs/timestamps remapped", replayCount, len(replayCorpus))
	scheduleNow := time.Now()
	replayChats, emptyChats := prepareDockerRealDataPollSchedules(t, store, beforeState, replayCorpus, beforeState.ControlChat.TeamsChatID, scheduleNow)
	// The copied production snapshot can contain hundreds of active chats whose
	// durable queue has no ordinary replay row.  Leaving all of them due lets the
	// ordinary admission lane consume its bounded quantum before a real replay
	// chat is ever handed to Graph, which turns this acceptance test into a
	// scheduler-shape false negative.  Keep every copied frontier, cursor,
	// failure, and ownership field intact; only the disposable due-time ordering
	// is changed so the complete real queued corpus is observed first.
	prioritizeDockerRealDataReplaySchedules(t, store, beforeState, replayCorpus, beforeState.ControlChat.TeamsChatID, scheduleNow)
	paginationWitness := prioritizeDockerRealDataPaginationWitness(t, store, beforeState, replayCorpus, beforeState.ControlChat.TeamsChatID, scheduleNow)
	t.Logf("real-data disposable schedule: replay chats prioritized=%d active chats with empty fake corpus deferred=%d; only copied due-time ordering changed; source schedule/database is untouched", replayChats, emptyChats)
	if paginationWitness != "" {
		t.Logf("real-data disposable pagination witness: chat=%q has %d replay messages and was moved to the front of due ordering", paginationWitness, len(replayCorpus[paginationWitness]))
	}
	// Validate the exact durable admission boundary before starting the listener.
	// A schedule update that is visible through ChatPoll but absent from the hot
	// candidate query would otherwise look like a Graph or token failure while
	// producing zero requests.
	admissionNow := time.Now()
	admitted, admissionHandled, admissionErr := store.HotPollWorkCandidatesExcludingIdleAt(ctx, beforeState.ControlChat.TeamsChatID, admissionNow.Add(-inboundPollParkAfter), admissionNow)
	if admissionErr != nil {
		t.Fatalf("inspect real-data durable poll admission: %v", admissionErr)
	}
	t.Logf("real-data durable poll admission: handled=%t candidates=%d replay_chats=%d", admissionHandled, len(admitted), func() int {
		count := 0
		for _, session := range admitted {
			if len(replayCorpus[strings.TrimSpace(session.TeamsChatID)]) > 0 {
				count++
			}
		}
		return count
	}())
	if paginationWitness != "" {
		witnessAdmitted := false
		for _, session := range admitted {
			if strings.TrimSpace(session.TeamsChatID) == paginationWitness {
				witnessAdmitted = true
				break
			}
		}
		witnessPoll, witnessFound, witnessPollErr := store.ChatPoll(ctx, paginationWitness)
		operationalHint := witnessPoll.RecoveryRequired || witnessPoll.Attempt != nil || witnessPoll.PendingPage != nil || witnessPoll.Gap != nil || strings.TrimSpace(witnessPoll.ContinuationPath) != "" || strings.TrimSpace(witnessPoll.DeferredContinuationPath) != ""
		t.Logf("real-data pagination witness admission: chat=%q admitted=%t poll_found=%t poll_error=%v updated_at=%s next_poll_at=%s last_activity_at=%s operational_hint=%t pending=%t attempt=%t gap=%t continuation=%q", paginationWitness, witnessAdmitted, witnessFound, witnessPollErr, witnessPoll.UpdatedAt.Format(time.RFC3339Nano), witnessPoll.NextPollAt.Format(time.RFC3339Nano), witnessPoll.LastActivityAt.Format(time.RFC3339Nano), operationalHint, witnessPoll.PendingPage != nil, witnessPoll.Attempt != nil, witnessPoll.Gap != nil, strings.TrimSpace(witnessPoll.ContinuationPath))
	}
	if !admissionHandled || len(admitted) == 0 {
		t.Fatalf("real-data durable poll admission returned no candidates after waking %d replay chats", replayChats)
	}

	const graphToken = "docker-real-data-deterministic-token"
	graphServerState := newDockerRealDataGraphServer(graphToken, user, replayCorpus)
	graphServerState.controlChatID = beforeState.ControlChat.TeamsChatID
	if resume {
		graphServerState.setDurableUnknownPostWitness(resumeUnknownPostWitness)
	}
	// Arm the unknown-result fault only for the synthetic final body emitted by
	// the disposable executor. This prevents an old/control outbox row copied
	// from the source snapshot from consuming the fault and making the experiment
	// pass without exercising the target replay turn.
	// Match only the first synthetic executor result by exact rendered body.
	// A substring match could let an inherited/terminal outbox row consume the
	// fault before the replay lane reaches its own unknown POST boundary.
	// The high-intensity 429 mode isolates inbound poll liveness. Keep the
	// ambiguous-POST scenario in the ordinary real-data experiment, where it is
	// measured independently; combining both faults would make a short 429
	// recovery run depend on outbox recovery and hide which lane stalled.
	unknownFaultEnabled := !resume && mode != dockerRealDataModeComplete && !rateLimitExperiment
	if !unknownFaultEnabled {
		// A process restart must not replay the first process's unknown POST.  A
		// non-empty impossible marker is used because an empty marker means
		// "match every POST" in the deterministic Graph server. Complete mode
		// uses a separate drain gate, so it does not intentionally leave an
		// ambiguous final outbox behind.
		graphServerState.setUnknownPostMarker("__docker_real_data_unknown_post_disabled__")
	} else {
		// The first executor result can be held behind an inherited per-chat
		// FIFO, so result #1 is not guaranteed to be POSTed in the measured
		// window. The ordinary real-data run needs one deterministic unknown
		// message-POST witness, not a particular result number. Match any
		// synthetic executor-result body so inherited operational outbox rows
		// can be sent normally and cannot consume the fault before the replay
		// lane reaches its own unknown POST boundary. The production outbox must
		// preserve that one ambiguous row and never replay it.
		graphServerState.setUnknownPostMarker(dockerRealDataAnyExecutionMarker)
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
	expiredProviderChats := dockerRealDataExpiredProviderContinuationChats(beforeState, beforeState.ControlChat.TeamsChatID)
	expiredProviderTokens := dockerRealDataExpiredProviderTokens(graphServerState, beforeState)
	expiredOutboxProviderTokens, expiredOutboxErr := dockerRealDataExpiredProviderTokensFromSQLite(ctx, graphServerState, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
	if expiredOutboxErr != nil {
		t.Fatalf("read copied outbox provider recovery cursors: %v", expiredOutboxErr)
	}
	expiredProviderTokens += expiredOutboxProviderTokens
	t.Logf("real-data persisted provider continuations marked expired in isolated Graph: chat_poll=%d outbox_recovery=%d total=%d; executable active work chats with opaque next requests=%d; arbitrary opaque tokens remain invalid", expiredProviderTokens-expiredOutboxProviderTokens, expiredOutboxProviderTokens, expiredProviderTokens, len(expiredProviderChats))
	if len(expiredProviderChats) > 0 {
		// The copied production schedule can contain many due chats and the
		// listener intentionally admits only a bounded quantum. Move one actual
		// expired-continuation chat to the front of the disposable due order so a
		// short, realistic run proves the recovery response is handled by the
		// production poller. This changes no frontier, cursor, receipt, or source
		// identity; it is the same due-time-only adjustment used for replay chats.
		recoveryChat := expiredProviderChats[0]
		recoveryPoll := beforeState.ChatPolls[recoveryChat]
		pollState := strings.TrimSpace(recoveryPoll.PollState)
		if pollState == "" {
			pollState = inboundPollStateWarm
		}
		if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
			ChatID:            recoveryChat,
			PollState:         pollState,
			NextPollAt:        scheduleNow.Add(-2 * time.Minute),
			LastActivityAt:    scheduleNow.Add(-time.Minute),
			ClearBlockedUntil: true,
			ResetFailures:     true,
		}); err != nil {
			t.Fatalf("wake copied expired-continuation chat %q: %v", recoveryChat, err)
		}
		t.Logf("real-data expired-continuation witness: chat=%q moved to the front of disposable due ordering", recoveryChat)
	}
	// Bind the retryable fault to the first replay chat that the production
	// scheduler actually selects. Picking an arbitrary corpus entry can leave
	// the fault unobserved when the real schedule does not reach that chat in
	// the measured window.
	var docker429PollSessions []Session
	var docker429ThrottledSessions []Session
	var docker429RateLimitedChats []string
	if rateLimitExperiment {
		var selectionErr error
		docker429PollSessions, selectionErr = dockerRealData429PollSessions(registry, beforeState, replayCorpus, beforeState.ControlChat.TeamsChatID, dockerRealData429Chats, dockerRealData429HealthyChats)
		if selectionErr != nil {
			t.Fatalf("select real-data 429 poll sessions: %v", selectionErr)
		}
		docker429ThrottledSessions = docker429PollSessions[:dockerRealData429Chats]
		if rateLimitAccountWide {
			// An account-level throttle affects every selected chat. Keep both
			// sessions in the throttled set so every round must observe the same
			// tenant-wide 429 before the final recovery round.
			docker429ThrottledSessions = docker429PollSessions
		}
		for _, session := range docker429ThrottledSessions {
			docker429RateLimitedChats = append(docker429RateLimitedChats, session.ChatID)
		}
		// The high-intensity path invokes these exact copied real chats, so the
		// fault cannot silently bind to an unrelated schedule row. Account scope
		// uses one shared request budget; chat scope keeps independent budgets.
		if rateLimitAccountWide {
			// The durable account gate should collapse each concurrent wave to
			// one provider request. A budget proportional to the number of chats
			// would let a broken gate consume duplicate 429s and still appear to
			// recover, so keep the oracle's exact one-per-round budget here.
			graphServerState.setPersistentGlobalList429WithScope(dockerRealData429Failures, rateLimitScope)
		} else {
			graphServerState.setPersistentList429(docker429RateLimitedChats, dockerRealData429Chats, dockerRealData429Failures)
		}
	} else {
		graphServerState.setFaultChat("")
	}
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
	executorHoldFirst := 3 * time.Second
	if rateLimitExperiment {
		// The 429 experiment measures poll recovery, not executor latency. A
		// synchronous no-network executor keeps each successful real message at
		// the same durable inbound -> turn -> completion boundary without adding a
		// teardown-sized delay to the four retry rounds.
		executorHoldFirst = 0
	}
	executor := &dockerRealDataExecutor{holdFirst: executorHoldFirst}
	traceWriter := &dockerRealDataTraceWriter{}
	// Store timing is opt-in and observation-only. Keep the real-data fixture's
	// lock/hold tail visible so an Amdahl diagnosis can distinguish JSON/SQLite
	// work from contention without changing the production lock contract.
	store.SetTimingObserver(traceWriter.recordStoreTiming)
	t.Cleanup(func() { store.SetTimingObserver(nil) })
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
	var measuredElapsed time.Duration
	var lastListenerErr error
	phaseErrorsAtMeasureStart := make(map[string]uint64)
	phaseErrorsAtStop := make(map[string]uint64)
	phaseDeadlinesAtMeasureStart := make(map[string]uint64)
	phaseDeadlinesAtStop := make(map[string]uint64)
	startupPhaseErrors := make(map[string]uint64)
	startupPhaseDeadlines := make(map[string]uint64)
	var gracefulStopAt []time.Time
	var runStartedAt []time.Time
	var syntheticTurnsBeforeRun []map[string]dockerRealDataSyntheticTurnObservation
	var correlationBeforeRun []dockerRealDataCorrelationAudit
	phaseNames := dockerRealDataProductionPhaseNames()
	runListener := func(activeStore *teamstore.Store) error {
		thisRunStartedAt := time.Now()
		turnSnapshot, snapshotErr := dockerRealDataSyntheticTurnSnapshot(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
		if snapshotErr != nil {
			return fmt.Errorf("read synthetic turn boundary before listener run: %w", snapshotErr)
		}
		correlation, correlationErr := dockerRealDataDurableCorrelationAudit(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName), false)
		if correlationErr != nil {
			return fmt.Errorf("read synthetic correlation before listener run: %w", correlationErr)
		}
		runStartedAt = append(runStartedAt, thisRunStartedAt)
		syntheticTurnsBeforeRun = append(syntheticTurnsBeforeRun, turnSnapshot)
		correlationBeforeRun = append(correlationBeforeRun, correlation)
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
		bridge.controlLeaseClaimHook = func(decision teamstore.ControlLeaseDecision, err error) {
			traceWriter.recordLeaseClaim(decision, err)
			traceWriter.recordLeaseClaimWitness(activeStore)
		}
		bridge.ownerFailureHook = traceWriter.recordOwnerFailure
		bridge.pollChatTraceHook = traceWriter.recordPollChat
		bridge.mainLoopPhaseTraceHook = func(name string, duration time.Duration, err error) {
			traceWriter.recordTiming("phase."+strings.TrimSpace(name), duration, err)
		}
		bridge.pollDecisionTraceHook = func(stage string, decisions []inboundPollDecision) {
			traceWriter.recordPollSelection(stage, graphServerState.faultChatSnapshot(), decisions)
		}
		bridge.outboxPhaseTraceHook = func(name string, duration time.Duration, err error) {
			traceWriter.recordTiming("outbox.phase."+strings.TrimSpace(name), duration, err)
			_, _ = fmt.Fprintf(traceWriter, "Teams outbox step name=%s duration=%s err=%v\n", name, duration, err)
		}
		bridge.outboxSendTraceHook = func(outboxID, stage string, duration time.Duration, err error) {
			// Keep the realistic fixture output bounded and independent from the
			// general listener log cap. Sub-10ms stages are not useful for the
			// Amdahl diagnosis, while every error and material long-tail stage
			// remains visible with its durable row identity.
			traceWriter.recordOutboxSendStage(outboxID, stage, duration, err)
		}
		bridge.pollPhaseTraceHook = func(name string, duration time.Duration, err error) {
			traceWriter.recordTiming("poll.phase."+strings.TrimSpace(name), duration, err)
			_, _ = fmt.Fprintf(traceWriter, "Teams poll step name=%s duration=%s err=%v\n", name, duration, err)
		}
		bridge.queuedTurnTraceHook = traceWriter.recordQueuedTurn
		bridge.pollMessageTraceHook = traceWriter.recordPollMessage
		bridge.historyWatchPathHook = func(hookCtx context.Context, path string) error {
			started := time.Now()
			err := maintenanceTrace.observeHistory(hookCtx, path)
			traceWriter.recordTiming("diagnostic.history-observer", time.Since(started), err)
			return err
		}
		bridge.linkedTranscriptSessionHook = func(hookCtx context.Context, session Session) error {
			started := time.Now()
			err := maintenanceTrace.observeLinked(hookCtx, session)
			traceWriter.recordTiming("diagnostic.linked-observer", time.Since(started), err)
			return err
		}
		bridge.historyWatchJobTraceHook = func(path string, duration time.Duration, err error) {
			traceWriter.recordTiming("history.job", duration, err)
		}
		bridge.linkedTranscriptJobTraceHook = func(sessionID string, duration time.Duration, err error) {
			traceWriter.recordTiming("linked.job", duration, err)
		}
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
			startupDeadline := time.NewTimer(startupDeadlineDuration)
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
				t.Logf("real-data listener returned before its first main-loop cycle: err=%v phases=%#v lease_claims=%v owner_failures=%v lines=%v store_timings=%v", listenErr, bridge.mainLoopPhaseStatsSnapshot("poll"), traceWriter.leaseClaimsSnapshot(), traceWriter.ownerFailuresSnapshot(), traceWriter.linesSnapshot(), traceWriter.storeTimingsSnapshot())
				return
			case <-startupDeadline.C:
				startupTimedOut.Store(true)
				startupFailure = errors.New("first main-loop cycle did not complete before the startup deadline")
				t.Logf("real-data listener did not complete its first main-loop cycle before the startup deadline: phases=%#v lease_claims=%v owner_failures=%v lines=%v store_timings=%v", bridge.mainLoopPhaseStatsSnapshot("poll"), traceWriter.leaseClaimsSnapshot(), traceWriter.ownerFailuresSnapshot(), traceWriter.linesSnapshot(), traceWriter.storeTimingsSnapshot())
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
			measuredWindow.beforeTurns, measuredWindow.beforeTurnsReadError = dockerRealDataSyntheticTurnSnapshot(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
			if measuredWindow.beforeTurnsReadError != nil {
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
			measuredWindow.startedAt = time.Now()
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-timer.C:
				sampleEndedAt := time.Now()
				measuredWindow.endedAt = sampleEndedAt
				measuredWindow.executorAfter = executor.runs.Load()
				measuredWindow.after, measuredWindow.afterCountsReadError = dockerRealDataCountsFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
				measuredWindow.afterTurns, measuredWindow.afterTurnsReadError = dockerRealDataSyntheticTurnSnapshot(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
				measuredWindow.completedInWindow = dockerRealDataCompletedTransitionsBetween(measuredWindow.beforeTurns, measuredWindow.afterTurns, measuredWindow.startedAt, measuredWindow.endedAt)
				measuredWindow.completed = measuredWindow.afterCountsReadError == nil && measuredWindow.afterTurnsReadError == nil
				measuredWindow.stopRequestedAt = sampleEndedAt
				measuredWindowCompleted.Store(true)
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
			// Async reservation becoming idle is an in-process lifecycle signal, not
			// proof that the final durable Running -> terminal transaction has
			// committed.  A worker can release its reservation immediately after a
			// provider result while its owner-fenced completion is still waiting on
			// SQLite.  Observe the synthetic rows until that boundary closes before
			// canceling the owner context; otherwise the harness manufactures a
			// Running row at every graceful stop and reports a false failure.
			durableDeadline := time.Now().Add(30 * time.Second)
			for {
				counts, countErr := dockerRealDataCountsFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
				if countErr == nil && counts.running == 0 {
					break
				}
				if !time.Now().Before(durableDeadline) {
					t.Logf("real-data listener still has a synthetic durable turn at graceful-stop deadline: counts=%#v err=%v", counts, countErr)
					break
				}
				time.Sleep(10 * time.Millisecond)
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
				Interval:                   pollInterval,
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
			measuredElapsed += dockerRealDataMeasuredWindowDuration(measuredWindow, duration)
			if measuredWindow.beforeCountsReadError != nil {
				return fmt.Errorf("read measured Docker window baseline counters: %w", measuredWindow.beforeCountsReadError)
			}
			if measuredWindow.afterCountsReadError != nil {
				return fmt.Errorf("read measured Docker window final counters: %w", measuredWindow.afterCountsReadError)
			}
			if measuredWindow.beforeTurnsReadError != nil {
				return fmt.Errorf("read measured Docker window synthetic-turn baseline: %w", measuredWindow.beforeTurnsReadError)
			}
			if measuredWindow.afterTurnsReadError != nil {
				return fmt.Errorf("read measured Docker window synthetic-turn final snapshot: %w", measuredWindow.afterTurnsReadError)
			}
			measuredInbound += measuredWindow.after.inbound - measuredWindow.before.inbound
			measuredCompleted += measuredWindow.completedInWindow
			measuredExecutorRuns += measuredWindow.executorAfter - measuredWindow.executorBefore
			measuredWindowInbound = append(measuredWindowInbound, measuredWindow.after.inbound-measuredWindow.before.inbound)
			measuredWindowCompletedDeltas = append(measuredWindowCompletedDeltas, measuredWindow.completedInWindow)
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

	// A full production listener cycle also runs inherited outbox, history, and
	// linked-transcript work. On a copied multi-gigabyte state those phases can
	// legitimately consume the entire phase budget before a second poll wave is
	// reached. That is useful diagnostic information, but it is an inefficient
	// way to answer the narrower 429 question. The explicitly requested
	// poll-only mode keeps the same copied SQLite/registry/Codex corpus, fake
	// Graph boundary, owner lease, per-chat poll frontier, and real message
	// handler, then drives only the selected poll quantum directly. No schedule
	// clearing or failure reset is performed between rounds; the loop waits for
	// each durable Retry-After gate. The default Docker 429 experiment uses the
	// full Bridge.Listen path above.
	run429PollOnly := func(activeStore *teamstore.Store) error {
		if len(docker429PollSessions) != dockerRealData429Chats+dockerRealData429HealthyChats {
			return fmt.Errorf("429 poll-only selection has %d sessions; want %d", len(docker429PollSessions), dockerRealData429Chats+dockerRealData429HealthyChats)
		}
		runRegistry, err := LoadRegistry(registryPath)
		if err != nil {
			return fmt.Errorf("reload copied registry projection for 429 poll-only run: %w", err)
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
			leaseDuration:            5 * time.Minute,
			out:                      traceWriter,
			executor:                 executor,
			controlFallbackExecutor:  executor,
			store:                    activeStore,
			markAnswerChatsUnread:    true,
			groupChatGuardEnabled:    true,
			maxWorkChatPollsPerCycle: len(docker429PollSessions),
			pollWorkerBudget:         mainLoopPollWorkerBudget,
		}
		bridge.controlLeaseClaimHook = func(decision teamstore.ControlLeaseDecision, err error) {
			traceWriter.recordLeaseClaim(decision, err)
			traceWriter.recordLeaseClaimWitness(activeStore)
		}
		bridge.ownerFailureHook = traceWriter.recordOwnerFailure
		bridge.pollChatTraceHook = traceWriter.recordPollChat
		bridge.pollDecisionTraceHook = func(stage string, decisions []inboundPollDecision) {
			traceWriter.recordPollSelection(stage, graphServerState.faultChatSnapshot(), decisions)
		}
		bridge.queuedTurnTraceHook = traceWriter.recordQueuedTurn
		bridge.pollMessageTraceHook = traceWriter.recordPollMessage
		active, err := bridge.claimControlLease(ctx)
		if err != nil {
			return fmt.Errorf("claim disposable owner lease for 429 poll-only run: %w", err)
		}
		if !active {
			return fmt.Errorf("429 poll-only run entered standby instead of acquiring the disposable owner lease")
		}
		leaseMachineID := bridge.machine.ID
		leaseGeneration := bridge.currentLeaseGeneration()
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = activeStore.ReleaseControlLeaseIfHolder(cleanupCtx, leaseMachineID, leaseGeneration)
		}()
		ownerTrace := startDockerRealDataOwnerTrace(bridge)
		defer ownerTrace.stopTrace()

		var pollInboundWriter *globalInboundSQLiteWriter
		if _, ok := globalInboundLedgerPathForRegistry(bridge.registryPath); ok {
			pollInboundWriter = &globalInboundSQLiteWriter{}
			defer func() { _ = pollInboundWriter.close() }()
		}
		measureBefore, err := dockerRealDataCountsFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
		if err != nil {
			return fmt.Errorf("read 429 poll-only baseline counters: %w", err)
		}
		measureExecutorBefore := executor.runs.Load()
		measureStarted := time.Now()
		type pollResult struct {
			chatID string
			err    error
		}
		rateLimitedSet := make(map[string]struct{}, len(docker429RateLimitedChats))
		for _, chatID := range docker429RateLimitedChats {
			rateLimitedSet[chatID] = struct{}{}
		}
		for round := 0; round <= dockerRealData429Failures; round++ {
			roundTimeout := 30 * time.Second
			if round == 0 || round == dockerRealData429Failures {
				// These rounds execute one real copied message through the normal
				// durable handler. The intermediate 429 rounds contain no handler
				// work and retain the short watchdog.
				roundTimeout = 2 * time.Minute
			}
			roundCtx, roundCancel := context.WithTimeout(ctx, roundTimeout)
			if pollInboundWriter != nil {
				roundCtx = context.WithValue(roundCtx, pollInboundLedgerWriterContextKey{}, pollInboundWriter)
			}
			requestSnapshotBeforeRound := graphServerState.graphRequestsSnapshot()
			results := make(chan pollResult, len(docker429PollSessions))
			var workers sync.WaitGroup
			roundSessions := docker429ThrottledSessions
			if round == 0 || rateLimitAccountWide {
				// Exercise sibling availability once while the selected real chat is
				// rate-limited. In account scope every selected chat is throttled on
				// every pre-recovery round; in chat scope repeating the healthy handler
				// would only add unrelated full-state queue preparation cost.
				roundSessions = docker429PollSessions
			}
			for _, selected := range roundSessions {
				selected := selected
				workers.Add(1)
				go func() {
					defer workers.Done()
					poll, hasPoll, pollErr := activeStore.ChatPoll(roundCtx, selected.ChatID)
					if pollErr == nil {
						_, pollErr = bridge.pollChatWithRoleStateOptions(roundCtx, selected.ChatID, dockerRealDataDefaultTop, inboundPollRoleWork, false, poll, hasPoll, pollChatWithRoleOptions{
							AllowBacklogDrain:        true,
							MaxBacklogActions:        1,
							RecoverStaleContinuation: true,
							GraphBudget:              bridge.pollWorkerBudget,
						}, func(handleCtx context.Context, msg ChatMessage, text string) error {
							return bridge.handleResolvedSessionMessageWithQueueState(handleCtx, &selected, selected.ChatID, msg, text, nil, nil)
						})
					}
					results <- pollResult{chatID: selected.ChatID, err: pollErr}
				}()
			}
			workers.Wait()
			close(results)
			var roundErrors []string
			sawAccountRateLimit := false
			for result := range results {
				_, rateLimited := rateLimitedSet[result.chatID]
				if round < dockerRealData429Failures && rateLimitAccountWide {
					// A durable account gate intentionally suppresses sibling
					// requests after the first observed 429 in this wave.  Only
					// the first provider failure is required; nil means this
					// worker observed the already-persisted shared gate.
					if result.err != nil && isGraphRateLimitError(result.err) {
						sawAccountRateLimit = true
					} else if result.err != nil {
						roundErrors = append(roundErrors, fmt.Sprintf("%s: %v", result.chatID, result.err))
					}
					continue
				}
				if round < dockerRealData429Failures && rateLimited {
					if result.err == nil || !isGraphRateLimitError(result.err) {
						roundErrors = append(roundErrors, fmt.Sprintf("%s: %v", result.chatID, result.err))
					}
				} else if result.err != nil {
					roundErrors = append(roundErrors, fmt.Sprintf("%s: %v", result.chatID, result.err))
				}
			}
			roundCancel()
			if round < dockerRealData429Failures && rateLimitAccountWide && !sawAccountRateLimit {
				return fmt.Errorf("429 poll-only round %d did not observe any account-scoped provider 429", round+1)
			}
			if len(roundErrors) > 0 {
				return fmt.Errorf("429 poll-only round %d errors: %s", round+1, strings.Join(roundErrors, "; "))
			}
			if round == dockerRealData429Failures {
				break
			}
			var retryAt time.Time
			if rateLimitAccountWide {
				limit, found, gateErr := activeStore.ChatRateLimit(ctx, graphReadAccountRateLimitKey)
				if gateErr != nil {
					return fmt.Errorf("read durable account 429 gate after round %d: %w", round+1, gateErr)
				}
				if !found || !limit.BlockedUntil.After(time.Now()) {
					return fmt.Errorf("account 429 gate was not durable after round %d: %#v", round+1, limit)
				}
				retryAt = limit.BlockedUntil
				// The fake Graph records both request start and completion. Once the
				// first account/global 429 response has completed, a healthy sibling
				// must not start another list GET before the durable shared gate
				// expires. Requests already in flight before that response are a
				// legitimate concurrency race; requests started afterwards prove that
				// the read gate was not consulted or was scoped per chat.
				requestSnapshotAfterRound := graphServerState.graphRequestsSnapshot()
				first429CompletedAt := time.Time{}
				for index := len(requestSnapshotBeforeRound); index < len(requestSnapshotAfterRound); index++ {
					record := requestSnapshotAfterRound[index]
					if record.Operation == dockerRealDataGraphOpMessageList &&
						record.StatusCode == http.StatusTooManyRequests &&
						dockerRealData429ScopeIsAccountWide(record.RateLimitScope) {
						first429CompletedAt = record.CompletedAt
						break
					}
				}
				if first429CompletedAt.IsZero() {
					return fmt.Errorf("account 429 round %d lacks a timestamped provider witness: requests=%#v", round+1, requestSnapshotAfterRound)
				}
				for index := len(requestSnapshotBeforeRound); index < len(requestSnapshotAfterRound); index++ {
					record := requestSnapshotAfterRound[index]
					if record.Operation != dockerRealDataGraphOpMessageList ||
						record.StartedAt.Before(first429CompletedAt) ||
						!record.StartedAt.Before(retryAt) {
						continue
					}
					return fmt.Errorf("account 429 round %d started a sibling list GET after the shared 429 and before gate expiry: record=%#v gate=%s", round+1, record, retryAt)
				}
			} else {
				for _, selected := range docker429ThrottledSessions {
					poll, found, pollErr := activeStore.ChatPoll(ctx, selected.ChatID)
					if pollErr != nil {
						return fmt.Errorf("read durable 429 gate for %s after round %d: %w", selected.ChatID, round+1, pollErr)
					}
					if !found || poll.LastErrorAt.IsZero() || !poll.NextPollAt.After(poll.LastErrorAt) {
						return fmt.Errorf("chat %s did not persist a Retry-After gate after round %d: %#v", selected.ChatID, round+1, poll)
					}
					if poll.NextPollAt.After(retryAt) {
						retryAt = poll.NextPollAt
					}
				}
			}
			wait := time.Until(retryAt) + 20*time.Millisecond
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return ctx.Err()
				}
			}
		}
		measureEnded := time.Now()
		measureExecutorAfter := executor.runs.Load()
		measureAfter, err := dockerRealDataCountsFromSQLite(ctx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
		if err != nil {
			return fmt.Errorf("read 429 poll-only final counters: %w", err)
		}
		measuredWindows++
		measuredElapsed += measureEnded.Sub(measureStarted)
		measuredInbound += measureAfter.inbound - measureBefore.inbound
		measuredCompleted += measureAfter.completed - measureBefore.completed
		measuredExecutorRuns += measureExecutorAfter - measureExecutorBefore
		measuredWindowInbound = append(measuredWindowInbound, measureAfter.inbound-measureBefore.inbound)
		measuredWindowCompletedDeltas = append(measuredWindowCompletedDeltas, measureAfter.completed-measureBefore.completed)
		ownerTrace.stopTrace()
		generations, changes := ownerTrace.snapshot()
		ownerGenerations = append(ownerGenerations, generations...)
		ownerChangesByRun = append(ownerChangesByRun, changes)
		ownerChanges += changes
		bridges = append(bridges, bridge)
		t.Logf("real-data 429 poll-only run: chats=%v rate_limited=%v rounds=%d elapsed=%s inbound_delta=%d completed_delta=%d graph_429=%d", func() []string {
			ids := make([]string, 0, len(docker429PollSessions))
			for _, session := range docker429PollSessions {
				ids = append(ids, session.ChatID)
			}
			return ids
		}(), docker429RateLimitedChats, dockerRealData429Failures+1, time.Since(measureStarted), measureAfter.inbound-measureBefore.inbound, measureAfter.completed-measureBefore.completed, graphServerState.status429.Load())
		return nil
	}

	if rateLimitPollOnly {
		if err := run429PollOnly(store); err != nil {
			t.Fatal(err)
		}
	} else if err := runListener(store); err != nil {
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
	postStateWitnessOutboxID := graphServerState.unknownPostOutboxIDSnapshot()
	if resume {
		postStateWitnessOutboxID = resumeWitnessOutboxID
	}
	afterState, err := dockerRealDataPostStateFromSQLite(postCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName), replayCorpus, postStateWitnessOutboxID)
	if err != nil {
		t.Fatalf("read copied real-data synthetic state after listener experiment: %v", err)
	}
	afterHistoryState, err := store.HistoryWatchState(postCtx)
	if err != nil {
		t.Fatalf("read copied real-data history state after listener experiment: %v", err)
	}
	afterState.HistoryWatch = afterHistoryState.HistoryWatch
	afterState.HistoryWatchReady = afterHistoryState.HistoryWatchReady
	residuals := dockerRealDataSyntheticResiduals(afterState, replayCorpus)
	if chatCoverage {
		beforeRecoveryChats := dockerRealDataPollRecoveryChats(beforeState, replayCorpus)
		afterRecoveryChats := dockerRealDataPollRecoveryChats(afterState, replayCorpus)
		newRecoveryChats := dockerRealDataNewPollRecoveryChats(beforeState, afterState, replayCorpus)
		baselineRecoveryBoundaries := 0
		for chatID := range replayCorpus {
			if poll, found := beforeState.ChatPolls[strings.TrimSpace(chatID)]; found && dockerRealDataPollHasRecoveryBoundary(poll) {
				baselineRecoveryBoundaries++
			}
		}
		// The copied source may already contain a bounded recovery lane. Do not
		// call that inherited source condition synthetic unfinished work, but do
		// fail if this run creates recovery for a previously clean representative
		// chat.
		residuals.PollRecovery = len(newRecoveryChats)
		t.Logf("real-data chat-coverage poll recovery audit: active_before=%d active_after=%d baseline_recovery_boundaries=%d new_clean_chat_recovery=%d new_chats=%v", len(beforeRecoveryChats), len(afterRecoveryChats), baselineRecoveryBoundaries, len(newRecoveryChats), newRecoveryChats)
	}
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
	steadyElapsed := measuredElapsed
	if steadyElapsed <= 0 {
		steadyElapsed = duration * time.Duration(measuredWindows)
	}
	if steadyElapsed <= 0 {
		steadyElapsed = runElapsed
	}
	unknown := graphServerState.unknownPaths()
	servedMessages := graphServerState.servedMessages()
	servedMessageRequests := graphServerState.servedMessageRequests()
	chatDrainAudit := auditDockerRealDataChatDrain(afterState, replayCorpus, servedMessages)
	t.Logf("real-data per-chat drain audit: expected_chats=%d expected_messages=%d served_messages=%d inbound_messages=%d completed_messages=%d mismatches=%v", chatDrainAudit.ExpectedChats, chatDrainAudit.ExpectedMessages, chatDrainAudit.ServedMessages, chatDrainAudit.InboundMessages, chatDrainAudit.CompletedMessages, chatDrainAudit.Mismatches)
	if (mode == dockerRealDataModeComplete || requireAllLagging) && len(chatDrainAudit.Mismatches) != 0 {
		t.Fatalf("complete real-data drain did not close every replay message in its source chat: audit=%#v", chatDrainAudit)
	}
	persistedSyntheticIDs, err := dockerRealDataSyntheticInboundIDs(postCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
	if err != nil {
		t.Fatalf("read Graph-served synthetic inbound IDs: %v", err)
	}
	protectedPollMessageIDs := dockerRealDataProtectedPollMessageIDs(afterState)
	if len(servedMessages) == 0 && (!resume || resumeWorkExpected) {
		phaseStats := make(map[string]mainLoopPhaseStats)
		if len(bridges) > 0 {
			for _, phaseName := range phaseNames {
				phaseStats[phaseName] = bridges[len(bridges)-1].mainLoopPhaseStatsSnapshot(phaseName)
			}
		}
		t.Fatalf("real-data fake Graph served no replay messages; throughput would be an unmeasured scheduler result: list_gets=%d status429=%d invalid_queries=%d unsupported_filters=%d poll_traces=%s queued_turns=%s poll_messages=%s requests=%s phase_stats=%v listener_lines=%s listener_error=%v", graphServerState.listGETs.Load(), graphServerState.status429.Load(), graphServerState.invalidListQueries.Load(), graphServerState.unsupportedFilters.Load(), dockerRealDataTraceSummary(traceWriter.pollChatsSnapshot()), dockerRealDataTraceSummary(traceWriter.queuedTurnsSnapshot()), dockerRealDataTraceSummary(traceWriter.pollMessagesSnapshot()), dockerRealDataRequestSummary(graphServerState.listRequests()), phaseStats, dockerRealDataTraceSummary(traceWriter.linesSnapshot()), lastListenerErr)
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
		unprotected := make(map[string][]string)
		for chatID, messages := range replayCorpus {
			for _, message := range messages {
				messageID := strings.TrimSpace(message.ID)
				if messageID == "" {
					continue
				}
				if _, served := servedMessages[messageID]; !served {
					continue
				}
				if _, found := persistedSyntheticIDs[messageID]; found {
					continue
				}
				if _, protected := protectedPollMessageIDs[messageID]; protected {
					continue
				}
				unprotected[messageID] = []string{chatID}
				unprotected[messageID] = append(unprotected[messageID], servedMessageRequests[messageID]...)
			}
		}
		t.Logf("real-data durability diagnostic: unprotected_served=%v poll_messages=%s queued_turns=%s list_requests=%s", unprotected, dockerRealDataTraceSummary(traceWriter.pollMessagesSnapshot()), dockerRealDataTraceSummary(traceWriter.queuedTurnsSnapshot()), dockerRealDataRequestSummary(graphServerState.listRequests()))
		t.Fatalf("fake Graph served replay messages without durable inbound events (first %d): %v", len(missingDurableInbound), missingDurableInbound)
	}
	backlogSamples, ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog := maintenanceTrace.snapshot()
	ordinaryHistoryPaths, ordinaryLinkedSessions := maintenanceTrace.ordinaryWorkSnapshot()
	ordinaryLinkedSuppressed, ordinaryLinkedUnsuppressed := maintenanceTrace.ordinaryLinkedPolicySnapshot()
	ordinaryUnindexedLinked := maintenanceTrace.ordinaryUnindexedLinkedSnapshot()
	ordinaryUnindexedLinkedIDs := maintenanceTrace.ordinaryUnindexedLinkedIDsSnapshot()
	phaseStats := func(name string) mainLoopPhaseStats {
		var total mainLoopPhaseStats
		for _, bridge := range bridges {
			current := bridge.mainLoopPhaseStatsSnapshot(name)
			total.Runs += current.Runs
			total.DeadlineExceeded += current.DeadlineExceeded
			total.Errors += current.Errors
			total.Deferred += current.Deferred
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
	duplicateServedMessages := 0
	for _, count := range servedMessages {
		if count > 1 {
			duplicateServedMessages += count - 1
		}
	}
	phaseStatsByName := make(map[string]mainLoopPhaseStats, len(phaseNames))
	for _, phaseName := range phaseNames {
		phaseStatsByName[phaseName] = phaseStats(phaseName)
	}
	pollStats := phaseStatsByName["poll"]
	historyWatchStats := phaseStatsByName["history-watch"]
	linkedTranscriptStats := phaseStatsByName["linked-transcript"]
	// Emit this before the intentionally verbose aggregate diagnostic so a
	// command-line log cap cannot hide the per-send bottleneck evidence.
	t.Logf("real-data outbox send stage timings: %s", dockerRealDataTraceSummary(traceWriter.outboxSendStagesSnapshot()))
	t.Logf("real-data Teams experiment: resume=%t startup_load=%s listener_wall=%s measured_windows=%d measured_window=%s startup_ready_after=%s first_execution_after=%s baseline_inbound=%d final_inbound=%d inbound_delta=%d final_completed=%d completed_delta=%d failed=%d queued=%d running=%d interrupted=%d executor_runs=%d measured_inbound=%d measured_completed=%d measured_executor_runs=%d wall_inbound_per_sec=%.3f measured_inbound_per_sec=%.3f measured_completed_per_sec=%.3f measured_executor_per_sec=%.3f graph_list_gets=%d graph_item_gets=%d graph_posts_total=%d graph_message_post_attempts=%d graph_message_post_responses=%d graph_message_post_accepts=%d graph_mark_unread_posts=%d graph_429=%d graph_503=%d unknown_posts=%d repeated_unknown_posts=%d graph_served_unique=%d graph_served_duplicates=%d history_offset_delta=%d poll_runs=%d poll_deadlines=%d poll_errors=%d poll_last_duration=%s poll_last_error=%q history_watch_runs=%d history_watch_deadlines=%d history_watch_errors=%d history_watch_last_duration=%s history_watch_last_error=%q linked_transcript_runs=%d linked_transcript_deadlines=%d linked_transcript_errors=%d linked_transcript_last_duration=%s linked_transcript_last_error=%q phase_stats=%v startup_phase_errors=%v startup_phase_deadlines=%v owner_generations=%v owner_changes=%d owner_changes_by_run=%v backlog_samples=%d ordinary_history_while_backlog=%d ordinary_linked_while_backlog=%d ordinary_linked_suppressed=%d ordinary_linked_unsuppressed=%d ordinary_unindexed_linked=%d unindexed_linked_candidates=%d ordinary_history_paths=%v ordinary_linked_sessions=%v ordinary_unindexed_linked_ids=%v baseline_history_offset_sum=%d final_history_offset_sum=%d graph_list_requests=%s unknown_graph_paths=%v listener_error=%v", resume, loadElapsed, runElapsed, measuredWindows, steadyElapsed, startupReadyElapsed, firstExecutionElapsed, beforeCounts.inbound, afterCounts.inbound, inboundDelta, afterCounts.completed, completedDelta, afterCounts.failed, afterCounts.queued, afterCounts.running, afterCounts.interrupted, runs, measuredInbound, measuredCompleted, measuredExecutorRuns, float64(inboundDelta)/runElapsed.Seconds(), float64(measuredInbound)/steadyElapsed.Seconds(), float64(measuredCompleted)/steadyElapsed.Seconds(), float64(measuredExecutorRuns)/steadyElapsed.Seconds(), graphServerState.listGETs.Load(), graphServerState.itemGETs.Load(), graphServerState.posts.Load(), graphServerState.messagePostAttempts.Load(), graphServerState.messagePostResponses.Load(), graphServerState.messagePostAccepts.Load(), graphServerState.markUnreadPosts.Load(), graphServerState.status429.Load(), graphServerState.status503.Load(), graphServerState.unknownPosts.Load(), graphServerState.unknownPostRepeats.Load(), len(servedMessages), duplicateServedMessages, afterHistoryOffsets-beforeHistoryOffsets, pollStats.Runs, pollStats.DeadlineExceeded, pollStats.Errors, pollStats.LastDuration, pollStats.LastError, historyWatchStats.Runs, historyWatchStats.DeadlineExceeded, historyWatchStats.Errors, historyWatchStats.LastDuration, historyWatchStats.LastError, linkedTranscriptStats.Runs, linkedTranscriptStats.DeadlineExceeded, linkedTranscriptStats.Errors, linkedTranscriptStats.LastDuration, linkedTranscriptStats.LastError, phaseStatsByName, startupPhaseErrors, startupPhaseDeadlines, ownerGenerations, ownerChanges, ownerChangesByRun, backlogSamples, ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog, ordinaryLinkedSuppressed, ordinaryLinkedUnsuppressed, ordinaryUnindexedLinked, len(linkedUnindexed), ordinaryHistoryPaths, ordinaryLinkedSessions, ordinaryUnindexedLinkedIDs, beforeHistoryOffsets, afterHistoryOffsets, dockerRealDataRequestSummary(graphServerState.listRequests()), unknown, lastListenerErr)
	t.Logf("real-data outbox step timings: %s", dockerRealDataTraceSummary(traceWriter.linesContaining("Teams outbox step")))
	t.Logf("real-data outbox send stage timings: %s", dockerRealDataTraceSummary(traceWriter.outboxSendStagesSnapshot()))
	t.Logf("real-data store lock timings: %s", dockerRealDataTraceSummary(traceWriter.storeTimingsSnapshot()))
	t.Logf("real-data poll step timings: %s", dockerRealDataTraceSummary(traceWriter.linesContaining("Teams poll step")))
	t.Logf("real-data per-chat poll timings: %s", dockerRealDataTraceSummary(traceWriter.pollChatsSnapshot()))
	t.Logf("real-data poll selection trace: %s", dockerRealDataTraceSummary(traceWriter.pollSelectionsSnapshot()))
	t.Logf("real-data queued-turn admission trace: %s", dockerRealDataTraceSummary(traceWriter.queuedTurnsSnapshot()))
	t.Logf("real-data poll-message disposition trace: %s", dockerRealDataTraceSummary(traceWriter.pollMessagesSnapshot()))
	t.Logf("real-data timing aggregates phase: %s", traceWriter.timingAggregatesSummaryFor("phase.", "poll.phase.", "outbox.phase.", "outbox.send.", "history.", "linked.", "diagnostic."))
	t.Logf("real-data timing aggregates store: %s", traceWriter.timingAggregatesSummaryFor("store."))

	// Validate provider/query and no-duplicate safety before the throughput gate.
	// A deliberately short diagnostic run may not reach the configured completion
	// count, but it must still fail (or pass) on the Graph fault it was meant to
	// exercise rather than hiding that result behind a throughput assertion.
	requireGraphWork := !resume || resumeWorkExpected
	if len(unknown) != 0 {
		t.Fatalf("fake Graph observed unexpected routes in the production listener: %v", unknown)
	}
	if graphServerState.unsupportedFilters.Load() != 0 || graphServerState.invalidListQueries.Load() != 0 {
		t.Fatalf("strict fake Graph observed invalid production list queries: unsupported_filters=%d invalid_queries=%d diagnostics=%v requests=%s", graphServerState.unsupportedFilters.Load(), graphServerState.invalidListQueries.Load(), graphServerState.invalidListDiagnosticsSnapshot(), dockerRealDataRequestSummary(graphServerState.listRequests()))
	}
	if requireGraphWork && !rateLimitExperiment {
		faultChatID := graphServerState.faultChatSnapshot()
		if faultChatID == "" || graphServerState.listPageCount(faultChatID) == 0 {
			faultID, faultResponses, faultRemaining := graphServerState.faultStateSnapshot()
			faultPoll := afterState.ChatPolls[faultChatID]
			candidateState, candidateSessions, candidateHandled, candidateErr := store.HotPollScheduleAndWorkCandidatesExcludingIdleAt(postCtx, beforeState.ControlChat.TeamsChatID, time.Now().Add(-inboundPollParkAfter), time.Now())
			candidateFound := false
			for _, candidate := range candidateSessions {
				if strings.TrimSpace(candidate.TeamsChatID) == strings.TrimSpace(faultChatID) {
					candidateFound = true
					break
				}
			}
			_, readyFound := candidateState.ChatPolls[faultChatID]
			t.Fatalf("retryable Graph fault never recovered to a successful page: fault_chat=%q snapshot_chat=%q successful_pages=%d responses=%v request_paths=%v remaining=%d poll=%#v candidate_handled=%t candidate_err=%v candidate_count=%d candidate_found=%t ready_poll_found=%t", faultChatID, faultID, graphServerState.listPageCount(faultChatID), faultResponses, graphServerState.faultRequestPathsSnapshot(), faultRemaining, faultPoll, candidateHandled, candidateErr, len(candidateSessions), candidateFound, readyFound)
		}
		if faultPoll, found := afterState.ChatPolls[faultChatID]; !found {
			t.Fatalf("retryable Graph fault chat has no durable poll state after recovery: chat=%q", faultChatID)
		} else if strings.Contains(faultPoll.LastError, "429") || strings.Contains(faultPoll.LastError, "503") {
			// A successful Graph page may be committed as a partial quantum when
			// the bounded action budget leaves its immutable receipt pending. In
			// that case the old provider error remains as a diagnostic until the
			// receipt is fully drained; LastSuccessfulPollAt proves the retry gate
			// is no longer active, and the pending page is locally executable.
			if faultPoll.PendingPage == nil || faultPoll.LastSuccessfulPollAt.IsZero() || !faultPoll.LastSuccessfulPollAt.After(faultPoll.LastErrorAt) {
				t.Fatalf("retryable Graph fault remained the terminal durable poll error after a later successful page: chat=%q poll=%#v", faultChatID, faultPoll)
			}
			t.Logf("real-data retryable Graph fault completed a successful page and retained only a non-terminal diagnostic while its durable page drains: chat=%q", faultChatID)
		}
	}
	if requireGraphWork && !rateLimitExperiment && len(expiredProviderChats) > 0 && graphServerState.expiredContinuationCount() == 0 {
		t.Fatalf("real-data replay had %d executable active work chats with opaque provider continuations (%d total copied tokens), but never exercised an expired continuation response", len(expiredProviderChats), expiredProviderTokens)
	}
	// Keep the fault-specific recovery diagnosis above the generic fault-budget
	// assertions. Otherwise a short/slow real-data run reports only that the
	// injected 503s were not reached, hiding whether the failed chat was absent
	// from durable admission or merely waiting for its retry deadline.
	if requireGraphWork && !rateLimitExperiment && (graphServerState.status429.Load() == 0 || graphServerState.status503.Load() == 0) {
		faultChatID, faultResponses, faultRemaining := graphServerState.faultStateSnapshot()
		faultPoll, _ := afterState.ChatPolls[faultChatID]
		t.Fatalf("real-data replay did not exercise both retryable Graph failures: 429=%d 503=%d fault_chat=%q fault_responses=%v fault_request_paths=%v fault_remaining=%d fault_poll=%#v list_requests=%s", graphServerState.status429.Load(), graphServerState.status503.Load(), faultChatID, faultResponses, graphServerState.faultRequestPathsSnapshot(), faultRemaining, faultPoll, dockerRealDataRequestSummary(graphServerState.listRequests()))
	}
	if requireGraphWork && !rateLimitExperiment && graphServerState.status503.Load() < int64(defaultGraphRetries+1) {
		t.Fatalf("real-data replay did not exhaust the in-process 503 retry budget: 503=%d retry_budget=%d", graphServerState.status503.Load(), defaultGraphRetries)
	}
	ambiguousUnknownRows := dockerRealDataAmbiguousExecutionOutboxes(afterState)
	if resume {
		if graphServerState.unknownPosts.Load() != 0 || graphServerState.unknownPostRepeats.Load() != 0 || graphServerState.unknownPostMismatches.Load() != 0 {
			t.Fatalf("resumed process replayed an unknown Graph POST result: unknown=%d repeats=%d payload_mismatches=%d", graphServerState.unknownPosts.Load(), graphServerState.unknownPostRepeats.Load(), graphServerState.unknownPostMismatches.Load())
		}
		witness := readDockerRealDataUnknownPostWitness(t, statePath)
		var preserved *teamstore.OutboxMessage
		for _, row := range afterState.OutboxMessages {
			if strings.TrimSpace(row.ID) != witness.OutboxID {
				continue
			}
			copy := row
			preserved = &copy
			break
		}
		if preserved == nil || strings.TrimSpace(preserved.TeamsChatID) != witness.ChatID || dockerRealDataOutboxBodyHash(preserved.Body) != witness.BodyHash || !teamstore.OutboxSendIsAmbiguous(*preserved) {
			t.Fatalf("resumed process did not preserve the durable ambiguous outbox without retrying it: witness=%#v row=%#v", witness, preserved)
		}
	} else if unknownFaultEnabled {
		if graphServerState.unknownPosts.Load() != 1 || graphServerState.unknownPostRepeats.Load() != 0 || graphServerState.unknownPostMismatches.Load() != 0 || graphServerState.unknownPostAcceptedAttempts() != 1 {
			t.Fatalf("unknown Graph POST result was not handled as one remotely accepted, non-replayed attempt: unknown=%d repeats=%d payload_mismatches=%d remote_accepts=%d", graphServerState.unknownPosts.Load(), graphServerState.unknownPostRepeats.Load(), graphServerState.unknownPostMismatches.Load(), graphServerState.unknownPostAcceptedAttempts())
		}
		unknownPostChat := graphServerState.unknownPostChatSnapshot()
		unknownPostOutboxID := graphServerState.unknownPostOutboxIDSnapshot()
		row, found := afterState.OutboxMessages[unknownPostOutboxID]
		if unknownPostChat == "" || unknownPostOutboxID == "" || !found || strings.TrimSpace(row.TeamsChatID) != unknownPostChat || !dockerRealDataUnknownPostDispositionSafe(row) {
			t.Fatalf("unknown Graph POST was not represented by one safe durable outbox disposition: chat=%q outbox_id=%q found=%t row=%#v all_execution_rows=%#v", unknownPostChat, unknownPostOutboxID, found, row, ambiguousUnknownRows)
		}
		writeDockerRealDataUnknownPostWitness(t, statePath, row, graphServerState)
	} else if graphServerState.unknownPosts.Load() != 0 || len(ambiguousUnknownRows) != 0 {
		t.Fatalf("complete real-data mode unexpectedly left an ambiguous Graph POST: unknown=%d rows=%#v", graphServerState.unknownPosts.Load(), ambiguousUnknownRows)
	}
	if graphServerState.unknownPostMismatches.Load() != 0 {
		t.Fatalf("fake Graph observed unknown-POST witness payload mismatches: %d", graphServerState.unknownPostMismatches.Load())
	}
	if duplicateOutboxIDs := graphServerState.duplicateMessagePostOutboxIDs(); len(duplicateOutboxIDs) > 0 {
		t.Fatalf("fake Graph observed duplicate message POSTs for durable outbox IDs: %v", duplicateOutboxIDs)
	}

	// This is a liveness/measurement experiment, not a fixed-rate benchmark.
	// The copied corpus must still contain at least dockerRealDataMinimumReplay
	// eligible messages, and every measured window must make positive durable
	// progress above.  Requiring 100 completions in a one-minute run made the
	// gate reject a valid real-data result merely because SQLite/Graph fault
	// timing was slower than the historical workstation baseline.
	minimumCompleted := int64(1)
	if rateLimitExperiment {
		// The 429 mode is a bounded liveness/recovery experiment. Its wall clock
		// is intentionally short and the executor keeps its first call open to
		// exercise graceful durable completion, so requiring a 100-message drain
		// would turn the test back into a throughput smoke test.
		minimumCompleted = 1
	}
	completionForGate := dockerRealDataCompletionForGate(mode, measuredCompleted, completedDelta)
	if (!resume && completionForGate <= 0) || (resume && resumeWorkExpected && completionForGate <= 0) || (!resume && inboundDelta <= 0) || (!resume && runs <= 0) {
		phaseDiagnostics := make(map[string]mainLoopPhaseStats, len(phaseNames))
		if len(bridges) > 0 {
			for _, phaseName := range phaseNames {
				phaseDiagnostics[phaseName] = bridges[len(bridges)-1].mainLoopPhaseStatsSnapshot(phaseName)
			}
		}
		t.Fatalf("real copied Teams workload did not make durable execution progress: before=%#v after=%#v executor_runs=%d listener_err=%v phases=%#v listener_lines=%s poll_traces=%s queued_turns=%s poll_messages=%s graph_requests=%s", beforeCounts, afterCounts, runs, lastListenerErr, phaseDiagnostics, dockerRealDataTraceSummary(traceWriter.linesSnapshot()), dockerRealDataTraceSummary(traceWriter.pollChatsSnapshot()), dockerRealDataTraceSummary(traceWriter.queuedTurnsSnapshot()), dockerRealDataTraceSummary(traceWriter.pollMessagesSnapshot()), dockerRealDataRequestSummary(graphServerState.listRequests()))
	}
	if len(measuredWindowInbound) != measuredWindows || len(measuredWindowCompletedDeltas) != measuredWindows {
		t.Fatalf("real-data measured window accounting is incomplete: windows=%d inbound_deltas=%v completed_deltas=%v", measuredWindows, measuredWindowInbound, measuredWindowCompletedDeltas)
	}
	for index := range measuredWindowInbound {
		if !resume && measuredWindowInbound[index] <= 0 {
			t.Fatalf("real-data measured window %d made no durable progress: inbound_delta=%d completed_delta=%d", index+1, measuredWindowInbound[index], measuredWindowCompletedDeltas[index])
		}
		if mode == dockerRealDataModeThroughput && (!resume || resumeWorkExpected) && measuredWindowCompletedDeltas[index] <= 0 {
			t.Fatalf("real-data throughput window %d had no durable completion before the measurement sample: measured_completed_delta=%d overall_completion_delta=%d inbound_delta=%d", index+1, measuredWindowCompletedDeltas[index], completedDelta, measuredWindowInbound[index])
		}
	}
	if resume {
		if resumeWorkExpected {
			minimumCompleted = 1
		} else {
			minimumCompleted = 0
		}
	}
	drainCompleted := completedDelta - measuredCompleted
	if drainCompleted < 0 {
		drainCompleted = 0
	}
	t.Logf("real-data completion accounting: measured=%d overall=%d post-window-drain=%d gate=%d", measuredCompleted, completedDelta, drainCompleted, completionForGate)
	if mode == dockerRealDataModeThroughput && completionForGate < minimumCompleted {
		t.Fatalf("real-data process %s completed only %d synthetic turns inside the measured window (overall including drain=%d); want at least %d new completions to make the measured rate meaningful (corpus=%d duration=%s windows=%d)", map[bool]string{true: "resume", false: "initial"}[resume], completionForGate, completedDelta, minimumCompleted, replayCount, duration, measuredWindows)
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
	correlationAudit, err := dockerRealDataDurableCorrelationAudit(postCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName), mode == dockerRealDataModeComplete)
	if err != nil {
		t.Fatalf("read synthetic inbound/turn correlation: %v", err)
	}
	if correlationAudit.DuplicateTurn != 0 || (resume && beforeCorrelationAudit.DuplicateTurn != 0) {
		t.Fatalf("synthetic durable inbound/turn correlation contains duplicate admission: before=%+v after=%+v", beforeCorrelationAudit, correlationAudit)
	}
	// A graceful stop can land after the inbound row is durable but before the
	// separate QueueTurn transaction commits, or while the last admitted turn is
	// still completing. This is a valid boundary only in the explicitly bounded
	// two-process throughput experiment. The important invariant is not "zero
	// non-terminal rows": it is that every queued/running row that existed before
	// this process was started is repaired (or reaches a terminal status), while
	// any newly-created boundary row is proven to have been updated during this
	// process's own timed window. This catches a stale row carried across restart
	// without treating intentional timed teardown as a product failure.
	allowGracefulAdmissionBoundary := dockerRealDataAllowsGracefulAdmissionBoundary(mode)
	currentMissing := make(map[string]struct{}, len(correlationAudit.MissingMessageIDs))
	for _, messageID := range correlationAudit.MissingMessageIDs {
		currentMissing[messageID] = struct{}{}
	}
	previousBoundaryAudit := beforeCorrelationAudit
	if len(correlationBeforeRun) > 0 {
		previousBoundaryAudit = correlationBeforeRun[len(correlationBeforeRun)-1]
	}
	previousMissing := make(map[string]struct{}, len(previousBoundaryAudit.MissingMessageIDs))
	for _, messageID := range previousBoundaryAudit.MissingMessageIDs {
		previousMissing[messageID] = struct{}{}
	}
	unrepairedPreviousMissing := make([]string, 0)
	for messageID := range previousMissing {
		if _, found := currentMissing[messageID]; found {
			unrepairedPreviousMissing = append(unrepairedPreviousMissing, messageID)
		}
	}
	sort.Strings(unrepairedPreviousMissing)
	if len(unrepairedPreviousMissing) != 0 {
		t.Fatalf("real-data process did not repair inbound rows orphaned by its previous boundary: ids=%v before=%+v after=%+v", unrepairedPreviousMissing, previousBoundaryAudit, correlationAudit)
	}
	newMissing := make([]string, 0, len(currentMissing))
	for messageID := range currentMissing {
		if _, existed := previousMissing[messageID]; !existed {
			newMissing = append(newMissing, messageID)
		}
	}
	sort.Strings(newMissing)
	accounted := afterCounts.completed + afterCounts.failed + afterCounts.queued + afterCounts.running + afterCounts.interrupted
	if accounted != afterCounts.inbound {
		missingAccounting := afterCounts.inbound - accounted
		if !allowGracefulAdmissionBoundary || missingAccounting != correlationAudit.MissingTurn || len(newMissing) != int(correlationAudit.MissingTurn) {
			t.Fatalf("synthetic durable turn accounting is not closed: inbound=%d completed=%d failed=%d queued=%d running=%d interrupted=%d correlation=%+v", afterCounts.inbound, afterCounts.completed, afterCounts.failed, afterCounts.queued, afterCounts.running, afterCounts.interrupted, correlationAudit)
		}
		t.Logf("real-data graceful boundary: %d newly-created inbound row(s) have not acquired a turn yet; all previous-boundary orphan rows were repaired", correlationAudit.MissingTurn)
	}
	if afterCounts.failed != 0 {
		interruptedDetails, detailErr := dockerRealDataInterruptedTurnDetails(postCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
		t.Logf("synthetic interrupted turn details: rows=%v err=%v", interruptedDetails, detailErr)
		t.Fatalf("synthetic executions did not close successfully: after=%#v executor_runs=%d", afterCounts, runs)
	}
	if len(runStartedAt) > 0 {
		finalTurns, snapshotErr := dockerRealDataSyntheticTurnSnapshot(postCtx, filepath.Join(filepath.Dir(statePath), teamstore.SQLiteFileName))
		if snapshotErr != nil {
			t.Fatalf("read final synthetic turn boundaries: %v", snapshotErr)
		}
		latestRunStart := runStartedAt[len(runStartedAt)-1]
		latestBeforeTurns := syntheticTurnsBeforeRun[len(syntheticTurnsBeforeRun)-1]
		staleBoundaryIDs := make([]string, 0)
		currentBoundaryIDs := make([]string, 0)
		for turnID, beforeTurn := range latestBeforeTurns {
			if !dockerRealDataTurnNeedsBoundaryRepair(beforeTurn.Status) {
				continue
			}
			afterTurn, found := finalTurns[turnID]
			if !found || dockerRealDataTurnNeedsBoundaryRepair(afterTurn.Status) {
				staleBoundaryIDs = append(staleBoundaryIDs, turnID)
			}
		}
		for turnID, afterTurn := range finalTurns {
			if !dockerRealDataTurnIsNonTerminal(afterTurn.Status) {
				continue
			}
			currentBoundaryIDs = append(currentBoundaryIDs, turnID)
			if beforeTurn, existed := latestBeforeTurns[turnID]; existed && dockerRealDataTurnIsNonTerminal(beforeTurn.Status) {
				continue
			}
			if afterTurn.UpdatedAt.IsZero() || afterTurn.UpdatedAt.Before(latestRunStart) {
				staleBoundaryIDs = append(staleBoundaryIDs, turnID)
			}
		}
		sort.Strings(staleBoundaryIDs)
		sort.Strings(currentBoundaryIDs)
		if len(staleBoundaryIDs) > 0 {
			beforeStatuses := make([]string, 0, len(staleBoundaryIDs))
			afterStatuses := make([]string, 0, len(staleBoundaryIDs))
			beforeDetails := make([]string, 0, len(staleBoundaryIDs))
			afterDetails := make([]string, 0, len(staleBoundaryIDs))
			for _, turnID := range staleBoundaryIDs {
				before := latestBeforeTurns[turnID]
				after, found := finalTurns[turnID]
				beforeStatuses = append(beforeStatuses, fmt.Sprintf("%s:%s", turnID, before.Status))
				beforeDetails = append(beforeDetails, fmt.Sprintf("%s{session=%s status=%s machine=%s gen=%d reason=%q thread=%s codex=%s queued=%s started=%s updated=%s}", turnID, before.SessionID, before.Status, before.MachineID, before.LeaseGeneration, before.RecoveryReason, before.CodexThreadID, before.CodexTurnID, before.QueuedAt.Format(time.RFC3339Nano), before.StartedAt.Format(time.RFC3339Nano), before.UpdatedAt.Format(time.RFC3339Nano)))
				if found {
					afterStatuses = append(afterStatuses, fmt.Sprintf("%s:%s", turnID, after.Status))
					afterDetails = append(afterDetails, fmt.Sprintf("%s{session=%s status=%s machine=%s gen=%d reason=%q thread=%s codex=%s queued=%s started=%s updated=%s}", turnID, after.SessionID, after.Status, after.MachineID, after.LeaseGeneration, after.RecoveryReason, after.CodexThreadID, after.CodexTurnID, after.QueuedAt.Format(time.RFC3339Nano), after.StartedAt.Format(time.RFC3339Nano), after.UpdatedAt.Format(time.RFC3339Nano)))
				} else {
					afterStatuses = append(afterStatuses, fmt.Sprintf("%s:<missing>", turnID))
					afterDetails = append(afterDetails, fmt.Sprintf("%s{missing}", turnID))
				}
			}
			t.Logf("real-data stale boundary details: before=%v after=%v", beforeDetails, afterDetails)
			t.Logf("real-data stale boundary queued-turn trace: %s", dockerRealDataTraceSummary(traceWriter.queuedTurnsSnapshot()))
			t.Fatalf("real-data restart left queued/running synthetic turn boundaries from before the current process: ids=%v before_statuses=%v after_statuses=%v before_count=%d after_count=%d", staleBoundaryIDs, beforeStatuses, afterStatuses, len(latestBeforeTurns), len(finalTurns))
		}
		if len(currentBoundaryIDs) > 0 {
			if !allowGracefulAdmissionBoundary {
				t.Fatalf("real-data run left non-terminal synthetic turn boundaries: ids=%v after=%#v", currentBoundaryIDs, finalTurns)
			}
			t.Logf("real-data graceful boundary: current process left %d newly-created non-terminal turn(s) at timed stop: ids=%v", len(currentBoundaryIDs), currentBoundaryIDs)
		}
		boundaryCount := int64(len(currentBoundaryIDs))
		if !allowGracefulAdmissionBoundary && runs != completedDelta {
			t.Fatalf("executor invocation count=%d does not equal this process's durable completion delta=%d (before=%d after=%d); possible owner/fence loss or duplicate admission", runs, completedDelta, beforeCounts.completed, afterCounts.completed)
		}
		if allowGracefulAdmissionBoundary && (runs < completedDelta || runs > completedDelta+boundaryCount) {
			t.Fatalf("executor invocation count=%d is not explained by durable completion delta=%d plus current boundary turns=%d (before=%d after=%d)", runs, completedDelta, boundaryCount, beforeCounts.completed, afterCounts.completed)
		}
	} else if runs != completedDelta {
		t.Fatalf("executor invocation count=%d does not equal this process's durable completion delta=%d (before=%d after=%d); possible owner/fence loss or duplicate admission", runs, completedDelta, beforeCounts.completed, afterCounts.completed)
	}
	if correlationAudit.Unresolved != 0 {
		if !allowGracefulAdmissionBoundary || len(newMissing) != int(correlationAudit.MissingTurn) {
			t.Fatalf("synthetic durable inbound/turn correlation has %d unresolved or duplicate message(s): %+v", correlationAudit.Unresolved, correlationAudit)
		}
		t.Logf("real-data correlation is intentionally open only at the current graceful admission boundary: %+v", correlationAudit)
	}
	for run, changes := range ownerChangesByRun {
		if changes != 0 {
			t.Fatalf("listener owner generation changed within isolated run %d: changes=%d all_generations=%v", run+1, changes, ownerGenerations)
		}
	}
	observedPaginatedChats := 0
	durablePaginationWitnesses := 0
	largeReplayChats := 0
	unobservedLargeReplayChats := 0
	for chatID, corpus := range replayCorpus {
		if len(corpus) <= dockerRealDataPageSize {
			continue
		}
		largeReplayChats++
		pages := graphServerState.listPageCount(chatID)
		nextLinks := graphServerState.nextLinkCount(chatID)
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
		// durable action. A short measurement window can therefore end after the
		// first page. Count that as pagination coverage only when the fake Graph
		// emitted a real provider nextLink and the copied SQLite state retained
		// the corresponding pending/continuation/gap frontier; a second GET also
		// satisfies the stronger form of the witness.
		durableContinuation := false
		if poll, found := afterState.ChatPolls[chatID]; found {
			durableContinuation = poll.PendingPage != nil ||
				strings.TrimSpace(poll.ContinuationPath) != "" ||
				strings.TrimSpace(poll.DeferredContinuationPath) != "" ||
				poll.Gap != nil
		}
		if nextLinks > 0 && durableContinuation {
			durablePaginationWitnesses++
		}
		if pages >= 2 || nextLinks > 0 && durableContinuation {
			observedPaginatedChats++
		}
	}
	t.Logf("real-data pagination coverage: observed_paginated_chats=%d durable_nextlink_witnesses=%d large_replay_chats=%d unobserved_large_replay_chats=%d", observedPaginatedChats, durablePaginationWitnesses, largeReplayChats, unobservedLargeReplayChats)
	// Chat-coverage mode intentionally reduces the real backlog to one
	// representative message per chat. It therefore cannot contain a
	// multi-page corpus by construction; the dedicated throughput/all-data mode
	// keeps the full queue and owns this pagination assertion.
	if requireGraphWork && !rateLimitExperiment && !chatCoverage && observedPaginatedChats == 0 {
		t.Fatalf("real-data listener observed no multi-page chat or durable nextLink; pagination_witness=%q pages=%d next_links=%d messages=%d large_replay_chats=%d unobserved_large_replay_chats=%d", paginationWitness, graphServerState.listPageCount(paginationWitness), graphServerState.nextLinkCount(paginationWitness), len(replayCorpus[paginationWitness]), largeReplayChats, unobservedLargeReplayChats)
	}
	if rateLimitExperiment {
		if rateLimitScope == dockerRealData429ScopeChat {
			rateLimitedChats := graphServerState.persistentRateLimitChats()
			if len(rateLimitedChats) != dockerRealData429Chats {
				t.Fatalf("real-data chat-scoped 429 experiment bound %d chats, want %d selected replay chats: %v", len(rateLimitedChats), dockerRealData429Chats, rateLimitedChats)
			}
		} else if !graphServerState.persistentGlobalList429Configured() {
			t.Fatal("real-data account-scoped 429 experiment did not arm the shared Graph request budget")
		}
		want429 := int64(len(docker429RateLimitedChats) * dockerRealData429Failures)
		if rateLimitAccountWide {
			// One provider 429 opens the durable account gate for the whole
			// concurrent wave; sibling reads are deliberately suppressed.
			want429 = int64(dockerRealData429Failures)
		}
		if graphServerState.status429.Load() != want429 {
			t.Fatalf("real-data %s-scoped 429 experiment did not exercise exactly the finite high-intensity throttle: 429=%d want=%d chats=%v", rateLimitScope, graphServerState.status429.Load(), want429, docker429RateLimitedChats)
		}
		for _, chatID := range docker429RateLimitedChats {
			if graphServerState.listPageCount(chatID) == 0 {
				t.Fatalf("real-data 429 chat never recovered to a successful page: chat=%q", chatID)
			}
			if poll, found := afterState.ChatPolls[chatID]; !found {
				t.Fatalf("real-data 429 chat has no durable poll state after recovery: chat=%q", chatID)
			} else if strings.Contains(poll.LastError, "429") {
				// The production quantum intentionally handles only one action from a
				// large durable page. A successful Graph head therefore clears the
				// provider gate but may retain the old diagnostic until the local receipt
				// is fully drained. That is not a terminal block: the pending page is
				// immediately executable without another Graph request.
				if poll.PendingPage == nil || poll.LastSuccessfulPollAt.IsZero() || !poll.LastSuccessfulPollAt.After(poll.LastErrorAt) {
					t.Fatalf("real-data 429 chat retained a terminal rate-limit error after automatic recovery: chat=%q poll=%#v", chatID, poll)
				}
				t.Logf("real-data 429 chat completed a successful Graph recovery and retained only a non-terminal diagnostic while its durable page drains: chat=%q", chatID)
			}
		}
	}
	replayBacklogObserved := false
	if !rateLimitExperiment {
		replayBacklogObserved = maintenanceTrace.syntheticBacklogObserved()
		// The first process must prove that optional maintenance yields to a
		// durable replay backlog.  A restart process can legitimately start
		// after the previous measured window has drained every non-terminal
		// synthetic row; newly admitted rows are created by the poll phase and
		// are not required to overlap the next maintenance callback.  Requiring
		// overlap on that second process made a valid restart/liveness run fail
		// nondeterministically based only on phase ordering.
		requireSyntheticBacklogOverlap := !resume || beforeCounts.queued > 0 || beforeCounts.running > 0
		if requireSyntheticBacklogOverlap && !replayBacklogObserved {
			t.Fatalf("real-data replay never observed its synthetic Teams backlog while optional maintenance was running; backlog_samples=%d ordinary_history=%d ordinary_linked=%d", backlogSamples, ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog)
		}
		if !requireSyntheticBacklogOverlap && !replayBacklogObserved {
			t.Logf("real-data restart had no pre-existing synthetic non-terminal row to overlap optional maintenance; fairness overlap assertion is not applicable")
		}
		// Fairness is intentionally allowed during a long-lived backlog, but it
		// must be measured by durable state, not callback counts. A safe history
		// quantum may encounter an incomplete JSONL tail or an already-EOF file:
		// in those cases the physical offset must not move speculatively, while a
		// bounded partial-read/recovery checkpoint or the restart-safe fairness
		// cursor can still advance. Require one of those durable facts.
		ordinaryHistoryChanged, ordinaryHistoryAdded, ordinaryHistoryDetails := dockerRealDataOrdinaryHistoryDurableProgress(beforeState, afterHistoryState, historyMandatory)
		fairCursorChanges := dockerRealDataBacklogFairCursorChanges(beforeState.ServiceControl, afterState.ServiceControl)
		t.Logf("real-data backlog fairness durability: ordinary_history_changed=%d ordinary_history_added=%d checkpoint_details=%v fair_cursor_changes=%v offset_delta=%d", ordinaryHistoryChanged, ordinaryHistoryAdded, ordinaryHistoryDetails, fairCursorChanges, afterHistoryOffsets-beforeHistoryOffsets)
		if ordinaryHistoryChanged == 0 && ordinaryHistoryAdded == 0 && len(fairCursorChanges) == 0 {
			t.Fatalf("optional history maintenance made no durable progress while Teams backlog remained: history_callbacks=%d linked_callbacks=%d offsets_before=%d offsets_after=%d", ordinaryHistoryWhileBacklog, ordinaryLinkedWhileBacklog, beforeHistoryOffsets, afterHistoryOffsets)
		}
		if len(linkedUnindexed) == 0 {
			t.Logf("real-data replay has no unindexed active linked session candidate; fairness is not observable in this snapshot: ordinary_sessions=%v", ordinaryLinkedSessions)
		}
		if len(linkedUnindexed) > 0 {
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
	} else if rateLimitPollOnly {
		t.Logf("real-data 429 experiment used the explicitly requested targeted poll-only mode; optional maintenance fairness assertions are not applicable")
	} else {
		t.Logf("real-data 429 experiment used the full Bridge.Listen main loop; optional maintenance fairness was measured with the ordinary listener policy")
	}
	latestGracefulStop := time.Time{}
	for _, stoppedAt := range gracefulStopAt {
		if stoppedAt.After(latestGracefulStop) {
			latestGracefulStop = stoppedAt
		}
	}
	for phaseName, stats := range phaseStatsByName {
		allow429PollErrors := rateLimitExperiment && phaseName == "poll"
		resumeAmbiguousWitness := resume && resumeWitnessOutboxID != "" && graphServerState.durableUnknownPostWitness
		expectedPhaseErrors := dockerRealDataExpectedPhaseErrorBudget(phaseName, unknownFaultEnabled, graphServerState.unknownPosts.Load(), graphServerState.unknownPostRepeats.Load(), graphServerState.unknownPostMismatches.Load(), resumeAmbiguousWitness)
		startupErrors := startupPhaseErrors[phaseName]
		if startupErrors > expectedPhaseErrors && !allow429PollErrors {
			t.Fatalf("%s phase produced %d error(s) during startup/first cycle before the measured window (expected budget=%d): stats=%#v", phaseName, startupErrors, expectedPhaseErrors, stats)
		}
		remainingExpectedPhaseErrors := uint64(0)
		if startupErrors < expectedPhaseErrors {
			remainingExpectedPhaseErrors = expectedPhaseErrors - startupErrors
		}
		if startupErrors != 0 && expectedPhaseErrors != 0 && !allow429PollErrors {
			t.Logf("%s phase recorded %d intentionally injected fault error(s) during startup; remaining expected budget=%d", phaseName, startupErrors, remainingExpectedPhaseErrors)
		}
		if startupPhaseDeadlines[phaseName] != 0 {
			t.Fatalf("%s phase exceeded its budget %d time(s) during startup/first cycle: stats=%#v", phaseName, startupPhaseDeadlines[phaseName], stats)
		}
		beforeStop := phaseErrorsAtStop[phaseName]
		atMeasureStart := phaseErrorsAtMeasureStart[phaseName]
		measuredErrors := beforeStop - atMeasureStart
		teardownCancellation := dockerRealDataIntentionalTeardownPhaseCancellation(stats, latestGracefulStop)
		teardownErrorBudget := uint64(0)
		if teardownCancellation && measuredErrors > 0 {
			// The phase stats snapshot is taken at the measured timer boundary,
			// while graceful teardown continues afterward. A phase that was
			// already waiting on its child context can finish with the intentional
			// cancellation before Listen returns, so it lands in measuredErrors
			// even though it is not workload failure. Allow only that one final
			// cancellation; any additional error remains fatal.
			teardownErrorBudget = 1
		}
		if measuredErrors > remainingExpectedPhaseErrors+teardownErrorBudget && !allow429PollErrors {
			t.Fatalf("%s phase produced an error during the measured listener window: start_errors=%d stop_errors=%d measured_errors=%d expected_remaining=%d stats=%#v", phaseName, atMeasureStart, beforeStop, measuredErrors, remainingExpectedPhaseErrors, stats)
		}
		if stats.Errors < beforeStop {
			t.Fatalf("%s phase error counter regressed: stop_errors=%d final_errors=%d stats=%#v", phaseName, beforeStop, stats.Errors, stats)
		}
		postStop := stats.Errors - beforeStop
		if postStop == 0 {
			if teardownErrorBudget != 0 && measuredErrors > remainingExpectedPhaseErrors {
				t.Logf("%s phase cancellation observed during intentional Docker teardown before final listener snapshot: measured_errors=%d last_error=%q", phaseName, measuredErrors, stats.LastError)
			}
			continue
		}
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
		// backlog. When no mandatory recovery candidate exists, the optional
		// fairness quantum is one cold job and must remain a short no-op-sized
		// phase. If mandatory recovery is present, its bounded work is deliberately
		// included in the same phase and may account for the extra time; the phase
		// error/deadline checks above still enforce that it cannot consume the owner
		// budget or become an unbounded scan.
		const optionalMaintenanceBudget = time.Second
		if len(historyMandatory) == 0 && historyWatchStats.LastDuration > optionalMaintenanceBudget {
			t.Fatalf("optional history maintenance remained expensive while the synthetic Teams backlog was observed: queued_after=%d history=%#v", afterCounts.queued, historyWatchStats)
		}
		if len(linkedMandatory) == 0 && linkedTranscriptStats.LastDuration > optionalMaintenanceBudget {
			t.Fatalf("optional linked maintenance remained expensive while the synthetic Teams backlog was observed: queued_after=%d linked=%#v", afterCounts.queued, linkedTranscriptStats)
		}
		if len(historyMandatory) > 0 && historyWatchStats.LastDuration > optionalMaintenanceBudget {
			t.Logf("history-watch backlog phase includes mandatory recovery; retaining measured duration: %s", historyWatchStats.LastDuration)
		}
		if len(linkedMandatory) > 0 && linkedTranscriptStats.LastDuration > optionalMaintenanceBudget {
			t.Logf("linked-transcript backlog phase includes mandatory recovery; retaining measured duration: %s", linkedTranscriptStats.LastDuration)
		}
		if afterHistoryOffsets < beforeHistoryOffsets {
			t.Fatalf("history cursor regressed while the synthetic Teams backlog was observed: before=%d after=%d queued_after=%d", beforeHistoryOffsets, afterHistoryOffsets, afterCounts.queued)
		}
	}
}
