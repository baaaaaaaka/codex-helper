package migrate

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Legacy record shapes. Only the fields the converter uses are declared;
// unknown fields are ignored. Field names and JSON tags follow
// internal/teams/store at v0.1.20 and v0.1.22-rc.59, which agree on every
// field used here.

// lbool accepts true/false, numbers (non-zero is true) and quoted forms. Old
// stores contain at least one numeric last_offset_known (hazard: wrong type).
type lbool bool

func (b *lbool) UnmarshalJSON(p []byte) error {
	s := strings.Trim(string(bytes.TrimSpace(p)), `"`)
	switch strings.ToLower(s) {
	case "true":
		*b = true
	case "false", "", "null":
		*b = false
	default:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return errors.New("bool: " + s)
		}
		*b = f != 0
	}
	return nil
}

// ltime is a legacy RFC 3339 time. Zero ("0001-01-01T00:00:00Z"), empty,
// null and unparsable values decode to the zero time without an error.
type ltime struct{ time.Time }

func (t *ltime) UnmarshalJSON(p []byte) error {
	var s string
	if json.Unmarshal(p, &s) != nil {
		t.Time = time.Time{}
		return nil
	}
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		v = time.Time{}
	}
	t.Time = v
	return nil
}

// ms converts a legacy time to unix milliseconds (zero time -> 0).
func ms(t ltime) int64 {
	if t.IsZero() || t.Year() < 1970 {
		return 0
	}
	return t.UnixMilli()
}

type modelSnapshot struct {
	Name  string `json:"name"`
	Model string `json:"model"`
}

type legacySession struct {
	ID                      string        `json:"id"`
	Status                  string        `json:"status"`
	TeamsChatID             string        `json:"teams_chat_id"`
	TeamsChatURL            string        `json:"teams_chat_url"`
	TeamsTopic              string        `json:"teams_topic"`
	UserTitle               string        `json:"user_title"`
	TitleSource             string        `json:"title_source"`
	CodexThreadID           string        `json:"codex_thread_id"`
	RunnerKind              string        `json:"runner_kind"`
	Cwd                     string        `json:"cwd"`
	CodexHome               string        `json:"codex_home"`
	Profile                 string        `json:"profile"`
	Model                   string        `json:"model"`
	ModelProfile            modelSnapshot `json:"model_profile"`
	ModelSelectionSource    string        `json:"model_selection_source"`
	PendingModelProfile     modelSnapshot `json:"pending_model_profile"`
	PendingReasoningEffort  string        `json:"pending_reasoning_effort"`
	ReasoningEffort         string        `json:"reasoning_effort"`
	ReasoningEffortSource   string        `json:"reasoning_effort_source"`
	Sandbox                 string        `json:"sandbox"`
	PollFrontierInitialized lbool         `json:"poll_frontier_initialized"`
	QuarantinedAt           ltime         `json:"quarantined_at"`
	QuarantineReason        string        `json:"quarantine_reason"`
	CreatedAt               ltime         `json:"created_at"`
	UpdatedAt               ltime         `json:"updated_at"`
}

type legacyInbound struct {
	ID             string `json:"id"`
	SessionID      string `json:"session_id"`
	TeamsChatID    string `json:"teams_chat_id"`
	TeamsMessageID string `json:"teams_message_id"`
	Text           string `json:"text"`
	Source         string `json:"source"`
	Status         string `json:"status"`
	TurnID         string `json:"turn_id"`
	ReceivedAt     ltime  `json:"received_at"`
	CreatedAt      ltime  `json:"created_at"`
}

type legacyTurn struct {
	ID              string        `json:"id"`
	SessionID       string        `json:"session_id"`
	InboundEventID  string        `json:"inbound_event_id"`
	Status          string        `json:"status"`
	CodexThreadID   string        `json:"codex_thread_id"`
	CodexTurnID     string        `json:"codex_turn_id"`
	ModelProfile    modelSnapshot `json:"model_profile"`
	ReasoningEffort string        `json:"reasoning_effort"`
	FailureMessage  string        `json:"failure_message"`
	QueuedAt        ltime         `json:"queued_at"`
	StartedAt       ltime         `json:"started_at"`
	CompletedAt     ltime         `json:"completed_at"`
	FailedAt        ltime         `json:"failed_at"`
	InterruptedAt   ltime         `json:"interrupted_at"`
	CreatedAt       ltime         `json:"created_at"`
}

type legacyOutbox struct {
	ID                           string `json:"id"`
	SessionID                    string `json:"session_id"`
	TurnID                       string `json:"turn_id"`
	TeamsChatID                  string `json:"teams_chat_id"`
	Kind                         string `json:"kind"`
	Body                         string `json:"body"`
	Sequence                     int64  `json:"sequence"`
	PartIndex                    int    `json:"part_index"`
	PartCount                    int    `json:"part_count"`
	AttachmentPath               string `json:"attachment_path"`
	AttachmentSize               int64  `json:"attachment_size"`
	AttachmentUploadURL          string `json:"attachment_upload_url"`
	AttachmentUploadExpiry       ltime  `json:"attachment_upload_expiry"`
	AttachmentUploadOffset       int64  `json:"attachment_upload_offset"`
	AttachmentUploadPostState    string `json:"attachment_upload_session_post_state"`
	AttachmentMessagePostState   string `json:"attachment_message_post_state"`
	DriveItemID                  string `json:"drive_item_id"`
	QuoteReplyToMessageID        string `json:"quote_reply_to_message_id"`
	MentionOwner                 lbool  `json:"mention_owner"`
	Status                       string `json:"status"`
	TeamsMessageID               string `json:"teams_message_id"`
	BlockedByUnresolvedExecution lbool  `json:"blocked_by_unresolved_execution"`
	BlockedByTerminalFailure     lbool  `json:"blocked_by_terminal_failure"`
	BlockedBySourceRewrite       lbool  `json:"blocked_by_source_rewrite"`
	CreatedAt                    ltime  `json:"created_at"`
	SentAt                       ltime  `json:"sent_at"`
	NextAttemptAt                ltime  `json:"next_attempt_at"`
	SendAttemptToken             string `json:"send_attempt_token"`
}

// unknownOutcome reports whether the old row's send outcome is unknown:
// sending, accepted-not-sent, an in-flight post state, or blocked_by_*.
func (o legacyOutbox) unknownOutcome() bool {
	switch o.Status {
	case "sending", "accepted":
		return true
	case "sent", "skipped":
		return false
	}
	inflight := func(s string) bool { return s == "started" || s == "uncertain" || s == "ambiguous" }
	return bool(o.BlockedByUnresolvedExecution) || bool(o.BlockedByTerminalFailure) || bool(o.BlockedBySourceRewrite) ||
		inflight(o.AttachmentMessagePostState) || inflight(o.AttachmentUploadPostState)
}

type legacyPollGap struct {
	SafeCursor ltime `json:"safe_cursor"`
}

type legacyPoll struct {
	ChatID                      string         `json:"chat_id"`
	Seeded                      lbool          `json:"seeded"`
	PollState                   string         `json:"state"`
	ParkedAt                    ltime          `json:"parked_at"`
	LastModifiedCursor          ltime          `json:"last_modified_cursor"`
	ContinuationSafeCursor      ltime          `json:"continuation_safe_cursor"`
	ContinuationSafeCursorKnown lbool          `json:"continuation_safe_cursor_known"`
	ContinuationPath            string         `json:"continuation_path"`
	PendingPage                 *pendingPage   `json:"pending_page"`
	Gap                         *legacyPollGap `json:"gap"`
}

type pendingPage struct {
	Records []json.RawMessage `json:"records"`
}

type legacyAnchor struct {
	SessionID      string `json:"session_id"`
	ThreadID       string `json:"thread_id"`
	SourcePath     string `json:"transcript_source"`
	CutoffRecordID string `json:"cutoff_record_id"`
	CutoffLine     int64  `json:"cutoff_line"`
	CutoffOffset   int64  `json:"cutoff_offset"`
	Reason         string `json:"reason"`
	Provenance     string `json:"provenance"`
	State          string `json:"state"`
}

type legacyCheckpoint struct {
	ID                   string        `json:"id"`
	SessionID            string        `json:"session_id"`
	SourcePath           string        `json:"source_path"`
	SourceRewriteBlocked lbool         `json:"source_rewrite_blocked"`
	LastRecordID         string        `json:"last_record_id"`
	LastSourceLine       int64         `json:"last_source_line"`
	LastOffset           int64         `json:"last_offset"`
	LastOffsetKnown      lbool         `json:"last_offset_known"`
	SourceSize           int64         `json:"source_size"`
	SourceModTime        ltime         `json:"source_mod_time"`
	Status               string        `json:"status"`
	UnresolvedExecution  *legacyAnchor `json:"unresolved_execution"`
}

type legacyWatch struct {
	ID                   string `json:"id"`
	Path                 string `json:"path"`
	Size                 int64  `json:"size"`
	ModTime              ltime  `json:"mod_time"`
	SourceRewriteBlocked lbool  `json:"source_rewrite_blocked"`
	Offset               int64  `json:"offset"`
	Line                 int64  `json:"line"`
	SessionID            string `json:"session_id"`
	ThreadID             string `json:"thread_id"`
}

type legacyNotification struct {
	ID             string `json:"id"`
	Status         string `json:"status"`
	Title          string `json:"title"`
	ChatTitle      string `json:"chat_title"`
	RequestSummary string `json:"request_summary"`
	ButtonURL      string `json:"button_url"`
	CreatedAt      ltime  `json:"created_at"`
	SentAt         ltime  `json:"sent_at"`
}

type legacyFork struct {
	ID              string `json:"id"`
	ParentSessionID string `json:"parent_session_id"`
	ChildSessionID  string `json:"child_session_id"`
	ChildThreadID   string `json:"child_thread_id"`
	Phase           string `json:"phase"`
	CreatedAt       ltime  `json:"created_at"`
}

type legacyDelivery struct {
	ID             string `json:"id"`
	SessionID      string `json:"session_id"`
	TeamsChatID    string `json:"teams_chat_id"`
	OutboxID       string `json:"outbox_id"`
	TeamsMessageID string `json:"teams_message_id"`
	Status         string `json:"status"`
	CreatedAt      ltime  `json:"created_at"`
	SentAt         ltime  `json:"sent_at"`
}

type legacyRateLimit struct {
	ChatID       string `json:"chat_id"`
	BlockedUntil ltime  `json:"blocked_until"`
	Reason       string `json:"reason"`
}

type legacyArtifact struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type legacyControlChat struct {
	ScopeID        string `json:"scope_id"`
	TeamsChatID    string `json:"teams_chat_id"`
	TeamsChatURL   string `json:"teams_chat_url"`
	TeamsChatTopic string `json:"teams_chat_topic"`
	UserTitle      string `json:"user_title"`
	BoundAt        ltime  `json:"bound_at"`
}

type legacyLease struct {
	LeaseUntil    ltime `json:"lease_until"`
	LastHeartbeat ltime `json:"last_heartbeat"`
}

type legacyOwner struct {
	PID           int    `json:"pid"`
	Hostname      string `json:"hostname"`
	LastHeartbeat ltime  `json:"last_heartbeat"`
}

type legacyServiceControl struct {
	Paused lbool  `json:"paused"`
	Reason string `json:"reason"`
}

type legacyAutoUpdate struct {
	LastInstalledTag string `json:"last_installed_tag"`
}

type legacyScope struct {
	ID string `json:"id"`
}

type legacyRegistry struct {
	MachineHostnameOverride string `json:"machine_hostname_override"`
	ControlChatID           string `json:"control_chat_id"`
	ControlChatURL          string `json:"control_chat_url"`
	ControlChatTopic        string `json:"control_chat_topic"`
	Chats                   map[string]struct {
		SeenMessageIDs []string `json:"seen_message_ids"`
	} `json:"chats"`
}

type threadLink struct {
	SessionID     string `json:"session_id"`
	CodexThreadID string `json:"codex_thread_id"`
	ObservedAt    ltime  `json:"observed_at"`
}

// decode unmarshals one legacy row. A wrong-typed field is tolerated (the
// field stays zero and a warning is returned); malformed JSON is an error.
func decode(raw []byte, v any) (warn string, err error) {
	err = json.Unmarshal(raw, v)
	var te *json.UnmarshalTypeError
	if errors.As(err, &te) {
		return "field " + te.Field + ": " + te.Error(), nil
	}
	return "", err
}
