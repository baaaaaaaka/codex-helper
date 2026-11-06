package store

// Enumerations (mirrors of the CHECK constraints).
const (
	KindControl = "control"
	KindWork    = "work"

	ConvCreating = "creating"
	ConvActive   = "active"
	ConvParked   = "parked"
	ConvClosed   = "closed"
	ConvHold     = "hold"

	SenderOwner = "owner"
	SenderSelf  = "self"
	SenderPeer  = "peer"
	SenderOther = "other"

	InPrompt  = "prompt"
	InCommand = "command"
	InSystem  = "system"
	InEcho    = "echo"

	InQueued  = "queued"
	InHeld    = "held"
	InDone    = "done"
	InIgnored = "ignored"

	TurnQueued      = "queued"
	TurnStarting    = "starting"
	TurnRunning     = "running"
	TurnCompleted   = "completed"
	TurnFailed      = "failed"
	TurnInterrupted = "interrupted"
	TurnNotStarted  = "not_started"
	TurnCancelled   = "cancelled"

	OutAck      = "ack"
	OutProgress = "progress"
	OutFinal    = "final"
	OutMirror   = "mirror"
	OutNotice   = "notice"
	OutTitle    = "title"
	OutControl  = "control"

	OutQueued  = "queued"
	OutSending = "sending"
	OutSent    = "sent"
	OutUnknown = "unknown"
	OutDropped = "dropped"

	SrcTracking = "tracking"
	SrcBaseline = "baseline"
	SrcIgnored  = "ignored"
	SrcMissing  = "missing"
)

// Conversation is one chat (control or work).
type Conversation struct {
	ID             int64
	Label          string
	Kind           string
	Channel        string
	ExtChatID      string
	ExtURL         string
	Title          string
	TitleSource    string
	State          string
	ParkedAt       int64
	PausedAt       int64
	PausedReason   string
	PausedNotified bool
	CreatedAt      int64
	UpdatedAt      int64
}

// Thread is the Codex binding of a work conversation.
type Thread struct {
	ConvID        int64
	ThreadID      string
	Cwd           string
	CodexHome     string
	Profile       string
	Model         string
	ModelSource   string
	Effort        string
	EffortSource  string
	PendingModel  string
	PendingEffort string
	Sandbox       string
	ModelPin      string
	ForkParent    int64
	UpdatedAt     int64
}

// Stream is a durable receive cursor.
type Stream struct {
	Key       string
	ConvID    int64
	Cursor    string
	Seeded    bool
	UpdatedAt int64
}

// InboundFile is one attachment of an inbound message.
type InboundFile struct {
	Idx       int
	Name      string
	ExtRef    string
	LocalPath string
	Kind      string // file | image | voice
	State     string // pending | ready | failed
	Error     string
}

// InboundMsg is one received message with its pure classification.
type InboundMsg struct {
	ExtMsgID string
	ExtTime  int64
	Sender   string
	Kind     string
	Body     string
	Files    []InboundFile
}

// Admitted reports one newly persisted inbound row.
type Admitted struct {
	InboundID int64
	ExtMsgID  string
	Kind      string
	State     string
	TurnID    int64 // 0 when no turn was created
}

// Inbound is an inbound row.
type Inbound struct {
	ID         int64
	ConvID     int64
	ExtMsgID   string
	ExtTime    int64
	ReceivedAt int64
	Sender     string
	Kind       string
	State      string
	Body       string
	TurnID     int64
}

// Turn is a turns row.
type Turn struct {
	ID            int64
	ConvID        int64
	InboundID     int64
	TurnKey       string
	State         string
	ThreadID      string
	CodexTurnID   string
	Model         string
	Effort        string
	Attempt       int
	RetryDeadline int64
	QueuedAt      int64
	StartedAt     int64
	FinishedAt    int64
	Error         string
}

// OutFile is the upload state of an outbound file.
type OutFile struct {
	LocalPath       string
	Size            int64
	UploadURL       string
	UploadExpiresAt int64
	UploadedBytes   int64
	ExtItemID       string
}

// OutMsg is one rendered part to enqueue.
type OutMsg struct {
	DeliveryKey string
	Kind        string
	Part        int // 1-based; 0 means 1
	Parts       int // 0 means 1
	Body        string
	BodyHash    []byte
	Mention     bool
	ReplyTo     string
	File        *OutFile
}

// Outbox is an outbox row.
type Outbox struct {
	ID            int64
	ConvID        int64
	DeliveryKey   string
	Seq           int64
	Kind          string
	Part          int
	Parts         int
	Body          string
	BodyHash      []byte
	Mention       bool
	ReplyTo       string
	State         string
	AttemptToken  string
	ExtMsgID      string
	NextAttemptAt int64
	CreatedAt     int64
	SentAt        int64
	Note          string
	// HashAlgo is "" when BodyHash comes from the channel adapter, or
	// HashLegacy for an imported legacy hash (re-hashed by RehashLegacy).
	HashAlgo string
}

// HashLegacy marks an outbox body_hash imported from the legacy helper.
const HashLegacy = "legacy"

// TranscriptSource is a local Codex rollout file.
type TranscriptSource struct {
	ID        int64
	Path      string
	ThreadID  string
	ConvID    int64
	State     string
	Offset    int64
	Line      int64
	Size      int64
	MTime     int64
	Inode     int64
	UpdatedAt int64
}

// Fork is a fork operation.
type Fork struct {
	ID            int64
	ParentConv    int64
	ChildConv     int64
	Phase         string
	ChildThreadID string
	CreatedAt     int64
	UpdatedAt     int64
}

// Notification is an optional webhook card.
type Notification struct {
	ID        int64
	Key       string
	Kind      string // e.g. turn_completed, chat_ready, needs_attention
	Payload   string
	State     string
	CreatedAt int64
	SentAt    int64
}

// Gate is a throttling gate that survives restart.
type Gate struct {
	Key    string
	Until  int64
	Reason string
}
