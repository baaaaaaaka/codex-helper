package store

// ApplicationID identifies a core store file ('cxp1').
const ApplicationID = 0x63787031

// SchemaVersion is the PRAGMA user_version this code writes and understands.
const SchemaVersion = 2

// migrations[i] upgrades user_version i to i+1. Each step runs in one
// transaction together with the user_version bump. There is no pointer file.
var migrations = [][]string{
	schemaV1,
	// Constant defaults keep this migration metadata-only. Adding CHECK
	// constraints here would make SQLite scan the complete old history.
	{`ALTER TABLE turns ADD COLUMN queued_only INTEGER NOT NULL DEFAULT 1`,
		`ALTER TABLE turns ADD COLUMN steering INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE inbound ADD COLUMN queued_only INTEGER NOT NULL DEFAULT 1`,
		`ALTER TABLE turns ADD COLUMN shared INTEGER NOT NULL DEFAULT 0`},
}

// schemaV1 is the complete initial schema (STORE-DESIGN.md §3). No triggers
// and no JSON columns: every value is a typed column.
var schemaV1 = []string{
	`CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID`,

	`CREATE TABLE owner (
  id           INTEGER PRIMARY KEY CHECK (id = 1),
  generation   INTEGER NOT NULL,
  host         TEXT    NOT NULL,
  install_host TEXT    NOT NULL,
  pid          INTEGER NOT NULL,
  boot_id      TEXT    NOT NULL,
  heartbeat_at INTEGER NOT NULL
)`,

	`CREATE TABLE conversations (
  id              INTEGER PRIMARY KEY,
  label           TEXT NOT NULL UNIQUE,
  kind            TEXT NOT NULL CHECK (kind IN ('control','work')),
  channel         TEXT NOT NULL,
  ext_chat_id     TEXT,
  ext_url         TEXT,
  title           TEXT,
  title_source    TEXT CHECK (title_source IN ('user','auto','legacy')),
  state           TEXT NOT NULL CHECK (state IN ('creating','active','parked','closed','hold')),
  parked_at       INTEGER,
  paused_at       INTEGER,
  paused_reason   TEXT,
  paused_notified INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE (channel, ext_chat_id)
)`,
	`CREATE INDEX conversations_live ON conversations(state) WHERE state IN ('creating','active','hold')`,

	`CREATE TABLE threads (
  conv_id        INTEGER PRIMARY KEY REFERENCES conversations(id),
  thread_id      TEXT,
  cwd            TEXT NOT NULL,
  codex_home     TEXT,
  profile        TEXT,
  model          TEXT,
  model_source   TEXT,
  effort         TEXT,
  effort_source  TEXT,
  pending_model  TEXT,
  pending_effort TEXT,
  sandbox        TEXT,
  model_pin      TEXT,
  fork_parent    INTEGER REFERENCES conversations(id),
  updated_at     INTEGER NOT NULL
)`,
	`CREATE INDEX threads_by_thread ON threads(thread_id)`,

	`CREATE TABLE streams (
  key        TEXT PRIMARY KEY,
  conv_id    INTEGER REFERENCES conversations(id),
  cursor     TEXT,
  seeded     INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
) WITHOUT ROWID`,

	`CREATE TABLE turns (
  id             INTEGER PRIMARY KEY,
  conv_id        INTEGER NOT NULL REFERENCES conversations(id),
  inbound_id     INTEGER REFERENCES inbound(id),
  turn_key       TEXT NOT NULL UNIQUE,
  state          TEXT NOT NULL CHECK (state IN
                   ('queued','starting','running','completed','failed','interrupted','not_started','cancelled')),
  thread_id      TEXT,
  codex_turn_id  TEXT,
  model          TEXT,
  effort         TEXT,
  attempt        INTEGER NOT NULL DEFAULT 0,
  retry_deadline INTEGER,
  queued_at      INTEGER NOT NULL,
  started_at     INTEGER,
  finished_at    INTEGER,
  error          TEXT
)`,
	`CREATE INDEX turns_open ON turns(conv_id, id) WHERE state IN ('queued','starting','running')`,
	`CREATE INDEX turns_by_inbound ON turns(inbound_id) WHERE inbound_id IS NOT NULL`,

	`CREATE TABLE inbound (
  id          INTEGER PRIMARY KEY,
  conv_id     INTEGER NOT NULL REFERENCES conversations(id),
  ext_msg_id  TEXT NOT NULL,
  ext_time    INTEGER NOT NULL,
  received_at INTEGER NOT NULL,
  sender      TEXT NOT NULL CHECK (sender IN ('owner','self','peer','other')),
  kind        TEXT NOT NULL CHECK (kind IN ('prompt','command','system','echo')),
  state       TEXT NOT NULL CHECK (state IN ('queued','held','done','ignored')),
  body        TEXT,
  turn_id     INTEGER REFERENCES turns(id),
  UNIQUE (conv_id, ext_msg_id)
)`,
	`CREATE INDEX inbound_open ON inbound(conv_id, ext_time) WHERE state IN ('queued','held')`,
	// Foreign-key child lookup for turns(id); without it deleting a turn scans inbound.
	`CREATE INDEX inbound_by_turn ON inbound(turn_id) WHERE turn_id IS NOT NULL`,

	`CREATE TABLE inbound_files (
  inbound_id INTEGER NOT NULL REFERENCES inbound(id),
  idx        INTEGER NOT NULL,
  name       TEXT NOT NULL,
  ext_ref    TEXT NOT NULL,
  local_path TEXT,
  kind       TEXT NOT NULL CHECK (kind IN ('file','image','voice')),
  state      TEXT NOT NULL CHECK (state IN ('pending','ready','failed')),
  error      TEXT,
  PRIMARY KEY (inbound_id, idx)
) WITHOUT ROWID`,

	`CREATE TABLE outbox (
  id              INTEGER PRIMARY KEY,
  conv_id         INTEGER NOT NULL REFERENCES conversations(id),
  delivery_key    TEXT NOT NULL UNIQUE,
  seq             INTEGER NOT NULL,
  kind            TEXT NOT NULL CHECK (kind IN ('ack','progress','final','mirror','notice','title','control')),
  part            INTEGER NOT NULL DEFAULT 1,
  parts           INTEGER NOT NULL DEFAULT 1,
  body            TEXT,
  body_hash       BLOB,
  mention         INTEGER NOT NULL DEFAULT 0,
  reply_to        TEXT,
  state           TEXT NOT NULL CHECK (state IN ('queued','sending','sent','unknown','dropped')),
  attempt_token   TEXT,
  ext_msg_id      TEXT,
  next_attempt_at INTEGER,
  created_at      INTEGER NOT NULL,
  sent_at         INTEGER,
  note            TEXT,
  hash_algo       TEXT CHECK (hash_algo IN ('legacy')), -- NULL: the channel adapter's hash; 'legacy': imported, re-hash at first start
  UNIQUE (conv_id, seq)
)`,
	`CREATE INDEX outbox_pending ON outbox(conv_id, seq) WHERE state IN ('queued','sending','unknown')`,
	`CREATE INDEX outbox_ext ON outbox(conv_id, ext_msg_id) WHERE ext_msg_id IS NOT NULL`,
	`CREATE INDEX outbox_hash ON outbox(conv_id, body_hash) WHERE state IN ('sending','sent','unknown')`,
	`CREATE INDEX outbox_legacy_hash ON outbox(id) WHERE hash_algo IS NOT NULL`,

	`CREATE TABLE outbox_files (
  outbox_id         INTEGER PRIMARY KEY REFERENCES outbox(id),
  local_path        TEXT NOT NULL,
  size              INTEGER NOT NULL,
  upload_url        TEXT,
  upload_expires_at INTEGER,
  uploaded_bytes    INTEGER NOT NULL DEFAULT 0,
  ext_item_id       TEXT
)`,

	`CREATE TABLE transcript_sources (
  id         INTEGER PRIMARY KEY,
  path       TEXT NOT NULL UNIQUE,
  thread_id  TEXT,
  conv_id    INTEGER REFERENCES conversations(id),
  state      TEXT NOT NULL CHECK (state IN ('tracking','baseline','ignored','missing')),
  offset     INTEGER NOT NULL CHECK (offset >= 0),
  line       INTEGER NOT NULL CHECK (line >= 0),
  size       INTEGER NOT NULL,
  mtime      INTEGER NOT NULL,
  inode      INTEGER,
  updated_at INTEGER NOT NULL
)`,
	`CREATE INDEX transcript_by_conv ON transcript_sources(conv_id)`,

	`CREATE TABLE forks (
  id              INTEGER PRIMARY KEY,
  parent_conv     INTEGER NOT NULL REFERENCES conversations(id),
  child_conv      INTEGER REFERENCES conversations(id),
  phase           TEXT NOT NULL CHECK (phase IN ('requested','thread_forked','chat_created','done','hold')),
  child_thread_id TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL
)`,

	`CREATE TABLE notifications (
  id         INTEGER PRIMARY KEY,
  key        TEXT NOT NULL UNIQUE,
  kind       TEXT NOT NULL,
  payload    TEXT NOT NULL,
  state      TEXT NOT NULL CHECK (state IN ('queued','sending','sent','unknown','dropped')),
  created_at INTEGER NOT NULL,
  sent_at    INTEGER
)`,

	`CREATE TABLE gates (
  key    TEXT PRIMARY KEY,
  until  INTEGER NOT NULL,
  reason TEXT
) WITHOUT ROWID`,
}

// Tables lists every table of the current schema in creation order.
var Tables = []string{
	"meta", "owner", "conversations", "threads", "streams", "turns", "inbound",
	"inbound_files", "outbox", "outbox_files", "transcript_sources", "forks",
	"notifications", "gates",
}
