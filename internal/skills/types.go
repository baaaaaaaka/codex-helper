package skills

import "time"

const (
	configVersion = 1
	stateVersion  = 1

	ConfigFilename = "skill-subscriptions.json"
	StateFilename  = "skill-subscriptions-state.json"

	TargetCodexHome = "codex-home"
	TargetAgents    = "agents"

	StatusReady        = "ready"
	StatusSyncing      = "syncing"
	StatusSyncFailed   = "sync_failed"
	StatusAuthRequired = "auth_required"
	StatusInvalid      = "invalid_layout"
	StatusConflict     = "conflict"
	StatusModified     = "local_modified"
	StatusDisabled     = "disabled"
)

type Config struct {
	Version int      `json:"version"`
	Sources []Source `json:"sources"`
}

type Source struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	URL         string    `json:"url"`
	RemoteURL   string    `json:"remote_url"`
	Provider    string    `json:"provider,omitempty"`
	Ref         string    `json:"ref,omitempty"`
	Path        string    `json:"path,omitempty"`
	TargetKind  string    `json:"target_kind"`
	TargetRoot  string    `json:"target_root"`
	AutoSync    bool      `json:"auto_sync"`
	AddedAt     time.Time `json:"added_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	LastAddedBy string    `json:"last_added_by,omitempty"`
}

type State struct {
	Version int           `json:"version"`
	Sources []SourceState `json:"sources"`
}

type SourceState struct {
	ID              string           `json:"id"`
	Status          string           `json:"status"`
	LastSyncAt      time.Time        `json:"last_sync_at,omitempty"`
	LastAutoSyncDay string           `json:"last_auto_sync_day,omitempty"`
	LastCommit      string           `json:"last_commit,omitempty"`
	LastError       string           `json:"last_error,omitempty"`
	InstalledSkills []InstalledSkill `json:"installed_skills,omitempty"`
}

type InstalledSkill struct {
	Name       string         `json:"name"`
	ExportName string         `json:"export_name"`
	SourcePath string         `json:"source_path"`
	TargetPath string         `json:"target_path"`
	Files      []FileManifest `json:"files"`
}

type FileManifest struct {
	RelPath string `json:"rel_path"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
	Mode    uint32 `json:"mode"`
}

type AddOptions struct {
	Name       string
	Ref        string
	Path       string
	TargetKind string
	AutoSync   *bool
}

type SyncOptions struct {
	Name string
	All  bool
}

type SyncResult struct {
	Source    Source
	State     SourceState
	Commit    string
	Installed []InstalledSkill
	Error     error
}

type StatusEntry struct {
	Source Source
	State  SourceState
}

type ChangeKind string

const (
	ChangeModified ChangeKind = "modified"
	ChangeAdded    ChangeKind = "added"
	ChangeDeleted  ChangeKind = "deleted"
)

type LocalChange struct {
	Source     Source
	Skill      InstalledSkill
	Kind       ChangeKind
	RelPath    string
	SourcePath string
	Commit     string
	OldSHA256  string
	NewSHA256  string
	OldMode    uint32
	NewMode    uint32
	Size       int64
}
