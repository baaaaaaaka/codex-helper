package skills

import "time"

type PushReview struct {
	Chat      string         `json:"chat"`
	Name      string         `json:"name,omitempty"`
	Direct    bool           `json:"direct,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
	ExpiresAt time.Time      `json:"expires_at"`
	Sources   []ReviewSource `json:"sources"`
}

type ReviewSource struct {
	SourceID      string         `json:"source_id"`
	SourceName    string         `json:"source_name"`
	RemoteURL     string         `json:"remote_url,omitempty"`
	Ref           string         `json:"ref,omitempty"`
	BaseCommit    string         `json:"base_commit"`
	ReviewRefSpec string         `json:"review_refspec"`
	Changes       []ReviewChange `json:"changes"`
}

type ReviewChange struct {
	Kind       string `json:"kind"`
	RelPath    string `json:"rel_path"`
	SourcePath string `json:"source_path"`
	Commit     string `json:"commit,omitempty"`
	OldSHA256  string `json:"old_sha256,omitempty"`
	NewSHA256  string `json:"new_sha256,omitempty"`
	OldMode    uint32 `json:"old_mode,omitempty"`
	NewMode    uint32 `json:"new_mode,omitempty"`
	Size       int64  `json:"size,omitempty"`
}
