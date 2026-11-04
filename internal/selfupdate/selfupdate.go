// Package selfupdate holds the data of a helper self-update: the release
// check, the selected candidate and the outcome of installing it. The
// release selection and the install live in internal/update and the CLI;
// the Teams engine decides when to check and install.
package selfupdate

import (
	"context"
	"time"
)

// Updater checks for and installs helper releases.
type Updater interface {
	Check(context.Context, Check) (Decision, error)
	ApplyWithOptions(context.Context, Candidate, ApplyOptions) (ApplyResult, error)
}

// Check is one release check.
type Check struct {
	InstalledVersion  string
	Now               time.Time
	IncludePrerelease bool
	Manual            bool // an explicit request: ignore the release priority delay
}

// Decision is the outcome of a check: the release to install, if any, and
// when to check again.
type Decision struct {
	Candidate    *Candidate
	NextCheckAt  time.Time
	BackoffUntil time.Time
	LastError    string
}

// Candidate is a release selected for installation.
type Candidate struct {
	TagName     string
	Version     string
	Priority    string
	PublishedAt time.Time
	EligibleAt  time.Time
	Asset       string
}

// ApplyOptions modify an install.
type ApplyOptions struct {
	// OwnsPendingReplacement: the caller already holds the pending
	// replacement of a locked executable (Windows) and may finish it.
	OwnsPendingReplacement bool
}

// ApplyResult is the outcome of an install.
type ApplyResult struct {
	Version            string
	InstallPath        string
	RestartRequired    bool
	PendingReplacePath string
	ActivationPending  bool
	ActivationReason   string
}
