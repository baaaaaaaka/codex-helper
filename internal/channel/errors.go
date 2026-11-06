package channel

import (
	"errors"
	"fmt"
	"time"
)

// Outcome classification of a failed channel call. Adapters return one of
// these (possibly wrapped); anything else is a local error (bad input, file
// system) and should be treated like Rejected.
//
//	Rejected   the service did not act on the request; safe to try again later
//	           (Permanent: retrying the same request will fail the same way).
//	Ambiguous  a write may or may not have been applied; never replay it
//	           blindly, resolve it with ReadBack first.
//	Throttled  the service asked us to slow down; nothing was applied.
//	AuthRequired the credentials are missing or rejected; a human must sign in.
//	WindowTooLarge Receive only: the backlog is too large to admit in order.

// Rejected means the request was definitely not applied.
type Rejected struct {
	Permanent bool
	Err       error
}

func (e *Rejected) Error() string {
	if e.Permanent {
		return "channel: rejected (permanent): " + errText(e.Err)
	}
	return "channel: rejected: " + errText(e.Err)
}
func (e *Rejected) Unwrap() error { return e.Err }

// Ambiguous means a write reached the service but its outcome is unknown.
type Ambiguous struct{ Err error }

func (e *Ambiguous) Error() string { return "channel: outcome unknown: " + errText(e.Err) }
func (e *Ambiguous) Unwrap() error { return e.Err }

// ThrottleScope says what a Throttled error blocks.
type ThrottleScope int

const (
	// ScopeUnknown: the service did not say. The core treats it
	// conservatively as the account-wide gate of the request kind: the
	// account read gate for reads, the account write gate for writes.
	ScopeUnknown ThrottleScope = iota
	ScopeChat                  // only the chat the request addressed
	ScopeAccount               // every request of this kind for the account
	// ScopeCall: only further calls of the same method (for example
	// MarkUnread). It never blocks sends or receives.
	ScopeCall
)

func (s ThrottleScope) String() string {
	switch s {
	case ScopeChat:
		return "chat"
	case ScopeAccount:
		return "account"
	case ScopeCall:
		return "call"
	default:
		return "unknown"
	}
}

// Throttled means the service refused the request because of rate limits.
// RetryAfter is what the service asked for (capped by the adapter); the core
// adds jitter.
type Throttled struct {
	RetryAfter time.Duration
	Scope      ThrottleScope
	Err        error
}

func (e *Throttled) Error() string {
	return fmt.Sprintf("channel: throttled (%s scope, retry after %s): %s", e.Scope, e.RetryAfter, errText(e.Err))
}
func (e *Throttled) Unwrap() error { return e.Err }

// AuthRequired means the account must be signed in again.
type AuthRequired struct{ Err error }

func (e *AuthRequired) Error() string { return "channel: sign-in required: " + errText(e.Err) }
func (e *AuthRequired) Unwrap() error { return e.Err }

// WindowTooLarge is returned by Receive when the backlog behind the cursor
// is larger than the adapter will walk in one window. Nothing is returned
// and the cursor is not moved: admitting part of the window would break
// ordering. The core must surface it (a visible notice in the chat) and
// decide: retry later, or skip the backlog with SkipCursor.
type WindowTooLarge struct {
	Pages      int       // pages fetched before giving up
	Seen       int       // messages seen before giving up (a lower bound of the backlog)
	OldestSeen time.Time // oldest modification time reached; older messages were not seen
	NewestSeen time.Time // newest modification time seen
	// SkipCursor would resume after the newest message seen, skipping every
	// message not yet admitted. Only use it after telling the user.
	SkipCursor string
}

func (e *WindowTooLarge) Error() string {
	return fmt.Sprintf("channel: receive window larger than %d pages (reached %s)", e.Pages, e.OldestSeen.UTC().Format(time.RFC3339))
}

// ErrNotEditable is wrapped by Annotator.Annotate when the message cannot
// be edited without losing part of it (or is not the signed-in account's).
var ErrNotEditable = errors.New("channel: message cannot be edited safely")

// ErrNotFound is wrapped by Get and FetchFile when the object does not exist.
var ErrNotFound = errors.New("channel: not found")

func errText(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// IsAmbiguous reports whether err leaves a write's outcome unknown.
func IsAmbiguous(err error) bool {
	var a *Ambiguous
	return errors.As(err, &a)
}

// AsThrottled returns the Throttled error inside err, if any.
func AsThrottled(err error) (*Throttled, bool) {
	var t *Throttled
	ok := errors.As(err, &t)
	return t, ok
}
