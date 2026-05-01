package teams

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

type DerivedCacheState string

const (
	DerivedCacheFresh   DerivedCacheState = "fresh"
	DerivedCacheStale   DerivedCacheState = "stale"
	DerivedCacheRebuild DerivedCacheState = "rebuild"
)

const (
	CacheReasonFresh                     = "fresh"
	CacheReasonStaleWhileRevalidate      = "stale_while_revalidate"
	CacheReasonBadCache                  = "bad_cache"
	CacheReasonMissingCache              = "missing_cache"
	CacheReasonSchemaMismatch            = "schema_mismatch"
	CacheReasonSourceFingerprintMissing  = "source_fingerprint_missing"
	CacheReasonSourceFingerprintMismatch = "source_fingerprint_mismatch"
	CacheReasonSourceMTimeMissing        = "source_mtime_missing"
	CacheReasonSourceMTimeChanged        = "source_mtime_changed"
	CacheReasonActiveSessionMTimeChanged = "active_session_mtime_changed"
	CacheReasonExpired                   = "expired"
)

type DerivedCachePolicy struct {
	SchemaVersion        int
	TTL                  time.Duration
	StaleWhileRevalidate time.Duration
}

type CacheSourceSnapshot struct {
	Fingerprint   string
	MTime         time.Time
	ActiveSession bool
}

type DerivedCacheRecord struct {
	SchemaVersion     int
	SourceFingerprint string
	SourceMTime       time.Time
	GeneratedAt       time.Time
	InvalidReason     string
}

type DerivedCacheDecision struct {
	State     DerivedCacheState
	UseCache  bool
	Refresh   bool
	Rebuild   bool
	Reason    string
	Stale     bool
	ExpiresAt time.Time
	StaleAt   time.Time
}

type DurableCheckpoint struct {
	Key       string
	SourceID  string
	Cursor    string
	Sequence  int64
	UpdatedAt time.Time
}

type DurableCheckpointDecision struct {
	UseCheckpoint bool
	Reason        string
}

func (p DerivedCachePolicy) Evaluate(now time.Time, source CacheSourceSnapshot, record DerivedCacheRecord) DerivedCacheDecision {
	schema := p.SchemaVersion
	if schema <= 0 {
		schema = 1
	}
	expiresAt, staleAt := p.cacheDeadlines(record.GeneratedAt)
	if strings.TrimSpace(record.InvalidReason) != "" {
		return rebuildDecision(CacheReasonBadCache, expiresAt, staleAt)
	}
	if record.SchemaVersion == 0 || record.GeneratedAt.IsZero() {
		return rebuildDecision(CacheReasonMissingCache, expiresAt, staleAt)
	}
	if record.SchemaVersion != schema {
		return rebuildDecision(CacheReasonSchemaMismatch, expiresAt, staleAt)
	}
	sourceFingerprint := strings.TrimSpace(source.Fingerprint)
	recordFingerprint := strings.TrimSpace(record.SourceFingerprint)
	if sourceFingerprint == "" || recordFingerprint == "" {
		return rebuildDecision(CacheReasonSourceFingerprintMissing, expiresAt, staleAt)
	}
	if sourceFingerprint != recordFingerprint {
		return rebuildDecision(CacheReasonSourceFingerprintMismatch, expiresAt, staleAt)
	}
	if !source.MTime.IsZero() {
		if record.SourceMTime.IsZero() {
			return rebuildDecision(CacheReasonSourceMTimeMissing, expiresAt, staleAt)
		}
		if !record.SourceMTime.Equal(source.MTime) {
			if source.ActiveSession && p.canServeStale(now, record.GeneratedAt) {
				return staleDecision(CacheReasonActiveSessionMTimeChanged, expiresAt, staleAt)
			}
			return rebuildDecision(CacheReasonSourceMTimeChanged, expiresAt, staleAt)
		}
	}
	if p.isFresh(now, record.GeneratedAt) {
		return DerivedCacheDecision{
			State:     DerivedCacheFresh,
			UseCache:  true,
			Reason:    CacheReasonFresh,
			ExpiresAt: expiresAt,
			StaleAt:   staleAt,
		}
	}
	if p.canServeStale(now, record.GeneratedAt) {
		return staleDecision(CacheReasonStaleWhileRevalidate, expiresAt, staleAt)
	}
	return rebuildDecision(CacheReasonExpired, expiresAt, staleAt)
}

func CacheSourceFingerprint(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(strconv.Itoa(len(part))))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0xff})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func EvaluateDurableCheckpoint(checkpoint DurableCheckpoint) DurableCheckpointDecision {
	if !checkpoint.Usable() {
		return DurableCheckpointDecision{Reason: "missing_checkpoint"}
	}
	return DurableCheckpointDecision{UseCheckpoint: true, Reason: "usable"}
}

func (c DurableCheckpoint) Usable() bool {
	return strings.TrimSpace(c.Key) != "" &&
		(strings.TrimSpace(c.Cursor) != "" || c.Sequence > 0)
}

func (p DerivedCachePolicy) isFresh(now time.Time, generatedAt time.Time) bool {
	if generatedAt.IsZero() || p.TTL <= 0 {
		return false
	}
	age := now.Sub(generatedAt)
	if age < 0 {
		age = 0
	}
	return age <= p.TTL
}

func (p DerivedCachePolicy) canServeStale(now time.Time, generatedAt time.Time) bool {
	if generatedAt.IsZero() {
		return false
	}
	ttl := p.TTL
	if ttl < 0 {
		ttl = 0
	}
	swr := p.StaleWhileRevalidate
	if swr <= 0 {
		return p.isFresh(now, generatedAt)
	}
	age := now.Sub(generatedAt)
	if age < 0 {
		age = 0
	}
	return age <= ttl+swr
}

func (p DerivedCachePolicy) cacheDeadlines(generatedAt time.Time) (time.Time, time.Time) {
	if generatedAt.IsZero() {
		return time.Time{}, time.Time{}
	}
	expiresAt := generatedAt
	if p.TTL > 0 {
		expiresAt = generatedAt.Add(p.TTL)
	}
	staleAt := expiresAt
	if p.StaleWhileRevalidate > 0 {
		staleAt = expiresAt.Add(p.StaleWhileRevalidate)
	}
	return expiresAt, staleAt
}

func staleDecision(reason string, expiresAt time.Time, staleAt time.Time) DerivedCacheDecision {
	return DerivedCacheDecision{
		State:     DerivedCacheStale,
		UseCache:  true,
		Refresh:   true,
		Reason:    reason,
		Stale:     true,
		ExpiresAt: expiresAt,
		StaleAt:   staleAt,
	}
}

func rebuildDecision(reason string, expiresAt time.Time, staleAt time.Time) DerivedCacheDecision {
	return DerivedCacheDecision{
		State:     DerivedCacheRebuild,
		Rebuild:   true,
		Reason:    reason,
		ExpiresAt: expiresAt,
		StaleAt:   staleAt,
	}
}
