package teams

import (
	"testing"
	"time"
)

func TestDerivedCachePolicyTTLAndStaleWhileRevalidate(t *testing.T) {
	now := time.Date(2026, 4, 30, 11, 0, 0, 0, time.UTC)
	policy := DerivedCachePolicy{SchemaVersion: 3, TTL: time.Minute, StaleWhileRevalidate: 2 * time.Minute}
	source := CacheSourceSnapshot{Fingerprint: "fp-1", MTime: now.Add(-time.Hour)}
	record := DerivedCacheRecord{
		SchemaVersion:     3,
		SourceFingerprint: "fp-1",
		SourceMTime:       source.MTime,
		GeneratedAt:       now.Add(-30 * time.Second),
	}

	decision := policy.Evaluate(now, source, record)
	if !decision.UseCache || decision.Refresh || decision.Rebuild || decision.State != DerivedCacheFresh {
		t.Fatalf("fresh decision = %#v", decision)
	}

	record.GeneratedAt = now.Add(-2 * time.Minute)
	decision = policy.Evaluate(now, source, record)
	if !decision.UseCache || !decision.Refresh || decision.Rebuild || decision.State != DerivedCacheStale {
		t.Fatalf("stale-while-revalidate decision = %#v", decision)
	}

	record.GeneratedAt = now.Add(-4 * time.Minute)
	decision = policy.Evaluate(now, source, record)
	if decision.UseCache || !decision.Rebuild || decision.Reason != CacheReasonExpired {
		t.Fatalf("expired decision = %#v", decision)
	}
}

func TestDerivedCachePolicyFingerprintMismatchRebuilds(t *testing.T) {
	now := time.Date(2026, 4, 30, 11, 5, 0, 0, time.UTC)
	policy := DerivedCachePolicy{SchemaVersion: 1, TTL: time.Hour, StaleWhileRevalidate: time.Hour}
	source := CacheSourceSnapshot{Fingerprint: "fp-new", MTime: now.Add(-time.Hour)}
	record := DerivedCacheRecord{
		SchemaVersion:     1,
		SourceFingerprint: "fp-old",
		SourceMTime:       source.MTime,
		GeneratedAt:       now.Add(-time.Minute),
	}

	decision := policy.Evaluate(now, source, record)
	if decision.UseCache || !decision.Rebuild || decision.Reason != CacheReasonSourceFingerprintMismatch {
		t.Fatalf("fingerprint mismatch decision = %#v", decision)
	}
}

func TestDerivedCachePolicyMTimeChangeActiveSessionRefreshes(t *testing.T) {
	now := time.Date(2026, 4, 30, 11, 10, 0, 0, time.UTC)
	policy := DerivedCachePolicy{SchemaVersion: 1, TTL: time.Minute, StaleWhileRevalidate: 5 * time.Minute}
	recordMTime := now.Add(-time.Hour)
	record := DerivedCacheRecord{
		SchemaVersion:     1,
		SourceFingerprint: "fp-1",
		SourceMTime:       recordMTime,
		GeneratedAt:       now.Add(-30 * time.Second),
	}
	source := CacheSourceSnapshot{
		Fingerprint:   "fp-1",
		MTime:         recordMTime.Add(time.Second),
		ActiveSession: true,
	}

	decision := policy.Evaluate(now, source, record)
	if !decision.UseCache || !decision.Refresh || decision.Rebuild || decision.Reason != CacheReasonActiveSessionMTimeChanged {
		t.Fatalf("active-session mtime decision = %#v", decision)
	}

	source.ActiveSession = false
	decision = policy.Evaluate(now, source, record)
	if decision.UseCache || !decision.Rebuild || decision.Reason != CacheReasonSourceMTimeChanged {
		t.Fatalf("inactive mtime decision = %#v", decision)
	}
}

func TestDerivedCachePolicyBadCacheRebuildsAndCheckpointIsIndependent(t *testing.T) {
	now := time.Date(2026, 4, 30, 11, 15, 0, 0, time.UTC)
	policy := DerivedCachePolicy{SchemaVersion: 1, TTL: time.Hour, StaleWhileRevalidate: time.Hour}
	source := CacheSourceSnapshot{Fingerprint: "fp-1", MTime: now.Add(-time.Hour)}
	record := DerivedCacheRecord{
		SchemaVersion:     1,
		SourceFingerprint: "fp-1",
		SourceMTime:       source.MTime,
		GeneratedAt:       now.Add(-time.Minute),
		InvalidReason:     "json parse failed",
	}

	cacheDecision := policy.Evaluate(now, source, record)
	if cacheDecision.UseCache || !cacheDecision.Rebuild || cacheDecision.Reason != CacheReasonBadCache {
		t.Fatalf("bad cache decision = %#v", cacheDecision)
	}

	checkpointDecision := EvaluateDurableCheckpoint(DurableCheckpoint{
		Key:      "graph-poll",
		SourceID: "chat-1",
		Cursor:   "2026-04-30T11:15:00Z",
	})
	if !checkpointDecision.UseCheckpoint {
		t.Fatalf("durable checkpoint should remain usable despite bad derived cache: %#v", checkpointDecision)
	}
}

func TestDerivedCachePolicySchemaMismatchRebuilds(t *testing.T) {
	now := time.Date(2026, 4, 30, 11, 20, 0, 0, time.UTC)
	policy := DerivedCachePolicy{SchemaVersion: 2, TTL: time.Hour, StaleWhileRevalidate: time.Hour}
	source := CacheSourceSnapshot{Fingerprint: "fp-1", MTime: now.Add(-time.Hour)}
	record := DerivedCacheRecord{
		SchemaVersion:     1,
		SourceFingerprint: "fp-1",
		SourceMTime:       source.MTime,
		GeneratedAt:       now.Add(-time.Minute),
	}

	decision := policy.Evaluate(now, source, record)
	if decision.UseCache || !decision.Rebuild || decision.Reason != CacheReasonSchemaMismatch {
		t.Fatalf("schema mismatch decision = %#v", decision)
	}
}
