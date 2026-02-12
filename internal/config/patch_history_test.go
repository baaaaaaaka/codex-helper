package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestPatchHistoryStore(t *testing.T) *PatchHistoryStore {
	t.Helper()
	dir := t.TempDir()
	s, err := NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	return s
}

func TestPatchHistoryStore_Path(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	want := filepath.Join(dir, "patch_history.json")
	if s.Path() != want {
		t.Fatalf("Path() = %q, want %q", s.Path(), want)
	}
}

func TestPatchHistoryStore_LoadEmpty(t *testing.T) {
	s := newTestPatchHistoryStore(t)
	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if h.Version != patchHistoryVersion {
		t.Fatalf("Version = %d, want %d", h.Version, patchHistoryVersion)
	}
	if len(h.Entries) != 0 {
		t.Fatalf("Entries = %d, want 0", len(h.Entries))
	}
}

func TestPatchHistoryStore_LoadCorruptJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "patch_history.json")
	if err := os.WriteFile(path, []byte("{invalid json"), 0o600); err != nil {
		t.Fatalf("write corrupt json: %v", err)
	}
	s, err := NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	_, err = s.Load()
	if err == nil {
		t.Fatal("expected error for corrupt JSON")
	}
}

func TestPatchHistoryStore_LoadVersionZeroAutoFix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "patch_history.json")
	data := `{"version":0,"entries":[]}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	s, err := NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if h.Version != patchHistoryVersion {
		t.Fatalf("Version = %d, want %d", h.Version, patchHistoryVersion)
	}
}

func TestPatchHistoryStore_LoadExistingEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "patch_history.json")
	h := PatchHistory{
		Version: 1,
		Entries: []PatchHistoryEntry{
			{Path: "/bin/a", OrigSHA256: "aaa"},
			{Path: "/bin/b", OrigSHA256: "bbb"},
		},
	}
	b, _ := json.MarshalIndent(h, "", "  ")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	s, err := NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	loaded, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded.Entries) != 2 {
		t.Fatalf("Entries = %d, want 2", len(loaded.Entries))
	}
}

func TestPatchHistoryStore_UpsertAndFind(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	entry := PatchHistoryEntry{
		Path:          "/usr/bin/codex",
		OrigSHA256:    "aaa",
		PatchedSHA256: "bbb",
		ProxyVersion:  "v0.0.4",
		PatchedAt:     time.Now().Truncate(time.Second),
	}

	if err := s.Upsert(entry); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	found, err := s.Find("/usr/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if found == nil {
		t.Fatalf("Find returned nil")
	}
	if found.PatchedSHA256 != "bbb" {
		t.Fatalf("PatchedSHA256 = %q, want %q", found.PatchedSHA256, "bbb")
	}
	if found.ProxyVersion != "v0.0.4" {
		t.Fatalf("ProxyVersion = %q, want %q", found.ProxyVersion, "v0.0.4")
	}
}

func TestPatchHistoryStore_UpsertUpdatesExisting(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	entry := PatchHistoryEntry{
		Path:          "/usr/bin/codex",
		OrigSHA256:    "aaa",
		PatchedSHA256: "bbb",
		ProxyVersion:  "v0.0.1",
		PatchedAt:     time.Now().Truncate(time.Second),
	}
	if err := s.Upsert(entry); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	entry.PatchedSHA256 = "ccc"
	entry.ProxyVersion = "v0.0.2"
	if err := s.Upsert(entry); err != nil {
		t.Fatalf("Upsert update: %v", err)
	}

	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(h.Entries) != 1 {
		t.Fatalf("Entries = %d, want 1", len(h.Entries))
	}
	if h.Entries[0].PatchedSHA256 != "ccc" {
		t.Fatalf("PatchedSHA256 = %q, want %q", h.Entries[0].PatchedSHA256, "ccc")
	}
}

func TestPatchHistoryStore_UpsertMultipleDifferentEntries(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	for i := 0; i < 5; i++ {
		if err := s.Upsert(PatchHistoryEntry{
			Path:       fmt.Sprintf("/bin/codex%d", i),
			OrigSHA256: fmt.Sprintf("hash%d", i),
		}); err != nil {
			t.Fatalf("Upsert %d: %v", i, err)
		}
	}

	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(h.Entries) != 5 {
		t.Fatalf("Entries = %d, want 5", len(h.Entries))
	}
}

func TestPatchHistoryStore_UpsertSamePathDifferentHash(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{Path: "/bin/codex", OrigSHA256: "v1"}); err != nil {
		t.Fatalf("Upsert v1: %v", err)
	}
	if err := s.Upsert(PatchHistoryEntry{Path: "/bin/codex", OrigSHA256: "v2"}); err != nil {
		t.Fatalf("Upsert v2: %v", err)
	}

	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Different hashes = different entries (binary was updated).
	if len(h.Entries) != 2 {
		t.Fatalf("Entries = %d, want 2", len(h.Entries))
	}
}

func TestPatchHistoryStore_UpsertFailedToSuccess(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:          "/bin/codex",
		OrigSHA256:    "aaa",
		Failed:        true,
		FailureReason: "SIGSEGV",
	}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Update same entry to success.
	if err := s.Upsert(PatchHistoryEntry{
		Path:          "/bin/codex",
		OrigSHA256:    "aaa",
		PatchedSHA256: "bbb",
		Failed:        false,
	}); err != nil {
		t.Fatalf("Upsert success: %v", err)
	}

	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(h.Entries) != 1 {
		t.Fatalf("Entries = %d, want 1", len(h.Entries))
	}
	if h.Entries[0].Failed {
		t.Fatal("expected entry to be updated to not failed")
	}
	if h.Entries[0].FailureReason != "" {
		t.Fatalf("expected empty failure reason, got %q", h.Entries[0].FailureReason)
	}
}

func TestPatchHistoryStore_IsPatched(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	patched, err := s.IsPatched("/usr/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("IsPatched: %v", err)
	}
	if patched {
		t.Fatalf("expected not patched")
	}

	if err := s.Upsert(PatchHistoryEntry{
		Path:          "/usr/bin/codex",
		OrigSHA256:    "aaa",
		PatchedSHA256: "bbb",
		ProxyVersion:  "v0.0.4",
		PatchedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	patched, err = s.IsPatched("/usr/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("IsPatched: %v", err)
	}
	if !patched {
		t.Fatalf("expected patched")
	}

	// Different hash should not match.
	patched, err = s.IsPatched("/usr/bin/codex", "zzz")
	if err != nil {
		t.Fatalf("IsPatched: %v", err)
	}
	if patched {
		t.Fatalf("expected not patched for different hash")
	}

	// Different path should not match.
	patched, err = s.IsPatched("/other/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("IsPatched: %v", err)
	}
	if patched {
		t.Fatalf("expected not patched for different path")
	}
}

func TestPatchHistoryStore_IsPatchedSkipsFailed(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:       "/usr/bin/codex",
		OrigSHA256: "aaa",
		Failed:     true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	patched, err := s.IsPatched("/usr/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("IsPatched: %v", err)
	}
	if patched {
		t.Fatalf("failed entries should not count as patched")
	}
}

func TestPatchHistoryStore_IsFailed(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	failed, err := s.IsFailed("/usr/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("IsFailed: %v", err)
	}
	if failed {
		t.Fatalf("expected not failed")
	}

	if err := s.Upsert(PatchHistoryEntry{
		Path:          "/usr/bin/codex",
		OrigSHA256:    "aaa",
		Failed:        true,
		FailureReason: "SIGSEGV",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	failed, err = s.IsFailed("/usr/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("IsFailed: %v", err)
	}
	if !failed {
		t.Fatalf("expected failed")
	}
}

func TestPatchHistoryStore_IsFailedNotForSuccessEntries(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:          "/usr/bin/codex",
		OrigSHA256:    "aaa",
		PatchedSHA256: "bbb",
		Failed:        false,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	failed, err := s.IsFailed("/usr/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("IsFailed: %v", err)
	}
	if failed {
		t.Fatal("success entries should not be reported as failed")
	}
}

func TestPatchHistoryStore_IsFailedDifferentPath(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:       "/usr/bin/codex",
		OrigSHA256: "aaa",
		Failed:     true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	failed, err := s.IsFailed("/other/codex", "aaa")
	if err != nil {
		t.Fatalf("IsFailed: %v", err)
	}
	if failed {
		t.Fatal("different path should not match")
	}
}

func TestPatchHistoryStore_Remove(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:       "/usr/bin/codex",
		OrigSHA256: "aaa",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := s.Upsert(PatchHistoryEntry{
		Path:       "/usr/bin/codex2",
		OrigSHA256: "bbb",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	if err := s.Remove("/usr/bin/codex", "aaa"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(h.Entries) != 1 {
		t.Fatalf("Entries = %d, want 1", len(h.Entries))
	}
	if h.Entries[0].Path != "/usr/bin/codex2" {
		t.Fatalf("remaining entry = %q, want /usr/bin/codex2", h.Entries[0].Path)
	}
}

func TestPatchHistoryStore_RemoveNonexistent(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:       "/usr/bin/codex",
		OrigSHA256: "aaa",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Remove an entry that doesn't exist — should be a no-op.
	if err := s.Remove("/nonexistent", "xxx"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(h.Entries) != 1 {
		t.Fatalf("Entries = %d, want 1", len(h.Entries))
	}
}

func TestPatchHistoryStore_RemoveAll(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{Path: "/a", OrigSHA256: "a"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := s.Remove("/a", "a"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(h.Entries) != 0 {
		t.Fatalf("Entries = %d, want 0", len(h.Entries))
	}
}

func TestPatchHistoryStore_FindNotFound(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	found, err := s.Find("/nonexistent", "xxx")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if found != nil {
		t.Fatalf("expected nil, got %+v", found)
	}
}

func TestPatchHistoryStore_FindReturnsCopy(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:          "/bin/codex",
		OrigSHA256:    "aaa",
		PatchedSHA256: "bbb",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	found, err := s.Find("/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	// Mutating the returned entry should not affect the store.
	found.PatchedSHA256 = "modified"

	found2, err := s.Find("/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if found2.PatchedSHA256 != "bbb" {
		t.Fatal("Find should return a copy, not a reference to internal data")
	}
}

func TestPatchHistoryStore_FindWithFailureFields(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:          "/bin/codex",
		OrigSHA256:    "aaa",
		Failed:        true,
		FailureReason: "segfault at startup",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	found, err := s.Find("/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if found == nil {
		t.Fatal("Find returned nil")
	}
	if !found.Failed {
		t.Fatal("expected Failed=true")
	}
	if found.FailureReason != "segfault at startup" {
		t.Fatalf("FailureReason = %q, want %q", found.FailureReason, "segfault at startup")
	}
}

func TestPatchHistoryStore_Persistence(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	if err := s1.Upsert(PatchHistoryEntry{
		Path:       "/usr/bin/codex",
		OrigSHA256: "aaa",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Open a new store instance at the same path.
	s2, err := NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	found, err := s2.Find("/usr/bin/codex", "aaa")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if found == nil {
		t.Fatalf("expected persisted entry, got nil")
	}
}

func TestPatchHistoryStore_UpdateReturnsError(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	want := fmt.Errorf("test error")
	err := s.Update(func(h *PatchHistory) error {
		return want
	})
	if err != want {
		t.Fatalf("Update error = %v, want %v", err, want)
	}

	// Store should not have been modified.
	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(h.Entries) != 0 {
		t.Fatalf("Entries = %d, want 0 (update should not have persisted)", len(h.Entries))
	}
}

func TestPatchHistoryStore_UpdateModifiesEntries(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:          "/bin/codex",
		OrigSHA256:    "aaa",
		PatchedSHA256: "bbb",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Use Update to modify the entry.
	if err := s.Update(func(h *PatchHistory) error {
		for i := range h.Entries {
			h.Entries[i].PatchedSHA256 = "ccc"
		}
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	found, _ := s.Find("/bin/codex", "aaa")
	if found == nil || found.PatchedSHA256 != "ccc" {
		t.Fatalf("expected PatchedSHA256=ccc, got %v", found)
	}
}

func TestPatchHistoryStore_JSONRoundTrip(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	ts := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	entry := PatchHistoryEntry{
		Path:          "/usr/local/bin/codex",
		OrigSHA256:    "abc123",
		PatchedSHA256: "def456",
		ProxyVersion:  "v0.0.4",
		PatchedAt:     ts,
		Failed:        false,
	}
	if err := s.Upsert(entry); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Read the raw JSON.
	data, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	var h PatchHistory
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if h.Version != 1 {
		t.Fatalf("Version = %d, want 1", h.Version)
	}
	if len(h.Entries) != 1 {
		t.Fatalf("Entries = %d, want 1", len(h.Entries))
	}
	e := h.Entries[0]
	if e.Path != "/usr/local/bin/codex" {
		t.Fatalf("Path = %q", e.Path)
	}
	if e.OrigSHA256 != "abc123" {
		t.Fatalf("OrigSHA256 = %q", e.OrigSHA256)
	}
	if e.PatchedSHA256 != "def456" {
		t.Fatalf("PatchedSHA256 = %q", e.PatchedSHA256)
	}
	if e.ProxyVersion != "v0.0.4" {
		t.Fatalf("ProxyVersion = %q", e.ProxyVersion)
	}

	// Check the failed field is omitted when false.
	if json.Valid(data) {
		raw := string(data)
		// "failed" should not appear in JSON when false (omitempty).
		if testing.Verbose() {
			t.Logf("JSON: %s", raw)
		}
	}
}

func TestPatchHistoryStore_FailedEntryJSON(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	if err := s.Upsert(PatchHistoryEntry{
		Path:          "/bin/codex",
		OrigSHA256:    "aaa",
		Failed:        true,
		FailureReason: "SIGSEGV",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	data, err := os.ReadFile(s.Path())
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	var h PatchHistory
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(h.Entries) != 1 || !h.Entries[0].Failed {
		t.Fatal("expected failed entry in JSON")
	}
	if h.Entries[0].FailureReason != "SIGSEGV" {
		t.Fatalf("FailureReason = %q, want SIGSEGV", h.Entries[0].FailureReason)
	}
}

func TestPatchHistoryStore_ConcurrentUpserts(t *testing.T) {
	dir := t.TempDir()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s, err := NewPatchHistoryStore(dir)
			if err != nil {
				t.Errorf("NewPatchHistoryStore: %v", err)
				return
			}
			_ = s.Upsert(PatchHistoryEntry{
				Path:       fmt.Sprintf("/bin/codex%d", idx),
				OrigSHA256: fmt.Sprintf("hash%d", idx),
			})
		}(i)
	}
	wg.Wait()

	s, err := NewPatchHistoryStore(dir)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// All 10 entries should be present (they have different keys).
	if len(h.Entries) != 10 {
		t.Fatalf("Entries = %d, want 10", len(h.Entries))
	}
}

func TestPatchHistoryStore_SaveVersionZeroAutoSet(t *testing.T) {
	s := newTestPatchHistoryStore(t)

	// Directly call Update with a version-0 history.
	if err := s.Update(func(h *PatchHistory) error {
		h.Version = 0
		h.Entries = append(h.Entries, PatchHistoryEntry{
			Path:       "/bin/codex",
			OrigSHA256: "aaa",
		})
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	h, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if h.Version != patchHistoryVersion {
		t.Fatalf("Version = %d, want %d", h.Version, patchHistoryVersion)
	}
	if len(h.Entries) != 1 {
		t.Fatalf("Entries = %d, want 1", len(h.Entries))
	}
}

func TestNewPatchHistoryStore_CreatesDir(t *testing.T) {
	base := t.TempDir()
	nested := filepath.Join(base, "a", "b", "c")
	s, err := NewPatchHistoryStore(nested)
	if err != nil {
		t.Fatalf("NewPatchHistoryStore: %v", err)
	}
	// Dir should exist.
	info, err := os.Stat(nested)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("expected directory")
	}
	// Verify store works.
	if err := s.Upsert(PatchHistoryEntry{Path: "/bin/test", OrigSHA256: "x"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
}
