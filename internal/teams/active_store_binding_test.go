package teams

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestActiveStoreBindingExactGenerationIsAtomicAndZeroRewrite(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	now := time.Date(2026, 7, 31, 3, 0, 0, 0, time.UTC)
	binding := ActiveStoreBinding{
		CanonicalPath:   filepath.Join(tmp, "state", "teams", "scopes", "scope-1", "state.json"),
		ScopeID:         "scope-1",
		PID:             1234,
		StartedAt:       now,
		LeaseGeneration: 9,
	}
	if err := WriteActiveStoreBinding(binding); err != nil {
		t.Fatalf("WriteActiveStoreBinding: %v", err)
	}
	path, err := ActiveStoreBindingPath()
	if err != nil {
		t.Fatalf("ActiveStoreBindingPath: %v", err)
	}
	fixedModTime := now.Add(-time.Hour)
	if err := os.Chtimes(path, fixedModTime, fixedModTime); err != nil {
		t.Fatalf("set binding sentinel mtime: %v", err)
	}
	if err := WriteActiveStoreBinding(binding); err != nil {
		t.Fatalf("repeat WriteActiveStoreBinding: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat active-store binding: %v", err)
	}
	if !info.ModTime().Equal(fixedModTime) {
		t.Fatalf("identical owner generation rewrote binding: mtime=%s want=%s", info.ModTime(), fixedModTime)
	}
	got, ok, err := LoadActiveStoreBindingReadOnly()
	if err != nil || !ok {
		t.Fatalf("LoadActiveStoreBindingReadOnly: binding=%#v ok=%t err=%v", got, ok, err)
	}
	if got != binding {
		t.Fatalf("active-store binding = %#v, want %#v", got, binding)
	}
}
