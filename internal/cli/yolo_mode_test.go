package cli

import "testing"

func TestResolveYoloEnabledDefaultsFalse(t *testing.T) {
	store := newTempStore(t)
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if resolveYoloEnabled(cfg) {
		t.Fatalf("expected yolo disabled by default")
	}
}

func TestPersistYoloEnabledStoresValue(t *testing.T) {
	store := newTempStore(t)
	if err := persistYoloEnabled(store, true); err != nil {
		t.Fatalf("persist yolo: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !resolveYoloEnabled(cfg) {
		t.Fatalf("expected yolo enabled from config")
	}
}

func TestPersistYoloEnabledRoundTrip(t *testing.T) {
	store := newTempStore(t)

	if err := persistYoloEnabled(store, true); err != nil {
		t.Fatalf("persist true: %v", err)
	}
	cfg, _ := store.Load()
	if !resolveYoloEnabled(cfg) {
		t.Fatalf("expected true after persist(true)")
	}

	if err := persistYoloEnabled(store, false); err != nil {
		t.Fatalf("persist false: %v", err)
	}
	cfg, _ = store.Load()
	if resolveYoloEnabled(cfg) {
		t.Fatalf("expected false after persist(false)")
	}
}
