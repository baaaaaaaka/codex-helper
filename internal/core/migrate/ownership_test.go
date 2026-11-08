package migrate

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
)

type conversionLease struct{ check func() error }

func (l conversionLease) Check() error  { return l.check() }
func (l conversionLease) Verify() error { return l.check() }

// Fail every ownership checkpoint in turn, including after import and fsync.
// Test the result on disk, not how many stages the implementation contains:
// an interrupted conversion is never published, and retry keeps the bindings.
func TestConversionOwnershipAtEveryCheckpoint(t *testing.T) {
	for _, version := range []string{"v0120", "rc59"} {
		for _, form := range []string{"json", "sqlite-wal"} {
			t.Run(version+"/"+form, func(t *testing.T) {
				dir := fixture(t, version, form)
				checks := 0
				owner := conversionLease{check: func() error { checks++; return nil }}
				if _, err := RunContext(t.Context(), Options{Source: dir, Lease: owner}); err != nil {
					t.Fatal(err)
				}
				if checks < 3 {
					t.Fatal("conversion did not check ownership during work and before publication")
				}
				for _, failure := range []string{"fenced", "canceled"} {
					for cut := 1; cut <= checks; cut++ {
						t.Run(fmt.Sprintf("%s/%d", failure, cut), func(t *testing.T) {
							dir := fixture(t, version, form)
							before := legacyHashes(t, dir)
							ctx, cancel := context.WithCancel(t.Context())
							defer cancel()
							n := 0
							owner := conversionLease{check: func() error {
								n++
								if n >= cut {
									if failure == "canceled" {
										cancel()
										return ctx.Err()
									}
									return lease.ErrFenced
								}
								return nil
							}}
							_, err := RunContext(ctx, Options{Source: dir, Lease: owner})
							want := lease.ErrFenced
							if failure == "canceled" {
								want = context.Canceled
							}
							if !errors.Is(err, want) {
								t.Fatalf("error %v, want %v", err, want)
							}
							for _, name := range []string{TargetName, MarkerName} {
								if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
									t.Fatalf("interrupted migration published %s: %v", name, err)
								}
							}
							if got := legacyHashes(t, dir); got != before {
								t.Fatal("interrupted conversion changed the legacy data")
							}
							rep, err := Run(Options{Source: dir})
							if err != nil || !rep.OK() {
								t.Fatalf("retry failed: %+v, %v", rep, err)
							}
							db := openResult(t, filepath.Join(dir, TargetName))
							eq(t, "original control", qs(t, db, "SELECT ext_chat_id FROM conversations WHERE kind = 'control'"), "19:control@thread.v2")
							eq(t, "original work", qs(t, db, "SELECT ext_chat_id FROM conversations WHERE label = 's001'"), "19:chat-s001@thread.v2")
							eq(t, "original thread", qs(t, db, "SELECT thread_id FROM threads WHERE conv_id = (SELECT id FROM conversations WHERE label = 's001')"), "11111111-1111-4111-8111-000000000001")
							if got := legacyHashes(t, dir); got != before {
								t.Fatal("retry changed the legacy data")
							}
						})
					}
				}
			})
		}
	}
}

func legacyHashes(t *testing.T, dir string) [3][32]byte {
	t.Helper()
	var hashes [3][32]byte
	for i, name := range []string{"state.json", "store.sqlite", "store.sqlite-wal"} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		hashes[i] = sha256.Sum256(raw)
	}
	return hashes
}

func TestConversionCanceledBeforeStartCreatesNothing(t *testing.T) {
	dir := fixture(t, "rc59", "json")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := RunContext(ctx, Options{Source: dir})
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "state.json" {
		t.Fatalf("canceled before start wrote files: %v, %v", entries, err)
	}
}
