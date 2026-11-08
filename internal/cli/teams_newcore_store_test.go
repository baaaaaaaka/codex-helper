package cli

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	"github.com/gofrs/flock"
)

// Reproduce the production sequence with real leases: a failed migration,
// an aborted first claim, and a new process planning the same store again.
// No Teams, service manager, or Codex process is involved.
func TestTeamsCoreStoreResumesMigrationAfterAbortedStart(t *testing.T) {
	t.Setenv(envTeamsCoreStore, "")
	for _, tc := range []struct{ version, form string }{
		{"v0120", "json"}, {"v0120", "sqlite-wal"},
		{"rc59", "json"}, {"rc59", "sqlite-wal"},
	} {
		t.Run(tc.version+"/"+tc.form, func(t *testing.T) {
			root, scope := migrateFixture(t, tc.version, tc.form)
			anchor, err := planTeamsCoreStore(root)
			if err != nil {
				t.Fatal(err)
			}
			cfg := lease.Config{Anchor: anchor, Self: lease.Local("fixture-host", teamsBootID())}
			first, err := lease.Acquire(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = first.Release() })
			lock := flock.New(filepath.Join(scope, "state.json.lock"))
			if err := lock.Lock(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lock.Unlock() })
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err = prepareTeamsCoreStore(ctx, first.Plan(), first, "", io.Discard)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("blocked conversion: %v", err)
			}
			if err := lock.Unlock(); err != nil {
				t.Fatal(err)
			}
			if err := first.Release(); err != nil {
				t.Fatal(err)
			}
			info, err := lease.Read(anchor)
			// Aborted claims remain on disk but are not a published head.
			if err != nil || !info.Exists || info.CurrentGen != 0 || info.HeadGen != 0 {
				t.Fatalf("unfinished first start: %+v, %v", info, err)
			}
			if _, err := os.Stat(anchor); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("database after failure: %v", err)
			}

			// Read-only discovery must not call this an already converted store.
			before := dirHashes(t, scope)
			_, err = resolveTeamsCoreStorePath(root)
			var unconverted errTeamsStoreNotConverted
			if !errors.As(err, &unconverted) || unconverted.source != scope {
				t.Fatalf("status: %v", err)
			}
			afterRead := dirHashes(t, scope)
			if len(afterRead) != len(before) {
				t.Fatal("read-only discovery added or removed files")
			}
			for name, hash := range before {
				if got := afterRead[name]; got != hash {
					t.Fatalf("read-only discovery changed %s", name)
				}
			}
			again, err := planTeamsCoreStore(root)
			if err != nil || again != anchor {
				t.Fatalf("second plan: %q, %v", again, err)
			}
			second, err := lease.Acquire(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Release()
			if err := second.WaitQuiet(context.Background()); err != nil {
				t.Fatal(err)
			}
			st, err := openTeamsCoreStore(context.Background(), second, "", io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			convs, err := st.Conversations()
			if err != nil {
				t.Fatal(err)
			}
			threads, err := st.Threads()
			if err != nil {
				t.Fatal(err)
			}
			var control, work int64
			for _, c := range convs {
				if c.Label == "control" && c.ExtChatID == "19:control@thread.v2" {
					control = c.ID
				}
				if c.Label == "s001" && c.ExtChatID == "19:chat-s001@thread.v2" {
					work = c.ID
				}
			}
			if control == 0 || work == 0 {
				t.Fatalf("original chat bindings lost: %+v", convs)
			}
			bound := false
			for _, th := range threads {
				if th.ConvID == work && th.ThreadID == "11111111-1111-4111-8111-000000000001" {
					bound = true
				}
			}
			if !bound {
				t.Fatalf("original Codex thread lost: %+v", threads)
			}
			streams, err := st.Streams()
			if err != nil {
				t.Fatal(err)
			}
			seeded := false
			for _, stream := range streams {
				if stream.ConvID == work && stream.Seeded && stream.Cursor == "2026-09-01T10:00:05Z" {
					seeded = true
				}
			}
			if !seeded {
				t.Fatalf("original receive cursor lost: %+v", streams)
			}
		})
	}
}

type teamsStoreTestWriter func([]byte) (int, error)

func (w teamsStoreTestWriter) Write(p []byte) (int, error) { return w(p) }

// The log write happens after the startup ownership check. Losing ownership
// there must reach the converter too, rather than only the final store open.
func TestTeamsCoreStoreLossAfterPreparationStarts(t *testing.T) {
	for _, loss := range []string{"released", "canceled"} {
		t.Run(loss, func(t *testing.T) {
			_, scope := migrateFixture(t, "rc59", "json")
			path := filepath.Join(scope, migrate.TargetName)
			holder, err := lease.Acquire(t.Context(), lease.Config{Anchor: path, Self: lease.Local("test-host", teamsBootID())})
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Release()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started := false
			log := teamsStoreTestWriter(func(p []byte) (int, error) {
				if strings.Contains(string(p), "Converting the Teams store") {
					started = true
					if loss == "released" {
						if err := holder.Release(); err != nil {
							t.Fatal(err)
						}
					} else {
						cancel()
					}
				}
				return len(p), nil
			})
			st, err := openTeamsCoreStore(ctx, holder, "", log)
			if st != nil {
				st.Close()
			}
			want := lease.ErrReleased
			if loss == "canceled" {
				want = context.Canceled
			}
			if !started || !errors.Is(err, want) {
				t.Fatalf("started=%v, err=%v, want %v", started, err, want)
			}
			for _, name := range []string{migrate.TargetName, migrate.MarkerName, migrate.ReportName} {
				if _, err := os.Stat(filepath.Join(scope, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("lost startup published %s: %v", name, err)
				}
			}
		})
	}
}

func TestTeamsCoreStorePreparationRefusesMissingPublishedStore(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "core.sqlite")
	plan := lease.Plan{FromGen: 1, InPlace: true, Src: path, DB: path}
	if err := prepareTeamsCoreStore(context.Background(), plan, nil, "", io.Discard); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing published store: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty replacement created: %v", err)
	}
}

func TestTeamsCoreStorePublishesOnlyAfterOpeningDatabase(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "failed open"}[corrupt], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "core.sqlite")
			if corrupt {
				writeFile(t, path, "not a database")
			}
			holder, err := lease.Acquire(context.Background(), lease.Config{Anchor: path, Self: lease.Local("test-host", teamsBootID())})
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Release()
			st, err := openTeamsCoreStore(context.Background(), holder, "", io.Discard)
			if corrupt {
				if err == nil {
					st.Close()
					t.Fatal("corrupt database opened")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer st.Close()
				if _, err := st.Conversations(); err != nil {
					t.Fatal(err)
				}
			}
			info, err := lease.Read(path)
			if err != nil {
				t.Fatal(err)
			}
			if (info.CurrentGen != 0) == corrupt {
				t.Fatalf("database published=%v, failed open=%v", info.CurrentGen != 0, corrupt)
			}
		})
	}
}

func TestTeamsCoreStoreDoesNotWriteAfterLeaseLoss(t *testing.T) {
	for _, source := range []string{"fresh", "legacy"} {
		for _, loss := range []string{"released", "fenced", "canceled"} {
			t.Run(source+"/"+loss, func(t *testing.T) {
				dir := t.TempDir()
				if source == "legacy" {
					_, dir = migrateFixture(t, "v0120", "json")
				}
				path := filepath.Join(dir, "core.sqlite")
				cfg := lease.Config{Anchor: path, Self: lease.Local("test-host", teamsBootID())}
				holder, err := lease.Acquire(t.Context(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer holder.Release()
				if err := holder.WaitQuiet(t.Context()); err != nil {
					t.Fatal(err)
				}
				// Ownership can end after WaitQuiet, before preparation or open.
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				want := lease.ErrReleased
				if loss == "canceled" {
					cancel()
					want = context.Canceled
				} else if loss == "released" {
					if err := holder.Release(); err != nil {
						t.Fatal(err)
					}
				} else {
					// Use the lease's process-liveness seam to claim a real later
					// generation without waiting for production timeouts.
					cfg.Self.PID++
					cfg.Alive = func(int) bool { return false }
					cfg.Immediate = true
					successor, err := lease.Acquire(t.Context(), cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer successor.Release()
					want = lease.ErrFenced
				}
				before := dirHashes(t, dir)
				// A successor may renew its lease while we check the old owner.
				delete(before, filepath.Base(lease.Path(path)))
				st, err := openTeamsCoreStore(ctx, holder, "", io.Discard)
				if st != nil {
					st.Close()
				}
				if !errors.Is(err, want) {
					t.Fatalf("lost lease: %v, want %v", err, want)
				}
				after := dirHashes(t, dir)
				delete(after, filepath.Base(lease.Path(path)))
				if len(before) != len(after) {
					t.Fatal("a process without the lease added or removed files")
				}
				for name, hash := range before {
					if after[name] != hash {
						t.Fatalf("a process without the lease changed %s", name)
					}
				}
			})
		}
	}
}

func TestTeamsCoreStoreRefusesMigrationToAnotherFilename(t *testing.T) {
	_, scope := migrateFixture(t, "v0120", "json")
	path := filepath.Join(scope, "custom.sqlite")
	before := dirHashes(t, scope)
	err := prepareTeamsCoreStore(context.Background(), lease.Plan{InPlace: true, Src: path, DB: path}, nil, "", io.Discard)
	if err == nil {
		t.Fatal("conversion would write core.sqlite but startup would open an empty custom.sqlite")
	}
	after := dirHashes(t, scope)
	if len(before) != len(after) {
		t.Fatal("refused migration changed files")
	}
	for name, hash := range before {
		if after[name] != hash {
			t.Fatalf("refused migration changed %s", name)
		}
	}
}

func TestTeamsCoreStorePreparationAfterMigrationBeforeActivation(t *testing.T) {
	t.Setenv(envTeamsCoreStore, "")
	for _, keepMarker := range []bool{true, false} {
		t.Run(map[bool]string{true: "with marker", false: "rename before marker"}[keepMarker], func(t *testing.T) {
			root, scope := migrateFixture(t, "v0120", "json")
			anchor, err := planTeamsCoreStore(root)
			if err != nil {
				t.Fatal(err)
			}
			first, err := lease.Acquire(context.Background(), lease.Config{Anchor: anchor, Self: lease.Local("first-host", teamsBootID())})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = first.Release() })
			if err := prepareTeamsCoreStore(context.Background(), first.Plan(), first, "", io.Discard); err != nil {
				t.Fatal(err)
			}
			// A crash can happen after the database rename but before the marker.
			if !keepMarker {
				if err := os.Remove(filepath.Join(scope, migrate.MarkerName)); err != nil {
					t.Fatal(err)
				}
			}

			// Conversion committed but this process never activated its lease.
			if err := first.Release(); err != nil {
				t.Fatal(err)
			}
			second, err := lease.Acquire(context.Background(), lease.Config{Anchor: anchor, Self: lease.Local("second-host", "other-boot")})
			if err != nil {
				t.Fatal(err)
			}
			defer second.Release()
			if err := second.WaitQuiet(context.Background()); err != nil {
				t.Fatal(err)
			}
			before := dirHashes(t, scope)
			if err := prepareTeamsCoreStore(context.Background(), second.Plan(), second, "", io.Discard); err != nil {
				t.Fatal(err)
			}
			after := dirHashes(t, scope)
			for _, name := range []string{migrate.TargetName, migrate.MarkerName, migrate.ReportName} {
				if before[name] != after[name] {
					t.Fatalf("completed conversion repeated: %s", name)
				}
			}
		})
	}
}
