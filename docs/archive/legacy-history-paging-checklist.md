# Legacy history paging and safe migration checklist

This checklist is the execution contract for the clean CXP and Codex worktrees. The raw legacy
rollout is the source of truth; SQLite is a rebuildable read projection. No migration step may
silently discard a valid rollout, duplicate a turn/item, or make the official `codex resume`
path incompatible.

## 0. Scope and invariants

- [x] Confirm the failure is the CXP app-server JSONL line cap reached by TUI `thread/read(includeTurns=true)`.
- [x] Keep the CXP broker transparent: it must not parse, reassemble, or buffer an entire transcript.
- [x] Keep a finite transport safety limit; do not replace it with an unbounded or 1 GiB scanner.
- [x] Keep the raw rollout recoverable and make the projection rebuildable from it.
- [x] Preserve direct `codex resume`, CXP TUI, and CXP Teams service contracts.

## 1. Clean workspaces and baseline

- [x] Create a clean CXP worktree from the latest `origin/main`.
- [x] Create a clean official Codex worktree from the latest `origin/main`.
- [x] Leave the original dirty CXP worktree and the original Codex checkout untouched.
- [x] Record baseline commits and clean/dirty status in the implementation notes.

Baseline: CXP worktree `6d02075f29a24629d60e6c698231e447be5616c6`; Codex worktree
`902bd9e06b3ecb32cbf7f8e64cd23b956be3e7fe`. Both new worktrees were clean before this checklist;
the original CXP checkout remains user-dirty and is not part of this change.

## 2. Codex storage and migration

- [x] Make legacy-to-paginated migration available for normal local app-server use, without
  changing the legacy file in place before a verified replacement/projection exists.
- [x] Make migration targeted/idempotent and serialized with live writers and existing migration
  locks; a busy or failed migration must leave the original usable. The app-server invokes the
  existing `LocalThreadStore::migrate_rollouts` for the one thread being resumed; `SkippedBusy`
  stays on bounded legacy replay rather than publishing a partial projection.
- [x] Preserve valid EOF semantics, reject malformed records safely, and keep journal/recovery
  behavior crash-safe. The existing migration suite passed 47/47 focused tests.
- [x] Ensure the migration preserves logical ordering, stable turn/item IDs, rollback/compaction
  semantics, archived status, names, metadata, and Teams checkpoints/outbox expectations. The
  canonical migration path and CXP Teams runner were left unchanged; the migration suite covers
  the history/metadata invariants.
- [x] Ensure a migrated thread is observable as paginated before any client asks for pages, so a
  normal `excludeTurns=true` resume cannot race migration and fall back to a full response. An
  explicit `initialTurnsPage` remains a bounded read-only legacy compatibility path.

## 3. Bounded app-server history API

- [x] Route normal legacy resume history through the canonical Codex projection and existing
  `thread/turns/list` / `thread/items/list` paging primitives. Direct legacy page requests and
  `SkippedBusy` writers use bounded app-server-local replay so a read does not itself mutate the
  source file.
- [x] Keep each response bounded by page count and item-size safeguards; no compatibility path may
  reconstruct the full history into one JSON-RPC result. Internal legacy replay is never serialized
  as a full `thread/read` response.
- [x] Keep cursors opaque, scoped to the thread/read view, and monotonic so retries do not create
  duplicates or skips; return an explicit failure when the source changes incompatibly. The
  existing turn cursor plus the bounded legacy item cursor enforce thread/scope/anchor checks.
- [x] Keep `thread/read(includeTurns=true)` behavior for clients that explicitly require the old
  contract, except that official TUI resume must not use it for migrated legacy sessions.
- [x] Add/retain focused server tests for legacy migration, page boundaries, empty/partial EOF,
  malformed input, repeated requests, restart/rebuild, source mutation, rollback/compaction, and
  oversized individual records. `codex-thread-store` migration tests passed 47/47; app-server
  `thread_read` passed 22/22 (including legacy item-page duplicate coverage); the migration
  integration test passed.

## 4. Official Codex TUI

- [x] Remove the legacy resume hydration fallback that calls full
  `thread/read(includeTurns=true)` after pagination support is negotiated.
- [x] Bootstrap recent history with bounded turn/item pages and retain an opaque cursor for older
  history.
- [x] Load older history only on explicit scroll/backfill demand or the existing bounded top-up
  policy; do not repeatedly repaint the same rows or create visible scrolling churn.
- [x] Deduplicate by stable turn/item IDs and preserve transcript order, scroll position, and
  existing rendering behavior.
- [x] Keep an explicit old-server compatibility fallback only when the server cannot negotiate the
  paging contract; document that fallback as a bounded/known limitation rather than silently using
  it with a capable server. A one-time method-not-found downgrade also covers servers that fail to
  advertise the capability before a page request.
- [x] Add TUI request-trace and state tests proving resume does not issue a full legacy
  `thread/read` and that delayed loading is quiet and stable.

## 5. CXP broker and runner

- [x] Keep the app-server stdout scanner at a finite defensive limit and make its error explicit;
  the normal legacy-resume path must never depend on raising this limit.
- [x] Verify the broker forwards complete JSONL frames without transcript-aware buffering and
  handles text/binary/close/error paths without leaking goroutines or locks.
- [x] Review bounded read/write deadlines and WebSocket message limits against the existing remote
  TUI protocol. No new transport cap was added: the broker is byte-transparent, the process
  scanner remains the finite defensive boundary, and an arbitrary WebSocket message cap would
  recreate the same failure for large but valid protocol frames.
- [x] Run CXP tests proving protocol frames cross the broker, a deliberately oversized process
  stdout line is rejected, and Teams metadata-only resume still uses its existing low-volume path.
- [x] Confirm that the CXP implementation needs no source change for this fix: `codex_open` still
  launches the official Codex remote TUI, while the app-server owns migration and paging.
- [x] Do not change Teams to depend on TUI transcript hydration or alter its checkpoints/outbox
  state machine.

## 6. Regression, stress, and handoff

- [x] Run focused Rust tests for thread-store migration, app-server paging, and TUI hydration:
  migration 47/47, app-server `thread_read` 22/22, rollout integration 1/1, and TUI lifecycle
  requests 12/12.
- [x] Run focused Go tests for app-server transport, remote broker, Codex launch, and Teams resume:
  `go test ./internal/codexrunner ./internal/cli` passed.
- [ ] Run a large synthetic transcript test at tens/hundreds of MiB and a representative 1 GiB
  source where practical; assert no single wire line exceeds the defensive cap. This remains a
  performance/stress gate, not a correctness blocker for the bounded protocol path.
- [x] Run `git diff --check`, inspect all diffs for unrelated changes, and verify no source rollout
  or user state was modified by tests.
- [x] Mark every completed item here and record the remaining stress/release gates.

## Definition of done

- [x] With the updated official Codex app-server/TUI build, a legacy session can be resumed
  through `cxp resume` in the remote TUI without
  `bufio.Scanner: token too long`.
- [x] Direct `codex resume` remains functional and displays the same logical transcript; its
  explicit full-read contract remains intact.
- [x] CXP Teams service resumes and continues turns without duplicate/lost history or checkpoint
  corruption.
- [x] No full legacy transcript is emitted as one app-server stdout JSONL response by the normal
  capable-client path.
- [x] A failed/interrupted migration can be retried without data loss or duplicate logical items.

Release gate: the CXP documentation commit and full Go gate are complete. This prerelease does
not bundle the official Codex binary, so installing the corresponding Codex build remains a
separate consumer step. The 1 GiB stress gate remains intentionally unchecked.
