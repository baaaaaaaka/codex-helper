# Beacon Mode Plan

Beacon mode lets a stable coordinator, normally the Teams service on a login or jump host, dispatch Codex work to scheduler-backed worker machines. The coordinator owns user interaction, Teams delivery, profile selection, recovery, upgrade decisions, and cleanup. Workers only execute fenced jobs and write results to a shared queue.

This document records the current compatibility constraints with the existing Teams bridge, outbox, recovery, and upgrade code.

## Goals

- `new` without an explicit beacon profile runs locally on the machine where the current `cxp` or Teams service is running.
- Users can explicitly choose an execution profile such as CPU, GPU, Slurm, or LSF.
- Users can mark a job as shared or exclusive. Shared is the default.
- Users can list leased machines, see which conversations/jobs are running on each machine, release a machine, and kill or quarantine affected jobs.
- The same profile and lease model must be usable from Teams commands, TUI, and CLI options.
- Slurm and LSF are first-class providers, but the worker protocol must stay scheduler-neutral.
- Workers use a long-lived worker plus shared-file queue by default. SSH/dropbear is only a fallback for login semantics, not the main execution path.

## Non-Goals

- A beacon worker must not send Teams or Graph messages directly.
- A beacon worker must not own Teams control chat polling.
- A beacon worker must not mutate helper state unless the write is scoped to a fenced beacon job result.
- A beacon profile is not the same object as the existing SSH proxy profile.
- A beacon execution profile is not the same object as `CODEX_HELPER_TEAMS_PROFILE`, which scopes Teams state and control chat identity.

## Profile Model

Use separate namespaces:

- SSH proxy profile: existing `config.Profile`, used by the current proxy stack.
- Teams scope profile: `CODEX_HELPER_TEAMS_PROFILE`, used to separate Teams state/control chats.
- Beacon execution profile: new scheduler/execution profile for local, Slurm, LSF, image, GPU count, queue, exclusivity, and proxy selection.

Profile creation must ask for proxy behavior explicitly:

- no proxy
- use an existing SSH proxy profile

Provider-specific creation:

- Slurm asks for nodes, GPU count, partition, image, and duration.
- LSF asks for queue name. Other provider details are derived by the provider adapter.

Teams and TUI may create structured beacon profiles. They must not accept raw provider shell commands, secrets, or unapproved advanced fields. A created profile remains draft until:

- the user confirms it
- proxy selection resolves
- provider preview succeeds
- doctor checks pass

## Execution Target Selection

New conversation behavior:

- default target is local
- legacy proxy preferences do not make `new` remote
- explicit beacon profile selects beacon execution

Existing conversation profile switch:

- target profile must be ready
- idle compatible switch applies immediately
- queued or running conversations get a pending switch
- incompatible resume requires an explicit fork

Every queued turn stores an execution target snapshot. Later profile switches do not mutate already queued or claimed work.

## State Layout

Beacon state must not be embedded in legacy Teams state JSON. Existing Teams code can rewrite state through structs that do not preserve unknown fields. Beacon state needs its own store, with its own schema version and locking/fencing rules.

Recommended shared layout:

```text
~/.cache/codex-helper/beacon/
  profiles.json
  scopes/<scope-id>/
    leases/
    jobs/
    workers/
    events/
    tombstones/
```

The exact base path can change, but it must be shared by the login host and worker machines.

## Roles

Active coordinator:

- dispatch jobs
- acquire/release leases
- reconcile workers
- clean tombstoned files
- enqueue Teams outbox
- flush Teams outbox through existing Teams bridge paths
- apply profile switches
- decide whether to reuse or allocate machines

Standby coordinator:

- read state for status
- wait for Teams control lease
- do not dispatch, clean, or enqueue Teams messages

Worker:

- heartbeat
- claim fenced jobs
- execute Codex
- write event chain, terminal result, and artifact metadata
- never call Graph or Teams send APIs

## Worker Protocol

Use at-least-once file transport with explicit fencing:

- job id
- job attempt
- worker id
- scheduler/provider job id
- lease epoch
- claim epoch
- protocol read/write version
- execution signature
- HMAC or equivalent integrity check
- monotonic event sequence with previous-hash chain

The claim epoch is the fencing boundary for worker terminal writes. It must not be the current Teams coordinator epoch, because the coordinator can restart while a worker is still valid.

Terminal writes are accepted only when all fencing fields and event-chain integrity checks match. Late writes, duplicate terminal records, event gaps, HMAC failures, and future write versions are quarantined.

## Recovery

Current Teams `recover` interrupts queued/running Teams turns. Beacon cannot rely on normal Teams turn status as the source of truth for remote execution.

Beacon-aware recovery must reconcile beacon state first:

- queued or claimed before execution: requeue
- start intent or started with live worker: monitor and keep the Teams projection delegated
- started with dead worker and no valid terminal: ambiguous, no replay
- valid terminal: queue protected result delivery
- corrupt or late terminal: quarantine

The coordinator must not auto-replay a job after Codex may have started. Replay requires explicit user action.

## Outbox Rules

Beacon outbox must use existing Teams outbox semantics:

- progress/status: `status-*` or `progress-*`, transient and upgrade non-blocking
- final answer: kind containing `final` or `answer`, protected
- artifacts: kind containing `artifact` or attachment metadata, protected
- quarantine or needs-attention: protected, not transient

Workers write result files and metadata. The active coordinator turns those records into Teams outbox entries.

## Upgrade Compatibility

Helper and Codex upgrades must consider beacon blockers.

Block upgrade when any of these exist:

- active beacon job
- started beacon job without terminal
- protected beacon result/artifact outbox
- protocol-mismatch job that still needs reconciliation
- normal Teams queued/running turns
- normal protected Teams outbox

Do not block upgrade for:

- idle workers
- drained workers with no active jobs
- transient beacon status/progress messages

Codex upgrade must be scoped to one install target. It requires a managed persistent target, per-target lock, no active job on that target, and a maintenance worker or local maintenance path. Read-only images, unknown origins, fixed paths, and ephemeral overlays are not auto-upgraded.

## Machine Lifecycle

Lease states:

- starting
- accepting
- draining
- drained
- expired
- lost
- incompatible

The scheduler is authoritative. A fresh worker heartbeat cannot keep a lease alive if Slurm or LSF says the job is gone. If the scheduler is unknown, drain conservatively.

Cleanup rules:

- never delete live scheduler jobs
- never delete started ambiguous work
- tombstone before delete
- delete only after retention expires
- manual release drains first, then kills provider job if requested, then quarantines or tombstones affected jobs

## Provider Notes

Slurm:

- provider profile records nodes, GPU count, partition, image, duration
- provider can use `submit_job --just_alloc` or equivalent allocation mode
- worker starts inside allocation/container and writes lease metadata to shared storage

LSF:

- provider profile records queue name
- provider derives host/job/container environment from LSF metadata
- direct SSH to system sshd is not a primary execution path because it may not run inside the allocation cgroup/environment

## Required Tests

Simulation tests must cover:

- default local target despite legacy proxy preference
- structured profile creation in Teams/TUI/CLI
- queued target snapshot across profile switch
- shared and exclusive reservations
- Slurm and LSF provider profile validation
- live worker recovery without interrupting the remote job
- valid terminal after coordinator restart
- active beacon jobs blocking helper/Codex upgrade
- protected result outbox blocking upgrade
- transient progress not blocking upgrade
- active coordinator only dispatching, cleaning, and touching Teams outbox
- standby coordinator not dispatching
- worker never sending Teams outbox directly
- lease expiry, long-running jobs, hung workers, dead scheduler jobs, preemption, requeue, node failure
- multiple workers racing to claim jobs
- cleanup of tombstones and zombie files
