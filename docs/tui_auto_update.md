# Quiet TUI updates and safe startup

The installed full CLI is a stable bootstrap and last-resort fallback. Automatic
updates never replace it. Runtime versions are immutable files below the adjacent
`.cxp-runtime/versions/` directory. Source builds with an unknown version and
unmanaged invocations do not start the automatic worker.

## User-visible behavior

The worker starts 30–119 seconds after the first history TUI frame. Its context
belongs to the outer TUI invocation, so opening a long Codex session does not
cancel it. It checks every 30 minutes. It only prepares a candidate: no window,
Codex session, or Teams service is restarted. The candidate is tried by a future
TUI invocation. A window already using an older version can remain open.

`CXP_AUTO_UPDATE=0`, `off`, or `false` disables both background checks and pending
trials. A non-`latest` `CODEX_PROXY_VERSION` also disables them. Explicit
`cxp upgrade` remains available. Help, version, service commands, and scripts
cannot consume or confirm a TUI trial.

## One authoritative record

`boot.sqlite` is a bounded native SQLite snapshot (boot protocol 2) with a generation, a confirmed `good`
version, an optional `previous` version, and an optional candidate digest and
attempt token. Once it exists, legacy `active`, `previous`, install records, and
`pending-update.json` cannot select the startup version.

| State | Default | Next TUI start |
| --- | --- | --- |
| Stable A | A | Start A |
| B prepared | A | One window may durably claim B |
| B claimed, unconfirmed | A | Other windows start A |
| B first frame acknowledged | B; retain A | Start B |

The claim is committed **before** executing B. A missing, modified, crashed,
timed-out, or killed candidate consumes its opportunity. A lost confirmation
also leaves A as the default. The same version is never automatically rearmed;
a later version or explicit upgrade is needed.

Writers use nonblocking startup locks and generation checks. An explicit update
or rollback invalidates an older in-flight claim. A busy or unwritable update
directory means starting the known-good version, without waiting for repair.
Failure to save a success record can leave this window on healthy B while new
windows continue to use A.

## Startup handoff and fallback

For an interactive TUI, the stable bootstrap stays as a parent. A nonce-authenticated
loopback connection connects before local CLI initialization. The candidate
waits for the parent to acknowledge process-tree ownership before continuing.
It reports `READY` after the first draw and before accepting input or starting
background workers. The parent grants `GO`, then attempts to confirm B. This
readiness does not depend on GitHub, model service availability, or authentication.

Before handoff, a crash or a 10-second deadline terminates the owned process tree,
restores the terminal, and starts A in the same invocation. Recovery does not
require a successful state write, lock, cleanup, or network request. Each
invocation makes at most one fallback attempt. If the confirmed version itself
fails startup, its retained previous version can be used. A fresh installation without boot evidence can use the full bootstrap CLI.
Corrupt, incompatible, or lost published boot state stops before application
setup. An unavailable candidate instead uses the confirmed runtime.

Ctrl+C and termination cancel the invocation. Errors after handoff do not replay
the command or user input. Human setup prompts use a separate `SETUP` handoff:
they may wait indefinitely, but do not promote the candidate. A killed parent
before handoff closes the connection and makes the candidate exit. Unix process
groups and Windows Job Objects own startup descendants; handoff releases the
Windows kill-on-close rule so ordinary detached terminal lifetimes stay intact.

## Download and publisher boundary

The worker uses only `auto-update-index/tui-update.json` from the configured
GitHub repository over HTTPS. It does not fall back to the Release API or accept
the legacy index URL override. Stable `p0` releases are immediately eligible;
`p1` releases wait 48 hours. Drafts, prereleases, `p2`, wrong
platforms, and incompatible boot protocols are excluded.

The manifest binds repository, canonical version, boot protocol, publication time,
asset name, size, and SHA-256. Before executing any downloaded code, the worker
checks the complete size and digest. It then runs bounded, inert version and
protocol probes, and publishes the immutable file before the boot record. The
physical bootstrap must support this protocol before the first record is
created. Supported historical launchers cross the protocol boundary through the
migration adapter, which transfers startup ownership before native state writes
and replaces the physical launcher once its old image is released. Incompatible
legacy rollback targets are not retained. Old writers finish before business-state migration.
Safe candidate-owned upgrades preserve that bootstrap and reject candidates
that lack the boot protocol.

Static metadata has no renewal deadline. A user returning after a long offline
period can still install the last approved stable release. The confirmed
version comparison prevents automatic downgrades. This does not promise
freshness when GitHub or a cache serves old metadata.

This uses GitHub HTTPS and repository release permissions as the publisher trust
root. A digest in this manifest is **not** an offline signature or protection
against a compromised publisher account. CI needs no new signing secret. Any
future incompatible configuration or state migration must change the boot
protocol and provide a tested automatic bootstrap migration. Automatic
releases must remain readable by retained fallback versions.

## Bounded I/O and failure handling

Startup reads a record of at most 64 KiB and checks the selected local executable.
It does not download, wait for an update lock, hash every confirmed executable,
or rewrite unchanged state. A trial hashes its candidate once after its claim.
Small record writes use a synced temporary file, platform atomic replacement,
and supported directory syncing. Version publication syncs the file and its
directory ancestry before recording it. OS/filesystem crash-durability limits
still apply; process-kill tests are not physical power-loss tests.

All windows share a nonblocking worker lock and a cooldown persisted **before**
any HTTP request. Failures back off exponentially; valid `Retry-After` values
are honored up to 24 hours. Read-only/full state prevents network retry storms.
Each attempt has a three-minute context, downloads at most 256 MiB, and owns one
partial download path. Before downloading it requires space for both copies plus
64 MiB of headroom, and rechecks before installation. Insufficient or unknown
free space defers the update. This preflight cannot reserve space against other
processes writing concurrently. Probes have deadlines, a 16 KiB output bound, bounded pipe
waiting, and process-tree cleanup. Cooldown and diagnostics occupy one row in `update-state.sqlite`;
there is no startup popup or update log stream in the TUI.

The first version deliberately has no automatic deletion of published runtimes:
an old window may still need one. At eight retained version directories, new
automatic downloads defer; explicit upgrade remains available. Unpublished
temporary files are cleaned only while their writer lock is held. Published
versions, the current bootstrap, and fallback programs are never removed by this
worker. This favors safe deferral over guessing whether an old window is alive.
Hot reload, delta patches, download resumption, and launcher self-update are
outside this protocol.

## Tests and release gates

Ordinary tests cover file creation, short write, flush, close, replacement and
directory-sync failures; corrupt records and paths; concurrent claims; stale
confirmation; read-only/busy startup; manual update coexistence; download faults;
disk-space deferral; cooldown persistence; priority rules; bounded output; and first-draw ordering.

Run focused tests with:

```sh
bash scripts/ci/test_tui_update_safety.sh unit
```

In a disposable native CI environment, run the real-process tests with:

```sh
bash scripts/ci/test_tui_update_safety.sh all
```

Source-development lifecycle tests must use the development skill's ephemeral
Docker harness rather than the running installed helper. The tagged native
suite uses real compiled executables and private roots, a local fake HTTP
endpoint, real dispatcher invocations, process kills, and fresh follow-up
starts. Production-CLI terminal tests use PTY on Linux/macOS and ConPTY on
Windows. They exercise first-frame promotion and crash recovery through the
actual installed entry. POSIX tests additionally cover an old live window,
busy/read-only state, opt-out, corrupt-record refusal, Ctrl+C, and terminal
mode restoration.

PR CI runs Linux amd64, macOS arm64 and amd64, Windows 2022 and Windows 2025,
plus focused Linux race tests. The aggregate Test job requires this gate.
Release CI repeats native tests against the exact packaged binaries and retains
only passing bytes. The publisher hashes those bytes and emits the TUI manifest
only after all required release gates pass. A shared Python/Go golden fixture
checks the publisher/consumer protocol. Local Linux success does not establish
native macOS or Windows success; those jobs must pass before publication.
