# Teams startup recovery tests

The startup contract is: acquire ownership, prepare the selected database,
open it, then publish the lease. A lease file alone does not mean that a
migration finished. Losing ownership or canceling startup must stop conversion
before it publishes a database. A published database that disappears must not
be replaced with an empty one.

| Contract | Tests |
| --- | --- |
| Recover an aborted first claim without losing chat IDs, thread IDs or receive cursors | `TestTeamsCoreStoreResumesMigrationAfterAbortedStart`; `TestEntryMigrationResumesAfterInterruptedStart` |
| Preserve both supported legacy versions and JSON / SQLite-WAL forms | Four fixture combinations in the store and converter unit tests |
| Cancel or fence conversion at every ownership checkpoint, including before publication | `TestConversionOwnershipAtEveryCheckpoint`; original source and WAL hashes must remain unchanged; retry must retain bindings |
| No writes from a startup already canceled, released or fenced | `TestTeamsCoreStoreDoesNotWriteAfterLeaseLoss`; `TestTeamsCoreStoreLossAfterPreparationStarts` |
| Reuse a completed conversion even if startup stopped between the database rename and migration marker | `TestTeamsCoreStorePreparationAfterMigrationBeforeActivation` |
| Create the database before publishing; refuse failed opens, missing published data and mismatched target names | Store unit tests; `TestEntryMigrationRefusesMissingPublishedDatabase` |
| Recover a process killed while building its temporary database | `TestEntryMigrationResumesAfterCrashDuringImport` |
| A suspended former owner cannot resume retries or publish after takeover | `TestEntryMigrationTakeoverFencesWaitingOwner`; `TestEntryMigrationTakeoverFencesImporter` |
| Recovery answers in the original Teams chat and Codex thread without creating another control chat | Migration E2E checks plus reviewed transcript goldens |
| An open runtime pointer reader permits activation and retains its original contents; fresh readers see the new version | `TestVersionPointerReplacementWhileReaderOpen` covers active/previous pointers on every native CI OS; `TestVersionPointerReadsLongPath` preserves extended-length reads |
| Runtime pointer read errors and failed restarts leave the watchdog alive, skip health collection and recover on the next tick | `TestTeamsServiceWatchdogRuntimeRefreshErrorsDoNotStopLoop`; `TestTeamsServiceWatchdogRuntimeRestartErrorsDoNotStopLoop`; native Windows `TestTeamsServiceWatchdogRuntimeRefreshRecoversSharingViolation` |
| A running watchdog follows activation and rollback before checking health | Watchdog unit tests; `TestTeamsServiceWatchdogRuntimeProcessUpgradeAndRollback` runs real dispatch/restart on each native CI OS; `TestEntryWatchdogRuntimeUpgradeAndRollback` and `TestEntryWatchdogRuntimeLegacyLauncher` run release binaries through the production CLI on Linux |
| A local shutdown protects its final reply before publishing the shared stop; a failed write resumes work | `TestShutdownIntentPrecedesPublishedStop`; `TestShutdownRecordFailureResumesWork`; shutdown E2E scenarios |
| Input admitted between steering and starting still targets the active native turn; uncertain delivery pauses without replay | `TestInputAdmittedBetweenSteerAndStart` explicitly controls the boundary without sleeps, also covering explicit queued and idle execution; `TestUncertainSteerPausesWithoutReplay`; shared Teams/TUI input E2E |
| Fake Codex response encoding cannot race with queue dispatch or deletion | `TestQueueResponsesRemainUnchangedAfterDispatchAndDelete`; complete fake Codex package race check |

The converter fault tests discover the number of ownership checkpoints from a
successful conversion, then inject each failure at every checkpoint. New
conversion stages therefore join the fault coverage without maintaining a
second list of stage names. Tests assert the files and recovered bindings.

The E2E tests use private state, a fake Graph server and a fake Codex
app-server. They launch the production entry point. Process suspension,
termination, filesystem locks and lease takeover are real; only takeover time
is shortened. No production pause hook was added. Observing a temporary WAL
means the importer is building its temporary store, not a particular SQL
statement. The Linux watchdog E2E observes `/proc/<pid>/exe`; the portable
process test records the actual executable and confirms continued health
checks after each transition.

## Gates

From the repository root, in an isolated test environment:

```sh
bash scripts/ci/test_teams_startup_recovery.sh unit
bash scripts/ci/test_teams_startup_recovery.sh e2e
```

Local real-process execution must use the development skill's ephemeral Docker
harness. These tests must never operate an installed helper or service.

CI runs the unit gate on Linux, Windows and macOS, and repeats the seven startup
E2E scenarios three times on Linux. The core race job also checks the startup
and watchdog CLI tests. The existing full unit and E2E suites still
run. The scripts check that every critical test is present before execution;
an empty selection cannot pass the gate. Python regression tests verify that
missing tests and failed Go commands propagate failure. The existing final
CI aggregate depends on the unit and E2E jobs.
The release workflow reuses this CI workflow and requires it before packaging;
the workflow tests also guard that dependency.
The full engine unit and core race jobs include the shutdown and input-admission
boundary tests. The race job separately runs the fake Codex package, so test
backend concurrency failures also block the CI aggregate and release.

These tests do not replace a real shared-filesystem compatibility test or an
installed service-manager upgrade test. They do not repair an empty database
already published by an older defective version. An old watchdog must first
be switched to code that contains the runtime-refresh behavior.

## Historical systemd watchdog repair

On Linux, a managed main service repairs a running pre-rc.73 watchdog from
its own immutable runtime root after acquiring a renewing startup claim and
before waiting or converting the store. It checks systemd's MainPID against
itself, rechecks both instances and the selected runtime, and restarts only
`codex-helper-teams-watchdog.service`. A stopped watchdog, foreground helper,
foreign installation, or modern watchdog is left alone. The check has a
20-second total budget; failures are diagnosed without stopping the healthy
main service or rolling back data. No lease or conversion timing changes.

`TestLegacyTeamsWatchdogHandoff` covers the decision and failure boundaries.
`TestEntryLegacyWatchdogHandoff` uses the production CLI, real processes and
/proc, a service-manager substitute, and fake Graph/Codex to verify migration
and the original chat/thread response. Its fake service manager starts an
isolated executable-identity stand-in so the old process stays alive until the
handoff. CI repeats this scenario three times in the first E2E shard.
The dedicated `watchdog-systemd` job uses the fixed SHA-256-verified v0.1.20
release executable with a real user service manager on an ephemeral
GitHub-hosted Linux runner. It refuses existing units and verifies that the
main process survives. The aggregate CI/release gate requires that job.
A full historical Teams-command upgrade remains a separate compatibility
limitation; the E2E here exercises the new startup after activation.

The repair adds bounded service queries at managed startup only. Modern
watchdogs require two MainPID reads and one executable inspection; there is
no new periodic repair, JSON rewrite, durable marker or history scan. The
watchdog anchor lookup replaces the existing resolved-store lookup, allowing
a fresh claimed lease to be observed before a database exists. A stale
startup claim still triggers the existing recovery policy.
