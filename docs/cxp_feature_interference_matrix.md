# cxp Feature Interference Matrix

This document is the cross-feature ledger for `cxp` / `codex-proxy`.

It exists to answer three questions before adding more beacon behavior:

1. What feature families does the current CLI/helper expose?
2. Which pairs can interfere with each other?
3. Where does the current code already serialize, fence, defer, or block that
   interference?

When adding a feature, add a row/column here first. If the new cell is `B`,
`F`, or `G`, add or update tests before enabling the behavior.

This matrix is not meant to be the only safety net. The feature families are
wide on purpose so the table stays readable, but every implementation review
must also do the leaf-level pass below. A change is not considered reviewed just
because its family-level cells look safe.

## Anti-Omission Review Method

Use this method for every beacon design or implementation change:

1. List touched leaf features, not just feature families. Leaf features include
   Cobra subcommands, Teams control/work commands, background loops, provider
   adapter operations, state transitions, env vars, store files, generated
   service configs, static help text, docs, CI scripts, and worker protocol
   messages.
2. Map every touched leaf feature to one or more feature families in this file.
   If a leaf does not fit a family or a cross-cutting surface below, add it
   before continuing.
3. Build the shared-resource list for the change: config file, Teams store,
   beacon store, Codex history, skill store, outbox, auth cache, helper owner
   lease, install lock, scheduler job, worker process, filesystem queue, env
   var, network proxy, and user-visible Teams message.
4. For each touched family pair, read the matrix cell and the detail row. If the
   cell is `B`, `F`, or `G`, the PR must either add/point to a test or record a
   deliberate gap.
5. Check state-machine boundaries explicitly. At minimum, beacon changes must
   name the affected Teams turn phase, beacon allocation state, machine state,
   job phase, terminal state, outbox state, and service owner state.
6. Check disruptive-operation boundaries explicitly: helper reload/restart,
   helper upgrade, Codex upgrade, user cancel, retry, profile switch, release,
   hard kill, provider timeout, provider preemption, worker heartbeat loss, and
   Graph auth loss.
7. Check generated/static guidance drift explicitly: Cobra help, README command
   sections, Teams control fallback prompt, built-in `cxp` skill references,
   install scripts, service config templates, and CI workflow scripts.
8. Add the new leaf feature to this document when it introduces a new
   command/state/operation or changes a `B`, `F`, or `G` interaction.

The family matrix is therefore the index; the leaf-level checklist is the
coverage guard. Missing a leaf feature or shared resource is treated as a review
failure, even if the broad family row already exists.

## Legend

| Mark | Meaning |
| --- | --- |
| `N` | No direct interference beyond ordinary shared files/process environment. |
| `S` | Shared configuration/state, but current code keeps the concerns separated. |
| `D` | Deferred or serialized by queueing, target snapshots, idempotency, or ownership rules. |
| `B` | Blocks another operation until work/state is safe. |
| `F` | Fenced by identity, epochs, hashes, or confirmation tokens. |
| `G` | Gap or partial implementation. Do not assume safe behavior without more design/tests. |

## Cross-Cutting Surfaces

These are not separate matrix columns because they apply to almost every
family. They must still be named in every design/review.

| ID | Surface | Leaf features that must not be omitted | Main interference risk |
| --- | --- | --- | --- |
| `C01` | Root CLI and effective identity | Root command dispatch, default history TUI, profile/config parsing, `--config`, `--upgrade-codex`, hidden `__internal-npm-wrapper`, root/sudo `CXP_USER_HOME`, `CODEX_DIR`, `CODEX_HOME`, and launch identity. | A command can read/write the wrong config, history, skill, auth, or Teams/beacon store when run as root, through sudo, or from a service. |
| `C02` | Stores, locks, idempotency, and audit | Teams store, beacon store, skill store, install lock, publish lock, dashboard state, outbox hashes, retry tokens, confirm tokens, beacon audit log, request IDs, and epoch/fencing fields. | Two operations can both look valid unless the correct lock, idempotency key, epoch, or audit transition is used. |
| `C03` | Env/process/service config | Service env allowlist, loopback proxy filtering, `CODEX_HELPER_TEAMS_CHILD`, `CODEX_HELPER_CLI_PATH`, `CODEX_HELPER_BEACON_STORE`, `CODEX_HELPER_BEACON_SLURM_QUERY`, `CODEX_HELPER_BEACON_SLURM_SUBMIT`, `CODEX_HELPER_BEACON_SLURM_CANCEL`, `CODEX_HELPER_BEACON_SLURM_RENEW`, `CODEX_HELPER_BEACON_LSF_QUERY`, `CODEX_HELPER_BEACON_LSF_SUBMIT`, `CODEX_HELPER_BEACON_LSF_CANCEL`, `CODEX_HELPER_BEACON_LSF_RENEW`, `CODEX_PROXY_*`, `CODEX_PROXY_UPDATE_INDEX_URL`, generated systemd/LaunchAgent/Task XML, and WSL `--exec env`. | Foreground CLI, service child, remote worker, and CI can see different tools, proxy vars, auth roots, or store paths. |
| `C04` | Static guidance and docs | Cobra help, README, Teams control fallback prompt, built-in `cxp` skill command references, release notes, and user-facing Teams status text. | Codex or users can be guided toward the wrong command, especially for beacon/profile/proxy distinctions. |
| `C05` | CI/scripts/drift checks | Install bootstrap scripts, release-index generation, service config rendering tests, monitor workflows, shell/PowerShell scripts, and workflow job matrices. | A code path can be locally tested but not covered in the actual packaged or service/CI environment. |
| `C06` | Helper-generated content filtering | Polling filters, durable outbox echo hashes, helper/Codex rendered output detection, attachment echo filtering, transcript import filters, history indexing filters. | Helper messages can be mistaken for user prompts, or real user prompts can be over-filtered. |

## Feature Families

| ID | Feature family | User-visible entrypoints | Primary state/code anchors |
| --- | --- | --- | --- |
| `F01` | Install and helper self-upgrade | install scripts, `cxp upgrade`, Teams helper update/reload/restart flows, TUI-triggered update/restart, pending activation | `install.sh`, `install.ps1`, `internal/cli/upgrade.go`, `internal/update`, `internal/cli/teams_upgrade.go`, Teams helper lifecycle paths |
| `F02` | Codex CLI bootstrap/upgrade | `cxp --upgrade-codex`, Codex install/update bootstrap, npm wrapper guard, automatic Codex upgrade after incompatible CLI errors | `internal/cli/codex_install.go`, `internal/cli/codex_self_update.go`, `internal/cli/upgrade_codex.go`, Teams Codex upgrade request paths |
| `F03` | SSH proxy profiles and command proxying | `cxp init`, `cxp run`, `cxp proxy ...`, saved proxy preference, daemon/list/stop/prune/doctor | `internal/cli/run.go`, `internal/cli/proxy.go`, `internal/config`, `internal/localproxy`, `internal/stack`, `internal/ssh` |
| `F04` | Local Codex history and TUI | `cxp`, `cxp tui`, `cxp history ...`, `history open`, local session import/sync, TUI proxy/yolo/skills menus | `internal/cli/tui.go`, `internal/cli/history.go`, `internal/cli/codex_open.go`, `internal/codexhistory` |
| `F05` | Teams control chat and workspace navigation | `projects`, `sessions`, `new`, `continue`, `ask`, unknown natural text fallback, dashboard selections, `helper ...`, control webhook commands | `internal/teams/bridge.go`, `internal/teams/dashboard.go`, `internal/teams/store`, `internal/cli/teams_control_fallback_help.go` |
| `F06` | Teams work queue, Codex execution, retry, cancel, status | Work chat task messages, `helper status`, `helper retry`, `helper cancel`, queued/running status, deferred replay, beacon-targeted execution | `internal/teams/bridge.go`, `internal/codexrunner`, `internal/teams/store`, `internal/teams/beacon_job_executor.go` |
| `F07` | Teams Graph auth, outbox, file upload, workflow cards | `cxp teams auth ...`, Graph read/send polling, inbound attachments, `helper file`, `teams send-file`, workflow webhook notifications | `internal/cli/teams.go`, `internal/cli/teams_workflow.go`, `internal/teams/auth.go`, `internal/teams/graph.go`, `internal/teams/attachments.go`, outbox paths |
| `F08` | Teams service, watchdog, owner lease, pause/drain/recover | `cxp teams run`, `--auto-service`, `cxp teams service ...`, `cxp teams pause\|resume\|drain\|recover`, watchdog, auto-update | `internal/cli/teams_service.go`, `internal/cli/teams_service_watchdog.go`, `internal/cli/teams_auto_update.go`, `internal/teams/store` upgrade/control paths |
| `F09` | Skills and built-in `cxp` skill | `cxp skills ...`, `cxp skills install-builtin`, Teams `helper skills ...`, TUI skill menu, daily auto-sync, installer built-in skill repair | `internal/cli/skills.go`, `internal/skills`, `internal/skills/builtin/cxp` |
| `F10` | Beacon profiles and target switching | `cxp beacon profile ...`, `cxp beacon switch-profile ...`, `new --beacon`, `--beacon-isolation`, Teams `beacon switch\|local\|fork\|status`, Teams/CLI profile update/history/rollback/gc | `internal/beacon/profile.go`, `internal/beacon/target.go`, `internal/teams/beacon_commands.go` |
| `F11` | Beacon managed allocation and provider adapters | `cxp beacon allocation ...`, `cxp beacon release ...`, `cxp beacon provider template ...`, profile-stored and env fallback provider query/submit/cancel/renew scripts, reconcile/reconcile-all | `internal/beacon/managed.go`, `internal/beacon/provider_adapter.go`, `internal/cli/beacon.go` allocation/provider/release commands |
| `F12` | Beacon worker, job, machine, terminal, release/kill lifecycle | `cxp beacon worker run-once\|serve`, `cxp beacon machine ...`, Teams/CLI `beacon release`, worker heartbeat, job claim/start/terminal, release/kill | `internal/beacon/jobs.go`, `internal/beacon/policies.go`, `internal/teams/beacon_job_executor.go`, `internal/cli/beacon.go` worker/machine commands |

## Leaf Feature Inventory

This inventory is the "do not miss this" list for code review and test design.

| Family | Leaf features that must be covered | Current safety model and known weak spots |
| --- | --- | --- |
| `F01` | Shell/PowerShell installers, checksum/native-binary validation, install target envs, `cxp upgrade`, release index p0/p1/p2 priority, 48h p1 delay, pending helper replacement, Windows activation status/log JSON, Teams `helper update/reload/restart`, TUI update/restart, post-upgrade built-in skill repair. | Install/update paths use locks, blocker predicates, pending replacement, and service control notices. Weak spots: built-in skill repair with locally modified/unmanaged skills, generated service config drift, and duplicate-process takeover during WSL/bootstrap. |
| `F02` | Managed Codex install, Node install root, `--upgrade-codex`, CLI compatibility checks, `codex install/update -g @openai/codex` npm wrapper guard, self-update refusal, queued Teams Codex-upgrade request after CLI failure. | Codex upgrade is separated from user-turn replay. Weak spots: install lock timeout behavior, service `teams run --upgrade-codex` vs queued upgrade, and per-beacon-target Codex install matching. |
| `F03` | SSH proxy profiles, saved proxy preference, `cxp run`, ephemeral proxy stack, long-lived proxy daemon, `proxy list/stop/prune/doctor`, host key verification, HTTP proxy health, self-loop protection, target process supervision, TTY/process group handling. | Proxy state is mostly separate from Teams/beacon state. Weak spots: stopping/pruning a proxy while a running command expects it, and saved proxy profiles not being applied to git-backed skill operations. |
| `F04` | History list/show/open/TUI, persistent cache truth vs session JSONL files, root/non-root shared cache, local history import/sync, title sync, TUI refresh, proxy/yolo toggles, update prompt, skills menu, helper-session filtering. | History import is gated and duplicate/ambiguous local activity is avoided. Weak spots: root/sudo effective-path drift and ensuring beacon terminal output remains the source of truth for remote sessions. |
| `F05` | Control chat binding/create/recreate, workspace/session dashboard, number-selection expiry, `ask` and unknown natural text fallback, wrong-chat routing, owner-only helper commands, control webhook commands, `helper skills`, `beacon` admin/status commands, helper-generated message filtering. | Dashboard/control state is stored and commands are routed by chat type and ownership. Weak spots: fallback help can drift from Cobra/README/skill docs, and fallback execution shares F06 queue/outbox semantics. |
| `F06` | Work queue admission, queued/running status snapshots, async Codex execution, running cancel handles, queued cancel interruption, retry rehydration, hosted/reference/message attachment replay, duplicate prompt suppression, deferred inbound replay, external-user ownership, beacon turn preparation, local vs remote execution. | Store idempotency, snapshots, cancel handles, recovery notices, beacon job tombstones, and optional provider cancel prevent most duplicate execution. Weak spots: provider cancel still depends on site adapter configuration, and retries with lost auth/attachments need explicit coverage. |
| `F07` | Graph auth full/read/file-write flows, token cache/logout/status, Graph read/send retries, allowed SharePoint hosts, inbound attachment downloads, outbound artifacts and `teams send-file`, durable outbox FIFO, per-chat rate limits, protected/transient outbox classes, workflow webhook notifications and owner mention suppression. | Coordinator owns Teams output and stores durable outbox state. Weak spots: workflow fallback/mention suppression drift, Graph auth loss during retry, and classifying beacon final/artifact messages as protected. |
| `F08` | Foreground `teams run`, `--auto-service`, recoverable poll-failure retry loop, service install/uninstall/enable/disable/status/start/stop/restart/doctor/watchdog, owner lease, pause/resume/drain/recover, watchdog takeover, auto-update flags, pending activation, env filtering. | Service owner state, blockers, protected outbox, active-owner periodic beacon reconcile, and active-owner lease renewal protect active work. Weak spots: generated service configs can drift, service env can change history/auth/proxy scope, and production scheduler load behavior still needs live validation. |
| `F09` | Built-in `cxp` skill install/repair, command references, git-backed add/sync/remove/doctor/push, source store/state, git mirrors, target publish locks, local-change refusal, manifest hash validation, TUI skill menu, daily auto-sync, Teams `helper skills`. | Skill manager uses source metadata, publish locks, hashes, and local-change checks. Weak spots: daily auto-sync racing manual skill actions, git operations using only ambient proxy env, and model invocation not being a runtime guarantee. |
| `F10` | Beacon profile list/create/update/history/rollback/gc/status/doctor/confirm/delete, profile revisions/history, local/beacon target switching, pending target, `beacon local`, `beacon fork`, `new --beacon`, isolation modes, profile snapshots, proxy/profile validation, local provider values. | Profile update creates a new revision and pins existing zero-revision references before replacing the latest profile; rollback republishes an old revision as a new latest revision; GC only prunes historical revisions no target/allocation references. Turn/allocation snapshots isolate queued/running work from later profile switches. `profile doctor` now validates provider fields plus query/submit/cancel/renew adapters visible to Teams/CLI, and `doctor --smoke` verifies submit/query/cancel against a real scheduler allocation when explicitly requested. Delete archives/disables the latest profile and pins old references instead of breaking in-use chats. `beacon local` declares future local execution and asks the release path to drain/release old demand when safe. Local provider machines can remain provider-job-less; Slurm/LSF managed allocations require provider job identity. Weak spots: live service-env smoke still depends on site-specific scheduler access. |
| `F11` | Managed allocation create/list/status/cancel/release/reconcile/reconcile-all, high-level `beacon release`, provider template rendering, profile-stored provider commands, provider query/submit/cancel/renew env fallback vars, allocation state projection, provider job id tracking, desired TTL/deadline, renew epochs, conservative pre-start replacement. | Provider RUN is not enough; allocation/machine state must match worker readiness. Provider query/submit/renew/cancel run outside the beacon store lock where they have side effects. Profile-stored adapter commands are snapshotted into allocations and override helper environment fallback, so profile edits can take effect for future turns without helper reload. Renew results are epoch-fenced, pre-start provider loss can reset an allocation for replacement without rereading the current profile, and allocation cancel can release a provider job even when no beacon machine registered. High-level release now produces an affected-chat/turn/resource preview and requires confirmation for global shared or forced impact; Work-chat release detaches only the current chat from shared workers. Active-owner reconcile retries durable cancel/release intents and cancels allocations whose conversation demand has ended. Weak spots: site-specific provider cancel/renew integration and live scheduler load behavior still need environment coverage. |
| `F12` | Worker serve/run-once, allocation-scoped worker execution, high-level release, machine list/status/release/kill, doctor, heartbeat, membership proof, bootstrap diagnostics, job enqueue/claim/start-intent/started/terminal, terminal fencing, artifact validation, stale heartbeat drain, stale claim recovery. | Worker/machine/job transitions use request, turn, worker, lease, claim, provider, terminal fences, tombstones, and late-terminal rejection. Worker registration stores node/log/store/codex/cxp/protocol diagnostics so Teams status can show bootstrap context without asking the user to inspect Slurm/LSF manually. Manual release drains started work by default and cancels only allocations that have not reached possible execution unless forced. Weak spots: BYO attach/attestation and full production resource accounting are still open. |

## Pairwise Matrix

Read this as "row feature interacting with column feature". The table is
symmetrical; only the upper triangle is filled.

| Pair | F01 | F02 | F03 | F04 | F05 | F06 | F07 | F08 | F09 | F10 | F11 | F12 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `F01` install/helper upgrade | - | `S` | `S` | `S` | `B` | `B` | `B` | `B` | `S/G` | `B` | `B/G` | `B/G` |
| `F02` Codex bootstrap/upgrade |  | - | `S` | `S` | `S` | `B` | `S` | `B` | `S` | `B/G` | `B/G` | `B/G` |
| `F03` proxy/run |  |  | - | `S` | `S` | `S` | `S` | `S` | `G` | `S` | `S/G` | `B` |
| `F04` history/TUI |  |  |  | - | `D` | `D` | `S` | `S` | `S` | `S` | `S` | `S` |
| `F05` Teams control |  |  |  |  | - | `D` | `B` | `B` | `S` | `D` | `S` | `S` |
| `F06` Teams work execution |  |  |  |  |  | - | `B` | `B` | `S` | `D` | `D/G` | `F/G` |
| `F07` Teams auth/outbox/files |  |  |  |  |  |  | - | `B` | `S` | `S` | `S` | `F/G` |
| `F08` service/watchdog/recover |  |  |  |  |  |  |  | - | `S` | `B` | `B/G` | `B/G` |
| `F09` skills |  |  |  |  |  |  |  |  | - | `S` | `S` | `S` |
| `F10` beacon target/profile |  |  |  |  |  |  |  |  |  | - | `D` | `D/F` |
| `F11` beacon allocation/provider |  |  |  |  |  |  |  |  |  |  | - | `D/F/G` |
| `F12` beacon worker/job/machine |  |  |  |  |  |  |  |  |  |  |  | - |

## Self-Interference Rows

Diagonal cells are shown as `-` in the compact matrix, but these self-pairs
still require review.

| Family | Self-interference to review | Current handling | Gaps |
| --- | --- | --- | --- |
| `F01-F01` | Concurrent helper upgrades, reload/restart/update while pending replacement exists, install script repair racing service activation. | Install/update lock, pending activation records, service blockers, and user-visible notices. | WSL duplicate-process takeover and post-upgrade skill repair need explicit regression tests. |
| `F02-F02` | Concurrent Codex installs/upgrades and npm wrapper interception. | Codex install lock and wrapper guard separate managed install from upstream self-update. | Lock timeout behavior should be tested so a timed-out command cannot corrupt the managed install. |
| `F03-F03` | Proxy daemon stop/prune while `cxp run` or another command still depends on the proxy. | Process supervision and daemon metadata make most lifetimes explicit. | Need coverage for stop/prune against active proxy users and stale daemon metadata. |
| `F04-F04` | History cache readers/writers, session-file truth, TUI refresh, import/sync checkpoints. | Persistent cache and transcript checkpoints are separate from raw session files. | Root/sudo shared-cache interactions need matrix-backed tests. |
| `F05-F05` | Dashboard selection state, control fallback, wrong-chat routing, chat recreate/binding. | Store-backed dashboard state has expiry and command-specific consumption rules. | Static fallback help must be drift-tested against command refs. |
| `F06-F06` | Duplicate work messages, queued/running status, cancel vs retry, deferred inbound replay, external-user ownership. | Store idempotency, turn status, cancel handles, and replay/defer paths. | Retry after partial attachment/auth failure needs explicit coverage. |
| `F07-F07` | Outbox FIFO/rate limits, transient vs protected messages, workflow notification fallback, attachment echo loops. | Per-chat FIFO, rate-limit state, protected predicates, and known-outbox hashes. | Workflow fallback and protected beacon artifact classes need coverage. |
| `F08-F08` | Foreground run, service child, watchdog, owner takeover, auto-update, pause/drain/recover all changing lifecycle state. | Owner lease, pause/drain states, watchdog state, and update blockers. | Generated service configs and env allowlists need drift tests. |
| `F09-F09` | Manual skill add/sync/remove/doctor/push racing daily auto-sync or installer repair. | Source metadata, local-change refusal, manifest hashes, and publish locks. | Daily auto-sync vs manual action and modified built-in skill repair need tests. |
| `F10-F10` | Profile edit/delete/switch/fork/local while queued or running work references the profile. | In-use deletion checks, target snapshots, pending target, and fork semantics. | Local provider representation vs unsupported adapter execution must be made explicit. |
| `F11-F11` | Provider query/submit/reconcile/reconcile-all racing allocation creation, renewal, cancel, or replacement. | Allocation state, provider job IDs, renew epochs, replacement epochs, and provider deadlines are stored in beacon state. Provider side-effect calls run outside the store lock; submit rechecks terminal/provider/replacement state immediately before calling the external adapter, and renew/apply paths compare epochs/provider IDs before applying results. | Site adapters must make submit/renew/cancel idempotent by deterministic request or provider job ID; production load tests should verify scheduler retry/backoff behavior. |
| `F12-F12` | Release/kill, stale heartbeat drain, duplicate terminal writes, claim races, stale claimed-job recovery. | Confirmation tokens, terminal byte-idempotency, quarantine on conflict, tombstones for canceled/killed jobs, late-terminal rejection, and stale-claim requeue that clears old lease/provider binding. | BYO attach/attestation and production resource accounting still need design/tests. |

## Current Interference Handling

| Pair | Interference | Current handling | Gaps / beacon-next notes |
| --- | --- | --- | --- |
| `C01-F01/F02/F04/F08/F09` | Effective identity changes config roots, Codex install roots, history roots, service store paths, and skill targets. | Effective-path helpers centralize root/sudo path selection and service env generation carries selected roots. | Add tests that run representative install/history/skill/service paths with root/sudo-style env combinations. |
| `C02-F05/F06/F07/F10/F11/F12` | Store idempotency, epochs, and confirm tokens are the only thing preventing duplicate Teams/beacon side effects. | Teams and beacon stores record request/turn/job/machine IDs, hashes, claims, renewal epochs, replacement epochs, and confirmation tokens. Renew applies results only when provider job ID and renew epoch still match. Profile mutations, renew start/result, release/cancel, projection loss, and replacement enqueue append redacted audit records. | Expose richer admin audit/history views after deciding how much detail is useful in Teams. |
| `C03-F03/F07/F08/F11/F12` | Service or worker env filtering can remove proxy, auth, provider, or store variables needed by Graph, proxy, or beacon. | Service config has env allowlists and loopback proxy drop/keep controls; beacon provider env vars are explicit fallback, while profile-stored adapter commands avoid requiring a service reload for ordinary profile registration/update. `TestBeaconProviderEnvironmentVariablesStayDocumented` verifies README, built-in skill refs, control fallback help, and this matrix list every managed Slurm/LSF provider adapter env var. | Broader service/update env-var taxonomy should still cover non-beacon env additions. |
| `C04-F05/F09` | Static guidance can tell Codex or users to run stale commands. | README, fallback help, and built-in skill references are manually maintained. `TestTeamsControlFallbackBeaconDigestStaysAlignedWithDocsAndSkill` ties the compact control-chat beacon digest to README and built-in `cxp` skill command references. | Broader Cobra-help drift across all commands should still be generated separately. |
| `C06-F05/F06/F07/F04` | Helper-generated messages can be reprocessed by poll, retry, transcript import, or history indexing. | Polling and transcript paths ignore durable outbox echoes, rendered helper/Codex output, attachment echoes, system-injected history, and active-owner beacon reconcile/renewal stdout forms. `TestBridgePollDropsBeaconMaintenanceOutputWithoutDurableMatch` covers beacon maintenance false positives. | Keep future beacon status/renewal notices in helper-output forms and add a filter test with the notice. |
| `F01-F04` | TUI update/restart prompts can mutate the helper while history/TUI state is open. | Update prompt and restart paths are explicit commands rather than silent mutation. | TUI restart/update needs coverage with history cache and skill menu open. |
| `F01-F05/F06/F07/F08` | Helper upgrade/reload/restart can interrupt Teams polling, queued turns, running turns, workflow notices, and protected outbox delivery. | Teams service control uses pause/drain/recover state, owner heartbeat, protected outbox predicates, and completion/failure notices. Running turns block or are marked interrupted only through explicit recovery paths. Beacon lease maintenance skips paused helpers and, during drain, only renews allocations whose job has already reached a possible-start phase. | Add live service-drain tests against real scheduler adapters. |
| `F01-F08` | Auto-update release index, pending activation, and service restart all share install/lifecycle locks. | Auto-update priority/delay rules and activation status records prevent silent replacement. | Release-index generation and pending activation should be covered in CI against generated service configs. |
| `F01-F09` | Helper install/upgrade repairs bundled skills while users may have local skill edits or unmanaged copies. | Built-in skill install/repair and skip env exist; skill manager has local-change checks. | Repair behavior for modified/unmanaged built-in `cxp` skill should be explicit and tested. |
| `F01/F02-F10/F11/F12` | Helper or Codex binary changes can invalidate a beacon worker execution signature while jobs are queued/running remotely. | Beacon plan records execution signature fields on target/allocation/job structs; upgrade blocker policy has beacon-specific blocker hooks in `UpgradeBlockersForState`, lease renewal does not change the execution signature, and `TestRemainingPlanPerBeaconTargetUpgradeMatchingIsExact` simulates exact install-target matching. | Per-target Codex upgrade for beacon is still partial at runtime. Long-running beacon jobs need install-target locks and live target matching before upgrades can proceed. |
| `F02-F03` | Codex install/upgrade can run through proxy env or a saved proxy route, but bootstrap must not depend on an unhealthy helper proxy. | Codex bootstrap is separate from `cxp run` and proxy daemon state. | Test upgrade with ambient HTTP proxy and ensure saved proxy profile semantics are documented. |
| `F02-F04` | History/TUI can trigger Codex install/upgrade checks and must not replay or corrupt local sessions after install. | Compatibility failures request a manual retry after upgrade. | Add coverage for TUI/history open after upgrade refusal and after successful managed install. |
| `F02-F06` | A Teams request can fail because the Codex CLI is too old; automatic Codex upgrade must not silently retry a user turn. | Teams queues an upgrade notice and tells the user to retry manually after upgrade. Ambiguous execution is marked interrupted and not retried automatically. | Beacon worker-side Codex upgrade must follow the same "no automatic replay after possible start" rule. |
| `F02-F08` | Codex upgrade can run before foreground `teams run` listens or be requested later from Teams after a CLI failure. | `teams run --upgrade-codex` is pre-listen; queued Teams upgrade notices do not auto-retry user turns. | Ensure service/watchdog restart does not convert a queued Codex upgrade into silent task replay. |
| `F03-F04` | TUI/history uses proxy/yolo preferences while `cxp run` and proxy daemon own live process state. | Preferences and live proxy stack are separate. | Saved proxy preference should be tested with history/TUI open and active proxy commands. |
| `F03-F09` | Git-backed skill add/sync/push may need the same proxy route as command execution. | Current skill git operations use ambient env; saved proxy profiles are not automatically applied. | This is an intentional gap until skill git transport is wired through proxy profiles or documented as ambient-env only. |
| `F03-F10` | SSH proxy profile and beacon execution profile are different concepts, but a beacon profile may need a proxy route inside the worker. | Beacon profiles have `ProxyMode`/`ProxyProfile`; profile validation checks the proxy profile exists; docs and control fallback explicitly warn not to answer beacon questions with `cxp proxy`. | Worker bootstrap currently depends on doctor/proxy checks, but provider adapter templates do not validate proxy behavior. Real Slurm/LSF tests should include bad proxy routes. |
| `F03-F12` | A remote worker may need the selected proxy route, auth path, HOME, tmp, and bind mounts inside the scheduler/container environment. | Worker doctor has blockers for proxy/auth/HOME/tmp/container/modules/binds/proxy-env and stores `needs_attention` instead of accepting the machine. Beacon enqueue rechecks `MachineCanAcceptAllocation`, so stale heartbeat/doctor/provider/signature/TTL/membership drift after planning cannot still receive the job. | Real Slurm/LSF tests should include bad proxy routes and container bind/env failures. |
| `F04-F05/F06` | Local Codex history import/sync can race with Teams prompt dispatch, title updates, outbox, workflow notifications, and local-session publication. | Teams gates queued work while import is active or failed, uses transcript checkpoints, skips historical workflow notifications except detected finals, and marks ambiguous local Codex activity instead of starting a duplicate request. | Beacon worker results create Codex thread IDs remotely; history import/title sync must keep treating beacon terminal output as the coordinator-confirmed source. |
| `F04-F08` | Service env resolution can change `CODEX_HOME`/`CODEX_DIR`, which changes history/cache/store scope. | Service config generation carries selected env roots. | Add service env tests that verify history/cache roots under foreground, service, and sudo-style launches. |
| `F04-F09` | TUI starts daily skill auto-sync and exposes a skills text menu while history/cache UI is active. | Skill menu and history UI are separate UI paths. | Daily auto-sync must be tested with manual skill operations and TUI refresh. |
| `F05-F06` | Control chat fallback and Work chat task messages share durable queued execution, while dashboard commands have separate selection state. | Bridge has wrong-chat command routing, owner-only checks, duplicate message idempotency, dashboard expiry/consumption rules, and `startQueuedTurn` serialization. | Natural-language control fallback must be represented as F06 execution, not a free-form helper answer path. |
| `F05-F07` | Control commands may depend on Graph auth, control webhook state, file-write auth, and workflow fallback mentions. | Auth scopes, allowed hosts, and webhook config are stored separately from dashboard state. | Retry/control paths must surface auth loss without creating duplicate or helper-generated messages. |
| `F05-F10` | Beacon profile administration is global, while target switching and release are usually per Work chat. | Control chat accepts profile/global release/history/rollback/gc commands; Work chat accepts `beacon status`, `beacon switch`, `beacon local`, `beacon release`, and `beacon fork`. Wrong-chat commands return guidance. Profile update creates a revision instead of forcing delete/recreate while chats are bound. Profile delete archives/disables rather than breaking pinned references. | Renewal policy editing should probably be control-chat/admin scope; per-chat target switches should only select a ready policy snapshot. |
| `F06-F07` | Retry/status/cancel interact with Graph auth, original Teams messages, hosted/reference attachments, outbox status messages, and workflow notifications. | Retry rehydrates the original message and attachments, rejects empty/helper-generated originals, uses protected/transient outbox classes, status snapshots strip helper artifacts, and `TestRemainingPlanRetrySimulationBlocksAuthAttachmentAndAmbiguousReplay` simulates auth loss, attachment loss, helper-generated originals, and possible-start retry/fork boundaries. | Add bridge-level tests for real Graph auth loss, hosted/reference attachment loss, workflow fallback, and status while outbox is rate-limited. |
| `F06-F08` | Running Teams turns conflict with service reload/restart/recover, owner failover, drain, and foreground poll retry loops. | Active owner heartbeat, running-turn cancel handles, recovery notices, and `MarkTurnInterrupted` prevent silent duplicate execution after restart. Beacon cancel records remote intent separately from Teams turn status, and lease renewal records renew epochs/errors separately from local executor state. | Provider cancel remains best-effort when the site has no cancel adapter. |
| `F06-F10` | A user can switch, update, rollback, archive, or prune beacon profile history while a turn is queued or running. | Every turn snapshots its target; `SwitchProfile` applies immediately only when idle, otherwise records pending target. Existing queued turns keep their snapshot; incompatible switches require `--fork`. Profile update/rollback pins existing unversioned references to the previous revision before making a new latest revision. Profile delete archives/disables the latest revision and pins old references instead of rejecting in-use chats. Profile history GC keeps any revision still referenced by a conversation, queued turn, turn target, or allocation snapshot. Replacement uses `AllocationRequest.ProfileSnapshot`, clears only provider/machine binding, and never rereads the conversation's newer current/pending profile. | Live UX should continue to verify that archived profiles are not offered as selectable ready targets. |
| `F06-F11` | A Teams Work turn targeting beacon must wait for a managed allocation instead of running local Codex, and Work-chat release must not require allocation vocabulary. | `prepareBeaconTurnExecution` calls `PlanTurnExecution`; explicit beacon target creates an allocation request and disables local fallback. `BeaconJobExecutor` waits for allocation readiness. Teams/CLI reconcile paths run provider query/submit outside the beacon store lock. Profile-stored adapter commands are included in the allocation snapshot, so future turns can use newly registered adapters without helper reload. Renewal/cancel also run outside the lock and apply results only when fences still match. Work-chat `beacon release` cancels/drains safe single-chat resources, detaches only the current chat from shared workers, and leaves the profile binding unchanged. Control-chat release keeps preview/confirm for global shared or forced impact. `beacon local` reuses the same release path after changing future target state. | Site renew/cancel commands need real Slurm/LSF validation. |
| `F06-F12` | Teams turn completion depends on a remote worker terminal result, while cancel/retry/status still happen in the local helper. | `BeaconJobExecutor` enqueues one beacon job only after rechecking machine readiness against the current allocation, then waits for terminal. Worker terminal is accepted through request/turn/worker/lease/claim/provider fencing. Teams ambiguous execution is not retried automatically. Teams cancel tombstones beacon jobs, marks allocations canceled, removes machine job bindings, rejects late terminal writes, and attempts provider cancel when configured. | Provider cancel is best-effort when the site has no cancel adapter; retry semantics after a tombstoned remote job still require explicit user action. |
| `F07-F08` | Outbox delivery, file upload, and workflow notification can be in-flight during helper upgrade/recovery. | Protected outbox messages block upgrade/recover cleanup; transient status messages are allowed to be skipped/superseded; workflow availability suppresses owner mentions and stale workflow config falls back to control-chat mention. `TestRemainingPlanWorkflowFallbackDoesNotDropProtectedBeaconOutput` simulates webhook failure, transient drop, and protected beacon output waiting instead of disappearing. | Add bridge-level workflow fallback tests that combine protected beacon outbox with real outbox delivery state. |
| `F07-F12` | Worker artifacts/results must not let remote code send arbitrary Teams messages or files. | Worker writes fenced terminal payload to shared beacon state; coordinator owns Teams outbox. Artifact validation rejects unsafe paths, hardlinks, symlinks, changed files, bad hashes, and upload failures. | Full beacon artifact ingestion is still a sensitive path; renewal/finalizing grace must not mark a job failed while a protected artifact upload is pending. |
| `F08-F11/F12` | Service watchdog/recover can see stale beacon allocations, machines, claims, jobs, pending release intents, and idle workers. | Manual `reconcile-all` and the active Teams owner periodic reconcile query existing provider jobs, project provider state into machines/jobs, retry durable cancel/release intents, drain stale worker heartbeats, drain idle no-demand workers, and recover stale claimed/started job attempts conservatively without submitting new allocations. A separate active-owner lease maintenance pass renews due provider jobs; during service drain it only protects allocations that may already have started. Upgrade blockers inspect beacon machines/jobs/allocations. | Live scheduler tests should verify renew backoff and failure surfacing under watchdog takeover. |
| `F09-F10/F11/F12` | The built-in `cxp` skill should guide Codex toward the correct beacon commands without loading a huge reference for unrelated tasks. | Installer repairs bundled skills; the built-in `cxp` skill contains command maps and beacon/proxy distinction. Control fallback prompt includes a compact `cxp` digest. | Skill invocation is model behavior, not a hard runtime guarantee. Critical disruptive operations still need command-level safeguards and explicit helper messages. |
| `F10-F11` | Target selection and allocation submission can race with profile updates, rollback, history GC, profile deletion/archive, release, and later profile switches. | Allocation records `ProfileSnapshot`, target snapshot, and profile revision when available. Profile update and rollback store the previous revision and pin existing references before publishing the new draft/latest revision. Profile-stored adapter commands are part of the snapshot and override env fallback. Switches only affect future turns. Renewal/replacement uses `AllocationRequest.ProfileSnapshot` and never rereads a mutated current profile to decide provider resources. History GC keeps referenced old revisions. Delete archives the latest profile and pins old references; release preview/confirm prevents accidental shared-resource impact. | Live scheduler tests still need sustained site coverage. |
| `F10-F12` | Shared/exclusive isolation, profile target, and manual release affect which worker can claim a job. | Machine matching checks profile, provider job id, lease id, isolation, active jobs, chat ownership, provider RUN, fresh heartbeat, TTL, doctor, membership proof, external ownership, and execution signature. `TestGeneratedMachineReadinessIsolationAndHealthMatrix` covers these combinations, `TestBeaconJobExecutorRechecksMachineReadinessBeforeEnqueue` covers the ready-plan-to-enqueue race, Work-chat shared release detaches only the current chat, release drains started work by default, and `TestRemainingPlanResourceAccountingCoversPrewarmSharedAndExclusive` simulates profile-revision/signature-aware prewarm and resource-vector placement. | Prewarm/resource accounting policy still needs runtime implementation and production load tests. |
| `F11-F12` | Scheduler job state is not the same as an accepting worker, provider projection is not the same as job/machine state, and allocation replacement/cancel can duplicate or kill work if not fenced. | Provider RUN alone is insufficient by policy. Worker registration requires doctor and membership proof. Job claim/terminal paths fence on request, turn, worker, lease, claim epoch, and provider job. Provider projection now propagates to machines/jobs from both single and all-allocation reconcile paths, active allocations block upgrades, pre-submit cancel is rechecked, lease renewal is epoch-fenced, and provider loss auto-replaces only before any job can have started. Manual allocation cancel cancels provider jobs with no registered machine, but started work drains by default. After start intent/started, provider loss becomes quarantine/needs-attention rather than replay. Remaining-plan simulations cover BYO release ordering, fake scheduler release/renew/provider-loss ordering, load/backoff idempotency, and artifact final/needs-attention ordering. | BYO attach/attestation and production resource accounting remain open as runtime implementation work. |

## Coverage Anchors

This is the current coverage map. New changes should extend the row that owns
the touched leaf, or add a new row before merging.

| Area | Existing coverage anchors | Gaps to close |
| --- | --- | --- |
| Root/effective paths, install, helper update | `root_command_test.go`, `effective_paths_test.go`, `upgrade_cmd_test.go`, `internal/update/*_test.go`, install script contract tests. | Root/sudo interactions across history, skills, service env, and Codex install; post-upgrade built-in skill repair with local changes. |
| Codex install/self-update | `codex_install*_test.go`, `codex_self_update_test.go`, managed install tests, simulated per-beacon-target upgrade matching in `internal/beacon/sim/remaining_plan_simulation_test.go`. | Codex install lock timeout, `teams run --upgrade-codex` vs queued Teams upgrade, runtime beacon-targeted Codex install locks. |
| Proxy/run | `run*_test.go`, `proxy*_test.go`, local proxy/stack tests. | Proxy stop/prune while reused by a running command; saved proxy profile not covering skill git ops. |
| History/TUI | `codex_open_test.go`, `history_dispatch_test.go`, `internal/codexhistory/*_test.go`. | TUI skill auto-sync interaction and service/root history scope. |
| Skills | `skills_test.go`, `internal/skills/*_test.go`, skill smoke scripts. | Daily auto-sync vs manual sync/remove/push; built-in repair with modified/unmanaged skill; git proxy behavior. |
| Teams control/dashboard/filtering | `internal/teams/bridge_test.go` control fallback/dashboard/filtering clusters. | Generated drift test for fallback help, README, skill refs, and Cobra help; helper-generated filtering across poll/retry/transcript/history. |
| Teams work/retry/cancel/files | `bridge_test.go` retry/files, drain/defer/replay, status/cancel, outbox FIFO/rate-limit clusters, simulated retry/auth/attachment/possible-start replay checks in `internal/beacon/sim/remaining_plan_simulation_test.go`. | Bridge-level Graph auth loss, hosted/reference attachment loss, external-user ownership edge cases, complete queued-list status rendering. |
| Teams Graph/auth/workflow | `auth_test.go`, `graph_test.go`, `workflow_notifications_test.go`, simulated workflow fallback vs protected beacon output checks in `internal/beacon/sim/remaining_plan_simulation_test.go`. | Bridge-level workflow fallback/mention suppression tied to protected outbox and historical transcript sync. |
| Service/watchdog/auto-update/recover | `teams_service_test.go`, `teams_service_watchdog_test.go`, `teams_auto_update_test.go`, `teams_reload_test.go`, upgrade blocker tests, active-owner beacon reconcile/lease-renewal tests, beacon provider env-var documentation drift test, simulated non-beacon env-family service config drift checks in `internal/beacon/sim/remaining_plan_simulation_test.go`. | Generated service config drift against actual renderers, duplicate-process takeover, live scheduler renew behavior. |
| Beacon profile/target | `internal/beacon/beacon_test.go`, profile revision history/rollback/gc/archive tests, strict profile doctor adapter tests, explicit doctor submit/query/cancel smoke tests, `internal/teams/beacon_commands_test.go`, `internal/cli/beacon_cmd_test.go`, `TestPlanTurnExecutionLocalProviderReadyMachineDoesNotRequireProviderJob`, simulated helper-service adapter visibility in `internal/beacon/sim/remaining_plan_simulation_test.go`. | Live helper-service doctor smoke still needs real scheduler coverage. |
| Beacon allocation/provider | `internal/beacon/managed_test.go`, renew/cancel audit tests, generated combination and order-permutation cases in `internal/beacon/interference_matrix_test.go`, `provider_adapter_test.go`, profile-stored adapter override tests, provider env-var documentation drift test, provider template query/submit/cancel/renew smoke checks in `internal/cli/beacon_cmd_test.go`, opt-in real scheduler submit/cancel test `TestLiveSchedulerProfileAdapterSubmitCancel`, simulator tests, generated fake-scheduler release/renew/provider-loss order tests in `internal/beacon/sim/remaining_plan_simulation_test.go`. | Live site-specific renew policy and sustained production scheduler load tests. |
| Beacon worker/job/machine | `internal/beacon/managed_test.go`, generated combination, order-permutation, and machine readiness cases in `internal/beacon/interference_matrix_test.go`, `beacon_cmd_test.go`, CLI profile release tests, release/kill tests, refined-plan simulations, Teams cancel tombstone tests, Teams Work/control `beacon release` tests, shared Work-release detach tests, bootstrap diagnostics status tests, idle worker drain tests, switch-local auto-release tests, active-owner no-demand cleanup tests, simulated BYO release ordering, resource/prewarm accounting, and artifact pipeline ordering in `internal/beacon/sim/remaining_plan_simulation_test.go`. | Production BYO attach/attestation and full production resource accounting implementation remain open. |
| CI/scripts/docs drift | `.github/workflows/ci.yml`, release-index scripts, monitor workflow, static help tests. | Leaf-to-test anchor table generated check; env var documentation lint; workflow/job to feature-family appendix. |

## Generated Test Oracle

The generated beacon interference tests are not meant to encode incidental
current behavior. Their expected values are derived from these safety rules:

1. Provider loss before possible Codex execution may reset the allocation for a
   replacement. Provider loss after claim/start intent/started must not replay
   the user task automatically.
2. Cancel before provider reconciliation is terminal for the allocation. Later
   provider data may update raw diagnostic fields but must not revive or
   reinterpret the canceled work.
3. Provider loss before cancel keeps the provider-loss diagnostic. Claimed jobs
   stay failed/tombstoned; possible-start jobs stay quarantined while the later
   cancel intent is still recorded.
4. Renewal protects existing provider jobs only. During service drain, renewal
   is allowed only for work that may already have started; it must not create or
   extend pre-start scheduler work.
5. Stale renewal results lose to newer cancel, provider-job changes,
   replacement, or terminal job state.

## Beacon Design Implications

The current matrix points to these requirements for timeout renewal and
automatic Slurm/LSF machine management:

1. Renewal must be a separate provider operation from submit. The adapter API
   needs `query` fields for remaining TTL/deadline and opt-in `renew` and
   `cancel` paths.
2. Provider commands must run outside the beacon store lock. Store updates use
   request/job/machine epochs so a stale renewal result cannot overwrite a
   newer terminal, cancel, release, or replacement allocation.
3. Readiness must be recomputed from provider RUN, fresh worker heartbeat,
   doctor, membership proof, signature/protocol match, resource availability,
   and TTL. `machine.State=accepting` is not enough.
4. Replacement allocation is safe only before Codex may have started. The code
   only auto-resets a lost allocation when no job exists or every job is still
   `queued`; once a job reaches claim/start intent/started, renewal failure or
   provider loss must not auto-replay the user task.
5. Renewal/replacement honors the turn/allocation snapshot even if the user
   switches profiles while the job is waiting or running.
6. Helper reload/restart/upgrade and Codex upgrade blockers must include
   renewing allocations, active renewal operations, protected beacon terminal or
   artifact outbox, and unreconciled replacement attempts.
7. Teams cancel/status/retry must record remote beacon intent separately from
   the local executor context. Canceling a local wait is not the same as
   canceling a scheduler job or tombstoning a beacon job.
8. Active Teams owners run a periodic beacon reconcile that never submits new
   allocations. Lease renewal is a separate owner-aware controller; paused
   helpers skip it, and draining helpers only renew allocations that may already
   have started.
9. Static guidance must keep saying that beacon profiles are not proxy profiles,
   and command-level safeguards must enforce that distinction even when a model
   skips the built-in `cxp` skill.
