# CXP Desktop App Repair Checklist

Workspace: `/home/baka/.local/state/codex-helper-workspaces/cxp-windows-launch-investigation-20261006`

The pre-existing Windows launch investigation changes and `research-artifacts/app.asar` are preserved.

## Windows identity and elevation

- [x] Detect the effective native Windows process token elevation; do not apply this policy to WSL or other systems.
- [x] Warn when native Windows `cxp app` runs elevated without automatically lowering the desktop app's privileges.
- [x] Refuse managed install/update writes in an elevated or unverifiable process before creating or replacing files; allow verified same-user normal-worker writes and existing managed or Store apps to launch.
- [x] Disable legacy `winget` installation for elevated or unverifiable Windows tokens.
- [x] Implement same-user delegation using a verified normal desktop process context, checking SID/session/native profile/cache and inherited profile environment. Never adopt a different desktop user's identity, select a temporary CI account in production, fabricate a restricted token, or retry writes elevated. Real UAC testing rejected linked-token launch (identification-level token, error 1346) and actual normal-primary-token launch (missing privilege, error 1314); only the verified normal-parent mechanism is retained.
- [x] Limit delegation to the managed installation/update worker, preserving the invoking CXP/app launch privileges. Before any managed-cache write, the worker independently verifies its actual non-elevated token, expected identity/session, and exact target root. Parent execution waits for and propagates worker failure, rereads the verified published state, and never retries the write elevated. Use the existing process-tree cancellation helper and a bounded 15-minute worker lifetime.
- [x] Complete real same-user UAC process-creation and inert installation/update/cancellation acceptance. Native tests verify the actual child token, initial fake-package publication, unchanged refresh, changed-package replacement, and cancellation preserving the existing valid installation. Missing context, worker failure without elevated retry, SID/session/profile/cache/root mismatch, and malformed/oversized requests are covered by focused regressions. Official-package signature/download and managed launch remain covered separately by native CI, not claimed from fake-package tests.
- [ ] Validate the four native Windows AppX/unpackaged × standard/admin launch combinations against the same ChatGPT package and record package identity, token, startup logs, and exit stage.
- [ ] Validate admin/non-admin install and upgrade on Windows, including a later standard-user update of any pre-existing cache.

## Windows managed process verification

- [x] Use CIM image-path and process-creation data instead of `Process.MainModule.FileName`; verify the exact expected executable and avoid fallback launches after uncertainty.
- [x] Add focused unit coverage for launch identity, uncertain-start handling, and child-process verification.
- [x] Run a local Windows CIM path smoke with a temporary fake child.
- [x] Run the full credential-free managed-install/launch CI smoke on a clean Windows session. The earlier local refusal preserved the pre-existing host ChatGPT/Codex process.

## Windows installer integrity

- [x] Keep checksum download/manifest lookup best-effort when no usable target digest is available.
- [x] Make a retrieved target digest authoritative: local hashing errors and mismatches abort before replacing existing binaries.
- [x] Give each installer invocation unique asset/checksum temporary paths and clean them on success or failure.
- [x] Add Windows integration coverage for checksum mismatch preservation, a missing target manifest row, temp cleanup, and concurrent downloads.
- [x] Track Defender false positives separately: checksum verification fixes integrity, not unsigned-file reputation. No actual detection sample/hash was available, so the Defender root cause remains unresolved.

## macOS managed app and upgrade

- [x] Resolve the verified current-user `~/Applications/ChatGPT.app`, then the legacy `Codex.app`; never select or update `/Applications`.
- [x] Refuse symlinked `~/Applications` roots and app-bundle candidates so the stable path cannot redirect to a system copy.
- [x] Add `--upgrade-codex-app` dispatch for Intel and Apple Silicon macOS while preserving Windows/WSL behavior.
- [x] Reuse an existing verified official app at its selected path; install to the user Applications directory only when none is usable.
- [x] Serialize CXP app launch and upgrade; reject launches/updates while `ChatGPT` or `Codex` is running, including another installation copy.
- [x] Stage and verify the DMG app before replacement, compare numeric bundle versions, no-op on equality, reject downgrades, roll back handled failures, and restore a verifiable backup after an interrupted swap.
- [x] Remove only stale CXP staging directories left by an interrupted operation, while holding the per-user app lock.
- [x] Document the accepted operational constraint: close all app instances and do not reopen through Finder during upgrade.
- [x] Extend the native macOS GitHub network smoke to perform a real `--upgrade-codex-app` run after closing the app and verify the same bundle path/signature.
- [x] Run the native Apple Silicon macOS DMG/install/update smoke against the official package and verify the same selected bundle path/signature after the upgrade command.
- [x] Native Intel macOS acceptance is excluded from the release gate at the user's explicit request. Preserve existing Intel compatibility code and compile checks; do not claim native Intel acceptance passed.
- [x] Record macOS same-bundle LaunchServices forwarding and OpenAI updater/background-write behavior as unverified; these are not controlled by the CXP lock.

## Validation and CI

- [x] Add focused Windows/macOS regression selectors to CI and update Linux-only unsupported-platform checks.
- [x] Remove the failed linked/restricted-token launcher and its implementation-string assertions; keep production elevation checks unchanged.
- [ ] Verify the standard-account Windows app install/managed-launch smokes pass in GitHub CI.
- [x] Run focused Linux Go tests and the 32-test CI shard-contract suite.
- [x] Cross-compile Windows/amd64 and macOS/amd64 + arm64 test binaries.
- [x] Run Windows installer checksum/concurrency tests and Windows token/elevation/managed-launch unit tests through host PowerShell.
- [x] Run the complete Go suite, focused regression tests, cross-compiles, CI shard-contract tests, shell syntax checks, and `git diff --check`; inspect the final diff.
- [x] Record remaining native Windows AppX/UAC and macOS GUI/updater checks as environment-limited rather than claiming they passed.

## Validation notes

- `CXP_RUNTIME_DISABLE=1 go test ./... -count=1` completed but was not green: the Windows PowerShell script-parse tests could not reach PowerShell from WSL (`UtilBindVsockAnyPort`), and `internal/helperruntime/TestLaunchKeepsExplicitSameBasePrereleaseActive` failed. The changed packages' focused regression tests passed.
- Windows checksum/concurrency tests, Windows elevation/managed-launch tests, and a CIM process-path smoke passed through host PowerShell. The full managed-app install/launch smoke was not run to completion because a pre-existing ChatGPT/Codex process was detected; it was left untouched.
- GitHub CI confirmed elevated hosted tokens. Historical attempts failed separately: a temporary account lacked Store capability (`0x80070520` after App Installer registration), Task Scheduler still returned an elevated token, no linked limited token was available, and restricted-token launches encountered access-denied errors. Removing LUA restrictions left `TokenElevation=true`. These failures do not establish the cause of the original AppX identity report or the earlier access-denied errors. The failed native launcher has been removed rather than extended again.

## CI convergence execution (2026-10-07)

- [x] Work only in the investigation workspace; preserve research artifacts and the production feature changes.
- [x] Start a validation branch from GitHub main instead of pushing hypotheses directly to main.
- [x] Replace synthetic-token construction with a temporary real account using loaded native profile data; do not derive profile paths from a guessed username.
- [x] Add a native test-binary probe of the production TokenElevation query before invoking CXP or installing an app.
- [x] Separate policy regressions, managed smoke, and Store smoke. Probe each smoke's actual account before installation rather than creating a redundant third account. Independent smoke failures remain required failures.
- [x] Start each test account with a fresh environment before any child code executes; derive profile variables only from its verified native profile.
- [x] Replace process-runner implementation assertions with native behavior tests for UTF-8 output, argument boundaries, failing exit codes, and missing executables.
- [x] Preserve failure diagnostics and clean only processes belonging to the unique test-account SID.
- [x] Restrict temporary-account creation to disposable GitHub-hosted Windows runners; account profile residue is not allowed on the development host or persistent self-hosted workers.
- [x] Run focused tests, script checks, and affected target cross-compiles; record actual results below.
- [x] Review the complete convergence diff and run the final applicable local test gate once; investigate every local failure without changing unrelated code.
- [x] Validate the real standard-account probe on native Windows CI; both smoke accounts passed SID/profile/hive and actual TokenElevation=false checks. This proves the managed context, not Store capability.
- [x] Complete the managed installation/launch/update smoke independently of Store capability.
- [ ] Complete the Store smoke on a supported real user session; lack of capability remains a failed gate, not a successful skip.
- [ ] Complete the original Windows identity/admin/multiple-copy validation and native macOS verification described above.
- [ ] Merge only after required validation; recompute the release version and publish only when the release stop conditions are satisfied.
- macOS Intel/Apple Silicon test binaries cross-compiled successfully. The native macOS DMG install/update network smoke is configured in GitHub CI but could not run on this Linux/WSL host; live LaunchServices and OpenAI background-updater behavior remain unverified.
- `python3 scripts/tests/test_ci_targeted_shards.py` passed (32 tests); the macOS network smoke passed `bash -n`; `git diff --check` passed.

### Convergence validation evidence

- The workflow contract test and all 32 shard-contract tests passed. Focused Windows elevation/managed/upgrade and macOS upgrade/recovery/lock tests passed on Linux. Windows/amd64 and macOS/amd64 + arm64 CLI test binaries compiled.
- Native Windows PowerShell parsed the three controller/process/test scripts. The process-runner behavior test passed under PowerShell 7 with `RemoteSigned`, including UTF-8 output, argument boundaries, nonzero exit diagnostics, and a missing executable. Only inert fixtures were run, not an app lifecycle or local-user controller.
- The native production token API test passed on the WSL host with `actual Windows TokenElevation=false`. This does not validate the temporary-account context on GitHub runners.
- The complete local Go run finished with three explained failures, not a green result: sandbox-blocked WSL interop, an inherited `CXP_RUNTIME_DISABLE=1` suppressing a helperruntime launch test, and a long `GOTMPDIR` leaving too little filename budget in an existing Teams test. The exact parse test passed with narrowly authorized interop, the helperruntime test passed with runtime markers unset, and the Teams test passed under a validated short temporary root. No production or unrelated test changes were made to hide these failures.
- CI-only I/O is bounded to one settings file and logs per smoke, four staged scripts, and prebuilt fixtures. There is no production hot-path or state-format change; hosted diagnostics expire after seven days.
- Draft validation PR: https://github.com/baaaaaaaka/codex-helper/pull/131; code commit `05faa90d942a0df9abbd0c62f8186269e7d73072`. No merge or prerelease was performed.
- Native Windows job https://github.com/baaaaaaaka/codex-helper/actions/runs/37573335104/job/112636651471 passed policy regressions, native process-runner behavior tests, both real-account probes, and the managed credential-free install/launch/update chain. The managed package was `26.930.7945.0-0fcd11295dfd239e`, matching the reported problem B version. The runner token was elevated; both real test accounts had `TokenElevation=false`.
- Store failed explicitly before installation because App Installer was not registered for its new standard account. No registration fallback, token emulation, policy bypass, successful skip, or retry was added. This is the remaining Windows CI environment blocker; the required job stays red.
- The managed result is process/install acceptance, not proof of a usable authenticated main UI or resolution of the original AppX identity report. Do not merge or publish a prerelease with required failures.
- Windows platform integration passed the actual checksum-mismatch preservation, missing-target-row compatibility, and concurrent-download isolation tests: https://github.com/baaaaaaaka/codex-helper/actions/runs/37573335104/job/112636651374.
- Apple Silicon macOS 26 passed the official DMG install and real `--upgrade-codex-app` invocation, selected `~/Applications/ChatGPT.app`, and post-command signature/path checks: https://github.com/baaaaaaaka/codex-helper/actions/runs/37573335104/job/112636651766. A latest-to-latest invocation can be a no-op; it is not evidence of replacing an older signed bundle. Native Intel desktop app acceptance, authenticated UI checks, and vendor background updater behavior remain outstanding.
- The entire CI run completed. Full Go, race, workflow lint, runtime contracts, and the other native/loopback/distro jobs passed. Only the Store-owning Windows shard failed, and the required aggregate `Test` failed because that shard was red. This is one underlying environment blocker, not a second new defect: https://github.com/baaaaaaaka/codex-helper/actions/runs/37573335104/job/112644372118.
- Post-run evidence updates are retained locally in this checklist and in PR comments; they were not pushed as another revision merely to restart the full CI matrix. The validated code remains commit `05faa90d942a0df9abbd0c62f8186269e7d73072`, with no merge/tag/release while the Store gate is red.
- The next CI action is to supply the documented App Installer prerequisite to the real non-elevated account and run the existing Store smoke. This is CI environment preparation, not a production package-registration fallback. Do not construct synthetic tokens or relax production elevation checks. The original Windows identity/admin/UI and macOS Intel acceptance items above remain independent requirements.

## CI prerequisite experiments (2026-10-07)

- [x] Distinguish stopping merge/release from stopping investigation. Accepted macOS operational risks do not, by themselves, prohibit further work.
- [x] Prepare App Installer with Microsoft's documented current-user `Add-AppxPackage -RegisterByFamilyName` command, once, after the actual standard-token/profile probe. Keep subsequent registration, Store, and licensing errors fatal; do not introduce a fallback chain.
- [x] Add an independently gated official signed-MSIX fixture: the elevated CI runner performs `Add-AppxPackage -Path`, then a verified standard account requests current-user registration and launches CXP. This avoids winget/Store acquisition, not Windows user identity or the package's privileged service preparation, and does not replace the Store-install gate.
- [x] Require a fresh account without an existing OpenAI.Codex package so a preinstalled app cannot make the Store install test pass without installation.
- [x] Run the 32 shard-contract tests, the Go workflow contract, native PowerShell AST checks, formatting/diff checks, and inspect the complete experiment diff.
- [x] Re-download the official MSIX and inspect its manifest: `OpenAI.Codex` x64 `26.930.7945.0`, SHA256 `0fcd11295dfd239ef8b6a2cb088a4ead18316b80a87e0c0e1abbad9d830edef3`. It declares `packagedServices`/`localSystemServices` and `CodexSandboxService.OpenAI.Codex` with `StartAccount=localSystem`; Microsoft's MSIX documentation requires admin privileges to install service-bearing packages. It lists Windows.Desktop >=10.0.19041.0, not a separate framework-package dependency. Therefore do not assume direct MSIX installation is non-elevated merely because it bypasses winget.
- [x] Run both routes independently in GitHub CI and retain the failing Store gate; only the direct signed-package preparation/registered-launch route passed.
- The independent prerequisite experiment is commit `6dd69d1e46254f2a524c6e6d4ffdd4b768d7d4cc`, run https://github.com/baaaaaaaka/codex-helper/actions/runs/37578818053. It changes CI preparation and coverage only, not production elevation checks. Its native results remain pending.
- The freshly downloaded MSIX's `app/resources/app.asar` and the preserved research copy have identical SHA256 `611d6da979d8bbabfec97dd90dcce27a9522e7016e6ccf135d59cab693ab08da`. In `.vite/build/bootstrap-C8gUBg5L.js`, the inspected missing-identity branch logs `windows_runtime_framework_identity_unavailable`, leaves the identity-dependent core gate disabled, selects the bundled runtime, and continues to import/run main startup. Updater initialization separately reports missing package family as unavailable. These specific branches are not a demonstrated fatal UI identity requirement; later native startup/UI behavior still needs validation.
- Native result: https://github.com/baaaaaaaka/codex-helper/actions/runs/37578818053/job/112653622632. The managed chain passed again. Official App Installer registration succeeded for the real standard account and winget found ChatGPT in `msstore`, but installation returned `0x80070520` (specified logon session does not exist). The remaining failure is no longer an absent App Installer; the exact underlying Store call is not identified by the console output alone. Do not equate loaded profile/native session ID with all Store logon capabilities, or add credentials/token emulation to hide the error.
- In the same native job, the verified elevated runner installed the official signed MSIX; a separate verified standard account successfully registered the package family and launched CXP, observing the installed ChatGPT process. The package was `OpenAI.Codex_26.930.7945.0_x64__2p2nqsd0c76g0`, with the exact reported problem B package hash. This supplies Store-independent registered-package/process coverage without changing production permissions. It does not prove Store acquisition, kernel process package identity, authenticated UI usability, or resolution of problem A.
- Next Store diagnostic is the native winget/deployment failure stage in the temporary account, followed by a supported-session decision. Do not blindly add another launcher or mark the existing Store install gate green because the independent signed-package fixture passed. Original launch-identity/admin/multiple-copy and macOS upgrade acceptance can proceed independently against the now-validated fixtures.
- [x] Add current-test-account WinGet log retention in the Store smoke's `finally` path. Retain at most three newest `.log` files, each at most 4 MiB, through the existing seven-day artifact uploader; never collect other profiles, credentials, or non-log files. Collection failures warn without replacing the original Store failure. No production behavior, package registration, launcher, or fallback changes.
- [x] Native inert PowerShell tests cover absent logs, newest-three selection, content preservation, exclusion of non-log/oversized files, and preservation of the original exception on collection failure. The first UNC execution was refused by RemoteSigned; copying only the two inert scripts to a unique native temporary directory passed under the same policy and was cleaned up. All 32 shard contracts, the affected root Go package, and diff checks passed.
- [x] Retrieve the retained WinGet diagnostics from the next native run before selecting another Store environment change.
- Retrieved native diagnostics for commit `f6ff788a840580850935dc2727bde64a73c955bf`: https://github.com/baaaaaaaka/codex-helper/actions/runs/37581290103/job/112661564892. WinGet 1.11.510 on Windows.Server 10.0.26100.33438 received HTTP 200 for both source metadata and the ChatGPT manifest; the source reported no authentication requirement. Agreements were accepted, then a WinRT exception returned `0x80070520` before the package-install-start message. Matching v1.11.510 source places `EnsureStorePolicySatisfied` before `MSStoreInstall`; the policy path constructs `AppInstallManager` and calls its policy APIs. This narrows the failure to pre-install Store service/capability setup, not a demonstrated network, missing-App-Installer, package-service elevation, or managed-cache writer failure. The exact policy call is not resolved by that log.
- A read-only `AppInstallManager` construction check succeeded on the WSL Windows host under Windows PowerShell 5.1. PowerShell 7 lacked the WinRT type projection, so it is not a valid equivalent control. Add the same read-only constructor/registered-package-state probe to CI diagnostics, preserving the original smoke result; no registration or install fallback is added.
- [ ] Compare the CI account's read-only Store constructor and registration state with the successful host control. Native AST parsing, all 32 shard contracts, root Go package, and diff checks passed for the diagnostic addition.
- Constructor comparison completed for `6d834df51fe78185135dcea99631f44e6c968983`: https://github.com/baaaaaaaka/codex-helper/actions/runs/37582951887/job/112666881465. CI reports `Microsoft.WindowsStore registered=False`, `Microsoft.DesktopAppInstaller registered=True`, and successful `AppInstallManager` construction. Consequently missing App Installer and manager activation are ruled out for this run; missing Store registration is an observed difference, not yet proven causal. The matching WinGet pre-install policy calls remain the next read-only diagnostic target. Managed and independent signed-package smokes both passed again; the Store gate remains failed. No installer/session fallback was introduced.

## Same-user installation delegation implementation (2026-10-07)

- [x] Add a native-only install worker entered before helper-runtime/root CLI dispatch. Pass a bounded structured request over stdin; keep proxy settings and refresh intent, and reuse the existing managed installer/lock/state publication.
- [x] Run focused CLI/entry-point regressions and 32 CI shard-contract tests. Extend the existing native Windows policy selector and entry-point test invocation, not a new CI matrix or fallback chain.
- [x] Run inert native Windows worker tests, initial fake-package publication and unchanged refresh, and native token/profile guard checks. The host has a standard token: this is not evidence of successful elevated UAC process creation.
- [x] Cross-compile Windows/amd64 and macOS/arm64 + amd64; retain Intel compile coverage despite the waived native acceptance.
- [x] Audit startup/write cost: ordinary dispatch adds only an argument check, valid managed-cache reuse does not spawn a worker, and only install/update serializes a bounded request plus fixed-size state. No extra persistent state or polling was added.
- [x] Complete the local full Go gate and native successful inert UAC/cancellation acceptance before candidate submission. Docker bootstrap/no-exec temporary-directory failures were environmental; a subsequent run exposed a direct `os.Executable` usage, fixed using `helperpath.RawExecutable`. The final container run uses a writable executable `~/temp/t-*` scratch root (recognized by the existing history filter), the normal CI parallelism and 20-minute package timeout. Every Go package passed without changing history or Teams production code.
- Final native inert tests also pass with inherited proxy/custom-root environment and token/window/cancellation configuration assertions. The executable-directory full run finished non-green: the history fixture above, the subsequently fixed executable-path guard, and the Teams package exceeding the explicitly bounded 5-minute package timeout (Teams/store completed in 294.7 seconds). No Teams production changes or permission weakening were made; no full-suite success or prerelease readiness is claimed.
- Subsequent full run passed: `go test ./... -count=1 -parallel=16 -timeout=20m` inside the bundled ephemeral container; log retained in sibling scratch `cxp-ci-convergence.FKlzPI/delegation-full-suite.log`. The earlier non-green attempts remain historical, not the current local result.
- The real UAC inert acceptance passed with Defender antivirus and real-time protection enabled (`4.18.26080.4`, definitions `1.459.576.0`). This does not prove that future unsigned release hashes cannot trigger false positives or resolve the original detection report.
- Read-only Store policy controls passed on the native host: manager construction, `IsStoreBlockedByPolicyAsync=False`, and `GetIsAppAllowedToInstallAsync=True`. CI now probes the exact two WinGet policy operations independently with bounded waits, preserving the original Store smoke result; no install fallback or gate relaxation is introduced.
- [ ] Continue original identity/admin/UI and unsigned Defender investigation independently. Native Intel macOS acceptance remains waived; do not reintroduce it as a blocker or demand a user's live configuration.

## Approved CI acceptance boundary (2026-10-07)

- [x] The user accepts direct official MSIX as the required packaged installation/launch gate, while retaining actual Store acquisition as a separate interactive-environment acceptance item. This supersedes the earlier requirement for hosted Store acquisition, not production permission checks.
- [x] Run 37593030369 passed the managed and signed-MSIX chains. Its Store policy diagnostic resolves the failing operation: `IsStoreBlockedByPolicyAsync=false`, then `GetIsAppAllowedToInstallAsync` fails with `0x80070520` before installation. App Installer registration and manager construction succeeded; absent Store registration is observed, not proved causal.
- [x] Remove only the mandatory non-interactive Store-acquisition step. Preserve independently required managed and signed-MSIX smokes, their actual standard-token/profile probes, and failure propagation. Retain the Store script and its diagnostics without a successful skip or production fallback.
- [ ] Wait for all required jobs on the final revision before merge/release; verify current main, submission identity, latest published RC, and final Release artifacts.
- [ ] Separate Store acceptance: use a disposable native Windows desktop with a fresh **interactively logged-in standard account**, App Installer/Store available, and no existing `OpenAI.Codex` registration. Build the helper and CLI token-probe test binary from the candidate revision. In that account's PowerShell 7, set `CXP_RUNTIME_DISABLE=1` and `CXP_WINDOWS_APP_BACKEND=legacy`, run the token probe with `-test.run '^TestCurrentWindowsTokenElevationQuery$' -test.v` and require `TokenElevation=false`, then run `scripts/ci/codex_app_network_install_smoke.ps1 -Helper <absolute-helper-path>` without `-RegisteredPackage`. Retain output/version/hash. Do not run against a user's existing app or use the development host for source-built lifecycle tests; the script installs the actual app. A registered-package smoke is not Store-acquisition evidence.
- Final-boundary run `37595362599` passed managed and signed-MSIX gates (job `112707125121`), all full Go/race and other platform jobs, but the unchanged Ubuntu 20.04 supervisor public-lifecycle script returned exit 1 without identifying a failed assertion (job `112713576062`); aggregate failure follows. Do not describe the run as green. The same candidate passed eleven isolated supervisor traces after removing the harness-only state override and using executable private scratch; this is not proof of the hosted failure's cause. Add only inherited ERR line/exit diagnostics to the existing smoke, retaining assertions and production behavior, instead of a retry/timeout/permission workaround.

## Approved optional SQLite sidecar race repair (2026-10-07)

- [x] Run `37600373699` passed the Ubuntu 20.04 supervisor and all desktop gates, but Windows normal transcript partition 1 failed during cold SQLite schema preparation with raw access-denied. Independent job runners and call-path review exclude app-install delegation as a cause. No blind CI rerun was performed.
- [x] Locate native file operations before changing behavior. Natural cold-start repetitions caught optional SHM disappearance at `CreateFile(FILE_READ_ATTRIBUTES)` and WAL disappearance at `GetFileAttributes`, after the initial `Lstat` succeeded. A separate deterministic native `FileDispositionInfo` test proves delete-pending can produce `ERROR_ACCESS_DENIED` at the unchanged identity `CreateFile` call without changing ACLs. The original CI error 5 has not been tied to its exact API/path; mechanism evidence is not incident proof.
- [x] Obtain explicit user approval to include the narrow repair and regressions. Review confirms the same cold-start ordering is possible in production; do not pre-load the fixture to hide it.
- [x] Re-read only optional WAL/SHM identities after transient not-found or Windows permission errors, at most three times with two 10ms waits. Success still requires the complete existing identity/reparse validation or an actual absent `Lstat`. Persistent errors propagate; the main DB remains strict. Remove temporary production error wrappers and timing observers.
- [x] Native regression tests pass, including real delete-pending error 5, persistent error 5 remaining an error, subsequent confirmed absence, validated replacement, rejected reparse/unrelated errors, and required main DB. Add their explicit selector to the existing Windows platform-integration job; retain the unchanged cold-start listener test in normal/race recovery jobs.
- [x] Fixed-revision native cold listener acceptance passed 100 repetitions without fixture warmup. The complete Go suite passed inside the bundled ephemeral container (`go test ./... -count=1 -parallel=16 -timeout=20m`), and all 32 shard-contract tests plus root workflow contracts passed. Logs remain in sibling scratch `cxp-ci-convergence.FKlzPI/fixed-teams-cold-start-stress.log` and `sidecar-full-suite.log`.
- [ ] Require all final GitHub jobs before merge and prerelease; do not describe the earlier failed runs as green.
- [x] Audit startup/I/O cost: normally one unchanged identity query per optional sidecar, no extra writes or persistent state. Only transient-error paths repeat metadata reads, with at most 20ms added wait per sidecar; no polling loop, growing scan, permission change, or broader SQLite recovery policy.
