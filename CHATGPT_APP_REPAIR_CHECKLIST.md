# CXP Desktop App Repair Checklist

Workspace: `/home/baka/.local/state/codex-helper-workspaces/cxp-windows-launch-investigation-20261006`

The pre-existing Windows launch investigation changes and `research-artifacts/app.asar` are preserved.

## Windows identity and elevation

- [x] Detect the effective native Windows process token elevation; do not apply this policy to WSL or other systems.
- [x] Warn when native Windows `cxp app` runs elevated without automatically lowering the desktop app's privileges.
- [x] Refuse elevated or unverifiable-token install/update writes before creating or replacing managed files; allow existing managed or Store apps to launch.
- [x] Disable legacy `winget` installation for elevated or unverifiable Windows tokens.
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
- [ ] Run equivalent native Intel macOS app acceptance; cross-compilation and other Intel proxy jobs do not establish desktop app installation support.
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
- [ ] Validate both acquisition routes in GitHub CI before claiming either is supported; preserve the required Store failure if its session/source remains unavailable.
- [ ] Continue original identity/admin/UI, Intel macOS, and unsigned Defender investigation independently. Do not demand a user's live configuration or treat accepted operational constraints as newly discovered blockers.
