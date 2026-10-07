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
- [ ] Run the full credential-free managed-install/launch CI smoke on a clean Windows session. The local run was safely refused because a pre-existing ChatGPT/Codex process was detected; it was not terminated.

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
- [ ] Run the native macOS DMG/update smoke and confirm the vendor bundle version format on current Intel and Apple Silicon runners.
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
- [ ] Validate the real standard-account probe on native Windows CI before treating the environment as supported.
- [ ] Complete the managed installation/launch/update smoke independently of Store capability.
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
- Native GitHub user-context, managed app, Store, and macOS results remain pending. Do not merge or publish a prerelease until required gates pass; missing Store capability must remain visible.
