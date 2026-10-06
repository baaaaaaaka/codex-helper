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
- [x] Implement Windows smoke launch using the runner user's linked limited token when available, with a same-user restricted medium-integrity LUA-token fallback when the runner exposes no linked token.
- [ ] Verify the standard-account Windows app install/managed-launch smokes pass in GitHub CI.
- [x] Run focused Linux Go tests and the 32-test CI shard-contract suite.
- [x] Cross-compile Windows/amd64 and macOS/amd64 + arm64 test binaries.
- [x] Run Windows installer checksum/concurrency tests and Windows token/elevation/managed-launch unit tests through host PowerShell.
- [x] Run the complete Go suite, focused regression tests, cross-compiles, CI shard-contract tests, shell syntax checks, and `git diff --check`; inspect the final diff.
- [x] Record remaining native Windows AppX/UAC and macOS GUI/updater checks as environment-limited rather than claiming they passed.

## Validation notes

- `CXP_RUNTIME_DISABLE=1 go test ./... -count=1` completed but was not green: the Windows PowerShell script-parse tests could not reach PowerShell from WSL (`UtilBindVsockAnyPort`), and `internal/helperruntime/TestLaunchKeepsExplicitSameBasePrereleaseActive` failed. The changed packages' focused regression tests passed.
- Windows checksum/concurrency tests, Windows elevation/managed-launch tests, and a CIM process-path smoke passed through host PowerShell. The full managed-app install/launch smoke was not run to completion because a pre-existing ChatGPT/Codex process was detected; it was left untouched.
- GitHub CI confirmed Windows hosted jobs use elevated tokens. An isolated local account lost the Store logon-session context (`0x80070520`), Task Scheduler's `Interactive`/`Limited` configuration still returned an elevated token, and the runner's current token did not expose a linked limited token. The smoke prefers that linked token, then derives a same-user restricted token with Administrators disabled and medium integrity; both paths retain the production guard and verify the child is not an effective Administrator. The local WSL-host check derived the restricted token but could not create its child because the caller lacks `SeImpersonatePrivilege` (`CreateProcessWithTokenW` returned `ERROR_PRIVILEGE_NOT_HELD` 1314); only native Windows CI can prove the hosted runner's privilege and Store/managed-launch behavior.
- macOS Intel/Apple Silicon test binaries cross-compiled successfully. The native macOS DMG install/update network smoke is configured in GitHub CI but could not run on this Linux/WSL host; live LaunchServices and OpenAI background-updater behavior remain unverified.
- `python3 scripts/tests/test_ci_targeted_shards.py` passed (32 tests); the macOS network smoke passed `bash -n`; `git diff --check` passed.
