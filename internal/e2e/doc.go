// Package e2e is the root of the black-box end-to-end harness for the Teams
// service (CLEANUP-PLAN §10). It contains test code only:
//
//   - fakegraph: strict in-memory Microsoft Graph (Teams chats/messages,
//     onlineMeetings) and login.microsoftonline.com token endpoint, with
//     fault injection, a full request log and a contract-violation list.
//   - harness: per-scenario isolated environment, sandbox HTTP transport,
//     SUT interface, fake Codex, rollout writer, waiters and verdicts.
//   - sut/oldcore: adapter for the current internal/teams core (exported API
//     only); sut.MustSelect picks the adapter from E2E_SUT.
//   - steps: shared black-box scenario steps.
//   - scenarios/*: scenario packages named after the SPEC guarantees (R-*).
//
// Scenarios observe only the fake Teams service, the fake Codex and the SUT
// output. Run them only in the offline Docker runner (no network, throwaway
// HOME), e.g. `go test -json -count=1 ./internal/e2e/...`; each scenario
// prints one `E2E-VERDICT {json}` line with its structured result.
package e2e
