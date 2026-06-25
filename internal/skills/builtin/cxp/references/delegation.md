# Cross-Machine Delegation

Use this only when the user naturally asks for another signed-in Teams machine or remote agent to inspect part of the task. The user does not need to remember a command.

Active `cxp teams run` helpers publish machine capability heartbeats to the hidden Teams registry automatically after they become the owner when effective Teams write auth is available. A dedicated full-token cache is not required if the normal write token covers chat and online-meeting scopes. Each accepting machine also creates a hidden per-machine inbox and publishes its opaque inbox locator in the machine card. The heartbeat patches the same machine slot roughly every 5 minutes and marks the machine draining on shutdown when possible. Do not ask the user to run a publish command for normal use.

Model-facing flow:

1. Resolve candidates:

```bash
cxp delegate resolve --query "<what needs the other machine>" --source-session "<current-codex-thread>" --workspace-fingerprint "<repo-or-workspace-key>" --json
```

The result may include:

- `candidates[]`: all currently visible registry candidates from the inspected window, including online machines that did not confidently match the query. The model should inspect this list when the user does not remember the exact machine name.
- `candidate_token`: required for `start`.
- `new_thread_token`: starts a fresh remote Codex thread on the target machine.
- `thread_candidates[]`: reusable remote threads from recent related delegations on that same target machine.

If the JSON action is `ask_user` with online accepting candidates, first decide whether the candidate list is enough to choose a target. Ask the user only when multiple plausible machines remain or the task would be risky on the wrong machine.

The model decides whether to reuse a thread. Reuse only when the candidate summary clearly matches the current task, source session, workspace, and target machine. If there is doubt, prefer `new_thread_token`; do not ask the user to decide unless the target machine itself is ambiguous.

2. If the JSON action is `start`, write a compact task spec to a local JSON file and start. Use exactly one thread token when available:

```bash
cxp delegate start --candidate-token <token> --new-thread-token <new_thread_token> --task-file <task.json> --source-session "<current-codex-thread>" --workspace-fingerprint "<repo-or-workspace-key>" --json
cxp delegate start --candidate-token <token> --thread-token <thread_candidates[n].thread_token> --task-file <task.json> --source-session "<current-codex-thread>" --workspace-fingerprint "<repo-or-workspace-key>" --json
```

3. Poll or wait. `start` stores the target inbox route locally, so status/wait do not scan raw registry messages:

```bash
cxp delegate status --id <delegation_id> --json
cxp delegate wait --id <delegation_id> --timeout 30m --json
```

By default `wait` returns on a terminal result or on a target-side `question`, so Agent A can ask the user once without waiting for the full timeout. Use `--until terminal` only when an intermediate question should not interrupt the wait.

4. Cancel only when the user changes course or the current task no longer needs the remote result:

```bash
cxp delegate cancel --id <delegation_id> --json
```

Target-side/debug flow:

```bash
cxp delegate machine publish-once --machine-id <machine> --capability <cap> --json
cxp delegate claim --id <delegation_id> --machine-id <machine> --worker-instance <worker> --json
cxp delegate progress --id <delegation_id> --claim-id <claim> --claim-epoch <n> --machine-id <machine> --worker-instance <worker> --body "<new evidence>" --json
cxp delegate question --id <delegation_id> --claim-id <claim> --claim-epoch <n> --machine-id <machine> --worker-instance <worker> --body "<one question>" --json
cxp delegate result --id <delegation_id> --claim-id <claim> --claim-epoch <n> --machine-id <machine> --worker-instance <worker> --status complete --body "<summary>" --json
```

`publish-once` is for diagnostics and tests; normal machine availability comes from the active helper runtime heartbeat.

Normal target helpers claim and execute inbox requests automatically. Use `claim` and `result` only for diagnostics, recovery, or focused protocol tests. When using `claim` manually, execute work only if the JSON has `winning=true` and `should_execute=true`; if `recheck_after_seconds` is present, wait that long and re-read status before expensive work.

When the target helper executes a request with a reused remote thread, it passes the `remote_thread_id` as the Codex session id. If that reused context is wrong for the task, Agent B should reply with `CXP_REUSE_REJECTED: <short reason>`; the helper publishes a `reuse_rejected` terminal state and Agent A should rerun `start` with `new_thread_token`.

Task spec rules:

- Include the objective, constraints, allowed actions, and artifact references needed by the remote machine.
- Keep secrets, tokens, raw Teams chat ids, and unrelated local paths out of the spec.
- Hidden registry and inbox meeting chats are transport records, not a secrecy or deletion boundary. They remain subject to Teams tenant retention, eDiscovery, and audit policy.
- Ask at most one clarification if `resolve` returns `ask_user`.
- Do not delegate again from the remote result unless the user explicitly asks; the protocol is bounded task delegation, not open-ended agent back-and-forth.
- Treat stale/offline/draining machines as not startable even if they look relevant.
- If `start` fails because the candidate changed, run `resolve` again instead of reusing the stale token.
- If `wait` returns `reuse_rejected`, do not reuse that thread again for the same task. Start a new remote thread with the current candidate's `new_thread_token`.
- Docker or other minimal runtime tests need a CA bundle available for Microsoft Graph TLS; do not copy token JSON into logs or artifacts.

Loop and race rules:

- start is idempotent: the same source turn, target machine, and task hash reuse the same delegation id.
- Claims and terminal results are epoch-fenced by the helper; do not accept a result whose JSON state is not terminal for the winning claim.
- The registry is for machine discovery and heartbeat only. Delegation request/claim/progress/question/result records live in the target machine inbox. Status and wait read the target inbox route saved by start and can page through older messages.
- Prefer `wait` over frequent status polling. For long tasks, status checks every few minutes are enough unless the user asks for progress.
- Progress/question records should be sparse: publish on state changes, a real blocker/question, or useful new evidence. Do not stream every step.
