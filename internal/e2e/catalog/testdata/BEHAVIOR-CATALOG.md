# Behaviour catalog — rebuilt Teams core vs SPEC (phase 2, E2E layer "behaviour catalog")

- Audited code: `cleanup/core-integration-20261104` @ `87c27310` (read-only), plus the unmerged `cleanup/owner-ux-20261105` treated as present (`cxp teams stop/start`, control `shutdown`, capacity `continue`, "⏳ Queued" ACK, park texts).
- Authority order: owner decisions (`history/DECISIONS-zh.md` incl. 2026-11-05 batches, `history/SPEC-review-zh.md` 决定-1..9, "意见" all empty, 11-03 "都没有意见" = defaults accepted) > `history/SPEC.md` > v0.1.20 (`codex-helper-v0120` @ `2ae5e748`) > CORE-DESIGN / COVERAGE-REPORT / E2E-BLACKBOX-REPORT.
- Canonical copy: `internal/e2e/catalog/testdata/BEHAVIOR-CATALOG.md` on branch `cleanup/catalog-20261106` (read by the catalog test). `cxp-cleanup-work/BEHAVIOR-CATALOG.md` is a copy.
- Paths: core paths are relative to `internal/`. v0.1.20 paths are relative to `codex-helper-v0120/internal/teams/`; `B:` = `bridge.go`. Exact v0.1.20 texts for every gap are in Appendix A.
- Catalog test: `internal/e2e/catalog` (stdlib only). Default run checks consistency; strict run (`CXP_CATALOG_STRICT=1`, or `go test ./internal/e2e/catalog/ -args -strict`) fails while any `keep` row has e2e `NONE`.

## 1. Prioritised gap list (MISSING / PARTIAL `keep` features)

Priority = how likely the owner hits it: phone use of the control and work chats, reopening old sessions, heavy skill use. P1 = hit in normal daily use; P2 = hit weekly or on a specific workflow; P3 = rare / CLI / ops.
Size: S < 100 LOC, M 100–400, L > 400 (product code, excluding tests).

### Batches (for parallel implementers; grouped by the files they touch)

| batch | scope | main files | depends on |
|---|---|---|---|
| **C** parser | command grammar: prefixes `!` `/` `cx` `codex` `helper:`; bare work-chat `help/model/models/effort/efforts/default`; v0.1.20 short names (`st cl rt rs x f rn d ph`); Wrong-chat, unknown-command, coworker-reject, bare-@codex texts; `helper close`; help + `help advanced`; ready-text fix | `core/engine/receive.go` (classification ~:720-790), `core/engine/commands.go` (`controlCommand` :227, `workCommand` :282, help :194/:209), `core/engine/control.go` :329 | lands **first**: it splits dispatch so A/B/E add handlers in their own files |
| **A** dashboard + publish | `p/w/s` workspaces/sessions + number selection with 10-min view; `continue/c/publish/import` + history import; `open/o`, `details/d`, `status/st` list; `mkdir/m` + path hint; `new` parity; `helper publish-history [full]` | new `core/engine/dashboard.go`, `core/engine/import.go`; `mirror.go` (shared import cursor); `commands.go` hooks | C |
| **B** control Q&A + settings | control Q&A (`ask/q` + fallback), hidden context, control history; `default …`; `model …`/`effort …` full grammar in both chats; `helper webhook …` from chat; skills `push` review | new `core/engine/control_qa.go`, `commands_ext.go` (`cmdModel` :139, `cmdEffort` :209), `core/skillcmd/skillcmd.go` | C |
| **D** Teams edits | PATCH user marker; freeze notice (PATCH latest); ASR transcript PATCH; chat title auto-update + `rename` / `rename hostname`; mark unread; control title 🏠 | `channel/teams` (Edit/RenameChat/MarkUnread already exist on the adapter), `core/engine/render.go`, `maint.go` :191, `files.go` :185-215, `runner.go` `finish` :515 | none (parallel with C) |
| **E** turn commands | `cancel <turn-id>` and naming the prompt; general `retry last/<turn-id>`; fork parity (child gets parent history, fork while busy); `helper file`; StallLimit; setup-error classes; drain "saved this message" notice; restart `now`/`force` semantics | `commands.go` (`cmdCancel` :416, `cmdRetry` :483, `cmdRestart` :570), `commands_ext.go` `cmdFork` :381, `runner.go` (`userError` :857), `silence.go` :52/:78, `upgrade.go` :227 | C |
| **F** prompt input | prompt envelope (User section, safety block, outbound root + manifest instructions, math contract); quotes to Codex; inbound attachment limits; `<oai-mem-citation>` stripping; raw API-key rejection | `runner.go` `turnInput` :321, `receive.go` (quotes are parsed in `channel/teams/receive.go:271` but dropped), `transcript/record.go`, `outbound.go` | none |
| **G** CLI | `teams control --recreate`; `teams chat recreate`; `teams chat publish-history`; `teams send-file`; `run --once/--interval`; `teams pause/resume` (if owner confirms); `status` installed/pending version + last poll; install-host pin | `cli/teams.go` (:311 control, :388 run), new `cli/teams_chat.go`, `cli/teams_newcore_status.go` :106, `core/store` Ident | A for publish-history, D for recreate notices |
| **H** stats | rollout-based `helper stats` (resets, model/tier, subagents, cache hit, unknown bucket) | `commands_ext.go` `cmdStats` :101 / `formatUsage` :76, new rollout usage reader under `core/transcript` | none |

### Gap list

| # | id | pri | batch | size | gap (one line) | v0.1.20 location → exact text (see Appendix A) | depends |
|---|---|---|---|---|---|---|---|
| 1 | F-CTL-10 | P1 | B | L | **DONE** (batch B, `cleanup/gap-b-20261106`, scenarios CTL-QA, CTL-QA-RESTART, CTL-QA-VOICE): `ask/q <text>`, unrecognised control text and a control voice clip run a control-chat Codex turn (core/engine/control_qa.go) with v0.1.20's ACK, envelope, history and low effort; was: unrecognised control text and `ask/q` got the help / "not available" | `runControlFallback` B:4901, ack `queueControlFallbackAck` B:7488 "❓ **Codex received your control-chat question.**…", prompt `artifact_handoff.go:74`, history `control_history.go` (500 entries, tail 20/8000 chars), session `__control_fallback__` effort low | C |
| 2 | F-CTL-5 + F-MIR-5 | P1 | A | L | **DONE** (batch A, `cleanup/gap-a-20261106`, scenarios DASH-FLOW, IMPORT-AGAIN, IMPORT-CRASH; `core/engine/import.go`): `continue/c/publish/import <n/id>` and the history import into a new/reopened work chat do not exist | `publishCodexSessionWithProgress` B:15710/:15741; "✅ Work chat created/reopened…Preparing local Codex history now…", "Import complete. This chat is ready; reply here to continue the Codex session.", "Published local Codex session as %s: %s…" (B:16174, B:15918) | C, A-dashboard |
| 3 | F-CTL-3 | P1 | A | L | **DONE** (batch A, `cleanup/gap-a-20261106`, scenario DASH-FLOW; `core/engine/dashboard.go`): No `p/projects/w/s/sessions/history` dashboards and no bare-number selection | `formatWorkspaceDashboard` B:19702 "## 📁 Workspaces…", `formatWorkspaceSessionsDashboard` B:19840 "## Sessions…", `resolveControlSelection` B:20304, view TTL 10 min `dashboard.go:17`, error texts `controlCommandErrorMessage` B:20769 | C |
| 4 | F-CTL-6 | P1 | A | M | **DONE** (batch A, `cleanup/gap-a-20261106`, scenario DASH-FLOW): `open/o` and `details/d` missing; `st/status` prints helper status, not the "## Active Work chats" list with links and `r <id>` hints | `formatOpenControlTarget` B:20036, `formatDetailsControlTarget` B:15573, `formatSessionList` B:15242 | A-dashboard |
| 5 | (parser) F-TRN-7/F-TRN-8/F-CTL-9 | P1 | C | M | **DONE** (batch C, `cleanup/gap-c-20261106`, scenarios CMD-WORK-FORMS, CMD-CONTROL-FORMS; grammar `core/engine/parse.go`, one handler table `core/engine/cmdtable.go`; commands of other batches answer "`x` is not available in this helper version yet." until they land, never Codex): Work chats accept only `helper …`: bare `model`, `effort`, `help`, `default` and `!status` / `/x` / `cx x` go to Codex as prompts (the ready text even advertises `!status`) | `dashboard_commands.go` `workBareCommandName` :221, `splitDashboardCommand` :603, short names :571 | — |
| 6 | F-WRK-4 | P1 | C | S | **DONE** (batch C, scenario CMD-CLOSE): `helper close` missing although the work-chat ready text (`control.go:334`) advertises it | B:8007 "Session closed. The helper will no longer read or respond in this Teams chat.\n\nThis chat remains visible in Teams. Use the 🏠 control chat to open or create another work chat." | C |
| 7 | F-TRN-14 | P1 | F | M | **DONE** (batch F, `cleanup/gap-f-20261106`, scenario F-TRN-14): Codex receives the raw text: no User-message section, no Teams safety block (do not restart/kill the helper…), no outbound-root/manifest instructions, no math contract | `artifact_handoff.go:28` `TeamsCodexPrompt`, :184 `teamsUserMessagePrompt` | — |
| 8 | F-RND-4 / R-8 | P1 | D | M | DONE (batch D, EDIT-LABEL). No user marker: the engine never calls the adapter's `Edit` (PATCH) | `annotateIncomingUserMessage` B:4081/:4109 | — |
| 9 | F-RND-9 | P1 | F | M | **DONE** (batch F, `cleanup/gap-f-20261106`, scenario F-RND-9): Quoted/replied messages are parsed (`channel/teams/receive.go:271`, `quoteFrom` :329) but never reach Codex | v0.1.20 quote context in `artifact_handoff.go` user prompt (inline ≤2000 runes, else file) | — |
| 10 | F-TRN-7 | P1 | B | M | **DONE** (batch B, `cleanup/gap-b-20261106`, scenarios CMD-MODEL, CMD-MODEL-CTL): v0.1.20's `model` grammar in both chats (core/engine/modelcmds.go:300 `cmdModelChat`): bare `model`/`models`, `status`, `list` (and natural-language list requests), `switch/use <m>`, `reset`, `fork <m>` (work chat: new Work chat, same cwd, model pinned), `effort …`; removed third-party setup subcommands answer "Third-party model setup was removed…"; was: status/list/reset/<id> only, control chat list-only | `model_profile_commands.go:15` (control), :125 (work); "Model profile switched for this Control chat…", "Forked Work chat %s with model profile %s:\n<url>" | C |
| 11 | F-TRN-8 | P1 | B | S | **DONE** (batch B, `cleanup/gap-b-20261106`, scenario CMD-EFFORT): v0.1.20's `effort` grammar in both chats (core/engine/modelcmds.go:704 `cmdEffortChat` → :469 `effortCommand`): bare `effort`/`efforts`, `status`, `list`, `set/switch/use <e>`, `reset`, bare value; was: `effort <e>` only, none in the control chat | `reasoning_effort_commands.go:20/:32`; "Current chat effort (…)", "Reasoning effort updated for this … chat…" | C |
| 12 | F-CTL-15 | P1 | B | M | **DONE** (batch B, `cleanup/gap-b-20261106`, scenario CMD-DEFAULT): control chat `default status`, `default model status/list/set/reset`, `default effort status/list/set/reset` (core/engine/defaults.go:89 `cmdDefaultControl`, v0.1.20 grammar and texts with "future launches" → "newly created Teams chats"); kept in the store, applied by :40 `newChatThread` to `new`, `model fork` and the control chat's first question; work chat keeps v0.1.20's refusal | `default_commands.go:35`, texts in `internal/cli/teams_defaults.go:45`; work chat: "`default` changes global settings and can only be used in the Control chat. Use `model` or `effort` to change only this Work chat." | C |
| 13 | F-WRK-2 | P1 | D | M | DONE (batch D, EDIT-TITLE). Work-chat titles never update from the first prompt / Codex title (stay "s001 · <dir>"); no `helper rename` | `maybeUpdateWorkChatTitleFromLocalSession` B:10749 (gate B:10804), `renameSessionChat` B:10349 "Work chat renamed.\n\nNew title:\n…" | — |
| 14 | F-CTL-4 | P2 | A | S | **DONE** (batch A, `cleanup/gap-a-20261106`, scenario NEW-MKDIR): `new` rejects missing and relative dirs (v0.1.20 creates 0700, resolves relative/`~`), no bare `new` in the opened workspace, no `--model` | `createWorkspaceDirectory`/new in B (help B:5150 "create the directory if missing") | A-dashboard for bare `new` |
| 15 | F-CTL-7 | P2 | A | S | **DONE** (batch A, `cleanup/gap-a-20261106`, scenario NEW-MKDIR): No `mkdir/m` and no path-hint reply for path-like text | B:7830 "Directory is ready: <dir>\n\nNext: send `new <dir>`…"; B:7358 "📁 Detected path: `%s`…" | C |
| 16 | F-TRN-3 | P2 | E | S | **DONE** (batch E, scenarios CANCEL-CMDS, R-20-33): `helper cancel <turn-id>` missing; replies do not name the cancelled prompt; no control-chat cancel (needs Q&A) — control-chat cancel stays batch B (hook `controlCancelText`) | `cancelTurnCommandForScope` B:9490; usage "usage: `helper cancel last`, …, or `helper cancel <turn-id>`…" | C |
| 17 | F-TRN-4 | P2 | E | M | **DONE** (batch E, scenarios RETRY-CMDS, SETUP-ERR): `helper retry` only for duplicate-ignored / Codex-too-old turns; no `retry last` for failed or interrupted turns, no `<turn-id>`; interruption text says "resend it" | `retryTurnCommand` B:8236; "no failed or interrupted turn is available to retry in this session." etc. | C |
| 18 | F-WRK-5 / R-22 | P2 | D | S | DONE (batch D, EDIT-FREEZE). Auto-park after 48 h (`maint.go:191`) is silent: no freeze notice, so the owner does not know the chat stopped listening | freeze notice (PATCH latest message) per SPEC F-WRK-5; text must say messages are read after the wake (owner 11-05 Q6) | D-edit |
| 19 | F-RND-11 | P2 | D | S | **DONE** (batch D: transcript PATCH, EDIT-VOICE; batch B: control-chat voice is a control question, CTL-QA-VOICE) | `asr_input.go` (PATCH under card) | D-edit, B for control |
| 20 | F-WRK-10 / F-CTL-13 / R-41 | P2 | E | M | **DONE** (batch E fork while busy + rc.6 texts, scenario FORK-RUNNING; fork history `cleanup/fork-history-20261106`, scenarios CMD-FORK, FORK-RUNNING, FORK-CTL, FORK-CRASH; `core/engine/fork.go`): the fork chat gets the parent's visible history up to the cutoff (a snapshot of the parent's rollout, imported as catch-up under the child's delivery keys, crash-safe), then rc.6's "History import complete. The new Codex chat is ready.", and only after that marker is delivered the parent gets "✅ Fork complete" with the link; was: child started at the current position, fork refused while a turn ran, texts differed | SPEC F-WRK-10 steps 1-5; v0.1.20 0.1.21-rc.6 `1a210e6` | A-import |
| 21 | F-STA-1 / R-42 | P2 | H | L | Stats are store meta counters, not computed from rollouts: no reset reconstruction, no model/tier sections, no subagents, cache hit or `unknown` bucket | `token_stats.go:1013/:1083` "STATS: Codex tokens…" | — |
| 22 | F-MIR-6 | P2 | A | M | **DONE** in chat (batch A, `cleanup/gap-a-20261106`, scenario IMPORT-AGAIN) and as the CLI `teams chat publish-history <sel> --full [--force] --yes` (batch G, `cleanup/gap-g-20261106`, scenario PUBLISH-HISTORY-CLI): `helper publish-history [full]` / `ph` missing (and the CLI form) | B:8051, B:16039, B:16083 "Usage: `helper publish-history` imports the pending backlog…" | A-import |
| 23 | F-RND-10 | P2 | E | S | **DONE** (batch E, scenario SEND-FILE; the CLI `cxp teams send-file` batch G, `cleanup/gap-g-20261106`, scenario SEND-FILE-CLI): `helper file <rel-path>` / `!file` missing (manifest upload works) | `sendFileCommand` B:9748 "usage: `helper file <relative-path>` or `!file <relative-path>`\nOutbound root: <root>" | C |
| 24 | F-CTL-8 | P2 | D | M | DONE (batch D, EDIT-RENAME). `rename <title>` and `rename hostname <name>` missing | `renameControlChat` B:10683, `renameMachineHostname` B:10507 | D-rename |
| 25 | F-CTL-9 | P2 | C | S | **DONE** (batch C, scenario CMD-HELP): Only a short help; no `help advanced`; texts differ from v0.1.20 | help B:5123, advanced B:5150, work B:5216/:5241 | — |
| 26 | F-CTL-16 | P2 | C | S | **DONE** (batch C, scenarios CTL-ATTACH, CMD-CONTROL-FORMS; the path hint for `!./x` stays with batch A): Control attachments not rejected with a text; no "⚠️ Wrong chat" for work-only `helper …`; quotes ignored everywhere | `text.go:113/:121`, `isWorkOnlyHelperCommand` B:7330 | — |
| 27 | F-WRK-12 | P2 | C | S | **DONE** (batch C, scenario GROUP-CMDS): Bare `@codex` with no text gets no reply | B:2031 "Mention received, but there is no Codex request text. Add the task after `@codex`." | — |
| 28 | F-WRK-13 / R-27 | P2 | C | S | **DONE** (batch C, scenario GROUP-CMDS): Coworker `helper …` commands are ignored silently (no rejection), coworker ACK text differs | `rejectExternalWorkCommand` B:8218 "Only the helper owner can run helper commands in this Work chat. Send a normal question or task here and I will pass it to Codex."; coworker ACK "⏳ Codex received your question. Request accepted." | — |
| 29 | F-CTL-11 / R-18 | P2 | E | S | **DONE** (batch E, scenarios RESTART-DRAIN, RESTART): `helper restart` restarts at once when idle (no confirmation step), `restart now` interrupts running work instead of waiting, no `restart force` | help B:5150 "`helper restart` - show the safe restart confirmation", "`helper restart now` - restart … after sending a confirmation" | — |
| 30 | F-TRN-18 | P2 | E | S | **DONE** (batch E, scenario SILENCE; coordinator ruling: match v0.1.20, no stall kill): `StallLimit` 1 h (`silence.go:52/:78`) kills a silent turn; SPEC and v0.1.20 never kill ("I will not retry automatically. To stop the running request, send `helper cancel running`") — needs owner confirmation | B:92-99, `sendIdleStatus` B:12037 | — |
| 31 | F-TRN-1 | P2 | D | S | DONE (batch D, EDIT-UNREAD). Final answers do not mark the chat unread (adapter has `MarkUnread`, engine never calls it) | SPEC F-TRN-1 step 4 | — |
| 32 | F-TRN-16 | P2 | E | S | **DONE** (batch E, scenario SETUP-ERR): Not-started turns get a generic text; no "⚠️ I couldn't start Codex due to a setup issue on my side…" with the class (Node missing, auth…) | SPEC F-TRN-16 | — |
| 33 | F-CTL-14 | P2 | B | M | **DONE** (batch B, `cleanup/gap-b-20261106`, scenario WEBHOOK-CTL): control chat `helper webhook` help / `setup` / `status` / `<https-url>` / `test` / `off` (core/engine/webhook.go:104 `cmdWebhook`, v0.1.20 texts); the URL is saved as the private file `workflow-webhook-url` next to the store and turned on in the store (cli/teams_newcore_workflow.go:207 `SetWebhook`), live without a restart; one test card per command (webhook.go:176, recorded before the post); a shortened hyperlink is not read (no hrefs reach the engine): the setup text asks for the plain URL | `workflowWebhookFromControl` B:5529 | C |
| 34 | F-CTL-17 | P2 | B | S | **DONE** (batch B, `cleanup/gap-b-20261106`, scenario SKILLS-PUSH; CMD-SKILLS updated): `helper skills push [--direct] [name]` reviews the local skill edits in the chat and `helper skills push [--direct] confirm` commits and pushes exactly the reviewed edits (core/skillcmd/push.go:236/:266, v0.1.20 texts); the review is kept per chat for 30 minutes in a private file next to the skill config (survives a restart); a stale review is refused; `remove` still points to the local CLI | `helper skills push` / `push confirm` (help B:5216) | — |
| 35 | F-WRK-3 | P2 | C | S | **DONE** (batch C: `!status` and `helper close` exist, so the ready text is accurate, scenarios CMD-WORK-FORMS, CMD-CLOSE; batch A: the imported-chat intro, scenario DASH-FLOW): Ready text advertises non-existent `!status` and `helper close`; imported chats get no intro (no import) | `sessionReadyMessage` B:7841, lifecycle B:7867 | A-import |
| 36 | F-RND-8 | P3 | F | S | **DONE** (batch F, `cleanup/gap-f-20261106`, scenario F-RND-8): Limit is 16 attachments total (`channel/teams/receive.go:228`), not 5 images / 5 files / 3 quotes; overflow note differs | SPEC F-RND-8 | — |
| 37 | F-MIR-8 / R-23 | P3 | F | S | **DONE** (batch F, `cleanup/gap-f-20261106`, scenario F-MIR-8): No `<oai-mem-citation>` stripping found; no canary scenario for hidden records | origin/main `artifact_handoff.go:576` (not in v0.1.20) | — |
| 38 | F-SVC-10 | P3 | E | S | **DONE** (batch E, scenario RESTART-DRAIN): Messages that arrive during a restart/update drain get no "…I saved this message and will resume it…" notice | SPEC F-SVC-10 | — |
| 39 | F-TRN-17 / R-40 | P3 | G | M | **DONE** (batch G, `cleanup/gap-g-20261106`, coordinator ruling: hidden aliases of `teams stop/start`; scenario PAUSE-ALIAS): `cxp teams pause/resume` deleted (DELETED-CLI asserted "unknown"); Q-19 default kept them | SPEC F-TRN-17 | — |
| 40 | F-CTL-2 | P3 | G | M | **DONE** (batch G, `cleanup/gap-g-20261106`, scenario CONTROL-RECREATE): `cxp teams control --recreate --yes` missing (only prints the link) | SPEC F-CTL-2 | — |
| 41 | F-WRK-9 | P3 | G | M | **DONE** (batch G, `cleanup/gap-g-20261106`, scenario WORK-RECREATE): `cxp teams chat recreate <sel> [--history full] [--activate] --yes` missing | `recreate.go:169` "Work chat recreated" | A-import |
| 42 | F-CLI-10 / F-CLI-3 | P3 | G | S | **DONE** (batch G, `cleanup/gap-g-20261106`, scenarios RUN-ONCE, RUN-INTERVAL): `teams run --once` and `--interval` missing | SPEC F-CLI-3/F-CLI-10 | — |
| 43 | F-CLI-1 / R-17 | P3 | G | S | **DONE** (batch G, `cleanup/gap-g-20261106`, scenario STATUS-VERSIONS): `teams status` lacks installed-vs-running (pending) version and last poll | SPEC F-CLI-1 | — |
| 44 | F-XM-1 | P3 | G | S | **CLOSED, exempt** (batch G, `cleanup/gap-g-20261106`, coordinator ruling): the install-host pin stays unwired as decided in TAKEOVER-REPORT §8.1 ("Any host with the service may take over. Pinning a service to one host would make that host's death a full outage."); the takeover rules are covered by the sharedhome scenarios. Was: service is not pinned to its install host (`store.Ident.InstallHost` never set by the CLI) — owner 定稿 共用 home | — | — |
| 45 | F-CTL-1 | P3 | D | S | DONE (batch D, EDIT-RENAME). Control chat title lacks 🏠 and the user-title segment | `ControlChatTitle` (🏠 Codex Control - <machine>…) | D-rename |
| 46 | R-26 | P3 | F | S | **DONE** (batch F, `cleanup/gap-f-20261106`, scenario R-26): No raw-API-key rejection in chat found in the core (v0.1.20 rejects them first in `handleControlMessage` B:4613) | B:4613 | — |
| 47 | F-SET-2 | P3 | G | S | **DONE** (batch G, `cleanup/gap-g-20261106`, scenario AUTH-SCOPES): No fail-closed gate for scopes broader than the defaults | SPEC F-SET-2 | — |

### Contradictions between SPEC and owner decisions (and other inconsistencies)

1. **F-TRN-9** SPEC `keep (breadth: Q-6)` — owner (第一级 4, 定稿) removed all third-party model APIs and `model provider/catalog`. Catalog: removed-by-owner.
2. **F-SET-3, F-WRK-11, F-CLI-4, F-CLI-5** SPEC `ask-owner` — owner removed (第一级 4, 定稿 删除). Also `helper reload` (part of F-CTL-11, SPEC "keep (reload: see Q-17)") removed; `teams path` modes `captured-terminal`/`service` (F-CLI-6) removed.
3. **F-TRN-17 pause/resume**: SPEC keep and the Q-19 default kept `cxp teams pause/resume` (drain internal); the core deleted them and scenario DELETED-CLI asserts they are unknown. Not an owner removal — needs an owner answer (the kill switch `teams stop/start` is a different behaviour: it stops reading, pause defers). **Resolved by batch G** (2026-11-06, coordinator ruling): `teams pause/resume` are hidden aliases of `teams stop/start`; PAUSE-ALIAS asserts it, DELETED-CLI no longer lists them.
4. **F-WRK-8**: SPEC "parked chats read every 10 min" and 决定-5 / 第二级 2 default ("auto-wake within ~30 s") vs 定稿: no Graph poll at all; a local rollout update wakes the chat; messages sent while parked are seen after the wake; freeze text must say so.
5. **F-MIR-3 / R-9**: SPEC "≈1 month" window, Q-15, 第二级 9 "120 天" and 定稿 "120 天" vs 2026-11-05 追加: no window at all.
6. **F-RND-5**: SPEC "final when the turn took ≥60 s" vs owner 第二级 7 / 定稿: @ on every final, delete the 60 s code; 11-05: catch-up is silent (no @).
7. **F-WRK-14**: SPEC auto-quarantine + offline CLI vs owner breaker (pause auto-start only, survives restart, one notice, no offline CLI) and `helper unpause` (第一级 8; 决定-6 originally said `helper resume`).
8. **F-TRN-10**: SPEC "no retry after side effects", "up to 10 consecutive" vs 定稿 (delete the side-effect guard, 5-min window from the original start) and 11-05 Q2 (send only `continue`; resend the original only if Codex never received it).
9. **F-XM-4**: SPEC "no such code; default not build" and 第二级 10 default "不做停用开关文件" vs 定稿 "新增停用开关文件" and 11-05 Q4 (`cxp teams stop/start`, control `shutdown`, `DISABLED`).
10. **F-XM-1**: SPEC priority classes (Q-12) vs 定稿 "删除机器类型和优先级; 服务固定在安装它的主机上". Note: scenario TAKEOVER-BATCH still checks "an interactive standby takes over before a batch-job standby" — a priority-like rule the owner removed; verify with the owner/implementer. **Install-host pin resolved by batch G** (2026-11-06, coordinator ruling): stays unwired per TAKEOVER-REPORT §8.1; F-XM-1 is exempt for it.
11. **F-SVC-3**: SPEC/第一级 4 default "remove the Startup-folder fallback" vs 定稿: keep a one-shot idempotent `wsl.exe … cxp teams service start`, no watchdog loop.
12. **F-TRN-5**: SPEC "reported once after that session's queue is idle" vs 定稿: at restart recovery, immediately, one per turn, before the next turn.
13. **F-RND-11**: SPEC Q-2 default "drop torch" etc. — owner: keep external command + llama.cpp (old-glibc build, VC++ DLLs), no Linux GPU, 4 failure-class E2Es, new P0 guarantee R-50 (not in SPEC's R list; SPEC says 49 guarantees).
14. **Q-1 / F-XM-2, F-XM-3, F-CLI-8**: SPEC ask-owner (default remove delegation) vs 决定-3 keep.
15. **F-TRN-18 vs core `StallLimit` 1 h**: SPEC keep "never kills a long turn"; the core stops silent turns after 1 h (SILENCE asserts it). No owner decision found that allows it. **Resolved by batch E** (2026-11-06, coordinator ruling: match v0.1.20): `StallLimit` removed; SILENCE asserts the turn is never stopped.
16. **F-TRN-2 ACK text**: SPEC "⚠️ Your request is queued." listing all requests vs owner 11-05 "⏳ Queued" listing only the running request and the count ahead (owner-ux `187b8ca4`). 11-05 also says single-message format follows v0.1.20 — the owner explicitly overrode it for this text.
17. **Code vs itself**: the work-chat ready text (`core/engine/control.go:334`) advertises `!status` and `helper close`, neither of which exists. **Resolved by batch C** (2026-11-06): both exist now (parse.go, cmdtable.go `cmdCloseChat`); the text is unchanged.
18. **Evidence vs code**: COVERAGE-REPORT §3 marks R-8 "unit", but the cited tests cover the outbox marker; the user marker is not implemented.
19. **Scenario tags**: CMD-FORK claims F-CTL-4 (`new`) — it exercises `new` then `helper fork`, so F-WRK-10 is right and F-CTL-13 (control `fork`) is not exercised. **Resolved by the fork-history change** (2026-11-06): FORK-CTL exercises control `fork s001`.

## 2. Catalog

Columns: id · status (`keep` / `removed-by-owner` / `ask-owner`, with the owner citation when answered) · implementation (`file:line`, or **MISSING** / **PARTIAL** + what is missing; `n/a` for removed) · e2e (scenario ids; `NONE`; or `exempt: <reason>` only where the offline Linux black-box suite cannot observe it) · v0.1.20 parity.

<!-- catalog:begin -->
| id | status | implementation | e2e | v0.1.20 parity |
|---|---|---|---|---|
| F-SET-1 | keep | cli/teams.go:114 `auth login` (alias `full`), :260 status, :278 logout | exempt: interactive device-code sign-in against real Entra ID; the fake Graph has no authority endpoint | `auth full` kept as alias |
| F-SET-2 | keep | channel/teams/auth.go:198 `ValidateScopes`: a configured scope outside the allowlist is refused with "unexpected Teams Graph scope(s): …; set CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1 to override" by `teams auth config`, `auth`, `status` and `run` (fail closed); the service passes the opt-in through (cli/teams_service.go); cli/teams.go:161 `auth config` + `CODEX_HELPER_TEAMS_*` env | AUTH-SCOPES | v0.1.20 refused broader scopes without ALLOW_UNSAFE_SCOPES |
| F-SET-3 | removed-by-owner (DECISIONS 第一级 4 【删除】, 定稿 删除 分离 token) | n/a | DELETED-CLI (`auth read/file-write` unknown) | — |
| F-SET-4 | keep | cli/teams.go:76 `teams setup` | CLI-READONLY | same |
| F-SET-5 | keep | cli/teams_service.go:134 bootstrap, BOOTSTRAP COMPLETE :836 | exempt: bootstrap installs and starts the per-user service (systemd --user, launchd or Task Scheduler), which the offline container lacks; and the service child is started from a persisted launch spec whose environment allowlist (cli/teams_service.go teamsServiceEnvironmentAllowlist) drops the test-only Graph seam, so in the offline suite it cannot reach the fake Graph | same flags |
| F-SET-6 | keep | cli/teams_graph_proxy.go:115 (proxy warning) | exempt: needs a reusable local SSH proxy profile and daemon in front of Graph; the offline suite has no proxy and the fake Graph is reached through the test seam, which bypasses proxy mode | same |
| F-CTL-1 | keep | core/engine/control.go:18 `ensureControl` creates the chat with core/engine/rename.go:105 `controlTitle` = "🏠 Codex Control - <machine>[ - user title]" (:110 `controlTitleFor`; user title from meta, kept over restarts and recreation); no profile segment (the core has no Teams profile) | R-1-2-3, EDIT-RENAME | v0.1.20 "🏠 Codex Control - <machine>[ - profile][ - user title]" |
| F-CTL-2 | keep | cli/teams.go:314 `teams control --recreate --yes` → cli/teams_chat.go:26 → request file (cli/teams_newcore_clireq.go:78 `submitTeamsCLIRequest`; the helper runs it on its 5 s watcher tick, :160) → core/engine/clireq.go:109 `CLIRequest` / :297 `cliRecreate`: new chat, @mentioned "🔁 This chat moved" link in the old chat (:266 `textChatMoved`) confirmed before :528 `cliRebind` moves the control conversation, then "Control chat recreated." (@mention) + ready/help in the new chat; old chat kept, no longer read; the command prints the new and previous chat | CONTROL-RECREATE | v0.1.20 `control --recreate`; "Restart or reload…" line replaced by "The running Teams helper now uses the new control chat." (no restart needed) |
| F-CTL-3 | keep | core/engine/dashboard.go:466 `cmdWorkspaces`, :525 `cmdWorkspaceSessions`, :668 `cmdSelect` (bare number), :636 `dashResolve`; the list is store state with a 10-min expiry (:43 `dashViewTTL`, meta `engine_dashboard_view`), used up by any other command (:156 `dashConsume`); no timer | DASH-FLOW | B:19702, B:19840, B:20304, dashboard.go:17 |
| F-CTL-4 | keep | commands.go:202 `cmdNew` via dashboard.go:1100 `parseNew`: relative/`~`/`$VAR` paths (:1017 `resolveUserPath`), a missing directory is created 0700, bare `new` uses the opened workspace, `--model`/`--mp` (validated) and `-- <title>`; title "s001 · name" until the first answer (batch D retitles) | R-1-2-3, CMD-FORK, NEW-MKDIR | v0.1.20 creates the dir, title "💬 <machine> - New message in <ws>" |
| F-CTL-5 | keep | core/engine/import.go:596 `cmdContinue`, :629 `publishSession` (bare number too), :742 `publishNew` (new chat, "✅ Work chat created…Preparing…", "Published local Codex session as…"), :652 `publishLinked` ("Already published…", reopen of a parked chat) | DASH-FLOW, IMPORT-AGAIN | B:15710 / B:15741 |
| F-CTL-6 | keep | core/engine/dashboard.go:691 `cmdOpen` (wakes a parked chat), :799 `cmdDetails` (list number or session), :847 `sessionDetails` (also work `helper details`), :904 `activeChatsText` (control `st/status`: "## Active Work chats" with links and `r <id>` hints) | DASH-FLOW | B:20036, B:15573, B:15242 |
| F-CTL-7 | keep | core/engine/dashboard.go:1079 `cmdMkdir` (0700), :1072 `pathHintText` via cmdtable.go:207 (path-like text) and :243 (path-like unknown command) | NEW-MKDIR | B:7830, B:7343/B:7358 |
| F-CTL-8 | keep | core/engine/commands.go:254 → core/engine/rename.go:301 `cmdRename`: :338 `renameControl` (`rename <title>`, stored in meta so a recreated control chat keeps it), :375 `cmdRenameHostname` (control + every open work chat, :511 `stripMachinePrefix` against all old labels so prefixes never nest, user titles kept, partial failures listed as in v0.1.20); every rename goes through :264 `renameChat` → `waitWrite` (account write pacer) | EDIT-RENAME | B:10683, B:10507 |
| F-CTL-9 | keep | core/engine/cmdtable.go:308 `controlHelpText`, :334 `controlAdvancedHelpText`, :387 `sessionHelpText`, :407 `sessionAdvancedHelpText` (v0.1.20 verbatim minus restore-thread, `helper reload`, beacon and `model setup` / `model doctor`; plus `shutdown`, `cxp teams stop/start`, control `fork`, `helper unpause`, `helper fork`); `help advanced` / `all` / `more` / `full` (parse.go `isAdvancedHelpArg`); grammar core/engine/parse.go:264 `parseWork`, :299 `parseControl` (prefixes `/` `!` `cx` `codex` `helper[:]`, short names, bare work words), dispatch cmdtable.go:20 `workCommands` / :93 `controlCommands` | R-1-2-3, CMD-HELP | B:5123, B:5150, B:5216, B:5241 |
| F-CTL-10 | keep | core/engine/control_qa.go:33 `controlQuestion` (`ask/q <text>`, text that is not a command or a path, a voice clip; receive.go:808, only for messages newer than the chat on an unseeded stream: catch-up stays silent) → an ordinary turn of the control conversation: ACK control_qa.go:78 `ackControl` (v0.1.20 text naming `helper cancel turn-<n>`), prompt :171 `controlPrompt` (v0.1.20 envelope adapted: no beacon/model-profile lines), history :306 `controlHistory` (store-backed, 500 entries, tail 20 / 8000 chars, written to `control-chat-history.jsonl` beside the store), :434 `controlRequest` (low effort, home dir); answer @mentions the owner, no "Codex finished" notice, never mirrored; `helper cancel …` :509 | CTL-QA, CTL-QA-RESTART, CTL-QA-VOICE | B:4901, B:7488, artifact_handoff.go:74 |
| F-CTL-11 | keep (owner: `helper reload` removed, 第一级 4 / 定稿) | core/engine/restart.go:90 `cmdRestart`: bare `restart` = v0.1.20 confirmation; `now` waits for running work (at most Timing.UpdateDrain, durable request `engine_restart_request`, resumed at startup :126), holds new turn starts and restarts (:147 `runRestart`); `force` restarts at once; one "✅ Helper restart completed" from the next process (`metaRestart`); upgrade.go:491 `cmdHelperUpdate` | RESTART, RESTART-DRAIN, CODEX-UPGRADE | v0.1.20: `restart` = confirmation prompt, `force` interrupts; v0.1.20 refused `now` while busy, the core waits (SPEC F-SVC-10) |
| F-CTL-12 | keep | commands.go:620 `cmdParkLabel`, :648 `cmdWake` (`r/wake/resume/unpark`) | PARK, PARK-CTL, WAKE-SYNTAX | owner-ux `9b043d12` park text says parked messages run after the wake |
| F-CTL-13 | keep | control `fork <session>` → core/engine/cmdtable.go:120 → core/engine/fork.go:87 `cmdFork` (see F-WRK-10): "⏳ Fork requested…" in the control chat; the fork chat gets the history then the marker; the link ("✅ Fork complete") goes to the parent work chat as in rc.6, the control chat's "✅ Work chat created: sNNN" follows the marker (fork.go:416 `forkSettled`) | FORK-CTL, CMD-FORK, FORK-RUNNING (work-chat `helper fork`) | same verb; rc.6 sends the link to the parent chat |
| F-CTL-14 | keep | core/engine/webhook.go:104 `cmdWebhook` (v0.1.20 grammar and texts: help, `setup` naming the control chat and its id, `status`, `off`, `test`, `<url>` / `set/url/enable <url>` checked by :95 `safeWebhookURL`), :176 `webhookTest` (at most one card per command across restarts, meta engine_webhook_test); the setting is the Notifier's :22 `WebhookSettings`, implemented by cli/teams_newcore_workflow.go:201/:207 (private 0600 file next to the store, store meta read at every card); CLI `teams workflow` unchanged | WEBHOOK-CTL | B:5529 |
| F-CTL-15 | keep | core/engine/defaults.go:89 `cmdDefaultControl` (v0.1.20 `parseDefaultCommand` grammar: `default [status]`, `default help`, `default model/effort status/list/set/reset` with aliases), :262 `defaultModelSet` (effort reconciled as in v0.1.20 `setGlobalDefaultModel`), stored in the cxp config `defaults.model`/`defaults.reasoningEffort` as before the rebuild (cli/teams_newcore_defaults.go; store meta when the engine has no config); :40 `newChatThread` gives new chats (commands.go:222 `new`, modelcmds.go:616 `model fork`, control_qa.go:478 the control chat) the defaults, existing chats unchanged; work chat answers v0.1.20's "can only be used in the Control chat" (cmdtable.go:67) | CMD-DEFAULT, CMD-DEFAULT-CONFIG | default_commands.go:35, cli/teams_defaults.go:45 |
| F-CTL-16 | keep | core/engine/envelope.go:563 `controlAttachmentRefusal` (called at :514 in `screen` :500, after the R-26 key check): v0.1.20 attachment / control / explicit-command-quote texts, types from channel `InboundMsg.Attachments` (channel/teams/receive.go:277 `attachmentTypes`, v0.1.20 order); quotes kept for `ask` and plain text; "⚠️ Wrong chat" for work-only `helper …` (parse.go:480 `workOnlyName`, cmdtable.go:222 `unknownControlText`); unknown `!x` / `cx x` / removed names: "unknown control command: `x`" + help | CTL-ATTACH, CMD-CONTROL-FORMS | text.go:113/:121, B:7330, B:5271 |
| F-CTL-17 | keep (owner: DECISIONS 第一级 4 "这个保留，是常用功能"; 定稿 skills 保留) | commands_ext.go:117 `cmdSkills` → core/skillcmd/skillcmd.go:47 `ChatCommand` (engine `ChatSkills`, the chat id keys the push review): list/add/sync; push.go:236 `push` review (v0.1.20 `reviewTeamsSkillPush`, 30-minute TTL, per chat, file `teams-skill-push-reviews.json`), :266 `confirmPush` (re-checks the edits with :327 `reviewStillMatches`, pushes to the reviewed review-branch refspec or `--direct`, removes each pushed source from the review); `remove` refused with a pointer to the local CLI | CMD-SKILLS, SKILLS-PUSH | v0.1.20 `skills push` / `push confirm` in Teams |
| F-CTL-18 | removed-by-owner (beacon; SPEC + 定稿) | n/a | n/a | — |
| F-WRK-1 | keep | core/engine (one conv = sNNN, one thread, cwd); thread bound before `turn/start` (runner.go) | R-1-2-3, B-5 | labels sNNN kept |
| F-WRK-2 | keep | core/engine/rename.go:746 `afterAnswers` (once the live answer's owner notification has gone out, :719 `notified`) → :530 `queueAutoTitle` → :595 `autoTitle`: Codex thread name (`session_index.jsonl`, :665) > first owner prompt > rollout first prompt (:172 `promptTitle`), gate :577 `autoAllowed` (= v0.1.20 `sessionAllowsAutoTitleUpdate`); the chat's next notification waits for it (:557 `retitling`, core/engine/notify.go:171) so it names the chat as the owner sees it; `helper rename <title>` core/engine/commands.go:305 → rename.go:301 (title source `user`, stops auto titles); catch-up never retitles | EDIT-TITLE, EDIT-RENAME | B:10749, B:10804, B:10349 |
| F-WRK-3 | keep | control.go:322 `textWorkChatReady` (new/fork), :314 `textLocalDetected`; the commands it advertises (`!status`, `helper close`) exist (contradiction 17 resolved by batch C); an imported chat gets the lifecycle notice, import.go:484 `textImportTitle` and "Import complete…" (:509 `importFinish`) | R-1-2-3, CMD-WORK-FORMS, CMD-CLOSE, DASH-FLOW | B:7841, B:7867 |
| F-WRK-4 | keep | core/engine/cmdtable.go:263 `cmdCloseChat` (`helper close`, `!close`, `!cl`): v0.1.20 text; the conversation is closed in the reply transaction (store `Close`): no reads, no turns, out of `list`; queued requests cancelled silently, a running one finishes | CMD-CLOSE | B:8007 |
| F-WRK-5 | keep | core/engine/maint.go:191 `parkIdle` after 48 h records the owed notice in the park transaction (:226 → core/engine/annotations.go:106 `owesFreeze`, meta `engine_freeze:<conv>`) and queues it (:149 `queueFreeze` → :159 `freeze`): PATCHed onto the chat's newest helper post or admitted owner message, else posted; once per park, resumed after a restart (:112 `resumeFreezes`); text :130 `freezeText` says messages are read once the chat wakes | EDIT-FREEZE | freeze notice PATCHed onto the latest message |
| F-WRK-6 | keep | commands.go:648 `cmdWake`; local rollout wake engine.go:44 `MirrorParkedStat`, mirror.go:790 (open/select/continue wake a parked chat too: dashboard.go `wakeStep`, not covered by a scenario) | PARK, PARK-CTL | same `r <id>` |
| F-WRK-7 | keep | commands.go:550 `cmdPark` | PARK, PARK-CTL | same |
| F-WRK-8 | keep (owner: 定稿 parked chat — no Graph poll; local rollout update wakes it; messages sent while parked are seen after the wake) | engine.go:44 `MirrorParkedStat`, mirror.go:790 | PARK | v0.1.20 polled parked chats (superseded) |
| F-WRK-9 | keep | cli/teams_chat.go:40 `teams chat recreate <session, thread or chat id> [--history full] [--activate] --yes` → core/engine/clireq.go:297 `cliRecreate` (selector :209 `cliSession`: label, Codex thread, chat id, 12-char chat prefix): new chat, moved link in the old chat, :528 `cliRebind` (one transaction: chat, stream, wake/reactivate); `--history full` imports the whole transcript in a fresh namespace before "Work chat recreated: sNNN." (@mention) + ready anchor; the session keeps its Codex thread; `--activate` reactivates a held session | WORK-RECREATE | recreate.go:169 |
| F-WRK-10 | keep | core/engine/fork.go:87 `cmdFork`: accepted while a request runs, cut at the last completed Codex turn (:183 `lastCompletedCodexTurn`); :216 `forkPrepare` copies the parent's rollout up to the cutoff (:284 `forkCutoff`: running turn excluded) to FilesDir/fork-history/<child>.jsonl and records where the child's own rollout starts; the fork's transaction creates the chat, binds the child, queues its notice and an import job (import.go:58 `importFork`, key namespace `<parent thread>@c<child>`); import.go:565 → fork.go:397 `forkHistoryDone` queues the marker "History import complete. The new Codex chat is ready."; fork.go:416 `forkSettled` (sender.go/receive.go, in the transaction that settles the marker) queues one "✅ Fork complete. Open the new Codex chat:" + link (key `fork-done:<chat>`) in the parent; :448 `forkSweep` removes retired snapshots | CMD-FORK, FORK-RUNNING, FORK-CTL, FORK-CRASH | SPEC F-WRK-10 steps 1-5; rc.6 `1a210e6` (link only after the visible history is confirmed) |
| F-WRK-11 | removed-by-owner (DECISIONS 第一级 4 【删除】; 定稿) | n/a | DELETED-CMDS | — |
| F-WRK-12 | keep | core/engine/receive.go:813/:824 group gating on v0.1.20's @codex pattern (parse.go:494 `stripCodexMention`); bare @codex with no text and no attachment from anyone: v0.1.20 hint (receive.go:748/:816, :857 `bareMention`); the owner's commands count without the mention | R-43, GROUP-CMDS | B:2031 |
| F-WRK-13 | keep | core/engine/receive.go:753 a coworker's command (any form, after the @codex is stripped) is refused with v0.1.20's owner-only text, quoting the message (v0.1.20 @-mentioned the coworker; the core can only mention the owner); coworker ACK "⏳ Codex received your question. Request accepted." (:967) | GROUP-CMDS | B:8218, B:8328 |
| F-WRK-14 | keep (owner: 第二级 3 breaker, 第一级 8 `helper unpause`, 定稿) | receive.go:1113 `tripBreaker` (notice :1122, control :1126); receive.go:1096 `breakerHit` counts hits at their message times (:1087 `hitTime`, as v0.1.20); receive.go:467/546 the first read of a process (backlog) never counts (finding 8); receive.go:746 a converted old-helper marker whose message id is in the outbox is own; commands.go:280 `cmdUnpause` | BREAKER, BREAKER-BACKLOG | SPEC quarantine replaced by the breaker |
| F-WRK-15 | removed-by-owner (beacon) | n/a | n/a | — |
| F-TRN-1 | keep | core/engine/sender.go:441 → rename.go:697 `noteFinalSent` records the answer post's createdDateTime; :746 `afterAnswers` queues `MarkUnread(chat, created − 1 ms)` once per answer (:769) after its owner notification; catch-up answers have no notification, so they are not marked | R-1-2-3, EDIT-UNREAD | v0.1.20 marked unread |
| F-TRN-2 | keep (owner: 11-05 第二批 排队确认措辞 "⏳ Queued") | silence.go:189 `textQueuedAck` (owner-ux `187b8ca4`), runner.go:260 start notice, silence.go:108 `remindQueued` | R-6 | text deliberately differs (owner) |
| F-TRN-3 | keep | work chat: core/engine/turncmds.go:104 `cmdCancel` `last` (= bare, owner ruling) / `queued` / `running` / `all` / `<turn-id>` (`turn-N`, `#N`, `N`) and `!cancel`; :190 `cancelOne` names the prompt and what stays queued, :228 `cancelAll`; v0.1.20 usage and refusal texts; the queued turn is closed in the reply transaction. Control-chat cancel: cmdtable.go:142 `controlCancelText` hook for batch B (Q&A) | R-20-33, CANCEL-CMDS, SILENCE | B:9490 |
| F-TRN-4 | keep | core/engine/turncmds.go:294 `cmdRetry`: `last` (newest failed / interrupted / not-started / cancelled turn whose message has no later run), `<turn-id>`, `!retry`; bare = the ignored duplicate (:408) else usage; refusals for answered, already-retried and unknown turns; the new turn is created in the reply transaction (`RetryTurn`), so a retry never runs twice nor duplicates a final; failure / interruption / setup notices name `helper retry last` | RETRY-CMDS, SETUP-ERR, DUP, CODEX-UPGRADE | B:8236; uses the session's current model/effort, not the failed turn's |
| F-TRN-5 | keep (owner: 第二级 8 + 定稿 中断通知) | runner.go:912 `reconcile`; render.go:99 textInterrupted (v0.1.20 text, points to `helper retry last`) | INT-NOTICE, R-4-29, B-5, RETRY-CMDS | v0.1.20 text |
| F-TRN-6 | keep (owner: 第二级 6 【可以】) | receive.go:854 (engine.go:93 DuplicateFor 3 min) | DUP, DUP-EDGE | same |
| F-TRN-7 | keep | core/engine/modelcmds.go:300 `cmdModelChat` (work + control; parse.go:196 `workBare` for bare `model`/`models`): :200 status, :239 list (`verifiedModels` :214, v0.1.20 format), :399 `modelSwitch` (`switch/use`, `reset`; effort reconciled as v0.1.20 `reasoningEffortForModelSwitch` :376; applies to turns that start after it, the thread is kept), :616 `modelFork` + :688 `announceModelFork` (work-chat `fork <m>`: new Work chat, same cwd, model pinned, link posted in the source chat); runner.go reads the thread model at turn start; every turn sends the model and effort the chat shows (`TurnSettings`, codexagent `Run`) | CMD-MODEL, CMD-MODEL-CTL, CMD-WORK-FORMS, DELETED-CMDS, MODEL-SHOWN | model_profile_commands.go:15/:125 |
| F-TRN-8 | keep | core/engine/modelcmds.go:704 `cmdEffortChat` / :469 `effortCommand` (work + control): :529 status, :551 list, :572 `effortSet` (`set/switch/use <e>`, bare value; checked against the chat model), `reset` to the model default; bare `effort`/`efforts` (parse.go:196) | CMD-EFFORT, CMD-WORK-FORMS | reasoning_effort_commands.go:20 |
| F-TRN-9 | removed-by-owner (DECISIONS 第一级 4 "把第三方 api 的功能移除掉"; 定稿 第三方模型 API 全部移除; SPEC says keep) | n/a (`model provider/catalog` refused) | DELETED-CMDS | — |
| F-TRN-10 | keep (owner: 定稿 满载重试 + 11-05 第二批 Q2 `continue`) | runner.go:704 `capacityRetry`; `continuesPrompt` (runner.go) continue-once | CAP-RETRY, CAP-WINDOW, CAP-CONTINUE | owner redefined |
| F-TRN-11 | keep (owner: 定稿 原样保留) | upgrade.go:127 `codexTooOld`, :227 `updateStep` | CODEX-UPGRADE | same |
| F-TRN-12 | keep | core/codex/approval.go:10 (auto-approve), app-server runtime; config.toml/MCP changes apply from the next turn without a helper restart: core/codex/pool.go:187 `AcquireCurrent` (idle server started before the change is replaced at turn start; running turns untouched), codexagent/agent.go:92 | R-1-2-3 (app-server path; approvals not exercised), CODEX-CONFIG | same |
| F-TRN-13 | keep | cli/teams_user_path.go:42, cli/teams_child_guard.go:16/:42 `CODEX_HELPER_CLI_PATH` | CHILD-GUARD | same |
| F-TRN-14 | keep | core/engine/envelope.go:80 `teamsCodexPrompt` (v0.1.20 `TeamsCodexPrompt` byte for byte: User message, Teams safety, math contract, outbound root + manifest instructions; owner changes: no `helper reload now` line, native-subagent sentence of ae23659), :120 `codexInput` (ASR lead, attachment list, quotes, local files), runner.go:223; unit differential golden engine/envelope_test.go `TestCodexInputMatchesV0120` against v0.1.20 bytes (testdata/v0120_envelope.json) | F-TRN-14 | artifact_handoff.go:28/:184 |
| F-TRN-15 | keep | runner.go progress streaming; stream reconnect status (v0.1.20 text, once per 5 min per turn): core/codex/turn.go:146 + retry.go `parseRetry`, engine/runner.go:379 `Retrying`, render.go `textReconnecting` | R-20-33, B-5, RETRY-STATUS, TURN-UNPHASED, TURN-PROGRESS-INTEGRITY | same |
| F-TRN-16 | keep | core/engine/setuperr.go:19 `setupClass` (Node.js missing, config route, launch failure), :42 `textNotStartedTurn` / :69 `textFailedTurn` (v0.1.20 missingNodeRuntime / codexConfiguration / infraLaunchFailure notices + "Diagnostic for the admin"), used by runner.go:541/:567; core/codex/server.go:110 keeps the app-server's start-up stderr tail so the class is visible; never retried automatically | SETUP-ERR | SPEC text "⚠️ I couldn't start Codex due to a setup issue on my side…"; no auth-specific class (v0.1.20 had none) |
| F-TRN-17 | keep (Q-19 default: keep `pause/resume`, drain internal; coordinator ruling batch G: hidden aliases of `teams stop/start`) | cli/teams_stop.go:105 `teams pause [reason]` (hidden alias of `teams stop`), :120 `teams resume` (hidden alias of `teams start`); the reason is accepted, not kept; `drain` stays removed | PAUSE-ALIAS | v0.1.20 `teams pause/resume/drain` (pause = kill switch now: it stops reading instead of deferring) |
| F-TRN-18 | keep | core/engine/silence.go:16 `watchSilence`: hints at SilenceHint / SilenceStuck (:137 v0.1.20 "…It may be stuck. I will not retry automatically…" text), never stops a silent turn (StallLimit removed; coordinator ruling: match v0.1.20) | SILENCE | v0.1.20 never killed (B:92-99) |
| F-TRN-19 | removed-by-owner (beacon) | n/a | n/a | — |
| F-MIR-1 | keep | mirror.go:288 detection | R-9-10 | "Local Codex chat detected" per 11-05 |
| F-MIR-2 | keep | mirror.go | R-9-10 | same |
| F-MIR-3 | keep (owner: 2026-11-05 追加 — no window) | no window (all sessions caught up) | NO-WINDOW | owner redefined |
| F-MIR-4 | keep (owner: 11-05 catch-up silent) | mirror.go | CATCHUP-LIVE, ENTRY-RESUMECHECK, R-9-10, SHADOW-REALDATA, CATCHUP-CRASH, MIR-PROGRESS-CATCHUP | same |
| F-MIR-5 | keep | core/engine/import.go:168 `queueImport` (persisted job), :235 `importApply` (mirror goroutine moves the shared cursor: mirror.go:748), :449 `importProgress` (mirror.go:1040), :509 `importFinish`; records go through the mirror outbox with its delivery keys (silent catch-up format), so a crash, restart or a second import never posts a record twice | DASH-FLOW, IMPORT-AGAIN, IMPORT-CRASH | B:15741, B:16174 |
| F-MIR-6 | keep | chat `helper publish-history [full]` / `!ph` (core/engine/import.go:846 `cmdPublishHistory`; `full` re-reads the rollout in the same delivery namespace, so it posts only what is missing); CLI cli/teams_chat.go:71 `teams chat publish-history <sel> --full [--force] --yes` → core/engine/clireq.go:672 `cliPublishHistory` (same import; "already published" when nothing is missing; `--force` replays in a fresh namespace) | IMPORT-AGAIN, PUBLISH-HISTORY-CLI | B:8051, B:16039, B:16083 |
| F-MIR-7 | keep | mirror.go:831 rewritten-rollout realign | MIR-REWRITE | same |
| F-MIR-8 | keep | transcript/record.go:358/:380 hide tool/reasoning/token events; `<oai-mem-citation>` blocks stripped by transcript/strip.go:15 `StripMemCitations` (port of v0.1.20 `StripOAIMemoryCitationBlocks`) in answers (engine/outbound.go:177), progress (runner.go:1010) and mirrored TUI turns (transcript/tracker.go:269/:338); a block inside a closed code fence stays. Note: v0.1.20 already had it (artifact_handoff.go:501), not only origin/main | F-MIR-8 | artifact_handoff.go:501 |
| F-MIR-9 | keep | transcript/record.go:320 bounded classification of large records | MIR-BAD-RECORDS | same |
| F-MIR-10 | keep | receive.go:443 `admit` independent of mirror state | CATCHUP-LIVE | same |
| F-RND-1 | keep | channel/teams/render.go:37 (ported v0.1.20 markdown, 11-05) | R-36 | same |
| F-RND-2 | keep | channel/teams/render.go labels | R-1-2-3 | v0.1.20 labels restored 11-05 |
| F-RND-3 | keep | channel/teams/render.go:57-76 | R-36 | same |
| F-RND-4 | keep | core/engine/receive.go:570 → core/engine/annotations.go:35 `labelAdmitted` (live admitted owner commands and prompts only; catch-up stays silent) → edit queue core/engine/edits.go:51 `queueEdit` / :103 `runEdit` (account write pacer, read pacer for the GET; :174 `flushEdits` makes the pending ones at a graceful stop or restart, engine.go:516) → channel/teams/annotate.go:50 `Annotate` (:208 `labelEdit`: GET, refuse when the PATCH could lose hosted images/cards or the author is not the account, idempotent) | EDIT-LABEL | B:4081/:4109 |
| F-RND-5 | keep (owner: 第二级 7 / 定稿 every final; 11-05 silent catch-up) | core/engine/notify.go:126, runner.go:756 `finalMention` | NOTIFY-BURST, NOTIFY-WH, NOTIFY-NOWH, NOTIFY-WH500, NOTIFY-WHDOWN, WORKFLOW-LIVE | owner redefined |
| F-RND-6 | keep | channel/teams/workflow.go:28 | NOTIFY-WH, NOTIFY-NOWH, NOTIFY-WH500, NOTIFY-WHDOWN, WORKFLOW-LIVE | same |
| F-RND-7 | keep (owner: 第一级 7 / 定稿 faithful + code-block fallback) | channel/teams/math.go, mathfont.go | FORMULA | same |
| F-RND-8 | keep | engine/envelope.go:59 limits 5 media (images + voice/video) / 5 files / 3 quotes, :500 `screen` refuses the whole message with v0.1.20's text (no turn, no ACK, quoted in group chats, as v0.1.20); the 16-attachment cut in channel/teams/receive.go is gone; limits apply to prompts only | F-RND-8, R-37 | SPEC limits; B `validateTeamsAttachmentLimits` |
| F-RND-9 | keep | channel/teams/receive.go:316/:360 (forward flag, send time); quotes stored as inbound_files rows (engine/envelope.go:367 `admittedFiles`), resolved by :413 `fetchQuote` (reply → GET full message, forward → its own text, failure → preview; transient errors retry like files) from files.go:161; prompt section :204 `withQuotes` (≤2000 runes inline, longer → `referenced-message-NNN.txt` listed as a local file) | F-RND-9 | quote context in the prompt |
| F-RND-10 | keep | manifest upload core/engine/outbound.go:201; `helper file <rel-path>` / `!file` core/engine/sendfile.go:37 `cmdSendFile` (outbound root `core-files/outbound`, relative paths only, symlinks resolved inside the root, staged + resumable upload like a manifest file); Codex is told the outbound root and manifest format (engine/envelope.go:80, F-TRN-14); CLI cli/teams_chat.go:101 `teams send-file <path> --session <s> or --chat-id <c> --yes [--allow-local-path] [--message]` (v0.1.20 path rules, :172 `resolveTeamsSendFilePath`; file staged with the request) → core/engine/clireq.go:750 `cliSendFile` | R-37-out, SEND-FILE, SEND-FILE-CLI | B:9748 |
| F-RND-11 | keep (owner: 决定-4, 第一级 6, 定稿 ASR) | core/engine/files.go:177-230 `fetchOne` transcribes → prompt; transcript PATCHed under the voice clip by core/engine/files.go:156 → annotations.go:66 `labelVoice` → channel/teams/annotate.go:229 `transcriptEdit` (failed clips are not edited); a control-chat clip is a control question (core/engine/control_qa.go:33) | VOICE, EDIT-VOICE, CTL-QA-VOICE | asr_input.go |
| F-STA-1 | keep | core/engine/stats.go:39 `cmdStats` (work chat :151, control :349) from the rollouts, in the scoped tables of 285784d3^ (`renderStats` :694; owner request 2026-11-07); core/transcript/usage.go:752 (v0.1.20 reconstruction), usage_cache.go:94 (incremental reader, no timer) | CMD-STATS, STATS-RESETS | token_stats.go:1013/:1083 |
| F-XM-1 | keep (owner: 定稿 共用 home — standby + takeover, no priority classes, pinned to install host; install-host pin: coordinator ruling batch G, TAKEOVER-REPORT §8.1) | core/lease/acquire.go:185; the install-host pin is deliberately not wired (TAKEOVER-REPORT §8.1: any host with the service may take over, pinning would make that host's death a full outage) — exempt from implementation and E2E; a home copied while its helper runs (move guard, core/engine/takeover.go): the old copy stands down at the new copy's takeover notice (`standDown`, stop record `superseded`), the new copy closes what the old one took after the copy and lists it (`closeCopied`, receive.go `admit`) | KILL-SWITCH, SERVICE-STANDBY, STANDBY-LOCKED, STANDBY-TAKEOVER, TAKEOVER-BATCH, TAKEOVER-CRASH-MID, TAKEOVER-KILLED, TAKEOVER-LEFTOVER, TAKEOVER-SAME-HOST, TAKEOVER-SIMULTANEOUS, TAKEOVER-THAW-EARLY, STOP-SHARED, MOVED-HOME, MOVED-LIVE-STOPPED, MOVED-LIVE-BOTH | rc.59 lease lessons |
| F-XM-2 | keep (owner: 决定-3) | core/engine/registry.go:37, core/delegate/registry.go:67 | DELEG-RT, INBOX | same |
| F-XM-3 | keep (owner: 决定-3) | core/engine/delegation.go:90 | DELEG-RT, DELEG-RESTART | same |
| F-XM-4 | keep (owner: 定稿 停用开关文件 + 11-05 第二批 Q4) | core/store/store.go:40 `ErrDisabled`; `teams stop/start` + control `shutdown` / `!shutdown` / `/shutdown` / `helper shutdown` (cli/teams_stop.go, engine `cmdShutdown`; parse.go; never labelled, annotations.go:51) | KILL-SWITCH, SERVICE-STANDBY, STOP-START, SHUTDOWN, SHUTDOWN-FORMS, STOP-RESTART, STOP-SHARED | new |
| F-SVC-1 | keep | cli/teams_service.go:105 (systemd/launchd/Task Scheduler backends) | exempt: needs systemd --user, launchd or Windows Task Scheduler; the offline container has none | same |
| F-SVC-2 | keep | cli/teams_service_local_supervisor.go:111 | exempt: the service child is started from a persisted launch spec whose environment allowlist (cli/teams_service.go teamsServiceEnvironmentAllowlist) drops the test-only Graph seam, so in the offline suite it cannot reach the fake Graph; the restart behaviour itself is unit-tested (teams_service_local_supervisor*_test.go) | same |
| F-SVC-3 | keep (owner: 定稿 WSL one-shot Startup entry) | cli/teams_service.go:451 | exempt: needs a Windows host with WSL | owner redefined |
| F-SVC-4 | keep | cli/teams_service.go:854 | exempt: Windows UAC | same |
| F-SVC-5 | keep | cli/teams_service.go:1289 auto-service | exempt: `teams run` repairs and starts the background service through a service backend the offline container lacks; and the service child is started from a persisted launch spec whose environment allowlist (cli/teams_service.go teamsServiceEnvironmentAllowlist) drops the test-only Graph seam, so in the offline suite it cannot reach the fake Graph | same |
| F-SVC-6 | keep | cli/teams_service_watchdog.go:118 | exempt: the watchdog acts on an installed service backend (systemd/launchd/Task Scheduler/local supervisor), and the service child is started from a persisted launch spec whose environment allowlist (cli/teams_service.go teamsServiceEnvironmentAllowlist) drops the test-only Graph seam, so in the offline suite it cannot reach the fake Graph; unit-tested in teams_service_watchdog_test.go | same |
| F-SVC-7 | keep | cli/teams_service.go:105-437 | exempt: the service subcommands drive systemd --user, launchd or Task Scheduler, none of which the offline container has; and the service child is started from a persisted launch spec whose environment allowlist (cli/teams_service.go teamsServiceEnvironmentAllowlist) drops the test-only Graph seam, so in the offline suite it cannot reach the fake Graph | same |
| F-SVC-8 | keep | core/engine/upgrade.go:491 `cmdHelperUpdate`, cli/teams_auto_update.go:31 | exempt: needs a GitHub release feed and a managed install (CXP_RUNTIME_DISABLE=1 in the suite) | same |
| F-SVC-9 | keep | cli/managed_install.go | exempt: managed runtime install is disabled in the suite (CXP_RUNTIME_DISABLE=1); release infrastructure | same |
| F-SVC-10 | keep | core/engine/restart.go:269 `drainNotice` replaces the ACK (receive.go:969) of a message admitted while a restart or an update waits for running work: "…I saved this message and will resume it after …"; the turn stays queued and runs once after the restart; upgrade.go:227 `updateStep` drains | RESTART-DRAIN | SPEC text; v0.1.20 `sendDeferredServiceControlNotice` |
| F-SVC-11 | keep (owner: 第一级 5 v0.1.20 + rc.59 only) | core/migrate/migrate.go:62 | ENTRY-AUTOMIGRATE, ENTRY-MIGRATE-RESTART, ENTRY-MIGRATE-IMPORT-CRASH, ENTRY-MIGRATE-TAKEOVER, ENTRY-MIGRATE-FENCED-IMPORT, ENTRY-WATCHDOG-RUNTIME, ENTRY-WATCHDOG-LEGACY, ENTRY-MIGRATE-MISSING-DB, ENTRY-FRESH, ENTRY-MIGRATE, ENTRY-CONTROLSEEN, ENTRY-OLDANSWER, ENTRY-RESUMECHECK, ACCEPT-REALSTORE, CATCHUP-LIVE, SHADOW-REALDATA, BREAKER-BACKLOG | new |
| F-SVC-12 | keep | core/engine/upgrade.go:596 `cmdCodexUpdate` | CODEX-UPGRADE | same |
| F-CLI-1 | keep | cli/teams_newcore_status.go:84 "Installed version" (this build) next to the store's running version; cli/teams_newcore_poll.go:108 "Pending version: X installed, not running yet …"; :90 "Last poll: <time> (<age> ago)" from core-poll.json, written on the helper's 5 s watcher tick (may lag up to 10 s); while a helper holds the store the store lines (and `teams control`, the bootstrap's control-chat lookup) come from core-status.json, which the helper rewrites after commits (cli/teams_newcore_state.go), never from the store file; with no helper the store is read directly | STATUS-VERSIONS, STATUS-SOURCE | SPEC |
| F-CLI-2 | keep | cli/teams.go:523 (`--live`, `--appserver-probe`) | CLI-READONLY | same |
| F-CLI-3 | keep | cli/teams.go:392 `teams run`, :458 `--interval` (the control and active chats; idle tiers keep their production periods), :459 `--once` | ENTRY-FRESH, RUN-ONCE, RUN-INTERVAL | SPEC flag list |
| F-CLI-4 | removed-by-owner (第一级 4 【删除】; 定稿) | n/a | DELETED-CLI | — |
| F-CLI-5 | removed-by-owner (第一级 4 【删除】; 定稿) | n/a | DELETED-CLI | — |
| F-CLI-6 | keep (owner: 定稿 drop `captured-terminal` and `service` modes) | cli/teams_path.go:17-135 | DELETED-CLI (removed modes refused) | narrowed by owner |
| F-CLI-7 | removed-by-owner (beacon) | n/a | n/a | — |
| F-CLI-8 | keep (owner: 决定-3) | cli/delegate.go:69 | DELEG-RT | same |
| F-CLI-9 | removed-by-owner (beacon) | n/a | n/a | — |
| F-CLI-10 | keep | cli/teams.go:459 `teams run --once`: cli/teams_newcore.go:325 `watchTeamsOnceCycle` stops after one cycle (every chat read once, the work it brought in finished, kept command-line requests done) | RUN-ONCE | SPEC |
| R-1 | keep | outbox + read-back (see R-5) | R-1-2-3, R-5, R-5a-crash, R-5b, R-36, R-37-out, B-5, DUP-EDGE, DELEG-RT, ENTRY-MIGRATE-RESTART, ENTRY-MIGRATE-IMPORT-CRASH, ENTRY-MIGRATE-TAKEOVER, ENTRY-MIGRATE-FENCED-IMPORT, ENTRY-CONTROLSEEN, ENTRY-OLDANSWER, BREAKER-BACKLOG, KILL-SWITCH, STANDBY-LOCKED, STANDBY-TAKEOVER, TAKEOVER-*, STOP-START, SHUTDOWN, STOP-RESTART, STOP-SHARED, MOVED-HOME, MOVED-LIVE-STOPPED, MOVED-LIVE-BOTH, MIR-BAD-RECORDS, MIR-PROGRESS-CATCHUP, TURN-PROGRESS-INTEGRITY, TURN-PROGRESS-BATCH-CRASH | — |
| R-2 | keep | outbox marker + read-back; global invariant no-duplicate-post | R-1-2-3, R-5, R-5a-crash, R-5b, R-9-10, R-36, B-5, DELEG-RT, NO-WINDOW, IMPORT-CRASH, FORK-CRASH, MIR-REWRITE, TURN-UNPHASED, CATCHUP-CRASH, NEW-CRASH, GRAPH-OUTAGE, THROTTLE-CRASH, MIR-PROGRESS-CATCHUP, TURN-PROGRESS-INTEGRITY, TURN-PROGRESS-BATCH-CRASH | — |
| R-3 | keep | inbound dedupe | R-1-2-3, R-4-29, R-4-paging, R-6, R-7, R-37, R-43, DUP, DUP-EDGE, PARK | — |
| R-4 | keep | receive.go cursor + dispositions | R-4-29, R-4-paging, BREAKER, DELEG-RESTART, PARK | — |
| R-5 | keep | runner.go:912 `reconcile`; outbox read-back | R-5, R-5a-crash, R-5b, R-4-29, B-5, ACTIVE-WRITER, ACTIVE-WRITER-RESUME, DELEG-RESTART, CODEX-LOST, TURN-PROGRESS-BATCH-CRASH | — |
| R-6 | keep | per-chat outbox order; global invariant chat-order | R-6, R-36, R-4-paging | — |
| R-7 | keep | outbox marker identity (receive.go:746 the converted outbox's message ids identify the previous helper's posts) | R-7, R-5a-crash, BREAKER, BREAKER-BACKLOG | — |
| R-8 | keep | core/engine/annotations.go:35 `labelAdmitted` + channel/teams/annotate.go:50 `Annotate` (F-RND-4): one lossless PATCH per message, never a helper post, never during catch-up | EDIT-LABEL | — |
| R-9 | keep | mirror.go | R-9-10, NO-WINDOW, MOVED-HOME, GRAPH-OUTAGE | window removed by owner |
| R-10 | keep | mirror.go | R-9-10, MOVED-HOME, CATCHUP-CRASH | — |
| R-11 | keep | per-chat isolation | R-11, BREAKER, BREAKER-BACKLOG, PARK, GRAPH-OUTAGE | — |
| R-12 | keep | Retry-After handling; global invariant retry-after | R-12, INBOX, THROTTLE-CRASH | — |
| R-13 | keep | lease + supervisor backoff | SERVICE-STANDBY, STANDBY-LOCKED, STANDBY-TAKEOVER, TAKEOVER-CRASH-MID, TAKEOVER-LEFTOVER, TAKEOVER-SAME-HOST, STOP-RESTART | — |
| R-14 | keep | tiered reads; global invariants idle-quiet, cpu-budget | IDLE, INBOX, PARK, ACCEPT-REALSTORE, SHADOW-REALDATA | — |
| R-15 | keep | (bench) | exempt: throughput benchmark tier (gotest-docker-bench.sh), not a pass/fail black-box scenario | — |
| R-16 | keep | core/migrate; upgrade.go drain | ENTRY-MIGRATE, ENTRY-AUTOMIGRATE (conversion part; UAC/activation exempt) | — |
| R-17 | keep | see F-CLI-1 | STATUS-VERSIONS | — |
| R-18 | keep | core/engine/restart.go:90 `cmdRestart` / :147 `runRestart`: confirmation, `now` waits, `force`; one confirmation after the restart | RESTART, RESTART-DRAIN | — |
| R-19 | keep | silence.go hints (v0.1.20 texts); setup-error classes setuperr.go (F-TRN-16) | SILENCE, SETUP-ERR | — |
| R-20 | keep | turncmds.go:104 `cmdCancel` incl. `<turn-id>` (F-TRN-3) | R-20-33, CANCEL-CMDS, SILENCE | — |
| R-21 | keep | per-chat runners; control never queued | R-11 | — |
| R-22 | keep | freeze notice once per park (core/engine/annotations.go:106 meta marker, :159 `freeze` with `Present` check in channel/teams/annotate.go:285 `noticeEdit`); other notices once | BREAKER, DUP, IDLE, SILENCE, VOICE, WAKE-SYNTAX, EDIT-FREEZE | — |
| R-23 | keep | the envelope stays out of Teams and titles (transcript/strip.go:209 `StripHelperPrompt`, discover.go:163); `<oai-mem-citation>` stripped (F-MIR-8, transcript/strip.go:15) | F-TRN-14, F-MIR-8 | — |
| R-24 | keep | cli/teams_user_path.go, teams_child_guard.go; cli/teams_newcore_codexenv.go:122 (managed Codex for app-servers = the upgrade target), cli/teams_newcore_update.go:77 (unchanged-version upgrade: no restart); config.toml changes reach the next turn (core/codex/pool.go:187) | CODEX-MANAGED, CODEX-CONFIG, CHILD-GUARD | — |
| R-25 | keep | cli/teams_child_guard.go:14 | CHILD-GUARD | — |
| R-26 | keep | engine/envelope.go:549 `containsRawAPIKey` (v0.1.20 port), :500 `screen` / :537 `refuse`: a prompt or command with a raw key from a human is refused with one unquoted reply, never run or sent to Codex, and its body is stored redacted (receive.go:506); reply text adapted (no `model setup`, owner removed third-party APIs) | R-26 | B:4613 |
| R-27 | keep | control chat owner-only (receive.go:744); coworker commands refused with v0.1.20's text (receive.go:753), coworker ACK text (:967) | R-43, GROUP-CMDS | — |
| R-28 | keep | core/lease/acquire.go:185 | TAKEOVER-*, STANDBY-TAKEOVER, KILL-SWITCH | — |
| R-29 | keep | store-backed queue/outbox | R-4-29 | — |
| R-30 | keep | per-session runner | R-6 | — |
| R-31 | keep | turn captures model/effort | MODEL-SWITCH-BUSY | — |
| R-32 | keep | thread bound before `turn/start` | B-5, CODEX-LOST | — |
| R-33 | keep | runner.go `finish` | R-20-33, B-5, INT-NOTICE, TURN-UNPHASED, CODEX-LOST | — |
| R-34 | keep | mirror recognises Teams turns | R-9-10 | — |
| R-35 | keep | silence.go:189 queued ACK, runner.go:260 starting notice, silence.go:108 reminder | R-6 | ACK text per owner |
| R-36 | keep | channel/teams/render.go | R-36 | — |
| R-37 | keep | files.go, outbound.go; per-message limits engine/envelope.go:59 (F-RND-8) | R-37, R-37-out, VOICE, F-RND-8 | — |
| R-38 | keep | core/migrate refuses unsupported/corrupt stores | ENTRY-MIGRATE | — |
| R-39 | keep | `new` idempotent (createOrGet + record) | NEW-CRASH | — |
| R-40 | keep | see F-TRN-17 (`pause/resume` = hidden aliases of `stop/start`; drain internal only) | PAUSE-ALIAS | — |
| R-41 | keep | see F-WRK-10: the child's history is the parent's records up to the cutoff, once each (delivery keys of the child's namespace; a crash mid-import resumes from the stored cursor); the child's own rollout is mirrored from where it stood at the fork (its source row is written in the fork's transaction; core/engine/mirror.go:569 keeps discovery off a child rollout until it is bound), so nothing of the parent is posted twice and none of the child's records is skipped; only forks converted from v0.1.20 keep "mirror from the size first seen" (core/engine/mirror.go:580) | CMD-FORK, FORK-RUNNING, FORK-CTL, FORK-CRASH | — |
| R-42 | keep | core/transcript/usage.go:242 resets, :599 fork-prefix attribution; see F-STA-1 | CMD-STATS, STATS-RESETS | — |
| R-43 | keep | receive.go:785 | R-43 | — |
| R-44 | keep | read-only rollout access by construction | MIR-BAD-RECORDS, MIR-REWRITE | — |
| R-45 | keep | no DELETE call in channel/teams | exempt: enforced in every scenario by the global invariant graph-contract (the fake models DELETE only for upload sessions) | — |
| R-46 | keep | cli/teams_newcore_status.go read-only open | CLI-READONLY | — |
| R-47 | keep | core/engine/defaults.go:89 `default …` in the control chat (F-CTL-15); model setup removed by owner | CMD-DEFAULT | — |
| R-48 | keep | upgrade.go:596 waits for drain, no auto-retry | CODEX-UPGRADE | — |
| R-49 | keep | notify.go, channel/teams/workflow.go | NOTIFY-WH, NOTIFY-NOWH, NOTIFY-WH500, NOTIFY-WHDOWN, WORKFLOW-LIVE | — |
| R-50 | keep (owner: 决定-4, new P0 — a voice message is never silently lost) | core/engine/files.go:185-215 | VOICE | — |
<!-- catalog:end -->

DC-1…DC-9 (SPEC §2.2) are internal design constraints, not user-visible behaviour; they are not catalogued and not enforced by the test.

## 3. Scenario tag aliases

Scenario `Guarantees` tags that are not catalog ids. `-` = deliberately not a catalog row. Tags of the form `F-XXX-n` / `R-n` (optionally with a letter suffix, e.g. `R-5a`) map to themselves.

<!-- aliases:begin -->
| tag | maps to | note |
|---|---|---|
| F-WRK-dup | F-TRN-6 | duplicate guard |
| F-ATT-voice | F-RND-11 | voice / ASR |
| F-CTL-park | F-CTL-12 | park/wake syntax |
| cmd-fork | F-WRK-10 | |
| cmd-model | F-TRN-7 | |
| cmd-skills | F-CTL-17 | |
| cmd-stats | F-STA-1 | |
| interrupt-notice-once-no-internal-ids | F-TRN-5 | |
| auto-migrate | F-SVC-11 | |
| entry-point | F-SVC-11 | |
| unsupported-store-refused | R-38 | |
| catch-up-silent | F-MIR-4 | owner 11-05 silent catch-up |
| catch-up | F-MIR-4 | |
| cpu-budget | R-14 | |
| no-hot-loop | R-14 | |
| codex-auto-upgrade | F-TRN-11 | |
| helper-restart-one-confirmation | R-18 | |
| owner-notify-every-final | F-RND-5 | |
| active-writer-one-notice | R-5 | R-5 (c) |
| capacity-retry-5min | F-TRN-10 | |
| D13 | F-RND-7 | design decision D13 (formula) |
| INC-02 | R-7 | incident family |
| removed-commands | - | asserts removed commands stay removed |
| cmd-goal | - | owner request 2026-11-07: `helper goal`, goal and model lines, `helper cancel` pauses the goal (after SPEC) |
| dash-order | - | owner request 2026-11-07: chat lists (`st`, `list`, `stats`, the new-chat summary) by Codex activity, the most recent last (after SPEC) |
| B-5 | - | E2E plan scenario label |
| N-2 | - | CORE-DESIGN latency target |
| N-3 | - | CORE-DESIGN latency target |
<!-- aliases:end -->

## Appendix A — v0.1.20 command texts (verbatim, extracted from v0.1.20 @ 2ae5e748)

Extracted by reading the source; re-check against the cited line before porting. "B" = `bridge.go`. Known corrections: there is no "incomplete trailing record" import text; `helper update status` is not a v0.1.20 command; `helper ph` is not recognised (only `!ph`, `/ph`, `cx ph`).

### Control chat

Dispatch is `handleControlMessage` B:4613. Before dispatch it does three things in order: it rejects raw API keys, records the user message to control history, and checks attachments.

Unless noted, errors go through `controlCommandErrorMessage` B:20769:
- view expired: "This numbered list expired. Send `projects` or `sessions` again, then choose one of the newly shown numbers."
- no view: "I do not have a current list yet. Send `projects` or `sessions` first, then choose a number."
- number not in list: "That number is not in the current list. Send `projects` or `sessions` again, then choose one of the newly shown numbers."
- anything else: `err.Error()`

#### 1. Dashboards and number selection

**Workspaces (`p`)** — `formatWorkspaceDashboard` B:19702 (~65 lines, plus ~90 lines of helpers to B:19840).
- Discovery error reply: "workspace discovery failed: "+err
- Empty: `"## 📁 Workspaces\n\nNo local Codex workspaces found on this machine.\n\nCodex history is stored locally. If you used Codex on another computer, run this helper there too.\n\n**Next:** send `new <directory>` to create a Work chat for a directory."`
- Header: `## 📁 Workspaces` / `` / `Reply with a number to open a workspace.` / `` / `---`
- Each row (body lines indented 2 spaces, then `---`):
  - `**%d. %s**` (title, or "Unknown workspace")
  - `` `abs path` `` or "Path not recorded by Codex"
  - "Sessions: none", or `Sessions: %d active, %d idle, last updated 2006-01-02 15:04`
  - when no path: "Older Codex records without a working directory are grouped here."
  - blank line, then ``**Next:** send `%d` or `p %d` to open this workspace``
- Footer: `` / `**Other options:**` / ``- Send a workspace number, for example `1`, to open it.`` / ``- `n <directory>` or `new <directory>` - create a new Work chat directly.`` / `` / ``Numbers apply to your next reply and expire after 10 minutes. If a number looks wrong, send `projects` again.``
- Ordering is in `dashboard.go:226` (`BuildControlDashboard`): workspaces by UpdatedAt desc, then title, then path. Sessions are numbered from 1 within each workspace.

**Workspace sessions (`w`/`p <n|path>`) and `s`** — `formatWorkspaceSessionsDashboard` B:19840 (~125 lines).
- A number resolves through the view. A path becomes `workspaceIDForPath`. With no argument it uses the selected workspace.
- Wrong kind: "number %d is not a workspace in the current dashboard view"
- Empty, with a workspace: `"## Sessions\n\nWorkspace: %s\n\nNo local Codex sessions were found in this workspace on this machine.\n\nCodex history is stored locally. If you used Codex on another computer, run this helper there too.\n\n**Next:** send `new` to create a new Work chat in this workspace."`
- Empty, no workspace: `"## Sessions\n\nNo local Codex sessions found on this machine.\n\n**Next:** send `projects` to choose a workspace.\n\nTip: send `new <directory>` to create a new Work chat."`
- Header: `## Sessions` / `` / `Workspace: <path>` / `` / `Reply with a number to continue a session in Teams.` / `` / `---`. The Workspace line is omitted when no workspace is selected.
- Each row is `**%d. %s**`, then these indented lines:
  - path line
  - `Session: active|idle, last updated …` or `Session: idle, update time not recorded`
  - if linked and active: `**Chat:** [▶ Open chat](url)`
  - if parked: ``**Resume parked chat:** send `r <id>` ``
  - `**Next:** ` + one of:
    - "send `%d` or `c %d` to continue this session in Teams"
    - "send `%d` to open this Teams chat or import updates" (linked, active)
    - "send `%d` to create a new Work chat from this closed Teams chat" (linked, closed)
- Footer: ``Need debug IDs? Send `details <number>`. Numbers expire after one reply.`` / `` / ``**Next:** send `new` to create a new Work chat in this workspace.``

**Bare number** — `resolveControlSelection` B:20304.
- A workspace number renders the sessions view.
- A session number calls `publishCodexSessionWithProgress` and then clears the view.
- An error clears the view.

**How the view is stored and cleared**
- `persistControlDashboard` B:20430 writes `store.DashboardViews[controlChatID]`: kind, workspace id, items, ExpiresAt = now + 10 min (`DefaultDashboardViewTTL`, dashboard.go:17). It also writes the `DashboardNumbers` and `Workspaces` records.
- `controlCommandConsumesDashboardView` B:4889 returns false only for Workspaces, Workspace, Sessions and Select. Every other command, including non-command text, makes `handleControlMessage` defer `clearControlDashboardView` (B:20508, deletes `Views[chatID]`).
- `ResolveDashboardNumber` (dashboard.go:263) checks expiry. `dashboardSelectionStillVisible` re-runs discovery.

#### 2. `o`/`open <n>` — `formatOpenControlTarget` B:20036 (~25 lines, plus `formatSessionSelection` B:20379)
- Workspace number: "number %d is a workspace in the current dashboard view; send `project %d` to list its sessions"
- Not in Teams: "This local Codex session is not in Teams yet.\nNext: send `%d` or `continue %d` to create a Work chat and import its history.\nUse `details %d` to show technical IDs." The variant without a number says: "…Next: send `sessions`, then choose its number to create a Work chat and import its history.\nUse `details <number>` from the sessions list to show technical IDs."
- Closed chat: "This Codex session has a closed Teams work chat. The helper no longer polls that chat.\nNext: send `continue %d` to create a new work chat and continue from local history.\nUse `details %d` to show technical IDs."
- Linked: `formatOpenSessionMessage` B:15537 sends `%s [%s] %s` (id, status, topic), the chat URL, and then one of:
  - "Next: open this Teams work chat and send a message there to continue. `open` only shows the linked chat; it does not import local history."
  - "This parked work chat was resumed. Next: open this Teams work chat and send a message there to continue."
  - closed: "This work chat is closed, so the helper no longer reads or responds there. Use `sessions` then `continue <number>` to continue the local Codex session in a new work chat."
- It auto-resumes parked chats (`resumeWorkChatIfParked`), which posts "This chat has been resumed. Messages sent here will be read again." in the work chat.
- Other replies: "usage: `open <number>`", "session not found: %s".

#### 3. `d`/`details <n>` — `formatDetailsControlTarget` B:15573 (~80 lines with helpers)
- Directory number: "number %d is a directory in the current list; send `project %d` to list its sessions"
- Local, unpublished session: lines `Local Codex session`, `List number: N`, `Title:`, `Working directory:`, `Updated: <RFC3339>|unknown`, `Codex session ID:`, `Codex thread:`, `Teams chat: not published`, ``Next: send `continue N` to create a work chat and import its history.``
- Linked session (`formatSessionDetails` B:15652): `Session:`, `Chat polling: listening|closed`, `Title:`, `Working directory:`, `Model profile:` (only if not default), `Codex thread:`, the beacon target line, `Teams chat: <url>`, `Latest turn:`, `Codex status: running|idle`, `Latest Codex turn:`. If no status is known: "Codex status: will start when you send a task".
- With no argument it shows the status list.

#### 4. `st`/`status` — `formatSessionList` B:15242 (~55 lines, plus ~60 lines of helpers)
- Ordering is the reverse of registry order, i.e. most recently added first.
- Lines are joined with `\n`. Format:
```
## Active Work chats
**<Topic>** [active|🧊parked]
  **Session:** `<id>`
  **Folder:** `<cwd>` | not recorded
  **Last used:** 2006-01-02 15:04 | not recorded
  **Chat:** [▶ Open chat](<url>)
  **Resume parked chat:** send `r <id>`      (parked only)
  **Model profile:** <label>                 (non-default only)
---
```
- Footer:
  - "%d closed work chat(s) hidden. The helper no longer reads or responds in closed chats."
  - a quarantine section: `## ⚠️ Quarantined Work chats`, rows `- **%s** — session `%s`; inspect locally, then run `cxp teams chat unquarantine %s --yes` only when safe.`
  - the ASR line, e.g. "Speech recognition: configured." (asr_input.go:393)
  - "Next: open one of these Teams chats to continue work, or send `new <directory>` to create another Work chat."
- No active chats: "Control status: no linked work chats yet.\n\n<ASR>\n\nNext: send `projects` to choose a workspace, or `new <directory>` to create a Work chat."
- Only closed or quarantined chats: lines joined with `\n\n`:
  - "Control status: no active linked work chats."
  - "%d closed work chat(s) are hidden because the helper no longer polls them."
  - the ASR line
  - "Send `projects` to choose a workspace, `new <directory>` to create a Work chat, or `sessions` then `continue <number>` to import an existing local Codex session."
- Parked state comes from the store `ChatPolls[chatID].PollState == parked`.

#### 5. `c`/`continue`/`publish`/`import` — `publishCodexSessionWithProgress` B:15710 → `publishCodexSessionLocalWithOptions` B:15741 (~175 lines, plus the import engine)
- Usage: "usage: `continue <number-or-session-id>`"
- Directory number: "number %d is a directory in the current list; send `project %d` first to list its sessions, then `continue <session-number>`"
- Not found: "Codex session not found: %s"
- Progress message (`publishHistoryPreparingMessage` B:15918): `✅ Work chat created|Work chat reopened: <id>` / `` / `Open Work chat:` / `<url>` / `` / `Preparing local Codex history now. Long sessions can take a few minutes.` / `Wait for "Import complete" in the Work chat before sending a new task there.`
- New chat: Graph `CreateMeetingChat` with `WorkChatTitle`, TitleSource=auto, model and effort from the global default. It posts the lifecycle notice (item 20), imports, then the work chat gets `formatTranscriptImportCompleteMessage` B:16174:
  - "Import complete. This chat is ready; reply here to continue the Codex session."
  - or "Import complete. Imported %d visible history item(s) and skipped %d background tool event(s) from history to keep this Teams chat readable. New live Codex status updates will still appear here.\n\nThis chat is ready; reply here to continue the Codex session."
- Control-chat result: "Published local Codex session as %s: %s\n\nOpen this Teams work chat and send a message there to continue."
- Already-linked session:
  - "Already published as %s: %s\n\nHistory import is still running. Wait for \"Import complete\" in the Work chat before sending a new task there."
  - or "Already published as %s: %s\n\n%s%s Open this Teams work chat and send a message there to continue.", where the first `%s` is "No new local history was imported." / "New local history was imported." / "New local history import was queued.", and the second is "" or " The parked Work chat was resumed."
- Checkpoint missing (B:121): "Local Codex history sync needs attention. I could not find the saved transcript checkpoint, so I did not guess an import position or import local history from the wrong place."
- While the service is paused or draining, the request is deferred, or the reply is "Teams bridge is paused|draining; publishing existing sessions are not being accepted.[ Reason: …]".

#### 6. `m`/`mkdir` — `createWorkspaceDirectory` B:7830 (10 lines)
- Replies: "usage: `mkdir <directory>`"; "create workspace failed: "+err; "Directory is ready: <dir>\n\nNext: send `new <quoted dir>` to create a work chat for this directory."
- Creates the directory with MkdirAll 0700.

**Path hint**
- `looksLikeControlPath` B:7343 matches a single line starting with `./`, `../`, `~/`, or `X:\`/`X:/`.
- `controlPathHintMessage` B:7358: "📁 Detected path: `%s`\n\nChoose one:\n\n- `new %s` - create a 💬 Work chat there\n- `mkdir %s` - only create the directory". The path is strconv-quoted if it has spaces or quotes.
- `unknownControlCommandMessage` also uses this hint for unknown command names containing `/` or starting with `.`.

#### 7. Rename
**`renameControlChat` B:10683 (~30 lines)**
- Replies: "usage: `helper rename <title>` or `rename <title>`"; "rename failed: control chat is not bound yet"; "rename failed: "+err; "Control chat renamed.\n\nNew title:\n<topic truncated to 96>"
- Graph `UpdateChatTopic` with `ControlChatTitle{MachineLabel, Profile, UserTitle}`. Sets registry `ControlChatUserTitle` and TitleSource=user, and persists the binding.

**`parseRenameHostnameArgument` B:10385 + `renameMachineHostname` B:10507 (~175 lines)**
- Usage: "usage: `helper rename hostname <name>`"
- Calls Graph `UpdateChatTopic` on the control chat and on every work chat (from store sessions plus the registry), and saves MachineIdentity, Machines and ControlChat in the store.
- Reply: "Hostname renamed.\n\nNew hostname: X", then "Control chat: updated" or "Control chat: not bound", then "Work chats: %d updated". On failures: "", "Some chat titles were not updated:", "- …" (up to 4), then "- and %d more".
- The same handler runs in work chats (it replies in that chat).

#### 8. Help texts (B:5123 and B:5150, verbatim)
`help`:
```
## 🏠 Control chat
Use this chat to choose a workspace and create or continue 💬 Work chats.

Start here:
- `p` / `projects` - choose a workspace
- `n <directory>` / `new <directory>` - create a new Work chat for a directory
- `new <directory> --model <model>` - create a Work chat pinned to a model
- `model status|list|switch|reset` - inspect or change only this Control chat's model
- `effort status|list|set|reset` - inspect or change this Control chat's reasoning effort
- `default status` / `default model ...` / `default effort ...` - manage global defaults for future launches and chats
- `s` / `sessions` - show sessions in the selected workspace
- `c <number>` / `continue <number>` - continue an old local Codex session in Teams
- `st` / `status` - show active Work chats
- `helper rename <title>` - rename this Control chat
- `helper rename hostname <name>` - rename this machine in all related chat titles
- `skills` - list installed skill subscriptions

After `p`, reply with a number such as `1` to open that workspace. On a workspace page, send `new` to create a Work chat in that workspace.

For quick helper questions, type the question here. For project work, use a 💬 Work chat.

Send `help advanced` for all commands.
```
`help advanced` (also accepts the argument `all`, `more` or `full`, via `isAdvancedHelpArg` B:5262):
```
## 🏠 Control chat advanced help

Workspace flow:
- `p` / `projects` - list directories with local Codex history
- `p 1` / `project 1` / `1` - open workspace 1 from the `projects` list
- `new` / `n` - create a Work chat in the currently opened workspace
- `n <directory>` / `new <directory>` - create the directory if missing, then create a Work chat there

History flow:
- `s` / `sessions` / `history` - list local sessions in the selected workspace
- `c 1` / `continue 1` / `1` on a sessions page - create/open a Work chat and import that session history

Other control commands:
- `st` / `status` - list active Teams work chats
- `d <number>` / `details <number>` - show technical IDs and details
- `park <session-number-or-id>` / `resume <session-number-or-id>` / `unpark <session-number-or-id>` - manually pause or resume polling for a Work chat
- `m <directory>` / `mkdir <directory>` - create a directory only
- `ask <question>` - ask a quick helper question in this control chat
- `helper cancel last` / `helper cancel queued` / `helper cancel running` / `helper cancel all` - stop queued/running Codex question(s) in this Control chat
- `model status|list|switch|reset` and `effort status|list|set|reset` - inspect or change only this Control chat (`model use` aliases switch)
- `model setup|doctor` changes availability only; Control-only `default model ...` / `default effort ...` manage future global defaults
- `helper rename <title>` - rename this Control chat
- `helper rename hostname <name>` - rename this machine in the Control chat and all linked Work chats
- `h` / `help` / `menu` - show short help
- `helper restart` - show the safe restart confirmation
- `helper restart now` - restart the local Teams helper after sending a confirmation
- `helper update now` - update to the latest stable helper release
- `helper update prerelease` - update to the newest helper release or pre-release
- `codex update now` - wait for active work, then update the local Codex CLI
- `helper webhook setup` - show a guided Workflow webhook setup flow
- `helper webhook <url>` - enable Workflow notification cards with a Teams Workflow webhook URL
- `helper webhook off` - disable Workflow notification cards
- `helper skills list|add|sync|push` - manage skill subscriptions

Beacon commands:
- `beacon list` - list beacon profiles and machines
- `beacon profile create <name> ...` - create a draft beacon profile
- add `--query-command <script> --submit-command <script> --cancel-command <script> --renew-command <script>` to store provider adapters on the profile
- Slurm/LSF adapters use your normal shell setup by default; add `--adapter-shell direct` when user-shell capture is incompatible or a clean service environment is required
- `beacon profile update <name> ...` - create a new profile revision
- `beacon profile history <name>` / `beacon profile rollback <name> <revision>` / `beacon profile gc <name>` - inspect, restore, and prune revisions
- `beacon profile doctor <name>` - validate profile fields and provider adapters
- `beacon profile confirm <name>` - confirm review; incomplete profiles remain draft until doctor requirements pass
- `beacon release <target> [--force] [--confirm <token>]` - preview and release a beacon resource
- advanced: `beacon allocation ...` and `beacon machine ...` inspect internal state
- `new <directory> --beacon <profile>` - create a Work chat on a ready beacon profile
- Beacon execution profiles are separate from SSH proxy profiles managed by `cxp proxy`.

Local CLI:
- `cxp skills install-builtin` - install or repair bundled local skills in `$HOME/.agents/skills`
- `cxp skills migrate` - migrate managed legacy skills from `~/.codex/skills` to `$HOME/.agents/skills`

work chat commands:
Inside a 💬 Work chat, send your task as a regular Teams message. Use `helper help`, `helper status`, `helper stats`, `helper retry last`, `helper file <relative-path>`, or `helper close` for helper actions.
Status words: `queued`/`running` means wait, `completed` means done, `failed` or `interrupted` means check recent messages and changed files before `helper retry last`.
If this chat stops replying for about a minute, send `helper status`. From the control chat, `helper restart now` restarts the installed helper; `helper reload now` is for source-checkout development reloads.

copy-ready examples:
`p`
`n /home/baka/project/codex-helper`
`m ~/tmp/mobile-fix`
```

#### 9. Control Q&A — `runControlFallback` B:4901 (~60 lines, plus the queued runner B:4963 and input prep B:4997, ~110 lines; prompt builder ~250 lines)
- `ask` with no text: "usage: `ask <question>`". `ask` rewrites the message body to the escaped argument.
- Empty text with no media replies with `controlHelpText()`.
- Session: a durable session with the fixed id `__control_fallback__` (B:125). Created by `ensureControlFallbackSession`, RunnerKind `control_fallback`; model and effort come from the global default.
- Ack (`queueControlFallbackAck` B:7488, outbox kind "ack", AckKind "control_prompt"): "❓ **Codex received your control-chat question.**\n\nThis message is not a helper command, so **Codex will answer it here after helper-local preparation succeeds**. The request has been accepted; attachments or Teams voice/video may still be processed before Codex starts.\n\nTo stop this control-chat request, send `helper cancel <turnID>` here. For project work, send `new <directory>`, then send the task inside the new 💬 Work chat."
- Executor (`effectiveControlFallbackExecutor` B:7396): `-c model_reasoning_effort="low"`. `DefaultControlFallbackModel` is `""` (artifact_handoff.go:18-19), so the Codex default model is used unless `controlFallbackModel` is set; `--model` is added only when a model is set.
- Hidden prompt (`ControlFallbackCodexPromptWithContext` artifact_handoff.go:74) contains:
  - the history keyword
  - a `<codex-helper-control-context version="1">` block: routing rules, the list of control, work, cxp and beacon commands, and "Reply in the user's language…"
  - "Current helper state:": helper_version, control chat title and id, active work chats as `` `id` - title - cwd=`…` - chat=`…` ``, and the current dashboard view (up to about 12 items)
  - "Control chat context:": the history path plus the recent tail from control_history.go. That is a JSONL/SQLite file `control-chat-history.jsonl` next to the store, max 500 entries; the prompt tail is 20 items / 8000 chars. Every user message and every helper outbox message in the control chat is recorded.
  - the help digest (≤7200 chars)
  - `</codex-helper-control-context>`
  - the math contract
  - "User message:\n<prompt>"
- Cancel (`cancelControlFallbackCommand` B:9479 → `cancelTurnCommandForScope` B:9490, ~80 lines):
  - empty: "usage: `helper cancel last`, `helper cancel queued`, `helper cancel running`, `helper cancel all`, or `helper cancel <turn-id>`\n\nThese commands apply to the current Control chat Codex question."
  - nothing to cancel: "no running or queued turn is available to cancel in this …" (also "no queued turn…" / "no running turn…")
  - "turn not found in this …: id"
  - "turn %s is %s and cannot be canceled."
  - "This Codex request is running, but this helper process does not own its live cancel handle. …"
  - after a cancel during prep: "Codex request canceled."

#### 10. Misc control texts
- `unknownControlCommandMessage` B:5271: "unknown control command: `%s`\n\n<controlHelpText>"
- "⚠️ Wrong chat\n\nThis is the 🏠 control chat. `helper ...` commands like `helper file`, `helper retry`, and `helper close` work inside a 💬 Work chat.\n\nTo start project work, send `new <directory>` here, then open the new Work chat and send the task there."
  - How it's reached: `helperPrefixedControlNameIsWorkOnly` (dashboard_commands.go:154) makes `helper close|retry|restore(-thread)|cancel|stop|send-file|send-image|file|image|publish-history|sync-history|import-history|stats|usage|tokens` parse as a helper Unknown command, not a forward to Codex. Then `isWorkOnlyHelperCommand` B:7330 (the same list plus `rename`) picks this message.
  - Exception: `cancel`/`stop` with an explicit argument parse as the Cancel command instead.
- Attachments (text.go:113/121):
  - Generic: "I could not process %d Teams attachment(s): %s. Supported inputs are plain text plus Teams-hosted inline/reference file attachments that the current Graph scopes can download. For generated files from Codex, use `helper file <relative-path>` from the Teams outbound root."
  - Control adds: " Files and images belong in a 💬 Work chat; the 🏠 control chat only handles text commands like `projects`, `new <directory>`, `continue <number>`, and `status`."
  - Explicit (a quoted message on a non-`ask` command) adds: " Quoted Teams messages in the 🏠 control chat are only supported with `ask <question>` or natural-language fallback prompts; explicit control commands do not consume quoted context."

#### 11. Model / effort / default
**Model — `handleModelControlCommand` model_profile_commands.go:15 (~110 lines, file is 716 lines)**
- `status|current` → `formatChatModelStatus` :310: "Chat: Control", summary lines, "Model profile: …", "Switchable: yes. Switching preserves the existing Codex thread and portable context, if present." or "Switchable: no - …", "Codex thread: …".
- `list|ls|profiles`, and natural-language list requests (English or Chinese) → `modelManagerList`.
- `provider(s)` list/setup: "usage: `model provider setup <provider>`".
- `catalog list|sync`: "usage: `model catalog sync <name>`"; "Catalog management is available from the local CLI: `cxp model catalog list|sync <name>`."
- `setup|guide|add|create` (may start Teams key intake); success text: "Model available: Codex Official\n\nCurrent chats and global defaults are unchanged." or "Saved model %s as `%s`.…"
- `key` → key-intake usage. `doctor|check`.
- `switch|use|set` (usage "usage: `model switch <model>`") → :343. Possible replies:
  - "Model profile switched for this Control chat\nPrevious: …\nEffective next-turn model: …\nSelection: …\n\nThe next message will continue the current Codex thread with its portable context preserved." (or "…will start a Codex thread with the selected model.")
  - "Model profile already selected for this Control chat: …\nSelection: …"
  - "Model profile switch scheduled for this … chat\nPrevious: …\nNext: …\n\nCurrent queued or running work keeps its captured model. The switch will apply automatically when this chat becomes idle."
- `reset` is a switch to the global default.
- `default|set-default`: "Global defaults moved to the Control-only `default` command. Use `default model set <model>` or `default model reset`."
- `delete <profile> --confirm`. `effort …` delegates to the effort handler.
- Anything else → `modelControlUsage()` :263 ("Model commands:" plus 15 bullets).

**Effort — `handleReasoningEffortControlCommand` reasoning_effort_commands.go:20 → :32 (~70 lines, file 258)**
- Runs against the control-fallback session.
- `status`: "Current chat effort (Control)\nEffective for next turn: `x`\nSource: <chat override|global default|model-advertised default|Codex runtime default|helper fallback|runtime fallback>\nApplies to: new turns in this chat; running and already queued turns keep their captured effort.\nModel for this chat: `m`\nModel-advertised default: `d`\nAvailable: …". If the list call fails: "Dynamic list unavailable: …".
- `list`: "Current chat effort choices for M:" with rows "- `e` (current, model-advertised default) - desc", then "Use `effort set <value>` to switch this chat."
- `set|switch|use <v>`, or a bare value: "Reasoning effort updated for this Control chat\nPrevious: `p`\nEffective for next turn: `e`\nModel: `m`\nSource: …\nApplies to new turns in this chat; running or already queued turns keep their captured effort."
- `reset|default` uses the global default, then the model default.
- Errors: "usage: `effort set <value>`"; "reasoning effort %q is not supported by model %q; available values: …".
- Usage text: "Reasoning effort commands:\n- `effort status` - show this chat's current effort\n- `effort list` - list model-advertised choices\n- `effort set <value>` - switch future turns in this chat\n- `effort reset` - use the current model's advertised default".
- The catalog comes from the executor's `model/list`.

**Default — `handleDefaultControlCommand` default_commands.go:35 (93-line file)**
- Not configured: "Global default management is not configured for this Teams service."
- Grammar: `default [status]`, `default help`, `default <setting> status|list|set <v>|reset|help`.
- Errors: "usage: `default status`", "usage: `default %s list`", "usage: `default %s set <value>`", "usage: `default %s reset`", "unknown default action %q; use `default %s status|list|set|reset`".
- Output text is owned by `internal/cli/teams_defaults.go:45` (`HandleDefaultCommand`).

#### 12. `helper webhook` — `workflowWebhookFromControl` B:5529 (~170 lines with texts)
- `""|help|?` → help: "## 🔔 Workflow webhook" plus commands setup / <https-url> / test / status / off, and "The webhook URL is stored locally as a private secret file and is not echoed back."
- `setup|guide|start` → the 5-step guide (B:5615), which includes "Search `webhook`, then choose exactly: **Send webhook alerts to a chat**".
- `status|st`: "🔔 Workflow webhook enabled.\n\nWebhook URL: configured as a local private secret file." plus "Bound control chat: `id`" and "Updated: …". Disabled: "🔕 Workflow webhook disabled.\n\nSend `helper webhook <https-url>` to enable Workflow notification cards."
- `off|disable(d)`: "🔕 Workflow webhook disabled.\n\nThe helper will stop sending Workflow notification cards." On failure: "⚠️ Workflow webhook was not disabled.\n\n…"
- `test`: "✅ Workflow webhook test card queued." / "⚠️ Workflow webhook test failed.\n\n…"
- `set/url/enable <url>` or a bare URL. The URL is taken from the anchor href in the Teams HTML.
  - Bad URL: "⚠️ Workflow webhook URL was not saved.\n\nSend an absolute `https://...` Teams Workflow webhook URL, for example:\n`helper webhook https://...`"
  - Save failed: "⚠️ Workflow webhook was not saved.\n\n…"
  - Success: "✅ Workflow webhook enabled.\n\nNotification cards will be sent for important helper events.\nWebhook URL: saved locally as a private secret file; I will not echo it back.\nBound control chat: `id`\n\nSend `helper webhook test` to send a test card."
- Mutating sub-commands are deduplicated with `controlCommandAlreadyHandled`.

#### 13. Updates
**`updateHelperFromControl` B:5399 (~75 lines)**
- Accepted arguments: `now|latest|release|stable` and `prerelease|pre|rc|--pre…` (`parseHelperUpdateRequest` B:5725). Anything else, including `status`, shows the help text "## ⬇️ Helper update…" (B:5758), which ends "Automatic updates currently check: stable releases only|stable releases plus eligible pre-releases." The parser only routes `helper update <x>` when `x` parses, so `update status` likely never reaches this handler.
- Replies:
  - "⚠️ Helper update is not available in this helper process. Start it with the normal Teams service command, then try again."
  - "⚠️ Helper update check failed\n\n…"
  - "✅ Helper is already on the latest stable release for this machine." / "…latest available release or pre-release for this machine."
  - "⬇️ Helper update scheduled\n\nTarget: `tag`\nChannel: stable release|release/pre-release\n\nI will install it when active Codex work is idle, then restart the helper."
  - "⚠️ Helper update failed\n\n…"

**`updateCodexFromControl` B:5475**
- Anything other than `now` shows the help "## ⬆️ Codex CLI update…".
- Replies: "⚠️ Codex CLI update is not available in this helper process. …"; "⚠️ Codex CLI update could not be scheduled\n\n…"; a drain-blocked message; "⬆️ Codex CLI update scheduled\n\nI will wait for active Codex requests to finish, then update the local Codex CLI using its detected install source.\nI will post the completion or failure here. Existing requests are not retried automatically."

### Work chat

Dispatch is `handleResolvedSessionMessageWithQueueState` B:7981; the switch is at B:8001-8103. Every helper command first checks `messageAuthoredByCurrentUser`, otherwise it goes to item 19.

#### 14. Commands
- **close** (B:8007): sets status closed and calls `closeDurableSession`. Reply: "Session closed. The helper will no longer read or respond in this Teams chat.\n\nThis chat remains visible in Teams. Use the 🏠 control chat to open or create another work chat."
- **details**: `formatSessionDetails` (item 3).
- **rename** (`renameSessionChat` B:10349, ~35 lines): Graph `UpdateChatTopic` with `WorkChatTitle{…UserTitle}`; sets UserTitle and TitleSource=user.
  - Replies: "usage: `helper rename <title>` or `!rename <title>`"; "rename failed: …"; "Work chat renamed.\n\nNew title:\n<≤96>\n\nUse `helper details` to see the full Teams title and link."
  - `rename hostname X` goes to item 7.
- **publish-history** (B:8051; handlers B:16039 and B:16083, ~85 lines). Any other argument: "Usage: `helper publish-history` imports the pending backlog; `helper publish-history full` imports the complete local Codex history into this chat."
  - Shared replies: "No local Codex history file is linked to this Work chat.", "History import is already running. Wait for `Import complete` before sending another task.", "History import failed: …", then the Import complete text or "No new visible local Codex history needed to be imported. This chat is ready."
  - `full`: "Full history import is already running for this chat.", "Full local Codex history is already published to this chat.", "Full history import failed: …", "Full local Codex history is now up to date in this chat."
- **file** (`sendFileCommand` B:9748, ~45 lines):
  - "usage: `helper file <relative-path>` or `!file <relative-path>`\nOutbound root: <root>"
  - "Teams file upload auth check failed: …"
  - "Teams file upload is not authenticated. Run `codex-proxy teams auth file-write` locally, put files under <root>, then retry `helper file <relative-path>`. Token cache: …"
  - "Teams file upload rejected: %v. `helper file` only reads relative files under the Teams outbound root: %s"
  - On success it queues an attachment upload with the text "file attached: <name>".
- **status** (`formatWorkSessionStatus` B:15374, ~95 lines), lines joined with `\n`:
  - `STATUS: Work chat`, `Session: id`, `Chat: listening|closed`, `Folder: cwd`, `Model profile:` (non-default only), the ASR line, the beacon line
  - `Codex status: running|idle`, `Last request: queued|running|completed|failed|interrupted`, `Latest error:`. With no turns: "Codex status: will start when you send a task" / "Last request: none"
  - per-state hint (queued / running / retry)
  - `Messages waiting to send: N` and `Last send error:`
  - a `Next: …` line (5 variants; idle is "Next: send a task message here to start a Codex run, or `helper help` for commands.")
- **retry** (`retryTurnCommand` B:8236, ~45 lines):
  - "usage: `helper retry last`, `helper retry <turn-id>`, or `!retry <turn-id>`"
  - "no failed or interrupted turn is available to retry in this session."
  - "turn not found in this session: id"
  - "turn %s is %s; only failed or interrupted turns can be retried."
  - "retry cannot find the original Teams message for id"
  - "retry failed while reading the original Teams message: …"
  - Graph `GetMessage` re-reads the original message, then a new turn is queued.
- **cancel**: `last|queued|running|all|<turn-id>` are all supported, same handler as item 9 with scope "Work chat".
- **default**: "`default` changes global settings and can only be used in the Control chat. Use `model` or `effort` to change only this Work chat."
- **effort**: same texts as item 11 with "Work"; uses `b.executor`.
- **model** (`handleModelWorkCommand` :125):
  - status / list / setup / doctor
  - `switch|use` ("usage: `model switch <profile>`")
  - reset
  - `fork|new` ("usage: `model fork <profile>`"; creates a new chat; reply "Forked Work chat %s with model profile %s:\n<url>")
  - effort
  - providers: "Provider configuration is Control-only; use `model provider list` or `model provider setup <provider>` in the Control chat."
  - catalog: "Catalog management is Control-only; …"
  - anything else → `modelWorkUsage` (6 bullets)
- **help** (`sessionHelpText` B:5216):
```
💬 Work chat
Send your task as a regular Teams message. Messages starting with `helper` or `!` are helper commands.

Common commands:
`helper status` or `!status` - check progress
`helper stats` or `!stats` - show Codex token usage from the linked local history, grouped by model and service tier
`helper file <relative-path>` or `!file <relative-path>` - upload a file prepared in the helper's Teams upload folder
`helper restore-thread <thread-id>` - restore a missing Codex thread binding before retrying an interrupted turn
`helper close` or `!close` - close this Codex session in Teams
`helper details` or `!details` - show debug IDs and links
`beacon status` - show this Work chat execution target
`beacon switch <profile>` or `beacon switch local` - switch future turns
`model status`, `model switch <model>`, `model reset`, or `model fork <model>` - inspect or change this Work chat model
`effort status`, `effort list`, `effort set <value>`, or `effort reset` - inspect or change this Work chat's reasoning effort
`default ...` - global defaults are Control-chat-only
`helper publish-history` - import a paused local Codex history backlog
`helper publish-history full` - publish the complete local Codex history once to this chat
`helper skills list` / `helper skills add <url>` / `helper skills push` / `helper skills push confirm` - inspect and push skill subscriptions

Send `helper help advanced` for retry, cancel, and rename commands.
```
  `sessionAdvancedHelpText` is at B:5241. Unknown work command: "unknown work chat command: `%s`\n\n<sessionHelpText>" (B:7387).

#### 15. Prefix-free and prefixed forms (dashboard_commands.go)
- **Bare (no prefix)**, `workBareCommandName` :221:
  - `help|menu|?|h` (only with no argument or an advanced argument)
  - `beacon`
  - `model`, and `models` (= list)
  - `effort|reasoning-effort|thinking-effort`, and `efforts` (= list)
  - `default|defaults`
  - Everything else goes to Codex.
- **Prefixes** (`splitDashboardCommand` :603): `/x`, `!x`, `cx x`, `codex x`, `helper|assistant|codex-helper x`. A trailing ":" on the prefix is allowed.
  - `helper`/`codex` use the natural names (`workNaturalCommandName` :528): help, status, stats/usage/tokens, close, park, resume/unpark, retry, restore(-thread), cancel/stop, send-file/send-image/file/image, rename, details/detail, publish-history/sync-history/import-history, skills, beacon, model…, effort…, default…
  - `/`, `!` and `cx` additionally accept the short names (:571): h, st, cl, rt, rs, x, img/f, rn, d, ph/sync. So `!ph` works but `helper ph` doesn't.
- **Unknown names**: with `/`, `codex` or `helper` they are forwarded to Codex. With `!` or `cx` they get the unknown-command help.

#### 16. `helper stats` — `formatWorkSessionStats` token_stats.go:1013 + `formatCodexTokenStatsLines` :1083 (~250 lines of formatting in a 1725-line file)
- Sent as rendered HTML via `sendStatsToChat` B:14725.
- Header: "STATS: Codex tokens", "Session: id", "Codex thread: …|not linked yet", "Latest request: …", "Latest Codex turn: …".
- Then "Source: <src> at transcript line N (<path>)", and a Reliability line: "Reliability: reconstructed conversation usage from %d unique Codex `token_count` update(s) in local history; malformed trailing JSONL lines are ignored and reported below." (or the fallback / no-snapshot variants), plus an optional "Aggregation: …" line.
- Sections: "Last recorded model usage:", "Conversation total:", "Model/tier usage:" (blocks of "Model: …" / "Tier: …" / token lines), "Analysis:", "Rate limits:", "Diagnostics: skipped malformed/non-usage transcript lines (…)."
- Unavailable cases: "Token stats unavailable: …" (no thread / no transcript / read error); "Token usage unavailable: no Codex usage event was found in the linked transcript."
- Reads the local transcript only; no Graph calls.

### Other texts

#### 17. Queued requests
- Delay and repeat are both 10 min (`queuedTurnAttentionDelay` and `…RepeatDelay`, B:100-101).
- `sendQueuedTurnAttentionIfDue` B:9079: "⏳ Still waiting.\n<reason>\n\nYour Teams message is queued and will run after this clears.\nIf this looks stale, send `helper status` here. To drop this queued message, send `helper cancel <turnID>`." plus "\n\n---\n\n<queue snapshot>".
  - Reasons (B:9110): "Local Codex is still active for this chat." / "I am still preparing local history for this chat." / "Local history import is paused until you send `helper publish-history`." / "I am still syncing recent local Codex updates before running your Teams request." / "Local history sync needs attention before I can continue." / "The helper is waiting for a required local sync step."
- Start notice (`formatQueuedTurnStartNotice` B:11702): "▶️ **Codex is starting this queued request.**\n\n**▶️ Now running:**\n<summary>\n\n---\n\n**⏳ Still queued:**\n<list|No other queued requests.>"

#### 18. Long-turn hints (B:92-99, used in `sendIdleStatus` B:12037)
- Idle status starts at 2 min and repeats every 5 min: "Still working. No new Codex update yet."
- At ≥7 min it appends "\n\nThis is taking longer than usual. To stop the running request, send `helper cancel running` in this chat."
- At ≥15 min the message is replaced by "Codex has not produced any update for %s. It may be stuck.\n\nI will not retry automatically. To stop the running request, send `helper cancel running` in this chat." (duration such as "15m").

#### 19. Coworkers and bare @codex
- `rejectExternalWorkCommand` B:8218 @mentions the author: "Only the helper owner can run helper commands in this Work chat. Send a normal question or task here and I will pass it to Codex."
- A non-owner prompt is acked with "⏳ Codex received your question. Request accepted." (the owner gets "⏳ Codex is working. Request accepted.").
- Bare mention (B:2031): "Mention received, but there is no Codex request text. Add the task after `@codex`."

#### 20. Lifecycle notice and title auto-update
**Notice**
- `workChatLifecycleNotice` B:7867 (via `workChatCreatedNotice` B:7863) sends "Work chat created: <id>.\nWorking directory: <cwd>". It is an owner-mention outbox of kind chat-created. Recreated chats use "Work chat recreated" (recreate.go:169).
- Locally detected sessions get "Local Codex chat detected: <id>." instead (B:798).
- New `new` chats get `sessionReadyMessage` B:7841: "💬 Work chat is ready.\nSend a task in this chat. Codex will start automatically and continue this session.\nStatus: waiting for your first task.\nSession: …\nProject: …\nWorking directory: …\nExecution target: …\nCommands: `helper status` or `!status`, `helper stats`, `helper help`, `helper close` to close this Codex session in Teams.\nNeed more details? Send `helper status`."

**Title auto-update**
- `maybeUpdateWorkChatTitleFromLocalSession` B:10749 does Graph `UpdateChatTopic` with `WorkChatTitle{Topic: Codex summary or first prompt}` and sets TitleSource=auto.
- Called from:
  - `refreshWorkChatTitleFromExecutionResult` B:10736, using `result.CodexThreadTitle` after each completed turn (B:11351)
  - otherwise `refreshWorkChatTitleFromCodexHistory` B:10714 when nothing is queued
  - the history sync at B:18051
- Gate: `sessionAllowsAutoTitleUpdate` B:10804 returns false once TitleSource=="user" or UserTitle is set, which is exactly what `helper rename` sets.
- Legacy chats with no source are updated only when the topic is empty, contains "New message in ", or contains "codex work".
