# Teams notifications and optional chat titles

## Behavior

Workflow cards use event headings from the earlier Teams implementation:

| Event | Heading | Action |
| --- | --- | --- |
| Teams creates a work chat | 💬 Codex chat ready | Open chat |
| A local session is discovered | 💬 New local Codex chat detected | Open chat |
| An answer completes | ✅ Codex finished | Open answer |
| A request fails | ⚠️ Codex needs attention | Open chat |
| A helper update completes | ✅ Helper update completed | Open Control |
| Update activation fails | ⚠️ Helper update activation failed | Open Control |
| A helper restart completes | ✅ Helper restart completed | Open Control |

The chat name and request summary are separate fields. Question headings keep
their existing meaning. Cards contain a short action hint, not the answer body.
Ready cards use the existing durable notification record and wait for the work
chat's ready message. Uncertain webhook delivery falls back to a control-chat
mention without retrying the webhook. Lifecycle confirmations remain sufficient
when no webhook is enabled.

Answer files use `🤖 ✅ Codex answer:`. Upload diagnostics retain their existing
labels. Ready messages show the saved model and effort. Unresolved defaults say
`Codex account default` and `model default`; creation does not query Codex.

## Title request

Only the first accepted task on a newly bound ordinary Teams work thread can
start a naming request. Existing, resumed, imported, forked, control, inbox and
delegated threads do not generate titles. Existing Codex names and explicit
`helper rename` titles take precedence.

One optional request can run per agent. It retains the existing app-server
lease and resolved model and effort. A full slot, busy rename lock, error or
restart skips naming. There is no queue, retry, recovery record or new timer.
Normal answers, files and completion cards do not wait for model generation.

The first unwrapped task is limited to 960 UTF-8 bytes. The ephemeral thread
uses read-only permissions, no approval interaction, no tools, no MCP servers,
no plugins and a small JSON schema. Results must be at most 8 KiB and contain a
nonempty single-line title of at most 36 Unicode characters. Generation has a
30-second deadline. Interrupt and unsubscribe each have separate 2-second
cleanup deadlines. Canonical name checks and saving use short deadlines.

The existing initialize response gates this feature to Codex 0.166 or later.
Older or unknown versions skip naming before any auxiliary RPC. Native tests
showed that 0.131 silently ignores relevant tool controls and does not reliably
cancel the hidden request. Normal tasks remain supported on that version.

Before saving, CXP checks the canonical name again. Saving runs under the
existing rename lock; the model request does not hold that lock. The generated
name is sent directly to the existing paced Teams title editor. Equal Teams
titles cause no PATCH. Manual rename cancels generation and remains protected
when a late result arrives.

Codex has no atomic name-if-empty operation across independent clients. An
external TUI can still race between read and set. No cross-process lock is
added. Title-only changes from external clients do not gain a new poller.
Unsubscribe detaches the client; it does not promise immediate eviction from
Codex's in-memory thread manager. The existing server idle-reaping policy
eventually releases that memory. Ephemeral requests create no saved rollout.

## Cost and persistence audit

The idle and existing-thread paths start no title work. A new-thread eligibility
check reads existing manual-title and fork metadata. Model forks retain the
existing fork-child marker so their empty threads cannot start naming. There is at most one retained title
input, one goroutine and one auxiliary lease, all released after completion or
cancellation. Input and response bounds apply before prompt construction or
result retention. No full-history read or new index scan is introduced.

Ready and service notifications reuse the existing transaction, pending JSON
record and outbox. Registration wakes the existing notification loop directly,
including when its cached pending count was zero. There is no additional polling
or state file. A successful new title adds at most one canonical Codex name
write and uses the existing Teams title persistence path.

On a Ryzen 9 7950X, `BenchmarkTitlePromptBounded` with a 3 MiB input measured
1390 ns/op, 2323 B/op and 3 allocations/op. This is a new bounded operation,
not an overall service latency claim. The held-generation E2E verifies the
important latency contract: answers, uploaded files and completion cards arrive
before naming finishes.

## Regression and CI coverage

| Area | Tests | CI owner |
| --- | --- | --- |
| Card fields, bounds, single delivery | `TestOwnerNoticeCard`, engine notice event table | Unit jobs |
| Ready ordering, defaults, ambiguous fallback, restart dedupe | Engine notification tests; notify scenarios | Unit jobs; E2E shards |
| File label, readable uploads, held naming | `TestR37_AgentFilesReachChat` | E2E shards |
| New title persistence and subsequent tasks | `TestGeneratedTitle` | E2E shards |
| Manual rename and cancelled/restarted naming | Held and restart title scenarios | E2E shards |
| Invalid output, isolation, cleanup, existing names | Codex title unit tests | Unit jobs; race-core |
| One slot, lease lifetime, bounded close | `TestTitleSingleSlotAndShutdown` | Unit jobs; race-core |
| Native schema, zero tools, canonical saving, cancellation | `TestRealCodexTitle*` with synthetic Responses | Shared Codex contract; nightly runtime matrix |
| Older-version safe fallback | Native 0.131 runtime title contract | Nightly Linux/macOS/Windows matrix |
| Existing unpark and park message behavior | Parked chat and edit/freeze scenarios | E2E shards; release nightly |

The shared contract script explicitly selects the title tests and repeats them
three times in an offline Docker container. Nightly uses official old/current
binaries with isolated homes and synthetic responses. Release keeps the existing
CI plus nightly gates. No live Teams or account credentials are needed for
these tests. Real source-process tests run only in isolated Docker containers.

Scheduled tasks, a generic background job framework, continuous name watching
and changes to park/unpark behavior are outside this change.
