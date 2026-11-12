# Teams message integrity

This change removes the 20-progress limit, group-chat progress suppression,
pressure-based content replacement, and the catch-up filter that kept only
prompts and answers. Progress from new helper executions also survives a
helper restart. Existing Graph pacing and retry-after gates remain in force.

## One durable path

For native work-chat executions, Codex rollout records are the progress source.
Native callbacks wake the existing transcript reader. They do not publish a
second copy. The runner still owns the final answer, its statistics, and files.
Other agents and control-chat executions retain their direct callback path.

The store records this ownership before the execution starts. The known client
key identifies records that arrive before the native Started callback. A final
is persisted in the existing outbox with a turn hold until the reader commits
the terminal record and preceding progress. The completed execution immediately
releases its native slot; transport pressure in one chat cannot consume slots
needed by other chats. Held parts do not count toward source backpressure.
At the terminal's exact source position, the same transaction moves held final
parts after preceding progress and before subsequent source records. Identities,
files, and payloads stay unchanged. Committing the terminal wakes delivery even
when it contains no progress, without waiting for a maintenance tick.
Restart restores the hold without executing
the completed task again. A next native task in the same chat waits behind
substantive output still queued or sending. Unknown outcomes remain available
for reconciliation.
If a terminal is visible before the runner stores its outcome, the reader waits
at that boundary; subsequent local turns cannot pass the unfinished final.
The source cursor and all output parts commit in the same transaction. A failed
read, render, or transaction cannot advance the cursor past missing content.
Restart reconciliation recovers native acceptance before committing a terminal.
An interrupted producer without a terminal record uses settled EOF instead;
this fallback cannot prove that a broken producer will never append later.

The ownership flag defaults to false on old turns. Upgrade does not replay all
old helper executions. Catch-up now retains statuses and compactions in the
unread source range, without producing old completion notifications. Content
that an older helper discarded after advancing its cursor is not automatically
backfilled. A broad historical replay would need a separate, explicit operation.
Capacity retries share the request's stable client key. Ownership retains that
key as well as the latest native id, so unread progress from an earlier attempt
remains identifiable after restart even though the turn's native id was replaced.

## Lossless transport batches

Each source record and rendered part retains its own durable delivery key.
An already represented record must contain every declared part. One part, a
timestamp watermark, or a matching text alone cannot hide a missing part.
Queued, unknown, sent, and explicitly failed records are representations; this
does not mean that each record was successfully delivered.

After obtaining its normal send slot, the sender can combine up to 16 adjacent
whole progress messages of the same execution. The Teams adapter also limits
the combined HTML to its existing 24 KiB target. It preserves every block in
order. Finals, other executions, delayed rows, split messages, replies,
mentions, hosted content, and file records stop a batch.

Batch membership, exact transport body, and send state commit atomically.
Original source bodies remain in the outbox. All members move together on
success, definite rejection, ambiguity, or restart. A retry uses the frozen
body. Later arrivals never change an attempt whose outcome is unknown.
Echo detection uses the actual combined transport hash, not hashes of member
messages that were never sent individually.

There is no merge timer, second durable queue, or independent send budget.
Within a chat, queued and sending content keeps its order. Unknown outcomes
retain their complete content and attempt identity without blocking later
sends or tasks. A delayed remote acceptance may become visible out of order;
the helper never replays an unknown POST to repair that uncertainty. Other
chats keep their own send slots. After eight foreground slots, an aged catch-up
head can obtain a paced slot, so continuous live traffic does not starve catch-up.

## Uncertain external delivery

A connection failure after POST cannot establish whether Teams accepted it.
Absence from a later listing is also not proof of rejection. Substantive
content therefore stays unknown, with one durable control warning per
uncertain transport and low-rate read-back. Unknown rows are excluded from
active delivery backpressure, other notifications' waits and automatic parking waits.
The content is neither blindly resent nor silently discarded. A later matching
marker or transport hash can settle the whole batch. An "answer ready" notice
waits only for its own final to be confirmed; later notifications can continue.
An explicit rejection received during shutdown still returns the attempt to
the retry queue; cancellation alone cannot turn a known 429 into an unknown send.

A permanent Graph rejection records an explicit failure, retains the content,
and warns in control. Subsequent messages can continue in that case. These
rules prevent silent local deletion; they cannot guarantee that Teams will
accept a permanently rejected message. Without an external idempotency key,
the helper cannot guarantee both automatic eventual delivery and zero
duplicates after every ambiguous network failure.

## Visibility and resource bounds

The owner's initial prompt confirmation is appended below the original Teams
message, after a horizontal rule. This includes acceptance, additional input,
queueing, duplicate suppression, held requests, restart or upgrade deferral,
and Control-chat questions. Later notices, command replies, Codex output, and
other members' confirmations keep their separate messages.

The confirmation stays in the existing outbox. Its optional `append_to` target
selects an idempotent edit, not a second delivery queue. The edit preserves
attachments and mentions. Unsupported content, messages that cannot be edited,
confirmations that do not fit, and unavailable edit-read budgets use the
separate-message path so read throttles do not hold later answers. That fallback
is stored before POST. An uncertain PATCH is retried with the same confirmation
key; an uncertain POST still uses the normal read-back rules. A successful edit
does not register the user's message as a helper post. Voice transcript edits
preserve the confirmation, and confirmation edits preserve the media card.
The transcript stays above the confirmation's horizontal rule even when ASR
finishes after the receipt is sent. User-authored rules and quoted receipts
remain in their original positions.

Schema version 7 adds one nullable column without rewriting history. Existing
outbox rows have no target and retain their original delivery method. A build
that supports only an older schema cannot open the upgraded store.

Still-working notices use the time since the last Codex event. Defaults are
two minutes for the first notice and five minutes between repeats. Notices
include cancel guidance from seven minutes of silence. From the 15-minute
stuck threshold, the next scheduled notice uses the stuck wording. With the
default cadence, notices are due at 2, 7, 12, and 17 minutes, so the first
stuck wording normally appears at about 17 minutes. An open native question
or pending content/notice prevents periodic notices from accumulating behind
the backlog.

While a chat has an unanswered native question, one first-line
`❓Pending agent question` marker sits on its latest sent Codex final,
progress message, agent-labeled local mirror, or question message. A new agent
message moves the marker; question status edits restore it after replacing the
message body. Answering or closing the last pending question removes it. The
store keeps only that message's external id per chat, and startup reconciles
pending questions and saved ids without reading chat history. The delivered
question also marks its work chat unread once; this uses the existing
notification/edit queue. Local user-prompt mirrors, incoming user or coworker
messages, and ordinary helper notices are not marked.

The reader processes at most 256 source records and its existing 8 MiB byte
budget per file per pass. Its existing line-size rules still apply. At 256
pending rows for a chat it leaves further source data on disk until sending
makes room. This is a high-water mark, not an absolute row cap: one bounded
pass can render multiple parts per source record. Files that did not change
are not reread. Complete records can skip repeat rendering on recovery.

Requests rejected before native acceptance leave no client-map entry. Accepted
requests retain their stable client key and native-id ownership identifiers,
not message payloads, like the existing helper-turn index. Persistent content
grows with actual content, as the outbox
already did. Frozen batches add one bounded body per batch while retaining
the originals. The schema upgrade adds constant-default columns and a partial
batch index; it scans outbox metadata once and does not rewrite old bodies.
An isolated startup ownership audit of 100,000 accepted requests loaded both
identifier maps in about 100 ms and retained about 14.2 MB of heap. The indexes
grow with request count, rather than with the size of message bodies.

The final local resource audit covers an 814 MB store with 1.18 million inbound
rows and 450,000 outbox rows; hot queries retain indexed plans. Migrating a
28.4 MB old outbox took about 107 ms and added one 4 KiB database page. A
1,000-record ID-less transcript benchmark adds about 5.6% parse time and 7.8%
allocation bytes compared with the base revision, for stable fallback keys and
legacy aliases. Large tool-output head classification remains about 0.5 µs.
These are measurements on this machine, not transport throughput guarantees.

## Regression coverage

- Store: atomic batch rollback, immutable retry, whole-batch settlement,
  missing split parts, legacy aliases, echo identity, bounded batch selection,
  migration defaults and disk growth, indexed hot queries, and read-only active
  scheduling with 100,000 retained unknown rows.
- Transcript: selective ownership, client identity before native callbacks,
  unphased look-behind, and fallback identities through unrelated line shifts.
- Engine: 120 progress records under throttle and group mode, restart without
  duplicates, pre-callback rollout writes, pre-acceptance restart, source binding
  races, uncertain sends with late read-back, local-state retries, and visible
  silence-clock reset while Codex emits activity. A 1,000-record source tests
  bounded backpressure during definite throttling and continued publication
  during uncertain sends, with retained source records across restart.
  Held-final restart, final ordering, and execution in another chat with only
  one native slot remain covered. Rejection before acceptance releases transient ownership.
  Consecutive native turns publish finals with maintenance disabled, and a
  definite rejection during shutdown remains retryable.
  Restart replay also preserves progress from an earlier capacity attempt.
- Question marker: latest-message movement, question status edits, one unread
  mark, cleanup after resolution, and marker repair after restart.
- Teams adapter: idempotent prefix add/remove, author check, and preservation
  of message body, attachments, and mentions.
- Process e2e: 120 records with 429 and restart, accepted merged POST followed
  by process crash, historical commentary/compaction through two restarts, and
  an unconfirmed final followed by status and a new answer in the same chat.
  Further cases exercise cancellation behind unknown progress, a queued task
  during read-back failure, and a missing merged batch across restart.

The CI scenario job discovers whole scenario packages with
`scripts/ci/e2e_shards.py`; these new scenarios are in existing packages and
are included without a new workflow selector. Golden transcripts normalize
only complete transport progress blocks into logical messages. Repeated
blocks remain repeated, and raw POST counts, ordering, throttle, and restart
invariants are checked separately. Unit tests run in the existing unit and
race jobs.

The active-outbox index in schema v5 keeps scheduling independent of retained
unknown history. The migration adds only an index; it does not rewrite message
bodies or add delivery states. Schema-v4-only binaries cannot open a v5 store.
Task completion means execution finished, not that every message was delivered.
Work-chat status reports unconfirmed output separately when it exists.
