# Managed voice downloads

The helper installs the small runtime and FFmpeg first, validates them, then
installs the audio encoder and main model. One request runs at a time. It
visits every source before retrying a failed source (up to three rounds per
installation attempt). The existing overall setup limit still applies.

Requests have a 15 second response-header deadline, and transfers retain the
existing no-data deadline (60 seconds by default). With multiple sources, a
30 second window below 128 KiB/s yields once to another source. Later rounds
allow slow transfers so a slow but working network can finish. The successful
source host is preferred for the next asset in the same helper process.

Partial files are keyed by the asset name and pinned hash and protected by
the existing asset lock. If locking is unavailable, the attempt uses a
private partial instead. There is no partial metadata rewrite. The existing
24 hour temporary-file retention removes abandoned partials; their size is
bounded by the pinned asset size. A failed read preserves progress. A local
write failure stops without trying other mirrors. A resumed response must
match the requested range and complete size exactly; a server returning 200
restarts the file rather than appending. Short responses retain their prefix
for retry. A checksum failure after resuming discards the partial and allows
a fresh attempt before treating the source as corrupt. Completed files are
read sequentially to verify SHA-256, then synced once before atomic
installation. Hash state is not serialized. This costs one local read per downloaded asset and
keeps buffer memory independent of model size. Warm installed assets do not
download or run this full-file verification again.

Diagnostics show source host, response wait, periodic byte progress, resume
offset, failure category and checksum duration. URLs, signed redirect query
strings, credentials and transcript text are not logged.

## Additional mirrors

`CODEX_HELPER_TEAMS_ASR_MIRRORS` accepts comma-separated HTTPS directory URLs.
Each directory must contain the exact files named by the current manifest,
including the runtime and FFmpeg archives. Additional mirrors follow the
primary URL and precede the built-in model fallback. Pins never change when
adding a mirror. HTTP, credentials and query strings in a base are rejected.
The service installer preserves this setting like the other ASR settings.

Mirror hosting is provisioned separately. Copy the exact published files
without rebuilding them; the client verifies every completed download against
the existing pinned checksum. No hosting repository, extra credential or
cross-repository publishing workflow is added by this change.
Without the mirror setting the default sources remain unchanged: runtime and
FFmpeg on GitHub, and models on GitHub with Hugging Face as fallback.
A second GitHub repository provides another copy, not an independent CDN.
The directory setting also supports mirrors hosted outside GitHub.

## Current chat membership

Teams reads the actual member list at the first chat read, after a new system
event, after a new human sender in an owner-only chat, and every five minutes
while polling. An application message is not
evidence of another human participant and cannot invoke Codex. A one-member
meeting chat is single-user even though Graph calls its chat type `group`.
Successful member checks can clear a historical `engine_group` flag; failed
checks preserve the old state and use ordinary read backoff. Channels that
do not offer membership reads keep conservative human-message inference.
Voice edits wait for the first successful chat read and check the group
state when they run, so a voice resumed at startup cannot be skipped because
of the old flag. This uses the existing bounded edit queue.
