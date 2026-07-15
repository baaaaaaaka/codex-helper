# Third-party model open issues

This is the working ledger for the DeepSeek/MiMo external-catalog experiment.
It deliberately contains no API keys or other provider credentials.

- Scope: `explore/deepseek-mimo-external-config`
- Recorded: 2026-07-14
- Status vocabulary: `open` means an implementation or validation task remains;
  `blocked` means the next live step needs external state such as a fresh key;
  `intentional` is documented behavior, not a defect.

## Open implementation and design issues

### OPEN-01 — Native MiMo `web_search` is dropped by the CXP adapter

Status: **resolved in catalog-v2 adapter path; live provider smoke remains opt-in**

MiMo's OpenAI-compatible API accepts a native tool with `type: "web_search"`.
The old generic bridge dropped that tool and could return a convincing but
ungrounded answer. Catalog v2 now declares `nativeTool.inputTypes`, the
compiled upstream type, and an allow-list of provider fields. The Responses
facade forwards the typed native payload through Chat Completions (including
tools nested in a namespace) and fails closed when a route declares a native
tool but receives an unknown shape. Unit, facade, and Docker loopback tests
cover this route. A credential-free CI run still cannot prove provider billing
or actual search invocation; that remains an opt-in operator smoke test.

### OPEN-02 — DeepSeek native search is only available on its Anthropic path

Status: **resolved for JSON-selected wire conversion and source annotations**

The DeepSeek OpenAI-compatible endpoint rejects the native `web_search` tool,
while the Anthropic-compatible endpoint can return server-side search results.
Catalog v2 supports named interfaces and selects the Anthropic interface when a
native `webSearch` feature points there, including its independent credential
and auth header. `deepseek-anthropic-v1` converts typed Responses requests to
Messages requests and maps buffered/SSE text, reasoning, tool-use, usage, and
server-search URL citations. The converter is selected only by the registered
`conversion.profile`; arbitrary templates are rejected. Provider-specific
search ranking or citation quality remains a provider concern, not a wire
conversion gap.

### OPEN-03 — Search metadata and fallback settings are not fully wired

Status: **resolved for JSON routing and fail-closed validation**

Catalog v2 `features.webSearch` is authoritative for route, support state,
fallback selector, effort, tier, source mode, and required-URL policy. Native
routes select the declared interface; plugin routes generate the configured
fallback role; unsupported routes fail before a provider request. Source
metadata is either returned as URL annotations/text according to JSON or
rejected when the policy says unsupported/required URL. The remaining quality
question is whether the provider's citations are useful, not whether CXP
silently ignored the declaration.

### OPEN-04 — JSON mode is not exposed through the CXP Responses contract

Status: **resolved in the request contract; provider rejection remains explicit**

`ResponsesRequest` and the OpenAI-compatible chat adapter carry and forward
`response_format`. Catalog capability/policy fields remain declarative; a
provider that rejects a particular JSON schema returns its upstream error
instead of being reported as successful. Wire fixtures cover the forwarding
path.

### OPEN-05 — Real MiMo search coverage is absent from credential-free CI

Status: **open / coverage gap**

Repository CI correctly contains no real external API token. That means native
MiMo search, provider billing, and plugin propagation are not exercised in CI.
Keep credential-free contract tests in CI and add an opt-in, external-environment
smoke job or documented operator test that receives credentials out of band.

### OPEN-06 — MiMo V2.5 search quality is inconsistent across models

Status: **open / quality concern**

The direct probes consistently report real search usage for both models, but
the answer quality is not stable: one `mimo-v2.5` run cited unrelated research
and a U.S. Embassy page instead of weather information, while later runs (and
`mimo-v2.5-pro`) returned weather with a source URL. Search invocation works,
but grounding quality still needs a small multi-query matrix before declaring
both models production-ready.

### OPEN-07 — The provider API surface is wider than the current CXP/catalog protocol

Status: **substantially resolved in JSON; provider-specific limits remain explicit**

The live matrix found working features that the current CXP contract cannot
express as one provider/model route: DeepSeek Anthropic-native search result
items, MiMo Chat audio/video, and MiMo's native Responses-only request shapes.
The external catalog now has a typed conversion switch and the Responses
facade exposes explicit `operation`, `prefix`, and `suffix` fields for Beta
prefix/FIM requests. `deepseek-beta-v1` converts FIM/prefix completions and
reuses the existing chat adapter for normal chat.

Catalog v2 models named interfaces, typed feature routes, native tools, source
policies, audio/video message policies, and explicit Responses field support.
The Chat adapter forwards image/audio/video parts when the model policy allows;
the Anthropic converter rejects unsupported audio/video rather than silently
dropping them. MiMo's Responses-only differences are represented as explicit
`unsupported` fields. Provider features not covered by a registered converter
still need a new compiled adapter; JSON cannot inject arbitrary wire templates.

## Live validation

## Implemented catalog-v2 contract

The current worktree implements the JSON-level portion of the design:

- `catalogVersion: 2` uses `providers -> interfaces` and `providers -> models`;
- providers may declare a stable `defaultInterface`; interfaces select a
  registered adapter, URL, and credential-header shape;
- models use typed `features` with `native`, `translated`, `plugin`, or
  `unsupported`
  support and may route each feature to a different interface;
- native tools declare accepted Responses input types, upstream wire type, and
  an allow-listed set of provider fields;
- source handling declares `annotations`, `text`, or `unsupported` mode and can
  require URL citations;
- model `responses` policy explicitly marks `previousResponseId`, `background`,
  and `contextManagement` as native, translated, plugin, or unsupported;
- audio/video message policies are forwarded by the Chat adapter when allowed;
  unsupported Anthropic media is rejected explicitly;
- search fallback selector, effort, tier, and source requirements are carried
  into the generated fallback role configuration;
- catalog routes remain selected as `provider/model`, while credentials stay in
  local provider bindings;
- a catalog-declared native tool is forwarded only through a compiled mapping;
  unknown tools fail closed instead of being silently removed;
- `response_format` is forwarded through the CXP Responses facade and chat
  adapter.

The implementation intentionally keeps conversion profiles compiled and
versioned. JSON selects a registered converter but cannot inject templates or
scripts. Anthropic Messages and DeepSeek Beta/FIM are registered profiles;
provider-specific citation quality and unsupported media remain provider or
converter boundaries rather than silently guessed behavior.

### LIVE-01 — Native MiMo Web Search works through the official API

Status: **verified for direct provider calls**

In Docker on 2026-07-14, both `mimo-v2.5` and `mimo-v2.5-pro` returned HTTP 200
with `usage.web_search_usage` present (`tool_usage=2`, `page_usage=4`). The
earlier 401 result was a false negative: the shell variable was not exported
into Docker. This proves the plugin is active for the tested credential and
models; the credential-free CXP forwarding path is now covered by loopback
contract tests, while real provider billing remains an opt-in smoke test.

After changing the provider switch, the official FAQ still recommends allowing
up to five minutes for the switch cache to expire.

## Confirmed working or intentional behavior (not open issues)

- MiMo v2.5 text, reasoning, streaming, context continuation, function and
  parallel tools, strict tools, JSON mode, vision, audio URL, video URL, native
  web search, and prompt-cache accounting all passed. MiMo v2.5-pro passed the
  text/reasoning/tool/JSON/search paths, but its image path returned HTTP 404;
  this is a model-level capability difference, not evidence that v2.5 image
  support is broken.
- MiMo's direct Responses API passed default, streaming, function, structured
  JSON, and v2.5 image input. Its Responses `web_search` tool was rejected by
  the gateway; Chat Completions is the supported native-search path tested.
- DeepSeek v4-flash and v4-pro passed text, streaming, reasoning, JSON mode,
  logprobs, required/parallel/strict tools, multi-turn tool results, and cache
  accounting. OpenAI image and native web-search inputs were rejected as
  unsupported.
- DeepSeek's Anthropic endpoint passed basic, reasoning, streaming, function
  calling (with thinking disabled), and native web search. Forced tool choice
  while thinking was enabled was rejected; this is an API constraint that the
  adapter must surface.
- DeepSeek v4-pro beta chat-prefix completion and FIM completion both passed.
- MiMo authentication is not currently an upstream blocker: the official FAQ
  documents both `api-key` and `Authorization: Bearer`. Docker probes returned
  HTTP 200 with web-search usage for each header form. The catalog should still
  make the chosen auth form explicit when a provider policy requires it.
- No real provider credential is stored in this worktree or in repository CI.

## Evidence from the latest Docker run

The matrix below is from a credentialed Docker run on 2026-07-14. `pass` means
the provider returned a successful response with the expected structural
field; `reject` means the provider explicitly rejected the feature; `ignored`
means HTTP 200 but the requested field was not returned.

| Capability | MiMo v2.5 | MiMo v2.5-pro | DeepSeek v4-flash | DeepSeek v4-pro |
| --- | --- | --- | --- | --- |
| Basic / streaming / reasoning | pass | pass | pass | pass |
| JSON mode | pass | pass | pass | pass |
| Required function call | pass | pass | pass | pass |
| Parallel function calls | pass (2) | accepted, 0 calls | pass (2) | pass (2) |
| Strict function schema | pass | pass | pass (beta) | pass (beta) |
| Multi-turn tool result | pass | pass | pass | pass |
| Prompt-cache usage fields | present | present | present | present |
| Logprobs | ignored | ignored | pass | pass |
| Image input | pass | HTTP 404 | rejected (400) | rejected (400) |
| Audio URL / video URL | pass / pass | not tested | not supported | not supported |
| Native Chat web search | pass | pass | rejected (400) | rejected (400) |

Additional protocol checks:

- MiMo Responses API: default, streaming, function, JSON, and v2.5 image all
  passed; pro image returned HTTP 404; Responses-native `web_search` returned
  HTTP 400.
- DeepSeek beta: v4-pro chat-prefix and FIM both returned HTTP 200.
- DeepSeek Anthropic: basic, reasoning, streaming, function (thinking
  disabled), and native web search all passed. The search response contained
  both `server_tool_use` and `web_search_tool_result` blocks.

Not covered by this live matrix: one-million-token upper-bound stress, rate
limit/concurrency behavior, cancellation/retry semantics, MiMo ASR/TTS model
endpoints, and MiMo Responses `previous_response_id`/background semantics.
The MiMo Responses documentation currently marks `previous_response_id`,
`background`, and `context_management` as incompatible, so those should be
represented as explicit unsupported capabilities rather than assumed to work.

- Direct `mimo-v2.5` request with official `api-key` header: **HTTP 200**;
  `web_search_usage.tool_usage=2`, `page_usage=4`.
- Direct `mimo-v2.5-pro` request with official `api-key` header: **HTTP 200**;
  `web_search_usage.tool_usage=2`, `page_usage=4`.
- Direct `mimo-v2.5` request with `Authorization: Bearer`: **HTTP 200**;
  `web_search_usage` was present. This rules out a MiMo Bearer-auth defect.
- Historical CXP facade request carrying the native `web_search` tool: **HTTP
  200**, no `function_call` output; this was the pre-catalog drop bug recorded
  above. The regression is now covered by native-tool forwarding and
  fail-closed tests.
- Adapter, facade, catalog-route, and source-policy tests: **PASS**.
- Focused Docker regression run for `internal/responsesadapter`,
  `internal/modelcatalog`, and `internal/modelprofile`: **PASS**.

These results are intentionally recorded separately: the provider-native API
works, while the old CXP drop was an adapter bug independent of key state. The
Docker output contained no credential value.
