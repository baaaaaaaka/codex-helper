# Proxy recovery and platform validation

The proxy keeps its HTTP listener stable while replacing the SSH/SOCKS backend
by generation. A replacement is admitted only after TCP, SOCKS, and (when
configured) route probes succeed. Request failures are coalesced and require
request plus active-backend probe evidence before recovery starts.

When one proxy dials another HTTP proxy, it forwards bounded internal hop
metadata through `HeaderContextDialer`. A repeated proxy ID such as A->B->A
is rejected immediately with 508; anonymous or otherwise unique chains are
bounded by the default maximum hop count. DNS aliases are resolved only for a
same-port target, with a short timeout, and are compared using the listener's
actual IPv4/IPv6 address family. An unresolved alias is not assumed to be a
self-loop.

When a profile has no explicit route target, CXP performs only the local SOCKS
readiness check. It must not infer a remote probe target from the profile's SSH
Host/Port: those fields can be a local SSH config alias or the SSH endpoint
itself, neither of which is necessarily a valid destination through the
remote SOCKS server.

Health requests use a short TTL and a single in-flight probe. A burst of
clients therefore produces one SOCKS probe, while the health response still
reports the raw backend-failure counter. Recovery admission has a durable
restart/request budget in the instance record; once blocked, a replacement
daemon or supervisor exits without opening another listener or SSH process.

## Lifecycle

Each daemon instance persists a broker ID, broker epoch, owner token, and
owner lease timestamps. Heartbeats use compare-and-swap fencing. A stale owner
can be reclaimed only when its process is dead and its lease has expired;
ports from the stale record are not reused blindly.

`proxy start` is an explicit “create a new instance” command and may create
more than one instance when invoked repeatedly. Desktop/app ensure-and-reuse
paths use the health/route-aware reuse scan and the startup lock; those paths,
not explicit starts, are the one-instance concurrency contract.

For a bounded local crash supervisor, start the proxy with:

```text
codex-proxy proxy start --supervised
```

The supervisor allows three restarts per minute with a five-second backoff;
the count is also persisted in the instance recovery budget, so re-entering
the supervisor cannot reset it. Candidate backends use a distinct SOCKS port
and are switched in only after TCP, SOCKS, and the configured route target
probe succeed. Explicit SSH config aliases and ProxyJump profiles must provide
that route target separately from the SSH endpoint.
Native definitions can be rendered for review or installation by an operator:

```text
codex-proxy proxy supervisor render <instance-id> --platform linux
codex-proxy proxy supervisor render <instance-id> --platform darwin
codex-proxy proxy supervisor render <instance-id> --platform windows
```

Rendering does not install or activate an OS service. This keeps normal proxy
startup unprivileged and prevents a test or upgrade from changing a user's
system service registration unexpectedly.

## Validation boundary

Hosted CI runs the deterministic loopback matrix on Ubuntu, macOS, and
Windows, the Linux Docker SSH/SOCKS fault lab, request/health storm and
short resource-budget tests, and cross-compiles the native process-control
paths. The resource test performs a bounded repeated start/health/failure/
close workload, rebinds every listener immediately, and compares live heap
objects only after the allocator has reached a steady sample window; it does
not use a long wall-clock soak or a noisy RSS threshold.
The scheduled/manual `.github/workflows/proxy-native-platform.yml` workflow
also uses GitHub-hosted macOS and Windows runners: it exercises both hosted
macOS architectures, native `plutil`/PowerShell definition lint, and the
Windows raw-CONNECT path. Installed Edge and WSL2 are availability-gated
optional checks; a hosted Windows image without a usable WSL distribution
reports a warning and skips that boundary instead of waiting for a
self-hosted label. WSL2 `nat` binds a wildcard address and uses the WSL
gateway; `mirrored` uses the production 127.0.0.1 bind, so the two forwarding
assumptions are not conflated.

Hosted CI cannot prove physical lid close, kernel interface reset, or a real
macOS LaunchAgent after sleep. Those remain explicit self-hosted/device-lab
checks rather than being represented by a loopback test. Unix tunnel shutdown
uses a process group; Windows uses a Job Object when the host permits nested
jobs and falls back to `taskkill /T` when a managed runner rejects Job Object
assignment.
