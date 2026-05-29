#!/usr/bin/env bash
set -euo pipefail

helper="${1:-}"
mode="${2:-direct}"
if [ -z "$helper" ]; then
  echo "usage: $0 /path/to/codex-proxy-or-cxp [direct|proxy]" >&2
  exit 2
fi
if [ ! -x "$helper" ]; then
  echo "helper is not executable: $helper" >&2
  exit 2
fi
if [ "$(uname -s)" != "Darwin" ]; then
  echo "Codex desktop app network install smoke is macOS-only for this script" >&2
  exit 2
fi
if [ "$mode" != "direct" ] && [ "$mode" != "proxy" ]; then
  echo "unsupported smoke mode: $mode" >&2
  exit 2
fi

base="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/codex-desktop-network-install-smoke"
rm -rf "$base"
mkdir -p "$base/work" "$base/home" "$base/cache"

out="$base/app-launch.out"
config="$base/config.json"
home="$base/home"
app="$home/Applications/Codex.app"
exe="$app/Contents/MacOS/Codex"
existing_pids="$base/existing-codex-pids"
proxy_log="$base/proxy.log"
pgrep -f "$exe" >"$existing_pids" 2>/dev/null || true
proxy_pid=""

cleanup() {
  if [ -n "$proxy_pid" ]; then
    kill "$proxy_pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ "$mode" = "proxy" ]; then
  proxy_script="$base/connect_proxy.py"
  proxy_port_file="$base/proxy.port"
  cat >"$proxy_script" <<'PY'
import http.server
import select
import socket
import socketserver
import sys

instance_id, port_file, log_path = sys.argv[1:4]

class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        if self.path == "/_codex_proxy/health":
            body = ('{"ok":true,"instanceId":"%s"}\n' % instance_id).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def do_CONNECT(self):
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(self.path + "\n")
        host, sep, port = self.path.rpartition(":")
        if not sep:
            self.send_error(400, "missing CONNECT port")
            return
        try:
            upstream = socket.create_connection((host, int(port)), timeout=30)
        except OSError as exc:
            self.send_error(502, "connect upstream: %s" % exc)
            return
        self.send_response(200, "Connection Established")
        self.end_headers()
        sockets = [self.connection, upstream]
        try:
            while True:
                readable, _, _ = select.select(sockets, [], [], 30)
                if not readable:
                    return
                for sock in readable:
                    data = sock.recv(65536)
                    if not data:
                        return
                    (upstream if sock is self.connection else self.connection).sendall(data)
        finally:
            upstream.close()

with Server(("127.0.0.1", 0), Handler) as server:
    with open(port_file, "w", encoding="utf-8") as fh:
        fh.write(str(server.server_address[1]))
    server.serve_forever()
PY
  instance_id="codex-desktop-smoke-proxy-instance"
  profile_id="codex-desktop-smoke-proxy-profile"
  python3 "$proxy_script" "$instance_id" "$proxy_port_file" "$proxy_log" &
  proxy_pid="$!"
  for _ in $(seq 1 50); do
    [ -s "$proxy_port_file" ] && break
    sleep 0.1
  done
  if [ ! -s "$proxy_port_file" ]; then
    echo "local proxy did not start" >&2
    exit 1
  fi
  proxy_port="$(cat "$proxy_port_file")"
  now="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  cat >"$config" <<JSON
{"version":1,"proxyEnabled":true,"profiles":[{"id":"$profile_id","name":"ci-proxy","host":"127.0.0.1","port":22,"user":"ci","createdAt":"$now"}],"instances":[{"id":"$instance_id","profileId":"$profile_id","kind":"daemon","httpPort":$proxy_port,"socksPort":0,"daemonPid":$proxy_pid,"startedAt":"$now","lastSeenAt":"$now"}]}
JSON
fi

run_app() {
  env \
    HOME="$home" \
    XDG_CACHE_HOME="$base/cache" \
    PATH="/usr/bin:/bin:/usr/sbin:/sbin:$PATH" \
    "$helper" --config "$config" app --cwd "$base/work"
}

if [ "$mode" = "direct" ]; then
  if ! printf 'n\n' | run_app >"$out" 2>&1; then
    echo "cxp app failed during Codex desktop app network install smoke" >&2
    sed -n '1,200p' "$out" >&2 || true
    exit 1
  fi
elif ! run_app >"$out" 2>&1; then
  echo "cxp app failed during Codex desktop app network install smoke" >&2
  sed -n '1,200p' "$out" >&2 || true
  exit 1
fi

if [ ! -d "$app" ]; then
  echo "cxp app did not install Codex.app at $app" >&2
  sed -n '1,160p' "$out" >&2 || true
  exit 1
fi
if [ ! -x "$exe" ]; then
  echo "Codex.app executable is missing or not executable: $exe" >&2
  exit 1
fi
if [ "$mode" = "proxy" ] && ! grep -q 'persistent.oaistatic.com:443' "$proxy_log"; then
  echo "Codex desktop app DMG download did not use the configured proxy" >&2
  sed -n '1,160p' "$out" >&2 || true
  sed -n '1,160p' "$proxy_log" >&2 || true
  exit 1
fi

for _ in $(seq 1 90); do
  launched_pids=()
  for pid in $(pgrep -f "$exe" 2>/dev/null || true); do
    if ! grep -Fxq "$pid" "$existing_pids"; then
      launched_pids+=("$pid")
    fi
  done
  if [ "${#launched_pids[@]}" -gt 0 ]; then
    if [ "$mode" = "proxy" ]; then
      found_proxy_arg=0
      for pid in "${launched_pids[@]}"; do
        cmdline="$(ps -ww -p "$pid" -o command= 2>/dev/null || true)"
        if grep -Fq -- "--proxy-server=http://127.0.0.1:$proxy_port" <<<"$cmdline"; then
          found_proxy_arg=1
          break
        fi
      done
      if [ "$found_proxy_arg" != "1" ]; then
        echo "Codex desktop app process did not receive the configured Chromium proxy argument" >&2
        for pid in "${launched_pids[@]}"; do
          ps -ww -p "$pid" -o pid= -o command= >&2 || true
        done
        kill "${launched_pids[@]}" >/dev/null 2>&1 || true
        exit 1
      fi
    fi
    echo "Codex desktop app network install smoke passed: $app"
    kill "${launched_pids[@]}" >/dev/null 2>&1 || true
    exit 0
  fi
  sleep 1
done

echo "Codex desktop app was installed but no launched Codex process was observed" >&2
sed -n '1,160p' "$out" >&2 || true
exit 1
