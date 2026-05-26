#!/usr/bin/env bash
set -euo pipefail

root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/codex-helper-teams-supervisor-smoke-$$"
mkdir -p "$root"

log() {
  printf '[teams-supervisor-smoke] %s\n' "$*"
}

cleanup_pids=()
cleanup_launchd_label=""
cleanup_systemd_unit=""

remove_cleanup_pid() {
  local remove="$1"
  local next=()
  local pid
  for pid in "${cleanup_pids[@]:-}"; do
    if [ "$pid" != "$remove" ]; then
      next+=("$pid")
    fi
  done
  cleanup_pids=("${next[@]}")
}

cleanup() {
  for pid in "${cleanup_pids[@]:-}"; do
    [ -n "$pid" ] || continue
    kill "$pid" >/dev/null 2>&1 || true
  done
  if [ -n "$cleanup_launchd_label" ] && [ "$(uname -s)" = "Darwin" ]; then
    launchctl bootout "gui/$(id -u)/$cleanup_launchd_label" >/dev/null 2>&1 || true
  fi
  if [ -n "$cleanup_systemd_unit" ] && command -v systemctl >/dev/null 2>&1; then
    systemctl --user stop "$cleanup_systemd_unit" >/dev/null 2>&1 || true
    systemctl --user disable "$cleanup_systemd_unit" >/dev/null 2>&1 || true
    rm -f "$HOME/.config/systemd/user/$cleanup_systemd_unit"
    systemctl --user daemon-reload >/dev/null 2>&1 || true
  fi
  rm -rf "$root"
}
trap cleanup EXIT

wait_for_lines() {
  local file="$1"
  local min_lines="$2"
  local deadline=$((SECONDS + 30))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ -f "$file" ]; then
      local lines
      lines="$(wc -l <"$file" | tr -d ' ')"
      if [ "${lines:-0}" -ge "$min_lines" ]; then
        return 0
      fi
    fi
    sleep 1
  done
  log "timed out waiting for $file to reach $min_lines lines"
  [ -f "$file" ] && sed 's/^/[heartbeat] /' "$file" || true
  return 1
}

wait_for_file_pattern() {
  local file="$1"
  local pattern="$2"
  local deadline=$((SECONDS + 30))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ -f "$file" ] && grep -q "$pattern" "$file"; then
      return 0
    fi
    sleep 1
  done
  log "timed out waiting for $file to match $pattern"
  [ -f "$file" ] && sed 's/^/[status] /' "$file" || true
  return 1
}

wait_for_pid_gone() {
  local pid="$1"
  local i
  [ -n "$pid" ] || return 0
  for i in $(seq 1 50); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

wait_for_process_gone() {
  local pid="$1"
  local deadline=$((SECONDS + 30))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  log "timed out waiting for pid $pid to exit"
  ps -p "$pid" -o pid,ppid,pgid,sid,stat,cmd || true
  return 1
}

write_token_cache() {
  local path="$1"
  local expires
  expires="$(($(date +%s) + 3600))"
  mkdir -p "$(dirname "$path")"
  cat >"$path" <<EOF
{"access_token":"ci-token","refresh_token":"ci-refresh","expires_at":$expires,"token_type":"Bearer"}
EOF
  chmod 600 "$path"
}

replace_json_executable() {
  local path="$1"
  local executable="$2"
  local escaped
  escaped="$(printf '%s' "$executable" | sed 's/[\/&]/\\&/g')"
  sed -i.bak "0,/\"Executable\": \"[^\"]*\"/s//\"Executable\": \"$escaped\"/" "$path"
}

write_probe_loop() {
  local path="$1"
  cat >"$path" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
heartbeat="$1"
while :; do
  printf '%s pid=%s\n' "$(date +%s)" "$$" >>"$heartbeat"
  sleep 1
done
EOS
  chmod +x "$path"
}

local_supervisor_smoke() {
  [ "$(uname -s)" = "Linux" ] || return 0
  case "${CODEX_HELPER_TEAMS_CHILD:-}" in
    1|true|TRUE|yes|YES)
      if [ "${CODEX_HELPER_CI_ALLOW_DETACHED_SMOKE:-}" != "1" ]; then
        log "::warning::skipping detached local-supervisor smoke inside a Teams-launched Codex turn"
        return 0
      fi
      ;;
  esac
  log "linux local-supervisor smoke: setsid, flock, crash restart, status, and PGID stop"
  local cxp="${CODEX_HELPER_CI_CXP:-}"
  if [ -z "$cxp" ]; then
    log "::warning::CODEX_HELPER_CI_CXP is not set; skipping real local-supervisor binary smoke"
    return 0
  fi
  if [ ! -x "$cxp" ]; then
    log "CODEX_HELPER_CI_CXP is not executable: $cxp"
    return 1
  fi
  for required in setsid ps sed; do
    if ! command -v "$required" >/dev/null 2>&1; then
      log "::warning::missing required command for detached local-supervisor smoke: $required; skipping shell-level detach probe"
      return 0
    fi
  done

  local smoke="$root/local-supervisor"
  local home="$smoke/home"
  local config_home="$smoke/config"
  local cache_home="$smoke/cache"
  local runtime_dir="$smoke/runtime"
  local other_runtime_dir="$smoke/other-runtime"
  local runs="$smoke/child-runs.log"
  local config="$smoke/local-supervisor.json"
  local fake="$smoke/fake-cxp"
  local supervisor_log="$smoke/supervisor.log"
  local duplicate_log="$smoke/duplicate.log"
  local status="$config_home/codex-helper/teams/run/local-supervisor/local-supervisor-status.json"
  mkdir -p "$home" "$config_home" "$cache_home" "$runtime_dir" "$other_runtime_dir"

  cat >"$fake" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
if [ "${1:-}" = "teams" ] && [ "${2:-}" = "run" ]; then
  printf '%s child pid=%s args=%s\n' "$(date +%s)" "$$" "$*" >>"${CXP_LOCAL_SUPERVISOR_PROBE_RUNS:?}"
  if [ "${CXP_LOCAL_SUPERVISOR_PROBE_MODE:-}" = "crash" ]; then
    exit 7
  fi
  while :; do
    sleep 1
  done
fi
printf 'unexpected fake-cxp args: %s\n' "$*" >&2
exit 64
EOS
  chmod +x "$fake"

  cat >"$config" <<EOF
{
  "version": 1,
  "enabled": true,
  "spec": {
    "Executable": "$fake",
    "WorkingDir": "$smoke",
    "RegistryPath": "",
    "Environment": {
      "CODEX_HELPER_TEAMS_SERVICE": "1",
      "CXP_LOCAL_SUPERVISOR_PROBE_RUNS": "$runs",
      "CXP_LOCAL_SUPERVISOR_PROBE_MODE": "crash"
    }
  }
}
EOF

  HOME="$home" XDG_CONFIG_HOME="$config_home" XDG_CACHE_HOME="$cache_home" XDG_RUNTIME_DIR="$runtime_dir" \
    setsid "$cxp" teams service local-supervisor --config "$config" >"$supervisor_log" 2>&1 &
  local supervisor_pid="$!"
  cleanup_pids+=("$supervisor_pid")

  wait_for_file_pattern "$status" '"supervisor_pid"'
  wait_for_lines "$runs" 2

  if HOME="$home" XDG_CONFIG_HOME="$config_home" XDG_CACHE_HOME="$cache_home" XDG_RUNTIME_DIR="$other_runtime_dir" \
    setsid "$cxp" teams service local-supervisor --config "$config" >"$duplicate_log" 2>&1; then
    log "duplicate local supervisor unexpectedly acquired the lock"
    return 1
  fi
  if ! grep -qi 'already running' "$duplicate_log"; then
    log "duplicate local supervisor did not report lock contention"
    sed 's/^/[duplicate] /' "$duplicate_log" || true
    return 1
  fi

  local pgid
  pgid="$(sed -n 's/.*"supervisor_pgid": \([0-9][0-9]*\).*/\1/p' "$status" | tail -n 1)"
  if [ -z "$pgid" ]; then
    log "could not parse supervisor_pgid from status"
    sed 's/^/[status] /' "$status" || true
    return 1
  fi
  local my_pgid
  my_pgid="$(ps -o pgid= -p $$ | tr -d ' ')"
  if [ "$pgid" = "$my_pgid" ]; then
    log "supervisor PGID matches current shell PGID; refusing test kill"
    return 1
  fi
  local actual_pgid
  actual_pgid="$(ps -o pgid= -p "$supervisor_pid" | tr -d ' ')"
  if [ "$actual_pgid" != "$pgid" ]; then
    log "status PGID $pgid does not match live supervisor PGID $actual_pgid"
    ps -p "$supervisor_pid" -o pid,ppid,pgid,sid,stat,cmd || true
    return 1
  fi
  if ! ps -p "$supervisor_pid" -o args= | grep -q -- "teams service local-supervisor"; then
    log "supervisor cmdline does not look like local-supervisor; refusing test kill"
    ps -p "$supervisor_pid" -o pid,ppid,pgid,sid,stat,cmd || true
    return 1
  fi
  if ! ps -p "$supervisor_pid" -o args= | grep -q -- "$config"; then
    log "supervisor cmdline does not include temp config; refusing test kill"
    ps -p "$supervisor_pid" -o pid,ppid,pgid,sid,stat,cmd || true
    return 1
  fi
  kill -TERM "-$pgid"
  wait_for_process_gone "$supervisor_pid"
  wait "$supervisor_pid" 2>/dev/null || true
  remove_cleanup_pid "$supervisor_pid"
}

local_supervisor_public_lifecycle_smoke() {
  [ "$(uname -s)" = "Linux" ] || return 0
  log "linux local-supervisor public lifecycle smoke: install, enable, start, status, and stop"
  case "${CODEX_HELPER_TEAMS_CHILD:-}" in
    1|true|TRUE|yes|YES)
      log "::warning::skipping public lifecycle smoke inside a Teams-launched Codex turn; service management guards must stay active"
      return 0
      ;;
  esac
  local cxp="${CODEX_HELPER_CI_CXP:-}"
  if [ -z "$cxp" ]; then
    log "::warning::CODEX_HELPER_CI_CXP is not set; skipping public local-supervisor lifecycle smoke"
    return 0
  fi
  if [ ! -x "$cxp" ]; then
    log "CODEX_HELPER_CI_CXP is not executable: $cxp"
    return 1
  fi
  local smoke="$root/local-supervisor-public"
  local home="$smoke/home"
  local config_home="$smoke/config"
  local cache_home="$smoke/cache"
  local runtime_dir="$smoke/runtime"
  local runs="$smoke/public-child-runs.log"
  local child_env="$smoke/public-child-env.log"
  local fake="$smoke/fake-cxp"
  local config="$config_home/codex-helper/teams/local-supervisor.json"
  local status="$config_home/codex-helper/teams/run/local-supervisor/local-supervisor-status.json"
  mkdir -p "$home" "$config_home" "$cache_home" "$runtime_dir"
  write_token_cache "$cache_home/read-token.json"
  write_token_cache "$cache_home/write-token.json"

  cat >"$fake" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [ "\${1:-}" = "teams" ] && [ "\${2:-}" = "service" ] && [ "\${3:-}" = "local-supervisor" ]; then
  exec "$cxp" "\$@"
fi
if [ "\${1:-}" = "teams" ] && [ "\${2:-}" = "run" ]; then
  printf '%s public child pid=%s args=%s\n' "\$(date +%s)" "\$\$" "\$*" >>"$runs"
  printenv | sort >"$child_env"
  while :; do
    sleep 1
  done
fi
printf 'unexpected fake-cxp args: %s\n' "\$*" >&2
exit 64
EOF
  chmod +x "$fake"

  local common_env=(
    "PATH=$PATH"
    "HOME=$home"
    "XDG_CONFIG_HOME=$config_home"
    "XDG_CACHE_HOME=$cache_home"
    "XDG_RUNTIME_DIR=$runtime_dir"
    "DBUS_SESSION_BUS_ADDRESS=unix:path=$smoke/no-user-bus"
    "CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND=local-supervisor"
    "CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND=local-supervisor"
    "CODEX_HELPER_TEAMS_TENANT_ID=tenant"
    "CODEX_HELPER_TEAMS_CLIENT_ID=chat-client"
    "CODEX_HELPER_TEAMS_READ_CLIENT_ID=read-client"
    "CODEX_HELPER_TEAMS_TOKEN_CACHE=$cache_home/write-token.json"
    "CODEX_HELPER_TEAMS_READ_TOKEN_CACHE=$cache_home/read-token.json"
  )

  CODEX_PROXY_DEBUG=1 CODEX_HELPER_TEAMS_CHILD=1 HTTP_PROXY=http://127.0.0.1:9 env -i "${common_env[@]}" "$cxp" teams service install >"$smoke/install.log" 2>&1
  replace_json_executable "$config" "$fake"
  CODEX_PROXY_DEBUG=1 CODEX_HELPER_TEAMS_CHILD=1 HTTP_PROXY=http://127.0.0.1:9 env -i "${common_env[@]}" "$cxp" teams service enable >"$smoke/enable.log" 2>&1
  CODEX_PROXY_DEBUG=1 CODEX_HELPER_TEAMS_CHILD=1 HTTP_PROXY=http://127.0.0.1:9 env -i "${common_env[@]}" "$cxp" teams service start >"$smoke/start.log" 2>&1
  wait_for_file_pattern "$status" '"supervisor_pid"'
  local public_supervisor_pid
  public_supervisor_pid="$(sed -n 's/.*"supervisor_pid": \([0-9][0-9]*\).*/\1/p' "$status" | tail -n 1)"
  if [ -n "$public_supervisor_pid" ]; then
    cleanup_pids+=("$public_supervisor_pid")
  fi
  wait_for_lines "$runs" 1
  wait_for_file_pattern "$child_env" 'CODEX_HELPER_TEAMS_SERVICE=1'
  for forbidden_env in CODEX_PROXY_DEBUG CODEX_HELPER_TEAMS_CHILD HTTP_PROXY; do
    if grep -q "^$forbidden_env=" "$child_env"; then
      log "public lifecycle child inherited forbidden env $forbidden_env"
      sed 's/^/[public-child-env] /' "$child_env" || true
      return 1
    fi
  done
  local public_child_pid
  public_child_pid="$(sed -n 's/.*"child_pid": \([0-9][0-9]*\).*/\1/p' "$status" | tail -n 1)"
  if [ -n "$public_child_pid" ]; then
    cleanup_pids+=("$public_child_pid")
  fi
  CODEX_PROXY_DEBUG=1 CODEX_HELPER_TEAMS_CHILD=1 HTTP_PROXY=http://127.0.0.1:9 env -i "${common_env[@]}" "$cxp" teams service status >"$smoke/status.log" 2>&1
  if ! grep -q 'Active: true' "$smoke/status.log"; then
    log "public lifecycle status did not report active"
    sed 's/^/[public-status] /' "$smoke/status.log" || true
    return 1
  fi
  CODEX_PROXY_DEBUG=1 CODEX_HELPER_TEAMS_CHILD=1 HTTP_PROXY=http://127.0.0.1:9 env -i "${common_env[@]}" "$cxp" teams service stop >"$smoke/stop.log" 2>&1
  if [ -n "$public_child_pid" ] && ! wait_for_pid_gone "$public_child_pid"; then
    log "public lifecycle child pid $public_child_pid remained after stop"
    return 1
  fi
  if [ -n "$public_supervisor_pid" ] && ! wait_for_pid_gone "$public_supervisor_pid"; then
    log "public lifecycle supervisor pid $public_supervisor_pid remained after stop"
    return 1
  fi
  CODEX_PROXY_DEBUG=1 CODEX_HELPER_TEAMS_CHILD=1 HTTP_PROXY=http://127.0.0.1:9 env -i "${common_env[@]}" "$cxp" teams service status >"$smoke/status-after-stop.log" 2>&1
  if ! grep -q 'Active: false' "$smoke/status-after-stop.log"; then
    log "public lifecycle status did not report inactive after stop"
    sed 's/^/[public-status-after-stop] /' "$smoke/status-after-stop.log" || true
    return 1
  fi
  [ -n "$public_supervisor_pid" ] && remove_cleanup_pid "$public_supervisor_pid"
  [ -n "$public_child_pid" ] && remove_cleanup_pid "$public_child_pid"
}

local_supervisor_bootstrap_auto_fallback_smoke() {
  [ "$(uname -s)" = "Linux" ] || return 0
  log "linux local-supervisor bootstrap auto-fallback smoke: no-start path without backend override"
  case "${CODEX_HELPER_TEAMS_CHILD:-}" in
    1|true|TRUE|yes|YES)
      log "::warning::skipping bootstrap smoke inside a Teams-launched Codex turn; service management guards must stay active"
      return 0
      ;;
  esac
  local cxp="${CODEX_HELPER_CI_CXP:-}"
  if [ -z "$cxp" ]; then
    log "::warning::CODEX_HELPER_CI_CXP is not set; skipping bootstrap auto-fallback smoke"
    return 0
  fi
  if [ ! -x "$cxp" ]; then
    log "CODEX_HELPER_CI_CXP is not executable: $cxp"
    return 1
  fi
  local smoke="$root/local-supervisor-bootstrap"
  local home="$smoke/home"
  local config_home="$smoke/config"
  local cache_home="$smoke/cache"
  local runtime_dir="$smoke/runtime"
  mkdir -p "$home" "$config_home" "$cache_home" "$runtime_dir"
  local bootstrap_env=(
    "PATH=$PATH"
    "HOME=$home"
    "XDG_CONFIG_HOME=$config_home"
    "XDG_CACHE_HOME=$cache_home"
    "XDG_RUNTIME_DIR=$runtime_dir"
    "DBUS_SESSION_BUS_ADDRESS=unix:path=$smoke/no-user-bus"
  )
  env -i "${bootstrap_env[@]}" "$cxp" teams service bootstrap --no-start >"$smoke/bootstrap.log" 2>&1
  if ! grep -q 'local-supervisor-no-start' "$smoke/bootstrap.log"; then
    log "bootstrap did not auto-fallback to local-supervisor"
    sed 's/^/[bootstrap] /' "$smoke/bootstrap.log" || true
    return 1
  fi
  if [ ! -f "$config_home/codex-helper/teams/local-supervisor.json" ]; then
    log "bootstrap auto-fallback did not write local-supervisor config"
    sed 's/^/[bootstrap] /' "$smoke/bootstrap.log" || true
    return 1
  fi
}

terminal_detach_smoke() {
  log "terminal detach smoke: externally detached child should survive parent shell exit and SIGHUP"
  local probe="$root/probe-loop.sh"
  local heartbeat="$root/terminal-heartbeat.log"
  local pidfile="$root/terminal.pid"
  write_probe_loop "$probe"
  (
    nohup "$probe" "$heartbeat" >/dev/null 2>&1 &
    echo "$!" >"$pidfile"
  )
  local pid
  pid="$(cat "$pidfile")"
  cleanup_pids+=("$pid")
  wait_for_lines "$heartbeat" 2
  kill -HUP "$pid" >/dev/null 2>&1 || true
  wait_for_lines "$heartbeat" 4
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    log "detached child died after parent/SIGHUP simulation"
    return 1
  fi
}

linux_systemd_user_smoke() {
  [ "$(uname -s)" = "Linux" ] || return 0
  log "linux systemd --user smoke: test real user supervisor when available"
  if ! command -v systemctl >/dev/null 2>&1; then
    log "::warning::systemctl is not available; hosted CI cannot exercise systemd --user here"
    return 0
  fi
  if ! systemctl --user show-environment >/dev/null 2>&1; then
    log "::warning::systemd --user is not available in this runner session; this is the no-user-manager/no-linger boundary"
    if command -v loginctl >/dev/null 2>&1; then
      user_name="$(id -un 2>/dev/null || id -u)"
      loginctl show-user "$user_name" -p Linger -p State 2>/dev/null || true
    fi
    return 0
  fi

  local unit="codex-helper-teams-ci-smoke-$RANDOM.service"
  local heartbeat="$root/systemd-heartbeat.log"
  local unitdir="$HOME/.config/systemd/user"
  mkdir -p "$unitdir"
  cleanup_systemd_unit="$unit"
  cat >"$unitdir/$unit" <<EOF
[Unit]
Description=Codex Helper Teams CI user-service smoke

[Service]
Type=simple
ExecStart=/bin/sh -c 'while :; do date +%%s >> "$heartbeat"; sleep 1; done'
Restart=on-failure
RestartSec=1s

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user start "$unit"
  wait_for_lines "$heartbeat" 2
  local pid
  pid="$(systemctl --user show "$unit" -p MainPID --value 2>/dev/null || true)"
  if [ -n "$pid" ] && [ "$pid" != "0" ]; then
    local before
    before="$(wc -l <"$heartbeat" | tr -d ' ')"
    kill -KILL "$pid" >/dev/null 2>&1 || true
    wait_for_lines "$heartbeat" "$((before + 2))"
  fi
}

macos_launchagent_smoke() {
  [ "$(uname -s)" = "Darwin" ] || return 0
  log "macOS LaunchAgent smoke: bootstrap, heartbeat, and restart after process kill"
  local label="com.codex-helper.teams-ci-smoke.$RANDOM.$$"
  local plist="$root/$label.plist"
  local heartbeat="$root/launchagent-heartbeat.log"
  cleanup_launchd_label="$label"
  cat >"$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/sh</string>
    <string>-c</string>
    <string>while :; do date +%s &gt;&gt; "$heartbeat"; sleep 1; done</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>StandardOutPath</key>
  <string>$root/launchagent.out</string>
  <key>StandardErrorPath</key>
  <string>$root/launchagent.err</string>
</dict>
</plist>
EOF
  plutil -lint "$plist"
  launchctl bootstrap "gui/$(id -u)" "$plist"
  wait_for_lines "$heartbeat" 2
  local pid
  pid="$(launchctl print "gui/$(id -u)/$label" 2>/dev/null | awk -F'= ' '/pid =/{print $2; exit}' || true)"
  if [ -n "$pid" ]; then
    kill "$pid" >/dev/null 2>&1 || true
    wait_for_lines "$heartbeat" 4
  fi
}

local_supervisor_smoke
local_supervisor_public_lifecycle_smoke
local_supervisor_bootstrap_auto_fallback_smoke
terminal_detach_smoke
linux_systemd_user_smoke
macos_launchagent_smoke
