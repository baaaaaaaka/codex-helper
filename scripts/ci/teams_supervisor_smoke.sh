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

cleanup() {
  for pid in "${cleanup_pids[@]:-}"; do
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

write_probe_loop() {
  local path="$1"
  cat >"$path" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
heartbeat="$1"
trap '' HUP
while :; do
  printf '%s pid=%s\n' "$(date +%s)" "$$" >>"$heartbeat"
  sleep 1
done
EOS
  chmod +x "$path"
}

terminal_detach_smoke() {
  log "terminal detach smoke: child should survive parent shell exit and SIGHUP"
  local probe="$root/probe-loop.sh"
  local heartbeat="$root/terminal-heartbeat.log"
  local pidfile="$root/terminal.pid"
  write_probe_loop "$probe"
  (
    "$probe" "$heartbeat" >/dev/null 2>&1 &
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

terminal_detach_smoke
linux_systemd_user_smoke
macos_launchagent_smoke
