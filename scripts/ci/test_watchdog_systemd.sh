#!/usr/bin/env bash
set -euo pipefail

# Never run this against a developer's user service manager.
[[ ${GITHUB_ACTIONS:-} == true && ${RUNNER_ENVIRONMENT:-} == github-hosted ]]
[[ -n ${RUNNER_TEMP:-} && -x ${CXP_LEGACY_WATCHDOG_BINARY:-} ]]
main=codex-helper-teams.service
watchdog=codex-helper-teams-watchdog.service
for unit in "$main" "$watchdog"; do
  if systemctl --user cat "$unit" >/dev/null 2>&1; then
    echo "Refusing to replace pre-existing unit $unit" >&2
    exit 1
  fi
done
root=$(mktemp -d "$RUNNER_TEMP/cxp-watchdog-systemd.XXXXXX")
mkdir -p "$root/versions/v0.1.20" "$root/versions/v9.9.1" "$root/home" "$root/units"
printf 'v9.9.1\n' > "$root/active"
cp "$CXP_LEGACY_WATCHDOG_BINARY" "$root/versions/v0.1.20/cxp"
go build -ldflags '-X github.com/baaaaaaaka/codex-helper/internal/cli.version=v9.9.1' -o "$root/versions/v9.9.1/cxp" ./cmd/codex-proxy
go test -c -o "$root/cli.test" ./internal/cli
cleanup() {
  systemctl --user stop "$watchdog" "$main" || true
  systemctl --user disable "$watchdog" "$main" || true
  systemctl --user daemon-reload || true
}
trap cleanup EXIT
for version in v0.1.20 v9.9.1; do
  cat > "$root/units/$watchdog.$version" <<EOF
[Service]
Type=simple
Environment=HOME=$root/home
Environment=CXP_RUNTIME_DISABLE=1
Environment=CODEX_HELPER_STATE_DIR=$root/state
ExecStart=$root/versions/$version/cxp teams service watchdog --loop --interval 100ms --quiet
EOF
done
cp "$root/units/$watchdog.v0.1.20" "$root/units/$watchdog"
cat > "$root/units/$main" <<EOF
[Service]
Type=simple
Environment=HOME=$root/home
Environment=CXP_SYSTEMD_WATCHDOG_TEST_ROOT=$root
Environment=GITHUB_ACTIONS=true
Environment=RUNNER_ENVIRONMENT=github-hosted
Environment=XDG_RUNTIME_DIR=$XDG_RUNTIME_DIR
Environment=DBUS_SESSION_BUS_ADDRESS=$DBUS_SESSION_BUS_ADDRESS
ExecStart=$root/cli.test -test.run=^TestLegacyTeamsWatchdogSystemdChild$ -test.v
EOF
systemctl --user link "$root/units/$watchdog" "$root/units/$main"
systemctl --user start "$watchdog"
oldexe=
for ((i=0; i<50; i++)); do
  oldpid=$(systemctl --user show -p MainPID --value "$watchdog")
  oldexe=$(readlink "/proc/$oldpid/exe" || true)
  [[ $oldexe == "$root/versions/v0.1.20/cxp" ]] && break
  sleep 0.1
done
[[ $oldexe == "$root/versions/v0.1.20/cxp" ]]
# Match a real upgrade: the installed command is updated, the old process
# remains running until the main startup repairs it.
cp "$root/units/$watchdog.v9.9.1" "$root/units/$watchdog"
systemctl --user daemon-reload
systemctl --user start "$main"
for ((i=0; i<150; i++)); do
  [[ -f $root/main-survived ]] && break
  sleep 0.2
done
journalctl --user -u "$main" -u "$watchdog" --no-pager -n 50
[[ $(cat "$root/main-survived") == ok ]]
[[ $(systemctl --user show -p NRestarts --value "$main") == 0 ]]
