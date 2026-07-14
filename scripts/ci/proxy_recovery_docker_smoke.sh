#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the proxy recovery Docker smoke" >&2
  exit 1
fi
if ! command -v ssh-keygen >/dev/null 2>&1; then
  echo "ssh-keygen is required for the proxy recovery Docker smoke" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
network="codex-helper-proxy-recovery-${$}"
image="codex-helper-proxy-recovery:${$}"
ssh_container="${network}-ssh"
runner_container="${network}-runner"
upstream_container="${network}-upstream"

cleanup() {
  set +e
  docker rm -f "$runner_container" "$ssh_container" "$upstream_container" >/dev/null 2>&1
  docker network rm "$network" >/dev/null 2>&1
  docker image rm "$image" >/dev/null 2>&1
  rm -rf "$tmp_dir"
}

on_exit() {
  rc=$?
  if [[ "$rc" -ne 0 ]]; then
    echo "proxy recovery Docker smoke failed; runner logs:" >&2
    docker logs "$runner_container" 2>&1 || true
    echo "SSH server container logs:" >&2
    docker logs "$ssh_container" 2>&1 || true
  fi
  cleanup
  exit "$rc"
}
trap on_exit EXIT

wait_for_sshd() {
  for _ in {1..50}; do
    if docker exec "$ssh_container" sh -c \
      'test -s /run/sshd/proxy-recovery.pid && kill -0 "$(cat /run/sshd/proxy-recovery.pid)"' \
      >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

chmod 700 "$tmp_dir"
mkdir -p "$tmp_dir/ready" "$tmp_dir/done"
ssh-keygen -t ed25519 -N "" -f "$tmp_dir/test_key" >/dev/null
go test -c -o "$tmp_dir/localproxy.test" ./internal/localproxy
go test -c -o "$tmp_dir/stack.test" ./internal/stack
go test -c -o "$tmp_dir/ssh.test" ./internal/ssh
go test -c -o "$tmp_dir/cli.test" ./internal/cli
go test -race -c -o "$tmp_dir/localproxy-race.test" ./internal/localproxy
go test -race -c -o "$tmp_dir/stack-race.test" ./internal/stack

docker build \
  --file "$repo_root/scripts/tests/Dockerfile.proxy-recovery" \
  --tag "$image" \
  "$repo_root/scripts/tests"
docker network create "$network" >/dev/null

authorized_key="$(<"$tmp_dir/test_key.pub")"
docker run --detach \
  --name "$ssh_container" \
  --network "$network" \
  --network-alias proxy-ssh \
  --cap-add NET_ADMIN \
  --env SSH_TEST_USER=ci \
  --env SSH_AUTHORIZED_KEY="$authorized_key" \
  "$image" >/dev/null

docker run --detach \
  --name "$upstream_container" \
  --network "$network" \
  --network-alias proxy-upstream \
  --entrypoint /usr/bin/socat \
  "$image" TCP-LISTEN:3333,fork,reuseaddr EXEC:/bin/cat >/dev/null

if ! wait_for_sshd; then
  echo "sshd did not become ready" >&2
  exit 1
fi

docker run --detach \
  --name "$runner_container" \
  --network "$network" \
  --cap-add NET_ADMIN \
  --volume "$tmp_dir:/lab:rw" \
  --env SSH_TEST_ENABLED=1 \
  --env SSH_STACK_INTEGRATION_TEST=1 \
  --env STACK_DOCKER_RECOVERY_TEST=1 \
  --env SSH_TEST_HOST=proxy-ssh \
  --env SSH_TEST_PORT=2222 \
  --env SSH_TEST_USER=ci \
  --env SSH_TEST_KEY=/lab/test_key \
  --env STACK_DOCKER_READY_DIR=/lab/ready \
  --env STACK_DOCKER_DONE_DIR=/lab/done \
  --env STACK_DOCKER_FAULT_ROUNDS=4 \
  --env STACK_DOCKER_REQUIRE_GENERATION_ROUNDS=1,0,0,1 \
  --env STACK_DOCKER_TARGET_HOST=proxy-upstream \
  --env STACK_DOCKER_TARGET_PORT=3333 \
  --entrypoint /bin/sh \
  "$image" \
  -c '
    set -eu
    unset SSH_TEST_ENABLED SSH_STACK_INTEGRATION_TEST STACK_DOCKER_RECOVERY_TEST
    /lab/localproxy.test -test.run "^Test.*$" -test.v
    /lab/stack.test -test.run "^Test.*$" -test.v
    /lab/ssh.test -test.run "^Test.*$" -test.v
    /lab/localproxy-race.test -test.run "^Test.*$" -test.v
    /lab/stack-race.test -test.run "^Test.*$" -test.v
    /lab/cli.test -test.run "Test(WithCodexAppProxyStartupLockSerializesCallers|CodexAppAuth(MacProxyBrowser.*|WindowsProxy(ReachabilityScriptChecksHealthEndpoint|BrowserScriptUsesChromiumProxy)))$" -test.v
    export SSH_TEST_ENABLED=1 SSH_STACK_INTEGRATION_TEST=1
    export STACK_DOCKER_TARGET_HOST=proxy-upstream STACK_DOCKER_TARGET_PORT=3333
    export STACK_DOCKER_HEALTHY_TEST=1
    export STACK_DOCKER_HEALTHY_READY_FILE=/lab/healthy-ready
    export STACK_DOCKER_HEALTHY_START_FILE=/lab/healthy-start
    export STACK_DOCKER_HEALTHY_DONE_FILE=/lab/healthy-done
    touch /lab/healthy-start
    /lab/stack.test -test.run "^TestStackIntegrationDockerHealthySteadyState$" -test.v
    export STACK_DOCKER_RECOVERY_TEST=1
    exec /lab/stack.test -test.run "^TestStackIntegrationDockerNetworkRecovery$" -test.v
  ' >/dev/null

for round in 0 1 2 3; do
  ready=0
  for _ in {1..300}; do
    if [[ -f "$tmp_dir/ready/$round" ]]; then
      ready=1
      break
    fi
    running="$(docker inspect --format '{{.State.Running}}' "$runner_container" 2>/dev/null || true)"
    if [[ "$running" != "true" ]]; then
      break
    fi
    sleep 0.2
  done
  if [[ "$ready" -ne 1 ]]; then
    echo "runner did not reach Docker fault round $round" >&2
    exit 1
  fi

  case "$round" in
    0|3)
      echo "round $round: stopping SSH peer and reconnecting its Docker endpoint"
      docker stop --time 0 "$ssh_container" >/dev/null
      docker network disconnect "$network" "$ssh_container"
      sleep 2
      docker start "$ssh_container" >/dev/null
      docker network connect --alias proxy-ssh "$network" "$ssh_container"
      if ! wait_for_sshd; then
        echo "sshd did not become ready after round $round" >&2
        exit 1
      fi
      ;;
    1)
      echo "round $round: rebuilding the Docker network with live peers"
      docker network disconnect "$network" "$ssh_container"
      docker network disconnect "$network" "$runner_container"
      docker network disconnect "$network" "$upstream_container"
      docker network rm "$network" >/dev/null
      docker network create "$network" >/dev/null
      sleep 1
      docker network connect --alias proxy-ssh "$network" "$ssh_container"
      docker network connect --alias proxy-upstream "$network" "$upstream_container"
      docker network connect "$network" "$runner_container"
      if ! wait_for_sshd; then
        echo "sshd did not remain ready after network rebuild" >&2
        exit 1
      fi
      ;;
    2)
      echo "round $round: applying 100% packet loss to runner network namespace"
      docker exec "$runner_container" tc qdisc replace dev eth0 root netem loss 100%
      sleep 2
      docker exec "$runner_container" tc qdisc del dev eth0 root
      ;;
  esac
  touch "$tmp_dir/done/$round"
done

runner_status="$(docker wait "$runner_container")"
docker logs "$runner_container"
if [[ "$runner_status" != "0" ]]; then
  echo "runner exited with status $runner_status" >&2
  exit 1
fi

echo "proxy recovery Docker smoke passed"
