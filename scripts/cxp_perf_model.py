#!/usr/bin/env python3
"""Generate cxp performance fixtures and run the cxp benchmark model."""

from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class CXPPerfProfile:
    name: str
    description: str
    work_chats: int
    turns_per_chat: int
    messages_per_poll: int
    message_bytes: int
    outbox_per_chat: int
    lookup_per_cycle: int
    history_files: int
    history_lines: int
    rate_limited: bool = False


@dataclass(frozen=True)
class CXPPerfEnvironment:
    name: str
    description: str
    schema_version: int = 5
    machine_count: int = 1
    machine_kind: str = "primary"
    include_control_lease: bool = True
    stale_poll_cursors: bool = False
    extra_dashboard_views: int = 0


@dataclass(frozen=True)
class CXPPerfExternalScenario:
    name: str
    description: str
    graph_mode: str = ""
    codex_mode: str = "codex-success"
    service_mode: str = ""
    control_prompt: str = ""
    queue_outbox: bool = False


PROFILES: tuple[CXPPerfProfile, ...] = (
    CXPPerfProfile("light-user", "one or two short chats, mostly idle", 2, 6, 0, 96, 1, 4, 2, 20),
    CXPPerfProfile("many-short-chats", "many short-lived chats with small prompts", 80, 4, 1, 128, 1, 48, 20, 40),
    CXPPerfProfile("few-very-long-chats", "one or two chats with very long accumulated history", 2, 1500, 1, 2048, 8, 64, 4, 4000),
    CXPPerfProfile("many-long-chats", "many active work chats, each with long history", 40, 500, 1, 1024, 4, 160, 40, 1000),
    CXPPerfProfile("idle-chat-hoarder", "hundreds of inactive chats that still need scheduling decisions", 240, 2, 0, 80, 0, 64, 10, 20),
    CXPPerfProfile("ci-burst-user", "short CI-like commands with frequent status/output messages", 24, 80, 2, 256, 10, 128, 24, 200),
    CXPPerfProfile("attachment-heavy-user", "moderate chats with large artifacts and attachment metadata", 12, 120, 1, 512, 6, 64, 12, 200),
    CXPPerfProfile("recovery-replay-user", "helper restart after downtime with duplicate message replay", 32, 120, 3, 256, 3, 192, 32, 300),
    CXPPerfProfile("rate-limited-tenant", "many chats under Graph 429/backoff pressure", 48, 40, 0, 160, 2, 96, 12, 80, True),
    CXPPerfProfile("multi-workspace-power-user", "many workspaces, many chats, long local history", 64, 240, 1, 768, 4, 192, 80, 600),
)


ENVIRONMENTS: tuple[CXPPerfEnvironment, ...] = (
    CXPPerfEnvironment("current-single-machine", "current schema with one primary helper"),
    CXPPerfEnvironment("multi-machine-handoff", "current schema with two active helper machines", machine_count=2, extra_dashboard_views=8),
    CXPPerfEnvironment("ephemeral-ci-machine", "ephemeral helper identity with CI-like paths", machine_kind="ephemeral", extra_dashboard_views=2),
    CXPPerfEnvironment("legacy-schema-v1", "older state schema that must migrate without data loss", schema_version=1, stale_poll_cursors=True),
)


EXTERNAL_SCENARIOS: tuple[CXPPerfExternalScenario, ...] = (
    CXPPerfExternalScenario("all-ok-streaming", "Graph read/write succeeds and Codex streams status before a final answer", codex_mode="codex-streaming"),
    CXPPerfExternalScenario("codex-exec-error", "Codex exits with a terminal execution error after accepting the prompt", codex_mode="codex-error"),
    CXPPerfExternalScenario("codex-ambiguous-after-accept", "Codex returns a turn id but the helper cannot confirm completion", codex_mode="codex-ambiguous"),
    CXPPerfExternalScenario("codex-canceled", "Codex reports cancellation before a final result can be verified", codex_mode="codex-canceled"),
    CXPPerfExternalScenario("codex-thread-switch", "Codex reports a different thread id than the resumed Teams session", codex_mode="codex-thread-switch"),
    CXPPerfExternalScenario("graph-read-429", "Graph read is throttled and the poll path must park/back off", graph_mode="graph-read-429"),
    CXPPerfExternalScenario("graph-read-401", "Graph read keeps returning unauthorized after token refresh", graph_mode="graph-read-401"),
    CXPPerfExternalScenario("graph-read-403", "Graph read is forbidden for a chat", graph_mode="graph-read-403"),
    CXPPerfExternalScenario("graph-read-503", "Graph read has a transient service failure", graph_mode="graph-read-503"),
    CXPPerfExternalScenario("graph-read-network-drop", "Graph transport fails before a response is available", graph_mode="graph-read-network-drop"),
    CXPPerfExternalScenario("graph-read-malformed-json", "Graph returns HTTP 200 with invalid JSON", graph_mode="graph-read-malformed-json"),
    CXPPerfExternalScenario("graph-send-429", "Pending outbox delivery is throttled by Graph", graph_mode="graph-send-429", queue_outbox=True),
    CXPPerfExternalScenario("graph-send-403", "Pending outbox delivery is rejected as forbidden", graph_mode="graph-send-403", queue_outbox=True),
    CXPPerfExternalScenario("service-helper-restart", "Control chat asks the helper service layer to restart", service_mode="service-restart-command", control_prompt="helper restart now"),
    CXPPerfExternalScenario("service-helper-reload", "Control chat asks the helper service layer to reload", service_mode="service-reload-command", control_prompt="helper reload now"),
)


def utc_timestamp(offset: timedelta = timedelta()) -> str:
    return (datetime(2026, 5, 23, 7, 0, 0, tzinfo=timezone.utc) + offset).isoformat().replace("+00:00", "Z")


def repeat_text(size: int) -> str:
    if size <= 0:
        return "perf"
    return "x" * size


def scaled_count(value: int, scale: float, *, minimum: int = 0) -> int:
    if value <= 0:
        return 0
    return max(minimum, int(math.ceil(value * scale)))


def scale_profile(profile: CXPPerfProfile, scale: float) -> CXPPerfProfile:
    if scale <= 0:
        raise ValueError("--scale must be greater than 0")
    if scale == 1:
        return profile
    return replace(
        profile,
        work_chats=scaled_count(profile.work_chats, scale, minimum=1),
        turns_per_chat=scaled_count(profile.turns_per_chat, scale, minimum=1),
        messages_per_poll=scaled_count(profile.messages_per_poll, scale),
        outbox_per_chat=scaled_count(profile.outbox_per_chat, scale),
        lookup_per_cycle=scaled_count(profile.lookup_per_cycle, scale, minimum=1),
        history_files=scaled_count(profile.history_files, scale, minimum=1),
        history_lines=scaled_count(profile.history_lines, scale, minimum=1),
    )


def select_profiles(name: str) -> list[CXPPerfProfile]:
    if name == "all":
        return list(PROFILES)
    for profile in PROFILES:
        if profile.name == name:
            return [profile]
    raise ValueError(f"unknown profile: {name}")


def select_environments(name: str) -> list[CXPPerfEnvironment]:
    if name == "all":
        return list(ENVIRONMENTS)
    for environment in ENVIRONMENTS:
        if environment.name == name:
            return [environment]
    raise ValueError(f"unknown environment: {name}")


def session_id(index: int) -> str:
    return f"perf-session-{index:03d}"


def chat_id(index: int) -> str:
    return f"perf-chat-{index:03d}"


def inbound_id(chat_index: int, turn_index: int) -> str:
    return f"perf-inbound-{chat_index:03d}-{turn_index:06d}"


def inbound_message_id(chat_index: int, turn_index: int) -> str:
    return f"perf-message-{chat_index:03d}-{turn_index:06d}"


def build_machine(index: int, environment: CXPPerfEnvironment) -> dict[str, object]:
    now = utc_timestamp()
    machine_id = f"perf-machine-{index:02d}"
    status = "active" if index == 0 else "standby"
    return {
        "id": machine_id,
        "scope_id": "perf-scope",
        "label": f"perf-host-{index:02d}",
        "hostname": f"perf-host-{index:02d}",
        "os_user": "perf",
        "account_id": "perf-user",
        "user_principal": "perf@example.test",
        "profile": "perf",
        "kind": environment.machine_kind,
        "priority": 100 - index,
        "status": status,
        "last_seen": now,
        "created_at": now,
        "updated_at": now,
    }


def build_state(profile: CXPPerfProfile, environment: CXPPerfEnvironment) -> dict[str, object]:
    now = utc_timestamp()
    machines = {f"perf-machine-{i:02d}": build_machine(i, environment) for i in range(environment.machine_count)}
    machine_identity = dict(machines["perf-machine-00"])
    machine_identity.pop("os_user", None)
    machine_identity.pop("status", None)
    machine_identity.pop("last_seen", None)

    sessions: dict[str, object] = {}
    inbound_events: dict[str, object] = {}
    outbox_messages: dict[str, object] = {}
    message_provenance: dict[str, object] = {}
    chat_polls: dict[str, object] = {}
    workspaces: dict[str, object] = {}

    body = repeat_text(profile.message_bytes)
    for chat_index in range(profile.work_chats):
        sid = session_id(chat_index)
        cid = chat_id(chat_index)
        sessions[sid] = {
            "id": sid,
            "status": "active",
            "teams_chat_id": cid,
            "cwd": f"/workspace/project-{chat_index:03d}",
            "created_at": now,
            "updated_at": now,
        }
        workspaces[f"workspace-{chat_index:03d}"] = {
            "id": f"workspace-{chat_index:03d}",
            "scope_id": "perf-scope",
            "path": f"/workspace/project-{chat_index:03d}",
            "label": f"project-{chat_index:03d}",
            "created_at": now,
            "updated_at": now,
        }
        cursor = utc_timestamp(timedelta(minutes=-30 if environment.stale_poll_cursors else -1))
        chat_polls[cid] = {
            "chat_id": cid,
            "seeded": True,
            "poll_state": "warm",
            "next_poll_at": utc_timestamp(timedelta(seconds=-1)),
            "last_activity_at": now,
            "last_modified_cursor": cursor,
            "last_successful_poll_at": cursor,
            "updated_at": now,
        }
        for turn_index in range(profile.turns_per_chat):
            created = utc_timestamp(timedelta(milliseconds=chat_index * profile.turns_per_chat + turn_index))
            iid = inbound_id(chat_index, turn_index)
            mid = inbound_message_id(chat_index, turn_index)
            inbound_events[iid] = {
                "id": iid,
                "session_id": sid,
                "teams_chat_id": cid,
                "teams_message_id": mid,
                "text": body,
                "status": "persisted",
                "created_at": created,
                "updated_at": created,
            }
            pid = f"perf-provenance-{chat_index:03d}-{turn_index:06d}"
            message_provenance[pid] = {
                "id": pid,
                "teams_chat_id": cid,
                "teams_message_id": mid,
                "origin": "user_inbound",
                "session_id": sid,
                "inbound_id": iid,
                "created_at": created,
                "updated_at": created,
            }
        for outbox_index in range(profile.outbox_per_chat):
            oid = f"perf-outbox-{chat_index:03d}-{outbox_index:03d}"
            outbox_messages[oid] = {
                "id": oid,
                "session_id": sid,
                "teams_chat_id": cid,
                "kind": "answer",
                "body": body,
                "sequence": outbox_index + 1,
                "part_index": 1,
                "part_count": 1,
                "status": "sent",
                "teams_message_id": f"perf-helper-message-{chat_index:03d}-{outbox_index:03d}",
                "created_at": now,
                "updated_at": now,
                "sent_at": now,
            }

    state: dict[str, object] = {
        "schema_version": environment.schema_version,
        "created_at": now,
        "updated_at": now,
        "scope": {
            "id": "perf-scope",
            "account_id": "perf-user",
            "user_principal": "perf@example.test",
            "os_user": "perf",
            "profile": "perf",
            "config_path": "/tmp/cxp-perf/config.toml",
            "codex_home": "/tmp/cxp-perf/codex",
            "created_at": now,
            "updated_at": now,
        },
        "machine_identity": machine_identity,
        "machines": machines,
        "sessions": sessions,
        "turns": {},
        "inbound_events": inbound_events,
        "outbox_messages": outbox_messages,
        "message_provenance": message_provenance,
        "chat_polls": chat_polls,
        "workspaces": workspaces,
        "dashboard_views": {
            f"dashboard-{i:03d}": {
                "id": f"dashboard-{i:03d}",
                "scope_id": "perf-scope",
                "created_at": now,
                "updated_at": now,
            }
            for i in range(environment.extra_dashboard_views)
        },
    }
    if environment.include_control_lease:
        state["control_lease"] = {
            "holder_id": "perf-machine-00",
            "scope_id": "perf-scope",
            "status": "active",
            "expires_at": utc_timestamp(timedelta(minutes=2)),
            "updated_at": now,
        }
    return state


def build_registry(profile: CXPPerfProfile) -> dict[str, object]:
    now = utc_timestamp()
    return {
        "version": 1,
        "user_id": "perf-user",
        "control_chat_id": "control-chat",
        "chats": {},
        "sessions": [
            {
                "id": session_id(chat_index),
                "chat_id": chat_id(chat_index),
                "chat_url": f"https://teams.example/{chat_id(chat_index)}",
                "topic": "perf",
                "status": "active",
                "created_at": now,
                "updated_at": now,
            }
            for chat_index in range(profile.work_chats)
        ],
    }


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_history(root: Path, profile: CXPPerfProfile) -> None:
    history_root = root / "history"
    history_root.mkdir(parents=True, exist_ok=True)
    body = repeat_text(min(profile.message_bytes, 512))
    for file_index in range(profile.history_files):
        sid = session_id(file_index % max(1, profile.work_chats))
        path = history_root / f"{sid}.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for line_index in range(profile.history_lines):
                record = {
                    "timestamp": utc_timestamp(timedelta(seconds=line_index)),
                    "session_id": sid,
                    "role": "user" if line_index % 2 == 0 else "assistant",
                    "content": body,
                    "sequence": line_index + 1,
                }
                handle.write(json.dumps(record, sort_keys=True) + "\n")


def generate_fixture(root: Path, profile: CXPPerfProfile, environment: CXPPerfEnvironment, *, clean: bool) -> dict[str, object]:
    target = root / profile.name / environment.name
    if clean and target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    state = build_state(profile, environment)
    registry = build_registry(profile)
    write_json(target / "state.json", state)
    write_json(target / "registry.json", registry)
    write_history(target, profile)

    manifest = {
        "profile": asdict(profile),
        "environment": asdict(environment),
        "files": {
            "state": "state.json",
            "registry": "registry.json",
            "history": "history",
        },
        "counts": {
            "sessions": len(state["sessions"]),
            "inbound_events": len(state["inbound_events"]),
            "message_provenance": len(state["message_provenance"]),
            "outbox_messages": len(state["outbox_messages"]),
            "history_files": profile.history_files,
            "history_lines_per_file": profile.history_lines,
        },
    }
    write_json(target / "manifest.json", manifest)
    return {"path": str(target), **manifest["counts"]}


def print_profile_list(as_json: bool) -> None:
    payload = {
        "profiles": [asdict(profile) for profile in PROFILES],
        "environments": [asdict(environment) for environment in ENVIRONMENTS],
        "external_scenarios": [asdict(scenario) for scenario in EXTERNAL_SCENARIOS],
    }
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    print("Profiles:")
    for profile in PROFILES:
        print(f"  {profile.name}: {profile.description}")
    print("Environments:")
    for environment in ENVIRONMENTS:
        print(f"  {environment.name}: {environment.description}")
    print("External scenarios:")
    for scenario in EXTERNAL_SCENARIOS:
        print(f"  {scenario.name}: {scenario.description}")


BENCH_TARGETS = {
    "all": "BenchmarkCXPPerfModel",
    "store": "BenchmarkCXPPerfModelStoreProfiles",
    "bridge-poll": "BenchmarkCXPPerfModelBridgePollProfiles",
    "sync-total-cycle": "BenchmarkCXPPerfModelSyncTotalCycleProfiles",
    "daemon-poll-ingest": "BenchmarkCXPPerfModelDaemonPollIngestProfiles",
    "sqlite-daemon-poll-ingest": "BenchmarkCXPPerfModelSQLiteDaemonPollIngestProfiles",
    "daemon-total-cycle": "BenchmarkCXPPerfModelDaemonTotalCycleProfiles",
    "sqlite-daemon-total-cycle": "BenchmarkCXPPerfModelSQLiteDaemonTotalCycleProfiles",
    "daemon-idle-cycle": "BenchmarkCXPPerfModelDaemonIdleCycleProfiles",
    "sqlite-daemon-idle-cycle": "BenchmarkCXPPerfModelSQLiteDaemonIdleCycleProfiles",
    "daemon-queued-drain": "BenchmarkCXPPerfModelDaemonQueuedTurnDrainProfiles",
    "sqlite-daemon-queued-drain": "BenchmarkCXPPerfModelSQLiteDaemonQueuedTurnDrainProfiles",
    "daemon-outbox-flush": "BenchmarkCXPPerfModelDaemonOutboxFlushProfiles",
    "sqlite-daemon-outbox-flush": "BenchmarkCXPPerfModelSQLiteDaemonOutboxFlushProfiles",
    "sqlite-deferred-inbound": "BenchmarkCXPPerfModelSQLiteDeferredInboundNoDeferredProfiles",
    "sqlite-history-watch-checkpoint": "BenchmarkCXPPerfModelSQLiteHistoryWatchCheckpointUpdateProfiles",
    "sqlite-history-watch-active": "BenchmarkCXPPerfModelSQLiteHistoryWatchActiveAppendProfiles",
    "listen-once": "BenchmarkCXPPerfModelListenOnceProfiles",
    "external": "BenchmarkCXPPerfModelExternalScenarios",
}


def bench_regex(target: str, profile_name: str, scenario_name: str) -> str:
    prefix = BENCH_TARGETS[target]
    if target == "external":
        if scenario_name == "all":
            return prefix
        return f"{prefix}/{scenario_name}$"
    if profile_name == "all":
        return prefix
    return f"{prefix}/{profile_name}$"


def run_bench(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo_root).resolve()
    cmd = [
        "go",
        "test",
        "./internal/teams",
        "-run",
        "^$",
        "-bench",
        bench_regex(args.target, args.profile, args.scenario),
        "-benchmem",
        "-benchtime",
        args.benchtime,
        "-count",
        str(args.count),
    ]
    print(" ".join(cmd), file=sys.stderr)
    return subprocess.run(cmd, cwd=str(repo_root)).returncode


def run_generate(args: argparse.Namespace) -> int:
    profiles = [scale_profile(profile, args.scale) for profile in select_profiles(args.profile)]
    environments = select_environments(args.environment)
    root = Path(args.out).resolve()
    generated = []
    for profile in profiles:
        for environment in environments:
            generated.append(generate_fixture(root, profile, environment, clean=args.clean))
    write_json(
        root / "manifest.json",
        {
            "scale": args.scale,
            "profiles": [profile.name for profile in profiles],
            "environments": [environment.name for environment in environments],
            "generated": generated,
        },
    )
    print(json.dumps({"out": str(root), "generated": len(generated)}, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List built-in profiles and environment variants")
    list_parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    generate_parser = subparsers.add_parser("generate", help="Generate state/registry/history fixture directories")
    generate_parser.add_argument("--out", required=True, help="Output directory")
    generate_parser.add_argument("--profile", default="all", choices=["all", *[profile.name for profile in PROFILES]])
    generate_parser.add_argument("--environment", default="current-single-machine", choices=["all", *[env.name for env in ENVIRONMENTS]])
    generate_parser.add_argument("--scale", type=float, default=1.0, help="Scale counts down or up; 1.0 is full size")
    generate_parser.add_argument("--clean", action="store_true", help="Remove matching generated directories before writing")

    bench_parser = subparsers.add_parser("bench", help="Run Go benchmarks for the cxp perf model")
    bench_parser.add_argument("--repo-root", default=".", help="Repository root")
    bench_parser.add_argument("--target", default="all", choices=sorted(BENCH_TARGETS), help="Benchmark layer to run")
    bench_parser.add_argument("--profile", default="all", choices=["all", *[profile.name for profile in PROFILES]])
    bench_parser.add_argument("--scenario", default="all", choices=["all", *[scenario.name for scenario in EXTERNAL_SCENARIOS]])
    bench_parser.add_argument("--benchtime", default="1x")
    bench_parser.add_argument("--count", type=int, default=1)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "list":
            print_profile_list(args.json)
            return 0
        if args.command == "generate":
            return run_generate(args)
        if args.command == "bench":
            return run_bench(args)
    except ValueError as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
