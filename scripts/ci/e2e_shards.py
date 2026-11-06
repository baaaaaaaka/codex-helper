#!/usr/bin/env python3
"""Split the black-box E2E scenario packages into CI shards.

The shards are whole Go packages: every scenario package has its own
TestMain that builds the system under test, so a package is the natural unit
of work. The package list comes from `go list` at run time, so a new scenario
package is picked up without editing the workflow; it is packed with
DEFAULT_WEIGHT until it gets a measured weight in e2e_shard_weights.json.

Packing is longest-first onto the lightest shard. Every package lands in
exactly one shard (the packing is a partition of the list), and every shard
computes the same plan from the same inputs, so the union of the shard jobs
is the whole suite.

Usage:
  e2e_shards.py --count 3 --index 0        # packages of shard 0, one per line
  e2e_shards.py --count 3 --plan           # the whole plan with weights
  e2e_shards.py --count 3 --index 0 --packages a b c   # no `go list`
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

SCENARIO_PATTERN = "./internal/e2e/scenarios/..."
WEIGHTS_FILE = Path(__file__).resolve().with_name("e2e_shard_weights.json")
# A package without a measurement is assumed to be about as heavy as a large
# measured package, so a new scenario never hides in an already full shard.
DEFAULT_WEIGHT = 60


def list_packages(pattern: str = SCENARIO_PATTERN) -> list[str]:
    out = subprocess.run(
        ["go", "list", pattern], check=True, text=True, capture_output=True
    ).stdout
    packages = sorted({line.strip() for line in out.splitlines() if line.strip()})
    if not packages:
        raise SystemExit(f"go list {pattern} returned no packages")
    return packages


def load_weights(path: Path = WEIGHTS_FILE) -> dict[str, float]:
    data = json.loads(path.read_text(encoding="utf-8"))
    weights = data.get("weights", {})
    if not isinstance(weights, dict):
        raise SystemExit(f"{path}: 'weights' must be an object")
    out: dict[str, float] = {}
    for name, value in weights.items():
        if not isinstance(value, (int, float)) or value <= 0:
            raise SystemExit(f"{path}: weight of {name!r} must be a positive number")
        out[name] = float(value)
    return out


def weight_of(package: str, weights: dict[str, float]) -> float:
    # Weights are keyed by the last path element (the scenario directory).
    return weights.get(package.rstrip("/").rsplit("/", 1)[-1], float(DEFAULT_WEIGHT))


def plan(packages: list[str], count: int, weights: dict[str, float]) -> list[list[str]]:
    if count < 1:
        raise SystemExit("--count must be >= 1")
    if len(set(packages)) != len(packages):
        raise SystemExit("duplicate packages in the input")
    shards: list[list[str]] = [[] for _ in range(count)]
    loads = [0.0] * count
    # Heaviest first; ties broken by name so every shard computes the same plan.
    for pkg in sorted(packages, key=lambda p: (-weight_of(p, weights), p)):
        i = min(range(count), key=lambda k: (loads[k], k))
        shards[i].append(pkg)
        loads[i] += weight_of(pkg, weights)
    return [sorted(s) for s in shards]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--count", type=int, required=True)
    ap.add_argument("--index", type=int)
    ap.add_argument("--plan", action="store_true", help="print every shard with its weight")
    ap.add_argument("--packages", nargs="*", help="use these packages instead of `go list`")
    ap.add_argument("--weights", type=Path, default=WEIGHTS_FILE)
    args = ap.parse_args(argv)

    packages = sorted(args.packages) if args.packages else list_packages()
    weights = load_weights(args.weights)
    shards = plan(packages, args.count, weights)

    if args.plan:
        for i, shard in enumerate(shards):
            total = sum(weight_of(p, weights) for p in shard)
            print(f"shard {i}: weight {total:.0f}")
            for pkg in shard:
                tag = "" if pkg.rsplit("/", 1)[-1] in weights else " (unmeasured, default weight)"
                print(f"  {pkg} {weight_of(pkg, weights):.0f}{tag}")
        return 0
    if args.index is None or not 0 <= args.index < args.count:
        ap.error("--index must be in [0, --count)")
    shard = shards[args.index]
    if not shard:
        # More shards than packages: an empty shard has nothing to run.
        print(f"shard {args.index} of {args.count} is empty", file=sys.stderr)
    for pkg in shard:
        print(pkg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
