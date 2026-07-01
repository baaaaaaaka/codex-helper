import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def targeted_job() -> str:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    start = workflow.index("  targeted-test:\n")
    end = workflow.index("  codex-runtime-contract:\n", start)
    return workflow[start:end]


def step_blocks(job: str) -> dict[str, str]:
    matches = list(re.finditer(r"^      - name: (.+)$", job, re.MULTILINE))
    blocks: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(job)
        blocks[match.group(1)] = job[match.start() : end]
    return blocks


class TargetedShardWorkflowTests(unittest.TestCase):
    def test_declares_three_parallel_shards(self):
        job = targeted_job()
        self.assertIn(
            "shard: [core, platform-integration, state-perf]",
            job,
        )
        self.assertNotIn("needs:", job)

    def test_every_non_setup_step_selects_exactly_one_shard(self):
        for name, block in step_blocks(targeted_job()).items():
            if name in {"Checkout", "Setup Go"}:
                continue
            matches = re.findall(
                r"^        if: matrix\.shard == '(core|platform-integration|state-perf)'(?: && .+)?$",
                block,
                re.MULTILINE,
            )
            self.assertEqual(len(matches), 1, name)

    def test_heavy_steps_are_assigned_to_expected_shards(self):
        blocks = step_blocks(targeted_job())
        expected = {
            "Teams SQLite store migration and perf regressions": "state-perf",
            "Teams perf benchmark smoke": "state-perf",
            "Skills local git smoke (Windows)": "platform-integration",
            "Codex desktop app network install smoke (Windows)": "platform-integration",
            "Install Codex for integration (Windows)": "platform-integration",
            "Teams Graph 429 stress (Linux only)": "core",
            "Cross-compile check (Linux only)": "core",
        }
        for name, shard in expected.items():
            self.assertIn(
                f"if: matrix.shard == '{shard}'",
                blocks[name],
                name,
            )


if __name__ == "__main__":
    unittest.main()
