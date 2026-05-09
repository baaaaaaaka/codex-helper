from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import release_update_index


class ReleaseUpdateIndexTests(unittest.TestCase):
    def test_updates_index_with_priority_asset_and_preserves_existing_releases(self) -> None:
        release = {
            "tagName": "v1.2.4",
            "name": "v1.2.4",
            "isPrerelease": False,
            "isDraft": False,
            "publishedAt": "2026-05-04T00:00:00Z",
            "assets": [
                {"name": "codex-helper-auto-update-p0.txt"},
                {"name": "codex-proxy_1.2.4_linux_amd64"},
            ],
        }
        existing = {
            "version": 1,
            "repo": "owner/name",
            "generated_at": "2026-05-03T00:00:00Z",
            "releases": [
                {
                    "tag_name": "v1.2.3",
                    "version": "1.2.3",
                    "priority": "p1",
                    "published_at": "2026-05-03T00:00:00Z",
                    "assets": [{"name": "codex-proxy_1.2.3_linux_amd64"}],
                }
            ],
        }

        out = release_update_index.update_index(
            "owner/name",
            release,
            existing,
            "2026-05-04T01:00:00Z",
            100,
        )

        self.assertEqual(out["version"], 1)
        self.assertEqual(out["repo"], "owner/name")
        self.assertEqual([rel["tag_name"] for rel in out["releases"]], ["v1.2.4", "v1.2.3"])
        self.assertEqual(out["releases"][0]["priority"], "p0")
        self.assertEqual(
            out["releases"][0]["assets"],
            [
                {"name": "codex-helper-auto-update-p0.txt"},
                {"name": "codex-proxy_1.2.4_linux_amd64"},
            ],
        )

    def test_same_stable_version_sorts_before_prerelease(self) -> None:
        release = {
            "tagName": "v1.2.4",
            "publishedAt": "2026-05-04T00:00:00Z",
            "assets": [{"name": "codex-proxy_1.2.4_linux_amd64"}],
        }
        existing = {
            "version": 1,
            "repo": "owner/name",
            "releases": [
                {
                    "tag_name": "v1.2.4-rc.10",
                    "version": "1.2.4-rc.10",
                    "priority": "p0",
                    "prerelease": True,
                    "published_at": "2026-05-03T00:00:00Z",
                    "assets": [{"name": "codex-proxy_1.2.4-rc.10_linux_amd64"}],
                }
            ],
        }

        out = release_update_index.update_index("owner/name", release, existing, "2026-05-04T01:00:00Z", 100)

        self.assertEqual([rel["tag_name"] for rel in out["releases"]], ["v1.2.4", "v1.2.4-rc.10"])

    def test_conflicting_priority_sources_fail_closed_to_p2(self) -> None:
        release = {
            "tagName": "v1.2.4",
            "priority": "p0",
            "body": '<!-- codex-helper-release: {"auto_update_priority":"p1"} -->',
            "publishedAt": "2026-05-04T00:00:00Z",
            "assets": [{"name": "codex-proxy_1.2.4_linux_amd64"}],
        }

        entry = release_update_index.index_entry("owner/name", release)

        self.assertEqual(entry["priority"], "p2")

    def test_cli_writes_json_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            release_path = root / "release.json"
            output_path = root / "update-index.json"
            release_path.write_text(
                json.dumps(
                    {
                        "tagName": "v1.2.4",
                        "body": '<!-- codex-helper-release: {"priority":"p1"} -->',
                        "publishedAt": "2026-05-04T00:00:00Z",
                        "assets": [{"name": "codex-proxy_1.2.4_linux_amd64"}],
                    }
                ),
                encoding="utf-8",
            )

            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve().parents[1] / "release_update_index.py"),
                    "--repo",
                    "owner/name",
                    "--release-json",
                    str(release_path),
                    "--output",
                    str(output_path),
                    "--generated-at",
                    "2026-05-04T01:00:00Z",
                ],
                check=True,
            )

            out = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(out["releases"][0]["tag_name"], "v1.2.4")
            self.assertEqual(out["releases"][0]["priority"], "p1")


if __name__ == "__main__":
    unittest.main()
