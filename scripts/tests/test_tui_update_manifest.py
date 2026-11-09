from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import release_update_index as publisher


class TUIUpdateManifestTests(unittest.TestCase):
    def release(self):
        return {"tagName": "v2.0.0", "publishedAt": "2026-11-09T00:00:00Z",
                "priority": "p0", "assets": [{"name": name} for name in self.names()]}

    def names(self):
        return ["codex-proxy_2.0.0_" + platform.replace("/", "_")
                + (".exe" if platform.startswith("windows/") else "")
                for platform in publisher.TUI_PLATFORMS]

    def test_complete_manifest_matches_the_go_reader_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in self.names():
                (root / name).write_bytes(b"hello")
            manifest = publisher.tui_manifest("owner/name", self.release(), root,
                                              "2026-11-09T00:00:00Z")
            fixture = Path(__file__).resolve().parents[2] / "internal/update/testdata/tui-update.json"
            self.assertEqual(manifest, json.loads(fixture.read_text()))
            for asset in manifest["assets"].values():
                self.assertEqual(asset["sha256"], hashlib.sha256(b"hello").hexdigest())
                self.assertEqual(asset["size"], 5)

    def test_incomplete_assets_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises((ValueError, FileNotFoundError)):
                publisher.tui_manifest("owner/name", self.release(), Path(directory),
                                       "2026-11-09T00:00:00Z")

    def test_unapproved_channels_never_publish(self):
        for patch in ({"priority": "p2"}, {"isPrerelease": True}, {"isDraft": True},
                      {"tagName": "v2.0.0-rc.1"}, {"tagName": "../escape"}):
            with self.subTest(patch=patch):
                release = deepcopy(self.release())
                release.update(patch)
                self.assertIsNone(publisher.tui_manifest("owner/name", release, Path("unused"),
                                                        "2026-11-09T00:00:00Z"))
