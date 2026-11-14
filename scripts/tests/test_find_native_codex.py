import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'ci' / 'find_native_codex.py'


@unittest.skipUnless(sys.platform == 'linux' and platform.machine() in ('x86_64', 'aarch64'), 'Linux supported architecture')
class NativeCodexLookupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.wrapper = self.root / '@openai' / 'codex'
        self.wrapper.mkdir(parents=True)
        (self.wrapper / 'package.json').write_text(json.dumps({'version': '0.131.0'}))
        machine = platform.machine()
        self.package = self.root / '@openai' / ('codex-linux-x64' if machine == 'x86_64' else 'codex-linux-arm64')
        self.triple = machine + '-unknown-linux-musl'

    def binary(self, folder, version='0.131.0', package=None):
        binary = (package or self.package) / 'vendor' / self.triple / folder / 'codex'
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text('#!/bin/sh\necho codex-cli ' + version + '\n')
        binary.chmod(0o755)
        return binary

    def run_lookup(self, version='0.131.0'):
        return subprocess.run([sys.executable, str(SCRIPT), str(self.root), version], text=True, capture_output=True)

    def test_old_and_new_layout(self):
        for folder in ('codex', 'bin'):
            with self.subTest(folder=folder):
                binary = self.binary(folder)
                result = self.run_lookup()
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), str(binary))
                binary.unlink()

    def test_global_install_nested_dependency(self):
        nested = self.wrapper / 'node_modules' / '@openai' / self.package.name
        binary = self.binary('codex', package=nested)
        result = self.run_lookup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(binary))

    def test_missing_and_wrong_version_fail(self):
        self.assertNotEqual(self.run_lookup().returncode, 0)
        self.binary('codex', '0.132.0')
        result = self.run_lookup()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('expected', result.stderr)
        self.assertNotEqual(self.run_lookup('0.132.0').returncode, 0)


if __name__ == '__main__':
    unittest.main()
