from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "cleanup_patched_binaries.py"
SPEC = importlib.util.spec_from_file_location("cleanup_patched_binaries", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
cleanup = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cleanup
SPEC.loader.exec_module(cleanup)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@unittest.skipUnless(os.name == "posix" and hasattr(os, "geteuid"), "POSIX only")
class CleanupPatchedBinariesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.home = self.root / "home"
        self.tmp = self.root / "tmp"
        self.home.mkdir(mode=0o700)
        self.tmp.mkdir(mode=0o755)
        self.uid = os.geteuid()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def cleaner(
        self,
        *,
        purge: bool,
        terminate_active: bool = False,
        verify_passes: int = 2,
        claude_originals: dict[Path, Path] | None = None,
    ) -> cleanup.PatchedBinaryCleaner:
        return cleanup.PatchedBinaryCleaner(
            home=self.home,
            tmp_root=self.tmp,
            uid=self.uid,
            purge=purge,
            terminate_active=terminate_active,
            settle_seconds=0,
            verify_passes=verify_passes,
            claude_originals=claude_originals,
        )

    @staticmethod
    def write(path: Path, data: bytes, mode: int = 0o600) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        path.chmod(mode)
        return path

    def write_history(
        self,
        flavor: str,
        entries: list[dict[str, object]],
        *,
        version: int = 1,
    ) -> Path:
        path = self.home / ".config" / f"{flavor}-proxy" / "patch_history.json"
        self.write(
            path,
            (json.dumps({"version": version, "entries": entries}, indent=2) + "\n").encode(),
        )
        return path

    def test_audit_is_non_mutating_and_purge_is_idempotent_for_codex(self) -> None:
        binary = self.write(
            self.home / ".config" / "codex-proxy" / "codex-patched",
            b"#!/bin/sh\n# /tmp/cxreq/requirements.toml\n",
            0o755,
        )

        audit = self.cleaner(purge=False).run()
        self.assertFalse(audit["clean"])
        self.assertTrue(binary.exists())
        self.assertIn("would_remove", audit["counts"])

        purge = self.cleaner(purge=True).run()
        self.assertTrue(purge["clean"], purge)
        self.assertFalse(binary.exists())
        self.assertTrue(self.cleaner(purge=True).run()["clean"])

    def test_codex_random_binary_and_invalid_lease_are_removed_in_reserved_root(self) -> None:
        binary = self.write(
            self.home / ".config" / "codex-proxy" / "codex-patched-deadbeef",
            b"#!/bin/sh\n",
            0o755,
        )
        lease = self.write(Path(str(binary) + ".lease"), b"not-json")

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertFalse(binary.exists())
        self.assertFalse(lease.exists())

    def test_codex_marker_proves_custom_home_location(self) -> None:
        binary = self.write(
            self.home / "custom" / "codex-patched-copy",
            b"\x7fELFpayload/tmp/cxabcdef-1234/reqs.tomlmore",
            0o755,
        )

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertFalse(binary.exists())

    def test_unproven_same_name_is_preserved_and_blocks_success(self) -> None:
        binary = self.write(
            self.home / "personal" / "codex-patched",
            b"#!/bin/sh\necho personal\n",
            0o755,
        )

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(binary.exists())
        self.assertTrue(
            any(
                item["status"] in {"ambiguous", "residual"}
                and os.path.realpath(item["path"]) == os.path.realpath(binary)
                for item in report["findings"]
            )
        )

    def test_unproven_same_name_with_lease_is_still_preserved(self) -> None:
        binary = self.write(
            self.home / "personal" / "codex-patched",
            b"#!/bin/sh\necho personal\n",
            0o755,
        )
        lease = self.write(
            Path(str(binary) + ".lease"),
            json.dumps(
                {"version": 1, "pid": 99999999, "heartbeat_unix": int(time.time())}
            ).encode(),
        )

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(binary.exists())
        self.assertTrue(lease.exists())
        self.assertTrue(any(item["status"] == "ambiguous" for item in report["findings"]))

    def test_unrelated_project_directory_named_codex_proxy_is_not_reserved(self) -> None:
        binary = self.write(
            self.home / "project" / "codex-proxy" / "codex-patched",
            b"#!/bin/sh\necho project fixture\n",
            0o755,
        )

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(binary.exists())
        self.assertTrue(any(item["status"] == "ambiguous" for item in report["findings"]))

    def test_unknown_patch_history_schema_is_not_treated_as_claude(self) -> None:
        target = self.write(self.home / "bin" / "tool", b"patched", 0o755)
        backup = self.write(
            target.with_name(target.name + ".claude-proxy.bak"), b"original", 0o755
        )
        history = self.write(
            self.home / "project" / "patch_history.json",
            json.dumps(
                {
                    "version": 2,
                    "entries": [
                        {
                            "path": str(target),
                            "specsSha256": "a" * 64,
                            "patchedSha256": digest(target.read_bytes()),
                        }
                    ],
                }
            ).encode(),
        )

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertEqual(target.read_bytes(), b"patched")
        self.assertEqual(backup.read_bytes(), b"original")
        self.assertTrue(history.exists())

    def test_proxy_named_project_history_is_not_trusted_for_cleanup(self) -> None:
        target = self.write(self.home / "bin" / "personal-codex", b"personal", 0o755)
        history = self.write(
            self.home / "project" / "codex-proxy" / "patch_history.json",
            json.dumps(
                {
                    "version": 1,
                    "entries": [
                        {
                            "path": str(target),
                            "patchedSha256": digest(target.read_bytes()),
                        }
                    ],
                }
            ).encode(),
        )

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertEqual(target.read_bytes(), b"personal")
        self.assertTrue(history.exists())
        self.assertTrue(
            any(
                item["kind"] == "patch_history" and item["status"] == "ambiguous"
                for item in report["findings"]
            )
        )

    def test_current_user_tmp_codex_binary_and_requirements_are_removed(self) -> None:
        binary = self.write(
            self.tmp
            / f"codex-proxy-yolo-uid-{self.uid}"
            / "codex-patched-session",
            b"#!/bin/sh\n",
            0o755,
        )
        lease = self.write(
            Path(str(binary) + ".lease"),
            json.dumps(
                {"version": 1, "pid": 99999999, "heartbeat_unix": int(time.time())}
            ).encode(),
        )
        requirements = self.write(
            self.tmp / "cx123456-abcd" / "reqs.toml",
            next(iter(cleanup.LEGACY_REQUIREMENTS)),
        )

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertFalse(binary.exists())
        self.assertFalse(lease.exists())
        self.assertFalse(requirements.exists())

    def test_shared_writable_tmp_helper_directory_is_never_cleaned(self) -> None:
        root = self.tmp / f"codex-proxy-yolo-uid-{self.uid}"
        binary = self.write(root / "codex-patched-session", b"#!/bin/sh\n", 0o755)
        root.chmod(0o777)

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(binary.exists())
        self.assertTrue(
            any(item["status"] in {"unsafe", "residual"} for item in report["findings"])
        )

    def test_unknown_tmp_requirements_are_not_deleted(self) -> None:
        requirements = self.write(
            self.tmp / "cx123456-abcd" / "reqs.toml", b"user content\n"
        )

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertEqual(requirements.read_bytes(), b"user content\n")

    def test_filesystem_snapshot_namespace_is_not_scanned_or_cleaned(self) -> None:
        snapshot_binary = self.write(
            self.home
            / ".snapshot"
            / "nightly.2026-06-30"
            / ".config"
            / "codex-proxy"
            / "codex-patched-old",
            b"#!/bin/sh\n# /tmp/cxreq/requirements.toml\n",
            0o755,
        )

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertTrue(snapshot_binary.exists())

    def test_home_path_through_parent_alias_uses_one_physical_namespace(self) -> None:
        physical_parent = self.root / "physical-parent"
        physical_home = physical_parent / "home"
        physical_home.mkdir(parents=True, mode=0o700)
        alias_parent = self.root / "alias-parent"
        alias_parent.symlink_to(physical_parent, target_is_directory=True)
        aliased_home = alias_parent / "home"
        binary = self.write(
            aliased_home / ".config" / "codex-proxy" / "codex-patched-alias",
            b"#!/bin/sh\n",
            0o755,
        )
        cleaner = cleanup.PatchedBinaryCleaner(
            home=aliased_home,
            tmp_root=self.tmp,
            uid=self.uid,
            purge=True,
            settle_seconds=0,
        )

        report = cleaner.run()

        self.assertTrue(report["clean"], report)
        self.assertFalse(binary.exists())
        paths = [item["path"] for item in report["findings"]]
        self.assertEqual(len(paths), len(set(paths)), paths)

    def _write_yolo_root(self, base: Path) -> Path:
        mirror_dir = (
            base
            / "claude-proxy"
            / "hosts"
            / "host-a"
            / "yolo-patches"
            / ("yolo-" + "a" * 32)
        )
        executable = self.write(mirror_dir / "claude", b"patched mirror", 0o755)
        manifest = {
            "version": 1,
            "sourcePath": str(self.home / ".local" / "bin" / "claude"),
            "sourceSize": 8,
            "sourceSha256": "1" * 64,
            "specsSha256": "2" * 64,
            "patchedSha256": digest(executable.read_bytes()),
            "proxyVersion": "v0.0.75",
            "goos": "linux",
            "goarch": "amd64",
            "executable": "claude",
            "createdAt": "2026-01-01T00:00:00Z",
        }
        self.write(mirror_dir / "manifest.json", json.dumps(manifest).encode())
        self.write(mirror_dir / "active.999.deadbeef.json", b"{}")
        return mirror_dir.parent

    def _write_glibc_mirror_root(self, base: Path) -> Path:
        key = "b" * 64 + "-patched-1234567890abcdef"
        root = base / "claude-proxy" / "hosts" / "host-a" / "claude"
        self.write(root / key / "claude", b"glibc mirror", 0o755)
        return root

    def test_home_claude_mirrors_are_removed_but_runtime_cache_is_preserved(self) -> None:
        yolo = self._write_yolo_root(self.home / ".cache")
        glibc = self._write_glibc_mirror_root(self.home / ".cache")
        self.write(
            glibc
            / ("b" * 64 + "-patched-1234567890abcdef")
            / "claude.claude-proxy.bak",
            b"mirror-local backup",
            0o755,
        )
        runtime = self.write(
            self.home
            / ".cache"
            / "claude-proxy"
            / "hosts"
            / "host-a"
            / "glibc-compat"
            / "v1"
            / "runtime"
            / "libc.so.6",
            b"runtime",
        )
        recovery = self.write(
            self.home
            / ".cache"
            / "claude-proxy"
            / "hosts"
            / "host-a"
            / "install-recovery"
            / "claude",
            b"recovery",
            0o755,
        )

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertFalse(yolo.exists())
        self.assertFalse(glibc.exists())
        self.assertTrue(runtime.exists())
        self.assertTrue(recovery.exists())

    def test_user_owned_symlinked_cache_is_a_trusted_home_extension(self) -> None:
        scratch_cache = self.root / "scratch-user" / ".cache"
        scratch_cache.mkdir(parents=True, mode=0o775)
        scratch_cache.chmod(0o775)
        (self.home / ".cache").symlink_to(scratch_cache, target_is_directory=True)
        yolo = self._write_yolo_root(scratch_cache)
        unrelated = self.write(scratch_cache / "keep.txt", b"keep")

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertFalse(yolo.exists())
        self.assertTrue((self.home / ".cache").is_symlink())
        self.assertEqual(unrelated.read_bytes(), b"keep")

    def test_physical_history_target_under_symlinked_local_is_in_home_scope(self) -> None:
        physical_scratch = self.root / "physical-scratch"
        alias_scratch = self.root / "alias-scratch"
        physical_scratch.mkdir()
        alias_scratch.symlink_to(physical_scratch, target_is_directory=True)
        scratch_local = alias_scratch / ".local"
        scratch_local.mkdir(parents=True)
        (self.home / ".local").symlink_to(scratch_local, target_is_directory=True)
        original = b"official Claude from scratch home"
        patched = b"patched Claude from scratch home!"
        target = self.write(
            scratch_local / "share" / "claude" / "versions" / "2.1.193",
            patched,
            0o755,
        )
        backup = self.write(
            target.with_name(target.name + ".claude-proxy.bak"), original, 0o755
        )
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(target),
                    "origSha256": digest(original),
                    "patchedSha256": digest(patched),
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
        )

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertEqual(target.read_bytes(), original)
        self.assertFalse(backup.exists())
        self.assertFalse(history.exists())
        self.assertTrue((self.home / ".local").is_symlink())

    def test_alias_fallback_preserves_symlink_below_trusted_root(self) -> None:
        physical_scratch = self.root / "physical-scratch"
        alias_scratch = self.root / "alias-scratch"
        scratch_local = physical_scratch / ".local"
        scratch_local.mkdir(parents=True)
        alias_scratch.symlink_to(physical_scratch, target_is_directory=True)
        (self.home / ".local").symlink_to(scratch_local, target_is_directory=True)
        outside = self.root / "outside"
        outside.mkdir()
        target = self.write(outside / "codex-patched", b"#!/bin/sh\n", 0o755)
        (scratch_local / "escape").symlink_to(outside, target_is_directory=True)
        cleaner = self.cleaner(purge=True)
        cleaner.validate_roots()

        canonical = cleaner.canonicalize_path(
            alias_scratch / ".local" / "escape" / target.name
        )
        trusted_local = next(
            root
            for root in cleaner.trusted_home_roots
            if root.logical == self.home / ".local"
        )

        # Compare against the registered physical root without resolving the
        # malicious suffix: resolving the complete expectation would follow
        # "escape" and stop testing that ensure_safe_chain can still see it.
        self.assertEqual(canonical, trusted_local.physical / "escape" / target.name)
        with self.assertRaises(cleanup.SafetyError):
            cleaner.ensure_safe_chain(canonical, leaf_kind="file")
        self.assertTrue(target.exists())

    def test_changed_home_extension_symlink_aborts_before_deletion(self) -> None:
        first_cache = self.root / "first" / ".cache"
        second_cache = self.root / "second" / ".cache"
        first_cache.mkdir(parents=True)
        second_cache.mkdir(parents=True)
        link = self.home / ".cache"
        link.symlink_to(first_cache, target_is_directory=True)
        yolo = self._write_yolo_root(first_cache)
        cleaner = self.cleaner(purge=True, verify_passes=1)
        cleaner.validate_roots()
        link.unlink()
        link.symlink_to(second_cache, target_is_directory=True)

        cleaner.one_pass()
        report = cleaner.report()

        self.assertFalse(report["clean"])
        self.assertTrue(yolo.exists())
        self.assertTrue(any(item["status"] == "unsafe" for item in report["findings"]))

    def test_home_extension_cannot_point_at_temporary_root(self) -> None:
        (self.home / ".cache").symlink_to(self.tmp, target_is_directory=True)

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(any(item["status"] == "unsafe" for item in report["findings"]))

    def test_tmp_claude_mirror_requires_manifest_or_glibc_key(self) -> None:
        invalid = (
            self.tmp
            / "claude-proxy"
            / "hosts"
            / "host-a"
            / "yolo-patches"
            / ("yolo-" + "c" * 32)
        )
        self.write(invalid / "claude", b"unknown", 0o755)

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(invalid.exists())

        self.write(
            invalid / "manifest.json",
            json.dumps(
                {
                    "version": 1,
                    "patchedSha256": "d" * 64,
                    "executable": "claude",
                }
            ).encode(),
        )
        second = self.cleaner(purge=True).run()
        self.assertTrue(second["clean"], second)
        self.assertFalse(invalid.parent.exists())

    def test_symlinked_mirror_root_never_traverses_or_deletes_target(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        sentinel = self.write(outside / "keep", b"keep")
        host = self.home / ".cache" / "claude-proxy" / "hosts" / "host-a"
        host.mkdir(parents=True)
        (host / "yolo-patches").symlink_to(outside, target_is_directory=True)

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertEqual(sentinel.read_bytes(), b"keep")
        self.assertTrue((host / "yolo-patches").is_symlink())

    def test_held_claude_mirror_lock_blocks_deletion(self) -> None:
        try:
            import fcntl
        except ImportError:
            self.skipTest("requires fcntl")
        yolo = self._write_yolo_root(self.home / ".cache")
        lock = yolo / ".lock"
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            report = self.cleaner(purge=True).run()
            self.assertFalse(report["clean"])
            self.assertTrue(yolo.exists())
            self.assertTrue(
                any(item["status"] == "active_process" for item in report["findings"])
            )
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def test_claude_v1_timestamped_backup_restores_in_place_patch(self) -> None:
        original = b"#!/bin/sh\necho original\n"
        patched = b"#!/bin/sh\necho patched!\n"
        target = self.write(self.home / ".local" / "bin" / "claude", patched, 0o755)
        backup = self.write(
            target.with_name(target.name + ".claude-proxy.123456789.bak"),
            original,
            0o755,
        )
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(target),
                    "origSha256": digest(original),
                    "patchedSha256": digest(patched),
                    "proxyVersion": "v0.0.5",
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
        )

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertEqual(target.read_bytes(), original)
        self.assertFalse(backup.exists())
        self.assertFalse(history.exists())

    def test_claude_v2_stable_backup_restores_in_place_patch(self) -> None:
        original = b"official claude bytes"
        patched = b"patched claude bytes!"
        target = self.write(self.home / "bin" / "claude", patched, 0o755)
        backup = self.write(
            target.with_name(target.name + ".claude-proxy.bak"), original, 0o755
        )
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(target),
                    "specsSha256": "a" * 64,
                    "patchedSha256": digest(patched),
                    "proxyVersion": "v0.0.74",
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
            version=2,
        )

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertEqual(target.read_bytes(), original)
        self.assertFalse(backup.exists())
        self.assertFalse(history.exists())

    def test_confirmed_in_place_patch_without_backup_requires_repair(self) -> None:
        patched = b"patched with missing backup"
        target = self.write(self.home / "bin" / "claude", patched, 0o755)
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(target),
                    "specsSha256": "a" * 64,
                    "patchedSha256": digest(patched),
                    "proxyVersion": "v0.0.74",
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
            version=2,
        )

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertEqual(target.read_bytes(), patched)
        self.assertTrue(history.exists())
        self.assertTrue(
            any(item["status"] == "repair_required" for item in report["findings"])
        )

    def test_old_in_place_patch_without_backup_uses_hash_verified_original(self) -> None:
        original = b"official old Claude"
        patched = b"patched old Claude!"
        target = self.write(self.home / "bin" / "claude", patched, 0o755)
        source = self.write(self.root / "trusted-original", original, 0o755)
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(target),
                    "origSha256": digest(original),
                    "patchedSha256": digest(patched),
                    "proxyVersion": "v0.0.26",
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
        )

        report = self.cleaner(
            purge=True, claude_originals={target: source}
        ).run()

        self.assertTrue(report["clean"], report)
        self.assertEqual(target.read_bytes(), original)
        self.assertEqual(source.read_bytes(), original)
        self.assertFalse(history.exists())

    def test_old_in_place_patch_rejects_wrong_explicit_original(self) -> None:
        original = b"official old Claude"
        patched = b"patched old Claude!"
        target = self.write(self.home / "bin" / "claude", patched, 0o755)
        wrong = self.write(self.root / "wrong-original", b"not official", 0o755)
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(target),
                    "origSha256": digest(original),
                    "patchedSha256": digest(patched),
                    "proxyVersion": "v0.0.26",
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
        )

        report = self.cleaner(
            purge=True, claude_originals={target: wrong}
        ).run()

        self.assertFalse(report["clean"])
        self.assertEqual(target.read_bytes(), patched)
        self.assertTrue(history.exists())
        self.assertTrue(
            any(item["status"] == "repair_required" for item in report["findings"])
        )

    def test_already_restored_target_removes_stale_backup_and_history(self) -> None:
        original = b"original"
        patched = b"patched!"
        target = self.write(self.home / "bin" / "claude", original, 0o755)
        backup = self.write(
            target.with_name(target.name + ".claude-proxy.bak"), original, 0o755
        )
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(target),
                    "origSha256": digest(original),
                    "patchedSha256": digest(patched),
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
        )

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertEqual(target.read_bytes(), original)
        self.assertFalse(backup.exists())
        self.assertFalse(history.exists())

    def test_changed_claude_target_preserves_recovery_backup_and_history(self) -> None:
        original = b"original"
        patched = b"patched!"
        externally_updated = b"new official release"
        target = self.write(self.home / "bin" / "claude", externally_updated, 0o755)
        backup = self.write(
            target.with_name(target.name + ".claude-proxy.bak"), original, 0o755
        )
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(target),
                    "origSha256": digest(original),
                    "patchedSha256": digest(patched),
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
        )

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertEqual(target.read_bytes(), externally_updated)
        self.assertEqual(backup.read_bytes(), original)
        self.assertTrue(history.exists())
        self.assertTrue(any(item["status"] == "ambiguous" for item in report["findings"]))

    def test_atomic_restore_rejects_changed_source_before_replacing_target(self) -> None:
        patched = b"patched target"
        source = self.write(self.home / "backup", b"unexpected source", 0o755)
        target = self.write(self.home / "bin" / "claude", patched, 0o755)
        cleaner = self.cleaner(purge=True)
        cleaner.validate_roots()
        target_identity = cleaner.ensure_safe_chain(target, leaf_kind="file")

        with self.assertRaises(cleanup.SafetyError):
            cleaner.atomic_restore(
                target,
                source,
                target_identity,
                expected_source_hash=digest(b"expected original"),
            )

        self.assertEqual(target.read_bytes(), patched)

    def test_symlinked_history_lock_cannot_redirect_cleanup(self) -> None:
        original = b"original"
        patched = b"patched!"
        target = self.write(self.home / "bin" / "claude", patched, 0o755)
        self.write(target.with_name(target.name + ".claude-proxy.bak"), original, 0o755)
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(target),
                    "origSha256": digest(original),
                    "patchedSha256": digest(patched),
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
        )
        sentinel = self.write(self.root / "outside-lock-target", b"do not touch")
        Path(str(history) + ".lock").symlink_to(sentinel)

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertEqual(sentinel.read_bytes(), b"do not touch")
        self.assertEqual(target.read_bytes(), original)
        self.assertTrue(history.exists())

    def test_orphan_claude_backup_is_preserved_as_ambiguous(self) -> None:
        backup = self.write(
            self.home / "bin" / "claude.claude-proxy.bak", b"possible original", 0o755
        )

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(backup.exists())
        self.assertTrue(any(item["status"] == "ambiguous" for item in report["findings"]))

    def test_history_target_outside_home_is_ignored(self) -> None:
        outside = self.write(self.root / "outside-claude", b"patched", 0o755)
        history = self.write_history(
            "claude",
            [
                {
                    "path": str(outside),
                    "origSha256": "1" * 64,
                    "patchedSha256": digest(outside.read_bytes()),
                    "patchedAt": "2026-01-01T00:00:00Z",
                }
            ],
        )

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertTrue(outside.exists())
        self.assertTrue(history.exists())
        self.assertTrue(any(item["status"] == "ignored" for item in report["findings"]))

    def test_hard_linked_candidate_is_never_removed(self) -> None:
        binary = self.write(
            self.home / ".config" / "codex-proxy" / "codex-patched-linked",
            b"#!/bin/sh\n",
            0o755,
        )
        second = self.home / "second-link"
        os.link(binary, second)

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(binary.exists())
        self.assertTrue(second.exists())
        self.assertTrue(
            any(item["status"] in {"unsafe", "residual"} for item in report["findings"])
        )

    def test_active_candidate_is_reported_without_killing_it(self) -> None:
        sleep = shutil.which("sleep")
        if not sleep or not Path("/proc").is_dir():
            self.skipTest("requires sleep and procfs")
        binary = self.home / ".config" / "codex-proxy" / "codex-patched-active"
        binary.parent.mkdir(parents=True)
        shutil.copy2(sleep, binary)
        binary.chmod(0o755)
        process = subprocess.Popen([str(binary), "30"])
        try:
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    if os.stat(f"/proc/{process.pid}/exe").st_ino == os.stat(binary).st_ino:
                        break
                except OSError:
                    pass
                time.sleep(0.01)
            report = self.cleaner(purge=True).run()
            self.assertFalse(report["clean"])
            self.assertIsNone(process.poll())
            self.assertTrue(binary.exists())
            self.assertTrue(
                any(item["status"] == "active_process" for item in report["findings"])
            )
        finally:
            process.terminate()
            process.wait(timeout=5)

    def test_terminate_active_only_stops_exact_candidate_then_cleans_it(self) -> None:
        sleep = shutil.which("sleep")
        if not sleep or not Path("/proc").is_dir():
            self.skipTest("requires sleep and procfs")
        binary = self.home / ".config" / "codex-proxy" / "codex-patched-active"
        binary.parent.mkdir(parents=True)
        shutil.copy2(sleep, binary)
        binary.chmod(0o755)
        process = subprocess.Popen([str(binary), "30"])
        waiter = threading.Thread(target=process.wait, daemon=True)
        waiter.start()
        try:
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    if os.stat(f"/proc/{process.pid}/exe").st_ino == os.stat(binary).st_ino:
                        break
                except OSError:
                    pass
                time.sleep(0.01)
            report = self.cleaner(purge=True, terminate_active=True).run()
            waiter.join(timeout=5)
            self.assertTrue(report["clean"], report)
            self.assertIsNotNone(process.returncode)
            self.assertFalse(binary.exists())
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    @unittest.skipUnless(
        getattr(os, "geteuid", lambda: -1)() == 0,
        "requires root to create foreign-owned fixture",
    )
    def test_foreign_owned_tmp_candidate_is_never_removed(self) -> None:
        binary = self.write(
            self.tmp
            / f"codex-proxy-yolo-uid-{self.uid}"
            / "codex-patched-foreign",
            b"#!/bin/sh\n",
            0o755,
        )
        os.chown(binary, 65534, 65534)

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(binary.exists())
        self.assertTrue(
            any(item["status"] in {"unsafe", "residual"} for item in report["findings"])
        )

    @unittest.skipUnless(
        getattr(os, "geteuid", lambda: -1)() == 0,
        "requires root to create foreign-owned requirements fixture",
    )
    def test_foreign_owned_tmp_requirements_are_ignored_without_read_error(self) -> None:
        directory = self.tmp / "cx123456-abcd"
        requirements = self.write(
            directory / "reqs.toml", next(iter(cleanup.LEGACY_REQUIREMENTS))
        )
        os.chown(requirements, 65534, 65534)
        os.chown(directory, 65534, 65534)
        directory.chmod(0o700)

        report = self.cleaner(purge=True).run()

        self.assertTrue(report["clean"], report)
        self.assertTrue(requirements.exists())
        self.assertFalse(
            any(item["kind"] == "codex_requirements" for item in report["findings"])
        )

    @unittest.skipUnless(
        getattr(os, "geteuid", lambda: -1)() == 0,
        "requires root to create foreign-owned symlink fixture",
    )
    def test_foreign_owned_home_extension_symlink_is_rejected(self) -> None:
        scratch_cache = self.root / "scratch-user" / ".cache"
        scratch_cache.mkdir(parents=True)
        link = self.home / ".cache"
        link.symlink_to(scratch_cache, target_is_directory=True)
        os.lchown(link, 65534, 65534)

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(link.is_symlink())
        self.assertTrue(any(item["status"] == "unsafe" for item in report["findings"]))

    @unittest.skipUnless(
        getattr(os, "geteuid", lambda: -1)() == 0,
        "requires root to create foreign-owned mirror fixture",
    )
    def test_foreign_owned_file_blocks_symlinked_mirror_tree_cleanup(self) -> None:
        scratch_cache = self.root / "scratch-user" / ".cache"
        scratch_cache.mkdir(parents=True)
        (self.home / ".cache").symlink_to(scratch_cache, target_is_directory=True)
        yolo = self._write_yolo_root(scratch_cache)
        foreign = self.write(yolo / "foreign", b"foreign")
        os.chown(foreign, 65534, 65534)

        report = self.cleaner(purge=True).run()

        self.assertFalse(report["clean"])
        self.assertTrue(yolo.exists())
        self.assertTrue(foreign.exists())
        self.assertTrue(
            any(item["status"] in {"unsafe", "residual"} for item in report["findings"])
        )


@unittest.skipUnless(
    os.name == "posix"
    and hasattr(os, "geteuid")
    and os.environ.get("PATCH_CLEANUP_CLI_INTEGRATION") == "1",
    "Docker-only real HOME integration",
)
class CleanupPatchedBinariesCLIIntegrationTests(unittest.TestCase):
    def test_cli_cleans_real_home_and_current_uid_tmp_without_touching_unrelated(self) -> None:
        import pwd

        uid = os.geteuid()
        home = Path(pwd.getpwuid(uid).pw_dir)
        config = home / ".config" / "codex-proxy"
        config.mkdir(parents=True, exist_ok=True)
        home_binary = config / "codex-patched-cli"
        home_binary.write_bytes(b"#!/bin/sh\n")
        home_binary.chmod(0o755)

        tmp_dir = Path("/tmp") / f"codex-proxy-yolo-uid-{uid}"
        tmp_dir.mkdir(mode=0o755, exist_ok=True)
        tmp_binary = tmp_dir / "codex-patched-cli"
        tmp_binary.write_bytes(b"#!/bin/sh\n")
        tmp_binary.chmod(0o755)

        claude_original = b"official old Claude CLI fixture"
        claude_patched = b"patched old Claude CLI fixture!"
        claude_target = home / "bin" / "claude"
        claude_target.parent.mkdir(parents=True, exist_ok=True)
        claude_target.write_bytes(claude_patched)
        claude_target.chmod(0o755)
        claude_source = home / "official-claude-source"
        claude_source.write_bytes(claude_original)
        claude_source.chmod(0o755)
        claude_history = home / ".config" / "claude-proxy" / "patch_history.json"
        claude_history.parent.mkdir(parents=True, exist_ok=True)
        claude_history.write_text(
            json.dumps(
                {
                    "version": 1,
                    "entries": [
                        {
                            "path": str(claude_target),
                            "origSha256": digest(claude_original),
                            "patchedSha256": digest(claude_patched),
                            "proxyVersion": "v0.0.26",
                            "patchedAt": "2026-01-01T00:00:00Z",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        unrelated = home / "keep-me.txt"
        unrelated.write_text("keep", encoding="utf-8")

        process = subprocess.run(
            [
                sys.executable,
                "-B",
                str(SCRIPT),
                "--purge",
                "--settle-seconds",
                "0",
                "--claude-original",
                f"{claude_target}={claude_source}",
                "--json",
                "-",
            ],
            text=True,
            capture_output=True,
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        report = json.loads(process.stdout)
        self.assertTrue(report["clean"], report)
        self.assertFalse(home_binary.exists())
        self.assertFalse(tmp_binary.exists())
        self.assertEqual(claude_target.read_bytes(), claude_original)
        self.assertEqual(claude_source.read_bytes(), claude_original)
        self.assertFalse(claude_history.exists())
        self.assertEqual(unrelated.read_text(encoding="utf-8"), "keep")


if __name__ == "__main__":
    unittest.main()
