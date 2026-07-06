#!/usr/bin/env python3
"""Audit and remove legacy binaries created by codex-helper/claude-helper.

The cleaner is deliberately fail-closed:

* ordinary files are only removed when helper provenance can be established;
* a Claude executable patched in-place is restored, never deleted;
* filesystem mutations are limited to validated current-user home/XDG roots
  and current-user-owned helper artifacts below /tmp;
* symlink traversal, ownership changes, hard links, and inode replacement are
  treated as blockers rather than guessed through.

Run without --purge to audit.  A zero exit status means that the selected
scope is clean, not merely that the cleaner did not encounter an exception.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import re
import signal
import stat
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

try:
    import pwd
except ImportError:  # pragma: no cover - POSIX-only runtime dependency.
    pwd = None  # type: ignore[assignment]

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows CLI exits before this matters.
    fcntl = None  # type: ignore[assignment]


CODEX_BINARY_RE = re.compile(r"^codex-patched(?:-[A-Za-z0-9._-]+)?(?:\.exe)?$")
CODEX_REQ_DIR_RE = re.compile(r"^cx[0-9a-f]{6}-[0-9a-f]{4}$")
CLAUDE_OLD_BACKUP_RE = re.compile(r"^.+\.claude-proxy\.\d+\.bak$")
YOLO_DIR_RE = re.compile(r"^yolo-[0-9a-f]{32}$")
GLIBC_MIRROR_RE = re.compile(
    r"^[0-9a-f]{64}-(?:patched|wrapper)-[A-Za-z0-9._-]+$"
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

LEGACY_REQUIREMENTS = {
    (
        'allowed_approval_policies = ["never", "on-request", "on-failure", "untrusted"]\n'
        'allowed_sandbox_modes = ["danger-full-access", "workspace-write", "read-only"]\n'
    ).encode(),
    (
        'allowed_approval_policies = ["never", "on-request", "on-failure", "untrusted"]\n'
        'allowed_approval_policiez = ["never", "on-request", "on-failure", "untrusted"]\n'
        'allowed_sandbox_modes = ["danger-full-access", "workspace-write", "read-only"]\n'
        'allowed_sandbox_modez = ["danger-full-access", "workspace-write", "read-only"]\n'
    ).encode(),
}

CODEX_MARKERS = (
    b"/tmp/cxreq/requirements.toml",
    b"allowed_approval_policiez",
    b"allowed_sandbox_modez",
)
CODEX_HASHED_REQ_RE = re.compile(rb"/tmp/cx[0-9a-f]{6}-[0-9a-f]{4}/reqs\.toml")

ARTIFACT_STATUSES = {
    "would_remove",
    "would_restore",
    "removed",
    "restored",
    "ambiguous",
    "active_process",
    "repair_required",
    "unsafe",
    "error",
    "residual",
}
BLOCKING_STATUSES = {
    "ambiguous",
    "active_process",
    "repair_required",
    "unsafe",
    "error",
    "residual",
}


@dataclasses.dataclass(frozen=True)
class FileIdentity:
    device: int
    inode: int
    mode: int
    uid: int
    links: int
    size: int

    @classmethod
    def from_stat(cls, info: os.stat_result) -> "FileIdentity":
        return cls(
            device=info.st_dev,
            inode=info.st_ino,
            mode=info.st_mode,
            uid=info.st_uid,
            links=info.st_nlink,
            size=info.st_size,
        )


@dataclasses.dataclass
class Finding:
    kind: str
    path: str
    status: str
    reason: str
    evidence: list[str] = dataclasses.field(default_factory=list)
    sha256: str = ""
    pid: int = 0

    def as_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        return {key: value for key, value in result.items() if value not in ("", 0, [])}


@dataclasses.dataclass
class HistoryDocument:
    path: Path
    flavor: str
    data: dict[str, Any]
    source_hash: str
    remove_indexes: set[int] = dataclasses.field(default_factory=set)


@dataclasses.dataclass(frozen=True)
class TrustedHomeRoot:
    logical: Path
    physical: Path
    physical_identity: FileIdentity
    symlinks: tuple[tuple[Path, FileIdentity, str], ...] = ()


class SafetyError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def lexical_beneath(path: Path, root: Path) -> bool:
    try:
        return os.path.commonpath((str(path), str(root))) == str(root)
    except ValueError:
        return False


def normalized_absolute(path: os.PathLike[str] | str) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def mode_is_executable(info: os.stat_result, path: Path) -> bool:
    if info.st_mode & 0o111:
        return True
    try:
        with path.open("rb") as stream:
            magic = stream.read(4)
    except OSError:
        return False
    return magic.startswith((b"MZ", b"\x7fELF", b"#!")) or magic in {
        b"\xcf\xfa\xed\xfe",
        b"\xca\xfe\xba\xbe",
        b"\xfe\xed\xfa\xcf",
    }


def contains_codex_patch_marker(path: Path) -> bool:
    overlap = b""
    try:
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    return False
                data = overlap + chunk
                if any(marker in data for marker in CODEX_MARKERS):
                    return True
                if CODEX_HASHED_REQ_RE.search(data):
                    return True
                overlap = data[-128:]
    except OSError:
        return False


def parse_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


class PatchedBinaryCleaner:
    """Stateful cleaner with injectable roots for isolated tests."""

    def __init__(
        self,
        *,
        home: Path,
        tmp_root: Path = Path("/tmp"),
        uid: int | None = None,
        purge: bool = False,
        terminate_active: bool = False,
        settle_seconds: float = 0.25,
        verify_passes: int = 2,
        claude_originals: dict[Path, Path] | None = None,
    ) -> None:
        self.home = normalized_absolute(home)
        # macOS exposes system aliases such as /var -> /private/var and
        # /tmp -> /private/tmp. Keep the user-facing HOME spelling, but perform
        # all temporary-root operations against a single physical namespace.
        self.tmp_root = normalized_absolute(os.path.realpath(tmp_root))
        self.uid = os.geteuid() if uid is None else uid
        self.purge = purge
        self.terminate_active = terminate_active
        self.settle_seconds = max(0.0, settle_seconds)
        self.verify_passes = max(1, verify_passes)
        self.claude_originals = {
            normalized_absolute(target): normalized_absolute(source)
            for target, source in (claude_originals or {}).items()
        }
        self.trusted_home_roots: list[TrustedHomeRoot] = []
        self.findings: list[Finding] = []
        self.histories: list[HistoryDocument] = []
        self.mirror_roots: set[Path] = set()
        self.handled_paths: set[Path] = set()
        self.history_paths: set[Path] = set()

    def add(
        self,
        kind: str,
        path: Path,
        status: str,
        reason: str,
        evidence: Iterable[str] = (),
        *,
        digest: str = "",
        pid: int = 0,
    ) -> None:
        self.findings.append(
            Finding(
                kind=kind,
                path=str(path),
                status=status,
                reason=reason,
                evidence=sorted(set(evidence)),
                sha256=digest,
                pid=pid,
            )
        )

    def validate_roots(self) -> None:
        if self.home == Path("/"):
            raise SafetyError("refuse to use / as home")
        home_info = os.lstat(self.home)
        if stat.S_ISLNK(home_info.st_mode) or not stat.S_ISDIR(home_info.st_mode):
            raise SafetyError(f"home is not a physical directory: {self.home}")
        if home_info.st_uid != self.uid:
            raise SafetyError(
                f"home owner uid {home_info.st_uid} does not match current uid {self.uid}"
            )
        physical_home = normalized_absolute(os.path.realpath(self.home))
        physical_home_info = os.lstat(physical_home)
        if not self._same_object(physical_home_info, FileIdentity.from_stat(home_info)):
            raise SafetyError(
                f"home alias does not resolve to the validated directory: {self.home}"
            )

        tmp_info = os.lstat(self.tmp_root)
        if stat.S_ISLNK(tmp_info.st_mode) or not stat.S_ISDIR(tmp_info.st_mode):
            raise SafetyError(f"temporary root is not a physical directory: {self.tmp_root}")

        self.trusted_home_roots = [
            TrustedHomeRoot(
                logical=self.home,
                physical=physical_home,
                physical_identity=FileIdentity.from_stat(physical_home_info),
            )
        ]
        standard_roots = [
            self.home / ".cache",
            self.home / ".config",
            self.home / ".local",
            self.home / "Library" / "Caches",
            self.home / "Library" / "Application Support",
        ]
        explicit_roots: list[Path] = []
        for variable in ("XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME"):
            value = os.environ.get(variable, "").strip()
            if value:
                explicit_roots.append(normalized_absolute(value))
        for logical in standard_roots:
            self.register_trusted_home_root(logical, require_matching_basename=True)
        for logical in explicit_roots:
            self.register_trusted_home_root(logical, require_matching_basename=False)

        # Command-line repair mappings may use the logical HOME spelling while
        # patch history records the resolved physical path.
        self.claude_originals = {
            self.canonicalize_path(target): source
            for target, source in self.claude_originals.items()
        }

    def register_trusted_home_root(
        self, logical: Path, *, require_matching_basename: bool
    ) -> None:
        logical = normalized_absolute(logical)
        if not os.path.lexists(logical):
            return
        if any(root.logical == logical for root in self.trusted_home_roots):
            return

        symlinks: list[tuple[Path, FileIdentity, str]] = []
        if lexical_beneath(logical, self.home):
            current = self.home
            for component in logical.relative_to(self.home).parts:
                current = current / component
                info = os.lstat(current)
                if stat.S_ISLNK(info.st_mode):
                    if info.st_uid != self.uid:
                        raise SafetyError(
                            f"HOME extension symlink owner uid {info.st_uid} does not "
                            f"match current uid {self.uid}: {current}"
                        )
                    symlinks.append(
                        (current, FileIdentity.from_stat(info), os.readlink(current))
                    )
        else:
            info = os.lstat(logical)
            if stat.S_ISLNK(info.st_mode):
                if info.st_uid != self.uid:
                    raise SafetyError(
                        f"XDG symlink owner uid {info.st_uid} does not match current "
                        f"uid {self.uid}: {logical}"
                    )
                symlinks.append(
                    (logical, FileIdentity.from_stat(info), os.readlink(logical))
                )

        physical = normalized_absolute(os.path.realpath(logical))
        try:
            physical_info = os.lstat(physical)
        except OSError as error:
            raise SafetyError(f"cannot resolve HOME extension {logical}: {error}") from error
        if stat.S_ISLNK(physical_info.st_mode) or not stat.S_ISDIR(physical_info.st_mode):
            raise SafetyError(f"HOME extension target is not a physical directory: {physical}")
        if physical_info.st_uid != self.uid:
            raise SafetyError(
                f"HOME extension target owner uid {physical_info.st_uid} does not "
                f"match current uid {self.uid}: {physical}"
            )
        dangerous = {
            Path("/"),
            Path("/home"),
            Path("/tmp"),
            Path("/var"),
            Path("/usr"),
            self.tmp_root,
        }
        if physical in dangerous or lexical_beneath(physical, self.tmp_root):
            raise SafetyError(f"HOME extension target is too broad or temporary: {physical}")
        if symlinks and require_matching_basename and physical.name != logical.name:
            raise SafetyError(
                f"HOME extension basename mismatch: {logical} -> {physical}"
            )
        self.trusted_home_roots.append(
            TrustedHomeRoot(
                logical=logical,
                physical=physical,
                physical_identity=FileIdentity.from_stat(physical_info),
                symlinks=tuple(symlinks),
            )
        )

    def verify_trusted_home_root(self, root: TrustedHomeRoot) -> None:
        for link, expected, target in root.symlinks:
            current = os.lstat(link)
            if not self._identity_matches(current, expected) or os.readlink(link) != target:
                raise SafetyError(f"HOME extension symlink changed during cleanup: {link}")
        current_root = os.lstat(root.physical)
        if not self._same_object(current_root, root.physical_identity):
            raise SafetyError(
                f"HOME extension target changed during cleanup: {root.physical}"
            )
        if root.symlinks and normalized_absolute(os.path.realpath(root.logical)) != root.physical:
            raise SafetyError(
                f"HOME extension now resolves to a different target: {root.logical}"
            )

    def canonicalize_path(self, path: Path) -> Path:
        path = normalized_absolute(path)
        logical_matches = [
            root for root in self.trusted_home_roots if lexical_beneath(path, root.logical)
        ]
        if logical_matches:
            root = max(logical_matches, key=lambda item: len(item.logical.parts))
            return root.physical / path.relative_to(root.logical)

        # The usual scan path is already in a registered physical namespace,
        # so keep it allocation- and syscall-light. The fallback below exists
        # for persisted history paths that use another spelling of a stable
        # system alias (for example /var versus /private/var on macOS).
        if lexical_beneath(path, self.tmp_root) or any(
            lexical_beneath(path, root.physical) for root in self.trusted_home_roots
        ):
            return path

        # Do not call realpath() on the whole candidate: that would follow a
        # symlink below HOME and could bypass ensure_safe_chain(). Instead,
        # accept only an existing ancestor that is the exact same directory
        # (device/inode/owner/type) as an already validated trusted root, then
        # retain the lexical suffix so later safety checks still see every
        # symlink, ownership boundary, and nested mount below that root.
        for ancestor in (path, *path.parents):
            try:
                info = os.stat(ancestor)
            except OSError:
                continue
            if not stat.S_ISDIR(info.st_mode):
                continue
            for root in self.trusted_home_roots:
                if self._same_object(info, root.physical_identity):
                    return root.physical / path.relative_to(ancestor)
        return path

    def trusted_home_root_for(self, path: Path) -> TrustedHomeRoot | None:
        path = self.canonicalize_path(path)
        matches = [
            root for root in self.trusted_home_roots if lexical_beneath(path, root.physical)
        ]
        if not matches:
            return None
        return max(matches, key=lambda item: len(item.physical.parts))

    def scope_for(self, path: Path) -> str | None:
        path = self.canonicalize_path(path)
        if self.trusted_home_root_for(path) is not None:
            return "home"
        if lexical_beneath(path, self.tmp_root):
            return "tmp"
        return None

    def _relative_chain(self, path: Path, root: Path) -> Iterator[Path]:
        relative = path.relative_to(root)
        current = root
        for component in relative.parts:
            current = current / component
            yield current

    def ensure_safe_chain(self, path: Path, *, leaf_kind: str) -> FileIdentity:
        path = self.canonicalize_path(path)
        scope = self.scope_for(path)
        if scope is None:
            raise SafetyError("path is outside home and temporary roots")
        trusted_root = self.trusted_home_root_for(path) if scope == "home" else None
        if trusted_root is not None:
            self.verify_trusted_home_root(trusted_root)
        root = trusted_root.physical if trusted_root is not None else self.tmp_root
        previous_device = os.lstat(root).st_dev
        components = list(self._relative_chain(path, root))
        if not components:
            raise SafetyError("refuse to operate on a scope root")
        for index, component in enumerate(components):
            info = os.lstat(component)
            is_leaf = index == len(components) - 1
            if stat.S_ISLNK(info.st_mode):
                raise SafetyError(f"symbolic link in path: {component}")
            if info.st_uid != self.uid:
                raise SafetyError(
                    f"owner uid {info.st_uid} does not match current uid {self.uid}: {component}"
                )
            if info.st_dev != previous_device:
                raise SafetyError(f"nested mount point is not automatically cleaned: {component}")
            previous_device = info.st_dev
            if scope == "tmp" and not is_leaf and info.st_mode & 0o022:
                raise SafetyError(f"shared-writable temporary directory: {component}")
            if not is_leaf and not stat.S_ISDIR(info.st_mode):
                raise SafetyError(f"non-directory parent component: {component}")

        info = os.lstat(path)
        if leaf_kind == "file":
            if not stat.S_ISREG(info.st_mode):
                raise SafetyError("candidate is not a regular file")
            if info.st_nlink != 1:
                raise SafetyError(f"candidate has {info.st_nlink} hard links")
        elif leaf_kind == "directory":
            if not stat.S_ISDIR(info.st_mode):
                raise SafetyError("candidate is not a directory")
        else:
            raise ValueError(f"unknown leaf kind {leaf_kind!r}")
        return FileIdentity.from_stat(info)

    @staticmethod
    def _identity_matches(info: os.stat_result, expected: FileIdentity) -> bool:
        return FileIdentity.from_stat(info) == expected

    @staticmethod
    def _same_object(info: os.stat_result, expected: FileIdentity) -> bool:
        """Compare fields that cannot legitimately change while emptying a directory."""
        return (
            info.st_dev == expected.device
            and info.st_ino == expected.inode
            and info.st_uid == expected.uid
            and stat.S_IFMT(info.st_mode) == stat.S_IFMT(expected.mode)
        )

    def unlink_verified(self, path: Path, expected: FileIdentity) -> None:
        path = self.canonicalize_path(path)
        parent_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        parent_fd = os.open(path.parent, parent_flags)
        try:
            current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not self._identity_matches(current, expected):
                raise SafetyError("candidate identity changed before unlink")
            os.unlink(path.name, dir_fd=parent_fd)
        finally:
            os.close(parent_fd)

    def rmdir_empty_verified(self, path: Path, expected: FileIdentity) -> None:
        path = self.canonicalize_path(path)
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        parent_fd = os.open(path.parent, flags)
        try:
            current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not self._same_object(current, expected):
                raise SafetyError("directory identity changed before rmdir")
            os.rmdir(path.name, dir_fd=parent_fd)
        finally:
            os.close(parent_fd)

    def _snapshot_tree(self, root: Path) -> dict[Path, FileIdentity]:
        root = self.canonicalize_path(root)
        snapshots: dict[Path, FileIdentity] = {}
        root_identity = self.ensure_safe_chain(root, leaf_kind="directory")
        snapshots[Path(".")] = root_identity
        root_device = root_identity.device
        for current, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
            current_path = Path(current)
            kept_dirs: list[str] = []
            for name in dirnames:
                child = current_path / name
                info = os.lstat(child)
                relative = child.relative_to(root)
                if info.st_uid != self.uid:
                    raise SafetyError(f"foreign-owned mirror entry: {child}")
                if stat.S_ISLNK(info.st_mode):
                    snapshots[relative] = FileIdentity.from_stat(info)
                    continue
                if not stat.S_ISDIR(info.st_mode):
                    raise SafetyError(f"unexpected mirror entry type: {child}")
                if info.st_dev != root_device:
                    raise SafetyError(f"nested mount in mirror tree: {child}")
                if self.scope_for(root) == "tmp" and info.st_mode & 0o022:
                    raise SafetyError(f"shared-writable mirror directory: {child}")
                snapshots[relative] = FileIdentity.from_stat(info)
                kept_dirs.append(name)
            dirnames[:] = kept_dirs
            for name in filenames:
                child = current_path / name
                info = os.lstat(child)
                relative = child.relative_to(root)
                if info.st_uid != self.uid:
                    raise SafetyError(f"foreign-owned mirror entry: {child}")
                if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
                    raise SafetyError(f"hard-linked mirror entry: {child}")
                if not (stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)):
                    raise SafetyError(f"unsupported mirror entry type: {child}")
                snapshots[relative] = FileIdentity.from_stat(info)
        return snapshots

    def _remove_tree_fd(
        self,
        fd: int,
        relative: Path,
        snapshots: dict[Path, FileIdentity],
    ) -> None:
        names = os.listdir(fd)
        expected_children = {
            item.parts[len(relative.parts)]
            for item in snapshots
            if item != Path(".")
            and len(item.parts) == len(relative.parts) + 1
            and item.parts[: len(relative.parts)] == relative.parts
        }
        if set(names) != expected_children:
            raise SafetyError("mirror tree changed while it was being removed")
        for name in names:
            child_relative = relative / name if relative != Path(".") else Path(name)
            expected = snapshots[child_relative]
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if not self._identity_matches(info, expected):
                raise SafetyError(f"mirror entry identity changed: {child_relative}")
            if stat.S_ISDIR(info.st_mode):
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
                child_fd = os.open(name, flags, dir_fd=fd)
                try:
                    self._remove_tree_fd(child_fd, child_relative, snapshots)
                finally:
                    os.close(child_fd)
                current = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if not self._same_object(current, expected):
                    raise SafetyError(f"mirror directory identity changed: {child_relative}")
                os.rmdir(name, dir_fd=fd)
            else:
                os.unlink(name, dir_fd=fd)

    def remove_tree_verified(self, root: Path, snapshots: dict[Path, FileIdentity]) -> None:
        root = self.canonicalize_path(root)
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        root_fd = os.open(root, flags)
        try:
            self._remove_tree_fd(root_fd, Path("."), snapshots)
        finally:
            os.close(root_fd)
        parent_fd = os.open(root.parent, flags)
        try:
            current = os.stat(root.name, dir_fd=parent_fd, follow_symlinks=False)
            if not self._same_object(current, snapshots[Path(".")]):
                raise SafetyError("mirror root identity changed before rmdir")
            os.rmdir(root.name, dir_fd=parent_fd)
        finally:
            os.close(parent_fd)

    def process_pids_for_file(self, path: Path) -> list[int]:
        path = self.canonicalize_path(path)
        if not Path("/proc").is_dir():
            return []
        try:
            target = os.stat(path, follow_symlinks=False)
        except OSError:
            return []
        result: list[int] = []
        for entry in os.scandir("/proc"):
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            if self.pid_exec_matches_file(pid, target):
                result.append(pid)
        return sorted(result)

    def pid_exec_matches_file(self, pid: int, target: os.stat_result) -> bool:
        try:
            proc = os.stat(f"/proc/{pid}")
            executable = os.stat(f"/proc/{pid}/exe")
        except OSError:
            return False
        return (
            proc.st_uid == self.uid
            and executable.st_dev == target.st_dev
            and executable.st_ino == target.st_ino
        )

    def process_pids_under(self, root: Path) -> list[int]:
        root = self.canonicalize_path(root)
        if not Path("/proc").is_dir():
            return []
        result: list[int] = []
        for entry in os.scandir("/proc"):
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            if self.pid_exec_is_under(pid, root):
                result.append(pid)
        return sorted(result)

    def pid_exec_is_under(self, pid: int, root: Path) -> bool:
        try:
            if os.stat(f"/proc/{pid}").st_uid != self.uid:
                return False
            target = os.readlink(f"/proc/{pid}/exe")
            if target.endswith(" (deleted)"):
                target = target[: -len(" (deleted)")]
        except OSError:
            return False
        return lexical_beneath(normalized_absolute(target), root)

    def stop_exact_processes(
        self,
        pids: Sequence[int],
        *,
        path: Path | None = None,
        root: Path | None = None,
    ) -> list[int]:
        if (path is None) == (root is None):
            raise ValueError("exactly one process identity scope is required")
        if path is not None:
            target = os.stat(self.canonicalize_path(path), follow_symlinks=False)

            def still_matches(pid: int) -> bool:
                return self.pid_exec_matches_file(pid, target)

        else:
            canonical_root = self.canonicalize_path(root)  # type: ignore[arg-type]

            def still_matches(pid: int) -> bool:
                return self.pid_exec_is_under(pid, canonical_root)

        pids = [
            pid
            for pid in pids
            if pid not in {os.getpid(), os.getppid()} and still_matches(pid)
        ]
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 2.0
        remaining = set(pids)
        while remaining and time.monotonic() < deadline:
            remaining = {pid for pid in remaining if still_matches(pid)}
            if remaining:
                time.sleep(0.05)
        for pid in sorted(remaining):
            if not still_matches(pid):
                continue
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 1.0
        while remaining and time.monotonic() < deadline:
            remaining = {pid for pid in remaining if still_matches(pid)}
            if remaining:
                time.sleep(0.05)
        return sorted(remaining)

    def home_walk(self) -> Iterator[Path]:
        walk_roots: list[TrustedHomeRoot] = []
        for root in sorted(self.trusted_home_roots, key=lambda item: len(item.physical.parts)):
            if any(lexical_beneath(root.physical, existing.physical) for existing in walk_roots):
                continue
            walk_roots.append(root)
        for trusted_root in walk_roots:
            try:
                self.verify_trusted_home_root(trusted_root)
            except (OSError, SafetyError) as error:
                self.add("scan", trusted_root.logical, "unsafe", str(error))
                continue
            root_device = trusted_root.physical_identity.device
            for current, dirnames, filenames in os.walk(
                trusted_root.physical, topdown=True, followlinks=False
            ):
                current_path = Path(current)
                kept: list[str] = []
                for name in dirnames:
                    child = current_path / name
                    if name in {".snapshot", ".snapshots"}:
                        # Filesystem snapshots are historical, often mounted on
                        # different devices, and must never be treated as the
                        # current user's live HOME namespace.
                        continue
                    try:
                        info = os.lstat(child)
                    except OSError as error:
                        self.add("scan", child, "error", f"lstat failed: {error}")
                        continue
                    if stat.S_ISLNK(info.st_mode):
                        continue
                    if info.st_dev != root_device:
                        self.add("scan", child, "unsafe", "nested mount point was not scanned")
                        continue
                    kept.append(name)
                dirnames[:] = kept
                for name in filenames:
                    path = current_path / name
                    try:
                        os.lstat(path)
                    except OSError as error:
                        self.add("scan", path, "error", f"lstat failed: {error}")
                        continue
                    yield path

    def cache_roots(self) -> list[Path]:
        candidates = [self.home / ".cache", self.home / "Library" / "Caches"]
        xdg = os.environ.get("XDG_CACHE_HOME", "").strip()
        if xdg:
            candidate = normalized_absolute(xdg)
            if lexical_beneath(candidate, self.home):
                candidates.append(candidate)
        result: list[Path] = []
        for candidate in candidates:
            candidate = self.canonicalize_path(candidate)
            if candidate not in result:
                result.append(candidate)
        return result

    def helper_mirror_roots(self) -> list[tuple[Path, str]]:
        roots: list[tuple[Path, str]] = []
        bases = [(root / "claude-proxy" / "hosts", "home") for root in self.cache_roots()]
        bases.append((self.tmp_root / "claude-proxy" / "hosts", "tmp"))
        for hosts_root, scope in bases:
            try:
                self.ensure_safe_chain(hosts_root, leaf_kind="directory")
            except FileNotFoundError:
                continue
            except (OSError, SafetyError) as error:
                if os.path.lexists(hosts_root):
                    self.add("claude_mirror", hosts_root, "unsafe", str(error))
                continue
            try:
                hosts = list(os.scandir(hosts_root))
            except FileNotFoundError:
                continue
            except OSError as error:
                self.add("claude_mirror", hosts_root, "error", f"scan failed: {error}")
                continue
            for host in hosts:
                if host.is_symlink():
                    self.add("claude_mirror", Path(host.path), "unsafe", "host root is a symlink")
                    continue
                if not host.is_dir(follow_symlinks=False):
                    continue
                for leaf in ("yolo-patches", "claude"):
                    path = Path(host.path) / leaf
                    if path.exists() or path.is_symlink():
                        roots.append((path, scope))
        return roots

    def tmp_codex_paths(self) -> Iterator[Path]:
        uid_root = self.tmp_root / f"codex-proxy-yolo-uid-{self.uid}"
        if os.path.lexists(uid_root) and not uid_root.is_symlink() and uid_root.is_dir():
            for current, dirnames, filenames in os.walk(uid_root, followlinks=False):
                dirnames[:] = [
                    name for name in dirnames if not (Path(current) / name).is_symlink()
                ]
                for name in filenames:
                    yield Path(current) / name
        try:
            entries = list(os.scandir(self.tmp_root))
        except OSError:
            return
        for entry in entries:
            if entry.is_file(follow_symlinks=False) and CODEX_BINARY_RE.fullmatch(entry.name):
                yield Path(entry.path)

    def discover_histories_and_candidates(
        self,
    ) -> tuple[list[Path], list[Path], list[Path]]:
        histories: list[Path] = []
        codex: list[Path] = []
        backups: list[Path] = []
        for path in self.home_walk():
            name = path.name
            if name == "patch_history.json":
                histories.append(path)
            if CODEX_BINARY_RE.fullmatch(name):
                codex.append(path)
            if name.endswith(".claude-proxy.bak") or CLAUDE_OLD_BACKUP_RE.fullmatch(name):
                backups.append(path)
        codex.extend(self.tmp_codex_paths())
        return sorted(set(histories)), sorted(set(codex)), sorted(set(backups))

    def history_flavor(self, path: Path) -> str:
        # A directory merely named claude-proxy/codex-proxy inside a project is
        # not sufficient provenance for destructive cleanup. Only the helpers'
        # default config roots are implicit; nonstandard config locations must
        # remain ambiguous instead of being guessed through.
        bases = [
            self.home / ".config",
            self.home / "Library" / "Application Support",
        ]
        xdg = os.environ.get("XDG_CONFIG_HOME", "").strip()
        if xdg:
            xdg_base = normalized_absolute(xdg)
            if self.scope_for(xdg_base) == "home":
                bases.append(xdg_base)
        wanted = self.canonicalize_path(path)
        for base in bases:
            base = self.canonicalize_path(base)
            for flavor in ("claude", "codex"):
                candidate = base / f"{flavor}-proxy" / "patch_history.json"
                if wanted == candidate:
                    return flavor
        return "unknown"

    def load_histories(self, paths: Sequence[Path]) -> None:
        for path in paths:
            try:
                identity = self.ensure_safe_chain(path, leaf_kind="file")
                data = parse_json(path)
                if not isinstance(data, dict) or not isinstance(data.get("entries"), list):
                    raise ValueError("expected an object with an entries array")
                digest = sha256_file(path)
            except (OSError, ValueError, json.JSONDecodeError, SafetyError) as error:
                self.add("patch_history", path, "ambiguous", f"cannot safely parse history: {error}")
                continue
            flavor = self.history_flavor(path)
            if flavor == "unknown":
                self.add(
                    "patch_history",
                    path,
                    "ambiguous",
                    "history schema is not tied to a known helper config root",
                    [f"uid={identity.uid}"],
                    digest=digest,
                )
                continue
            self.histories.append(
                HistoryDocument(path=path, flavor=flavor, data=data, source_hash=digest)
            )
            self.history_paths.add(path)

    def history_entries_for(self, flavor: str, path: Path) -> list[tuple[HistoryDocument, int, dict[str, Any]]]:
        result: list[tuple[HistoryDocument, int, dict[str, Any]]] = []
        wanted = self.canonicalize_path(path)
        for document in self.histories:
            if document.flavor != flavor:
                continue
            for index, entry in enumerate(document.data.get("entries", [])):
                if not isinstance(entry, dict):
                    continue
                raw_path = entry.get("path")
                if (
                    isinstance(raw_path, str)
                    and os.path.isabs(raw_path)
                    and self.canonicalize_path(Path(raw_path)) == wanted
                ):
                    result.append((document, index, entry))
        return result

    def mark_history_entries_under(self, root: Path) -> None:
        for document in self.histories:
            if document.flavor != "claude":
                continue
            for index, entry in enumerate(document.data.get("entries", [])):
                if not isinstance(entry, dict):
                    continue
                raw_path = entry.get("path")
                if not isinstance(raw_path, str) or not os.path.isabs(raw_path):
                    continue
                if lexical_beneath(
                    self.canonicalize_path(Path(raw_path)), self.canonicalize_path(root)
                ):
                    document.remove_indexes.add(index)

    def tmp_mirror_has_provenance(self, root: Path) -> bool:
        if root.name == "yolo-patches":
            try:
                children = list(os.scandir(root))
            except OSError:
                return False
            for child in children:
                if not child.is_dir(follow_symlinks=False) or not YOLO_DIR_RE.fullmatch(child.name):
                    continue
                manifest = Path(child.path) / "manifest.json"
                try:
                    data = parse_json(manifest)
                except (OSError, ValueError, json.JSONDecodeError):
                    continue
                if (
                    isinstance(data, dict)
                    and data.get("version") == 1
                    and isinstance(data.get("patchedSha256"), str)
                    and SHA256_RE.fullmatch(data["patchedSha256"])
                    and isinstance(data.get("executable"), str)
                    and Path(data["executable"]).name == data["executable"]
                ):
                    return True
            return False
        if root.name == "claude":
            try:
                return any(
                    child.is_dir(follow_symlinks=False)
                    and GLIBC_MIRROR_RE.fullmatch(child.name)
                    for child in os.scandir(root)
                )
            except OSError:
                return False
        return False

    def process_mirror_root(self, root: Path, scope: str) -> None:
        root = self.canonicalize_path(root)
        evidence = ["claude-helper reserved mirror root", f"scope={scope}"]
        try:
            self.ensure_safe_chain(root, leaf_kind="directory")
        except (OSError, SafetyError) as error:
            self.add("claude_mirror", root, "unsafe", str(error), evidence)
            return
        if scope == "tmp" and not self.tmp_mirror_has_provenance(root):
            self.add(
                "claude_mirror",
                root,
                "ambiguous",
                "temporary mirror root lacks a valid helper manifest or glibc mirror key",
                evidence,
            )
            return
        lock_fd: int | None = None
        root_fd: int | None = None
        if self.purge and fcntl is not None:
            try:
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
                root_fd = os.open(root, flags)
                lock_fd = os.open(
                    ".lock",
                    os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=root_fd,
                )
                lock_info = os.fstat(lock_fd)
                if (
                    lock_info.st_uid != self.uid
                    or not stat.S_ISREG(lock_info.st_mode)
                    or lock_info.st_nlink != 1
                ):
                    raise SafetyError("mirror lock is not a current-user-owned regular file")
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (OSError, BlockingIOError, SafetyError) as error:
                if lock_fd is not None:
                    os.close(lock_fd)
                    lock_fd = None
                if root_fd is not None:
                    os.close(root_fd)
                    root_fd = None
                self.add(
                    "claude_mirror",
                    root,
                    "active_process",
                    f"cannot acquire helper mirror lock: {error}",
                    evidence,
                )
                return
            finally:
                if root_fd is not None:
                    os.close(root_fd)
                    root_fd = None
        try:
            snapshots = self._snapshot_tree(root)
        except (OSError, SafetyError) as error:
            if lock_fd is not None:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            self.add("claude_mirror", root, "unsafe", str(error), evidence)
            return
        pids = self.process_pids_under(root)
        if pids and self.purge and self.terminate_active:
            remaining = self.stop_exact_processes(pids, root=root)
            if remaining:
                self.add(
                    "claude_mirror",
                    root,
                    "active_process",
                    "mirror executable remains active",
                    evidence,
                    pid=remaining[0],
                )
                if lock_fd is not None:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    os.close(lock_fd)
                return
            pids = []
        if pids:
            self.add(
                "claude_mirror",
                root,
                "active_process",
                "mirror executable is active; rerun after it exits or use --terminate-active",
                evidence,
                pid=pids[0],
            )
            if lock_fd is not None:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            return
        if not self.purge:
            self.add("claude_mirror", root, "would_remove", "reserved mirror tree", evidence)
            self.mark_history_entries_under(root)
            return
        try:
            self.remove_tree_verified(root, snapshots)
        except (OSError, SafetyError) as error:
            self.add("claude_mirror", root, "error", f"remove failed: {error}", evidence)
            if lock_fd is not None:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            return
        if lock_fd is not None:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
        self.add("claude_mirror", root, "removed", "reserved mirror tree", evidence)
        self.handled_paths.add(root)
        self.mark_history_entries_under(root)

    @staticmethod
    def backup_target(path: Path) -> Path | None:
        name = path.name
        stable_suffix = ".claude-proxy.bak"
        if name.endswith(stable_suffix):
            return path.with_name(name[: -len(stable_suffix)])
        match = re.match(r"^(?P<base>.+)\.claude-proxy\.\d+\.bak$", name)
        if match:
            return path.with_name(match.group("base"))
        return None

    def backup_candidates(self, target: Path, backups: Sequence[Path]) -> list[Path]:
        return [path for path in backups if self.backup_target(path) == target]

    def atomic_restore(
        self,
        target: Path,
        source: Path,
        target_identity: FileIdentity,
        *,
        expected_source_hash: str,
        explicit_source: bool = False,
    ) -> None:
        target = self.canonicalize_path(target)
        if not explicit_source:
            source = self.canonicalize_path(source)
        if explicit_source:
            source_info = os.lstat(source)
            if stat.S_ISLNK(source_info.st_mode) or not stat.S_ISREG(source_info.st_mode):
                raise SafetyError("explicit Claude original is not a physical regular file")
            source_identity = FileIdentity.from_stat(source_info)
            source_mode = source_info.st_mode
        else:
            source_identity = self.ensure_safe_chain(source, leaf_kind="file")
            source_mode = source_identity.mode
        with source.open("rb") as stream:
            data = stream.read()
        if sha256_bytes(data) != expected_source_hash:
            raise SafetyError("restore source changed after provenance validation")
        current_source = os.lstat(source)
        if not self._identity_matches(current_source, source_identity):
            raise SafetyError("restore source identity changed before staging")
        current = os.lstat(target)
        if not self._identity_matches(current, target_identity):
            raise SafetyError("target identity changed before restore")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.cleanup-", dir=target.parent
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, stat.S_IMODE(source_mode))
            with os.fdopen(descriptor, "wb", closefd=True) as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            descriptor = -1
            if sha256_file(temporary) != sha256_bytes(data):
                raise SafetyError("staged restore hash mismatch")
            current = os.lstat(target)
            if not self._identity_matches(current, target_identity):
                raise SafetyError("target identity changed during restore")
            os.replace(temporary, target)
            directory_fd = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def process_claude_histories(self, backups: Sequence[Path]) -> None:
        mirror_roots = list(self.mirror_roots)
        associated_backups: set[Path] = set()
        for document in self.histories:
            if document.flavor != "claude":
                continue
            entries = document.data.get("entries", [])
            for index, entry in enumerate(entries):
                if not isinstance(entry, dict) or entry.get("failed") is True:
                    continue
                raw_target = entry.get("path")
                patched_hash = str(entry.get("patchedSha256", "")).lower()
                if not isinstance(raw_target, str) or not os.path.isabs(raw_target):
                    continue
                target = self.canonicalize_path(Path(raw_target))
                if any(lexical_beneath(target, root) for root in mirror_roots):
                    continue
                if self.scope_for(target) != "home":
                    self.add(
                        "claude_inplace",
                        target,
                        "ignored",
                        "history target is outside the current user's home",
                        [f"history={document.path}"],
                    )
                    continue
                candidates = self.backup_candidates(target, backups)
                associated_backups.update(candidates)
                if not target.exists():
                    if candidates:
                        self.add(
                            "claude_inplace",
                            target,
                            "ambiguous",
                            "target is missing while helper backup remains",
                            [f"history={document.path}"],
                        )
                    else:
                        document.remove_indexes.add(index)
                    continue
                try:
                    target_identity = self.ensure_safe_chain(target, leaf_kind="file")
                    current_hash = sha256_file(target)
                except (OSError, SafetyError) as error:
                    self.add("claude_inplace", target, "unsafe", str(error))
                    continue
                if not SHA256_RE.fullmatch(patched_hash):
                    self.add(
                        "claude_inplace",
                        target,
                        "ambiguous",
                        "history entry has no valid patched SHA-256",
                        [f"history={document.path}"],
                        digest=current_hash,
                    )
                    continue
                original_hash = str(entry.get("origSha256", "")).lower()
                valid_backups: list[tuple[Path, str, bool]] = []
                for backup in candidates:
                    try:
                        self.ensure_safe_chain(backup, leaf_kind="file")
                        backup_hash = sha256_file(backup)
                    except (OSError, SafetyError):
                        continue
                    if SHA256_RE.fullmatch(original_hash) and backup_hash != original_hash:
                        continue
                    if not original_hash and backup.name != target.name + ".claude-proxy.bak":
                        continue
                    valid_backups.append((backup, backup_hash, True))

                evidence = [f"history={document.path}", f"patched_sha256={patched_hash}"]
                if current_hash == patched_hash:
                    explicit_original = self.claude_originals.get(target)
                    if not valid_backups and explicit_original is not None:
                        try:
                            explicit_info = os.lstat(explicit_original)
                            if stat.S_ISLNK(explicit_info.st_mode) or not stat.S_ISREG(
                                explicit_info.st_mode
                            ):
                                raise SafetyError(
                                    "explicit Claude original is not a physical regular file"
                                )
                            explicit_hash = sha256_file(explicit_original)
                            if not SHA256_RE.fullmatch(original_hash):
                                raise SafetyError(
                                    "history has no origSha256 for validating the explicit original"
                                )
                            if explicit_hash != original_hash:
                                raise SafetyError(
                                    "explicit Claude original does not match history origSha256"
                                )
                            valid_backups.append((explicit_original, explicit_hash, False))
                            evidence.append("explicit original matched history origSha256")
                        except (OSError, SafetyError) as error:
                            self.add(
                                "claude_inplace",
                                target,
                                "repair_required",
                                f"explicit original rejected: {error}",
                                evidence,
                                digest=current_hash,
                            )
                            continue
                    if not valid_backups:
                        self.add(
                            "claude_inplace",
                            target,
                            "repair_required",
                            "confirmed patched target has no trustworthy original backup",
                            evidence,
                            digest=current_hash,
                        )
                        continue
                    distinct_hashes = {digest for _, digest, _ in valid_backups}
                    if len(distinct_hashes) != 1:
                        self.add(
                            "claude_inplace",
                            target,
                            "ambiguous",
                            "multiple backups disagree on the original content",
                            evidence,
                            digest=current_hash,
                        )
                        continue
                    backup, original, remove_source = sorted(
                        valid_backups, key=lambda item: str(item[0])
                    )[0]
                    pids = self.process_pids_for_file(target)
                    if pids and self.purge and self.terminate_active:
                        remaining = self.stop_exact_processes(pids, path=target)
                        if remaining:
                            self.add(
                                "claude_inplace",
                                target,
                                "active_process",
                                "patched target remains active",
                                evidence,
                                pid=remaining[0],
                            )
                            continue
                        pids = []
                    if pids:
                        self.add(
                            "claude_inplace",
                            target,
                            "active_process",
                            "patched target is active",
                            evidence,
                            pid=pids[0],
                        )
                        continue
                    if not self.purge:
                        self.add(
                            "claude_inplace",
                            target,
                            "would_restore",
                            f"restore from {backup}",
                            evidence + [f"original_sha256={original}"],
                            digest=current_hash,
                        )
                        document.remove_indexes.add(index)
                        continue
                    try:
                        self.atomic_restore(
                            target,
                            backup,
                            target_identity,
                            expected_source_hash=original,
                            explicit_source=not remove_source,
                        )
                        if sha256_file(target) != original:
                            raise SafetyError("restored target hash mismatch")
                    except (OSError, SafetyError) as error:
                        self.add(
                            "claude_inplace", target, "error", f"restore failed: {error}", evidence
                        )
                        continue
                    backup_failed = False
                    for candidate, candidate_hash, should_remove in valid_backups:
                        if candidate_hash != original or not should_remove:
                            continue
                        try:
                            identity = self.ensure_safe_chain(candidate, leaf_kind="file")
                            self.unlink_verified(candidate, identity)
                            self.handled_paths.add(candidate)
                        except (OSError, SafetyError) as error:
                            backup_failed = True
                            self.add(
                                "claude_backup",
                                candidate,
                                "error",
                                f"remove after restore failed: {error}",
                            )
                    self.add(
                        "claude_inplace",
                        target,
                        "restored",
                        f"restored from {backup}",
                        evidence + [f"original_sha256={original}"],
                        digest=original,
                    )
                    if not backup_failed:
                        document.remove_indexes.add(index)
                    self.handled_paths.add(target)
                    continue

                # Only a target that exactly matches the recorded original (or
                # an independently verified backup) proves that recovery is no
                # longer needed. If another update changed the target, retain
                # both backup and history instead of destroying the sole route
                # back to the original executable.
                if current_hash == original_hash or any(
                    current_hash == backup_hash for _, backup_hash, _ in valid_backups
                ):
                    reason = "target already contains the original content"
                else:
                    self.add(
                        "claude_inplace",
                        target,
                        "ambiguous",
                        "target changed after the recorded patch; preserving backup and history",
                        evidence,
                        digest=current_hash,
                    )
                    continue
                if not self.purge:
                    self.add(
                        "claude_inplace",
                        target,
                        "would_remove",
                        "remove stale helper backup/history; " + reason,
                        evidence,
                        digest=current_hash,
                    )
                    document.remove_indexes.add(index)
                    continue
                backup_failed = False
                for candidate, _, should_remove in valid_backups:
                    if not should_remove:
                        continue
                    try:
                        identity = self.ensure_safe_chain(candidate, leaf_kind="file")
                        self.unlink_verified(candidate, identity)
                        self.handled_paths.add(candidate)
                    except (OSError, SafetyError) as error:
                        backup_failed = True
                        self.add("claude_backup", candidate, "error", f"remove failed: {error}")
                if not backup_failed:
                    document.remove_indexes.add(index)
                    self.add(
                        "claude_inplace",
                        target,
                        "removed",
                        "removed stale helper backup/history; " + reason,
                        evidence,
                        digest=current_hash,
                    )

        for backup in backups:
            if (
                backup in associated_backups
                or backup in self.handled_paths
                or any(
                    lexical_beneath(self.canonicalize_path(backup), root)
                    for root in mirror_roots
                )
            ):
                continue
            self.add(
                "claude_backup",
                backup,
                "ambiguous",
                "helper-named backup has no matching Claude patch history",
            )

    def known_codex_roots(self) -> list[Path]:
        roots = [self.canonicalize_path(self.home / ".config") / "codex-proxy"]
        xdg = os.environ.get("XDG_CONFIG_HOME", "").strip()
        if xdg:
            xdg_root = self.canonicalize_path(normalized_absolute(xdg)) / "codex-proxy"
            if self.scope_for(xdg_root) == "home":
                roots.append(xdg_root)
        roots.extend(
            document.path.parent
            for document in self.histories
            if document.flavor == "codex"
        )
        roots.append(self.tmp_root / f"codex-proxy-yolo-uid-{self.uid}")
        return sorted(set(roots))

    def codex_reserved_root(self, path: Path) -> bool:
        path = self.canonicalize_path(path)
        if lexical_beneath(path, self.tmp_root / f"codex-proxy-yolo-uid-{self.uid}"):
            return True
        for root in self.known_codex_roots():
            if lexical_beneath(path, root):
                return True
        return False

    def process_codex_binary(self, path: Path) -> None:
        if path in self.handled_paths:
            return
        evidence: list[str] = []
        try:
            identity = self.ensure_safe_chain(path, leaf_kind="file")
        except (OSError, SafetyError) as error:
            self.add("codex_binary", path, "unsafe", str(error))
            return
        if not mode_is_executable(os.lstat(path), path):
            self.add(
                "codex_binary",
                path,
                "ambiguous",
                "helper-like name is not an executable file",
            )
            return
        digest = sha256_file(path)
        history_entries = self.history_entries_for("codex", path)
        if self.codex_reserved_root(path):
            evidence.append("codex-helper reserved directory")
        if contains_codex_patch_marker(path):
            evidence.append("legacy Codex patch marker")
        if any(str(entry.get("patchedSha256", "")).lower() == digest for _, _, entry in history_entries):
            evidence.append("matching Codex patch history SHA-256")
        lease_path = path.with_name(path.name + ".lease")
        lease: dict[str, Any] | None = None
        if lease_path.is_file() and not lease_path.is_symlink():
            try:
                self.ensure_safe_chain(lease_path, leaf_kind="file")
                parsed = parse_json(lease_path)
                if (
                    isinstance(parsed, dict)
                    and parsed.get("version") == 1
                    and isinstance(parsed.get("pid"), int)
                    and parsed["pid"] > 0
                    and isinstance(parsed.get("heartbeat_unix"), int)
                    and parsed["heartbeat_unix"] > 0
                ):
                    lease = parsed
            except (OSError, ValueError, json.JSONDecodeError, SafetyError):
                pass
        if lease is not None and evidence:
            evidence.append("valid Codex patch lease")
        if not evidence:
            self.add(
                "codex_binary",
                path,
                "ambiguous",
                "helper-like filename has no helper provenance",
                digest=digest,
            )
            return
        pids = self.process_pids_for_file(path)
        if pids and self.purge and self.terminate_active:
            remaining = self.stop_exact_processes(pids, path=path)
            if remaining:
                self.add(
                    "codex_binary",
                    path,
                    "active_process",
                    "patched Codex remains active",
                    evidence,
                    digest=digest,
                    pid=remaining[0],
                )
                return
            pids = []
        if pids:
            self.add(
                "codex_binary",
                path,
                "active_process",
                "patched Codex is active",
                evidence,
                digest=digest,
                pid=pids[0],
            )
            return
        if not self.purge:
            self.add("codex_binary", path, "would_remove", "proven helper copy", evidence, digest=digest)
            for document, index, _ in history_entries:
                document.remove_indexes.add(index)
            return
        try:
            self.unlink_verified(path, identity)
        except (OSError, SafetyError) as error:
            self.add("codex_binary", path, "error", f"remove failed: {error}", evidence)
            return
        self.handled_paths.add(path)
        if lease_path.exists() and not lease_path.is_symlink():
            try:
                lease_identity = self.ensure_safe_chain(lease_path, leaf_kind="file")
                self.unlink_verified(lease_path, lease_identity)
                self.handled_paths.add(lease_path)
            except (OSError, SafetyError) as error:
                self.add("codex_lease", lease_path, "error", f"remove failed: {error}")
        self.add("codex_binary", path, "removed", "proven helper copy", evidence, digest=digest)
        for document, index, _ in history_entries:
            document.remove_indexes.add(index)

    def process_orphan_leases(self, roots: Sequence[Path]) -> None:
        for root in roots:
            if not root.is_dir() or root.is_symlink():
                continue
            try:
                entries = list(os.scandir(root))
            except OSError:
                continue
            for entry in entries:
                if not entry.name.endswith(".lease"):
                    continue
                binary_name = entry.name[: -len(".lease")]
                if not CODEX_BINARY_RE.fullmatch(binary_name):
                    continue
                path = Path(entry.path)
                binary = path.with_name(binary_name)
                if binary.exists() or path in self.handled_paths:
                    continue
                try:
                    identity = self.ensure_safe_chain(path, leaf_kind="file")
                except (OSError, SafetyError) as error:
                    self.add("codex_lease", path, "unsafe", str(error))
                    continue
                if not self.purge:
                    self.add("codex_lease", path, "would_remove", "orphan lease in reserved directory")
                    continue
                try:
                    self.unlink_verified(path, identity)
                    self.add("codex_lease", path, "removed", "orphan lease in reserved directory")
                    self.handled_paths.add(path)
                except (OSError, SafetyError) as error:
                    self.add("codex_lease", path, "error", f"remove failed: {error}")

    def process_requirements(self) -> None:
        candidates = [self.tmp_root / "cxreq" / "requirements.toml"]
        try:
            for entry in os.scandir(self.tmp_root):
                if entry.is_dir(follow_symlinks=False) and CODEX_REQ_DIR_RE.fullmatch(entry.name):
                    candidates.append(Path(entry.path) / "reqs.toml")
        except OSError:
            pass
        for path in candidates:
            try:
                parent_info = os.lstat(path.parent)
            except FileNotFoundError:
                continue
            except OSError as error:
                self.add("codex_requirements", path, "error", f"stat parent failed: {error}")
                continue
            if parent_info.st_uid != self.uid:
                # /tmp/cx*-* names are shared across all users.  A foreign-owned
                # directory is outside this cleaner's scope, even when its name
                # and contents came from another helper instance.
                continue
            if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
                self.add(
                    "codex_requirements",
                    path,
                    "unsafe",
                    "known temporary requirements parent is not a physical directory",
                )
                continue
            try:
                file_info = os.lstat(path)
            except FileNotFoundError:
                continue
            except PermissionError as error:
                self.add("codex_requirements", path, "error", f"stat failed: {error}")
                continue
            except OSError as error:
                self.add("codex_requirements", path, "error", f"stat failed: {error}")
                continue
            if file_info.st_uid != self.uid:
                continue
            try:
                data = path.read_bytes()
            except FileNotFoundError:
                continue
            except OSError as error:
                self.add("codex_requirements", path, "error", f"read failed: {error}")
                continue
            if data not in LEGACY_REQUIREMENTS:
                self.add(
                    "codex_requirements",
                    path,
                    "ambiguous",
                    "known temporary path contains unrecognized content",
                )
                continue
            try:
                parent_identity = self.ensure_safe_chain(path.parent, leaf_kind="directory")
                identity = self.ensure_safe_chain(path, leaf_kind="file")
            except (OSError, SafetyError) as error:
                self.add("codex_requirements", path, "unsafe", str(error))
                continue
            if not self.purge:
                self.add(
                    "codex_requirements",
                    path,
                    "would_remove",
                    "exact legacy requirements content",
                )
                continue
            try:
                self.unlink_verified(path, identity)
                self.add(
                    "codex_requirements", path, "removed", "exact legacy requirements content"
                )
                self.handled_paths.add(path)
                try:
                    self.rmdir_empty_verified(path.parent, parent_identity)
                except (OSError, SafetyError):
                    pass
            except (OSError, SafetyError) as error:
                self.add("codex_requirements", path, "error", f"remove failed: {error}")

    def write_history(self, document: HistoryDocument) -> None:
        if not document.remove_indexes:
            return
        if not self.purge:
            self.add(
                "patch_history",
                document.path,
                "would_remove",
                f"remove {len(document.remove_indexes)} handled entries",
            )
            return
        lock_path = Path(str(document.path) + ".lock")
        lock_fd: int | None = None
        try:
            if fcntl is not None:
                lock_fd = os.open(
                    lock_path,
                    os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                lock_info = os.fstat(lock_fd)
                if (
                    lock_info.st_uid != self.uid
                    or not stat.S_ISREG(lock_info.st_mode)
                    or lock_info.st_nlink != 1
                ):
                    raise SafetyError(
                        "patch history lock is not a current-user-owned regular file"
                    )
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if sha256_file(document.path) != document.source_hash:
                raise SafetyError("patch history changed during cleanup")
            current = parse_json(document.path)
            if current != document.data:
                raise SafetyError("patch history content changed during cleanup")
            entries = current.get("entries", [])
            current["entries"] = [
                entry for index, entry in enumerate(entries) if index not in document.remove_indexes
            ]
            if current["entries"]:
                encoded = (json.dumps(current, indent=2, sort_keys=False) + "\n").encode()
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".patch_history.cleanup-", dir=document.path.parent
                )
                temporary = Path(temporary_name)
                try:
                    os.fchmod(descriptor, 0o600)
                    with os.fdopen(descriptor, "wb", closefd=True) as stream:
                        stream.write(encoded)
                        stream.flush()
                        os.fsync(stream.fileno())
                    descriptor = -1
                    os.replace(temporary, document.path)
                finally:
                    if descriptor >= 0:
                        os.close(descriptor)
                    try:
                        temporary.unlink()
                    except FileNotFoundError:
                        pass
                reason = f"removed {len(document.remove_indexes)} handled entries"
            else:
                identity = self.ensure_safe_chain(document.path, leaf_kind="file")
                self.unlink_verified(document.path, identity)
                reason = "removed empty helper patch history"
            self.add("patch_history", document.path, "removed", reason)
            self.handled_paths.add(document.path)
        except (OSError, ValueError, json.JSONDecodeError, BlockingIOError, SafetyError) as error:
            self.add("patch_history", document.path, "error", f"update failed: {error}")
        finally:
            if lock_fd is not None:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                finally:
                    os.close(lock_fd)

    def one_pass(self) -> None:
        histories, codex_candidates, backups = self.discover_histories_and_candidates()
        self.load_histories(histories)

        for root, scope in self.helper_mirror_roots():
            self.mirror_roots.add(root)
            self.process_mirror_root(root, scope)

        self.process_claude_histories(backups)
        for path in codex_candidates:
            self.process_codex_binary(path)

        self.process_orphan_leases(self.known_codex_roots())
        self.process_requirements()
        for document in self.histories:
            self.write_history(document)

    def run(self) -> dict[str, Any]:
        try:
            self.validate_roots()
        except (OSError, SafetyError) as error:
            self.add("scope", self.home, "unsafe", str(error))
            return self.report()
        self.one_pass()
        if self.purge:
            for pass_index in range(self.verify_passes):
                if self.settle_seconds:
                    time.sleep(self.settle_seconds)
                verifier = PatchedBinaryCleaner(
                    home=self.home,
                    tmp_root=self.tmp_root,
                    uid=self.uid,
                    purge=False,
                    terminate_active=False,
                    settle_seconds=0,
                    verify_passes=1,
                    claude_originals=self.claude_originals,
                )
                verifier.validate_roots()
                verifier.one_pass()
                residuals = [
                    item
                    for item in verifier.findings
                    if item.status in ARTIFACT_STATUSES
                ]
                for item in residuals:
                    self.add(
                        item.kind,
                        Path(item.path),
                        "residual",
                        f"verification pass {pass_index + 1}: {item.reason}",
                        item.evidence,
                        digest=item.sha256,
                        pid=item.pid,
                    )
        return self.report()

    def report(self) -> dict[str, Any]:
        findings = sorted(
            (finding.as_dict() for finding in self.findings),
            key=lambda item: (item.get("path", ""), item.get("kind", ""), item.get("status", "")),
        )
        counts: dict[str, int] = {}
        for finding in findings:
            status = str(finding["status"])
            counts[status] = counts.get(status, 0) + 1
        clean = not any(finding["status"] in ARTIFACT_STATUSES for finding in findings)
        if self.purge:
            # Successfully removed/restored actions are historical facts, not residuals.
            clean = not any(finding["status"] in BLOCKING_STATUSES for finding in findings)
        return {
            "version": 1,
            "mode": "purge" if self.purge else "audit",
            "uid": self.uid,
            "home": str(self.home),
            "tmpRoot": str(self.tmp_root),
            "clean": clean,
            "counts": dict(sorted(counts.items())),
            "findings": findings,
        }


def report_exit_code(report: dict[str, Any]) -> int:
    if report.get("clean") is True:
        return 0
    statuses = {item.get("status") for item in report.get("findings", [])}
    if "repair_required" in statuses:
        return 12
    if "active_process" in statuses:
        return 11
    if "unsafe" in statuses:
        return 13
    if "error" in statuses or "residual" in statuses:
        return 14
    return 10


def current_home() -> Path:
    if pwd is None:
        raise RuntimeError("POSIX passwd database is unavailable")
    return Path(pwd.getpwuid(os.geteuid()).pw_dir)


def has_readable_procfs() -> bool:
    try:
        os.stat("/proc/self")
        os.stat("/proc/self/exe")
        return True
    except OSError:
        return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--audit", action="store_true", help="audit only (default)")
    mode.add_argument("--purge", action="store_true", help="perform proven cleanup actions")
    parser.add_argument(
        "--terminate-active",
        action="store_true",
        help="terminate only current-user processes executing a proven candidate",
    )
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=0.25,
        help="delay between post-clean verification scans (default: 0.25)",
    )
    parser.add_argument(
        "--claude-original",
        action="append",
        default=[],
        metavar="TARGET=SOURCE",
        help=(
            "trusted original for an old in-place Claude patch; SOURCE is only used "
            "when its SHA-256 equals history origSha256 (repeatable)"
        ),
    )
    parser.add_argument("--json", metavar="PATH", help="write the complete JSON report; use - for stdout")
    return parser


def parse_claude_originals(
    parser: argparse.ArgumentParser, values: Sequence[str]
) -> dict[Path, Path]:
    result: dict[Path, Path] = {}
    for value in values:
        if "=" not in value:
            parser.error(f"invalid --claude-original {value!r}; expected TARGET=SOURCE")
        raw_target, raw_source = value.split("=", 1)
        if not raw_target.strip() or not raw_source.strip():
            parser.error(f"invalid --claude-original {value!r}; expected TARGET=SOURCE")
        target = normalized_absolute(raw_target.strip())
        source = normalized_absolute(raw_source.strip())
        if target in result and result[target] != source:
            parser.error(f"multiple originals supplied for Claude target {target}")
        result[target] = source
    return result


def human_report(report: dict[str, Any]) -> str:
    state = "clean" if report["clean"] else "not clean"
    lines = [
        f"patched-binary cleanup: {state} (mode={report['mode']}, uid={report['uid']})",
        f"home: {report['home']}",
        f"tmp:  {report['tmpRoot']}",
    ]
    if report["counts"]:
        lines.append(
            "findings: "
            + ", ".join(f"{key}={value}" for key, value in report["counts"].items())
        )
    for item in report["findings"]:
        lines.append(
            f"[{item['status']}] {item['kind']}: {item['path']} ({item['reason']})"
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    if os.name != "posix" or not hasattr(os, "geteuid"):
        print("cleanup_patched_binaries.py currently supports POSIX systems only", file=sys.stderr)
        return 20
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.terminate_active and not args.purge:
        print("--terminate-active requires --purge", file=sys.stderr)
        return 20
    if args.purge and not has_readable_procfs():
        print(
            "--purge requires readable Linux-style procfs so active executables "
            "cannot be mistaken for inactive artifacts; audit mode remains available",
            file=sys.stderr,
        )
        return 20
    claude_originals = parse_claude_originals(parser, args.claude_original)
    cleaner = PatchedBinaryCleaner(
        home=current_home(),
        uid=os.geteuid(),
        purge=args.purge,
        terminate_active=args.terminate_active,
        settle_seconds=args.settle_seconds,
        claude_originals=claude_originals,
    )
    report = cleaner.run()
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.json == "-":
        sys.stdout.write(encoded)
    else:
        print(human_report(report))
        if args.json:
            Path(args.json).write_text(encoded, encoding="utf-8")
    return report_exit_code(report)


if __name__ == "__main__":
    raise SystemExit(main())
