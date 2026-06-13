#!/usr/bin/env python3
"""Scan managed Teams ASR native dependencies on the current Linux host.

The scanner intentionally does not fail fast. It downloads the pinned Linux ASR
runtime assets, inspects every ELF file it can find, runs ldd -v on each file,
and probes Python audio/native modules one by one. The JSON/text reports are
written before the process exits non-zero for detected compatibility issues.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback
import urllib.request
import zipfile
from collections import defaultdict


LLAMA_ASSETS = [
    {
        "component": "llama-vulkan-linux-x64",
        "name": "llama-b9437-bin-ubuntu-vulkan-x64.tar.gz",
        "url": "https://github.com/ggml-org/llama.cpp/releases/download/b9437/llama-b9437-bin-ubuntu-vulkan-x64.tar.gz",
        "sha256": "177e7e70d13ac17df524a4126404025eff2b1f4b5a6f393e07b4bf1c25d31c65",
        "archive": "tar.gz",
    },
    {
        "component": "llama-cpu-linux-x64",
        "name": "llama-b9437-bin-ubuntu-x64.tar.gz",
        "url": "https://github.com/ggml-org/llama.cpp/releases/download/b9437/llama-b9437-bin-ubuntu-x64.tar.gz",
        "sha256": "07b0bf370a696329463d999ecb5c4860717eef6824e55eaf062214d70e78174d",
        "archive": "tar.gz",
    },
]

FFMPEG_ASSET = {
    "component": "imageio-ffmpeg-linux-x64",
    "name": "imageio_ffmpeg-0.6.0-py3-none-manylinux2014_x86_64.whl",
    "url": "https://files.pythonhosted.org/packages/a0/2d/43c8522a2038e9d0e7dbdf3a61195ecc31ca576fb1527a528c877e87d973/imageio_ffmpeg-0.6.0-py3-none-manylinux2014_x86_64.whl",
    "sha256": "c7e46fcec401dd990405049d2e2f475e2b397779df2519b544b8aab515195282",
    "archive": "zip",
}

PYTHON_ASSET = {
    "component": "python-build-standalone-linux-x64",
    "name": "cpython-3.10.20+20260510-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz",
    "url": "https://github.com/astral-sh/python-build-standalone/releases/download/20260510/cpython-3.10.20+20260510-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz",
    "sha256": "",
    "archive": "tar.gz",
}

# Default probe avoids torch-sized downloads while covering the audio/native
# closure that has produced libsndfile/FFmpeg-style failures.
AUDIO_PROBE_PACKAGES = [
    ("soundfile", True),
    ("soxr", True),
    ("av", True),
    ("librosa", True),
    # These top-level packages are useful to preserve in the wheelhouse report,
    # but their transitive closure can pull torch/CUDA wheels. The explicit
    # audio packages above cover the native media libraries we need in default CI.
    ("qwen-omni-utils", False),
    ("qwen-asr==0.0.6", False),
]

FULL_PROBE_PACKAGES = [
    ("qwen-asr==0.0.6", True),
    ("imageio-ffmpeg==0.6.0", True),
    ("torch>=2.4,<2.13", True),
]

IMPORT_PROBES = [
    ("soundfile", "soundfile"),
    ("soxr", "soxr"),
    ("av", "av"),
    ("librosa", "librosa"),
]

BLOCKING_ISSUE_KINDS = set(
    [
        "download_failed",
        "extract_failed",
        "missing_library",
        "missing_symbol_version",
        "pip_download_failed",
        "pip_install_failed",
        "python_import_failed",
        "python_unavailable",
        "pip_unavailable",
        "tool_missing",
        "venv_failed",
    ]
)

NATIVE_COMPAT_REPAIRABLE_LIBRARIES = set(
    [
        "ld-linux-x86-64.so.2",
        "libc.so.6",
        "libm.so.6",
        "libpthread.so.0",
        "libdl.so.2",
        "librt.so.1",
        "libresolv.so.2",
        "libgcc_s.so.1",
        "libstdc++.so.6",
        "libgomp.so.1",
        "libssl.so.3",
        "libcrypto.so.3",
        "libnss_files.so.2",
        "libnss_dns.so.2",
    ]
)
NATIVE_COMPAT_REPAIR_PROFILES = [
    {
        "name": "linux-x64-glibc-2.35-conda-runtime-v1",
        "limits": {
            "GLIBC_": (2, 35),
            "GLIBCXX_": (3, 4, 30),
            "CXXABI_": (1, 3, 13),
            "OPENSSL_": (3, 0, 0),
            "GCC_": (12, 0, 0),
        },
    },
    {
        "name": "linux-x64-glibc-2.39-conda-runtime-v1",
        "limits": {
            "GLIBC_": (2, 39),
            "GLIBCXX_": (3, 4, 33),
            "CXXABI_": (1, 3, 15),
            "OPENSSL_": (3, 0, 0),
            "GCC_": (14, 0, 0),
        },
    },
]

RUNTIME_CANDIDATES = [
    {
        "name": "llama-cpu-teams-media",
        "backend": "llama",
        "device": "cpu",
        "components": ["llama-cpu-linux-x64", "imageio-ffmpeg-linux-x64"],
        "description": "CPU llama.cpp runtime plus managed ffmpeg for Teams f4a/video/speed conversion.",
        "download_scope": "llama CPU binary and managed ffmpeg only; excludes Vulkan and transformers/torch wheels. On old Linux hosts, native compat repair adds the smallest pinned glibc/libstdc++/libgomp/patchelf profile that satisfies the missing symbols.",
    },
    {
        "name": "llama-vulkan-teams-media",
        "backend": "llama",
        "device": "vulkan",
        "components": ["llama-vulkan-linux-x64", "imageio-ffmpeg-linux-x64"],
        "description": "Vulkan llama.cpp runtime plus managed ffmpeg.",
        "download_scope": "Vulkan llama binary and managed ffmpeg; may require system Vulkan loader/driver libraries.",
    },
    {
        "name": "transformers-full",
        "backend": "qwen-asr-transformers",
        "device": "auto",
        "components": ["python-build-standalone-linux-x64"],
        "component_prefixes": ["python-wheel:", "python-import-probe", "python-wheel-download"],
        "requires_python_probe": "full",
        "description": "Full qwen-asr Python fallback, including torch-sized transitive dependencies.",
        "download_scope": "standalone Python plus full qwen-asr/torch wheel closure; intentionally excluded from default audio probe.",
    },
]

MINIMAL_PLAN_DEFINITIONS = [
    {
        "name": "cpu-only-teams-media",
        "description": "Smallest default Teams media runtime for a CPU-only Linux host.",
        "candidate_order": ["llama-cpu-teams-media"],
        "excluded_components": [
            "llama-vulkan-linux-x64",
            "python-build-standalone-linux-x64",
            "python-wheel:*",
            "python-import-probe",
            "python-wheel-download",
        ],
    },
    {
        "name": "accelerated-or-cpu-teams-media",
        "description": "Auto llama runtime order with CPU fallback when accelerated runtime is unusable.",
        "candidate_order": ["llama-vulkan-teams-media", "llama-cpu-teams-media"],
        "excluded_components": [
            "python-build-standalone-linux-x64",
            "python-wheel:*",
            "python-import-probe",
            "python-wheel-download",
        ],
    },
    {
        "name": "transformers-fallback",
        "description": "Large Python fallback path; only considered proven when --python-probe full is used.",
        "candidate_order": ["transformers-full"],
        "excluded_components": [],
    },
]

MISSING_VERSION_RE = re.compile(
    r"(?P<library>(?:/[^:\s]+|lib[^:\s]+)):\s+version [`'](?P<version>[^`']+)[`'] not found"
    r"(?: \(required by (?P<required_by>[^)]+)\))?"
)
MISSING_LIB_RE = re.compile(r"^\s*(?P<library>\S+)\s+=>\s+not found\b")
CANNOT_OPEN_RE = re.compile(
    r"(?P<library>lib[^\s:'\"]+\.so(?:\.[0-9]+)*)[^:\n]*:\s+cannot open shared object file"
)
READ_ELF_NEEDED_RE = re.compile(r"Shared library: \[(?P<library>[^\]]+)\]")
VERSION_NAME_RE = re.compile(r"\bName:\s+(?P<version>[A-Za-z0-9_.+-]+)")


def run_command(argv, env=None, timeout=120):
    try:
        proc = subprocess.run(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "returncode": 124,
            "stdout": decode_process_output(exc.stdout),
            "stderr": decode_process_output(exc.stderr) + "\ncommand timed out",
            "argv": argv,
        }
    except OSError as exc:
        return {"returncode": 127, "stdout": "", "stderr": str(exc), "argv": argv}
    return {
        "returncode": proc.returncode,
        "stdout": decode_process_output(proc.stdout),
        "stderr": decode_process_output(proc.stderr),
        "argv": argv,
    }


def decode_process_output(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def issue(component, path, kind, library="", required_version="", detail=""):
    return {
        "component": component,
        "path": path,
        "kind": kind,
        "library": library,
        "required_version": required_version,
        "detail": detail.strip(),
    }


def add_unique_issue(issues, seen, item):
    key = (
        item.get("component", ""),
        item.get("path", ""),
        item.get("kind", ""),
        item.get("library", ""),
        item.get("required_version", ""),
        item.get("detail", ""),
    )
    if key not in seen:
        seen.add(key)
        issues.append(item)


def parse_ldd_issues(component, path, output):
    found = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        version = MISSING_VERSION_RE.search(line)
        if version:
            found.append(
                issue(
                    component,
                    path,
                    "missing_symbol_version",
                    version.group("library") or "",
                    version.group("version") or "",
                    line,
                )
            )
            continue
        missing = MISSING_LIB_RE.search(line)
        if missing:
            found.append(
                issue(component, path, "missing_library", missing.group("library") or "", "", line)
            )
            continue
        cannot_open = CANNOT_OPEN_RE.search(line)
        if cannot_open:
            found.append(
                issue(
                    component,
                    path,
                    "missing_library",
                    cannot_open.group("library") or "",
                    "",
                    line,
                )
            )
    return found


def parse_import_issues(component, module, output):
    found = []
    lower = output.lower()
    if "libsndfile" in lower or "sndfile library not found" in lower:
        found.append(issue(component, module, "missing_library", "libsndfile", "", last_lines(output, 12)))
    for match in CANNOT_OPEN_RE.finditer(output):
        found.append(
            issue(component, module, "missing_library", match.group("library") or "", "", last_lines(output, 12))
        )
    return found


def parse_readelf_needed(output):
    return sorted(set(match.group("library") for match in READ_ELF_NEEDED_RE.finditer(output)))


def parse_readelf_versions(output):
    versions = set()
    for match in VERSION_NAME_RE.finditer(output):
        value = match.group("version")
        if "_" in value or value.startswith("OPENSSL"):
            versions.add(value)
    return sorted(versions)


def last_lines(text, limit):
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines[-limit:])


def ensure_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def download(asset, downloads_dir, issues, seen):
    ensure_dir(downloads_dir)
    dest = os.path.join(downloads_dir, asset["name"])
    expected = asset.get("sha256", "").strip()
    if os.path.exists(dest) and (not expected or sha256_file(dest) == expected):
        return dest

    tmp = dest + ".tmp"
    request = urllib.request.Request(asset["url"], headers={"User-Agent": "codex-helper-asr-dependency-scan"})
    last_error = None
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                h = hashlib.sha256()
                with open(tmp, "wb") as fh:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        h.update(chunk)
                        fh.write(chunk)
            got = h.hexdigest()
            if expected and got != expected:
                raise RuntimeError("sha256 mismatch: got {0}, want {1}".format(got, expected))
            os.rename(tmp, dest)
            return dest
        except Exception as exc:  # noqa: BLE001 - CI diagnostic should keep going.
            last_error = exc
            try:
                os.unlink(tmp)
            except OSError:
                pass
            if attempt < 3:
                time.sleep(5 * attempt)

    add_unique_issue(
        issues,
        seen,
        issue(asset["component"], asset["name"], "download_failed", "", "", str(last_error)),
    )
    return None


def safe_extract_tar_gz(archive, dest):
    with tarfile.open(archive, "r:gz") as tar:
        base = os.path.realpath(dest)
        for member in tar.getmembers():
            target = os.path.realpath(os.path.join(dest, member.name))
            if target != base and not target.startswith(base + os.sep):
                raise RuntimeError("archive contains unsafe path: {0}".format(member.name))
        try:
            tar.extractall(dest, filter="data")
        except TypeError:
            tar.extractall(dest)


def safe_extract_zip(archive, dest):
    with zipfile.ZipFile(archive) as zf:
        base = os.path.realpath(dest)
        for member in zf.infolist():
            target = os.path.realpath(os.path.join(dest, member.filename))
            if target != base and not target.startswith(base + os.sep):
                raise RuntimeError("archive contains unsafe path: {0}".format(member.filename))
        zf.extractall(dest)


def extract_asset(asset, archive_path, extracts_dir, issues, seen):
    dest = os.path.join(extracts_dir, safe_name(asset["component"]))
    if os.path.isdir(dest):
        return dest
    ensure_dir(dest)
    try:
        if asset["archive"] == "tar.gz":
            safe_extract_tar_gz(archive_path, dest)
        elif asset["archive"] == "zip":
            safe_extract_zip(archive_path, dest)
        else:
            raise RuntimeError("unsupported archive kind: {0}".format(asset["archive"]))
    except Exception as exc:  # noqa: BLE001 - CI diagnostic should keep going.
        add_unique_issue(
            issues,
            seen,
            issue(asset["component"], archive_path, "extract_failed", "", "", str(exc)),
        )
        return None
    return dest


def safe_name(value):
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-") or "asset"


def is_elf(path):
    try:
        with open(path, "rb") as fh:
            return fh.read(4) == b"\x7fELF"
    except OSError:
        return False


def iter_elf_files(root):
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            path = os.path.join(dirpath, name)
            if is_elf(path):
                yield path


def collect_library_dirs(native_paths):
    dirs = set()
    for path in native_paths:
        dirs.add(os.path.dirname(path))
    return sorted(dirs)


def scan_native_tree(component, root, report, issues, seen):
    native_paths = list(iter_elf_files(root))
    report["native_files_scanned"] += len(native_paths)
    report["components"].setdefault(component, {"native_files_scanned": 0})
    report["components"][component]["native_files_scanned"] += len(native_paths)
    library_dirs = collect_library_dirs(native_paths)
    env = os.environ.copy()
    if library_dirs:
        existing = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = ":".join(library_dirs + ([existing] if existing else []))

    readelf = shutil.which("readelf")
    ldd = shutil.which("ldd")
    if not readelf:
        add_unique_issue(issues, seen, issue(component, root, "tool_missing", "readelf", "", "readelf is required"))
    if not ldd:
        add_unique_issue(issues, seen, issue(component, root, "tool_missing", "ldd", "", "ldd is required"))

    for path in native_paths:
        rel = os.path.relpath(path, root)
        record = {
            "component": component,
            "path": rel,
            "needed": [],
            "versions": [],
        }
        if readelf:
            dyn = run_command([readelf, "-d", path], timeout=60)
            record["needed"] = parse_readelf_needed(dyn["stdout"] + dyn["stderr"])
            ver = run_command([readelf, "--version-info", path], timeout=60)
            record["versions"] = parse_readelf_versions(ver["stdout"] + ver["stderr"])
        if ldd:
            ldd_proc = run_command([ldd, "-v", path], env=env, timeout=120)
            output = ldd_proc["stdout"] + ldd_proc["stderr"]
            for item in parse_ldd_issues(component, rel, output):
                add_unique_issue(issues, seen, item)
        report["native_files"].append(record)
        for lib in record["needed"]:
            report["needed_libraries"][lib] = True
        for version in record["versions"]:
            report["required_versions"][version] = True


def find_python_executable(root):
    candidates = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if name in ("python3", "python"):
                candidates.append(os.path.join(dirpath, name))
    candidates.sort(key=lambda p: (0 if p.endswith(os.path.join("bin", "python3")) else 1, len(p)))
    for candidate in candidates:
        if os.access(candidate, os.X_OK):
            return candidate
    return None


def ensure_python_tools(python, issues, seen):
    version = run_command([python, "--version"], timeout=60)
    if version["returncode"] != 0:
        add_unique_issue(
            issues,
            seen,
            issue("python-probe", python, "python_unavailable", "", "", version["stdout"] + version["stderr"]),
        )
        return False
    pip = run_command([python, "-m", "pip", "--version"], timeout=60)
    if pip["returncode"] == 0:
        return True
    ensurepip = run_command([python, "-m", "ensurepip", "--upgrade"], timeout=180)
    if ensurepip["returncode"] != 0:
        add_unique_issue(
            issues,
            seen,
            issue("python-probe", python, "pip_unavailable", "", "", ensurepip["stdout"] + ensurepip["stderr"]),
        )
        return False
    return True


def pip_download_packages(python, packages, wheelhouse, issues, seen):
    ensure_dir(wheelhouse)
    for entry in packages:
        if isinstance(entry, (list, tuple)):
            package, include_deps = entry
        else:
            package, include_deps = entry, True
        cmd = [
            python,
            "-m",
            "pip",
            "download",
            "--only-binary=:all:",
            "--dest",
            wheelhouse,
        ]
        if not include_deps:
            cmd.append("--no-deps")
        cmd.append(package)
        proc = run_command(cmd, timeout=900)
        if proc["returncode"] != 0:
            add_unique_issue(
                issues,
                seen,
                issue("python-wheel-download", package, "pip_download_failed", "", "", last_lines(proc["stdout"] + proc["stderr"], 30)),
            )


def unpack_wheels(wheelhouse, unpack_root, issues, seen):
    ensure_dir(unpack_root)
    roots = []
    for name in sorted(os.listdir(wheelhouse)):
        if not name.endswith(".whl"):
            continue
        wheel = os.path.join(wheelhouse, name)
        dest = os.path.join(unpack_root, safe_name(name[:-4]))
        if not os.path.isdir(dest):
            ensure_dir(dest)
            try:
                safe_extract_zip(wheel, dest)
            except Exception as exc:  # noqa: BLE001 - CI diagnostic should keep going.
                add_unique_issue(issues, seen, issue("python-wheel", name, "extract_failed", "", "", str(exc)))
                continue
        roots.append((name, dest))
    return roots


def venv_python_path(venv_dir):
    return os.path.join(venv_dir, "bin", "python")


def create_venv(python, venv_dir, issues, seen):
    proc = run_command([python, "-m", "venv", venv_dir], timeout=300)
    if proc["returncode"] != 0:
        add_unique_issue(
            issues,
            seen,
            issue("python-import-probe", venv_dir, "venv_failed", "", "", last_lines(proc["stdout"] + proc["stderr"], 30)),
        )
        return None
    return venv_python_path(venv_dir)


def run_import_probes(base_python, wheelhouse, work_dir, issues, seen):
    venv_dir = os.path.join(work_dir, "import-venv")
    python = create_venv(base_python, venv_dir, issues, seen)
    if not python:
        return
    run_command([python, "-m", "pip", "install", "--upgrade", "pip"], timeout=300)

    for package, module in IMPORT_PROBES:
        install = run_command(
            [
                python,
                "-m",
                "pip",
                "install",
                "--no-index",
                "--find-links",
                wheelhouse,
                package,
            ],
            timeout=600,
        )
        if install["returncode"] != 0:
            add_unique_issue(
                issues,
                seen,
                issue(
                    "python-import-probe",
                    package,
                    "pip_install_failed",
                    "",
                    "",
                    last_lines(install["stdout"] + install["stderr"], 30),
                ),
            )
            continue
        probe = run_command([python, "-c", "import {0}; print('ok')".format(module)], timeout=180)
        output = probe["stdout"] + probe["stderr"]
        if probe["returncode"] != 0:
            add_unique_issue(
                issues,
                seen,
                issue("python-import-probe", module, "python_import_failed", "", "", last_lines(output, 30)),
            )
            for item in parse_import_issues("python-import-probe", module, output):
                add_unique_issue(issues, seen, item)


def write_json(path, report):
    data = dict(report)
    data["needed_libraries"] = sorted(data["needed_libraries"].keys())
    data["required_versions"] = sorted(data["required_versions"].keys())
    data["missing_libraries"] = missing_libraries(report)
    data["missing_symbol_versions"] = missing_symbol_versions(report)
    data["issues"] = report["issues"]
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2, sort_keys=True)
        fh.write("\n")


def missing_libraries(report):
    return sorted(
        set(
            item.get("library", "")
            for item in report["issues"]
            if item.get("kind") == "missing_library" and item.get("library")
        )
    )


def missing_symbol_versions(report):
    pairs = set()
    for item in report["issues"]:
        if item.get("kind") == "missing_symbol_version" and item.get("library") and item.get("required_version"):
            pairs.add((item["library"], item["required_version"]))
    return [{"library": library, "required_version": version} for library, version in sorted(pairs)]


def report_component_names(report):
    names = set(report.get("components", {}).keys())
    for record in report.get("native_files", []):
        component = record.get("component", "")
        if component:
            names.add(component)
    for item in report.get("issues", []):
        component = item.get("component", "")
        if component:
            names.add(component)
    return names


def issue_matches_component(item, component):
    return item.get("component", "") == component


def issue_matches_prefix(item, prefix):
    return item.get("component", "").startswith(prefix)


def candidate_issues(report, candidate):
    components = candidate.get("components", [])
    prefixes = candidate.get("component_prefixes", [])
    found = []
    for item in report.get("issues", []):
        if any(issue_matches_component(item, component) for component in components) or any(
            issue_matches_prefix(item, prefix) for prefix in prefixes
        ):
            found.append(item)
    return found


def blocking_issues(items):
    return [item for item in items if item.get("kind", "") in BLOCKING_ISSUE_KINDS]


def native_compat_repairable_issue(item):
    return native_compat_repair_profile([item]) is not None


def native_compat_repairable_symbol_version(version):
    return any(native_compat_profile_repairs_symbol_version(profile, version) for profile in NATIVE_COMPAT_REPAIR_PROFILES)


def native_compat_profile_repairs_symbol_version(profile, version):
    for prefix, limit in profile["limits"].items():
        if not version.startswith(prefix):
            continue
        parsed = parse_symbol_version_tuple(version[len(prefix) :])
        return bool(parsed) and version_tuple_at_most(parsed, limit)
    return False


def version_tuple_at_most(parsed, limit):
    parsed = tuple(parsed)
    limit = tuple(limit)
    if len(parsed) > len(limit):
        if any(part != 0 for part in parsed[len(limit) :]):
            return False
        parsed = parsed[: len(limit)]
    while len(parsed) < len(limit):
        parsed = parsed + (0,)
    return parsed <= limit


def parse_symbol_version_tuple(value):
    parts = []
    for raw in value.split("."):
        match = re.match(r"^(\d+)", raw)
        if not match:
            break
        parts.append(int(match.group(1)))
        if match.group(1) != raw:
            break
    return tuple(parts)


def native_compat_repair_profile(issues):
    if not issues:
        return None
    for profile in NATIVE_COMPAT_REPAIR_PROFILES:
        if all(native_compat_issue_matches_repair_profile(profile, item) for item in issues):
            return profile
    return None


def native_compat_issue_matches_repair_profile(profile, item):
    kind = item.get("kind", "")
    if kind == "missing_library":
        return os.path.basename(item.get("library", "")) in NATIVE_COMPAT_REPAIRABLE_LIBRARIES
    if kind == "missing_symbol_version":
        return native_compat_profile_repairs_symbol_version(profile, item.get("required_version", ""))
    return False


def native_compat_repairable_candidate(candidate, issues):
    return native_compat_candidate_repair_profile(candidate, issues) is not None


def native_compat_candidate_repair_profile(candidate, issues):
    if candidate.get("backend") != "llama":
        return None
    if not issues:
        return None
    return native_compat_repair_profile(issues)


def evaluate_runtime_candidate(report, candidate, python_probe):
    result = {
        "name": candidate["name"],
        "backend": candidate.get("backend", ""),
        "device": candidate.get("device", ""),
        "components": list(candidate.get("components", [])),
        "component_prefixes": list(candidate.get("component_prefixes", [])),
        "description": candidate.get("description", ""),
        "download_scope": candidate.get("download_scope", ""),
        "status": "usable",
        "usable": True,
        "blocking_issues": [],
        "missing_components": [],
    }
    required_probe = candidate.get("requires_python_probe", "")
    if required_probe and python_probe != required_probe:
        result["status"] = "not_scanned"
        result["usable"] = False
        result["reason"] = "requires --python-probe {0}; current probe is {1}".format(required_probe, python_probe)
        return result

    component_names = report_component_names(report)
    missing_components = [
        component
        for component in candidate.get("components", [])
        if component not in component_names
    ]
    if missing_components:
        result["status"] = "not_scanned"
        result["usable"] = False
        result["missing_components"] = missing_components
        result["reason"] = "required components were not scanned"
        return result

    issues = blocking_issues(candidate_issues(report, candidate))
    if issues:
        repair_profile = native_compat_candidate_repair_profile(candidate, issues)
        if repair_profile:
            result["status"] = "repairable"
            result["usable"] = True
            result["repair"] = "native_compat_patchelf"
            result["repair_profile"] = repair_profile["name"]
            result["repairable_issues"] = issues[:25]
        else:
            result["status"] = "blocked"
            result["usable"] = False
            result["blocking_issues"] = issues[:25]
    return result


def evaluate_minimal_plans(candidates):
    by_name = {candidate["name"]: candidate for candidate in candidates}
    plans = []
    for definition in MINIMAL_PLAN_DEFINITIONS:
        checked = []
        selected = None
        for name in definition["candidate_order"]:
            candidate = by_name.get(name)
            if not candidate:
                continue
            checked.append({"name": name, "status": candidate["status"]})
            if candidate.get("usable"):
                selected = candidate
                break
        plan = {
            "name": definition["name"],
            "description": definition["description"],
            "candidate_order": list(definition["candidate_order"]),
            "excluded_components": list(definition.get("excluded_components", [])),
            "checked_candidates": checked,
            "status": selected["status"] if selected else "blocked",
            "usable": selected is not None,
            "selected_candidate": selected["name"] if selected else "",
            "selected_components": list(selected.get("components", [])) if selected else [],
            "download_scope": selected.get("download_scope", "") if selected else "",
        }
        if not selected:
            blockers = []
            for name in definition["candidate_order"]:
                candidate = by_name.get(name)
                if candidate:
                    blockers.extend(candidate.get("blocking_issues", []))
            plan["blocking_issues"] = blockers[:25]
        plans.append(plan)
    return plans


def finalize_runtime_plans(report, python_probe):
    candidates = [evaluate_runtime_candidate(report, candidate, python_probe) for candidate in RUNTIME_CANDIDATES]
    report["runtime_candidates"] = candidates
    report["minimal_runtime_plans"] = evaluate_minimal_plans(candidates)


def render_text(report, detail_limit=200):
    lines = []
    lines.append("Teams ASR dependency scan")
    lines.append("status: {0}".format("FAILED" if report["issues"] else "OK"))
    lines.append("native files scanned: {0}".format(report["native_files_scanned"]))
    lines.append("")
    missing_libs = missing_libraries(report)
    lines.append("Missing libraries ({0}):".format(len(missing_libs)))
    if missing_libs:
        for lib in missing_libs:
            lines.append("  - {0}".format(lib))
    else:
        lines.append("  none")
    lines.append("")
    missing_versions = missing_symbol_versions(report)
    lines.append("Missing symbol versions ({0}):".format(len(missing_versions)))
    if missing_versions:
        for item in missing_versions:
            lines.append("  - {0} {1}".format(item["library"], item["required_version"]))
    else:
        lines.append("  none")
    lines.append("")
    lines.append("Needed libraries ({0}):".format(len(report["needed_libraries"])))
    for lib in sorted(report["needed_libraries"].keys()):
        lines.append("  - {0}".format(lib))
    lines.append("")
    lines.append("Version requirements ({0}):".format(len(report["required_versions"])))
    for version in sorted(report["required_versions"].keys()):
        lines.append("  - {0}".format(version))
    lines.append("")
    plans = report.get("minimal_runtime_plans", [])
    lines.append("Minimal runtime plans ({0}):".format(len(plans)))
    if not plans:
        lines.append("  none")
    else:
        for plan in plans:
            line = "  - {0}: {1}".format(plan["name"], plan["status"])
            if plan.get("selected_candidate"):
                line += " via {0}".format(plan["selected_candidate"])
            lines.append(line)
            if plan.get("selected_components"):
                lines.append("    selected components: {0}".format(", ".join(plan["selected_components"])))
            if plan.get("download_scope"):
                lines.append("    scope: {0}".format(plan["download_scope"]))
    lines.append("")
    detail_limit = max(0, int(detail_limit))
    lines.append("Detailed issue samples ({0} total, showing {1}):".format(len(report["issues"]), min(len(report["issues"]), detail_limit)))
    if not report["issues"]:
        lines.append("  none")
    else:
        grouped = defaultdict(list)
        shown = 0
        for item in report["issues"][:detail_limit]:
            grouped[item["kind"]].append(item)
        for kind in sorted(grouped):
            lines.append("  {0}:".format(kind))
            for item in grouped[kind]:
                label = "{component}:{path}".format(**item)
                if item.get("library"):
                    label += " {0}".format(item["library"])
                if item.get("required_version"):
                    label += " {0}".format(item["required_version"])
                lines.append("    - {0}".format(label))
                detail = item.get("detail", "").strip()
                if detail:
                    for detail_line in detail.splitlines()[:6]:
                        lines.append("      {0}".format(detail_line))
                shown += 1
        if shown < len(report["issues"]):
            lines.append("  ... {0} additional detailed occurrences omitted from text report; see JSON.".format(len(report["issues"]) - shown))
    lines.append("")
    return "\n".join(lines)


def build_report():
    return {
        "scanner_version": 1,
        "native_files_scanned": 0,
        "native_files": [],
        "components": {},
        "needed_libraries": {},
        "required_versions": {},
        "issues": [],
        "runtime_candidates": [],
        "minimal_runtime_plans": [],
    }


def run_scan(args):
    report = build_report()
    issues = report["issues"]
    seen_issues = set()

    work_dir = os.path.abspath(args.work_dir)
    downloads = os.path.join(work_dir, "downloads")
    extracts = os.path.join(work_dir, "extracts")
    ensure_dir(downloads)
    ensure_dir(extracts)

    assets = list(LLAMA_ASSETS) + [FFMPEG_ASSET, PYTHON_ASSET]
    extracted_assets = {}
    for asset in assets:
        archive = download(asset, downloads, issues, seen_issues)
        if not archive:
            continue
        root = extract_asset(asset, archive, extracts, issues, seen_issues)
        if not root:
            continue
        extracted_assets[asset["component"]] = root
        scan_native_tree(asset["component"], root, report, issues, seen_issues)

    packages = []
    if args.python_probe == "audio":
        packages = AUDIO_PROBE_PACKAGES
    elif args.python_probe == "full":
        packages = FULL_PROBE_PACKAGES

    python_root = extracted_assets.get(PYTHON_ASSET["component"])
    python = find_python_executable(python_root) if python_root else None
    if packages and not python:
        add_unique_issue(
            issues,
            seen_issues,
            issue("python-probe", PYTHON_ASSET["component"], "python_unavailable", "", "", "could not find Python executable"),
        )
    elif packages and ensure_python_tools(python, issues, seen_issues):
        wheelhouse = os.path.join(work_dir, "wheelhouse")
        pip_download_packages(python, packages, wheelhouse, issues, seen_issues)
        unpack_root = os.path.join(work_dir, "wheels-unpacked")
        wheel_roots = unpack_wheels(wheelhouse, unpack_root, issues, seen_issues)
        for wheel_name, wheel_root in wheel_roots:
            scan_native_tree("python-wheel:{0}".format(wheel_name), wheel_root, report, issues, seen_issues)
        if args.import_probes:
            run_import_probes(python, wheelhouse, work_dir, issues, seen_issues)

    finalize_runtime_plans(report, args.python_probe)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description="Scan managed Teams ASR native dependencies.")
    parser.add_argument("--work-dir", default="", help="Persistent work directory for downloads/extracts.")
    parser.add_argument("--report-json", default="", help="Path for JSON report.")
    parser.add_argument("--report-text", default="", help="Path for text report.")
    parser.add_argument(
        "--python-probe",
        choices=("none", "audio", "full"),
        default="audio",
        help="Python wheel scope to scan. full includes torch and can be large.",
    )
    parser.add_argument(
        "--skip-import-probes",
        dest="import_probes",
        action="store_false",
        help="Skip per-module Python import checks.",
    )
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=200,
        help="Maximum per-file issue occurrences to print in the text report.",
    )
    parser.add_argument("--no-fail", action="store_true", help="Write reports but exit zero even with issues.")
    args = parser.parse_args(argv)

    remove_work_dir = False
    if not args.work_dir:
        args.work_dir = tempfile.mkdtemp(prefix="teams-asr-dependency-scan-")
        remove_work_dir = True
    ensure_dir(args.work_dir)
    if not args.report_json:
        args.report_json = os.path.join(args.work_dir, "teams-asr-dependency-scan.json")
    if not args.report_text:
        args.report_text = os.path.join(args.work_dir, "teams-asr-dependency-scan.txt")

    try:
        try:
            report = run_scan(args)
        except Exception as exc:  # noqa: BLE001 - CI diagnostic should always produce artifacts.
            report = build_report()
            report["issues"].append(
                issue("scanner", args.work_dir, "scanner_failed", "", "", "{0}\n{1}".format(exc, traceback.format_exc()))
            )
            finalize_runtime_plans(report, args.python_probe)
        ensure_dir(os.path.dirname(os.path.abspath(args.report_json)))
        ensure_dir(os.path.dirname(os.path.abspath(args.report_text)))
        write_json(args.report_json, report)
        rendered = render_text(report, args.detail_limit)
        with open(args.report_text, "w") as fh:
            fh.write(rendered)
        sys.stdout.write(rendered)
        return 0 if args.no_fail or not report["issues"] else 1
    finally:
        if remove_work_dir:
            shutil.rmtree(args.work_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
