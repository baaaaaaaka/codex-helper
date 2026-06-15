from __future__ import annotations

import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "ci" / "teams_asr_dependency_scan.py"


def load_scanner():
    spec = importlib.util.spec_from_file_location("teams_asr_dependency_scan", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TeamsASRDependencyScanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scanner = load_scanner()

    def test_parse_ldd_issues_collects_missing_libraries_and_versions(self) -> None:
        output = """
        linux-vdso.so.1 (0x00007ffd)
        libvulkan.so.1 => not found
        /tmp/llama-mtmd-cli: /lib64/libc.so.6: version `GLIBC_2.34' not found (required by /tmp/llama-mtmd-cli)
        /tmp/llama-mtmd-cli: /lib64/libstdc++.so.6: version `GLIBCXX_3.4.29' not found (required by /tmp/llama-mtmd-cli)
        """

        issues = self.scanner.parse_ldd_issues("llama", "llama-mtmd-cli", output)

        got = {
            (item["kind"], item["library"], item["required_version"])
            for item in issues
        }
        self.assertEqual(
            got,
            {
                ("missing_library", "libvulkan.so.1", ""),
                ("missing_symbol_version", "/lib64/libc.so.6", "GLIBC_2.34"),
                ("missing_symbol_version", "/lib64/libstdc++.so.6", "GLIBCXX_3.4.29"),
            },
        )

    def test_parse_ldd_issues_collects_cannot_open_shared_object(self) -> None:
        output = "ImportError: libsndfile.so.1: cannot open shared object file: No such file or directory"

        issues = self.scanner.parse_ldd_issues("python-wheel", "soundfile.py", output)

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["kind"], "missing_library")
        self.assertEqual(issues[0]["library"], "libsndfile.so.1")

    def test_parse_import_issues_recognizes_libsndfile_text(self) -> None:
        output = """
        Traceback (most recent call last):
          File "<string>", line 1, in <module>
        OSError: sndfile library not found
        """

        issues = self.scanner.parse_import_issues("python-import-probe", "soundfile", output)

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["library"], "libsndfile")

    def test_download_rejects_cached_and_downloaded_size_mismatch(self) -> None:
        class FakeResponse:
            def __init__(self, body: bytes) -> None:
                self.body = body
                self.offset = 0

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                return None

            def read(self, _size: int) -> bytes:
                if self.offset:
                    return b""
                self.offset = len(self.body)
                return self.body

        original_urlopen = self.scanner.urllib.request.urlopen
        original_sleep = self.scanner.time.sleep
        bodies = [b"good"]

        def fake_urlopen(_request, timeout=120):
            return FakeResponse(bodies[-1])

        self.scanner.urllib.request.urlopen = fake_urlopen
        self.scanner.time.sleep = lambda _seconds: None
        self.addCleanup(lambda: setattr(self.scanner.urllib.request, "urlopen", original_urlopen))
        self.addCleanup(lambda: setattr(self.scanner.time, "sleep", original_sleep))

        with tempfile.TemporaryDirectory() as temp_dir:
            asset = {
                "component": "asset",
                "name": "asset.bin",
                "url": "https://example.invalid/asset.bin",
                "sha256": hashlib.sha256(b"good").hexdigest(),
                "size": 4,
            }
            stale = Path(temp_dir) / "asset.bin"
            stale.write_bytes(b"bad")
            issues = []
            got = self.scanner.download(asset, temp_dir, issues, set())
            self.assertEqual(Path(got).read_bytes(), b"good")
            self.assertEqual(issues, [])

            bodies.append(b"bad")
            asset["name"] = "bad-size.bin"
            asset["sha256"] = hashlib.sha256(b"bad").hexdigest()
            issues = []
            got = self.scanner.download(asset, temp_dir, issues, set())
            self.assertIsNone(got)
            self.assertEqual(issues[0]["kind"], "download_failed")
            self.assertIn("size mismatch", issues[0]["detail"])

    def test_render_text_groups_all_issues(self) -> None:
        report = self.scanner.build_report()
        report["native_files_scanned"] = 2
        report["components"]["llama"] = {"native_files_scanned": 1}
        report["components"]["python"] = {"native_files_scanned": 1}
        report["needed_libraries"]["libc.so.6"] = True
        report["required_versions"]["GLIBC_2.34"] = True
        report["issues"].append(
            self.scanner.issue("llama", "cli", "missing_symbol_version", "libc.so.6", "GLIBC_2.34", "detail")
        )
        report["issues"].append(
            self.scanner.issue("python", "soundfile", "missing_library", "libsndfile", "", "detail")
        )

        text = self.scanner.render_text(report)

        self.assertIn("Missing libraries (1):", text)
        self.assertIn("Missing symbol versions (1):", text)
        self.assertIn("Detailed issue samples (2 total, showing 2):", text)
        self.assertIn("missing_symbol_version", text)
        self.assertIn("libsndfile", text)

    def test_render_text_limits_detailed_occurrences(self) -> None:
        report = self.scanner.build_report()
        for i in range(3):
            report["issues"].append(
                self.scanner.issue("llama", f"cli-{i}", "missing_library", "libssl.so.3", "", "detail")
            )

        text = self.scanner.render_text(report, detail_limit=1)

        self.assertIn("Missing libraries (1):", text)
        self.assertIn("Detailed issue samples (3 total, showing 1):", text)
        self.assertIn("2 additional detailed occurrences omitted", text)

    def test_pip_download_packages_can_skip_transitive_dependencies(self) -> None:
        calls = []

        def fake_run_command(argv, env=None, timeout=120):
            calls.append(argv)
            return {"returncode": 0, "stdout": "", "stderr": "", "argv": argv}

        self.scanner.run_command = fake_run_command
        with tempfile.TemporaryDirectory() as temp_dir:
            self.scanner.pip_download_packages(
                "python",
                [("soundfile", True), ("qwen-asr==0.0.6", False)],
                temp_dir,
                [],
                set(),
            )

        self.assertNotIn("--no-deps", calls[0])
        self.assertIn("--no-deps", calls[1])
        self.assertEqual(calls[1][-1], "qwen-asr==0.0.6")

    def test_decode_process_output_replaces_non_utf8_bytes(self) -> None:
        text = self.scanner.decode_process_output(b"ok\xff")

        self.assertEqual(text, "ok\ufffd")

    def test_audio_probe_scope_excludes_torch_and_cuda_packages(self) -> None:
        packages = [entry[0] if isinstance(entry, tuple) else entry for entry in self.scanner.AUDIO_PROBE_PACKAGES]
        joined = " ".join(packages).lower()

        self.assertIn("soundfile==0.13.1", packages)
        self.assertNotIn("torch", joined)
        for marker in ("cuda", "cudnn", "cublas", "nccl"):
            self.assertNotIn(marker, joined)

    def test_minimal_cpu_plan_ignores_unselected_candidate_issues(self) -> None:
        report = self.scanner.build_report()
        report["components"]["llama-cpu-linux-x64"] = {"native_files_scanned": 1}
        report["components"]["imageio-ffmpeg-linux-x64"] = {"native_files_scanned": 1}
        report["components"]["llama-vulkan-linux-x64"] = {"native_files_scanned": 1}
        report["components"]["python-import-probe"] = {"native_files_scanned": 0}
        report["issues"].append(
            self.scanner.issue("llama-vulkan-linux-x64", "llama-mtmd-cli", "missing_library", "libvulkan.so.1", "", "detail")
        )
        report["issues"].append(
            self.scanner.issue("python-import-probe", "soundfile", "missing_library", "libsndfile", "", "detail")
        )

        self.scanner.finalize_runtime_plans(report, "audio")

        plans = {plan["name"]: plan for plan in report["minimal_runtime_plans"]}
        cpu_plan = plans["cpu-only-teams-media"]
        self.assertTrue(cpu_plan["usable"])
        self.assertEqual(cpu_plan["selected_candidate"], "llama-cpu-teams-media")
        self.assertEqual(cpu_plan["selected_components"], ["llama-cpu-linux-x64", "imageio-ffmpeg-linux-x64"])
        self.assertNotIn("llama-vulkan-linux-x64", cpu_plan["selected_components"])
        self.assertNotIn("python-build-standalone-linux-x64", cpu_plan["selected_components"])

        candidates = {candidate["name"]: candidate for candidate in report["runtime_candidates"]}
        self.assertFalse(candidates["llama-vulkan-teams-media"]["usable"])
        self.assertEqual(candidates["llama-vulkan-teams-media"]["status"], "blocked")

    def test_minimal_cpu_plan_repairs_cpu_runtime_issue(self) -> None:
        report = self.scanner.build_report()
        report["components"]["llama-cpu-linux-x64"] = {"native_files_scanned": 1}
        report["components"]["imageio-ffmpeg-linux-x64"] = {"native_files_scanned": 1}
        report["issues"].append(
            self.scanner.issue(
                "llama-cpu-linux-x64",
                "llama-mtmd-cli",
                "missing_symbol_version",
                "/lib64/libc.so.6",
                "GLIBC_2.34",
                "detail",
            )
        )

        self.scanner.finalize_runtime_plans(report, "audio")

        plans = {plan["name"]: plan for plan in report["minimal_runtime_plans"]}
        self.assertTrue(plans["cpu-only-teams-media"]["usable"])
        self.assertEqual(plans["cpu-only-teams-media"]["status"], "repairable")
        self.assertEqual(plans["cpu-only-teams-media"]["selected_candidate"], "llama-cpu-teams-media")
        candidates = {candidate["name"]: candidate for candidate in report["runtime_candidates"]}
        self.assertEqual(candidates["llama-cpu-teams-media"]["repair"], "native_compat_patchelf")
        self.assertEqual(candidates["llama-cpu-teams-media"]["repair_profile"], "linux-x64-glibc-2.35-conda-runtime-v1")
        self.assertEqual(candidates["llama-cpu-teams-media"]["repairable_issues"][0]["required_version"], "GLIBC_2.34")

    def test_minimal_cpu_plan_repairs_glibc238_with_glibc239_profile(self) -> None:
        report = self.scanner.build_report()
        report["components"]["llama-cpu-linux-x64"] = {"native_files_scanned": 1}
        report["components"]["imageio-ffmpeg-linux-x64"] = {"native_files_scanned": 1}
        report["issues"].append(
            self.scanner.issue(
                "llama-cpu-linux-x64",
                "llama-mtmd-cli",
                "missing_symbol_version",
                "/lib64/libc.so.6",
                "GLIBC_2.38",
                "detail",
            )
        )

        self.scanner.finalize_runtime_plans(report, "audio")

        plans = {plan["name"]: plan for plan in report["minimal_runtime_plans"]}
        self.assertTrue(plans["cpu-only-teams-media"]["usable"])
        self.assertEqual(plans["cpu-only-teams-media"]["status"], "repairable")
        candidates = {candidate["name"]: candidate for candidate in report["runtime_candidates"]}
        self.assertTrue(candidates["llama-cpu-teams-media"]["usable"])
        self.assertEqual(candidates["llama-cpu-teams-media"]["status"], "repairable")
        self.assertEqual(candidates["llama-cpu-teams-media"]["repair_profile"], "linux-x64-glibc-2.39-conda-runtime-v1")

    def test_minimal_cpu_plan_repairs_full_path_runtime_library_issue(self) -> None:
        report = self.scanner.build_report()
        report["components"]["llama-cpu-linux-x64"] = {"native_files_scanned": 1}
        report["components"]["imageio-ffmpeg-linux-x64"] = {"native_files_scanned": 1}
        report["issues"].append(
            self.scanner.issue(
                "llama-cpu-linux-x64",
                "llama-mtmd-cli",
                "missing_library",
                "/lib64/libstdc++.so.6",
                "",
                "detail",
            )
        )

        self.scanner.finalize_runtime_plans(report, "audio")

        candidates = {candidate["name"]: candidate for candidate in report["runtime_candidates"]}
        self.assertTrue(candidates["llama-cpu-teams-media"]["usable"])
        self.assertEqual(candidates["llama-cpu-teams-media"]["status"], "repairable")

    def test_minimal_cpu_plan_repairs_nss_runtime_library_issue(self) -> None:
        report = self.scanner.build_report()
        report["components"]["llama-cpu-linux-x64"] = {"native_files_scanned": 1}
        report["components"]["imageio-ffmpeg-linux-x64"] = {"native_files_scanned": 1}
        report["issues"].append(
            self.scanner.issue(
                "llama-cpu-linux-x64",
                "llama-mtmd-cli",
                "missing_library",
                "libnss_files.so.2",
                "",
                "detail",
            )
        )

        self.scanner.finalize_runtime_plans(report, "audio")

        candidates = {candidate["name"]: candidate for candidate in report["runtime_candidates"]}
        self.assertTrue(candidates["llama-cpu-teams-media"]["usable"])
        self.assertEqual(candidates["llama-cpu-teams-media"]["repair_profile"], "linux-x64-glibc-2.35-conda-runtime-v1")

    def test_minimal_cpu_plan_blocks_mixed_unrepairable_library_with_repairable_symbol(self) -> None:
        report = self.scanner.build_report()
        report["components"]["llama-cpu-linux-x64"] = {"native_files_scanned": 1}
        report["components"]["imageio-ffmpeg-linux-x64"] = {"native_files_scanned": 1}
        report["issues"].append(
            self.scanner.issue(
                "llama-cpu-linux-x64",
                "llama-mtmd-cli",
                "missing_library",
                "libvulkan.so.1",
                "",
                "detail",
            )
        )
        report["issues"].append(
            self.scanner.issue(
                "llama-cpu-linux-x64",
                "llama-mtmd-cli",
                "missing_symbol_version",
                "/lib64/libc.so.6",
                "GLIBC_2.38",
                "detail",
            )
        )

        self.scanner.finalize_runtime_plans(report, "audio")

        candidates = {candidate["name"]: candidate for candidate in report["runtime_candidates"]}
        self.assertFalse(candidates["llama-cpu-teams-media"]["usable"])
        self.assertEqual(candidates["llama-cpu-teams-media"]["status"], "blocked")

    def test_minimal_cpu_plan_blocks_unrepairable_runtime_issue(self) -> None:
        for library, version in (
            ("libvulkan.so.1", ""),
            ("/lib64/libc.so.6", "GLIBC_2.40"),
            ("/lib64/libc.so.6", "GLIBC_2.35.1"),
        ):
            with self.subTest(library=library, version=version):
                report = self.scanner.build_report()
                report["components"]["llama-cpu-linux-x64"] = {"native_files_scanned": 1}
                report["components"]["imageio-ffmpeg-linux-x64"] = {"native_files_scanned": 1}
                kind = "missing_symbol_version" if version else "missing_library"
                report["issues"].append(
                    self.scanner.issue("llama-cpu-linux-x64", "llama-mtmd-cli", kind, library, version, "detail")
                )

                self.scanner.finalize_runtime_plans(report, "audio")

                plans = {plan["name"]: plan for plan in report["minimal_runtime_plans"]}
                self.assertFalse(plans["cpu-only-teams-media"]["usable"])
                self.assertEqual(plans["cpu-only-teams-media"]["status"], "blocked")
                candidates = {candidate["name"]: candidate for candidate in report["runtime_candidates"]}
                self.assertFalse(candidates["llama-cpu-teams-media"]["usable"])
                self.assertEqual(candidates["llama-cpu-teams-media"]["status"], "blocked")

    def test_transformers_candidate_requires_full_probe(self) -> None:
        report = self.scanner.build_report()
        report["components"]["python-build-standalone-linux-x64"] = {"native_files_scanned": 1}
        report["components"]["python-wheel:qwen_asr.whl"] = {"native_files_scanned": 1}

        self.scanner.finalize_runtime_plans(report, "audio")

        candidates = {candidate["name"]: candidate for candidate in report["runtime_candidates"]}
        self.assertFalse(candidates["transformers-full"]["usable"])
        self.assertEqual(candidates["transformers-full"]["status"], "not_scanned")
        self.assertIn("--python-probe full", candidates["transformers-full"]["reason"])


if __name__ == "__main__":
    unittest.main()
