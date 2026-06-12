from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "ci" / "assert_teams_asr_dependency_report.py"


def load_assertions():
    spec = importlib.util.spec_from_file_location("assert_teams_asr_dependency_report", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TeamsASRDependencyReportAssertTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertions = load_assertions()

    def report(self):
        return {
            "native_files_scanned": 12,
            "needed_libraries": ["libc.so.6", "libstdc++.so.6"],
            "missing_libraries": ["libsndfile", "libvulkan.so.1"],
            "missing_symbol_versions": [
                {"library": "/lib64/libc.so.6", "required_version": "GLIBC_2.34"},
                {"library": "/lib64/libstdc++.so.6", "required_version": "GLIBCXX_3.4.29"},
            ],
            "components": {
                "llama-cpu-linux-x64": {"native_files_scanned": 4},
                "llama-vulkan-linux-x64": {"native_files_scanned": 4},
            },
            "native_files": [
                {"component": "llama-cpu-linux-x64", "needed": ["libc.so.6"]},
                {"component": "python-import-probe", "needed": []},
            ],
            "issues": [
                {"kind": "missing_library", "library": "libsndfile", "component": "python-import-probe"},
                {"kind": "missing_library", "library": "libvulkan.so.1", "component": "llama-vulkan-linux-x64"},
                {
                    "kind": "missing_symbol_version",
                    "library": "/lib64/libc.so.6",
                    "required_version": "GLIBC_2.34",
                    "component": "llama-cpu-linux-x64",
                },
                {
                    "kind": "missing_symbol_version",
                    "library": "/lib64/libstdc++.so.6",
                    "required_version": "GLIBCXX_3.4.29",
                    "component": "llama-cpu-linux-x64",
                },
            ],
            "runtime_candidates": [
                {"name": "llama-cpu-teams-media", "status": "repairable"},
                {"name": "llama-vulkan-teams-media", "status": "blocked"},
                {"name": "transformers-full", "status": "not_scanned"},
            ],
            "minimal_runtime_plans": [
                {"name": "cpu-only-teams-media", "status": "repairable"},
                {"name": "accelerated-or-cpu-teams-media", "status": "repairable"},
            ],
        }

    def test_validate_known_centos_baseline(self) -> None:
        errors = self.assertions.validate_report(self.report(), "CentOS7")

        self.assertEqual(errors, [])

    def test_validate_reports_missing_expected_finding(self) -> None:
        report = self.report()
        report["missing_libraries"] = ["libvulkan.so.1"]
        report["issues"] = [item for item in report["issues"] if item.get("library") != "libsndfile"]

        errors = self.assertions.validate_report(report, "CentOS7")

        self.assertIn("missing expected library finding libsndfile", "\n".join(errors))

    def test_validate_rejects_default_cuda_marker(self) -> None:
        report = self.report()
        report["native_files"].append({"component": "python-wheel:torch", "needed": ["libcuda.so.1"]})

        errors = self.assertions.validate_report(report, "CentOS7")

        text = "\n".join(errors)
        self.assertIn("libcuda", text)
        self.assertIn("python-wheel:torch", text)


if __name__ == "__main__":
    unittest.main()
