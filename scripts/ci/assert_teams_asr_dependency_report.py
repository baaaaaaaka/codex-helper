#!/usr/bin/env python3
"""Assert Teams ASR dependency scan reports cover known compatibility findings."""

import argparse
import json
import os
import sys


KNOWN_BASELINES = {
    "centos7": {
        "missing_libraries": ["libsndfile", "libvulkan.so.1"],
        "missing_versions": ["GLIBC_2.34", "GLIBCXX_3.4.29"],
    },
    "rocky8": {
        "missing_libraries": ["libvulkan.so.1"],
        "missing_versions": ["GLIBC_2.34", "GLIBCXX_3.4.29"],
    },
    "ubuntu20.04": {
        "missing_libraries": ["libvulkan.so.1"],
        "missing_versions": ["GLIBC_2.34", "GLIBCXX_3.4.29"],
    },
}

FORBIDDEN_DEFAULT_MARKERS = [
    "libcuda",
    "cublas",
    "cudnn",
    "cufft",
    "cusparse",
    "nccl",
    "nvrtc",
    "python-wheel:torch",
]


def normalize_label(label):
    return "".join(ch for ch in label.strip().lower() if ch.isalnum() or ch == ".")


def load_report(path):
    with open(path) as fh:
        return json.load(fh)


def report_missing_libraries(report):
    values = set(report.get("missing_libraries", []))
    for item in report.get("issues", []):
        if item.get("kind") == "missing_library" and item.get("library"):
            values.add(item["library"])
    return values


def report_missing_versions(report):
    values = set()
    for item in report.get("missing_symbol_versions", []):
        if item.get("required_version"):
            values.add(item["required_version"])
    for item in report.get("issues", []):
        if item.get("kind") == "missing_symbol_version" and item.get("required_version"):
            values.add(item["required_version"])
    return values


def components_and_libraries_text(report):
    values = []
    values.extend(report.get("needed_libraries", []))
    values.extend(report.get("missing_libraries", []))
    values.extend(report.get("components", {}).keys())
    for record in report.get("native_files", []):
        values.append(record.get("component", ""))
        values.extend(record.get("needed", []))
    for item in report.get("issues", []):
        values.append(item.get("component", ""))
        values.append(item.get("library", ""))
    return "\n".join(value for value in values if value).lower()


def by_name(items):
    return {item.get("name", ""): item for item in items}


def validate_report(report, label, expected_cpu_plan_status="repairable"):
    errors = []
    normalized = normalize_label(label)
    baseline = KNOWN_BASELINES.get(normalized)
    if not baseline:
        errors.append("no known Teams ASR dependency baseline for label {0!r}".format(label))
        return errors

    if int(report.get("native_files_scanned") or 0) <= 0:
        errors.append("native_files_scanned must be positive")
    if not report.get("issues"):
        errors.append("known incompatible baseline must still report dependency issues")

    libraries = report_missing_libraries(report)
    for library in baseline["missing_libraries"]:
        if library not in libraries:
            errors.append("missing expected library finding {0}".format(library))

    versions = report_missing_versions(report)
    for version in baseline["missing_versions"]:
        if version not in versions:
            errors.append("missing expected symbol version finding {0}".format(version))

    candidates = by_name(report.get("runtime_candidates", []))
    for name in ("llama-cpu-teams-media", "llama-vulkan-teams-media", "transformers-full"):
        if name not in candidates:
            errors.append("missing runtime candidate {0}".format(name))
    if candidates.get("transformers-full", {}).get("status") != "not_scanned":
        errors.append("default audio probe must leave transformers-full as not_scanned")

    plans = by_name(report.get("minimal_runtime_plans", []))
    cpu_plan = plans.get("cpu-only-teams-media")
    if not cpu_plan:
        errors.append("missing cpu-only-teams-media minimal plan")
    elif cpu_plan.get("status") != expected_cpu_plan_status:
        errors.append(
            "cpu-only-teams-media plan status = {0!r}, want {1!r}".format(
                cpu_plan.get("status"), expected_cpu_plan_status
            )
        )

    scan_text = components_and_libraries_text(report)
    for marker in FORBIDDEN_DEFAULT_MARKERS:
        if marker in scan_text:
            errors.append("default audio probe unexpectedly scanned forbidden dependency marker {0}".format(marker))
    return errors


def main(argv=None):
    parser = argparse.ArgumentParser(description="Assert Teams ASR dependency scan report coverage.")
    parser.add_argument("--report-json", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--expected-cpu-plan-status", default="repairable")
    args = parser.parse_args(argv)

    report_path = os.path.abspath(args.report_json)
    report = load_report(report_path)
    errors = validate_report(report, args.label, args.expected_cpu_plan_status)
    if errors:
        for error in errors:
            sys.stderr.write("ERROR: {0}\n".format(error))
        return 1
    sys.stdout.write(
        "Teams ASR dependency report baseline ok for {0}: native_files_scanned={1}, issues={2}\n".format(
            args.label,
            report.get("native_files_scanned", 0),
            len(report.get("issues", [])),
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
