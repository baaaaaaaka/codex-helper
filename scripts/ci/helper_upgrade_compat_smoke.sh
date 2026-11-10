#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
export REPO="${REPO:-${GITHUB_REPOSITORY:?REPO is required}}"
export TARGET_TAG="${TARGET_TAG:-${TAG:-${GITHUB_REF_NAME:?TARGET_TAG is required}}}"
python3 scripts/ci/sqlite_upgrade_artifacts.py
