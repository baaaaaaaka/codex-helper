#!/usr/bin/env python3
"""Locate the native binary belonging to one installed Linux npm wrapper."""
import json
import os
from pathlib import Path
import platform
import subprocess
import sys


def main():
    if len(sys.argv) != 3:
        raise ValueError('usage: find_native_codex.py NODE_MODULES EXPECTED_VERSION')
    root, expected = Path(sys.argv[1]), sys.argv[2]
    arch = {'x86_64': ('x64', 'x86_64'), 'aarch64': ('arm64', 'aarch64')}.get(platform.machine())
    if sys.platform != 'linux' or not arch:
        raise ValueError(f'unsupported platform: {sys.platform} {platform.machine()}')
    wrapper = root / '@openai' / 'codex'
    metadata = json.loads((wrapper / 'package.json').read_text())
    if metadata.get('version') != expected:
        raise ValueError(f'wrapper version {metadata.get("version")!r}, expected {expected!r}')
    triple = f'{arch[1]}-unknown-linux-musl'
    package_name = f'codex-linux-{arch[0]}'
    native_packages = (wrapper / 'node_modules' / '@openai' / package_name,
                       root / '@openai' / package_name)
    # Older versions use codex/codex; recent versions use bin/codex.
    for package in (*native_packages, wrapper):
        for subdir in ('bin', 'codex'):
            binary = package / 'vendor' / triple / subdir / 'codex'
            if not binary.is_file() or not os.access(binary, os.X_OK):
                continue
            version = subprocess.run([str(binary), '--version'], check=True, capture_output=True, text=True).stdout.strip().split()
            if not version or version[-1] != expected:
                raise ValueError(f'{binary}: version {version!r}, expected {expected!r}')
            print(binary.resolve())
            return
    raise ValueError(f'no executable native Codex {expected} for {triple} under {root}')


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f'Codex native lookup failed: {exc}', file=sys.stderr)
        sys.exit(1)
