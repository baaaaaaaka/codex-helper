#!/usr/bin/env python3
"""Package a staged voice asset directory as a reproducible .tar.gz and
write its pin file.

usage: asr-package.py STAGED_DIR OUT_DIR PLATFORM

STAGED_DIR's name becomes the archive's single top-level directory (the
installer in internal/asr strips it). Entries are sorted, owned by 0:0,
dated 2026-01-01 and stored without symlinks, so the same files always give
the same archive on every OS. Writes OUT_DIR/<name>.tar.gz and
OUT_DIR/<name>.pin.json ({"name", "sha256", "size", "platform"}).
"""
import gzip
import hashlib
import io
import json
import os
import sys
import tarfile

MTIME = 1767225600  # 2026-01-01T00:00:00Z


def main(staged, out_dir, platform):
    staged = os.path.abspath(staged)
    name = os.path.basename(staged.rstrip(os.sep))
    files = []
    for root, dirs, names in os.walk(staged):
        dirs.sort()
        for n in sorted(names):
            p = os.path.join(root, n)
            if os.path.islink(p):
                raise SystemExit("symlink in staged dir: %s" % p)
            files.append(p)
    files.sort(key=lambda p: os.path.relpath(p, staged).replace(os.sep, "/"))
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tar:
        top = tarfile.TarInfo(name + "/")
        top.type, top.mode, top.mtime = tarfile.DIRTYPE, 0o755, MTIME
        tar.addfile(top)
        for p in files:
            rel = os.path.relpath(p, staged).replace(os.sep, "/")
            info = tarfile.TarInfo(name + "/" + rel)
            info.size = os.path.getsize(p)
            info.mtime = MTIME
            # Windows has no exec bit: binaries and libraries are executable.
            exe = os.access(p, os.X_OK) and os.name != "nt"
            exe = exe or rel.lower().endswith((".exe", ".dll", ".so", ".dylib")) or "." not in rel
            info.mode = 0o755 if exe else 0o644
            with open(p, "rb") as f:
                tar.addfile(info, f)
    os.makedirs(out_dir, exist_ok=True)
    archive = os.path.join(out_dir, name + ".tar.gz")
    with open(archive, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as gz:
            gz.write(buf.getvalue())
    with open(archive, "rb") as f:
        data = f.read()
    pin = {"name": name + ".tar.gz", "sha256": hashlib.sha256(data).hexdigest(), "size": len(data), "platform": platform}
    with open(os.path.join(out_dir, name + ".pin.json"), "w") as f:
        json.dump(pin, f)
        f.write("\n")
    print(json.dumps(pin))


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit(__doc__)
    main(*sys.argv[1:])
