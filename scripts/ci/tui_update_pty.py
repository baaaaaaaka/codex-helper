#!/usr/bin/env python3
"""Production CXP boot handoff in a real POSIX terminal; no external services."""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pty
import select
import shutil
import signal
import struct
import tempfile
import termios
import time


class Terminal:
    def __init__(self, binary: Path, root: Path, env: dict[str, str]):
        self.started = time.monotonic()
        self.output = bytearray()
        grant_read, grant_write = os.pipe()
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.close(grant_write)
            os.read(grant_read, 1)
            os.close(grant_read)
            os.chdir(root)
            os.execve(str(binary), [str(binary), "--config", str(root / "config.json"),
                                    "tui", "--codex-dir", str(root / "codex")], env)
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 100, 0, 0))
        self.original = termios.tcgetattr(self.fd)
        os.close(grant_read)
        os.write(grant_write, b"1")
        os.close(grant_write)
        self.exited = False

    def pump(self):
        if select.select([self.fd], [], [], 0.02)[0]:
            try:
                self.output.extend(os.read(self.fd, 65536))
            except OSError:
                pass

    def until(self, predicate, label):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            self.pump()
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.exited = True
                raise AssertionError(f"{label}: exited {status}: {self.output[-3000:]!r}")
            if predicate():
                return
        raise AssertionError(f"{label}: timeout: {self.output[-3000:]!r}")

    def interactive(self):
        return bool(self.output) and not (termios.tcgetattr(self.fd)[3] & termios.ICANON)

    def quit(self):
        os.write(self.fd, b"q")
        self.wait_exit(0)

    def wait_exit(self, expected):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            self.pump()
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.exited = True
                assert os.waitstatus_to_exitcode(status) == expected, (status, self.output[-3000:])
                # Startup recovery must put the terminal back into cooked mode.
                final = termios.tcgetattr(self.fd)
                assert (final[3] & (termios.ECHO | termios.ICANON)) == (
                    self.original[3] & (termios.ECHO | termios.ICANON)), final
                return
        raise AssertionError(f"TUI did not quit: {self.output[-3000:]!r}")

    def close(self):
        if not self.exited:
            with contextlib.suppress(ProcessLookupError):
                os.kill(self.pid, signal.SIGTERM)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                pid, _ = os.waitpid(self.pid, os.WNOHANG)
                if pid:
                    self.exited = True
                    break
                self.pump()
            if not self.exited:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(self.pid, signal.SIGKILL)
                os.waitpid(self.pid, 0)
        os.close(self.fd)


@contextlib.contextmanager
def installed(binary: Path):
    with tempfile.TemporaryDirectory(prefix="cxp-tui-pty-") as name:
        root = Path(name)
        entry = root / "cxp"
        shutil.copyfile(binary, entry)
        entry.chmod(0o755)
        runtime = root / ".cxp-runtime"
        for version in ("v1.0.0", "v2.0.0", "v3.0.0"):
            dest = runtime / "versions" / version / "cxp"
            dest.parent.mkdir(parents=True)
            shutil.copyfile(binary, dest)
            dest.chmod(0o755)
        (runtime / "runtime.lock").touch()
        (root / "codex").mkdir()
        # Existing, direct-mode configuration: no question, SSH or model service.
        (root / "config.json").write_text(json.dumps({"proxyEnabled": False}))
        env = {k: v for k, v in os.environ.items()
               if not k.upper().startswith(("CXP_RUNTIME", "CXP_BOOT"))
               and k.upper() not in ("CXP_ENTRY_PATH", "CODEX_DIR", "CODEX_HOME")}
        env.update(HOME=name, USERPROFILE=name, CODEX_HOME=str(root / "codex"),
                   CODEX_DIR=str(root / "codex"), XDG_CONFIG_HOME=str(root / "config"),
                   XDG_CACHE_HOME=str(root / "cache"), CODEX_HELPER_STATE_DIR=str(root / "state"),
                   CXP_RUNTIME_FORCE="1", CXP_AUTO_UPDATE="1", CODEX_PROXY_VERSION="latest",
                   TERM="xterm-256color", HTTP_PROXY="http://127.0.0.1:1",
                   HTTPS_PROXY="http://127.0.0.1:1", ALL_PROXY="http://127.0.0.1:1",
                   CXP_BOOT_TEST_DIR=name)
        terms = []
        def start():
            term = Terminal(entry, root, env)
            terms.append(term)
            return term
        try:
            yield root, runtime, env, start
        finally:
            for term in terms:
                term.close()
            runtime.chmod(0o755)
            for file in (runtime / "boot.json", runtime / "runtime.lock"):
                if file.exists():
                    file.chmod(0o600)


def record(runtime: Path, candidate: Path | None = None, good="v1.0.0", generation=1):
    data = {"schema": 1, "generation": generation, "good": good}
    if candidate is not None:
        data["candidate"] = {"version": candidate.parent.name,
                             "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest()}
    path = runtime / "boot.json"
    temp = runtime / "test-record"
    temp.write_text(json.dumps(data))
    temp.replace(path)


def read_record(runtime: Path):
    return json.loads((runtime / "boot.json").read_text())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--fault-fixture", required=True, type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve()
    # Keep an old TUI alive while a second window confirms the candidate.
    with installed(binary) as (root, runtime, env, start):
        record(runtime)
        old = start()
        old.until(old.interactive, "old TUI")
        elapsed = time.monotonic() - old.started
        memory = Path(f"/proc/{old.pid}/smaps_rollup")
        if memory.exists():
            usage = {line.split(":", 1)[0]: line.split(":", 1)[1].strip()
                     for line in memory.read_text().splitlines() if ":" in line}
            print(f"Confirmed TUI first frame: {elapsed:.3f}s; "
                  f"bootstrap RSS={usage.get('Rss')}, PSS={usage.get('Pss')}, "
                  f"private dirty={usage.get('Private_Dirty')}")
        record(runtime, runtime / "versions/v2.0.0/cxp", generation=2)
        new = start()
        new.until(lambda: read_record(runtime)["good"] == "v2.0.0", "candidate first frame")
        assert os.waitpid(old.pid, os.WNOHANG) == (0, 0), "old window was restarted"
        new.quit()
        old.quit()
        # A busy/read-only update directory must not delay ordinary startup.
        record(runtime, runtime / "versions/v3.0.0/cxp", good="v2.0.0", generation=4)
        before = (runtime / "boot.json").read_bytes()
        with (runtime / "runtime.lock").open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            runtime.chmod(0o555)
            (runtime / "runtime.lock").chmod(0o444)
            (runtime / "boot.json").chmod(0o444)
            next_window = start()
            next_window.until(next_window.interactive, "read-only/busy startup")
            next_window.quit()
        assert before == (runtime / "boot.json").read_bytes(), "startup needed a repair write"
        runtime.chmod(0o755)
        (runtime / "boot.json").chmod(0o600)
        env["CXP_AUTO_UPDATE"] = "off"
        manual = start()
        manual.until(manual.interactive, "automatic updates disabled")
        manual.quit()
        assert before == (runtime / "boot.json").read_bytes(), "opt-out still consumed candidate"
        (runtime / "boot.json").write_text("{broken")
        base = start()
        base.until(base.interactive, "corrupt record base fallback")
        base.quit()
        assert (runtime / "boot.json").read_text() == "{broken"
    # --version succeeds for this candidate, but real startup exits before READY.
    with installed(binary) as (root, runtime, env, start):
        candidate = runtime / "versions/v2.0.0/cxp"
        shutil.copyfile(args.fault_fixture, candidate)
        candidate.chmod(0o755)
        record(runtime, candidate)
        env["CXP_BOOT_TEST_MODE"] = "crash"
        fallback = start()
        fallback.until(fallback.interactive, "same-invocation rollback")
        fallback.quit()
        state = read_record(runtime)
        assert state["good"] == "v1.0.0" and state["candidate"]["attempt"]
        again = start()
        again.until(again.interactive, "fresh invocation after failure")
        again.quit()
        events = (root / "events").read_text()
        assert events.count("entered v2.0.0") == 1 and "input v2.0.0" not in events
    # Ctrl+C reaches the candidate's foreground process group during startup.
    # It must cancel this invocation, with no fallback that replays input.
    with installed(binary) as (root, runtime, env, start):
        candidate = runtime / "versions/v2.0.0/cxp"
        shutil.copyfile(args.fault_fixture, candidate)
        candidate.chmod(0o755)
        record(runtime, candidate)
        env["CXP_BOOT_TEST_MODE"] = "hang"
        cancelled = start()
        cancelled.until(lambda: (root / "events").exists()
                        and "entered v2.0.0" in (root / "events").read_text(),
                        "candidate before Ctrl+C")
        os.write(cancelled.fd, b"\x03")
        cancelled.wait_exit(130)
        assert read_record(runtime)["good"] == "v1.0.0"
        assert "entered v1.0.0" not in (root / "events").read_text()
    print("Production TUI PTY safety checks passed")


if __name__ == "__main__":
    main()
