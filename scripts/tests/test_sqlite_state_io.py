import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "ci" / "verify_sqlite_state_io.py"
spec = importlib.util.spec_from_file_location("state_io", SCRIPT)
state_io = importlib.util.module_from_spec(spec)
spec.loader.exec_module(state_io)


class SQLiteStateIOTests(unittest.TestCase):
    def trace(self, quiet_write="", heartbeat_bytes=4096):
        lines = []
        for phase in ("migration", "read-only", "unchanged", "heartbeat", "settings", "cold-open"):
            lines.append(f'41 write(1<pipe:[123]>, "CXP_PERF_BEGIN {phase} rows=100000\\n", 64) = 64')
            if phase in ("heartbeat", "settings"):
                lines += [
                    '42 pwrite64(7</fixture/config.sqlite-journal>, "...", 4096, 0 <unfinished ...>',
                    '43 pread64(8</unrelated.sqlite>, "...", 4096, 0) = 4096',
                    f'42 <... pwrite64 resumed>) = {heartbeat_bytes}',
                    '42 fsync(7</fixture/config.sqlite-journal>) = 0',
                ]
            elif phase == "read-only":
                lines.append(quiet_write or '42 pread64(7</fixture/config.sqlite>, "...", 4096, 0) = 4096')
            lines.append(f'41 write(1<pipe:[123]>, "CXP_PERF_END {phase} rows=100000\\n", 64) = 64')
        return "\n".join(lines)

    def verify(self, trace):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trace"
            path.write_text(trace)
            with contextlib.redirect_stdout(io.StringIO()) as output:
                state_io.verify(path)
            return output.getvalue()

    def test_split_calls_count_journal_bytes_and_ignore_other_databases(self):
        result = self.verify(self.trace())
        self.assertIn("pwrite64_bytes=4096", result)
        self.assertIn("fsync_calls=1", result)

    def test_quiet_path_write_is_rejected(self):
        with self.assertRaises(AssertionError):
            self.verify(self.trace('42 pwrite64(7</fixture/config.sqlite>, "...", 8, 0) = 8'))

    def test_excessive_heartbeat_writes_are_rejected(self):
        with self.assertRaises(AssertionError):
            self.verify(self.trace(heartbeat_bytes=4 * 1024 * 1024))

    def test_missing_phase_is_rejected(self):
        with self.assertRaises(AssertionError):
            self.verify(self.trace().replace("CXP_PERF_BEGIN unchanged", "UNMEASURED"))
