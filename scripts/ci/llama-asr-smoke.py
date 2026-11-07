#!/usr/bin/env python3
"""Real-model smoke test for a llama-asr runtime build.

Starts llama-server exactly the way internal/asr does (CPU, 4 threads,
2048-token context, API key, loopback only), sends one WAV file, and checks
the transcript. Standard library only, so it runs on every CI runner.

usage: llama-asr-smoke.py SERVER MODEL MMPROJ WAV EXPECTED_WORD
"""
import base64
import json
import os
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

PROMPT = ("Transcribe the audio exactly. Do not translate. Preserve the spoken language. "
          "Preserve English words, acronyms, code identifiers, file names, and paths as spoken.")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def log_tail(path, n=60):
    try:
        with open(path, errors="replace") as f:
            return "".join(f.readlines()[-n:])
    except OSError as e:
        return str(e)


def main(server, model, mmproj, wav, expected):
    # llama-server runs in its own directory (as internal/asr runs it), so
    # every path it gets is absolute.
    server, model, mmproj, wav = (os.path.abspath(p) for p in (server, model, mmproj, wav))
    logpath = os.path.abspath("llama-server-smoke.log")
    key = secrets.token_hex(24)
    keyfile = os.path.join(tempfile.mkdtemp(), "key")
    with open(keyfile, "w") as f:
        f.write(key)
    port = free_port()
    args = [server, "-m", model, "--mmproj", mmproj, "--host", "127.0.0.1", "--port", str(port),
            "--api-key-file", keyfile, "-t", "4", "-tb", "4", "-c", "2048", "-np", "1", "--threads-http", "2",
            "--no-webui", "--no-warmup", "--device", "none", "--no-mmproj-offload"]
    env = {k: v for k, v in os.environ.items() if not k.startswith("LLAMA_ARG_")}
    log = open(logpath, "w")
    proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, env=env, cwd=os.path.dirname(server))
    base = "http://127.0.0.1:%d" % port
    try:
        start = time.time()
        while True:
            if proc.poll() is not None:
                raise SystemExit("llama-server exited with %s:\n%s" % (proc.returncode, log_tail(logpath)))
            try:
                with urllib.request.urlopen(base + "/health", timeout=2) as r:
                    if r.status == 200:
                        break
            except OSError:
                pass
            if time.time() - start > 300:
                raise SystemExit("llama-server not healthy after 300 s:\n%s" % log_tail(logpath))
            time.sleep(0.2)
        print("healthy after %.1f s" % (time.time() - start))
        with open(wav, "rb") as f:
            audio = base64.b64encode(f.read()).decode()
        body = json.dumps({
            "messages": [{"role": "user", "content": [
                {"type": "input_audio", "input_audio": {"data": audio, "format": "wav"}},
                {"type": "text", "text": PROMPT}]}],
            "temperature": 0, "max_tokens": 1024, "cache_prompt": False}).encode()
        req = urllib.request.Request(base + "/v1/chat/completions", data=body, headers={
            "Content-Type": "application/json", "Authorization": "Bearer " + key})
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=600) as r:
            out = json.load(r)
        content = out["choices"][0]["message"]["content"]
        print("request %.2f s: %s" % (time.time() - t0, content))
        try:
            urllib.request.urlopen(urllib.request.Request(base + "/v1/chat/completions", data=body, headers={
                "Content-Type": "application/json"}), timeout=60)
            raise SystemExit("server answered without the API key")
        except urllib.error.HTTPError as e:
            if e.code != 401:
                raise
        status = "/proc/%d/status" % proc.pid
        if os.path.exists(status):
            print([l.strip() for l in open(status) if l.startswith("VmHWM")][0])
        if expected.lower() not in content.lower():
            raise SystemExit("transcript lacks %r" % expected)
        print("smoke OK")
    finally:
        proc.kill()
        proc.wait()


if __name__ == "__main__":
    if len(sys.argv) != 6:
        raise SystemExit(__doc__)
    main(*sys.argv[1:])
