"""Real P2P test runner.

Launches a seeder and N-1 leechers, captures stdout/stderr logs, waits for
outputs, computes SHA-256 and file sizes, and writes a JSON summary.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
import socket

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def make_sample(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(os.urandom(size))


def find_free_port(start_port: int) -> int:
    port = start_port
    while port < start_port + 200:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                port += 1
                continue
            return port
    raise RuntimeError(f"No free port found starting at {start_port}")


def launch(args_list: list[str], cwd: Path, stdout_path: Path, stderr_path: Path):
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stdout = stdout_path.open("w", encoding="utf-8")
    stderr = stderr_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, *args_list],
        cwd=str(cwd),
        stdout=stdout,
        stderr=stderr,
        text=True,
    )
    return proc, stdout, stderr


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--peers", type=int, default=2)
    parser.add_argument("--block-size", type=int, default=1024)
    parser.add_argument("--file-size", type=int, default=10240)
    parser.add_argument("--label", type=str, default=None, help="short label for the run (used in folder name)")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--base-port", type=int, default=11000)
    args = parser.parse_args()

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    short_label = args.label or f"peers{args.peers}_b{args.block_size}_s{args.file_size}"
    run_name = f"{short_label}_{ts}"
    outdir = RESULTS / run_name
    outdir.mkdir(parents=True, exist_ok=True)
    runner_log = outdir / "runner.log"

    def log(message: str) -> None:
        line = f"[{datetime.utcnow().isoformat()}Z] {message}"
        print(line)
        with runner_log.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    sample = outdir / "sample.bin"
    result_path = outdir / "result.json"
    try:
        log(f"run={run_name}")
        make_sample(sample, args.file_size)
        sample_sha = sha256_file(sample)
        total_blocks = (args.file_size + args.block_size - 1) // args.block_size
        log(f"sample={sample} sha256={sample_sha} total_blocks={total_blocks}")

        processes = []
        process_meta = []

        seeder_port = find_free_port(args.base_port)
        seeder_stdout = outdir / "seeder.out.log"
        seeder_stderr = outdir / "seeder.err.log"
        seeder_args = [
            "run_peer.py",
            "--mode", "server",
            "--host", "127.0.0.1",
            "--port", str(seeder_port),
            "--file", str(sample),
            "--block-size", str(args.block_size),
        ]
        seeder, seeder_out_handle, seeder_err_handle = launch(seeder_args, ROOT, seeder_stdout, seeder_stderr)
        processes.append((seeder, seeder_out_handle, seeder_err_handle))
        process_meta.append({
            "role": "seeder",
            "port": seeder_port,
            "pid": seeder.pid,
            "args": " ".join([sys.executable, *seeder_args]),
            "stdout": str(seeder_stdout),
            "stderr": str(seeder_stderr),
        })
        log(f"seed pid={seeder.pid} port={seeder_port}")

        time.sleep(0.8)

        next_port = seeder_port + 1
        for i in range(1, args.peers):
            port = find_free_port(next_port)
            next_port = port + 1
            out_file = outdir / f"out_{i}.bin"
            stdout_path = outdir / f"peer-{port}.out.log"
            stderr_path = outdir / f"peer-{port}.err.log"
            peer_args = [
                "run_peer.py",
                "--mode",
                "peer",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--neighbors",
                f"127.0.0.1:{seeder_port}",
                "--out",
                str(out_file),
                "--block-size",
                str(args.block_size),
                "--total-blocks",
                str(total_blocks),
            ]
            peer, peer_out_handle, peer_err_handle = launch(peer_args, ROOT, stdout_path, stderr_path)
            processes.append((peer, peer_out_handle, peer_err_handle))
            process_meta.append({
                "role": "peer",
                "port": port,
                "pid": peer.pid,
                "args": " ".join([sys.executable, *peer_args]),
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
                "out": str(out_file),
            })
            log(f"peer pid={peer.pid} port={port}")
            time.sleep(0.3)

        deadline = time.time() + args.timeout
        details = []
        for i in range(1, args.peers):
            out_file = outdir / f"out_{i}.bin"
            while time.time() < deadline and not out_file.exists():
                time.sleep(0.2)

            exists = out_file.exists()
            out_sha = None
            out_size = None
            checksum_ok = False
            size_ok = False
            if exists:
                out_sha = sha256_file(out_file)
                out_size = out_file.stat().st_size
                checksum_ok = out_sha == sample_sha
                size_ok = out_size == args.file_size

            details.append({
                "out": str(out_file),
                "exists": exists,
                "checksum_ok": checksum_ok,
                "size_ok": size_ok,
                "size": out_size,
                "sha256": out_sha,
            })
            log(f"checked {out_file.name} exists={exists} checksum_ok={checksum_ok} size_ok={size_ok}")

        success = all(d["checksum_ok"] and d["size_ok"] for d in details)
        result = {
            "name": run_name,
            "timestamp": ts,
            "peers": args.peers,
            "block_size": args.block_size,
            "file_size": args.file_size,
            "total_blocks": total_blocks,
            "sample": {
                "path": str(sample),
                "sha256": sample_sha,
                "size": args.file_size,
            },
            "processes": process_meta,
            "details": details,
            "success": success,
            "runner_log": str(runner_log),
        }
        result_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        log(f"wrote result={result_path}")
        return 0 if success else 1

    except Exception as exc:
        tb = traceback.format_exc()
        with runner_log.open("a", encoding="utf-8") as fh:
            fh.write(tb + "\n")
        failure = {
            "name": run_name,
            "timestamp": ts,
            "peers": args.peers,
            "block_size": args.block_size,
            "file_size": args.file_size,
            "error": repr(exc),
            "traceback": tb,
            "runner_log": str(runner_log),
        }
        result_path.write_text(json.dumps(failure, indent=2), encoding="utf-8")
        print(f"RESULT_JSON={result_path}")
        print(f"RUN_DIR={outdir}")
        print(f"SUCCESS=False")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
