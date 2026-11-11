#!/usr/bin/env python3
"""Start, inspect, or stop the local 3-node town-power cluster."""
import argparse
import json
import os
import signal
import socket
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".local-run"
PID_FILE = STATE / "pids.json"
PORTS = (2551, 2552, 2553)
JAR = ROOT / "target" / "town-power-cluster.jar"


def active(pid: int) -> bool:
    try:
        return b"town-power-cluster.jar" in Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status"))
    args = parser.parse_args()
    pids = json.loads(PID_FILE.read_text()) if PID_FILE.exists() else {}

    if args.action == "stop":
        for port, pid in pids.items():
            if active(pid):
                os.kill(pid, signal.SIGTERM)
                print(f"Stopping node {port} (PID {pid})")
        PID_FILE.unlink(missing_ok=True)
        return

    if args.action == "status":
        for port, pid in pids.items():
            print(f"Node {port}: {'running' if active(pid) else 'stopped'} (PID {pid})")
        return

    if any(active(pid) for pid in pids.values()):
        raise SystemExit("Demo already running; use status or stop first.")
    if not JAR.exists():
        raise SystemExit("Build first: mvn -B -ntp package")

    for port in (*PORTS, 9551, 9552, 9553):
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                raise SystemExit(f"Port {port} already in use; no nodes started.")

    STATE.mkdir(exist_ok=True)
    env = os.environ.copy()
    env["HTTP_BIND_HOST"] = "127.0.0.1"
    pids = {}
    for port in PORTS:
        log = (STATE / f"node-{port}.log").open("a")
        process = subprocess.Popen(
            ["java", "-Xms64m", "-Xmx256m", "-XX:ActiveProcessorCount=2", "-jar", str(JAR), str(port)],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        pids[str(port)] = process.pid
        PID_FILE.write_text(json.dumps(pids))
        print(f"Started node {port} (PID {process.pid})")

    print("City map:     http://127.0.0.1:9551/city")
    print("Shard viewer: http://127.0.0.1:9551/viewer")


if __name__ == "__main__":
    main()
