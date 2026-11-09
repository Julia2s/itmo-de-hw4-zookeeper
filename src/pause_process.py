from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def wait_for_backup(log_path: Path, minimum_seconds: float, timeout_seconds: float) -> None:
    started = time.monotonic()
    while True:
        elapsed = time.monotonic() - started
        if (elapsed >= minimum_seconds and log_path.exists()
                and "run complete at " in log_path.read_text(encoding="utf-8")):
            print(f"backup_completed_observed_at={timestamp()}", flush=True)
            return
        if elapsed >= timeout_seconds:
            raise TimeoutError(f"Backup did not finish within {timeout_seconds:g} seconds")
        time.sleep(0.2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Suspend and resume one coordinator process.")
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--seconds", type=float, default=40)
    parser.add_argument("--until-log", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=300)
    args = parser.parse_args()
    if args.seconds < 0 or (args.until_log and args.timeout_seconds < args.seconds):
        parser.error("seconds must be nonnegative and no greater than timeout-seconds")

    process = psutil.Process(args.pid)
    process.suspend()
    print(f"suspended_at={timestamp()} pid={args.pid}", flush=True)
    try:
        if args.until_log:
            wait_for_backup(args.until_log, args.seconds, args.timeout_seconds)
        else:
            time.sleep(args.seconds)
    finally:
        process.resume()
        print(f"resumed_at={timestamp()} pid={args.pid}", flush=True)


if __name__ == "__main__":
    main()

