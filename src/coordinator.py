from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, KazooException, NodeExistsError

from checkpoint import CheckpointStore
from common import VACANCIES_KEY, get_s3_client, write_frequency_results
from mapreduce_v2 import STACK_SIZES, decode_lines, iter_byte_chunks, map_chunk
from publisher import claim_fencing_epoch, ensure_result_pointer, publish_fenced, publish_unfenced

SESSION_TIMEOUT_SECONDS = 10
ZK_HOSTS = os.environ.get("ZK_HOSTS", "localhost:2181,localhost:2182,localhost:2183")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def safe_name(value: str, variable: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ValueError(f"{variable} may contain only letters, numbers, dot, underscore and dash")
    return value


def make_logger(run_id: str, coordinator_id: str) -> logging.Logger:
    log_dir = Path("logs") / "hw4" / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"{run_id}.{coordinator_id}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_dir / f"{coordinator_id}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


class CoordinatorElection:
    def __init__(self, client: KazooClient, path: str, identifier: str, logger: logging.Logger):
        self.client = client
        self.path = path
        self.identifier = identifier
        self.logger = logger
        self.candidate_path: str | None = None

    def join(self) -> int:
        self.client.ensure_path(self.path)
        self.candidate_path = self.client.create(
            f"{self.path}/candidate-",
            self.identifier.encode("utf-8"),
            ephemeral=True,
            sequence=True,
            makepath=True,
        )
        candidate_name = self.candidate_path.rsplit("/", 1)[-1]
        sequence = int(candidate_name.rsplit("-", 1)[-1])
        self.logger.info("joined election as %s (epoch=%s)", candidate_name, sequence + 1)

        while True:
            candidates = sorted(
                name for name in self.client.get_children(self.path)
                if name.startswith("candidate-")
            )
            if candidate_name not in candidates:
                raise RuntimeError("election session expired before leadership was acquired")
            position = candidates.index(candidate_name)
            if position == 0:
                return sequence + 1

            predecessor = f"{self.path}/{candidates[position - 1]}"
            changed = Event()
            if self.client.exists(predecessor, watch=lambda _event: changed.set()) is None:
                continue
            self.logger.info("waiting for predecessor %s", candidates[position - 1])
            changed.wait()

    def close(self) -> None:
        if self.candidate_path:
            try:
                self.client.delete(self.candidate_path)
            except KazooException:
                pass


def aggregate_checkpoints(store: CheckpointStore, indices: set[int]) -> tuple[dict[int, Counter], dict]:
    counters: dict[int, Counter] = {k: Counter() for k in STACK_SIZES}
    totals = {
        "lines_total": 0,
        "lines_prefiltered": 0,
        "lines_passed_prefilter": 0,
        "lines_with_skills": 0,
        "lines_malformed": 0,
        "events_per_k": {str(k): 0 for k in STACK_SIZES},
    }
    for index in sorted(indices):
        chunk_counters, stats = store.load(index)
        for k in STACK_SIZES:
            counters[k].update(chunk_counters[k])
            totals["events_per_k"][str(k)] += stats["events_per_k"][str(k)]
        for field in ("lines_total", "lines_prefiltered", "lines_passed_prefilter",
                      "lines_with_skills", "lines_malformed"):
            totals[field] += stats[field]
    return counters, totals


def run_coordinator(run_id: str, coordinator_id: str) -> None:
    run_id = safe_name(run_id, "RUN_ID")
    coordinator_id = safe_name(coordinator_id, "COORDINATOR_ID")
    fencing_enabled = os.environ.get("FENCING", "0") == "1"
    logger = make_logger(run_id, coordinator_id)
    logger.info("starting coordinator=%s pid=%d session_timeout_seconds=%d fencing_enabled=%s",
                coordinator_id, os.getpid(), SESSION_TIMEOUT_SECONDS, fencing_enabled)
    election_root = f"/hw4/runs/{run_id}/election"
    result_path = f"/hw4/runs/{run_id}/result"
    fence_path = f"/hw4/runs/{run_id}/fence"
    complete_path = f"/hw4/runs/{run_id}/complete"
    checkpoint_prefix = f"checkpoints/hw4/{run_id}"
    result_prefix = f"results/hw4/{run_id}/{coordinator_id}"

    zk = KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT_SECONDS)
    zk.start(timeout=SESSION_TIMEOUT_SECONDS)
    ensure_result_pointer(zk, result_path)
    election = CoordinatorElection(zk, election_root, coordinator_id, logger)
    try:
        epoch = election.join()
        logger.info("became leader epoch=%d at %s", epoch, utc_now())
        fencing_version = None
        if fencing_enabled:
            fencing_version = claim_fencing_epoch(zk, fence_path, epoch)
            logger.info("captured fencing token epoch=%d fence_version=%d",
                        epoch, fencing_version)
        if zk.exists(complete_path) is not None:
            logger.info("run is already complete; exiting without recomputation")
            return

        s3 = get_s3_client()
        info = s3.head_object(VACANCIES_KEY)
        if info is None:
            raise FileNotFoundError(f"S3 object {VACANCIES_KEY!r} is missing")
        store = CheckpointStore(s3, checkpoint_prefix)
        existing = store.saved_indices()
        logger.info("input=%s size_bytes=%d restored_checkpoints=%d",
                    VACANCIES_KEY, info.size, len(existing))

        for index, (chunk_data, _is_last) in enumerate(iter_byte_chunks(s3, VACANCIES_KEY, info.size)):
            if index in existing:
                logger.info("checkpoint restored chunk=%d; skipping map", index)
                continue
            counters, stats = map_chunk(decode_lines(chunk_data))
            store.save(index, counters, stats)
            existing.add(index)
            logger.info("checkpoint saved chunk=%d lines=%d", index, stats["lines_total"])

        counters, totals = aggregate_checkpoints(store, existing)
        local_dir = Path("data") / "results_hw4" / run_id / coordinator_id
        written = write_frequency_results(s3, counters, result_prefix, str(local_dir))
        summary = {
            "input_key": VACANCIES_KEY,
            "chunks": len(existing),
            "lines_total": totals["lines_total"],
            "rows_matched": totals["lines_passed_prefilter"],
            "rows_with_skills": totals["lines_with_skills"],
            "lines_malformed": totals["lines_malformed"],
            "events_per_k": totals["events_per_k"],
            "n_skills_1": written[1]["n_combinations"],
            "n_skills_2": written[2]["n_combinations"],
            "n_skills_3": written[3]["n_combinations"],
            "total_frequency": {str(k): written[k]["total_frequency"] for k in STACK_SIZES},
            "files": {str(k): written[k]["s3_key"] for k in STACK_SIZES},
        }
        summary_path = local_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        s3.put_object(
            f"{result_prefix}/summary.json",
            summary_path.read_bytes(),
            content_type="application/json; charset=utf-8",
        )
        try:
            if fencing_enabled:
                if fencing_version is None:
                    raise RuntimeError("Fencing version was not captured after leadership acquisition")
                pointer_version = publish_fenced(
                    zk, result_path, coordinator_id, epoch, result_prefix, summary,
                    fence_path=fence_path, expected_fence_version=fencing_version,
                )
            else:
                pointer_version = publish_unfenced(
                    zk, result_path, coordinator_id, epoch, result_prefix, summary
                )
        except BadVersionError:
            logger.error(
                "result write rejected coordinator=%s epoch=%d error=BadVersionError expected_fence_version=%s",
                coordinator_id, epoch, fencing_version,
            )
            return
        logger.info("wrote result coordinator=%s epoch=%d pointer_version=%d",
                    coordinator_id, epoch, pointer_version)
        try:
            zk.create(complete_path, json.dumps(summary).encode("utf-8"))
        except NodeExistsError:
            pass
        logger.info("run complete at %s", utc_now())
    finally:
        election.close()
        zk.stop()
        zk.close()


if __name__ == "__main__":
    run_id = os.environ.get("RUN_ID", "unfenced-demo")
    coordinator_id = os.environ.get("COORDINATOR_ID", "coordinator-1")
    run_coordinator(run_id, coordinator_id)

