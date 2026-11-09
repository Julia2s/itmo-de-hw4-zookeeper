from __future__ import annotations

import io
import math
import os
import sys
import time
from collections import Counter
from itertools import combinations
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (
    get_s3_client, VACANCIES_KEY, parse_skills, is_devops, parse_vacancy_line,
    VARIANT_NAME_KEYWORDS, VARIANT_ROLE, VARIANT_FILTER_MODE,
    build_manifest, write_manifest, write_frequency_results,
)

STACK_SIZES = (1, 2, 3)
CHUNK_SIZE_BYTES = 8 * 1024 * 1024
OUTPUT_PREFIX = "results/mapreduce_v2"
LOCAL_OUT_DIR = "data/results_v2"
MANIFEST_DIR = "data/manifests"

if VARIANT_FILTER_MODE == "role":
    _PREFILTER = (VARIANT_ROLE.lower(),)
elif VARIANT_FILTER_MODE == "name":
    _PREFILTER = tuple(kw.lower() for kw in VARIANT_NAME_KEYWORDS)
else:
    _PREFILTER = (VARIANT_ROLE.lower(),) + tuple(kw.lower() for kw in VARIANT_NAME_KEYWORDS)

def decode_lines(data: bytes) -> list[str]:
    """Split one chunk of complete lines into strings (no trailing blank)."""
    if not data:
        return []
    if data.endswith(b"\n"):
        data = data[:-1]
    return data.decode("utf-8", errors="replace").split("\n")

def iter_byte_chunks(cli, key: str, file_size: int, chunk_bytes: int = CHUNK_SIZE_BYTES):
    """Yield (chunk_bytes, is_last) aligned to newline boundaries.

    Ranges are read sequentially with bounded memory. A line longer than
    `chunk_bytes` is accumulated into `pending` and emitted whole, so no
    line is ever processed twice and none is cut in the middle.
    """
    pending = b""
    offset = 0
    while offset < file_size:
        end = min(offset + chunk_bytes, file_size) - 1
        data = cli.get_object(key, start=offset, end=end)
        offset = end + 1
        is_last = offset >= file_size
        buf = pending + data
        pending = b""
        if is_last:
            yield buf, True
            continue
        nl = buf.rfind(b"\n")
        if nl < 0:
            pending = buf
            continue
        yield buf[: nl + 1], False
        pending = buf[nl + 1 :]
    if pending:
        yield pending, True

def map_chunk(chunk_lines: list[str]) -> tuple[dict[int, Counter], dict]:
    counters: dict[int, Counter] = {k: Counter() for k in STACK_SIZES}
    n_total = 0
    n_prefiltered = 0
    n_passed = 0
    n_with_skills = 0
    events: dict[int, int] = {k: 0 for k in STACK_SIZES}
    n_malformed = 0

    for line in chunk_lines:
        n_total += 1
        line_lc = line.lower()
        if not any(kw in line_lc for kw in _PREFILTER):
            continue
        n_prefiltered += 1
        row, reason = parse_vacancy_line(line)
        if row is None:
            if reason == "malformed":
                n_malformed += 1
            continue
        if not is_devops(row):
            continue
        n_passed += 1
        skills = parse_skills(row.get("key_skills", ""))
        if not skills:
            continue
        n_with_skills += 1
        sorted_skills = sorted(skills)
        for k in STACK_SIZES:
            counters[k].update(combinations(sorted_skills, k))
            events[k] += math.comb(len(sorted_skills), k)
    return counters, {
        "lines_total": n_total,
        "lines_prefiltered": n_prefiltered,
        "lines_passed_prefilter": n_passed,
        "lines_with_skills": n_with_skills,
        "lines_malformed": n_malformed,
        "events_per_k": events,
    }

def run() -> dict:
    cli = get_s3_client()
    t0 = time.perf_counter()

    info = cli.head_object(VACANCIES_KEY)
    if info is None:
        raise FileNotFoundError(f"S3-объект {VACANCIES_KEY} не найден")
    file_size = info.size
    t1 = time.perf_counter()

    final_counters: dict[int, Counter] = {k: Counter() for k in STACK_SIZES}
    stats_accum = {
        "lines_total": 0, "lines_prefiltered": 0,
        "lines_passed_prefilter": 0, "lines_with_skills": 0,
        "lines_malformed": 0,
        "events_per_k": {k: 0 for k in STACK_SIZES},
        "n_chunks": 0,
    }

    for chunk_data, _is_last in iter_byte_chunks(cli, VACANCIES_KEY, file_size):
        counters_chunk, chunk_stats = map_chunk(decode_lines(chunk_data))
        for k in STACK_SIZES:
            final_counters[k].update(counters_chunk[k])
        for field in ("lines_total", "lines_prefiltered", "lines_passed_prefilter",
                      "lines_with_skills", "lines_malformed"):
            stats_accum[field] += chunk_stats[field]
        for k in STACK_SIZES:
            stats_accum["events_per_k"][k] += chunk_stats["events_per_k"][k]
        stats_accum["n_chunks"] += 1

    t2 = time.perf_counter()

    written = write_frequency_results(cli, final_counters, OUTPUT_PREFIX, LOCAL_OUT_DIR)
    t3 = time.perf_counter()

    result = {
        "file_size_bytes": file_size,
        "filter_mode": VARIANT_FILTER_MODE,
        "chunk_size_bytes": CHUNK_SIZE_BYTES,
        "n_chunks": stats_accum["n_chunks"],
        "lines_total": stats_accum["lines_total"],
        "lines_prefiltered": stats_accum["lines_prefiltered"],
        "rows_matched": stats_accum["lines_passed_prefilter"],
        "rows_with_skills": stats_accum["lines_with_skills"],
        "lines_malformed": stats_accum["lines_malformed"],
        "events_per_k": stats_accum["events_per_k"],
        "n_skills_1": written[1]["n_combinations"],
        "n_skills_2": written[2]["n_combinations"],
        "n_skills_3": written[3]["n_combinations"],
        "total_frequency": {k: written[k]["total_frequency"] for k in STACK_SIZES},
        "phase_time_stat_s": round(t1 - t0, 3),
        "phase_time_map_s": round(t2 - t1, 3),
        "phase_time_reduce_write_s": round(t3 - t2, 3),
        "phase_time_total_s": round(t3 - t0, 3),
        "results": written,
    }

    manifest = build_manifest("mapreduce_v2", cli, {
        "counts": {
            "lines_total": result["lines_total"],
            "rows_matched": result["rows_matched"],
            "rows_with_skills": result["rows_with_skills"],
            "n_skills_1": result["n_skills_1"],
            "n_skills_2": result["n_skills_2"],
            "n_skills_3": result["n_skills_3"],
        },
        "chunk_size_bytes": CHUNK_SIZE_BYTES,
        "n_chunks": result["n_chunks"],
        "timings": {
            "stat_s": result["phase_time_stat_s"],
            "map_s": result["phase_time_map_s"],
            "reduce_write_s": result["phase_time_reduce_write_s"],
            "total_s": result["phase_time_total_s"],
        },
    })
    result["manifest"] = write_manifest(cli, "mapreduce_v2", manifest, MANIFEST_DIR)
    return result

if __name__ == "__main__":
    import json
    stats = run()
    print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
