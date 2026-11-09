from __future__ import annotations

import json
from collections import Counter


class CheckpointStore:
    """Persist one deterministic MapReduce result per input chunk in S3."""

    def __init__(self, client, prefix: str) -> None:
        self.client = client
        self.prefix = prefix.rstrip("/")

    def key(self, chunk_index: int) -> str:
        return f"{self.prefix}/chunks/{chunk_index:06d}.json"

    def saved_indices(self) -> set[int]:
        chunk_prefix = f"{self.prefix}/chunks/"
        indices = set()
        for item in self.client.list_objects(chunk_prefix):
            name = item.key.rsplit("/", 1)[-1]
            if name.endswith(".json") and name[:-5].isdigit():
                indices.add(int(name[:-5]))
        return indices

    def save(self, chunk_index: int, counters: dict[int, Counter], stats: dict) -> None:
        payload = {
            "chunk_index": chunk_index,
            "counters": {
                str(k): [[list(combo), count] for combo, count in counter.items()]
                for k, counter in counters.items()
            },
            "stats": stats,
        }
        self.client.put_object(
            self.key(chunk_index),
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            content_type="application/json; charset=utf-8",
        )

    def load(self, chunk_index: int) -> tuple[dict[int, Counter], dict]:
        payload = json.loads(self.client.get_object(self.key(chunk_index)).decode("utf-8"))
        counters = {
            int(k): Counter({tuple(combo): int(count) for combo, count in rows})
            for k, rows in payload["counters"].items()
        }
        return counters, payload["stats"]

