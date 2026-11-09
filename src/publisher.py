from __future__ import annotations

import json
from datetime import datetime, timezone

from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError


def ensure_result_pointer(client, path: str) -> None:
    """Create a versioned pointer before workers begin publishing."""
    client.ensure_path(path.rsplit("/", 1)[0])
    try:
        client.create(path, b'{"status":"pending"}')
    except NodeExistsError:
        pass


def _result_data(coordinator_id: str, epoch: int, result_prefix: str, summary: dict) -> bytes:
    payload = {
        "status": "published",
        "coordinator_id": coordinator_id,
        "epoch": epoch,
        "result_prefix": result_prefix,
        "summary": summary,
        "published_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def publish_unfenced(client, path: str, coordinator_id: str, epoch: int,
                     result_prefix: str, summary: dict) -> int:
    """Publish without checking the leader epoch; retained for the HW4 demo."""
    data = _result_data(coordinator_id, epoch, result_prefix, summary)
    try:
        stat = client.set(path, data, version=-1)
    except NoNodeError:
        ensure_result_pointer(client, path)
        stat = client.set(path, data, version=-1)
    return stat.version


def claim_fencing_epoch(client, fence_path: str, epoch: int) -> int:
    """Register a newer leader epoch and return its ZooKeeper version."""
    client.ensure_path(fence_path.rsplit("/", 1)[0])
    try:
        client.create(fence_path, b"0")
    except NodeExistsError:
        pass

    while True:
        data, stat = client.get(fence_path)
        if epoch <= int(data.decode("ascii")):
            raise ValueError(f"Epoch {epoch} is not newer than the registered epoch")
        try:
            return client.set(fence_path, str(epoch).encode("ascii"), version=stat.version).version
        except BadVersionError:
            continue


def publish_fenced(client, path: str, coordinator_id: str, epoch: int,
                   result_prefix: str, summary: dict, fence_path: str,
                   expected_fence_version: int) -> int:
    """Atomically verify the registered epoch token and publish the pointer."""
    transaction = client.transaction()
    transaction.check(fence_path, version=expected_fence_version)
    transaction.set_data(path, _result_data(coordinator_id, epoch, result_prefix, summary))
    results = transaction.commit()
    for result in results:
        if isinstance(result, BadVersionError):
            raise result
    for result in results:
        if isinstance(result, Exception):
            raise result
    return results[1].version

