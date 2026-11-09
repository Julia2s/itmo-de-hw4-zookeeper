import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from kazoo.exceptions import BadVersionError, NodeExistsError

from publisher import claim_fencing_epoch, publish_fenced


class FakeZooKeeper:
    def __init__(self):
        self.nodes = {"/run/result": (b'{"status":"pending"}', 0)}

    def ensure_path(self, path):
        pass

    def create(self, path, data):
        if path in self.nodes:
            raise NodeExistsError()
        self.nodes[path] = (data, 0)

    def get(self, path):
        data, version = self.nodes[path]
        return data, SimpleNamespace(version=version)

    def set(self, path, data, version=-1):
        _, current_version = self.nodes[path]
        if version != -1 and version != current_version:
            raise BadVersionError()
        self.nodes[path] = (data, current_version + 1)
        return SimpleNamespace(version=current_version + 1)

    def transaction(self):
        return FakeTransaction(self)


class FakeTransaction:
    def __init__(self, client):
        self.client = client

    def check(self, path, version):
        self.fence_path, self.fence_version = path, version

    def set_data(self, path, data):
        self.result_path, self.result_data = path, data

    def commit(self):
        if self.client.get(self.fence_path)[1].version != self.fence_version:
            return [BadVersionError(), RuntimeError("transaction rolled back")]
        return [True, self.client.set(self.result_path, self.result_data)]


class FencingTests(unittest.TestCase):
    def test_new_epoch_rejects_old_writer_even_before_new_result(self):
        client = FakeZooKeeper()
        old_token = claim_fencing_epoch(client, "/run/fence", 1)
        new_token = claim_fencing_epoch(client, "/run/fence", 2)

        with self.assertRaises(BadVersionError):
            publish_fenced(client, "/run/result", "coordinator-1", 1, "old", {},
                           "/run/fence", old_token)
        self.assertEqual(client.get("/run/result")[1].version, 0)

        version = publish_fenced(client, "/run/result", "coordinator-2", 2, "new", {},
                                 "/run/fence", new_token)
        self.assertEqual(version, 1)
        result = json.loads(client.get("/run/result")[0])
        self.assertEqual((result["coordinator_id"], result["epoch"]), ("coordinator-2", 2))

    def test_epoch_cannot_move_backwards(self):
        client = FakeZooKeeper()
        claim_fencing_epoch(client, "/run/fence", 2)
        with self.assertRaises(ValueError):
            claim_fencing_epoch(client, "/run/fence", 1)


if __name__ == "__main__":
    unittest.main()
