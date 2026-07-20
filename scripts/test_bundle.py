import json
import tempfile
import unittest
from pathlib import Path

from scripts import bundle


def result(slate_commit, runner_commit):
    return {
        "source": {
            "slate_commit": slate_commit,
            "runner_commit": runner_commit,
        }
    }


class SourceIdentityTests(unittest.TestCase):
    def setUp(self):
        self.preparation = {
            task: result("golden-commit", "preparation-runner")
            for task in bundle.PREPARATION
        }
        self.workloads = {
            task: result("measured-commit", "benchmark-runner")
            for task in bundle.WORKLOADS
        }

    def test_accepts_golden_data_from_another_slatedb_commit(self):
        source = bundle.validate_source_identities(self.preparation, self.workloads)

        self.assertEqual(source["slate_commit"], "measured-commit")

    def test_rejects_mixed_workload_commits(self):
        self.workloads["balanced"] = result("other-commit", "benchmark-runner")

        with self.assertRaisesRegex(ValueError, "different SlateDB commit"):
            bundle.validate_source_identities(self.preparation, self.workloads)

    def test_rejects_mixed_preparation_commits(self):
        self.preparation["compaction"] = result("other-commit", "preparation-runner")

        with self.assertRaisesRegex(ValueError, "golden-data SlateDB commit"):
            bundle.validate_source_identities(self.preparation, self.workloads)


class TransferCapacityTests(unittest.TestCase):
    def setUp(self):
        self.environment = {
            "runner_type": "test-runner",
            "object_store": "Tigris",
            "endpoint": "https://t3.storage.dev",
            "region": "fra",
        }
        self.result = {
            "status": "ok",
            "scale": 1.0,
            **self.environment,
            "parallel_objects": 2,
            "requests_per_process": 4,
            "max_concurrent_requests": 8,
            "warmup_bytes": 4 * 1024 * 1024 * 1024,
            "measured_bytes": 16 * 1024 * 1024 * 1024,
            "upload": {
                "elapsed_seconds": 16.0,
                "mib_per_second": 1024.0,
            },
            "download": {
                "elapsed_seconds": 32.0,
                "mib_per_second": 512.0,
            },
        }

    def read(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.json"
            path.write_text(json.dumps(self.result), encoding="utf-8")
            return bundle.read_transfer_capacity(path, 1.0, self.environment)

    def test_accepts_self_describing_probe_configuration(self):
        self.assertEqual(self.read(), self.result)

    def test_rejects_inconsistent_transfer_concurrency(self):
        self.result["max_concurrent_requests"] = 7

        with self.assertRaisesRegex(ValueError, "inconsistent transfer concurrency"):
            self.read()


if __name__ == "__main__":
    unittest.main()
