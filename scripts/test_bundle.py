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
        self.result = self.warp_result()

    def warp_result(self):
        def benchmark(name, operation, size, concurrency, duration):
            return {
                "name": name,
                "operation": operation,
                "object_size_bytes": size,
                "concurrency": concurrency,
                "duration_seconds": duration,
                "benchdata": f"warp/{name}.csv.zst",
            }

        return {
            "version": 2,
            "status": "ok",
            "scale": 1.0,
            **self.environment,
            "tool": {"name": "warp", "version": "v1.5.0"},
            "benchmarks": [
                benchmark("large-put", "PUT", 4 * 1024 * 1024, 64, 60),
                benchmark("large-get", "GET", 4 * 1024 * 1024, 64, 60),
                benchmark("small-put", "PUT", 4 * 1024, 1, 30),
                benchmark("small-get", "GET", 4 * 1024, 1, 30),
                benchmark("small-list", "LIST", 4 * 1024, 1, 30),
            ],
        }

    def legacy_result(self):
        return {
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

    def test_accepts_legacy_transfer_capacity(self):
        self.result = self.legacy_result()

        self.assertEqual(self.read(), self.result)

    def test_rejects_incorrect_warp_operation(self):
        self.result["benchmarks"][1]["operation"] = "PUT"

        with self.assertRaisesRegex(ValueError, "invalid large-get operation"):
            self.read()

    def test_rejects_incorrect_warp_benchdata(self):
        self.result["benchmarks"][4]["benchdata"] = "other.csv.zst"

        with self.assertRaisesRegex(ValueError, "invalid small-list benchmark data"):
            self.read()


if __name__ == "__main__":
    unittest.main()
