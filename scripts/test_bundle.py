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
        self.workloads = {
            task: result("measured-commit", "benchmark-runner")
            for task in bundle.WORKLOADS
        }

    def test_accepts_a_single_workload(self):
        workloads = {"sustained-ingest": self.workloads["sustained-ingest"]}

        source = bundle.validate_source_identities(workloads)

        self.assertEqual(source["slate_commit"], "measured-commit")

    def test_rejects_no_workloads(self):
        with self.assertRaisesRegex(ValueError, "no workload results"):
            bundle.validate_source_identities({})

    def test_rejects_mixed_workload_commits(self):
        self.workloads["balanced"] = result("other-commit", "benchmark-runner")

        with self.assertRaisesRegex(ValueError, "different SlateDB commit"):
            bundle.validate_source_identities(self.workloads)

    def test_rejects_mixed_runner_commits(self):
        self.workloads["balanced"] = result("measured-commit", "other-runner")

        with self.assertRaisesRegex(ValueError, "different benchmark runner commit"):
            bundle.validate_source_identities(self.workloads)


class GoldenManifestTests(unittest.TestCase):
    def manifest(self):
        return {
            "status": "ok",
            "golden_id": "golden",
            "source": {},
            "environment": {},
            "configuration": {
                "task": {"task": "compaction"},
            },
            "checkpoint": {},
            "dataset": {},
        }

    def read(self, manifest):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "golden.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            return bundle.read_golden(path, "golden")

    def test_accepts_final_compacted_manifest(self):
        manifest = self.manifest()

        self.assertEqual(self.read(manifest), manifest)

    def test_rejects_intermediate_bulk_load_manifest(self):
        manifest = self.manifest()
        manifest["configuration"]["task"]["task"] = "bulk-load"

        with self.assertRaisesRegex(ValueError, "not the final compacted"):
            self.read(manifest)


class WorkloadDiscoveryTests(unittest.TestCase):
    def test_discovers_an_allowlisted_subset_in_canonical_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for task in ("sustained-ingest", "balanced"):
                task_directory = root / task
                task_directory.mkdir()
                (task_directory / "result.json").write_text("{}", encoding="utf-8")

            tasks = bundle.discover_workloads(root)

        self.assertEqual(tasks, ["balanced", "sustained-ingest"])

    def test_rejects_unknown_workloads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            task_directory = root / "typo"
            task_directory.mkdir()
            (task_directory / "result.json").write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unknown workloads: typo"):
                bundle.discover_workloads(root)

    def test_rejects_an_empty_workload_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "any workload results"):
                bundle.discover_workloads(Path(directory))


class PatchManifestTests(unittest.TestCase):
    def test_records_patch_names_and_digests_in_filename_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "0002-second.patch").write_text("second\n", encoding="utf-8")
            (root / "0001-first.patch").write_text("first\n", encoding="utf-8")
            (root / "README.md").write_text("ignored\n", encoding="utf-8")

            patches = bundle.read_patches(root)

        self.assertEqual(
            [patch["name"] for patch in patches],
            ["0001-first.patch", "0002-second.patch"],
        )
        self.assertTrue(all(len(patch["sha256"]) == 64 for patch in patches))

    def test_empty_patch_directory_produces_an_empty_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(bundle.read_patches(Path(directory)), [])


class TransferCapacityTests(unittest.TestCase):
    def setUp(self):
        self.environment = {
            "runner_type": "test-runner",
            "object_store": "s3",
            "endpoint": "AWS default",
            "region": "us-east-1",
        }
        self.result = self.warp_result()

    def warp_result(self):
        latency = {
            "request": {
                "average": 2.1,
                "p50": 2.0,
                "p90": 3.0,
                "p99": 4.0,
                "min": 1.0,
                "max": 5.0,
            },
            "ttfb": {
                "average": 1.1,
                "p50": 1.0,
                "p90": 2.0,
                "p99": 3.0,
                "min": 0.5,
                "max": 4.0,
            },
        }

        def benchmark(name, operation, size, concurrency, duration):
            return {
                "name": name,
                "operation": operation,
                "object_size_bytes": size,
                "concurrency": concurrency,
                "duration_seconds": duration,
                "latency_ms": latency,
                "benchdata": f"warp/{name}.csv.zst",
            }

        return {
            "version": 3,
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

    def read(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.json"
            path.write_text(json.dumps(self.result), encoding="utf-8")
            return bundle.read_transfer_capacity(path, 1.0, self.environment)

    def test_accepts_self_describing_probe_configuration(self):
        self.assertEqual(self.read(), self.result)

    def test_rejects_incorrect_warp_operation(self):
        self.result["benchmarks"][1]["operation"] = "PUT"

        with self.assertRaisesRegex(ValueError, "invalid large-get operation"):
            self.read()

    def test_rejects_incorrect_warp_benchdata(self):
        self.result["benchmarks"][4]["benchdata"] = "other.csv.zst"

        with self.assertRaisesRegex(ValueError, "invalid small-list benchmark data"):
            self.read()

    def test_rejects_missing_warp_latency(self):
        del self.result["benchmarks"][0]["latency_ms"]

        with self.assertRaisesRegex(ValueError, "no large-put latency"):
            self.read()

    def test_rejects_unordered_warp_latency(self):
        self.result["benchmarks"][0]["latency_ms"]["request"]["p99"] = 6.0

        with self.assertRaisesRegex(ValueError, "unordered large-put request latency"):
            self.read()


if __name__ == "__main__":
    unittest.main()
