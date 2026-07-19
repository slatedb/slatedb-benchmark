import unittest

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


if __name__ == "__main__":
    unittest.main()
