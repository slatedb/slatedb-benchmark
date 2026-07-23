import json
import subprocess
import unittest
from pathlib import Path


FILTER = Path(__file__).with_name("warp-latency.jq")


def full_analysis(average, p50, p90, p99, minimum, maximum, ttfb=None):
    requests = {
        "dur_avg_millis": average,
        "dur_median_millis": p50,
        "dur_90_millis": p90,
        "dur_99_millis": p99,
        "fastest_millis": minimum,
        "slowest_millis": maximum,
    }
    if ttfb is not None:
        requests["first_byte"] = {
            "average_millis": ttfb[0],
            "median_millis": ttfb[1],
            "p90_millis": ttfb[2],
            "p99_millis": ttfb[3],
            "fastest_millis": ttfb[4],
            "slowest_millis": ttfb[5],
        }
    return {"operations": [{"single_sized_requests": requests}]}


def summarize(aggregate):
    result = subprocess.run(
        ["jq", "-e", "-f", str(FILTER)],
        input=json.dumps(aggregate),
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


class WarpLatencyTests(unittest.TestCase):
    def test_summarizes_request_and_ttfb_latency(self):
        summary = summarize(
            full_analysis(3, 3, 4, 5, 1, 8, (2, 2, 3, 4, 0.5, 6))
        )

        self.assertEqual(
            summary,
            {
                "request": {
                    "average": 3,
                    "p50": 3,
                    "p90": 4,
                    "p99": 5,
                    "min": 1,
                    "max": 8,
                },
                "ttfb": {
                    "average": 2,
                    "p50": 2,
                    "p90": 3,
                    "p99": 4,
                    "min": 0.5,
                    "max": 6,
                },
            },
        )

    def test_omits_ttfb_when_warp_does_not_report_it(self):
        summary = summarize(full_analysis(2, 2, 3, 4, 1, 5))

        self.assertEqual(set(summary), {"request"})

    def test_transfer_probe_records_full_request_data(self):
        script = Path(__file__).with_name("transfer-capacity.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn('"$warp_bin" analyze --json --full --no-color', script)
        self.assertIn('--analyze.op="$operation" "$raw"', script)
        self.assertIn('local raw="$base.csv.zst"', script)
        self.assertIn("    --full\n", script)
        self.assertIn('configured_endpoint=${AWS_ENDPOINT_URL_S3:-}', script)
        self.assertIn('endpoint="AWS default"', script)
        self.assertNotIn("t3.storage.dev", script)


if __name__ == "__main__":
    unittest.main()
