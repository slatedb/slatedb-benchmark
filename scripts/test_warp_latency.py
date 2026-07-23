import json
import subprocess
import unittest
from pathlib import Path


FILTER = Path(__file__).with_name("warp-latency.jq")


def request_segment(average, p50, p90, p99, minimum, maximum, ttfb=None):
    requests = {
        "dur_avg_millis": average,
        "dur_median_millis": p50,
        "dur_90_millis": p90,
        "dur_99_millis": p99,
        "fastest_millis": minimum,
        "slowest_millis": maximum,
        "merged_entries": 1,
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
    return {"single_sized_requests": requests}


def summarize(*segments):
    aggregate = {
        "by_op_type": {
            "PUT": {
                "requests_by_client": {
                    "client": list(segments),
                }
            }
        }
    }
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
            request_segment(2, 2, 3, 4, 1, 5, (1, 1, 2, 3, 0.5, 4)),
            request_segment(4, 4, 5, 6, 2, 8, (3, 3, 4, 5, 1, 6)),
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
        summary = summarize(request_segment(2, 2, 3, 4, 1, 5))

        self.assertEqual(set(summary), {"request"})


if __name__ == "__main__":
    unittest.main()
