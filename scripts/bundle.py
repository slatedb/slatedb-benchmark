#!/usr/bin/env python3
import argparse
import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path


PREPARATION = ["bulk-load", "full-compaction"]
WORKLOADS = [
    "idle",
    "point-read-uniform",
    "point-read-skewed",
    "point-read-missing",
    "read-heavy",
    "balanced",
    "update-heavy",
    "range-scan",
    "sustained-ingest",
    "transaction-contention",
]


def arguments():
    parser = argparse.ArgumentParser(description="Assemble a SlateDB benchmark result bundle")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--golden", required=True)
    parser.add_argument("--started-at", required=True)
    parser.add_argument("--max-parallel", required=True, type=int)
    return parser.parse_args()


def read_result(path, task, golden):
    with path.open(encoding="utf-8") as file:
        result = json.load(file)
    if result.get("status") != "ok":
        raise ValueError(f"{path} does not contain a successful result")
    if result.get("task") != task:
        raise ValueError(f"{path} contains task {result.get('task')!r}, expected {task!r}")
    if result.get("golden_id") != golden:
        raise ValueError(f"{path} belongs to another golden data set")
    return result


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main():
    args = arguments()
    if args.max_parallel < 1:
        raise ValueError("max parallel must be positive")

    preparation = {
        task: read_result(args.input / "preparation" / task / "result.json", task, args.golden)
        for task in PREPARATION
    }
    workloads = {
        task: read_result(args.input / "workload" / task / "result.json", task, args.golden)
        for task in WORKLOADS
    }

    full = preparation["full-compaction"]
    bulk = preparation["bulk-load"]
    if full.get("source_checkpoint") != bulk["checkpoint"]:
        raise ValueError("full compaction did not use the published bulk-load checkpoint")

    source = workloads[WORKLOADS[0]]["source"]
    version = source["slate_version"]
    slate_commit = source["slate_commit"]
    runner_commit = source["runner_commit"]
    scale = workloads[WORKLOADS[0]]["configuration"]["scale"]
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", version) or version in {".", ".."}:
        raise ValueError(f"invalid SlateDB version {version!r}")
    sessions = {result["session"] for result in workloads.values()}
    if len(sessions) != 1:
        raise ValueError("workload results belong to different sessions")
    for task, result in workloads.items():
        if result["source"]["slate_commit"] != slate_commit:
            raise ValueError(f"{task} used a different SlateDB commit")
        if result["source"]["runner_commit"] != runner_commit:
            raise ValueError(f"{task} used a different benchmark runner commit")
        if result["configuration"]["scale"] != scale:
            raise ValueError(f"{task} used a different scale")
        if task == "sustained-ingest":
            if result["initial_state"]["kind"] != "empty":
                raise ValueError("sustained-ingest did not start empty")
        else:
            initial = result["initial_state"]
            checkpoint = full["checkpoint"]
            if (
                initial["checkpoint_id"] != checkpoint["checkpoint_id"]
                or initial["manifest_id"] != checkpoint["manifest_id"]
                or initial["lsm_digest_sha256"] != checkpoint["lsm_digest_sha256"]
            ):
                raise ValueError(f"{task} did not start from the golden checkpoint")
    for task, result in preparation.items():
        if result["source"]["slate_commit"] != slate_commit:
            raise ValueError(f"{task} used a different SlateDB commit")
        if result["configuration"]["scale"] != scale:
            raise ValueError(f"{task} used a different scale")

    destination = args.output / version
    if destination.exists():
        shutil.rmtree(destination)
    checksums = {}
    configurations = {}
    for kind, results in (("preparation", preparation), ("workload", workloads)):
        for task, result in results.items():
            relative = Path(kind) / task / "result.json"
            target = destination / relative
            write_json(target, result)
            checksums[relative.as_posix()] = sha256(target)
            configurations[task] = result["configuration"]

    manifest = {
        "status": "ok",
        "golden_id": args.golden,
        "started_at": args.started_at,
        "finished_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": source,
        "preparation_runner_commits": {
            task: preparation[task]["source"]["runner_commit"] for task in PREPARATION
        },
        "resolved_configuration": configurations,
        "max_parallel": args.max_parallel,
        "results": checksums,
    }
    write_json(destination / "run.json", manifest)
    print(destination)


if __name__ == "__main__":
    main()
