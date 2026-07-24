#!/usr/bin/env python3
import argparse
import hashlib
import json
import math
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path


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
PATCH_DIRECTORY = Path(__file__).resolve().parent.parent / "patches" / "slatedb"


def arguments():
    parser = argparse.ArgumentParser(description="Assemble a SlateDB benchmark result bundle")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--golden", required=True)
    parser.add_argument("--started-at", required=True)
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
    if result.get("recorded_interval_ns", 0) <= 0:
        raise ValueError(f"{path} has no recorded metrics")
    for field in ("environment", "application", "object_store", "process", "machine"):
        if not isinstance(result.get(field), dict):
            raise ValueError(f"{path} has no {field} metrics")
    return result


def read_golden(path, golden):
    with path.open(encoding="utf-8") as file:
        manifest = json.load(file)
    if manifest.get("status") != "ok":
        raise ValueError(f"{path} does not contain a successful golden manifest")
    if manifest.get("golden_id") != golden:
        raise ValueError(f"{path} belongs to another golden data set")
    if manifest.get("configuration", {}).get("task", {}).get("task") != "compaction":
        raise ValueError(f"{path} is not the final compacted golden manifest")
    for field in ("source", "environment", "checkpoint", "dataset"):
        if not isinstance(manifest.get(field), dict):
            raise ValueError(f"{path} has no {field}")
    return manifest


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_patches(directory=PATCH_DIRECTORY):
    return [
        {"name": path.name, "sha256": sha256(path)}
        for path in sorted(directory.glob("*.patch"))
        if path.is_file()
    ]


def read_series(path, result):
    if result.get("series", {}).get("file") != "series.json":
        raise ValueError(f"{path.parent / 'result.json'} has an invalid series reference")
    with path.open(encoding="utf-8") as file:
        series = json.load(file)
    required = {
        "rate_elapsed_ns",
        "rate_duration_ns",
        "latency_elapsed_ns",
        "latency_duration_ns",
        "resource_elapsed_ns",
        "resource_duration_ns",
        "application",
        "object_store",
        "process",
        "machine",
    }
    if set(series) != required:
        raise ValueError(f"{path} does not contain the expected workload series")
    if sha256(path) != result["series"].get("sha256"):
        raise ValueError(f"{path} does not match its result digest")
    return path


def read_transfer_capacity(path, scale, environment):
    with path.open(encoding="utf-8") as file:
        result = json.load(file)
    if result.get("status") != "ok":
        raise ValueError(f"{path} does not contain a successful transfer probe")
    if result.get("scale") != scale:
        raise ValueError(f"{path} used a different scale")
    for field in ("runner_type", "object_store", "endpoint", "region"):
        if result.get(field) != environment[field]:
            raise ValueError(f"{path} used a different {field.replace('_', ' ')}")
    validate_warp_transfer_capacity(path, result)
    return result


def validate_warp_transfer_capacity(path, result):
    if result.get("version") != 3:
        raise ValueError(f"{path} has unsupported transfer probe version")
    tool = result.get("tool")
    if (
        not isinstance(tool, dict)
        or tool.get("name") != "warp"
        or not isinstance(tool.get("version"), str)
        or not tool["version"]
    ):
        raise ValueError(f"{path} has invalid Warp tool metadata")
    benchmarks = result.get("benchmarks")
    if (
        not isinstance(benchmarks, list)
        or len(benchmarks) != 5
        or not all(isinstance(benchmark, dict) for benchmark in benchmarks)
    ):
        raise ValueError(f"{path} has invalid Warp benchmarks")
    expected = {
        "large-put": "PUT",
        "large-get": "GET",
        "small-put": "PUT",
        "small-get": "GET",
        "small-list": "LIST",
    }
    if {benchmark.get("name") for benchmark in benchmarks} != set(expected):
        raise ValueError(f"{path} has unexpected Warp benchmarks")
    for benchmark in benchmarks:
        name = benchmark["name"]
        if benchmark.get("operation") != expected[name]:
            raise ValueError(f"{path} has invalid {name} operation")
        for field in ("object_size_bytes", "concurrency", "duration_seconds"):
            value = benchmark.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{path} has invalid {name} {field.replace('_', ' ')}")
        latency = benchmark.get("latency_ms")
        if latency is None:
            raise ValueError(f"{path} has no {name} latency")
        validate_warp_latency(path, name, latency)
        if benchmark.get("benchdata") != f"warp/{name}.csv.zst":
            raise ValueError(f"{path} has invalid {name} benchmark data")


def validate_warp_latency(path, name, latency):
    if not isinstance(latency, dict) or set(latency) not in (
        {"request"},
        {"request", "ttfb"},
    ):
        raise ValueError(f"{path} has invalid {name} latency")
    fields = {"average", "p50", "p90", "p99", "min", "max"}
    for kind, summary in latency.items():
        if not isinstance(summary, dict) or set(summary) != fields:
            raise ValueError(f"{path} has invalid {name} {kind} latency")
        for field, value in summary.items():
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(f"{path} has invalid {name} {kind} {field} latency")
        if not (
            summary["min"]
            <= summary["p50"]
            <= summary["p90"]
            <= summary["p99"]
            <= summary["max"]
        ):
            raise ValueError(f"{path} has unordered {name} {kind} latency")


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def discover_workloads(directory):
    if not directory.is_dir():
        raise ValueError(f"{directory} does not contain workload results")
    discovered = {
        child.name
        for child in directory.iterdir()
        if child.is_dir() and (child / "result.json").is_file()
    }
    unknown = discovered.difference(WORKLOADS)
    if unknown:
        raise ValueError(f"{directory} contains unknown workloads: {', '.join(sorted(unknown))}")
    tasks = [task for task in WORKLOADS if task in discovered]
    if not tasks:
        raise ValueError(f"{directory} does not contain any workload results")
    return tasks


def validate_source_identities(workloads):
    if not workloads:
        raise ValueError("no workload results were provided")
    source = next(iter(workloads.values()))["source"]
    slate_commit = source["slate_commit"]
    runner_commit = source["runner_commit"]
    for task, result in workloads.items():
        if result["source"]["slate_commit"] != slate_commit:
            raise ValueError(f"{task} used a different SlateDB commit")
        if result["source"]["runner_commit"] != runner_commit:
            raise ValueError(f"{task} used a different benchmark runner commit")

    return source


def main():
    args = arguments()

    golden = read_golden(args.input / "golden.json", args.golden)
    workload_tasks = discover_workloads(args.input / "workload")
    workloads = {
        task: read_result(args.input / "workload" / task / "result.json", task, args.golden)
        for task in workload_tasks
    }
    workload_series = {
        task: read_series(args.input / "workload" / task / "series.json", workloads[task])
        for task in workload_tasks
    }

    source = validate_source_identities(workloads)
    version = source["slate_version"]
    first_workload = next(iter(workloads.values()))
    scale = first_workload["configuration"]["scale"]
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", version) or version in {".", ".."}:
        raise ValueError(f"invalid SlateDB version {version!r}")
    sessions = {result["session"] for result in workloads.values()}
    if len(sessions) != 1:
        raise ValueError("workload results belong to different sessions")
    run_id = next(iter(sessions))
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", run_id) or run_id in {".", ".."}:
        raise ValueError(f"invalid benchmark run ID {run_id!r}")
    for task, result in workloads.items():
        if result["configuration"]["scale"] != scale:
            raise ValueError(f"{task} used a different scale")
        if task == "sustained-ingest":
            if result["initial_state"]["kind"] != "empty":
                raise ValueError("sustained-ingest did not start empty")
        else:
            initial = result["initial_state"]
            checkpoint = golden["checkpoint"]
            if (
                initial["checkpoint_id"] != checkpoint["checkpoint_id"]
                or initial["manifest_id"] != checkpoint["manifest_id"]
                or initial["lsm_digest_sha256"] != checkpoint["lsm_digest_sha256"]
            ):
                raise ValueError(f"{task} did not start from the golden checkpoint")
    if golden["configuration"]["scale"] != scale:
        raise ValueError("golden manifest used a different scale")

    transfer_capacity_path = args.input / "transfer-capacity" / "result.json"
    transfer_capacity = (
        read_transfer_capacity(
            transfer_capacity_path,
            scale,
            first_workload["environment"],
        )
        if transfer_capacity_path.is_file()
        else None
    )

    destination = args.output / version / run_id
    if destination.exists():
        shutil.rmtree(destination)
    golden_target = destination / "golden.json"
    write_json(golden_target, golden)
    checksums = {"golden.json": sha256(golden_target)}
    configurations = {"golden": golden["configuration"]}
    for task, result in workloads.items():
        relative = Path("workload") / task / "result.json"
        target = destination / relative
        write_json(target, result)
        checksums[relative.as_posix()] = sha256(target)
        configurations[task] = result["configuration"]
        series_relative = Path("workload") / task / "series.json"
        series_target = destination / series_relative
        series_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(workload_series[task], series_target)
        checksums[series_relative.as_posix()] = sha256(series_target)

    manifest = {
        "status": "ok",
        "run_id": run_id,
        "golden_id": args.golden,
        "started_at": args.started_at,
        "finished_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "patches": read_patches(),
        "source": source,
        "golden_runner_commit": golden["source"]["runner_commit"],
        "resolved_configuration": configurations,
        "max_parallel": len(workloads),
        "results": checksums,
    }
    if transfer_capacity is not None:
        manifest["transfer_capacity"] = transfer_capacity
    write_json(destination / "run.json", manifest)
    print(destination)


if __name__ == "__main__":
    main()
