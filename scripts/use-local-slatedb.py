#!/usr/bin/env python3

import json
import re
import subprocess
import sys
from pathlib import Path


DEPENDENCIES = {
    "slatedb": ("slatedb", ["aws", "wal_disable", "zstd"]),
    "slatedb-common": ("slatedb-common", ["serde"]),
}
RESOLVED_PACKAGES = {
    "slatedb": "slatedb",
    "slatedb-common": "slatedb-common",
    "slatedb-txn-obj": "slatedb-txn-obj",
}


def toml_string(value: str) -> str:
    return json.dumps(value)


def configure_dependencies(repo_root: Path, slatedb_root: Path) -> None:
    manifest_path = repo_root / "Cargo.toml"
    manifest = manifest_path.read_text()
    section = re.search(
        r"(?ms)^\[workspace\.dependencies\]\n(?P<body>.*?)(?=^\[|\Z)", manifest
    )
    if section is None:
        raise RuntimeError("Cargo.toml does not contain [workspace.dependencies]")

    body = section.group("body")
    for name, (directory, features) in DEPENDENCIES.items():
        dependency_path = (slatedb_root / directory).resolve()
        if not (dependency_path / "Cargo.toml").is_file():
            raise RuntimeError(f"{dependency_path} is not a Cargo package")
        replacement = (
            f"{name} = {{ path = {toml_string(str(dependency_path))}, "
            f"features = {json.dumps(features)} }}"
        )
        pattern = re.compile(rf"(?m)^{re.escape(name)}\s*=\s*\{{[^\n]*\}}$")
        body, replacements = pattern.subn(replacement, body)
        if replacements != 1:
            raise RuntimeError(
                f"expected one {name} entry in [workspace.dependencies], found {replacements}"
            )

    manifest = manifest[: section.start("body")] + body + manifest[section.end("body") :]
    manifest_path.write_text(manifest)


def verify_metadata(repo_root: Path, slatedb_root: Path) -> None:
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1"],
        cwd=repo_root,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    metadata = json.loads(result.stdout)
    packages = metadata["packages"]
    failures = []

    for name, directory in RESOLVED_PACKAGES.items():
        expected_manifest = (slatedb_root / directory / "Cargo.toml").resolve()
        matches = [package for package in packages if package["name"] == name]
        if len(matches) != 1:
            failures.append(f"expected one resolved {name} package, found {len(matches)}")
            continue
        package = matches[0]
        actual_manifest = Path(package["manifest_path"]).resolve()
        if package["source"] is not None or actual_manifest != expected_manifest:
            failures.append(
                f"{name} resolved from {package['source'] or actual_manifest}, "
                f"expected {expected_manifest}"
            )

    runner_manifest = (repo_root / "runner" / "Cargo.toml").resolve()
    runners = [
        package
        for package in packages
        if Path(package["manifest_path"]).resolve() == runner_manifest
    ]
    if len(runners) != 1:
        failures.append(f"expected one runner package, found {len(runners)}")
    else:
        direct_dependencies = {
            dependency["name"]: dependency for dependency in runners[0]["dependencies"]
        }
        for name, (directory, _) in DEPENDENCIES.items():
            dependency = direct_dependencies.get(name)
            expected_path = (slatedb_root / directory).resolve()
            actual_path = (
                Path(dependency["path"]).resolve()
                if dependency is not None and dependency["path"] is not None
                else None
            )
            if (
                dependency is None
                or dependency["source"] is not None
                or actual_path != expected_path
            ):
                failures.append(
                    f"runner dependency {name} is not the expected path {expected_path}"
                )

    if failures:
        raise RuntimeError("Cargo resolved the wrong SlateDB source:\n- " + "\n- ".join(failures))

    versions = ", ".join(
        f"{package['name']} {package['version']}"
        for package in packages
        if package["name"] in RESOLVED_PACKAGES
    )
    print(f"verified local SlateDB resolution from {slatedb_root}: {versions}")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: use-local-slatedb.py <benchmark-root> <slatedb-root>")
    repo_root = Path(sys.argv[1]).resolve()
    slatedb_root = Path(sys.argv[2]).resolve()
    configure_dependencies(repo_root, slatedb_root)
    verify_metadata(repo_root, slatedb_root)


if __name__ == "__main__":
    main()
