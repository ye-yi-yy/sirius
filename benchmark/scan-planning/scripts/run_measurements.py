"""Run independent revision binaries serially and preserve unmodified output and JSON."""

import argparse
import hashlib
import json
import os
import platform
import shutil
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument("--baseline", type=Path, required=True)
p.add_argument("--candidate", type=Path, default=Path(__file__).resolve().parents[3])
p.add_argument("--fixture", type=Path, required=True)
p.add_argument(
    "--output",
    type=Path,
    required=True,
    help="New collection directory; reuse only for subsequent stages or --resume",
)
p.add_argument("--pixi", default="pixi")
p.add_argument(
    "--binary-relative",
    default="build/release/extension/sirius/test/cpp/sirius_unittest",
)
p.add_argument(
    "--workloads",
    nargs="+",
    choices=["small4", "small32", "small128", "glob", "iceberg"],
    default=["small4", "glob"],
    help="Workloads for rebind-repeat",
)
p.add_argument(
    "--stage",
    choices=[
        "preflight",
        "capacity",
        "latency",
        "memory",
        "io",
        "sensitivity",
        "rebind-repeat",
    ],
    required=True,
)
p.add_argument("--attempts", type=int, default=25)
p.add_argument("--resume", action="store_true")
p.add_argument("--rounds", type=int, default=4)
a = p.parse_args()
repo = a.candidate.resolve()
if a.attempts <= 0 or a.rounds <= 0:
    p.error("attempts and rounds must be positive")
if repo == a.baseline.resolve():
    p.error("baseline and candidate must be independent checkouts")
pixi = shutil.which(a.pixi)
if not pixi:
    p.error("pixi executable not found")
collection = a.output.resolve()
out = collection / "results" / a.stage
out.mkdir(parents=True, exist_ok=True)
fixture = a.fixture.resolve()
glob = fixture / "glob"
paths = sorted(glob.glob("*.parquet"))
assert len(paths) == 10000 and all(len(os.fsencode(x)) == 96 for x in paths)
width = 96 - len(os.fsencode(glob)) - 1 - len(".parquet")
added = glob / (str(10000).zfill(width) + ".parquet")
settings = {
    "SCAN_PLANNING_GLOB": str(glob / "*.parquet"),
    "SCAN_PLANNING_SOURCE": str(fixture / "source.parquet"),
    "SCAN_PLANNING_ADD_PATH": str(added),
    "SCAN_PLANNING_ICEBERG": str(
        repo / "test/cpp/integration/data/iceberg_snapshot_deletes"
    ),
    "SCAN_PLANNING_EXPECTED_FILES": "10000",
    "SCAN_PLANNING_ALLOCATOR": "glibc",
    "SCAN_PLANNING_SAMPLE_US": "25" if a.stage == "sensitivity" else "100",
}
env = {k: v for k, v in os.environ.items() if not k.startswith("SCAN_PLANNING_")}
env["LD_LIBRARY_PATH"] = "/usr/lib/wsl/lib:" + env.get("LD_LIBRARY_PATH", "")


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def command_output(command, cwd=repo):
    result = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True)
    return {"stdout": result.stdout, "stderr": result.stderr, "exit": result.returncode}


builds = {}
for label, checkout in [("baseline", a.baseline.resolve()), ("candidate", repo)]:
    binary = checkout / a.binary_relative
    cache = checkout / "build/release/CMakeCache.txt"
    if not binary.is_file() or not cache.is_file():
        p.error(f"build binary and CMake cache missing for {label}")
    cache_lines = cache.read_text().splitlines()
    if "SIRIUS_ENABLE_PLANNING_MEASUREMENTS:BOOL=ON" not in cache_lines:
        p.error(f"measurement observations are not enabled for {label}")
    revision = command_output(["git", "rev-parse", "HEAD"], checkout)
    if revision["exit"]:
        p.error(f"cannot read {label} revision")
    builds[label] = {
        "checkout": str(checkout),
        "binary": str(binary),
        "binary_sha256": digest(binary),
        "revision": revision["stdout"].strip(),
        "configuration_lines": [
            line for line in cache_lines if line.startswith(("CMAKE_", "SIRIUS_"))
        ],
        "working_tree_status": command_output(["git", "status", "--short"], checkout),
        "submodules": command_output(["git", "submodule", "status"], checkout),
        "tracked_diff": command_output(
            ["git", "diff", "HEAD", "--binary", "--", "src", "cmake", "test/cpp"],
            checkout,
        ),
        "input_hashes": {
            str(path.relative_to(checkout)): digest(path)
            for directory in ["src", "cmake", "test/cpp"]
            for path in sorted((checkout / directory).rglob("*"))
            if path.is_file()
            and path.suffix in [".cpp", ".hpp", ".cu", ".cuh", ".cmake", ".yaml"]
        },
    }
fixture_hash = digest(fixture / "source.parquet")
metadata_path = collection / "manifest.json"
if metadata_path.exists():
    recorded = json.loads(metadata_path.read_text())
    for label in builds:
        if builds[label]["binary_sha256"] != recorded["builds"][label]["binary_sha256"]:
            p.error("binary changed: use a new output directory")
    if recorded.get("fixture_sha256") != fixture_hash or recorded.get("fixture") != str(
        fixture
    ):
        p.error("fixture changed: use a new output directory")
else:
    recorded = {
        "builds": builds,
        "fixture": str(fixture),
        "fixture_sha256": fixture_hash,
        "environment": {
            "platform": platform.platform(),
            "cpu": command_output(["lscpu"]),
            "gpu": command_output(
                [
                    pixi,
                    "run",
                    "-e",
                    "default",
                    "nvidia-smi",
                    "--query-gpu=name,driver_version",
                    "--format=csv",
                ]
            ),
            "glibc": command_output(["ldd", "--version"]),
        },
        "compiler": command_output([pixi, "run", "-e", "default", "c++", "--version"])[
            "stdout"
        ]
        or "unavailable",
    }
    metadata_path.write_text(json.dumps(recorded, indent=2) + "\n")
if (out / "manifest.json").exists() and not a.resume:
    p.error("stage already exists; use --resume or a new output directory")
manifest = {
    "stage": a.stage,
    "settings": {"attempts": a.attempts, "rounds": a.rounds, "workloads": a.workloads},
    "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "runs": [],
}
if a.resume and (out / "manifest.json").exists():
    previous = json.loads((out / "manifest.json").read_text())
    if previous.get("settings") != manifest["settings"]:
        p.error("resume settings differ from the recorded stage")
    manifest = previous
    manifest.pop("completed_utc", None)


def run(
    label,
    name,
    scenario="small",
    scans=4,
    operation="fresh",
    mode="latency",
    attempts=None,
    extra=None,
    case="[scan_planning_measurements]",
):
    checkout = repo if label == "candidate" else a.baseline.resolve()
    binary = checkout / a.binary_relative
    tag = f"{name}-{label}"
    if any(out.glob(tag + ".*")):
        if a.resume and any(
            r["name"] == tag and r.get("validated") for r in manifest["runs"]
        ):
            previous_run = next(r for r in manifest["runs"] if r["name"] == tag)
            for suffix, expected_hash in previous_run["sha256"].items():
                if digest(out / (tag + suffix)) != expected_hash:
                    raise RuntimeError("Recorded output changed: " + tag + suffix)
            print("SKIP completed", tag, flush=True)
            return
        raise RuntimeError("Refusing to overwrite existing run: " + tag)
    values = {
        **settings,
        "SCAN_PLANNING_SCENARIO": scenario,
        "SCAN_PLANNING_SCANS": str(scans),
        "SCAN_PLANNING_OPERATION": operation,
        "SCAN_PLANNING_MODE": mode,
        "SCAN_PLANNING_ATTEMPTS": str(attempts or a.attempts),
        **(extra or {}),
    }
    command = [pixi, "run", "-e", "default", str(binary), case]
    start = time.monotonic()
    print("START", tag, flush=True)
    with (out / (tag + ".stdout.log")).open("w") as stdout, (
        out / (tag + ".stderr.log")
    ).open("w") as stderr:
        result = subprocess.run(
            command,
            cwd=repo,
            env={**env, **values},
            stdout=stdout,
            stderr=stderr,
            timeout=3600,
        )
    (out / (tag + ".exit")).write_text(str(result.returncode) + "\n")
    records = []
    for line in (out / (tag + ".stdout.log")).read_text().splitlines():
        offset = line.find('{"record":')
        if offset >= 0:
            records.append(json.loads(line[offset:]))
    (out / (tag + ".jsonl")).write_text(
        "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records)
    )
    item = {
        "name": tag,
        "revision": label,
        "scenario": scenario,
        "scans": scans,
        "operation": operation,
        "api_window": (
            "PendingQuery(sql)"
            if scenario == "iceberg" and operation in ["fresh", "sharing"]
            else (
                "PendingQuery(prepared)"
                if operation == "rebind"
                else (
                    "Execute with scoped rebuild"
                    if operation == "rebuild"
                    else (
                        "Execute failure cleanup"
                        if operation == "exception"
                        else "Prepare"
                    )
                )
            )
        ),
        "mode": mode,
        "command": command,
        "cwd": str(repo),
        "environment": values,
        "exit": result.returncode,
        "seconds": time.monotonic() - start,
        "records": len(records),
    }
    manifest["runs"].append(item)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("END", tag, result.returncode, round(item["seconds"], 2), flush=True)
    if result.returncode:
        raise RuntimeError("Failed run: " + tag)
    if (
        case == "[scan_planning_measurements]"
        and values.get("SCAN_PLANNING_METRICS") != "1"
    ):
        kind = "native_preparation" if scenario == "native" else "planning_attempt"
        expected = int(values["SCAN_PLANNING_ATTEMPTS"])
        rows = [r for r in records if r["record"] == kind]
        assert len(rows) == expected, tag
        assert [r["attempt"] for r in rows] == list(range(expected)), tag
        if kind == "planning_attempt":
            assert all(r["timing_valid"] == (mode == "latency") for r in rows), tag
        if mode in ["memory", "io"]:
            assert (
                sum(r["record"] == "planning_" + mode for r in records) == expected
            ), tag
        if mode == "memory":
            assert all(
                r["duckdb_jemalloc_observed"]
                for r in records
                if r["record"] == "planning_memory"
            ), tag

    if added.exists():
        raise RuntimeError("Benchmark left added inventory file: " + str(added))
    item["validated"] = True
    item["sha256"] = {
        suffix: digest(out / (tag + suffix))
        for suffix in [".stdout.log", ".stderr.log", ".jsonl", ".exit"]
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


workloads = [
    ("small4", "small", 4),
    ("small32", "small", 32),
    ("small128", "small", 128),
    ("glob", "glob", 1),
    ("iceberg", "iceberg", 1),
]


def pair(name, scenario, scans, operation, mode, attempts=None):
    order = (
        ["candidate", "baseline"]
        if len(manifest["runs"]) // 2 % 2
        else ["baseline", "candidate"]
    )
    for label in order:
        run(label, name, scenario, scans, operation, mode, attempts)


if a.stage == "rebind-repeat":
    manifest["binary_hashes"] = {
        label: value["binary_sha256"] for label, value in builds.items()
    }
    manifest["protocol"] = (
        f"{a.rounds} process pairs per workload, {a.attempts} attempts per process; alternating order; all attempts retained."
    )
    for round_index in range(a.rounds):
        order = (
            ["baseline", "candidate"]
            if round_index % 2 == 0
            else ["candidate", "baseline"]
        )
        for name, scenario, scans in [w for w in workloads if w[0] in a.workloads]:
            for label in order:
                run(
                    label,
                    f"round{round_index + 1}-{name}-rebind",
                    scenario,
                    scans,
                    "rebind",
                    "latency",
                )
elif a.stage == "preflight":
    for label in ["baseline", "candidate"]:
        run(label, "observers", case="[planning_observers]", attempts=1)
    for mode in ["latency", "memory", "io"]:
        for operation in ["fresh", "rebind", "rebuild", "exception", "sharing"]:
            pair(f"{mode}-small4-{operation}", "small", 4, operation, mode, 1)
    for name, scenario, scans in workloads[1:]:
        for operation in ["fresh", "rebind", "rebuild"]:
            pair(
                f"latency-{name}-{operation}", scenario, scans, operation, "latency", 1
            )
    run("candidate", "memory-mismatch", operation="mismatch", mode="memory", attempts=1)
    pair("native", "native", 1, "fresh", "latency", 1)
elif a.stage == "capacity":
    for name, scenario, scans in workloads:
        run(
            "candidate",
            name,
            scenario,
            scans,
            extra={"SCAN_PLANNING_METRICS": "1"},
            attempts=1,
        )
    os.link(fixture / "source.parquet", added)
    try:
        # The generic runner validates cleanup; this input belongs to the driver.
        old = settings["SCAN_PLANNING_ADD_PATH"]
        settings["SCAN_PLANNING_ADD_PATH"] = ""
        # Save the enlarged fixture separately without applying the cleanup check.
        grown = added
        added = glob / "not-created-by-benchmark"
        run(
            "candidate",
            "glob-grown",
            "glob",
            1,
            extra={
                "SCAN_PLANNING_METRICS": "1",
                "SCAN_PLANNING_EXPECTED_FILES": "10001",
            },
            attempts=1,
        )
    finally:
        grown.unlink()
        added = grown
        settings["SCAN_PLANNING_ADD_PATH"] = old
elif a.stage == "latency":
    for name, scenario, scans in workloads:
        for operation in ["fresh", "rebind", "rebuild"]:
            pair(f"{name}-{operation}", scenario, scans, operation, "latency")
    pair("native", "native", 1, "fresh", "latency")
elif a.stage in ["memory", "sensitivity"]:
    selected = (
        [workloads[0], workloads[3], workloads[4]]
        if a.stage == "memory"
        else [workloads[0], workloads[3]]
    )
    for name, scenario, scans in selected:
        for operation in ["fresh", "rebind", "rebuild"]:
            pair(f"{name}-{operation}", scenario, scans, operation, "memory")
    if a.stage == "memory":
        for operation in ["sharing", "exception"]:
            pair("glob-" + operation, "glob", 1, operation, "memory")
        run("candidate", "glob-mismatch", "glob", 1, "mismatch", "memory")
elif a.stage == "io":
    for name, scenario, scans in [workloads[0], workloads[3], workloads[4]]:
        for operation in ["fresh", "rebind", "rebuild"]:
            pair(f"{name}-{operation}", scenario, scans, operation, "io", 5)
manifest["completed_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
(out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print("COMPLETE", a.stage, flush=True)
