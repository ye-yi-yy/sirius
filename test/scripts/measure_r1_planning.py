#!/usr/bin/env python3
# Copyright 2026, Sirius Contributors.
# Licensed under the Apache License, Version 2.0 (the "License");
# See the LICENSE file at the repo root for the full text.
"""Run T6 with a merged-base binary and an opt-in Linux/glibc allocation probe.

Run from the repository root in the pixi environment. Binaries must be built
with identical options and dependency versions. A baseline-only T6 test plugin
is compiled from the same source and loaded into the pre-R1 test binary; no
baseline implementation files are changed. Keep the supplied baseline's build
provenance alongside the generated manifest.
"""

import argparse
import hashlib
import json
import os
import re
from collections import Counter
from pathlib import Path
import shlex
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-binary", required=True, type=Path)
    parser.add_argument(
        "--binary",
        type=Path,
        default=Path("build/release/extension/sirius/test/cpp/sirius_unittest"),
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--strace", default="strace")
    parser.add_argument("--attempts", type=int, default=25)
    args = parser.parse_args()
    if args.attempts < 20:
        parser.error("latency acceptance requires at least 20 attempts")
    root = Path.cwd()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    manifest = {"attempts": args.attempts, "results": {}, "binaries": {}}
    for label, binary in (
        ("baseline", args.baseline_binary),
        ("candidate", args.binary),
    ):
        manifest["binaries"][label] = {
            "path": str(binary.resolve()),
            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        }

    def run(label, command, env=None, timeout=900):
        start = time.time()
        with (out / f"{label}.log").open("w") as log:
            result = subprocess.run(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                env=env,
                timeout=timeout,
                check=False,
            )
        manifest["results"][label] = {
            "exit": result.returncode,
            "seconds": time.time() - start,
            "command": [str(arg) for arg in command],
            "environment": {
                key: value
                for key, value in (env or {}).items()
                if key.startswith("T6_") or key == "LD_PRELOAD"
            },
        }
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        if result.returncode:
            raise RuntimeError(f"{label} failed; see {out / (label + '.log')}")

    probe = out / "allocation_probe.so"
    run(
        "build-probe",
        [
            "cc",
            "-shared",
            "-fPIC",
            "-O2",
            "-std=c11",
            "test/scripts/r1_allocation_probe.c",
            "-ldl",
            "-pthread",
            "-o",
            str(probe),
        ],
    )
    # Reuse exact include paths and compiler settings from the candidate build.
    commands = json.loads(Path("build/release/compile_commands.json").read_text())
    entry = next(
        item for item in commands if item["file"].endswith("test_table_scan_r1_t6.cpp")
    )
    command = shlex.split(entry["command"])
    if Path(command[0]).name == "sccache":
        command.pop(0)
    cleaned = []
    skip = False
    for item in command:
        if skip:
            skip = False
        elif item in ("-o", "-MF", "-MT", "-MQ"):
            skip = True
        elif item not in ("-c", "-MD", "-MMD"):
            cleaned.append(item)
    baseline_plugin = out / "baseline_t6.so"
    run(
        "build-baseline-plugin",
        cleaned
        + ["-shared", "-fPIC", "-DSIRIUS_T6_BASELINE=1", "-o", str(baseline_plugin)],
    )
    with tempfile.TemporaryDirectory(prefix="sirius-t6-") as directory:
        fixture = Path(directory)
        source = fixture / "source.parquet"
        sql = f"COPY (SELECT 1::INTEGER AS id) TO '{source}' (FORMAT PARQUET)"
        run(
            "fixture",
            ["build/release/duckdb", "-c", sql],
            env={**os.environ, "SIRIUS_DISABLE": "1"},
        )
        glob_dir = fixture / "glob"
        glob_dir.mkdir()
        stem_size = 96 - len(str(glob_dir)) - 1 - len(".parquet")
        if stem_size < 5:
            raise RuntimeError(
                "temporary directory is too long for 96-byte fixture paths"
            )

        def path_for(index):
            return glob_dir / (str(index).zfill(stem_size) + ".parquet")

        for index in range(10000):
            os.link(source, path_for(index))
        assert len(str(path_for(0)).encode()) == 96
        for scenario in ("small", "glob", "iceberg"):
            for side, binary in (
                ("baseline", args.baseline_binary),
                ("candidate", args.binary),
            ):
                env = {
                    **os.environ,
                    "T6_SCENARIO": scenario,
                    "T6_ATTEMPTS": str(args.attempts),
                    "T6_GLOB": str(glob_dir / "*.parquet"),
                    "T6_ICEBERG": str(
                        root / "test/cpp/integration/data/iceberg_snapshot_deletes"
                    ),
                }
                if side == "baseline":
                    env["LD_PRELOAD"] = str(baseline_plugin)
                run(
                    f"{side}-{scenario}-latency",
                    [str(binary.resolve()), "T6 indicative benchmark"],
                    env,
                )
                env["T6_TRACE"] = "1"
                trace_env = dict(env)
                preload = trace_env.pop("LD_PRELOAD", None)
                trace_target = (
                    [str(binary.resolve()), "T6 indicative benchmark"]
                    if preload is None
                    else [
                        "env",
                        "LD_PRELOAD=" + preload,
                        str(binary.resolve()),
                        "T6 indicative benchmark",
                    ]
                )
                run(
                    f"{side}-{scenario}-io",
                    [
                        args.strace,
                        "-f",
                        "-qq",
                        "-e",
                        "trace=%file,%network,write",
                        "-o",
                        str(out / f"{side}-{scenario}.strace"),
                    ]
                    + trace_target,
                    trace_env,
                )
        run(
            "candidate-glob-capacity",
            [str(args.binary.resolve()), "T6 indicative benchmark"],
            {
                **os.environ,
                "LD_PRELOAD": str(probe),
                "T6_SCENARIO": "glob",
                "T6_GLOB": str(glob_dir / "*.parquet"),
                "T6_ATTEMPTS": "1",
                "T6_ALLOCATIONS": "1",
                "T6_MEMORY": "0",
                "T6_ADD_PATH": str(path_for(10000)),
                "T6_SOURCE": str(source),
            },
        )
        # Sample RSS in a separate run: the sampler itself allocates memory and must not
        # contaminate the malloc-family totals above.
        path_for(10000).unlink()
        run(
            "candidate-glob-rss",
            [str(args.binary.resolve()), "T6 indicative benchmark"],
            {
                **os.environ,
                "T6_SCENARIO": "glob",
                "T6_GLOB": str(glob_dir / "*.parquet"),
                "T6_ATTEMPTS": "1",
                "T6_MEMORY": "1",
                "T6_ADD_PATH": str(path_for(10000)),
                "T6_SOURCE": str(source),
            },
        )
        run(
            "candidate-native",
            [str(args.binary.resolve()), "T6 indicative benchmark"],
            {**os.environ, "T6_SCENARIO": "native", "T6_ATTEMPTS": str(args.attempts)},
        )

    def latency_values(path):
        text = path.read_text()
        line = next(line for line in text.splitlines() if "T6_RESULT " in line)
        return dict(re.findall(r"(\w+)=([^\s]+)", line))

    def planning_calls(path):
        windows = {"finalize": [], "rebuild": []}
        phase = None
        calls = Counter()
        for line in path.open():
            marker = re.search(r"T6_MARK (BEGIN|END) (finalize|rebuild)", line)
            if marker:
                if marker[1] == "BEGIN":
                    if phase is not None:
                        raise RuntimeError("overlapping trace windows")
                    phase, calls = marker[2], Counter()
                else:
                    if phase != marker[2]:
                        raise RuntimeError("unmatched trace marker")
                    windows[phase].append(dict(calls))
                    phase = None
                continue
            syscall = re.match(r"\s*\d+\s+(\w+)\(", line)
            if phase and syscall and syscall[1] != "write":
                calls[syscall[1]] += 1
        if phase or any(len(items) != args.attempts for items in windows.values()):
            raise RuntimeError(f"missing planning trace windows: {path}")
        return windows

    acceptance = {"latency": {}, "io": {}, "capacity": {}}
    passed = True
    for scenario in ("small", "glob", "iceberg"):
        before = latency_values(out / f"baseline-{scenario}-latency.log")
        after = latency_values(out / f"candidate-{scenario}-latency.log")
        limit = {"small": 100, "glob": 5000}.get(scenario)
        deltas = {
            key: float(after[key]) - float(before[key])
            for key in (
                "finalize_median_us",
                "finalize_p95_us",
                "rebuild_median_us",
                "rebuild_p95_us",
            )
        }
        latency_pass = limit is None or all(value <= limit for value in deltas.values())
        acceptance["latency"][scenario] = {
            "added_us": deltas,
            "limit_us": limit,
            "pass": latency_pass,
        }
        base_calls = planning_calls(out / f"baseline-{scenario}.strace")
        new_calls = planning_calls(out / f"candidate-{scenario}.strace")
        io_pass = base_calls == new_calls
        acceptance["io"][scenario] = {
            "baseline": base_calls,
            "candidate": new_calls,
            "pass": io_pass,
        }
        passed = passed and latency_pass and io_pass
    capacity_log = (out / "candidate-glob-capacity.log").read_text()
    capacity_line = next(
        line for line in capacity_log.splitlines() if "T6_ALLOCATIONS " in line
    )
    acceptance["capacity"] = dict(re.findall(r"(\w+)=([^\s]+)", capacity_line))
    acceptance["pass"] = passed
    (out / "acceptance.json").write_text(json.dumps(acceptance, indent=2) + "\n")
    if not passed:
        raise RuntimeError(
            f"AC9 latency or I/O gate failed; see {out / 'acceptance.json'}"
        )


if __name__ == "__main__":
    main()
