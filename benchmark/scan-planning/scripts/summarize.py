"""Derive statistics from preserved successful-run JSON without dropping outliers."""

import argparse
import json
import math
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--input",
    type=Path,
    required=True,
    help="Collection directory containing manifest.json and results/",
)
parser.add_argument(
    "--output",
    type=Path,
    required=True,
    help="Directory for report.md and summary.json",
)
parser.add_argument(
    "--report-inputs",
    type=Path,
    help="Optional explicit rebind collection selection JSON",
)
args = parser.parse_args()
root = args.input.resolve()
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=True)
summary = {}


def stats(values):
    values = sorted(values)
    return {
        "n": len(values),
        "median": values[math.ceil(len(values) * 0.5) - 1],
        "p95": values[math.ceil(len(values) * 0.95) - 1],
        "minimum": values[0],
        "maximum": values[-1],
    }


def phase(r, name):
    return r["phases"][name]["inclusive_us"]


for stage in ["latency", "memory", "sensitivity", "io"]:
    manifest_path = root / "results" / stage / "manifest.json"
    if not manifest_path.exists():
        continue
    section = {}
    for run in json.loads(manifest_path.read_text())["runs"]:
        if run["exit"]:
            continue
        records = [
            json.loads(line)
            for line in (manifest_path.parent / (run["name"] + ".jsonl"))
            .read_text()
            .splitlines()
        ]
        entry = {"run": run, "statistics": {}}
        if stage == "latency":
            rows = [r for r in records if r["record"] == "planning_attempt"]
            if rows:
                fields = {
                    "api_total_us": [r["api_total_us"] for r in rows],
                    "normalized_planning_us": [
                        r["api_total_us"] - phase(r, "native_walk") for r in rows
                    ],
                    "hook_finalize_us": [
                        phase(r, "optimizer_hook")
                        + phase(r, "finalize")
                        - phase(r, "native_walk")
                        for r in rows
                    ],
                }
                for name in rows[0]["phases"]:
                    fields[name + "_us"] = [phase(r, name) for r in rows]
            else:
                rows = [r for r in records if r["record"] == "native_preparation"]
                fields = {"native_walk_us": [r["native_walk_us"] for r in rows]}
                if rows[0]["checkpoint_acquire_available"]:
                    fields["checkpoint_acquire_us"] = [
                        r["checkpoint_acquire_us"] for r in rows
                    ]
            entry["first_attempt"] = rows[0]
            entry["statistics"] = {k: stats(v) for k, v in fields.items()}
        elif stage in ["memory", "sensitivity"]:
            rows = [r for r in records if r["record"] == "planning_memory"]
            assert rows and all(
                r["duckdb_jemalloc_observed"] and not r["exact_peak_verified"]
                for r in rows
            )
            entry["first_attempt"] = rows[0]
            entry["statistics"] = {
                k: stats([r[k] for r in rows])
                for k in rows[0]
                if isinstance(rows[0][k], (int, float))
                and not isinstance(rows[0][k], bool)
                and k != "attempt"
            }
        else:
            rows = [r for r in records if r["record"] == "planning_io"]
            entry["statistics"] = {
                k: stats([r[k] for r in rows])
                for k in rows[0]
                if isinstance(rows[0][k], int) and k != "attempt"
            }
        section[run["name"]] = entry
    summary[stage] = section
# The report input manifest fixes the evidence used for the accepted results.
selection = json.loads(args.report_inputs.read_text()) if args.report_inputs else {}
for case, source in selection.items():
    directory = root / source["directory"]
    collection = json.loads((directory / "manifest.json").read_text())
    assert "completed_utc" in collection
    main_builds = json.loads((root / "manifest.json").read_text())["builds"]
    assert collection.get("binary_hashes") == {
        label: value["binary_sha256"] for label, value in main_builds.items()
    }, "Selected rebind binaries differ from main collection"
    for revision in ["baseline", "candidate"]:
        rows, files, first_attempts = [], [], []
        selected_runs = [
            r
            for r in collection["runs"]
            if r["revision"] == revision
            and r["operation"] == "rebind"
            and r["name"].endswith("-" + source["workload"] + "-rebind-" + revision)
        ]
        assert selected_runs, (case, revision)
        for run in selected_runs:
            name = run["name"]
            assert run["exit"] == 0
            path = directory / (name + ".jsonl")
            batch = [json.loads(line) for line in path.read_text().splitlines()]
            expected = int(run["environment"]["SCAN_PLANNING_ATTEMPTS"])
            assert len(batch) == expected
            assert [r["attempt"] for r in batch] == list(range(expected))
            assert all(
                r["record"] == "planning_attempt"
                and r["timing_valid"]
                and r["operation"] == "rebind"
                and r["mode"] == "latency"
                for r in batch
            )
            rows.extend(batch)
            first_attempts.append(batch[0])
            files.append(str(path.relative_to(root)))
        fields = {
            "api_total_us": [r["api_total_us"] for r in rows],
            "normalized_planning_us": [
                r["api_total_us"] - phase(r, "native_walk") for r in rows
            ],
            "hook_finalize_us": [
                phase(r, "optimizer_hook")
                + phase(r, "finalize")
                - phase(r, "native_walk")
                for r in rows
            ],
        }
        for name in rows[0]["phases"]:
            fields[name + "_us"] = [phase(r, name) for r in rows]
        entry = summary.setdefault("latency", {}).setdefault(case + "-" + revision, {})
        entry.update(
            {
                "run": run,
                "source_files": files,
                "first_attempt": rows[0],
                "first_attempts": first_attempts,
                "statistics": {k: stats(v) for k, v in fields.items()},
            }
        )

(output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
lines = [
    "# Scan Planning Measurements",
    "",
    "## Method",
    "",
    "The baseline and candidate were compiled separately against their own source trees and libraries. The source and environment manifest records revisions, binary hashes, dependencies, inputs and reproduction commands. Sample counts, process order and source records are recorded for each case in summary.json and the collection manifests. Every process includes its first attempt. No successful outliers are removed. Modes run in separate processes, and benchmark processes run serially.",
    "",
    "Iceberg fresh preparation uses PendingQuery(sql), including executor initialization: direct API Prepare installed no GPU candidate in either revision. Iceberg rebind retains an API-prepared CPU generation while constructing the GPU generation, so it is not evidence for two simultaneous GPU generations. Other fresh cases use Prepare. The primary prepare/rebind window is the complete API call minus native metadata walk time in that same attempt. Raw API totals and all inclusive phases remain in the raw records and summary. Execute rebuild uses its explicit scope; its API total also includes an injected error and cleanup and is diagnostic only. Parent and child phases are not added together. Statistics use nearest-rank median and P95. Differences below are differences of independent statistics, not percentiles of paired differences.",
    "",
]
if "latency" in summary:
    lines += [
        "## Latency",
        "",
        "All times are microseconds. The limit is added time: 100 microseconds for four scans and 5,000 microseconds for the 10,000-file inventory. Wider joins and Iceberg are reported without an invented threshold.",
        "",
        "| Workload / operation | Baseline median | Candidate median | Added median | Baseline P95 | Candidate P95 | Added P95 | Applicable limit / result |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    latency = summary["latency"]
    for name, c in latency.items():
        if not name.endswith("-candidate") or name == "native-candidate":
            continue
        b = latency.get(name[:-9] + "baseline")
        if not b:
            continue
        key = (
            "execute_rebuild_us"
            if c["run"]["operation"] == "rebuild"
            else "normalized_planning_us"
        )
        bs, cs = b["statistics"][key], c["statistics"][key]
        dm, dp = cs["median"] - bs["median"], cs["p95"] - bs["p95"]
        limit = (
            100
            if c["run"]["scenario"] == "small" and c["run"]["scans"] == 4
            else 5000 if c["run"]["scenario"] == "glob" else None
        )
        verdict = (
            "Not specified"
            if limit is None
            else f'{limit}: median {"PASS" if dm<=limit else "FAIL"}; P95 {"PASS" if dp<=limit else "FAIL"}'
        )
        lines.append(
            f'| {name[:-10]} | {bs["median"]:.3f} | {cs["median"]:.3f} | {dm:.3f} | {bs["p95"]:.3f} | {cs["p95"]:.3f} | {dp:.3f} | {verdict} |'
        )
    lines += [
        "",
        "### Separate phase timings",
        "",
        "Values are median / P95, in microseconds. These are inclusive scopes and are not additive.",
        "",
        "| Workload / operation / revision | Optimizer hook | Finalize | Hook + finalize, walk excluded | Execute rebuild | Lifecycle lock wait | Native walk |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name, e in latency.items():
        if name.startswith("native-"):
            continue
        values = []
        for key in [
            "optimizer_hook_us",
            "finalize_us",
            "hook_finalize_us",
            "execute_rebuild_us",
            "lifecycle_lock_wait_us",
            "native_walk_us",
        ]:
            s = e["statistics"][key]
            values.append(f'{s["median"]:.3f} / {s["p95"]:.3f}')
        lines.append("| " + name + " | " + " | ".join(values) + " |")
    lines += [
        "",
        "### Native preparation",
        "",
        "| Revision | Metadata walk median / P95 (µs) | Checkpoint acquisition median / P95 (µs) |",
        "|---|---:|---:|",
    ]
    for label in ["baseline", "candidate"]:
        e = latency.get("native-" + label)
        if not e:
            continue
        w = e["statistics"]["native_walk_us"]
        c = e["statistics"].get("checkpoint_acquire_us")
        ct = f'{c["median"]:.3f} / {c["p95"]:.3f}' if c else "Not present in baseline"
        lines.append(f'| {label} | {w["median"]:.3f} / {w["p95"]:.3f} | {ct} |')
    lines += [
        "",
        "### First attempt and raw API totals",
        "",
        "Every process first attempt is included in the aggregate above. The first-API column lists every process start for cases assembled from multiple processes.",
        "",
        "| Workload / revision | First API (µs) | API median / P95 (µs) |",
        "|---|---:|---:|",
    ]
    for name, e in latency.items():
        if "api_total_us" not in e["statistics"]:
            continue
        s = e["statistics"]["api_total_us"]
        first = " / ".join(
            f'{r["api_total_us"]:.3f}'
            for r in e.get("first_attempts", [e["first_attempt"]])
        )
        lines.append(f'| {name} | {first} | {s["median"]:.3f} / {s["p95"]:.3f} |')
for stage in ["memory", "sensitivity"]:
    if stage not in summary:
        continue
    lines += [
        "",
        "## " + ("Allocator memory" if stage == "memory" else "Sampling sensitivity"),
        "",
        "Values are bytes. Peaks are `sampled_allocator_observed`; exact historical allocation peaks are not verified. Rebind values are relative to the origin before the old generation was created, and the fixture explicitly retains the old generation through the endpoint. System glibc arena accounting and DuckDB prefixed jemalloc allocated bytes are observed together; their samples are not atomic across allocators. Allocator caches and allocations between samples limit precision.",
        "",
        "| Workload / operation / revision | Peak from fresh origin, median / max | Retained from fresh origin, median / max | Largest sample gap (µs) |",
        "|---|---:|---:|---:|",
    ]
    for name, e in summary[stage].items():
        s = e["statistics"]
        p = s["peak_from_fresh_origin_bytes"]
        r = s["retained_from_fresh_origin_bytes"]
        g = s["maximum_sample_gap_us"]
        lines.append(
            f'| {name} | {p["median"]} / {p["maximum"]} | {r["median"]} / {r["maximum"]} | {g["maximum"]} |'
        )
    lines += [
        "",
        "### Baseline comparison",
        "",
        "Differences of median and maximum observed growth are supplemental process-level comparisons, not attribution to individual feature allocations.",
        "",
        "| Workload / operation | Added median sampled peak | Difference of maximum sampled peaks | Added median retained | Difference of maximum retained |",
        "|---|---:|---:|---:|---:|",
    ]
    for name, c in summary[stage].items():
        if not name.endswith("-candidate"):
            continue
        b = summary[stage].get(name[:-9] + "baseline")
        if not b:
            continue
        vals = []
        for metric in [
            "peak_from_fresh_origin_bytes",
            "retained_from_fresh_origin_bytes",
        ]:
            for stat in ["median", "maximum"]:
                vals.append(
                    str(c["statistics"][metric][stat] - b["statistics"][metric][stat])
                )
        lines.append("| " + name[:-10] + " | " + " | ".join(vals) + " |")
if "io" in summary:
    lines += [
        "",
        "## Observed I/O",
        "",
        "Attempt counts are recorded in the run manifests. The table shows the maximum counter across attempts for each revision. Counter ranges and raw values remain in the JSON. These cover DuckDB local filesystem APIs and Sirius datasource requests, not every syscall, network request or diagnostic log write. Zero difference within this coverage does not establish zero process-wide I/O.",
        "",
        "| Workload / operation / revision | Opens | Read calls | Read bytes | Metadata requests | Directory lists | Files enumerated | Sirius opens / reads / requested bytes / metadata |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, e in summary["io"].items():
        s = e["statistics"]
        fields = [
            "opens",
            "read_calls",
            "bytes_read",
            "metadata_requests",
            "directory_lists",
            "files_enumerated",
        ]
        values = [str(s[k]["maximum"]) for k in fields]
        tail = " / ".join(
            str(s[k]["maximum"])
            for k in [
                "sirius_datasource_opens",
                "sirius_datasource_read_calls",
                "sirius_datasource_requested_bytes",
                "sirius_datasource_metadata_requests",
            ]
        )
        lines.append("| " + name + " | " + " | ".join(values) + " | " + tail + " |")
lines += [
    "",
    "## Capacity observations",
    "",
    "F counts bound inventory entries; L is canonical identity text capacity; E is one captured side's evidence capacity; C is transient owned path capacity; I is temporary sort-index capacity. Byte capacities include reserved storage and can include inline string capacity. These are supplemental measurements, not a reconstruction of the allocator peak. Definitions and formulas are in the plan.",
    "",
    "| Inventory | F | L | E | C | I | Fresh bound | Rebind bound | Retained bound |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
]
for p in sorted((root / "results/capacity").glob("*.stdout.log")):
    for line in p.read_text().splitlines():
        if line.startswith("SCAN_PLANNING_CAPTURE_METRICS"):
            d = dict(x.split("=", 1) for x in line.split()[1:])
            lines.append(
                "| "
                + p.name.removesuffix(".stdout.log")
                + " | "
                + " | ".join(
                    d[k]
                    for k in [
                        "F",
                        "L",
                        "E",
                        "C",
                        "I",
                        "fresh_bound",
                        "rebind_bound",
                        "steady_bound",
                    ]
                )
                + " |"
            )
lines += [
    "",
    "## Interpretation and reproduction",
    "",
    "The strict peak-live-memory criterion remains unverified: periodic and phase-boundary allocator statistics can miss short-lived peaks. Retained readings include allocator accounting effects and are process-level observations, not exact attribution of requested bytes. The sampled observations and capacity limits must not be reported as exact peak verification.",
    "",
    "The manifest records the actual environment. Extension loading, fixture setup and pinning occur outside the windows; these results do not measure cold extension-load cost. Laptop power settings and competing host activity are not controlled by the WSL runner. Sequential revision batches do not isolate host drift; complete API differences also include DuckDB optimization and surrounding work and must not all be attributed to scan validation. Measurements include first-run and scheduling variation. Instrumentation is enabled equally in both measurement builds; a separate observer-disabled comparison is required to quantify instrumentation overhead.",
    "",
    "See `plan.md` for windows and definitions, `manifest.json` for the environment, the optional report-inputs file for explicitly selected rebind data sources, and `summary.json` plus per-stage manifests for every run command and derived statistic. Unmodified standard output, standard error, exit status and extracted JSON are retained for each run. Failed preflight observations are retained separately and excluded from formal results.",
    "",
]

manifest = json.loads((root / "manifest.json").read_text())
environment = manifest["environment"]
cpu_lines = environment["cpu"]["stdout"].splitlines()


def field(prefix):
    return next(
        (
            line.split(":", 1)[1].strip()
            for line in cpu_lines
            if line.startswith(prefix)
        ),
        "unavailable",
    )


cpu, cpus = field("Model name:"), field("CPU(s):")
gpu_lines = environment["gpu"]["stdout"].splitlines()
gpu = gpu_lines[1].split(",")[0] if len(gpu_lines) > 1 else "GPU details unavailable"
compiler = manifest.get("compiler", "unavailable").splitlines()[0]
glibc_lines = environment["glibc"]["stdout"].splitlines()
glibc = glibc_lines[0] if glibc_lines else "allocator version unavailable"
environment_lines = [
    "## Environment",
    "",
    f"{cpu}; {cpus} logical CPUs; {gpu}. {compiler}. {glibc}.",
    "",
    "Build configurations, revisions and binary hashes are recorded in the collection manifest. Compare them before interpreting the baseline difference. The fixture uses one DuckDB planning thread and the checked-in integration configuration. Runtime warnings and environment limitations remain in the per-run logs.",
    "",
]
lines[lines.index("## Method") : lines.index("## Method")] = environment_lines

completed_stages = []
for stage in ["capacity", "latency", "memory", "sensitivity", "io"]:
    manifest_path = root / "results" / stage / "manifest.json"
    if manifest_path.exists() and "completed_utc" in json.loads(
        manifest_path.read_text()
    ):
        completed_stages.append(stage)
status = (
    "Formal collection complete."
    if len(completed_stages) == 5
    else "Collection in progress. Completed stages: "
    + ", ".join(completed_stages)
    + "."
)
lines[2:2] = [status, ""]
extra = []
if "latency" in summary:
    extra += [
        "## Capture, comparison and publication",
        "",
        "Values are median / P95 in microseconds. Baseline zero means the feature-specific scope is absent; baseline total planning remains measured.",
        "",
        "| Workload / operation / revision | Logical capture | Physical capture | Candidate build | Comparison | Publication |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name, entry in summary["latency"].items():
        if name.startswith("native-"):
            continue
        values = []
        for key in [
            "capture_logical_us",
            "capture_physical_us",
            "candidate_build_us",
            "comparison_us",
            "publish_us",
        ]:
            value = entry["statistics"][key]
            values.append(f'{value["median"]:.3f} / {value["p95"]:.3f}')
        extra.append("| " + name + " | " + " | ".join(values) + " |")
    extra.append("")
capacities = {}
for path in (root / "results/capacity").glob("*.stdout.log"):
    for line in path.read_text().splitlines():
        if line.startswith("SCAN_PLANNING_CAPTURE_METRICS"):
            fields = dict(item.split("=", 1) for item in line.split()[1:])
            capacities[path.name.removesuffix("-candidate.stdout.log")] = fields
if "memory" in summary:
    extra += [
        "## Sampled observations against capacity limits",
        "",
        "The differences below subtract each revision's maximum observed process growth. They are not a bound on feature-attributed peak memory. Rebind uses the larger inventory's terms conservatively and retained limits allow both surviving generations. Negative differences are retained, not clamped to zero. No row establishes an exact historical peak.",
        "",
        "| Workload / operation | Difference of maximum sampled peaks (bytes) | Peak limit (bytes) | Sampled comparison | Difference of maximum retained (bytes) | Retained limit (bytes) |",
        "|---|---:|---:|---|---:|---:|",
    ]
    for name, candidate in summary["memory"].items():
        run = candidate["run"]
        if (
            not name.endswith("-candidate")
            or run["operation"] not in ["fresh", "rebind"]
            or run["scenario"] != "glob"
        ):
            continue
        baseline = summary["memory"].get(name[:-9] + "baseline")
        if not baseline:
            continue
        workload = name.rsplit("-", 2)[0]
        capacity = capacities.get(
            "glob-grown"
            if workload == "glob" and run["operation"] == "rebind"
            else workload
        )
        if not capacity:
            continue
        difference = (
            candidate["statistics"]["peak_from_fresh_origin_bytes"]["maximum"]
            - baseline["statistics"]["peak_from_fresh_origin_bytes"]["maximum"]
        )
        retained = (
            candidate["statistics"]["retained_from_fresh_origin_bytes"]["maximum"]
            - baseline["statistics"]["retained_from_fresh_origin_bytes"]["maximum"]
        )
        bound = int(
            capacity["rebind_bound" if run["operation"] == "rebind" else "fresh_bound"]
        )
        retained_bound = int(capacity["steady_bound"]) * (
            2 if run["operation"] == "rebind" else 1
        )
        result = (
            "Below; exact peak unverified"
            if difference <= bound
            else "Above; attribution uncertain"
        )
        extra.append(
            f"| {name[:-10]} | {difference} | {bound} | {result} | {retained} | {retained_bound} |"
        )
    extra.append("")

if "memory" in summary:
    extra += [
        "## Memory window readings",
        "",
        "Values below are medians in bytes. Raw records retain every before/peak/after reading and allocator component. Peak-added and retained deltas here use the immediate window baseline; the separate origin-relative table includes the old generation for rebind.",
        "",
        "| Workload / operation / revision | Before | Sampled peak | After | Peak added | Retained delta |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name, entry in summary["memory"].items():
        values = [
            str(entry["statistics"][key]["median"])
            for key in [
                "live_before_bytes",
                "peak_live_bytes",
                "live_after_bytes",
                "peak_added_bytes",
                "retained_delta_bytes",
            ]
        ]
        extra.append("| " + name + " | " + " | ".join(values) + " |")
    extra.append("")


findings = ["## Results", ""]
if "latency" in summary:
    for workload, limit in [("small4", 100), ("glob", 5000)]:
        for operation in ["fresh", "rebind", "rebuild"]:
            key = workload + "-" + operation + "-"
            baseline = summary["latency"].get(key + "baseline")
            candidate = summary["latency"].get(key + "candidate")
            if not baseline or not candidate:
                continue
            metric = (
                "execute_rebuild_us"
                if operation == "rebuild"
                else "normalized_planning_us"
            )
            delta_median = (
                candidate["statistics"][metric]["median"]
                - baseline["statistics"][metric]["median"]
            )
            delta_p95 = (
                candidate["statistics"][metric]["p95"]
                - baseline["statistics"][metric]["p95"]
            )
            result = "PASS" if max(delta_median, delta_p95) <= limit else "FAIL"
            findings.append(
                f"- {workload} {operation}: added median {delta_median:.3f} µs; added P95 {delta_p95:.3f} µs; limit {limit} µs: {result}."
            )
if "memory" in summary:
    comparisons = []
    for stage in ["memory", "sensitivity"]:
        for operation in ["fresh", "rebind"]:
            entries = summary.get(stage, {})
            baseline = entries.get(f"glob-{operation}-baseline")
            candidate = entries.get(f"glob-{operation}-candidate")
            capacity = capacities.get("glob-grown" if operation == "rebind" else "glob")
            if not baseline or not candidate or not capacity:
                continue
            peak = (
                candidate["statistics"]["peak_from_fresh_origin_bytes"]["maximum"]
                - baseline["statistics"]["peak_from_fresh_origin_bytes"]["maximum"]
            )
            retained = (
                candidate["statistics"]["retained_from_fresh_origin_bytes"]["maximum"]
                - baseline["statistics"]["retained_from_fresh_origin_bytes"]["maximum"]
            )
            peak_limit = int(
                capacity["rebind_bound" if operation == "rebind" else "fresh_bound"]
            )
            retained_limit = int(capacity["steady_bound"]) * (
                2 if operation == "rebind" else 1
            )
            comparisons.append(peak <= peak_limit and retained <= retained_limit)
    verdict = (
        "INCOMPLETE"
        if len(comparisons) != 4
        else "PASS" if all(comparisons) else "FAIL"
    )
    findings.append(
        f"- Allocator-observation memory comparison: {verdict}. This compares observed peak growth and retained memory with the capacity limits at both sampling intervals; the strict historical peak-allocation criterion remains unverified."
    )
if "io" in summary:
    differences = {}
    windows = 0
    for name, candidate in summary["io"].items():
        if not name.endswith("-candidate"):
            continue
        baseline_name = name[:-9] + "baseline"
        if baseline_name not in summary["io"]:
            continue

        def load_io(run_name):
            return [
                json.loads(line)
                for line in (root / "results/io" / (run_name + ".jsonl"))
                .read_text()
                .splitlines()
                if json.loads(line)["record"] == "planning_io"
            ]

        candidate_rows, baseline_rows = load_io(name), load_io(baseline_name)
        assert len(candidate_rows) == len(baseline_rows)
        fields = list(candidate["statistics"])
        differences[name[:-10]] = {
            field: [c[field] - b[field] for c, b in zip(candidate_rows, baseline_rows)]
            for field in fields
        }
        windows += len(candidate_rows)
    summary["io_differences"] = differences
    equal = all(
        value == 0
        for case in differences.values()
        for values in case.values()
        for value in values
    )
    findings.append(
        f"- {windows} baseline/candidate I/O windows: "
        + (
            "all observed counters are equal."
            if equal
            else "observed counter differences are present; see the I/O difference table."
        )
        + " Coverage is limited to the documented local-filesystem and datasource APIs."
    )
    extra += [
        "## I/O differences",
        "",
        "Values show minimum / maximum candidate-minus-baseline counter differences over matching attempt numbers. These are API request counters, not a count of every process syscall.",
        "",
        "| Workload / operation | Opens | Read calls | Read bytes | Metadata | Directory lists | Files enumerated | Sirius opens | Sirius reads | Sirius requested bytes | Sirius metadata |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, fields in differences.items():
        values = []
        for field in [
            "opens",
            "read_calls",
            "bytes_read",
            "metadata_requests",
            "directory_lists",
            "files_enumerated",
            "sirius_datasource_opens",
            "sirius_datasource_read_calls",
            "sirius_datasource_requested_bytes",
            "sirius_datasource_metadata_requests",
        ]:
            values.append(str(min(fields[field])) + " / " + str(max(fields[field])))
        extra.append("| " + name + " | " + " | ".join(values) + " |")
    extra.append("")
findings.append("")
lines[lines.index("## Environment") : lines.index("## Environment")] = findings
(output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

position = lines.index("## Interpretation and reproduction")
lines[position:position] = extra

(output / "report.md").write_text("\n".join(lines))
print("Wrote", output / "report.md", "and", output / "summary.json")
