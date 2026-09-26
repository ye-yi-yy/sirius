"""Check completed records without editing or filtering observations."""

import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--input", type=Path, required=True)
parser.add_argument(
    "--verify-binaries",
    action="store_true",
    help="Also require the recorded executables to still exist and match",
)
args = parser.parse_args()
root = args.input.resolve()
checks = {}
for stage in ["latency", "memory", "sensitivity", "io", "rebind-repeat"]:
    if not (root / "results" / stage / "manifest.json").exists():
        continue
    manifest = json.loads((root / "results" / stage / "manifest.json").read_text())
    assert "completed_utc" in manifest, stage
    count = 0
    for run in manifest["runs"]:
        assert run["exit"] == 0, run["name"]
        for suffix, expected_hash in run.get("sha256", {}).items():
            path = root / "results" / stage / (run["name"] + suffix)
            with path.open("rb") as stream:
                assert (
                    hashlib.file_digest(stream, "sha256").hexdigest() == expected_hash
                ), path
        records = [
            json.loads(line)
            for line in (root / "results" / stage / (run["name"] + ".jsonl"))
            .read_text()
            .splitlines()
        ]
        expected = int(run["environment"]["SCAN_PLANNING_ATTEMPTS"])
        main_kind = (
            "native_preparation" if run["scenario"] == "native" else "planning_attempt"
        )
        rows = [row for row in records if row["record"] == main_kind]
        assert sorted(row["attempt"] for row in rows) == list(range(expected)), run[
            "name"
        ]
        for row in rows:
            if main_kind == "planning_attempt":
                assert row["timing_valid"] == (stage in ["latency", "rebind-repeat"])
                assert (
                    row["mode"] == run["mode"] and row["operation"] == run["operation"]
                )
                assert row["api_total_us"] >= 0
        if stage in ["memory", "sensitivity"]:
            memory = [row for row in records if row["record"] == "planning_memory"]
            assert sorted(row["attempt"] for row in memory) == list(range(expected))
            for row in memory:
                before, peak, after = (
                    row[key]
                    for key in [
                        "live_before_bytes",
                        "peak_live_bytes",
                        "live_after_bytes",
                    ]
                )
                assert peak >= max(before, after)
                assert row["peak_added_bytes"] == peak - before
                assert row["retained_delta_bytes"] == after - before
                assert (
                    row["peak_from_fresh_origin_bytes"]
                    == peak - row["fresh_origin_bytes"]
                )
                assert (
                    row["retained_from_fresh_origin_bytes"]
                    == after - row["fresh_origin_bytes"]
                )
                assert row["system_at_peak_bytes"] + row["duckdb_at_peak_bytes"] == peak
                assert (
                    row["duckdb_jemalloc_observed"] and not row["exact_peak_verified"]
                )
                assert row["samples"] >= 2
        if stage == "io":
            io = [row for row in records if row["record"] == "planning_io"]
            assert sorted(row["attempt"] for row in io) == list(range(expected))
        count += expected
    checks[stage] = {"runs": len(manifest["runs"]), "attempts": count, "valid": True}
if not checks:
    raise RuntimeError("No completed measurement stages found")
if args.verify_binaries:
    manifest = json.loads((root / "manifest.json").read_text())
    for label, build in manifest["builds"].items():
        with Path(build["binary"]).open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        assert actual == build["binary_sha256"], label
    checks["binary_hashes_match"] = True
print(json.dumps(checks, indent=2))
