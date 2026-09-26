# Scan planning measurements

These tools build equivalent measurement fixtures independently for a baseline and a candidate, collect raw observations, and generate a report. Python 3.11 or newer, Git, Pixi and the project's Linux CUDA build prerequisites are required. Follow the repository's development setup before building. The measurement option is disabled in normal builds.

## Prepare independent checkouts

Choose the baseline before the scan-contract feature and a candidate revision containing this measurement package. Record exact revisions; branch names may move. The bundled baseline patch has been checked for application to `b149e564599bf89b10757a5e4f3920b30dba874d` and `0dca58628f8e485eb0bf132ce062a1e6ccac4b6a`. The original baseline was built and exercised during collection; application to the newer baseline is checked separately and does not establish build or performance equivalence. Other baselines require review of the measurement adapters even if the patch applies.

Set the following variables to your repository URL, chosen revisions and absolute directories:

```bash
REPOSITORY_URL=https://github.com/ye-yi-yy/sirius.git
BASELINE_REF=<baseline-commit>
CANDIDATE_REF=<measurement-branch-or-commit>
BASELINE=/absolute/path/baseline
CANDIDATE=/absolute/path/candidate
FIXTURE=/tmp/scan-fixture
OUTPUT=/absolute/path/collection

git clone "$REPOSITORY_URL" "$BASELINE"
git -C "$BASELINE" checkout --detach "$BASELINE_REF"
git -C "$BASELINE" submodule update --init --recursive
git clone "$REPOSITORY_URL" "$CANDIDATE"
git -C "$CANDIDATE" checkout --detach "$CANDIDATE_REF"
git -C "$CANDIDATE" submodule update --init --recursive

python3 "$CANDIDATE/benchmark/scan-planning/scripts/prepare_baseline.py" \
  "$BASELINE" --expected-ref "$BASELINE_REF" --check-only
python3 "$CANDIDATE/benchmark/scan-planning/scripts/prepare_baseline.py" \
  "$BASELINE" --expected-ref "$BASELINE_REF"
```

The baseline must be clean. The tool verifies HEAD and checks the entire patch before applying it. It does not read the candidate's staged or unstaged diff. Use `--patch` for a separately reviewed adapter for another baseline. Never copy the candidate binary or load a candidate-built plugin into the baseline process.

The candidate already contains its measurement fixture; do not apply the historical candidate patch. Confirm that the chosen candidate revision contains `src/common/planning_measurement.cpp` and `test/cpp/scan/test_scan_planning_measurements.cpp`.

## Build each revision

Run the following for both checkouts. The Makefile target creates the preset link in an initialized DuckDB submodule without building. Configure from the release preset so the first build does not depend on an existing local cache.

```bash
for CHECKOUT in "$BASELINE" "$CANDIDATE"; do
  (
    cd "$CHECKOUT"
    pixi install -e default
    pixi run -e default make duckdb/CMakePresets.json
    pixi run -e default cmake -S duckdb --preset release \
      -DSIRIUS_ENABLE_PLANNING_MEASUREMENTS=ON
    pixi run -e default cmake --build build/release \
      --target sirius_unittest duckdb -j 2
  )
done
```

Use compatible compiler, dependency and CUDA architecture settings for both builds, and inspect any differences in their manifests. On WSL, make the driver visible with `export LD_LIBRARY_PATH=/usr/lib/wsl/lib:${LD_LIBRARY_PATH:-}`. A Linux-only PATH avoids slow Windows-path probing during configuration. Record power settings and competing workloads. Apply any necessary platform workaround equally and document it; it is not part of the feature comparison.

## Create inputs and collect

Choose a new fixture directory with a short absolute path. The generator creates 10,000 hard links to a one-row Parquet file and enforces 96-byte absolute inventory paths. The DuckDB executable must be from a completed build.

```bash
cd "$CANDIDATE"
pixi run -e default python benchmark/scan-planning/scripts/create_fixture.py \
  "$FIXTURE" --duckdb build/release/duckdb

for STAGE in preflight capacity latency memory sensitivity io; do
  python3 benchmark/scan-planning/scripts/run_measurements.py \
    --baseline "$BASELINE" --candidate "$CANDIDATE" \
    --fixture "$FIXTURE" --output "$OUTPUT" --stage "$STAGE" || break
done
```

Inspect preflight failures before continuing. The runner executes binaries through `pixi run -e default` with the candidate as the common working directory. Both use the fixture's integration configuration and local Iceberg data. Baseline capacity capture and candidate-only mismatch injection are not treated as equivalent baseline operations.

Stages run serially. Latency and memory use separate processes. The default is 25 attempts; `--attempts` changes this count. I/O uses five attempts and preflight one. Memory uses a 100 microsecond sampling interval, sensitivity 25 microseconds. Requested intervals do not guarantee coverage of all short-lived peaks. See plan.md for F/L/E/C/I, timing windows and allocator coverage.

The output directory contains an environment/build manifest and `results/<stage>/` logs, extracted JSON and per-stage manifests. New stages verify the executable hashes and input identity recorded for that collection. Changed binaries or inputs require a new output directory. `--resume` skips only validated runs with matching output hashes and stage settings. It does not overwrite failed or partial runs; retain those records and use a new collection directory after correcting the failure. An interrupted inventory operation may leave its extra file behind; inspect the fixture before retrying.

## Additional rebind collection

Use the same runner; separate numbered scripts are unnecessary. Choose workloads, rounds and a new output directory explicitly. The runner records the binaries used for this collection rather than requiring the original author's binaries.

```bash
REBIND_OUTPUT=/absolute/path/rebind-collection
python3 benchmark/scan-planning/scripts/run_measurements.py \
  --baseline "$BASELINE" --candidate "$CANDIDATE" \
  --fixture "$FIXTURE" --output "$REBIND_OUTPUT" \
  --stage rebind-repeat --workloads small4 glob --rounds 4 --attempts 25
```

All first and slow successful attempts are retained. Process order alternates by round. A collection does not by itself isolate scheduling, power or operating-system effects.

## Validate and report

```bash
python3 benchmark/scan-planning/scripts/validate_results.py --input "$OUTPUT"
python3 benchmark/scan-planning/scripts/summarize.py \
  --input "$OUTPUT" --output "$OUTPUT/report"
```

Validation checks record completeness and recorded output hashes without requiring the old executables to remain installed. Add `--verify-binaries` to check those executables too. The summary reads raw data and calculates verdicts; it does not rerun queries or assume that a previous collection passed. Partial collections remain incomplete. The output contains `report.md` and `summary.json`; raw inputs are not overwritten.

To explicitly use another rebind collection, create a selection file:

```json
{
  "small4-rebind": {"directory": "/absolute/path/rebind-collection/results/rebind-repeat", "workload": "small4"},
  "glob-rebind": {"directory": "/absolute/path/rebind-collection/results/rebind-repeat", "workload": "glob"}
}
```

Pass it with `--report-inputs /path/to/selection.json`. Paths may be absolute or relative to `--input`. Use collections from the same binaries and configuration, validate each collection first, and keep their provenance. Selection is explicit; the tool does not choose the best-performing round. All rounds for each selected workload are pooled. Without this option only the main collection is summarized.

Historical results, local configuration, cached Python bytecode and duplicate one-off scripts are not required in the measurement branch. Keep the raw collection and manifests with any published evidence. New code or a different baseline requires new measurements; reformatting old observations does not measure the new revision.

## Check the tools without measuring

```bash
pixi run -e default python benchmark/scan-planning/scripts/test_reproduction_tools.py
```

These checks use temporary repositories and synthetic records. They verify baseline preconditions, output isolation, resume checks and report calculations without running SQL or GPU measurements.
