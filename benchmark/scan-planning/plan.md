# Scan Planning Measurement Plan

## Status

This plan defines the measurement procedure. Each collection records its own versions, build configuration, raw observations and acceptance conclusions. Functional preflight checks are separate from performance measurements. See README.md for the command sequence.

## Build and environment

1. Record the candidate revision and the reviewed working-tree patch. Save the patch and its hash with the reproduction materials under `benchmark/scan-planning/`.
2. Resolve the upstream development revision and its relationship to the candidate. Use the merged development base for an isolated feature comparison. If current upstream development has unrelated changes, identify that difference before selecting the baseline; do not silently compare two different feature sets.
3. Build the equivalent fixture separately against each revision's own DuckDB headers, libraries and submodules. Adapt only unavailable measurement APIs and report those differences.
4. Use Release builds with the same compiler, optimization flags, CUDA architecture list, dependencies, configuration, logging level and DuckDB thread count. The fixture sets DuckDB threads to one so synchronous planning observations remain on the calling thread. Preserve normal Sirius worker configuration.
5. Enable `SIRIUS_ENABLE_PLANNING_MEASUREMENTS` only in the measurement builds. The default build keeps observations disabled. Retain a normal build for checks of instrumentation overhead.
6. Record binary hashes, compiler and allocator versions, build flags, CPU/GPU models, WSL configuration and dataset paths. Apply any required NUMA workaround to both builds in temporary measurement checkouts and record it separately from the feature patch.
7. Run only one benchmark process at a time. Keep the laptop plugged in and use the same power settings. Record competing workloads, CPU throttling and failures; do not remove slow successful samples after seeing the results.

## Workloads

| Workload | Input | Purpose |
|---|---|---|
| Small join | Four persisted and pinned tables, joined by key with a sum | Required small-plan overhead |
| Wider joins | 32 and 128 scans; other positive counts remain supported | Scaling evidence |
| File inventory | 10,000 local Parquet files, each with a fixed 96-byte absolute path | Large bound-inventory overhead |
| Iceberg | Local snapshot fixture with delete files and a fixed snapshot selector | Inventory and manifest behavior |
| Native preparation | Persisted 300,000-row native table under a read-only transaction | Checkpoint acquisition and metadata walk |

The primary memory matrix covers the four-scan query, the 10,000-file inventory and Iceberg. Wider joins provide latency scaling and capacity evidence. The original peak-growth limits apply to the 10,000-file workload; do not invent corresponding peak limits for the wider joins or Iceberg. Sharing, mismatch and exception memory paths use the file inventory.

Verify the input file count and total path bytes before collecting results. Recreate equivalent database state independently for each executable. Dataset generation, loading extensions, checkpointing, pinning and fixture initialization stay outside measurement windows and are identical between builds.

## Windows

| Operation | Start | End and retained state |
|---|---|---|
| Fresh prepare | Immediately before `Connection::Prepare` | Immediately after return, before assertions or output; prepared statement remains alive |
| Prepared rebind | Old prepared statement already exists; immediately before `PendingQuery` | After rebind and pending-query initialization, before fetching data; the old API handle and the active new generation remain alive |
| Execute rebuild | Immediately before resetting the validated candidate, or before rebuilding when no candidate exists | After identity comparison and publication, with the new candidate still alive; subsequent injected failure and cleanup are excluded |
| Equal identity sharing | Same window as successful fresh prepare | Published shared identity retained; the existing identity-sharing unit test checks pointer sharing and separate evidence records |
| Mismatch | Before fresh prepare with a finalize-stage identity mismatch | Prepare has returned its error and unwound its temporary planning state |
| Exception cleanup | Before executing a pending query with a validated candidate | Injected pre-scan failure has returned through execution cleanup |
| Native preparation | Checkpoint-key acquisition and metadata walk have individual boundaries | Each operation finishes before its own timer stops |

For the file-inventory rebind, prepare with 10,000 files, add one fixture-owned hard link outside the window, then force a real DuckDB rebind. Keep the old prepared object alive through the new capture. Remove only the added link after all endpoint readings and statement destruction, so the next attempt starts with the same inventory.

Rebuild and exception cases use the existing pre-scan failure hook to avoid materialization. The rebuild measurements end before that failure. The exception case deliberately includes cleanup and is reported separately from successful planning.

The local Iceberg workload requires SQL replan. Direct `Connection::Prepare` does not install a GPU plan in either revision because no current SQL is available after logical-copy failure. Its fresh GPU planning case therefore uses `Connection::PendingQuery(sql)`, including executor initialization but no data execution. Report this window separately from Prepare. Its rebind starts with the API-prepared CPU generation retained and installs a GPU generation during `PendingQuery`; this does not demonstrate simultaneous retention of two GPU generations for Iceberg. The native and Parquet rebind cases retain the old GPU generation.

Pending rebind includes DuckDB executor initialization, but does not execute or fetch query data. Its retained state includes both generations. Report that state explicitly rather than presenting it as the resident state of a single fresh prepared statement.

## Execution order

1. Build and run observer functionality checks on both binaries. Confirm that a deliberately added read on an already-open file handle changes the observed counters, and that creating a lazy glob alone does not enumerate its contents.
2. Validate each requested workload and operation once. Check successful plan installation, actual rebind/rebuild counters, expected mismatch/failure, statement lifetime and file cleanup. Stop if a requested path did not occur.
3. Collect capacity metrics in separate processes for the original and enlarged inventories. Preserve per-scan F/L/E/C/I values. These runs do not precede timing in the same process.
4. Run latency-only processes: no allocator reader, sampling thread or observed local filesystem.
5. Run memory-only processes: allocator statistics and boundary/periodic sampling enabled; do not use their timings for latency acceptance.
6. Run I/O-only processes: observed DuckDB local filesystem plus Sirius datasource counters; no allocator sampling.
7. Repeat baseline/candidate pairs in alternating order. Collect at least 25 successful attempts per latency workload and operation. Keep first-attempt results, report them separately, and include them in the primary aggregate. Collect at least 25 memory attempts for fresh prepare and rebind, and enough independent I/O attempts to establish repeatability.
8. Run native preparation separately. Normalize before/after comparisons to place the native walk on the same side of the timing boundary.
9. Validate all records before generating tables. Missing backend coverage, an unexpected CPU fallback, an absent phase, an untriggered rebind, a failed read counter control or a failed sample invalidates that run.

## Latency analysis

Preserve one JSON record per attempt with the API duration and inclusive phase duration/call counts:

- Optimizer hook, including logical capture and copying.
- Logical capture.
- Finalize.
- Physical capture.
- Candidate construction, including candidate-side capture.
- Comparison.
- Sharing and publication.
- Execute rebuild.
- Lifecycle lock wait.

Checkpoint acquisition and native walk remain on their own lines. The observer timers are inclusive: do not sum a parent with its child phases. Calculate any combined duration per attempt before calculating percentiles. Never add independently calculated percentiles.

Report complete Prepare/PendingQuery API time as well as the Sirius hook/finalize breakdown. This prevents initial capture or surrounding cleanup from disappearing from the report. For the original planning criterion, compare complete planning windows with native walk removed on both builds; also show raw API and walk-inclusive totals. Keep lock wait visible and do not silently discard it from the primary numbers.

Use median and nearest-rank P95 over at least 20 attempts. Report baseline, candidate and their difference for each statistic; a percentile of paired deltas is a separate quantity and must be labeled separately. The four-scan limit is 100 microseconds added; the 10,000-file limit is 5 milliseconds added. Report pass/fail for both required statistics from the chosen complete window. Wider joins have scaling results without an invented threshold.

An execution API duration that includes the injected failure is a diagnostic total. Use the scoped execute-rebuild duration for rebuild acceptance. Missing feature-only phases in the baseline are absent work, not a missing measurement of the baseline's total planning path.

## Memory analysis

The current build uses the system allocator for ordinary C++ allocations and a separately prefixed jemalloc for DuckDB allocator calls. Observe both when present.

- System glibc: `malloc_info` arena in-use estimate plus glibc mmap allocations. This includes allocator accounting effects and is not exact requested allocation size. The implementation does not depend on `mallinfo2` or its headers.
- DuckDB jemalloc: refresh `epoch`, then read `stats.allocated` and `stats.active`. Active pages are supplemental and are not added to allocated bytes.
- Optional process jemalloc: explicitly select the jemalloc backend only when the process malloc provider matches the queried control API. Report this as a different allocator configuration and apply it equally to both builds. Do not mix its latency results with the normal allocator runs.

Allocator readers and the sampling thread are initialized before the baseline reading. The sampling interval defaults to 100 microseconds. Read at window endpoints and phase boundaries as well as periodically. Report actual sample count and maximum observed gap. Repeat memory runs at a shorter interval as a sensitivity check; a shorter interval still does not establish an exact historical peak.

Required fields:

- `live_before_bytes`, `peak_live_bytes`, `live_after_bytes`.
- `peak_added_bytes = peak_live_bytes - live_before_bytes`.
- Signed `retained_delta_bytes = live_after_bytes - live_before_bytes`.
- System and DuckDB components at the same selected peak sample, with backing/active bytes separate.
- Fresh origin and peak/retained change relative to that origin for rebind. The immediate pre-rebind baseline already contains the old generation and must not be used to exclude it from a total-generation bound.
- Backend presence, sampling interval, sample count and maximum gap.

Compare per-revision growth with the paired baseline. Report the raw process-level values and the differential values; allocator caching and unrelated activity mean the differential is not a direct attribution of every byte to one component.

### Capacity metric definitions

`read_view_capture_metrics` reports the following terms for one captured scan view. F is a count; L, E, C and I are byte capacities.

| Symbol | Metric field | Meaning and counting rule |
|---|---|---|
| F | `file_count` | Number of file entries in the bound inventory used by this capture. This is not the number of filesystem calls or directory entries visited. Native-table and stream identities can have no file inventory and therefore report zero. |
| L | `canonical_capacity` | Capacity of the canonical text encoding the bound read identity, including space for the terminating byte: `fingerprint.canonical.capacity() + 1`. It is not the size of the entire identity object. |
| E | `evidence_capacity` | Capacity of one captured side's file-evidence record: file-size and last-modified arrays, their presence flags, the etag string-object array, and the reported capacity of its etag strings. It is not the sum of all logical, physical and candidate records. |
| C | `transient_path_capacity` | Capacity of temporary owned path storage used during capture: the `owned_paths` and `owned_files` container buffers plus their reported path-string capacities. It excludes the contents of the input files. A borrowed inventory can avoid these copies. |
| I | `sort_index_capacity` | Capacity in bytes of the temporary index vector used to order paths or align file evidence: `order.capacity() * sizeof(size_t)`. When capture records multiple sorting stages, it retains the largest recorded value. This is not the capacity of every registry or ingestible index. |

Container capacities include reserved space, rather than only populated elements. String counters use `capacity() + 1`; small-string storage can be inline. These are the implementation's reported capacity metrics, not allocator-observed live bytes or a complete accounting of metadata allocations. In particular, I does not stand for all added indexes; policy and ingestible-index allocations are covered by the allocator observation within its stated coverage.

Report the terms per scan and identify the inventory and binding generation. For the file-addition rebind, retain both the 10,000-file and 10,001-file measurements. Do not substitute one side's E for the sum of all surviving evidence records. When sizes differ across sides or generations, use their actual terms or conservative maxima as required by the bound.

Use measured F/L/E/C/I values to display the existing limits:

- Fresh: `3L + 2E + C + I + 256 KiB`.
- Rebind: `5(L + E) + C + I + 256 KiB`, retaining the old generation in the numerator; use the actual differing terms or conservative maxima for the 10,000/10,001 inventories.
- Retained: `L + 2E + 256 KiB` per scan per surviving binding generation.

Capacity metrics remain supplemental; they are not used to reconstruct an assumed allocation peak. Whole-allocator sampling includes policy and ingestible-index allocations within the observed allocators without a symbol-name attribution list. It cannot identify those components separately.

A sampled peak, even below the bound, does not prove the strict historical live-allocation peak criterion. The report must say `sampled_allocator_observed` and must not claim exact peak verification. Thread caches, allocator accounting, non-atomic sampling across allocators, and short-lived allocations between samples remain limitations. A stricter acceptance conclusion would require additional evidence or an explicit change of the acceptance standard.

## I/O analysis

Run with identical logging settings and inputs. Keep lazy inventory initialization inside the measured window. Compare these counters separately:

- DuckDB local filesystem opens, read requests, successfully returned read bytes, metadata API requests, directory-list requests and directory entries delivered.
- Sirius datasource opens, read requests, requested bytes and metadata access requests. Requests can be cache hits; requested bytes are not confirmed physical bytes read.

Install the local observer before database creation so handles opened during setup remain observable when read inside a later window. Preserve lazy glob behavior. The existing-handle read control covers sequential and positional reads. Sirius counters are process-wide to include worker-thread requests.

These counters describe the covered APIs. They do not count every operating-system syscall, diagnostic-log write, HTTP protocol request, direct backend call or arbitrary extension-private filesystem. The local Iceberg fixture can establish whether its local manifest reads increased. Do not turn zero observed API growth into a claim of zero unobserved network or process-wide I/O. If the acceptance claim needs those paths, extend the corresponding datasource/backend observation and rerun its negative control before collecting that evidence.

## Command templates

Run commands through the activated environment in each revision's own checkout. Save stdout, stderr and exit status under `benchmark/scan-planning/results/` with the corresponding run manifest. The following are templates for the approved measurement stage, not commands already executed:

```bash
pixi run -e default cmake -S duckdb -B build/release \
  -DSIRIUS_ENABLE_PLANNING_MEASUREMENTS=ON
pixi run -e default cmake --build build/release --target sirius_unittest -j 6

SCAN_PLANNING_MODE=latency SCAN_PLANNING_OPERATION=fresh \
SCAN_PLANNING_SCENARIO=small SCAN_PLANNING_SCANS=4 SCAN_PLANNING_ATTEMPTS=25 \
pixi run -e default build/release/extension/sirius/test/cpp/sirius_unittest \
  '[scan_planning_measurements]'

SCAN_PLANNING_MODE=memory SCAN_PLANNING_OPERATION=rebind \
SCAN_PLANNING_SCENARIO=glob SCAN_PLANNING_ATTEMPTS=25 \
SCAN_PLANNING_GLOB="$DATASET_GLOB" SCAN_PLANNING_SOURCE="$SOURCE_PARQUET" \
SCAN_PLANNING_ADD_PATH="$ADDED_PARQUET" \
SCAN_PLANNING_ALLOCATOR=glibc SCAN_PLANNING_SAMPLE_US=100 \
pixi run -e default build/release/extension/sirius/test/cpp/sirius_unittest \
  '[scan_planning_measurements]'

SCAN_PLANNING_MODE=io SCAN_PLANNING_OPERATION=fresh \
SCAN_PLANNING_SCENARIO=iceberg SCAN_PLANNING_ICEBERG="$ICEBERG_FIXTURE" \
SCAN_PLANNING_ATTEMPTS=25 \
pixi run -e default build/release/extension/sirius/test/cpp/sirius_unittest \
  '[scan_planning_measurements]'
```

Repeat the selected workloads with `fresh`, `rebind`, `rebuild`, `sharing`, `mismatch` and `exception` as applicable. Use `SCAN_PLANNING_METRICS=1` in a separate process for capacities. Run `SCAN_PLANNING_SCENARIO=native` in latency mode for checkpoint acquisition and walk.

## Reproduction materials

Keep the measurement code, scripts, raw JSON, logs, manifests and report in the repository so the measurement can be reproduced from a dedicated branch. Use `benchmark/scan-planning/` for the reproduction package:

| Path | Contents |
|---|---|
| `plan.md` | Workloads, measurement windows, metric definitions and execution steps |
| `scripts/` | Dataset generation, baseline/candidate build preparation, run orchestration, JSON collection and report generation |
| `patches/` | Exact baseline fixture adaptations and any environment-specific build adaptations, with hashes and application instructions |
| `results/` | Unmodified per-attempt JSON, stdout, stderr, exit status and derived summaries, separated by revision, workload, operation and measurement mode |
| `manifest.json` | Source revisions, patch and binary hashes, build commands, dependency versions, allocator configuration, environment and input-data description |
| `report.md` | Environment, workload/window definitions, separate latency/memory/I/O tables, first-attempt and all-attempt statistics, applicable bounds and coverage limits |

The measurement implementation remains in its source and test locations. Reference those paths from the manifest instead of duplicating the implementation. Preserve the dataset-generation seed, path-length rules, input checksums and exact commands so the same data can be reconstructed. Keep raw observations separate from calculated summaries and retain failed-run logs with their failure reasons. Results and run manifests are populated when measurements are executed.
