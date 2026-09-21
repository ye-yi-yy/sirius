# Commit C read-view evidence

This note records the A1 eligibility matrix and the indicative T6 measurement required after
commit C. It is not the final AC 9 measurement: the R18 plan requires that measurement after
commit D.

Commit C remains **owner-gated for merge**: R18 requires recorded approval on #1796 for the
multi-scan Iceberg and selector-unproven compatibility changes. Local test results do not
replace that approval. This document is the local matrix handoff for the PR description;
publication to the PR and the approval record are separate delivery steps.

## A1 eligibility matrix

`Hooks=on` means the optimizer-extension hook captured the original logical bindings. `Replan,
hooks=on` is the execution path where copying the saved logical plan fails and Sirius falls back
to SQL replanning. `Replan, hooks=off` is the path exercised with the optimizer extension
disabled. `copy` describes provenance from the hook original, not the last use of `Copy()`.
A hooks-off SQL-replanned template can be retained and successfully copied during execution
rebuild; that candidate still has `replan` provenance and uses single-scan correspondence.

The outcomes below were observed by the T2 integration cases, including their execution
counters. "Error" means a classified read-view error; S3 always applies its source veto before
considering CPU replay.

| Scans | Candidate | Hooks | Source | Fallback on | Fallback off | Evidence |
|---|---|---:|---|---|---|---|
| single | copy | on | local | GPU | GPU | ordinary matching scan |
| multi | copy | on | local | GPU | GPU | matching join; table-index correspondence |
| single | copy | on | S3 | GPU | GPU | matching S3 view |
| multi | copy | on | S3 | GPU | GPU | matching S3 self-join |
| single/multi | copy | off | local/S3 | N/A | N/A | no hook-original copy provenance; copied SQL-replan templates stay replan |
| single | replan | on | local | GPU | GPU | execution-copy failure retains single-scan correspondence |
| multi | replan | on | local | CPU | error | `reason=no_correspondence` |
| single | replan | on | S3 | GPU | GPU | forced execution-copy failure, `execution_rebuilds +1`, no mismatch |
| multi | replan | on | S3 | error | error | execution-copy failure, S3 veto and `none/no_correspondence`, no CPU replay |
| single | replan | off | local | GPU | GPU | single ordinary scan identity correspondence |
| multi | replan | off | local | CPU | error | `reason=no_correspondence` |
| single | replan | off | S3 | GPU | GPU | single ordinary S3 scan identity correspondence |
| multi | replan | off | S3 | error | error | source veto plus `reason=no_correspondence` |
| single | copied replan template | off | local/S3 | GPU | GPU | pin-epoch rebuild, no mismatch or runtime fallback |
| any mismatch | copy/replan | any | local | CPU | error | `binding_mismatch`, `fingerprint_mismatch`, or `selector_unproven` |
| any mismatch | copy/replan | any | S3 | error | error | S3 CPU replay is forbidden |

Selector-outside-bind sources add the following compatibility rows. Commit C's Iceberg path is
a SQL replan, not a copy candidate.

| Scans | Candidate | Hooks/proof | Source | Fallback on | Fallback off | Evidence |
|---|---|---|---|---|---|---|
| single | replan | hook selector equals candidate | local/S3 | GPU | GPU | pinned `iceberg_scan` with deletes |
| single | replan | hook absent or selector differs | local | CPU | error | `reason=selector_unproven` |
| single | replan | hook absent or selector differs | S3 | error | error | selector error under source veto |
| multi | replan | any | local | CPU | error | `reason=no_correspondence` |
| multi | replan | any | S3 | error | error | source veto plus `reason=no_correspondence` |

The observed read-view suite after the performance work was:

```text
[transparent][read_view]
25 cases, 646 assertions passed
```

It covers matching copy candidates, copy mismatches, SQL replans with hooks off, execution copy
failure, single/multi scans, fallback on/off, selector proof and drift, local/S3 source policy,
and a prepared glob whose inventory changes between executions.

## Review corrections (2026-09-22)

- Removed the load-time catalog snapshot that granted trust to pre-existing replacements.
  Static builds use the independent factories; loadable builds resolve DuckDB's exported
  native/Parquet factories through the host symbol namespace, using the pinned C++ ABI. A
  mutable catalog is only the second check, never the reference. Missing factory exports
  leave a source unverified. The initial fix still depended on the test's `RTLD_GLOBAL`
  import; the default-host follow-up below corrects that coverage gap.
- Preserved the retained logical template's `candidate_origin` through successful copies.
  A failed copy followed by SQL binding still uses `replan`. Comparison and replay stay at
  their existing boundaries.
- Restored the original Python/loadable Parquet views and host pins. The native-table variant
  is an additional parameterized run in a separate process; both require fallback off and
  COUNT-column/COUNT-star fusion diagnostics.
- Added genuinely cold subprocesses that register a same-signature replacement before the
  first Sirius load, for static and dynamic loading. They bypass only the unittest's shared
  environment startup, not production verification. They check a real bound scan, planner
  rejection, query rejection, and that the authentic source can still be verified after restore.
- Added a hooks-off join-flip preflight with asymmetric Parquet inputs and bind-time `nextval`.
  The two optimized plans have identical path traversal order but exchanged table indices;
  their CPU results are 10 and 100. Normal SQL must decline the candidate and replay the
  original result 10, or report `none/no_correspondence` with fallback off. Sequence counts
  pin the two binds per statement. No new production injection or admission bypass is used.
- Filled execution-copy-failure S3 cells and the hooks-off copied-template rebuild cells.
  Counters assert installation, execution rebuild, mismatch, and absence of forbidden replay.

The initial new tests reproduced pre-load replacement admission and a spurious mismatch/CPU
replay after copying a replan template. Removing the catalog snapshot first made the restored
Parquet dynamic-load test fail with unverified callbacks; resolving independent host factories
made that same source path pass. Test-fixture failures (a missing optional aggregate extension,
and `ExtractPlan`'s read-only automatic transaction with bind-time `nextval`) were corrected
in test setup and are not counted as product RED evidence.

Initial review-fix validation on the Commit C branch, before the default-host follow-up:

```bash
pixi run -e default cmake --build build/release -j 8
pixi run -e default env SIRIUS_TEST_S3_AUTO=1 SIRIUS_TEST_S3_STRICT=1 \
  build/release/extension/sirius/test/cpp/sirius_unittest \
  '[scan][contracts],[transparent][read_view],[dynamic_load]'
pixi run -e default build/release/extension/sirius/test/cpp/sirius_unittest
pixi run -e default make s3-test
pixi run -e default pre-commit run --all-files
```

- Release build: passed.
- Targeted contracts/read-view/dynamic-load selection: 43 cases / 2,598 assertions passed.
  Six process-isolated registry children additionally pass 37, 43, 25, 31, 16 and 17 assertions.
  S3 strict mode was enabled, so the S3 cells are executed, not missing-fixture skips.
- Full C++ suite: 3,417 cases / 33,868,004 assertions passed, exit 0. This default invocation
  permits optional integration-fixture skips; the strict S3 gate is reported separately.
- Standard S3 gate: 96 cases / 31,662 assertions passed, exit 0, with automatic MinIO fixtures
  and strict missing-fixture handling (`make s3-test`). The separate large-data gate was not
  rerun for this review fix; these results do not close all final R1/AC10 requirements.
- Full-repository pre-commit: passed.

Scope audit: six production files contain only independent registry-reference resolution,
removal of the unsafe initializer, and template-origin storage/forwarding. Six test/evidence
files restore coverage, add adversarial paths, and report observations. No DuckDB/submodule
changes, MinIO/pixi edits, decoder/split changes, new product test options, or Commit D code.
The `.ssw` execution plan is maintained outside the product repository.

### Default Python host follow-up

Comparing the fixture to commit B exposed another changed precondition: C had added
`sys.setdlopenflags(RTLD_NOW | RTLD_GLOBAL)` before importing DuckDB. Re-running the same
Parquet fixture with ordinary Python import failed at `LOAD Sirius` with an unresolved
`duckdb::ExtensionHelper::LoadExtension` symbol (exit 1); global import passed (exit 0).
The earlier green suite therefore did not prove restoration of the original loading model.

The registry now registers Parquet statically in its private reference database, avoiding that
unlinked host-loader symbol. For loadable builds, the host system catalog object's dynamic type
locates the already-loaded DuckDB module; factory symbols must belong to that same module.
Neither mutable table-function callbacks nor global symbol visibility establish trust. The
code does not promote the host to global visibility or load another library. Missing references
still fail closed, and the callback/overload checks are unchanged.

The resolver uses `dladdr1`'s existing `link_map` name, with the main handle for an empty name.
A cold-path probe showed that reopening an executable's disk path with `RTLD_NOLOAD` still
causes an `open`; using its loader record avoids that probe. A focused `strace` check of the
main-handle and local-DuckDB lookups recorded no file syscalls between its markers. This is
not a substitute for T6. The first full-suite run was interrupted to rebuild this final version.

The dynamic-load regression covers default local and explicit global host visibility, each with
Parquet views/pins and an additional native-table variant in separate processes. Default import
is not modified. Every variant retains fallback-off, row-result and GPU COUNT-fusion assertions.
Final follow-up validation reran the same commands listed above:

- Release build: passed; no unresolved DuckDB symbols in the loadable extension's dynamic
  symbol table (`nm -D --undefined-only ... | c++filt`).
- Strict-S3 contracts/read-view/dynamic-load selection: 43 cases / 2,624 assertions passed,
  plus the six registry children / 169 assertions. All four source/host-visibility variants ran.
- Full C++ suite: 3,417 cases / 33,868,029 assertions passed, exit 0. Optional-fixture skips
  remain permitted by this default command; strict S3 is separate.
- Standard `make s3-test`: 96 cases / 31,662 assertions passed, exit 0, with real MinIO and
  strict fixture handling.
- Full-repository pre-commit: passed.

T6 and `s3-test-large` were not rerun. The owner approval and PR-publication delivery steps
remain open; neither passing tests nor this host-compatibility fix closes those gates.

## T6 indicative measurement

These are the historical performance-recovery measurements of `00060928`, before the review
corrections above; they are not a fresh measurement of the review-fix worktree.

### Method

- Baseline: `b2620e3839e7e4997ae2ea56019a3caf5f0e67f3` (commit B).
- Candidate: commit C worktree after the performance recovery described below.
- Build: release, same checkout dependencies and machine.
- Machine: AMD Ryzen 9 5950X (32 logical CPUs), NVIDIA RTX 5060 8 GiB.
- Attempts: 50 for the small and 10,000-file cells; the unchanged Iceberg report retains its
  earlier 25 attempts. All use nearest-rank p95.
- Finalize: the complete `Connection::Prepare` attempt.
- Execution rebuild: a prepared statement with an injected pin-registry epoch change; a test
  hook stops after successful plan generation and read-view comparison, before GPU data work.
- Lock wait: zero contending holders in this single-threaded run. Uncontended acquisition is
  included in the whole-attempt numbers; there is no separately instrumented nonzero wait.
- The local glob contains exactly 10,000 files. Every complete path is 96 bytes; total path
  bytes are 960,000.
- Planning I/O was counted between explicit begin/end markers with `strace -f -e
  trace=%file,write`, including the first lazy inventory initialization.
- The Iceberg case reads snapshot `9400000000000002` of
  `iceberg_snapshot_deletes`, which contains a positional delete file.

### Latency

All values are microseconds. `Added` is candidate minus baseline.

| Scenario | Phase | Baseline median / p95 | Candidate median / p95 | Added median / p95 | R18 limit |
|---|---|---:|---:|---:|---:|
| four pinned native scans | finalize | 459.529 / 474.278 | 474.559 / 498.788 | +15.030 / +24.510 | +100 |
| four pinned native scans | execution rebuild | 698.547 / 715.916 | 726.068 / 814.184 | +27.521 / +98.268 | +100 |
| 10,000-file local glob | finalize | 116876 / 122616 | 107290 / 114349 | -9586 / -8267 | +5000 |
| 10,000-file local glob | execution rebuild | 171977 / 193860 | 159047 / 196637 | -12930 / +2777 | +5000 |
| Iceberg snapshot with deletes | finalize | 1233.630 / 1732.160 | 1037.330 / 1780.220 | -196.300 / +48.060 | report only |
| Iceberg snapshot with deletes | execution rebuild | 9180.140 / 10153.500 | 9724.360 / 11057.600 | +544.220 / +904.100 | report only |

The small-query and 10,000-file targets pass for both median and p95 in this indicative commit-C
run. The main recovered cost was local ETag extraction: the bind value is a raw BLOB, and reading
its bytes directly avoids string-cast escaping while preserving the same R18 evidence field.
Canonical construction now avoids temporary decimal strings and redundant path passes, and
comparison uses hash buckets only as an accelerator while retaining collision-safe full canonical
equality. Commit D must still repeat AC 9 after adding its lease and native-preparation work.

### Planning I/O

| Scenario | Baseline file calls | Candidate file calls | Added |
|---|---:|---:|---:|
| 10,000-file local glob, finalize | 50,004 | 50,004 | 0 |
| Iceberg with deletes, finalize | 36 | 36 | 0 |

For Iceberg, both traces contain the same metadata JSON, manifest-list, data-manifest and
delete-manifest opens. The bound `GetAllFiles()` capture adds no manifest I/O. There was no
network I/O in either local fixture.

### Capacity and RSS

The production capture metrics for the 10,000-file fixture report:

| Term | Bytes |
|---|---:|
| F | 10,000 files |
| L, canonical capacity | 1,000,197 |
| E, one evidence side including local etags | 830,000 |
| C, transient path-copy peak | 1,290,000 |
| I, sort-index capacity | 80,000 |

The actual modeled simultaneous allocations and R18 bounds are:

| State | Actual modeled bytes | Bound bytes | Result |
|---|---:|---:|---|
| fresh finalize: `3L + 2E + C + I` | 6,030,591 | 6,292,735 including 256 KiB | pass |
| rebind: `4L + 4E + C + I` | 8,690,788 | 10,783,129 (`5(L+E)+C+I+256KiB`) | pass |
| retained `PREPARE`, no `EXECUTE`: `L + 2E` | 2,660,197 | 2,922,341 including 256 KiB | pass |

In the paired 50-attempt glob run, candidate-minus-baseline retained RSS growth was 892 KiB;
whole-process RSS includes DuckDB's pre-existing bind, allocator, and plan allocations, while the
table above isolates the instrumented read-view capacities. The retained candidate side record
carries no evidence array; there is no third E record.

This evidence is indicative only. Commit D must rerun the same matrix with key acquisition and
the native row-group walk reported separately, as required by R18.
