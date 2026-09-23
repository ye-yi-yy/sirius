# Scan Contracts

Scan contracts connect DuckDB's bound inputs to the GPU scans and splits that
consume them. Before transparent execution installs a GPU plan, Sirius verifies
the scan functions and checks that the candidate reads the same bound data as
DuckDB's original plan. During execution, contracts check split ownership, and
native scans hold a checkpoint lease while reading the database layout.

The contracts describe input identity and execution ownership. Format support,
row visibility, filtering and decoding are enforced by the scan implementations
described in [Scan](scan.md).

## Architecture

The main records have separate responsibilities:

| Record | Responsibility |
|---|---|
| `bound_read_identity` | Immutable source identity, bound schema, options and table, file or stream identity |
| `bound_read_view` | A captured identity with its own file evidence, selector evidence and statement-scoped provider references |
| `bound_table_scan` | The read view plus output columns, predicates and materializer requirements for one scan |
| `read_view_registry` | Plan-owned scan contracts and their binding-comparison results |
| `split_materializer_certificate` | The producing scan contract and materializer profile for one split slice |
| `split_dependencies` | Footer, datasource or checkpoint information retained for materialization |

The execution path is:

1. Verify each source function and capture the original logical binding before
   copying the plan.
2. Capture DuckDB's physical scan bindings during finalize, and capture each GPU
   candidate from the binding it actually lowers.
3. Compare the bindings before installing the GPU operator.
4. Prepare scans inside the execution window, acquiring native checkpoint leases
   before inspecting row-group layouts.
5. Check fresh splits against their consuming scan contracts before materialization.
6. Drain scan work and release leases before any permitted CPU replay.

## Source verification

The registry recognizes `seq_scan`, `parquet_scan`, `read_parquet`,
`sirius_read_parquet`, `iceberg_scan` and `sirius_stream_source`. Each entry defines
the expected bind-data type, lowering function, dynamic-filter mode and default
CPU replay permission.

Admission requires a matching bind-data type, an independently trusted function
definition and a matching current catalog registration. The comparison covers
the overload signature, scan and initialization callbacks, binding and
serialization callbacks, optimizer callbacks, and scan behavior flags.
Presentation and profiling callbacks do not determine admission. A matching
function name or bind-data type alone is insufficient.

Native and Parquet definitions come from DuckDB's factories. In a loadable
extension, Sirius resolves those factories from the host DuckDB module.
Sirius-owned functions use the same factories as their registration.

Iceberg's trusted definitions come from the already-loaded extension's registration
entry point, run in a private CPU reference catalog. This initialization occurs
when Iceberg loads, or when Sirius loads if Iceberg is already present. It adds
extension-load work; scan lookup neither creates the reference database nor
retries its initialization. Missing trusted definitions cause GPU admission to
decline.

The caller's mutable catalog only confirms registration. Replacing a function
before its first lookup cannot make the replacement trusted.

## Bound read identity

Identity includes the verified source, bound column names and types, relevant
reader options, and the source-specific input:

| Source | Input identity |
|---|---|
| DuckDB-native table | Catalog and table identity, schema and database path |
| Parquet, Sirius S3 Parquet and Iceberg | Bound file paths as a sorted multiset |
| Streaming source | Stream identity and bound schema |

Duplicate paths are significant. Projection, filters, optimizer state,
transaction state and provider pointers are outside read identity. File pruning
that changes the bound inventory does change identity.

The canonical representation uses typed, length-prefixed values. Equality compares
the full canonical content; the hash only selects a comparison bucket. A hash
collision cannot establish equality.

Parquet capture exports the nested format options through the trusted Parquet
serializer into `ParquetOptionsSerialization`. It encodes the option values
deterministically, without another footer read or a call to Iceberg's outer
serializer. Sirius S3 Parquet retains the bound names and types obtained during
binding.

### File and selector evidence

Original captures retain available file sizes, modification times and tags from
the bound inventory. Capture does not issue stat, HEAD or footer requests to fill
missing evidence. These observations describe what was available at capture
time; they do not participate in identity equality.

GPU candidate capture uses the resolved paths already needed for lowering and
does not allocate another file-evidence array. After a successful comparison,
equal captures share immutable identity storage. Evidence and provider references
remain attached to their own captures.

Iceberg also requires the evaluated logical selector parameters because its
bound snapshot selector is not exposed symmetrically through `MultiFileBindData`.
This selector evidence is checked separately from canonical identity.

Path encoding avoids retaining another inventory beside the canonical content.
Already sorted inventories need no sorting index during capture. Parquet keeps
an index from scan file order to evidence order, preserving file order and
duplicate entries without copying the path strings.

## Plan correspondence

Finalize compares the multiset of physical original identities with the GPU
candidate identities. It also establishes which logical scan each candidate
represents:

| Candidate origin | Required correspondence |
|---|---|
| Plan copy | One-to-one matching by `table_index`, with equal identities and any required selector evidence |
| SQL replan with one scan | Equal physical identity; Iceberg also requires matching selector evidence from the original logical capture |
| SQL replan with multiple scans | Declined because traversal order cannot establish correspondence |
| Iceberg without matching selector evidence | Declined |

Logical captures belong to the same planning generation as the comparison.
Missing identity, changed inventory, changed schema or options, and unproven
correspondence prevent installation of the GPU candidate.

The first execution can reuse the validated GPU plan. If a pin-registry change
requires an execution-time rebuild, the rebuilt candidate is checked before GPU
work. Repeated execution of a prepared statement rebinds and finalizes again.

For example, if a glob resolves to a different file set during SQL replanning,
the new candidate is rejected even if its output types are unchanged. CPU replay,
when allowed, uses DuckDB's preserved physical plan.

## Split ownership

Each GPU scan and streaming source has a `bound_table_scan` in the plan's
registry. Its contract handle is allocated from a process-wide monotonic counter
and is never reused. Two scans can share read identity while retaining separate
contracts for different projections or predicates.

Fresh splits carry their consuming contract handle and per-slice materializer
certificates. Parquet coalescing preserves every slice's certificate and
dependencies. Native splits retain their range certificates, including the
empty or fully pruned completion case. Insert-delta splits receive the consuming
scan's handle when cut from a shared delta job.

Before materialization, the scan validates both the split handle and its
certificate handles. A split from another scan is rejected even when both scans
read the same files. Resident cached batches use pin identity and MVCC guards.
Streaming sources have scan contracts but do not produce fresh scan splits.

An eligibility certificate begins as `not_evaluated` with evidence scope `none`.
Successful plan comparison publishes `supported` with scope
`binding_correspondence`. This result proves binding correspondence; individual
scan paths still enforce format support and data visibility.

## Native checkpoint lease

A native ingestible's constructor does not walk row groups or hold a checkpoint
key. During execution preparation, `sirius_scan_manager` acquires a shared
checkpoint key for each database read by a native scan, then calls
`ensure_metadata_prepared()`. Under that key, the ingestible records the checkpoint
iteration, captures the row-group layout and publishes metadata readiness.
Decode rechecks the iteration before staging bytes.

The scan manager owns the keys alongside those used for pinned scans. They remain
held for the execution window and are released by scan-manager reset after scan
work has drained. A retained plan, an idle prepared statement and the interval
between prepared executions hold no execution lease.

Success, cancellation and execution errors all require window cleanup before CPU
replay. Replay uses the completed window's captured release result: a released
lease permits the remaining replay checks; a window that never started preserves
the entry point's normal policy. Failure to begin or clean up an entered window
prevents replay. Failed cleanup can retain keys until later cleanup or scan-manager
destruction.

A non-forced `CHECKPOINT` on the same database fails while a shared key is held.
`FORCE CHECKPOINT` waits for release and, while waiting, can also delay new
non-read-only transactions. Long GPU queries can therefore delay checkpoints and
writes. A checkpoint completed before preparation is accepted because the layout
is captured under the subsequently acquired key.

`pin_table` acquires its execution window's key before the native metadata walk
and retains it through materialization. Pin-served queries also check the stored
checkpoint iteration and their transaction's visibility.

A refusal discovered by the deferred metadata walk occurs during execution
preparation. It can trigger permitted CPU replay; with fallback disabled it is an
execution error. Prepared executions retry preparation independently.

### Internal metadata queries

Iceberg metadata queries use `SiriusContext::open_internal_connection`. The wrapper
installs `InternalQueryGuard` before starting a read-only transaction and accepts
one SELECT or SET statement at a time. It prevents transaction-control escapes
and recursive GPU admission.

The internal transaction-start guard rejects a non-read-only start while the
current execution window holds checkpoint keys. Planning metadata queries
complete before native execution leases are acquired.

## CPU replay policy

Transparent execution derives replay permission across the whole original plan:

| Source or discovery result | Source permission |
|---|---|
| Local files and DuckDB-native tables | Permit CPU replay |
| Sirius-owned S3 data | Forbid CPU replay |
| Streaming sources | Forbid CPU replay |
| Incomplete source discovery | Forbid CPU replay |

The policy inspects bound MultiFile paths even when a function is not registered
for GPU lowering, so an S3 source hidden behind a view still vetoes replay.
Unclassified sources retain the entry point's existing policy. SQL text provides
an additional S3 signal when a complete bound plan is unavailable.

Source permission is only one condition for replay. The fallback setting, runtime
health and completed-window cleanup must also permit it. The explicit
`gpu_execution()` entry uses its SQL-text source policy and the same lease-release
requirement; it does not use the transparent path's full bound-plan discovery.

## Diagnostics

Canonical and raw pipeline dumps show read identity, evidence depth, correspondence,
materializer profile and eligibility scope. Filter handling and replay permission
have separate fields:

| Field | Meaning |
|---|---|
| `pushdown_mode` | The scan's filter application mode |
| `scan_cpu_replay` / `scan_replay_veto` | One scan's source permission and reason |
| `plan_cpu_replay` / `veto` | Aggregate source permission across the GPU plan |
| `scope=gpu_plan` | The aggregate describes the dumped GPU plan |

These fields report source permission, not the final runtime decision to replay.
Veto reasons include S3, streaming sources and incomplete information.

Execution statistics expose `read_view_mismatches`, `certificate_mismatches`,
`checkpoint_revalidation_failures`, `execution_rebuilds` and
`lease_held_at_replay` to distinguish binding declines, ownership failures,
layout failures and rebuild activity.

## Limits

- File identity is based on bound paths and reader options. It cannot detect an
  object overwritten in place under the same path.
- Captured evidence does not provide object-version cache keys or complete
  snapshot certification.
- Existing Iceberg feature gates, delete handling and Parquet inheritance remain
  in the scan implementation.
- Native scans retain their existing row-group granularity, codec restrictions
  and MVCC coverage.
- Provider references are non-owning and statement-scoped. Sharing identity does
  not extend their lifetime or authorize use on another execution lane.

## Implementation map

| Area | Entry points |
|---|---|
| Identity and evidence capture | [bound_read_view.hpp](../../src/op/scan/table_scan/bound_read_view.hpp), [bound_read_view.cpp](../../src/op/scan/table_scan/bound_read_view.cpp) |
| Source verification and lowering | [scan_source_registry.cpp](../../src/planner/scan_source_registry.cpp), [sirius_plan_get.cpp](../../src/planner/sirius_plan_get.cpp) |
| Scan and split contracts | [scan_contract.hpp](../../src/op/scan/table_scan/scan_contract.hpp) |
| Plan comparison and registry | [read_view_registry.cpp](../../src/transparent/read_view_registry.cpp) |
| Replay policy and execution | [plan_source_policy.cpp](../../src/transparent/plan_source_policy.cpp), [physical_sirius_execution.cpp](../../src/transparent/physical_sirius_execution.cpp) |
| Native layout and lease ownership | [duckdb_native_gpu_ingestible.cpp](../../src/op/scan/duckdb_native_gpu_ingestible.cpp), [sirius_scan_manager.cpp](../../src/scan_manager/sirius_scan_manager.cpp) |
| Execution cleanup and internal connections | [sirius_context.cpp](../../src/sirius_context.cpp) |
| Pipeline diagnostics | [sirius_pipeline_converter.cpp](../../src/pipeline/sirius_pipeline_converter.cpp) |

Related details are covered in [Scan](scan.md),
[Physical Plan Generation](physical-plan-generation.md) and
[Streaming Sources](streaming-fragments.md).
