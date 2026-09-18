# Shared Scan Framework

The framework gives table scans shared binding evidence, plan contracts and
execution lifetimes while preserving their existing readers. Source-specific
behavior lives in adapters; the logical planner, physical planner and fallback
policy consume their common interface.

## Sources and adapter boundary

Each DuckDB `DatabaseInstance` owns a `source_registry` with five adapters:

| Source | Entry points | Binding evidence |
| --- | --- | --- |
| DuckDB native | `seq_scan` | Verified implementation, schema and table identity |
| Standard Parquet | `read_parquet`, `parquet_scan` | Unverified compatibility path |
| Sirius-owned Parquet | `sirius_read_parquet` | Verified implementation, schema and retained URI |
| Iceberg | `iceberg_scan` | Unverified compatibility path |
| Stream | `sirius_stream_source` | Verified implementation, schema and retained declaration |

Verification checks the implementation and bind payload before source-specific
access. Native, Sirius-owned Parquet and Stream use their existing factories.
Standard Parquet and Iceberg currently use function-name and MultiFile payload
checks for routing, which do not establish implementation or bound-option identity.

The [adapter interface](../../src/include/scan/source_adapter.hpp) provides:

| Method | Responsibility |
| --- | --- |
| `profile` | Runtime form, dynamic-filter capability, verification status and replay veto |
| `verify_binding` | Recognize the implementation and bind payload |
| `try_capture_bound_view` | Capture retained binding identity without rebinding or I/O |
| `preflight_source` | Apply source-specific planning gates and inspect pin residency |
| `declare_resources` | Retain native storage needed for execution preparation |
| `create_scan_runtime` | Construct an ingestible or a direct source operator |
| `inspect_source` | Report byte-source facts for whole-plan fallback policy |

Adapters retain native MVCC/overflow-string checks, Parquet path handling and
Iceberg snapshot/schema/delete gates. An ingestible uses the common table-scan
operator; a direct source such as Stream supplies its own operator. There is no
single replacement reader/runtime: an adapter may reuse an existing one.

## Binding evidence and ownership

`bound_schema` owns full column names and serialized DuckDB types, including
nested metadata. Returned types are independent copies. Supported schemas also
have a canonical identity; unsupported semantic metadata remains owned but
cannot be claimed equal through a missing identity.

`bound_read_view` identifies a source using its database instance, kind/profile,
full schema and source-specific identity: native table OIDs and qualified name,
the retained Sirius-owned Parquet URI, or Stream catalog/ID/declaration
generation. Equality uses canonical data, not the diagnostic hash. Projection,
filters, cardinality and transaction IDs are not part of this identity.
A matching view does not prove that bytes are unchanged or that repeating
binding is safe.

Stream bind data retains an immutable declaration containing schema, repository
and expected senders. Lowering uses that declaration rather than looking up the
latest declaration by ID. Replacement and erasure fail while bindings or
operators retain it. A plan-owned attachment releases only its own
declaration/operator pair, and fragment cleanup erases only its published
generations.

Standard Parquet and Iceberg have no verified read view yet.
`legacy_multi_file_paths` preserves their existing `GetAllFiles` behavior for
preflight, runtime construction and fallback discovery. It may expand the file
inventory and perform I/O; its result is execution data, not evidence of what
the original binding observed.

## Planning and execution flow

1. **Capture the original.** Retain logical-hook evidence and evaluated
   selectors before copying the template. Walk the original physical plan,
   including wrapper-owned subplans, to capture source policy.
2. **Compare and lower.** Compare candidate source occurrences and output types
   against the original. Verified views are compared as multisets, preserving
   duplicates. Copy-derived plans match hook occurrences by table index and
   compare required selectors. SQL replans have stricter correspondence rules;
   the Parquet/Iceberg compatibility exception is described below.
3. **Retain the validated plan.** `OnFinalizePrepare` keeps the physical plan
   and pin-registry epoch. Its scan registry is inactive and owns no checkpoint
   keys. First execution reuses the plan if the epoch is unchanged; an epoch
   change rebuilds and compares again. Prepared re-execution goes through
   DuckDB rebind/finalize with new binding evidence.
4. **Prepare execution.** `sirius_scan_manager::prepare_for_query` activates the
   registry once. It starts all required native transactions before acquiring
   any checkpoint key, then shares one lease per actual attached database.
   Fresh native metadata walks run on the query thread under those leases;
   fully resident pin hits skip the walk.
5. **Validate asynchronous handoffs.** Consumer tickets and slice certificates
   retain ownership and reject expired windows, foreign consumers, invalid
   ranges or inconsistent dependencies before decode.
6. **Drain and release.** Cleanup stops metadata issuance and drains task
   creation, execution, device work and I/O before closing registries and
   releasing leases. Failed drain retains owners until runtime teardown and
   prohibits CPU replay. Idle prepared plans and buffered results hold no
   active checkpoint leases.

`StandaloneQueryScope::finish()` records the attempt's lease-release result
before releasing the lifecycle slot. CPU replay accepts only `released` or
`not_entered`; it does not inspect a scan manager that another query may already
own. Replay also requires the preserved source policy, a read-only CPU
statement and the existing fallback/error rules to permit it. Known S3 sources,
adapter vetoes and incomplete file discovery can prohibit replay.

## Runtime contracts

A plan-owned `query_scan_registry` assigns immutable consumer tickets containing
source identity, generation/window, output layouts, required columns,
projection, copied static filters and materializer. Execution activates the
registry once; cleanup closes it permanently.

Parquet slices retain inventory occurrences, footers, selected row groups,
reader options and datasource owners. Iceberg also retains delete-data owners.
Native slices retain descriptor snapshots, storage, checkpoint leases,
datasource and staging owners. Coalescing preserves constituent certificates
and rejects mixed consumers. Connector enqueue, prefetch and materialization
check membership and range/dependency associations.

Resident pinned batches retain their existing identity, MVCC and layout checks.
Runtime dependency checks protect ownership and decode assumptions; they do
not certify original file inventories or physical byte versions.

## Internal metadata connections

Use [`scan::open_internal_connection`](../../src/include/scan/internal_connection.hpp)
for framework-owned metadata SQL. Iceberg metadata/schema probes and
positional-delete reads use this helper.

The helper installs an internal-query guard, starts `BEGIN TRANSACTION READ ONLY`,
then mirrors explicitly requested Boolean settings. `Query` accepts exactly
one SELECT and requires the same read-only transaction. The raw connection is
not exposed. Rollback and connection destruction run while the guard is active,
including constructor failure.

Helper connections retain the parent's protected-window ancestry, so ordinary
nested planning guards cannot bypass a native lease by switching connections.
Read-only helpers avoid DuckDB's non-read-only transaction-start lock. This
controls connection lifetime and transaction mode; it does not establish
original binding provenance or qualify arbitrary provider callbacks. Iceberg
discovery remains plan-time work over an explicitly selected snapshot.

## Diagnostics

The source registry and `SiriusContext::transparent_execution_stats` share
database-owned contract counters:

| Counter | Meaning |
| --- | --- |
| `read_view_mismatches` | Original/candidate view or correspondence mismatch |
| `certificate_mismatches` | Consumer or slice contract violation |
| `checkpoint_revalidation_failures` | Native checkpoint dependency failed revalidation |
| `scan_capture_incomplete` | An attempt observed incomplete capture, including compatibility captures |
| `scan_planning_safety_declines` | A planning-safety refusal |
| `scan_source_verification_declines` | A source-verification refusal |

Attempt diagnostics deduplicate repeated reports and emit at most one bounded
contract-refusal INFO report per attempt. Unproven safety or verification does
not increment a decline counter when compatibility behavior still permits the
plan. Execution rebuilds start a new diagnostic attempt.

Plan dumps show each consumer's source/profile, instance/generation, candidate
origin, identity hash, correspondence, evidence scope/depth, selector status
and independent repeat-bind, speculation and CPU-replay verdicts. Missing
evidence remains unverified or unproven. `execution_rebuilds` tracks rebuilt
plans; `lease_held_at_replay` must remain zero for permitted replay.

## Adding a source

1. Implement `scan_source_adapter` under
   [`src/scan/adapters/`](../../src/scan/adapters/). Define a profile and verify
   the actual factory and payload before accessing provider data.
2. Implement side-effect-free read-view capture with owned schema and source
   identity. Keep unavailable evidence explicit; do not manufacture it by
   rebinding, enumerating files or evaluating selectors again.
3. Supply preflight gates, resource declarations, runtime construction and
   source-policy facts. Reuse an ingestible/direct operator where its semantics
   fit; add a reader when storage, decoding or delivery semantics require it.
   Preserve the shared ticket, dependency and cleanup contracts.
4. Declare the factory in
   [`source_adapters.hpp`](../../src/scan/adapters/source_adapters.hpp),
   register it in [`source_registry.cpp`](../../src/scan/source_registry.cpp)
   and add implementation files to [`CMakeLists.txt`](../../CMakeLists.txt).
5. Extend source identity encoding when needed. The source-kind tag is not a
   dispatch switch. Existing runtime/filter capabilities need no source-specific
   branches in the common planners, capture or fallback policy; new execution
   semantics may require a shared capability extension.

## Current limitations and follow-up

- **Provider verification:** Standard Parquet needs supported factory and
  bound-option access; Iceberg needs a descriptor bridge from the actual
  provider. Verify concrete bind data, reader and interface before enabling
  verified capture.
- **Original file evidence:** Providers must expose retained inventories,
  options and provenance without expansion or I/O. Distinguish complete-empty,
  unavailable and unsupported capture before enforcing file-multiset and option
  equality. Runtime inventories cannot substitute for this evidence.
- **Original binding observations:** DuckDB must expose observations before
  binding and table-argument evaluation, including statement generations,
  occurrence identity and retained evaluated selectors. Nested/internal planning
  must be isolated. Operation, copy and collation behavior then need separate
  repeat-bind, speculation and CPU-replay qualification.
- **Compatibility admission:** The current binding audit reports unavailable
  observations and unproven safety. Legacy copy/replan behavior remains;
  exactly-once selector evaluation is not guaranteed. Parquet/Iceberg comparison
  checks occurrence counts and function names, not file/option/selector
  equality, and retains a multi-source SQL-replan exception. Remove that
  exception only when the provider and original-binding evidence support it.
- **External prerequisites:** Native decoder correctness and provider-internal
  connection isolation still need their separate prerequisite changes integrated.
  Current helpers and ownership checks do not replace those changes.

Full statement safety certification, physical byte-version guarantees and a
unified reader/runtime implementation are outside the current framework.
