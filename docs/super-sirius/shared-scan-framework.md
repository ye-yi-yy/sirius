# Shared Scan Framework: R1 Staging

The binding ownership package is committed as Sirius `63af08a8`, based on
DuckDB `3ff87f1ec7282ef44727e0e1d84237e88dd9a7b5`. Subsequent staging adds
source adapters, original/candidate comparison, window-owned scan contracts,
slice certificates, checkpoint leases and original-plan source policy. The DuckDB submodule remains at that revision without local source
patches. Standard Parquet and Iceberg use explicit unverified compatibility
adapters; unavailable binding observations retain unproven verdicts.

R1 is not complete or qualified. A descriptor match or complete read view
does not prove that repeating a binder or executing a CPU replay is safe.

## Full schema snapshots

`scan::bound_schema` owns column names, their order and serialized DuckDB
logical type metadata. Returning types deserializes independent objects:
DuckDB logical type copies otherwise share mutable auxiliary metadata.
Serialization is an ownership mechanism, not the equality representation.

The optional canonical schema identity is versioned and uses typed fields with
byte-length prefixes. It includes names, logical and physical types, aliases,
decimal width/scale, string collation, nested child names/order, array size and
enum dictionary order. It does not use display strings or DuckDB's permissive
type equality. Unqualified types or extension-specific semantic metadata retain
their owned payload but have no canonical identity. An absent identity is not
an empty schema. Equality of supported independently captured schemas compares
the complete canonical text; sharing the same immutable owner also establishes
schema equality.

`SiriusReadParquetBindData` retains this schema along with its existing URI and
row count. `Copy()` shares the immutable snapshot and `Equals()` includes it.
The metadata-only constructor remains available with a null schema, which
read-view capture classifies as unavailable. The binding callback always
supplies the full schema. No table-function serializer or additional I/O is
introduced.

This schema identity is only one component of the bound-read-view key. It
does not identify files, options, source implementations or physical versions.

## Stream declaration ownership

Each `stream_bind_catalog` has a process-unique, non-wrapping instance ID.
Declarations receive monotonically increasing, non-reused generation numbers.
The immutable declaration owns:

- Catalog instance, stream ID and declaration generation.
- Full schema and the existing Sirius runtime types.
- Repository and expected senders.

The DuckDB bind payload retains this exact declaration, including across
`FunctionData::Copy()`. Physical lowering consumes the retained declaration
instead of resolving the latest entry for an ID. Attachment verifies it still
belongs to the current catalog and generation.

The mutable `built` pointer is kept in the catalog entry, outside the immutable
declaration. A plan-owned `stream_source_attachment` retains the declaration and
catalog and clears only its own declaration/operator pair on destruction.
Existing one-leaf-per-stream restrictions still apply.

Replacement, erasure and clear fail with `stream_binding_in_use` while a binding
or operator retains the declaration. Clear checks all entries before deleting
any. `get()` returns a detached snapshot, not a reference into an unlocked map;
binding code uses `get_declaration()` to retain ownership explicitly.

Fragments record the generations they publish. Success teardown, failed builds
and retries release sessions and plans before erasing those generations. A stale
fragment cannot erase a newer declaration for the same ID, and an unbuilt
fragment cannot erase a peer's declaration. FFI setup declarations are tracked
separately from the generations later published by the streaming fragment.

FFI type resolution retains the full DuckDB schema before conversion to Sirius
runtime types. Direct C++ callers can supply the same snapshot through
`stream_input_spec::schema` or `stream_input_binding::schema`. Without it,
scalar schemas are reconstructed from the supplied runtime types; nested types
whose child metadata has been erased are rejected rather than inventing a
schema.

## Source adapters and compatibility

Each DatabaseInstance owns one registry through its extension callback manager.
The registry owns five implementations of `scan_source_adapter`: native,
standard Parquet, Sirius-owned Parquet, stream and Iceberg. It resolves the
adapter by calling `verify_binding` before provider-specific payload access.
`source_registry.cpp` is the single built-in registration site.

Each adapter provides:

- `profile`: dynamic-filter mode, runtime form, verification status and replay veto.
- `verify_binding`: recognition of the implementation and its bind payload.
- `try_capture_bound_view`: source identity from the retained binding, without binding or I/O.
- `preflight_source`: existing source-specific planning gates and cache residency evidence.
- `declare_resources`: attached native storage and transactions needed before protection.
- `create_scan_runtime`: the existing ingestible or direct source operator.
- `inspect_source`: byte-source facts for the original-plan fallback policy.

Logical planning calls preflight and handles shared projection/filter/schema
work. Physical lowering wraps an ingestible using the profile's dynamic-filter
capability; direct sources use their existing source operator. Source kind is
an identity tag, not behavioral dispatch in either planner, read-view capture
or fallback policy. Stream construction does not require a synthetic file list.

Native preflight owns the overflow-string and pinned MVCC checks. Iceberg owns
its snapshot, schema-evolution and delete gates and delete discovery. Standard
and Sirius-owned Parquet own their respective path/partition extraction. These
are moves of existing behavior; adapters do not introduce new bind calls or
replace the existing readers. Whole-plan replay decisions remain in the
framework, combining adapter facts and vetoes across all original sources.

To add another scan, implement an adapter, register its factory and add its
source file to the build. Add or reuse a reader/runtime implementation as
needed, and supply a distinct canonical identity if supported. Extend the
source-kind tag when the new identity needs one; it carries no dispatch logic. A new source
using existing runtime/filter capabilities does not require source-specific
branches in the common planner, capture or fallback code. New execution
semantics can still require an explicit shared capability extension.

Native scans use DuckDB's existing TableScanFunction factory; Sirius-owned
Parquet and streams use the same factories as their registration. Their
adapters check callbacks, signatures, named parameter types, semantic flags
and concrete bind payloads without DuckDB source changes.

Standard Parquet aliases and Iceberg retain their existing implementations
through explicit compatibility profiles:

- `duckdb.parquet.legacy.unverified`: parquet_scan and read_parquet.
- `iceberg.legacy.unverified`: iceberg_scan.

These adapters use name/MultiFile payload checks for compatibility routing and
set implementation_verified to false. They do not claim verified callback or
bound-option identity. Read-view capture returns unverified for both profiles;
a known function name or successful scan cannot supply the missing evidence.
Native, Sirius-owned Parquet and stream verification remains available through
existing APIs.

TODO(R1 D1): provide supported Parquet factory/bound-option access and the
actual Iceberg provider descriptor bridge. Validate concrete inner bind data,
reader and interface before enabling verified Parquet view capture. No local
DuckDB header relocation, private-layout replica or options accessor is used.

## Inventory and read views

`legacy_multi_file_paths` retains the existing MultiFile GetAllFiles behavior
for standard Parquet and Iceberg. Adapter preflight, runtime lowering and
source-policy discovery use this internal compatibility helper. It can expand the inventory and
perform I/O, just as the old implementation did; it supplies execution paths,
not verified read-view or repeat-safety evidence. Missing inventories remain
unavailable instead of being certified as complete. Sirius-owned URI lowering
continues to read the retained bind payload.

TODO(R1 D2): obtain supported non-expanding inventory and original publication
APIs, preserve original owner/provenance across allowed materialization,
distinguish complete-empty, unavailable and unsupported captures, and retain
file options. Only then enable Parquet/Iceberg file-multiset and option equality.
The framework's reserved file evidence type is Sirius-owned and does not
depend on new DuckDB declarations.

`bound_read_view` currently represents a versioned, typed, length-prefixed
identity for supported schemas and verified profiles:

- Database instance, source kind/profile and full schema.
- Native catalog/table OIDs and the resolved qualified table name.
- The exact supplied Sirius-owned Parquet URI.
- Stream catalog instance, stream ID and declaration generation.

Cardinality, projection, filters and transaction IDs do not enter the view.
The typed selector encoder reads already evaluated values without evaluating
expressions. Hook evidence is retained before template copying, including when
copying fails. Physical-original capture walks unique `GetChildren()` nodes.
Finalize and execution rebuild each compare their own candidate against the
preserved generation. No Parquet or Iceberg read view is completed by enumerating
files or inferring the missing reader options.

## Binding-audit compatibility

`capture_planning_repeat_audit` preserves the framework API using only existing
Sirius connection state. It reports observations_available=false and unproven
for repeat binding, speculative execution and CPU replay. It does not register
a DuckDB observer, reconstruct an audit from optimized expressions, evaluate
selectors again or synthesize a safe verdict.

The optimizer/finalize integration keeps the legacy copy/replan behavior while
this bridge is absent. In particular, volatile selectors such as nextval no
longer have the experimental early-binding veto from the reverted DuckDB
patch. Exactly-once selector evaluation is not guaranteed by this staging.
The existing connection planning generation is bookkeeping, not proof of an
original pre-evaluation observation.

TODO(R1 D3): consume supported observations before scalar/aggregate/cast
binding and table-argument evaluation; allocate original statement generations
and occurrence tokens; retain original evaluated selectors and source views;
isolate nested/internal planning; then qualify operation, copy and collation
profiles before enabling repeat-safety admission. DuckDB source patches for
these hooks are deferred.

## Original-plan source policy

Transparent finalize captures a whole-plan policy from DuckDB's original
physical plan before any candidate replan. The walk uses GetChildren, includes
wrapper-owned subplans and visits shared nodes once. Discovery completeness
and byte-source classification remain separate.

Known S3 and adapter replay vetoes dominate unknown/incomplete sources.
Non-replayable adapters contribute their source names to a shared veto set;
the policy has no stream- or owned-Parquet-specific boolean branches. An unregistered
MultiFile reader still participates through legacy inventory enumeration; a
missing or failed inventory prevents replay. Unknown non-file sources and
unverified profiles remain unclassified. A failed scan capture does not stop
discovery of vetoes elsewhere in the plan. Parquet and Iceberg share the internal
compatibility discovery helper to preserve existing local/S3 routing while provider
migration remains pending.

All finalize decline paths and runtime fallback consume this preserved policy.
SQL text can add an S3 veto, never clear one. A missing candidate also follows
the source policy. Runtime fallback additionally requires the preserved CPU
statement to be read-only. Candidate installation retains the original CPU
plan until construction succeeds.

This centralizes existing source vetoes, not the complete R1 replay predicate.
Non-expanding discovery, original statement safety observations, consumer/window
ownership and failed-drain qualification are still required.

## Candidate correspondence and runtime windows

Verified source views are compared as multisets, preserving independent duplicate
occurrences. `original_copy_chain` requires a bijection to hook occurrences by
`table_index`; SQL replans require a single source or an empty correspondence.
Copying a replan-derived template preserves its origin. Required evaluated
selectors are compared against the same-generation hook partner, and candidate
output types must match the retained CPU plan before lowering.

Compatibility providers remain explicitly unproven. Their occurrence counts
and function names are checked without claiming file/option/selector equality.
The requested compatibility path also retains existing multi-source replans
when a Standard Parquet or Iceberg source is present. Remove this exception
when D1/D2/D3 can qualify the real provider path.

Each validation or execution build receives a fresh registry and process-wide,
monotonic, non-wrapping 64-bit handles. Immutable tickets retain the source,
generation, window, output layouts, required columns, projection, static
filters and materializer. Filters are copied before lowering consumes them.
Stream receives a ticket without creating file splits. Registry membership and
active-window checks reject expired or foreign consumers.

Validation contracts and leases die before installing the reusable transparent
operator. Execution rebuild captures and compares again with fresh contracts.
The registry never publishes `admission_supported` while D3 remains unproven.

## Construction, native leases and drain

Adapters declare resources before the construction seal. All existing
SQL-dependent provider preparation finishes before the window acquires native
checkpoint readers. Native preflight uses a temporary reader for its existing
storage probes; runtime layout capture uses one shared checkpoint lease per
actual AttachedDatabase, acquired in database order. Transactions start before
leases. Pin population records its checkpoint iteration under its own scoped
lease.

A protected window rejects new internal-query brackets and metadata-connection
creation until drain releases its leases. Iceberg metadata uses an explicit
read-only transaction. This is a construction restriction, not D3 evidence or
permission for arbitrary provider callbacks. Supporting new internal SQL lanes
under a lease still requires separate qualification.

Native prepare, metadata walks, staging and decode revalidate the attached
database, block manager, iteration and layout association. The engine, plan,
storage, pin runtime and fragment declarations are retained through mandatory
drain. Cleanup stops metadata issuance, drains task creation, execution,
device work and I/O, then closes registries and releases leases. Failed drain
retains owners until runtime teardown and prohibits CPU replay. Buffered results
and idle prepared plans do not retain active leases.

## Certified slices and diagnostics

Fresh Parquet slices retain a runtime inventory occurrence (including duplicate
paths), footer, selected row groups, reader options/plan and datasource owner.
Iceberg slices additionally retain the existing delete-data owner. This runtime
inventory is not original-binding D2 evidence. Native slices retain the exact
row-group/segment descriptor snapshot, lease, storage, datasource and staging
owners. Insert-delta staging remains shared while each consuming scan receives
its own certificates.

Coalescing preserves constituent certificates and rejects mixed consumers.
Connector enqueue, prefetch and materialization validate live membership and
per-slice range/dependency associations before decode. Resident pinned batches
continue through their existing identity/MVCC/layout checks. Empty results
retain the dependencies still used by their existing materializer.

The source registry owns `read_view_mismatches`, `certificate_mismatches` and
`checkpoint_revalidation_failures`. Plan dumps include a bounded ticket/window,
source, identity hash, correspondence and evidence/replay classification, with
no paths, selectors or predicate values. Safety/admission remain explicitly
unproven. This adds runtime enforcement, not physical byte-version guarantees.

Iceberg delete-data memoization uses a typed key containing database instance,
connection, query ordinal, planning generation, transaction, exact table path,
snapshot and the effective version-guessing setting. It remains query-local and
never supplies missing original evidence.

## Remaining external qualification

D1/D2 bridges for actual Standard Parquet/Iceberg providers and DuckDB D3
original-binding observations remain deferred. The compatibility adapters and
audit TODOs describe the external changes needed before full admission can be
enabled. R0 provenance, real-provider compatibility, performance/concurrency
matrices and the complete R1 acceptance qualification are still required.
R2a-R4 statement certification, byte-version contracts and new reader/runtime
implementations are outside this change.

## Validation

The release build of `duckdb`, `sirius_loadable_extension` and `sirius_unittest`
passes with the current runtime-contract changes (2026-09-18), including the
device fence in `SiriusContext`. Formatting/static hooks and whitespace checks
pass. DuckDB tracked source and the submodule revision are unchanged. Pixi
activation now preserves an already-correct CMakePresets symlink.

Runtime verification exposed and fixed two implementation issues:

- Logical-plan copies do not preserve resolved output types. Candidate type
  resolution now precedes evidence capture, avoiding false
  `output_schema_mismatch` refusals for supported queries.
- Schema-only Parquet files have no row-group column chunks. Byte accounting
  skips those absent chunks while the empty split retains its schema and
  dependency certificates.

Only existing tests were adapted to the new contract; no test cases were added.
Operator-only plan-tree fixtures now supply the construction window normally
installed by `create_plan`; a missing window produces an explicit error instead
of dereferencing null. Lifecycle AC-6 now permits the CPU fallback it expects
when the optimizer is disabled and no current logical capture/SQL candidate
exists. It still verifies that the old generation is not consumed and the new
binding returns the correct result.

Existing-suite results:

- Plan-tree shapes: 18 cases, 1,211 assertions passed.
- Stream catalog/session/batches, owned-Parquet metadata and Iceberg
  layout/delete/Puffin helpers: 71 cases, 379 assertions passed. Provider checks
  that return early without S3 fixtures do not qualify live S3 behavior.
- Streaming fragments: 5 cases, 83 assertions passed, including output surviving
  window cleanup, two-fragment chains, Parquet scans and multiple batches.
- Local transparent runtime fallback: 6 cases passed, covering own uncommitted
  writes, snapshot stability across a concurrent commit, fallback-disabled
  errors and subsequent recovery. The broader selector reported 31 cases and
  110 assertions; its S3/child-runner early returns are not provider validation.
- Query lifecycle: 11 cases, 78 assertions passed, including unconsumed/pending
  results, concurrent execution/preparation, pin-state changes, stale capture
  rejection, unavailable runtime and planning-error recovery.
- Cancelled-waiter gate: 1 case, 6 assertions passed. Cancellation does not enter
  a later execution window or trigger replay; a follow-up query succeeds.
- Scan/scan-manager selection: 298 of 300 cases passed together. The CPU-only
  `any_uncheckpointed_appends` case encountered a default-size GPU allocation
  failure in the combined process and passed independently (27 assertions).
  The remaining default-config case fails because WSL reports unknown capacity
  for NUMA node -1; it does not exercise the shared scan contract.

Fourteen manual SQL CPU/GPU comparisons passed with fallback disabled on the
supported GPU paths: native filters/projections/self-join/empty results,
Parquet aliases/duplicates/globs/empty and pruned files, owned Parquet, Hive
partition projection, mixed native/Parquet joins, and a pinned native self-join
over insert deltas. `FORCE CHECKPOINT` completed after normal queries and after
unpinning, checking that query-scoped leases had been released.

The integration suites above used their original configuration. Its 32 GB host
capacity is a budget, not an immediate 32 GB allocation; the initial host pools
allocate about 5 GiB. The earlier assumption that this capacity alone prevented
running the suites was incorrect.

Local command output is retained under `build/r1-validation/`. Live S3 and
actual-provider Iceberg end-to-end/ABI qualification remain unverified. Full R1
acceptance, performance and concurrency qualification are still not claimed.
