# Shared Scan Framework: R1 Staging

The binding ownership package is committed as Sirius `63af08a8`, based on
DuckDB `3ff87f1ec7282ef44727e0e1d84237e88dd9a7b5`. Subsequent staging adds
source adapters, read-view representation and original-plan source policy. The DuckDB submodule remains at that revision without local source
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
expressions. Original binder publication and finalize/execution comparison
remain unwired. No Parquet or Iceberg read view is completed by enumerating
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

## Remaining R1 work and enablement

Supported DuckDB D1/D2/D3 bridges and the actual Iceberg provider bridge are
deferred. Complete operation qualification and observation coverage, lineage
and occurrence comparison, transaction preflight/sealing, fresh native
checkpoint leases, scan tickets and slice certificates remain unfinished.
Counters, plan-dump evidence and the complete exit/drain audit also remain.

Compatibility paths deliberately preserve existing execution without claiming
R1 qualification. Complete coverage and qualified operation profiles are
required before enabling R1 comparison/admission. R0 prerequisites, provider
qualification and the eligibility/performance matrices are separate gates.

## Validation

The existing CAT-6 expectation was corrected: live bindings and runtime
attachments prevent declaration replacement/removal; after release, a new
generation can be published and stale teardown cannot erase it. The fragment
fixture now uses the connection's actual catalog. The unsupported-filter case
uses a real native binding: a name-only seq_scan stub is rejected earlier by
the new implementation contract. No new Catch test cases were added.

The test runner creates shared environments in a truly paused state and
initializes GPU resources only for selected tests which need them. Previously
it created and destroyed the shared databases before Catch selected tests.
The stream catalog and owned-Parquet metadata suites then passed all 16 cases
and 63 assertions without initializing shared GPU environments.

The adapter refactor builds the DuckDB executable, loadable Sirius extension
and sirius_unittest against the unchanged submodule. Formatting and
static hooks passed, and DuckDB's tracked working tree and index have no diff.
No test cases were added or changed as part of removing the source patches
or moving source behavior into adapters.

Existing suites passed in separate processes:

- Plan-tree shapes: 18 cases, 1,211 assertions.
- Stream catalog/session, owned-Parquet metadata and Iceberg batch layouts:
  34 cases, 171 assertions. Three live S3 metadata cases returned early because
  S3 credentials/fixtures were not configured; their provider behavior was not tested.

A combined invocation exited during shared integration-environment startup,
without a Catch assertion report. The integration configuration requests 32 GB
of host memory on this roughly 15 GB machine. Separate invocations passed;
the combined process remains unqualified.

Twelve manual SQL result checks passed after the adapter refactor on temporary
data with the minimal config: native scans and filters, both Parquet aliases,
glob expansion, duplicate file paths, Hive partition columns, a mixed
native/Parquet join, planning fallback, native/Parquet runtime fallback
under injected GPU failures, and GPU execution after recovery. Supported
scans ran with CPU fallback disabled; both runtime fallback banners were
observed. These checks do not qualify volatile selector repeat safety.

The streaming-fragment integration fixture was corrected and compiled, but its
full integration suite was not run: the checked-in integration configuration
reserves 32 GB of host memory, exceeding this machine's available memory.
Actual-provider Iceberg end-to-end/ABI qualification and R1 performance and
concurrency matrices remain outstanding. Full R1 qualification is not claimed.
