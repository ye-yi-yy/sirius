# Shared Scan Framework: Binding Ownership

This is the first production package for #1796, based on Sirius
`1f996e3cd022f7f416bf83fd12c889005e8f730d` and DuckDB
`3ff87f1ec7282ef44727e0e1d84237e88dd9a7b5`. It implements immutable schema
ownership and stream declaration lifetimes from P1. It does not establish R1
admission or qualify any adapter.

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
The metadata-only constructor remains available with a null schema, which a
future capture path must classify as incomplete. The binding callback always
supplies the full schema. No table-function serializer or additional I/O is
introduced.

This schema identity is only one component of a future bound-read-view key. It
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

## Remaining R1 work

Verified source descriptors, non-expanding provider capture, original-binding
safety observations, complete view/options encoding, planning correspondence,
source replay policy, native checkpoint leases and certified slice consumers
remain separate packages. No registry or admission path may treat the types
introduced here as evidence that those checks have passed.

This package intentionally adds or modifies no test cases. Existing tests can
be used for regression checks, but new qualification and performance evidence
are still required before R1 enablement. The old stream catalog CAT-6 expectation
that a declaration with an attached operator can be overwritten conflicts with
the new lifetime contract; that test is left unchanged for the later test stage.

## Validation of this package

On September 17, 2026, the release build of both `sirius_loadable_extension` and
`sirius_unittest` completed, including linking. Repository pre-commit checks
passed for the changed production and documentation files.

The existing stream catalog, source, session, fragment, FFI and Sirius Parquet
metadata tests were selected for execution. Both launches were interrupted
before a test summary; WSL boot history confirmed that the instance restarted
during those attempts. Runtime regression results are therefore unavailable.
No passing runtime or R1 qualification result is claimed.
