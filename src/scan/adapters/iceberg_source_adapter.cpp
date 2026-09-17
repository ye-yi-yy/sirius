/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "ingestible_support.hpp"
#include "io/uri_parser.hpp"
#include "log/logging.hpp"
#include "op/scan/iceberg_gpu_ingestible.hpp"
#include "op/scan/iceberg_metadata_connection.hpp"

#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/main/connection.hpp>

#include <algorithm>
#include <map>
#include <unordered_set>

namespace sirius::scan {
namespace {
/// Descends into children so a struct's fields count toward the id space they occupy.
void collect_field_ids(std::vector<duckdb::MultiFileColumnDefinition> const& columns,
                       std::vector<int32_t>& out)
{
  for (auto const& column : columns) {
    if (!column.identifier.IsNull() &&
        column.identifier.type().id() == duckdb::LogicalTypeId::INTEGER) {
      out.push_back(column.identifier.GetValue<int32_t>());
    }
    collect_field_ids(column.children, out);
  }
}

/**
 * @brief Refuse tables whose Iceberg field-id space has a gap, meaning a column was dropped.
 *
 * Iceberg never reuses a field id, so a column dropped and re-added under the same name gets a
 * NEW id and must read NULL in older data files. This path resolves columns by NAME, so it finds
 * the dropped column and returns data the table removed, with no error and no fallback.
 *
 * With no drops N fields occupy ids 1..N, so `max > count` means an id was retired. Reads only
 * bind data, opens no files.
 *
 * A PRE-FILTER, not the whole test: a plain ADDED column keeps the space contiguous and slips
 * through, but that case fails loudly instead of returning wrong rows. Resolving by field id is
 * the complete fix; DuckDB's MultiFileColumnMapper already implements it.
 */
std::optional<std::string> iceberg_retired_field_id_decline_reason(duckdb::LogicalGet& op)
{
  auto const* bind_data = dynamic_cast<duckdb::MultiFileBindData const*>(op.bind_data.get());
  if (bind_data == nullptr) { return std::nullopt; }

  // `reader_bind.schema` is the Iceberg schema and the only one carrying field ids; the generic
  // `columns` list may leave `identifier` null, so it is a fallback only.
  std::vector<int32_t> field_ids;
  collect_field_ids(
    bind_data->reader_bind.schema.empty() ? bind_data->columns : bind_data->reader_bind.schema,
    field_ids);
  if (field_ids.empty()) { return std::nullopt; }

  auto const max_field_id = *std::max_element(field_ids.begin(), field_ids.end());
  if (max_field_id <= static_cast<int32_t>(field_ids.size())) { return std::nullopt; }

  return "iceberg_scan table has a gap in its Iceberg field ids (highest is " +
         std::to_string(max_field_id) + " across " + std::to_string(field_ids.size()) +
         " fields), so a column was dropped; this scan path resolves columns by name and would "
         "read a dropped column's data in place of the re-added one";
}

std::string escape_sql_literal(std::string const& s)
{
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    if (c == '\'') { out += '\''; }
    out += c;
  }
  return out;
}

/// Type is empty for a nested container: parquet_schema() reports NULL there.
using iceberg_field_key = std::pair<std::string, int32_t>;
using iceberg_field_map = std::map<iceberg_field_key, std::string>;

/// Descends into children so nested fields count toward the schema each file must match.
///
/// @p order receives the same ids in recursive PREORDER — the walk order is load-bearing, because
/// it is what a file's physical layout is compared against.
void collect_field_id_names(std::vector<duckdb::MultiFileColumnDefinition> const& columns,
                            iceberg_field_map& out,
                            std::vector<int32_t>& order)
{
  for (auto const& column : columns) {
    if (!column.identifier.IsNull() &&
        column.identifier.type().id() == duckdb::LogicalTypeId::INTEGER) {
      auto const id = column.identifier.GetValue<int32_t>();
      out.emplace(iceberg_field_key{column.name, id},
                  column.children.empty() ? column.type.ToString() : std::string{});
      order.push_back(id);
    }
    collect_field_id_names(column.children, out, order);
  }
}

/**
 * @brief Refuse tables whose data files do not all carry the table's current (name, field id,
 *        type) schema, in the same physical ORDER.
 *
 * The scan resolves columns by NAME and emits them in the FILE's order, so every evolution
 * mis-reads: rename and add throw at SCAN time, which takes the runtime fallback and deadlocks the
 * connection; promotion (int -> long) throws nothing and returns the file's narrower type; and a
 * permutation returns the right columns under the wrong names. Over-declining is the intended
 * bias, and a valid-but-declined Iceberg layout is read correctly by DuckDB.
 *
 * Replaced wholesale once columns resolve by field id (DuckDB's MultiFileColumnMapper).
 *
 * @warning Reads every data file's Parquet footer on the planning thread. Fold into the footer
 *          cache the scan needs anyway rather than leaving two passes.
 */
std::optional<std::string> iceberg_schema_evolution_decline_reason(duckdb::LogicalGet& op,
                                                                   duckdb::Connection& conn)
{
  auto const* bind_data = dynamic_cast<duckdb::MultiFileBindData const*>(op.bind_data.get());
  if (bind_data == nullptr) { return std::nullopt; }

  iceberg_field_map table_schema;
  std::vector<int32_t> table_field_order;
  collect_field_id_names(
    bind_data->reader_bind.schema.empty() ? bind_data->columns : bind_data->reader_bind.schema,
    table_schema,
    table_field_order);
  // No field ids at all means a name-mapped table, which is what this path already assumes.
  if (table_schema.empty()) { return std::nullopt; }

  auto const files =
    detail::legacy_multi_file_paths(op.bind_data.get()).value_or(std::vector<std::string>{});
  if (files.empty()) { return std::nullopt; }

  // Apache writers record URIs in manifests, so these paths can arrive as `file:///...`. Strip
  // the scheme for the same reason the datasource boundary does: an unstripped path makes this
  // probe fail, and a failing probe declines -- which would quietly send every Apache-written
  // table to the CPU and undo the file:// fix this branch already landed.
  std::vector<std::string> probe_paths;
  probe_paths.reserve(files.size());
  std::string file_list = "[";
  for (std::size_t i = 0; i < files.size(); ++i) {
    probe_paths.push_back(sirius::io::strip_file_scheme(files[i]));
    if (i > 0) { file_list += ','; }
    file_list += "'" + escape_sql_literal(probe_paths.back()) + "'";
  }
  file_list += "]";

  // Do NOT filter `field_id IS NOT NULL` here: that drops every row of a file that has no ids,
  // and a file with no rows is indistinguishable from one that was never probed.
  // column_id is the row's index into the Parquet footer's flattened schema, i.e. its position in
  // the file's own recursive preorder. It is how physical ORDER is compared, which membership in a
  // (name, field id) map cannot see.
  auto result =
    conn.Query("SELECT file_name, name, field_id, duckdb_type, column_id FROM parquet_schema(" +
               file_list + ")");
  if (!result || result->HasError()) {
    return "the iceberg schema probe could not read this table's data-file footers (" +
           std::string(result ? result->GetError() : "null result") +
           "), so the files could not be proven to carry the table's current schema";
  }

  struct file_footer {
    iceberg_field_map schema;
    /// (column_id, field_id) for every ID-bearing footer row, sorted into physical order below.
    std::vector<std::pair<int64_t, int32_t>> id_order;
  };
  std::map<std::string, file_footer> per_file;
  while (true) {
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    for (duckdb::idx_t i = 0; i < chunk->size(); ++i) {
      auto& footer  = per_file[chunk->GetValue(0, i).ToString()];
      auto const id = chunk->GetValue(2, i);
      // A row with no field id is either the schema root or a synthetic nesting level (a 3-level
      // list's `list` group, a map's `key_value`); neither is a field the table declares.
      if (id.IsNull()) { continue; }
      auto const type     = chunk->GetValue(3, i);
      auto const field_id = static_cast<int32_t>(id.GetValue<int64_t>());
      footer.schema.emplace(iceberg_field_key{chunk->GetValue(1, i).ToString(), field_id},
                            type.IsNull() ? std::string{} : type.ToString());
      auto const column_id = chunk->GetValue(4, i);
      footer.id_order.emplace_back(column_id.IsNull() ? 0 : column_id.GetValue<int64_t>(),
                                   field_id);
    }
  }

  for (auto const& path : probe_paths) {
    auto const it = per_file.find(path);
    if (it == per_file.end()) {
      return "iceberg_scan data file '" + path +
             "' returned no Parquet schema rows, so it could not be proven to carry the table's "
             "current schema";
    }
    auto const& file_schema = it->second.schema;
    if (file_schema.empty()) {
      return "iceberg_scan data file '" + path +
             "' carries no Parquet field ids while the table's schema declares them, so it is "
             "name-mapped; this scan path resolves columns by name and would read the wrong "
             "column or fail at scan time";
    }

    for (auto const& [key, table_type] : table_schema) {
      auto const found = file_schema.find(key);
      if (found == file_schema.end()) {
        return "iceberg_scan data file '" + path +
               "' does not carry the table's current schema (no match for " + key.first + "#" +
               std::to_string(key.second) +
               "), so the table's schema has evolved; this scan path resolves columns by name and "
               "would read the wrong column or fail at scan time";
      }
      // Empty on either side is a nested container, whose type is implied by its children.
      if (!table_type.empty() && !found->second.empty() && found->second != table_type) {
        return "iceberg_scan data file '" + path + "' stores " + key.first + "#" +
               std::to_string(key.second) + " as " + found->second + " while the table declares " +
               table_type +
               ", so the column's type was promoted; this scan path reads the file's own physical "
               "type and would hand back a column of the wrong type";
      }
    }
    // Extra fields are dropped columns, which a name-based lookup would happily resolve to.
    if (file_schema.size() != table_schema.size()) {
      return "iceberg_scan data file '" + path + "' carries " + std::to_string(file_schema.size()) +
             " field ids where the table declares " + std::to_string(table_schema.size()) +
             ", so the table's schema has evolved; this scan path resolves columns by name and "
             "would read the wrong column or fail at scan time";
    }

    // Everything above is MEMBERSHIP, which a permuted file satisfies. Order matters because the
    // GPU path does not map columns by field id: for a full `SELECT *`, build_scan_plan leaves
    // needs_reader_projection false, so cuDF emits columns in the first footer's order while the
    // rest of the plan expects the bound snapshot's. DuckDB's own reader uses BY_FIELD_ID and is
    // unaffected, so the two disagree silently -- and a castable permutation converts the values
    // rather than erroring, since the runtime schema check only logs. Nested children are included
    // because a reordered struct child fails the same way.
    auto id_order = it->second.id_order;
    std::sort(id_order.begin(), id_order.end());
    std::vector<int32_t> file_field_order;
    file_field_order.reserve(id_order.size());
    for (auto const& [column_id, field_id] : id_order) {
      file_field_order.push_back(field_id);
    }
    if (file_field_order != table_field_order) {
      return "iceberg_scan data file '" + path +
             "' stores the table's fields in a different physical order than the bound snapshot's "
             "schema declares; this scan path emits columns in the file's own order and would hand "
             "back the right columns under the wrong names";
    }
  }

  return std::nullopt;
}

// Why an `iceberg_scan` table must decline the GPU path, or nullopt when it may run there.
//
// V2 positional deletes and V3 deletion vectors are applied on GPU. Equality deletes are not:
// they match on key VALUES, so the scan must force-project the key columns even when the query
// does not select them, which is not wired.
//
// Conservative by construction: any failure to PROVE the table free of equality deletes declines
// to CPU. A false positive costs performance; a false negative would drop deletes silently.
//
// Each decline returns the reason it actually hit — a probe that never managed to look is not
// the same as a table that really carries equality deletes.
std::optional<std::string> iceberg_gpu_scan_decline_reason(duckdb::LogicalGet& op,
                                                           duckdb::ClientContext& context)
{
  if (op.parameters.empty() || op.parameters.front().IsNull()) {
    return "iceberg_scan called without a table path, so its delete files cannot be inspected";
  }

  std::string table_path;
  try {
    table_path = op.parameters.front().GetValue<std::string>();
  } catch (...) {
    return "iceberg_scan table path is not a string, so its delete files cannot be inspected";
  }
  if (table_path.empty()) {
    return "iceberg_scan table path is empty, so its delete files cannot be inspected";
  }

  // iceberg_scan has three snapshot selectors; the delete path honours only snapshot_from_id,
  // so the others would resolve deletes against the CURRENT snapshot while the scan reads the
  // time-travelled one — filtering snapshot A's data by snapshot B's deletes.
  //
  // Deliberately coarse: narrowing it to tables that actually have deletes at the SELECTED
  // snapshot needs the very selector resolution that is missing. To lift it, thread the full
  // selector into read_iceberg_delete_data AND into its cache key — the key currently records
  // snapshot_from_id or "current", so two timestamps against one table would collide.
  for (auto const& selector : {"snapshot_from_timestamp", "version"}) {
    auto it = op.named_parameters.find(selector);
    if (it != op.named_parameters.end() && !it->second.IsNull()) {
      return std::string("iceberg_scan was given '") + selector +
             "', but the GPU scan path resolves delete files only by snapshot_from_id, so its "
             "deletes would be read from the wrong snapshot";
    }
  }

  // The flag rewrites bound data-file paths to <table_path>/data/<name>, while delete discovery
  // calls iceberg_metadata() without it and keeps the manifests' originals. The two then match
  // nothing and every delete is silently dropped.
  if (auto it = op.named_parameters.find("allow_moved_paths");
      it != op.named_parameters.end() && !it->second.IsNull()) {
    bool moved = false;
    try {
      moved = it->second.GetValue<bool>();
    } catch (...) {
      moved = true;  // Unparsable: assume the rewrite is on rather than assume it is off.
    }
    if (moved) {
      return "iceberg_scan was given 'allow_moved_paths', which rewrites its data-file paths; the "
             "GPU path discovers delete files under the paths the manifests record, so the two "
             "would not match and the table's deletes would be dropped";
    }
  }

  // "current" is resolved independently by DuckDB's bind, by Sirius's rebind of the serialized
  // plan, and by delete discovery; nothing makes the three agree, so a commit landing between any
  // two pairs one snapshot's data files with another's deletes. The id DuckDB bound is not
  // reachable -- it lives in types the iceberg extension compiles privately.
  //
  // Do NOT resolve it here instead. Highest sequence number is not the current snapshot under
  // rollback, branches or staged WAP commits, and comparing data-file sets cannot separate two
  // snapshots that differ only in deletes. Lifting this needs serializable iceberg bind data
  // upstream, or Sirius not planning twice.
  {
    auto const it = op.named_parameters.find("snapshot_from_id");
    if (it == op.named_parameters.end() || it->second.IsNull()) {
      return "iceberg_scan was called without 'snapshot_from_id', so the snapshot DuckDB bound "
             "cannot be recovered; the GPU path resolves delete files in a separate pass and "
             "would risk pairing one snapshot's data files with another's deletes";
    }
  }

  if (auto reason = iceberg_retired_field_id_decline_reason(op)) { return reason; }

  std::string escaped;
  escaped.reserve(table_path.size());
  for (char c : table_path) {
    if (c == '\'') { escaped += '\''; }
    escaped += c;
  }

  std::string query = "SELECT count(*) FROM iceberg_metadata('" + escaped + "'";
  // Inspect the same snapshot the scan will read.
  auto sid_it = op.named_parameters.find("snapshot_from_id");
  if (sid_it != op.named_parameters.end() && !sid_it->second.IsNull()) {
    try {
      query += ", snapshot_from_id = " + std::to_string(sid_it->second.GetValue<int64_t>());
    } catch (...) {
      return "iceberg_scan snapshot_from_id is not an integer, so the snapshot the scan will read "
             "cannot be inspected for delete files";
    }
  }
  // status <> 'DELETED' matches discover_from_manifests: a manifest keeps listing entries later
  // commits retired, so without it a table whose only equality-delete file has been compacted away
  // is refused the GPU forever over a delete that no longer applies. Both sites must agree on
  // which entries are live, or the gate's verdict describes a different table than the scan reads.
  query += ") WHERE content = 'EQUALITY_DELETES' AND status <> 'DELETED'";

  try {
    // Opening a second Connection to the same database re-registers the SAME SiriusContext, so
    // its query-lifecycle callbacks would fire QueryBegin (resetting next_operator_id and
    // task_creator state) and QueryEnd (clearing all data repositories) underneath the query
    // currently being planned. InternalQueryGuard suppresses both; without it this probe hangs
    // the outer query. Same pattern as sirius_extension.cpp's CPU-fallback replay.
    auto sirius_ctx = context.registered_state->Get<duckdb::SiriusContext>("sirius_state");
    if (!sirius_ctx) {
      SIRIUS_LOG_DEBUG(
        "[iceberg_source_adapter] iceberg delete probe: no SiriusContext — CPU fallback for '{}'",
        table_path);
      return "the iceberg delete probe could not acquire the Sirius context, so the table could "
             "not be proven free of equality-delete files";
    }
    duckdb::SiriusContext::InternalQueryGuard guard(context);

    // See iceberg_metadata_connection: one entry point, shared with discover_from_manifests so
    // both agree on which tables are legible, and pinned to the same snapshot the scan was bound
    // to. It mirrors the session's `unsafe_enable_version_guessing` rather than forcing it; a
    // table the outer session cannot read fails here and the decline below sends it to DuckDB.
    sirius::op::scan::iceberg_metadata_connection metadata_conn(context);
    auto result = metadata_conn.Query(query);
    if (!result || result->HasError()) {
      SIRIUS_LOG_DEBUG(
        "[iceberg_source_adapter] iceberg delete probe failed for '{}': {} — CPU fallback",
        table_path,
        result ? result->GetError() : "null result");
      return "the iceberg delete probe could not read this table's metadata (" +
             std::string(result ? result->GetError() : "null result") +
             "), so it could not be proven free of equality-delete files";
    }
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
      return "the iceberg delete probe returned no rows, so the table could not be proven free of "
             "equality-delete files";
    }
    auto const n_delete_files = chunk->GetValue(0, 0).GetValue<int64_t>();
    if (n_delete_files > 0) {
      SIRIUS_LOG_INFO(
        "[iceberg_source_adapter] iceberg table '{}' has {} equality-delete file(s); the GPU scan "
        "path "
        "does not apply equality deletes yet — falling back to DuckDB CPU.",
        table_path,
        n_delete_files);
      return "this iceberg table has " + std::to_string(n_delete_files) +
             " equality-delete file(s), which the GPU scan path does not apply yet";
    }

    // Reuses this connection deliberately: it is already bracketed as an internal query, and a
    // third Connection here would re-register the same SiriusContext underneath the query being
    // planned.
    if (auto reason = iceberg_schema_evolution_decline_reason(op, metadata_conn.get())) {
      return reason;
    }

    return std::nullopt;
  } catch (std::exception const& e) {
    SIRIUS_LOG_DEBUG(
      "[iceberg_source_adapter] iceberg plan-time probe threw for '{}': {} — CPU fallback",
      table_path,
      e.what());
    return "an iceberg plan-time probe threw (" + std::string(e.what()) +
           "), so the table could not be proven free of equality-delete files or of schema "
           "evolution";
  }
}

//! Build an `iceberg_ingestible_table_info`: the parquet bind data plus the table's delete
//! data, resolved here at plan time.
//!
//! Reading the deletes can fail — an unreadable manifest, a delete file that is not where the
//! metadata says. Every such failure throws out of here, which the caller turns into a CPU
//! fallback. It must never be softened into "no deletes": that is indistinguishable from an
//! append-only table, and would return rows the table logically deleted.
std::unique_ptr<sirius::op::scan::iceberg_ingestible_table_info> build_iceberg_table_info(
  sirius::op::sirius_physical_table_scan& scan_op,
  const sirius::operator_params& op_params,
  duckdb::ClientContext& context)
{
  auto info = std::make_unique<sirius::op::scan::iceberg_ingestible_table_info>();
  detail::populate_parquet_table_info(
    *info,
    scan_op,
    op_params,
    detail::legacy_multi_file_paths(scan_op.bind_data.get()).value_or(std::vector<std::string>{}));
  info->partition_indices =
    scan_op.bind_data->Cast<duckdb::MultiFileBindData>().reader_bind.hive_partitioning_indexes;

  if (scan_op.parameters.empty() || scan_op.parameters.front().IsNull()) {
    throw duckdb::NotImplementedException("iceberg_scan has no table path parameter");
  }
  info->table_path = scan_op.parameters.front().GetValue<std::string>();

  // Second line of the preflight gate in this adapter, which already declines an unpinned scan: a
  // caller reaching here without an id must fail loudly rather than read "current" a third time.
  // Deriving one here is not a fallback -- see that gate for why every derivation is unsound.
  auto const sid_it = scan_op.named_parameters.find("snapshot_from_id");
  if (sid_it == scan_op.named_parameters.end() || sid_it->second.IsNull()) {
    throw duckdb::NotImplementedException(
      "iceberg_scan reached GPU physical planning without 'snapshot_from_id': the snapshot its "
      "delete files must be read from is unknown, and reading them from whatever is current now "
      "risks pairing one snapshot's data files with another's deletes");
  }
  std::optional<uint64_t> const snapshot_id =
    static_cast<uint64_t>(sid_it->second.GetValue<int64_t>());

  auto sirius_ctx = context.registered_state->Get<duckdb::SiriusContext>("sirius_state");
  if (!sirius_ctx) {
    throw duckdb::NotImplementedException(
      "iceberg delete data cannot be read without a registered SiriusContext");
  }

  // Delete discovery opens its own Connection to run iceberg_metadata() and to read the
  // positional-delete parquet files, which re-registers this same SiriusContext. Bracket the
  // planning connection as an internal query so its lifecycle callbacks stay out of the way of
  // the query being planned. Same guard the delete gate uses.
  duckdb::SiriusContext::InternalQueryGuard guard(context);
  info->delete_data = sirius::op::scan::read_iceberg_delete_data(
    context, info->table_path, sirius_ctx->get_scan_manager().io_ctx(), snapshot_id);

  return info;
}

class iceberg_source_adapter final : public scan_source_adapter {
 public:
  const source_profile& profile() const noexcept override
  {
    static const source_profile value{source_kind::iceberg,
                                      "iceberg.legacy.unverified",
                                      dynamic_filter_mode::reader,
                                      byte_source_class::file_inventory,
                                      true,
                                      true,
                                      false};
    return value;
  }
  binding_verification verify_binding(const binding_ref& binding) const override
  {
    // TODO(R1 D1): use a versioned descriptor from the actual provider bridge.
    return binding.function.name == "iceberg_scan" &&
               dynamic_cast<const duckdb::MultiFileBindData*>(binding.data)
             ? binding_verification::compatibility
             : binding_verification::rejected;
  }
  read_view_capture try_capture_bound_view(const capture_request& request) const override
  {
    // TODO(R1 D1/D2/D3): require the provider descriptor, original inventory and selectors.
    return {request.instance, request.generation, request.origin};
  }
  source_preflight_result preflight_source(const source_preflight_request& request) const override
  {
    // Existing delete/snapshot/schema gates remain ahead of cache probing.
    if (auto reason = iceberg_gpu_scan_decline_reason(request.get, request.context)) {
      throw duckdb::NotImplementedException("iceberg_scan declines the GPU scan path: " + *reason);
    }
    return detail::parquet_pin_preflight(request, [&] {
      return detail::legacy_multi_file_paths(request.get.bind_data.get())
        .value_or(std::vector<std::string>{});
    });
  }
  scan_runtime_handle create_scan_runtime(const runtime_build_request& request) const override
  {
    return detail::make_ingestible_runtime(
      build_iceberg_table_info(request.physical(), request.parameters(), request.context), request);
  }
  source_policy_evidence inspect_source(const binding_ref& binding) const override
  {
    return detail::inspect_legacy_multi_file_source(binding);
  }
};
}  // namespace
std::unique_ptr<scan_source_adapter> make_iceberg_source_adapter()
{
  return std::make_unique<iceberg_source_adapter>();
}
}  // namespace sirius::scan
