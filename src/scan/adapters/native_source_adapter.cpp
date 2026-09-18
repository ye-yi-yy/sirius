/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "ingestible_support.hpp"
#include "op/scan/duckdb_mvcc_visibility.hpp"
#include "op/scan/duckdb_native_gpu_ingestible.hpp"
#include "op/sirius_physical_table_scan.hpp"
#include "scan/scan_contract.hpp"
#include "scan/source_registry.hpp"
#include "scan_manager/sirius_scan_manager.hpp"
#include "sirius_context.hpp"

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>

#include <numeric>
#include <typeinfo>

namespace sirius::scan {
namespace {
//! Build a `duckdb_native_ingestible_table_info` from a `seq_scan` TABLE_SCAN. Requires a
//! live `ClientContext` (the ingestible reads table storage during `prepare_for_query`);
//! throws on non-base-table scans.
std::unique_ptr<sirius::op::scan::duckdb_native_ingestible_table_info>
build_duckdb_native_table_info(sirius::op::sirius_physical_table_scan& scan_op,
                               const sirius::operator_params& op_params,
                               duckdb::ClientContext& context)
{
  if (!scan_op.bind_data) {
    throw std::runtime_error("[native_source_adapter] seq_scan has no bind_data");
  }
  auto* table_scan_bind = dynamic_cast<duckdb::TableScanBindData*>(scan_op.bind_data.get());
  if (table_scan_bind == nullptr) {
    throw std::runtime_error(
      "[native_source_adapter] seq_scan bind_data is not "
      "TableScanBindData; the GPU-native duckdb scan path supports only seq_scan over base "
      "tables.");
  }
  auto& bind_data = *table_scan_bind;
  auto& table     = bind_data.table.Cast<duckdb::DuckTableEntry>();

  auto info     = std::make_unique<sirius::op::scan::duckdb_native_ingestible_table_info>();
  info->storage = &table.GetStorage();
  info->context = &context;
  info->db_path = table.GetStorage().GetAttached().GetStorageManager().GetDBPath();
  // Qualified-name identity for the pin cache — derived from the resolved
  // DuckTableEntry so it matches the pin-side derivation (build_duckdb_pin_info)
  // exactly. Without these a pin_table(format='duckdb', ...) query silently misses
  // the pinned cache and falls through to disk.
  info->catalog_name           = table.ParentCatalog().GetName();
  info->schema_name            = table.ParentSchema().name;
  info->table_name             = table.name;
  info->approximate_batch_size = op_params.scan_task_batch_size;

  std::vector<std::size_t> source_ids_fallback;
  if (scan_op.projection_ids.empty()) {
    source_ids_fallback.resize(scan_op.column_ids.size());
    std::iota(source_ids_fallback.begin(), source_ids_fallback.end(), 0);
  }
  auto const& source_ids =
    scan_op.projection_ids.empty() ? source_ids_fallback : scan_op.projection_ids;

  info->projected_cols.reserve(source_ids.size());
  info->projected_types.reserve(source_ids.size());
  for (std::size_t k = 0; k < source_ids.size(); ++k) {
    auto pid            = source_ids[k];
    auto const& col_idx = scan_op.column_ids[pid];
    sirius::op::scan::projected_column pc;
    pc.is_rowid = col_idx.IsRowIdColumn();
    if (!pc.is_rowid) { pc.storage_idx = duckdb::StorageIndex(col_idx.GetPrimaryIndex()); }
    info->projected_cols.push_back(pc);

    sirius::logical_type t;
    if (k < scan_op.types.size()) {
      t = scan_op.types[k];
    } else {
      t = scan_op.returned_types.at(col_idx.GetPrimaryIndex());
    }
    info->projected_types.push_back(t);
  }

  // Filters drive row-group pruning in the metadata walk and post-decode filtering.
  if (scan_op.table_filters) {
    info->table_filters = duckdb::make_uniq<duckdb::TableFilterSet>();
    for (auto& [col_idx, filt] : scan_op.table_filters->filters) {
      info->table_filters->filters[col_idx] = filt->Copy();
    }
  }
  info->column_ids     = scan_op.column_ids;
  info->projection_ids = scan_op.projection_ids;
  info->returned_types = scan_op.returned_types;
  info->output_types   = scan_op.types;
  return info;
}

class native_source_adapter final : public detail::factory_source_adapter {
 public:
  native_source_adapter()
    : factory_source_adapter({source_kind::native,
                              "duckdb.native.v1",
                              dynamic_filter_mode::post_decode,
                              byte_source_class::native_storage,
                              true,
                              false,
                              true,
                              scan_runtime_form::ingestible,
                              true},
                             duckdb::TableScanFunction::GetFunction())
  {
  }

  void declare_resources(query_scan_registry& window,
                         duckdb::ClientContext& context,
                         const binding_ref& binding) const override
  {
    const auto& bind = static_cast<const duckdb::TableScanBindData&>(*binding.data);
    window.declare_native(context, bind.table.Cast<duckdb::DuckTableEntry>().GetStorage());
  }

  bool valid_payload(const binding_ref& binding) const override
  {
    const auto* bind = dynamic_cast<const duckdb::TableScanBindData*>(binding.data);
    return bind && typeid(*bind) == typeid(duckdb::TableScanBindData) &&
           typeid(bind->table) == typeid(duckdb::DuckTableEntry) && !bind->is_index_scan &&
           !bind->is_create_index &&
           &bind->table.ParentCatalog().GetDatabase() == &binding.database;
  }

  read_view_capture try_capture_bound_view(const capture_request& request) const override
  {
    const auto& bind = static_cast<const duckdb::TableScanBindData&>(*request.binding.data);
    std::string key;
    detail::identity_number(key, "catalog_oid", bind.table.ParentCatalog().GetOid());
    detail::identity_number(key, "table_oid", bind.table.oid);
    detail::identity_field(key, "catalog", bind.table.ParentCatalog().GetName());
    detail::identity_field(key, "schema_name", bind.table.ParentSchema().name);
    detail::identity_field(key, "table", bind.table.name);
    return detail::complete_read_view(
      request, profile(), detail::capture_schema(request), std::move(key));
  }

  source_preflight_result preflight_source(const source_preflight_request& request) const override
  {
    const auto& native_bind = static_cast<const duckdb::TableScanBindData&>(*request.get.bind_data);
    auto& native_storage    = native_bind.table.Cast<duckdb::DuckTableEntry>().GetStorage();
    duckdb::DuckTransaction::Get(request.context, native_storage.GetAttached());
    // The early pin/codec probe borrows storage only within this scope. The final runtime
    // acquires the shared window lease after all providers finish their metadata SQL.
    native_checkpoint_lease probe(
      native_storage, 0, source_registry::get(*request.context.db).counters);
    auto& op                                 = request.get;
    auto& context                            = request.context;
    auto* sirius_state                       = request.sirius_state;
    auto column_ids                          = op.GetColumnIds();
    const scan_manager::pinned_entry* pinned = nullptr;
    bool serves_insert_deltas                = false;
    if (sirius_state) {
      auto* bind = dynamic_cast<duckdb::TableScanBindData*>(op.bind_data.get());
      if (bind != nullptr && bind->table.IsDuckTable()) {
        auto& table = bind->table.Cast<duckdb::DuckTableEntry>();
        pinned      = sirius_state->get_scan_manager().find_pinned_entry_for_duckdb_table(
          table.ParentCatalog().GetName(),
          table.ParentSchema().name,
          table.name,
          &column_ids,
          &op.returned_types);
        // Rows beyond the pinned prefix serve as insert-delta splits, decoded fresh at native
        // width. A narrow sidecar over them would pay per-batch exact-range verification and, on
        // an out-of-range inserted value, fail the query over to the CPU fallback — so the
        // residency gate below installs no narrow targets for a delta-serving scan. Entry chunks
        // and their storage metadata are untouched by deltas. See issue ticket #1311.
        if (pinned != nullptr && pinned->mvcc != nullptr &&
            static_cast<std::size_t>(table.GetStorage().GetTotalRows()) > pinned->mvcc->n_cache()) {
          serves_insert_deltas = true;
        }
      }
    }
    // Plan-time probe for the duckdb-native seq_scan path: strings at/over
    // StringUncompressed::GetStringBlockLimit (a per-value limit) live in overflow
    // blocks the GPU string decoder cannot resolve. Refuse HERE, where the throw still
    // becomes a clean CPU fallback — the walker's refusal at pipeline conversion
    // surfaces as a mid-query error with none. Conservative for DICT_FSST, which
    // inlines strings up to 16 KiB (see prepare_duckdb_native_walk).
    if (op.bind_data) {
      auto* table_scan_bind = dynamic_cast<duckdb::TableScanBindData*>(op.bind_data.get());
      if (table_scan_bind != nullptr && table_scan_bind->table.IsDuckTable()) {
        auto& table   = table_scan_bind->table.Cast<duckdb::DuckTableEntry>();
        auto& storage = table.GetStorage();
        auto const block_size =
          storage.GetAttached().GetStorageManager().GetBlockManager().GetBlockSize();
        auto const overflow_limit = duckdb::StringUncompressed::GetStringBlockLimit(block_size);
        for (auto const& col_idx : column_ids) {
          if (!col_idx.HasPrimaryIndex() || col_idx.IsRowIdColumn() || col_idx.IsVirtualColumn() ||
              col_idx.IsEmptyColumn()) {
            continue;
          }
          auto const primary = col_idx.GetPrimaryIndex();
          if (primary >= op.returned_types.size() ||
              op.returned_types[primary].id() != duckdb::LogicalTypeId::VARCHAR) {
            continue;
          }
          auto stats = table.GetStatistics(context, primary);
          if (!stats || !duckdb::StringStats::HasMaxStringLength(*stats) ||
              duckdb::StringStats::MaxStringLength(*stats) >= overflow_limit) {
            throw duckdb::NotImplementedException(
              "duckdb-native scan: varchar column %llu may contain strings at/over the "
              "overflow-block limit (%llu bytes); overflow strings are not GPU-decodable",
              static_cast<unsigned long long>(primary),
              static_cast<unsigned long long>(overflow_limit));
          }
        }

        // Sentinel columns (rowid/virtual/empty/field) have no storage backing,
        // which makes a pin unservable for this scan.
        bool has_unservable_column = false;
        for (auto const& col_idx : column_ids) {
          if (!col_idx.HasPrimaryIndex() || col_idx.IsRowIdColumn() || col_idx.IsVirtualColumn() ||
              col_idx.IsEmptyColumn()) {
            has_unservable_column = true;
            break;
          }
        }

        // Plan and serve must judge recorded types against the scan's the same way. Treat a
        // mismatched pin as unpinned so the fresh disk path remains available.
        if (pinned != nullptr && pinned->mvcc != nullptr &&
            sirius::scan_manager::pinned_native_types_match_columns(
              *pinned, column_ids, op.returned_types)) {
          // Cache-or-CPU guards: while this table is MVCC-pinned, a GPU plan
          // either serves exactly from the pinned cache (DELETE keep-masks) or is
          // refused HERE, where the throw still becomes a clean CPU fallback. The
          // disk-native path is MVCC-blind, and the pin's checkpoint suppression
          // makes its snapshot increasingly stale — so scans the pin cannot serve
          // never fall through to it.

          // (a) snapshot-too-old: this transaction opened before the pin, so
          // the cache's base image is from its future.
          auto const start_time =
            duckdb::DuckTransaction::Get(context, table.ParentCatalog()).start_time;
          if (start_time < pinned->mvcc->v_base) {
            throw duckdb::NotImplementedException(
              "duckdb-native scan: transaction snapshot (%llu) predates the pinned cache "
              "snapshot (%llu) for table '%s'",
              static_cast<unsigned long long>(start_time),
              static_cast<unsigned long long>(pinned->mvcc->v_base),
              table.name);
          }
          // (b) transaction-local appends: rows in this transaction's
          // LocalStorage live outside the table's segment trees, so neither the
          // cache nor the insert delta can serve them. Committed rows beyond
          // the pinned prefix are served by the prepare-time insert-delta job,
          // masked to this snapshot's visibility.
          if (duckdb::LocalStorage::Get(context, storage.GetAttached()).GetStorage(storage)) {
            throw duckdb::NotImplementedException(
              "duckdb-native scan: table '%s' has uncommitted appends in this transaction; "
              "transaction-local inserts are not served from the cache",
              table.name);
          }
          bool pin_serves = !has_unservable_column;
          if (pin_serves && !column_ids.empty()) {
            pin_serves = !pinned->cache_info.column_projection_for(column_ids).empty();
          }
          if (!pin_serves) {
            // (d) column-mismatch: the scan would fall through to the MVCC-blind
            // disk-native read, so it always declines. The disabled clean-table
            // relaxation below lets a table that provably matches its
            // last-checkpointed image fall through instead (#1160).
            throw duckdb::NotImplementedException(
              "duckdb-native scan: table '%s' is MVCC-pinned and the pin cannot serve the "
              "requested columns",
              table.name);
          }
#if 0
        // Disabled — these guards walk every row group of the table at plan
        // time, per query. With this block off, (d) above has no clean-table
        // relaxation. The update-chain branch is redundant because UPDATE
        // statements on pinned tables are rejected before execution; direct
        // UPDATE serving remains out of scope here (#1162).
        auto const n_cache = pinned->mvcc->n_cache();
        std::vector<duckdb::storage_t> projected;
        for (auto const& col_idx : column_ids) {
          if (col_idx.HasPrimaryIndex() && !col_idx.IsRowIdColumn() &&
              !col_idx.IsVirtualColumn() && !col_idx.IsEmptyColumn()) {
            projected.push_back(col_idx.GetPrimaryIndex());
          }
        }
        if (!pin_serves) {
          // (d) column-mismatch: the scan falls through to the disk-native
          // read, so it must pass the same exactness check as an unpinned
          // scan. Guards (a)/(b) already excluded post-pin inserts and
          // transaction-local appends.
          auto& txn = duckdb::DuckTransaction::Get(context, table.ParentCatalog());
          if (sirius::op::scan::check_native_read_mvcc_state(
                storage, projected, duckdb::TransactionData(txn)) !=
              sirius::op::scan::native_read_mvcc_state::exact) {
            throw duckdb::NotImplementedException(
              "duckdb-native scan: table '%s' is MVCC-pinned, the pin cannot serve the "
              "requested columns, and the table has diverged from its last-checkpointed "
              "image",
              table.name);
          }
        } else if (sirius::op::scan::any_update_chains(storage, projected, n_cache)) {
          // (c) update-present on a column the cache would serve: update
          // chains version values in place, invisibly to the DELETE
          // keep-masks — the cached values would be stale.
          throw duckdb::NotImplementedException(
            "duckdb-native scan: table '%s' has in-memory update chains on a scanned "
            "column; updated values are not served from the cache",
            table.name);
        }
#endif
        }
#if 0
      // Disabled — plan-time MVCC guards for duckdb-native scans of unpinned
      // tables: the exactness walk touches every row group of the table, per
      // query; #1160 tracks running it from the scan manager at execution
      // time instead. With this block off, the disk-native read of an
      // unpinned table is MVCC-blind (#1143): uncheckpointed deletes and
      // update chains are served silently, and committed-but-uncheckpointed
      // inserts fail loudly at execution.
      if (pinned == nullptr || pinned->mvcc == nullptr) {
        // No MVCC-pinned cache for this table: the plan is the disk-native
        // read, which applies no visibility filtering — refuse any state it
        // would misread HERE, where the throw still becomes a clean CPU
        // fallback. Residual race: a row committing between this check and
        // the scan's metadata capture is read unmasked; the prepare-time
        // keep-masks planned in #1143 close it.
        std::vector<duckdb::storage_t> projected;
        for (auto const& col_idx : column_ids) {
          if (col_idx.HasPrimaryIndex() && !col_idx.IsRowIdColumn() &&
              !col_idx.IsVirtualColumn() && !col_idx.IsEmptyColumn()) {
            projected.push_back(col_idx.GetPrimaryIndex());
          }
        }
        if (duckdb::LocalStorage::Get(context, storage.GetAttached()).GetStorage(storage)) {
          throw duckdb::NotImplementedException(
            "duckdb-native scan: table '%s' has uncommitted appends in this transaction; "
            "the disk-native read cannot see transaction-local rows",
            table.name);
        }
        auto& txn = duckdb::DuckTransaction::Get(context, table.ParentCatalog());
        switch (sirius::op::scan::check_native_read_mvcc_state(
          storage, projected, duckdb::TransactionData(txn))) {
          case sirius::op::scan::native_read_mvcc_state::has_update_chains:
            throw duckdb::NotImplementedException(
              "duckdb-native scan: table '%s' has in-memory update chains on a scanned "
              "column; the disk-native read would return stale values",
              table.name);
          case sirius::op::scan::native_read_mvcc_state::has_invisible_rows:
            throw duckdb::NotImplementedException(
              "duckdb-native scan: table '%s' has rows not visible to this transaction "
              "(uncheckpointed deletes or in-flight inserts); the disk-native read is "
              "MVCC-blind",
              table.name);
          case sirius::op::scan::native_read_mvcc_state::exact: break;
        }
      }
#endif
      }
    }

    return {pinned, serves_insert_deltas};
  }

  scan_runtime_handle create_scan_runtime(const runtime_build_request& request) const override
  {
    auto info =
      build_duckdb_native_table_info(request.physical(), request.parameters(), request.context);
    if (!request.window) {
      throw duckdb::InternalException("Native runtime requires a construction window");
    }
    info->checkpoint_lease = request.window->lease_for(*info->storage);
    return detail::make_ingestible_runtime(std::move(info), request);
  }
  source_policy_evidence inspect_source(const binding_ref&) const override { return {}; }
};
}  // namespace

std::unique_ptr<scan_source_adapter> make_native_source_adapter()
{
  return std::make_unique<native_source_adapter>();
}
}  // namespace sirius::scan
