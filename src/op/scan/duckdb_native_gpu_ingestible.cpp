/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// sirius
#include "op/scan/owning_table_view.hpp"
#include "scan_manager/sirius_scan_manager.hpp"

#include <expression/ast/from_duckdb.hpp>
#include <expression_evaluator/expression_evaluator.hpp>
#include <helper/utils.hpp>
#include <io/io_context.hpp>
#include <io/sirius_datasource.hpp>
#include <log/logging.hpp>
#include <op/dynamic_filter/sirius_dynamic_filter.hpp>
#include <op/scan/duckdb_native_decoder.hpp>
#include <op/scan/duckdb_native_gpu_ingestible.hpp>
#include <op/scan/scan_plan.hpp>
#include <op/scan/scan_utils.hpp>
#include <op/scan/sirius_gpu_scan_operator_data.hpp>

// duckdb
#include "scan/source_registry.hpp"

#include <duckdb/storage/single_file_block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction.hpp>

// cudf
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/memory_resource.hpp>

// cucascade
#include <cucascade/memory/memory_space.hpp>

// standard library
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <utility>
#include <vector>

namespace sirius::op::scan {

namespace {

//===----------Batch Coalescer----------===//
class duckdb_native_batch_coalescer : public batch_coalescer {
 public:
  duckdb_native_batch_coalescer(std::size_t approximate_batch_size, std::vector<bool> is_varchar)
    : _cap(approximate_batch_size),
      _is_varchar(std::move(is_varchar)),
      _any_varchar(std::find(_is_varchar.begin(), _is_varchar.end(), true) != _is_varchar.end()),
      _col_bytes(_is_varchar.size(), 0)
  {
  }

  /// @brief Add a new scan info to the coalescer. The input granularity is a single scan info from
  /// a metadata parse task (a chunk of row groups). The output is a vector of scan infos that have
  /// been coalesced, each of which represents the metadata for a data batch.
  std::vector<std::unique_ptr<scan_info>> push(std::unique_ptr<scan_info> info) override
  {
    std::vector<std::unique_ptr<scan_info>> emitted;
    auto* scan_info = dynamic_cast<duckdb_native_scan_info*>(info.get());
    if (scan_info == nullptr) {
      throw std::runtime_error("Native coalescer received non-native metadata");
    }
    if (scan_info->consumer) {
      scan_info->validate_slices(scan_info->consumer);
      if (_consumer && _consumer != scan_info->consumer) {
        sirius::scan::certificate_failure(_consumer, "coalescing_consumers");
      }
      _consumer = scan_info->consumer;
      _lease    = scan_info->checkpoint_lease;
    } else if (_consumer) {
      sirius::scan::certificate_failure(_consumer, "missing_consumer");
    }

    // Each metadata task may open a distinct byte-source object for the same file.
    // Keep each certificate attached to the object used by its resulting split.
    if (_have_template && _datasource && scan_info->datasource &&
        &_datasource->io_object() != &scan_info->datasource->io_object()) {
      if (!_acc.empty()) { emitted.push_back(emit_current()); }
      _have_template = false;
    }
    if (!_have_template) {
      _datasource    = scan_info->datasource;
      _block_manager = scan_info->block_manager;
      _have_template = true;
    }

    for (auto& rg : scan_info->row_groups) {
      if (rg.row_count == 0) { continue; }

      auto const rg_bytes = rg.decoded_bytes_budget;
      if (!_acc.empty()) {
        bool const exceed_total = (_cap > 0) && (_acc_bytes + rg_bytes > _cap);
        bool exceed_varchar     = false;
        if (_any_varchar) {
          for (std::size_t c = 0; c < _is_varchar.size(); ++c) {
            if (_is_varchar[c] &&
                _col_bytes[c] + rg.varchar_bytes_per_col[c] >= kCudfInt32StringsThreshold) {
              exceed_varchar = true;
              break;
            }
          }
        }
        if (exceed_total || exceed_varchar) { emitted.push_back(emit_current()); }
      }

      _acc_bytes += rg_bytes;
      if (_any_varchar) {
        for (std::size_t c = 0; c < _is_varchar.size(); ++c) {
          _col_bytes[c] += rg.varchar_bytes_per_col[c];
        }
      }
      _acc.push_back(std::move(rg));
    }
    return emitted;
  }

  std::vector<std::unique_ptr<scan_info>> flush() override
  {
    std::vector<std::unique_ptr<scan_info>> out;
    if (!_acc.empty()) { out.push_back(emit_current()); }
    // Whole scan coalesced to nothing (every row group empty or stats-pruned): emit
    // one empty split so the scan still creates a task. decode_duckdb_native_split
    // turns an empty row-group list into a schema-correct 0-row table. Without this,
    // zero splits mean zero tasks and the pipeline-completion signal never fires.
    if (!_produced_any && _have_template) {
      auto split              = std::make_unique<duckdb_native_scan_info>();
      split->datasource       = _datasource->duplicate();
      split->block_manager    = _block_manager;
      split->consumer         = _consumer;
      split->checkpoint_lease = _lease;
      _produced_any           = true;
      out.push_back(std::move(split));
    }
    return out;
  }

 private:
  std::unique_ptr<scan_info> emit_current()
  {
    auto split              = std::make_unique<duckdb_native_scan_info>();
    split->consumer         = _consumer;
    split->checkpoint_lease = _lease;
    split->row_groups       = std::move(_acc);
    split->datasource       = _datasource->duplicate();
    split->block_manager    = _block_manager;
    _acc.clear();
    _acc_bytes = 0;
    std::fill(_col_bytes.begin(), _col_bytes.end(), 0);
    _produced_any = true;
    return split;
  }

  sirius::scan::bound_table_scan_ptr _consumer;
  std::shared_ptr<sirius::scan::native_checkpoint_lease> _lease;
  const std::size_t _cap;
  const std::vector<bool> _is_varchar;
  const bool _any_varchar;

  std::vector<std::size_t> _col_bytes;
  std::vector<duckdb_row_group_metadata> _acc;
  std::size_t _acc_bytes = 0;

  bool _have_template = false;
  bool _produced_any  = false;
  std::shared_ptr<sirius::io::sirius_datasource> _datasource;
  duckdb::SingleFileBlockManager const* _block_manager = nullptr;
};

}  // namespace

//===----------------------------------------------------------------------===//
// duckdb_native_gpu_ingestible — construction
//===----------------------------------------------------------------------===//
duckdb_native_gpu_ingestible::duckdb_native_gpu_ingestible(
  std::unique_ptr<duckdb_native_ingestible_table_info> info)
  : _info(std::move(info))
{
  auto const& bind = *_info;
  if (bind.storage == nullptr) {
    throw std::invalid_argument(
      "[duckdb_native_gpu_ingestible] table_info.storage must be non-null");
  }
  if (bind.context == nullptr) {
    throw std::invalid_argument(
      "[duckdb_native_gpu_ingestible] table_info.context must be non-null");
  }
  if (bind.projected_cols.size() != bind.projected_types.size()) {
    throw std::invalid_argument(
      "[duckdb_native_gpu_ingestible] projected_cols and projected_types must be parallel");
  }

  _info->storage_owner = bind.storage->shared_from_this();
  if (auto reason = unsupported_projected_type_reason(bind.projected_cols, bind.projected_types)) {
    throw std::runtime_error("duckdb-native scan rejected query: " + *reason);
  }

  auto& sm          = bind.storage->GetAttached().GetStorageManager();
  auto const* sf_bm = dynamic_cast<duckdb::SingleFileBlockManager const*>(&sm.GetBlockManager());
  if (sf_bm == nullptr) {
    throw std::runtime_error(
      "[prepare_duckdb_native_walk] duckdb-native scan rejected query: requires a single-file "
      "block manager");
  }
  _block_manager = sf_bm;

  duckdb::vector<duckdb::idx_t> source_ids_fallback;
  if (bind.projection_ids.empty()) {
    source_ids_fallback.reserve(bind.column_ids.size());
    for (duckdb::idx_t i = 0; i < bind.column_ids.size(); ++i) {
      source_ids_fallback.push_back(i);
    }
  }
  auto const& source_ids = bind.projection_ids.empty() ? source_ids_fallback : bind.projection_ids;

  // Pre-build the coalesced filter expression once.
  if (bind.table_filters && !bind.table_filters->filters.empty()) {
    std::vector<std::optional<std::size_t>> emission_order_map(bind.column_ids.size());
    for (std::size_t k = 0; k < source_ids.size(); ++k) {
      emission_order_map[source_ids[k]] = k;
    }

    auto filter_expr_duckdb = sirius::op::convert_table_filters_to_expression(
      *bind.table_filters, bind.column_ids, bind.returned_types, emission_order_map);
    if (filter_expr_duckdb) {
      _filter_expression = std::shared_ptr<duckdb::Expression>(filter_expr_duckdb.release());
    }
  }

  _chunk_row_groups = metadata_parse_chunk();
}

void duckdb_native_gpu_ingestible::prepare_dependencies(
  sirius::scan_manager::sirius_scan_manager& manager)
{
  auto lease = manager.checkpoint_lease_for(*_info->storage);
  if (_info->checkpoint_lease && _info->checkpoint_lease != lease) {
    throw std::runtime_error("Sirius scan contract: native execution window changed");
  }
  _info->checkpoint_lease = std::move(lease);
  _block_manager          = _info->checkpoint_lease->block_manager();
  _info->context_owner    = _info->context->shared_from_this();
}

void duckdb_native_gpu_ingestible::validate_dependencies() const
{
  if (!_info->checkpoint_lease) {
    throw std::runtime_error("Sirius scan contract: native checkpoint key not held");
  }
  _info->checkpoint_lease->validate(*_info->storage);
}

void duckdb_native_gpu_ingestible::ensure_metadata_prepared()
{
  if (_walk_ready.load(std::memory_order_acquire)) { return; }
  std::call_once(_walk_once, [this] {
    validate_dependencies();
    _block_manager       = _info->checkpoint_lease->block_manager();
    const auto& bind     = *_info;
    _info->context_owner = bind.context->shared_from_this();
    duckdb::Value inject;
    if (bind.context->TryGetCurrentSetting("sirius_test_inject_native_walk_failure", inject) &&
        !inject.IsNull() && inject.GetValue<bool>()) {
      throw duckdb::ExecutorException("injected native metadata walk failure");
    }
    // Partition statistics use ClientContext/LocalStorage; prepare runs on the query thread.
    auto plan = prepare_duckdb_native_walk(*bind.storage,
                                           *bind.context,
                                           bind.projected_cols,
                                           bind.projected_types,
                                           bind.table_filters.get(),
                                           &bind.column_ids);
    if (!plan.viable) {
      throw std::runtime_error("duckdb-native scan rejected query: " +
                               plan.viability_failure_reason);
    }
    _plan = std::move(plan);
    _num_ranges.store(
      std::max<std::size_t>(1, utils::ceil_div(_plan.n_row_groups, _chunk_row_groups)),
      std::memory_order_relaxed);
    _walk_ready.store(true, std::memory_order_release);
  });
}

std::uint64_t duckdb_native_gpu_ingestible::checkpoint_iteration() const
{
  if (metadata_walk_pending()) {
    throw std::runtime_error("Sirius native metadata has not been prepared");
  }
  validate_dependencies();
  return _info->checkpoint_lease->iteration();
}

duckdb_native_gpu_ingestible::~duckdb_native_gpu_ingestible() = default;

//===----------------------------------------------------------------------===//
// split-provider interface
//===----------------------------------------------------------------------===//
bool duckdb_native_gpu_ingestible::has_processed_all_metadata() const
{
  return _walk_ready.load(std::memory_order_acquire) &&
         _next_range_idx.load(std::memory_order_relaxed) >=
           _num_ranges.load(std::memory_order_relaxed);
}

duckdb_native_gpu_ingestible::metadata_scan_task_t
duckdb_native_gpu_ingestible::next_split_provider(io::ioctx_resolver resolve)
{
  ensure_metadata_prepared();
  auto const idx = _next_range_idx.fetch_add(1, std::memory_order_relaxed);
  if (idx >= _num_ranges.load(std::memory_order_relaxed)) { return nullptr; }

  auto const rg_begin = idx * _chunk_row_groups;
  auto const rg_end   = std::min(rg_begin + _chunk_row_groups, _plan.n_row_groups);

  // All ranges read the one `.duckdb` file; the resolver returns a valid ioctx or
  // throws if no backend supports the path.
  auto io_ctx = resolve(_info->db_path);
  // Runs on a scan-manager dispatcher thread:
  return [this, rg_begin, rg_end, io_ctx = std::move(io_ctx)]() -> std::unique_ptr<scan_info> {
    validate_dependencies();
    auto range = walk_duckdb_native_row_group_range(_plan, rg_begin, rg_end);
    if (!range.viable) {
      throw std::runtime_error("duckdb-native scan rejected query (range [" +
                               std::to_string(rg_begin) + ", " + std::to_string(rg_end) +
                               ")): " + range.viability_failure_reason);
    }
    auto split           = std::make_unique<duckdb_native_scan_info>();
    split->row_groups    = std::move(range.row_groups);
    split->datasource    = io_ctx->open_datasource(_info->db_path);
    split->block_manager = _block_manager;
    certify(*split);
    return split;
  };
}

//===----------------------------------------------------------------------===//
// materialize_table
//===----------------------------------------------------------------------===//
namespace {
bool same_segments(const std::vector<duckdb_segment_descriptor>& a,
                   const std::vector<duckdb_segment_descriptor>& b)
{
  if (a.size() != b.size()) { return false; }
  for (std::size_t i = 0; i < a.size(); ++i) {
    const auto& x = a[i];
    const auto& y = b[i];
    if (x.block_id != y.block_id || x.segment_start != y.segment_start ||
        x.segment_count != y.segment_count || x.compression != y.compression ||
        x.additional_blocks != y.additional_blocks || x.host_ptr != y.host_ptr ||
        x.bytes_size != y.bytes_size || x.max_string_length != y.max_string_length ||
        x.all_null != y.all_null || x.segment_stats != y.segment_stats ||
        (x.block_id >= 0 && x.block_offset != y.block_offset)) {
      return false;
    }
  }
  return true;
}
bool same_native_range(const duckdb_row_group_metadata& a, const duckdb_row_group_metadata& b)
{
  if (a.row_group_index != b.row_group_index || a.row_group_start != b.row_group_start ||
      a.row_count != b.row_count || a.columns.size() != b.columns.size()) {
    return false;
  }
  for (std::size_t i = 0; i < a.columns.size(); ++i) {
    const auto& x = a.columns[i];
    const auto& y = b.columns[i];
    if (x.is_rowid != y.is_rowid || x.is_array != y.is_array) { return false; }
    if (x.is_rowid) { continue; }
    if (x.column_id != y.column_id || !same_segments(x.data_segments, y.data_segments) ||
        !same_segments(x.validity_segments, y.validity_segments) ||
        !same_segments(x.array_child_data_segments, y.array_child_data_segments) ||
        !same_segments(x.array_child_validity_segments, y.array_child_validity_segments)) {
      return false;
    }
  }
  return true;
}
}  // namespace

void duckdb_native_gpu_ingestible::certify(duckdb_native_scan_info& split) const
{
  validate_dependencies();
  split.consumer         = scan_contract();
  split.checkpoint_lease = _info->checkpoint_lease;
  if (!split.consumer) { return; }  // Direct pin population has no query consumer.
  for (auto& group : split.row_groups) {
    if (group.certificate) { sirius::scan::certificate_failure(split.consumer, "recertification"); }
    group.certificate = std::make_shared<const sirius::scan::native_slice_certificate>(
      sirius::scan::native_slice_certificate{
        split.consumer,
        split.checkpoint_lease,
        _info->storage_owner,
        std::make_shared<const duckdb_row_group_metadata>(group),
        split.datasource,
        split.staging_keepalive});
  }
}

void duckdb_native_scan_info::validate_slices(
  const sirius::scan::bound_table_scan_ptr& expected) const
{
  sirius::scan::validate_consumer(expected, consumer);
  if (!checkpoint_lease || checkpoint_lease->block_manager() != block_manager) {
    sirius::scan::certificate_failure(expected, "native_lease");
  }
  checkpoint_lease->validate();
  for (const auto& group : row_groups) {
    const auto& cert = group.certificate;
    if (!cert || cert->consumer != expected || cert->lease != checkpoint_lease || !cert->storage ||
        !cert->layout || !same_native_range(group, *cert->layout) ||
        (cert->datasource &&
         (!datasource || &cert->datasource->io_object() != &datasource->io_object()))) {
      sirius::scan::certificate_failure(expected, "native_range_or_dependencies");
    }
    cert->lease->validate(*cert->storage);
  }
}

filtered_table duckdb_native_gpu_ingestible::materialize_metadata_to_table(
  scan_info const& info,
  ::cucascade::memory::memory_space const& mem_space,
  rmm::cuda_stream_view stream,
  bool /*like_swar_fastpath*/,
  std::shared_ptr<const like_multiliteral_cache> /*like_cache*/)
{
  validate_dependencies();
  if (scan_contract()) { info.validate_slices(scan_contract()); }
  auto const& split = static_cast<duckdb_native_scan_info const&>(info);
  if (!split.datasource && !split.host_backed_only) {
    throw std::runtime_error("[duckdb_native_gpu_ingestible] scan_info has no datasource");
  }
  auto& mem_space_mut = const_cast<::cucascade::memory::memory_space&>(mem_space);
  auto table          = decode_duckdb_native_split(
    split.row_groups, *_info, split.datasource.get(), mem_space_mut, stream);
  SIRIUS_LOG_DEBUG(
    "[duckdb_native_gpu_ingestible::materialize_table] decoded split: row_groups={} rows={} "
    "cols={}",
    split.row_groups.size(),
    table->num_rows(),
    table->num_columns());
  // duckdb-native applies filter + projection inside post_filter_and_project,
  // never during materialization — always UNFILTERED here.
  return filtered_table{.table = owning_table_view{std::move(table)},
                        .state = filter_state::UNFILTERED};
}

//===----------------------------------------------------------------------===//
// batch_coalescer
//===----------------------------------------------------------------------===//
std::unique_ptr<batch_coalescer> duckdb_native_gpu_ingestible::create_batch_coalescer() const
{
  std::vector<bool> is_varchar;
  is_varchar.reserve(_info->projected_types.size());
  for (auto const& t : _info->projected_types) {
    is_varchar.push_back(t.is_varchar());
  }
  return std::make_unique<duckdb_native_batch_coalescer>(_info->approximate_batch_size,
                                                         std::move(is_varchar));
}

//===----------------------------------------------------------------------===//
// post_filter_and_project — filter eval + projection to output arity
//===----------------------------------------------------------------------===//

namespace {

/// Positions of `width` minus `elided`, ascending. Empty when nothing would be
/// left: a zero-column table carries no row count, and the rowid needs one.
std::vector<std::size_t> kept_positions(std::size_t width, std::span<std::size_t const> elided)
{
  if (elided.empty() || elided.size() >= width) { return {}; }
  std::vector<std::size_t> kept;
  kept.reserve(width - elided.size());
  for (std::size_t pos = 0; pos < width; ++pos) {
    if (std::find(elided.begin(), elided.end(), pos) == elided.end()) { kept.push_back(pos); }
  }
  return kept;
}

}  // namespace

std::unique_ptr<cudf::table> duckdb_native_gpu_ingestible::post_filter_and_project(
  filtered_table&& input,
  ::cucascade::memory::memory_space const& mem_space,
  rmm::cuda_stream_view stream,
  bool like_swar_fastpath,
  std::shared_ptr<const like_multiliteral_cache> like_cache,
  std::unique_ptr<cudf::column>* /*survivors*/,
  std::span<std::size_t const> elided)
{
  auto const output_arity = _info->output_types.size();
  auto const decoded_cols =
    _info->projection_ids.empty() ? _info->column_ids.size() : _info->projection_ids.size();
  auto const projection_required = (output_arity > 0) && (decoded_cols > output_arity);

  rmm::device_async_resource_ref mr_ref(mem_space.get_default_allocator());

  //===----------Filter Evaluation----------===//
  // A ROW_FILTERED state means the decode already applied the whole conjunction
  // and compacted to the survivors, so only the projection below is left.
  owning_table_view final_table;
  if (_filter_expression && input.state != filter_state::ROW_FILTERED) {
    auto sirius_filter_ast = sirius::ast::from_duckdb(*_filter_expression);
    sirius::expression_evaluator exec(sirius_filter_ast.get(),
                                      mr_ref,
                                      stream,
                                      strategy_from_config(),
                                      sirius::expression_evaluator::default_min_ast_size,
                                      like_swar_fastpath,
                                      std::move(like_cache));
    if (projection_required) {
      // Fold the projection into the filter gather so pure-filter columns are never materialized.
      std::vector<cudf::size_type> output_indices(output_arity);
      std::iota(output_indices.begin(), output_indices.end(), cudf::size_type{0});
      final_table = owning_table_view{exec.select(input.table.view(), output_indices)};
    } else {
      // Nothing to project away, or output_arity == 0 (count(*)) — keep all columns.
      final_table = owning_table_view{exec.select(input.table.view())};
    }
    // The select only enqueued its reads; record before input.table's read-lock owner is dropped.
    input.table.record_reader_event(stream);
  } else {
    final_table = std::move(input.table);
  }

  //===----------Projection----------===//
  // No filter was applied, but pure-filter columns may still have been decoded (e.g. a pinned
  // column-superset scan): drop the trailing columns. This is a no-op after the folded filter
  // gather above, which already produced exactly output_arity columns.
  if (projection_required &&
      static_cast<std::size_t>(final_table.view().num_columns()) > output_arity) {
    std::vector<size_t> selected_cols(output_arity);
    std::iota(selected_cols.begin(), selected_cols.end(), 0);
    final_table.select_columns(selected_cols);
  }

  if (auto const kept =
        kept_positions(static_cast<std::size_t>(final_table.view().num_columns()), elided);
      !kept.empty()) {
    final_table.select_columns(kept);
  }
  return final_table.release(stream, mr_ref);
}

//===----------------------------------------------------------------------===//
// materialized_column_order
//===----------------------------------------------------------------------===//
std::vector<std::size_t> duckdb_native_gpu_ingestible::materialized_column_order() const
{
  // The decoder emits columns in source_ids order (projection_ids, or column_ids order when
  // projection is empty) — output columns first, pure-filter columns trailing — which is the
  // layout post_filter_and_project's emission_order_map filter and [0..output_arity)
  // projection assume. Return the corresponding column primary (storage) indices, matching
  // how the pin cache keys columns (ColumnIndex::GetPrimaryIndex).
  auto const& column_ids     = _info->column_ids;
  auto const& projection_ids = _info->projection_ids;
  std::vector<std::size_t> order;
  if (projection_ids.empty()) {
    order.reserve(column_ids.size());
    for (auto const& c : column_ids) {
      order.push_back(c.GetPrimaryIndex());
    }
  } else {
    order.reserve(projection_ids.size());
    for (auto const pid : projection_ids) {
      order.push_back(column_ids[pid].GetPrimaryIndex());
    }
  }
  return order;
}

std::shared_ptr<duckdb_native_gpu_ingestible> make_ingestible(
  std::unique_ptr<duckdb_native_ingestible_table_info> info)
{
  return std::make_shared<duckdb_native_gpu_ingestible>(std::move(info));
}

}  // namespace sirius::op::scan
