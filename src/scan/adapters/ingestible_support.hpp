/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once

#include "adapter_support.hpp"
#include "op/dynamic_filter/sirius_dynamic_filter.hpp"
#include "op/scan/gpu_ingestible.hpp"
#include "op/scan/parquet_gpu_ingestible.hpp"
#include "op/sirius_physical_table_scan.hpp"
#include "scan_manager/sirius_scan_manager.hpp"
#include "sirius_context.hpp"

#include <duckdb/planner/operator/logical_get.hpp>

namespace sirius::scan::detail {

template <typename ResolvePaths>
source_preflight_result parquet_pin_preflight(const source_preflight_request& request,
                                              ResolvePaths&& resolve)
{
  if (!request.sirius_state || !request.compressed_materialization) { return {}; }
  const auto paths = resolve();
  if (paths.empty()) { return {}; }
  return {request.sirius_state->get_scan_manager().find_pinned_entry_for_parquet_files(paths),
          false};
}

// Common Parquet carrier fields; provider-owned partition/path/visibility data stays in adapters.
void populate_parquet_table_info(op::scan::parquet_ingestible_table_info&,
                                 op::sirius_physical_table_scan&,
                                 const operator_params&,
                                 std::vector<std::string> paths);

template <typename Info>
scan_runtime_handle make_ingestible_runtime(std::unique_ptr<Info> info,
                                            const runtime_build_request& request)
{
  auto filters = request.physical().sirius_dynamic_filters;
  if (filters && !filters->has_producers()) { filters.reset(); }
  info->sirius_dynamic_filters = std::move(filters);
  return scan_runtime_handle(make_ingestible(std::move(info)));
}

}  // namespace sirius::scan::detail
