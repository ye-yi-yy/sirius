/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "ingestible_support.hpp"

#include <duckdb/common/multi_file/multi_file_states.hpp>

namespace sirius::scan {
namespace {
class parquet_source_adapter final : public scan_source_adapter {
 public:
  const source_profile& profile() const noexcept override
  {
    static const source_profile value{source_kind::parquet,
                                      "duckdb.parquet.legacy.unverified",
                                      dynamic_filter_mode::reader,
                                      byte_source_class::file_inventory,
                                      true,
                                      false,
                                      false};
    return value;
  }
  binding_verification verify_binding(const binding_ref& binding) const override
  {
    // TODO(R1 D1): use supported actual factory/bound-option access, without DuckDB patches.
    return (binding.function.name == "parquet_scan" || binding.function.name == "read_parquet") &&
               dynamic_cast<const duckdb::MultiFileBindData*>(binding.data)
             ? binding_verification::compatibility
             : binding_verification::rejected;
  }
  read_view_capture try_capture_bound_view(const capture_request& request) const override
  {
    // TODO(R1 D1/D2): qualify bound options and non-expanding original inventory publication.
    return {request.instance, request.generation, request.origin};
  }
  source_preflight_result preflight_source(const source_preflight_request& request) const override
  {
    return detail::parquet_pin_preflight(request, [&] {
      return detail::legacy_multi_file_paths(request.get.bind_data.get())
        .value_or(std::vector<std::string>{});
    });
  }
  scan_runtime_handle create_scan_runtime(const runtime_build_request& request) const override
  {
    auto& scan = request.physical();
    auto info  = std::make_unique<op::scan::parquet_ingestible_table_info>();
    detail::populate_parquet_table_info(
      *info,
      scan,
      request.parameters(),
      detail::legacy_multi_file_paths(scan.bind_data.get()).value_or(std::vector<std::string>{}));
    info->partition_indices =
      scan.bind_data->Cast<duckdb::MultiFileBindData>().reader_bind.hive_partitioning_indexes;
    return detail::make_ingestible_runtime(std::move(info), request);
  }
  source_policy_evidence inspect_source(const binding_ref& binding) const override
  {
    return detail::inspect_legacy_multi_file_source(binding);
  }
};
}  // namespace
std::unique_ptr<scan_source_adapter> make_parquet_source_adapter()
{
  return std::make_unique<parquet_source_adapter>();
}
}  // namespace sirius::scan
