/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "ingestible_support.hpp"
#include "scan/source_factories.hpp"
#include "sirius_extension.hpp"

#include <typeinfo>

namespace sirius::scan {
namespace {
class owned_parquet_source_adapter final : public detail::factory_source_adapter {
 public:
  owned_parquet_source_adapter()
    : factory_source_adapter({source_kind::owned_parquet,
                              "sirius.parquet.v1",
                              dynamic_filter_mode::reader,
                              byte_source_class::file_inventory,
                              false,
                              false},
                             duckdb::sirius_parquet_scan_function())
  {
  }
  bool valid_payload(const binding_ref& binding) const override
  {
    return binding.data && typeid(*binding.data) == typeid(duckdb::SiriusReadParquetBindData);
  }
  read_view_capture try_capture_bound_view(const capture_request& request) const override
  {
    const auto& bind = static_cast<const duckdb::SiriusReadParquetBindData&>(*request.binding.data);
    if (!bind.schema) {
      return {request.instance, request.generation, request.origin, capture_status::unavailable};
    }
    const auto schema = detail::capture_schema(request);
    if (!bind.schema->canonical_identity() || !bind.schema->equals(*schema)) {
      return {request.instance, request.generation, request.origin, capture_status::unsupported};
    }
    std::string key;
    detail::identity_field(key, "uri", bind.uri);
    return detail::complete_read_view(request, profile(), bind.schema, std::move(key));
  }
  source_preflight_result preflight_source(const source_preflight_request& request) const override
  {
    return detail::parquet_pin_preflight(request, [&] {
      return std::vector<std::string>{
        static_cast<const duckdb::SiriusReadParquetBindData&>(*request.get.bind_data).uri};
    });
  }
  scan_runtime_handle create_scan_runtime(const runtime_build_request& request) const override
  {
    auto& scan       = request.physical();
    const auto& bind = static_cast<const duckdb::SiriusReadParquetBindData&>(*scan.bind_data);
    auto info        = std::make_unique<op::scan::parquet_ingestible_table_info>();
    detail::populate_parquet_table_info(*info, scan, request.parameters(), {bind.uri});
    return detail::make_ingestible_runtime(std::move(info), request);
  }
  source_policy_evidence inspect_source(const binding_ref& binding) const override
  {
    const auto& bind = static_cast<const duckdb::SiriusReadParquetBindData&>(*binding.data);
    return {true, detail::is_s3(bind.uri)};
  }
};
}  // namespace
std::unique_ptr<scan_source_adapter> make_owned_parquet_source_adapter()
{
  return std::make_unique<owned_parquet_source_adapter>();
}
}  // namespace sirius::scan
