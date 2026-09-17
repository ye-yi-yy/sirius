/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "adapter_support.hpp"
#include "exec/stream_plan_bindings.hpp"
#include "op/sirius_physical_streaming_source.hpp"

#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include <typeinfo>

namespace sirius::scan {
namespace {
class stream_source_adapter final : public detail::factory_source_adapter {
 public:
  stream_source_adapter()
    : factory_source_adapter({source_kind::stream,
                              "sirius.stream.v1",
                              dynamic_filter_mode::none,
                              byte_source_class::stream,
                              false,
                              false,
                              true,
                              scan_runtime_form::source_operator},
                             exec::stream_source_function_descriptor())
  {
  }
  bool valid_payload(const binding_ref& binding) const override
  {
    const auto* bind = dynamic_cast<const exec::stream_source_bind_data*>(binding.data);
    return bind && typeid(*bind) == typeid(exec::stream_source_bind_data) && bind->declaration;
  }
  read_view_capture try_capture_bound_view(const capture_request& request) const override
  {
    const auto& bind = static_cast<const exec::stream_source_bind_data&>(*request.binding.data);
    const auto& declaration = *bind.declaration;
    const auto schema       = detail::capture_schema(request);
    if (!declaration.schema->canonical_identity() || !declaration.schema->equals(*schema)) {
      return {request.instance, request.generation, request.origin, capture_status::unsupported};
    }
    std::string key;
    detail::identity_number(key, "stream_catalog", declaration.catalog_instance);
    detail::identity_number(key, "stream_id", declaration.stream_id);
    detail::identity_number(key, "declaration_generation", declaration.generation);
    return detail::complete_read_view(request, profile(), declaration.schema, std::move(key));
  }
  source_preflight_result preflight_source(const source_preflight_request&) const override
  {
    return {};
  }
  scan_runtime_handle create_scan_runtime(const runtime_build_request& request) const override
  {
    auto& op      = request.logical();
    auto& context = request.context;
    auto const* bind =
      dynamic_cast<sirius::exec::stream_source_bind_data const*>(op.bind_data.get());
    if (bind == nullptr || !bind->declaration) {
      throw duckdb::InternalException("sirius_stream_source is missing its stream bind data");
    }

    auto catalog        = sirius::exec::catalog_for(context);
    auto const& binding = *bind->declaration;

    // Projection pushdown is off; a narrowed column list here would disagree with the binder.
    auto column_ids = op.GetColumnIds();
    if (column_ids.size() != binding.types.size()) {
      throw duckdb::NotImplementedException(
        "sirius_stream_source: column projection into a stream read is not supported (stream "
        "declares %llu columns, plan requests %llu)",
        static_cast<unsigned long long>(binding.types.size()),
        static_cast<unsigned long long>(column_ids.size()));
    }

    auto source = duckdb::make_uniq<sirius::op::sirius_physical_streaming_source>(
      binding.types, op.EstimateCardinality(context), binding.repository, binding.expected_senders);

    // Lower the retained binding; never resolve a possibly newer declaration by stream ID.
    source->attach_binding(std::move(catalog), bind->declaration);
    return scan_runtime_handle(std::move(source));
  }
  source_policy_evidence inspect_source(const binding_ref&) const override { return {}; }
};
}  // namespace
std::unique_ptr<scan_source_adapter> make_stream_source_adapter()
{
  return std::make_unique<stream_source_adapter>();
}
}  // namespace sirius::scan
