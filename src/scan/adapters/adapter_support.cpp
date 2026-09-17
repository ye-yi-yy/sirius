/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "ingestible_support.hpp"

#include <duckdb/common/multi_file/multi_file_states.hpp>

namespace sirius::scan::detail {

void identity_field(std::string& out, const std::string& tag, const std::string& input)
{
  out += std::to_string(tag.size()) + ":" + tag + std::to_string(input.size()) + ":" + input;
}

bound_schema_ptr capture_schema(const capture_request& request)
{
  return std::make_shared<const bound_schema>(
    std::vector<std::string>(request.names.begin(), request.names.end()), request.types);
}

read_view_capture complete_read_view(const capture_request& request,
                                     const source_profile& profile,
                                     bound_schema_ptr schema,
                                     std::string source_identity)
{
  if (!schema || !schema->canonical_identity()) {
    return {request.instance, request.generation, request.origin, capture_status::unsupported};
  }
  std::string key;
  identity_number(key, "read_view_version", 1);
  identity_number(key, "instance", request.instance);
  identity_number(key, "kind", static_cast<unsigned>(profile.kind));
  identity_field(key, "profile", std::string(profile.id));
  identity_field(key, "schema", *schema->canonical_identity());
  key += source_identity;
  auto view = std::make_shared<const bound_read_view>(
    bound_read_view{profile.kind, std::string(profile.id), std::move(schema), std::move(key), {}});
  return {request.instance,
          request.generation,
          request.origin,
          capture_status::complete,
          std::move(view)};
}

bool same_type(const duckdb::LogicalType& lhs, const duckdb::LogicalType& rhs)
{
  if (lhs.id() != rhs.id() || lhs.InternalType() != rhs.InternalType()) { return false; }
  if (!lhs.AuxInfo() && !rhs.AuxInfo()) { return true; }
  // Signature-only sentinels have no payload. They are not readable column types.
  if (lhs.id() == duckdb::LogicalTypeId::ANY || lhs.id() == duckdb::LogicalTypeId::INVALID ||
      rhs.id() == duckdb::LogicalTypeId::ANY || rhs.id() == duckdb::LogicalTypeId::INVALID) {
    return lhs.id() == rhs.id() && !lhs.AuxInfo() && !rhs.AuxInfo();
  }
  bound_schema left({""}, {lhs});
  bound_schema right({""}, {rhs});
  return left.canonical_identity() && right.canonical_identity() && left.equals(right);
}

bool same_types(const duckdb::vector<duckdb::LogicalType>& lhs,
                const duckdb::vector<duckdb::LogicalType>& rhs)
{
  if (lhs.size() != rhs.size()) { return false; }
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    if (!same_type(lhs[i], rhs[i])) { return false; }
  }
  return true;
}

bool same_implementation(const duckdb::TableFunction& actual, const duckdb::TableFunction& expected)
{
  if (actual.name != expected.name || actual.extra_info != expected.extra_info ||
      !same_types(actual.arguments, expected.arguments) ||
      !same_types(actual.original_arguments, expected.original_arguments) ||
      !same_type(actual.varargs, expected.varargs) ||
      actual.named_parameters.size() != expected.named_parameters.size()) {
    return false;
  }
  for (const auto& parameter : expected.named_parameters) {
    auto found = actual.named_parameters.find(parameter.first);
    if (found == actual.named_parameters.end() || !same_type(found->second, parameter.second)) {
      return false;
    }
  }
  // All currently qualified factories have no semantic function_info. A non-null payload
  // requires its own versioned provider validator, not pointer equality.
  if (actual.function_info || expected.function_info) { return false; }
#define SIRIUS_COMPARE_SCAN_MEMBER(member) \
  if (actual.member != expected.member) { return false; }
  SIRIUS_COMPARE_SCAN_MEMBER(bind)
  SIRIUS_COMPARE_SCAN_MEMBER(bind_replace)
  SIRIUS_COMPARE_SCAN_MEMBER(bind_operator)
  SIRIUS_COMPARE_SCAN_MEMBER(init_global)
  SIRIUS_COMPARE_SCAN_MEMBER(init_local)
  SIRIUS_COMPARE_SCAN_MEMBER(function)
  SIRIUS_COMPARE_SCAN_MEMBER(in_out_function)
  SIRIUS_COMPARE_SCAN_MEMBER(in_out_function_final)
  SIRIUS_COMPARE_SCAN_MEMBER(statistics)
  SIRIUS_COMPARE_SCAN_MEMBER(statistics_extended)
  SIRIUS_COMPARE_SCAN_MEMBER(dependency)
  SIRIUS_COMPARE_SCAN_MEMBER(cardinality)
  SIRIUS_COMPARE_SCAN_MEMBER(rows_scanned)
  SIRIUS_COMPARE_SCAN_MEMBER(get_metrics)
  SIRIUS_COMPARE_SCAN_MEMBER(pushdown_complex_filter)
  SIRIUS_COMPARE_SCAN_MEMBER(pushdown_expression)
  SIRIUS_COMPARE_SCAN_MEMBER(to_string)
  SIRIUS_COMPARE_SCAN_MEMBER(dynamic_to_string)
  SIRIUS_COMPARE_SCAN_MEMBER(table_scan_progress)
  SIRIUS_COMPARE_SCAN_MEMBER(get_partition_data)
  SIRIUS_COMPARE_SCAN_MEMBER(get_bind_info)
  SIRIUS_COMPARE_SCAN_MEMBER(type_pushdown)
  SIRIUS_COMPARE_SCAN_MEMBER(get_multi_file_reader)
  SIRIUS_COMPARE_SCAN_MEMBER(supports_pushdown_type)
  SIRIUS_COMPARE_SCAN_MEMBER(supports_pushdown_extract)
  SIRIUS_COMPARE_SCAN_MEMBER(get_partition_info)
  SIRIUS_COMPARE_SCAN_MEMBER(get_partition_stats)
  SIRIUS_COMPARE_SCAN_MEMBER(get_virtual_columns)
  SIRIUS_COMPARE_SCAN_MEMBER(get_row_id_columns)
  SIRIUS_COMPARE_SCAN_MEMBER(set_scan_order)
  SIRIUS_COMPARE_SCAN_MEMBER(serialize)
  SIRIUS_COMPARE_SCAN_MEMBER(deserialize)
  SIRIUS_COMPARE_SCAN_MEMBER(verify_serialization)
  SIRIUS_COMPARE_SCAN_MEMBER(projection_pushdown)
  SIRIUS_COMPARE_SCAN_MEMBER(filter_pushdown)
  SIRIUS_COMPARE_SCAN_MEMBER(filter_prune)
  SIRIUS_COMPARE_SCAN_MEMBER(sampling_pushdown)
  SIRIUS_COMPARE_SCAN_MEMBER(late_materialization)
  SIRIUS_COMPARE_SCAN_MEMBER(order_preservation_type)
  SIRIUS_COMPARE_SCAN_MEMBER(global_initialization)
#undef SIRIUS_COMPARE_SCAN_MEMBER
  return true;
}

bool is_s3(const std::string& path)
{
  return path.size() >= 5 && (path[0] == 's' || path[0] == 'S') && path[1] == '3' &&
         path[2] == ':' && path[3] == '/' && path[4] == '/';
}

std::optional<std::vector<std::string>> legacy_multi_file_paths(const duckdb::FunctionData* data)
{
  const auto* bind = dynamic_cast<const duckdb::MultiFileBindData*>(data);
  if (!bind || !bind->file_list) { return std::nullopt; }
  std::vector<std::string> paths;
  for (const auto& file : bind->file_list->GetAllFiles()) {
    paths.push_back(file.path);
  }
  return paths;
}

source_policy_evidence inspect_legacy_multi_file_source(const binding_ref& binding)
{
  source_policy_evidence result;
  // Preserve unknown non-file routing; classification and replay safety are separate.
  if (!dynamic_cast<const duckdb::MultiFileBindData*>(binding.data)) { return result; }
  const auto paths                      = legacy_multi_file_paths(binding.data);
  result.byte_source_discovery_complete = paths.has_value();
  if (paths) {
    for (const auto& path : *paths) {
      result.reads_s3 |= is_s3(path);
    }
  }
  return result;
}

void populate_parquet_table_info(op::scan::parquet_ingestible_table_info& info,
                                 op::sirius_physical_table_scan& scan,
                                 const operator_params& params,
                                 std::vector<std::string> paths)
{
  if (paths.empty()) { throw std::runtime_error("[scan source adapter] No input files to scan"); }
  info.returned_types         = scan.returned_types;
  info.column_ids             = scan.column_ids;
  info.projection_ids         = scan.projection_ids;
  info.names                  = scan.names;
  info.table_filters          = std::move(scan.table_filters);
  info.resolved_file_paths    = std::move(paths);
  info.scan_output_arity      = scan.types.size();
  info.approximate_batch_size = params.scan_task_batch_size;
}

}  // namespace sirius::scan::detail
