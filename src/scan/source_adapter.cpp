/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "scan/source_adapter.hpp"

#include "op/scan/gpu_ingestible.hpp"
#include "op/sirius_physical_operator.hpp"

#include <duckdb/common/exception.hpp>

namespace sirius::scan {

duckdb::LogicalGet& runtime_build_request::logical() const
{
  return std::get<std::reference_wrapper<duckdb::LogicalGet>>(binding).get();
}
op::sirius_physical_table_scan& runtime_build_request::physical() const
{
  return std::get<std::reference_wrapper<op::sirius_physical_table_scan>>(binding).get();
}
const operator_params& runtime_build_request::parameters() const
{
  if (!params) { throw duckdb::InternalException("scan runtime parameters are missing"); }
  return *params;
}
scan_runtime_handle::scan_runtime_handle(std::shared_ptr<op::scan::gpu_ingestible> ingestible)
  : _value(std::move(ingestible))
{
}
scan_runtime_handle::scan_runtime_handle(duckdb::unique_ptr<op::sirius_physical_operator> source)
  : _value(std::move(source))
{
}
scan_runtime_handle::scan_runtime_handle(scan_runtime_handle&&) noexcept = default;
scan_runtime_handle::~scan_runtime_handle()                              = default;
std::shared_ptr<op::scan::gpu_ingestible> scan_runtime_handle::take_ingestible()
{
  return std::move(std::get<std::shared_ptr<op::scan::gpu_ingestible>>(_value));
}
duckdb::unique_ptr<op::sirius_physical_operator> scan_runtime_handle::take_source_operator()
{
  return std::move(std::get<duckdb::unique_ptr<op::sirius_physical_operator>>(_value));
}

}  // namespace sirius::scan
