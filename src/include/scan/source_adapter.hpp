/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once

#include "scan/bound_read_view.hpp"

#include <duckdb/common/unique_ptr.hpp>

#include <functional>
#include <memory>
#include <variant>

namespace duckdb {
class ClientContext;
class DatabaseInstance;
class LogicalGet;
class SiriusContext;
}  // namespace duckdb
namespace sirius {
struct operator_params;
namespace op {
class sirius_physical_operator;
class sirius_physical_table_scan;
namespace scan {
class gpu_ingestible;
}
}  // namespace op
namespace scan_manager {
struct pinned_entry;
}
}  // namespace sirius

namespace sirius::scan {
class query_scan_registry;

struct binding_ref {
  duckdb::DatabaseInstance& database;
  const duckdb::TableFunction& function;
  const duckdb::FunctionData* data;
};

enum class binding_verification { rejected, verified, compatibility };

struct capture_request {
  std::uint64_t instance;
  std::uint64_t generation;
  capture_origin origin;
  binding_ref binding;
  const duckdb::vector<std::string>& names;
  const duckdb::vector<duckdb::LogicalType>& types;
};

struct source_preflight_request {
  duckdb::ClientContext& context;
  duckdb::LogicalGet& get;
  duckdb::SiriusContext* sirius_state;
  bool compressed_materialization;
};

struct source_preflight_result {
  const scan_manager::pinned_entry* pinned = nullptr;
  bool serves_insert_deltas                = false;
};

/// Two existing construction lanes; a provider need not pretend to be a file ingestible.
struct runtime_build_request {
  duckdb::ClientContext& context;
  std::variant<std::reference_wrapper<duckdb::LogicalGet>,
               std::reference_wrapper<op::sirius_physical_table_scan>>
    binding;
  const operator_params* params = nullptr;
  std::shared_ptr<query_scan_registry> window;

  duckdb::LogicalGet& logical() const;
  op::sirius_physical_table_scan& physical() const;
  const operator_params& parameters() const;
};

/// Owns an ingestible or direct source operator, never result-producing work.
class scan_runtime_handle {
 public:
  explicit scan_runtime_handle(std::shared_ptr<op::scan::gpu_ingestible> ingestible);
  explicit scan_runtime_handle(duckdb::unique_ptr<op::sirius_physical_operator> source);
  scan_runtime_handle(scan_runtime_handle&&) noexcept;
  ~scan_runtime_handle();
  std::shared_ptr<op::scan::gpu_ingestible> take_ingestible();
  duckdb::unique_ptr<op::sirius_physical_operator> take_source_operator();

 private:
  std::variant<std::shared_ptr<op::scan::gpu_ingestible>,
               duckdb::unique_ptr<op::sirius_physical_operator>>
    _value;
};

/// Facts only. The framework combines these with source vetoes and statement policy.
struct source_policy_evidence {
  bool byte_source_discovery_complete = true;
  bool reads_s3                       = false;
};

/// Sirius-owned, database-lifetime adapter. Requests are borrowed for one call only.
/// Verification/capture never bind or perform I/O. Compatibility preflight/runtime retain
/// existing source behavior. The window orders provider preparation before protected construction.
class scan_source_adapter {
 public:
  virtual void declare_resources(query_scan_registry&,
                                 duckdb::ClientContext&,
                                 const binding_ref&) const
  {
  }
  virtual ~scan_source_adapter()                                                          = default;
  virtual const source_profile& profile() const noexcept                                  = 0;
  virtual binding_verification verify_binding(const binding_ref&) const                   = 0;
  virtual read_view_capture try_capture_bound_view(const capture_request&) const          = 0;
  virtual source_preflight_result preflight_source(const source_preflight_request&) const = 0;
  virtual scan_runtime_handle create_scan_runtime(const runtime_build_request&) const     = 0;
  // Compatibility discovery may enumerate through the original provider; never reuse it as D2.
  virtual source_policy_evidence inspect_source(const binding_ref&) const = 0;
};

}  // namespace sirius::scan
