/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once

#include "scan/scan_contract.hpp"
#include "scan/source_adapter.hpp"

#include <duckdb/planner/extension_callback.hpp>

#include <vector>

namespace sirius::scan {

/// One database-owned collection of adapters. Resolves before provider-specific casts;
/// lookup verifies actual factories or returns an explicitly marked compatibility adapter.
class source_registry final : public duckdb::ExtensionCallback {
 public:
  explicit source_registry(duckdb::DatabaseInstance& db);
  source_registry(const source_registry&)            = delete;
  source_registry& operator=(const source_registry&) = delete;
  static source_registry& get(duckdb::DatabaseInstance& db);
  [[nodiscard]] std::uint64_t instance_id() const noexcept { return _instance; }
  [[nodiscard]] duckdb::DatabaseInstance& database() const noexcept { return _db; }
  [[nodiscard]] const scan_source_adapter* lookup(const duckdb::TableFunction& function,
                                                  const duckdb::FunctionData* bind_data) const;
  const scan_source_adapter& require(const duckdb::TableFunction& function,
                                     const duckdb::FunctionData* bind_data,
                                     scan_attempt_diagnostics* diagnostics = nullptr) const;

  std::shared_ptr<contract_counters> counters = std::make_shared<contract_counters>();

 private:
  duckdb::DatabaseInstance& _db;
  const std::uint64_t _instance;
  std::vector<std::unique_ptr<scan_source_adapter>> _adapters;
};

}  // namespace sirius::scan
